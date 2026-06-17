from __future__ import annotations

import json
import shutil
import subprocess
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import func, select

from app.core.config import (
    JATO_MONTHLY_UPDATE_JOB_ROOT,
    PARQUET_PATH,
    PARTITIONED_PATH,
    PRECOMPUTED_DIR,
    PROJECT_ROOT,
)
from app.db.models import (
    ConfigImportBatch,
    ConfigProject,
    ConfigVariant,
    CountryNewsArticle,
    CountryNewsDigest,
    CurrentPrice,
    FinanceObservation,
    ImportBatch,
    MatchOverride,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
    ScrapeBatch,
    VocRawDocument,
    VocSourceRun,
)
from app.db.session import get_database_health, get_session_factory
from app.services.country_chat_service import get_country_chat_metadata
from app.services.jato_monthly_update_service import list_jato_monthly_update_jobs
from app.services.local_wiki_service import (
    get_local_wiki_collection_name,
    get_local_wiki_manifest_path,
)
from app.services.query_service import get_data_freshness

RAW_DATA_ROOT = PROJECT_ROOT / "01_RAW_DATA"
PROCESSED_DATA_ROOT = PROJECT_ROOT / "04_Processed_data"
BASELINE_ROOT = RAW_DATA_ROOT / "baseline"
PATCH_ROOT = RAW_DATA_ROOT / "patches"
ARCHIVE_ROOT = RAW_DATA_ROOT / "historyDataArchive"
NEWS_RAW_ROOT = PROCESSED_DATA_ROOT / "news" / "raw"
VOC_RAW_ROOT = PROCESSED_DATA_ROOT / "voc"
VOC_IMPLEMENTATION_STATUS_PATH = (
    PROJECT_ROOT
    / "Markdown_Readme"
    / "Fullstack"
    / "02_DataETL"
    / "VOC_FORUM_IMPLEMENTATION_STATUS_2026-04-19.md"
)
VOC_FEASIBILITY_PATH = (
    PROJECT_ROOT
    / "Markdown_Readme"
    / "Fullstack"
    / "02_DataETL"
    / "VOC_FORUM_SCRAPING_FEASIBILITY_2026-04-17.md"
)
VOC_TOOLKIT_README_PATH = PROJECT_ROOT / "07_ScrapingToolkit" / "README.md"
WIKI_MANIFEST_PATH = get_local_wiki_manifest_path()
WIKI_DB_ROOT = WIKI_MANIFEST_PATH.parent
ACTIVITY_WINDOW_DAYS = 84
AIRFLOW_UI_URL = "http://127.0.0.1:8080"
AIRFLOW_STACK_SERVICES = (
    "airflow-postgres",
    "airflow-webserver",
    "airflow-scheduler",
)

_MONTH_NAME_ORDER = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _run_airflow_subprocess(
    command: list[str], *, timeout: int = 30
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _docker_compose_airflow_base_command() -> list[str] | None:
    docker_binary = shutil.which("docker")
    if not docker_binary:
        return None
    version_result = _run_airflow_subprocess(
        [docker_binary, "compose", "version"], timeout=10
    )
    if version_result.returncode != 0:
        return None
    return [docker_binary, "compose", "--profile", "airflow"]


def _normalize_airflow_service_payload(
    service: str,
    payload: dict[str, Any] | None,
) -> dict[str, Any]:
    item = payload or {}
    raw_state = str(item.get("State") or "stopped").strip().lower()
    state = raw_state or "stopped"
    status_text = str(item.get("Status") or "").strip()
    health = str(item.get("Health") or "").strip()
    publishers = item.get("Publishers")
    published_ports: list[str] = []
    if isinstance(publishers, list):
        for publisher in publishers:
            if not isinstance(publisher, dict):
                continue
            target_port = publisher.get("TargetPort")
            published_port = publisher.get("PublishedPort")
            if (
                isinstance(target_port, int)
                and isinstance(published_port, int)
                and published_port > 0
            ):
                published_ports.append(f"{published_port}->{target_port}")
    return {
        "service": service,
        "state": state,
        "status": status_text or state,
        "health": health or None,
        "containerName": (
            str(item.get("Name") or item.get("Names") or "").strip()
            or None
        ),
        "publishedPorts": published_ports,
    }


def _parse_airflow_compose_ps(stdout: str) -> list[dict[str, Any]]:
    text = stdout.strip()
    payloads: list[dict[str, Any]] = []
    if text:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            payloads = [parsed]
        elif isinstance(parsed, list):
            payloads = [item for item in parsed if isinstance(item, dict)]
        else:
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    payloads.append(payload)

    raw_by_service: dict[str, dict[str, Any]] = {}
    for payload in payloads:
        service = str(payload.get("Service") or "").strip()
        if not service:
            continue
        raw_by_service[service] = payload
    return [
        _normalize_airflow_service_payload(service, raw_by_service.get(service))
        for service in AIRFLOW_STACK_SERVICES
    ]


def _build_airflow_status_payload(
    *,
    available: bool,
    services: list[dict[str, Any]] | None = None,
    detail: str | None = None,
) -> dict[str, Any]:
    normalized_services = services or [
        _normalize_airflow_service_payload(service, None)
        for service in AIRFLOW_STACK_SERVICES
    ]
    running_services = sum(
        1 for item in normalized_services if item.get("state") == "running"
    )
    total_services = len(AIRFLOW_STACK_SERVICES)
    running = running_services == total_services and total_services > 0
    webserver_running = any(
        item.get("service") == "airflow-webserver"
        and item.get("state") == "running"
        for item in normalized_services
    )
    if not available:
        mode = "unavailable"
        summary = (
            detail
            or "Docker Compose 不可用，当前环境无法通过网页按钮启动本地 Airflow。"
        )
    elif running:
        mode = "running"
        summary = detail or "本地 Airflow 栈运行中，可直接打开 Web UI 或停止服务。"
    elif running_services > 0:
        mode = "partial"
        summary = detail or "本地 Airflow 仅部分运行，可继续启动补齐或先停止。"
    else:
        mode = "stopped"
        summary = detail or "本地 Docker 可用，Airflow 当前未启动。"
    return {
        "available": available,
        "mode": mode,
        "detail": summary,
        "uiUrl": AIRFLOW_UI_URL,
        "running": running,
        "runningServices": running_services,
        "totalServices": total_services,
        "updatedAt": _utc_now().isoformat(),
        "services": normalized_services,
        "actions": {
            "canStart": available and running_services < total_services,
            "canStop": available and running_services > 0,
            "canOpenUi": webserver_running,
        },
    }


def _require_airflow_compose_base_command() -> list[str]:
    command = _docker_compose_airflow_base_command()
    if command is None:
        raise ValueError(
            "Docker Compose 不可用，当前环境无法执行本地 Airflow 控制命令。"
        )
    return command


def _compose_failure_message(action: str, result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "").strip()
    if detail:
        detail = detail.splitlines()[-1]
    return detail or f"docker compose {action} failed with exit code {result.returncode}"


def read_airflow_ops_status() -> dict[str, Any]:
    command = _docker_compose_airflow_base_command()
    if command is None:
        return _build_airflow_status_payload(available=False)

    result = _run_airflow_subprocess(
        [*command, "ps", "--format", "json"],
        timeout=20,
    )
    if result.returncode != 0:
        return _build_airflow_status_payload(
            available=True,
            detail=_compose_failure_message("ps", result),
        )

    return _build_airflow_status_payload(
        available=True,
        services=_parse_airflow_compose_ps(result.stdout),
    )


def start_airflow_stack() -> dict[str, Any]:
    status = read_airflow_ops_status()
    if status.get("running"):
        return {
            "action": "start",
            "detail": "Airflow 已经在运行。",
            "status": status,
        }

    command = _require_airflow_compose_base_command()
    startup_steps = [
        ([*command, "up", "-d", "airflow-postgres"], 120),
        ([*command, "up", "airflow-init"], 180),
        ([*command, "up", "-d", "airflow-webserver", "airflow-scheduler"], 180),
    ]
    for step_command, timeout in startup_steps:
        result = _run_airflow_subprocess(step_command, timeout=timeout)
        if result.returncode != 0:
            raise RuntimeError(_compose_failure_message("up", result))

    return {
        "action": "start",
        "detail": "Airflow 本地栈已启动。",
        "status": read_airflow_ops_status(),
    }


def stop_airflow_stack() -> dict[str, Any]:
    status = read_airflow_ops_status()
    if not status.get("available"):
        _require_airflow_compose_base_command()
    if not status.get("actions", {}).get("canStop"):
        return {
            "action": "stop",
            "detail": "Airflow 当前已经停止。",
            "status": status,
        }

    command = _require_airflow_compose_base_command()
    result = _run_airflow_subprocess(
        [*command, "stop", *AIRFLOW_STACK_SERVICES],
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(_compose_failure_message("stop", result))

    return {
        "action": "stop",
        "detail": "Airflow 本地栈已停止。",
        "status": read_airflow_ops_status(),
    }


def _relative_to_project(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def _safe_stat(path: Path) -> dict[str, Any]:
    exists = path.exists()
    payload: dict[str, Any] = {
        "path": _relative_to_project(path) or str(path),
        "exists": exists,
        "isDir": path.is_dir() if exists else False,
        "sizeBytes": None,
        "fileCount": None,
        "updatedAt": None,
    }
    if not exists:
        return payload

    stat = path.stat()
    payload["updatedAt"] = datetime.fromtimestamp(stat.st_mtime, UTC).isoformat()
    if path.is_file():
        payload["sizeBytes"] = stat.st_size
        return payload

    try:
        payload["fileCount"] = sum(1 for _ in path.iterdir())
    except OSError:
        payload["fileCount"] = None
    return payload


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _read_json_value(path: Path) -> Any | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _iter_json_files(root: Path, pattern: str = "*.json") -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    try:
        files = [path for path in root.rglob(pattern) if path.is_file()]
    except OSError:
        return []
    return sorted(files, key=lambda item: item.stat().st_mtime, reverse=True)


def _coerce_iso_timestamp(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC).isoformat()


def _latest_timestamp(left: str | None, right: str | None) -> str | None:
    if not left:
        return right
    if not right:
        return left
    return max(left, right)


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_month_label(value: str | None) -> tuple[int, int]:
    text = str(value or "").strip()
    if not text:
        return (0, 0)
    parts = text.split()
    if len(parts) >= 2 and parts[0].isdigit():
        return (int(parts[0]), _MONTH_NAME_ORDER.get(parts[1][:3].lower(), 0))
    return (0, 0)


def _status_from_exists(exists: bool) -> str:
    return "ready" if exists else "warning"


def _tone_from_database_health(health: dict[str, Any]) -> str:
    if not bool(health.get("enabled")):
        return "inactive"
    if bool(health.get("connected")):
        return "active"
    return "warning"


def _metric(label: str, value: Any, tone: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"label": label, "value": value}
    if tone:
        payload["tone"] = tone
    return payload


def _iso_or_none(value: datetime | None) -> str | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return None


def _build_file_inventory() -> list[dict[str, Any]]:
    partition_manifest = PARTITIONED_PATH / "manifest.json"
    inventory = [
        {
            "key": "jato-parquet",
            "label": "JATO Full Archive",
            "kind": "file",
            **_safe_stat(PARQUET_PATH),
        },
        {
            "key": "jato-partitioned",
            "label": "JATO Partitioned Dataset",
            "kind": "directory",
            **_safe_stat(PARTITIONED_PATH),
        },
        {
            "key": "jato-partition-manifest",
            "label": "Partition Manifest",
            "kind": "file",
            **_safe_stat(partition_manifest),
        },
        {
            "key": "jato-raw-baseline",
            "label": "Raw Baseline Directory",
            "kind": "directory",
            **_safe_stat(BASELINE_ROOT),
        },
        {
            "key": "jato-raw-patches",
            "label": "Raw Patch Directory",
            "kind": "directory",
            **_safe_stat(PATCH_ROOT),
        },
        {
            "key": "jato-raw-archive",
            "label": "Raw Archive Directory",
            "kind": "directory",
            **_safe_stat(ARCHIVE_ROOT),
        },
        {
            "key": "jato-monthly-jobs",
            "label": "Monthly Update Job Root",
            "kind": "directory",
            **_safe_stat(JATO_MONTHLY_UPDATE_JOB_ROOT),
        },
        {
            "key": "summaries-root",
            "label": "Precomputed Summary Root",
            "kind": "directory",
            **_safe_stat(PRECOMPUTED_DIR),
        },
        {
            "key": "wiki-manifest",
            "label": "Local Wiki Manifest",
            "kind": "file",
            **_safe_stat(WIKI_MANIFEST_PATH),
        },
        {
            "key": "wiki-db-root",
            "label": "Local Wiki DB Root",
            "kind": "directory",
            **_safe_stat(WIKI_DB_ROOT),
        },
        {
            "key": "news-raw-root",
            "label": "News Raw Run Root",
            "kind": "directory",
            **_safe_stat(NEWS_RAW_ROOT),
        },
        {
            "key": "voc-raw-root",
            "label": "VOC Raw Root",
            "kind": "directory",
            **_safe_stat(VOC_RAW_ROOT),
        },
    ]
    return inventory


def _coerce_news_batch_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
        return [payload]
    return []


def _build_news_raw_domain() -> dict[str, Any]:
    recent_items: list[dict[str, Any]] = []
    run_count = 0
    country_count = 0
    article_count = 0
    error_count = 0
    latest_updated_at: str | None = None

    for path in _iter_json_files(NEWS_RAW_ROOT):
        batch_items = _coerce_news_batch_items(_read_json_value(path))
        if not batch_items:
            continue
        stat = _safe_stat(path)
        updated_at = stat.get("updatedAt")
        latest_updated_at = _latest_timestamp(latest_updated_at, updated_at)
        run_count += 1
        file_country_count = 0
        file_article_count = 0
        file_error_count = 0
        batch_codes: list[str] = []
        for item in batch_items:
            batch_codes.append(str(item.get("batch_code") or path.stem))
            file_country_count += _coerce_int(
                item.get("country_count") or len(item.get("countries") or []),
            )
            file_article_count += _coerce_int(item.get("article_count"))
            file_error_count += len(item.get("errors") or [])
        country_count += file_country_count
        article_count += file_article_count
        error_count += file_error_count
        label = (
            batch_codes[0]
            if len(batch_codes) == 1
            else f"{path.stem} ({len(batch_codes)} batches)"
        )
        recent_items.append(
            {
                "label": label,
                "value": (
                    f"{file_country_count} countries · "
                    f"{file_article_count} articles · "
                    f"{file_error_count} errors"
                ),
                "updatedAt": updated_at,
            }
        )

    if run_count <= 0:
        status = "inactive" if NEWS_RAW_ROOT.exists() else "warning"
    elif error_count > 0:
        status = "warning"
    else:
        status = "ready"

    return {
        "key": "news-raw",
        "label": "News Raw Fetches",
        "storage": "local json",
        "status": status,
        "updatedAt": latest_updated_at,
        "summary": (
            "RSS/Atom 原始抓取批次 JSON，按运行维度统计 article 数、国家数和错误数。"
            f" 根目录：{_relative_to_project(NEWS_RAW_ROOT) or str(NEWS_RAW_ROOT)}"
        ),
        "metrics": [
            _metric("Runs", run_count),
            _metric("Countries", country_count),
            _metric("Articles", article_count),
            _metric("Errors", error_count),
        ],
        "recentItems": recent_items,
    }


def _build_voc_raw_domain() -> dict[str, Any]:
    source_count = 0
    country_count = 0
    document_count = 0
    error_count = 0
    latest_updated_at: str | None = None
    country_rollups: dict[str, dict[str, Any]] = {}

    for path in _iter_json_files(VOC_RAW_ROOT, pattern="*.json"):
        if path.parent.name != "raw":
            continue
        payload = _read_json_value(path)
        if not isinstance(payload, dict):
            continue
        source = payload.get("source")
        source_payload = source if isinstance(source, dict) else {}
        country_code = str(
            source_payload.get("country_code") or path.parent.parent.name.upper(),
        ).strip().upper()
        if not country_code:
            continue
        country_label = str(
            source_payload.get("country_label") or country_code,
        ).strip() or country_code
        source_documents = _coerce_int(
            payload.get("documentCount") or len(payload.get("documents") or []),
        )
        source_errors = len(payload.get("errors") or [])
        updated_at = _coerce_iso_timestamp(payload.get("collectedAt")) or _safe_stat(path).get(
            "updatedAt",
        )
        latest_updated_at = _latest_timestamp(latest_updated_at, updated_at)
        source_count += 1
        document_count += source_documents
        error_count += source_errors
        rollup = country_rollups.setdefault(
            country_code,
            {
                "label": country_label,
                "sourceCount": 0,
                "documentCount": 0,
                "errorCount": 0,
                "updatedAt": None,
            },
        )
        rollup["sourceCount"] = _coerce_int(rollup.get("sourceCount")) + 1
        rollup["documentCount"] = _coerce_int(rollup.get("documentCount")) + source_documents
        rollup["errorCount"] = _coerce_int(rollup.get("errorCount")) + source_errors
        rollup["updatedAt"] = _latest_timestamp(
            rollup.get("updatedAt"),
            updated_at,
        )

    country_count = len(country_rollups)
    recent_items = [
        {
            "label": item["label"],
            "value": (
                f"{_coerce_int(item['sourceCount'])} sources · "
                f"{_coerce_int(item['documentCount'])} docs · "
                f"{_coerce_int(item['errorCount'])} errors"
            ),
            "updatedAt": item.get("updatedAt"),
        }
        for _, item in sorted(
            country_rollups.items(),
            key=lambda pair: str(pair[1].get("updatedAt") or ""),
            reverse=True,
        )
    ]

    if source_count <= 0:
        status = "inactive" if VOC_RAW_ROOT.exists() else "warning"
    elif error_count > 0:
        status = "warning"
    else:
        status = "ready"

    return {
        "key": "voc-raw",
        "label": "VOC Raw Collector",
        "storage": "local json",
        "status": status,
        "updatedAt": latest_updated_at,
        "summary": (
            "公开论坛 / 媒体评论 / EV 社区的原始抓取文档，按国家汇总 sources、documents 与 errors。"
            f" 根目录：{_relative_to_project(VOC_RAW_ROOT) or str(VOC_RAW_ROOT)}"
        ),
        "metrics": [
            _metric("Countries", country_count),
            _metric("Sources", source_count),
            _metric("Documents", document_count),
            _metric("Errors", error_count),
        ],
        "recentItems": recent_items,
    }


def _artifact_item(key: str, label: str, path: Path, kind: str) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "kind": kind,
        **_safe_stat(path),
    }


def _voc_text_extraction_methods(documents: list[dict[str, Any]]) -> list[str]:
    counter: defaultdict[str, int] = defaultdict(int)
    for document in documents:
        if not isinstance(document, dict):
            continue
        extraction = document.get("textExtraction")
        if not isinstance(extraction, dict):
            continue
        method = str(extraction.get("method") or "").strip()
        if method:
            counter[method] += 1
    return [
        f"{method} × {count}"
        for method, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _build_voc_country_snapshot(country_root: Path) -> dict[str, Any] | None:
    if not country_root.exists() or not country_root.is_dir():
        return None

    country_code = country_root.name.strip().upper()
    raw_root = country_root / "raw"
    enriched_path = country_root / "enriched" / "customer_insight_signals.json"
    deck_path = country_root / "deck" / "customer_insight_deck.json"

    raw_source_runs: list[dict[str, Any]] = []
    raw_source_count = 0
    raw_document_count = 0
    raw_error_count = 0
    publish_ready_count = 0
    latest_updated_at: str | None = None
    country_label = country_code

    for path in _iter_json_files(raw_root, pattern="*.json"):
        payload = _read_json_value(path)
        if not isinstance(payload, dict):
            continue
        source = payload.get("source")
        source_payload = source if isinstance(source, dict) else {}
        country_label = (
            str(source_payload.get("country_label") or country_label).strip()
            or country_label
        )
        updated_at = _coerce_iso_timestamp(payload.get("collectedAt")) or _safe_stat(path).get(
            "updatedAt",
        )
        latest_updated_at = _latest_timestamp(latest_updated_at, updated_at)
        auto_review = payload.get("autoReview") if isinstance(payload.get("autoReview"), dict) else {}
        documents = [
            item
            for item in payload.get("documents") or []
            if isinstance(item, dict)
        ]
        raw_source_count += 1
        raw_document_count += _coerce_int(payload.get("documentCount") or len(documents))
        raw_error_count += len(payload.get("errors") or [])
        publish_ready_count += _coerce_int(auto_review.get("publishReadyCount"))
        raw_source_runs.append(
            {
                "sourceCode": str(source_payload.get("source_code") or path.stem),
                "siteName": str(source_payload.get("site_name") or "").strip() or path.stem,
                "siteType": str(source_payload.get("site_type") or "").strip() or "unknown",
                "language": str(source_payload.get("language") or "").strip() or None,
                "publishTier": str(auto_review.get("publishTier") or "").strip() or None,
                "publishDecision": str(auto_review.get("publishDecision") or "").strip() or None,
                "documentCount": _coerce_int(payload.get("documentCount") or len(documents)),
                "publishReadyCount": _coerce_int(auto_review.get("publishReadyCount")),
                "errorCount": len(payload.get("errors") or []),
                "updatedAt": updated_at,
                "path": _relative_to_project(path) or str(path),
                "textExtractionMethods": _voc_text_extraction_methods(documents),
            }
        )

    raw_source_runs.sort(
        key=lambda item: (
            str(item.get("updatedAt") or ""),
            str(item.get("sourceCode") or ""),
        ),
        reverse=True,
    )

    enriched_payload = _read_json_if_exists(enriched_path)
    deck_payload = _read_json_if_exists(deck_path)
    if isinstance(enriched_payload, dict):
        country_label = (
            str(enriched_payload.get("countryLabel") or country_label).strip()
            or country_label
        )
        latest_updated_at = _latest_timestamp(
            latest_updated_at,
            _coerce_iso_timestamp(enriched_payload.get("generatedAt")) or _safe_stat(enriched_path).get("updatedAt"),
        )
    if isinstance(deck_payload, dict):
        country_label = (
            str(deck_payload.get("countryLabel") or country_label).strip()
            or country_label
        )
        latest_updated_at = _latest_timestamp(
            latest_updated_at,
            _coerce_iso_timestamp(deck_payload.get("generatedAt")) or _safe_stat(deck_path).get("updatedAt"),
        )

    status = "inactive"
    if raw_source_count > 0:
        status = "warning" if raw_error_count > 0 else "ready"
    if raw_source_count <= 0 and (enriched_path.exists() or deck_path.exists()):
        status = "warning"

    return {
        "code": country_code,
        "label": country_label,
        "status": status,
        "updatedAt": latest_updated_at,
        "rawSourceCount": raw_source_count,
        "rawDocumentCount": raw_document_count,
        "publishReadyCount": publish_ready_count,
        "rawErrorCount": raw_error_count,
        "signalObservationCount": _coerce_int(
            enriched_payload.get("signalObservationCount") if isinstance(enriched_payload, dict) else 0
        ),
        "deckEvidenceCardCount": len(deck_payload.get("evidenceCards") or []) if isinstance(deck_payload, dict) else 0,
        "qualityScoreAvg": (
            float(enriched_payload.get("qualityScoreAvg") or 0)
            if isinstance(enriched_payload, dict)
            else 0.0
        ),
        "artifacts": {
            "rawRoot": raw_root,
            "enrichedPath": enriched_path,
            "deckPath": deck_path,
        },
        "rawSourceRuns": raw_source_runs,
        "enrichedPayload": enriched_payload or {},
        "deckPayload": deck_payload or {},
    }


def _list_voc_country_snapshots() -> list[dict[str, Any]]:
    if not VOC_RAW_ROOT.exists() or not VOC_RAW_ROOT.is_dir():
        return []
    snapshots: list[dict[str, Any]] = []
    for path in sorted(VOC_RAW_ROOT.iterdir()):
        if not path.is_dir():
            continue
        snapshot = _build_voc_country_snapshot(path)
        if snapshot is not None:
            snapshots.append(snapshot)
    snapshots.sort(
        key=lambda item: (
            str(item.get("updatedAt") or ""),
            str(item.get("code") or ""),
        ),
        reverse=True,
    )
    return snapshots


def _build_voc_documentation_refs() -> list[dict[str, Any]]:
    return [
        {
            "label": "VOC implementation status",
            **_safe_stat(VOC_IMPLEMENTATION_STATUS_PATH),
        },
        {
            "label": "VOC feasibility",
            **_safe_stat(VOC_FEASIBILITY_PATH),
        },
        {
            "label": "Scraping toolkit README",
            **_safe_stat(VOC_TOOLKIT_README_PATH),
        },
    ]


def _read_voc_staging_summary(country_code: str) -> dict[str, Any]:
    health = get_database_health()
    if not bool(health.get("enabled")) or not bool(health.get("connected")):
        return {
            "databaseConnected": False,
            "sourceRunCount": 0,
            "documentCount": 0,
            "publishReadyCount": 0,
            "latestCollectedAt": None,
        }

    session_factory = get_session_factory()
    with session_factory() as session:
        source_run_count = session.execute(
            select(func.count()).select_from(VocSourceRun).where(
                VocSourceRun.country_code == country_code,
            ),
        ).scalar_one()
        document_count = session.execute(
            select(func.count()).select_from(VocRawDocument).where(
                VocRawDocument.country_code == country_code,
            ),
        ).scalar_one()
        publish_ready_count = session.execute(
            select(func.coalesce(func.sum(VocSourceRun.publish_ready_count), 0)).where(
                VocSourceRun.country_code == country_code,
            ),
        ).scalar_one()
        latest_collected_at = session.execute(
            select(func.max(VocSourceRun.collected_at_utc)).where(
                VocSourceRun.country_code == country_code,
            ),
        ).scalar_one()

    return {
        "databaseConnected": True,
        "sourceRunCount": _coerce_int(source_run_count),
        "documentCount": _coerce_int(document_count),
        "publishReadyCount": _coerce_int(publish_ready_count),
        "latestCollectedAt": _iso_or_none(latest_collected_at),
    }


def read_voc_management_overview(country_code: str | None = None) -> dict[str, Any]:
    country_snapshots = _list_voc_country_snapshots()
    selected_snapshot = None
    normalized_country = str(country_code or "").strip().upper()
    if normalized_country:
        selected_snapshot = next(
            (item for item in country_snapshots if item["code"] == normalized_country),
            None,
        )
    if selected_snapshot is None and country_snapshots:
        selected_snapshot = country_snapshots[0]

    if selected_snapshot is None:
        selected_snapshot = {
            "code": normalized_country or "",
            "label": normalized_country or "VOC",
            "status": "warning",
            "updatedAt": None,
            "rawSourceCount": 0,
            "rawDocumentCount": 0,
            "publishReadyCount": 0,
            "rawErrorCount": 0,
            "signalObservationCount": 0,
            "deckEvidenceCardCount": 0,
            "qualityScoreAvg": 0.0,
            "artifacts": {
                "rawRoot": VOC_RAW_ROOT / (normalized_country.lower() if normalized_country else "unknown") / "raw",
                "enrichedPath": VOC_RAW_ROOT / (normalized_country.lower() if normalized_country else "unknown") / "enriched" / "customer_insight_signals.json",
                "deckPath": VOC_RAW_ROOT / (normalized_country.lower() if normalized_country else "unknown") / "deck" / "customer_insight_deck.json",
            },
            "rawSourceRuns": [],
            "enrichedPayload": {},
            "deckPayload": {},
        }

    overall_metrics = [
        _metric("Countries", len(country_snapshots)),
        _metric("Sources", sum(item["rawSourceCount"] for item in country_snapshots)),
        _metric("Documents", sum(item["rawDocumentCount"] for item in country_snapshots)),
        _metric("Signal observations", sum(item["signalObservationCount"] for item in country_snapshots)),
        _metric("Deck-ready countries", sum(1 for item in country_snapshots if Path(item["artifacts"]["deckPath"]).exists())),
    ]
    country_metrics = [
        _metric("Sources", selected_snapshot["rawSourceCount"]),
        _metric("Documents", selected_snapshot["rawDocumentCount"]),
        _metric("Publish-ready", selected_snapshot["publishReadyCount"]),
        _metric("Signal observations", selected_snapshot["signalObservationCount"]),
        _metric("Evidence cards", selected_snapshot["deckEvidenceCardCount"]),
        _metric("Raw errors", selected_snapshot["rawErrorCount"]),
    ]
    if selected_snapshot["qualityScoreAvg"] > 0:
        country_metrics.append(_metric("Avg quality score", round(selected_snapshot["qualityScoreAvg"], 2)))

    deck_payload = selected_snapshot.get("deckPayload") or {}
    staging = _read_voc_staging_summary(selected_snapshot["code"]) if selected_snapshot["code"] else {
        "databaseConnected": False,
        "sourceRunCount": 0,
        "documentCount": 0,
        "publishReadyCount": 0,
        "latestCollectedAt": None,
    }

    return {
        "generatedAt": _utc_now().isoformat(),
        "selectedCountryCode": selected_snapshot["code"],
        "selectedCountryLabel": selected_snapshot["label"],
        "availableCountries": [
            {
                "code": item["code"],
                "label": item["label"],
                "status": item["status"],
                "updatedAt": item["updatedAt"],
                "rawSourceCount": item["rawSourceCount"],
                "rawDocumentCount": item["rawDocumentCount"],
                "publishReadyCount": item["publishReadyCount"],
                "signalObservationCount": item["signalObservationCount"],
                "deckReady": Path(item["artifacts"]["deckPath"]).exists(),
            }
            for item in country_snapshots
        ],
        "overallMetrics": overall_metrics,
        "countryMetrics": country_metrics,
        "artifacts": [
            _artifact_item("voc-country-raw", "VOC raw directory", selected_snapshot["artifacts"]["rawRoot"], "directory"),
            _artifact_item("voc-country-enriched", "VOC enriched signals", selected_snapshot["artifacts"]["enrichedPath"], "file"),
            _artifact_item("voc-country-deck", "VOC country deck", selected_snapshot["artifacts"]["deckPath"], "file"),
        ],
        "sourceRuns": selected_snapshot["rawSourceRuns"],
        "observedSections": [
            str(item).strip()
            for item in deck_payload.get("observedSections") or []
            if str(item).strip()
        ],
        "inferredSections": [
            str(item).strip()
            for item in deck_payload.get("inferredSections") or []
            if str(item).strip()
        ],
        "topPainPoints": [
            item
            for item in deck_payload.get("painPoints") or []
            if isinstance(item, dict)
        ][:5],
        "topProductSignals": [
            item
            for item in deck_payload.get("productSignals") or []
            if isinstance(item, dict)
        ][:5],
        "evidenceCards": [
            {
                "title": str(item.get("title") or ""),
                "url": str(item.get("url") or ""),
                "siteName": str(item.get("siteName") or ""),
                "publishTier": str(item.get("publishTier") or ""),
                "signals": [
                    str(signal).strip()
                    for signal in item.get("signals") or []
                    if str(signal).strip()
                ],
                "snippet": (
                    str((item.get("evidenceSnippets") or [""])[0] or "").strip()
                ),
            }
            for item in deck_payload.get("evidenceCards") or []
            if isinstance(item, dict)
        ][:5],
        "documentation": _build_voc_documentation_refs(),
        "staging": staging,
    }


def _build_jato_domain(file_inventory: list[dict[str, Any]]) -> dict[str, Any]:
    freshness_items = get_data_freshness()
    latest_month = max(
        (str(item.get("latestMonth", "")) for item in freshness_items),
        key=_parse_month_label,
        default="",
    )
    jobs_payload = list_jato_monthly_update_jobs(limit=5)
    jobs = jobs_payload.get("items") if isinstance(jobs_payload, dict) else []
    latest_job = jobs[0] if isinstance(jobs, list) and jobs else None
    parquet_item = next(
        (item for item in file_inventory if item["key"] == "jato-parquet"),
        None,
    )
    baseline_count = sum(
        1 for path in BASELINE_ROOT.glob("*")
        if path.is_file()
    ) if BASELINE_ROOT.exists() else 0
    patch_count = sum(1 for path in PATCH_ROOT.glob("*") if path.is_dir()) if PATCH_ROOT.exists() else 0

    updated_at = None
    if parquet_item is not None:
        updated_at = parquet_item.get("updatedAt")
    if updated_at is None and latest_job is not None:
        updated_at = latest_job.get("updatedAt")

    return {
        "key": "jato",
        "label": "JATO Core",
        "storage": "files",
        "status": _status_from_exists(bool(parquet_item and parquet_item.get("exists"))),
        "updatedAt": updated_at,
        "summary": "Parquet 数据集、raw baseline/patch、月更任务与国家 freshness。",
        "metrics": [
            _metric("Countries", len(freshness_items)),
            _metric("Latest month", latest_month or "-"),
            _metric("Baselines", baseline_count),
            _metric("Patch months", patch_count),
            _metric("Monthly jobs", len(jobs) if isinstance(jobs, list) else 0),
        ],
        "recentItems": jobs if isinstance(jobs, list) else [],
    }


def _build_country_assistant_domain() -> dict[str, Any]:
    try:
        metadata = get_country_chat_metadata()
        available_countries = metadata.get("availableCountries")
        countries = available_countries if isinstance(available_countries, list) else []
        models = metadata.get("availableChatModels")
        chat_models = models if isinstance(models, list) else []
        provider_available = bool(metadata.get("providerAvailable"))
        return {
            "key": "country-assistant",
            "label": "Country Assistant",
            "storage": "hybrid",
            "status": "ready" if provider_available else "warning",
            "updatedAt": None,
            "summary": "国家助手元数据、可用国家、聊天模型与 provider 状态。",
            "metrics": [
                _metric("Countries", len(countries)),
                _metric("Chat models", len(chat_models)),
                _metric("Provider", "ready" if provider_available else "fallback"),
            ],
            "recentItems": [
                {
                    "label": item.get("label") or item.get("value"),
                    "value": item.get("value"),
                }
                for item in countries
                if isinstance(item, dict)
            ],
        }
    except Exception as exc:
        return {
            "key": "country-assistant",
            "label": "Country Assistant",
            "storage": "hybrid",
            "status": "warning",
            "updatedAt": None,
            "summary": f"Country Assistant metadata unavailable: {exc}",
            "metrics": [
                _metric("Countries", 0, tone="warning"),
            ],
            "recentItems": [],
        }


def _build_wiki_domain(file_inventory: list[dict[str, Any]]) -> dict[str, Any]:
    manifest = _read_json_if_exists(WIKI_MANIFEST_PATH) or {}
    manifest_item = next(
        (item for item in file_inventory if item["key"] == "wiki-manifest"),
        None,
    )
    updated_at = manifest_item.get("updatedAt") if manifest_item else None
    document_count = int(manifest.get("documentCount", 0) or 0)
    collection_name = str(
        manifest.get("collectionName") or get_local_wiki_collection_name()
    ).strip()
    source_path = str(manifest.get("sourcePath") or "").strip() or "-"
    return {
        "key": "wiki",
        "label": "Local Wiki",
        "storage": "files",
        "status": _status_from_exists(bool(manifest_item and manifest_item.get("exists"))),
        "updatedAt": updated_at,
        "summary": "本地 Chroma wiki 库、manifest、源数据路径与文档规模。",
        "metrics": [
            _metric("Collection", collection_name),
            _metric("Documents", document_count),
            _metric("Source", source_path),
        ],
        "recentItems": [
            {"label": "Manifest", "value": manifest_item.get("path") if manifest_item else "-"},
            {"label": "DB root", "value": _relative_to_project(WIKI_DB_ROOT) or str(WIKI_DB_ROOT)},
        ],
    }


def _query_table_summary(
    session: Any,
    *,
    model: Any,
    schema: str,
    label: str,
    domain: str,
    timestamp_column: Any,
) -> dict[str, Any]:
    row_count = int(
        session.execute(select(func.count()).select_from(model)).scalar_one()
        or 0
    )
    last_event_at = session.execute(select(func.max(timestamp_column))).scalar_one()
    return {
        "key": f"{schema}.{model.__tablename__}",
        "label": label,
        "domain": domain,
        "schema": schema,
        "table": model.__tablename__,
        "rowCount": row_count,
        "lastEventAt": (
            _iso_or_none(last_event_at)
        ),
        "status": "ready" if row_count > 0 else "inactive",
    }


def _collect_recent_snapshot_items(session: Any) -> list[dict[str, Any]]:
    snapshot_rows: list[dict[str, Any]] = []

    observation_rows = session.execute(
        select(
            MsrpObservation.country,
            MsrpObservation.brand,
            MsrpObservation.jato_model,
            MsrpObservation.source_snapshot_path,
            MsrpObservation.observed_at_utc,
        )
        .where(MsrpObservation.source_snapshot_path.is_not(None))
        .order_by(MsrpObservation.observed_at_utc.desc())
        .limit(8)
    ).all()
    snapshot_rows.extend(
        {
            "group": "Observation",
            "label": f"{row.country} · {row.brand} · {row.jato_model}",
            "value": str(row.source_snapshot_path),
            "updatedAt": _iso_or_none(row.observed_at_utc),
        }
        for row in observation_rows
    )

    current_price_rows = session.execute(
        select(
            CurrentPrice.country,
            CurrentPrice.brand,
            CurrentPrice.jato_model,
            CurrentPrice.source_snapshot_path,
            CurrentPrice.updated_at_utc,
        )
        .where(CurrentPrice.source_snapshot_path.is_not(None))
        .order_by(CurrentPrice.updated_at_utc.desc())
        .limit(8)
    ).all()
    snapshot_rows.extend(
        {
            "group": "Current Price",
            "label": f"{row.country} · {row.brand} · {row.jato_model}",
            "value": str(row.source_snapshot_path),
            "updatedAt": _iso_or_none(row.updated_at_utc),
        }
        for row in current_price_rows
    )

    review_rows = session.execute(
        select(
            ReviewCase.country,
            ReviewCase.brand,
            ReviewCase.jato_model,
            ReviewCase.source_snapshot_path,
            ReviewCase.updated_at_utc,
        )
        .where(ReviewCase.source_snapshot_path.is_not(None))
        .order_by(ReviewCase.updated_at_utc.desc())
        .limit(8)
    ).all()
    snapshot_rows.extend(
        {
            "group": "Review",
            "label": f"{row.country} · {row.brand} · {row.jato_model}",
            "value": str(row.source_snapshot_path),
            "updatedAt": _iso_or_none(row.updated_at_utc),
        }
        for row in review_rows
    )

    deduped: dict[str, dict[str, Any]] = {}
    for item in snapshot_rows:
        key = str(item["value"])
        existing = deduped.get(key)
        if existing is None or str(item.get("updatedAt") or "") > str(existing.get("updatedAt") or ""):
            deduped[key] = item

    return sorted(
        deduped.values(),
        key=lambda item: str(item.get("updatedAt") or ""),
        reverse=True,
    )[:8]


def _add_activity_counts(
    activity: dict[date, int],
    source_counts: dict[str, int],
    *,
    source: str,
    stamps: list[datetime],
    since_day: date,
) -> None:
    for stamp in stamps:
        if not isinstance(stamp, datetime):
            continue
        day = stamp.astimezone(UTC).date()
        if day < since_day:
            continue
        activity[day] += 1
        source_counts[source] += 1


def _query_day_counts(
    session: Any,
    *,
    model: Any,
    timestamp_column: Any,
    since_at: datetime,
) -> list[tuple[date, int]]:
    rows = session.execute(
        select(
            func.date_trunc("day", timestamp_column).label("event_day"),
            func.count().label("count"),
        )
        .where(timestamp_column.is_not(None))
        .where(timestamp_column >= since_at)
        .group_by("event_day")
        .order_by("event_day")
    ).all()
    results: list[tuple[date, int]] = []
    for row in rows:
        event_day = row[0]
        count = row[1]
        if isinstance(event_day, datetime):
            results.append((event_day.astimezone(UTC).date(), int(count or 0)))
    return results


def _build_database_payload() -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    dict[date, int],
    dict[str, int],
    list[dict[str, Any]],
]:
    health = get_database_health()
    if not bool(health.get("connected")):
        return (
            health,
            [],
            {},
            {},
            [
                {
                    "key": "database",
                    "label": "Database",
                    "storage": "database",
                    "status": "inactive" if not bool(health.get("enabled")) else "warning",
                    "updatedAt": None,
                    "summary": str(health.get("detail") or "Database unavailable"),
                    "metrics": [
                        _metric("Enabled", "yes" if bool(health.get("enabled")) else "no"),
                        _metric("Connected", "yes" if bool(health.get("connected")) else "no"),
                    ],
                    "recentItems": [],
                }
            ],
        )

    session_factory = get_session_factory()
    since_at = _utc_now() - timedelta(days=ACTIVITY_WINDOW_DAYS - 1)
    activity_by_day: dict[date, int] = defaultdict(int)
    activity_by_source: dict[str, int] = defaultdict(int)

    with session_factory() as session:
        table_items = [
            _query_table_summary(
                session,
                model=ImportBatch,
                schema="ops",
                label="Ops Import Batches",
                domain="ops",
                timestamp_column=ImportBatch.created_at_utc,
            ),
            _query_table_summary(
                session,
                model=CountryNewsDigest,
                schema="ops",
                label="Country News Digests",
                domain="news",
                timestamp_column=CountryNewsDigest.synced_at_utc,
            ),
            _query_table_summary(
                session,
                model=CountryNewsArticle,
                schema="ops",
                label="Country News Articles",
                domain="news",
                timestamp_column=CountryNewsArticle.synced_at_utc,
            ),
            _query_table_summary(
                session,
                model=VocSourceRun,
                schema="ops",
                label="VOC Source Runs",
                domain="voc",
                timestamp_column=VocSourceRun.collected_at_utc,
            ),
            _query_table_summary(
                session,
                model=VocRawDocument,
                schema="ops",
                label="VOC Raw Documents",
                domain="voc",
                timestamp_column=VocRawDocument.collected_at_utc,
            ),
            _query_table_summary(
                session,
                model=ConfigProject,
                schema="engineering",
                label="Config Projects",
                domain="engineering",
                timestamp_column=ConfigProject.updated_at_utc,
            ),
            _query_table_summary(
                session,
                model=ConfigImportBatch,
                schema="engineering",
                label="Config Import Batches",
                domain="engineering",
                timestamp_column=ConfigImportBatch.created_at_utc,
            ),
            _query_table_summary(
                session,
                model=ConfigVariant,
                schema="engineering",
                label="Config Variants",
                domain="engineering",
                timestamp_column=ConfigVariant.updated_at_utc,
            ),
            _query_table_summary(
                session,
                model=MsrpSource,
                schema="msrp",
                label="MSRP Sources",
                domain="msrp",
                timestamp_column=MsrpSource.updated_at_utc,
            ),
            _query_table_summary(
                session,
                model=ScrapeBatch,
                schema="msrp",
                label="MSRP Scrape Batches",
                domain="msrp",
                timestamp_column=ScrapeBatch.started_at_utc,
            ),
            _query_table_summary(
                session,
                model=MsrpObservation,
                schema="msrp",
                label="MSRP Observations",
                domain="msrp",
                timestamp_column=MsrpObservation.observed_at_utc,
            ),
            _query_table_summary(
                session,
                model=FinanceObservation,
                schema="msrp",
                label="MSRP Finance Observations",
                domain="msrp",
                timestamp_column=FinanceObservation.observed_at_utc,
            ),
            _query_table_summary(
                session,
                model=CurrentPrice,
                schema="msrp",
                label="MSRP Current Prices",
                domain="msrp",
                timestamp_column=CurrentPrice.updated_at_utc,
            ),
            _query_table_summary(
                session,
                model=PriceHistory,
                schema="msrp",
                label="MSRP Price History",
                domain="msrp",
                timestamp_column=PriceHistory.last_confirmed_at_utc,
            ),
            _query_table_summary(
                session,
                model=MatchOverride,
                schema="review",
                label="Match Overrides",
                domain="review",
                timestamp_column=MatchOverride.created_at_utc,
            ),
            _query_table_summary(
                session,
                model=ReviewCase,
                schema="review",
                label="Review Cases",
                domain="review",
                timestamp_column=ReviewCase.updated_at_utc,
            ),
            _query_table_summary(
                session,
                model=ReviewDecision,
                schema="review",
                label="Review Decisions",
                domain="review",
                timestamp_column=ReviewDecision.decided_at_utc,
            ),
        ]

        for source, model, stamp in [
            ("news", CountryNewsDigest, CountryNewsDigest.synced_at_utc),
            ("voc", VocSourceRun, VocSourceRun.collected_at_utc),
            ("engineering", ConfigImportBatch, ConfigImportBatch.created_at_utc),
            ("msrp", ScrapeBatch, ScrapeBatch.started_at_utc),
            ("msrp", MsrpObservation, MsrpObservation.observed_at_utc),
            ("msrp", FinanceObservation, FinanceObservation.observed_at_utc),
            ("review", ReviewDecision, ReviewDecision.decided_at_utc),
        ]:
            for day, count in _query_day_counts(
                session,
                model=model,
                timestamp_column=stamp,
                since_at=since_at,
            ):
                activity_by_day[day] += count
                activity_by_source[source] += count

        news_rows = session.execute(
            select(CountryNewsDigest)
            .order_by(CountryNewsDigest.synced_at_utc.desc())
            .limit(50)
        ).scalars().all()
        news_recent = [
            {
                "label": str(row.country_label or row.country_code),
                "value": int(row.article_count or 0),
                "updatedAt": row.synced_at_utc.astimezone(UTC).isoformat(),
            }
            for row in news_rows
        ]
        voc_rows = session.execute(
            select(VocSourceRun)
            .order_by(VocSourceRun.collected_at_utc.desc())
            .limit(50)
        ).scalars().all()
        voc_recent = [
            {
                "label": str(row.country_label or row.country_code),
                "value": (
                    f"{int(row.document_count or 0)} docs · "
                    f"{int(row.publish_ready_count or 0)} ready · "
                    f"{int(row.error_count or 0)} errors"
                ),
                "updatedAt": row.collected_at_utc.astimezone(UTC).isoformat(),
            }
            for row in voc_rows
        ]

        sources_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.sources"),
            0,
        )
        observation_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.observations"),
            0,
        )
        finance_observation_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.finance_observations"),
            0,
        )
        current_price_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.current_prices"),
            0,
        )
        price_history_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.price_history"),
            0,
        )
        review_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "review.review_cases"),
            0,
        )
        variant_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "engineering.config_variants"),
            0,
        )
        msrp_updated_at = None
        for item in table_items:
            if item["key"] in {
                "msrp.observations",
                "msrp.finance_observations",
                "msrp.current_prices",
                "msrp.price_history",
            }:
                msrp_updated_at = _latest_timestamp(
                    msrp_updated_at,
                    item["lastEventAt"],
                )
        observation_snapshots = int(
            session.execute(
                select(func.count())
                .select_from(MsrpObservation)
                .where(MsrpObservation.source_snapshot_path.is_not(None))
            ).scalar_one()
            or 0
        )
        current_price_snapshots = int(
            session.execute(
                select(func.count())
                .select_from(CurrentPrice)
                .where(CurrentPrice.source_snapshot_path.is_not(None))
            ).scalar_one()
            or 0
        )
        review_snapshots = int(
            session.execute(
                select(func.count())
                .select_from(ReviewCase)
                .where(ReviewCase.source_snapshot_path.is_not(None))
            ).scalar_one()
            or 0
        )
        recent_snapshot_items = _collect_recent_snapshot_items(session)

    domains = [
        {
            "key": "news",
            "label": "News Cache",
            "storage": "database",
            "status": "ready",
            "updatedAt": next(
                (item["lastEventAt"] for item in table_items if item["key"] == "ops.country_news_digests"),
                None,
            ),
            "summary": "国家新闻 digest / article 缓存，支持按国家生成摘要和事件回放。",
            "metrics": [
                _metric("Digests", next((item["rowCount"] for item in table_items if item["key"] == "ops.country_news_digests"), 0)),
                _metric("Articles", next((item["rowCount"] for item in table_items if item["key"] == "ops.country_news_articles"), 0)),
            ],
            "recentItems": news_recent,
        },
        {
            "key": "voc",
            "label": "VOC Staging",
            "storage": "database",
            "status": "ready",
            "updatedAt": next(
                (item["lastEventAt"] for item in table_items if item["key"] == "ops.voc_source_runs"),
                None,
            ),
            "summary": "VOC raw source runs 与文档 staging 表，持久化 raw 文本、auto review 与 publish gate。",
            "metrics": [
                _metric("Runs", next((item["rowCount"] for item in table_items if item["key"] == "ops.voc_source_runs"), 0)),
                _metric("Documents", next((item["rowCount"] for item in table_items if item["key"] == "ops.voc_raw_documents"), 0)),
            ],
            "recentItems": voc_recent,
        },
        {
            "key": "msrp",
            "label": "MSRP Core",
            "storage": "database",
            "status": "ready",
            "updatedAt": msrp_updated_at,
            "summary": "MSRP sources、批次、价格观测、finance observations、current prices 与 price history。",
            "metrics": [
                _metric("Sources", sources_count),
                _metric("Observations", observation_count),
                _metric("Finance observations", finance_observation_count),
                _metric("Current prices", current_price_count),
                _metric("Price history", price_history_count),
                _metric("Review cases", review_count),
            ],
            "recentItems": [],
        },
        {
            "key": "snapshots",
            "label": "Source Snapshots",
            "storage": "database",
            "status": (
                "ready"
                if observation_snapshots + current_price_snapshots + review_snapshots > 0
                else "inactive"
            ),
            "updatedAt": next(
                (
                    item.get("updatedAt")
                    for item in recent_snapshot_items
                    if item.get("updatedAt")
                ),
                None,
            ),
            "summary": "抓取快照路径覆盖，拆分查看 observation / current price / review 三个环节的最近快照。",
            "metrics": [
                _metric("Observation", observation_snapshots),
                _metric("Current price", current_price_snapshots),
                _metric("Review", review_snapshots),
                _metric("Recent paths", len(recent_snapshot_items)),
            ],
            "recentItems": [
                {
                    "label": f"{item['group']} · {item['label']}",
                    "value": item["value"],
                    "updatedAt": item.get("updatedAt"),
                }
                for item in recent_snapshot_items
            ],
        },
        {
            "key": "engineering",
            "label": "Engineering Config",
            "storage": "database",
            "status": "ready",
            "updatedAt": next(
                (item["lastEventAt"] for item in table_items if item["key"] == "engineering.config_import_batches"),
                None,
            ),
            "summary": "配置项目、导入批次与 variant 明细。",
            "metrics": [
                _metric("Projects", next((item["rowCount"] for item in table_items if item["key"] == "engineering.config_projects"), 0)),
                _metric("Import batches", next((item["rowCount"] for item in table_items if item["key"] == "engineering.config_import_batches"), 0)),
                _metric("Variants", variant_count),
            ],
            "recentItems": [],
        },
    ]
    return (health, table_items, dict(activity_by_day), dict(activity_by_source), domains)


def _build_activity_payload(
    *,
    database_day_counts: dict[date, int],
    activity_by_source: dict[str, int],
    database_connected: bool,
) -> dict[str, Any]:
    today = _utc_now().date()
    since_day = today - timedelta(days=ACTIVITY_WINDOW_DAYS - 1)
    day_counts: dict[date, int] = defaultdict(int, database_day_counts)
    source_counts: dict[str, int] = defaultdict(int, activity_by_source)

    jobs_payload = list_jato_monthly_update_jobs(limit=50)
    job_items = jobs_payload.get("items") if isinstance(jobs_payload, dict) else []
    job_stamps = []
    if isinstance(job_items, list):
        for item in job_items:
            value = item.get("createdAt")
            if isinstance(value, str):
                try:
                    job_stamps.append(datetime.fromisoformat(value))
                except ValueError:
                    continue
    _add_activity_counts(
        day_counts,
        source_counts,
        source="jato",
        stamps=job_stamps,
        since_day=since_day,
    )

    wiki_manifest_stat = _safe_stat(WIKI_MANIFEST_PATH)
    if wiki_manifest_stat.get("updatedAt"):
        _add_activity_counts(
            day_counts,
            source_counts,
            source="wiki",
            stamps=[datetime.fromisoformat(str(wiki_manifest_stat["updatedAt"]))],
            since_day=since_day,
        )

    parquet_stat = _safe_stat(PARQUET_PATH)
    if parquet_stat.get("updatedAt"):
        _add_activity_counts(
            day_counts,
            source_counts,
            source="jato",
            stamps=[datetime.fromisoformat(str(parquet_stat["updatedAt"]))],
            since_day=since_day,
        )

    days: list[dict[str, Any]] = []
    cursor = since_day
    max_count = 0
    while cursor <= today:
        count = int(day_counts.get(cursor, 0))
        max_count = max(max_count, count)
        days.append(
            {
                "date": cursor.isoformat(),
                "count": count,
            }
        )
        cursor += timedelta(days=1)

    for item in days:
        count = int(item["count"])
        if max_count <= 0 or count <= 0:
            level = 0
        else:
            level = min(4, max(1, int(round((count / max_count) * 4))))
        item["level"] = level

    return {
        "days": days,
        "maxCount": max_count,
        "totalCount": sum(int(item["count"]) for item in days),
        "rangeStart": since_day.isoformat(),
        "rangeEnd": today.isoformat(),
        "sourceCounts": [
            {"label": label, "count": count}
            for label, count in sorted(source_counts.items())
        ],
        "databaseConnected": database_connected,
    }


def _build_airflow_domain(status: dict[str, Any]) -> dict[str, Any]:
    running_services = int(status.get("runningServices") or 0)
    total_services = int(status.get("totalServices") or 0)
    available = bool(status.get("available"))
    if not available:
        domain_status = "blocked"
    elif bool(status.get("running")):
        domain_status = "ready"
    elif running_services > 0:
        domain_status = "warning"
    else:
        domain_status = "inactive"

    services = status.get("services")
    service_items = services if isinstance(services, list) else []
    return {
        "key": "airflow",
        "label": "Local Airflow",
        "storage": "local docker",
        "status": domain_status,
        "updatedAt": status.get("updatedAt"),
        "summary": str(status.get("detail") or ""),
        "metrics": [
            _metric("Docker", "available" if available else "missing"),
            _metric("Services", f"{running_services}/{total_services}"),
            _metric(
                "UI",
                "online" if status.get("actions", {}).get("canOpenUi") else "offline",
            ),
        ],
        "recentItems": [
            {
                "label": item.get("service") or "service",
                "value": " · ".join(
                    part
                    for part in (
                        str(item.get("status") or "").strip(),
                        str(item.get("health") or "").strip(),
                    )
                    if part
                )
                or str(item.get("state") or "unknown"),
                "updatedAt": status.get("updatedAt"),
            }
            for item in service_items
        ],
    }


def read_data_management_overview() -> dict[str, Any]:
    file_inventory = _build_file_inventory()
    jato_domain = _build_jato_domain(file_inventory)
    assistant_domain = _build_country_assistant_domain()
    wiki_domain = _build_wiki_domain(file_inventory)
    news_raw_domain = _build_news_raw_domain()
    voc_raw_domain = _build_voc_raw_domain()
    airflow_status = read_airflow_ops_status()
    airflow_domain = _build_airflow_domain(airflow_status)
    (
        database_health,
        database_tables,
        database_day_counts,
        activity_by_source,
        database_domains,
    ) = _build_database_payload()

    domains = [
        jato_domain,
        assistant_domain,
        wiki_domain,
        news_raw_domain,
        voc_raw_domain,
        airflow_domain,
        *database_domains,
    ]
    return {
        "generatedAt": _utc_now().isoformat(),
        "database": database_health,
        "domains": domains,
        "airflow": airflow_status,
        "fileInventory": file_inventory,
        "databaseTables": database_tables,
        "activity": _build_activity_payload(
            database_day_counts=database_day_counts,
            activity_by_source=activity_by_source,
            database_connected=bool(database_health.get("connected")),
        ),
    }
