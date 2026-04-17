from __future__ import annotations

import json
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
    ImportBatch,
    MatchOverride,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
    ScrapeBatch,
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
BASELINE_ROOT = RAW_DATA_ROOT / "baseline"
PATCH_ROOT = RAW_DATA_ROOT / "patches"
ARCHIVE_ROOT = RAW_DATA_ROOT / "historyDataArchive"
WIKI_MANIFEST_PATH = get_local_wiki_manifest_path()
WIKI_DB_ROOT = WIKI_MANIFEST_PATH.parent
ACTIVITY_WINDOW_DAYS = 84

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
    ]
    return inventory


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
        "recentItems": jobs[:5] if isinstance(jobs, list) else [],
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
                for item in countries[:6]
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
            ("engineering", ConfigImportBatch, ConfigImportBatch.created_at_utc),
            ("msrp", ScrapeBatch, ScrapeBatch.started_at_utc),
            ("msrp", MsrpObservation, MsrpObservation.observed_at_utc),
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
            .limit(6)
        ).scalars().all()
        news_recent = [
            {
                "label": str(row.country_label or row.country_code),
                "value": int(row.article_count or 0),
                "updatedAt": row.synced_at_utc.astimezone(UTC).isoformat(),
            }
            for row in news_rows
        ]

        sources_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.sources"),
            0,
        )
        observation_count = next(
            (item["rowCount"] for item in table_items if item["key"] == "msrp.observations"),
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
            "key": "msrp",
            "label": "MSRP Core",
            "storage": "database",
            "status": "ready",
            "updatedAt": next(
                (item["lastEventAt"] for item in table_items if item["key"] == "msrp.observations"),
                None,
            ),
            "summary": "MSRP sources、批次、观测、current prices 与 snapshot 路径覆盖。",
            "metrics": [
                _metric("Sources", sources_count),
                _metric("Observations", observation_count),
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


def read_data_management_overview() -> dict[str, Any]:
    file_inventory = _build_file_inventory()
    jato_domain = _build_jato_domain(file_inventory)
    assistant_domain = _build_country_assistant_domain()
    wiki_domain = _build_wiki_domain(file_inventory)
    (
        database_health,
        database_tables,
        database_day_counts,
        activity_by_source,
        database_domains,
    ) = _build_database_payload()

    domains = [jato_domain, assistant_domain, wiki_domain, *database_domains]
    return {
        "generatedAt": _utc_now().isoformat(),
        "database": database_health,
        "domains": domains,
        "fileInventory": file_inventory,
        "databaseTables": database_tables,
        "activity": _build_activity_payload(
            database_day_counts=database_day_counts,
            activity_by_source=activity_by_source,
            database_connected=bool(database_health.get("connected")),
        ),
    }
