#!/usr/bin/env python3
"""Sync an official engineering configuration source into the warehouse.

The script makes the source-table path repeatable:

1. register an official source snapshot in ``ops.import_batches``;
2. build the workbook digest with the same extractor used by the API;
3. create draft trims/features/values for selected digest compare groups.

It intentionally reuses the existing engineering-config API helpers and
repository layer instead of introducing a parallel import model.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = REPO_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))
HERMES_SCRIPT_DIR = REPO_ROOT / "03_Scripts" / "hermes"
if str(HERMES_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(HERMES_SCRIPT_DIR))

from app.api.routes import engineering_config as config_api  # noqa: E402
from app.core.security import UserContext  # noqa: E402
from app.db.models import ImportBatch  # noqa: E402
from app.db.session import get_session_factory  # noqa: E402

try:
    from pipeline_status_writer import write_pipeline_status  # noqa: E402
except ImportError:  # pragma: no cover - optional for isolated unit imports.
    write_pipeline_status = None  # type: ignore[assignment]


SCHEMA_VERSION = "engineering_config_source_sync_v1"
PIPELINE_ID = "engineering_config_source_sync"
DEFAULT_SOURCE_FILE = (
    REPO_ROOT
    / "02_Config_MetaData"
    / config_api.DEFAULT_LOCAL_CONFIG_WORKBOOK
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _source_payload(batch: object, *, status: str) -> dict[str, Any]:
    return {
        "sourceId": str(getattr(batch, "import_batch_id", "")),
        "uploadStatus": status,
        "sourceFileName": getattr(batch, "source_file_name", None),
        "sourceFilePath": getattr(batch, "source_file_path", None),
        "sourceFileHash": getattr(batch, "source_file_hash", None),
        "domain": getattr(batch, "domain", None),
    }


def _digest_groups(digest: dict[str, Any]) -> list[dict[str, Any]]:
    groups = digest.get("compareGroups")
    if not isinstance(groups, list):
        return []
    return [item for item in groups if isinstance(item, dict)]


def _is_importable_group(group: dict[str, Any]) -> bool:
    trims = group.get("trims")
    rows = group.get("rows")
    return isinstance(trims, list) and len(trims) >= 2 and isinstance(rows, list) and bool(rows)


def _compact_name(parts: Sequence[str]) -> str:
    result: list[str] = []
    for part in parts:
        cleaned = part.strip()
        if cleaned and cleaned not in result:
            result.append(cleaned)
    return " / ".join(result)


def _trim_full_name(trim: dict[str, Any], group: dict[str, Any]) -> str:
    full_name = str(trim.get("fullTrimName") or "").strip()
    if full_name:
        return full_name
    model_name = str(
        trim.get("modelName")
        or group.get("modelName")
        or "Unknown model"
    ).strip()
    trim_name = str(
        trim.get("trimName")
        or trim.get("fullTrimName")
        or "Unnamed trim"
    ).strip()
    return _compact_name([model_name, trim_name]) or trim_name or model_name


def _group_summary(group: dict[str, Any], *, index: int) -> dict[str, Any]:
    trims = group.get("trims") if isinstance(group.get("trims"), list) else []
    rows = group.get("rows") if isinstance(group.get("rows"), list) else []
    return {
        "index": index,
        "groupId": group.get("groupId"),
        "modelName": group.get("modelName"),
        "trimCount": len(trims),
        "featureCount": len(rows),
        "importable": _is_importable_group(group),
    }


def _select_groups(
    digest: dict[str, Any],
    *,
    group_id: str | None,
    group_index: int | None,
    all_groups: bool,
    limit_groups: int,
) -> list[tuple[int, dict[str, Any]]]:
    groups = _digest_groups(digest)
    if group_id:
        selected = [
            (index, group)
            for index, group in enumerate(groups)
            if str(group.get("groupId") or "") == group_id
        ]
        if not selected:
            raise ValueError(f"Digest group not found: {group_id}")
        return selected

    if group_index is not None:
        if group_index < 0 or group_index >= len(groups):
            raise ValueError(f"Digest group index out of range: {group_index}")
        return [(group_index, groups[group_index])]

    importable = [
        (index, group)
        for index, group in enumerate(groups)
        if _is_importable_group(group)
    ]
    if all_groups:
        return importable[:max(1, limit_groups)]
    return importable[:1]


def _normalised_context(args: argparse.Namespace) -> dict[str, Any]:
    return config_api._normalise_related_context({  # noqa: SLF001
        "relatedContext": {
            "brand": args.brand,
            "model": args.model,
            "market": args.market,
            "country": args.country,
            "modelYear": args.model_year,
            "contextType": args.context_type,
        }
    })


def _import_result_int(import_item: dict[str, Any], key: str) -> int:
    result = import_item.get("result")
    if not isinstance(result, dict):
        return 0
    try:
        return int(result.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _pipeline_status_payload(
    report: dict[str, Any],
    *,
    started_at: datetime,
    finished_at: datetime,
    exit_code: int,
) -> dict[str, Any] | None:
    if write_pipeline_status is None:
        return None

    report_status = str(report.get("status") or "failed")
    source = report.get("source") if isinstance(report.get("source"), dict) else {}
    selected_groups = [
        item for item in report.get("selectedGroups") or []
        if isinstance(item, dict)
    ]
    imports = [
        item for item in report.get("imports") or []
        if isinstance(item, dict)
    ]
    draft_created_count = sum(1 for item in imports if item.get("status") == "draft_created")
    skipped_existing_count = sum(1 for item in imports if item.get("status") == "skipped_existing_trims")
    trim_count = sum(_import_result_int(item, "trimCount") for item in imports)
    value_record_count = sum(_import_result_int(item, "valueRecordCount") for item in imports)
    records_processed = value_record_count or trim_count or len(imports) or len(selected_groups)
    pipeline_status = "success" if report_status in {"passed", "dry_run"} else "failed"
    failed_count = 0 if pipeline_status == "success" else 1
    message = (
        f"Engineering config source sync {report_status}; "
        f"groups={len(selected_groups)}, drafts={draft_created_count}, values={value_record_count}."
    )
    source_file_path = str(source.get("sourceFilePath") or "")
    artifact_refs = [source_file_path] if source_file_path else []

    return write_pipeline_status(
        pipeline_id=PIPELINE_ID,
        status=pipeline_status,
        started_at=_utc_iso(started_at),
        finished_at=_utc_iso(finished_at),
        exit_code=exit_code,
        duration_seconds=int(max(0, (finished_at - started_at).total_seconds())),
        records_processed=records_processed,
        failed_count=failed_count,
        warning_count=skipped_existing_count,
        artifact_refs=artifact_refs,
        source="03_Scripts/engineering_config_source_sync.py",
        message=message,
        extra={
            "schemaVersion": SCHEMA_VERSION,
            "reportStatus": report_status,
            "sourceFileName": source.get("sourceFileName"),
            "sourceFilePath": source.get("sourceFilePath"),
            "sourceFileHash": source.get("sourceFileHash"),
            "sourceUploadStatus": source.get("uploadStatus"),
            "selectedGroupCount": len(selected_groups),
            "importCount": len(imports),
            "draftCreatedCount": draft_created_count,
            "skippedExistingTrimCount": skipped_existing_count,
            "trimCount": trim_count,
            "valueRecordCount": value_record_count,
            "dryRun": report_status == "dry_run",
            "error": report.get("error"),
        },
        repo_root=REPO_ROOT,
    )


def _register_source_snapshot(
    session: object,
    *,
    source_path: Path,
    related_context: dict[str, Any],
    user: UserContext,
) -> tuple[object, str]:
    safe_name = config_api._safe_upload_file_name(source_path.name)  # noqa: SLF001
    if config_api._file_extension(safe_name) not in config_api.SOURCE_UPLOAD_EXTENSIONS:  # noqa: SLF001
        raise ValueError(f"Unsupported source file type: {safe_name}")
    config_api._validate_source_file_content(source_path, safe_name)  # noqa: SLF001
    source_file_hash = config_api._sha256_for_path(source_path)  # noqa: SLF001
    existing_batch = config_api.repo.get_import_batch_by_hash(
        session,
        config_api.SOURCE_IMPORT_DOMAIN,
        source_file_hash,
    )
    if existing_batch is not None:
        link = config_api._create_source_context_link(  # noqa: SLF001
            session,
            existing_batch,
            related_context=related_context,
            user=user,
        )
        if link is not None:
            session.flush()
            session.commit()
        return existing_batch, "duplicate"

    now = _utc_now()
    batch = ImportBatch(
        domain=config_api.SOURCE_IMPORT_DOMAIN,
        source_file_name=safe_name,
        source_file_path=str(source_path),
        source_file_hash=source_file_hash,
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by=user.name,
        started_at_utc=now,
        finished_at_utc=now,
    )
    config_api.repo.add_import_batch(session, batch)
    session.flush()
    link = config_api._create_source_context_link(  # noqa: SLF001
        session,
        batch,
        related_context=related_context,
        user=user,
    )
    if link is not None:
        session.flush()
    session.commit()
    return batch, "registered"


def _all_group_trims_exist(session: object, group: dict[str, Any]) -> bool:
    trims = group.get("trims")
    if not isinstance(trims, list) or not trims:
        return False
    for trim in trims:
        if not isinstance(trim, dict):
            return False
        full_name = _trim_full_name(trim, group)
        if config_api.repo.get_vehicle_trim_by_full_name(session, full_name) is None:
            return False
    return True


def run_sync(
    *,
    session: object,
    source_file: str | Path,
    group_id: str | None = None,
    group_index: int | None = None,
    all_groups: bool = False,
    limit_groups: int = 1,
    related_context: dict[str, Any] | None = None,
    user_name: str = "engineering-config-source-sync",
    dry_run: bool = False,
    force_draft: bool = False,
) -> dict[str, Any]:
    source_path = Path(source_file).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    digest = config_api.build_source_digest(source_path, source_path.name)
    if not digest or digest.get("status") == "failed":
        raise ValueError("Source digest is not ready")
    selected_groups = _select_groups(
        digest,
        group_id=group_id,
        group_index=group_index,
        all_groups=all_groups,
        limit_groups=limit_groups,
    )
    selected_summary = [
        _group_summary(group, index=index)
        for index, group in selected_groups
    ]
    for item in selected_summary:
        if not item["importable"]:
            raise ValueError(f"Digest group is not importable: {item['groupId']}")

    if dry_run:
        return {
            "schemaVersion": SCHEMA_VERSION,
            "status": "dry_run",
            "source": {
                "sourceFileName": source_path.name,
                "sourceFilePath": str(source_path),
            },
            "digestSummary": digest.get("summary", {}),
            "selectedGroups": selected_summary,
            "imports": [],
        }

    user = UserContext(role="admin", name=user_name)
    source_batch, source_status = _register_source_snapshot(
        session,
        source_path=source_path,
        related_context=related_context or {},
        user=user,
    )

    imports: list[dict[str, Any]] = []
    for index, group in selected_groups:
        group_id_value = str(group.get("groupId") or "")
        if (
            source_status == "duplicate"
            and not force_draft
            and _all_group_trims_exist(session, group)
        ):
            imports.append({
                **_group_summary(group, index=index),
                "status": "skipped_existing_trims",
            })
            continue

        result = config_api.create_draft_from_source_digest_group(
            source_batch.import_batch_id,
            group_id_value,
            None,
            session,
            user,
        )
        imports.append({
            **_group_summary(group, index=index),
            "status": "draft_created",
            "result": result,
        })

    return {
        "schemaVersion": SCHEMA_VERSION,
        "status": "passed",
        "source": _source_payload(source_batch, status=source_status),
        "digestSummary": digest.get("summary", {}),
        "selectedGroups": selected_summary,
        "imports": imports,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Register an official config source and import digest groups.",
    )
    parser.add_argument("--source-file", default=str(DEFAULT_SOURCE_FILE))
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--group-index", type=int, default=None)
    parser.add_argument("--all-groups", action="store_true")
    parser.add_argument("--limit-groups", type=int, default=1)
    parser.add_argument("--brand", default="Omoda/Jaecoo")
    parser.add_argument("--model", default="EU config resource table")
    parser.add_argument("--market", default="EU")
    parser.add_argument("--country", default="EU")
    parser.add_argument("--model-year", default=None)
    parser.add_argument("--context-type", default="source_snapshot")
    parser.add_argument("--user-name", default="engineering-config-source-sync")
    parser.add_argument("--force-draft", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    started_at = _utc_now()
    session = get_session_factory()()
    try:
        report = run_sync(
            session=session,
            source_file=args.source_file,
            group_id=args.group_id,
            group_index=args.group_index,
            all_groups=args.all_groups,
            limit_groups=max(1, args.limit_groups),
            related_context=_normalised_context(args),
            user_name=args.user_name,
            dry_run=args.dry_run,
            force_draft=args.force_draft,
        )
    except Exception as exc:
        session.rollback()
        failed_report = {
            "schemaVersion": SCHEMA_VERSION,
            "status": "failed",
            "error": str(exc),
            "source": {
                "sourceFileName": Path(args.source_file).name,
                "sourceFilePath": str(Path(args.source_file).expanduser()),
            },
            "selectedGroups": [],
            "imports": [],
        }
        _pipeline_status_payload(
            failed_report,
            started_at=started_at,
            finished_at=_utc_now(),
            exit_code=1,
        )
        print(json.dumps(failed_report, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1
    finally:
        session.close()

    _pipeline_status_payload(
        report,
        started_at=started_at,
        finished_at=_utc_now(),
        exit_code=0,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
