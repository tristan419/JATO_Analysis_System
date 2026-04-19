import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.config import ENGINEERING_IMPORT_ROOT, PROJECT_ROOT
from app.db.models import (
    ConfigImportBatch,
    ConfigProject,
    ConfigVariant,
    ImportBatch,
)
from app.infra import engineering_repository as repo
from app.services.engineering_normalization_service import (
    rebuild_project_normalized_config,
)


DEFAULT_ENGINEERING_SHEET = "Data Export"
ENGINEERING_IMPORT_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
BATTERY_RANGE_CANDIDATES = (
    "Battery range",
    "Battery Range",
    "battery range",
    "WLTP range",
    "EV range",
    "续航里程",
    "电池续航",
)
BATTERY_CAPACITY_CANDIDATES = (
    "Battery kwh",
    "Battery kWh",
    "Useable battery kilowatt hour (kWh)",
    "Battery capacity",
    "Battery Capacity",
    "电池容量",
)
MSRP_CANDIDATES = (
    "MSRP规整",
    "MSRP including delivery charge",
    "MSRP",
    "MSRP区间",
    "Target MSRP",
)
COLUMN_CANDIDATES: dict[str, tuple[str, ...]] = {
    "external_row_key": (
        "config id",
        "配置id",
        "配置编码",
        "row id",
        "rowid",
        "id",
    ),
    "brand": ("make", "brand", "品牌"),
    "model": ("model", "车型"),
    "market_country": (
        "国家",
        "country",
        "market country",
        "market",
        "country/region",
    ),
    "trim_name": (
        "trim name",
        "trim",
        "版型名",
        "版型",
        "version name",
        "version",
    ),
    "version_name": (
        "version name",
        "version",
        "版本名",
        "版本",
        "trim name",
        "trim",
    ),
    "powertrain": ("动总规整", "powertrain", "动力", "动力总成"),
    "body_style": ("body style", "bodystyle", "车身形式", "车身类型"),
    "drive_type": ("drive type", "drivetype", "drive", "驱动形式"),
    "battery_kwh": BATTERY_CAPACITY_CANDIDATES,
    "range_km": BATTERY_RANGE_CANDIDATES,
    "target_msrp": MSRP_CANDIDATES,
}


def _normalize_column_token(value: str) -> str:
    return re.sub(r"[\s_\-()/]+", "", value.strip().casefold())


def _resolve_candidate_column(
    columns: list[str],
    candidates: tuple[str, ...],
) -> str | None:
    normalized_columns = {
        _normalize_column_token(column): column for column in columns
    }
    for candidate in candidates:
        resolved = normalized_columns.get(_normalize_column_token(candidate))
        if resolved:
            return resolved
    return None


def _resolve_column_mapping(columns: list[str]) -> dict[str, str | None]:
    return {
        field_name: _resolve_candidate_column(columns, candidates)
        for field_name, candidates in COLUMN_CANDIDATES.items()
    }


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(column).strip() for column in result.columns]

    object_columns = result.select_dtypes(include=["object"]).columns
    for column in object_columns:
        result[column] = result[column].astype("string")

    return result


def _read_excel_with_fallback(
    source_file: Path,
    sheet_name: str,
) -> pd.DataFrame:
    try:
        return pd.read_excel(
            source_file,
            sheet_name=sheet_name,
            engine="calamine",
        )
    except Exception:
        try:
            return pd.read_excel(
                source_file,
                sheet_name=sheet_name,
            )
        except Exception as exc:
            raise RuntimeError(
                "读取工程配置 Excel 失败：calamine 与默认引擎均不可用。"
            ) from exc


def _coerce_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if pd.isna(value):
            return None
        return float(value)
    text = _coerce_text(value)
    if not text:
        return None
    normalized = re.sub(r"[^0-9.\-]", "", text)
    if not normalized or normalized in {"-", ".", "-."}:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _json_safe_value(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return value
    if pd.isna(value):
        return None
    return str(value)


def _build_row_hash(payload: dict[str, object]) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _hash_file(source_file: Path) -> str:
    digest = hashlib.sha256()
    with source_file.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_import_root() -> Path:
    root = ENGINEERING_IMPORT_ROOT
    if not root.is_absolute():
        root = (PROJECT_ROOT / root).resolve()
    return root


def _resolve_import_file_path(source_file_path: str) -> Path:
    candidate = Path(source_file_path)
    if not candidate.is_absolute():
        candidate = (PROJECT_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()

    allowed_root = _resolve_import_root()
    try:
        candidate.relative_to(allowed_root)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                "Import file must stay under the configured "
                f"engineering import root: {allowed_root}"
            ),
        ) from exc

    if not candidate.exists():
        raise HTTPException(
            status_code=400,
            detail=f"Import file does not exist: {candidate}",
        )
    if candidate.suffix.lower() not in ENGINEERING_IMPORT_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail="Engineering import only supports Excel files.",
        )
    return candidate


def _to_project_relative_path(source_file: Path) -> str:
    try:
        return str(source_file.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(source_file)


def _extract_row_text(
    row: dict[str, object],
    mapping: dict[str, str | None],
    field_name: str,
) -> str | None:
    source_column = mapping.get(field_name)
    if not source_column:
        return None
    return _coerce_text(row.get(source_column))


def _extract_row_float(
    row: dict[str, object],
    mapping: dict[str, str | None],
    field_name: str,
) -> float | None:
    source_column = mapping.get(field_name)
    if not source_column:
        return None
    return _coerce_float(row.get(source_column))


def _build_variants_for_import(
    project: ConfigProject,
    config_import_batch_id: UUID,
    df: pd.DataFrame,
    stored_source_file_path: str,
    column_mapping: dict[str, str | None],
) -> tuple[list[ConfigVariant], int, list[str]]:
    used_columns = {
        source_column
        for source_column in column_mapping.values()
        if source_column is not None
    }
    variants: list[ConfigVariant] = []
    skipped_rows = 0
    warnings: list[str] = []
    seen_hashes: set[str] = set()

    for row_number, row in enumerate(df.to_dict(orient="records"), start=2):
        brand = (
            _extract_row_text(row, column_mapping, "brand")
            or project.brand
        )
        model = (
            _extract_row_text(row, column_mapping, "model")
            or project.model
        )
        market_country = (
            _extract_row_text(row, column_mapping, "market_country")
            or project.market_country
        )
        version_name = _extract_row_text(
            row,
            column_mapping,
            "version_name",
        )
        trim_name = (
            _extract_row_text(row, column_mapping, "trim_name")
            or version_name
        )
        version_name = version_name or trim_name

        if not trim_name:
            skipped_rows += 1
            warnings.append(
                f"Row {row_number}: trim/version value is empty, skipped."
            )
            continue

        attributes: dict[str, object] = {}
        for column_name, raw_value in row.items():
            if column_name in used_columns:
                continue
            safe_value = _json_safe_value(raw_value)
            if safe_value is not None:
                attributes[column_name] = safe_value

        external_row_key = (
            _extract_row_text(row, column_mapping, "external_row_key")
            or str(row_number)
        )
        variant_payload = {
            "brand": brand,
            "model": model,
            "trim_name": trim_name,
            "version_name": version_name,
            "market_country": market_country,
            "powertrain": _extract_row_text(
                row,
                column_mapping,
                "powertrain",
            ),
            "body_style": _extract_row_text(
                row,
                column_mapping,
                "body_style",
            ),
            "drive_type": _extract_row_text(
                row,
                column_mapping,
                "drive_type",
            ),
            "battery_kwh": _extract_row_float(
                row,
                column_mapping,
                "battery_kwh",
            ),
            "range_km": _extract_row_float(
                row,
                column_mapping,
                "range_km",
            ),
            "target_msrp": _extract_row_float(
                row,
                column_mapping,
                "target_msrp",
            ),
            "attributes_json": attributes or None,
        }
        row_hash = _build_row_hash(
            {
                "projectId": str(project.project_id),
                "externalRowKey": external_row_key,
                **variant_payload,
            }
        )
        if row_hash in seen_hashes:
            skipped_rows += 1
            warnings.append(
                f"Row {row_number}: duplicate normalized payload, skipped."
            )
            continue
        seen_hashes.add(row_hash)

        variants.append(
            ConfigVariant(
                project_id=project.project_id,
                config_import_batch_id=config_import_batch_id,
                external_row_key=external_row_key,
                brand=brand,
                model=model,
                trim_name=trim_name,
                version_name=version_name,
                market_country=market_country,
                powertrain=variant_payload["powertrain"],
                body_style=variant_payload["body_style"],
                drive_type=variant_payload["drive_type"],
                battery_kwh=variant_payload["battery_kwh"],
                range_km=variant_payload["range_km"],
                target_msrp=variant_payload["target_msrp"],
                is_active=True,
                row_hash=row_hash,
                attributes_json=variant_payload["attributes_json"],
                source_file_path=stored_source_file_path,
            )
        )

    return variants, skipped_rows, warnings


def _serialize_import_summary(summary: dict[str, object]) -> str:
    return json.dumps(
        summary,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _parse_import_summary(notes: str | None) -> dict[str, object] | None:
    if not notes:
        return None
    try:
        payload = json.loads(notes)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _build_import_summary(
    *,
    user_notes: str | None,
    source_file_name: str,
    source_file_path: str,
    sheet_name: str,
    status: str,
    raw_rows: int,
    inserted_rows: int,
    skipped_rows: int,
    deactivated_rows: int,
    column_mapping: dict[str, str],
    warnings: list[str],
    normalization_summary: dict[str, object] | None = None,
    failure_detail: str | None = None,
) -> dict[str, object]:
    return {
        "userNotes": user_notes,
        "sourceFileName": source_file_name,
        "sourceFilePath": source_file_path,
        "sheetName": sheet_name,
        "status": status,
        "rawRows": raw_rows,
        "insertedRows": inserted_rows,
        "skippedRows": skipped_rows,
        "deactivatedRows": deactivated_rows,
        "warningCount": len(warnings),
        "warnings": warnings[:20],
        "columnMapping": column_mapping,
        "normalization": normalization_summary,
        "failureDetail": failure_detail,
    }


def _import_batch_payload(import_batch: ImportBatch) -> dict[str, object]:
    return {
        "importBatchId": str(import_batch.import_batch_id),
        "domain": import_batch.domain,
        "sourceFileName": import_batch.source_file_name,
        "sourceFilePath": import_batch.source_file_path,
        "sourceFileHash": import_batch.source_file_hash,
        "importStatus": import_batch.import_status,
        "rowCount": import_batch.row_count,
        "errorCount": import_batch.error_count,
        "triggeredBy": import_batch.triggered_by,
        "startedAtUtc": (
            import_batch.started_at_utc.isoformat()
            if import_batch.started_at_utc is not None
            else None
        ),
        "finishedAtUtc": (
            import_batch.finished_at_utc.isoformat()
            if import_batch.finished_at_utc is not None
            else None
        ),
        "createdAtUtc": import_batch.created_at_utc.isoformat(),
    }


def _project_payload(project: ConfigProject) -> dict[str, object]:
    return {
        "projectId": str(project.project_id),
        "projectCode": project.project_code,
        "brand": project.brand,
        "model": project.model,
        "marketCountry": project.market_country,
        "displayName": project.display_name,
        "status": project.status,
        "createdAtUtc": project.created_at_utc.isoformat(),
        "updatedAtUtc": project.updated_at_utc.isoformat(),
    }


def _config_import_batch_payload(
    batch: ConfigImportBatch,
    include_summary: bool = False,
) -> dict[str, object]:
    summary = _parse_import_summary(batch.notes)
    return {
        "configImportBatchId": str(batch.config_import_batch_id),
        "projectId": str(batch.project_id),
        "importBatchId": str(batch.import_batch_id),
        "sourceSchemaVersion": batch.source_schema_version,
        "replaceMode": batch.replace_mode,
        "importStatus": batch.import_status,
        "rowCount": batch.row_count,
        "validFromDate": (
            batch.valid_from_date.isoformat()
            if batch.valid_from_date is not None
            else None
        ),
        "notes": summary.get("userNotes") if summary else batch.notes,
        "warningCount": summary.get("warningCount") if summary else None,
        "failureDetail": summary.get("failureDetail") if summary else None,
        "summary": summary if include_summary else None,
        "createdAtUtc": batch.created_at_utc.isoformat(),
    }


def _config_variant_payload(variant: ConfigVariant) -> dict[str, object]:
    return {
        "variantId": str(variant.variant_id),
        "projectId": str(variant.project_id),
        "configImportBatchId": str(variant.config_import_batch_id),
        "externalRowKey": variant.external_row_key,
        "brand": variant.brand,
        "model": variant.model,
        "trimName": variant.trim_name,
        "versionName": variant.version_name,
        "marketCountry": variant.market_country,
        "powertrain": variant.powertrain,
        "bodyStyle": variant.body_style,
        "driveType": variant.drive_type,
        "batteryKwh": (
            float(variant.battery_kwh)
            if variant.battery_kwh is not None
            else None
        ),
        "rangeKm": (
            float(variant.range_km) if variant.range_km is not None else None
        ),
        "targetMsrp": (
            float(variant.target_msrp)
            if variant.target_msrp is not None
            else None
        ),
        "isActive": variant.is_active,
        "rowHash": variant.row_hash,
        "attributes": variant.attributes_json,
        "sourceFilePath": variant.source_file_path,
        "createdAtUtc": variant.created_at_utc.isoformat(),
        "updatedAtUtc": variant.updated_at_utc.isoformat(),
    }


def _commit_or_conflict(session: Session, detail: str) -> None:
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=detail) from exc


def list_config_projects(
    session: Session,
    status: str | None,
    brand: str | None,
    market_country: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_projects(session, status, brand, market_country, limit)
    return {
        "rows": len(items),
        "items": [_project_payload(item) for item in items],
    }


def create_config_project(session: Session, data: dict) -> dict[str, object]:
    project = ConfigProject(**data)
    repo.add_project(session, project)
    _commit_or_conflict(session, "Project code already exists")
    session.refresh(project)
    return _project_payload(project)


def update_config_project(
    session: Session,
    project_id: str,
    data: dict,
) -> dict[str, object] | None:
    project = repo.get_project(session, UUID(project_id))
    if project is None:
        return None
    for key, value in data.items():
        if value is not None:
            setattr(project, key, value)
    project.updated_at_utc = datetime.now(timezone.utc)
    _commit_or_conflict(session, "Project code already exists")
    session.refresh(project)
    return _project_payload(project)


def archive_config_project(
    session: Session,
    project_id: str,
) -> dict[str, object] | None:
    project = repo.get_project(session, UUID(project_id))
    if project is None:
        return None
    project.status = "archived"
    project.updated_at_utc = datetime.now(timezone.utc)
    repo.deactivate_project_variants(session, project.project_id)
    _commit_or_conflict(session, "Project code already exists")
    session.refresh(project)
    return _project_payload(project)


def list_config_import_batches(
    session: Session,
    project_id: UUID | None,
    import_status: str | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_config_import_batches(
        session,
        project_id,
        import_status,
        limit,
    )
    return {
        "rows": len(items),
        "items": [_config_import_batch_payload(item) for item in items],
    }


def list_config_variants(
    session: Session,
    project_id: UUID | None,
    config_import_batch_id: UUID | None,
    model: str | None,
    market_country: str | None,
    is_active: bool | None,
    limit: int,
) -> dict[str, object]:
    items = repo.list_config_variants(
        session,
        project_id,
        config_import_batch_id,
        model,
        market_country,
        is_active,
        limit,
    )
    return {
        "rows": len(items),
        "items": [_config_variant_payload(item) for item in items],
    }


def get_config_import_batch_detail(
    session: Session,
    config_import_batch_id: str,
    sample_limit: int,
) -> dict[str, object] | None:
    batch = repo.get_config_import_batch(
        session,
        UUID(config_import_batch_id),
    )
    if batch is None:
        return None

    import_batch = repo.get_import_batch(session, batch.import_batch_id)
    sample_variants = repo.list_config_variants(
        session,
        batch.project_id,
        batch.config_import_batch_id,
        None,
        None,
        None,
        max(1, min(int(sample_limit), 100)),
    )
    return {
        "configImportBatch": _config_import_batch_payload(
            batch,
            include_summary=True,
        ),
        "opsImportBatch": (
            _import_batch_payload(import_batch)
            if import_batch is not None
            else None
        ),
        "sampleVariants": [
            _config_variant_payload(item) for item in sample_variants
        ],
        "sampleRows": len(sample_variants),
    }


def get_config_import_batch_page_data(
    session: Session,
    config_import_batch_id: str,
    sample_limit: int,
) -> dict[str, object] | None:
    detail = get_config_import_batch_detail(
        session,
        config_import_batch_id,
        sample_limit,
    )
    if detail is None:
        return None

    batch = detail["configImportBatch"]
    ops_batch = detail["opsImportBatch"]
    summary = batch.get("summary") or {}
    warning_items = list(summary.get("warnings") or [])
    column_mapping = dict(summary.get("columnMapping") or {})
    status = str(batch.get("importStatus") or "unknown")
    tone = (
        "critical"
        if status == "failed"
        else "warning"
        if status in {"pending", "completed_with_errors"}
        else "positive"
    )
    return {
        "header": {
            "title": f"Engineering Import {config_import_batch_id}",
            "status": status,
            "tone": tone,
            "projectId": batch.get("projectId"),
            "configImportBatchId": batch.get("configImportBatchId"),
            "opsImportBatchId": batch.get("importBatchId"),
            "sourceFilePath": summary.get("sourceFilePath"),
            "sheetName": summary.get("sheetName"),
            "createdAtUtc": batch.get("createdAtUtc"),
        },
        "summaryCards": [
            {
                "key": "rawRows",
                "label": "Raw Rows",
                "value": summary.get("rawRows"),
                "tone": "neutral",
            },
            {
                "key": "insertedRows",
                "label": "Inserted Rows",
                "value": summary.get("insertedRows", batch.get("rowCount")),
                "tone": "positive",
            },
            {
                "key": "skippedRows",
                "label": "Skipped Rows",
                "value": summary.get("skippedRows", 0),
                "tone": "warning",
            },
            {
                "key": "deactivatedRows",
                "label": "Deactivated Rows",
                "value": summary.get("deactivatedRows", 0),
                "tone": "neutral",
            },
        ],
        "warningPanel": {
            "count": len(warning_items),
            "items": warning_items,
            "failureDetail": summary.get("failureDetail"),
        },
        "fullSummary": summary,
        "mappingRows": [
            {"field": field_name, "sourceColumn": source_column}
            for field_name, source_column in sorted(column_mapping.items())
        ],
        "opsImportBatch": ops_batch,
        "sampleVariants": detail["sampleVariants"],
        "sampleRows": detail["sampleRows"],
    }


def _persist_failed_config_import(
    session: Session,
    import_batch_id: UUID,
    config_import_batch_id: UUID,
    *,
    user_notes: str | None,
    source_file_name: str,
    source_file_path: str,
    sheet_name: str,
    raw_rows: int,
    skipped_rows: int,
    column_mapping: dict[str, str],
    warnings: list[str],
    failure_detail: str,
) -> None:
    import_batch = repo.get_import_batch(session, import_batch_id)
    config_import_batch = repo.get_config_import_batch(
        session,
        config_import_batch_id,
    )
    if import_batch is None or config_import_batch is None:
        session.rollback()
        return

    import_batch.import_status = "failed"
    import_batch.row_count = 0
    import_batch.error_count = max(1, skipped_rows)
    import_batch.finished_at_utc = datetime.now(timezone.utc)
    config_import_batch.import_status = "failed"
    config_import_batch.row_count = 0
    config_import_batch.notes = _serialize_import_summary(
        _build_import_summary(
            user_notes=user_notes,
            source_file_name=source_file_name,
            source_file_path=source_file_path,
            sheet_name=sheet_name,
            status="failed",
            raw_rows=raw_rows,
            inserted_rows=0,
            skipped_rows=skipped_rows,
            deactivated_rows=0,
            column_mapping=column_mapping,
            warnings=warnings,
            normalization_summary=None,
            failure_detail=failure_detail,
        )
    )
    try:
        session.commit()
    except Exception:
        session.rollback()


def run_config_import(
    session: Session,
    project_id: str,
    data: dict[str, object],
) -> dict[str, object]:
    project = repo.get_project(session, UUID(project_id))
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    source_file = _resolve_import_file_path(str(data["source_file_path"]))
    sheet_name = (
        _coerce_text(data.get("sheet_name"))
        or DEFAULT_ENGINEERING_SHEET
    )
    stored_source_file_path = _to_project_relative_path(source_file)
    user_notes = _coerce_text(data.get("notes"))
    raw_rows = 0
    inserted_rows = 0
    skipped_rows = 0
    deactivated_rows = 0
    warnings: list[str] = []
    column_mapping: dict[str, str] = {}
    normalization_summary: dict[str, object] | None = None

    import_batch = ImportBatch(
        domain="engineering",
        source_file_name=source_file.name,
        source_file_path=stored_source_file_path,
        source_file_hash=_hash_file(source_file),
        import_status="pending",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=None,
    )
    repo.add_import_batch(session, import_batch)
    session.flush()

    config_import_batch = ConfigImportBatch(
        project_id=project.project_id,
        import_batch_id=import_batch.import_batch_id,
        source_schema_version=_coerce_text(
            data.get("source_schema_version")
        ),
        replace_mode=str(data.get("replace_mode") or "full_replace"),
        import_status="pending",
        row_count=0,
        valid_from_date=data.get("valid_from_date"),
        notes=user_notes,
    )
    repo.add_config_import_batch(session, config_import_batch)
    session.flush()
    session.commit()
    session.refresh(import_batch)
    session.refresh(config_import_batch)

    try:
        df = _normalize_dataframe(
            _read_excel_with_fallback(source_file, sheet_name)
        )
        raw_rows = int(len(df))
        if df.empty:
            raise HTTPException(
                status_code=400,
                detail="Engineering import source file is empty.",
            )

        resolved_mapping = _resolve_column_mapping(
            [str(column) for column in df.columns]
        )
        column_mapping = {
            field_name: column_name
            for field_name, column_name in resolved_mapping.items()
            if column_name is not None
        }

        variants, skipped_rows, warnings = _build_variants_for_import(
            project,
            config_import_batch.config_import_batch_id,
            df,
            stored_source_file_path,
            column_mapping,
        )
        if not variants:
            raise HTTPException(
                status_code=400,
                detail="Engineering import produced zero valid rows.",
            )

        deactivated_rows = 0
        if config_import_batch.replace_mode == "full_replace":
            deactivated_rows = repo.deactivate_project_variants(
                session,
                project.project_id,
            )

        repo.add_variants(session, variants)
        inserted_rows = len(variants)
        normalization_summary = rebuild_project_normalized_config(
            session,
            project,
        )

        import_batch.import_status = "success"
        import_batch.row_count = inserted_rows
        import_batch.error_count = skipped_rows
        import_batch.finished_at_utc = datetime.now(timezone.utc)
        config_import_batch.import_status = "success"
        config_import_batch.row_count = inserted_rows
        config_import_batch.notes = _serialize_import_summary(
            _build_import_summary(
                user_notes=user_notes,
                source_file_name=source_file.name,
                source_file_path=stored_source_file_path,
                sheet_name=sheet_name,
                status="success",
                raw_rows=raw_rows,
                inserted_rows=inserted_rows,
                skipped_rows=skipped_rows,
                deactivated_rows=deactivated_rows,
                column_mapping=column_mapping,
                warnings=warnings,
                normalization_summary=normalization_summary,
            )
        )

        _commit_or_conflict(
            session,
            "Engineering import contains duplicate rows",
        )
        session.refresh(import_batch)
        session.refresh(config_import_batch)
    except HTTPException as exc:
        session.rollback()
        _persist_failed_config_import(
            session,
            import_batch.import_batch_id,
            config_import_batch.config_import_batch_id,
            user_notes=user_notes,
            source_file_name=source_file.name,
            source_file_path=stored_source_file_path,
            sheet_name=sheet_name,
            raw_rows=raw_rows,
            skipped_rows=skipped_rows,
            column_mapping=column_mapping,
            warnings=warnings,
            failure_detail=str(exc.detail),
        )
        raise
    except IntegrityError as exc:
        session.rollback()
        _persist_failed_config_import(
            session,
            import_batch.import_batch_id,
            config_import_batch.config_import_batch_id,
            user_notes=user_notes,
            source_file_name=source_file.name,
            source_file_path=stored_source_file_path,
            sheet_name=sheet_name,
            raw_rows=raw_rows,
            skipped_rows=skipped_rows,
            column_mapping=column_mapping,
            warnings=warnings,
            failure_detail="Engineering import contains duplicate rows",
        )
        raise HTTPException(
            status_code=409,
            detail="Engineering import contains duplicate rows",
        ) from exc
    except RuntimeError as exc:
        session.rollback()
        _persist_failed_config_import(
            session,
            import_batch.import_batch_id,
            config_import_batch.config_import_batch_id,
            user_notes=user_notes,
            source_file_name=source_file.name,
            source_file_path=stored_source_file_path,
            sheet_name=sheet_name,
            raw_rows=raw_rows,
            skipped_rows=skipped_rows,
            column_mapping=column_mapping,
            warnings=warnings,
            failure_detail=str(exc),
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        session.rollback()
        _persist_failed_config_import(
            session,
            import_batch.import_batch_id,
            config_import_batch.config_import_batch_id,
            user_notes=user_notes,
            source_file_name=source_file.name,
            source_file_path=stored_source_file_path,
            sheet_name=sheet_name,
            raw_rows=raw_rows,
            skipped_rows=skipped_rows,
            column_mapping=column_mapping,
            warnings=warnings,
            failure_detail=str(exc),
        )
        raise HTTPException(
            status_code=500,
            detail="Engineering import failed unexpectedly",
        ) from exc

    return {
        "projectId": str(project.project_id),
        "importBatch": _config_import_batch_payload(
            config_import_batch,
            include_summary=True,
        ),
        "sourceFileName": source_file.name,
        "sourceFilePath": stored_source_file_path,
        "sheetName": sheet_name,
        "rawRows": raw_rows,
        "insertedRows": inserted_rows,
        "skippedRows": skipped_rows,
        "deactivatedRows": deactivated_rows,
        "columnMapping": column_mapping,
        "warnings": warnings[:20],
        "normalization": normalization_summary,
    }
