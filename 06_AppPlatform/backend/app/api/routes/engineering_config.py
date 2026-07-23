"""API routes for engineering configuration matrix upload, parse, and comparison."""

from __future__ import annotations

import hashlib
import json
import re
import uuid as uuid_module
import zipfile
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from urllib.parse import quote
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.schemas import (
    ConfigFeatureValueCreate,
    ConfigFeatureValueUpdate,
    EngineeringConfigBusinessSummaryComposeRequest,
    EngineeringConfigCompareFactRequest,
    SourceDigestDraftCreate,
    VehicleTrimUpdate,
)
from app.core.config import PROJECT_ROOT
from app.core.security import UserContext, require_min_role
from app.db.models import (
    ConfigAuditLog,
    ConfigVersion,
    EngineeringConfigSourceContextLink,
    FeatureCatalog,
    ImportBatch,
    TrimFeatureValue,
    VehicleTrim,
)
from app.db.session import get_db_session
from app.infra import engineering_config_repository as repo
from app.services.config_availability import classify_availability
from app.services.config_field_mapping_parser import parse_field_mapping
from app.services.config_text_normalization import (
    config_feature_semantic_keys,
    normalize_config_feature_key,
)
from app.services.engineering_config_business_summary_composer import (
    compose_engineering_config_business_summary,
    get_engineering_config_business_summary_readiness,
)
from app.services.engineering_config_compare_facts import (
    build_business_summary_facts,
    build_compare_export_facts,
)
from app.services.engineering_config_export_service import (
    compare_export_filename,
    compare_pdf_export_filename,
    generate_engineering_config_compare_pdf,
    generate_engineering_config_compare_xlsx,
)
from app.services.engineering_config_matrix_parser import parse_config_matrix
from app.services.engineering_config_ocr_readiness_service import (
    get_engineering_config_ocr_readiness,
)
from app.services.engineering_config_source_digest import build_source_digest
from app.services.identity_key_service import build_identity_key

router = APIRouter(prefix="/engineering-config", tags=["engineering_config"])

UPLOAD_SESSION_DIR = PROJECT_ROOT / "04_Processed_data" / "ops" / "eng_config_uploads"
MAX_UPLOAD_FILE_SIZE = 100 * 1024 * 1024
MATRIX_UPLOAD_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
SOURCE_UPLOAD_EXTENSIONS = {
    ".xlsx",
    ".xlsm",
    ".xls",
    ".pdf",
    ".csv",
    ".tsv",
    ".html",
    ".htm",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
}
SOURCE_IMPORT_DOMAIN = "engineering_config_source"
SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT = 100
SOURCE_SNAPSHOT_SEARCH_CANDIDATE_LIMIT = 500
DEFAULT_LOCAL_CONFIG_WORKBOOK = "欧盟在售车型可控资源表20260226.xlsx"
ComparisonType = Literal[
    "COMMON_SAME",
    "DIFFERENT_VALUE",
    "UNIQUE_TO_TRIM",
    "PARTIAL_AVAILABLE",
    "MISSING_OR_UNKNOWN",
]
AVAILABLE_STATES = {"STANDARD", "OPTIONAL", "VALUE"}
UNKNOWN_STATES = {"UNKNOWN"}
_SOURCE_BATCH_UNSET = object()


def _session_dir(upload_id: str) -> Path:
    return UPLOAD_SESSION_DIR / upload_id


def _session_meta_path(upload_id: str) -> Path:
    return _session_dir(upload_id) / "session.json"


def _load_session_meta(upload_id: str) -> dict:
    path = _session_meta_path(upload_id)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Upload session not found")
    return json.loads(path.read_text())


def _save_session_meta(upload_id: str, meta: dict) -> None:
    _session_dir(upload_id).mkdir(parents=True, exist_ok=True)
    _session_meta_path(upload_id).write_text(
        json.dumps(meta, ensure_ascii=False, default=str)
    )


def _safe_upload_file_name(file_name: str) -> str:
    safe_name = Path(file_name.replace("\\", "/")).name.strip()
    if not safe_name:
        raise HTTPException(status_code=400, detail="File name is required")
    return safe_name


def _file_extension(file_name: str) -> str:
    return Path(file_name).suffix.lower()


def _detect_source_type(file_name: str) -> str:
    ext = _file_extension(file_name)
    if ext in MATRIX_UPLOAD_EXTENSIONS:
        return "xlsx" if ext in {".xlsx", ".xlsm"} else "xls"
    if ext == ".pdf":
        return "pdf"
    if ext == ".csv":
        return "csv"
    if ext == ".tsv":
        return "tsv"
    if ext in {".html", ".htm"}:
        return "html"
    if ext in {".png", ".jpg", ".jpeg", ".webp"}:
        return "image"
    return "source_document"


def _default_mime_type(file_name: str) -> str:
    ext = _file_extension(file_name)
    if ext in {".xlsx", ".xlsm"}:
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if ext == ".xls":
        return "application/vnd.ms-excel"
    if ext == ".pdf":
        return "application/pdf"
    if ext == ".csv":
        return "text/csv"
    if ext == ".tsv":
        return "text/tab-separated-values"
    if ext in {".html", ".htm"}:
        return "text/html"
    if ext in {".jpg", ".jpeg"}:
        return "image/jpeg"
    if ext == ".png":
        return "image/png"
    if ext == ".webp":
        return "image/webp"
    return "application/octet-stream"


def _create_upload_session(
    file_name: str,
    total_size: int,
    chunk_size: int,
    upload_kind: str,
    mime_type: str | None = None,
) -> dict:
    safe_name = _safe_upload_file_name(file_name)
    upload_id = str(uuid_module.uuid4())
    total_chunks = (total_size + chunk_size - 1) // chunk_size
    meta = {
        "uploadId": upload_id,
        "fileName": safe_name,
        "totalSize": total_size,
        "chunkSize": chunk_size,
        "totalChunks": total_chunks,
        "uploadedChunks": [],
        "status": "initiated",
        "uploadKind": upload_kind,
        "sourceType": _detect_source_type(safe_name),
        "mimeType": (mime_type or "").strip() or _default_mime_type(safe_name),
        "createdAtUtc": datetime.now(timezone.utc).isoformat(),
    }
    _save_session_meta(upload_id, meta)
    return meta


def _store_upload_chunk(upload_id: str, part_number: int, chunk_data: bytes) -> dict:
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"initiated", "uploading"}:
        raise HTTPException(status_code=409, detail="Upload session not in uploadable state")
    if part_number < 0 or part_number >= int(meta["totalChunks"]):
        raise HTTPException(status_code=400, detail="Part number out of range")
    if len(chunk_data) > int(meta["chunkSize"]):
        raise HTTPException(status_code=400, detail="Chunk exceeds configured chunk size")

    part_path = _session_dir(upload_id) / f"part_{part_number:05d}"
    part_path.write_bytes(chunk_data)

    uploaded = set(meta.get("uploadedChunks", []))
    uploaded.add(part_number)
    meta["uploadedChunks"] = sorted(uploaded)
    meta["status"] = "uploading"
    _save_session_meta(upload_id, meta)

    return {"uploadId": upload_id, "partNumber": part_number, "receivedBytes": len(chunk_data)}


def _assemble_upload_session(upload_id: str) -> dict:
    meta = _load_session_meta(upload_id)
    if len(meta["uploadedChunks"]) != meta["totalChunks"]:
        raise HTTPException(
            status_code=400,
            detail=f"Missing chunks: {meta['totalChunks'] - len(meta['uploadedChunks'])} remaining",
        )

    assembled_path = _session_dir(upload_id) / f"source{_file_extension(meta['fileName'])}"
    with assembled_path.open("wb") as out:
        for i in range(meta["totalChunks"]):
            part_path = _session_dir(upload_id) / f"part_{i:05d}"
            out.write(part_path.read_bytes())

    meta["status"] = "assembled"
    meta["assembledPath"] = str(assembled_path)
    if assembled_path.stat().st_size != int(meta["totalSize"]):
        raise HTTPException(status_code=400, detail="Assembled file size mismatch")
    _save_session_meta(upload_id, meta)
    return meta


def _sha256_for_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_file_prefix(path: Path, size: int = 512) -> bytes:
    with path.open("rb") as handle:
        return handle.read(size)


def _validate_source_file_content(source_path: Path, file_name: str) -> None:
    ext = _file_extension(file_name)
    prefix = _read_file_prefix(source_path)
    if ext == ".pdf" and not prefix.startswith(b"%PDF-"):
        raise HTTPException(status_code=400, detail="PDF file signature mismatch")
    if ext in {".xlsx", ".xlsm"}:
        if not prefix.startswith(b"PK\x03\x04"):
            raise HTTPException(status_code=400, detail="XLSX file signature mismatch")
        try:
            with zipfile.ZipFile(source_path) as workbook:
                names = set(workbook.namelist())
        except zipfile.BadZipFile as exc:
            raise HTTPException(status_code=400, detail="XLSX file is not a valid workbook archive") from exc
        if "[Content_Types].xml" not in names or "xl/workbook.xml" not in names:
            raise HTTPException(status_code=400, detail="XLSX workbook structure mismatch")
    if ext == ".xls" and not prefix.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        raise HTTPException(status_code=400, detail="XLS file signature mismatch")
    if ext == ".png" and not prefix.startswith(b"\x89PNG\r\n\x1a\n"):
        raise HTTPException(status_code=400, detail="PNG file signature mismatch")
    if ext in {".jpg", ".jpeg"} and not prefix.startswith(b"\xff\xd8\xff"):
        raise HTTPException(status_code=400, detail="JPEG file signature mismatch")
    if ext == ".webp" and not (prefix.startswith(b"RIFF") and prefix[8:12] == b"WEBP"):
        raise HTTPException(status_code=400, detail="WEBP file signature mismatch")
    if ext in {".html", ".htm"}:
        leading = prefix.lstrip().lower()
        if not (
            leading.startswith(b"<!doctype html")
            or leading.startswith(b"<html")
            or leading.startswith(b"<table")
        ):
            raise HTTPException(status_code=400, detail="HTML file signature mismatch")
    if ext in {".csv", ".tsv"} and b"\x00" in prefix:
        raise HTTPException(status_code=400, detail="Text source file contains binary data")


def _extract_status_for_source_digest(
    source_digest: dict | None = None,
) -> str:
    if (
        source_digest is not None
        and source_digest.get("status") == "ready"
        and source_digest.get("compareGroups")
    ):
        return "digest_ready"
    return "pending"


def _next_action_for_extract_status(extract_status: str) -> str:
    if extract_status == "digest_ready":
        return "review_digest"
    if extract_status == "pending":
        return "extractor_pending"
    return "review_status"


@lru_cache(maxsize=128)
def _cached_source_digest(source_file_path: str, source_file_name: str, mtime_ns: int, file_size: int) -> dict | None:
    _ = (mtime_ns, file_size)
    return build_source_digest(source_file_path, source_file_name)


def _safe_source_digest(batch: ImportBatch) -> dict | None:
    try:
        source_path = Path(batch.source_file_path)
        stat = source_path.stat()
        return _cached_source_digest(
            str(source_path),
            batch.source_file_name,
            int(stat.st_mtime_ns),
            int(stat.st_size),
        )
    except Exception as exc:
        return {
            "digestType": "unavailable",
            "status": "failed",
            "fileName": batch.source_file_name,
            "modelName": None,
            "summary": {
                "sheetCount": 0,
                "tableCount": 0,
                "candidateTrimCount": 0,
                "comparableGroupCount": 0,
                "featureCount": 0,
                "differenceCount": 0,
            },
            "sheets": [],
            "compareGroups": [],
            "errorMessage": str(exc),
        }


def _source_digest_status_payload(source_digest: dict | None) -> dict | None:
    if not source_digest:
        return None
    summary = source_digest.get("summary") if isinstance(source_digest.get("summary"), dict) else {}
    ocr_engine_candidates = source_digest.get("ocrEngineCandidates")
    return {
        "digestType": source_digest.get("digestType"),
        "sourceFormat": source_digest.get("sourceFormat"),
        "status": source_digest.get("status"),
        "message": source_digest.get("message"),
        "errorMessage": source_digest.get("errorMessage"),
        "ocrEngine": source_digest.get("ocrEngine"),
        "ocrEngineCandidates": ocr_engine_candidates if isinstance(ocr_engine_candidates, list) else None,
        "ocrEvaluation": source_digest.get("ocrEvaluation"),
        "summary": {
            "candidateTrimCount": summary.get("candidateTrimCount", 0),
            "comparableGroupCount": summary.get("comparableGroupCount", 0),
            "featureCount": summary.get("featureCount", 0),
            "differenceCount": summary.get("differenceCount", 0),
        },
    }


def _source_query_matches_values(query: str, values: list[object]) -> bool:
    lowered_query = query.strip().lower()
    if not lowered_query:
        return True
    for value in values:
        if value is None:
            continue
        if isinstance(value, list):
            if _source_query_matches_values(query, value):
                return True
            continue
        if lowered_query in str(value).lower():
            return True
    return False


def _add_source_search_match(matches: list[str], label: str, value: object, *, limit: int = 8) -> None:
    if value is None or len(matches) >= limit:
        return
    cleaned = str(value).strip()
    if not cleaned:
        return
    text = f"{label} {cleaned[:80]}"
    if text not in matches:
        matches.append(text)


def _first_source_query_match_value(query: str, values: list[object]) -> object | None:
    lowered_query = query.strip().lower()
    for value in values:
        if value is None:
            continue
        if isinstance(value, list):
            nested_value = _first_source_query_match_value(query, value)
            if nested_value is not None:
                return nested_value
            continue
        if lowered_query in str(value).lower():
            return value
    return None


def _source_digest_trim_powertrain_values(trim: dict, profile: dict) -> list[object]:
    return [
        trim.get("powertrain"),
        trim.get("energyType"),
        trim.get("energy_type"),
        trim.get("drivetrain"),
        trim.get("engine"),
        trim.get("fuel"),
        trim.get("fuelType"),
        trim.get("fuel_type"),
        profile.get("powertrain"),
        profile.get("energyType"),
        profile.get("energy_type"),
        profile.get("drivetrain"),
        profile.get("engine"),
        profile.get("fuel"),
        profile.get("fuelType"),
        profile.get("fuel_type"),
    ]


def _source_digest_search_matches(source_digest: dict | None, query: str) -> list[str]:
    if not source_digest:
        return []
    matches: list[str] = []
    if _source_query_matches_values(query, [source_digest.get("fileName")]):
        _add_source_search_match(matches, "Digest", source_digest.get("fileName"))
    if _source_query_matches_values(query, [source_digest.get("modelName")]):
        _add_source_search_match(matches, "Model", source_digest.get("modelName"))
    if _source_query_matches_values(query, [source_digest.get("digestType"), source_digest.get("sourceFormat")]):
        _add_source_search_match(matches, "Digest 类型", source_digest.get("sourceFormat") or source_digest.get("digestType"))
    if _source_query_matches_values(query, [source_digest.get("ocrEngine")]):
        _add_source_search_match(matches, "OCR", source_digest.get("ocrEngine"))
    ocr_evaluation = source_digest.get("ocrEvaluation")
    if isinstance(ocr_evaluation, dict) and _source_query_matches_values(query, [ocr_evaluation.get("selectedEngine"), ocr_evaluation.get("selectedEngines")]):
        _add_source_search_match(matches, "OCR", ocr_evaluation.get("selectedEngine") or ocr_evaluation.get("selectedEngines"))
    for candidate in source_digest.get("ocrEngineCandidates") or []:
        if isinstance(candidate, dict) and _source_query_matches_values(query, [candidate.get("engine")]):
            _add_source_search_match(matches, "OCR 候选", candidate.get("engine"))
    for group in source_digest.get("compareGroups") or []:
        if not isinstance(group, dict):
            continue
        if _source_query_matches_values(query, [group.get("modelName"), group.get("title")]):
            _add_source_search_match(matches, "Model", group.get("modelName") or group.get("title"))
        if _source_query_matches_values(query, [group.get("sourceSheet")]):
            _add_source_search_match(matches, "Sheet", group.get("sourceSheet"))
        if _source_query_matches_values(query, [group.get("sourceKind")]):
            _add_source_search_match(matches, "来源类型", group.get("sourceKind"))
        for trim in group.get("trims") or []:
            if not isinstance(trim, dict):
                continue
            profile = trim.get("profile") if isinstance(trim.get("profile"), dict) else {}
            market_values = [trim.get("market"), trim.get("country"), profile.get("country")]
            model_year_values = [profile.get("modelYear"), profile.get("model_year")]
            powertrain_values = _source_digest_trim_powertrain_values(trim, profile)
            material_values = [trim.get("materialNo"), profile.get("materialNo"), profile.get("material_no")]
            sales_version_values = [trim.get("salesVersion"), profile.get("configurationVersion"), profile.get("configuration_version")]
            trim_values = [
                trim.get("trimName"),
                trim.get("fullTrimName"),
                trim.get("modelName"),
                *market_values,
                *model_year_values,
                *powertrain_values,
                *material_values,
                *sales_version_values,
            ]
            if _source_query_matches_values(query, market_values):
                _add_source_search_match(matches, "市场", next((value for value in market_values if value), None))
            if _source_query_matches_values(query, model_year_values):
                _add_source_search_match(matches, "年款", next((value for value in model_year_values if value), None))
            if _source_query_matches_values(query, powertrain_values):
                _add_source_search_match(matches, "动力", _first_source_query_match_value(query, powertrain_values))
            if _source_query_matches_values(query, material_values):
                _add_source_search_match(matches, "物料号", next((value for value in material_values if value), None))
            if _source_query_matches_values(query, sales_version_values):
                _add_source_search_match(matches, "Sales version", next((value for value in sales_version_values if value), None))
            if _source_query_matches_values(query, trim_values):
                _add_source_search_match(
                    matches,
                    "配置列",
                    trim.get("materialNo") or trim.get("salesVersion") or trim.get("trimName") or trim.get("fullTrimName"),
                )
        for row in group.get("rows") or []:
            if not isinstance(row, dict):
                continue
            row_values = [
                row.get("category"),
                row.get("featureKey"),
                row.get("featureCode"),
                row.get("featureName"),
            ]
            if _source_query_matches_values(query, row_values):
                _add_source_search_match(matches, "配置项", row.get("featureName") or row.get("featureCode"))
    return matches


def _source_digest_matches_query(source_digest: dict | None, query: str) -> bool:
    if not source_digest:
        return False
    values: list[object] = [
        source_digest.get("digestType"),
        source_digest.get("sourceFormat"),
        source_digest.get("ocrEngine"),
        source_digest.get("fileName"),
        source_digest.get("modelName"),
    ]
    ocr_evaluation = source_digest.get("ocrEvaluation")
    if isinstance(ocr_evaluation, dict):
        values.extend([
            ocr_evaluation.get("selectedEngine"),
            ocr_evaluation.get("selectedEngines"),
            ocr_evaluation.get("strategy"),
            ocr_evaluation.get("reason"),
        ])
    for candidate in source_digest.get("ocrEngineCandidates") or []:
        if isinstance(candidate, dict):
            values.extend([
                candidate.get("engine"),
                candidate.get("sourceType"),
                candidate.get("sheetName"),
                candidate.get("message"),
            ])
    for sheet in source_digest.get("sheets") or []:
        if isinstance(sheet, dict):
            values.extend([sheet.get("name"), sheet.get("sampleRows")])
    for group in source_digest.get("compareGroups") or []:
        if not isinstance(group, dict):
            continue
        values.extend([
            group.get("groupId"),
            group.get("title"),
            group.get("sourceSheet"),
            group.get("modelName"),
            group.get("sourceKind"),
        ])
        for trim in group.get("trims") or []:
            if isinstance(trim, dict):
                profile = trim.get("profile") if isinstance(trim.get("profile"), dict) else {}
                values.extend([
                    trim.get("trimId"),
                    trim.get("trimName"),
                    trim.get("fullTrimName"),
                    trim.get("modelName"),
                    trim.get("market"),
                    trim.get("country"),
                    trim.get("materialNo"),
                    trim.get("salesVersion"),
                    *_source_digest_trim_powertrain_values(trim, profile),
                    profile.get("country"),
                    profile.get("modelYear"),
                    profile.get("model_year"),
                    profile.get("materialNo"),
                    profile.get("material_no"),
                    profile.get("configurationVersion"),
                    profile.get("configuration_version"),
                ])
        for row in group.get("rows") or []:
            if isinstance(row, dict):
                values.extend([
                    row.get("category"),
                    row.get("featureKey"),
                    row.get("featureCode"),
                    row.get("featureName"),
                ])
    return _source_query_matches_values(query, values)


def _source_snapshot_search_matches(
    batch: ImportBatch,
    context_links: list[EngineeringConfigSourceContextLink],
    query: str,
) -> list[str]:
    matches: list[str] = []
    if _source_query_matches_values(query, [batch.source_file_name]):
        _add_source_search_match(matches, "文件", batch.source_file_name)
    if _source_query_matches_values(query, [batch.import_status]):
        _add_source_search_match(matches, "状态", batch.import_status)
    if _source_query_matches_values(query, [batch.triggered_by]):
        _add_source_search_match(matches, "上传人", batch.triggered_by)
    for link in context_links:
        context_values = [
            link.brand,
            link.model_name,
            link.model_year,
            link.market,
            link.country,
            link.powertrain,
            link.segment,
            link.trim_ids,
            link.sales_version_ids,
            link.context_type,
            link.scenario,
            link.identity_anchor,
        ]
        if _source_query_matches_values(query, context_values):
            context_label = " · ".join(
                value
                for value in (link.brand, link.model_name, link.market or link.country, link.powertrain, link.segment)
                if value
            )
            _add_source_search_match(matches, "上下文", context_label or link.context_type)
        if _source_query_matches_values(query, [link.scenario]):
            _add_source_search_match(matches, "场景", link.scenario)
        if _source_query_matches_values(query, [link.identity_anchor]):
            _add_source_search_match(matches, "身份锚点", link.identity_anchor)
    for match in _source_digest_search_matches(_safe_source_digest(batch), query):
        _add_source_search_match(matches, "", match)
    return [match.strip() for match in matches]


def _source_snapshot_matches_query(
    batch: ImportBatch,
    context_links: list[EngineeringConfigSourceContextLink],
    query: str,
) -> bool:
    values: list[object] = [
        batch.source_file_name,
        batch.source_file_hash,
        batch.source_file_path,
        batch.import_status,
        batch.triggered_by,
    ]
    for link in context_links:
        values.extend([
            link.brand,
            link.model_name,
            link.model_year,
            link.market,
            link.country,
            link.powertrain,
            link.segment,
            link.trim_ids,
            link.sales_version_ids,
            link.context_type,
            link.scenario,
            link.identity_anchor,
            link.created_by,
        ])
    return (
        _source_query_matches_values(query, values)
        or _source_digest_matches_query(_safe_source_digest(batch), query)
    )


def _safe_context_text(value: object, max_length: int = 120) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    return cleaned[:max_length]


def _normalise_trim_ids(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        cleaned = _safe_context_text(item, 80)
        if cleaned and cleaned not in result:
            result.append(cleaned)
        if len(result) >= 8:
            break
    return result


def _normalise_context_ids(value: object) -> list[str]:
    return _normalise_trim_ids(value)


def _safe_context_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return date.fromisoformat(value.strip()[:10])
    except ValueError:
        return None


def _normalise_related_context(payload: dict | None) -> dict:
    raw = payload or {}
    context_value = raw.get("relatedContext", raw.get("context"))
    context = context_value if isinstance(context_value, dict) else raw
    trim_ids = context.get("trimIds", context.get("selectedTrimIds"))
    sales_version_ids = context.get("salesVersionIds", context.get("sales_version_ids"))
    return {
        "brand": _safe_context_text(context.get("brand")),
        "model": _safe_context_text(context.get("model", context.get("modelName"))),
        "market": _safe_context_text(context.get("market", context.get("country"))),
        "country": _safe_context_text(context.get("country", context.get("market"))),
        "powertrain": _safe_context_text(context.get("powertrain")),
        "segment": _safe_context_text(context.get("segment")),
        "modelYear": _safe_context_text(context.get("modelYear", context.get("model_year"))),
        "trimIds": _normalise_trim_ids(trim_ids),
        "salesVersionIds": _normalise_context_ids(sales_version_ids),
        "contextType": _safe_context_text(context.get("contextType", context.get("context_type"))) or "compare",
        "scenario": _safe_context_text(context.get("scenario")),
        "identityAnchor": _safe_context_text(context.get("identityAnchor", context.get("identity_anchor"))),
        "sourceRole": _safe_context_text(context.get("sourceRole", context.get("source_role"))),
        "documentType": _safe_context_text(context.get("documentType", context.get("document_type"))),
        "sourceUrl": _safe_context_text(context.get("sourceUrl", context.get("source_url")), 2000),
        "effectiveFrom": _safe_context_date(context.get("effectiveFrom", context.get("effective_from"))),
        "effectiveTo": _safe_context_date(context.get("effectiveTo", context.get("effective_to"))),
    }


def _has_related_context(context: dict) -> bool:
    return any(
        bool(context.get(key))
        for key in (
            "brand",
            "model",
            "market",
            "country",
            "powertrain",
            "segment",
            "modelYear",
            "sourceRole",
            "documentType",
            "sourceUrl",
            "effectiveFrom",
            "effectiveTo",
        )
    ) or bool(context.get("trimIds")) or bool(context.get("salesVersionIds"))


def _source_context_link_payload(link: EngineeringConfigSourceContextLink) -> dict:
    created_at = link.created_at_utc
    effective_from = getattr(link, "effective_from", None)
    effective_to = getattr(link, "effective_to", None)
    context = {
        "brand": link.brand,
        "model": link.model_name,
        "market": link.market,
        "country": link.country,
        "powertrain": link.powertrain,
        "segment": link.segment,
        "modelYear": link.model_year,
        "trimIds": link.trim_ids or [],
        "salesVersionIds": link.sales_version_ids or [],
        "contextType": link.context_type,
        "scenario": link.scenario,
        "identityAnchor": link.identity_anchor,
        "sourceRole": getattr(link, "source_role", None),
        "documentType": getattr(link, "document_type", None),
        "sourceUrl": getattr(link, "source_url", None),
        "effectiveFrom": effective_from.isoformat() if effective_from else None,
        "effectiveTo": effective_to.isoformat() if effective_to else None,
    }
    return {
        "id": str(link.id),
        "sourceId": str(link.source_id),
        "batchId": str(link.batch_id),
        "brand": link.brand,
        "model": link.model_name,
        "market": link.market,
        "country": link.country,
        "powertrain": link.powertrain,
        "segment": link.segment,
        "modelYear": link.model_year,
        "trimIds": link.trim_ids or [],
        "salesVersionIds": link.sales_version_ids or [],
        "contextType": link.context_type,
        "scenario": link.scenario,
        "identityAnchor": link.identity_anchor,
        "sourceRole": getattr(link, "source_role", None),
        "documentType": getattr(link, "document_type", None),
        "sourceUrl": getattr(link, "source_url", None),
        "effectiveFrom": effective_from.isoformat() if effective_from else None,
        "effectiveTo": effective_to.isoformat() if effective_to else None,
        "status": getattr(link, "status", "active") or "active",
        "createdBy": link.created_by,
        "createdAt": created_at.isoformat() if created_at else None,
        "relatedContext": context,
    }


def _context_link_status(link: EngineeringConfigSourceContextLink) -> str:
    return getattr(link, "status", "active") or "active"


def _context_link_matches_country(link: EngineeringConfigSourceContextLink, country: str | None) -> bool:
    country_value = country.strip().lower() if country else ""
    if not country_value:
        return True
    return country_value in ((link.country or "").lower()) or country_value in ((link.market or "").lower())


def _context_link_matches_text(value: str | None, expected: str | None) -> bool:
    expected_value = expected.strip().lower() if expected else ""
    if not expected_value:
        return True
    actual_value = (value or "").strip().lower()
    if not actual_value:
        return True
    return expected_value in actual_value


def _filter_source_context_links_for_scope(
    context_links: list[EngineeringConfigSourceContextLink],
    *,
    brand: str | None = None,
    country: str | None = None,
    model_year: str | None = None,
    powertrain: str | None = None,
    trash_only: bool = False,
    include_trash: bool = False,
) -> list[EngineeringConfigSourceContextLink]:
    expected_status = "trashed" if trash_only else None
    filtered: list[EngineeringConfigSourceContextLink] = []
    for link in context_links:
        status = _context_link_status(link)
        if status == "purged":
            continue
        if expected_status and status != expected_status:
            continue
        if not expected_status and not include_trash and status != "active":
            continue
        if not _context_link_matches_country(link, country):
            continue
        if not _context_link_matches_text(link.brand, brand):
            continue
        if not _context_link_matches_text(link.model_year, model_year):
            continue
        if not _context_link_matches_text(link.powertrain, powertrain):
            continue
        filtered.append(link)
    return filtered


def _source_context_from_links(
    context_links: list[EngineeringConfigSourceContextLink],
) -> tuple[dict, list[dict]]:
    context_payloads = [_source_context_link_payload(link) for link in context_links]
    if not context_payloads:
        return _normalise_related_context({}), []
    latest = context_payloads[0]["relatedContext"]
    return _normalise_related_context(latest), context_payloads


def _create_source_context_link(
    session: Session,
    batch: ImportBatch,
    *,
    related_context: dict,
    user: UserContext,
) -> EngineeringConfigSourceContextLink | None:
    if not _has_related_context(related_context):
        return None
    link = EngineeringConfigSourceContextLink(
        source_id=batch.import_batch_id,
        batch_id=batch.import_batch_id,
        brand=related_context.get("brand"),
        model_name=related_context.get("model"),
        model_year=related_context.get("modelYear"),
        market=related_context.get("market"),
        country=related_context.get("country"),
        powertrain=related_context.get("powertrain"),
        segment=related_context.get("segment"),
        trim_ids=related_context.get("trimIds") or [],
        sales_version_ids=related_context.get("salesVersionIds") or [],
        context_type=related_context.get("contextType") or "compare",
        scenario=related_context.get("scenario"),
        identity_anchor=related_context.get("identityAnchor"),
        source_role=related_context.get("sourceRole"),
        document_type=related_context.get("documentType"),
        source_url=related_context.get("sourceUrl"),
        effective_from=related_context.get("effectiveFrom"),
        effective_to=related_context.get("effectiveTo"),
        created_by=user.name,
    )
    repo.add_source_context_link(session, link)
    return link


def _source_snapshot_payload(
    batch: ImportBatch,
    *,
    context_links: list[EngineeringConfigSourceContextLink] | None = None,
    include_digest: bool = False,
    include_digest_status: bool = True,
    source_search_matches: list[str] | None = None,
    status: str | None = None,
    duplicate: bool = False,
    linked_to_current_context: bool = False,
) -> dict:
    storage_path = Path(batch.source_file_path)
    file_type = _detect_source_type(batch.source_file_name)
    if status is not None:
        upload_status = status
    elif batch.import_status == "trashed":
        upload_status = "trashed"
    elif batch.import_status == "purged":
        upload_status = "purged"
    else:
        upload_status = "duplicate" if duplicate else "registered"
    source_digest = _safe_source_digest(batch) if include_digest else None
    status_digest = source_digest if include_digest else (_safe_source_digest(batch) if include_digest_status else None)
    source_digest_status = _source_digest_status_payload(status_digest)
    extract_status = _extract_status_for_source_digest(status_digest)
    created_at = batch.created_at_utc or batch.started_at_utc
    file_size = storage_path.stat().st_size if storage_path.exists() else 0
    source_id = str(batch.import_batch_id)
    related_context, contexts = _source_context_from_links(context_links or [])
    payload = {
        "source_id": source_id,
        "batch_id": source_id,
        "import_batch_id": source_id,
        "import_type": "source_snapshot",
        "asset_type": "source_snapshot",
        "upload_type": "source_snapshot",
        "filename": batch.source_file_name,
        "file_type": file_type,
        "mime_type": _default_mime_type(batch.source_file_name),
        "file_size": file_size,
        "sha256": batch.source_file_hash,
        "storage_path": batch.source_file_path,
        "upload_status": upload_status,
        "library_status": batch.import_status,
        "in_trash": upload_status == "trashed",
        "extract_status": extract_status,
        "next_action": _next_action_for_extract_status(extract_status),
        "created_by": batch.triggered_by,
        "created_at": created_at.isoformat() if created_at else None,
        "error_message": None,
        "duplicate": duplicate,
        "deduplicated": duplicate,
        "linked_to_current_context": linked_to_current_context,
        "related_context": related_context,
        "contexts": contexts,
        "source_search_matches": source_search_matches or [],
        "source_digest_status": source_digest_status,
        "source_digest": source_digest,
        "sourceId": source_id,
        "batchId": source_id,
        "importBatchId": source_id,
        "importType": "source_snapshot",
        "assetType": "source_snapshot",
        "uploadType": "source_snapshot",
        "sourceFileName": batch.source_file_name,
        "fileType": file_type,
        "mimeType": _default_mime_type(batch.source_file_name),
        "fileSize": file_size,
        "sourceFileHash": batch.source_file_hash,
        "sourceFilePath": batch.source_file_path,
        "uploadStatus": upload_status,
        "libraryStatus": batch.import_status,
        "inTrash": upload_status == "trashed",
        "extractStatus": extract_status,
        "nextAction": _next_action_for_extract_status(extract_status),
        "createdBy": batch.triggered_by,
        "createdAt": created_at.isoformat() if created_at else None,
        "errorMessage": None,
        "deduplicated": duplicate,
        "linkedToCurrentContext": linked_to_current_context,
        "relatedContext": related_context,
        "contexts": contexts,
        "sourceSearchMatches": source_search_matches or [],
        "sourceDigestStatus": source_digest_status,
        "sourceDigest": source_digest,
    }
    return payload


def _trim_payload(
    trim: VehicleTrim,
    session: Session | None = None,
    source_batch: ImportBatch | None | object = _SOURCE_BATCH_UNSET,
    identity_version: ConfigVersion | None = None,
) -> dict:
    def identity_value(field_name: str) -> object:
        if identity_version is not None and hasattr(identity_version, field_name):
            return getattr(identity_version, field_name)
        return getattr(trim, field_name)

    vehicle_code = identity_value("vehicle_code")
    material_no = identity_value("material_no")
    identity_key = identity_value("identity_key")
    sales_version = vehicle_code or material_no or identity_key
    source_upload_id = identity_value("source_upload_id")
    if source_batch is _SOURCE_BATCH_UNSET and session is not None and source_upload_id is not None:
        source_batch = repo.get_import_batch(session, source_upload_id)
    if source_batch is _SOURCE_BATCH_UNSET:
        source_batch = None
    source_created_at = None
    if source_batch is not None:
        source_created_at = source_batch.created_at_utc or source_batch.started_at_utc
    has_material_no = bool(str(material_no or "").strip())
    return {
        "trimId": str(trim.trim_id),
        "fullTrimName": trim.full_trim_name if identity_version is None else identity_value("trim_name"),
        "brand": identity_value("brand"),
        "modelName": identity_value("model_name"),
        "trimName": identity_value("trim_name"),
        "market": identity_value("market"),
        "country": identity_value("market"),
        "modelYear": identity_value("model_year"),
        "energyType": trim.energy_type,
        "drivetrain": trim.drivetrain,
        "engine": trim.engine,
        "status": trim.status,
        "vehicleCode": vehicle_code,
        "materialNo": material_no,
        "identityKey": identity_key,
        "salesVersion": sales_version,
        "sourceUploadId": str(source_upload_id) if source_upload_id else None,
        "sourceFileName": source_batch.source_file_name if source_batch else None,
        "sourceFilePath": source_batch.source_file_path if source_batch else None,
        "sourceCreatedBy": source_batch.triggered_by if source_batch else None,
        "sourceCreatedAt": source_created_at.isoformat() if source_created_at else None,
        "importStatus": source_batch.import_status if source_batch else None,
        "hasMaterialNo": has_material_no,
        "dataOrigin": "own_catalog" if has_material_no else "external_or_scraped",
        "msrp": None,
        "targetPrice": None,
    }


def _require_editable_trim(
    trim: VehicleTrim | None,
    trim_id: UUID,
) -> VehicleTrim:
    if trim is None or getattr(trim, "status", None) == "purged":
        raise HTTPException(status_code=404, detail=f"Trim not found: {trim_id}")
    if getattr(trim, "status", None) == "trashed":
        raise HTTPException(
            status_code=409,
            detail="Config trim is in trash; restore it before editing",
        )
    return trim


def _require_editable_trim_for_value(
    session: Session,
    value: TrimFeatureValue,
) -> tuple[VehicleTrim, ConfigVersion | None]:
    trim_id = getattr(value, "trim_id", None)
    if trim_id is None:
        raise HTTPException(status_code=409, detail="Feature value is missing trim context")
    trim = _require_editable_trim(repo.get_vehicle_trim(session, trim_id), trim_id)
    draft_version = _require_editable_config_version(session, trim)
    refresh = getattr(session, "refresh", None)
    if callable(refresh):
        refresh(value)
    return trim, draft_version


def _require_editable_config_version(
    session: Session,
    trim: VehicleTrim,
) -> ConfigVersion | None:
    repo.acquire_config_trim_lock(session, trim.trim_id)
    latest_version = repo.get_latest_config_version_for_trim(
        session,
        trim.trim_id,
        for_update=True,
    )
    if latest_version is not None and latest_version.status != "draft":
        raise HTTPException(
            status_code=409,
            detail="Published configuration is immutable; create a new draft before editing",
        )
    return latest_version


def _manual_snapshot_value(
    feature: FeatureCatalog,
    *,
    raw_value: str,
    normalized_value: str | None,
    availability: str,
    unit: str | None,
) -> dict:
    return {
        "featureId": str(feature.feature_id),
        "featureCode": feature.feature_code,
        "featureName": feature.standard_field_name,
        "category": feature.category,
        "rawValue": raw_value,
        "normalizedValue": normalized_value,
        "availability": availability,
        "unit": unit,
        "sourceRow": 0,
        "sourceColumn": "manual",
        "sourceUploadId": None,
        "source": None,
        "inferred": False,
        "inferenceReason": None,
        "confidence": None,
    }


def _upsert_draft_snapshot_value(
    version: ConfigVersion | None,
    feature: FeatureCatalog,
    *,
    raw_value: str,
    normalized_value: str | None,
    availability: str,
    unit: str | None,
) -> None:
    if version is None:
        return
    replacement = _manual_snapshot_value(
        feature,
        raw_value=raw_value,
        normalized_value=normalized_value,
        availability=availability,
        unit=unit,
    )
    feature_id = str(feature.feature_id)
    snapshot_values = [
        dict(item)
        for item in (version.snapshot_values or [])
        if isinstance(item, dict) and str(item.get("featureId") or "") != feature_id
    ]
    snapshot_values.append(replacement)
    version.snapshot_values = snapshot_values
    version.snapshot_feature_count = len(snapshot_values)


def _remove_draft_snapshot_value(
    version: ConfigVersion | None,
    feature_id: UUID,
) -> None:
    if version is None:
        return
    feature_id_text = str(feature_id)
    snapshot_values = [
        dict(item)
        for item in (version.snapshot_values or [])
        if isinstance(item, dict) and str(item.get("featureId") or "") != feature_id_text
    ]
    version.snapshot_values = snapshot_values
    version.snapshot_feature_count = len(snapshot_values)


def _same_text(left: str | None, right: str | None) -> bool:
    return (left or "").strip().casefold() == (right or "").strip().casefold()


def _compact_metric(value: object) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


_ADVANCED_ANALYSIS_COUNTRY_ALIASES = {
    "germany": "德国",
    "de": "德国",
    "deu": "德国",
    "德国": "德国",
    "france": "法国",
    "fr": "法国",
    "fra": "法国",
    "法国": "法国",
    "italy": "意大利",
    "it": "意大利",
    "ita": "意大利",
    "意大利": "意大利",
    "spain": "西班牙",
    "es": "西班牙",
    "esp": "西班牙",
    "西班牙": "西班牙",
    "sweden": "瑞典",
    "se": "瑞典",
    "swe": "瑞典",
    "瑞典": "瑞典",
    "norway": "挪威",
    "no": "挪威",
    "nor": "挪威",
    "挪威": "挪威",
    "denmark": "丹麦",
    "dk": "丹麦",
    "dnk": "丹麦",
    "丹麦": "丹麦",
    "finland": "芬兰",
    "fi": "芬兰",
    "fin": "芬兰",
    "芬兰": "芬兰",
    "austria": "奥地利",
    "at": "奥地利",
    "aut": "奥地利",
    "奥地利": "奥地利",
    "switzerland": "瑞士",
    "ch": "瑞士",
    "che": "瑞士",
    "瑞士": "瑞士",
    "netherlands": "荷兰",
    "nl": "荷兰",
    "nld": "荷兰",
    "holland": "荷兰",
    "荷兰": "荷兰",
    "belgium": "比利时",
    "be": "比利时",
    "bel": "比利时",
    "比利时": "比利时",
    "poland": "波兰",
    "pl": "波兰",
    "pol": "波兰",
    "波兰": "波兰",
    "czechia": "捷克",
    "czech republic": "捷克",
    "cz": "捷克",
    "cze": "捷克",
    "捷克": "捷克",
    "hungary": "匈牙利",
    "hu": "匈牙利",
    "hun": "匈牙利",
    "匈牙利": "匈牙利",
    "croatia": "克罗地亚",
    "hr": "克罗地亚",
    "hrv": "克罗地亚",
    "克罗地亚": "克罗地亚",
    "slovenia": "斯洛文尼亚",
    "si": "斯洛文尼亚",
    "svn": "斯洛文尼亚",
    "斯洛文尼亚": "斯洛文尼亚",
    "romania": "罗马尼亚",
    "ro": "罗马尼亚",
    "rou": "罗马尼亚",
    "罗马尼亚": "罗马尼亚",
    "slovakia": "斯洛伐克",
    "sk": "斯洛伐克",
    "svk": "斯洛伐克",
    "斯洛伐克": "斯洛伐克",
    "greece": "希腊",
    "gr": "希腊",
    "grc": "希腊",
    "希腊": "希腊",
    "portugal": "葡萄牙",
    "pt": "葡萄牙",
    "prt": "葡萄牙",
    "葡萄牙": "葡萄牙",
}

_ADVANCED_ANALYSIS_SEGMENT_ALIASES = {
    "suv c": "SUV C及以上",
    "suv c+": "SUV C及以上",
    "suv c及以上": "SUV C及以上",
    "suv c and above": "SUV C及以上",
}


def _advanced_analysis_country(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    return _ADVANCED_ANALYSIS_COUNTRY_ALIASES.get(cleaned.casefold(), cleaned)


def _advanced_analysis_segment(value: str | None) -> str | None:
    cleaned = (value or "").strip()
    if not cleaned:
        return None
    normalized = re.sub(r"\s+", " ", cleaned.replace("_", " ").replace("-", " ")).strip().casefold()
    return _ADVANCED_ANALYSIS_SEGMENT_ALIASES.get(normalized, cleaned)


def _list_recommended_config_trims(
    session: Session,
    *,
    brand: str | None,
    model_name: str,
    market: str,
    energy_type: str | None,
    limit: int,
) -> list[VehicleTrim]:
    trims = repo.list_vehicle_trims(
        session,
        brand=brand,
        model_name=model_name,
        market=market,
        energy_type=energy_type,
        status="active",
        limit=limit,
    )
    if trims:
        return trims
    if brand:
        trims = repo.list_vehicle_trims(
            session,
            model_name=model_name,
            market=market,
            energy_type=energy_type,
            status="active",
            limit=limit,
        )
    if trims:
        return trims
    if energy_type:
        trims = repo.list_vehicle_trims(
            session,
            brand=brand,
            model_name=model_name,
            market=market,
            status="active",
            limit=limit,
        )
        if trims:
            return trims
        if brand:
            return repo.list_vehicle_trims(
                session,
                model_name=model_name,
                market=market,
                status="active",
                limit=limit,
            )
    return trims


def _normalised_recommendation_text(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("_", " ").replace("-", " ")).strip().casefold()


def _recommendation_text_matches(value: object, expected: str | None) -> bool:
    expected_text = _normalised_recommendation_text(expected)
    if not expected_text:
        return True
    value_text = _normalised_recommendation_text(value)
    if not value_text:
        return False
    return expected_text == value_text or expected_text in value_text or value_text in expected_text


def _source_digest_group_matches_recommendation(group: dict, *, brand: str | None, model_name: str, energy_type: str | None) -> bool:
    group_text_values: list[object] = [
        group.get("modelName"),
        group.get("title"),
        group.get("sourceSheet"),
    ]
    trim_powertrain_values: list[object] = []
    trim_brand_values: list[object] = []
    for trim in group.get("trims") or []:
        if not isinstance(trim, dict):
            continue
        profile = trim.get("profile") if isinstance(trim.get("profile"), dict) else {}
        group_text_values.extend([
            trim.get("modelName"),
            trim.get("trimName"),
            trim.get("fullTrimName"),
        ])
        trim_brand_values.extend([trim.get("brand"), profile.get("brand"), profile.get("make")])
        trim_powertrain_values.extend(_source_digest_trim_powertrain_values(trim, profile))

    model_matches = any(_recommendation_text_matches(value, model_name) for value in group_text_values)
    if not model_matches:
        return False
    brand_text = _normalised_recommendation_text(brand)
    brand_matches = not brand_text or not any(_normalised_recommendation_text(value) for value in trim_brand_values) or any(
        _recommendation_text_matches(value, brand)
        for value in trim_brand_values
    )
    if not brand_matches:
        return False
    powertrain_text = _normalised_recommendation_text(energy_type)
    return not powertrain_text or not trim_powertrain_values or any(
        _recommendation_text_matches(value, energy_type)
        for value in trim_powertrain_values
    )


def _source_context_link_matches_recommendation(
    link: EngineeringConfigSourceContextLink,
    *,
    brand: str | None,
    model_name: str,
    market: str | None,
    energy_type: str | None,
    segment: str | None,
) -> bool:
    if not _recommendation_text_matches(link.model_name, model_name):
        return False
    checks = [
        (link.brand, brand),
        (link.country or link.market, market),
        (link.powertrain, energy_type),
        (link.segment, segment),
    ]
    return all(_recommendation_text_matches(value, expected) for value, expected in checks)


def _recommended_source_digest_coverage(
    session: Session,
    *,
    brand: str | None,
    model_name: str,
    market: str,
    energy_type: str | None,
    segment: str | None,
    candidate_batches: list[ImportBatch] | None = None,
    limit: int = SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT,
) -> dict[str, object]:
    search_terms = [brand, model_name, market, energy_type, segment]
    source_search_query = " ".join(term.strip() for term in search_terms if isinstance(term, str) and term.strip())
    if candidate_batches is None:
        try:
            batches = repo.list_source_snapshot_batches(
                session,
                SOURCE_IMPORT_DOMAIN,
                country=market,
                segment=segment,
                query=None,
                limit=limit,
            )
        except Exception:
            batches = []
    else:
        batches = candidate_batches

    source_ids: set[str] = set()
    group_count = 0
    trim_count = 0
    matches: list[dict[str, object]] = []
    for batch in batches:
        digest = _safe_source_digest(batch)
        compare_groups = digest.get("compareGroups") if isinstance(digest, dict) else None
        if not isinstance(compare_groups, list):
            continue
        context_links = _filter_source_context_links_for_scope(
            repo.list_source_context_links(session, batch.import_batch_id),
            brand=brand,
            country=market,
            powertrain=energy_type,
        )
        source_context_matches = any(
            _source_context_link_matches_recommendation(
                link,
                brand=brand,
                model_name=model_name,
                market=market,
                energy_type=energy_type,
                segment=segment,
            )
            for link in context_links
        )
        source_id = str(batch.import_batch_id)
        source_group_count = 0
        source_trim_count = 0
        for group in compare_groups:
            if not isinstance(group, dict):
                continue
            group_matches = _source_digest_group_matches_recommendation(
                group,
                brand=brand,
                model_name=model_name,
                energy_type=energy_type,
            )
            context_single_group_fallback = source_context_matches and len(compare_groups) == 1
            if not group_matches and not context_single_group_fallback:
                continue
            source_group_count += 1
            source_trim_count += len(group.get("trims") or [])
        if source_group_count <= 0:
            continue
        source_ids.add(source_id)
        group_count += source_group_count
        trim_count += source_trim_count
        if len(matches) < 5:
            matches.append({
                "sourceId": source_id,
                "sourceFileName": batch.source_file_name,
                "groupCount": source_group_count,
                "trimCount": source_trim_count,
            })

    return {
        "available": group_count > 0,
        "sourceCount": len(source_ids),
        "groupCount": group_count,
        "trimCount": trim_count,
        "searchQuery": source_search_query,
        "matches": matches,
    }


def _recommendation_reason(row: dict) -> str:
    evidence = row.get("match_evidence")
    if isinstance(evidence, list) and evidence:
        details = [
            str(item.get("detail") or item.get("label") or item.get("field"))
            for item in evidence
            if isinstance(item, dict)
        ]
        details = [detail for detail in details if detail.strip()]
        if details:
            return "；".join(details[:3])
    shared_dims = row.get("shared_dims")
    if isinstance(shared_dims, list) and shared_dims:
        return "同 " + " / ".join(str(dim) for dim in shared_dims[:3])
    return "高级分析按车型画像和份额迁移排序"


def _source_ref_column_letter(column_number: int) -> str:
    if column_number <= 0:
        return ""
    letters = ""
    current = column_number
    while current:
        current, remainder = divmod(current - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def _parsed_source_ref_from_cell(cell: str, source_row: int) -> dict[str, object]:
    pdf_ocr_match = re.fullmatch(r"P(?P<page>\d+)OCRR(?P<row>\d+)C(?P<column>\d+)", cell)
    if pdf_ocr_match:
        column_number = int(pdf_ocr_match.group("column"))
        return {
            "rowNumber": int(pdf_ocr_match.group("row")),
            "columnNumber": column_number,
            "columnLetter": _source_ref_column_letter(column_number),
            "cell": cell,
            "sourceCell": cell,
            "sourceType": "pdf_ocr",
            "pageNumber": int(pdf_ocr_match.group("page")),
        }

    pdf_text_match = re.fullmatch(r"P(?P<page>\d+)R(?P<row>\d+)C(?P<column>\d+)", cell)
    if pdf_text_match:
        column_number = int(pdf_text_match.group("column"))
        return {
            "rowNumber": int(pdf_text_match.group("row")),
            "columnNumber": column_number,
            "columnLetter": _source_ref_column_letter(column_number),
            "cell": cell,
            "sourceCell": cell,
            "sourceType": "pdf_text",
            "pageNumber": int(pdf_text_match.group("page")),
        }

    image_ocr_match = re.fullmatch(r"OCRR(?P<row>\d+)C(?P<column>\d+)", cell)
    if image_ocr_match:
        column_number = int(image_ocr_match.group("column"))
        return {
            "rowNumber": int(image_ocr_match.group("row")),
            "columnNumber": column_number,
            "columnLetter": _source_ref_column_letter(column_number),
            "cell": cell,
            "sourceCell": cell,
            "sourceType": "image_ocr",
        }

    spreadsheet_match = re.fullmatch(r"(?P<column>[A-Za-z]+)(?P<row>\d+)", cell)
    if spreadsheet_match:
        return {
            "rowNumber": source_row or int(spreadsheet_match.group("row")),
            "columnNumber": 0,
            "columnLetter": spreadsheet_match.group("column").upper(),
            "cell": cell,
            "sourceCell": cell,
        }

    column_letter = "".join(char for char in cell if char.isalpha()) or cell
    return {
        "rowNumber": source_row,
        "columnNumber": 0,
        "columnLetter": column_letter,
        "cell": cell or "-",
        "sourceCell": cell or "-",
    }


def _source_cell_key(row_number: object, cell: object) -> tuple[int, str] | None:
    if not isinstance(row_number, int):
        return None
    cell_text = str(cell or "").strip().upper()
    if not cell_text:
        return None
    return row_number, cell_text


def _source_evidence_lookup_keys(source: dict[str, object]) -> list[tuple[int, str]]:
    keys: list[tuple[int, str]] = []
    row_number = source.get("rowNumber")
    for field in ("cell", "sourceCell"):
        key = _source_cell_key(row_number, source.get(field))
        if key is not None and key not in keys:
            keys.append(key)
    return keys


def _source_evidence_payload(value: dict[str, object], source: dict[str, object]) -> dict[str, object | None]:
    evidence: dict[str, object | None] = dict(source)
    for key in ("inferred", "inferenceReason", "confidence"):
        if value.get(key) is not None:
            evidence[key] = value.get(key)
    if value.get("rawValue") is not None:
        evidence["rawValue"] = value.get("rawValue")
    return evidence


def _source_digest_evidence_index(source_digest: dict | None) -> dict[tuple[int, str], list[dict[str, object | None]]]:
    if not isinstance(source_digest, dict):
        return {}
    index: dict[tuple[int, str], list[dict[str, object | None]]] = {}
    compare_groups = source_digest.get("compareGroups")
    if not isinstance(compare_groups, list):
        return index
    for group in compare_groups:
        if not isinstance(group, dict):
            continue
        rows = group.get("rows")
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            values = row.get("values")
            if not isinstance(values, list):
                continue
            for value in values:
                if not isinstance(value, dict):
                    continue
                source = value.get("source")
                if not isinstance(source, dict):
                    continue
                evidence = _source_evidence_payload(value, source)
                for key in _source_evidence_lookup_keys(source):
                    index.setdefault(key, []).append(evidence)
    return index


@lru_cache(maxsize=128)
def _cached_source_evidence_index(
    source_file_path: str,
    source_file_name: str,
    mtime_ns: int,
    file_size: int,
) -> dict[tuple[int, str], list[dict[str, object | None]]]:
    source_digest = _cached_source_digest(source_file_path, source_file_name, mtime_ns, file_size)
    return _source_digest_evidence_index(source_digest)


def _source_evidence_for_value(
    batch: ImportBatch,
    parsed_source: dict[str, object],
    raw_value: str | None,
) -> dict[str, object | None] | None:
    try:
        source_path = Path(batch.source_file_path)
        stat = source_path.stat()
    except OSError:
        return None
    key = _source_cell_key(parsed_source.get("rowNumber"), parsed_source.get("cell"))
    if key is None:
        return None
    evidence_items = _cached_source_evidence_index(
        str(source_path),
        batch.source_file_name,
        int(stat.st_mtime_ns),
        int(stat.st_size),
    ).get(key, [])
    if not evidence_items:
        return None
    raw_text = str(raw_value or "")
    for item in evidence_items:
        if str(item.get("rawValue") or "") == raw_text:
            return item
    return evidence_items[0]


def _source_ref_payload_for_value(
    value: TrimFeatureValue,
    session: Session,
    source_batches: dict[UUID, ImportBatch] | None = None,
) -> dict | None:
    if hasattr(value, "snapshot_source"):
        snapshot_source = getattr(value, "snapshot_source", None)
        return dict(snapshot_source) if isinstance(snapshot_source, dict) else None
    # Manual changes intentionally detach from the source file. Showing the
    # old coordinate as proof for an overridden value would be misleading.
    if (getattr(value, "source_column", None) or "").strip().lower() == "manual":
        return None
    source_upload_id = getattr(value, "source_upload_id", None)
    if source_upload_id is None:
        return None
    batch = source_batches.get(source_upload_id) if source_batches is not None else repo.get_import_batch(session, source_upload_id)
    if batch is None:
        return None
    source_column = (getattr(value, "source_column", None) or "").strip()
    cell = source_column
    source_row = getattr(value, "source_row", 0)
    if source_column and not any(char.isdigit() for char in source_column) and source_row:
        cell = f"{source_column}{source_row}"
    parsed = _parsed_source_ref_from_cell(cell, source_row)
    payload = {
        "sheetName": batch.source_file_name,
        **parsed,
        "mergedRange": None,
    }
    evidence = _source_evidence_for_value(batch, parsed, getattr(value, "raw_value", None))
    if evidence is not None:
        for key in (
            "sheetName",
            "rowNumber",
            "columnNumber",
            "columnLetter",
            "cell",
            "sourceCell",
            "mergedRange",
            "sourceType",
            "pageNumber",
            "inferenceReason",
            "confidence",
            "inferred",
        ):
            if key in evidence:
                payload[key] = evidence[key]
    return payload


def _is_unknown_compare_cell(cell: dict | None) -> bool:
    return cell is None or str(cell.get("availability") or "") in UNKNOWN_STATES


def _is_available_compare_cell(cell: dict | None) -> bool:
    return cell is not None and str(cell.get("availability") or "") in AVAILABLE_STATES


def _compare_cell_signature(cell: dict | None) -> tuple[str, str, str]:
    if cell is None:
        return ("MISSING", "", "")
    return (
        str(cell.get("availability") or ""),
        str(cell.get("normalizedValue") or ""),
        str(cell.get("rawValue") or ""),
    )


def _api_value_state(raw_value: str | None, availability: str | None) -> str:
    if availability in {"STANDARD", "OPTIONAL", "NOT_AVAILABLE"}:
        return "marker_value"
    if availability == "NOT_APPLICABLE":
        return "not_applicable"
    if availability == "UNKNOWN":
        return "blank"
    if raw_value and raw_value.replace(",", "").replace(".", "", 1).isdigit():
        return "numeric_value"
    return "text_value"


def _api_display_value(raw_value: str | None, availability: str | None) -> str:
    if availability == "STANDARD":
        return "标配"
    if availability == "OPTIONAL":
        return "选装"
    if availability == "NOT_AVAILABLE":
        return "不配备"
    if availability == "NOT_APPLICABLE":
        return "不适用"
    if availability == "UNKNOWN":
        return "待确认"
    return raw_value or ""


def _classify_comparison_type(values: list[dict | None]) -> ComparisonType:
    if any(_is_unknown_compare_cell(value) for value in values):
        return "MISSING_OR_UNKNOWN"

    available_count = sum(1 for value in values if _is_available_compare_cell(value))
    if available_count == len(values):
        signatures = {_compare_cell_signature(value) for value in values}
        return "COMMON_SAME" if len(signatures) == 1 else "DIFFERENT_VALUE"

    if available_count == 1:
        return "UNIQUE_TO_TRIM"

    if available_count > 1:
        return "PARTIAL_AVAILABLE"

    signatures = {_compare_cell_signature(value) for value in values}
    return "COMMON_SAME" if len(signatures) == 1 else "DIFFERENT_VALUE"


def _business_note(
    comparison_type: ComparisonType,
    values: list[dict | None],
    trims: list[VehicleTrim],
) -> str:
    if comparison_type == "COMMON_SAME":
        return "共同配置，当前不构成版本差异"
    if comparison_type == "DIFFERENT_VALUE":
        return "配置值或标配/选装状态不同，可用于解释版本定位差异"
    if comparison_type == "MISSING_OR_UNKNOWN":
        return "存在缺失或未知数据，需先确认配置源"

    available_trim_names = [
        trims[idx].full_trim_name
        for idx, value in enumerate(values)
        if _is_available_compare_cell(value)
    ]
    if comparison_type == "UNIQUE_TO_TRIM" and available_trim_names:
        return f"{available_trim_names[0]} 独有配置"
    if available_trim_names:
        return f"{len(available_trim_names)} 个版本具备，其他版本不具备"
    return "部分版本具备"


# ── Feature Catalog ────────────────────────────────────────────────


def _feature_mapping_aliases(raw_aliases: object) -> list[str]:
    if not isinstance(raw_aliases, list):
        return []
    aliases: list[str] = []
    for item in raw_aliases:
        alias = str(item).strip() if item is not None else ""
        if alias and alias not in aliases:
            aliases.append(alias)
    return aliases


def _merged_feature_aliases(existing_aliases: object, incoming_aliases: list[str]) -> list[str] | None:
    merged = _feature_mapping_aliases(existing_aliases)
    for alias in incoming_aliases:
        if alias not in merged:
            merged.append(alias)
    return merged or None


def _upsert_feature_catalog_from_mapping(
    session: Session,
    parsed_mapping: dict,
) -> dict[str, object]:
    created_count = 0
    updated_count = 0
    unchanged_count = 0
    items = parsed_mapping.get("features") if isinstance(parsed_mapping.get("features"), list) else []

    for item in items:
        if not isinstance(item, dict):
            continue
        category = str(item.get("category") or "").strip()
        standard_field_name = str(item.get("standard_field_name") or "").strip()
        feature_code = str(item.get("feature_code") or "").strip()
        if not category or not standard_field_name or not feature_code:
            continue
        seq = int(item.get("seq") or item.get("display_order") or 0)
        display_order = int(item.get("display_order") or seq or 0)
        incoming_aliases = _feature_mapping_aliases(item.get("aliases"))
        feature = repo.get_feature_catalog_by_category_field(session, category, standard_field_name)
        if feature is None:
            feature = repo.get_feature_catalog_by_code(session, feature_code)
        if feature is None:
            feature = FeatureCatalog(
                seq=seq,
                category=category,
                standard_field_name=standard_field_name,
                feature_code=feature_code,
                unit=None,
                data_type="string",
                aliases=incoming_aliases or None,
                display_order=display_order,
                is_active=True,
            )
            session.add(feature)
            created_count += 1
            continue

        next_aliases = _merged_feature_aliases(getattr(feature, "aliases", None), incoming_aliases)
        changed = False
        if getattr(feature, "aliases", None) != next_aliases:
            feature.aliases = next_aliases
            changed = True
        if seq and getattr(feature, "seq", None) != seq:
            feature.seq = seq
            changed = True
        if display_order and getattr(feature, "display_order", None) != display_order:
            feature.display_order = display_order
            changed = True
        if getattr(feature, "is_active", True) is not True:
            feature.is_active = True
            changed = True
        if changed:
            updated_count += 1
        else:
            unchanged_count += 1

    return {
        "totalFeatures": len(items),
        "createdFeatureCount": created_count,
        "updatedFeatureCount": updated_count,
        "unchangedFeatureCount": unchanged_count,
        "warningCount": len(parsed_mapping.get("warnings") or []),
        "warnings": parsed_mapping.get("warnings") or [],
        "categories": parsed_mapping.get("categories") or [],
    }


def _build_feature_catalog_mapping_audit(
    meta: dict,
    summary: dict[str, object],
    user: UserContext,
    imported_at_utc: str | None = None,
) -> dict[str, object]:
    upload_id = str(meta.get("uploadId") or "")
    return {
        "uploadId": upload_id,
        "fileName": str(meta.get("fileName") or ""),
        "status": "feature_catalog_imported",
        "importedBy": user.name,
        "importedRole": user.role,
        "importedAtUtc": imported_at_utc or datetime.now(timezone.utc).isoformat(),
        "artifactRef": f"eng_config_uploads/{upload_id}/session.json" if upload_id else "eng_config_uploads/session.json",
        "persistedIn": "upload_session_meta",
        "summary": summary,
    }


@router.get("/feature-catalog")
def get_feature_catalog(
    category: str | None = Query(default=None),
    is_active: bool | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=1000),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    features = repo.list_feature_catalog(session, category, is_active, limit)
    return {
        "rows": len(features),
        "items": [
            {
                "featureId": str(f.feature_id),
                "seq": f.seq,
                "category": f.category,
                "standardFieldName": f.standard_field_name,
                "featureCode": f.feature_code,
                "unit": f.unit,
                "dataType": f.data_type,
                "aliases": f.aliases,
                "displayOrder": f.display_order,
                "isActive": f.is_active,
            }
            for f in features
        ],
    }


@router.post("/feature-catalog/upload")
def upload_feature_catalog(
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    """Parse an uploaded field mapping Excel and store in FeatureCatalog.

    The file must be placed at a known path before calling this endpoint.
    """
    _ = session
    raise HTTPException(
        status_code=400,
        detail="Use /engineering-config/feature-catalog/upload/initiate, upload parts, then /complete.",
    )


@router.post("/feature-catalog/upload/initiate")
def initiate_feature_catalog_upload(
    file_name: str = Query(),
    total_size: int = Query(ge=1, le=MAX_UPLOAD_FILE_SIZE),
    chunk_size: int = Query(default=5 * 1024 * 1024, ge=1024, le=50 * 1024 * 1024),
    _=Depends(require_min_role("editor")),
) -> dict:
    safe_name = _safe_upload_file_name(file_name)
    if _file_extension(safe_name) not in MATRIX_UPLOAD_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Feature catalog mapping upload requires .xlsx, .xlsm, or .xls")
    return _create_upload_session(safe_name, total_size, chunk_size, "feature_catalog")


@router.put("/feature-catalog/upload/{upload_id}/parts/{part_number}")
async def upload_feature_catalog_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _=Depends(require_min_role("editor")),
) -> dict:
    chunk_data = await request.body()
    return _store_upload_chunk(upload_id, part_number, chunk_data)


@router.post("/feature-catalog/upload/{upload_id}/complete")
def complete_feature_catalog_upload(
    upload_id: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    meta = _assemble_upload_session(upload_id)
    if meta.get("uploadKind") != "feature_catalog":
        raise HTTPException(status_code=409, detail="Upload session is not for feature catalog mapping")
    source_path = Path(meta["assembledPath"])
    _validate_source_file_content(source_path, meta["fileName"])
    try:
        parsed_mapping = parse_field_mapping(source_path)
        summary = _upsert_feature_catalog_from_mapping(session, parsed_mapping)
        session.commit()
    except HTTPException:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        meta["status"] = "feature_catalog_failed"
        meta["error"] = str(exc)
        _save_session_meta(upload_id, meta)
        raise HTTPException(status_code=400, detail=f"Feature catalog mapping import failed: {exc}")

    meta["status"] = "feature_catalog_imported"
    meta["featureCatalogSummary"] = summary
    audit = _build_feature_catalog_mapping_audit(meta, summary, user)
    meta["featureCatalogAudit"] = audit
    _save_session_meta(upload_id, meta)
    return {
        "uploadId": upload_id,
        "fileName": meta["fileName"],
        "status": "feature_catalog_imported",
        "summary": summary,
        "audit": audit,
    }


# ── Matrix Upload (chunked) ────────────────────────────────────────


@router.post("/matrix/upload/initiate")
def initiate_matrix_upload(
    file_name: str = Query(),
    total_size: int = Query(ge=1, le=MAX_UPLOAD_FILE_SIZE),
    chunk_size: int = Query(default=5 * 1024 * 1024, ge=1024, le=50 * 1024 * 1024),
    _=Depends(require_min_role("editor")),
) -> dict:
    safe_name = _safe_upload_file_name(file_name)
    if _file_extension(safe_name) not in MATRIX_UPLOAD_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Engineering config matrix upload requires .xlsx, .xlsm, or .xls")
    return _create_upload_session(safe_name, total_size, chunk_size, "matrix")


@router.put("/matrix/upload/{upload_id}/parts/{part_number}")
async def upload_matrix_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _=Depends(require_min_role("editor")),
) -> dict:
    chunk_data = await request.body()
    return _store_upload_chunk(upload_id, part_number, chunk_data)


@router.post("/matrix/upload/{upload_id}/complete")
def complete_matrix_upload(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    meta = _assemble_upload_session(upload_id)
    _validate_source_file_content(Path(meta["assembledPath"]), meta["fileName"])
    return {**meta, "next": f"/engineering-config/matrix/upload/{upload_id}/parse"}


@router.post("/source/upload/initiate")
def initiate_source_upload(
    file_name: str = Query(),
    total_size: int = Query(ge=1, le=MAX_UPLOAD_FILE_SIZE),
    chunk_size: int = Query(default=5 * 1024 * 1024, ge=1024, le=50 * 1024 * 1024),
    mime_type: str | None = Query(default=None),
    _=Depends(require_min_role("editor")),
) -> dict:
    safe_name = _safe_upload_file_name(file_name)
    if _file_extension(safe_name) not in SOURCE_UPLOAD_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Unsupported source file type")
    return _create_upload_session(safe_name, total_size, chunk_size, "source", mime_type)


@router.put("/source/upload/{upload_id}/parts/{part_number}")
async def upload_source_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _=Depends(require_min_role("editor")),
) -> dict:
    chunk_data = await request.body()
    return _store_upload_chunk(upload_id, part_number, chunk_data)


@router.post("/source/upload/{upload_id}/complete")
def complete_source_upload(
    upload_id: str,
    payload: dict | None = Body(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    meta = _assemble_upload_session(upload_id)
    related_context = _normalise_related_context(payload)
    source_path = Path(meta["assembledPath"])
    _validate_source_file_content(source_path, meta["fileName"])
    source_file_hash = _sha256_for_path(source_path)
    existing_batch = repo.get_import_batch_by_hash(
        session,
        SOURCE_IMPORT_DOMAIN,
        source_file_hash,
    )
    if existing_batch is not None:
        link = _create_source_context_link(
            session,
            existing_batch,
            related_context=related_context,
            user=user,
        )
        try:
            if link is not None:
                session.flush()
            session.commit()
        except Exception as exc:
            session.rollback()
            raise HTTPException(status_code=409, detail=f"Source context link failed: {exc}")
        meta["status"] = "duplicate"
        meta["importBatchId"] = str(existing_batch.import_batch_id)
        meta["sourceFileHash"] = source_file_hash
        meta["duplicateOf"] = str(existing_batch.import_batch_id)
        _save_session_meta(upload_id, meta)
        return {
            **_source_snapshot_payload(
                existing_batch,
                context_links=[link] if link is not None else [],
                include_digest=True,
                status="duplicate",
                duplicate=True,
                linked_to_current_context=link is not None,
            ),
            "parseMode": "stored_source",
            "message": "Duplicate source file. Existing source snapshot returned.",
        }

    import_batch = ImportBatch(
        domain=SOURCE_IMPORT_DOMAIN,
        source_file_name=meta["fileName"],
        source_file_path=str(source_path),
        source_file_hash=source_file_hash,
        import_status="stored",
        row_count=0,
        error_count=0,
        triggered_by=user.name,
        started_at_utc=datetime.now(timezone.utc),
        finished_at_utc=datetime.now(timezone.utc),
    )
    repo.add_import_batch(session, import_batch)
    try:
        session.flush()
        link = _create_source_context_link(
            session,
            import_batch,
            related_context=related_context,
            user=user,
        )
        if link is not None:
            session.flush()
        session.commit()
    except Exception as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Source intake failed: {exc}")

    meta["status"] = "registered"
    meta["importBatchId"] = str(import_batch.import_batch_id)
    meta["sourceFileHash"] = import_batch.source_file_hash
    _save_session_meta(upload_id, meta)

    return {
        **_source_snapshot_payload(
            import_batch,
            context_links=[link] if link is not None else [],
            include_digest=True,
            status="registered",
            linked_to_current_context=link is not None,
        ),
        "parseMode": "stored_source",
        "message": "Source snapshot registered. Workbook/CSV/TSV/HTML, text-based PDF, and OCR-readable images can be converted to editable config columns when compare groups are detected. Scanned PDF/image OCR remains pending when no OCR engine is configured.",
    }


@router.get("/source/local-workbook-digest")
def get_local_source_workbook_digest(
    file_name: str = Query(default=DEFAULT_LOCAL_CONFIG_WORKBOOK),
    _=Depends(require_min_role("viewer")),
) -> dict:
    return _load_local_source_workbook_digest(file_name)


def _load_local_source_workbook_digest(file_name: str) -> dict:
    safe_name = _safe_upload_file_name(file_name)
    if _file_extension(safe_name) not in MATRIX_UPLOAD_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Local workbook digest requires .xlsx, .xlsm, or .xls")
    source_path = (PROJECT_ROOT / "02_Config_MetaData" / safe_name).resolve()
    allowed_root = (PROJECT_ROOT / "02_Config_MetaData").resolve()
    if allowed_root not in source_path.parents:
        raise HTTPException(status_code=400, detail="Local workbook must be under 02_Config_MetaData")
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="Local workbook not found")
    _validate_source_file_content(source_path, safe_name)
    stat = source_path.stat()
    digest = _cached_source_digest(
        str(source_path),
        safe_name,
        int(stat.st_mtime_ns),
        int(stat.st_size),
    )
    if digest is None:
        raise HTTPException(status_code=422, detail="Local workbook digest is not available")
    return digest


@router.post("/business-summary/compose")
def compose_business_summary(
    payload: EngineeringConfigBusinessSummaryComposeRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    compare_facts, base_trim_id, filters = _canonical_compare_facts_for_request(
        session,
        payload,
    )
    try:
        composer_facts = build_business_summary_facts(
            compare_facts,
            base_trim_id=base_trim_id,
            filters=filters,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return compose_engineering_config_business_summary(composer_facts)


@router.get("/business-summary/readiness")
def get_business_summary_readiness(
    _=Depends(require_min_role("viewer")),
) -> dict:
    return get_engineering_config_business_summary_readiness()


@router.get("/ocr/readiness")
def get_ocr_readiness(
    _=Depends(require_min_role("viewer")),
) -> dict:
    return get_engineering_config_ocr_readiness()


@router.get("/source/snapshots")
def list_source_snapshots(
    limit: int = Query(default=20, ge=1, le=100),
    brand: str | None = Query(default=None),
    country: str | None = Query(default=None),
    model_year: str | None = Query(default=None, alias="modelYear"),
    powertrain: str | None = Query(default=None),
    segment: str | None = Query(default=None),
    q: str | None = Query(default=None),
    include_trash: bool = Query(default=False, alias="includeTrash"),
    trash_only: bool = Query(default=False, alias="trashOnly"),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    query_text = q.strip() if q else ""
    candidate_limit = SOURCE_SNAPSHOT_SEARCH_CANDIDATE_LIMIT if query_text else SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT
    batches = repo.list_source_snapshot_batches(
        session,
        SOURCE_IMPORT_DOMAIN,
        brand=brand,
        country=country,
        model_year=model_year,
        powertrain=powertrain,
        segment=segment,
        query=query_text or None,
        include_trash=include_trash,
        trash_only=trash_only,
        limit=candidate_limit,
    )
    if query_text and not batches:
        batches = repo.list_source_snapshot_batches(
            session,
            SOURCE_IMPORT_DOMAIN,
            brand=brand,
            country=country,
            model_year=model_year,
            powertrain=powertrain,
            segment=segment,
            query=None,
            include_trash=include_trash,
            trash_only=trash_only,
            limit=candidate_limit,
        )
    context_links_by_source = {
        batch.import_batch_id: _filter_source_context_links_for_scope(
            repo.list_source_context_links(session, batch.import_batch_id),
            brand=brand,
            country=country,
            model_year=model_year,
            powertrain=powertrain,
            trash_only=trash_only,
            include_trash=include_trash,
        )
        for batch in batches
    }
    source_search_matches_by_source: dict[UUID, list[str]] = {}
    if query_text:
        matched_batches: list[ImportBatch] = []
        for batch in batches:
            search_matches = _source_snapshot_search_matches(
                batch,
                context_links_by_source.get(batch.import_batch_id, []),
                query_text,
            )
            if not search_matches:
                continue
            source_search_matches_by_source[batch.import_batch_id] = search_matches
            matched_batches.append(batch)
        total_rows = len(matched_batches)
        batches = matched_batches[:limit]
    else:
        total_rows = len(batches)
        batches = batches[:limit]
    return {
        "rows": total_rows,
        "items": [
            _source_snapshot_payload(
                batch,
                context_links=context_links_by_source.get(batch.import_batch_id, []),
                source_search_matches=source_search_matches_by_source.get(batch.import_batch_id),
                status="trashed" if trash_only else None,
            )
            for batch in batches
        ],
    }


@router.delete("/source/snapshots/{source_id}")
def trash_source_snapshot(
    source_id: UUID,
    country: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    batch = repo.get_import_batch(session, source_id)
    if batch is None or batch.domain != SOURCE_IMPORT_DOMAIN or batch.import_status == "purged":
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    country_value = country.strip() if country else ""
    if country_value:
        updated = repo.set_source_context_status(session, source_id, "trashed", country=country_value)
        if updated == 0:
            raise HTTPException(status_code=404, detail="Source snapshot country context not found")
        session.commit()
        context_links = _filter_source_context_links_for_scope(
            repo.list_source_context_links(session, batch.import_batch_id),
            country=country_value,
            trash_only=True,
        )
        return {
            **_source_snapshot_payload(
                batch,
                context_links=context_links,
                status="trashed",
            ),
            "message": f"Source snapshot moved to {country_value} trash by {user.name}.",
        }
    if batch.import_status != "trashed":
        updated = repo.set_import_batch_status(session, source_id, "trashed")
        if updated == 0:
            raise HTTPException(status_code=404, detail="Source snapshot not found")
        session.commit()
        batch.import_status = "trashed"
    return {
        **_source_snapshot_payload(
            batch,
            context_links=repo.list_source_context_links(session, batch.import_batch_id),
            status="trashed",
        ),
        "message": f"Source snapshot moved to trash by {user.name}.",
    }


@router.post("/source/snapshots/{source_id}/restore")
def restore_source_snapshot(
    source_id: UUID,
    country: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    batch = repo.get_import_batch(session, source_id)
    if batch is None or batch.domain != SOURCE_IMPORT_DOMAIN or batch.import_status == "purged":
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    country_value = country.strip() if country else ""
    if country_value:
        updated = repo.set_source_context_status(session, source_id, "active", country=country_value)
        if updated == 0:
            raise HTTPException(status_code=404, detail="Source snapshot country context not found")
        session.commit()
        context_links = _filter_source_context_links_for_scope(
            repo.list_source_context_links(session, batch.import_batch_id),
            country=country_value,
        )
        return {
            **_source_snapshot_payload(
                batch,
                context_links=context_links,
                status="registered",
            ),
            "message": f"Source snapshot restored from {country_value} trash by {user.name}.",
        }
    if batch.import_status == "trashed":
        updated = repo.set_import_batch_status(session, source_id, "stored")
        if updated == 0:
            raise HTTPException(status_code=404, detail="Source snapshot not found")
        session.commit()
        batch.import_status = "stored"
    return {
        **_source_snapshot_payload(
            batch,
            context_links=repo.list_source_context_links(session, batch.import_batch_id),
            status="registered",
        ),
        "message": f"Source snapshot restored by {user.name}.",
    }


@router.delete("/source/trash")
def clear_source_snapshot_trash(
    country: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    country_value = country.strip() if country else ""
    if not country_value:
        raise HTTPException(status_code=400, detail="country is required to clear source trash")
    purged_count = repo.clear_source_snapshot_trash(
        session,
        SOURCE_IMPORT_DOMAIN,
        country=country_value,
    )
    session.commit()
    return {
        "cleared": purged_count,
        "country": country_value,
        "message": f"Cleared {purged_count} trashed source snapshots by {user.name}.",
    }


@router.get("/source/snapshots/{source_id}")
def get_source_snapshot_detail(
    source_id: UUID,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    batch = repo.get_import_batch(session, source_id)
    if batch is None or batch.domain != SOURCE_IMPORT_DOMAIN:
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    return _source_snapshot_payload(
        batch,
        context_links=_filter_source_context_links_for_scope(
            repo.list_source_context_links(session, batch.import_batch_id),
            include_trash=True,
        ),
        include_digest=True,
    )


def _digest_string(value: object, fallback: str = "") -> str:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or fallback
    return fallback


def _digest_first_string(*values: object) -> str:
    for value in values:
        cleaned = _digest_string(value)
        if cleaned:
            return cleaned
    return ""


def _digest_compact_name(parts: list[str | None]) -> str:
    result: list[str] = []
    for part in parts:
        cleaned = (part or "").strip()
        if cleaned and cleaned not in result:
            result.append(cleaned)
    return " / ".join(result)


def _digest_source_identity_label(source_batch: ImportBatch) -> str:
    file_name = _digest_string(source_batch.source_file_name, "source")
    source_id = str(source_batch.import_batch_id)
    short_source_id = source_id[:8] if source_id else ""
    return _digest_compact_name([file_name, short_source_id])


def _digest_profile(trim: dict) -> dict:
    profile = trim.get("profile")
    return profile if isinstance(profile, dict) else {}


def _digest_trim_powertrain_fields(trim: dict, profile: dict) -> tuple[str | None, str | None, str | None]:
    energy_type = _digest_first_string(
        trim.get("energyType"),
        trim.get("energy_type"),
        trim.get("powertrain"),
        trim.get("fuelType"),
        trim.get("fuel_type"),
        trim.get("fuel"),
        profile.get("energyType"),
        profile.get("energy_type"),
        profile.get("powertrain"),
        profile.get("fuelType"),
        profile.get("fuel_type"),
        profile.get("fuel"),
    ) or None
    drivetrain = _digest_first_string(trim.get("drivetrain"), profile.get("drivetrain")) or None
    engine = _digest_first_string(trim.get("engine"), profile.get("engine")) or None
    return energy_type, drivetrain, engine


CONTEXT_BACKED_DIGEST_EXTENSIONS = {".csv", ".tsv", ".html", ".htm", ".pdf", ".png", ".jpg", ".jpeg", ".webp"}


def _active_source_context_links_for_draft(
    session: Session,
    source_id: UUID,
) -> list[EngineeringConfigSourceContextLink]:
    context_links = repo.list_source_context_links(session, source_id)
    if not context_links:
        return []
    active_context_links = _filter_source_context_links_for_scope(context_links)
    if not active_context_links:
        raise HTTPException(
            status_code=409,
            detail="Source snapshot is in trash; restore it before creating editable config columns",
        )
    return active_context_links


def _source_context_for_draft(
    context_links: list[EngineeringConfigSourceContextLink],
) -> dict:
    related_context, _contexts = _source_context_from_links(context_links)
    return related_context


def _source_context_applies_to_digest(source_file_name: str) -> bool:
    return _file_extension(source_file_name) in CONTEXT_BACKED_DIGEST_EXTENSIONS


def _context_string(source_context: dict, key: str) -> str:
    value = source_context.get(key)
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _digest_source_ref(value: dict | None) -> tuple[int, str]:
    source = value.get("source") if isinstance(value, dict) else None
    if not isinstance(source, dict):
        return 0, "digest"
    row_number = source.get("rowNumber")
    source_row = row_number if isinstance(row_number, int) else 0
    source_column = _digest_string(
        source.get("cell"),
        _digest_string(source.get("columnLetter"), str(source.get("columnNumber") or "digest")),
    )
    return source_row, source_column


def _digest_group_ocr_engine(digest: dict, group: dict) -> str | None:
    engine = _digest_string(digest.get("ocrEngine"))
    if engine:
        return engine
    for row in group.get("rows", []):
        if not isinstance(row, dict):
            continue
        values = row.get("values")
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, dict):
                continue
            source = value.get("source")
            if not isinstance(source, dict):
                continue
            source_engine = _digest_string(source.get("ocrEngine"))
            if source_engine:
                return source_engine
    return None


def _digest_draft_source_payload(source_batch: ImportBatch, digest: dict, group: dict) -> dict[str, object | None]:
    ocr_candidates = digest.get("ocrEngineCandidates")
    return {
        "sourceFileName": source_batch.source_file_name,
        "groupTitle": _digest_string(group.get("title"), _digest_string(group.get("modelName"), group.get("groupId"))),
        "sourceDigestType": _digest_string(digest.get("digestType")) or None,
        "sourceFormat": _digest_string(digest.get("sourceFormat"), _digest_string(digest.get("digestType"))) or None,
        "sourceKind": _digest_string(group.get("sourceKind"), "config_matrix"),
        "ocrEngine": _digest_group_ocr_engine(digest, group),
        "ocrEngineCandidates": ocr_candidates if isinstance(ocr_candidates, list) else None,
        "ocrEvaluation": digest.get("ocrEvaluation") if isinstance(digest.get("ocrEvaluation"), dict) else None,
    }


def _normalise_digest_draft_trim_ids(payload: SourceDigestDraftCreate | None) -> list[str]:
    if payload is None:
        return []
    selected: list[str] = []
    for value in payload.trim_ids:
        item = str(value).strip()
        if item and item not in selected:
            selected.append(item)
    return selected


def _normalise_digest_draft_trim_identity_overrides(
    payload: SourceDigestDraftCreate | None,
) -> dict[str, dict[str, str]]:
    if payload is None:
        return {}
    overrides: dict[str, dict[str, str]] = {}
    field_map = {
        "brand": "brand",
        "model_name": "modelName",
        "trim_name": "trimName",
        "full_trim_name": "fullTrimName",
        "market": "market",
        "country": "country",
        "model_year": "modelYear",
        "energy_type": "energyType",
        "drivetrain": "drivetrain",
        "engine": "engine",
        "material_no": "materialNo",
        "sales_version": "salesVersion",
    }
    for item in payload.trim_identity_overrides:
        trim_id = item.trim_id.strip()
        if not trim_id:
            continue
        if trim_id in overrides:
            raise HTTPException(status_code=422, detail=f"Duplicate digest trim identity override: {trim_id}")
        override: dict[str, str] = {}
        for source_field, target_field in field_map.items():
            value = getattr(item, source_field)
            if isinstance(value, str) and value.strip():
                override[target_field] = value.strip()
        if override:
            overrides[trim_id] = override
    return overrides


def _apply_digest_trim_identity_overrides(
    trims: list[dict],
    overrides: dict[str, dict[str, str]],
) -> list[dict]:
    if not overrides:
        return trims
    result: list[dict] = []
    for trim in trims:
        trim_id = _digest_string(trim.get("trimId"))
        override = overrides.get(trim_id)
        if not override:
            result.append(trim)
            continue
        next_trim = {**trim, "__identityOverride": override}
        profile = dict(_digest_profile(trim))
        if "brand" in override:
            profile["brand"] = override["brand"]
        if "market" in override:
            next_trim["market"] = override["market"]
        if "country" in override:
            next_trim["country"] = override["country"]
            profile["country"] = override["country"]
        if "modelYear" in override:
            next_trim["modelYear"] = override["modelYear"]
            profile["modelYear"] = override["modelYear"]
        for field in ("modelName", "trimName", "fullTrimName", "energyType", "drivetrain", "engine", "materialNo", "salesVersion"):
            if field in override:
                next_trim[field] = override[field]
        if "energyType" in override:
            profile["energyType"] = override["energyType"]
            profile["powertrain"] = override["energyType"]
        if "drivetrain" in override:
            profile["drivetrain"] = override["drivetrain"]
        if "engine" in override:
            profile["engine"] = override["engine"]
        if "materialNo" in override:
            profile["materialNo"] = override["materialNo"]
        if "salesVersion" in override:
            profile["configurationVersion"] = override["salesVersion"]
        next_trim["profile"] = profile
        result.append(next_trim)
    return result


def _project_digest_row_values(row: dict, selected_indexes: list[int], selected_trim_ids: set[str]) -> dict:
    values = row.get("values") if isinstance(row.get("values"), list) else []
    unique_trim_ids = row.get("uniqueTrimIds") if isinstance(row.get("uniqueTrimIds"), list) else []
    return {
        **row,
        "uniqueTrimIds": [
            item
            for item in unique_trim_ids
            if isinstance(item, str) and item in selected_trim_ids
        ],
        "values": [
            values[index] if index < len(values) else None
            for index in selected_indexes
        ],
    }


def _feature_catalog_alias_keys(feature: FeatureCatalog) -> set[str]:
    keys = {normalize_config_feature_key(feature.standard_field_name)}
    aliases = getattr(feature, "aliases", None)
    if isinstance(aliases, list):
        keys.update(
            normalize_config_feature_key(str(alias))
            for alias in aliases
            if isinstance(alias, str) and alias.strip()
        )
    for value in [feature.standard_field_name, *(aliases if isinstance(aliases, list) else [])]:
        if isinstance(value, str):
            keys.update(config_feature_semantic_keys(value))
    return {key for key in keys if key}


def _feature_catalog_alias_preference(feature: FeatureCatalog, source_feature_name: str) -> tuple[int, int, int, str]:
    standard_name = feature.standard_field_name or ""
    exact_source_name = normalize_config_feature_key(standard_name) == normalize_config_feature_key(source_feature_name)
    richness = 0
    if "/" in standard_name or "／" in standard_name:
        richness += 20
    if re.search(r"[\u4e00-\u9fff]", standard_name):
        richness += 20
    if not exact_source_name:
        richness += 10
    category = getattr(feature, "category", "") or ""
    category_matrix_score = 30 if re.search(r"[A-Za-z]", category) and re.search(r"[\u4e00-\u9fff]", category) else 0
    display_order = getattr(feature, "display_order", 9999) or 9999
    return (richness + category_matrix_score, len(standard_name), -int(display_order), standard_name)


def _feature_catalog_match_for_digest_row(
    session: Session,
    feature_name: str,
    catalog_features: list[FeatureCatalog] | None = None,
) -> FeatureCatalog | None:
    lookup_keys = {
        normalize_config_feature_key(feature_name),
        *config_feature_semantic_keys(feature_name),
    }
    lookup_keys = {key for key in lookup_keys if key}
    if not lookup_keys:
        return None

    candidates_by_id: dict[UUID, FeatureCatalog] = {}
    searchable_features = catalog_features if catalog_features is not None else repo.list_feature_catalog(session, is_active=True, limit=1000)
    for feature in searchable_features:
        if not getattr(feature, "is_active", True):
            continue
        if lookup_keys & _feature_catalog_alias_keys(feature):
            candidates_by_id[feature.feature_id] = feature
    if not candidates_by_id:
        return None

    candidates = list(candidates_by_id.values())
    if len(candidates) == 1:
        return candidates[0]

    source_key = normalize_config_feature_key(feature_name)
    non_exact_candidates = [
        feature
        for feature in candidates
        if normalize_config_feature_key(feature.standard_field_name) != source_key
    ]
    if non_exact_candidates:
        candidates = non_exact_candidates
    return max(candidates, key=lambda feature: _feature_catalog_alias_preference(feature, feature_name))


def _feature_catalog_match_reason(feature: FeatureCatalog, row: dict, created: bool) -> str:
    if created:
        return "created"
    feature_name = _digest_string(row.get("featureName"), _digest_string(row.get("featureCode"), ""))
    feature_code = _digest_string(row.get("featureCode"))
    if feature_code and feature.feature_code == feature_code:
        return "feature_code"
    category = _digest_string(row.get("category"), "Uncategorized")
    if feature.category == category and normalize_config_feature_key(feature.standard_field_name) == normalize_config_feature_key(feature_name):
        return "category_field"
    if normalize_config_feature_key(feature_name) in _feature_catalog_alias_keys(feature):
        return "alias"
    if set(config_feature_semantic_keys(feature_name)) & _feature_catalog_alias_keys(feature):
        return "semantic_alias"
    return "reused"


def _create_digest_feature(
    session: Session,
    row: dict,
    *,
    next_order: int,
    feature_code: str | None = None,
) -> FeatureCatalog:
    category = _digest_string(row.get("category"), "Uncategorized")
    feature_name = _digest_string(
        row.get("featureName"),
        _digest_string(row.get("featureCode"), "Unnamed feature"),
    )
    feature = FeatureCatalog(
        seq=next_order,
        category=category,
        standard_field_name=feature_name,
        feature_code=feature_code
        or _digest_string(row.get("featureCode"), f"digest_{next_order}"),
        unit=None,
        data_type="string",
        aliases=[feature_name],
        display_order=next_order,
        is_active=True,
    )
    session.add(feature)
    session.flush()
    return feature


def _digest_feature_for_row(
    session: Session,
    row: dict,
    *,
    next_order: int,
    catalog_features: list[FeatureCatalog] | None = None,
) -> tuple[FeatureCatalog, bool, str]:
    category = _digest_string(row.get("category"), "Uncategorized")
    feature_name = _digest_string(row.get("featureName"), _digest_string(row.get("featureCode"), "Unnamed feature"))
    feature_code = _digest_string(row.get("featureCode"), f"digest_{next_order}")
    feature = repo.get_feature_catalog_by_code(session, feature_code)
    if feature is None:
        feature = repo.get_feature_catalog_by_category_field(session, category, feature_name)
    alias_feature = _feature_catalog_match_for_digest_row(session, feature_name, catalog_features)
    if feature is not None and alias_feature is not None and alias_feature.feature_id != feature.feature_id:
        source_semantic_keys = config_feature_semantic_keys(feature_name)
        if source_semantic_keys and _feature_catalog_alias_preference(alias_feature, feature_name) > _feature_catalog_alias_preference(feature, feature_name):
            feature = alias_feature
    if feature is None:
        feature = alias_feature
    if feature is not None:
        return feature, False, _feature_catalog_match_reason(feature, row, False)
    feature = _create_digest_feature(
        session,
        row,
        next_order=next_order,
        feature_code=feature_code,
    )
    return feature, True, "created"


def _upsert_digest_trim(
    session: Session,
    *,
    trim: dict,
    group: dict,
    source_batch: ImportBatch,
    user: UserContext,
    source_context: dict | None = None,
) -> tuple[VehicleTrim, bool, ConfigVersion]:
    source_context = source_context or {}
    allow_context_identity = (
        _source_context_applies_to_digest(source_batch.source_file_name)
        and group.get("sourceKind") != "price_list"
    )
    identity_override = trim.get("__identityOverride") if isinstance(trim.get("__identityOverride"), dict) else {}
    profile = _digest_profile(trim)
    brand = _digest_string(profile.get("brand"), "Unknown")
    model_name = _digest_string(trim.get("modelName"), _digest_string(group.get("modelName"), "Unknown model"))
    trim_name = _digest_string(trim.get("trimName"), _digest_string(trim.get("fullTrimName"), "Unnamed trim"))
    if allow_context_identity:
        brand = _context_string(source_context, "brand") or brand
        model_name = _context_string(source_context, "model") or model_name
    brand = _digest_string(identity_override.get("brand"), brand)
    model_name = _digest_string(identity_override.get("modelName"), model_name)
    trim_name = _digest_string(identity_override.get("trimName"), trim_name)
    if identity_override:
        full_trim_name = _digest_string(identity_override.get("fullTrimName"), _digest_compact_name([model_name, trim_name]))
    else:
        full_trim_name = _digest_string(trim.get("fullTrimName"), _digest_compact_name([model_name, trim_name]))
        if allow_context_identity and full_trim_name == trim_name:
            full_trim_name = _digest_compact_name([model_name, trim_name])
    market = _digest_string(trim.get("market"), _digest_string(trim.get("country"), _digest_string(profile.get("country")))) or None
    model_year = _digest_string(trim.get("modelYear"), _digest_string(profile.get("modelYear"))) or None
    if allow_context_identity:
        market = _context_string(source_context, "country") or _context_string(source_context, "market") or market
        model_year = _context_string(source_context, "modelYear") or model_year
    market = _digest_string(identity_override.get("market"), _digest_string(identity_override.get("country"), market or "")) or None
    model_year = _digest_string(identity_override.get("modelYear"), model_year or "") or None
    energy_type, drivetrain, engine = _digest_trim_powertrain_fields(trim, profile)
    energy_type = _digest_string(identity_override.get("energyType"), energy_type or "") or None
    drivetrain = _digest_string(identity_override.get("drivetrain"), drivetrain or "") or None
    engine = _digest_string(identity_override.get("engine"), engine or "") or None
    material_no = _digest_string(trim.get("materialNo"), _digest_string(profile.get("materialNo"))) or None
    sales_version = _digest_string(trim.get("salesVersion"), _digest_string(profile.get("configurationVersion"))) or None
    material_no = _digest_string(identity_override.get("materialNo"), material_no or "") or None
    sales_version = _digest_string(identity_override.get("salesVersion"), sales_version or "") or None
    identity_key = build_identity_key(
        material_no=material_no,
        vehicle_code=sales_version,
        market=market,
        model_year=model_year,
        trim_name=trim_name,
    ) or full_trim_name
    identity_key = _digest_compact_name([identity_key, f"source:{source_batch.import_batch_id}"])
    existing = repo.get_vehicle_trim_by_source_full_name(
        session,
        source_batch.import_batch_id,
        full_trim_name,
    )
    if existing is None:
        existing = repo.get_vehicle_trim_by_config_version_source_full_name(
            session,
            source_batch.import_batch_id,
            full_trim_name,
        )
    if existing is None:
        vehicle_trim = VehicleTrim(
            source_upload_id=source_batch.import_batch_id,
            identity_key=identity_key,
            material_no=material_no,
            vehicle_code=sales_version,
            market=market,
            brand=brand,
            model_name=model_name,
            trim_name=trim_name,
            full_trim_name=full_trim_name,
            model_year=model_year,
            energy_type=energy_type,
            drivetrain=drivetrain,
            engine=engine,
            status=_digest_string(trim.get("sourceStatus"), "active"),
        )
        repo.add_vehicle_trim(session, vehicle_trim)
        session.flush()
        created = True
    else:
        vehicle_trim = existing
        repo.acquire_config_trim_lock(session, vehicle_trim.trim_id)
        vehicle_trim.source_upload_id = source_batch.import_batch_id
        vehicle_trim.identity_key = identity_key
        vehicle_trim.material_no = material_no
        vehicle_trim.vehicle_code = sales_version
        vehicle_trim.market = market
        vehicle_trim.brand = brand
        vehicle_trim.model_name = model_name
        vehicle_trim.trim_name = trim_name
        vehicle_trim.model_year = model_year
        vehicle_trim.energy_type = energy_type or vehicle_trim.energy_type
        vehicle_trim.drivetrain = drivetrain or vehicle_trim.drivetrain
        vehicle_trim.engine = engine or vehicle_trim.engine
        vehicle_trim.status = _digest_string(trim.get("sourceStatus"), vehicle_trim.status)
        created = False
    latest_version = repo.get_latest_config_version_for_trim(
        session,
        vehicle_trim.trim_id,
        for_update=True,
    )
    version = ConfigVersion(
        trim_id=vehicle_trim.trim_id,
        identity_key=identity_key,
        material_no=material_no,
        vehicle_code=sales_version,
        market=market,
        model_year=model_year,
        brand=brand,
        model_name=model_name,
        trim_name=trim_name,
        status="draft",
        version_no=(latest_version.version_no + 1) if latest_version else 1,
        source_upload_id=source_batch.import_batch_id,
        parent_version_id=latest_version.version_id if latest_version else None,
        created_by=user.name,
        snapshot_values=[],
        snapshot_feature_count=0,
    )
    session.add(version)
    return vehicle_trim, created, version


@router.post("/source/snapshots/{source_id}/digest-groups/{group_id}/draft")
def create_draft_from_source_digest_group(
    source_id: UUID,
    group_id: str,
    payload: SourceDigestDraftCreate | None = Body(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    source_batch = repo.get_import_batch(session, source_id)
    if source_batch is None or source_batch.domain != SOURCE_IMPORT_DOMAIN:
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    if source_batch.import_status == "purged":
        raise HTTPException(status_code=404, detail="Source snapshot not found")
    if source_batch.import_status == "trashed":
        raise HTTPException(
            status_code=409,
            detail="Source snapshot is in trash; restore it before creating editable config columns",
        )
    if _file_extension(source_batch.source_file_name) not in MATRIX_UPLOAD_EXTENSIONS | {".csv", ".tsv", ".html", ".htm", ".pdf", ".png", ".jpg", ".jpeg", ".webp"}:
        raise HTTPException(status_code=422, detail="Digest draft requires a workbook, tabular, text-based PDF, or OCR-readable image source")
    source_path = Path(source_batch.source_file_path)
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="Source file not found")
    source_context_links = _active_source_context_links_for_draft(session, source_batch.import_batch_id)
    digest = build_source_digest(source_path, source_batch.source_file_name)
    if not digest or digest.get("status") == "failed":
        raise HTTPException(status_code=422, detail="Source digest is not ready")
    if digest.get("digestType") not in {"workbook", "tabular", "pdf_text", "pdf_ocr", "image_ocr"}:
        raise HTTPException(status_code=422, detail="Digest draft requires a workbook, tabular, PDF text/OCR, or image OCR compare group")
    group = next((item for item in digest.get("compareGroups", []) if item.get("groupId") == group_id), None)
    if group is None:
        raise HTTPException(status_code=404, detail="Digest compare group not found")
    all_trims = [item for item in group.get("trims", []) if isinstance(item, dict)]
    all_rows = [item for item in group.get("rows", []) if isinstance(item, dict)]
    selected_digest_trim_ids = _normalise_digest_draft_trim_ids(payload)
    trim_identity_overrides = _normalise_digest_draft_trim_identity_overrides(payload)
    trim_index_by_id = {
        _digest_string(trim.get("trimId")): index
        for index, trim in enumerate(all_trims)
        if _digest_string(trim.get("trimId"))
    }
    missing_override_trim_ids = [trim_id for trim_id in trim_identity_overrides if trim_id not in trim_index_by_id]
    if missing_override_trim_ids:
        raise HTTPException(status_code=422, detail=f"Digest trim identity override not found: {missing_override_trim_ids[0]}")
    if selected_digest_trim_ids:
        if len(selected_digest_trim_ids) < 2 or len(selected_digest_trim_ids) > 4:
            raise HTTPException(status_code=422, detail="Digest draft requires 2-4 selected trim columns")
        missing_trim_ids = [trim_id for trim_id in selected_digest_trim_ids if trim_id not in trim_index_by_id]
        if missing_trim_ids:
            raise HTTPException(status_code=422, detail=f"Selected digest trim not found: {missing_trim_ids[0]}")
        extra_override_trim_ids = [trim_id for trim_id in trim_identity_overrides if trim_id not in selected_digest_trim_ids]
        if extra_override_trim_ids:
            raise HTTPException(status_code=422, detail=f"Digest trim identity override is not selected: {extra_override_trim_ids[0]}")
        selected_indexes = [trim_index_by_id[trim_id] for trim_id in selected_digest_trim_ids]
        selected_trim_id_set = set(selected_digest_trim_ids)
        trims = [all_trims[index] for index in selected_indexes]
        rows = [
            _project_digest_row_values(row, selected_indexes, selected_trim_id_set)
            for row in all_rows
        ]
    else:
        trims = all_trims
        rows = all_rows
    if len(trims) < 2 or not rows:
        raise HTTPException(status_code=422, detail="Digest compare group is not importable")
    trims = _apply_digest_trim_identity_overrides(trims, trim_identity_overrides)
    source_context = _source_context_for_draft(source_context_links)

    repo.acquire_source_digest_lock(session, source_batch.import_batch_id, group_id)
    existing_features = repo.list_feature_catalog(session, limit=1000)
    next_order = max([feature.display_order for feature in existing_features] + [0]) + 1
    trim_by_digest_id: dict[str, VehicleTrim] = {}
    version_by_trim_id: dict[UUID, ConfigVersion] = {}
    created_trim_count = 0
    created_version_ids: list[str] = []
    for trim in trims:
        vehicle_trim, created, version = _upsert_digest_trim(
            session,
            trim=trim,
            group=group,
            source_batch=source_batch,
            user=user,
            source_context=source_context,
        )
        trim_by_digest_id[_digest_string(trim.get("trimId"), str(vehicle_trim.trim_id))] = vehicle_trim
        version_by_trim_id[vehicle_trim.trim_id] = version
        created_trim_count += 1 if created else 0
        session.flush()
        created_version_ids.append(str(version.version_id))

    created_feature_count = 0
    reused_feature_count = 0
    alias_matched_feature_count = 0
    semantic_alias_matched_feature_count = 0
    feature_match_reason_counts: dict[str, int] = {}
    feature_match_samples: list[dict[str, str]] = []
    inserted_value_count = 0
    updated_value_count = 0
    feature_by_row_index: dict[int, FeatureCatalog] = {}
    reserved_feature_ids: set[UUID] = set()
    for row_index, row in enumerate(rows):
        feature, created, match_reason = _digest_feature_for_row(
            session,
            row,
            next_order=next_order,
            catalog_features=existing_features,
        )
        if feature.feature_id in reserved_feature_ids:
            source_feature_code = _digest_string(
                row.get("featureCode"),
                f"digest_{next_order}",
            )
            exact_feature = repo.get_feature_catalog_by_code(
                session,
                source_feature_code,
            )
            if (
                exact_feature is not None
                and exact_feature.feature_id not in reserved_feature_ids
            ):
                feature = exact_feature
                created = False
                match_reason = "feature_code"
            else:
                collision_code = source_feature_code
                if exact_feature is not None:
                    collision_code = (
                        f"{source_feature_code}_{next_order}"
                    )
                feature = _create_digest_feature(
                    session,
                    row,
                    next_order=next_order,
                    feature_code=collision_code,
                )
                created = True
                match_reason = "created_semantic_collision"
        reserved_feature_ids.add(feature.feature_id)
        feature_by_row_index[row_index] = feature
        feature_match_reason_counts[match_reason] = feature_match_reason_counts.get(match_reason, 0) + 1
        if created:
            created_feature_count += 1
            next_order += 1
            existing_features.append(feature)
        else:
            reused_feature_count += 1
            if match_reason in {"alias", "semantic_alias"}:
                alias_matched_feature_count += 1
                if match_reason == "semantic_alias":
                    semantic_alias_matched_feature_count += 1
                if len(feature_match_samples) < 5:
                    feature_match_samples.append({
                        "sourceFeatureName": _digest_string(row.get("featureName"), _digest_string(row.get("featureCode"), "")),
                        "matchedFeatureName": feature.standard_field_name,
                        "matchedFeatureCode": feature.feature_code,
                        "matchReason": match_reason,
                    })

    for row_index, row in enumerate(rows):
        feature = feature_by_row_index[row_index]
        values = row.get("values") if isinstance(row.get("values"), list) else []
        for value_index, trim in enumerate(trims):
            vehicle_trim = trim_by_digest_id.get(_digest_string(trim.get("trimId")))
            if vehicle_trim is None:
                continue
            value = values[value_index] if value_index < len(values) and isinstance(values[value_index], dict) else {}
            raw_value = _digest_string(value.get("rawValue"))
            availability = _digest_string(value.get("availability")) or classify_availability(raw_value)[0]
            normalized = value.get("normalizedValue")
            normalized_value = normalized if isinstance(normalized, str) else classify_availability(raw_value)[1]
            unit = value.get("unit") if isinstance(value.get("unit"), str) else None
            source_row, source_column = _digest_source_ref(value)
            version = version_by_trim_id[vehicle_trim.trim_id]
            snapshot_values = (
                list(version.snapshot_values)
                if isinstance(version.snapshot_values, list)
                else []
            )
            snapshot_values.append({
                "featureId": str(feature.feature_id),
                "featureCode": feature.feature_code,
                "featureName": feature.standard_field_name,
                "category": feature.category,
                "rawValue": raw_value,
                "normalizedValue": normalized_value,
                "availability": availability,
                "unit": unit,
                "sourceRow": source_row,
                "sourceColumn": source_column,
                "sourceUploadId": str(source_batch.import_batch_id),
                "source": value.get("source") if isinstance(value.get("source"), dict) else None,
                "inferred": bool(value.get("inferred")),
                "inferenceReason": value.get("inferenceReason") if isinstance(value.get("inferenceReason"), str) else None,
                "confidence": value.get("confidence") if isinstance(value.get("confidence"), (int, float)) else None,
            })
            version.snapshot_values = snapshot_values
            version.snapshot_feature_count = len(snapshot_values)
            existing_value = repo.get_trim_feature_value_by_trim_feature(
                session,
                vehicle_trim.trim_id,
                feature.feature_id,
            )
            if existing_value is None:
                session.add(
                    TrimFeatureValue(
                        trim_id=vehicle_trim.trim_id,
                        feature_id=feature.feature_id,
                        raw_value=raw_value,
                        normalized_value=normalized_value,
                        availability=availability,
                        unit=unit,
                        source_row=source_row,
                        source_column=source_column,
                        source_upload_id=source_batch.import_batch_id,
                        version=1,
                        updated_by=user.name,
                    )
                )
                inserted_value_count += 1
            else:
                existing_value.raw_value = raw_value
                existing_value.normalized_value = normalized_value
                existing_value.availability = availability
                existing_value.unit = unit
                existing_value.source_row = source_row
                existing_value.source_column = source_column
                existing_value.source_upload_id = source_batch.import_batch_id
                existing_value.version += 1
                existing_value.updated_by = user.name
                updated_value_count += 1

    retained_feature_ids = [feature.feature_id for feature in feature_by_row_index.values()]
    deleted_value_count = sum(
        repo.delete_trim_feature_values_not_in(
            session,
            vehicle_trim.trim_id,
            retained_feature_ids,
        )
        for vehicle_trim in trim_by_digest_id.values()
    )
    value_record_count = inserted_value_count + updated_value_count
    try:
        session.commit()
    except Exception as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=f"Digest draft create failed: {exc}")

    trim_ids = [str(trim.trim_id) for trim in trim_by_digest_id.values()]
    return {
        "sourceId": str(source_batch.import_batch_id),
        "groupId": group_id,
        "importBatchId": str(source_batch.import_batch_id),
        **_digest_draft_source_payload(source_batch, digest, group),
        "trimIds": trim_ids,
        "compareTrimIds": trim_ids[:4],
        "trimCount": len(trim_ids),
        "createdTrimCount": created_trim_count,
        "reusedTrimCount": len(trim_ids) - created_trim_count,
        "featureCount": len(rows),
        "createdFeatureCount": created_feature_count,
        "reusedFeatureCount": reused_feature_count,
        "aliasMatchedFeatureCount": alias_matched_feature_count,
        "semanticAliasMatchedFeatureCount": semantic_alias_matched_feature_count,
        "featureMatchReasonCounts": feature_match_reason_counts,
        "featureMatchSamples": feature_match_samples,
        "valueRecordCount": value_record_count,
        "insertedValueCount": inserted_value_count,
        "updatedValueCount": updated_value_count,
        "deletedValueCount": deleted_value_count,
        "createdVersionIds": created_version_ids,
    }


@router.post("/matrix/upload/{upload_id}/parse")
def parse_uploaded_matrix(
    upload_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"assembled", "parsing"}:
        raise HTTPException(status_code=409, detail="Upload not assembled yet")

    source_path = Path(meta["assembledPath"])
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="Assembled file not found")

    # Load feature catalog for matching
    features = repo.list_feature_catalog(session, is_active=True, limit=1000)
    catalog_dicts = [
        {
            "category": f.category,
            "standard_field_name": f.standard_field_name,
            "feature_code": f.feature_code,
        }
        for f in features
    ]

    try:
        result = parse_config_matrix(source_path, feature_catalog=catalog_dicts or None)
    except Exception as exc:
        meta["status"] = "parse_failed"
        meta["error"] = str(exc)
        _save_session_meta(upload_id, meta)
        raise HTTPException(status_code=400, detail=f"Parse failed: {exc}")

    meta["status"] = "parsed"
    meta["preview"] = result["summary"]
    meta["warningCount"] = len(result["warnings"])
    meta["warnings"] = result["warnings"][:50]
    meta["unmatchedFeatureCount"] = len(result["unmatched_features"])
    meta["unmatchedFeatures"] = [
        {"category": c, "fieldName": f}
        for c, f in result["unmatched_features"][:50]
    ]
    _save_session_meta(upload_id, meta)

    return {
        "uploadId": upload_id,
        "summary": result["summary"],
        "trims": result["trims"],
        "categories": result["categories"],
        "warningCount": len(result["warnings"]),
        "warnings": result["warnings"][:50],
        "unmatchedFeatures": meta["unmatchedFeatures"],
        "sampleValues": result["values"][:20],
    }


@router.post("/matrix/upload/{upload_id}/match")
def match_upload_against_versions(
    upload_id: str, session: Session = Depends(get_db_session), _=Depends(require_min_role("editor")),
) -> dict:
    meta = _load_session_meta(upload_id)
    if meta["status"] != "parsed":
        raise HTTPException(status_code=409, detail="Upload must be parsed before matching")
    source_path = Path(meta["assembledPath"])
    features = repo.list_feature_catalog(session, is_active=True, limit=1000)
    catalog_dicts = [{"category": f.category, "standard_field_name": f.standard_field_name, "feature_code": f.feature_code} for f in features]
    result = parse_config_matrix(source_path, feature_catalog=catalog_dicts or None)
    from sqlalchemy import select as sa_select, desc as sa_desc

    match_results = []
    for t in result["trims"]:
        ik = build_identity_key(vehicle_code=t.get("model_name", ""), trim_name=t.get("trim_name", ""))
        info = {"fullTrimName": t.get("full_trim_name", ""), "identityKey": ik, "isNew": True, "hasDraftConflict": False}
        if ik:
            pub = session.execute(sa_select(ConfigVersion).where(ConfigVersion.identity_key == ik, ConfigVersion.status == "published").order_by(sa_desc(ConfigVersion.created_at_utc)).limit(1)).scalars().first()
            if pub: info.update({"isNew": False, "latestPublishedVersionId": str(pub.version_id)})
            draft = session.execute(sa_select(ConfigVersion).where(ConfigVersion.identity_key == ik, ConfigVersion.status == "draft").limit(1)).scalars().first()
            if draft: info.update({"hasDraftConflict": True, "existingDraftVersionId": str(draft.version_id)})
        match_results.append(info)

    meta["matchResults"] = match_results; meta["status"] = "matched"; _save_session_meta(upload_id, meta)
    new = sum(1 for m in match_results if m["isNew"]); existing = sum(1 for m in match_results if not m["isNew"]); conflicts = sum(1 for m in match_results if m["hasDraftConflict"])
    return {"uploadId": upload_id, "matchResults": match_results, "summary": {"totalTrims": len(match_results), "newTrims": new, "existingTrims": existing, "draftConflicts": conflicts}}


@router.get("/matrix/upload/{upload_id}/preview")
def get_upload_preview(
    upload_id: str, session: Session = Depends(get_db_session), _=Depends(require_min_role("editor")),
) -> dict:
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"matched", "parsed"}:
        raise HTTPException(status_code=409, detail="Upload must be matched or parsed first")
    source_path = Path(meta["assembledPath"])
    features = repo.list_feature_catalog(session, is_active=True, limit=1000)
    catalog_dicts = [{"category": f.category, "standard_field_name": f.standard_field_name, "feature_code": f.feature_code} for f in features]
    result = parse_config_matrix(source_path, feature_catalog=catalog_dicts or None)
    match_results = meta.get("matchResults", []); mr_by_trim = {m["fullTrimName"]: m for m in match_results}
    from sqlalchemy import select as sa_select

    diff_rows: list[dict] = []
    for t in result["trims"]:
        ftn = t.get("full_trim_name", ""); mr = mr_by_trim.get(ftn, {})
        if mr.get("isNew") or not mr.get("latestPublishedVersionId"): continue
        pub_ver = session.get(ConfigVersion, UUID(mr["latestPublishedVersionId"]))
        if pub_ver is None: continue
        pub_vals = repo.list_trim_feature_values(session, pub_ver.trim_id)
        pub_map: dict[str, dict] = {}
        for v in pub_vals:
            feat = session.get(FeatureCatalog, v.feature_id)
            pub_map[feat.feature_code if feat else ""] = {"rawValue": v.raw_value, "availability": v.availability}
        for nv in [v for v in result["values"] if v["full_trim_name"] == ftn]:
            code = nv.get("feature_code", ""); old = pub_map.get(code)
            dt = "NEW_FEATURE" if old is None else ("CHANGED" if old["rawValue"] != nv["raw_value"] or old["availability"] != nv["availability"] else "UNCHANGED")
            diff_rows.append({"category": nv.get("category", ""), "featureName": nv.get("feature_name", ""), "featureCode": code, "oldValue": old["rawValue"] if old else None, "oldAvailability": old["availability"] if old else None, "newValue": nv.get("raw_value", ""), "newAvailability": nv.get("availability", ""), "diffType": dt})

    changed = sum(1 for d in diff_rows if d["diffType"] == "CHANGED"); new_feat = sum(1 for d in diff_rows if d["diffType"] == "NEW_FEATURE")
    return {"uploadId": upload_id, "summary": {**result["summary"], "newTrims": sum(1 for m in match_results if m.get("isNew")), "existingTrims": sum(1 for m in match_results if not m.get("isNew")), "draftConflicts": sum(1 for m in match_results if m.get("hasDraftConflict")), "changedValues": changed, "newFeatures": new_feat}, "diffRows": diff_rows[:200], "warnings": result["warnings"][:50], "unmatchedFeatures": [{"category": c, "fieldName": f} for c, f in result["unmatched_features"][:50]]}


@router.post("/matrix/upload/{upload_id}/confirm")
def confirm_upload_as_draft(
    upload_id: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    raise HTTPException(
        status_code=410,
        detail="Legacy matrix write is disabled; register the file as a Source Snapshot and create columns from Source Digest",
    )
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"matched", "parsed"}:
        raise HTTPException(status_code=409, detail="Upload must be parsed/matched first")
    source_path = Path(meta["assembledPath"])
    features = repo.list_feature_catalog(session, is_active=True, limit=1000)
    catalog_dicts = [{"category": f.category, "standard_field_name": f.standard_field_name, "feature_code": f.feature_code} for f in features]
    result = parse_config_matrix(source_path, feature_catalog=catalog_dicts or None)

    import_batch = ImportBatch(domain="engineering_config", source_file_name=meta["fileName"], source_file_path=meta["assembledPath"], import_status="pending", row_count=0, error_count=0, triggered_by=user.name, started_at_utc=datetime.now(timezone.utc))
    repo.add_import_batch(session, import_batch); session.flush()
    feature_by_code = {f.feature_code: f for f in features}; created = 0

    for t in result["trims"]:
        ik = build_identity_key(vehicle_code=t.get("model_name", ""), trim_name=t.get("trim_name", "")) or ""
        existing = repo.get_vehicle_trim_by_source_full_name(
            session,
            import_batch.import_batch_id,
            t["full_trim_name"],
        )
        if existing: existing.identity_key = ik; existing.source_upload_id = import_batch.import_batch_id; trim_obj = existing
        else: trim_obj = VehicleTrim(source_upload_id=import_batch.import_batch_id, identity_key=ik, brand=t["brand"], model_name=t["model_name"], trim_name=t["trim_name"], full_trim_name=t["full_trim_name"], status="active"); repo.add_vehicle_trim(session, trim_obj); session.flush()

        ver = ConfigVersion(trim_id=trim_obj.trim_id, identity_key=ik, brand=t["brand"], model_name=t["model_name"], trim_name=t["trim_name"], status="draft", version_no=1, source_upload_id=import_batch.import_batch_id, created_by=user.name)
        session.add(ver); session.flush(); created += 1

        for v in [v for v in result["values"] if v["full_trim_name"] == t["full_trim_name"]]:
            feat = feature_by_code.get(v.get("feature_code", ""))
            if not feat: continue
            session.add(TrimFeatureValue(trim_id=trim_obj.trim_id, feature_id=feat.feature_id, raw_value=v.get("raw_value", ""), normalized_value=v.get("normalized_value"), availability=v.get("availability", "UNKNOWN"), unit=v.get("unit"), source_row=v.get("source_row", 0), source_column=v.get("source_column", ""), source_upload_id=import_batch.import_batch_id, version=1))

    import_batch.import_status = "success"; import_batch.row_count = result["summary"]["value_record_count"]; import_batch.finished_at_utc = datetime.now(timezone.utc)
    try: session.commit()
    except Exception as exc: session.rollback(); raise HTTPException(status_code=409, detail=f"Confirm failed: {exc}")
    meta["status"] = "draft_created"; _save_session_meta(upload_id, meta)
    return {"uploadId": upload_id, "importBatchId": str(import_batch.import_batch_id), "createdVersions": created, "valueRecordCount": result["summary"]["value_record_count"]}


@router.post("/versions/{version_id}/publish")
def publish_version(
    version_id: str,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("admin")),
) -> dict:
    ver = session.get(ConfigVersion, UUID(version_id))
    if ver is None: raise HTTPException(status_code=404, detail="Version not found")
    if ver.status != "draft": raise HTTPException(status_code=409, detail="Only draft versions can be published")
    if (
        not ver.snapshot_values
        or ver.snapshot_feature_count <= 0
        or ver.snapshot_feature_count != len(ver.snapshot_values)
    ):
        raise HTTPException(status_code=409, detail="Version snapshot is missing; regenerate the draft from its source before publishing")
    repo.acquire_config_identity_lock(session, ver.identity_key)
    repo.acquire_config_trim_lock(session, ver.trim_id)
    latest_version = repo.get_latest_config_version_for_trim(
        session,
        ver.trim_id,
        for_update=True,
    )
    if latest_version is None or latest_version.version_id != ver.version_id:
        raise HTTPException(status_code=409, detail="Only the latest draft version can be published")
    previous_versions = [
        item
        for item in repo.list_published_config_versions_by_identity(session, ver.identity_key)
        if item.version_id != ver.version_id
    ]
    for previous in previous_versions:
        previous.status = "archived"
    if previous_versions:
        # The partial unique index permits one published version per identity.
        # Persist archival first so PostgreSQL cannot reorder both UPDATEs and
        # temporarily observe two published rows.
        session.flush()
    ver.status = "published"
    ver.published_by = user.name
    ver.published_at_utc = datetime.now(timezone.utc)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Another configuration version was published concurrently; refresh and retry",
        ) from exc
    archived_ids = [str(item.version_id) for item in previous_versions]
    return {
        "versionId": str(ver.version_id),
        "status": "published",
        "identityKey": ver.identity_key,
        "publishedBy": user.name,
        "archivedPreviousVersionId": archived_ids[0] if archived_ids else None,
        "archivedPreviousVersionIds": archived_ids,
    }


@router.post("/matrix/upload/{upload_id}/import")
def import_parsed_matrix(
    upload_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    raise HTTPException(
        status_code=410,
        detail="Legacy matrix write is disabled; register the file as a Source Snapshot and create columns from Source Digest",
    )
    meta = _load_session_meta(upload_id)
    if meta["status"] != "parsed":
        raise HTTPException(status_code=409, detail="Upload must be parsed before import")

    source_path = Path(meta["assembledPath"])
    features = repo.list_feature_catalog(session, is_active=True, limit=1000)
    catalog_dicts = [
        {
            "category": f.category,
            "standard_field_name": f.standard_field_name,
            "feature_code": f.feature_code,
        }
        for f in features
    ]

    result = parse_config_matrix(source_path, feature_catalog=catalog_dicts or None)

    # Create ImportBatch
    import_batch = ImportBatch(
        domain="engineering_config",
        source_file_name=meta["fileName"],
        source_file_path=meta["assembledPath"],
        import_status="pending",
        row_count=0,
        error_count=0,
        triggered_by="api",
        started_at_utc=datetime.now(timezone.utc),
    )
    repo.add_import_batch(session, import_batch)
    session.flush()

    # Build feature lookup by feature_code
    feature_by_code: dict[str, FeatureCatalog] = {}
    for f in features:
        feature_by_code[f.feature_code] = f

    # Upsert trims
    trim_by_full_name: dict[str, VehicleTrim] = {}
    for t in result["trims"]:
        existing = repo.get_vehicle_trim_by_source_full_name(
            session,
            import_batch.import_batch_id,
            t["full_trim_name"],
        )
        if existing:
            existing.source_upload_id = import_batch.import_batch_id
            existing.brand = t["brand"]
            existing.model_name = t["model_name"]
            existing.trim_name = t["trim_name"]
            trim_by_full_name[t["full_trim_name"]] = existing
        else:
            vt = VehicleTrim(
                source_upload_id=import_batch.import_batch_id,
                brand=t["brand"],
                model_name=t["model_name"],
                trim_name=t["trim_name"],
                full_trim_name=t["full_trim_name"],
                status="active",
            )
            repo.add_vehicle_trim(session, vt)
            session.flush()
            trim_by_full_name[t["full_trim_name"]] = vt

    session.flush()

    # Upsert feature values
    inserted = 0
    skipped = 0
    for v in result["values"]:
        trim = trim_by_full_name.get(v["full_trim_name"])
        if not trim:
            skipped += 1
            continue

        feat = None
        if v["feature_code"]:
            feat = feature_by_code.get(v["feature_code"])
        if not feat:
            # Try matching by category + feature_name
            for f in features:
                if f.category == v["category"] and f.standard_field_name == v["feature_name"]:
                    feat = f
                    break
        if not feat:
            skipped += 1
            continue

        tfv = TrimFeatureValue(
            trim_id=trim.trim_id,
            feature_id=feat.feature_id,
            raw_value=v["raw_value"],
            normalized_value=v["normalized_value"],
            availability=v["availability"],
            unit=v["unit"],
            source_row=v["source_row"],
            source_column=v["source_column"],
            source_upload_id=import_batch.import_batch_id,
            version=1,
        )
        session.add(tfv)
        inserted += 1

    import_batch.import_status = "success"
    import_batch.row_count = inserted
    import_batch.error_count = skipped
    import_batch.finished_at_utc = datetime.now(timezone.utc)

    try:
        session.commit()
    except Exception as exc:
        session.rollback()
        import_batch.import_status = "failed"
        import_batch.error_count = inserted
        session.commit()
        raise HTTPException(status_code=409, detail=f"Import failed: {exc}")

    return {
        "importBatchId": str(import_batch.import_batch_id),
        "insertedRows": inserted,
        "skippedRows": skipped,
        "trimCount": len(trim_by_full_name),
    }


# ── Trims ──────────────────────────────────────────────────────────


@router.get("/recommendations/competitors")
def list_competitor_recommendations(
    country: str | None = Query(default=None),
    market: str | None = Query(default=None),
    model_name: str | None = Query(default=None),
    model: str | None = Query(default=None),
    powertrain: str | None = Query(default=None),
    segment: str | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=10),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    country_value = _safe_context_text(country or market, 80)
    model_value = _safe_context_text(model_name or model, 120)
    powertrain_value = _safe_context_text(powertrain, 80)
    segment_value = _safe_context_text(segment, 80)
    advanced_analysis_country = _advanced_analysis_country(country_value)
    advanced_analysis_segment = _advanced_analysis_segment(segment_value)
    if not country_value or not model_value:
        return {
            "country": country_value,
            "modelName": model_value,
            "rows": 0,
            "items": [],
            "message": "missing_model_or_country",
        }

    try:
        from app.services.advanced_analysis_service import compute_competitor_set

        competitor_payload = compute_competitor_set(
            country=advanced_analysis_country,
            fuel_types=[powertrain_value] if powertrain_value else None,
            segments=[advanced_analysis_segment] if advanced_analysis_segment else None,
            target_model=model_value,
            top_n=max(5, limit * 2),
        )
    except Exception as exc:
        return {
            "country": country_value,
            "modelName": model_value,
            "powertrain": powertrain_value,
            "segment": segment_value,
            "rows": 0,
            "items": [],
            "message": "advanced_analysis_unavailable",
            "errorMessage": str(exc),
        }

    competitors = competitor_payload.get("competitors")
    if not isinstance(competitors, list):
        return {
            "country": country_value,
            "modelName": model_value,
            "powertrain": powertrain_value,
            "segment": segment_value,
            "rows": 0,
            "items": [],
            "message": competitor_payload.get("error") or "no_competitors",
            "source": {
                "type": "advanced_analysis_competitor_set",
                "analysisMode": competitor_payload.get("analysis_mode"),
                "targetPeriod": competitor_payload.get("target_period"),
                "basePeriod": competitor_payload.get("base_period"),
                "advancedAnalysisCountry": advanced_analysis_country,
                "advancedAnalysisSegment": advanced_analysis_segment,
            },
        }

    items: list[dict] = []
    seen_competitor_models: set[str] = set()
    try:
        source_digest_candidate_batches = repo.list_source_snapshot_batches(
            session,
            SOURCE_IMPORT_DOMAIN,
            country=country_value,
            segment=segment_value,
            query=None,
            limit=SOURCE_SNAPSHOT_LIST_CANDIDATE_LIMIT,
        )
    except Exception:
        source_digest_candidate_batches = []

    for index, row in enumerate(competitors):
        if not isinstance(row, dict):
            continue
        competitor_model = _safe_context_text(row.get("model"), 120)
        if not competitor_model or _same_text(competitor_model, model_value):
            continue
        competitor_model_key = competitor_model.casefold()
        if competitor_model_key in seen_competitor_models:
            continue
        seen_competitor_models.add(competitor_model_key)
        profile = row.get("profile") if isinstance(row.get("profile"), dict) else {}
        make = _safe_context_text(row.get("make") or profile.get("make"), 80)
        config_trims = _list_recommended_config_trims(
            session,
            brand=make,
            model_name=competitor_model,
            market=country_value,
            energy_type=powertrain_value,
            limit=8,
        )
        trim_payloads = [_trim_payload(trim, session) for trim in config_trims]
        source_digest_coverage = _recommended_source_digest_coverage(
            session,
            brand=make,
            model_name=competitor_model,
            market=country_value,
            energy_type=powertrain_value,
            segment=segment_value,
            candidate_batches=source_digest_candidate_batches,
        )
        source_digest_available = bool(source_digest_coverage["available"])
        items.append(
            {
                "rank": len(items) + 1,
                "sourceRank": index + 1,
                "modelName": competitor_model,
                "brand": make,
                "profile": profile,
                "role": row.get("role"),
                "similarityScore": _compact_metric(row.get("similarity_score")),
                "salesTarget": _compact_metric(row.get("sales_tgt")),
                "salesBase": _compact_metric(row.get("sales_base")),
                "deltaVolume": _compact_metric(row.get("dV")),
                "shareTarget": _compact_metric(row.get("share_tgt")),
                "shareChange": _compact_metric(row.get("share_change")),
                "pureShareShift": _compact_metric(row.get("pure_share_shift")),
                "estimatedFlow": _compact_metric(row.get("estimated_flow")),
                "sharedDimensions": row.get("shared_dims") if isinstance(row.get("shared_dims"), list) else [],
                "matchEvidence": row.get("match_evidence") if isinstance(row.get("match_evidence"), list) else [],
                "recommendationReason": _recommendation_reason(row),
                "configAvailable": len(trim_payloads) > 0,
                "configTrimCount": len(trim_payloads),
                "sourceDigestAvailable": source_digest_available,
                "sourceDigestSourceCount": source_digest_coverage["sourceCount"],
                "sourceDigestGroupCount": source_digest_coverage["groupCount"],
                "sourceDigestTrimCount": source_digest_coverage["trimCount"],
                "sourceDigestSearchQuery": source_digest_coverage["searchQuery"],
                "sourceDigestMatches": source_digest_coverage["matches"],
                "trims": trim_payloads,
                "nextAction": "select_config_trim" if trim_payloads else "create_from_source_digest" if source_digest_available else "upload_source",
            }
        )
        if len(items) >= limit:
            break

    return {
        "country": country_value,
        "modelName": model_value,
        "powertrain": powertrain_value,
        "segment": segment_value,
        "rows": len(items),
        "items": items,
        "message": "ok" if items else "no_competitors",
        "source": {
            "type": "advanced_analysis_competitor_set",
            "analysisMode": competitor_payload.get("analysis_mode"),
            "targetPeriod": competitor_payload.get("target_period"),
            "basePeriod": competitor_payload.get("base_period"),
            "scopeModelCount": competitor_payload.get("scope_model_count"),
            "advancedAnalysisCountry": advanced_analysis_country,
            "advancedAnalysisSegment": advanced_analysis_segment,
        },
    }


@router.get("/trims")
def list_trims(
    brand: str | None = Query(default=None),
    model_name: str | None = Query(default=None),
    trim_name: str | None = Query(default=None),
    market: str | None = Query(default=None),
    model_year: str | None = Query(default=None),
    energy_type: str | None = Query(default=None),
    source: str | None = Query(default=None),
    has_material_no: bool | None = Query(default=None),
    q: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    trim_filters = {
        "brand": brand,
        "model_name": model_name,
        "trim_name": trim_name,
        "market": market,
        "model_year": model_year,
        "energy_type": energy_type,
        "source_query": source,
        "has_material_no": has_material_no,
        "status": status,
        "query": q,
    }
    trims = repo.list_vehicle_trims(
        session,
        **trim_filters,
        limit=limit,
    )
    total_rows = repo.count_vehicle_trims(session, **trim_filters)
    source_ids = {
        source_upload_id
        for trim in trims
        if (source_upload_id := getattr(trim, "source_upload_id", None)) is not None
    }
    source_batches = repo.list_import_batches_by_ids(session, source_ids) if source_ids else {}
    return {
        "rows": total_rows,
        "items": [
            {
                **_trim_payload(t, source_batch=source_batches.get(getattr(t, "source_upload_id", None))),
                "status": t.status,
            }
            for t in trims
        ],
    }


@router.delete("/trims/trash")
def clear_trim_trash(
    market: str | None = Query(default=None),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    market_value = market.strip() if market else ""
    if not market_value:
        raise HTTPException(status_code=400, detail="market is required to clear trim trash")
    purged_count = repo.clear_vehicle_trim_trash(session, market=market_value)
    session.commit()
    return {
        "cleared": purged_count,
        "market": market_value,
        "message": f"Cleared {purged_count} trashed config trims by {user.name}.",
    }


@router.get("/trims/{trim_id}")
def get_trim_detail(
    trim_id: str,
    category: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=2000),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    trim = repo.get_vehicle_trim(session, UUID(trim_id))
    if trim is None:
        raise HTTPException(status_code=404, detail="Trim not found")

    values = repo.list_trim_feature_values(session, trim.trim_id, category, limit)

    grouped: dict[str, list[dict]] = {}
    for v in values:
        feat = session.get(FeatureCatalog, v.feature_id)
        cat = feat.category if feat else "Unknown"
        if cat not in grouped:
            grouped[cat] = []
        grouped[cat].append(
            {
                "valueId": str(v.value_id),
                "featureId": str(v.feature_id),
                "featureCode": feat.feature_code if feat else None,
                "featureName": feat.standard_field_name if feat else "Unknown",
                "rawValue": v.raw_value,
                "normalizedValue": v.normalized_value,
                "availability": v.availability,
                "unit": v.unit,
                "version": v.version,
            }
        )

    return {
        "trim": {**_trim_payload(trim, session), "status": trim.status},
        "featuresByCategory": grouped,
        "categoryCount": len(grouped),
    }


def _compare_version_for_scope(
    versions: list[ConfigVersion],
    version_scope: Literal["published", "latest"],
) -> ConfigVersion | None:
    if not versions:
        return None
    if version_scope == "latest":
        return versions[0]
    published = next(
        (
            version
            for version in versions
            if version.status == "published" and _version_has_immutable_snapshot(version)
        ),
        None,
    )
    if published is not None:
        return published
    return next(
        (version for version in versions if _version_has_immutable_snapshot(version)),
        None,
    )


def _version_has_immutable_snapshot(version: ConfigVersion) -> bool:
    snapshot_values = getattr(version, "snapshot_values", None)
    if not isinstance(snapshot_values, list) or not snapshot_values:
        return False
    feature_count = getattr(version, "snapshot_feature_count", None)
    return feature_count is None or feature_count == len(snapshot_values)


def _snapshot_feature_identity(item: dict) -> str:
    feature_id = str(item.get("featureId") or "").strip()
    if feature_id:
        return f"feature:{feature_id}"
    return "legacy:" + "\x1f".join(
        str(item.get(key) or "").strip().casefold()
        for key in ("category", "featureCode", "featureName")
    )


def _snapshot_value_adapter(
    item: dict,
    *,
    live_value: TrimFeatureValue | None,
    editable: bool,
    source_upload_id: UUID | None,
) -> SimpleNamespace:
    source = dict(item["source"]) if isinstance(item.get("source"), dict) else None
    inferred = bool(item.get("inferred"))
    inference_reason = item.get("inferenceReason") if isinstance(item.get("inferenceReason"), str) else None
    confidence = item.get("confidence") if isinstance(item.get("confidence"), (int, float)) else None
    if source is not None:
        source["inferred"] = inferred
        if inference_reason is not None:
            source["inferenceReason"] = inference_reason
        if confidence is not None:
            source["confidence"] = confidence
    raw_value = str(item.get("rawValue") or "")
    return SimpleNamespace(
        value_id=getattr(live_value, "value_id", None) if editable else None,
        raw_value=raw_value,
        normalized_value=item.get("normalizedValue") if isinstance(item.get("normalizedValue"), str) else None,
        availability=str(item.get("availability") or "UNKNOWN"),
        unit=item.get("unit") if isinstance(item.get("unit"), str) else None,
        version=getattr(live_value, "version", None) if editable else None,
        source_upload_id=source_upload_id,
        source_row=int(item.get("sourceRow") or 0),
        source_column=str(item.get("sourceColumn") or ""),
        snapshot_source=source,
    )


def _compare_trim_version_payload(
    trim: VehicleTrim,
    *,
    session: Session,
    selected_version: ConfigVersion | None,
    versions: list[ConfigVersion],
    source_batches: dict[UUID, ImportBatch],
    requested_scope: Literal["published", "latest"],
) -> dict:
    source_upload_id = (
        selected_version.source_upload_id
        if selected_version is not None
        else getattr(trim, "source_upload_id", None)
    )
    payload = _trim_payload(
        trim,
        session,
        source_batch=source_batches.get(source_upload_id),
        identity_version=selected_version,
    )
    published_available = any(
        version.status == "published" and _version_has_immutable_snapshot(version)
        for version in versions
    )
    draft_available = any(version.status == "draft" for version in versions)
    payload.update({
        "configVersionId": str(selected_version.version_id) if selected_version else None,
        "configVersionNo": selected_version.version_no if selected_version else None,
        "configVersionStatus": selected_version.status if selected_version else "legacy",
        "draftVersionAvailable": draft_available,
        "publishedVersionAvailable": published_available,
        "versionFallback": bool(
            requested_scope == "published"
            and (
                selected_version is None
                or selected_version.status != "published"
            )
        ),
    })
    return payload


@router.get("/compare")
def compare_trims(
    trim_ids: str = Query(description="Comma-separated trim UUIDs, 2-4 trims"),
    differences_only: bool = Query(default=False),
    version_scope: Literal["published", "latest"] = Query(default="published"),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    try:
        ids = [UUID(tid.strip()) for tid in trim_ids.split(",") if tid.strip()]
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Compare trim IDs must be valid UUIDs") from exc
    if len(ids) < 2 or len(ids) > 4:
        raise HTTPException(status_code=400, detail="Compare requires 2-4 trim IDs")
    if len(set(ids)) != len(ids):
        raise HTTPException(status_code=400, detail="Compare requires 2-4 distinct trim IDs")

    return build_compare_facts(
        session,
        ids,
        differences_only=differences_only,
        version_scope=version_scope,
    )


def build_compare_facts(
    session: Session,
    ids: list[UUID],
    *,
    differences_only: bool,
    version_scope: Literal["published", "latest"],
) -> dict:

    trim_by_id = {trim.trim_id: trim for trim in repo.list_vehicle_trims_by_ids(session, ids)}
    trims: list[VehicleTrim] = []
    for tid in ids:
        trim = trim_by_id.get(tid)
        if trim is None:
            raise HTTPException(status_code=404, detail=f"Trim not found: {tid}")
        if trim.status in {"trashed", "purged"}:
            raise HTTPException(status_code=404, detail=f"Trim not found: {tid}")
        trims.append(trim)

    versions_by_trim = repo.list_config_versions_for_trims(session, ids)
    selected_versions = {
        trim_id: _compare_version_for_scope(versions_by_trim.get(trim_id, []), version_scope)
        for trim_id in ids
    }
    live_values_by_trim: dict[UUID, dict[str, tuple[TrimFeatureValue, FeatureCatalog]]] = {
        trim_id: {} for trim_id in ids
    }
    source_upload_ids: set[UUID] = set()
    for value, feature in repo.list_trim_feature_values_with_features(session, ids):
        identity = f"feature:{feature.feature_id}"
        live_values_by_trim.setdefault(value.trim_id, {})[identity] = (value, feature)
        source_upload_id = getattr(value, "source_upload_id", None)
        if source_upload_id is not None:
            source_upload_ids.add(source_upload_id)
    for version in selected_versions.values():
        if version is not None and version.source_upload_id is not None:
            source_upload_ids.add(version.source_upload_id)
    source_batches = repo.list_import_batches_by_ids(session, source_upload_ids)

    trim_value_maps: dict[UUID, dict[str, object]] = {trim_id: {} for trim_id in ids}
    features_lookup: dict[str, object] = {}
    editable_trim_ids = {
        trim_id
        for trim_id in ids
        if (
            version_scope == "latest"
            and selected_versions.get(trim_id) is not None
            and selected_versions[trim_id].status == "draft"
            and versions_by_trim.get(trim_id)
            and versions_by_trim[trim_id][0].version_id == selected_versions[trim_id].version_id
        )
    }
    for trim in trims:
        version = selected_versions.get(trim.trim_id)
        versions = versions_by_trim.get(trim.trim_id, [])
        snapshot_values = version.snapshot_values if version is not None else None
        if not isinstance(snapshot_values, list):
            for identity, (value, feature) in live_values_by_trim.get(trim.trim_id, {}).items():
                trim_value_maps[trim.trim_id][identity] = value
                features_lookup[identity] = feature
            continue
        editable = trim.trim_id in editable_trim_ids
        live_map = live_values_by_trim.get(trim.trim_id, {})
        for order, item in enumerate(snapshot_values):
            if not isinstance(item, dict):
                continue
            identity = _snapshot_feature_identity(item)
            live_pair = live_map.get(identity)
            live_value = live_pair[0] if live_pair is not None else None
            feature = live_pair[1] if live_pair is not None else SimpleNamespace(
                feature_id=str(item.get("featureId") or "") or None,
                feature_code=str(item.get("featureCode") or identity),
                standard_field_name=str(item.get("featureName") or item.get("featureCode") or identity),
                category=str(item.get("category") or "Unknown"),
                display_order=order,
            )
            features_lookup[identity] = feature
            trim_value_maps[trim.trim_id][identity] = _snapshot_value_adapter(
                item,
                live_value=live_value,
                editable=editable,
                source_upload_id=version.source_upload_id if version is not None else None,
            )
    trim_values = [(trim, trim_value_maps.get(trim.trim_id, {})) for trim in trims]

    # Feature codes are labels, not identities; separate catalog rows may share one.
    all_feature_ids: list[str] = []
    seen_feature_ids: set[str] = set()
    for _, vmap in trim_values:
        for feature_id in vmap:
            if feature_id not in seen_feature_ids:
                seen_feature_ids.add(feature_id)
                all_feature_ids.append(feature_id)

    # Sort by category then display_order
    all_feature_ids.sort(
        key=lambda feature_id: (
            features_lookup[feature_id].category if feature_id in features_lookup else "ZZZ",
            features_lookup[feature_id].display_order if feature_id in features_lookup else 9999,
        )
    )

    all_rows: list[dict] = []
    for feature_id in all_feature_ids:
        feat = features_lookup.get(feature_id)
        values_list: list[dict | None] = []
        for trim, vmap in trim_values:
            v = vmap.get(feature_id)
            if v:
                source_payload = _source_ref_payload_for_value(v, session, source_batches)
                manual_override = (getattr(v, "source_column", None) or "").strip().lower() == "manual"
                inferred = bool(source_payload.get("inferred")) if isinstance(source_payload, dict) else False
                inference_reason = source_payload.get("inferenceReason") if isinstance(source_payload, dict) else None
                confidence = source_payload.get("confidence") if isinstance(source_payload, dict) else None
                display_value = _api_display_value(v.raw_value, v.availability)
                if inferred and v.availability == "NOT_AVAILABLE":
                    display_value = "不配备*"
                values_list.append(
                    {
                        "valueId": (
                            str(v.value_id)
                            if trim.trim_id in editable_trim_ids and v.value_id is not None
                            else None
                        ),
                        "rawValue": v.raw_value,
                        "normalizedValue": v.normalized_value,
                        "availability": v.availability,
                        "unit": v.unit,
                        "version": v.version if trim.trim_id in editable_trim_ids else None,
                        "valueState": _api_value_state(v.raw_value, v.availability),
                        "displayValue": display_value,
                        "inferred": inferred,
                        "inferenceReason": str(inference_reason) if inference_reason is not None else None,
                        "confidence": confidence if isinstance(confidence, (int, float)) else None,
                        "manualOverride": manual_override,
                        "source": source_payload,
                    }
                )
            else:
                values_list.append(None)

        comparison_type = _classify_comparison_type(values_list)
        unique_trim_ids = [
            str(trims[idx].trim_id)
            for idx, value in enumerate(values_list)
            if comparison_type == "UNIQUE_TO_TRIM"
            and _is_available_compare_cell(value)
        ]
        all_rows.append(
            {
                "category": feat.category if feat else "Unknown",
                "featureId": str(feat.feature_id) if feat and feat.feature_id else None,
                "featureCode": feat.feature_code if feat else str(feature_id),
                "featureName": feat.standard_field_name if feat else str(feature_id),
                "comparisonType": comparison_type,
                "uniqueTrimIds": unique_trim_ids,
                "businessNote": _business_note(comparison_type, values_list, trims),
                "values": values_list,
            }
        )

    rows = [
        row
        for row in all_rows
        if not differences_only or row["comparisonType"] != "COMMON_SAME"
    ]
    category_counts: dict[str, int] = {}
    for row in all_rows:
        category = str(row["category"])
        category_counts[category] = category_counts.get(category, 0) + 1
    unique_feature_count = sum(
        1 for row in all_rows if row["comparisonType"] == "UNIQUE_TO_TRIM"
    )
    partial_available_count = sum(
        1 for row in all_rows if row["comparisonType"] == "PARTIAL_AVAILABLE"
    )
    difference_count = sum(
        1 for row in all_rows if row["comparisonType"] != "COMMON_SAME"
    )
    confirmed_difference_count = sum(
        1
        for row in all_rows
        if row["comparisonType"] not in {"COMMON_SAME", "MISSING_OR_UNKNOWN", "MISSING_UNKNOWN"}
    )
    inferred_difference_count = sum(
        1
        for row in all_rows
        if row["comparisonType"] not in {"COMMON_SAME", "MISSING_OR_UNKNOWN", "MISSING_UNKNOWN"}
        and any(bool(value.get("inferred")) for value in row["values"] if isinstance(value, dict))
    )
    summary = {
        "totalFeatures": len(all_rows),
        "shownFeatures": len(rows),
        "commonSameCount": sum(
            1 for row in all_rows if row["comparisonType"] == "COMMON_SAME"
        ),
        "differentValueCount": sum(
            1 for row in all_rows if row["comparisonType"] == "DIFFERENT_VALUE"
        ),
        "uniqueFeatureCount": unique_feature_count,
        "partialAvailableCount": partial_available_count,
        "uniqueOrPartialCount": unique_feature_count + partial_available_count,
        "missingOrUnknownCount": sum(
            1 for row in all_rows if row["comparisonType"] == "MISSING_OR_UNKNOWN"
        ),
        "confirmedDifferenceCount": confirmed_difference_count,
        "rawConfirmedDifferenceCount": max(0, confirmed_difference_count - inferred_difference_count),
        "inferredDifferenceCount": inferred_difference_count,
        "differenceCount": difference_count,
        "differenceCategories": sorted(
            {
                str(row["category"])
                for row in all_rows
                if row["comparisonType"] != "COMMON_SAME"
            }
        ),
        "categoryCounts": category_counts,
    }
    groups_by_category: dict[str, list[dict]] = {}
    for row in rows:
        category = str(row["category"])
        groups_by_category.setdefault(category, []).append(row)

    trim_payloads = [
        _compare_trim_version_payload(
            trim,
            session=session,
            selected_version=selected_versions.get(trim.trim_id),
            versions=versions_by_trim.get(trim.trim_id, []),
            source_batches=source_batches,
            requested_scope=version_scope,
        )
        for trim in trims
    ]
    version_fallback_count = sum(1 for trim in trim_payloads if trim["versionFallback"])
    uses_draft = any(trim["configVersionStatus"] == "draft" for trim in trim_payloads)

    return {
        "versionScope": version_scope,
        "usesDraft": uses_draft,
        "versionFallbackCount": version_fallback_count,
        "trims": trim_payloads,
        "summary": summary,
        "groups": [
            {
                "category": category,
                "items": category_rows,
            }
            for category, category_rows in groups_by_category.items()
        ],
        "rows": rows,
        "totalFeatures": len(all_feature_ids),
        "shownFeatures": len(rows),
    }


def _canonical_compare_request(
    payload: EngineeringConfigCompareFactRequest,
) -> tuple[list[UUID], str, dict]:
    try:
        ids = [UUID(trim_id.strip()) for trim_id in payload.trimIds]
        base_trim_id = str(UUID(payload.baseTrimId.strip()))
        target_trim_id = payload.filters.targetTrimId
        if target_trim_id:
            target_trim_id = str(UUID(target_trim_id.strip()))
    except (AttributeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Compare trim IDs must be valid UUIDs") from exc
    if len(set(ids)) != len(ids):
        raise HTTPException(status_code=400, detail="Compare requires 2-4 distinct trim IDs")
    if UUID(base_trim_id) not in ids:
        raise HTTPException(status_code=400, detail="Base trim must be part of trimIds")
    if target_trim_id and UUID(target_trim_id) not in ids:
        raise HTTPException(status_code=400, detail="Target trim must be part of trimIds")
    if target_trim_id == base_trim_id:
        raise HTTPException(status_code=400, detail="Target trim must differ from base trim")
    filters = payload.filters.model_dump()
    filters["targetTrimId"] = target_trim_id
    return ids, base_trim_id, filters


def _canonical_compare_facts_for_request(
    session: Session,
    payload: EngineeringConfigCompareFactRequest,
) -> tuple[dict, str, dict]:
    if payload.factSource is not None:
        return _local_digest_compare_facts(payload)
    ids, base_trim_id, filters = _canonical_compare_request(payload)
    return (
        build_compare_facts(
            session,
            ids,
            differences_only=False,
            version_scope=payload.versionScope,
        ),
        base_trim_id,
        filters,
    )


def _local_digest_compare_facts(
    payload: EngineeringConfigCompareFactRequest,
) -> tuple[dict, str, dict]:
    source = payload.factSource
    if source is None or source.kind != "local_workbook_digest":
        raise HTTPException(status_code=400, detail="Unsupported compare fact source")

    trim_ids = [trim_id.strip() for trim_id in payload.trimIds]
    base_trim_id = payload.baseTrimId.strip()
    target_trim_id = payload.filters.targetTrimId.strip() if payload.filters.targetTrimId else None
    if any(not trim_id for trim_id in trim_ids) or len(set(trim_ids)) != len(trim_ids):
        raise HTTPException(status_code=400, detail="Compare requires 2-4 distinct trim IDs")
    if base_trim_id not in trim_ids:
        raise HTTPException(status_code=400, detail="Base trim must be part of trimIds")
    if target_trim_id and target_trim_id not in trim_ids:
        raise HTTPException(status_code=400, detail="Target trim must be part of trimIds")
    if target_trim_id == base_trim_id:
        raise HTTPException(status_code=400, detail="Target trim must differ from base trim")

    digest = _load_local_source_workbook_digest(source.fileName)
    groups = digest.get("compareGroups")
    if not isinstance(groups, list):
        raise HTTPException(status_code=422, detail="Local workbook digest has no compare groups")
    group = next(
        (
            candidate
            for candidate in groups
            if isinstance(candidate, dict) and str(candidate.get("groupId") or "") == source.groupId
        ),
        None,
    )
    if group is None:
        raise HTTPException(status_code=404, detail="Local workbook digest group not found")

    source_trims = group.get("trims")
    if not isinstance(source_trims, list):
        raise HTTPException(status_code=422, detail="Local workbook digest group has no trims")
    trim_index_by_id = {
        str(trim.get("trimId") or trim.get("trimName") or ""): index
        for index, trim in enumerate(source_trims)
        if isinstance(trim, dict)
    }
    try:
        selected_indexes = [trim_index_by_id[trim_id] for trim_id in trim_ids]
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Local digest trim not found: {exc.args[0]}") from exc

    selected_trims = [
        _local_digest_trim_payload(
            source_trims[index],
            group=group,
            digest=digest,
            file_name=source.fileName,
        )
        for index in selected_indexes
    ]
    source_rows = group.get("rows") if isinstance(group.get("rows"), list) else []
    rows: list[dict] = []
    groups_by_category: dict[str, list[dict]] = {}
    for source_row in source_rows:
        if not isinstance(source_row, dict):
            continue
        source_values = source_row.get("values") if isinstance(source_row.get("values"), list) else []
        row = {
            **source_row,
            "uniqueTrimIds": [
                trim_id
                for trim_id in source_row.get("uniqueTrimIds", [])
                if isinstance(trim_id, str) and trim_id in trim_ids
            ],
            "values": [
                source_values[index] if index < len(source_values) else None
                for index in selected_indexes
            ],
        }
        rows.append(row)
        category = str(row.get("category") or "Unknown")
        groups_by_category.setdefault(category, []).append(row)

    summary = dict(group.get("summary") or {})
    summary["totalFeatures"] = len(rows)
    summary["shownFeatures"] = len(rows)
    filters = payload.filters.model_dump()
    filters["targetTrimId"] = target_trim_id
    return (
        {
            "versionScope": payload.versionScope,
            "usesDraft": False,
            "versionFallbackCount": 0,
            "factSource": source.model_dump(),
            "trims": selected_trims,
            "summary": summary,
            "groups": [
                {"category": category, "items": items}
                for category, items in groups_by_category.items()
            ],
            "rows": rows,
            "totalFeatures": len(rows),
            "shownFeatures": len(rows),
        },
        base_trim_id,
        filters,
    )


def _local_digest_trim_payload(
    trim: dict,
    *,
    group: dict,
    digest: dict,
    file_name: str,
) -> dict:
    trim_id = str(trim.get("trimId") or trim.get("trimName") or "")
    material_no = trim.get("materialNo")
    has_material_no = bool(material_no or trim.get("hasMaterialNo"))
    market = trim.get("market") or trim.get("country")
    return {
        "trimId": trim_id,
        "fullTrimName": trim.get("fullTrimName") or trim.get("trimName") or trim_id,
        "brand": trim.get("brand") or digest.get("brand") or "本品资料",
        "modelName": trim.get("modelName") or group.get("modelName") or digest.get("modelName"),
        "trimName": trim.get("trimName") or trim_id,
        "market": market,
        "country": trim.get("country") or market,
        "modelYear": trim.get("modelYear"),
        "energyType": trim.get("energyType"),
        "drivetrain": trim.get("drivetrain"),
        "engine": trim.get("engine"),
        "vehicleCode": trim.get("vehicleCode") or material_no,
        "materialNo": material_no,
        "identityKey": trim.get("identityKey") or material_no or trim_id,
        "salesVersion": trim.get("salesVersion") or trim.get("trimName"),
        "sourceUploadId": None,
        "sourceFileName": file_name,
        "sourceFilePath": None,
        "sourceCreatedBy": None,
        "importStatus": "digest_ready",
        "hasMaterialNo": has_material_no,
        "dataOrigin": trim.get("dataOrigin") or ("own_catalog" if has_material_no else "external_or_scraped"),
        "profile": trim.get("profile"),
        "configVersionId": None,
        "configVersionNo": None,
        "configVersionStatus": "digest_preview",
        "draftVersionAvailable": False,
        "publishedVersionAvailable": False,
        "versionFallback": False,
        "msrp": None,
        "targetPrice": None,
    }


@router.post("/compare/export/xlsx")
def export_compare_xlsx(
    payload: EngineeringConfigCompareFactRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> StreamingResponse:
    export_facts = _canonical_export_facts(session, payload)
    output = generate_engineering_config_compare_xlsx(export_facts)
    filename = compare_export_filename(export_facts)
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": (
                f"attachment; filename=\"config-compare.xlsx\"; filename*=UTF-8''{quote(filename)}"
            )
        },
    )


@router.post("/compare/export/pdf")
def export_compare_pdf(
    payload: EngineeringConfigCompareFactRequest,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> StreamingResponse:
    export_facts = _canonical_export_facts(session, payload)
    output = generate_engineering_config_compare_pdf(export_facts)
    filename = compare_pdf_export_filename(export_facts)
    return StreamingResponse(
        output,
        media_type="application/pdf",
        headers={
            "Content-Disposition": (
                f"attachment; filename=\"config-compare.pdf\"; filename*=UTF-8''{quote(filename)}"
            )
        },
    )


def _canonical_export_facts(
    session: Session,
    payload: EngineeringConfigCompareFactRequest,
) -> dict:
    compare_facts, base_trim_id, filters = _canonical_compare_facts_for_request(
        session,
        payload,
    )
    try:
        business_summary_result = None
        if filters.get("includeBusinessSummary"):
            composer_facts = build_business_summary_facts(
                compare_facts,
                base_trim_id=base_trim_id,
                filters=filters,
            )
            business_summary_result = compose_engineering_config_business_summary(composer_facts)
        return build_compare_export_facts(
            compare_facts,
            base_trim_id=base_trim_id,
            filters=filters,
            business_summary_result=business_summary_result,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ── Edit ───────────────────────────────────────────────────────────


@router.patch("/values/{value_id}")
def update_feature_value(
    value_id: str,
    payload: ConfigFeatureValueUpdate,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    current = repo.get_trim_feature_value(session, UUID(value_id))
    if current is None:
        raise HTTPException(status_code=404, detail="Feature value not found")
    _, draft_version = _require_editable_trim_for_value(session, current)

    old_raw = current.raw_value
    old_availability = current.availability
    old_source_upload_id = getattr(current, "source_upload_id", None)
    old_source_row = getattr(current, "source_row", 0)
    old_source_column = getattr(current, "source_column", "manual")

    raw_value = payload.raw_value
    if raw_value is None:
        raise HTTPException(status_code=422, detail="raw_value is required")
    if raw_value == current.raw_value:
        manual_override = (old_source_column or "").strip().lower() == "manual"
        return {
            "valueId": value_id,
            "rawValue": current.raw_value,
            "availability": current.availability,
            "normalizedValue": current.normalized_value,
            "valueState": _api_value_state(current.raw_value, current.availability),
            "displayValue": _api_display_value(current.raw_value, current.availability),
            "version": current.version,
            "manualOverride": manual_override,
            "unchanged": True,
        }
    availability, normalized, unit = classify_availability(raw_value)
    audit_comment = (payload.comment or "").strip() or "Manual value update"

    success = repo.update_trim_feature_value(
        session,
        UUID(value_id),
        raw_value=raw_value,
        normalized_value=normalized,
        availability=availability,
        unit=unit,
        updated_by=user.name,
        expected_version=payload.expected_version,
    )

    if not success:
        session.rollback()
        raise HTTPException(status_code=409, detail="Version conflict — refresh and retry")

    # Audit log
    audit = ConfigAuditLog(
        entity_type="trim_feature_value",
        entity_id=UUID(value_id),
        field_name="raw_value",
        old_value=old_raw,
        new_value=raw_value,
        changed_by=user.name,
        source="manual",
        comment=audit_comment,
    )
    repo.add_audit_log(session, audit)

    if old_availability != availability:
        audit2 = ConfigAuditLog(
            entity_type="trim_feature_value",
            entity_id=UUID(value_id),
            field_name="availability",
            old_value=old_availability,
            new_value=availability,
            changed_by=user.name,
            source="manual",
            comment=audit_comment,
        )
        repo.add_audit_log(session, audit2)

    if draft_version is not None:
        feature = session.get(FeatureCatalog, current.feature_id)
        if feature is None:
            session.rollback()
            raise HTTPException(status_code=409, detail="Feature catalog row is missing")
        _upsert_draft_snapshot_value(
            draft_version,
            feature,
            raw_value=raw_value,
            normalized_value=normalized,
            availability=availability,
            unit=unit,
        )

    source_changes = (
        ("source_upload_id", str(old_source_upload_id) if old_source_upload_id else None, None),
        ("source_row", str(old_source_row), "0"),
        ("source_column", old_source_column, "manual"),
    )
    for field_name, old_value, new_value in source_changes:
        if old_value == new_value:
            continue
        repo.add_audit_log(session, ConfigAuditLog(
            entity_type="trim_feature_value",
            entity_id=UUID(value_id),
            field_name=field_name,
            old_value=old_value,
            new_value=new_value,
            changed_by=user.name,
            source="manual",
            comment=audit_comment,
        ))

    session.commit()
    return {
        "valueId": value_id,
        "rawValue": raw_value,
        "availability": availability,
        "normalizedValue": normalized,
        "valueState": _api_value_state(raw_value, availability),
        "displayValue": _api_display_value(raw_value, availability),
        "version": payload.expected_version + 1,
        "manualOverride": True,
    }


@router.post("/values")
def create_feature_value(
    payload: ConfigFeatureValueCreate, session: Session = Depends(get_db_session), user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    trim_id = UUID(payload.trim_id)
    trim = _require_editable_trim(repo.get_vehicle_trim(session, trim_id), trim_id)
    draft_version = _require_editable_config_version(session, trim)
    feat = session.get(FeatureCatalog, UUID(payload.feature_id))
    if feat is None: raise HTTPException(status_code=404, detail="Feature not found")
    availability, normalized, unit = classify_availability(payload.raw_value)
    tfv = TrimFeatureValue(trim_id=trim.trim_id, feature_id=feat.feature_id, raw_value=payload.raw_value, normalized_value=normalized, availability=availability, unit=unit, source_row=0, source_column="manual", source_upload_id=None, version=1, updated_by=user.name)
    session.add(tfv)
    session.flush()
    _upsert_draft_snapshot_value(
        draft_version,
        feat,
        raw_value=payload.raw_value,
        normalized_value=normalized,
        availability=availability,
        unit=unit,
    )
    repo.add_audit_log(session, ConfigAuditLog(entity_type="trim_feature_value", entity_id=tfv.value_id, field_name="raw_value", new_value=payload.raw_value, changed_by=user.name, source="manual", comment="Created"))
    try: session.commit(); session.refresh(tfv)
    except Exception as exc: session.rollback(); raise HTTPException(status_code=409, detail=f"Create failed: {exc}")
    return {
        "valueId": str(tfv.value_id),
        "rawValue": tfv.raw_value,
        "availability": availability,
        "normalizedValue": normalized,
        "valueState": _api_value_state(tfv.raw_value, availability),
        "displayValue": _api_display_value(tfv.raw_value, availability),
        "manualOverride": True,
        "version": tfv.version,
    }


@router.delete("/values/{value_id}")
def delete_feature_value(
    value_id: str,
    comment: str | None = Query(default=None, max_length=500),
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    current = repo.get_trim_feature_value(session, UUID(value_id))
    if current is None: raise HTTPException(status_code=404, detail="Feature value not found")
    _, draft_version = _require_editable_trim_for_value(session, current)
    audit_comment = (comment or "").strip() or "Deleted configuration value"
    deleted_fields = (
        ("raw_value", current.raw_value),
        ("availability", current.availability),
        ("source_upload_id", str(current.source_upload_id) if current.source_upload_id else None),
        ("source_row", str(current.source_row)),
        ("source_column", current.source_column),
    )
    success = repo.delete_trim_feature_value(session, UUID(value_id))
    if not success: raise HTTPException(status_code=404, detail="Feature value not found")
    _remove_draft_snapshot_value(draft_version, current.feature_id)
    for field_name, old_value in deleted_fields:
        repo.add_audit_log(session, ConfigAuditLog(
            entity_type="trim_feature_value",
            entity_id=UUID(value_id),
            field_name=field_name,
            old_value=old_value,
            new_value=None,
            changed_by=user.name,
            source="manual",
            comment=audit_comment,
        ))
    session.commit()
    return {"valueId": value_id, "deleted": True}


@router.patch("/trims/{trim_id}")
def update_vehicle_trim(
    trim_id: str,
    payload: VehicleTrimUpdate,
    session: Session = Depends(get_db_session),
    user: UserContext = Depends(require_min_role("editor")),
) -> dict:
    trim_uuid = UUID(trim_id)
    current = _require_editable_trim(repo.get_vehicle_trim(session, trim_uuid), trim_uuid)
    draft_version = _require_editable_config_version(session, current)
    refresh = getattr(session, "refresh", None)
    if callable(refresh):
        refresh(current)

    submitted = payload.model_dump(exclude={"comment"}, exclude_unset=True)
    if not submitted:
        raise HTTPException(status_code=400, detail="At least one trim field is required")

    required_identity_fields = {"brand", "model_name", "trim_name", "full_trim_name"}
    updates: dict[str, str | None] = {}
    for field_name, raw_value in submitted.items():
        normalized_value = raw_value.strip() if isinstance(raw_value, str) else raw_value
        if field_name in required_identity_fields and not normalized_value:
            raise HTTPException(status_code=400, detail=f"{field_name} cannot be blank")
        updates[field_name] = normalized_value or None

    changes = [
        (field_name, getattr(current, field_name), new_value)
        for field_name, new_value in updates.items()
        if getattr(current, field_name) != new_value
    ]
    if not changes:
        return {**_trim_payload(current, session), "unchanged": True}

    trim = repo.update_vehicle_trim(session, trim_uuid, **updates)
    if trim is None:
        raise HTTPException(status_code=404, detail="Trim not found")
    if draft_version is not None:
        for field_name in (
            "identity_key",
            "material_no",
            "vehicle_code",
            "market",
            "model_year",
            "brand",
            "model_name",
            "trim_name",
        ):
            setattr(draft_version, field_name, getattr(trim, field_name))
    for field_name, old_value, new_value in changes:
        repo.add_audit_log(
            session,
            ConfigAuditLog(
                entity_type="vehicle_trim",
                entity_id=trim_uuid,
                field_name=field_name,
                old_value=None if old_value is None else str(old_value),
                new_value=None if new_value is None else str(new_value),
                changed_by=user.name,
                source="manual",
                comment=payload.comment.strip(),
            ),
        )
    try:
        session.commit()
        session.refresh(trim)
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=409,
            detail="Trim identity conflicts with another column in this source",
        ) from exc
    return _trim_payload(trim, session)


@router.get("/audit-log")
def get_audit_log(
    entity_type: str | None = Query(default=None),
    entity_id: UUID | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    logs = repo.list_audit_logs(session, entity_type, entity_id, limit)
    return {
        "rows": len(logs),
        "items": [
            {
                "auditId": str(l.audit_id),
                "entityType": l.entity_type,
                "entityId": str(l.entity_id),
                "fieldName": l.field_name,
                "oldValue": l.old_value,
                "newValue": l.new_value,
                "changedBy": l.changed_by,
                "changedAtUtc": l.changed_at_utc.isoformat(),
                "source": l.source,
                "comment": l.comment,
            }
            for l in logs
        ],
    }
