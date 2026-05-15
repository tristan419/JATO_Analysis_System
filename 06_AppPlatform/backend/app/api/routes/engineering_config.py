"""API routes for engineering configuration matrix upload, parse, and comparison."""

from __future__ import annotations

import json
import uuid as uuid_module
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app.api.schemas import ConfigFeatureValueUpdate
from app.core.config import PROJECT_ROOT
from app.core.security import require_min_role
from app.db.models import (
    ConfigAuditLog,
    FeatureCatalog,
    ImportBatch,
    TrimFeatureValue,
    VehicleTrim,
)
from app.db.session import get_db_session
from app.infra import engineering_config_repository as repo
from app.services.config_availability import classify_availability
from app.services.config_field_mapping_parser import parse_field_mapping
from app.services.engineering_config_matrix_parser import parse_config_matrix

router = APIRouter(prefix="/engineering-config", tags=["engineering_config"])

UPLOAD_SESSION_DIR = PROJECT_ROOT / "04_Processed_data" / "ops" / "eng_config_uploads"


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


# ── Feature Catalog ────────────────────────────────────────────────


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
    raise HTTPException(
        status_code=501,
        detail="Feature catalog upload requires multipart file upload. Use /upload/initiate pattern.",
    )


# ── Matrix Upload (chunked) ────────────────────────────────────────


@router.post("/matrix/upload/initiate")
def initiate_matrix_upload(
    file_name: str = Query(),
    total_size: int = Query(ge=1),
    chunk_size: int = Query(default=5 * 1024 * 1024, ge=1024, le=50 * 1024 * 1024),
    _=Depends(require_min_role("editor")),
) -> dict:
    upload_id = str(uuid_module.uuid4())
    total_chunks = (total_size + chunk_size - 1) // chunk_size
    meta = {
        "uploadId": upload_id,
        "fileName": file_name,
        "totalSize": total_size,
        "chunkSize": chunk_size,
        "totalChunks": total_chunks,
        "uploadedChunks": [],
        "status": "initiated",
        "createdAtUtc": datetime.now(timezone.utc).isoformat(),
    }
    _save_session_meta(upload_id, meta)
    return meta


@router.put("/matrix/upload/{upload_id}/parts/{part_number}")
async def upload_matrix_chunk(
    upload_id: str,
    part_number: int,
    request: Request,
    _=Depends(require_min_role("editor")),
) -> dict:
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"initiated", "uploading"}:
        raise HTTPException(status_code=409, detail="Upload session not in uploadable state")

    chunk_data = await request.body()
    part_path = _session_dir(upload_id) / f"part_{part_number:05d}"
    part_path.write_bytes(chunk_data)

    uploaded = set(meta.get("uploadedChunks", []))
    uploaded.add(part_number)
    meta["uploadedChunks"] = sorted(uploaded)
    meta["status"] = "uploading"
    _save_session_meta(upload_id, meta)

    return {"uploadId": upload_id, "partNumber": part_number, "receivedBytes": len(chunk_data)}


@router.post("/matrix/upload/{upload_id}/complete")
def complete_matrix_upload(
    upload_id: str,
    _=Depends(require_min_role("editor")),
) -> dict:
    meta = _load_session_meta(upload_id)
    if len(meta["uploadedChunks"]) != meta["totalChunks"]:
        raise HTTPException(
            status_code=400,
            detail=f"Missing chunks: {meta['totalChunks'] - len(meta['uploadedChunks'])} remaining",
        )

    assembled_path = _session_dir(upload_id) / meta["fileName"]
    with assembled_path.open("wb") as out:
        for i in range(meta["totalChunks"]):
            part_path = _session_dir(upload_id) / f"part_{i:05d}"
            out.write(part_path.read_bytes())

    meta["status"] = "assembled"
    meta["assembledPath"] = str(assembled_path)
    _save_session_meta(upload_id, meta)

    return {**meta, "next": f"/engineering-config/matrix/upload/{upload_id}/parse"}


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


@router.post("/matrix/upload/{upload_id}/import")
def import_parsed_matrix(
    upload_id: str,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
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
        existing = repo.get_vehicle_trim_by_full_name(session, t["full_trim_name"])
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


@router.get("/trims")
def list_trims(
    brand: str | None = Query(default=None),
    model_name: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    trims = repo.list_vehicle_trims(session, brand, model_name, status, limit)
    return {
        "rows": len(trims),
        "items": [
            {
                "trimId": str(t.trim_id),
                "brand": t.brand,
                "modelName": t.model_name,
                "trimName": t.trim_name,
                "fullTrimName": t.full_trim_name,
                "energyType": t.energy_type,
                "drivetrain": t.drivetrain,
                "engine": t.engine,
                "modelYear": t.model_year,
                "status": t.status,
            }
            for t in trims
        ],
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
        "trim": {
            "trimId": str(trim.trim_id),
            "brand": trim.brand,
            "modelName": trim.model_name,
            "trimName": trim.trim_name,
            "fullTrimName": trim.full_trim_name,
            "energyType": trim.energy_type,
            "drivetrain": trim.drivetrain,
            "engine": trim.engine,
            "modelYear": trim.model_year,
            "status": trim.status,
        },
        "featuresByCategory": grouped,
        "categoryCount": len(grouped),
    }


@router.get("/compare")
def compare_trims(
    trim_ids: str = Query(description="Comma-separated trim UUIDs, 2-4 trims"),
    differences_only: bool = Query(default=False),
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("viewer")),
) -> dict:
    ids = [UUID(tid.strip()) for tid in trim_ids.split(",") if tid.strip()]
    if len(ids) < 2 or len(ids) > 4:
        raise HTTPException(status_code=400, detail="Compare requires 2-4 trim IDs")

    trims = []
    trim_values: list[tuple[VehicleTrim, dict[str, TrimFeatureValue]]] = []
    for tid in ids:
        trim = repo.get_vehicle_trim(session, tid)
        if trim is None:
            raise HTTPException(status_code=404, detail=f"Trim not found: {tid}")
        trims.append(trim)
        vals = repo.list_trim_feature_values(session, tid)
        val_map: dict[str, TrimFeatureValue] = {}
        for v in vals:
            feat = session.get(FeatureCatalog, v.feature_id)
            key = feat.feature_code if feat else ""
            val_map[key] = v
        trim_values.append((trim, val_map))

    # Collect all feature codes across all trims
    all_feature_codes: list[str] = []
    seen_codes: set[str] = set()
    for _, vmap in trim_values:
        for code in vmap:
            if code not in seen_codes:
                seen_codes.add(code)
                all_feature_codes.append(code)

    # Build comparison rows
    features_lookup: dict[str, FeatureCatalog] = {}
    for code in all_feature_codes:
        feat = repo.get_feature_catalog_by_code(session, code)
        if feat:
            features_lookup[code] = feat

    # Sort by category then display_order
    all_feature_codes.sort(
        key=lambda c: (
            features_lookup[c].category if c in features_lookup else "ZZZ",
            features_lookup[c].display_order if c in features_lookup else 9999,
        )
    )

    rows: list[dict] = []
    for code in all_feature_codes:
        feat = features_lookup.get(code)
        values_list: list[dict | None] = []
        for _, vmap in trim_values:
            v = vmap.get(code)
            if v:
                values_list.append(
                    {
                        "valueId": str(v.value_id),
                        "rawValue": v.raw_value,
                        "normalizedValue": v.normalized_value,
                        "availability": v.availability,
                        "unit": v.unit,
                    }
                )
            else:
                values_list.append(None)

        if differences_only:
            # Hide row if all values are the same
            non_null = [x for x in values_list if x is not None]
            if len(non_null) >= 2:
                first_avail = non_null[0]["availability"]
                first_val = non_null[0]["rawValue"]
                if all(
                    x["availability"] == first_avail and x["rawValue"] == first_val
                    for x in non_null
                ):
                    continue

        rows.append(
            {
                "category": feat.category if feat else "Unknown",
                "featureCode": code,
                "featureName": feat.standard_field_name if feat else code,
                "values": values_list,
            }
        )

    return {
        "trims": [
            {
                "trimId": str(t.trim_id),
                "fullTrimName": t.full_trim_name,
                "brand": t.brand,
                "modelName": t.model_name,
            }
            for t in trims
        ],
        "rows": rows,
        "totalFeatures": len(all_feature_codes),
        "shownFeatures": len(rows),
    }


# ── Edit ───────────────────────────────────────────────────────────


@router.patch("/values/{value_id}")
def update_feature_value(
    value_id: str,
    payload: ConfigFeatureValueUpdate,
    session: Session = Depends(get_db_session),
    _=Depends(require_min_role("editor")),
) -> dict:
    current = repo.get_trim_feature_value(session, UUID(value_id))
    if current is None:
        raise HTTPException(status_code=404, detail="Feature value not found")

    old_raw = current.raw_value
    old_availability = current.availability

    raw_value = payload.raw_value
    if raw_value is not None:
        availability, normalized, unit = classify_availability(raw_value)
    else:
        availability = current.availability
        normalized = current.normalized_value

    success = repo.update_trim_feature_value(
        session,
        UUID(value_id),
        raw_value=raw_value or current.raw_value,
        normalized_value=normalized,
        availability=availability,
        updated_by=payload.updated_by or "api",
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
        changed_by=payload.updated_by,
        source="manual",
        comment=payload.comment,
    )
    repo.add_audit_log(session, audit)

    if old_availability != availability:
        audit2 = ConfigAuditLog(
            entity_type="trim_feature_value",
            entity_id=UUID(value_id),
            field_name="availability",
            old_value=old_availability,
            new_value=availability,
            changed_by=payload.updated_by,
            source="manual",
            comment=payload.comment,
        )
        repo.add_audit_log(session, audit2)

    session.commit()
    return {"valueId": value_id, "availability": availability, "normalizedValue": normalized}


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
