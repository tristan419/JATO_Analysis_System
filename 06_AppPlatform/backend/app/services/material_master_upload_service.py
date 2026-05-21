"""Upload orchestration for Material Master XLSX files.

Manages the chunked upload session lifecycle: initiate → chunk → complete
→ parse → preview → publish.  Uses filesystem-based session state
matching the engineering_config upload pattern.
"""

from __future__ import annotations

import json
import uuid as uuid_module
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.config import PROJECT_ROOT
from app.services.material_master_parser import parse_material_master_xlsx
from app.services.order_genius_service import preview_parsed_upload, publish_baseline

UPLOAD_SESSION_DIR = PROJECT_ROOT / "04_Processed_data" / "ops" / "order_genius_uploads"


def _session_dir(upload_id: str) -> Path:
    return UPLOAD_SESSION_DIR / upload_id


def _session_meta_path(upload_id: str) -> Path:
    return _session_dir(upload_id) / "session.json"


def _load_session_meta(upload_id: str) -> dict:
    path = _session_meta_path(upload_id)
    if not path.exists():
        raise FileNotFoundError(f"Upload session {upload_id} not found")
    return json.loads(path.read_text())


def _save_session_meta(upload_id: str, meta: dict) -> None:
    _session_dir(upload_id).mkdir(parents=True, exist_ok=True)
    _session_meta_path(upload_id).write_text(
        json.dumps(meta, ensure_ascii=False, default=str)
    )


def initiate_upload(
    file_name: str,
    total_size: int,
    chunk_size: int = 5 * 1024 * 1024,
) -> dict:
    """Create a new chunked upload session."""
    upload_id = str(uuid_module.uuid4())
    total_chunks = (total_size + chunk_size - 1) // chunk_size
    meta = {
        "upload_id": upload_id,
        "file_name": file_name,
        "total_size": total_size,
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "uploaded_chunks": [],
        "status": "initiated",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _save_session_meta(upload_id, meta)
    return meta


def get_session(upload_id: str) -> dict:
    return _load_session_meta(upload_id)


def upload_chunk(upload_id: str, part_number: int, data: bytes) -> dict:
    """Write a single chunk to disk."""
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"initiated", "uploading"}:
        raise ValueError("Upload session not in uploadable state")

    part_path = _session_dir(upload_id) / f"part_{part_number:05d}"
    part_path.write_bytes(data)

    if part_number not in meta["uploaded_chunks"]:
        meta["uploaded_chunks"].append(part_number)
    meta["status"] = "uploading"
    _save_session_meta(upload_id, meta)

    return {
        "upload_id": upload_id,
        "part_number": part_number,
        "received_bytes": len(data),
    }


def complete_upload(upload_id: str) -> dict:
    """Assemble all chunks into the final file."""
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"initiated", "uploading"}:
        raise ValueError("Upload session not in completable state")

    expected = set(range(meta["total_chunks"]))
    received = set(meta["uploaded_chunks"])
    missing = expected - received
    if missing:
        raise ValueError(
            f"Missing chunks: {sorted(missing)}. "
            f"Received {len(received)}/{meta['total_chunks']}"
        )

    assembled_dir = _session_dir(upload_id) / "assembled"
    assembled_dir.mkdir(parents=True, exist_ok=True)
    assembled_path = assembled_dir / meta["file_name"]

    with open(assembled_path, "wb") as out:
        for i in range(meta["total_chunks"]):
            part_path = _session_dir(upload_id) / f"part_{i:05d}"
            out.write(part_path.read_bytes())

    import hashlib

    file_hash = hashlib.sha256(assembled_path.read_bytes()).hexdigest()

    meta["status"] = "assembled"
    meta["assembled_path"] = str(assembled_path)
    meta["file_hash"] = file_hash
    _save_session_meta(upload_id, meta)

    return {
        "upload_id": upload_id,
        "status": "assembled",
        "file_hash": file_hash,
        "next": f"/v1/order-genius/material-master-uploads/{upload_id}/parse",
    }


def parse_upload(upload_id: str) -> dict:
    """Parse the assembled XLSX and store preview in session meta."""
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"assembled"}:
        raise ValueError("Upload must be assembled before parsing")

    assembled_path = Path(meta["assembled_path"])
    if not assembled_path.exists():
        raise FileNotFoundError(f"Assembled file not found: {assembled_path}")

    parsed = parse_material_master_xlsx(assembled_path)
    meta["parsed"] = parsed
    meta["status"] = "parsed"
    _save_session_meta(upload_id, meta)

    return {
        "upload_id": upload_id,
        "status": "parsed",
        "total_rows": len(parsed.get("rows", [])),
        "warnings_count": len(parsed.get("warnings", [])),
        "sheet_names": parsed.get("sheet_names", []),
        "next": f"/v1/order-genius/material-master-uploads/{upload_id}/preview",
    }


def preview_upload(session: Session, upload_id: str) -> dict:
    """Return parsed preview data, classified against current DB state."""
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"parsed", "previewed", "published"}:
        raise ValueError("Upload must be parsed before preview")

    parsed = meta.get("parsed", {})
    preview = preview_parsed_upload(session, parsed)
    preview["upload_id"] = upload_id

    meta["preview"] = preview
    meta["status"] = "previewed"
    _save_session_meta(upload_id, meta)

    return preview


def publish_upload(
    session: Session, upload_id: str, published_by: str, notes: str | None = None
) -> dict:
    """Publish the parsed upload as a new baseline."""
    meta = _load_session_meta(upload_id)
    if meta["status"] not in {"parsed", "previewed"}:
        raise ValueError("Upload must be parsed and previewed before publishing")

    parsed = meta.get("parsed", {})

    result = publish_baseline(
        session=session,
        parsed=parsed,
        source_file_name=meta["file_name"],
        source_file_hash=meta.get("file_hash"),
        published_by=published_by,
    )

    meta["status"] = "published"
    meta["published_at_utc"] = datetime.now(timezone.utc).isoformat()
    meta["published_by"] = published_by
    meta["notes"] = notes
    _save_session_meta(upload_id, meta)

    return result
