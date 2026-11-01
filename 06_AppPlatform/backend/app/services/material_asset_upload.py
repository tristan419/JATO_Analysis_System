"""Material upload adapter over the existing chunk toolkit, not a second upload engine."""
from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
from threading import BoundedSemaphore
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core.config import DATABASE_URL, PROJECT_ROOT
from app.db.models import MaterialAssetFile
from app.services import material_asset_service as svc
from upload_toolkit import upload_engine as engine
from upload_toolkit.file_utils import write_json

CHUNK_BYTES = 1024 * 1024
PROCESSING = BoundedSemaphore(1)


def root(username: str) -> Path:
    base = Path(os.getenv("APP_MATERIAL_ASSET_UPLOAD_ROOT", str(PROJECT_ROOT / "04_Processed_data" / "ops" / "material_asset_uploads")))
    scope = hashlib.sha256((DATABASE_URL + "|" + username).encode()).hexdigest()
    return base / scope


def lock(session: Session, upload_id: str) -> None:
    if not re.fullmatch(r"upload-[a-f0-9]{12}", upload_id):
        raise HTTPException(404, "Upload unavailable / 上传不可用")
    # Serialize save/cancel/chunks using the existing DB transaction; no job engine.
    if session.bind.dialect.name == "postgresql":
        key = int(hashlib.sha256(upload_id.encode()).hexdigest()[:15], 16)
        session.execute(select(func.pg_advisory_xact_lock(key)))


def get(session: Session, upload_id: str, username: str) -> dict:
    lock(session, upload_id)
    try:
        return engine.get_upload_session(root(username), upload_id)
    except RuntimeError as exc:
        raise HTTPException(404, "Upload unavailable; choose the file again / 上传不可用，请重新选择文件") from exc


def initiate(session: Session, body: dict, username: str, materials: dict) -> dict:
    material = svc.require_material(materials, body["materialKey"])
    kind = body["kind"]
    if kind == "config":
        if Path(body["filename"]).suffix.lower() != ".xlsx" or body["sizeBytes"] > svc.MAX_CONFIG_BYTES:
            raise HTTPException(422, "Choose an XLSX under 32 MiB / 请选择32MiB以内XLSX")
    elif kind in {"exterior", "interior"}:
        allowed = material["colours"] if kind == "exterior" else material["interiors"]
        if body["colourKey"] not in allowed:
            raise HTTPException(422, "Choose an existing BOM colour / 请选择既有BOM色码")
    else:
        raise HTTPException(422, "Invalid asset type / 资料类型无效")
    if body.get("groupId"):
        group = svc.group_access(session, UUID(body["groupId"]), materials, editor=True)
        if (group.brand, group.model_name, group.kind, group.colour_key) != (material["brand"], material["modelName"], kind, body["colourKey"]):
            raise HTTPException(422, "Photo group does not match this material colour / 图片组与物料色码不匹配")
    directory = root(username)
    engine.cleanup_expired_sessions(directory, max_age_days=2)
    resume_key = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
    previous = engine.find_resumable_session(directory, resume_key=resume_key, filename=body["filename"], size_bytes=body["sizeBytes"])
    if previous:
        return previous
    if sum(1 for _ in directory.glob("*/upload_state.json")) >= 64:
        raise HTTPException(409, "Cancel unused uploads first / 请先取消不用的上传")
    state = engine.create_upload_session(directory, filename=Path(body["filename"]).name, size_bytes=body["sizeBytes"], chunk_size=CHUNK_BYTES, resume_key=resume_key, triggered_by=username)
    state["target"] = body
    write_json(directory / state["uploadId"] / "upload_state.json", state)
    return state


def receive(session: Session, upload_id: str, username: str, part: int, content: bytes, digest: str) -> dict:
    state = get(session, upload_id, username)
    expected = min(CHUNK_BYTES, state["sizeBytes"] - (part - 1) * CHUNK_BYTES)
    if part < 1 or part > state["totalChunks"] or len(content) != expected:
        raise HTTPException(422, "Incorrect chunk size/index / 分片大小或序号错误")
    try:
        return engine.receive_chunk(root(username), upload_id, part, content, digest, expected_chunk_size=CHUNK_BYTES)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc


def assembled(session: Session, upload_id: str, username: str) -> tuple[dict, bytes]:
    state = get(session, upload_id, username)
    try:
        state = engine.complete_upload_session(root(username), upload_id, assembled_filename="source")
    except RuntimeError as exc:
        raise HTTPException(409, str(exc)) from exc
    content = (root(username) / upload_id / "assembled" / "source").read_bytes()
    digest_list = "".join(state["chunkDigests"][str(part)] for part in range(1, state["totalChunks"] + 1))
    if len(content) != state["sizeBytes"] or hashlib.sha256(digest_list.encode()).hexdigest() != state["target"]["fileFingerprint"]:
        raise HTTPException(422, "File checksum mismatch; cancel and choose the original / 文件校验失败，请取消并重新选择原文件")
    return state, content


def saved(session: Session, upload_id: str, username: str) -> MaterialAssetFile | None:
    return session.scalar(select(MaterialAssetFile).where(MaterialAssetFile.upload_id == upload_id, MaterialAssetFile.created_by == username))
