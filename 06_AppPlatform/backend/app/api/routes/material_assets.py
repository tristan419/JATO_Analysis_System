"""Material Assets API: editor writes, authorized Order Filler reads/downloads."""
from __future__ import annotations

import io
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask

from app.core.security import require_min_role, require_roles, ROLE_LEVEL
from app.db.models import MaterialAssetBinding, MaterialAssetFile, MaterialAssetGroup, ConfigVersion
from app.db.session import get_db_session
from app.services import material_asset_service as svc
from app.services import material_asset_upload as uploads
from upload_toolkit import upload_engine

router = APIRouter(prefix="/order-genius/material-assets", tags=["material_assets"])
reader = require_roles("order_filler", "editor", "admin", "developer")
writer = require_min_role("editor")


class LinkChange(BaseModel):
    materialKey: str
    kind: str
    colourKey: str = ""
    groupId: UUID | None = None
    versionId: UUID | None = None
    rowVersion: int = Field(ge=0)


class LinkBatch(BaseModel):
    changes: list[LinkChange] = Field(min_length=1, max_length=100)


class DeleteRequest(BaseModel):
    rowVersion: int = Field(ge=1)
    confirmed: bool = False


class UploadRequest(BaseModel):
    materialKey: str
    kind: str
    colourKey: str = ""
    groupId: UUID | None = None
    rowVersion: int = Field(ge=0)
    filename: str = Field(min_length=1, max_length=255)
    sizeBytes: int = Field(gt=0, le=svc.MAX_FILE_BYTES)
    fileFingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")


class SaveUpload(BaseModel):
    sourceColumn: int | None = Field(default=None, ge=0)
    sourceSheet: str | None = Field(default=None, min_length=1, max_length=31)


class MonthlyDownload(BaseModel):
    referenceMonth: str
    signature: str = Field(pattern=r"^[a-f0-9]{64}$")
    selected: list[str] = Field(default_factory=list, max_length=10000)


@router.post("/uploads")
def start_upload(body: UploadRequest, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    return uploads.initiate(session, body.model_dump(mode="json"), user.name, svc.catalog_materials(session, user))


@router.put("/uploads/{upload_id}/parts/{part}")
async def upload_part(upload_id: str, part: int, request: Request, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    uploads.get(session, upload_id, user.name)
    chunks, total = [], 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > uploads.CHUNK_BYTES:
            raise HTTPException(413, "Chunk too large / 分片过大")
        chunks.append(chunk)
    result = uploads.receive(session, upload_id, user.name, part, b"".join(chunks), request.headers.get("X-Chunk-SHA256", ""))
    session.commit()
    return result


@router.post("/uploads/{upload_id}/preview")
def preview_upload(upload_id: str, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    with uploads.PROCESSING:
        state, content = uploads.assembled(session, upload_id, user.name)
        material = svc.require_material(svc.catalog_materials(session, user), state["target"]["materialKey"])
        if state["target"]["kind"] != "config":
            raise HTTPException(422, "Not a configuration / 不是配置表")
        return svc.parse_configuration(content, state["filename"], target_template=material["bomTemplate"])


@router.post("/uploads/{upload_id}/save")
def save_upload(upload_id: str, body: SaveUpload, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    # Bound file reads as well as decoding: pending 100-MiB inputs stay on disk.
    with uploads.PROCESSING:
        return save_completed_upload(upload_id, body, session, user)


def save_completed_upload(upload_id: str, body: SaveUpload, session: Session, user) -> dict:
    uploads.lock(session, upload_id)
    materials = svc.catalog_materials(session, user)
    previous = uploads.saved(session, upload_id, user.name)
    if previous:
        authorized_file(session, previous.asset_file_id, materials, user)
        return {"saved": True, "groupId": str(previous.asset_group_id) if previous.asset_group_id else None}
    state, content = uploads.assembled(session, upload_id, user.name)
    target = state["target"]
    material = svc.require_material(materials, target["materialKey"])
    group = None
    if target["kind"] == "config":
        if body.sourceColumn is None:
            raise HTTPException(422, "Choose a configuration column / 请选择配置列")
        version = svc.attach_configuration(session, materials, target["materialKey"], content, state["filename"], body.sourceColumn, target["rowVersion"], user.name, body.sourceSheet)
        file = session.scalar(select(MaterialAssetFile).where(MaterialAssetFile.config_version_id == version.version_id))
    else:
        if target["groupId"]:
            group = svc.group_access(session, UUID(target["groupId"]), materials, editor=True, lock=True)
            if (group.brand, group.model_name, group.kind, group.colour_key) != (material["brand"], material["modelName"], target["kind"], target["colourKey"]):
                raise HTTPException(422, "Photo group no longer matches the target / 图片组与目标不匹配")
            file = svc.add_photos(session, group, [(state["filename"], content)], user.name, expected_row_version=target["rowVersion"])[0]
        else:
            group, files = svc.create_photo_group(session, materials, target, [(state["filename"], content)], user.name)
            file = files[0]
    file.upload_id = upload_id
    commit(session)
    upload_engine.cleanup_upload_session(uploads.root(user.name), upload_id)
    return {"saved": True, "groupId": str(group.asset_group_id) if group else None, "rowVersion": group.row_version if group else None}


@router.delete("/uploads/{upload_id}")
def cancel_upload(upload_id: str, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    uploads.lock(session, upload_id)
    if uploads.saved(session, upload_id, user.name):
        return {"cancelled": False, "saved": True}
    uploads.get(session, upload_id, user.name)
    upload_engine.cleanup_upload_session(uploads.root(user.name), upload_id)
    return {"cancelled": True, "saved": False}


def editable(user) -> bool:
    return ROLE_LEVEL[user.role] >= ROLE_LEVEL["editor"]


def commit(session: Session) -> None:
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(409, "Concurrent change; reload / 数据已变化，请重读") from exc


async def read_photos(files: list[UploadFile]) -> list[tuple[str, bytes]]:
    if not 1 <= len(files) <= 16:
        raise HTTPException(422, "Choose 1–16 photos / 请选择1–16张图片")
    result = []
    total = 0
    for file in files:
        content = await file.read(8 * 1024 * 1024 + 1)
        if len(content) > 8 * 1024 * 1024:
            raise HTTPException(422, "Use resumable upload for images over 8 MiB / 大于8MiB的图片请使用分片上传")
        total += len(content)
        if total > 32 * 1024 * 1024:
            raise HTTPException(422, "Use resumable upload for batches over 32 MiB / 超过32MiB的批次请使用分片上传")
        result.append((file.filename or "image", content))
    return result


@router.get("")
def catalog(session: Session = Depends(get_db_session), user=Depends(reader)) -> dict:
    return svc.read_catalog(session, svc.catalog_materials(session, user), editor=editable(user))


@router.put("/bindings")
def save_bindings(body: LinkBatch, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    svc.replace_bindings(session, svc.catalog_materials(session, user), [c.model_dump(mode="json") for c in body.changes], user.name)
    commit(session)
    return {"saved": True}


@router.post("/groups")
async def create_group(material_key: str = Form(), kind: str = Form(), colour_key: str = Form(), name: str = Form(min_length=1, max_length=150), row_version: int = Form(ge=0),
                       files: list[UploadFile] = File(), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    target = {"materialKey": material_key, "kind": kind, "colourKey": colour_key, "rowVersion": row_version}
    group, _ = svc.create_photo_group(session, svc.catalog_materials(session, user), target, await read_photos(files), user.name, name.strip())
    commit(session)
    return {"groupId": str(group.asset_group_id)}


@router.post("/groups/{group_id}/files")
async def add_files(group_id: UUID, row_version: int = Form(ge=1), files: list[UploadFile] = File(), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    group = svc.group_access(session, group_id, svc.catalog_materials(session, user), editor=True, lock=True)
    svc.add_photos(session, group, await read_photos(files), user.name, expected_row_version=row_version)
    commit(session)
    return {"saved": True}


@router.delete("/groups/{group_id}")
def delete_group(group_id: UUID, body: DeleteRequest, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    group = svc.group_access(session, group_id, svc.catalog_materials(session, user), editor=True, lock=True)
    if group.row_version != body.rowVersion:
        raise HTTPException(409, "Photo group changed; reload / 图片组已变化，请重读")
    if not body.confirmed or session.execute(select(MaterialAssetBinding).where(MaterialAssetBinding.asset_group_id == group_id)).first():
        raise HTTPException(409, "Confirm deletion of an unreferenced group / 仅可确认删除未被引用的图片组")
    files = list(session.scalars(select(MaterialAssetFile).where(MaterialAssetFile.asset_group_id == group_id)))
    svc.move_to_trash(session, files, user.name)
    group.deleted_at_utc = datetime.now(timezone.utc)
    commit(session)
    return {"deleted": True}


@router.delete("/files/{file_id}")
def delete_file(file_id: UUID, body: DeleteRequest, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    materials = svc.catalog_materials(session, user)
    file = session.get(MaterialAssetFile, file_id)
    if not file or file.deleted_at_utc or not file.asset_group_id:
        raise HTTPException(404, "Photo unavailable / 图片不可用")
    group = svc.group_access(session, file.asset_group_id, materials, editor=True, lock=True)
    if not body.confirmed or body.rowVersion != group.row_version:
        raise HTTPException(409, "Confirm shared-photo removal with current version / 请核对当前版本并确认移除共享图片")
    svc.move_to_trash(session, [file], user.name)
    group.row_version += 1
    commit(session)
    return {"deleted": True}


def authorized_file(session: Session, file_id: UUID, materials: dict, user) -> MaterialAssetFile:
    file = session.get(MaterialAssetFile, file_id)
    if not file or file.deleted_at_utc:
        raise HTTPException(404, "File unavailable / 文件不可用")
    if file.asset_group_id:
        svc.group_access(session, file.asset_group_id, materials, editor=editable(user))
    else:
        svc.version_access(session, file.config_version_id, materials)
    return file


@router.get("/files/{file_id}")
def get_file(file_id: UUID, download: bool = False, variant: str = Query("display", pattern="^(thumbnail|display|original)$"), session: Session = Depends(get_db_session), user=Depends(reader)):
    file = authorized_file(session, file_id, svc.catalog_materials(session, user), user)
    from urllib.parse import quote
    image_variant = bool(file.asset_group_id) and not download and variant != "original"
    if image_variant and (file.thumbnail is None or file.display_image is None):
        # Legacy cache fills use the same parent→file lock order as removal, so a
        # concurrent trash move counts the renditions against its 2-GB quota.
        svc.group_access(session, file.asset_group_id, svc.catalog_materials(session, user), editor=editable(user), lock=True)
        file = session.scalar(select(MaterialAssetFile).where(MaterialAssetFile.asset_file_id == file_id).with_for_update().execution_options(populate_existing=True))
        if file is None or file.deleted_at_utc:
            raise HTTPException(404, "Photo unavailable / 图片不可用")
        if file.thumbnail is None or file.display_image is None:
            _, _, file.thumbnail, file.display_image = svc.prepare_image(file.content)
        commit(session)
    content = (file.thumbnail if variant == "thumbnail" else file.display_image) if image_variant else file.content
    return StreamingResponse(io.BytesIO(content), media_type="image/webp" if image_variant else file.content_type, headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff",
        "Content-Disposition": ("attachment" if download else "inline") + "; filename*=UTF-8''" + quote(file.filename)})


@router.post("/config-preview")
async def preview_config(file: UploadFile = File(), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    if not svc.catalog_materials(session, user):
        raise HTTPException(403, "No authorized materials / 无获授权物料")
    return svc.parse_configuration(await file.read(svc.MAX_CONFIG_BYTES + 1), file.filename or "")


@router.post("/configurations")
async def upload_config(material_key: str = Form(), source_column: int = Form(ge=0), row_version: int = Form(ge=0), file: UploadFile = File(),
                        source_sheet: str | None = Form(default=None, min_length=1, max_length=31), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    materials = svc.catalog_materials(session, user)
    content = await file.read(svc.MAX_CONFIG_BYTES + 1)
    version = svc.attach_configuration(session, materials, material_key, content, file.filename or "config.xlsx", source_column, row_version, user.name, source_sheet)
    commit(session)
    return {"versionId": str(version.version_id)}


@router.delete("/configurations/{version_id}")
def delete_config(version_id: UUID, confirmed: bool = Query(False), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    version = svc.version_access(session, version_id, svc.catalog_materials(session, user), lock=True)
    if not confirmed:
        raise HTTPException(409, "Confirm moving this configuration to the recycle bin / 请确认将配置移入回收站")
    svc.move_to_trash(session, list(session.scalars(select(MaterialAssetFile).where(MaterialAssetFile.config_version_id == version_id))), user.name)
    version.deleted_at_utc = datetime.now(timezone.utc)
    commit(session)
    return {"deleted": True}


@router.get("/trash")
def recycle_bin(session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    return svc.read_trash(session, svc.catalog_materials(session, user))


@router.post("/trash/{file_id}/restore")
def restore_file(file_id: UUID, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    file = svc.trash_file_access(session, file_id, svc.catalog_materials(session, user))
    if file.asset_group_id:
        group = session.get(MaterialAssetGroup, file.asset_group_id)
        group.deleted_at_utc = None
        group.row_version += 1
    else:
        session.get(ConfigVersion, file.config_version_id).deleted_at_utc = None
    file.deleted_at_utc, file.deleted_by = None, None
    commit(session)
    return {"restored": True}


@router.delete("/trash/{file_id}")
def purge_file(file_id: UUID, confirmed: bool = Query(False), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    file = svc.trash_file_access(session, file_id, svc.catalog_materials(session, user))
    if not confirmed:
        raise HTTPException(409, "Confirm permanent deletion; this cannot be undone / 永久删除不可恢复，请确认")
    version = session.get(ConfigVersion, file.config_version_id) if file.config_version_id else None
    session.delete(file)
    session.flush()
    # Keep immutable source records used by successors; only the explicitly selected file is purged.
    if version:
        remaining_files = session.scalar(select(MaterialAssetFile.asset_file_id).where(MaterialAssetFile.config_version_id == version.version_id).limit(1))
        successors = session.scalar(select(ConfigVersion.version_id).where(ConfigVersion.parent_version_id == version.version_id).limit(1))
        bindings = session.scalar(select(MaterialAssetBinding.binding_id).where(MaterialAssetBinding.config_version_id == version.version_id).limit(1))
        if not any((remaining_files, successors, bindings)):
            session.delete(version)
    commit(session)
    return {"deleted": True}


@router.get("/configurations/{version_id}")
def configuration(version_id: UUID, session: Session = Depends(get_db_session), user=Depends(reader)) -> dict:
    version = svc.version_access(session, version_id, svc.catalog_materials(session, user))
    return {"id": str(version.version_id), "source": version.notes, "values": version.values_snapshot_json}


@router.get("/compare")
def compare(material_keys: str = Query(alias="materialKeys"), session: Session = Depends(get_db_session), user=Depends(reader)) -> dict:
    materials = svc.catalog_materials(session, user)
    keys = list(dict.fromkeys(material_keys.split(",")))
    if not 2 <= len(keys) <= 8:
        raise HTTPException(422, "Compare 2–8 materials / 对比须选2–8个基准物料")
    versions = []
    for key in keys:
        material = svc.require_material(materials, key)
        binding = session.execute(select(MaterialAssetBinding).where(MaterialAssetBinding.brand == material["brand"], MaterialAssetBinding.model_name == material["modelName"],
            MaterialAssetBinding.bom_template == material["bomTemplate"], MaterialAssetBinding.kind == "config")).scalar_one_or_none()
        if not binding:
            raise HTTPException(409, "Configuration missing: " + material["bomTemplate"] + " / 请先关联配置表")
        versions.append(svc.version_access(session, binding.config_version_id, materials))
    return {"materials": [{"key": k, "bomTemplate": materials[k]["bomTemplate"], "modelName": materials[k]["modelName"], "versionId": str(v.version_id), "version": v.version_no} for k, v in zip(keys, versions)], "rows": svc.compare_versions(versions)}


@router.get("/download")
def download(file_ids: str = Query(alias="fileIds"), session: Session = Depends(get_db_session), user=Depends(reader)):
    ids = list(dict.fromkeys(file_ids.split(",")))
    if not file_ids.strip():
        raise HTTPException(422, "Choose files / 请选择文件")
    materials = svc.catalog_materials(session, user)
    try:
        files = [authorized_file(session, UUID(id), materials, user) for id in ids]
    except ValueError as exc:
        raise HTTPException(422, "Invalid file ID / 文件ID无效") from exc
    output = svc.build_download_archive(session, [(file.asset_file_id, str(file.asset_file_id) + "_" + svc.archive_segment(file.filename)) for file in files], max_bytes=128 * 1024 * 1024)
    return archive_response(output, "material-assets.zip")


def archive_response(output, filename: str) -> StreamingResponse:
    def chunks():
        try:
            while chunk := output.read(1024 * 1024):
                yield chunk
        finally:
            output.close()
    return StreamingResponse(chunks(), media_type="application/zip", background=BackgroundTask(output.close), headers={"Cache-Control": "private, no-store", "Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/download-preview")
def monthly_download_preview(reference_month: str = Query(alias="referenceMonth"), session: Session = Depends(get_db_session), user=Depends(reader)) -> dict:
    svc.month_bounds(reference_month)
    return svc.download_preview(session, svc.catalog_materials(session, user, reference_month), reference_month)


@router.post("/download")
def monthly_download(body: MonthlyDownload, session: Session = Depends(get_db_session), user=Depends(reader)):
    preview = monthly_download_preview(body.referenceMonth, session, user)
    if body.signature != preview["signature"]:
        raise HTTPException(409, "Assets changed; reload preview / 资料已变化，请重读预览")
    entries, index = svc.selected_download(preview, body.selected)
    # Reuse the same per-file authorization as single-album downloads.
    materials = svc.catalog_materials(session, user)
    for id in {id for id, _ in entries}:
        authorized_file(session, id, materials, user)
    return archive_response(svc.build_download_archive(session, entries, index), f"Material_Assets_{body.referenceMonth}.zip")
