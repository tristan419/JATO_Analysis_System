"""Material Assets API: editor writes, authorized Order Filler reads/downloads."""
from __future__ import annotations

import io
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.security import require_min_role, require_roles, ROLE_LEVEL
from app.db.models import MaterialAssetBinding, MaterialAssetFile, MaterialAssetGroup, ConfigVersion
from app.db.session import get_db_session
from app.services import material_asset_service as svc

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
        content = await file.read(svc.MAX_FILE_BYTES + 1)
        total += len(content)
        svc.validate_image(content)
        if total > svc.MAX_GROUP_BYTES:
            raise HTTPException(422, "At most 32 MiB per upload / 每次上传最多32MiB")
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
    material = svc.require_material(svc.catalog_materials(session, user), material_key)
    allowed = material["colours"] if kind == "exterior" else material["interiors"] if kind == "interior" else {}
    if colour_key not in allowed:
        raise HTTPException(422, "Use an existing BOM colour / 请使用既有BOM色码")
    group = MaterialAssetGroup(brand=material["brand"], model_name=material["modelName"], kind=kind, colour_key=colour_key, name=name.strip(), created_by=user.name)
    session.add(group)
    session.flush()
    svc.add_photos(session, group, await read_photos(files), user.name)
    svc.replace_bindings(session, {material_key: material}, [{"materialKey": material_key, "kind": kind, "colourKey": colour_key,
        "groupId": str(group.asset_group_id), "versionId": None, "rowVersion": row_version}], user.name)
    commit(session)
    return {"groupId": str(group.asset_group_id)}


@router.post("/groups/{group_id}/files")
async def add_files(group_id: UUID, row_version: int = Form(ge=1), files: list[UploadFile] = File(), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    group = svc.group_access(session, group_id, svc.catalog_materials(session, user), editor=True, lock=True)
    if row_version != group.row_version:
        raise HTTPException(409, "Photo group changed; reload / 图片组已变化，请重读")
    svc.add_photos(session, group, await read_photos(files), user.name)
    commit(session)
    return {"saved": True}


@router.delete("/groups/{group_id}")
def delete_group(group_id: UUID, body: DeleteRequest, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    group = svc.group_access(session, group_id, svc.catalog_materials(session, user), editor=True, lock=True)
    if group.row_version != body.rowVersion:
        raise HTTPException(409, "Photo group changed; reload / 图片组已变化，请重读")
    if not body.confirmed or session.execute(select(MaterialAssetBinding).where(MaterialAssetBinding.asset_group_id == group_id)).first():
        raise HTTPException(409, "Confirm deletion of an unreferenced group / 仅可确认删除未被引用的图片组")
    for file in session.execute(select(MaterialAssetFile).where(MaterialAssetFile.asset_group_id == group_id)).scalars():
        session.delete(file)
    session.flush()
    session.delete(group)
    commit(session)
    return {"deleted": True}


@router.delete("/files/{file_id}")
def delete_file(file_id: UUID, body: DeleteRequest, session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    materials = svc.catalog_materials(session, user)
    file = session.get(MaterialAssetFile, file_id)
    if not file or not file.asset_group_id:
        raise HTTPException(404, "Photo unavailable / 图片不可用")
    group = svc.group_access(session, file.asset_group_id, materials, editor=True, lock=True)
    if not body.confirmed or body.rowVersion != group.row_version:
        raise HTTPException(409, "Confirm shared-photo removal with current version / 请核对当前版本并确认移除共享图片")
    session.delete(file)
    group.row_version += 1
    commit(session)
    return {"deleted": True}


def authorized_file(session: Session, file_id: UUID, materials: dict, user) -> MaterialAssetFile:
    file = session.get(MaterialAssetFile, file_id)
    if not file:
        raise HTTPException(404, "File unavailable / 文件不可用")
    if file.asset_group_id:
        svc.group_access(session, file.asset_group_id, materials, editor=editable(user))
    else:
        svc.version_access(session, file.config_version_id, materials)
    return file


@router.get("/files/{file_id}")
def get_file(file_id: UUID, download: bool = False, session: Session = Depends(get_db_session), user=Depends(reader)):
    file = authorized_file(session, file_id, svc.catalog_materials(session, user), user)
    from urllib.parse import quote
    return StreamingResponse(io.BytesIO(file.content), media_type=file.content_type, headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff",
        "Content-Disposition": ("attachment" if download else "inline") + "; filename*=UTF-8''" + quote(file.filename)})


@router.post("/config-preview")
async def preview_config(file: UploadFile = File(), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    if not svc.catalog_materials(session, user):
        raise HTTPException(403, "No authorized materials / 无获授权物料")
    return svc.parse_configuration(await file.read(svc.MAX_FILE_BYTES + 1), file.filename or "")


@router.post("/configurations")
async def upload_config(material_key: str = Form(), source_column: int = Form(ge=0), row_version: int = Form(ge=0), file: UploadFile = File(),
                        session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    materials = svc.catalog_materials(session, user)
    material = svc.require_material(materials, material_key)
    content = await file.read(svc.MAX_FILE_BYTES + 1)
    parsed = svc.parse_configuration(content, file.filename or "", source_column)
    version = svc.create_configuration(session, material, parsed, content, file.filename or "config.xlsx", user.name)
    svc.replace_bindings(session, materials, [{"materialKey": material_key, "kind": "config", "colourKey": "", "groupId": None,
        "versionId": str(version.version_id), "rowVersion": row_version}], user.name)
    commit(session)
    return {"versionId": str(version.version_id)}


@router.delete("/configurations/{version_id}")
def delete_config(version_id: UUID, confirmed: bool = Query(False), session: Session = Depends(get_db_session), user=Depends(writer)) -> dict:
    version = svc.version_access(session, version_id, svc.catalog_materials(session, user), lock=True)
    if not confirmed or session.execute(select(MaterialAssetBinding).where(MaterialAssetBinding.config_version_id == version_id)).first() or session.execute(select(ConfigVersion).where(ConfigVersion.parent_version_id == version_id)).first():
        raise HTTPException(409, "Referenced version cannot be deleted / 被关联或有后续版本引用的配置不可删除")
    for file in session.execute(select(MaterialAssetFile).where(MaterialAssetFile.config_version_id == version_id)).scalars():
        session.delete(file)
    session.flush()
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
    if not 1 <= len(ids) <= 50:
        raise HTTPException(422, "Choose 1–50 files / 请选择1–50个文件")
    materials = svc.catalog_materials(session, user)
    try:
        files = [authorized_file(session, UUID(id), materials, user) for id in ids]
    except ValueError as exc:
        raise HTTPException(422, "Invalid file ID / 文件ID无效") from exc
    total = session.scalar(select(func.sum(func.octet_length(MaterialAssetFile.content))).where(MaterialAssetFile.asset_file_id.in_([f.asset_file_id for f in files]))) or 0
    if total > 128 * 1024 * 1024:
        raise HTTPException(413, "Download up to 128 MiB at once / 单次下载最多128MiB，请分批选择")
    output = io.BytesIO()
    with ZipFile(output, "w", ZIP_DEFLATED) as archive:
        for file in files:
            archive.writestr(str(file.asset_file_id) + "_" + file.filename.replace("\\", "_").replace("/", "_"), file.content)
    output.seek(0)
    return StreamingResponse(output, media_type="application/zip", headers={"Cache-Control": "private, no-store", "Content-Disposition": 'attachment; filename="material-assets.zip"'})
