"""Material documents and model-shared photos; no BOM/price/order writes."""
from __future__ import annotations

import hashlib
import io
import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import UUID
from zipfile import BadZipFile, ZipFile

from fastapi import HTTPException
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from app.core.security import ordering_brands, ordering_countries
from app.db.models import (ConfigVersion, MaterialAssetBinding, MaterialAssetFile,
                           MaterialAssetGroup, VehicleTrim)
from app.infra import engineering_config_repository as config_repo
from app.infra import order_genius_repository as bom_repo
from app.services.engineering_config_matrix_parser import parse_config_matrix
from app.services.ordering_normalization import clean_text

MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_GROUP_BYTES = 32 * 1024 * 1024


def material_key(brand: str, model: str, template: str) -> str:
    return hashlib.sha256(json.dumps([brand, model, template], ensure_ascii=False).encode()).hexdigest()


def colour_key(code: str | None, name: str | None = None) -> str:
    """Use saved identifiers, never a HEX/name similarity match."""
    return "code:" + code.strip().upper() if clean_text(code) else "name:" + clean_text(name).upper()


def catalog_materials(session: Session, user) -> dict[str, dict]:
    countries = ordering_countries(session, user)
    if countries == set():
        return {}
    skus = bom_repo.list_all_material_skus_for_admin(
        session, country_code=countries, allowed_brands=ordering_brands(session, user), limit=None)
    materials: dict[str, dict] = {}
    for sku in skus:
        template = clean_text(sku.bom_template).upper()
        if "**" not in template:
            continue
        key = material_key(sku.brand, sku.model_name, template)
        first, last = bom_repo.get_lifecycle_dates(sku)
        item = materials.setdefault(key, {
            "key": key, "brand": sku.brand, "modelName": sku.model_name, "bomTemplate": template,
            "version": sku.version, "powertrain": sku.powertrain, "effectiveFrom": first.isoformat() if first else None,
            "effectiveTo": last.isoformat() if last else None, "historical": False,
            "colours": {}, "interiors": {}, "notes": [], "issues": [],
        })
        if item["version"] != sku.version or item["effectiveFrom"] != (first.isoformat() if first else None) or item["effectiveTo"] != (last.isoformat() if last else None):
            item["issues"] = ["Conflicting BOM fields/dates / BOM字段或日期不一致"]
        item["historical"] = item["historical"] or bom_repo.resolve_effective_lifecycle_status(sku, date.today()) == "historical"
        code = clean_text(sku.exterior_color_code).upper()
        if code:
            previous = item["colours"].get(colour_key(code))
            if previous and (previous["hex"], previous["name"]) != (sku.colour_hex, sku.exterior_color_name):
                item["issues"] = ["Conflicting BOM colour fields / BOM色码字段不一致"]
            item["colours"][colour_key(code)] = {"key": colour_key(code), "code": code,
                "name": sku.exterior_color_name, "hex": sku.colour_hex}
        if clean_text(sku.interior_colour_code) or clean_text(sku.interior_color_name):
            inner = colour_key(sku.interior_colour_code, sku.interior_color_name)
            item["interiors"][inner] = {"key": inner, "code": sku.interior_colour_code,
                "name": sku.interior_color_name, "hex": None}
        if sku.remark and sku.remark not in item["notes"]:
            item["notes"].append(sku.remark)
    return materials


def require_material(materials: dict, key: str) -> dict:
    material = materials.get(key)
    if material is None:
        raise HTTPException(404, "Material unavailable in this scope / 当前权限范围无此基准物料")
    return material


def binding_material(binding: MaterialAssetBinding) -> str:
    return material_key(binding.brand, binding.model_name, binding.bom_template)


def group_access(session: Session, group_id: UUID, materials: dict, *, editor: bool = False, lock: bool = False) -> MaterialAssetGroup:
    stmt = select(MaterialAssetGroup).where(MaterialAssetGroup.asset_group_id == group_id)
    group = session.execute(stmt.with_for_update() if lock else stmt).scalar_one_or_none()
    if group is None or not any(m["brand"] == group.brand and m["modelName"] == group.model_name for m in materials.values()):
        raise HTTPException(404, "Photo group unavailable / 图片组不可用")
    if not editor:
        bindings = session.execute(select(MaterialAssetBinding).where(MaterialAssetBinding.asset_group_id == group_id)).scalars()
        if not any(binding_material(b) in materials for b in bindings):
            raise HTTPException(404, "Photo group unavailable / 图片组不可用")
    return group


def version_access(session: Session, version_id: UUID, materials: dict, *, lock: bool = False) -> ConfigVersion:
    stmt = select(ConfigVersion).where(ConfigVersion.version_id == version_id)
    version = session.execute(stmt.with_for_update() if lock else stmt).scalar_one_or_none()
    if version is None or not version.values_snapshot_json:
        raise HTTPException(404, "Version snapshot unavailable / 无不可变配置版本")
    if not any(m["brand"] == version.brand and m["modelName"] == version.model_name and m["bomTemplate"] == version.material_no for m in materials.values()):
        raise HTTPException(404, "Configuration unavailable / 配置不可用")
    return version


def file_metadata(file: MaterialAssetFile) -> dict:
    return {"id": str(file.asset_file_id), "filename": file.filename, "contentType": file.content_type, "sha256": file.sha256}


def read_catalog(session: Session, materials: dict, *, editor: bool) -> dict:
    bindings = [b for b in session.execute(select(MaterialAssetBinding)).scalars() if binding_material(b) in materials]
    linked = {b.asset_group_id for b in bindings if b.asset_group_id}
    models = {(m["brand"], m["modelName"]) for m in materials.values()}
    groups = [g for g in session.execute(select(MaterialAssetGroup)).scalars()
              if (g.brand, g.model_name) in models and (editor or g.asset_group_id in linked)]
    versions = [v for v in session.execute(select(ConfigVersion).where(ConfigVersion.values_snapshot_json.is_not(None))).scalars()
                if any(m["brand"] == v.brand and m["modelName"] == v.model_name and m["bomTemplate"] == v.material_no for m in materials.values())]
    group_ids, version_ids = {g.asset_group_id for g in groups}, {v.version_id for v in versions}
    files = list(session.execute(select(MaterialAssetFile).where(or_(MaterialAssetFile.asset_group_id.in_(group_ids), MaterialAssetFile.config_version_id.in_(version_ids)))).scalars())
    return {"today": date.today().isoformat(), "materials": [{**m, "colours": list(m["colours"].values()), "interiors": list(m["interiors"].values())} for m in materials.values()],
        "bindings": [{"id": str(b.binding_id), "materialKey": binding_material(b), "kind": b.kind, "colourKey": b.colour_key,
                      "groupId": str(b.asset_group_id) if b.asset_group_id else None, "versionId": str(b.config_version_id) if b.config_version_id else None, "rowVersion": b.row_version} for b in bindings],
        "groups": [{"id": str(g.asset_group_id), "brand": g.brand, "modelName": g.model_name, "kind": g.kind, "colourKey": g.colour_key, "name": g.name, "rowVersion": g.row_version,
                    "references": [binding_material(b) for b in bindings if b.asset_group_id == g.asset_group_id],
                    "files": [file_metadata(f) for f in files if f.asset_group_id == g.asset_group_id]} for g in groups],
        "versions": [{"id": str(v.version_id), "brand": v.brand, "modelName": v.model_name, "bomTemplate": v.material_no, "number": v.version_no,
                      "parentId": str(v.parent_version_id) if v.parent_version_id else None, "createdBy": v.created_by, "createdAt": v.created_at_utc.isoformat(), "source": v.notes,
                      "references": [binding_material(b) for b in bindings if b.config_version_id == v.version_id],
                      "files": [file_metadata(f) for f in files if f.config_version_id == v.version_id]} for v in versions]}


def replace_bindings(session: Session, materials: dict, changes: list[dict], username: str) -> None:
    if not changes or len(changes) > 100:
        raise HTTPException(422, "Choose 1–100 links / 请选择1–100处关联")
    identities = [(c["materialKey"], c["kind"], c["colourKey"]) for c in changes]
    if len(set(identities)) != len(identities):
        raise HTTPException(422, "Duplicate links / 关联重复")
    # Album/version locks serialize new references with deletion, across the whole transaction.
    groups = {id: group_access(session, UUID(id), materials, editor=True, lock=True) for id in sorted({c["groupId"] for c in changes if c.get("groupId")})}
    versions = {id: version_access(session, UUID(id), materials, lock=True) for id in sorted({c["versionId"] for c in changes if c.get("versionId")})}
    planned = []
    for change in sorted(changes, key=lambda c: (c["materialKey"], c["kind"], c["colourKey"])):
        material = require_material(materials, change["materialKey"])
        if material["issues"]:
            raise HTTPException(409, "Resolve conflicting BOM fields first / 请先核对BOM字段")
        kind, key = change["kind"], change["colourKey"]
        if kind == "config":
            version = versions.get(change.get("versionId"))
            if key or change.get("groupId") or (version and (version.brand, version.model_name, version.material_no) != (material["brand"], material["modelName"], material["bomTemplate"])):
                raise HTTPException(422, "Configuration must belong to this base / 配置须属于此基准物料")
        elif kind in {"exterior", "interior"}:
            group = groups.get(change.get("groupId"))
            allowed = material["colours"] if kind == "exterior" else material["interiors"]
            if key not in allowed or change.get("versionId") or (group and (group.brand, group.model_name, group.kind, group.colour_key) != (material["brand"], material["modelName"], kind, key)):
                raise HTTPException(422, "Use an existing same-model colour / 仅可关联同Model的既有色码")
        else:
            raise HTTPException(422, "Invalid link kind / 关联类型无效")
        current = session.execute(select(MaterialAssetBinding).where(MaterialAssetBinding.brand == material["brand"], MaterialAssetBinding.model_name == material["modelName"],
            MaterialAssetBinding.bom_template == material["bomTemplate"], MaterialAssetBinding.kind == kind, MaterialAssetBinding.colour_key == key).with_for_update()).scalar_one_or_none()
        if (current.row_version if current else 0) != change["rowVersion"]:
            raise HTTPException(409, "Links changed; reload without discarding input / 关联已变化，请重读；输入保留")
        planned.append((material, change, current))
    for material, change, current in planned:
        if not change.get("groupId") and not change.get("versionId"):
            if current:
                session.delete(current)
            continue
        if current is None:
            current = MaterialAssetBinding(brand=material["brand"], model_name=material["modelName"], bom_template=material["bomTemplate"], kind=change["kind"], colour_key=change["colourKey"], row_version=0, updated_by=username)
            session.add(current)
        current.asset_group_id = UUID(change["groupId"]) if change.get("groupId") else None
        current.config_version_id = UUID(change["versionId"]) if change.get("versionId") else None
        current.row_version += 1
        current.updated_by = username


def validate_image(content: bytes) -> str:
    if not content or len(content) > MAX_FILE_BYTES:
        raise HTTPException(422, "Each image must be under 8 MiB / 单图须小于8MiB")
    if content.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if content.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if content.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if content[:4] == b"RIFF" and content[8:12] == b"WEBP":
        return "image/webp"
    raise HTTPException(422, "Use PNG, JPEG, GIF or WebP / 请使用PNG、JPEG、GIF或WebP")


def add_photos(session: Session, group: MaterialAssetGroup, files: list[tuple[str, bytes]], username: str) -> None:
    if not 1 <= len(files) <= 16 or sum(len(data) for _, data in files) > MAX_GROUP_BYTES:
        raise HTTPException(422, "Upload 1–16 images, at most 32 MiB / 每次1–16张，总计不超过32MiB")
    validated = [(name, data, validate_image(data)) for name, data in files]
    for filename, content, mime in validated:
        session.add(MaterialAssetFile(asset_group_id=group.asset_group_id, filename=Path(filename).name[:255], content_type=mime,
                                      sha256=hashlib.sha256(content).hexdigest(), content=content, created_by=username))
    group.row_version += 1


def parse_configuration(content: bytes, filename: str, source_column: int | None = None) -> dict:
    if not content or len(content) > MAX_FILE_BYTES or Path(filename).suffix.lower() != ".xlsx":
        raise HTTPException(422, "Use an XLSX under 8 MiB / 请使用8MiB以内XLSX配置表")
    try:
        with ZipFile(io.BytesIO(content)) as archive:
            if sum(i.file_size for i in archive.infolist()) > 64 * 1024 * 1024 or any("vbaProject" in name or "externalLinks/" in name for name in archive.namelist()):
                raise ValueError("Unsupported workbook size/macros/external links")
        with TemporaryDirectory(prefix="material-config-") as directory:
            path = Path(directory) / "config.xlsx"
            path.write_bytes(content)
            result = parse_config_matrix(path)
    except (BadZipFile, ValueError, KeyError, OSError, IndexError) as exc:
        raise HTTPException(422, "Configuration cannot be parsed using the existing matrix format / 配置表无法按现有配置矩阵格式解析：" + str(exc)[:300]) from exc
    columns = [{"column": item["source_column"], "name": item["full_trim_name"]} for item in result["trims"]]
    if source_column is None:
        return {"columns": columns, "warnings": result["warnings"]}
    selected = [c for c in columns if c["column"] == source_column]
    if len(selected) != 1:
        raise HTTPException(422, "Choose the exact configuration column / 请选择具体配置列")
    from openpyxl.utils import get_column_letter
    source_letter = get_column_letter(source_column + 1)
    values = [v for v in result["values"] if v["source_column"] == source_letter]
    keys = [(v["category"], v["feature_name"]) for v in values]
    if not values or len(set(keys)) != len(keys):
        raise HTTPException(422, "Empty or ambiguous configuration column / 配置列为空或有重复字段，请核对源文件")
    return {"column": selected[0], "values": values, "warnings": result["warnings"]}


def create_configuration(session: Session, material: dict, parsed: dict, content: bytes, filename: str, username: str) -> ConfigVersion:
    identity = "|".join([material["brand"], material["modelName"], material["bomTemplate"]])
    trim = session.execute(select(VehicleTrim).where(VehicleTrim.identity_key == identity).with_for_update()).scalars().first()
    if trim is None:
        trim = VehicleTrim(identity_key=identity, material_no=material["bomTemplate"], brand=material["brand"], model_name=material["modelName"],
                           trim_name=material["version"], full_trim_name=identity, status="active")
        config_repo.add_vehicle_trim(session, trim)
        session.flush()
    previous = session.execute(select(ConfigVersion).where(ConfigVersion.trim_id == trim.trim_id).order_by(ConfigVersion.version_no.desc())).scalars().first()
    version = ConfigVersion(trim_id=trim.trim_id, identity_key=identity, material_no=material["bomTemplate"], brand=material["brand"], model_name=material["modelName"],
        trim_name=material["version"], status="published", version_no=previous.version_no + 1 if previous else 1,
        parent_version_id=previous.version_id if previous else None, created_by=username, notes=parsed["column"]["name"], values_snapshot_json=parsed["values"])
    session.add(version)
    session.flush()
    session.add(MaterialAssetFile(config_version_id=version.version_id, filename=Path(filename).name[:255], content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                  sha256=hashlib.sha256(content).hexdigest(), content=content, created_by=username))
    return version


def compare_versions(versions: list[ConfigVersion]) -> list[dict]:
    maps = [{(v["category"], v["feature_name"]): v for v in version.values_snapshot_json or []} for version in versions]
    keys = sorted({key for mapping in maps for key in mapping})
    return [{"category": category, "featureName": name, "values": [mapping.get((category, name)) for mapping in maps]} for category, name in keys]
