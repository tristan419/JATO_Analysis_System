"""Small explicit base-link writer and reminders over the existing BOM projection."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import MaterialNoticeRead, MaterialReplacement
from app.infra import order_genius_repository as repo


def replacement_catalog(session: Session, allowed_brands: set[str] | None) -> dict:
    groups: dict[tuple[str, str], list] = {}
    for sku in repo.list_all_material_skus_for_admin(session, allowed_brands=allowed_brands, limit=None):
        if sku.bom_template:
            groups.setdefault((sku.brand.strip().upper(), sku.bom_template), []).append(sku)
    nodes = []
    for (brand, template), members in sorted(groups.items()):
        identities = {(sku.model_name, sku.version, sku.powertrain) for sku in members}
        if len(identities) != 1:
            continue  # Inconsistent bases must first be repaired in existing BOM editing.
        model, version, powertrain = next(iter(identities))
        nodes.append({"brand": brand, "bomTemplate": template, "modelName": model,
                      "version": version, "powertrain": powertrain or "",
                      "materialCodes": sorted(sku.material_code for sku in members)})
    records = session.execute(select(MaterialReplacement)).scalars().all()
    links = [{"brand": row.brand, "from": row.source_template, "to": row.target_template,
              "note": row.note or "", "rowVersion": row.row_version}
             for row in records if allowed_brands is None or row.brand in allowed_brands]
    return {"nodes": nodes, "links": links}


def save_replacement(session: Session, *, brand: str, source: str, target: str | None,
                     note: str, expected_version: int, actor: str,
                     allowed_brands: set[str] | None) -> dict:
    """Only the shared relation changes. No SKU, FOB, lifecycle or PI writes."""
    if allowed_brands is not None and brand not in allowed_brands:
        raise HTTPException(403, "Brand not assigned / 未获配该品牌")
    catalog = replacement_catalog(session, {brand})
    by_template = {node["bomTemplate"]: node for node in catalog["nodes"]}
    if source not in by_template or (target is not None and target not in by_template):
        raise HTTPException(400, "Base missing or inconsistent; maintain BOM first / 基准物料缺失或不一致，请先维护BOM")
    if source == target:
        raise HTTPException(400, "Cannot link a base to itself / 不能关联自身")
    rows = session.execute(select(MaterialReplacement).where(
        MaterialReplacement.brand == brand).with_for_update()).scalars().all()
    current = next((row for row in rows if row.source_template == source), None)
    if (current.row_version if current else 0) != expected_version:
        raise HTTPException(409, "Relation changed; reload / 关联已改变，请重读")
    successors = {row.source_template: row.target_template for row in rows if row.target_template}
    successors[source] = target
    if target and any(key != source and value == target for key, value in successors.items()):
        raise HTTPException(400, "Target already has a predecessor / 目标已有前任，请先断开原关联")
    visited = {source}
    cursor = target
    while cursor:
        if cursor in visited:
            raise HTTPException(400, "Replacement cycle rejected / 不允许闭环")
        visited.add(cursor)
        cursor = successors.get(cursor)
    note = note.strip()
    if current and current.target_template == target and (current.note or "") == note:
        return {"changed": False, "rowVersion": current.row_version}
    if current is None and target is None and not note:
        return {"changed": False, "rowVersion": 0}
    if current is None:
        current = MaterialReplacement(brand=brand, source_template=source, row_version=0)
        session.add(current)
    current.previous_target = current.target_template
    current.target_template = target
    current.note = note or None
    current.changed_by = actor
    current.changed_at = datetime.now(timezone.utc)
    current.row_version += 1
    session.flush()
    return {"changed": True, "rowVersion": current.row_version}


def material_notices(session: Session, projection: dict, *, user_id: UUID,
                     username: str) -> list[dict]:
    """No write on read/date crossing; permission is the fresh BOM projection."""
    nodes = {node["bomTemplate"]: node for node in projection["nodes"]}
    contents = []
    for event in projection["events"]:
        node = nodes[event["bomTemplate"]]
        contents.append({"kind": event["kind"], "date": event["date"],
                         "bomTemplate": node["bomTemplate"], "brand": node["brand"],
                         "modelName": node["modelName"], "target": None, "note": "",
                         "firstOrderDate": node["effectiveFrom"], "finalOrderDate": node["effectiveTo"]})
    today = date.fromisoformat(projection["today"])
    records = session.execute(select(MaterialReplacement).where(
        MaterialReplacement.changed_at >= datetime.combine(today - timedelta(days=30), datetime.min.time(), tzinfo=timezone.utc)
    )).scalars().all()
    for record in records:
        node = nodes.get(record.source_template)
        if not node or node["brand"] != record.brand or record.changed_by == username:
            continue
        if record.target_template and record.target_template not in nodes:
            continue
        contents.append({"kind": "relation", "date": record.changed_at.date().isoformat(),
                         "bomTemplate": node["bomTemplate"], "brand": node["brand"],
                         "modelName": node["modelName"], "target": record.target_template,
                         "previousTarget": record.previous_target, "note": record.note or "",
                         "changedBy": record.changed_by, "revision": record.row_version})
    read = set(session.execute(select(MaterialNoticeRead.notice_key).where(
        MaterialNoticeRead.user_id == user_id)).scalars())
    for content in contents:
        content["countryCode"] = projection["countryCode"]
        key = hashlib.sha256(json.dumps(content, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        content["key"] = key
        content["read"] = key in read
    return sorted(contents, key=lambda item: (item["date"], item["bomTemplate"], item["kind"]))


def acknowledge_material_notice(session: Session, *, key: str, user_id: UUID,
                                notices: list[dict], read: bool) -> None:
    if not any(notice["key"] == key for notice in notices):
        raise HTTPException(403, "Notice outside current authorization / 提醒不在当前授权范围")
    row = session.get(MaterialNoticeRead, (user_id, key))
    if read and row is None:
        session.add(MaterialNoticeRead(user_id=user_id, notice_key=key, read_at=datetime.now(timezone.utc)))
    elif not read and row is not None:
        session.delete(row)
