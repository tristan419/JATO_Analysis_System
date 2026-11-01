from io import BytesIO
import hashlib
import os
from types import SimpleNamespace
from uuid import uuid4

import openpyxl
import pytest
from PIL import Image
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import Column, MetaData, Table, create_engine, inspect, select
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.routes import material_assets as route
from app.core.security import UserContext, get_current_user
from app.db.models import ConfigVersion, MaterialAssetBinding, MaterialAssetFile, MaterialAssetGroup, VehicleTrim
from app.db.session import get_db_session
from app.services import material_asset_service as svc

app = FastAPI()
app.include_router(route.router, prefix="/v1")


@compiles(JSONB, "sqlite")
def compile_jsonb(_type, _compiler, **_kw):
    return "JSON"


@pytest.fixture
def db():
    postgres_url = os.getenv("APP_TEST_ASSET_DATABASE_URL")
    if postgres_url:
        engine = create_engine(postgres_url)
        with engine.connect() as connection:
            transaction = connection.begin()
            with Session(bind=connection, join_transaction_mode="create_savepoint") as session:
                yield session
            transaction.rollback()
        engine.dispose()
        return
    engine = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}, execution_options={"schema_translate_map": {"ordering": None, "engineering_config": None, "ops": None}})
    metadata = MetaData()
    Table("import_batches", metadata, Column("import_batch_id", UUID(as_uuid=True), primary_key=True), schema="ops")
    for cls in (VehicleTrim, ConfigVersion, MaterialAssetGroup, MaterialAssetFile, MaterialAssetBinding):
        cls.__table__.to_metadata(metadata)
    metadata.create_all(engine)
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA foreign_keys=ON")
        conn.connection.driver_connection.create_function("octet_length", 1, lambda value: len(value) if value is not None else None)
    with Session(engine) as session:
        yield session
    engine.dispose()


def material(template="T7150D**WT1001", model="J7", code="CL"):
    return {"key": svc.material_key("JAECOO", model, template), "brand": "JAECOO", "modelName": model, "bomTemplate": template,
            "version": "Large battery FWD", "powertrain": "PHEV", "historical": False, "effectiveFrom": None, "effectiveTo": None,
            "colours": {f"code:{code}": {"key": f"code:{code}", "code": code, "name": "Black", "hex": "#000000"}},
            "interiors": {"name:BLACK-BLACK": {"key": "name:BLACK-BLACK", "code": None, "name": "Black-Black", "hex": None}}, "issues": [], "notes": []}


def group(db, model="J7", code="CL"):
    album = MaterialAssetGroup(brand="JAECOO", model_name=model, kind="exterior", colour_key=f"code:{code}", name="Black", created_by="editor")
    db.add(album); db.flush(); return album


def link(m, album, version=0):
    return {"materialKey": m["key"], "kind": "exterior", "colourKey": "code:CL", "groupId": str(album.asset_group_id), "versionId": None, "rowVersion": version}


def matrix(column=4, value="标配"):
    book = openpyxl.Workbook(); sheet = book.active; sheet.title = "在售可控资源表"
    sheet.cell(3, column, "JAECOO7-Exclusive"); sheet.cell(4, 2, "配置大类"); sheet.cell(4, 3, "配置子项")
    sheet.cell(5, 1, "F1"); sheet.cell(5, 2, "Safety"); sheet.cell(5, 3, "Airbag"); sheet.cell(5, column, value)
    out = BytesIO(); book.save(out); return out.getvalue()


def photo(format="PNG", size=(32, 24)):
    if format == "HEIF":
        from pillow_heif import register_heif_opener
        register_heif_opener()
    output = BytesIO()
    with Image.new("RGB", size, (20, 50, 90)) as image:
        image.save(output, format)
    return output.getvalue()


def test_existing_parser_requires_explicit_column_and_retains_aa_provenance():
    original = matrix(27)
    assert svc.parse_configuration(original, "config.xlsx")["columns"][0]["column"] == 26
    parsed = svc.parse_configuration(original, "config.xlsx", 26)
    assert parsed["values"][0]["source_column"] == "AA"
    assert parsed["values"][0]["source_row"] == 5
    with pytest.raises(HTTPException) as error:
        svc.parse_configuration(original, "config.xlsx", 3)
    assert error.value.status_code == 422


@pytest.mark.parametrize("filename,content", [("config.pdf", b"pdf"), ("config.xlsx", b"not a workbook"), ("config.xlsx", b"")])
def test_bad_config_is_not_reported_saved(filename, content):
    with pytest.raises(HTTPException) as error:
        svc.parse_configuration(content, filename)
    assert error.value.status_code == 422


def test_batch_reuse_uses_same_model_and_saved_colour_not_hex(db):
    one, two = material(), material("T7150D**WT1002")
    materials = {m["key"]: m for m in [one, two]}; album = group(db)
    svc.replace_bindings(db, materials, [link(one, album), link(two, album)], "editor"); db.commit()
    assert len(db.scalars(select(MaterialAssetBinding)).all()) == 2
    assert len(db.scalars(select(MaterialAssetGroup)).all()) == 1
    other_model, other_colour = material("T7150D**WT1003", "J8"), material("T7150D**WT1004", code="BW")
    for wrong in [other_model, other_colour]:
        with pytest.raises(HTTPException) as error:
            svc.replace_bindings(db, {wrong["key"]: wrong, **materials}, [link(wrong, album)], "editor")
        assert error.value.status_code == 422


def test_stale_batch_leaves_every_binding_unchanged(db):
    one, two = material(), material("T7150D**WT1002"); mats = {m["key"]: m for m in [one, two]}; album = group(db)
    svc.replace_bindings(db, mats, [link(one, album), link(two, album)], "editor"); db.commit()
    changes = [dict(link(one, album, 1), groupId=None), dict(link(two, album, 0), groupId=None)]
    with pytest.raises(HTTPException) as error: svc.replace_bindings(db, mats, changes, "editor")
    assert error.value.status_code == 409
    db.rollback()
    assert all(b.asset_group_id == album.asset_group_id for b in db.scalars(select(MaterialAssetBinding)))


def test_files_are_deferred_and_unlink_does_not_delete_originals(db):
    m, album = material(), group(db)
    original = photo()
    svc.add_photos(db, album, [("black.png", original)], "editor")
    svc.replace_bindings(db, {m["key"]: m}, [link(m, album)], "editor"); db.commit(); db.expire_all()
    file = db.scalars(select(MaterialAssetFile)).one(); assert "content" in inspect(file).unloaded
    svc.replace_bindings(db, {m["key"]: m}, [dict(link(m, album, 1), groupId=None)], "editor"); db.commit()
    assert file.content == original
    with pytest.raises(HTTPException): svc.group_access(db, album.asset_group_id, {m["key"]: m})
    assert svc.group_access(db, album.asset_group_id, {m["key"]: m}, editor=True) == album


def test_versions_are_immutable_and_missing_is_not_unequipped(db):
    m = material(); content = matrix()
    first = svc.create_configuration(db, m, svc.parse_configuration(content, "config.xlsx", 3), content, "v1.xlsx", "editor")
    second_content = matrix(value="-")
    second = svc.create_configuration(db, m, svc.parse_configuration(second_content, "config.xlsx", 3), second_content, "v2.xlsx", "editor")
    db.commit(); db.expire_all()
    assert second.version_no == 2 and second.parent_version_id == first.version_id
    assert first.values_snapshot_json[0]["raw_value"] == "标配"
    rows = svc.compare_versions([first, second, SimpleNamespace(values_snapshot_json=[])])
    assert rows[0]["values"][0]["raw_value"] == "标配" and rows[0]["values"][2] is None


def test_empty_country_scope_never_queries_unrestricted_bom(monkeypatch):
    monkeypatch.setattr(svc, "ordering_countries", lambda *_: set())
    monkeypatch.setattr(svc.bom_repo, "list_all_material_skus_for_admin", lambda *_args, **_kw: pytest.fail("Unrestricted call"))
    assert svc.catalog_materials(None, UserContext("order_filler", "limited")) == {}


def test_catalog_reads_saved_base_colours_and_rejects_conflicting_dates(monkeypatch):
    from datetime import date
    monkeypatch.setattr(svc, "ordering_countries", lambda *_: {"CH"})
    monkeypatch.setattr(svc, "ordering_brands", lambda *_: {"JAECOO"})
    def sku(start): return SimpleNamespace(bom_template="T7150D**WT1001", brand="JAECOO", model_name="J7", version="FWD", powertrain="PHEV", lifecycle_status="active", is_active=True, effective_from_date=start, effective_to_date=None, effective_from_month=None, effective_to_month=None, exterior_color_code="CL", exterior_color_name="Black", colour_hex="#000000", interior_colour_code=None, interior_color_name="Black-Black", remark="")
    monkeypatch.setattr(svc.bom_repo, "list_all_material_skus_for_admin", lambda *_a, **_kw: [sku(date(2026, 1, 1)), sku(date(2026, 2, 1))])
    result = next(iter(svc.catalog_materials(None, UserContext("editor", "e")).values()))
    assert result["issues"] and result["colours"]["code:CL"]["hex"] == "#000000"


def test_routes_enforce_roles_and_authorize_every_download(db, monkeypatch):
    m = material(); album = group(db); svc.add_photos(db, album, [("a.png", photo())], "e"); db.commit()
    monkeypatch.setattr(svc, "catalog_materials", lambda *_: {m["key"]: m})
    app.dependency_overrides[get_db_session] = lambda: db
    app.dependency_overrides[get_current_user] = lambda: UserContext("order_filler", "f")
    try:
        client = TestClient(app)
        assert client.post("/v1/order-genius/material-assets/groups", data={}).status_code == 403
        assert client.get(f"/v1/order-genius/material-assets/files/{db.scalars(select(MaterialAssetFile)).one().asset_file_id}").status_code == 404
        svc.replace_bindings(db, {m["key"]: m}, [link(m, album)], "e"); db.commit()
        file = db.scalars(select(MaterialAssetFile)).one()
        assert client.get(f"/v1/order-genius/material-assets/files/{file.asset_file_id}?download=true").content.startswith(b"\x89PNG")
        assert client.get(f"/v1/order-genius/material-assets/download?fileIds={file.asset_file_id},{uuid4()}").status_code == 404
        app.dependency_overrides[get_current_user] = lambda: UserContext("viewer", "v")
        assert client.get("/v1/order-genius/material-assets").status_code == 403
    finally: app.dependency_overrides.clear()


def test_referenced_group_is_protected_and_config_trash_keeps_snapshot_and_binding(db, monkeypatch):
    m = material(); mats = {m["key"]: m}; album = group(db)
    svc.replace_bindings(db, mats, [link(m, album)], "e"); db.commit()
    monkeypatch.setattr(svc, "catalog_materials", lambda *_: mats)
    with pytest.raises(HTTPException) as error: route.delete_group(album.asset_group_id, route.DeleteRequest(rowVersion=1, confirmed=True), db, UserContext("editor", "e"))
    assert error.value.status_code == 409
    content = matrix(); version = svc.create_configuration(db, m, svc.parse_configuration(content, "x.xlsx", 3), content, "x.xlsx", "e")
    svc.replace_bindings(db, mats, [{"materialKey": m["key"], "kind": "config", "colourKey": "", "groupId": None, "versionId": str(version.version_id), "rowVersion": 0}], "e"); db.commit()
    route.delete_config(version.version_id, True, db, UserContext("editor", "e"))
    assert version.deleted_at_utc is not None and version.values_snapshot_json
    assert db.scalar(select(MaterialAssetBinding).where(MaterialAssetBinding.config_version_id == version.version_id))


@pytest.fixture
def client(db, monkeypatch, tmp_path):
    m = material()
    monkeypatch.setattr(svc, "catalog_materials", lambda *_: {m["key"]: m})
    monkeypatch.setenv("APP_MATERIAL_ASSET_UPLOAD_ROOT", str(tmp_path))
    app.dependency_overrides[get_db_session] = lambda: db
    app.dependency_overrides[get_current_user] = lambda: UserContext("editor", "e")
    with TestClient(app) as value:
        yield value
    app.dependency_overrides.clear()


@pytest.mark.parametrize("format", ["JPEG", "PNG", "GIF", "WEBP", "BMP", "TIFF", "AVIF", "HEIF"])
def test_actual_image_formats_produce_bounded_webp_renditions(format):
    original = photo(format)
    mime, stored, thumbnail, display = svc.prepare_image(original)
    assert mime.startswith("image/") and stored == original
    for content, maximum in ((thumbnail, 480), (display, 2048)):
        with Image.open(BytesIO(content)) as image:
            assert image.format == "WEBP" and max(image.size) <= maximum
            assert not image.info.get("exif")


def test_60_mb_photo_is_stored_under_6_mb_not_alongside_original(db):
    original = photo("TIFF", (5000, 4000))
    assert 50_000_000 < len(original) < 70_000_000
    album = group(db)
    file = svc.add_photos(db, album, [("large.tiff", original)], "e")[0]; db.commit()
    assert len(file.content) <= 6_000_000 and file.filename == "large.webp"
    assert file.sha256 == hashlib.sha256(file.content).hexdigest()
    with Image.open(BytesIO(file.content)) as image:
        assert max(image.size) <= 4096
        assert all(abs(a - b) < 8 for a, b in zip(image.convert("RGB").getpixel((10, 10)), (20, 50, 90)))
    assert len(file.thumbnail) < len(file.display_image) < len(original)


def test_damaged_or_disguised_images_are_not_accepted():
    for content in (b"\x89PNG\r\n\x1a\nnot a png", b"<svg></svg>", b"plain text"):
        with pytest.raises(HTTPException) as error:
            svc.prepare_image(content)
        assert error.value.status_code == 422


def test_image_orientation_is_corrected_in_preview():
    output = BytesIO()
    with Image.new("RGB", (120, 60), "red") as image:
        exif = Image.Exif(); exif[274] = 6
        image.save(output, "JPEG", exif=exif)
    _, _, _, display = svc.prepare_image(output.getvalue())
    with Image.open(BytesIO(display)) as image:
        assert image.size == (60, 120) and not image.getexif()


def test_photo_delete_restore_and_confirmed_purge(client, db):
    m = material(); album = group(db)
    file = svc.add_photos(db, album, [("black.png", photo())], "e")[0]
    svc.replace_bindings(db, {m["key"]: m}, [link(m, album)], "e"); db.commit()
    path = f"/v1/order-genius/material-assets/files/{file.asset_file_id}"
    assert client.get(path + "?variant=thumbnail").headers["content-type"] == "image/webp"
    response = client.request("DELETE", path, json={"rowVersion": album.row_version, "confirmed": True})
    assert response.status_code == 200
    assert client.get(path).status_code == 404
    trash = client.get("/v1/order-genius/material-assets/trash").json()
    assert trash["entries"][0]["folder"] == "photos"
    assert trash["usedBytes"] == len(file.content) + len(file.thumbnail) + len(file.display_image)
    assert client.get("/v1/order-genius/material-assets").json()["groups"][0]["files"] == []
    restore = f"/v1/order-genius/material-assets/trash/{file.asset_file_id}"
    assert client.post(restore + "/restore").status_code == 200
    assert client.get(path).status_code == 200
    assert client.request("DELETE", path, json={"rowVersion": album.row_version, "confirmed": True}).status_code == 200
    assert client.delete(restore).status_code == 409
    assert client.delete(restore + "?confirmed=true").status_code == 200
    assert db.get(MaterialAssetFile, file.asset_file_id) is None


def test_configuration_folder_restore_does_not_overwrite_new_binding(client, db):
    m = material(); data = matrix()
    old = svc.create_configuration(db, m, svc.parse_configuration(data, "v1.xlsx", 3), data, "v1.xlsx", "e"); db.commit()
    assert client.delete(f"/v1/order-genius/material-assets/configurations/{old.version_id}?confirmed=true").status_code == 200
    entry = client.get("/v1/order-genius/material-assets/trash").json()["entries"][0]
    assert entry["folder"] == "configurations"
    new = svc.create_configuration(db, m, svc.parse_configuration(data, "v2.xlsx", 3), data, "v2.xlsx", "e")
    svc.replace_bindings(db, {m["key"]: m}, [{"materialKey": m["key"], "kind": "config", "colourKey": "", "groupId": None, "versionId": str(new.version_id), "rowVersion": 0}], "e"); db.commit()
    assert client.post(f"/v1/order-genius/material-assets/trash/{entry['id']}/restore").status_code == 200
    assert db.scalars(select(MaterialAssetBinding)).one().config_version_id == new.version_id


def test_both_folders_share_quota_and_full_bin_does_not_delete_any_file(client, db, monkeypatch):
    m = material(); album = group(db)
    file = svc.add_photos(db, album, [("photo.png", photo())], "e")[0]; db.commit()
    svc.move_to_trash(db, [file], "e"); db.commit()
    monkeypatch.setattr(svc, "TRASH_MAX_BYTES", svc.trash_usage(db))
    data = matrix(); version = svc.create_configuration(db, m, svc.parse_configuration(data, "v.xlsx", 3), data, "v.xlsx", "e"); db.commit()
    response = client.delete(f"/v1/order-genius/material-assets/configurations/{version.version_id}?confirmed=true")
    assert response.status_code == 409 and "full" in response.json()["detail"]
    assert version.deleted_at_utc is None
    assert db.scalars(select(MaterialAssetFile).where(MaterialAssetFile.config_version_id == version.version_id)).one().deleted_at_utc is None


def test_recycle_bin_is_role_and_model_scoped(client, db, monkeypatch):
    outside = group(db, model="J8")
    file = svc.add_photos(db, outside, [("secret.png", photo())], "e")[0]; db.commit()
    svc.move_to_trash(db, [file], "e"); db.commit()
    assert client.get("/v1/order-genius/material-assets/trash").json()["entries"] == []
    assert client.post(f"/v1/order-genius/material-assets/trash/{file.asset_file_id}/restore").status_code == 404
    app.dependency_overrides[get_current_user] = lambda: UserContext("order_filler", "f")
    assert client.get("/v1/order-genius/material-assets/trash").status_code == 403
    assert client.post("/v1/order-genius/material-assets/uploads", json={}).status_code == 403


def upload_body(data, kind="exterior", filename="photo.png"):
    chunks = [data[offset:offset + route.uploads.CHUNK_BYTES] for offset in range(0, len(data), route.uploads.CHUNK_BYTES)]
    hashes = [hashlib.sha256(chunk).hexdigest() for chunk in chunks]
    return {"materialKey": material()["key"], "kind": kind, "colourKey": "code:CL" if kind == "exterior" else "", "groupId": None,
            "rowVersion": 0, "filename": filename, "sizeBytes": len(data), "fileFingerprint": hashlib.sha256("".join(hashes).encode()).hexdigest()}, chunks, hashes


def test_chunk_upload_resumes_checks_identity_and_saves_only_once(client, db):
    body, chunks, hashes = upload_body(photo("TIFF", (700, 600)), filename="photo.tiff")
    state = client.post("/v1/order-genius/material-assets/uploads", json=body).json()
    path = f"/v1/order-genius/material-assets/uploads/{state['uploadId']}"
    assert len(chunks) == 2
    assert client.put(path + "/parts/1", content=chunks[0], headers={"X-Chunk-SHA256": hashes[0]}).status_code == 200
    resumed = client.post("/v1/order-genius/material-assets/uploads", json=body).json()
    assert resumed["uploadId"] == state["uploadId"] and resumed["receivedChunks"] == [1]
    assert client.post(path + "/save", json={}).status_code == 409
    app.dependency_overrides[get_current_user] = lambda: UserContext("editor", "other")
    assert client.delete(path).status_code == 404
    app.dependency_overrides[get_current_user] = lambda: UserContext("editor", "e")
    assert client.put(path + "/parts/2", content=chunks[1], headers={"X-Chunk-SHA256": "0" * 64}).status_code == 409
    assert client.put(path + "/parts/2", content=chunks[1], headers={"X-Chunk-SHA256": hashes[1]}).status_code == 200
    assert client.post(path + "/save", json={}).status_code == 200
    assert client.post(path + "/save", json={}).status_code == 200
    assert len(db.scalars(select(MaterialAssetFile)).all()) == 1
    assert client.delete(path).json() == {"cancelled": False, "saved": True}


def test_chunk_cancellation_cleans_partial_and_never_creates_assets(client, db):
    body, chunks, hashes = upload_body(photo())
    state = client.post("/v1/order-genius/material-assets/uploads", json=body).json()
    path = f"/v1/order-genius/material-assets/uploads/{state['uploadId']}"
    assert client.put(path + "/parts/1", content=chunks[0], headers={"X-Chunk-SHA256": hashes[0]}).status_code == 200
    assert client.delete(path).status_code == 200
    assert not (route.uploads.root("e") / state["uploadId"]).exists()
    assert client.post(path + "/save", json={}).status_code == 404
    assert not db.scalars(select(MaterialAssetFile)).all()


def test_new_upload_reaps_expired_aware_sessions_without_touching_fresh_parts(client):
    from datetime import datetime, timedelta, timezone
    from upload_toolkit.file_utils import write_json
    directory = route.uploads.root("e")
    stale = route.upload_engine.create_upload_session(directory, filename="expired.png", size_bytes=10, chunk_size=10)
    stale["createdAt"] = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    write_json(directory / stale["uploadId"] / "upload_state.json", stale)
    body, _, _ = upload_body(photo())
    response = client.post("/v1/order-genius/material-assets/uploads", json=body)
    assert response.status_code == 200 and not (directory / stale["uploadId"]).exists()
    assert (directory / response.json()["uploadId"]).exists()


def test_configuration_upload_preview_and_explicit_column(client, db):
    body, chunks, hashes = upload_body(matrix(27), kind="config", filename="source.xlsx")
    state = client.post("/v1/order-genius/material-assets/uploads", json=body).json()
    path = f"/v1/order-genius/material-assets/uploads/{state['uploadId']}"
    assert client.put(path + "/parts/1", content=chunks[0], headers={"X-Chunk-SHA256": hashes[0]}).status_code == 200
    assert client.post(path + "/preview").json()["columns"][0]["column"] == 26
    assert client.post(path + "/save", json={}).status_code == 422
    assert client.post(path + "/save", json={"sourceColumn": 26}).status_code == 200
    assert db.scalars(select(ConfigVersion)).one().values_snapshot_json[0]["source_column"] == "AA"


def test_same_model_bases_can_explicitly_share_one_configuration_not_interior_links(db):
    one, two = material(), material("T7150D**WT1002")
    two["interiors"] = {"name:BROWN": {"key": "name:BROWN", "code": None, "name": "Brown", "hex": None}}
    data = matrix(); version = svc.create_configuration(db, one, svc.parse_configuration(data, "same.xlsx", 3), data, "same.xlsx", "e")
    mats = {m["key"]: m for m in (one, two)}
    def config_link(m): return {"materialKey": m["key"], "kind": "config", "colourKey": "", "groupId": None, "versionId": str(version.version_id), "rowVersion": 0}
    svc.replace_bindings(db, mats, [config_link(one), config_link(two)], "e"); db.commit()
    assert len(db.scalars(select(ConfigVersion)).all()) == 1 and len(db.scalars(select(MaterialAssetFile)).all()) == 1
    # A reader authorized for only the second base can read its explicitly shared source.
    assert svc.version_access(db, version.version_id, {two["key"]: two}) == version
    catalog = svc.read_catalog(db, {two["key"]: two}, editor=False)
    assert len(catalog["versions"]) == 1 and catalog["materials"][0]["interiors"][0]["name"] == "Brown"
    newer = svc.create_configuration(db, one, svc.parse_configuration(data, "new.xlsx", 3), data, "new.xlsx", "e")
    svc.replace_bindings(db, mats, [{**config_link(one), "versionId": str(newer.version_id), "rowVersion": 1}], "e"); db.commit()
    assert db.scalar(select(MaterialAssetBinding).where(MaterialAssetBinding.bom_template == two["bomTemplate"])).config_version_id == version.version_id
    with pytest.raises(HTTPException) as denied: svc.version_access(db, newer.version_id, {two["key"]: two})
    assert denied.value.status_code == 404
    third = material("T7150D**WT1003", "J8")
    with pytest.raises(HTTPException) as error: svc.replace_bindings(db, {**mats, third["key"]: third}, [config_link(third)], "e")
    assert error.value.status_code == 422


def test_chunk_upload_adds_to_existing_group_with_uuid_and_version_checks(client, db):
    m = material(); album = group(db)
    svc.replace_bindings(db, {m["key"]: m}, [link(m, album)], "e"); db.commit()
    body, chunks, hashes = upload_body(photo())
    body.update(groupId=str(album.asset_group_id), rowVersion=album.row_version)
    state = client.post("/v1/order-genius/material-assets/uploads", json=body)
    assert state.status_code == 200
    path = f"/v1/order-genius/material-assets/uploads/{state.json()['uploadId']}"
    assert client.put(path + "/parts/1", content=chunks[0], headers={"X-Chunk-SHA256": hashes[0]}).status_code == 200
    assert client.post(path + "/save", json={}).status_code == 200
    assert db.scalars(select(MaterialAssetFile)).one().asset_group_id == album.asset_group_id


@pytest.mark.skipif(not os.getenv("APP_TEST_ASSET_DATABASE_URL"), reason="Requires isolated PostgreSQL")
def test_postgres_concurrent_moves_cannot_exceed_shared_quota(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from sqlalchemy import delete
    engine = create_engine(os.environ["APP_TEST_ASSET_DATABASE_URL"])
    with Session(engine) as seed:
        album = group(seed)
        files = svc.add_photos(seed, album, [("a.png", photo()), ("b.png", photo())], "e"); seed.commit()
        ids = [file.asset_file_id for file in files]; album_id = album.asset_group_id
        monkeypatch.setattr(svc, "TRASH_MAX_BYTES", len(files[0].content) + len(files[0].thumbnail) + len(files[0].display_image))
    barrier = Barrier(2)
    def move(id):
        with Session(engine) as session:
            file = session.get(MaterialAssetFile, id); barrier.wait()
            try:
                svc.move_to_trash(session, [file], "e"); session.commit(); return "saved"
            except HTTPException as error:
                session.rollback(); return error.status_code
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = list(executor.map(move, ids))
        assert outcomes.count("saved") == 1 and outcomes.count(409) == 1
        with Session(engine) as session:
            assert svc.trash_usage(session) <= svc.TRASH_MAX_BYTES
    finally:
        with Session(engine) as session:
            session.execute(delete(MaterialAssetFile).where(MaterialAssetFile.asset_file_id.in_(ids)))
            session.execute(delete(MaterialAssetGroup).where(MaterialAssetGroup.asset_group_id == album_id)); session.commit()
        engine.dispose()
