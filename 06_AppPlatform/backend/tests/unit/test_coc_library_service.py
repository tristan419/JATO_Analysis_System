import io
import asyncio
import json
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from upload_toolkit.job_engine import load_job_state, state_path
from app.services import coc_library_service as library
from app.services import coc_match_service as match
from upload_toolkit.job_engine import persist_job_state
from app.api.routes import order_genius_vehicle_allocation as route

VIN = "LVUGTB220TDE99425"
VIN2 = "LVUGTB220TDE99426"


@pytest.fixture(autouse=True)
def private_library(tmp_path, monkeypatch):
    monkeypatch.setattr(library, "LIBRARY_ROOT", tmp_path / "library")


def source(tmp_path, filename, entries):
    path = tmp_path / filename
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in entries:
            archive.writestr(name, content)
    result = library.import_library_source(path, filename, "admin", background=False)
    state = load_job_state(state_path(library.LIBRARY_ROOT, result["sourceId"]))
    assert state["status"] == "success", state
    return result["sourceId"]


def activate(source_id, replace=None):
    preview = library.preview_source(source_id)
    library.activate_source(source_id, preview["fingerprint"], replace or [])
    return preview


def test_nested_paths_filename_matching_and_exact_bytes(tmp_path):
    nested = io.BytesIO()
    with zipfile.ZipFile(nested, "w") as z:
        z.writestr(f"folder/{VIN.lower()}.PDF", b"not parsed; original bytes")
        z.writestr("readme.pdf", b"ignore invalid VIN")
        z.writestr(f"__MACOSX/._{VIN}.pdf", b"ignore sidecar")
    sid = source(tmp_path, "nested.zip", [("dir/child.zip", nested.getvalue())])
    assert not library.lookup_vins([VIN])  # indexed is not published
    assert activate(sid)["items"][0]["status"] == "new"
    member = library.lookup_vins([VIN])[VIN]
    assert json.loads(member["path"]) == ["dir/child.zip", f"folder/{VIN.lower()}.PDF"]
    target = tmp_path / "download.zip"
    library.write_pdf_zip([VIN.lower(), VIN], target)
    with zipfile.ZipFile(target) as z:
        assert z.namelist() == [f"{VIN}.pdf"]
        assert z.read(f"{VIN}.pdf") == b"not parsed; original bytes"
    assert library.library_sources()["vinCount"] == 1


def test_append_duplicates_delete_same_content_provider(tmp_path):
    a = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"a")])
    activate(a)
    b = source(tmp_path, "b.zip", [(f"nested/{VIN}.pdf", b"a"), (f"{VIN2}.pdf", b"b")])
    assert [r["status"] for r in activate(b)["items"]] == ["duplicate", "new"]
    preview = library.delete_source(a)
    assert preview["lostCount"] == 0
    library.delete_source(a, preview["fingerprint"])
    assert set(library.lookup_vins([VIN, VIN2])) == {VIN, VIN2}
    assert library.delete_source(b)["lostCount"] == 2


def test_three_nested_zips_download_required_vins_from_index_only(tmp_path, monkeypatch):
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as z:
        z.writestr(f"cars/{VIN}.pdf", b"first original PDF")
        z.writestr(f"cars/{VIN2}.pdf", b"second original PDF")
    for depth in range(2):
        outer = io.BytesIO()
        with zipfile.ZipFile(outer, "w") as z:
            z.writestr(f"folder-{depth}/child.zip", content.getvalue())
        content = outer
    sid = source(tmp_path, "three-level.zip", [("outer.zip", content.getvalue())])
    activate(sid)
    assert len(json.loads(library.lookup_vins([VIN2])[VIN2]["path"])) == 4
    # Downloads must read the indexed cache, not re-scan nested source archives.
    monkeypatch.setattr(match, "list_archive_members", lambda *_args, **_kwargs: pytest.fail("archive re-scanned"))
    target = tmp_path / "selected.zip"
    assert library.write_pdf_zip([VIN2, VIN2.lower()], target) == {"count": 1}
    with zipfile.ZipFile(target) as z:
        assert z.testzip() is None
        assert z.namelist() == [f"{VIN2}.pdf"]
        assert z.read(f"{VIN2}.pdf") == b"second original PDF"


def test_conflict_confirmation_and_no_old_content_fallback(tmp_path):
    a = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"old")])
    activate(a)
    b = source(tmp_path, "b.zip", [(f"{VIN}.pdf", b"new")])
    assert activate(b, [VIN])["items"][0]["status"] == "conflict"
    preview = library.delete_source(b)
    assert preview["lostVins"] == [VIN]
    library.delete_source(b, preview["fingerprint"])
    assert not library.lookup_vins([VIN])


def test_conflict_without_confirmation_keeps_original(tmp_path):
    a = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"old")])
    activate(a)
    original = library.lookup_vins([VIN])[VIN]["sha"]
    b = source(tmp_path, "b.zip", [(f"{VIN}.pdf", b"new")])
    activate(b)
    assert library.lookup_vins([VIN])[VIN]["sha"] == original


def test_explicit_old_package_reuses_cache_but_requires_preview(tmp_path):
    a = source(tmp_path, "old.zip", [(f"{VIN}.pdf", b"old")])
    activate(a)
    original = (library.LIBRARY_ROOT / a / "pdfs.zip").read_bytes()
    b = source(tmp_path, "new.zip", [(f"{VIN}.pdf", b"new")])
    activate(b, [VIN])
    delete = library.delete_source(b)
    library.delete_source(b, delete["fingerprint"])
    result = library.import_library_source(tmp_path / "old.zip", "old.zip", "admin", background=False)
    assert result == {"sourceId": a, "duplicate": True, "needsReview": True}
    assert not library.lookup_vins([VIN])
    assert (library.LIBRARY_ROOT / a / "pdfs.zip").read_bytes() == original
    assert activate(a)["items"][0]["status"] == "new"
    assert VIN in library.lookup_vins([VIN])


def test_upload_cleanup_for_new_and_duplicate_sources(tmp_path, monkeypatch):
    from upload_toolkit.upload_engine import create_upload_session, receive_chunk, complete_upload_session
    upload_root = tmp_path / "uploads"
    monkeypatch.setattr(match, "_COC_UPLOAD_SESSION_ROOT", upload_root)
    monkeypatch.setattr(library.CocLibraryIndexRunner, "start", lambda self: None)
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr(f"{VIN}.pdf", b"pdf")
    data = raw.getvalue()
    ids = []
    for _ in range(2):
        session = create_upload_session(upload_root, filename="source.zip", size_bytes=len(data), chunk_size=8 * 1024**2, triggered_by="admin")
        uid = session["uploadId"]
        receive_chunk(upload_root, uid, 1, data)
        complete_upload_session(upload_root, uid)
        result = library.import_library_upload(uid, "admin")
        ids.append(result["sourceId"])
        assert not (upload_root / uid).exists()
    assert ids[0] == ids[1]
    assert (library.LIBRARY_ROOT / ids[0] / "source.zip").read_bytes() == data
    library.CocLibraryIndexRunner(ids[0], library.LIBRARY_ROOT, isolated=False)._run_wrapper()
    deletion = library.delete_source(ids[0])
    library.delete_source(ids[0], deletion["fingerprint"])
    assert not (library.LIBRARY_ROOT / ids[0]).exists()
    assert not list(upload_root.iterdir())


def test_failed_index_cleans_cache_and_nested_work_files(tmp_path, monkeypatch):
    def interrupted(path, **kwargs):
        directory = kwargs["temporary_root"]
        (directory / "jato-coc-archive-interrupted").mkdir()
        (directory / "coc-rar-interrupted").mkdir()
        raise ValueError("memory limit")
    monkeypatch.setattr(library, "list_archive_members", interrupted)
    path = tmp_path / "failed.zip"
    path.write_bytes(b"original")
    result = library.import_library_source(path, "failed.zip", "admin", background=False)
    directory = library.LIBRARY_ROOT / result["sourceId"]
    assert (directory / "source.zip").read_bytes() == b"original"
    assert not (directory / "pdfs.zip").exists()
    assert not list(directory.glob("*interrupted"))
    assert library.library_sources()["items"][0]["status"] == "failed"


def test_serial_index_and_disk_headroom_guards(tmp_path, monkeypatch):
    monkeypatch.setattr(library.CocLibraryIndexRunner, "start", lambda self: None)
    path = tmp_path / "a.zip"
    path.write_bytes(b"first")
    library.import_library_source(path, "a.zip", "admin")
    path.write_bytes(b"second")
    with pytest.raises(HTTPException, match="indexing"):
        library.import_library_source(path, "a.zip", "admin")
    monkeypatch.setattr(library.shutil, "disk_usage", lambda _: SimpleNamespace(free=1))
    with pytest.raises(ValueError, match="disk space"):
        library._check_disk()


def test_discard_transfer_reuses_cleanup_and_checks_owner(tmp_path, monkeypatch):
    from app.api.routes import coc_match as coc_route
    monkeypatch.setattr(match, "_COC_UPLOAD_SESSION_ROOT", tmp_path / "uploads")
    session = match.initiate_coc_match_upload(filename="a.zip", size_bytes=3, triggered_by="admin")
    uid = session["uploadId"]
    match.upload_coc_match_chunk(uid, 1, b"zip")
    with pytest.raises(HTTPException) as denied:
        coc_route.delete_coc_upload_session(uid, SimpleNamespace(name="editor"))
    assert denied.value.status_code == 403
    assert (tmp_path / "uploads" / uid).exists()
    assert coc_route.delete_coc_upload_session(uid, SimpleNamespace(name="admin"))["deleted"]
    assert not (tmp_path / "uploads" / uid).exists()
    with pytest.raises(HTTPException):
        coc_route.delete_coc_upload_session("..", SimpleNamespace(name="admin"))


def test_isolated_index_reuses_monthly_supervisor_and_cleans_after_limit(tmp_path, monkeypatch):
    worker = library._resource_worker()
    captured = {}
    def stop(**kwargs):
        captured.update(kwargs)
        directory = kwargs["receipt_path"].parent
        (directory / "pdfs.zip").write_bytes(b"partial")
        kwargs["on_sample"]({"rssBytes": 200, "peakRssBytes": 200, "rssLimitBytes": 150})
        worker._atomic_write_json(kwargs["receipt_path"], {"returnCode": -15, "terminationReason": "rss_limit"})
    monkeypatch.setattr(worker, "_supervise_digest_upload", stop)
    monkeypatch.setattr(worker, "_cgroup_memory_snapshot", lambda: None)
    monkeypatch.setattr(library.CocLibraryIndexRunner, "_launch_supervised", lambda self: self._run_isolated())
    monkeypatch.setattr(library.CocLibraryIndexRunner, "start", lambda self: self._run_wrapper())
    path = tmp_path / "a.zip"
    path.write_bytes(b"source")
    result = library.import_library_source(path, "a.zip", "admin")
    assert captured["worker_command"][1] == "-c"
    directory = library.LIBRARY_ROOT / result["sourceId"]
    assert (directory / "source.zip").exists() and not (directory / "pdfs.zip").exists()
    state = library.library_sources()
    assert state["items"][0]["resources"]["peakRssBytes"] == 200
    assert state["items"][0]["status"] == "failed"
    assert state["diskFreeBytes"] > 0 and state["libraryBytes"] > 0


def test_real_isolated_small_zip_indexes_without_touching_orders(tmp_path):
    import time
    path = tmp_path / "small.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(f"{VIN}.pdf", b"pdf bytes")
    result = library.import_library_source(path, "small.zip", "admin")
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        state = load_job_state(state_path(library.LIBRARY_ROOT, result["sourceId"]))
        resource_path = library.LIBRARY_ROOT / result["sourceId"] / "resources.json"
        if state["status"] in {"success", "failed"} and resource_path.exists() and load_job_state(resource_path).get("status") == "finished":
            break
        time.sleep(0.05)
    assert state["status"] == "success", state
    resources = library.library_sources()["items"][0]["resources"]
    assert resources["rssLimitBytes"] > 0
    assert resources["status"] == "finished"
    assert not library.lookup_vins([VIN])
    activate(result["sourceId"])
    assert VIN in library.lookup_vins([VIN])


def test_fresh_watchdog_keeps_other_api_worker_from_marking_index_interrupted(tmp_path):
    from datetime import datetime, timedelta, UTC
    directory = library.LIBRARY_ROOT / "other-worker"
    directory.mkdir(parents=True)
    with library._db() as conn:
        conn.execute("INSERT INTO sources(id,filename,status,owner,source_hash) VALUES('other-worker','a.zip','indexing','admin','x')")
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    (directory / "state.json").write_text(json.dumps({"status": "success", "updatedAt": old}))
    (directory / "resources.json").write_text(json.dumps({"status": "running", "updatedAt": datetime.now(UTC).isoformat()}))
    assert library.library_sources()["items"][0]["status"] == "indexing"
    (directory / "resources.json").write_text(json.dumps({"status": "running", "updatedAt": old}))
    assert library.library_sources()["items"][0]["status"] == "failed"


def test_stale_preview_rejected(tmp_path):
    a = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"a")])
    old = library.preview_source(a)
    b = source(tmp_path, "b.zip", [(f"{VIN2}.pdf", b"b")])
    activate(b)
    with pytest.raises(HTTPException, match="Library changed"):
        library.activate_source(a, old["fingerprint"], [])


def test_internal_different_content_requires_repack(tmp_path):
    sid = source(tmp_path, "a.zip", [(f"one/{VIN}.pdf", b"one"), (f"two/{VIN}.pdf", b"two")])
    preview = library.preview_source(sid)
    assert preview["items"][0]["status"] == "invalid"
    with pytest.raises(HTTPException):
        library.activate_source(sid, preview["fingerprint"], [])


def test_exact_source_is_not_imported_twice(tmp_path):
    sid = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"a")])
    duplicate = library.import_library_source(tmp_path / "a.zip", "renamed.zip", "admin", background=False)
    assert duplicate == {"sourceId": sid, "duplicate": True, "needsReview": False}


def test_missing_download_fails_before_output(tmp_path):
    with pytest.raises(HTTPException):
        library.write_pdf_zip([VIN], tmp_path / "absent.zip")
    assert not (tmp_path / "absent.zip").exists()


def test_pi_counts_and_market_authorization(tmp_path, monkeypatch):
    sid = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"a")])
    activate(sid)
    units = [SimpleNamespace(car_code="a", vin=VIN, country_code="CH"),
             SimpleNamespace(car_code="b", vin=VIN2, country_code="CH"),
             SimpleNamespace(car_code="c", vin=None, country_code="CH"),
             SimpleNamespace(car_code="d", vin=VIN, country_code="SE")]
    monkeypatch.setattr(route.vehicle_repo, "get_header_by_code", lambda *args: object())
    monkeypatch.setattr(route.vehicle_repo, "list_vehicles_for_bulk_update", lambda *args, **kwargs: units)
    calls = []
    def validate(session, user, country):
        calls.append(country)
        if country != "CH":
            raise HTTPException(403, "denied")
    monkeypatch.setattr(route, "_validate_country", validate)
    monkeypatch.setattr(route, "ordering_brands", lambda *args: None)
    result = route.lookup_pi_cocs("PI", None, SimpleNamespace(role="admin"))
    assert (result["total"], result["available"], result["awaitingVin"], result["missing"]) == (3, 1, 1, 1)
    assert calls == ["CH", "SE"]
    with pytest.raises(HTTPException):
        route.download_pi_cocs("PI", {"carCodes": ["d"]}, None, SimpleNamespace())


@pytest.mark.parametrize("confirmation", [None, {}, {"a": VIN}, {"a": 123}])
def test_pi_download_rejects_missing_or_changed_vin_confirmation(monkeypatch, confirmation):
    unit = SimpleNamespace(car_code="a", vin=VIN2, country_code="CH")
    monkeypatch.setattr(route, "_coc_pi_vehicles", lambda *_: [unit])
    writes = []
    monkeypatch.setattr(library, "write_pdf_zip", lambda *_: writes.append(True))
    with pytest.raises(HTTPException) as error:
        route.download_pi_cocs("PI", {"carCodes": ["a"], "vinsByCarCode": confirmation}, None, SimpleNamespace())
    assert error.value.status_code == 409
    assert "重新查库" in error.value.detail and not writes


def test_pi_download_keeps_confirmed_vin_pdf_bytes(tmp_path, monkeypatch):
    sid = source(tmp_path, "confirmed.zip", [(f"{VIN}.pdf", b"original PDF bytes")])
    activate(sid)
    monkeypatch.setattr(route, "_coc_pi_vehicles", lambda *_: [SimpleNamespace(car_code="a", vin=VIN, country_code="CH")])
    response = route.download_pi_cocs("PI", {"carCodes": ["a"], "vinsByCarCode": {"a": VIN}}, None, SimpleNamespace())
    try:
        with zipfile.ZipFile(response.path) as archive:
            assert archive.namelist() == [f"{VIN}.pdf"]
            assert archive.read(f"{VIN}.pdf") == b"original PDF bytes"
    finally:
        asyncio.run(response.background())
    assert not Path(response.path).exists()


def test_100000_vin_index_uses_batched_lookup(tmp_path):
    vins = [f"LVUGTB220{i:08d}" for i in range(100000)]
    with library._db() as conn:
        conn.execute("INSERT INTO sources(id,filename,status,owner,source_hash) VALUES('s','s.zip','active','admin','sha')")
        conn.executemany("INSERT INTO members(source_id,vin,path,cache_name,sha,bytes,selected) VALUES('s',?,'[]','p','sha',1,1)", [(v,) for v in vins])
    with library._db() as conn:
        statements = []
        conn.set_trace_callback(statements.append)
        result = library.lookup_vins(vins[:1200], conn=conn)
    assert len(result) == 1200
    assert len(statements) == 3


def test_unconfigured_library_is_explicit(monkeypatch):
    monkeypatch.setattr(library, "LIBRARY_ROOT", None)
    assert library.library_sources()["configured"] is False
    with pytest.raises(HTTPException) as exc:
        library.lookup_vins([VIN])
    assert exc.value.status_code == 503


def test_interrupted_index_can_be_removed_without_recovery_system(tmp_path):
    from datetime import datetime, timedelta, UTC
    sid = "interrupted"
    directory = library.LIBRARY_ROOT / sid
    directory.mkdir(parents=True)
    with library._db() as conn:
        conn.execute("INSERT INTO sources(id,filename,status,owner,source_hash) VALUES(?, 'a.zip','indexing','admin','sha')", (sid,))
    (directory / "state.json").write_text(json.dumps({"jobId": sid, "status": "running", "updatedAt": (datetime.now(UTC) - timedelta(hours=2)).isoformat()}))
    assert library.library_sources()["items"][0]["status"] == "failed"
    preview = library.delete_source(sid)
    library.delete_source(sid, preview["fingerprint"])
    assert not directory.exists()


def test_comparison_library_queries_registry_only(tmp_path, monkeypatch):
    sid = source(tmp_path, "a.zip", [(f"{VIN}.pdf", b"a"), (f"{VIN2}.pdf", b"b")])
    activate(sid)
    root = tmp_path / "jobs"
    directory = root / "job"
    directory.mkdir(parents=True)
    archive = directory / "empty.zip"
    with zipfile.ZipFile(archive, "w"):
        pass
    monkeypatch.setattr(match, "COC_MATCH_JOB_ROOT", root)
    monkeypatch.setattr(match, "COC_DB_PATH", root / "history.db")
    monkeypatch.setattr(match, "read_excel_rows", lambda _: [{"chassis": VIN, "country": "CH", "model": ""}])
    persist_job_state(state_path(root, "job"), {"jobId": "job", "status": "queued", "useLibrary": True})
    runner = match.CocMatchJobRunner(job_id="job", state_dir=root, excel_path=directory / "registry.xlsx", archive_path=archive,
                                   country="CH", month="2026-10", file_ext=".pdf", triggered_by="test")
    runner.run()
    result = load_job_state(state_path(root, "job"))
    assert result["matchedCount"] == 1 and result["missingCount"] == 0
    assert result["extraFileCount"] == 0  # other global VIN is not a mismatch


@pytest.mark.skipif(not os.getenv("JATO_TEST_COC_SOURCE"), reason="User source verification requires deployed RAR extractor")
def test_approved_real_rar_index_download(tmp_path):
    path = Path(os.environ["JATO_TEST_COC_SOURCE"])
    assert library._hash_file(path) == "3af7405c5cceb3f23e449347f888582c7c8acb76937da102de445b821fb44d3c"
    result = library.import_library_source(path, path.name, "test", background=False)
    sid = result["sourceId"]
    state = load_job_state(state_path(library.LIBRARY_ROOT, sid))
    assert state["status"] == "success", state
    preview = activate(sid)
    assert len(preview["items"]) == 1958
    vins = [item["vin"] for item in preview["items"]]
    target = tmp_path / "all-cocs.zip"
    library.write_pdf_zip(vins, target)
    with zipfile.ZipFile(target) as archive:
        assert len(archive.infolist()) == 1958
        assert archive.testzip() is None
    print("Real RAR: 1958 indexed VINs; flattened ZIP 1958 PDFs; CRC verified; source SHA256 unchanged")
