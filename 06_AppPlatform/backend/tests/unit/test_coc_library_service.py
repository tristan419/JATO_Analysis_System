import io
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
    assert duplicate == {"sourceId": sid, "duplicate": True}


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
    result = route.lookup_pi_cocs("PI", None, SimpleNamespace())
    assert (result["total"], result["available"], result["awaitingVin"], result["missing"]) == (3, 1, 1, 1)
    assert calls == ["CH", "SE"]
    with pytest.raises(HTTPException):
        route.download_pi_cocs("PI", {"carCodes": ["d"]}, None, SimpleNamespace())


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
