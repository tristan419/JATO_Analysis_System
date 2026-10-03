import io
import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from upload_toolkit.job_engine import load_job_state, persist_job_state, state_path

import app.services.coc_match_service as coc_match_service
from app.services.coc_match_service import (
    CocMatchJobRunner,
    _build_coc_match_failure_result,
    classify_coc_difference,
    find_archive_only_files,
    list_archive_files,
    list_archive_members,
    match_cocs,
    read_excel_rows,
    _list_rar_files_python,
)


def test_match_cocs_reports_missing_excel_rows() -> None:
    rows = [
        {"chassis": "A001", "model": "J7", "country": "CZ"},
        {"chassis": "A002", "model": "J7", "country": "CZ"},
    ]

    matched, missing = match_cocs(rows, {"A001"})

    assert [item["chassis"] for item in matched] == ["A001"]
    assert [item["chassis"] for item in missing] == ["A002"]


def test_find_archive_only_files_reports_extra_archive_files() -> None:
    rows = [
        {"chassis": "A001", "model": "J7", "country": "CZ"},
        {"chassis": "A002", "model": "J7", "country": "CZ"},
    ]

    archive_only = find_archive_only_files(rows, {"A001", "A002", "A003", "A004"})

    assert archive_only == [{"filename": "A003"}, {"filename": "A004"}]


def test_read_excel_rows_finds_vin_header_in_multi_column_sheet(tmp_path: Path) -> None:
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Exported COC registry"])
    sheet.append(["Order", "Model", "VIN", "Country", "Comment"])
    sheet.append([1, "J7", "LVUGTB220TDE99425", "CZ", "ok"])
    sheet.append([2, "J7", "LVUGTB220TDE99426", "CZ", "ok"])
    sheet.append([3, "J7", None, "CZ", "skip"])
    path = tmp_path / "registry.xlsx"
    workbook.save(path)

    assert read_excel_rows(path) == [
        {"chassis": "LVUGTB220TDE99425", "model": "J7", "country": "CZ"},
        {"chassis": "LVUGTB220TDE99426", "model": "J7", "country": "CZ"},
    ]


def test_read_excel_rows_infers_headerless_single_vin_column(tmp_path: Path) -> None:
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.append([" LVUGTBHD5TC114629 "])
    sheet.append(["not-a-vin-note"])
    sheet.append(["LNNBDDEH0TG030197"])
    sheet.append([None])
    sheet.append(["LNNBDDEH0TG030457"])
    path = tmp_path / "headerless-vins.xlsx"
    workbook.save(path)

    assert read_excel_rows(path) == [
        {"chassis": "LVUGTBHD5TC114629", "model": "", "country": ""},
        {"chassis": "LNNBDDEH0TG030197", "model": "", "country": ""},
        {"chassis": "LNNBDDEH0TG030457", "model": "", "country": ""},
    ]


def test_classify_coc_difference_detects_one_sided_and_bidirectional() -> None:
    assert classify_coc_difference(0, 0) == "matched"
    assert classify_coc_difference(1, 0) == "missing_archive_files"
    assert classify_coc_difference(0, 1) == "archive_only_files"
    assert classify_coc_difference(1, 1) == "bidirectional_mismatch"


def test_build_coc_match_failure_result_explains_excel_stage() -> None:
    state = {
        "phase": "reading_excel",
        "excelFilename": "vin.xlsx",
        "archiveFilename": "coc.rar",
        "fileExt": ".pdf",
        "country": "SE",
        "month": "2026-07",
    }

    result = _build_coc_match_failure_result(
        state,
        HTTPException(status_code=400, detail="Excel 未找到 VIN / Chassis / 车架号 表头。"),
    )

    assert result["stage"] == "reading_excel"
    assert result["stageLabel"] == "Excel 读取"
    assert result["message"] == "Excel 未找到 VIN / Chassis / 车架号 表头。"
    assert "VIN / Chassis / 车架号" in result["suggestion"]
    assert result["retryable"] is False
    assert result["actionLabel"] == "重新上传"
    assert result["excelFilename"] == "vin.xlsx"
    assert result["archiveFilename"] == "coc.rar"


def test_read_excel_rows_fails_when_vin_column_has_no_valid_rows(tmp_path: Path) -> None:
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["VIN", "Model", "Country"])
    sheet.append([None, "J7", "CZ"])
    sheet.append(["", "J7", "CZ"])
    path = tmp_path / "empty-vin.xlsx"
    workbook.save(path)

    with pytest.raises(HTTPException) as exc:
        read_excel_rows(path)

    assert "没有有效 VIN 数据" in str(exc.value.detail)


def test_coc_match_runner_persists_failure_result_for_excel_error(tmp_path: Path) -> None:
    job_root = tmp_path / "jobs"
    job_id = "coc-match-test"
    job_dir = job_root / job_id
    job_dir.mkdir(parents=True)
    excel_path = job_dir / "excel-bad.xlsx"
    archive_path = job_dir / "archive-coc.zip"
    excel_path.write_text("not an excel workbook", encoding="utf-8")
    with zipfile.ZipFile(archive_path, "w") as zf:
        zf.writestr("A001.pdf", b"pdf")
    persist_job_state(
        state_path(job_root, job_id),
        {
            "jobId": job_id,
            "status": "queued",
            "phase": "pending",
            "country": "SE",
            "month": "2026-07",
            "fileExt": ".pdf",
            "excelFilename": "bad.xlsx",
            "archiveFilename": "coc.zip",
            "failureResult": None,
        },
    )
    runner = CocMatchJobRunner(
        job_id=job_id,
        state_dir=job_root,
        excel_path=excel_path,
        archive_path=archive_path,
        country="SE",
        month="2026-07",
        file_ext=".pdf",
        triggered_by="test",
    )

    with pytest.raises(Exception):
        runner.run()

    state = load_job_state(state_path(job_root, job_id))
    assert state["phase"] == "reading_excel"
    assert state["failureResult"]["stage"] == "reading_excel"
    assert state["failureResult"]["stageLabel"] == "Excel 读取"
    assert state["failureResult"]["retryable"] is False
    assert state["failureResult"]["excelFilename"] == "bad.xlsx"


def test_coc_match_runner_succeeds_with_warning_when_archive_has_no_target_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from openpyxl import Workbook

    job_root = tmp_path / "jobs"
    monkeypatch.setattr(coc_match_service, "COC_MATCH_JOB_ROOT", job_root)
    monkeypatch.setattr(coc_match_service, "COC_DB_PATH", job_root / "coc_match_history.db")
    job_id = "coc-match-no-target-files"
    job_dir = job_root / job_id
    job_dir.mkdir(parents=True)

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["VIN", "Model", "Country"])
    sheet.append(["LVUGTB220TDE99425", "J7", "SE"])
    excel_path = job_dir / "excel-vin.xlsx"
    workbook.save(excel_path)

    archive_path = job_dir / "archive-coc.zip"
    with zipfile.ZipFile(archive_path, "w") as zf:
        zf.writestr("notes.txt", b"no pdf here")

    persist_job_state(
        state_path(job_root, job_id),
        {
            "jobId": job_id,
            "status": "queued",
            "phase": "pending",
            "country": "SE",
            "month": "2026-07",
            "fileExt": ".pdf",
            "excelFilename": "vin.xlsx",
            "archiveFilename": "coc.zip",
            "failureResult": None,
        },
    )
    runner = CocMatchJobRunner(
        job_id=job_id,
        state_dir=job_root,
        excel_path=excel_path,
        archive_path=archive_path,
        country="SE",
        month="2026-07",
        file_ext=".pdf",
        triggered_by="test",
    )

    runner.run()

    state = load_job_state(state_path(job_root, job_id))
    assert state["status"] == "success"
    assert state["matchedCount"] == 0
    assert state["missingCount"] == 1
    assert "没有找到 .PDF COC 文件" in state["inputWarning"]


def test_get_coc_match_job_marks_stale_running_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_id = "coc-match-stale"
    monkeypatch.setattr(coc_match_service, "COC_MATCH_JOB_ROOT", tmp_path)
    monkeypatch.setattr(coc_match_service, "_COC_MATCH_STALE_AFTER_SECONDS", -1)
    persist_job_state(
        state_path(tmp_path, job_id),
        {
            "jobId": job_id,
            "jobType": "match",
            "status": "running",
            "phase": "listing_archive",
            "country": "SE",
            "month": "2026-07",
            "fileExt": ".pdf",
            "excelFilename": "vin.xlsx",
            "archiveFilename": "coc.zip",
            "createdAt": "2000-01-01T00:00:00",
            "startedAt": "2000-01-01T00:00:00",
            "updatedAt": "2000-01-01T00:00:00",
            "failureResult": None,
        },
    )

    payload = coc_match_service.get_coc_match_job(job_id)

    assert payload["status"] == "failed"
    assert payload["phase"] == "failed"
    assert payload["failureResult"]["stage"] == "listing_archive"
    assert payload["failureResult"]["retryable"] is True
    assert payload["failureResult"]["actionLabel"] == "重试"


def test_list_archive_files_reads_zip_without_external_unzip(tmp_path: Path) -> None:
    archive = tmp_path / "coc.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("nested/A001.pdf", b"pdf")
        zf.writestr("A002.PDF", b"pdf")
        zf.writestr("ignore.txt", b"txt")

    assert list_archive_files(archive, [".pdf"]) == {"A001", "A002"}


def test_rar5_python_fallback_reads_sample_archive_when_available() -> None:
    sample = Path("/Users/litristan/Downloads/COCtrack/CZ/27.rar")
    if not sample.exists():
        return

    names = _list_rar_files_python(sample, [".pdf"])

    assert "LVUGTB220TDE99425" in names
    assert len(names) >= 1


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    content = io.BytesIO()
    with zipfile.ZipFile(content, "w") as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return content.getvalue()


def test_archive_paths_keep_duplicates_and_ignore_sidecars_without_reading_pdfs(tmp_path, monkeypatch):
    path = tmp_path / "photos.zip"
    path.write_bytes(_zip_bytes({
        "country/trim/A001.pdf": b"one",
        "other/A001.PDF": b"two",
        "__MACOSX/country/._A001.pdf": b"sidecar",
        "nested/._B002.pdf": b"sidecar",
        "ignore.xlsx": b"not a coc",
    }))
    monkeypatch.setattr(zipfile.ZipFile, "read", lambda *args: pytest.fail("PDF body must not be read"))
    assert list_archive_members(path) == [
        {"memberPath": ["country/trim/A001.pdf"], "stem": "A001"},
        {"memberPath": ["other/A001.PDF"], "stem": "A001"},
    ]
    assert list_archive_files(path) == {"A001"}


def test_nested_zip_works_in_existing_match_entry_and_retains_path_chain(tmp_path):
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({
        "market/group/inner.ZIP": _zip_bytes({"deeper/level/A001.pdf": b"pdf"}),
        "A002.pdf": b"pdf",
    }))
    assert list_archive_members(path)[0] == {
        "memberPath": ["market/group/inner.ZIP", "deeper/level/A001.pdf"], "stem": "A001",
    }
    assert list_archive_files(path) == {"A001", "A002"}


@pytest.mark.parametrize("limits, message", [
    ({"max_depth": 0}, "嵌套超限"),
    ({"max_members": 1}, "文件数超限"),
    ({"max_nested_bytes": 1}, "大小超限"),
])
def test_nested_limits_fail_instead_of_returning_partial_missing(tmp_path, limits, message):
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({"A001.pdf": b"pdf", "inner.zip": _zip_bytes({"A002.pdf": b"pdf"})}))
    with pytest.raises(ValueError, match=message):
        list_archive_members(path, **limits)


def test_nested_bytes_are_cumulative_and_directory_depth_is_not_archive_depth(tmp_path):
    child = _zip_bytes({"a/b/c/d/e/f/g/A001.pdf": b"pdf"})
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({"one.zip": child, "two.zip": child}))
    assert len(list_archive_members(path, max_depth=1, max_nested_bytes=len(child) * 2)) == 2
    with pytest.raises(ValueError, match="大小超限"):
        list_archive_members(path, max_nested_bytes=len(child) * 2 - 1)


def test_failed_nested_scan_cleans_temporary_archives(tmp_path, monkeypatch):
    real_temporary = coc_match_service.tempfile.TemporaryDirectory
    monkeypatch.setattr(coc_match_service.tempfile, "TemporaryDirectory", lambda **kw: real_temporary(dir=tmp_path, **kw))
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({"../child.zip": b"not a zip"}))
    with pytest.raises(ValueError, match="无法读取 ZIP"):
        list_archive_members(path)
    assert list(tmp_path.iterdir()) == [path]


def test_zip_inside_rar_inside_zip_uses_exact_member_and_never_reads_pdf(tmp_path, monkeypatch):
    child = _zip_bytes({"A001.pdf": b"pdf"})
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({"market/inner.rar": b"rar"}))
    monkeypatch.setattr(coc_match_service.shutil, "which", lambda name: "/usr/bin/7z" if name == "7z" else None)
    monkeypatch.setattr(coc_match_service, "_rar_member_listing", lambda path, tool: [
        ("COC/A002.pdf", 3), ("-group*/inner.zip", len(child)),
    ])
    calls = []
    def extract(command, **kwargs):
        calls.append(command)
        assert command[-2:] == ["--", "-group*/inner.zip"]
        assert "-spd" in command and kwargs["timeout"] == 60
        kwargs["stdout"].write(child)
        return SimpleNamespace(returncode=0, stdout=child)
    monkeypatch.setattr(coc_match_service.subprocess, "run", extract)
    result = list_archive_members(path)
    assert result == [
        {"memberPath": ["market/inner.rar", "COC/A002.pdf"], "stem": "A002"},
        {"memberPath": ["market/inner.rar", "-group*/inner.zip", "A001.pdf"], "stem": "A001"},
    ]
    assert len(calls) == 1


def test_nested_zip_is_streamed_not_read_into_memory(tmp_path, monkeypatch):
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({"inner.zip": _zip_bytes({"A001.pdf": b"pdf"})}))
    def reject_read(*args, **kwargs):
        raise AssertionError("Nested archives must stream through open(), not read()")
    monkeypatch.setattr(zipfile.ZipFile, "read", reject_read)
    assert list_archive_files(path) == {"A001"}


def test_rar_technical_listing_preserves_names_and_skips_directories(monkeypatch, tmp_path):
    monkeypatch.setattr(coc_match_service.subprocess, "run", lambda *a, **kw: SimpleNamespace(
        returncode=0, stdout="Path = folder\nSize = 0\nFolder = +\n\nPath = folder/A = 1.pdf\nSize = 123\nAttributes = A\n",
    ))
    assert coc_match_service._rar_member_listing(tmp_path / "input.rar", "7z") == [("folder/A = 1.pdf", 123)]


def test_nested_rar_without_extractor_is_explicit_error(tmp_path, monkeypatch):
    path = tmp_path / "outer.zip"
    path.write_bytes(_zip_bytes({"inner.rar": b"rar"}))
    monkeypatch.setattr(coc_match_service.shutil, "which", lambda name: None)
    with pytest.raises(ValueError, match="改传 ZIP"):
        list_archive_members(path)


def test_legacy_rar_header_listing_cannot_silently_skip_embedded_archive(tmp_path, monkeypatch):
    monkeypatch.setattr(coc_match_service.shutil, "which", lambda name: None)
    monkeypatch.setattr(coc_match_service, "_list_rar_files_python", lambda path, extensions: {"inner"})
    with pytest.raises(ValueError, match="RAR 内含子包"):
        list_archive_files(tmp_path / "input.rar")


def test_rar_command_failure_and_incomplete_nested_output_reject_scan(tmp_path, monkeypatch):
    path = tmp_path / "input.rar"
    path.write_bytes(b"rar")
    monkeypatch.setattr(coc_match_service.shutil, "which", lambda name: "/usr/bin/7z")
    monkeypatch.setattr(coc_match_service.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=2, stdout=""))
    with pytest.raises(ValueError, match="无法读取 RAR"):
        list_archive_members(path)
    monkeypatch.setattr(coc_match_service, "_rar_member_listing", lambda path, tool: [("inner.zip", 100)])
    monkeypatch.setattr(coc_match_service.subprocess, "run", lambda *a, **kw: SimpleNamespace(returncode=0, stdout=b"short"))
    with pytest.raises(ValueError, match="不完整"):
        list_archive_members(path)


def test_rar_timeout_is_actionable_and_retryable(tmp_path, monkeypatch):
    def timed_out(command, **kwargs):
        raise subprocess.TimeoutExpired(command, 60)
    monkeypatch.setattr(coc_match_service.subprocess, "run", timed_out)
    with pytest.raises(ValueError, match="超时") as failure:
        coc_match_service._rar_member_listing(tmp_path / "input.rar", "7z")
    result = _build_coc_match_failure_result({"phase": "listing_archive"}, failure.value)
    assert result["retryable"] is True
    assert result["actionLabel"] == "重试"


@pytest.mark.parametrize("broken", [False, True])
def test_existing_job_reads_nested_archive_or_fails_without_partial_report(tmp_path, monkeypatch, broken):
    job_id = "nested-job"
    job_dir = tmp_path / job_id
    job_dir.mkdir()
    monkeypatch.setattr(coc_match_service, "COC_MATCH_JOB_ROOT", tmp_path)
    monkeypatch.setattr(coc_match_service, "COC_DB_PATH", tmp_path / "history.db")
    monkeypatch.setattr(coc_match_service, "read_excel_rows", lambda path: [
        {"chassis": "A001", "model": "test", "country": "CZ"},
        {"chassis": "A002", "model": "test", "country": "CZ"},
    ])
    archive = job_dir / "input.zip"
    archive.write_bytes(_zip_bytes({
        "A001.pdf": b"pdf",
        "inner.zip": b"broken" if broken else _zip_bytes({"deep/A002.pdf": b"pdf"}),
    }))
    persist_job_state(state_path(tmp_path, job_id), {"jobId": job_id, "status": "queued", "phase": "pending"})
    runner = CocMatchJobRunner(job_id=job_id, state_dir=tmp_path, excel_path=job_dir / "registry",
        archive_path=archive, country="CZ", month="2026-09", file_ext=".pdf", triggered_by="test")
    if broken:
        runner._run_wrapper()
        state = load_job_state(state_path(tmp_path, job_id))
        assert state["status"] == "failed" and state["failureResult"]["stage"] == "listing_archive"
        assert not (job_dir / "report.html").exists()
    else:
        runner.run()
        state = load_job_state(state_path(tmp_path, job_id))
        assert state["status"] == "success" and state["matchedCount"] == 2 and state["missingCount"] == 0
        assert (job_dir / "report.html").exists()
