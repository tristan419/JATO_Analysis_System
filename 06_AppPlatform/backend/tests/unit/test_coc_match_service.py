import zipfile
from pathlib import Path

from app.services.coc_match_service import (
    classify_coc_difference,
    find_archive_only_files,
    list_archive_files,
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


def test_classify_coc_difference_detects_one_sided_and_bidirectional() -> None:
    assert classify_coc_difference(0, 0) == "matched"
    assert classify_coc_difference(1, 0) == "missing_archive_files"
    assert classify_coc_difference(0, 1) == "archive_only_files"
    assert classify_coc_difference(1, 1) == "bidirectional_mismatch"


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
