from app.services.coc_match_service import find_archive_only_files, match_cocs


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
