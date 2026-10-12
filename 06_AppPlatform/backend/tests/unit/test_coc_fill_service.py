from __future__ import annotations

from datetime import date
from dataclasses import replace
from contextlib import nullcontext
from types import SimpleNamespace
import pytest
from fastapi import HTTPException
from app.services import coc_fill_service as service

from app.services.coc_fill_service import (
    MaterialGroupRow,
    WvtaCocRecord,
    resolve_fill_decision,
)


def _row(
    material_group: str = "T7000Z5**MY0013",
    *,
    country: str | None = "GQ",
    production_start: date | None = date(2026, 5, 11),
    production_end: date | None = date(2026, 5, 28),
    existing_wvta: str | None = None,
    existing_coc: str | None = None,
) -> MaterialGroupRow:
    return MaterialGroupRow(
        sheet_name="Sheet1",
        row_number=6,
        material_group=material_group,
        material_no=None,
        model="T13J BEV",
        country=country,
        production_date_raw="2026年5月11-5月28",
        production_date_start=production_start,
        production_date_end=production_end,
        wvta_cell="I6",
        coc_cell="J6",
        existing_wvta=existing_wvta,
        existing_coc=existing_coc,
        header_inferred=False,
    )


def _record(
    material_group: str,
    wvta_no: str,
    coc_no: str,
    *,
    valid_from: date | None,
    valid_to: date | None,
    comments: str | None = None,
) -> WvtaCocRecord:
    return WvtaCocRecord(
        material_group=material_group,
        wvta_no=wvta_no,
        coc_no=coc_no,
        brand="JAECOO",
        model="T13J",
        powertrain="BEV",
        version="舒适型-FWD",
        sales_name="JAECOO5 EV",
        valid_from=valid_from,
        valid_to=valid_to,
        comments=comments,
        page_number=2,
        table_row_number=35,
    )


def test_resolve_fill_decision_is_strict_by_default_for_multiple_candidates() -> None:
    material = "T7000Z5**MY0013"
    records = [
        _record(
            material,
            "e4*2018/858*00273*02",
            "00273-02&402&104V&COC002-宁德",
            valid_from=date(2026, 3, 11),
            valid_to=date(2026, 4, 14),
        ),
        _record(
            material,
            "e4*2018/858*00273*03",
            "00273-03&402&104V&COC002-宁德",
            valid_from=date(2026, 4, 15),
            valid_to=None,
            comments="宁德电池",
        ),
    ]

    decision = resolve_fill_decision(
        _row(material),
        {material: records},
        overwrite_existing=False,
    )

    assert decision.status == "ambiguous"
    assert decision.written_wvta is None
    assert decision.written_coc is None


def _pdf_row(material="T7000Z5**MY0013", wvta="e4*2018/858*00273*03", coc="00273-03&402&104V&COC002", first="15/4/2026", last="/"):
    return ["JAECOO", "T13J", "BEV", "Select", material, wvta, coc, "IP", "V", "Tyre", "JAECOO", "JAECOO5 EV", first, last, "Note"]


def _parse_fake_pdf(monkeypatch, rows, lines=()):
    import pdfplumber
    boxes = [SimpleNamespace(cells=[(col*100, row*40, (col+1)*100, (row+1)*40) for col in range(len(values))]) for row, values in enumerate(rows)]
    table = SimpleNamespace(extract=lambda: rows, rows=boxes)
    page = SimpleNamespace(find_tables=lambda: [table], lines=lines)
    monkeypatch.setattr(pdfplumber, "open", lambda _path: nullcontext(SimpleNamespace(pages=[page])))
    return service.extract_wvta_coc_records(None)


def test_pdf_revision_spaces_brand_and_merged_identity(monkeypatch):
    first = _pdf_row(wvta="e4*2018/858*00273*03 Rev. 01", coc="00273-03 Rev. 01 &402&104V&COC002")
    second = _pdf_row(material="T7000Z5**MY0031", first="7/8/2026")
    second[:4] = [None, None, None, "Select"]
    records = _parse_fake_pdf(monkeypatch, [first, second])
    assert records[0].wvta_no.endswith("Rev.01") and records[0].coc_no.startswith("00273-03Rev.01&")
    assert records[1].model == "T13J" and records[1].powertrain == "BEV"
    assert records[1].brand == "JAECOO" and records[1].valid_from == date(2026, 8, 7)


def test_pdf_parallel_source_lines_are_split_with_individual_cancellation(monkeypatch):
    row = _pdf_row(material="T7000Z5**MY0013\nT7000Z5**MY0031", first="15/4/2026\n7/8/2026")
    line = {"x0":420,"x1":480,"top":30,"bottom":30,"stroking_color":(1,0,0)}
    records = _parse_fake_pdf(monkeypatch, [row], [line])
    assert len(records) == 2
    assert records[0].source_status == "recorded"
    assert records[1].source_status == "cancelled"
    assert records[1].valid_from == date(2026, 8, 7)


def test_pdf_unaligned_or_reversed_source_is_review_not_usable(monkeypatch):
    row = _pdf_row(material="T7000Z5**MY0013\nT7000Z5**MY0031", coc="00273-03&COC001\n00273-03&COC002\n00273-03&COC003")
    records = _parse_fake_pdf(monkeypatch, [row])
    assert {record.source_status for record in records} == {"review"}
    decision = resolve_fill_decision(_row(), {records[0].material_group: records}, overwrite_existing=False)
    assert decision.written_coc is None and "待核对" in decision.reason
    records = _parse_fake_pdf(monkeypatch, [_pdf_row(first="7/8/2026", last="6/8/2026")])
    assert records[0].source_status == "review"


@pytest.mark.parametrize("status", ["cancelled", "review"])
def test_unusable_source_is_never_automatically_filled(status):
    record = replace(_record("T7000Z5**MY0013", "WVTA", "COC", valid_from=None, valid_to=None), source_status=status)
    decision = resolve_fill_decision(_row(), {record.material_group:[record]}, overwrite_existing=False, conflict_strategy="date_country")
    assert decision.written_wvta is None


def test_reference_scopes_exact_brand_base_and_country_and_keeps_old_on_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "COC_MATCH_JOB_ROOT", tmp_path)
    pdf = tmp_path / "source.pdf"; pdf.write_bytes(b"fixture")
    monkeypatch.setattr(service, "_get_fill_assembled_path", lambda _id: pdf)
    record = _record("T7000Z5**MY0013", "WVTA", "COC", valid_from=date(2026,3,11), valid_to=None)
    poland = replace(record, coc_no="COC-波兰专用", comments="Note")
    foreign = replace(record, material_group="OTHER", brand="OMODA")
    monkeypatch.setattr(service, "extract_wvta_coc_records", lambda _path: [record,poland,foreign])
    digest = service.sha256_hex_for_path(pdf)
    result = service.set_wvta_reference("test-upload", triggered_by="admin", expected_sha256=digest)
    assert result["recordCount"] == 3 and len(result["sha256"]) == 64
    nodes = [{"bomTemplate":record.material_group, "brand":"JAECOO"}]
    response = service.read_wvta_reference("CH", nodes)
    assert len(response["records"]) == 1
    assert len(service.read_wvta_reference("PL", nodes)["records"]) == 2
    assert service.read_wvta_reference("CH", [dict(nodes[0], brand="OMODA")])["records"] == []
    old = (tmp_path / "_wvta_reference.json").read_bytes()
    def fail(_path): raise HTTPException(422, "Unreadable PDF")
    monkeypatch.setattr(service, "extract_wvta_coc_records", fail)
    with pytest.raises(HTTPException): service.set_wvta_reference("bad-upload", triggered_by="admin", expected_sha256=digest)
    with pytest.raises(HTTPException) as error: service.set_wvta_reference("bad-upload", triggered_by="admin", expected_sha256="wrong")
    assert error.value.status_code == 400
    assert (tmp_path / "_wvta_reference.json").read_bytes() == old
    assert service.read_wvta_reference("CH", [])["records"] == []


def test_missing_and_invalid_reference_are_distinct(tmp_path, monkeypatch):
    monkeypatch.setattr(service, "COC_MATCH_JOB_ROOT", tmp_path)
    assert service.read_wvta_reference("CH", [])["status"] == "missing"
    (tmp_path / "_wvta_reference.json").write_text("invalid")
    with pytest.raises(HTTPException) as error: service.read_wvta_reference("CH", [])
    assert error.value.status_code == 503


def test_country_scope_uses_explicit_coc_markers_and_nordic_region():
    assert not service.certificate_matches_country("CH", "", "COC-波兰专用")
    assert service.certificate_matches_country("PL", "", "COC-波兰专用")
    assert service.certificate_matches_country("SE", "北欧物料号")
    assert service.certificate_matches_country("CH", "北欧物料号")
    assert service.certificate_matches_country("SE", "北欧专用COC")
    assert not service.certificate_matches_country("CH", "北欧专用COC")
    assert not service.certificate_matches_country("CH", "", "COC-NORDIC ONLY")
    assert not service.certificate_matches_country("SE", "葡萄牙专用")


def test_resolve_fill_decision_uses_date_and_country_to_avoid_special_coc() -> None:
    material = "T7000Z5**MY0013"
    records = [
        _record(
            material,
            "e4*2018/858*00273*02",
            "00273-02&402&104V&COC002-宁德",
            valid_from=date(2026, 3, 11),
            valid_to=date(2026, 4, 14),
        ),
        _record(
            material,
            "e4*2018/858*00273*03",
            "00273-03&428&104V&COC003-波兰专用",
            valid_from=date(2026, 4, 15),
            valid_to=None,
            comments="波兰专用COC",
        ),
        _record(
            material,
            "e4*2018/858*00273*03",
            "00273-03&402&104V&COC002-宁德",
            valid_from=date(2026, 4, 15),
            valid_to=None,
            comments="宁德电池",
        ),
    ]

    decision = resolve_fill_decision(
        _row(material),
        {material: records},
        overwrite_existing=False,
        conflict_strategy="date_country",
    )

    assert decision.status == "filled"
    assert decision.written_wvta == "e4*2018/858*00273*03"
    assert decision.written_coc == "00273-03&402&104V&COC002-宁德"


def test_resolve_fill_decision_skips_existing_cells_by_default() -> None:
    material = "T71506J**MH0001"
    decision = resolve_fill_decision(
        _row(material, existing_wvta="old", existing_coc="old-coc"),
        {
            material: [
                _record(
                    material,
                    "e9*2018/858*11785*02",
                    "11785-02&120&96H&COC001-EB",
                    valid_from=date(2025, 12, 15),
                    valid_to=None,
                )
            ]
        },
        overwrite_existing=False,
    )

    assert decision.status == "skipped_existing"
    assert decision.written_wvta is None
    assert decision.written_coc is None


def test_resolve_fill_decision_requires_exact_material_group() -> None:
    decision = resolve_fill_decision(
        _row("T716015B**MH0001"),
        {
            "T716015**MH0001": [
                _record(
                    "T716015**MH0001",
                    "e9*2018/858*11846*01",
                    "11846-01&125&103V&COC001-EB",
                    valid_from=date(2026, 2, 3),
                    valid_to=None,
                )
            ]
        },
        overwrite_existing=False,
    )

    assert decision.status == "not_found"


def test_date_country_does_not_fall_back_when_production_date_is_outside_all_periods() -> None:
    material = "T7000Z5**MY0013"
    record = _record(
        material,
        "e4*2018/858*00273*02",
        "00273-02&402&104V&COC002-宁德",
        valid_from=date(2026, 3, 11),
        valid_to=date(2026, 4, 14),
    )

    decision = resolve_fill_decision(
        _row(material, production_start=date(2026, 4, 15), production_end=date(2026, 4, 15)),
        {material: [record]},
        overwrite_existing=False,
        conflict_strategy="date_country",
    )

    assert decision.status == "not_found"
    assert decision.written_wvta is None
    assert "未回退旧候选" in decision.reason


def test_date_country_uses_inclusive_validity_boundaries() -> None:
    material = "T7000Z5**MY0013"
    old = _record(
        material,
        "e4*2018/858*00273*02",
        "00273-02&402&104V&COC002-宁德",
        valid_from=date(2026, 3, 1),
        valid_to=date(2026, 4, 28),
    )
    new = _record(
        material,
        "e4*2018/858*00273*03",
        "00273-03&402&104V&COC002-宁德",
        valid_from=date(2026, 4, 29),
        valid_to=None,
    )

    old_decision = resolve_fill_decision(
        _row(material, production_start=date(2026, 4, 28), production_end=date(2026, 4, 28)),
        {material: [old, new]},
        overwrite_existing=False,
        conflict_strategy="date_country",
    )
    new_decision = resolve_fill_decision(
        _row(material, production_start=date(2026, 4, 29), production_end=date(2026, 4, 29)),
        {material: [old, new]},
        overwrite_existing=False,
        conflict_strategy="date_country",
    )

    assert old_decision.written_wvta.endswith("*02")
    assert new_decision.written_wvta.endswith("*03")


def test_date_country_prefers_matching_country_special_over_generic_record() -> None:
    material = "T7000Z5**MY0013"
    generic = _record(
        material,
        "e4*2018/858*00273*03",
        "00273-03&402&104V&COC002-宁德",
        valid_from=date(2026, 4, 15),
        valid_to=None,
    )
    poland = _record(
        material,
        "e4*2018/858*00273*03",
        "00273-03&428&104V&COC003-波兰专用",
        valid_from=date(2026, 4, 15),
        valid_to=None,
        comments="波兰专用COC",
    )

    decision = resolve_fill_decision(
        _row(material, country="PL"),
        {material: [generic, poland]},
        overwrite_existing=False,
        conflict_strategy="date_country",
    )

    assert decision.status == "filled"
    assert decision.written_coc == poland.coc_no


def test_date_country_without_production_date_keeps_multiple_periods_ambiguous() -> None:
    material = "T7000Z5**MY0013"
    records = [
        _record(material, "e4*2018/858*00273*02", "old", valid_from=date(2026, 3, 1), valid_to=date(2026, 4, 28)),
        _record(material, "e4*2018/858*00273*03", "new", valid_from=date(2026, 4, 29), valid_to=None),
    ]

    decision = resolve_fill_decision(
        _row(material, production_start=None, production_end=None),
        {material: records},
        overwrite_existing=False,
        conflict_strategy="date_country",
    )

    assert decision.status == "ambiguous"
    assert decision.written_wvta is None
