from __future__ import annotations

from datetime import date

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
