from __future__ import annotations

import openpyxl

from app.services.bom_admin_export_service import (
    BOM_ADMIN_EXPORT_SCHEMA_VERSION,
    build_bom_admin_workbook,
)


def _item(
    material_code: str,
    *,
    model: str = "JAECOO8 SHS",
    colour: str,
    code: str,
    tier: str,
    base: float,
    surcharge: float,
) -> dict:
    return {
        "materialCode": material_code,
        "brand": "JAECOO" if model.startswith("JAECOO") else "OMODA",
        "modelCode": "T26SHS",
        "modelName": model,
        "version": "Luxury-AWD(5座）",
        "bomTemplate": "T6481QN**LX0002",
        "colour": colour,
        "colourCode": code,
        "colourHex": "#F0ECE0",
        "colourTier": tier,
        "interiorColorName": "Black-Black",
        "rowVersion": 4,
        "fobByCountry": {
            "AT": {
                "baseFobEur": base,
                "colourSurchargeEur": surcharge,
                "finalFobEur": base + surcharge,
            },
            "CZ": {
                "baseFobEur": base - 150,
                "colourSurchargeEur": surcharge,
                "finalFobEur": base - 150 + surcharge,
            },
        },
    }


def test_bom_admin_export_matches_material_master_shape_and_preserves_tier() -> None:
    workbook_bytes = build_bom_admin_workbook(
        [
            _item("T6481QNBWLX0002", colour="Khaki white", code="BW", tier="single", base=28000, surcharge=0),
            _item("T6481QNZELX0002", colour="Black & White", code="ZE", tier="dual", base=28000, surcharge=300),
            _item("T6481QNUELX0002", colour="Matte Gray", code="UE", tier="special", base=28000, surcharge=300),
        ],
        ["AT", "CZ"],
    )

    wb = openpyxl.load_workbook(workbook_bytes, data_only=False)
    assert wb.sheetnames == [
        "JAECOO8 SHS",
        "_BOM_ADMIN_DATA",
        "_BOM_ADMIN_SCHEMA",
    ]
    ws = wb["JAECOO8 SHS"]
    assert ws["A1"].value == "No."
    assert ws["E1"].value == "BOM"
    assert ws["F1"].value == "Material Code"
    assert ws["J1"].value == "Colour Tier"
    assert ws["K1"].value == "Austria FOB"
    assert ws["L1"].value == "CZ FOB"
    assert ws["A1"].font.name == "Calibri"
    assert ws["A1"].font.sz == 14
    assert ws["A1"].font.bold is True
    assert "B3:B5" in {str(cell_range) for cell_range in ws.merged_cells.ranges}
    assert "C3:C5" in {str(cell_range) for cell_range in ws.merged_cells.ranges}

    exported = {
        ws.cell(row, 6).value: {
            "tier": ws.cell(row, 10).value,
            "atFinal": ws.cell(row, 11).value,
            "czFinal": ws.cell(row, 12).value,
            "fill": ws.cell(row, 7).fill.fgColor.rgb,
        }
        for row in range(3, 6)
    }
    assert exported["T6481QNBWLX0002"]["tier"] == "Single"
    assert exported["T6481QNZELX0002"]["tier"] == "Dual"
    assert exported["T6481QNUELX0002"]["tier"] == "Special"
    assert exported["T6481QNZELX0002"]["atFinal"] == 28300
    assert exported["T6481QNZELX0002"]["czFinal"] == 28150
    assert exported["T6481QNZELX0002"]["fill"] == exported["T6481QNUELX0002"]["fill"]
    assert exported["T6481QNBWLX0002"]["fill"] != exported["T6481QNZELX0002"]["fill"]

    schema = wb["_BOM_ADMIN_SCHEMA"]
    assert schema.sheet_state == "hidden"
    assert schema["B1"].value == BOM_ADMIN_EXPORT_SCHEMA_VERSION

    data = wb["_BOM_ADMIN_DATA"]
    assert data.sheet_state == "hidden"
    assert data["A1"].value == "Schema Version"
    assert data["O1"].value == "Single Base"
    assert data["P1"].value == "Surcharge"
    assert data["Q1"].value == "Final FOB"
    dual_at = next(
        row
        for row in data.iter_rows(min_row=2, values_only=True)
        if row[6] == "T6481QNZELX0002" and row[13] == "AT"
    )
    assert dual_at[11] == "dual"
    assert dual_at[14:17] == (28000, 300, 28300)
    assert dual_at[17] == "ready"
    assert len(dual_at[19]) == 64


def test_bom_admin_export_marks_price_conflict_for_review() -> None:
    row = _item(
        "T6481QNBWLX0002",
        colour="Khaki white",
        code="BW",
        tier="single",
        base=28000,
        surcharge=0,
    )
    row["fobByCountry"]["AT"] = {
        "status": "conflict",
        "records": [
            {"paymentTermCode": "LC", "finalFobEur": 28000},
            {"paymentTermCode": "TT", "finalFobEur": 28300},
        ],
    }

    workbook_bytes = build_bom_admin_workbook([row], ["AT"])
    wb = openpyxl.load_workbook(workbook_bytes, data_only=False)

    assert wb["JAECOO8 SHS"]["K3"].value == "REVIEW"
    data = wb["_BOM_ADMIN_DATA"]
    assert data["R2"].value == "conflict"
    assert '"paymentTermCode": "LC"' in data["S2"].value


def test_bom_admin_export_can_limit_prices_to_one_country() -> None:
    workbook_bytes = build_bom_admin_workbook(
        [_item("T6481QNZELX0002", colour="Black & White", code="ZE", tier="dual", base=28000, surcharge=300)],
        ["AT", "CZ"],
        country_code="CZ",
    )

    wb = openpyxl.load_workbook(workbook_bytes, data_only=False)
    ws = wb["JAECOO8 SHS"]
    assert ws.max_column == 11
    assert ws["K1"].value == "CZ FOB"
    assert ws["K3"].value == 28150
    data = wb["_BOM_ADMIN_DATA"]
    assert data.max_row == 2
    assert data["N2"].value == "CZ"
    assert data["O2"].value == 27850
    assert data["P2"].value == 300
    assert data["Q2"].value == 28150


def test_bom_admin_export_never_infers_missing_tier_from_name_or_swatch() -> None:
    missing_tier = _item(
        "T6481QNZZLX0002",
        colour="Matte Black & White",
        code="ZZ",
        tier="",
        base=28000,
        surcharge=300,
    )
    missing_tier["colourHex"] = "#111111|#F0ECE0"

    workbook_bytes = build_bom_admin_workbook([missing_tier], ["AT"])
    wb = openpyxl.load_workbook(workbook_bytes, data_only=False)
    ws = wb["JAECOO8 SHS"]

    assert ws["J3"].value == "Missing tier"
    assert ws["K3"].value == 28300
    data = wb["_BOM_ADMIN_DATA"]
    assert data["L2"].value is None
    assert data["O2"].value == 28000
    assert data["P2"].value == 300
    assert data["Q2"].value == 28300
    assert data["R2"].value == "missing_tier"


def test_bom_admin_export_keeps_same_name_different_codes_separate() -> None:
    workbook_bytes = build_bom_admin_workbook(
        [
            _item("OMODA-BW", model="OMODA7 SHS", colour="Khaki white", code="BW", tier="single", base=20000, surcharge=0),
            _item("OMODA-BX", model="OMODA9 SHS", colour="Khaki white", code="BX", tier="single", base=25000, surcharge=0),
        ],
        ["AT"],
    )
    wb = openpyxl.load_workbook(workbook_bytes, data_only=False)

    assert wb["OMODA7 SHS"]["H3"].value == "BW"
    assert wb["OMODA9 SHS"]["H3"].value == "BX"


def test_bom_admin_export_rejects_unknown_country_scope() -> None:
    try:
        build_bom_admin_workbook(
            [_item("T6481QNZELX0002", colour="Black & White", code="ZE", tier="dual", base=28000, surcharge=300)],
            ["AT", "CZ"],
            country_code="SE",
        )
    except ValueError as exc:
        assert str(exc) == "Country SE has no active BOM FOB data"
    else:
        raise AssertionError("Expected an unavailable country to be rejected")
