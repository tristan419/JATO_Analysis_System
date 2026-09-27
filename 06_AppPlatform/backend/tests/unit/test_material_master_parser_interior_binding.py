from pathlib import Path

import openpyxl

from app.services.material_master_parser import parse_material_master_xlsx


def test_interior_is_bound_to_placeholder_bom_template(tmp_path: Path) -> None:
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "OMODA9 ICE"
    worksheet.append([
        "No.",
        "Code Name",
        "Full Name",
        "Configuration",
        "BOM",
        "Exterior Color",
        "Interior Color",
        "Sweden FOB",
    ])
    worksheet.append([
        1,
        "O9",
        "OMODA9",
        "Exclusive",
        "T9000**EX001",
        None,
        "Black/Black",
        None,
    ])
    worksheet.append([None, None, None, None, None, "Black (BK)", None, 30000])
    worksheet.append([None, None, None, None, "T9000**EX002", None, None, None])
    worksheet.append([None, None, None, None, None, "White (WT)", None, 30000])

    file_path = tmp_path / "material_master.xlsx"
    workbook.save(file_path)

    parsed = parse_material_master_xlsx(file_path)
    rows_by_code = {row["material_code"]: row for row in parsed["rows"]}

    assert rows_by_code["T9000BKEX001"]["interior_color_name"] == "Black/Black"
    assert rows_by_code["T9000BKEX001"]["interior_package"] == "Black/Black"
    assert rows_by_code["T9000BKEX001"]["interior_colour_code"] == "BB"
    assert rows_by_code["T9000BKEX001"]["colour_tier"] is None
    assert rows_by_code["T9000WTEX002"]["interior_color_name"] is None


def test_legacy_workbook_colour_names_do_not_assign_pricing_tier(tmp_path: Path) -> None:
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "OMODA9 SHS"
    worksheet.append([
        "No.",
        "Code Name",
        "Full Name",
        "Configuration",
        "BOM",
        "Exterior Color",
        "Interior Color",
        "Sweden FOB",
    ])
    worksheet.append([
        1,
        "O9",
        "OMODA9 SHS",
        "Premium",
        "T9000**EX001",
        "Black & White (ZE)",
        "Black/Black",
        30000,
    ])
    worksheet.append([None, None, None, None, None, "Matte Gray (UE)", None, 30300])

    file_path = tmp_path / "legacy_material_master.xlsx"
    workbook.save(file_path)

    parsed = parse_material_master_xlsx(file_path)

    assert [row["colour_tier"] for row in parsed["rows"]] == [None, None]
    assert [row["exterior_color_type"] for row in parsed["rows"]] == ["dual", "single"]
