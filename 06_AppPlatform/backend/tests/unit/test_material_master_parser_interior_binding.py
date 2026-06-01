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
    assert rows_by_code["T9000WTEX002"]["interior_color_name"] is None
