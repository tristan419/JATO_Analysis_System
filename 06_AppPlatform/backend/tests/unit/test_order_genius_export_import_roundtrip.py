from pathlib import Path

from app.services.order_genius_export_service import generate_order_genius_excel
from app.services.order_quantity_parser import parse_order_quantity_xlsx


def test_order_genius_export_can_be_imported_back_with_current_columns(
    tmp_path: Path,
) -> None:
    months = {str(month): {"quantity": 0} for month in range(1, 13)}
    months["1"]["quantity"] = 4
    months["2"]["quantity"] = 3

    export_buffer = generate_order_genius_excel(
        rows=[
            {
                "brand": "OMODA",
                "modelName": "OMODA 5",
                "version": "Comfort",
                "colour": "White",
                "interiorColorName": "Black",
                "materialCode": "T19C-AB",
                "fobEur": 12345.0,
                "powertrain": "ICE",
                "lifecycleStatus": "active",
                "months": months,
                "ttl": 7,
            }
        ],
        country_code="SE",
        country_name="Sweden",
        year=2026,
    )

    export_path = tmp_path / "order-genius-export.xlsx"
    export_path.write_bytes(export_buffer.getvalue())

    parsed = parse_order_quantity_xlsx(export_path)

    assert parsed.errors == []
    assert parsed.country_code == "SE"
    assert parsed.year == 2026
    assert [row.material_code for row in parsed.rows] == ["T19C-AB"]

    row = parsed.rows[0]
    assert row.model_name == "OMODA 5"
    assert row.version == "Comfort"
    assert row.colour == "White"
    assert [cell.quantity for cell in row.cells[:2]] == [4, 3]
    assert sum(cell.quantity for cell in row.cells) == 7
