from __future__ import annotations

from app.services.country_material_finance_import_service import (
    parse_country_material_finance_text,
)


def test_parse_country_material_finance_text_with_cbu_headers() -> None:
    text = "\n".join(
        [
            "Material Code\tFOB\tUnit Margin\tMargin %\tUnit Profit\tProfit %\tFOB Delta\tMargin Delta\tNote",
            "T7000Z5**MY0026\t14,930\t2100\t16.82%\t1800\t14.41%\t200\t-300\tprice change",
        ]
    )

    result = parse_country_material_finance_text(text, "NL")

    assert result["warnings"] == []
    assert len(result["rows"]) == 1
    row = result["rows"][0]
    assert row["error"] == ""
    assert row["materialCode"] == "T7000Z5**MY0026"
    assert row["update"]["fobEur"] == 14930
    assert row["update"]["vehicleMarginEur"] == 2100
    assert row["update"]["vehicleMarginRate"] == 0.1682
    assert row["update"]["vehicleProfitEur"] == 1800
    assert row["update"]["vehicleProfitRate"] == 0.1441
    assert row["update"]["fobDeltaEur"] == 200
    assert row["update"]["marginDeltaEur"] == -300
    assert row["update"]["memo"] == "price change"


def test_parse_country_material_finance_text_requires_template_material_code() -> None:
    result = parse_country_material_finance_text("Material Code\tFOB\nT7000Z5BWMY0026\t14930", "NL")

    assert result["rows"][0]["update"]["fobEur"] == 14930
    assert result["rows"][0]["error"] == "T7000Z5BWMY0026 is not a BOM template"


def test_parse_country_material_finance_text_with_chinese_headers() -> None:
    text = "\n".join(
        [
            "物料号\tFOB\t单车边际\t边际率\t单车利润\t利润率\tFOB增减\t边际增减\t备注",
            "T71513X**MH0002\t19350\t4100\t21.19%\t3600\t18.6%\t0\t363\timage source",
        ]
    )

    result = parse_country_material_finance_text(text, "NL")

    row = result["rows"][0]
    assert row["error"] == ""
    assert row["materialCode"] == "T71513X**MH0002"
    assert row["update"]["vehicleMarginEur"] == 4100
    assert row["update"]["vehicleMarginRate"] == 0.2119
    assert row["update"]["vehicleProfitEur"] == 3600
    assert row["update"]["vehicleProfitRate"] == 0.186
    assert row["update"]["fobDeltaEur"] == 0
    assert row["update"]["marginDeltaEur"] == 363
    assert row["update"]["memo"] == "image source"
