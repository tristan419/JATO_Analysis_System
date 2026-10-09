"""Synthetic fixtures only: actual customer templates stay outside this public repo."""
import json
from copy import deepcopy
from io import BytesIO
from unittest.mock import Mock

import openpyxl
import pytest
from fastapi import HTTPException

from app.api.routes import order_genius_vehicle_allocation as route
from app.services import order_genius_vehicle_exporter as exporter


@pytest.fixture
def templates(tmp_path, monkeypatch):
    profiles = {}
    for country in ("CH", "SE"):
        profiles[country] = dict(name=f"{country} OJ", buyerName="Example Buyer",
            buyerAddress="Example address in Norway", buyerEmail="", portOfShipment="Loading",
            portOfDischarge="Discharge")
        wb = openpyxl.Workbook()
        ws = wb.active
        for coordinate, value in {"A1": "Example Seller", "A3": "Seller address", "C24": "Example bank",
                                  "C21": "CIF" if country == "CH" else "CFR", "C22": "TT" if country == "CH" else "LC 90"}.items():
            ws[coordinate] = value
        ws["B28"] = "Old sample vehicle"
        ws.merge_cells("B28:B30")
        ws["A35"] = "Old sample footer"
        ws.auto_filter.ref = "A27:XFD48"
        wb.save(tmp_path / f"{country}_OJ.xlsx")
    (tmp_path / "profiles.json").write_text(json.dumps(profiles))
    monkeypatch.setattr(exporter, "PI_TEMPLATE_ROOT", tmp_path)
    return tmp_path


def pi(country="SE", quantity=2):
    return {"header": {"piCode": f"PI-{country}-202609-001", "countryCode": country,
        "orderingAccountCode": country, "orderDate": "2026-09-01"},
        "lines": [{"piLineCode": "L01", "brand": "OMODA", "quantity": quantity}],
        "vehicles": [{"piLineCode": "L01", "countryCode": country, "brand": "OMODA",
            "modelName": "MODEL BEV", "version": "Trim", "powertrain": "BEV",
            "exteriorColorName": "White", "exteriorColorCode": "BW", "interiorColorName": "Black",
            "materialCode": "SAVED-MATERIAL", "fobEur": 10000.11, "freightEur": 123.45,
            "insuranceEur": 6.78} for _ in range(quantity)]}


@pytest.mark.parametrize("country,qty_col,fob_col,total_col", [("CH", "F", "H", "L"), ("SE", "H", "J", "M")])
def test_invoice_saved_snapshots_formulas_cached_totals_and_print(templates, country, qty_col, fob_col, total_col):
    original = (templates / f"{country}_OJ.xlsx").read_bytes()
    detail = pi(country, quantity=45)
    before = deepcopy(detail)
    output = exporter.generate_pi_invoice_excel(detail, {"handlingEur": 10 if country == "CH" else 0})
    formula = openpyxl.load_workbook(output).active
    values = openpyxl.load_workbook(BytesIO(output.getvalue()), data_only=True).active
    assert formula.title == "Proforma Invoice"
    assert formula["A7"].value == "PROFORMA INVOICE"
    assert "Example Buyer" in formula["B9"].value
    assert values[f"{qty_col}28"].value == 45
    assert values[f"{qty_col}29"].value == 45
    assert values[f"{fob_col}29"].value == 450004.95
    assert values[f"{total_col}29"].value == (456315.3 if country == "CH" else 455865.3)
    assert formula[f"{total_col}28"].value.startswith("=ROUND(")
    assert formula[f"{total_col}29"].value == f"=SUM({total_col}28:{total_col}28)"
    assert formula["A30"].value == "10-Port of Shipment: Loading"
    assert not any(cell.value == "Old sample footer" for row in formula for cell in row)
    assert formula.auto_filter.ref is None
    assert formula.page_setup.fitToWidth == 1
    assert formula.print_title_rows == "$26:$27"
    assert (templates / f"{country}_OJ.xlsx").read_bytes() == original
    assert detail == before


def test_different_saved_costs_split_rows_and_zero_override_is_explicit(templates):
    detail = pi()
    detail["vehicles"][1]["freightEur"] = 200
    split = openpyxl.load_workbook(exporter.generate_pi_invoice_excel(detail, {}), data_only=True).active
    assert (split["H28"].value, split["H29"].value, split["H30"].value) == (1, 1, 2)
    merged = openpyxl.load_workbook(exporter.generate_pi_invoice_excel(detail, {"freightEur": 0, "insuranceEur": 0}), data_only=True).active
    assert merged["H28"].value == 2
    assert merged["M29"].value == 20000.22


@pytest.mark.parametrize("key,value", [("freightEur", None), ("insuranceEur", None), ("fobEur", 0),
                                      ("fobEur", None), ("freightEur", -1), ("insuranceEur", float("nan"))])
def test_invalid_or_missing_costs_block_instead_of_guessing(templates, key, value):
    detail = pi()
    detail["vehicles"][0][key] = value
    with pytest.raises(ValueError):
        exporter.generate_pi_invoice_excel(detail, {})


def test_missing_costs_can_be_filled_for_file_without_order_mutation(templates):
    detail = pi()
    for vehicle in detail["vehicles"]:
        vehicle["freightEur"] = vehicle["insuranceEur"] = None
    context = exporter.pi_invoice_context(detail)
    assert context["missingFreightUnits"] == 2
    result = exporter.generate_pi_invoice_excel(detail, {"freightEur": 0, "insuranceEur": 0})
    assert openpyxl.load_workbook(result, data_only=True).active["M29"].value == 20000.22
    assert detail["vehicles"][0]["freightEur"] is None


def test_account_country_not_buyer_country_selects_template(templates):
    assert exporter.pi_invoice_context(pi())["templateKey"] == "SE_OJ"
    assert "Norway" in exporter.pi_invoice_context(pi())["buyerAddress"]
    detail = pi("CH")
    detail["header"]["orderingAccountCode"] = "SE"
    assert exporter.pi_invoice_context(detail)["templateKey"] == "SE_OJ"


@pytest.mark.parametrize("brand", ["CHERY", "", None])
def test_unknown_or_other_brand_not_silently_using_oj_template(templates, brand):
    detail = pi()
    detail["lines"][0]["brand"] = brand
    with pytest.raises(ValueError, match="No matching"):
        exporter.pi_invoice_context(detail)


def test_dynamic_rows_and_text_are_safe(templates):
    detail = pi(quantity=20)
    for index, vehicle in enumerate(detail["vehicles"]):
        vehicle["materialCode"] = f"MATERIAL-{index}"
    detail["vehicles"][0]["modelName"] = "=BADFORMULA()"
    ws = openpyxl.load_workbook(exporter.generate_pi_invoice_excel(detail, {"referenceNo": "=BAD()"})).active
    assert ws["B28"].data_type == "s"
    assert ws["K9"].data_type == "s"
    assert ws["M48"].value == "=SUM(M28:M47)"
    assert ws["A49"].value == "10-Port of Shipment: Loading"


def test_incomplete_saved_pi_is_not_exported_as_complete(templates):
    detail = pi()
    detail["vehicles"].pop()
    with pytest.raises(ValueError, match="incomplete"):
        exporter.generate_pi_invoice_excel(detail, {})


def test_rounding_matches_excel_and_null_ports_do_not_become_literal_none(templates):
    assert str(exporter._invoice_money("123.445", "Freight")) == "123.45"
    with pytest.raises(ValueError, match="reference number"):
        exporter.generate_pi_invoice_excel(pi(), {"portOfDischarge": None})


def test_export_checks_whole_pi_authorization_before_template_access(monkeypatch):
    monkeypatch.setattr(route, "get_pi_detail", Mock(return_value=pi()))
    denied = Mock(side_effect=HTTPException(403, "Outside authorized country / brand"))
    monkeypatch.setattr(route, "_validate_whole_pi_access", denied)
    generate = Mock()
    monkeypatch.setattr(route, "generate_pi_invoice_excel", generate)
    with pytest.raises(HTTPException) as error:
        route.export_pi_invoice("PI-SE-202609-001", {}, Mock(), Mock())
    assert error.value.status_code == 403
    generate.assert_not_called()


def test_unconfigured_template_returns_actionable_error(tmp_path, monkeypatch):
    monkeypatch.setattr(exporter, "PI_TEMPLATE_ROOT", tmp_path)
    with pytest.raises(ValueError, match="contact|管理员"):
        exporter.pi_invoice_context(pi())
