"""Common-layout examples only; no customer or bank data in fixtures."""
import hashlib
from copy import deepcopy
from io import BytesIO
from unittest.mock import Mock

import openpyxl
import pytest
from fastapi import HTTPException

from app.core.security import UserContext
from app.api.routes import order_genius_vehicle_allocation as route
from app.services import pi_invoice_templates as templates
from app.services import order_genius_vehicle_exporter as exporter


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setattr(exporter, "PI_TEMPLATE_ROOT", tmp_path)
    wb = openpyxl.Workbook(); ws = wb.active
    for cell, value in {"A1": "Seller", "A2": "Seller address", "A3": "Buyer", "A4": "Buyer address",
                        "A5": "Bank", "A6": "CFR", "A7": "LC", "F1": "Date", "F2": "Reference"}.items():
        ws[cell] = value
    for col, label in enumerate(["No", "Model", "Version", "QTY.", "FOB Per Unit (EUR)", "Freight (EUR)", "Insurance (EUR)", "Total FOB", "TOTAL CFR (EUR)"], 1):
        ws.cell(10, col, label)
    ws["B11"] = "Example order"; ws["B12"] = "Old sample"; ws["A13"] = "Total"
    ws.merge_cells("A13:C13"); ws["A14"] = "Port of Shipment: Old"; ws["A15"] = "Port of Discharge: Old"
    ws.merge_cells("A14:I14"); ws.merge_cells("A15:I15")
    ws.column_dimensions["B"].width = 28
    ws.row_dimensions[11].height = 34
    output = BytesIO(); wb.save(output)
    content = output.getvalue()
    fields = {key: {"cell": cell, "value": value, "prefix": ""} for key, cell, value in [
        ("sellerName", "A1", "Seller"), ("sellerAddress", "A2", "Address"), ("buyerName", "A3", "Buyer"),
        ("buyerAddress", "A4", "Buyer address"), ("bankDetails", "A5", "Bank"), ("priceTerm", "A6", "CFR"),
        ("paymentTerm", "A7", "LC"), ("invoiceDate", "F1", ""), ("referenceNo", "F2", ""),
        ("portOfShipment", "A14", "Loading"), ("portOfDischarge", "A15", "Discharge")]}
    fields["portOfShipment"]["prefix"] = "Port of Shipment: "
    fields["portOfDischarge"]["prefix"] = "Port of Discharge: "
    analysis = templates.analyse(content)
    config = {key: analysis[key] for key in ("sheetName", "headerRow", "dataStart", "totalRow", "columns", "charges", "currency")}
    config.update(fields=fields, charges=["freightEur", "insuranceEur"])
    return content, dict(name="Country invoice", country="CZ", brands=["OMODA", "JAECOO"], shared=False,
                         sha256=hashlib.sha256(content).hexdigest(), config=config)


def detail():
    return {"header": {"piCode": "PI-CZ-202609-001", "countryCode": "CZ", "orderDate": "2026-09-01"},
            "lines": [{"piLineCode": "L01", "brand": "OMODA", "quantity": 2}],
            "vehicles": [{"piLineCode": "L01", "modelName": "MODEL", "version": "Trim", "powertrain": "BEV",
                          "materialCode": f"SAVED-{i}", "exteriorColorName": "White", "fobEur": 100.11,
                          "freightEur": 3.45, "insuranceEur": 0.67} for i in range(2)]}


def test_analysis_recognizes_table_and_never_saves(source):
    content, payload = source
    parsed = templates.analyse(content)
    assert (parsed["headerRow"], parsed["dataStart"], parsed["totalRow"]) == (10, 11, 13)
    assert parsed["columns"]["fobEur"] == "E"
    assert parsed["fields"]["portOfDischarge"]["cell"] == "A15"
    assert not templates.records()


def test_personal_shared_admin_scope_and_independent_order(source):
    content, payload = source
    first = templates.save(content, payload, "alice", False)
    second = templates.save(content, {**payload, "name": "Shared", "shared": True}, "bob", False)
    assert templates.catalog("carol", False, "CZ", {"OMODA"})["items"] == [second]
    with pytest.raises(PermissionError): templates.load_template(first["id"], "bob", False, "CZ", {"OMODA"})
    assert templates.load_template(first["id"], "admin", True, "CZ", {"OMODA"})["owner"] == "alice"
    assert not templates.catalog("alice", False, "SE", {"OMODA"})["items"]
    assert not templates.catalog("alice", False, "CZ", {"CHERY"})["items"]
    templates.reorder("alice", False, "CZ", {"OMODA"}, [second["id"], first["id"]])
    assert templates.catalog("alice", False, "CZ", {"OMODA"})["items"][0]["id"] == second["id"]
    assert templates.catalog("bob", False, "CZ", {"OMODA"})["items"] == [second]
    with pytest.raises(PermissionError): templates.save(content, {**payload, "id": second["id"]}, "alice", False)
    with pytest.raises(PermissionError): templates.reorder("alice", False, "CZ", {"OMODA"}, [], "bob")
    templates.reorder("admin", True, "CZ", {"OMODA"}, [second["id"], first["id"]], "alice")
    with pytest.raises(ValueError): templates.reorder("alice", False, "CZ", {"OMODA"}, [first["id"]])


def test_custom_invoice_uses_saved_snapshots_cached_formulas_and_footer_mapping(source):
    content, payload = source
    record = templates.save(content, payload, "alice", False)
    template = templates.load_template(record["id"], "alice", False, "CZ", {"OMODA"})
    pi = detail(); before = deepcopy(pi)
    output = exporter.generate_pi_invoice_excel(pi, {}, template)
    ws = openpyxl.load_workbook(output).active
    cached = openpyxl.load_workbook(BytesIO(output.getvalue()), data_only=True).active
    assert ws["B11"].value == "MODEL"
    assert str(next(merge for merge in ws.merged_cells.ranges if merge.min_col == 2)) == "B11:B12"
    assert ws["I11"].value == "=ROUND((E11+F11+G11)*D11,2)"
    assert cached["I13"].value == 208.46
    assert cached["H13"].value == 200.22
    assert cached["D13"].value == 2
    assert ws["D13"].number_format == "#,##0"
    assert ws["I13"].number_format == "#,##0.00"
    assert ws["A14"].value == "Port of Shipment: Loading"
    assert ws["F2"].value == "PI-CZ-202609-001"
    assert ws.column_dimensions["B"].width == 28
    assert pi == before and template["path"].read_bytes() == content


def test_custom_total_format_is_preserved(source):
    content, payload = source
    wb = openpyxl.load_workbook(BytesIO(content)); wb.active["I13"].number_format = '"EUR "#,##0.00'
    output = BytesIO(); wb.save(output); content = output.getvalue()
    payload["sha256"] = hashlib.sha256(content).hexdigest()
    record = templates.save(content, payload, "alice", False)
    template = templates.load_template(record["id"], "alice", False, "CZ", {"OMODA"})
    ws = openpyxl.load_workbook(exporter.generate_pi_invoice_excel(detail(), {}, template)).active
    assert ws["I13"].number_format == '"EUR "#,##0.00'


def test_table_shrinks_footer_moves_and_zero_overrides_do_not_mutate_pi(source):
    content, payload = source
    record = templates.save(content, payload, "alice", False)
    template = templates.load_template(record["id"], "alice", False, "CZ", {"OMODA"})
    pi = detail(); pi["vehicles"][1]["materialCode"] = "SAVED-0"
    pi["vehicles"][0]["freightEur"] = pi["vehicles"][1]["freightEur"] = None
    with pytest.raises(ValueError): exporter.generate_pi_invoice_excel(pi, {}, template)
    out = exporter.generate_pi_invoice_excel(pi, {"freightEur": 0, "insuranceEur": 0, "referenceNo": "=BAD()"}, template)
    ws = openpyxl.load_workbook(out).active
    cached = openpyxl.load_workbook(BytesIO(out.getvalue()), data_only=True).active
    assert ws["A13"].value == "Port of Shipment: Loading"
    assert ws["A14"].value == "Port of Discharge: Discharge"
    assert cached["I12"].value == 200.22
    assert ws["F2"].data_type == "s"
    assert pi["vehicles"][0]["freightEur"] is None


@pytest.mark.parametrize("change", ["missing", "overlap", "charge", "bounds", "currency"])
def test_invalid_mapping_blocks_save(source, change):
    content, payload = source
    config = payload["config"]
    if change == "missing": config["fields"]["buyerName"]["value"] = ""
    if change == "overlap": config["columns"]["total"] = "D"
    if change == "charge": config["charges"] = ["unknown"]
    if change == "bounds": config["fields"]["bankDetails"]["cell"] = "A11"
    if change == "currency": config["currency"] = "CHF"
    with pytest.raises(ValueError): templates.save(content, payload, "alice", False)
    assert not templates.records()


def test_changed_source_and_damaged_top_template_never_fall_back(source, monkeypatch):
    content, payload = source
    first = templates.save(content, payload, "alice", False)
    second = templates.save(content, {**payload, "name": "Second"}, "alice", False)
    templates.reorder("alice", False, "CZ", {"OMODA"}, [first["id"], second["id"]])
    templates._path(first["id"]).joinpath("source.xlsx").write_bytes(b"damaged")
    monkeypatch.setattr(route, "_template_scope", Mock(return_value=(detail(), "CZ", {"OMODA"})))
    with pytest.raises(ValueError, match="damaged"):
        route._selected_template(Mock(), UserContext("order_filler", "alice"), "PI")


def test_scope_is_rechecked_before_any_template_write(source, monkeypatch):
    content, payload = source
    monkeypatch.setattr(route, "_template_scope", Mock(side_effect=HTTPException(403, "Revoked")))
    with pytest.raises(HTTPException): route._save_template_for_pi(Mock(), UserContext("order_filler", "alice"), "PI", content, payload)
    assert not templates.records()


def test_no_macros_external_links_multiple_worksheets_or_bad_xlsx(source):
    content, _ = source
    with pytest.raises(ValueError): templates.analyse(b"not xlsx")
    wb = openpyxl.load_workbook(BytesIO(content)); wb.create_sheet("Other")
    out = BytesIO(); wb.save(out)
    with pytest.raises(ValueError, match="one invoice"): templates.analyse(out.getvalue())


def test_delete_only_owned_source_and_existing_copies_survive(source):
    content, payload = source
    original = templates.save(content, {**payload, "shared": True}, "alice", False)
    copy = templates.save(content, payload, "bob", False)
    with pytest.raises(PermissionError): templates.remove(original["id"], "bob", False, "CZ", {"OMODA"})
    templates._path(original["id"]).joinpath("source.xlsx").write_bytes(b"damaged")
    templates.remove(original["id"], "alice", False, "CZ", {"OMODA"})
    assert [record["id"] for record in templates.records()] == [copy["id"]]
    with pytest.raises(ValueError): templates.remove("../../", "admin", True, "CZ", {"OMODA"})


def test_export_rejects_changed_template_since_preview(source, monkeypatch):
    content, payload = source
    record = templates.save(content, payload, "alice", False)
    template = templates.load_template(record["id"], "alice", False, "CZ", {"OMODA"})
    monkeypatch.setattr(route, "_invoice_detail", Mock(return_value=detail()))
    monkeypatch.setattr(route, "_selected_template", Mock(return_value=template))
    with pytest.raises(HTTPException) as exc:
        route.export_pi_invoice("PI", {"templateId": record["id"], "templateRevision": "stale"}, Mock(), UserContext("order_filler", "alice"))
    assert exc.value.status_code == 409 and "changed since preview" in exc.value.detail
