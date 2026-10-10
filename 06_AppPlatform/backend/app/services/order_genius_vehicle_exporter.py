"""Excel export for Order Genius PI vehicle allocation."""

from __future__ import annotations

import io
import json
from copy import copy
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from zipfile import ZipFile, ZIP_DEFLATED
from xml.etree import ElementTree

import openpyxl
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from app.core.config import COC_MATCH_JOB_ROOT

# Private business documents live with existing slot-scoped operations data,
# never in the public source tree or an immutable release directory.
PI_TEMPLATE_ROOT = COC_MATCH_JOB_ROOT.parent / "pi_templates"


HEADERS = [
    "PI Code",
    "Official PI No",
    "Car Code",
    "VIN",
    "BOM",
    "Material Code",
    "Brand",
    "Model",
    "Version",
    "Powertrain",
    "Exterior Colour",
    "Interior Colour",
    "Order Date",
    "Production Date",
    "ETD",
    "ETA",
    "Ship Name",
    "Country",
    "Dealer Code",
    "Dealer Name",
    "Customer Ref",
    "Allocation Status",
    "Logistics Status",
    "Ready for Pickup Date",
    "Shipping Schedule URL",
    "Feishu Tracking URL",
    "Remark",
]

HEADER_FILL = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
HEADER_FONT = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
NORMAL_FONT = Font(name="Calibri", size=11)
STRIPED_FILL = PatternFill(start_color="EAF2F8", end_color="EAF2F8", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style="thin"),
    right=Side(style="thin"),
    top=Side(style="thin"),
    bottom=Side(style="thin"),
)
CENTER_ALIGN = Alignment(horizontal="center", vertical="center")
LEFT_ALIGN = Alignment(horizontal="left", vertical="center")

COLUMN_LABELS = dict(zip([
    "piCode", "officialPiNo", "carCode", "vin", "bom", "materialCode", "brand", "modelName", "version", "powertrain",
    "exteriorColorName", "interiorColorName", "orderDate", "productionDate", "etd", "eta", "shipName", "countryCode",
    "dealerCode", "dealerName", "customerRef", "allocationStatus", "logisticsStatus", "readyForPickupDate",
    "shippingScheduleUrl", "feishuTrackingUrl", "remark",
], HEADERS))
COLUMN_LABELS.update({"config": "Config", "fobEur": "FOB (EUR)", "cocPdf": "COC PDF",
                      "freightEur": "Freight / 运费 (EUR)", "insuranceEur": "Insurance / 保费 (EUR)",
                      "actualDepartureDate": "Actual departure", "actualArrivalDate": "Actual arrival",
                      "piCode": "PI", "materialCode": "Material", "exteriorColorName": "Exterior",
                      "interiorColorName": "Interior", "remark": "Note / 备注",
                      "readyForPickupDate": "Ready for pickup / 可提车"})


def generate_vehicle_allocation_excel(vehicles: list[dict], columns: list[str] | None = None) -> io.BytesIO:
    if columns is not None and (not columns or len(columns) != len(set(columns)) or any(key not in COLUMN_LABELS for key in columns)):
        raise ValueError("Invalid export columns / 导出列无效，请恢复默认列后重试")
    headers = [COLUMN_LABELS[key] for key in columns] if columns is not None else HEADERS
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Vehicle Allocation"

    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = CENTER_ALIGN
        cell.border = THIN_BORDER

    for row_idx, vehicle in enumerate(vehicles, start=2):
        row_values = ([f"{vehicle.get('modelName') or '-'} / {vehicle.get('version') or '-'}" if key == "config" else vehicle.get(key) for key in columns] if columns is not None else [
            vehicle.get("piCode"),
            vehicle.get("officialPiNo"),
            vehicle.get("carCode"),
            vehicle.get("vin"),
            vehicle.get("bom"),
            vehicle.get("materialCode"),
            vehicle.get("brand"),
            vehicle.get("modelName"),
            vehicle.get("version"),
            vehicle.get("powertrain"),
            vehicle.get("exteriorColorName"),
            vehicle.get("interiorColorName"),
            vehicle.get("orderDate"),
            vehicle.get("productionDate"),
            vehicle.get("etd"),
            vehicle.get("eta"),
            vehicle.get("shipName"),
            vehicle.get("countryCode"),
            vehicle.get("dealerCode"),
            vehicle.get("dealerName"),
            vehicle.get("customerRef"),
            vehicle.get("allocationStatus"),
            vehicle.get("logisticsStatus"),
            vehicle.get("readyForPickupDate"),
            vehicle.get("shippingScheduleUrl"),
            vehicle.get("feishuTrackingUrl"),
            vehicle.get("remark"),
        ])
        row_fill = STRIPED_FILL if row_idx % 2 == 0 else None
        for col_idx, value in enumerate(row_values, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=_excel_value(value))
            cell.font = NORMAL_FONT
            if row_fill:
                cell.fill = row_fill
            cell.alignment = LEFT_ALIGN
            cell.border = THIN_BORDER

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{max(1, len(vehicles) + 1)}"
    for col_idx, header in enumerate(headers, 1):
        width = max(12, min(28, len(header) + 4))
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


def _excel_value(value: object) -> object:
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def pi_invoice_context(detail: dict) -> dict:
    """Offer fixed template information; opening this preview never writes orders."""
    header = detail["header"]
    country = str(header.get("orderingAccountCode") or header["countryCode"]).upper()
    profiles_path = PI_TEMPLATE_ROOT / "profiles.json"
    if not profiles_path.is_file():
        raise ValueError("PI templates not configured / 尚未配置 PI 模板，请联系管理员")
    profiles = json.loads(profiles_path.read_text())
    if country not in {"CH", "SE"} or country not in profiles or not detail["lines"] or any(
        str(line.get("brand") or "").upper() not in {"OMODA", "JAECOO"}
        for line in detail["lines"]
    ):
        raise ValueError("No matching country / brand PI template / 该订购账户国家及品牌尚未配置 PI 模板")
    profile = profiles[country]
    vehicles = detail["vehicles"]
    template_path = PI_TEMPLATE_ROOT / f"{country}_OJ.xlsx"
    if not template_path.is_file():
        raise ValueError("PI template file missing / PI 模板文件缺失，请联系管理员")
    workbook = openpyxl.load_workbook(template_path)
    sheet = workbook.active
    fixed = {key: str(sheet[cell].value or "") for key, cell in {
        "sellerName": "A1", "sellerAddress": "A3", "bankDetails": "C24",
        "priceTerm": "C21", "paymentTerm": "C22"}.items()}
    workbook.close()
    return {
        "piCode": header["piCode"], "templateKey": f"{country}_OJ",
        "templateName": profile["name"], "buyerName": profile["buyerName"],
        "buyerAddress": profile["buyerAddress"], "buyerEmail": profile["buyerEmail"],
        **fixed,
        "unitCount": len(vehicles), "lineCount": len(detail["lines"]),
        "missingFreightUnits": sum(v.get("freightEur") is None for v in vehicles),
        "missingInsuranceUnits": sum(v.get("insuranceEur") is None for v in vehicles),
        "defaults": {
            "invoiceDate": header.get("orderDate") or date.today().isoformat(),
            "referenceNo": header.get("officialPiNo") or header["piCode"],
            "portOfShipment": profile["portOfShipment"],
            "portOfDischarge": header.get("portOfDischarge") or profile["portOfDischarge"],
        },
    }


def _invoice_money(value: object, label: str, *, positive: bool = False) -> Decimal:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"Missing or invalid {label} / 请填写有效的 {label}") from None
    if not amount.is_finite() or amount < 0 or amount >= Decimal("1000000000000") or (positive and amount == 0):
        raise ValueError(f"Invalid {label} / {label} 金额无效")
    return amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def generate_pi_invoice_excel(detail: dict, options: dict) -> io.BytesIO:
    """Fill a clean country template exclusively from saved PI snapshots."""
    context = pi_invoice_context(detail)
    country = context["templateKey"][:2]
    header = detail["header"]
    vehicles = detail["vehicles"]
    counts = {line["piLineCode"]: 0 for line in detail["lines"]}
    grouped: dict[tuple, int] = {}
    for vehicle in vehicles:
        code = vehicle["piLineCode"]
        if code not in counts:
            raise ValueError("PI vehicle / line mismatch / 车辆与 PI 明细不一致，请重新读取")
        counts[code] += 1
        fob = _invoice_money(vehicle.get("fobEur"), "FOB", positive=True)
        freight = _invoice_money(options.get("freightEur", vehicle.get("freightEur")), "Freight / 运费")
        insurance = _invoice_money(options.get("insuranceEur", vehicle.get("insuranceEur")), "Insurance / 保费")
        key = (vehicle.get("modelName"), vehicle.get("version"), vehicle.get("powertrain"),
               vehicle.get("exteriorColorName"), vehicle.get("exteriorColorCode"),
               vehicle.get("interiorColorName"), vehicle.get("materialCode"), fob, freight, insurance)
        grouped[key] = grouped.get(key, 0) + 1
    if not vehicles or any(counts[line["piLineCode"]] != line["quantity"] for line in detail["lines"]):
        raise ValueError("PI vehicle quantity is incomplete / PI 车辆数量与明细不一致，请重新读取")
    defaults = context["defaults"]
    try:
        invoice_date = date.fromisoformat(str(options.get("invoiceDate", defaults["invoiceDate"])))
    except ValueError:
        raise ValueError("Invalid invoice date / 发票日期无效") from None
    fields = {key: options.get(key, defaults[key]) for key in defaults if key != "invoiceDate"}
    if any(not isinstance(value, str) for value in fields.values()):
        raise ValueError("Check reference number and ports / 请填写有效的 PI 编号及港口")
    fields = {key: value.strip() for key, value in fields.items()}
    if any(not value or len(value) > 300 for value in fields.values()):
        raise ValueError("Check reference number and ports / 请填写有效的 PI 编号及港口")
    handling = _invoice_money(options.get("handlingEur", 0), "Handling Charges / 操作费")
    if country == "SE" and handling:
        raise ValueError("SE template has no handling charge column / SE 模板不含操作费")

    template = PI_TEMPLATE_ROOT / f"{country}_OJ.xlsx"
    wb = openpyxl.load_workbook(template)
    ws = wb.active
    ws.title = "Proforma Invoice"
    last_col = 12 if country == "CH" else 13
    row_styles = [copy(ws.cell(28, col)._style) for col in range(1, last_col + 1)]
    for merged in list(ws.merged_cells.ranges):
        if merged.max_row >= 28 or merged.max_col > last_col or 8 <= merged.min_row <= 11:
            ws.unmerge_cells(str(merged))
    # Original templates contain order examples and entire-column filter ranges.
    ws.delete_rows(28, ws.max_row - 27)
    ws.auto_filter.ref = None
    wb.defined_names.clear()
    for row in range(17, 25):
        if f"C{row}:{get_column_letter(last_col)}{row}" not in ws.merged_cells:
            ws.merge_cells(start_row=row, start_column=3, end_row=row, end_column=last_col)
    for row in (9, 10, 11):
        ws.merge_cells(start_row=row, start_column=2, end_row=row, end_column=8 if country == "CH" else 9)
    for row in ((9, 10) if country == "CH" else (8, 9)):
        ws.merge_cells(start_row=row, start_column=10 if country == "CH" else 11, end_row=row, end_column=last_col)
    def text_at(row: int, col: int, value: object) -> None:
        cell = ws.cell(row, col, str(value or ""))
        cell.data_type = "s"
    text_at(7, 1, "PROFORMA INVOICE")
    text_at(9, 2, f"M/S: {context['buyerName']}")
    text_at(10, 2, f"Address: {context['buyerAddress']}")
    text_at(11, 2, f"Email: {context['buyerEmail']}" if context["buyerEmail"] else "")
    for row in ws.iter_rows(min_row=1, max_row=27, max_col=last_col):
        for cell in row:
            if cell.value is not None:
                cell.font = Font(name="Arial", size=10, bold=cell.font.bold)
    for row in (9, 10, 11):
        ws.cell(row, 2).alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
    ws.row_dimensions[9].height = 30 if country == "SE" else 20
    ws.row_dimensions[10].height = 30
    ws.cell(9 if country == "CH" else 8, 10 if country == "CH" else 11, invoice_date).number_format = "yyyy-mm-dd"
    text_at(10 if country == "CH" else 9, 10 if country == "CH" else 11, fields["referenceNo"])
    ws.cell(10 if country == "CH" else 9, 10 if country == "CH" else 11).alignment = Alignment(wrap_text=True, vertical="center")
    for row in ((9, 10) if country == "CH" else (8, 9)):
        ws.cell(row, 10 if country == "CH" else 11).font = Font(name="Arial", size=10, bold=True)
    if country == "SE":
        ws.row_dimensions[27].height = 60
    text_at(25, 1, "9-Description and Quantity: Brand New SUV Motor Vehicles")
    quantity_col, fob_col, amount_col = (6, 7, 8) if country == "CH" else (8, 9, 10)
    for col in (amount_col, last_col):
        dimension = ws.column_dimensions[get_column_letter(col)]
        dimension.width = max(dimension.width, 16)
    material_column = ws.column_dimensions["E" if country == "CH" else "G"]
    material_column.width = max(material_column.width, 20)
    cached: dict[str, Decimal | int] = {}
    for number, (key, quantity) in enumerate(grouped.items(), 1):
        model, version, powertrain, colour, colour_code, interior, material, fob, freight, insurance = key
        colour_label = str(colour or colour_code or "-")
        if colour and colour_code and colour_code not in colour_label:
            colour_label += f" ({colour_code})"
        row = 27 + number
        values = ([number, model, version, colour_label, material, quantity, fob, None, freight, insurance, handling, None]
                  if country == "CH" else [number, model, powertrain, version, colour_label, interior or "-", material,
                                            quantity, fob, None, freight, insurance, None])
        for col, value in enumerate(values, 1):
            cell = ws.cell(row, col, value)
            cell._style = copy(row_styles[col - 1])
            cell.font = Font(name="Arial", size=10)
            cell.alignment = Alignment(horizontal="right" if col >= quantity_col else "center", vertical="center", wrap_text=True)
            if isinstance(value, str):
                cell.data_type = "s"
            if col >= fob_col:
                cell.number_format = '#,##0.00'
        ws.row_dimensions[row].height = 32
        q = f"{get_column_letter(quantity_col)}{row}"
        ws.cell(row, amount_col, f"=ROUND({get_column_letter(fob_col)}{row}*{q},2)")
        components = "+".join(f"{get_column_letter(col)}{row}" for col in ([7, 9, 10, 11] if country == "CH" else [9, 11, 12]))
        ws.cell(row, last_col, f"=ROUND(({components})*{q},2)")
        cached[f"{get_column_letter(amount_col)}{row}"] = (fob * quantity).quantize(Decimal("0.01"))
        cached[f"{get_column_letter(last_col)}{row}"] = ((fob + freight + insurance + (handling if country == "CH" else 0)) * quantity).quantize(Decimal("0.01"))
    total_row = 28 + len(grouped)
    keys = list(grouped)
    # Version runs stop at model boundaries; never reorder invoice details.
    for column, key_size in ((2, 1), (3 if country == "CH" else 4, 2)):
        start = 0
        for end in range(1, len(keys) + 1):
            if end < len(keys) and keys[end][:key_size] == keys[start][:key_size]:
                continue
            if str(keys[start][key_size - 1] or "").strip() and end - start > 1:
                ws.merge_cells(start_row=28 + start, start_column=column,
                               end_row=27 + end, end_column=column)
            start = end
    ws.merge_cells(start_row=total_row, start_column=1, end_row=total_row, end_column=quantity_col - 1)
    text_at(total_row, 1, "Total")
    for col in range(1, last_col + 1):
        cell = ws.cell(total_row, col)
        cell.font = Font(name="Arial", size=10, bold=True)
        cell.fill = STRIPED_FILL
        cell.border = THIN_BORDER
        cell.number_format = "#,##0" if col == quantity_col else "#,##0.00"
    for col in (quantity_col, amount_col, last_col):
        letter = get_column_letter(col)
        ws.cell(total_row, col, f"=SUM({letter}28:{letter}{total_row - 1})")
        cached[f"{letter}{total_row}"] = len(vehicles) if col == quantity_col else sum(cached[f"{letter}{row}"] for row in range(28, total_row))
    for offset, label in enumerate([
        f"10-Port of Shipment: {fields['portOfShipment']}",
        f"11-Port of Discharge: {fields['portOfDischarge']}",
        "12-Validity: 3 Months from issue date.", "", "Sincerely yours,",
    ], 1):
        row = total_row + offset
        ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=last_col)
        text_at(row, 1, label)
        ws.cell(row, 1).font = Font(name="Arial", size=10)
        ws.row_dimensions[row].height = 22
    ws.print_area = f"A1:{get_column_letter(last_col)}{total_row + 5}"
    ws.print_options.gridLines = False
    ws.sheet_view.showGridLines = False
    ws.page_setup.orientation = "landscape"
    ws.page_setup.paperSize = ws.PAPERSIZE_A4
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.print_title_rows = "26:27"
    output = io.BytesIO()
    wb.save(output)
    # openpyxl keeps live formulas but cannot write their calculated cache.
    # Cache only the totals computed above; Excel still recalculates on edits.
    result = io.BytesIO()
    with ZipFile(output) as source, ZipFile(result, "w", ZIP_DEFLATED) as target:
        for item in source.infolist():
            content = source.read(item.filename)
            if item.filename == "xl/worksheets/sheet1.xml":
                ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
                root = ElementTree.fromstring(content)
                for cell in root.iter(f"{ns}c"):
                    if cell.get("r") in cached:
                        cell.find(f"{ns}v").text = str(cached[cell.get("r")])
                content = ElementTree.tostring(root, encoding="utf-8")
            target.writestr(item, content)
    result.seek(0)
    return result
