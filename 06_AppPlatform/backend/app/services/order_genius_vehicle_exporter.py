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
from app.services.bom_admin_export_service import _normalise_font_xml_order
from app.services.vehicle_status_flow_config import get_vehicle_status_flow_config

# Private business documents live with existing slot-scoped operations data,
# never in the public source tree or an immutable release directory.
PI_TEMPLATE_ROOT = COC_MATCH_JOB_ROOT.parent / "pi_templates"


HEADERS = [
    "PI Code",
    "Official PI No",
    "Car Code",
    "VIN",
    "Engine ID",
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
    "piCode", "officialPiNo", "carCode", "vin", "engineId", "bom", "materialCode", "brand", "modelName", "version", "powertrain",
    "exteriorColorName", "interiorColorName", "orderDate", "productionDate", "etd", "eta", "shipName", "countryCode",
    "dealerCode", "dealerName", "customerRef", "allocationStatus", "logisticsStatus", "readyForPickupDate",
    "shippingScheduleUrl", "feishuTrackingUrl", "remark",
], HEADERS))
COLUMN_LABELS.update({"config": "Config", "fobEur": "FOB (EUR)", "cocPdf": "COC PDF", "orderMonth": "Order month",
                      "freightEur": "Freight / 运费 (EUR)", "insuranceEur": "Insurance / 保费 (EUR)",
                      "actualDepartureDate": "Actual departure", "actualArrivalDate": "Actual arrival",
                      "piCode": "PI", "materialCode": "Material", "exteriorColorName": "Exterior",
                      "interiorColorName": "Interior", "remark": "Note / 备注",
                      "readyForPickupDate": "Ready for pickup / 可提车"})


def generate_vehicle_allocation_excel(vehicles: list[dict], columns: list[str] | None = None, *, custom_labels: dict[str, str] | None = None) -> io.BytesIO:
    labels = {**COLUMN_LABELS, **(custom_labels or {})}
    if columns is not None and (not columns or len(columns) != len(set(columns)) or any(key not in labels for key in columns)):
        raise ValueError("Invalid export columns / 导出列无效，请恢复默认列后重试")
    headers = [labels[key] for key in columns] if columns is not None else HEADERS
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Vehicle Allocation"

    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=_excel_value(header))
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
            vehicle.get("engineId"),
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
    ws.page_setup.orientation = "landscape"
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{max(1, len(vehicles) + 1)}"
    for col_idx, header in enumerate(headers, 1):
        width = max(12, min(48, max(len(str(ws.cell(row, col_idx).value or "")) for row in range(1, ws.max_row + 1)) + 3))
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return _normalise_font_xml_order(output)


def _excel_value(value: object) -> object:
    if isinstance(value, str) and value.startswith(("=", "+", "-", "@")):
        return "'" + value
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def generate_vehicle_pivot_excel(vehicles: list[dict], config: dict) -> io.BytesIO:
    """Export the authorized source set plus formula-driven cross-tab; no repricing."""
    dimensions = {"countryCode", "piCode", "orderMonth", "brand", "modelName", "version", "powertrain", "shipName",
                  "materialCode", "exteriorColorName", "allocationStatus", "logisticsStatus"}
    metrics = {"units": "Units / 台数", "vin": "VIN assigned / 已录", "missingVin": "VIN missing / 待录",
               "allocated": "Allocated / 已分配", "ready": "Ready for pickup / 可提车", "fob": "FOB total (EUR) / 成交价合计"}
    rows, columns, measures, filters = (config.get(key) for key in ("rows", "columns", "measures", "filters"))
    flow = get_vehicle_status_flow_config(None, None)
    status_labels = {step["key"]: f'{step["labelEn"]} / {step["labelZh"]}' for step in [*flow["allocation"], *flow["logistics"]]}
    def display_label(key, value):
        if key in {"allocationStatus", "logisticsStatus"}:
            return status_labels.get(value, value)
        return "Not set / 未填写" if key == "shipName" and value == "—" else value
    if (any(not isinstance(keys, list) or len(keys) > len(dimensions)
            or any(not isinstance(key, str) or key not in dimensions for key in keys)
            or len(keys) != len(set(keys)) for keys in (rows, columns))
            or not isinstance(measures, list) or not measures or len(measures) > len(metrics)
            or any(not isinstance(key, str) or key not in metrics for key in measures)
            or len(measures) != len(set(measures)) or not isinstance(filters, dict)
            or any(key not in dimensions or not isinstance(values, list) or any(not isinstance(v, str) for v in values)
                   for key, values in filters.items())):
        raise ValueError("Invalid pivot dimensions/measure / 透视维度或指标无效")
    if len(rows + columns + list(filters)) != len(set(rows + columns + list(filters))):
        raise ValueError("Invalid pivot: a dimension belongs to one area / 同一字段只能放在一个区域")
    vehicles = list({v["carCode"]: v for v in vehicles}.values())
    vehicles = [v for v in vehicles if all(not values or str(v.get(key) or "—") in values for key, values in filters.items())]
    if not vehicles:
        raise ValueError("No vehicles for pivot / 无可透视车辆")
    def row_labels(vehicle):
        return tuple(str(vehicle.get(key) or "—") for key in rows)
    depths = list(range(1, len(rows) + 1)) if rows else [0]
    row_keys = sorted({row_labels(v)[:depth] for v in vehicles for depth in depths})
    col_keys = sorted({tuple(str(v.get(key) or "—") for key in columns) for v in vehicles})
    if len(row_keys) > 1000 or len(col_keys) * len(measures) > 100:
        raise ValueError("Pivot too large; narrow filters / 透视组合过多，请收窄筛选")
    row_ids = {key: i for i, key in enumerate(row_keys, 1)}
    col_ids = {key: i for i, key in enumerate(col_keys, 1)}
    column_count = len(col_keys) if columns else 0
    source_columns = list(dict.fromkeys(["carCode", "piCode", "countryCode", *rows, *columns, *filters,
                                       "vin", "engineId", "freightEur", "insuranceEur", "fobEur", "allocationStatus", "logisticsStatus"]))
    wb = openpyxl.load_workbook(generate_vehicle_allocation_excel(vehicles, source_columns))
    source = wb.worksheets[0]; source.title = "Source vehicles"
    prefix_start = source.max_column + 1
    column_group = prefix_start + len(depths)
    metric_start = column_group + 1
    for offset, depth in enumerate(depths):
        source.cell(1, prefix_start + offset, f"Row level {depth}")
    source.cell(1, column_group, "Column group")
    for offset, measure in enumerate(measures):
        source.cell(1, metric_start + offset, metrics[measure])
    totals: dict[tuple[int, int, str], Decimal] = {}
    for index, vehicle in enumerate(vehicles, 2):
        c = col_ids[tuple(str(vehicle.get(key) or "—") for key in columns)]
        values = {"units": 1, "vin": int(bool(vehicle.get("vin"))), "missingVin": int(not vehicle.get("vin")),
                 "allocated": int(vehicle.get("allocationStatus") == "allocated"),
                 "ready": int(vehicle.get("logisticsStatus") == "ready_for_pickup"),
                 "fob": vehicle.get("fobEur") or 0}
        source.cell(index, column_group, c)
        for offset, measure in enumerate(measures):
            source.cell(index, metric_start + offset, values[measure])
        for offset, depth in enumerate(depths):
            r = row_ids[row_labels(vehicle)[:depth]]
            source.cell(index, prefix_start + offset, r)
            for measure in measures:
                totals[r, c, measure] = totals.get((r, c, measure), Decimal(0)) + Decimal(str(values[measure]))
    ws = wb.create_sheet("Pivot", 0)
    label_count = max(1, len(rows))
    ws.append([*([COLUMN_LABELS[key] for key in rows] or ["All vehicles / 全部车辆"]),
               *[_excel_value(f'{" → ".join(display_label(key, value) for key, value in zip(columns, labels, strict=True))} · {metrics[measure]}') for labels in col_keys if columns for measure in measures],
               *[f"Total · {metrics[measure]}" for measure in measures]])
    cached = {}
    last = max(2, len(vehicles) + 1)
    col_col = get_column_letter(column_group)
    for r, labels in enumerate(row_keys, 1):
        ws.append([_excel_value(display_label(key, label)) for key, label in zip(rows[:len(labels)], labels, strict=True)] or ["All vehicles / 全部车辆"])
        ws.row_dimensions[r + 1].outlineLevel = min(7, max(0, len(labels) - 1))
        row_col = get_column_letter(prefix_start + depths.index(len(labels)))
        for c in range(1, column_count + 2):
            for offset, measure in enumerate(measures):
                metric = get_column_letter(metric_start + offset)
                condition = f",'Source vehicles'!${col_col}$2:${col_col}${last},{c}" if c <= column_count else ""
                cell = ws.cell(r + 1, label_count + (c - 1) * len(measures) + offset + 1,
                    f"=SUMIFS('Source vehicles'!${metric}$2:${metric}${last},'Source vehicles'!${row_col}$2:${row_col}${last},{r}{condition})")
                cached[cell.coordinate] = (totals.get((r, c, measure), Decimal(0)) if c <= column_count
                                          else sum(totals.get((r, column, measure), Decimal(0)) for column in range(1, len(col_keys) + 1)))
    total_row = len(row_keys) + 2
    ws.cell(total_row, 1, "Total")
    for c in range(1, column_count + 2):
        for offset, measure in enumerate(measures):
            metric = get_column_letter(metric_start + offset)
            formula = (f"SUMIFS('Source vehicles'!${metric}$2:${metric}${last},'Source vehicles'!${col_col}$2:${col_col}${last},{c})"
                       if c <= column_count else f"SUM('Source vehicles'!${metric}$2:${metric}${last})")
            cell = ws.cell(total_row, label_count + (c - 1) * len(measures) + offset + 1, "=" + formula)
            # Only leaf groups contribute to grand totals; parent subtotals are not added again.
            cached[cell.coordinate] = sum(value for (r, column, key), value in totals.items()
                if len(row_keys[r - 1]) == len(rows) and key == measure and (c > column_count or column == c))
    ws.cell(total_row + 2, 1, f"{len(vehicles)} source vehicles; metrics: {', '.join(measures)}; missing FOB: {sum(v.get('fobEur') is None for v in vehicles)}. Known amounts only; never repriced.")
    ws.merge_cells(start_row=total_row + 2, start_column=1, end_row=total_row + 2, end_column=ws.max_column)
    ws.row_dimensions[total_row + 2].height = 42
    for sheet in (ws, source):
        sheet.freeze_panes = "A2"; sheet.sheet_view.showGridLines = False
        sheet.page_setup.orientation = "landscape"; sheet.page_setup.fitToWidth = 1; sheet.page_setup.fitToHeight = 0
        sheet.sheet_properties.pageSetUpPr.fitToPage = True
        for c in range(1, sheet.max_column + 1):
            sheet.column_dimensions[get_column_letter(c)].width = max(24, sheet.column_dimensions[get_column_letter(c)].width) if sheet is source else 28
        for row in sheet:
            for cell in row:
                cell.font = HEADER_FONT if cell.row == 1 else NORMAL_FONT
                if cell.row == 1: cell.fill = HEADER_FILL
                cell.alignment = Alignment(vertical="center", wrap_text=True)
                if cell.data_type == "f": cell.number_format = '#,##0.00' if measures[(cell.column - label_count - 1) % len(measures)] == "fob" else '#,##0'
        sheet.row_dimensions[1].height = 54
    ws.row_dimensions[1].height = 84
    return _normalise_font_xml_order(_save_invoice(wb, cached))


def pi_invoice_context(detail: dict, template: dict | None = None) -> dict:
    """Offer fixed template information; opening this preview never writes orders."""
    context = _invoice_context(detail["header"], detail["lines"], template)
    charges = context.get("charges", ["freightEur", "insuranceEur", "handlingEur"])
    for key, field in (("missingFreightUnits", "freightEur"), ("missingInsuranceUnits", "insuranceEur")):
        context[key] = sum(v.get(field) is None for v in detail["vehicles"]) if field in charges else 0
    return context


def _invoice_context(header: dict, lines: list[dict], template: dict | None = None) -> dict:
    """Template identity and fixed fields are independent of the source order/month."""
    unit_count = sum(line["quantity"] for line in lines)
    if template:
        fields = template["config"]["fields"]
        values = {key: field["value"] for key, field in fields.items()}
        charges = template["config"]["charges"]
        return {"piCode": header["piCode"], "templateKey": template["id"], "templateName": template["name"],
                **{key: values.get(key, "") for key in ("buyerName", "buyerAddress", "buyerEmail", "sellerName", "sellerAddress", "bankDetails", "priceTerm", "paymentTerm")},
                "unitCount": unit_count, "lineCount": len(lines), "charges": charges,
                "missingFreightUnits": unit_count if "freightEur" in charges else 0,
                "missingInsuranceUnits": unit_count if "insuranceEur" in charges else 0,
                "templateRevision": template["updatedAt"],
                "defaults": {"invoiceDate": header.get("orderDate") or date.today().isoformat(),
                             "referenceNo": header.get("officialPiNo") or header["piCode"],
                             "portOfShipment": values.get("portOfShipment", ""),
                             "portOfDischarge": header.get("portOfDischarge") or values.get("portOfDischarge", "")}}
    country = str(header.get("orderingAccountCode") or header["countryCode"]).upper()
    profiles_path = PI_TEMPLATE_ROOT / "profiles.json"
    if not profiles_path.is_file():
        raise ValueError("PI templates not configured / 尚未配置 PI 模板，请联系管理员")
    profiles = json.loads(profiles_path.read_text())
    if country not in {"CH", "SE"} or country not in profiles or not lines or any(
        str(line.get("brand") or "").upper() not in {"OMODA", "JAECOO"}
        for line in lines
    ):
        raise ValueError("No matching country / brand PI template / 该订购账户国家及品牌尚未配置 PI 模板")
    profile = profiles[country]
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
        "unitCount": unit_count, "lineCount": len(lines),
        "missingFreightUnits": unit_count,
        "missingInsuranceUnits": unit_count,
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


def _invoice_groups(detail: dict, options: dict, charges: list[str]) -> dict:
    """Single saved-snapshot quantity/cost validation shared by both layouts."""
    vehicles = detail["vehicles"]
    counts = {line["piLineCode"]: 0 for line in detail["lines"]}
    grouped: dict[tuple, int] = {}
    for vehicle in vehicles:
        code = vehicle["piLineCode"]
        if code not in counts:
            raise ValueError("PI vehicle / line mismatch / 车辆与 PI 明细不一致，请重新读取")
        counts[code] += 1
        fob = _invoice_money(vehicle.get("fobEur"), "FOB", positive=True)
        freight = _invoice_money(options.get("freightEur", vehicle.get("freightEur")), "Freight / 运费") if "freightEur" in charges else Decimal(0)
        insurance = _invoice_money(options.get("insuranceEur", vehicle.get("insuranceEur")), "Insurance / 保费") if "insuranceEur" in charges else Decimal(0)
        key = (vehicle.get("modelName"), vehicle.get("version"), vehicle.get("powertrain"),
               vehicle.get("exteriorColorName"), vehicle.get("exteriorColorCode"),
               vehicle.get("interiorColorName"), vehicle.get("materialCode"), fob, freight, insurance)
        grouped[key] = grouped.get(key, 0) + 1
    if not vehicles or any(counts[line["piLineCode"]] != line["quantity"] for line in detail["lines"]):
        raise ValueError("PI vehicle quantity is incomplete / PI 车辆数量与明细不一致，请重新读取")
    return grouped


def _invoice_fields(context: dict, options: dict) -> tuple:
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
    return invoice_date, fields


def generate_pi_invoice_excel(detail: dict, options: dict, template: dict | None = None) -> io.BytesIO:
    """Fill a clean country template exclusively from saved PI snapshots."""
    context = pi_invoice_context(detail, template)
    charges = template["config"]["charges"] if template else ["freightEur", "insuranceEur", "handlingEur"]
    grouped = _invoice_groups(detail, options, charges)
    return _render_invoice(context, grouped, options, template)


def monthly_invoice_context(plan: dict, template: dict | None = None) -> dict:
    reference = f"PI-{plan['country']}-{plan['month'].replace('-', '')}"
    context = _invoice_context({"piCode": reference, "countryCode": plan["country"]}, plan["lines"], template)
    models: dict[tuple, dict] = {}
    for line in plan["lines"]:
        identity = (line.get("brand") or "", line.get("modelName") or "")
        group = models.setdefault(identity, {"brand": identity[0], "modelName": identity[1], "unitCount": 0})
        group["unitCount"] += line["quantity"]
    return {**context, "freightModels": list(models.values())}


def generate_monthly_invoice_excel(plan: dict, options: dict, template: dict | None = None) -> io.BytesIO:
    context = monthly_invoice_context(plan, template)
    charges = context.get("charges", ["freightEur", "insuranceEur", "handlingEur"])
    freight = Decimal(0)
    model_freight: dict[tuple, Decimal] = {}
    if "modelFreightEur" in options:
        if "freightEur" not in charges or "freightEur" in options:
            raise ValueError("Choose uniform or model freight, with a freight-enabled template / 请选择统一或车型运费，模板须计入运费")
        groups = options["modelFreightEur"]
        if not isinstance(groups, list):
            raise ValueError("Invalid model freight list / 车型运费列表无效")
        expected = {(item["brand"], item["modelName"]) for item in context["freightModels"]}
        for group in groups:
            if not isinstance(group, dict) or not all(isinstance(group.get(field), str) for field in ("brand", "modelName")):
                raise ValueError("Invalid freight model / 运费车型无效")
            identity = (group["brand"], group["modelName"])
            if identity not in expected or identity in model_freight:
                raise ValueError(f"Unexpected or repeated freight model: {' / '.join(identity)} / 运费车型超范围或重复，请重读月计划")
            model_freight[identity] = _invoice_money(group.get("freightEur"), f"Freight / 运费 ({' / '.join(identity)})")
        missing = expected - model_freight.keys()
        if missing:
            raise ValueError(f"Missing model freight: {', '.join(' / '.join(key) for key in sorted(missing))} / 请填写所有车型运费，或重读月计划")
    elif "freightEur" in charges:
        freight = _invoice_money(options.get("freightEur"), "Freight / 运费")
    insurance = _invoice_money(options.get("insuranceEur"), "Insurance / 保费") if "insuranceEur" in charges else Decimal(0)
    grouped: dict[tuple, int] = {}
    for line in plan["lines"]:
        line_freight = model_freight[(line.get("brand") or "", line.get("modelName") or "")] if model_freight else freight
        key = tuple(line.get(field) for field in ("modelName", "version", "powertrain", "exteriorColorName",
                    "exteriorColorCode", "interiorColorName", "materialCode")) + (
                    _invoice_money(line["fobEur"], "FOB", positive=True), line_freight, insurance)
        grouped[key] = grouped.get(key, 0) + line["quantity"]
    return _render_invoice(context, grouped, options, template)


def _render_invoice(context: dict, grouped: dict, options: dict, template: dict | None) -> io.BytesIO:
    invoice_date, fields = _invoice_fields(context, options)
    handling = _invoice_money(options.get("handlingEur", 0), "Handling Charges / 操作费")
    if template:
        if "handlingEur" not in template["config"]["charges"] and handling:
            raise ValueError("Template excludes handling / 模板不计操作费")
        return _render_mapped_invoice(template, context, fields, invoice_date, grouped, handling)
    country = context["templateKey"][:2]
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
        cached[f"{letter}{total_row}"] = sum(grouped.values()) if col == quantity_col else sum(cached[f"{letter}{row}"] for row in range(28, total_row))
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
    return _save_invoice(wb, cached)

def _save_invoice(wb, cached: dict) -> io.BytesIO:
    output = io.BytesIO()
    wb.save(output)
    wb.close()
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


def _render_mapped_invoice(template: dict, context: dict, fields: dict, invoice_date: date,
                           grouped: dict, handling: Decimal) -> io.BytesIO:
    """Preserve confirmed header/footer layout; replace all example details."""
    from openpyxl.utils import column_index_from_string
    from app.services.pi_invoice_templates import validate_config
    config = validate_config(template["path"].read_bytes(), template["config"])
    wb = openpyxl.load_workbook(template["path"], keep_links=False)
    ws = wb[config["sheetName"]]
    start, old_total = config["dataStart"], config["totalRow"]
    columns = {key: column_index_from_string(value) for key, value in config["columns"].items()}
    total_row, delta = start + len(grouped), len(grouped) - (old_total - start)
    max_row, max_col = ws.max_row, ws.max_column
    styles = [copy(ws.cell(start, col)._style) for col in range(1, max_col + 1)]
    height = ws.row_dimensions[start].height or 32
    footer_cells = [copy(cell) for row in ws.iter_rows(min_row=old_total, max_row=max_row) for cell in row if cell.__class__.__name__ != "MergedCell"]
    footer_heights = {row: ws.row_dimensions[row].height for row in range(old_total, max_row + 1)}
    footer_merges = [copy(merged) for merged in ws.merged_cells.ranges if merged.min_row >= old_total]
    for merged in list(ws.merged_cells.ranges):
        if merged.max_row >= start:
            ws.unmerge_cells(str(merged))
    ws.delete_rows(start, max_row - start + 1)
    for old in footer_cells:
        cell = ws.cell(old.row + delta, old.column)
        cell._style, cell.alignment = copy(old._style), copy(old.alignment)
        if old.row > old_total or (isinstance(old.value, str) and old.data_type != "f"):
            cell.value = old.value
            cell.data_type = old.data_type
    for old_row, row_height in footer_heights.items():
        ws.row_dimensions[old_row + delta].height = row_height
    for merged in footer_merges:
        ws.merge_cells(start_row=merged.min_row + delta, end_row=merged.max_row + delta,
                       start_column=merged.min_col, end_column=merged.max_col)
    for key, field in config["fields"].items():
        old = ws[field["cell"]]
        row = old.row + (delta if old.row > old_total else 0)
        cell = ws.cell(row, old.column)
        value = invoice_date if key == "invoiceDate" else fields.get(key, field["value"])
        cell.value = value if key == "invoiceDate" and not field["prefix"] else field["prefix"] + str(value)
        if isinstance(cell.value, str):
            cell.data_type = "s"
        if key == "invoiceDate" and not field["prefix"]:
            cell.number_format = "yyyy-mm-dd"
    cached = {}
    for index, (key, quantity) in enumerate(grouped.items()):
        model, version, powertrain, colour, code, interior, material, fob, freight, insurance = key
        row = start + index
        values = dict(number=index + 1, modelName=model or "-", version=version or "-", powertrain=powertrain or "-",
                      exteriorColorName=f"{colour or code or '-'}" + (f" ({code})" if colour and code and code not in str(colour) else ""),
                      interiorColorName=interior or "-", materialCode=material, quantity=quantity, fobEur=fob,
                      freightEur=freight, insuranceEur=insurance, handlingEur=handling)
        for col in range(1, max_col + 1):
            ws.cell(row, col)._style = copy(styles[col - 1])
        for field, col in columns.items():
            if field in {"total", "fobAmount"}:
                continue
            cell = ws.cell(row, col, values[field])
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            if isinstance(cell.value, str):
                cell.data_type = "s"
        q, f = f'{config["columns"]["quantity"]}{row}', f'{config["columns"]["fobEur"]}{row}'
        components = "+".join([f, *(f'{config["columns"][charge]}{row}' for charge in config["charges"])])
        for field, formula, amount in (("fobAmount", f"=ROUND({f}*{q},2)", fob * quantity),
                                     ("total", f"=ROUND(({components})*{q},2)", (fob + freight + insurance + (handling if "handlingEur" in config["charges"] else 0)) * quantity)):
            if field in columns:
                cell = ws.cell(row, columns[field], formula)
                cell.number_format = "#,##0.00"
                cached[cell.coordinate] = amount.quantize(Decimal("0.01"))
        ws.row_dimensions[row].height = height
    keys = list(grouped)
    for field, size in (("modelName", 1), ("version", 2)):
        if field not in columns:
            continue
        run = 0
        for end in range(1, len(keys) + 1):
            if end < len(keys) and keys[end][:size] == keys[run][:size]:
                continue
            if str(keys[run][size - 1] or "").strip() and end - run > 1:
                ws.merge_cells(start_row=start + run, end_row=start + end - 1,
                               start_column=columns[field], end_column=columns[field])
            run = end
    for field in ("quantity", "fobAmount", "total"):
        if field in columns:
            letter = config["columns"][field]
            cell = ws.cell(total_row, columns[field], f"=SUM({letter}{start}:{letter}{total_row - 1})")
            if cell.number_format == "General":
                cell.number_format = "#,##0" if field == "quantity" else "#,##0.00"
            cached[cell.coordinate] = context["unitCount"] if field == "quantity" else sum(cached[f"{letter}{row}"] for row in range(start, total_row))
            if field != "quantity":
                width = len(f"{cached[cell.coordinate]:,.2f}") + 10
                ws.column_dimensions[letter].width = max(ws.column_dimensions[letter].width, width)
    if not any(isinstance(cell.value, str) and cell.data_type != "f" for cell in ws[total_row]):
        numeric = {columns[key] for key in ("quantity", "fobAmount", "total") if key in columns}
        label = next((cell for cell in ws[total_row] if cell.column not in numeric and cell.__class__.__name__ != "MergedCell"), None)
        if label is not None:
            label.value = "Total"
    ws.auto_filter.ref = None
    wb.defined_names.clear()
    ws.print_area = f"A1:{get_column_letter(max_col)}{max_row + delta}"
    ws.print_title_rows = f'{config["headerRow"]}:{start - 1}'
    ws.page_setup.fitToWidth, ws.page_setup.fitToHeight = 1, 0
    ws.sheet_properties.pageSetUpPr.fitToPage = True
    return _save_invoice(wb, cached)
