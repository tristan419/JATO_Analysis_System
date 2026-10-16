import { useState } from "react";
import type { PiVehicleUnit } from "../types/orderGeniusVehicle";
import { CommandMultiSelect } from "./CommandSelect";

export const PIVOT_DIMENSIONS = {
  countryCode: "Country / 国家", piCode: "PI", orderMonth: "Order month / 订单月",
  modelName: "Model / 车型", version: "Trim / 配置", materialCode: "Material / 物料",
  exteriorColorName: "Colour / 颜色", allocationStatus: "Allocation / 分配", logisticsStatus: "Logistics / 物流",
};
export const PIVOT_MEASURES = { units: "Units / 台数", vin: "VIN assigned / 已录", missingVin: "VIN missing / 待录",
  allocated: "Allocated / 已分配", ready: "Ready for pickup / 可提车", fob: "FOB total (EUR) / 成交价合计" };
type Dimension = keyof typeof PIVOT_DIMENSIONS;
type Measure = keyof typeof PIVOT_MEASURES;
export interface VehiclePivotConfig { rows: Dimension[]; column: Dimension; measure: Measure }
interface Props {
  vehicles: PiVehicleUnit[]; filtered: PiVehicleUnit[]; selected: ReadonlySet<string>; busy: boolean;
  onDrill: (codes: string[]) => void;
  onExport: (vehicles: PiVehicleUnit[], pivot: VehiclePivotConfig) => void;
}
export function pivotValue(vehicle: PiVehicleUnit, measure: Measure): number {
  if (measure === "units") return 1;
  if (measure === "vin") return vehicle.vin ? 1 : 0;
  if (measure === "missingVin") return vehicle.vin ? 0 : 1;
  if (measure === "allocated") return vehicle.allocationStatus === "allocated" ? 1 : 0;
  if (measure === "ready") return vehicle.logisticsStatus === "ready_for_pickup" ? 1 : 0;
  return vehicle.fobEur ?? 0;
}
export function buildVehiclePivot(vehicles: PiVehicleUnit[], config: VehiclePivotConfig): Map<string, Map<string, PiVehicleUnit[]>> {
  const result = new Map<string, Map<string, PiVehicleUnit[]>>();
  for (const vehicle of new Map(vehicles.map((v) => [v.carCode, v])).values()) {
    const row = JSON.stringify(config.rows.map((key) => vehicle[key] || "—"));
    const column = String(vehicle[config.column] || "—");
    const columns = result.get(row) ?? new Map<string, PiVehicleUnit[]>();
    columns.set(column, [...(columns.get(column) ?? []), vehicle]); result.set(row, columns);
  }
  return result;
}
export function VehicleAnalysisPivot({ vehicles, filtered, selected, busy, onDrill, onExport }: Props) {
  const [rows, setRows] = useState<Dimension[]>(["countryCode", "modelName"]);
  const [column, setColumn] = useState<Dimension>("logisticsStatus");
  const [measure, setMeasure] = useState<Measure>("units");
  const [scope, setScope] = useState("filtered");
  const source = scope === "selected" ? vehicles.filter((v) => selected.has(v.carCode)) : filtered;
  const config: VehiclePivotConfig = { rows, column, measure };
  const groups = buildVehiclePivot(source, config);
  const columns = [...new Set(source.map((v) => String(v[column] || "—")))].sort();
  const oversized = groups.size > 1000 || columns.length > 100;
  const missing = measure === "fob" ? source.filter((v) => v.fobEur === null).length : 0;
  const value = (items: PiVehicleUnit[]): string => items.reduce((sum, item) => sum + pivotValue(item, measure), 0)
    .toLocaleString(undefined, { maximumFractionDigits: measure === "fob" ? 2 : 0 });
  return <section className="va-analysis-pivot" aria-label="Vehicle pivot / 车辆透视">
    <div className="va-button-row">
      <label>Rows / 行维度<CommandMultiSelect selected={rows} options={Object.entries(PIVOT_DIMENSIONS).map(([key, label]) => ({ value: key, label }))}
        onChange={(values) => { const next = values.filter((key): key is Dimension => key in PIVOT_DIMENSIONS); if (next.length) setRows(next); }} placeholder="Choose row dimensions / 选择行维度" /></label>
      <label>Columns / 列维度<select aria-label="Pivot column" value={column} onChange={(e) => { const v = e.target.value; if (v in PIVOT_DIMENSIONS) setColumn(v as Dimension); }}>{Object.entries(PIVOT_DIMENSIONS).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label>
      <label>Measure / 指标<select aria-label="Pivot measure" value={measure} onChange={(e) => { const v = e.target.value; if (v in PIVOT_MEASURES) setMeasure(v as Measure); }}>{Object.entries(PIVOT_MEASURES).map(([key, label]) => <option key={key} value={key}>{label}</option>)}</select></label>
      <label>Scope / 范围<select aria-label="Pivot scope" value={scope} onChange={(e) => setScope(e.target.value)}><option value="filtered">Filtered / 当前筛选</option><option value="selected">Selected / 已勾选</option></select></label>
      <button type="button" disabled={busy || !source.length || oversized} onClick={() => onExport(source, config)}>Export pivot / 导出透视</button>
    </div>
    <p role="status">{source.length} vehicles / 台 · {groups.size} row groups / 行分组 · Click a value to inspect source vehicles / 点击数字查看来源车辆{missing ? ` · ${missing} missing FOB / 缺成交价，未按零价补齐；合计仅含已知金额` : ""}</p>
    {oversized ? <p role="alert">Too many row/column combinations. Narrow filters or dimensions / 行列组合过多，请收窄筛选或维度；未截断数据。</p> : <div style={{ overflow: "auto" }}><table className="data-table"><thead><tr>{rows.map((key) => <th key={key}>{PIVOT_DIMENSIONS[key]}</th>)}{columns.map((c) => <th key={c}>{c}</th>)}<th>Total / 合计</th></tr></thead>
      <tbody>{[...groups].sort(([a], [b]) => a.localeCompare(b)).map(([key, cells]) => {
        const labels: string[] = JSON.parse(key);
        const all = [...cells.values()].flat();
        return <tr key={key}>{labels.map((label, index) => <th key={index}>{label}</th>)}{columns.map((c) => { const items = cells.get(c) ?? []; return <td key={c}>{items.length ? <button type="button" onClick={() => onDrill(items.map((v) => v.carCode))}>{value(items)}</button> : "—"}</td>; })}
          <td><button type="button" onClick={() => onDrill(all.map((v) => v.carCode))}>{value(all)}</button></td></tr>;
      })}<tr><th colSpan={Math.max(1, rows.length)}>Total / 合计</th>{columns.map((c) => <td key={c}>{value(source.filter((v) => String(v[column] || "—") === c))}</td>)}<td>{value(source)}</td></tr></tbody></table></div>}
  </section>;
}
