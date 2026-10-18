import { useState, type CSSProperties } from "react";
import type { PiVehicleUnit } from "../types/orderGeniusVehicle";
import { CommandMultiSelect } from "./CommandSelect";
import { orderGeniusPowertrainColor } from "../utils/orderGeniusBomAdmin";
import "./VehicleAnalysisPivot.css";

export const PIVOT_DIMENSIONS = {
  countryCode: "Country / 国家", piCode: "PI", orderMonth: "Order month / 订单月", brand: "Brand / 品牌",
  modelName: "Model / 车型", version: "Trim / 配置", powertrain: "Powertrain / 动力", materialCode: "Material / 物料",
  exteriorColorName: "Colour / 颜色", shipName: "Ship / 船名", allocationStatus: "Allocation / 分配", logisticsStatus: "Logistics / 物流",
};
export const PIVOT_MEASURES = { units: "Units / 台数", vin: "VIN assigned / 已录", missingVin: "VIN missing / 待录",
  allocated: "Allocated / 已分配", ready: "Ready for pickup / 可提车", fob: "FOB total (EUR) / 成交价合计" };
type Dimension = keyof typeof PIVOT_DIMENSIONS;
type Measure = keyof typeof PIVOT_MEASURES;
type Zone = "rows" | "columns" | "measures" | "filters";
export interface VehiclePivotConfig { rows: Dimension[]; columns: Dimension[]; measures: Measure[]; filters: Partial<Record<Dimension, string[]>> }
interface Props {
  vehicles: PiVehicleUnit[]; filtered: PiVehicleUnit[]; selected: ReadonlySet<string>; busy: boolean;
  onDrill: (codes: string[]) => void; onExport: (vehicles: PiVehicleUnit[], pivot: VehiclePivotConfig) => void;
  statusLabels?: Record<string, string>;
}
const isDimension = (key: string): key is Dimension => Object.prototype.hasOwnProperty.call(PIVOT_DIMENSIONS, key);
const isMeasure = (key: string): key is Measure => Object.prototype.hasOwnProperty.call(PIVOT_MEASURES, key);
const dimensionValue = (vehicle: PiVehicleUnit, key: Dimension): string => String(vehicle[key] || "—");
export function pivotValue(vehicle: PiVehicleUnit, measure: Measure): number {
  if (measure === "units") return 1;
  if (measure === "vin") return vehicle.vin ? 1 : 0;
  if (measure === "missingVin") return vehicle.vin ? 0 : 1;
  if (measure === "allocated") return vehicle.allocationStatus === "allocated" ? 1 : 0;
  if (measure === "ready") return vehicle.logisticsStatus === "ready_for_pickup" ? 1 : 0;
  return vehicle.fobEur ?? 0;
}
export function filterPivotVehicles(vehicles: PiVehicleUnit[], config: VehiclePivotConfig): PiVehicleUnit[] {
  return [...new Map(vehicles.map((v) => [v.carCode, v])).values()].filter((v) =>
    Object.entries(config.filters).every(([key, values]) => isDimension(key) && (!values?.length || values.includes(dimensionValue(v, key)))));
}
export function buildVehiclePivot(vehicles: PiVehicleUnit[], config: VehiclePivotConfig): Map<string, Map<string, PiVehicleUnit[]>> {
  const result = new Map<string, Map<string, PiVehicleUnit[]>>();
  for (const vehicle of filterPivotVehicles(vehicles, config)) {
    const row = JSON.stringify(config.rows.map((key) => dimensionValue(vehicle, key)));
    const column = JSON.stringify(config.columns.map((key) => dimensionValue(vehicle, key)));
    const columns = result.get(row) ?? new Map<string, PiVehicleUnit[]>();
    columns.set(column, [...(columns.get(column) ?? []), vehicle]); result.set(row, columns);
  }
  return result;
}
export function VehicleAnalysisPivot({ vehicles, filtered, selected, busy, onDrill, onExport, statusLabels = {} }: Props) {
  const [config, setConfig] = useState<VehiclePivotConfig>({ rows: ["countryCode", "modelName"], columns: ["logisticsStatus"], measures: ["units"], filters: {} });
  const [settings, setSettings] = useState(false), [closed, setClosed] = useState<Set<string>>(new Set());
  const [scope, setScope] = useState("filtered");
  const basis = scope === "selected" ? vehicles.filter((v) => selected.has(v.carCode)) : filtered;
  const source = filterPivotVehicles(basis, config), groups = buildVehiclePivot(source, config);
  const colKeys = config.columns.length ? [...new Set([...groups.values()].flatMap((cells) => [...cells.keys()]))].sort() : [];
  const columnLabels = colKeys.map((key): string[] => JSON.parse(key));
  const nodes = new Map<string, { labels: string[]; items: PiVehicleUnit[] }>();
  for (const [key, cells] of groups) {
    const labels: string[] = JSON.parse(key), items = [...cells.values()].flat();
    for (let depth = config.rows.length ? 1 : 0; depth <= labels.length; depth++) {
      const prefix = labels.slice(0, depth), id = JSON.stringify(prefix);
      nodes.set(id, { labels: prefix, items: [...(nodes.get(id)?.items ?? []), ...items] });
    }
  }
  const oversized = nodes.size > 1000 || colKeys.length * config.measures.length > 100;
  const missing = config.measures.includes("fob") ? source.filter((v) => v.fobEur === null).length : 0;
  const label = (key: Dimension, text: string): string => key === "allocationStatus" || key === "logisticsStatus"
    ? statusLabels[text] ?? text.replaceAll("_", " ") : text === "—" && key === "shipName" ? "Not set / 未填写" : text;
  const value = (items: PiVehicleUnit[], measure: Measure): string => items.reduce((sum, item) => sum + pivotValue(item, measure), 0)
    .toLocaleString(undefined, { maximumFractionDigits: measure === "fob" ? 2 : 0 });
  function fields(zone: Zone): string[] { return zone === "filters" ? Object.keys(config.filters) : config[zone]; }
  function move(field: string, zone: Zone, position?: number): void {
    if (busy || (zone === "measures" ? !isMeasure(field) : !isDimension(field))) return;
    setClosed(new Set());
    setConfig((previous) => {
      if (isMeasure(field)) { const measures = previous.measures.filter((key) => key !== field); measures.splice(position ?? measures.length, 0, field); return { ...previous, measures }; }
      if (!isDimension(field)) return previous;
      const next = { ...previous, rows: previous.rows.filter((key) => key !== field), columns: previous.columns.filter((key) => key !== field), filters: { ...previous.filters } };
      delete next.filters[field];
      if (zone === "filters") {
        const keys = Object.keys(next.filters).filter(isDimension); keys.splice(position ?? keys.length, 0, field);
        next.filters = Object.fromEntries(keys.map((key) => [key, previous.filters[key] ?? []]));
      } else if (zone === "rows" || zone === "columns") next[zone].splice(position ?? next[zone].length, 0, field);
      return next;
    });
  }
  function remove(field: string, zone: Zone): void {
    setClosed(new Set());
    setConfig((previous) => {
      if (zone === "filters" && isDimension(field)) { const filters = { ...previous.filters }; delete filters[field]; return { ...previous, filters }; }
      if (zone === "measures") return previous.measures.length > 1 ? { ...previous, measures: previous.measures.filter((key) => key !== field) } : previous;
      return zone === "rows" || zone === "columns" ? { ...previous, [zone]: previous[zone].filter((key) => key !== field) } : previous;
    });
  }
  const zones: Array<[Zone, string]> = [["rows", "Rows / 行"], ["columns", "Columns / 列"], ["measures", "Values / 值"], ["filters", "Filters / 筛选"]];
  const orderedNodes = [...nodes].sort(([, a], [, b]) => {
    for (let i = 0; i < Math.min(a.labels.length, b.labels.length); i++) { const order = a.labels[i].localeCompare(b.labels[i]); if (order) return order; }
    return a.labels.length - b.labels.length;
  });
  return <section className="va-analysis-pivot" aria-label="Vehicle pivot / 车辆透视">
    <div className="va-button-row"><button type="button" className="btn-secondary" aria-expanded={settings} onClick={() => setSettings(!settings)}>Field settings / 字段设置</button>
      <label>Scope / 范围<select aria-label="Pivot scope" value={scope} onChange={(e) => setScope(e.target.value)}><option value="filtered">Filtered / 当前筛选</option><option value="selected">Selected / 已勾选</option></select></label>
      <button type="button" disabled={busy || !source.length || oversized} onClick={() => onExport(source, config)}>Export pivot / 导出透视</button></div>
    {settings && <div className="va-pivot-fields"><p>Drag fields, or use Add and ↑ ↓. Filters here only affect this pivot. / 可拖动或点击编排；这里的筛选不改主表或勾选。</p>
      <div className="va-pivot-field-pool">{[...Object.entries(PIVOT_DIMENSIONS), ...Object.entries(PIVOT_MEASURES)].map(([key, text]) => <button type="button" disabled={busy} draggable={!busy} key={key}
        onDragStart={(event) => event.dataTransfer.setData("text/plain", key)} onClick={() => move(key, isMeasure(key) ? "measures" : "rows")}>{text}</button>)}</div>
      <div className="va-pivot-zones">{zones.map(([zone, title]) => <fieldset key={zone} disabled={busy} aria-label={title}
        onDragOver={(event) => event.preventDefault()} onDrop={(event) => { event.preventDefault(); move(event.dataTransfer.getData("text/plain"), zone); }}><legend>{title}</legend>
        {fields(zone).map((key, index) => <div className="va-pivot-field" draggable={!busy} key={key}
          onDragStart={(event) => event.dataTransfer.setData("text/plain", key)} onDragOver={(event) => event.preventDefault()}
          onDrop={(event) => { event.preventDefault(); event.stopPropagation(); move(event.dataTransfer.getData("text/plain"), zone, index); }}>
          <span>{isDimension(key) ? PIVOT_DIMENSIONS[key] : isMeasure(key) ? PIVOT_MEASURES[key] : key}</span>
          <button type="button" aria-label={`Move ${key} up`} disabled={index === 0} onClick={() => move(key, zone, index - 1)}>↑</button>
          <button type="button" aria-label={`Move ${key} down`} disabled={index === fields(zone).length - 1} onClick={() => move(key, zone, index + 1)}>↓</button>
          <button type="button" aria-label={`Remove ${key}`} disabled={zone === "measures" && config.measures.length === 1} onClick={() => remove(key, zone)}>×</button>
          {zone === "filters" && isDimension(key) && <CommandMultiSelect selected={config.filters[key] ?? []} placeholder="All / 不限" options={[...new Set(basis.map((v) => dimensionValue(v, key)))].sort().map((text) => ({ value: text, label: label(key, text) }))}
            onChange={(values) => setConfig((previous) => ({ ...previous, filters: { ...previous.filters, [key]: values } }))} />}
        </div>)}
        <select aria-label={`Add to ${zone}`} value="" onChange={(event) => { if (event.target.value) move(event.target.value, zone); }}><option value="">Add field / 添加字段…</option>
          {Object.entries(zone === "measures" ? PIVOT_MEASURES : PIVOT_DIMENSIONS).filter(([key]) => !fields(zone).includes(key)).map(([key, text]) => <option key={key} value={key}>{text}</option>)}</select>
      </fieldset>)}</div></div>}
    <p role="status">{source.length} vehicles / 台 · {groups.size} row groups / 行分组 · Click a value to inspect source vehicles / 点击数字查看来源车辆{missing ? ` · ${missing} missing FOB / 缺成交价，未按零价补齐；合计仅含已知金额` : ""}</p>
    {oversized ? <p role="alert">Too many row/column combinations. Narrow filters or dimensions / 行列组合过多，请收窄筛选或维度；未截断数据。</p> : <div className="va-pivot-scroll"><table className="data-table va-pivot-table"><thead>
      {config.columns.map((dimension, level) => { const headers: Array<{ text: string; span: number }> = []; let previous = "";
        for (const labels of columnLabels) { const prefix = JSON.stringify(labels.slice(0, level + 1)); if (prefix === previous) headers[headers.length - 1].span += config.measures.length; else headers.push({ text: label(dimension, labels[level]), span: config.measures.length }); previous = prefix; }
        return <tr key={dimension}>{level === 0 && <th rowSpan={config.columns.length + 1}>{config.rows.map((key) => PIVOT_DIMENSIONS[key]).join(" → ") || "All vehicles / 全部车辆"}</th>}
          {headers.map((header, index) => <th colSpan={header.span} key={index}>{header.text}</th>)}{level === 0 && <th rowSpan={config.columns.length} colSpan={config.measures.length}>Total / 合计</th>}</tr>; })}
      <tr>{!config.columns.length && <th>{config.rows.map((key) => PIVOT_DIMENSIONS[key]).join(" → ") || "All vehicles / 全部车辆"}</th>}
        {colKeys.flatMap((key) => config.measures.map((measure) => <th key={`${key}:${measure}`}>{PIVOT_MEASURES[measure]}</th>))}{config.measures.map((measure) => <th key={measure}>Total · {PIVOT_MEASURES[measure]}</th>)}</tr>
      </thead><tbody>{orderedNodes.filter(([, node]) => !node.labels.some((_, index) => index > 0 && closed.has(JSON.stringify(node.labels.slice(0, index))))).map(([key, node]) => {
        const colors = new Set(node.items.map((v) => orderGeniusPowertrainColor(v.powertrain))), branch = node.labels.length < config.rows.length;
        return <tr key={key} style={{ "--va-pivot-accent": colors.size === 1 ? [...colors][0] : "#94a3b8" } as CSSProperties}>
          <th scope="row" style={{ paddingLeft: 12 + Math.max(0, node.labels.length - 1) * 18 }}>{branch && <button className="va-pivot-value" type="button" aria-label={`Toggle ${node.labels.join(" / ")}`} aria-expanded={!closed.has(key)} onClick={() => setClosed((previous) => { const next = new Set(previous); if (next.has(key)) next.delete(key); else next.add(key); return next; })}>{closed.has(key) ? "+" : "−"}</button>}{node.labels.length ? label(config.rows[node.labels.length - 1], node.labels[node.labels.length - 1]) : "All vehicles / 全部车辆"}</th>
          {colKeys.flatMap((column) => { const items = node.items.filter((v) => JSON.stringify(config.columns.map((dimension) => dimensionValue(v, dimension))) === column); return config.measures.map((measure) => <td key={`${column}:${measure}`}>{items.length ? <button className="va-pivot-value" type="button" onClick={() => onDrill(items.map((v) => v.carCode))}>{value(items, measure)}</button> : "—"}</td>); })}
          {config.measures.map((measure) => <td key={measure} className="va-pivot-total"><button className="va-pivot-value" type="button" onClick={() => onDrill(node.items.map((v) => v.carCode))}>{value(node.items, measure)}</button></td>)}</tr>;
      })}<tr className="va-pivot-total"><th>Total / 合计</th>{colKeys.flatMap((column) => config.measures.map((measure) => <td key={`${column}:${measure}`}>{value(source.filter((v) => JSON.stringify(config.columns.map((key) => dimensionValue(v, key))) === column), measure)}</td>))}{config.measures.map((measure) => <td key={measure}>{value(source, measure)}</td>)}</tr></tbody></table></div>}
  </section>;
}
