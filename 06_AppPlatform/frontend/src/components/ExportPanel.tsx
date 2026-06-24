import { useEffect, useMemo, useState } from "react";

import { DebouncedNumberInput } from "./deckControls";
import {
  DEFAULT_LABEL_MODES,
  LABEL_MODE_LABELS,
  LABEL_OVERLAP_STRATEGIES,
  LABEL_POSITIONS,
  PALETTES,
  TICK_FORMATS,
  collectExportSeriesNamesFromGraphDiv,
  downloadPng,
  getExportPalette,
  getSeriesDefaultColor,
  normalizeExportLabelStrategy,
  resolveLabelFontSize,
  type ExportLabelMode,
  type ExportLabelOverlapStrategy,
  type ExportSettings,
} from "./ExportPanelHelpers";

export * from "./ExportPanelHelpers";

interface Props {
  value: ExportSettings;
  onChange: (s: ExportSettings) => void;
  graphDiv?: HTMLElement | null;
  seriesNames?: string[];
  labelModeOptions?: ExportLabelMode[];
  showExportButton?: boolean;
  showDimensionControls?: boolean;
  collapsible?: boolean;
  defaultOpen?: boolean;
}

export function ExportPanel({
  value: s,
  onChange,
  graphDiv,
  seriesNames,
  labelModeOptions,
  showExportButton = true,
  showDimensionControls = true,
  collapsible = true,
  defaultOpen = false,
}: Props) {
  const [open, setOpen] = useState(defaultOpen);
  const set = <K extends keyof ExportSettings>(k: K, v: ExportSettings[K]) => onChange({ ...s, [k]: v });
  const resolvedLabelModes = labelModeOptions && labelModeOptions.length > 0 ? labelModeOptions : DEFAULT_LABEL_MODES;
  const safeLabelMode = resolvedLabelModes.includes(s.dataLabelMode) ? s.dataLabelMode : "off";
  const safeLabelStrategy = normalizeExportLabelStrategy(s.dataLabelOverlapStrategy);
  const resolvedSeriesNames = useMemo(
    () => seriesNames ?? collectExportSeriesNamesFromGraphDiv(graphDiv),
    [graphDiv, seriesNames],
  );
  const bodyOpen = !collapsible || open;

  useEffect(() => {
    if (safeLabelMode !== s.dataLabelMode) {
      onChange({ ...s, dataLabelMode: safeLabelMode });
    }
  }, [onChange, s, safeLabelMode]);

  return (
    <div className={`export-panel${collapsible ? "" : " export-panel--static"}`}>
      {collapsible ? (
        <button type="button" className="btn btn-sm btn-secondary" onClick={() => setOpen(!open)}>
          {open ? "▾ 收起导出设置" : "▸ 导出图设置"}
        </button>
      ) : null}
      {bodyOpen ? (
        <div className="export-panel-body">
          <div className="export-row">
            <label><input type="checkbox" checked={s.showXGrid} onChange={e => set("showXGrid", e.target.checked)} /> X网格线</label>
            <label><input type="checkbox" checked={s.showYGrid} onChange={e => set("showYGrid", e.target.checked)} /> Y网格线</label>
            <label><input type="checkbox" checked={s.showAxisLine} onChange={e => set("showAxisLine", e.target.checked)} /> 坐标轴线</label>
            <label><input type="checkbox" checked={s.showLegend} onChange={e => set("showLegend", e.target.checked)} /> 图例</label>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>图例位置</label>
              <select value={s.legendPosition} onChange={e => set("legendPosition", e.target.value as ExportSettings["legendPosition"])}>
                <option value="right">右侧</option><option value="top">顶部</option>
                <option value="bottom">底部</option><option value="left">左侧</option>
              </select>
            </div>
            <div className="filter-group"><label>配色</label>
              <select value={s.colorScheme} onChange={e => set("colorScheme", e.target.value)}>
                {Object.keys(PALETTES).map(k => <option key={k} value={k}>{k}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>字号</label>
              <DebouncedNumberInput
                value={s.fontSize}
                min={8}
                max={24}
                style={{ width: 50 }}
                onCommit={(value) => {
                  if (value !== null) set("fontSize", value);
                }}
              />
            </div>
            <div className="filter-group"><label>标签字号</label>
              <DebouncedNumberInput
                value={resolveLabelFontSize(s)}
                min={7}
                max={28}
                style={{ width: 50 }}
                onCommit={(value) => {
                  if (value !== null) set("labelFontSize", value);
                }}
              />
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>X轴格式</label>
              <select value={s.xTickFormat} onChange={e => set("xTickFormat", e.target.value)}>
                {TICK_FORMATS.map(f => <option key={f.v} value={f.v}>{f.l}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>Y轴格式</label>
              <select value={s.yTickFormat} onChange={e => set("yTickFormat", e.target.value)}>
                {TICK_FORMATS.map(f => <option key={f.v} value={f.v}>{f.l}</option>)}
              </select>
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>背景色</label>
              <input type="color" value={s.paperBg} onChange={e => set("paperBg", e.target.value)} />
            </div>
            <div className="filter-group"><label>绘图区背景</label>
              <input type="color" value={s.plotBg} onChange={e => set("plotBg", e.target.value)} />
            </div>
            <div className="filter-group"><label>网格色</label>
              <input type="color" value={s.gridColor} onChange={e => set("gridColor", e.target.value)} />
            </div>
            <div className="filter-group"><label>轴线色</label>
              <input type="color" value={s.axisColor} onChange={e => set("axisColor", e.target.value)} />
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>标题</label>
              <input type="text" value={s.chartTitle} placeholder="图表标题" style={{ width: 140 }}
                onChange={e => set("chartTitle", e.target.value)} />
            </div>
            <div className="filter-group"><label>X轴标题</label>
              <input type="text" value={s.xTitle} placeholder="X轴" style={{ width: 100 }}
                onChange={e => set("xTitle", e.target.value)} />
            </div>
            <div className="filter-group"><label>Y轴标题</label>
              <input type="text" value={s.yTitle} placeholder="Y轴" style={{ width: 100 }}
                onChange={e => set("yTitle", e.target.value)} />
            </div>
          </div>
          {showDimensionControls ? (
            <div className="export-row">
              <div className="filter-group"><label>导出宽度</label>
                <DebouncedNumberInput
                  value={s.exportWidth}
                  min={400}
                  max={2400}
                  step={100}
                  style={{ width: 70 }}
                  onCommit={(value) => {
                    if (value !== null) set("exportWidth", value);
                  }}
                />
              </div>
              <div className="filter-group"><label>导出高度</label>
                <DebouncedNumberInput
                  value={s.exportHeight}
                  min={300}
                  max={1800}
                  step={100}
                  style={{ width: 70 }}
                  onCommit={(value) => {
                    if (value !== null) set("exportHeight", value);
                  }}
                />
              </div>
            </div>
          ) : null}
          <div className="export-row">
            <div className="filter-group"><label>数据标签</label>
              <select value={safeLabelMode} onChange={e => set("dataLabelMode", e.target.value as ExportLabelMode)}>
                {resolvedLabelModes.map(m => <option key={m} value={m}>{LABEL_MODE_LABELS[m] ?? m}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>标签位置</label>
              <select value={s.dataLabelPosition} onChange={e => set("dataLabelPosition", e.target.value)}>
                {LABEL_POSITIONS.map(p => <option key={p} value={p}>{p === "auto" ? "自动" : p}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>标签策略</label>
              <select
                value={safeLabelStrategy}
                onChange={e => set("dataLabelOverlapStrategy", e.target.value as ExportLabelOverlapStrategy)}
              >
                {LABEL_OVERLAP_STRATEGIES.map(item => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </div>
            <div className="filter-group"><label>小数位</label>
              <DebouncedNumberInput
                value={s.decimalPlaces}
                min={0}
                max={4}
                style={{ width: 50 }}
                onCommit={(value) => {
                  if (value !== null) set("decimalPlaces", value);
                }}
              />
            </div>
          </div>
          {resolvedSeriesNames.length > 0 && resolvedSeriesNames.length <= 30 && (
            <div className="export-row" style={{flexWrap:"wrap",gap:4}}>
              <span style={{width:"100%",fontSize:12,color:"var(--c-text-muted)"}}>逐系列配色</span>
              {resolvedSeriesNames.map((name, i) => (
                <label key={name} style={{display:"inline-flex",alignItems:"center",gap:2,fontSize:11}}>
                  <input type="color"
                    value={s.seriesColors[name] ?? getSeriesDefaultColor(name, getExportPalette(s.colorScheme)[i % getExportPalette(s.colorScheme).length])}
                    onChange={e => set("seriesColors", { ...s.seriesColors, [name]: e.target.value })}
                    style={{width:20,height:20,padding:0,border:"none"}} />
                  {name}
                </label>
              ))}
            </div>
          )}
          {showExportButton ? (
            <div className="export-row">
              <button className="btn btn-primary" onClick={() => { void downloadPng(graphDiv ?? null, s); }}
                disabled={!graphDiv}>📷 导出 PNG</button>
            </div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
