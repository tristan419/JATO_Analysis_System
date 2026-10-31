import { useEffect, useState, type CSSProperties } from "react";
import { Link } from "react-router-dom";
import { api } from "../../api/client";
import type { MaterialSkuMatrixRow, MaterialTimelineNode, MaterialTimelineResponse } from "../../types/orderGenius";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";
import { MaterialNotices } from "./MaterialNotices";
import { WvtaReferenceUpload } from "./WvtaReferenceUpload";
import { useAssetLanguage } from "./materialAssetUi";
import "./MaterialWorkspace.css";

interface Props {
  countries: string[]; noticeCountries?: string[]; authorizationKey: string;
  selectionRows: Array<MaterialSkuMatrixRow & { _countryCode?: string }>; selectionComplete: boolean;
  bomEditing?: boolean; canManageReference?: boolean; canMaintainRelations?: boolean;
  onViewModel: (brand: string, model: string, country?: string) => void;
}
interface TimelineRow { id: string; node: MaterialTimelineNode; countries: string[]; target?: string }
export const monthIndex = (iso: string): number => Number(iso.slice(0, 4)) * 12 + Number(iso.slice(5, 7)) - 1;
export const monthIso = (index: number): string => `${Math.floor(index / 12)}-${String(index % 12 + 1).padStart(2, "0")}`;
const day = (value: string): number => Date.parse(`${value}T00:00:00Z`);
export function timelineBar(node: MaterialTimelineNode, from: number, to: number): CSSProperties | null {
  if (node.issues.length || !node.effectiveFrom) return null;
  const left = day(`${monthIso(from)}-01`), right = day(`${monthIso(to + 1)}-01`);
  const start = Math.max(left, day(node.effectiveFrom));
  const end = Math.min(right, node.effectiveTo ? day(node.effectiveTo) + 86400000 : right);
  return end <= start ? null : { left: `${100 * (start - left) / (right - left)}%`, width: `${100 * (end - start) / (right - left)}%` };
}
function assetHref(nodes: MaterialTimelineNode[], compare = false): string {
  return `/product/order-genius/material-assets?${new URLSearchParams({ materials: JSON.stringify(nodes.map(n => [n.brand, n.modelName, n.bomTemplate])), ...(compare ? { compare: "1" } : {}) })}`;
}
export function MaterialTimelineBoard({ countries, noticeCountries = countries, authorizationKey, selectionRows, selectionComplete, bomEditing = false, canManageReference = false }: Props) {
  const { language, setLanguage, t } = useAssetLanguage();
  const [expanded, setExpanded] = useState(true);
  const [onlySelection, setOnlySelection] = useState(false);
  const [showWvta, setShowWvta] = useState(false);
  const [revision, setRevision] = useState(0);
  const [range, setRange] = useState<{ anchor: string; from: number; to: number } | null>(null);
  const [selected, setSelected] = useState<string[]>([]);
  const [focus, setFocus] = useState<string | null>(null);
  const countryKey = [...new Set(countries)].sort().join(",");
  const scope = `${authorizationKey}|${countryKey}|${showWvta}`;
  const [load, setLoad] = useState<{ scope: string; data: MaterialTimelineResponse[]; error: boolean }>({ scope: "", data: [], error: false });
  useEffect(() => {
    let current = true; setSelected([]); setFocus(null);
    if (bomEditing) return;
    const requested = countryKey ? countryKey.split(",") : [];
    void Promise.all(requested.map(country => api.getMaterialTimeline(country, showWvta))).then(data => {
      if (!current) return;
      if (data.some((item, index) => item.countryCode !== requested[index] || item.today !== data[0]?.today || (showWvta && item.wvtaReference?.sha256 !== data[0]?.wvtaReference?.sha256))) throw new Error("Scope changed");
      setLoad({ scope, data, error: false });
    }).catch(() => { if (current) setLoad({ scope, data: [], error: true }); });
    return () => { current = false; };
  }, [scope, countryKey, showWvta, revision, bomEditing]);
  const ready = !bomEditing && load.scope === scope;
  const data = ready ? load.data : [];
  const today = data[0]?.today ?? new Date().toISOString().slice(0, 10);
  const now = monthIndex(today), minimum = now - 36, maximum = now + 36;
  const from = range?.anchor === today ? range.from : now - 3, to = range?.anchor === today ? range.to : now;
  const setWindow = (left: number, right: number): void => setRange({ anchor: today, from: left, to: right });
  const rows: TimelineRow[] = [];
  for (const response of data) {
    const visible = new Set(response.nodes.filter(n => !onlySelection || (selectionComplete && selectionRows.some(row => row._countryCode === response.countryCode && (row.bomTemplate === n.bomTemplate || n.materialCodes.includes(row.materialCode))))).map(n => n.bomTemplate));
    if (onlySelection) {
      let changed = true;
      while (changed) { changed = false; for (const link of response.links) {
        if ((visible.has(link.from) || visible.has(link.to)) && (!visible.has(link.from) || !visible.has(link.to))) { visible.add(link.from); visible.add(link.to); changed = true; }
      } }
    }
    for (const node of response.nodes.filter(n => visible.has(n.bomTemplate))) {
      const target = response.links.find(link => link.from === node.bomTemplate)?.to;
      const id = JSON.stringify([node.brand, node.modelName, node.version, node.bomTemplate, node.effectiveFrom, node.effectiveTo, node.issues, target]);
      const existing = rows.find(r => r.id === id);
      if (existing) existing.countries.push(response.countryCode); else rows.push({ id, node, countries: [response.countryCode], target });
    }
  }
  rows.sort((a, b) => a.node.brand.localeCompare(b.node.brand) || a.node.modelName.localeCompare(b.node.modelName) || a.node.version.localeCompare(b.node.version) || (a.node.effectiveFrom ?? "9999").localeCompare(b.node.effectiveFrom ?? "9999") || a.node.bomTemplate.localeCompare(b.node.bomTemplate));
  const active = rows.filter(row => selected.includes(row.id));
  const months = Array.from({ length: to - from + 1 }, (_, i) => from + i);
  const pan = (direction: number): void => { const offset = Math.min(Math.max(direction * months.length, minimum - from), maximum - to); setWindow(from + offset, to + offset); };
  return <section className="mw-board" aria-label={t("Notice Board", "物料公告")}>
    <header className="mw-toolbar"><button type="button" className="mw-heading" aria-expanded={expanded} onClick={() => setExpanded(!expanded)}>{expanded ? "▾" : "▸"} {t("Notice Board", "物料公告")}</button>
      <span className="mw-muted" role="status">{!ready ? t("Loading…", "读取中…") : load.error ? t("Read failed", "读取失败") : `${rows.length} ${t("materials", "基准物料")}`}</span>
      <span className="mw-spacer" /><Link to="/product/order-genius/material-assets" className="mw-button">{t("Material Assets", "物料资料")}</Link>
      <button type="button" aria-label={t("Reload", "重读")} onClick={() => setRevision(v => v + 1)}>↻</button>
      <button type="button" onClick={() => setLanguage(language === "en" ? "zh" : "en")}>{language === "en" ? "中文" : "EN"}</button>
    </header>
    {ready && load.error ? <p role="alert">{t("Unable to load this scope. Retry or sign in again.", "当前范围读取失败，请重试或重新登录。")}</p> : null}
    {!bomEditing ? <MaterialNotices countries={noticeCountries} authorizationKey={authorizationKey} revision={revision} language={language} onViewModel={(brand, model) => { setExpanded(true); setOnlySelection(false); setFocus(`${brand}|${model}`); }} /> : null}
    {expanded ? <>
      <div className="mw-toolbar mw-calendar-tools"><button type="button" aria-label={t("Previous window", "上一窗口")} disabled={from === minimum} onClick={() => pan(-1)}>←</button>
        <input type="month" aria-label={t("Window start", "窗口开始")} value={monthIso(from)} min={monthIso(minimum)} max={monthIso(to)} onChange={e => { if (e.target.value) setWindow(Math.max(minimum, Math.min(to, monthIndex(e.target.value))), to); }} />
        <span>—</span><input type="month" aria-label={t("Window end", "窗口结束")} value={monthIso(to)} min={monthIso(from)} max={monthIso(maximum)} onChange={e => { if (e.target.value) setWindow(from, Math.min(maximum, Math.max(from, monthIndex(e.target.value)))); }} />
        <button type="button" aria-label={t("Next window", "下一窗口")} disabled={to === maximum} onClick={() => pan(1)}>→</button><button type="button" onClick={() => setWindow(now - 3, now)}>{t("Today", "今天")}</button>
        <span className="mw-spacer" /><label><input type="checkbox" checked={onlySelection} disabled={!selectionComplete && !onlySelection} onChange={e => { setOnlySelection(e.target.checked); setFocus(null); }} /> {t("Selection", "当前选品")}</label>
        {focus ? <button type="button" onClick={() => setFocus(null)}>{t("All models", "全部车型")}</button> : null}
        {active.length >= 2 ? <Link className="mw-button mw-primary" to={assetHref(active.map(r => r.node), true)}>{t("Compare", "对比")} {active.length}</Link> : null}
      </div>
      <div className="mw-range"><input type="range" aria-label={t("Start handle", "开始滑柄")} min={minimum} max={maximum} value={from} onChange={e => setWindow(Math.min(Number(e.target.value), to), to)} /><input type="range" aria-label={t("End handle", "结束滑柄")} min={minimum} max={maximum} value={to} onChange={e => setWindow(from, Math.max(Number(e.target.value), from))} /></div>
      <div className="mw-calendar" style={{ "--mw-months": months.length } as CSSProperties}>
        <div className="mw-calendar-head"><span>{t("Model / Trim / Material", "车型 / 版本 / 基准物料")}</span><div>{months.map(m => <span key={m}>{new Date(`${monthIso(m)}-01T00:00:00Z`).toLocaleDateString(language === "en" ? "en-GB" : "zh-CN", { year: "2-digit", month: "short", timeZone: "UTC" })}</span>)}</div></div>
        {rows.filter(row => !focus || `${row.node.brand}|${row.node.modelName}` === focus).map((row, index, visible) => {
          const n = row.node, bar = timelineBar(n, from, to), previous = visible[index - 1]?.node;
          return <div key={row.id}>
            {previous?.modelName !== n.modelName || previous?.brand !== n.brand ? <div className="mw-model-line">{n.brand} · {n.modelName}</div> : null}
            {previous?.version !== n.version || previous?.modelName !== n.modelName ? <div className="mw-trim-line">{n.version}</div> : null}
            <div className="mw-calendar-row"><div className="mw-material-label"><input type="checkbox" aria-label={`${t("Compare", "对比")} ${n.bomTemplate} ${row.countries.join(",")}`} checked={selected.includes(row.id)} disabled={!selected.includes(row.id) && selected.length >= 8} onChange={e => setSelected(s => e.target.checked ? [...s, row.id] : s.filter(id => id !== row.id))} />
              <Link to={assetHref([n])}>{n.bomTemplate}</Link><small title={row.countries.join(", ")}>{row.countries.length > 1 ? `${row.countries.length} ${t("markets", "国家")}` : row.countries[0]}</small>{n.issues.length ? <span title={n.issues.join("\n")} aria-label={t("Review required", "待核对")}>⚠</span> : null}
            </div><div className="mw-lane" title={`${n.effectiveFrom ?? "?"} → ${n.effectiveTo ?? "∞"}${row.target ? ` · → ${row.target}` : ""}`}>
              {bar ? <Link to={assetHref([n])} className="mw-bar" style={{ ...bar, background: orderGeniusPowertrainColor(n.powertrain) }} aria-label={`${n.bomTemplate} ${n.effectiveFrom} → ${n.effectiveTo ?? t("Open", "无截止")}`}><span>{n.effectiveFrom} → {n.effectiveTo ?? "∞"}</span></Link> : !n.effectiveFrom || n.issues.length ? <small>{t("Dates to review", "日期待核对")}</small> : null}
            </div></div>
          </div>;
        })}
      </div>
      <details className="mw-reference"><summary>{t("WVTA reference", "WVTA参考")}</summary><label><input type="checkbox" checked={showWvta} onChange={e => setShowWvta(e.target.checked)} />{t("Show reference", "查看参考")}</label>{canManageReference ? <WvtaReferenceUpload onSaved={() => setRevision(v => v + 1)} /> : null}
        {showWvta ? <div>{data[0]?.wvtaReference?.filename ?? t("No reference", "暂无参考文件")}{data[0]?.wvtaReference?.records.map((record, i) => <p key={i}>{record.materialGroup} · {record.wvtaNo} · {record.validFrom ?? "—"} → {record.validTo ?? "—"}</p>)}</div> : null}
      </details>
    </> : null}
  </section>;
}
