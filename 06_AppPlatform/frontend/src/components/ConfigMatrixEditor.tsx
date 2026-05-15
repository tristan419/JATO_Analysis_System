import React, { useState, useEffect, useCallback, useRef, useMemo } from "react";
import { api } from "../api/client";
import type { VehicleTrimItem, TrimDetail, AvailabilityState } from "../types/engineeringConfig";

const AVAIL_CLASS: Record<AvailabilityState, string> = { STANDARD: "cell-standard", OPTIONAL: "cell-optional", NOT_AVAILABLE: "cell-na", NOT_APPLICABLE: "cell-na", VALUE: "cell-value", UNKNOWN: "cell-unknown" };

interface EditingCell { trimId: string; featureCode: string; valueId: string | null; rawValue: string; version: number; }
interface CellData { valueId: string | null; rawValue: string; availability: AvailabilityState; version: number; }
interface FeatureRow { category: string; featureCode: string; featureName: string; featureId: string; cells: Record<string, CellData>; }

export function ConfigMatrixEditor() {
  const [trims, setTrims] = useState<VehicleTrimItem[]>([]);
  const [selIds, setSelIds] = useState<string[]>([]);
  const [rows, setRows] = useState<FeatureRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<EditingCell | null>(null);
  const [saving, setSaving] = useState<string | null>(null);
  const [flash, setFlash] = useState<string | null>(null);
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState("");
  const [hideEmpty, setHideEmpty] = useState(false);
  const inpRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setLoading(true);
    api.listEngineeringConfigTrims({ limit: 100 }).then((d) => {
      const items = (d.items || []) as unknown as VehicleTrimItem[];
      setTrims(items); if (items.length) setSelIds(items.slice(0, 3).map((t) => t.trimId));
    }).catch((e) => setError(e instanceof Error ? e.message : "加载失败")).finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    if (selIds.length === 0) return; setLoading(true);
    Promise.all(selIds.map((id) => api.getEngineeringConfigTrimDetail(id) as Promise<unknown>))
      .then((results) => {
        const details = results as TrimDetail[];
        const feats: { featureCode: string; category: string; featureName: string; featureId: string }[] = [];
        const seen = new Set<string>();
        for (const [, fvs] of Object.entries(details[0]?.featuresByCategory || {})) {
          for (const fv of fvs) { const c = fv.featureCode || ""; if (!seen.has(c)) { seen.add(c); feats.push({ featureCode: c, category: "", featureName: fv.featureName, featureId: fv.valueId || "" }); } }
        }
        for (const [cat, fvs] of Object.entries(details[0]?.featuresByCategory || {})) {
          for (const fv of fvs) { const idx = feats.findIndex((f) => f.featureCode === fv.featureCode); if (idx >= 0) feats[idx].category = cat; }
        }
        const built: FeatureRow[] = feats.map((f) => {
          const cells: Record<string, CellData> = {};
          for (const td of details) {
            const fvs = td.featuresByCategory[f.category] || []; const found = fvs.find((fv) => fv.featureCode === f.featureCode);
            cells[td.trim.trimId] = found ? { valueId: found.valueId, rawValue: found.rawValue, availability: found.availability, version: found.version } : { valueId: null, rawValue: "", availability: "UNKNOWN" as AvailabilityState, version: 0 };
          }
          return { ...f, cells };
        });
        setRows(built);
      }).catch((e) => setError(e instanceof Error ? e.message : "加载失败")).finally(() => setLoading(false));
  }, [selIds]);

  const toggleTrim = (id: string) => setSelIds((p) => p.includes(id) ? p.filter((x) => x !== id) : p.length < 5 ? [...p, id] : p);

  const startEdit = useCallback((trimId: string, fc: string) => {
    const row = rows.find((r) => r.featureCode === fc); const cell = row?.cells[trimId];
    setEditing({ trimId, featureCode: fc, valueId: cell?.valueId || null, rawValue: cell?.rawValue || "", version: cell?.version || 0 });
    setTimeout(() => inpRef.current?.focus(), 50);
  }, [rows]);

  const saveEdit = useCallback(async () => {
    if (!editing) return; const { trimId, featureCode, valueId, rawValue, version } = editing; const key = `${trimId}:${featureCode}`; setSaving(key);
    try {
      let result: { valueId: string; availability: string };
      if (valueId) {
        result = await api.updateEngineeringConfigFeatureValue(valueId, { raw_value: rawValue, expected_version: version, updated_by: localStorage.getItem("jato_user_name") || "editor" }) as unknown as { valueId: string; availability: string };
      } else {
        const row = rows.find((r) => r.featureCode === featureCode);
        result = await api.createEngineeringConfigFeatureValue({ trim_id: trimId, feature_id: row?.featureId || "", raw_value: rawValue, updated_by: localStorage.getItem("jato_user_name") || "editor" }) as unknown as { valueId: string; availability: string };
      }
      setRows((p) => p.map((r) => { if (r.featureCode !== featureCode) return r; const c = r.cells[trimId]; if (!c) return r; return { ...r, cells: { ...r.cells, [trimId]: { ...c, valueId: result.valueId || c.valueId, rawValue: rawValue, availability: result.availability as AvailabilityState, version: version + 1 } } }; }));
      setFlash(key); setTimeout(() => setFlash(null), 1200); setEditing(null);
    } catch (err) { setError(err instanceof Error ? err.message : "保存失败"); } finally { setSaving(null); }
  }, [editing, rows]);

  const filtered = useMemo(() => {
    let r = rows;
    if (search.trim()) { const q = search.toLowerCase(); r = r.filter((x) => x.featureName.toLowerCase().includes(q) || x.featureCode.toLowerCase().includes(q) || x.category.toLowerCase().includes(q)); }
    if (hideEmpty) r = r.filter((x) => Object.values(x.cells).some((c) => c.availability !== "NOT_AVAILABLE" && c.availability !== "UNKNOWN"));
    return r;
  }, [rows, search, hideEmpty]);

  const groups = useMemo(() => {
    const g: { category: string; rows: FeatureRow[] }[] = [];
    for (const row of filtered) { const last = g[g.length - 1]; if (last && last.category === row.category) last.rows.push(row); else g.push({ category: row.category, rows: [row] }); }
    return g;
  }, [filtered]);

  const selTrims = trims.filter((t) => selIds.includes(t.trimId));

  return (
    <div className="matrix-editor">
      <div className="matrix-toolbar">
        <div className="matrix-trim-selector">
          {trims.map((t) => <label key={t.trimId} className="matrix-trim-chip"><input type="checkbox" checked={selIds.includes(t.trimId)} onChange={() => toggleTrim(t.trimId)} /><span>{t.fullTrimName}</span></label>)}
        </div>
        <div className="matrix-toolbar-actions">
          <input className="input input-sm" placeholder="搜索配置项..." value={search} onChange={(e) => setSearch(e.target.value)} />
          <label className="toggle-label"><input type="checkbox" checked={hideEmpty} onChange={(e) => setHideEmpty(e.target.checked)} />隐藏空配置</label>
        </div>
      </div>
      {error && <div className="alert alert-error">{error}<button onClick={() => setError(null)} style={{ marginLeft: 8 }}>×</button></div>}
      <div className="matrix-table-wrapper">
        <table className="matrix-table">
          <thead><tr><th>大类</th><th>配置项</th>{selTrims.map((t) => <th key={t.trimId}>{t.fullTrimName}</th>)}</tr></thead>
          <tbody>
            {groups.map((g) => {
              const c = collapsed.has(g.category);
              return <React.Fragment key={g.category}>
                <tr className="matrix-cat-row" onClick={() => setCollapsed((p) => { const n = new Set(p); n.has(g.category) ? n.delete(g.category) : n.add(g.category); return n; })}><td colSpan={2 + selTrims.length}>{c ? "▶" : "▼"} {g.category} ({g.rows.length})</td></tr>
                {!c && g.rows.map((r) => <tr key={r.featureCode} className="matrix-data-row"><td>{r.category}</td><td>{r.featureName}</td>
                  {selTrims.map((t) => {
                    const cell = r.cells[t.trimId]; const key = `${t.trimId}:${r.featureCode}`; const isEdit = editing?.trimId === t.trimId && editing?.featureCode === r.featureCode;
                    if (isEdit) return <td key={t.trimId} className="matrix-cell editing"><input ref={inpRef} className="input input-sm edit-input" value={editing?.rawValue || ""} onChange={(e) => setEditing((p) => p ? { ...p, rawValue: e.target.value } : null)} onKeyDown={(e) => { if (e.key === "Enter") saveEdit(); if (e.key === "Escape") setEditing(null); }} autoFocus /><button className="btn btn-sm btn-primary" onClick={saveEdit} disabled={saving === key}>{saving === key ? "..." : "✓"}</button></td>;
                    return <td key={t.trimId} className={`matrix-cell ${AVAIL_CLASS[cell?.availability || "UNKNOWN"]} ${flash === key ? "cell-flash-saved" : ""}`} onClick={() => startEdit(t.trimId, r.featureCode)} title={cell?.rawValue || "点击编辑"}>
                      {cell?.availability === "STANDARD" && "● 标配"}{cell?.availability === "OPTIONAL" && "○ 选装"}{cell?.availability === "NOT_AVAILABLE" && "-"}{cell?.availability === "NOT_APPLICABLE" && "N/A"}{cell?.availability === "UNKNOWN" && "?"}{cell?.availability === "VALUE" && cell.rawValue}
                    </td>;
                  })}
                </tr>)}
              </React.Fragment>;
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
