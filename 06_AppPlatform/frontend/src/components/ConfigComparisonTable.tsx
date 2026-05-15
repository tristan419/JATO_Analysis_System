import React, { useState, useMemo } from "react";
import type { CompareResponse, CompareRow, AvailabilityState } from "../types/engineeringConfig";

function CellDisplay({ row, colIndex }: { row: CompareRow; colIndex: number }) {
  const cell = row.values[colIndex];
  if (!cell) return <td className="compare-cell compare-cell-missing">-</td>;
  const cls = `compare-cell compare-cell-${cell.availability.toLowerCase()}`;
  if (cell.availability === "STANDARD") return <td className={cls}>● 标配</td>;
  if (cell.availability === "OPTIONAL") return <td className={cls}>○ 选装</td>;
  if (cell.availability === "NOT_AVAILABLE" || cell.availability === "NOT_APPLICABLE") return <td className={cls}>-</td>;
  if (cell.availability === "UNKNOWN") return <td className={cls}>?</td>;
  return <td className={cls}>{cell.rawValue}{cell.unit && <span className="compare-unit"> {cell.unit}</span>}</td>;
}

export function ConfigComparisonTable({ data }: { data: CompareResponse }) {
  const [search, setSearch] = useState("");
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [hlDiffs, setHlDiffs] = useState(true);

  const filtered = useMemo(() => {
    if (!search.trim()) return data.rows;
    const q = search.toLowerCase();
    return data.rows.filter((r) => r.featureName.toLowerCase().includes(q) || r.category.toLowerCase().includes(q));
  }, [data.rows, search]);

  const byCat = useMemo(() => {
    const m: Record<string, CompareRow[]> = {};
    for (const r of filtered) { if (!m[r.category]) m[r.category] = []; m[r.category].push(r); }
    return m;
  }, [filtered]);

  const uniform = (r: CompareRow) => {
    const vals = r.values.filter((v) => v !== null);
    if (vals.length < 2) return true;
    const f = vals[0]; return vals.every((v) => v.availability === f.availability && v.rawValue === f.rawValue);
  };

  return (
    <div className="comparison-container">
      <div className="comparison-toolbar">
        <input className="input" placeholder="搜索配置项..." value={search} onChange={(e) => setSearch(e.target.value)} />
        <label className="toggle-label"><input type="checkbox" checked={hlDiffs} onChange={(e) => setHlDiffs(e.target.checked)} />高亮差异</label>
        <span className="text-muted">共 {filtered.length} 项</span>
      </div>
      <div className="comparison-table-wrapper">
        <table className="comparison-table">
          <thead><tr><th>大类</th><th>配置项</th>{data.trims.map((t) => <th key={t.trimId}>{t.fullTrimName}</th>)}</tr></thead>
          <tbody>
            {Object.entries(byCat).map(([cat, rows]) => {
              const c = collapsed.has(cat);
              return <React.Fragment key={cat}>
                <tr className="compare-category-row" onClick={() => setCollapsed((p) => { const n = new Set(p); n.has(cat) ? n.delete(cat) : n.add(cat); return n; })}>
                  <td colSpan={2 + data.trims.length}>{c ? "▶" : "▼"} {cat} ({rows.length})</td>
                </tr>
                {!c && rows.map((r) => <tr key={r.featureCode} className={hlDiffs && !uniform(r) ? "compare-row-diff" : ""}>{[<td key="cat">{r.category}</td>, <td key="feat">{r.featureName}</td>, ...data.trims.map((_, i) => <CellDisplay key={i} row={r} colIndex={i} />)]}</tr>)}
              </React.Fragment>;
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
