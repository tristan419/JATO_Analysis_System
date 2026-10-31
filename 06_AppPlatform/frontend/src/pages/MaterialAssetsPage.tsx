import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import { materialAssets, saveAssetBlob } from "../api/materialAssets";
import type { AssetCatalog, AssetMaterial, ConfigComparison } from "../types/materialAssets";
import { MaterialAssetInspector } from "../components/orderGenius/MaterialAssetInspector";
import { errorMessage, useAssetLanguage } from "../components/orderGenius/materialAssetUi";
import { orderGeniusPowertrainColor } from "../utils/orderGeniusBomAdmin";
import "../components/orderGenius/MaterialWorkspace.css";

function requestedMaterials(value: string | null): string[][] {
  if (!value) return [];
  try {
    const parsed: unknown = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.filter((item): item is string[] => Array.isArray(item) && item.length === 3 && item.every(part => typeof part === "string")) : [];
  } catch { return []; }
}
export function MaterialAssetsPage() {
  const { user, token, profileLoaded } = useAuth();
  const scope = JSON.stringify([token, user?.username, user?.role, user?.primaryCountry, user?.secondaryCountries, user?.brands]);
  if (!profileLoaded || !user) return <div role="status">Loading…</div>;
  if (!["order_filler", "editor", "admin", "developer"].includes(user.role)) return <div role="alert">403 · Not authorized / 无访问权限</div>;
  return <MaterialAssetsWorkspace key={scope} scope={scope} editor={user.role !== "order_filler"} />;
}
function MaterialAssetsWorkspace({ scope, editor }: { scope: string; editor: boolean }) {
  const { language, setLanguage, t } = useAssetLanguage();
  const [params] = useSearchParams();
  const [catalog, setCatalog] = useState<AssetCatalog | null>(null), [error, setError] = useState("");
  const [selected, setSelected] = useState<string[]>([]), [focus, setFocus] = useState<string | null>(null);
  const [query, setQuery] = useState(""), [historical, setHistorical] = useState(false), [revision, setRevision] = useState(0);
  const [comparison, setComparison] = useState<ConfigComparison | null>(null), [comparing, setComparing] = useState(false), [differences, setDifferences] = useState(true);
  const reload = async (): Promise<void> => { const result = await materialAssets.catalog(); setCatalog(result); setComparison(null); };
  useEffect(() => {
    const abort = new AbortController(); setError("");
    void materialAssets.catalog(abort.signal).then(data => {
      if (abort.signal.aborted) return;
      setCatalog(data);
      const requested = requestedMaterials(params.get("materials"));
      const keys = data.materials.filter(m => requested.some(parts => parts[0] === m.brand && parts[1] === m.modelName && parts[2] === m.bomTemplate)).map(m => m.key);
      setSelected(previous => requested.length ? keys.slice(0, 8) : previous.filter(key => data.materials.some(m => m.key === key)));
      setFocus(previous => keys[0] ?? (data.materials.some(m => m.key === previous) ? previous : data.materials.find(m => !m.historical)?.key ?? data.materials[0]?.key ?? null));
      if (keys.some(key => data.materials.find(m => m.key === key)?.historical)) setHistorical(true);
      if (params.get("compare") === "1" && keys.length >= 2) {
        setComparing(true);
        void materialAssets.compare(keys.slice(0, 8), abort.signal).then(result => { if (!abort.signal.aborted) setComparison(result); })
          .catch(e => { if (!abort.signal.aborted) setError(errorMessage(e)); }).finally(() => { if (!abort.signal.aborted) setComparing(false); });
      }
    }).catch(e => { if (!abort.signal.aborted) setError(errorMessage(e)); });
    return () => abort.abort();
  }, [params, revision]);
  const material = catalog?.materials.find(m => m.key === focus);
  const visible = catalog?.materials.filter(m => `${m.brand} ${m.modelName} ${m.bomTemplate} ${m.version} ${m.powertrain ?? ""}`.toLowerCase().includes(query.toLowerCase().trim())) ?? [];
  const renderModels = (items: AssetMaterial[]) => {
    const models = new Map<string, AssetMaterial[]>();
    for (const m of items) { const key = `${m.brand} · ${m.modelName}`; models.set(key, [...(models.get(key) ?? []), m]); }
    return [...models].sort(([a], [b]) => a.localeCompare(b)).map(([model, members]) => <div className="mw-model-group" key={model}><h3>{model}</h3>{members.sort((a, b) => a.version.localeCompare(b.version) || a.bomTemplate.localeCompare(b.bomTemplate)).map(m => <div key={m.key} className={`mw-material-item${focus === m.key ? " is-current" : ""}`} style={{ borderLeftColor: orderGeniusPowertrainColor(m.powertrain) }}>
      <input type="checkbox" aria-label={`${t("Compare", "对比")} ${m.bomTemplate}`} checked={selected.includes(m.key)} disabled={!selected.includes(m.key) && selected.length >= 8} onChange={e => { setSelected(keys => e.target.checked ? [...keys, m.key] : keys.filter(key => key !== m.key)); setComparison(null); }} />
      <button type="button" className="mw-link" onClick={() => { setFocus(m.key); setComparison(null); }}><strong>{m.bomTemplate}</strong><small>{m.version} · {m.powertrain}</small></button>
      <span title={t("Configuration / exterior / interior", "配置 / 外饰 / 内饰")} className="mw-asset-counts">{catalog?.bindings.some(b => b.materialKey === m.key && b.kind === "config") ? "▤" : "—"} · {catalog?.bindings.filter(b => b.materialKey === m.key && b.kind !== "config").length}</span>
    </div>)}</div>);
  };
  const compare = async (): Promise<void> => {
    setComparing(true); setError(""); setComparison(null);
    try { setComparison(await materialAssets.compare(selected)); } catch (e) { setError(errorMessage(e)); } finally { setComparing(false); }
  };
  return <main className="mw-workspace">
    <header className="mw-page-header"><div><span className="mw-eyebrow">ORDER GENIUS</span><h1>{t("Material Assets", "物料资料")}</h1></div><span className="mw-spacer" /><button type="button" onClick={() => setRevision(v => v + 1)}>{t("Reload", "重读")}</button><button type="button" onClick={() => setLanguage(language === "en" ? "zh" : "en")}>{language === "en" ? "中文" : "EN"}</button></header>
    {error ? <p role="alert">{error}</p> : null}
    {!catalog ? <div role="status">{error ? "—" : t("Loading…", "读取中…")}</div> : <div className="mw-assets-layout">
      <nav className="mw-material-list" aria-label={t("Materials", "物料")}><div className="mw-toolbar"><input type="search" aria-label={t("Find material", "搜索物料")} placeholder={t("Model, material…", "车型、物料号…")} value={query} onChange={e => setQuery(e.target.value)} /></div>
        <div className="mw-toolbar"><button type="button" className="mw-primary" disabled={selected.length < 2 || comparing} onClick={() => void compare()}>{comparing ? "…" : t("Compare", "对比")} {selected.length || ""}</button>{selected.length ? <button type="button" onClick={() => { setSelected([]); setComparison(null); }}>{t("Clear", "清空")}</button> : null}</div>
        {renderModels(visible.filter(m => !m.historical))}
        <details className="mw-historical" open={historical} onToggle={e => setHistorical(e.currentTarget.open)}><summary>{t("Historical", "历史")} <span className="mw-count">{visible.filter(m => m.historical).length}</span></summary>{renderModels(visible.filter(m => m.historical))}</details>
      </nav>
      {comparison ? <section className="mw-comparison"><header className="mw-toolbar"><h2>{t("Configuration comparison", "配置对比")}</h2><label><input type="checkbox" checked={differences} onChange={e => setDifferences(e.target.checked)} />{t("Differences only", "仅差异")}</label><span className="mw-spacer" /><button type="button" onClick={() => setComparison(null)}>{t("Close", "关闭")}</button></header>
        <div className="mw-table-scroll"><table className="mw-table"><thead><tr><th>{t("Feature", "配置项")}</th>{comparison.materials.map(m => <th key={m.key}><strong>{m.bomTemplate}</strong><small>{m.modelName} · v{m.version}</small><button type="button" className="mw-link" title={m.versionId} onClick={() => { const version = catalog.versions.find(v => v.id === m.versionId); if (version) void materialAssets.download(version.files.map(f => f.id)).then(blob => saveAssetBlob(blob, `${m.bomTemplate.replaceAll("*", "_")}.zip`)).catch(e => setError(errorMessage(e))); }}>{t("Source", "原文件")} ↗</button></th>)}</tr></thead><tbody>
          {comparison.rows.filter(row => !differences || new Set(row.values.map(v => JSON.stringify(v ? [v.raw_value, v.normalized_value, v.availability] : null))).size > 1).map(row => <tr key={`${row.category}|${row.featureName}`}><th><small>{row.category}</small>{row.featureName}</th>{row.values.map((value, i) => <td key={i} title={value ? `${comparison.materials[i].versionId}\n${value.source_column}${value.source_row}` : t("Not recorded in this source; not equivalent to unavailable", "该源文件未记录，不等于未配备")}>{value?.raw_value ?? "—"}<small>{value ? `${value.source_column}${value.source_row}` : ""}</small></td>)}</tr>)}
        </tbody></table></div>
      </section> : material ? <MaterialAssetInspector key={material.key} material={material} catalog={catalog} editor={editor} t={t} scope={scope} onReload={reload} /> : <div className="mw-empty">{t("No authorized materials", "暂无获授权物料")}</div>}
    </div>}
  </main>;
}
