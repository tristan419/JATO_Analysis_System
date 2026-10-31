import { useEffect, useState } from "react";
import { materialAssets, saveAssetBlob } from "../../api/materialAssets";
import type { AssetCatalog, ConfigComparison, MaterialIdentity } from "../../types/materialAssets";
import { OrderGeniusWorkspacePanel } from "./OrderGeniusWorkspacePanel";
import { MaterialCompareLabel, materialIdentityKey } from "./materialComparison";
import { errorMessage, useAssetLanguage, type AssetText } from "./materialAssetUi";

export function MaterialConfigurationComparison({ comparison, catalog, t }: { comparison: ConfigComparison; catalog: AssetCatalog; t: AssetText }) {
  const [differences, setDifferences] = useState(true), [error, setError] = useState("");
  const download = async (versionId: string, template: string): Promise<void> => {
    setError("");
    const version = catalog.versions.find(item => item.id === versionId);
    if (!version?.files.length) { setError(t("Source unavailable", "原文件不可用")); return; }
    try { saveAssetBlob(await materialAssets.download(version.files.map(file => file.id)), `${template.replaceAll("*", "_")}.zip`); }
    catch (failure) { setError(errorMessage(failure)); }
  };
  return <section className="mw-comparison">
    <header className="mw-toolbar"><h2>{t("Configuration comparison", "配置对比")}</h2><span className="mw-spacer" /><label><input type="checkbox" checked={differences} onChange={event => setDifferences(event.target.checked)} /> {t("Differences only", "仅差异")}</label></header>
    {error ? <p role="alert">{error}</p> : null}
    <div className="mw-table-scroll"><table className="mw-table"><thead><tr><th>{t("Feature", "配置项")}</th>{comparison.materials.map((item, index) => {
      const material = catalog.materials.find(candidate => candidate.key === item.key);
      return <th key={item.key}><MaterialCompareLabel index={index}>{item.bomTemplate}</MaterialCompareLabel><small>{item.modelName} · {material?.version} · v{item.version}</small>
        <small>{material?.effectiveFrom ?? "—"} → {material?.effectiveTo ?? "∞"}</small>
        <button type="button" className="mw-link" title={item.versionId} onClick={() => void download(item.versionId, item.bomTemplate)}>{t("Source", "原文件")} ↗</button></th>;
    })}</tr></thead><tbody>{comparison.rows.filter(row => !differences || new Set(row.values.map(value => JSON.stringify(value ? [value.raw_value, value.normalized_value, value.availability] : null))).size > 1).map(row => <tr key={JSON.stringify([row.category, row.featureName])}>
      <th><small>{row.category}</small>{row.featureName}</th>{row.values.map((value, index) => <td key={comparison.materials[index].key} title={value ? `${comparison.materials[index].versionId}\n${value.source_column}${value.source_row}` : t("Not recorded in this source; not equivalent to unavailable", "该源文件未记录，不等于未配备")}>{value?.raw_value ?? "—"}</td>)}
    </tr>)}</tbody></table></div>
  </section>;
}

export function MaterialComparisonPanel({ materials, onClose }: { materials: MaterialIdentity[]; onClose: () => void }) {
  const { t } = useAssetLanguage();
  const [result, setResult] = useState<{ catalog: AssetCatalog; comparison: ConfigComparison } | null>(null), [error, setError] = useState("");
  useEffect(() => {
    const abort = new AbortController(); setError(""); setResult(null);
    void (async () => {
      const catalog = await materialAssets.catalog(abort.signal);
      const keys = materials.map(item => catalog.materials.find(candidate => materialIdentityKey(candidate) === materialIdentityKey(item))?.key);
      if (keys.some(key => !key) || keys.length < 2) throw new Error(t("A selected material is unavailable. Re-select authorized materials.", "所选物料不可用，请重新选择有权访问的物料。"));
      const comparison = await materialAssets.compare(keys.filter((key): key is string => Boolean(key)), abort.signal);
      if (!abort.signal.aborted) setResult({ catalog, comparison });
    })().catch(failure => { if (!abort.signal.aborted) setError(errorMessage(failure)); });
    return () => abort.abort();
  }, [materials]);
  return <OrderGeniusWorkspacePanel label={t("Configuration comparison", "配置对比")} onClose={onClose}><section className="mw-workspace is-embedded">
    <header className="mw-page-header"><h1>{t("Compare materials", "物料对比")}</h1><span className="mw-spacer" /><button type="button" onClick={onClose}>{t("Close", "关闭")}</button></header>
    {error ? <p role="alert">{error}</p> : result ? <MaterialConfigurationComparison comparison={result.comparison} catalog={result.catalog} t={t} /> : <p role="status">{t("Loading…", "读取中…")}</p>}
  </section></OrderGeniusWorkspacePanel>;
}
