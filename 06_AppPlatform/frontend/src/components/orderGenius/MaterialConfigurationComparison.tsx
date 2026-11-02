import { Fragment, useEffect, useRef, useState } from "react";
import { materialAssets, saveAssetBlob } from "../../api/materialAssets";
import type { AssetCatalog, ConfigCell, ConfigComparison, MaterialIdentity } from "../../types/materialAssets";
import { OrderGeniusWorkspacePanel } from "./OrderGeniusWorkspacePanel";
import { comparisonStyle, MaterialCompareLabel, materialIdentityKey } from "./materialComparison";
import { configurationValue, errorMessage, useAssetLanguage, type AssetText } from "./materialAssetUi";

const signature = (cell: ConfigCell | null): string => JSON.stringify(cell ? [cell.raw_value, cell.normalized_value, cell.availability, cell.unit ?? null] : null);

export function MaterialConfigurationComparison({ comparison, catalog, t }: { comparison: ConfigComparison; catalog: AssetCatalog; t: AssetText }) {
  const [differences, setDifferences] = useState(true), [error, setError] = useState("");
  const [baseline, setBaseline] = useState(0), [common, setCommon] = useState(false);
  const [collapsed, setCollapsed] = useState<string[]>([]), [source, setSource] = useState<string | null>(null);
  const sourceButton = useRef<HTMLButtonElement | null>(null);
  const comparedMaterials = comparison.materials.map(item => catalog.materials.find(candidate => candidate.key === item.key));
  const materialRows = (["interiors", "colours"] as const).map(kind => ({
    key: kind, label: kind === "interiors" ? t("Interior", "内饰") : t("Exterior colours", "外饰颜色"),
    values: comparedMaterials.map(material => material?.[kind].map(colour => [colour.code, colour.name].filter(Boolean).join(" · ")).sort().join(", ") || "—"),
  }));
  const rows = comparison.rows.map(row => ({ ...row, key: JSON.stringify([row.category, row.featureName]), different: new Set(row.values.map(signature)).size > 1 }));
  const commonCount = rows.filter(row => !row.different).length + materialRows.filter(row => new Set(row.values).size === 1).length;
  const showCommon = !differences || common;
  const categories = [...new Set(rows.filter(row => row.different || showCommon).map(row => row.category))];
  const visibleBom = materialRows.filter(row => showCommon || new Set(row.values).size > 1);
  const closeSource = (): void => { setSource(null); sourceButton.current?.focus(); };
  const download = async (versionId: string, template: string): Promise<void> => {
    setError("");
    const version = catalog.versions.find(item => item.id === versionId);
    if (!version?.files.length) { setError(t("Source unavailable", "原文件不可用")); return; }
    try { saveAssetBlob(await materialAssets.download(version.files.map(file => file.id)), `${template.replaceAll("*", "_")}.zip`); }
    catch (failure) { setError(errorMessage(failure)); }
  };
  return <section className="mw-comparison">
    <header className="mw-toolbar"><h2>{t("Configuration comparison", "配置对比")}</h2><span className="mw-spacer" /><label><input type="checkbox" checked={differences} onChange={event => setDifferences(event.target.checked)} /> {t("Differences only", "仅差异")}</label>
      {differences && commonCount ? <button type="button" aria-expanded={common} onClick={() => setCommon(!common)}>{common ? "▾" : "▸"} {t("Common items", "相同项")} {commonCount}</button> : null}</header>
    {error ? <p role="alert">{error}</p> : null}
    <div className="mw-table-scroll mw-compare-scroll"><table className="mw-table mw-compare-table"><thead><tr><th scope="col">{t("Feature", "配置项")}</th>{comparison.materials.map((item, index) => {
      const material = catalog.materials.find(candidate => candidate.key === item.key);
      return <th scope="col" key={item.key}><article className={`mw-compare-card${baseline === index ? " is-baseline" : ""}`} style={comparisonStyle(index)}><MaterialCompareLabel index={index}>{item.bomTemplate}</MaterialCompareLabel><strong>{item.modelName}</strong><small>{material?.version} · v{item.version}</small>
        <small>{material?.effectiveFrom ?? "—"} → {material?.effectiveTo ?? "∞"}</small>
        <div className="mw-toolbar"><button type="button" aria-pressed={baseline === index} aria-label={`${t("Set baseline", "设为基准")} ${item.bomTemplate}`} onClick={() => setBaseline(index)}>{baseline === index ? t("Baseline", "基准") : t("Set baseline", "设为基准")}</button><button type="button" className="mw-link" title={item.versionId} onClick={() => void download(item.versionId, item.bomTemplate)}>{t("Source", "原文件")} ↗</button></div></article></th>;
    })}</tr></thead>{categories.map(category => <tbody key={category}><tr className="mw-compare-category"><th colSpan={comparison.materials.length + 1}><button type="button" aria-expanded={!collapsed.includes(category)} onClick={() => setCollapsed(current => current.includes(category) ? current.filter(value => value !== category) : [...current, category])}>{collapsed.includes(category) ? "▸" : "▾"} {category}</button></th></tr>
      {!collapsed.includes(category) ? rows.filter(row => row.category === category && (row.different || showCommon)).map(row => <Fragment key={row.key}><tr className={!row.different ? "is-common" : ""}><th scope="row">{row.featureName}</th>{row.values.map((value, index) => {
        const id = JSON.stringify([row.key, index]);
        return <td key={comparison.materials[index].key} className={signature(value) !== signature(row.values[baseline]) ? "is-different" : ""}><button type="button" className="mw-config-value" aria-expanded={source === id} aria-label={`${row.featureName} · ${comparison.materials[index].bomTemplate} · ${t("Source", "来源")}`} title={value ? `${comparison.materials[index].versionId}\n${value.source_column}${value.source_row}` : t("Not recorded in this source; not equivalent to unavailable", "该源文件未记录，不等于未配备")} onClick={event => { sourceButton.current = event.currentTarget; setSource(source === id ? null : id); }}>{configurationValue(value, t)}</button></td>;
      })}</tr>{row.values.map((value, index) => source === JSON.stringify([row.key, index]) ? <tr key={`source-${index}`} className="mw-compare-source"><td colSpan={comparison.materials.length + 1}><div role="region" aria-label={t("Cell source", "单元格来源")}><strong>{comparison.materials[index].bomTemplate}</strong><span>{catalog.versions.find(version => version.id === comparison.materials[index].versionId)?.files[0]?.filename ?? t("Source unavailable", "原文件不可用")}</span><span>{value ? `${value.source_sheet ?? "在售可控资源表"}!${value.source_column}${value.source_row}` : t("Not recorded", "未记录")}</span><code>{comparison.materials[index].versionId}</code><button type="button" className="mw-remove" aria-label={t("Close source", "关闭来源")} onClick={closeSource}>×</button></div></td></tr> : null)}</Fragment>) : null}
    </tbody>)}{visibleBom.length ? <tbody><tr className="mw-compare-category"><th colSpan={comparison.materials.length + 1}>BOM · {t("Appearance", "外观内饰")}</th></tr>{visibleBom.map(row => <tr key={row.key}><th scope="row">{row.label}</th>{row.values.map((value, index) => <td className={value !== row.values[baseline] ? "is-different" : ""} key={comparison.materials[index].key} title={`BOM · ${comparison.materials[index].bomTemplate}`}>{value}</td>)}</tr>)}</tbody> : null}
    {!categories.length && !visibleBom.length ? <tbody><tr><td colSpan={comparison.materials.length + 1}>{t("No recorded differences", "未记录差异")}</td></tr></tbody> : null}</table></div>
  </section>;
}

export function MaterialComparisonPanel({ materials, onClose }: { materials: MaterialIdentity[]; onClose: () => void }) {
  const { t, language, setLanguage } = useAssetLanguage();
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
    <header className="mw-page-header"><h1>{t("Compare materials", "物料对比")}</h1><span className="mw-spacer" /><button type="button" onClick={() => setLanguage(language === "en" ? "zh" : "en")}>{language === "en" ? "中文" : "EN"}</button><button type="button" onClick={onClose}>{t("Close", "关闭")}</button></header>
    {error ? <p role="alert">{error}</p> : result ? <MaterialConfigurationComparison comparison={result.comparison} catalog={result.catalog} t={t} /> : <p role="status">{t("Loading…", "读取中…")}</p>}
  </section></OrderGeniusWorkspacePanel>;
}
