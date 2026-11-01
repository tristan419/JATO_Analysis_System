import { useEffect, useState } from "react";
import { ConfirmDialog } from "../ConfirmDialog";
import { materialAssets, saveAssetBlob } from "../../api/materialAssets";
import type { AssetCatalog, AssetColour, AssetGroup, AssetKind, AssetLinkChange, AssetMaterial, AssetUploadTarget, AssetVersion, ConfigDetails } from "../../types/materialAssets";
import { parseOrderGeniusColourSwatch } from "../../utils/orderGeniusColourSwatch";
import type { AssetText } from "./materialAssetUi";
import { errorMessage } from "./materialAssetUi";
import { MaterialPhotoThumbnail, MaterialPhotoViewer } from "./MaterialPhotoViewer";
import { MaterialAssetUpload } from "./MaterialAssetUpload";
interface Props { material: AssetMaterial; catalog: AssetCatalog; editor: boolean; t: AssetText; scope: string; onReload: () => Promise<void> }
export function MaterialAssetInspector({ material, catalog, editor, t, scope, onReload }: Props) {
  const [busy, setBusy] = useState(false), [error, setError] = useState("");
  const [photo, setPhoto] = useState<AssetGroup | null>(null), [config, setConfig] = useState<AssetVersion | null>(null);
  const [upload, setUpload] = useState<{ target: AssetUploadTarget; files: File[] } | null>(null), [notice, setNotice] = useState("");
  const [details, setDetails] = useState<ConfigDetails | null>(null);
  const [reuse, setReuse] = useState<AssetGroup | null>(null), [targets, setTargets] = useState<string[]>([]);
  const [reuseConfig, setReuseConfig] = useState<string | null>(null);
  const [remove, setRemove] = useState<{ message: string; run: () => Promise<unknown> } | null>(null);
  const bindings = catalog.bindings.filter(b => b.materialKey === material.key);
  const boundConfig = bindings.find(b => b.kind === "config");
  const currentVersion = catalog.versions.find(v => v.id === boundConfig?.versionId);
  const versions = catalog.versions.filter(v => (v.brand === material.brand && v.modelName === material.modelName && v.bomTemplate === material.bomTemplate) || v.id === boundConfig?.versionId).sort((a, b) => b.number - a.number);
  const reusableConfigs = catalog.versions.filter(v => v.brand === material.brand && v.modelName === material.modelName && v.id !== currentVersion?.id);
  const canWrite = editor && !material.issues.length;
  const act = async (action: () => Promise<unknown>): Promise<void> => {
    if (busy) return; setBusy(true); setError("");
    try {
      await action(); setRemove(null); setReuse(null); setReuseConfig(null); setUpload(null);
      try { await onReload(); }
      catch (e) { setError(`${t("Saved. Refresh failed; reload to verify before repeating.", "已保存，但刷新失败；请重读核对，勿重复提交。")} ${errorMessage(e)}`); }
    }
    catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
  useEffect(() => {
    if (!config) return;
    const controller = new AbortController(); setDetails(null);
    void materialAssets.config(config.id, controller.signal).then(setDetails).catch(e => { if (!controller.signal.aborted) setError(errorMessage(e)); });
    return () => controller.abort();
  }, [config]);
  const change = (target: AssetMaterial, kind: AssetKind, key: string, groupId: string | null, versionId: string | null = null): AssetLinkChange => ({
    materialKey: target.key, kind, colourKey: key, groupId, versionId,
    rowVersion: catalog.bindings.find(b => b.materialKey === target.key && b.kind === kind && b.colourKey === key)?.rowVersion ?? 0,
  });
  const eligible = reuse ? catalog.materials.filter(m => m.brand === reuse.brand && m.modelName === reuse.modelName && !m.issues.length && (reuse.kind === "exterior" ? m.colours : m.interiors).some(c => c.key === reuse.colourKey)) : [];
  const renderSlot = (colour: AssetColour, kind: "exterior" | "interior") => {
    const binding = bindings.find(b => b.kind === kind && b.colourKey === colour.key);
    const group = catalog.groups.find(g => g.id === binding?.groupId);
    const available = catalog.groups.filter(g => g.brand === material.brand && g.modelName === material.modelName && g.kind === kind && g.colourKey === colour.key);
    return <div className="mw-colour-card" key={colour.key}>
      <div className="mw-toolbar"><span className="mw-swatch" style={{ background: parseOrderGeniusColourSwatch(colour.hex).background }} /><strong>{colour.code ?? colour.name}</strong><span className="mw-muted">{colour.name !== colour.code ? colour.name : ""}</span></div>
      {group ? <MaterialPhotoThumbnail group={group} scope={scope} onClick={() => setPhoto(group)} /> : <div className="mw-gallery-empty">{t("No photos", "暂无图片")}</div>}
      {canWrite ? <>
        <select aria-label={`${t("Photo group", "图片组")} ${kind} ${colour.code ?? colour.name}`} disabled={busy} value={group?.id ?? ""} onChange={e => { const id = e.target.value; if (!id) setRemove({ message: t("Unlink this material? Shared originals stay available.", "解除此物料关联？共享原图仍会保留。"), run: () => materialAssets.bind([change(material, kind, colour.key, null)]) }); else void act(() => materialAssets.bind([change(material, kind, colour.key, id)])); }}>
          <option value="">{t("Unlinked", "未关联")}</option>{available.map(g => <option key={g.id} value={g.id}>{g.name} · {g.files.length}</option>)}
        </select>
        <div className="mw-toolbar"><label className="mw-button">{group ? t("Add photos", "添加图片") : t("Upload photos", "上传图片")}<input type="file" accept=".jpg,.jpeg,.png,.gif,.webp,.bmp,.tif,.tiff,.avif,.heic,.heif" multiple disabled={busy || !!upload} onChange={e => {
          const files = Array.from(e.target.files ?? []); e.target.value = ""; if (!files.length) return;
          setNotice("");
          if (files.length > 16) { setError(t("Choose at most 16 photos", "每次最多选择16张图片")); return; }
          setUpload({ target: { materialKey: material.key, kind, colourKey: colour.key, groupId: group?.id ?? null, rowVersion: group?.rowVersion ?? binding?.rowVersion ?? 0 }, files });
        }} /></label>
        {group ? <button type="button" disabled={busy} onClick={() => { setReuse(group); setTargets([]); }}>{t("Reuse…", "批量复用…")}</button> : null}</div>
        {group ? <details><summary>{t("Shared photos", "共享图片")} <span className="mw-count">{group.references.length}</span></summary>
          <div className="mw-originals">{group.files.map(f => <div className="mw-original-card" key={f.id}><span>{f.filename}</span><button type="button" className="mw-remove" aria-label={`${t("Move to recycle bin", "移入回收站")} ${f.filename}`} disabled={busy} onClick={() => setRemove({ message: t(`Move this shared photo to the recycle bin? ${group.references.length} material links are affected.`, `移入回收站？将影响${group.references.length}个物料关联。`), run: () => materialAssets.deletePhoto(f.id, group.rowVersion) })}>×</button></div>)}</div>
        </details> : null}
      </> : null}
    </div>;
  };
  return <aside className="mw-inspector" aria-label={material.bomTemplate}>
    <header><span className="mw-muted">{material.brand} · {material.modelName}</span><h2>{material.bomTemplate}</h2><div className="mw-product-fields"><span>{material.version}</span><span>{material.powertrain}</span><span>{material.effectiveFrom ?? "—"} → {material.effectiveTo ?? "∞"}</span></div></header>
    {material.issues.length ? <p role="alert">{material.issues.join(" · ")}</p> : null}
    {error && !upload && !remove && !reuse && !config && reuseConfig === null ? <p role="alert">{error}</p> : null}
    {notice ? <div role="status">{notice}</div> : null}
    <section><div className="mw-toolbar"><h3>{t("Configuration", "配置表")}</h3><span className="mw-spacer" />{canWrite && reusableConfigs.length ? <button type="button" disabled={busy} onClick={() => { setReuseConfig(""); setError(""); }}>{t("Reuse configuration", "复用配置")}</button> : null}{canWrite ? <button type="button" disabled={busy} onClick={() => { setNotice(""); setUpload({ target: { materialKey: material.key, kind: "config", colourKey: "", groupId: null, rowVersion: boundConfig?.rowVersion ?? 0 }, files: [] }); setError(""); }}>{currentVersion ? t("Update", "更新") : t("Upload", "上传")}</button> : null}</div>
      {currentVersion ? <div className="mw-original-card mw-current-config"><button type="button" className="mw-config-card" onClick={() => { setConfig(currentVersion); setError(""); }}><strong>{currentVersion.files[0]?.filename} <span>v{currentVersion.number}</span></strong><small>{currentVersion.id}</small></button>{canWrite ? <button type="button" className="mw-remove" aria-label={t("Move current configuration to recycle bin", "当前配置移入回收站")} disabled={busy} onClick={() => setRemove({ message: t(`Move this configuration to the recycle bin? ${currentVersion.references.length} linked materials lose access until restored. Version history stays saved.`, `配置移入回收站？${currentVersion.references.length}个关联物料的对比将暂不可用，恢复后可继续使用；版本历史保留。`), run: () => materialAssets.deleteConfig(currentVersion.id) })}>×</button> : null}</div> : <span className="mw-muted">{t("Not linked", "未关联")}</span>}
      {versions.length ? <details><summary>{t("Versions", "版本")} <span className="mw-count">{versions.length}</span></summary>{versions.map(v => <div key={v.id} className="mw-version"><button type="button" className="mw-link" onClick={() => { setConfig(v); setError(""); }}>v{v.number} · {v.files[0]?.filename}</button><small>{v.createdAt.slice(0, 10)}</small>
        {canWrite && v.id !== currentVersion?.id ? <button type="button" disabled={busy} onClick={() => void act(() => materialAssets.bind([change(material, "config", "", null, v.id)]))}>{t("Use", "使用")}</button> : null}
        {canWrite ? <button type="button" className="mw-remove" aria-label={`${t("Move configuration to recycle bin", "配置移入回收站")} v${v.number}`} disabled={busy} onClick={() => setRemove({ message: t(`Move v${v.number} to the recycle bin? Linked comparisons become unavailable until restored; version history stays saved.`, `将v${v.number}移入回收站？关联对比暂不可用，恢复后可继续使用；版本历史保留。`), run: () => materialAssets.deleteConfig(v.id) })}>×</button> : null}
      </div>)}{canWrite && currentVersion ? <button type="button" onClick={() => setRemove({ message: t("Unlink this configuration? Existing versions and originals stay saved.", "解除配置关联？已有版本及原文件仍会保留。"), run: () => materialAssets.bind([change(material, "config", "", null)]) })}>{t("Unlink configuration", "解除配置关联")}</button> : null}</details> : null}
    </section>
    <section><h3>{t("Exterior", "外饰")}</h3><div className="mw-colours">{material.colours.map(c => renderSlot(c, "exterior"))}</div></section>
    <section><h3>{t("Interior", "内饰")}</h3><div className="mw-colours">{material.interiors.map(c => renderSlot(c, "interior"))}</div></section>
    {canWrite ? <details><summary>{t("Unlinked model albums", "Model未关联图片组")}</summary>{catalog.groups.filter(g => g.brand === material.brand && g.modelName === material.modelName && !g.references.length).map(g => <div className="mw-version" key={g.id}><button type="button" onClick={() => setPhoto(g)}>{g.name}</button><button type="button" className="mw-remove" aria-label={`${t("Move album to recycle bin", "图片组移入回收站")} ${g.name}`} disabled={busy} onClick={() => setRemove({ message: t(`Move ${g.name} and its photos to the recycle bin?`, `将${g.name}及图片移入回收站？`), run: () => materialAssets.deleteGroup(g.id, g.rowVersion) })}>×</button></div>)}</details> : null}
    {photo ? <MaterialPhotoViewer group={photo} scope={scope} t={t} onClose={() => setPhoto(null)} /> : null}
    {reuseConfig !== null ? <ConfirmDialog title={t("Reuse configuration", "复用配置")} description={material.bomTemplate} cancelLabel={t("Cancel", "取消")} confirmLabel={t("Use configuration", "使用配置")} loadingLabel={t("Saving…", "保存中…")} submitting={busy} confirmDisabled={!reuseConfig} error={error ? { title: t("Not saved", "未保存"), message: error } : null} onCancel={() => setReuseConfig(null)} onConfirm={() => void act(() => materialAssets.bind([change(material, "config", "", null, reuseConfig)]))}>
      <label className="mw-form-label">{t("Source version", "来源版本")}<select aria-label={t("Source version", "来源版本")} value={reuseConfig} onChange={event => setReuseConfig(event.target.value)}><option value="">{t("Choose…", "请选择…")}</option>{reusableConfigs.map(version => <option value={version.id} key={version.id}>{version.bomTemplate} · {version.files[0]?.filename} · v{version.number}</option>)}</select></label>
    </ConfirmDialog> : null}
    {upload ? <MaterialAssetUpload target={upload.target} label={material.bomTemplate} files={upload.files} t={t} onDone={(saved, cancelled) => {
      setUpload(null); setNotice(saved ? t(`Saved ${saved}${cancelled ? " · remaining uploads cancelled" : ""}`, `已保存${saved}项${cancelled ? "，其余上传已取消" : ""}`) : "");
      if (saved) void onReload().catch(cause => setError(errorMessage(cause)));
    }} /> : null}
    {config ? <ConfirmDialog title={`${material.bomTemplate} · v${config.number}`} description={config.id} cancelLabel={t("Close", "关闭")} confirmLabel={t("Download original", "下载原文件")} loadingLabel={t("Downloading…", "下载中…")} submitting={busy} confirmDisabled={!config.files.length} error={error ? { title: t("Unable to load", "读取失败"), message: error } : null} onCancel={() => setConfig(null)} onConfirm={() => { setBusy(true); void materialAssets.download(config.files.map(f => f.id)).then(blob => saveAssetBlob(blob, `${material.bomTemplate.replaceAll("*", "_")}-config.zip`)).catch(e => setError(errorMessage(e))).finally(() => setBusy(false)); }}>
      <div className="mw-source">{config.source} · {config.files[0]?.filename} · SHA256 {config.files[0]?.sha256}</div><div className="mw-table-scroll"><table className="mw-table"><thead><tr><th>{t("Feature", "配置项")}</th><th>{t("Value", "值")}</th><th>{t("Source", "来源")}</th></tr></thead><tbody>{details?.values.map(cell => <tr key={`${cell.category}|${cell.feature_name}`}><td>{cell.category} / {cell.feature_name}</td><td>{cell.raw_value ?? "—"}</td><td>{cell.source_column}{cell.source_row}</td></tr>)}</tbody></table></div>
    </ConfirmDialog> : null}
    {reuse ? <ConfirmDialog title={`${t("Reuse", "复用")} · ${reuse.name}`} description={`${reuse.modelName} · ${reuse.colourKey.replace(/^(code|name):/, "")}`} cancelLabel={t("Cancel", "取消")} confirmLabel={`${t("Apply to", "应用到")} ${targets.length}`} loadingLabel={t("Saving…", "保存中…")} submitting={busy} confirmDisabled={!targets.length} error={error ? { title: t("Not saved", "未保存"), message: error } : null} onCancel={() => setReuse(null)} onConfirm={() => void act(() => materialAssets.bind(eligible.filter(m => targets.includes(m.key)).map(m => change(m, reuse.kind, reuse.colourKey, reuse.id))))}>
      {eligible.map(m => <label className="mw-target" key={m.key}><input type="checkbox" checked={targets.includes(m.key)} onChange={e => setTargets(values => e.target.checked ? [...values, m.key] : values.filter(k => k !== m.key))} /><strong>{m.bomTemplate}</strong><span>{m.version}</span>{m.historical ? <small>{t("Historical", "历史")}</small> : null}</label>)}
    </ConfirmDialog> : null}
    {remove ? <ConfirmDialog title={t("Confirm removal", "确认移除")} description={remove.message} tone="danger" cancelLabel={t("Cancel", "取消")} confirmLabel={t("Confirm", "确认")} loadingLabel={t("Removing…", "移除中…")} submitting={busy} error={error ? { title: t("Not removed", "未移除"), message: error } : null} onCancel={() => setRemove(null)} onConfirm={() => void act(remove.run)} /> : null}
  </aside>;
}
