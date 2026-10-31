import { useEffect, useState } from "react";
import { ConfirmDialog } from "../ConfirmDialog";
import { materialAssets, saveAssetBlob } from "../../api/materialAssets";
import type { AssetCatalog, AssetColour, AssetGroup, AssetKind, AssetLinkChange, AssetMaterial, AssetVersion, ConfigDetails, ConfigPreview } from "../../types/materialAssets";
import { parseOrderGeniusColourSwatch } from "../../utils/orderGeniusColourSwatch";
import type { AssetText } from "./materialAssetUi";
import { errorMessage } from "./materialAssetUi";
import { MaterialPhotoThumbnail, MaterialPhotoViewer } from "./MaterialPhotoViewer";
interface Props { material: AssetMaterial; catalog: AssetCatalog; editor: boolean; t: AssetText; scope: string; onReload: () => Promise<void> }
export function MaterialAssetInspector({ material, catalog, editor, t, scope, onReload }: Props) {
  const [busy, setBusy] = useState(false), [error, setError] = useState("");
  const [photo, setPhoto] = useState<AssetGroup | null>(null), [config, setConfig] = useState<AssetVersion | null>(null);
  const [upload, setUpload] = useState(false), [file, setFile] = useState<File | null>(null), [preview, setPreview] = useState<ConfigPreview | null>(null), [column, setColumn] = useState("");
  const [details, setDetails] = useState<ConfigDetails | null>(null);
  const [reuse, setReuse] = useState<AssetGroup | null>(null), [targets, setTargets] = useState<string[]>([]);
  const [remove, setRemove] = useState<{ message: string; run: () => Promise<unknown> } | null>(null);
  const bindings = catalog.bindings.filter(b => b.materialKey === material.key);
  const boundConfig = bindings.find(b => b.kind === "config");
  const currentVersion = catalog.versions.find(v => v.id === boundConfig?.versionId);
  const versions = catalog.versions.filter(v => v.brand === material.brand && v.modelName === material.modelName && v.bomTemplate === material.bomTemplate).sort((a, b) => b.number - a.number);
  const canWrite = editor && !material.issues.length;
  const act = async (action: () => Promise<unknown>): Promise<void> => {
    if (busy) return; setBusy(true); setError("");
    try {
      await action(); setRemove(null); setReuse(null); setUpload(false);
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
  const chooseFile = async (selected: File | undefined): Promise<void> => {
    setFile(selected ?? null); setPreview(null); setColumn(""); setError(""); if (!selected) return;
    setBusy(true); const form = new FormData(); form.set("file", selected);
    try { setPreview(await materialAssets.previewConfig(form)); } catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
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
        <div className="mw-toolbar"><label className="mw-button">{group ? t("Add photos", "添加图片") : t("Upload photos", "上传图片")}<input type="file" accept="image/png,image/jpeg,image/gif,image/webp" multiple disabled={busy} onChange={e => {
          const files = Array.from(e.target.files ?? []); e.target.value = ""; if (!files.length) return;
          const form = new FormData(); files.forEach(f => form.append("files", f));
          if (group) { form.set("row_version", String(group.rowVersion)); void act(() => materialAssets.addPhotos(group.id, form)); }
          else { form.set("material_key", material.key); form.set("kind", kind); form.set("colour_key", colour.key); form.set("name", `${colour.code ?? colour.name} · ${kind}`); form.set("row_version", String(binding?.rowVersion ?? 0)); void act(() => materialAssets.createGroup(form)); }
        }} /></label>
        {group ? <button type="button" disabled={busy} onClick={() => { setReuse(group); setTargets([]); }}>{t("Reuse…", "批量复用…")}</button> : null}</div>
        {group ? <details><summary>{t("Manage shared originals", "管理共享原图")} <span className="mw-count">{group.references.length}</span></summary>
          <ul className="mw-files">{group.files.map(f => <li key={f.id}><span>{f.filename}</span><button type="button" disabled={busy} onClick={() => setRemove({ message: t(`Permanently delete this photo from the shared group? ${group.references.length} material links are affected.`, `永久删除这张共享原图？将影响${group.references.length}个物料关联。`), run: () => materialAssets.deletePhoto(f.id, group.rowVersion) })}>{t("Delete", "删除")}</button></li>)}</ul>
        </details> : null}
      </> : null}
    </div>;
  };
  return <aside className="mw-inspector" aria-label={material.bomTemplate}>
    <header><span className="mw-muted">{material.brand} · {material.modelName}</span><h2>{material.bomTemplate}</h2><div className="mw-product-fields"><span>{material.version}</span><span>{material.powertrain}</span><span>{material.effectiveFrom ?? "—"} → {material.effectiveTo ?? "∞"}</span></div></header>
    {material.issues.length ? <p role="alert">{material.issues.join(" · ")}</p> : null}
    {error && !upload && !remove && !reuse && !config ? <p role="alert">{error}</p> : null}
    <section><div className="mw-toolbar"><h3>{t("Configuration", "配置表")}</h3><span className="mw-spacer" />{canWrite ? <button type="button" disabled={busy} onClick={() => { setUpload(true); setFile(null); setPreview(null); setColumn(""); setError(""); }}>{currentVersion ? t("Update", "更新") : t("Upload", "上传")}</button> : null}</div>
      {currentVersion ? <button type="button" className="mw-config-card" onClick={() => { setConfig(currentVersion); setError(""); }}><strong>{currentVersion.files[0]?.filename} <span>v{currentVersion.number}</span></strong><small>{currentVersion.id}</small></button> : <span className="mw-muted">{t("Not linked", "未关联")}</span>}
      {versions.length ? <details><summary>{t("Versions", "版本")} <span className="mw-count">{versions.length}</span></summary>{versions.map(v => <div key={v.id} className="mw-version"><button type="button" className="mw-link" onClick={() => { setConfig(v); setError(""); }}>v{v.number} · {v.files[0]?.filename}</button><small>{v.createdAt.slice(0, 10)}</small>
        {canWrite && v.id !== currentVersion?.id ? <button type="button" disabled={busy} onClick={() => void act(() => materialAssets.bind([change(material, "config", "", null, v.id)]))}>{t("Use", "使用")}</button> : null}
        {canWrite ? <button type="button" disabled={busy || v.references.length > 0 || versions.some(child => child.parentId === v.id)} title={t("Only an unlinked version without successors can be deleted", "仅可删除未关联且无后续引用的版本")} onClick={() => setRemove({ message: t(`Permanently delete v${v.number} and its source file?`, `永久删除v${v.number}及其原文件？`), run: () => materialAssets.deleteConfig(v.id) })}>{t("Delete", "删除")}</button> : null}
      </div>)}{canWrite && currentVersion ? <button type="button" onClick={() => setRemove({ message: t("Unlink this configuration? Existing versions and originals stay saved.", "解除配置关联？已有版本及原文件仍会保留。"), run: () => materialAssets.bind([change(material, "config", "", null)]) })}>{t("Unlink configuration", "解除配置关联")}</button> : null}</details> : null}
    </section>
    <section><h3>{t("Exterior", "外饰")}</h3><div className="mw-colours">{material.colours.map(c => renderSlot(c, "exterior"))}</div></section>
    <section><h3>{t("Interior", "内饰")}</h3><div className="mw-colours">{material.interiors.map(c => renderSlot(c, "interior"))}</div></section>
    {canWrite ? <details><summary>{t("Unlinked model albums", "Model未关联图片组")}</summary>{catalog.groups.filter(g => g.brand === material.brand && g.modelName === material.modelName && !g.references.length).map(g => <div className="mw-version" key={g.id}><button type="button" onClick={() => setPhoto(g)}>{g.name}</button><button type="button" disabled={busy} onClick={() => setRemove({ message: t(`Permanently delete ${g.name} and all its originals?`, `永久删除${g.name}及全部原图？`), run: () => materialAssets.deleteGroup(g.id, g.rowVersion) })}>{t("Delete group", "删除图片组")}</button></div>)}</details> : null}
    {photo ? <MaterialPhotoViewer group={photo} scope={scope} t={t} onClose={() => setPhoto(null)} /> : null}
    {upload ? <ConfirmDialog title={`${t("Configuration", "配置表")} · ${material.bomTemplate}`} description={t("New immutable version", "保存为新的不可变版本")} cancelLabel={t("Cancel", "取消")} confirmLabel={t("Save version", "保存版本")} loadingLabel={t("Processing…", "处理中…")} submitting={busy} confirmDisabled={!file || !column || !preview} error={error ? { title: t("Not saved", "未保存"), message: error } : null} onCancel={() => setUpload(false)} onConfirm={() => { if (!file || !column) return; const form = new FormData(); form.set("file", file); form.set("material_key", material.key); form.set("source_column", column); form.set("row_version", String(boundConfig?.rowVersion ?? 0)); void act(() => materialAssets.uploadConfig(form)); }}>
      <label className="mw-form-label">XLSX <span title={t("Existing configuration matrix: sheet 在售可控资源表, trims on row 3, features from row 5. Maximum 8 MiB.", "现有配置矩阵格式：工作表在售可控资源表，第3行为配置版本，第5行起为配置项目。最大8MiB。")}>ⓘ</span><input type="file" accept=".xlsx" disabled={busy} onChange={e => void chooseFile(e.target.files?.[0])} /></label>
      {preview ? <label className="mw-form-label">{t("Configuration column", "配置列")}<select value={column} onChange={e => setColumn(e.target.value)}><option value="">{t("Choose…", "请选择…")}</option>{preview.columns.map(c => <option key={c.column} value={c.column}>{c.name}</option>)}</select></label> : null}
      {preview?.warnings.length ? <details><summary>{t("Source warnings", "源文件提示")} ({preview.warnings.length})</summary>{preview.warnings.map((w, i) => <p key={i}>{w}</p>)}</details> : null}
    </ConfirmDialog> : null}
    {config ? <ConfirmDialog title={`${material.bomTemplate} · v${config.number}`} description={config.id} cancelLabel={t("Close", "关闭")} confirmLabel={t("Download original", "下载原文件")} loadingLabel={t("Downloading…", "下载中…")} submitting={busy} confirmDisabled={!config.files.length} error={error ? { title: t("Unable to load", "读取失败"), message: error } : null} onCancel={() => setConfig(null)} onConfirm={() => { setBusy(true); void materialAssets.download(config.files.map(f => f.id)).then(blob => saveAssetBlob(blob, `${material.bomTemplate.replaceAll("*", "_")}-config.zip`)).catch(e => setError(errorMessage(e))).finally(() => setBusy(false)); }}>
      <div className="mw-source">{config.source} · {config.files[0]?.filename} · SHA256 {config.files[0]?.sha256}</div><div className="mw-table-scroll"><table className="mw-table"><thead><tr><th>{t("Feature", "配置项")}</th><th>{t("Value", "值")}</th><th>{t("Source", "来源")}</th></tr></thead><tbody>{details?.values.map(cell => <tr key={`${cell.category}|${cell.feature_name}`}><td>{cell.category} / {cell.feature_name}</td><td>{cell.raw_value ?? "—"}</td><td>{cell.source_column}{cell.source_row}</td></tr>)}</tbody></table></div>
    </ConfirmDialog> : null}
    {reuse ? <ConfirmDialog title={`${t("Reuse", "复用")} · ${reuse.name}`} description={`${reuse.modelName} · ${reuse.colourKey.replace(/^(code|name):/, "")}`} cancelLabel={t("Cancel", "取消")} confirmLabel={`${t("Apply to", "应用到")} ${targets.length}`} loadingLabel={t("Saving…", "保存中…")} submitting={busy} confirmDisabled={!targets.length} error={error ? { title: t("Not saved", "未保存"), message: error } : null} onCancel={() => setReuse(null)} onConfirm={() => void act(() => materialAssets.bind(eligible.filter(m => targets.includes(m.key)).map(m => change(m, reuse.kind, reuse.colourKey, reuse.id))))}>
      {eligible.map(m => <label className="mw-target" key={m.key}><input type="checkbox" checked={targets.includes(m.key)} onChange={e => setTargets(values => e.target.checked ? [...values, m.key] : values.filter(k => k !== m.key))} /><strong>{m.bomTemplate}</strong><span>{m.version}</span>{m.historical ? <small>{t("Historical", "历史")}</small> : null}</label>)}
    </ConfirmDialog> : null}
    {remove ? <ConfirmDialog title={t("Confirm removal", "确认移除")} description={remove.message} tone="danger" cancelLabel={t("Cancel", "取消")} confirmLabel={t("Confirm", "确认")} loadingLabel={t("Removing…", "移除中…")} submitting={busy} error={error ? { title: t("Not removed", "未移除"), message: error } : null} onCancel={() => setRemove(null)} onConfirm={() => void act(remove.run)} /> : null}
  </aside>;
}
