import { useEffect, useRef, useState } from "react";
import type { ReactElement } from "react";
import { ConfirmDialog } from "../ConfirmDialog";
import { materialAssets, saveAssetBlob } from "../../api/materialAssets";
import type { AssetDownloadMaterial, AssetDownloadPreview } from "../../types/materialAssets";
import type { AssetText } from "./materialAssetUi";
import { errorMessage } from "./materialAssetUi";

interface Props { initialMonth: string; t: AssetText; onClose: () => void }
export function MaterialAssetDownload({ initialMonth, t, onClose }: Props): ReactElement {
  const [month, setMonth] = useState(initialMonth), [data, setData] = useState<AssetDownloadPreview | null>(null);
  const [selected, setSelected] = useState<string[]>([]), [error, setError] = useState("");
  const [busy, setBusy] = useState(false), [revision, setRevision] = useState(0);
  const download = useRef<AbortController | null>(null);
  useEffect(() => {
    const abort = new AbortController(); setData(null); setSelected([]); setError("");
    if (/^\d{4}-\d{2}$/.test(month)) void materialAssets.downloadPreview(month, abort.signal)
      .then(result => { if (!abort.signal.aborted) setData(result); })
      .catch(cause => { if (!abort.signal.aborted) setError(errorMessage(cause)); });
    return () => abort.abort();
  }, [month, revision]);
  useEffect(() => () => download.current?.abort(), []);
  const current = data?.referenceMonth === month ? data : null;
  const allGroups = current?.materials.flatMap(material => material.groups).filter(group => group.files.length) ?? [];
  const chosen = selected.length ? allGroups.filter(group => selected.includes(group.key)) : allGroups;
  const files = chosen.flatMap(group => group.files), bytes = files.reduce((sum, file) => sum + file.bytes, 0);
  const models = new Map<string, AssetDownloadMaterial[]>();
  for (const material of current?.materials ?? []) {
    const key = `${material.brand} · ${material.modelName}`;
    models.set(key, [...(models.get(key) ?? []), material]);
  }
  const toggle = (keys: string[], checked: boolean): void => setSelected(previous => checked
    ? [...new Set([...previous, ...keys])] : previous.filter(key => !keys.includes(key)));
  const checkbox = (keys: string[], label: string): ReactElement => {
    const count = keys.filter(key => selected.includes(key)).length;
    return <input type="checkbox" aria-label={label} checked={!!keys.length && count === keys.length} disabled={busy || !keys.length}
      ref={element => { if (element) element.indeterminate = count > 0 && count < keys.length; }} onChange={event => toggle(keys, event.target.checked)} />;
  };
  const save = async (): Promise<void> => {
    if (!current || !files.length || bytes > current.maxBytes) return;
    const abort = new AbortController(); download.current = abort; setBusy(true); setError("");
    try {
      const blob = await materialAssets.downloadMonth(month, current.signature, selected, abort.signal);
      if (!abort.signal.aborted) saveAssetBlob(blob, `Material_Assets_${month}.zip`);
    } catch (cause) { if (!abort.signal.aborted) setError(errorMessage(cause)); }
    finally { if (!abort.signal.aborted) setBusy(false); }
  };
  const tooLarge = !!current && bytes > current.maxBytes;
  return <ConfirmDialog className="mw-download-dialog" title={t("Download materials", "下载物料资料")}
    description={current ? `${files.length} ${t("files", "个文件")} · ${(bytes / 1e6).toFixed(1)} MB` : "—"}
    cancelLabel={t("Close", "关闭")} confirmLabel={`${selected.length ? t("Download selected", "下载所选") : t("Download all", "下载全部")} ${files.length}`}
    loadingLabel={t("Preparing ZIP…", "正在打包…")} submitting={busy} confirmDisabled={!current || !files.length || tooLarge}
    error={error ? { title: t("Not downloaded", "未下载"), message: error, actions: [{ label: t("Reload preview", "重读预览"), onClick: () => setRevision(v => v + 1) }] } : null}
    onCancel={onClose} onConfirm={() => void save()}>
    <div className="mw-toolbar"><label>{t("Month", "月份")} <input type="month" aria-label={t("Month", "月份")} value={month} disabled={busy} onChange={event => { setData(null); setSelected([]); setMonth(event.target.value); }} /></label>
      {selected.length ? <button type="button" disabled={busy} onClick={() => setSelected([])} title={t("No selection downloads all in this month", "未勾选时下载本月范围全部资料")}>{t("Clear selection", "清空勾选")}</button> : null}</div>
    {tooLarge ? <p role="alert">{t(`Package limit ${(current.maxBytes / 1024 / 1024).toFixed(0)} MiB. Select fewer models/materials.`, `单包上限${(current.maxBytes / 1024 / 1024).toFixed(0)}MiB，请按车型或物料分批勾选。`)}</p> : null}
    {!current && !error && month ? <div role="status">{t("Loading…", "读取中…")}</div> : null}
    {current && !models.size ? <div className="mw-empty">{t("No effective materials this month", "本月无有效物料")}</div> : null}
    {[...models].map(([model, materials]) => <section className="mw-download-model" key={model}>
      <h3><label>{checkbox(materials.flatMap(material => material.groups.filter(group => group.files.length).map(group => group.key)), `${t("Select model", "勾选车型")} ${model}`)} {model}</label></h3>
      {materials.map(material => {
        const keys = material.groups.filter(group => group.files.length).map(group => group.key);
        return <div className="mw-download-material" key={material.key}>
          <label>{checkbox(keys, `${t("Select material", "勾选物料")} ${material.bomTemplate}`)} <strong>{material.bomTemplate}</strong></label>
          <details><summary><span>{material.version}</span><span>{material.effectiveFrom ?? "—"} → {material.effectiveTo ?? "∞"}</span><span>{material.groups.reduce((sum, group) => sum + group.files.length, 0)} {t("files", "个文件")}</span></summary>
            {material.groups.map(group => {
              const kind = group.kind === "config" ? t("Configuration", "配置表") : group.kind === "exterior" ? t("Exterior", "外饰") : t("Interior", "内饰");
              return <label className="mw-download-group" key={group.key}>{checkbox(group.files.length ? [group.key] : [], `${t("Select", "勾选")} ${kind} ${group.label} ${material.bomTemplate}`)}
                <span>{group.kind === "config" ? kind : `${kind} · ${group.label}`}</span><span>{group.files.length || t("Not linked / no file", "未关联或无文件")}</span>
              </label>;
            })}
          </details>{material.issues.length ? <span role="alert">{material.issues.join(" · ")}</span> : null}
        </div>;
      })}
    </section>)}
  </ConfirmDialog>;
}
