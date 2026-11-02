import { useEffect, useRef, useState } from "react";
import { ConfirmDialog } from "../ConfirmDialog";
import { materialAssets } from "../../api/materialAssets";
import type { AssetUploadTarget, ConfigPreview } from "../../types/materialAssets";
import type { AssetText } from "./materialAssetUi";
import { configurationValue, errorMessage } from "./materialAssetUi";

type Phase = "choose" | "uploading" | "pausing" | "paused" | "ready" | "saving" | "cancelling";
interface Props { target: AssetUploadTarget; label: string; files?: File[]; t: AssetText; onDone: (saved: number, cancelled: boolean) => void }
async function digest(blob: Blob): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", await blob.arrayBuffer());
  return [...new Uint8Array(hash)].map(value => value.toString(16).padStart(2, "0")).join("");
}
export function MaterialAssetUpload({ target, label, files = [], t, onDone }: Props) {
  const [phase, setPhase] = useState<Phase>("choose"), [error, setError] = useState("");
  const [progress, setProgress] = useState(0), [filename, setFilename] = useState("");
  const [preview, setPreview] = useState<ConfigPreview | null>(null), [column, setColumn] = useState("");
  const queue = useRef<File[]>(files), position = useRef(0), saved = useRef(0), currentTarget = useRef(target);
  const uploadId = useRef<string | null>(null), controller = useRef<AbortController | null>(null);
  const working = useRef<Promise<void> | null>(null), alive = useRef(true);
  const config = target.kind === "config";
  const transfer = async (): Promise<void> => {
    const abort = new AbortController(); controller.current = abort;
    setPhase("uploading"); setError("");
    try {
      while (position.current < queue.current.length) {
        const file = queue.current[position.current]; setFilename(file.name); setProgress(0);
        const limit = (config ? 32 : 100) * 1024 * 1024;
        if (file.size === 0 || file.size > limit) throw new Error(t(`Choose a file under ${config ? 32 : 100} MiB`, `请选择${config ? 32 : 100}MiB以内文件`));
        const chunkSize = 1024 * 1024, hashes: string[] = [];
        // Fingerprint the ordered chunk digests: never allocate a 70–100 MB ArrayBuffer.
        for (let offset = 0; offset < file.size; offset += chunkSize) {
          hashes.push(await digest(file.slice(offset, offset + chunkSize))); abort.signal.throwIfAborted();
        }
        const hash = await digest(new Blob([hashes.join("")])); abort.signal.throwIfAborted();
        let state = await materialAssets.startUpload(currentTarget.current, file, hash, abort.signal);
        uploadId.current = state.uploadId; setProgress(Math.round(state.uploadedBytes / file.size * 100));
        for (let part = 1; part <= state.totalChunks; part++) {
          if (state.receivedChunks.includes(part)) continue;
          const chunk = file.slice((part - 1) * state.chunkSize, part * state.chunkSize);
          const chunkHash = hashes[part - 1]; abort.signal.throwIfAborted();
          state = await materialAssets.uploadPart(state.uploadId, part, chunk, chunkHash, abort.signal);
          setProgress(Math.round(state.uploadedBytes / file.size * 100));
        }
        if (config) {
          const result = await materialAssets.previewUpload(state.uploadId, abort.signal);
          if (alive.current) { setPreview(result); setColumn(result.suggestedColumn === null ? "" : String(result.suggestedColumn)); setPhase("ready"); }
          return;
        }
        abort.signal.throwIfAborted(); setPhase("saving");
        const result = await materialAssets.saveUpload(state.uploadId, abort.signal);
        currentTarget.current = { ...currentTarget.current, groupId: result.groupId, rowVersion: result.rowVersion ?? 0 };
        saved.current++; position.current++; uploadId.current = null;
        if (alive.current) setPhase("uploading");
      }
      if (alive.current) onDone(saved.current, false);
    } catch (cause) {
      if (alive.current && controller.current === abort && !abort.signal.aborted) { setError(errorMessage(cause)); setPhase("paused"); }
    }
  };
  const start = (): void => { working.current = transfer(); };
  useEffect(() => {
    alive.current = true;
    if (files.length) start();
    return () => { alive.current = false; controller.current?.abort(); };
  }, []);
  const pause = async (): Promise<void> => {
    setPhase("pausing"); controller.current?.abort(); await working.current;
    if (alive.current) setPhase("paused");
  };
  const cancel = async (): Promise<void> => {
    if (phase === "saving" || phase === "cancelling") return;
    setPhase("cancelling"); controller.current?.abort(); await working.current;
    try {
      const result = uploadId.current ? await materialAssets.cancelUpload(uploadId.current) : { cancelled: true, saved: false };
      if (alive.current) onDone(saved.current + (result.saved ? 1 : 0), result.cancelled);
    } catch (cause) { if (alive.current) { setError(errorMessage(cause)); setPhase("paused"); } }
  };
  const saveConfig = async (): Promise<void> => {
    if (!uploadId.current || !column) return;
    const abort = new AbortController(); controller.current = abort; setPhase("saving"); setError("");
    try { await materialAssets.saveUpload(uploadId.current, abort.signal, Number(column)); if (alive.current) onDone(1, false); }
    catch (cause) { if (alive.current) { setError(errorMessage(cause)); setPhase("ready"); } }
  };
  const selected = preview?.columns.find(item => String(item.column) === column);
  return <ConfirmDialog title={config ? t("Upload configuration", "上传配置表") : t("Upload photos", "上传图片")}
    description={`${label} · ${filename || (config ? "XLSX" : target.colourKey.split(":").slice(1).join(":"))}`} cancelLabel={t("Cancel upload", "取消上传")} confirmLabel={phase === "paused" ? t("Resume", "继续上传") : t("Save version", "保存版本")}
    loadingLabel={phase === "pausing" ? t("Pausing…", "暂停中…") : phase === "cancelling" ? t("Cancelling…", "取消中…") : t("Saving…", "保存中…")} submitting={phase === "saving" || phase === "cancelling" || phase === "pausing"}
    confirmDisabled={phase !== "paused" && !(phase === "ready" && !!column && !!selected?.filledCount)}
    error={error ? { title: t("Not completed", "未完成"), message: error } : null}
    onCancel={() => void cancel()} onConfirm={() => phase === "paused" ? start() : void saveConfig()}>
    {config && phase === "choose" ? <label className="mw-form-label">XLSX<input type="file" accept=".xlsx" onChange={event => {
      const file = event.target.files?.[0]; if (!file) return;
      queue.current = [file]; setPreview(null); setColumn(""); start();
    }} /></label> : null}
    {config && phase === "choose" ? <a className="mw-button" href="/material-configuration-template.xlsx" download>{t("Download format", "下载格式模板")}</a> : null}
    {selected && !selected.filledCount ? <p role="alert">{t("This material column has no values. Fill it before saving.", "该物料列没有填写值，请先填写后再保存。")}</p> : null}
    {phase !== "choose" ? <div className="mw-upload-progress"><progress aria-label={t("Upload progress", "上传进度")} max={100} value={progress} /><span>{progress}% · {Math.min(position.current + 1, queue.current.length)}/{queue.current.length}</span>
      {phase === "uploading" ? <button type="button" onClick={() => void pause()}>{t("Pause", "暂停")}</button> : null}
      {phase === "paused" ? <span role="status">{t("Paused", "已暂停")}</span> : null}</div> : null}
    {preview ? <label className="mw-form-label">{t("Configuration column", "配置列")}<select aria-label={t("Configuration column", "配置列")} value={column} disabled={phase === "saving"} onChange={event => setColumn(event.target.value)}>
      <option value="">{t("Choose…", "请选择…")}</option>{preview.columns.map(item => <option key={item.column} value={item.column} disabled={preview.materialHeaders && item.column !== preview.suggestedColumn}>{item.name}</option>)}
    </select></label> : null}
    {selected ? <section aria-label={t("Configuration preview", "配置预览")}><strong>{selected.featureCount} {t("configuration items", "个配置项")}</strong><div className="mw-table-scroll"><table className="mw-table"><thead><tr><th>{t("Feature", "配置项")}</th><th>{t("Value", "值")}</th><th>{t("Source", "来源")}</th></tr></thead><tbody>{selected.sample.map(cell => <tr key={JSON.stringify([cell.category, cell.feature_name])}><th><small>{cell.category}</small>{cell.feature_name}</th><td>{configurationValue(cell, t)}</td><td>{cell.source_sheet}!{cell.source_column}{cell.source_row}</td></tr>)}</tbody></table></div></section> : null}
    {preview?.warnings.length ? <details><summary>{t("Source warnings", "源文件提示")} ({preview.warnings.length})</summary>{preview.warnings.map((warning, index) => <p key={index}>{warning}</p>)}</details> : null}
  </ConfirmDialog>;
}
