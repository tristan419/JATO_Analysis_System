import { useEffect, useState } from "react";
import { materialAssets } from "../../api/materialAssets";
import type { AssetTrash, AssetTrashEntry } from "../../types/materialAssets";
import { ConfirmDialog } from "../ConfirmDialog";
import type { AssetText } from "./materialAssetUi";
import { errorMessage } from "./materialAssetUi";

interface Props { t: AssetText; onClose: () => void; onChanged: () => Promise<void> }
function formatBytes(value: number): string {
  const [unit, divisor]: [string, number] = value >= 1e9 ? ["GB", 1e9] : value >= 1e6 ? ["MB", 1e6] : value >= 1e3 ? ["KB", 1e3] : ["B", 1];
  return `${(value / divisor).toFixed(divisor === 1 ? 0 : 1)} ${unit}`;
}
export function MaterialAssetTrash({ t, onClose, onChanged }: Props) {
  const [data, setData] = useState<AssetTrash | null>(null), [folder, setFolder] = useState<"photos" | "configurations">("photos");
  const [error, setError] = useState(""), [busy, setBusy] = useState(false), [purge, setPurge] = useState<AssetTrashEntry | null>(null);
  useEffect(() => {
    const abort = new AbortController();
    void materialAssets.trash(abort.signal).then(setData).catch(cause => { if (!abort.signal.aborted) setError(errorMessage(cause)); });
    return () => abort.abort();
  }, []);
  const change = async (id: string, permanent: boolean): Promise<void> => {
    setBusy(true); setError("");
    try { await (permanent ? materialAssets.purge(id) : materialAssets.restore(id)); setPurge(null); setData(await materialAssets.trash()); await onChanged(); }
    catch (cause) { setError(errorMessage(cause)); } finally { setBusy(false); }
  };
  return <ConfirmDialog title={t("Recycle bin", "回收站")} description={data ? `${formatBytes(data.usedBytes)} / ${formatBytes(data.maxBytes)}` : "…"}
    cancelLabel={t("Close", "关闭")} confirmLabel={t("Reload", "重读")} loadingLabel={t("Processing…", "处理中…")} submitting={busy}
    error={error ? { title: t("Unable to complete", "未完成"), message: error } : null} onCancel={onClose} onConfirm={() => void materialAssets.trash().then(setData).catch(cause => setError(errorMessage(cause)))}>
    <div className="mw-toolbar" role="tablist" aria-label={t("Folders", "目录")}>
      {(["photos", "configurations"] as const).map(kind => <button type="button" role="tab" key={kind} aria-selected={folder === kind} onClick={() => setFolder(kind)}>{kind === "photos" ? t("Photos", "图片") : t("Configurations", "配置表")} {data?.entries.filter(entry => entry.folder === kind).length ?? 0}</button>)}
    </div>
    <div className="mw-trash-list" role="tabpanel" aria-label={folder === "photos" ? t("Photos", "图片") : t("Configurations", "配置表")}>
      {data?.entries.filter(entry => entry.folder === folder).map(entry => <div className="mw-trash-entry" key={entry.id}><div><strong>{entry.filename}</strong><small>{entry.modelName} · {entry.label} · {formatBytes(entry.bytes)}</small></div>
        <button type="button" disabled={busy} onClick={() => void change(entry.id, false)}>{t("Restore", "恢复")}</button>
        <button type="button" disabled={busy} onClick={() => setPurge(entry)}>{t("Delete permanently", "永久删除")}</button>
      </div>)}
      {data && !data.entries.some(entry => entry.folder === folder) ? <div className="mw-empty">{t("Empty", "空")}</div> : null}
    </div>
    {purge ? <ConfirmDialog title={t("Delete permanently?", "永久删除？")} description={`${purge.filename} · ${t("This cannot be undone.", "此操作不可恢复。")}`} tone="danger"
      cancelLabel={t("Cancel", "取消")} confirmLabel={t("Delete permanently", "永久删除")} loadingLabel="…" submitting={busy}
      error={error ? { title: t("Not deleted", "未删除"), message: error } : null} onCancel={() => setPurge(null)} onConfirm={() => void change(purge.id, true)} /> : null}
  </ConfirmDialog>;
}
