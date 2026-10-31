import { useEffect, useRef, useState } from "react";
import { ConfirmDialog } from "../ConfirmDialog";
import { materialAssets, saveAssetBlob } from "../../api/materialAssets";
import type { AssetGroup } from "../../types/materialAssets";
import type { AssetText } from "./materialAssetUi";
import { errorMessage } from "./materialAssetUi";
import { useAssetImage } from "./materialAssetImage";
interface Props { group: AssetGroup; scope: string; t: AssetText; onClose: () => void }
export function MaterialPhotoViewer({ group, scope, t, onClose }: Props) {
  const [index, setIndex] = useState(0), [busy, setBusy] = useState(false), [error, setError] = useState("");
  const file = group.files[index];
  const image = useAssetImage(file?.id, scope);
  const download = async (): Promise<void> => {
    setBusy(true); setError("");
    try { saveAssetBlob(await materialAssets.download(group.files.map(f => f.id)), `${group.name}.zip`); }
    catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
  return <ConfirmDialog title={group.name} description={`${group.modelName} · ${group.colourKey.replace(/^(code|name):/, "")} · ${index + 1}/${group.files.length}`} cancelLabel={t("Close", "关闭")} confirmLabel={t("Download originals", "下载原图")} loadingLabel={t("Downloading…", "下载中…")} submitting={busy} confirmDisabled={!file} error={error ? { title: t("Unable to load", "读取失败"), message: error } : null} onCancel={onClose} onConfirm={() => void download()}>
    <div className="mw-photo-stage">{image.url ? <img src={image.url} alt={`${group.name} · ${file?.filename ?? ""}`} /> : <span role={image.error ? "alert" : "status"}>{image.error || (!file ? t("No photos", "暂无图片") : t("Loading…", "读取中…"))}</span>}</div>
    <div className="mw-toolbar"><button type="button" disabled={index === 0} onClick={() => setIndex(i => i - 1)}>←</button><span>{file?.filename}</span><button type="button" disabled={index >= group.files.length - 1} onClick={() => setIndex(i => i + 1)}>→</button></div>
  </ConfirmDialog>;
}

export function MaterialPhotoThumbnail({ group, scope, onClick }: { group: AssetGroup; scope: string; onClick: () => void }) {
  const element = useRef<HTMLButtonElement>(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const node = element.current; if (!node) return;
    if (typeof IntersectionObserver === "undefined") { setVisible(true); return; }
    const observer = new IntersectionObserver(entries => { if (entries.some(entry => entry.isIntersecting)) { setVisible(true); observer.disconnect(); } });
    observer.observe(node); return () => observer.disconnect();
  }, []);
  const image = useAssetImage(group.files[0]?.id, scope, visible);
  return <button type="button" ref={element} className="mw-gallery-tile" onClick={onClick} title={image.error || group.name} aria-label={`${group.name} · ${group.files.length}`}>
    {image.url ? <img src={image.url} alt={group.name} /> : <span>▧</span>}<strong>{group.name}</strong><small>{group.files.length}</small>
  </button>;
}
