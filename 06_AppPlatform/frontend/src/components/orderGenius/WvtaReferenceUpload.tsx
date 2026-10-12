import { useState } from "react";
import { api } from "../../api/client";
import { ConfirmDialog, type ConfirmDialogError } from "../ConfirmDialog";

export function WvtaReferenceUpload({ onSaved }: { onSaved: () => void }) {
  const [file, setFile] = useState<File | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<ConfirmDialogError | null>(null);
  const [message, setMessage] = useState("");
  async function save(): Promise<void> {
    if (!file || submitting) return;
    setSubmitting(true); setError(null); setMessage("");
    try {
      const digest = await crypto.subtle.digest("SHA-256", await file.arrayBuffer());
      const sha256 = Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, "0")).join("");
      const uploadId = await api.cocFillUploadFile(file, `wvta-reference-${sha256}`);
      const result = await api.setWvtaReference(uploadId, sha256);
      setMessage(`Reference saved / 参考已保存：${result.recordCount} records · ${result.cancelledCount} cancelled / 作废 · ${result.reviewCount} review / 待核对`);
      setFile(null); onSaved();
    } catch (cause) {
      setError({ title: "Reference save not confirmed / 参考保存未确认", message: cause instanceof Error ? cause.message : "Upload or parsing failed; reload to check / 上传或解析失败，可重读核对" });
    } finally { setSubmitting(false); }
  }
  return <div className="og-wvta-upload">
    <label>Replace WVTA reference PDF / 更换WVTA参考PDF
      <input type="file" accept=".pdf" disabled={submitting} onChange={(event) => {
        const chosen = event.currentTarget.files?.[0];
        if (chosen?.name.toLowerCase().endsWith(".pdf")) { setFile(chosen); setError(null); }
        event.currentTarget.value = "";
      }} />
    </label>
    {message ? <span role="status">{message}</span> : null}
    {file ? <ConfirmDialog title="Replace WVTA reference / 更换WVTA参考" description={<>
      <p>{file.name}</p><p>Shared reference only. Matching authorized materials can read certificate segments. BOM dates, links, prices and PI remain unchanged / 仅更换共享参考；只展示授权物料的证书分段，不改BOM日期、关联、价格或PI。</p>
      <p>Cancelled or uncertain source rows are evidence, not usable certificate periods / 作废或无法可靠配对的源行仅供核对，不作为有效证书期。</p>
    </>} cancelLabel="Cancel / 取消" confirmLabel="Upload reference / 上传参考" loadingLabel="Uploading and parsing / 上传解析中…"
      submitting={submitting} error={error} onCancel={() => setFile(null)} onConfirm={() => { void save(); }} /> : null}
  </div>;
}
