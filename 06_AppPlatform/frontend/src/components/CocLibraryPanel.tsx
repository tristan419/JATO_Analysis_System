import { useEffect, useState } from "react";
import { api } from "../api/client";
import { FileDropzone } from "./upload/FileDropzone";
import type { CocLibraryState, CocSourcePreview, CocDeletePreview } from "../types/cocLibrary";

function memorySize(bytes: number | null | undefined): string {
  return bytes == null ? "Unavailable / 无数据" : `${(bytes / 1024 ** 2).toFixed(1)} MiB`;
}

export function CocLibraryPanel() {
  const [library, setLibrary] = useState<CocLibraryState | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<CocSourcePreview | null>(null);
  const [deletion, setDeletion] = useState<CocDeletePreview | null>(null);
  const [replacements, setReplacements] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const indexing = library?.items.some((s) => s.status === "indexing" && s.job.status !== "failed") ?? false;

  async function reload(): Promise<void> { setLibrary(await api.cocLibrary()); }
  async function action(work: () => Promise<void>): Promise<void> {
    setBusy(true); setError("");
    try { await work(); await reload(); }
    catch { setError("Could not complete this action. Refresh and preview again; contact admin if indexing failed. / 操作未完成，请刷新后重新预览；索引失败请联系管理员检查来源包及服务日志。"); }
    finally { setBusy(false); }
  }
  useEffect(() => {
    let active = true;
    const refresh = (): void => { void api.cocLibrary().then((state) => { if (active) setLibrary(state); }).catch(() => { if (active) setError("Cannot read library; refresh or contact admin / 无法读取在线库，请刷新或联系管理员"); }); };
    refresh();
    const timer = indexing ? window.setInterval(refresh, 2500) : null;
    return () => { active = false; if (timer !== null) window.clearInterval(timer); };
  }, [indexing]);

  return <section style={{ display: "grid", gap: 12 }}>
    <p>One shared library · {library?.vinCount ?? 0} VINs / 一个共享库。支持追加嵌套 ZIP/RAR；按 PDF 文件名匹配 VIN，不读取正文。</p>
    {library?.configured ? <p>Library disk / 库磁盘 {memorySize(library.libraryBytes)} · Disk free / 磁盘可用 {memorySize(library.diskFreeBytes)}。每次只索引一个来源；原包及 PDF 缓存保留，中间文件完成或失败后清理。</p> : null}
    {library && !library.configured ? <p role="alert">Persistent storage not configured / 在线库持久目录未配置，请联系管理员。</p> : null}
    {error ? <div role="alert">{error} <button type="button" disabled={busy} onClick={() => void action(reload)}>Refresh / 刷新</button></div> : null}
    {notice ? <p role="status">{notice}</p> : null}
    <FileDropzone accept=".zip,.rar" label="Append source / 追加来源包" hint="ZIP / RAR · nested archives supported / 支持多层嵌套" file={file} onFile={setFile} onClear={() => setFile(null)} />
    <button type="button" disabled={busy || indexing || !file || !library?.configured} onClick={() => void action(async () => {
      if (!file) return;
      const id = await api.cocUploadFile(file, (done, count) => setNotice(`Uploading ${done}/${count} / 正在上传`));
      const result = await api.cocLibraryImport(id).catch(async (reason: unknown) => {
        await api.cocDiscardUpload(id).catch(() => undefined);
        throw reason;
      });
      setFile(null); setNotice(result.needsReview ? "Existing source reused; preview to enable missing VINs / 已复用原包及缓存，请预览确认重新启用 VIN" : result.duplicate ? "Source already exists / 来源包已存在，未重复导入" : "Indexing; preview when ready / 正在索引，完成后请预览确认");
    })}>Upload & index / 上传并索引</button>
    <div style={{ maxHeight: 300, overflow: "auto" }}>
      {library?.items.map((source) => <div key={source.id} style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", padding: "8px 0", borderBottom: "1px solid #dbe6f4" }}>
        <span style={{ flex: 1 }}>{source.filename} · {source.job.pdfCount ?? source.pdfCount} PDF · {({ indexing: "Indexing / 索引中", review: "Review / 待确认", active: "Active / 可查找", failed: "Failed / 失败" })[source.status]}</span>
        {source.resources ? <small>Index RAM / 索引内存 {source.resources.status === "finished" ? "Stopped / 已结束" : memorySize(source.resources.rssBytes)} · Peak / 峰值 {memorySize(source.resources.peakRssBytes)} · Warning / 告警 {memorySize(source.resources.rssWarningBytes)} · Limit / 上限 {memorySize(source.resources.rssLimitBytes)}</small> : null}
        {source.job.invalidCount ? <small>{source.job.invalidCount} invalid VIN filenames / 名称待处理，未入索引；请检查原包后重传。</small> : null}
        {source.status === "failed" || source.job.status === "failed" ? <small>Indexing stopped; original source retained. Check memory/disk limits and archive decoder, delete and retry a smaller ZIP / 索引停止，原包保留；请检查内存、磁盘余量和解包工具，删除后改传较小 ZIP。</small> : null}
        {source.status === "review" ? <button type="button" disabled={busy} onClick={() => void action(async () => { setPreview(await api.cocLibraryPreview(source.id)); setDeletion(null); setReplacements(new Set()); })}>Preview / 预览</button> : null}
        <button type="button" disabled={busy || (source.status === "indexing" && source.job.status !== "failed")} onClick={() => void action(async () => { setDeletion(await api.cocLibraryDeletePreview(source.id)); setPreview(null); })}>Delete source / 删除来源</button>
      </div>)}
    </div>
    {preview ? <section>
      <p>{preview.items.filter((r) => r.status === "new").length} new / 新增 · {preview.items.filter((r) => r.status === "duplicate").length} duplicate / 重复 · {preview.items.filter((r) => r.status === "conflict").length} conflicts / 冲突</p>
      <p>Different PDF replacement may affect {preview.affectedVehicles} vehicles in {preview.affectedPis.length} PIs / 不同内容替换可能影响这些 PI 的 COC：{preview.affectedPis.join(", ") || "—"}。未勾选冲突保留原 PDF。</p>
      <div style={{ maxHeight: 250, overflow: "auto" }}>{preview.items.filter((row) => row.status === "conflict" || row.status === "invalid").map((row) => <label key={row.vin} style={{ display: "block" }}>
        <input type="checkbox" disabled={row.status === "invalid" || busy} checked={replacements.has(row.vin)} onChange={(event) => setReplacements((old) => { const next = new Set(old); if (event.target.checked) next.add(row.vin); else next.delete(row.vin); return next; })} /> {row.vin} · {row.status === "invalid" ? "Different PDFs within source; repack / 包内冲突，请整理重传" : `Replace / 替换 ${row.oldSha?.slice(0, 8)} → ${row.newSha.slice(0, 8)}`}
      </label>)}</div>
      <button type="button" disabled={busy || preview.items.some((r) => r.status === "invalid")} onClick={() => void action(async () => { await api.cocLibraryActivate(preview, [...replacements]); setPreview(null); setNotice("Library updated / 在线库已更新；PI 页请重新查库"); })}>Confirm append{replacements.size ? ` + replace ${replacements.size}` : ""} / 确认追加及所选替换</button>
      <button type="button" onClick={() => setPreview(null)}>Cancel / 取消</button>
    </section> : null}
    {deletion ? <section role="alert">
      <p>Delete entire source? {deletion.lostCount} VINs will lose PDF access; {deletion.affectedVehicles} vehicles / 删除整包后，这些 VIN 将缺 PDF。影响 PI：{deletion.affectedPis.join(", ") || "—"}。不会恢复旧的不同内容 PDF；VIN 和 PI 不删除。</p>
      <button type="button" disabled={busy} onClick={() => void action(async () => { await api.cocLibraryDelete(deletion); setDeletion(null); setNotice("Source removed / 来源包及索引已删除，无法直接撤销；可重新上传原包"); })}>Confirm delete / 确认删除</button>
      <button type="button" onClick={() => setDeletion(null)}>Cancel / 取消</button>
    </section> : null}
  </section>;
}
