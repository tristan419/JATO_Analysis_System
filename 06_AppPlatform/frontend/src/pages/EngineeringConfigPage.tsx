import { useState, useCallback, useEffect, type DragEvent, type ChangeEvent } from "react";
import { ConfigComparisonTable } from "../components/ConfigComparisonTable";
import { ConfigMatrixEditor } from "../components/ConfigMatrixEditor";
import { ConfigDiffPanel } from "../components/ConfigDiffPanel";
import { PageBannerStack, PageLoadingShell } from "../components/PageFeedback";
import { api } from "../api/client";
import type { VehicleTrimItem, TrimDetail, CompareResponse, ParsePreview, AvailabilityState } from "../types/engineeringConfig";

type TabId = "trims" | "compare" | "matrix" | "upload" | "diff" | "detail";
type UploadStage = "select" | "uploading" | "parsing" | "matching" | "preview" | "confirming" | "done";

const TAB_LABELS: Record<TabId, string> = { trims: "车型列表", compare: "配置对比", matrix: "矩阵编辑", upload: "上传导入", diff: "变更历史", detail: "详情" };
const AVAIL_LABELS: Record<AvailabilityState, string> = { STANDARD: "标配", OPTIONAL: "选装", NOT_AVAILABLE: "不配备", NOT_APPLICABLE: "不适用", VALUE: "参数", UNKNOWN: "未知", CANCELLED_OR_REMOVED: "已停售/移除" };
const AVAIL_CLASSES: Record<AvailabilityState, string> = { STANDARD: "chip-positive", OPTIONAL: "chip-warning", NOT_AVAILABLE: "chip-muted", NOT_APPLICABLE: "chip-muted", VALUE: "chip-info", UNKNOWN: "chip-error", CANCELLED_OR_REMOVED: "chip-muted" };

function getRole() { return localStorage.getItem("jato_user_role") || "viewer"; }

export function EngineeringConfigPage() {
  const role = getRole();
  const [tab, setTab] = useState<TabId>("trims");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Upload
  const [stage, setStage] = useState<UploadStage>("select");
  const [dragActive, setDragActive] = useState(false);
  const [file, setFile] = useState<File | null>(null);
  const [uploadId, setUploadId] = useState<string | null>(null);
  const [progress, setProgress] = useState({ pct: 0, detail: "" });
  const [preview, setPreview] = useState<ParsePreview | null>(null);
  const [fullPrev, setFullPrev] = useState<Record<string, unknown> | null>(null);

  // Trims
  const [trims, setTrims] = useState<VehicleTrimItem[]>([]);
  const [trimFilter, setTrimFilter] = useState({ brand: "", model: "" });
  const [selTrim, setSelTrim] = useState<TrimDetail | null>(null);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [compareData, setCompareData] = useState<CompareResponse | null>(null);
  const [diffOnly, setDiffOnly] = useState(false);

  const tabs: TabId[] = role === "admin" || role === "editor" ? ["trims", "compare", "matrix", "upload", "diff"] : ["trims", "compare"];
  useEffect(() => { if (!tabs.includes(tab)) setTab(tabs[0]); }, [role]);

  // Upload handlers
  const hDrag = (e: DragEvent) => { e.preventDefault(); e.stopPropagation(); };
  const hDragIn = (e: DragEvent) => { e.preventDefault(); e.stopPropagation(); if (e.dataTransfer.items?.length) setDragActive(true); };
  const hDragOut = (e: DragEvent) => { e.preventDefault(); e.stopPropagation(); setDragActive(false); };
  const hDrop = (e: DragEvent) => { e.preventDefault(); e.stopPropagation(); setDragActive(false); const f = e.dataTransfer.files?.[0]; if (f?.name.match(/\.xlsx?$/i)) { setFile(f); setStage("select"); setError(null); } else setError("仅支持 .xlsx / .xlsm / .xls"); };
  const hFile = (e: ChangeEvent<HTMLInputElement>) => { const f = e.target.files?.[0]; if (f) { setFile(f); setStage("select"); setError(null); } };

  const doUpload = async () => {
    if (!file) return; setLoading(true); setError(null);
    try {
      const CHUNK = 5 * 1024 * 1024; const total = Math.ceil(file.size / CHUNK);
      setStage("uploading"); setProgress({ pct: 0, detail: "初始化..." });
      const init = await api.initiateEngineeringConfigUpload(file.name, file.size); const uid = init.uploadId; setUploadId(uid);
      for (let i = 0; i < total; i++) { setProgress({ pct: Math.round((i / total) * 60), detail: `分片 ${i + 1}/${total}` }); await api.uploadEngineeringConfigChunk(uid, i, file.slice(i * CHUNK, (i + 1) * CHUNK)); }
      setProgress({ pct: 70, detail: "组装..." }); await api.completeEngineeringConfigUpload(uid);
      setStage("parsing"); setProgress({ pct: 80, detail: "解析..." });
      const prev = await api.parseEngineeringConfigUpload(uid) as unknown as ParsePreview; setPreview(prev);
      setStage("matching"); setProgress({ pct: 90, detail: "匹配版本..." });
      await api.matchEngineeringConfigUpload(uid);
      const fp = await api.getEngineeringConfigUploadPreview(uid); setFullPrev(fp);
      setStage("preview"); setProgress({ pct: 100, detail: "预览就绪" });
    } catch (err) { setError(err instanceof Error ? err.message : "失败"); setStage("select"); } finally { setLoading(false); }
  };

  const doConfirm = async () => {
    if (!uploadId) return; setLoading(true); setStage("confirming");
    try { await api.confirmEngineeringConfigUpload(uploadId); setStage("done"); setFile(null); setUploadId(null); } catch (err) { setError(err instanceof Error ? err.message : "确认失败"); } finally { setLoading(false); }
  };

  const reset = () => { setFile(null); setUploadId(null); setPreview(null); setFullPrev(null); setStage("select"); setProgress({ pct: 0, detail: "" }); };

  // Trim list
  const loadTrims = useCallback(async () => {
    setLoading(true);
    try { const d = await api.listEngineeringConfigTrims({ brand: trimFilter.brand || undefined, model_name: trimFilter.model || undefined }); setTrims((d.items || []) as unknown as VehicleTrimItem[]); } catch (err) { setError(err instanceof Error ? err.message : "加载失败"); } finally { setLoading(false); }
  }, [trimFilter]);
  useEffect(() => { if (tab === "trims") loadTrims(); }, [tab, loadTrims]);

  const openDetail = async (id: string) => { setLoading(true); try { const d = await api.getEngineeringConfigTrimDetail(id) as unknown as TrimDetail; setSelTrim(d); setTab("detail"); } catch (err) { setError(err instanceof Error ? err.message : "失败"); } finally { setLoading(false); } };
  const toggleCmp = (id: string) => setCompareIds((p) => p.includes(id) ? p.filter((x) => x !== id) : p.length < 4 ? [...p, id] : p);
  const doCompare = useCallback(async () => { if (compareIds.length < 2) return; setLoading(true); try { const d = await api.compareEngineeringConfigTrims(compareIds, diffOnly) as unknown as CompareResponse; setCompareData(d); } catch (err) { setError(err instanceof Error ? err.message : "失败"); } finally { setLoading(false); } }, [compareIds, diffOnly]);
  useEffect(() => { if (compareIds.length >= 2) doCompare(); }, [compareIds, diffOnly]);

  return (
    <section className="crud-shell">
      <div className="admin-tabs">{tabs.map((t) => <button key={t} className={`admin-tab ${tab === t ? "admin-tab-active" : ""}`} onClick={() => setTab(t)}>{TAB_LABELS[t]}</button>)}</div>
      <PageBannerStack
        items={[
          ...(error ? [{
            id: "engineering-config-error",
            tone: "error" as const,
            title: "工程配置加载失败",
            message: error,
            action: <button className="btn btn-sm" onClick={() => setError(null)}>×</button>,
          }] : []),
        ]}
      />
      {loading && <PageLoadingShell label="加载中" kicker="Engineering" />}

      {!loading && tab === "trims" && <div className="card"><div className="card-header">车型列表</div><div className="card-body">
        <div className="filter-bar"><input className="input" placeholder="品牌" value={trimFilter.brand} onChange={(e) => setTrimFilter((f) => ({ ...f, brand: e.target.value }))} /><button className="btn btn-secondary" onClick={loadTrims}>刷新</button></div>
        <div style={{ marginTop: 8 }}><span className="text-muted">对比: {compareIds.length}/4</span>{compareIds.length >= 2 && <button className="btn btn-accent" style={{ marginLeft: 8 }} onClick={() => setTab("compare")}>开始对比</button>}</div>
        {trims.map((t) => <div key={t.trimId} className={`trim-card ${compareIds.includes(t.trimId) ? "trim-card-selected" : ""}`} style={{ marginTop: 8, padding: 8, border: "1px solid #ddd", borderRadius: 6 }}>
          <span style={{ cursor: "pointer", fontWeight: 600 }} onClick={() => openDetail(t.trimId)}>{t.fullTrimName}</span> <span className="chip">{t.brand}</span>
          <button className={`btn btn-sm ${compareIds.includes(t.trimId) ? "btn-accent" : "btn-secondary"}`} style={{ float: "right" }} onClick={() => toggleCmp(t.trimId)}>{compareIds.includes(t.trimId) ? "取消" : "对比"}</button>
        </div>)}
      </div></div>}

      {!loading && tab === "compare" && <div className="card"><div className="card-header">配置对比 <label className="toggle-label" style={{ marginLeft: 16 }}><input type="checkbox" checked={diffOnly} onChange={(e) => setDiffOnly(e.target.checked)} />只看差异</label></div><div className="card-body">{compareData ? <ConfigComparisonTable data={compareData} /> : <p className="text-muted">请选 2-4 个车型</p>}</div></div>}

      {tab === "matrix" && <ConfigMatrixEditor />}
      {tab === "diff" && <ConfigDiffPanel />}

      {!loading && tab === "upload" && <div className="card"><div className="card-header">上传导入</div><div className="card-body">
        <div className="upload-stages" style={{ display: "flex", gap: 8, marginBottom: 16 }}>
          {["选择文件", "上传解析", "版本匹配", "预览确认", "完成"].map((l, i) => { const idx = i === 0 ? "select" : i === 3 ? "preview" : "mid"; const active = (stage === "select" && i === 0) || (["uploading", "parsing", "matching"].includes(stage) && i >= 1 && i <= 2) || (stage === "preview" && i === 3) || (stage === "done" && i === 4); return <span key={i} className={`upload-stage-chip ${active ? "stage-active" : ""}`}>{i + 1}. {l}</span>; })}
        </div>
        {stage === "select" && <><div className={`dropzone ${dragActive ? "dropzone-active" : ""} ${file ? "dropzone-has-file" : ""}`} onDragEnter={hDragIn} onDragLeave={hDragOut} onDragOver={hDrag} onDrop={hDrop} onClick={() => document.getElementById("ec-fi")?.click()} tabIndex={0} role="button">{file ? <div><span>{file.name}</span><span>{(file.size / 1024 / 1024).toFixed(2)} MB</span></div> : <div><span>拖放 Excel 文件</span><span className="dropzone-hint">.xlsx / .xlsm / .xls</span></div>}</div><input id="ec-fi" type="file" accept=".xlsx,.xlsm,.xls" style={{ display: "none" }} onChange={hFile} />{file && <button className="btn btn-primary" style={{ marginTop: 12 }} onClick={doUpload}>上传并解析</button>}</>}
        {progress.pct > 0 && stage !== "preview" && stage !== "done" && <div style={{ marginTop: 12 }}><div className="progress-bar-bg"><div className="progress-bar-fill" style={{ width: `${progress.pct}%` }} /></div><span>{progress.detail}</span></div>}
        {stage === "preview" && fullPrev && <div><h4>导入预览</h4>
          {(() => { const s = fullPrev.summary as Record<string, number>; return <div className="summary-chips"><span className="chip chip-positive">{s?.trimCount || 0} 车型</span><span className="chip chip-info">{s?.newTrims || 0} 新增</span><span className="chip chip-warning">{s?.existingTrims || 0} 已有</span><span className="chip chip-neutral">{s?.changedValues || 0} 变更</span></div>; })()}
          {((fullPrev.diffRows as unknown[]) || []).length > 0 && <details open><summary>变更详情</summary><table className="data-table"><thead><tr><th>配置项</th><th>旧值</th><th>新值</th></tr></thead><tbody>{((fullPrev.diffRows || []) as unknown[]).filter((d: unknown) => (d as Record<string, unknown>).diffType !== "UNCHANGED").slice(0, 100).map((d: unknown, i: number) => { const dr = d as Record<string, unknown>; return <tr key={i}><td>{dr.featureName as string}</td><td className="diff-old">{dr.oldValue as string || "-"}</td><td className="diff-new">{dr.newValue as string || "-"}</td></tr>; })}</tbody></table></details>}
          <div style={{ marginTop: 16, display: "flex", gap: 8 }}><button className="btn btn-primary" onClick={doConfirm}>{loading ? "..." : "确认导入为 Draft"}</button><button className="btn btn-secondary" onClick={reset}>重来</button></div>
        </div>}
        {stage === "done" && <div><span className="chip chip-positive">导入完成</span><p>已保存为 Draft，等待发布。</p><button className="btn btn-primary" onClick={reset}>继续上传</button></div>}
      </div></div>}

      {!loading && tab === "detail" && selTrim && <div className="card"><div className="card-header">{selTrim.trim.fullTrimName} <button className="btn btn-sm btn-secondary" style={{ marginLeft: 12 }} onClick={() => setTab("trims")}>返回</button></div><div className="card-body">{Object.entries(selTrim.featuresByCategory).map(([cat, fvs]) => <details key={cat} open><summary>{cat} ({fvs.length})</summary><table className="data-table"><thead><tr><th>配置项</th><th>值</th><th>状态</th></tr></thead><tbody>{fvs.map((fv) => <tr key={fv.valueId}><td>{fv.featureName}</td><td>{fv.rawValue || "-"}</td><td><span className={`chip ${AVAIL_CLASSES[fv.availability]}`}>{AVAIL_LABELS[fv.availability]}</span></td></tr>)}</tbody></table></details>)}</div></div>}

    </section>
  );
}
