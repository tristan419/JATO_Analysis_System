import { useState, useEffect, useRef, useCallback, type DragEvent } from "react";
import { api } from "../api/client";
import type { CocMatchJob } from "../types";

/* ── Helpers ───────────────────────────────────── */

function formatTs(ts: string | null | undefined): string {
  if (!ts) return "-";
  const d = new Date(ts);
  return d.toLocaleString("zh-CN", { hour12: false });
}

function statusLabel(s: string): string {
  const m: Record<string, string> = {
    queued: "排队中",
    running: "运行中",
    success: "已完成",
    failed: "失败",
  };
  return m[s] ?? s;
}

function statusColor(s: string): string {
  if (s === "success") return "#16a34a";
  if (s === "failed") return "#dc2626";
  if (s === "running") return "#2563eb";
  return "#6b7280";
}

/* ── Dropzone component ────────────────────────── */

function Dropzone({
  accept,
  hint,
  file,
  onFile,
}: {
  accept: string;
  hint: string;
  file: File | null;
  onFile: (f: File) => void;
}) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const handleDrop = useCallback(
    (e: DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      setDragging(false);
      const f = e.dataTransfer.files[0];
      if (f) onFile(f);
    },
    [onFile]
  );

  return (
    <div
      className={`dropzone ${file ? "has-file" : ""} ${dragging ? "dragover" : ""}`}
      onClick={() => inputRef.current?.click()}
      onDragOver={(e) => { e.preventDefault(); setDragging(true); }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      style={{
        border: `2px dashed ${file ? "#16a34a" : dragging ? "#2563eb" : "#d1d5db"}`,
        borderRadius: 12, padding: "24px 16px", textAlign: "center",
        cursor: "pointer", background: file ? "#f0fdf4" : dragging ? "#eff6ff" : "#fafafa",
        transition: "all 0.2s", flex: 1,
      }}
    >
      <div style={{ fontSize: 28, marginBottom: 6 }}>{accept.includes("zip") ? "📦" : "📊"}</div>
      <div style={{ fontSize: 13, color: "#888" }}>{hint}</div>
      {file && <div style={{ fontSize: 12, color: "#16a34a", fontWeight: 600, marginTop: 4 }}>{file.name}</div>}
      <input ref={inputRef} type="file" accept={accept} hidden
        onChange={(e) => { const f = e.target.files?.[0]; if (f) onFile(f); }} />
    </div>
  );
}

/* ── Main page ─────────────────────────────────── */

export function CocMatchPage() {
  const [excelFile, setExcelFile] = useState<File | null>(null);
  const [archiveFile, setArchiveFile] = useState<File | null>(null);
  const [country, setCountry] = useState("");
  const [month, setMonth] = useState(() => {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
  });
  const [fileExt, setFileExt] = useState<".pdf" | ".xml">(".pdf");
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [currentJob, setCurrentJob] = useState<CocMatchJob | null>(null);
  const [jobList, setJobList] = useState<CocMatchJob[]>([]);
  const [pollId, setPollId] = useState<string | null>(null);
  const pollingRef = useRef(false);

  // Load job list on mount
  useEffect(() => {
    api.cocMatchListJobs(20).then((res) => {
      setJobList(res.items as unknown as CocMatchJob[]);
    }).catch(() => {});
  }, []);

  // Refresh job list when a job finishes
  const refreshJobs = useCallback(() => {
    api.cocMatchListJobs(20).then((res) => {
      setJobList(res.items as unknown as CocMatchJob[]);
    }).catch(() => {});
  }, []);

  // Poll job status
  useEffect(() => {
    if (!pollId) return;
    pollingRef.current = true;
    let cancelled = false;

    const poll = async () => {
      while (pollingRef.current && !cancelled) {
        await new Promise((r) => setTimeout(r, 3000));
        if (cancelled || !pollingRef.current) break;
        try {
          const res = await api.cocMatchGetJob(pollId);
          setCurrentJob(res.item);
          if (res.item.status === "success" || res.item.status === "failed") {
            pollingRef.current = false;
            setPollId(null);
            refreshJobs();
          }
        } catch {
          pollingRef.current = false;
          setPollId(null);
        }
      }
    };
    poll();

    return () => { cancelled = true; pollingRef.current = false; };
  }, [pollId, refreshJobs]);

  const [uploadDetail, setUploadDetail] = useState<string | null>(null);

  const handleUpload = async () => {
    if (!excelFile || !archiveFile) return;
    if (!country.trim()) { setError("请输入国家代码"); return; }

    setUploading(true);
    setError(null);
    setCurrentJob(null);

    // Check if chunked upload is needed
    const isLarge = excelFile.size >= 50 * 1024 * 1024 || archiveFile.size >= 50 * 1024 * 1024;
    setUploadDetail(isLarge
      ? `文件较大（> 50MB），使用分片上传...`
      : null
    );

    try {
      const res = await api.cocMatchUploadAndCreateJob(
        excelFile, archiveFile, country.toUpperCase(), fileExt,
        month || undefined,
      );
      setCurrentJob(res.item);
      setPollId(res.item.jobId);
      setUploadDetail(null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "上传失败");
      setUploadDetail(null);
    } finally {
      setUploading(false);
    }
  };

  const handleRetry = async (jobId: string) => {
    try {
      const res = await api.cocMatchRetryJob(jobId);
      setCurrentJob(res.item);
      setPollId(res.item.jobId);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "重试失败");
    }
  };

  const isReady = excelFile && archiveFile && country.trim();

  return (
    <div style={{ padding: 24, maxWidth: 900, margin: "0 auto" }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>COC 比对工具</h1>
      <p style={{ color: "#888", fontSize: 14, marginBottom: 24 }}>
        上传 Excel 注册表和 ZIP/RAR 压缩包，自动比对 COC 文件匹配情况
      </p>

      {/* ── Upload Form ── */}
      <div style={{
        background: "white", borderRadius: 12, padding: 24, marginBottom: 24,
        boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
      }}>
        <div style={{ display: "flex", gap: 16, marginBottom: 20 }}>
          <Dropzone accept=".xlsx,.xlsm,.xls" hint="拖拽 / 点击选择 Excel 注册表"
            file={excelFile} onFile={setExcelFile} />
          <Dropzone accept=".zip,.rar" hint="拖拽 / 点击选择 ZIP/RAR 压缩包"
            file={archiveFile} onFile={setArchiveFile} />
        </div>

        <div style={{ display: "flex", gap: 16, marginBottom: 20, alignItems: "flex-end" }}>
          <div style={{ flex: 1 }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#555", marginBottom: 4, display: "block" }}>
              国家代码
            </label>
            <input type="text" placeholder="如 CZ, SK, HU" value={country}
              onChange={(e) => setCountry(e.target.value.toUpperCase())}
              maxLength={10}
              style={{
                width: "100%", padding: "10px 12px", border: "1px solid #d1d5db",
                borderRadius: 8, fontSize: 14,
              }} />
          </div>
          <div style={{ flex: 1 }}>
            <label style={{ fontSize: 12, fontWeight: 600, color: "#555", marginBottom: 4, display: "block" }}>
              月份（可选）
            </label>
            <input type="month" value={month}
              onChange={(e) => setMonth(e.target.value)}
              style={{
                width: "100%", padding: "10px 12px", border: "1px solid #d1d5db",
                borderRadius: 8, fontSize: 14,
              }} />
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            <label style={{
              padding: "10px 18px", border: `2px solid ${fileExt === ".pdf" ? "#2563eb" : "#e5e7eb"}`,
              borderRadius: 8, cursor: "pointer", fontSize: 14, fontWeight: 600,
              background: fileExt === ".pdf" ? "#eff6ff" : "white", color: fileExt === ".pdf" ? "#2563eb" : "#333",
            }}>
              <input type="radio" name="ext" value=".pdf" checked={fileExt === ".pdf"}
                onChange={() => setFileExt(".pdf")} style={{ display: "none" }} />
              PDF
            </label>
            <label style={{
              padding: "10px 18px", border: `2px solid ${fileExt === ".xml" ? "#2563eb" : "#e5e7eb"}`,
              borderRadius: 8, cursor: "pointer", fontSize: 14, fontWeight: 600,
              background: fileExt === ".xml" ? "#eff6ff" : "white", color: fileExt === ".xml" ? "#2563eb" : "#333",
            }}>
              <input type="radio" name="ext" value=".xml" checked={fileExt === ".xml"}
                onChange={() => setFileExt(".xml")} style={{ display: "none" }} />
              XML
            </label>
          </div>
        </div>

        <button onClick={handleUpload} disabled={!isReady || uploading}
          style={{
            width: "100%", padding: 14, border: "none", borderRadius: 10,
            fontSize: 16, fontWeight: 600, cursor: isReady && !uploading ? "pointer" : "not-allowed",
            background: isReady && !uploading ? "#2563eb" : "#9ca3af", color: "white",
          }}>
          {uploading ? "上传中..." : "开始比对"}
        </button>

        {error && (
          <div style={{ marginTop: 16, padding: 12, borderRadius: 8, background: "#fef2f2", color: "#dc2626", fontSize: 14 }}>
            {error}
          </div>
        )}

        {uploadDetail && (
          <div style={{ marginTop: 16, padding: 12, borderRadius: 8, background: "#eff6ff", fontSize: 14 }}>
            {uploadDetail}
          </div>
        )}

        {/* Current job progress */}
        {currentJob && currentJob.status !== "success" && currentJob.status !== "failed" && (
          <div style={{ marginTop: 16, padding: 16, borderRadius: 8, background: "#eff6ff", fontSize: 14 }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <div style={{
                width: 12, height: 12, borderRadius: "50%", background: "#2563eb",
                animation: currentJob.status === "running" ? "pulse 1s infinite" : "none",
              }} />
              <span style={{ fontWeight: 600 }}>
                {currentJob.status === "queued" ? "任务已排队，等待处理..." : "正在比对中..."}
              </span>
            </div>
          </div>
        )}

        {/* Success state */}
        {currentJob?.status === "success" && (
          <div style={{ marginTop: 16, padding: 16, borderRadius: 8, background: "#f0fdf4", fontSize: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <strong style={{ color: "#16a34a" }}>比对完成</strong>
                <span style={{ color: "#666", marginLeft: 8 }}>
                  共 {currentJob.totalRows} 条 · 匹配 {currentJob.matchedCount} · 缺失 {currentJob.missingCount} · 覆盖率 {currentJob.coverageRate}%
                </span>
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <a href={`/v1/coc-match/jobs/${currentJob.jobId}/report`} target="_blank" rel="noopener noreferrer"
                  style={{
                    padding: "8px 20px", background: "#16a34a", color: "white", borderRadius: 8,
                    textDecoration: "none", fontWeight: 600, fontSize: 14,
                  }}>
                  查看报告
                </a>
                <a href={`/v1/coc-match/jobs/${currentJob.jobId}/report?download=1`}
                  style={{
                    padding: "8px 20px", background: "white", color: "#16a34a", borderRadius: 8,
                    border: "2px solid #16a34a", textDecoration: "none", fontWeight: 600, fontSize: 14,
                  }}>
                  下载报告
                </a>
              </div>
            </div>
          </div>
        )}

        {/* Failed state */}
        {currentJob?.status === "failed" && (
          <div style={{ marginTop: 16, padding: 16, borderRadius: 8, background: "#fef2f2", fontSize: 14 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <div>
                <strong style={{ color: "#dc2626" }}>比对失败</strong>
                {currentJob.error && <span style={{ color: "#666", marginLeft: 8 }}>{currentJob.error}</span>}
              </div>
              <button onClick={() => handleRetry(currentJob.jobId)}
                style={{
                  padding: "8px 20px", background: "#dc2626", color: "white", border: "none",
                  borderRadius: 8, fontWeight: 600, fontSize: 14, cursor: "pointer",
                }}>
                重试
              </button>
            </div>
          </div>
        )}
      </div>

      {/* ── Job List ── */}
      <div style={{
        background: "white", borderRadius: 12, overflow: "hidden",
        boxShadow: "0 1px 3px rgba(0,0,0,0.08)",
      }}>
        <div style={{ padding: "16px 24px", borderBottom: "1px solid #eee", fontWeight: 600 }}>
          COC 比对记录
        </div>
        {jobList.length === 0 ? (
          <div style={{ padding: "24px", textAlign: "center", color: "#999", fontSize: 14 }}>
            暂无比对记录
          </div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead>
                <tr>
                  <th style={thStyle}>国家</th>
                  <th style={thStyle}>月份</th>
                  <th style={thStyle}>状态</th>
                  <th style={thStyle}>总行数</th>
                  <th style={thStyle}>匹配</th>
                  <th style={thStyle}>缺失</th>
                  <th style={thStyle}>覆盖率</th>
                  <th style={thStyle}>创建时间</th>
                  <th style={thStyle}>操作</th>
                </tr>
              </thead>
              <tbody>
                {jobList.map((job) => (
                  <tr key={job.jobId} style={{ borderBottom: "1px solid #f0f0f0" }}>
                    <td style={tdStyle}>{job.country}</td>
                    <td style={tdStyle}>{job.month}</td>
                    <td style={{ ...tdStyle, color: statusColor(job.status), fontWeight: 600 }}>
                      {statusLabel(job.status)}
                    </td>
                    <td style={tdStyle}>{job.totalRows ?? "-"}</td>
                    <td style={tdStyle}>{job.matchedCount ?? "-"}</td>
                    <td style={tdStyle}>{job.missingCount ?? "-"}</td>
                    <td style={tdStyle}>
                      {job.coverageRate != null ? `${job.coverageRate}%` : "-"}
                    </td>
                    <td style={tdStyle}>{formatTs(job.createdAt)}</td>
                    <td style={tdStyle}>
                      {job.status === "success" && (
                        <span style={{ fontSize: 13, whiteSpace: "nowrap" }}>
                          <a href={`/v1/coc-match/jobs/${job.jobId}/report`} target="_blank" rel="noopener noreferrer"
                            style={{ color: "#2563eb", cursor: "pointer", textDecoration: "underline" }}>
                            查看
                          </a>
                          <span style={{ color: "#d1d5db", margin: "0 4px" }}>|</span>
                          <a href={`/v1/coc-match/jobs/${job.jobId}/report?download=1`}
                            style={{ color: "#16a34a", cursor: "pointer", textDecoration: "underline" }}>
                            下载
                          </a>
                        </span>
                      )}
                      {job.status === "failed" && (
                        <button onClick={() => handleRetry(job.jobId)}
                          style={{ color: "#dc2626", fontSize: 13, textDecoration: "underline", background: "none", border: "none", cursor: "pointer" }}>
                          重试
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Pulse keyframes */}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.4; }
        }
      `}</style>
    </div>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: "left", padding: "10px 16px", fontSize: 12, fontWeight: 600,
  color: "#666", background: "#fafafa", textTransform: "uppercase",
  whiteSpace: "nowrap",
};

const tdStyle: React.CSSProperties = {
  padding: "10px 16px", fontSize: 14, whiteSpace: "nowrap",
};
