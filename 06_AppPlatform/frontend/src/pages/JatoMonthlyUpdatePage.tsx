import { ChangeEvent, DragEvent, FormEvent, KeyboardEvent, useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { AdminToolsNav } from "../components/AdminToolsNav";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  JatoMonthlyUpdateConflictSample,
  JatoMonthlyUpdateCleanupResult,
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateReviewBundle,
  JatoMonthlyUpdateUploadProgress,
} from "../types";
import {
  buildMonthlyUpdateArtifactEntries,
  formatMonthlyUpdateFileSize,
  formatMonthlyUpdateNumber,
  formatMonthlyUpdatePhase,
  formatMonthlyUpdateSeconds,
  formatMonthlyUpdateTimestamp,
  getDefaultMonthlyUpdateMonth,
  getMonthlyUpdateStatusBadgeClass,
  getMonthlyUpdateUploadStageLabel,
  isMonthlyUpdateUploadFilenameAccepted,
  shouldPollMonthlyUpdateJobs,
} from "../utils/jatoMonthlyUpdate";

function formatReviewMetricValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (Array.isArray(value)) {
    return value.map((item) => formatReviewMetricValue(item)).join(", ");
  }
  if (typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([key, item]) => `${key}: ${formatReviewMetricValue(item)}`)
      .join(" · ");
  }
  return String(value);
}

function formatReviewMetrics(metrics: Record<string, unknown>): string {
  const entries = Object.entries(metrics);
  if (entries.length === 0) {
    return "-";
  }
  return entries
    .map(([key, value]) => `${key}: ${formatReviewMetricValue(value)}`)
    .join(" · ");
}

function formatConflictSampleBusinessKey(
  businessKey: JatoMonthlyUpdateConflictSample["businessKey"]
): string {
  const entries = Object.entries(businessKey);
  if (entries.length === 0) {
    return "-";
  }
  return entries.map(([key, value]) => `${key}: ${formatReviewMetricValue(value)}`).join(" · ");
}

function formatDigestPreview(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  return value.length > 24 ? `${value.slice(0, 10)}...${value.slice(-8)}` : value;
}

export function JatoMonthlyUpdatePage() {
  const [jobs, setJobs] = useState<JatoMonthlyUpdateJob[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<JatoMonthlyUpdateJob | null>(null);
  const [jobsLoading, setJobsLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [cleanupRunning, setCleanupRunning] = useState(false);
  const [retryingJobId, setRetryingJobId] = useState<string | null>(null);
  const [publishingJobId, setPublishingJobId] = useState<string | null>(null);
  const [reviewLoadingJobId, setReviewLoadingJobId] = useState<string | null>(null);
  const [reviewBundle, setReviewBundle] = useState<JatoMonthlyUpdateReviewBundle | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [cleanupResult, setCleanupResult] =
    useState<JatoMonthlyUpdateCleanupResult | null>(null);
  const [uploadProgress, setUploadProgress] =
    useState<JatoMonthlyUpdateUploadProgress | null>(null);
  const [month, setMonth] = useState(getDefaultMonthlyUpdateMonth());
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const refreshJobs = useCallback(async (preferredJobId?: string, silent = false) => {
    if (!silent) {
      setJobsLoading(true);
    }
    try {
      const response = await api.listJatoMonthlyUpdateJobs(30);
      setJobs(response.items);
      setSelectedJobId((current) => {
        if (preferredJobId && response.items.some((item) => item.jobId === preferredJobId)) {
          return preferredJobId;
        }
        if (current && response.items.some((item) => item.jobId === current)) {
          return current;
        }
        return response.items[0]?.jobId ?? null;
      });
      if (response.items.length === 0) {
        setSelectedJob(null);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      if (!silent) {
        setJobsLoading(false);
      }
    }
  }, []);

  const loadJobDetail = useCallback(async (jobId: string, silent = false) => {
    if (!silent) {
      setDetailLoading(true);
    }
    try {
      const response = await api.getJatoMonthlyUpdateJob(jobId);
      setSelectedJob(response.item);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      if (!silent) {
        setDetailLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refreshJobs();
  }, [refreshJobs]);

  useEffect(() => {
    if (!selectedJobId) {
      setSelectedJob(null);
      return;
    }
    void loadJobDetail(selectedJobId);
  }, [loadJobDetail, selectedJobId]);

  useEffect(() => {
    setReviewBundle(null);
    setReviewLoadingJobId(null);
  }, [selectedJobId]);

  const hasActiveJob = shouldPollMonthlyUpdateJobs(jobs);

  useEffect(() => {
    if (!hasActiveJob) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      void refreshJobs(selectedJobId ?? undefined, true);
      if (selectedJobId) {
        void loadJobDetail(selectedJobId, true);
      }
    }, 5000);
    return () => window.clearInterval(timer);
  }, [hasActiveJob, loadJobDetail, refreshJobs, selectedJobId]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!uploadFile) {
      setError("请选择一个 JATO Excel 文件后再启动月更任务。");
      return;
    }
    setSubmitting(true);
    setError("");
    setNotice("");
    setUploadProgress(null);
    try {
      const response = await api.createJatoMonthlyUpdateJob(
        month,
        uploadFile,
        setUploadProgress
      );
      setNotice(`已创建任务 ${response.item.jobId}，后台开始串行执行 prepare / compare / refresh。`);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      setUploadFile(null);
      setDragActive(false);
      setUploadProgress(null);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      await refreshJobs(response.item.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSubmitting(false);
    }
  }

  function applySelectedFile(nextFile: File | null) {
    if (!nextFile) {
      setUploadFile(null);
      return;
    }
    if (!isMonthlyUpdateUploadFilenameAccepted(nextFile.name)) {
      setError("只支持 .xlsx / .xlsm / .xls 文件。");
      setUploadFile(null);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      return;
    }
    setError("");
    setNotice("");
    setUploadProgress(null);
    setUploadFile(nextFile);
  }

  function handleFileChange(event: ChangeEvent<HTMLInputElement>) {
    applySelectedFile(event.target.files?.[0] ?? null);
  }

  function handleDropzoneKeyboard(event: KeyboardEvent<HTMLDivElement>) {
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    event.preventDefault();
    fileInputRef.current?.click();
  }

  function handleDragState(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    if (event.type === "dragenter" || event.type === "dragover") {
      setDragActive(true);
      return;
    }
    if (event.type === "dragleave") {
      setDragActive(false);
    }
  }

  function handleDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    setDragActive(false);
    const files = event.dataTransfer?.files;
    if (!files || files.length === 0) {
      return;
    }
    if (files.length > 1) {
      setError("月更入口一次只接受一个 JATO Excel 文件。");
      return;
    }
    applySelectedFile(files[0]);
  }

  async function handleCleanup() {
    if (hasActiveJob) {
      setError("存在运行中的月更任务，请等待完成后再执行一键清理。");
      return;
    }
    const confirmed = window.confirm(
      "将归档旧 baseline/patch，并删除成功任务的临时上传副本。当前激活 baseline、最新 patch 月份以及报告文件会保留。继续吗？"
    );
    if (!confirmed) {
      return;
    }
    setCleanupRunning(true);
    setError("");
    setNotice("");
    try {
      const response = await api.runJatoMonthlyUpdateCleanup();
      setCleanupResult(response.item);
      setNotice(
        `清理完成：归档 baseline ${response.item.archivedBaselineCount} 个，归档 patch 目录 ${response.item.archivedPatchDirCount} 个，清理上传副本 ${response.item.removedJobUploadDirCount} 个。`
      );
      await refreshJobs(selectedJobId ?? undefined, true);
      if (selectedJobId) {
        await loadJobDetail(selectedJobId, true);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setCleanupRunning(false);
    }
  }

  async function handleRetryFailedJob(job: JatoMonthlyUpdateJob) {
    setRetryingJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.retryFailedJatoMonthlyUpdateJob(job.jobId);
      setNotice(`已基于失败任务 ${job.jobId} 重新创建任务 ${response.item.jobId}，直接复用原上传副本，无需重新上传。`);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      await refreshJobs(response.item.jobId, true);
      await loadJobDetail(response.item.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setRetryingJobId(null);
    }
  }

  async function handleReviewJob(job: JatoMonthlyUpdateJob) {
    if (reviewBundle?.jobId === job.jobId) {
      setReviewBundle(null);
      return;
    }
    setReviewLoadingJobId(job.jobId);
    setError("");
    try {
      const response = await api.getJatoMonthlyUpdateReview(job.jobId);
      setReviewBundle(response.item);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setReviewLoadingJobId(null);
    }
  }

  async function handlePublishJob(job: JatoMonthlyUpdateJob) {
    const confirmed = window.confirm(
      "将把当前 staging candidate promote 为 active 数据集，并自动备份现有 active parquet / manifest / partitioned_dataset_v1 / fingerprint。继续吗？"
    );
    if (!confirmed) {
      return;
    }
    setPublishingJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.publishJatoMonthlyUpdateJob(job.jobId);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      setNotice(
        `已将任务 ${job.jobId} 的 staging candidate promote 到 active 数据集。备份目录：${response.item.publication?.backupDir ?? "-"}`
      );
      await refreshJobs(response.item.jobId, true);
      await loadJobDetail(response.item.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setPublishingJobId(null);
    }
  }

  const successCount = jobs.filter((job) => job.status === "success").length;
  const failedCount = jobs.filter((job) => job.status === "failed").length;
  const runningCount = jobs.filter((job) => job.status === "running" || job.status === "queued").length;

  const artifactEntries = buildMonthlyUpdateArtifactEntries(selectedJob);

  const rawCompare = selectedJob?.summaries?.rawCompare;
  const refresh = selectedJob?.summaries?.refresh;
  const canReviewSelectedJob = Boolean(selectedJob?.artifacts?.rawCompareReportPath);
  const canPublishSelectedJob = Boolean(
    selectedJob
    && selectedJob.status === "success"
    && selectedJob.phase === "completed"
  );
  const isSelectedJobPublished = Boolean(selectedJob?.publication?.publishedAt);

  return (
    <section className="crud-shell">
      <CollapsibleDeckHero
        collapsed={heroCollapsed}
        onToggle={() => setHeroCollapsed((current) => !current)}
        expandedLabel="展开 JATO 月更概览"
        collapsedLabel="收起 JATO 月更概览"
        expandedTitle="Expand JATO monthly update overview"
        collapsedTitle="Collapse JATO monthly update overview"
        className="header-card dashboard-hero crud-hero"
        head={(
          <>
            <div className="dashboard-hero-copy crud-hero-copy">
              <span className="page-kicker">06 / MSRP Admin</span>
              <h1>JATO Monthly Update</h1>
              <p>
                隐藏在 MSRP 管理工具里的管理员入口。上传每月 JATO patch xlsx 后，后端会自动串行复用现有
                prepare、raw compare 和 candidate refresh 脚本。
              </p>
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Publish</span>
                <span className="selection-ribbon-value">保留人工确认，现已支持网页 review / promote / cleanup</span>
              </div>
            </div>
            <div className="dashboard-hero-actions crud-hero-actions">
              <div className="hero-meta-block hero-meta-block-immersive">
                <span className="hero-meta-label">Total jobs</span>
                <strong className="hero-meta-value">{jobs.length}</strong>
                <span className="hero-meta-subvalue">保留最近 30 条</span>
              </div>
              <div className="hero-meta-block hero-meta-block-immersive">
                <span className="hero-meta-label">Running</span>
                <strong className="hero-meta-value">{runningCount}</strong>
                <span className="hero-meta-subvalue">自动轮询中</span>
              </div>
              <div className="hero-meta-block hero-meta-block-immersive">
                <span className="hero-meta-label">Success</span>
                <strong className="hero-meta-value">{successCount}</strong>
                <span className="hero-meta-subvalue">candidate refresh 完成</span>
              </div>
              <div className="hero-meta-block hero-meta-block-immersive">
                <span className="hero-meta-label">Failed</span>
                <strong className="hero-meta-value">{failedCount}</strong>
                <span className="hero-meta-subvalue">显式保留错误日志</span>
              </div>
            </div>
          </>
        )}
        body={(
          <div className="dashboard-hero-rail">
            <div className="dashboard-hero-chip-row">
              <span className="dashboard-hero-chip">{month}</span>
              <span className="dashboard-hero-chip">{uploadFile?.name ?? "No file selected"}</span>
            </div>
            <div className="dashboard-hero-rail-actions">
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                  onClick={() => {
                    setMonth(getDefaultMonthlyUpdateMonth());
                    setUploadFile(null);
                    setUploadProgress(null);
                    setDragActive(false);
                    setNotice("");
                    setError("");
                    if (fileInputRef.current) {
                    fileInputRef.current.value = "";
                  }
                }}
              >
                重置
              </button>
              <button
                type="button"
                className="btn btn-sm btn-secondary"
                onClick={() => {
                  void refreshJobs(selectedJobId ?? undefined);
                  if (selectedJobId) {
                    void loadJobDetail(selectedJobId, true);
                  }
                }}
              >
                刷新任务
              </button>
            </div>
          </div>
        )}
      />

      {error && <div className="alert alert-error">{error}</div>}
      {notice && <div className="alert">{notice}</div>}

      <div className="card crud-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">Create Monthly Update Job</div>
            <p className="section-note">
              第一期仅执行 candidate 流程并回传日志 / 关键产物；正式 release promote 仍然保留人工确认步骤。
              这里的月份表示本次 patch 的归档批次，不要求所有国家都推进到该月。
            </p>
          </div>
          <div className="table-status-chip">
            <span>Current focus</span>
            <strong>{selectedJob?.jobId ?? "Idle"}</strong>
          </div>
        </div>

        <form className="monthly-update-upload-form" onSubmit={handleSubmit}>
          <div className="monthly-update-field-grid">
            <label className="monthly-update-field">
              <span>Patch batch month</span>
              <input
                type="month"
                value={month}
                onChange={(event) => setMonth(event.target.value)}
                required
              />
              <small>用于 patch 归档目录、compare id 和 staging 命名；允许混杂月份，只要没有国家回退。</small>
            </label>
            <label className="monthly-update-field">
              <span>JATO patch xlsx</span>
              <input
                ref={fileInputRef}
                type="file"
                accept=".xlsx,.xlsm,.xls"
                onChange={handleFileChange}
                className="monthly-update-file-input"
              />
              <div
                className={`monthly-update-dropzone${dragActive ? " is-dragging" : ""}${uploadFile ? " has-file" : ""}`}
                role="button"
                tabIndex={0}
                onClick={() => fileInputRef.current?.click()}
                onKeyDown={handleDropzoneKeyboard}
                onDragEnter={handleDragState}
                onDragOver={handleDragState}
                onDragLeave={handleDragState}
                onDrop={handleDrop}
              >
                <strong>
                  {uploadFile ? uploadFile.name : "拖拽 JATO Excel 到这里，或点击选择文件"}
                </strong>
                <span>
                  {uploadFile
                    ? `${formatMonthlyUpdateFileSize(uploadFile.size)} · 自动分片上传，支持失败重试与刷新后续传。`
                    : "支持 .xlsx / .xlsm / .xls；上传时会自动分片、失败重试，并在刷新后按已收分片继续。"}
                </span>
              </div>
            </label>
          </div>

          <div className="monthly-update-form-actions">
            <p className="monthly-update-note">
              上传后会先把文件落到受控目录，再复用现有 `prepare_monthly_raw_update.py`、
              `raw_compare_review.py`、`run_data_refresh_job.py`。大文件会先分片上传、自动重试，再在服务端做分片校验和整文件
              SHA-256 指纹后入队；刷新页面后重新选择同一文件，也会从已完成分片继续。
            </p>
            <button
              type="submit"
              className="btn btn-primary"
              disabled={submitting || !month || uploadFile === null}
            >
              {submitting ? "上传并启动中..." : "启动月更任务"}
            </button>
          </div>
          {submitting && uploadProgress && (
            <div className="monthly-update-upload-progress">
              <div className="monthly-update-upload-progress-head">
                <strong>{getMonthlyUpdateUploadStageLabel(uploadProgress.stage)}</strong>
                <span>
                  {formatMonthlyUpdateFileSize(uploadProgress.uploadedBytes)} / {formatMonthlyUpdateFileSize(uploadProgress.totalBytes)}
                </span>
              </div>
              <div className="monthly-update-upload-progress-bar">
                <span
                  style={{
                    width: `${uploadProgress.totalBytes > 0
                      ? Math.min((uploadProgress.uploadedBytes / uploadProgress.totalBytes) * 100, 100)
                      : 0}%`
                  }}
                />
              </div>
              <div className="monthly-update-upload-progress-meta">
                <span>
                  chunks {uploadProgress.uploadedChunks}/{uploadProgress.totalChunks || "-"}
                </span>
                <span>chunk size {formatMonthlyUpdateFileSize(uploadProgress.chunkSize)}</span>
              </div>
              {uploadProgress.detail && (
                <div className="monthly-update-upload-progress-detail">
                  {uploadProgress.detail}
                </div>
              )}
            </div>
          )}
        </form>
      </div>

      <div className="card crud-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">Cleanup Reminder</div>
            <p className="section-note">
              这块会固定显示，作为你每次月更前后的清理提醒。
            </p>
          </div>
          <div className="monthly-update-cleanup-actions">
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => void handleCleanup()}
              disabled={cleanupRunning || hasActiveJob}
            >
              {cleanupRunning ? "清理中..." : "一键清理"}
            </button>
          </div>
        </div>
        <div className="alert alert-warning monthly-update-reminder">
          <strong>建议保留策略：</strong> baseline 目录只保留一个当前激活最新 baseline；旧 baseline 和旧 patch 做归档，
          不要继续堆在 active 目录里；job 目录中的临时上传副本成功后可清理。
        </div>
        {cleanupResult && (
          <div className="monthly-update-cleanup-result">
            <div className="monthly-update-cleanup-result-head">
              <span className="card-title">Last Cleanup</span>
              <span className="section-note">
                {formatMonthlyUpdateTimestamp(cleanupResult.cleanedAt)}
              </span>
            </div>
            <div className="monthly-update-cleanup-summary">
              <span>active baseline: {cleanupResult.activeBaselinePath ?? "-"}</span>
              <span>active patch month: {cleanupResult.activePatchMonth ?? "-"}</span>
              <span>archived baselines: {formatMonthlyUpdateNumber(cleanupResult.archivedBaselineCount)}</span>
              <span>archived patch dirs: {formatMonthlyUpdateNumber(cleanupResult.archivedPatchDirCount)}</span>
              <span>removed upload dirs: {formatMonthlyUpdateNumber(cleanupResult.removedJobUploadDirCount)}</span>
            </div>
          </div>
        )}
        <div className="monthly-update-cleanup-grid">
          <article className="monthly-update-cleanup-card">
            <span>保留</span>
            <strong>01_RAW_DATA/baseline/</strong>
            <small>只留当前激活最新 baseline，供下一轮 compare 使用。</small>
          </article>
          <article className="monthly-update-cleanup-card">
            <span>归档</span>
            <strong>旧 baseline / 旧 patch</strong>
            <small>保留追溯能力，但不要继续放在 active 目录影响自动识别。</small>
          </article>
          <article className="monthly-update-cleanup-card">
            <span>可清理</span>
            <strong>04_Processed_data/ops/jato_monthly_update_jobs/*/uploads/</strong>
            <small>这是 job 级临时上传副本；成功后可以删，原 patch 归档仍保留。</small>
          </article>
          <article className="monthly-update-cleanup-card">
            <span>建议保留</span>
            <strong>raw compare / refresh reports</strong>
            <small>用于回溯、审计、排错，不建议和临时上传副本一起清掉。</small>
          </article>
        </div>
      </div>

      <div className="monthly-update-grid">
        <div className="card crud-table-card monthly-update-card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">Recent Jobs</div>
              <p className="section-note">列表用于切换查看详情，运行中的任务会自动刷新。</p>
            </div>
            <div className="table-status-chip">
              <span>Count</span>
              <strong>{jobs.length}</strong>
            </div>
          </div>

          {jobsLoading && jobs.length === 0 ? (
            <LoadingSurface mode="inline" label="正在加载任务" detail="同步月更作业列表" kicker="JATO" />
          ) : (
            <div className="table-wrapper">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Status</th>
                    <th>Month</th>
                    <th>Patch file</th>
                    <th>Phase</th>
                    <th>Updated</th>
                    <th>Triggered By</th>
                    <th style={{ width: 96 }}>操作</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((job) => (
                    <tr
                      key={job.jobId}
                      className={job.jobId === selectedJobId ? "is-selected" : ""}
                    >
                      <td>
                        <span className={`badge ${getMonthlyUpdateStatusBadgeClass(job.status)}`}>
                          {job.status}
                        </span>
                      </td>
                      <td><strong>{job.month}</strong></td>
                      <td title={job.upload?.originalFilename ?? "-"}>
                        {job.upload?.originalFilename ?? "-"}
                      </td>
                      <td>{formatMonthlyUpdatePhase(job.phase)}</td>
                      <td>{formatMonthlyUpdateTimestamp(job.updatedAt)}</td>
                      <td>{job.triggeredBy || "-"}</td>
                      <td>
                        <div className="crud-row-actions">
                          <button
                            type="button"
                            className="btn btn-sm btn-secondary"
                            onClick={() => setSelectedJobId(job.jobId)}
                          >
                            详情
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {jobs.length === 0 && (
                    <tr>
                      <td colSpan={7}>
                        <div className="crud-empty-state">暂无月更任务</div>
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="card crud-card monthly-update-card">
          <div className="detail-section-head">
            <div>
              <div className="card-title">Job Detail</div>
              <p className="section-note">状态、摘要、路径和日志都集中在这里查看。</p>
            </div>
            {selectedJob && (
                <div className="crud-row-actions">
                  {canReviewSelectedJob && (
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => void handleReviewJob(selectedJob)}
                      disabled={reviewLoadingJobId === selectedJob.jobId}
                    >
                      {reviewLoadingJobId === selectedJob.jobId
                        ? "加载 review..."
                        : reviewBundle?.jobId === selectedJob.jobId
                          ? "收起 Review"
                          : "Review Candidate"}
                    </button>
                  )}
                  {canPublishSelectedJob && (
                    <button
                      type="button"
                      className="btn btn-primary"
                      onClick={() => void handlePublishJob(selectedJob)}
                      disabled={
                        publishingJobId === selectedJob.jobId
                        || isSelectedJobPublished
                        || hasActiveJob
                      }
                    >
                      {isSelectedJobPublished
                        ? "Published"
                        : publishingJobId === selectedJob.jobId
                          ? "Publishing..."
                          : "Publish Candidate"}
                    </button>
                  )}
                  {selectedJob.status === "failed" && (
                    <button
                      type="button"
                    className="btn btn-secondary"
                    onClick={() => void handleRetryFailedJob(selectedJob)}
                    disabled={retryingJobId === selectedJob.jobId || !selectedJob.upload?.storedPath}
                  >
                    {retryingJobId === selectedJob.jobId ? "重试中..." : "Retry Failed Job"}
                  </button>
                )}
                <div className="table-status-chip">
                  <span>Job</span>
                  <strong>{selectedJob.jobId}</strong>
                </div>
              </div>
            )}
          </div>

          {detailLoading && !selectedJob ? (
            <LoadingSurface mode="inline" label="正在加载详情" detail="同步当前任务状态" kicker="JATO" />
          ) : !selectedJob ? (
            <div className="crud-empty-state">选择左侧任务查看详情</div>
          ) : (
            <div className="monthly-update-detail-stack">
              <div className="admin-detail-grid">
                <div className="admin-detail-item">
                  <span>Status</span>
                  <strong><span className={`badge ${getMonthlyUpdateStatusBadgeClass(selectedJob.status)}`}>{selectedJob.status}</span></strong>
                </div>
                <div className="admin-detail-item">
                  <span>Phase</span>
                  <strong>{formatMonthlyUpdatePhase(selectedJob.phase)}</strong>
                </div>
                <div className="admin-detail-item">
                  <span>Month</span>
                  <strong>{selectedJob.month}</strong>
                </div>
                <div className="admin-detail-item">
                  <span>Triggered by</span>
                  <strong>{selectedJob.triggeredBy || "-"}</strong>
                </div>
                <div className="admin-detail-item">
                  <span>Created</span>
                  <strong>{formatMonthlyUpdateTimestamp(selectedJob.createdAt)}</strong>
                </div>
                <div className="admin-detail-item">
                  <span>Finished</span>
                  <strong>{formatMonthlyUpdateTimestamp(selectedJob.finishedAt)}</strong>
                </div>
              </div>

              {selectedJob.error && (
                <div className="alert alert-error">{selectedJob.error}</div>
              )}

              {selectedJob.publication?.publishedAt && (
                <div className="alert alert-success">
                  已 publish 到 active：{formatMonthlyUpdateTimestamp(selectedJob.publication.publishedAt)}
                  {" · "}
                  {selectedJob.publication.publishedBy || "-"}
                  {" · backup "}
                  {selectedJob.publication.backupDir || "-"}
                </div>
              )}

              <div className="monthly-update-summary-grid">
                <article className="monthly-update-summary-card">
                  <span>Raw compare blockers</span>
                  <strong>{formatMonthlyUpdateNumber(rawCompare?.blockerCount)}</strong>
                  <small>decision: {rawCompare?.decisionSuggestion || "-"}</small>
                </article>
                <article className="monthly-update-summary-card">
                  <span>Countries advanced</span>
                  <strong>{formatMonthlyUpdateNumber(rawCompare?.advancedCountryCount)}</strong>
                  <small>regressed {formatMonthlyUpdateNumber(rawCompare?.regressedCountryCount)}</small>
                </article>
                <article className="monthly-update-summary-card">
                  <span>Refresh rows</span>
                  <strong>{formatMonthlyUpdateNumber(refresh?.rowCount)}</strong>
                  <small>{formatMonthlyUpdateNumber(refresh?.columnCount)} columns</small>
                </article>
                <article className="monthly-update-summary-card">
                  <span>Refresh elapsed</span>
                  <strong>{formatMonthlyUpdateSeconds(refresh?.jobElapsedSeconds)}</strong>
                  <small>{formatMonthlyUpdateNumber(refresh?.changedCountryCount)} countries changed</small>
                </article>
              </div>

              {reviewBundle?.jobId === selectedJob.jobId && (
                <div className="card crud-card">
                  <div className="detail-section-head">
                    <div>
                      <div className="card-title">Review Candidate</div>
                      <p className="section-note">
                        这里集中展示 raw compare checklist 与人工 review 要点；确认后可直接点击 Publish Candidate。
                      </p>
                    </div>
                    <div className="table-status-chip">
                      <span>Decision</span>
                      <strong>{reviewBundle.decisionSuggestion || "-"}</strong>
                    </div>
                  </div>
                  <div className="monthly-update-summary-grid">
                    <article className="monthly-update-summary-card">
                      <span>Compare ID</span>
                      <strong>{reviewBundle.compareId || "-"}</strong>
                      <small>{reviewBundle.compareKeyColumns.join(" / ") || "-"}</small>
                    </article>
                    <article className="monthly-update-summary-card">
                      <span>Review findings</span>
                      <strong>{formatMonthlyUpdateNumber(reviewBundle.reviewFindings.length)}</strong>
                      <small>{reviewBundle.reviewDir || "-"}</small>
                    </article>
                    <article className="monthly-update-summary-card">
                      <span>Conflict samples</span>
                      <strong>{formatMonthlyUpdateNumber(reviewBundle.conflictSampleCount)}</strong>
                      <small>{reviewBundle.sampledCountries.join(", ") || "-"}</small>
                    </article>
                    <article className="monthly-update-summary-card">
                      <span>Refresh status</span>
                      <strong>{reviewBundle.refreshSummary?.jobStatus || refresh?.jobStatus || "-"}</strong>
                      <small>{formatMonthlyUpdateSeconds(reviewBundle.refreshSummary?.jobElapsedSeconds)}</small>
                    </article>
                  </div>
                  <div>
                    <div className="card-title">Review Findings</div>
                    {reviewBundle.reviewFindings.length === 0 ? (
                      <div className="crud-empty-state">暂无需要人工确认的 findings</div>
                    ) : (
                      <div className="table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>Severity</th>
                              <th>Target</th>
                              <th>Rule</th>
                              <th>Message</th>
                              <th>Details</th>
                            </tr>
                          </thead>
                          <tbody>
                            {reviewBundle.reviewFindings.map((finding, index) => (
                              <tr key={`${finding.ruleId}-${finding.target}-${index}`}>
                                <td>{finding.severity}</td>
                                <td>{finding.target || "-"}</td>
                                <td>{finding.ruleId || "-"}</td>
                                <td>{finding.message || "-"}</td>
                                <td>{formatReviewMetrics(finding.metrics)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="card-title">Conflict Samples</div>
                    {reviewBundle.conflictSamples.length === 0 ? (
                      <div className="crud-empty-state">暂无可展示的冲突样本</div>
                    ) : (
                      <div className="table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>Country</th>
                              <th>Business key</th>
                              <th>Changed fields</th>
                              <th>Old digest</th>
                              <th>New digest</th>
                            </tr>
                          </thead>
                          <tbody>
                            {reviewBundle.conflictSamples.map((sample, index) => (
                              <tr key={`${sample.country}-${index}`}>
                                <td>{sample.country || "-"}</td>
                                <td>{formatConflictSampleBusinessKey(sample.businessKey)}</td>
                                <td>{sample.changedFields.join(", ") || "-"}</td>
                                <td title={sample.oldValueDigest || undefined}>
                                  {formatDigestPreview(sample.oldValueDigest)}
                                </td>
                                <td title={sample.newValueDigest || undefined}>
                                  {formatDigestPreview(sample.newValueDigest)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="card-title">Review Checklist</div>
                    <pre className="monthly-update-pre">
                      {reviewBundle.checklistMarkdown || "暂无 checklist 输出"}
                    </pre>
                  </div>
                </div>
              )}

              <div>
                <div className="card-title">Artifacts</div>
                {artifactEntries.length === 0 ? (
                  <div className="crud-empty-state">暂无产物路径</div>
                ) : (
                  <dl className="monthly-update-artifact-list">
                    {artifactEntries.map(([label, value]) => (
                      <div key={`${label}-${value}`} className="monthly-update-artifact-item">
                        <dt>{label}</dt>
                        <dd className="text-mono">{value}</dd>
                      </div>
                    ))}
                  </dl>
                )}
              </div>

              <div className="monthly-update-command-list">
                {selectedJob.plan?.compareCommand && (
                  <div>
                    <div className="card-title">Raw Compare Command</div>
                    <pre className="monthly-update-pre">{selectedJob.plan.compareCommand}</pre>
                  </div>
                )}
                {selectedJob.plan?.refreshCommand && (
                  <div>
                    <div className="card-title">Refresh Command</div>
                    <pre className="monthly-update-pre">{selectedJob.plan.refreshCommand}</pre>
                  </div>
                )}
              </div>

              <div>
                <div className="card-title">Log Tail</div>
                <pre className="monthly-update-pre monthly-update-log">
                  {selectedJob.logTail || "暂无日志输出"}
                </pre>
              </div>
            </div>
          )}
        </div>
      </div>
      <AdminToolsNav />
    </section>
  );
}
