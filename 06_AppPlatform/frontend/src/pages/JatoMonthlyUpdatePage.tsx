import { ChangeEvent, DragEvent, FormEvent, KeyboardEvent, useCallback, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  JatoHistoricalReclassificationDecision,
  JatoHistoricalReclassificationDecisionInput,
  JatoMonthlyUpdateBaselinePromotionResult,
  JatoMonthlyUpdateConflictSample,
  JatoMonthlyUpdateCleanupResult,
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateMaintenanceStatus,
  JatoMonthlyUpdateReviewBundle,
  JatoMonthlyUpdateStorageMetric,
  JatoMonthlyUpdateUploadProgress,
  PublishBlocker,
} from "../types";
import {
  buildMonthlyUpdateArtifactEntries,
  formatMonthlyUpdateFileSize,
  formatMonthlyUpdateNumber,
  formatMonthlyUpdatePhase,
  formatMonthlyUpdateSeconds,
  formatMonthlyUpdateTimestamp,
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

async function copySourceFeedback(value: string): Promise<boolean> {
  if (!navigator.clipboard?.writeText) {
    return false;
  }
  try {
    await navigator.clipboard.writeText(value);
    return true;
  } catch {
    return false;
  }
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

function formatUploadRoute(value: string | null | undefined): string {
  switch (value) {
    case "single_country":
      return "单国分区 Review";
    case "partial_country":
      return "多国分区 Review";
    case "full_batch":
      return "完整批次";
    default:
      return "待识别";
  }
}

function formatSampleKeyRecord(item: Record<string, unknown>): string {
  const entries = Object.entries(item);
  if (entries.length === 0) {
    return "-";
  }
  return entries.map(([key, value]) => `${key}: ${formatReviewMetricValue(value)}`).join(" · ");
}

function formatSampleKeyRecords(items: Record<string, unknown>[]): string {
  if (items.length === 0) {
    return "-";
  }
  return items.map((item) => `- ${formatSampleKeyRecord(item)}`).join("\n");
}

function formatSignedNumber(value: number): string {
  if (!Number.isFinite(value)) {
    return "0";
  }
  if (value > 0) {
    return `+${formatMonthlyUpdateNumber(value)}`;
  }
  if (value < 0) {
    return `-${formatMonthlyUpdateNumber(Math.abs(value))}`;
  }
  return "0";
}

function formatMonthlySalesChangeStatus(value: string): string {
  switch (value) {
    case "unchanged":
      return "持平";
    case "changed":
      return "变动";
    case "added":
      return "新增月份";
    case "removed":
      return "候选缺失";
    default:
      return value || "-";
  }
}

function sumStorageMetricBytes(metrics: JatoMonthlyUpdateStorageMetric[]): number {
  return metrics.reduce((total, metric) => total + metric.bytes, 0);
}

function formatCleanupTierLabel(tier: "safe" | "cautious"): string {
  return tier === "cautious" ? "谨慎删" : "安全删";
}

function isBaselinePromotionActive(
  promotion: JatoMonthlyUpdateBaselinePromotionResult | null
): boolean {
  return promotion?.status === "queued" || promotion?.status === "running";
}

function formatBaselinePromotionStatus(
  status: JatoMonthlyUpdateBaselinePromotionResult["status"]
): string {
  switch (status) {
    case "queued":
      return "排队中";
    case "running":
      return "保存中";
    case "success":
      return "已完成";
    case "failed":
      return "失败";
  }
}

function formatBaselinePromotionSuccess(
  promotion: JatoMonthlyUpdateBaselinePromotionResult
): string {
  return `已保存新的 baseline：${promotion.baselinePath ?? "-"}；latest month ${promotion.detectedLatestMonth ?? "-"}；自动归档旧 baseline ${promotion.archivedBaselineCount} 个。`;
}

function formatBaselinePromotionFailure(
  promotion: JatoMonthlyUpdateBaselinePromotionResult
): string {
  return (
    promotion.failureDigest?.sourceFeedback
    || promotion.failureDigest?.message
    || promotion.error
    || "baseline 保存失败，请查看 worker 日志后重试。"
  );
}

type PendingMaintenanceAction =
  | { kind: "promote-baseline" }
  | { kind: "cleanup"; cleanupTier: "safe" | "cautious" };

export function JatoMonthlyUpdatePage() {
  const [jobs, setJobs] = useState<JatoMonthlyUpdateJob[]>([]);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<JatoMonthlyUpdateJob | null>(null);
  const [jobsLoading, setJobsLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [cleanupRunning, setCleanupRunning] = useState(false);
  const [maintenanceLoading, setMaintenanceLoading] = useState(false);
  const [retryingJobId, setRetryingJobId] = useState<string | null>(null);
  const [recheckingJobId, setRecheckingJobId] = useState<string | null>(null);
  const [cancellingJobId, setCancellingJobId] = useState<string | null>(null);
  const [publishingJobId, setPublishingJobId] = useState<string | null>(null);
  const [rollingBackJobId, setRollingBackJobId] = useState<string | null>(null);
  const [baselinePromotionSubmitting, setBaselinePromotionSubmitting] = useState(false);
  const [reviewLoadingJobId, setReviewLoadingJobId] = useState<string | null>(null);
  const [approvingReviewJobId, setApprovingReviewJobId] = useState<string | null>(null);
  const [
    resolvingHistoricalReclassificationJobId,
    setResolvingHistoricalReclassificationJobId,
  ] = useState<string | null>(null);
  const [copiedSourceFeedbackKey, setCopiedSourceFeedbackKey] = useState<string | null>(null);
  const [reviewBundle, setReviewBundle] = useState<JatoMonthlyUpdateReviewBundle | null>(null);
  const [
    historicalReclassificationDecisions,
    setHistoricalReclassificationDecisions,
  ] = useState<Record<string, JatoHistoricalReclassificationDecision>>({});
  const [selectedReviewCountry, setSelectedReviewCountry] = useState<string | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [uploadMonth, setUploadMonth] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [cleanupResult, setCleanupResult] =
    useState<JatoMonthlyUpdateCleanupResult | null>(null);
  const [pendingMaintenanceAction, setPendingMaintenanceAction] =
    useState<PendingMaintenanceAction | null>(null);
  const [maintenanceNotice, setMaintenanceNotice] = useState("");
  const [maintenanceError, setMaintenanceError] = useState("");
  const [selectedCleanupTier, setSelectedCleanupTier] =
    useState<"safe" | "cautious">("safe");
  const [maintenanceStatus, setMaintenanceStatus] =
    useState<JatoMonthlyUpdateMaintenanceStatus | null>(null);
  const [baselinePromotion, setBaselinePromotion] =
    useState<JatoMonthlyUpdateBaselinePromotionResult | null>(null);
  const [uploadProgress, setUploadProgress] =
    useState<JatoMonthlyUpdateUploadProgress | null>(null);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [abandoningUpload, setAbandoningUpload] = useState(false);
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const [publishBlocker, setPublishBlocker] = useState<PublishBlocker | null>(null);
  const [smartMergingJobId, setSmartMergingJobId] = useState<string | null>(null);
  const [infoCollapsed, setInfoCollapsed] = useState(true);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const watchedBaselinePromotionIdRef = useRef<string | null>(null);
  const abandonedUploadIdRef = useRef<string | null>(null);

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

  const refreshMaintenanceStatus = useCallback(async (silent = false) => {
    if (!silent) {
      setMaintenanceLoading(true);
    }
    try {
      const response = await api.getJatoMonthlyUpdateMaintenanceStatus();
      setMaintenanceStatus(response.item);
      setBaselinePromotion(response.item.baselinePromotion);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      if (!silent) {
        setMaintenanceLoading(false);
      }
    }
  }, []);

  useEffect(() => {
    void refreshJobs();
  }, [refreshJobs]);

  useEffect(() => {
    void refreshMaintenanceStatus();
  }, [refreshMaintenanceStatus]);

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
    setSelectedReviewCountry(null);
    setPublishBlocker(null);
  }, [selectedJobId]);

  const hasActiveJob = shouldPollMonthlyUpdateJobs(jobs);
  const baselinePromotionActive = isBaselinePromotionActive(baselinePromotion);
  const promotingBaseline = baselinePromotionSubmitting || baselinePromotionActive;
  const uploadOrJobBusy = submitting || hasActiveJob;
  const maintenanceBusy = cleanupRunning || promotingBaseline;
  const monthlyUpdateBusy = uploadOrJobBusy || maintenanceBusy;
  const abandonableUploadId = uploadProgress?.uploadId && [
    "verifying",
    "resuming",
    "uploading",
    "retrying",
    "assembling",
    "digesting",
  ].includes(uploadProgress.stage)
    ? uploadProgress.uploadId
    : null;

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

  useEffect(() => {
    if (!baselinePromotionActive) {
      return undefined;
    }
    const timer = window.setInterval(() => {
      void refreshMaintenanceStatus(true);
    }, 5000);
    return () => window.clearInterval(timer);
  }, [baselinePromotionActive, refreshMaintenanceStatus]);

  useEffect(() => {
    if (!baselinePromotion) {
      return;
    }
    const operationId = baselinePromotion.operationId;
    if (baselinePromotion.status === "queued") {
      watchedBaselinePromotionIdRef.current = operationId;
      setMaintenanceError("");
      setMaintenanceNotice(
        `baseline 保存任务 ${operationId || "-"} 已进入隔离队列，页面会自动刷新状态。`
      );
      return;
    }
    if (baselinePromotion.status === "running") {
      watchedBaselinePromotionIdRef.current = operationId;
      setMaintenanceError("");
      setMaintenanceNotice(
        `baseline 保存任务 ${operationId || "-"} 正在隔离 worker 中导出，页面会自动刷新状态。`
      );
      return;
    }
    if (watchedBaselinePromotionIdRef.current !== operationId) {
      return;
    }
    watchedBaselinePromotionIdRef.current = null;
    if (baselinePromotion.status === "success") {
      const successMessage = formatBaselinePromotionSuccess(baselinePromotion);
      setMaintenanceError("");
      setMaintenanceNotice(successMessage);
      setNotice(successMessage);
      return;
    }
    setMaintenanceNotice("");
    setMaintenanceError(formatBaselinePromotionFailure(baselinePromotion));
  }, [baselinePromotion]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (monthlyUpdateBusy) {
      return;
    }
    if (!uploadFile) {
      setError("请选择一个 JATO Excel 文件后再启动月更任务。");
      return;
    }
    setSubmitting(true);
    abandonedUploadIdRef.current = null;
    setError("");
    setNotice("");
    setUploadProgress(null);
    try {
      const response = await api.createJatoMonthlyUpdateJob(
        uploadFile,
        (progress) => {
          if (!abandonedUploadIdRef.current || progress.uploadId !== abandonedUploadIdRef.current) {
            setUploadProgress(progress);
          }
        },
        uploadMonth ?? undefined,
      );
      setNotice(
        `已创建任务 ${response.item.jobId}，自动识别最新数据月 ${response.item.month || "-"}，批次 ${response.item.batchId || "-"}；系统会按国家范围选择分区 Review 或完整批次管线。`
      );
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      setUploadFile(null);
      setDragActive(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
      await refreshJobs(response.item.jobId, true);
      await refreshMaintenanceStatus(true);
    } catch (err) {
      if (!abandonedUploadIdRef.current) {
        setError((err as Error).message);
      }
    } finally {
      setSubmitting(false);
    }
  }

  function applySelectedFile(nextFile: File | null) {
    if (monthlyUpdateBusy) {
      return;
    }
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
    if (monthlyUpdateBusy) {
      event.target.value = "";
      return;
    }
    applySelectedFile(event.target.files?.[0] ?? null);
  }

  function handleDropzoneKeyboard(event: KeyboardEvent<HTMLDivElement>) {
    if (monthlyUpdateBusy) {
      return;
    }
    if (event.key !== "Enter" && event.key !== " ") {
      return;
    }
    event.preventDefault();
    fileInputRef.current?.click();
  }

  function handleDragState(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    event.stopPropagation();
    if (monthlyUpdateBusy) {
      setDragActive(false);
      return;
    }
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
    if (monthlyUpdateBusy) {
      return;
    }
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

  async function handleAbandonUpload(): Promise<void> {
    if (!abandonableUploadId || abandoningUpload) {
      return;
    }
    setAbandoningUpload(true);
    abandonedUploadIdRef.current = abandonableUploadId;
    setError("");
    try {
      await api.abandonJatoMonthlyUpdateUpload(abandonableUploadId);
      setUploadProgress(null);
      setNotice("已放弃本次上传并清除本地续传信息；candidate 与 active 均未修改。");
      await refreshMaintenanceStatus(true);
    } catch (err) {
      abandonedUploadIdRef.current = null;
      setError((err as Error).message);
    } finally {
      setAbandoningUpload(false);
    }
  }

  function requestCleanup() {
    if (uploadOrJobBusy) {
      setMaintenanceError("上传、digest 或月更任务进行中，请等待完成后再执行一键清理。");
      setMaintenanceNotice("");
      return;
    }
    if (maintenanceBusy) {
      setMaintenanceNotice("");
      return;
    }
    setError("");
    setMaintenanceError("");
    setMaintenanceNotice(`${formatCleanupTierLabel(selectedCleanupTier)}已就绪，请在下方确认执行。`);
    setPendingMaintenanceAction({ kind: "cleanup", cleanupTier: selectedCleanupTier });
  }

  async function executeCleanup(cleanupTier: "safe" | "cautious") {
    if (uploadOrJobBusy) {
      setMaintenanceError("上传、digest 或月更任务进行中，请等待完成后再执行一键清理。");
      return;
    }
    if (maintenanceBusy) {
      return;
    }
    setCleanupRunning(true);
    setError("");
    setNotice("");
    setMaintenanceError("");
    setMaintenanceNotice(`${formatCleanupTierLabel(cleanupTier)}执行中，请等待结果返回。`);
    setPendingMaintenanceAction(null);
    try {
      const response = await api.runJatoMonthlyUpdateCleanup(cleanupTier);
      setCleanupResult(response.item);
      const successMessage =
        `${formatCleanupTierLabel(response.item.cleanupTier)}完成：释放 ${formatMonthlyUpdateFileSize(response.item.freedBytes)}，归档 baseline ${response.item.archivedBaselineCount} 个，归档 patch 目录 ${response.item.archivedPatchDirCount} 个，清理 upload session ${response.item.removedUploadSessionDirCount} 个，清理上传副本 ${response.item.removedJobUploadDirCount} 个。`
      setMaintenanceNotice(successMessage);
      setNotice(successMessage);
      await refreshJobs(selectedJobId ?? undefined, true);
      await refreshMaintenanceStatus(true);
      if (selectedJobId) {
        await loadJobDetail(selectedJobId, true);
      }
    } catch (err) {
      setMaintenanceError((err as Error).message);
    } finally {
      setCleanupRunning(false);
    }
  }

  function requestPromoteBaseline() {
    if (uploadOrJobBusy) {
      setMaintenanceError("上传、digest 或月更任务进行中，请等待完成后再保存新的 baseline。");
      setMaintenanceNotice("");
      return;
    }
    if (maintenanceBusy) {
      setMaintenanceNotice("");
      return;
    }
    setError("");
    setMaintenanceError("");
    setMaintenanceNotice("保存当前 active 为 baseline 已就绪，请在下方确认执行。");
    setPendingMaintenanceAction({ kind: "promote-baseline" });
  }

  async function executePromoteBaseline() {
    if (uploadOrJobBusy) {
      setMaintenanceError("上传、digest 或月更任务进行中，请等待完成后再保存新的 baseline。");
      return;
    }
    if (maintenanceBusy) {
      return;
    }
    setBaselinePromotionSubmitting(true);
    setError("");
    setNotice("");
    setMaintenanceError("");
    setMaintenanceNotice("正在提交 baseline 保存任务。");
    setPendingMaintenanceAction(null);
    try {
      const response = await api.promoteCurrentActiveToJatoBaseline();
      setBaselinePromotion(response.item);
      watchedBaselinePromotionIdRef.current = response.item.operationId;
      if (response.item.status === "success") {
        const successMessage = formatBaselinePromotionSuccess(response.item);
        setMaintenanceNotice(successMessage);
        setNotice(successMessage);
      } else if (response.item.status === "failed") {
        setMaintenanceNotice("");
        setMaintenanceError(formatBaselinePromotionFailure(response.item));
      } else {
        setMaintenanceNotice(
          response.item.status === "queued"
            ? `baseline 保存任务 ${response.item.operationId || "-"} 已进入隔离队列，页面会自动刷新状态。`
            : `baseline 保存任务 ${response.item.operationId || "-"} 正在隔离 worker 中导出，页面会自动刷新状态。`
        );
      }
      await refreshMaintenanceStatus(true);
    } catch (err) {
      setMaintenanceError((err as Error).message);
    } finally {
      setBaselinePromotionSubmitting(false);
    }
  }

  async function handleRecheckJob(job: JatoMonthlyUpdateJob) {
    setRecheckingJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.recheckJatoMonthlyUpdateJob(job.jobId);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      const runtime = response.item.runtimeCheck;
      setNotice(
        runtime?.resolvedAs === "stale_failed"
          ? `刷新查验完成：任务 ${job.jobId} 已标记为 stale_failed。`
          : `刷新查验完成：任务 ${job.jobId} 状态已更新。`
      );
      await refreshJobs(response.item.jobId, true);
      await loadJobDetail(response.item.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setRecheckingJobId(null);
    }
  }

  async function handleCancelJob(job: JatoMonthlyUpdateJob) {
    const confirmed = window.confirm(
      [
        "确认终止当前 JATO 月更任务？",
        "",
        `Job: ${job.jobId}`,
        `Phase: ${formatMonthlyUpdatePhase(job.phase)}`,
        `Batch: ${job.batchId || job.plan?.batchId || "-"}`,
        "",
        "这会停止后台脚本，并将任务标记为 cancelled。已生成的 staging/review 临时产物不会自动 publish。",
      ].join("\n")
    );
    if (!confirmed) {
      return;
    }
    setCancellingJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.cancelJatoMonthlyUpdateJob(job.jobId);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      setNotice(`已终止任务 ${job.jobId}。`);
      await refreshJobs(response.item.jobId, true);
      await loadJobDetail(response.item.jobId, true);
      await refreshMaintenanceStatus(true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setCancellingJobId(null);
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
      setHistoricalReclassificationDecisions({});
      return;
    }
    setReviewLoadingJobId(job.jobId);
    setError("");
    try {
      const response = await api.getJatoMonthlyUpdateReview(job.jobId);
      setReviewBundle(response.item);
      setHistoricalReclassificationDecisions({});
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setReviewLoadingJobId(null);
    }
  }

  async function handleApproveReview(job: JatoMonthlyUpdateJob) {
    if (!reviewBundle || reviewBundle.jobId !== job.jobId) {
      setError("请先打开并核对 Review Candidate，再执行批准。");
      return;
    }
    if (reviewBundle.historicalReclassificationReport.status === "decision_required") {
      setError("请先应用历史分类选择并等待完整 Candidate 重建，再执行批准。");
      return;
    }
    if (!window.confirm("确认批准当前 candidate 的 Review？批准将绑定当前候选指纹；任何重建后都必须重新 Review。")) {
      return;
    }
    setApprovingReviewJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.approveJatoMonthlyUpdateReview(job.jobId);
      setSelectedJob(response.item);
      setReviewBundle((current) => current?.jobId === job.jobId
        ? { ...current, approval: response.item.reviewApproval ?? null }
        : current);
      setNotice(`Review 已批准：${job.jobId}。Publish 只接受这一候选指纹。`);
      await refreshJobs(job.jobId, true);
      await loadJobDetail(job.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setApprovingReviewJobId(null);
    }
  }

  async function handleResolveHistoricalReclassification(job: JatoMonthlyUpdateJob) {
    if (!reviewBundle || reviewBundle.jobId !== job.jobId) {
      setError("请先打开 Review Candidate，再处理历史分类变化。");
      return;
    }
    const requiredCountries = reviewBundle.historicalReclassificationReport.countries
      .filter((countryReport) => countryReport.decisionRequired);
    const missingCountries = requiredCountries
      .filter((countryReport) => !historicalReclassificationDecisions[countryReport.country])
      .map((countryReport) => countryReport.country);
    if (missingCountries.length > 0) {
      setError(`请先为所有历史重分类国家做决定：${missingCountries.join("、")}。`);
      return;
    }
    const decisions: JatoHistoricalReclassificationDecisionInput[] = requiredCountries.map(
      (countryReport) => ({
        country: countryReport.country,
        decision: historicalReclassificationDecisions[countryReport.country],
      })
    );
    const decisionSummary = decisions
      .map(({ country, decision }) => (
        `${country}：${decision === "use_latest" ? "采用最新 washed 分类" : "保留当前 active 历史"}`
      ))
      .join("\n");
    if (!window.confirm(
      `确认应用以下历史分类选择并生成完整 Candidate？\n\n${decisionSummary}\n\n这一步只重建 staging，不会修改 active；完成后仍需重新 Review、Approve 和 Publish。`
    )) {
      return;
    }
    setResolvingHistoricalReclassificationJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.resolveJatoMonthlyUpdateHistoricalReclassification(
        job.jobId,
        decisions,
      );
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      setJobs((current) => [
        response.item,
        ...current.filter((item) => item.jobId !== response.item.jobId),
      ]);
      setReviewBundle(null);
      setHistoricalReclassificationDecisions({});
      setNotice(
        `已应用 ${decisions.length} 个国家的历史分类选择，完整 Candidate 正在隔离重建；active 尚未改变。`
      );
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setResolvingHistoricalReclassificationJobId(null);
    }
  }

  function parsePublishBlocker(error: unknown): PublishBlocker | null {
    const msg = error instanceof Error ? error.message : String(error);
    const spaceIndex = msg.indexOf(" ");
    if (spaceIndex < 0) return null;
    const detailStr = msg.slice(spaceIndex + 1).trim();
    if (!detailStr) return null;
    try {
      const parsed = JSON.parse(detailStr) as Record<string, unknown>;
      if (
        typeof parsed.blockerType === "string"
        && [
          "country_regression",
          "sales_doubling",
          "historical_sales_changed",
          "historical_configuration_changed",
          "historical_configuration_guard_unavailable",
          "ambiguous_logical_country",
          "stale_candidate",
          "candidate_bundle_invalid",
          "rollback_target_stale",
        ].includes(parsed.blockerType)
        && typeof parsed.message === "string"
      ) {
        return parsed as unknown as PublishBlocker;
      }
    } catch {
      // not a structured blocker — generic error
    }
    return null;
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
    setPublishBlocker(null);
    try {
      const response = await api.publishJatoMonthlyUpdateJob(job.jobId);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      const operation = response.item.pendingOperation;
      setNotice(operation?.status === "queued" || operation?.status === "running"
        ? `任务 ${job.jobId} 已进入隔离 Publish 队列；active 尚未改变，页面会自动刷新结果。`
        : `已将任务 ${job.jobId} 的 staging candidate promote 到 active 数据集。备份目录：${response.item.publication?.backupDir ?? "-"}`);
      await refreshJobs(response.item.jobId, true);
      await refreshMaintenanceStatus(true);
      await loadJobDetail(response.item.jobId, true);
    } catch (err) {
      const blocker = parsePublishBlocker(err);
      if (blocker) {
        setPublishBlocker(blocker);
      } else {
        setError((err as Error).message);
      }
    } finally {
      setPublishingJobId(null);
    }
  }

  async function handleRollbackJob(job: JatoMonthlyUpdateJob) {
    const confirmed = window.confirm(
      "将把当前 active 数据集恢复到这次 publish 之前的备份，并额外保留一份回滚前快照。继续吗？"
    );
    if (!confirmed) {
      return;
    }
    setRollingBackJobId(job.jobId);
    setError("");
    setNotice("");
    try {
      const response = await api.rollbackJatoMonthlyUpdateJob(job.jobId);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      const operation = response.item.pendingOperation;
      setNotice(operation?.status === "queued" || operation?.status === "running"
        ? `任务 ${job.jobId} 已进入隔离 Rollback 队列；active 尚未改变，页面会自动刷新结果。`
        : `已回滚任务 ${job.jobId} 的 publish。恢复来源：${response.item.publication?.backupDir ?? "-"}；回滚前快照：${response.item.publication?.rollbackBackupDir ?? "-"}`);
      await refreshJobs(response.item.jobId, true);
      await refreshMaintenanceStatus(true);
      await loadJobDetail(response.item.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setRollingBackJobId(null);
    }
  }

  const hasSmartMerge = Boolean(selectedJob?.summaries?.smartMerge);
  const isSmartMerging = smartMergingJobId === selectedJob?.jobId;

  async function handleSmartMerge(job: JatoMonthlyUpdateJob) {
    const confirmed = window.confirm(
      "将对回归国家使用 active 最新数据、前进/持平国家使用 patch 数据，生成 Smart-Merged Candidate。这将在当前 staging 产物基础上重建分区、清单和指纹。继续吗？"
    );
    if (!confirmed) {
      return;
    }
    setSmartMergingJobId(job.jobId);
    setError("");
    setNotice("");
    setPublishBlocker(null);
    try {
      const response = await api.smartMergeJatoMonthlyUpdateCandidate(job.jobId);
      setSelectedJob(response.item);
      setSelectedJobId(response.item.jobId);
      setNotice(`已触发任务 ${job.jobId} 的 Smart Merge，后台合并中，请稍候刷新查看进度。`);
      await refreshJobs(response.item.jobId, true);
      await loadJobDetail(response.item.jobId, true);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSmartMergingJobId(null);
    }
  }

  const successCount = jobs.filter((job) => job.status === "success").length;
  const failedCount = jobs.filter((job) => job.status === "failed").length;
  const runningCount = jobs.filter((job) => (
    job.status === "running"
    || job.status === "queued"
    || job.pendingOperation?.status === "queued"
    || job.pendingOperation?.status === "running"
  )).length;

  const artifactEntries = buildMonthlyUpdateArtifactEntries(selectedJob);

  const rawCompare = selectedJob?.summaries?.rawCompare;
  const refresh = selectedJob?.summaries?.refresh;
  const candidateScope = selectedJob?.artifacts?.candidateScope;
  const isOriginallyPartitionScoped = (
    selectedJob?.jobType === "single_country"
    || selectedJob?.jobType === "partial_country"
  );
  const isPartitionScopedReview = (
    candidateScope === "target_country_partition_only"
    || candidateScope === "target_country_partitions_only"
    || (
      isOriginallyPartitionScoped
      && candidateScope !== "full_smart_merge"
    )
  );
  const canReviewSelectedJob = Boolean(
    selectedJob?.artifacts?.rawCompareReportPath
    || selectedJob?.artifacts?.reviewBundlePath
    || (isPartitionScopedReview && selectedJob?.status === "success")
    || (
      candidateScope === "full_smart_merge"
      && selectedJob?.status === "success"
      && selectedJob?.phase === "completed"
    )
  );
  const canPublishSelectedJob = Boolean(
    selectedJob
    && selectedJob.status === "success"
    && selectedJob.phase === "completed"
    && !isPartitionScopedReview
  );
  const canCancelSelectedJob = Boolean(
    selectedJob
    && (selectedJob.status === "queued" || selectedJob.status === "running")
  );
  const selectedRuntimeLog = selectedJob?.runtimeCheck?.log;
  const selectedRuntimeLogUpdatedAt = selectedRuntimeLog && typeof selectedRuntimeLog.updatedAt === "string"
    ? selectedRuntimeLog.updatedAt
    : null;
  const selectedRuntimeLogAge = selectedRuntimeLog && typeof selectedRuntimeLog.ageSeconds === "number"
    ? selectedRuntimeLog.ageSeconds
    : null;
  const hasSelectedJobBeenRolledBack = Boolean(selectedJob?.publication?.rolledBackAt);
  const isSelectedJobPublished = Boolean(
    selectedJob?.publication?.publishedAt && !selectedJob?.publication?.rolledBackAt
  );
  const canRollbackSelectedJob = Boolean(
    selectedJob?.publication?.publishedAt && !selectedJob?.publication?.rolledBackAt
  );
  const hasReviewedSelectedJob = reviewBundle?.jobId === selectedJob?.jobId;
  const hasApprovedSelectedJob = Boolean(
    selectedJob?.reviewApproval?.decision === "approved"
    && reviewBundle?.candidateFingerprint
    && selectedJob.reviewApproval.candidateFingerprint === reviewBundle.candidateFingerprint
  );
  const availableReviewCountries = reviewBundle
    ? Array.from(
      new Set(
        [
          ...reviewBundle.overlapChangeSummary.map((item) => item.country),
          ...reviewBundle.countryFreshnessSummary.map((item) => item.country),
          ...reviewBundle.countryCoverageSummary.map((item) => item.country),
          ...reviewBundle.sampledCountries
        ].filter((item) => Boolean(item))
      )
    )
    : [];
  const activeReviewCountry = availableReviewCountries.includes(selectedReviewCountry ?? "")
    ? selectedReviewCountry
    : (availableReviewCountries[0] ?? null);
  const activeOverlapSummary = activeReviewCountry
    ? reviewBundle?.overlapChangeSummary.find((item) => item.country === activeReviewCountry) ?? null
    : null;
  const activeFreshnessSummary = activeReviewCountry
    ? reviewBundle?.countryFreshnessSummary.find((item) => item.country === activeReviewCountry) ?? null
    : null;
  const activeCoverageSummary = activeReviewCountry
    ? reviewBundle?.countryCoverageSummary.find((item) => item.country === activeReviewCountry) ?? null
    : null;
  const activeMonthlySalesSummary = activeReviewCountry
    ? reviewBundle?.countryMonthlySalesSummary.find((item) => item.country === activeReviewCountry) ?? null
    : null;
  const activeConflictSamples = activeReviewCountry
    ? (reviewBundle?.conflictSamples.filter((item) => item.country === activeReviewCountry) ?? [])
    : (reviewBundle?.conflictSamples ?? []);
  const historicalReclassificationReport = reviewBundle?.historicalReclassificationReport ?? null;
  const historicalReclassificationDecisionCountries = (
    historicalReclassificationReport?.countries.filter(
      (countryReport) => countryReport.decisionRequired
    ) ?? []
  );
  const requiredHistoricalReclassificationCountries = (
    historicalReclassificationReport?.status === "decision_required"
      ? historicalReclassificationDecisionCountries
      : []
  );
  const missingHistoricalReclassificationCountries = requiredHistoricalReclassificationCountries
    .filter((countryReport) => !historicalReclassificationDecisions[countryReport.country])
    .map((countryReport) => countryReport.country);
  const hasCompletedHistoricalReclassificationDecisions = (
    missingHistoricalReclassificationCountries.length === 0
  );
  const appliedHistoricalReclassificationDecisionCount = (
    historicalReclassificationReport?.status === "resolved"
      ? historicalReclassificationDecisionCountries.filter(
        (countryReport) => Boolean(countryReport.decision)
      ).length
      : historicalReclassificationDecisionCountries.length
        - missingHistoricalReclassificationCountries.length
  );
  const hasRecordedResolvedHistoricalReclassificationDecisions = (
    historicalReclassificationReport?.status !== "resolved"
    || appliedHistoricalReclassificationDecisionCount
      === historicalReclassificationDecisionCountries.length
  );
  const safeCleanupMetrics = maintenanceStatus?.storageMetrics.filter((metric) => metric.cleanupTier === "safe") ?? [];
  const cautiousCleanupMetrics = maintenanceStatus?.storageMetrics.filter((metric) => metric.cleanupTier === "cautious") ?? [];
  const protectedCleanupMetrics = maintenanceStatus?.storageMetrics.filter((metric) => metric.cleanupTier === "protected") ?? [];
  const pendingMaintenanceTitle = pendingMaintenanceAction?.kind === "promote-baseline"
    ? "确认保存当前 active 为 baseline"
    : pendingMaintenanceAction
      ? `确认执行${formatCleanupTierLabel(pendingMaintenanceAction.cleanupTier)}`
      : "";
  const pendingMaintenanceDescription = pendingMaintenanceAction?.kind === "promote-baseline"
    ? "系统会读取当前 active parquet，导出新的 baseline xlsx，并把旧 active baseline 自动归档。"
    : pendingMaintenanceAction?.cleanupTier === "cautious"
      ? "谨慎删会在安全删之外删除 raw compare reviews、staging outputs、refresh backups 和 archived baselines/patches；当前 active baseline、active dataset 和最新 patch batch 会保留。"
      : "安全删会归档旧 baseline/patch，删除 upload session cache 和已结束任务的临时上传副本；当前 active baseline、最新 patch batch、staging、refresh backups 和报告会保留。";

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
              <span className="dashboard-hero-chip">Auto batch</span>
              <span className="dashboard-hero-chip">{uploadFile?.name ?? "No file selected"}</span>
            </div>
            <div className="dashboard-hero-rail-actions">
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                onClick={() => {
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
                  void refreshMaintenanceStatus(true);
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
              上传后会自动识别文件里的最新有效月份，并自动生成批次号；仍允许 mixed freshness，只要没有国家回退。
              本次上传覆盖到的国家会直接替换网站当前该国家快照，不会把新旧月份累加到一起；未上传的国家继续沿用 current active。
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
              <span>JATO patch xlsx</span>
              <input
                ref={fileInputRef}
                type="file"
                accept=".xlsx,.xlsm,.xls"
                onChange={handleFileChange}
                className="monthly-update-file-input"
                disabled={monthlyUpdateBusy}
              />
              <div
                className={`monthly-update-dropzone${dragActive ? " is-dragging" : ""}${uploadFile ? " has-file" : ""}${monthlyUpdateBusy ? " is-disabled" : ""}`}
                role="button"
                tabIndex={monthlyUpdateBusy ? -1 : 0}
                aria-disabled={monthlyUpdateBusy}
                onClick={() => {
                  if (!monthlyUpdateBusy) {
                    fileInputRef.current?.click();
                  }
                }}
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
            <label style={{ display: "block", marginBottom: 12, fontSize: 12, fontWeight: 600, color: "#555" }}>
              预期数据月份（可选；系统以工作簿 digest 的真实月份为准，不一致会拒绝）
              <input
                type="text"
                placeholder="2026-04"
                value={uploadMonth ?? ""}
                onChange={(e) => setUploadMonth(e.target.value.trim() || null)}
                disabled={monthlyUpdateBusy}
                style={{
                  display: "block", marginTop: 4, padding: "8px 12px",
                  border: "1px solid #d1d5db", borderRadius: 8, fontSize: 14, width: 200,
                }}
              />
            </label>
            <p className="monthly-update-note">
              系统会读取 Data Export 的国家范围：只覆盖 active 中部分国家时走低内存分区 Review（跳过全量 baseline Raw Compare），覆盖全部国家时才走完整批次管线。分区路径会逐国检查月份回退、字段缺失、历史销量和未上传分区指纹。
            </p>
            <button
              type="submit"
              className="btn btn-primary"
              disabled={monthlyUpdateBusy || uploadFile === null}
            >
              {submitting ? "上传并启动中..." : maintenanceBusy ? "维护进行中..." : "启动月更任务"}
            </button>
          </div>
          {uploadProgress && (
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
              {abandonableUploadId && (
                <div className="monthly-update-upload-progress-detail">
                  <button
                    type="button"
                    className="btn btn-sm btn-secondary"
                    disabled={abandoningUpload}
                    onClick={() => {
                      void handleAbandonUpload();
                    }}
                  >
                    {abandoningUpload ? "正在放弃上传..." : "放弃本次上传"}
                  </button>
                  <span> 放弃后只结束上传会话；candidate 与 active 不会修改。</span>
                </div>
              )}
              {uploadProgress.detail && (
                <div className="monthly-update-upload-progress-detail">
                  {uploadProgress.detail}
                </div>
              )}
              {uploadProgress.ingestDigest && (
                <div className="monthly-update-upload-progress-detail">
                  <strong>
                    {formatUploadRoute(uploadProgress.ingestDigest.route)}
                    {" · "}
                    {formatMonthlyUpdateNumber(uploadProgress.ingestDigest.dataRowCount)} rows
                  </strong>
                  <div>
                    {uploadProgress.ingestDigest.countries.map((country) => (
                      <span key={country} style={{ display: "inline-block", marginRight: 12 }}>
                        {country}: {uploadProgress.ingestDigest?.activeLatestMonths[country] || "-"}
                        {" → "}
                        {uploadProgress.ingestDigest?.countryLatestMonths[country] || "-"}
                      </span>
                    ))}
                  </div>
                  {[
                    ...uploadProgress.ingestDigest.blockers,
                    ...uploadProgress.ingestDigest.warnings,
                  ].map((issue) => (
                    <div key={`${issue.code}-${issue.countries.join(",")}`}>
                      {issue.code}: {issue.message}
                      {issue.sourceFeedback ? `｜给洗数人员：${issue.sourceFeedback}` : ""}
                    </div>
                  ))}
                </div>
              )}
              {uploadProgress.failureDigest && (
                <div className="monthly-update-upload-progress-detail">
                  {uploadProgress.failureDigest.code}: {uploadProgress.failureDigest.message}
                </div>
              )}
            </div>
          )}
        </form>
      </div>

      <div className="card crud-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">How JATO Monthly Update Works</div>
            <p className="section-note">
              月更前向递增原则、严格 Publish 规则和 Smart Merge 选项的简要说明。
            </p>
          </div>
          <button
            type="button"
            className="btn btn-sm btn-secondary"
            onClick={() => setInfoCollapsed((c) => !c)}
          >
            {infoCollapsed ? "展开说明" : "收起说明"}
          </button>
        </div>
        {!infoCollapsed && (
          <div className="monthly-update-info-body">
            <section>
              <h4>Forward-Only Principle（前向递增原则）</h4>
              <p>
                每次 JATO 月更上传的数据只会推进国家的 latest month，不会回退。如果 candidate 中某个国家的最新数据月比 active 更早，视为 <strong>regression</strong>，普通 Publish 会被阻止。
              </p>
            </section>
            <section>
              <h4>Country Regression Check（国家回退检查）</h4>
              <p>
                Publish 前系统逐国家比较 active 与 candidate 的 latest month。如果 candidate 的 latest month 早于 active 的 latest month，Publish 被阻止。此时需要重新上传包含该国家正确月份的 Excel，或使用 Smart Merge。
              </p>
            </section>
            <section>
              <h4>Smart-Merged Candidate（Smart Merge 候选）</h4>
              <p>
                当出现 regression 时，可以选择 Smart Merge：回退国家沿用 active 数据，前进/持平国家使用 patch 数据，未上传国家沿用 active。Smart Merge 创建新 candidate，<em>仍需要 Review → Publish，不直接修改 active。</em>
              </p>
            </section>
            <section>
              <h4>Sales Doubling Protection（2x 重复销量防护）</h4>
              <p>
                Publish 前还会检查 candidate 中重叠月份的销量是否接近 active 的 2x。如果多个重叠月份疑似翻倍，说明可能存在分区文件重复合并或 active/staging 状态不一致。<strong>这种情况 Smart Merge 不可用。</strong>必须重建 candidate 或回滚到正确 active。
              </p>
            </section>
            <section>
              <h4>Worked Example（示例）</h4>
              <pre className="monthly-update-pre">
{`Active:       [SE:2026-03] [DE:2026-03] [FR:2026-03] [NL:2026-01]
Upload:       [SE:2026-02] [DE:2026-03] [NL:2026-02] (FR not in upload)
Candidate:    [SE:2026-02] [DE:2026-03] [NL:2026-02] [FR:(active)2026-03]
→ SE regression (2026-03 → 2026-02) → Publish blocked
Smart Merge:  [SE:keep active 2026-03] [DE:patch 2026-03] [NL:patch 2026-02] [FR:active 2026-03]
→ SE stays at active, all others advance → Review → Publish`}
              </pre>
            </section>
          </div>
        )}
      </div>

      <div className="card crud-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">Cleanup Reminder</div>
            <p className="section-note">
              这块会固定显示，作为你每次月更前后的清理提醒、baseline 固化入口和磁盘占用概览。
            </p>
          </div>
          <div className="monthly-update-cleanup-actions">
	            <button
	              type="button"
	              className="btn btn-secondary"
	              onClick={requestPromoteBaseline}
              disabled={monthlyUpdateBusy}
	            >
	              {baselinePromotion?.status === "queued"
	                ? "保存排队中..."
	                : promotingBaseline
	                  ? "保存中..."
	                  : "保存当前 active 为 baseline"}
	            </button>
            <div className="filter-group" style={{ minWidth: 180 }}>
              <label>一键清理级别</label>
              <select
	                value={selectedCleanupTier}
	                onChange={(event) => setSelectedCleanupTier(event.target.value as "safe" | "cautious")}
                disabled={monthlyUpdateBusy}
	              >
	                <option value="safe">安全删（推荐）</option>
	                <option value="cautious">谨慎删</option>
              </select>
            </div>
            <button
	              type="button"
	              className="btn btn-secondary"
	              onClick={requestCleanup}
              disabled={monthlyUpdateBusy}
	            >
	              {cleanupRunning ? "清理中..." : `执行${formatCleanupTierLabel(selectedCleanupTier)}`}
	            </button>
          </div>
        </div>
	        <div className="alert alert-warning monthly-update-reminder">
	          <strong>建议保留策略：</strong> baseline 目录只保留一个当前激活最新 baseline；旧 baseline 和旧 patch 做归档，
	          不要继续堆在 active 目录里；job 目录中的已结束任务临时上传副本可清理。
	        </div>
	        {maintenanceError && (
	          <div className="alert alert-error monthly-update-maintenance-feedback">
	            {maintenanceError}
	          </div>
	        )}
	        {maintenanceNotice && !maintenanceError && (
	          <div className="alert alert-info monthly-update-maintenance-feedback">
	            {maintenanceNotice}
	          </div>
	        )}
	        {pendingMaintenanceAction && (
	          <div className="monthly-update-maintenance-confirm">
	            <div>
	              <strong>{pendingMaintenanceTitle}</strong>
	              <span>{pendingMaintenanceDescription}</span>
	            </div>
	            <div className="crud-row-actions">
	              <button
	                type="button"
	                className="btn btn-primary"
                disabled={monthlyUpdateBusy}
	                onClick={() => {
	                  if (pendingMaintenanceAction.kind === "promote-baseline") {
	                    void executePromoteBaseline();
	                    return;
	                  }
	                  void executeCleanup(pendingMaintenanceAction.cleanupTier);
	                }}
	              >
	                确认执行
	              </button>
	              <button
	                type="button"
	                className="btn btn-secondary"
	                disabled={maintenanceBusy}
	                onClick={() => {
	                  setPendingMaintenanceAction(null);
	                  setMaintenanceNotice("");
	                  setMaintenanceError("");
	                }}
	              >
	                取消
	              </button>
	            </div>
	          </div>
	        )}
	        {maintenanceStatus && (
	          <div className="monthly-update-cleanup-result">
            <div className="monthly-update-cleanup-result-head">
              <span className="card-title">Runtime Snapshot</span>
              <span className="section-note">
                {formatMonthlyUpdateTimestamp(maintenanceStatus.checkedAt)}
                {maintenanceLoading ? " · 刷新中" : ""}
              </span>
            </div>
            <div className="monthly-update-cleanup-summary">
              <span>active baseline: {maintenanceStatus.activeBaselinePath ?? "-"}</span>
              <span>baseline source: {maintenanceStatus.activeBaselineSource ?? "-"}</span>
              <span>latest patch batch: {maintenanceStatus.latestPatchBatch ?? "-"}</span>
              <span>tracked disk: {formatMonthlyUpdateFileSize(maintenanceStatus.trackedStorageBytes)}</span>
              <span>jobs: {formatMonthlyUpdateNumber(maintenanceStatus.jobCount)}</span>
              <span>upload sessions: {formatMonthlyUpdateNumber(maintenanceStatus.uploadSessionCount)}</span>
            </div>
          </div>
        )}
        {baselinePromotion && (
          <div className="monthly-update-cleanup-result">
            <div className="monthly-update-cleanup-result-head">
              <span className="card-title">Baseline Save</span>
              <span className="section-note">
                {formatMonthlyUpdateTimestamp(
                  baselinePromotion.finishedAt
                  ?? baselinePromotion.startedAt
                  ?? baselinePromotion.requestedAt
                )}
              </span>
            </div>
            <div className="monthly-update-cleanup-summary">
              <span>status: {formatBaselinePromotionStatus(baselinePromotion.status)}</span>
              <span>operation: {baselinePromotion.operationId || "-"}</span>
              <span>requested by: {baselinePromotion.requestedBy ?? "-"}</span>
              <span>source fingerprint: {formatDigestPreview(baselinePromotion.sourceActiveFingerprint)}</span>
              {baselinePromotion.status === "success" && (
                <>
                  <span>baseline: {baselinePromotion.baselinePath ?? "-"}</span>
                  <span>latest month: {baselinePromotion.detectedLatestMonth ?? "-"}</span>
                  <span>countries: {formatMonthlyUpdateNumber(baselinePromotion.countryCount)}</span>
                  <span>rows: {formatMonthlyUpdateNumber(baselinePromotion.rowCount)}</span>
                  <span>archived baselines: {formatMonthlyUpdateNumber(baselinePromotion.archivedBaselineCount)}</span>
                </>
              )}
              {baselinePromotion.status === "failed" && (
                <>
                  <span>error: {baselinePromotion.error ?? baselinePromotion.failureDigest?.message ?? "-"}</span>
                  <span>source feedback: {baselinePromotion.failureDigest?.sourceFeedback ?? "-"}</span>
                </>
              )}
            </div>
          </div>
        )}
        {cleanupResult && (
          <div className="monthly-update-cleanup-result">
            <div className="monthly-update-cleanup-result-head">
              <span className="card-title">Last Cleanup</span>
              <span className="section-note">
                {formatMonthlyUpdateTimestamp(cleanupResult.cleanedAt)}
              </span>
            </div>
            <div className="monthly-update-cleanup-summary">
              <span>cleanup tier: {formatCleanupTierLabel(cleanupResult.cleanupTier)}</span>
              <span>freed: {formatMonthlyUpdateFileSize(cleanupResult.freedBytes)}</span>
              <span>active baseline: {cleanupResult.activeBaselinePath ?? "-"}</span>
              <span>active patch batch: {cleanupResult.activePatchMonth ?? "-"}</span>
              <span>archived baselines: {formatMonthlyUpdateNumber(cleanupResult.archivedBaselineCount)}</span>
              <span>archived patch dirs: {formatMonthlyUpdateNumber(cleanupResult.archivedPatchDirCount)}</span>
              <span>removed upload sessions: {formatMonthlyUpdateNumber(cleanupResult.removedUploadSessionDirCount)}</span>
              <span>removed upload dirs: {formatMonthlyUpdateNumber(cleanupResult.removedJobUploadDirCount)}</span>
              <span>deleted staging dirs: {formatMonthlyUpdateNumber(cleanupResult.deletedStagingDirCount)}</span>
              <span>deleted backup dirs: {formatMonthlyUpdateNumber(cleanupResult.deletedRefreshBackupDirCount)}</span>
            </div>
          </div>
        )}
        {maintenanceStatus && (
          <div className="monthly-update-summary-grid" style={{ marginBottom: 16 }}>
            {maintenanceStatus.storageMetrics.map((metric) => (
              <article key={metric.key} className="monthly-update-summary-card">
                <span>{metric.label}</span>
                <strong>{formatMonthlyUpdateFileSize(metric.bytes)}</strong>
                <small>
                  {formatMonthlyUpdateNumber(metric.fileCount)} files · {metric.paths[0] ?? "-"}
                </small>
              </article>
            ))}
          </div>
        )}
        <div className="monthly-update-cleanup-grid">
          <article className="monthly-update-cleanup-card">
            <span>安全删</span>
            <strong>{formatMonthlyUpdateFileSize(sumStorageMetricBytes(safeCleanupMetrics))}</strong>
            <small>
              {safeCleanupMetrics.map((metric) => metric.label).join(" / ") || "upload session cache / job upload copies"}
              。会归档旧 baseline / patch，并删除 upload session 与已结束任务上传副本。
            </small>
          </article>
          <article className="monthly-update-cleanup-card">
            <span>谨慎删</span>
            <strong>{formatMonthlyUpdateFileSize(sumStorageMetricBytes(cautiousCleanupMetrics))}</strong>
            <small>
              {cautiousCleanupMetrics.map((metric) => metric.label).join(" / ") || "archived baselines / raw compare reviews / staging outputs / refresh backups"}
              。可明显降盘，但会影响回看、重建和 rollback。
            </small>
          </article>
          <article className="monthly-update-cleanup-card">
            <span>不要删</span>
            <strong>{formatMonthlyUpdateFileSize(sumStorageMetricBytes(protectedCleanupMetrics))}</strong>
            <small>
              {protectedCleanupMetrics.map((metric) => metric.label).join(" / ") || "active baseline / patch batches / active dataset"}
              。这部分是当前运行或重建所需核心数据，不进入一键删除。
            </small>
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
                    <th>Data month</th>
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
                      <td>
                        <strong>{job.month}</strong>
                        {job.batchId && (
                          <div className="section-note">{job.batchId}</div>
                        )}
                      </td>
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
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => void handleRecheckJob(selectedJob)}
                    disabled={recheckingJobId === selectedJob.jobId}
                  >
                    {recheckingJobId === selectedJob.jobId ? "查验中..." : "刷新查验"}
                  </button>
                  {canCancelSelectedJob && (
                    <button
                      type="button"
                      className="btn btn-danger"
                      onClick={() => void handleCancelJob(selectedJob)}
                      disabled={cancellingJobId === selectedJob.jobId}
                    >
                      {cancellingJobId === selectedJob.jobId ? "终止中..." : "终止任务"}
                    </button>
                  )}
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
                  {canPublishSelectedJob
                    && hasReviewedSelectedJob
                    && historicalReclassificationReport?.status !== "decision_required" && (
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => void handleApproveReview(selectedJob)}
                      disabled={
                        hasApprovedSelectedJob
                        || approvingReviewJobId === selectedJob.jobId
                        || !hasRecordedResolvedHistoricalReclassificationDecisions
                        || Boolean(reviewBundle?.reviewFindings.some((finding) => finding.severity === "blocker"))
                      }
                    >
                      {hasApprovedSelectedJob
                        ? "Review Approved"
                        : approvingReviewJobId === selectedJob.jobId
                          ? "批准中..."
                          : "Approve Review"}
                    </button>
                  )}
                  {hasReviewedSelectedJob
                    && historicalReclassificationReport?.status === "decision_required" && (
                    <button
                      type="button"
                      className="btn btn-primary"
                      onClick={() => void handleResolveHistoricalReclassification(selectedJob)}
                      disabled={
                        !hasCompletedHistoricalReclassificationDecisions
                        || resolvingHistoricalReclassificationJobId === selectedJob.jobId
                      }
                    >
                      {resolvingHistoricalReclassificationJobId === selectedJob.jobId
                        ? "正在生成完整 Candidate..."
                        : !hasCompletedHistoricalReclassificationDecisions
                          ? `还需选择 ${missingHistoricalReclassificationCountries.length} 个国家`
                          : "应用选择并生成完整 Candidate"}
                    </button>
                  )}
                  {canPublishSelectedJob && (
                    <button
                      type="button"
                      className="btn btn-primary"
                      onClick={() => void handlePublishJob(selectedJob)}
                      disabled={
                        isSelectedJobPublished
                        || !hasApprovedSelectedJob
                        || publishingJobId === selectedJob.jobId
                        || hasActiveJob
                      }
                    >
                      {isSelectedJobPublished
                        ? "Published"
                        : !hasApprovedSelectedJob
                          ? "先批准 Review 再 Publish"
                          : publishingJobId === selectedJob.jobId
                            ? "Publishing..."
                            : "Publish Candidate"}
                    </button>
                  )}
                  {(canRollbackSelectedJob || hasSelectedJobBeenRolledBack) && (
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={() => void handleRollbackJob(selectedJob)}
                      disabled={
                        hasSelectedJobBeenRolledBack
                        || rollingBackJobId === selectedJob.jobId
                        || hasActiveJob
                      }
                    >
                      {hasSelectedJobBeenRolledBack
                        ? "Rolled Back"
                        : rollingBackJobId === selectedJob.jobId
                          ? "Rolling back..."
                          : "Rollback Publish"}
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
                  <span>Detected latest month</span>
                  <strong>{selectedJob.month}</strong>
                </div>
                <div className="admin-detail-item">
                  <span>Batch</span>
                  <strong>{selectedJob.batchId || selectedJob.plan?.batchId || "-"}</strong>
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

              {selectedJob.currentProcess && (
                <div className="alert alert-info">
                  当前子进程：PID {selectedJob.currentProcess.pid}
                  {" · "}
                  {selectedJob.currentProcess.label || "-"}
                  {" · heartbeat "}
                  {formatMonthlyUpdateTimestamp(selectedJob.currentProcess.lastHeartbeatAt)}
                </div>
              )}

              {selectedJob.runtimeCheck && (
                <div className="alert alert-info">
                  刷新查验：{formatMonthlyUpdateTimestamp(selectedJob.runtimeCheck.checkedAt)}
                  {" · process "}
                  {selectedJob.runtimeCheck.processAlive ? "alive" : "not found"}
                  {" · worker "}
                  {selectedJob.runtimeCheck.workerAlive ? "alive" : "not found"}
                  {" · thread "}
                  {selectedJob.runtimeCheck.threadAlive ? "alive" : "not found"}
                  {" · log "}
                  {formatMonthlyUpdateTimestamp(selectedRuntimeLogUpdatedAt)}
                  {selectedRuntimeLogAge !== null ? ` (${formatMonthlyUpdateSeconds(selectedRuntimeLogAge)})` : ""}
                  {selectedJob.runtimeCheck.resolvedAs ? ` · ${selectedJob.runtimeCheck.resolvedAs}` : ""}
                </div>
              )}

              {selectedJob.error && (
                <div className="alert alert-error">{selectedJob.error}</div>
              )}
              {selectedJob.failureDigest && (
                <div className="monthly-update-feedback-cell is-blocker">
                  <strong>
                    {selectedJob.failureDigest.code} · {selectedJob.failureDigest.phase}
                  </strong>
                  <div>{selectedJob.failureDigest.message}</div>
                  {selectedJob.failureDigest.sourceFeedback && (
                    <div>处理建议：{selectedJob.failureDigest.sourceFeedback}</div>
                  )}
                </div>
              )}

              {selectedJob.pendingOperation && (
                <div className={`monthly-update-feedback-cell ${
                  selectedJob.pendingOperation.status === "failed"
                    ? "is-blocker"
                    : "is-review"
                }`}>
                  <strong>
                    {selectedJob.pendingOperation.type === "publish" ? "Publish" : "Rollback"}
                    {" · "}
                    {selectedJob.pendingOperation.status}
                  </strong>
                  <div>
                    请求人：{selectedJob.pendingOperation.requestedBy || "-"}
                    {" · "}
                    {formatMonthlyUpdateTimestamp(selectedJob.pendingOperation.requestedAt)}
                  </div>
                  {(selectedJob.pendingOperation.status === "queued"
                    || selectedJob.pendingOperation.status === "running") && (
                    <div>正在独立 worker 中执行；active 只会在全部校验和预复制完成后切换。</div>
                  )}
                  {selectedJob.pendingOperation.failureDigest && (
                    <>
                      <div>{selectedJob.pendingOperation.failureDigest.message}</div>
                      {selectedJob.pendingOperation.failureDigest.sourceFeedback && (
                        <div>处理建议：{selectedJob.pendingOperation.failureDigest.sourceFeedback}</div>
                      )}
                    </>
                  )}
                </div>
              )}

              {isSelectedJobPublished && (
                <div className="alert alert-success">
                  已 publish 到 active：{formatMonthlyUpdateTimestamp(selectedJob.publication?.publishedAt ?? null)}
                  {" · "}
                  {selectedJob.publication?.publishedBy || "-"}
                  {" · backup "}
                  {selectedJob.publication?.backupDir || "-"}
                </div>
              )}

              {hasSelectedJobBeenRolledBack && (
                <div className="alert alert-warning">
                  已从 active 回滚本次 publish：{formatMonthlyUpdateTimestamp(selectedJob.publication?.rolledBackAt ?? null)}
                  {" · "}
                  {selectedJob.publication?.rolledBackBy || "-"}
                  {" · restore-pre backup "}
                  {selectedJob.publication?.rollbackBackupDir || "-"}
                </div>
              )}

              {publishBlocker && selectedJob && (
                <div className="monthly-update-blocker-panel">
                  <div className="detail-section-head">
                    <div>
                      <div className="card-title">Publish Blocked</div>
                      <p className="section-note">{publishBlocker.message}</p>
                      {publishBlocker.sourceFeedback && (
                        <p className="section-note">
                          给洗数人员：{publishBlocker.sourceFeedback}
                        </p>
                      )}
                    </div>
                    <button
                      type="button"
                      className="btn btn-sm btn-ghost"
                      onClick={() => setPublishBlocker(null)}
                    >
                      关闭
                    </button>
                  </div>

                  {publishBlocker.blockerType === "country_regression" && publishBlocker.regressions && (
                    <>
                      <div className="card-title" style={{ marginTop: 10, marginBottom: 8 }}>Country Regressions</div>
                      <div className="table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>Country</th>
                              <th>Active Latest Month</th>
                              <th>Candidate Latest Month</th>
                            </tr>
                          </thead>
                          <tbody>
                            {publishBlocker.regressions.map((reg) => (
                              <tr key={reg.country}>
                                <td><strong>{reg.country}</strong></td>
                                <td>{reg.activeLatestMonth ?? "-"}</td>
                                <td>{reg.candidateLatestMonth ?? "-"}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <div className="monthly-update-blocker-actions" style={{ marginTop: 12 }}>
                        <button type="button" className="btn btn-secondary" disabled>
                          重新上传修正 Excel
                        </button>
                        <button
                          type="button"
                          className="btn btn-secondary"
                          disabled={isSmartMerging || hasSmartMerge}
                          onClick={() => void handleSmartMerge(selectedJob!)}
                          title={hasSmartMerge ? "已执行过 Smart Merge" : "对回归国家使用 active 数据创建合并候选"}
                        >
                          {isSmartMerging ? "合并中..." : hasSmartMerge ? "Smart Merge 已完成" : "创建 Smart-Merged Candidate"}
                        </button>
                      </div>
                    </>
                  )}

                  {publishBlocker.blockerType === "sales_doubling" && publishBlocker.anomalies && (
                    <>
                      <div className="alert alert-warning" style={{ marginTop: 10 }}>
                        <strong>Sales Doubling Detected</strong>
                        <p style={{ margin: "4px 0 0" }}>
                          注意：candidate 数据疑似重复合并，重叠月份销量约为当前 active 的 2x。
                          在重建 candidate 之前不要执行 publish。
                        </p>
                      </div>
                      <div className="monthly-update-blocker-actions" style={{ marginTop: 12 }}>
                        <button type="button" className="btn btn-secondary" disabled>
                          重新上传 / 重建 Candidate
                        </button>
                        <button type="button" className="btn btn-secondary" disabled>
                          查看 Integrity 详情
                        </button>
                      </div>
                    </>
                  )}
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
                        {isPartitionScopedReview
                          ? "部分国家任务 success 只表示分区 Review 候选已生成；它没有改动 active，因此 Dashboard 仍显示原月份。"
                          : "这里集中展示 raw compare checklist 与人工 review 要点；没有 blocker 时仍需 Approve Review 才能 Publish。"}
                      </p>
                    </div>
                    <div className="table-status-chip">
                      <span>Decision</span>
                      <strong>{reviewBundle.decisionSuggestion || "-"}</strong>
                    </div>
                  </div>
                  <div className={reviewBundle.approval?.decision === "approved" ? "alert alert-success" : "alert alert-info"}>
                    {isPartitionScopedReview
                      ? "部分国家候选为隔离 Review 产物，只读验证上传国家和未上传分区；不能直接 Publish。"
                      : reviewBundle.approval?.decision === "approved"
                        ? `Review 已由 ${reviewBundle.approval.reviewedBy || "-"} 批准（${formatMonthlyUpdateTimestamp(reviewBundle.approval.reviewedAt)}）。`
                        : "Review 尚未批准；核对所有 findings 后再执行 Approve Review。"}
                  </div>
                  {historicalReclassificationReport && historicalReclassificationReport.countries.length > 0 && (
                    <section className="monthly-update-reclassification">
                      <div className="detail-section-head">
                        <div>
                          <div className="card-title">Historical Classification Changes</div>
                          <p className="section-note">
                            这里展示 washed 文件对 active 历史分析标签的重新分桶。销量没有累加；
                            只有逐月总销量稳定的国家才允许在“采用最新分类”和“保留 active 历史”之间选择。
                          </p>
                        </div>
                        <div className="table-status-chip">
                          <span>Decision progress</span>
                          <strong>
                            {appliedHistoricalReclassificationDecisionCount}
                            /{historicalReclassificationDecisionCountries.length}
                          </strong>
                        </div>
                      </div>
                      {missingHistoricalReclassificationCountries.length > 0 && (
                        <div className="alert alert-warning">
                          批准已锁定：请为以下国家明确选择历史处理方式：
                          {missingHistoricalReclassificationCountries.join("、")}。系统不会默认替你选择。
                        </div>
                      )}
                      {historicalReclassificationReport.status === "resolved" && (
                        <div className={
                          hasRecordedResolvedHistoricalReclassificationDecisions
                            ? "alert alert-success"
                            : "alert alert-warning"
                        }>
                          {hasRecordedResolvedHistoricalReclassificationDecisions
                            ? "历史分类选择已应用到当前完整 Candidate。请重新核对本报告与 findings，然后再执行标准 Approve Review；active 仍未改变。"
                            : "报告缺少部分国家的已应用选择。为避免无法审计的默认行为，Approve Review 已锁定；请刷新 Review。"}
                        </div>
                      )}
                      <div className="monthly-update-reclassification-list">
                        {historicalReclassificationReport.countries.map((countryReport) => {
                          const selectedDecision = historicalReclassificationDecisions[countryReport.country];
                          const decisionGroupName = `historical-reclassification-${countryReport.country}`;
                          return (
                            <article
                              key={countryReport.country}
                              className="monthly-update-reclassification-country"
                            >
                              <div className="monthly-update-reclassification-country-head">
                                <div>
                                  <h4>{countryReport.country}</h4>
                                  <p>
                                    比较至 {countryReport.comparedThrough || "-"} ·
                                    {" "}{formatMonthlyUpdateNumber(countryReport.historicalMonthCount)} 个历史月份
                                  </p>
                                </div>
                                <div className="monthly-update-reclassification-kpis">
                                  <span>
                                    组合差异格
                                    <strong>{formatMonthlyUpdateNumber(countryReport.jointMismatchCellCount)}</strong>
                                  </span>
                                  <span>
                                    重新分桶销量
                                    <strong>{formatMonthlyUpdateNumber(countryReport.jointMovedSales)}</strong>
                                  </span>
                                  <span className={countryReport.monthlyTotalsStable ? "is-safe" : "is-danger"}>
                                    逐月总销量
                                    <strong>{countryReport.monthlyTotalsStable ? "一致" : "不一致"}</strong>
                                  </span>
                                </div>
                              </div>

                              {historicalReclassificationReport.status === "decision_required"
                                && countryReport.decisionRequired && (
                                <fieldset className="monthly-update-reclassification-decisions">
                                  <legend>选择 {countryReport.country} 的历史分类处理方式（必选）</legend>
                                  <label className={selectedDecision === "use_latest" ? "is-selected" : ""}>
                                    <input
                                      type="radio"
                                      name={decisionGroupName}
                                      value="use_latest"
                                      checked={selectedDecision === "use_latest"}
                                      onChange={() => setHistoricalReclassificationDecisions((current) => ({
                                        ...current,
                                        [countryReport.country]: "use_latest",
                                      }))}
                                    />
                                    <span>
                                      <strong>采用最新 washed 分类</strong>
                                      <small>以本次文件为准，替换该国家受影响历史月份的分析标签。</small>
                                    </span>
                                  </label>
                                  <label className={selectedDecision === "keep_active" ? "is-selected" : ""}>
                                    <input
                                      type="radio"
                                      name={decisionGroupName}
                                      value="keep_active"
                                      checked={selectedDecision === "keep_active"}
                                      onChange={() => setHistoricalReclassificationDecisions((current) => ({
                                        ...current,
                                        [countryReport.country]: "keep_active",
                                      }))}
                                    />
                                    <span>
                                      <strong>保留当前 active 历史</strong>
                                      <small>历史月份沿用现有标签，只让本次上传覆盖允许推进的月份。</small>
                                    </span>
                                  </label>
                                </fieldset>
                              )}
                              {historicalReclassificationReport.status === "resolved"
                                && countryReport.decisionRequired && (
                                <div className={
                                  countryReport.decision ? "alert alert-success" : "alert alert-warning"
                                }>
                                  {countryReport.decision === "use_latest"
                                    ? "已应用选择：采用最新 washed 分类。当前完整 Candidate 使用本次文件中的历史分析标签。"
                                    : countryReport.decision === "keep_active"
                                      ? "已应用选择：保留当前 active 历史。当前完整 Candidate 沿用 active 的历史分析标签。"
                                      : "当前报告已标记 resolved，但未返回该国家的已应用选择；请刷新 Review 后再批准。"}
                                </div>
                              )}

                              <div className="table-wrapper">
                                <table className="data-table">
                                  <thead>
                                    <tr>
                                      <th>分析字段</th>
                                      <th>差异格</th>
                                      <th>重新分桶销量</th>
                                      <th>主要旧值</th>
                                      <th>主要新值</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {countryReport.dimensionSummaries.map((dimension) => (
                                      <tr key={`${countryReport.country}-${dimension.dimension}`}>
                                        <td>{dimension.dimension || "-"}</td>
                                        <td>{formatMonthlyUpdateNumber(dimension.mismatchCellCount)}</td>
                                        <td>{formatMonthlyUpdateNumber(dimension.movedSales)}</td>
                                        <td>
                                          {dimension.oldValues.map((item) => (
                                            `${item.value || "?"} (${formatMonthlyUpdateNumber(item.sales)})`
                                          )).join(" · ") || "-"}
                                        </td>
                                        <td>
                                          {dimension.newValues.map((item) => (
                                            `${item.value || "?"} (${formatMonthlyUpdateNumber(item.sales)})`
                                          )).join(" · ") || "-"}
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>

                              <details className="monthly-update-reclassification-details">
                                <summary>
                                  查看可精确配对的 old → new 明细
                                  （展示 {formatMonthlyUpdateNumber(countryReport.exactChanges.length)}
                                  /{formatMonthlyUpdateNumber(countryReport.exactChangeCount)} 项）
                                </summary>
                                {countryReport.exactChanges.length === 0 ? (
                                  <div className="crud-empty-state">暂无可精确配对的变化</div>
                                ) : (
                                  <div className="table-wrapper">
                                    <table className="data-table">
                                      <thead>
                                        <tr>
                                          <th>字段</th>
                                          <th>Make / Model</th>
                                          <th>Old → New</th>
                                          <th>月份</th>
                                          <th>销量</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {countryReport.exactChanges.map((change, index) => {
                                          const monthlyTransferTitle = change.monthlyTransfers
                                            .map((item) => `${item.month}: ${formatMonthlyUpdateNumber(item.sales)}`)
                                            .join(" · ");
                                          return (
                                            <tr
                                              key={[
                                                countryReport.country,
                                                change.dimension,
                                                change.make,
                                                change.model,
                                                change.oldValue,
                                                change.newValue,
                                                index,
                                              ].join("-")}
                                            >
                                              <td>{change.dimension || "-"}</td>
                                              <td>{[change.make, change.model].filter(Boolean).join(" ") || "-"}</td>
                                              <td>
                                                <span className="monthly-update-reclassification-change">
                                                  <del>{change.oldValue || "?"}</del>
                                                  <span aria-hidden="true">→</span>
                                                  <ins>{change.newValue || "?"}</ins>
                                                </span>
                                              </td>
                                              <td title={monthlyTransferTitle || undefined}>
                                                {change.affectedMonths.join(", ") || "-"}
                                              </td>
                                              <td>{formatMonthlyUpdateNumber(change.transferredSales)}</td>
                                            </tr>
                                          );
                                        })}
                                      </tbody>
                                    </table>
                                  </div>
                                )}
                              </details>
                              {countryReport.truncation.truncated && (
                                <div className="alert alert-info">
                                  为控制 Review 体积，页面最多展示
                                  {" "}{formatMonthlyUpdateNumber(countryReport.truncation.exactChangeLimit)}
                                  {" "}条精确变化，并为每个方向展示前
                                  {" "}{formatMonthlyUpdateNumber(
                                    countryReport.truncation.valueLimitPerDirection
                                  )} 个主要值；上方总数仍为完整统计。
                                </div>
                              )}
                              {countryReport.complexChangeCount > 0 && (
                                <div className="alert alert-info">
                                  另有 {formatMonthlyUpdateNumber(countryReport.complexChangeCount)} 项属于合并、
                                  拆分或交叉抵消，无法可靠地一一配对；请选择时以字段汇总和洗数说明为准。
                                </div>
                              )}
                            </article>
                          );
                        })}
                      </div>
                    </section>
                  )}
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
                      <small>{availableReviewCountries.join(", ") || "-"}</small>
                    </article>
                    <article className="monthly-update-summary-card">
                      <span>Refresh status</span>
                      <strong>{reviewBundle.refreshSummary?.jobStatus || refresh?.jobStatus || "-"}</strong>
                      <small>{formatMonthlyUpdateSeconds(reviewBundle.refreshSummary?.jobElapsedSeconds)}</small>
                    </article>
                  </div>
                  <div>
                    {availableReviewCountries.length > 0 && (
                      <div className="crud-toolbar-grid" style={{ marginBottom: 12 }}>
                        <div className="filter-group">
                          <label>Sample country</label>
                          <select
                            value={activeReviewCountry ?? ""}
                            onChange={(event) => setSelectedReviewCountry(event.target.value)}
                          >
                            {availableReviewCountries.map((country) => (
                              <option key={country} value={country}>
                                {country}
                              </option>
                            ))}
                          </select>
                        </div>
                      </div>
                    )}
                    {(activeReviewCountry && (activeOverlapSummary || activeFreshnessSummary || activeCoverageSummary)) && (
                      <div className="monthly-update-summary-grid" style={{ marginBottom: 16 }}>
                        <article className="monthly-update-summary-card">
                          <span>Selected country</span>
                          <strong>{activeReviewCountry || "-"}</strong>
                          <small>
                            candidate {activeFreshnessSummary?.newLatestMonth || "-"}
                            {" / "}
                            baseline {activeFreshnessSummary?.oldLatestMonth || "-"}
                          </small>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Freshness</span>
                          <strong>{activeFreshnessSummary?.freshnessStatus || "-"}</strong>
                          <small>
                            row delta {formatSignedNumber(activeFreshnessSummary?.rowDelta ?? 0)}
                          </small>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Added months</span>
                          <strong>{formatMonthlyUpdateNumber(activeCoverageSummary?.addedMonths.length ?? 0)}</strong>
                          <small>{activeCoverageSummary?.addedMonths.join(", ") || "无新增月份"}</small>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Change rate</span>
                          <strong>{activeOverlapSummary ? activeOverlapSummary.changeRate.toFixed(2) : "-"}</strong>
                          <small>
                            {activeOverlapSummary
                              ? `${formatMonthlyUpdateNumber(activeOverlapSummary.changedRecordCount)} changed`
                              : "-"}
                          </small>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Added / Removed</span>
                          <strong>{formatMonthlyUpdateNumber(activeOverlapSummary?.addedRecordCount ?? 0)}</strong>
                          <small>removed {formatMonthlyUpdateNumber(activeOverlapSummary?.removedRecordCount ?? 0)}</small>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Overlap months</span>
                          <strong>{formatMonthlyUpdateNumber(activeCoverageSummary?.overlappingMonths.length ?? 0)}</strong>
                          <small>{activeCoverageSummary?.overlappingMonths.join(", ") || "-"}</small>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Unchanged</span>
                          <strong>{formatMonthlyUpdateNumber(activeOverlapSummary?.unchangedRecordCount ?? 0)}</strong>
                          <small>{activeOverlapSummary?.compareKeyColumns.join(" / ") || "-"}</small>
                        </article>
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="card-title">Country Monthly Sales Check</div>
                    {reviewBundle.countryMonthlySalesError && (
                      <div className="alert alert-warning" style={{ marginBottom: 12 }}>
                        {reviewBundle.countryMonthlySalesError}
                      </div>
                    )}
                    {!activeReviewCountry ? (
                      <div className="crud-empty-state">先选择一个国家查看逐月销量</div>
                    ) : !activeMonthlySalesSummary || activeMonthlySalesSummary.rows.length === 0 ? (
                      <div className="crud-empty-state">当前国家暂无逐月销量汇总</div>
                    ) : (
                      <div className="table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>Month</th>
                              <th>{reviewBundle.countrySalesReferenceLabel || "Reference"}</th>
                              <th>Candidate</th>
                              <th>Delta</th>
                              <th>Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {activeMonthlySalesSummary.rows.map((row) => (
                              <tr key={`${activeMonthlySalesSummary.country}-${row.month}`}>
                                <td>{row.month}</td>
                                <td>{formatMonthlyUpdateNumber(row.referenceSales)}</td>
                                <td>{formatMonthlyUpdateNumber(row.candidateSales)}</td>
                                <td>
                                  {row.deltaSales === null || row.deltaSales === undefined
                                    ? "-"
                                    : formatSignedNumber(row.deltaSales)}
                                </td>
                                <td>{formatMonthlySalesChangeStatus(row.changeStatus)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
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
                              <th>给洗数方反馈</th>
                            </tr>
                          </thead>
                          <tbody>
                            {reviewBundle.reviewFindings.map((finding, index) => {
                              const findingKey = `${finding.ruleId}-${finding.target}-${index}`;
                              return (
                                <tr key={findingKey}>
                                  <td>{finding.severity}</td>
                                  <td>{finding.target || "-"}</td>
                                  <td>{finding.ruleId || "-"}</td>
                                  <td>{finding.message || "-"}</td>
                                  <td>{formatReviewMetrics(finding.metrics)}</td>
                                  <td>
                                    {finding.sourceFeedback ? (
                                      <div className="monthly-update-feedback-cell">
                                        <span>{finding.sourceFeedback}</span>
                                        <button
                                          type="button"
                                          className="btn btn-secondary"
                                          onClick={() => {
                                            void copySourceFeedback(finding.sourceFeedback!).then((copied) => {
                                              if (copied) setCopiedSourceFeedbackKey(findingKey);
                                            });
                                          }}
                                        >
                                          {copiedSourceFeedbackKey === findingKey ? "已复制" : "复制反馈"}
                                        </button>
                                      </div>
                                    ) : "-"}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                  <div>
                    <div className="card-title">Conflict Samples</div>
                    {activeConflictSamples.length === 0 ? (
                      <div className="crud-empty-state">
                        当前国家暂无字段级样本；可先参考下方 sample key snapshots。后续新任务会按国家保留样本。
                      </div>
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
                            {activeConflictSamples.map((sample, index) => (
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
                  {activeOverlapSummary && (
                    <div>
                      <div className="card-title">Sample Key Snapshots</div>
                      <div className="monthly-update-summary-grid">
                        <article className="monthly-update-summary-card">
                          <span>Added keys</span>
                          <strong>{formatMonthlyUpdateNumber(activeOverlapSummary.sampleAddedKeys.length)}</strong>
                          <pre className="monthly-update-pre">
                            {formatSampleKeyRecords(activeOverlapSummary.sampleAddedKeys)}
                          </pre>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Removed keys</span>
                          <strong>{formatMonthlyUpdateNumber(activeOverlapSummary.sampleRemovedKeys.length)}</strong>
                          <pre className="monthly-update-pre">
                            {formatSampleKeyRecords(activeOverlapSummary.sampleRemovedKeys)}
                          </pre>
                        </article>
                        <article className="monthly-update-summary-card">
                          <span>Changed keys</span>
                          <strong>{formatMonthlyUpdateNumber(activeOverlapSummary.sampleChangedKeys.length)}</strong>
                          <pre className="monthly-update-pre">
                            {formatSampleKeyRecords(activeOverlapSummary.sampleChangedKeys)}
                          </pre>
                        </article>
                      </div>
                    </div>
                  )}
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
    </section>
  );
}
