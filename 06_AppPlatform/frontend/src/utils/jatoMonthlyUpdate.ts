import type { JatoMonthlyUpdateJob } from "../types";

const MONTHLY_UPDATE_ALLOWED_EXTENSIONS = [".xlsx", ".xlsm", ".xls"];
const MONTHLY_UPDATE_RETRY_BASE_DELAY_MS = 1200;

export function formatMonthlyUpdateTimestamp(value?: string | null): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

export function formatMonthlyUpdatePhase(value: string): string {
  return value ? value.replaceAll("_", " ") : "-";
}

export function formatMonthlyUpdateNumber(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toLocaleString();
}

export function formatMonthlyUpdateSeconds(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${value.toFixed(1)}s`;
}

export function formatMonthlyUpdateFileSize(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value) || value < 0) {
    return "-";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  if (value < 1024 * 1024 * 1024) {
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function isMonthlyUpdateUploadFilenameAccepted(filename: string): boolean {
  const normalized = filename.trim().toLowerCase();
  return MONTHLY_UPDATE_ALLOWED_EXTENSIONS.some((suffix) => normalized.endsWith(suffix));
}

export function getMonthlyUpdateUploadStageLabel(stage?: string | null): string {
  switch (stage) {
    case "initiating":
      return "初始化上传会话";
    case "verifying":
      return "核对续传文件";
    case "resuming":
      return "恢复上传会话";
    case "uploading":
      return "分片上传中";
    case "retrying":
      return "分片重试中";
    case "assembling":
      return "服务端组装文件";
    case "digesting":
      return "识别数据范围";
    case "invalid":
      return "文件校验未通过";
    case "creating_job":
      return "创建月更任务";
    case "queued":
      return "任务已入队";
    default:
      return "准备上传";
  }
}

export function buildMonthlyUpdateUploadResumeKey(input: {
  filename: string;
  sizeBytes: number;
  lastModified: number;
  probeSha256: string;
}): string {
  return [
    input.filename.trim(),
    String(input.sizeBytes),
    String(input.lastModified),
    input.probeSha256.trim().toLowerCase(),
  ].join(":");
}

export function getMonthlyUpdateRetryDelayMs(attempt: number): number {
  if (attempt <= 1) {
    return MONTHLY_UPDATE_RETRY_BASE_DELAY_MS;
  }
  return MONTHLY_UPDATE_RETRY_BASE_DELAY_MS * (2 ** (attempt - 1));
}

export function getMonthlyUpdateStatusBadgeClass(status: string): string {
  if (status === "success") {
    return "badge-active";
  }
  if (status === "failed") {
    return "badge-danger";
  }
  if (status === "cancelled") {
    return "badge-inactive";
  }
  if (status === "running" || status === "queued") {
    return "badge-warning";
  }
  return "badge-inactive";
}

export function shouldPollMonthlyUpdateJobs(jobs: JatoMonthlyUpdateJob[]): boolean {
  return jobs.some((job) => (
    job.status === "queued"
    || job.status === "running"
    || job.pendingOperation?.status === "queued"
    || job.pendingOperation?.status === "running"
  ));
}

export function buildMonthlyUpdateArtifactEntries(
  job: JatoMonthlyUpdateJob | null
): Array<[string, string]> {
  if (!job?.artifacts) {
    return [];
  }
  return [
    ["Baseline", job.artifacts.baselinePath],
    ["Staged patch", job.artifacts.stagedPatchPath],
    ["Supplement parquet", job.artifacts.supplementParquetPath],
    ["Plan", job.artifacts.planPath ?? job.plan?.path],
    ["Review dir", job.artifacts.reviewDir],
    ["Raw compare report", job.artifacts.rawCompareReportPath],
    ["Refresh report", job.artifacts.refreshReportPath],
    ["Partition output", job.artifacts.partitionOutputPath],
    ["Manifest", job.artifacts.manifestPath],
    ["Fingerprint", job.artifacts.fingerprintPath],
    ["Summaries", job.artifacts.summariesOutputPath],
    ["Review bundle", job.artifacts.reviewBundlePath],
  ].filter((entry): entry is [string, string] => Boolean(entry[1]));
}
