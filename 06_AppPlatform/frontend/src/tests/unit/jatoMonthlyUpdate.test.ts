import { describe, expect, it } from "vitest";

import type { JatoMonthlyUpdateJob } from "../../types";
import {
  buildMonthlyUpdateArtifactEntries,
  buildMonthlyUpdateUploadResumeKey,
  formatMonthlyUpdateFileSize,
  formatMonthlyUpdateNumber,
  formatMonthlyUpdatePhase,
  formatMonthlyUpdateSeconds,
  formatMonthlyUpdateTimestamp,
  getMonthlyUpdateStatusBadgeClass,
  getMonthlyUpdateRetryDelayMs,
  getMonthlyUpdateUploadStageLabel,
  isMonthlyUpdateUploadFilenameAccepted,
  shouldPollMonthlyUpdateJobs,
} from "../../utils/jatoMonthlyUpdate";

function makeJob(overrides: Partial<JatoMonthlyUpdateJob> = {}): JatoMonthlyUpdateJob {
  return {
    jobId: "jato-update-1",
    month: "2026-03",
    status: "queued",
    phase: "raw_compare",
    triggeredBy: "analyst",
    createdAt: "2026-04-13T00:00:00+00:00",
    updatedAt: "2026-04-13T00:05:00+00:00",
    startedAt: "2026-04-13T00:01:00+00:00",
    finishedAt: null,
    error: null,
    upload: {
      originalFilename: "patch.xlsx",
      storedPath: "uploads/patch.xlsx",
      sizeBytes: 1024,
    },
    plan: {
      path: "01_RAW_DATA/patches/2026-03/monthly_update_plan.md",
      compareId: "2026-02_vs_2026-03",
      compareCommand: "python 03_Scripts/raw_compare_review.py ...",
      refreshCommand: "python 03_Scripts/data_pipeline/run_data_refresh_job.py ...",
    },
    artifacts: {
      baselinePath: "01_RAW_DATA/baseline/JATO-2026.2-full.xlsx",
      stagedPatchPath: "01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx",
      planPath: null,
      reviewDir: "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03",
      rawCompareReportPath: "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03/raw_compare_report.json",
      refreshReportPath: "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json",
      partitionOutputPath: null,
      manifestPath: null,
      fingerprintPath: "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json",
    },
    summaries: {
      rawCompare: {
        compareId: "2026-02_vs_2026-03",
        decisionSuggestion: "manual_review_required",
        compareKeyMode: "auto",
        compareKeyColumns: ["国家", "MakeModel"],
        blockerCount: 1,
        reviewCount: 2,
        infoCount: 0,
        advancedCountryCount: 4,
        regressedCountryCount: 1,
        newCountryCount: 0,
        missingCountryCount: 0,
        addedCountryCount: 1,
        removedCountryCount: 0,
      },
      refresh: {
        jobStatus: "success",
        jobElapsedSeconds: 22.3,
        rowCount: 12000,
        columnCount: 96,
        partitionCount: 21,
        changedRows: 340,
        changedCountryCount: 4,
        fingerprintMatched: false,
        fingerprintUpdated: true,
        conflictGroupCount: 12,
        conflictRowCount: 34,
      },
    },
    logPath: "04_Processed_data/ops/jato_monthly_update_jobs/jato-update-1/job.log",
    logTail: "line1\nline2",
    ...overrides,
  };
}

describe("jato monthly update helpers", () => {
  it("formats timestamps, numbers and seconds defensively", () => {
    expect(formatMonthlyUpdateTimestamp("2026-04-13T00:00:00+00:00")).toBeTruthy();
    expect(formatMonthlyUpdateTimestamp(null)).toBe("-");
    expect(formatMonthlyUpdatePhase("raw_compare")).toBe("raw compare");
    expect(formatMonthlyUpdatePhase("")).toBe("-");
    expect(formatMonthlyUpdateNumber(12345)).toBe("12,345");
    expect(formatMonthlyUpdateNumber(Number.NaN)).toBe("-");
    expect(formatMonthlyUpdateSeconds(22.34)).toBe("22.3s");
    expect(formatMonthlyUpdateSeconds(undefined)).toBe("-");
    expect(formatMonthlyUpdateFileSize(500 * 1024 * 1024)).toBe("500.0 MB");
    expect(formatMonthlyUpdateFileSize(undefined)).toBe("-");
  });

  it("maps statuses to existing badge classes", () => {
    expect(getMonthlyUpdateStatusBadgeClass("success")).toBe("badge-active");
    expect(getMonthlyUpdateStatusBadgeClass("failed")).toBe("badge-danger");
    expect(getMonthlyUpdateStatusBadgeClass("cancelled")).toBe("badge-inactive");
    expect(getMonthlyUpdateStatusBadgeClass("running")).toBe("badge-warning");
    expect(getMonthlyUpdateStatusBadgeClass("queued")).toBe("badge-warning");
    expect(getMonthlyUpdateStatusBadgeClass("completed")).toBe("badge-inactive");
  });

  it("polls while ETL or active-bundle operations are queued or running", () => {
    expect(shouldPollMonthlyUpdateJobs([makeJob({ status: "queued" })])).toBe(true);
    expect(shouldPollMonthlyUpdateJobs([makeJob({ status: "running" })])).toBe(true);
    expect(shouldPollMonthlyUpdateJobs([makeJob({
      status: "success",
      pendingOperation: {
        operationId: "jato-publish-1",
        type: "publish",
        status: "queued",
        requestedAt: "2026-04-13T00:06:00+00:00",
        requestedBy: "admin",
        startedAt: null,
        finishedAt: null,
        error: null,
        failureDigest: null,
      },
    })])).toBe(true);
    expect(shouldPollMonthlyUpdateJobs([makeJob({
      status: "success",
      pendingOperation: {
        operationId: "jato-review-refresh-1",
        type: "review_refresh",
        status: "running",
        requestedAt: "2026-04-13T00:06:00+00:00",
        requestedBy: "admin",
        startedAt: "2026-04-13T00:06:01+00:00",
        finishedAt: null,
        error: null,
        failureDigest: null,
      },
    })])).toBe(true);
    expect(shouldPollMonthlyUpdateJobs([makeJob({ status: "success" })])).toBe(false);
  });

  it("builds artifact entries and falls back to plan path when needed", () => {
    const entries = buildMonthlyUpdateArtifactEntries(makeJob());

    expect(entries).toEqual([
      ["Baseline", "01_RAW_DATA/baseline/JATO-2026.2-full.xlsx"],
      ["Staged patch", "01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx"],
      ["Plan", "01_RAW_DATA/patches/2026-03/monthly_update_plan.md"],
      ["Review dir", "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03"],
      ["Raw compare report", "04_Processed_data/reviews/raw_compare/2026-02_vs_2026-03/raw_compare_report.json"],
      ["Refresh report", "04_Processed_data/staging/2026-03-mixed/refresh_job_report.json"],
      ["Fingerprint", "04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json"],
    ]);
  });

  it("returns an empty artifact list when no job is selected", () => {
    expect(buildMonthlyUpdateArtifactEntries(null)).toEqual([]);
  });

  it("recognizes accepted upload filenames and upload stages", () => {
    expect(isMonthlyUpdateUploadFilenameAccepted("patch.xlsx")).toBe(true);
    expect(isMonthlyUpdateUploadFilenameAccepted("patch.xlsm")).toBe(true);
    expect(isMonthlyUpdateUploadFilenameAccepted("patch.csv")).toBe(false);
    expect(getMonthlyUpdateUploadStageLabel("uploading")).toBe("分片上传中");
    expect(getMonthlyUpdateUploadStageLabel("verifying")).toBe("核对续传文件");
    expect(getMonthlyUpdateUploadStageLabel("retrying")).toBe("分片重试中");
    expect(getMonthlyUpdateUploadStageLabel("queued")).toBe("任务已入队");
    expect(getMonthlyUpdateUploadStageLabel("unknown")).toBe("准备上传");
  });

  it("builds a stable resume key and backoff delays", () => {
    expect(buildMonthlyUpdateUploadResumeKey({
      filename: "patch.xlsx",
      sizeBytes: 10,
      lastModified: 123456,
      probeSha256: "ABCDEF",
    })).toBe("patch.xlsx:10:123456:abcdef");
    expect(getMonthlyUpdateRetryDelayMs(1)).toBe(1200);
    expect(getMonthlyUpdateRetryDelayMs(3)).toBe(4800);
  });
});
