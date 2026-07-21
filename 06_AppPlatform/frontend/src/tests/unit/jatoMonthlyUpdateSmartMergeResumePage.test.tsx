// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { JatoMonthlyUpdatePage } from "../../pages/JatoMonthlyUpdatePage";
import type {
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateMaintenanceStatus,
  JatoMonthlyUpdateSmartMergeRecovery,
} from "../../types";

vi.mock("../../api/client", () => ({
  api: {
    listJatoMonthlyUpdateJobs: vi.fn(),
    getJatoMonthlyUpdateJob: vi.fn(),
    getJatoMonthlyUpdateMaintenanceStatus: vi.fn(),
    resumeJatoMonthlyUpdateSmartMerge: vi.fn(),
    retryFailedJatoMonthlyUpdateJob: vi.fn(),
  },
}));

const requestId = "5ebd4dba-09cb-4dfa-8bcc-d6d465caad0e";
const sourceCandidateFingerprint = "a".repeat(64);
const activeBaseFingerprint = "b".repeat(64);
const reportFingerprint = "c".repeat(64);
const resolutionFingerprint = "d".repeat(64);
const smartMergeRecovery: JatoMonthlyUpdateSmartMergeRecovery = {
  canResume: true,
  sourceCandidateFingerprint,
  activeBaseFingerprint,
  reportFingerprint,
  resolutionFingerprint,
};

function makeJob(overrides: Partial<JatoMonthlyUpdateJob> = {}): JatoMonthlyUpdateJob {
  return {
    jobId: "jato-update-171a8b20",
    month: "2026-06",
    batchId: "2026-06-r1",
    jobType: "partial_country",
    countryScope: ["丹麦", "德国"],
    status: "failed",
    phase: "smart_merge_failed",
    triggeredBy: "admin",
    createdAt: "2026-07-21T15:00:00+00:00",
    updatedAt: "2026-07-21T15:29:38+00:00",
    startedAt: "2026-07-21T15:01:00+00:00",
    finishedAt: "2026-07-21T15:29:38+00:00",
    error: "读取 Smart Merge active（德国）的德国分区失败。",
    activeBaseFingerprint,
    upload: {
      originalFilename: "jato-202606-16-countries.xlsx",
      storedPath: "04_Processed_data/ops/jato-update-171a8b20/uploads/jato.xlsx",
    },
    plan: null,
    artifacts: {
      candidateScope: "target_country_partitions_only",
    },
    summaries: null,
    smartMergeRecovery,
    ...overrides,
  };
}

function makeMaintenanceStatus(): JatoMonthlyUpdateMaintenanceStatus {
  return {
    checkedAt: "2026-07-21T15:30:00+00:00",
    activeBaselinePath: "01_RAW_DATA/baseline/JATO-2026.5.xlsx",
    activeBaselineSource: "active",
    latestPatchBatch: "2026-06-r1",
    jobCount: 1,
    uploadSessionCount: 0,
    baselinePromotion: null,
    trackedStorageBytes: 0,
    storageMetrics: [],
  };
}

describe("JATO Smart Merge safe resume interaction", () => {
  const failedJob = makeJob();

  beforeEach(() => {
    vi.resetAllMocks();
    vi.stubGlobal("crypto", {
      randomUUID: vi.fn(() => requestId),
    });
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [failedJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(),
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
  });

  afterEach(() => {
    cleanup();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("submits once and only reads job state after an ambiguous response", async () => {
    let submittedRequestId: string | null = null;
    const acceptedJob = makeJob({
      smartMergeRecovery: {
        ...smartMergeRecovery,
        canResume: false,
      },
      pendingOperation: {
        operationId: "jato-smart_merge_resume-1",
        type: "smart_merge_resume",
        status: "running",
        phase: "smart_merging",
        requestId,
        requestedAt: "2026-07-22T00:00:00+00:00",
        requestedBy: "admin",
        startedAt: "2026-07-22T00:00:01+00:00",
        finishedAt: null,
        error: null,
        failureDigest: null,
        expectedSourceCandidateFingerprint: sourceCandidateFingerprint,
        expectedActiveFingerprint: activeBaseFingerprint,
        expectedReportFingerprint: reportFingerprint,
        expectedResolutionFingerprint: resolutionFingerprint,
      },
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockImplementation(async () => ({
      item: submittedRequestId ? acceptedJob : failedJob,
    }));
    vi.mocked(api.resumeJatoMonthlyUpdateSmartMerge).mockImplementation(
      async (_jobId, payload) => {
        submittedRequestId = payload.requestId;
        throw new Error("502 Bad Gateway");
      },
    );

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    const resumeButton = await screen.findByRole("button", {
      name: "仅续跑 Smart Merge",
    });
    expect(screen.queryByRole("button", { name: "Retry Failed Job" })).toBeNull();
    fireEvent.click(resumeButton);
    fireEvent.click(resumeButton);

    await waitFor(() => {
      expect(api.resumeJatoMonthlyUpdateSmartMerge).toHaveBeenCalledTimes(1);
      expect(screen.getByText(/页面将只读轮询状态，不会重复提交/)).toBeTruthy();
    });
    expect(api.resumeJatoMonthlyUpdateSmartMerge).toHaveBeenCalledWith(
      failedJob.jobId,
      {
        requestId,
        expectedSourceCandidateFingerprint: sourceCandidateFingerprint,
        expectedActiveFingerprint: activeBaseFingerprint,
        expectedReportFingerprint: reportFingerprint,
        expectedResolutionFingerprint: resolutionFingerprint,
      },
    );
    expect(api.retryFailedJatoMonthlyUpdateJob).not.toHaveBeenCalled();
    expect(api.getJatoMonthlyUpdateJob).toHaveBeenCalledTimes(2);
    expect(window.confirm).toHaveBeenCalledWith(expect.stringContaining("不重跑 ETL"));
    expect(window.confirm).toHaveBeenCalledWith(expect.stringContaining("不会修改 active"));
    expect(screen.getByText(/Smart Merge 安全续跑 · running/)).toBeTruthy();
  });

  it("keeps the dedicated action visible but locked when recovery seals fail", async () => {
    const blockedJob = makeJob({
      smartMergeRecovery: {
        ...smartMergeRecovery,
        canResume: false,
      },
    });
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [blockedJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: blockedJob });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    const resumeButton = await screen.findByRole("button", {
      name: "仅续跑 Smart Merge",
    }) as HTMLButtonElement;
    expect(resumeButton.disabled).toBe(true);
    expect(screen.queryByRole("button", { name: "Retry Failed Job" })).toBeNull();
  });
});
