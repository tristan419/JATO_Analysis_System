// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { JatoMonthlyUpdatePage } from "../../pages/JatoMonthlyUpdatePage";
import type {
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateMaintenanceStatus,
  JatoMonthlyUpdateReviewBundle,
  JatoMonthlyUpdateReviewIssue,
} from "../../types";

vi.mock("../../api/client", () => ({
  api: {
    listJatoMonthlyUpdateJobs: vi.fn(),
    getJatoMonthlyUpdateJob: vi.fn(),
    getJatoMonthlyUpdateMaintenanceStatus: vi.fn(),
    getJatoMonthlyUpdateReview: vi.fn(),
    refreshJatoMonthlyUpdateReview: vi.fn(),
    recheckJatoMonthlyUpdateJob: vi.fn(),
    approveJatoMonthlyUpdateReview: vi.fn(),
    publishJatoMonthlyUpdateJob: vi.fn(),
    rollbackJatoMonthlyUpdateJob: vi.fn(),
    smartMergeJatoMonthlyUpdateCandidate: vi.fn(),
    resolveJatoMonthlyUpdateHistoricalReclassification: vi.fn(),
    promoteCurrentActiveToJatoBaseline: vi.fn(),
    runJatoMonthlyUpdateCleanup: vi.fn(),
    createJatoMonthlyUpdateJob: vi.fn(),
    abandonJatoMonthlyUpdateUpload: vi.fn(),
  },
}));

const candidateFingerprint = "a".repeat(64);

function makeJob(overrides: Partial<JatoMonthlyUpdateJob> = {}): JatoMonthlyUpdateJob {
  return {
    jobId: "jato-review-refresh-1",
    month: "2026-06",
    batchId: "2026-06-r1",
    jobType: "full_batch",
    countryScope: ["捷克", "丹麦"],
    status: "success",
    phase: "completed",
    triggeredBy: "admin",
    createdAt: "2026-07-21T00:00:00+00:00",
    updatedAt: "2026-07-21T00:05:00+00:00",
    startedAt: "2026-07-21T00:00:01+00:00",
    finishedAt: "2026-07-21T00:05:00+00:00",
    error: null,
    activeBaseFingerprint: "b".repeat(64),
    upload: null,
    plan: null,
    artifacts: {
      candidateScope: "full_candidate",
      reviewBundlePath: "04_Processed_data/ops/review_bundle.json",
      rawCompareReportPath: "04_Processed_data/reviews/raw_compare_report.json",
    },
    summaries: null,
    reviewApproval: {
      decision: "approved",
      reviewedAt: "2026-07-21T00:06:00+00:00",
      reviewedBy: "admin",
      candidateFingerprint,
      activeBaseFingerprint: "b".repeat(64),
    },
    ...overrides,
  };
}

function makeReview(): JatoMonthlyUpdateReviewBundle {
  return {
    jobId: "jato-review-refresh-1",
    reviewDir: null,
    compareId: "2026-05_vs_2026-06",
    decisionSuggestion: "ready_for_review",
    compareKeyColumns: [],
    checklistMarkdown: null,
    reviewFindings: [],
    sampledCountries: [],
    conflictSampleCount: 0,
    conflictSamples: [],
    overlapChangeSummary: [],
    countryFreshnessSummary: [],
    countryCoverageSummary: [],
    countrySalesReferenceLabel: "active",
    countryMonthlySalesSummary: [],
    countryMonthlySalesError: null,
    timeAxisCheck: {},
    countryScopeSummary: {},
    refreshSummary: null,
    candidateFingerprint,
    approval: null,
    historicalReclassificationReport: {
      status: "not_required",
      countries: [],
      resolutionValidation: [],
    },
  };
}

function makeMaintenanceStatus(): JatoMonthlyUpdateMaintenanceStatus {
  return {
    checkedAt: "2026-07-21T00:05:00+00:00",
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

function staleError(issue: JatoMonthlyUpdateReviewIssue): Error {
  return Object.assign(new Error(`409 ${JSON.stringify(issue)}`), {
    reviewIssue: issue,
  });
}

describe("JATO Review refresh interaction", () => {
  const baseJob = makeJob();

  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [baseJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: baseJob });
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(),
    });
    vi.mocked(api.recheckJatoMonthlyUpdateJob).mockResolvedValue({ item: baseJob });
  });

  afterEach(() => {
    cleanup();
  });

  it("shows the stale contract and never reposts after an ambiguous 502", async () => {
    const issue: JatoMonthlyUpdateReviewIssue = {
      blockerType: "review_bundle_stale",
      reason: "legacy_stat_signature",
      rebuildBlockerReason: null,
      message: "Review bundle 使用旧版或缺失的候选签名。",
      canRebuild: true,
      candidateFingerprint,
      activeBaseFingerprint: "b".repeat(64),
      currentActiveFingerprint: "b".repeat(64),
      reviewRefresh: null,
    };
    vi.mocked(api.getJatoMonthlyUpdateReview)
      .mockResolvedValueOnce({ item: makeReview() })
      .mockRejectedValueOnce(staleError(issue));
    let submittedRequestId = "";
    vi.mocked(api.refreshJatoMonthlyUpdateReview).mockImplementation(
      async (_jobId, requestId) => {
        submittedRequestId = requestId;
        throw new Error("502 Bad Gateway");
      },
    );
    vi.mocked(api.getJatoMonthlyUpdateJob).mockImplementation(async () => ({
      item: submittedRequestId
        ? makeJob({
          reviewApproval: null,
          pendingOperation: {
            operationId: "jato-review_refresh-1",
            type: "review_refresh",
            status: "running",
            phase: "building_review",
            requestId: submittedRequestId,
            requestedAt: "2026-07-21T00:07:00+00:00",
            requestedBy: "admin",
            startedAt: "2026-07-21T00:07:01+00:00",
            finishedAt: null,
            error: null,
            failureDigest: null,
            expectedCandidateFingerprint: candidateFingerprint,
            expectedActiveFingerprint: "b".repeat(64),
          },
        })
        : baseJob,
    }));

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    expect(await screen.findByRole("button", { name: "Publish Candidate" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "收起 Review" }));
    fireEvent.click(screen.getByRole("button", { name: "Review Candidate" }));

    expect(await screen.findByText("Review 已过期")).toBeTruthy();
    expect(screen.getByText("原因：legacy_stat_signature · Candidate aaaaaaaaaaaa…")).toBeTruthy();
    expect((screen.getByRole("button", { name: "先批准 Review 再 Publish" }) as HTMLButtonElement).disabled)
      .toBe(true);

    const refreshButton = screen.getByRole("button", {
      name: "重建 Review（不改 Candidate）",
    });
    fireEvent.click(refreshButton);
    fireEvent.click(refreshButton);

    await waitFor(() => {
      expect(api.refreshJatoMonthlyUpdateReview).toHaveBeenCalledTimes(1);
      expect(screen.getByText(/页面将只读轮询状态，不会重复提交/)).toBeTruthy();
    });
    expect(api.getJatoMonthlyUpdateJob).toHaveBeenCalled();
    expect((screen.getByRole("button", { name: "Review Candidate" }) as HTMLButtonElement).disabled)
      .toBe(true);
    expect((screen.getByRole("button", { name: "先批准 Review 再 Publish" }) as HTMLButtonElement).disabled)
      .toBe(true);
    expect((screen.getByRole("button", { name: "刷新任务状态" }) as HTMLButtonElement).disabled)
      .toBe(false);
  });

  it("explains why the rebuild safety gate is closed", async () => {
    vi.mocked(api.getJatoMonthlyUpdateReview).mockRejectedValue(staleError({
      blockerType: "review_bundle_stale",
      reason: "candidate_metadata_changed",
      rebuildBlockerReason: "active_lineage_changed",
      message: "Candidate 元数据已变化。",
      canRebuild: false,
      candidateFingerprint,
      activeBaseFingerprint: "b".repeat(64),
      currentActiveFingerprint: "c".repeat(64),
      reviewRefresh: null,
    }));

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    expect(await screen.findByText("当前安全门禁不允许重建：active_lineage_changed"))
      .toBeTruthy();
    expect(screen.queryByRole("button", { name: "重建 Review（不改 Candidate）" }))
      .toBeNull();
  });

  it.each(["queued", "running", "success", "failed"] as const)(
    "drops an already-open Review when another tab reports refresh %s",
    async (status) => {
      const pending = status === "queued" || status === "running";
      vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
        item: makeReview(),
      });
      await act(async () => {
        render(<JatoMonthlyUpdatePage />);
      });
      fireEvent.click(await screen.findByRole("button", {
        name: "Review Candidate",
      }));
      expect(await screen.findByRole("button", { name: "Review Approved" }))
        .toBeTruthy();
      expect((screen.getByRole("button", {
        name: "Publish Candidate",
      }) as HTMLButtonElement).disabled).toBe(false);

      const externalJob = makeJob({
        reviewApproval: null,
        pendingOperation: {
          operationId: `jato-review_refresh-${status}`,
          type: "review_refresh",
          status,
          phase: pending ? status : "completed",
          requestId: `review-request-${status}`,
          requestedAt: "2026-07-21T00:07:00+00:00",
          requestedBy: "other-admin",
          startedAt: status === "queued" ? null : "2026-07-21T00:07:01+00:00",
          finishedAt: pending ? null : "2026-07-21T00:07:02+00:00",
          error: status === "failed" ? "worker failed" : null,
          failureDigest: null,
          expectedCandidateFingerprint: candidateFingerprint,
          expectedActiveFingerprint: "b".repeat(64),
        },
      });
      vi.mocked(api.recheckJatoMonthlyUpdateJob).mockResolvedValue({
        item: externalJob,
      });
      vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
        rows: 1,
        items: [externalJob],
      });
      vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({
        item: externalJob,
      });

      fireEvent.click(screen.getByRole("button", { name: "刷新任务状态" }));

      await waitFor(() => {
        expect(screen.queryByRole("button", { name: "Review Approved" }))
          .toBeNull();
        expect(screen.queryByText(/这里集中展示 raw compare checklist/))
          .toBeNull();
      });
      expect((screen.getByRole("button", {
        name: "先批准 Review 再 Publish",
      }) as HTMLButtonElement).disabled).toBe(true);
      const reviewButton = screen.getByRole("button", {
        name: "Review Candidate",
      }) as HTMLButtonElement;
      expect(reviewButton.disabled).toBe(pending);
      expect((screen.getByRole("button", {
        name: "刷新任务状态",
      }) as HTMLButtonElement).disabled).toBe(false);
    },
  );
});
