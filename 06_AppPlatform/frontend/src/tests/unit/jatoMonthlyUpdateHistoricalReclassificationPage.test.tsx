// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { JatoMonthlyUpdatePage } from "../../pages/JatoMonthlyUpdatePage";
import type {
  JatoHistoricalReclassificationDecision,
  JatoHistoricalReclassificationReport,
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateMaintenanceStatus,
  JatoMonthlyUpdateReviewBundle,
} from "../../types";

vi.mock("../../api/client", () => ({
  api: {
    listJatoMonthlyUpdateJobs: vi.fn(),
    getJatoMonthlyUpdateJob: vi.fn(),
    getJatoMonthlyUpdateMaintenanceStatus: vi.fn(),
    getJatoMonthlyUpdateReview: vi.fn(),
    resolveJatoMonthlyUpdateHistoricalReclassification: vi.fn(),
    approveJatoMonthlyUpdateReview: vi.fn(),
    promoteCurrentActiveToJatoBaseline: vi.fn(),
    runJatoMonthlyUpdateCleanup: vi.fn(),
    createJatoMonthlyUpdateJob: vi.fn(),
    abandonJatoMonthlyUpdateUpload: vi.fn(),
  },
}));

function makeArtifacts(candidateScope: string) {
  return {
    candidateScope,
    baselinePath: null,
    stagedPatchPath: null,
    planPath: null,
    reviewDir: "04_Processed_data/reviews/jato-review-1",
    rawCompareReportPath: "04_Processed_data/reviews/jato-review-1/raw_compare_report.json",
    reviewBundlePath: null,
    refreshReportPath: null,
    partitionOutputPath: null,
    manifestPath: null,
    fingerprintPath: null,
  };
}

function makeJob(overrides: Partial<JatoMonthlyUpdateJob> = {}): JatoMonthlyUpdateJob {
  return {
    jobId: "jato-review-1",
    month: "2026-06",
    batchId: "2026-06-r1",
    jobType: "full_batch",
    countryScope: ["捷克", "丹麦"],
    status: "success",
    phase: "completed",
    triggeredBy: "admin",
    createdAt: "2026-07-20T09:00:00+00:00",
    updatedAt: "2026-07-20T09:05:00+00:00",
    startedAt: "2026-07-20T09:00:01+00:00",
    finishedAt: "2026-07-20T09:05:00+00:00",
    error: null,
    upload: null,
    plan: null,
    artifacts: makeArtifacts("full_candidate"),
    summaries: null,
    ...overrides,
  };
}

function makeMaintenanceStatus(): JatoMonthlyUpdateMaintenanceStatus {
  return {
    checkedAt: "2026-07-20T09:05:00+00:00",
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

function makeCountryReport(
  country: string,
  mismatchCellCount: number,
  decision: JatoHistoricalReclassificationDecision | null = null,
  allowedDecisions: JatoHistoricalReclassificationDecision[] = ["use_latest", "keep_active"],
) {
  return {
    country,
    decision,
    comparedThrough: "2026-03",
    historicalMonthCount: 39,
    jointMismatchCellCount: mismatchCellCount,
    jointMovedSales: country === "捷克" ? 8035 : 6207,
    monthlyTotalsStable: true,
    decisionRequired: true,
    allowedDecisions,
    dimensionSummaries: [{
      dimension: "Powertrain",
      mismatchCellCount: 3,
      movedSales: 143,
      oldValues: [{ value: "MHEV", sales: 143, monthCount: 2 }],
      newValues: [{ value: "HEV", sales: 143, monthCount: 2 }],
    }],
    exactChanges: [{
      dimension: "Powertrain",
      make: "KIA",
      model: "Sportage",
      oldValue: "MHEV",
      newValue: "HEV",
      transferredSales: 143,
      affectedMonths: ["2026-01", "2026-02"],
      monthlyTransfers: [
        { month: "2026-01", sales: 70 },
        { month: "2026-02", sales: 73 },
      ],
      confidence: "exact",
    }],
    exactChangeCount: 1,
    complexChangeCount: 2,
    truncation: {
      truncated: false,
      exactChangeLimit: 20,
      valueLimitPerDirection: 8,
    },
  };
}

function makeReviewBundle(
  status: JatoHistoricalReclassificationReport["status"] = "decision_required"
): JatoMonthlyUpdateReviewBundle {
  return {
    jobId: "jato-review-1",
    reviewDir: "04_Processed_data/reviews/jato-review-1",
    compareId: "2026-05_vs_2026-06",
    decisionSuggestion: "manual_review_required",
    compareKeyColumns: ["国家", "Make", "Model"],
    checklistMarkdown: null,
    reviewFindings: [],
    sampledCountries: ["捷克", "丹麦"],
    conflictSampleCount: 0,
    conflictSamples: [],
    overlapChangeSummary: [],
    countryFreshnessSummary: [],
    countryCoverageSummary: [],
    countrySalesReferenceLabel: "Active",
    countryMonthlySalesSummary: [],
    countryMonthlySalesError: null,
    timeAxisCheck: {},
    countryScopeSummary: {},
    refreshSummary: null,
    candidateFingerprint: "candidate-sha",
    approval: null,
    historicalReclassificationReport: {
      status,
      countries: [
        makeCountryReport("捷克", 5217, status === "resolved" ? "use_latest" : null),
        makeCountryReport("丹麦", 1101, status === "resolved" ? "keep_active" : null),
      ],
      resolutionValidation: status === "resolved"
        ? [{
          country: "丹麦",
          decision: "keep_active",
          status: "pass",
          currentStabilityStatus: "pass",
          reason: null,
        }]
        : [],
    },
  };
}

describe("JATO historical reclassification review interaction", () => {
  const job = makeJob();

  beforeEach(() => {
    vi.resetAllMocks();
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({ rows: 1, items: [job] });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: job });
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(),
    });
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: makeReviewBundle(),
    });
    vi.mocked(api.resolveJatoMonthlyUpdateHistoricalReclassification).mockResolvedValue({
      item: makeJob({
        status: "queued",
        phase: "historical_reclassification_resolution",
      }),
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
  });

  it("only offers keep_active for unstable historical sales and submits that allowed choice", async () => {
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: {
        ...makeReviewBundle(),
        historicalReclassificationReport: {
          status: "decision_required",
          countries: [
            makeCountryReport("捷克", 5217),
            {
              ...makeCountryReport("丹麦", 1101, null, ["keep_active"]),
              monthlyTotalsStable: false,
            },
          ],
          resolutionValidation: [],
        },
      },
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    expect(await screen.findByText("use_latest 已锁定；active latest 之后仍取上传。")).toBeTruthy();
    expect(screen.getAllByRole("radio", { name: /采用最新 washed 分类/ })).toHaveLength(1);
    const keepActiveRadios = screen.getAllByRole("radio", { name: /保留当前 active 历史/ });
    expect(keepActiveRadios).toHaveLength(2);
    expect(keepActiveRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);

    fireEvent.click(keepActiveRadios[0]);
    expect((screen.getByRole("button", { name: "还需选择 1 个国家" }) as HTMLButtonElement).disabled)
      .toBe(true);
    fireEvent.click(keepActiveRadios[1]);
    fireEvent.click(screen.getByRole("button", { name: "应用选择并生成完整 Candidate" }));

    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledWith(
      "jato-review-1",
      [
        { country: "捷克", decision: "keep_active" },
        { country: "丹麦", decision: "keep_active" },
      ],
    );
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("requires every country decision before starting the full candidate rebuild", async () => {
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    expect(await screen.findByText("Historical Classification Changes")).toBeTruthy();
    expect(screen.getByText("5,217")).toBeTruthy();
    expect(screen.getAllByText("KIA Sportage").length).toBeGreaterThan(0);

    const lockedButton = screen.getByRole("button", { name: "还需选择 2 个国家" });
    expect((lockedButton as HTMLButtonElement).disabled).toBe(true);
    expect(screen.queryByRole("button", { name: "Approve Review" })).toBeNull();

    const useLatestRadios = screen.getAllByRole("radio", { name: /采用最新 washed 分类/ });
    const keepActiveRadios = screen.getAllByRole("radio", { name: /保留当前 active 历史/ });
    expect(useLatestRadios).toHaveLength(2);
    expect(keepActiveRadios).toHaveLength(2);
    expect(useLatestRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);
    expect(keepActiveRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);

    fireEvent.click(useLatestRadios[0]);
    expect((screen.getByRole("button", { name: "还需选择 1 个国家" }) as HTMLButtonElement).disabled)
      .toBe(true);
    fireEvent.click(keepActiveRadios[1]);

    const resolveButton = screen.getByRole("button", {
      name: "应用选择并生成完整 Candidate",
    });
    expect((resolveButton as HTMLButtonElement).disabled).toBe(false);
    vi.mocked(api.listJatoMonthlyUpdateJobs)
      .mockRejectedValueOnce(new Error("temporary list failure"))
      .mockResolvedValue({
        rows: 1,
        items: [makeJob({
          status: "running",
          phase: "historical_reclassification_resolution",
        })],
      });
    vi.useFakeTimers();
    await act(async () => {
      fireEvent.click(resolveButton);
    });
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledWith(
      "jato-review-1",
      [
        { country: "捷克", decision: "use_latest" },
        { country: "丹麦", decision: "keep_active" },
      ],
    );
    expect(api.approveJatoMonthlyUpdateReview).not.toHaveBeenCalled();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(api.listJatoMonthlyUpdateJobs).toHaveBeenCalledTimes(2);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(api.listJatoMonthlyUpdateJobs).toHaveBeenCalledTimes(3);
  });

  it("unlocks standard approval and publish after a partial job becomes full_smart_merge", async () => {
    const fullSmartMergeJob = makeJob({
      jobType: "partial_country",
      artifacts: {
        ...makeArtifacts("full_smart_merge"),
        rawCompareReportPath: null,
        reviewBundlePath: "04_Processed_data/reviews/jato-review-1/review_bundle.json",
      },
    });
    const approvedJob = makeJob({
      jobType: "partial_country",
      artifacts: {
        ...makeArtifacts("full_smart_merge"),
        rawCompareReportPath: null,
        reviewBundlePath: "04_Processed_data/reviews/jato-review-1/review_bundle.json",
      },
      reviewApproval: {
        decision: "approved",
        reviewedAt: "2026-07-20T09:10:00+00:00",
        reviewedBy: "admin",
        candidateFingerprint: "candidate-sha",
      },
    });
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [fullSmartMergeJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: fullSmartMergeJob });
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: makeReviewBundle("resolved"),
    });
    vi.mocked(api.approveJatoMonthlyUpdateReview).mockResolvedValue({ item: approvedJob });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    expect(await screen.findByRole("button", { name: "Approve Review" })).toBeTruthy();
    expect(screen.getByText("2/2")).toBeTruthy();
    expect(screen.getByText(/已应用选择：采用最新 washed 分类/)).toBeTruthy();
    expect(screen.getByText(/最终 Candidate 复核通过：已保留当前 active 历史/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: /应用选择并生成完整 Candidate/ })).toBeNull();
    const publishBeforeApproval = screen.getByRole("button", {
      name: "先批准 Review 再 Publish",
    });
    expect((publishBeforeApproval as HTMLButtonElement).disabled).toBe(true);
    expect(screen.queryByText(/部分国家候选为隔离 Review 产物/)).toBeNull();

    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [approvedJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: approvedJob });
    fireEvent.click(screen.getByRole("button", { name: "Approve Review" }));

    await waitFor(() => {
      expect(api.approveJatoMonthlyUpdateReview).toHaveBeenCalledWith("jato-review-1");
    });
    const publishAfterApproval = await screen.findByRole("button", {
      name: "Publish Candidate",
    });
    expect((publishAfterApproval as HTMLButtonElement).disabled).toBe(false);
  });

  it("keeps approval locked when resolved keep_active verification is missing", async () => {
    const fullSmartMergeJob = makeJob({
      jobType: "partial_country",
      artifacts: {
        ...makeArtifacts("full_smart_merge"),
        rawCompareReportPath: null,
        reviewBundlePath: "04_Processed_data/reviews/jato-review-1/review_bundle.json",
      },
    });
    const unresolvedVerification = makeReviewBundle("resolved");
    unresolvedVerification.historicalReclassificationReport.resolutionValidation = [];
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [fullSmartMergeJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: fullSmartMergeJob });
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: unresolvedVerification,
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    expect(await screen.findAllByText(/最终 Candidate 历史复核缺失或失败/)).toHaveLength(2);
    expect((screen.getByRole("button", { name: "Approve Review" }) as HTMLButtonElement).disabled)
      .toBe(true);
    expect(screen.getByText(/当前不能批准 Publish/)).toBeTruthy();
  });

  it("keeps a legacy partial job fail-closed when candidateScope is missing", async () => {
    const legacyPartialJob = makeJob({
      jobType: "partial_country",
      artifacts: {
        ...makeArtifacts(""),
        candidateScope: null,
      },
    });
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [legacyPartialJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: legacyPartialJob });
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: makeReviewBundle("resolved"),
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    expect(await screen.findByText(/部分国家候选为隔离 Review 产物/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Approve Review" })).toBeNull();
    expect(screen.queryByRole("button", { name: /Publish/ })).toBeNull();
  });
});
