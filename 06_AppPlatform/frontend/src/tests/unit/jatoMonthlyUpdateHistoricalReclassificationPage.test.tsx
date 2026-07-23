// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { JatoMonthlyUpdatePage } from "../../pages/JatoMonthlyUpdatePage";
import type {
  JatoHistoricalReclassificationDecision,
  JatoHistoricalReclassificationReport,
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateMaintenanceStatus,
  JatoMonthlyUpdateReviewBundle,
  JatoMonthlyUpdateReviewFinding,
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

function makeBlockerFinding(
  overrides: Partial<JatoMonthlyUpdateReviewFinding> = {},
): JatoMonthlyUpdateReviewFinding {
  return {
    severity: "blocker",
    scope: "country",
    target: "奥地利",
    ruleId: "SC004",
    message: "目标国家 candidate 存在完全相同的配置指纹。",
    metrics: {
      duplicateRows: 5987,
      keyColumnCount: 12,
    },
    suggestedAction: "修复重复配置后重新上传。",
    sourceFeedback: "奥地利存在 5,987 行重复配置，请恢复配置区分字段后重新导出。",
    ...overrides,
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
  });

  it("allows a per-country mixed override and confirms only the overwrite impact", async () => {
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: {
        ...makeReviewBundle(),
        historicalReclassificationReport: {
          status: "decision_required",
          countries: [
            makeCountryReport("捷克", 5217),
            {
              ...makeCountryReport("丹麦", 1101),
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

    expect(await screen.findByText(/本次上传与 active 的历史逐月总销量不一致/)).toBeTruthy();
    const useLatestRadios = await screen.findAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    });
    expect(useLatestRadios).toHaveLength(2);
    const keepActiveRadios = screen.getAllByRole("radio", { name: /保留当前 active 历史/ });
    expect(keepActiveRadios).toHaveLength(2);
    expect(useLatestRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);
    expect(keepActiveRadios.every((radio) => (radio as HTMLInputElement).checked)).toBe(true);
    const unstableRiskId = useLatestRadios[1].getAttribute("aria-describedby");
    expect(unstableRiskId).toBeTruthy();
    expect(document.getElementById(unstableRiskId as string)?.textContent).toContain(
      "历史 Dashboard、同比和份额会变化",
    );
    expect(useLatestRadios[0].getAttribute("aria-describedby")).toBeNull();

    fireEvent.click(useLatestRadios[1]);
    fireEvent.click(screen.getByRole("button", { name: "应用选择并生成完整 Candidate" }));

    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("保留 active 历史1 个国家");
    expect(dialog.textContent).toContain("使用上传覆盖历史1 个国家");
    expect(dialog.textContent).toContain("捷克");
    expect(dialog.textContent).toContain("保留当前 active 历史");
    expect(dialog.textContent).toContain("丹麦");
    expect(dialog.textContent).toContain("覆盖至 2026-03 的 39 个历史月份");
    expect(dialog.textContent).toContain("历史 Dashboard、同比与份额会变化");
    expect(within(dialog).getByText("整国替换历史，不与 active 累加")).toBeTruthy();
    expect(dialog.textContent).toContain("未上传国家");

    fireEvent.click(within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }));

    await waitFor(() => {
      expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledWith(
        "jato-review-1",
        [
          { country: "捷克", decision: "keep_active" },
          { country: "丹麦", decision: "use_latest" },
        ],
      );
    });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("defaults every country to keep_active and confirms the safe summary before submitting", async () => {
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    expect(await screen.findByText("Historical Data Changes")).toBeTruthy();
    expect(screen.getByText("5,217")).toBeTruthy();
    expect(screen.getAllByText("KIA Sportage").length).toBeGreaterThan(0);

    expect(screen.queryByRole("button", { name: "Approve Review" })).toBeNull();

    const useLatestRadios = screen.getAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    });
    const keepActiveRadios = screen.getAllByRole("radio", { name: /保留当前 active 历史/ });
    expect(useLatestRadios).toHaveLength(2);
    expect(keepActiveRadios).toHaveLength(2);
    expect(useLatestRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);
    expect(keepActiveRadios.every((radio) => (radio as HTMLInputElement).checked)).toBe(true);
    expect(screen.getByText("2/2")).toBeTruthy();

    expect(screen.getByRole("group", {
      name: "批量设置所有国家的历史数据处理方式",
    })).toBeTruthy();
    expect(screen.getByRole("button", {
      name: "全部保留 active 历史",
    }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.getByRole("button", {
      name: "高风险：全部使用上传覆盖历史",
    }).getAttribute("aria-pressed")).toBe("false");

    const resolveButton = screen.getByRole("button", {
      name: "按默认保留 active 并生成完整 Candidate",
    });
    expect((resolveButton as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(resolveButton);

    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("保留 active 历史2 个国家");
    expect(dialog.textContent).toContain("使用上传覆盖历史0 个国家");
    expect(within(dialog).getByText("本批次全部保留 active 历史")).toBeTruthy();
    expect(dialog.textContent).toContain("现有历史不会被上传文件覆盖");
    expect(within(dialog).queryByText("整国替换历史，不与 active 累加")).toBeNull();

    fireEvent.click(within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }));

    await waitFor(() => {
      expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledWith(
        "jato-review-1",
        [
          { country: "捷克", decision: "keep_active" },
          { country: "丹麦", decision: "keep_active" },
        ],
      );
    });
    expect(api.approveJatoMonthlyUpdateReview).not.toHaveBeenCalled();
  });

  it("blocks candidate generation and surfaces non-resolvable Review feedback beside the action", async () => {
    const review = makeReviewBundle();
    review.reviewFindings = [makeBlockerFinding()];
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({ item: review });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    const lockedButton = await screen.findByRole("button", {
      name: "先修复 Review blocker 再生成 Candidate",
    }) as HTMLButtonElement;
    expect(lockedButton.disabled).toBe(true);
    expect(lockedButton.getAttribute("aria-describedby"))
      .toBe("historical-reclassification-blockers");
    const blockerAlert = screen.getByRole("alert");
    expect(within(blockerAlert).getByText(/当前 Review 有规则 blocker，历史选择不能消解/))
      .toBeTruthy();
    expect(within(blockerAlert).getByText("规则 SC004 · 目标 奥地利")).toBeTruthy();
    expect(within(blockerAlert).getByText(/奥地利存在 5,987 行重复配置/)).toBeTruthy();

    fireEvent.click(lockedButton);
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
  });

  it("keeps SC011 historical_sales_changed eligible for keep_active or use_latest resolution", async () => {
    const review = makeReviewBundle();
    review.historicalReclassificationReport.countries[0] = {
      ...review.historicalReclassificationReport.countries[0],
      monthlyTotalsStable: false,
    };
    review.reviewFindings = [makeBlockerFinding({
      target: "捷克",
      ruleId: "SC011",
      message: "目标国家 candidate 改写了 active 已有历史销量。",
      metrics: {
        reason: "historical_sales_changed",
        countryMismatchCount: 27,
      },
      sourceFeedback: "请确认使用最新 washed 历史，或保留 active 历史。",
    })];
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({ item: review });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    expect(screen.queryByText(/当前 Review 有规则 blocker，历史选择不能消解/)).toBeNull();
    const useLatestRadios = await screen.findAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    });
    fireEvent.click(useLatestRadios[0]);
    const resolveButton = screen.getByRole("button", {
      name: "应用选择并生成完整 Candidate",
    }) as HTMLButtonElement;
    expect(resolveButton.disabled).toBe(false);
    fireEvent.click(resolveButton);

    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    expect(dialog.textContent).toContain("捷克");
    expect(dialog.textContent).toContain("使用本次上传覆盖历史");
    expect(within(dialog).getByText("整国替换历史，不与 active 累加")).toBeTruthy();
    fireEvent.click(within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }));

    await waitFor(() => {
      expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledWith(
        "jato-review-1",
        [
          { country: "捷克", decision: "use_latest" },
          { country: "丹麦", decision: "keep_active" },
        ],
      );
    });
  });

  it("honors top-level blockerType precedence and locks non-sales SC011 blockers", async () => {
    const review = makeReviewBundle();
    review.historicalReclassificationReport.countries[0] = {
      ...review.historicalReclassificationReport.countries[0],
      monthlyTotalsStable: false,
    };
    review.reviewFindings = [makeBlockerFinding({
      target: "捷克",
      ruleId: "SC011",
      blockerType: "historical_configuration_changed",
      message: "目标国家历史配置变化无法由销量历史选择消解。",
      metrics: {
        blockerType: "historical_sales_changed",
        reason: "historical_sales_changed",
        countryMismatchCount: 27,
      },
      sourceFeedback: "请先核对捷克历史配置变化后重新导出。",
    })];
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({ item: review });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    const lockedButton = await screen.findByRole("button", {
      name: "先修复 Review blocker 再生成 Candidate",
    }) as HTMLButtonElement;
    expect(lockedButton.disabled).toBe(true);
    const blockerAlert = screen.getByRole("alert");
    expect(within(blockerAlert).getByText("规则 SC011 · 目标 捷克")).toBeTruthy();
    expect(within(blockerAlert).getByText(/请先核对捷克历史配置变化后重新导出/))
      .toBeTruthy();
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
  });

  it("toggles every country between bulk use_latest and bulk keep_active", async () => {
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    const keepAllButton = await screen.findByRole("button", {
      name: "全部保留 active 历史",
    });
    const useLatestAllButton = screen.getByRole("button", {
      name: "高风险：全部使用上传覆盖历史",
    });
    const useLatestRadios = screen.getAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    });
    const keepActiveRadios = screen.getAllByRole("radio", {
      name: /保留当前 active 历史/,
    });

    expect(keepAllButton.getAttribute("aria-pressed")).toBe("true");
    expect(useLatestAllButton.getAttribute("aria-pressed")).toBe("false");

    fireEvent.click(useLatestAllButton);
    expect(keepAllButton.getAttribute("aria-pressed")).toBe("false");
    expect(useLatestAllButton.getAttribute("aria-pressed")).toBe("true");
    expect(useLatestRadios.every((radio) => (radio as HTMLInputElement).checked)).toBe(true);
    expect(keepActiveRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);

    fireEvent.click(keepAllButton);
    expect(keepAllButton.getAttribute("aria-pressed")).toBe("true");
    expect(useLatestAllButton.getAttribute("aria-pressed")).toBe("false");
    expect(useLatestRadios.every((radio) => !(radio as HTMLInputElement).checked)).toBe(true);
    expect(keepActiveRadios.every((radio) => (radio as HTMLInputElement).checked)).toBe(true);

    fireEvent.click(useLatestAllButton);
    fireEvent.click(screen.getByRole("button", {
      name: "应用选择并生成完整 Candidate",
    }));

    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("保留 active 历史0 个国家");
    expect(dialog.textContent).toContain("使用上传覆盖历史2 个国家");
    expect(dialog.textContent).toContain("捷克");
    expect(dialog.textContent).toContain("丹麦");
    expect(dialog.textContent?.match(/覆盖至 2026-03 的 39 个历史月份/g)).toHaveLength(2);
    fireEvent.click(within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }));

    await waitFor(() => {
      expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledWith(
        "jato-review-1",
        [
          { country: "捷克", decision: "use_latest" },
          { country: "丹麦", decision: "use_latest" },
        ],
      );
    });
  });

  it("locks bulk and individual choices while the candidate rebuild request is pending", async () => {
    let completeResolution!: (value: { item: JatoMonthlyUpdateJob }) => void;
    vi.mocked(api.resolveJatoMonthlyUpdateHistoricalReclassification).mockReturnValue(
      new Promise((resolve) => {
        completeResolution = resolve;
      }),
    );

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    fireEvent.click(await screen.findByRole("button", {
      name: "按默认保留 active 并生成完整 Candidate",
    }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    const confirmButton = within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    });
    fireEvent.click(confirmButton);
    fireEvent.click(confirmButton);

    await waitFor(() => {
      expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledTimes(1);
      expect((screen.getByRole("button", {
        name: "全部保留 active 历史",
      }) as HTMLButtonElement).disabled).toBe(true);
    });
    expect((screen.getByRole("button", {
      name: "高风险：全部使用上传覆盖历史",
    }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getAllByRole("radio").every(
      (radio) => radio.matches(":disabled")
    )).toBe(true);
    expect((within(dialog).getByRole("button", {
      name: "正在生成 Candidate...",
    }) as HTMLButtonElement).disabled).toBe(true);

    await act(async () => {
      completeResolution({
        item: makeJob({
          status: "queued",
          phase: "historical_reclassification_resolution",
        }),
      });
    });
  });

  it("restores the safe default after closing and reopening Review", async () => {
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    fireEvent.click(await screen.findByRole("button", {
      name: "高风险：全部使用上传覆盖历史",
    }));
    expect(screen.getAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    }).every((radio) => (radio as HTMLInputElement).checked)).toBe(true);

    fireEvent.click(screen.getByRole("button", { name: "收起 Review" }));
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    await waitFor(() => {
      expect(screen.getAllByRole("radio", {
        name: /保留当前 active 历史/,
      }).every((radio) => (radio as HTMLInputElement).checked)).toBe(true);
    });
    expect(screen.getByRole("button", {
      name: "全部保留 active 历史",
    }).getAttribute("aria-pressed")).toBe("true");
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
    expect(screen.getByText(/已应用选择：当前完整 Candidate 使用本次 washed 文件/)).toBeTruthy();
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

    const approvalDialog = await screen.findByRole("dialog", {
      name: "确认批准 Review",
    });
    expect(api.approveJatoMonthlyUpdateReview).not.toHaveBeenCalled();
    expect(approvalDialog.textContent).toContain("批准不等于发布");
    fireEvent.click(within(approvalDialog).getByRole("button", {
      name: "确认批准",
    }));

    await waitFor(() => {
      expect(api.approveJatoMonthlyUpdateReview).toHaveBeenCalledWith("jato-review-1");
    });
    const publishAfterApproval = await screen.findByRole("button", {
      name: "Publish Candidate",
    });
    expect((publishAfterApproval as HTMLButtonElement).disabled).toBe(false);
  });

  it("keeps unstable use_latest visibly high-risk after the candidate rebuild", async () => {
    const fullSmartMergeJob = makeJob({
      artifacts: {
        ...makeArtifacts("full_smart_merge"),
        reviewBundlePath: "04_Processed_data/reviews/jato-review-1/review_bundle.json",
      },
    });
    const resolvedReview = makeReviewBundle("resolved");
    resolvedReview.historicalReclassificationReport.countries[0] = {
      ...resolvedReview.historicalReclassificationReport.countries[0],
      monthlyTotalsStable: false,
      decision: "use_latest",
    };
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [fullSmartMergeJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({
      item: fullSmartMergeJob,
    });
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: resolvedReview,
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));

    const warning = await screen.findByText(/高风险选择已应用/);
    expect(warning.className).toContain("alert-danger");
    expect(warning.textContent).toContain("历史 Dashboard、同比和份额会变化");
    expect(screen.getByRole("button", { name: "Approve Review" })).toBeTruthy();
    expect(screen.queryByRole("group", {
      name: "批量设置所有国家的历史数据处理方式",
    })).toBeNull();
    expect(screen.queryByRole("button", { name: "全部保留 active 历史" })).toBeNull();
    expect(screen.queryByRole("button", {
      name: "高风险：全部使用上传覆盖历史",
    })).toBeNull();
  });

  it("returns to the country choices without rebuilding when confirmation is cancelled", async () => {
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    const useLatestRadios = await screen.findAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    });
    fireEvent.click(useLatestRadios[0]);
    fireEvent.click(useLatestRadios[1]);
    fireEvent.click(screen.getByRole("button", {
      name: "应用选择并生成完整 Candidate",
    }));

    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    fireEvent.click(within(dialog).getByRole("button", { name: "返回修改" }));

    await waitFor(() => {
      expect(screen.queryByRole("dialog", {
        name: "确认生成完整 Candidate",
      })).toBeNull();
    });
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
    expect(screen.getAllByRole("radio", {
      name: /使用本次上传覆盖历史/,
    }).every((radio) => (radio as HTMLInputElement).checked)).toBe(true);
  });

  it("keeps structured API failure feedback inside the confirmation dialog", async () => {
    vi.mocked(api.resolveJatoMonthlyUpdateHistoricalReclassification).mockRejectedValue(
      new Error(
        "409 {\"blockerType\":\"historical_configuration_changed\",\"message\":\"历史分类变化\",\"ruleId\":\"SC011\",\"target\":\"丹麦\",\"suggestedAction\":\"联系洗数人员\",\"sourceFeedback\":\"丹麦字段需修正\"}"
      ),
    );
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    fireEvent.click(await screen.findByRole("button", {
      name: "按默认保留 active 并生成完整 Candidate",
    }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    const confirmButton = within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }) as HTMLButtonElement;
    fireEvent.click(confirmButton);

    const alert = await within(dialog).findByRole("alert");
    expect(within(alert).getByText("数据门禁阻止了本次操作")).toBeTruthy();
    expect(within(alert).getByText("历史分类变化")).toBeTruthy();
    expect(within(alert).getByText("HTTP 状态")).toBeTruthy();
    expect(within(alert).getByText("409")).toBeTruthy();
    expect(within(alert).getByText("门禁类型")).toBeTruthy();
    expect(within(alert).getByText("historical_configuration_changed")).toBeTruthy();
    expect(within(alert).getByText("规则")).toBeTruthy();
    expect(within(alert).getByText("SC011")).toBeTruthy();
    expect(within(alert).getByText("对象")).toBeTruthy();
    expect(within(alert).getByText("丹麦")).toBeTruthy();
    expect(within(alert).getByText("建议处理")).toBeTruthy();
    expect(within(alert).getByText("联系洗数人员")).toBeTruthy();
    expect(within(alert).getByText("给洗数人员")).toBeTruthy();
    expect(within(alert).getByText("丹麦字段需修正")).toBeTruthy();
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("dialog", {
      name: "确认生成完整 Candidate",
    })).toBeTruthy();
    await waitFor(() => {
      expect(confirmButton.disabled).toBe(false);
    });
  });

  it("replaces a gateway HTML failure page with safe retry guidance", async () => {
    vi.mocked(api.resolveJatoMonthlyUpdateHistoricalReclassification).mockRejectedValue(
      new Error(
        "502 <html><head><title>502 Bad Gateway</title></head><body><h1>502 Bad Gateway</h1></body></html>"
      ),
    );
    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
    fireEvent.click(await screen.findByRole("button", {
      name: "按默认保留 active 并生成完整 Candidate",
    }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认生成完整 Candidate",
    });
    fireEvent.click(within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }));

    const alert = await within(dialog).findByRole("alert");
    expect(alert.textContent).toContain("服务暂时未完成操作");
    expect(alert.textContent).toContain("HTTP 状态");
    expect(alert.textContent).toContain("502");
    expect(alert.textContent).toContain("网关未返回结构化失败原因");
    expect(alert.textContent).toContain("不要立即重复提交");
    expect(alert.textContent).toContain("已锁定再次提交");
    expect(alert.textContent).not.toContain("<html>");
    expect((within(dialog).getByRole("button", {
      name: "确认并生成 Candidate",
    }) as HTMLButtonElement).disabled).toBe(true);
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).toHaveBeenCalledTimes(1);
  });

  it("fails closed when no common keep_active default strategy is available", async () => {
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: {
        ...makeReviewBundle(),
        historicalReclassificationReport: {
          status: "decision_required",
          countries: [
            {
              ...makeCountryReport("捷克", 5217, null, ["use_latest"]),
              defaultDecision: null,
            },
            {
              ...makeCountryReport("丹麦", 1101, null, ["keep_active"]),
              defaultDecision: "keep_active",
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

    expect(await screen.findByText(/以下国家没有可用的 keep_active 默认策略/)).toBeTruthy();
    expect(screen.getByText(/捷克。请先修复后端决策契约/)).toBeTruthy();
    const keepAllButton = screen.getByRole("button", {
      name: "全部保留 active 历史",
    }) as HTMLButtonElement;
    const useLatestAllButton = screen.getByRole("button", {
      name: "高风险：全部使用上传覆盖历史",
    }) as HTMLButtonElement;
    expect(keepAllButton.disabled).toBe(true);
    expect(useLatestAllButton.disabled).toBe(true);
    expect(keepAllButton.getAttribute("aria-pressed")).toBe("false");
    expect(useLatestAllButton.getAttribute("aria-pressed")).toBe("false");

    const useLatestRadio = screen.getByRole("radio", {
      name: /使用本次上传覆盖历史/,
    }) as HTMLInputElement;
    const keepActiveRadio = screen.getByRole("radio", {
      name: /保留当前 active 历史/,
    }) as HTMLInputElement;
    expect(useLatestRadio.checked).toBe(false);
    expect(keepActiveRadio.checked).toBe(true);
    const lockedButton = screen.getByRole("button", { name: "还需选择 1 个国家" });
    expect((lockedButton as HTMLButtonElement).disabled).toBe(true);
    expect(api.resolveJatoMonthlyUpdateHistoricalReclassification).not.toHaveBeenCalled();
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
