// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { JatoMonthlyUpdatePage } from "../../pages/JatoMonthlyUpdatePage";
import type {
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
    cancelJatoMonthlyUpdateJob: vi.fn(),
    publishJatoMonthlyUpdateJob: vi.fn(),
    rollbackJatoMonthlyUpdateJob: vi.fn(),
    smartMergeJatoMonthlyUpdateCandidate: vi.fn(),
    recheckJatoMonthlyUpdateJob: vi.fn(),
    approveJatoMonthlyUpdateReview: vi.fn(),
    resolveJatoMonthlyUpdateHistoricalReclassification: vi.fn(),
    promoteCurrentActiveToJatoBaseline: vi.fn(),
    runJatoMonthlyUpdateCleanup: vi.fn(),
    createJatoMonthlyUpdateJob: vi.fn(),
    abandonJatoMonthlyUpdateUpload: vi.fn(),
  },
}));

const candidateFingerprint = "a".repeat(64);
const activeFingerprint = "b".repeat(64);

function makeJob(overrides: Partial<JatoMonthlyUpdateJob> = {}): JatoMonthlyUpdateJob {
  return {
    jobId: "jato-action-confirmation-1",
    month: "2026-06",
    batchId: "2026-06-r1",
    jobType: "full_batch",
    countryScope: ["德国", "丹麦"],
    status: "success",
    phase: "completed",
    triggeredBy: "admin",
    createdAt: "2026-07-23T02:00:00+00:00",
    updatedAt: "2026-07-23T02:05:00+00:00",
    startedAt: "2026-07-23T02:00:01+00:00",
    finishedAt: "2026-07-23T02:05:00+00:00",
    error: null,
    activeBaseFingerprint: activeFingerprint,
    upload: null,
    plan: null,
    artifacts: {
      candidateScope: "full_candidate",
      reviewBundlePath: "04_Processed_data/reviews/jato-action-confirmation-1/review_bundle.json",
      rawCompareReportPath: "04_Processed_data/reviews/jato-action-confirmation-1/raw_compare_report.json",
    },
    summaries: null,
    reviewApproval: {
      decision: "approved",
      reviewedAt: "2026-07-23T02:06:00+00:00",
      reviewedBy: "admin",
      candidateFingerprint,
      activeBaseFingerprint: activeFingerprint,
    },
    ...overrides,
  };
}

function makeMaintenanceStatus(): JatoMonthlyUpdateMaintenanceStatus {
  return {
    checkedAt: "2026-07-23T02:05:00+00:00",
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

function makeReview(): JatoMonthlyUpdateReviewBundle {
  return {
    jobId: "jato-action-confirmation-1",
    reviewDir: "04_Processed_data/reviews/jato-action-confirmation-1",
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

async function renderJob(job: JatoMonthlyUpdateJob): Promise<void> {
  vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
    rows: 1,
    items: [job],
  });
  vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: job });
  await act(async () => {
    render(<JatoMonthlyUpdatePage />);
  });
}

async function openApprovedReview(): Promise<void> {
  fireEvent.click(await screen.findByRole("button", { name: "Review Candidate" }));
  expect(await screen.findByRole("button", { name: "Review Approved" })).toBeTruthy();
}

async function submitTwiceAndReadAlert(
  dialog: HTMLElement,
  confirmLabel: string,
): Promise<HTMLElement> {
  const confirmButton = within(dialog).getByRole("button", {
    name: confirmLabel,
  });
  fireEvent.click(confirmButton);
  fireEvent.click(confirmButton);
  return within(dialog).findByRole("alert");
}

function expectNoNativePrompt(): void {
  expect(window.confirm).not.toHaveBeenCalled();
  expect(window.alert).not.toHaveBeenCalled();
}

describe("JATO remaining action confirmation dialogs", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.spyOn(window, "confirm").mockReturnValue(false);
    vi.spyOn(window, "alert").mockImplementation(() => undefined);
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(),
    });
    vi.mocked(api.getJatoMonthlyUpdateReview).mockResolvedValue({
      item: makeReview(),
    });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("confirms cancel once and keeps a rejected cancellation inside the dialog", async () => {
    const runningJob = makeJob({
      status: "running",
      phase: "candidate_refresh",
      finishedAt: null,
      reviewApproval: null,
    });
    vi.mocked(api.cancelJatoMonthlyUpdateJob).mockRejectedValue(
      new Error(
        "409 {\"blockerType\":\"cancel_conflict\",\"message\":\"任务状态已变化\",\"ruleId\":\"CANCEL001\",\"target\":\"jato-action-confirmation-1\",\"suggestedAction\":\"刷新任务状态\"}"
      ),
    );
    await renderJob(runningJob);

    fireEvent.click(await screen.findByRole("button", { name: "终止任务" }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认终止月更任务",
    });
    expect(api.cancelJatoMonthlyUpdateJob).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("后台任务将被中断");
    expect(dialog.textContent).toContain("active 数据不会因终止动作改变");

    const alert = await submitTwiceAndReadAlert(dialog, "确认终止任务");
    expect(api.cancelJatoMonthlyUpdateJob).toHaveBeenCalledTimes(1);
    expect(api.cancelJatoMonthlyUpdateJob).toHaveBeenCalledWith(runningJob.jobId);
    expect(alert.textContent).toContain("任务状态已变化");
    expect(alert.textContent).toContain("HTTP 状态409");
    expect(screen.getByRole("dialog", { name: "确认终止月更任务" })).toBeTruthy();
    expectNoNativePrompt();
  });

  it("confirms publish once and keeps a rejected active switch inside the dialog", async () => {
    const publishJob = makeJob();
    vi.mocked(api.publishJatoMonthlyUpdateJob).mockRejectedValue(
      new Error(
        "409 {\"blockerType\":\"stale_candidate\",\"message\":\"Candidate 指纹已过期\",\"ruleId\":\"PUB001\",\"target\":\"active\",\"suggestedAction\":\"重新 Review\"}"
      ),
    );
    await renderJob(publishJob);
    await openApprovedReview();

    fireEvent.click(screen.getByRole("button", { name: "Publish Candidate" }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认发布 Candidate",
    });
    expect(api.publishJatoMonthlyUpdateJob).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("确认后会改变 active 数据");
    expect(dialog.textContent).toContain("发布前会自动备份 active");

    const alert = await submitTwiceAndReadAlert(dialog, "确认发布");
    expect(api.publishJatoMonthlyUpdateJob).toHaveBeenCalledTimes(1);
    expect(api.publishJatoMonthlyUpdateJob).toHaveBeenCalledWith(publishJob.jobId);
    expect(alert.textContent).toContain("Candidate 指纹已过期");
    expect(alert.textContent).toContain("重新 Review");
    expect(screen.getByRole("dialog", { name: "确认发布 Candidate" })).toBeTruthy();
    expectNoNativePrompt();
  });

  it("confirms rollback once and keeps a rejected restore inside the dialog", async () => {
    const publishedJob = makeJob({
      publication: {
        publishedAt: "2026-07-23T02:10:00+00:00",
        publishedBy: "admin",
        backupDir: "04_Processed_data/backups/jato-action-confirmation-1",
      },
    });
    vi.mocked(api.rollbackJatoMonthlyUpdateJob).mockRejectedValue(
      new Error(
        "409 {\"blockerType\":\"rollback_target_stale\",\"message\":\"回滚目标已失效\",\"ruleId\":\"ROLLBACK001\",\"target\":\"active\",\"suggestedAction\":\"核对备份指纹\"}"
      ),
    );
    await renderJob(publishedJob);

    fireEvent.click(await screen.findByRole("button", { name: "Rollback Publish" }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认回滚 active",
    });
    expect(api.rollbackJatoMonthlyUpdateJob).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("确认后会改变 active 数据");
    expect(dialog.textContent).toContain("不会使用未验证的临时产物");

    const alert = await submitTwiceAndReadAlert(dialog, "确认回滚");
    expect(api.rollbackJatoMonthlyUpdateJob).toHaveBeenCalledTimes(1);
    expect(api.rollbackJatoMonthlyUpdateJob).toHaveBeenCalledWith(publishedJob.jobId);
    expect(alert.textContent).toContain("回滚目标已失效");
    expect(alert.textContent).toContain("核对备份指纹");
    expect(screen.getByRole("dialog", { name: "确认回滚 active" })).toBeTruthy();
    expectNoNativePrompt();
  });

  it("confirms initial Smart Merge once and keeps a rejected rebuild inside the dialog", async () => {
    const smartMergeJob = makeJob();
    vi.mocked(api.publishJatoMonthlyUpdateJob).mockRejectedValue(
      new Error(
        "409 {\"blockerType\":\"country_regression\",\"message\":\"德国月份回退\",\"regressions\":[{\"country\":\"德国\",\"activeLatestMonth\":\"2026-05\",\"candidateLatestMonth\":\"2026-04\"}],\"suggestedAction\":\"使用 Smart Merge\"}"
      ),
    );
    vi.mocked(api.smartMergeJatoMonthlyUpdateCandidate).mockRejectedValue(
      new Error(
        "409 {\"blockerType\":\"smart_merge_conflict\",\"message\":\"Smart Merge 输入已变化\",\"ruleId\":\"MERGE001\",\"target\":\"德国\",\"suggestedAction\":\"重新打开 Review\"}"
      ),
    );
    await renderJob(smartMergeJob);
    await openApprovedReview();

    fireEvent.click(screen.getByRole("button", { name: "Publish Candidate" }));
    const publishDialog = await screen.findByRole("dialog", {
      name: "确认发布 Candidate",
    });
    fireEvent.click(within(publishDialog).getByRole("button", { name: "确认发布" }));
    await within(publishDialog).findByRole("alert");
    fireEvent.click(within(publishDialog).getByRole("button", { name: "返回检查" }));

    fireEvent.click(await screen.findByRole("button", {
      name: "创建 Smart-Merged Candidate",
    }));
    const dialog = await screen.findByRole("dialog", {
      name: "确认生成 Smart-Merged Candidate",
    });
    expect(api.smartMergeJatoMonthlyUpdateCandidate).not.toHaveBeenCalled();
    expect(dialog.textContent).toContain("德国");
    expect(dialog.textContent).toContain("Candidate 2026-04");
    expect(dialog.textContent).toContain("保留 active 2026-05");
    expect(dialog.textContent).toContain("active 不会在此步骤改变");

    const alert = await submitTwiceAndReadAlert(dialog, "确认 Smart Merge");
    expect(api.smartMergeJatoMonthlyUpdateCandidate).toHaveBeenCalledTimes(1);
    expect(api.smartMergeJatoMonthlyUpdateCandidate).toHaveBeenCalledWith(smartMergeJob.jobId);
    expect(alert.textContent).toContain("Smart Merge 输入已变化");
    expect(alert.textContent).toContain("重新打开 Review");
    expect(screen.getByRole("dialog", {
      name: "确认生成 Smart-Merged Candidate",
    })).toBeTruthy();
    expectNoNativePrompt();
  });
});
