// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { JatoMonthlyUpdatePage } from "../../pages/JatoMonthlyUpdatePage";
import type {
  JatoMonthlyUpdateBaselinePromotionResult,
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdateMaintenanceStatus,
} from "../../types";

vi.mock("../../api/client", () => ({
  api: {
    listJatoMonthlyUpdateJobs: vi.fn(),
    getJatoMonthlyUpdateJob: vi.fn(),
    getJatoMonthlyUpdateMaintenanceStatus: vi.fn(),
    promoteCurrentActiveToJatoBaseline: vi.fn(),
    runJatoMonthlyUpdateCleanup: vi.fn(),
    createJatoMonthlyUpdateJob: vi.fn(),
    abandonJatoMonthlyUpdateUpload: vi.fn(),
  },
}));

function makePromotion(
  overrides: Partial<JatoMonthlyUpdateBaselinePromotionResult> = {}
): JatoMonthlyUpdateBaselinePromotionResult {
  return {
    operationId: "jato-baseline-123",
    status: "queued",
    requestedAt: "2026-07-20T09:00:00+00:00",
    requestedBy: "admin",
    startedAt: null,
    finishedAt: null,
    error: null,
    failureDigest: null,
    sourceActiveFingerprint: "active-sha",
    promotedAt: null,
    triggeredBy: null,
    sourceParquetPath: null,
    baselinePath: null,
    detectedLatestMonth: null,
    countryCount: 0,
    rowCount: 0,
    archivedBaselineCount: 0,
    archivedBaselines: [],
    ...overrides,
  };
}

function makeMaintenanceStatus(
  promotion: JatoMonthlyUpdateBaselinePromotionResult | null
): JatoMonthlyUpdateMaintenanceStatus {
  return {
    checkedAt: "2026-07-20T09:00:00+00:00",
    activeBaselinePath: "01_RAW_DATA/baseline/JATO-2026.5.xlsx",
    activeBaselineSource: "active",
    latestPatchBatch: "2026-06-r1",
    jobCount: 18,
    uploadSessionCount: 0,
    baselinePromotion: promotion,
    trackedStorageBytes: 4096,
    storageMetrics: [],
  };
}

describe("JATO monthly update baseline promotion status", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.resetAllMocks();
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 0,
      items: [],
    });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it("keeps the action disabled while queued and polls until success", async () => {
    const queued = makeMaintenanceStatus(makePromotion());
    const success = makeMaintenanceStatus(makePromotion({
      status: "success",
      startedAt: "2026-07-20T09:00:01+00:00",
      finishedAt: "2026-07-20T09:00:03+00:00",
      promotedAt: "2026-07-20T09:00:03+00:00",
      triggeredBy: "admin",
      sourceParquetPath: "04_Processed_data/jato_full_archive.parquet",
      baselinePath: "01_RAW_DATA/baseline/JATO-2026.6.xlsx",
      detectedLatestMonth: "2026-06",
      countryCount: 21,
      rowCount: 123456,
    }));
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus)
      .mockResolvedValueOnce({ item: queued })
      .mockResolvedValueOnce({ item: success });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    const queuedButton = screen.getByRole("button", { name: "保存排队中..." });
    expect((queuedButton as HTMLButtonElement).disabled).toBe(true);
    expect((document.querySelector(".monthly-update-file-input") as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByPlaceholderText("2026-04") as HTMLInputElement).disabled).toBe(true);
    expect(screen.getByRole("button", { name: /拖拽 JATO Excel 到这里/ }).getAttribute("aria-disabled")).toBe("true");
    expect(screen.getByText("status: 排队中")).toBeTruthy();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });

    expect(api.getJatoMonthlyUpdateMaintenanceStatus).toHaveBeenCalledTimes(2);
    expect(screen.getByText("status: 已完成")).toBeTruthy();
    expect(screen.getByText("baseline: 01_RAW_DATA/baseline/JATO-2026.6.xlsx")).toBeTruthy();
    expect((
      screen.getByRole("button", { name: "保存当前 active 为 baseline" }) as HTMLButtonElement
    ).disabled).toBe(false);
  });

  it("shows structured source feedback for a failed worker operation", async () => {
    const failed = makeMaintenanceStatus(makePromotion({
      status: "failed",
      startedAt: "2026-07-20T09:00:01+00:00",
      finishedAt: "2026-07-20T09:00:03+00:00",
      error: "export failed",
      failureDigest: {
        code: "BASELINE_EXPORT_FAILED",
        category: "resource",
        phase: "baseline_promotion",
        retryable: true,
        message: "导出 baseline 失败",
        sourceFeedback: "请确认 active parquet 完整后重试。",
        technicalDetail: null,
        nextAction: "retry",
      },
    }));
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: failed,
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    expect(screen.getByText("status: 失败")).toBeTruthy();
    expect(screen.getByText("error: export failed")).toBeTruthy();
    expect(screen.getByText("source feedback: 请确认 active parquet 完整后重试。")).toBeTruthy();
  });

  it("locks upload and maintenance controls while a monthly update job is active", async () => {
    const activeJob = {
      jobId: "jato-update-active",
      status: "running",
      phase: "candidate_refresh",
    } as JatoMonthlyUpdateJob;
    vi.mocked(api.listJatoMonthlyUpdateJobs).mockResolvedValue({
      rows: 1,
      items: [activeJob],
    });
    vi.mocked(api.getJatoMonthlyUpdateJob).mockResolvedValue({ item: activeJob });
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(null),
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });

    expect((document.querySelector(".monthly-update-file-input") as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByPlaceholderText("2026-04") as HTMLInputElement).disabled).toBe(true);
    expect(screen.getByRole("button", { name: /拖拽 JATO Excel 到这里/ }).getAttribute("aria-disabled")).toBe("true");
    expect((screen.getByRole("button", { name: "保存当前 active 为 baseline" }) as HTMLButtonElement).disabled).toBe(true);
    expect((screen.getByRole("button", { name: /执行安全删/ }) as HTMLButtonElement).disabled).toBe(true);
  });

  it("lets the user abandon an unconsumed digest without touching active", async () => {
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(null),
    });
    vi.mocked(api.createJatoMonthlyUpdateJob).mockImplementation((_file, onProgress) => {
      onProgress?.({
        uploadId: "upload-123",
        stage: "digesting",
        uploadedBytes: 4,
        totalBytes: 4,
        uploadedChunks: 1,
        totalChunks: 1,
        chunkSize: 4,
        detail: "正在识别国家",
      });
      return new Promise<never>(() => {});
    });
    vi.mocked(api.abandonJatoMonthlyUpdateUpload).mockResolvedValue({
      uploadId: "upload-123",
      status: "abandoned",
    } as Awaited<ReturnType<typeof api.abandonJatoMonthlyUpdateUpload>>);

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    const fileInput = document.querySelector(".monthly-update-file-input") as HTMLInputElement;
    fireEvent.change(fileInput, {
      target: { files: [new File([new Uint8Array([1, 2, 3, 4])], "patch.xlsx")] },
    });
    fireEvent.click(screen.getByRole("button", { name: "启动月更任务" }));

    await act(async () => {});
    fireEvent.click(screen.getByRole("button", { name: "放弃本次上传" }));

    await act(async () => {});
    expect(api.abandonJatoMonthlyUpdateUpload).toHaveBeenCalledWith("upload-123");
    expect(screen.getByText("已放弃本次上传并清除本地续传信息；candidate 与 active 均未修改。")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "放弃本次上传" })).toBeNull();
  });

  it("hides abandon once task creation may have consumed the upload", async () => {
    vi.mocked(api.getJatoMonthlyUpdateMaintenanceStatus).mockResolvedValue({
      item: makeMaintenanceStatus(null),
    });
    vi.mocked(api.createJatoMonthlyUpdateJob).mockImplementation((_file, onProgress) => {
      onProgress?.({
        uploadId: "upload-creating",
        stage: "creating_job",
        uploadedBytes: 4,
        totalBytes: 4,
        uploadedChunks: 1,
        totalChunks: 1,
        chunkSize: 4,
        detail: "准备创建月更任务",
      });
      return new Promise<never>(() => {});
    });

    await act(async () => {
      render(<JatoMonthlyUpdatePage />);
    });
    const fileInput = document.querySelector(".monthly-update-file-input") as HTMLInputElement;
    fireEvent.change(fileInput, {
      target: { files: [new File([new Uint8Array([1, 2, 3, 4])], "patch.xlsx")] },
    });
    fireEvent.click(screen.getByRole("button", { name: "启动月更任务" }));

    await act(async () => {});
    expect(screen.queryByRole("button", { name: "放弃本次上传" })).toBeNull();
  });
});
