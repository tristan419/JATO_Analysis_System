// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

describe("JATO monthly update maintenance API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("maps the asynchronous baseline promotion embedded in maintenance status", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      item: {
        checkedAt: "2026-07-20T09:00:00+00:00",
        activeBaselinePath: "01_RAW_DATA/baseline/JATO-2026.6.xlsx",
        activeBaselineSource: "active",
        latestPatchBatch: "2026-06-r1",
        jobCount: 18,
        uploadSessionCount: 1,
        trackedStorageBytes: 4096,
        storageMetrics: [],
        baselinePromotion: {
          operationId: "jato-baseline-123",
          status: "running",
          requestedAt: "2026-07-20T08:58:00+00:00",
          requestedBy: "admin",
          startedAt: "2026-07-20T08:59:00+00:00",
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
        },
      },
    })));

    const response = await api.getJatoMonthlyUpdateMaintenanceStatus();

    expect(response.item.baselinePromotion).toMatchObject({
      operationId: "jato-baseline-123",
      status: "running",
      requestedBy: "admin",
      sourceActiveFingerprint: "active-sha",
      promotedAt: null,
      baselinePath: null,
    });
  });

  it("maps structured failure feedback from a failed promotion", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      item: {
        operationId: "jato-baseline-456",
        status: "failed",
        requestedAt: "2026-07-20T09:00:00+00:00",
        requestedBy: "admin",
        startedAt: "2026-07-20T09:00:01+00:00",
        finishedAt: "2026-07-20T09:00:02+00:00",
        error: "export failed",
        failureDigest: {
          code: "BASELINE_EXPORT_FAILED",
          category: "resource",
          phase: "baseline_promotion",
          retryable: true,
          message: "导出 baseline 失败",
          sourceFeedback: "请确认 active parquet 完整后重试。",
          technicalDetail: { exitCode: -9 },
          nextAction: "retry",
        },
        sourceActiveFingerprint: "active-sha",
      },
    })));

    const response = await api.promoteCurrentActiveToJatoBaseline();

    expect(response.item.status).toBe("failed");
    expect(response.item.failureDigest).toMatchObject({
      code: "BASELINE_EXPORT_FAILED",
      retryable: true,
      sourceFeedback: "请确认 active parquet 完整后重试。",
    });
    expect(response.item.promotedAt).toBeNull();
  });
});
