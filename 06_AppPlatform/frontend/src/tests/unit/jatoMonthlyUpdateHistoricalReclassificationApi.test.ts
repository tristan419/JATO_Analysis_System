// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

describe("JATO monthly update historical reclassification API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("maps the country, dimension and exact old-to-new review report", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      item: {
        jobId: "jato-review-1",
        reviewFindings: [{
          severity: "blocker",
          scope: "country",
          target: "捷克",
          ruleId: "SC011",
          blockerType: "historical_configuration_changed",
          message: "历史配置发生变化。",
          metrics: {
            blockerType: "historical_sales_changed",
          },
          suggestedAction: "核对配置变化。",
          sourceFeedback: "请核对捷克历史配置变化。",
        }],
        historicalReclassificationReport: {
          status: "resolved",
          countries: [{
            country: "捷克",
            decision: "keep_active",
            comparedThrough: "2026-03",
            historicalMonthCount: 39,
            jointMismatchCellCount: 5217,
            jointMovedSales: 8035,
            monthlyTotalsStable: true,
            decisionRequired: true,
            allowedDecisions: ["use_latest", "keep_active"],
            dimensionSummaries: [{
              dimension: "Powertrain",
              mismatchCellCount: 73,
              movedSales: 332,
              oldValues: [{ value: "MHEV", sales: 214, monthCount: 12 }],
              newValues: [{ value: "HEV", sales: 172, monthCount: 12 }],
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
          }],
          resolutionValidation: [{
            country: "捷克",
            decision: "keep_active",
            status: "pass",
            currentStabilityStatus: "pass",
            reason: null,
          }],
        },
      },
    })));

    const response = await api.getJatoMonthlyUpdateReview("jato-review-1");

    expect(response.item.reviewFindings[0]).toMatchObject({
      ruleId: "SC011",
      blockerType: "historical_configuration_changed",
      metrics: {
        blockerType: "historical_sales_changed",
      },
    });
    expect(response.item.historicalReclassificationReport).toMatchObject({
      status: "resolved",
      countries: [{
        country: "捷克",
        decision: "keep_active",
        comparedThrough: "2026-03",
        jointMismatchCellCount: 5217,
        monthlyTotalsStable: true,
        allowedDecisions: ["use_latest", "keep_active"],
        dimensionSummaries: [{
          dimension: "Powertrain",
          movedSales: 332,
          oldValues: [{ value: "MHEV", sales: 214, monthCount: 12 }],
        }],
        exactChanges: [{
          make: "KIA",
          model: "Sportage",
          oldValue: "MHEV",
          newValue: "HEV",
          transferredSales: 143,
          monthlyTransfers: [
            { month: "2026-01", sales: 70 },
            { month: "2026-02", sales: 73 },
          ],
        }],
      }],
      resolutionValidation: [{
        country: "捷克",
        decision: "keep_active",
        status: "pass",
        currentStabilityStatus: "pass",
        reason: null,
      }],
    });
  });

  it("fails closed when allowed decisions are missing or contain an invalid value", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      item: {
        jobId: "jato-review-1",
        historicalReclassificationReport: {
          status: "decision_required",
          countries: [
            {
              country: "捷克",
              decisionRequired: true,
              defaultDecision: "keep_active",
              allowedDecisions: ["keep_active"],
            },
            { country: "丹麦", decisionRequired: true },
            {
              country: "瑞典",
              decisionRequired: true,
              defaultDecision: "use_latest",
              allowedDecisions: ["use_latest", "invalid"],
            },
            {
              country: "挪威",
              decision: "use_latest",
              decisionRequired: true,
              defaultDecision: "keep_active",
              allowedDecisions: ["keep_active"],
            },
          ],
        },
      },
    })));

    const response = await api.getJatoMonthlyUpdateReview("jato-review-1");

    expect(response.item.historicalReclassificationReport.countries.map((country) => (
      country.allowedDecisions
    ))).toEqual([["keep_active"], [], [], ["keep_active"]]);
    expect(response.item.historicalReclassificationReport.countries.map((country) => (
      country.defaultDecision
    ))).toEqual(["keep_active", null, null, "keep_active"]);
    expect(response.item.historicalReclassificationReport.countries[3]?.decision).toBeNull();
    expect(response.item.historicalReclassificationReport.resolutionValidation).toEqual([]);
  });

  it("rejects a missing or invalid outer historical report contract", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(Response.json({ item: { jobId: "jato-review-1" } }))
      .mockResolvedValueOnce(Response.json({
        item: {
          jobId: "jato-review-1",
          historicalReclassificationReport: {
            status: "unknown",
            countries: [],
          },
        },
      }))
      .mockResolvedValueOnce(Response.json({
        item: {
          jobId: "jato-review-1",
          historicalReclassificationReport: {
            status: "not_required",
            countries: [{
              country: "荷兰",
              decisionRequired: true,
              allowedDecisions: ["keep_active"],
            }],
          },
        },
      }))
      .mockResolvedValueOnce(Response.json({
        item: {
          jobId: "jato-review-1",
          historicalReclassificationReport: {
            status: "resolved",
            countries: [{
              country: "荷兰",
              decision: "keep_active",
              decisionRequired: true,
              allowedDecisions: ["keep_active"],
            }],
            resolutionValidation: [{
              country: "荷兰",
              decision: "keep_active",
              status: "unknown",
            }],
          },
        },
      }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.getJatoMonthlyUpdateReview("jato-review-1")).rejects.toThrow(
      /缺少历史重分类报告/
    );
    await expect(api.getJatoMonthlyUpdateReview("jato-review-1")).rejects.toThrow(
      /历史重分类状态无效/
    );
    await expect(api.getJatoMonthlyUpdateReview("jato-review-1")).rejects.toThrow(
      /状态与逐国范围不一致/
    );
    await expect(api.getJatoMonthlyUpdateReview("jato-review-1")).rejects.toThrow(
      /keep_active 最终复核结构无效/
    );
  });

  it("submits one explicit decision per country to the resolution endpoint", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.method).toBe("POST");
      expect(JSON.parse(String(init?.body))).toEqual({
        decisions: [
          { country: "捷克", decision: "use_latest" },
          { country: "丹麦", decision: "keep_active" },
        ],
      });
      return Response.json({
        item: {
          jobId: "jato-review-1",
          status: "queued",
          phase: "historical_reclassification_resolution",
          artifacts: {
            candidateScope: "full_smart_merge",
          },
        },
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await api.resolveJatoMonthlyUpdateHistoricalReclassification(
      "jato-review-1",
      [
        { country: "捷克", decision: "use_latest" },
        { country: "丹麦", decision: "keep_active" },
      ],
    );

    expect(String(fetchMock.mock.calls[0]?.[0])).toMatch(
      /\/msrp\/monthly-update-jobs\/jato-review-1\/historical-reclassification-resolution$/
    );
    expect(response.item).toMatchObject({
      jobId: "jato-review-1",
      status: "queued",
      phase: "historical_reclassification_resolution",
      artifacts: {
        candidateScope: "full_smart_merge",
      },
    });
  });
});
