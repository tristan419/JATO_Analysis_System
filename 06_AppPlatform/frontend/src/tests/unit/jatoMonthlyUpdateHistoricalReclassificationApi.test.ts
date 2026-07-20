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
        historicalReclassificationReport: {
          status: "resolved",
          countries: [{
            country: "捷克",
            decision: "use_latest",
            comparedThrough: "2026-03",
            historicalMonthCount: 39,
            jointMismatchCellCount: 5217,
            jointMovedSales: 8035,
            monthlyTotalsStable: true,
            decisionRequired: true,
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
        },
      },
    })));

    const response = await api.getJatoMonthlyUpdateReview("jato-review-1");

    expect(response.item.historicalReclassificationReport).toMatchObject({
      status: "resolved",
      countries: [{
        country: "捷克",
        decision: "use_latest",
        comparedThrough: "2026-03",
        jointMismatchCellCount: 5217,
        monthlyTotalsStable: true,
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
    });
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
