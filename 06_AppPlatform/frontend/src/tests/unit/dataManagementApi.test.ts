import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

function createStorageMock() {
  const store = new Map<string, string>();
  return {
    getItem(key: string) {
      return store.has(key) ? store.get(key) ?? null : null;
    },
    setItem(key: string, value: string) {
      store.set(key, value);
    },
    removeItem(key: string) {
      store.delete(key);
    },
    clear() {
      store.clear();
    },
  };
}

describe("data management api", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", createStorageMock());
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("returns airflow action payloads on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            item: {
              action: "start",
              detail: "Airflow 本地栈已启动。",
              status: {
                available: true,
                mode: "running",
                detail: "running",
                uiUrl: "http://127.0.0.1:8080",
                running: true,
                runningServices: 3,
                totalServices: 3,
                updatedAt: "2026-04-18T08:00:00+00:00",
                services: [],
                actions: {
                  canStart: false,
                  canStop: true,
                  canOpenUi: true,
                },
              },
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.startAirflow()).resolves.toMatchObject({
      action: "start",
      detail: "Airflow 本地栈已启动。",
      status: {
        mode: "running",
      },
    });
  });

  it("returns voc sync payloads on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            item: {
              root: "04_Processed_data/voc",
              countryCount: 8,
              sourceRunCount: 24,
              documentCount: 46,
              errorCount: 1,
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.syncVocRawToStore()).resolves.toMatchObject({
      countryCount: 8,
      sourceRunCount: 24,
      documentCount: 46,
    });
  });

  it("passes country filters to voc overview endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          item: {
            generatedAt: "2026-04-20T12:00:00+00:00",
            selectedCountryCode: "NO",
            selectedCountryLabel: "Norway / 挪威",
            availableCountries: [],
            overallMetrics: [],
            countryMetrics: [],
            artifacts: [],
            sourceRuns: [],
            observedSections: [],
            inferredSections: [],
            topPainPoints: [],
            topProductSignals: [],
            evidenceCards: [],
            documentation: [],
            staging: {
              databaseConnected: false,
              sourceRunCount: 0,
              documentCount: 0,
              publishReadyCount: 0,
              latestCollectedAt: null,
            },
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.getVocManagementOverview("NO")).resolves.toMatchObject({
      selectedCountryCode: "NO",
    });
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/data-management/voc/overview?country=NO"),
      expect.any(Object),
    );
  });

  it("builds Hermes proposals URLs without duplicate question marks", async () => {
    const fetchMock = vi.fn().mockImplementation(() => Promise.resolve(
      new Response(JSON.stringify([]), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    ));
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.hermesProposals()).resolves.toEqual([]);
    expect(fetchMock).toHaveBeenLastCalledWith(
      expect.stringContaining("/hermes/proposals"),
      expect.any(Object),
    );
    expect(String(fetchMock.mock.calls.at(-1)?.[0])).not.toContain("??");

    await expect(api.hermesProposals("implemented")).resolves.toEqual([]);
    expect(fetchMock).toHaveBeenLastCalledWith(
      expect.stringContaining("/hermes/proposals?status=implemented"),
      expect.any(Object),
    );
    expect(String(fetchMock.mock.calls.at(-1)?.[0])).not.toContain("??status=implemented");
  });

  it("loads standard Hermes pipeline status records", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify([
          {
            pipelineId: "unified_scraping_readiness",
            status: "success",
            recordsProcessed: 747,
            readinessStatus: "passed",
          },
        ]),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.hermesPipelineStatuses()).resolves.toMatchObject([
      {
        pipelineId: "unified_scraping_readiness",
        status: "success",
        recordsProcessed: 747,
        readinessStatus: "passed",
      },
    ]);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/hermes/pipeline/status"),
      expect.any(Object),
    );
  });

  it("maps MSRP finance observations and passes filters", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          rows: 1,
          total: 7,
          limit: 50,
          offset: 0,
          summary: {
            priceSemanticsCounts: { lease_monthly: 1 },
            financeTypeCounts: { private_lease: 1 },
            monthlyPaymentCount: 1,
            monthlyPaymentEurMin: 520.87,
            monthlyPaymentEurMax: 520.87,
            netPriceAfterSubsidyCount: 1,
            netPriceAfterSubsidyEurMin: 65043.48,
            netPriceAfterSubsidyEurMax: 65043.48,
            subsidyObservationCount: 1,
          },
          items: [
            {
              financeObservationId: "fo-1",
              observationId: "obs-1",
              scrapeBatchId: "batch-1",
              country: "Sweden",
              brand: "Volvo",
              jatoModel: "XC60",
              jatoTrim: "Plus",
              jatoPowertrain: "BEV",
              officialModel: "XC60",
              officialTrim: "Plus",
              priceSemantics: "lease_monthly",
              financeType: "private_lease",
              monthlyPayment: 5990,
              monthlyPaymentEur: 520.87,
              netPriceAfterSubsidyEur: 65043.48,
              subsidyAmountEur: 2173.91,
              currency: "SEK",
              sourceUrl: "https://example.test/finance",
              observedAtUtc: "2026-06-11T20:45:00Z",
              createdAtUtc: "2026-06-11T20:45:01Z",
              updatedAtUtc: "2026-06-11T20:45:01Z",
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.listMsrpFinanceObservations({
      country: "Sweden",
      brand: "Volvo",
      jato_model: "XC60",
      price_semantics: "lease_monthly",
      finance_type: "private_lease",
      has_monthly_payment: true,
      has_subsidy: true,
      has_net_price_after_subsidy: true,
      limit: 50,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/msrp/finance-observations?"),
      expect.any(Object),
    );
    const url = String(fetchMock.mock.calls[0][0]);
    expect(url).toContain("country=Sweden");
    expect(url).toContain("brand=Volvo");
    expect(url).toContain("jato_model=XC60");
    expect(url).toContain("price_semantics=lease_monthly");
    expect(url).toContain("finance_type=private_lease");
    expect(url).toContain("has_monthly_payment=true");
    expect(url).toContain("has_subsidy=true");
    expect(url).toContain("has_net_price_after_subsidy=true");
    expect(result.total).toBe(7);
    expect(result.summary.financeTypeCounts.private_lease).toBe(1);
    expect(result.items[0]).toMatchObject({
      financeObservationId: "fo-1",
      monthlyPayment: 5990,
      monthlyPaymentEur: 520.87,
      netPriceAfterSubsidyEur: 65043.48,
      subsidyAmountEur: 2173.91,
    });
  });

  it("maps MSRP reconciliation response and passes filters", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          schemaVersion: "msrp_multi_source_reconciliation_v1",
          generatedAtUtc: "2026-06-11T21:00:00Z",
          filters: {
            country: "Sweden",
            brand: "Volvo",
            jatoModel: "XC60",
          },
          thresholdPct: 1,
          summary: {
            observationRows: 2,
            reconciliationGroupCount: 1,
            statusCounts: { conflict: 1 },
            limit: 20,
          },
          items: [
            {
              country: "Sweden",
              brand: "Volvo",
              jatoModel: "XC60",
              jatoTrim: "Base",
              jatoPowertrain: "BEV",
              status: "conflict",
              recommendedAction: "review_conflicting_sources",
              sourceCount: 2,
              observationCount: 2,
              minMsrpValue: 50000,
              maxMsrpValue: 53000,
              avgMsrpValue: 51500,
              spreadValue: 3000,
              spreadPct: 5.83,
              thresholdPct: 1,
              currentPrice: null,
              sourceObservations: [
                {
                  observationId: "obs-a",
                  sourceId: "src-a",
                  sourceCode: "volvo_primary",
                  sourceType: "manufacturer_official",
                  sourceMsrpValue: 50000,
                  sourceCurrency: "EUR",
                  msrpValue: 50000,
                  currency: "EUR",
                  observedAtUtc: "2026-06-11T20:50:00Z",
                  sourceUrl: "https://example.test/a",
                  matchStatus: "auto_accepted",
                  matchConfidence: 0.97,
                  sourcePayloadHash: "hash-a",
                },
              ],
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.listMsrpReconciliation({
      country: "Sweden",
      brand: "Volvo",
      jato_model: "XC60",
      limit: 20,
      threshold_pct: 1,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/msrp/reconciliation?"),
      expect.any(Object),
    );
    const url = String(fetchMock.mock.calls[0][0]);
    expect(url).toContain("country=Sweden");
    expect(url).toContain("brand=Volvo");
    expect(url).toContain("jato_model=XC60");
    expect(url).toContain("limit=20");
    expect(url).toContain("threshold_pct=1");
    expect(result.summary.statusCounts.conflict).toBe(1);
    expect(result.items[0]).toMatchObject({
      status: "conflict",
      spreadPct: 5.83,
      sourceCount: 2,
    });
    expect(result.items[0].sourceObservations[0]).toMatchObject({
      sourceCode: "volvo_primary",
      msrpValue: 50000,
    });
  });

  it("queues MSRP reconciliation conflicts for review", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          item: {
            schemaVersion: "msrp_reconciliation_review_queue_v1",
            generatedAtUtc: "2026-06-11T21:10:00Z",
            filters: {
              country: "Sweden",
              brand: "Volvo",
              jatoModel: "XC60",
            },
            thresholdPct: 1,
            summary: {
              observationRows: 2,
              reconciliationGroupCount: 1,
              conflictGroupCount: 1,
              reviewCasesQueued: 1,
              reviewCasesCreated: 1,
              reviewCasesReused: 0,
              limit: 20,
            },
            sampleConflicts: [
              {
                country: "Sweden",
                brand: "Volvo",
                jatoModel: "XC60",
                jatoTrim: "Base",
                jatoPowertrain: "BEV",
                sourceCount: 2,
                spreadPct: 5.83,
                spreadValue: 3000,
                reviewObservationId: "obs-a",
              },
            ],
            sampleReviewCases: [
              {
                reviewCaseId: "case-a",
                observationId: "obs-a",
                country: "Sweden",
                brand: "Volvo",
                jatoModel: "XC60",
                jatoTrim: "Base",
                jatoPowertrain: "BEV",
                officialModel: "XC60",
                officialTrim: "Base",
                officialEdition: null,
                officialPowertrain: "BEV",
                candidateMatches: [],
                matchConfidence: 0.97,
                reviewStatus: "open",
                sourceUrl: "https://example.test/a",
                sourceSnapshotPath: null,
                currentAssignee: null,
                createdAtUtc: "2026-06-11T21:10:00Z",
                updatedAtUtc: "2026-06-11T21:10:00Z",
              },
            ],
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.queueMsrpReconciliationReviewCases({
      country: "Sweden",
      brand: "Volvo",
      jato_model: "XC60",
      limit: 20,
      threshold_pct: 1,
    });

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/msrp/reconciliation/review-cases?"),
      expect.objectContaining({ method: "POST" }),
    );
    expect(result.summary).toMatchObject({
      reviewCasesQueued: 1,
      reviewCasesCreated: 1,
      reviewCasesReused: 0,
    });
    expect(result.sampleConflicts[0].spreadValue).toBe(3000);
    expect(result.sampleReviewCases[0]).toMatchObject({
      id: "case-a",
      reviewStatus: "open",
    });
  });

  it("maps source-observation review candidates in review case details", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          item: {
            reviewCase: {
              reviewCaseId: "case-a",
              observationId: "obs-a",
              country: "Sweden",
              brand: "Volvo",
              jatoModel: "XC60",
              jatoTrim: "Base",
              jatoPowertrain: "BEV",
              officialModel: "XC60",
              officialTrim: "Base",
              officialEdition: null,
              officialPowertrain: "BEV",
              candidateMatches: [
                {
                  candidateType: "source_observation",
                  reconciliationStatus: "conflict",
                  recommendedAction: "review_conflicting_sources",
                  thresholdPct: 1,
                  spreadPct: 5.83,
                  spreadValue: 3000,
                  sourceRank: 1,
                  observationId: "obs-a",
                  sourceId: "src-a",
                  sourceCode: "volvo_primary",
                  sourceType: "manufacturer_official",
                  sourceMsrpValue: 50000,
                  sourceCurrency: "EUR",
                  msrpValue: 50000,
                  currency: "EUR",
                  observedAtUtc: "2026-06-11T21:00:00Z",
                  sourceUrl: "https://example.test/primary",
                  matchStatus: "auto_accepted",
                  matchConfidence: 0.97,
                  sourcePayloadHash: "hash-a",
                },
              ],
              matchConfidence: 0.97,
              reviewStatus: "open",
              sourceUrl: "https://example.test/primary",
              sourceSnapshotPath: null,
              currentAssignee: null,
              createdAtUtc: "2026-06-11T21:00:00Z",
              updatedAtUtc: "2026-06-11T21:00:00Z",
            },
            observation: null,
            decisions: [],
            currentPrice: null,
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.getReviewCaseDetail("case-a");
    const candidate = result.item.candidateMatches?.[0];

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/review/cases/case-a"),
      expect.any(Object),
    );
    expect(candidate).toMatchObject({
      candidateType: "source_observation",
      observationId: "obs-a",
      sourceCode: "volvo_primary",
      msrpValue: 50000,
      spreadPct: 5.83,
      matchConfidence: 0.97,
    });
  });

  it("posts accepted source observation decisions", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          item: {
            decision: {
              reviewDecisionId: "decision-a",
              reviewCaseId: "case-a",
              observationId: "obs-case",
              decision: "approve",
              note: "Accepted MSRP source pdf",
              decidedBy: "analyst",
              decidedAtUtc: "2026-06-11T21:20:00Z",
            },
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await api.createReviewDecision("case-a", {
      decision: "approve",
      accepted_observation_id: "obs-selected",
      note: "Accepted MSRP source pdf",
      decided_by: "analyst",
    });

    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/review/cases/case-a/decisions"),
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          decision: "approve",
          accepted_observation_id: "obs-selected",
          note: "Accepted MSRP source pdf",
          decided_by: "analyst",
        }),
      }),
    );
  });

  it("preserves conflict detail for airflow stop errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: "Airflow 当前已经停止。" }),
          { status: 409, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.stopAirflow()).rejects.toThrow(
      "409 Airflow 当前已经停止。",
    );
  });

  it("preserves runtime detail for airflow start errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: "postgres failed" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.startAirflow()).rejects.toThrow("500 postgres failed");
  });
});
