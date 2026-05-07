// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { CustomerInsightsPage } from "../../pages/CustomerInsightsPage";
import type { CustomerInsightDeckResponse, CustomerInsightEvidenceCard } from "../../types";

vi.mock("../../api/client", () => ({
  api: {
    nordicCustomerDeck: vi.fn(),
    nordicHevCustomerDeck: vi.fn(),
  },
}));

vi.mock("../../components/LazyPlotlyChart", () => ({
  LazyPlotlyChart: () => <div data-testid="plotly-chart" />,
  preloadPlotlyChartRuntime: vi.fn(() => Promise.resolve()),
}));

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (reason?: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function buildDeck(
  mode: "benchmark" | "forum_live",
  subtitle: string,
  coverageLabel = "Nordic benchmark sample",
  evidenceCards: CustomerInsightEvidenceCard[] = [],
): CustomerInsightDeckResponse {
  return {
    metadata: {
      protocolVersion: "customer-insight-test/v1",
      datasetLabel: mode === "benchmark" ? "Benchmark Excel" : "Forum VOC live",
      sourceFile: "fixture.json",
      respondentCount: mode === "benchmark" ? 113 : 7,
      updatedAt: Date.now(),
      mode,
      modeLabel: mode === "benchmark" ? "Benchmark Excel" : "Forum VOC live",
      sourceKind: mode === "benchmark" ? "benchmark_excel" : "forum_voc",
      sampleUnitLabel: mode === "benchmark" ? "samples" : "docs",
      coverageLabel,
      countryCodes: mode === "forum_live" ? ["SE", "NO"] : ["SE", "FI", "NO", "DK"],
    },
    page: {
      title: "看客户",
      subtitle,
      summaryText: `${subtitle} summary`,
      methodologyNote: "methodology",
      conclusionCards: [
        {
          label: "Top signal",
          headline: `${subtitle} headline`,
          detail: "detail",
        },
      ],
      metrics: [],
      profile: {
        sampleSources: [],
        attentionChannels: [],
        gender: [],
        age: [],
        household: [],
        weeklyCommute: [],
      },
      occupation: { items: [] },
      lifestyle: { items: [] },
      powertrain: { items: [] },
      philosophy: { items: [] },
      purchaseUses: { items: [] },
      decisionFactors: { items: [] },
      persona: {
        title: "persona",
        summary: "summary",
        facts: [],
        notes: [],
      },
      forumLive: mode === "forum_live"
        ? {
            sourceMix: [],
            siteTypes: [],
            languages: [],
            publishTiers: [],
            sentiment: [],
            ownershipStages: [],
            painPoints: [],
            productSignals: [],
            powertrains: [],
            decisionFactors: [],
            evidenceCards,
            observedSections: [],
            inferredSections: [],
          }
        : undefined,
    },
  };
}

describe("CustomerInsightsPage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it("clears the previous deck while a new mode request is in flight", async () => {
    const initialRequest = deferred<ReturnType<typeof buildDeck>>();
    const nextRequest = deferred<ReturnType<typeof buildDeck>>();
    const mockNordicCustomerDeck = vi.mocked(api.nordicCustomerDeck);
    mockNordicCustomerDeck
      .mockReturnValueOnce(initialRequest.promise)
      .mockReturnValueOnce(nextRequest.promise);

    render(<CustomerInsightsPage />);

    initialRequest.resolve(buildDeck("benchmark", "Benchmark subtitle"));
    await screen.findByText("Benchmark subtitle");

    fireEvent.change(screen.getByLabelText("数据模式"), {
      target: { value: "forum_live" },
    });

    await waitFor(() => {
      expect(screen.queryByText("Benchmark subtitle")).toBeNull();
      expect(screen.getByText("正在整理 live forum VOC")).toBeTruthy();
    });

    nextRequest.resolve(buildDeck("forum_live", "Forum subtitle", "SE / NO"));
    await screen.findByText("Forum subtitle");
  });

  it("does not keep a stale deck visible when the follow-up request fails", async () => {
    const initialRequest = deferred<ReturnType<typeof buildDeck>>();
    const nextRequest = deferred<ReturnType<typeof buildDeck>>();
    const mockNordicCustomerDeck = vi.mocked(api.nordicCustomerDeck);
    mockNordicCustomerDeck
      .mockReturnValueOnce(initialRequest.promise)
      .mockReturnValueOnce(nextRequest.promise);

    render(<CustomerInsightsPage />);

    initialRequest.resolve(buildDeck("benchmark", "Benchmark subtitle"));
    await screen.findByText("Benchmark subtitle");

    fireEvent.change(screen.getByLabelText("数据模式"), {
      target: { value: "forum_live" },
    });

    nextRequest.reject(new Error("forum request failed"));

    await waitFor(() => {
      expect(screen.getByText("北欧用户调研加载失败")).toBeTruthy();
      expect(screen.getByText("forum request failed")).toBeTruthy();
      expect(screen.queryByText("Benchmark subtitle")).toBeNull();
    });
  });

  it("shows scraped content details for forum live evidence cards", async () => {
    const mockNordicCustomerDeck = vi.mocked(api.nordicCustomerDeck);
    mockNordicCustomerDeck
      .mockResolvedValueOnce(buildDeck("benchmark", "Benchmark subtitle"))
      .mockResolvedValueOnce(
        buildDeck("forum_live", "Forum subtitle", "NO", [
          {
            title: "Charging issue thread",
            url: "https://example.com/no/thread-1",
            siteName: "Bil24",
            siteType: "media_comments",
            countryCode: "NO",
            language: "no",
            publishTier: "high",
            sentiment: "neutral",
            signals: ["Charging convenience"],
            evidenceSnippets: ["Public fast chargers were often occupied."],
            excerpt: "Drivers say public fast chargers were often occupied.",
            contentPreview: "Drivers say public fast chargers were often occupied and charging convenience remains inconsistent in busy corridors.",
            contentTruncated: false,
            observationCount: 2,
            observations: [
              {
                signalKind: "productSignal",
                label: "Charging convenience",
                sentence: "Charging convenience remains inconsistent in busy corridors.",
                matchedTokens: ["charging"],
                sentiment: "neutral",
              },
            ],
          },
        ]),
      );

    render(<CustomerInsightsPage />);
    await screen.findByText("Benchmark subtitle");

    fireEvent.change(screen.getByLabelText("数据模式"), {
      target: { value: "forum_live" },
    });

    await screen.findByText("Forum subtitle");
    fireEvent.click(screen.getByRole("button", { name: "查看抓取内容" }));

    await waitFor(() => {
      expect(screen.getByText("抓取正文预览")).toBeTruthy();
      expect(screen.getByText("Drivers say public fast chargers were often occupied.")).toBeTruthy();
      expect(screen.getByText("Charging convenience remains inconsistent in busy corridors.")).toBeTruthy();
    });
  });

  it("supports a benchmark-only custom loader for dedicated customer pages", async () => {
    const customLoader = vi.fn().mockResolvedValue(buildDeck("benchmark", "HEV subtitle"));

    render(
      <CustomerInsightsPage
        deckLoader={customLoader}
        modeOptions={["benchmark"]}
        benchmarkCopy={{ loadingLabel: "正在整理 HEV 页" }}
      />,
    );

    await screen.findByText("HEV subtitle");

    expect(customLoader).toHaveBeenCalledWith("benchmark", undefined);
    expect(screen.queryByLabelText("数据模式")).toBeNull();
  });
});
