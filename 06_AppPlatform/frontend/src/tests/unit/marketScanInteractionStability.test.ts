import { describe, expect, it } from "vitest";

import { DEFAULT_EXPORT, type ExportSettings } from "../../components/ExportPanelHelpers";
import {
  buildMarketScanFuelTrendChartData,
  resolveMarketScanLoadingPresentation,
} from "../../pages/MarketScanPage";
import marketScanPageSource from "../../pages/MarketScanPage.tsx?raw";
import type { MarketScanFuelTrendItem } from "../../types";

const fuelTrendItems: MarketScanFuelTrendItem[] = [
  {
    label: "26.05",
    totalVolume: 100,
    fuelMix: { ICE: 30, BEV: 70 },
  },
  {
    label: "26.06",
    totalVolume: 100,
    fuelMix: { ICE: 20, BEV: 80 },
  },
];

function settings(dataLabelMode: ExportSettings["dataLabelMode"]): ExportSettings {
  return {
    ...DEFAULT_EXPORT,
    dataLabelMode,
    decimalPlaces: 0,
  };
}

function traceText(trace: unknown): string[] | undefined {
  const text = (trace as { text?: unknown }).text;
  return Array.isArray(text) ? text.map(String) : undefined;
}

function traceTextPosition(trace: unknown): unknown {
  return (trace as { textposition?: unknown }).textposition;
}

function traceShowLegend(trace: unknown): unknown {
  return (trace as { showlegend?: unknown }).showlegend;
}

describe("Monthly Fuel Trend data labels", () => {
  it("keeps only total value labels in Off mode", () => {
    const traces = buildMarketScanFuelTrendChartData(
      fuelTrendItems,
      ["ICE", "BEV"],
      settings("off"),
    );

    expect(traces.map((trace) => trace.name)).toEqual(["ICE", "BEV", "Total Labels"]);
    expect(traces.slice(0, 2).every((trace) => traceText(trace) === undefined)).toBe(true);
    expect(traceText(traces[2])).toEqual(["100", "100"]);
    expect(traceTextPosition(traces[2])).toBe("top center");
    expect(traceShowLegend(traces[2])).toBe(false);
  });

  it("shows each fuel segment's data value in Value mode", () => {
    const traces = buildMarketScanFuelTrendChartData(
      fuelTrendItems,
      ["ICE", "BEV"],
      settings("value"),
    );

    expect(traceText(traces[0])).toEqual(["30", "20"]);
    expect(traceText(traces[1])).toEqual(["70", "80"]);
    expect(traces.every((trace) => traceTextPosition(trace) === "inside")).toBe(true);
    expect(traces.some((trace) => trace.name === "Total Labels")).toBe(false);
  });

  it("calculates Percentage within each month's stacked column", () => {
    const traces = buildMarketScanFuelTrendChartData(
      fuelTrendItems,
      ["ICE", "BEV"],
      settings("percent"),
    );

    expect(traceText(traces[0])).toEqual(["30%", "20%"]);
    expect(traceText(traces[1])).toEqual(["70%", "80%"]);
    expect(traces.every((trace) => traceTextPosition(trace) === "inside")).toBe(true);
    expect(traces.some((trace) => trace.name === "Total Labels")).toBe(false);
  });
});

describe("Market Scan async loading feedback", () => {
  it("uses a blocking overlay only when switching views", () => {
    expect(resolveMarketScanLoadingPresentation("suvAll", "suvAll")).toBe("refresh");
    expect(resolveMarketScanLoadingPresentation("overview", "suvAll")).toBe("view");
  });

  it("binds the refresh overlay to the real request lifecycle", () => {
    const source = marketScanPageSource;
    const requestStart = source.indexOf("setLoading(true);");
    const requestCall = source.indexOf("api.marketScanDeck({", requestStart);
    const requestFinally = source.indexOf(".finally(() =>", requestCall);
    const requestStop = source.indexOf("setLoading(false);", requestFinally);

    expect(requestStart).toBeGreaterThan(-1);
    expect(requestCall).toBeGreaterThan(requestStart);
    expect(requestFinally).toBeGreaterThan(requestCall);
    expect(requestStop).toBeGreaterThan(requestFinally);
    expect(source).toContain('const showViewTransitionOverlay = refreshingActiveView && loadingPresentation === "view";');
    expect(source).toContain('{showViewTransitionOverlay ? (\n              <div className="market-scan-refresh-layer">');
    expect(source).toContain('mode="overlay"');
  });
});
