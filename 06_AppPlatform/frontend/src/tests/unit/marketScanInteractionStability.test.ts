import { describe, expect, it } from "vitest";

import { DEFAULT_EXPORT, type ExportSettings } from "../../components/ExportPanelHelpers";
import { buildMarketScanFuelTrendChartData } from "../../pages/MarketScanPage";
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

describe("Monthly Fuel Trend data labels", () => {
  it("hides every label in Off mode without adding a total-label trace", () => {
    const traces = buildMarketScanFuelTrendChartData(
      fuelTrendItems,
      ["ICE", "BEV"],
      settings("off"),
    );

    expect(traces.map((trace) => trace.name)).toEqual(["ICE", "BEV"]);
    expect(traces.every((trace) => traceText(trace) === undefined)).toBe(true);
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
    expect(source).toContain('{refreshingActiveView ? (\n              <div className="market-scan-refresh-layer">');
    expect(source).toContain('mode="overlay"');
  });
});
