/**
 * Shared Plotly default configuration and layout constants.
 *
 * Extracted from PlotlyChart.tsx and MarketScanPage.tsx to eliminate duplication.
 */
import type { Config, Layout } from "plotly.js";

/** Base Plotly config — no logo, responsive, customized export button */
export const BASE_CHART_CONFIG: Partial<Config> = {
  displaylogo: false,
  responsive: true,
  modeBarButtonsToRemove: ["sendDataToCloud" as never],
  toImageButtonOptions: { format: "png", filename: "jato_chart", height: 800, width: 1200, scale: 2 },
};

/** Base Plotly layout — used by PlotlyChart component and overridden per page */
export const BASE_CHART_LAYOUT: Partial<Layout> = {
  autosize: true,
  margin: { l: 52, r: 24, t: 28, b: 52 },
  font: { family: '"Helvetica Neue", Helvetica, Arial, sans-serif', size: 11 },
  paper_bgcolor: "white",
  plot_bgcolor: "white",
  hovermode: "closest",
};

/** Transparent background variant for MarketScan and overlay charts */
export const TRANSPARENT_CHART_LAYOUT: Partial<Layout> = {
  paper_bgcolor: "rgba(0, 0, 0, 0)",
  plot_bgcolor: "rgba(0, 0, 0, 0)",
  margin: { l: 52, r: 24, t: 20, b: 48 },
  legend: {
    orientation: "h",
    yanchor: "bottom",
    y: 1.02,
    xanchor: "left",
    x: 0,
  },
  font: { family: '"Helvetica Neue", Helvetica, Arial, sans-serif', size: 11 },
  hoverlabel: { bgcolor: "#0f172a", font: { color: "#f8fafc" } },
};

/** Build a category axis from labels (prevents duplicate logic) */
export function buildCategoryAxis(
  labels: string[],
  extra: Partial<Layout["xaxis"]> = {},
): Partial<Layout["xaxis"]> {
  const ordered = Array.from(new Set(labels));
  return {
    type: "category",
    categoryorder: "array",
    categoryarray: ordered,
    ...extra,
  };
}

/* ── Bar chart label formatting ─────────────────────── */

/** Compact label used in horizontal bar rankings: "12,345台 · 23.5%" */
export function formatCompactBarLabel(volume: number, share: number): string {
  return `${volume.toLocaleString()}台 · ${(share * 100).toFixed(1)}%`;
}

/** Default label position for bar charts */
export function barLabelPosition(orientation?: string): string {
  return orientation === "h" ? "middle right" : "outside";
}
