import { Suspense, lazy } from "react";
import type { CSSProperties } from "react";

import { LoadingSurface } from "./LoadingSurface";
import type { PlotlyChartProps } from "./PlotlyChart";

let plotlyChartModulePromise: Promise<typeof import("./PlotlyChart")> | null = null;

function loadPlotlyChartModule() {
  if (!plotlyChartModulePromise) {
    plotlyChartModulePromise = import("./PlotlyChart").catch((error) => {
      plotlyChartModulePromise = null;
      throw error;
    });
  }
  return plotlyChartModulePromise;
}

const PlotlyChart = lazy(() =>
  loadPlotlyChartModule().then((module) => ({ default: module.PlotlyChart }))
);

export function preloadPlotlyChartRuntime() {
  return loadPlotlyChartModule().then(() => undefined);
}

const FALLBACK_SHELL_STYLE: CSSProperties = {
  width: "100%",
  minHeight: 220,
  display: "grid",
  placeItems: "center",
  border: "1px dashed var(--c-border-soft)",
  background: "linear-gradient(180deg, rgba(255, 255, 255, 0.96) 0%, rgba(237, 243, 249, 0.92) 100%)",
};

function ChartFallback({ height }: { height: number }) {
  return (
    <div style={{ ...FALLBACK_SHELL_STYLE, minHeight: Math.max(height, 220) }}>
      <LoadingSurface
        mode="inline"
        kicker="Chart"
        label="正在加载图表运行时"
        detail="Plotly 会按需下载，页面其他区域不再等待它。"
      />
    </div>
  );
}

export function LazyPlotlyChart(props: PlotlyChartProps) {
  return (
    <Suspense fallback={<ChartFallback height={props.height ?? 450} />}>
      <PlotlyChart {...props} />
    </Suspense>
  );
}