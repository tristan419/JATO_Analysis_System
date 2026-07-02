import { Suspense, lazy, useEffect, useRef, useState } from "react";
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
const CHART_VIEWPORT_ROOT_MARGIN = "160px 0px";
const CHART_VISIBILITY_FALLBACK_DELAY_MS = 6_000;

type VisibilityWindow = Window & typeof globalThis & {
  IntersectionObserver?: typeof IntersectionObserver;
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

function resolvePlaceholderHeight(props: PlotlyChartProps): number {
  if (typeof props.height === "number") return props.height;
  if (typeof props.style?.height === "number") return props.style.height;
  if (typeof props.layout?.height === "number") return props.layout.height;
  return 450;
}

export function LazyPlotlyChart(props: PlotlyChartProps) {
  const [shouldLoad, setShouldLoad] = useState(false);
  const placeholderRef = useRef<HTMLDivElement | null>(null);
  const height = resolvePlaceholderHeight(props);

  useEffect(() => {
    if (shouldLoad) return undefined;
    const element = placeholderRef.current;
    if (!element) return undefined;
    const visibilityWindow = window as VisibilityWindow;
    if (typeof visibilityWindow.IntersectionObserver !== "function") {
      const handle = window.setTimeout(() => setShouldLoad(true), CHART_VISIBILITY_FALLBACK_DELAY_MS);
      return () => window.clearTimeout(handle);
    }
    const observer = new visibilityWindow.IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        setShouldLoad(true);
        observer.disconnect();
      }
    }, {
      rootMargin: CHART_VIEWPORT_ROOT_MARGIN,
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [shouldLoad]);

  if (!shouldLoad) {
    return (
      <div ref={placeholderRef}>
        <ChartFallback height={height} />
      </div>
    );
  }

  return (
    <Suspense fallback={<ChartFallback height={height} />}>
      <PlotlyChart {...props} />
    </Suspense>
  );
}

export type { PlotlyChartProps } from "./PlotlyChart";
