import { Suspense, lazy } from "react";
import type { CSSProperties } from "react";
import type { Data, Layout, Config } from "plotly.js";

const Plot = lazy(() => import("react-plotly.js"));

const BASE_CONFIG: Partial<Config> = {
  displaylogo: false,
  responsive: true,
  modeBarButtonsToRemove: ["sendDataToCloud" as never],
  toImageButtonOptions: { format: "png", filename: "jato_chart", height: 800, width: 1200, scale: 2 },
};

const BASE_LAYOUT: Partial<Layout> = {
  autosize: true,
  margin: { l: 60, r: 30, t: 40, b: 60 },
  font: { family: "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif", size: 12 },
  paper_bgcolor: "white",
  plot_bgcolor: "white",
  hovermode: "closest",
};

interface Props {
  data: Data[];
  layout?: Partial<Layout>;
  config?: Partial<Config>;
  height?: number;
  style?: CSSProperties;
}

export function PlotlyChart({ data, layout, config, height = 450, style }: Props) {
  return (
    <Suspense
      fallback={
        <div
          style={{
            width: "100%",
            height,
            borderRadius: 12,
            background: "#f8fafc",
            border: "1px solid #e2e8f0",
          }}
        />
      }
    >
      <Plot
        data={data}
        layout={{ ...BASE_LAYOUT, height, ...layout } as Layout}
        config={{ ...BASE_CONFIG, ...config } as Config}
        useResizeHandler
        style={{ width: "100%", ...style }}
      />
    </Suspense>
  );
}
