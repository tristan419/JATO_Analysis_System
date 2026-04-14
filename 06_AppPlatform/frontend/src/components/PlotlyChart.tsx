import type { CSSProperties } from "react";
import type { Data, Layout, Config } from "plotly.js";
import Plotly from "plotly.js-cartesian-dist-min";
import createPlotlyComponent from "react-plotly.js/factory";

import { BASE_CHART_CONFIG, BASE_CHART_LAYOUT } from "../utils/plotlyDefaults";

const Plot = createPlotlyComponent(Plotly);

interface Props {
  data: Data[];
  layout?: Partial<Layout>;
  config?: Partial<Config>;
  height?: number;
  style?: CSSProperties;
}

export type PlotlyChartProps = Props;

export function PlotlyChart({ data, layout, config, height = 450, style }: Props) {
  return (
    <Plot
      data={data}
      layout={{ ...BASE_CHART_LAYOUT, height, ...layout } as Layout}
      config={{ ...BASE_CHART_CONFIG, ...config } as Config}
      useResizeHandler
      style={{ width: "100%", ...style }}
    />
  );
}
