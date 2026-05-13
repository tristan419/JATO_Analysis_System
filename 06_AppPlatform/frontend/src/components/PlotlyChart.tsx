import type { CSSProperties } from "react";
import type { Data, Layout, Config, PlotMouseEvent, PlotSelectionEvent } from "plotly.js";
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
  onHover?: (event: Readonly<PlotMouseEvent>) => void;
  onUnhover?: (event: Readonly<PlotMouseEvent>) => void;
  onClick?: (event: Readonly<PlotMouseEvent>) => void;
  onSelected?: (event: Readonly<PlotSelectionEvent>) => void;
}

export type PlotlyChartProps = Props;

export function PlotlyChart({ data, layout, config, height = 450, style, onHover, onUnhover, onClick, onSelected }: Props) {
  return (
    <Plot
      data={data}
      layout={{ ...BASE_CHART_LAYOUT, height, ...layout } as Layout}
      config={{ ...BASE_CHART_CONFIG, ...config } as Config}
      useResizeHandler
      style={{ width: "100%", ...style }}
      onHover={onHover}
      onUnhover={onUnhover}
      onClick={onClick}
      onSelected={onSelected}
    />
  );
}
