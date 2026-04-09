import type { CSSProperties } from "react";
import type { Data, Layout, Config } from "plotly.js";
import Plotly from "plotly.js-cartesian-dist-min";
import createPlotlyComponent from "react-plotly.js/factory";

const Plot = createPlotlyComponent(Plotly);

const BASE_CONFIG: Partial<Config> = {
  displaylogo: false,
  responsive: true,
  modeBarButtonsToRemove: ["sendDataToCloud" as never],
  toImageButtonOptions: { format: "png", filename: "jato_chart", height: 800, width: 1200, scale: 2 },
};

const BASE_LAYOUT: Partial<Layout> = {
  autosize: true,
  margin: { l: 52, r: 24, t: 28, b: 52 },
  font: { family: '"Helvetica Neue", Helvetica, Arial, sans-serif', size: 11 },
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

export type PlotlyChartProps = Props;

export function PlotlyChart({ data, layout, config, height = 450, style }: Props) {
  return (
    <Plot
      data={data}
      layout={{ ...BASE_LAYOUT, height, ...layout } as Layout}
      config={{ ...BASE_CONFIG, ...config } as Config}
      useResizeHandler
      style={{ width: "100%", ...style }}
    />
  );
}
