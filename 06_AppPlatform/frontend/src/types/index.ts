export type FilterMap = Record<string, string[]>;

export interface AnalysisQuery {
  filters: FilterMap;
  group_by?: string;
  metric_candidates: string[];
  top_n: number;
  prefer_precomputed: boolean;
}

export interface CrudItem {
  id: string;
  code: string;
  name: string;
  status: string;
  notes: string;
}

export interface CrudListResponse {
  page: number;
  pageSize: number;
  total: number;
  items: CrudItem[];
}

export interface TimeSeriesPoint {
  time: string;
  value: number;
}

export interface OverviewResponse {
  route: string;
  kpis: {
    totalRows: number;
    countryCount: number;
    brandCount: number;
    modelCount: number;
    versionCount: number;
    cumulativeSales?: number;
    avgMsrp?: number;
  };
  monthSeries: TimeSeriesPoint[];
  yearSeries: TimeSeriesPoint[];
}

export interface DetailResponse {
  page: number;
  pageSize: number;
  total: number;
  items: Record<string, unknown>[];
}

export interface AdvancedChartResponse {
  group: string;
  chart: string;
  rows: number;
  items: Record<string, string | number>[];
  meta?: Record<string, unknown>;
}

export interface GroupedTimeSeriesItem {
  time: string;
  value: number;
  series: string;
}

export interface GroupedTimeSeriesResponse {
  grain: string;
  rows: number;
  items: GroupedTimeSeriesItem[];
  others_detail?: OthersDetailItem[];
}

export interface OthersDetailItem {
  name: string;
  sales: number;
  share: number;
}

/* ---- Bug 2: Model Version Bubble ---- */
export interface ModelVersionItem {
  Version: string;
  Powertrain: string;
  Trim: string;
  Length: number;
  MSRP: number;
  Sales: number;
}

export interface ModelVersionsResponse {
  rows: number;
  items: ModelVersionItem[];
}

/* ---- Bug 3: OJ Positioning Map ---- */
export interface PositioningMapItem {
  Brand: string;
  Model: string;
  Length: number;
  MSRP: number;
  Sales: number;
  Segment: string;
  cluster: number;
}

export interface PositioningMapResponse {
  rows: number;
  items: PositioningMapItem[];
  target: { Length: number; MSRP: number } | null;
  cluster_top3: string[];
}

/* ---- RV Finance Dashboard ---- */
export interface RvFinanceVehicle {
  vehicle: string;
  msrp: number;
  down_pct: number;
  rv_pct: number;
  apr_pct: number;
  term: number;
}

export interface RvFinanceResult extends RvFinanceVehicle {
  down: number;
  principal: number;
  balloon: number;
  pv_rv: number;
  net_financed: number;
  monthly: number;
  total_payments: number;
}

export interface RvWaterfallStep {
  label: string;
  value: number;
  type?: string;
}

export interface RvSensitivityPoint {
  param: string;
  scenario: string;
  param_value: number;
  monthly: number;
  delta: number;
}

export interface RvSensitivitySummaryRow {
  param: string;
  low: number;
  base: number;
  high: number;
}

export interface RvContourMatrix {
  apr_values: number[];
  rv_values: number[];
  z: number[][];
}

export interface RvFinanceResponse {
  results: RvFinanceResult[];
  waterfall: RvWaterfallStep[];
  sensitivity: RvSensitivityPoint[];
  contour: RvContourMatrix;
  currency?: string;
  rate?: number;
  presets?: Record<string, Partial<RvFinanceVehicle>>;
}
