// Request types
export interface AdvancedAnalysisBaseRequest {
  country?: string;
  target_period?: string;
  time_range?: { start: string; end: string };
  fuel_types?: string[];
  segments?: string[];
}

export type AdvancedAnalysisSalesMode = "month" | "ytd" | "rolling12";

export interface AdvancedAnalysisKpiRequest extends AdvancedAnalysisBaseRequest {
  group_by?: string[];
  top_n?: number;
}

export interface AdvancedAnalysisShiftShareRequest extends AdvancedAnalysisBaseRequest {
  base_period?: string;
  cell_dims?: string[];
}

export interface AdvancedAnalysisSeasonalRequest extends AdvancedAnalysisBaseRequest {
  model_filter?: string;
  segment_filter?: string;
}

export interface AdvancedAnalysisCellAttributionRequest extends AdvancedAnalysisBaseRequest {
  cell_dims?: string[];
  top_n_cells?: number;
}

export interface AdvancedAnalysisTransferMatrixRequest extends AdvancedAnalysisBaseRequest {
  cell_dims?: string[];
  top_n_models?: number;
}

// Response types
export interface KpiRow {
  segment?: string;
  model?: string;
  make?: string;
  powertrain?: string;
  period: string;
  sales: number;
  mom: number | null;
  yoy: number | null;
  trailing12_sum: number | null;
  rolling12_avg: number | null;
  share: number | null;
}

export interface KpiTableResponse {
  group_by: string[];
  rows: KpiRow[];
  total_rows: number;
}

export interface ShiftShareItem {
  segment?: string;
  model?: string;
  sales: number;
  sales_0: number;
  dV: number;
  market_growth_effect: number;
  share_shift_effect: number;
  interaction_effect: number;
}

export interface ShiftShareResponse {
  base_period: string;
  target_period: string;
  cell_dims: string[];
  total_market_delta: number;
  winners: ShiftShareItem[];
  losers: ShiftShareItem[];
}

export interface SeasonalDecompositionResponse {
  periods: string[];
  observed: number[];
  trend: number[];
  seasonal: number[];
  resid: number[];
  model_filter?: string;
  segment_filter?: string;
  error?: string;
}

export interface CellAttributionItem {
  segment?: string;
  registration_type?: string;
  drive_type?: string;
  sales: number;
  sales_0: number;
  dV: number;
  yoy_pct: number;
}

export interface CellAttributionResponse {
  base_period: string;
  target_period: string;
  cell_dims: string[];
  cells: CellAttributionItem[];
  error?: string;
}

export interface AdvancedAnalysisNestedShiftShareRequest extends AdvancedAnalysisBaseRequest {
  base_period?: string;
  hierarchy?: string[];
}


// Nested shift-share response
export interface NestedShiftShareCell {
  level: number;
  segment?: string;
  registration_type?: string;
  drive_type?: string;
  sales: number;
  sales_0: number;
  dV: number;
  dM: number;
  market_growth_effect: number;
  share_shift_effect: number;
  interaction_effect: number;
}

export interface NestedShiftShareLevel {
  level: number;
  label: string;
  cell_dims: string[];
  cell_count: number;
  cells: NestedShiftShareCell[];
}

export interface NestedShiftShareResponse {
  base_period: string;
  target_period: string;
  hierarchy: string[];
  levels: NestedShiftShareLevel[];
  error?: string;
}

export interface TransferLink {
  source: number;
  target: number;
  value: number;
}

export interface TransferNode {
  label: string;
}

export interface TransferMatrixResponse {
  nodes: TransferNode[];
  links: TransferLink[];
  base_period?: string;
  target_period?: string;
  total_transfer_volume?: number;
  message?: string;
  error?: string;
}

// ── Drill-down types ──

export interface AdvancedAnalysisDrilldownRequest extends AdvancedAnalysisBaseRequest {
  base_period?: string;
  scope_filters?: Array<{ dim: string; value: string }>;
  top_n?: number;
}

export interface ScopeSummary {
  total_sales: number;
  total_sales_0: number;
  dV: number;
  yoy_pct: number;
  market_state: "growth" | "decline" | "stable";
}

export interface DrilldownCell {
  label: string;
  sales: number;
  sales_0: number;
  dV: number;
  yoy_pct: number;
  market_state: "growth" | "decline" | "stable";
}

export interface DrilldownModel {
  model: string;
  sales: number;
  sales_0: number;
  dV: number;
  market_growth_effect: number;
  share_shift_effect: number;
  interaction_effect: number;
}

// ── Transfer Mart (one-page analysis) ──

export interface AdvancedAnalysisTransferMartRequest extends AdvancedAnalysisBaseRequest {
  base_period?: string;
  sales_mode?: AdvancedAnalysisSalesMode;
  scope_filters?: Array<{ dim: string; value: string }>;
  top_n?: number;
}

export interface AdvancedAnalysisCompetitorSetRequest extends AdvancedAnalysisTransferMartRequest {
  target_model?: string;
  profile_specs?: CompetitorProfileSpecs;
}

export interface TransferMartScopeSummary {
  total_sales_tgt: number;
  total_sales_base: number;
  dM: number;
  yoy_pct: number;
  market_state: "growth" | "decline" | "stable";
}

export interface TransferMartWaterfallItem {
  label: string;
  value: number;
  kind: "market" | "mix" | "share" | "interaction";
}

export interface TransferMartModel {
  model: string;
  sales_tgt: number;
  sales_base: number;
  dV: number;
  share_tgt: number;
  share_base: number;
  share_change: number;
  market_carryover: number;
  channel_mix: number;
  drive_mix: number;
  powertrain_mix: number;
  pure_share_shift: number;
  interaction: number;
  donors: Array<{ model: string; estimated_flow: number }>;
  recipients: Array<{ model: string; estimated_flow: number }>;
  resilience: "strong" | "weak" | "neutral";
}

export interface TransferMartHeatmapCell {
  channel: string;
  drive: string;
  net_shift: number;
}

export interface TransferMartPowertrainOrigin {
  powertrain: string;
  origin: string;
  shift: number;
  sales: number;
}

export interface TransferMartMomentum {
  model: string;
  share_slope: number;
  recent_shares: number[];
  trend: "rising" | "falling" | "flat";
}

export interface TransferMartTimeseriesItem {
  period: string;
  channel?: string;
  powertrain?: string;
  volume: number;
  share?: number;
}

export interface ModelSharePoint {
  period: string;
  share: number;
}

export interface ModelTimeseries {
  model: string;
  shares: ModelSharePoint[];
}

export interface TransferMartResponse {
  base_period: string;
  target_period: string;
  sales_mode?: AdvancedAnalysisSalesMode;
  scope_summary: TransferMartScopeSummary;
  market_waterfall: TransferMartWaterfallItem[];
  winners: TransferMartModel[];
  losers: TransferMartModel[];
  models: TransferMartModel[];
  channel_drive_heatmap: TransferMartHeatmapCell[];
  powertrain_origin_breakdown: TransferMartPowertrainOrigin[];
  momentum: TransferMartMomentum[];
  channel_timeseries: TransferMartTimeseriesItem[];
  powertrain_timeseries: TransferMartTimeseriesItem[];
  model_timeseries: ModelTimeseries[];
  error?: string;
}

export interface CompetitorModelProfile {
  make?: string;
  segment?: string;
  body_type?: string;
  powertrain?: string;
  registration_type?: string;
  drive_type?: string;
  origin?: string;
  length_mm?: number;
  msrp?: number;
  ev_range?: number;
  fuel_consumption?: number;
  co2_emission?: number;
  battery_kwh?: number;
}

export type CompetitorProductSpecKey =
  | "length_mm"
  | "msrp"
  | "ev_range"
  | "fuel_consumption"
  | "co2_emission"
  | "battery_kwh";

export type CompetitorProfileSpecs = Partial<Record<CompetitorProductSpecKey, number>>;

export interface CompetitorMatchEvidence {
  field: string;
  label: string;
  target: string | number;
  candidate: string | number;
  score: number;
  detail: string;
}

export type CompetitorRole = "target" | "likely_source" | "likely_recipient" | "co_winner" | "co_loser" | "adjacent";

export interface CompetitorModel {
  model: string;
  make: string;
  profile: CompetitorModelProfile;
  sales_tgt: number;
  sales_base: number;
  dV: number;
  share_tgt: number;
  share_base: number;
  share_change: number;
  pure_share_shift: number;
  similarity_score: number;
  shared_dims: string[];
  match_evidence: CompetitorMatchEvidence[];
  role: CompetitorRole;
  estimated_flow: number;
}

export interface CompetitorBattleFlow {
  source: string;
  target: string;
  value: number;
  similarity_score: number;
  reason: string;
}

export interface ModelChannelTimeseriesItem {
  model: string;
  period: string;
  channel: string;
  volume: number;
  total_volume: number;
  share: number;
}

export interface CompetitorSetResponse {
  base_period: string;
  target_period: string;
  sales_mode?: AdvancedAnalysisSalesMode;
  analysis_mode?: "profile" | "target_model";
  target_model: string;
  target: CompetitorModel;
  competitors: CompetitorModel[];
  battle_flows: CompetitorBattleFlow[];
  profile_dimensions: string[];
  model_options: string[];
  model_channel_timeseries: ModelChannelTimeseriesItem[];
  scope_model_count: number;
  error?: string;
}

export type AdvancedAnalysisProfileDimension =
  | "segment"
  | "body_type"
  | "powertrain"
  | "registration_type"
  | "drive_type"
  | "origin"
  | "make";

export type AdvancedAnalysisProfileOptions = Record<AdvancedAnalysisProfileDimension | "model", string[]>;

export interface AdvancedAnalysisProfileOptionsResponse {
  country?: string;
  options: AdvancedAnalysisProfileOptions;
}

export interface AdvancedAnalysisCountriesResponse {
  countries: string[];
}

// ── Drill-down types (legacy) ──

export interface DrilldownResponse {
  scope_path: Array<{ dim: string; value: string }>;
  scope_summary: ScopeSummary;
  drill_dim: string;
  available_dims: string[];
  is_model_level: boolean;
  cells?: DrilldownCell[];
  winners?: DrilldownModel[];
  losers?: DrilldownModel[];
  base_period: string;
  target_period: string;
  error?: string;
}
