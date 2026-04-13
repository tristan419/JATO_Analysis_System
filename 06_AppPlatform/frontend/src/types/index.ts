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

/* ---- Engineering Config ---- */
export interface ConfigProject {
  id: string;
  projectCode: string;
  brand: string;
  model: string;
  marketCountry: string;
  displayName: string;
  status: string;
  createdAt: string;
  updatedAt: string;
}

export interface ConfigImportBatch {
  id: string;
  projectId: string;
  importStatus: string;
  sourceFilePath: string;
  sheetName: string;
  sourceSchemaVersion: string | null;
  replaceMode: string;
  totalRows: number;
  importedRows: number;
  skippedRows: number;
  errorCount: number;
  notes: string | null;
  createdAt: string;
  finishedAt: string | null;
}

export interface ConfigVariant {
  id: string;
  projectId: string;
  configImportBatchId: string;
  model: string;
  trim: string;
  marketCountry: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
  metaJson: Record<string, unknown> | null;
}

/* ---- Review Cases ---- */
export interface ReviewCase {
  id: string;
  observationId: string;
  reviewStatus: string;
  country: string;
  brand: string;
  sourceCode?: string;
  sourceRegistryUrl?: string;
  sourceType?: string;
  extractorName?: string;
  extractorVersion?: string;
  jatoModel: string;
  jatoTrim: string;
  jatoPowertrain: string | null;
  officialModel: string;
  officialTrim: string;
  officialEdition: string | null;
  officialPowertrain: string | null;
  msrpValue?: number;
  currency?: string;
  sourceMsrpValue?: number;
  sourceCurrency?: string;
  fxRateToEur?: number;
  fxRateAsOfDate?: string;
  fxSource?: string;
  priceLabel?: string;
  observedAtUtc?: string;
  sourceUrl?: string;
  sourceSnapshotPath?: string | null;
  matchConfidence: number;
  matchReason?: Record<string, unknown> | null;
  currentAssignee: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface ReviewDecision {
  id: string;
  reviewCaseId: string;
  decision: string;
  decidedOfficialModel: string | null;
  decidedOfficialTrim: string | null;
  note: string | null;
  decidedBy: string;
  decidedAt: string;
}

export interface ReviewScopeCountrySummary {
  country: string;
  latestMonth: string;
  windowStartMonth: string;
  windowEndMonth: string;
  candidateCount: number;
  missingCount: number;
  topMissingBrands: string[];
  topMissingModels: string[];
}

export interface ReviewBacklogOpportunity {
  priorityRank: number;
  country: string;
  countryCode: string;
  brand: string;
  brandSlug: string;
  candidateModelCount: number;
  sales12mSum: number;
  topModels: string[];
  sourceCode: string;
  fileName: string;
  relativePath: string;
}

export interface ReviewWorkbench {
  candidateScopeAvailable: boolean;
  backlogAvailable: boolean;
  generatedAtUtc: string | null;
  reportTopN: number;
  countryCount: number;
  candidateCount: number;
  coverageSummary: {
    modelSource: number;
    brandSource: number;
    missingSource: number;
  };
  countryScope: ReviewScopeCountrySummary[];
  backlog: ReviewBacklogOpportunity[];
}

export interface ReviewCaseDetail extends ReviewCase {
  decisions: ReviewDecision[];
  observation: Record<string, unknown> | null;
  currentPrice: CurrentPrice | null;
}

/* ---- MSRP Current Prices ---- */
export interface CurrentPrice {
  id: string;
  country: string;
  brand: string;
  jatoModel: string;
  jatoTrim: string;
  jatoPowertrain: string | null;
  officialModel: string;
  officialTrim: string;
  officialEdition: string | null;
  officialPowertrain: string | null;
  effectiveObservationId: string;
  currentMsrpValue: number;
  currency: string;
  sourceMsrpValue?: number;
  sourceCurrency?: string;
  fxRateToEur?: number;
  fxRateAsOfDate?: string;
  fxSource?: string;
  taxIncluded: boolean;
  matchConfidence: number;
  matchStatus: string;
  sourceUrl: string;
  sourceSnapshotPath: string | null;
  lastPriceChangeAtUtc: string | null;
  updatedAtUtc: string;
  msrpValue?: number;
  observedAtUtc?: string;
  materializedAt?: string;
}

export interface PriceHistoryEntry {
  id: string;
  country: string;
  brand: string;
  jatoModel: string;
  jatoTrim: string;
  msrpValue: number;
  currency: string;
  sourceMsrpValue: number;
  sourceCurrency: string;
  validFromUtc: string;
  validToUtc: string | null;
  lastConfirmedAtUtc: string;
  startedByObservationId: string;
  endedByObservationId: string | null;
  lastConfirmedByObservationId: string;
  createdAtUtc: string;
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
