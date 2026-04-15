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

export interface DataFreshnessItem {
  country: string;
  latestMonth: string;
  monthsInWindow: number;
}

export interface CountryChatTurn {
  role: "user" | "assistant";
  content: string;
}

export interface CountryChatOption {
  value: string;
  label: string;
}

export interface CountryChatRankItem {
  label: string;
  value: number;
}

export interface CountryChatInsightCard {
  title: string;
  conclusion: string;
  tone: string;
  relatedChartLink: string;
}

export interface CountryChatMarketEvent {
  sourceCode?: string;
  countryCode?: string | null;
  countryLabel?: string;
  publisher?: string;
  title: string;
  summary?: string | null;
  url: string;
  publishedAt?: string | null;
  tags?: string[];
}

export interface CountryChatNewsDigest {
  countryCode?: string | null;
  countryLabel?: string;
  articleCount: number;
  updatedAt?: string | null;
  headline?: string;
  summary?: string;
  highlights?: string[];
  stale?: boolean;
  summaryProvider?: string | null;
  summaryModel?: string | null;
  syncTimestamp?: string | null;
}

export interface CountryChatChartLink {
  label: string;
  href: string;
}

export interface CountryChatProviderRole {
  capability: string;
  provider: string;
  model?: string | null;
  mode?: string | null;
}

export interface CountryChatNewsOpsStatus {
  country: string;
  countryCode?: string | null;
  countryLabel?: string;
  configured: boolean;
  feedCount: number;
  databaseEnabled: boolean;
  hasSnapshot: boolean;
  articleCount: number;
  syncTimestamp?: string | null;
  updatedAt?: string | null;
  summaryProvider?: string | null;
  summaryModel?: string | null;
  stale?: boolean | null;
  liveFetchDefaultEnabled: boolean;
  onlineRefreshSupported: boolean;
  geminiConfigured: boolean;
  geminiModel?: string | null;
  chatProvider?: {
    provider?: string | null;
    available?: boolean;
    reason?: string | null;
    model?: string | null;
  };
  providerRoles?: CountryChatProviderRole[];
}

export interface CountryChatNewsRefreshResponse {
  payload: {
    countryCode?: string | null;
    countryLabel?: string;
    marketEvents: CountryChatMarketEvent[];
    newsDigest: CountryChatNewsDigest | null;
  };
  status: CountryChatNewsOpsStatus;
}

export interface CountryChatAnalysisMeta {
  availableYears?: Array<number | string>;
  selectedYear?: number | null;
  selectedMonth?: number | null;
  yearLockedByQuestion?: boolean;
  defaultLatestYearApplied?: boolean;
  availableModels?: string[];
  selectedModel?: string | null;
  modelTopN?: number;
}

export interface CountryChatSnapshot {
  country: string;
  route: string;
  kpis: OverviewResponse["kpis"];
  yearSeries: TimeSeriesPoint[];
  monthSeries: TimeSeriesPoint[];
  topBrands: CountryChatRankItem[];
  topModels: CountryChatRankItem[];
  powertrainMix: CountryChatRankItem[];
  marketEvents?: CountryChatMarketEvent[];
  newsDigest?: CountryChatNewsDigest | null;
  insightCards?: CountryChatInsightCard[];
  periodLabel?: string;
  resolvedPeriod?: string;
  overviewSummary?: Record<string, unknown>;
  ytdBrandRanking?: MarketScanRankingGroup | Record<string, unknown>[];
  monthlyBrandRanking?: MarketScanRankingGroup | Record<string, unknown>[];
  originAnalysis?: {
    summaryText?: string;
    matrix?: MarketScanMatrix;
    [key: string]: unknown;
  };
  segmentMatrix?: MarketScanMatrix;
  suvSedanTrend?: { items?: MarketScanBodyShareTrendItem[] } | Record<string, unknown>[];
  drilldown?: Record<string, unknown>;
  suvA?: Record<string, unknown>;
  positioningMap?: PositioningMapResponse;
  priceDistribution?: Record<string, unknown>[];
  nevRangeDistribution?: Record<string, unknown>[];
  bevShareBySegment?: Record<string, unknown>[];
  powertrainVsPrice?: Record<string, unknown>[];
  segmentShareByLength?: Record<string, unknown>[];
  powertrainBubble?: Record<string, unknown>[];
  modelVersionBubble?: ModelVersionItem[];
  priceMigration?: Record<string, unknown>[];
  pricePerMeter?: Record<string, unknown>[];
  salesVsPrice?: Record<string, unknown>[];
  nevCapacityVsMsrp?: Record<string, unknown>[];
  seasonalityHeatmap?: Record<string, unknown>[];
  estimatedTco?: Record<string, unknown>[];
  analysisMeta?: CountryChatAnalysisMeta;
  [key: string]: unknown;
}

export interface CountryChatMetadataResponse {
  availableCountries: CountryChatOption[];
  provider: string;
  providerAvailable: boolean;
  providerReason?: string | null;
  defaultModel?: string | null;
  suggestedPrompts: string[];
}

export interface CountryChatResponse {
  country: string;
  question: string;
  answer: string;
  intent: string;
  primaryIntent?: string;
  intents?: string[];
  provider: string;
  providerAvailable: boolean;
  providerReason?: string | null;
  contextSnapshot: CountryChatSnapshot;
  suggestedPrompts: string[];
  chartLinks?: CountryChatChartLink[];
  extractedParams?: Record<string, unknown> | null;
}

export interface CountryChatDeckResponse {
  country: string;
  question: string;
  primaryIntent?: string;
  intents?: string[];
  deckIntents?: string[];
  contextSnapshot: CountryChatSnapshot;
  controls?: CountryChatAnalysisMeta;
  extractedParams?: Record<string, unknown> | null;
}

export interface MarketScanDelta {
  value: number | null;
  display: string;
  tone: string;
}

export interface MarketScanCountryOption {
  value: string;
  label: string;
}

export interface MarketScanMetadataLabels {
  pageTitle: string;
  currentMonthShort: string;
  previousMonthShort: string;
  sameMonthLastYearShort: string;
  currentYtd: string;
  priorYtd: string;
  ytdWindow: string;
}

export interface MarketScanMetadata {
  protocolVersion: string;
  requestedPeriod: string | null;
  resolvedPeriod: string;
  latestPeriod: string;
  priorPeriod: string | null;
  sameMonthLastYearPeriod: string | null;
  selectedCountry: string;
  selectedCountryLabel: string;
  selectedFuelTypes: string[];
  selectedDrilldownSegment: string;
  availableCountries: MarketScanCountryOption[];
  availablePeriods: MarketScanCountryOption[];
  availableFuelTypes: string[];
  availableSegments: MarketScanCountryOption[];
  labels: MarketScanMetadataLabels;
}

export interface MarketScanMetricCell {
  key: string;
  value: number | null;
  display: string;
  tone: string;
}

export interface MarketScanMatrixRow {
  metricKey: string;
  label: string;
  cells: MarketScanMetricCell[];
}

export interface MarketScanMatrix {
  columns: string[];
  rows: MarketScanMatrixRow[];
}

export interface MarketScanOverviewSummary {
  headline: string;
  subheadline: string;
  currentMonthVolume: number;
  currentMonthYoY: MarketScanDelta;
  ytdVolume: number;
  ytdYoY: MarketScanDelta;
}

export interface MarketScanOverviewTrendItem {
  period: string;
  label: string;
  totalVolume: number;
  fuelMix: Record<string, number>;
  mom: MarketScanDelta;
  yoy: MarketScanDelta;
}

export interface MarketScanRankingItem {
  rank: number;
  brand?: string;
  model?: string;
  volume: number;
  sharePct: number;
  shareDisplay?: string;
  driveSharePct?: number;
  driveShareDisplay?: string;
  priorVolume?: number;
  priorMonthVolume?: number;
  yoy: MarketScanDelta;
  mom?: MarketScanDelta;
  barPct: number;
  fuelMix?: Record<string, number>;
  driveMix?: Record<string, number>;
}

export interface MarketScanRankingGroup {
  title: string;
  currentLabel?: string;
  priorLabel?: string;
  previousMonthLabel?: string;
  items: MarketScanRankingItem[];
}

export interface MarketScanOverviewPage {
  summary: MarketScanOverviewSummary;
  trend: {
    periods: string[];
    items: MarketScanOverviewTrendItem[];
  };
  ytdBrandRanking: MarketScanRankingGroup;
  monthlyBrandRanking: MarketScanRankingGroup;
}

export interface MarketScanOriginTrendPoint {
  period: string;
  label: string;
  volume: number;
  sharePct: number;
}

export interface MarketScanOriginSeries {
  origin: string;
  points: MarketScanOriginTrendPoint[];
}

export interface MarketScanOriginPage {
  summaryText: string;
  trend: {
    series: MarketScanOriginSeries[];
  };
  matrix: MarketScanMatrix;
}

export interface MarketScanBodyShareTrendItem {
  period: string;
  label: string;
  totalVolume: number;
  suvSharePct: number;
  sedanSharePct: number;
}

export interface MarketScanSegmentPage {
  summaryText: string;
  matrix: MarketScanMatrix;
  bodyShareTrend: {
    items: MarketScanBodyShareTrendItem[];
  };
}

export interface MarketScanFuelTrendItem {
  label: string;
  totalVolume: number;
  fuelMix: Record<string, number>;
}

export interface MarketScanFuelPanel {
  fuelType: string;
  monthTitle: string;
  ytdTitle: string;
  monthRanking: MarketScanRankingItem[];
  ytdRanking: MarketScanRankingItem[];
}

export interface MarketScanDrilldownPage {
  segment: string;
  segmentLabel: string;
  title: string;
  summaryText: string;
  totalRanking: {
    title: string;
    items: MarketScanRankingItem[];
  };
  ytdFuelTrend: {
    items: MarketScanFuelTrendItem[];
  };
  fuelPanels: MarketScanFuelPanel[];
}

export interface MarketScanDeckResults {
  overview: MarketScanOverviewPage;
  origin: MarketScanOriginPage;
  segment: MarketScanSegmentPage;
  drilldown: MarketScanDrilldownPage;
  suvA: MarketScanDrilldownPage;
  suvB: MarketScanDrilldownPage;
}

export type MarketScanPageKey = keyof MarketScanDeckResults;

export interface MarketScanDeckResponse {
  metadata: MarketScanMetadata;
  results: MarketScanDeckResults;
}

export interface MarketScanDeckRequest {
  country?: string | null;
  target_period?: string | null;
  fuel_types?: string[];
  trend_window_months?: number;
  origin_window_months?: number;
  body_window_months?: number;
  ranking_limit?: number;
  drilldown_segment?: string | null;
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
