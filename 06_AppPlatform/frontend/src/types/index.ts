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
  extracted_params?: Record<string, unknown>;
  intent_route?: string;
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

export interface CountryChatModelOption {
  id: string;
  provider: string;
  model?: string | null;
  label: string;
  description?: string | null;
  available: boolean;
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

export interface CountryChatModelUsage {
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  promptCacheHitTokens?: number;
  promptCacheMissTokens?: number;
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
  modelUsage?: CountryChatModelUsage;
}

export interface CountryChatRenderHint {
  kind: string;
  title: string;
  intent?: string;
}

export interface CountryChatMarketScanScope {
  pageKey: "drilldown" | "suvA" | "suvB" | string;
  resolvedSegment?: string | null;
  resolvedSegmentLabel?: string | null;
  summaryText?: string;
  totalRanking: MarketScanRankingItem[];
  focusModel?: string | null;
  focusModelRank?: number | null;
  focusModelItem?: MarketScanRankingItem | null;
  resolvedPeriod?: string | null;
  selectedFuelTypes?: string[];
  modelPerformance?: CountryChatModelPerformanceFocus | null;
}

export interface CountryChatShareMixItem {
  label: string;
  value: number;
  sharePct: number;
}

export interface CountryChatMarketSignal {
  title: string;
  summary?: string | null;
  publisher?: string | null;
  publishedAt?: string | null;
  reason?: string | null;
}

export interface CountryChatModelPerformanceFocus {
  model: string;
  rank?: number | null;
  volume?: number | null;
  shareDisplay?: string | null;
  yoyDisplay?: string | null;
  leaderModel?: string | null;
  leaderShareDisplay?: string | null;
  leaderVolumeGap?: number | null;
  channelMix?: CountryChatShareMixItem[];
  driveMix?: CountryChatShareMixItem[];
  awdSharePct?: number | null;
  awdShareDisplay?: string | null;
  bodyStyleDistribution?: CountryChatShareMixItem[];
  versionAxis?: "trim" | "version" | string;
  versionDistribution?: CountryChatShareMixItem[];
  bodyStyleNote?: string | null;
  newsSignals?: CountryChatMarketSignal[];
}

export interface CountryChatGroundingLayer {
  kind: string;
  label: string;
  detail: string;
  freshness?: string | null;
}

export interface CountryChatEvidenceTable {
  title: string;
  columns: string[];
  rows: string[][];
}

export interface CountryChatSourceCoverage {
  requiredReady: number;
  requiredTotal: number;
  prefetchedCount: number;
}

export interface CountryChatTrustAssessment {
  confidence: string;
  evidenceSufficiency: string;
  evidenceScore: number;
  routeRationale: string;
  missingFacts: string[];
  sourceCoverage?: CountryChatSourceCoverage;
}

export interface CountryChatGroundingAnswerPath {
  routeTrigger?: string;
  evidenceUsed?: string[];
  steps: string[];
}

export interface CountryChatExecutionPlanSource {
  key: string;
  label?: string;
  required?: boolean;
  status?: string;
  reason?: string;
  toolName?: string;
  query?: Record<string, unknown>;
}

export interface CountryChatExecutionPlanTool {
  name: string;
  arguments?: Record<string, unknown>;
}

export interface CountryChatExecutionPlan {
  route?: string;
  country?: string;
  answerStrategy?: string;
  orchestrationMode?: string;
  sourcePlan?: CountryChatExecutionPlanSource[];
  allowedToolNames?: string[];
  prefetchedToolNames?: string[];
  prefetchTools?: CountryChatExecutionPlanTool[];
}

export interface CountryChatGrounding {
  strategyLabel: string;
  summary: string;
  answerPath?: CountryChatGroundingAnswerPath;
  reasoningNotes?: string[];
  layers: CountryChatGroundingLayer[];
  keyFindings: string[];
  evidenceTables: CountryChatEvidenceTable[];
  trust?: CountryChatTrustAssessment;
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
  marketScanScope?: CountryChatMarketScanScope;
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
  defaultChatModel?: string | null;
  availableChatModels: CountryChatModelOption[];
  suggestedPrompts: string[];
}

export interface CountryChatResponse {
  country: string;
  question: string;
  answer: string;
  intent: string;
  primaryIntent?: string;
  intents?: string[];
  focusedIntents?: string[];
  intentRoute?: string;
  provider: string;
  model?: string | null;
  chatModelId?: string | null;
  providerAvailable: boolean;
  providerReason?: string | null;
  answerMode?: string | null;
  grounding?: CountryChatGrounding | null;
  contextSnapshot: CountryChatSnapshot;
  suggestedPrompts: string[];
  chartLinks?: CountryChatChartLink[];
  renderHints?: CountryChatRenderHint[];
  extractedParams?: Record<string, unknown> | null;
  executionPlan?: CountryChatExecutionPlan | null;
}

export interface CountryChatDeckResponse {
  country: string;
  question: string;
  primaryIntent?: string;
  intents?: string[];
  deckIntents?: string[];
  intentRoute?: string;
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
  selectedTimeRange?: MarketScanPeriodRange | null;
  customRangeActive?: boolean;
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
  rolling12Volume: number;
  rolling12YoY: MarketScanDelta;
  ytdVolume: number;
  ytdYoY: MarketScanDelta;
  customRangeVolume?: number;
  customRangeYoY?: MarketScanDelta;
  customRangeLabel?: string;
}

export interface MarketScanOverviewTrendItem {
  period: string;
  label: string;
  totalVolume: number;
  fuelMix: Record<string, number>;
  suvTotalVolume?: number;
  suvFuelMix?: Record<string, number>;
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
  registrationMix?: Record<string, number>;
  modelBreakdown?: Array<{
    model: string;
    volume: number;
    sharePct: number;
    powertrain?: string;
  }>;
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
  rolling12BrandRanking: MarketScanRankingGroup;
  monthlyBrandRanking: MarketScanRankingGroup;
  customRangeBrandRanking?: MarketScanRankingGroup;
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

export interface MarketScanOriginBrandSeries {
  brand: string;
  points: MarketScanOriginTrendPoint[];
}

export interface MarketScanOriginBrandGroup {
  origin: string;
  series: MarketScanOriginBrandSeries[];
}

export interface MarketScanOriginPage {
  summaryText: string;
  trend: {
    series: MarketScanOriginSeries[];
  };
  brandTrend: {
    groups: MarketScanOriginBrandGroup[];
  };
  matrix: MarketScanMatrix;
  customRangeMatrixRow?: MarketScanMatrixRow | null;
  customRangeYoYMatrixRow?: MarketScanMatrixRow | null;
}

export interface MarketScanBodyShareTrendItem {
  period: string;
  label: string;
  totalVolume: number;
  suvSharePct: number;
  sedanSharePct: number;
}

export interface MarketScanSuvSegmentShareTrendItem {
  period: string;
  label: string;
  totalVolume: number;
  segmentSharePct: Record<string, number>;
}

export interface MarketScanChannelMixItem {
  label: string;
  volume: number;
  channelMix: Record<string, number>;
  channelSharePct: Record<string, number>;
}

export interface MarketScanChannelMixWindow {
  title: string;
  items: MarketScanChannelMixItem[];
  defaultView?: string;
  views?: Record<string, {
    title: string;
    items: MarketScanChannelMixItem[];
  }>;
}

export interface MarketScanChannelMixOption {
  value: string;
  label: string;
}

export interface MarketScanSegmentPage {
  summaryText: string;
  matrix: MarketScanMatrix;
  bodyShareTrend: {
    items: MarketScanBodyShareTrendItem[];
  };
  suvSegmentShareTrend: {
    items: MarketScanSuvSegmentShareTrendItem[];
  };
  channelMix?: {
    options?: MarketScanChannelMixOption[];
    month: MarketScanChannelMixWindow;
    ytd: MarketScanChannelMixWindow;
    rolling12: MarketScanChannelMixWindow;
    customRange?: MarketScanChannelMixWindow | null;
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
  rolling12Title: string;
  customRangeTitle?: string | null;
  monthRanking: MarketScanRankingItem[];
  ytdRanking: MarketScanRankingItem[];
  rolling12Ranking: MarketScanRankingItem[];
  customRangeRanking?: MarketScanRankingItem[];
}

export interface MarketScanDrilldownPage {
  segment: string;
  segmentLabel: string;
  title: string;
  summaryText: string;
  monthTotalRanking: {
    title: string;
    items: MarketScanRankingItem[];
  };
  totalRanking: {
    title: string;
    items: MarketScanRankingItem[];
  };
  rolling12TotalRanking: {
    title: string;
    items: MarketScanRankingItem[];
  };
  customRangeTotalRanking?: {
    title: string;
    items: MarketScanRankingItem[];
  } | null;
  monthFuelTrend: {
    items: MarketScanFuelTrendItem[];
  };
  ytdFuelTrend: {
    items: MarketScanFuelTrendItem[];
  };
  rolling12FuelTrend: {
    items: MarketScanFuelTrendItem[];
  };
  customRangeFuelTrend?: {
    items: MarketScanFuelTrendItem[];
  } | null;
  fuelPanels: MarketScanFuelPanel[];
  originFuelPanels?: MarketScanOriginFuelPanelGroup[];
}

export interface MarketScanOriginFuelPanelGroup {
  origin: string;
  originLabel: string;
  fuelPanels: MarketScanFuelPanel[];
}

export interface MarketScanDeckResults {
  overview: MarketScanOverviewPage;
  origin: MarketScanOriginPage;
  segment: MarketScanSegmentPage;
  drilldown: MarketScanDrilldownPage;
  suvAll: MarketScanDrilldownPage;
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
  time_range?: MarketScanPeriodRange | null;
  fuel_types?: string[];
  trend_window_months?: number;
  origin_window_months?: number;
  body_window_months?: number;
  ranking_limit?: number;
  drilldown_segment?: string | null;
  view?: string | null;
}

export interface MarketScanPeriodRange {
  start: string;
  end: string;
}

export interface PositioningPricingMetric {
  label: string;
  value: number | string;
  detail: string;
}

export interface PositioningPricingPriceBandItem {
  bandStart: number;
  bandEnd: number;
  bandMid: number;
  bandWidth: number;
  label: string;
  sales: number;
  fuelMix: Record<string, number>;
}

export interface PositioningPricingBubbleItem {
  brand: string;
  model: string;
  powertrain: string;
  segment: string;
  length: number;
  msrp: number;
  msrpMin: number;
  msrpMax: number;
  sales: number;
  variantCount: number;
}

export interface PositioningPricingPage {
  key: PositioningPricingPageKey;
  title: string;
  subtitle: string;
  summaryText: string;
  metrics: PositioningPricingMetric[];
  lengthRange: {
    min: number;
    max: number;
  };
  priceBands: {
    bandSize: number;
    range: {
      min: number;
      max: number;
    };
    items: PositioningPricingPriceBandItem[];
  };
  bubbleChart: {
    items: PositioningPricingBubbleItem[];
    bubbleLimit: number;
  };
}

export interface PositioningPricingPriceOverlay {
  sourceMode?: string | null;
  mode?: string | null;
  candidateRows?: number;
  linkCandidateRows?: number;
  reason?: string | null;
  matchedRows?: number;
  matchedModels?: number;
  linkMatches?: number;
  directMatches?: number;
}

export interface PositioningPricingMetadata {
  protocolVersion: string;
  requestedPeriod: string | null;
  resolvedPeriod: string;
  latestPeriod: string;
  selectedCountry: string;
  selectedCountryLabel: string;
  selectedFuelTypes: string[];
  selectedSalesMode: PositioningPricingSalesMode;
  selectedTimeRange?: MarketScanPeriodRange | null;
  customRangeActive?: boolean;
  selectedTopN: number;
  availableSalesModes: MarketScanCountryOption[];
  availableCountries: MarketScanCountryOption[];
  availablePeriods: MarketScanCountryOption[];
  availableFuelTypes: string[];
  labels: {
    pageTitle: string;
    currentMonthShort: string;
    salesModeLabel: string;
  };
  priceOverlay?: PositioningPricingPriceOverlay | null;
}

export interface PositioningPricingDeckResponse {
  metadata: PositioningPricingMetadata;
  pages: Record<PositioningPricingPageKey, PositioningPricingPage>;
}

export interface PositioningPricingDeckRequest {
  country?: string | null;
  target_period?: string | null;
  time_range?: MarketScanPeriodRange | null;
  fuel_types?: string[];
  sales_mode?: PositioningPricingSalesMode;
  top_n?: number;
  msrp_min?: number | null;
  msrp_max?: number | null;
  length_min?: number | null;
  length_max?: number | null;
  price_band_size?: number | null;
}

export type PositioningPricingSalesMode = "month" | "ytd" | "rolling12";
export type PositioningPricingPageKey = "overview" | "suvAll" | "suvA0" | "suvA" | "suvBPlus";

export interface VersionComparisonBubbleItem {
  model: string;
  version: string;
  trim: string;
  powertrain: string;
  length: number;
  msrp: number;
  msrpMin: number;
  msrpMax: number;
  sales: number;
  variantCount: number;
  segment?: string;
  bodyType?: string;
  driveType?: string;
}

export interface VersionComparisonModelOption {
  value: string;
  label: string;
  segment: string;
  powertrain: string;
  bodyType: string;
  driveType: string;
  lengthMm: number;
  msrpMedian: number;
}

export type VersionComparisonMode = "same_segment" | "free_comparison";

export interface VersionComparisonPage {
  title: string;
  subtitle: string;
  summaryText: string;
  metrics: PositioningPricingMetric[];
  priceBands: {
    bandSize: number;
    range: {
      min: number;
      max: number;
    };
    items: PositioningPricingPriceBandItem[];
  };
  bubbleChart: {
    items: VersionComparisonBubbleItem[];
  };
}

export interface VersionComparisonMetadata {
  protocolVersion: string;
  requestedPeriod: string | null;
  resolvedPeriod: string;
  latestPeriod: string;
  selectedCountry: string;
  selectedCountryLabel: string;
  selectedFuelTypes: string[];
  selectedSalesMode: PositioningPricingSalesMode;
  selectedTimeRange?: MarketScanPeriodRange | null;
  customRangeActive?: boolean;
  comparisonMode: VersionComparisonMode;
  selectedSegment: string;
  selectedModels: string[];
  isMixedSegment: boolean;
  availableSalesModes: MarketScanCountryOption[];
  availableCountries: MarketScanCountryOption[];
  availablePeriods: MarketScanCountryOption[];
  availableFuelTypes: string[];
  availableSegments: MarketScanCountryOption[];
  availableModels: VersionComparisonModelOption[];
  availableBodyTypes: string[];
  availableDriveTypes: string[];
  suggestedLengthMin: number | null;
  suggestedLengthMax: number | null;
  labels: {
    pageTitle: string;
    currentMonthShort: string;
    salesModeLabel: string;
  };
}

export interface VersionComparisonDeckResponse {
  metadata: VersionComparisonMetadata;
  page: VersionComparisonPage;
}

export interface RankingTrendPoint {
  month: string;
  sales: number;
  ytdSales: number;
  marketShare: number;
  msrpMin: number;
  msrpMax: number;
  msrpAvg: number;
}

export interface RankingTrendResponse {
  entityType: "brand" | "model";
  brand: string;
  model?: string | null;
  context: {
    country: string;
    segment?: string | null;
    sourceTable: string;
    filtersApplied: boolean;
  };
  summary: {
    currentMonthSales: number;
    ytdSales: number;
    marketShare: number;
    rankChange: number;
  };
  trend: RankingTrendPoint[];
  topModels?: { model: string; sales: number; shareWithinBrand: number; growth?: number }[];
  models?: { model: string; sales: number; shareWithinBrand: number; growth?: number }[];
}

export interface VersionComparisonDeckRequest {
  country?: string | null;
  target_period?: string | null;
  time_range?: MarketScanPeriodRange | null;
  fuel_types?: string[];
  sales_mode?: PositioningPricingSalesMode;
  comparison_mode?: VersionComparisonMode;
  segment?: string | null;
  models?: string[];
  msrp_min?: number | null;
  msrp_max?: number | null;
  price_band_size?: number | null;
  body_type?: string | null;
  drive_types?: string[];
  segments?: string[];
  length_min?: number | null;
  length_max?: number | null;
}

export interface CustomerInsightShareItem {
  label: string;
  rawLabel: string;
  value: number;
  sharePct: number;
  mentionCount?: number;
}

export type CustomerInsightMode = "benchmark" | "forum_live";

export interface CustomerInsightPersonaFact {
  label: string;
  value: string;
}

export interface CustomerInsightPersona {
  title: string;
  summary: string;
  facts: CustomerInsightPersonaFact[];
  notes: string[];
}

export interface CustomerInsightConclusionCard {
  label: string;
  headline: string;
  detail: string;
}

export interface CustomerInsightEvidenceCard {
  title: string;
  url: string;
  siteName: string;
  siteType: string;
  sourceCode?: string;
  countryCode?: string;
  countryLabel?: string;
  language?: string;
  publishedAt?: string | null;
  collectedAt?: string | null;
  publishTier: string;
  publishDecision?: string;
  sentiment: string;
  qualityScore?: number;
  observationCount?: number;
  signals: string[];
  evidenceSnippets: string[];
  excerpt?: string;
  contentPreview?: string;
  contentTruncated?: boolean;
  observations: CustomerInsightEvidenceObservation[];
}

export interface CustomerInsightEvidenceObservation {
  signalKind: string;
  label: string;
  sentence: string;
  matchedTokens: string[];
  sentiment: string;
}

export interface CustomerInsightForumLiveSection {
  sourceMix: CustomerInsightShareItem[];
  siteTypes: CustomerInsightShareItem[];
  languages: CustomerInsightShareItem[];
  publishTiers: CustomerInsightShareItem[];
  sentiment: CustomerInsightShareItem[];
  ownershipStages: CustomerInsightShareItem[];
  painPoints: CustomerInsightShareItem[];
  productSignals: CustomerInsightShareItem[];
  powertrains: CustomerInsightShareItem[];
  decisionFactors: CustomerInsightShareItem[];
  evidenceCards: CustomerInsightEvidenceCard[];
  observedSections: string[];
  inferredSections: string[];
}

export interface CustomerInsightPage {
  title: string;
  subtitle: string;
  summaryText: string;
  methodologyNote: string;
  conclusionCards: CustomerInsightConclusionCard[];
  metrics: PositioningPricingMetric[];
  profile: {
    sampleSources: CustomerInsightShareItem[];
    attentionChannels: CustomerInsightShareItem[];
    gender: CustomerInsightShareItem[];
    age: CustomerInsightShareItem[];
    household: CustomerInsightShareItem[];
    weeklyCommute: CustomerInsightShareItem[];
  };
  occupation: {
    items: CustomerInsightShareItem[];
  };
  lifestyle: {
    items: CustomerInsightShareItem[];
  };
  powertrain: {
    items: CustomerInsightShareItem[];
  };
  philosophy: {
    items: CustomerInsightShareItem[];
  };
  purchaseUses: {
    items: CustomerInsightShareItem[];
  };
  decisionFactors: {
    items: CustomerInsightShareItem[];
  };
  persona: CustomerInsightPersona;
  forumLive?: CustomerInsightForumLiveSection;
}

export interface CustomerInsightMetadata {
  protocolVersion: string;
  datasetLabel: string;
  sourceFile: string;
  respondentCount: number;
  updatedAt: number;
  mode: CustomerInsightMode;
  modeLabel: string;
  sourceKind: string;
  sampleUnitLabel: string;
  coverageLabel: string;
  countryCodes: string[];
}

export interface CustomerInsightDeckResponse {
  metadata: CustomerInsightMetadata;
  page: CustomerInsightPage;
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

export interface PositioningPeerCorridor {
  peerCount: number;
  salesTotal: number;
  lengthMin: number;
  lengthMax: number;
  msrpP25: number;
  msrpMedian: number;
  msrpP75: number;
  pricePerMeterMedian?: number | null;
  targetLength?: number | null;
  targetMsrp?: number | null;
  targetPricePerMeter?: number | null;
  targetResidual?: number | null;
  targetResidualPct?: number | null;
  targetPricePerMeterResidualPct?: number | null;
  positionLabel?: string | null;
  stanceCode?: string | null;
  stanceLabel?: string | null;
  stanceDetail?: string | null;
  salesWeighted?: boolean;
}

export interface PositioningMapResponse {
  rows: number;
  items: PositioningMapItem[];
  target: { Length: number; MSRP: number } | null;
  cluster_top3: string[];
  peerCorridor?: PositioningPeerCorridor | null;
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

export interface MsrpSource {
  id: string;
  sourceCode: string;
  country: string;
  brand: string;
  sourceUrl: string;
  sourceType: string;
  tier: number;
  extractorName: string;
  extractorVersion: string;
  priceSemantics: string;
  requiresLocation: boolean;
  enabled: boolean;
  notes: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface MatchOverride {
  id: string;
  country: string;
  brand: string;
  jatoModel: string;
  jatoTrim: string;
  jatoPowertrain: string | null;
  officialModel: string;
  officialTrim: string;
  validFromDate: string;
  validToDate: string | null;
  overrideReason: string;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

export interface ReviewCandidateMatch {
  currentPriceId?: string;
  jatoModel: string;
  jatoTrim: string;
  jatoPowertrain: string | null;
  officialModel: string;
  officialTrim: string;
  officialEdition: string | null;
  officialPowertrain: string | null;
  currentMsrpValue?: number;
  currency?: string;
  score: number;
  reason?: Record<string, unknown> | null;
}

export interface MsrpObservationRecord {
  observationId: string;
  scrapeBatchId: string;
  sourceId: string;
  sourceCode?: string;
  sourceType?: string;
  extractorName?: string;
  extractorVersion?: string;
  country: string;
  brand: string;
  jatoModel: string;
  jatoTrim: string;
  jatoPowertrain: string | null;
  officialModel: string;
  officialTrim: string;
  officialEdition: string | null;
  officialPowertrain: string | null;
  msrpValue: number;
  currency: string;
  sourceMsrpValue: number;
  sourceCurrency: string;
  fxRateToEur: number;
  fxRateAsOfDate: string;
  fxSource: string;
  taxIncluded: boolean;
  priceLabel: string;
  availabilityText: string | null;
  observedAtUtc: string;
  sourceUrl: string;
  sourceSnapshotPath: string | null;
  sourcePayloadHash: string | null;
  extractionVersion: string;
  matchConfidence: number;
  matchStatus: string;
  matchReason: Record<string, unknown> | null;
  sourceContext: Record<string, unknown> | null;
  createdAtUtc: string;
  updatedAtUtc: string;
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
  candidateMatches: ReviewCandidateMatch[] | null;
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
  observation: MsrpObservationRecord | null;
  currentPrice: CurrentPrice | null;
}

/* ---- MSRP Current Prices ---- */
export interface CurrentPrice {
  id: string;
  country: string;
  brand: string;
  sourceCode?: string;
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
  jatoPowertrain: string | null;
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

export interface JatoMonthlyUpdateUpload {
  originalFilename: string;
  storedPath?: string | null;
  sizeBytes?: number;
  sha256?: string | null;
}

export interface JatoMonthlyUpdatePlan {
  path?: string | null;
  batchId?: string | null;
  compareId?: string | null;
  compareCommand?: string | null;
  refreshCommand?: string | null;
}

export interface JatoMonthlyUpdateArtifacts {
  jobDir?: string | null;
  logPath?: string | null;
  baselinePath?: string | null;
  stagedPatchPath?: string | null;
  supplementParquetPath?: string | null;
  planPath?: string | null;
  reviewDir?: string | null;
  rawCompareReportPath?: string | null;
  stagingOutputPath?: string | null;
  manifestPath?: string | null;
  partitionOutputPath?: string | null;
  refreshReportPath?: string | null;
  fingerprintPath?: string | null;
}

export interface JatoMonthlyUpdatePublication {
  publishedAt?: string | null;
  publishedBy?: string | null;
  backupDir?: string | null;
  activeParquetPath?: string | null;
  activeManifestPath?: string | null;
  activePartitionPath?: string | null;
  activeFingerprintPath?: string | null;
  activeRefreshReportPath?: string | null;
  rolledBackAt?: string | null;
  rolledBackBy?: string | null;
  rollbackBackupDir?: string | null;
}

export interface JatoMonthlyUpdateRawCompareSummary {
  compareId: string;
  decisionSuggestion: string;
  compareKeyMode: string;
  compareKeyColumns: string[];
  blockerCount: number;
  reviewCount: number;
  infoCount: number;
  advancedCountryCount: number;
  regressedCountryCount: number;
  newCountryCount: number;
  missingCountryCount: number;
  addedCountryCount: number;
  removedCountryCount: number;
}

export interface JatoMonthlyUpdateRefreshSummary {
  jobStatus: string;
  jobElapsedSeconds: number;
  rowCount: number;
  columnCount: number;
  partitionCount: number;
  changedRows: number;
  changedCountryCount: number;
  fingerprintMatched: boolean;
  fingerprintUpdated: boolean;
  conflictGroupCount: number;
  conflictRowCount: number;
}

export interface JatoMonthlyUpdateSmartMergeSummary {
  mergedAt: string;
  regressedCountryCount: number;
  regressedCountries: string[];
  totalRowCount: number;
}

export interface JatoMonthlyUpdateSummaries {
  rawCompare?: JatoMonthlyUpdateRawCompareSummary;
  refresh?: JatoMonthlyUpdateRefreshSummary;
  smartMerge?: JatoMonthlyUpdateSmartMergeSummary;
}

export interface JatoMonthlyUpdateCurrentProcess {
  pid: number;
  label: string;
  command: string;
  startedAt: string;
  lastHeartbeatAt: string;
}

export interface JatoMonthlyUpdateRuntimeCheck {
  checkedAt: string;
  statusAtCheck?: string;
  phaseAtCheck?: string;
  threadAlive?: boolean;
  processPid?: number | null;
  processAlive?: boolean;
  log?: Record<string, unknown>;
  artifacts?: Array<Record<string, unknown>>;
  resolvedAs?: string;
  resolvedBy?: string;
  resolvedAt?: string;
}

export interface JatoMonthlyUpdateCancellation {
  cancelledAt: string;
  cancelledBy: string;
  phaseAtCancel: string;
  termination?: Record<string, unknown>;
}

export interface JatoMonthlyUpdateJob {
  jobId: string;
  month: string;
  batchId?: string | null;
  status: string;
  phase: string;
  triggeredBy: string;
  createdAt: string;
  updatedAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  error: string | null;
  upload: JatoMonthlyUpdateUpload | null;
  plan: JatoMonthlyUpdatePlan | null;
  artifacts: JatoMonthlyUpdateArtifacts | null;
  summaries: JatoMonthlyUpdateSummaries | null;
  publication?: JatoMonthlyUpdatePublication | null;
  currentProcess?: JatoMonthlyUpdateCurrentProcess | null;
  runtimeCheck?: JatoMonthlyUpdateRuntimeCheck | null;
  cancellation?: JatoMonthlyUpdateCancellation | null;
  logPath?: string | null;
  logTail?: string | null;
}

export interface JatoMonthlyUpdateReviewFinding {
  severity: string;
  scope: string;
  target: string;
  ruleId: string;
  message: string;
  metrics: Record<string, unknown>;
  suggestedAction: string;
}

export interface JatoMonthlyUpdateConflictSample {
  country: string;
  businessKey: Record<string, unknown>;
  oldValueDigest?: string | null;
  newValueDigest?: string | null;
  changedFields: string[];
}

export interface JatoMonthlyUpdateOverlapChangeSummary {
  country: string;
  compareMonths: string[];
  compareKeyColumns: string[];
  addedRecordCount: number;
  removedRecordCount: number;
  changedRecordCount: number;
  unchangedRecordCount: number;
  changeRate: number;
  sampleAddedKeys: Record<string, unknown>[];
  sampleRemovedKeys: Record<string, unknown>[];
  sampleChangedKeys: Record<string, unknown>[];
}

export interface JatoMonthlyUpdateCountryFreshnessSummary {
  country: string;
  oldLatestMonth?: string | null;
  newLatestMonth?: string | null;
  freshnessStatus: string;
  rowDelta: number;
}

export interface JatoMonthlyUpdateCountryCoverageSummary {
  country: string;
  oldMonths: string[];
  newMonths: string[];
  addedMonths: string[];
  removedMonths: string[];
  overlappingMonths: string[];
  coverageStatus: string;
}

export interface JatoMonthlyUpdateCountryMonthlySalesRow {
  month: string;
  referenceSales?: number | null;
  candidateSales?: number | null;
  deltaSales?: number | null;
  changeStatus: string;
}

export interface JatoMonthlyUpdateCountryMonthlySalesSummary {
  country: string;
  rows: JatoMonthlyUpdateCountryMonthlySalesRow[];
}

export interface JatoMonthlyUpdateReviewBundle {
  jobId: string;
  reviewDir?: string | null;
  compareId: string;
  decisionSuggestion: string;
  compareKeyColumns: string[];
  checklistMarkdown?: string | null;
  reviewFindings: JatoMonthlyUpdateReviewFinding[];
  sampledCountries: string[];
  conflictSampleCount: number;
  conflictSamples: JatoMonthlyUpdateConflictSample[];
  overlapChangeSummary: JatoMonthlyUpdateOverlapChangeSummary[];
  countryFreshnessSummary: JatoMonthlyUpdateCountryFreshnessSummary[];
  countryCoverageSummary: JatoMonthlyUpdateCountryCoverageSummary[];
  countrySalesReferenceLabel: string;
  countryMonthlySalesSummary: JatoMonthlyUpdateCountryMonthlySalesSummary[];
  countryMonthlySalesError?: string | null;
  timeAxisCheck: Record<string, unknown>;
  countryScopeSummary: Record<string, unknown>;
  refreshSummary?: JatoMonthlyUpdateRefreshSummary | null;
}

export interface JatoMonthlyUpdateCleanupResult {
  cleanedAt: string;
  triggeredBy: string;
  cleanupTier: "safe" | "cautious";
  activeBaselinePath: string | null;
  activePatchMonth: string | null;
  freedBytes: number;
  archivedBaselineCount: number;
  archivedBaselines: string[];
  archivedPatchDirCount: number;
  archivedPatchDirs: string[];
  removedUploadSessionDirCount: number;
  removedUploadSessionDirs: string[];
  removedJobUploadDirCount: number;
  removedJobUploadDirs: string[];
  deletedReviewDirCount: number;
  deletedReviewDirs: string[];
  deletedStagingDirCount: number;
  deletedStagingDirs: string[];
  deletedRefreshBackupDirCount: number;
  deletedRefreshBackupDirs: string[];
  deletedArchivedBaselineCount: number;
  deletedArchivedBaselines: string[];
  deletedArchivedPatchDirCount: number;
  deletedArchivedPatchDirs: string[];
}

export interface JatoMonthlyUpdateStorageMetric {
  key: string;
  label: string;
  bytes: number;
  fileCount: number;
  dirCount: number;
  paths: string[];
  cleanupTier: "safe" | "cautious" | "protected";
}

export interface JatoMonthlyUpdateMaintenanceStatus {
  checkedAt: string;
  activeBaselinePath: string | null;
  activeBaselineSource: string | null;
  latestPatchBatch: string | null;
  jobCount: number;
  uploadSessionCount: number;
  trackedStorageBytes: number;
  storageMetrics: JatoMonthlyUpdateStorageMetric[];
}

export interface JatoMonthlyUpdateBaselinePromotionResult {
  promotedAt: string;
  triggeredBy: string;
  sourceParquetPath: string | null;
  baselinePath: string | null;
  detectedLatestMonth: string | null;
  countryCount: number;
  rowCount: number;
  archivedBaselineCount: number;
  archivedBaselines: string[];
}

export interface JatoMonthlyUpdateUploadSession {
  uploadId: string;
  filename: string;
  sizeBytes: number;
  chunkSize: number;
  totalChunks: number;
  receivedChunkCount: number;
  receivedChunks: number[];
  uploadedBytes: number;
  status: string;
  createdAt: string | null;
  updatedAt: string | null;
  completedAt: string | null;
  assembledPath: string | null;
  resumeKey: string | null;
  fileSha256: string | null;
  triggeredBy: string | null;
}

export interface JatoMonthlyUpdateUploadProgress {
  stage: "initiating" | "resuming" | "uploading" | "retrying" | "assembling" | "creating_job" | "queued";
  uploadedBytes: number;
  totalBytes: number;
  uploadedChunks: number;
  totalChunks: number;
  chunkSize: number;
  detail?: string | null;
}

export interface PublishCountryRegression {
  country: string;
  activeLatestMonth: string | null;
  candidateLatestMonth: string | null;
}

export interface PublishSalesDoublingSampleMonth {
  month: string;
  referenceSales: number | null;
  candidateSales: number | null;
  ratio: number;
}

export interface PublishSalesDoublingAnomaly {
  country: string;
  suspiciousMonthCount: number;
  sampleMonths: PublishSalesDoublingSampleMonth[];
  rolling12Ratio: number | null;
}

export interface PublishBlocker {
  blockerType: "country_regression" | "sales_doubling";
  message: string;
  regressions?: PublishCountryRegression[];
  anomalies?: PublishSalesDoublingAnomaly[];
}

export interface DataManagementDatabaseStatus {
  enabled: boolean;
  connected: boolean;
  detail?: string | null;
}

export interface DataManagementMetric {
  label: string;
  value: string | number;
  tone?: string;
}

export interface DataManagementRecentItem {
  label: string;
  value: string | number;
  updatedAt?: string | null;
}

export interface DataManagementDomain {
  key: string;
  label: string;
  status: string;
  storage: string;
  updatedAt?: string | null;
  summary: string;
  metrics: DataManagementMetric[];
  recentItems: DataManagementRecentItem[];
}

export interface DataManagementFileItem {
  key: string;
  label: string;
  kind: string;
  path: string;
  exists: boolean;
  isDir: boolean;
  sizeBytes?: number | null;
  fileCount?: number | null;
  updatedAt?: string | null;
}

export interface DataManagementTableItem {
  key: string;
  label: string;
  domain: string;
  schema: string;
  table: string;
  rowCount: number;
  lastEventAt?: string | null;
  status: string;
}

export interface DataManagementActivityDay {
  date: string;
  count: number;
  level: number;
}

export interface DataManagementActivitySourceCount {
  label: string;
  count: number;
}

export interface DataManagementActivity {
  days: DataManagementActivityDay[];
  maxCount: number;
  totalCount: number;
  rangeStart: string;
  rangeEnd: string;
  sourceCounts: DataManagementActivitySourceCount[];
  databaseConnected: boolean;
}

export interface DataManagementAirflowService {
  service: string;
  state: string;
  status: string;
  health?: string | null;
  containerName?: string | null;
  publishedPorts: string[];
}

export interface DataManagementAirflowActions {
  canStart: boolean;
  canStop: boolean;
  canOpenUi: boolean;
}

export interface DataManagementAirflowStatus {
  available: boolean;
  mode: string;
  detail: string;
  uiUrl: string;
  running: boolean;
  runningServices: number;
  totalServices: number;
  updatedAt: string;
  services: DataManagementAirflowService[];
  actions: DataManagementAirflowActions;
}

export interface DataManagementAirflowActionResponse {
  action: string;
  detail: string;
  status: DataManagementAirflowStatus;
}

export interface DataManagementVocSyncResponse {
  root: string;
  countryCount: number;
  sourceRunCount: number;
  documentCount: number;
  errorCount: number;
}

export interface DataManagementOverviewResponse {
  generatedAt: string;
  database: DataManagementDatabaseStatus;
  domains: DataManagementDomain[];
  airflow: DataManagementAirflowStatus;
  fileInventory: DataManagementFileItem[];
  databaseTables: DataManagementTableItem[];
  activity: DataManagementActivity;
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

/* ---- COC Match ---- */

export interface CocMatchJob {
  jobId: string;
  status: string;
  country: string;
  month: string;
  fileExt: string;
  excelFilename: string;
  archiveFilename: string;
  totalRows?: number;
  matchedCount?: number;
  missingCount?: number;
  extraFileCount?: number;
  differenceType?: string | null;
  hasBidirectionalMismatch?: boolean;
  coverageRate?: number;
  previousRun?: { month: string; matched: number; total: number } | null;
  diffSummary?: { gained: number; lost: number; newEntries: number } | null;
  triggeredBy: string;
  error?: string | null;
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
}
