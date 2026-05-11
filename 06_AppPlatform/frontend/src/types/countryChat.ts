import type {
  MarketScanBodyShareTrendItem,
  MarketScanMatrix,
  MarketScanRankingGroup,
  MarketScanRankingItem,
  ModelVersionItem,
  OverviewResponse,
  PositioningMapResponse,
  TimeSeriesPoint,
} from "./index";

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

export interface CountryChatCrossTabRow {
  _index: string;
  _total: number;
  [key: string]: string | number;
}

export interface CountryChatCrossTabs {
  driveByFuel?: CountryChatCrossTabRow[];
  registrationByFuel?: CountryChatCrossTabRow[];
  driveBySegment?: CountryChatCrossTabRow[];
  segmentByFuel?: CountryChatCrossTabRow[];
  fuelBySegment?: CountryChatCrossTabRow[];
  driveByOrigin?: CountryChatCrossTabRow[];
  registrationByOrigin?: CountryChatCrossTabRow[];
  registrationBySegment?: CountryChatCrossTabRow[];
  availableDimensions: string[];
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
  crossTabs?: CountryChatCrossTabs;
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

export interface CountryCopilotSourcePlanItem {
  source_id: string;
  source_lane: string;
  required: boolean;
  reason: string;
  expected_output: string;
  fallback_source_id?: string | null;
  freshness_required?: boolean;
  confidence_weight?: number;
}

export interface CountryCopilotSourcePlan {
  question: string;
  intent: string;
  execution_mode: string;
  items: CountryCopilotSourcePlanItem[];
  answer_mode: string;
  requires_sql_planner: boolean;
  requires_tool_planner: boolean;
  requires_evidence_retrieval: boolean;
}

export interface CountryCopilotEvidenceSource {
  source_id: string;
  source_lane: string;
  source_name: string;
  freshness?: string | null;
  confidence: "high" | "medium" | "low";
  coverage: "strong" | "partial" | "thin" | "missing";
  limitations?: string[];
}

export interface CountryCopilotEvidencePack {
  evidence_pack_id: string;
  question: string;
  country?: string | null;
  intent: string;
  answer_mode: string;
  sources: CountryCopilotEvidenceSource[];
  claims?: CountryCopilotEvidenceClaim[];
  limitations: string[];
  next_data_needed: string[];
}

export interface CountryCopilotEvidenceClaim {
  claim_id: string;
  claim: string;
  claim_type: string;
  supporting_source_ids: string[];
  confidence: "high" | "medium" | "low";
  limitations?: string[];
}

export interface CountryCopilotGovernanceTrace {
  intent: string;
  focusedIntents?: string[];
  intentRoute?: string;
  planGenerated: boolean;
}

export interface CountryCopilotAnswerBlock {
  block_type: string;
  title: string;
  content: string;
  data?: Record<string, unknown> | null;
}

export interface CountryCopilotStructuredAnswer {
  summary: string;
  blocks: CountryCopilotAnswerBlock[];
  limitations: string[];
  recommendations: string[];
  answer_mode: string;
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
  sourcePlan?: CountryCopilotSourcePlan | null;
  evidencePack?: CountryCopilotEvidencePack | null;
  governanceTrace?: CountryCopilotGovernanceTrace | null;
  structuredAnswer?: CountryCopilotStructuredAnswer | null;
  auditId?: string | null;
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
