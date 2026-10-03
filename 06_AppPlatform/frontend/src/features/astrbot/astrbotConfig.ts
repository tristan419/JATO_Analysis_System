export type AstrBotView = "agent" | "profile" | "mcp" | "extensions" | "providers" | "memory" | "usage" | "eval";

export interface AstrBotViewConfig {
  label: string;
  to: string;
  nativeHash: string;
  kicker: string;
  title: string;
  summary: string;
}

export interface AstrBotToolDefinition {
  name: string;
  description: string;
  required: string[];
}

export interface AstrBotRuntimeProbe {
  name: string;
  url: string;
  status: string;
  reachable: boolean;
  httpStatus: number | null;
  latencyMs: number;
  detail: string;
}

export interface AstrBotMcpStatus extends AstrBotRuntimeProbe {
  transport: string;
  toolCount: number;
  tools: AstrBotToolDefinition[];
}

export interface AstrBotProviderStatus {
  sourceId: string;
  providerId: string;
  model: string;
  apiBase: string;
  keySource: string;
  keyConfigured: boolean;
  status: string;
}

export interface AstrBotRetrievalDependencyStatus {
  pageIndex: {
    name: string;
    status: string;
    keySource: string;
    keyConfigured: boolean;
    mcpUrl: string;
    fallback: string;
  };
  miniRag: {
    name: string;
    status: string;
    libraryInstalled: boolean;
    apiConfigured: boolean;
    apiUrl: string;
    workingDir: string;
    corpusStatus: string;
    fallback: string;
  };
}

export interface AstrBotPresetDialog {
  title: string;
  user: string;
  assistant: string;
}

export interface AstrBotAgentProfile {
  id: string;
  shortId: string;
  name: string;
  positioning: string;
  systemPrompt: string;
  modelFailureMessage: string;
  defaultLanguage: string;
  communicationStyle: string[];
  coreCapabilities: string[];
  suggestedTools: string[];
  suggestedSkills: string[];
  automotiveSkills: string[];
  presetDialogs: AstrBotPresetDialog[];
  safetyRules: string[];
}

export interface AstrBotAgentSkill {
  id: string;
  name: string;
  domain: string;
  routeMode: string;
  description: string;
  defaultCountry: string;
  defaultQuestion: string;
  outputContract: string[];
  safety?: string;
}

export interface AstrBotRuntimeStatus {
  updatedAt: string;
  runtime: AstrBotRuntimeProbe;
  mcp: AstrBotMcpStatus;
  provider: AstrBotProviderStatus;
  retrieval: AstrBotRetrievalDependencyStatus;
  profile: AstrBotAgentProfile;
  skills: {
    defaultSkillId: string;
    items: AstrBotAgentSkill[];
  };
  memory: AgentMemoryStats;
  dataBoundary: {
    mode: string;
    directDatabaseAccess: boolean;
    directParquetAccess: boolean;
  };
}

export interface AstrBotToolCallResponse {
  tool: string;
  metadata: Record<string, unknown>;
  data: Record<string, unknown>;
}

export interface AgentRunRecord {
  runId: string;
  createdAt: string;
  profileId: string;
  skillId: string;
  skillName: string;
  country: string;
  mode: string;
  question: string;
  selectedTool: string;
  routeReason: string;
  evidenceSource: string;
  evidenceCount: number;
  displayCards: { label: string; value: string }[];
  resultSummary: string;
  limitations: string[];
  truncated: boolean;
  primaryResultTool: string;
}

export interface AgentRunListResponse {
  items: AgentRunRecord[];
  total: number;
  limit: number;
  offset: number;
}

export interface AgentMemoryStats {
  totalRuns: number;
  maxRuns: number;
  bySkill: Record<string, number>;
  byCountry: Record<string, number>;
  byTool: Record<string, number>;
  latestRunAt: string | null;
}

export interface AgentCompareResponse {
  runIds: string[];
  found: string[];
  missing: string[];
  comparison: { field: string; [runId: string]: unknown }[];
  runs: AgentRunRecord[];
}

export interface AgentConversationTurn {
  turnId: string;
  sessionId: string;
  role: "user" | "assistant";
  text: string;
  metadata: Record<string, unknown>;
  recordedAt: string;
}

export interface AgentConversationHistory {
  sessionId: string;
  turns: AgentConversationTurn[];
  totalTurns: number;
}

export interface AgentConversationSession {
  sessionId: string;
  startedAt: string;
  lastActivityAt: string;
  turnCount: number;
  country?: string;
  latestQuestion?: string;
  latestAnswerPreview?: string;
  latestAnswerTitle?: string;
  answerStatus?: string;
  confidence?: string;
  toolCalls?: string[];
}

export interface AgentConversationSessionsResponse {
  items: AgentConversationSession[];
}

// ── Phase 7: Eval types ──

export interface EvalQuestion {
  id: string;
  category: string;
  country: string;
  question: string;
  expectedRetrievalPath?: string;
  expectedIntent?: string;
  expectedTools: string[];
  expectedFollowUpTypes?: string[];
  difficulty: string;
}

export interface EvalScoreBreakdown {
  path: { expected: string; actual: string; score: number };
  evidence: { itemCount: number; sourceCount: number; score: number };
  tool: { expected: string[]; selected: string; score: number };
  chart: { expected: boolean; produced: number; score: number };
}

export interface EvalScores {
  composite: number;
  retrievalPathCorrectness: number;
  evidenceTraceability: number;
  citationCoverage: number;
  chartCorrectness: number;
  toolSelectionRelevance: number;
  breakdown: EvalScoreBreakdown;
}

export interface EvalResult {
  evalId: string;
  runAt: string;
  questionId: string;
  category: string;
  country: string;
  question: string;
  expectedRetrievalPath: string;
  expectedTools: string[];
  actualTool: string;
  actualRetrievalPath: string;
  allRetrievalPaths: string[];
  evidenceCount: number;
  sourceCount: number;
  chartCount: number;
  scores: EvalScores;
  resultTool: string;
  error?: string;
}

export interface EvalSummary {
  totalRuns: number;
  byCategory: Record<string, { count: number; avgComposite: number; avgEvidence: number; avgCitation: number; avgTool: number }>;
  overallScores: { count: number; avgComposite: number; avgEvidence: number; avgCitation: number; avgTool: number };
  latestRunAt: string | null;
}

export interface EvalQuestionsResponse {
  items: EvalQuestion[];
  total: number;
  byCategory: Record<string, number>;
}

export interface EvalResultsResponse {
  items: EvalResult[];
  total: number;
  limit: number;
  offset: number;
  summary: { count: number; avgComposite: number; avgEvidence: number; avgCitation: number; avgTool: number };
}

export interface EvalCategoryRunResponse {
  category: string;
  total: number;
  results: EvalResult[];
  summary: { count: number; avgComposite: number; avgEvidence: number; avgCitation: number; avgTool: number };
}

export interface EvalFullRunResponse {
  totalRun: number;
  byCategory: Record<string, { count: number; avgComposite: number; avgEvidence: number; avgCitation: number; avgTool: number }>;
  overallSummary: { count: number; avgComposite: number; avgEvidence: number; avgCitation: number; avgTool: number };
  results: EvalResult[];
}

export interface EvalSideBySideSummary {
  count: number;
  pendingHumanScoring: number;
  pendingBaselineScoring?: number;
  pendingReplacementBaselineScoring?: number;
  scoredCount?: number;
  baselineScoredCount?: number;
  replacementBaselineScoredCount?: number;
  astrbotErrorCount: number;
  countryCopilotErrorCount: number;
  avgAstrBotComposite: number;
  avgAstrBotHumanScore?: number;
  avgCountryCopilotHumanScore?: number;
  humanScoreSourceCounts?: Record<string, number>;
  baselineSourceCounts?: Record<string, number>;
  replacementBaselineSourceCounts?: Record<string, number>;
  humanWins?: Record<string, number>;
  replacementWins?: Record<string, number>;
  failureTagCounts?: Record<string, number>;
  topFailureTags?: Array<{ tag: string; count: number }>;
  repairGapCounts?: Record<string, number>;
  sourceRepairBacklogCount?: number;
  topRepairGaps?: Array<{
    gap: string;
    tag?: string;
    count: number;
    sampleCandidates?: string[];
    sampleQuestionIds?: string[];
    sampleQuestions?: Array<{
      questionId: string;
      category?: string;
      country?: string;
      question?: string;
      priority?: string;
      answerStatus?: string;
      repairAction?: string;
    }>;
  }>;
  astrbotWinRate?: number;
  replacementAstrbotWinRate?: number;
  categoryLevelScore?: Record<string, {
    count: number;
    scoredCount: number;
    avgAstrBot: number;
    avgCopilot: number;
    astrbotWinRate: number;
    wins: Record<string, number>;
  }>;
  judgeCalibration?: EvalJudgeCalibrationSummary;
  referenceJudgePaths?: EvalReferenceJudgePathMatrix;
  recommendedNextActions?: EvalRecommendedNextAction[];
  replacementReadinessVerdict?: string;
  replacementReadiness?: EvalReplacementReadiness;
  selfTestBaseline?: EvalSelfTestBaseline;
}

export interface EvalReplacementReadiness {
  status: string;
  verdict: string;
  replacementReady: boolean;
  businessBaselineReady: boolean;
  winRateReady: boolean;
  executionClean: boolean;
  hallucinationClean: boolean;
  totalQuestions: number;
  minimumRequiredScores: number;
  scoredCount: number;
  pendingCount: number;
  sourceCounts: Record<string, number>;
  astrbotWinRate: number;
  avgAstrBotScore: number;
  avgCountryCopilotScore: number;
  astrbotErrorCount: number;
  countryCopilotErrorCount: number;
  hallucinationRiskCount: number;
  failureTagTotal: number;
  reasons: string[];
  recommendedNextAction: string;
}

export interface EvalSelfTestBaseline {
  status: string;
  selfTestReady: boolean;
  totalQuestions: number;
  minimumRequiredScores: number;
  scoredCount: number;
  pendingCount: number;
  sourceCounts: Record<string, number>;
  codexReviewedCount: number;
  trustedBaselineCount: number;
  astrbotWinRate: number;
  avgAstrBotScore: number;
  avgCountryCopilotScore: number;
  recommendedNextAction: string;
}

export interface EvalJudgeCalibrationItem {
  questionId: string;
  category: string;
  question: string;
  gptJudgeScores: {
    astrbot?: Record<string, number>;
    copilot?: Record<string, number>;
  };
  gptJudgeWinner: string;
  gptFailureTags: string[];
  humanScores: {
    astrbot?: Record<string, number>;
    copilot?: Record<string, number>;
  };
  humanWinner: string;
  humanFailureTags: string[];
  agreementStatus: "match" | "partial" | "mismatch" | "pending";
  humanNotes: string;
}

export interface EvalJudgeCalibrationSummary {
  gptJudgedCount: number;
  humanReviewedCount: number;
  matchCount: number;
  partialCount: number;
  mismatchCount: number;
  agreementRate: number;
  weightedAgreementRate: number;
  needsHumanReviewCount: number;
  mismatchExamples: EvalJudgeCalibrationItem[];
  items: EvalJudgeCalibrationItem[];
}

export interface EvalRecommendedNextAction {
  tag: string;
  count: number;
  module: string;
  recommendation: string;
  priority: string;
}

export interface EvalReferenceJudgePath {
  id: string;
  label: string;
  status: string;
  readinessStatus: string;
  active: boolean;
  implemented: boolean;
  role: string;
  evidence: string;
  provider: string;
  model: string;
  apiBase: string;
  keySource: string;
  keyConfigured: boolean;
  env: {
    provider: string;
    model: string;
    apiBase: string;
    keySource: string;
  };
  nextAction: string;
}

export interface EvalReferenceJudgePathMatrix {
  source: string;
  activePathId: string;
  activeProvider: {
    provider: string;
    model: string;
    apiBase: string;
    keySource: string;
  };
  paths: EvalReferenceJudgePath[];
}

export interface EvalJudgePreflightResponse {
  ready: boolean;
  enabled: boolean;
  missingKey: boolean;
  missing_key?: boolean;
  liveCheck: boolean;
  status: "ready" | "ok" | "disabled" | "missing_key" | "failed" | string;
  reason: string;
  provider: {
    provider: string;
    model: string;
    apiBase: string;
    keySource: string;
  };
  referenceJudgePaths?: EvalReferenceJudgePathMatrix;
  responsePreview?: string;
}

export interface EvalCodexReviewNote {
  questionId: string;
  uiStatus: "pass" | "warning" | "fail" | string;
  suggestedWinner: string;
  suggestedScores: {
    astrbot?: Record<string, number>;
    countryCopilot?: Record<string, number>;
    copilot?: Record<string, number>;
  };
  suggestedFailureTags: string[];
  reviewNotes: string;
  uiIssues?: string[];
  screenshots: string[];
  createdAt: string;
  source: "codex_review";
}

export interface EvalCodexReviewNotesResponse {
  items: EvalCodexReviewNote[];
  total: number;
  limit: number;
  latestByQuestionId: Record<string, EvalCodexReviewNote>;
}

export interface EvalCodexReviewScoringArtifactsResponse {
  available: boolean;
  runId?: string;
  artifactDir?: string;
  hasManualTemplate?: boolean;
  hasDraft?: boolean;
  hasReferenceJudgePacket?: boolean;
  manualTemplatePath?: string;
  codexDraftSheetPath?: string;
  referenceJudgePacketJsonPath?: string;
  referenceJudgePacketMdPath?: string;
  manualTemplateText?: string;
  codexDraftSheetText?: string;
  referenceJudgePacketJsonText?: string;
  referenceJudgePacketMdText?: string;
  rowCount?: number;
  warning?: string;
  reason?: string;
}

export interface EvalSideBySideAstrBot {
  status: string;
  error?: string;
  answerTitle?: string;
  answerPreview?: string;
  evidenceBackedLead?: string;
  bullets?: string[];
  limitations?: string[];
  answerStatus?: string;
  confidence?: string;
  selectedTool?: string;
  retrievalPath?: string;
  allRetrievalPaths?: string[];
  evidenceCount?: number;
  sourceCount?: number;
  evidencePackage?: Record<string, unknown>;
  evidenceRefCount?: number;
  evidenceConfidence?: string;
  evidenceDigest?: string[];
  displayPlan?: string;
  missingEvidence?: unknown;
  sourceRepairCandidates?: EvalSourceRepairCandidates;
  visualArtifacts?: unknown;
  qualityScore?: {
    intentScore?: number;
    toolScore?: number;
    groundingScore?: number;
    followUpScore?: number;
    safetyScore?: number;
    engineeringQualityScore?: number;
    businessCompletenessScore?: number;
    totalScore?: number;
    failures?: string[];
  };
  followUps?: unknown;
  recommendedActions?: unknown;
  chartCount?: number;
  composer?: string;
  modelUsageStatus?: string;
  scores?: Partial<EvalScores> & { error?: string };
}

export interface EvalSourceRepairCandidate {
  sourceCode: string;
  brand?: string;
  model?: string;
  sourceUrl?: string;
  relativePath?: string;
  draftStatus?: string;
  currentPriceRows?: number;
  candidateSourceType?: string;
  candidateDomain?: string;
  sourceSearchQuery?: string;
}

export interface EvalSourceRepairCandidates {
  dataStatus?: string;
  missingOwnModelSource?: boolean;
  candidateCount?: number;
  materializedCandidateCount?: number;
  ownModel?: EvalSourceRepairCandidate[];
  competitorCorridor?: EvalSourceRepairCandidate[];
}

export interface EvalEvidenceRepairSummary {
  primaryGap?: string;
  missingEvidenceCount?: number;
  blockingEvidenceCount?: number;
  weakEvidenceCount?: number;
  sourceCandidateCount?: number;
  ownModelCandidateCount?: number;
  competitorCandidateCount?: number;
  materializedCandidateCount?: number;
  missingOwnModelSource?: boolean;
  dataStatus?: string;
  sourceSummary?: string;
  nextStep?: string;
}

export interface EvalEvidenceRepairTask {
  taskId: string;
  taskType: string;
  title: string;
  input: string;
  output: string;
  owner: string;
  priority: string;
  status: string;
  evidenceName?: string;
  sourceCandidates?: string[];
  commandHint?: string;
}

export interface EvalSideBySideCountryCopilot {
  status: string;
  error?: string;
  answerPreview?: string;
  answerMode?: string;
  provider?: string;
  model?: string;
  providerReason?: string;
  intentRoute?: string;
  focusedIntents?: string[];
  confidence?: string;
  evidenceTableCount?: number;
  sourceCount?: number;
  chartLinkCount?: number;
}

export interface EvalSideBySideComparison {
  bothReturned: boolean;
  requiresHumanScoring: boolean;
  astrbotAnswerChars: number;
  countryCopilotAnswerChars: number;
  answerLengthDelta: number;
  errorCount: number;
  recommendedManualDecision: string;
}

export interface EvalSideBySideHumanScoring {
  status: string;
  source?: string;
  judgeProvider?: {
    source?: string;
    pathId?: string;
    label?: string;
    provider?: string;
    model?: string;
    apiBase?: string;
    keySource?: string;
  };
  dimensions?: string[];
  winner?: string;
  notes?: string;
  astrbotScores?: Record<string, number>;
  countryCopilotScores?: Record<string, number>;
  copilotScores?: Record<string, number>;
  scoreTotals?: {
    astrbot?: number;
    countryCopilot?: number;
    delta?: number;
    astrbotCompleted?: number;
    countryCopilotCompleted?: number;
    requiredDimensions?: number;
    astrbotComplete?: boolean;
    countryCopilotComplete?: boolean;
    complete?: boolean;
  };
  updatedAt?: string;
}

export interface EvalScoreDimension {
  key: string;
  label: string;
}

export interface EvalBusinessValidationProjection {
  question: string;
  category: string;
  expectedIntent: string;
  expectedTools: string[];
  astrbotAnswer: string;
  copilotAnswer: string;
  astrbotEvidencePackage?: Record<string, unknown>;
  astrbotVisualArtifacts?: unknown;
  astrbotFollowUps?: unknown;
  astrbotQualityScore?: Record<string, unknown>;
  astrbotEvidenceDigest?: string[];
  astrbotDisplayPlan?: string;
  astrbotScores: Record<string, number>;
  copilotScores: Record<string, number>;
  winner: string;
  humanNotes: string;
  failureTags?: string[];
  businessPlaybook?: Record<string, unknown>;
}

export interface EvalSideBySideRecord {
  id?: string;
  comparisonId: string;
  runAt: string;
  validationType?: string;
  questionId: string;
  category: string;
  country: string;
  question: string;
  expectedIntent?: string;
  expectedRetrievalPath?: string;
  expectedTools?: string[];
  expectedFollowUpTypes?: string[];
  scoreSchema?: EvalScoreDimension[];
  astrbot?: EvalSideBySideAstrBot;
  countryCopilot?: EvalSideBySideCountryCopilot;
  comparison?: EvalSideBySideComparison;
  humanScoring?: EvalSideBySideHumanScoring;
  businessValidation?: EvalBusinessValidationProjection;
  businessPlaybook?: Record<string, unknown>;
  failureTags?: string[];
  copilotAnswer?: string;
  astrbotAnswer?: string;
  astrbotEvidencePackage?: Record<string, unknown>;
  astrbotVisualArtifacts?: unknown;
  astrbotFollowUps?: unknown;
  astrbotQualityScore?: Record<string, unknown>;
  astrbotEvidenceDigest?: string[];
  astrbotDisplayPlan?: string;
  scores?: {
    astrbot?: Record<string, number>;
    copilot?: Record<string, number>;
    astrbotAverage?: number;
    copilotAverage?: number;
    astrbotComposite?: number;
    complete?: boolean;
  };
  winner?: string;
  humanNotes?: string;
  llmJudge?: Record<string, unknown>;
  errors?: Record<string, string>;
}

export interface EvalSideBySideResultsResponse {
  items: EvalSideBySideRecord[];
  total: number;
  limit: number;
  offset: number;
  summary: EvalSideBySideSummary;
}

export interface EvalBusinessQuestionsResponse {
  items: EvalQuestion[];
  total: number;
  byCategory: Record<string, number>;
  scoreDimensions: EvalScoreDimension[];
}

export interface EvalBusinessRunResponse {
  category?: string;
  total: number;
  results: EvalSideBySideRecord[];
  summary: EvalSideBySideSummary;
  markdown: string;
}

export interface EvalBusinessJudgeExistingResponse {
  status: string;
  category?: string;
  limit: number;
  latestPerQuestion: boolean;
  scoreReadyOnly?: boolean;
  totalRecords: number;
  candidateCount: number;
  selectedCount: number;
  attemptedCount: number;
  judgedCount: number;
  savedCount: number;
  failedCount: number;
  skippedCount: number;
  statusCounts: Record<string, number>;
  results: Array<{
    comparisonId?: string;
    questionId?: string;
    category?: string;
    status?: string;
    reason?: string;
    saved?: boolean;
    winner?: string;
    astrbotScore?: number;
    countryCopilotScore?: number;
  }>;
  summary: EvalSideBySideSummary;
}

export interface EvalEvidenceRepairItem {
  questionId: string;
  comparisonId?: string;
  category: string;
  country?: string;
  question: string;
  priority: "P0" | "P1" | string;
  primaryGap?: string;
  commandHint?: string;
  answerStatus: string;
  selectedTool: string;
  failureTags: string[];
  missingEvidence: Array<{ name: string; reason: string; impact: string }>;
  recommendedActions: Array<{ action: string; rationale: string; priority: string }>;
  sourceRepairCandidates?: EvalSourceRepairCandidates;
  repairSummary?: EvalEvidenceRepairSummary;
  repairAction?: string;
  repairTasks?: EvalEvidenceRepairTask[];
}

export interface EvalSourceRepairBacklogItem {
  priority: "P0" | "P1" | string;
  sourceType: string;
  role?: string;
  label: string;
  brand?: string;
  model?: string;
  candidateSourceType?: string;
  candidateDomain?: string;
  sourceDraftPath?: string;
  relativePath?: string;
  sourceSearchQuery?: string;
  sourceUrl?: string;
  affectedCount: number;
  questionIds: string[];
  categories: string[];
  countries: string[];
  primaryGaps: string[];
  failureTags: string[];
  recommendedAction: string;
}

export interface EvalBusinessReportResponse {
  items: EvalSideBySideRecord[];
  total: number;
  summary: EvalSideBySideSummary;
  evidenceRepairQueue?: EvalEvidenceRepairItem[];
  sourceRepairBacklog?: EvalSourceRepairBacklogItem[];
  scoreDimensions: EvalScoreDimension[];
  markdown: string;
}

export interface EvalSideBySideCategoryRunResponse {
  category: string;
  total: number;
  results: EvalSideBySideRecord[];
  summary: EvalSideBySideSummary;
}

const DEFAULT_ASTRBOT_NATIVE_BASE_URL = "http://localhost:6185/";

export const ASTRBOT_VIEW_CONFIGS: Record<AstrBotView, AstrBotViewConfig> = {
  agent: {
    label: "Chat",
    to: "/astrbot",
    nativeHash: "#/",
    kicker: "Chat",
    title: "Hermes Agent Chat",
    summary: "Ask business, code, market, pricing and document questions through governed JATO tools.",
  },
  profile: {
    label: "Persona",
    to: "/astrbot/profile",
    nativeHash: "#/persona",
    kicker: "Persona",
    title: "Agent Persona",
    summary: "The default JATO persona defines tone, coding behavior, market analysis boundaries and safety rules.",
  },
  mcp: {
    label: "Tools",
    to: "/astrbot/mcp",
    nativeHash: "#/extension#mcp",
    kicker: "Tooling",
    title: "JATO MCP Tools",
    summary: "The agent connects to JATO through one streamable HTTP MCP server.",
  },
  extensions: {
    label: "Skills",
    to: "/astrbot/extensions",
    nativeHash: "#/extension",
    kicker: "Policy",
    title: "Skill Surface",
    summary: "Extension capability is kept inside a controlled JATO boundary while developer fallback remains available.",
  },
  providers: {
    label: "Models",
    to: "/astrbot/providers",
    nativeHash: "#/provider",
    kicker: "Model",
    title: "CountryCopilot Provider Reuse",
    summary: "The agent uses the same DP V4 provider secret source as CountryCopilot.",
  },
  memory: {
    label: "Memory",
    to: "/astrbot/memory",
    nativeHash: "#/",
    kicker: "History",
    title: "Agent Memory / Run History",
    summary: "Every routed agent run is stored with skill, tool, evidence source and result summary for comparison and optimization.",
  },
  usage: {
    label: "Usage",
    to: "/astrbot/usage",
    nativeHash: "#/",
    kicker: "Usage",
    title: "Conversation Usage",
    summary: "Track session activity, model tokens, cost, status, tool mix and recent agent calls.",
  },
  eval: {
    label: "Evaluation",
    to: "/astrbot/eval",
    nativeHash: "#/",
    kicker: "Evaluation",
    title: "A/B Evaluation",
    summary: "Run the 100-question eval set to measure retrieval correctness, evidence traceability, and tool selection accuracy.",
  },
};

export const ASTRBOT_PROVIDER = {
  sourceId: "jato_countrycopilot_deepseek",
  providerId: "jato_countrycopilot_deepseek_chat",
  model: "deepseek-chat",
  apiBase: "https://api.deepseek.com/v1",
  keySource: "$DEEPSEEK_API_KEY",
};

export const ASTRBOT_MCP_ENDPOINT = "http://127.0.0.1:8185/mcp";

export function normalizeAstrBotBaseUrl(value?: string): string {
  const trimmed = value?.trim() ?? "";
  const baseUrl = trimmed || DEFAULT_ASTRBOT_NATIVE_BASE_URL;
  return baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
}

export function getAstrBotNativeBaseUrl(): string {
  return normalizeAstrBotBaseUrl(import.meta.env.VITE_ASTRBOT_EMBED_URL as string | undefined);
}

export function buildAstrBotNativeUrl(view: AstrBotView): string {
  return `${getAstrBotNativeBaseUrl()}${ASTRBOT_VIEW_CONFIGS[view].nativeHash}`;
}

export function resolveAstrBotView(pathname: string): AstrBotView {
  if (pathname.endsWith("/profile")) {
    return "profile";
  }
  if (pathname.endsWith("/mcp")) {
    return "mcp";
  }
  if (pathname.endsWith("/extensions")) {
    return "extensions";
  }
  if (pathname.endsWith("/providers")) {
    return "providers";
  }
  if (pathname.endsWith("/memory")) {
    return "memory";
  }
  if (pathname.endsWith("/usage")) {
    return "usage";
  }
  if (pathname.endsWith("/eval")) {
    return "eval";
  }
  return "agent";
}
