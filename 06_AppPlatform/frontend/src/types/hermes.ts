// Hermes Governance Layer — TypeScript types

// === Markdown Diagrams ===
export interface HermesMermaidBlock {
  file: string;
  title: string;
  diagramIndex: number;
  raw: string;
  type: string;
  category?: string;
  categoryLabel?: string;
}

// === Sentinel Inbox ===
export type HermesSentinelSeverity = "low" | "medium" | "high" | "critical";
export type HermesSentinelMailboxStatus = "new" | "read" | "acked" | "archived" | "resolved";

export interface HermesSentinelNotification {
  id: string;
  severity: HermesSentinelSeverity | string;
  source: string;
  title: string;
  body: string;
  actions: string[];
  actionLevel?: string;
  blocking?: boolean;
  recommendedAction?: string;
  context?: Record<string, unknown>;
  status: HermesSentinelMailboxStatus | string;
  createdAt: string;
  updatedAt?: string;
}

export interface HermesSentinelProbe {
  probe: string;
  overall: "ok" | "warning" | "critical" | string;
  findings: Array<Record<string, unknown>>;
  status?: Record<string, unknown>;
}

export interface HermesSentinelStatusResponse {
  overall: "ok" | "warning" | "critical" | string;
  probes: HermesSentinelProbe[];
  notifications: HermesSentinelNotification[];
  emittedNotifications?: HermesSentinelNotification[];
  unreadCount: number;
  checkedAt: string;
}

export interface HermesFullDesignDocumentResponse {
  exists: boolean;
  path: string;
  content: string;
  updatedAt: string | null;
}

export interface HermesDeployStatusResponse {
  status: "ok" | "warning" | "critical" | string;
  release: Record<string, unknown>;
  expected: Record<string, unknown>;
  drift: Record<string, unknown>;
  lastDeploy: Record<string, unknown>;
  warnings: string[];
  checkedAt: string;
}

// === Governance Gaps ===
export interface HermesGap {
  gapId: string;
  title?: string;
  name?: string;
  category: string;
  severity: string;
  status: "open" | "resolved" | "in_progress";
  affectedAssets?: string[];
  evidence?: string[];
  recommendedAction?: string;
  owner?: string;
  notes?: string;
  description?: string;
}

// === Evidence Ledger ===
export interface HermesEvidenceRecord {
  createdAt?: string;
  timestamp?: string;
  artifactId?: string;
  sourceRef?: string;
  fact?: string;
  quote?: string;
  type?: string;
  event?: string;
}

export interface HermesEvidenceLedgerResponse {
  totalCount: number;
  records: HermesEvidenceRecord[];
  byType: Record<string, number>;
  rangeStart: string;
  rangeEnd: string;
}

// === Architecture ===
export interface HermesArchModule {
  governor: string;
  icon: string;
  phase: string;
  scripts: string[];
  inputs: string[];
  outputs: string[];
  answers: string[];
  triggers: string;
}

export interface HermesArchDependency {
  from: string;
  to: string;
  what: string;
}

export interface HermesArchRouting {
  task: string;
  ask: string;
  run: string;
  gets: string;
}

export interface HermesArchResponse {
  modules: HermesArchModule[];
  dependencies: HermesArchDependency[];
  routing: HermesArchRouting[];
}

// === Toolchain ===
export interface HermesToolchainScript {
  name: string;
  path: string;
  sizeBytes: number;
}

export interface HermesToolchainWorkflowStep {
  step: number;
  phase: string;
  script: string;
  action: string;
  description: string;
}

export interface HermesToolchainResponse {
  scripts: HermesToolchainScript[];
  registries: { name: string; path: string }[];
  reports: { name: string; path: string }[];
  workflow: HermesToolchainWorkflowStep[];
  scriptCount: number;
  registryCount: number;
  reportCount: number;
}

// === Overview ===
export interface HermesOverviewResponse {
  registries: Record<string, number>;
  reports: Record<string, boolean>;
  proposals: { total: number; implemented: number; pending: number; draft: number };
  gaps: { total: number; open: number; resolved: number };
  error?: string;
}

// === Pipeline Health ===
export interface HermesPipelineItem {
  pipelineId?: string;
  name?: string;
  type?: string;
  role?: string;
  status?: string;
  riskLevel?: string;
  risk?: string;
  schedule?: string;
  schedulerDecision?: string;
  lastObserved?: Record<string, unknown>;
  dependsOn?: string[];
  consumers?: string[];
}

export interface HermesPipelineStatusRecord {
  pipelineId: string;
  status: string;
  lastRunAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  exitCode?: number | null;
  durationSeconds?: number;
  recordsProcessed?: number;
  failedCount?: number;
  warningCount?: number;
  artifactRefs?: string[];
  source?: string;
  message?: string;
  statusPath?: string;
  standardStatusFile?: boolean;
  readinessStatus?: string;
  smokeStatus?: string;
  contractStatus?: string;
  stageStatus?: string;
  snapshotWeek?: string;
  jobsByKind?: Record<string, number>;
  statusCounts?: Record<string, number>;
  runtimeCounts?: Record<string, number>;
  msrpMissingRequirementKeys?: string[];
  sourceDraftTodoPlaceholderCount?: number;
  productionStatus?: string;
  goalCompletionStatus?: string;
  localP0Ready?: boolean;
  warnings?: string[];
}

export interface HermesPipelineHealthResponse {
  summary?: Record<string, unknown>;
  pipelines?: HermesPipelineItem[];
  allPipelines?: HermesPipelineItem[];
  findings?: unknown[];
  [key: string]: unknown;
}

// === Source Quality ===
export interface HermesSourceItem {
  sourceId?: string;
  name?: string;
  sourceType?: string;
  status?: string;
  qualityScore?: number;
  quality?: Record<string, unknown>;
  country?: string;
  countries?: string;
  governanceStatus?: string;
  knownIssues?: string[];
}

export interface HermesSourceQualityResponse {
  summary?: Record<string, unknown>;
  sources?: HermesSourceItem[];
  [key: string]: unknown;
}

// === Cost ===
export interface HermesCostDay {
  date: string;
  costCny: number;
  overDailyBudget: boolean;
}

export interface HermesCostResponse {
  days?: HermesCostDay[];
  totalCny?: number;
  dailyBudgetCny?: number;
  monthlyBudgetCny?: number;
  monthlyStatus?: string;
  byModelCny?: Record<string, number>;
  alerts?: string[];
  emailSent?: boolean;
  alertEmail?: string;
  [key: string]: unknown;
}

// === Activity ===
export interface HermesActivityDay {
  date: string;
  count: number;
}

export interface HermesActivityResponse {
  totalRecords: number;
  days: HermesActivityDay[];
  byCommand: Record<string, number>;
  lastRun: Record<string, unknown> | null;
}

// === Daily Summary ===
export interface HermesDailySummaryResponse {
  date: string;
  activityCount: number;
  costCny: number;
  dailyBudgetCny: number;
  monthlyBudgetCny: number;
  costStatus: string;
}

// === Feature Kanban ===
export interface HermesKanbanFeature {
  featureId: string;
  name: string;
  status: string;
  implementationStatus: string;
  riskLevel: string;
  phase?: string;
  color?: string;
  dependencies?: string[];
  routes?: string[];
  backendApis?: string[];
  tests?: string[];
  docs?: string[];
  knownIssues?: string[];
  governanceStatus?: string;
}

export interface HermesKanbanColumn {
  label: string;
  color: string;
  features: HermesKanbanFeature[];
}

export interface HermesFeatureKanbanResponse {
  summary: {
    total: number;
    active: number;
    beta: number;
    planned: number;
    archived?: number;
    withTests: number;
    withDocs?: number;
    withIssues: number;
  };
  columns: Record<string, HermesKanbanColumn>;
}

// === Hermes Chat ===
export type HermesReplyType = "direct_answer" | "run_created" | "clarification_needed" | "blocked_by_policy";
export interface HermesChatRequest { message: string; sessionId?: string; context?: { userRole?: "user"|"admin"|"developer"; country?: string; model?: string }; }
export interface HermesChatSuggestedAction { label: string; action: string; intent?: string; runId?: string; command?: string; }
export interface HermesChatResponse { sessionId: string; messageId: string; replyType: HermesReplyType; answer: string; intent: string; confidence: number; entities: Record<string, string[]>; runId?: string; command?: string; tasks?: string[]; dataRefs: string[]; suggestedActions: HermesChatSuggestedAction[]; }
export interface HermesChatSession { sessionId: string; createdAt: string; updatedAt: string; messageCount: number; }
export interface HermesChatSessionDetail { sessionId: string; createdAt: string; updatedAt: string; messages: HermesChatMessage[]; }
export interface HermesChatMessage { messageId: string; role: "user"|"assistant"; content: string; replyType?: string; timestamp: string; }

// === Hermes Commands ===
export interface HermesCommand { commandId: string; label: string; description: string; requiredRole: "user"|"admin"|"developer"; mapsToIntent: string; parameters: { name: string; type: "string"; required: boolean; default?: string }[]; }
export interface HermesCommandExecuteRequest { commandId: string; parameters?: Record<string, string>; sessionId?: string; }
export interface HermesCommandExecuteResponse { commandId: string; runId: string; script: string; exitCode: number; stdout: string; stderr: string; status: "success"|"failed"|"timeout"|"error"; }

// === Hermes DevSync ===
export type HermesFeatureStatus = "idea"|"planned"|"in_progress"|"implemented"|"verified"|"done"|"blocked"|"deprecated";
export interface HermesDevEvent { eventId: string; eventType: string; source: string; title: string; summary: string; linkedFeatureIds: string[]; changedFiles: string[]; addedFiles?: string[]; addedEndpoints?: string[]; frontendChanges?: string[]; backendChanges?: string[]; tests?: Record<string, string>; risks?: string[]; nextSteps?: string[]; createdAt: string; }
export interface HermesDevFeature { featureId: string; title: string; status: HermesFeatureStatus; category: string; source: string; summary: string; linkedEventIds: string[]; endpoints: string[]; frontend: string[]; backend: string[]; tests: Record<string, string>; docs: string[]; evidenceIds: string[]; gaps: string[]; createdAt: string; lastUpdatedAt: string; }
export interface HermesDevSyncResult { synced: number; featuresUpdated: string[]; featuresCreated: string[]; docsGenerated: number; evidenceWritten: number; gapsCreated: number; }

// === MSRP Country Progress ===
export interface HermesMsrpCountryProgressCountry {
  countryCode: string;
  countryLabel?: string;
  total: number;
  pass: number;
  empty: number;
  fail: number;
  errors: number;
  passPct: number;
  status: string;
  completed?: boolean;
  runId?: string;
  batch?: string;
  timestamp?: string;
  gateStatus?: string;
  runStatus?: string;
  isLatestRun?: boolean;
  topFailureReason?: string;
  failureBreakdown?: Record<string, number>;
  strategyRecommendations?: Record<string, number>;
  financeObservationCandidates?: number;
  financeMonthlyPaymentCount?: number;
  financeSemanticsCounts?: Record<string, number>;
  financeTypeCounts?: Record<string, number>;
}
export interface HermesMsrpSourceRepairBacklogGroup {
  failureReason: string;
  count: number;
  transientRegressionCount?: number;
  sourceRepairIssueCount?: number;
  priorityScore?: number;
  priorityBand?: string;
  priorityWeights?: {
    sourceRepair?: number;
    country?: number;
    hostCluster?: number;
    transientRecheck?: number;
  };
  reviewAssist?: {
    preferred?: string;
    llmFit?: string;
    neuralNetworkFit?: string;
    reason?: string;
  };
  referenceAssist?: {
    preferred?: string;
    thirdPartyReference?: string;
    referencePolicy?: string;
    officialSourceRequiredForIngest?: boolean;
    acceptanceRules?: string[];
    reason?: string;
  };
  recommendedAction?: string;
  recommendedStrategy: string;
  recommendedStrategies?: Record<string, number>;
  affectedCountries: string[];
  affectedCountryCount?: number;
  affectedBrands?: string[];
  sampleSources?: string[];
  sampleTransientRegressions?: Array<{
    countryCode: string;
    sourceCode: string;
    failureReason?: string;
    recommendedStrategy?: string;
    lastKnownGoodRunId?: string;
    lastKnownGoodAt?: string;
    recommendedAction?: string;
  }>;
  status?: string;
}
export interface HermesMsrpSourceRepairBacklog {
  schemaVersion: string;
  runId?: string | null;
  generatedAt?: string | null;
  totalIssueCount: number;
  transientRegressionCount?: number;
  sourceRepairIssueCount?: number;
  groups: HermesMsrpSourceRepairBacklogGroup[];
}
export interface HermesMsrpSourceReferenceEvidenceItem {
  countryCode: string;
  pricingCountry?: string;
  brand?: string;
  modelQuery: string;
  sourceCodes?: string[];
  referenceSource?: string;
  referencePolicy?: string;
  officialIngestEligible: boolean;
  localReferenceCount: number;
  localPriceReferences?: Array<{
    evId?: string;
    name?: string;
    startPrice?: number;
    currency?: string;
    pricingCountry?: string;
    isConverted?: boolean;
    infoUrl?: string;
  }>;
  fetchError?: string | null;
  reviewRecommendation?: string;
}
export interface HermesMsrpSourceReferenceEvidence {
  schemaVersion: string;
  generatedAt?: string | null;
  backlogRunId?: string | null;
  referenceSource?: string;
  referencePolicy?: string;
  officialSourceRequiredForIngest?: boolean;
  officialIngestEligible?: boolean;
  summary: {
    evidenceItemCount: number;
    localReferenceCount: number;
    missingLocalReferenceCount: number;
    officialIngestEligibleCount: number;
  };
  items: HermesMsrpSourceReferenceEvidenceItem[];
}
export interface HermesMsrpStableCoverage {
  gateThreshold?: number;
  countryCount?: number;
  readyCountryCount?: number;
  blockedCountryCount?: number;
  stablePassRate?: number;
  sourceCount?: number;
  readySourceCount?: number;
  sourcePassRate?: number;
  financeObservationCandidates?: number;
  financeMonthlyPaymentCount?: number;
  financeSemanticsCounts?: Record<string, number>;
  financeTypeCounts?: Record<string, number>;
  latestRunId?: string;
  activeRunId?: string;
  activeRunRunning?: boolean;
  activeRunPartial?: boolean;
  activeRunPassRate?: number;
  probeDiffersFromStableRun?: boolean;
  readyCountries?: string[];
  blockedCountries?: string[];
  topFailureReasons?: { reason: string; count: number }[];
  probeRegressionCount?: number;
}
export interface HermesMsrpCountryProgressResponse {
  probe: string;
  overall: string;
  generatedAt: string;
  status: {
    runId?: string;
    schemaVersion?: string;
    running?: boolean;
    partial?: boolean;
    overallPassPct?: number;
    gateThreshold?: number;
    gateStatus?: string;
    stableLatestRunId?: string;
    activeRunId?: string;
    expectedCountries?: string[];
    observedCountries?: string[];
    missingCountries?: string[];
    duplicateCountries?: string[];
    financeObservationCandidates?: number;
    financeMonthlyPaymentCount?: number;
    financeSemanticsCounts?: Record<string, number>;
    financeTypeCounts?: Record<string, number>;
  };
  countries: HermesMsrpCountryProgressCountry[];
  allCountriesLatest?: HermesMsrpCountryProgressCountry[];
  stableCoverage?: HermesMsrpStableCoverage;
  topBlockingCountries?: { countryCode: string; passPct: number; reason: string; recommendedAction: string }[];
  topFailureReasons?: { reason: string; count: number }[];
  sourceRepairBacklog?: HermesMsrpSourceRepairBacklog;
  sourceReferenceEvidence?: HermesMsrpSourceReferenceEvidence;
  findings?: { type: string; severity: string; message: string }[];
}

export interface HermesMsrpDryrunHistoryRun {
  runId: string;
  mode: string;
  batch: string;
  startedAt: string;
  finishedAt: string;
  status: string;
  gateStatus: string;
  gateThreshold: number;
  passPct: number;
  total: number;
  pass: number;
  empty: number;
  fail: number;
  errors: number;
  financeObservationCandidates?: number;
  financeMonthlyPaymentCount?: number;
  expectedCountryCount: number;
  observedCountryCount: number;
  missingCountryCount: number;
  artifactPath: string;
  latestArtifactPath?: string;
  reportMdPath?: string;
  runDir?: string;
  logFile?: string;
}
export interface HermesMsrpDryrunHistoryResponse {
  schemaVersion: string;
  updatedAt: string;
  latestRunId: string;
  runs: HermesMsrpDryrunHistoryRun[];
}
