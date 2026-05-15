// Hermes Governance Layer — TypeScript types

// === Markdown Diagrams ===
export interface HermesMermaidBlock {
  file: string;
  title: string;
  diagramIndex: number;
  raw: string;
  type: string;
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
