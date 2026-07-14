export type MonitoringStatus =
  | "pending"
  | "active"
  | "degraded"
  | "manual_evidence_required"
  | "paused"
  | "retired";

export type RepairDomain =
  | "source"
  | "parser"
  | "semantic"
  | "result"
  | "mapping"
  | "fx"
  | "runtime";

export type GateStatus = "pass" | "fail" | "not_applicable";

export interface GateResult {
  status: GateStatus;
  reasons: string[];
  policyVersion: string;
}

export interface GateDecision {
  schemaVersion: "1.0";
  targetId: string;
  observationId: string;
  sourceGate: GateResult;
  mappingGate: GateResult;
  fxGate: GateResult | null;
  eligibleForLocalMaterialization: boolean;
  eligibleForNormalizedMaterialization: boolean;
  evaluatedAt: string;
}

export interface PersistedGateDecision extends GateDecision {
  gateDecisionId: string;
  evaluationContext: Record<string, unknown> | null;
  createdBy: string;
  createdAtUtc: string;
}

export interface MonitoringTarget {
  targetId: string;
  targetKey: string;
  country: string;
  brand: string;
  model: string;
  trimScope: string | null;
  powertrainScope: string | null;
  rosterType: "country_top30" | "manual" | "future_roster";
  rosterRank: number | null;
  monitoringStatus: MonitoringStatus;
  activeSourceVersionId: string | null;
  fallbackSourceVersionId: string | null;
  schedule: Record<string, unknown> | null;
  owner: string | null;
  notes: string | null;
  rowVersion: number;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface MonitoringTargetListItem extends MonitoringTarget {
  gateSummary: PersistedGateDecision | null;
  openCaseCount: number;
  manualEvidenceCaseCount: number;
}

export interface EvidenceReference {
  evidenceAssetId: string;
  sha256: string;
  evidenceType: string;
}

export interface EvidenceAsset extends EvidenceReference {
  targetId: string | null;
  sourceId: string | null;
  repairCaseId: string | null;
  sourceUrl: string | null;
  finalUrl: string | null;
  redirectChain: string[];
  officialDomainVerified: boolean;
  filename: string | null;
  mimeType: string | null;
  mimeSignature: string | null;
  sizeBytes: number | null;
  storageKey: string | null;
  capturedAtUtc: string;
  documentDate: string | null;
  validFrom: string | null;
  validUntil: string | null;
  pageCount: number | null;
  contentHash: string | null;
  textHash: string | null;
  sourceType: string;
  semanticLane: string;
  lifecycleState: string;
  createdBy: string;
  createdAtUtc: string;
}

export interface SourceVersion {
  sourceVersionId: string;
  sourceId: string;
  targetId: string;
  versionNumber: number;
  profile: Record<string, unknown>;
  profileYaml: string;
  profileSha256: string;
  evidenceRefs: EvidenceReference[];
  extractorName: string;
  extractorType: string;
  extractorVersion: string;
  semanticLane: string;
  currency: string;
  taxMode: string;
  validFrom: string | null;
  validUntil: string | null;
  previousVersionId: string | null;
  validationSummary: Record<string, unknown> | null;
  dryrunSummary: Record<string, unknown> | null;
  replaySummary: Record<string, unknown> | null;
  conflictSummary: Record<string, unknown> | null;
  gateResult: GateDecision | null;
  versionStatus: string;
  createdBy: string;
  approvedBy: string | null;
  approvedAtUtc: string | null;
  publishedAtUtc: string | null;
  decisionReason: string | null;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface RepairCase {
  caseId: string;
  repairDomain: RepairDomain;
  targetId: string | null;
  sourceId: string | null;
  observationId: string | null;
  mappingReference: string | null;
  fxRunId: string | null;
  caseType: string;
  failureClassifier: string;
  severity: "low" | "medium" | "high" | "critical";
  priority: number;
  firstSeenAtUtc: string;
  lastSeenAtUtc: string;
  occurrenceCount: number;
  recentRunIds: string[];
  evidenceRefs: EvidenceReference[];
  manualEvidenceRequired: boolean;
  agentRunRefs: string[];
  proposalRefs: string[];
  caseStatus: string;
  resolution: Record<string, unknown> | null;
  recurrenceOfCaseId: string | null;
  owner: string | null;
  createdBy: string;
  rowVersion: number;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface RepairProposal {
  proposalId: string;
  caseId: string;
  targetId: string | null;
  sourceId: string | null;
  sourceVersionId: string | null;
  proposalOrigin: "manual" | "deterministic" | "hermes_agent";
  proposalType: string;
  agentRunId: string | null;
  agentStepId: string | null;
  dpv4Metadata: Record<string, unknown> | null;
  inputEvidenceRefs: EvidenceReference[];
  proposedChange: Record<string, unknown>;
  fieldDiff: Array<Record<string, unknown>>;
  assumptions: string[];
  unresolvedQuestions: string[];
  riskFlags: string[];
  validationResult: Record<string, unknown> | null;
  dryrunResult: Record<string, unknown> | null;
  replayResult: Record<string, unknown> | null;
  conflictResult: Record<string, unknown> | null;
  gateResult: GateDecision | null;
  proposalStatus: string;
  author: string;
  reviewer: string | null;
  reviewedAtUtc: string | null;
  decisionReason: string | null;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface ResultCorrection {
  correctionDecisionId: string;
  originalObservationId: string;
  gateDecisionId: string;
  originalCurrentPriceId: string | null;
  originalPriceHistoryId: string | null;
  correctionType: string;
  reason: string;
  evidenceRefs: EvidenceReference[];
  sourceVersionId: string | null;
  correctedInputs: Record<string, unknown>;
  replayResult: Record<string, unknown> | null;
  gateResult: PersistedGateDecision;
  replacementObservationId: string | null;
  rematerializationRefs: string[];
  decisionStatus: string;
  createdBy: string;
  approvedBy: string | null;
  approvedAtUtc: string | null;
  createdAtUtc: string;
  updatedAtUtc: string;
}

export interface FxNormalizationRun {
  fxRunId: string;
  observationId: string;
  gateDecisionId: string;
  localCurrency: string;
  localValue: string;
  fxProvider: string;
  rateToNormalized: string;
  rateEffectiveDate: string;
  rateRetrievedAtUtc: string;
  policyVersion: string;
  normalizedCurrency: string;
  normalizedValue: string;
  gateResult: PersistedGateDecision;
  runStatus: string;
  failureReason: string | null;
  decisionReason: string | null;
  supersededRunId: string | null;
  createdBy: string;
  approvedBy: string | null;
  approvedAtUtc: string | null;
  createdAtUtc: string;
}

export interface TargetListResponse {
  rows: number;
  total: number;
  items: MonitoringTargetListItem[];
}

export interface TargetDetailResponse {
  item: MonitoringTarget;
  gateSnapshot: PersistedGateDecision | null;
  evidence: EvidenceAsset[];
  sourceVersions: SourceVersion[];
  repairCases: RepairCase[];
  resultCorrections: ResultCorrection[];
  fxRuns: FxNormalizationRun[];
}

export interface RepairCaseDetailResponse {
  item: RepairCase;
  proposals: RepairProposal[];
}

export interface TargetFilters {
  country: string;
  brand: string;
  monitoringStatus: string;
  rosterType: string;
}

export interface MonitoringTargetCreate {
  country: string;
  brand: string;
  model: string;
  trimScope?: string;
  powertrainScope?: string;
  rosterType: "country_top30" | "manual" | "future_roster";
  rosterRank?: number;
  owner?: string;
  notes?: string;
}

export interface UrlEvidenceCreate {
  sourceId?: string;
  repairCaseId?: string;
  sourceUrl: string;
  finalUrl?: string;
  officialDomain: string;
  sourceType: string;
  semanticLane: string;
}

export interface UploadSession {
  uploadSessionId: string;
  targetId: string;
  completedEvidenceAssetId: string | null;
  chunkSizeBytes: number;
  totalParts: number;
  receivedParts: Array<{
    partNumber: number;
    sizeBytes: number;
    sha256: string;
    receivedAtUtc: string;
  }>;
  uploadStatus: string;
  rowVersion: number;
}

export interface PdfEvidenceUpload {
  targetId: string;
  sourceId?: string;
  repairCaseId?: string;
  sourceUrl: string;
  officialDomain: string;
  sourceType: string;
  semanticLane: string;
  file: File;
  onProgress?: (uploadedParts: number, totalParts: number) => void;
}

export interface HermesDiagnosisRequest {
  sourceGateSnapshot: GateResult;
  mappingGateSnapshot: GateResult;
  fxGateSnapshot?: GateResult;
  allowedToolIds: string[];
  authorityPolicyVersion: string;
  composerPolicyVersion: string;
  attemptBudget: number;
  timeBudgetSeconds: number;
  tokenBudget: number;
  costBudgetUsd: string;
}
