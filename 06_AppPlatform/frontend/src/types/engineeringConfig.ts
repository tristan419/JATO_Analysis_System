export interface FeatureCatalogItem {
  featureId: string; seq: number; category: string;
  standardFieldName: string; featureCode: string; unit: string | null;
  dataType: string; aliases: string[] | null; displayOrder: number; isActive: boolean;
}

export interface VehicleTrimItem {
  trimId: string; brand: string; modelName: string; trimName: string;
  fullTrimName: string; energyType: string | null; drivetrain: string | null;
  engine: string | null; modelYear: string | null; status: string;
  market?: string | null; country?: string | null; vehicleCode?: string | null;
  materialNo?: string | null; identityKey?: string | null; salesVersion?: string | null;
  sourceUploadId?: string | null; sourceFileName?: string | null; sourceFilePath?: string | null; sourceCreatedBy?: string | null; sourceCreatedAt?: string | null;
  importStatus?: string | null; hasMaterialNo?: boolean; dataOrigin?: "own_catalog" | "external_or_scraped" | string;
}

export interface EngineeringConfigCompetitorMatchEvidence {
  field: string;
  label?: string;
  target?: string | number;
  candidate?: string | number;
  score?: number;
  detail?: string;
}

export interface EngineeringConfigCompetitorRecommendation {
  rank: number;
  sourceRank: number;
  modelName: string;
  brand?: string | null;
  profile: Record<string, unknown>;
  role?: string | null;
  similarityScore?: number | null;
  salesTarget?: number | null;
  salesBase?: number | null;
  deltaVolume?: number | null;
  shareTarget?: number | null;
  shareChange?: number | null;
  pureShareShift?: number | null;
  estimatedFlow?: number | null;
  sharedDimensions: string[];
  matchEvidence: EngineeringConfigCompetitorMatchEvidence[];
  recommendationReason: string;
  configAvailable: boolean;
  configTrimCount: number;
  sourceDigestAvailable?: boolean;
  sourceDigestSourceCount?: number;
  sourceDigestGroupCount?: number;
  sourceDigestTrimCount?: number;
  sourceDigestSearchQuery?: string | null;
  sourceDigestMatches?: Array<{
    sourceId: string;
    sourceFileName?: string | null;
    groupCount: number;
    trimCount: number;
  }>;
  trims: VehicleTrimItem[];
  nextAction: "select_config_trim" | "create_from_source_digest" | "upload_source" | string;
}

export interface EngineeringConfigCompetitorRecommendationResponse {
  country: string | null;
  modelName: string | null;
  powertrain?: string | null;
  segment?: string | null;
  rows: number;
  items: EngineeringConfigCompetitorRecommendation[];
  message?: string | null;
  errorMessage?: string | null;
  source?: {
    type?: string;
    analysisMode?: string | null;
    targetPeriod?: string | null;
    basePeriod?: string | null;
    scopeModelCount?: number | null;
    advancedAnalysisCountry?: string | null;
    advancedAnalysisSegment?: string | null;
  };
}

export interface TrimFeatureValueItem {
  valueId: string; featureId: string; featureCode: string | null; featureName: string;
  rawValue: string; normalizedValue: string | null;
  availability: AvailabilityState; unit: string | null; version: number;
}

export type AvailabilityState =
  "STANDARD" | "OPTIONAL" | "NOT_AVAILABLE" | "NOT_APPLICABLE" | "VALUE" | "UNKNOWN" | "CANCELLED_OR_REMOVED";

export type ComparisonType =
  "COMMON_SAME"
  | "DIFFERENT_VALUE"
  | "UNIQUE_TO_TRIM"
  | "PARTIAL_AVAILABLE"
  | "MISSING_OR_UNKNOWN"
  | "MISSING_UNKNOWN"
  | "NOT_APPLICABLE"
  | "CANCELLED_OR_REMOVED"
  | "AVAILABILITY_DIFFERENT"
  | "OPTIONAL_DIFFERENT"
  | "UNIQUE_OR_PARTIAL";

export type ConfigValueState =
  "marker_value"
  | "blank"
  | "not_applicable"
  | "cancelled_or_removed"
  | "text_value"
  | "numeric_value";

export interface TrimDetail {
  trim: VehicleTrimItem; featuresByCategory: Record<string, TrimFeatureValueItem[]>; categoryCount: number;
}

export interface CompareRow {
  category: string; featureCode: string; featureName: string; featureId?: string | null;
  comparisonType: ComparisonType; uniqueTrimIds: string[]; businessNote: string;
  values: (CompareCellValue | null)[];
}

export interface CompareCellValue {
  valueId?: string | null; rawValue: string; normalizedValue: string | null;
  availability: AvailabilityState; unit: string | null;
  version?: number | null;
  valueState?: ConfigValueState;
  displayValue?: string | null;
  inferred?: boolean;
  inferenceReason?: string | null;
  confidence?: number | null;
  manualOverride?: boolean;
  source?: EngineeringConfigSourceCellRef | null;
}

export interface CompareSummary {
  totalFeatures: number; shownFeatures: number; commonSameCount: number;
  differentValueCount: number; uniqueFeatureCount: number;
  partialAvailableCount: number; uniqueOrPartialCount?: number; missingOrUnknownCount: number;
  confirmedDifferenceCount?: number; valueDifferentCount?: number;
  rawConfirmedDifferenceCount?: number; inferredDifferenceCount?: number;
  availabilityDifferentCount?: number; optionalDifferentCount?: number;
  notApplicableCount?: number; cancelledOrRemovedCount?: number;
  differenceCount: number; categoryCounts?: Record<string, number>; differenceCategories: string[];
}

export interface EngineeringConfigTrimProfile {
  country?: string | null;
  configurationVersion?: string | null;
  materialNo?: string | null;
  familyIdentifier?: string | null;
  variantVersion?: string | null;
  [key: string]: string | null | undefined;
}

export interface CompareTrimItem {
  trimId: string; fullTrimName: string; brand: string; modelName: string;
  trimName?: string | null; market?: string | null; country?: string | null;
  modelYear?: string | null; energyType?: string | null; drivetrain?: string | null;
  engine?: string | null; vehicleCode?: string | null; materialNo?: string | null;
  identityKey?: string | null; salesVersion?: string | null;
  sourceUploadId?: string | null; sourceFileName?: string | null; sourceFilePath?: string | null; sourceCreatedBy?: string | null; sourceCreatedAt?: string | null;
  importStatus?: string | null; hasMaterialNo?: boolean; dataOrigin?: "own_catalog" | "external_or_scraped" | string;
  profile?: EngineeringConfigTrimProfile | null;
  msrp?: number | null; targetPrice?: number | null;
  configVersionId?: string | null;
  configVersionNo?: number | null;
  configVersionStatus?: "draft" | "published" | "archived" | "legacy" | string;
  draftVersionAvailable?: boolean;
  publishedVersionAvailable?: boolean;
  versionFallback?: boolean;
}

export interface CompareGroup {
  category: string; items: CompareRow[];
}

export interface CompareResponse {
  versionScope?: EngineeringConfigVersionScope;
  usesDraft?: boolean;
  versionFallbackCount?: number;
  trims: CompareTrimItem[];
  summary?: CompareSummary; groups?: CompareGroup[];
  rows: CompareRow[]; totalFeatures: number; shownFeatures: number;
}

export type EngineeringConfigVersionScope = "published" | "latest";

export interface EngineeringConfigCompareFactFilters {
  deltaFilter?:
    | "ALL"
    | "DIFFERENCE"
    | "ADDED"
    | "REMOVED"
    | "VALUE_CHANGED"
    | "OPTIONAL_CHANGED"
    | "INFERRED"
    | "MISSING_SOURCE"
    | "MERGED_SOURCE"
    | "UNKNOWN"
    | "COMMON";
  category?: string | null;
  search?: string | null;
  targetTrimId?: string | null;
  includeBusinessSummary?: boolean;
  forceRefresh?: boolean;
}

export interface EngineeringConfigCompareFactRequest {
  trimIds: string[];
  baseTrimId: string;
  versionScope: EngineeringConfigVersionScope;
  factSource?: {
    kind: "local_workbook_digest";
    fileName: string;
    groupId: string;
  };
  filters?: EngineeringConfigCompareFactFilters;
}

export type EngineeringConfigBusinessSummaryComposeRequest = EngineeringConfigCompareFactRequest;
export type EngineeringConfigCompareExportRequest = EngineeringConfigCompareFactRequest;

export interface EngineeringConfigBusinessSummaryEvidenceRef {
  section: "mainUpgrades" | "replacementsOrReductions" | "evidenceStatus" | string;
  itemIndex: number;
  evidenceKey: string;
  featureCode?: string;
  category?: string;
  reason?: string;
}

export interface EngineeringConfigBusinessSummaryItem {
  targetTrimId: string;
  targetLabel: string;
  headline: string;
  mainUpgrades: string[];
  replacementsOrReductions: string[];
  evidenceStatus: string[];
  evidenceRefs?: EngineeringConfigBusinessSummaryEvidenceRef[];
  evidenceBoundClaimCount?: number;
  unsupportedEvidenceCount?: number;
  recommendedUse: string;
}

export interface EngineeringConfigBusinessSummaryUsage {
  provider: string;
  model: string;
  status: "ok" | "missing_key" | "failed" | string;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  promptCacheHitTokens?: number;
  promptCacheMissTokens?: number;
  cacheHit?: boolean;
  finishReason?: string;
  estimated?: boolean;
  fallbackReason?: string;
}

export interface EngineeringConfigBusinessSummaryComposeResponse {
  summaries: EngineeringConfigBusinessSummaryItem[];
  usage: EngineeringConfigBusinessSummaryUsage;
}

export interface EngineeringConfigBusinessSummaryReadiness {
  ready: boolean;
  status: "ready" | "missing_key" | "failed" | string;
  provider: string;
  model: string;
  apiBase?: string | null;
  keySource: string;
  providerConfigured?: boolean;
  runtimeUrl?: string | null;
  runtimeUsed?: boolean;
  runtimeStatus?: string | null;
  liveCheck?: string | null;
  cacheSize: number;
  cacheLimit: number;
  pipeline: "compare_runtime_compose" | string;
  persisted: boolean;
  message: string;
  notes: string[];
}

export interface ParsePreview {
  uploadId: string;
  summary: { categoryCount: number; featureCount: number; trimCount: number; valueRecordCount: number };
  trims: { brand: string; model_name: string; trim_name: string; full_trim_name: string }[];
  categories: string[]; warningCount: number; warnings: string[];
  unmatchedFeatures: { category: string; fieldName: string }[];
  sampleValues: Record<string, unknown>[];
}

export interface EngineeringConfigSourceContext {
  brand: string | null;
  model: string | null;
  market: string | null;
  country?: string | null;
  powertrain?: string | null;
  segment?: string | null;
  modelYear: string | null;
  trimIds: string[];
  salesVersionIds?: string[];
  contextType?: "compare" | "upload_page" | "import_review" | string;
  scenario?: string | null;
  identityAnchor?: string | null;
}

export interface EngineeringConfigSourceContextRelation {
  id: string;
  sourceId: string;
  batchId: string;
  brand: string | null;
  model: string | null;
  market: string | null;
  country: string | null;
  powertrain?: string | null;
  segment?: string | null;
  modelYear: string | null;
  trimIds: string[];
  salesVersionIds: string[];
  contextType: string;
  scenario?: string | null;
  identityAnchor?: string | null;
  status?: "active" | "trashed" | "purged" | string;
  createdBy: string | null;
  createdAt: string | null;
  relatedContext: EngineeringConfigSourceContext;
}

export interface EngineeringConfigSourceDigestValue {
  valueId: string;
  rawValue: string;
  normalizedValue: string | null;
  availability: AvailabilityState;
  unit: string | null;
  valueState?: ConfigValueState;
  displayValue?: string | null;
  inferred?: boolean;
  inferenceReason?: string | null;
  confidence?: number | null;
  source?: EngineeringConfigSourceCellRef | null;
}

export interface EngineeringConfigSourceCellRef {
  sheetName: string;
  rowNumber: number;
  columnNumber: number;
  columnLetter: string;
  cell: string;
  sourceCell?: string | null;
  mergedRange?: string | null;
  inferenceReason?: string | null;
  confidence?: number | null;
  sourceType?: "pdf_text" | "pdf_ocr" | "image_ocr" | string;
  pageNumber?: number;
  ocrEngine?: string | null;
}

export interface EngineeringConfigSourceDigestRow {
  category: string;
  featureKey?: string;
  featureCode: string;
  featureName: string;
  comparisonType: ComparisonType;
  uniqueTrimIds: string[];
  businessNote: string;
  reviewFlags?: string[];
  reviewNotes?: string[];
  values: Array<EngineeringConfigSourceDigestValue | null>;
}

export interface EngineeringConfigSourceDigestTrim {
  trimId: string;
  trimName: string;
  fullTrimName: string;
  modelName: string;
  sourceSheet: string;
  identityStatus?: "temporary_ocr_column" | string;
  identityNote?: string | null;
  market?: string | null;
  country?: string | null;
  materialNo?: string | null;
  salesVersion?: string | null;
  powertrain?: string | null;
  energyType?: string | null;
  energy_type?: string | null;
  drivetrain?: string | null;
  engine?: string | null;
  fuel?: string | null;
  fuelType?: string | null;
  fuel_type?: string | null;
  hasMaterialNo?: boolean;
  dataOrigin?: "own_catalog" | "external_or_scraped" | string;
  sourceStatus?: "active" | "cancelled" | string;
  profile?: EngineeringConfigTrimProfile;
  sourceRow?: number;
  sourceColumn?: number;
}

export interface EngineeringConfigSourceDigestGroup {
  groupId: string;
  title: string;
  sourceSheet: string;
  modelName: string;
  trimCount: number;
  featureCount: number;
  differenceCount: number;
  sourceKind?: "price_list" | string;
  identityStatus?: "temporary_ocr_column" | string;
  identityNote?: string | null;
  trims: EngineeringConfigSourceDigestTrim[];
  rows: EngineeringConfigSourceDigestRow[];
  summary: CompareSummary;
}

export interface EngineeringConfigSourceDigestSheet {
  name: string;
  rowCount: number;
  columnCount: number;
  nonEmptyCellCount: number;
  sampleRows: string[][];
}

export interface EngineeringConfigOcrCandidateScore {
  semanticScore?: number;
  comparableGroupCount?: number;
  featureCount?: number;
  differenceCount?: number;
  candidateTrimCount?: number;
  totalFeatureCount?: number;
  totalDifferenceCount?: number;
  totalCandidateTrimCount?: number;
  tableShapeScore: number;
  rowCount: number;
  columnCount: number;
  nonEmptyCount: number;
}

export interface EngineeringConfigOcrCandidate {
  engine: string;
  sourceType?: "pdf_ocr" | "image_ocr" | string;
  sheetName?: string;
  selected?: boolean;
  comparableTableDetected?: boolean;
  score?: EngineeringConfigOcrCandidateScore;
  pageNumber?: number;
  message?: string | null;
  textPreview?: string | null;
  lineCount?: number | null;
}

export interface EngineeringConfigOcrEvaluation {
  strategy: "highest_config_semantic_score" | "highest_table_score" | string;
  reason?: "highest_config_semantic_score" | "highest_table_score" | "no_comparable_table_detected" | string;
  candidateCount: number;
  comparableCandidateCount: number;
  selectedCandidateCount: number;
  selectedEngine?: string | null;
  selectedEngines?: string[];
  selectedScore?: EngineeringConfigOcrCandidateScore | null;
  selectedSheetName?: string | null;
  selectedPageNumber?: number | null;
  selectedReasonDetails?: string[];
}

export interface EngineeringConfigOcrReadinessComponent {
  name: string;
  available: boolean;
  detail: string;
}

export interface EngineeringConfigOcrReadiness {
  status: "ready" | "degraded" | "not_configured" | string;
  ready: boolean;
  defaultEngine: string | null;
  imageOcrReady: boolean;
  pdfOcrReady: boolean;
  pdfRenderReady: boolean;
  paddleOcrReady: boolean;
  legacyOcrReady: boolean;
  configuredLanguage: string;
  components: EngineeringConfigOcrReadinessComponent[];
  warnings: string[];
  notes: string[];
}

export interface EngineeringConfigSourceDigest {
  digestType: "workbook" | "tabular" | "pdf_text" | "pdf_ocr" | "image_ocr" | "unavailable" | string;
  workbookFormat?: "eu_config_resource_table" | string;
  status: "ready" | "pending" | "failed" | string;
  sourceFormat?: "pdf_text" | "pdf_ocr" | "image_ocr" | string;
  ocrEngine?: string | null;
  ocrEngineCandidates?: EngineeringConfigOcrCandidate[];
  ocrEvaluation?: EngineeringConfigOcrEvaluation | null;
  fileName: string;
  modelName: string | null;
  summary: {
    sheetCount: number;
    tableCount: number;
    candidateTrimCount: number;
    comparableGroupCount: number;
    featureCount: number;
    differenceCount: number;
  };
  sheets: EngineeringConfigSourceDigestSheet[];
  compareGroups: EngineeringConfigSourceDigestGroup[];
  errorMessage?: string | null;
  message?: string | null;
}

export interface EngineeringConfigSourceDigestStatus {
  digestType: "workbook" | "tabular" | "pdf_text" | "pdf_ocr" | "image_ocr" | "unavailable" | string;
  status: "ready" | "pending" | "failed" | string;
  sourceFormat?: "pdf_text" | "pdf_ocr" | "image_ocr" | string;
  ocrEngine?: string | null;
  ocrEngineCandidates?: EngineeringConfigOcrCandidate[];
  ocrEvaluation?: EngineeringConfigOcrEvaluation | null;
  summary?: Partial<EngineeringConfigSourceDigest["summary"]>;
  errorMessage?: string | null;
  message?: string | null;
}

export interface EngineeringConfigSourceSnapshot {
  sourceId: string;
  batchId: string;
  importBatchId: string;
  importType?: "source_snapshot" | string;
  assetType?: "source_snapshot" | string;
  uploadType: "source_snapshot";
  sourceFileName: string;
  fileType: string;
  mimeType: string;
  fileSize: number;
  sourceFileHash: string | null;
  sourceFilePath: string;
  uploadStatus: "registered" | "duplicate" | "trashed" | "purged" | string;
  libraryStatus?: "stored" | "trashed" | "purged" | string;
  inTrash?: boolean;
  sourceSearchMatches?: string[];
  extractStatus: "pending" | "digest_ready" | "not_applicable" | string;
  nextAction: "extractor_pending" | "review_digest" | "matrix_import" | string;
  createdBy: string | null;
  createdAt: string | null;
  errorMessage: string | null;
  duplicate?: boolean;
  deduplicated?: boolean;
  linkedToCurrentContext?: boolean;
  relatedContext: EngineeringConfigSourceContext;
  contexts: EngineeringConfigSourceContextRelation[];
  sourceDigestStatus?: EngineeringConfigSourceDigestStatus | null;
  sourceDigest?: EngineeringConfigSourceDigest | null;
}

export interface EngineeringConfigSourceSnapshotList {
  rows: number;
  items: EngineeringConfigSourceSnapshot[];
}

export interface EngineeringConfigDigestTrimIdentityOverride {
  trimId: string;
  brand?: string | null;
  modelName?: string | null;
  trimName?: string | null;
  fullTrimName?: string | null;
  market?: string | null;
  country?: string | null;
  modelYear?: string | null;
  energyType?: string | null;
  drivetrain?: string | null;
  engine?: string | null;
  materialNo?: string | null;
  salesVersion?: string | null;
}

export interface EngineeringConfigDigestDraftResult {
  sourceId: string;
  groupId: string;
  importBatchId: string;
  sourceFileName?: string | null;
  groupTitle?: string | null;
  sourceDigestType?: "workbook" | "tabular" | "pdf_text" | "pdf_ocr" | "image_ocr" | "unavailable" | string | null;
  sourceFormat?: "pdf_text" | "pdf_ocr" | "image_ocr" | string | null;
  sourceKind?: "price_list" | "config_matrix" | string | null;
  ocrEngine?: string | null;
  ocrEngineCandidates?: EngineeringConfigOcrCandidate[];
  ocrEvaluation?: EngineeringConfigOcrEvaluation | null;
  trimIds: string[];
  compareTrimIds: string[];
  trimCount: number;
  createdTrimCount: number;
  reusedTrimCount: number;
  featureCount: number;
  createdFeatureCount: number;
  reusedFeatureCount: number;
  aliasMatchedFeatureCount?: number;
  semanticAliasMatchedFeatureCount?: number;
  featureMatchReasonCounts?: Record<string, number>;
  featureMatchSamples?: Array<{
    sourceFeatureName: string;
    matchedFeatureName: string;
    matchedFeatureCode: string;
    matchReason: string;
  }>;
  valueRecordCount: number;
  insertedValueCount: number;
  updatedValueCount: number;
  createdVersionIds: string[];
}

export interface AuditLogItem {
  auditId: string; entityType: string; entityId: string; fieldName: string;
  oldValue: string | null; newValue: string | null; changedBy: string | null;
  changedAtUtc: string; source: string; comment: string | null;
}
