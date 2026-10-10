export interface OrderGeniusOptions {
  countryCode: string;
  paymentTermCode: string | null;
  brands: string[];
  models: string[];
  powertrains: string[];
  versions: string[];
  colours: string[];
  materialCodes: string[];
}

export interface MonthCell {
  quantity: number;
  isEditable: boolean;
  rowVersion: number;
  fobEur?: number | null;
  reason?: string;
  availableRanges?: Array<{ from: string; to: string }>;
  requiresOrderDate?: boolean;
}

export interface FobConflict {
  materialCode: string;
  countryCode: string;
  status: "conflict";
  reason: string;
  records: Array<{
    paymentTermCode: string | null;
    baseFobEur: number | null;
    colourSurchargeEur: number | null;
    finalFobEur: number;
  }>;
}

export interface MaterialSkuMatrixRow {
  materialCode: string;
  bomTemplate?: string | null;
  brand: string;
  modelName: string;
  version: string;
  colour: string;
  colourCode: string;
  colourTier?: string | null;
  colourHex?: string | null;
  interiorColorName?: string | null;
  interiorColourCode?: string | null;
  interiorPackage?: string | null;
  editionTag?: string | null;
  powertrain: string | null;
  fobEur: number | null;
  fobPeriod?: {
    periodId?: string;
    selectionDate: string;
    validFrom?: string;
    validTo?: string | null;
    baseFobEur?: number;
    colourTier?: string | null;
    surchargeEur?: number;
    surchargeSource?: string | null;
    status: string;
  } | null;
  fobConflict?: FobConflict | null;
  lifecycleStatus: string;
  editable: boolean;
  displayStyle: string | null;
  historicalBackfill?: boolean;
  priceSource?: "dated_period" | "undated_default" | "missing";
  historicalSurchargeReview?: {
    status: "matched" | "missing_rule" | "missing_evidence" | "changed";
    savedSurchargeEur: number | null;
    currentSurchargeEur: number | null;
    currentSource: string | null;
    requiresConfirmation: boolean;
  } | null;
  remark: string | null;
  effectiveFrom?: string | null;
  effectiveTo?: string | null;
  months: Record<string, MonthCell>;
  ttl: number;
}

export interface MatrixResponse {
  countryCode: string;
  countryName: string | null;
  paymentTermCode: string | null;
  year: number;
  selectionDate?: string | null;
  rows: MaterialSkuMatrixRow[];
  totalRows: number;
  fobConflicts?: FobConflict[];
}

export interface MatrixBatchResponse {
  matrices: Record<string, MatrixResponse>;
  errors: Record<string, string>;
}

export interface CountryTemplateFobPeriod {
  periodId: string;
  countryCode: string;
  bomTemplate: string;
  validFrom: string;
  validTo: string | null;
  baseFobEur: number;
  remark: string | null;
  rowVersion: number;
}

export interface FobPeriodDeletionPreview {
  deleted: boolean;
  periodId: string;
  fingerprint: string;
  lastPeriod: boolean;
  defaultBaseFobEur: number | null;
}

export interface BomTemplateLifecycleUpdateRequest {
  lifecycleStatus: "active" | "phase_out" | "historical";
  effectiveFrom?: string | null;
  effectiveTo?: string | null;
  rowVersion: number;
  previewOnly?: boolean;
}

export interface BomTemplateLifecycleUpdateResponse {
  bomTemplate: string;
  materialCodes: string[];
  effectiveFrom: string | null;
  effectiveTo: string | null;
  affectedPeriods: Array<{
    periodId: string;
    countryCode: string;
    validFrom: string;
    validTo: string | null;
    beforeTemplateStart: boolean;
    afterTemplateEnd: boolean;
    suggestedActions: string[];
  }>;
  canApply: boolean;
  lifecycleStatus?: string;
  rowVersions?: Record<string, number>;
}

export type BomLifecycleReviewKind =
  | "expiring_soon"
  | "expired_pending_archive"
  | "period_outside_lifecycle"
  | "overlapping_periods"
  | "inconsistent_template";

export interface BomLifecycleReviewItem {
  kind: BomLifecycleReviewKind;
  severity: "info" | "warning" | "error";
  bomTemplate: string;
  materialCode: string;
  countryCode: string | null;
  periodId?: string;
  effectiveFrom?: string | null;
  effectiveTo?: string | null;
  validFrom?: string;
  validTo?: string | null;
  message: string;
  messageZh: string;
  suggestedActions: string[];
}

export interface BomLifecycleReviewResponse {
  asOf: string;
  warningDays: number;
  items: BomLifecycleReviewItem[];
}

export interface QuantityCellUpdate {
  countryCode: string;
  orderYear: number;
  orderMonth: number;
  materialCode: string;
  quantity: number;
  rowVersion: number;
  includeHistorical?: boolean;
}

export interface QuantityCellResponse {
  orderQuantityCellId: string;
  countryCode: string;
  orderYear: number;
  orderMonth: number;
  materialCode: string;
  quantity: number;
  fobEur: number;
  rowVersion: number;
}

export interface RemarkUpdate {
  remark: string;
  rowVersion: number;
}

export interface RemarkResponse {
  materialCode: string;
  remark: string | null;
  rowVersion: number;
}

export interface PaymentTermRule {
  paymentTermRuleId: string;
  paymentTermCode: string;
  paymentMethod: string;
  lcDays: number;
  fobAdjustmentEur: number;
  adjustmentRate: number | null;
  isActive: boolean;
}

export interface ColourSurchargeRule {
  colourSurchargeRuleId: string;
  brand: string;
  colourType: string;
  surchargeEur: number;
  isActive: boolean;
}

export interface SpecialColourSurchargeRule {
  specialColourSurchargeRuleId: string;
  brand: string;
  modelName: string | null;
  colourCode: string;
  colourTier: "dual" | "special";
  colourName: string | null;
  surchargeEur: number;
  isActive: boolean;
}

export interface ColourHexOption {
  colourHex: string;
  skuCount: number;
}

export interface ColourNameOption {
  colourName: string;
  normalizedColourName: string;
  skuCount: number;
}

export type ColourHexRuleStatus =
  | "fillable"
  | "missing"
  | "name_conflict"
  | "swatch_conflict"
  | "complete";

export interface ColourHexRule {
  brand: string;
  colourCode: string;
  colourName: string | null;
  normalizedColourName: string | null;
  standardColourName: string | null;
  skuCount: number;
  fillableSkuCount: number;
  placeholderNameSkuCount: number;
  missingSwatchSkuCount: number;
  sampleMaterialCodes: string[];
  status: ColourHexRuleStatus;
  standardColourHex: string | null;
  nameOptions: ColourNameOption[];
  hexOptions: ColourHexOption[];
  hasNameConflict: boolean;
  hasSwatchConflict: boolean;
}

export interface ColourHexRuleSummary {
  totalRules: number;
  fillable: number;
  missing: number;
  nameConflict: number;
  swatchConflict: number;
  complete: number;
  fillableSkus: number;
  invalidIdentitySkuCount: number;
  invalidIdentitySampleMaterialCodes: string[];
}

export interface ColourHexRulePreviewItem {
  materialCode: string;
  brand: string;
  colourCode: string;
  oldColourName: string | null;
  newColourName: string;
  oldColourHex: string | null;
  newColourHex: string | null;
}

export interface ColourHexRulePreviewRule {
  brand: string;
  colourCode: string;
  colourName: string;
  colourHex: string | null;
  source: "existing_sku" | "persistent_rule";
  skuCount: number;
  hasNameConflict: boolean;
  hasSwatchConflict: boolean;
  nameOptions: ColourNameOption[];
}

export interface ColourHexRulePreview {
  rules: ColourHexRulePreviewRule[];
  items: ColourHexRulePreviewItem[];
  total: number;
  ruleCount: number;
  generatedRuleCount: number;
  unresolvedRuleCount: number;
  unresolvedConflictCount: number;
  fingerprint: string;
}

export interface ColourHexRuleApplyResult {
  updated: number;
  unchanged: number;
  rulesCreated: number;
  generatedRules: number;
  conflicts: number;
  missingRules: number;
  materialCodes: string[];
  items: ColourHexRulePreviewItem[];
  fingerprint: string;
}

export interface ColourHexRuleLookup {
  brand: string;
  colourCode: string;
  status: ColourHexRuleStatus | "none";
  colourName: string | null;
  colourHex: string | null;
  source: "persistent_rule" | "brand_code_rule" | "name_candidates" | "none";
  hasNameConflict: boolean;
  hasSwatchConflict: boolean;
  nameCandidates: Array<{
    brand: string;
    colourCode: string;
    colourName: string | null;
    colourHex: string | null;
    status: ColourHexRuleStatus;
    hasNameConflict: boolean;
    hasSwatchConflict: boolean;
  }>;
}

export type ColourTierRepriceDetailStatus = "updated" | "unchanged" | "skipped";
export type ColourTierRepriceSkipReason =
  | "manual_fob"
  | "missing_single_base"
  | "ambiguous_single_base"
  | "missing_colour_tier"
  | "missing_colour_surcharge_rule"
  | null;

export interface ColourTierRepriceDetail {
  countryCode: string;
  oldFinalFobEur: number | null;
  newFinalFobEur: number | null;
  colourSurchargeEur: number | null;
  status: ColourTierRepriceDetailStatus;
  reason: ColourTierRepriceSkipReason;
}

export interface ColourTierRepriceReport {
  materialCode: string;
  brand: string;
  colourCode: string;
  colourTier: string | null;
  surchargeEur: number | null;
  rows: number;
  updated: number;
  unchanged: number;
  skippedManual: number;
  skippedNoBase: number;
  skippedAmbiguous: number;
  skippedMissingTier: number;
  skippedMissingRule: number;
  details: ColourTierRepriceDetail[];
}

export interface ColourTierUpdateResult {
  materialCode: string;
  colourTier: string;
  reprice: ColourTierRepriceReport;
}

export type ColourSurchargeRepriceCategory =
  | "auto_reprice"
  | "already_correct"
  | "missing_base"
  | "ambiguous_base"
  | "explicit_final"
  | "missing_tier"
  | "missing_rule"
  | "single_surcharge_conflict"
  | "not_applicable";

export interface ColourSurchargeRepriceItem {
  materialCode: string;
  brand: string;
  modelName: string | null;
  version: string | null;
  bomTemplate: string | null;
  colourCode: string;
  colourName: string | null;
  colourTier: string | null;
  countryCode: string;
  paymentTermCode: string | null;
  currentBaseFobEur: number | null;
  currentColourSurchargeEur: number | null;
  currentFinalFobEur: number | null;
  currentSourceMode: string | null;
  currentUploadedFobEur: number | null;
  category: ColourSurchargeRepriceCategory;
  reason: string | null;
  trustedSingleBaseFobEur?: number | null;
  surchargeEur?: number | null;
  expectedFinalFobEur?: number | null;
  surchargeRuleStatus?: string;
  surchargeRuleSource?: string;
  singleBaseCandidates?: number[];
}

export interface ColourSurchargeRepriceSummary {
  rows: number;
  autoReprice: number;
  alreadyCorrect: number;
  missingBase: number;
  ambiguousBase: number;
  explicitFinal: number;
  missingTier: number;
  missingRule: number;
  singleSurchargeConflict: number;
  notApplicable: number;
}

export interface ColourSurchargeRepriceAudit {
  filters: {
    materialCodes: string[];
    countryCode: string | null;
  };
  fingerprint: string;
  summary: ColourSurchargeRepriceSummary;
  items: ColourSurchargeRepriceItem[];
}

export interface ColourSurchargeRepriceApplyResult {
  previewFingerprint: string;
  totals: {
    requested: number;
    updated: number;
    unchanged: number;
    skipped: number;
  };
  details: Array<{
    materialCode: string;
    countryCode: string;
    updated: number;
    skippedManual: number;
    skippedNoBase: number;
    skippedAmbiguous: number;
    skippedMissingTier: number;
    skippedMissingRule: number;
  }>;
}

export interface CountryMaterialFinanceRow {
  financeId: string | null;
  countryCode: string;
  materialCode: string;
  brand: string;
  modelName: string;
  version: string;
  powertrain: string | null;
  colour: string;
  colourCode: string;
  bomTemplate: string | null;
  bomFobEur: number | null;
  fobEur: number | null;
  retailPriceEur: number | null;
  wholesalePriceEur: number | null;
  dealerPriceEur: number | null;
  costEur: number | null;
  marginEur: number | null;
  marginRate: number | null;
  vehicleMarginEur: number | null;
  vehicleMarginRate: number | null;
  vehicleProfitEur: number | null;
  vehicleProfitRate: number | null;
  fobDeltaEur: number | null;
  marginDeltaEur: number | null;
  memo: string | null;
  sourceMode: string | null;
  sourcePayload: Record<string, unknown> | null;
  updatedBy: string | null;
  updatedAtUtc: string | null;
}

export interface CountryMaterialFinanceUpdate {
  countryCode: string;
  fobEur?: number | null;
  retailPriceEur?: number | null;
  wholesalePriceEur?: number | null;
  dealerPriceEur?: number | null;
  costEur?: number | null;
  marginEur?: number | null;
  marginRate?: number | null;
  vehicleMarginEur?: number | null;
  vehicleMarginRate?: number | null;
  vehicleProfitEur?: number | null;
  vehicleProfitRate?: number | null;
  fobDeltaEur?: number | null;
  marginDeltaEur?: number | null;
  memo?: string | null;
  sourceMode?: string;
  sourcePayload?: Record<string, unknown> | null;
}

export interface CountryMaterialFinanceImportRow {
  lineNumber: number;
  materialCode: string;
  update: CountryMaterialFinanceUpdate | null;
  error: string;
}

export interface CountryMaterialFinanceImportPreview {
  rows: CountryMaterialFinanceImportRow[];
  warnings: string[];
}

export interface CountryMaterialFinanceHistoryItem {
  historyId: string;
  financeId: string | null;
  countryCode: string;
  materialCode: string;
  oldValues: Record<string, unknown> | null;
  newValues: Record<string, unknown>;
  changedFields: string[];
  sourceMode: string | null;
  sourcePayload: Record<string, unknown> | null;
  changedBy: string | null;
  changedAtUtc: string | null;
}

export interface CountryPaymentTerm {
  countryCode: string;
  countryName: string;
  paymentTermCode: string | null;
  paymentMethod: string | null;
  lcDays: number | null;
}

export interface MaterialUploadSession {
  uploadId: string;
  fileName: string;
  totalSize: number;
  chunkSize: number;
  totalChunks: number;
  uploadedChunks: number[];
  status: string;
}

export interface MaterialUploadPreviewRow {
  rowIndex: number;
  sheetName: string;
  brand: string;
  modelName: string;
  version: string;
  exteriorColorName: string;
  exteriorColorCode: string;
  exteriorColorType: string;
  interiorColorName: string | null;
  bomTemplate: string | null;
  materialCode: string;
  baseFobEur: number | null;
  powertrain: string | null;
  warnings: string[];
}

export interface MaterialUploadPreview {
  uploadId: string;
  totalRows: number;
  newSkus: number;
  existingSkus: number;
  sheetNames: string[];
  rows: MaterialUploadPreviewRow[];
  warnings: string[];
}

export interface PublishBaselineResponse {
  baselineVersionId: string;
  baselineName: string;
  skuCount: number;
  fobCount: number;
  status: string;
}

export interface BaselineVersion {
  baselineVersionId: string;
  baselineName: string;
  sourceFileName: string;
  status: string;
  publishedBy: string | null;
  publishedAtUtc: string | null;
  createdAtUtc: string;
}

export interface QuantityImportCell {
  month: number;
  oldQuantity: number | null;
  newQuantity: number;
  error: string;
  rowVersion: number;
}

export interface QuantityImportRow {
  materialCode: string;
  modelName: string;
  version: string;
  colour: string;
  excelFob: number | null;
  systemFob: number | null;
  fobChanged: boolean;
  lifecycleStatus: string;
  cells: QuantityImportCell[];
  rowErrors: string[];
}

export interface QuantityImportFobChange {
  materialCode: string;
  excelFob: number;
  systemFob: number;
}

export interface QuantityImportNewRow {
  materialCode: string;
  modelName: string;
  version: string;
  colour: string;
  reason: string;
}

export interface QuantityImportPreview {
  importId: string;
  countryCode: string;
  year: number;
  matchedRows: QuantityImportRow[];
  newRows: QuantityImportNewRow[];
  fobChanges: QuantityImportFobChange[];
  totalCells: number;
  errorCells: number;
  errors: string[];
  status: "ok" | "warning" | "error";
}

export interface QuantityImportResult {
  importId: string;
  status: string;
  appliedCells: number;
  skippedCells: number;
  errors: string[];
}
