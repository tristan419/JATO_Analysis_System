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
  lifecycleStatus: string;
  editable: boolean;
  displayStyle: string | null;
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
  rows: MaterialSkuMatrixRow[];
  totalRows: number;
}

export interface MatrixBatchResponse {
  matrices: Record<string, MatrixResponse>;
  errors: Record<string, string>;
}

export interface QuantityCellUpdate {
  countryCode: string;
  orderYear: number;
  orderMonth: number;
  materialCode: string;
  quantity: number;
  rowVersion: number;
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
}

export interface ColourHexRulePreviewItem {
  materialCode: string;
  brand: string;
  colourCode: string;
  oldColourName: string | null;
  newColourName: string;
  oldColourHex: string | null;
  newColourHex: string;
}

export interface ColourHexRulePreview {
  items: ColourHexRulePreviewItem[];
  total: number;
  ruleCount: number;
  fingerprint: string;
}

export interface ColourHexRuleApplyResult {
  updated: number;
  unchanged: number;
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
  source: "brand_code_rule" | "none";
  hasNameConflict: boolean;
  hasSwatchConflict: boolean;
}

export type ColourTierRepriceDetailStatus = "updated" | "unchanged" | "skipped";
export type ColourTierRepriceSkipReason = "manual_fob" | "missing_single_base" | null;

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
  colourTier: string;
  surchargeEur: number;
  rows: number;
  updated: number;
  unchanged: number;
  skippedManual: number;
  skippedNoBase: number;
  details: ColourTierRepriceDetail[];
}

export interface ColourTierUpdateResult {
  materialCode: string;
  colourTier: string;
  reprice: ColourTierRepriceReport;
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
