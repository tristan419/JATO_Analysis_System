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
  brand: string;
  modelName: string;
  version: string;
  colour: string;
  colourCode: string;
  powertrain: string | null;
  fobEur: number | null;
  lifecycleStatus: string;
  editable: boolean;
  displayStyle: string | null;
  remark: string | null;
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

export interface CountryPaymentTerm {
  countryCode: string;
  countryName: string;
  paymentTermCode: string;
  paymentMethod: string;
  lcDays: number;
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
