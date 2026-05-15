export interface FeatureCatalogItem {
  featureId: string; seq: number; category: string;
  standardFieldName: string; featureCode: string; unit: string | null;
  dataType: string; aliases: string[] | null; displayOrder: number; isActive: boolean;
}

export interface VehicleTrimItem {
  trimId: string; brand: string; modelName: string; trimName: string;
  fullTrimName: string; energyType: string | null; drivetrain: string | null;
  engine: string | null; modelYear: string | null; status: string;
}

export interface TrimFeatureValueItem {
  valueId: string; featureCode: string | null; featureName: string;
  rawValue: string; normalizedValue: string | null;
  availability: AvailabilityState; unit: string | null; version: number;
}

export type AvailabilityState =
  "STANDARD" | "OPTIONAL" | "NOT_AVAILABLE" | "NOT_APPLICABLE" | "VALUE" | "UNKNOWN";

export interface TrimDetail {
  trim: VehicleTrimItem; featuresByCategory: Record<string, TrimFeatureValueItem[]>; categoryCount: number;
}

export interface CompareRow {
  category: string; featureCode: string; featureName: string;
  values: (CompareCellValue | null)[];
}

export interface CompareCellValue {
  valueId: string; rawValue: string; normalizedValue: string | null;
  availability: AvailabilityState; unit: string | null;
}

export interface CompareResponse {
  trims: { trimId: string; fullTrimName: string; brand: string; modelName: string }[];
  rows: CompareRow[]; totalFeatures: number; shownFeatures: number;
}

export interface ParsePreview {
  uploadId: string;
  summary: { categoryCount: number; featureCount: number; trimCount: number; valueRecordCount: number };
  trims: { brand: string; model_name: string; trim_name: string; full_trim_name: string }[];
  categories: string[]; warningCount: number; warnings: string[];
  unmatchedFeatures: { category: string; fieldName: string }[];
  sampleValues: Record<string, unknown>[];
}

export interface AuditLogItem {
  auditId: string; entityType: string; entityId: string; fieldName: string;
  oldValue: string | null; newValue: string | null; changedBy: string | null;
  changedAtUtc: string; source: string; comment: string | null;
}
