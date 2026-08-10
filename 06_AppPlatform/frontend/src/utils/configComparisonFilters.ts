import type {
  AvailabilityState,
  CompareCellValue,
  CompareResponse,
  CompareRow,
  ComparisonType,
} from "../types/engineeringConfig";
import { rowDeltasForBase, type ConfigDelta, type ConfigDeltaType } from "./configDelta";

export type ConfigComparisonDeltaFilter =
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

const AVAILABILITY_LABELS: Record<AvailabilityState, string> = {
  STANDARD: "标配",
  OPTIONAL: "选装",
  NOT_AVAILABLE: "不配备",
  NOT_APPLICABLE: "不适用",
  VALUE: "参数",
  UNKNOWN: "未知",
  CANCELLED_OR_REMOVED: "取消 / 删除",
};

export function fallbackComparisonType(row: CompareRow): ComparisonType {
  if (row.comparisonType === "MISSING_OR_UNKNOWN") return "MISSING_UNKNOWN";
  if (row.comparisonType) return row.comparisonType;
  const nonNullValues = row.values.filter((value): value is CompareCellValue => value !== null);
  if (nonNullValues.length !== row.values.length || nonNullValues.some((value) => value.availability === "UNKNOWN")) {
    return "MISSING_UNKNOWN";
  }
  const signatures = new Set(nonNullValues.map((value) => `${value.availability}:${value.normalizedValue ?? ""}:${value.rawValue}`));
  return signatures.size <= 1 ? "COMMON_SAME" : "DIFFERENT_VALUE";
}

export function cellText(cell: CompareCellValue | null): string {
  if (cell === null) return "待确认";
  if (cell.displayValue) return cell.displayValue;
  if (cell.valueState === "blank") return "待确认";
  if (cell.availability === "NOT_AVAILABLE") return cell.inferred ? "不配备*" : "不配备";
  if (cell.valueState === "not_applicable") return "不适用";
  if (cell.valueState === "cancelled_or_removed") return cell.rawValue || "取消 / 删除";
  if (cell.availability === "STANDARD") return "标配";
  if (cell.availability === "OPTIONAL") return "选装";
  if (cell.availability === "NOT_APPLICABLE") return "不适用";
  if (cell.availability === "UNKNOWN") return cell.rawValue || "未知";
  return cell.rawValue || AVAILABILITY_LABELS[cell.availability];
}

export function isMergedSourceCell(cell: CompareCellValue | null): boolean {
  const source = cell?.source;
  return Boolean(source?.mergedRange && source.sourceCell && source.sourceCell !== source.cell);
}

function hasInferredValue(row: CompareRow): boolean {
  return row.values.some((value) => Boolean(value?.inferred));
}

function rowHasMissingSource(row: CompareRow): boolean {
  return row.values.some((value) => !value?.source);
}

function rowHasMergedSource(row: CompareRow): boolean {
  return row.values.some((value) => isMergedSourceCell(value));
}

function hasDeltaType(deltas: ConfigDelta[], deltaType: ConfigDeltaType): boolean {
  return deltas.some((delta) => delta.deltaType === deltaType);
}

function hasBaseDifference(deltas: ConfigDelta[]): boolean {
  return deltas.some((delta) => delta.deltaType !== "SAME");
}

function isCommonByDeltas(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): boolean {
  if (!baseModeActive) return fallbackComparisonType(row) === "COMMON_SAME";
  return deltas.every((delta) => delta.deltaType === "SAME");
}

function isUnknownByDeltas(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): boolean {
  if (baseModeActive) return hasDeltaType(deltas, "UNKNOWN");
  return fallbackComparisonType(row) === "MISSING_UNKNOWN"
    || fallbackComparisonType(row) === "MISSING_OR_UNKNOWN"
    || hasDeltaType(deltas, "UNKNOWN");
}

function matchesDeltaFilter(row: CompareRow, deltas: ConfigDelta[], deltaFilter: ConfigComparisonDeltaFilter, baseModeActive: boolean): boolean {
  if (deltaFilter === "ALL") return true;
  if (deltaFilter === "DIFFERENCE") return rowHasDifference(row, deltas, baseModeActive);
  if (deltaFilter === "INFERRED") return rowHasDifference(row, deltas, baseModeActive) && hasInferredValue(row);
  if (deltaFilter === "MISSING_SOURCE") return rowHasMissingSource(row);
  if (deltaFilter === "MERGED_SOURCE") return rowHasMergedSource(row);
  if (deltaFilter === "COMMON") return isCommonByDeltas(row, deltas, baseModeActive);
  if (deltaFilter === "UNKNOWN") return isUnknownByDeltas(row, deltas, baseModeActive);
  if (!baseModeActive && deltaFilter === "VALUE_CHANGED") return fallbackComparisonType(row) === "DIFFERENT_VALUE";
  return hasDeltaType(deltas, deltaFilter);
}

function rowHasMissingSourceInDeltas(deltas: ConfigDelta[]): boolean {
  return deltas.some((delta) => !delta.baseValue?.source || !delta.targetValue?.source);
}

function rowHasMergedSourceInDeltas(deltas: ConfigDelta[]): boolean {
  return deltas.some((delta) => isMergedSourceCell(delta.baseValue) || isMergedSourceCell(delta.targetValue));
}

function matchesTargetScopedDeltaFilter(
  row: CompareRow,
  deltas: ConfigDelta[],
  deltaFilter: ConfigComparisonDeltaFilter,
  baseModeActive: boolean,
  targetTrimId?: string | null,
): boolean {
  if (!targetTrimId || !baseModeActive) return matchesDeltaFilter(row, deltas, deltaFilter, baseModeActive);
  const targetDeltas = deltas.filter((delta) => delta.targetTrim.trimId === targetTrimId);
  if (targetDeltas.length === 0) return false;
  if (deltaFilter === "ALL") return true;
  if (deltaFilter === "DIFFERENCE") return hasBaseDifference(targetDeltas);
  if (deltaFilter === "INFERRED") return hasBaseDifference(targetDeltas) && targetDeltas.some((delta) => delta.inferred);
  if (deltaFilter === "MISSING_SOURCE") return rowHasMissingSourceInDeltas(targetDeltas);
  if (deltaFilter === "MERGED_SOURCE") return rowHasMergedSourceInDeltas(targetDeltas);
  if (deltaFilter === "COMMON") return targetDeltas.every((delta) => delta.deltaType === "SAME");
  if (deltaFilter === "UNKNOWN") return hasDeltaType(targetDeltas, "UNKNOWN");
  return hasDeltaType(targetDeltas, deltaFilter);
}

export function rowHasDifference(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): boolean {
  if (!baseModeActive) return row.comparisonType !== "COMMON_SAME";
  return hasBaseDifference(deltas);
}

export function rowMatchesConfigDeltaFilter(
  data: CompareResponse,
  row: CompareRow,
  deltaFilter: ConfigComparisonDeltaFilter,
  baseTrimId?: string | null,
): boolean {
  const rowWithType = {
    ...row,
    comparisonType: fallbackComparisonType(row),
  };
  const baseModeActive = Boolean(baseTrimId && data.trims.some((trim) => trim.trimId === baseTrimId));
  const deltas = baseModeActive ? rowDeltasForBase(data, rowWithType, baseTrimId) : [];
  return matchesDeltaFilter(rowWithType, deltas, deltaFilter, baseModeActive);
}

export function rowMatchesConfigScope(
  data: CompareResponse,
  row: CompareRow,
  deltaFilter: ConfigComparisonDeltaFilter,
  baseTrimId?: string | null,
  targetTrimId?: string | null,
): boolean {
  const rowWithType = {
    ...row,
    comparisonType: fallbackComparisonType(row),
  };
  const baseModeActive = Boolean(baseTrimId && data.trims.some((trim) => trim.trimId === baseTrimId));
  const deltas = baseModeActive ? rowDeltasForBase(data, rowWithType, baseTrimId) : [];
  return matchesTargetScopedDeltaFilter(rowWithType, deltas, deltaFilter, baseModeActive, targetTrimId);
}

function categoryLabel(category: string): string {
  return category.replace(/\s+/g, " ").trim() || "未分类";
}

function normalizedSearchText(value: string): string {
  return value.replace(/\s+/g, " ").trim().toLowerCase();
}

export function rowMatchesConfigSearch(row: CompareRow, search: string): boolean {
  const q = normalizedSearchText(search);
  if (!q) return true;
  const normalizedCategory = categoryLabel(row.category).toLowerCase();
  const valueMatches = row.values.some((value) => {
    const tokens = value
      ? [
        value.rawValue,
        value.normalizedValue,
        value.displayValue,
        value.unit,
        value.valueState,
        value.availability,
        AVAILABILITY_LABELS[value.availability],
        cellText(value),
      ]
      : ["待确认"];
    return tokens.some((token) => normalizedSearchText(String(token ?? "")).includes(q));
  });
  return row.featureName.toLowerCase().includes(q)
    || row.featureCode.toLowerCase().includes(q)
    || row.category.toLowerCase().includes(q)
    || normalizedCategory.includes(q)
    || (row.businessNote || "").toLowerCase().includes(q)
    || valueMatches;
}
