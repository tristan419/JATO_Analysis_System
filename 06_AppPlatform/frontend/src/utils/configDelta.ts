import type { CompareCellValue, CompareResponse, CompareRow, CompareTrimItem } from "../types/engineeringConfig";

export type ConfigDeltaType =
  | "ADDED"
  | "REMOVED"
  | "SAME"
  | "OPTIONAL_CHANGED"
  | "VALUE_CHANGED"
  | "UNKNOWN";

export interface ConfigDelta {
  row: CompareRow;
  baseTrim: CompareTrimItem;
  targetTrim: CompareTrimItem;
  baseIndex: number;
  targetIndex: number;
  baseValue: CompareCellValue | null;
  targetValue: CompareCellValue | null;
  deltaType: ConfigDeltaType;
  inferred: boolean;
}

export interface CategoryDeltaSummary {
  category: string;
  totalDifferenceCount: number;
  addedCount: number;
  removedCount: number;
  optionalCount: number;
  valueChangedCount: number;
  inferredCount: number;
  unknownCount: number;
}

export interface TrimDeltaSummary {
  targetTrim: CompareTrimItem;
  totalDifferenceCount: number;
  addedCount: number;
  removedCount: number;
  optionalCount: number;
  valueChangedCount: number;
  inferredCount: number;
  unknownCount: number;
  categorySummaries: CategoryDeltaSummary[];
  deltas: ConfigDelta[];
}

export interface BusinessDifferenceSummary {
  baseTrim: CompareTrimItem | null;
  baseIndex: number;
  targetSummaries: TrimDeltaSummary[];
}

const PRESENT_STATES = new Set(["STANDARD", "VALUE"]);

function isUnknownCell(value: CompareCellValue | null): boolean {
  return value === null || value.availability === "UNKNOWN";
}

function isPresent(value: CompareCellValue | null): boolean {
  return Boolean(value && PRESENT_STATES.has(value.availability));
}

function isOptional(value: CompareCellValue | null): boolean {
  return value?.availability === "OPTIONAL";
}

function comparableSignature(value: CompareCellValue | null): string {
  if (!value) return "missing";
  return [
    value.availability,
    value.normalizedValue ?? "",
    value.rawValue ?? "",
    value.displayValue ?? "",
  ].join("|");
}

export function classifyConfigDelta(
  baseValue: CompareCellValue | null,
  targetValue: CompareCellValue | null,
): ConfigDeltaType {
  if (comparableSignature(baseValue) === comparableSignature(targetValue)) return "SAME";
  if (isUnknownCell(baseValue) || isUnknownCell(targetValue)) return "UNKNOWN";

  const baseAvailable = isPresent(baseValue) || isOptional(baseValue);
  const targetAvailable = isPresent(targetValue) || isOptional(targetValue);
  if (!baseAvailable && targetAvailable) return "ADDED";
  if (baseAvailable && !targetAvailable) return "REMOVED";
  if ((isOptional(baseValue) || isOptional(targetValue)) && baseValue?.availability !== targetValue?.availability) {
    return "OPTIONAL_CHANGED";
  }
  if (baseValue?.availability === "VALUE" || targetValue?.availability === "VALUE") return "VALUE_CHANGED";
  return "VALUE_CHANGED";
}

export function deltaEvidenceTarget(delta: ConfigDelta): { trim: CompareTrimItem; cell: CompareCellValue | null } {
  if (delta.targetValue?.inferred) return { trim: delta.targetTrim, cell: delta.targetValue };
  if (delta.baseValue?.inferred) return { trim: delta.baseTrim, cell: delta.baseValue };
  if (delta.deltaType === "REMOVED") return { trim: delta.baseTrim, cell: delta.baseValue };
  return { trim: delta.targetTrim, cell: delta.targetValue };
}

function emptyCategorySummary(category: string): CategoryDeltaSummary {
  return {
    category,
    totalDifferenceCount: 0,
    addedCount: 0,
    removedCount: 0,
    optionalCount: 0,
    valueChangedCount: 0,
    inferredCount: 0,
    unknownCount: 0,
  };
}

function applyDeltaCounts(summary: CategoryDeltaSummary | TrimDeltaSummary, delta: ConfigDelta): void {
  if (delta.deltaType === "SAME") return;
  summary.totalDifferenceCount += 1;
  if (delta.deltaType === "ADDED") summary.addedCount += 1;
  if (delta.deltaType === "REMOVED") summary.removedCount += 1;
  if (delta.deltaType === "OPTIONAL_CHANGED") summary.optionalCount += 1;
  if (delta.deltaType === "VALUE_CHANGED") summary.valueChangedCount += 1;
  if (delta.deltaType === "UNKNOWN") summary.unknownCount += 1;
  if (delta.inferred) summary.inferredCount += 1;
}

function buildTargetSummary(
  data: CompareResponse,
  baseTrim: CompareTrimItem,
  baseIndex: number,
  targetTrim: CompareTrimItem,
  targetIndex: number,
): TrimDeltaSummary {
  const categoryMap = new Map<string, CategoryDeltaSummary>();
  const targetSummary: TrimDeltaSummary = {
    targetTrim,
    totalDifferenceCount: 0,
    addedCount: 0,
    removedCount: 0,
    optionalCount: 0,
    valueChangedCount: 0,
    inferredCount: 0,
    unknownCount: 0,
    categorySummaries: [],
    deltas: [],
  };

  data.rows.forEach((row) => {
    const baseValue = row.values[baseIndex] ?? null;
    const targetValue = row.values[targetIndex] ?? null;
    const deltaType = classifyConfigDelta(baseValue, targetValue);
    const delta: ConfigDelta = {
      row,
      baseTrim,
      targetTrim,
      baseIndex,
      targetIndex,
      baseValue,
      targetValue,
      deltaType,
      inferred: Boolean(baseValue?.inferred || targetValue?.inferred),
    };
    targetSummary.deltas.push(delta);
    if (deltaType === "SAME") return;
    const categorySummary = categoryMap.get(row.category) ?? emptyCategorySummary(row.category);
    applyDeltaCounts(categorySummary, delta);
    categoryMap.set(row.category, categorySummary);
    applyDeltaCounts(targetSummary, delta);
  });

  targetSummary.categorySummaries = Array.from(categoryMap.values()).sort((a, b) => {
    if (b.totalDifferenceCount !== a.totalDifferenceCount) return b.totalDifferenceCount - a.totalDifferenceCount;
    return a.category.localeCompare(b.category, undefined, { numeric: true, sensitivity: "base" });
  });

  return targetSummary;
}

export function buildBusinessDifferenceSummary(data: CompareResponse, baseTrimId: string | null | undefined): BusinessDifferenceSummary {
  const baseIndex = Math.max(0, data.trims.findIndex((trim) => trim.trimId === baseTrimId));
  const baseTrim = data.trims[baseIndex] ?? null;
  if (!baseTrim) return { baseTrim: null, baseIndex: -1, targetSummaries: [] };
  return {
    baseTrim,
    baseIndex,
    targetSummaries: data.trims
      .map((targetTrim, targetIndex) => ({ targetTrim, targetIndex }))
      .filter(({ targetIndex }) => targetIndex !== baseIndex)
      .map(({ targetTrim, targetIndex }) => buildTargetSummary(data, baseTrim, baseIndex, targetTrim, targetIndex)),
  };
}

export function rowDeltasForBase(data: CompareResponse, row: CompareRow, baseTrimId: string | null | undefined): ConfigDelta[] {
  const baseIndex = Math.max(0, data.trims.findIndex((trim) => trim.trimId === baseTrimId));
  const baseTrim = data.trims[baseIndex];
  if (!baseTrim) return [];
  return data.trims
    .map((targetTrim, targetIndex) => ({ targetTrim, targetIndex }))
    .filter(({ targetIndex }) => targetIndex !== baseIndex)
    .map(({ targetTrim, targetIndex }) => {
      const baseValue = row.values[baseIndex] ?? null;
      const targetValue = row.values[targetIndex] ?? null;
      return {
        row,
        baseTrim,
        targetTrim,
        baseIndex,
        targetIndex,
        baseValue,
        targetValue,
        deltaType: classifyConfigDelta(baseValue, targetValue),
        inferred: Boolean(baseValue?.inferred || targetValue?.inferred),
      };
    });
}
