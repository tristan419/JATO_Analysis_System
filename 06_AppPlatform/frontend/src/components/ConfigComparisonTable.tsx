import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  AvailabilityState,
  CompareCellValue,
  CompareResponse,
  CompareRow,
  CompareTrimItem,
  ComparisonType,
  ConfigValueState,
  EngineeringConfigBusinessSummaryItem,
  EngineeringConfigBusinessSummaryUsage,
  EngineeringConfigCompareExportRequest,
} from "../types/engineeringConfig";
import type { SourceEvidenceSelection } from "./SourceEvidenceDrawer";
import { SearchDropdownFilter, type SearchDropdownOption } from "./SearchDropdownFilter";
import { rowDeltasForBase, type ConfigDelta, type ConfigDeltaType } from "../utils/configDelta";
import { api } from "../api/client";

const LazySourceEvidenceDrawer = React.lazy(() => import("./SourceEvidenceDrawer").then((module) => ({
  default: module.SourceEvidenceDrawer,
})));

const COMPARISON_META: Record<ComparisonType, { label: string; className: string }> = {
  COMMON_SAME: { label: "共同配置", className: "comparison-diff-tag--common" },
  DIFFERENT_VALUE: { label: "值不同", className: "comparison-diff-tag--value" },
  UNIQUE_TO_TRIM: { label: "独有配置", className: "comparison-diff-tag--unique" },
  PARTIAL_AVAILABLE: { label: "部分具备", className: "comparison-diff-tag--partial" },
  MISSING_OR_UNKNOWN: { label: "缺失 / 未知", className: "comparison-diff-tag--missing" },
  MISSING_UNKNOWN: { label: "待确认", className: "comparison-diff-tag--missing" },
  NOT_APPLICABLE: { label: "不适用", className: "comparison-diff-tag--missing" },
  CANCELLED_OR_REMOVED: { label: "取消 / 删除", className: "comparison-diff-tag--missing" },
  AVAILABILITY_DIFFERENT: { label: "可用性差异", className: "comparison-diff-tag--partial" },
  OPTIONAL_DIFFERENT: { label: "选装差异", className: "comparison-diff-tag--partial" },
  UNIQUE_OR_PARTIAL: { label: "部分具备", className: "comparison-diff-tag--unique" },
};

const DELTA_META: Record<ConfigDeltaType, { label: string; className: string }> = {
  ADDED: { label: "新增", className: "comparison-diff-tag--unique" },
  REMOVED: { label: "减少", className: "comparison-diff-tag--removed" },
  SAME: { label: "共同配置", className: "comparison-diff-tag--common" },
  OPTIONAL_CHANGED: { label: "选装变化", className: "comparison-diff-tag--partial" },
  VALUE_CHANGED: { label: "值变化", className: "comparison-diff-tag--value" },
  UNKNOWN: { label: "待确认", className: "comparison-diff-tag--missing" },
};

const AVAILABILITY_LABELS: Record<AvailabilityState, string> = {
  STANDARD: "标配",
  OPTIONAL: "选装",
  NOT_AVAILABLE: "不配备",
  NOT_APPLICABLE: "不适用",
  VALUE: "参数",
  UNKNOWN: "未知",
  CANCELLED_OR_REMOVED: "取消 / 删除",
};

const VALUE_LEGEND_ITEMS = [
  { key: "standard", label: "标配", detail: "当前配置列已配置", className: "compare-cell-standard" },
  { key: "optional", label: "选装", detail: "可选配置，不等同于标配", className: "compare-cell-optional" },
  { key: "not-available", label: "不配备", detail: "来源明确或业务规则判定为无配置", className: "compare-cell-not_available" },
  { key: "inferred-not-available", label: "不配备*", detail: "规则推断，不是 Excel 原文", className: "compare-cell-evidence-button--inferred" },
  { key: "unknown", label: "待确认", detail: "空白或缺值，不能直接等同于不配备", className: "compare-cell-missing" },
  { key: "value", label: "参数值", detail: "尺寸、功率、版本号等文本或数值", className: "compare-cell-value" },
];

const EVIDENCE_LEGEND_ITEMS = [
  { key: "source", label: "来源", detail: "可追溯到文件坐标", className: "compare-cell-evidence-button--source" },
  { key: "manual", label: "人工", detail: "有权限用户已覆盖原始值", className: "compare-cell-evidence-button--manual" },
  { key: "inferred", label: "推断", detail: "按规则生成显示值", className: "compare-cell-evidence-button--inferred" },
  { key: "merged", label: "合并", detail: "来自横向合并格展开", className: "compare-cell-evidence-button--merged" },
  { key: "missing", label: "缺源", detail: "当前配置暂无来源证据", className: "compare-cell-evidence-button--missing" },
];

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

export interface ConfigComparisonCellSavePayload {
  cell: CompareCellValue | null;
  expectedVersion?: number;
  featureId?: string | null;
  rawValue: string;
  row: CompareRow;
  trim: CompareTrimItem;
  valueId?: string;
}

export interface ConfigComparisonCellSaveResult {
  availability?: AvailabilityState;
  displayValue?: string | null;
  normalizedValue?: string | null;
  rawValue?: string;
  manualOverride?: boolean;
  valueId?: string;
  valueState?: ConfigValueState;
  version?: number;
  unchanged?: boolean;
}

export interface ConfigComparisonTableExportActions {
  canExport: boolean;
  copyCurrentScope: () => Promise<void>;
  copyLabel: string;
  exportPdf: () => Promise<void>;
  exportXlsx: () => Promise<void>;
  exportingPdf: boolean;
  exportingXlsx: boolean;
  rangeLabel: string;
  rowCount: number;
  trimCount: number;
}

export type ConfigComparisonTableExportStatus = Pick<
  ConfigComparisonTableExportActions,
  "canExport" | "copyLabel" | "exportingPdf" | "exportingXlsx" | "rangeLabel" | "rowCount" | "trimCount"
>;

type ConfigComparisonLegendMode = "compact" | "full";
type ConfigComparisonToolbarMode = "simple" | "full";
type ConfigComparisonCellEvidenceMode = "compact" | "full";
type ConfigComparisonColumnMode = "matrix" | "full";
type ConfigComparisonCategorySummaryMode = "compact" | "full";

const SIMPLE_DELTA_FILTER_KEYS: ReadonlySet<ConfigComparisonDeltaFilter> = new Set([
  "ALL",
  "DIFFERENCE",
]);

interface ConfigComparisonTableProps {
  data: CompareResponse;
  baseTrimId?: string | null;
  businessSummaryExport?: EngineeringConfigBusinessSummaryItem[];
  businessSummaryUsage?: EngineeringConfigBusinessSummaryUsage | null;
  categoryFilter?: string | null;
  categorySummaryMode?: ConfigComparisonCategorySummaryMode;
  deltaFilter?: ConfigComparisonDeltaFilter;
  cellEvidenceMode?: ConfigComparisonCellEvidenceMode;
  columnMode?: ConfigComparisonColumnMode;
  legendMode?: ConfigComparisonLegendMode;
  searchValue?: string;
  targetTrimId?: string | null;
  toolbarMode?: ConfigComparisonToolbarMode;
  valuesEditable?: boolean;
  factSource?: EngineeringConfigCompareExportRequest["factSource"];
  exportActionsRef?: React.MutableRefObject<ConfigComparisonTableExportActions | null>;
  focusedFeatureCode?: string | null;
  focusedFeatureRequestKey?: number;
  onExportActionsChange?: (status: ConfigComparisonTableExportStatus | null) => void;
  onCategoryFilterChange?: (category: string | null) => void;
  onDeltaFilterChange?: (filter: ConfigComparisonDeltaFilter) => void;
  onOpenEvidence?: (selection: SourceEvidenceSelection) => void;
  onSaveCell?: (payload: ConfigComparisonCellSavePayload) => Promise<ConfigComparisonCellSaveResult>;
  onSearchChange?: (value: string) => void;
  onTargetTrimChange?: (trimId: string | null) => void;
}

interface RowWithDeltas {
  row: CompareRow;
  rowAnchorKey: string;
  deltas: ConfigDelta[];
  rowKey: string;
}

interface CategoryDeltaSummary {
  added: number;
  removed: number;
  valueChanged: number;
  optionalChanged: number;
  unknown: number;
  inferred: number;
  baseDifferenceRows: number;
}

interface CategoryDeltaSummaryChip {
  key: string;
  label: string;
  count: number;
  className: string;
  filter: ConfigComparisonDeltaFilter;
}

interface ConfigComparisonFilterChip {
  key: ConfigComparisonDeltaFilter;
  label: string;
  count: number;
}

interface VisibleTrimEntry {
  trim: CompareTrimItem;
  index: number;
}

interface RangeStatusMetric {
  key: string;
  label: string;
  value: number;
  detail: string;
  toneClass?: string;
  filter?: ConfigComparisonDeltaFilter;
  resetScope?: boolean;
  active?: boolean;
}

interface ConfigCompareExportScope {
  baseLabel?: string;
  categoryLabel?: string;
  rangeLabel: string;
  rowCount?: number;
  searchLabel?: string;
  targetLabel?: string;
  title: string;
}

function tsvCell(value: string | number | null | undefined): string {
  return String(value ?? "").replace(/[\t\r\n]+/g, " ").replace(/\s+/g, " ").trim();
}

function downloadBlob(blob: Blob, fileName: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

function exportFileName(scope: ConfigCompareExportScope, extension: "xlsx" | "pdf" = "xlsx"): string {
  const safe = scope.title.replace(/[\\/:*?"<>|]+/g, "-").replace(/\s+/g, "-").slice(0, 90);
  return `${safe || "config-compare"}-${new Date().toISOString().slice(0, 10)}.${extension}`;
}

function fallbackComparisonType(row: CompareRow): ComparisonType {
  if (row.comparisonType === "MISSING_OR_UNKNOWN") return "MISSING_UNKNOWN";
  if (row.comparisonType) return row.comparisonType;
  const nonNullValues = row.values.filter((value): value is CompareCellValue => value !== null);
  if (nonNullValues.length !== row.values.length || nonNullValues.some((value) => value.availability === "UNKNOWN")) {
    return "MISSING_UNKNOWN";
  }
  const signatures = new Set(nonNullValues.map((value) => `${value.availability}:${value.normalizedValue ?? ""}:${value.rawValue}`));
  return signatures.size <= 1 ? "COMMON_SAME" : "DIFFERENT_VALUE";
}

function cellClass(cell: CompareCellValue | null, rowType: ComparisonType): string {
  if (cell === null) return "compare-cell compare-cell-missing comparison-value-missing";
  const availabilityClass = `compare-cell-${cell.availability.toLowerCase()}`;
  const typeClass = rowType === "DIFFERENT_VALUE"
    ? "comparison-value-different"
    : rowType === "UNIQUE_TO_TRIM" || rowType === "UNIQUE_OR_PARTIAL"
      ? "comparison-value-unique"
      : rowType === "MISSING_OR_UNKNOWN" || rowType === "MISSING_UNKNOWN"
        ? "comparison-value-missing"
        : "";
  return `compare-cell ${availabilityClass} ${typeClass}`.trim();
}

function cellText(cell: CompareCellValue | null): string {
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

interface CellEvidenceActionMeta {
  key: "source" | "manual" | "inferred" | "merged" | "missing";
  label: string;
  compactLabel: string;
  className: string;
  title: string;
}

function isMergedSourceCell(cell: CompareCellValue | null): boolean {
  const source = cell?.source;
  return Boolean(source?.mergedRange && source.sourceCell && source.sourceCell !== source.cell);
}

function cellEvidenceActionMeta(cell: CompareCellValue | null): CellEvidenceActionMeta {
  if (cell?.manualOverride) {
    return {
      key: "manual",
      label: "人工",
      compactLabel: "人",
      className: "compare-cell-evidence-button--manual",
      title: "查看人工覆盖说明",
    };
  }
  if (cell?.inferred) {
    return {
      key: "inferred",
      label: "推断",
      compactLabel: "*",
      className: "compare-cell-evidence-button--inferred",
      title: "查看规则推断来源",
    };
  }
  if (isMergedSourceCell(cell)) {
    return {
      key: "merged",
      label: "合并",
      compactLabel: "M",
      className: "compare-cell-evidence-button--merged",
      title: "查看合并格来源",
    };
  }
  if (!cell?.source) {
    return {
      key: "missing",
      label: "缺源",
      compactLabel: "!",
      className: "compare-cell-evidence-button--missing",
      title: "查看缺失来源说明",
    };
  }
  return {
    key: "source",
    label: "来源",
    compactLabel: "i",
    className: "compare-cell-evidence-button--source",
    title: "查看来源",
  };
}

function CellDisplay({
  cell,
  editable,
  evidenceMode,
  featureName,
  onOpenEvidence,
  onSaveCell,
  row,
  rowType,
  trim,
}: {
  cell: CompareCellValue | null;
  editable: boolean;
  evidenceMode: ConfigComparisonCellEvidenceMode;
  featureName: string;
  onOpenEvidence: () => void;
  onSaveCell?: (payload: ConfigComparisonCellSavePayload) => Promise<ConfigComparisonCellSaveResult>;
  row: CompareRow;
  rowType: ComparisonType;
  trim: CompareTrimItem;
}) {
  const [editingValue, setEditingValue] = useState<string | null>(null);
  const [editingDirty, setEditingDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);
  const autoSaveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const savedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const editingValueRef = useRef<string | null>(null);
  const editingRevisionRef = useRef(0);
  const saveInFlightRef = useRef(false);
  const evidenceMeta = cellEvidenceActionMeta(cell);
  const evidenceButtonLabel = evidenceMode === "compact" ? evidenceMeta.compactLabel : evidenceMeta.label;
  const compactEvidenceCell = evidenceMode === "compact";
  const evidenceAriaLabel = `查看 ${trim.fullTrimName || trim.trimName || "配置列"} ${featureName} 的配置来源`;
  const canUpdateCell = Boolean(editable && onSaveCell && cell?.valueId && typeof cell.version === "number");
  const canCreateCell = Boolean(editable && onSaveCell && !cell && row.featureId && trim.trimId);
  const canEditCell = canUpdateCell || canCreateCell;
  const clearAutoSaveTimer = useCallback((): void => {
    if (autoSaveTimerRef.current) {
      clearTimeout(autoSaveTimerRef.current);
      autoSaveTimerRef.current = null;
    }
  }, []);
  const beginEdit = (event?: React.MouseEvent<HTMLTableCellElement> | React.KeyboardEvent<HTMLTableCellElement>): void => {
    if (!canEditCell) return;
    event?.stopPropagation();
    const initialValue = cell?.rawValue ?? "";
    editingValueRef.current = initialValue;
    editingRevisionRef.current = 0;
    setEditingValue(initialValue);
    setEditingDirty(false);
    setSaveError(null);
  };
  const clearCellValue = (): void => {
    if (!canUpdateCell) return;
    clearAutoSaveTimer();
    editingValueRef.current = "";
    editingRevisionRef.current += 1;
    setEditingValue("");
    setEditingDirty((cell?.rawValue ?? "") !== "");
    setSaveError(null);
  };
  const cancelEdit = (): void => {
    clearAutoSaveTimer();
    editingValueRef.current = null;
    editingRevisionRef.current += 1;
    setEditingValue(null);
    setEditingDirty(false);
    setSaveError(null);
  };
  const saveEdit = useCallback(async (): Promise<void> => {
    const valueToSave = editingValueRef.current;
    if (!canEditCell || !onSaveCell || valueToSave === null || saveInFlightRef.current) return;
    if (canUpdateCell && cell && valueToSave === cell.rawValue) {
      cancelEdit();
      return;
    }
    if (canCreateCell && !valueToSave.trim()) {
      cancelEdit();
      return;
    }
    clearAutoSaveTimer();
    const savedRevision = editingRevisionRef.current;
    saveInFlightRef.current = true;
    setSaving(true);
    setSaveError(null);
    try {
      const result = await onSaveCell({
        cell,
        expectedVersion: cell?.version ?? undefined,
        featureId: row.featureId,
        rawValue: valueToSave,
        row,
        trim,
        valueId: cell?.valueId ?? undefined,
      });
      if (editingRevisionRef.current === savedRevision) {
        editingValueRef.current = null;
        setEditingValue(null);
        setEditingDirty(false);
        setSaved(true);
        if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
        savedTimerRef.current = setTimeout(() => setSaved(false), 1200);
      } else {
        setEditingDirty(editingValueRef.current !== (result.rawValue ?? valueToSave));
      }
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : "保存失败");
    } finally {
      saveInFlightRef.current = false;
      setSaving(false);
    }
  }, [canCreateCell, canEditCell, canUpdateCell, cell, clearAutoSaveTimer, onSaveCell, row, trim]);

  useEffect(() => {
    if (editingValue === null || !editingDirty || saving) return undefined;
    clearAutoSaveTimer();
    autoSaveTimerRef.current = setTimeout(() => {
      void saveEdit();
    }, 1200);
    return clearAutoSaveTimer;
  }, [clearAutoSaveTimer, editingDirty, editingValue, saveEdit, saving]);

  useEffect(() => {
    if (editingValue === null) return;
    inputRef.current?.focus();
    inputRef.current?.select();
  }, [editingValue]);

  useEffect(() => () => {
    clearAutoSaveTimer();
    if (savedTimerRef.current) clearTimeout(savedTimerRef.current);
  }, [clearAutoSaveTimer]);

  const openCompactEvidence = (event: React.MouseEvent<HTMLTableCellElement>): void => {
    if (!compactEvidenceCell || canEditCell) return;
    event.stopPropagation();
    onOpenEvidence();
  };
  const handleCompactEvidenceKeyDown = (event: React.KeyboardEvent<HTMLTableCellElement>): void => {
    if (canEditCell) {
      if (event.key === "Enter" || event.key === "F2") {
        event.preventDefault();
        beginEdit(event);
      }
      if (canUpdateCell && (event.key === "Delete" || event.key === "Backspace")) {
        event.preventDefault();
        event.stopPropagation();
        clearCellValue();
      }
      return;
    }
    if (!compactEvidenceCell) return;
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    event.stopPropagation();
    onOpenEvidence();
  };
  if (editingValue !== null) {
    return (
      <td
        className={`${cellClass(cell, rowType)} compare-cell--editing ${canUpdateCell ? "compare-cell--editing-clearable" : ""} ${saveError ? "compare-cell--save-error" : ""}`.trim()}
        onClick={(event) => event.stopPropagation()}
      >
        <input
          ref={inputRef}
          className="compare-cell-edit-input"
          value={editingValue}
          aria-label={`${trim.fullTrimName || trim.trimName || "配置列"} ${featureName} 配置值，修改后 1.2 秒自动保存`}
          onChange={(event) => {
            const nextValue = event.target.value;
            editingValueRef.current = nextValue;
            editingRevisionRef.current += 1;
            setEditingValue(nextValue);
            setEditingDirty(nextValue !== (cell?.rawValue ?? ""));
          }}
          onKeyDown={(event) => {
            if (event.key === "Enter") {
              event.preventDefault();
              void saveEdit();
            }
            if (event.key === "Escape" && !saving) {
              event.preventDefault();
              cancelEdit();
            }
          }}
        />
        {canUpdateCell ? (
          <button
            className="compare-cell-edit-clear"
            type="button"
            disabled={saving || editingValue.length === 0}
            aria-label={`清空 ${trim.fullTrimName || trim.trimName || "配置列"} ${featureName} 配置值，1.2 秒后自动保存`}
            onClick={(event) => {
              event.stopPropagation();
              clearCellValue();
            }}
          >
            清空
          </button>
        ) : null}
        <button
          className="compare-cell-edit-save"
          type="button"
          disabled={saving}
          onClick={(event) => {
            event.stopPropagation();
            void saveEdit();
          }}
        >
          {saving ? "..." : "✓"}
        </button>
        <button
          className={`compare-cell-evidence-button compare-cell-evidence-marker ${evidenceMeta.className}`}
          type="button"
          title={evidenceMeta.title}
          aria-label={evidenceAriaLabel}
          onClick={(event) => {
            event.stopPropagation();
            onOpenEvidence();
          }}
        >
          {evidenceButtonLabel}
        </button>
        <span className="compare-cell-autosave-hint" role={saveError ? "alert" : "status"}>
          {saveError || (saving ? "保存中" : editingDirty ? "1.2 秒后自动保存" : "修改后自动保存")}
        </span>
      </td>
    );
  }
  return (
    <td
      className={`${cellClass(cell, rowType)} ${
        compactEvidenceCell && !canEditCell
          ? `compare-cell--evidence-compact compare-cell--evidence-${evidenceMeta.key}`
          : ""
      } ${canEditCell ? "compare-cell--editable" : ""} ${saved ? "compare-cell--save-saved" : ""}`.trim()}
      data-evidence-cell-action={compactEvidenceCell && !canEditCell ? "open" : undefined}
      aria-label={compactEvidenceCell && !canEditCell ? evidenceAriaLabel : undefined}
      onClick={canEditCell ? beginEdit : openCompactEvidence}
      onKeyDown={handleCompactEvidenceKeyDown}
      role={compactEvidenceCell || canEditCell ? "button" : undefined}
      tabIndex={compactEvidenceCell || canEditCell ? 0 : undefined}
      title={canEditCell ? "点击编辑，Delete 清空，修改后 1.2 秒自动保存" : compactEvidenceCell ? evidenceAriaLabel : undefined}
    >
      <span>{cellText(cell)}</span>
      {cell?.unit ? <span className="compare-unit"> {cell.unit}</span> : null}
      {canEditCell ? (
        <span className="compare-cell-edit-marker" aria-hidden="true">编辑</span>
      ) : null}
      {compactEvidenceCell && !canEditCell ? (
        <span
          className={`compare-cell-evidence-button compare-cell-evidence-marker ${evidenceMeta.className}`}
          title={evidenceMeta.title}
          aria-hidden="true"
        >
          {evidenceButtonLabel}
        </span>
      ) : (
        <button
          className={`compare-cell-evidence-button ${evidenceMeta.className}`}
          type="button"
          title={evidenceMeta.title}
          aria-label={evidenceAriaLabel}
          onClick={(event) => {
            event.stopPropagation();
            onOpenEvidence();
          }}
        >
          {evidenceButtonLabel}
        </button>
      )}
    </td>
  );
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

function rowHasDifference(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): boolean {
  if (!baseModeActive) return row.comparisonType !== "COMMON_SAME";
  return hasBaseDifference(deltas);
}

function rowHasPendingEvidence(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): boolean {
  return isUnknownByDeltas(row, deltas, baseModeActive);
}

function rowClassName(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): "compare-row-pending" | "compare-row-diff" | "compare-row-same" {
  if (rowHasPendingEvidence(row, deltas, baseModeActive)) return "compare-row-pending";
  return rowHasDifference(row, deltas, baseModeActive) ? "compare-row-diff" : "compare-row-same";
}

function rowSemanticLabel(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): string {
  if (rowHasPendingEvidence(row, deltas, baseModeActive)) return "待确认配置";
  return rowHasDifference(row, deltas, baseModeActive) ? "差异配置" : "共同配置";
}

function tableScopeUnitLabel(deltaFilter: ConfigComparisonDeltaFilter, simpleRowMode = false): string {
  if (simpleRowMode) {
    if (deltaFilter === "UNKNOWN") return "待确认行";
    if (deltaFilter === "MISSING_SOURCE") return "来源问题行";
    if (deltaFilter === "MERGED_SOURCE") return "合并格行";
    if (
      deltaFilter === "DIFFERENCE"
      || deltaFilter === "ADDED"
      || deltaFilter === "REMOVED"
      || deltaFilter === "VALUE_CHANGED"
      || deltaFilter === "OPTIONAL_CHANGED"
      || deltaFilter === "INFERRED"
    ) {
      return "差异行";
    }
    return "配置行";
  }
  if (deltaFilter === "UNKNOWN") return "项待确认";
  if (deltaFilter === "MISSING_SOURCE" || deltaFilter === "MERGED_SOURCE") return "项证据";
  if (
    deltaFilter === "DIFFERENCE"
    || deltaFilter === "ADDED"
    || deltaFilter === "REMOVED"
    || deltaFilter === "VALUE_CHANGED"
    || deltaFilter === "OPTIONAL_CHANGED"
    || deltaFilter === "INFERRED"
  ) {
    return "项差异";
  }
  return "项配置";
}

function categoryCountText(rowCount: number, diffCount: number, deltaFilter: ConfigComparisonDeltaFilter, targetTrimId?: string | null): string {
  if (targetTrimId && deltaFilter !== "ALL" && deltaFilter !== "COMMON" && deltaFilter !== "MISSING_SOURCE" && deltaFilter !== "MERGED_SOURCE") return `${rowCount} 项差异`;
  if (deltaFilter === "DIFFERENCE") return `${rowCount} 项差异`;
  if (deltaFilter === "ADDED") return `${rowCount} 项新增配置`;
  if (deltaFilter === "REMOVED") return `${rowCount} 项减少配置`;
  if (deltaFilter === "VALUE_CHANGED") return `${rowCount} 项值变化`;
  if (deltaFilter === "OPTIONAL_CHANGED") return `${rowCount} 项选装变化`;
  if (deltaFilter === "INFERRED") return `${rowCount} 项规则推断`;
  if (deltaFilter === "MISSING_SOURCE") return `${rowCount} 项来源问题`;
  if (deltaFilter === "MERGED_SOURCE") return `${rowCount} 项合并格`;
  if (deltaFilter === "UNKNOWN") return `${rowCount} 项待确认`;
  if (deltaFilter === "COMMON") return `${rowCount} 项共同配置`;
  return `${rowCount} 项 / ${diffCount} 项差异`;
}

function emptyCategoryDeltaSummary(): CategoryDeltaSummary {
  return {
    added: 0,
    removed: 0,
    valueChanged: 0,
    optionalChanged: 0,
    unknown: 0,
    inferred: 0,
    baseDifferenceRows: 0,
  };
}

function semanticDeltasForTarget(deltas: ConfigDelta[], targetTrimId?: string | null): ConfigDelta[] {
  return targetTrimId ? deltas.filter((delta) => delta.targetTrim.trimId === targetTrimId) : deltas;
}

function summarizeCategoryDeltas(
  rows: RowWithDeltas[],
  baseModeActive: boolean,
  targetTrimId?: string | null,
): CategoryDeltaSummary {
  const summary = emptyCategoryDeltaSummary();
  rows.forEach(({ row, deltas }) => {
    const semanticDeltas = semanticDeltasForTarget(deltas, targetTrimId);
    if (rowHasDifference(row, semanticDeltas, baseModeActive)) summary.baseDifferenceRows += 1;
    if (!baseModeActive) {
      if (fallbackComparisonType(row) === "DIFFERENT_VALUE") summary.valueChanged += 1;
      if (fallbackComparisonType(row) === "MISSING_UNKNOWN" || fallbackComparisonType(row) === "MISSING_OR_UNKNOWN") summary.unknown += 1;
      if (hasInferredValue(row) && rowHasDifference(row, semanticDeltas, baseModeActive)) summary.inferred += 1;
      return;
    }
    semanticDeltas.forEach((delta) => {
      if (delta.deltaType === "ADDED") summary.added += 1;
      if (delta.deltaType === "REMOVED") summary.removed += 1;
      if (delta.deltaType === "VALUE_CHANGED") summary.valueChanged += 1;
      if (delta.deltaType === "OPTIONAL_CHANGED") summary.optionalChanged += 1;
      if (delta.deltaType === "UNKNOWN") summary.unknown += 1;
      if (delta.deltaType !== "SAME" && delta.inferred) summary.inferred += 1;
    });
  });
  return summary;
}

function categoryDeltaSummaryChips(summary: CategoryDeltaSummary): CategoryDeltaSummaryChip[] {
  const chips: CategoryDeltaSummaryChip[] = [
    { key: "added", label: "新增", count: summary.added, className: "comparison-diff-tag--unique", filter: "ADDED" },
    { key: "removed", label: "减少", count: summary.removed, className: "comparison-diff-tag--removed", filter: "REMOVED" },
    { key: "value", label: "值变化", count: summary.valueChanged, className: "comparison-diff-tag--value", filter: "VALUE_CHANGED" },
    { key: "optional", label: "选装", count: summary.optionalChanged, className: "comparison-diff-tag--partial", filter: "OPTIONAL_CHANGED" },
    { key: "unknown", label: "待确认", count: summary.unknown, className: "comparison-diff-tag--missing", filter: "UNKNOWN" },
    { key: "inferred", label: "推断", count: summary.inferred, className: "comparison-diff-tag--inferred", filter: "INFERRED" },
  ];
  return chips.filter((chip) => chip.count > 0);
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

function rowDeltaTags(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): Array<{ key: string; label: string; className: string }> {
  if (!baseModeActive) {
    const meta = COMPARISON_META[row.comparisonType];
    return [{ key: row.comparisonType, label: meta.label, className: meta.className }];
  }
  const tags: Array<{ key: string; label: string; className: string }> = [];
  const orderedTypes: ConfigDeltaType[] = ["ADDED", "REMOVED", "VALUE_CHANGED", "OPTIONAL_CHANGED", "UNKNOWN"];
  orderedTypes.forEach((deltaType) => {
    const count = deltas.filter((delta) => delta.deltaType === deltaType).length;
    if (count === 0) return;
    const meta = DELTA_META[deltaType];
    tags.push({ key: deltaType, label: `${meta.label} ${count}`, className: meta.className });
  });
  const inferredCount = deltas.filter((delta) => delta.deltaType !== "SAME" && delta.inferred).length;
  if (inferredCount > 0) {
    tags.push({ key: "INFERRED", label: `规则推断 ${inferredCount}`, className: "comparison-diff-tag--inferred" });
  }
  if (tags.length > 0) return tags;
  return [{ key: "SAME", label: DELTA_META.SAME.label, className: DELTA_META.SAME.className }];
}

function RowDeltaTags({ row, deltas, baseModeActive }: { row: CompareRow; deltas: ConfigDelta[]; baseModeActive: boolean }) {
  return (
    <div className="comparison-diff-tag-stack">
      {rowDeltaTags(row, deltas, baseModeActive).map((tag) => (
        <span className={`comparison-diff-tag ${tag.className}`} key={tag.key}>
          {tag.label}
        </span>
      ))}
    </div>
  );
}

function rowMatrixHintTags(row: CompareRow, deltas: ConfigDelta[], baseModeActive: boolean): Array<{ key: string; label: string; className: string }> {
  return rowDeltaTags(row, deltas, baseModeActive).filter((tag) => tag.key !== "SAME" && tag.key !== "COMMON_SAME");
}

function categoryAnchorId(category: string): string {
  const normalized = category
    .trim()
    .toLowerCase()
    .replace(/[^0-9a-z\u4e00-\u9fff]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `config-category-${normalized || "uncategorized"}`;
}

function compareRowKey(row: CompareRow, index: number): string {
  return `${row.category}::${row.featureCode}::${index}`;
}

function featureRowAnchorId(rowKey: string): string {
  const normalized = rowKey
    .trim()
    .toLowerCase()
    .replace(/[^0-9a-z\u4e00-\u9fff]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `config-feature-${normalized || "row"}`;
}

function categoryLabel(category: string): string {
  return category.replace(/\s+/g, " ").trim() || "未分类";
}

function trimLabel(trim: CompareTrimItem): string {
  return trim.fullTrimName || trim.trimName || trim.trimId;
}

function selectedDisplayTitle(trims: CompareTrimItem[]): string {
  const labels = trims.map(trimLabel).filter(Boolean);
  return labels.length > 0 ? labels.join(" vs ") : "Engineering Config Compare";
}

function trimHeaderIdentityLabel(trim: CompareTrimItem): string {
  if (trim.materialNo || trim.vehicleCode) return `物料号 ${trim.materialNo || trim.vehicleCode}`;
  if (trim.salesVersion) return `Sales version ${trim.salesVersion}`;
  if (trim.identityKey) return `Identity ${trim.identityKey}`;
  if (trim.sourceFileName || trim.sourceUploadId) return `来源 ${trim.sourceFileName || trim.sourceUploadId}`;
  return "车型 / 市场锚点";
}

function trimHeaderOriginLabel(trim: CompareTrimItem): string {
  if (trim.dataOrigin === "own_catalog" || trim.materialNo || trim.hasMaterialNo) return "本品";
  if (trim.dataOrigin === "external_or_scraped") return "竞品 / 外部";
  return "身份待确认";
}

function trimHeaderContextValues(trim: CompareTrimItem): string[] {
  return [
    trim.market || trim.country,
    trim.modelYear,
    trim.sourceFileName || trim.sourceUploadId,
    trim.sourceCreatedBy ? `来源人 ${trim.sourceCreatedBy}` : null,
  ]
    .map((value) => value?.replace(/\s+/g, " ").trim())
    .filter((value): value is string => Boolean(value));
}

function trimHeaderContextLabel(trim: CompareTrimItem): string {
  const values = trimHeaderContextValues(trim);
  return values.slice(0, 3).join(" · ") || "市场 / 来源待补";
}

function trimHeaderTitleContextLabel(trim: CompareTrimItem): string {
  const values = trimHeaderContextValues(trim);
  return values.join(" · ") || "市场 / 来源待补";
}

function trimExportLabel(trim: CompareTrimItem): string {
  const origin = trimHeaderOriginLabel(trim);
  const identity = trimHeaderIdentityLabel(trim);
  const context = trimHeaderTitleContextLabel(trim);
  const parts = [
    trimLabel(trim),
    origin !== "身份待确认" ? origin : "",
    identity !== "车型 / 市场锚点" ? identity : "",
    context !== "市场 / 来源待补" ? context : "",
  ];
  return Array.from(new Set(parts.map((part) => part.replace(/\s+/g, " ").trim()).filter(Boolean))).join(" · ");
}

function addSearchOption(options: Map<string, SearchDropdownOption>, value: string | null | undefined, label: string | null | undefined, meta: string): void {
  const normalizedValue = value?.replace(/\s+/g, " ").trim();
  if (!normalizedValue) return;
  const key = normalizedValue.toLowerCase();
  if (options.has(key)) return;
  options.set(key, {
    value: normalizedValue,
    label: label?.replace(/\s+/g, " ").trim() || normalizedValue,
    meta,
  });
}

function buildConfigSearchOptions(data: CompareResponse): SearchDropdownOption[] {
  const options = new Map<string, SearchDropdownOption>();
  data.rows.forEach((row) => {
    const displayCategory = categoryLabel(row.category);
    addSearchOption(options, row.featureName, row.featureName, `配置项 · ${displayCategory}`);
    addSearchOption(options, row.featureCode, row.featureCode, `配置编码 · ${displayCategory}`);
    addSearchOption(options, displayCategory, displayCategory, "配置大类");
    row.values.forEach((value, index) => {
      const trim = data.trims[index];
      const trimMeta = trim ? trimLabel(trim) : "配置值";
      if (!value) {
        addSearchOption(options, "待确认", "待确认", `配置值 · ${trimMeta}`);
        return;
      }
      addSearchOption(options, cellText(value), cellText(value), `配置值 · ${trimMeta}`);
      addSearchOption(options, value.rawValue, value.rawValue, `原始值 · ${trimMeta}`);
      addSearchOption(options, value.displayValue, value.displayValue, `显示值 · ${trimMeta}`);
      addSearchOption(options, value.normalizedValue, value.normalizedValue, `归一值 · ${trimMeta}`);
    });
  });
  return Array.from(options.values());
}

function deltaFilterScopeLabel(deltaFilter: ConfigComparisonDeltaFilter): string {
  if (deltaFilter === "ALL") return "全部配置";
  if (deltaFilter === "DIFFERENCE") return "差异项";
  if (deltaFilter === "ADDED") return "新增配置";
  if (deltaFilter === "REMOVED") return "减少配置";
  if (deltaFilter === "VALUE_CHANGED") return "值变化";
  if (deltaFilter === "OPTIONAL_CHANGED") return "选装变化";
  if (deltaFilter === "INFERRED") return "规则推断";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题";
  if (deltaFilter === "MERGED_SOURCE") return "合并格展开";
  if (deltaFilter === "UNKNOWN") return "待确认";
  if (deltaFilter === "COMMON") return "共同配置";
  return "全部配置";
}

function simpleDeltaFilterLabel(deltaFilter: ConfigComparisonDeltaFilter, fallbackLabel: string): string {
  if (deltaFilter === "ALL") return "全部配置行";
  if (deltaFilter === "DIFFERENCE") return "差异行";
  if (deltaFilter === "ADDED") return "新增行";
  if (deltaFilter === "REMOVED") return "减少行";
  if (deltaFilter === "VALUE_CHANGED") return "值变化行";
  if (deltaFilter === "OPTIONAL_CHANGED") return "选装变化行";
  if (deltaFilter === "INFERRED") return "规则推断行";
  if (deltaFilter === "MISSING_SOURCE") return "来源问题行";
  if (deltaFilter === "MERGED_SOURCE") return "合并格行";
  if (deltaFilter === "UNKNOWN") return "待确认行";
  if (deltaFilter === "COMMON") return "共同配置行";
  return fallbackLabel;
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

export function ConfigComparisonTable({
  data,
  baseTrimId,
  businessSummaryExport = [],
  businessSummaryUsage = null,
  categoryFilter,
  categorySummaryMode = "full",
  cellEvidenceMode = "full",
  columnMode = "full",
  deltaFilter,
  legendMode = "full",
  searchValue,
  targetTrimId,
  toolbarMode = "full",
  valuesEditable = false,
  factSource,
  exportActionsRef,
  focusedFeatureCode,
  focusedFeatureRequestKey = 0,
  onExportActionsChange,
  onCategoryFilterChange,
  onDeltaFilterChange,
  onOpenEvidence,
  onSaveCell,
  onSearchChange,
  onTargetTrimChange,
}: ConfigComparisonTableProps) {
  const [localSearch, setLocalSearch] = useState("");
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set());
  const [evidenceSelection, setEvidenceSelection] = useState<SourceEvidenceSelection | null>(null);
  const [localDeltaFilter, setLocalDeltaFilter] = useState<ConfigComparisonDeltaFilter>("ALL");
  const [localCategoryFilter, setLocalCategoryFilter] = useState<string | null>(null);
  const [activeRowKey, setActiveRowKey] = useState<string | null>(null);
  const [copyFeedback, setCopyFeedback] = useState<string | null>(null);
  const [exportingXlsx, setExportingXlsx] = useState(false);
  const [exportingPdf, setExportingPdf] = useState(false);
  const [simpleControlsOpen, setSimpleControlsOpen] = useState(false);
  const baseModeActive = Boolean(baseTrimId && data.trims.some((trim) => trim.trimId === baseTrimId));
  const categoryFilterControlled = categoryFilter !== undefined;
  const activeCategoryFilter = categoryFilterControlled ? categoryFilter : localCategoryFilter;
  const deltaFilterControlled = deltaFilter !== undefined;
  const activeDeltaFilter = deltaFilterControlled ? deltaFilter : localDeltaFilter;
  const searchControlled = searchValue !== undefined;
  const activeSearch = searchControlled ? searchValue : localSearch;
  const activeTargetTrimId = targetTrimId ?? null;

  const rowsWithType = useMemo(() => data.rows.map((row) => ({
    ...row,
    comparisonType: fallbackComparisonType(row),
  })), [data.rows]);

  const rowsWithDeltas = useMemo(() => {
    const featureCodeCounts = new Map<string, number>();
    rowsWithType.forEach((row) => {
      featureCodeCounts.set(row.featureCode, (featureCodeCounts.get(row.featureCode) ?? 0) + 1);
    });
    return rowsWithType.map((row, index) => {
      const rowKey = compareRowKey(row, index);
      return {
        row,
        rowAnchorKey: (featureCodeCounts.get(row.featureCode) ?? 0) > 1 ? rowKey : row.featureCode,
        deltas: baseModeActive ? rowDeltasForBase(data, row, baseTrimId) : [],
        rowKey,
      };
    });
  }, [baseModeActive, baseTrimId, data, rowsWithType]);

  const categoryOptions = useMemo(() => (
    [...new Set(rowsWithDeltas
      .filter(({ row, deltas }) => (
        matchesTargetScopedDeltaFilter(row, deltas, activeDeltaFilter, baseModeActive, activeTargetTrimId)
        && rowMatchesConfigSearch(row, activeSearch)
      ))
      .map(({ row }) => row.category))]
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }))
  ), [activeDeltaFilter, activeSearch, activeTargetTrimId, baseModeActive, rowsWithDeltas]);
  const categoryDropdownOptions = useMemo(() => categoryOptions.map((category) => {
    const categoryCount = rowsWithDeltas.filter(({ row, deltas }) => (
      row.category === category
      && matchesTargetScopedDeltaFilter(row, deltas, activeDeltaFilter, baseModeActive, activeTargetTrimId)
      && rowMatchesConfigSearch(row, activeSearch)
    )).length;
    return {
      value: category,
      label: categoryLabel(category),
      meta: `当前范围 ${categoryCount} ${tableScopeUnitLabel(activeDeltaFilter)}`,
    };
  }), [activeDeltaFilter, activeSearch, activeTargetTrimId, baseModeActive, categoryOptions, rowsWithDeltas]);

  const filtered = useMemo(() => {
    return rowsWithDeltas.filter(({ row, deltas }) => {
      const matchesCategory = !activeCategoryFilter || row.category === activeCategoryFilter;
      const matchesType = matchesTargetScopedDeltaFilter(row, deltas, activeDeltaFilter, baseModeActive, activeTargetTrimId);
      return matchesCategory
        && rowMatchesConfigSearch(row, activeSearch)
        && matchesType;
    });
  }, [activeCategoryFilter, activeDeltaFilter, activeSearch, activeTargetTrimId, baseModeActive, rowsWithDeltas]);

  useEffect(() => {
    if (!activeCategoryFilter || categoryOptions.includes(activeCategoryFilter)) return;
    if (categoryFilterControlled) onCategoryFilterChange?.(null);
    else setLocalCategoryFilter(null);
  }, [activeCategoryFilter, categoryFilterControlled, categoryOptions, onCategoryFilterChange]);

  const byCat = useMemo(() => {
    const groups: Record<string, RowWithDeltas[]> = {};
    for (const entry of filtered) {
      if (!groups[entry.row.category]) groups[entry.row.category] = [];
      groups[entry.row.category].push(entry);
    }
    return groups;
  }, [filtered]);
  const visibleCategories = Object.keys(byCat);
  const allVisibleCategoriesCollapsed = visibleCategories.length > 0
    && visibleCategories.every((category) => collapsed.has(category));

  const setActiveCategoryFilter = (category: string | null): void => {
    if (categoryFilterControlled) onCategoryFilterChange?.(category);
    else setLocalCategoryFilter(category);
  };

  const setActiveDeltaFilter = (filter: ConfigComparisonDeltaFilter): void => {
    if (deltaFilterControlled) onDeltaFilterChange?.(filter);
    else setLocalDeltaFilter(filter);
  };

  const setActiveSearch = (value: string): void => {
    if (searchControlled) onSearchChange?.(value);
    else setLocalSearch(value);
  };
  const hasActiveSearch = activeSearch.trim().length > 0;
  const hasColumnFocus = Boolean(activeTargetTrimId);
  const hasRowScopeFilter = hasActiveSearch || Boolean(activeCategoryFilter) || activeDeltaFilter !== "ALL";
  const hasScopedFilter = Boolean(activeCategoryFilter) || activeDeltaFilter !== "ALL" || hasColumnFocus;
  const hasResettableScope = hasRowScopeFilter || hasColumnFocus;
  const simpleToolbarMode = toolbarMode === "simple";
  const resetScopeButtonLabel = hasRowScopeFilter
    ? simpleToolbarMode ? "恢复全部配置行" : "恢复全部配置"
    : "显示全部目标列";
  const resetTableScope = (): void => {
    setActiveSearch("");
    setActiveCategoryFilter(null);
    setActiveDeltaFilter("ALL");
    if (!hasRowScopeFilter) onTargetTrimChange?.(null);
  };
  const toggleCategory = (category: string): void => {
    setCollapsed((previous) => {
      const next = new Set(previous);
      if (next.has(category)) next.delete(category);
      else next.add(category);
      return next;
    });
  };
  const expandCategory = (category: string): void => {
    setCollapsed((previous) => {
      if (!previous.has(category)) return previous;
      const next = new Set(previous);
      next.delete(category);
      return next;
    });
  };
  const toggleVisibleCategories = (): void => {
    setCollapsed((previous) => {
      const next = new Set(previous);
      if (allVisibleCategoriesCollapsed) {
        visibleCategories.forEach((category) => next.delete(category));
      } else {
        visibleCategories.forEach((category) => next.add(category));
      }
      return next;
    });
  };
  const toggleActiveRow = (rowKey: string): void => {
    setActiveRowKey((current) => current === rowKey ? null : rowKey);
  };
  const focusRow = (entry: RowWithDeltas): void => {
    expandCategory(entry.row.category);
    setActiveRowKey(entry.rowKey);
    window.requestAnimationFrame(() => {
      document.getElementById(featureRowAnchorId(entry.rowAnchorKey))?.scrollIntoView?.({ behavior: "smooth", block: "center", inline: "nearest" });
    });
  };
  const focusDifferenceAt = (index: number): void => {
    if (visibleDifferenceEntries.length === 0) return;
    const nextIndex = ((index % visibleDifferenceEntries.length) + visibleDifferenceEntries.length) % visibleDifferenceEntries.length;
    focusRow(visibleDifferenceEntries[nextIndex]);
  };
  const focusPreviousDifference = (): void => {
    focusDifferenceAt(activeVisibleDifferenceIndex >= 0 ? activeVisibleDifferenceIndex - 1 : visibleDifferenceEntries.length - 1);
  };
  const focusNextDifference = (): void => {
    focusDifferenceAt(activeVisibleDifferenceIndex >= 0 ? activeVisibleDifferenceIndex + 1 : 0);
  };

  useEffect(() => {
    if (!focusedFeatureCode) return;
    const entry = filtered.find((item) => item.row.featureCode === focusedFeatureCode);
    if (!entry) return;
    focusRow(entry);
  }, [filtered, focusedFeatureCode, focusedFeatureRequestKey]);

  const countRowsByFilter = (filter: ConfigComparisonDeltaFilter): number => rowsWithDeltas.filter(({ row, deltas }) => (
    (!activeCategoryFilter || row.category === activeCategoryFilter)
      && rowMatchesConfigSearch(row, activeSearch)
      && matchesTargetScopedDeltaFilter(row, deltas, filter, baseModeActive, activeTargetTrimId)
  )).length;

  const filterChips: ConfigComparisonFilterChip[] = [
    { key: "ALL", label: "全部", count: countRowsByFilter("ALL") },
    { key: "DIFFERENCE", label: "差异项", count: countRowsByFilter("DIFFERENCE") },
    { key: "ADDED", label: "新增配置", count: countRowsByFilter("ADDED") },
    { key: "REMOVED", label: "减少配置", count: countRowsByFilter("REMOVED") },
    { key: "VALUE_CHANGED", label: "值变化", count: countRowsByFilter("VALUE_CHANGED") },
    { key: "OPTIONAL_CHANGED", label: "选装变化", count: countRowsByFilter("OPTIONAL_CHANGED") },
    { key: "INFERRED", label: "规则推断", count: countRowsByFilter("INFERRED") },
    { key: "MISSING_SOURCE", label: "来源问题", count: countRowsByFilter("MISSING_SOURCE") },
    { key: "MERGED_SOURCE", label: "合并格", count: countRowsByFilter("MERGED_SOURCE") },
    { key: "UNKNOWN", label: "待确认", count: countRowsByFilter("UNKNOWN") },
    { key: "COMMON", label: "共同配置", count: countRowsByFilter("COMMON") },
  ];
  const primaryFilterChips = toolbarMode === "simple"
    ? filterChips.filter((chip) => SIMPLE_DELTA_FILTER_KEYS.has(chip.key))
    : filterChips;
  const baseTrim = baseModeActive ? data.trims.find((trim) => trim.trimId === baseTrimId) ?? null : null;
  const activeTargetTrim = activeTargetTrimId ? data.trims.find((trim) => trim.trimId === activeTargetTrimId) ?? null : null;
  const searchOptions = useMemo(() => buildConfigSearchOptions(data), [data]);
  const normalizedSearch = activeSearch.replace(/\s+/g, " ").trim();
  const totalConfigCount = data.totalFeatures || rowsWithDeltas.length;
  const differenceConfigCount = countRowsByFilter("DIFFERENCE");
  const unknownConfigCount = countRowsByFilter("UNKNOWN");
  const commonConfigCount = countRowsByFilter("COMMON");
  const visibleDifferenceEntries = filtered
    .filter(({ row, deltas }) => rowHasDifference(row, semanticDeltasForTarget(deltas, activeTargetTrimId), baseModeActive));
  const firstVisibleDifferenceRow = visibleDifferenceEntries[0]?.row ?? null;
  const showDifferenceNavigator = !simpleToolbarMode || hasRowScopeFilter || hasColumnFocus;
  const activeVisibleDifferenceIndex = activeRowKey
    ? visibleDifferenceEntries.findIndex((entry) => entry.rowKey === activeRowKey)
    : -1;
  const activeDifferencePosition = activeVisibleDifferenceIndex >= 0 ? activeVisibleDifferenceIndex + 1 : null;
  const currentScopeUnitLabel = tableScopeUnitLabel(activeDeltaFilter, simpleToolbarMode);
  const rangeScopeLabel = simpleToolbarMode
    ? simpleDeltaFilterLabel(activeDeltaFilter, deltaFilterScopeLabel(activeDeltaFilter))
    : deltaFilterScopeLabel(activeDeltaFilter);
  const differenceMetricLabel = simpleToolbarMode ? "差异行" : "差异项";
  const unknownMetricLabel = simpleToolbarMode ? "待确认行" : "待确认";
  const commonMetricLabel = simpleToolbarMode ? "共同配置行" : "共同配置";
  const firstDifferenceButtonLabel = simpleToolbarMode ? "定位首个差异行" : "定位首个差异";
  const previousDifferenceButtonLabel = simpleToolbarMode ? "上一个差异行" : "上一个差异";
  const nextDifferenceButtonLabel = simpleToolbarMode ? "下一个差异行" : "下一个差异";
  const emptyTableTitle = simpleToolbarMode ? "当前筛选没有配置行" : "当前筛选没有配置项";
  const emptyTableDetail = simpleToolbarMode
    ? "请从搜索下拉选择配置项，或调整大类 / 行筛选；空白 / 待确认行不会被自动当成无配置隐藏。"
    : "请调整搜索、大类或差异范围；空白 / 待确认项不会被自动当成无配置隐藏。";
  const emptyTableResetLabel = simpleToolbarMode ? "恢复全部配置行" : "清空筛选并显示全部配置";
  const differencePositionText = activeDifferencePosition
    ? `${simpleToolbarMode ? "差异行" : "差异"} ${activeDifferencePosition}/${visibleDifferenceEntries.length}`
    : simpleToolbarMode ? `${visibleDifferenceEntries.length} 行差异` : `${visibleDifferenceEntries.length} 项差异`;
  const rangeStatusTitle = hasRowScopeFilter
    ? "当前为筛选视图"
    : hasColumnFocus
      ? "当前为目标列视图"
      : simpleToolbarMode ? "当前展示全部配置行" : "当前展示全部配置项";
  const rangeStatusDetail = [
    `当前展示 ${filtered.length}/${totalConfigCount} 行`,
    activeDeltaFilter === "ALL" && !hasRowScopeFilter ? "未隐藏共同项" : null,
    `范围 ${rangeScopeLabel}`,
    activeTargetTrim ? `${toolbarMode === "simple" ? "目标配置列" : "目标"} ${trimLabel(activeTargetTrim)}` : null,
    activeCategoryFilter ? `大类 ${categoryLabel(activeCategoryFilter)}` : null,
    normalizedSearch ? `搜索 ${normalizedSearch}` : null,
  ].filter((item): item is string => Boolean(item));
  const simpleRangeStatusLabel = simpleToolbarMode
    ? `当前展示 ${filtered.length}/${totalConfigCount} ${currentScopeUnitLabel}`
    : null;
  const expertRangeStatusMetrics: RangeStatusMetric[] = [
    { key: "unknown", label: unknownMetricLabel, value: unknownConfigCount, detail: "需核对来源", toneClass: "is-warning", filter: "UNKNOWN", active: activeDeltaFilter === "UNKNOWN" },
    { key: "common", label: commonMetricLabel, value: commonConfigCount, detail: "当前口径", toneClass: "is-common", filter: "COMMON", active: activeDeltaFilter === "COMMON" },
  ];
  const rangeStatusMetrics: RangeStatusMetric[] = [
    { key: "total", label: simpleToolbarMode ? "总配置行" : "总配置项", value: totalConfigCount, detail: simpleToolbarMode ? "xlsx 原表行数" : "原始矩阵行数", resetScope: hasRowScopeFilter, active: !hasRowScopeFilter },
    { key: "shown", label: simpleToolbarMode ? "当前展示行" : "当前展示", value: filtered.length, detail: currentScopeUnitLabel, toneClass: hasRowScopeFilter ? "is-filtered" : undefined },
    { key: "difference", label: differenceMetricLabel, value: differenceConfigCount, detail: "当前口径", toneClass: "is-difference", filter: "DIFFERENCE", active: activeDeltaFilter === "DIFFERENCE" },
    ...(simpleToolbarMode ? [] : expertRangeStatusMetrics),
  ];
  const scopeChips = [
    baseTrim ? { key: "base", label: toolbarMode === "simple" ? "基准列" : "基准", value: trimLabel(baseTrim) } : null,
    {
      key: "target",
      label: toolbarMode === "simple" ? "目标配置列" : "目标",
      value: activeTargetTrim ? trimLabel(activeTargetTrim) : toolbarMode === "simple" ? "全部目标列" : "全部对比对象",
    },
    { key: "scope", label: "范围", value: rangeScopeLabel },
    activeCategoryFilter ? { key: "category", label: "大类", value: categoryLabel(activeCategoryFilter) } : null,
    normalizedSearch ? { key: "search", label: "搜索", value: normalizedSearch } : null,
    { key: "shown", label: "当前", value: `${filtered.length} ${currentScopeUnitLabel}` },
  ].filter((chip): chip is { key: string; label: string; value: string } => Boolean(chip));
  const matrixColumnMode = columnMode === "matrix";
  const compactCellMode = cellEvidenceMode === "compact";
  const allTrimEntries: VisibleTrimEntry[] = data.trims.map((trim, index) => ({ trim, index }));
  const targetFocusedMatrixMode = matrixColumnMode && Boolean(baseTrim && activeTargetTrim);
  const visibleTrimEntries = targetFocusedMatrixMode
    ? allTrimEntries.filter(({ trim }) => trim.trimId === baseTrim?.trimId || trim.trimId === activeTargetTrim?.trimId)
    : allTrimEntries;
  const targetQuickOptions = toolbarMode === "simple" && baseModeActive && onTargetTrimChange
    ? data.trims.filter((trim) => trim.trimId !== baseTrim?.trimId).map((trim) => {
        const differenceCount = rowsWithDeltas.filter(({ row, deltas }) => (
          (!activeCategoryFilter || row.category === activeCategoryFilter)
          && rowMatchesConfigSearch(row, activeSearch)
          && rowHasDifference(row, semanticDeltasForTarget(deltas, trim.trimId), baseModeActive)
        )).length;
        return { trim, differenceCount };
      })
    : [];
  const showTargetQuickbar = targetQuickOptions.length > 0;
  const targetQuickDifferenceTotal = targetQuickOptions.reduce((total, item) => total + item.differenceCount, 0);
  const targetQuickScopeLabel = activeTargetTrim
    ? `当前只看目标列 ${trimLabel(activeTargetTrim)}；配置行仍保持全量`
    : targetQuickOptions.length > 1
      ? "当前显示全部目标列；差异行按配置行去重"
      : "当前显示全部目标列";
  const visibleColumnCount = visibleTrimEntries.length + (matrixColumnMode ? 1 : 4);
  const tableMinWidth = (matrixColumnMode ? 280 : 870) + visibleTrimEntries.length * 220;
  const activeRowEntry = activeRowKey ? filtered.find((entry) => entry.rowKey === activeRowKey) ?? null : null;
  const activeRowSemanticDeltas = activeRowEntry ? semanticDeltasForTarget(activeRowEntry.deltas, activeTargetTrimId) : [];
  const copyScopeButtonLabel = hasRowScopeFilter ? "复制当前筛选" : hasColumnFocus ? "复制当前目标列" : "复制当前表格";
  const copyScopeSuccessPrefix = hasRowScopeFilter ? "已复制当前筛选" : hasColumnFocus ? "已复制当前目标列" : "已复制当前表格";
  const exportScope: ConfigCompareExportScope = {
    baseLabel: baseTrim ? trimLabel(baseTrim) : undefined,
    categoryLabel: activeCategoryFilter ? categoryLabel(activeCategoryFilter) : undefined,
    title: selectedDisplayTitle(visibleTrimEntries.map(({ trim }) => trim)),
    rangeLabel: rangeScopeLabel,
    rowCount: filtered.length,
    searchLabel: normalizedSearch || undefined,
    targetLabel: activeTargetTrim ? trimLabel(activeTargetTrim) : undefined,
  };
  const trimColumnRoleLabel = (trim: CompareTrimItem): string | null => {
    if (baseTrim?.trimId === trim.trimId) return toolbarMode === "simple" ? "基准列" : "基准";
    if (activeTargetTrim?.trimId === trim.trimId) return toolbarMode === "simple" ? "目标列" : "目标";
    return null;
  };

  const openEvidence = (selection: SourceEvidenceSelection): void => {
    if (onOpenEvidence) onOpenEvidence(selection);
    else setEvidenceSelection(selection);
  };
  const rowTsvFields = ({ row, deltas }: RowWithDeltas): string[] => {
    const semanticDeltas = semanticDeltasForTarget(deltas, activeTargetTrimId);
    return [
      row.featureName,
      categoryLabel(row.category),
      rowDeltaTags(row, semanticDeltas, baseModeActive).map((tag) => tag.label).join(" / "),
      ...visibleTrimEntries.map(({ index }) => cellText(row.values[index] ?? null)),
      row.businessNote || "",
    ];
  };
  const copyTsvRows = async (entries: RowWithDeltas[], successMessage: string): Promise<void> => {
    if (!navigator.clipboard?.writeText) {
      setCopyFeedback("当前浏览器不支持复制。");
      return;
    }
    const header = ["配置项", "大类", "差异类型", ...visibleTrimEntries.map(({ trim }) => trimExportLabel(trim)), "业务备注"];
    const tsv = [header, ...entries.map(rowTsvFields)].map((line) => line.map(tsvCell).join("\t")).join("\n");
    try {
      await navigator.clipboard.writeText(tsv);
      setCopyFeedback(successMessage);
    } catch (reason: unknown) {
      setCopyFeedback(reason instanceof Error ? reason.message : "复制失败，请重试。");
    }
  };
  const copyCurrentScope = async (): Promise<void> => {
    await copyTsvRows(filtered, `${copyScopeSuccessPrefix}：${filtered.length} ${currentScopeUnitLabel}。`);
  };
  const copyActiveRow = async (): Promise<void> => {
    if (!activeRowEntry) {
      setCopyFeedback("请先选择一行配置。");
      return;
    }
    await copyTsvRows([activeRowEntry], `已复制选中行：${activeRowEntry.row.featureName}`);
  };
  const buildExportPayload = (): EngineeringConfigCompareExportRequest => ({
    trimIds: visibleTrimEntries.map(({ trim }) => trim.trimId),
    baseTrimId: baseTrim?.trimId ?? visibleTrimEntries[0]?.trim.trimId ?? "",
    versionScope: data.versionScope ?? "published",
    factSource,
    filters: {
      deltaFilter: activeDeltaFilter,
      category: activeCategoryFilter ?? null,
      search: activeSearch.trim() || null,
      targetTrimId: activeTargetTrimId,
      includeBusinessSummary: businessSummaryExport.length > 0 && Boolean(businessSummaryUsage),
    },
  });
  const exportCurrentScopeXlsx = async (): Promise<void> => {
    if (filtered.length === 0) {
      setCopyFeedback("当前表格没有可导出的配置行。");
      return;
    }
    const fileName = exportFileName(exportScope);
    setExportingXlsx(true);
    try {
      const blob = await api.exportEngineeringConfigCompareXlsx(buildExportPayload());
      downloadBlob(blob, fileName);
      setCopyFeedback(`已导出 XLSX：${filtered.length} ${currentScopeUnitLabel}。`);
    } catch (reason: unknown) {
      setCopyFeedback(reason instanceof Error ? reason.message : "导出 XLSX 失败，请重试。");
    } finally {
      setExportingXlsx(false);
    }
  };
  const exportCurrentScopePdf = async (): Promise<void> => {
    if (filtered.length === 0) {
      setCopyFeedback("当前表格没有可导出的配置行。");
      return;
    }
    const fileName = exportFileName(exportScope, "pdf");
    setExportingPdf(true);
    try {
      const blob = await api.exportEngineeringConfigComparePdf(buildExportPayload());
      downloadBlob(blob, fileName);
      setCopyFeedback(`已导出 PDF：${filtered.length} ${currentScopeUnitLabel}。`);
    } catch (reason: unknown) {
      setCopyFeedback(reason instanceof Error ? reason.message : "导出 PDF 失败，请重试。");
    } finally {
      setExportingPdf(false);
    }
  };
  const renderTableActionButtons = (): React.ReactNode => (
    <>
      <button
        className="btn btn-sm btn-secondary comparison-copy-scope"
        type="button"
        onClick={() => {
          void copyCurrentScope();
        }}
      >
        {copyScopeButtonLabel}
      </button>
      <button
        className="btn btn-sm btn-secondary comparison-export-scope"
        type="button"
        disabled={exportingXlsx}
        onClick={() => {
          void exportCurrentScopeXlsx();
        }}
      >
        {exportingXlsx ? "导出中..." : "导出 XLSX"}
      </button>
      <button
        className="btn btn-sm btn-secondary comparison-print-scope"
        type="button"
        disabled={exportingPdf}
        onClick={() => {
          void exportCurrentScopePdf();
        }}
      >
        {exportingPdf ? "导出中..." : "导出 PDF"}
      </button>
    </>
  );
  const renderTableActionDisclosure = (): React.ReactNode => (
    <details
      className="comparison-table-actions-disclosure"
      aria-label="配置表操作"
      open={hasColumnFocus && !hasRowScopeFilter}
    >
      <summary>表格操作</summary>
      <div className="comparison-table-actions-disclosure__body">
        <small>
          {hasColumnFocus
            ? "当前操作只包含基准列和目标配置列，配置行仍保持当前表格范围。"
            : "复制或导出当前表格范围；筛选后会跟随当前可见配置行。"}
        </small>
        <div className="comparison-table-actions-disclosure__buttons">
          {renderTableActionButtons()}
        </div>
      </div>
    </details>
  );
  const renderTableActions = (): React.ReactNode => {
    if (filtered.length === 0) return null;
    if (simpleToolbarMode) {
      if (exportActionsRef || onExportActionsChange) return null;
      return renderTableActionDisclosure();
    }
    return renderTableActionButtons();
  };
  useEffect(() => {
    const actions: ConfigComparisonTableExportActions = {
      canExport: filtered.length > 0,
      copyCurrentScope,
      copyLabel: copyScopeButtonLabel,
      exportPdf: exportCurrentScopePdf,
      exportXlsx: exportCurrentScopeXlsx,
      exportingPdf,
      exportingXlsx,
      rangeLabel: exportScope.rangeLabel,
      rowCount: filtered.length,
      trimCount: visibleTrimEntries.length,
    };
    if (exportActionsRef) exportActionsRef.current = actions;
    onExportActionsChange?.({
      canExport: actions.canExport,
      copyLabel: actions.copyLabel,
      exportingPdf: actions.exportingPdf,
      exportingXlsx: actions.exportingXlsx,
      rangeLabel: actions.rangeLabel,
      rowCount: actions.rowCount,
      trimCount: actions.trimCount,
    });
    return () => {
      if (exportActionsRef) exportActionsRef.current = null;
    };
  }, [
    exportActionsRef,
    exportCurrentScopePdf,
    exportCurrentScopeXlsx,
    exportingPdf,
    exportingXlsx,
    exportScope.rangeLabel,
    copyCurrentScope,
    copyScopeButtonLabel,
    filtered.length,
    onExportActionsChange,
    visibleTrimEntries.length,
  ]);
  const renderFilterChip = (chip: ConfigComparisonFilterChip): React.ReactElement => {
    const label = toolbarMode === "simple" ? simpleDeltaFilterLabel(chip.key, chip.label) : chip.label;
    return (
      <button
        key={chip.key}
        type="button"
        className={`comparison-filter-chip ${activeDeltaFilter === chip.key ? "is-active" : ""}`}
        onClick={() => setActiveDeltaFilter(chip.key)}
      >
        {label} {chip.count}
      </button>
    );
  };
  const legendGroups = (
    <div className="comparison-legend-content">
      <div className="comparison-legend-group">
        <span>配置值</span>
        <div>
          {VALUE_LEGEND_ITEMS.map((item) => (
            <span className="comparison-legend-item" key={item.key}>
              <strong className={item.className}>{item.label}</strong>
              <small>{item.detail}</small>
            </span>
          ))}
        </div>
      </div>
      <div className="comparison-legend-group">
        <span>证据</span>
        <div>
          {EVIDENCE_LEGEND_ITEMS.map((item) => (
            <span className="comparison-legend-item" key={item.key}>
              <strong className={`comparison-legend-evidence ${item.className}`}>{item.label}</strong>
              <small>{item.detail}</small>
            </span>
          ))}
        </div>
      </div>
    </div>
  );
  const renderTableToolbar = (): React.ReactNode => (
    <div className={`comparison-toolbar ${toolbarMode === "simple" ? "comparison-toolbar--simple" : ""}`.trim()}>
      <div className="comparison-table-search-field">
        <SearchDropdownFilter
          allowCustomValue={!simpleToolbarMode}
          label="搜索配置"
          value={activeSearch}
          options={searchOptions}
          placeholder={simpleToolbarMode ? "选择配置项 / 大类 / 值..." : "搜索配置项 / 大类 / 值..."}
          emptyLabel="无匹配配置项"
          onChange={setActiveSearch}
        />
      </div>
      <div className="comparison-category-filter">
        <SearchDropdownFilter
          label="配置大类"
          value={activeCategoryFilter ?? ""}
          options={categoryDropdownOptions}
          placeholder="选择大类..."
          emptyLabel="当前范围无大类"
          onChange={(value) => setActiveCategoryFilter(value || null)}
        />
      </div>
      {hasScopedFilter ? (
        <button className="btn btn-sm btn-secondary comparison-reset-scope" type="button" onClick={resetTableScope}>{resetScopeButtonLabel}</button>
      ) : null}
      {visibleCategories.length > 0 ? (
        <button
          className="btn btn-sm btn-secondary comparison-category-outline-toggle"
          type="button"
          aria-label={`${allVisibleCategoriesCollapsed ? "展开" : "折叠"}当前 ${visibleCategories.length} 个配置大类`}
          onClick={toggleVisibleCategories}
        >
          {allVisibleCategoriesCollapsed ? "展开大类" : "折叠大类"}
        </button>
      ) : null}
      {renderTableActions()}
      <div className={`comparison-type-filter ${toolbarMode === "simple" ? "comparison-type-filter--simple" : ""}`} aria-label="差异类型筛选">
        <div className="comparison-type-filter-primary">
          {primaryFilterChips.map(renderFilterChip)}
        </div>
      </div>
      {toolbarMode !== "simple" ? (
        <span className="text-muted">当前 {filtered.length} {tableScopeUnitLabel(activeDeltaFilter)}</span>
      ) : null}
    </div>
  );
  const targetQuickbar: React.ReactNode = showTargetQuickbar ? (
    <section className="comparison-excel-quickbar comparison-excel-quickbar--simple" aria-label="Excel 目标列快捷选择">
      <div className="comparison-excel-quickbar__label">
        <span>目标列</span>
        <small>{targetQuickScopeLabel}</small>
      </div>
      <div className="comparison-excel-quickbar__options">
        <button
          className={`comparison-excel-target-chip ${!activeTargetTrim ? "is-active" : ""}`}
          type="button"
          aria-label={`显示全部目标列，按目标累计差异 ${targetQuickDifferenceTotal} 行次，表格差异行按配置行去重`}
          aria-pressed={!activeTargetTrim}
          onClick={() => onTargetTrimChange?.(null)}
        >
          <span>全部目标列</span>
          <small>按目标累计 {targetQuickDifferenceTotal} 行次</small>
        </button>
        {targetQuickOptions.map(({ trim, differenceCount }) => {
          const active = activeTargetTrim?.trimId === trim.trimId;
          return (
            <button
              className={`comparison-excel-target-chip ${active ? "is-active" : ""}`}
              type="button"
              key={trim.trimId}
              aria-label={`聚焦目标列：${trimLabel(trim)}，差异行 ${differenceCount}`}
              aria-pressed={active}
              onClick={() => onTargetTrimChange?.(trim.trimId)}
            >
              <span>{trimLabel(trim)}</span>
              <small>差异行 {differenceCount}</small>
            </button>
          );
        })}
      </div>
    </section>
  ) : null;
  const simpleControlsSummaryLabel = activeTargetTrim ? `目标列 ${trimLabel(activeTargetTrim)}` : "全部目标列";
  const simpleControlsSummaryDetail = [
    hasRowScopeFilter ? `筛选 ${filtered.length}/${totalConfigCount}` : `全部配置行 ${filtered.length}/${totalConfigCount}`,
    activeCategoryFilter ? categoryLabel(activeCategoryFilter) : null,
    normalizedSearch ? `搜索 ${normalizedSearch}` : null,
    activeDeltaFilter !== "ALL" ? rangeScopeLabel : null,
  ].filter((item): item is string => Boolean(item)).join(" · ");
  const simpleControlPanel: React.ReactNode = simpleToolbarMode ? (
    <details
      className="comparison-simple-controls"
      aria-label="配置表筛选和目标列"
      open={simpleControlsOpen}
      onToggle={(event) => setSimpleControlsOpen(event.currentTarget.open)}
    >
      <summary>
        <span>筛选 / 目标列</span>
        <strong>{simpleControlsSummaryLabel}</strong>
        <small>{simpleControlsSummaryDetail}</small>
      </summary>
      {simpleControlsOpen ? (
        <div className="comparison-simple-controls__body">
          {renderTableToolbar()}
          {targetQuickbar}
        </div>
      ) : null}
    </details>
  ) : (
    <>
      {renderTableToolbar()}
      {targetQuickbar}
    </>
  );

  return (
    <div aria-label="配置对比表" className="comparison-container" id="config-compare-table" tabIndex={-1}>
      {simpleControlPanel}
      {copyFeedback ? (
        <span className={`comparison-copy-feedback ${simpleToolbarMode ? "comparison-copy-feedback--simple" : ""}`.trim()} role="status">
          {copyFeedback}
        </span>
      ) : null}

      {valuesEditable ? (
        <div className="comparison-edit-state-strip" aria-label="配置表在线编辑状态" role="status">
          <span>在线编辑已开启</span>
          <small>点击配置值进入编辑；证据按钮仍可打开来源。Enter 保存，Esc 取消，Delete 清空已有值。</small>
        </div>
      ) : null}

      {toolbarMode !== "simple" || hasResettableScope ? (
        <div className="comparison-scope-strip" aria-label="当前表格口径">
          <div className="comparison-scope-chips">
            {scopeChips.map((chip) => (
              <span className="comparison-scope-chip" key={chip.key}>
                <small>{chip.label}</small>
                <strong>{chip.value}</strong>
              </span>
            ))}
          </div>
          {hasResettableScope ? (
            <button className="btn btn-sm btn-secondary comparison-scope-reset" type="button" onClick={resetTableScope}>
              {resetScopeButtonLabel}
            </button>
          ) : null}
        </div>
      ) : null}

      <section
        className={`comparison-range-status ${toolbarMode === "simple" ? "comparison-range-status--simple" : ""} ${hasRowScopeFilter ? "is-filtered" : ""}`.trim()}
        aria-label="配置表范围状态"
      >
        <div className="comparison-range-status__body">
          <span>Excel 配置表</span>
          <strong>{rangeStatusTitle}</strong>
          {simpleRangeStatusLabel ? (
            <b
              className="comparison-range-status__scope-pill"
              aria-label={`当前表格范围：${simpleRangeStatusLabel}`}
            >
              {simpleRangeStatusLabel}
            </b>
          ) : null}
          <small>{rangeStatusDetail.join(" · ")}</small>
          {simpleToolbarMode && hasRowScopeFilter ? (
            <button
              className="comparison-range-status__restore"
              type="button"
              aria-label="从状态栏恢复全部配置行"
              onClick={resetTableScope}
            >
              恢复全部
            </button>
          ) : null}
          {showDifferenceNavigator && firstVisibleDifferenceRow ? (
            <span className="comparison-range-status__jump-group" aria-label="差异行巡检">
              <button
                className="comparison-range-status__jump"
                type="button"
                onClick={() => focusDifferenceAt(0)}
              >
                {firstDifferenceButtonLabel}
              </button>
              {visibleDifferenceEntries.length > 1 ? (
                <>
                  <button
                    className="comparison-range-status__jump"
                    type="button"
                    aria-label={previousDifferenceButtonLabel}
                    onClick={focusPreviousDifference}
                  >
                    上一个
                  </button>
                  <button
                    className="comparison-range-status__jump"
                    type="button"
                    aria-label={nextDifferenceButtonLabel}
                    onClick={focusNextDifference}
                  >
                    下一个
                  </button>
                  <small>{differencePositionText}</small>
                </>
              ) : null}
            </span>
          ) : null}
          {showDifferenceNavigator && activeRowEntry ? (
            <button
              className="comparison-range-status__jump"
              type="button"
              onClick={() => {
                void copyActiveRow();
              }}
            >
              复制选中行
            </button>
          ) : null}
        </div>
        <div className="comparison-range-status__metrics">
          {rangeStatusMetrics.map((metric) => {
            const actionable = Boolean(metric.filter || metric.resetScope);
            const metricClassName = [
              "comparison-range-status__metric",
              metric.toneClass,
              actionable ? "is-actionable" : null,
              metric.active ? "is-active" : null,
            ].filter(Boolean).join(" ");
            const metricContent = (
              <>
                <small>{metric.label}</small>
                <strong>{metric.value}</strong>
                <em>{metric.detail}</em>
              </>
            );
            if (!actionable) {
              return <span className={metricClassName} key={metric.key}>{metricContent}</span>;
            }
            return (
              <button
                className={metricClassName}
                type="button"
                key={metric.key}
                aria-label={`显示${metric.label}：${metric.value}，${metric.detail}`}
                onClick={() => {
                  if (metric.resetScope) resetTableScope();
                  else if (metric.filter) setActiveDeltaFilter(metric.filter);
                }}
              >
                {metricContent}
              </button>
            );
          })}
        </div>
        {activeRowEntry ? (
          <div className="comparison-active-row-strip" aria-label="选中配置行摘要">
            <div className="comparison-active-row-strip__feature">
              <span>选中配置</span>
              <strong>{activeRowEntry.row.featureName}</strong>
              <small>{categoryLabel(activeRowEntry.row.category)} · {activeRowEntry.row.featureCode}</small>
            </div>
            <div className="comparison-active-row-strip__delta" aria-label="选中行差异类型">
              <RowDeltaTags row={activeRowEntry.row} deltas={activeRowSemanticDeltas} baseModeActive={baseModeActive} />
            </div>
            <div className="comparison-active-row-strip__values" aria-label="选中行当前可见配置值">
              {visibleTrimEntries.map(({ trim, index }) => {
                const roleLabel = trimColumnRoleLabel(trim);
                const cell = activeRowEntry.row.values[index] ?? null;
                const trimName = trimLabel(trim);
                const evidenceMeta = cellEvidenceActionMeta(cell);
                const showEvidenceMarker = evidenceMeta.key !== "source";
                return (
                  <button
                    className={`comparison-active-row-value ${roleLabel?.startsWith("基准") ? "is-base" : roleLabel?.startsWith("目标") ? "is-target" : ""}`.trim()}
                    key={trim.trimId}
                    type="button"
                    aria-label={`从选中行摘要查看 ${trim.fullTrimName || trim.trimName || "配置列"} ${activeRowEntry.row.featureName} 的配置来源`}
                    onClick={() => openEvidence({
                      row: activeRowEntry.row,
                      trim,
                      cell,
                      selectionReason: "选中配置行摘要",
                    })}
                  >
                    <small>{roleLabel ? `${roleLabel} · ${trimName}` : trimName}</small>
                    <strong>
                      <span>{cellText(cell)}</span>
                      {showEvidenceMarker ? (
                        <span
                          className={`comparison-active-row-evidence-marker ${evidenceMeta.className}`}
                          title={evidenceMeta.title}
                          aria-hidden="true"
                        >
                          {evidenceMeta.compactLabel}
                        </span>
                      ) : null}
                    </strong>
                  </button>
                );
              })}
            </div>
            <button
              className="comparison-active-row-strip__clear"
              type="button"
              onClick={() => setActiveRowKey(null)}
            >
              取消选中
            </button>
          </div>
        ) : null}
      </section>

      {legendMode === "compact" ? null : (
        <section className="comparison-legend-strip" aria-label="配置值与证据图例">
          {legendGroups}
        </section>
      )}

      <div className="comparison-table-wrapper">
        <table className={`comparison-table ${matrixColumnMode ? "comparison-table--matrix" : ""} ${compactCellMode ? "comparison-table--compact-cells" : ""}`.trim()} style={{ minWidth: tableMinWidth }}>
          <colgroup>
            <col className="comparison-col-feature" />
            {matrixColumnMode ? null : (
              <>
                <col className="comparison-col-category" />
                <col className="comparison-col-delta" />
              </>
            )}
            {visibleTrimEntries.map(({ trim }) => <col className="comparison-col-trim" key={trim.trimId} />)}
            {matrixColumnMode ? null : <col className="comparison-col-note" />}
          </colgroup>
          <thead>
            <tr>
              <th className="matrix-sticky-feature-col">配置项</th>
              {matrixColumnMode ? null : (
                <>
                  <th>大类</th>
                  <th>差异类型</th>
                </>
              )}
              {visibleTrimEntries.map(({ trim }) => {
                const roleLabel = trimColumnRoleLabel(trim);
                const headerTitle = [
                  trim.fullTrimName,
                  trimHeaderOriginLabel(trim),
                  trimHeaderIdentityLabel(trim),
                  trimHeaderTitleContextLabel(trim),
                  roleLabel,
                ].filter(Boolean).join(" · ");
                return (
                  <th className="comparison-trim-header-cell" key={trim.trimId} title={headerTitle}>
                    <span className="comparison-trim-header-name">{trim.fullTrimName}</span>
                    <span className="comparison-trim-header-identity" aria-hidden="true">
                      <span className="comparison-trim-header-origin">{trimHeaderOriginLabel(trim)}</span>
                      <span>{trimHeaderIdentityLabel(trim)}</span>
                    </span>
                    {compactCellMode ? null : (
                      <span className="comparison-trim-header-context" aria-hidden="true">{trimHeaderContextLabel(trim)}</span>
                    )}
                    {roleLabel ? (
                      <span className={`comparison-trim-header-role ${roleLabel.startsWith("基准") ? "is-base" : "is-target"}`} aria-hidden="true">
                        {roleLabel}
                      </span>
                    ) : null}
                  </th>
                );
              })}
              {matrixColumnMode ? null : <th>业务备注</th>}
            </tr>
          </thead>
          <tbody>
            {filtered.length === 0 ? (
              <tr>
                <td className="comparison-empty-table-cell" colSpan={visibleColumnCount}>
                  <div className="comparison-empty-table-state">
                    <strong>{emptyTableTitle}</strong>
                    <small>{emptyTableDetail}</small>
                    {hasResettableScope ? (
                      <button className="btn btn-sm btn-secondary" type="button" onClick={resetTableScope}>
                        {emptyTableResetLabel}
                      </button>
                    ) : null}
                  </div>
                </td>
              </tr>
            ) : null}
            {Object.entries(byCat).map(([cat, rows]) => {
              const isCollapsed = collapsed.has(cat);
              const categorySummary = summarizeCategoryDeltas(rows, baseModeActive, activeTargetTrimId);
              const summaryChips = categoryDeltaSummaryChips(categorySummary);
              const showCategorySummaryChips = categorySummaryMode === "full" && summaryChips.length > 0;
              return (
                <React.Fragment key={cat}>
                  <tr
                    id={categoryAnchorId(cat)}
                    className="comparison-category-row"
                    onClick={() => toggleCategory(cat)}
                  >
                    <td colSpan={visibleColumnCount}>
                      <div className="comparison-category-row-content">
                        <button
                          className="comparison-category-toggle-button"
                          type="button"
                          aria-expanded={!isCollapsed}
                          aria-label={`${isCollapsed ? "展开" : "折叠"} ${categoryLabel(cat)} 配置大类`}
                          onClick={(event) => {
                            event.stopPropagation();
                            toggleCategory(cat);
                          }}
                        >
                          {isCollapsed ? "▶" : "▼"}
                        </button>
                        <span className="comparison-category-name">{categoryLabel(cat)}</span>
                        <span className="comparison-category-count">{categoryCountText(rows.length, categorySummary.baseDifferenceRows, activeDeltaFilter, activeTargetTrimId)}</span>
                        {showCategorySummaryChips ? (
                          <span className="comparison-category-summary" aria-label={`${categoryLabel(cat)} 当前大类差异摘要`}>
                            {summaryChips.map((chip) => (
                              <button
                                className={`comparison-category-summary-chip ${chip.className}`}
                                type="button"
                                key={chip.key}
                                aria-label={`查看 ${categoryLabel(cat)} 大类摘要：${chip.label}项，共 ${chip.count} 项`}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  expandCategory(cat);
                                  setActiveCategoryFilter(cat);
                                  setActiveDeltaFilter(chip.filter);
                                }}
                              >
                                {chip.label} {chip.count}
                              </button>
                            ))}
                          </span>
                        ) : null}
                      </div>
                    </td>
                  </tr>
                  {!isCollapsed && rows.map(({ row, rowAnchorKey, deltas, rowKey }, rowIndex) => {
                    const semanticDeltas = semanticDeltasForTarget(deltas, activeTargetTrimId);
                    const className = rowClassName(row, semanticDeltas, baseModeActive);
                    const semanticLabel = rowSemanticLabel(row, semanticDeltas, baseModeActive);
                    const rowActive = activeRowKey === rowKey;
                    const matrixHintTags = matrixColumnMode ? rowMatrixHintTags(row, semanticDeltas, baseModeActive) : [];
                    const renderRowKey = `${cat}-${rowKey}-${rowIndex}`;
                    return (
                      <tr
                        id={featureRowAnchorId(rowAnchorKey)}
                        key={renderRowKey}
                        className={`${className} ${rowActive ? "compare-row-active" : ""}`.trim()}
                        aria-label={`${row.featureName}，${semanticLabel}`}
                        aria-selected={rowActive}
                        onClick={() => toggleActiveRow(rowKey)}
                      >
                        <td
                          className="matrix-sticky-feature-col comparison-feature-cell"
                          title={`${row.featureName} · ${categoryLabel(row.category)} · ${row.featureCode}`}
                        >
                          <span>{row.featureName}</span>
                          {matrixHintTags.length > 0 ? (
                            <span className="comparison-feature-delta-tags" aria-label={`${row.featureName} 差异提示`}>
                              {matrixHintTags.map((tag) => (
                                <span className={`comparison-feature-delta-tag ${tag.className}`} key={tag.key}>
                                  {tag.label}
                                </span>
                              ))}
                            </span>
                          ) : null}
                          <small>{row.featureCode}</small>
                        </td>
                        {matrixColumnMode ? null : (
                          <>
                            <td>{categoryLabel(row.category)}</td>
                            <td>
                              <RowDeltaTags row={row} deltas={semanticDeltas} baseModeActive={baseModeActive} />
                            </td>
                          </>
                        )}
                        {visibleTrimEntries.map(({ trim, index }) => (
                          <CellDisplay
                            key={`${renderRowKey}-${trim.trimId}`}
                            cell={row.values[index] ?? null}
                            editable={valuesEditable}
                            evidenceMode={cellEvidenceMode}
                            featureName={row.featureName}
                            row={row}
                            rowType={row.comparisonType}
                            trim={trim}
                            onOpenEvidence={() => openEvidence({ row, trim, cell: row.values[index] ?? null })}
                            onSaveCell={onSaveCell}
                          />
                        ))}
                        {matrixColumnMode ? null : <td className="comparison-note-cell">{row.businessNote || "-"}</td>}
                      </tr>
                    );
                  })}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
      {legendMode === "compact" ? (
        <details className="comparison-legend-strip comparison-legend-strip--compact" aria-label="配置值与证据图例">
          <summary className="comparison-legend-summary">
            <span>图例 / 证据说明</span>
            <small>标配、选装、不配备*、待确认与来源标记</small>
          </summary>
          {legendGroups}
        </details>
      ) : null}
      {!onOpenEvidence && evidenceSelection ? (
        <React.Suspense fallback={null}>
          <LazySourceEvidenceDrawer selection={evidenceSelection} onClose={() => setEvidenceSelection(null)} />
        </React.Suspense>
      ) : null}
    </div>
  );
}
