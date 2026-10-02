import { useMemo, useState } from "react";

import { LoadingActionButton } from "../LoadingActionButton";
import { UploadDigestPanel } from "../UploadDigestPanel";
import type { UploadDigestMetric } from "../UploadDigestPanel";
import type { VehicleImportPreview, VehicleImportPreviewRow } from "../../types/orderGeniusVehicle";
import { SheetGroupedPreview, type SheetGroupedPreviewGroup } from "../workbench/SheetGroupedPreview";

interface VehicleImportDigestPanelProps {
  preview: VehicleImportPreview | null;
  busy: boolean;
  exporting?: boolean;
  compact?: boolean;
  onPickFile: () => void;
  onApply: () => void | Promise<void>;
  onExport?: () => void | Promise<void>;
  onClear?: () => void;
  onTargetChange?: (sourceRow: number, carCode: string) => void;
}

interface VehicleImportSummary {
  parsed: number;
  matched: number;
  ready: number;
  duplicate: number;
  invalid: number;
  overflow: number;
  errors: number;
  warnings: number;
  newUnits: number;
  updatedUnits: number;
}

function issueMatches(message: string, pattern: RegExp): boolean {
  return pattern.test(message.toLowerCase());
}

function countIssue(errors: string[], pattern: RegExp): number {
  return errors.filter((error) => issueMatches(error, pattern)).length;
}

function summarizePreview(preview: VehicleImportPreview | null): VehicleImportSummary {
  if (!preview) {
    return {
      parsed: 0,
      matched: 0,
      ready: 0,
      duplicate: 0,
      invalid: 0,
      overflow: 0,
      errors: 0,
      warnings: 0,
      newUnits: 0,
      updatedUnits: 0,
    };
  }
  const rowErrors = new Set(
    preview.previewRows
      .filter((row) => row.errors.length > 0)
      .map((row) => row.sourceRow),
  );
  const duplicate = countIssue(preview.errors, /duplicate|duplicates/);
  const invalid = countIssue(preview.errors, /invalid|format|required/);
  const overflow = countIssue(preview.errors, /overflow|exceed|no empty|slot/);
  const ready = preview.previewRows.filter((row) => !rowErrors.has(row.sourceRow)).length;
  return {
    parsed: preview.totalRows,
    matched: preview.newUnits + preview.updatedUnits,
    ready,
    duplicate,
    invalid,
    overflow,
    errors: preview.errors.length,
    warnings: preview.warnings.length,
    newUnits: preview.newUnits,
    updatedUnits: preview.updatedUnits,
  };
}

function buildMetrics(summary: VehicleImportSummary): UploadDigestMetric[] {
  return [
    { label: "Parsed", value: summary.parsed },
    { label: "Matched", value: summary.matched, tone: summary.matched > 0 ? "success" : "neutral" },
    { label: "Ready", value: summary.ready, tone: summary.ready > 0 ? "success" : "neutral" },
    { label: "New", value: summary.newUnits, tone: summary.newUnits > 0 ? "success" : "neutral" },
    { label: "Updated", value: summary.updatedUnits, tone: summary.updatedUnits > 0 ? "success" : "neutral" },
    { label: "Duplicate", value: summary.duplicate, tone: summary.duplicate > 0 ? "danger" : "neutral" },
    { label: "Invalid", value: summary.invalid, tone: summary.invalid > 0 ? "danger" : "neutral" },
    { label: "Overflow", value: summary.overflow, tone: summary.overflow > 0 ? "danger" : "neutral" },
    { label: "Errors", value: summary.errors, tone: summary.errors > 0 ? "danger" : "neutral" },
    { label: "Warnings", value: summary.warnings, tone: summary.warnings > 0 ? "warning" : "neutral" },
  ];
}

function rowStatus(row: VehicleImportPreviewRow): string {
  if (row.errors.length > 0) {
    return row.errors.join("; ");
  }
  if (row.warnings.length > 0) {
    return row.warnings.join("; ");
  }
  return row.action === "create" ? "Ready to create" : "Ready to update";
}

export function VehicleImportDigestPanel({
  preview,
  busy,
  exporting = false,
  compact = false,
  onPickFile,
  onApply,
  onExport,
  onClear,
  onTargetChange,
}: VehicleImportDigestPanelProps) {
  const summary = useMemo(() => summarizePreview(preview), [preview]);
  const scoped = preview?.mode === "pi_vin_fill";
  const removing = Boolean(preview?.removeVins);
  const ready = scoped ? preview.updatedUnits : summary.ready;
  const canApply = Boolean(preview) && preview?.status !== "error" && ready > 0;
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [previewTouched, setPreviewTouched] = useState(false);
  const [editingRow, setEditingRow] = useState<number | null>(null);
  const groups: SheetGroupedPreviewGroup<VehicleImportPreviewRow>[] = [];
  if (scoped) {
    const byMaterial = new Map<string, VehicleImportPreviewRow[]>();
    for (const row of preview.previewRows) {
      const key = row.materialCode || "Missing BOM / 缺物料";
      byMaterial.set(key, [...(byMaterial.get(key) ?? []), row]);
    }
    for (const [key, rows] of byMaterial) {
      groups.push({ key, title: key, rows, metrics: [
        { label: removing ? "Clear / 清除" : "Fill / 新增", value: rows.filter((row) => row.action === (removing ? "remove" : "fill")).length },
        ...(!removing ? [{ label: "Replace / 替换", value: rows.filter((row) => row.action === "replace").length }] : []),
        { label: removing ? "Skip / 已清跳过" : "Skip / 已录跳过", value: rows.filter((row) => row.action === "skip").length },
        { label: "Conflict / 冲突", value: rows.filter((row) => row.action === "conflict").length },
        { label: "Remaining / 应用后待录", value: (preview.targetVehicles ?? []).filter((v) => v.materialCode === key && !v.vin).length - rows.filter((row) => row.action === "fill").length + rows.filter((row) => row.action === "remove").length },
      ] });
    }
  }
  const previewRows = compact ? preview?.previewRows.slice(0, 12) ?? [] : preview?.previewRows.slice(0, 80) ?? [];
  const hiddenRows = preview ? Math.max(0, preview.previewRows.length - previewRows.length) : 0;

  return (
    <section className={`vehicle-import-digest-panel${compact ? " is-compact" : ""}`}>
      <UploadDigestPanel
        title={removing ? "Clear VIN preview / 清 VIN 预览" : scoped ? "BOM + VIN preview / 匹配预览" : "Vehicle import preview"}
        subtitle={scoped ? `PI: ${preview.piCode} · VIN only; no new vehicles / 仅补 VIN，不新增车辆。确认目标后再 Apply。` : "Excel workbooks and parsed image rows use the same digest and apply workflow. Preview first, then apply valid rows."}
        metrics={scoped ? [
          { label: "Rows / 文件行", value: preview.totalRows },
          { label: removing ? "Clear / 清除" : "Fill / 新增", value: (removing ? preview.removedUnits : preview.filledUnits) ?? 0 },
          ...(!removing ? [{ label: "Replace / 替换", value: preview.replacedUnits ?? 0 }] : []),
          { label: removing ? "Skip / 已清跳过" : "Skip / 已录跳过", value: preview.skippedUnits ?? 0 },
          { label: "Conflicts / 冲突", value: preview.conflictUnits ?? 0, tone: preview.status === "error" ? "danger" : "neutral" },
          { label: "Remaining / 应用后待录", value: preview.remainingUnits ?? 0 },
        ] : buildMetrics(summary)}
        errors={preview?.errors ?? []}
        warnings={preview?.warnings ?? []}
        footer={
          <div className="vehicle-import-actions">
            <LoadingActionButton
              variant="secondary"
              size={compact ? "sm" : "default"}
              loading={busy}
              loadingLabel="Previewing..."
              onClick={onPickFile}
            >
              Import File
            </LoadingActionButton>
            {onExport ? (
              <LoadingActionButton
                variant="secondary"
                size={compact ? "sm" : "default"}
                loading={exporting}
                loadingLabel="Exporting..."
                onClick={() => void onExport()}
              >
                Export View
              </LoadingActionButton>
            ) : null}
            {onClear && preview ? (
              <button type="button" className="btn btn-ghost" onClick={onClear} disabled={busy}>
                Clear
              </button>
            ) : null}
            <LoadingActionButton
              size={compact ? "sm" : "default"}
              loading={busy}
              loadingLabel="Applying..."
              disabled={!canApply}
              onClick={() => void onApply()}
            >
              Apply {ready}
            </LoadingActionButton>
          </div>
        }
      >
        {scoped ? (
          <SheetGroupedPreview<VehicleImportPreviewRow>
            title="By material / 按物料分组"
            groups={groups}
            columns={[{ key: "row", label: "Row / 行" }, { key: "vin", label: "Old → new VIN / 旧→新 VIN" },
              { key: "target", label: "Target CarCode / 目标车辆" }, { key: "status", label: "Status / 状态" }]}
            expandedGroupKeys={expandedGroups} previewTouched={previewTouched} emptyText="No file rows / 文件无数据"
            onToggleGroup={(group, expanded) => {
              setPreviewTouched(true);
              setExpandedGroups((current) => {
                const next = new Set(current); if (expanded) next.delete(group.key); else next.add(group.key); return next;
              });
            }}
            renderRow={(row) => (
              <tr key={row.sourceRow}>
                <td>{row.sourceRow}</td>
                <td>{removing ? `${row.vin} → —` : `${row.oldVin ?? "—"} → ${row.vin}`}</td>
                <td>
                  {editingRow === row.sourceRow ? <select aria-label={`Target CarCode row ${row.sourceRow}`} value={row.carCode ?? ""}
                    disabled={busy || row.action === "skip"} onChange={(event) => { setEditingRow(null); onTargetChange?.(row.sourceRow, event.target.value); }}>
                    <option value="">Select target / 指定目标</option>
                    {(preview.targetVehicles ?? []).filter((v) => v.materialCode === row.materialCode && (!v.vin || preview.allowReplacing || v.carCode === row.carCode)).map((v) => (
                      <option key={v.carCode} value={v.carCode}>{v.carCode} · {v.countryCode} · {v.vin ?? "Empty / 空位"}</option>
                    ))}
                  </select> : <button type="button" disabled={busy || row.action === "skip" || removing}
                    onClick={() => setEditingRow(row.sourceRow)} aria-label={`Choose target row ${row.sourceRow}`}>
                    {row.carCode ?? "Select target / 指定目标"}
                  </button>}
                </td>
                <td>{row.errors.length ? row.errors.join("; ") : ({ fill: "Ready to fill / 待补录", replace: "Replace / 替换", remove: "Clear VIN only / 仅清 VIN", skip: removing ? "Already empty / 已清跳过" : "Already imported / 已录跳过", conflict: "Conflict / 冲突", create: "", update: "" })[row.action]}</td>
              </tr>
            )}
          />
        ) : preview ? (
          <div className="vehicle-import-preview-table">
            <div className="vehicle-import-preview-head">
              <span>Row</span>
              <span>Action</span>
              <span>PI</span>
              <span>Car / VIN</span>
              <span>Material</span>
              <span>Status</span>
            </div>
            {previewRows.map((row) => (
              <div
                key={`${row.sourceRow}-${row.carCode ?? row.vin ?? row.materialCode ?? "row"}`}
                className={`vehicle-import-preview-row${row.errors.length > 0 ? " is-error" : ""}`}
              >
                <span>{row.sourceRow}</span>
                <strong>{row.action}</strong>
                <span>{row.piCode ?? "-"}</span>
                <span>{row.carCode ?? row.vin ?? "-"}</span>
                <span>{row.materialCode ?? "-"}</span>
                <span>{rowStatus(row)}</span>
              </div>
            ))}
            {hiddenRows > 0 ? (
              <div className="vehicle-import-preview-empty">{hiddenRows} more preview rows hidden.</div>
            ) : null}
          </div>
        ) : (
          <div className="vehicle-import-preview-empty">
            Choose an Excel workbook or image file to parse rows, detect duplicates and validate VINs before applying.
          </div>
        )}
      </UploadDigestPanel>
    </section>
  );
}
