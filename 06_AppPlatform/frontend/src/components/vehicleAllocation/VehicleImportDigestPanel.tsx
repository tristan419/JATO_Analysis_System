import { useMemo } from "react";

import { LoadingActionButton } from "../LoadingActionButton";
import { UploadDigestPanel } from "../UploadDigestPanel";
import type { UploadDigestMetric } from "../UploadDigestPanel";
import type { VehicleImportPreview, VehicleImportPreviewRow } from "../../types/orderGeniusVehicle";

interface VehicleImportDigestPanelProps {
  preview: VehicleImportPreview | null;
  busy: boolean;
  exporting?: boolean;
  compact?: boolean;
  onPickFile: () => void;
  onApply: () => void | Promise<void>;
  onExport?: () => void | Promise<void>;
  onClear?: () => void;
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
}: VehicleImportDigestPanelProps) {
  const summary = useMemo(() => summarizePreview(preview), [preview]);
  const canApply = Boolean(preview) && preview?.status !== "error" && summary.ready > 0;
  const previewRows = compact ? preview?.previewRows.slice(0, 12) ?? [] : preview?.previewRows.slice(0, 80) ?? [];
  const hiddenRows = preview ? Math.max(0, preview.previewRows.length - previewRows.length) : 0;

  return (
    <section className={`vehicle-import-digest-panel${compact ? " is-compact" : ""}`}>
      <UploadDigestPanel
        title="Vehicle import preview"
        subtitle="Excel workbooks and parsed image rows use the same digest and apply workflow. Preview first, then apply valid rows."
        metrics={buildMetrics(summary)}
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
              Apply {summary.ready}
            </LoadingActionButton>
          </div>
        }
      >
        {preview ? (
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
