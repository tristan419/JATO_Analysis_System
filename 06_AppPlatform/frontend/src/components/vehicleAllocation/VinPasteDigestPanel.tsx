import { useMemo } from "react";

import { LoadingActionButton } from "../LoadingActionButton";
import { UploadDigestPanel } from "../UploadDigestPanel";
import type { PiVehicleUnit } from "../../types/orderGeniusVehicle";
import type { UploadDigestMetric } from "../UploadDigestPanel";

interface VinPasteDigestPanelProps {
  scopeLabel: string;
  vehicles: PiVehicleUnit[];
  pasteText: string;
  applying: boolean;
  fileBusy?: boolean;
  applyMessage?: string;
  onPasteTextChange: (value: string) => void;
  onPickFile?: () => void;
  onApply: (vins: string[]) => void | Promise<void>;
}

interface VinPasteRow {
  lineNumber: number;
  vin: string;
  target: PiVehicleUnit | null;
  issue: VinPasteIssue;
  error: string;
  warning: string;
}

type VinPasteIssue = "" | "invalid" | "duplicate" | "existing" | "overflow";

interface VinPasteSummary {
  pasted: number;
  matched: number;
  ready: number;
  duplicate: number;
  invalid: number;
  existing: number;
  overflow: number;
  errors: number;
  warnings: number;
}

const VIN_PATTERN = /^[A-HJ-NPR-Z0-9]{17}$/;

function parseVinLines(value: string): string[] {
  return value
    .split(/\r?\n|[\t,; ]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function sortVehicleScope(vehicles: PiVehicleUnit[]): PiVehicleUnit[] {
  return [...vehicles].sort((a, b) => {
    const lineCompare = a.piLineCode.localeCompare(b.piLineCode);
    return lineCompare || a.carCode.localeCompare(b.carCode);
  });
}

function buildVinPreview(pasteText: string, vehicles: PiVehicleUnit[]): VinPasteRow[] {
  const vins = parseVinLines(pasteText);
  const emptyVehicles = sortVehicleScope(vehicles).filter((vehicle) => !vehicle.vin);
  const existingVins = new Set(
    vehicles
      .map((vehicle) => vehicle.vin?.trim().toUpperCase())
      .filter((vin): vin is string => Boolean(vin)),
  );
  const seen = new Map<string, number>();

  return vins.map((vin, index) => {
    const firstSeen = seen.get(vin);
    seen.set(vin, index + 1);
    const target = emptyVehicles[index] ?? null;
    let issue: VinPasteIssue = "";
    let error = "";
    let warning = "";

    if (!VIN_PATTERN.test(vin)) {
      issue = "invalid";
      error = `${vin} is not a 17-character VIN`;
    } else if (firstSeen !== undefined) {
      issue = "duplicate";
      error = `${vin} duplicates line ${firstSeen}`;
    } else if (existingVins.has(vin)) {
      issue = "existing";
      error = `${vin} already exists in current PI scope`;
    } else if (!target) {
      issue = "overflow";
      error = `${vin} has no empty vehicle slot in this scope`;
    }

    if (!error && target && target.materialCode) {
      warning = `${target.carCode} · ${target.materialCode}`;
    }

    return {
      lineNumber: index + 1,
      vin,
      target,
      issue,
      error,
      warning,
    };
  });
}

function summarizeVinPreview(rows: VinPasteRow[]): VinPasteSummary {
  const duplicate = rows.filter((row) => row.issue === "duplicate").length;
  const invalid = rows.filter((row) => row.issue === "invalid").length;
  const existing = rows.filter((row) => row.issue === "existing").length;
  const overflow = rows.filter((row) => row.issue === "overflow").length;
  const ready = rows.filter((row) => !row.error && row.target).length;
  return {
    pasted: rows.length,
    matched: ready,
    ready,
    duplicate,
    invalid,
    existing,
    overflow,
    errors: duplicate + invalid + existing + overflow,
    warnings: rows.filter((row) => row.warning).length,
  };
}

function buildMetrics(summary: VinPasteSummary, scopeEmptyVehicles: number): UploadDigestMetric[] {
  return [
    { label: "Pasted", value: summary.pasted },
    { label: "Matched", value: summary.matched, tone: summary.matched > 0 ? "success" : "neutral" },
    { label: "Ready", value: summary.ready, tone: summary.ready > 0 ? "success" : "neutral" },
    { label: "Duplicate", value: summary.duplicate, tone: summary.duplicate > 0 ? "danger" : "neutral" },
    { label: "Invalid", value: summary.invalid, tone: summary.invalid > 0 ? "danger" : "neutral" },
    { label: "Overflow", value: summary.overflow, tone: summary.overflow > 0 ? "danger" : "neutral" },
    { label: "Errors", value: summary.errors, tone: summary.errors > 0 ? "danger" : "neutral" },
    { label: "Warnings", value: summary.warnings, tone: summary.warnings > 0 ? "warning" : "neutral" },
    { label: "Empty VIN", value: scopeEmptyVehicles, tone: scopeEmptyVehicles > 0 ? "warning" : "neutral" },
  ];
}

export function VinPasteDigestPanel({
  scopeLabel,
  vehicles,
  pasteText,
  applying,
  fileBusy = false,
  applyMessage,
  onPasteTextChange,
  onPickFile,
  onApply,
}: VinPasteDigestPanelProps) {
  const previewRows = useMemo(
    () => buildVinPreview(pasteText, vehicles),
    [pasteText, vehicles],
  );
  const scopeEmptyVehicles = vehicles.filter((vehicle) => !vehicle.vin).length;
  const summary = summarizeVinPreview(previewRows);
  const readyRows = previewRows.filter((row) => !row.error && row.target);
  const errors = previewRows
    .filter((row) => row.error)
    .map((row) => `Line ${row.lineNumber}: ${row.error}`);
  const warnings = previewRows
    .filter((row) => row.warning)
    .slice(0, 12)
    .map((row) => `Line ${row.lineNumber}: ${row.warning}`);

  return (
    <section className="vin-paste-digest-panel">
      <UploadDigestPanel
        title="VIN paste preview"
        subtitle={`Scope: ${scopeLabel}. VINs assign to empty vehicles in PI line and car-code order.`}
        metrics={buildMetrics(summary, scopeEmptyVehicles)}
        errors={errors}
        warnings={warnings}
        footer={
          <div className="vin-paste-actions">
            {onPickFile ? (
              <LoadingActionButton
                variant="secondary"
                size="sm"
                loading={fileBusy}
                loadingLabel="Reading..."
                onClick={onPickFile}
              >
                Import VIN XLSX
              </LoadingActionButton>
            ) : null}
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              onClick={() => onPasteTextChange("")}
              disabled={applying || fileBusy || !pasteText.trim()}
            >
              Clear
            </button>
            <LoadingActionButton
              size="sm"
              loading={applying}
              loadingLabel="Applying..."
              disabled={fileBusy || readyRows.length === 0 || errors.length > 0}
              onClick={() => void onApply(readyRows.map((row) => row.vin))}
            >
              Apply {readyRows.length}
            </LoadingActionButton>
            {applyMessage ? <span className="vin-paste-message">{applyMessage}</span> : null}
          </div>
        }
      >
        <textarea
          className="vin-paste-input"
          value={pasteText}
          placeholder="Paste VINs from Excel, one per line..."
          onChange={(event) => onPasteTextChange(event.target.value)}
        />
        <div className="vin-paste-preview-table">
          <div className="vin-paste-preview-head">
            <span>Line</span>
            <span>VIN</span>
            <span>Target vehicle</span>
            <span>Status</span>
          </div>
          {previewRows.length === 0 ? (
            <div className="vin-paste-preview-empty">Paste VINs to preview assignment before applying.</div>
          ) : previewRows.slice(0, 80).map((row) => (
            <div
              key={`${row.lineNumber}-${row.vin}`}
              className={`vin-paste-preview-row${row.error ? " is-error" : ""}`}
            >
              <span>{row.lineNumber}</span>
              <strong>{row.vin}</strong>
              <span>{row.target ? row.target.carCode : "-"}</span>
              <span>{row.error || row.warning || "Ready"}</span>
            </div>
          ))}
          {previewRows.length > 80 ? (
            <div className="vin-paste-preview-empty">{previewRows.length - 80} more rows hidden.</div>
          ) : null}
        </div>
      </UploadDigestPanel>
    </section>
  );
}
