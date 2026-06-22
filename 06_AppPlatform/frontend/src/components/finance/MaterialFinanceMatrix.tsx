import { Fragment, type FocusEvent, useEffect, useRef, useState } from "react";
import { animate, stagger } from "animejs";

import type {
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
} from "../../types/orderGenius";
import { useStaggerEntrance } from "../../hooks/useStaggerEntrance";
import { LoadingActionButton } from "../LoadingActionButton";

interface MaterialFinanceDraft {
  fobEur: string;
  vehicleMarginEur: string;
  vehicleMarginRatePercent: string;
  vehicleProfitEur: string;
  vehicleProfitRatePercent: string;
  fobDeltaEur: string;
  marginDeltaEur: string;
  memo: string;
}

type MaterialFinanceSaveState = "saved" | "dirty" | "saving" | "error";

const FINANCE_AUTOSAVE_DELAY_MS = 1200;

interface MaterialFinanceMatrixProps {
  rows: CountryMaterialFinanceRow[];
  title?: string;
  density?: "compact" | "standard";
  savingMaterialCode?: string | null;
  onViewHistory?: (row: CountryMaterialFinanceRow) => void | Promise<void>;
  onSaveRow: (row: CountryMaterialFinanceRow, update: CountryMaterialFinanceUpdate) => void | Promise<void>;
}

interface MaterialFinanceGroup {
  key: string;
  brand: string;
  modelName: string;
  powertrain: string;
  rows: CountryMaterialFinanceRow[];
}

function formatMoney(value: number | null): string {
  return value == null ? "-" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function compareText(left: string, right: string): number {
  return left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" });
}

function normalizedText(value: string | null): string {
  return (value ?? "").trim();
}

function brandRank(value: string | null): number {
  const brand = normalizedText(value).toUpperCase();
  if (brand.includes("OMODA")) return 0;
  if (brand.includes("JAECOO")) return 1;
  return 9;
}

function modelNumberRank(value: string | null): number {
  const model = normalizedText(value).toUpperCase();
  const match = model.match(/(?:OMODA|JAECOO)\s*(\d+)/);
  if (!match) return Number.MAX_SAFE_INTEGER;
  return Number(match[1]);
}

function powertrainRank(value: string | null): number {
  const powertrain = normalizedText(value).toUpperCase();
  if (powertrain === "ICE") return 0;
  if (powertrain === "HEV") return 1;
  if (powertrain === "BEV") return 2;
  if (powertrain === "PHEV" || powertrain.includes("SHS")) return 3;
  return 9;
}

function powertrainTone(value: string | null): string {
  const powertrain = normalizedText(value).toUpperCase();
  if (powertrain === "ICE") return "ice";
  if (powertrain === "HEV") return "hev";
  if (powertrain === "BEV") return "bev";
  if (powertrain === "PHEV" || powertrain.includes("SHS")) return "phev";
  return "other";
}

function compareFinanceRows(left: CountryMaterialFinanceRow, right: CountryMaterialFinanceRow): number {
  return brandRank(left.brand) - brandRank(right.brand)
    || modelNumberRank(left.modelName) - modelNumberRank(right.modelName)
    || powertrainRank(left.powertrain) - powertrainRank(right.powertrain)
    || compareText(normalizedText(left.modelName), normalizedText(right.modelName))
    || compareText(normalizedText(left.version), normalizedText(right.version))
    || compareText(left.materialCode, right.materialCode);
}

function buildFinanceGroups(rows: CountryMaterialFinanceRow[]): MaterialFinanceGroup[] {
  const grouped = new Map<string, MaterialFinanceGroup>();
  [...rows].sort(compareFinanceRows).forEach((row) => {
    const brand = normalizedText(row.brand) || "-";
    const modelName = normalizedText(row.modelName) || "-";
    const powertrain = normalizedText(row.powertrain) || "-";
    const key = `${brand}::${modelName}::${powertrain}`;
    const existing = grouped.get(key);
    if (existing) {
      existing.rows.push(row);
      return;
    }
    grouped.set(key, {
      key,
      brand,
      modelName,
      powertrain,
      rows: [row],
    });
  });
  return Array.from(grouped.values());
}

function formatDateTime(value: string | null): string {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatSourceMode(value: string | null): string {
  const mode = (value ?? "").trim();
  if (!mode) return "Base";
  const labels: Record<string, string> = {
    manual: "Manual",
    copied: "Copied",
    uploaded: "Uploaded",
    adjusted: "Adjusted",
    cell_edit: "Cell edit",
    copied_from_country: "Country copy",
    manual_country_adjust: "Country adjust",
  };
  return labels[mode] ?? mode.replaceAll("_", " ");
}

function sourceModeTone(value: string | null): string {
  const mode = (value ?? "").trim();
  if (mode === "copied" || mode === "copied_from_country") return "copied";
  if (mode === "uploaded") return "uploaded";
  if (mode === "adjusted" || mode === "manual_country_adjust") return "adjusted";
  if (mode === "manual" || mode === "cell_edit") return "manual";
  return "base";
}

function sourceModeTitle(row: CountryMaterialFinanceRow): string {
  const copiedFrom = typeof row.sourcePayload?.copiedFromBomTemplate === "string"
    ? row.sourcePayload.copiedFromBomTemplate
    : "";
  return [
    `Source: ${formatSourceMode(row.sourceMode)}`,
    copiedFrom ? `Copied from ${copiedFrom}` : "",
    row.updatedBy ? `Updated by ${row.updatedBy}` : "",
    row.updatedAtUtc ? `Updated at ${formatDateTime(row.updatedAtUtc)}` : "",
  ].filter(Boolean).join(" · ");
}

function getSourcePayloadNumber(row: CountryMaterialFinanceRow, key: string): number | null {
  const value = row.sourcePayload?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function getSourcePayloadStringList(row: CountryMaterialFinanceRow, key: string): string[] {
  const value = row.sourcePayload?.[key];
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string" && item.trim().length > 0)
    : [];
}

function draftFromRow(row: CountryMaterialFinanceRow): MaterialFinanceDraft {
  return {
    fobEur: row.fobEur == null ? "" : String(row.fobEur),
    vehicleMarginEur: row.vehicleMarginEur == null ? (row.marginEur == null ? "" : String(row.marginEur)) : String(row.vehicleMarginEur),
    vehicleMarginRatePercent: row.vehicleMarginRate == null
      ? (row.marginRate == null ? "" : String(Number((row.marginRate * 100).toFixed(4))))
      : String(Number((row.vehicleMarginRate * 100).toFixed(4))),
    vehicleProfitEur: row.vehicleProfitEur == null ? "" : String(row.vehicleProfitEur),
    vehicleProfitRatePercent: row.vehicleProfitRate == null ? "" : String(Number((row.vehicleProfitRate * 100).toFixed(4))),
    fobDeltaEur: row.fobDeltaEur == null ? "" : String(row.fobDeltaEur),
    marginDeltaEur: row.marginDeltaEur == null ? "" : String(row.marginDeltaEur),
    memo: row.memo ?? "",
  };
}

function nullableNumber(value: string): number | null {
  const text = value.trim();
  if (!text) return null;
  if (text === "-") return null;
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
}

function displayDraftValue(value: string): string {
  return value.trim() ? value : "-";
}

function financeCellInputProps(
  value: string,
  saveState: MaterialFinanceSaveState,
  className = "",
) {
  return {
    className: ["material-finance-edit-field", `is-${saveState}`, className].filter(Boolean).join(" "),
    inputMode: "decimal" as const,
    type: "text",
    value: displayDraftValue(value),
    onFocus: (event: FocusEvent<HTMLInputElement>) => {
      if (event.currentTarget.value === "-") {
        event.currentTarget.select();
      }
    },
  };
}

function draftFingerprint(draft: MaterialFinanceDraft): string {
  return [
    draft.fobEur,
    draft.vehicleMarginEur,
    draft.vehicleMarginRatePercent,
    draft.vehicleProfitEur,
    draft.vehicleProfitRatePercent,
    draft.fobDeltaEur,
    draft.marginDeltaEur,
    draft.memo,
  ].join("\u001f");
}

function saveStateLabel(saveState: MaterialFinanceSaveState): string {
  if (saveState === "dirty") return "Unsaved";
  if (saveState === "saving") return "Saving...";
  if (saveState === "error") return "Save failed";
  return "Saved";
}

function findFinanceRow(container: HTMLElement, materialCode: string): HTMLTableRowElement | null {
  return Array.from(container.querySelectorAll<HTMLTableRowElement>(".material-finance-row"))
    .find((row) => row.dataset.materialCode === materialCode) ?? null;
}

function updateFromDraft(countryCode: string, draft: MaterialFinanceDraft): CountryMaterialFinanceUpdate {
  const vehicleMarginRatePercent = nullableNumber(draft.vehicleMarginRatePercent);
  const vehicleProfitRatePercent = nullableNumber(draft.vehicleProfitRatePercent);
  return {
    countryCode,
    fobEur: nullableNumber(draft.fobEur),
    vehicleMarginEur: nullableNumber(draft.vehicleMarginEur),
    vehicleMarginRate: vehicleMarginRatePercent == null ? null : vehicleMarginRatePercent / 100,
    vehicleProfitEur: nullableNumber(draft.vehicleProfitEur),
    vehicleProfitRate: vehicleProfitRatePercent == null ? null : vehicleProfitRatePercent / 100,
    fobDeltaEur: nullableNumber(draft.fobDeltaEur),
    marginDeltaEur: nullableNumber(draft.marginDeltaEur),
    memo: draft.memo.trim() || null,
    sourceMode: "manual",
  };
}

export function MaterialFinanceMatrix({
  rows,
  title,
  density = "standard",
  savingMaterialCode = null,
  onViewHistory,
  onSaveRow,
}: MaterialFinanceMatrixProps) {
  const [drafts, setDrafts] = useState<Record<string, MaterialFinanceDraft>>({});
  const [saveStates, setSaveStates] = useState<Record<string, MaterialFinanceSaveState>>({});
  const [expandedGroupKeys, setExpandedGroupKeys] = useState<Set<string>>(() => new Set());
  const tbodyRef = useRef<HTMLTableSectionElement | null>(null);
  const expandedGroupKeyRef = useRef<string | null>(null);
  const savingMaterialCodeRef = useRef<string | null>(null);
  const latestDraftsRef = useRef<Record<string, MaterialFinanceDraft>>({});
  const saveStatesRef = useRef<Record<string, MaterialFinanceSaveState>>({});
  const autosaveTimersRef = useRef<Record<string, number>>({});
  const saveTokensRef = useRef<Record<string, number>>({});
  const financeGroups = buildFinanceGroups(rows);

  useStaggerEntrance(tbodyRef, rows.length > 0, {
    selector: ".material-finance-row",
    staggerDelay: 42,
    duration: 460,
    translateY: 10,
  });

  useEffect(() => {
    const serverDrafts = Object.fromEntries(rows.map((row) => [row.materialCode, draftFromRow(row)]));
    setDrafts((current) => {
      const nextDrafts = { ...serverDrafts };
      for (const row of rows) {
        const state = saveStatesRef.current[row.materialCode];
        if ((state === "dirty" || state === "saving" || state === "error") && current[row.materialCode]) {
          nextDrafts[row.materialCode] = current[row.materialCode];
        }
      }
      latestDraftsRef.current = nextDrafts;
      return nextDrafts;
    });
    setSaveStates((current) => Object.fromEntries(
      rows.map((row) => {
        const state = current[row.materialCode];
        return [row.materialCode, state === "dirty" || state === "saving" || state === "error" ? state : "saved"];
      }),
    ));
  }, [rows]);

  useEffect(() => {
    saveStatesRef.current = saveStates;
  }, [saveStates]);

  useEffect(() => () => {
    Object.values(autosaveTimersRef.current).forEach((timerId) => window.clearTimeout(timerId));
  }, []);

  useEffect(() => {
    const container = tbodyRef.current;
    if (!container) return;

    if (savingMaterialCode) {
      savingMaterialCodeRef.current = savingMaterialCode;
      const row = findFinanceRow(container, savingMaterialCode);
      if (!row) return;
      try {
        animate(row, {
          translateX: [0, 3, 0],
          duration: 420,
          ease: "outQuad",
        });
      } catch {
        /* decorative only */
      }
      return;
    }

    const completedMaterialCode = savingMaterialCodeRef.current;
    savingMaterialCodeRef.current = null;
    if (!completedMaterialCode) return;

    const row = findFinanceRow(container, completedMaterialCode);
    if (!row) return;
    const cells = row.querySelectorAll<HTMLTableCellElement>("td");
    try {
      animate(cells, {
        backgroundColor: ["#dcfce7", "#ffffff"],
        duration: 760,
        ease: "outQuad",
      });
    } catch {
      /* decorative only */
    }
  }, [savingMaterialCode]);

  useEffect(() => {
    const groupKey = expandedGroupKeyRef.current;
    if (!groupKey || !expandedGroupKeys.has(groupKey)) return;
    expandedGroupKeyRef.current = null;

    const container = tbodyRef.current;
    if (!container) return;
    const groupRows = Array.from(container.querySelectorAll<HTMLTableRowElement>(".material-finance-row"))
      .filter((row) => row.dataset.financeGroupKey === groupKey);
    if (groupRows.length === 0) return;

    try {
      animate(groupRows, {
        opacity: [0, 1],
        translateY: [8, 0],
        delay: stagger(32),
        duration: 300,
        ease: "outQuad",
      });
    } catch {
      /* decorative only */
    }
  }, [expandedGroupKeys]);

  const saveDraft = async (row: CountryMaterialFinanceRow, draft: MaterialFinanceDraft) => {
    const materialCode = row.materialCode;
    if (autosaveTimersRef.current[materialCode] != null) {
      window.clearTimeout(autosaveTimersRef.current[materialCode]);
      delete autosaveTimersRef.current[materialCode];
    }
    const saveToken = (saveTokensRef.current[materialCode] ?? 0) + 1;
    saveTokensRef.current[materialCode] = saveToken;
    const savedFingerprint = draftFingerprint(draft);
    setSaveStates((current) => ({ ...current, [materialCode]: "saving" }));
    try {
      await onSaveRow(row, updateFromDraft(row.countryCode, draft));
      const latestDraft = latestDraftsRef.current[materialCode] ?? draft;
      if (saveTokensRef.current[materialCode] === saveToken && draftFingerprint(latestDraft) === savedFingerprint) {
        setSaveStates((current) => ({ ...current, [materialCode]: "saved" }));
      }
    } catch {
      if (saveTokensRef.current[materialCode] === saveToken) {
        setSaveStates((current) => ({ ...current, [materialCode]: "error" }));
      }
    }
  };

  const queueAutosave = (row: CountryMaterialFinanceRow, draft: MaterialFinanceDraft) => {
    const materialCode = row.materialCode;
    if (autosaveTimersRef.current[materialCode] != null) {
      window.clearTimeout(autosaveTimersRef.current[materialCode]);
    }
    setSaveStates((current) => ({ ...current, [materialCode]: "dirty" }));
    autosaveTimersRef.current[materialCode] = window.setTimeout(() => {
      delete autosaveTimersRef.current[materialCode];
      const latestDraft = latestDraftsRef.current[materialCode] ?? draft;
      void saveDraft(row, latestDraft);
    }, FINANCE_AUTOSAVE_DELAY_MS);
  };

  const updateDraft = (
    row: CountryMaterialFinanceRow,
    patch: Partial<MaterialFinanceDraft>,
  ) => {
    const existing = latestDraftsRef.current[row.materialCode] ?? drafts[row.materialCode] ?? draftFromRow(row);
    const nextDraft = { ...existing, ...patch };
    latestDraftsRef.current[row.materialCode] = nextDraft;
    setDrafts((current) => ({
      ...current,
      [row.materialCode]: nextDraft,
    }));
    queueAutosave(row, nextDraft);
  };

  const toggleGroup = (groupKey: string) => {
    setExpandedGroupKeys((current) => {
      const next = new Set(current);
      if (next.has(groupKey)) {
        next.delete(groupKey);
      } else {
        expandedGroupKeyRef.current = groupKey;
        next.add(groupKey);
      }
      return next;
    });
  };

  return (
    <section className={`material-finance-matrix material-finance-matrix-${density}`}>
      {title ? <h4 className="material-finance-matrix-title">{title}</h4> : null}
      <div className="material-finance-table-wrap">
        <table className="data-table material-finance-table">
          <colgroup>
            <col className="material-finance-col-bom" />
            <col className="material-finance-col-model" />
            <col className="material-finance-col-version" />
            <col className="material-finance-col-powertrain" />
            <col className="material-finance-col-bom-fob" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-money" />
            <col className="material-finance-col-memo" />
            <col className="material-finance-col-updated" />
            <col className="material-finance-col-action" />
          </colgroup>
          <thead>
            <tr>
              <th>BOM</th>
              <th>Model</th>
              <th>Version</th>
              <th>Powertrain</th>
              <th>BOM FOB</th>
              <th>FOB</th>
              <th>Unit Margin</th>
              <th>Margin %</th>
              <th>Unit Profit</th>
              <th>Profit %</th>
              <th>FOB Δ</th>
              <th>Margin Δ</th>
              <th>Note</th>
              <th>Updated</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody ref={tbodyRef}>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={15} className="material-finance-empty">No finance rows.</td>
              </tr>
            ) : financeGroups.map((group) => {
              const collapsed = !expandedGroupKeys.has(group.key);
              const tone = powertrainTone(group.powertrain);
              return (
                <Fragment key={group.key}>
                  <tr className={`material-finance-group-row material-finance-group-row-${tone}`}>
                    <td colSpan={15}>
                      <button
                        type="button"
                        className="material-finance-group-toggle"
                        aria-expanded={!collapsed}
                        onClick={() => toggleGroup(group.key)}
                      >
                        <span className="material-finance-group-caret">{collapsed ? "▸" : "▾"}</span>
                        <strong>{group.brand} {group.modelName}</strong>
                        <span className={`material-finance-group-powertrain material-finance-group-powertrain-${tone}`}>
                          {group.powertrain}
                        </span>
                        <span>{group.rows.length} BOM rows</span>
                      </button>
                    </td>
                  </tr>
                  {collapsed ? null : group.rows.map((row) => {
                    const draft = drafts[row.materialCode] ?? draftFromRow(row);
                    const localSaveState = saveStates[row.materialCode] ?? "saved";
                    const saving = savingMaterialCode === row.materialCode || localSaveState === "saving";
                    const rowSaveState: MaterialFinanceSaveState = saving ? "saving" : localSaveState;
                    const skuCount = getSourcePayloadNumber(row, "skuCount");
                    const colourCodes = getSourcePayloadStringList(row, "colourCodes");
                    return (
                      <tr
                        key={`${row.countryCode}-${row.materialCode}`}
                        data-finance-group-key={group.key}
                        data-material-code={row.materialCode}
                        className={`material-finance-row is-${rowSaveState}`}
                      >
                        <td>
                          <div className="material-finance-code">{row.materialCode}</div>
                          <div className="material-finance-subtle">
                            {skuCount == null ? row.bomTemplate || "-" : `${skuCount} colour SKUs`}
                            {colourCodes.length > 0 ? ` · ${colourCodes.join("/")}` : ""}
                          </div>
                        </td>
                        <td>
                          <div className="material-finance-product-main">{row.modelName}</div>
                          <div className="material-finance-subtle">{row.brand}</div>
                        </td>
                        <td>
                          <div className="material-finance-product-main">{row.version || "-"}</div>
                          <div className="material-finance-subtle">Template level</div>
                        </td>
                        <td>
                          <span className={`material-finance-powertrain-chip material-finance-powertrain-${powertrainTone(row.powertrain)}`}>
                            {row.powertrain || "-"}
                          </span>
                        </td>
                        <td className="material-finance-number">{formatMoney(row.bomFobEur)}</td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.fobEur, rowSaveState)}
                            aria-label={`${row.materialCode} FOB`}
                            onChange={(event) => updateDraft(row, { fobEur: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.vehicleMarginEur, rowSaveState)}
                            aria-label={`${row.materialCode} unit margin`}
                            onChange={(event) => updateDraft(row, { vehicleMarginEur: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.vehicleMarginRatePercent, rowSaveState)}
                            aria-label={`${row.materialCode} margin percent`}
                            onChange={(event) => updateDraft(row, { vehicleMarginRatePercent: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.vehicleProfitEur, rowSaveState)}
                            aria-label={`${row.materialCode} unit profit`}
                            onChange={(event) => updateDraft(row, { vehicleProfitEur: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.vehicleProfitRatePercent, rowSaveState)}
                            aria-label={`${row.materialCode} profit percent`}
                            onChange={(event) => updateDraft(row, { vehicleProfitRatePercent: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.fobDeltaEur, rowSaveState, "material-finance-signed-input")}
                            aria-label={`${row.materialCode} FOB delta`}
                            placeholder="+/-"
                            onChange={(event) => updateDraft(row, { fobDeltaEur: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <input
                            {...financeCellInputProps(draft.marginDeltaEur, rowSaveState, "material-finance-signed-input")}
                            aria-label={`${row.materialCode} margin delta`}
                            placeholder="+/-"
                            onChange={(event) => updateDraft(row, { marginDeltaEur: event.target.value === "-" ? "" : event.target.value })}
                          />
                        </td>
                        <td>
                          <textarea
                            className={`material-finance-edit-field is-${rowSaveState}`}
                            value={draft.memo}
                            onChange={(event) => updateDraft(row, { memo: event.target.value })}
                            rows={density === "compact" ? 2 : 3}
                          />
                        </td>
                        <td>
                          <span
                            className={`material-finance-source-badge material-finance-source-${sourceModeTone(row.sourceMode)}`}
                            title={sourceModeTitle(row)}
                          >
                            {formatSourceMode(row.sourceMode)}
                          </span>
                          <div>{row.updatedBy || "-"}</div>
                          <div className="material-finance-subtle">{formatDateTime(row.updatedAtUtc)}</div>
                        </td>
                        <td>
                          <div className="crud-row-actions review-row-actions material-finance-row-actions">
                            <LoadingActionButton
                              size="sm"
                              variant="primary"
                              loading={saving}
                              loadingLabel="Saving..."
                              onClick={() => void saveDraft(row, latestDraftsRef.current[row.materialCode] ?? draft)}
                            >
                              Save now
                            </LoadingActionButton>
                            <span className={`material-finance-save-state material-finance-save-state-${rowSaveState}`}>
                              {saveStateLabel(rowSaveState)}
                            </span>
                            {onViewHistory ? (
                              <button
                                type="button"
                                className="btn btn-sm btn-ghost material-finance-history-button"
                                onClick={() => void onViewHistory(row)}
                              >
                                History
                              </button>
                            ) : null}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </Fragment>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
