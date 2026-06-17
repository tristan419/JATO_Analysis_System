import { useEffect, useState } from "react";

import type {
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
} from "../../types/orderGenius";

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

interface MaterialFinanceMatrixProps {
  rows: CountryMaterialFinanceRow[];
  title?: string;
  density?: "compact" | "standard";
  savingMaterialCode?: string | null;
  onSaveRow: (row: CountryMaterialFinanceRow, update: CountryMaterialFinanceUpdate) => void | Promise<void>;
}

function formatMoney(value: number | null): string {
  return value == null ? "-" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatPercent(value: number | null): string {
  return value == null ? "-" : `${(value * 100).toFixed(2)}%`;
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
  const parsed = Number(text);
  return Number.isFinite(parsed) ? parsed : null;
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
  onSaveRow,
}: MaterialFinanceMatrixProps) {
  const [drafts, setDrafts] = useState<Record<string, MaterialFinanceDraft>>({});

  useEffect(() => {
    setDrafts(Object.fromEntries(rows.map((row) => [row.materialCode, draftFromRow(row)])));
  }, [rows]);

  const updateDraft = (
    materialCode: string,
    patch: Partial<MaterialFinanceDraft>,
  ) => {
    setDrafts((current) => {
      const existing = current[materialCode];
      if (!existing) return current;
      return {
        ...current,
        [materialCode]: { ...existing, ...patch },
      };
    });
  };

  return (
    <section className={`material-finance-matrix material-finance-matrix-${density}`}>
      {title ? <h4 className="material-finance-matrix-title">{title}</h4> : null}
      <div className="material-finance-table-wrap">
        <table className="data-table material-finance-table">
          <colgroup>
            <col className="material-finance-col-bom" />
            <col className="material-finance-col-product" />
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
              <th>Product</th>
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
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={13} className="material-finance-empty">No finance rows.</td>
              </tr>
            ) : rows.map((row) => {
              const draft = drafts[row.materialCode] ?? draftFromRow(row);
              const saving = savingMaterialCode === row.materialCode;
              const skuCount = getSourcePayloadNumber(row, "skuCount");
              const colourCodes = getSourcePayloadStringList(row, "colourCodes");
              return (
                <tr key={`${row.countryCode}-${row.materialCode}`}>
                  <td>
                    <div className="material-finance-code">{row.materialCode}</div>
                    <div className="material-finance-subtle">
                      {skuCount == null ? row.bomTemplate || "-" : `${skuCount} colour SKUs`}
                    </div>
                  </td>
                  <td>
                    <div>{row.modelName}</div>
                    <div className="material-finance-subtle">
                      {row.version} · {row.powertrain || "-"}
                      {colourCodes.length > 0 ? ` · ${colourCodes.join("/")}` : ""}
                    </div>
                  </td>
                  <td className="material-finance-number">{formatMoney(row.bomFobEur)}</td>
                  <td><input type="number" value={draft.fobEur} onChange={(event) => updateDraft(row.materialCode, { fobEur: event.target.value })} /></td>
                  <td>
                    <input type="number" value={draft.vehicleMarginEur} onChange={(event) => updateDraft(row.materialCode, { vehicleMarginEur: event.target.value })} />
                    <div className="material-finance-subtle">Current {formatMoney(row.vehicleMarginEur ?? row.marginEur)}</div>
                  </td>
                  <td>
                    <input type="number" value={draft.vehicleMarginRatePercent} onChange={(event) => updateDraft(row.materialCode, { vehicleMarginRatePercent: event.target.value })} />
                    <div className="material-finance-subtle">Current {formatPercent(row.vehicleMarginRate ?? row.marginRate)}</div>
                  </td>
                  <td>
                    <input type="number" value={draft.vehicleProfitEur} onChange={(event) => updateDraft(row.materialCode, { vehicleProfitEur: event.target.value })} />
                    <div className="material-finance-subtle">Current {formatMoney(row.vehicleProfitEur)}</div>
                  </td>
                  <td>
                    <input type="number" value={draft.vehicleProfitRatePercent} onChange={(event) => updateDraft(row.materialCode, { vehicleProfitRatePercent: event.target.value })} />
                    <div className="material-finance-subtle">Current {formatPercent(row.vehicleProfitRate)}</div>
                  </td>
                  <td>
                    <input className="material-finance-signed-input" type="number" placeholder="+/- EUR" value={draft.fobDeltaEur} onChange={(event) => updateDraft(row.materialCode, { fobDeltaEur: event.target.value })} />
                    <div className="material-finance-subtle">Current {formatMoney(row.fobDeltaEur)}</div>
                  </td>
                  <td>
                    <input className="material-finance-signed-input" type="number" placeholder="+/- EUR" value={draft.marginDeltaEur} onChange={(event) => updateDraft(row.materialCode, { marginDeltaEur: event.target.value })} />
                    <div className="material-finance-subtle">Current {formatMoney(row.marginDeltaEur)}</div>
                  </td>
                  <td>
                    <textarea
                      value={draft.memo}
                      onChange={(event) => updateDraft(row.materialCode, { memo: event.target.value })}
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
                    <button
                      type="button"
                      className="btn btn-sm btn-primary"
                      disabled={saving}
                      onClick={() => void onSaveRow(row, updateFromDraft(row.countryCode, draft))}
                    >
                      {saving ? "Saving..." : "Save"}
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
