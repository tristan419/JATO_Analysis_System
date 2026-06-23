import { useMemo } from "react";

import type {
  CountryMaterialFinanceImportPreview,
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
} from "../../types/orderGenius";
import { formatCountryCodeTooltip } from "../../utils/jatoCountries";
import { CountryCbuPastePanel } from "./CountryCbuPastePanel";
import { MaterialFinanceMatrix } from "./MaterialFinanceMatrix";

interface MaterialFinanceWorkbenchProps {
  countryCode: string;
  countryCodes: string[];
  rows: CountryMaterialFinanceRow[];
  loading: boolean;
  error?: string;
  savingMaterialCode?: string | null;
  onCountryChange: (countryCode: string) => void | Promise<void>;
  onPreviewImport?: (
    countryCode: string,
    payload: { file?: File; text?: string },
  ) => Promise<CountryMaterialFinanceImportPreview>;
  onViewHistory?: (row: CountryMaterialFinanceRow) => void | Promise<void>;
  onSaveRow: (row: CountryMaterialFinanceRow, update: CountryMaterialFinanceUpdate) => void | Promise<void>;
}

function hasFinanceValue(row: CountryMaterialFinanceRow): boolean {
  return [
    row.fobEur,
    row.vehicleMarginEur,
    row.vehicleMarginRate,
    row.vehicleProfitEur,
    row.vehicleProfitRate,
    row.fobDeltaEur,
    row.marginDeltaEur,
  ].some((value) => value != null) || Boolean(row.memo?.trim());
}

export function MaterialFinanceWorkbench({
  countryCode,
  countryCodes,
  rows,
  loading,
  error = "",
  savingMaterialCode = null,
  onCountryChange,
  onPreviewImport,
  onViewHistory,
  onSaveRow,
}: MaterialFinanceWorkbenchProps) {
  const isNlPriceCbu = countryCode === "NL";
  const filledRows = useMemo(
    () => rows.filter(hasFinanceValue).length,
    [rows],
  );
  const tickerItem = useMemo(
    () => {
      const row = rows.find(hasFinanceValue) ?? rows[0];
      return row ? `${row.materialCode} · ${row.modelName}` : "No CBU rows";
    },
    [rows],
  );

  return (
    <section className="material-finance-workbench">
      <header className="material-finance-workbench-head">
        <div className="review-table-toolbar-status material-finance-workbench-reel">
          <div className="review-toolbar-kpi-strip review-toolbar-kpi-strip--centered">
            <span className="review-toolbar-kpi-label">Filled / Rows</span>
            <strong className="review-toolbar-kpi-value">
              <span>{filledRows}</span>
              <span className="review-toolbar-kpi-divider">/</span>
              <span>{rows.length}</span>
            </strong>
            <span className="review-toolbar-kpi-meta">{countryCode}</span>
          </div>
          <div className="rolling-ticker-card is-reel-only">
            <div className="rolling-ticker-window" aria-label="CBU BOMs">
              <div className="rolling-ticker-track">
                <span className="rolling-ticker-item">{tickerItem}</span>
              </div>
            </div>
          </div>
        </div>
        <div className="material-finance-country-strip" aria-label="CBU country">
          {countryCodes.map((code) => (
            <button
              key={code}
              type="button"
              data-country-code={code}
              className={`material-finance-country-chip${code === countryCode ? " is-active" : ""}`}
              aria-label={formatCountryCodeTooltip(code)}
              title={formatCountryCodeTooltip(code)}
              onClick={() => {
                if (code !== countryCode) void onCountryChange(code);
              }}
            >
              {code}
            </button>
          ))}
        </div>
      </header>
      {loading ? (
        <div className="material-finance-empty">Loading finance rows...</div>
      ) : !isNlPriceCbu ? (
        <CountryCbuPastePanel
          countryCode={countryCode}
          rows={rows}
          savingMaterialCode={savingMaterialCode}
          onPreviewImport={onPreviewImport}
          onSaveRow={onSaveRow}
        />
      ) : (
        <MaterialFinanceMatrix
          rows={rows}
          title="NL CBU / margin matrix"
          savingMaterialCode={savingMaterialCode}
          onViewHistory={onViewHistory}
          onSaveRow={onSaveRow}
        />
      )}
      {error ? <div className="material-finance-error">{error}</div> : null}
    </section>
  );
}
