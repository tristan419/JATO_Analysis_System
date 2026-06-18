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
  scopeLabel: string;
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

export function MaterialFinanceWorkbench({
  countryCode,
  countryCodes,
  scopeLabel,
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

  return (
    <section className="material-finance-workbench">
      <header className="material-finance-workbench-head">
        <div>
          <span className="material-finance-workbench-eyebrow">
            {isNlPriceCbu ? "NL PRICE CBU" : `${countryCode} CBU`}
          </span>
          <h3>{scopeLabel}</h3>
        </div>
        <div className="material-finance-country-strip" aria-label="CBU country">
          {countryCodes.map((code) => (
            <button
              key={code}
              type="button"
              className={`material-finance-country-chip${code === countryCode ? " is-active" : ""}`}
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
