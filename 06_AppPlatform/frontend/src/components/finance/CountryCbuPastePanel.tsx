import { useMemo, useState } from "react";

import { UploadDigestPanel } from "../UploadDigestPanel";
import type {
  CountryMaterialFinanceImportPreview,
  CountryMaterialFinanceImportRow,
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
} from "../../types/orderGenius";

interface CountryCbuPastePanelProps {
  countryCode: string;
  rows: CountryMaterialFinanceRow[];
  savingMaterialCode?: string | null;
  onPreviewImport?: (
    countryCode: string,
    payload: { file?: File; text?: string },
  ) => Promise<CountryMaterialFinanceImportPreview>;
  onSaveRow: (row: CountryMaterialFinanceRow, update: CountryMaterialFinanceUpdate) => void | Promise<void>;
}

type CountryCbuPasteField =
  | "materialCode"
  | "fobEur"
  | "retailPriceEur"
  | "wholesalePriceEur"
  | "dealerPriceEur"
  | "costEur"
  | "vehicleMarginEur"
  | "vehicleMarginRate"
  | "vehicleProfitEur"
  | "vehicleProfitRate"
  | "fobDeltaEur"
  | "marginDeltaEur"
  | "memo";

interface ParsedCountryCbuRow {
  lineNumber: number;
  materialCode: string;
  row: CountryMaterialFinanceRow | null;
  update: CountryMaterialFinanceUpdate | null;
  error: string;
}

const DEFAULT_FIELDS: CountryCbuPasteField[] = [
  "materialCode",
  "fobEur",
  "retailPriceEur",
  "wholesalePriceEur",
  "dealerPriceEur",
  "costEur",
  "memo",
];

const HEADER_ALIASES: Record<string, CountryCbuPasteField> = {
  material: "materialCode",
  materialcode: "materialCode",
  bom: "materialCode",
  bomtemplate: "materialCode",
  template: "materialCode",
  fob: "fobEur",
  fobeur: "fobEur",
  retail: "retailPriceEur",
  retailprice: "retailPriceEur",
  retailpriceeur: "retailPriceEur",
  wholesale: "wholesalePriceEur",
  wholesaleprice: "wholesalePriceEur",
  wholesalepriceeur: "wholesalePriceEur",
  dealer: "dealerPriceEur",
  dealerprice: "dealerPriceEur",
  dealerpriceeur: "dealerPriceEur",
  cost: "costEur",
  costeur: "costEur",
  cbu: "costEur",
  unitmargin: "vehicleMarginEur",
  vehiclemargin: "vehicleMarginEur",
  vehiclemargineur: "vehicleMarginEur",
  margin: "vehicleMarginEur",
  marginrate: "vehicleMarginRate",
  marginpercent: "vehicleMarginRate",
  marginpct: "vehicleMarginRate",
  unitprofit: "vehicleProfitEur",
  vehicleprofit: "vehicleProfitEur",
  vehicleprofiteur: "vehicleProfitEur",
  profit: "vehicleProfitEur",
  profitrate: "vehicleProfitRate",
  profitpercent: "vehicleProfitRate",
  profitpct: "vehicleProfitRate",
  fobdelta: "fobDeltaEur",
  fobadjust: "fobDeltaEur",
  fobadjustment: "fobDeltaEur",
  margindelta: "marginDeltaEur",
  marginadjust: "marginDeltaEur",
  marginadjustment: "marginDeltaEur",
  note: "memo",
  memo: "memo",
  remark: "memo",
};

const RATE_FIELDS = new Set<CountryCbuPasteField>(["vehicleMarginRate", "vehicleProfitRate"]);

function normalizeHeader(value: string): string {
  return value.trim().toLowerCase().replace("%", "percent").replace("Δ", "delta").replace("△", "delta").replace(/[^a-z0-9]/g, "");
}

function fieldFromHeader(value: string): CountryCbuPasteField | null {
  const header = value.trim().toLowerCase();
  if (!header) return null;
  if (header.includes("物料") || header.includes("料号")) return "materialCode";
  if (header.includes("边际") && (header.includes("增") || header.includes("调") || header.includes("差"))) return "marginDeltaEur";
  if (header.includes("fob") && (header.includes("增") || header.includes("调") || header.includes("差"))) return "fobDeltaEur";
  if (header.includes("单车边际") || header.includes("车辆边际")) return "vehicleMarginEur";
  if (header.includes("边际率")) return "vehicleMarginRate";
  if (header.includes("单车利润") || header.includes("车辆利润")) return "vehicleProfitEur";
  if (header.includes("利润率")) return "vehicleProfitRate";
  if (header.includes("成本")) return "costEur";
  if (header.includes("零售") || header.includes("建议售价")) return "retailPriceEur";
  if (header.includes("批发")) return "wholesalePriceEur";
  if (header.includes("经销")) return "dealerPriceEur";
  if (header.includes("备注") || header.includes("说明")) return "memo";
  return HEADER_ALIASES[normalizeHeader(value)] ?? null;
}

function splitCells(line: string): string[] {
  return line.includes("\t") ? line.split("\t") : line.split(",");
}

function cleanMaterialCode(value: string): string {
  return value.trim().toUpperCase();
}

function parseOptionalNumber(value: string, rate: boolean): number | undefined {
  const text = value.trim();
  if (!text) return undefined;
  const negative = text.startsWith("(") && text.endsWith(")");
  const normalized = text.replace(/eur/ig, "").replace(/[€%,()\s]/g, "");
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) return undefined;
  const signed = negative ? -parsed : parsed;
  return rate && Math.abs(signed) > 1 ? Math.round((signed / 100) * 1_000_000) / 1_000_000 : signed;
}

function buildUpdate(
  countryCode: string,
  cells: string[],
  fields: (CountryCbuPasteField | null)[],
): CountryMaterialFinanceUpdate | null {
  const update: CountryMaterialFinanceUpdate = {
    countryCode,
    sourceMode: "uploaded",
    sourcePayload: { entryMode: "excel_paste" },
  };
  let changed = false;
  fields.forEach((field, index) => {
    if (!field || field === "materialCode") return;
    const cell = cells[index] ?? "";
    if (field === "memo") {
      if (cell.trim()) {
        update.memo = cell.trim();
        changed = true;
      }
      return;
    }
    const parsed = parseOptionalNumber(cell, RATE_FIELDS.has(field));
    if (parsed !== undefined) {
      update[field] = parsed;
      changed = true;
    }
  });
  return changed ? update : null;
}

function parsePasteRows(
  pasteText: string,
  countryCode: string,
  rows: CountryMaterialFinanceRow[],
): ParsedCountryCbuRow[] {
  const rowByMaterial = new Map(rows.map((row) => [row.materialCode.toUpperCase(), row]));
  const lines = pasteText.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0) return [];

  const firstCells = splitCells(lines[0]);
  const mappedHeaders = firstCells.map(fieldFromHeader);
  const hasHeader = mappedHeaders.includes("materialCode");
  const fields = hasHeader ? mappedHeaders : DEFAULT_FIELDS;
  const dataLines = hasHeader ? lines.slice(1) : lines;
  const firstDataLineNumber = hasHeader ? 2 : 1;

  return dataLines.map((line, index) => {
    const cells = splitCells(line);
    const materialIndex = fields.findIndex((field) => field === "materialCode");
    const materialCode = cleanMaterialCode(cells[materialIndex] ?? "");
    const row = rowByMaterial.get(materialCode) ?? null;
    const update = buildUpdate(countryCode, cells, fields);
    let error = "";
    if (!materialCode) error = "Missing material code";
    else if (!materialCode.includes("**")) error = `${materialCode} is not a BOM template`;
    else if (!row) error = `${materialCode} is outside current scope`;
    else if (!update) error = `${materialCode} has no changed values`;
    return {
      lineNumber: firstDataLineNumber + index,
      materialCode,
      row,
      update,
      error,
    };
  });
}

function hydrateImportRows(
  previewRows: CountryMaterialFinanceImportRow[],
  rows: CountryMaterialFinanceRow[],
): ParsedCountryCbuRow[] {
  const rowByMaterial = new Map(rows.map((row) => [row.materialCode.toUpperCase(), row]));
  return previewRows.map((previewRow) => {
    const materialCode = cleanMaterialCode(previewRow.materialCode);
    const row = rowByMaterial.get(materialCode) ?? null;
    let error = previewRow.error;
    if (!materialCode) error = "Missing material code";
    else if (!materialCode.includes("**")) error = `${materialCode} is not a BOM template`;
    else if (!row) error = `${materialCode} is outside current scope`;
    else if (!previewRow.update) error = error || `${materialCode} has no finance values`;
    return {
      lineNumber: previewRow.lineNumber,
      materialCode,
      row,
      update: previewRow.update,
      error,
    };
  });
}

function formatMoney(value: number | null): string {
  return value == null ? "-" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function CountryCbuPastePanel({
  countryCode,
  rows,
  savingMaterialCode = null,
  onPreviewImport,
  onSaveRow,
}: CountryCbuPastePanelProps) {
  const [pasteText, setPasteText] = useState("");
  const [previewRows, setPreviewRows] = useState<CountryMaterialFinanceImportRow[] | null>(null);
  const [previewWarnings, setPreviewWarnings] = useState<string[]>([]);
  const [previewFileName, setPreviewFileName] = useState("");
  const [previewing, setPreviewing] = useState(false);
  const [applyMessage, setApplyMessage] = useState("");
  const [applying, setApplying] = useState(false);

  const parsedRows = useMemo(
    () => previewRows
      ? hydrateImportRows(previewRows, rows)
      : parsePasteRows(pasteText, countryCode, rows),
    [countryCode, pasteText, previewRows, rows],
  );
  const validRows = parsedRows.filter((row) => !row.error && row.row && row.update);
  const errors = parsedRows.filter((row) => row.error).map((row) => `Line ${row.lineNumber}: ${row.error}`);

  const previewFile = async (file: File) => {
    if (!onPreviewImport) return;
    setPreviewing(true);
    setApplyMessage("");
    try {
      const preview = await onPreviewImport(countryCode, { file });
      setPreviewRows(preview.rows);
      setPreviewWarnings(preview.warnings);
      setPreviewFileName(file.name);
      setPasteText("");
    } catch (error) {
      setPreviewRows([]);
      setPreviewWarnings([error instanceof Error ? error.message : String(error)]);
      setPreviewFileName(file.name);
    } finally {
      setPreviewing(false);
    }
  };

  const applyRows = async () => {
    setApplying(true);
    setApplyMessage("");
    try {
      for (const parsedRow of validRows) {
        if (parsedRow.row && parsedRow.update) {
          await onSaveRow(parsedRow.row, parsedRow.update);
        }
      }
      setApplyMessage(`${validRows.length} template rows saved.`);
    } finally {
      setApplying(false);
    }
  };

  return (
    <section className="country-cbu-paste-panel">
      <UploadDigestPanel
        title={`${countryCode} CBU digest`}
        subtitle={previewFileName ? `Preview file: ${previewFileName}` : "Paste template-level CBU rows from Excel, or preview xlsx/image files."}
        metrics={[
          { label: "Scope rows", value: rows.length },
          { label: "Parsed", value: parsedRows.length },
          { label: "Ready", value: validRows.length, tone: validRows.length > 0 ? "success" : "neutral" },
          { label: "Errors", value: errors.length, tone: errors.length > 0 ? "danger" : "neutral" },
        ]}
        warnings={previewWarnings}
        errors={errors}
        footer={
          <div className="country-cbu-paste-actions">
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              onClick={() => {
                setPasteText("");
                setPreviewRows(null);
                setPreviewWarnings([]);
                setPreviewFileName("");
              }}
            >
              Clear
            </button>
            <button
              type="button"
              className="btn btn-sm btn-primary"
              disabled={applying || validRows.length === 0}
              onClick={() => void applyRows()}
            >
              {applying ? "Saving..." : `Apply ${validRows.length}`}
            </button>
            {applyMessage ? <span>{applyMessage}</span> : null}
          </div>
        }
      >
        {onPreviewImport ? (
          <div className="country-cbu-import-row">
            <label className="country-cbu-file-control">
              <span>File / image preview</span>
              <input
                type="file"
                accept=".xlsx,.xlsm,.csv,.tsv,.txt,image/*"
                disabled={previewing}
                onChange={(event) => {
                  const file = event.target.files?.[0];
                  event.currentTarget.value = "";
                  if (file) void previewFile(file);
                }}
              />
            </label>
            {previewing ? <span>Parsing...</span> : null}
          </div>
        ) : null}
        <textarea
          className="country-cbu-paste-input"
          value={pasteText}
          placeholder="Material Code\tFOB\tUnit Margin\tMargin %\tUnit Profit\tProfit %\tFOB Delta\tMargin Delta\tNote"
          onChange={(event) => {
            setPasteText(event.target.value);
            setPreviewRows(null);
            setPreviewWarnings([]);
            setPreviewFileName("");
            setApplyMessage("");
          }}
        />
        <div className="country-cbu-template-list">
          {rows.length === 0 ? (
            <div className="material-finance-empty">No BOM-template rows for this country and scope.</div>
          ) : rows.map((row) => (
            <div key={`${row.countryCode}-${row.materialCode}`} className="country-cbu-template-row">
              <div>
                <strong>{row.materialCode}</strong>
                <span>{row.modelName} · {row.version} · {row.powertrain || "-"}</span>
              </div>
              <div>
                <span>BOM FOB</span>
                <strong>{formatMoney(row.bomFobEur)}</strong>
              </div>
              <div>
                <span>CBU FOB</span>
                <strong>{formatMoney(row.fobEur)}</strong>
              </div>
              <div>
                <span>Cost</span>
                <strong>{formatMoney(row.costEur)}</strong>
              </div>
              <div>
                <span>Status</span>
                <strong>{savingMaterialCode === row.materialCode ? "Saving" : row.sourceMode || "Base"}</strong>
              </div>
            </div>
          ))}
        </div>
      </UploadDigestPanel>
    </section>
  );
}
