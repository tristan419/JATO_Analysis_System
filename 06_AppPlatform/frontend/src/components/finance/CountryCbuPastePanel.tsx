import { useMemo, useState } from "react";

import { UploadDigestPanel } from "../UploadDigestPanel";
import type {
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
} from "../../types/orderGenius";

interface CountryCbuPastePanelProps {
  countryCode: string;
  rows: CountryMaterialFinanceRow[];
  savingMaterialCode?: string | null;
  onSaveRow: (row: CountryMaterialFinanceRow, update: CountryMaterialFinanceUpdate) => void | Promise<void>;
}

type CountryCbuPasteField =
  | "materialCode"
  | "fobEur"
  | "retailPriceEur"
  | "wholesalePriceEur"
  | "dealerPriceEur"
  | "costEur"
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
  note: "memo",
  memo: "memo",
  remark: "memo",
};

function normalizeHeader(value: string): string {
  return value.trim().toLowerCase().replace(/[^a-z0-9]/g, "");
}

function splitCells(line: string): string[] {
  return line.includes("\t") ? line.split("\t") : line.split(",");
}

function cleanMaterialCode(value: string): string {
  return value.trim().toUpperCase();
}

function parseOptionalNumber(value: string): number | undefined {
  const text = value.trim();
  if (!text) return undefined;
  const negative = text.startsWith("(") && text.endsWith(")");
  const normalized = text.replace(/[(),\s]/g, "");
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) return undefined;
  return negative ? -parsed : parsed;
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
    const parsed = parseOptionalNumber(cell);
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
  const mappedHeaders = firstCells.map((cell) => HEADER_ALIASES[normalizeHeader(cell)] ?? null);
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

function formatMoney(value: number | null): string {
  return value == null ? "-" : value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export function CountryCbuPastePanel({
  countryCode,
  rows,
  savingMaterialCode = null,
  onSaveRow,
}: CountryCbuPastePanelProps) {
  const [pasteText, setPasteText] = useState("");
  const [applyMessage, setApplyMessage] = useState("");
  const [applying, setApplying] = useState(false);

  const parsedRows = useMemo(
    () => parsePasteRows(pasteText, countryCode, rows),
    [countryCode, pasteText, rows],
  );
  const validRows = parsedRows.filter((row) => !row.error && row.row && row.update);
  const errors = parsedRows.filter((row) => row.error).map((row) => `Line ${row.lineNumber}: ${row.error}`);

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
        title={`${countryCode} CBU paste`}
        subtitle="Paste template-level CBU rows from Excel."
        metrics={[
          { label: "Scope rows", value: rows.length },
          { label: "Parsed", value: parsedRows.length },
          { label: "Ready", value: validRows.length, tone: validRows.length > 0 ? "success" : "neutral" },
          { label: "Errors", value: errors.length, tone: errors.length > 0 ? "danger" : "neutral" },
        ]}
        errors={errors}
        footer={
          <div className="country-cbu-paste-actions">
            <button type="button" className="btn btn-sm btn-ghost" onClick={() => setPasteText("")}>
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
        <textarea
          className="country-cbu-paste-input"
          value={pasteText}
          placeholder="Material Code\tFOB\tRetail\tWholesale\tDealer\tCost\tNote"
          onChange={(event) => {
            setPasteText(event.target.value);
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
