import type { VehicleImportParsedRowsPayload, VehicleImportPreview } from "../../types/orderGeniusVehicle";

export type VehicleImportSourceKind = "spreadsheet" | "parsedRows" | "image" | "unsupported";

const SPREADSHEET_EXTENSIONS = [".xlsx", ".xls", ".xlsm"];
const PARSED_ROW_EXTENSIONS = [".json"];
const IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif"];

type ParsedRowsFile = Pick<File, "name" | "text">;

function filenameHasExtension(filename: string, extensions: string[]): boolean {
  const normalized = filename.trim().toLowerCase();
  return extensions.some((extension) => normalized.endsWith(extension));
}

export function detectVehicleImportSource(file: Pick<File, "name" | "type">): VehicleImportSourceKind {
  const mimeType = file.type.toLowerCase();
  if (
    mimeType.includes("spreadsheet")
    || mimeType.includes("excel")
    || filenameHasExtension(file.name, SPREADSHEET_EXTENSIONS)
  ) {
    return "spreadsheet";
  }
  if (mimeType.includes("json") || filenameHasExtension(file.name, PARSED_ROW_EXTENSIONS)) {
    return "parsedRows";
  }
  if (mimeType.startsWith("image/") || filenameHasExtension(file.name, IMAGE_EXTENSIONS)) {
    return "image";
  }
  return "unsupported";
}

export async function parseVehicleImportRowsPayload(file: ParsedRowsFile): Promise<VehicleImportParsedRowsPayload> {
  const text = await file.text();
  const parsed = JSON.parse(text) as unknown;
  const rawRows = Array.isArray(parsed)
    ? parsed
    : typeof parsed === "object" && parsed !== null && "rows" in parsed
      ? (parsed as { rows?: unknown }).rows
      : null;

  if (!Array.isArray(rawRows)) {
    throw new Error("Parsed image result must be a JSON array or an object with rows.");
  }
  const rows = rawRows.map((row, index) => {
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new Error(`Parsed row ${index + 1} must be an object.`);
    }
    return row as Record<string, unknown>;
  });

  return {
    rows,
    source: file.name || "parsed-image-rows",
  };
}

export function buildUnsupportedVehicleImportPreview(
  filename: string,
  sourceKind: Exclude<VehicleImportSourceKind, "spreadsheet" | "parsedRows">,
): VehicleImportPreview {
  const sourceLabel = sourceKind === "image" ? "Image" : "File";
  const guidance = sourceKind === "image"
    ? "Image VIN parsing is not configured yet. Convert the image to vehicle rows first; the parsed rows will use this same digest and apply workflow."
    : "Unsupported file type. Use an Excel workbook or a parsed image result for vehicle allocation import.";

  return {
    importId: "",
    totalRows: 0,
    newHeaders: 0,
    newLines: 0,
    newUnits: 0,
    updatedUnits: 0,
    warnings: [],
    errors: [`${sourceLabel} import not ready for ${filename || "this file"}. ${guidance}`],
    previewRows: [],
    status: "error",
  };
}
