import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type DragEvent,
  type FormEvent,
  type KeyboardEvent,
  type CSSProperties,
} from "react";
import { animate } from "animejs";
import { createPortal } from "react-dom";
import { BomTemplateLifecycleEditor } from "../components/orderGenius/BomTemplateLifecycleEditor";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { PiInvoiceExportDialog } from "../components/PiInvoiceExportDialog";

import { api, apiUrl, AUTH_FAILURE_EVENT } from "../api/client";
import { useAuth } from "../contexts/AuthContext";
import { OrderingBrandNotice } from "../components/RoleUpgradeModal";
import { isAdminRole } from "../utils/pageNavigation";
import { useAccountCountryOptions } from "../hooks/useAccountCountryOptions";
import { useResolvedCountry } from "../hooks/useResolvedCountry";
import { formatCountryCodeTooltip } from "../utils/jatoCountries";
import { compareProductModels } from "../utils/orderGeniusProductSort";
import {
  buildBomEditScopeKey,
  resolveBomAdminColourTier,
  type BomAdminColourTier,
} from "../utils/orderGeniusBomAdmin";
import {
  buildOrderGeniusColourSwatch,
  MISSING_COLOUR_SWATCH_HEX,
  normalizeColourHex,
  parseOrderGeniusColourSwatch,
} from "../utils/orderGeniusColourSwatch";
import { resolveCommonParentMaterialRemark } from "../utils/orderGeniusRemarks";
import { getCachedPageValue, setCachedPageValue } from "../utils/pageCache";
import type { CellValueChangedEvent } from "ag-grid-community";
import {
  getOrderGeniusRowId,
  OrderGeniusGrid,
  type OrderGeniusGridRow,
} from "../components/OrderGeniusGrid";
import { CommandSelect } from "../components/CommandSelect";
import { DeckFloatingDrawer, FlipToolCard } from "../components/deckControls";
import { MaterialFinanceMatrix, MaterialFinanceWorkbench } from "../components/finance";
import {
  BomEditPanel,
  BomFobRepriceAuditCard,
  type BomFobAuditTemplateTarget,
  type BomEditCountryOption,
} from "../components/orderGenius";
import type {
  ColourHexRuleApplyResult,
  ColourHexRuleLookup,
  ColourHexRulePreview,
  ColourHexRuleStatus,
  ColourHexRule,
  ColourHexRuleSummary,
  ColourSurchargeRule,
  SpecialColourSurchargeRule,
  ColourTierRepriceReport,
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
  CountryPaymentTerm,
  CountryTemplateFobPeriod,
  FobPeriodDeletionPreview,
  MaterialSkuMatrixRow,
  MaterialUploadPreview,
  MatrixResponse,
  FobConflict,
  MonthCell,
  OrderGeniusOptions,
  PublishBaselineResponse,
  QuantityCellUpdate,
  QuantityImportPreview,
  QuantityImportResult,
} from "../types/orderGenius";
import type {
  PiOrderHeader,
  VehicleAllocationPlan,
  VehicleAllocationPlanLine,
} from "../types/orderGeniusVehicle";

const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB
const MONTHS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const BOM_ADMIN_SURCHARGE_BRANDS = ["OMODA", "JAECOO"] as const;
const BOM_ADMIN_SURCHARGE_TYPES = [
  { value: "dual", label: "Dual" },
  { value: "special", label: "Special" },
] as const;
const POWERTRAIN_COMMAND_OPTIONS = ["BEV", "HEV", "PHEV", "ICE", "MHEV", "REEV", "Other"].map((value) => ({
  value,
  label: value,
  keywords: [value.toLowerCase()],
}));
const BOM_ADMIN_TOOLS_COMPACT_BREAKPOINT = 680;
const BOM_ADMIN_TOOLS_PHONE_BREAKPOINT = 520;
const DEFAULT_COLOUR_SURCHARGES: Record<string, number> = {
  "OMODA|dual": 200,
  "OMODA|special": 200,
  "JAECOO|dual": 300,
  "JAECOO|special": 300,
};
type SpecialColourSurchargeDraft = {
  brand: string;
  modelName: string;
  colourCode: string;
  colourTier: "dual" | "special";
  colourName: string;
  surchargeEur: string;
};

const EMPTY_SPECIAL_COLOUR_SURCHARGE_DRAFT: SpecialColourSurchargeDraft = {
  brand: "OMODA",
  modelName: "",
  colourCode: "",
  colourTier: "special",
  colourName: "",
  surchargeEur: "0",
};
const BOM_ADMIN_FIXED_COLUMN_COUNT = 9;
const BOM_ADMIN_COUNTRY_COLUMN_WIDTH = 75;
type BomAdminSkuColourFields = {
  colour?: string | null;
  colourCode?: string | null;
  colourType?: string | null;
  colourTier?: string | null;
  colourHex?: string | null;
  editionTag?: string | null;
};
const BOM_ADMIN_STICKY_COLUMN_WIDTHS = {
  bom: 150,
  interior: 90,
  single: 120,
  dual: 100,
  special: 80,
} as const;
type BomAdminStickyColumn = keyof typeof BOM_ADMIN_STICKY_COLUMN_WIDTHS;
const BOM_ADMIN_STICKY_COLUMN_LEFTS = {
  bom: 0,
  interior: BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom,
  single: BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom + BOM_ADMIN_STICKY_COLUMN_WIDTHS.interior,
  dual:
    BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom
    + BOM_ADMIN_STICKY_COLUMN_WIDTHS.interior
    + BOM_ADMIN_STICKY_COLUMN_WIDTHS.single,
  special:
    BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom
    + BOM_ADMIN_STICKY_COLUMN_WIDTHS.interior
    + BOM_ADMIN_STICKY_COLUMN_WIDTHS.single
    + BOM_ADMIN_STICKY_COLUMN_WIDTHS.dual,
} as const;
const BOM_ADMIN_TRAILING_COLUMN_WIDTHS = {
  lifecycle: 118,
  actions: 176,
  from: 92,
  to: 92,
} as const;
const BOM_ADMIN_FIXED_COLUMN_WIDTH =
  Object.values(BOM_ADMIN_STICKY_COLUMN_WIDTHS).reduce((total, width) => total + width, 0)
  + Object.values(BOM_ADMIN_TRAILING_COLUMN_WIDTHS).reduce((total, width) => total + width, 0);

const BOM_LIFECYCLE_OPTIONS = [
  {
    value: "active",
    label: "Active",
    description: "Active：没有结束日期且已经生效；有正价 FOB 就可以填数量、导出 PI。",
  },
  {
    value: "phase_out",
    label: "Phase out",
    description: "Phase out：已设置结束日期但尚未超过最后有效日；当天仍可下单。",
  },
  {
    value: "historical",
    label: "History",
    description: "History：已经超过最后有效日；普通选品不显示，历史补录需显式包含。",
  },
] as const;
type BomLifecycleStatus = (typeof BOM_LIFECYCLE_OPTIONS)[number]["value"];

function colourSurchargeKey(brand: string, colourType: string): string {
  return `${brand.trim().toUpperCase()}|${colourType.trim().toLowerCase()}`;
}

function normalizeBomLifecycleStatus(value: unknown): BomLifecycleStatus {
  const raw = String(value ?? "").trim().toLowerCase();
  if (raw === "phase_out" || raw === "phase out" || raw === "phased_out") return "phase_out";
  if (raw === "historical" || raw === "history") return "historical";
  return "active";
}

function formatBomLifecycleLabel(value: unknown): string {
  const status = normalizeBomLifecycleStatus(value);
  return BOM_LIFECYCLE_OPTIONS.find((option) => option.value === status)?.label ?? "Active";
}

function formatBomLifecycleTooltip(value: unknown): string {
  const status = normalizeBomLifecycleStatus(value);
  return BOM_LIFECYCLE_OPTIONS.find((option) => option.value === status)?.description ?? "";
}

function inferBomAdminColourTier(sku: BomAdminSkuColourFields): BomAdminColourTier {
  return resolveBomAdminColourTier(sku);
}

function formatSurchargeDraft(value: number): string {
  return Number.isInteger(value) ? String(value) : value.toFixed(2);
}

function formatOrderGeniusFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function getErrorStatus(error: unknown): number | null {
  if (!error || typeof error !== "object" || !("status" in error)) return null;
  const status = Number((error as { status?: unknown }).status);
  return Number.isInteger(status) ? status : null;
}

function cleanText(value: string): string | null {
  const text = value.trim();
  return text ? text : null;
}

function deriveMaterialTemplate(codes: string[]): string {
  const cleanCodes = codes.map((code) => code.trim().toUpperCase()).filter(Boolean);
  if (cleanCodes.length === 0) return "";
  if (cleanCodes.length === 1) return cleanCodes[0];
  let prefix = cleanCodes[0];
  let reversedSuffix = cleanCodes[0].split("").reverse().join("");
  for (const code of cleanCodes.slice(1)) {
    let prefixLength = 0;
    while (prefixLength < prefix.length && prefixLength < code.length && prefix[prefixLength] === code[prefixLength]) {
      prefixLength += 1;
    }
    prefix = prefix.substring(0, prefixLength);

    const reversedCode = code.split("").reverse().join("");
    let suffixLength = 0;
    while (
      suffixLength < reversedSuffix.length
      && suffixLength < reversedCode.length
      && reversedSuffix[suffixLength] === reversedCode[suffixLength]
    ) {
      suffixLength += 1;
    }
    reversedSuffix = reversedSuffix.substring(0, suffixLength);
  }
  const suffix = reversedSuffix.split("").reverse().join("");
  if (prefix && suffix && prefix.length + suffix.length < cleanCodes[0].length) {
    return `${prefix}**${suffix}`;
  }
  return prefix + suffix || cleanCodes[0];
}

type MaterialTemplateRemarkSource = {
  materialCode: string;
  bomTemplate?: string | null;
  colourCode?: string | null;
  remark?: string | null;
};

function materialTemplateForRow(row: MaterialTemplateRemarkSource): string {
  const stored = row.bomTemplate?.trim().toUpperCase();
  if (stored) return stored;
  const materialCode = row.materialCode.trim().toUpperCase();
  const colourCode = row.colourCode?.trim().toUpperCase();
  if (materialCode && colourCode) {
    const colourIndex = materialCode.indexOf(colourCode);
    if (colourIndex >= 0) {
      return `${materialCode.slice(0, colourIndex)}**${materialCode.slice(colourIndex + colourCode.length)}`;
    }
  }
  return deriveMaterialTemplate([materialCode]) || materialCode;
}

function buildMaterialTemplateRemarkMap(rows: MaterialTemplateRemarkSource[]): Map<string, string> {
  const remarkByTemplate = new Map<string, string>();
  for (const row of rows) {
    const template = materialTemplateForRow(row);
    const remark = String(row.remark || "").trim();
    if (template && remark && !remarkByTemplate.has(template)) {
      remarkByTemplate.set(template, remark);
    }
  }
  return remarkByTemplate;
}

function normalizeAccountCode(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12);
}

function formatOrderGeniusCountryOptionLabel(
  countryCode: string,
  countryName: string | null | undefined,
): string {
  const tooltip = formatCountryCodeTooltip(countryCode);
  if (!tooltip.includes("Unknown country")) return tooltip;
  const normalized = String(countryCode || "").trim().toUpperCase();
  const name = String(countryName || "").trim();
  return name ? `${normalized} · ${name}` : normalized;
}

function uniqueCountryCodes(rows: OrderGeniusGridRow[]): string[] {
  const result: string[] = [];
  for (const row of rows) {
    const countryCode = (row._countryCode || "").trim().toUpperCase();
    if (countryCode && !result.includes(countryCode)) result.push(countryCode);
  }
  return result;
}

function quantityCellKey(
  countryCode: string | null | undefined,
  materialCode: string,
  month: number,
  year: number,
): string {
  const country = String(countryCode || "").trim().toUpperCase();
  const material = String(materialCode || "").trim().toUpperCase();
  return `${year}|${country}|${material}|${month}`;
}

function patchMatrixQuantityCell(
  matrices: Record<string, MatrixResponse>,
  countryCode: string,
  materialCode: string,
  month: number,
  nextCell: Partial<MonthCell>,
): Record<string, MatrixResponse> {
  const target = matrices[countryCode];
  if (!target) return matrices;
  const normalizedMaterial = materialCode.trim().toUpperCase();
  let changed = false;
  const monthKey = String(month);
  const rows = target.rows.map((row) => {
    if (row.materialCode.trim().toUpperCase() !== normalizedMaterial) return row;
    const currentCell = row.months[monthKey] ?? { quantity: 0, isEditable: true, rowVersion: 0 };
    const patchedCell: MonthCell = {
      ...currentCell,
      ...nextCell,
      quantity: nextCell.quantity ?? currentCell.quantity,
      isEditable: nextCell.isEditable ?? currentCell.isEditable,
      rowVersion: nextCell.rowVersion ?? currentCell.rowVersion,
    };
    if (
      patchedCell.quantity === currentCell.quantity
      && patchedCell.isEditable === currentCell.isEditable
      && patchedCell.rowVersion === currentCell.rowVersion
    ) {
      return row;
    }
    changed = true;
    const months = { ...row.months, [monthKey]: patchedCell };
    return {
      ...row,
      months,
      ttl: Object.values(months).reduce((sum, cell) => sum + cell.quantity, 0),
    };
  });
  if (!changed) return matrices;
  return {
    ...matrices,
    [countryCode]: {
      ...target,
      rows,
    },
  };
}

function suggestedOrderingAccountCode(countries: string[]): string {
  const sorted = [...countries].sort();
  if (sorted.includes("FI") && sorted.includes("SE")) return "NORDIC";
  return sorted.join("").slice(0, 12) || "ACCOUNT";
}

type MatrixRowWithCountry = MaterialSkuMatrixRow & { _countryCode?: string; sheet_name?: string | null };
function matrixDisplayFob(row: MaterialSkuMatrixRow, month: number | null): number | null {
  const cell = month == null ? undefined : row.months[String(month)];
  return cell && Object.prototype.hasOwnProperty.call(cell, "fobEur") ? cell.fobEur ?? null : row.fobEur ?? null;
}
type ProductGroupEntry = [string, MatrixRowWithCountry[]];

interface PiBatchForm {
  officialPiNo: string;
  orderDate: string;
  shipName: string;
  eta: string;
  orderingAccountCode: string;
  orderingAccountName: string;
  portOfDischarge: string;
  shipmentBatchCode: string;
}

type PiBatchMode = "by_country" | "by_account";
type OrderGeniusControlTab = "filters" | "bom" | "exports" | "pi";

const ORDER_GENIUS_CONTROL_TAB_LABELS: Record<OrderGeniusControlTab, string> = {
  filters: "Filters",
  bom: "BOM Admin",
  exports: "Import / Export",
  pi: "PI Batch",
};

interface PiBatchAllocation {
  countryCode: string;
  quantity: number;
  fobEur: number | null;
  historicalFobOverrideEur?: number;
  historicalPriceReason?: string | null;
}

interface PiBatchLineItem {
  materialCode: string;
  quantity: number;
  fobEur: number | null;
  modelName: string;
  version: string;
  exteriorColorName: string;
  interiorColorName: string | null;
  allocations?: PiBatchAllocation[];
  historicalFobOverrideEur?: number;
  historicalPriceReason?: string | null;
}

function compareProductGroupEntries(a: ProductGroupEntry, b: ProductGroupEntry): number {
  const [brandA = "", modelA = "", versionA = "", ptA = ""] = a[0].split("|");
  const [brandB = "", modelB = "", versionB = "", ptB = ""] = b[0].split("|");
  return compareProductModels(brandA, modelA, ptA, brandB, modelB, ptB)
    || versionA.localeCompare(versionB);
}

function formatProductModelName(brand: string, modelName: string, version?: string): string {
  const cleanBrand = brand.trim();
  const cleanModel = modelName.trim();
  const modelStartsWithBrand = cleanBrand
    ? cleanModel.toUpperCase().startsWith(cleanBrand.toUpperCase())
    : false;
  const baseName = modelStartsWithBrand ? cleanModel : `${cleanBrand} ${cleanModel}`.trim();
  return [baseName, version?.trim()].filter(Boolean).join(" ");
}

interface AddMaterialFormState {
  materialCode: string;
  brand: string;
  modelName: string;
  version: string;
  colour: string;
  colourCode: string;
  colourBatch: string;
  powertrain: string;
}

interface MaterialColourInput {
  colourCode: string;
  colour: string;
  colourHex: string | null;
  lineNumber: number;
}

interface MaterialSkuCreateDraft {
  materialCode: string;
  brand: string;
  modelName: string;
  version: string;
  colour: string;
  colourCode: string;
  colourHex: string | null;
  powertrain: string;
}

const EMPTY_ADD_MATERIAL: AddMaterialFormState = {
  materialCode: "",
  brand: "",
  modelName: "",
  version: "",
  colour: "",
  colourCode: "",
  colourBatch: "",
  powertrain: "",
};

function splitColourBatchLines(value: string): string[] {
  return value
    .split(/[\n;]+/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function parseColourBatch(value: string): { colours: MaterialColourInput[]; errors: string[] } {
  const colours: MaterialColourInput[] = [];
  const errors: string[] = [];
  const seenCodes = new Set<string>();
  const lines = splitColourBatchLines(value);

  lines.forEach((rawLine, index) => {
    const lineNumber = index + 1;
    let line = rawLine.replace(/^[-*•]\s*/, "").trim();
    let colourHex: string | null = null;
    const hexMatch = line.match(/\s+(#[0-9a-fA-F]{6})$/);
    if (hexMatch) {
      colourHex = hexMatch[1].toUpperCase();
      line = line.slice(0, -hexMatch[0].length).trim();
    }

    let code = "";
    let colour = "";
    const explicitMatch = line.match(/^([A-Za-z0-9]{2})\s*(?:=|,|\t)\s*(.+)$/);
    const spacedMatch = line.match(/^([A-Za-z0-9]{2})\s+(.+)$/);
    const match = explicitMatch ?? spacedMatch;
    if (match) {
      code = match[1].trim().toUpperCase();
      colour = match[2].trim();
    }

    if (!code || !colour) {
      errors.push(`Line ${lineNumber}: use "BW Khaki white"`);
      return;
    }
    if (!/^[A-Z0-9]{2}$/.test(code)) {
      errors.push(`Line ${lineNumber}: colour code must be 2 letters/numbers`);
      return;
    }
    if (seenCodes.has(code)) {
      errors.push(`Line ${lineNumber}: duplicate colour code ${code}`);
      return;
    }
    seenCodes.add(code);
    colours.push({ colourCode: code, colour, colourHex, lineNumber });
  });

  return { colours, errors };
}

function buildMaterialDrafts(form: AddMaterialFormState): {
  drafts: MaterialSkuCreateDraft[];
  errors: string[];
  isBatch: boolean;
} {
  const materialCodeTemplate = form.materialCode.trim().toUpperCase();
  const baseMissing = [
    ["Material Code", materialCodeTemplate],
    ["Brand", form.brand],
    ["Model", form.modelName],
    ["Version", form.version],
    ["Powertrain", form.powertrain],
  ].filter(([, value]) => !String(value).trim()).map(([label]) => label);
  if (baseMissing.length > 0) {
    return { drafts: [], errors: [`Missing: ${baseMissing.join(", ")}`], isBatch: false };
  }

  const batchInput = form.colourBatch.trim();
  const isTemplate = materialCodeTemplate.includes("**");
  if (batchInput || isTemplate) {
    const { colours, errors } = parseColourBatch(batchInput);
    if (!isTemplate) errors.push("Use ** in Material Code for batch colours");
    if (isTemplate && colours.length === 0) errors.push("Batch colours are required when Material Code contains **");
    const drafts = colours.map((colour) => ({
      materialCode: materialCodeTemplate.replace("**", colour.colourCode),
      brand: form.brand.trim(),
      modelName: form.modelName.trim(),
      version: form.version.trim(),
      colour: colour.colour,
      colourCode: colour.colourCode,
      colourHex: colour.colourHex,
      powertrain: form.powertrain.trim(),
    }));
    return { drafts, errors, isBatch: true };
  }

  const singleMissing = [
    ["Colour", form.colour],
    ["Code", form.colourCode],
  ].filter(([, value]) => !String(value).trim()).map(([label]) => label);
  if (singleMissing.length > 0) {
    return { drafts: [], errors: [`Missing: ${singleMissing.join(", ")}`], isBatch: false };
  }
  const colourCode = form.colourCode.trim().toUpperCase();
  if (!/^[A-Z0-9]{2}$/.test(colourCode)) {
    return { drafts: [], errors: ["Code must be 2 letters/numbers"], isBatch: false };
  }
  return {
    drafts: [{
      materialCode: materialCodeTemplate,
      brand: form.brand.trim(),
      modelName: form.modelName.trim(),
      version: form.version.trim(),
      colour: form.colour.trim(),
      colourCode,
      colourHex: null,
      powertrain: form.powertrain.trim(),
    }],
    errors: [],
    isBatch: false,
  };
}

export function OrderGeniusPage() {
  const { user, refreshUser } = useAuth();
  const { allCountriesISO } = useResolvedCountry("iso");
  const userCountries = (() => {
    const codes = [...(user?.secondaryCountries ?? [])];
    if (user?.primaryCountry && !codes.includes(user.primaryCountry)) {
      codes.unshift(user.primaryCountry);
    }
    return codes;
  })();
  const isAdmin = isAdminRole(user?.role);
  const authorizationKey = JSON.stringify([user?.username, user?.role, user?.primaryCountry, user?.secondaryCountries, user?.brands]);
  const canMaintainBom = isAdmin || (user?.role === "editor" && Boolean(user.brands?.length));
  const canFillOrders = isAdmin || ((user?.role === "editor" || user?.role === "order_filler") && Boolean(user.brands?.length));
  // ── Filter state ──────────────────────────────────────────────────
  const [countries, setCountries] = useState<CountryPaymentTerm[]>([]);
  const [selectedCountries, setSelectedCountries] = useState<string[]>(allCountriesISO);
  const primaryCountry = selectedCountries[0] ?? "SE";
  const [countrySearchQuery, setCountrySearchQuery] = useState("");
  const [countryPickerOpen, setCountryPickerOpen] = useState(false);
  const countryPickerRef = useRef<HTMLDivElement | null>(null);

  const searchedCountryOptions = useMemo(() => {
    const q = countrySearchQuery.trim().toLowerCase();
    let filtered = countries;
    // Non-admin users only see their assigned countries
    if (!isAdmin) {
      filtered = countries.filter((c) => userCountries.includes(c.countryCode));
    }
    return filtered
      .map((c) => ({
        value: c.countryCode,
        label: formatOrderGeniusCountryOptionLabel(c.countryCode, c.countryName),
        searchText: `${c.countryCode} ${c.countryName || ""} ${formatCountryCodeTooltip(c.countryCode)}`.toLowerCase(),
      }))
      .filter((c) => !q || c.searchText.includes(q));
  }, [countries, countrySearchQuery, isAdmin, userCountries]);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (countryPickerRef.current && !countryPickerRef.current.contains(event.target as Node)) {
        setCountryPickerOpen(false);
        setCountrySearchQuery("");
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);
  const [selectedYear, setSelectedYear] = useState(new Date().getFullYear());
  const [selectedMonth, setSelectedMonth] = useState<number | null>(null); // null = all months
  const [selectionDate, setSelectionDate] = useState("");
  const [includeHistorical, setIncludeHistorical] = useState(false);
  const [visibleColumns, setVisibleColumns] = useState({
    months: true, amount: true, ttlQty: true, ttlAmount: true, fob: true, materialCode: true, remark: true,
  });
  const [brandFilter, setBrandFilter] = useState("");
  const [modelFilter, setModelFilter] = useState("");
  const [powertrainFilter, setPowertrainFilter] = useState("");
  const [versionFilter, setVersionFilter] = useState("");
  const [colourFilter, setColourFilter] = useState("");
  const [materialSearch, setMaterialSearch] = useState("");
  const [debouncedMaterialSearch, setDebouncedMaterialSearch] = useState("");
  const [groupByProduct, setGroupByProduct] = useState(true);
  const [expandedProductGroups, setExpandedProductGroups] = useState<Set<string>>(() => new Set());
  const [showPtAdmin, setShowPtAdmin] = useState(false);
  const [showBomAdmin, setShowBomAdmin] = useState(false);
  const [showDeck, setShowDeck] = useState(true);
  const [controlTab, setControlTab] = useState<OrderGeniusControlTab>("filters");
  const [consolidatedView, setConsolidatedView] = useState(false);
  const [hideEmptyRows, setHideEmptyRows] = useState(false);

  const [options, setOptions] = useState<OrderGeniusOptions | null>(null);
  const [matrices, setMatrices] = useState<Record<string, MatrixResponse>>({});
  const [matrixConflictNotice, setMatrixConflictNotice] = useState<FobConflict[]>([]);
  const [fobCountryCodes, setFobCountryCodes] = useState<string[] | null>(null);
  const [bomAdminCopyTargetCountry, setBomAdminCopyTargetCountry] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [authFailureNotice, setAuthFailureNotice] = useState<{
    message: string;
    requiresLogin: boolean;
  } | null>(null);
  const authFailureProbeRef = useRef<Promise<boolean> | null>(null);

  useEffect(() => {
    const handleAuthFailure = (event: Event) => {
      const detail = (event as CustomEvent<{ path?: string; status?: number }>).detail;
      const status = Number(detail?.status);
      if (status === 401) {
        setAuthFailureNotice({
          message: "登录已失效，未保存的 BOM/订单输入仍保留在当前页面；请重新登录后再重试。",
          requiresLogin: true,
        });
        return;
      }
      if (status !== 403) return;
      setAuthFailureNotice({
        message: "正在核验当前登录身份……",
        requiresLogin: false,
      });
      const probe = authFailureProbeRef.current || refreshUser({ preserveSession: true });
      authFailureProbeRef.current = probe;
      void probe
        .then((isValid) => {
          setAuthFailureNotice(isValid
            ? {
                message: "当前账号没有执行此 BOM 操作的权限；输入未清除。",
                requiresLogin: false,
              }
            : {
                message: "登录已失效，未保存的 BOM/订单输入仍保留在当前页面；请重新登录后再重试。",
                requiresLogin: true,
              });
        })
        .catch(() => {
          setAuthFailureNotice({
            message: "无法核验登录状态；输入未清除，请检查网络后重试。",
            requiresLogin: false,
          });
        })
        .finally(() => {
          if (authFailureProbeRef.current === probe) authFailureProbeRef.current = null;
        });
    };
    window.addEventListener(AUTH_FAILURE_EVENT, handleAuthFailure);
    return () => window.removeEventListener(AUTH_FAILURE_EVENT, handleAuthFailure);
  }, [refreshUser]);

  useEffect(() => {
    if (!authFailureNotice?.requiresLogin) return undefined;
    const handleWindowFocus = () => {
      const probe = authFailureProbeRef.current || refreshUser({ preserveSession: true });
      authFailureProbeRef.current = probe;
      void probe
        .then((isValid) => {
          if (!isValid) return;
          setAuthFailureNotice({
            message: "登录已恢复，请主动重试刚才的保存。",
            requiresLogin: false,
          });
        })
        .catch(() => {
          setAuthFailureNotice({
            message: "无法核验登录状态；输入未清除，请检查网络后重试。",
            requiresLogin: false,
          });
        })
        .finally(() => {
          if (authFailureProbeRef.current === probe) authFailureProbeRef.current = null;
        });
    };
    window.addEventListener("focus", handleWindowFocus);
    return () => window.removeEventListener("focus", handleWindowFocus);
  }, [authFailureNotice?.requiresLogin, refreshUser]);

  const reLoginAfterAuthFailure = useCallback(() => {
    const redirect = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    const loginUrl = `/login?redirect=${encodeURIComponent(redirect)}`;
    let loginWindow: Window | null = null;
    try {
      loginWindow = window.open(loginUrl, "_blank", "noopener,noreferrer");
    } catch {
      loginWindow = null;
    }
    if (!loginWindow) {
      setAuthFailureNotice({
        message: "浏览器阻止了登录新标签，请允许弹窗后再重试；当前输入仍保留。",
        requiresLogin: true,
      });
    }
  }, []);

  // ── Upload state ──────────────────────────────────────────────────
  const [showUpload, setShowUpload] = useState(false);
  const [uploadFile, setUploadFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState("");
  const [uploadSessionId, setUploadSessionId] = useState("");
  const [uploadStatus, setUploadStatus] = useState("");
  const [uploadPreview, setUploadPreview] = useState<MaterialUploadPreview | null>(null);
  const [uploadDragActive, setUploadDragActive] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const [publishResult, setPublishResult] = useState<PublishBaselineResponse | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // ── Quantity import ────────────────────────────────────────────────
  const [showQtyImport, setShowQtyImport] = useState(false);
  const [qtyImportFile, setQtyImportFile] = useState<File | null>(null);
  const [qtyImportPreview, setQtyImportPreview] = useState<QuantityImportPreview | null>(null);
  const [qtyImportLoading, setQtyImportLoading] = useState(false);
  const [qtyImportResult, setQtyImportResult] = useState<QuantityImportResult | null>(null);
  const [qtyImportDragActive, setQtyImportDragActive] = useState(false);
  const qtyImportInputRef = useRef<HTMLInputElement>(null);

  // ── Quantity editing state ───────────────────────────────────────
  const [savingCells, setSavingCells] = useState<Set<string>>(new Set());
  const [cellErrors, setCellErrors] = useState<Record<string, string>>({});
  const [quantityDrafts, setQuantityDrafts] = useState<Record<string, number>>({});
  const quantityVersionRef = useRef<Record<string, number>>({});
  const gridApiRef = useRef<any>(null);

  // ── PI batch creation ─────────────────────────────────────────────
  const [piSelectedRowIds, setPiSelectedRowIds] = useState<Set<string>>(new Set());
  const [piBatchQuantities, setPiBatchQuantities] = useState<Record<string, number>>({});
  const [piBatchMode, setPiBatchMode] = useState<PiBatchMode>("by_country");
  const [piBatchForm, setPiBatchForm] = useState<PiBatchForm>({
    officialPiNo: "",
    orderDate: "",
    shipName: "",
    eta: "",
    orderingAccountCode: "",
    orderingAccountName: "",
    portOfDischarge: "",
    shipmentBatchCode: "",
  });
  const [orderingAccountCodeEdited, setOrderingAccountCodeEdited] = useState(false);
  const [creatingPiBatch, setCreatingPiBatch] = useState(false);
  const [piBatchNotice, setPiBatchNotice] = useState("");
  const [piBatchError, setPiBatchError] = useState("");
  const [confirmHistoricalPi, setConfirmHistoricalPi] = useState(false);
  const [confirmUndatedHistoricalFob, setConfirmUndatedHistoricalFob] = useState(false);
  const [confirmHistoricalSurcharge, setConfirmHistoricalSurcharge] = useState(false);
  const [historicalFobOverrides, setHistoricalFobOverrides] = useState<Record<string, string>>({});
  const [historicalPriceReasons, setHistoricalPriceReasons] = useState<Record<string, string>>({});
  const [piBatchCreatedCodes, setPiBatchCreatedCodes] = useState<string[]>([]);
  const [piAllocationPlans, setPiAllocationPlans] = useState<Record<string, VehicleAllocationPlan>>({});
  const [piExistingBatches, setPiExistingBatches] = useState<PiOrderHeader[]>([]);
  const [piPlanLoading, setPiPlanLoading] = useState(false);
  const [piPlanError, setPiPlanError] = useState("");
  const [piBatchRefreshKey, setPiBatchRefreshKey] = useState(0);
  const matrixRequestIdRef = useRef(0);
  const countryInitDone = useRef(false);
  useEffect(() => {
    matrixRequestIdRef.current += 1;
    countryInitDone.current = false;
    setMatrices({});
    setOptions(null);
    setQuantityDrafts({});
    quantityVersionRef.current = {};
    setPiSelectedRowIds(new Set());
    setPiBatchQuantities({});
    setPiAllocationPlans({});
    setPiExistingBatches([]);
    setQtyImportPreview(null);
    setQtyImportFile(null);
    setShowQtyImport(false);
    setUploadPreview(null);
    setShowBomAdmin(false);
    setSelectedCountries(isAdmin ? allCountriesISO : userCountries);
    setBrandFilter("");
    setModelFilter("");
    setVersionFilter("");
    setColourFilter("");
  }, [authorizationKey]);
  const selectedOrderMonthIsFuture = selectedMonth != null
    && (selectedYear * 12 + selectedMonth) > (new Date().getFullYear() * 12 + new Date().getMonth() + 1);

  useEffect(() => {
    if (selectedOrderMonthIsFuture && includeHistorical) setIncludeHistorical(false);
    setConfirmHistoricalPi(false);
    setConfirmUndatedHistoricalFob(false);
    setConfirmHistoricalSurcharge(false);
    setHistoricalFobOverrides({});
    setHistoricalPriceReasons({});
  }, [includeHistorical, selectedMonth, selectedOrderMonthIsFuture, selectedYear]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setDebouncedMaterialSearch(materialSearch.trim());
    }, 450);
    return () => window.clearTimeout(timer);
  }, [materialSearch]);

  useEffect(() => {
    if (gridApiRef.current) {
      setTimeout(() => {
        gridApiRef.current.refreshCells({ force: true });
        gridApiRef.current.resetRowHeights();
      }, 100);
    }
  }, [visibleColumns]);

  // ── Load countries ────────────────────────────────────────────────
  useEffect(() => {
    let active = true;
    api.getOrderGeniusCountries()
      .then((res) => {
        if (!active) return;
        setCountries(res.items);
        if (res.items.length > 0 && !countryInitDone.current) {
          countryInitDone.current = true;
          const validCodes = new Set(res.items.map((c) => c.countryCode));
          const resolved = (isAdmin ? allCountriesISO : userCountries).filter((c) => validCodes.has(c));
          if (resolved.length === 0) {
            const fallback = res.items.find((c) => c.countryCode === "SE");
            setSelectedCountries(isAdmin ? [fallback?.countryCode ?? res.items[0].countryCode] : []);
          } else {
            setSelectedCountries(resolved);
          }
        }
      })
      .catch(() => { if (active) setError("Failed to load countries"); });
    return () => { active = false; };
  }, [allCountriesISO, authorizationKey]);

  const loadFobCountries = useCallback(async () => {
    try {
      const res = await api.getOrderGeniusFobCountries();
      setFobCountryCodes((res.countries || []).map((code) => code.trim().toUpperCase()).filter(Boolean));
    } catch {
      setFobCountryCodes(null);
    }
  }, []);

  useEffect(() => {
    void loadFobCountries();
  }, [loadFobCountries, authorizationKey]);

  // ── Load options (use primary country for filter dropdowns) ────────
  useEffect(() => {
    if (!primaryCountry) return;
    let active = true;
    api
      .getOrderGeniusOptions({
        country: primaryCountry,
        brand: brandFilter || undefined,
        model: modelFilter || undefined,
        powertrain: powertrainFilter || undefined,
        version: versionFilter || undefined,
        colour: colourFilter || undefined,
      })
      .then((next) => { if (active) setOptions(next); })
      .catch((e: unknown) => { if (active) setError(getErrorMessage(e)); });
    return () => { active = false; };
  }, [authorizationKey, primaryCountry, brandFilter, modelFilter, powertrainFilter, versionFilter, colourFilter]);

  // ── Load matrices for all selected countries ────────────────────────
  const loadMatrices = useCallback((): Promise<boolean> => {
    const requestId = matrixRequestIdRef.current + 1;
    matrixRequestIdRef.current = requestId;
    if (selectedCountries.length === 0) {
      setMatrices({});
      setLoading(false);
      return Promise.resolve(true);
    }
    setLoading(true);
    setPiPlanLoading(true);
    setError("");
    const params = {
      year: selectedYear,
      brand: brandFilter || undefined,
      model: modelFilter || undefined,
      powertrain: powertrainFilter || undefined,
      version: versionFilter || undefined,
      colour: colourFilter || undefined,
      materialCodeSearch: debouncedMaterialSearch || undefined,
      selectionDate: selectionDate || undefined,
      includeHistorical,
    };
    return api
      .getOrderGeniusMatrixBatch({ countries: selectedCountries, ...params })
      .then((response) => {
        if (requestId !== matrixRequestIdRef.current) return false;
        const next: Record<string, MatrixResponse> = {};
        for (const country of selectedCountries) {
          const matrix = response.matrices[country];
          if (matrix) next[country] = matrix;
        }
        setMatrices(next);
        setMatrixConflictNotice(
          Object.values(response.matrices).flatMap((matrix) => matrix.fobConflicts || []),
        );
        const firstError = Object.values(response.errors)[0];
        if (firstError) setError(firstError);
        return !firstError;
      })
      .catch((e: unknown) => {
        if (requestId === matrixRequestIdRef.current) setError(getErrorMessage(e));
        return false;
      })
      .finally(() => {
        if (requestId === matrixRequestIdRef.current) {
          setLoading(false);
          setPiBatchRefreshKey((key) => key + 1);
        }
      });
  }, [
    authorizationKey, selectedCountries, selectedYear, brandFilter, modelFilter,
    powertrainFilter, versionFilter, colourFilter, debouncedMaterialSearch, selectionDate, includeHistorical,
  ]);

  useEffect(() => {
    loadMatrices();
  }, [loadMatrices]);

  // ── Combined matrix data ───────────────────────────────────────────
  const combinedMatrix = useMemo(() => {
    const allRows: MatrixRowWithCountry[] = [];
    let totalRows = 0;
    for (const country of selectedCountries) {
      const m = matrices[country];
      if (m) {
        totalRows += m.totalRows;
        for (const row of m.rows) {
          allRows.push({ ...row, _countryCode: country });
        }
      }
    }
    return { rows: allRows, totalRows };
  }, [matrices, selectedCountries]);

  const materialSuggestions = useMemo(() => {
    const seen = new Set<string>();
    const suggestions: Array<{ materialCode: string; remark: string | null }> = [];
    const remarkByTemplate = buildMaterialTemplateRemarkMap(combinedMatrix.rows);
    for (const row of combinedMatrix.rows) {
      const materialCode = row.materialCode?.trim();
      if (!materialCode || seen.has(materialCode)) continue;
      seen.add(materialCode);
      const templateRemark = remarkByTemplate.get(materialTemplateForRow(row));
      suggestions.push({ materialCode, remark: templateRemark ?? row.remark ?? null });
      if (suggestions.length >= 300) break;
    }
    return suggestions;
  }, [combinedMatrix.rows]);

  // ── Grid data + cell editing ──────────────────────────────────────

  const visibleMatrixRows = useMemo(() => combinedMatrix.rows.filter((row) => {
    if (selectedMonth == null || row.months?.[String(selectedMonth)]?.isEditable !== false) return true;
    if (row.fobConflict || /^(Missing|Conflicting)/.test(row.months[String(selectedMonth)]?.reason || "")) return true;
    const key = quantityCellKey(row._countryCode, row.materialCode, selectedMonth, selectedYear);
    if ((row.months[String(selectedMonth)]?.quantity ?? 0) > 0 || Object.prototype.hasOwnProperty.call(quantityDrafts, key)) return true;
    const plan = piAllocationPlans[row._countryCode || primaryCountry];
    return plan?.year === selectedYear && plan.month === selectedMonth
      && plan.existingLines.some((line) => line.materialCode === row.materialCode);
  }), [combinedMatrix.rows, piAllocationPlans, primaryCountry, quantityDrafts, selectedMonth, selectedYear]);

  const flatRows = useMemo<OrderGeniusGridRow[]>(() => {
    const remarkByTemplate = buildMaterialTemplateRemarkMap(visibleMatrixRows);

    const bomTemplateForRow = (row: MatrixRowWithCountry): string => materialTemplateForRow(row);

    const parentMaterialRemarkForRow = (row: MatrixRowWithCountry): string | undefined => {
      const remark = remarkByTemplate.get(bomTemplateForRow(row)) || String(row.remark || "").trim();
      return remark || undefined;
    };

    const commonParentMaterialRemark = (rows: MatrixRowWithCountry[]): string | undefined => {
      return resolveCommonParentMaterialRemark(
        rows,
        bomTemplateForRow,
        parentMaterialRemarkForRow,
      );
    };

    const getEffectiveQuantity = (r: MatrixRowWithCountry, month: number): number => {
      const stateKey = quantityCellKey(r._countryCode, r.materialCode, month, selectedYear);
      if (Object.prototype.hasOwnProperty.call(quantityDrafts, stateKey)) {
        return quantityDrafts[stateKey] ?? 0;
      }
      return r.months?.[String(month)]?.quantity ?? 0;
    };

    const makeRow = (r: MatrixRowWithCountry, indent = false, includeParentRemark = true): OrderGeniusGridRow => {
      const row: OrderGeniusGridRow = {
        materialCode: r.materialCode,
        bomTemplate: r.bomTemplate,
        modelName: r.modelName,
        version: r.version,
        colour: r.colour,
        colourCode: r.colourCode,
        colourTier: r.colourTier,
        colourHex: r.colourHex,
        interiorColorName: r.interiorColorName,
        fobEur: matrixDisplayFob(r, selectedMonth),
        _months: r.months,
        lifecycleStatus: r.lifecycleStatus,
        editable: r.editable,
        historicalBackfill: r.historicalBackfill,
        priceSource: r.priceSource,
        historicalSurchargeReview: r.historicalSurchargeReview,
        remark: includeParentRemark ? parentMaterialRemarkForRow(r) : undefined,
        _countryCode: r._countryCode,
        _indent: indent || undefined,
        _versions: {},
        _errors: {},
        _saving: new Set(),
      };
      const months = r.months || {};
      let ttlAmount = 0;
      for (let m = 1; m <= 12; m++) {
        const monthKey = `month_${m}` as `month_${number}`;
        const md = months[String(m)];
        const stateKey = quantityCellKey(r._countryCode, r.materialCode, m, selectedYear);
        const quantity = getEffectiveQuantity(r, m);
        const amount = quantity * (md?.fobEur ?? (md?.isEditable === false ? 0 : r.fobEur ?? 0));
        row[monthKey] = quantity;
        row[`_amount_${m}`] = amount;
        row._versions[monthKey] = md?.rowVersion ?? 0;
        if (savingCells.has(stateKey)) row._saving.add(monthKey);
        if (cellErrors[stateKey]) row._errors[monthKey] = cellErrors[stateKey];
        ttlAmount += amount;
      }
      row._ttlAmount = ttlAmount;
      return row;
    };

    // Only the saved product field supplies the group and its colour.
    const canonPt = (row: MatrixRowWithCountry): string => {
      return getBomAdminPowertrainGroup(row.powertrain);
    };

    if (!groupByProduct) {
      return visibleMatrixRows.map((row) => makeRow(row));
    }

    const aggregateRows = (rows: MatrixRowWithCountry[]) => {
      let ttl = 0;
      let ttlAmount = 0;
      let floorFob: number | null = null;
      const monthlySums: number[] = new Array(13).fill(0);
      const monthlyAmounts: number[] = new Array(13).fill(0);
      for (const row of rows) {
        const fob = row.fobEur ?? 0;
        for (let m = 1; m <= 12; m++) {
          const quantity = getEffectiveQuantity(row, m);
          const month = row.months[String(m)];
          const amount = quantity * (month && Object.prototype.hasOwnProperty.call(month, "fobEur") ? month.fobEur ?? 0 : fob);
          monthlySums[m] += quantity;
          monthlyAmounts[m] += amount;
          ttl += quantity;
          ttlAmount += amount;
        }
        const displayFob = matrixDisplayFob(row, selectedMonth);
        if (displayFob != null && displayFob > 0 && (floorFob === null || displayFob < floorFob)) {
          floorFob = displayFob;
        }
      }
      return { ttl, ttlAmount, floorFob, monthlySums, monthlyAmounts };
    };

    const makeGroupHeader = (params: {
      groupKey: string;
      label: string;
      meta: string;
      color: string;
      rows: MatrixRowWithCountry[];
      countryCode?: string;
      level: number;
      kind: "trim" | "country" | "bom";
      expanded: boolean;
    }): OrderGeniusGridRow => {
      const aggregate = aggregateRows(params.rows);
      const header: OrderGeniusGridRow = {
        materialCode: `__grp_${params.groupKey.replace(/[^a-zA-Z0-9]/g, "_")}`,
        modelName: params.label,
        version: "",
        colour: "",
        fobEur: aggregate.floorFob,
        lifecycleStatus: "active",
        editable: false,
        remark: commonParentMaterialRemark(params.rows),
        _countryCode: params.countryCode,
        _versions: {},
        _errors: {},
        _saving: new Set(),
        __type: "groupHeader",
        __groupLabel: params.label,
        __groupMeta: params.meta,
        __groupColor: params.color,
        __groupKey: params.groupKey,
        __groupKind: params.kind,
        __groupLevel: params.level,
        __expanded: params.expanded,
      };
      for (let m = 1; m <= 12; m++) {
        header[`month_${m}`] = aggregate.monthlySums[m];
        header[`_amount_${m}`] = aggregate.monthlyAmounts[m];
      }
      header._ttlAmount = aggregate.ttlAmount;
      return header;
    };

    const appendBomChildren = (
      target: OrderGeniusGridRow[],
      rows: MatrixRowWithCountry[],
      parentKey: string,
      color: string,
      level: number,
      forceExpanded: boolean,
    ) => {
      const bomGroups = new Map<string, MatrixRowWithCountry[]>();
      for (const row of rows) {
        const bomTemplate = bomTemplateForRow(row);
        if (!bomGroups.has(bomTemplate)) bomGroups.set(bomTemplate, []);
        bomGroups.get(bomTemplate)!.push(row);
      }
      const sortedBomGroups = [...bomGroups.entries()].sort(([left], [right]) => left.localeCompare(right));
      if (sortedBomGroups.length <= 1) {
        const singleBomEntry = sortedBomGroups[0];
        const singleBomTemplate = singleBomEntry?.[0] ?? "";
        const singleBomRows = singleBomEntry?.[1] ?? rows;
        const parentRemark = commonParentMaterialRemark(singleBomRows);
        if (singleBomTemplate && parentRemark) {
          const bomKey = `${parentKey}|bom|${singleBomTemplate}`;
          const aggregate = aggregateRows(singleBomRows);
          target.push(makeGroupHeader({
            groupKey: bomKey,
            label: singleBomTemplate,
            meta: `${singleBomRows.length} variants · ${aggregate.ttl.toLocaleString()} units`,
            color,
            rows: singleBomRows,
            countryCode: singleBomRows[0]?._countryCode,
            level,
            kind: "bom",
            expanded: true,
          }));
          for (const row of singleBomRows) target.push(makeRow(row, true, false));
        } else {
          for (const row of rows) target.push(makeRow(row, level > 0, false));
        }
        return;
      }
      for (const [bomTemplate, bomRows] of sortedBomGroups) {
        const bomKey = `${parentKey}|bom|${bomTemplate}`;
        const aggregate = aggregateRows(bomRows);
        const expanded = forceExpanded || expandedProductGroups.has(bomKey);
        target.push(makeGroupHeader({
          groupKey: bomKey,
          label: bomTemplate,
          meta: `${bomRows.length} variants · ${aggregate.ttl.toLocaleString()} units`,
          color,
          rows: bomRows,
          countryCode: bomRows[0]?._countryCode,
          level,
          kind: "bom",
          expanded,
        }));
        if (expanded) {
          for (const row of bomRows) target.push(makeRow(row, true, false));
        }
      }
    };

    const appendCountryChildren = (
      target: OrderGeniusGridRow[],
      rows: MatrixRowWithCountry[],
      parentKey: string,
      color: string,
      forceExpanded: boolean,
    ) => {
      const countryGroups = new Map<string, MatrixRowWithCountry[]>();
      for (const row of rows) {
        const countryCode = row._countryCode || "-";
        if (!countryGroups.has(countryCode)) countryGroups.set(countryCode, []);
        countryGroups.get(countryCode)!.push(row);
      }
      const sortedCountryGroups = [...countryGroups.entries()].sort(([left], [right]) => {
        if (left === "NL" && right !== "NL") return -1;
        if (right === "NL" && left !== "NL") return 1;
        return left.localeCompare(right);
      });
      if (sortedCountryGroups.length <= 1) {
        appendBomChildren(target, rows, parentKey, color, 1, forceExpanded);
        return;
      }
      for (const [countryCode, countryRows] of sortedCountryGroups) {
        const countryKey = `${parentKey}|country|${countryCode}`;
        const bomCount = new Set(countryRows.map(bomTemplateForRow)).size;
        const aggregate = aggregateRows(countryRows);
        const expanded = forceExpanded || expandedProductGroups.has(countryKey);
        target.push(makeGroupHeader({
          groupKey: countryKey,
          label: countryCode,
          meta: `${bomCount} BOM groups · ${countryRows.length} variants · ${aggregate.ttl.toLocaleString()} units`,
          color,
          rows: countryRows,
          countryCode,
          level: 1,
          kind: "country",
          expanded,
        }));
        if (expanded) {
          appendBomChildren(target, countryRows, countryKey, color, 2, forceExpanded);
        }
      }
    };

    // Deduplicate by full row identity
    const seen = new Set<string>();
    const deduped: MatrixRowWithCountry[] = [];
    for (const r of visibleMatrixRows) {
      const dk = `${r._countryCode || ""}|${r.materialCode}|${r.lifecycleStatus}|${r.modelName}|${r.version}|${r.colour}|${r.interiorColorName || ""}`;
      if (!seen.has(dk)) { seen.add(dk); deduped.push(r); }
    }

    // Group by: brand | modelName | version | canonicalPowertrain
    const groups = new Map<string, MatrixRowWithCountry[]>();
    for (const r of deduped) {
      if (!r.modelName || !r.version) continue; // skip rows without core data
      const pt = canonPt(r);
      const brand = r.brand || r.modelName?.split(" ")[0] || "";
      const key = `${brand}|${r.modelName}|${r.version}|${pt}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key)!.push(r);
    }

    const result: OrderGeniusGridRow[] = [];
    const sortedGroups = [...groups.entries()].sort(compareProductGroupEntries);
    for (const [groupKey, groupRows] of sortedGroups) {
      if (groupRows.length === 0) continue;
      const [brand, modelName, version, pt] = groupKey.split('|');
      if (!modelName || !pt || pt === 'Other') continue;
      const color = PT_COLORS[pt] ?? "#9ca3af";
      // Sort: active rows first, then by colour and interior variant.
      groupRows.sort((a, b) => {
        if (a.lifecycleStatus !== b.lifecycleStatus) return a.lifecycleStatus === "active" ? -1 : 1;
        return (a.colour || "").localeCompare(b.colour || "")
          || (a.interiorColorName || "").localeCompare(b.interiorColorName || "")
          || a.materialCode.localeCompare(b.materialCode);
      });
      const groupAggregate = aggregateRows(groupRows);
      const expanded = expandedProductGroups.has(groupKey);
      const displayName = formatProductModelName(brand, modelName, version);
      const labelName = formatProductModelName(brand, modelName);
      result.push(makeGroupHeader({
        groupKey,
        label: displayName,
        meta: `${labelName} · ${version} · ${pt} · ${groupRows.length} variants · ${groupAggregate.ttl.toLocaleString()} units`,
        color,
        rows: groupRows,
        countryCode: groupRows[0]?._countryCode,
        level: 0,
        kind: "trim",
        expanded,
      }));
      if (expanded || (consolidatedView && selectedCountries.length > 1)) {
        appendCountryChildren(result, groupRows, groupKey, color, consolidatedView && selectedCountries.length > 1);
      }
    }
    return result;
  }, [cellErrors, visibleMatrixRows, consolidatedView, expandedProductGroups, groupByProduct, quantityDrafts, savingCells, selectedCountries.length, selectedMonth, selectedYear]);

  // Stable refs so callback identity doesn't change on re-render (prevents grid flash)
  const selCountriesRef = useRef(selectedCountries); selCountriesRef.current = selectedCountries;
  const selYearRef = useRef(selectedYear); selYearRef.current = selectedYear;
  const selectionDateRef = useRef(selectionDate); selectionDateRef.current = selectionDate;
  const loadMatricesRef = useRef(loadMatrices); loadMatricesRef.current = loadMatrices;

  const toggleProductGroup = useCallback((groupKey: string) => {
    setExpandedProductGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupKey)) next.delete(groupKey);
      else next.add(groupKey);
      return next;
    });
  }, []);

  const saveQuantityCell = useCallback(
    async (data: OrderGeniusGridRow, month: number, newValue: unknown, oldValue: unknown) => {
      if (data.__type === "groupHeader") return;
      const monthField = `month_${month}` as `month_${number}`;
      const year = selYearRef.current;
      const countryCode = data._countryCode || selCountriesRef.current[0] || "SE";
      const key = quantityCellKey(countryCode, data.materialCode, month, year);
      const oldRowVersion = quantityVersionRef.current[key] ?? data._versions[monthField] ?? 0;
      const oldQuantityRaw = Number(oldValue);
      const oldQuantity = oldValue != null && Number.isFinite(oldQuantityRaw) ? oldQuantityRaw : null;
      const nextQuantityRaw = Number(newValue);
      const qty = Number.isFinite(nextQuantityRaw) ? Math.max(0, nextQuantityRaw) : 0;
      if (oldQuantity != null && qty === oldQuantity) return;

      const clearDraft = () => {
        setQuantityDrafts((prev) => {
          if (prev[key] !== qty) return prev;
          const next = { ...prev };
          delete next[key];
          return next;
        });
      };

      setQuantityDrafts((prev) => ({ ...prev, [key]: qty }));
      setSavingCells((prev) => new Set(prev).add(key));
      setPiPlanLoading(true);
      setCellErrors((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });
      setMatrices((prev) => patchMatrixQuantityCell(prev, countryCode, data.materialCode, month, {
        quantity: qty,
        isEditable: data.editable !== false,
        rowVersion: oldRowVersion,
      }));

      const payload: QuantityCellUpdate = {
        countryCode,
        orderYear: year,
        orderMonth: month,
        materialCode: data.materialCode,
        quantity: qty,
        rowVersion: oldRowVersion,
        includeHistorical: data.historicalBackfill === true,
      };

      try {
        const result = await api.updateQuantityCell(payload);
        data[monthField] = result.quantity;
        data._versions[monthField] = result.rowVersion;
        quantityVersionRef.current[key] = result.rowVersion;
        if (selYearRef.current === year) setMatrices((prev) => patchMatrixQuantityCell(prev, countryCode, data.materialCode, month, {
          quantity: result.quantity,
          isEditable: true,
          rowVersion: result.rowVersion,
        }));
        clearDraft();
      } catch (err: unknown) {
        const msg = getErrorMessage(err);
        const authStatus = getErrorStatus(err);
        if (authStatus === 401 || authStatus === 403) {
          setCellErrors((prev) => ({ ...prev, [key]: msg }));
          return;
        }
        if (msg.toLowerCase().includes("conflict") || msg.includes("409")) {
          try {
            const latestMatrix = await api.getOrderGeniusMatrix({
              country: countryCode,
              year,
              materialCodeSearch: data.materialCode,
              selectionDate: selectionDateRef.current || undefined,
            });
            const normalizedMaterial = data.materialCode.trim().toUpperCase();
            const latestRow = latestMatrix.rows.find(
              (row) => row.materialCode.trim().toUpperCase() === normalizedMaterial,
            );
            const latestCell = latestRow?.months?.[String(month)];
            const latestQuantity = latestCell?.quantity ?? 0;
            if (oldQuantity != null && latestQuantity !== oldQuantity) {
              throw new Error("Concurrent update conflict — refresh and try again.");
            }
            const retryResult = await api.updateQuantityCell({
              ...payload,
              rowVersion: latestCell?.rowVersion ?? oldRowVersion,
            });
            data[monthField] = retryResult.quantity;
            data._versions[monthField] = retryResult.rowVersion;
            quantityVersionRef.current[key] = retryResult.rowVersion;
            if (selYearRef.current === year) setMatrices((prev) => patchMatrixQuantityCell(prev, countryCode, data.materialCode, month, {
              quantity: retryResult.quantity,
              isEditable: true,
              rowVersion: retryResult.rowVersion,
            }));
            setCellErrors((prev) => {
              if (!Object.prototype.hasOwnProperty.call(prev, key)) return prev;
              const next = { ...prev };
              delete next[key];
              return next;
            });
            clearDraft();
          } catch (retryErr: unknown) {
            setCellErrors((prev) => ({ ...prev, [key]: getErrorMessage(retryErr) }));
            loadMatricesRef.current();
          }
        } else if (oldQuantity == null) {
          setCellErrors((prev) => ({ ...prev, [key]: msg }));
          loadMatricesRef.current();
        } else {
          setCellErrors((prev) => ({ ...prev, [key]: msg }));
        }
      } finally {
        setPiBatchRefreshKey((current) => current + 1);
        setSavingCells((prev) => {
          const next = new Set(prev);
          next.delete(key);
          return next;
        });
      }
    },
    [], // stable — all dynamic values via refs
  );

  const handleCellValueChanged = useCallback((event: CellValueChangedEvent<OrderGeniusGridRow>) => {
    const field = event.colDef.field;
    if (!event.data || !field?.startsWith("month_")) return;
    return saveQuantityCell(event.data, Number(field.slice(6)), event.newValue, event.oldValue);
  }, [saveQuantityCell]);

  // ── Consolidated planning view (multi-country) ──────────────────
  const displayRows = useMemo(() => {
    let filtered = flatRows.filter((r) => {
      const modelName = r.modelName?.trim() || "";
      const modelOk = modelName && !/^[\d\s]+$/.test(modelName);
      if (r.__type === "groupHeader") return Boolean(modelOk && r.__groupLabel);
      return modelOk;
    });
    // Group headers already carry monthly sums, so empty filtering works while children are collapsed.
    if (hideEmptyRows) {
      const monthsToCheck = selectedMonth ? [selectedMonth] : Array.from({length:12},(_,i)=>i+1);
      const rowHasData = (r: OrderGeniusGridRow): boolean =>
        monthsToCheck.some((m) => (r[`month_${m}`] || 0) > 0);
      filtered = filtered.filter((r) => {
        if (r.__type === "consolidated_parent") return true;
        return rowHasData(r);
      });
    }
    if (!consolidatedView || selectedCountries.length <= 1) return filtered;

    // Group by product identity (model+version+material), sum across countries
    const groups = new Map<string, { parent: OrderGeniusGridRow; children: OrderGeniusGridRow[] }>();
    for (const row of filtered) {
      if (row.__type === "groupHeader") continue;
      const key = `${row.modelName}|${row.version}|${row.materialCode}`;
      if (!groups.has(key)) {
        groups.set(key, {
          parent: {
            ...row,
            materialCode: row.materialCode,
            modelName: row.modelName,
            version: row.version,
            _countryCode: "",
            _saving: new Set(),
            _errors: {},
            __type: "consolidated_parent",
          },
          children: [],
        });
        const parent = groups.get(key)!.parent;
        for (let m = 1; m <= 12; m++) {
          parent[`month_${m}`] = 0;
          parent[`_amount_${m}`] = 0;
        }
        parent._ttlAmount = 0;
      }
      const g = groups.get(key)!;
      g.children.push(row);
      for (let m = 1; m <= 12; m++) {
        const amountField = `_amount_${m}` as `_amount_${number}`;
        g.parent[`month_${m}`] = (g.parent[`month_${m}`] || 0) + (row[`month_${m}`] || 0);
        g.parent[amountField] = (g.parent[amountField] || 0) + (row[amountField] || 0);
        g.parent._ttlAmount = (g.parent._ttlAmount || 0) + (row[amountField] || 0);
      }
    }

    const result: OrderGeniusGridRow[] = [];
    for (const [, g] of groups) {
      const ttl = Array.from({ length: 12 }, (_, idx) => g.parent[`month_${idx + 1}`] || 0)
        .reduce((sum, quantity) => sum + quantity, 0);
      g.parent._ttlAmount = Array.from({ length: 12 }, (_, idx) => g.parent[`_amount_${idx + 1}`] || 0)
        .reduce((sum, amount) => sum + amount, 0);
      g.parent.__groupLabel = `${g.parent.modelName} · ${g.parent.version} · ${g.children.length} countries · TTL ${ttl}`;
      result.push(g.parent);
      if (g.children.length > 1) {
        for (const child of g.children) {
          child._indent = true;
          result.push(child);
        }
      }
    }
    return result;
  }, [flatRows, consolidatedView, selectedCountries.length, hideEmptyRows, selectedMonth]);

  const piCandidateRows = useMemo<OrderGeniusGridRow[]>(() => {
    if (selectedMonth == null) return [];
    const remarkByTemplate = buildMaterialTemplateRemarkMap(combinedMatrix.rows);
    const seen = new Set<string>();
    const result: OrderGeniusGridRow[] = [];
    for (const row of combinedMatrix.rows) {
      const modelName = row.modelName?.trim() || "";
      if (!modelName || /^[\d\s]+$/.test(modelName)) continue;
      const rowKey = `${row._countryCode || ""}|${row.materialCode}|${row.lifecycleStatus}|${row.modelName}|${row.version}|${row.colour}|${row.interiorColorName || ""}`;
      if (seen.has(rowKey)) continue;
      seen.add(rowKey);
      const stateKey = quantityCellKey(row._countryCode, row.materialCode, selectedMonth, selectedYear);
      const quantity = Object.prototype.hasOwnProperty.call(quantityDrafts, stateKey)
        ? quantityDrafts[stateKey] ?? 0
        : row.months?.[String(selectedMonth)]?.quantity ?? 0;
      const candidateRow: OrderGeniusGridRow = {
        materialCode: row.materialCode,
        bomTemplate: row.bomTemplate,
        modelName: row.modelName,
        version: row.version,
        colour: row.colour,
        colourCode: row.colourCode,
        colourTier: row.colourTier,
        colourHex: row.colourHex,
        interiorColorName: row.interiorColorName,
        fobEur: matrixDisplayFob(row, selectedMonth),
        _months: row.months,
        lifecycleStatus: row.lifecycleStatus,
        editable: row.editable,
        historicalBackfill: row.historicalBackfill,
        priceSource: row.priceSource,
        historicalSurchargeReview: row.historicalSurchargeReview,
        remark: remarkByTemplate.get(materialTemplateForRow(row)) ?? row.remark ?? undefined,
        _countryCode: row._countryCode,
        _versions: {},
        _errors: {},
        _saving: new Set(),
      };
      candidateRow[`month_${selectedMonth}`] = quantity;
      candidateRow._versions[`month_${selectedMonth}`] = row.months?.[String(selectedMonth)]?.rowVersion ?? 0;
      result.push(candidateRow);
    }
    return result;
  }, [combinedMatrix.rows, quantityDrafts, selectedMonth, selectedYear]);

  const currentQuantityKeys = piCandidateRows.map((row) => quantityCellKey(row._countryCode, row.materialCode, selectedMonth ?? 0, selectedYear));
  const quantitySaving = currentQuantityKeys.some((key) => savingCells.has(key));
  const quantityError = currentQuantityKeys.map((key) => cellErrors[key]).find(Boolean);

  useEffect(() => {
    if (selectedMonth == null || selectedCountries.length === 0) {
      setPiAllocationPlans({});
      setPiExistingBatches([]);
      setPiPlanError("");
      setPiPlanLoading(false);
      return;
    }
    if (quantitySaving) return;
    let cancelled = false;
    setPiPlanLoading(true);
    setPiPlanError("");

    const loadBatchesForCountry = async (countryCode: string): Promise<PiOrderHeader[]> => {
      const month = `${selectedYear}-${String(selectedMonth).padStart(2, "0")}`;
      const first = await api.getVehicleAllocationPis({ country: countryCode, month, page: 1, pageSize: 200 });
      const batches = [...first.items];
      const totalPages = Math.ceil(first.total / 200);
      for (let pageNumber = 2; pageNumber <= totalPages; pageNumber += 1) {
        const page = await api.getVehicleAllocationPis({
          country: countryCode,
          month,
          page: pageNumber,
          pageSize: 200,
        });
        batches.push(...page.items);
      }
      return batches;
    };

    void Promise.all(selectedCountries.map(async (countryCode) => ({
      countryCode,
      plan: await api.getVehicleAllocationOrderMatrixPlan(countryCode, selectedYear, selectedMonth),
      batches: await loadBatchesForCountry(countryCode),
    })))
      .then((results) => {
        if (cancelled) return;
        const plans: Record<string, VehicleAllocationPlan> = {};
        const batches = new Map<string, PiOrderHeader>();
        results.forEach((result) => {
          plans[result.countryCode] = result.plan;
          result.batches.forEach((batch) => batches.set(batch.piCode, batch));
        });
        setPiAllocationPlans(plans);
        setPiExistingBatches(Array.from(batches.values()).sort((a, b) => (
          b.piCode.localeCompare(a.piCode)
        )));
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setPiAllocationPlans({});
          setPiExistingBatches([]);
          setPiPlanError(getErrorMessage(err));
        }
      })
      .finally(() => {
        if (!cancelled) setPiPlanLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [piBatchRefreshKey, selectedCountries, selectedMonth, selectedYear, quantitySaving]);

  const piReady = selectedMonth != null && !loading && !error && !quantitySaving && !quantityError && !piPlanLoading && !piPlanError
    && selectedCountries.length > 0 && selectedCountries.every((country) => {
      const plan = piAllocationPlans[country];
      return plan?.countryCode === country && plan.year === selectedYear && plan.month === selectedMonth;
    });

  const piPlanLinesByCountryMaterial = useMemo(() => {
    const result = new Map<string, VehicleAllocationPlanLine>();
    Object.entries(piAllocationPlans).forEach(([countryCode, plan]) => {
      plan.lineItems.forEach((line) => {
        if (line.materialCode) result.set(`${countryCode}|${line.materialCode}`, line);
      });
    });
    return result;
  }, [piAllocationPlans]);

  const piCodesByCountryMaterial = useMemo(() => {
    const result = new Map<string, string[]>();
    Object.entries(piAllocationPlans).forEach(([countryCode, plan]) => {
      plan.existingLines.forEach((line) => {
        const materialCode = line.materialCode?.trim();
        const piCode = line.piCode?.trim();
        if (!materialCode || !piCode) return;
        const key = `${countryCode}|${materialCode}`;
        const current = result.get(key) ?? [];
        if (!current.includes(piCode)) current.push(piCode);
        result.set(key, current);
      });
    });
    return result;
  }, [piAllocationPlans]);

  const piPlanTotals = useMemo(() => {
    const totals = { selectedQuantity: 0, generatedQuantity: 0, remainingQuantity: 0, overGeneratedQuantity: 0 };
    for (const row of piCandidateRows) {
      const line = piPlanLinesByCountryMaterial.get(`${row._countryCode || primaryCountry}|${row.materialCode}`);
      if (!line) continue;
      totals.selectedQuantity += line.selectedQuantity;
      totals.generatedQuantity += line.generatedQuantity;
      totals.remainingQuantity += line.remainingQuantity;
      totals.overGeneratedQuantity += line.overGeneratedQuantity;
    }
    return totals;
  }, [piCandidateRows, piPlanLinesByCountryMaterial, primaryCountry]);

  const remainingPiQuantity = useCallback((row: OrderGeniusGridRow): number => {
    const countryCode = row._countryCode || primaryCountry;
    return piPlanLinesByCountryMaterial.get(`${countryCode}|${row.materialCode}`)?.remainingQuantity ?? 0;
  }, [piPlanLinesByCountryMaterial, primaryCountry]);

  const selectablePiRows = useMemo(() => {
    if (selectedMonth == null) return [];
    const monthField = `month_${selectedMonth}` as `month_${number}`;
    return piCandidateRows.filter((row) =>
      row.__type !== "groupHeader"
      && row.__type !== "consolidated_parent"
      && (row.lifecycleStatus !== "historical" || (includeHistorical && row.historicalBackfill === true))
      && (row[monthField] || 0) > 0
      && (row.fobEur ?? 0) > 0
      && row._months?.[String(selectedMonth)]?.isEditable !== false
      && remainingPiQuantity(row) > 0,
    );
  }, [includeHistorical, piCandidateRows, remainingPiQuantity, selectedMonth]);

  const selectablePiRowsById = useMemo(() => {
    const result = new Map<string, OrderGeniusGridRow>();
    for (const row of selectablePiRows) {
      result.set(getOrderGeniusRowId(row), row);
    }
    return result;
  }, [selectablePiRows]);

  const selectablePiRowIds = useMemo(() => new Set(piReady ? selectablePiRowsById.keys() : []), [piReady, selectablePiRowsById]);
  const availablePiUnits = selectablePiRows.reduce((sum, row) => sum + remainingPiQuantity(row), 0);
  const piReadinessText = quantitySaving ? "Saving quantity…"
    : quantityError ? `Quantity not saved: ${quantityError}`
    : error ? `Price refresh failed: ${error}`
    : piPlanError ? `PI availability failed: ${piPlanError}`
    : !piReady ? "Updating PI availability…" : `PI ready · ${availablePiUnits} units available`;
  const retryPiAvailability = () => {
    if (quantityError && selectedMonth != null) {
      for (const row of piCandidateRows) {
        const key = quantityCellKey(row._countryCode, row.materialCode, selectedMonth, selectedYear);
        if (cellErrors[key] && quantityDrafts[key] != null) void saveQuantityCell(row, selectedMonth, quantityDrafts[key], null);
      }
    } else {
      setPiPlanLoading(true);
      if (error) void loadMatrices();
      else setPiBatchRefreshKey((key) => key + 1);
    }
  };

  const selectedPiRows = useMemo(() => {
    const result: OrderGeniusGridRow[] = [];
    for (const rowId of piSelectedRowIds) {
      const row = selectablePiRowsById.get(rowId);
      if (row) result.push(row);
    }
    return result;
  }, [piSelectedRowIds, selectablePiRowsById]);
  const selectedHistoricalPiRows = useMemo(
    () => selectedPiRows.filter((row) => row.historicalBackfill === true),
    [selectedPiRows],
  );
  const selectedHistoricalNeedsUndatedConfirmation = useMemo(
    () => selectedHistoricalPiRows.some((row) => row.priceSource === "undated_default"),
    [selectedHistoricalPiRows],
  );
  const selectedHistoricalNeedsSurchargeConfirmation = useMemo(
    () => selectedHistoricalPiRows.some(
      (row) => row.historicalSurchargeReview?.requiresConfirmation === true,
    ),
    [selectedHistoricalPiRows],
  );

  const selectedPiQuantityTotal = useMemo(() => {
    return selectedPiRows.reduce((sum, row) => {
      const rowId = getOrderGeniusRowId(row);
      const remainingQuantity = remainingPiQuantity(row);
      return sum + Math.min(piBatchQuantities[rowId] ?? remainingQuantity, remainingQuantity);
    }, 0);
  }, [piBatchQuantities, remainingPiQuantity, selectedPiRows]);

  const allSelectablePiRowsSelected = useMemo(() => {
    return selectedMonth != null
      && selectablePiRows.length > 0
      && selectablePiRows.every((row) => piSelectedRowIds.has(getOrderGeniusRowId(row)));
  }, [piSelectedRowIds, selectablePiRows, selectedMonth]);
  const partialSelectablePiRowsSelected = selectedPiRows.length > 0 && !allSelectablePiRowsSelected;

  const selectedPiCountries = useMemo(() => uniqueCountryCodes(selectedPiRows), [selectedPiRows]);
  const piBatchScopeSummary = useMemo(() => {
    if (selectedMonth == null) return "Select one month";
    if (selectedPiRows.length === 0) return "Select PI rows";
    if (piBatchMode === "by_account") {
      return `1 PI · ${selectedPiCountries.join("/") || primaryCountry}`;
    }
    return `${selectedPiCountries.length || 1} PI${(selectedPiCountries.length || 1) > 1 ? "s" : ""} · by country`;
  }, [piBatchMode, primaryCountry, selectedMonth, selectedPiCountries, selectedPiRows.length]);

  useEffect(() => {
    setPiSelectedRowIds(new Set());
    setPiBatchQuantities({});
    setOrderingAccountCodeEdited(false);
    setPiBatchNotice("");
    setPiBatchError("");
    setPiBatchCreatedCodes([]);
  }, [selectedMonth, selectedYear, selectedCountries]);

  useEffect(() => {
    if (piBatchMode !== "by_account" || selectedPiCountries.length === 0) return;
    if (orderingAccountCodeEdited) return;
    const nextSuggestion = suggestedOrderingAccountCode(selectedPiCountries);
    setPiBatchForm((current) => {
      if (current.orderingAccountCode === nextSuggestion) return current;
      return {
        ...current,
        orderingAccountCode: nextSuggestion,
      };
    });
  }, [orderingAccountCodeEdited, piBatchMode, selectedPiCountries]);

  useEffect(() => {
    if (!piReady) return;
    setPiSelectedRowIds((current) => {
      const next = new Set<string>();
      current.forEach((rowId) => {
        if (selectablePiRowsById.has(rowId)) next.add(rowId);
      });
      return next.size === current.size ? current : next;
    });
  }, [piReady, selectablePiRowsById]);

  // ── Upload handlers ───────────────────────────────────────────────

  function selectUploadFile(file: File): void {
    setUploadFile(file);
    setUploadSessionId("");
    setUploadStatus("");
    setUploadPreview(null);
    setPublishResult(null);
    setUploadProgress("");
    setError("");
  }

  const handleFileSelect = (e: ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (f) selectUploadFile(f);
  };

  const handleUploadDragState = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setUploadDragActive(event.type !== "dragleave");
  };

  const handleUploadDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setUploadDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (file) selectUploadFile(file);
  };

  const handleUploadDropzoneKeyboard = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      fileInputRef.current?.click();
    }
  };

  const handleUpload = async () => {
    if (!uploadFile) return;
    setUploadProgress("Initiating...");
    setUploadStatus("");
    setError("");
    try {
      const session = await api.initiateMaterialMasterUpload(
        uploadFile.name,
        uploadFile.size,
      );
      setUploadSessionId(session.uploadId);
      const totalChunks = session.totalChunks;
      const chunkSize = session.chunkSize || CHUNK_SIZE;

      for (let i = 0; i < totalChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, uploadFile.size);
        const blob = uploadFile.slice(start, end);
        setUploadProgress(
          `Uploading chunk ${i + 1}/${totalChunks} (${Math.round(
            (i / totalChunks) * 100,
          )}%)`,
        );
        await api.uploadMaterialMasterChunk(session.uploadId, i, blob);
      }

      setUploadProgress("Assembling file...");
      await api.completeMaterialMasterUpload(session.uploadId);

      setUploadProgress("Parsing...");
      const parseResult = await api.parseMaterialMasterUpload(session.uploadId);
      const parsedRows = Number(
        (parseResult as Record<string, unknown>).totalRows
        ?? (parseResult as Record<string, unknown>).total_rows
        ?? 0,
      );
      setUploadStatus(`Parsed: ${parsedRows} rows`);

      setUploadProgress("Loading preview...");
      const preview = await api.getMaterialMasterPreview(session.uploadId);
      setUploadPreview(preview);
      setUploadProgress("");
    } catch (err) {
      setError(getErrorMessage(err));
      setUploadProgress("");
    }
  };

  const handlePublish = async () => {
    if (!uploadSessionId) return;
    setPublishing(true);
    setError("");
    try {
      const result = await api.publishMaterialMaster(uploadSessionId);
      setPublishResult(result);
      setUploadStatus("Published!");
      loadMatrices();
    } catch (err) {
      setError(getErrorMessage(err));
    } finally {
      setPublishing(false);
    }
  };

  const clearUpload = () => {
    setUploadFile(null);
    setUploadSessionId("");
    setUploadStatus("");
    setUploadPreview(null);
    setPublishResult(null);
    setUploadProgress("");
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  // ── Quantity import handlers ───────────────────────────────────────

  const handleQtyImportFile = (e: ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0];
    if (!f) return;
    processQtyImportFile(f);
  };

  const handleQtyImportDragState = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setQtyImportDragActive(event.type !== "dragleave");
  };

  const handleQtyImportDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    setQtyImportDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (file) processQtyImportFile(file);
  };

  const handleQtyImportDropzoneKeyboard = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      qtyImportInputRef.current?.click();
    }
  };

  const processQtyImportFile = (f: File) => {
    setQtyImportFile(f);
    setQtyImportPreview(null);
    setQtyImportResult(null);
    setQtyImportLoading(true);
    api.previewOrderQuantityImport(f)
      .then((preview) => setQtyImportPreview(preview))
      .catch((err: unknown) => setError(getErrorMessage(err)))
      .finally(() => setQtyImportLoading(false));
    if (qtyImportInputRef.current) qtyImportInputRef.current.value = "";
  };

  const handleQtyImportApply = async () => {
    if (!qtyImportPreview?.importId) return;
    setQtyImportLoading(true);
    try {
      const result = await api.applyOrderQuantityImport(qtyImportPreview.importId);
      setQtyImportResult(result);
      loadMatrices();
    } catch (err: unknown) {
      setError(getErrorMessage(err));
    } finally {
      setQtyImportLoading(false);
    }
  };

  const closeQtyImport = () => {
    setShowQtyImport(false);
    setQtyImportFile(null);
    setQtyImportPreview(null);
    setQtyImportResult(null);
  };

  const togglePiBatchRow = useCallback((row: OrderGeniusGridRow, selected: boolean): void => {
    const rowId = getOrderGeniusRowId(row);
    if (!piReady || !selectablePiRowsById.has(rowId)) return;
    const remainingQuantity = remainingPiQuantity(row);
    setPiSelectedRowIds((current) => {
      const next = new Set(current);
      if (selected) next.add(rowId);
      else next.delete(rowId);
      return next;
    });
    setPiBatchQuantities((current) => {
      const next = { ...current };
      if (selected) next[rowId] = remainingQuantity;
      else delete next[rowId];
      return next;
    });
    setPiBatchNotice("");
    setPiBatchError("");
    setPiBatchCreatedCodes([]);
  }, [piReady, remainingPiQuantity, selectablePiRowsById]);

  const updatePiBatchQuantity = (row: OrderGeniusGridRow, quantity: number): void => {
    const rowId = getOrderGeniusRowId(row);
    const remainingQuantity = remainingPiQuantity(row);
    const nextQuantity = Math.max(0, Math.min(Math.floor(quantity || 0), remainingQuantity));
    setPiBatchQuantities((current) => ({ ...current, [rowId]: nextQuantity }));
    setPiBatchNotice("");
    setPiBatchError("");
    setPiBatchCreatedCodes([]);
  };

  const toggleAllPiBatchRows = useCallback((selected: boolean): void => {
    if (!piReady) return;
    if (!selected || selectedMonth == null) {
      setPiSelectedRowIds(new Set());
      setPiBatchQuantities({});
      setPiBatchNotice("");
      setPiBatchError("");
      setPiBatchCreatedCodes([]);
      return;
    }
    const nextIds = new Set<string>();
    const nextQuantities: Record<string, number> = {};
    for (const row of selectablePiRows) {
      const rowId = getOrderGeniusRowId(row);
      nextIds.add(rowId);
      nextQuantities[rowId] = remainingPiQuantity(row);
    }
    setPiSelectedRowIds(nextIds);
    setPiBatchQuantities(nextQuantities);
    setPiBatchNotice("");
    setPiBatchError("");
    setPiBatchCreatedCodes([]);
  }, [piReady, remainingPiQuantity, selectablePiRows, selectedMonth]);

  const piSelectionSummary = useMemo(() => ({
    selectedCount: selectedPiRows.length,
    selectableCount: piReady ? selectablePiRows.length : 0,
    allSelected: allSelectablePiRowsSelected,
    partialSelected: partialSelectablePiRowsSelected,
    onToggleAll: toggleAllPiBatchRows,
  }), [
    allSelectablePiRowsSelected,
    partialSelectablePiRowsSelected,
    piReady,
    selectablePiRows.length,
    selectedPiRows.length,
    toggleAllPiBatchRows,
  ]);

  const vehicleAllocationUrl = (piCode: string): string => {
    const params = new URLSearchParams({ pi: piCode });
    return `/product/order-genius/vehicle-allocation?${params.toString()}`;
  };

  const clearPiBatchSelection = (): void => {
    setPiSelectedRowIds(new Set());
    setPiBatchQuantities({});
    setOrderingAccountCodeEdited(false);
    setPiBatchNotice("");
    setPiBatchError("");
    setPiBatchCreatedCodes([]);
  };

  const handleCreatePiBatch = async (): Promise<void> => {
    if (!piReady) {
      setPiBatchError(piReadinessText);
      return;
    }
    if (selectedMonth == null) {
      setPiBatchError("Select one month before creating PI");
      return;
    }
    if (selectedPiRows.length === 0) {
      setPiBatchError("Select at least one order row");
      return;
    }
    if (selectedHistoricalPiRows.length > 0) {
      if (!includeHistorical || !confirmHistoricalPi) {
        setPiBatchError("Confirm that this Historical PI backfill will not reactivate the material / 请确认历史补录不会重新启用物料");
        return;
      }
      if (!piBatchForm.orderDate) {
        setPiBatchError("Historical PI backfill requires an order date inside the selected month / 历史补录必须选择订单月份内的具体日期");
        return;
      }
      if (selectedHistoricalNeedsUndatedConfirmation && !confirmUndatedHistoricalFob) {
        setPiBatchError("Confirm the undated default FOB or create a dated price period / 请确认无日期基准价或先建立日期价格区间");
        return;
      }
      if (selectedHistoricalNeedsSurchargeConfirmation && !confirmHistoricalSurcharge) {
        setPiBatchError("Review and confirm the Historical colour surcharge / 请核对并确认历史颜色加价");
        return;
      }
    }

    const byCountry = new Map<string, Map<string, PiBatchLineItem>>();
    const byMaterial = new Map<string, PiBatchLineItem>();
    for (const row of selectedPiRows) {
      const rowId = getOrderGeniusRowId(row);
      const remainingQuantity = remainingPiQuantity(row);
      const requestedQuantity = Math.floor(piBatchQuantities[rowId] ?? remainingQuantity);
      if (requestedQuantity <= 0) continue;
      if (requestedQuantity > remainingQuantity) {
        setPiBatchError(`PI quantity exceeds remaining quantity: ${row.materialCode} (remaining ${remainingQuantity})`);
        return;
      }
      const countryCode = row._countryCode || primaryCountry;
      const historicalOverrideText = historicalFobOverrides[rowId]?.trim() ?? "";
      const historicalOverride = historicalOverrideText === "" ? undefined : Number(historicalOverrideText);
      const historicalReason = cleanText(historicalPriceReasons[rowId] ?? "");
      if (historicalOverride !== undefined && (!Number.isFinite(historicalOverride) || historicalOverride < 0)) {
        setPiBatchError(`Historical FOB override must be zero or greater: ${row.materialCode}`);
        return;
      }
      if (historicalOverride !== undefined && !historicalReason) {
        setPiBatchError(`Historical FOB override requires a reason: ${row.materialCode}`);
        return;
      }
      const items = byCountry.get(countryCode) ?? new Map<string, PiBatchLineItem>();
      const existing = items.get(row.materialCode);
      if (existing) {
        existing.quantity += requestedQuantity;
      } else {
        items.set(row.materialCode, {
          materialCode: row.materialCode,
          quantity: requestedQuantity,
          fobEur: row.fobEur,
          modelName: row.modelName,
          version: row.version,
          exteriorColorName: row.colour,
          interiorColorName: row.interiorColorName ?? null,
          historicalFobOverrideEur: historicalOverride,
          historicalPriceReason: historicalReason,
        });
      }
      byCountry.set(countryCode, items);

      const materialExisting = byMaterial.get(row.materialCode);
      const allocation: PiBatchAllocation = {
        countryCode,
        quantity: requestedQuantity,
        fobEur: row.fobEur,
        historicalFobOverrideEur: historicalOverride,
        historicalPriceReason: historicalReason,
      };
      if (materialExisting) {
        materialExisting.quantity += requestedQuantity;
        const existingAllocation = materialExisting.allocations?.find((item) => item.countryCode === countryCode);
        if (existingAllocation) {
          existingAllocation.quantity += requestedQuantity;
        } else {
          materialExisting.allocations = [...(materialExisting.allocations ?? []), allocation];
        }
      } else {
        byMaterial.set(row.materialCode, {
          materialCode: row.materialCode,
          quantity: requestedQuantity,
          fobEur: row.fobEur,
          modelName: row.modelName,
          version: row.version,
          exteriorColorName: row.colour,
          interiorColorName: row.interiorColorName ?? null,
          allocations: [allocation],
        });
      }
    }

    if (byCountry.size === 0) {
      setPiBatchError("Selected PI quantity must be greater than 0");
      return;
    }
    if (piBatchMode === "by_account") {
      const accountCode = normalizeAccountCode(
        piBatchForm.orderingAccountCode || suggestedOrderingAccountCode(selectedPiCountries),
      );
      if (accountCode.length < 2) {
        setPiBatchError("Ordering account code must be at least 2 letters or numbers");
        return;
      }
    }

    setCreatingPiBatch(true);
    setPiBatchError("");
    setPiBatchNotice("");
    setPiBatchCreatedCodes([]);
    try {
      const createdCodes: string[] = [];
      if (piBatchMode === "by_account") {
        const marketCountryCodes = selectedPiCountries.length > 0 ? selectedPiCountries : [primaryCountry];
        const accountCode = normalizeAccountCode(
          piBatchForm.orderingAccountCode || suggestedOrderingAccountCode(marketCountryCodes),
        );
        const result = await api.generateVehicleAllocationFromOrderMatrix({
          countryCode: marketCountryCodes[0],
          orderYear: selectedYear,
          orderMonth: selectedMonth,
          orderingAccountCode: accountCode,
          orderingAccountName: cleanText(piBatchForm.orderingAccountName),
          marketCountryCodes,
          shipmentBatchCode: cleanText(piBatchForm.shipmentBatchCode),
          portOfDischarge: cleanText(piBatchForm.portOfDischarge),
          officialPiNo: cleanText(piBatchForm.officialPiNo),
          orderDate: cleanText(piBatchForm.orderDate),
          shipName: cleanText(piBatchForm.shipName),
          eta: cleanText(piBatchForm.eta),
          lineItems: Array.from(byMaterial.values()),
          includeHistorical: selectedHistoricalPiRows.length > 0,
          confirmHistorical: confirmHistoricalPi,
          confirmUndatedDefaultFob: confirmUndatedHistoricalFob,
          confirmHistoricalSurcharge,
        });
        createdCodes.push(result.piCode);
      } else {
        for (const [countryCode, lineItems] of byCountry) {
          const result = await api.generateVehicleAllocationFromOrderMatrix({
            countryCode,
            orderYear: selectedYear,
            orderMonth: selectedMonth,
            orderingAccountCode: countryCode,
            marketCountryCodes: [countryCode],
            officialPiNo: cleanText(piBatchForm.officialPiNo),
            orderDate: cleanText(piBatchForm.orderDate),
            shipName: cleanText(piBatchForm.shipName),
            eta: cleanText(piBatchForm.eta),
            lineItems: Array.from(lineItems.values()),
            includeHistorical: selectedHistoricalPiRows.length > 0,
            confirmHistorical: confirmHistoricalPi,
            confirmUndatedDefaultFob: confirmUndatedHistoricalFob,
            confirmHistoricalSurcharge,
          });
          createdCodes.push(result.piCode);
        }
      }
      clearPiBatchSelection();
      setPiBatchForm((current) => ({
        ...current,
        officialPiNo: "",
        shipName: "",
        eta: "",
        shipmentBatchCode: "",
      }));
      setPiBatchNotice(`Created ${createdCodes.join(", ")}`);
      setPiBatchCreatedCodes(createdCodes);
      setPiBatchRefreshKey((key) => key + 1);
    } catch (err: unknown) {
      setPiBatchError(`PI creation failed / PI 创建失败: ${getErrorMessage(err)}`);
      setPiBatchRefreshKey((key) => key + 1);
    } finally {
      setCreatingPiBatch(false);
    }
  };

  // ── Export ─────────────────────────────────────────────────────────

  const [mergeExport, setMergeExport] = useState(false);
  const [showPiExportOptions, setShowPiExportOptions] = useState(false);
  const [showInvoiceExport, setShowInvoiceExport] = useState(false);
  const [piExportFreightEur, setPiExportFreightEur] = useState("");
  const [piExportInsuranceEur, setPiExportInsuranceEur] = useState("");
  const [piExportDomesticFreightEur, setPiExportDomesticFreightEur] = useState("");
  const [piExportDomesticInsuranceEur, setPiExportDomesticInsuranceEur] = useState("");

  const buildExportOptions = () => ({
    brand: brandFilter || undefined,
    model: modelFilter || undefined,
    powertrain: powertrainFilter || undefined,
    version: versionFilter || undefined,
    colour: colourFilter || undefined,
    materialCodeSearch: materialSearch || undefined,
    selectedMonth: selectedMonth ?? undefined,
    selectionDate: selectionDate || undefined,
    hideEmptyRows,
  });

  const exportMonthSuffix = () => selectedMonth ? `_M${String(selectedMonth).padStart(2, "0")}` : "";

  const optionalExportNumber = (value: string): number | undefined => {
    const trimmed = value.trim();
    if (!trimmed) return undefined;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : undefined;
  };

  const handleExport = async () => {
    const exportOptions = {
      ...buildExportOptions(),
      quantitiesOnly: hideEmptyRows,
    };
    const monthSuffix = exportMonthSuffix();
    try {
      if (selectedCountries.length > 1 && mergeExport) {
        // Merged export: download one file per country with multi-country columns
        for (const country of selectedCountries) {
          const blob = await api.exportOrderGenius(country, selectedYear, exportOptions);
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `Order_Genius_${country}-${selectedYear}${monthSuffix}_filtered.xlsx`;
          a.click();
          URL.revokeObjectURL(url);
          if (selectedCountries.length > 1) await new Promise((r) => setTimeout(r, 300));
        }
      } else {
        for (const country of selectedCountries) {
          const blob = await api.exportOrderGenius(country, selectedYear, exportOptions);
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `Order_Genius_${country}-${selectedYear}${monthSuffix}_filtered.xlsx`;
          a.click();
          URL.revokeObjectURL(url);
          if (selectedCountries.length > 1) await new Promise((r) => setTimeout(r, 300));
        }
      }
    } catch (err) {
      setError(`Export failed: ${getErrorMessage(err)}`);
    }
  };

  const handlePiExport = async () => {
    const freightEur = optionalExportNumber(piExportFreightEur);
    const insuranceEur = optionalExportNumber(piExportInsuranceEur);
    const domesticFreightEur = optionalExportNumber(piExportDomesticFreightEur);
    const domesticInsuranceEur = optionalExportNumber(piExportDomesticInsuranceEur);
    if (piExportFreightEur.trim() && freightEur === undefined) {
      setError("PI 单车运费必须是非负数字");
      return;
    }
    if (piExportInsuranceEur.trim() && insuranceEur === undefined) {
      setError("PI 单车保费必须是非负数字");
      return;
    }
    if (piExportDomesticFreightEur.trim() && domesticFreightEur === undefined) {
      setError("PI 一次内销单车运费必须是非负数字");
      return;
    }
    if (piExportDomesticInsuranceEur.trim() && domesticInsuranceEur === undefined) {
      setError("PI 一次内销单车保费必须是非负数字");
      return;
    }
    const exportOptions = {
      ...buildExportOptions(),
      freightEur,
      insuranceEur,
      domesticFreightEur,
      domesticInsuranceEur,
    };
    const monthSuffix = exportMonthSuffix();
    try {
      for (const country of selectedCountries) {
        const blob = await api.exportOrderGeniusPi(country, selectedYear, exportOptions);
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `PI_${country}-${selectedYear}${monthSuffix}_filtered.xlsx`;
        a.click();
        URL.revokeObjectURL(url);
        if (selectedCountries.length > 1) await new Promise((r) => setTimeout(r, 300));
      }
    } catch (err) {
      setError(`PI export failed: ${getErrorMessage(err)}`);
    }
  };

  // ── Derived ────────────────────────────────────────────────────────

  const selectedPaymentTerm = useMemo(
    () =>
      countries.find((c) => c.countryCode === primaryCountry)
        ?.paymentTermCode ?? null,
    [countries, primaryCountry],
  );
  const countryNameByCode = useMemo(() => {
    const map = new Map<string, string>();
    for (const country of countries) {
      map.set(country.countryCode, country.countryName);
    }
    return map;
  }, [countries]);
  const missingFobCountryCodes = useMemo(() => {
    if (!canFillOrders || fobCountryCodes === null) return [];
    const fobSet = new Set(fobCountryCodes);
    return selectedCountries.filter((countryCode) => !fobSet.has(countryCode));
  }, [canFillOrders, fobCountryCodes, selectedCountries]);
  const missingFobCountryLabels = missingFobCountryCodes.map((countryCode) =>
    formatOrderGeniusCountryOptionLabel(countryCode, countryNameByCode.get(countryCode)),
  );
  const openBomAdminPanel = () => {
    if (!canMaintainBom) return;
    const targetCountry = missingFobCountryCodes[0] ?? null;
    setBomAdminCopyTargetCountry(targetCountry);
    setShowDeck(true);
    setControlTab("bom");
    setShowPtAdmin(false);
    setShowBomAdmin(true);
  };
  const openBomAdminForMissingFob = () => {
    openBomAdminPanel();
  };
  const removeMissingFobCountries = () => {
    const missing = new Set(missingFobCountryCodes);
    setSelectedCountries((current) => {
      const next = current.filter((countryCode) => !missing.has(countryCode));
      return next.length > 0 ? next : current;
    });
  };
  const selectedMonthLabel = selectedMonth ? MONTHS[selectedMonth - 1] : "All months";
  const activeFilterSummary = [
    selectedCountries.length === 1 ? selectedCountries[0] : `${selectedCountries.length} countries`,
    String(selectedYear),
    selectedMonthLabel,
    brandFilter || "All brands",
    modelFilter || "All models",
    powertrainFilter || "All powertrains",
  ].join(" · ");
  const orderGeniusControlTabs: Array<{ id: OrderGeniusControlTab; label: string; meta: string }> = [
    { id: "filters", label: ORDER_GENIUS_CONTROL_TAB_LABELS.filters, meta: activeFilterSummary },
    {
      id: "bom",
      label: "BOM Admin",
      meta: `${showBomAdmin ? "BOM open" : "BOM closed"} · ${showPtAdmin ? "PT open" : selectedPaymentTerm || "Payment terms"}`,
    },
    {
      id: "exports",
      label: ORDER_GENIUS_CONTROL_TAB_LABELS.exports,
      meta: `${combinedMatrix.totalRows} rows · ${showUpload || showQtyImport ? "panel open" : "ready"}`,
    },
    {
      id: "pi",
      label: "PI Batch",
      meta: selectedMonth ? `${selectedPiRows.length} rows · ${selectedPiQuantityTotal} units` : "Select month",
    },
  ];

  return (
    <section className="crud-shell">
      <OrderingBrandNotice user={user} />
      <header className="crud-hero">
        <h1>Order Genius</h1>
        <p>
          Country order matrix with FOB pricing, monthly quantity editing, and
          Excel export.
        </p>
      </header>
      <div className="order-genius-summary-strip" aria-label="Current Order Genius filters">
        <span>{selectedCountries.length === 1 ? selectedCountries[0] : `${selectedCountries.length} countries`}</span>
        <span>{selectedYear}</span>
        <span>{selectedMonthLabel}</span>
        <span>{brandFilter || "All brands"}</span>
        <span>{modelFilter || "All models"}</span>
        <span>{powertrainFilter || "All powertrains"}</span>
        {selectedMonth != null ? (
          <div className={`og-pi-readiness ${quantityError || piPlanError || error ? "is-error" : piReady ? "is-ready" : "is-pending"}`} role="status" aria-live="polite">
            {piReadinessText}
            {quantityError || piPlanError || error ? <button type="button" onClick={retryPiAvailability}>Retry</button> : null}
          </div>
        ) : null}
      </div>
      {authFailureNotice ? (
        <div
          role="alert"
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 12,
            flexWrap: "wrap",
            margin: "10px 0 14px",
            padding: "10px 12px",
            border: `1px solid ${authFailureNotice.requiresLogin ? "#fbbf24" : "#bfdbfe"}`,
            background: authFailureNotice.requiresLogin ? "#fffbeb" : "#eff6ff",
            color: authFailureNotice.requiresLogin ? "#92400e" : "#1e3a8a",
            fontSize: 12,
            fontWeight: 700,
          }}
        >
          <span>{authFailureNotice.message}</span>
          <span style={{ display: "inline-flex", gap: 8 }}>
            {authFailureNotice.requiresLogin ? (
              <button type="button" className="btn btn-sm btn-primary" onClick={reLoginAfterAuthFailure}>重新登录</button>
            ) : null}
            <button type="button" className="btn btn-sm btn-ghost" onClick={() => setAuthFailureNotice(null)}>知道了</button>
          </span>
        </div>
      ) : null}

      <DeckFloatingDrawer
        open={showDeck}
        onOpenChange={setShowDeck}
        triggerPrimary="Filters & Actions"
        triggerSecondaryOpen={ORDER_GENIUS_CONTROL_TAB_LABELS[controlTab]}
        triggerSecondaryClosed={activeFilterSummary}
        eyebrow="Order Genius"
        title="Filters & Actions"
        closeLabel="Close"
        ariaLabel="Order Genius controls"
        className="order-genius-control-drawer"
        panelClassName="order-genius-control-panel"
        bodyClassName="order-genius-control-panel-body"
      >
      {error ? (
        <div className="alert alert-error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}
      {matrixConflictNotice.length > 0 ? (
        <div
          className="alert"
          role="status"
          style={{ marginBottom: 16, color: "#92400e", background: "#fffbeb", borderColor: "#fbbf24" }}
        >
          {matrixConflictNotice.length} 个物料／国家存在待确认 FOB 基准；正常行仍可查看。请在 BOM Admin 确认模板＋国家 Single 基准后再重算。
        </div>
      ) : null}

      <div className="deck-control-tabs order-genius-control-tabs" role="tablist" aria-label="Order Genius control sections">
        {orderGeniusControlTabs.filter((tab) => tab.id !== "bom" || canMaintainBom).map((tab) => (
          <button
            key={tab.id}
            type="button"
            role="tab"
            aria-selected={controlTab === tab.id}
            className={`deck-control-tab${controlTab === tab.id ? " is-active" : ""}`}
            onClick={() => {
              if (tab.id === "bom") {
                openBomAdminPanel();
                return;
              }
              setControlTab(tab.id);
            }}
          >
            <span>{tab.label}</span>
            <small>{tab.meta}</small>
          </button>
        ))}
      </div>

      {/* ── Filter bar ─────────────────────────────────────────────── */}
      {controlTab === "filters" ? (
      <div className="order-genius-control-section">
      <div className="order-genius-filter-grid">
        <div className="og-month-controls">
          <label>Order month
            <select aria-label="Order month" value={selectedMonth ?? ""} onChange={(event) => {
              setSelectedMonth(event.target.value ? Number(event.target.value) : null);
              setSelectionDate("");
            }}>
              <option value="">All months</option>
              {MONTHS.map((month, index) => <option key={month} value={index + 1}>{month}</option>)}
            </select>
          </label>
          <label>Year
            <select aria-label="Order year" value={selectedYear} onChange={(event) => { setSelectedYear(Number(event.target.value)); setSelectionDate(""); }}>
              {[selectedYear - 1, selectedYear, selectedYear + 1].map((year) => <option key={year} value={year}>{year}</option>)}
            </select>
          </label>
        </div>
        <label style={{ cursor: "pointer", fontSize: 12, color: "#475569", display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={groupByProduct} onChange={(e) => setGroupByProduct(e.target.checked)} />
          Group by product
        </label>
        <label style={{ cursor: "pointer", fontSize: 12, color: "#64748b", display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={hideEmptyRows} onChange={(e) => setHideEmptyRows(e.target.checked)} />
          Hide empty rows
        </label>
        <div className="market-scan-field version-comparison-model-picker-field" ref={countryPickerRef} style={{ minWidth: 200 }}>
          <span>Countries{selectedCountries.length > 0 ? ` (${selectedCountries.length})` : ""}</span>
          <div className="version-comparison-model-picker">
            <div className="version-comparison-model-picker-input-row">
              <input type="text" className="version-comparison-model-search"
                placeholder={`${selectedCountries.length} ${selectedCountries.length === 1 ? "country" : "countries"} selected — Search…`}
                value={countrySearchQuery}
                onChange={(e) => { setCountrySearchQuery(e.target.value); setCountryPickerOpen(true); }}
                onFocus={() => { setCountrySearchQuery(""); setCountryPickerOpen(true); }}
                disabled={countries.length === 0} />
            </div>
            {countryPickerOpen && searchedCountryOptions.length > 0 ? (
              <div className="version-comparison-model-dropdown">
                <div className="version-comparison-model-dropdown-actions">
                  <button type="button" className="version-comparison-batch-btn"
                    onClick={() => setSelectedCountries(searchedCountryOptions.map((o) => o.value))}>Select all</button>
                  <button type="button" className="version-comparison-batch-btn"
                    onClick={() => { const vals = new Set(searchedCountryOptions.map((o) => o.value)); setSelectedCountries((prev) => prev.filter((c) => !vals.has(c))); }}>Deselect shown</button>
                  <button type="button" className="version-comparison-batch-btn"
                    onClick={() => setSelectedCountries([])}>Clear</button>
                  <span className="version-comparison-dropdown-count">{searchedCountryOptions.length} options · {selectedCountries.length} selected</span>
                </div>
                {searchedCountryOptions.slice(0, 30).map((opt) => {
                  const active = selectedCountries.includes(opt.value);
                  return (
                    <button key={opt.value} type="button"
                      className={`version-comparison-model-option${active ? " is-selected" : ""}`}
                      onClick={() => {
                        setSelectedCountries((prev) => {
                          if (active) { const next = prev.filter((x) => x !== opt.value); return next.length > 0 ? next : prev; }
                          return [...prev, opt.value];
                        });
                        setBrandFilter(""); setModelFilter(""); setPowertrainFilter(""); setVersionFilter(""); setColourFilter("");
                      }}>
                      <span className={`version-comparison-model-checkbox${active ? " is-checked" : ""}`}>{active ? "✓" : ""}</span>
                      <span className="version-comparison-model-option-name">{opt.label}</span>
                    </button>
                  );
                })}
              </div>
            ) : null}
            {countryPickerOpen && searchedCountryOptions.length === 0 && countrySearchQuery.trim() ? (
              <div className="version-comparison-model-dropdown"><div className="version-comparison-model-empty">无匹配国家</div></div>
            ) : null}
          </div>
        </div>

        <details className="og-more-filters">
          <summary>More filters</summary>
          <div className="order-genius-filter-grid">
        <label
          className="order-genius-historical-toggle"
          title={selectedOrderMonthIsFuture
            ? "Historical materials cannot be used for a future month; extend the BOM template end date first."
            : "Explicitly show Historical materials for current or past order backfill."}
        >
          <input
            type="checkbox"
            checked={includeHistorical}
            disabled={selectedMonth == null || selectedOrderMonthIsFuture}
            onChange={(event) => setIncludeHistorical(event.currentTarget.checked)}
          />
          Include historical materials
        </label>

        <label className="order-genius-date-filter">
          <span>Price as of</span>
          <input
            type="date"
            value={selectionDate}
            onChange={(event) => setSelectionDate(event.target.value)}
          />
        </label>

        {options?.brands ? (
          <select
            value={brandFilter}
            onChange={(e) => { setBrandFilter(e.target.value); setModelFilter(""); setPowertrainFilter(""); setVersionFilter(""); setColourFilter(""); }}
          >
            <option value="">All Brands</option>
            {options.brands.map((b) => (<option key={b} value={b}>{b}</option>))}
          </select>
        ) : null}

        {options?.models ? (
          <select value={modelFilter} onChange={(e) => { setModelFilter(e.target.value); setPowertrainFilter(""); setVersionFilter(""); setColourFilter(""); }}>
            <option value="">All Models</option>
            {options.models.map((m) => (<option key={m} value={m}>{m}</option>))}
          </select>
        ) : null}

        {options?.powertrains ? (
          <select value={powertrainFilter} onChange={(e) => { setPowertrainFilter(e.target.value); setVersionFilter(""); setColourFilter(""); }}>
            <option value="">All Powertrains</option>
            {options.powertrains.map((p) => (<option key={p} value={p}>{p}</option>))}
          </select>
        ) : null}

        {options?.versions ? (
          <select value={versionFilter} onChange={(e) => { setVersionFilter(e.target.value); setColourFilter(""); }}>
            <option value="">All Versions</option>
            {options.versions.map((v) => (<option key={v} value={v}>{v}</option>))}
          </select>
        ) : null}

        {options?.colours ? (
          <select value={colourFilter} onChange={(e) => setColourFilter(e.target.value)}>
            <option value="">All Colours</option>
            {options.colours.map((c) => (<option key={c} value={c}>{c}</option>))}
          </select>
        ) : null}

        <input
          type="text"
          list="material-suggestions"
          placeholder="Material code..."
          value={materialSearch}
          onChange={(e) => setMaterialSearch(e.target.value)}
          style={{ minWidth: 160 }}
        />
        <datalist id="material-suggestions">
          {materialSuggestions.map((suggestion) => (
            <option key={suggestion.materialCode} value={suggestion.materialCode}>
              {suggestion.remark ? `${suggestion.materialCode} (${suggestion.remark})` : suggestion.materialCode}
            </option>
          ))}
        </datalist>

        {selectedCountries.length > 1 && (
          <label style={{ cursor: "pointer", fontSize: 12, color: "#0f766e", display: "flex", alignItems: "center", gap: 4 }}>
            <input type="checkbox" checked={consolidatedView} onChange={(e) => setConsolidatedView(e.target.checked)} />
            Consolidated
          </label>
        )}

          </div>
        </details>
        <button type="button" className="btn btn-sm btn-primary order-genius-refresh-button" onClick={loadMatrices}>
          Refresh
        </button>
      </div>
      </div>
      ) : null}

      {controlTab === "bom" ? (
      <div className="order-genius-control-section">
      <div className="order-genius-action-grid">
        {isAdmin && (
          <button type="button" className="btn btn-sm btn-ghost"
                  onClick={() => setShowPtAdmin(!showPtAdmin)}
                  style={showPtAdmin ? { background: "#0f766e", color: "#fff" } : undefined}>
            {showPtAdmin ? "Hide PT Admin" : "Payment Terms"}
          </button>
        )}
        {canMaintainBom && (
          <button type="button" className="btn btn-sm btn-ghost"
                  onClick={() => {
                    if (showBomAdmin) {
                      setShowBomAdmin(false);
                      return;
                    }
                    openBomAdminPanel();
                  }}
                  style={showBomAdmin ? { background: "#b45309", color: "#fff" } : undefined}>
            {showBomAdmin ? "Hide BOM Admin" : "BOM Admin"}
          </button>
        )}
        {!canMaintainBom ? (
          <div className="order-genius-muted-note">Admin tools are available to admin users only.</div>
        ) : null}
        {canFillOrders && user?.role !== "order_filler" ? (
          <a className="btn btn-sm btn-ghost" href="/product/order-genius/cbu">CBU Finance</a>
        ) : null}
      </div>
      </div>
      ) : null}

      {controlTab === "exports" ? (
      <div className="order-genius-control-section">
      <div className="order-genius-action-grid">
        <button type="button" className="btn btn-sm btn-ghost" onClick={handleExport}
                disabled={combinedMatrix.totalRows === 0}>
          Export XLSX
        </button>
        <button type="button" className="btn btn-sm btn-ghost" onClick={() => setShowInvoiceExport(true)}
                disabled={selectedCountries.length === 0}>
          Export PI
        </button>
        <button type="button" className="btn btn-sm btn-ghost" onClick={() => setShowPiExportOptions((open) => !open)}
                disabled={combinedMatrix.totalRows === 0}>
          Export PI data
        </button>
        {canFillOrders && (
          <button type="button" className="btn btn-sm btn-ghost"
                  onClick={() => { setShowQtyImport(true); setQtyImportFile(null); setQtyImportPreview(null); setQtyImportResult(null); }}>
            Import Quantities
          </button>
        )}
        {isAdmin && (
          <button type="button" className="btn btn-sm btn-ghost"
                  onClick={() => setShowUpload(!showUpload)}>
            {showUpload ? "Hide Upload" : "Upload Material Master"}
          </button>
        )}
      </div>
      {showPiExportOptions ? (
        <div className="og-pi-export-options">
          <div className="og-pi-export-options-copy">
            <strong>Internal PI data export</strong>
            <span>四个费用字段选填；一次内销单价自动使用 NL 价格，没有则为空。</span>
          </div>
          <label>
            单车运费
            <input
              type="number"
              min="0"
              step="1"
              value={piExportFreightEur}
              onChange={(event) => setPiExportFreightEur(event.target.value)}
              placeholder="blank"
            />
          </label>
          <label>
            单车保费
            <input
              type="number"
              min="0"
              step="1"
              value={piExportInsuranceEur}
              onChange={(event) => setPiExportInsuranceEur(event.target.value)}
              placeholder="blank"
            />
          </label>
          <label>
            一次内销单车运费
            <input
              type="number"
              min="0"
              step="1"
              value={piExportDomesticFreightEur}
              onChange={(event) => setPiExportDomesticFreightEur(event.target.value)}
              placeholder="blank"
            />
          </label>
          <label>
            一次内销单车保费
            <input
              type="number"
              min="0"
              step="1"
              value={piExportDomesticInsuranceEur}
              onChange={(event) => setPiExportDomesticInsuranceEur(event.target.value)}
              placeholder="blank"
            />
          </label>
          <button type="button" className="btn btn-sm btn-primary" onClick={handlePiExport}>
            Download PI data
          </button>
        </div>
      ) : null}
      </div>
      ) : null}

      {controlTab === "pi" ? (
        <div className="og-pi-batch-panel">
          <div className="og-pi-batch-head">
            <strong>PI Batch</strong>
            <span title="Select one month, tick PI rows, then create PI from the selected order quantities">
              {selectedMonth ? `${selectedPiRows.length} rows · ${selectedPiQuantityTotal} units · ${piBatchScopeSummary}` : "Select month"}
            </span>
            <label
              className="og-pi-batch-select-all"
              title={selectablePiRows.length > 0 ? "Select every visible row with a positive quantity for the selected month" : "No selectable PI rows"}
            >
              <input
                type="checkbox"
                checked={allSelectablePiRowsSelected}
                disabled={!piReady || selectablePiRows.length === 0}
                onChange={(event) => toggleAllPiBatchRows(event.currentTarget.checked)}
              />
              {partialSelectablePiRowsSelected
                ? `Selected ${selectedPiRows.length}/${selectablePiRows.length}`
                : "Select all"}
            </label>
          </div>
          {selectedMonth != null ? <div role="status" className="og-pi-panel-readiness">{piReadinessText}</div> : null}
          {selectedMonth ? (
            <div className="og-pi-batch-summary" aria-label="PI month allocation summary">
              <span><small>Month total</small><strong>{!piReady ? "—" : piPlanTotals.selectedQuantity}</strong></span>
              <span><small>Already in PI</small><strong>{!piReady ? "—" : piPlanTotals.generatedQuantity}</strong></span>
              <span><small>Waiting for next batch</small><strong>{!piReady ? "—" : piPlanTotals.remainingQuantity}</strong></span>
              <span className={piPlanTotals.overGeneratedQuantity > 0 ? "is-warning" : ""}>
                <small>Over allocated</small><strong>{!piReady ? "—" : piPlanTotals.overGeneratedQuantity}</strong>
              </span>
            </div>
          ) : null}
          {piPlanError ? (
            <div className="alert alert-error">
              PI batch history failed to load: {piPlanError}
              <button type="button" className="btn btn-sm btn-ghost" onClick={() => setPiBatchRefreshKey((key) => key + 1)}>
                Retry
              </button>
            </div>
          ) : null}
          {includeHistorical ? (
            <div className="og-historical-backfill-warning" role="note">
              <strong>历史物料补录 / Historical material backfill</strong>
              <span>
                你正在为历史物料创建 PI；这不会重新启用该物料。<br />
                You are creating a PI for a historical material. This will not reactivate the material.
              </span>
              {canMaintainBom ? <button type="button" className="btn btn-sm btn-ghost" onClick={openBomAdminPanel}>
                Open BOM Admin
              </button> : null}
            </div>
          ) : null}
          {selectedOrderMonthIsFuture ? (
            <div className="og-historical-backfill-warning" role="alert">
              <strong>Future Historical use is blocked / 未来月份不能使用 Historical</strong>
              <span>
                请先在 BOM Admin 延长模板最终截止日期。<br />
                Extend the BOM template final order date before creating this future PI.
              </span>
              {canMaintainBom ? <button type="button" className="btn btn-sm btn-ghost" onClick={openBomAdminPanel}>
                Open BOM Admin
              </button> : null}
            </div>
          ) : null}
          {selectedMonth && piExistingBatches.length > 0 ? (
            <div className="og-pi-existing-batches" aria-label="Existing PI batches">
              <strong>Existing batches</strong>
              {piExistingBatches.map((batch) => (
                <a key={batch.piCode} href={vehicleAllocationUrl(batch.piCode)}>
                  {batch.piCode}
                  <small>{batch.marketCountryCodes.join("/") || batch.countryCode}</small>
                </a>
              ))}
            </div>
          ) : null}
          <div className="og-pi-batch-mode" role="group" aria-label="PI batch scope">
            <button
              type="button"
              className={piBatchMode === "by_country" ? "is-active" : ""}
              aria-pressed={piBatchMode === "by_country"}
              title="Create one PI per market country. Use this when each country orders separately."
              onClick={() => setPiBatchMode("by_country")}
            >
              By country
            </button>
            <button
              type="button"
              className={piBatchMode === "by_account" ? "is-active" : ""}
              aria-pressed={piBatchMode === "by_account"}
              title="Create one PI for a shared ordering account, while each car still keeps its market country."
              onClick={() => setPiBatchMode("by_account")}
            >
              Ordering account
            </button>
          </div>
          <div className="og-pi-batch-fields">
            <input
              value={piBatchForm.officialPiNo}
              onChange={(event) => setPiBatchForm((current) => ({ ...current, officialPiNo: event.target.value }))}
              placeholder="Official PI"
              title="Supplier's official PI number. Can be filled later if not available now."
            />
            <label className="og-pi-order-date">Order date<input
              type="date"
              value={piBatchForm.orderDate}
              onChange={(event) => setPiBatchForm((current) => ({ ...current, orderDate: event.target.value }))}
              title="PI order date"
            /></label>
            <input
              value={piBatchForm.shipName}
              onChange={(event) => setPiBatchForm((current) => ({ ...current, shipName: event.target.value }))}
              placeholder="Ship"
              title="Ship name for this PI batch. This can also be updated in PI vehicle allocation later."
            />
            <input
              type="date"
              value={piBatchForm.eta}
              onChange={(event) => setPiBatchForm((current) => ({ ...current, eta: event.target.value }))}
              title="ETA, expected arrival date at port"
            />
            {piBatchMode === "by_account" ? (
              <>
                <input
                  value={piBatchForm.orderingAccountCode}
                  onChange={(event) => {
                    setOrderingAccountCodeEdited(true);
                    setPiBatchForm((current) => ({
                      ...current,
                      orderingAccountCode: normalizeAccountCode(event.target.value),
                    }));
                  }}
                  placeholder="Account code"
                  title="Ordering account for the PI code, for example NORDIC when Sweden and Finland share one distributor."
                />
                <input
                  value={piBatchForm.orderingAccountName}
                  onChange={(event) => setPiBatchForm((current) => ({ ...current, orderingAccountName: event.target.value }))}
                  placeholder="Account name"
                  title="Readable distributor or ordering account name"
                />
                <input
                  value={piBatchForm.portOfDischarge}
                  onChange={(event) => setPiBatchForm((current) => ({ ...current, portOfDischarge: event.target.value }))}
                  placeholder="Port"
                  title="Destination port for the shared shipment, for example Zeebrugge"
                />
                <input
                  value={piBatchForm.shipmentBatchCode}
                  onChange={(event) => setPiBatchForm((current) => ({ ...current, shipmentBatchCode: event.target.value }))}
                  placeholder="Batch"
                  title="Optional internal shipment batch code for later tracking"
                />
              </>
            ) : null}
          </div>
          {selectedPiRows.length > 0 ? (
            <div className="og-pi-batch-lines">
              {selectedPiRows.map((row) => {
                const rowId = getOrderGeniusRowId(row);
                const monthQuantity = selectedMonth == null ? 0 : row[`month_${selectedMonth}`] || 0;
                const planLine = piPlanLinesByCountryMaterial.get(`${row._countryCode || primaryCountry}|${row.materialCode}`);
                const existingPiCodes = piCodesByCountryMaterial.get(`${row._countryCode || primaryCountry}|${row.materialCode}`) ?? [];
                const generatedQuantity = planLine?.generatedQuantity ?? 0;
                const remainingQuantity = planLine?.remainingQuantity ?? 0;
                return (
                  <label
                    key={rowId}
                    title="This quantity will be linked back to the selected market country in PI allocation."
                  >
                    <span>
                      {row._countryCode ? `${row._countryCode} · ` : ""}{row.materialCode}
                      <small>{row.modelName} / {row.version} / {row.colour}</small>
                      <small>Total {monthQuantity} · in PI {generatedQuantity} · remaining {remainingQuantity}</small>
                      {row.historicalSurchargeReview ? (
                        <small className={row.historicalSurchargeReview.requiresConfirmation ? "is-warning" : ""}>
                          Historical surcharge: saved {row.historicalSurchargeReview.savedSurchargeEur ?? "unknown"}
                          {" · "}current {row.historicalSurchargeReview.currentSurchargeEur ?? "missing"}
                        </small>
                      ) : null}
                      {existingPiCodes.length > 0 ? (
                        <small className="og-pi-line-batches">
                          Batches {existingPiCodes.map((piCode) => (
                            <a key={piCode} href={vehicleAllocationUrl(piCode)} onClick={(event) => event.stopPropagation()}>{piCode}</a>
                          ))}
                        </small>
                      ) : null}
                    </span>
                    <input
                      type="number"
                      min={0}
                      max={remainingQuantity}
                      value={piBatchQuantities[rowId] ?? remainingQuantity}
                      onChange={(event) => updatePiBatchQuantity(row, Number(event.target.value))}
                      title={`Quantity for the next PI batch. Remaining ${remainingQuantity} of ${monthQuantity}.`}
                    />
                    {row.historicalBackfill ? (
                      <span className="og-historical-price-override">
                        <input
                          type="number"
                          min={0}
                          step="1"
                          value={historicalFobOverrides[rowId] ?? ""}
                          onChange={(event) => setHistoricalFobOverrides((current) => ({
                            ...current,
                            [rowId]: event.currentTarget.value,
                          }))}
                          placeholder={`FOB override (${row.fobEur ?? "none"})`}
                          title="Optional final FOB override for this Historical PI only"
                        />
                        <input
                          value={historicalPriceReasons[rowId] ?? ""}
                          onChange={(event) => setHistoricalPriceReasons((current) => ({
                            ...current,
                            [rowId]: event.currentTarget.value,
                          }))}
                          placeholder="Override reason"
                          title="Required only when a Historical FOB override is entered"
                        />
                      </span>
                    ) : null}
                  </label>
                );
              })}
            </div>
          ) : null}
          {selectedHistoricalPiRows.length > 0 ? (
            <div className="og-historical-backfill-confirmations">
              <label>
                <input
                  type="checkbox"
                  checked={confirmHistoricalPi}
                  onChange={(event) => setConfirmHistoricalPi(event.currentTarget.checked)}
                />
                Confirm Historical PI backfill; material stays Historical / 确认历史补录，物料保持 Historical
              </label>
              {selectedHistoricalNeedsUndatedConfirmation ? (
                <label>
                  <input
                    type="checkbox"
                    checked={confirmUndatedHistoricalFob}
                    onChange={(event) => setConfirmUndatedHistoricalFob(event.currentTarget.checked)}
                  />
                  Use the displayed undated default FOB / 使用当前展示的无日期基准价
                </label>
              ) : null}
              {selectedHistoricalNeedsSurchargeConfirmation ? (
                <label>
                  <input
                    type="checkbox"
                    checked={confirmHistoricalSurcharge}
                    onChange={(event) => setConfirmHistoricalSurcharge(event.currentTarget.checked)}
                  />
                  Confirm saved versus current colour surcharge / 确认历史与当前颜色加价差异
                </label>
              ) : null}
            </div>
          ) : null}
          <div className="og-pi-batch-actions">
            <button
              type="button"
              className="btn btn-sm btn-primary"
              disabled={creatingPiBatch || !piReady || selectedPiRows.length === 0}
              onClick={() => void handleCreatePiBatch()}
              title={piBatchMode === "by_account" ? "Create one PI for the ordering account and keep country allocations on each line" : "Create one PI per selected country"}
            >
              {creatingPiBatch ? "Creating…" : "Create PI"}
            </button>
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              disabled={selectedPiRows.length === 0}
              onClick={clearPiBatchSelection}
            >
              Clear
            </button>
          </div>
          {piBatchError ? (
            <div role="alert" className="alert alert-error">{piBatchError}</div>
          ) : null}
          {piBatchNotice ? (
            <div role="status" className="og-pi-batch-notice">
              <span>{piBatchNotice}</span>
              {piBatchCreatedCodes.length > 0 ? (
                <span className="og-pi-batch-links">
                  {piBatchCreatedCodes.map((piCode) => (
                    <a key={piCode} href={vehicleAllocationUrl(piCode)}>
                      Open {piCode}
                    </a>
                  ))}
                </span>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}

      {/* ── Quantity Import Modal ────────────────────────────────── */}
      {controlTab === "exports" && showQtyImport ? (
        <div className="card crud-card" style={{ padding: 16, marginBottom: 16 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
            <h3 style={{ margin: 0 }}>Import Order Quantities</h3>
            <button type="button" className="btn btn-sm btn-ghost" onClick={closeQtyImport}>Close</button>
          </div>
          {!qtyImportPreview ? (
            <div>
              <p style={{ fontSize: 13, color: "#64748b", marginBottom: 12 }}>
                Upload an exported Order Genius XLSX file with edited quantities.
                The system will match by (Country, Year, Month, Material Code) and show a diff preview before applying changes.
              </p>
              <input ref={qtyImportInputRef} type="file" accept=".xlsx" onChange={handleQtyImportFile} className="monthly-update-file-input" />
              <div
                className={`monthly-update-dropzone${qtyImportDragActive ? " is-dragging" : ""}${qtyImportFile ? " has-file" : ""}`}
                role="button" tabIndex={0}
                onClick={() => qtyImportInputRef.current?.click()}
                onKeyDown={handleQtyImportDropzoneKeyboard}
                onDragEnter={handleQtyImportDragState}
                onDragOver={handleQtyImportDragState}
                onDragLeave={handleQtyImportDragState}
                onDrop={handleQtyImportDrop}
              >
                <strong>{qtyImportFile ? qtyImportFile.name : "拖拽 Order Quantity Excel 到这里，或点击选择文件"}</strong>
                <span>{qtyImportFile ? `${(qtyImportFile.size / 1024).toFixed(1)} KB · 上传后会解析并显示差异预览。` : "支持 .xlsx；导出的 Order Genius 文件可直接回传。"}</span>
              </div>
              {qtyImportLoading ? <div style={{ fontSize: 13, color: "#64748b", marginTop: 8 }}>Parsing file...</div> : null}
            </div>
          ) : qtyImportResult ? (
            <div style={{ background: "#f0fdf4", border: "1px solid #86efac", padding: 12, borderRadius: 6 }}>
              <strong>Import Applied</strong>
              <p style={{ fontSize: 13, margin: "4px 0" }}>{qtyImportResult.appliedCells} cells updated{qtyImportResult.skippedCells > 0 ? `, ${qtyImportResult.skippedCells} skipped` : ""}</p>
              {qtyImportResult.errors.length > 0 ? (
                <div style={{ fontSize: 12, color: "#dc2626", maxHeight: 120, overflow: "auto" }}>
                  {qtyImportResult.errors.slice(0, 10).map((e, i) => (<div key={i}>{e}</div>))}
                </div>
              ) : null}
              <button type="button" className="btn btn-sm btn-primary" onClick={closeQtyImport} style={{ marginTop: 8 }}>Done</button>
            </div>
          ) : (
            <div>
              <div style={{ display: "flex", gap: 16, marginBottom: 12, fontSize: 13 }}>
                <span>Country: <strong>{qtyImportPreview.countryCode}</strong></span>
                <span>Year: <strong>{qtyImportPreview.year}</strong></span>
                <span>Total cells: <strong>{qtyImportPreview.totalCells}</strong></span>
                {qtyImportPreview.errorCells > 0 ? <span style={{ color: "#dc2626" }}>Errors: <strong>{qtyImportPreview.errorCells}</strong></span> : null}
                <span style={{ color: qtyImportPreview.newRows.length > 0 ? "#d97706" : "#16a34a" }}>
                  Matched: <strong>{qtyImportPreview.matchedRows.length}</strong>
                  {qtyImportPreview.newRows.length > 0 ? ` · New: ${qtyImportPreview.newRows.length}` : ""}
                </span>
              </div>
              {qtyImportPreview.fobChanges.length > 0 ? (
                <div style={{ background: "#fef3c7", border: "1px solid #f59e0b", padding: 8, marginBottom: 12, borderRadius: 4, fontSize: 12 }}>
                  <strong>FOB mismatch</strong> — {qtyImportPreview.fobChanges.length} codes have different FOB. System FOB will be used.
                </div>
              ) : null}
              {qtyImportPreview.errors.length > 0 ? (
                <div style={{ background: "#fef2f2", padding: 8, marginBottom: 12, borderRadius: 4, fontSize: 12 }}>
                  {qtyImportPreview.errors.map((e, i) => (<div key={i} style={{ color: "#dc2626" }}>{e}</div>))}
                </div>
              ) : null}
              <div style={{ maxHeight: 360, overflow: "auto", marginBottom: 12 }}>
                <table className="data-table" style={{ fontSize: 11 }}>
                  <thead><tr><th>Material</th><th>Model</th><th>Month</th><th>Old</th><th>New</th><th>Status</th></tr></thead>
                  <tbody>
                    {qtyImportPreview.matchedRows.flatMap((row) =>
                      row.cells.map((cell) => (
                        <tr key={`${row.materialCode}_${cell.month}`} style={cell.error ? { background: "#fef2f2" } : undefined}>
                          <td style={{ fontFamily: "monospace" }}>{row.materialCode}</td>
                          <td>{row.modelName}</td>
                          <td style={{ textAlign: "center" }}>{cell.month}</td>
                          <td style={{ textAlign: "center" }}>{cell.oldQuantity ?? "-"}</td>
                          <td style={{ textAlign: "center", fontWeight: cell.oldQuantity !== cell.newQuantity ? 700 : undefined }}>{cell.newQuantity}</td>
                          <td style={{ fontSize: 10 }}>{cell.error ? <span style={{ color: "#dc2626" }}>{cell.error}</span> : cell.oldQuantity === cell.newQuantity ? "unchanged" : cell.oldQuantity == null ? "new" : `${cell.newQuantity - (cell.oldQuantity ?? 0) > 0 ? "+" : ""}${cell.newQuantity - (cell.oldQuantity ?? 0)}`}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
              <div style={{ display: "flex", gap: 8 }}>
                <button type="button" className="btn btn-sm btn-primary"
                  disabled={qtyImportLoading || qtyImportPreview.status === "error" || qtyImportPreview.matchedRows.length === 0}
                  onClick={handleQtyImportApply}>
                  {qtyImportLoading ? "Applying..." : `Apply ${qtyImportPreview.matchedRows.reduce((s, r) => s + r.cells.filter((c) => !c.error).length, 0)} changes`}
                </button>
                <button type="button" className="btn btn-sm btn-ghost" onClick={closeQtyImport}>Cancel</button>
              </div>
            </div>
          )}
        </div>
      ) : null}

      {/* ── Upload panel ───────────────────────────────────────────── */}
      {controlTab === "exports" && showUpload ? (
        <div className="card crud-card" style={{ padding: 16, marginBottom: 16 }}>
          <h3 style={{ marginTop: 0 }}>Material Master Upload</h3>
          <input
            ref={fileInputRef}
            type="file"
            accept=".xlsx,.xlsm,.xls"
            onChange={handleFileSelect}
            className="monthly-update-file-input"
          />
          <div
            className={`monthly-update-dropzone${uploadDragActive ? " is-dragging" : ""}${uploadFile ? " has-file" : ""}`}
            role="button"
            tabIndex={0}
            onClick={() => fileInputRef.current?.click()}
            onKeyDown={handleUploadDropzoneKeyboard}
            onDragEnter={handleUploadDragState}
            onDragOver={handleUploadDragState}
            onDragLeave={handleUploadDragState}
            onDrop={handleUploadDrop}
          >
            <strong>
              {uploadFile ? uploadFile.name : "拖拽 Material Master Excel 到这里，或点击选择文件"}
            </strong>
            <span>
              {uploadFile
                ? `${formatOrderGeniusFileSize(uploadFile.size)} · 上传后会分片解析并生成发布预览。`
                : "支持 .xlsx / .xlsm / .xls；适用于 OMODA&JAECOO Order Material Codes 文件。"}
            </span>
          </div>
          <div style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 12 }}>
            <button
              type="button"
              className="btn btn-sm btn-primary"
              disabled={!uploadFile || !!uploadProgress}
              onClick={handleUpload}
            >
              Upload &amp; Parse
            </button>
            <button
              type="button"
              className="btn btn-sm btn-ghost"
              onClick={clearUpload}
            >
              Clear
            </button>
          </div>
          {uploadProgress ? (
            <div style={{ marginTop: 8, fontSize: 13, color: "#64748b" }}>
              {uploadProgress}
            </div>
          ) : null}
          {uploadStatus ? (
            <div style={{ marginTop: 4, fontSize: 13, color: "#16a34a" }}>
              {uploadStatus}
            </div>
          ) : null}

          {/* Preview */}
          {uploadPreview ? (
            <div style={{ marginTop: 12 }}>
              <div style={{ display: "flex", gap: 16, marginBottom: 8, fontSize: 13 }}>
                <span>Total rows: <strong>{uploadPreview.totalRows}</strong></span>
                <span>New: <strong style={{ color: "#16a34a" }}>{uploadPreview.newSkus}</strong></span>
                <span>Existing: <strong style={{ color: "#d97706" }}>{uploadPreview.existingSkus}</strong></span>
              </div>
              {uploadPreview.warnings.length > 0 ? (
                <div style={{ maxHeight: 120, overflow: "auto", fontSize: 12, color: "#d97706", marginBottom: 8 }}>
                  {uploadPreview.warnings.slice(0, 20).map((w, i) => (
                    <div key={i}>{w}</div>
                  ))}
                </div>
              ) : null}
              <div style={{ overflowX: "auto", maxHeight: 300, marginBottom: 8 }}>
                <table className="data-table" style={{ fontSize: 12 }}>
                  <thead>
                    <tr>
                      <th>#</th><th>Sheet</th><th>Brand</th><th>Model</th>
                      <th>Version</th><th>Colour</th><th>Code</th>
                      <th>Material</th><th>FOB</th><th>Type</th>
                    </tr>
                  </thead>
                  <tbody>
                    {uploadPreview.rows.slice(0, 50).map((r) => (
                      <tr key={r.rowIndex}>
                        <td>{r.rowIndex}</td><td>{r.sheetName}</td>
                        <td>{r.brand}</td><td>{r.modelName}</td>
                        <td>{r.version}</td><td>{r.exteriorColorName}</td>
                        <td>{r.exteriorColorCode}</td>
                        <td>{r.materialCode}</td>
                        <td>{r.baseFobEur ?? "-"}</td>
                        <td>{r.exteriorColorType}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <button
                type="button"
                className="btn btn-sm btn-primary"
                disabled={publishing || !uploadSessionId}
                onClick={handlePublish}
              >
                {publishing ? "Publishing..." : "Publish Baseline"}
              </button>
              {publishResult ? (
                <span style={{ marginLeft: 12, fontSize: 13, color: "#16a34a" }}>
                  Published {publishResult.baselineName} — {publishResult.skuCount} SKUs,{" "}
                  {publishResult.fobCount} FOBs
                </span>
              ) : null}
            </div>
          ) : null}
        </div>
      ) : null}

      {/* ── Column visibility ───────────────────────────────────────── */}
      {controlTab === "filters" ? (
      <div className="order-genius-column-controls">
        {(["months","amount","ttlQty","ttlAmount","fob","materialCode","remark"] as const).map((col) => (
          <label key={col} style={{ cursor: "pointer", color: "#475569" }}>
            <input
              type="checkbox"
              checked={visibleColumns[col]}
              onChange={() => setVisibleColumns((v) => ({ ...v, [col]: !v[col] }))}
              style={{ marginRight: 4 }}
            />
            {{ months: "Months", amount: "Amount", ttlQty: "TTL Qty", ttlAmount: "TTL Amt", fob: "FOB", materialCode: "Material", remark: "Note" }[col]}
          </label>
        ))}
      </div>
      ) : null}
      </DeckFloatingDrawer>

      {showInvoiceExport ? <PiInvoiceExportDialog countries={selectedCountries}
        month={selectedMonth == null ? undefined : `${selectedYear}-${String(selectedMonth).padStart(2, "0")}`}
        onClose={() => setShowInvoiceExport(false)} /> : null}

      {missingFobCountryCodes.length > 0 ? (
        <div className="order-genius-missing-fob-alert" role="alert">
          <div>
            <strong>{missingFobCountryCodes.length} selected countries do not have BOM FOB yet.</strong>
            <p>
              {missingFobCountryLabels.join(" · ")}
            </p>
          </div>
          <div className="order-genius-missing-fob-actions">
            {canMaintainBom ? <button type="button" className="btn btn-sm btn-primary" onClick={openBomAdminForMissingFob}>
              Open BOM Admin
            </button> : <span>Contact admin to configure FOB / 请联系管理员配置 FOB</span>}
            <button type="button" className="btn btn-sm btn-ghost" onClick={removeMissingFobCountries}>
              Remove from view
            </button>
          </div>
        </div>
      ) : null}

      {/* ── Matrix grid (AG Grid) ─────────────────────────────────── */}
      {loading ? (
        <div style={{ padding: 32, textAlign: "center", color: "#64748b" }}>
          Loading...
        </div>
      ) : combinedMatrix.totalRows > 0 ? (
        <OrderGeniusGrid
          rows={displayRows}
          selectedMonth={selectedMonth}
          selectedRowIds={piSelectedRowIds}
          selectableRowIds={selectablePiRowIds}
          piSelectionSummary={piSelectionSummary}
          canEditQuantities={canFillOrders}
          visibleColumns={visibleColumns}
          showCountry={selectedCountries.length > 1}
          onCellValueChanged={handleCellValueChanged}
          onGridReady={(api) => { gridApiRef.current = api; }}
          onToggleGroup={toggleProductGroup}
          onTogglePiRow={togglePiBatchRow}
          columnWidthStorageScope={user?.username}
        />
      ) : (
        <div style={{ padding: 32, textAlign: "center", color: "#64748b" }}>
          {missingFobCountryCodes.length > 0 ? (
            <div style={{ display: "grid", gap: 12, justifyItems: "center" }}>
              <strong style={{ color: "#334155" }}>Selected country has no BOM FOB yet.</strong>
              <span>{missingFobCountryLabels.join(" · ")}</span>
              <div className="order-genius-missing-fob-actions">
                {canMaintainBom ? <button type="button" className="btn btn-sm btn-primary" onClick={openBomAdminForMissingFob}>
                  Open BOM Admin
                </button> : <span>Contact admin to configure FOB / 请联系管理员配置 FOB</span>}
                <button type="button" className="btn btn-sm btn-ghost" onClick={removeMissingFobCountries}>
                  Remove from view
                </button>
              </div>
            </div>
          ) : selectedCountries.length > 0 ? (
            !canFillOrders ? "Brand access required / 请先申请品牌权限。"
              : isAdmin ? "No data. Upload a Material Master file to get started."
                : "No authorized material data in this view / 当前视图没有授权物料。"
          ) : (
            "Select a country to view the order matrix."
          )}
        </div>
      )}

      {/* ── Payment Terms Admin ────────────────────────────────────── */}
      {showPtAdmin && <PaymentTermAdminPanel />}
      {showBomAdmin && (
        <div style={{
          position: "fixed", inset: 0, zIndex: 1000,
          display: "flex", alignItems: "flex-start", justifyContent: "center",
          padding: "3vh 2vw",
        }}>
          <div style={{
            position: "absolute", inset: 0,
            background: "rgba(15,23,42,0.35)",
          }} onClick={() => setShowBomAdmin(false)} />
          <div style={{
            position: "relative", width: "96vw", maxWidth: 1600, maxHeight: "94vh",
            overflow: "hidden", borderRadius: 0,
            background: "#fff",
            boxShadow: "0 25px 80px rgba(15,23,42,0.3)",
            WebkitOverflowScrolling: "touch",
          }}>
            <BomAdminPanel
              key={authorizationKey}
              isAdmin={isAdmin}
              initialCopyTargetCountry={bomAdminCopyTargetCountry}
              onFobCountriesChanged={loadFobCountries}
              onFobChanged={async () => {
                const [, refreshed] = await Promise.all([loadFobCountries(), loadMatrices()]);
                if (!refreshed) throw new Error("Saved, but the selection table refresh failed. Refresh to verify.");
              }}
            />
          </div>
        </div>
      )}
    </section>
  );
}

// ── Powertrain colour map ────────────────────────────────────────────────

// Powertrain family colors — must match powertrain_normalizer.py POWERTRAIN_COLORS
const PT_COLORS: Record<string, string> = {
  EV: "#16a34a", BEV: "#16a34a", HEV: "#d97706", PHEV: "#2563eb", SHS: "#2563eb",
  MHEV: "#ca8a04", ICE: "#4b5563", LPG: "#6b7280", REEV: "#0d9488", FCV: "#0891b2",
};
function ptColor(pt: string | null): string { return PT_COLORS[pt ?? ""] ?? "#9ca3af"; }

type BomCopyDraftSku = {
  sourceMaterialCode: string;
  colour: string;
  colourCode: string;
  colourType: string;
  colourTier: string;
  colourHex: string | null;
};

type BomDraftFobEntry = {
  status?: "conflict" | null;
  reason?: string | null;
  records?: Array<Record<string, unknown>>;
  baseFobEur?: number | null;
  uploadedFobEur?: number | null;
  finalFobEur?: number | null;
  paymentTermCode?: string | null;
  colourSurchargeEur?: number | null;
  fobSourceCountryCode?: string | null;
  fobSourceMode?: string | null;
  remark?: string | null;
};

type BomFobPatch = {
  materialCode: string;
  countryCode: string;
  baseFobEur?: number | null;
  colourSurchargeEur?: number | null;
  finalFobEur: number | null;
  paymentTermCode?: string | null;
  fobSourceMode?: string | null;
  fobSourceCountryCode?: string | null;
  remark?: string | null;
};

type BomCopyDraft = {
  draftKey: string;
  sourceBomTemplate: string;
  sourceDisplayLabel: string;
  bomTemplate: string;
  brand: string;
  modelName: string;
  version: string;
  powertrain: string;
  interiorColorName: string;
  editionTag: string | null;
  lifecycleStatus: string;
  effectiveFrom: string | null;
  effectiveTo: string | null;
  remark: string;
  fobByCountry: Record<string, BomDraftFobEntry>;
  bulkDeltaEur: string;
  bulkSelectedCountries: string[];
  skus: BomCopyDraftSku[];
};

type BomBulkFobEditor = {
  deltaEur: string;
  selectedCountries: string[];
};

type BomAdminCopyCountryForm = {
  sourceCountryCode: string;
  targetCountryCode: string;
  overwriteExisting: boolean;
};

type BomAdminAdjustCountryForm = {
  countryCode: string;
  deltaEur: string;
};

type BomAdminPageCache = {
  searchText: string;
  toolsFlipped: boolean;
  showAddMaterial: boolean;
  expandedGroups: string[];
  editingBoms: string[];
  bulkFobEditors: Record<string, BomBulkFobEditor>;
  copyCountryForm: BomAdminCopyCountryForm;
  adjustCountryForm: BomAdminAdjustCountryForm;
};

const BOM_ADMIN_PAGE_CACHE_KEY = "order-genius:bom-admin";
const BOM_ADMIN_PAGE_CACHE_TTL_MS = 30 * 60 * 1000;
const BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS = 1200;
const EMPTY_BOM_ADMIN_COPY_COUNTRY_FORM: BomAdminCopyCountryForm = {
  sourceCountryCode: "",
  targetCountryCode: "",
  overwriteExisting: false,
};
const EMPTY_BOM_ADMIN_ADJUST_COUNTRY_FORM: BomAdminAdjustCountryForm = {
  countryCode: "",
  deltaEur: "",
};

type BomAdminModelGroup = {
  brand: string;
  modelName: string;
  pt: string;
  versions: Map<string, any[]>;
};

function getBomAdminPowertrainGroup(powertrain: unknown): string {
  const saved = String(powertrain || "").trim().toUpperCase();
  if (saved === "EV") return "BEV";
  if (saved === "SHS") return "PHEV";
  if (saved === "FCEV") return "FCV";
  if (saved === "OTHER") return "Other";
  return saved || "Needs confirmation";
}

function getBomAdminModelGroupKey(brand: unknown, modelName: unknown, powertrain?: unknown): string {
  return `${String(brand || "")}|${String(modelName || "")}|${getBomAdminPowertrainGroup(powertrain)}`;
}

function getBomTemplateSearchText(bomTemplate: string): string {
  const normalized = bomTemplate.trim().toUpperCase();
  const wildcardIndex = normalized.indexOf("**");
  if (wildcardIndex >= 4) return normalized.slice(0, wildcardIndex);
  return normalized.replaceAll("*", "");
}

type BomAdminTierGroups = {
  single: any[];
  dual: any[];
  special: any[];
  allSkus: any[];
  countryCodes: string[];
  filledCountryCodes: string[];
  filledCountryCodeSet: ReadonlySet<string>;
};

type BomColourCodeEditor = {
  materialCode: string | null;
  colourCodeConfirmed: boolean;
  bomTemplate: string | null;
  brand: string;
  skuCount: number | null;
  correctingCode: boolean;
  suggestionSource: string | null;
  swatchAccepted: boolean;
  needsSynchronization: boolean;
  currentColourName: string;
  currentColourHex: string | null;
  storedColourHex: string | null;
  nextColourName: string;
  currentColourCode: string;
  nextColourCode: string;
  nextColourHex: string;
  nextColourHex2: string;
  isDualSwatch: boolean;
  colourNameTouched: boolean;
  colourHexTouched: boolean;
};

type BomAddColourEditor = {
  bomTemplate: string;
  tierName: BomAdminColourTier;
  sourceMaterialCode: string;
  brand: string;
  modelName: string;
  version: string;
  powertrain: string;
  interiorColorName: string;
  editionTag: string | null;
  fobSourceSku: any | null;
  fobSourceCountries: number;
  colourCode: string;
  colourName: string;
  colourHex: string;
  colourHex2: string;
  isDualSwatch: boolean;
  colourNameTouched: boolean;
  colourHexTouched: boolean;
};

function getBomColourCodeEditorTargetKey(editor: BomColourCodeEditor | null): string | null {
  if (!editor) return null;
  return [
    editor.materialCode,
    editor.brand,
    editor.currentColourCode,
  ].join("|");
}

function getBomAddColourEditorTargetKey(editor: BomAddColourEditor | null): string | null {
  if (!editor) return null;
  return [
    editor.bomTemplate,
    editor.sourceMaterialCode,
    editor.brand,
    editor.modelName,
    editor.version,
    editor.tierName,
  ].join("|");
}

type BomColourTierReview = {
  previousTier: BomAdminColourTier;
  nextTier: BomAdminColourTier;
  report: ColourTierRepriceReport;
};

const EMPTY_COLOUR_HEX_RULE_SUMMARY: ColourHexRuleSummary = {
  totalRules: 0,
  fillable: 0,
  missing: 0,
  nameConflict: 0,
  swatchConflict: 0,
  complete: 0,
  fillableSkus: 0,
  invalidIdentitySkuCount: 0,
  invalidIdentitySampleMaterialCodes: [],
};

type ColourRuleDetailsCategory = ColourHexRuleStatus | "missing_hex";

const COLOUR_RULE_STATUS_META: ReadonlyArray<{
  status: ColourRuleDetailsCategory;
  label: string;
  colour: string;
}> = [
  { status: "fillable", label: "Can fill", colour: "#0f766e" },
  { status: "missing", label: "Missing standard / 缺标准", colour: "#64748b" },
  { status: "missing_hex", label: "Missing HEX / 缺色卡", colour: "#64748b" },
  { status: "name_conflict", label: "Name conflict", colour: "#b45309" },
  { status: "swatch_conflict", label: "Swatch conflict", colour: "#dc2626" },
  { status: "complete", label: "Complete", colour: "#15803d" },
];

function normalizeColourPickerValue(value: string, fallback = MISSING_COLOUR_SWATCH_HEX): string {
  return normalizeColourHex(value) ?? fallback;
}

function isColourPickerValue(value: string): boolean {
  return normalizeColourHex(value) !== null;
}

function isSharedSwatchDraftValid(editor: BomColourCodeEditor): boolean {
  if (!editor.nextColourName.trim()) return false;
  if (!editor.nextColourHex.trim() && (!editor.isDualSwatch || !editor.nextColourHex2.trim())) return true;
  return isColourPickerValue(editor.nextColourHex) && (!editor.isDualSwatch || isColourPickerValue(editor.nextColourHex2));
}

function splitColourHexValue(value: unknown): {
  hex1: string;
  hex2: string;
  isDual: boolean;
  hasStoredHex: boolean;
} {
  const swatch = parseOrderGeniusColourSwatch(value);
  const [hex1, hex2] = swatch.colours;
  return {
    hex1: swatch.isMissing ? "" : hex1,
    hex2: swatch.isMissing ? "" : hex2 ?? "",
    isDual: swatch.isDual,
    hasStoredHex: !swatch.isMissing,
  };
}

function buildSwatchPayload(hex1: string, hex2: string, isDual: boolean): string {
  return buildOrderGeniusColourSwatch(hex1, hex2, isDual) ?? "";
}

function getColourRuleStandardChoices(rule: ColourHexRule): Array<{ colourName: string; colourHex: string }> {
  const names = rule.nameOptions.map((option) => option.colourName);
  const fallbackName = rule.standardColourName ?? rule.colourName;
  if (names.length === 0 && fallbackName) names.push(fallbackName);
  const hexes = rule.hexOptions.map((option) => option.colourHex);
  if (hexes.length === 0 && rule.standardColourHex) hexes.push(rule.standardColourHex);
  if (hexes.length === 0) hexes.push("");
  return names.flatMap((colourName) => hexes.map((colourHex) => ({ colourName, colourHex })));
}

function getColourRuleDetails(rules: ColourHexRule[], status: ColourRuleDetailsCategory): ColourHexRule[] {
  if (status === "missing_hex") return rules.filter(rule => rule.missingSwatchSkuCount > 0);
  if (status === "name_conflict") return rules.filter(rule => rule.hasNameConflict);
  if (status === "swatch_conflict") return rules.filter(rule => rule.hasSwatchConflict);
  return rules.filter(rule => rule.status === status);
}

function formatColourRuleLookupNote(lookup: ColourHexRuleLookup, manuallyChanged: boolean): string {
  if (lookup.hasNameConflict || lookup.hasSwatchConflict) {
    const fields = [lookup.hasNameConflict ? "name" : "", lookup.hasSwatchConflict ? "swatch" : ""]
      .filter(Boolean)
      .join(" + ");
    return `Brand + Code ${fields} conflict: not auto-filled. Enter values manually.`;
  }
  if (lookup.source === "persistent_rule" || lookup.source === "brand_code_rule") {
    return `Brand + Code rule: ${lookup.colourName ?? "no name"} · ${lookup.colourHex ?? "no swatch"}${manuallyChanged ? " · Manual values will create a rule difference." : " · Auto-filled."}`;
  }
  if (lookup.source === "name_candidates") {
    return "Existing same-name swatches: choose Use this swatch to adopt; codes remain separate. / 同名已有色卡仅供明确采用，色码不合并。";
  }
  return "No reusable Brand + Code rule. Enter a colour name; swatch is optional.";
}

interface BomFinanceQuickCard {
  countryCode: string;
  materialCode: string;
  materialCodes: string[];
  title: string;
  fob: number | null;
  remark: string;
  fobSourceMode?: string | null;
  fobSourceCountryCode?: string | null;
}

type BomFobEditor = {
  bomTemplate?: string | null;
  materialCodes: string[];
  countryCode: string;
  fob: number | null;
  originalFob: number | null;
  remark: string;
  fobSourceMode?: string | null;
  fobSourceCountryCode?: string | null;
};

type BomFobPeriodDraft = {
  periodId: string | null;
  rowVersion: number | null;
  validFrom: string;
  validTo: string;
  baseFobEur: string;
  remark: string;
};

const EMPTY_BOM_FOB_PERIOD_DRAFT: BomFobPeriodDraft = {
  periodId: null,
  rowVersion: null,
  validFrom: "",
  validTo: "",
  baseFobEur: "",
  remark: "",
};

type BomFobSaveResponse = {
  materialCode: string;
  countryCode: string;
  baseFobEur: number | null;
  colourSurchargeEur: number | null;
  finalFobEur: number | null;
  paymentTermCode?: string | null;
  fobSourceMode?: string | null;
  fobSourceCountryCode?: string | null;
  remark?: string | null;
};

interface BomFinanceDrawerScope {
  countryCode: string;
  brand: string;
  modelName: string;
  powertrain: string;
  version?: string;
}

interface BomAdminPanelProps {
  isAdmin: boolean;
  initialCopyTargetCountry?: string | null;
  onFobCountriesChanged?: () => void;
  onFobChanged?: () => void | Promise<void>;
}

type BomFinanceAction = {
  label: string;
  kind?: "primary" | "ghost";
  onClick: () => void;
  disabled?: boolean;
};

function BomFinanceActionBar({ actions }: { actions: BomFinanceAction[] }) {
  return (
    <div className="bom-finance-action-bar">
      {actions.map((action) => (
        <button
          key={action.label}
          type="button"
          className={`btn btn-sm ${action.kind === "primary" ? "btn-primary" : "btn-ghost"} bom-finance-action-button`}
          disabled={action.disabled}
          onClick={action.onClick}
        >
          {action.label}
        </button>
      ))}
    </div>
  );
}

function getDraftBaseFob(
  fob: BomDraftFobEntry | null | undefined,
): number | null {
  if (!fob) return null;
  const raw = fob.baseFobEur ?? fob.finalFobEur ?? fob.uploadedFobEur;
  if (raw == null) return null;
  const numeric = Number(raw);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

function bomMaterialKey(materialCode: unknown): string {
  return String(materialCode ?? "").trim().toUpperCase();
}

function formatBomSourceLabel(
  modelName: string,
  sourceSheetName: unknown,
  sourceRowNumber: unknown,
): string {
  const sheet = String(sourceSheetName || modelName || "").trim();
  const rawRow = sourceRowNumber == null ? "" : String(sourceRowNumber).trim();
  const rowMatch = rawRow.match(/\d+/);
  const row = rowMatch ? `R${rowMatch[0]}` : "";
  if (sheet && row) return `${sheet}·${row}`;
  if (sheet) return sheet;
  if (row) return row;
  return "";
}

// ── Payment Terms Admin Panel ──────────────────────────────────────────

// ── BOM Admin Panel ──────────────────────────────────────────────────

export function BomAdminPanel({
  isAdmin,
  initialCopyTargetCountry = null,
  onFobCountriesChanged,
  onFobChanged,
}: BomAdminPanelProps) {
  const cachedBomAdminRef = useRef<BomAdminPageCache | null | undefined>(undefined);
  if (cachedBomAdminRef.current === undefined) {
    cachedBomAdminRef.current = getCachedPageValue<BomAdminPageCache>(BOM_ADMIN_PAGE_CACHE_KEY);
  }
  const cachedBomAdmin = cachedBomAdminRef.current;
  const cachedSearchText = cachedBomAdmin?.searchText ?? "";
  const initialBomLoadSearchRef = useRef(cachedSearchText.trim());
  const appliedBomSearchRef = useRef(cachedSearchText.trim());
  const skipNextDebouncedLoadRef = useRef(true);
  const [skus, setSkus] = useState<any[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [activeFobCountries, setActiveFobCountries] = useState<string[]>([]);
  const { countryOptions: accountCountryOptions } = useAccountCountryOptions();
  const [loading, setLoading] = useState(true);
  const [searchText, setSearchText] = useState(cachedSearchText);
  const [debouncedSearch, setDebouncedSearch] = useState(cachedSearchText.trim());
  const [editFob, setEditFob] = useState<BomFobEditor | null>(null);
  const [fobPeriods, setFobPeriods] = useState<CountryTemplateFobPeriod[]>([]);
  const [periodDeletePreview, setPeriodDeletePreview] = useState<(CountryTemplateFobPeriod & FobPeriodDeletionPreview) | null>(null);
  const [fobPeriodsLoading, setFobPeriodsLoading] = useState(false);
  const [fobPeriodSaving, setFobPeriodSaving] = useState(false);
  const [fobPeriodError, setFobPeriodError] = useState("");
  const [fobPeriodDraft, setFobPeriodDraft] = useState<BomFobPeriodDraft>(EMPTY_BOM_FOB_PERIOD_DRAFT);
  const [financeQuickCard, setFinanceQuickCard] = useState<BomFinanceQuickCard | null>(null);
  const [financeQuickFlipped, setFinanceQuickFlipped] = useState(false);
  const [financeQuickRows, setFinanceQuickRows] = useState<CountryMaterialFinanceRow[]>([]);
  const [financeQuickLoading, setFinanceQuickLoading] = useState(false);
  const [financeDrawerScope, setFinanceDrawerScope] = useState<BomFinanceDrawerScope | null>(null);
  const [financeDrawerFlipped, setFinanceDrawerFlipped] = useState(false);
  const [financeDrawerRows, setFinanceDrawerRows] = useState<CountryMaterialFinanceRow[]>([]);
  const [financeDrawerLoading, setFinanceDrawerLoading] = useState(false);
  const [savingFinanceMaterialCode, setSavingFinanceMaterialCode] = useState<string | null>(null);
  const [financeError, setFinanceError] = useState("");
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(() => new Set(cachedBomAdmin?.expandedGroups ?? []));
  const [showAddMaterial, setShowAddMaterial] = useState(cachedBomAdmin?.showAddMaterial ?? false);
  const [toolsFlipped, setToolsFlipped] = useState(cachedBomAdmin?.toolsFlipped ?? false);
  const [isCompactToolsLayout, setIsCompactToolsLayout] = useState(() =>
    typeof window !== "undefined" && window.innerWidth <= BOM_ADMIN_TOOLS_COMPACT_BREAKPOINT,
  );
  const [isPhoneToolsLayout, setIsPhoneToolsLayout] = useState(() =>
    typeof window !== "undefined" && window.innerWidth <= BOM_ADMIN_TOOLS_PHONE_BREAKPOINT,
  );
  const [newMaterial, setNewMaterial] = useState<AddMaterialFormState>(EMPTY_ADD_MATERIAL);
  const [addMaterialError, setAddMaterialError] = useState("");
  const [addMaterialNotice, setAddMaterialNotice] = useState("");
  const [copyCountryForm, setCopyCountryForm] = useState<BomAdminCopyCountryForm>({
    ...EMPTY_BOM_ADMIN_COPY_COUNTRY_FORM,
    ...(cachedBomAdmin?.copyCountryForm ?? {}),
  });
  const [copyCountryMessage, setCopyCountryMessage] = useState("");
  const [bomAdminNotice, setBomAdminNotice] = useState("");
  const [bomAdminError, setBomAdminError] = useState("");
  const [savingProductKey, setSavingProductKey] = useState<string | null>(null);
  const [productSaveMessages, setProductSaveMessages] = useState<Record<string, { kind: "success" | "error"; text: string }>>({});
  const [copyingCountry, setCopyingCountry] = useState(false);
  const [adjustCountryForm, setAdjustCountryForm] = useState<BomAdminAdjustCountryForm>({
    ...EMPTY_BOM_ADMIN_ADJUST_COUNTRY_FORM,
    ...(cachedBomAdmin?.adjustCountryForm ?? {}),
  });
  const [adjustCountryMessage, setAdjustCountryMessage] = useState("");
  const [adjustingCountry, setAdjustingCountry] = useState(false);
  const [bomExportCountry, setBomExportCountry] = useState("");
  const [exportingBomAdmin, setExportingBomAdmin] = useState(false);
  const [bomExportStatus, setBomExportStatus] = useState("");
  const [copyDrafts, setCopyDrafts] = useState<Record<string, BomCopyDraft>>({});
  const [copyDraftErrors, setCopyDraftErrors] = useState<Record<string, string>>({});
  const [copyDraftSavingKey, setCopyDraftSavingKey] = useState<string | null>(null);
  const [copyDraftFocusKey, setCopyDraftFocusKey] = useState<string | null>(null);
  const [bulkFobEditors, setBulkFobEditors] = useState<Record<string, BomBulkFobEditor>>(cachedBomAdmin?.bulkFobEditors ?? {});
  const [bulkFobErrors, setBulkFobErrors] = useState<Record<string, string>>({});
  const [bulkFobSavingKey, setBulkFobSavingKey] = useState<string | null>(null);
  const [optimisticColourTiers, setOptimisticColourTiers] = useState<Record<string, BomAdminColourTier>>({});
  const [colourSurchargeRules, setColourSurchargeRules] = useState<ColourSurchargeRule[]>([]);
  const [colourSurchargeDrafts, setColourSurchargeDrafts] = useState<Record<string, string>>({});
  const [colourSurchargeStatus, setColourSurchargeStatus] = useState("");
  const [savingColourSurcharges, setSavingColourSurcharges] = useState(false);
  const [specialColourSurchargeRules, setSpecialColourSurchargeRules] = useState<SpecialColourSurchargeRule[]>([]);
  const [specialColourSurchargeDraft, setSpecialColourSurchargeDraft] = useState<SpecialColourSurchargeDraft>(EMPTY_SPECIAL_COLOUR_SURCHARGE_DRAFT);
  const [specialColourSurchargeExpanded, setSpecialColourSurchargeExpanded] = useState(false);
  const [specialColourSurchargeStatus, setSpecialColourSurchargeStatus] = useState("");
  const [savingSpecialColourSurcharge, setSavingSpecialColourSurcharge] = useState(false);
  const [colourHexRules, setColourHexRules] = useState<ColourHexRule[]>([]);
  const [colourHexRuleSummary, setColourHexRuleSummary] = useState<ColourHexRuleSummary>(EMPTY_COLOUR_HEX_RULE_SUMMARY);
  const [colourHexRuleStatus, setColourHexRuleStatus] = useState("");
  const [loadingColourHexRules, setLoadingColourHexRules] = useState(false);
  const [colourRuleDetailsStatus, setColourRuleDetailsStatus] = useState<ColourRuleDetailsCategory | null>(null);
  const [showColourRulePreview, setShowColourRulePreview] = useState(false);
  const [colourRulePreview, setColourRulePreview] = useState<ColourHexRulePreview | null>(null);
  const [colourRuleApplyResult, setColourRuleApplyResult] = useState<ColourHexRuleApplyResult | null>(null);
  const [colourRuleActionError, setColourRuleActionError] = useState("");
  const [loadingColourRulePreview, setLoadingColourRulePreview] = useState(false);
  const [applyingColourRulePreview, setApplyingColourRulePreview] = useState(false);
  const [colourCodeEditor, setColourCodeEditor] = useState<BomColourCodeEditor | null>(null);
  const [colourCodeRuleLookup, setColourCodeRuleLookup] = useState<ColourHexRuleLookup | null>(null);
  const [loadingColourCodeRuleLookup, setLoadingColourCodeRuleLookup] = useState(false);
  const [colourCodeEditorError, setColourCodeEditorError] = useState("");
  const [savingColourCodeEditor, setSavingColourCodeEditor] = useState(false);
  const [confirmingColourEdit, setConfirmingColourEdit] = useState(false);
  const [savedColourRefresh, setSavedColourRefresh] = useState<string | null>(null);
  const colourSaveInFlightRef = useRef(false);
  const [addColourEditor, setAddColourEditor] = useState<BomAddColourEditor | null>(null);
  const [addColourRuleLookup, setAddColourRuleLookup] = useState<ColourHexRuleLookup | null>(null);
  const [loadingAddColourRuleLookup, setLoadingAddColourRuleLookup] = useState(false);
  const [addColourEditorError, setAddColourEditorError] = useState("");
  const [savingAddColourEditor, setSavingAddColourEditor] = useState(false);
  const [colourTierReview, setColourTierReview] = useState<BomColourTierReview | null>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const materialCodeInputRef = useRef<HTMLInputElement>(null);
  const colourCodeEditorInputRef = useRef<HTMLInputElement>(null);
  const colourCodeLookupRequestRef = useRef(0);
  const addColourEditorCodeRef = useRef<HTMLInputElement>(null);
  const addColourEditorNameRef = useRef<HTMLInputElement>(null);
  const addColourLookupRequestRef = useRef(0);
  const copyDraftInputRefs = useRef<Record<string, HTMLInputElement | null>>({});
  const bomGroupRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const bomTemplateRowRefs = useRef<Record<string, HTMLTableRowElement | null>>({});
  const auditNavigationTargetRef = useRef<{ modelGroupKey: string; bomTemplate: string; materialCode?: string } | null>(null);
  const expandedBomGroupKeyRef = useRef<string | null>(null);
  const [dragSku, setDragSku] = useState<string | null>(null);
  const [dragOverTier, setDragOverTier] = useState<string | null>(null);
  const dragEnterCount = useRef(0);
  const dragMaterialCode = useRef<string | null>(null); // bypass dataTransfer quirks
  const [editingBoms, setEditingBoms] = useState<Set<string>>(() => new Set(cachedBomAdmin?.editingBoms ?? []));
  const toggleEditBom = (key: string) => {
    setEditingBoms(prev => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n; });
  };

  // Double-confirm delete state + performance refs
  const [pendingDeletes, setPendingDeletes] = useState<Set<string>>(new Set());
  const pendingDeleteTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const loadRef = useRef(false);  // prevent concurrent loads
  const loadCompletionRef = useRef<Promise<boolean>>(Promise.resolve(true));
  const currentLoadKeyRef = useRef<string | null>(null);
  const pendingLoadKeyRef = useRef<string | null>(null);
  const latestLoadKeyRef = useRef(cachedSearchText.trim());
  const loadTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);  // debounce loads
  const activeFobCountriesRef = useRef<string[]>([]);

  const colourCodeEditorTargetKey = getBomColourCodeEditorTargetKey(colourCodeEditor);
  const addColourEditorTargetKey = getBomAddColourEditorTargetKey(addColourEditor);

  // NL always first, then alphabetical
  const sortedCountries = useMemo(() => {
    const rest = countries.filter(c => c !== 'NL').sort();
    return countries.includes('NL') ? ['NL', ...rest] : rest;
  }, [countries]);
  const sortedActiveFobCountries = useMemo(() => {
    const rest = activeFobCountries.filter(c => c !== "NL").sort();
    return activeFobCountries.includes("NL") ? ["NL", ...rest] : rest;
  }, [activeFobCountries]);

  useEffect(() => {
    const targetCountry = String(initialCopyTargetCountry || "").trim().toUpperCase();
    if (!targetCountry) return;
    const sourceCountry = sortedActiveFobCountries.includes("CZ")
      ? "CZ"
      : sortedActiveFobCountries.find((countryCode) => countryCode !== targetCountry) || "";
    setToolsFlipped(true);
    setShowAddMaterial(false);
    setBomAdminNotice(`Showing all BOM templates. ${targetCountry} has no FOB yet; copy FOB from an existing country to create it.`);
    setCopyCountryMessage(`Target ${targetCountry} has no FOB yet. Choose a source country, then copy FOB.`);
    setCopyCountryForm((current) => ({
      ...current,
      sourceCountryCode: current.sourceCountryCode || sourceCountry,
      targetCountryCode: targetCountry,
    }));
    setAdjustCountryForm((current) => ({
      ...current,
      countryCode: current.countryCode || targetCountry,
    }));
  }, [initialCopyTargetCountry, sortedActiveFobCountries]);

  const countryLabels = useMemo(() => {
    const map = new Map<string, string>();
    for (const country of accountCountryOptions) {
      map.set(country.countryCode, country.countryName);
    }
    return map;
  }, [accountCountryOptions]);
  const countryTooltipByCode = useMemo(() => {
    const map = new Map<string, string>();
    for (const countryCode of sortedCountries) {
      map.set(countryCode, formatCountryCodeTooltip(countryCode));
    }
    return map;
  }, [sortedCountries]);

  const handleBomAdminExport = async () => {
    setExportingBomAdmin(true);
    setBomExportStatus("");
    try {
      const blob = await api.exportBomAdmin(bomExportCountry || undefined);
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = `BOM_Admin_Material_Master_${bomExportCountry || "ALL"}.xlsx`;
      anchor.click();
      URL.revokeObjectURL(url);
      setBomExportStatus(`Exported ${bomExportCountry || "all countries"}.`);
    } catch (error) {
      setBomExportStatus(`Export failed: ${getErrorMessage(error)}`);
    } finally {
      setExportingBomAdmin(false);
    }
  };
  const bomAdminTableMinWidth = BOM_ADMIN_FIXED_COLUMN_WIDTH + sortedCountries.length * BOM_ADMIN_COUNTRY_COLUMN_WIDTH;
  const renderBomAdminColumnGroup = () => (
    <colgroup>
      <col style={{ width: BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom }} />
      <col style={{ width: BOM_ADMIN_STICKY_COLUMN_WIDTHS.interior }} />
      <col style={{ width: BOM_ADMIN_STICKY_COLUMN_WIDTHS.single }} />
      <col style={{ width: BOM_ADMIN_STICKY_COLUMN_WIDTHS.dual }} />
      <col style={{ width: BOM_ADMIN_STICKY_COLUMN_WIDTHS.special }} />
      <col style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }} />
      <col style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions }} />
      <col style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from }} />
      <col style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to }} />
      {sortedCountries.map((countryCode) => (
        <col key={`country-col-${countryCode}`} style={{ width: BOM_ADMIN_COUNTRY_COLUMN_WIDTH }} />
      ))}
    </colgroup>
  );

  const addMaterialDraftSummary = useMemo(() => {
    const hasBatchIntent = newMaterial.materialCode.includes("**") || newMaterial.colourBatch.trim().length > 0;
    if (!hasBatchIntent) return "";
    const result = buildMaterialDrafts(newMaterial);
    if (result.drafts.length > 0) {
      const first = result.drafts[0];
      return `${result.drafts.length} colours -> ${result.drafts.length} SKUs · ${first.materialCode}`;
    }
    return result.errors[0] ?? "";
  }, [newMaterial]);

  const selectedColourRuleDetails = useMemo(() => {
    if (!colourRuleDetailsStatus) return [];
    return getColourRuleDetails(colourHexRules, colourRuleDetailsStatus);
  }, [colourHexRules, colourRuleDetailsStatus]);

  const formatBomFobTooltip = (
    countryCode: string,
    baseFob: number | null | undefined,
    colourSurchargeEur?: number | null,
    fobSourceMode?: string | null,
    fobSourceCountryCode?: string | null,
    remark?: string | null,
  ): string => {
    const fobLabel = baseFob != null && baseFob > 0
      ? `FOB ${baseFob.toLocaleString()} EUR`
      : "No FOB";
    const surchargeLabel = colourSurchargeEur != null && colourSurchargeEur > 0
      ? ` · surcharge +${colourSurchargeEur.toLocaleString()} EUR`
      : "";
    const sourceLabel = formatBomFobSourceLabel(fobSourceMode, fobSourceCountryCode);
    const remarkLabel = String(remark || "").trim();
    return `${countryTooltipByCode.get(countryCode) || formatCountryCodeTooltip(countryCode)} · ${fobLabel}${surchargeLabel}${sourceLabel ? ` · ${sourceLabel}` : ""}${remarkLabel ? ` · remark: ${remarkLabel}` : ""}`;
  };

  const formatBomFobSourceLabel = (
    fobSourceMode?: string | null,
    fobSourceCountryCode?: string | null,
  ): string => {
    if (fobSourceMode === "copied_from_country" || (fobSourceMode === "template_base" && fobSourceCountryCode)) {
      return `copied from ${fobSourceCountryCode || "source country"}`;
    }
    if (fobSourceMode === "manual_country_adjust") return "manual country adjustment";
    if (fobSourceMode === "template_base" || fobSourceMode === "template_base_country_adjust") return "template base + colour rule";
    if (fobSourceMode === "manual_edit") return "manual edit";
    if (fobSourceMode === "explicit_price_by_payment_term") return "uploaded/resolved FOB";
    return "";
  };

  const getBomFobSourceMarker = (fobSourceMode?: string | null, sourceCountry?: string | null): string => {
    if (fobSourceMode === "copied_from_country") return "C";
    if (fobSourceMode === "manual_country_adjust") return "B";
    if (fobSourceMode === "template_base_country_adjust") return "B";
    if (fobSourceMode === "template_base") return sourceCountry ? "C" : "M";
    if (fobSourceMode === "manual_edit") return "M";
    return "";
  };

  const getEffectiveColourTier = useCallback((sku: BomAdminSkuColourFields & { materialCode?: string | null }): BomAdminColourTier => {
    const optimisticTier = optimisticColourTiers[bomMaterialKey(sku.materialCode)];
    return optimisticTier ?? inferBomAdminColourTier(sku);
  }, [optimisticColourTiers]);

  const copyTargetOptions = useMemo(() => {
    const map = new Map<string, string>();
    for (const code of sortedCountries) map.set(code, code);
    for (const country of accountCountryOptions) map.set(country.countryCode, country.countryCode);
    return Array.from(map.keys()).sort();
  }, [accountCountryOptions, sortedCountries]);

  const loadColourSurcharges = useCallback(async () => {
    try {
      const res = await api.getOrderGeniusColourSurcharges();
      const rules = res.items || [];
      setColourSurchargeRules(rules);
      const nextDrafts: Record<string, string> = {};
      for (const brand of BOM_ADMIN_SURCHARGE_BRANDS) {
        for (const type of BOM_ADMIN_SURCHARGE_TYPES) {
          const key = colourSurchargeKey(brand, type.value);
          const rule = rules.find(
            (item) => colourSurchargeKey(item.brand, item.colourType) === key,
          );
          nextDrafts[key] = formatSurchargeDraft(
            rule ? Number(rule.surchargeEur) : DEFAULT_COLOUR_SURCHARGES[key] ?? 0,
          );
        }
      }
      setColourSurchargeDrafts(nextDrafts);
    } catch (e) {
      setColourSurchargeStatus(getErrorMessage(e));
    }
  }, []);

  const loadSpecialColourSurcharges = useCallback(async () => {
    try {
      const res = await api.getOrderGeniusSpecialColourSurcharges();
      setSpecialColourSurchargeRules(res.items || []);
    } catch (e) {
      setSpecialColourSurchargeStatus(getErrorMessage(e));
    }
  }, []);

  const loadColourHexRules = useCallback(async (): Promise<boolean> => {
    try {
      setLoadingColourHexRules(true);
      const res = await api.getOrderGeniusColourHexRules();
      setColourHexRules(res.items || []);
      setColourHexRuleSummary(res.summary || EMPTY_COLOUR_HEX_RULE_SUMMARY);
      setColourHexRuleStatus("");
      return true;
    } catch (e) {
      setColourHexRuleStatus(getErrorMessage(e));
      return false;
    } finally {
      setLoadingColourHexRules(false);
    }
  }, []);

  const load = useCallback(async (requestedSearch?: string): Promise<boolean> => {
    // An omitted search means refresh the currently applied query. An explicit
    // empty string is the Clear action and must remain distinguishable.
    const loadKey = requestedSearch === undefined
      ? appliedBomSearchRef.current
      : requestedSearch.trim();
    if (requestedSearch !== undefined) {
      appliedBomSearchRef.current = loadKey;
    }
    latestLoadKeyRef.current = loadKey;
    if (loadRef.current) {
      // Keep only the latest user intent. Empty string is meaningful here: it
      // represents an explicit Clear and must not be collapsed into "none".
      pendingLoadKeyRef.current = loadKey;
      return loadCompletionRef.current;
    }
    loadRef.current = true;
    let complete!: (loaded: boolean) => void;
    loadCompletionRef.current = new Promise<boolean>((resolve) => { complete = resolve; });
    let loaded = false;
    currentLoadKeyRef.current = loadKey;
    setLoading(true);
    setBomAdminError("");
    try {
      const normalizedSearch = loadKey.toUpperCase();
      const isCountry = /^[A-Z]{2}$/.test(normalizedSearch);
      const params: { country?: string; search?: string } = {};
      if (loadKey) {
        if (isCountry) {
          params.country = normalizedSearch;
          const fobCountries = activeFobCountriesRef.current;
          if (fobCountries.includes(normalizedSearch)) {
            setBomAdminNotice("");
          } else if (fobCountries.length > 0) {
            const sourceCountry = fobCountries.includes("CZ")
              ? "CZ"
              : fobCountries.find((countryCode) => countryCode !== normalizedSearch) || "";
            setToolsFlipped(true);
            setShowAddMaterial(false);
            setBomAdminNotice(`${normalizedSearch} has no BOM price yet. Clear the country filter to select templates and copy FOB.`);
            setCopyCountryMessage(`Target ${normalizedSearch} has no FOB yet. Choose a source country, then copy FOB.`);
            setCopyCountryForm((current) => ({
              ...current,
              sourceCountryCode: current.sourceCountryCode || sourceCountry,
              targetCountryCode: normalizedSearch,
            }));
            setAdjustCountryForm((current) => ({
              ...current,
              countryCode: current.countryCode || normalizedSearch,
            }));
          } else {
            setBomAdminNotice("");
          }
        } else {
          params.search = loadKey;
          setBomAdminNotice("");
        }
      } else {
        setBomAdminNotice("");
      }
      const res = await api.getBomAdmin(Object.keys(params).length > 0 ? params : undefined);
      if (latestLoadKeyRef.current === loadKey) {
        const nextItems = res.items || [];
        const auditTarget = auditNavigationTargetRef.current;
        if (auditTarget) {
          const targetSku = nextItems.find((sku) => (
            bomMaterialKey(sku.bomTemplate) === bomMaterialKey(auditTarget.bomTemplate)
            && (!auditTarget.materialCode || bomMaterialKey(sku.materialCode) === bomMaterialKey(auditTarget.materialCode))
          ));
          if (!targetSku) throw new Error("The audit material is no longer available. Refresh the audit and retry.");
          const groupKey = getBomAdminModelGroupKey(targetSku.brand, targetSku.modelName, targetSku.powertrain);
          auditTarget.modelGroupKey = groupKey;
          setExpandedGroups((current) => new Set([...current, groupKey]));
          const editKey = buildBomEditScopeKey(groupKey, targetSku.version || "Default", String(targetSku.bomTemplate));
          setEditingBoms((current) => new Set([...current, editKey]));
        }
        setSkus(nextItems);
        setOptimisticColourTiers((current) => {
          const next = { ...current };
          for (const sku of nextItems) {
            const materialKey = bomMaterialKey(sku?.materialCode);
            if (!materialKey || !next[materialKey]) continue;
            if (inferBomAdminColourTier(sku) === next[materialKey]) {
              delete next[materialKey];
            }
          }
          return Object.keys(next).length === Object.keys(current).length ? current : next;
        });
        const nextCountries = res.countries || [];
        const conflictCount = Array.isArray(res.fobConflicts) ? res.fobConflicts.length : 0;
        if (conflictCount > 0) {
          setBomAdminNotice(
            `${conflictCount} 个物料／国家的 FOB 基准待确认；正常行仍可编辑，请先在 BOM Admin 保存唯一模板＋国家 Single 基准。`,
          );
        }
        const nextActiveFobCountries = res.activeFobCountries || nextCountries;
        activeFobCountriesRef.current = nextActiveFobCountries;
        setCountries(nextCountries);
        setActiveFobCountries(nextActiveFobCountries);
        setBomAdminError("");
        loaded = true;
      }
    } catch (e) {
      console.error('[BOM Admin]', e);
      if (latestLoadKeyRef.current === loadKey) {
        setBomAdminError(getErrorMessage(e));
      }
    }
    finally {
      loadRef.current = false;
      currentLoadKeyRef.current = null;
      const pendingLoadKey = pendingLoadKeyRef.current;
      pendingLoadKeyRef.current = null;
      if (pendingLoadKey !== null) {
        loaded = await load(pendingLoadKey);
      } else {
        setLoading(false);
      }
      complete(loaded);
    }
    return loaded;
  }, []);

  const openAuditBomTemplate = useCallback(async (target: BomFobAuditTemplateTarget) => {
    const bomTemplate = target.bomTemplate.trim().toUpperCase();
    const search = getBomTemplateSearchText(bomTemplate);
    const modelGroupKey = getBomAdminModelGroupKey(target.brand, target.modelName);
    auditNavigationTargetRef.current = { modelGroupKey, bomTemplate, materialCode: target.materialCode };
    expandedBomGroupKeyRef.current = modelGroupKey;
    setExpandedGroups((current) => {
      const next = new Set(current);
      next.add(modelGroupKey);
      return next;
    });
    setToolsFlipped(false);
    setSearchText(search);
    if (debouncedSearch !== search) {
      skipNextDebouncedLoadRef.current = true;
      setDebouncedSearch(search);
    }
    if (!await load(search)) {
      auditNavigationTargetRef.current = null;
      throw new Error("Could not load the BOM correction editor. Please retry.");
    }
    setBomAdminNotice(`Editing BOM template ${bomTemplate}${target.materialCode ? ` · ${target.materialCode}` : ""}. Drag the colour to its intended Single / Dual / Special tier, or correct the country base; then refresh the FOB audit.`);
  }, [debouncedSearch, load]);

  const scheduleLoad = useCallback((delay = 0) => {
    if (loadTimerRef.current) clearTimeout(loadTimerRef.current);
    loadTimerRef.current = setTimeout(() => load(), delay);
  }, [load]);

  const patchBomSkus = useCallback((
    materialCodes: string[],
    updater: (sku: any) => any,
  ) => {
    const targetCodes = new Set(materialCodes.map(bomMaterialKey).filter(Boolean));
    if (targetCodes.size === 0) return;
    setSkus((current) => current.map((sku) =>
      targetCodes.has(bomMaterialKey(sku?.materialCode)) ? updater(sku) : sku,
    ));
  }, []);

  const patchBomFobs = useCallback((updates: BomFobPatch[]) => {
    const updatesByMaterial = new Map<string, BomFobPatch[]>();
    for (const update of updates) {
      const materialKey = bomMaterialKey(update.materialCode);
      const countryCode = update.countryCode.trim().toUpperCase();
      if (!materialKey || !countryCode) continue;
      const existing = updatesByMaterial.get(materialKey) ?? [];
      existing.push({ ...update, countryCode });
      updatesByMaterial.set(materialKey, existing);
    }
    if (updatesByMaterial.size === 0) return;
    setSkus((current) => current.map((sku) => {
      const materialUpdates = updatesByMaterial.get(bomMaterialKey(sku?.materialCode));
      if (!materialUpdates) return sku;
      const fobByCountry: Record<string, BomDraftFobEntry> = { ...(sku.fobByCountry || {}) };
      for (const update of materialUpdates) {
        const existing = fobByCountry[update.countryCode] || {};
        const hasRemarkUpdate = Object.prototype.hasOwnProperty.call(update, "remark");
        fobByCountry[update.countryCode] = {
          ...existing,
          baseFobEur: Object.prototype.hasOwnProperty.call(update, "baseFobEur")
            ? update.baseFobEur
            : existing.baseFobEur,
          uploadedFobEur: update.baseFobEur ?? update.finalFobEur,
          finalFobEur: update.finalFobEur,
          colourSurchargeEur: Object.prototype.hasOwnProperty.call(update, "colourSurchargeEur")
            ? update.colourSurchargeEur
            : existing.colourSurchargeEur,
          paymentTermCode: update.paymentTermCode ?? existing.paymentTermCode ?? null,
          fobSourceMode: update.fobSourceMode ?? existing.fobSourceMode ?? "manual_edit",
          fobSourceCountryCode: update.fobSourceCountryCode ?? existing.fobSourceCountryCode ?? null,
          remark: hasRemarkUpdate ? (update.remark ?? null) : existing.remark ?? null,
        };
      }
      return { ...sku, fobByCountry };
    }));
  }, []);

  const patchBomInterior = useCallback((
    materialCodes: string[],
    interiorColorName: string | null,
    editionTag: string | null,
  ) => {
    patchBomSkus(materialCodes, (sku) => ({
      ...sku,
      interiorColorName,
      editionTag,
    }));
  }, [patchBomSkus]);

  const getBomTemplateRemark = useCallback((allSkus: any[]): string => {
    for (const sku of allSkus) {
      const remark = String(sku?.remark || "").trim();
      if (remark) return remark;
    }
    return "";
  }, []);

  const getBomCountryFobRemark = useCallback((allSkus: any[], countryCode: string): string => {
    const normalizedCountry = countryCode.trim().toUpperCase();
    for (const sku of allSkus) {
      const fob = sku?.fobByCountry?.[normalizedCountry] as BomDraftFobEntry | undefined;
      const remark = String(fob?.remark || "").trim();
      if (remark) return remark;
    }
    return "";
  }, []);

  useEffect(() => { load(initialBomLoadSearchRef.current); }, [load]);
  useEffect(() => { void loadColourSurcharges(); }, [loadColourSurcharges]);
  useEffect(() => { void loadSpecialColourSurcharges(); }, [loadSpecialColourSurcharges]);
  useEffect(() => { void loadColourHexRules(); }, [loadColourHexRules]);

  useEffect(() => {
    const brand = String(colourCodeEditor?.brand || "").trim();
    const colourCode = String(colourCodeEditor?.nextColourCode || "").trim().toUpperCase();
    const colourName = String(colourCodeEditor?.nextColourName || "").trim();
    const targetKey = colourCodeEditorTargetKey;
    const requestId = ++colourCodeLookupRequestRef.current;
    setColourCodeRuleLookup(null);
    setColourCodeEditorError("");
    if (!brand || !/^[A-Z0-9]{1,4}$/.test(colourCode)) {
      setColourCodeRuleLookup(null);
      setLoadingColourCodeRuleLookup(false);
      return undefined;
    }
    setLoadingColourCodeRuleLookup(true);
    const timer = window.setTimeout(() => {
      void api.lookupOrderGeniusColourHexRule(brand, colourCode, colourName).then((lookup) => {
        if (colourCodeLookupRequestRef.current !== requestId) return;
        setColourCodeRuleLookup(lookup);
        setColourCodeEditor((current) => {
          if (
            !current
            || getBomColourCodeEditorTargetKey(current) !== targetKey
            || current.nextColourCode.trim().toUpperCase() !== colourCode
          ) return current;
          // Only an authoritative same-code standard enters the draft automatically.
          // Cross-code saved swatches require explicit adoption.
          if (!['persistent_rule', 'brand_code_rule'].includes(lookup.source) || lookup.hasNameConflict || lookup.hasSwatchConflict) {
            return current;
          }
          const swatch = splitColourHexValue(lookup.colourHex);
          return {
            ...current,
            nextColourName: current.colourNameTouched
              ? current.nextColourName
              : lookup.colourName ?? current.nextColourName,
            nextColourHex: current.colourHexTouched || !swatch.hasStoredHex ? current.nextColourHex : swatch.hex1,
            nextColourHex2: current.colourHexTouched || !swatch.hasStoredHex ? current.nextColourHex2 : swatch.hex2,
            isDualSwatch: current.colourHexTouched || !swatch.hasStoredHex ? current.isDualSwatch : swatch.isDual,
          };
        });
      }).catch((error: unknown) => {
        if (colourCodeLookupRequestRef.current !== requestId) return;
        setColourCodeRuleLookup(null);
        setColourCodeEditorError(getErrorMessage(error));
      }).finally(() => {
        if (colourCodeLookupRequestRef.current === requestId) {
          setLoadingColourCodeRuleLookup(false);
        }
      });
    }, BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS);
    return () => window.clearTimeout(timer);
  }, [colourCodeEditorTargetKey, colourCodeEditor?.nextColourCode, colourCodeEditor?.nextColourName]);

  useEffect(() => {
    const brand = String(addColourEditor?.brand || "").trim();
    const colourCode = String(addColourEditor?.colourCode || "").trim().toUpperCase();
    const colourName = String(addColourEditor?.colourName || "").trim();
    const targetKey = addColourEditorTargetKey;
    const requestId = ++addColourLookupRequestRef.current;
    setAddColourRuleLookup(null);
    setAddColourEditorError("");
    if (!brand || !/^[A-Z0-9]{1,4}$/.test(colourCode)) {
      setAddColourRuleLookup(null);
      setLoadingAddColourRuleLookup(false);
      return undefined;
    }
    setLoadingAddColourRuleLookup(true);
    const timer = window.setTimeout(() => {
      void api.lookupOrderGeniusColourHexRule(brand, colourCode, colourName).then((lookup) => {
        if (addColourLookupRequestRef.current !== requestId) return;
        setAddColourRuleLookup(lookup);
        setAddColourEditor((current) => {
          if (
            !current
            || getBomAddColourEditorTargetKey(current) !== targetKey
            || current.colourCode.trim().toUpperCase() !== colourCode
          ) return current;
          if (!['persistent_rule', 'brand_code_rule'].includes(lookup.source) || lookup.hasNameConflict || lookup.hasSwatchConflict) {
            return current;
          }
          const swatch = splitColourHexValue(lookup.colourHex);
          return {
            ...current,
            colourName: current.colourNameTouched
              ? current.colourName
              : lookup.colourName ?? current.colourName,
            colourHex: current.colourHexTouched ? current.colourHex : swatch.hex1,
            colourHex2: current.colourHexTouched ? current.colourHex2 : swatch.hex2,
            isDualSwatch: current.colourHexTouched ? current.isDualSwatch : swatch.isDual,
          };
        });
      }).catch((error: unknown) => {
        if (addColourLookupRequestRef.current !== requestId) return;
        setAddColourRuleLookup(null);
        setAddColourEditorError(getErrorMessage(error));
      }).finally(() => {
        if (addColourLookupRequestRef.current === requestId) {
          setLoadingAddColourRuleLookup(false);
        }
      });
    }, BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS);
    return () => window.clearTimeout(timer);
  }, [addColourEditorTargetKey, addColourEditor?.colourCode, addColourEditor?.colourName]);

  const replaceFinanceRow = (
    rows: CountryMaterialFinanceRow[],
    nextRow: CountryMaterialFinanceRow,
  ): CountryMaterialFinanceRow[] =>
    rows.map((row) => row.materialCode === nextRow.materialCode ? nextRow : row);

  const closeFinanceDrawer = () => {
    setFinanceDrawerScope(null);
    setFinanceDrawerFlipped(false);
    setFinanceDrawerRows([]);
    setFinanceDrawerLoading(false);
    setFinanceError("");
  };

  const closeFinanceQuickCard = () => {
    setFinanceQuickCard(null);
    setFinanceQuickFlipped(false);
    setFinanceQuickRows([]);
    setFinanceQuickLoading(false);
    setFinanceError("");
  };

  const buildFinanceDrawerScope = (
    countryCode: string,
    brand: string,
    modelName: string,
    powertrain: string,
    version?: string,
  ): BomFinanceDrawerScope => {
    return {
      countryCode,
      brand,
      modelName,
      powertrain,
      version,
    };
  };

  const openFinanceQuickCard = async (card: BomFinanceQuickCard) => {
    closeFinanceDrawer();
    setFinanceQuickCard(card);
    setFinanceQuickFlipped(false);
    setFinanceQuickRows([]);
    setFinanceError("");
    setFinanceQuickLoading(true);
    try {
      const rows = await api.listCountryMaterialFinance({
        country: card.countryCode,
        materialCodes: [card.materialCode],
      });
      setFinanceQuickRows(rows.items);
    } catch (err) {
      setFinanceError(getErrorMessage(err));
    } finally {
      setFinanceQuickLoading(false);
    }
  };

  const openFinanceDrawer = async (
    scope: BomFinanceDrawerScope,
    options: { animateFlip: boolean } = { animateFlip: true },
  ) => {
    setFinanceQuickCard(null);
    setFinanceQuickRows([]);
    setFinanceQuickFlipped(false);
    setFinanceDrawerScope(scope);
    if (options.animateFlip) setFinanceDrawerFlipped(false);
    setFinanceDrawerRows([]);
    setFinanceError("");
    setFinanceDrawerLoading(true);
    try {
      const rows = await api.listCountryMaterialFinance({
        country: scope.countryCode,
        brand: scope.brand,
        model: scope.modelName,
        powertrain: scope.powertrain,
        version: scope.version,
      });
      setFinanceDrawerRows(rows.items);
    } catch (err) {
      setFinanceError(getErrorMessage(err));
    } finally {
      setFinanceDrawerLoading(false);
      if (options.animateFlip) {
        window.setTimeout(() => setFinanceDrawerFlipped(true), 60);
      } else {
        setFinanceDrawerFlipped(true);
      }
    }
  };

  const handleFinanceDrawerCountryChange = async (countryCode: string) => {
    if (!financeDrawerScope) return;
    await openFinanceDrawer(
      buildFinanceDrawerScope(
        countryCode,
        financeDrawerScope.brand,
        financeDrawerScope.modelName,
        financeDrawerScope.powertrain,
        financeDrawerScope.version,
      ),
      { animateFlip: false },
    );
  };

  const handleFinanceSave = async (
    row: CountryMaterialFinanceRow,
    update: CountryMaterialFinanceUpdate,
  ) => {
    setSavingFinanceMaterialCode(row.materialCode);
    setFinanceError("");
    try {
      const saved = await api.updateMaterialCountryFinance(row.materialCode, update);
      setFinanceQuickRows((current) => replaceFinanceRow(current, saved));
      setFinanceDrawerRows((current) => replaceFinanceRow(current, saved));
      scheduleLoad(200);
    } catch (err) {
      setFinanceError(getErrorMessage(err));
      throw err;
    } finally {
      setSavingFinanceMaterialCode(null);
    }
  };

  // Clear pending deletes after 3s timeout
  useEffect(() => {
    if (pendingDeletes.size === 0) return;
    if (pendingDeleteTimer.current) clearTimeout(pendingDeleteTimer.current);
    pendingDeleteTimer.current = setTimeout(() => setPendingDeletes(new Set()), 3000);
    return () => { if (pendingDeleteTimer.current) clearTimeout(pendingDeleteTimer.current); };
  }, [pendingDeletes]);

  // Debounced search — auto-triggers 1.2s after user stops typing
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(searchText.trim()), 1200);
    return () => clearTimeout(timer);
  }, [searchText]);

  useEffect(() => {
    if (skipNextDebouncedLoadRef.current) {
      skipNextDebouncedLoadRef.current = false;
      return;
    }
    load(debouncedSearch);
  }, [debouncedSearch, load]);

  useEffect(() => {
    setCachedPageValue<BomAdminPageCache>(
      BOM_ADMIN_PAGE_CACHE_KEY,
      {
        searchText,
        toolsFlipped,
        showAddMaterial,
        expandedGroups: [...expandedGroups],
        editingBoms: [...editingBoms],
        bulkFobEditors,
        copyCountryForm,
        adjustCountryForm,
      },
      BOM_ADMIN_PAGE_CACHE_TTL_MS,
    );
  }, [
    adjustCountryForm,
    bulkFobEditors,
    copyCountryForm,
    editingBoms,
    expandedGroups,
    searchText,
    showAddMaterial,
    toolsFlipped,
  ]);

  useEffect(() => {
    if (!copyDraftFocusKey) return;
    const timer = window.setTimeout(() => {
      const input = copyDraftInputRefs.current[copyDraftFocusKey];
      if (!input) return;
      input.focus();
      const value = input.value;
      const suffixMatch = value.match(/\d+$/);
      if (suffixMatch && typeof suffixMatch.index === "number") {
        input.setSelectionRange(suffixMatch.index, value.length);
      } else {
        input.select();
      }
      setCopyDraftFocusKey(null);
    }, 50);
    return () => window.clearTimeout(timer);
  }, [copyDraftFocusKey]);

  useEffect(() => {
    if (!showAddMaterial) return;
    const timer = window.setTimeout(() => {
      materialCodeInputRef.current?.focus();
      materialCodeInputRef.current?.select();
    }, 50);
    return () => window.clearTimeout(timer);
  }, [showAddMaterial]);

  useEffect(() => {
    const bomTemplate = editFob?.bomTemplate?.trim();
    const countryCode = editFob?.countryCode.trim().toUpperCase();
    if (!bomTemplate || !countryCode || !bomTemplate.includes("**")) {
      setFobPeriods([]);
      setFobPeriodError("");
      setFobPeriodDraft(EMPTY_BOM_FOB_PERIOD_DRAFT);
      return;
    }
    let cancelled = false;
    setFobPeriods([]);
    setPeriodDeletePreview(null);
    setFobPeriodDraft(EMPTY_BOM_FOB_PERIOD_DRAFT);
    setFobPeriodsLoading(true);
    setFobPeriodError("");
    void api.listBomTemplateFobPeriods({ bomTemplate, countryCode })
      .then((response) => {
        if (!cancelled) setFobPeriods(response.periods);
      })
      .catch((error: unknown) => {
        if (!cancelled) setFobPeriodError(getErrorMessage(error));
      })
      .finally(() => {
        if (!cancelled) setFobPeriodsLoading(false);
      });
    return () => { cancelled = true; };
  }, [editFob?.bomTemplate, editFob?.countryCode]);

  const reloadFobPeriods = async () => {
    const bomTemplate = editFob?.bomTemplate?.trim();
    const countryCode = editFob?.countryCode.trim().toUpperCase();
    if (!bomTemplate || !countryCode) return;
    const response = await api.listBomTemplateFobPeriods({ bomTemplate, countryCode });
    setFobPeriods(response.periods);
  };

  const handleFobPeriodSave = async () => {
    if (!editFob?.bomTemplate || !fobPeriodDraft.validFrom || fobPeriodDraft.baseFobEur === "") return;
    const baseFobEur = Number(fobPeriodDraft.baseFobEur);
    if (!Number.isFinite(baseFobEur) || baseFobEur < 0) {
      setFobPeriodError("Period base FOB must be zero or greater.");
      return;
    }
    setFobPeriodSaving(true);
    setFobPeriodError("");
    try {
      await api.saveBomTemplateFobPeriod({
        periodId: fobPeriodDraft.periodId ?? undefined,
        rowVersion: fobPeriodDraft.rowVersion ?? undefined,
        bomTemplate: editFob.bomTemplate,
        countryCode: editFob.countryCode,
        validFrom: fobPeriodDraft.validFrom,
        validTo: fobPeriodDraft.validTo || null,
        baseFobEur,
        remark: fobPeriodDraft.remark.trim() || null,
      });
      setFobPeriodDraft(EMPTY_BOM_FOB_PERIOD_DRAFT);
      await reloadFobPeriods();
      if (!await load()) throw new Error("Period saved, but BOM refresh failed. Refresh to verify.");
      await onFobChanged?.();
    } catch (error: unknown) {
      setFobPeriodError(getErrorMessage(error));
    } finally {
      setFobPeriodSaving(false);
    }
  };

  const handleFobPeriodDelete = async (period: CountryTemplateFobPeriod, fingerprint?: string) => {
    setFobPeriodSaving(true);
    setFobPeriodError("");
    try {
      const result = await api.deleteBomTemplateFobPeriod(period.periodId, period.rowVersion, fingerprint);
      if (!result.deleted) {
        setPeriodDeletePreview({ ...period, ...result });
        return;
      }
      if (fobPeriodDraft.periodId === period.periodId) {
        setFobPeriodDraft(EMPTY_BOM_FOB_PERIOD_DRAFT);
      }
      await reloadFobPeriods();
      setPeriodDeletePreview(null);
      if (!await load()) throw new Error("Period removed, but BOM refresh failed. Refresh to verify.");
      await onFobChanged?.();
    } catch (error: unknown) {
      setFobPeriodError(getErrorMessage(error));
    } finally {
      setFobPeriodSaving(false);
    }
  };

  const handleFobPeriodEdit = (period: CountryTemplateFobPeriod) => {
    setFobPeriodError("");
    setFobPeriodDraft({
      periodId: period.periodId,
      rowVersion: period.rowVersion,
      validFrom: period.validFrom,
      validTo: period.validTo ?? "",
      baseFobEur: String(period.baseFobEur),
      remark: period.remark ?? "",
    });
  };

  const handleFobSave = async () => {
    if (!editFob) return;
    const remark = editFob.fob != null && editFob.fob > 0 ? editFob.remark.trim() : "";
    const nextFob = editFob.fob != null && editFob.fob > 0 ? editFob.fob : null;
    const originalFob = editFob.originalFob != null && editFob.originalFob > 0 ? editFob.originalFob : null;
    const didChangeFob = nextFob !== originalFob;
    try {
      if (editFob.bomTemplate?.includes("**")) {
        const result = await api.updateBomTemplateFob({
          bomTemplate: editFob.bomTemplate,
          materialCodes: editFob.materialCodes,
          countryCode: editFob.countryCode,
          baseFobEur: nextFob,
          remark,
        });
        const detailUpdates: BomFobPatch[] = result.details.flatMap((detail) => {
          const materialCode = String(detail.materialCode || "");
          if (!materialCode) return [];
          return [{
            materialCode,
            countryCode: editFob.countryCode,
            baseFobEur: result.baseFobEur,
            colourSurchargeEur: detail.colourSurchargeEur == null ? null : Number(detail.colourSurchargeEur),
            finalFobEur: detail.finalFobEur == null ? null : Number(detail.finalFobEur),
            fobSourceMode: result.baseFobEur == null ? null : "template_base",
            fobSourceCountryCode: null,
            remark,
          }];
        });
        patchBomFobs(detailUpdates);
        setEditFob(null);
        scheduleLoad(1200);
        onFobChanged?.();
        return;
      }
      const responses: BomFobSaveResponse[] = [];
      for (const mc of editFob.materialCodes) {
        responses.push(await api.updateSkuFob(mc, { countryCode: editFob.countryCode, baseFobEur: editFob.fob, remark }) as BomFobSaveResponse);
      }
      const responseByMaterial = new Map(
        responses.map((response) => [bomMaterialKey(response.materialCode), response]),
      );
      patchBomFobs(editFob.materialCodes.map((materialCode) => {
        const response = responseByMaterial.get(bomMaterialKey(materialCode));
        return {
          materialCode,
          countryCode: editFob.countryCode,
          baseFobEur: response?.baseFobEur ?? nextFob,
          colourSurchargeEur: response?.colourSurchargeEur ?? null,
          finalFobEur: response?.finalFobEur ?? nextFob,
          paymentTermCode: response?.paymentTermCode ?? null,
          fobSourceMode: response?.fobSourceMode ?? (didChangeFob ? "manual_edit" : editFob.fobSourceMode ?? null),
          fobSourceCountryCode: response?.fobSourceCountryCode ?? (didChangeFob ? null : editFob.fobSourceCountryCode ?? null),
          remark: response?.remark ?? remark,
        };
      }));
      setEditFob(null);
      scheduleLoad(1200);
      onFobChanged?.();
    } catch (e) { alert(getErrorMessage(e)); }
  };

  const resetNewMaterial = () => {
    setNewMaterial(EMPTY_ADD_MATERIAL);
    setAddMaterialError("");
    setAddMaterialNotice("");
  };

  const handleCreateMaterial = async () => {
    const { drafts, errors, isBatch } = buildMaterialDrafts(newMaterial);
    if (errors.length > 0) {
      setAddMaterialError(errors.join("; "));
      return;
    }
    if (drafts.length === 0) return;

    try {
      setAddMaterialError("");
      setAddMaterialNotice("");
      let created = 0;
      const failures: string[] = [];
      for (const draft of drafts) {
        try {
          await api.createMaterialSku({
            materialCode: draft.materialCode,
            brand: draft.brand,
            modelName: draft.modelName,
            version: draft.version,
            colour: draft.colour,
            colourCode: draft.colourCode,
            colourHex: draft.colourHex ?? undefined,
            colourType: "single",
            colourTier: "single",
            powertrain: draft.powertrain,
            bomTemplate: isBatch ? newMaterial.materialCode.trim().toUpperCase() : draft.materialCode,
          });
          created += 1;
        } catch (e) {
          failures.push(`${draft.materialCode}: ${getErrorMessage(e)}`);
        }
      }
      if (created > 0) scheduleLoad(100);
      if (failures.length > 0) {
        setAddMaterialNotice(created > 0 ? `Created ${created}/${drafts.length}.` : "");
        setAddMaterialError(failures.slice(0, 3).join("; "));
        return;
      }
      setShowAddMaterial(false);
      setAddMaterialNotice(isBatch ? `Created ${created} materials.` : "");
      resetNewMaterial();
      scheduleLoad(100);
    } catch(e) {
      setAddMaterialError(getErrorMessage(e));
    }
  };

  const resolveMaterialCodeFromTemplate = (
    bomTemplate: string,
    colourCode: string,
    fallbackCode: string,
  ) => {
    const normalizedTemplate = bomTemplate.trim().toUpperCase();
    if (!normalizedTemplate) return "";
    if (!normalizedTemplate.includes("**")) return normalizedTemplate;
    const normalizedColourCode = colourCode.trim().toUpperCase();
    if (!normalizedColourCode) return fallbackCode.trim().toUpperCase();
    return normalizedTemplate.replace("**", normalizedColourCode);
  };

  const dismissCopyDraft = (draftKey: string) => {
    setCopyDrafts((prev) => {
      const next = { ...prev };
      delete next[draftKey];
      return next;
    });
    setCopyDraftErrors((prev) => {
      const next = { ...prev };
      delete next[draftKey];
      return next;
    });
    setCopyDraftFocusKey((current) => (current === draftKey ? null : current));
  };

  const updateCopyDraft = (
    draftKey: string,
    updater: (draft: BomCopyDraft) => BomCopyDraft,
  ) => {
    setCopyDrafts((prev) => {
      const current = prev[draftKey];
      if (!current) return prev;
      return { ...prev, [draftKey]: updater(current) };
    });
  };

  const collectCountryCodes = useCallback((codes: string[]): string[] => {
    const seen = new Set<string>();
    const result: string[] = [];
    for (const code of codes) {
      const normalized = code.trim().toUpperCase();
      if (!normalized || seen.has(normalized)) continue;
      seen.add(normalized);
      result.push(normalized);
    }
    return result;
  }, []);

  const getDraftCountryCodes = (draft: BomCopyDraft): string[] => {
    return collectCountryCodes([
      ...sortedCountries,
      ...Object.keys(draft.fobByCountry || {}).sort(),
    ]).filter((country) => sortedCountries.includes(country));
  };

  const getFilledDraftCountryCodes = (draft: BomCopyDraft): string[] =>
    getDraftCountryCodes(draft).filter(
      (code) => getDraftBaseFob(draft.fobByCountry[code]) != null,
    );

  const setCopyDraftCountryScope = (
    draftKey: string,
    scope: "all" | "filled" | "clear",
  ) => {
    updateCopyDraft(draftKey, (draft) => ({
      ...draft,
      bulkSelectedCountries:
        scope === "all"
          ? getDraftCountryCodes(draft)
          : scope === "filled"
            ? getFilledDraftCountryCodes(draft)
            : [],
    }));
    setCopyDraftErrors((prev) => {
      if (!prev[draftKey]) return prev;
      const next = { ...prev };
      delete next[draftKey];
      return next;
    });
  };

  const toggleCopyDraftCountry = (
    draftKey: string,
    countryCode: string,
    checked: boolean,
  ) => {
    updateCopyDraft(draftKey, (draft) => {
      const existing = new Set(draft.bulkSelectedCountries);
      if (checked) existing.add(countryCode);
      else existing.delete(countryCode);
      return {
        ...draft,
        bulkSelectedCountries: getDraftCountryCodes(draft).filter((code) =>
          existing.has(code),
        ),
      };
    });
  };

  const applyCopyDraftFobDelta = (
    draftKey: string,
    quickDelta?: number,
  ) => {
    const draft = copyDrafts[draftKey];
    if (!draft) return;
    const numericDelta =
      quickDelta ?? Number(draft.bulkDeltaEur.trim());
    if (!Number.isFinite(numericDelta) || numericDelta === 0) {
      setCopyDraftErrors((prev) => ({
        ...prev,
        [draftKey]: "Enter a non-zero FOB delta first.",
      }));
      return;
    }
    const selectedCountries = draft.bulkSelectedCountries.filter((code) => sortedCountries.includes(code));
    if (selectedCountries.length === 0) {
      setCopyDraftErrors((prev) => ({
        ...prev,
        [draftKey]: "Select at least one country for the FOB delta.",
      }));
      return;
    }

    const nextFobByCountry: Record<string, BomDraftFobEntry> = {
      ...draft.fobByCountry,
    };
    let changedCountries = 0;
    for (const code of selectedCountries) {
      const currentEntry = nextFobByCountry[code];
      const baseFob = getDraftBaseFob(currentEntry);
      if (baseFob == null) continue;
      const nextValue = Math.max(
        0,
        Number((baseFob + numericDelta).toFixed(2)),
      );
      nextFobByCountry[code] = {
        ...currentEntry,
        uploadedFobEur: nextValue,
        finalFobEur: nextValue,
      };
      changedCountries += 1;
    }

    if (changedCountries === 0) {
      setCopyDraftErrors((prev) => ({
        ...prev,
        [draftKey]: "Selected countries do not have a source FOB yet.",
      }));
      return;
    }

    setCopyDrafts((prev) => {
      const current = prev[draftKey];
      if (!current) return prev;
      return {
        ...prev,
        [draftKey]: {
          ...current,
          fobByCountry: nextFobByCountry,
          bulkSelectedCountries: current.bulkSelectedCountries.filter(
            (code) => getDraftBaseFob(nextFobByCountry[code]) != null,
          ),
          bulkDeltaEur:
            quickDelta == null ? current.bulkDeltaEur : String(numericDelta),
        },
      };
    });

    setCopyDraftErrors((prev) => {
      if (!prev[draftKey]) return prev;
      const next = { ...prev };
      delete next[draftKey];
      return next;
    });
  };

  const getBomCountryCodes = (allSkus: any[]): string[] =>
    collectCountryCodes([
      ...sortedCountries,
      ...allSkus.flatMap((sku: any) =>
        Object.keys((sku?.fobByCountry as Record<string, BomDraftFobEntry>) || {}),
      ),
    ]).filter((country) => sortedCountries.includes(country));

  const getFilledBomCountryCodes = (allSkus: any[]): string[] =>
    getBomCountryCodes(allSkus).filter((countryCode) =>
      allSkus.some((sku: any) => getDraftBaseFob(sku?.fobByCountry?.[countryCode]) != null),
    );

  const getUnfilledBomCountryCodes = (allSkus: any[]): string[] =>
    getBomCountryCodes(allSkus).filter((countryCode) =>
      !allSkus.some((sku: any) => getDraftBaseFob(sku?.fobByCountry?.[countryCode]) != null),
    );

  const getBulkFobEditor = (
    bomKey: string,
    allSkus: any[],
  ): BomBulkFobEditor => {
    const current = bulkFobEditors[bomKey] || {
      deltaEur: "",
      selectedCountries: getFilledBomCountryCodes(allSkus),
    };
    return { ...current, selectedCountries: current.selectedCountries.filter((country) => sortedCountries.includes(country)) };
  };

  const updateBulkFobEditor = (
    bomKey: string,
    allSkus: any[],
    updater: (current: BomBulkFobEditor) => BomBulkFobEditor,
  ) => {
    setBulkFobEditors((prev) => {
      const current = prev[bomKey] || {
        deltaEur: "",
        selectedCountries: getFilledBomCountryCodes(allSkus),
      };
      return {
        ...prev,
        [bomKey]: updater(current),
      };
    });
  };

  const setBulkFobCountryScope = (
    bomKey: string,
    allSkus: any[],
    scope: "all" | "filled" | "unfilled" | "clear",
  ) => {
    updateBulkFobEditor(bomKey, allSkus, (current) => ({
      ...current,
      selectedCountries:
        scope === "all"
          ? getBomCountryCodes(allSkus)
          : scope === "filled"
            ? getFilledBomCountryCodes(allSkus)
            : scope === "unfilled"
              ? getUnfilledBomCountryCodes(allSkus)
              : [],
    }));
    setBulkFobErrors((prev) => {
      if (!prev[bomKey]) return prev;
      const next = { ...prev };
      delete next[bomKey];
      return next;
    });
  };

  const toggleBulkFobCountry = (
    bomKey: string,
    allSkus: any[],
    countryCode: string,
    checked: boolean,
  ) => {
    updateBulkFobEditor(bomKey, allSkus, (current) => {
      const selected = new Set(current.selectedCountries);
      if (checked) selected.add(countryCode);
      else selected.delete(countryCode);
      return {
        ...current,
        selectedCountries: getBomCountryCodes(allSkus).filter((code) =>
          selected.has(code),
        ),
      };
    });
  };

  const applyBulkFobDelta = async (
    bomKey: string,
    allSkus: any[],
    quickDelta?: number,
  ) => {
    const editor = getBulkFobEditor(bomKey, allSkus);
    const numericDelta = quickDelta ?? Number(editor.deltaEur.trim());
    if (!Number.isFinite(numericDelta) || numericDelta === 0) {
      setBulkFobErrors((prev) => ({
        ...prev,
        [bomKey]: "Enter a non-zero FOB delta first.",
      }));
      return;
    }
    if (editor.selectedCountries.length === 0) {
      setBulkFobErrors((prev) => ({
        ...prev,
        [bomKey]: "Select at least one country for the FOB delta.",
      }));
      return;
    }

    if (bomKey.includes("**")) {
      setBulkFobSavingKey(bomKey);
      try {
        const templateUpdates: BomFobPatch[] = [];
        for (const countryCode of editor.selectedCountries) {
          const sourceSku = allSkus.find((sku: any) => getDraftBaseFob(sku?.fobByCountry?.[countryCode]) != null);
          const currentBase = getDraftBaseFob(sourceSku?.fobByCountry?.[countryCode]);
          if (currentBase == null) continue;
          const result = await api.updateBomTemplateFob({
            bomTemplate: bomKey,
            materialCodes: allSkus.map((sku: any) => String(sku.materialCode || "")),
            countryCode,
            baseFobEur: Math.max(0, Number((currentBase + numericDelta).toFixed(2))),
            remark: getBomCountryFobRemark(allSkus, countryCode) || null,
          });
          for (const detail of result.details) {
            const materialCode = String(detail.materialCode || "");
            if (!materialCode) continue;
            templateUpdates.push({
              materialCode,
              countryCode,
              baseFobEur: result.baseFobEur,
              colourSurchargeEur: detail.colourSurchargeEur == null ? null : Number(detail.colourSurchargeEur),
              finalFobEur: detail.finalFobEur == null ? null : Number(detail.finalFobEur),
              fobSourceMode: "template_base_country_adjust",
              remark: getBomCountryFobRemark(allSkus, countryCode),
            });
          }
        }
        if (templateUpdates.length === 0) throw new Error("Selected countries do not have a template base yet.");
        patchBomFobs(templateUpdates);
        updateBulkFobEditor(bomKey, allSkus, (current) => ({ ...current, deltaEur: String(numericDelta) }));
        setBulkFobErrors((prev) => {
          if (!prev[bomKey]) return prev;
          const next = { ...prev };
          delete next[bomKey];
          return next;
        });
        scheduleLoad(1200);
        onFobChanged?.();
      } catch (err) {
        setBulkFobErrors((prev) => ({ ...prev, [bomKey]: getErrorMessage(err) }));
      } finally {
        setBulkFobSavingKey((current) => (current === bomKey ? null : current));
      }
      return;
    }

    const updates: Array<{
      materialCode: string;
      countryCode: string;
      finalFobEur: number;
      paymentTermCode?: string | null;
    }> = [];
    for (const sku of allSkus) {
      for (const countryCode of editor.selectedCountries) {
        const fob = sku?.fobByCountry?.[countryCode] as BomDraftFobEntry | undefined;
        const baseFob = getDraftBaseFob(fob);
        if (baseFob == null) continue;
        updates.push({
          materialCode: String(sku.materialCode || ""),
          countryCode,
          finalFobEur: Math.max(0, Number((baseFob + numericDelta).toFixed(2))),
          paymentTermCode: fob?.paymentTermCode,
        });
      }
    }

    if (updates.length === 0) {
      setBulkFobErrors((prev) => ({
        ...prev,
        [bomKey]: "Selected countries do not have a source FOB yet.",
      }));
      return;
    }

    setBulkFobSavingKey(bomKey);
    try {
      const savedUpdates: BomFobPatch[] = [];
      for (const update of updates) {
        const saved: BomFobSaveResponse = await api.updateSkuFob(update.materialCode, {
          countryCode: update.countryCode,
          baseFobEur: update.finalFobEur,
          paymentTermCode: update.paymentTermCode ?? undefined,
        });
        savedUpdates.push(saved);
      }
      patchBomFobs(savedUpdates);
      if (quickDelta != null) {
        updateBulkFobEditor(bomKey, allSkus, (current) => ({
          ...current,
          deltaEur: String(numericDelta),
        }));
      }
      setBulkFobErrors((prev) => {
        if (!prev[bomKey]) return prev;
        const next = { ...prev };
        delete next[bomKey];
        return next;
      });
      scheduleLoad(1200);
      onFobChanged?.();
    } catch (err) {
      setBulkFobErrors((prev) => ({
        ...prev,
        [bomKey]: getErrorMessage(err),
      }));
    } finally {
      setBulkFobSavingKey((current) => (current === bomKey ? null : current));
    }
  };

  const hasPositiveFob = (sku: any): boolean =>
    Object.values((sku?.fobByCountry as Record<string, BomDraftFobEntry>) || {}).some(
      (fob) => getDraftBaseFob(fob) != null,
    );

  const pickFobSourceSkuForNewColour = (tierSkus: any[], allSkus: any[]): any | null =>
    tierSkus.find(hasPositiveFob) || allSkus.find(hasPositiveFob) || null;

  const handleCopyMaterialFromBom = (
    draftKey: string,
    bomTemplate: string,
    ref: any,
    allSkus: any[],
    sourceDisplayLabel?: string,
    selectedCountryCodes?: string[],
  ) => {
    if (!String(ref.powertrain || "").trim()) {
      setProductSaveMessages((current) => ({ ...current, [draftKey]: {
        kind: "error", text: "Choose and save the source template powertrain before copying / 请先确认并保存源模板动力类型",
      } }));
      return;
    }
    const initialTemplate = String(
      bomTemplate || deriveMaterialTemplate(allSkus.map((sku: any) => String(sku.materialCode || "")).filter(Boolean)) || ref.materialCode || "",
    ).trim().toUpperCase();
    const sourceInfo = ref.sourcePayload || {};
    const modelName = String(ref.modelName || "");
    const templateBaseSource = allSkus.find((sku: any) => {
      if (getEffectiveColourTier(sku) === "single" && hasPositiveFob(sku)) return true;
      return Object.values((sku?.fobByCountry as Record<string, BomDraftFobEntry>) || {}).some(
        (fob) => fob?.baseFobEur != null,
      );
    });
    const baseDraft: BomCopyDraft = {
      draftKey,
      sourceBomTemplate: initialTemplate,
      sourceDisplayLabel: sourceDisplayLabel || formatBomSourceLabel(
        modelName,
        ref.sourceSheetName || sourceInfo.sheet_name,
        ref.sourceRowNumber ?? sourceInfo.row_index,
      ),
      bomTemplate: initialTemplate,
      brand: String(ref.brand || ""),
      modelName,
      version: String(ref.version || ""),
      powertrain: String(ref.powertrain),
      interiorColorName: String(ref.interiorColorName || ""),
      editionTag: ref.editionTag ? String(ref.editionTag) : null,
      lifecycleStatus: String(ref.lifecycleStatus || "active"),
      effectiveFrom: ref.effectiveFrom ? String(ref.effectiveFrom) : null,
      effectiveTo: ref.effectiveTo ? String(ref.effectiveTo) : null,
      remark: getBomTemplateRemark(allSkus),
      fobByCountry: Object.fromEntries(
        Object.entries(templateBaseSource?.fobByCountry || {}).map(([countryCode, fob]) => [
          countryCode,
          { ...((fob as BomDraftFobEntry) || {}) },
        ]),
      ),
      bulkDeltaEur: "",
      bulkSelectedCountries: [],
      skus: allSkus.map((sku: any) => ({
        sourceMaterialCode: String(sku.materialCode || ""),
        colour: String(sku.colour || ""),
        colourCode: String(sku.colourCode || "").toUpperCase(),
        colourType: String(sku.colourType || "single"),
        colourTier: inferBomAdminColourTier({
          colour: sku.colour,
          colourCode: sku.colourCode,
          colourType: sku.colourType,
          colourTier: sku.colourTier,
          colourHex: sku.colourHex,
          editionTag: sku.editionTag,
        }),
        colourHex: sku.colourHex || null,
      })),
    };
    const nextDraft: BomCopyDraft = {
      ...baseDraft,
      bulkSelectedCountries: Array.isArray(selectedCountryCodes)
        ? getDraftCountryCodes(baseDraft).filter((code) => selectedCountryCodes.includes(code))
        : getFilledDraftCountryCodes(baseDraft),
    };
    setCopyDrafts((prev) => ({ ...prev, [draftKey]: nextDraft }));
    setCopyDraftErrors((prev) => {
      const next = { ...prev };
      delete next[draftKey];
      return next;
    });
    setShowAddMaterial(false);
    resetNewMaterial();
    setCopyDraftFocusKey(draftKey);
  };

  const handleSaveCopiedBom = async (draftKey: string) => {
    const draft = copyDrafts[draftKey];
    if (!draft) return;
    if (!draft.powertrain.trim()) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "Choose a powertrain before saving / 请先选择动力类型" }));
      return;
    }
    const normalizedTemplate = draft.bomTemplate.trim().toUpperCase();
    if (!normalizedTemplate) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "BOM template is required." }));
      return;
    }
    if (draft.skus.length > 1 && !normalizedTemplate.includes("**")) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "Multiple colours need a BOM template with **." }));
      return;
    }
    const isTemplateBaseCopy = normalizedTemplate.includes("**");

    const targetCodes = draft.skus.map((sku) =>
      resolveMaterialCodeFromTemplate(normalizedTemplate, sku.colourCode, sku.sourceMaterialCode),
    );
    if (targetCodes.some((code) => !code)) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "Every copied colour needs a valid material code." }));
      return;
    }
    if (new Set(targetCodes).size !== targetCodes.length) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "This BOM template generates duplicate material codes." }));
      return;
    }
    const existingTargetCode = targetCodes.find((code) =>
      skus.some((sku) => String(sku.materialCode || "").toUpperCase() === code),
    );
    if (existingTargetCode) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: `Material code already exists: ${existingTargetCode}` }));
      return;
    }

    setCopyDraftSavingKey(draftKey);
    setCopyDraftErrors((prev) => {
      const next = { ...prev };
      delete next[draftKey];
      return next;
    });
    try {
      const createdSkus: any[] = [];
      for (const sku of draft.skus) {
        const materialCode = resolveMaterialCodeFromTemplate(
          normalizedTemplate,
          sku.colourCode,
          sku.sourceMaterialCode,
        );
        const effectiveColourTier = inferBomAdminColourTier(sku);
        await api.createMaterialSku({
          materialCode,
          bomTemplate: normalizedTemplate,
          brand: draft.brand,
          modelName: draft.modelName,
          version: draft.version,
          colour: sku.colour,
          colourCode: sku.colourCode,
          colourType: sku.colourType || effectiveColourTier || "single",
          colourTier: effectiveColourTier,
          powertrain: draft.powertrain,
          sourceBomTemplate: draft.sourceBomTemplate,
          lifecycleStatus: draft.lifecycleStatus || "active",
          effectiveFrom: draft.effectiveFrom,
          effectiveTo: draft.effectiveTo,
          remark: draft.remark || undefined,
        });
        if (effectiveColourTier !== "single") {
          await api.updateColourTier(materialCode, effectiveColourTier);
        }
        if (sku.colourHex) {
          await api.updateColourHex(materialCode, sku.colourHex);
        }
        if (draft.interiorColorName || draft.editionTag) {
          await api.updateSkuInterior(materialCode, {
            interiorColorName: draft.interiorColorName || null,
            editionTag: draft.editionTag || null,
          });
        }
        if (!isTemplateBaseCopy) {
          for (const countryCode of draft.bulkSelectedCountries) {
            const fob = draft.fobByCountry[countryCode];
            const baseFob = getDraftBaseFob(fob);
            if (baseFob == null) continue;
            await api.updateSkuFob(materialCode, {
              countryCode,
              baseFobEur: Number(baseFob),
              paymentTermCode: fob?.paymentTermCode ?? undefined,
              remark: fob?.remark ?? null,
            });
          }
        }
        createdSkus.push({
          materialCode,
          bomTemplate: normalizedTemplate,
          brand: draft.brand,
          modelName: draft.modelName,
          version: draft.version,
          colour: sku.colour,
          colourCode: sku.colourCode,
          colourType: sku.colourType || "single",
          colourTier: effectiveColourTier,
          colourHex: sku.colourHex,
          powertrain: draft.powertrain,
          interiorColorName: draft.interiorColorName || null,
          editionTag: draft.editionTag,
          lifecycleStatus: draft.lifecycleStatus || "active",
          effectiveFrom: draft.effectiveFrom,
          effectiveTo: draft.effectiveTo,
          remark: draft.remark,
          rowVersion: 1,
          fobByCountry: isTemplateBaseCopy
            ? {}
            : Object.fromEntries(
                draft.bulkSelectedCountries.flatMap((countryCode) => {
                  const fob = draft.fobByCountry[countryCode];
                  const baseFob = getDraftBaseFob(fob);
                  return baseFob == null
                    ? []
                    : [[countryCode, {
                      ...fob,
                      uploadedFobEur: Number(baseFob),
                      finalFobEur: Number(baseFob),
                      fobSourceMode: "manual_edit",
                    }]];
                }),
              ),
        });
      }
      if (isTemplateBaseCopy) {
        for (const countryCode of draft.bulkSelectedCountries) {
          const baseFob = getDraftBaseFob(draft.fobByCountry[countryCode]);
          if (baseFob == null) continue;
          const result = await api.updateBomTemplateFob({
            bomTemplate: normalizedTemplate,
            materialCodes: createdSkus.map((sku) => String(sku.materialCode || "")),
            countryCode,
            baseFobEur: Number(baseFob),
            remark: draft.remark || null,
          });
          const detailsByMaterial = new Map(
            result.details.map((detail) => [String(detail.materialCode || ""), detail]),
          );
          for (const createdSku of createdSkus) {
            const detail = detailsByMaterial.get(String(createdSku.materialCode || ""));
            if (!detail) continue;
            createdSku.fobByCountry = {
              ...(createdSku.fobByCountry || {}),
              [countryCode]: {
                ...(draft.fobByCountry[countryCode] || {}),
                baseFobEur: result.baseFobEur,
                uploadedFobEur: result.baseFobEur,
                colourSurchargeEur: detail.colourSurchargeEur == null ? null : Number(detail.colourSurchargeEur),
                finalFobEur: detail.finalFobEur == null ? null : Number(detail.finalFobEur),
                fobSourceMode: "template_base",
              },
            };
          }
        }
      }
      setSkus((current) => {
        const existingCodes = new Set(current.map((sku) => bomMaterialKey(sku?.materialCode)));
        const additions = createdSkus.filter((sku) => !existingCodes.has(bomMaterialKey(sku?.materialCode)));
        return additions.length > 0 ? [...current, ...additions] : current;
      });
      dismissCopyDraft(draftKey);
      scheduleLoad(1200);
      if (createdSkus.some((sku) => Object.keys(sku.fobByCountry || {}).length > 0)) {
        onFobChanged?.();
      }
    } catch (err) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: getErrorMessage(err) }));
    } finally {
      setCopyDraftSavingKey((current) => (current === draftKey ? null : current));
    }
  };

  const handleSaveColourSurcharges = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const updates: Array<{ brand: string; colourType: string; surchargeEur: number }> = [];
    for (const brand of BOM_ADMIN_SURCHARGE_BRANDS) {
      for (const type of BOM_ADMIN_SURCHARGE_TYPES) {
        const key = colourSurchargeKey(brand, type.value);
        const raw = (colourSurchargeDrafts[key] ?? "").trim();
        const surchargeEur = Number(raw);
        if (!raw || !Number.isFinite(surchargeEur) || surchargeEur < 0) {
          setColourSurchargeStatus(`${brand} ${type.label} needs a non-negative number.`);
          return;
        }
        updates.push({ brand, colourType: type.value, surchargeEur });
      }
    }
    try {
      setSavingColourSurcharges(true);
      setColourSurchargeStatus("");
      for (const update of updates) {
        await api.updateOrderGeniusColourSurcharge(update);
      }
      await loadColourSurcharges();
      setColourSurchargeStatus("Saved colour surcharge rules.");
    } catch (e) {
      setColourSurchargeStatus(getErrorMessage(e));
    } finally {
      setSavingColourSurcharges(false);
    }
  };

  const handleSaveSpecialColourSurcharge = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const brand = specialColourSurchargeDraft.brand.trim().toUpperCase();
    const colourCode = specialColourSurchargeDraft.colourCode.trim().toUpperCase();
    const modelName = specialColourSurchargeDraft.modelName.trim();
    const colourTier = specialColourSurchargeDraft.colourTier;
    const surchargeEur = Number(specialColourSurchargeDraft.surchargeEur.trim());
    if (!brand || !colourCode || !Number.isFinite(surchargeEur) || surchargeEur < 0) {
      setSpecialColourSurchargeStatus("Brand, colour code and a non-negative surcharge are required.");
      return;
    }
    try {
      setSavingSpecialColourSurcharge(true);
      setSpecialColourSurchargeStatus("");
      const result = await api.updateOrderGeniusSpecialColourSurcharge({
        brand,
        modelName: modelName || null,
        colourCode,
        colourTier,
        colourName: specialColourSurchargeDraft.colourName.trim() || null,
        surchargeEur,
      });
      await Promise.all([loadSpecialColourSurcharges(), load()]);
      const repriced = Number(result.reprice?.changed || 0);
      setSpecialColourSurchargeStatus(`Saved ${brand}${modelName ? ` ${modelName}` : ""} ${colourCode}: +${surchargeEur} EUR${repriced ? `; repriced ${repriced} automatic FOB rows` : ""}.`);
      setSpecialColourSurchargeDraft(EMPTY_SPECIAL_COLOUR_SURCHARGE_DRAFT);
    } catch (e) {
      setSpecialColourSurchargeStatus(getErrorMessage(e));
    } finally {
      setSavingSpecialColourSurcharge(false);
    }
  };

  const openColourRuleStandardEditor = (
    rule: ColourHexRule,
    colourName: string,
    colourHex: string,
    acceptCandidate: boolean = false,
  ): void => {
    const swatch = splitColourHexValue(colourHex);
    setColourHexRuleStatus("");
    setColourRuleDetailsStatus(null);
    setColourCodeEditorError("");
    setConfirmingColourEdit(false);
    setColourCodeEditor({
      materialCode: null,
      colourCodeConfirmed: true,
      bomTemplate: null,
      brand: rule.brand,
      skuCount: rule.skuCount,
      correctingCode: false,
      suggestionSource: null,
      swatchAccepted: acceptCandidate && Boolean(colourHex),
      needsSynchronization: rule.hasNameConflict || rule.hasSwatchConflict || rule.fillableSkuCount > 0,
      currentColourName: rule.standardColourName ?? rule.colourName ?? "",
      currentColourHex: rule.standardColourHex,
      storedColourHex: null,
      currentColourCode: rule.colourCode,
      nextColourCode: rule.colourCode,
      nextColourName: colourName,
      nextColourHex: swatch.hex1,
      nextColourHex2: swatch.hex2,
      isDualSwatch: swatch.isDual,
      colourNameTouched: acceptCandidate,
      colourHexTouched: acceptCandidate,
    });
  };

  const handlePreviewColourRuleFills = async () => {
    setShowColourRulePreview(true);
    setLoadingColourRulePreview(true);
    setColourRulePreview(null);
    setColourRuleApplyResult(null);
    setColourRuleActionError("");
    try {
      setColourRulePreview(await api.previewOrderGeniusColourHexRuleFills());
    } catch (error) {
      setColourRuleActionError(getErrorMessage(error));
    } finally {
      setLoadingColourRulePreview(false);
    }
  };

  const handleApplyColourRuleFills = async () => {
    if (!colourRulePreview) return;
    setApplyingColourRulePreview(true);
    setColourRuleActionError("");
    try {
      const result = await api.applyOrderGeniusColourHexRuleFills(
        colourRulePreview.fingerprint,
        colourRulePreview.items.map((item) => item.materialCode),
      );
      setColourRuleApplyResult(result);
      setColourHexRuleStatus(
        `Filled missing fields on ${result.updated} materials / 已补缺 ${result.updated} 条物料；${result.rulesCreated} new shared standards.`,
      );
      await refreshSavedColour(`Filled missing fields on ${result.updated} materials / 已补缺 ${result.updated} 条物料。`);
    } catch (error) {
      setColourRuleActionError(
        getErrorStatus(error) === 409
          ? `Data changed after preview. Refresh the preview before applying. ${getErrorMessage(error)}`
          : getErrorMessage(error),
      );
    } finally {
      setApplyingColourRulePreview(false);
    }
  };

  const openColourCodeEditor = (sku: { materialCode: string; brand?: string; bomTemplate?: string; colourCode?: string; colour?: string; colourHex?: string | null; storedColourHex: string | null; colourCodeConfirmed?: boolean }): void => {
    const currentColourCode = String(sku.colourCode || "").trim().toUpperCase();
    const currentColourName = String(sku.colour || "").trim();
    const currentSwatch = splitColourHexValue(sku.colourHex);
    const rule = colourHexRules.find(rule => rule.brand === sku.brand && rule.colourCode === currentColourCode);
    colourCodeLookupRequestRef.current += 1;
    setColourCodeRuleLookup(null);
    setLoadingColourCodeRuleLookup(false);
    setColourCodeEditorError("");
    setConfirmingColourEdit(false);
    setColourCodeEditor({
      materialCode: String(sku.materialCode || ""),
      colourCodeConfirmed: sku.colourCodeConfirmed !== false,
      bomTemplate: sku.bomTemplate ?? null,
      brand: String(sku.brand || ""),
      skuCount: rule?.skuCount ?? null,
      correctingCode: false,
      suggestionSource: null,
      swatchAccepted: false,
      needsSynchronization: Boolean(rule?.hasNameConflict || rule?.hasSwatchConflict || rule?.fillableSkuCount),
      currentColourName,
      currentColourHex: currentSwatch.hasStoredHex ? String(sku.colourHex) : null,
      storedColourHex: sku.storedColourHex,
      nextColourName: currentColourName,
      currentColourCode,
      nextColourCode: currentColourCode,
      nextColourHex: currentSwatch.hex1,
      nextColourHex2: currentSwatch.hex2,
      isDualSwatch: currentSwatch.isDual,
      colourNameTouched: false,
      colourHexTouched: false,
    });
  };

  const refreshSavedColour = async (notice: string) => {
    setSavedColourRefresh(notice);
    setBomAdminNotice("Saved; refreshing tables… / 已保存，正在刷新表格…");
    const reads = await Promise.allSettled([loadColourHexRules(), load(), onFobChanged?.()]);
    if (reads.some(read => read.status === "rejected" || read.value === false)) {
      setBomAdminNotice("Saved, but refresh failed. Re-read to verify; do not save again. / 已保存，但刷新失败，请重读核验，无须再次保存。");
      return;
    }
    setSavedColourRefresh(null);
    setBomAdminNotice(`${notice} Tables refreshed / 相关表格已刷新。`);
  };

  const draftColourHex = colourCodeEditor?.nextColourHex.trim()
    ? buildSwatchPayload(colourCodeEditor.nextColourHex, colourCodeEditor.nextColourHex2, colourCodeEditor.isDualSwatch)
    : null;
  const colourCodeChanged = Boolean(colourCodeEditor?.correctingCode && colourCodeEditor.nextColourCode !== colourCodeEditor.currentColourCode);
  const submittedColourHex = draftColourHex && colourCodeEditor && (
    colourCodeChanged || colourCodeEditor.swatchAccepted || (
      draftColourHex !== (colourCodeEditor.currentColourHex?.trim().toUpperCase() || null)
      && (colourCodeEditor.nextColourName.trim() === colourCodeEditor.currentColourName || colourCodeEditor.colourHexTouched || !colourCodeEditor.colourNameTouched)
    )
  ) ? draftColourHex : undefined;
  const keptColourHex = colourCodeChanged ? colourCodeEditor?.storedColourHex : colourCodeEditor?.currentColourHex;
  const needsCodeConfirmation = Boolean(colourCodeEditor?.materialCode && !colourCodeEditor.colourCodeConfirmed);
  const confirmCodeOnly = needsCodeConfirmation && !colourCodeChanged && !submittedColourHex
    && colourCodeEditor?.nextColourName.trim() === colourCodeEditor?.currentColourName;
  const colourScopeCount = colourHexRules.find(rule => rule.brand === colourCodeEditor?.brand && rule.colourCode === colourCodeEditor?.currentColourCode)?.skuCount ?? colourCodeEditor?.skuCount;
  const correctedMaterialCode = colourCodeEditor?.materialCode
    ? colourCodeEditor.bomTemplate?.includes("**")
      ? resolveMaterialCodeFromTemplate(colourCodeEditor.bomTemplate, colourCodeEditor.nextColourCode, colourCodeEditor.materialCode)
      : colourCodeEditor.currentColourCode
        ? colourCodeEditor.materialCode.replace(colourCodeEditor.currentColourCode, colourCodeEditor.nextColourCode)
        : colourCodeEditor.materialCode
    : null;

  const colourSuggestions = colourCodeRuleLookup?.source === "name_candidates"
    ? colourCodeRuleLookup.nameCandidates.filter(candidate => candidate.colourHex && !candidate.hasNameConflict && !candidate.hasSwatchConflict)
    : [];

  const handleColourCodeEditorSubmit = () => {
    if (!colourCodeEditor) return;
    const nextCode = colourCodeEditor.nextColourCode.trim().toUpperCase();
    if (!/^[A-Z0-9]{1,4}$/.test(nextCode)) {
      setColourCodeEditorError("Colour code must be 1-4 letters or numbers.");
      return;
    }
    const nextName = colourCodeEditor.nextColourName.trim();
    const codeChanged = nextCode !== colourCodeEditor.currentColourCode;
    const nameChanged = nextName !== colourCodeEditor.currentColourName;
    if (codeChanged && loadingColourCodeRuleLookup) {
      setColourCodeEditorError("Wait for the Brand + Code rule check to finish.");
      return;
    }
    if (!confirmCodeOnly && !isSharedSwatchDraftValid(colourCodeEditor)) {
      setColourCodeEditorError("Enter a colour name and complete valid HEX values, or leave HEX empty to keep existing swatches. / 填写名称及完整有效色值，或留空保留原色卡。");
      return;
    }
    const hexChanged = Boolean(submittedColourHex);
    if (!codeChanged && !nameChanged && !hexChanged && !colourCodeEditor.needsSynchronization && !needsCodeConfirmation) {
      setColourCodeEditor(null);
      setBomAdminNotice("No actual changes; nothing modified. / 无实际变化，未修改数据。");
      return;
    }
    setColourCodeEditorError("");
    colourCodeLookupRequestRef.current += 1;
    setLoadingColourCodeRuleLookup(false);
    setConfirmingColourEdit(true);
  };

  const saveConfirmedColour = async () => {
    if (!colourCodeEditor || colourSaveInFlightRef.current) return;
    colourSaveInFlightRef.current = true;
    setSavingColourCodeEditor(true);
    setColourCodeEditorError("");
    let notice: string;
    try {
      if (confirmCodeOnly && colourCodeEditor.materialCode) {
        await api.confirmColourCode(colourCodeEditor.materialCode);
        notice = `Confirmed ${colourCodeEditor.materialCode}; no shared fields changed / 仅确认当前物料色码，未修改共享字段。`;
      } else if (!colourCodeChanged) {
        const result = await api.setOrderGeniusColourHexRuleStandard({
          brand: colourCodeEditor.brand,
          colourCode: colourCodeEditor.currentColourCode,
          colourName: colourCodeEditor.nextColourName.trim(),
          colourHex: submittedColourHex,
          ...(needsCodeConfirmation && colourCodeEditor.materialCode ? { confirmMaterialCode: colourCodeEditor.materialCode } : {}),
        });
        notice = `Saved shared ${result.colourCode}; updated ${result.updated} materials / 已保存共享颜色，更新 ${result.updated} 条物料。`;
      } else {
        if (!colourCodeEditor.materialCode) return;
        const result = await api.updateColourCode(colourCodeEditor.materialCode, {
          colourCode: colourCodeEditor.nextColourCode,
          colourName: colourCodeEditor.nextColourName.trim(),
          colourHex: submittedColourHex,
        });
        notice = `Saved ${result.oldMaterialCode || colourCodeEditor.materialCode} → ${result.materialCode} / 已保存色码纠错。`;
      }
      setBomAdminError("");
      setConfirmingColourEdit(false);
      setColourCodeEditor(null);
      await refreshSavedColour(notice);
    } catch (err) {
      setColourCodeEditorError(`Save failed; draft kept. / 保存失败，输入已保留。 ${getErrorMessage(err)}`);
    } finally {
      colourSaveInFlightRef.current = false;
      setSavingColourCodeEditor(false);
    }
  };

  const openAddColourEditor = (
    bomTemplate: string,
    tierName: BomAdminColourTier,
    ref: any,
    tierSkus: any[],
    allSkus: any[],
  ) => {
    if (!String(ref?.powertrain || "").trim()) {
      const saveKey = buildBomEditScopeKey(getBomAdminModelGroupKey(ref.brand, ref.modelName, ref.powertrain), ref.version || "Default", bomTemplate);
      setProductSaveMessages((current) => ({ ...current, [saveKey]: {
        kind: "error", text: "Choose and save the source template powertrain before adding a colour / 请先确认并保存源模板动力类型",
      } }));
      return;
    }
    const fobSourceSku = pickFobSourceSkuForNewColour(tierSkus, allSkus);
    const fobSourceCountries = Object.values((fobSourceSku?.fobByCountry as Record<string, BomDraftFobEntry>) || {})
      .filter((fob) => getDraftBaseFob(fob) != null)
      .length;
    addColourLookupRequestRef.current += 1;
    setAddColourRuleLookup(null);
    setLoadingAddColourRuleLookup(false);
    setAddColourEditorError("");
    setAddColourEditor({
      bomTemplate: String(bomTemplate || "").trim().toUpperCase(),
      tierName,
      sourceMaterialCode: String(ref?.materialCode || "").trim().toUpperCase(),
      brand: String(ref?.brand || ""),
      modelName: String(ref?.modelName || ""),
      version: String(ref?.version || ""),
      powertrain: String(ref?.powertrain || ""),
      interiorColorName: String(ref?.interiorColorName || ""),
      editionTag: ref?.editionTag ? String(ref.editionTag) : null,
      fobSourceSku,
      fobSourceCountries,
      colourCode: "",
      colourName: "",
      colourHex: "",
      colourHex2: "",
      isDualSwatch: tierName === "dual",
      colourNameTouched: false,
      colourHexTouched: false,
    });
  };

  const handleAddColourEditorSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!addColourEditor) return;
    const colourCode = addColourEditor.colourCode.trim().toUpperCase();
    const colourName = addColourEditor.colourName.trim();
    if (!/^[A-Z0-9]{1,4}$/.test(colourCode)) {
      setAddColourEditorError("Colour code must be 1-4 letters or numbers.");
      return;
    }
    if (loadingAddColourRuleLookup) {
      setAddColourEditorError("Wait for the Brand + Code rule check to finish.");
      return;
    }
    if (
      (
        !addColourRuleLookup
        || !["persistent_rule", "brand_code_rule"].includes(addColourRuleLookup.source)
        || addColourRuleLookup.hasNameConflict
        || addColourRuleLookup.hasSwatchConflict
      )
      && !colourName
    ) {
      setAddColourEditorError("This code cannot be auto-filled: enter a colour name before creating it.");
      addColourEditorNameRef.current?.focus();
      return;
    }
    if (!addColourEditor.bomTemplate.includes("**")) {
      setAddColourEditorError("This BOM template needs ** before adding another colour.");
      return;
    }
    const newColourHexPayload = addColourEditor.colourHexTouched
      ? buildSwatchPayload(addColourEditor.colourHex, addColourEditor.colourHex2, addColourEditor.isDualSwatch)
      : "";
    const newColourHexParts = newColourHexPayload ? newColourHexPayload.split("|") : [];
    if (addColourEditor.colourHexTouched && (!isColourPickerValue(addColourEditor.colourHex)
      || (addColourEditor.isDualSwatch && !isColourPickerValue(addColourEditor.colourHex2)))
      || newColourHexParts.some((part) => !isColourPickerValue(part))) {
      setAddColourEditorError("Swatch must use #RRGGBB.");
      return;
    }
    const materialCode = resolveMaterialCodeFromTemplate(
      addColourEditor.bomTemplate,
      colourCode,
      addColourEditor.sourceMaterialCode,
    );
    if (!materialCode) {
      setAddColourEditorError("Material code cannot be generated from this BOM template.");
      return;
    }
    if (skus.some((sku) => String(sku.materialCode || "").toUpperCase() === materialCode)) {
      setAddColourEditorError(`Material code already exists: ${materialCode}`);
      return;
    }

    setSavingAddColourEditor(true);
    setAddColourEditorError("");
    try {
      const created = await api.createMaterialSku({
        materialCode,
        bomTemplate: addColourEditor.bomTemplate,
        brand: addColourEditor.brand,
        modelName: addColourEditor.modelName,
        version: addColourEditor.version,
        colour: addColourEditor.colourNameTouched ? colourName || undefined : undefined,
        colourCode,
        colourHex: addColourEditor.colourHexTouched ? newColourHexPayload : undefined,
        colourType: addColourEditor.tierName === "single" ? "single" : addColourEditor.tierName,
        colourTier: addColourEditor.tierName,
        powertrain: addColourEditor.powertrain,
        sourceMaterialCode: addColourEditor.fobSourceSku?.materialCode,
        automaticFobs: Boolean(addColourEditor.fobSourceSku),
        remark: String(addColourEditor.fobSourceSku?.remark || "").trim() || undefined,
      });
      if (addColourEditor.interiorColorName) {
        await api.updateSkuInterior(materialCode, {
          interiorColorName: addColourEditor.interiorColorName,
          editionTag: addColourEditor.editionTag,
        });
      }
      const automaticFobs = created?.automaticFobs as { created?: number; skippedNoBase?: number; skippedAmbiguous?: number; skippedMissingTier?: number; skippedMissingRule?: number } | undefined;
      const copiedFobs = Number(automaticFobs?.created || 0);
      const skippedFobs = Number(automaticFobs?.skippedNoBase || 0);
      const missingTierFobs = Number(automaticFobs?.skippedMissingTier || 0);
      const missingRuleFobs = Number(automaticFobs?.skippedMissingRule || 0);
      const skippedReason = [
        automaticFobs?.skippedAmbiguous ? `${automaticFobs.skippedAmbiguous} countries have conflicting bases` : "",
        skippedFobs > 0 ? `${skippedFobs} countries lack a Single base` : "",
        missingTierFobs > 0 ? `${missingTierFobs} countries lack a colour tier` : "",
        missingRuleFobs > 0 ? `${missingRuleFobs} countries lack a surcharge rule` : "",
      ].filter(Boolean).join("; ");
      setBomAdminError("");
      setBomAdminNotice(
        copiedFobs > 0
          ? `Created ${materialCode}; initialized ${copiedFobs} automatic FOB values${skippedReason ? `; ${skippedReason}.` : ""}`
          : `Created ${materialCode}; no automatic FOB was created${skippedReason ? ` because ${skippedReason}.` : "."}`,
      );
      setAddColourEditor(null);
      if (copiedFobs > 0) {
        onFobChanged?.();
      }
      await load();
    } catch (err) {
      setAddColourEditorError(getErrorMessage(err));
    } finally {
      setSavingAddColourEditor(false);
    }
  };

  const handleProductMetadataSave = async (
    event: FormEvent<HTMLFormElement>,
    allSkus: any[],
    saveKey: string,
  ) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const materialCodes = allSkus
      .map((sku) => String(sku?.materialCode || "").trim())
      .filter(Boolean);
    const leadCode = materialCodes[0];
    if (!leadCode) return;
    const remark = String(form.get("remark") || "").trim();
    let refreshedSaveKey = saveKey;
    setSavingProductKey(saveKey);
    setProductSaveMessages((prev) => {
      const next = { ...prev };
      delete next[saveKey];
      return next;
    });
    try {
      const saved = await api.updateSkuMetadata(leadCode, {
        materialCodes,
        brand: String(form.get("brand") || ""),
        modelName: String(form.get("modelName") || ""),
        version: String(form.get("version") || ""),
        powertrain: String(form.get("powertrain") || ""),
        remark,
        rowVersions: Object.fromEntries(allSkus.map((sku) => [String(sku.materialCode), Number(sku.rowVersion || 1)])),
      });
      if (!await load()) throw new Error("Product fields saved, but refresh failed. Refresh BOM Admin to verify.");
      const fields = saved.productFields;
      const groupKey = getBomAdminModelGroupKey(fields.brand, fields.modelName, fields.powertrain);
      const nextKey = buildBomEditScopeKey(groupKey, fields.version || "Default", String(allSkus[0].bomTemplate));
      refreshedSaveKey = nextKey;
      setSavingProductKey(nextKey);
      setExpandedGroups((current) => new Set([...current, groupKey]));
      setEditingBoms((current) => {
        const next = new Set(current);
        next.delete(saveKey);
        next.add(nextKey);
        return next;
      });
      await onFobChanged?.();
      setBomAdminError("");
      setBomAdminNotice("Saved product fields.");
      setProductSaveMessages((prev) => ({
        ...prev,
        [nextKey]: { kind: "success", text: "Saved product fields." },
      }));
    } catch (err) {
      const message = getErrorMessage(err);
      setBomAdminError(message);
      setProductSaveMessages((prev) => ({
        ...prev,
        [saveKey]: { kind: "error", text: message },
      }));
    } finally {
      setSavingProductKey((current) => (current === saveKey || current === refreshedSaveKey ? null : current));
    }
  };

  const toggleToolsCard = (flipped?: boolean) => {
    const nextFlipped = typeof flipped === "boolean" ? flipped : !toolsFlipped;
    setToolsFlipped(nextFlipped);
    if (nextFlipped) {
      setShowAddMaterial(false);
      setAddMaterialError("");
      setAddMaterialNotice("");
    }
    if (!nextFlipped) setBomAdminNotice("");
    setCopyCountryMessage("");
    setAdjustCountryMessage("");
    setCopyCountryForm(prev => ({
      ...prev,
      sourceCountryCode: prev.sourceCountryCode || (activeFobCountries.includes("CZ") ? "CZ" : sortedActiveFobCountries[0] || ""),
      targetCountryCode: prev.targetCountryCode || (countries.includes("SK") ? "" : "SK"),
    }));
    setAdjustCountryForm(prev => ({
      ...prev,
      countryCode: prev.countryCode || copyCountryForm.targetCountryCode || (countries.includes("SK") ? "SK" : sortedCountries[0] || ""),
    }));
  };

  useEffect(() => {
    if (typeof document === "undefined") return;
    const card = document.querySelector<HTMLElement>(".bom-admin-tools-card");
    if (!card) return;
    try {
      animate(card, {
        opacity: [0.92, 1],
        translateY: toolsFlipped ? [-4, 0] : [3, 0],
        duration: 220,
        ease: "outQuad",
      });
    } catch {
      /* decorative only */
    }
  }, [toolsFlipped]);

  useEffect(() => {
    if (!financeDrawerScope) return;
    const frame = window.requestAnimationFrame(() => {
      const shell = document.querySelector<HTMLElement>(".bom-finance-modal-shell");
      if (!shell) return;
      try {
        animate(shell, {
          opacity: [0, 1],
          scale: [0.985, 1],
          duration: 260,
          ease: "outQuad",
        });
      } catch {
        /* decorative only */
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [financeDrawerScope]);

  useEffect(() => {
    if (!financeQuickCard) return;
    const frame = window.requestAnimationFrame(() => {
      const shell = document.querySelector<HTMLElement>(".bom-finance-quick-modal-shell");
      if (!shell) return;
      try {
        animate(shell, {
          opacity: [0, 1],
          translateY: [14, 0],
          duration: 240,
          ease: "outQuad",
        });
      } catch {
        /* decorative only */
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [financeQuickCard]);

  // Focus only when a different editor target opens. Field edits and lookup
  // responses must never steal the caret from the input the user is typing in.
  useEffect(() => {
    if (!colourCodeEditorTargetKey) return;
    const frame = window.requestAnimationFrame(() => {
      colourCodeEditorInputRef.current?.focus();
      colourCodeEditorInputRef.current?.select();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [colourCodeEditorTargetKey]);

  useEffect(() => {
    if (!addColourEditorTargetKey) return;
    const frame = window.requestAnimationFrame(() => {
      addColourEditorCodeRef.current?.focus();
      addColourEditorCodeRef.current?.select();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [addColourEditorTargetKey]);

  const toggleAddMaterialForm = () => {
    const nextVisible = !showAddMaterial;
    setShowAddMaterial(nextVisible);
    if (nextVisible) setToolsFlipped(false);
    setAddMaterialError("");
    setAddMaterialNotice("");
  };

  useEffect(() => {
    if (typeof window === "undefined") return undefined;
    const updateLayout = () => {
      setIsCompactToolsLayout(window.innerWidth <= BOM_ADMIN_TOOLS_COMPACT_BREAKPOINT);
      setIsPhoneToolsLayout(window.innerWidth <= BOM_ADMIN_TOOLS_PHONE_BREAKPOINT);
    };
    updateLayout();
    window.addEventListener("resize", updateLayout);
    return () => window.removeEventListener("resize", updateLayout);
  }, []);

  const handleCopyCountryFobs = async () => {
    const sourceCountryCode = copyCountryForm.sourceCountryCode.trim().toUpperCase();
    const targetCountryCode = copyCountryForm.targetCountryCode.trim().toUpperCase();
    if (!sourceCountryCode || !targetCountryCode) {
      setCopyCountryMessage("Source and target country are required.");
      return;
    }
    if (sourceCountryCode === targetCountryCode) {
      setCopyCountryMessage("Source and target country must differ.");
      return;
    }
    setCopyingCountry(true);
    setCopyCountryMessage("");
    try {
      const res = await api.copyCountryFobs({
        sourceCountryCode,
        targetCountryCode,
        overwriteExisting: copyCountryForm.overwriteExisting,
      });
      setCopyCountryMessage(
        `${res.sourceCountryCode} -> ${res.targetCountryCode}: ${res.created} created, ${res.updated} updated, ${res.repriced} Dual/Special repriced, ${res.skipped} skipped, ${res.unchanged} unchanged.`,
      );
      setAdjustCountryForm(prev => ({ ...prev, countryCode: res.targetCountryCode }));
      await load();
      onFobCountriesChanged?.();
      onFobChanged?.();
    } catch (err) {
      setCopyCountryMessage(getErrorMessage(err));
    } finally {
      setCopyingCountry(false);
    }
  };

  const handleAdjustCountryFobs = async () => {
    const countryCode = adjustCountryForm.countryCode.trim().toUpperCase();
    const deltaEur = Number(adjustCountryForm.deltaEur);
    if (!countryCode) {
      setAdjustCountryMessage("Country is required.");
      return;
    }
    if (!Number.isFinite(deltaEur) || deltaEur === 0) {
      setAdjustCountryMessage("Delta must be a non-zero number.");
      return;
    }
    setAdjustingCountry(true);
    setAdjustCountryMessage("");
    try {
      const res = await api.adjustCountryFobs({ countryCode, deltaEur });
      const sign = res.deltaEur > 0 ? "+" : "";
      setAdjustCountryMessage(
        `${res.countryCode} ${sign}${res.deltaEur}: ${res.adjusted} adjusted, ${res.skippedNegative} skipped, ${res.unchanged} unchanged.`,
      );
      await load();
      onFobChanged?.();
    } catch (err) {
      setAdjustCountryMessage(getErrorMessage(err));
    } finally {
      setAdjustingCountry(false);
    }
  };

  const toggleGroup = (key: string) => {
    setExpandedGroups(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        expandedBomGroupKeyRef.current = key;
        next.add(key);
      }
      return next;
    });
  };

  useEffect(() => {
    const groupKey = expandedBomGroupKeyRef.current;
    if (!groupKey || !expandedGroups.has(groupKey)) return;
    expandedBomGroupKeyRef.current = null;

    const body = bomGroupRefs.current[groupKey]?.querySelector<HTMLElement>(".bom-admin-model-group-body");
    if (!body) return;
    try {
      animate(body, {
        opacity: [0, 1],
        translateY: [10, 0],
        duration: 260,
        ease: "outQuad",
      });
    } catch {
      /* decorative only */
    }
  }, [expandedGroups]);

  useEffect(() => {
    const target = auditNavigationTargetRef.current;
    if (!target || loading || !expandedGroups.has(target.modelGroupKey)) return;
    const row = bomTemplateRowRefs.current[bomMaterialKey(target.bomTemplate)];
    if (!row) return;
    auditNavigationTargetRef.current = null;
    const frame = window.requestAnimationFrame(() => {
      row.scrollIntoView?.({ behavior: "smooth", block: "center", inline: "nearest" });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [expandedGroups, skus, loading]);

  // Shared colour chip renderer used by BOM rows
  const renderColourChip = (s: any, isHist: boolean, editing: boolean) => {
    const effectiveTier = getEffectiveColourTier(s);
    const swatch = parseOrderGeniusColourSwatch(s.colourHex);
    const isDragging = dragSku === s.materialCode;
    const brand = String(s.brand || "").trim();
    const colourCode = String(s.colourCode || "").trim().toUpperCase();
    const colourName = String(s.colour || "").trim();
    const canEditSwatchRule = Boolean(brand && colourCode);
    const pricing: { amount?: number | null; source?: string | null; status?: string } | undefined = s.colourPricing;
    const surchargeSource = pricing?.source ?? "Pricing configuration missing";
    const surchargeLabel = pricing?.amount == null
      ? "Pricing configuration missing"
      : `${effectiveTier} · rule +${formatSurchargeDraft(pricing.amount)}€ (see country FOB for saved price)`;

    return (
      <span key={s.materialCode}
        draggable={editing}
        onDragStart={editing ? (e: any) => {
          e.dataTransfer.effectAllowed = 'move';
          dragMaterialCode.current = s.materialCode;
          setDragSku(s.materialCode);
        } : undefined}
        onDragEnd={editing ? () => { setDragSku(null); setDragOverTier(null); dragMaterialCode.current = null; } : undefined}
        title={`${s.colour}${s.colourCode ? ` (${s.colourCode})` : ''}${swatch.isDual ? ' · dual swatch' : ''}${swatch.isMissing ? ' · missing swatch' : ''} · Tier: ${effectiveTier} · ${surchargeSource} — Drag to reclassify, click swatch to edit colour rule`}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 2,
          fontSize: 10,
          color: isHist ? '#9ca3af' : '#475569',
          cursor: editing ? (isDragging ? 'grabbing' : 'grab') : 'default',
          opacity: isDragging ? 0.4 : 1,
          position: "relative",
        }}
      >
        <button
          type="button"
          className="bom-colour-swatch-button"
          title={canEditSwatchRule
            ? `Edit swatch rule for ${brand} ${colourCode} ${colourName}`
            : "Missing brand or colour code"}
          onClick={(event) => {
            event.stopPropagation();
            if (!canEditSwatchRule) {
              setColourHexRuleStatus("Brand and colour code are required to edit a swatch rule.");
              return;
            }
            openColourCodeEditor(s);
          }}
          style={{
            width: 18,
            height: 18,
            padding: 0,
            borderRadius: 3,
            flexShrink: 0,
            border: swatch.isMissing ? '1px dashed #94a3b8' : '1px solid #d1d5db',
            background: swatch.background,
            opacity: isHist ? 0.5 : 1,
            cursor: "pointer",
          }}
        />
        {s.colourCodeConfirmed === false ? (
          <span title="Unconfirmed colour code — click to edit and confirm" style={{ fontWeight: 700, whiteSpace: "nowrap", color: '#dc2626', cursor: 'pointer', textDecoration: 'underline' }}
            onClick={async (e2: any) => { e2.stopPropagation();
              openColourCodeEditor(s);
            }}>
            {s.colourCode || s.colour}
          </span>
        ) : editing ? (
          <span title="Edit colour" style={{ fontWeight: 700, whiteSpace: "nowrap", fontSize: 9, color: effectiveTier === 'special' ? '#d97706' : effectiveTier === 'dual' ? '#2563eb' : '#16a34a', cursor: 'pointer', textDecoration: 'underline', textUnderlineOffset: 2 }}
            onClick={async (e2: any) => { e2.stopPropagation();
              openColourCodeEditor(s);
            }}>
            {s.colourCode || s.colour}
          </span>
        ) : (
	          <span title={`${s.colour} · ${surchargeLabel} · Tier: ${effectiveTier} · Edit colour`}
              onClick={(event) => { event.stopPropagation(); openColourCodeEditor(s); }}
	            style={{ fontWeight: 500, whiteSpace: "nowrap", fontSize: 9, cursor: 'pointer', color: effectiveTier === 'special' ? '#d97706' : effectiveTier === 'dual' ? '#2563eb' : '#16a34a' }}>
	            {s.colourCode || s.colour}
          </span>
        )}
        {editing && isAdmin ? (
          pendingDeletes.has(s.materialCode) ? (
            <span title="Click again to confirm delete" style={{ cursor: 'pointer', color: '#fff', fontSize: 9, marginLeft: 1, fontWeight: 700, background: '#dc2626', borderRadius: 2, padding: '1px 3px' }}
              onClick={async (e2: any) => {
                e2.stopPropagation();
                try { await api.deleteMaterialSku(s.materialCode); setPendingDeletes(prev => { const n = new Set(prev); n.delete(s.materialCode); return n; }); scheduleLoad(300); } catch {}
              }}>Del?</span>
          ) : (
            <span title="Delete this colour" style={{ cursor: 'pointer', color: '#ef4444', fontSize: 10, marginLeft: 1, fontWeight: 700 }}
              onClick={(e2: any) => {
                e2.stopPropagation();
                setPendingDeletes(new Set([s.materialCode]));
              }}>×</span>
          )
        ) : null}
      </span>
    );
  };

  const renderDraftColourChip = (sku: BomCopyDraftSku) => {
    const effectiveTier = inferBomAdminColourTier(sku);
    const swatch = parseOrderGeniusColourSwatch(sku.colourHex);
    return (
      <span
        key={`${sku.sourceMaterialCode}-${sku.colourCode}`}
        title={`${sku.colour}${sku.colourCode ? ` (${sku.colourCode})` : ""} · ${effectiveTier}`}
        style={{ display: "inline-flex", alignItems: "center", gap: 2, fontSize: 10, color: "#475569" }}
      >
        <span
          style={{
            display: "inline-block",
            width: 16,
            height: 16,
            borderRadius: 3,
            flexShrink: 0,
            border: swatch.isMissing ? "1px dashed #94a3b8" : "1px solid #d1d5db",
            background: swatch.background,
          }}
        />
        <span
          style={{
            fontWeight: 500,
            whiteSpace: "nowrap",
            fontSize: 9,
            color: effectiveTier === "special" ? "#d97706" : effectiveTier === "dual" ? "#2563eb" : "#16a34a",
          }}
        >
          {sku.colourCode || sku.colour}
        </span>
      </span>
    );
  };

  // Two-level grouping: model+powertrain → version, with multiple BOM template rows per version
  const modelGroups = useMemo(() => {
    const map = new Map<string, BomAdminModelGroup>();
    for (const s of skus) {
      const pt = getBomAdminPowertrainGroup(s.powertrain);
      const mk = getBomAdminModelGroupKey(s.brand, s.modelName, s.powertrain);
      if (!map.has(mk)) map.set(mk, { brand: s.brand, modelName: s.modelName, pt, versions: new Map() });
      const vk = s.version || 'Default';
      if (!map.get(mk)!.versions.has(vk)) map.get(mk)!.versions.set(vk, []);
      // Skip duplicates: same material_code + colour only once per version
      const existing = map.get(mk)!.versions.get(vk)!;
      if (!existing.some((x: any) => x.materialCode === s.materialCode)) {
        existing.push(s);
      }
    }
    return map;
  }, [skus]);

  // Group SKUs within a version by BOM template (using stored bomTemplate from DB)
  // Returns: Map<bomTemplate, { single: SKU[], dual: SKU[], special: SKU[] }>
  const groupByTemplate = useCallback((vSkus: any[]): Map<string, BomAdminTierGroups> => {
    const byPeriod = new Map<string, any[]>();
    for (const s of vSkus) {
      const period = `${s.effectiveFrom || 'any'}_${s.effectiveTo || 'any'}`;
      // Use stored bomTemplate from DB; fall back to single-code derive
      const bt = s.bomTemplate || deriveMaterialTemplate([s.materialCode]);
      const gk = `${bt}|${period}`;
      if (!byPeriod.has(gk)) byPeriod.set(gk, []);
      byPeriod.get(gk)!.push(s);
    }
    const result = new Map<string, BomAdminTierGroups>();
    for (const [gk, gSkus] of byPeriod) {
      const bt = gk.split('|')[0];
      const entry = result.get(bt) || {
        single: [],
        dual: [],
        special: [],
        allSkus: [],
        countryCodes: [],
        filledCountryCodes: [],
        filledCountryCodeSet: new Set<string>(),
      };
      for (const s of gSkus) {
        const tier = getEffectiveColourTier(s);
        if (tier === 'special') entry.special.push(s);
        else if (tier === 'dual') entry.dual.push(s);
        else entry.single.push(s);
        entry.allSkus.push(s);
      }
      result.set(bt, entry);
    }
    for (const entry of result.values()) {
      const countryCodes = collectCountryCodes([
        ...sortedCountries,
        ...entry.allSkus.flatMap((sku: any) =>
          Object.keys((sku?.fobByCountry as Record<string, BomDraftFobEntry>) || {}),
        ),
      ]);
      const filledCountryCodes = countryCodes.filter((countryCode) =>
        entry.allSkus.some((sku: any) => getDraftBaseFob(sku?.fobByCountry?.[countryCode]) != null),
      );
      entry.countryCodes = countryCodes;
      entry.filledCountryCodes = filledCountryCodes;
      entry.filledCountryCodeSet = new Set(filledCountryCodes);
    }
    return result;
  }, [collectCountryCodes, getEffectiveColourTier, sortedCountries]);

  const sortedModelGroupEntries = useMemo(() => {
    return [...modelGroups.entries()].sort(([a], [b]) => {
      const [brandA = "", modelA = "", ptA = ""] = a.split("|");
      const [brandB = "", modelB = "", ptB = ""] = b.split("|");
      return compareProductModels(brandA, modelA, ptA, brandB, modelB, ptB);
    });
  }, [modelGroups]);

  const sortedVersionEntriesByModelKey = useMemo(() => {
    const result = new Map<string, [string, any[]][]>();
    for (const [modelKey, modelGroup] of modelGroups.entries()) {
      result.set(modelKey, [...modelGroup.versions.entries()].sort(([a], [b]) => a.localeCompare(b)));
    }
    return result;
  }, [modelGroups]);

  const sortedTemplateEntriesByVersionKey = useMemo(() => {
    const result = new Map<string, [string, BomAdminTierGroups][]>();
    for (const [modelKey, versionEntries] of sortedVersionEntriesByModelKey.entries()) {
      for (const [versionKey, versionSkus] of versionEntries) {
        result.set(
          `${modelKey}|${versionKey}`,
          [...groupByTemplate(versionSkus).entries()].sort(([a], [b]) => a.localeCompare(b)),
        );
      }
    }
    return result;
  }, [groupByTemplate, sortedVersionEntriesByModelKey]);

  const retryBomAdminLoad = () => {
    void load();
  };

  const reLoginForBomAdmin = () => {
    localStorage.removeItem("jato_auth_token");
    localStorage.removeItem("jato_user_name");
    localStorage.removeItem("jato_user_role");
    window.location.href = "/login";
  };

  const renderBomAdminRecoveryPanel = (
    title: string,
    detail: string,
    tone: "loading" | "error" | "empty",
  ) => {
    const borderColor = tone === "error" ? "#fecaca" : "#bfdbfe";
    const background = tone === "error" ? "#fef2f2" : "#eff6ff";
    const titleColor = tone === "error" ? "#991b1b" : "#1e3a8a";
    return (
      <div
        style={{
          margin: 16,
          padding: 16,
          border: `1px solid ${borderColor}`,
          background,
          color: "#334155",
        }}
      >
        <div style={{ fontSize: 11, fontWeight: 900, letterSpacing: 2, color: titleColor, textTransform: "uppercase", marginBottom: 6 }}>
          BOM Admin
        </div>
        <div style={{ fontSize: 18, fontWeight: 900, color: "#0f172a", marginBottom: 6 }}>{title}</div>
        <div style={{ fontSize: 13, lineHeight: 1.5, color: "#475569", maxWidth: 780 }}>{detail}</div>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 14 }}>
          <button type="button" className="btn btn-sm btn-primary" onClick={retryBomAdminLoad}>Retry</button>
          <button type="button" className="btn btn-sm btn-secondary" onClick={() => window.location.reload()}>Refresh app</button>
          <button type="button" className="btn btn-sm btn-secondary" onClick={reLoginForBomAdmin}>Re-login</button>
        </div>
        {bomAdminError ? (
          <div style={{ marginTop: 12, fontSize: 12, color: "#991b1b", fontWeight: 700 }}>
            {bomAdminError}
          </div>
        ) : null}
      </div>
    );
  };

  if (bomAdminError && skus.length === 0 && countries.length === 0 && !loading) {
    return renderBomAdminRecoveryPanel(
      "Could not load BOM data",
      "Chrome may be using a stale session or asset cache. Retry the API first; refresh the app if the page was open during deployment; re-login if the token is expired.",
      "error",
    );
  }

  if (loading && skus.length === 0 && countries.length === 0) return <div style={{ padding: 16, color: "#64748b" }}>Loading BOM data...</div>;

  if (!loading && skus.length === 0 && countries.length === 0) {
    return renderBomAdminRecoveryPanel(
      "No BOM data returned",
      "The request completed but returned no visible BOM rows or countries. Retry with the current search, refresh the app to pick up the latest bundle, or re-login if Chrome has stale account state.",
      "empty",
    );
  }

  const bomHeaderBaseStyle = {
    background: "#334155",
    color: "#ffffff",
    fontWeight: 900,
    borderBottom: "2px solid #0f172a",
    textShadow: "0 1px 0 rgba(0,0,0,0.35)",
  } as const;
  const getBomStickyCellStyle = (
    column: BomAdminStickyColumn,
    background: string,
    zIndex: number,
  ): CSSProperties => {
    const style: CSSProperties = {
      width: BOM_ADMIN_STICKY_COLUMN_WIDTHS[column],
      minWidth: BOM_ADMIN_STICKY_COLUMN_WIDTHS[column],
      background,
    };
    if (column === "bom" || !isCompactToolsLayout) {
      style.position = "sticky";
      style.left = BOM_ADMIN_STICKY_COLUMN_LEFTS[column];
      style.zIndex = zIndex;
    }
    return style;
  };
  const toolsCardHeight = toolsFlipped
    ? (isPhoneToolsLayout ? 620 : isCompactToolsLayout ? 520 : 390)
    : (isPhoneToolsLayout ? 118 : 84);
  const toolsRowMarginBottom = 12;
  const bomSearchPlaceholder = isCompactToolsLayout
    ? "Search model / material / country"
    : "Search model / material / country (e.g. JAECOO7, T716, SE) — Enter or auto 1.2s";
  const addMaterialButtonLabel = showAddMaterial
    ? (isPhoneToolsLayout ? "Hide Form" : "Hide + Material")
    : "+ Material";
  const renderFinanceQuickSummary = (): string => {
    if (financeQuickLoading) return "Loading...";
    const row = financeQuickRows[0];
    if (!row) return `FOB ${financeQuickCard?.fob?.toLocaleString() ?? "-"} · no CBU memo yet`;
    const margin = row.vehicleMarginEur ?? row.marginEur;
    const marginRate = row.vehicleMarginRate ?? row.marginRate;
    const profit = row.vehicleProfitEur;
    return [
      `FOB ${row.fobEur?.toLocaleString() ?? "-"}`,
      `Unit margin ${margin?.toLocaleString() ?? "-"}`,
      `Margin ${marginRate == null ? "-" : `${(marginRate * 100).toFixed(2)}%`}`,
      `Profit ${profit?.toLocaleString() ?? "-"}`,
    ].join(" · ");
  };

  return (
    <div style={{ padding: 20 }}>
      <div style={{ display: "flex", flexDirection: isPhoneToolsLayout ? "column" : "row", gap: isPhoneToolsLayout ? 2 : 8, justifyContent: "space-between", alignItems: isPhoneToolsLayout ? "flex-start" : "center", marginBottom: 12, position: "sticky", top: 0, background: "rgba(255,255,255,0.95)", backdropFilter: "blur(8px)", zIndex: 1, padding: "8px 0", borderBottom: "1px solid #e2e8f0" }}>
        <h3 style={{ margin: 0, lineHeight: 1.2 }}>BOM / Material Master</h3>
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap", justifyContent: isPhoneToolsLayout ? "flex-start" : "flex-end" }}>
          <div className="bom-fob-audit-legend" aria-label="FOB audit source legend">
            <span><b>C</b> copied FOB</span>
            <span><b>Pn</b> latest period · n saved periods</span>
            <span><b>B</b> country adjustment</span>
            <span><b>M</b> cell edit</span>
            <span><i className="bom-fob-legend-remark-dot" /> remark</span>
          </div>
          <span style={{ fontSize: 12, color: "#64748b", whiteSpace: "nowrap" }}>{skus.length} SKUs · {modelGroups.size} models · {sortedCountries.length} countries</span>
        </div>
      </div>
      <div className={`bom-admin-toolbar${toolsFlipped ? " is-tools-open" : ""}`} style={{ marginBottom: toolsRowMarginBottom }}>
        <div className="bom-admin-search-strip">
          <input ref={searchInputRef} type="text" placeholder={bomSearchPlaceholder} title="Search model / material / country (e.g. JAECOO7, T716, SE). Press Enter or wait 1.2s." value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                const nextSearch = searchText.trim();
                setDebouncedSearch(nextSearch);
                void load(nextSearch);
              }
            }}
            className="bom-admin-search-input" />
          <button
            className="btn btn-sm btn-ghost"
            style={{ flexShrink: 0 }}
            onClick={async () => {
              setSearchText("");
              setDebouncedSearch("");
              await load("");
              window.setTimeout(() => searchInputRef.current?.focus(), 0);
            }}
          >
            Clear
          </button>
        </div>
        <FlipToolCard
          flipped={toolsFlipped}
          ariaLabel="BOM admin tools"
          className="bom-admin-tools-card"
          height={toolsCardHeight}
          minHeight={toolsCardHeight}
          style={{
            transition: "width 180ms ease, height 180ms ease, min-height 180ms ease",
          }}
          frontStyle={{
            pointerEvents: toolsFlipped ? "none" : "auto",
          }}
          backStyle={{
            overflowY: "auto",
            pointerEvents: toolsFlipped ? "auto" : "none",
          }}
          front={
              <header className="bom-admin-tools-front-layout">
                <div className="bom-admin-tools-front-copy">
                  <span style={{ fontSize: 12, color: "#334155", fontWeight: 800, letterSpacing: "0.06em" }}>BOM ADMIN TOOLS</span>
                  <h2 style={{ margin: "3px 0 2px", fontSize: 17, lineHeight: 1.2 }}>Material and country helpers</h2>
                  <p style={{ margin: 0, fontSize: 12, color: "#64748b" }}>Copy FOB · Colour surcharge · Swatch rules.</p>
                </div>
                <div className="bom-admin-tools-front-actions">
                  <button type="button" className="btn btn-sm btn-secondary" onClick={() => toggleToolsCard(true)}>Edit tools</button>
                  {isAdmin ? <button className="btn btn-sm btn-ghost" onClick={toggleAddMaterialForm}>
                    {addMaterialButtonLabel}
                  </button> : null}
                </div>
              </header>
          }
          back={
            <>
              <header className="bom-admin-tools-back-head">
                <div>
                  <span style={{ fontSize: 12, color: "#334155", fontWeight: 800, letterSpacing: "0.06em" }}>BOM ADMIN TOOLS</span>
                  <h2 style={{ margin: "3px 0 2px", fontSize: 17, lineHeight: 1.2 }}>Copy FOB & colour tools</h2>
                  <p style={{ margin: 0, fontSize: 12, color: "#64748b" }}>Country copy · country adjust · surcharges · swatches.</p>
                </div>
                <button type="button" className="btn btn-sm btn-ghost" onClick={() => toggleToolsCard(false)}>Back</button>
              </header>
              <div className="bom-admin-tools-grid">
                <div className="bom-admin-tool-tile">
                  <div style={{ fontSize: 11, fontWeight: 800, color: "#334155", marginBottom: 8 }}>Copy Country FOB</div>
                  <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                    <select
                      value={copyCountryForm.sourceCountryCode}
                      onChange={(e) => setCopyCountryForm({ ...copyCountryForm, sourceCountryCode: e.target.value.toUpperCase() })}
                      style={{ fontSize: 11, width: 84 }}
                    >
                      <option value="">Source</option>
                      {sortedActiveFobCountries.map((code) => (
                        <option key={code} value={code}>
                          {code}
                        </option>
                      ))}
                    </select>
                    <span style={{ fontSize: 12, color: "#64748b" }}>to</span>
                    <input
                      type="text"
                      list="bom-copy-target-countries"
                      placeholder="SK"
                      value={copyCountryForm.targetCountryCode}
                      onChange={(e) => setCopyCountryForm({ ...copyCountryForm, targetCountryCode: e.target.value.toUpperCase().slice(0, 2) })}
                      style={{ width: 56, fontSize: 11, textTransform: "uppercase" }}
                    />
                    <datalist id="bom-copy-target-countries">
                      {copyTargetOptions.map((code) => (
                        <option key={code} value={code}>
                          {countryLabels.get(code) || code}
                        </option>
                      ))}
                    </datalist>
                    <label style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 11, color: "#475569" }}>
                      <input
                        type="checkbox"
                        checked={copyCountryForm.overwriteExisting}
                        onChange={(e) => setCopyCountryForm({ ...copyCountryForm, overwriteExisting: e.target.checked })}
                      />
                      overwrite
                    </label>
                    <button className="btn btn-sm btn-primary" type="button" disabled={copyingCountry} onClick={handleCopyCountryFobs}>
                      {copyingCountry ? "Copying..." : "Copy"}
                    </button>
                  </div>
                </div>
                <form
                  className="bom-admin-tool-tile"
                  onSubmit={(event) => {
                    event.preventDefault();
                    void handleAdjustCountryFobs();
                  }}
                >
                  <div style={{ fontSize: 11, fontWeight: 800, color: "#334155", marginBottom: 8 }}>Adjust Country FOB</div>
                  <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                    <input
                      type="text"
                      list="bom-adjust-countries"
                      placeholder="SK"
                      value={adjustCountryForm.countryCode}
                      onChange={(e) => setAdjustCountryForm({ ...adjustCountryForm, countryCode: e.target.value.toUpperCase().slice(0, 2) })}
                      style={{ width: 56, fontSize: 11, textTransform: "uppercase" }}
                    />
                    <datalist id="bom-adjust-countries">
                      {copyTargetOptions.map((code) => (
                        <option key={code} value={code}>
                          {countryLabels.get(code) || code}
                        </option>
                      ))}
                    </datalist>
                    <input
                      type="number"
                      step={1}
                      placeholder="+/- EUR"
                      value={adjustCountryForm.deltaEur}
                      onChange={(e) => setAdjustCountryForm({ ...adjustCountryForm, deltaEur: e.target.value })}
                      style={{ width: 82, fontSize: 11 }}
                    />
                    <button
                      className="btn btn-sm btn-ghost"
                      type="button"
                      onClick={() => setAdjustCountryForm({ ...adjustCountryForm, deltaEur: "200" })}
                    >
                      +200
                    </button>
                    <button
                      className="btn btn-sm btn-ghost"
                      type="button"
                      onClick={() => setAdjustCountryForm({ ...adjustCountryForm, deltaEur: "-300" })}
                    >
                      -300
                    </button>
                    <button className="btn btn-sm btn-primary" type="submit" disabled={adjustingCountry}>
                      {adjustingCountry ? "Applying..." : "Apply"}
                    </button>
                  </div>
                  {adjustCountryMessage ? (
                    <div style={{ marginTop: 7, fontSize: 11, color: adjustCountryMessage.includes("adjusted") ? "#0f766e" : "#dc2626", fontWeight: 600 }}>
                      {adjustCountryMessage}
                    </div>
                  ) : null}
                </form>
                <div className="bom-admin-tool-tile">
                  <div style={{ fontSize: 11, fontWeight: 800, color: "#334155", marginBottom: 8 }}>BOM Admin Excel</div>
                  <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
                    <select
                      aria-label="BOM Admin export country"
                      value={bomExportCountry}
                      onChange={(event) => setBomExportCountry(event.target.value)}
                      style={{ minWidth: 150, fontSize: 11 }}
                    >
                      <option value="">All countries</option>
                      {sortedActiveFobCountries.map((countryCode) => (
                        <option key={countryCode} value={countryCode}>
                          {countryLabels.get(countryCode) || countryCode} ({countryCode})
                        </option>
                      ))}
                    </select>
                    <button className="btn btn-sm btn-primary" type="button" disabled={exportingBomAdmin} onClick={() => void handleBomAdminExport()}>
                      {exportingBomAdmin ? "Exporting..." : "Export material workbook"}
                    </button>
                  </div>
                  <div style={{ marginTop: 7, fontSize: 9, lineHeight: 1.35, color: "#64748b" }}>
                    One model per sheet. Blue rows mean a saved Dual/Special surcharge tier; tier is never inferred from the colour name or fill.
                  </div>
                  {bomExportStatus ? (
                    <div style={{ marginTop: 7, fontSize: 11, color: bomExportStatus.startsWith("Exported") ? "#0f766e" : "#dc2626", fontWeight: 600 }}>
                      {bomExportStatus}
                    </div>
                  ) : null}
                </div>
                <form
                  className="bom-admin-tool-tile"
                  onSubmit={handleSaveColourSurcharges}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 7 }}>
                    <span style={{ fontSize: 11, fontWeight: 800, color: "#334155" }}>Colour Surcharges</span>
                    <button className="btn btn-sm btn-primary" type="submit" disabled={savingColourSurcharges}>
                      {savingColourSurcharges ? "Saving..." : "Save"}
                    </button>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "72px repeat(2, 1fr)", gap: 6, alignItems: "center" }}>
                    <span />
                    {BOM_ADMIN_SURCHARGE_TYPES.map((type) => (
                      <span key={type.value} style={{ fontSize: 10, fontWeight: 800, color: "#64748b", textTransform: "uppercase" }}>
                        {type.label}
                      </span>
                    ))}
                    {BOM_ADMIN_SURCHARGE_BRANDS.map((brand) => (
                      <Fragment key={brand}>
                        <span style={{ fontSize: 11, fontWeight: 800, color: "#334155" }}>{brand}</span>
                        {BOM_ADMIN_SURCHARGE_TYPES.map((type) => {
                          const key = colourSurchargeKey(brand, type.value);
                          return (
                            <input
                              key={key}
                              type="number"
                              min={0}
                              step={1}
                              value={colourSurchargeDrafts[key] ?? ""}
                              onChange={(e) => setColourSurchargeDrafts((prev) => ({ ...prev, [key]: e.target.value }))}
                              style={{ width: "100%", fontSize: 11 }}
                            />
                          );
                        })}
                      </Fragment>
                    ))}
                  </div>
                  {colourSurchargeStatus ? (
                    <div style={{ marginTop: 7, fontSize: 11, color: colourSurchargeStatus.startsWith("Saved") ? "#0f766e" : "#b45309" }}>
                      {colourSurchargeStatus}
                    </div>
                  ) : null}
                </form>
                <form className="bom-admin-tool-tile" onSubmit={handleSaveSpecialColourSurcharge}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: specialColourSurchargeExpanded ? 7 : 0 }}>
                    <button
                      type="button"
                      className="btn btn-sm btn-ghost"
                      aria-expanded={specialColourSurchargeExpanded}
                      onClick={() => setSpecialColourSurchargeExpanded((expanded) => !expanded)}
                      style={{ padding: 0, fontSize: 11, fontWeight: 800, color: "#334155" }}
                    >
                      {specialColourSurchargeExpanded ? "▾" : "▸"} Custom colour surcharge ({specialColourSurchargeRules.length})
                    </button>
                    {specialColourSurchargeExpanded ? (
                      <button className="btn btn-sm btn-primary" type="submit" disabled={savingSpecialColourSurcharge}>
                        {savingSpecialColourSurcharge ? "Saving..." : "Save"}
                      </button>
                    ) : null}
                  </div>
                  {specialColourSurchargeExpanded ? (
                    <>
                      <div style={{ display: "grid", gridTemplateColumns: "72px 1fr 70px 82px 1fr 64px", gap: 5, alignItems: "center" }}>
                        <select value={specialColourSurchargeDraft.brand} onChange={(e) => setSpecialColourSurchargeDraft((prev) => ({ ...prev, brand: e.target.value }))} style={{ fontSize: 10 }}>
                          {BOM_ADMIN_SURCHARGE_BRANDS.map((brand) => <option key={brand}>{brand}</option>)}
                        </select>
                        <input type="text" placeholder="Model (blank = brand)" value={specialColourSurchargeDraft.modelName} onChange={(e) => setSpecialColourSurchargeDraft((prev) => ({ ...prev, modelName: e.target.value }))} style={{ fontSize: 10 }} />
                        <input type="text" placeholder="Code" value={specialColourSurchargeDraft.colourCode} onChange={(e) => setSpecialColourSurchargeDraft((prev) => ({ ...prev, colourCode: e.target.value.toUpperCase() }))} style={{ fontSize: 10, textTransform: "uppercase" }} />
                        <select value={specialColourSurchargeDraft.colourTier} onChange={(e) => setSpecialColourSurchargeDraft((prev) => ({ ...prev, colourTier: e.target.value as "dual" | "special" }))} style={{ fontSize: 10 }}>
                          {BOM_ADMIN_SURCHARGE_TYPES.map((type) => <option key={type.value} value={type.value}>{type.label}</option>)}
                        </select>
                        <input type="text" placeholder="Colour name (optional)" value={specialColourSurchargeDraft.colourName} onChange={(e) => setSpecialColourSurchargeDraft((prev) => ({ ...prev, colourName: e.target.value }))} style={{ fontSize: 10 }} />
                        <input type="number" min={0} step={1} placeholder="EUR" value={specialColourSurchargeDraft.surchargeEur} onChange={(e) => setSpecialColourSurchargeDraft((prev) => ({ ...prev, surchargeEur: e.target.value }))} style={{ fontSize: 10 }} />
                      </div>
                      <div style={{ marginTop: 6, fontSize: 9, color: "#64748b" }}>Same tier only: model + code → brand + code → brand tier default. Single is always +0; 0 EUR explicitly waives the fallback.</div>
                      {specialColourSurchargeRules.length > 0 ? (
                        <div style={{ display: "grid", gap: 3, marginTop: 7, maxHeight: 94, overflowY: "auto" }}>
                          {specialColourSurchargeRules.map((rule) => (
                            <button key={rule.specialColourSurchargeRuleId} type="button" className="btn btn-sm btn-ghost" onClick={() => setSpecialColourSurchargeDraft({ brand: rule.brand, modelName: rule.modelName || "", colourCode: rule.colourCode, colourTier: rule.colourTier, colourName: rule.colourName || "", surchargeEur: String(rule.surchargeEur) })} style={{ display: "flex", justifyContent: "space-between", gap: 8, padding: "3px 5px", fontSize: 9, textAlign: "left" }}>
                              <span>{rule.brand}{rule.modelName ? ` · ${rule.modelName}` : ""} · {rule.colourCode} · {rule.colourTier}{rule.colourName ? ` · ${rule.colourName}` : ""}</span>
                              <strong>+{rule.surchargeEur} EUR</strong>
                            </button>
                          ))}
                        </div>
                      ) : null}
                      {specialColourSurchargeStatus ? (
                        <div style={{ marginTop: 7, fontSize: 11, color: specialColourSurchargeStatus.startsWith("Saved") ? "#0f766e" : "#b45309" }}>
                          {specialColourSurchargeStatus}
                        </div>
                      ) : null}
                    </>
                  ) : null}
                </form>
                <div className="bom-admin-tool-tile" style={{ minHeight: 124 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 7 }}>
                    <span style={{ fontSize: 11, fontWeight: 800, color: "#334155" }}>Colour Swatch Rules</span>
                    <button className="btn btn-sm btn-ghost" type="button" disabled={loadingColourHexRules} onClick={() => void loadColourHexRules()}>
                      {loadingColourHexRules ? "Refreshing..." : "Refresh"}
                    </button>
                  </div>
                  <div style={{ fontSize: 10, color: "#64748b", marginBottom: 7 }}>
                    {colourHexRuleSummary.totalRules} brand + code groups · categories may overlap / 分类可重叠 · missing HEX is not a colour-code conflict / 缺色卡不等于色码错误
                  </div>
                  {colourHexRuleSummary.invalidIdentitySkuCount > 0 ? (
                    <div style={{ marginBottom: 7, padding: "6px 8px", border: "1px solid #fbbf24", background: "#fffbeb", color: "#92400e", fontSize: 10, lineHeight: 1.35 }}>
                      {colourHexRuleSummary.invalidIdentitySkuCount} SKU(s) are excluded from shared rules because Brand + colour code is incomplete.
                      {colourHexRuleSummary.invalidIdentitySampleMaterialCodes.length > 0 ? ` Sample: ${colourHexRuleSummary.invalidIdentitySampleMaterialCodes.join(", ")}` : ""}
                    </div>
                  ) : null}
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(96px, 1fr))", gap: 4 }}>
                    {COLOUR_RULE_STATUS_META.map((meta) => (
                      <button key={meta.status} type="button" className="btn btn-sm btn-ghost" onClick={() => setColourRuleDetailsStatus(meta.status)} style={{ padding: 4, display: "grid", gap: 2, color: meta.colour, whiteSpace: "normal", height: "auto", minHeight: 44 }}>
                        <strong>{getColourRuleDetails(colourHexRules, meta.status).length}</strong><span style={{ fontSize: 10 }}>{meta.label}</span>
                      </button>
                    ))}
                  </div>
                  <button className="btn btn-sm btn-primary" type="button" disabled={loadingColourRulePreview || colourHexRuleSummary.totalRules === 0} onClick={() => void handlePreviewColourRuleFills()} style={{ marginTop: 7, width: "100%" }}>
                    {loadingColourRulePreview ? "Building preview..." : "Preview shared swatch standards"}
                  </button>
                  {colourHexRuleStatus ? (
                    <div style={{ marginTop: 7, fontSize: 11, color: colourHexRuleStatus.startsWith("Set") ? "#0f766e" : "#b45309" }}>
                      {colourHexRuleStatus}
                    </div>
	                  ) : null}
	                </div>
	                <BomFobRepriceAuditCard
	                  onOpenBomTemplate={openAuditBomTemplate}
	                  onApplied={async () => {
	                    await load();
	                    onFobChanged?.();
	                  }}
	                />
	              </div>
              {copyCountryMessage ? (
                <div style={{ fontSize: 11, color: copyCountryMessage.includes("created") ? "#0f766e" : "#dc2626", fontWeight: 600 }}>
                  {copyCountryMessage}
                </div>
              ) : null}
            </>
          }
        />
      </div>
      {bomAdminNotice ? (
        <div role="status" style={{ marginBottom: 8, padding: "8px 10px", border: "1px solid #bfdbfe", background: "#eff6ff", color: "#1d4ed8", fontSize: 12 }}>
          {bomAdminNotice}
          {savedColourRefresh ? <button className="btn btn-sm btn-ghost" type="button" disabled={savingColourCodeEditor} onClick={() => void refreshSavedColour(savedColourRefresh)}>Re-read saved colour / 重读已保存颜色</button> : null}
        </div>
      ) : null}
      {colourRuleDetailsStatus ? (
        <div className="bom-finance-modal-backdrop" onClick={() => setColourRuleDetailsStatus(null)}>
          <div className="bom-colour-code-edit-modal-shell" onClick={(event) => event.stopPropagation()}>
            <section className="bom-colour-code-edit-card" role="dialog" aria-modal="true" aria-label="Colour rule details">
              <div className="bom-colour-code-edit-head"><div><span className="bom-finance-eyebrow">COLOUR RULES</span><h4>{COLOUR_RULE_STATUS_META.find((meta) => meta.status === colourRuleDetailsStatus)?.label}</h4><p>{selectedColourRuleDetails.length} brand + code groups</p></div></div>
              <div style={{ display: "grid", gap: 8, maxHeight: "52vh", overflowY: "auto" }}>
                {selectedColourRuleDetails.length === 0 ? <p>No rules in this category.</p> : selectedColourRuleDetails.map((rule) => (
                  <div key={`${rule.brand}|${rule.colourCode}`} style={{ border: "1px solid #e2e8f0", padding: 8 }}>
                    <strong>{rule.brand} · {rule.colourCode}</strong>
                    <div style={{ fontSize: 11, color: "#64748b" }}>{rule.standardColourName ?? rule.colourName ?? "No standard name"} · {rule.standardColourHex ?? "No standard swatch"} · {rule.skuCount} SKUs</div>
                    <div style={{ fontSize: 10, color: "#64748b" }}>{rule.fillableSkuCount} fillable · {rule.placeholderNameSkuCount} placeholder names · {rule.missingSwatchSkuCount} missing swatches</div>
                    {(rule.hasNameConflict || rule.hasSwatchConflict) ? (
                      <div style={{ display: "flex", gap: 5, flexWrap: "wrap", marginTop: 6 }}>
                        {getColourRuleStandardChoices(rule).map((choice) => {
                          const key = `${rule.brand}|${rule.colourCode}|${choice.colourName}|${choice.colourHex}`;
                          return <button key={key} type="button" className="btn btn-sm btn-ghost" onClick={() => openColourRuleStandardEditor(rule, choice.colourName, choice.colourHex, true)}>{choice.colourName} · {choice.colourHex || "Enter HEX / 填写色卡"}</button>;
                        })}
                      </div>
                    ) : <button type="button" className="btn btn-sm btn-ghost" onClick={() => openColourRuleStandardEditor(rule, rule.standardColourName ?? rule.colourName ?? "", rule.standardColourHex ?? "")}>Edit colour</button>}
                    {rule.sampleMaterialCodes.length > 0 ? <div style={{ fontSize: 9, color: "#94a3b8", marginTop: 4 }}>{rule.sampleMaterialCodes.join(" · ")}</div> : null}
                  </div>
                ))}
              </div>
              <div className="bom-finance-action-bar"><button type="button" className="btn btn-sm btn-ghost" onClick={() => setColourRuleDetailsStatus(null)}>Close</button></div>
            </section>
          </div>
        </div>
      ) : null}
      {showColourRulePreview ? (
        <div className="bom-finance-modal-backdrop" onClick={() => { if (!applyingColourRulePreview) setShowColourRulePreview(false); }}>
          <div className="bom-colour-code-edit-modal-shell" onClick={(event) => event.stopPropagation()}>
            <section className="bom-colour-code-edit-card" role="dialog" aria-modal="true" aria-label="Colour rule fill preview">
              <div className="bom-colour-code-edit-head"><div><span className="bom-finance-eyebrow">COLOUR SWATCH RULES · PREVIEW</span><h4>{colourRuleApplyResult ? "Missing fields filled / 已补缺" : "Confirm Brand + Code standards"}</h4><p>{colourRulePreview ? `${colourRulePreview.ruleCount} groups · ${colourRulePreview.total} material updates / 待更新物料` : "Checking current rules..."}</p></div></div>
              <p>Fill missing names / HEX only. Keep existing values; never guess colours. / 仅补缺名称及色卡，保留已有值，不猜色。</p>
              {colourRulePreview ? <p>{colourRulePreview.items.filter(item => item.oldColourName !== item.newColourName).length} names / 名称 · {colourRulePreview.items.filter(item => item.oldColourHex !== item.newColourHex).length} swatches / 色卡</p> : null}
              {loadingColourRulePreview ? <p>Building preview...</p> : null}
              {colourRulePreview && colourRulePreview.unresolvedConflictCount > 0 ? (
                <div style={{ color: "#b45309", fontSize: 12 }}>
                  {colourRulePreview.unresolvedConflictCount} conflict groups excluded from batch / 冲突组已排除，需逐组确认。
                  <button type="button" className="btn btn-sm btn-ghost" onClick={() => { setShowColourRulePreview(false); setColourRuleDetailsStatus("name_conflict"); }}>Review name conflicts / 查看名称冲突</button>
                  <button type="button" className="btn btn-sm btn-ghost" onClick={() => { setShowColourRulePreview(false); setColourRuleDetailsStatus("swatch_conflict"); }}>Review HEX conflicts / 查看色卡冲突</button>
                </div>
              ) : null}
              {colourRuleActionError ? <div className="bom-colour-code-edit-error">{colourRuleActionError}</div> : null}
              {colourRuleApplyResult ? <div style={{ color: "#0f766e", fontWeight: 700 }}>{colourRuleApplyResult.updated} materials filled / 已补缺 · {colourRuleApplyResult.missingRules} unresolved / 待确认</div> : null}
              {colourRulePreview ? (
                <div style={{ maxHeight: "48vh", overflowY: "auto", border: "1px solid #e2e8f0" }}>
                  {colourRulePreview.rules.map((rule) => (
                    <div key={`${rule.brand}|${rule.colourCode}`} style={{ padding: 8, borderBottom: "1px solid #e2e8f0", fontSize: 11 }}>
                      <span
                        aria-label={`${rule.colourName} ${rule.colourHex}`}
                        style={{
                          display: "inline-block",
                          width: 18,
                          height: 18,
                          marginRight: 6,
                          verticalAlign: "middle",
                          border: "1px solid #cbd5e1",
                          background: parseOrderGeniusColourSwatch(rule.colourHex).background,
                        }}
                      />
                      <strong>{rule.brand} · {rule.colourCode}</strong> · {rule.colourName} · {rule.colourHex || "Missing HEX / 缺色卡"}
                      <br />
                      <span style={{ color: "#64748b" }}>
                        {rule.source === "persistent_rule" ? "Confirmed same-code standard / 已确认同码标准" : "Unique same-code values / 唯一同码值"} · {rule.skuCount} SKUs
                      </span>
                      {colourRulePreview.items.filter(item => item.brand === rule.brand && item.colourCode === rule.colourCode).map(item => <div key={item.materialCode}>
                        <code>{item.materialCode}</code> · {item.oldColourName || "Missing name / 缺名称"} → {item.newColourName} · {item.oldColourHex || "Missing HEX / 缺色卡"} → {item.newColourHex || "Keep missing / 仍缺色卡"}
                      </div>)}
                    </div>
                  ))}
                  {colourRulePreview.rules.length === 0 ? <div style={{ padding: 10, color: "#64748b", fontSize: 11 }}>No conflict-free standards to confirm / 当前没有可批量确认的无冲突标准。</div> : null}
                </div>
              ) : null}
              <div className="bom-finance-action-bar">
                {!colourRuleApplyResult && colourRulePreview?.rules.length ? <button type="button" className="btn btn-sm btn-primary" disabled={applyingColourRulePreview} onClick={() => void handleApplyColourRuleFills()}>{applyingColourRulePreview ? "Saving..." : `Confirm ${colourRulePreview.rules.length} shared standards`}</button> : null}
                <button type="button" className="btn btn-sm btn-ghost" disabled={applyingColourRulePreview} onClick={() => setShowColourRulePreview(false)}>Close</button>
              </div>
            </section>
          </div>
        </div>
      ) : null}
      {colourTierReview ? (
        <div className="bom-finance-modal-backdrop" onClick={() => setColourTierReview(null)}>
          <div className="bom-colour-code-edit-modal-shell" onClick={(event) => event.stopPropagation()}>
            <section className="bom-colour-code-edit-card" role="dialog" aria-modal="true" aria-label="Colour tier price review">
              <div className="bom-colour-code-edit-head"><div><span className="bom-finance-eyebrow">COLOUR TIER · PRICE REVIEW</span><h4>{colourTierReview.report.colourCode || colourTierReview.report.materialCode}: {colourTierReview.previousTier} → {colourTierReview.nextTier}</h4><p>{colourTierReview.report.brand} / {colourTierReview.nextTier} / {colourTierReview.report.surchargeEur == null ? "rule unavailable" : `+${colourTierReview.report.surchargeEur.toLocaleString()} EUR`}</p></div></div>
              <div style={{ fontSize: 11, fontWeight: 700 }}>{colourTierReview.report.rows} countries scanned · {colourTierReview.report.updated} updated · {colourTierReview.report.skippedManual} manual FOB skipped · {colourTierReview.report.skippedNoBase} missing Single base · {colourTierReview.report.skippedAmbiguous} ambiguous base · {colourTierReview.report.skippedMissingTier} missing tier · {colourTierReview.report.skippedMissingRule} missing rule</div>
              <div style={{ maxHeight: "48vh", overflowY: "auto", border: "1px solid #e2e8f0" }}>
                {colourTierReview.report.details.map((detail) => <div key={detail.countryCode} style={{ padding: 7, borderBottom: "1px solid #e2e8f0", fontSize: 11 }}><strong>{detail.countryCode}</strong> · {detail.oldFinalFobEur?.toLocaleString() ?? "-"} → {detail.newFinalFobEur?.toLocaleString() ?? "-"} · surcharge {detail.colourSurchargeEur?.toLocaleString() ?? "-"}{detail.reason ? ` · ${detail.reason}` : ""}</div>)}
              </div>
              <div className="bom-finance-action-bar"><button type="button" className="btn btn-sm btn-primary" onClick={() => setColourTierReview(null)}>Done</button></div>
            </section>
          </div>
        </div>
      ) : null}
      {financeDrawerScope ? (
        <div className="bom-finance-modal-backdrop" onClick={closeFinanceDrawer}>
          <div className="bom-finance-modal-shell" onClick={(event) => event.stopPropagation()}>
            <FlipToolCard
              flipped={financeDrawerFlipped}
              ariaLabel="Country CBU finance card"
              height="min(72vh, 720px)"
              minHeight="420px"
              className="bom-finance-flip-card"
              frontClassName="bom-finance-flip-face bom-finance-flip-front"
              backClassName="bom-finance-flip-face bom-finance-flip-back"
              front={
                <div className="bom-finance-card-front">
                  <div>
                    <span className="bom-finance-eyebrow">BOM ADMIN</span>
                    <h3>{financeDrawerScope.countryCode} CBU</h3>
                  </div>
                  <div className="bom-finance-card-front-actions">
                    <button
                      type="button"
                      className="btn btn-sm btn-primary"
                      onClick={() => setFinanceDrawerFlipped(true)}
                    >
                      Open CBU
                    </button>
                    <button
                      type="button"
                      className="btn btn-sm btn-ghost"
                      onClick={closeFinanceDrawer}
                    >
                      Close
                    </button>
                  </div>
                </div>
              }
              back={
                <div className="bom-finance-card-back">
                  <div className="bom-finance-card-back-actions">
                    <span className="bom-finance-eyebrow">BOM ADMIN · CBU DETAIL</span>
                    <button
                      type="button"
                      className="btn btn-sm btn-ghost"
                      onClick={closeFinanceDrawer}
                    >
                      Close
                    </button>
                  </div>
                  <MaterialFinanceWorkbench
                    countryCode={financeDrawerScope.countryCode}
                    countryCodes={sortedCountries}
                    rows={financeDrawerRows}
                    loading={financeDrawerLoading}
                    error={financeError}
                    savingMaterialCode={savingFinanceMaterialCode}
                    onCountryChange={handleFinanceDrawerCountryChange}
                    onSaveRow={handleFinanceSave}
                  />
                </div>
              }
            />
          </div>
        </div>
      ) : null}
      {colourCodeEditor ? (
        <ConfirmDialog
          className="bom-colour-confirmation"
          title={confirmingColourEdit ? (colourCodeChanged ? "Confirm code correction / 确认色码纠错" : confirmCodeOnly ? "Confirm code / 确认色码" : "Confirm shared colour / 确认共享颜色") : `Edit colour · ${colourCodeEditor.brand} · ${colourCodeEditor.currentColourCode}`}
          description={confirmCodeOnly ? `Confirm this material only / 仅确认当前物料：${colourCodeEditor.materialCode}` : colourCodeChanged
            ? `Correct one material / 纠正一条物料：${colourCodeEditor.materialCode}`
            : `Affects ${colourScopeCount ?? "?"} materials / 影响该品牌＋色码的有效物料`}
          cancelLabel={confirmingColourEdit ? "Back / 返回编辑" : "Cancel"}
          confirmLabel={confirmCodeOnly ? "Confirm code" : needsCodeConfirmation ? "Save & confirm code" : confirmingColourEdit ? "Confirm save / 确认保存" : "Save"}
          loadingLabel="Saving… / 保存中…"
          submitting={savingColourCodeEditor}
          confirmDisabled={(!confirmCodeOnly && !isSharedSwatchDraftValid(colourCodeEditor)) || (!confirmCodeOnly && !colourCodeChanged && colourScopeCount == null) || (colourCodeChanged && loadingColourCodeRuleLookup)}
          error={colourCodeEditorError ? { title: "Colour update / 颜色更新", message: colourCodeEditorError } : null}
          onCancel={() => { setColourCodeEditorError(""); if (confirmingColourEdit) setConfirmingColourEdit(false); else setColourCodeEditor(null); }}
          onConfirm={() => { if (confirmingColourEdit) void saveConfirmedColour(); else handleColourCodeEditorSubmit(); }}
        >
          {confirmingColourEdit ? (
            <div className="bom-colour-edit-fields">
              {colourCodeChanged ? <>
                <p><strong>{colourCodeEditor.currentColourCode} → {colourCodeEditor.nextColourCode}</strong></p>
                <p>{colourCodeEditor.materialCode} → {correctedMaterialCode}</p>
                <p>Only an unreferenced material can be corrected in place. With PI references, add/copy a new version and archive the original. / 无 PI 引用才可原地纠错；已有订单请新增或复制新版并归档旧版。</p>
                {submittedColourHex ? <p>The target {colourCodeEditor.brand} · {colourCodeEditor.nextColourCode} standard also updates {(colourHexRules.find(rule => rule.brand === colourCodeEditor.brand && rule.colourCode === colourCodeEditor.nextColourCode)?.skuCount ?? 0) + Number(skus.some(sku => sku.materialCode === colourCodeEditor.materialCode && sku.isActive !== false))} effective materials. / 同时同步目标色码组名称及色卡。</p> : null}
              </> : confirmCodeOnly ? <p>Confirm this material only; no name, HEX or order fields change. / 仅确认当前物料色码，不修改名称、色卡或订单字段。</p>
                : <p>Shared name / HEX only. All PIs display the latest library HEX; saved material codes, BOM, descriptions and prices stay unchanged. / 仅同步共享颜色；所有 PI 展示最新库色卡，订单物料号、BOM、描述及价格不变。</p>}
              <div className="bom-colour-edit-comparison">
                {[{ label: "Before / 修改前", name: colourCodeEditor.currentColourName, hex: colourCodeEditor.currentColourHex },
                  { label: "After / 修改后", name: colourCodeEditor.nextColourName, hex: submittedColourHex ?? keptColourHex }].map(value => (
                  <div key={value.label}>
                    <strong>{value.label}</strong><span>{value.name || "No standard / 未定标准"}</span>
                    <span className="bom-colour-edit-swatch" style={{ background: parseOrderGeniusColourSwatch(value.hex).background }} />
                    <code>{value.hex || "Missing HEX / 缺色卡"}</code>
                  </div>
                ))}
              </div>
              {!submittedColourHex ? <p>Keep each material's existing swatch / 保留每条物料原有色卡。</p> : null}
              {colourCodeChanged && !submittedColourHex && colourCodeEditor.storedColourHex !== colourCodeEditor.currentColourHex ? <p>Current shared display differs; correction keeps this material's stored swatch shown above. / 当前共享显示与物料保存值不同，本次纠错保留上方实际保存色卡。</p> : null}
              {colourCodeEditor.suggestionSource ? <p>{colourCodeEditor.suggestionSource}</p> : null}
            </div>
          ) : (
            <div className="bom-colour-edit-fields">
              {colourScopeCount == null ? <button type="button" className="btn btn-sm btn-ghost" disabled={loadingColourHexRules} onClick={() => void loadColourHexRules()}>Reload colour scope / 重读颜色范围</button> : null}
              {colourCodeEditor.materialCode ? <div className="bom-colour-edit-code">
                <span>{colourCodeEditor.brand} · {colourCodeEditor.currentColourCode}</span>
                <button type="button" className="btn btn-sm btn-ghost" onClick={() => setColourCodeEditor(prev => {
                  if (!prev) return prev;
                  const original = splitColourHexValue(prev.currentColourHex);
                  return { ...prev, correctingCode: !prev.correctingCode, nextColourCode: prev.currentColourCode, swatchAccepted: false, ...(prev.correctingCode ? { nextColourName: prev.currentColourName, nextColourHex: original.hex1, nextColourHex2: original.hex2, isDualSwatch: original.isDual, colourNameTouched: false, colourHexTouched: false, suggestionSource: null } : {}) };
                })}>
                  {colourCodeEditor.correctingCode ? "Cancel correction" : "Correct code"}
                </button>
              </div> : null}
              {colourCodeEditor.correctingCode ? <label>
                Code / 色码
                <input ref={colourCodeEditorInputRef} aria-label="Colour code" value={colourCodeEditor.nextColourCode} maxLength={4} onChange={event => {
                  const nextCode = event.target.value.replace(/[^a-zA-Z0-9]/g, "").slice(0, 4).toUpperCase();
                  colourCodeLookupRequestRef.current += 1;
                  setColourCodeRuleLookup(null);
                  setColourCodeEditor(prev => {
                    if (!prev) return prev;
                    const sameCode = nextCode === prev.currentColourCode;
                    const original = splitColourHexValue(prev.currentColourHex);
                    return { ...prev, nextColourCode: nextCode, nextColourName: sameCode ? prev.currentColourName : "", nextColourHex: sameCode ? original.hex1 : "", nextColourHex2: sameCode ? original.hex2 : "", isDualSwatch: sameCode && original.isDual, colourNameTouched: false, colourHexTouched: false, suggestionSource: null, swatchAccepted: false };
                  });
                  setColourCodeEditorError("");
                }} />
                <small>{colourCodeEditor.materialCode} → {correctedMaterialCode}</small>
              </label> : null}
              <label>Colour name / 颜色名称
                <input aria-label="Shared colour name" value={colourCodeEditor.nextColourName} onChange={event => setColourCodeEditor(prev => prev ? { ...prev, nextColourName: event.target.value, colourNameTouched: true } : prev)} />
              </label>
              <label className="bom-colour-edit-dual"><input type="checkbox" checked={colourCodeEditor.isDualSwatch} onChange={event => setColourCodeEditor(prev => prev ? { ...prev, isDualSwatch: event.target.checked, colourHexTouched: true } : prev)} /> Dual swatch / 双色色卡（不改变加价档位）</label>
              {[{ label: "Primary HEX", key: "nextColourHex" as const }, ...(colourCodeEditor.isDualSwatch ? [{ label: "Second HEX", key: "nextColourHex2" as const }] : [])].map(field => (
                <div key={field.key}><span>{field.label}</span>
                  <div className="bom-colour-edit-hex">
                    <span className="bom-colour-picker">
                      <span className="bom-colour-edit-swatch" style={{ background: parseOrderGeniusColourSwatch(colourCodeEditor[field.key]).background }} />
                      <input type="color" aria-label={`Choose ${field.label}`} value={normalizeColourPickerValue(colourCodeEditor[field.key])} onChange={event => setColourCodeEditor(prev => prev ? { ...prev, [field.key]: event.target.value.toUpperCase(), colourHexTouched: true, suggestionSource: null } : prev)} />
                    </span>
                    <input aria-label={field.label} placeholder="#RRGGBB" value={colourCodeEditor[field.key]} onChange={event => setColourCodeEditor(prev => prev ? { ...prev, [field.key]: event.target.value.toUpperCase(), colourHexTouched: true, suggestionSource: null } : prev)} />
                  </div>
                </div>
              ))}
              <small>HEX is optional. Empty keeps existing swatches. / HEX 可留空，保留原有色卡。</small>
              {loadingColourCodeRuleLookup ? <small>Checking Brand + Code rule… / 正在查询共享标准…</small> : null}
              {colourCodeRuleLookup?.hasNameConflict || colourCodeRuleLookup?.hasSwatchConflict ? <small>Brand + Code conflict: not auto-filled. Enter values manually. / 共享标准冲突，请确认后填写。</small> : null}
              {colourSuggestions.map(candidate => {
                const source = candidate.colourCode ? `Suggested from ${candidate.brand} · ${candidate.colourCode}` : "Approximate swatch from name / 名称近似色卡";
                return <div className="bom-colour-edit-suggestion" key={`${source}|${candidate.colourHex}`}>
                  <span className="bom-colour-edit-swatch" style={{ background: parseOrderGeniusColourSwatch(candidate.colourHex).background }} />
                  <span>{source}<br /><code>{candidate.colourHex}</code></span>
                  <button type="button" className="btn btn-sm btn-ghost" onClick={() => {
                    const swatch = splitColourHexValue(candidate.colourHex);
                    setColourCodeEditor(prev => prev ? { ...prev, nextColourName: prev.nextColourName || candidate.colourName || "", nextColourHex: swatch.hex1, nextColourHex2: swatch.hex2, isDualSwatch: swatch.isDual, colourHexTouched: true, suggestionSource: source, swatchAccepted: true } : prev);
                  }}>Use this swatch</button>
                </div>;
              })}
            </div>
          )}
        </ConfirmDialog>
      ) : null}
      {addColourEditor ? (
        <div
          className="bom-finance-modal-backdrop"
          onClick={() => {
            if (!savingAddColourEditor) setAddColourEditor(null);
          }}
        >
          <div className="bom-add-colour-edit-modal-shell" onClick={(event) => event.stopPropagation()}>
            <form className="bom-add-colour-edit-card" onSubmit={handleAddColourEditorSubmit}>
              <div className="bom-add-colour-edit-head">
                <div>
                  <span className="bom-finance-eyebrow">BOM ADMIN · ADD COLOUR</span>
                  <h4>Add colour SKU</h4>
                  <p>{addColourEditor.modelName} · {addColourEditor.version} · {addColourEditor.tierName}</p>
                </div>
                <div className={`bom-add-colour-tier-pill is-${addColourEditor.tierName}`}>
                  {addColourEditor.tierName}
                </div>
              </div>
              <div className="bom-add-colour-preview">
                <span>Material preview</span>
                <strong>
                  {addColourEditor.colourCode
                    ? resolveMaterialCodeFromTemplate(
                      addColourEditor.bomTemplate,
                      addColourEditor.colourCode,
                      addColourEditor.sourceMaterialCode,
                    )
                    : addColourEditor.bomTemplate.replace("**", "__")}
                </strong>
              </div>
              <div className="bom-add-colour-edit-grid">
                <label>
                  <span>Colour code</span>
                  <input
                    ref={addColourEditorCodeRef}
                    type="text"
                    value={addColourEditor.colourCode}
                    maxLength={4}
                    placeholder="KY"
                    onChange={(event) => {
                      const colourCode = event.target.value.replace(/[^a-zA-Z0-9]/g, "").slice(0, 4).toUpperCase();
                      addColourLookupRequestRef.current += 1;
                      setAddColourRuleLookup(null);
                      setAddColourEditor((prev) => prev ? { ...prev, colourCode, colourName: "", colourHex: MISSING_COLOUR_SWATCH_HEX, colourHex2: MISSING_COLOUR_SWATCH_HEX, isDualSwatch: prev.tierName === "dual", colourNameTouched: false, colourHexTouched: false } : prev);
                      setAddColourEditorError("");
                    }}
                  />
                </label>
                <label>
                  <span>Colour name optional</span>
                  <input
                    ref={addColourEditorNameRef}
                    type="text"
                    value={addColourEditor.colourName}
                    placeholder="Required when code is new"
                    onChange={(event) => {
                      setAddColourEditor((prev) => prev ? { ...prev, colourName: event.target.value, colourNameTouched: true } : prev);
                      setAddColourEditorError("");
                    }}
                  />
                </label>
              </div>
              {loadingAddColourRuleLookup ? <div className="bom-add-colour-edit-note">Checking Brand + Code rule...</div> : addColourRuleLookup ? <div className="bom-add-colour-edit-note">{formatColourRuleLookupNote(addColourRuleLookup, addColourEditor.colourNameTouched || addColourEditor.colourHexTouched)}</div> : null}
              {addColourRuleLookup?.source === "name_candidates" && addColourRuleLookup.nameCandidates.length > 0 ? (
                <div className="bom-add-colour-edit-note">
                  <span>Choose an existing name match:</span>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 4, marginTop: 4 }}>
                    {addColourRuleLookup.nameCandidates.map((candidate) => (
                      <button
                        key={`${candidate.colourCode}-${candidate.colourName || ""}-${candidate.colourHex || ""}`}
                        type="button"
                        className="btn btn-sm btn-ghost"
                        disabled={!candidate.colourName || !candidate.colourHex || candidate.hasNameConflict || candidate.hasSwatchConflict}
                        onClick={() => {
                          const swatch = splitColourHexValue(candidate.colourHex);
                          setAddColourEditor((prev) => prev ? {
                            ...prev,
                            colourName: candidate.colourName || prev.colourName,
                            colourHex: swatch.hex1,
                            colourHex2: swatch.hex2,
                            isDualSwatch: swatch.isDual,
                            colourNameTouched: true,
                            colourHexTouched: true,
                          } : prev);
                        }}
                      >
                        {candidate.colourCode} · {candidate.colourName || "no name"} · {candidate.colourHex || "no swatch"}
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
              <div className="bom-colour-swatch-option">
                <span>Swatch optional</span>
                <div className={`bom-colour-swatch-controls${addColourEditor.isDualSwatch ? " is-dual" : ""}`}>
                  <input
                    type="color"
                    value={normalizeColourPickerValue(addColourEditor.colourHex)}
                    onChange={(event) => setAddColourEditor((prev) => prev ? { ...prev, colourHex: event.target.value.toUpperCase(), colourHexTouched: true } : prev)}
                  />
                  <input
                    type="text"
                    value={addColourEditor.colourHex}
                    onChange={(event) => {
                      const colourHex = event.target.value.toUpperCase();
                      setAddColourEditor((prev) => prev ? { ...prev, colourHex, colourHexTouched: true } : prev);
                    }}
                  />
                  {addColourEditor.isDualSwatch ? (
                    <>
                      <input
                        type="color"
                        value={normalizeColourPickerValue(addColourEditor.colourHex2, normalizeColourPickerValue(addColourEditor.colourHex))}
                        onChange={(event) => setAddColourEditor((prev) => prev ? { ...prev, colourHex2: event.target.value.toUpperCase(), colourHexTouched: true } : prev)}
                      />
                      <input
                        type="text"
                        value={addColourEditor.colourHex2}
                        onChange={(event) => {
                          const colourHex2 = event.target.value.toUpperCase();
                          setAddColourEditor((prev) => prev ? { ...prev, colourHex2, colourHexTouched: true } : prev);
                        }}
                      />
                    </>
                  ) : null}
                  <button
                    type="button"
                    className="btn btn-sm btn-ghost"
                    onClick={() => setAddColourEditor((prev) => prev ? { ...prev, colourHex: "", colourHex2: "", colourHexTouched: true } : prev)}
                  >
                    Clear
                  </button>
                  {!addColourEditor.isDualSwatch ? <button type="button" className="btn btn-sm btn-ghost" onClick={() => setAddColourEditor((prev) => prev ? { ...prev, isDualSwatch: true, colourHex2: normalizeColourPickerValue(prev.colourHex), colourHexTouched: true } : prev)}>Dual swatch</button> : null}
                </div>
              </div>
              <div className="bom-add-colour-edit-note">
                New SKU will inherit product fields, interior and lifecycle from {addColourEditor.sourceMaterialCode}. Positive FOB values will be copied from {addColourEditor.fobSourceCountries} countries.
              </div>
              {addColourEditorError ? <div className="bom-colour-code-edit-error">{addColourEditorError}</div> : null}
              <div className="bom-finance-action-bar">
                <button
                  type="submit"
                  className="btn btn-sm btn-primary bom-finance-action-button"
                  disabled={savingAddColourEditor}
                >
                  {savingAddColourEditor ? "Creating..." : "Create colour"}
                </button>
                <button
                  type="button"
                  className="btn btn-sm btn-ghost bom-finance-action-button"
                  disabled={savingAddColourEditor}
                  onClick={() => setAddColourEditor(null)}
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}
      {isAdmin && showAddMaterial && (
        <div
          onKeyDown={(event) => {
            if (event.key === "Enter" && !(event.target instanceof HTMLTextAreaElement)) {
              event.preventDefault();
              void handleCreateMaterial();
            }
            if (event.key === "Escape") {
              event.preventDefault();
              setShowAddMaterial(false);
              setAddMaterialError("");
              setAddMaterialNotice("");
            }
          }}
          style={{ display: "flex", gap: 6, marginBottom: 8, padding: 6, background: '#f8fafc', borderRadius: 4, flexWrap: "wrap", alignItems: "stretch" }}
        >
          <input ref={materialCodeInputRef} type="text" placeholder="Material Code" value={newMaterial.materialCode}
            onChange={e => setNewMaterial({...newMaterial, materialCode: e.target.value})}
            style={{ width: 150, fontSize: 11, fontFamily: 'monospace' }} />
          <input type="text" placeholder="Brand" value={newMaterial.brand}
            onChange={e => setNewMaterial({...newMaterial, brand: e.target.value})}
            style={{ width: 76, fontSize: 11 }} />
          <input type="text" placeholder="Model" value={newMaterial.modelName}
            onChange={e => setNewMaterial({...newMaterial, modelName: e.target.value})}
            style={{ width: 112, fontSize: 11 }} />
          <input type="text" placeholder="Version" value={newMaterial.version}
            onChange={e => setNewMaterial({...newMaterial, version: e.target.value})}
            style={{ width: 96, fontSize: 11 }} />
          <input type="text" placeholder="Colour" value={newMaterial.colour}
            onChange={e => setNewMaterial({...newMaterial, colour: e.target.value})}
            style={{ width: 96, fontSize: 11 }} />
	          <input type="text" placeholder="Code" value={newMaterial.colourCode}
	            onChange={e => setNewMaterial({...newMaterial, colourCode: e.target.value})}
	            style={{ width: 60, fontSize: 11 }} />
	          <CommandSelect
	            value={newMaterial.powertrain}
	            options={POWERTRAIN_COMMAND_OPTIONS}
	            placeholder="Choose powertrain"
	            searchPlaceholder="Search powertrain..."
	            className="bom-add-powertrain-select"
	            onValueChange={(powertrain) => setNewMaterial({...newMaterial, powertrain})}
	          />
          <div style={{ display: "inline-flex", gap: 6, flexShrink: 0 }}>
            <button className="btn btn-sm btn-primary" onClick={async () => {
              await handleCreateMaterial();
            }}>Add</button>
            <button className="btn btn-sm btn-ghost" onClick={() => { setShowAddMaterial(false); setAddMaterialError(""); setAddMaterialNotice(""); }}>Cancel</button>
          </div>
	          <textarea
	            rows={3}
	            placeholder="BW Khaki white; CL Carbon black; Z9 Galaxy Blue"
	            title="Batch colours: BW Khaki white; CL Carbon crystal black; Z9 Galaxy Blue #1F5F9F"
	            value={newMaterial.colourBatch}
	            onChange={e => setNewMaterial({...newMaterial, colourBatch: e.target.value})}
	            style={{ flex: "1 1 100%", minWidth: 0, minHeight: 62, fontSize: 11, resize: "vertical", lineHeight: 1.35, padding: "6px 8px" }}
	          />
          {addMaterialNotice ? (
            <div style={{ flexBasis: "100%", color: "#2563eb", fontSize: 11, fontWeight: 600 }}>
              {addMaterialNotice}
            </div>
          ) : null}
          {addMaterialDraftSummary ? (
            <div
              style={{
                flexBasis: "100%",
                color: addMaterialDraftSummary.startsWith("Line") || addMaterialDraftSummary.startsWith("Use") || addMaterialDraftSummary.startsWith("Batch")
                  ? "#b45309"
                  : "#2563eb",
                fontSize: 11,
                fontWeight: 600,
              }}
            >
              {addMaterialDraftSummary}
            </div>
          ) : null}
          {addMaterialError ? (
            <div style={{ flexBasis: "100%", color: "#dc2626", fontSize: 11, fontWeight: 600 }}>
              {addMaterialError}
            </div>
          ) : null}
        </div>
      )}
      <div style={{ overflowY: "auto", overflowX: "hidden", maxHeight: toolsFlipped ? "calc(94vh - 340px)" : "calc(94vh - 210px)", minHeight: 320 }}>
        {sortedModelGroupEntries.map(([mk, mg]) => {
          const expanded = expandedGroups.has(mk);
          return (
            <div
              key={mk}
              ref={(node) => {
                bomGroupRefs.current[mk] = node;
              }}
              className="bom-admin-model-group"
              style={{ marginBottom: 2 }}
            >
              <div
                role="button"
                tabIndex={0}
                aria-expanded={expanded}
                onClick={() => toggleGroup(mk)}
                onKeyDown={(event) => {
                  if (event.key !== "Enter" && event.key !== " ") return;
                  event.preventDefault();
                  toggleGroup(mk);
                }}
                style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 10px", cursor: "pointer",
                  background: `${PT_COLORS[mg.pt] ?? '#9ca3af'}15`, borderLeft: `4px solid ${PT_COLORS[mg.pt] ?? '#9ca3af'}`, borderRadius: 2, fontWeight: 700, fontSize: 13 }}>
                <span style={{ fontSize: 14, flexShrink: 0 }}>{expanded ? '▾' : '▸'}</span>
                <span
                  title={`${mg.brand} ${mg.modelName}`}
                  style={{ color: PT_COLORS[mg.pt] ?? '#9ca3af', minWidth: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                >
                  {mg.brand} {mg.modelName}
                </span>
                <span style={{ fontWeight: 400, color: "#64748b", fontSize: 11, flexShrink: 0 }}>· {mg.pt} · {mg.versions.size} versions</span>
              </div>
              {expanded ? (
                <div className="bom-admin-model-group-body">
                {(sortedVersionEntriesByModelKey.get(mk) || []).map(([vk, vSkus]) => {
                const sortedTemplates = sortedTemplateEntriesByVersionKey.get(`${mk}|${vk}`) || [];
                return (
                  <div key={mk + '|' + vk} style={{ marginLeft: 20, marginBottom: 8 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: '#334155', padding: '4px 0', marginBottom: 2 }}>
                      {vk} · {vSkus.length} colour-SKUs · {sortedTemplates.length} BOM templates
                    </div>
                    <div className="bom-admin-table-scroll">
                      <table className="data-table bom-admin-table" style={{ fontSize: 11, width: bomAdminTableMinWidth, minWidth: bomAdminTableMinWidth, tableLayout: "fixed" }}>
                      {renderBomAdminColumnGroup()}
                      <thead>
                        <tr style={{ position: "sticky", top: 0, zIndex: 2 }}>
                          <th title="BOM template" style={{ ...bomHeaderBaseStyle, ...getBomStickyCellStyle("bom", "#334155", 3) }}>BOM</th>
                          <th title="Interior" style={{ ...bomHeaderBaseStyle, ...getBomStickyCellStyle("interior", "#334155", 3) }}>INT</th>
                          <th title="Single colour tier" style={{ ...bomHeaderBaseStyle, ...getBomStickyCellStyle("single", "#334155", 3) }}>Single</th>
                          <th title="Dual colour tier" style={{ ...bomHeaderBaseStyle, ...getBomStickyCellStyle("dual", "#334155", 3) }}>Dual</th>
                          <th title="Special colour tier" style={{ ...bomHeaderBaseStyle, ...getBomStickyCellStyle("special", "#334155", 3) }}>Spec</th>
                          <th title="Lifecycle status and active window" style={{ ...bomHeaderBaseStyle, width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }}>LC</th>
                          <th title="Actions" style={{ ...bomHeaderBaseStyle, width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions }}>Actions</th>
                          <th
                            colSpan={2}
                            title="Father material note"
                            style={{
                              ...bomHeaderBaseStyle,
                              width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from + BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to,
                              minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from + BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to,
                            }}
                          >
                            Note
                          </th>
                          {sortedCountries.map(c => (
                            <th key={c} title={countryTooltipByCode.get(c) || formatCountryCodeTooltip(c)} style={{ width: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, minWidth: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, textAlign: "center", color: c === 'NL' ? '#d97706' : '#64748b', fontWeight: c === 'NL' ? 700 : 600 }}>
                              <button
                                type="button"
                                className={`bom-country-cbu-trigger${c === "NL" ? " is-nl" : ""}`}
                                title={`${countryTooltipByCode.get(c) || formatCountryCodeTooltip(c)} · CBU detail`}
                                onClick={(event) => {
                                  event.stopPropagation();
                                  void openFinanceDrawer(buildFinanceDrawerScope(c, mg.brand, mg.modelName, mg.pt));
                                }}
                              >
                                <span className="bom-country-cbu-code">{c}</span>
                                <span className="bom-country-cbu-caret" aria-hidden="true" />
                              </button>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedTemplates.map(([bomTemplate, tiers]) => {
                          const allSkus = tiers.allSkus;
                          const ref = allSkus[0];
                          const isHist = ref.lifecycleStatus === 'historical';
                          const isPhaseOut = ref.lifecycleStatus === 'phase_out';
                          const lifecycleStatus = normalizeBomLifecycleStatus(ref.lifecycleStatus);
                          const allCodes = allSkus.map((s: any) => s.materialCode);
                          const intName = (ref as any).interiorColorName || '';
                          const edTag = (ref as any).editionTag || '';
                          const sourceInfo = (ref as any).sourcePayload || {};
                          const sourceLabel = formatBomSourceLabel(
                            String((ref as any).modelName || ""),
                            (ref as any).sourceSheetName || sourceInfo.sheet_name,
                            (ref as any).sourceRowNumber ?? sourceInfo.row_index,
                          );
                          const sourceWarnings = sourceInfo.warnings || [];
                          // Helper: render a tier cell with colour chips and drop zone
                          const draftKey = buildBomEditScopeKey(mk, vk, bomTemplate);
                          const editing = editingBoms.has(draftKey);
                          const currentBomTemplateRemark = getBomTemplateRemark(allSkus);
                          const productSaveMessage = productSaveMessages[draftKey];
                          const isSavingProduct = savingProductKey === draftKey;
                          const copyDraft = copyDrafts[draftKey];
                          const bulkFobEditor = bulkFobEditors[bomTemplate] || {
                            deltaEur: "",
                            selectedCountries: tiers.filledCountryCodes,
                          };
                          const editCountryOptions: BomEditCountryOption[] = tiers.countryCodes.map((countryCode) => {
                            const hasFob = tiers.filledCountryCodeSet.has(countryCode);
                            return {
                              code: countryCode,
                              hasFob,
                              selected: bulkFobEditor.selectedCountries.includes(countryCode),
                              title: `${countryCode}${countryLabels.get(countryCode) ? ` · ${countryLabels.get(countryCode)}` : ""}${hasFob ? "" : " · no FOB on this BOM yet"}`,
                              onToggle: (checked: boolean) => toggleBulkFobCountry(bomTemplate, allSkus, countryCode, checked),
                            };
                          });
                          const renderTierCell = (tierName: BomAdminColourTier, tierSkus: any[], borderColor: string, bgColor: string) => {
                            const isOver = editing && dragOverTier === tierName && dragSku && !tierSkus.some((s: any) => s.materialCode === dragSku);
                            const dragProps = editing ? {
                              onDragOver: (e: any) => {
                                e.preventDefault();
                                e.dataTransfer.dropEffect = 'move';
                                if (dragSku && !tierSkus.some((s: any) => s.materialCode === dragSku)) {
                                  setDragOverTier(tierName);
                                }
                              },
                              onDragEnter: () => { dragEnterCount.current++; },
                              onDragLeave: () => {
                                dragEnterCount.current--;
                                if (dragEnterCount.current <= 0) {
                                  dragEnterCount.current = 0;
                                  setDragOverTier(null);
                                }
                              },
                              onDrop: async (e: any) => {
                                e.preventDefault();
                                e.stopPropagation();
                                dragEnterCount.current = 0;
	                                const mc = dragMaterialCode.current;
	                                setDragSku(null);
	                                setDragOverTier(null);
	                                dragMaterialCode.current = null;
	                                if (mc && tierName && !tierSkus.some((s: any) => s.materialCode === mc)) {
	                                  const materialKey = bomMaterialKey(mc);
	                                  const previousTier = getEffectiveColourTier(allSkus.find((sku: any) => sku.materialCode === mc) || {});
	                                  setOptimisticColourTiers((prev) => ({ ...prev, [materialKey]: tierName }));
	                                  try {
	                                    const result = await api.updateColourTier(mc, tierName);
	                                    setColourTierReview({ previousTier, nextTier: tierName, report: result.reprice });
	                                    setBomAdminError("");
	                                    scheduleLoad(1200);
	                                  } catch (e) {
	                                    setOptimisticColourTiers((prev) => {
	                                      const next = { ...prev };
	                                      delete next[materialKey];
	                                      return next;
	                                    });
	                                    setBomAdminError(getErrorMessage(e));
	                                    console.error('Drag drop failed', e);
	                                  }
	                                }
                              },
                            } : {};
                            return (
                              <td
                                {...dragProps}
                                className="bom-admin-colour-tier-cell"
                                style={{
                                  padding: '3px 5px',
                                  outline: isOver ? `2px dashed ${borderColor}` : 'none',
                                  outlineOffset: -2,
                                  verticalAlign: 'middle',
                                  ...getBomStickyCellStyle(tierName, isOver ? bgColor : "#fff", 1),
                                }}>
                                <div className={`bom-admin-colour-tier-stack${tierSkus.length > 2 ? " is-multi-row" : ""}`} style={{ display: "flex", gap: 3, flexWrap: "wrap", alignItems: "center" }}>
                                  {tierSkus.length === 0 ? (
                                    <span style={{ fontSize: 9, color: '#cbd5e1' }}>—</span>
                                  ) : (
                                    tierSkus.map((s: any) => renderColourChip(s, isHist, editing))
                                  )}
                                  {editing && isAdmin ? (<span title={`Add colour to ${tierName} tier`}
                                    style={{ cursor: 'pointer', color: '#94a3b8', fontSize: 12, fontWeight: 700, padding: '0 3px' }}
                                    onClick={(e2: any) => {
                                      e2.stopPropagation();
                                      openAddColourEditor(bomTemplate, tierName, ref, tierSkus, allSkus);
                                    }}>＋</span>) : null}
                                </div>
                                {isOver ? <div style={{ fontSize: 9, color: borderColor, marginTop: 2 }}>Drop to reclassify</div> : null}
                                {editing && tierSkus.length === 0 ? (
                                  <div style={{ fontSize: 8, color: '#cbd5e1' }}>Drag here or click ＋</div>
                                ) : null}
                              </td>
                            );
                          };
                          return (
                            <Fragment key={bomTemplate}>
                            <tr
                              ref={(node) => {
                                bomTemplateRowRefs.current[bomMaterialKey(bomTemplate)] = node;
                              }}
                              style={isHist ? { opacity: 0.55, textDecoration: "line-through" }
                                   : isPhaseOut ? { opacity: 0.75 } : undefined}>
                              <td className="bom-admin-material-cell" style={{
                                borderLeft: `3px solid ${isHist ? '#9ca3af' : isPhaseOut ? '#d97706' : '#16a34a'}`,
                                color: isHist ? '#9ca3af' : '#1e293b',
                                maxWidth: 160,
                                overflow: "hidden",
                                ...getBomStickyCellStyle("bom", "white", 1),
                              }}>
                                <div className="bom-admin-material-main-line">
                                  {editing ? (
                                    <input className="bom-admin-inline-input" type="text" defaultValue={bomTemplate}
                                      placeholder="BOM / Material Code"
                                      onBlur={async (e) => {
                                        const v = e.target.value.trim();
                                        if (!v || v === bomTemplate) return;
                                        try {
                                          await api.updateBomTemplateMaterialCode(allCodes, v.toUpperCase());
                                          await load();
                                        } catch (err) {
                                          alert(getErrorMessage(err));
                                          e.target.value = bomTemplate;
                                        }
                                      }}
                                      style={{ fontFamily: "monospace", fontSize: 11, width: "100%", minWidth: BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom }} />
                                  ) : (
                                    <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                                      title={bomTemplate}>{bomTemplate}</div>
                                  )}
                                </div>
                                <div className="bom-admin-material-subline" style={{ color: sourceWarnings.length ? '#d97706' : '#94a3b8' }}>
                                  {sourceLabel}{sourceWarnings.length > 0 ? ` ⚠${sourceWarnings.length}` : ''}
                                </div>
                              </td>
                              <td className="bom-admin-interior-cell" style={getBomStickyCellStyle("interior", "white", 1)}>
                                <div className="bom-admin-interior-main-line">
                                  {editing ? (
                                    <input className="bom-admin-inline-input" type="text" defaultValue={intName + (edTag ? ` · ${edTag}` : '')}
                                      placeholder="Interior"
                                      onBlur={async (e) => {
                                        const v = e.target.value.trim();
                                        if (!v || v === intName + (edTag ? ` · ${edTag}` : '')) return;
                                        const edMatch = v.match(/^(.*?)\s*·\s*(.+)$/);
                                        const newInterior = edMatch ? edMatch[1].trim() : v;
	                                        const newEdition = edMatch ? edMatch[2].trim() : null;
	                                        for (const s of allSkus) {
	                                          try { await api.updateSkuInterior(s.materialCode, { interiorColorName: newInterior || null, editionTag: newEdition }); } catch {}
	                                        }
	                                        patchBomInterior(allCodes, newInterior || null, newEdition);
	                                        scheduleLoad(1200);
	                                      }}
	                                      style={{ fontSize: 10, width: '100%', minWidth: 80 }} />
                                  ) : (
                                    <span style={{ fontSize: 10, color: intName ? '#1e293b' : '#cbd5e1' }}>
                                      {intName || '—'}{edTag ? <span style={{ color: '#7c3aed' }}> · {edTag}</span> : null}
                                    </span>
                                  )}
                                </div>
                              </td>
                              {renderTierCell('single', tiers.single, '#16a34a', '#f0fdf4')}
                              {renderTierCell('dual', tiers.dual, '#2563eb', '#eff6ff')}
                              {renderTierCell('special', tiers.special, '#d97706', '#fffbeb')}
                              <td
                                className="bom-lifecycle-cell"
                                title={formatBomLifecycleTooltip(lifecycleStatus)}
                                style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }}
                              >
                                <div className="bom-lifecycle-readonly">
                                  <span className={`bom-lifecycle-pill bom-lifecycle-pill-${lifecycleStatus}`}>
                                    {formatBomLifecycleLabel(lifecycleStatus)}
                                  </span>
                                  {(ref.effectiveFrom || ref.effectiveTo) ? (
                                    <span className="bom-lifecycle-range">
                                      {ref.effectiveFrom || "Any"} → {ref.effectiveTo || "Open"}
                                    </span>
                                  ) : null}
                                </div>
                              </td>
                              <td className="bom-admin-actions-cell" style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, textAlign: "center" }}>
                                <div className="bom-admin-row-actions" style={{ display: "inline-flex", gap: 6, alignItems: "center", justifyContent: "center" }}>
                                  {!isAdmin ? null : pendingDeletes.has(bomTemplate) ? (
                                    <button className="btn btn-sm bom-admin-row-action-button" title="Click again to confirm delete"
                                      style={{ color: '#fff', background: '#dc2626' }}
                                      onClick={async () => {
                                        for (const s of allSkus) {
                                          try { await api.deleteMaterialSku(s.materialCode); } catch {}
                                        }
                                        setPendingDeletes(prev => { const n = new Set(prev); n.delete(bomTemplate); return n; });
                                        scheduleLoad(300);
                                      }}>Confirm?</button>
                                  ) : (
                                    <button className="btn btn-sm btn-ghost bom-admin-row-action-button" title="Delete permanently — double-click"
                                      style={{ color: '#dc2626' }}
                                      onClick={() => setPendingDeletes(new Set([bomTemplate]))}>Delete</button>
                                  )}
                                  <button className="btn btn-sm btn-ghost bom-admin-row-action-button"
                                    style={{ color: editing ? '#16a34a' : '#64748b' }}
                                    onClick={() => toggleEditBom(draftKey)}>
                                    {editing ? 'Done' : 'Edit'}
                                  </button>
                                </div>
                              </td>
                              <td
                                className="bom-admin-note-cell"
                                colSpan={2}
                                title={currentBomTemplateRemark || "No material note"}
                                style={{
                                  width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from + BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to,
                                  minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from + BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to,
                                }}
                              >
                                <span className={currentBomTemplateRemark ? "bom-material-note-summary" : "bom-material-note-empty"}>
                                  {currentBomTemplateRemark || "—"}
                                </span>
                              </td>
                              {sortedCountries.map(c => {
                                const fob = ref.fobByCountry?.[c];
                                const periods: CountryTemplateFobPeriod[] = ref.fobPeriodsByCountry?.[c] ?? [];
                                const latestPeriod = periods.reduce<CountryTemplateFobPeriod | null>((latest, period) => !latest || period.validFrom > latest.validFrom ? period : latest, null);
                                const hasConflict = fob?.status === "conflict";
                                const baseFob = getDraftBaseFob(fob);
                                const displayFob = latestPeriod ? latestPeriod.baseFobEur : baseFob;
                                const hasFob = displayFob != null;
                                const sourceMarker = latestPeriod ? `P${periods.length}` : getBomFobSourceMarker(fob?.fobSourceMode, fob?.fobSourceCountryCode);
                                const periodTooltip = latestPeriod ? [
                                  `${bomTemplate} / ${c} · Management summary, not a quote for today`,
                                  ...periods.map((period) => `${period.validFrom} → ${period.validTo || "Open"}: ${period.baseFobEur.toLocaleString()} EUR${period.baseFobEur === 0 ? " · Ordering paused" : ""}`),
                                  "Outside saved periods: no price; no default fallback.",
                                  `Undated default: ${baseFob?.toLocaleString() ?? "—"}${hasConflict ? " · requires review" : ""}`,
                                ].join("\n") : "";
                                const countryRemark = getBomCountryFobRemark(allSkus, c);
                                const hasCountryRemark = hasFob && countryRemark.length > 0;
                                const financeCountries = Array.isArray((ref as { financeCountries?: unknown }).financeCountries)
                                  ? ((ref as { financeCountries: string[] }).financeCountries)
                                  : [];
                                const hasFinance = financeCountries.includes(c);
                                return (
                                  <td key={c} className="bom-fob-price-cell" title={periodTooltip || (hasConflict ? "FOB 基准待确认：付款条件记录存在不同价格，先在 BOM Admin 保存模板＋国家 Single 基准" : formatBomFobTooltip(c, baseFob, fob?.colourSurchargeEur, fob?.fobSourceMode, fob?.fobSourceCountryCode, countryRemark))} style={{ width: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, minWidth: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, textAlign: "right", cursor: "pointer", padding: "2px 4px" }}
                                    onClick={() => {
                                      if (hasConflict && !latestPeriod) {
                                        setBomAdminError(`${bomTemplate} / ${c} 的 FOB 基准待确认；请先保存唯一 Single 基准。`);
                                        return;
                                      }
                                      if (c === "NL") {
                                        void openFinanceQuickCard({
                                          countryCode: c,
                                          materialCode: String(bomTemplate || ref.materialCode || allCodes[0] || ""),
                                          materialCodes: allCodes,
                                          title: `${c} finance · ${bomTemplate}`,
                                          fob: baseFob ?? null,
                                          remark: countryRemark,
                                          fobSourceMode: fob?.fobSourceMode ?? null,
                                          fobSourceCountryCode: fob?.fobSourceCountryCode ?? null,
                                        });
                                        return;
                                      }
                                      setEditFob({
                                        bomTemplate: String(bomTemplate || ""),
                                        materialCodes: allCodes,
                                        countryCode: c,
                                        fob: baseFob ?? null,
                                        originalFob: baseFob ?? null,
                                        remark: countryRemark,
                                        fobSourceMode: fob?.fobSourceMode ?? null,
                                        fobSourceCountryCode: fob?.fobSourceCountryCode ?? null,
                                      });
                                    }}>
                                    <span className="bom-fob-price-value" style={{ color: hasConflict && !latestPeriod ? "#b45309" : hasFob ? "#0f766e" : "#cbd5e1", fontWeight: hasFob || hasConflict ? 600 : 400 }}>
                                      {hasConflict && !latestPeriod ? "?" : displayFob != null ? displayFob.toLocaleString() : "—"}
                                      {hasFinance ? (
                                        <sup className="bom-finance-source-mark" title={`${c} finance / CBU maintained`}>
                                          %
                                        </sup>
                                      ) : null}
                                      {(sourceMarker || hasCountryRemark) ? (
                                        <sup
                                          className={`bom-fob-source-mark${hasCountryRemark ? " has-remark" : ""}${sourceMarker ? "" : " is-remark-only"}`}
                                          title={[
                                            latestPeriod ? periodTooltip : sourceMarker ? formatBomFobSourceLabel(fob?.fobSourceMode, fob?.fobSourceCountryCode) : "",
                                            hasCountryRemark ? `Remark: ${countryRemark}` : "",
                                          ].filter(Boolean).join(" · ")}
                                        >
                                          {sourceMarker ? <span className="bom-fob-source-mark-label">{sourceMarker}</span> : null}
                                          {hasCountryRemark ? (
                                            <span className="bom-fob-remark-mark" aria-label="Country FOB remark" />
                                          ) : null}
                                        </sup>
                                      ) : null}
                                    </span>
                                  </td>
                                );
                              })}
                            </tr>
                            {copyDraft ? (
                              <>
                              <tr style={{ background: "#eff6ff" }}>
                                <td
                                  style={{
                                    borderLeft: "3px solid #2563eb",
                                    maxWidth: 160,
                                    overflow: "hidden",
                                    ...getBomStickyCellStyle("bom", "#eff6ff", 3),
                                  }}
                                >
                                  <input
                                    ref={(node) => {
                                      copyDraftInputRefs.current[draftKey] = node;
                                    }}
                                    type="text"
                                    title={`Copied from ${copyDraft.sourceBomTemplate}. Change material code before Add.`}
                                    value={copyDraft.bomTemplate}
                                    placeholder="New material code"
                                    onChange={(event) => {
                                      const value = event.target.value.toUpperCase();
                                      updateCopyDraft(draftKey, (draft) => ({
                                        ...draft,
                                        bomTemplate: value,
                                      }));
                                    }}
                                    onKeyDown={(event) => {
                                      if (event.key === "Enter") {
                                        event.preventDefault();
                                        void handleSaveCopiedBom(draftKey);
                                      }
                                      if (event.key === "Escape") {
                                        event.preventDefault();
                                        dismissCopyDraft(draftKey);
                                      }
                                    }}
                                    style={{ fontFamily: "monospace", fontSize: 11, width: "100%", minWidth: BOM_ADMIN_STICKY_COLUMN_WIDTHS.bom }}
                                  />
                                  {copyDraft.sourceDisplayLabel ? (
                                    <div
                                      style={{ fontSize: 8, color: "#94a3b8", marginTop: 1, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                                      title={copyDraft.sourceDisplayLabel}
                                    >
                                      {copyDraft.sourceDisplayLabel}
                                    </div>
                                  ) : null}
                                  {copyDraftErrors[draftKey] ? (
                                    <div
                                      style={{ fontSize: 8, color: "#dc2626", marginTop: 2, lineHeight: 1.2 }}
                                      title={copyDraftErrors[draftKey]}
                                    >
                                      {copyDraftErrors[draftKey]}
                                    </div>
                                  ) : null}
                                </td>
                                <td style={getBomStickyCellStyle("interior", "#eff6ff", 2)}>
                                  <input
                                    type="text"
                                    value={`${copyDraft.interiorColorName || ""}${copyDraft.editionTag ? ` · ${copyDraft.editionTag}` : ""}`}
                                    placeholder="Interior"
                                    onChange={(event) => {
                                      const raw = event.target.value;
                                      const match = raw.match(/^(.*?)\s*·\s*(.+)$/);
                                      updateCopyDraft(draftKey, (draft) => ({
                                        ...draft,
                                        interiorColorName: match ? match[1].trim() : raw,
                                        editionTag: match ? match[2].trim() : null,
                                      }));
                                    }}
                                    style={{ fontSize: 10, width: "100%", minWidth: BOM_ADMIN_STICKY_COLUMN_WIDTHS.interior }}
                                  />
                                </td>
                                {(["single", "dual", "special"] as const).map((tierName) => (
                                  <td
                                    key={`${draftKey}-${tierName}`}
                                    style={{
                                      padding: "3px 5px",
                                      verticalAlign: "top",
                                      ...getBomStickyCellStyle(tierName, "#eff6ff", 2),
                                    }}
                                  >
                                    <div style={{ display: "flex", gap: 3, flexWrap: "wrap", alignItems: "center" }}>
                                      {copyDraft.skus.filter((sku) => inferBomAdminColourTier(sku) === tierName).length > 0 ? (
                                        copyDraft.skus
                                          .filter((sku) => inferBomAdminColourTier(sku) === tierName)
                                          .map((sku) => renderDraftColourChip(sku))
                                      ) : (
                                        <span style={{ fontSize: 9, color: "#cbd5e1" }}>—</span>
                                      )}
                                    </div>
                                  </td>
                                ))}
                                <td className="bom-lifecycle-cell" style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }}>
                                  <div className="bom-lifecycle-editor">
                                    <div className="bom-lifecycle-segment" aria-label="Draft lifecycle status">
                                      {BOM_LIFECYCLE_OPTIONS.map((option) => {
                                        const draftLifecycle = normalizeBomLifecycleStatus(copyDraft.lifecycleStatus);
                                        return (
                                          <button
                                            key={`${draftKey}-${option.value}`}
                                            type="button"
                                            className={`bom-lifecycle-option bom-lifecycle-option-${option.value}${draftLifecycle === option.value ? " is-active" : ""}`}
                                            title={option.description}
                                            onClick={() => {
                                              updateCopyDraft(draftKey, (draft) => ({
                                                ...draft,
                                                lifecycleStatus: option.value,
                                              }));
                                            }}
                                          >
                                            {option.label}
                                          </button>
                                        );
                                      })}
                                    </div>
                                    <div className="bom-lifecycle-window">
                                      <label>
                                        <span>From</span>
                                        <input
                                          type="text"
                                          placeholder="YYYY-MM"
                                          value={copyDraft.effectiveFrom || ""}
                                          onChange={(event) => {
                                            const value = event.target.value || null;
                                            updateCopyDraft(draftKey, (draft) => ({
                                              ...draft,
                                              effectiveFrom: value,
                                            }));
                                          }}
                                        />
                                      </label>
                                      <label>
                                        <span>To</span>
                                        <input
                                          type="text"
                                          placeholder="YYYY-MM"
                                          value={copyDraft.effectiveTo || ""}
                                          onChange={(event) => {
                                            const value = event.target.value || null;
                                            updateCopyDraft(draftKey, (draft) => ({
                                              ...draft,
                                              effectiveTo: value,
                                            }));
                                          }}
                                        />
                                      </label>
                                    </div>
                                  </div>
                                </td>
                                <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, textAlign: "center" }}>
                                  <div style={{ display: "inline-flex", gap: 6, alignItems: "center", justifyContent: "center" }}>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      style={{ color: "#64748b" }}
                                      onClick={() => dismissCopyDraft(draftKey)}
                                    >
                                      Cancel
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-primary"
                                      disabled={copyDraftSavingKey === draftKey}
                                      onClick={() => void handleSaveCopiedBom(draftKey)}
                                    >
                                      {copyDraftSavingKey === draftKey ? "Adding..." : "Add"}
                                    </button>
                                  </div>
                                </td>
                                <td
                                  colSpan={2}
                                  title={copyDraft.remark || "Draft note"}
                                  style={{
                                    width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from + BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to,
                                    minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from + BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to,
                                  }}
                                >
                                  <span className={copyDraft.remark ? "bom-material-note-summary" : "bom-material-note-empty"}>
                                    {copyDraft.remark || "Draft note below"}
                                  </span>
                                </td>
                                {sortedCountries.map((c) => {
                                  const fob = copyDraft.fobByCountry?.[c];
                                  const baseFob = getDraftBaseFob(fob);
                                  const hasFob = fob != null && baseFob != null && baseFob > 0;
                                  return (
                                    <td key={`${draftKey}-${c}`} title={formatBomFobTooltip(c, baseFob)} style={{ width: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, minWidth: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, textAlign: "right", padding: "2px 4px" }}>
                                      <span style={{ color: hasFob ? "#0f766e" : "#cbd5e1", fontWeight: hasFob ? 600 : 400 }}>
                                        {hasFob ? Number(baseFob).toLocaleString() : "-"}
                                      </span>
                                    </td>
                                  );
                                })}
                              </tr>
                              <tr style={{ background: "#dbeafe" }}>
                                <td
                                  colSpan={BOM_ADMIN_FIXED_COLUMN_COUNT + sortedCountries.length}
                                  style={{ background: "#eff6ff", borderLeft: "3px solid #2563eb", padding: "6px 8px", position: "relative", zIndex: 2 }}
                                >
                                  <div style={{ display: "grid", gridTemplateColumns: "auto minmax(240px, 1fr)", gap: 8, alignItems: "start", marginBottom: 8 }}>
                                    <span style={{ fontSize: 10, color: "#1d4ed8", fontWeight: 700, paddingTop: 7 }}>Draft note</span>
                                    <textarea
                                      value={copyDraft.remark}
                                      placeholder="What changed on this copied material..."
                                      rows={2}
                                      onChange={(event) => {
                                        const value = event.target.value;
                                        updateCopyDraft(draftKey, (draft) => ({
                                          ...draft,
                                          remark: value,
                                        }));
                                      }}
                                      style={{ minHeight: 42, resize: "vertical", fontSize: 11, lineHeight: 1.35 }}
                                    />
                                  </div>
                                  <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                                    <span style={{ fontSize: 10, color: "#1d4ed8", fontWeight: 700 }}>Draft FOB tools</span>
                                    <input
                                      type="number"
                                      value={copyDraft.bulkDeltaEur}
                                      placeholder="± EUR"
                                      onChange={(event) => {
                                        const value = event.target.value;
                                        updateCopyDraft(draftKey, (draft) => ({
                                          ...draft,
                                          bulkDeltaEur: value,
                                        }));
                                      }}
                                      style={{ width: 90, fontSize: 11 }}
                                    />
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-primary"
                                      onClick={() => applyCopyDraftFobDelta(draftKey)}
                                    >
                                      Apply to {copyDraft.bulkSelectedCountries.length || 0}
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      style={{ color: "#2563eb", borderColor: "#bfdbfe", background: "#eff6ff" }}
                                      onClick={() => applyCopyDraftFobDelta(draftKey, 200)}
                                    >
                                      +200
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      style={{ color: "#b45309", borderColor: "#fed7aa", background: "#fff7ed" }}
                                      onClick={() => applyCopyDraftFobDelta(draftKey, -300)}
                                    >
                                      -300
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      title="Select countries that already have FOB on this copied row"
                                      onClick={() => setCopyDraftCountryScope(draftKey, "filled")}
                                    >
                                      Filled
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      title="Select every visible country column"
                                      onClick={() => setCopyDraftCountryScope(draftKey, "all")}
                                    >
                                      All
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      title="Clear selected countries"
                                      onClick={() => setCopyDraftCountryScope(draftKey, "clear")}
                                    >
                                      Clear
                                    </button>
                                  </div>
                                  <details style={{ marginTop: 8 }}>
                                    <summary style={{ cursor: "pointer", fontSize: 10, color: "#475569", fontWeight: 600 }}>
                                      Selected countries ({copyDraft.bulkSelectedCountries.length})
                                    </summary>
                                    <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 8 }}>
                                      {getDraftCountryCodes(copyDraft).map((countryCode) => {
                                        const hasFob = getDraftBaseFob(copyDraft.fobByCountry[countryCode]) != null;
                                        return (
                                          <label
                                            key={`${draftKey}-country-${countryCode}`}
                                            style={{
                                              display: "inline-flex",
                                              alignItems: "center",
                                              gap: 4,
                                              padding: "4px 6px",
                                              borderRadius: 6,
                                              border: "1px solid #cbd5e1",
                                              background: hasFob ? "#ffffff" : "#f8fafc",
                                              fontSize: 10,
                                              color: hasFob ? "#1e293b" : "#94a3b8",
                                            }}
                                            title={`${countryCode}${countryLabels.get(countryCode) ? ` · ${countryLabels.get(countryCode)}` : ""}${hasFob ? "" : " · no FOB on source row"}`}
                                          >
                                            <input
                                              type="checkbox"
                                              checked={copyDraft.bulkSelectedCountries.includes(countryCode)}
                                              onChange={(event) => toggleCopyDraftCountry(draftKey, countryCode, event.target.checked)}
                                            />
                                            <span style={{ fontWeight: 700 }}>{countryCode}</span>
                                          </label>
                                        );
                                      })}
                                    </div>
                                  </details>
                                </td>
                              </tr>
                              </>
                            ) : null}
                            {editing ? (
                              <tr>
                                <td
                                  className="bom-edit-panel-cell"
                                  colSpan={BOM_ADMIN_FIXED_COLUMN_COUNT + sortedCountries.length}
                                  style={{ background: "#f8fafc", borderLeft: "3px solid #2563eb", padding: "6px 8px", position: "relative", zIndex: 2 }}
                                >
                                  <div style={{ display: "grid", gap: 8 }}>
                                    <BomEditPanel
                                      onSubmit={(event) => void handleProductMetadataSave(event, allSkus, draftKey)}
                                      productFields={(
                                        <>
                                          <input
                                            className="bom-edit-product-input bom-edit-product-input-brand"
                                            name="brand"
                                            type="text"
                                            required
                                            defaultValue={(ref as any).brand || ""}
                                            placeholder="Brand"
                                          />
                                          <input
                                            className="bom-edit-product-input"
                                            name="modelName"
                                            type="text"
                                            required
                                            defaultValue={(ref as any).modelName || ""}
                                            placeholder="Model"
                                          />
                                          <input
                                            className="bom-edit-product-input"
                                            name="version"
                                            type="text"
                                            required
                                            defaultValue={(ref as any).version || ""}
                                            placeholder="Version"
                                          />
                                          <CommandSelect
                                            name="powertrain"
                                            defaultValue={ref.powertrain ? getBomAdminPowertrainGroup(ref.powertrain) : ""}
                                            options={POWERTRAIN_COMMAND_OPTIONS}
                                            placeholder="Needs confirmation"
                                            searchPlaceholder="Search powertrain..."
                                            className="bom-edit-powertrain-select"
                                          />
                                          <span className="bom-edit-sku-count">
                                            {allCodes.length} SKUs
                                          </span>
                                        </>
                                      )}
                                      lifecycle={(
                                        <BomTemplateLifecycleEditor
                                          key={`${draftKey}|${ref.rowVersion}`}
                                          materialCode={ref.materialCode}
                                          bomTemplate={bomTemplate}
                                          status={lifecycleStatus}
                                          effectiveFrom={ref.effectiveFrom || null}
                                          effectiveTo={ref.effectiveTo || null}
                                          rowVersion={ref.rowVersion}
                                          onSaved={() => { scheduleLoad(0); onFobChanged?.(); }}
                                        />
                                      )}
                                      countries={editCountryOptions}
                                      selectedCountryCount={bulkFobEditor.selectedCountries.length}
                                      fobDeltaEur={bulkFobEditor.deltaEur}
                                      fobSaving={bulkFobSavingKey === bomTemplate}
                                      fobError={bulkFobErrors[bomTemplate] || null}
                                      onFobDeltaChange={(value) => {
                                        updateBulkFobEditor(bomTemplate, allSkus, (current) => ({
                                          ...current,
                                          deltaEur: value,
                                        }));
                                      }}
                                      onApplyFobDelta={() => void applyBulkFobDelta(bomTemplate, allSkus)}
                                      onApplyQuickFobDelta={(deltaEur) => void applyBulkFobDelta(bomTemplate, allSkus, deltaEur)}
                                      onSelectFilledCountries={() => setBulkFobCountryScope(bomTemplate, allSkus, "filled")}
                                      onSelectUnfilledCountries={() => setBulkFobCountryScope(bomTemplate, allSkus, "unfilled")}
                                      onSelectAllCountries={() => setBulkFobCountryScope(bomTemplate, allSkus, "all")}
                                      onClearCountries={() => setBulkFobCountryScope(bomTemplate, allSkus, "clear")}
                                      noteKey={`${draftKey}|remark|${currentBomTemplateRemark}`}
                                      noteDefaultValue={currentBomTemplateRemark}
                                      saveMessage={productSaveMessage}
                                      onCopyMaterial={isAdmin ? () => handleCopyMaterialFromBom(
                                        draftKey,
                                        bomTemplate,
                                        ref,
                                        allSkus,
                                        sourceLabel,
                                        bulkFobEditor.selectedCountries,
                                      ) : undefined}
                                      isSavingProduct={isSavingProduct}
                                    />
                                  </div>
                                </td>
                              </tr>
                            ) : null}
                            </Fragment>
                          );
                        })}
                      </tbody>
                      </table>
                    </div>
                  </div>
                );
              })}
                </div>
              ) : null}
            </div>
          );
        })}
	      </div>
      {financeQuickCard ? (
        <div className="bom-finance-modal-backdrop" onClick={closeFinanceQuickCard}>
          <div className="bom-finance-quick-modal-shell" onClick={(event) => event.stopPropagation()}>
            <FlipToolCard
              flipped={financeQuickFlipped}
              ariaLabel={`${financeQuickCard.countryCode} material finance quick card`}
              height={financeQuickFlipped ? 360 : 132}
              minHeight={financeQuickFlipped ? 360 : 132}
              className="bom-finance-quick-card"
              front={
                <div className="bom-finance-quick-face">
                  <div>
                    <span className="bom-finance-eyebrow">{financeQuickCard.countryCode} finance / CBU</span>
                    <h4>{financeQuickCard.title}</h4>
                    <p>{renderFinanceQuickSummary()}</p>
                  </div>
                  <BomFinanceActionBar
                    actions={[
                      { label: "Edit CBU", kind: "primary", onClick: () => setFinanceQuickFlipped(true) },
                      {
                        label: "Edit FOB",
                        onClick: () => {
                          setEditFob({
                            bomTemplate: financeQuickCard.materialCode,
                            materialCodes: financeQuickCard.materialCodes,
                            countryCode: financeQuickCard.countryCode,
                            fob: financeQuickCard.fob,
                            originalFob: financeQuickCard.fob,
                            remark: financeQuickCard.remark,
                            fobSourceMode: financeQuickCard.fobSourceMode ?? null,
                            fobSourceCountryCode: financeQuickCard.fobSourceCountryCode ?? null,
                          });
                          closeFinanceQuickCard();
                        },
                      },
                      { label: "Close", onClick: closeFinanceQuickCard },
                    ]}
                  />
                </div>
              }
              back={
                <div className="bom-finance-quick-back">
                  <div className="bom-finance-quick-back-head">
                    <span className="bom-finance-eyebrow">{financeQuickCard.countryCode} finance / CBU</span>
                    <BomFinanceActionBar
                      actions={[
                        { label: "Back", onClick: () => setFinanceQuickFlipped(false) },
                        { label: "Close", onClick: closeFinanceQuickCard },
                      ]}
                    />
                  </div>
                  {financeQuickLoading ? (
                    <div className="material-finance-empty">Loading finance row...</div>
                  ) : (
                    <MaterialFinanceMatrix
                      rows={financeQuickRows}
                      density="compact"
                      savingMaterialCode={savingFinanceMaterialCode}
                      onSaveRow={handleFinanceSave}
                    />
                  )}
                  {financeError ? <div className="material-finance-error">{financeError}</div> : null}
                </div>
              }
            />
          </div>
        </div>
      ) : null}
      {editFob ? createPortal((
        <div className="bom-finance-modal-backdrop" onClick={() => { if (!fobPeriodSaving) setEditFob(null); }}>
          <div className="bom-fob-edit-modal-shell" role="dialog" aria-modal="true" aria-label="Country template FOB periods" onClick={(event) => event.stopPropagation()} onKeyDown={(event) => { if (event.key === "Escape" && !fobPeriodSaving) setEditFob(null); }}>
            <div className="bom-fob-edit-card">
              <div>
                <span className="bom-finance-eyebrow">BOM ADMIN · FOB</span>
                <h4>{editFob.bomTemplate?.includes("**") ? "Edit template base FOB" : "Edit FOB"}</h4>
                <p>{editFob.bomTemplate} · {skus.find((sku) => sku.bomTemplate === editFob.bomTemplate)?.version} · {skus.find((sku) => sku.bomTemplate === editFob.bomTemplate)?.interiorColorName || "—"} · {editFob.countryCode} · {editFob.materialCodes.length} material codes</p>
              </div>
              <div className="bom-fob-edit-source-line">
                <span className="bom-fob-edit-source-pill">
                  {getBomFobSourceMarker(editFob.fobSourceMode, editFob.fobSourceCountryCode) || "BASE"}
                </span>
                <span>
                  {formatBomFobSourceLabel(editFob.fobSourceMode, editFob.fobSourceCountryCode) || "uploaded/resolved FOB"}
                </span>
                <span className="bom-fob-edit-source-value">
                  Current {editFob.originalFob != null ? editFob.originalFob.toLocaleString() : "-"}
                </span>
              </div>
              <div className="bom-fob-edit-grid">
                <label>
                  <span>Country</span>
                  <input type="text" value={editFob.countryCode} readOnly autoFocus />
                </label>
                <label>
                  <span>{editFob.bomTemplate?.includes("**") ? "Base FOB EUR" : "FOB EUR"}</span>
                  <input
                    type="number"
                    value={editFob.fob ?? ""}
                    onChange={(event) => setEditFob({ ...editFob, fob: event.target.value === "" ? null : Number(event.target.value) })}
                  />
                </label>
                <label className="bom-fob-edit-remark-field">
                  <span>Remark</span>
                  <textarea
                    value={editFob.remark}
                    placeholder="Country-specific FOB remark for this father material..."
                    rows={3}
                    onChange={(event) => setEditFob({ ...editFob, remark: event.target.value })}
                  />
                </label>
              </div>
              {editFob.bomTemplate?.includes("**") ? (
                <section className="bom-fob-period-editor" aria-label="Date-specific base FOB periods">
                  <div className="bom-fob-period-head">
                    <div>
                      <strong>Date-specific Single base</strong>
                      <span>Periods are authoritative for this country. Gaps have no price; zero pauses ordering. One Single base per month.</span>
                    </div>
                    {fobPeriodsLoading ? <span>Loading…</span> : null}
                  </div>
                  {fobPeriods.length > 0 ? (
                    <div className="bom-fob-period-list">
                      {fobPeriods.map((period, index) => (
                        <Fragment key={period.periodId}>
                        {index > 0 && fobPeriods[index - 1].validTo && new Date(period.validFrom).getTime() - new Date(fobPeriods[index - 1].validTo!).getTime() > 86400000 ? (
                          <p className="bom-fob-period-gap">Gap after {fobPeriods[index - 1].validTo} and before {period.validFrom}: no price — ordering unavailable</p>
                        ) : null}
                        <div className="bom-fob-period-row">
                          <span>{period.validFrom} → {period.validTo || "Open"}</span>
                          <strong>{period.baseFobEur === 0 ? "Ordering paused (0 EUR)" : `${period.baseFobEur.toLocaleString()} EUR`}</strong>
                          <span>{period.remark || "—"}</span>
                          <div className="bom-fob-period-actions">
                            <button
                              type="button"
                              className="btn btn-sm btn-ghost"
                              disabled={fobPeriodSaving}
                              onClick={() => handleFobPeriodEdit(period)}
                            >Edit</button>
                            <button
                              type="button"
                              className="btn btn-sm btn-ghost"
                              disabled={fobPeriodSaving}
                              onClick={() => void handleFobPeriodDelete(period)}
                            >Delete</button>
                          </div>
                        </div></Fragment>
                      ))}
                    </div>
                  ) : !fobPeriodsLoading ? <p>No periods configured: positive undated default FOB remains available within template dates.</p> : null}
                  {fobPeriods.length > 0 ? <p>Outside listed periods: no price — ordering unavailable.</p> : null}
                  {periodDeletePreview ? <div role="status">
                    <p>Remove {periodDeletePreview.validFrom} → {periodDeletePreview.validTo || "Open"}? {periodDeletePreview.lastPeriod ? periodDeletePreview.defaultBaseFobEur != null ? `The undated Single base ${periodDeletePreview.defaultBaseFobEur.toLocaleString()} EUR will apply within template dates${periodDeletePreview.defaultBaseFobEur === 0 ? "; ordering stays paused" : ""}.` : "No undated default exists: no price after deletion." : "These dates will become a no-price gap; the default will not apply."}</p>
                    <button type="button" className="btn btn-sm btn-primary" disabled={fobPeriodSaving} onClick={() => void handleFobPeriodDelete(periodDeletePreview, periodDeletePreview.fingerprint)}>Confirm remove period</button>
                    <button type="button" className="btn btn-sm btn-ghost" disabled={fobPeriodSaving} onClick={() => setPeriodDeletePreview(null)}>Cancel removal</button>
                  </div> : null}
                  <div className="bom-fob-period-draft">
                    <label><span>From</span><input type="date" disabled={fobPeriodSaving} value={fobPeriodDraft.validFrom} onChange={(event) => setFobPeriodDraft((current) => ({ ...current, validFrom: event.target.value }))} /></label>
                    <label><span>To</span><input type="date" disabled={fobPeriodSaving} value={fobPeriodDraft.validTo} onChange={(event) => setFobPeriodDraft((current) => ({ ...current, validTo: event.target.value }))} /></label>
                    <label><span>Single base EUR</span><input type="number" min="0" disabled={fobPeriodSaving} value={fobPeriodDraft.baseFobEur} onChange={(event) => setFobPeriodDraft((current) => ({ ...current, baseFobEur: event.target.value }))} /></label>
                    <label><span>Remark</span><input type="text" disabled={fobPeriodSaving} value={fobPeriodDraft.remark} onChange={(event) => setFobPeriodDraft((current) => ({ ...current, remark: event.target.value }))} /></label>
                    <div className="bom-fob-period-actions">
                      <button type="button" className="btn btn-sm btn-primary" disabled={fobPeriodSaving || !fobPeriodDraft.validFrom || fobPeriodDraft.baseFobEur === ""} onClick={() => void handleFobPeriodSave()}>
                        {fobPeriodDraft.periodId ? "Save changes" : "Add period"}
                      </button>
                      {fobPeriodDraft.periodId ? (
                        <button type="button" className="btn btn-sm btn-ghost" disabled={fobPeriodSaving} onClick={() => setFobPeriodDraft(EMPTY_BOM_FOB_PERIOD_DRAFT)}>Cancel edit</button>
                      ) : null}
                    </div>
                  </div>
                  {fobPeriodError ? <div className="form-error">{fobPeriodError}</div> : null}
                </section>
              ) : null}
              <BomFinanceActionBar
                actions={[
                  { label: `Save ${editFob.materialCodes.length}`, kind: "primary", disabled: fobPeriodSaving, onClick: () => void handleFobSave() },
                  { label: "Cancel", disabled: fobPeriodSaving, onClick: () => setEditFob(null) },
                ]}
              />
            </div>
          </div>
        </div>
      ), document.body) : null}
    </div>
  );
}

function PaymentTermAdminPanel() {
  const [pts, setPts] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editForm, setEditForm] = useState<any>({});
  const [impact, setImpact] = useState<Record<string, any> | null>(null);
  const [confirmMsg, setConfirmMsg] = useState("");
  const [showAddPt, setShowAddPt] = useState(false);
  const [newPt, setNewPt] = useState({ countryCode: "", countryName: "", paymentTermCode: "TT" });

  const addPaymentTerm = async () => {
    if (!newPt.countryCode || !newPt.countryName) return;
    const t = localStorage.getItem("jato_auth_token");
    const h: Record<string, string> = { "Content-Type": "application/json" };
    if (t) h["X-Auth-Token"] = t;
    try {
      const res = await fetch(apiUrl("/order-genius/payment-terms/countries"), {
        method: "POST", headers: h,
        body: JSON.stringify({ countryCode: newPt.countryCode.toUpperCase(), countryName: newPt.countryName, paymentTermCode: newPt.paymentTermCode, paymentMethod: "TT" }),
      });
      if (!res.ok) throw new Error(await res.text());
      setNewPt({ countryCode: "", countryName: "", paymentTermCode: "TT" });
      setShowAddPt(false);
      load();
    } catch (e) { alert(getErrorMessage(e)); }
  };

  const authHdrs = (): Record<string, string> => {
    const t = localStorage.getItem("jato_auth_token");
    return t ? { "X-Auth-Token": t } : {};
  };

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(apiUrl("/order-genius/payment-terms/countries"), { headers: authHdrs() });
      if (res.ok) setPts((await res.json()).items || []);
    } catch { /* */ }
    setLoading(false);
  }, []);

  useEffect(() => { load(); }, [load]);

  const startEdit = (row: any) => {
    setEditingId(row.id);
    setEditForm({ paymentTermCode: row.paymentTermCode, validFrom: row.validFrom || "", validTo: row.validTo || "", remark: row.remark || "", isActive: row.isActive });
    setImpact(null);
    setConfirmMsg("");
  };

  const cancelEdit = () => { setEditingId(null); setImpact(null); setConfirmMsg(""); };

  const saveEdit = async (row: any) => {
    const isCorrect = row.validFrom && row.validFrom < "2026-07";
    if (isCorrect && !confirmMsg) {
      try {
        const params = new URLSearchParams({ country: row.countryCode, oldPaymentTerm: row.paymentTermCode, newPaymentTerm: editForm.paymentTermCode || row.paymentTermCode, validFrom: editForm.validFrom || row.validFrom || "", validTo: editForm.validTo || row.validTo || "" });
        const res = await fetch(apiUrl("/order-genius/payment-terms/countries/impact?" + params), { headers: authHdrs() });
        if (res.ok) {
          const imp = await res.json();
          setImpact(imp);
          if (imp.fobRows > 0 || imp.orderMonths > 0) {
            setConfirmMsg(imp.message || "This change affects existing data. Confirm?");
            return;
          }
        }
      } catch { /* */ }
    }
    try {
      const res = await fetch(apiUrl("/order-genius/payment-terms/countries/" + row.id), {
        method: "PATCH", headers: { ...authHdrs(), "Content-Type": "application/json" },
        body: JSON.stringify({ ...editForm, correction: isCorrect }),
      });
      if (!res.ok) throw new Error(await res.text());
      cancelEdit();
      load();
    } catch (e) { alert(getErrorMessage(e)); }
  };

  const confirmSave = () => { setConfirmMsg(""); const row = pts.find((r: any) => r.id === editingId); if (row) saveEdit(row); };

  if (loading) return <div style={{ padding: 16, color: "#64748b" }}>Loading payment terms...</div>;

  return (
    <div className="card crud-card" style={{ padding: 16, marginTop: 16 }}>
      <h3 style={{ margin: "0 0 12px" }}>Payment Terms Admin</h3>
      <p style={{ fontSize: 12, color: "#64748b", marginBottom: 12 }}>
        Modifications to historical periods show impact but do NOT recalculate existing order snapshots.
      </p>
      <div style={{ display: "flex", gap: 8, marginBottom: 12, alignItems: "center", justifyContent: "flex-end" }}>
        <button className="btn btn-sm btn-ghost" onClick={() => setShowAddPt(!showAddPt)}>+ Country</button>
        {showAddPt && (
          <>
            <input type="text" placeholder="Code (e.g. ES)" value={newPt.countryCode}
              onChange={e => setNewPt({ ...newPt, countryCode: e.target.value.toUpperCase() })}
              style={{ width: 100, fontSize: 12, padding: "4px 8px" }} />
            <input type="text" placeholder="Name (e.g. Spain)" value={newPt.countryName}
              onChange={e => setNewPt({ ...newPt, countryName: e.target.value })}
              style={{ width: 120, fontSize: 12, padding: "4px 8px" }} />
            <select value={newPt.paymentTermCode} onChange={e => setNewPt({ ...newPt, paymentTermCode: e.target.value })}
              style={{ fontSize: 12, padding: "4px 8px" }}>
              {["TT","LC60","LC90","LC120"].map(p => <option key={p} value={p}>{p}</option>)}
            </select>
            <button className="btn btn-sm btn-primary" onClick={addPaymentTerm}>Add</button>
            <button className="btn btn-sm btn-ghost" onClick={() => setShowAddPt(false)}>Cancel</button>
          </>
        )}
      </div>
      {confirmMsg ? (
        <div style={{ background: "#fef3c7", border: "1px solid #f59e0b", padding: 12, marginBottom: 12, borderRadius: 6 }}>
          <strong>⚠️ Impact Warning</strong>
          <p style={{ fontSize: 13, margin: "4px 0" }}>{confirmMsg}</p>
          {impact ? <p style={{ fontSize: 12, color: "#64748b" }}>FOB rows: {impact.fobRows} · Order months: {impact.orderMonths}</p> : null}
          <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
            <button className="btn btn-sm btn-primary" onClick={confirmSave}>Confirm & Save</button>
            <button className="btn btn-sm btn-ghost" onClick={cancelEdit}>Cancel</button>
          </div>
        </div>
      ) : null}
      <div style={{ overflowX: "auto" }}>
        <table className="data-table" style={{ fontSize: 12 }}>
          <thead>
            <tr>
              <th>Country</th><th>Term</th><th>Valid From</th><th>Valid To</th><th>Status</th><th>Remark</th><th></th>
            </tr>
          </thead>
          <tbody>
            {pts.map((row: any) => {
              const isEditing = editingId === row.id;
              return (
                <tr key={row.id} style={!row.isActive ? { opacity: 0.6 } : undefined}>
                  <td>{row.countryCode} {row.countryName}</td>
                  <td>
                    {isEditing ? (
                      <select value={editForm.paymentTermCode} onChange={(e) => setEditForm({ ...editForm, paymentTermCode: e.target.value })} style={{ width: 80 }}>
                        {["TT","LC60","LC90","LC120"].map((pt) => <option key={pt} value={pt}>{pt}</option>)}
                      </select>
                    ) : row.paymentTermCode}
                  </td>
                  <td>
                    {isEditing ? (
                      <input type="text" value={editForm.validFrom} onChange={(e) => setEditForm({ ...editForm, validFrom: e.target.value })} style={{ width: 80 }} placeholder="YYYY-MM" />
                    ) : (row.validFrom || "—")}
                  </td>
                  <td>
                    {isEditing ? (
                      <input type="text" value={editForm.validTo} onChange={(e) => setEditForm({ ...editForm, validTo: e.target.value })} style={{ width: 80 }} placeholder="YYYY-MM or blank" />
                    ) : (row.validTo || "至今")}
                  </td>
                  <td>
                    {isEditing ? (
                      <label style={{ cursor: "pointer", display: "flex", alignItems: "center", gap: 4, fontSize: 12 }}>
                        <input type="checkbox" checked={editForm.isActive ?? row.isActive}
                          onChange={(e) => setEditForm({ ...editForm, isActive: e.target.checked })} />
                        {editForm.isActive ?? row.isActive ? "Active" : "Inactive"}
                      </label>
                    ) : (
                      <span style={{ color: row.isActive ? "#16a34a" : "#9ca3af" }}>{row.isActive ? "Active" : "Inactive"}</span>
                    )}
                  </td>
                  <td style={{ maxWidth: 150, overflow: "hidden", textOverflow: "ellipsis" }}>
                    {isEditing ? (
                      <input type="text" value={editForm.remark} onChange={(e) => setEditForm({ ...editForm, remark: e.target.value })} style={{ width: 120 }} />
                    ) : (row.remark || "")}
                  </td>
                  <td>
                    {isEditing ? (
                      <div style={{ display: "flex", gap: 4 }}>
                        <button className="btn btn-sm btn-primary" onClick={() => saveEdit(row)}>Save</button>
                        <button className="btn btn-sm btn-ghost" onClick={cancelEdit}>Cancel</button>
                        <button className="btn btn-sm btn-ghost" style={{ color: "#dc2626" }} onClick={async () => {
                          if (!confirm(`Close payment term for ${row.countryCode}? This deactivates it.`)) return;
                          const t = localStorage.getItem("jato_auth_token");
                          await fetch(apiUrl(`/order-genius/payment-terms/countries/${row.id}/close`), { method: "POST", headers: { "X-Auth-Token": t || "", "Content-Type": "application/json" }, body: "{}" });
                          setEditingId(null); load();
                        }}>Delete</button>
                      </div>
                    ) : (
                      <button className="btn btn-sm btn-ghost" onClick={() => startEdit(row)}>Edit</button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
