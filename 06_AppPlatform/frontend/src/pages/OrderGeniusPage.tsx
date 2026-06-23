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

import { api, apiUrl } from "../api/client";
import { useAuth } from "../contexts/AuthContext";
import { useAccountCountryOptions } from "../hooks/useAccountCountryOptions";
import { useResolvedCountry } from "../hooks/useResolvedCountry";
import { formatCountryCodeTooltip } from "../utils/jatoCountries";
import type { CellValueChangedEvent } from "ag-grid-community";
import {
  getOrderGeniusRowId,
  OrderGeniusGrid,
  type OrderGeniusGridRow,
} from "../components/OrderGeniusGrid";
import { DeckFloatingDrawer, FlipToolCard } from "../components/deckControls";
import { MaterialFinanceMatrix, MaterialFinanceWorkbench } from "../components/finance";
import type {
  ColourHexRule,
  ColourSurchargeRule,
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
  CountryPaymentTerm,
  MaterialSkuMatrixRow,
  MaterialUploadPreview,
  MatrixResponse,
  OrderGeniusOptions,
  PublishBaselineResponse,
  QuantityCellUpdate,
  QuantityImportPreview,
  QuantityImportResult,
} from "../types/orderGenius";

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
const BOM_ADMIN_TOOLS_COMPACT_BREAKPOINT = 680;
const BOM_ADMIN_TOOLS_PHONE_BREAKPOINT = 520;
const DEFAULT_COLOUR_SURCHARGES: Record<string, number> = {
  "OMODA|dual": 200,
  "OMODA|special": 200,
  "JAECOO|dual": 300,
  "JAECOO|special": 300,
};
const BOM_ADMIN_FIXED_COLUMN_COUNT = 9;
const BOM_ADMIN_COUNTRY_COLUMN_WIDTH = 75;
type BomAdminColourTier = "single" | "dual" | "special";
type BomAdminSkuColourFields = {
  colour?: string | null;
  colourCode?: string | null;
  colourType?: string | null;
  colourTier?: string | null;
  colourHex?: string | null;
  editionTag?: string | null;
};
const BOM_ADMIN_COLOUR_TIER_RANK: Record<BomAdminColourTier, number> = {
  single: 0,
  dual: 1,
  special: 2,
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
  lifecycle: 90,
  actions: 146,
  from: 92,
  to: 92,
} as const;
const BOM_ADMIN_FIXED_COLUMN_WIDTH =
  Object.values(BOM_ADMIN_STICKY_COLUMN_WIDTHS).reduce((total, width) => total + width, 0)
  + Object.values(BOM_ADMIN_TRAILING_COLUMN_WIDTHS).reduce((total, width) => total + width, 0);

function colourSurchargeKey(brand: string, colourType: string): string {
  return `${brand.trim().toUpperCase()}|${colourType.trim().toLowerCase()}`;
}

function normalizeBomAdminColourTier(value: unknown): BomAdminColourTier {
  const normalized = String(value || "").trim().toLowerCase().replace("_", "-");
  if (normalized === "dual" || normalized === "two-tone" || normalized === "dual-tone" || normalized === "dual tone") {
    return "dual";
  }
  if (normalized === "special" || normalized === "matte" || normalized === "black edition" || normalized === "pearl" || normalized === "metallic") {
    return "special";
  }
  return "single";
}

function mergeBomAdminColourTier(...tiers: unknown[]): BomAdminColourTier {
  return tiers.reduce<BomAdminColourTier>((best, tier) => {
    const normalized = normalizeBomAdminColourTier(tier);
    return BOM_ADMIN_COLOUR_TIER_RANK[normalized] > BOM_ADMIN_COLOUR_TIER_RANK[best]
      ? normalized
      : best;
  }, "single");
}

function inferBomAdminColourTier(sku: BomAdminSkuColourFields): BomAdminColourTier {
  const colourName = String(sku.colour || "").trim().toLowerCase();
  const colourType = String(sku.colourType || "").trim().toLowerCase().replace("_", "-");
  const colourCode = String(sku.colourCode || "").trim().toLowerCase();
  const editionTag = String(sku.editionTag || "").trim().toLowerCase();
  const colourHex = String(sku.colourHex || "").trim();
  const combined = [colourName, colourType, colourCode, editionTag].filter(Boolean).join(" ");
  const inferred = (
    editionTag
    || combined.includes("black edition")
    || combined.includes("matte")
    || combined.includes("pearl")
    || combined.includes("metallic")
    || combined.includes("special finish")
    || ["special", "matte", "pearl", "metallic"].includes(colourType)
  )
    ? "special"
    : (
      colourHex.includes("|")
      || ["dual", "two-tone", "dual-tone", "dual tone", "bi-color", "bi-colour"].includes(colourType)
      || /[/&／+＋]/.test(combined)
      || combined.includes("双色")
      || combined.includes("dual")
      || combined.includes("two tone")
      || combined.includes("two-tone")
      || combined.includes("contrast roof")
      || combined.includes("black roof")
      || /bi.?colou?r/.test(combined)
    )
      ? "dual"
      : "single";
  return mergeBomAdminColourTier(sku.colourTier, inferred);
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

function suggestedOrderingAccountCode(countries: string[]): string {
  const sorted = [...countries].sort();
  if (sorted.includes("FI") && sorted.includes("SE")) return "NORDIC";
  return sorted.join("").slice(0, 12) || "ACCOUNT";
}

type MatrixRowWithCountry = MaterialSkuMatrixRow & { _countryCode?: string; sheet_name?: string | null };
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
  filters: "筛选",
  bom: "BOM Admin",
  exports: "导入导出",
  pi: "PI Batch",
};

interface PiBatchAllocation {
  countryCode: string;
  quantity: number;
  fobEur: number | null;
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
}

function brandDisplayRank(brand: string): number {
  const upper = brand.toUpperCase();
  if (upper.includes("OMODA")) return 0;
  if (upper.includes("JAECOO")) return 1;
  return 2;
}

function firstModelNumber(value: string): number {
  const match = value.match(/\d+/);
  return match ? Number(match[0]) : Number.MAX_SAFE_INTEGER;
}

function powertrainDisplayRank(value: string): number {
  const upper = value.toUpperCase();
  if (upper === "ICE") return 0;
  if (upper === "HEV") return 1;
  if (upper === "BEV") return 2;
  if (upper === "PHEV" || upper.includes("SHS")) return 3;
  if (upper === "MHEV") return 4;
  return 9;
}

function compareProductGroupEntries(a: ProductGroupEntry, b: ProductGroupEntry): number {
  const [brandA = "", modelA = "", versionA = "", ptA = ""] = a[0].split("|");
  const [brandB = "", modelB = "", versionB = "", ptB = ""] = b[0].split("|");
  const brandRankDiff = brandDisplayRank(brandA) - brandDisplayRank(brandB);
  if (brandRankDiff !== 0) return brandRankDiff;
  const brandDiff = brandA.localeCompare(brandB);
  if (brandDiff !== 0) return brandDiff;
  const modelNumberDiff = firstModelNumber(modelA) - firstModelNumber(modelB);
  if (modelNumberDiff !== 0) return modelNumberDiff;
  const powertrainDiff = powertrainDisplayRank(ptA) - powertrainDisplayRank(ptB);
  return modelA.localeCompare(modelB) || powertrainDiff || versionA.localeCompare(versionB) || ptA.localeCompare(ptB);
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
  powertrain: "ICE",
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
      powertrain: form.powertrain.trim() || "ICE",
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
      powertrain: form.powertrain.trim() || "ICE",
    }],
    errors: [],
    isBatch: false,
  };
}

export function OrderGeniusPage() {
  const { user } = useAuth();
  const { allCountriesISO } = useResolvedCountry("iso");
  const userCountries = (() => {
    const codes = [...(user?.secondaryCountries ?? [])];
    if (user?.primaryCountry && !codes.includes(user.primaryCountry)) {
      codes.unshift(user.primaryCountry);
    }
    return codes;
  })();
  const isAdmin = user?.role === "admin";
  const canFillOrders = user?.role === "admin" || user?.role === "editor" || user?.role === "order_filler";
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
    if (!isAdmin && userCountries.length > 0) {
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
  const [fobCountryCodes, setFobCountryCodes] = useState<string[] | null>(null);
  const [bomAdminCopyTargetCountry, setBomAdminCopyTargetCountry] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

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
  const matrixRequestIdRef = useRef(0);

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
  const countryInitDone = useRef(false);
  useEffect(() => {
    api.getOrderGeniusCountries()
      .then((res) => {
        setCountries(res.items);
        if (res.items.length > 0 && !countryInitDone.current) {
          countryInitDone.current = true;
          const validCodes = new Set(res.items.map((c) => c.countryCode));
          const resolved = allCountriesISO.filter((c) => validCodes.has(c));
          if (resolved.length === 0) {
            const fallback = res.items.find((c) => c.countryCode === "SE");
            setSelectedCountries(fallback ? [fallback.countryCode] : [res.items[0].countryCode]);
          } else {
            setSelectedCountries(resolved);
          }
        }
      })
      .catch(() => setError("Failed to load countries"));
  }, [allCountriesISO]);

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
  }, [loadFobCountries]);

  // ── Load options (use primary country for filter dropdowns) ────────
  useEffect(() => {
    if (!primaryCountry) return;
    setLoading(true);
    setError("");
    api
      .getOrderGeniusOptions({
        country: primaryCountry,
        brand: brandFilter || undefined,
        model: modelFilter || undefined,
        powertrain: powertrainFilter || undefined,
        version: versionFilter || undefined,
        colour: colourFilter || undefined,
      })
      .then(setOptions)
      .catch((e: unknown) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [primaryCountry, brandFilter, modelFilter, powertrainFilter, versionFilter, colourFilter]);

  // ── Load matrices for all selected countries ────────────────────────
  const loadMatrices = useCallback(() => {
    const requestId = matrixRequestIdRef.current + 1;
    matrixRequestIdRef.current = requestId;
    if (selectedCountries.length === 0) {
      setMatrices({});
      setLoading(false);
      return;
    }
    setLoading(true);
    setError("");
    const params = {
      year: selectedYear,
      brand: brandFilter || undefined,
      model: modelFilter || undefined,
      powertrain: powertrainFilter || undefined,
      version: versionFilter || undefined,
      colour: colourFilter || undefined,
      materialCodeSearch: debouncedMaterialSearch || undefined,
    };
    void api
      .getOrderGeniusMatrixBatch({ countries: selectedCountries, ...params })
      .then((response) => {
        if (requestId !== matrixRequestIdRef.current) return;
        const next: Record<string, MatrixResponse> = {};
        for (const country of selectedCountries) {
          const matrix = response.matrices[country];
          if (matrix) next[country] = matrix;
        }
        setMatrices(next);
        if (Object.keys(next).length === 0) {
          const firstError = Object.values(response.errors)[0];
          if (firstError) setError(firstError);
        }
      })
      .catch((e: unknown) => {
        if (requestId === matrixRequestIdRef.current) setError(getErrorMessage(e));
      })
      .finally(() => {
        if (requestId === matrixRequestIdRef.current) setLoading(false);
      });
  }, [
    selectedCountries, selectedYear, brandFilter, modelFilter,
    powertrainFilter, versionFilter, colourFilter, debouncedMaterialSearch,
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
    for (const row of combinedMatrix.rows) {
      const materialCode = row.materialCode?.trim();
      if (!materialCode || seen.has(materialCode)) continue;
      seen.add(materialCode);
      suggestions.push({ materialCode, remark: row.remark ?? null });
      if (suggestions.length >= 300) break;
    }
    return suggestions;
  }, [combinedMatrix.rows]);

  // ── Grid data + cell editing ──────────────────────────────────────

  const flatRows = useMemo<OrderGeniusGridRow[]>(() => {
    const makeRow = (r: MatrixRowWithCountry, indent = false): OrderGeniusGridRow => {
      const row: OrderGeniusGridRow = {
        materialCode: r.materialCode,
        bomTemplate: r.bomTemplate,
        modelName: r.modelName,
        version: r.version,
        colour: r.colour,
        interiorColorName: r.interiorColorName,
        fobEur: r.fobEur ?? null,
        lifecycleStatus: r.lifecycleStatus,
        editable: r.editable,
        remark: r.remark ?? undefined,
        _countryCode: r._countryCode,
        _indent: indent || undefined,
        _versions: {},
        _errors: {},
        _saving: new Set(),
      };
      const months = r.months || {};
      for (let m = 1; m <= 12; m++) {
        const monthKey = `month_${m}` as `month_${number}`;
        const md = months[String(m)];
        row[monthKey] = md?.quantity ?? 0;
        row._versions[monthKey] = md?.rowVersion ?? 0;
      }
      return row;
    };

    // Extract canonical powertrain: model name is the authoritative source (DB field may be stale)
    const canonPt = (row: MatrixRowWithCountry): string => {
      const rawPt = (row.powertrain || "").toUpperCase();
      const model = (row.modelName || "").toUpperCase();
      const sheet = (row.sheet_name || "").toUpperCase();
      const combined = `${sheet} ${model} ${rawPt}`;
      // Order matters: PHEV/SHS before HEV, BEV before EV
      if (combined.includes("PHEV") || combined.includes("SHS") || combined.includes("PLUG")) return "PHEV";
      if (combined.includes("MHEV") || combined.includes("MILD HYBRID")) return "MHEV";
      if (combined.includes("REEV") || combined.includes("EREV") || combined.includes("RANGE EXTEND")) return "REEV";
      if (combined.includes("FCEV") || combined.includes("FCV") || combined.includes("FUEL CELL")) return "FCV";
      if (combined.includes("HEV") || combined.includes("HYBRID ELECTRIC")) return "HEV";
      if (combined.includes("BEV") || combined.includes("BATTERY ELECTRIC")) return "BEV";
      if (combined.includes("EV") || combined.includes("ELECTRIC")) return "BEV";
      if (combined.includes("ICE") || combined.includes("PETROL") || combined.includes("DIESEL") || combined.includes("GASOLINE") || combined.includes("LPG") || combined.includes("COMBUSTION")) return "ICE";
      return rawPt || "Other";
    };

    if (!groupByProduct) {
      return combinedMatrix.rows.map((row) => makeRow(row));
    }

    const aggregateRows = (rows: MatrixRowWithCountry[]) => {
      let ttl = 0;
      let floorFob: number | null = null;
      const monthlySums: number[] = new Array(13).fill(0);
      for (const row of rows) {
        const months = row.months || {};
        for (let m = 1; m <= 12; m++) {
          const quantity = months[String(m)]?.quantity ?? 0;
          monthlySums[m] += quantity;
          ttl += quantity;
        }
        const fob = row.fobEur ?? 0;
        if (fob > 0 && (floorFob === null || fob < floorFob)) {
          floorFob = fob;
        }
      }
      return { ttl, floorFob, monthlySums };
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
        remark: "",
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
      for (let m = 1; m <= 12; m++) header[`month_${m}`] = aggregate.monthlySums[m];
      return header;
    };

    const bomTemplateForRow = (row: MatrixRowWithCountry): string => {
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
        for (const row of rows) target.push(makeRow(row, level > 0));
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
          for (const row of bomRows) target.push(makeRow(row, true));
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
    for (const r of combinedMatrix.rows) {
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
  }, [combinedMatrix.rows, consolidatedView, expandedProductGroups, groupByProduct, selectedCountries.length]);

  const cellKey = (materialCode: string, month: number) =>
    `${materialCode}_${month}`;

  // Stable refs so callback identity doesn't change on re-render (prevents grid flash)
  const selCountriesRef = useRef(selectedCountries); selCountriesRef.current = selectedCountries;
  const selYearRef = useRef(selectedYear); selYearRef.current = selectedYear;
  const loadMatricesRef = useRef(loadMatrices); loadMatricesRef.current = loadMatrices;

  const toggleProductGroup = useCallback((groupKey: string) => {
    setExpandedProductGroups((prev) => {
      const next = new Set(prev);
      if (next.has(groupKey)) next.delete(groupKey);
      else next.add(groupKey);
      return next;
    });
  }, []);

  const handleCellValueChanged = useCallback(
    async (event: CellValueChangedEvent<OrderGeniusGridRow>) => {
      const { data, colDef, newValue } = event;
      const field = colDef.field as string;
      if (!field?.startsWith("month_") || !data) return;
      if (data.__type === "groupHeader") return;

      const month = parseInt(field.replace("month_", ""), 10);
      const key = cellKey(data.materialCode, month);
      const oldQty = data._versions[field];
      const qty = Number(newValue) || 0;

      setSavingCells((prev) => new Set(prev).add(key));
      setCellErrors((prev) => {
        const next = { ...prev };
        delete next[key];
        return next;
      });

      const countryCode = data._countryCode || selCountriesRef.current[0] || "SE";
      const payload: QuantityCellUpdate = {
        countryCode,
        orderYear: selYearRef.current,
        orderMonth: month,
        materialCode: data.materialCode,
        quantity: qty,
        rowVersion: oldQty,
      };

      try {
        const result = await api.updateQuantityCell(payload);
        setMatrices((prev) => {
          const target = prev[countryCode];
          if (!target) return prev;
          const rows = target.rows.map((r) => {
            if (r.materialCode !== data.materialCode) return r;
            const months = { ...r.months };
            months[String(month)] = {
              quantity: qty,
              isEditable: true,
              rowVersion: result.rowVersion,
            };
            const newTtl = Object.values(months).reduce((s, m) => s + m.quantity, 0);
            return { ...r, months, ttl: newTtl };
          });
          return { ...prev, [countryCode]: { ...target, rows } };
        });
      } catch (err: unknown) {
        const msg = getErrorMessage(err);
        setCellErrors((prev) => ({ ...prev, [key]: msg }));
        if (msg.toLowerCase().includes("conflict") || msg.includes("409")) {
          loadMatricesRef.current();
        }
      } finally {
        setSavingCells((prev) => {
          const next = new Set(prev);
          next.delete(key);
          return next;
        });
      }
    },
    [], // stable — all dynamic values via refs
  );

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
      }
      const g = groups.get(key)!;
      g.children.push(row);
      // Sum month quantities
      for (let m = 1; m <= 12; m++) {
        g.parent[`month_${m}`] = (g.parent[`month_${m}`] || 0) + (row[`month_${m}`] || 0);
      }
    }

    const result: OrderGeniusGridRow[] = [];
    for (const [, g] of groups) {
      const ttl = Array.from({ length: 12 }, (_, idx) => g.parent[`month_${idx + 1}`] || 0)
        .reduce((sum, quantity) => sum + quantity, 0);
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

  const selectablePiRows = useMemo(() => {
    if (selectedMonth == null) return [];
    const monthField = `month_${selectedMonth}` as `month_${number}`;
    return displayRows.filter((row) =>
      row.__type !== "groupHeader"
      && row.__type !== "consolidated_parent"
      && row.lifecycleStatus !== "historical"
      && (row[monthField] || 0) > 0,
    );
  }, [displayRows, selectedMonth]);

  const selectablePiRowsById = useMemo(() => {
    const result = new Map<string, OrderGeniusGridRow>();
    for (const row of selectablePiRows) {
      result.set(getOrderGeniusRowId(row), row);
    }
    return result;
  }, [selectablePiRows]);

  const selectedPiRows = useMemo(() => {
    const result: OrderGeniusGridRow[] = [];
    for (const rowId of piSelectedRowIds) {
      const row = selectablePiRowsById.get(rowId);
      if (row) result.push(row);
    }
    return result;
  }, [piSelectedRowIds, selectablePiRowsById]);

  const selectedPiQuantityTotal = useMemo(() => {
    return selectedPiRows.reduce((sum, row) => {
      const rowId = getOrderGeniusRowId(row);
      const monthQuantity = selectedMonth == null ? 0 : row[`month_${selectedMonth}`] || 0;
      return sum + Math.min(piBatchQuantities[rowId] ?? monthQuantity, monthQuantity);
    }, 0);
  }, [piBatchQuantities, selectedMonth, selectedPiRows]);

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
    setPiSelectedRowIds((current) => {
      const next = new Set<string>();
      current.forEach((rowId) => {
        if (selectablePiRowsById.has(rowId)) next.add(rowId);
      });
      return next.size === current.size ? current : next;
    });
  }, [selectablePiRowsById]);

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
    const monthQuantity = selectedMonth == null ? 0 : row[`month_${selectedMonth}`] || 0;
    setPiSelectedRowIds((current) => {
      const next = new Set(current);
      if (selected) next.add(rowId);
      else next.delete(rowId);
      return next;
    });
    setPiBatchQuantities((current) => {
      const next = { ...current };
      if (selected) next[rowId] = Math.max(1, monthQuantity);
      else delete next[rowId];
      return next;
    });
    setPiBatchNotice("");
  }, [selectedMonth]);

  const updatePiBatchQuantity = (row: OrderGeniusGridRow, quantity: number): void => {
    const rowId = getOrderGeniusRowId(row);
    const monthQuantity = selectedMonth == null ? 0 : row[`month_${selectedMonth}`] || 0;
    const nextQuantity = Math.max(0, Math.min(Math.floor(quantity || 0), monthQuantity));
    setPiBatchQuantities((current) => ({ ...current, [rowId]: nextQuantity }));
    setPiBatchNotice("");
  };

  const clearPiBatchSelection = (): void => {
    setPiSelectedRowIds(new Set());
    setPiBatchQuantities({});
    setOrderingAccountCodeEdited(false);
    setPiBatchNotice("");
  };

  const handleCreatePiBatch = async (): Promise<void> => {
    if (selectedMonth == null) {
      setError("Select one month before creating PI");
      return;
    }
    if (selectedPiRows.length === 0) {
      setError("Select at least one order row");
      return;
    }

    const byCountry = new Map<string, Map<string, PiBatchLineItem>>();
    const byMaterial = new Map<string, PiBatchLineItem>();
    for (const row of selectedPiRows) {
      const rowId = getOrderGeniusRowId(row);
      const monthQuantity = row[`month_${selectedMonth}`] || 0;
      const requestedQuantity = Math.floor(piBatchQuantities[rowId] ?? monthQuantity);
      if (requestedQuantity <= 0) continue;
      if (requestedQuantity > monthQuantity) {
        setError(`PI quantity exceeds order quantity: ${row.materialCode}`);
        return;
      }
      const countryCode = row._countryCode || primaryCountry;
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
        });
      }
      byCountry.set(countryCode, items);

      const materialExisting = byMaterial.get(row.materialCode);
      const allocation: PiBatchAllocation = {
        countryCode,
        quantity: requestedQuantity,
        fobEur: row.fobEur,
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
      setError("Selected PI quantity must be greater than 0");
      return;
    }
    if (piBatchMode === "by_account") {
      const accountCode = normalizeAccountCode(
        piBatchForm.orderingAccountCode || suggestedOrderingAccountCode(selectedPiCountries),
      );
      if (accountCode.length < 2) {
        setError("Ordering account code must be at least 2 letters or numbers");
        return;
      }
    }

    setCreatingPiBatch(true);
    setError("");
    setPiBatchNotice("");
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
    } catch (err: unknown) {
      setError(`PI batch failed: ${getErrorMessage(err)}`);
    } finally {
      setCreatingPiBatch(false);
    }
  };

  // ── Export ─────────────────────────────────────────────────────────

  const [mergeExport, setMergeExport] = useState(false);

  const buildExportOptions = () => ({
    brand: brandFilter || undefined,
    model: modelFilter || undefined,
    powertrain: powertrainFilter || undefined,
    version: versionFilter || undefined,
    colour: colourFilter || undefined,
    materialCodeSearch: materialSearch || undefined,
    selectedMonth: selectedMonth ?? undefined,
    hideEmptyRows,
  });

  const exportMonthSuffix = () => selectedMonth ? `_M${String(selectedMonth).padStart(2, "0")}` : "";

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
    const exportOptions = buildExportOptions();
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
    if (fobCountryCodes === null) return [];
    const fobSet = new Set(fobCountryCodes);
    return selectedCountries.filter((countryCode) => !fobSet.has(countryCode));
  }, [fobCountryCodes, selectedCountries]);
  const missingFobCountryLabels = missingFobCountryCodes.map((countryCode) =>
    formatOrderGeniusCountryOptionLabel(countryCode, countryNameByCode.get(countryCode)),
  );
  const openBomAdminPanel = () => {
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
    { id: "filters", label: "筛选", meta: activeFilterSummary },
    {
      id: "bom",
      label: "BOM Admin",
      meta: `${showBomAdmin ? "BOM open" : "BOM closed"} · ${showPtAdmin ? "PT open" : selectedPaymentTerm || "Payment terms"}`,
    },
    {
      id: "exports",
      label: "导入导出",
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
      </div>

      <DeckFloatingDrawer
        open={showDeck}
        onOpenChange={setShowDeck}
        triggerPrimary="筛选 / 操作"
        triggerSecondaryOpen={ORDER_GENIUS_CONTROL_TAB_LABELS[controlTab]}
        triggerSecondaryClosed={activeFilterSummary}
        eyebrow="Order Genius"
        title="筛选与操作"
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

      <div className="deck-control-tabs order-genius-control-tabs" role="tablist" aria-label="Order Genius control sections">
        {orderGeniusControlTabs.map((tab) => (
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
        <div className="market-scan-field version-comparison-model-picker-field" ref={countryPickerRef} style={{ minWidth: 200 }}>
          <span>Countries{selectedCountries.length > 0 ? ` (${selectedCountries.length})` : ""}</span>
          <div className="version-comparison-model-picker">
            <div className="version-comparison-model-picker-input-row">
              <input type="text" className="version-comparison-model-search"
                placeholder={`${selectedCountries.length} 个国家已选 — 搜索...`}
                value={countrySearchQuery}
                onChange={(e) => { setCountrySearchQuery(e.target.value); setCountryPickerOpen(true); }}
                onFocus={() => { setCountrySearchQuery(""); setCountryPickerOpen(true); }}
                disabled={countries.length === 0} />
            </div>
            {countryPickerOpen && searchedCountryOptions.length > 0 ? (
              <div className="version-comparison-model-dropdown">
                <div className="version-comparison-model-dropdown-actions">
                  <button type="button" className="version-comparison-batch-btn"
                    onClick={() => setSelectedCountries(searchedCountryOptions.map((o) => o.value))}>全选</button>
                  <button type="button" className="version-comparison-batch-btn"
                    onClick={() => { const vals = new Set(searchedCountryOptions.map((o) => o.value)); setSelectedCountries((prev) => prev.filter((c) => !vals.has(c))); }}>取消</button>
                  <button type="button" className="version-comparison-batch-btn"
                    onClick={() => setSelectedCountries([])}>清空</button>
                  <span className="version-comparison-dropdown-count">{searchedCountryOptions.length} 项 · {selectedCountries.length} 已选</span>
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

        <select
          value={selectedYear}
          onChange={(e) => setSelectedYear(Number(e.target.value))}
          style={{ minWidth: 80 }}
        >
          {[selectedYear - 1, selectedYear, selectedYear + 1].map((y) => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>

        <select
          value={selectedMonth ?? ""}
          onChange={(e) => setSelectedMonth(e.target.value ? Number(e.target.value) : null)}
          style={{ minWidth: 100 }}
        >
          <option value="">All months</option>
          {MONTHS.map((m, i) => (
            <option key={m} value={i + 1}>{m}</option>
          ))}
        </select>

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

        <label style={{ cursor: "pointer", fontSize: 12, color: "#475569", display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={groupByProduct} onChange={(e) => setGroupByProduct(e.target.checked)} />
          Group by product
        </label>
        <label style={{ cursor: "pointer", fontSize: 12, color: "#64748b", display: "flex", alignItems: "center", gap: 4 }}>
          <input type="checkbox" checked={hideEmptyRows} onChange={(e) => setHideEmptyRows(e.target.checked)} />
          Hide empty rows
        </label>
        {selectedCountries.length > 1 && (
          <label style={{ cursor: "pointer", fontSize: 12, color: "#0f766e", display: "flex", alignItems: "center", gap: 4 }}>
            <input type="checkbox" checked={consolidatedView} onChange={(e) => setConsolidatedView(e.target.checked)} />
            Consolidated
          </label>
        )}

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
        {isAdmin && (
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
        {!isAdmin ? (
          <div className="order-genius-muted-note">Admin tools are available to admin users only.</div>
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
        <button type="button" className="btn btn-sm btn-ghost" onClick={handlePiExport}
                disabled={combinedMatrix.totalRows === 0}>
          Export PI
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
      </div>
      ) : null}

      {controlTab === "pi" ? (
        <div className="og-pi-batch-panel">
          <div className="og-pi-batch-head">
            <strong>PI Batch</strong>
            <span title="Select one month, tick PI rows, then create PI from the selected order quantities">
              {selectedMonth ? `${selectedPiRows.length} rows · ${selectedPiQuantityTotal} units · ${piBatchScopeSummary}` : "Select month"}
            </span>
          </div>
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
            <input
              type="date"
              value={piBatchForm.orderDate}
              onChange={(event) => setPiBatchForm((current) => ({ ...current, orderDate: event.target.value }))}
              title="PI order date"
            />
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
                return (
                  <label
                    key={rowId}
                    title="This quantity will be linked back to the selected market country in PI allocation."
                  >
                    <span>
                      {row._countryCode ? `${row._countryCode} · ` : ""}{row.materialCode}
                      <small>{row.modelName} / {row.version} / {row.colour}</small>
                    </span>
                    <input
                      type="number"
                      min={0}
                      max={monthQuantity}
                      value={piBatchQuantities[rowId] ?? monthQuantity}
                      onChange={(event) => updatePiBatchQuantity(row, Number(event.target.value))}
                      title={`PI quantity for this row. Max ${monthQuantity}.`}
                    />
                  </label>
                );
              })}
            </div>
          ) : null}
          <div className="og-pi-batch-actions">
            <button
              type="button"
              className="btn btn-sm btn-primary"
              disabled={creatingPiBatch || selectedMonth == null || selectedPiRows.length === 0}
              onClick={() => void handleCreatePiBatch()}
              title={piBatchMode === "by_account" ? "Create one PI for the ordering account and keep country allocations on each line" : "Create one PI per selected country"}
            >
              {creatingPiBatch ? "Creating..." : "Create PI"}
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
          {piBatchNotice ? <div className="og-pi-batch-notice">{piBatchNotice}</div> : null}
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
            {{ months: "Months", amount: "Amount", ttlQty: "TTL Qty", ttlAmount: "TTL Amt", fob: "FOB", materialCode: "Material", remark: "Remark" }[col]}
          </label>
        ))}
      </div>
      ) : null}
      </DeckFloatingDrawer>

      {missingFobCountryCodes.length > 0 ? (
        <div className="order-genius-missing-fob-alert" role="alert">
          <div>
            <strong>{missingFobCountryCodes.length} selected countries do not have BOM FOB yet.</strong>
            <p>
              {missingFobCountryLabels.join(" · ")}
            </p>
          </div>
          <div className="order-genius-missing-fob-actions">
            <button type="button" className="btn btn-sm btn-primary" onClick={openBomAdminForMissingFob}>
              Open BOM Admin
            </button>
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
          canEditQuantities={canFillOrders}
          visibleColumns={visibleColumns}
          showCountry={selectedCountries.length > 1}
          onCellValueChanged={handleCellValueChanged}
          onGridReady={(api) => { gridApiRef.current = api; }}
          onToggleGroup={toggleProductGroup}
          onTogglePiRow={togglePiBatchRow}
        />
      ) : (
        <div style={{ padding: 32, textAlign: "center", color: "#64748b" }}>
          {missingFobCountryCodes.length > 0 ? (
            <div style={{ display: "grid", gap: 12, justifyItems: "center" }}>
              <strong style={{ color: "#334155" }}>Selected country has no BOM FOB yet.</strong>
              <span>{missingFobCountryLabels.join(" · ")}</span>
              <div className="order-genius-missing-fob-actions">
                <button type="button" className="btn btn-sm btn-primary" onClick={openBomAdminForMissingFob}>
                  Open BOM Admin
                </button>
                <button type="button" className="btn btn-sm btn-ghost" onClick={removeMissingFobCountries}>
                  Remove from view
                </button>
              </div>
            </div>
          ) : selectedCountries.length > 0 ? (
            "No data. Upload a Material Master file to get started."
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
              initialCopyTargetCountry={bomAdminCopyTargetCountry}
              onFobCountriesChanged={loadFobCountries}
              onFobChanged={() => {
                void loadFobCountries();
                loadMatrices();
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
  uploadedFobEur?: number | null;
  finalFobEur?: number | null;
  paymentTermCode?: string | null;
  colourSurchargeEur?: number | null;
  fobSourceCountryCode?: string | null;
  fobSourceMode?: string | null;
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
  fobByCountry: Record<string, BomDraftFobEntry>;
  bulkDeltaEur: string;
  bulkSelectedCountries: string[];
  skus: BomCopyDraftSku[];
};

type BomBulkFobEditor = {
  deltaEur: string;
  selectedCountries: string[];
};

type BomAdminModelGroup = {
  brand: string;
  modelName: string;
  pt: string;
  versions: Map<string, any[]>;
};

type BomAdminTierGroups = {
  single: any[];
  dual: any[];
  special: any[];
  allSkus: any[];
  countryCodes: string[];
  filledCountryCodes: string[];
  filledCountryCodeSet: ReadonlySet<string>;
};

type BomColourSwatchEditor = {
  materialCode: string;
  brand: string;
  colourCode: string;
  colourName: string;
  isDual: boolean;
  hex1: string;
  hex2: string;
  anchorLeft: number;
  anchorTop: number;
};

interface BomFinanceQuickCard {
  countryCode: string;
  materialCode: string;
  materialCodes: string[];
  title: string;
  fob: number | null;
}

interface BomFinanceDrawerScope {
  countryCode: string;
  brand: string;
  modelName: string;
  powertrain: string;
  version?: string;
}

interface BomAdminPanelProps {
  initialCopyTargetCountry?: string | null;
  onFobCountriesChanged?: () => void;
  onFobChanged?: () => void;
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
  const raw = fob.finalFobEur ?? fob.uploadedFobEur;
  if (raw == null) return null;
  const numeric = Number(raw);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
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

function BomAdminPanel({
  initialCopyTargetCountry = null,
  onFobCountriesChanged,
  onFobChanged,
}: BomAdminPanelProps) {
  const [skus, setSkus] = useState<any[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [activeFobCountries, setActiveFobCountries] = useState<string[]>([]);
  const { countryOptions: accountCountryOptions } = useAccountCountryOptions();
  const [loading, setLoading] = useState(true);
  const [searchText, setSearchText] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [editFob, setEditFob] = useState<{ materialCodes: string[]; countryCode: string; fob: number | null } | null>(null);
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
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [showAddMaterial, setShowAddMaterial] = useState(false);
  const [toolsFlipped, setToolsFlipped] = useState(false);
  const [isCompactToolsLayout, setIsCompactToolsLayout] = useState(() =>
    typeof window !== "undefined" && window.innerWidth <= BOM_ADMIN_TOOLS_COMPACT_BREAKPOINT,
  );
  const [isPhoneToolsLayout, setIsPhoneToolsLayout] = useState(() =>
    typeof window !== "undefined" && window.innerWidth <= BOM_ADMIN_TOOLS_PHONE_BREAKPOINT,
  );
  const [newMaterial, setNewMaterial] = useState<AddMaterialFormState>(EMPTY_ADD_MATERIAL);
  const [addMaterialError, setAddMaterialError] = useState("");
  const [addMaterialNotice, setAddMaterialNotice] = useState("");
  const [copyCountryForm, setCopyCountryForm] = useState({ sourceCountryCode: "", targetCountryCode: "", overwriteExisting: false });
  const [copyCountryMessage, setCopyCountryMessage] = useState("");
  const [bomAdminNotice, setBomAdminNotice] = useState("");
  const [copyingCountry, setCopyingCountry] = useState(false);
  const [adjustCountryForm, setAdjustCountryForm] = useState({ countryCode: "", deltaEur: "" });
  const [adjustCountryMessage, setAdjustCountryMessage] = useState("");
  const [adjustingCountry, setAdjustingCountry] = useState(false);
  const [copyDrafts, setCopyDrafts] = useState<Record<string, BomCopyDraft>>({});
  const [copyDraftErrors, setCopyDraftErrors] = useState<Record<string, string>>({});
  const [copyDraftSavingKey, setCopyDraftSavingKey] = useState<string | null>(null);
  const [copyDraftFocusKey, setCopyDraftFocusKey] = useState<string | null>(null);
  const [bulkFobEditors, setBulkFobEditors] = useState<Record<string, BomBulkFobEditor>>({});
  const [bulkFobErrors, setBulkFobErrors] = useState<Record<string, string>>({});
  const [bulkFobSavingKey, setBulkFobSavingKey] = useState<string | null>(null);
  const [colourSurchargeRules, setColourSurchargeRules] = useState<ColourSurchargeRule[]>([]);
  const [colourSurchargeDrafts, setColourSurchargeDrafts] = useState<Record<string, string>>({});
  const [colourSurchargeStatus, setColourSurchargeStatus] = useState("");
  const [savingColourSurcharges, setSavingColourSurcharges] = useState(false);
  const [colourHexRules, setColourHexRules] = useState<ColourHexRule[]>([]);
  const [colourHexRuleStatus, setColourHexRuleStatus] = useState("");
  const [savingColourHexRuleKey, setSavingColourHexRuleKey] = useState<string | null>(null);
  const [colourSwatchEditor, setColourSwatchEditor] = useState<BomColourSwatchEditor | null>(null);
  const [savingColourSwatchEditor, setSavingColourSwatchEditor] = useState(false);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const materialCodeInputRef = useRef<HTMLInputElement>(null);
  const copyDraftInputRefs = useRef<Record<string, HTMLInputElement | null>>({});
  const bomGroupRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const expandedBomGroupKeyRef = useRef<string | null>(null);
  const [dragSku, setDragSku] = useState<string | null>(null);
  const [dragOverTier, setDragOverTier] = useState<string | null>(null);
  const dragEnterCount = useRef(0);
  const dragMaterialCode = useRef<string | null>(null); // bypass dataTransfer quirks
  const [addColourKey, setAddColourKey] = useState<string | null>(null); // "{bomTemplate}|{tierName}" to show inline form
  const addColourCodeRef = useRef<HTMLInputElement>(null);
  const addColourNameRef = useRef<HTMLInputElement>(null);
  const [editingBoms, setEditingBoms] = useState<Set<string>>(new Set());
  const toggleEditBom = (key: string) => {
    setEditingBoms(prev => { const n = new Set(prev); n.has(key) ? n.delete(key) : n.add(key); return n; });
  };

  // Double-confirm delete state + performance refs
  const [pendingDeletes, setPendingDeletes] = useState<Set<string>>(new Set());
  const pendingDeleteTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const loadRef = useRef(false);  // prevent concurrent loads
  const currentLoadKeyRef = useRef<string | null>(null);
  const pendingLoadKeyRef = useRef<string | null>(null);
  const loadTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);  // debounce loads
  const activeFobCountriesRef = useRef<string[]>([]);


  // Color name → hex mapping for paint swatches
  const colourToHex = (name: string): string[] => {
    const n = name.toLowerCase();
    const map: Record<string, string> = {
      'carbon crystal black': '#1a1a1a', 'black': '#1a1a1a', 'new carbon black': '#1c1c1c',
      'khaki white': '#f0ece0', 'new khaki white': '#f5f0e8', 'white': '#f5f5f0',
      'moonlight silver': '#d4d0c8', 'silver': '#c0c0c0', 'aviation silver': '#c8c0b8',
      'olive gray': '#8a8a7a', 'gray': '#808080', 'fjord grey': '#6e7a7a', 'matte gray': '#5a5a5a', 'matte gray（black edition）': '#3a3a3a',
      'blood red': '#8b0000', 'red': '#cc0000',
      'aurora green': '#2ecc71', 'aquatic green': '#1abc9c', 'alpine green': '#27ae60',
      'mist green': '#82b74b', 'misty green': '#7daa4a', 'model green': '#3a7d44',
      'glacier blue': '#5b8db8', 'blue': '#1a5276',
    };
    // Check exact match first, then partial
    if (map[n]) return [map[n]];
    for (const [k, v] of Object.entries(map)) {
      if (n.includes(k)) return [v];
    }
    return ['#94a3b8']; // default gray
  };

  // Get swatch colors for a color name (handles dual colors with &)
  const getSwatchColors = (name: string): string[] => {
    const parts = name.split(/[&／]/);
    return parts.flatMap(p => colourToHex(p.trim()));
  };

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

  const colourHexConflicts = useMemo(
    () => colourHexRules.filter((rule) => rule.status === "conflict"),
    [colourHexRules],
  );

  const getColourSurchargeAmount = (brand: string, colourType: string): number => {
    const key = colourSurchargeKey(brand, colourType);
    const rule = colourSurchargeRules.find(
      (item) => colourSurchargeKey(item.brand, item.colourType) === key,
    );
    return rule ? Number(rule.surchargeEur) : DEFAULT_COLOUR_SURCHARGES[key] ?? 0;
  };

  const formatBomFobTooltip = (
    countryCode: string,
    baseFob: number | null | undefined,
    colourSurchargeEur?: number | null,
    fobSourceMode?: string | null,
    fobSourceCountryCode?: string | null,
  ): string => {
    const fobLabel = baseFob != null && baseFob > 0
      ? `FOB ${baseFob.toLocaleString()} EUR`
      : "No FOB";
    const surchargeLabel = colourSurchargeEur != null && colourSurchargeEur > 0
      ? ` · surcharge +${colourSurchargeEur.toLocaleString()} EUR`
      : "";
    const sourceLabel = formatBomFobSourceLabel(fobSourceMode, fobSourceCountryCode);
    return `${countryTooltipByCode.get(countryCode) || formatCountryCodeTooltip(countryCode)} · ${fobLabel}${surchargeLabel}${sourceLabel ? ` · ${sourceLabel}` : ""}`;
  };

  const formatBomFobSourceLabel = (
    fobSourceMode?: string | null,
    fobSourceCountryCode?: string | null,
  ): string => {
    if (fobSourceMode === "copied_from_country") {
      return `copied from ${fobSourceCountryCode || "source country"}`;
    }
    if (fobSourceMode === "manual_country_adjust") return "manual country adjustment";
    if (fobSourceMode === "manual_edit") return "manual edit";
    if (fobSourceMode === "explicit_price_by_payment_term") return "uploaded/resolved FOB";
    return "";
  };

  const getBomFobSourceMarker = (fobSourceMode?: string | null): string => {
    if (fobSourceMode === "copied_from_country") return "C";
    if (fobSourceMode === "manual_country_adjust") return "B";
    if (fobSourceMode === "manual_edit") return "M";
    return "";
  };

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

  const loadColourHexRules = useCallback(async () => {
    try {
      const res = await api.getOrderGeniusColourHexRules();
      setColourHexRules(res.items || []);
      setColourHexRuleStatus("");
    } catch (e) {
      setColourHexRuleStatus(getErrorMessage(e));
    }
  }, []);

  const load = useCallback(async (s?: string) => {
    const loadKey = s ?? "";
    if (loadRef.current) {
      if (currentLoadKeyRef.current !== loadKey) {
        pendingLoadKeyRef.current = loadKey;
      }
      return;
    }
    loadRef.current = true;
    currentLoadKeyRef.current = loadKey;
    setLoading(true);
    try {
      const normalizedSearch = String(s || "").trim().toUpperCase();
      const isCountry = /^[A-Z]{2}$/.test(normalizedSearch);
      const params: { country?: string; search?: string } = {};
      if (s) {
        if (isCountry) {
          const fobCountries = activeFobCountriesRef.current;
          if (fobCountries.includes(normalizedSearch)) {
            params.country = normalizedSearch;
            setBomAdminNotice("");
          } else if (fobCountries.length > 0) {
            const sourceCountry = fobCountries.includes("CZ")
              ? "CZ"
              : fobCountries.find((countryCode) => countryCode !== normalizedSearch) || "";
            setToolsFlipped(true);
            setShowAddMaterial(false);
            setBomAdminNotice(`${normalizedSearch} has no active BOM FOB yet. Showing all BOM templates so you can copy FOB into ${normalizedSearch}.`);
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
            params.search = s;
            setBomAdminNotice("");
          }
        } else {
          params.search = s;
          setBomAdminNotice("");
        }
      } else {
        setBomAdminNotice("");
      }
      const res = await api.getBomAdmin(Object.keys(params).length > 0 ? params : undefined);
      setSkus(res.items || []);
      const nextCountries = res.countries || [];
      const nextActiveFobCountries = res.activeFobCountries || nextCountries;
      activeFobCountriesRef.current = nextActiveFobCountries;
      setCountries(nextCountries);
      setActiveFobCountries(nextActiveFobCountries);
    } catch (e) { console.error('[BOM Admin]', e); }
    finally {
      loadRef.current = false;
      currentLoadKeyRef.current = null;
      setLoading(false);
      const pendingLoadKey = pendingLoadKeyRef.current;
      pendingLoadKeyRef.current = null;
      if (pendingLoadKey !== null) {
        window.setTimeout(() => {
          void load(pendingLoadKey || undefined);
        }, 0);
      }
    }
  }, []);

  const scheduleLoad = useCallback((delay = 0) => {
    if (loadTimerRef.current) clearTimeout(loadTimerRef.current);
    loadTimerRef.current = setTimeout(() => load(), delay);
  }, [load]);

  useEffect(() => { load(); }, [load]);
  useEffect(() => { void loadColourSurcharges(); }, [loadColourSurcharges]);
  useEffect(() => { void loadColourHexRules(); }, [loadColourHexRules]);

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
    load(debouncedSearch || undefined);
  }, [debouncedSearch]);

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

  const handleFobSave = async () => {
    if (!editFob) return;
    try {
      for (const mc of editFob.materialCodes) {
        await api.updateSkuFob(mc, { countryCode: editFob.countryCode, finalFobEur: editFob.fob });
      }
      setEditFob(null);
      scheduleLoad(200);
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
    ]);
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
    if (draft.bulkSelectedCountries.length === 0) {
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
    for (const code of draft.bulkSelectedCountries) {
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
    ]);

  const getFilledBomCountryCodes = (allSkus: any[]): string[] =>
    getBomCountryCodes(allSkus).filter((countryCode) =>
      allSkus.some((sku: any) => getDraftBaseFob(sku?.fobByCountry?.[countryCode]) != null),
    );

  const getBulkFobEditor = (
    bomKey: string,
    allSkus: any[],
  ): BomBulkFobEditor =>
    bulkFobEditors[bomKey] || {
      deltaEur: "",
      selectedCountries: getFilledBomCountryCodes(allSkus),
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
    scope: "all" | "filled" | "clear",
  ) => {
    updateBulkFobEditor(bomKey, allSkus, (current) => ({
      ...current,
      selectedCountries:
        scope === "all"
          ? getBomCountryCodes(allSkus)
          : scope === "filled"
            ? getFilledBomCountryCodes(allSkus)
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
      for (const update of updates) {
        await api.updateSkuFob(update.materialCode, {
          countryCode: update.countryCode,
          finalFobEur: update.finalFobEur,
          paymentTermCode: update.paymentTermCode ?? undefined,
        });
      }
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
      await load();
    } catch (err) {
      setBulkFobErrors((prev) => ({
        ...prev,
        [bomKey]: getErrorMessage(err),
      }));
    } finally {
      setBulkFobSavingKey((current) => (current === bomKey ? null : current));
    }
  };

  const handleCopyMaterialFromBom = (
    draftKey: string,
    bomTemplate: string,
    ref: any,
    allSkus: any[],
    sourceDisplayLabel?: string,
  ) => {
    const initialTemplate = String(
      bomTemplate || deriveMaterialTemplate(allSkus.map((sku: any) => String(sku.materialCode || "")).filter(Boolean)) || ref.materialCode || "",
    ).trim().toUpperCase();
    const sourceInfo = ref.sourcePayload || {};
    const modelName = String(ref.modelName || "");
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
      powertrain: String(ref.powertrain || "ICE"),
      interiorColorName: String(ref.interiorColorName || ""),
      editionTag: ref.editionTag ? String(ref.editionTag) : null,
      lifecycleStatus: String(ref.lifecycleStatus || "active"),
      effectiveFrom: ref.effectiveFrom ? String(ref.effectiveFrom) : null,
      effectiveTo: ref.effectiveTo ? String(ref.effectiveTo) : null,
      fobByCountry: Object.fromEntries(
        Object.entries(ref.fobByCountry || {}).map(([countryCode, fob]) => [
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
      bulkSelectedCountries: getFilledDraftCountryCodes(baseDraft),
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
    const normalizedTemplate = draft.bomTemplate.trim().toUpperCase();
    if (!normalizedTemplate) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "BOM template is required." }));
      return;
    }
    if (draft.skus.length > 1 && !normalizedTemplate.includes("**")) {
      setCopyDraftErrors((prev) => ({ ...prev, [draftKey]: "Multiple colours need a BOM template with **." }));
      return;
    }

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
      for (const sku of draft.skus) {
        const materialCode = resolveMaterialCodeFromTemplate(
          normalizedTemplate,
          sku.colourCode,
          sku.sourceMaterialCode,
        );
        await api.createMaterialSku({
          materialCode,
          bomTemplate: normalizedTemplate,
          brand: draft.brand,
          modelName: draft.modelName,
          version: draft.version,
          colour: sku.colour,
          colourCode: sku.colourCode,
          colourType: sku.colourType || "single",
          powertrain: draft.powertrain || "ICE",
          sourceBomTemplate: draft.sourceBomTemplate,
        });
        const effectiveColourTier = inferBomAdminColourTier(sku);
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
        if (draft.lifecycleStatus !== "active" || draft.effectiveFrom || draft.effectiveTo) {
          await api.updateSkuLifecycle(materialCode, {
            lifecycleStatus: draft.lifecycleStatus || "active",
            effectiveFrom: draft.effectiveFrom || undefined,
            effectiveTo: draft.effectiveTo || undefined,
            rowVersion: 1,
          });
        }
        for (const countryCode of draft.bulkSelectedCountries) {
          const fob = draft.fobByCountry[countryCode];
          const baseFob = getDraftBaseFob(fob);
          if (baseFob == null) continue;
          await api.updateSkuFob(materialCode, {
            countryCode,
            finalFobEur: Number(baseFob),
            paymentTermCode: fob?.paymentTermCode ?? undefined,
          });
        }
      }
      dismissCopyDraft(draftKey);
      await load();
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

  const handleSetColourHexStandard = async (rule: ColourHexRule, colourHex: string) => {
    const key = `${rule.brand}|${rule.colourCode}|${rule.normalizedColourName}|${colourHex}`;
    try {
      setSavingColourHexRuleKey(key);
      setColourHexRuleStatus("");
      const result = await api.setOrderGeniusColourHexRuleStandard({
        brand: rule.brand,
        colourCode: rule.colourCode,
        colourName: rule.colourName,
        colourHex,
      });
      await loadColourHexRules();
      scheduleLoad(100);
      setColourHexRuleStatus(`Set ${rule.colourCode} ${rule.colourName} to ${result.colourHex}; updated ${result.updated} SKUs.`);
    } catch (e) {
      setColourHexRuleStatus(getErrorMessage(e));
    } finally {
      setSavingColourHexRuleKey(null);
    }
  };

  const normalizeColourPickerValue = (value: string, fallback = "#94A3B8"): string => {
    const text = String(value || "").trim().toUpperCase();
    return /^#[0-9A-F]{6}$/.test(text) ? text : fallback;
  };

  const isColourPickerValue = (value: string): boolean => /^#[0-9A-Fa-f]{6}$/.test(String(value || "").trim());

  const handleSaveColourSwatchEditor = async () => {
    if (!colourSwatchEditor) return;
    const hex1 = normalizeColourPickerValue(colourSwatchEditor.hex1);
    const hex2 = normalizeColourPickerValue(colourSwatchEditor.hex2, hex1);
    const colourHex = colourSwatchEditor.isDual ? `${hex1}|${hex2}` : hex1;
    setSavingColourSwatchEditor(true);
    setColourHexRuleStatus("");
    try {
      const result = await api.setOrderGeniusColourHexRuleStandard({
        brand: colourSwatchEditor.brand,
        colourCode: colourSwatchEditor.colourCode,
        colourName: colourSwatchEditor.colourName,
        colourHex,
      });
      setColourSwatchEditor(null);
      await loadColourHexRules();
      await load();
      setColourHexRuleStatus(`Set ${result.colourCode} ${result.colourName} to ${result.colourHex}; updated ${result.updated} SKUs.`);
    } catch (e) {
      setColourHexRuleStatus(getErrorMessage(e));
    } finally {
      setSavingColourSwatchEditor(false);
    }
  };

  const handleProductMetadataSave = async (
    event: FormEvent<HTMLFormElement>,
    materialCodes: string[],
  ) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    const leadCode = materialCodes[0];
    if (!leadCode) return;
    try {
      await api.updateSkuMetadata(leadCode, {
        materialCodes,
        brand: String(form.get("brand") || ""),
        modelName: String(form.get("modelName") || ""),
        version: String(form.get("version") || ""),
        powertrain: String(form.get("powertrain") || ""),
      });
      load();
    } catch (err) {
      alert(getErrorMessage(err));
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
        `${res.sourceCountryCode} -> ${res.targetCountryCode}: ${res.created} created, ${res.updated} updated, ${res.skipped} skipped, ${res.unchanged} unchanged.`,
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

  // Shared colour chip renderer used by BOM rows
  const renderColourChip = (s: any, isHist: boolean, editing: boolean) => {
    const effectiveTier = inferBomAdminColourTier(s);
    const customHexRaw = s.colourHex || '';
    const customHexParts = customHexRaw ? customHexRaw.split('|') : [];
    const hasCustomDual = customHexParts.length >= 2;
    const computed = getSwatchColors(s.colour || '');
    const isDual = computed.length >= 2 || hasCustomDual || effectiveTier === "dual";
    // For display: custom overrides computed
    const hex1 = customHexParts[0] || computed[0] || '#94a3b8';
    const hex2 = customHexParts[1] || (hasCustomDual ? undefined : computed[1]);
    const displayHex = (!isDual || hasCustomDual) ? hex1 : undefined;
    const isDragging = dragSku === s.materialCode;
    const brand = String(s.brand || "").trim();
    const colourCode = String(s.colourCode || "").trim().toUpperCase();
    const colourName = String(s.colour || "").trim();
    const canEditSwatchRule = Boolean(brand && colourCode && colourName);
    const surchargeLabel = effectiveTier === "dual"
      ? `Dual +${formatSurchargeDraft(getColourSurchargeAmount(brand, "dual"))}€`
      : effectiveTier === "special"
        ? `Special +${formatSurchargeDraft(getColourSurchargeAmount(brand, "special"))}€`
        : "Single";

    return (
      <span key={s.materialCode}
        draggable={editing}
        onDragStart={editing ? (e: any) => {
          e.dataTransfer.effectAllowed = 'move';
          dragMaterialCode.current = s.materialCode;
          setDragSku(s.materialCode);
        } : undefined}
        onDragEnd={editing ? () => { setDragSku(null); setDragOverTier(null); dragMaterialCode.current = null; } : undefined}
        title={`${s.colour}${s.colourCode ? ` (${s.colourCode})` : ''}${isDual ? ' · 双色' : ''} · Tier: ${effectiveTier} — Drag to reclassify, click swatch to edit colour rule`}
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
          title={canEditSwatchRule
            ? `Edit swatch rule for ${brand} ${colourCode} ${colourName}`
            : "Missing brand, colour code or colour name"}
          onClick={(event) => {
            event.stopPropagation();
            if (!canEditSwatchRule) {
              setColourHexRuleStatus("Brand, colour code and colour name are required to edit a swatch rule.");
              return;
            }
            const rect = event.currentTarget.getBoundingClientRect();
            const editorWidth = 238;
            const editorHeight = isDual ? 142 : 104;
            const viewportWidth = typeof window === "undefined" ? editorWidth : window.innerWidth;
            const viewportHeight = typeof window === "undefined" ? editorHeight : window.innerHeight;
            setColourHexRuleStatus("");
            setColourSwatchEditor({
              materialCode: s.materialCode,
              brand,
              colourCode,
              colourName,
              isDual,
              hex1: normalizeColourPickerValue(hex1),
              hex2: normalizeColourPickerValue(hex2 || hex1, normalizeColourPickerValue(hex1)),
              anchorLeft: Math.max(8, Math.min(rect.left, viewportWidth - editorWidth - 8)),
              anchorTop: Math.max(8, Math.min(rect.bottom + 6, viewportHeight - editorHeight - 8)),
            });
          }}
          style={{
            width: 18,
            height: 18,
            padding: 0,
            borderRadius: 3,
            flexShrink: 0,
            border: customHexRaw ? '2px solid #3b82f6' : '1px solid #d1d5db',
            background: isDual
              ? `linear-gradient(135deg, ${hex1} 50%, ${hex2 || hex1} 50%)`
              : displayHex || hex1,
            opacity: isHist ? 0.5 : 1,
            cursor: "pointer",
          }}
        />
        {s.colourCodeConfirmed === false ? (
          <span title="Unconfirmed colour code — click to confirm" style={{ fontWeight: 700, whiteSpace: "nowrap", color: '#dc2626', cursor: 'pointer', textDecoration: 'underline' }}
            onClick={async (e2: any) => { e2.stopPropagation();
              const newCode = prompt('Enter correct colour code:', s.colourCode || '');
              if (newCode) { try { await api.updateColourCode(s.materialCode, newCode.toUpperCase()); load(); } catch {} }
              else { try { await api.confirmColourCode(s.materialCode); load(); } catch {} }
            }}>
            {s.colourCode || s.colour}
          </span>
        ) : editing ? (
          <span title="Click to edit colour code" style={{ fontWeight: 500, whiteSpace: "nowrap", fontSize: 9, color: effectiveTier === 'special' ? '#d97706' : effectiveTier === 'dual' ? '#2563eb' : '#16a34a', cursor: 'pointer' }}
            onClick={async (e2: any) => { e2.stopPropagation();
              const newCode = prompt('Edit colour code (leave blank to unconfirm):', s.colourCode || '');
              if (newCode != null) { try { await api.updateColourCode(s.materialCode, newCode.toUpperCase()); load(); } catch {} }
            }}>
            {s.colourCode || s.colour}
          </span>
        ) : (
	          <span title={`${s.colour} · ${surchargeLabel} · Tier: ${effectiveTier}`}
	            style={{ fontWeight: 500, whiteSpace: "nowrap", fontSize: 9, color: effectiveTier === 'special' ? '#d97706' : effectiveTier === 'dual' ? '#2563eb' : '#16a34a' }}>
	            {s.colourCode || s.colour}
          </span>
        )}
        {editing ? (
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
    const customHexRaw = sku.colourHex || "";
    const customHexParts = customHexRaw ? customHexRaw.split("|") : [];
    const computed = getSwatchColors(sku.colour || "");
    const first = customHexParts[0] || computed[0] || "#94a3b8";
    const second = customHexParts[1] || computed[1];
    const isDual = Boolean(second);
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
            border: customHexRaw ? "2px solid #3b82f6" : "1px solid #d1d5db",
            background: isDual
              ? `linear-gradient(135deg, ${first} 50%, ${second || first} 50%)`
              : first,
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
      const pt = (s.modelName || '').toUpperCase().includes('HEV') ? 'HEV' :
                 (s.modelName || '').toUpperCase().includes('SHS') ? 'PHEV' :
                 (s.modelName || '').toUpperCase().includes('BEV') || (s.modelName || '').toUpperCase().includes(' EV') ? 'BEV' :
                 (s.modelName || '').toUpperCase().includes('ICE') ? 'ICE' : 'Other';
      const mk = `${s.brand}|${s.modelName}|${pt}`;
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
        const tier = inferBomAdminColourTier(s);
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
  }, [collectCountryCodes, sortedCountries]);

  const sortedModelGroupEntries = useMemo(() => {
    return [...modelGroups.entries()].sort(([a], [b]) => {
      // OMODA before JAECOO, then by model number (smaller first)
      const brandA = a.split('|')[0] || '';
      const brandB = b.split('|')[0] || '';
      if (brandA !== brandB) return brandA === 'OMODA' ? -1 : brandA === 'JAECOO' ? 1 : brandA.localeCompare(brandB);
      const numberA = parseInt((a.match(/\d+/) || ['0'])[0]) || 0;
      const numberB = parseInt((b.match(/\d+/) || ['0'])[0]) || 0;
      return numberA - numberB;
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

  if (loading && skus.length === 0 && countries.length === 0) return <div style={{ padding: 16, color: "#64748b" }}>Loading BOM data...</div>;

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
    ? (isPhoneToolsLayout ? 540 : isCompactToolsLayout ? 430 : 252)
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
            <span><b>B</b> country adjustment</span>
            <span><b>M</b> cell edit</span>
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
                void load(nextSearch || undefined);
              }
            }}
            className="bom-admin-search-input" />
          <button
            className="btn btn-sm btn-ghost"
            style={{ flexShrink: 0 }}
            onClick={async () => {
              setSearchText("");
              setDebouncedSearch("");
              await load();
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
                  <button className="btn btn-sm btn-ghost" onClick={toggleAddMaterialForm}>
                    {addMaterialButtonLabel}
                  </button>
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
                <div className="bom-admin-tool-tile" style={{ minHeight: 124 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 7 }}>
                    <span style={{ fontSize: 11, fontWeight: 800, color: "#334155" }}>Colour Swatch Rules</span>
                    <button className="btn btn-sm btn-ghost" type="button" onClick={() => void loadColourHexRules()}>
                      Refresh
                    </button>
                  </div>
                  <div style={{ fontSize: 10, color: "#64748b", marginBottom: 7 }}>
                    {colourHexConflicts.length > 0
                      ? `${colourHexConflicts.length} conflicts need a standard swatch`
                      : `${colourHexRules.length} collected rules · no conflicts`}
                  </div>
                  <div style={{ display: "grid", gap: 6, maxHeight: 104, overflowY: "auto", paddingRight: 2 }}>
                    {colourHexConflicts.length === 0 ? (
                      <div style={{ fontSize: 11, color: "#0f766e", padding: "8px 0" }}>Colour rules clean.</div>
                    ) : (
                      colourHexConflicts.slice(0, 4).map((rule) => (
                        <div
                          key={`${rule.brand}|${rule.colourCode}|${rule.normalizedColourName}`}
                          style={{ padding: 6, background: "#fff", border: "1px solid #e2e8f0", borderRadius: 4 }}
                        >
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 6, alignItems: "center" }}>
                            <span style={{ fontSize: 10, fontWeight: 800, color: "#334155", overflow: "hidden", textOverflow: "ellipsis" }}>
                              {rule.brand} · {rule.colourCode} · {rule.colourName}
                            </span>
                            <span style={{ fontSize: 9, color: "#94a3b8", whiteSpace: "nowrap" }}>{rule.skuCount} SKUs</span>
                          </div>
                          <div style={{ display: "flex", gap: 5, flexWrap: "wrap", marginTop: 5 }}>
                            {rule.hexOptions.map((option) => {
                              const optionKey = `${rule.brand}|${rule.colourCode}|${rule.normalizedColourName}|${option.colourHex}`;
                              return (
                                <button
                                  key={option.colourHex}
                                  className="btn btn-sm btn-ghost"
                                  type="button"
                                  disabled={savingColourHexRuleKey === optionKey}
                                  title={`Set ${option.colourHex} as standard for ${rule.brand} ${rule.colourCode} ${rule.colourName}`}
                                  onClick={() => void handleSetColourHexStandard(rule, option.colourHex)}
                                  style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "2px 6px", fontSize: 10 }}
                                >
                                  <span
                                    style={{
                                      width: 14,
                                      height: 14,
                                      borderRadius: 3,
                                      border: "1px solid #cbd5e1",
                                      background: option.colourHex.includes("|")
                                        ? `linear-gradient(135deg, ${option.colourHex.split("|")[0]} 50%, ${option.colourHex.split("|")[1]} 50%)`
                                        : option.colourHex,
                                    }}
                                  />
                                  {option.colourHex} · {option.skuCount}
                                </button>
                              );
                            })}
                          </div>
                        </div>
                      ))
                    )}
                    {colourHexConflicts.length > 4 ? (
                      <div style={{ fontSize: 10, color: "#b45309" }}>
                        {colourHexConflicts.length - 4} more conflicts. Resolve visible ones, then refresh.
                      </div>
                    ) : null}
                  </div>
                  {colourHexRuleStatus ? (
                    <div style={{ marginTop: 7, fontSize: 11, color: colourHexRuleStatus.startsWith("Set") ? "#0f766e" : "#b45309" }}>
                      {colourHexRuleStatus}
                    </div>
	                  ) : null}
	                </div>
	              </div>
              {bomAdminNotice ? (
                <div style={{ marginTop: 8, padding: "8px 10px", border: "1px solid #bfdbfe", background: "#eff6ff", color: "#1d4ed8", fontSize: 11, fontWeight: 700 }}>
                  {bomAdminNotice}
                </div>
              ) : null}
              {copyCountryMessage ? (
                <div style={{ fontSize: 11, color: copyCountryMessage.includes("created") ? "#0f766e" : "#dc2626", fontWeight: 600 }}>
                  {copyCountryMessage}
                </div>
              ) : null}
            </>
          }
        />
      </div>
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
      {colourSwatchEditor ? (
        <div
          onClick={(event) => event.stopPropagation()}
          style={{
            position: "fixed",
            left: colourSwatchEditor.anchorLeft,
            top: colourSwatchEditor.anchorTop,
            zIndex: 3000,
            width: 238,
            padding: 8,
            background: "#fff",
            border: "1px solid #cbd5e1",
            boxShadow: "0 12px 30px rgba(15,23,42,0.18)",
            borderRadius: 4,
            cursor: "default",
            color: "#334155",
          }}
        >
          <div style={{ fontSize: 10, fontWeight: 800, marginBottom: 6, whiteSpace: "normal" }}>
            {colourSwatchEditor.brand} · {colourSwatchEditor.colourCode} · {colourSwatchEditor.colourName}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "58px 32px 1fr", gap: 6, alignItems: "center", marginBottom: 6 }}>
            <span style={{ fontSize: 10, color: "#64748b" }}>Primary</span>
            <input
              type="color"
              value={normalizeColourPickerValue(colourSwatchEditor.hex1)}
              onChange={(event) => setColourSwatchEditor(prev => prev ? { ...prev, hex1: event.target.value.toUpperCase() } : prev)}
              style={{ width: 30, height: 26, padding: 0 }}
            />
            <input
              type="text"
              value={colourSwatchEditor.hex1}
              onChange={(event) => setColourSwatchEditor(prev => prev ? { ...prev, hex1: event.target.value.toUpperCase() } : prev)}
              style={{ fontSize: 11, minWidth: 0 }}
            />
            {colourSwatchEditor.isDual ? (
              <Fragment>
                <span style={{ fontSize: 10, color: "#64748b" }}>Second</span>
                <input
                  type="color"
                  value={normalizeColourPickerValue(colourSwatchEditor.hex2, normalizeColourPickerValue(colourSwatchEditor.hex1))}
                  onChange={(event) => setColourSwatchEditor(prev => prev ? { ...prev, hex2: event.target.value.toUpperCase() } : prev)}
                  style={{ width: 30, height: 26, padding: 0 }}
                />
                <input
                  type="text"
                  value={colourSwatchEditor.hex2}
                  onChange={(event) => setColourSwatchEditor(prev => prev ? { ...prev, hex2: event.target.value.toUpperCase() } : prev)}
                  style={{ fontSize: 11, minWidth: 0 }}
                />
              </Fragment>
            ) : null}
          </div>
          <div style={{ display: "flex", justifyContent: "flex-end", gap: 6 }}>
            <button
              className="btn btn-sm btn-ghost"
              type="button"
              onClick={() => setColourSwatchEditor(null)}
              disabled={savingColourSwatchEditor}
            >
              Cancel
            </button>
            <button
              className="btn btn-sm btn-primary"
              type="button"
              onClick={() => void handleSaveColourSwatchEditor()}
              disabled={
                savingColourSwatchEditor
                || !isColourPickerValue(colourSwatchEditor.hex1)
                || (colourSwatchEditor.isDual && !isColourPickerValue(colourSwatchEditor.hex2))
              }
            >
              {savingColourSwatchEditor ? "Saving..." : "Save"}
            </button>
          </div>
        </div>
      ) : null}
      {showAddMaterial && (
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
	          <select value={newMaterial.powertrain} onChange={e => setNewMaterial({...newMaterial, powertrain: e.target.value})}
	            style={{ fontSize: 11, width: 70 }}>
	            {['BEV','HEV','PHEV','ICE','MHEV','REEV'].map(p => <option key={p} value={p}>{p}</option>)}
	          </select>
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
                          <th title="Lifecycle" style={{ ...bomHeaderBaseStyle, width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }}>LC</th>
                          <th title="Actions" style={{ ...bomHeaderBaseStyle, width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions }}>Actions</th>
                          <th title="Effective from" style={{ ...bomHeaderBaseStyle, width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from }}>From</th>
                          <th title="Effective to" style={{ ...bomHeaderBaseStyle, width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to }}>To</th>
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
                          const editing = editingBoms.has(bomTemplate);
                          const draftKey = `${mk}|${vk}|${bomTemplate}`;
                          const copyDraft = copyDrafts[draftKey];
                          const bulkFobEditor = bulkFobEditors[bomTemplate] || {
                            deltaEur: "",
                            selectedCountries: tiers.filledCountryCodes,
                          };
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
                                  try { await api.updateColourTier(mc, tierName); load(); } catch (e) { console.error('Drag drop failed', e); }
                                }
                              },
                            } : {};
                            return (
                              <td
                                {...dragProps}
                                style={{
                                  padding: '3px 5px',
                                  outline: isOver ? `2px dashed ${borderColor}` : 'none',
                                  outlineOffset: -2,
                                  verticalAlign: 'top',
                                  ...getBomStickyCellStyle(tierName, isOver ? bgColor : "#fff", 1),
                                }}>
                                <div style={{ display: "flex", gap: 3, flexWrap: "wrap", alignItems: "center" }}>
                                  {tierSkus.length === 0 ? (
                                    <span style={{ fontSize: 9, color: '#cbd5e1' }}>—</span>
                                  ) : (
                                    tierSkus.map((s: any) => renderColourChip(s, isHist, editing))
                                  )}
                                  {editing ? (<span title={`Add colour to ${tierName} tier`}
                                    style={{ cursor: 'pointer', color: '#94a3b8', fontSize: 12, fontWeight: 700, padding: '0 3px' }}
                                    onClick={(e2: any) => {
                                      e2.stopPropagation();
                                      const key = `${bomTemplate}|${tierName}`;
                                      setAddColourKey(addColourKey === key ? null : key);
                                      if (addColourKey !== key) {
                                        setTimeout(() => addColourCodeRef.current?.focus(), 50);
                                      }
                                    }}>＋</span>) : null}
                                </div>
                                {isOver ? <div style={{ fontSize: 9, color: borderColor, marginTop: 2 }}>Drop to reclassify</div> : null}
                                {editing && addColourKey === `${bomTemplate}|${tierName}` ? (
                                  <div style={{ display: 'flex', gap: 3, marginTop: 3, alignItems: 'center' }}
                                    onClick={(e2: any) => e2.stopPropagation()}>
                                    <input ref={addColourCodeRef} type="text" placeholder="Code" maxLength={2}
                                      style={{ width: 28, fontSize: 10, padding: '1px 3px', textTransform: 'uppercase' }}
                                      onKeyDown={async (e2) => {
                                        if (e2.key === 'Enter') addColourNameRef.current?.focus();
                                        if (e2.key === 'Escape') setAddColourKey(null);
                                      }} />
                                    <input ref={addColourNameRef} type="text" placeholder="Name"
                                      style={{ width: 70, fontSize: 10, padding: '1px 3px' }}
                                      onKeyDown={async (e2) => {
                                        if (e2.key === 'Escape') setAddColourKey(null);
                                        if (e2.key !== 'Enter') return;
                                        const code = addColourCodeRef.current?.value?.trim().toUpperCase() || '';
                                        const name = addColourNameRef.current?.value?.trim() || '';
                                        if (!code || !name) return;
                                        const newMat = (bomTemplate || '').includes('**')
                                          ? bomTemplate.replace('**', code)
                                          : (ref as any).materialCode?.replace(/[A-Z]{2}/, code) || '';
                                        if (!newMat) return;
                                        try {
                                          await api.createMaterialSku({
                                            materialCode: newMat,
                                            bomTemplate: bomTemplate,
                                            brand: (ref as any).brand || '',
                                            modelName: (ref as any).modelName || '',
                                            version: (ref as any).version || '',
                                            colour: name, colourCode: code, colourType: 'single',
                                            powertrain: (ref as any).powertrain || 'ICE',
                                          });
                                          await api.updateColourTier(newMat, tierName);
                                          const intN = (ref as any).interiorColorName;
                                          if (intN) await api.updateSkuInterior(newMat, { interiorColorName: intN, editionTag: (ref as any).editionTag || null });
                                          setAddColourKey(null);
                                          load();
                                        } catch (err) { /* */ }
                                      }} />
                                    <span style={{ cursor: 'pointer', fontSize: 10, color: '#16a34a' }}
                                      onClick={async () => {
                                        const code = addColourCodeRef.current?.value?.trim().toUpperCase() || '';
                                        const name = addColourNameRef.current?.value?.trim() || '';
                                        if (!code || !name) return;
                                        const newMat = (bomTemplate || '').includes('**')
                                          ? bomTemplate.replace('**', code)
                                          : (ref as any).materialCode?.replace(/[A-Z]{2}/, code) || '';
                                        if (!newMat) return;
                                        try {
                                          await api.createMaterialSku({
                                            materialCode: newMat,
                                            bomTemplate: bomTemplate,
                                            brand: (ref as any).brand || '',
                                            modelName: (ref as any).modelName || '',
                                            version: (ref as any).version || '',
                                            colour: name, colourCode: code, colourType: 'single',
                                            powertrain: (ref as any).powertrain || 'ICE',
                                          });
                                          await api.updateColourTier(newMat, tierName);
                                          const intN = (ref as any).interiorColorName;
                                          if (intN) await api.updateSkuInterior(newMat, { interiorColorName: intN, editionTag: (ref as any).editionTag || null });
                                          setAddColourKey(null);
                                          load();
                                        } catch (err) { /* */ }
                                      }}>✓</span>
                                    <span style={{ cursor: 'pointer', fontSize: 10, color: '#94a3b8' }}
                                      onClick={() => setAddColourKey(null)}>✕</span>
                                  </div>
                                ) : null}
                                {editing && tierSkus.length === 0 ? (
                                  <div style={{ fontSize: 8, color: '#cbd5e1' }}>Drag here or click ＋</div>
                                ) : null}
                              </td>
                            );
                          };
                          return (
                            <Fragment key={bomTemplate}>
                            <tr
                              style={isHist ? { opacity: 0.55, textDecoration: "line-through" }
                                   : isPhaseOut ? { opacity: 0.75 } : undefined}>
                              <td style={{
                                borderLeft: `3px solid ${isHist ? '#9ca3af' : isPhaseOut ? '#d97706' : '#16a34a'}`,
                                color: isHist ? '#9ca3af' : '#1e293b',
                                maxWidth: 160,
                                overflow: "hidden",
                                ...getBomStickyCellStyle("bom", "white", 1),
                              }}>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" defaultValue={bomTemplate}
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
                                <div style={{ fontSize: 8, color: sourceWarnings.length ? '#d97706' : '#94a3b8', marginTop: 1 }}>
                                  {sourceLabel}{sourceWarnings.length > 0 ? ` ⚠${sourceWarnings.length}` : ''}
                                </div>
                              </td>
                              <td style={getBomStickyCellStyle("interior", "white", 1)}>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" defaultValue={intName + (edTag ? ` · ${edTag}` : '')}
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
                                      load();
                                    }}
                                    style={{ fontSize: 10, width: '100%', minWidth: 80 }} />
                                ) : (
                                  <span style={{ fontSize: 10, color: intName ? '#1e293b' : '#cbd5e1' }}>
                                    {intName || '—'}{edTag ? <span style={{ color: '#7c3aed' }}> · {edTag}</span> : null}
                                  </span>
                                )}
                              </td>
                              {renderTierCell('single', tiers.single, '#16a34a', '#f0fdf4')}
                              {renderTierCell('dual', tiers.dual, '#2563eb', '#eff6ff')}
                              {renderTierCell('special', tiers.special, '#d97706', '#fffbeb')}
                              <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }}>
                                {editingBoms.has(bomTemplate) ? (
                                  <select value={ref.lifecycleStatus || 'active'}
                                    onChange={async (e) => {
                                      const v = e.target.value;
                                      for (const s of allSkus) {
                                        try { await api.updateSkuLifecycle(s.materialCode, { lifecycleStatus: v, rowVersion: s.rowVersion }); } catch {}
                                      }
                                      load();
                                    }}
                                    style={{ fontSize: 10, width: 80 }}>
                                    <option value="active">Active</option>
                                    <option value="phase_out">Phase Out</option>
                                    <option value="historical">Historical</option>
                                  </select>
                                ) : (
                                  <span style={{ fontSize: 10, fontWeight: 600,
                                    color: isHist ? '#9ca3af' : isPhaseOut ? '#d97706' : '#16a34a' }}>
                                    {ref.lifecycleStatus || 'active'}
                                  </span>
                                )}
                              </td>
                              <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, textAlign: "center" }}>
                                <div style={{ display: "inline-flex", gap: 6, alignItems: "center", justifyContent: "center" }}>
                                  {pendingDeletes.has(bomTemplate) ? (
                                    <button className="btn btn-sm" title="Click again to confirm delete"
                                      style={{ fontSize: 10, padding: "1px 6px", color: '#fff', background: '#dc2626' }}
                                      onClick={async () => {
                                        for (const s of allSkus) {
                                          try { await api.deleteMaterialSku(s.materialCode); } catch {}
                                        }
                                        setPendingDeletes(prev => { const n = new Set(prev); n.delete(bomTemplate); return n; });
                                        scheduleLoad(300);
                                      }}>Confirm?</button>
                                  ) : (
                                    <button className="btn btn-sm btn-ghost" title="Delete permanently — double-click"
                                      style={{ fontSize: 10, padding: "1px 6px", color: '#dc2626' }}
                                      onClick={() => setPendingDeletes(new Set([bomTemplate]))}>Delete</button>
                                  )}
                                  <button className="btn btn-sm btn-ghost"
                                    style={{ fontSize: 10, padding: '1px 6px', color: editingBoms.has(bomTemplate) ? '#16a34a' : '#64748b' }}
                                    onClick={() => toggleEditBom(bomTemplate)}>
                                    {editingBoms.has(bomTemplate) ? 'Save' : 'Edit'}
                                  </button>
                                </div>
                              </td>
                              <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from }}>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" placeholder="YYYY-MM" defaultValue={ref.effectiveFrom || ''}
                                    onBlur={async (e) => {
                                      const v = e.target.value || null;
                                      for (const s of allSkus) {
                                        try { await api.updateSkuLifecycle(s.materialCode, { lifecycleStatus: s.lifecycleStatus, effectiveFrom: v ?? undefined, rowVersion: s.rowVersion }); } catch {}
                                      }
                                    }}
                                    style={{ width: "100%", fontSize: 10 }} />
                                ) : (
                                  <span style={{ fontSize: 10, color: ref.effectiveFrom ? '#1e293b' : '#cbd5e1' }}>{ref.effectiveFrom || '—'}</span>
                                )}
                              </td>
                              <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to }}>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" placeholder="YYYY-MM" defaultValue={ref.effectiveTo || ''}
                                    onBlur={async (e) => {
                                      const v = e.target.value || null;
                                      for (const s of allSkus) {
                                        try { await api.updateSkuLifecycle(s.materialCode, { lifecycleStatus: s.lifecycleStatus, effectiveTo: v ?? undefined, rowVersion: s.rowVersion }); } catch {}
                                      }
                                    }}
                                    style={{ width: "100%", fontSize: 10 }} />
                                ) : (
                                  <span style={{ fontSize: 10, color: ref.effectiveTo ? '#1e293b' : '#cbd5e1' }}>{ref.effectiveTo || '—'}</span>
                                )}
                              </td>
                              {sortedCountries.map(c => {
                                const fob = ref.fobByCountry?.[c];
                                const baseFob = getDraftBaseFob(fob);
                                const hasFob = fob != null && baseFob != null && baseFob > 0;
                                const hasSurcharge = fob?.colourSurchargeEur && fob.colourSurchargeEur > 0;
                                const sourceMarker = getBomFobSourceMarker(fob?.fobSourceMode);
                                const financeCountries = Array.isArray((ref as { financeCountries?: unknown }).financeCountries)
                                  ? ((ref as { financeCountries: string[] }).financeCountries)
                                  : [];
                                const hasFinance = financeCountries.includes(c);
                                return (
                                  <td key={c} className="bom-fob-price-cell" title={formatBomFobTooltip(c, baseFob, fob?.colourSurchargeEur, fob?.fobSourceMode, fob?.fobSourceCountryCode)} style={{ width: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, minWidth: BOM_ADMIN_COUNTRY_COLUMN_WIDTH, textAlign: "right", cursor: "pointer", padding: "2px 4px" }}
                                    onClick={() => {
                                      if (c === "NL") {
                                        void openFinanceQuickCard({
                                          countryCode: c,
                                          materialCode: String(bomTemplate || ref.materialCode || allCodes[0] || ""),
                                          materialCodes: allCodes,
                                          title: `${c} finance · ${bomTemplate}`,
                                          fob: baseFob ?? null,
                                        });
                                        return;
                                      }
                                      setEditFob({ materialCodes: allCodes, countryCode: c, fob: baseFob ?? null });
                                    }}>
                                    <span className="bom-fob-price-value" style={{ color: hasFob ? "#0f766e" : "#cbd5e1", fontWeight: hasFob ? 600 : 400 }}>
                                      {hasFob ? baseFob!.toLocaleString() : "-"}
                                      {hasSurcharge ? <sup style={{ color: '#d97706', fontSize: 9 }}> +{fob.colourSurchargeEur}</sup> : null}
                                      {hasFinance ? (
                                        <sup className="bom-finance-source-mark" title={`${c} finance / CBU maintained`}>
                                          %
                                        </sup>
                                      ) : null}
                                      {sourceMarker ? (
                                        <sup className="bom-fob-source-mark" title={formatBomFobSourceLabel(fob?.fobSourceMode, fob?.fobSourceCountryCode)}>
                                          {sourceMarker}
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
                                <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.lifecycle }}>
                                  <select
                                    value={copyDraft.lifecycleStatus || "active"}
                                    onChange={(event) => {
                                      const value = event.target.value;
                                      updateCopyDraft(draftKey, (draft) => ({
                                        ...draft,
                                        lifecycleStatus: value,
                                      }));
                                    }}
                                    style={{ fontSize: 10, width: 80 }}
                                  >
                                    <option value="active">Active</option>
                                    <option value="phase_out">Phase Out</option>
                                    <option value="historical">Historical</option>
                                  </select>
                                </td>
                                <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.actions, textAlign: "center" }}>
                                  <div style={{ display: "inline-flex", gap: 6, alignItems: "center", justifyContent: "center" }}>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      style={{ fontSize: 10, padding: "1px 6px", color: "#64748b" }}
                                      onClick={() => dismissCopyDraft(draftKey)}
                                    >
                                      Cancel
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-primary"
                                      style={{ fontSize: 10, padding: "1px 6px" }}
                                      disabled={copyDraftSavingKey === draftKey}
                                      onClick={() => void handleSaveCopiedBom(draftKey)}
                                    >
                                      {copyDraftSavingKey === draftKey ? "Adding..." : "Add"}
                                    </button>
                                  </div>
                                </td>
                                <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.from }}>
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
                                    style={{ width: "100%", fontSize: 10 }}
                                  />
                                </td>
                                <td style={{ width: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to, minWidth: BOM_ADMIN_TRAILING_COLUMN_WIDTHS.to }}>
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
                                    style={{ width: "100%", fontSize: 10 }}
                                  />
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
                                      style={{ fontSize: 10, padding: "2px 8px" }}
                                      onClick={() => applyCopyDraftFobDelta(draftKey)}
                                    >
                                      Apply to {copyDraft.bulkSelectedCountries.length || 0}
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      style={{ fontSize: 10, padding: "2px 8px", color: "#2563eb", borderColor: "#bfdbfe", background: "#eff6ff" }}
                                      onClick={() => applyCopyDraftFobDelta(draftKey, 200)}
                                    >
                                      +200
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      style={{ fontSize: 10, padding: "2px 8px", color: "#b45309", borderColor: "#fed7aa", background: "#fff7ed" }}
                                      onClick={() => applyCopyDraftFobDelta(draftKey, -300)}
                                    >
                                      -300
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      title="Select countries that already have FOB on this copied row"
                                      style={{ fontSize: 10, padding: "2px 8px" }}
                                      onClick={() => setCopyDraftCountryScope(draftKey, "filled")}
                                    >
                                      Filled
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      title="Select every visible country column"
                                      style={{ fontSize: 10, padding: "2px 8px" }}
                                      onClick={() => setCopyDraftCountryScope(draftKey, "all")}
                                    >
                                      All
                                    </button>
                                    <button
                                      type="button"
                                      className="btn btn-sm btn-ghost"
                                      title="Clear selected countries"
                                      style={{ fontSize: 10, padding: "2px 8px" }}
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
                                <td colSpan={BOM_ADMIN_FIXED_COLUMN_COUNT + sortedCountries.length} style={{ background: "#f8fafc", borderLeft: "3px solid #2563eb", padding: "6px 8px", position: "relative", zIndex: 2 }}>
                                  <div style={{ display: "grid", gap: 8 }}>
                                    <form onSubmit={(event) => handleProductMetadataSave(event, allCodes)}
                                      style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                                      <span style={{ fontSize: 10, color: "#64748b", fontWeight: 700 }}>Product fields</span>
                                      <input name="brand" type="text" required defaultValue={(ref as any).brand || ""}
                                        placeholder="Brand" style={{ width: 90, fontSize: 11, textTransform: "uppercase" }} />
                                      <input name="modelName" type="text" required defaultValue={(ref as any).modelName || ""}
                                        placeholder="Model" style={{ width: 150, fontSize: 11 }} />
                                      <input name="version" type="text" required defaultValue={(ref as any).version || ""}
                                        placeholder="Version" style={{ width: 130, fontSize: 11 }} />
                                      <select name="powertrain" required defaultValue={(ref as any).powertrain || mg.pt || "ICE"}
                                        style={{ width: 80, fontSize: 11 }}>
                                        {['BEV','HEV','PHEV','ICE','MHEV','REEV','Other'].map(p => <option key={p} value={p}>{p}</option>)}
                                      </select>
                                      <span style={{ fontFamily: "monospace", fontSize: 10, color: "#94a3b8" }}>
                                        {allCodes.length} SKUs
                                      </span>
                                      <button
                                        type="button"
	                                        className="btn btn-sm btn-ghost"
	                                        style={{ fontSize: 10, padding: "2px 8px", color: "#2563eb", borderColor: "#bfdbfe", background: "#eff6ff" }}
	                                        onClick={() => handleCopyMaterialFromBom(draftKey, bomTemplate, ref, allSkus, sourceLabel)}
	                                      >
                                        Copy Material
                                      </button>
                                      <button type="submit" className="btn btn-sm btn-primary" style={{ fontSize: 10, padding: "2px 8px" }}>
                                        Save fields
                                      </button>
                                    </form>
                                    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                                      <span style={{ fontSize: 10, color: "#1d4ed8", fontWeight: 700 }}>FOB tools</span>
                                      <input
                                        type="number"
                                        value={bulkFobEditor.deltaEur}
                                        placeholder="± EUR"
                                        onChange={(event) => {
                                          const value = event.target.value;
                                          updateBulkFobEditor(bomTemplate, allSkus, (current) => ({
                                            ...current,
                                            deltaEur: value,
                                          }));
                                        }}
                                        style={{ width: 90, fontSize: 11 }}
                                      />
                                      <button
                                        type="button"
                                        className="btn btn-sm btn-primary"
                                        style={{ fontSize: 10, padding: "2px 8px" }}
                                        disabled={bulkFobSavingKey === bomTemplate}
                                        onClick={() => void applyBulkFobDelta(bomTemplate, allSkus)}
                                      >
                                        {bulkFobSavingKey === bomTemplate ? "Saving..." : `Apply to ${bulkFobEditor.selectedCountries.length || 0}`}
                                      </button>
                                      <button
                                        type="button"
                                        className="btn btn-sm btn-ghost"
                                        style={{ fontSize: 10, padding: "2px 8px", color: "#2563eb", borderColor: "#bfdbfe", background: "#eff6ff" }}
                                        disabled={bulkFobSavingKey === bomTemplate}
                                        onClick={() => void applyBulkFobDelta(bomTemplate, allSkus, 200)}
                                      >
                                        +200
                                      </button>
                                      <button
                                        type="button"
                                        className="btn btn-sm btn-ghost"
                                        style={{ fontSize: 10, padding: "2px 8px", color: "#b45309", borderColor: "#fed7aa", background: "#fff7ed" }}
                                        disabled={bulkFobSavingKey === bomTemplate}
                                        onClick={() => void applyBulkFobDelta(bomTemplate, allSkus, -300)}
                                      >
                                        -300
                                      </button>
                                      <button
                                        type="button"
                                        className="btn btn-sm btn-ghost"
                                        title="Select countries that already have FOB on this BOM"
                                        style={{ fontSize: 10, padding: "2px 8px" }}
                                        onClick={() => setBulkFobCountryScope(bomTemplate, allSkus, "filled")}
                                      >
                                        Filled
                                      </button>
                                      <button
                                        type="button"
                                        className="btn btn-sm btn-ghost"
                                        title="Select every visible country column"
                                        style={{ fontSize: 10, padding: "2px 8px" }}
                                        onClick={() => setBulkFobCountryScope(bomTemplate, allSkus, "all")}
                                      >
                                        All
                                      </button>
                                      <button
                                        type="button"
                                        className="btn btn-sm btn-ghost"
                                        title="Clear selected countries"
                                        style={{ fontSize: 10, padding: "2px 8px" }}
                                        onClick={() => setBulkFobCountryScope(bomTemplate, allSkus, "clear")}
                                      >
                                        Clear
                                      </button>
                                    </div>
                                    {bulkFobErrors[bomTemplate] ? (
                                      <div style={{ fontSize: 10, color: "#dc2626", fontWeight: 600 }}>
                                        {bulkFobErrors[bomTemplate]}
                                      </div>
                                    ) : null}
                                    <details>
                                      <summary style={{ cursor: "pointer", fontSize: 10, color: "#475569", fontWeight: 600 }}>
                                        Selected countries ({bulkFobEditor.selectedCountries.length})
                                      </summary>
                                      <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 8 }}>
                                        {tiers.countryCodes.map((countryCode) => {
                                          const hasFob = tiers.filledCountryCodeSet.has(countryCode);
                                          return (
                                            <label
                                              key={`${bomTemplate}-country-${countryCode}`}
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
                                              title={`${countryCode}${countryLabels.get(countryCode) ? ` · ${countryLabels.get(countryCode)}` : ""}${hasFob ? "" : " · no FOB on this BOM yet"}`}
                                            >
                                              <input
                                                type="checkbox"
                                                checked={bulkFobEditor.selectedCountries.includes(countryCode)}
                                                onChange={(event) => toggleBulkFobCountry(bomTemplate, allSkus, countryCode, event.target.checked)}
                                              />
                                              <span style={{ fontWeight: 700 }}>{countryCode}</span>
                                            </label>
                                          );
                                        })}
                                      </div>
                                    </details>
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
                            materialCodes: financeQuickCard.materialCodes,
                            countryCode: financeQuickCard.countryCode,
                            fob: financeQuickCard.fob,
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
      {editFob ? (
        <div className="bom-finance-modal-backdrop" onClick={() => setEditFob(null)}>
          <div className="bom-fob-edit-modal-shell" onClick={(event) => event.stopPropagation()}>
            <div className="bom-fob-edit-card">
              <div>
                <span className="bom-finance-eyebrow">BOM ADMIN · FOB</span>
                <h4>Edit FOB</h4>
                <p>{editFob.materialCodes.length} material codes</p>
              </div>
              <div className="bom-fob-edit-grid">
                <label>
                  <span>Country</span>
                  <input
                    type="text"
                    value={editFob.countryCode}
                    onChange={(event) => setEditFob({ ...editFob, countryCode: event.target.value.toUpperCase() })}
                  />
                </label>
                <label>
                  <span>FOB EUR</span>
                  <input
                    type="number"
                    value={editFob.fob ?? ""}
                    onChange={(event) => setEditFob({ ...editFob, fob: event.target.value === "" ? null : Number(event.target.value) })}
                  />
                </label>
              </div>
              <BomFinanceActionBar
                actions={[
                  { label: `Save ${editFob.materialCodes.length}`, kind: "primary", onClick: () => void handleFobSave() },
                  { label: "Cancel", onClick: () => setEditFob(null) },
                ]}
              />
            </div>
          </div>
        </div>
      ) : null}
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
