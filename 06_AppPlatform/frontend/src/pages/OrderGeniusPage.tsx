import {
  Fragment,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type DragEvent,
  type KeyboardEvent,
} from "react";

import { api, apiUrl } from "../api/client";
import { useAuth } from "../contexts/AuthContext";
import { useResolvedCountry } from "../hooks/useResolvedCountry";
import type { CellValueChangedEvent } from "ag-grid-community";
import {
  getOrderGeniusRowId,
  OrderGeniusGrid,
  type OrderGeniusGridRow,
} from "../components/OrderGeniusGrid";
import { DeckFloatingDrawer } from "../components/deckControls/DeckFloatingDrawer";
import type {
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

function normalizeAccountCode(value: string): string {
  return value.trim().toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 12);
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

function compareProductGroupEntries(a: ProductGroupEntry, b: ProductGroupEntry): number {
  const [brandA = "", modelA = "", versionA = "", ptA = ""] = a[0].split("|");
  const [brandB = "", modelB = "", versionB = "", ptB = ""] = b[0].split("|");
  const brandRankDiff = brandDisplayRank(brandA) - brandDisplayRank(brandB);
  if (brandRankDiff !== 0) return brandRankDiff;
  const brandDiff = brandA.localeCompare(brandB);
  if (brandDiff !== 0) return brandDiff;
  const modelNumberDiff = firstModelNumber(modelA) - firstModelNumber(modelB);
  if (modelNumberDiff !== 0) return modelNumberDiff;
  return modelA.localeCompare(modelB) || versionA.localeCompare(versionB) || ptA.localeCompare(ptB);
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
      .filter((c) => !q || c.countryName.toLowerCase().includes(q) || c.countryCode.toLowerCase().includes(q))
      .map((c) => ({ value: c.countryCode, label: c.countryName }));
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
  const [groupByProduct, setGroupByProduct] = useState(true);
  const [expandedProductGroups, setExpandedProductGroups] = useState<Set<string>>(() => new Set());
  const [showPtAdmin, setShowPtAdmin] = useState(false);
  const [showBomAdmin, setShowBomAdmin] = useState(false);
  const [showDeck, setShowDeck] = useState(true);
  const [consolidatedView, setConsolidatedView] = useState(false);
  const [hideEmptyRows, setHideEmptyRows] = useState(false);

  const [options, setOptions] = useState<OrderGeniusOptions | null>(null);
  const [matrices, setMatrices] = useState<Record<string, MatrixResponse>>({});
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
    if (selectedCountries.length === 0) return;
    setLoading(true);
    setError("");
    const params = {
      year: selectedYear,
      brand: brandFilter || undefined,
      model: modelFilter || undefined,
      powertrain: powertrainFilter || undefined,
      version: versionFilter || undefined,
      colour: colourFilter || undefined,
      materialCodeSearch: materialSearch || undefined,
    };
    Promise.all(
      selectedCountries.map((country) =>
        api
          .getOrderGeniusMatrix({ country, ...params })
          .then((matrix) => [country, matrix] as const)
          .catch(() => [country, null] as const),
      ),
    )
      .then((results) => {
        const next: Record<string, MatrixResponse> = {};
        for (const [country, matrix] of results) {
          if (matrix) next[country] = matrix;
        }
        setMatrices(next);
      })
      .catch((e: unknown) => setError(getErrorMessage(e)))
      .finally(() => setLoading(false));
  }, [
    selectedCountries, selectedYear, brandFilter, modelFilter,
    powertrainFilter, versionFilter, colourFilter, materialSearch,
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

  // ── Grid data + cell editing ──────────────────────────────────────

  const flatRows = ((): OrderGeniusGridRow[] => {
    const makeRow = (r: MatrixRowWithCountry): OrderGeniusGridRow => {
      const row: OrderGeniusGridRow = {
        materialCode: r.materialCode,
        modelName: r.modelName,
        version: r.version,
        colour: r.colour,
        interiorColorName: r.interiorColorName,
        fobEur: r.fobEur ?? null,
        lifecycleStatus: r.lifecycleStatus,
        editable: r.editable,
        remark: r.remark ?? undefined,
        _countryCode: r._countryCode,
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
      return combinedMatrix.rows.map(makeRow);
    }

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
      // Sum TTL, monthly totals, and floor FOB for the group
      let groupTtl = 0;
      let floorFob: number | null = null;
      const monthlySums: number[] = new Array(13).fill(0); // index 1-12
      for (const r of groupRows) {
        const months = r.months || {};
        for (let m = 1; m <= 12; m++) {
          const q = months[String(m)]?.quantity ?? 0;
          monthlySums[m] += q;
          groupTtl += q;
        }
        const fob = r.fobEur ?? 0;
        if (fob > 0 && (floorFob === null || fob < floorFob)) {
          floorFob = fob;
        }
      }
      const groupFob = floorFob ?? (groupRows[0]?.fobEur ?? null);
      const expanded = expandedProductGroups.has(groupKey);
      const displayName = formatProductModelName(brand, modelName, version);
      const labelName = formatProductModelName(brand, modelName);
      // Group header row (use group key as materialCode so getRowId is unique)
      const header: OrderGeniusGridRow = {
        materialCode: `__grp_${groupKey.replace(/[^a-zA-Z0-9]/g, '_')}`,
        modelName: displayName,
        version: "",
        colour: "",
        fobEur: groupFob,
        lifecycleStatus: "active",
        editable: false,
        remark: "",
        _countryCode: groupRows[0]?._countryCode,
        _versions: {},
        _errors: {},
        _saving: new Set(),
        __type: "groupHeader",
        __groupLabel: `${labelName} · ${version} · ${pt} · ${groupRows.length} variants · ${groupTtl.toLocaleString()} units`,
        __groupColor: color,
        __groupKey: groupKey,
        __expanded: expanded,
      };
      for (let m = 1; m <= 12; m++) header[`month_${m}`] = monthlySums[m];
      result.push(header);
      // Child rows
      if (expanded || (consolidatedView && selectedCountries.length > 1)) {
        for (const r of groupRows) {
          result.push(makeRow(r));
        }
      }
    }
    return result;
  })();

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
  }, [flatRows, consolidatedView, selectedCountries, hideEmptyRows, selectedMonth]);

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

  return (
    <section className="crud-shell">
      <header className="crud-hero">
        <h1>Order Genius</h1>
        <p>
          Country order matrix with FOB pricing, monthly quantity editing, and
          Excel export.
        </p>
      </header>

      <DeckFloatingDrawer
        open={showDeck}
        onOpenChange={setShowDeck}
        triggerPrimary="筛选 / 操作"
        triggerSecondaryOpen="收起面板"
        triggerSecondaryClosed="打开面板"
        eyebrow="Order Genius"
        title="筛选与操作"
        ariaLabel="Order Genius controls"
      >
      {error ? (
        <div className="alert alert-error" style={{ marginBottom: 16 }}>
          {error}
        </div>
      ) : null}

      {/* ── Filter bar ─────────────────────────────────────────────── */}
      <div
        style={{
          display: "flex",
          gap: 12,
          flexWrap: "wrap",
          alignItems: "center",
          marginBottom: 16,
        }}
      >
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
          {combinedMatrix.rows.map((r, index) => (
            <option key={`${r._countryCode || ""}-${r.materialCode}-${index}`} value={r.materialCode}>
              {r.remark ? `${r.materialCode} (${r.remark})` : r.materialCode}
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

        {isAdmin && (
          <button type="button" className="btn btn-sm btn-ghost"
                  onClick={() => setShowPtAdmin(!showPtAdmin)}
                  style={showPtAdmin ? { background: "#0f766e", color: "#fff" } : undefined}>
            {showPtAdmin ? "Hide PT Admin" : "Payment Terms"}
          </button>
        )}
        {isAdmin && (
          <button type="button" className="btn btn-sm btn-ghost"
                  onClick={() => setShowBomAdmin(!showBomAdmin)}
                  style={showBomAdmin ? { background: "#b45309", color: "#fff" } : undefined}>
            {showBomAdmin ? "Hide BOM Admin" : "BOM Admin"}
          </button>
        )}

        <button type="button" className="btn btn-sm btn-primary" onClick={loadMatrices}>
          Refresh
        </button>
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
      </div>

      {/* ── Quantity Import Modal ────────────────────────────────── */}
      {showQtyImport ? (
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
      {showUpload ? (
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
      <div style={{ display: "flex", gap: 12, flexWrap: "wrap", marginBottom: 8, fontSize: 12 }}>
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
      </DeckFloatingDrawer>

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
          {selectedCountries.length > 0
            ? "No data. Upload a Material Master file to get started."
            : "Select a country to view the order matrix."}
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
            overflow: "auto", borderRadius: 16,
            background: "#fff",
            boxShadow: "0 25px 80px rgba(15,23,42,0.3)",
            WebkitOverflowScrolling: "touch",
          }}>
            <BomAdminPanel />
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

// ── Payment Terms Admin Panel ──────────────────────────────────────────

// ── BOM Admin Panel ──────────────────────────────────────────────────

function BomAdminPanel() {
  const [skus, setSkus] = useState<any[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchText, setSearchText] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [editFob, setEditFob] = useState<{ materialCodes: string[]; countryCode: string; fob: number | null } | null>(null);
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [showAddMaterial, setShowAddMaterial] = useState(false);
  const [newMaterial, setNewMaterial] = useState({ materialCode: "", brand: "", modelName: "", version: "", colour: "", colourCode: "", powertrain: "ICE" });
  const searchInputRef = useRef<HTMLInputElement>(null);
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
  const loadTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);  // debounce loads


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

  const load = useCallback(async (s?: string) => {
    if (loadRef.current) return;  // skip if already loading
    loadRef.current = true;
    setLoading(true);
    try {
      const isCountry = s && /^[A-Z]{2}$/.test(s);
      const params: any = {};
      if (s) {
        if (isCountry) params.country = s;
        else params.search = s;
      }
      const res = await api.getBomAdmin(Object.keys(params).length > 0 ? params : undefined);
      setSkus(res.items || []);
      setCountries(res.countries || []);
    } catch (e) { console.error('[BOM Admin]', e); }
    finally { loadRef.current = false; setLoading(false); }
  }, []);

  const scheduleLoad = useCallback((delay = 0) => {
    if (loadRef.current) return;
    if (loadTimerRef.current) clearTimeout(loadTimerRef.current);
    loadTimerRef.current = setTimeout(() => load(), delay);
  }, [load]);

  useEffect(() => { load(); }, [load]);

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

  const handleFobSave = async () => {
    if (!editFob) return;
    try {
      for (const mc of editFob.materialCodes) {
        await api.updateSkuFob(mc, { countryCode: editFob.countryCode, finalFobEur: editFob.fob ?? undefined } as any);
      }
      setEditFob(null); scheduleLoad(200);
    } catch (e) { alert(getErrorMessage(e)); }
  };

  const toggleGroup = (key: string) => {
    setExpandedGroups(prev => { const next = new Set(prev); next.has(key) ? next.delete(key) : next.add(key); return next; });
  };

  // Shared colour chip renderer used by BOM rows
  const renderColourChip = (s: any, isHist: boolean, editing: boolean) => {
    const customHexRaw = s.colourHex || '';
    const customHexParts = customHexRaw ? customHexRaw.split('|') : [];
    const hasCustomDual = customHexParts.length >= 2;
    const computed = getSwatchColors(s.colour || '');
    const isDual = computed.length >= 2 || hasCustomDual;
    // For display: custom overrides computed
    const hex1 = customHexParts[0] || computed[0] || '#94a3b8';
    const hex2 = customHexParts[1] || (hasCustomDual ? undefined : computed[1]);
    const displayHex = (!isDual || hasCustomDual) ? hex1 : undefined;
    const isDragging = dragSku === s.materialCode;
    const brand = s.brand || '';

    const openColourPicker = (defaultHex: string, callback: (val: string) => void) => {
      const inp = document.createElement('input');
      inp.type = 'color';
      inp.value = defaultHex;
      inp.style.position = 'fixed'; inp.style.opacity = '0';
      document.body.appendChild(inp);
      inp.click();
      inp.addEventListener('change', () => { callback(inp.value); inp.remove(); });
      inp.addEventListener('blur', () => inp.remove());
    };

    return (
      <span key={s.materialCode}
        draggable={editing}
        onDragStart={editing ? (e: any) => {
          e.dataTransfer.effectAllowed = 'move';
          dragMaterialCode.current = s.materialCode;
          setDragSku(s.materialCode);
        } : undefined}
        onDragEnd={editing ? () => { setDragSku(null); setDragOverTier(null); dragMaterialCode.current = null; } : undefined}
        title={`${s.colour}${s.colourCode ? ` (${s.colourCode})` : ''}${isDual ? ' · 双色' : ''} · Tier: ${s.colourTier || 'single'} — Drag to reclassify, Click to edit colour`}
        style={{ display: "inline-flex", alignItems: "center", gap: 2, fontSize: 10, color: isHist ? '#9ca3af' : '#475569', cursor: isDragging ? 'grabbing' : 'grab', opacity: isDragging ? 0.4 : 1 }}
        onClick={(e) => {
          if (dragSku) return;
          e.stopPropagation();
          if (isDual) {
            // Dual-tone: pick both colours sequentially
            openColourPicker(hex1, (val1) => {
              openColourPicker(hex2 || val1, async (val2) => {
                const combined = `${val1}|${val2}`;
                try { await api.updateColourHex(s.materialCode, combined); load(); } catch {}
              });
            });
          } else {
            // Single colour
            openColourPicker(hex1, async (val) => {
              try { await api.updateColourHex(s.materialCode, val || null); load(); } catch {}
            });
          }
        }}>
        <span style={{
          display: "inline-block", width: 16, height: 16, borderRadius: 3, flexShrink: 0,
          border: customHexRaw ? '2px solid #3b82f6' : '1px solid #d1d5db',
          background: isDual
            ? `linear-gradient(135deg, ${hex1} 50%, ${hex2 || hex1} 50%)`
            : displayHex || hex1,
          opacity: isHist ? 0.5 : 1,
        }} />
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
          <span title="Click to edit colour code" style={{ fontWeight: 500, whiteSpace: "nowrap", fontSize: 9, color: s.colourTier === 'special' ? '#d97706' : s.colourTier === 'dual' ? '#2563eb' : '#16a34a', cursor: 'pointer' }}
            onClick={async (e2: any) => { e2.stopPropagation();
              const newCode = prompt('Edit colour code (leave blank to unconfirm):', s.colourCode || '');
              if (newCode != null) { try { await api.updateColourCode(s.materialCode, newCode.toUpperCase()); load(); } catch {} }
            }}>
            {s.colourCode || s.colour}
          </span>
        ) : (
          <span title={`${s.colour} · ${s.colourType === 'dual' ? '双色 +' + (brand === 'JAECOO' ? '300' : '200') + '€' : s.colourType === 'special' ? '特殊色 +' + (brand === 'JAECOO' ? '300' : '200') + '€' : '单色'} · Tier: ${s.colourTier || 'single'}`}
            style={{ fontWeight: 500, whiteSpace: "nowrap", fontSize: 9, color: s.colourTier === 'special' ? '#d97706' : s.colourTier === 'dual' ? '#2563eb' : '#16a34a' }}>
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

  // Derive BOM template from material codes: find common prefix + ** + suffix
  const deriveTemplate = (codes: string[]): string => {
    if (codes.length === 0) return '';
    if (codes.length === 1) return codes[0];
    let prefix = codes[0];
    let rev = codes[0].split('').reverse().join('');
    for (const c of codes.slice(1)) {
      let i = 0; while (i < prefix.length && i < c.length && prefix[i] === c[i]) i++;
      prefix = prefix.substring(0, i);
      const crev = c.split('').reverse().join('');
      let j = 0; while (j < rev.length && j < crev.length && rev[j] === crev[j]) j++;
      rev = rev.substring(0, j);
    }
    const suffix = rev.split('').reverse().join('');
    if (prefix && suffix && prefix.length + suffix.length < (codes[0]?.length || 0)) {
      return prefix + '**' + suffix;
    }
    return prefix + suffix || codes[0];
  };

  // Two-level grouping: model+powertrain → version, with multiple BOM template rows per version
  const modelGroups = useMemo(() => {
    const map = new Map<string, { brand: string; modelName: string; pt: string; versions: Map<string, any[]> }>();
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
  const groupByTemplate = (vSkus: any[]): Map<string, { single: any[]; dual: any[]; special: any[] }> => {
    const byPeriod = new Map<string, any[]>();
    for (const s of vSkus) {
      const period = `${s.effectiveFrom || 'any'}_${s.effectiveTo || 'any'}`;
      // Use stored bomTemplate from DB; fall back to single-code derive
      const bt = s.bomTemplate || deriveTemplate([s.materialCode]);
      const gk = `${bt}|${period}`;
      if (!byPeriod.has(gk)) byPeriod.set(gk, []);
      byPeriod.get(gk)!.push(s);
    }
    const result = new Map<string, { single: any[]; dual: any[]; special: any[] }>();
    for (const [gk, gSkus] of byPeriod) {
      const bt = gk.split('|')[0];
      const entry = result.get(bt) || { single: [], dual: [], special: [] };
      for (const s of gSkus) {
        const tier = s.colourTier || 'single';
        if (tier === 'special') entry.special.push(s);
        else if (tier === 'dual') entry.dual.push(s);
        else entry.single.push(s);
      }
      result.set(bt, entry);
    }
    return result;
  };

  if (loading) return <div style={{ padding: 16, color: "#64748b" }}>Loading BOM data...</div>;

  return (
    <div style={{ padding: 20 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12, position: "sticky", top: 0, background: "rgba(255,255,255,0.95)", backdropFilter: "blur(8px)", zIndex: 1, padding: "8px 0", borderBottom: "1px solid #e2e8f0" }}>
        <h3 style={{ margin: 0 }}>BOM / Material Master</h3>
        <span style={{ fontSize: 12, color: "#64748b" }}>{skus.length} SKUs · {modelGroups.size} models · {sortedCountries.length} countries</span>
      </div>
      <div style={{ display: "flex", gap: 8, marginBottom: 12 }}>
        <input ref={searchInputRef} type="text" placeholder="Search model / material / country (e.g. JAECOO7, T716, SE) — auto 1.2s" value={searchText}
          onChange={(e) => setSearchText(e.target.value)}
          style={{ minWidth: 340 }} />
        <button className="btn btn-sm btn-ghost" onClick={() => { setSearchText(''); load(); }}>Clear</button>
        <button className="btn btn-sm btn-ghost" onClick={() => { setShowAddMaterial(!showAddMaterial); }}
          style={{ marginLeft: 'auto' }}>+ Material</button>
      </div>
      {showAddMaterial && (
        <div style={{ display: "flex", gap: 6, marginBottom: 8, padding: 6, background: '#f8fafc', borderRadius: 4, flexWrap: "wrap", alignItems: "center" }}>
          <input type="text" placeholder="Material Code" value={newMaterial.materialCode}
            onChange={e => setNewMaterial({...newMaterial, materialCode: e.target.value})}
            style={{ width: 160, fontSize: 11, fontFamily: 'monospace' }} />
          <input type="text" placeholder="Brand" value={newMaterial.brand}
            onChange={e => setNewMaterial({...newMaterial, brand: e.target.value})}
            style={{ width: 80, fontSize: 11 }} />
          <input type="text" placeholder="Model" value={newMaterial.modelName}
            onChange={e => setNewMaterial({...newMaterial, modelName: e.target.value})}
            style={{ width: 120, fontSize: 11 }} />
          <input type="text" placeholder="Version" value={newMaterial.version}
            onChange={e => setNewMaterial({...newMaterial, version: e.target.value})}
            style={{ width: 100, fontSize: 11 }} />
          <input type="text" placeholder="Colour" value={newMaterial.colour}
            onChange={e => setNewMaterial({...newMaterial, colour: e.target.value})}
            style={{ width: 100, fontSize: 11 }} />
          <input type="text" placeholder="Code" value={newMaterial.colourCode}
            onChange={e => setNewMaterial({...newMaterial, colourCode: e.target.value})}
            style={{ width: 50, fontSize: 11 }} />
          <select value={newMaterial.powertrain} onChange={e => setNewMaterial({...newMaterial, powertrain: e.target.value})}
            style={{ fontSize: 11, width: 70 }}>
            {['BEV','HEV','PHEV','ICE','MHEV','REEV'].map(p => <option key={p} value={p}>{p}</option>)}
          </select>
          <button className="btn btn-sm btn-primary" onClick={async () => {
            if (!newMaterial.materialCode) return;
            try {
              await api.createMaterialSku({...newMaterial, colourType: 'single'});
              setShowAddMaterial(false); setNewMaterial({materialCode:'',brand:'',modelName:'',version:'',colour:'',colourCode:'',powertrain:'ICE'}); load();
            } catch(e) { alert(getErrorMessage(e)); }
          }}>Add</button>
          <button className="btn btn-sm btn-ghost" onClick={() => setShowAddMaterial(false)}>Cancel</button>
        </div>
      )}
      <div style={{ overflowX: "auto", maxHeight: "60vh" }}>
        {[...modelGroups.entries()].sort(([a], [b]) => {
          // OMODA before JAECOO, then by model number (smaller first)
          const ba = a.split('|')[0] || '';
          const bb = b.split('|')[0] || '';
          if (ba !== bb) return ba === 'OMODA' ? -1 : ba === 'JAECOO' ? 1 : ba.localeCompare(bb);
          // Extract number from model name for numeric sort
          const na = parseInt((a.match(/\d+/) || ['0'])[0]) || 0;
          const nb = parseInt((b.match(/\d+/) || ['0'])[0]) || 0;
          return na - nb;
        }).map(([mk, mg]) => {
          const expanded = expandedGroups.has(mk);
          return (
            <div key={mk} style={{ marginBottom: 2 }}>
              <div onClick={() => toggleGroup(mk)}
                style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 10px", cursor: "pointer",
                  background: `${PT_COLORS[mg.pt] ?? '#9ca3af'}15`, borderLeft: `4px solid ${PT_COLORS[mg.pt] ?? '#9ca3af'}`, borderRadius: 2, fontWeight: 700, fontSize: 13 }}>
                <span style={{ fontSize: 14 }}>{expanded ? '▾' : '▸'}</span>
                <span style={{ color: PT_COLORS[mg.pt] ?? '#9ca3af' }}>{mg.brand} {mg.modelName}</span>
                <span style={{ fontWeight: 400, color: "#64748b", fontSize: 11 }}>· {mg.pt} · {mg.versions.size} versions</span>
              </div>
              {expanded && [...mg.versions.entries()].sort(([a],[b]) => a.localeCompare(b)).map(([vk, vSkus]) => {
                const templates = groupByTemplate(vSkus);
                const sortedTemplates = [...templates.entries()].sort(([a], [b]) => a.localeCompare(b));
                return (
                  <div key={mk + '|' + vk} style={{ marginLeft: 20, marginBottom: 8 }}>
                    <div style={{ fontSize: 12, fontWeight: 600, color: '#334155', padding: '4px 0', marginBottom: 2 }}>
                      {vk} · {vSkus.length} colour-SKUs · {templates.size} BOM templates
                    </div>
                    <table className="data-table bom-admin-table" style={{ fontSize: 11, width: "auto", minWidth: "100%" }}>
                      <thead>
                        <tr style={{ position: "sticky", top: 0, zIndex: 2 }}>
                          <th style={{ minWidth: 150, position: "sticky", left: 0, zIndex: 3, background: "#f8fafc" }}>BOM</th>
                          <th style={{ minWidth: 90, position: "sticky", left: 150, zIndex: 3, background: "#f8fafc" }}>Interior</th>
                          <th style={{ minWidth: 120, position: "sticky", left: 240, zIndex: 3, background: "#f8fafc" }}>Single</th>
                          <th style={{ minWidth: 100, position: "sticky", left: 360, zIndex: 3, background: "#f8fafc" }}>Dual</th>
                          <th style={{ minWidth: 70, position: "sticky", left: 460, zIndex: 3, background: "#f8fafc" }}>Special</th>
                          <th style={{ minWidth: 70 }}>Lifecycle</th>
                          <th style={{ width: 55 }}></th>
                          <th style={{ width: 38 }}>Edit</th>
                          <th style={{ width: 65 }}>From</th>
                          <th style={{ width: 65 }}>To</th>
                          {sortedCountries.map(c => (
                            <th key={c} style={{ width: 75, textAlign: "right", color: c === 'NL' ? '#d97706' : '#64748b', fontWeight: c === 'NL' ? 700 : 600 }}>
                              {c}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedTemplates.map(([bomTemplate, tiers]) => {
                          const allSkus = [...tiers.single, ...tiers.dual, ...tiers.special];
                          const ref = allSkus[0];
                          const isHist = ref.lifecycleStatus === 'historical';
                          const isPhaseOut = ref.lifecycleStatus === 'phase_out';
                          const allCodes = allSkus.map((s: any) => s.materialCode);
                          const intName = (ref as any).interiorColorName || '';
                          const edTag = (ref as any).editionTag || '';
                          const sourceInfo = (ref as any).sourcePayload || {};
                          const sourceSheet = (ref as any).sourceSheetName || sourceInfo.sheet_name || '';
                          const sourceRow = (ref as any).sourceRowNumber ?? sourceInfo.row_index;
                          const sourceWarnings = sourceInfo.warnings || [];
                          // Helper: render a tier cell with colour chips and drop zone
                          const editing = editingBoms.has(bomTemplate);
                          const renderTierCell = (tierName: string, tierSkus: any[], borderColor: string, bgColor: string) => {
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
                            const tierLeft: Record<string, number> = { single: 240, dual: 360, special: 460 };
                            return (
                              <td
                                {...dragProps}
                                style={{
                                  padding: '3px 5px',
                                  background: isOver ? bgColor : '#fff',
                                  outline: isOver ? `2px dashed ${borderColor}` : 'none',
                                  outlineOffset: -2,
                                  minWidth: 80,
                                  verticalAlign: 'top',
                                  position: 'sticky',
                                  left: tierLeft[tierName] ?? 0,
                                  zIndex: 1,
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
                            <tr key={bomTemplate}
                              style={isHist ? { opacity: 0.55, textDecoration: "line-through" }
                                   : isPhaseOut ? { opacity: 0.75 } : undefined}>
                              <td style={{ borderLeft: `3px solid ${isHist ? '#9ca3af' : isPhaseOut ? '#d97706' : '#16a34a'}`,
                                color: isHist ? '#9ca3af' : '#1e293b', maxWidth: 160, minWidth: 130,
                                position: "sticky", left: 0, zIndex: 1, background: "white" }}>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" defaultValue={bomTemplate}
                                    placeholder="BOM / Material Code"
                                    onBlur={async (e) => {
                                      const v = e.target.value.trim();
                                      if (!v || v === bomTemplate) return;
                                      // Update ALL SKUs sharing this BOM template
                                      for (const s of allSkus) {
                                        try { await api.updateMaterialCode(s.materialCode, v); } catch {}
                                      }
                                      load();
                                    }}
                                    style={{ fontFamily: "monospace", fontSize: 11, width: "100%", minWidth: 130 }} />
                                ) : (
                                  <div style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                                    title={bomTemplate}>{bomTemplate}</div>
                                )}
                                <div style={{ fontSize: 8, color: sourceWarnings.length ? '#d97706' : '#94a3b8', marginTop: 1 }}>
                                  {sourceSheet}·R{sourceRow} {sourceWarnings.length > 0 ? `⚠${sourceWarnings.length}` : ''}
                                </div>
                              </td>
                              <td style={{ position: "sticky", left: 150, zIndex: 1, background: "white", minWidth: 90 }}>
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
                              <td>
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
                              <td style={{ textAlign: "center" }}>
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
                              </td>
                              <td style={{ textAlign: 'center' }}>
                                <button className="btn btn-sm btn-ghost"
                                  style={{ fontSize: 10, padding: '1px 4px', color: editingBoms.has(bomTemplate) ? '#16a34a' : '#64748b' }}
                                  onClick={() => toggleEditBom(bomTemplate)}>
                                  {editingBoms.has(bomTemplate) ? 'Save' : 'Edit'}
                                </button>
                              </td>
                              <td>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" placeholder="YYYY-MM" defaultValue={ref.effectiveFrom || ''}
                                    onBlur={async (e) => {
                                      const v = e.target.value || null;
                                      for (const s of allSkus) {
                                        try { await api.updateSkuLifecycle(s.materialCode, { lifecycleStatus: s.lifecycleStatus, effectiveFrom: v ?? undefined, rowVersion: s.rowVersion }); } catch {}
                                      }
                                    }}
                                    style={{ width: 60, fontSize: 10 }} />
                                ) : (
                                  <span style={{ fontSize: 10, color: ref.effectiveFrom ? '#1e293b' : '#cbd5e1' }}>{ref.effectiveFrom || '—'}</span>
                                )}
                              </td>
                              <td>
                                {editingBoms.has(bomTemplate) ? (
                                  <input type="text" placeholder="YYYY-MM" defaultValue={ref.effectiveTo || ''}
                                    onBlur={async (e) => {
                                      const v = e.target.value || null;
                                      for (const s of allSkus) {
                                        try { await api.updateSkuLifecycle(s.materialCode, { lifecycleStatus: s.lifecycleStatus, effectiveTo: v ?? undefined, rowVersion: s.rowVersion }); } catch {}
                                      }
                                    }}
                                    style={{ width: 60, fontSize: 10 }} />
                                ) : (
                                  <span style={{ fontSize: 10, color: ref.effectiveTo ? '#1e293b' : '#cbd5e1' }}>{ref.effectiveTo || '—'}</span>
                                )}
                              </td>
                              {sortedCountries.map(c => {
                                const fob = ref.fobByCountry?.[c];
                                const baseFob = fob?.uploadedFobEur ?? fob?.finalFobEur;
                                const hasFob = fob != null && baseFob != null && baseFob > 0;
                                const hasSurcharge = fob?.colourSurchargeEur && fob.colourSurchargeEur > 0;
                                return (
                                  <td key={c} style={{ textAlign: "right", cursor: "pointer", padding: "2px 4px" }}
                                    onClick={() => setEditFob({ materialCodes: allCodes, countryCode: c, fob: baseFob ?? null })}>
                                    <span style={{ color: hasFob ? "#0f766e" : "#cbd5e1", fontWeight: hasFob ? 600 : 400 }}>
                                      {hasFob ? baseFob!.toLocaleString() : "-"}
                                      {hasSurcharge ? <sup style={{ color: '#d97706', fontSize: 9 }}> +{fob.colourSurchargeEur}</sup> : null}
                                    </span>
                                  </td>
                                );
                              })}
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
      {editFob && (
        <div style={{ marginTop: 10, padding: 8, background: "#f1f5f9", borderRadius: 4, display: "flex", gap: 8, alignItems: "center" }}>
          <span style={{ fontFamily: "monospace", fontSize: 11 }}>{editFob.materialCodes.length} material codes</span>
          <span style={{ fontSize: 12 }}>in</span>
          <input type="text" value={editFob.countryCode}
            onChange={(e) => setEditFob({ ...editFob, countryCode: e.target.value.toUpperCase() })}
            style={{ width: 50, fontSize: 12, textAlign: "center" }} />
          <span style={{ fontSize: 12 }}>FOB €</span>
          <input type="number" value={editFob.fob ?? ""}
            onChange={(e) => setEditFob({ ...editFob, fob: e.target.value === "" ? null : Number(e.target.value) })}
            style={{ width: 100, fontSize: 12 }} />
          <button className="btn btn-sm btn-primary" onClick={handleFobSave}>Save to all {editFob.materialCodes.length}</button>
          <button className="btn btn-sm btn-ghost" onClick={() => setEditFob(null)}>Cancel</button>
        </div>
      )}
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
