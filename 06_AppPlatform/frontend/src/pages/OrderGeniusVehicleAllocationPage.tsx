import { useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react";
import { api } from "../api/client";
import { OrderingBrandNotice } from "../components/RoleUpgradeModal";
import { isAdminRole } from "../utils/pageNavigation";
import { CommandSelect, type CommandSelectOption } from "../components/CommandSelect";
import { LoadingActionButton } from "../components/LoadingActionButton";
import { PiInvoiceExportDialog } from "../components/PiInvoiceExportDialog";
import {
  VehicleImportDigestPanel,
  VehicleStatusBoard,
  VinPasteDigestPanel,
} from "../components/vehicleAllocation";
import {
  DeckControlTabs,
  DeckFloatingDrawer,
  type DeckControlTabItem,
} from "../components/deckControls";
import { useAuth } from "../contexts/AuthContext";
import type { PiCocLookup } from "../types/cocLibrary";
import { useAccountCountryOptions } from "../hooks/useAccountCountryOptions";
import type {
  AllocationStatus,
  LogisticsStatus,
  PiOrderDetail,
  PiOrderHeader,
  PiMonthSummary,
  PiVehicleUnit,
  UpdateVehiclePayload,
  VehicleAllocationFilters,
  VehicleStatusFlowConfig,
  VehicleStatusFlowStep,
  VehicleImportPreview,
} from "../types/orderGeniusVehicle";
import { VehicleAllocationGrid, type VehicleGridView, type VehicleColumnKey } from "../components/VehicleAllocationPivotGrid";
import { VehicleAllocationEditor } from "../components/VehicleAllocationEditor";
import { VEHICLE_COLUMNS, COLUMN_GROUPS, matchesVehicleText } from "../components/vehicleAllocationFields";
import { ptColor } from "../utils/colors";
import { formatCountryCodeTooltip } from "../utils/jatoCountries";
import { compareProductModels } from "../utils/orderGeniusProductSort";
import { parseOrderGeniusColourSwatch } from "../utils/orderGeniusColourSwatch";

const ALLOCATION_STATUSES: AllocationStatus[] = [
  "unallocated",
  "reserved",
  "allocated",
  "delivered",
  "cancelled",
];

const LOGISTICS_STATUSES: LogisticsStatus[] = [
  "pending",
  "in_production",
  "ready_for_shipping",
  "on_vessel",
  "arrived_at_port",
  "in_warehouse",
  "ready_for_pickup",
  "delivered",
];

type PiToolTab = "import" | "status" | "view";
const MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

const PI_TOOL_TABS: Array<DeckControlTabItem<PiToolTab>> = [
  { key: "import", label: "Import VINs", caption: "导入 VIN" },
  { key: "status", label: "Update status", caption: "更新状态" },
  { key: "view", label: "View", caption: "筛选与列" },
];

function display(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return String(value);
}

function marketCountriesText(header: PiOrderHeader): string {
  return header.marketCountryCodes?.length > 0 ? header.marketCountryCodes.join("/") : header.countryCode;
}

function normalizeCountryCode(value: string | null | undefined): string {
  return String(value ?? "").trim().toUpperCase();
}

function statusText(value: string): string {
  return value.replaceAll("_", " ");
}

function actionableError(reason: unknown, suggestion: string): string {
  const message = reason instanceof Error ? reason.message.replace(/^[45]\d\d\s+/, "") : "";
  return /[\u3400-\u9fff]/.test(message) && !/\b[45]\d\d\b|traceback|internal server error/i.test(message)
    ? message : suggestion;
}

const DEFAULT_ALLOCATION_STATUS_OPTIONS: Array<CommandSelectOption<AllocationStatus>> = ALLOCATION_STATUSES.map((status) => ({
  value: status,
  label: statusText(status),
}));

const DEFAULT_LOGISTICS_STATUS_OPTIONS: Array<CommandSelectOption<LogisticsStatus>> = LOGISTICS_STATUSES.map((status) => ({
  value: status,
  label: statusText(status),
}));

function isAllocationStatus(value: string): value is AllocationStatus {
  return ALLOCATION_STATUSES.includes(value as AllocationStatus);
}

function isLogisticsStatus(value: string): value is LogisticsStatus {
  return LOGISTICS_STATUSES.includes(value as LogisticsStatus);
}

function statusFlowLabel(step: VehicleStatusFlowStep): string {
  return step.labelZh ? `${step.labelEn} · ${step.labelZh}` : step.labelEn;
}

function allocationOptionsFromFlow(flow: VehicleStatusFlowConfig | null): Array<CommandSelectOption<AllocationStatus>> {
  if (!flow) {
    return DEFAULT_ALLOCATION_STATUS_OPTIONS;
  }
  const options = flow.allocation
    .filter((step): step is VehicleStatusFlowStep & { key: AllocationStatus } => isAllocationStatus(step.key))
    .sort((a, b) => a.order - b.order)
    .map((step) => ({
      value: step.key,
      label: statusFlowLabel(step),
      caption: step.terminal ? "Terminal" : undefined,
    }));
  return options.length > 0 ? options : DEFAULT_ALLOCATION_STATUS_OPTIONS;
}

function logisticsOptionsFromFlow(flow: VehicleStatusFlowConfig | null): Array<CommandSelectOption<LogisticsStatus>> {
  if (!flow) {
    return DEFAULT_LOGISTICS_STATUS_OPTIONS;
  }
  const options = flow.logistics
    .filter((step): step is VehicleStatusFlowStep & { key: LogisticsStatus } => isLogisticsStatus(step.key))
    .sort((a, b) => a.order - b.order)
    .map((step) => ({
      value: step.key,
      label: statusFlowLabel(step),
      caption: step.terminal ? "Terminal" : undefined,
    }));
  return options.length > 0 ? options : DEFAULT_LOGISTICS_STATUS_OPTIONS;
}

function isPiDetail(item: PiOrderDetail | PiVehicleUnit | null): item is PiOrderDetail {
  return Boolean(item && "header" in item);
}

function buildDownload(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

const DEFAULT_COLUMNS = VEHICLE_COLUMNS.filter((column) => !column.optional).map((column) => column.key);

function vehicleCell(vehicle: PiVehicleUnit, key: VehicleColumnKey): ReactNode {
  if (key === "freightEur" || key === "insuranceEur") return vehicle[key] == null ? "—" : vehicle[key].toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  if (key === "cocPdf") return "—";
  if (key === "fobEur") return <span title={vehicle.fobEur == null ? "No confirmed market price snapshot; check PI details / 缺已确认的市场价格快照，请核对 PI 明细" : "Confirmed PI market price snapshot; not today's BOM price / 已确认的 PI 市场价格快照；不是当前 BOM 价格"}>{vehicle.fobEur == null ? "—" : vehicle.fobEur.toLocaleString("en-GB")}</span>;
  if (key === "config" || key === "materialCode") return <span className="va-product-text" style={{ color: ptColor(vehicle.powertrain ?? "", "#475467") }}>{key === "config" ? `${display(vehicle.modelName)} / ${display(vehicle.version)}` : display(vehicle.materialCode)}</span>;
  if (key === "allocationStatus" || key === "logisticsStatus") {
    return <span className={`va-status va-status-${vehicle[key]}`}>{statusText(vehicle[key])}</span>;
  }
  return display(vehicle[key]);
}

export function OrderGeniusVehicleAllocationPage() {
  const { user } = useAuth();
  const isAdmin = isAdminRole(user?.role);
  const canEdit = isAdmin || ((user?.role === "order_filler" || user?.role === "editor") && Boolean(user.brands?.length));
  const { countryOptions: accountCountryOptions } = useAccountCountryOptions();
  const defaultCountry = isAdmin ? "" : user?.primaryCountry ?? "";
  const [filters, setFilters] = useState<VehicleAllocationFilters>({
    country: defaultCountry,
    page: 1,
    pageSize: 100,
  });
  const [piHeaders, setPiHeaders] = useState<PiOrderHeader[]>([]);
  const [piHeaderTotal, setPiHeaderTotal] = useState(0);
  const [piBrowseCountry, setPiBrowseCountry] = useState(defaultCountry);
  const [piBrowseMonth, setPiBrowseMonth] = useState("");
  const [piBrowseYear, setPiBrowseYear] = useState(new Date().getFullYear());
  const [piMonths, setPiMonths] = useState<PiMonthSummary[] | null>(null);
  const [piMonthsError, setPiMonthsError] = useState<string | null>(null);
  const [piMonthsRetry, setPiMonthsRetry] = useState(0);
  const [piBrowsePage, setPiBrowsePage] = useState(1);
  const [piListError, setPiListError] = useState<string | null>(null);
  const [selectedPi, setSelectedPi] = useState<PiOrderDetail | null>(null);
  const [piLoading, setPiLoading] = useState(false);
  const piRequest = useRef(0);
  useEffect(() => () => { piRequest.current += 1; }, []);
  const [cocLookup, setCocLookup] = useState<{ piCode: string; result: PiCocLookup } | null>(null);
  const [cocBusy, setCocBusy] = useState(false);
  const [cocError, setCocError] = useState("");
  const [cocDownloadConfirm, setCocDownloadConfirm] = useState<{ piCode: string; request: number; codes: string[]; missing: number; awaiting: number } | null>(null);
  const [cocFilter, setCocFilter] = useState<"all" | "available" | "missing" | "awaiting_vin">("all");
  const cocRequest = useRef(0);
  const cocResult = cocLookup?.piCode === selectedPi?.header.piCode ? cocLookup?.result : null;
  const cocStatuses = useMemo(() => new Map(cocResult?.items.map((item) => [item.carCode, item.status])), [cocResult]);
  useEffect(() => { cocRequest.current += 1; setCocLookup(null); setCocError(""); setCocDownloadConfirm(null); setCocFilter("all"); }, [selectedPi]);
  const [deleteConfirmPi, setDeleteConfirmPi] = useState<string | null>(null);
  const [showInvoiceExport, setShowInvoiceExport] = useState(false);
  // Multi-select state
  const [selectedCarCodes, setSelectedCarCodes] = useState<Set<string>>(new Set());
  useEffect(() => { setCocDownloadConfirm(null); }, [selectedCarCodes]);
  const [selectedLineCode, setSelectedLineCode] = useState<string | null>(null);
  const [editorTargets, setEditorTargets] = useState<PiVehicleUnit[]>([]);
  const gridView = useRef<VehicleGridView>({ vehicles: [], ordinaryVehicles: [], columns: [] });
  const [ordinaryCodes, setOrdinaryCodes] = useState<ReadonlySet<string>>(new Set());
  const [gridResetKey, setGridResetKey] = useState(0);
  const [searchTerm, setSearchTerm] = useState("");
  const [vinBatchText, setVinBatchText] = useState("");
  const [batchVins, setBatchVins] = useState<string[]>([]);
  const [vinSelectionScope, setVinSelectionScope] = useState<"global" | "filtered">("global");
  const [pivotBy, setPivotBy] = useState<"modelName" | "materialCode" | "logisticsStatus" | "countryCode">("modelName");
  const [sideLoading, setSideLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [bulkSaving, setBulkSaving] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [linesOpen, setLinesOpen] = useState(false);
  const pageRef = useRef<HTMLDivElement>(null);
  const linePanelRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const page = pageRef.current;
    if (!page) return;
    // Measure the actual shell/banner, not a monitor resolution or a second layout state.
    const fitViewport = (): void => {
      const top = page.getBoundingClientRect().top + window.scrollY;
      page.style.setProperty("--va-available-height", `${Math.max(0, window.innerHeight - top)}px`);
    };
    fitViewport();
    const observer = new ResizeObserver(fitViewport);
    document.querySelectorAll(".top-bar,.candidate-environment-banner").forEach((element) => observer.observe(element));
    window.addEventListener("resize", fitViewport);
    return () => { observer.disconnect(); window.removeEventListener("resize", fitViewport); };
  }, []);
  const [visibleColumnKeys, setVisibleColumnKeys] = useState<Set<VehicleColumnKey>>(new Set(DEFAULT_COLUMNS));
  const [toolDrawerOpen, setToolDrawerOpen] = useState(false);
  const [activeToolTab, setActiveToolTab] = useState<PiToolTab>("import");
  const toolBodyRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const panelBody = toolBodyRef.current?.parentElement;
    if (panelBody) panelBody.scrollTop = 0;
  }, [activeToolTab, toolDrawerOpen]);
  const [vinPasteText, setVinPasteText] = useState("");
  const [vinPasteMessage, setVinPasteMessage] = useState("");
  const [vinPasteApplying, setVinPasteApplying] = useState(false);
  const [importPreview, setImportPreview] = useState<VehicleImportPreview | null>(null);
  const [importBusy, setImportBusy] = useState(false);
  const [allowReplacing, setAllowReplacing] = useState(false);
  const [removeVins, setRemoveVins] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);
  const importInputRef = useRef<HTMLInputElement>(null);
  const [statusFlow, setStatusFlow] = useState<VehicleStatusFlowConfig | null>(null);

  const mutationBusy = saving || bulkSaving || vinPasteApplying || importBusy;
  const scopeBusy = mutationBusy || piLoading;
  const pageSize = 100;
  const selectedLine = selectedPi?.lines.find((line) => line.piLineCode === selectedLineCode) ?? null;
  const activeScopeLabel = selectedLine
    ? selectedLine.piLineCode
    : selectedPi
      ? selectedPi.header.piCode
      : "No PI selected";
  const statusFlowCountry = selectedPi?.header.countryCode || filters.country || defaultCountry;
  const statusFlowAccount = selectedPi?.header.orderingAccountCode ?? "";
  const allocationStatusOptions = allocationOptionsFromFlow(statusFlow);
  const logisticsStatusOptions = logisticsOptionsFromFlow(statusFlow);
  const countryCommandOptions = useMemo<Array<CommandSelectOption<string>>>(() => {
    const byCode = new Map<string, CommandSelectOption<string>>();
    accountCountryOptions.forEach((country) => {
      const code = normalizeCountryCode(country.countryCode);
      if (code) {
        byCode.set(code, {
          value: code,
          label: code,
          caption: `${country.countryName} · ${country.countryNameZh}`,
        });
      }
    });
    const addFallbackCode = (value: string | null | undefined): void => {
      const code = normalizeCountryCode(value);
      if (code && !byCode.has(code)) {
        byCode.set(code, {
          value: code,
          label: code,
          caption: formatCountryCodeTooltip(code),
        });
      }
    };
    addFallbackCode(defaultCountry);
    addFallbackCode(filters.country);
    addFallbackCode(piBrowseCountry);
    addFallbackCode(selectedPi?.header.countryCode);
    selectedPi?.header.marketCountryCodes.forEach(addFallbackCode);
    const allowed = new Set([user?.primaryCountry, ...(user?.secondaryCountries ?? [])].map((code) => normalizeCountryCode(code ?? "")));
    return Array.from(byCode.values()).filter((option) => isAdmin || allowed.has(option.value)).sort((a, b) => a.value.localeCompare(b.value));
  }, [accountCountryOptions, defaultCountry, filters.country, piBrowseCountry, selectedPi, user]);
  const vinPasteScopeVehicles = useMemo(() => {
    if (!selectedPi) {
      return [];
    }
    return selectedLineCode
      ? selectedPi.vehicles.filter((vehicle) => vehicle.piLineCode === selectedLineCode)
      : selectedPi.vehicles;
  }, [selectedPi, selectedLineCode]);
  const batchVinSet = new Set(batchVins);
  const batchMatches = vinPasteScopeVehicles.filter((vehicle) => (!filters.country || vehicle.countryCode === filters.country) && vehicle.vin && batchVinSet.has(vehicle.vin.toUpperCase()))
    .sort((a, b) => compareProductModels(a.brand ?? "", a.modelName ?? "", a.powertrain ?? "", b.brand ?? "", b.modelName ?? "", b.powertrain ?? "")
      || (a.version ?? "").localeCompare(b.version ?? "") || (a.bom ?? "").localeCompare(b.bom ?? "")
      || (a.materialCode ?? "").localeCompare(b.materialCode ?? "") || a.carCode.localeCompare(b.carCode));
  const foundVins = new Set(batchMatches.map((vehicle) => vehicle.vin?.toUpperCase()));
  const missingVins = batchVins.filter((vin) => !foundVins.has(vin));
  const filteredMatches = batchMatches.filter((vehicle) => ordinaryCodes.has(vehicle.carCode));
  const currentSelectedCount = [...selectedCarCodes].filter((code) => ordinaryCodes.has(code)).length;
  const gridVehicles = useMemo(() => {
    const query = (filters.keyword ?? "").toLowerCase();
    return vinPasteScopeVehicles.filter((vehicle) => (!filters.country || vehicle.countryCode === filters.country)
      && (!filters.carCode || vehicle.carCode === filters.carCode)
      && (!batchVins.length || Boolean(vehicle.vin && batchVins.includes(vehicle.vin.toUpperCase())))
      && (!query || matchesVehicleText(vehicle, query)))
      .sort((a, b) => compareProductModels(a.brand ?? "", a.modelName ?? "", a.powertrain ?? "", b.brand ?? "", b.modelName ?? "", b.powertrain ?? "")
        || (a.version ?? "").localeCompare(b.version ?? "") || (a.bom ?? "").localeCompare(b.bom ?? "")
        || (a.materialCode ?? "").localeCompare(b.materialCode ?? "") || a.carCode.localeCompare(b.carCode));
  }, [vinPasteScopeVehicles, filters.country, filters.keyword, filters.carCode, batchVins]);
  const pivotGroups = new Map<string, { count: number; vin: number; ready: number }>();
  for (const vehicle of vinPasteScopeVehicles) {
    const key = display(vehicle[pivotBy]);
    const group = pivotGroups.get(key) ?? { count: 0, vin: 0, ready: 0 };
    group.count += 1; group.vin += vehicle.vin ? 1 : 0;
    group.ready += vehicle.logisticsStatus === "ready_for_pickup" ? 1 : 0;
    pivotGroups.set(key, group);
  }

  const tableSummary = useMemo(() => {
    const vinMissing = vinPasteScopeVehicles.filter((item) => !item.vin).length;
    const ready = vinPasteScopeVehicles.filter((item) => item.logisticsStatus === "ready_for_pickup").length;
    const allocated = vinPasteScopeVehicles.filter((item) => item.allocationStatus === "allocated").length;
    return { vinMissing, vinAssigned: vinPasteScopeVehicles.length - vinMissing, ready, allocated };
  }, [vinPasteScopeVehicles]);

  useEffect(() => {
    if (!defaultCountry) {
      return;
    }
    setPiBrowseCountry((current) => current || defaultCountry);
  }, [defaultCountry]);

  useEffect(() => {
    if (!statusFlowCountry) {
      setStatusFlow(null);
      return;
    }
    let cancelled = false;
    api.getVehicleAllocationStatusFlow({
      country: statusFlowCountry,
      orderingAccountCode: statusFlowAccount || undefined,
    })
      .then((config) => {
        if (!cancelled) {
          setStatusFlow(config);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setStatusFlow(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [statusFlowAccount, statusFlowCountry]);

  useEffect(() => {
    let cancelled = false;
    setSideLoading(true);
    setPiListError(null);
    api.getVehicleAllocationPis({
      country: piBrowseCountry,
      month: piBrowseMonth,
      page: piBrowsePage,
      pageSize: 50,
    })
      .then((res) => {
        if (!cancelled) {
          setPiHeaders(res.items);
          setPiHeaderTotal(res.total);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setPiHeaders([]);
          setPiHeaderTotal(0);
          setPiListError(actionableError(err, "Could not load PI list. Check country/month and retry / 无法读取 PI 列表，请核对国家、月份并重试。"));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setSideLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [piBrowseCountry, piBrowseMonth, piBrowsePage, refreshKey]);

  useEffect(() => {
    let cancelled = false;
    setPiMonths(null);
    setPiMonthsError(null);
    api.getVehicleAllocationPiMonths(piBrowseYear, piBrowseCountry)
      .then((result) => { if (!cancelled) setPiMonths(result.items); })
      .catch((err: unknown) => {
        if (!cancelled) setPiMonthsError(actionableError(err, "Could not load months. Retry / 无法读取月份，请重试。"));
      });
    return () => { cancelled = true; };
  }, [piBrowseCountry, piBrowseYear, piMonthsRetry, refreshKey]);

  useEffect(() => {
    const piCode = new URLSearchParams(window.location.search).get("pi")?.trim().toUpperCase();
    if (piCode) {
      void selectPi(piCode);
    }
  }, []);

  useEffect(() => {
    if (!linesOpen) return;
    function closeOutside(event: PointerEvent): void {
      if (event.target instanceof Node && !linePanelRef.current?.contains(event.target)) setLinesOpen(false);
    }
    function closeOnEscape(event: KeyboardEvent): void {
      if (event.key === "Escape") setLinesOpen(false);
    }
    document.addEventListener("pointerdown", closeOutside);
    document.addEventListener("keydown", closeOnEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOutside);
      document.removeEventListener("keydown", closeOnEscape);
    };
  }, [linesOpen]);

  function clearScopeEdits(): void {
    gridView.current = { vehicles: [], ordinaryVehicles: [], columns: [] };
    setOrdinaryCodes(new Set());
    setBatchVins([]); setVinBatchText(""); setSearchTerm("");
    setSelectedCarCodes(new Set());
    setEditorTargets([]);
    setVinPasteText("");
    setVinPasteMessage("");
    setDeleteConfirmPi(null);
    setImportPreview(null);
    setImportError(null);
    setRemoveVins(false);
    setAllowReplacing(false);
  }

  const authorizationKey = JSON.stringify([user?.username, user?.role, user?.primaryCountry, user?.secondaryCountries, user?.brands]);
  useEffect(() => {
    piRequest.current += 1;
    clearScopeEdits();
    setSelectedPi(null);
    setSelectedLineCode("");
    setPiBrowseCountry(defaultCountry);
    setFilters((current) => ({ ...current, country: defaultCountry, piCode: undefined, piLineCode: undefined, page: 1 }));
  }, [authorizationKey]);

  function showImportError(reason: unknown): void {
    setImportError(actionableError(reason, "Could not complete VIN import. Check the selected PI and BOM/VIN headers, then preview again. / VIN 导入未完成，请核对所选 PI 和 BOM、VIN 表头后重新预览；若仍失败，请联系管理员检查服务日志。"));
  }

  async function previewVinFile(file: File): Promise<void> {
    if (!selectedPi || scopeBusy) return;
    setImportBusy(true); setImportPreview(null); setImportError(null);
    try {
      const preview = await api.previewVehicleAllocationImport(file, { piCode: selectedPi.header.piCode, allowReplacing, ...(removeVins ? { removeVins: true } : {}) });
      setImportPreview(preview);
    } catch (reason) { showImportError(reason); }
    finally { setImportBusy(false); }
  }

  async function repreviewVinTargets(replacing: boolean, sourceRow?: number, carCode?: string): Promise<void> {
    setAllowReplacing(replacing);
    if (!selectedPi || !importPreview || scopeBusy) return;
    const rows = importPreview.previewRows.map((row) => ({
      sourceRow: row.sourceRow, material_code: row.materialCode, vin: row.vin,
      old_vin: row.inputOldVin, car_code: row.sourceRow === sourceRow ? carCode : row.requestedCarCode,
    }));
    setImportBusy(true); setImportError(null); setImportPreview(null);
    try {
      setImportPreview(await api.previewVehicleAllocationParsedRows({ piCode: selectedPi.header.piCode, allowReplacing: replacing, rows, removeVins: importPreview.removeVins }));
    } catch (reason) { showImportError(reason); }
    finally { setImportBusy(false); }
  }

  async function applyVinFile(): Promise<void> {
    if (!importPreview || importPreview.status !== "ok" || !importPreview.updatedUnits || scopeBusy || !selectedPi) return;
    if ((importPreview.replacedUnits ?? 0) > 0 && !window.confirm(`Replace ${importPreview.replacedUnits} existing VINs as previewed? / 确认按预览替换 ${importPreview.replacedUnits} 个已录 VIN？`)) return;
    if (importPreview.removeVins && !window.confirm(`Clear ${importPreview.removedUnits} VINs only? Vehicles, quantities, prices and logistics remain. / 仅清除 ${importPreview.removedUnits} 个 VIN？车辆位、数量、价格及物流保持不变。`)) return;
    setImportBusy(true); setImportError(null);
    try {
      const result = await api.applyVehicleAllocationImport(importPreview.importId);
      setImportPreview(null);
      setNotice(importPreview.removeVins ? `VIN cleared: ${result.removedUnits} / 已清 VIN ${result.removedUnits}，车辆位保留` : `VIN saved: ${result.updatedUnits}; already imported: ${result.skippedUnits ?? 0} / VIN 已保存 ${result.updatedUnits}，已录跳过 ${result.skippedUnits ?? 0}`);
      setRefreshKey((value) => value + 1);
      try {
        setSelectedPi(await api.getVehicleAllocationPi(selectedPi.header.piCode));
      } catch {
        setError("VINs were saved, but the PI view could not refresh. Select this PI again; do not re-apply. / VIN 已保存，但 PI 页面刷新失败。请重新选择此 PI，无须重复应用。");
      }
    } catch (reason) {
      setImportPreview(null); showImportError(reason);
    } finally { setImportBusy(false); }
  }

  async function previewSelectedVinRemoval(): Promise<void> {
    if (!selectedPi || scopeBusy) return;
    const rows = selectedPi.vehicles.filter((v) => selectedCarCodes.has(v.carCode) && v.vin).map((v, index) => ({
      sourceRow: index + 1, material_code: v.materialCode, car_code: v.carCode, vin: v.vin,
    }));
    if (!rows.length) { setImportError("Select vehicles with VINs first / 请先勾选有 VIN 的车辆"); return; }
    setRemoveVins(true); setImportBusy(true); setImportError(null); setImportPreview(null);
    try {
      setImportPreview(await api.previewVehicleAllocationParsedRows({ piCode: selectedPi.header.piCode, removeVins: true, rows }));
    } catch (reason) { showImportError(reason); }
    finally { setImportBusy(false); }
  }

  function setPiVehicleScope(detail: PiOrderDetail, lineCode: string | null): void {
    setSelectedPi(detail);
    setSelectedLineCode(lineCode);
    setFilters((current) => ({
      ...current,
      country: "",
      piCode: detail.header.piCode,
      piLineCode: lineCode ?? undefined,
      carCode: undefined,
      keyword: undefined,
      vin: undefined,
      page: 1,
    }));
    clearScopeEdits();
    setLinesOpen(false);
  }

  function openPiTool(tab: PiToolTab): void {
    if (tab === "status" && !scopeBusy) setEditorTargets(vinPasteScopeVehicles.filter((vehicle) => selectedCarCodes.has(vehicle.carCode)));
    setActiveToolTab(tab);
    setToolDrawerOpen(true);
  }

  async function applyVinPaste(vins: string[]): Promise<void> {
    if (scopeBusy) return;
    if (!selectedPi) {
      setError("先选择 PI");
      return;
    }
    if (vins.length === 0) {
      return;
    }
    setVinPasteApplying(true);
    setVinPasteMessage("");
    setError(null);
    setNotice(null);
    try {
      const result = await api.bulkUpdateVehicleAllocationVehicles({
        piCode: selectedPi.header.piCode,
        piLineCode: selectedLineCode ?? undefined,
        carCodes: vinPasteScopeVehicles.map((vehicle) => vehicle.carCode),
        rowVersions: Object.fromEntries(vinPasteScopeVehicles.map((vehicle) => [vehicle.carCode, vehicle.rowVersion])),
        vinList: vins,
      });
      const detail = await api.getVehicleAllocationPi(selectedPi.header.piCode);
      setSelectedPi(detail);
      setEditorTargets([]);
      setRefreshKey((key) => key + 1);
      setVinPasteText("");
      const message = `Assigned ${result.vinAssigned}/${result.matchedUnits} VINs`;
      setVinPasteMessage(message);
      setNotice(message);
    } catch (err: unknown) {
      setError(actionableError(err, "Could not complete VIN paste. Re-read the PI before retrying / VIN 粘贴未完成，请先重新读取 PI 核对已有 VIN，再重试。"));
    } finally {
      setVinPasteApplying(false);
    }
  }

  async function runSearch(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (scopeBusy) return;
    const keyword = searchTerm.trim();
    if (!keyword) {
      return;
    }
    setError(null);
    setNotice(null);
    if (!/^(PI-|CAR-)/i.test(keyword) && !/^[A-HJ-NPR-Z0-9]{17}$/i.test(keyword)) {
      setBatchVins([]); clearSelection();
      setFilters((current) => ({ ...current, keyword, carCode: undefined, vin: undefined, page: 1 }));
      return;
    }
    const request = beginPiRead();
    try {
      const result = await api.searchVehicleAllocation(keyword);
      if (request !== piRequest.current) return;
      if (result.type === "pi" && isPiDetail(result.item)) {
        const detail = result.item;
        setPiVehicleScope(detail, null);
        setNotice(`Loaded ${detail.header.piCode}`);
        return;
      }
      if (result.type === "vehicle" && result.item && !isPiDetail(result.item)) {
        const vehicle = result.item;
        const detail = await api.getVehicleAllocationPi(vehicle.piCode);
        if (request !== piRequest.current) return;
        setPiVehicleScope(detail, vehicle.piLineCode);
        selectVehicle(vehicle);
        setNotice(`Loaded ${vehicle.carCode}`);
        return;
      }
      setBatchVins([]); clearSelection();
      setFilters((current) => ({ ...current, keyword, carCode: undefined, vin: undefined, page: 1 }));
    } catch (err: unknown) {
      if (request === piRequest.current) setError(actionableError(err, "Search failed. Check PI/CarCode/VIN and search again / 搜索未完成，请核对 PI、CarCode 或 VIN 后重试。"));
    } finally { if (request === piRequest.current) setPiLoading(false); }
  }

  function beginPiRead(retainPi = false): number {
    const request = ++piRequest.current;
    clearScopeEdits();
    if (!retainPi) { setSelectedPi(null); setSelectedLineCode(null); }
    setPiLoading(true);
    return request;
  }

  async function selectPi(piCode: string): Promise<void> {
    if (mutationBusy) return;
    setError(null);
    setNotice(null);
    const request = beginPiRead(selectedPi?.header.piCode === piCode);
    try {
      const detail = await api.getVehicleAllocationPi(piCode);
      if (request === piRequest.current) setPiVehicleScope(detail, null);
    } catch (err: unknown) {
      if (request === piRequest.current) {
        setSelectedPi(null);
        setError(actionableError(err, "Could not load this PI. Retry from the PI list / 无法读取此 PI，请从 PI 列表重新选择；仍失败请联系管理员。"));
      }
    } finally { if (request === piRequest.current) setPiLoading(false); }
  }

  async function searchCocLibrary(): Promise<void> {
    if (!selectedPi || cocBusy) return;
    const request = cocRequest.current;
    const piCode = selectedPi.header.piCode;
    setCocBusy(true); setCocError(""); setCocDownloadConfirm(null);
    try {
      const result = await api.piCocLookup(piCode);
      if (request === cocRequest.current) {
        setCocLookup({ piCode, result });
        setVisibleColumnKeys((old) => new Set([...old, "cocPdf"]));
      }
    } catch (reason) {
      if (request === cocRequest.current) setCocError(actionableError(reason, "Could not search COC library. Retry, or ask admin to check library configuration / 查库未完成，请重试或联系管理员检查在线库配置。"));
    } finally { setCocBusy(false); }
  }

  async function downloadCocs(codes: string[], confirmed = false): Promise<void> {
    if (!selectedPi || cocBusy || scopeBusy || !codes.length || !cocResult) return;
    const request = cocRequest.current;
    const piCode = selectedPi.header.piCode;
    const statuses = new Map(cocResult.items.map((item) => [item.carCode, item.status]));
    const available = codes.filter((code) => statuses.get(code) === "available");
    setCocError("");
    if (!available.length) {
      setCocError("No selected vehicles have a PDF. Search the library or record missing VINs first / 勾选车辆均无可下载 PDF，请重新查库或先补录 VIN。");
      return;
    }
    if (!confirmed && available.length !== codes.length) {
      const awaiting = codes.filter((code) => statuses.get(code) === "awaiting_vin").length;
      setCocDownloadConfirm({ piCode, request, codes: available, awaiting, missing: codes.length - available.length - awaiting });
      return;
    }
    setCocDownloadConfirm(null);
    setCocBusy(true); setCocError("");
    try {
      const confirmedVins: Record<string, string> = {};
      for (const item of cocResult.items) {
        if (available.includes(item.carCode) && item.vin) confirmedVins[item.carCode] = item.vin;
      }
      const blob = await api.piCocDownload(piCode, available, confirmedVins);
      if (request === cocRequest.current) buildDownload(blob, `${piCode}-COC.zip`);
    }
    catch (reason) { if (request === cocRequest.current) setCocError(actionableError(reason, "Could not download; search library again and select available rows / 下载未完成，请重新查库并勾选有 PDF 的车辆。")); }
    finally { setCocBusy(false); }
  }

  function selectLineScope(lineCode: string | null): void {
    if (!selectedPi) {
      return;
    }
    clearScopeEdits();
    setSelectedLineCode(lineCode);
    setFilters((current) => ({
      ...current,
      piCode: selectedPi.header.piCode,
      piLineCode: lineCode ?? undefined,
      carCode: undefined,
      keyword: undefined,
      vin: undefined,
      page: 1,
    }));
  }

  function clearSelection(): void { setSelectedCarCodes(new Set()); setEditorTargets([]); }
  function changeSelection(codes: Set<string>): void { setSelectedCarCodes(codes); setEditorTargets([]); }

  function changeGridView(view: VehicleGridView): void {
    gridView.current = view;
    const codes = new Set(view.ordinaryVehicles.map((vehicle) => vehicle.carCode));
    if (ordinaryCodes.size !== codes.size || [...ordinaryCodes].some((code) => !codes.has(code))) setOrdinaryCodes(codes);
  }

  function selectVehicle(vehicle: PiVehicleUnit): void {
    if (scopeBusy) return;
    setEditorTargets([vehicle]);
    setActiveToolTab("status");
    setToolDrawerOpen(true);
  }

  async function saveEditor(fields: UpdateVehiclePayload): Promise<void> {
    if (!canEdit || !selectedPi || !editorTargets.length || scopeBusy) return;
    setBulkSaving(true); setError(null);
    try {
      if (editorTargets.length === 1) {
        const vehicle = editorTargets[0];
        await api.updateVehicleAllocationVehicle(vehicle.carCode, { ...fields, rowVersion: vehicle.rowVersion });
      } else {
        await api.bulkUpdateVehicleAllocationVehicles({
          piCode: selectedPi.header.piCode,
          carCodes: editorTargets.map((vehicle) => vehicle.carCode),
          rowVersions: Object.fromEntries(editorTargets.map((vehicle) => [vehicle.carCode, vehicle.rowVersion])),
          fields,
        });
      }
      setNotice(`Updated ${editorTargets.length} vehicles / 车辆已更新`);
      setEditorTargets([]); clearSelection();
      try { setSelectedPi(await api.getVehicleAllocationPi(selectedPi.header.piCode)); }
      catch { setError("Saved, but refresh failed. Re-read this PI; do not save again / 已保存，但刷新失败，请重新读取 PI，无须重复保存。"); }
    } catch (reason) {
      setError(actionableError(reason, "Not saved. Re-read the PI before retrying / 未保存，请重新读取 PI 后再试。"));
    } finally { setBulkSaving(false); }
  }

  async function handleDeletePi(piCode: string): Promise<void> {
    if (scopeBusy) return;
    setSaving(true);
    setError(null);
    try {
      await api.deleteVehicleAllocationPi(piCode);
      setDeleteConfirmPi(null);
      setSelectedPi(null);
      setSelectedLineCode(null);
      clearScopeEdits();
      setFilters({ country: defaultCountry, page: 1, pageSize });
      setRefreshKey((key) => key + 1);
      setNotice("PI deleted; monthly demand is retained and the allocation is released. / PI 已删除，月需求保留，占用已释放，可回选品重新创建。");
    } catch { setError("PI could not be deleted. Select it again and retry / PI 删除未完成，请重新选择并重试。"); }
    finally { setSaving(false); }
  }

  function searchVinBatch(): void {
    const vins = [...new Set(vinBatchText.toUpperCase().split(/[\s,;]+/).filter(Boolean))];
    if (vins.length > 1000 || vins.some((vin) => !/^[A-HJ-NPR-Z0-9]{17}$/.test(vin))) {
      setError("Use up to 1,000 complete 17-character VINs / 请使用最多 1,000 个完整的 17 位 VIN。"); return;
    }
    setError(null); clearSelection(); setBatchVins(vins);
    setFilters({ country: filters.country, piCode: selectedPi?.header.piCode, piLineCode: selectedLineCode ?? undefined, page: 1, pageSize });
  }

  async function exportCurrentView(): Promise<void> {
    if (!selectedPi || scopeBusy || exporting) return;
    setExporting(true);
    setError(null);
    try {
      const view = gridView.current;
      if (!view.vehicles.length || !view.columns.length) { setError("Nothing visible to export / 当前无可导出内容"); return; }
      const blob = await api.exportVehicleAllocation({ country: filters.country, piCode: selectedPi?.header.piCode, piLineCode: selectedLineCode ?? undefined, columns: view.columns, carCodes: view.vehicles.map((vehicle) => vehicle.carCode) });
      const country = filters.country || "ALL";
      buildDownload(blob, `Vehicle_Allocation_${country}_${new Date().toISOString().slice(0, 10)}.xlsx`);
    } catch (err: unknown) {
      setError(actionableError(err, "Export failed. Check view filters and retry / 导出未完成，请核对查看筛选后重试。"));
    } finally {
      setExporting(false);
    }
  }

  return (
    <div className="vehicle-allocation-page" ref={pageRef}>
      <OrderingBrandNotice user={user} />
      <div className="va-header">
        <div>
          <div className="va-kicker">Order Genius</div>
          <h1>Vehicle Allocation</h1>
        </div>
        <form className="va-search" onSubmit={runSearch}>
          <input
            value={searchTerm}
            onChange={(event) => setSearchTerm(event.target.value)}
            placeholder="Search PI, Car Code, VIN, material, model, colour…"
            title="Exact PI/CarCode/VIN lookup, or filter details by keyword / 精确定位或按明细字段筛选"
          />
          <LoadingActionButton type="submit" disabled={scopeBusy} title="Load the matching PI or vehicle">
            Search
          </LoadingActionButton>
        </form>
      </div>

      {(error || notice) && (
        <div className={`va-message ${error ? "is-error" : "is-notice"}`}>
          {error || notice}
          {error ? <div className="va-button-row">
            <button type="button" disabled={scopeBusy} onClick={() => {
              setRefreshKey((key) => key + 1);
              if (selectedPi) void selectPi(selectedPi.header.piCode);
            }}>Re-read PI / 重新读取 PI</button>
            <a href="/product/order-genius">Check selection / 核对选品</a>
          </div> : null}
        </div>
      )}
      {cocError ? <div className="va-message is-error" role="alert">{cocError}
        <div className="va-button-row"><button type="button" disabled={cocBusy || scopeBusy} onClick={() => void searchCocLibrary()}>Retry / 重试</button>
          {isAdmin || user?.role === "editor" ? <a href="/product/coc-match">Open COC workbench / 打开 COC 工作台</a> : null}
        </div></div> : null}
      {cocDownloadConfirm ? <div className="va-message is-notice" role="alert">
        Download {cocDownloadConfirm.codes.length} available PDFs only? Missing PDF {cocDownloadConfirm.missing} · Awaiting VIN {cocDownloadConfirm.awaiting} / 仅下载 {cocDownloadConfirm.codes.length} 份可用 PDF？缺 PDF {cocDownloadConfirm.missing} · 待录 VIN {cocDownloadConfirm.awaiting}。不会把不完整下载标成整批。
        <div className="va-button-row"><button type="button" disabled={cocBusy || scopeBusy} onClick={() => {
          if (cocDownloadConfirm.request === cocRequest.current && cocDownloadConfirm.piCode === selectedPi?.header.piCode) void downloadCocs(cocDownloadConfirm.codes, true);
        }}>Confirm available only / 确认仅下载可用</button><button type="button" onClick={() => setCocDownloadConfirm(null)}>Cancel / 取消</button></div>
      </div> : null}

      <DeckFloatingDrawer
        open={toolDrawerOpen}
        onOpenChange={setToolDrawerOpen}
        triggerPrimary="PI Tools"
        triggerSecondaryOpen="Close tools"
        triggerSecondaryClosed={selectedPi ? `${selectedPi.summary.totalUnits ?? 0} vehicles` : "Open tools"}
        eyebrow="Order Genius"
        ariaLabel="PI vehicle allocation tools"
        closeLabel="Close"
        className="vehicle-allocation-tool-drawer"
        panelClassName="vehicle-allocation-tool-panel"
        title={activeToolTab === "status" && editorTargets.length ? editorTargets.length === 1 ? editorTargets[0].carCode : `Update ${editorTargets.length} selected vehicles` : "Vehicle allocation tools"}
        footer={activeToolTab === "status" ? <LoadingActionButton type="submit" form="vehicle-edit-form" loading={bulkSaving} disabled={!canEdit || scopeBusy || !editorTargets.length}>Save changes / 保存修改</LoadingActionButton> : undefined}
      >
        <DeckControlTabs
          tabs={PI_TOOL_TABS}
          activeKey={activeToolTab}
          onChange={openPiTool}
          ariaLabel="PI vehicle allocation tools"
        />
        <div className="va-tool-tab-body" ref={toolBodyRef}>
          {activeToolTab === "import" ? (
            <>
              <section className="va-tool-card">
                <strong>COC online library / 共享在线库</strong>
                <p>Search whole PI by VIN; no changes to VINs or orders / 按 VIN 查整批 PI，不修改 VIN 或订单。</p>
                <button type="button" disabled={!selectedPi || cocBusy || scopeBusy} onClick={() => void searchCocLibrary()}>{cocBusy ? "Working / 处理中…" : "Search library / 在库里查找"}</button>
                <button type="button" disabled={!cocResult || cocBusy || !selectedCarCodes.size} onClick={() => void downloadCocs([...selectedCarCodes])}>Download selected COCs / 下载勾选 COC</button>
                {cocResult ? <>
                  <p role="status">COC PDF {cocResult.available}/{cocResult.total} · Awaiting VIN / 待录 VIN {cocResult.awaitingVin} · Missing PDF / 缺 PDF {cocResult.missing}</p>
                  <label>COC results / 查库结果 <select value={cocFilter} onChange={(event) => { const value = event.target.value; if (value === "all" || value === "available" || value === "missing" || value === "awaiting_vin") setCocFilter(value); }}>
                    <option value="all">All / 全部</option><option value="available">PDF available / 有 PDF</option><option value="missing">Missing PDF / 缺 PDF</option><option value="awaiting_vin">Awaiting VIN / 待录 VIN</option>
                  </select></label>
                  <div style={{ maxHeight: 220, overflow: "auto" }}>
                    {cocResult.items.filter((item) => cocFilter === "all" || item.status === cocFilter).map((item) => <div key={item.carCode}>
                      <button type="button" onClick={() => { clearSelection(); setFilters({ country: filters.country, piCode: selectedPi?.header.piCode, carCode: item.carCode, page: 1, pageSize }); setToolDrawerOpen(false); }}>{item.carCode}</button> · {item.vin || "Awaiting VIN / 待录 VIN"} · {item.status === "available" ? "PDF available / 有 PDF" : item.status === "missing" ? "Missing PDF / 缺 PDF" : "—"}
                    </div>)}
                  </div>
                </> : null}
              </section>
              <section className="va-tool-card">
                <strong>BOM + VIN file import / 物料号与 VIN 文件导入</strong>
                <p>{selectedPi ? `Whole PI: ${selectedPi.header.piCode} / 匹配整批 PI，不受所选明细或车辆页码限制。` : "Select a PI first / 请先选择 PI"}</p>
                <p>BOM / Material Code + VIN; optional Car Code / Old VIN for correction. / 必填物料号和 VIN，纠错可加 Car Code 或 Old VIN。</p>
                <label className="va-check"><input type="checkbox" checked={removeVins} disabled={scopeBusy}
                  onChange={(event) => { setRemoveVins(event.target.checked); setImportPreview(null); }} />Remove these VINs from file / 按原文件清 VIN（先预览）</label>
                <button type="button" disabled={scopeBusy || !selectedCarCodes.size} onClick={() => void previewSelectedVinRemoval()}>Clear selected VINs / 清勾选 VIN（先预览）</button>
                {!removeVins ? <label className="va-check"><input type="checkbox" checked={allowReplacing} disabled={scopeBusy}
                  onChange={(event) => void repreviewVinTargets(event.target.checked)} />Allow replacing existing VINs / 允许替换已录 VIN</label> : null}
                <input ref={importInputRef} type="file" accept=".xlsx" hidden aria-label="BOM and VIN XLSX"
                  onChange={(event) => { const file = event.target.files?.[0]; event.target.value = ""; if (file) void previewVinFile(file); }} />
                {importError ? <div role="alert" className="va-import-error">{importError}<br />
                  <button type="button" onClick={() => importInputRef.current?.click()} disabled={scopeBusy}>Re-upload / 重新上传</button>
                  <a href="/product/order-genius">Check order selection / 核对选品订单</a>
                </div> : null}
                <VehicleImportDigestPanel preview={importPreview} busy={scopeBusy}
                  onPickFile={() => { if (selectedPi) importInputRef.current?.click(); else setImportError("Select a PI before uploading / 请先选择目标 PI 再上传"); }}
                  onApply={applyVinFile} onClear={() => { setImportPreview(null); setImportError(null); }}
                  onTargetChange={(row, car) => void repreviewVinTargets(allowReplacing, row, car)} />
              </section>
              <details>
                <summary>VIN paste · auxiliary / 辅助粘贴</summary>
                <VinPasteDigestPanel
                  scopeLabel={activeScopeLabel}
                  vehicles={vinPasteScopeVehicles}
                  pasteText={vinPasteText}
                  applying={scopeBusy}
                  applyMessage={vinPasteMessage}
                  onPasteTextChange={setVinPasteText}
                  onApply={(vins) => void applyVinPaste(vins)}
                />
              </details>
            </>
          ) : null}
          {activeToolTab === "status" ? <>
            <details className="va-status-details"><summary>Status details / 状态详情</summary>
              <VehicleStatusBoard scopeLabel={activeScopeLabel} vehicles={vinPasteScopeVehicles} statusFlow={statusFlow} />
            </details>
            {error ? <p role="alert">{error}<button type="button" disabled={scopeBusy} onClick={() => selectedPi && void selectPi(selectedPi.header.piCode)}>Re-read PI</button></p> : null}
            {editorTargets.length ? <VehicleAllocationEditor key={editorTargets.map((vehicle) => vehicle.carCode).join("|")} vehicles={editorTargets} readOnly={!canEdit} busy={scopeBusy} allocationOptions={allocationStatusOptions} logisticsOptions={logisticsStatusOptions} onSave={saveEditor} /> : <p>Select vehicle rows, then choose Update selected status. / 请明确勾选车辆后更新；不会自动修改整批 PI。</p>}
          </> : null}
          {activeToolTab === "view" ? (
            <>
              <p>Drag headers to move columns; drag header edges to resize. Filter and sort from each header. / 拖动表头移列，拖动边界调宽，在表头筛选及排序。</p>
              <section className="va-tool-card">
                <label htmlFor="vin-batch-search">VIN batch search / VIN 批量搜索</label>
                <textarea id="vin-batch-search" value={vinBatchText} onChange={(event) => setVinBatchText(event.target.value)} placeholder="One complete VIN per line / 每行一个完整 VIN" />
                <label>Selection scope / 勾选范围<select aria-label="VIN selection scope" value={vinSelectionScope} disabled={scopeBusy} onChange={(event) => setVinSelectionScope(event.target.value === "filtered" ? "filtered" : "global")}>
                  <option value="global">Global matches / 全局匹配</option><option value="filtered">Filtered matches / 当前筛选匹配</option>
                </select></label>
                <div className="va-button-row">
                  <button type="button" disabled={!selectedPi || scopeBusy} onClick={searchVinBatch}>Search VIN batch / 搜索 VIN</button>
                  <button type="button" disabled={!(vinSelectionScope === "global" ? batchMatches : filteredMatches).length || scopeBusy} onClick={() => {
                    const eligible = vinSelectionScope === "global" ? batchMatches : filteredMatches;
                    changeSelection(new Set(eligible.map((vehicle) => vehicle.carCode)));
                    setNotice(`${batchMatches.length} global matches · ${filteredMatches.length} filtered matches · ${eligible.length} selected / 全局匹配、当前筛选匹配、已选`);
                  }}>Select matches / 勾选匹配</button>
                  <button type="button" className="btn-secondary" onClick={() => { setBatchVins([]); setVinBatchText(""); clearSelection(); }}>Clear batch search / 清除批量搜索</button>
                </div>
                {batchVins.length ? <p role="status">{batchMatches.length} global matches / 全局匹配 · {filteredMatches.length} filtered matches / 当前匹配 · {missingVins.length} not found in this PI/line / 本范围未找到{missingVins.length ? `: ${missingVins.join(", ")}` : ""}</p> : null}
                <p>Current filtered selected / 当前筛选已选 {currentSelectedCount}/{ordinaryCodes.size} · Global selected / 全局已选 {selectedCarCodes.size}/{vinPasteScopeVehicles.length} · Outside current filters / 筛选外已选 {selectedCarCodes.size - currentSelectedCount}</p>
              </section>

              <fieldset className="va-columns"><legend>Columns / 显示列</legend>
                {COLUMN_GROUPS.map((group) => <details key={group.label} open><summary>{group.label}</summary>
                  <div className="va-column-options">{VEHICLE_COLUMNS.filter((column) => group.keys.includes(column.key)).map((column) => <label key={column.key}>
                    <input type="checkbox" checked={visibleColumnKeys.has(column.key)} onChange={() => setVisibleColumnKeys((current) => { const next = new Set(current); if (next.has(column.key)) next.delete(column.key); else next.add(column.key); return next; })} />{column.label}
                  </label>)}</div>
                </details>)}
                <button type="button" className="btn-secondary" onClick={() => {
                  clearSelection(); setBatchVins([]); setVinBatchText(""); setSearchTerm("");
                  setFilters((current) => ({ country: current.country, piCode: current.piCode }));
                  setVisibleColumnKeys(new Set(DEFAULT_COLUMNS)); setGridResetKey((value) => value + 1);
                }}>Reset columns & filters / 重置列与筛选</button>
              </fieldset>
              <LoadingActionButton disabled={!selectedPi || scopeBusy} loading={exporting} loadingLabel="Exporting..." onClick={() => void exportCurrentView()} variant="secondary">Export current view / 导出所见</LoadingActionButton>
              <button type="button" className="btn-secondary" disabled={!selectedPi || scopeBusy}
                onClick={() => setShowInvoiceExport(true)}>Export PI</button>
              <details className="va-pivot"><summary>PI pivot summary / 整批透视摘要</summary>
                <p>Whole PI/line, not the current page or view filters / 当前 PI 或明细范围，不是分页或筛选后数量</p>
                <select aria-label="Pivot by" value={pivotBy} onChange={(event) => {
                  const value = event.target.value;
                  if (value === "modelName" || value === "materialCode" || value === "logisticsStatus" || value === "countryCode") setPivotBy(value);
                }}><option value="modelName">Model / 车型</option><option value="materialCode">Material / 物料</option><option value="logisticsStatus">Logistics / 物流</option><option value="countryCode">Country / 国家</option></select>
                <table><thead><tr><th>Group / 分组</th><th>Units / 台</th><th>VIN</th><th>Ready for pickup / 可提车</th></tr></thead><tbody>{[...pivotGroups].map(([key, group]) => <tr key={key}><td>{key}</td><td>{group.count}</td><td>{group.vin}</td><td>{group.ready}</td></tr>)}</tbody></table>
              </details>
            </>
          ) : null}
        </div>
      </DeckFloatingDrawer>

      {showInvoiceExport && selectedPi ? <PiInvoiceExportDialog key={selectedPi.header.piCode}
        piCode={selectedPi.header.piCode} onClose={() => setShowInvoiceExport(false)} /> : null}

      <div className="va-layout">
        <aside className="va-side">
          <section className="va-panel va-browse-panel">
            <div className="va-panel-head">
              <h2>PI</h2>
              <span>{sideLoading ? "Loading" : `${piHeaderTotal}`}</span>
            </div>
            <div className="va-form" aria-label="Browse existing PI batches">
              <div className="va-form-row">
                <label>Browse Country</label>
                <CommandSelect
                  value={normalizeCountryCode(piBrowseCountry)}
                  options={countryCommandOptions}
                  placeholder="All accessible"
                  searchPlaceholder="Search country..."
                  onChange={(value) => {
                    setPiBrowseCountry(value);
                    setPiBrowsePage(1);
                  }}
                />
              </div>
              <div className="va-form-row">
                <label>Browse Month</label>
                <details className="va-month-browser" open>
                  <summary>{piBrowseMonth || "All months"}</summary>
                  <div className="va-month-controls">
                    <input type="number" aria-label="Browse Year" min={1} max={9999} value={piBrowseYear}
                      onChange={(event) => {
                        const year = Number(event.target.value);
                        if (Number.isInteger(year) && year >= 1 && year <= 9999) setPiBrowseYear(year);
                      }} />
                    <button type="button" className="btn btn-sm btn-ghost" aria-pressed={!piBrowseMonth}
                      onClick={() => { setPiBrowseMonth(""); setPiBrowsePage(1); }}>All months</button>
                  </div>
                  {piMonthsError ? (
                    <div className="alert alert-error" role="alert">{piMonthsError}
                      <button type="button" className="btn btn-sm btn-ghost"
                        onClick={() => setPiMonthsRetry((value) => value + 1)}>Retry months</button>
                    </div>
                  ) : piMonths === null ? <p role="status">Loading months…</p> : null}
                  <div className="va-month-grid">
                    {MONTH_NAMES.map((name, index) => {
                      const month = `${String(piBrowseYear).padStart(4, "0")}-${String(index + 1).padStart(2, "0")}`;
                      const counts = piMonths?.find((item) => item.month === month);
                      const piCount = counts?.piCount ?? 0;
                      return <button key={month} type="button" disabled={piMonths === null}
                        className={piCount ? "has-pis" : "is-empty"} aria-pressed={piBrowseMonth === month}
                        aria-label={`${name} ${piBrowseYear}`}
                        title={piMonths === null ? "Counts unavailable" : `${piCount} PIs · ${counts?.vehicleCount ?? 0} vehicles`}
                        onClick={() => { setPiBrowseMonth(month); setPiBrowsePage(1); }}>
                        {name}{piCount > 0 ? <small>{piCount}</small> : null}
                      </button>;
                    })}
                  </div>
                  <small>PI order month · vehicle counts in browse country</small>
                </details>
              </div>
              {piListError ? (
                <div className="alert alert-error">
                  {piListError}
                  <button type="button" className="btn btn-sm btn-ghost" onClick={() => setRefreshKey((key) => key + 1)}>
                    Retry
                  </button>
                </div>
              ) : null}
            </div>
            <a className="va-selection-link" href="/product/order-genius">Create PI in Order Genius</a>
            <div className="va-pi-list">
              {piHeaders.map((pi) => (
                <button
                  type="button"
                  key={pi.piCode}
                  disabled={mutationBusy}
                  className={selectedPi?.header.piCode === pi.piCode ? "is-active" : ""}
                  onClick={() => void selectPi(pi.piCode)}
                  title={`Account ${display(pi.orderingAccountCode)} · Markets ${marketCountriesText(pi)}`}
                >
                  <span>{pi.piCode}</span>
                  <small>{display(pi.officialPiNo)} · {display(pi.orderingAccountCode)} · {marketCountriesText(pi)} · {statusText(pi.status)}</small>
                </button>
              ))}
            </div>
            {piHeaderTotal > 50 ? (
              <div className="va-button-row" aria-label="PI list pagination">
                <button
                  type="button"
                  className="btn btn-sm btn-ghost"
                  disabled={piBrowsePage <= 1 || sideLoading}
                  onClick={() => setPiBrowsePage((pageNumber) => Math.max(1, pageNumber - 1))}
                >
                  Previous
                </button>
                <span>{piBrowsePage} / {Math.max(1, Math.ceil(piHeaderTotal / 50))}</span>
                <button
                  type="button"
                  className="btn btn-sm btn-ghost"
                  disabled={piBrowsePage * 50 >= piHeaderTotal || sideLoading}
                  onClick={() => setPiBrowsePage((pageNumber) => pageNumber + 1)}
                >
                  Next
                </button>
              </div>
            ) : null}
          </section>

          {selectedPi ? (
            <div className="va-panel va-lines-panel" ref={linePanelRef}>
              <button type="button" className="btn-secondary" aria-expanded={linesOpen} aria-controls="pi-line-list"
                onClick={() => setLinesOpen((current) => !current)}>
                PI lines · {selectedPi.lines.length} / 明细
              </button>
              <small className="va-scope-label">{selectedLine ? display(selectedLine.materialCode) : "All PI / 整批"}</small>
              {linesOpen ? (
                <div className="va-line-list" id="pi-line-list" role="region" aria-label="PI lines">
                  <div className={`va-line-row va-line-all ${selectedLineCode === null ? "is-active" : ""}`}>
                    <button type="button" disabled={scopeBusy} className="va-line-body" onClick={() => selectLineScope(null)}
                      title="Use the whole PI as the vehicle table and batch-edit scope">
                      <strong>All PI</strong>
                      <span>{selectedPi.header.piCode} · Qty {selectedPi.vehicleTotal}</span>
                      <small>{selectedPi.vehicles.filter((vehicle) => !vehicle.vin).length} no VIN</small>
                    </button>
                  </div>
                  {selectedPi.lines.slice().sort((a, b) => compareProductModels(a.brand ?? "", a.modelName ?? "", a.powertrain ?? "", b.brand ?? "", b.modelName ?? "", b.powertrain ?? "")
                    || (a.version ?? "").localeCompare(b.version ?? "") || (a.bom ?? "").localeCompare(b.bom ?? "")
                    || (a.materialCode ?? "").localeCompare(b.materialCode ?? "") || a.piLineCode.localeCompare(b.piLineCode)).map((line) => (
                    <div
                      key={line.piLineCode}
                      className={`va-line-row ${selectedLineCode === line.piLineCode ? "is-active" : ""}`}
                    >
                      <button type="button" disabled={scopeBusy} className="va-line-body" onClick={() => selectLineScope(line.piLineCode)}
                        title="Line quantity and market split generated from order matrix allocation">
                        <strong>{line.piLineCode}</strong>
                        <span className="va-line-config" style={{ color: ptColor(line.powertrain ?? "", "#475467") }}>{display(line.materialCode)} · {display(line.modelName)} / {display(line.version)} · Qty {line.quantity}</span>
                        <small>
                          {(line.allocations ?? []).length > 0
                            ? (line.allocations ?? []).map((allocation) => `${allocation.marketCountryCode} ${allocation.quantity}`).join(" · ")
                            : `Market ${selectedPi.header.countryCode} ${line.quantity}`}
                        </small>
                      </button>
                    </div>
                  ))}
                </div>
              ) : null}

            </div>
          ) : null}
        </aside>
        <main className="va-main">
          {selectedPi && (
            <section className="va-pi-detail">
              <div className="va-pi-title">
                <div>
                  <h2>{selectedPi.header.piCode}</h2>
                  <span title="PI account, market countries, port, ship, and ETA">
                    {display(selectedPi.header.officialPiNo)}
                    {" · Account "}{display(selectedPi.header.orderingAccountCode)}
                    {" · Markets "}{marketCountriesText(selectedPi.header)}
                    {" · Port "}{display(selectedPi.header.portOfDischarge)}
                    {" · Ship "}{display(selectedPi.header.shipName)}
                    {" · ETA "}{display(selectedPi.header.eta)}
                  </span>
                </div>
                <div className="va-pi-metrics">
                  <span>{vinPasteScopeVehicles.length} units / 台</span>
                  <button type="button" onClick={() => openPiTool("import")}>{cocResult ? `COC PDF ${cocResult.available}/${cocResult.total} · 待录 VIN ${cocResult.awaitingVin} · 缺 PDF ${cocResult.missing}` : "COC library / 在线库"}</button>
                  <span>{tableSummary.vinMissing} no VIN / 待录</span>
                  <span>{tableSummary.vinAssigned} VIN assigned / 已录</span>
                  <button type="button" className="btn-secondary" onClick={() => openPiTool("status")}>
                    Status / 状态 · {tableSummary.ready} ready for pickup · {tableSummary.allocated} allocated
                  </button>
                  {(isAdmin || selectedPi.canDelete) && (deleteConfirmPi === selectedPi.header.piCode ? (
                    <span style={{ display: "flex", flexWrap: "wrap", maxWidth: "100%", gap: 4, alignItems: "center" }}>
                      <span style={{ fontSize: 12, color: "#dc2626", fontWeight: 600 }}>Delete {selectedPi.lines.reduce((sum, line) => sum + line.quantity, 0)} units + VINs; release PI allocation, retain monthly demand? / 删除整批及 VIN、释放占用，保留月需求？</span>
                      <button type="button" className="btn btn-sm btn-primary" style={{ background: "#dc2626", padding: "2px 10px", fontSize: 11 }}
                        disabled={scopeBusy} onClick={() => handleDeletePi(selectedPi.header.piCode)}>Yes</button>
                      <button type="button" className="btn btn-sm btn-ghost" style={{ padding: "2px 10px", fontSize: 11 }}
                        onClick={() => setDeleteConfirmPi(null)}>No</button>
                    </span>
                  ) : (
                    <button type="button" className="btn btn-sm btn-ghost"
                      onClick={() => setDeleteConfirmPi(selectedPi.header.piCode)}
                      style={{ color: "#dc2626", fontSize: 11 }}
                      title="Delete this PI and all its lines, allocations, and vehicles">
                      Delete PI
                    </button>
                  ))}
                </div>
              </div>
            </section>
          )}

          {selectedCarCodes.size > 0 ? (
            <div className="va-selected-actions">
              <span>{selectedCarCodes.size} selected / 已勾选 · {selectedCarCodes.size - currentSelectedCount} outside current filters / 筛选外已选</span>
              <button type="button" onClick={() => openPiTool("status")}>Update selected status / 更新勾选状态</button>
              <button type="button" className="btn-secondary" onClick={clearSelection}>Clear selection / 清除勾选</button>
            </div>
          ) : null}

          {piLoading ? <p role="status">Loading PI / 正在读取 PI…</p> : null}
          <VehicleAllocationGrid
            key={`${selectedPi?.header.piCode ?? ""}:${selectedLineCode ?? ""}:${filters.country ?? ""}`}
            filterScope={`${filters.keyword ?? ""}:${filters.carCode ?? ""}:${batchVins.join(",")}`}
            vehicles={vinPasteScopeVehicles} ordinaryVehicles={gridVehicles} columns={VEHICLE_COLUMNS} groups={COLUMN_GROUPS}
            cocStatuses={cocStatuses}
            visibleKeys={visibleColumnKeys} selectedCodes={selectedCarCodes} busy={scopeBusy}
            resetKey={gridResetKey} onEdit={selectVehicle} onSelection={changeSelection}
            onFilterChange={clearSelection} onViewChange={changeGridView}
            renderCell={(vehicle, key) => {
              if (key === "exteriorColorName") {
                const swatch = parseOrderGeniusColourSwatch(vehicle.colourHex);
                return <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
                  <span title={vehicle.colourHex || "Missing / Review HEX · 缺失或待确认色卡"} style={{ width: 14, height: 14, flexShrink: 0, border: "1px solid #cbd5e1", borderRadius: 3, background: swatch.background }} />
                  {vehicleCell(vehicle, key)}
                </span>;
              }
              if (key !== "cocPdf") return vehicleCell(vehicle, key);
              const status = cocResult?.items.find((item) => item.carCode === vehicle.carCode)?.status;
              return status === "available" ? <button type="button" disabled={cocBusy} onClick={() => void downloadCocs([vehicle.carCode])}>PDF ↓</button> : status === "missing" ? "Missing / 缺 PDF" : status === "awaiting_vin" ? "Awaiting VIN / 待录" : "Search library / 查库";
            }}
          />
        </main>
      </div>

      <style>{`
        .vehicle-allocation-page{width:100%;height:max(640px,var(--va-available-height,calc(100dvh - 128px)));padding:clamp(16px,1.2vw,32px);color:#111827;display:flex;flex-direction:column;min-width:0}
        .vehicle-allocation-page > .va-header,.vehicle-allocation-page > .va-message{flex:none}
        .va-header{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;margin-bottom:16px;padding-right:280px}
        .va-kicker{font-size:11px;font-weight:700;text-transform:uppercase;color:#667085}
        .va-header h1{font-size:28px;font-weight:600;line-height:1.1;margin:4px 0 0}
        .va-search{display:flex;gap:8px;min-width:340px}
        .va-search input,.va-filters input,.va-form input,.va-bulk-panel textarea,.va-bulk-fields input,.va-editor input,.va-editor textarea{border:1px solid #cfd6df;background:#fff;color:#111827;border-radius:6px;padding:9px 10px;min-width:0}
        .va-search input{flex:1}
        .vehicle-allocation-page button{border:1px solid #1c69d4;background:#1c69d4;color:white;border-radius:6px;padding:9px 12px;cursor:pointer;font-weight:600}
        .vehicle-allocation-page .btn-secondary,.vehicle-allocation-page .btn-ghost{background:#fff;color:#344054;border-color:#cfd6df}
        .vehicle-allocation-page .btn-secondary:hover,.vehicle-allocation-page .btn-ghost:hover{background:#f8fafc;border-color:#b8c2ce}
        .vehicle-allocation-page .btn-danger{background:#dc2626;color:#fff;border-color:#dc2626}
        .vehicle-allocation-page button:disabled{background:#a8b3c1;border-color:#a8b3c1;cursor:not-allowed}
        .va-message{padding:10px 12px;border-radius:6px;margin-bottom:14px;max-height:18vh;overflow:auto;overflow-wrap:anywhere}
        .va-message.is-error{background:#fff1f0;color:#a8071a;border:1px solid #ffa39e}
        .va-message.is-notice{background:#f0f7ff;color:#174ea6;border:1px solid #b7d6ff}
        .va-layout{display:grid;grid-template-columns:clamp(280px,20vw,360px) minmax(0,1fr);gap:16px;align-items:stretch;flex:1;min-height:360px}
        .va-side,.va-main{display:flex;flex-direction:column;gap:16px;min-width:0;min-height:0}
        .va-browse-panel{display:flex;flex-direction:column;flex:1;min-height:0;overflow:auto}
        .va-browse-panel > :not(.va-pi-list){flex:none}
        .va-lines-panel{display:flex;flex-direction:column;flex:none;max-height:40%;min-height:0;overflow:auto}
        .va-lines-panel > button,.va-lines-panel > small{flex:none;align-self:flex-start}
        .va-main > :not(.va-grid){flex:none}
        .va-panel,.va-filters,.va-pi-detail,.va-table-wrap{background:#fff;border:1px solid #d8dee6;border-radius:8px}
        .va-panel{padding:14px}
        .va-panel-head,.va-table-head,.va-pi-title,.va-drawer-head{display:flex;align-items:center;justify-content:space-between;gap:12px}
        .va-pi-title{flex-wrap:wrap}
        .va-pi-title{display:grid;grid-template-columns:minmax(0,1fr)}
        .va-pi-title > div{min-width:0;overflow-wrap:anywhere}
        .va-panel-head h2,.va-pi-title h2,.va-drawer h2{font-size:15px;margin:0}
        .va-panel-head span,.va-table-head span,.va-pi-title span,.va-drawer-head span{color:#667085;font-size:12px}
        .va-form{display:grid;gap:10px;margin-top:12px}
        .va-form-row{display:grid;gap:4px}
        .va-form-row label{font-size:12px;font-weight:700;color:#475467}
        .va-month-browser{border:1px solid #cfd6df;border-radius:6px;padding:9px 10px;min-width:0}
        .va-month-browser summary{cursor:pointer;font-weight:600}
        .va-month-controls{display:flex;gap:8px;margin:10px 0;align-items:center}
        .va-month-controls input{width:90px}
        .va-month-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;margin-bottom:8px}
        .va-month-grid button{display:flex;align-items:center;justify-content:center;gap:4px;border:1px solid transparent;border-radius:5px;padding:8px 2px;background:#f8fafc;color:#334155;cursor:pointer;min-width:0}
        .va-month-grid button.is-empty{color:#64748b;background:transparent}
        .va-month-grid button[aria-pressed="true"]{border-color:#2563eb;background:#eff6ff;color:#1d4ed8}
        .va-month-grid button:disabled{color:#94a3b8;cursor:wait}
        .va-month-grid small{font-size:10px;background:#e2e8f0;border-radius:8px;padding:1px 4px}
        .va-pi-list{display:grid;align-content:start;grid-auto-rows:max-content;gap:8px;margin-top:12px;flex:1;min-height:80px;overflow:auto}
        .va-pi-list button{background:#fff;color:#111827;border-color:#d8dee6;text-align:left;display:grid;gap:2px}
        .va-pi-list button.is-active{border-color:#1c69d4;background:#eef5ff}
        .va-pi-list small{color:#667085}
        .va-button-row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px}
        .va-filters{display:grid;grid-template-columns:repeat(2,minmax(112px,1fr));gap:10px;padding:12px}
        .va-check{display:flex;align-items:center;gap:6px;min-height:38px;font-size:12px;color:#475467}
        .va-pi-detail{padding:14px}
        .va-pi-metrics{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-start;align-items:center;padding-top:10px;border-top:1px solid #e5eaf0}
        .va-product-text{font-weight:600;filter:brightness(.72)}
        .va-grid{background:white;border:1px solid #d8dee6;border-radius:8px;overflow:hidden;display:flex;flex-direction:column;flex:1;min-height:0;min-width:0}
        .va-grid-toolbar{display:flex;align-items:center;gap:8px 12px;flex-wrap:wrap;padding:10px;flex:none}
        .va-grid-toolbar > span{flex:1 1 100%;min-width:0;overflow-wrap:anywhere}
        .va-grid-body{flex:1;min-height:0;min-width:0}
        .va-grid-toolbar label{display:flex;align-items:center;gap:6px}
        .va-editor fieldset{border:1px solid #d8dee6;border-radius:8px;display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:12px}
        .va-editor label{display:grid;gap:6px;font-size:12px}
        .va-editor textarea{min-height:80px}
        .va-edit-help,.va-edit-actions{font-size:11px;color:#667085}
        .va-edit-actions{display:flex;gap:6px;flex-wrap:wrap}
        .va-edit-actions button{padding:2px 5px}
        .va-column-options{display:grid;grid-template-columns:1fr 1fr;gap:6px;padding:8px 0}
        .va-grid .ag-root button{padding:0;border:0;border-radius:0;background:transparent;color:inherit}
        .va-grid .ag-root button:disabled{background:transparent;border:0}
        .va-grid .ag-header{z-index:2}

        .va-column-choice{display:flex;gap:4px;align-items:center;margin:5px 0}
        .va-column-choice label{flex:1}
        .va-column-choice button{padding:3px 7px}
        .va-columns details{width:100%}
        .va-tool-card textarea{width:100%;min-height:90px;resize:vertical}
        .va-pivot table{width:100%;font-size:12px;border-collapse:collapse}
        .va-pivot th,.va-pivot td{text-align:left;padding:6px;border-bottom:1px solid #e5eaf0}
        .va-status-counts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
        .va-status-counts h4{margin:5px 0;font-size:13px}
        .va-status-counts section > div{display:grid;grid-template-columns:1fr auto;gap:4px;font-size:12px;margin:8px 0}
        .va-status-counts .is-empty{opacity:.5}
        .va-count-track{grid-column:1 / -1;height:4px;background:#edf1f6;border-radius:4px;overflow:hidden}
        .va-count-track span{display:block;height:100%}
        .va-pi-metrics span{border:1px solid #d8dee6;border-radius:999px;padding:4px 8px;background:#f8fafc}
        .va-bulk-panel{display:grid;gap:10px;margin-top:12px;border:1px solid #d8dee6;border-radius:8px;background:#fbfcfe;padding:12px}
        .va-bulk-head{display:flex;align-items:center;justify-content:space-between;gap:12px}
        .va-bulk-head h3{font-size:14px;margin:0}
        .va-bulk-head span{font-size:12px;color:#667085}
        .va-bulk-panel textarea{min-height:90px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
        .va-bulk-fields{display:grid;grid-template-columns:repeat(2,minmax(112px,1fr));gap:8px}
        .va-bulk-fields label,.va-bulk-fields .va-field{display:grid;gap:5px;font-size:12px;font-weight:700;color:#475467}
        .va-line-list{display:grid;align-content:start;gap:6px;margin-top:12px;flex:1;min-height:0;overflow:auto}
        .va-line-row{display:flex;align-items:stretch;border:1px solid #e5eaf0;border-radius:6px;background:#fbfcfe;color:#111827;overflow:hidden}
        .va-line-row.is-active{border-color:#1c69d4;background:#eef5ff}
        .va-line-all{background:#fff}
        .vehicle-allocation-page .va-line-body{display:grid;grid-template-columns:1fr;gap:8px;align-items:center;flex:1;padding:8px;border:none;background:#f8fafc;cursor:pointer;text-align:left;color:#344054;font:inherit}
        .va-line-body strong{font-size:12px}
        .va-line-body span,.va-line-body small{font-size:12px;color:#667085;overflow-wrap:anywhere;white-space:normal}
        .va-line-config{font-weight:600;filter:brightness(.72)}
        .va-empty{text-align:center;color:#667085;padding:28px!important}
        .va-status{display:inline-flex;align-items:center;border-radius:999px;padding:3px 8px;background:#eef2f6;color:#344054;font-size:12px}
        .va-status-allocated,.va-status-delivered,.va-status-ready_for_pickup{background:#e8f7ee;color:#16794a}
        .va-status-reserved,.va-status-on_vessel,.va-status-in_production{background:#fff7e6;color:#ad6800}
        .va-status-unallocated,.va-status-pending{background:#eef2f6;color:#475467}
        .va-status-cancelled{background:#fff1f0;color:#a8071a}
        .va-wide{grid-column:1/-1}
        .va-columns{display:grid;grid-template-columns:minmax(0,1fr);gap:10px;border:1px solid #d8dee6;border-radius:8px;padding:12px}
        .va-columns label{display:flex;align-items:center;gap:6px;font-size:12px}
        .va-columns button{grid-column:1/-1}
        .va-import-error{color:#b42318;background:#fff5f4;padding:10px;border-radius:6px;margin:8px 0;font-size:12px}
        .va-import-error a{display:inline-block;margin:8px}
        .vehicle-import-digest-panel .upload-digest-panel{box-shadow:none;padding:12px}
        .vehicle-import-actions{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
        .vehicle-import-digest-panel td{padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;overflow-wrap:anywhere}
        .vehicle-import-digest-panel select{min-width:200px}
        .va-selection-link,.va-scope-label{display:block;margin-top:10px;font-size:12px}
        .va-selected-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
        .va-save{width:100%;margin-top:14px}
        .vehicle-allocation-tool-drawer{top:calc(100dvh - var(--va-available-height,calc(100dvh - 128px)) + 8px);width:min(260px,calc(100vw - 32px))}
        .vehicle-allocation-tool-panel{width:min(720px,calc(100vw - 32px));height:min(72vh,760px);max-height:calc(var(--va-available-height,calc(100dvh - 128px)) - 84px)}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .deck-floating-toggle{display:flex;align-items:center;justify-content:space-between;background:rgba(255,255,255,.72);color:#1f2937;border-color:rgba(203,213,225,.8)}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .deck-control-tab{border:1px solid rgba(203,213,225,.72);background:rgba(255,255,255,.58);color:#334155;text-align:left}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .deck-control-tab.is-active{border-color:#93c5fd;background:#dbeafe;color:#1d4ed8}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .btn{display:inline-flex;align-items:center;justify-content:center;border-radius:6px}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .btn-primary{background:#1c69d4;color:#fff;border-color:#1c69d4}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .btn-secondary{background:#fff;color:#344054;border-color:#cfd6df}
        .vehicle-allocation-page .vehicle-allocation-tool-drawer .btn-ghost{background:#fff;color:#344054;border-color:#cfd6df}
        .va-tool-tab-body{display:grid;gap:12px;margin-top:12px}
        .va-tool-card{display:grid;gap:12px;padding:14px;border:1px solid rgba(203,213,225,.86);border-radius:8px;background:rgba(255,255,255,.86)}
        .va-tool-card-head{display:flex;align-items:center;justify-content:space-between;gap:12px}
        .va-tool-card-head span{color:#0f766e;font-size:10px;font-weight:900;letter-spacing:.14em;text-transform:uppercase}
        .va-tool-card-head strong{color:#111827;font-size:13px}
        .va-tool-card p{margin:0;color:#64748b;font-size:12px;line-height:1.5}
        .va-tool-status-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
        .va-tool-status-grid div{display:grid;gap:2px;padding:10px;border:1px solid #e2e8f0;border-radius:6px;background:#f8fafc}
        .va-tool-status-grid span{color:#64748b;font-size:10px;font-weight:800;text-transform:uppercase}
        .va-tool-status-grid strong{color:#111827;font-size:16px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .va-tool-pill-grid{display:flex;flex-wrap:wrap;gap:6px}
        .va-tool-pill-grid > span,.va-tool-import-summary span{display:inline-flex;align-items:center;border:1px solid #dbe6f4;border-radius:999px;background:#f8fafc;color:#475467;padding:4px 8px;font-size:11px;font-weight:700}
        .va-tool-pill-grid > span{gap:8px}
        .va-tool-pill-grid > span.is-empty{border-color:#e2e8f0!important;background:#fff;color:#94a3b8}
        .va-tool-pill-grid > span strong{display:inline-flex;min-width:18px;justify-content:center;border-radius:999px;background:#fff;color:#111827;padding:1px 5px;font-size:10px}
        .va-tool-pill-grid > span.is-empty strong{color:#94a3b8}
        .va-tool-actions{display:flex;flex-wrap:wrap;gap:8px}
        .va-tool-import-summary{display:flex;flex-wrap:wrap;gap:6px}
        .vin-paste-input{width:100%;min-height:132px;padding:10px;border:1px solid #cfd6df;border-radius:6px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace;font-size:12px}
        .vin-paste-preview-table{display:grid;min-width:0;border:1px solid #e2e8f0;border-radius:6px;overflow:hidden}
        .vin-paste-preview-head,.vin-paste-preview-row{display:grid;grid-template-columns:56px minmax(150px,1fr) minmax(180px,1fr) minmax(120px,1.2fr);gap:8px;align-items:center;padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px}
        .vin-paste-preview-head{background:#f8fafc;color:#475467;font-weight:800;letter-spacing:.06em;text-transform:uppercase}
        .vin-paste-preview-row strong{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
        .vin-paste-preview-row.is-error{background:#fff7f7;color:#b91c1c}
        .vin-paste-preview-empty{padding:14px;color:#64748b;font-size:12px;text-align:center}
        .vin-paste-actions{display:flex;align-items:center;justify-content:flex-end;flex-wrap:wrap;gap:8px;width:100%}
        .vin-paste-message{color:#0f766e;font-size:12px;font-weight:800}
        @media (max-width:1100px){
          .vehicle-allocation-page{height:auto;min-height:0}
          .va-layout{grid-template-columns:minmax(0,1fr);flex:none;min-height:0}
          .va-browse-panel{flex:none;overflow:visible}
          .va-pi-list{flex:none;max-height:280px}
          .va-lines-panel{max-height:none;overflow:visible}
          .va-line-list{flex:none;max-height:40dvh}
          .va-grid{flex:none}
          .va-grid-body{flex:none;height:clamp(320px,60dvh,720px)}
          .va-header{align-items:stretch;flex-direction:column;padding-right:0}
          .vehicle-allocation-tool-drawer{top:auto;bottom:16px;width:240px}
          .vehicle-allocation-tool-panel{position:fixed;top:calc(100dvh - var(--va-available-height,calc(100dvh - 128px)) + 8px);right:16px;height:calc(var(--va-available-height,calc(100dvh - 128px)) - 24px);max-height:none}
          .va-search{min-width:0}
          .va-filters{grid-template-columns:repeat(2,minmax(0,1fr))}
          .va-bulk-fields{grid-template-columns:repeat(2,minmax(0,1fr))}
          .va-line-row{grid-template-columns:1fr}
        }
        @media (max-width:640px){
          .vehicle-allocation-page{padding:14px}
          .va-editor fieldset,.va-column-options{grid-template-columns:1fr}
        }
      `}</style>
    </div>
  );
}
