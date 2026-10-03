import { useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react";
import { api } from "../api/client";
import { CommandSelect, type CommandSelectOption } from "../components/CommandSelect";
import { LoadingActionButton } from "../components/LoadingActionButton";
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
import { ptColor } from "../utils/colors";
import { formatCountryCodeTooltip } from "../utils/jatoCountries";
import { compareProductModels } from "../utils/orderGeniusProductSort";

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

interface EditableVehicleForm {
  freightEur: string;
  insuranceEur: string;
  vin: string;
  productionDate: string;
  etd: string;
  eta: string;
  actualDepartureDate: string;
  actualArrivalDate: string;
  readyForPickupDate: string;
  shipName: string;
  dealerCode: string;
  dealerName: string;
  customerRef: string;
  allocationStatus: AllocationStatus;
  logisticsStatus: LogisticsStatus;
  remark: string;
}

interface BulkVehicleForm {
  freightEur: string;
  insuranceEur: string;
  dealerCode: string;
  productionDate: string;
  etd: string;
  eta: string;
  readyForPickupDate: string;
  shipName: string;
  allocationStatus: AllocationStatus | "";
  logisticsStatus: LogisticsStatus | "";
}

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

function cleanText(value: string): string | null {
  const text = value.trim();
  return text ? text : null;
}

function dateInput(value: string | null | undefined): string {
  return value ? value.slice(0, 10) : "";
}

function statusText(value: string): string {
  return value.replaceAll("_", " ");
}

function actionableError(reason: unknown, suggestion: string): string {
  const message = reason instanceof Error ? reason.message : "";
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

function toEditForm(vehicle: PiVehicleUnit): EditableVehicleForm {
  return {
    freightEur: vehicle.freightEur == null ? "" : String(vehicle.freightEur),
    insuranceEur: vehicle.insuranceEur == null ? "" : String(vehicle.insuranceEur),
    vin: vehicle.vin ?? "",
    productionDate: dateInput(vehicle.productionDate),
    etd: dateInput(vehicle.etd),
    eta: dateInput(vehicle.eta),
    actualDepartureDate: dateInput(vehicle.actualDepartureDate),
    actualArrivalDate: dateInput(vehicle.actualArrivalDate),
    readyForPickupDate: dateInput(vehicle.readyForPickupDate),
    shipName: vehicle.shipName ?? "",
    dealerCode: vehicle.dealerCode ?? "",
    dealerName: vehicle.dealerName ?? "",
    customerRef: vehicle.customerRef ?? "",
    allocationStatus: vehicle.allocationStatus,
    logisticsStatus: vehicle.logisticsStatus,
    remark: vehicle.remark ?? "",
  };
}

function toVehiclePayload(form: EditableVehicleForm): UpdateVehiclePayload {
  return {
    freightEur: form.freightEur === "" ? null : Number(form.freightEur),
    insuranceEur: form.insuranceEur === "" ? null : Number(form.insuranceEur),
    vin: cleanText(form.vin),
    productionDate: cleanText(form.productionDate),
    etd: cleanText(form.etd),
    eta: cleanText(form.eta),
    actualDepartureDate: cleanText(form.actualDepartureDate),
    actualArrivalDate: cleanText(form.actualArrivalDate),
    readyForPickupDate: cleanText(form.readyForPickupDate),
    shipName: cleanText(form.shipName),
    dealerCode: cleanText(form.dealerCode),
    dealerName: cleanText(form.dealerName),
    customerRef: cleanText(form.customerRef),
    allocationStatus: form.allocationStatus,
    logisticsStatus: form.logisticsStatus,
    remark: cleanText(form.remark),
  };
}

function toBulkFieldPayload(form: BulkVehicleForm): UpdateVehiclePayload {
  const payload: UpdateVehiclePayload = {};
  if (form.freightEur !== "") payload.freightEur = Number(form.freightEur);
  if (form.insuranceEur !== "") payload.insuranceEur = Number(form.insuranceEur);
  if (form.productionDate) {
    payload.productionDate = form.productionDate;
  }
  if (form.etd) {
    payload.etd = form.etd;
  }
  if (form.eta) {
    payload.eta = form.eta;
  }
  if (form.readyForPickupDate) {
    payload.readyForPickupDate = form.readyForPickupDate;
  }
  if (form.shipName.trim()) {
    payload.shipName = form.shipName.trim();
  }
  if (form.dealerCode.trim()) {
    payload.dealerCode = form.dealerCode.trim();
  }
  if (form.allocationStatus) {
    payload.allocationStatus = form.allocationStatus;
  }
  if (form.logisticsStatus) {
    payload.logisticsStatus = form.logisticsStatus;
  }
  return payload;
}

function isPiDetail(item: PiOrderDetail | PiVehicleUnit | null): item is PiOrderDetail {
  return Boolean(item && "header" in item);
}

function validCostInputs(...values: string[]): boolean {
  return values.every((value) => value === "" || (/^\d+(?:\.\d{1,2})?$/.test(value) && Number(value) < 10**12));
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

type VehicleColumnKey = keyof PiVehicleUnit | "config" | "cocPdf";
const VEHICLE_COLUMNS: Array<{ key: VehicleColumnKey; label: string; optional?: boolean }> = [
  { key: "carCode", label: "Car Code" }, { key: "vin", label: "VIN" },
  { key: "piCode", label: "PI" }, { key: "countryCode", label: "Country" },
  { key: "materialCode", label: "Material" }, { key: "config", label: "Config" },
  { key: "exteriorColorName", label: "Exterior" }, { key: "interiorColorName", label: "Interior" },
  { key: "fobEur", label: "FOB (EUR)" },
  { key: "freightEur", label: "Freight / 运费 (EUR)", optional: true },
  { key: "insuranceEur", label: "Insurance / 保费 (EUR)", optional: true },
  { key: "cocPdf", label: "COC PDF", optional: true },
  { key: "allocationStatus", label: "Allocation" }, { key: "logisticsStatus", label: "Logistics" },
  { key: "shipName", label: "Ship" }, { key: "eta", label: "ETA" },
  { key: "readyForPickupDate", label: "Ready for pickup / 可提车" },
  { key: "productionDate", label: "Production", optional: true },
  { key: "etd", label: "ETD", optional: true },
  { key: "actualDepartureDate", label: "Actual departure", optional: true },
  { key: "actualArrivalDate", label: "Actual arrival", optional: true },
  { key: "dealerCode", label: "Dealer", optional: true },
  { key: "remark", label: "Note / 备注", optional: true },
];
const COLUMN_GROUPS = [
  { label: "Identity / 车辆", keys: ["carCode", "vin", "piCode", "countryCode", "materialCode", "config", "exteriorColorName", "interiorColorName"] },
  { label: "Price & documents / 价格与文件", keys: ["fobEur", "freightEur", "insuranceEur", "cocPdf", "remark"] },
  { label: "Status & dates / 状态与日期", keys: ["allocationStatus", "logisticsStatus", "shipName", "eta", "readyForPickupDate", "productionDate", "etd", "actualDepartureDate", "actualArrivalDate", "dealerCode"] },
];
const DEFAULT_COLUMNS = VEHICLE_COLUMNS.filter((column) => !column.optional).map((column) => column.key);
const EMPTY_BULK_FORM: BulkVehicleForm = {
  freightEur: "", insuranceEur: "",
  dealerCode: "", productionDate: "", etd: "", eta: "", readyForPickupDate: "",
  shipName: "", allocationStatus: "", logisticsStatus: "",
};

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
  const canEdit = user?.role === "order_filler" || user?.role === "editor" || user?.role === "admin";
  const { countryOptions: accountCountryOptions } = useAccountCountryOptions();
  const defaultCountry = user?.primaryCountry ?? "";
  const [filters, setFilters] = useState<VehicleAllocationFilters>({
    country: defaultCountry,
    page: 1,
    pageSize: 100,
  });
  const [vehicles, setVehicles] = useState<PiVehicleUnit[]>([]);
  const [total, setTotal] = useState(0);
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
  const [cocLookup, setCocLookup] = useState<{ piCode: string; result: PiCocLookup } | null>(null);
  const [cocBusy, setCocBusy] = useState(false);
  const [cocError, setCocError] = useState("");
  const [cocDownloadConfirm, setCocDownloadConfirm] = useState<{ piCode: string; request: number; codes: string[]; missing: number; awaiting: number } | null>(null);
  const [cocFilter, setCocFilter] = useState<"all" | "available" | "missing" | "awaiting_vin">("all");
  const cocRequest = useRef(0);
  const cocResult = cocLookup?.piCode === selectedPi?.header.piCode ? cocLookup?.result : null;
  useEffect(() => { cocRequest.current += 1; setCocLookup(null); setCocError(""); setCocDownloadConfirm(null); setCocFilter("all"); }, [selectedPi]);
  const [deleteConfirmPi, setDeleteConfirmPi] = useState<string | null>(null);
  // Multi-select state
  const [selectedCarCodes, setSelectedCarCodes] = useState<Set<string>>(new Set());
  useEffect(() => { setCocDownloadConfirm(null); }, [selectedCarCodes]);
  const [selectedLineCode, setSelectedLineCode] = useState<string | null>(null);
  const [selectedVehicle, setSelectedVehicle] = useState<PiVehicleUnit | null>(null);
  const [editForm, setEditForm] = useState<EditableVehicleForm | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [vinBatchText, setVinBatchText] = useState("");
  const [batchVins, setBatchVins] = useState<string[]>([]);
  const [pivotBy, setPivotBy] = useState<"modelName" | "materialCode" | "logisticsStatus" | "countryCode">("modelName");
  const [loading, setLoading] = useState(false);
  const [sideLoading, setSideLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [bulkSaving, setBulkSaving] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [bulkForm, setBulkForm] = useState<BulkVehicleForm>(EMPTY_BULK_FORM);
  const [linesOpen, setLinesOpen] = useState(false);
  const linePanelRef = useRef<HTMLDivElement>(null);
  const [visibleColumnKeys, setVisibleColumnKeys] = useState<Set<VehicleColumnKey>>(new Set(DEFAULT_COLUMNS));
  const [columnOrder, setColumnOrder] = useState(VEHICLE_COLUMNS.map((column) => column.key));
  const orderedColumns = columnOrder.flatMap((key) => VEHICLE_COLUMNS.filter((column) => column.key === key));
  const visibleColumns = orderedColumns.filter((column) => visibleColumnKeys.has(column.key));
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

  const scopeBusy = saving || bulkSaving || vinPasteApplying || importBusy;
  const filteredUpdateNeedsSelection = Boolean(batchVins.length || filters.keyword || filters.carCode || filters.vin || filters.materialCode || filters.allocationStatus || filters.logisticsStatus || filters.vinMissingOnly || filters.unallocatedOnly);
  const page = filters.page ?? 1;
  const pageSize = filters.pageSize ?? 100;
  const selectedLine = selectedPi?.lines.find((line) => line.piLineCode === selectedLineCode) ?? null;
  const vehicleScopeReady = Boolean(
    filters.piCode
    || filters.piLineCode
    || filters.carCode
    || filters.vin
    || filters.keyword
    || filters.materialCode
    || filters.bom
    || filters.shipName
    || filters.allocationStatus
    || filters.logisticsStatus
    || filters.vinMissingOnly
    || filters.unallocatedOnly
  );
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
    return Array.from(byCode.values()).sort((a, b) => a.value.localeCompare(b.value));
  }, [accountCountryOptions, defaultCountry, filters.country, piBrowseCountry, selectedPi]);
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
  const tableVehicles = batchVins.length ? batchMatches.slice((page - 1) * pageSize, page * pageSize) : vehicles;
  const tableTotal = batchVins.length ? batchMatches.length : total;
  const totalPages = Math.max(1, Math.ceil(tableTotal / pageSize));
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
    return { vinMissing, ready, allocated };
  }, [vinPasteScopeVehicles]);

  useEffect(() => {
    if (!defaultCountry) {
      return;
    }
    setFilters((current) => current.country ? current : { ...current, country: defaultCountry });
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
    if (!vehicleScopeReady) {
      setVehicles([]);
      setTotal(0);
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    api.listVehicleAllocationVehicles(filters)
      .then((res) => {
        if (cancelled) {
          return;
        }
        setVehicles(res.items);
        setTotal(res.total);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(actionableError(err, "Could not load vehicles. Re-read this PI / 无法读取车辆，请重新读取当前 PI；仍失败请联系管理员。"));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [filters, refreshKey, vehicleScopeReady]);

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
    setBatchVins([]); setVinBatchText(""); setSearchTerm("");
    setSelectedCarCodes(new Set());
    setBulkForm(EMPTY_BULK_FORM);
    setSelectedVehicle(null);
    setEditForm(null);
    setVinPasteText("");
    setVinPasteMessage("");
    setDeleteConfirmPi(null);
    setImportPreview(null);
    setImportError(null);
    setRemoveVins(false);
    setAllowReplacing(false);
  }

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

  function updateFilter<K extends keyof VehicleAllocationFilters>(
    key: K,
    value: VehicleAllocationFilters[K],
  ): void {
    if (key !== "page") {
      setBatchVins([]); setVinBatchText(""); setSelectedCarCodes(new Set());
    }
    setFilters((current) => ({ ...current, [key]: value, page: key === "page" ? value as number : 1 }));
  }

  function setPiVehicleScope(detail: PiOrderDetail, lineCode: string | null): void {
    setSelectedPi(detail);
    setSelectedLineCode(lineCode);
    setFilters((current) => ({
      ...current,
      country: detail.header.countryCode,
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
        vinList: vins,
      });
      const detail = await api.getVehicleAllocationPi(selectedPi.header.piCode);
      setSelectedPi(detail);
      setSelectedVehicle(null);
      setEditForm(null);
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
    const keyword = searchTerm.trim();
    if (!keyword) {
      return;
    }
    setError(null);
    setNotice(null);
    if (!/^(PI-|CAR-)/i.test(keyword) && !/^[A-HJ-NPR-Z0-9]{17}$/i.test(keyword)) {
      setBatchVins([]); setSelectedCarCodes(new Set());
      setFilters((current) => ({ ...current, keyword, carCode: undefined, vin: undefined, page: 1 }));
      return;
    }
    try {
      const result = await api.searchVehicleAllocation(keyword);
      if (result.type === "pi" && isPiDetail(result.item)) {
        const detail = result.item;
        setPiVehicleScope(detail, null);
        setNotice(`Loaded ${detail.header.piCode}`);
        return;
      }
      if (result.type === "vehicle" && result.item && !isPiDetail(result.item)) {
        const vehicle = result.item;
        const detail = await api.getVehicleAllocationPi(vehicle.piCode);
        setPiVehicleScope(detail, vehicle.piLineCode);
        setSelectedVehicle(vehicle);
        setEditForm(toEditForm(vehicle));
        setNotice(`Loaded ${vehicle.carCode}`);
        return;
      }
      setBatchVins([]); setSelectedCarCodes(new Set());
      setFilters((current) => ({ ...current, keyword, carCode: undefined, vin: undefined, page: 1 }));
    } catch (err: unknown) {
      setError(actionableError(err, "Search failed. Check PI/CarCode/VIN and search again / 搜索未完成，请核对 PI、CarCode 或 VIN 后重试。"));
    }
  }

  async function selectPi(piCode: string): Promise<void> {
    setError(null);
    setNotice(null);
    try {
      const detail = await api.getVehicleAllocationPi(piCode);
      setPiVehicleScope(detail, null);
    } catch (err: unknown) {
      setError(actionableError(err, "Could not load this PI. Retry from the PI list / 无法读取此 PI，请从 PI 列表重新选择；仍失败请联系管理员。"));
    }
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
      const blob = await api.piCocDownload(piCode, available);
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

  function selectVehicle(vehicle: PiVehicleUnit): void {
    setSelectedVehicle(vehicle);
    setEditForm(toEditForm(vehicle));
  }

  async function saveVehicle(): Promise<void> {
    if (!canEdit || !selectedVehicle || !editForm || scopeBusy) {
      return;
    }
    if (!validCostInputs(editForm.freightEur, editForm.insuranceEur)) {
      setError("Use non-negative EUR amounts with up to 2 decimals / EUR金额须非负且最多两位小数。"); return;
    }
    setSaving(true);
    setError(null);
    try {
      const next = await api.updateVehicleAllocationVehicle(
        selectedVehicle.carCode,
        toVehiclePayload(editForm),
      );
      setSelectedVehicle(next);
      setEditForm(toEditForm(next));
      setVehicles((current) => current.map((item) => item.carCode === next.carCode ? next : item));
      setSelectedPi((current) => current ? {
        ...current, vehicles: current.vehicles.map((item) => item.carCode === next.carCode ? next : item),
      } : current);
      setNotice(`Saved ${next.carCode}`);
    } catch (err: unknown) {
      setError(actionableError(err, "Could not save this vehicle. Keep your input and check the VIN/status before retrying / 车辆保存未完成，输入已保留；请核对 VIN、状态后重试。"));
    } finally {
      setSaving(false);
    }
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

  function toggleVehicleSelect(carCode: string) {
    setSelectedCarCodes((prev) => {
      const next = new Set(prev);
      if (next.has(carCode)) next.delete(carCode); else next.add(carCode);
      return next;
    });
  }

  function toggleSelectAll() {
    const allCodes = tableVehicles.map((v) => v.carCode);
    if (allCodes.every((c) => selectedCarCodes.has(c))) {
      setSelectedCarCodes((current) => new Set([...current].filter((code) => !allCodes.includes(code))));
    } else {
      setSelectedCarCodes((current) => new Set([...current, ...allCodes]));
    }
  }

  function searchVinBatch(): void {
    const vins = [...new Set(vinBatchText.toUpperCase().split(/[\s,;]+/).filter(Boolean))];
    if (vins.length > 1000 || vins.some((vin) => !/^[A-HJ-NPR-Z0-9]{17}$/.test(vin))) {
      setError("Use up to 1,000 complete 17-character VINs / 请使用最多 1,000 个完整的 17 位 VIN。"); return;
    }
    setError(null); setSelectedCarCodes(new Set()); setBatchVins(vins);
    setFilters({ country: filters.country, piCode: selectedPi?.header.piCode, piLineCode: selectedLineCode ?? undefined, page: 1, pageSize });
  }

  function moveColumn(key: VehicleColumnKey, direction: number): void {
    setColumnOrder((current) => {
      const index = current.indexOf(key); const target = index + direction;
      if (target < 0 || target >= current.length) return current;
      const next = [...current]; [next[index], next[target]] = [next[target], next[index]]; return next;
    });
  }

  async function exportCurrentView(): Promise<void> {
    setExporting(true);
    setError(null);
    try {
      const blob = await api.exportVehicleAllocation({ ...filters, columns: visibleColumns.map((column) => column.key), ...(batchVins.length ? { carCodes: batchMatches.map((vehicle) => vehicle.carCode) } : {}) });
      const country = filters.country || "ALL";
      buildDownload(blob, `Vehicle_Allocation_${country}_${new Date().toISOString().slice(0, 10)}.xlsx`);
    } catch (err: unknown) {
      setError(actionableError(err, "Export failed. Check view filters and retry / 导出未完成，请核对查看筛选后重试。"));
    } finally {
      setExporting(false);
    }
  }

  async function applyBulkUpdate(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (!canEdit || scopeBusy) return;
    if (!validCostInputs(bulkForm.freightEur, bulkForm.insuranceEur)) {
      setError("Use non-negative EUR amounts with up to 2 decimals / EUR金额须非负且最多两位小数。"); return;
    }
    if ((bulkForm.freightEur !== "" || bulkForm.insuranceEur !== "") && !selectedCarCodes.size) {
      setError("Select vehicle rows before updating costs / 更新运保费前请明确勾选车辆。"); return;
    }
    if (filteredUpdateNeedsSelection && !selectedCarCodes.size) {
      setError("Select search results before updating / 搜索或筛选后请先勾选要更新的车辆。"); return;
    }
    if (!selectedPi) {
      setError("先选择 PI");
      return;
    }
    const fields = toBulkFieldPayload(bulkForm);
    if (Object.keys(fields).length === 0) {
      setError("Choose at least one status field. / 请填写至少一个状态字段。");
      return;
    }
    setBulkSaving(true);
    setError(null);
    try {
      const result = await api.bulkUpdateVehicleAllocationVehicles({
        piCode: selectedPi.header.piCode,
        piLineCode: selectedLineCode ?? undefined,
        carCodes: selectedCarCodes.size ? Array.from(selectedCarCodes) : undefined,
        fields,
      });
      const detail = await api.getVehicleAllocationPi(selectedPi.header.piCode);
      setSelectedPi(detail);
      setSelectedVehicle(null);
      setEditForm(null);
      setRefreshKey((key) => key + 1);
      setBulkForm(EMPTY_BULK_FORM);
      setSelectedCarCodes(new Set());
      setNotice(
        `Updated ${result.updatedUnits}/${result.matchedUnits} vehicles`
        + " / 车辆状态已更新",
      );
    } catch (err: unknown) {
      setError(actionableError(err, "Could not complete status update. Input is retained; re-read the PI to check saved status before retrying / 状态更新未完成，输入已保留；请先重新读取 PI 核对已保存状态，再重试。"));
    } finally {
      setBulkSaving(false);
    }
  }

  return (
    <div className="vehicle-allocation-page">
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
          {user?.role === "admin" || user?.role === "editor" ? <a href="/product/coc-match">Open COC workbench / 打开 COC 工作台</a> : null}
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
        title="Vehicle allocation tools"
        eyebrow="Order Genius"
        ariaLabel="PI vehicle allocation tools"
        closeLabel="Close"
        className="vehicle-allocation-tool-drawer"
        panelClassName="vehicle-allocation-tool-panel"
      >
        <DeckControlTabs
          tabs={PI_TOOL_TABS}
          activeKey={activeToolTab}
          onChange={setActiveToolTab}
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
                      <button type="button" onClick={() => { setFilters({ piCode: selectedPi?.header.piCode, carCode: item.carCode, page: 1, pageSize }); setToolDrawerOpen(false); }}>{item.carCode}</button> · {item.vin || "Awaiting VIN / 待录 VIN"} · {item.status === "available" ? "PDF available / 有 PDF" : item.status === "missing" ? "Missing PDF / 缺 PDF" : "—"}
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
          {activeToolTab === "status" ? (
            <>
              <details className="va-status-details">
                <summary>Status details / 状态详情</summary>
                <VehicleStatusBoard scopeLabel={activeScopeLabel} vehicles={vinPasteScopeVehicles} statusFlow={statusFlow} />
              </details>
              <form className="va-bulk-panel" onSubmit={(event) => void applyBulkUpdate(event)}>
                <div className="va-bulk-head">
                  <div>
                    <h3>Update status</h3>
                    <span title="Current batch edit scope">{selectedCarCodes.size ? `${selectedCarCodes.size} selected vehicles / 勾选车辆` : `${activeScopeLabel} · ${vinPasteScopeVehicles.length} vehicles / 全范围`}</span>
                  </div>
                  <LoadingActionButton
                    type="submit"
                    loading={bulkSaving}
                    disabled={!canEdit || !selectedPi || scopeBusy || ((filteredUpdateNeedsSelection || bulkForm.freightEur !== "" || bulkForm.insuranceEur !== "") && !selectedCarCodes.size)}
                    loadingLabel="Applying..."
                  >
                    Update status
                  </LoadingActionButton>
                </div>
                <div className="va-bulk-fields">
                  <label>Freight / 运费 (EUR)<input type="number" min="0" step="0.01" value={bulkForm.freightEur} placeholder="Keep / 保留" onChange={(event) => setBulkForm((current) => ({ ...current, freightEur: event.target.value }))} /></label>
                  <label>Insurance / 保费 (EUR)<input type="number" min="0" step="0.01" value={bulkForm.insuranceEur} placeholder="Keep / 保留" onChange={(event) => setBulkForm((current) => ({ ...current, insuranceEur: event.target.value }))} /></label>
                  <label>Production<input type="date" value={bulkForm.productionDate} onChange={(event) => setBulkForm((current) => ({ ...current, productionDate: event.target.value }))} /></label>
                  <label>ETD<input type="date" value={bulkForm.etd} onChange={(event) => setBulkForm((current) => ({ ...current, etd: event.target.value }))} /></label>
                  <label>ETA<input type="date" value={bulkForm.eta} onChange={(event) => setBulkForm((current) => ({ ...current, eta: event.target.value }))} /></label>
                  <label>Ready Pickup<input type="date" value={bulkForm.readyForPickupDate} onChange={(event) => setBulkForm((current) => ({ ...current, readyForPickupDate: event.target.value }))} /></label>
                  <label>Ship<input value={bulkForm.shipName} onChange={(event) => setBulkForm((current) => ({ ...current, shipName: event.target.value }))} /></label>
                  <label>Dealer code<input value={bulkForm.dealerCode} onChange={(event) => setBulkForm((current) => ({ ...current, dealerCode: event.target.value }))} /></label>
                  <div className="va-field">
                    <span>Allocation</span>
                    <CommandSelect
                      value={bulkForm.allocationStatus}
                      options={allocationStatusOptions}
                      placeholder="Keep allocation"
                      searchPlaceholder="Search allocation..."
                      allowClear
                      onChange={(value) => setBulkForm((current) => ({ ...current, allocationStatus: value }))}
                    />
                  </div>
                  <div className="va-field">
                    <span>Logistics</span>
                    <CommandSelect
                      value={bulkForm.logisticsStatus}
                      options={logisticsStatusOptions}
                      placeholder="Keep logistics"
                      searchPlaceholder="Search logistics..."
                      allowClear
                      onChange={(value) => setBulkForm((current) => ({ ...current, logisticsStatus: value }))}
                    />
                  </div>
                </div>
              </form>

            </>
          ) : null}
          {activeToolTab === "view" ? (
            <>
              <section className="va-filters">
                <input
                  value={filters.carCode ?? ""}
                  onChange={(event) => updateFilter("carCode", event.target.value.toUpperCase())}
                  placeholder="Car Code"
                  title="Filter by generated Car Code"
                />
                <input
                  value={filters.materialCode ?? ""}
                  onChange={(event) => updateFilter("materialCode", event.target.value.toUpperCase())}
                  placeholder="Material"
                  title="Filter by material code"
                />
                <CommandSelect
                  value={filters.allocationStatus ?? ""}
                  options={allocationStatusOptions}
                  placeholder="Allocation"
                  searchPlaceholder="Search allocation..."
                  allowClear
                  onChange={(value) => updateFilter("allocationStatus", value)}
                />
                <CommandSelect
                  value={filters.logisticsStatus ?? ""}
                  options={logisticsStatusOptions}
                  placeholder="Logistics"
                  searchPlaceholder="Search logistics..."
                  allowClear
                  onChange={(value) => updateFilter("logisticsStatus", value)}
                />
                <label className="va-check">
                  <input
                    type="checkbox"
                    checked={Boolean(filters.vinMissingOnly)}
                    onChange={(event) => updateFilter("vinMissingOnly", event.target.checked)}
                  />
                  VIN missing
                </label>
                <label className="va-check">
                  <input
                    type="checkbox"
                    checked={Boolean(filters.unallocatedOnly)}
                    onChange={(event) => updateFilter("unallocatedOnly", event.target.checked)}
                  />
                  Unallocated
                </label>
                <LoadingActionButton
                  type="button"
                  variant="secondary"
                  onClick={() => {
                    setBatchVins([]); setVinBatchText(""); setSearchTerm(""); setSelectedCarCodes(new Set());
                    setFilters({
                      country: selectedPi?.header.countryCode || defaultCountry,
                      piCode: selectedPi?.header.piCode, piLineCode: selectedLineCode ?? undefined,
                      page: 1, pageSize,
                    });
                  }}
                >
                  Reset
                </LoadingActionButton>
              </section>

              <section className="va-tool-card">
                <label htmlFor="vin-batch-search">VIN batch search / VIN 批量搜索</label>
                <textarea id="vin-batch-search" value={vinBatchText} onChange={(event) => setVinBatchText(event.target.value)} placeholder="One complete VIN per line / 每行一个完整 VIN" />
                <div className="va-button-row">
                  <button type="button" disabled={!selectedPi || scopeBusy} onClick={searchVinBatch}>Search VIN batch / 搜索 VIN</button>
                  <button type="button" disabled={!batchMatches.length || scopeBusy} onClick={() => setSelectedCarCodes(new Set(batchMatches.map((vehicle) => vehicle.carCode)))}>Select matches / 勾选全部匹配</button>
                  <button type="button" className="btn-secondary" onClick={() => { setBatchVins([]); setVinBatchText(""); setSelectedCarCodes(new Set()); }}>Clear batch search / 清除批量搜索</button>
                </div>
                {batchVins.length ? <p role="status">{batchMatches.length} matched / 匹配 · {missingVins.length} not found in this PI/line / 本范围未找到{missingVins.length ? `: ${missingVins.join(", ")}` : ""}</p> : null}
              </section>

              <fieldset className="va-columns">
                <legend>Columns / 显示列</legend>
                {COLUMN_GROUPS.map((group) => <details key={group.label} open>
                  <summary>{group.label}</summary>
                  <div className="va-button-row">
                    <button type="button" className="btn-secondary" onClick={() => setVisibleColumnKeys((current) => new Set([...current].filter((key) => !group.keys.includes(key))))}>Collapse group / 折叠列组</button>
                    <button type="button" className="btn-secondary" onClick={() => setVisibleColumnKeys((current) => new Set([...current, ...VEHICLE_COLUMNS.filter((column) => group.keys.includes(column.key)).map((column) => column.key)]))}>Expand group / 展开列组</button>
                  </div>
                  {orderedColumns.filter((column) => group.keys.includes(column.key)).map((column) => (
                  <div className="va-column-choice" key={column.key}><label>
                    <input type="checkbox" checked={visibleColumnKeys.has(column.key)}
                      onChange={() => setVisibleColumnKeys((current) => {
                        const next = new Set(current);
                        if (next.has(column.key)) next.delete(column.key); else next.add(column.key);
                        return next;
                      })} />
                    {column.label}
                  </label><button type="button" aria-label={`Move ${column.label} left`} disabled={columnOrder.indexOf(column.key) === 0} onClick={() => moveColumn(column.key, -1)}>←</button><button type="button" aria-label={`Move ${column.label} right`} disabled={columnOrder.indexOf(column.key) === columnOrder.length - 1} onClick={() => moveColumn(column.key, 1)}>→</button></div>
                ))}</details>)}
                <button type="button" className="btn-secondary" onClick={() => { setVisibleColumnKeys(new Set(DEFAULT_COLUMNS)); setColumnOrder(VEHICLE_COLUMNS.map((column) => column.key)); }}>Restore default columns / 恢复默认列</button>
              </fieldset>
              <LoadingActionButton disabled={!visibleColumns.length || !selectedPi} loading={exporting} loadingLabel="Exporting..." onClick={() => void exportCurrentView()} variant="secondary">Export current view / 导出</LoadingActionButton>
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

      <div className="va-layout">
        <aside className="va-side">
          <section className="va-panel">
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
                <details className="va-month-browser">
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
                  disabled={scopeBusy}
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
            <div className="va-panel" ref={linePanelRef}>
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
                  <button type="button" className="btn-secondary" onClick={() => openPiTool("status")}>
                    Status / 状态 · {tableSummary.ready} ready for pickup · {tableSummary.allocated} allocated
                  </button>
                  {user?.role === "admin" && (deleteConfirmPi === selectedPi.header.piCode ? (
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
              <span>{selectedCarCodes.size} selected / 已勾选</span>
              <button type="button" onClick={() => openPiTool("status")}>Update selected status / 更新勾选状态</button>
              <button type="button" className="btn-secondary" onClick={() => setSelectedCarCodes(new Set())}>Clear selection / 清除勾选</button>
            </div>
          ) : null}

          <section className="va-table-wrap">
            <div className="va-table-head">
              <span>{loading ? "Loading vehicles" : selectedPi ? `${tableVehicles.length} shown · ${activeScopeLabel}${batchVins.length ? ` · VIN batch ${tableTotal} matches` : ""}` : "Select a PI"}</span>
              <div>
                <button type="button" disabled={page <= 1} onClick={() => updateFilter("page", page - 1)}>Prev</button>
                <span>{page} / {totalPages}</span>
                <button type="button" disabled={page >= totalPages} onClick={() => updateFilter("page", page + 1)}>Next</button>
              </div>
            </div>
            <div className="va-table-scroll">
              <table className="va-table" aria-label="Vehicle details">
                <thead>
                  <tr>
                    <th style={{ width: 32 }}>
                      <input type="checkbox"
                        checked={tableVehicles.length > 0 && tableVehicles.every((v) => selectedCarCodes.has(v.carCode))}
                        onChange={toggleSelectAll}
                        aria-label="Select current page vehicles"
                        style={{ margin: 0 }} />
                    </th>
                    {visibleColumns.map((column) => <th key={column.key}>{column.label}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {tableVehicles.map((vehicle) => {
                    const isChecked = selectedCarCodes.has(vehicle.carCode);
                    return (
                    <tr
                      key={vehicle.carCode}
                      className={selectedVehicle?.carCode === vehicle.carCode ? "is-selected" : ""}
                    >
                      <td>
                        <input type="checkbox" aria-label={`Select ${vehicle.carCode}`} checked={isChecked}
                          onChange={() => toggleVehicleSelect(vehicle.carCode)}
                          onClick={(e) => e.stopPropagation()}
                          style={{ margin: 0 }} />
                      </td>
                      {visibleColumns.map((column) => (
                        <td key={column.key} onClick={() => selectVehicle(vehicle)}>{column.key === "cocPdf" ? (() => {
                          const status = cocResult?.items.find((item) => item.carCode === vehicle.carCode)?.status;
                          return status === "available" ? <button type="button" disabled={cocBusy} onClick={(event) => { event.stopPropagation(); void downloadCocs([vehicle.carCode]); }}>PDF ↓</button> : status === "missing" ? "Missing / 缺 PDF" : status === "awaiting_vin" ? "Awaiting VIN / 待录" : "Search library / 查库";
                        })() : vehicleCell(vehicle, column.key)}</td>
                      ))}
                    </tr>
                  );})}
                  {!loading && tableVehicles.length === 0 && (
                    <tr>
                      <td colSpan={visibleColumns.length + 1} className="va-empty">{selectedPi ? "No vehicles in current scope" : "Select a PI to view vehicles"}</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </main>
      </div>

      {selectedVehicle && editForm && (
        <aside className="va-drawer">
          <div className="va-drawer-head">
            <div>
              <h2>{selectedVehicle.carCode}</h2>
              <span>{display(selectedVehicle.piCode)} · {display(selectedVehicle.materialCode)}</span>
            </div>
            <button type="button" onClick={() => { setSelectedVehicle(null); setEditForm(null); }}>Close</button>
          </div>
          <div className="va-drawer-grid">
            <label>VIN<input value={editForm.vin} onChange={(event) => setEditForm((current) => current ? { ...current, vin: event.target.value.toUpperCase() } : current)} /></label>
            <div className="va-field">
              <span>Allocation</span>
              <CommandSelect
                value={editForm.allocationStatus}
                options={allocationStatusOptions}
                searchPlaceholder="Search allocation..."
                onChange={(value) => setEditForm((current) => current && value ? { ...current, allocationStatus: value } : current)}
              />
            </div>
            <div className="va-field">
              <span>Logistics</span>
              <CommandSelect
                value={editForm.logisticsStatus}
                options={logisticsStatusOptions}
                searchPlaceholder="Search logistics..."
                onChange={(value) => setEditForm((current) => current && value ? { ...current, logisticsStatus: value } : current)}
              />
            </div>
            <label>Production<input type="date" value={editForm.productionDate} onChange={(event) => setEditForm((current) => current ? { ...current, productionDate: event.target.value } : current)} /></label>
            <label>ETD<input type="date" value={editForm.etd} onChange={(event) => setEditForm((current) => current ? { ...current, etd: event.target.value } : current)} /></label>
            <label>ETA<input type="date" value={editForm.eta} onChange={(event) => setEditForm((current) => current ? { ...current, eta: event.target.value } : current)} /></label>
            <label>Actual Departure<input type="date" value={editForm.actualDepartureDate} onChange={(event) => setEditForm((current) => current ? { ...current, actualDepartureDate: event.target.value } : current)} /></label>
            <label>Actual Arrival<input type="date" value={editForm.actualArrivalDate} onChange={(event) => setEditForm((current) => current ? { ...current, actualArrivalDate: event.target.value } : current)} /></label>
            <label>Ready Pickup<input type="date" value={editForm.readyForPickupDate} onChange={(event) => setEditForm((current) => current ? { ...current, readyForPickupDate: event.target.value } : current)} /></label>
            <label>Ship<input value={editForm.shipName} onChange={(event) => setEditForm((current) => current ? { ...current, shipName: event.target.value } : current)} /></label>
            <label>Dealer Code<input value={editForm.dealerCode} onChange={(event) => setEditForm((current) => current ? { ...current, dealerCode: event.target.value } : current)} /></label>
            <label>Dealer Name<input value={editForm.dealerName} onChange={(event) => setEditForm((current) => current ? { ...current, dealerName: event.target.value } : current)} /></label>
            <label>Customer Ref<input value={editForm.customerRef} onChange={(event) => setEditForm((current) => current ? { ...current, customerRef: event.target.value } : current)} /></label>
            <label>Freight / 运费 (EUR)<input type="number" min="0" step="0.01" readOnly={!canEdit} value={editForm.freightEur} onChange={(event) => setEditForm((current) => current ? { ...current, freightEur: event.target.value } : current)} /></label>
            <label>Insurance / 保费 (EUR)<input type="number" min="0" step="0.01" readOnly={!canEdit} value={editForm.insuranceEur} onChange={(event) => setEditForm((current) => current ? { ...current, insuranceEur: event.target.value } : current)} /></label>
            <label className="va-wide">Note / 备注<textarea readOnly={!canEdit} value={editForm.remark} onChange={(event) => setEditForm((current) => current ? { ...current, remark: event.target.value } : current)} /></label>
          </div>
          <LoadingActionButton
            className="va-save"
            loading={saving}
            disabled={!canEdit || scopeBusy}
            loadingLabel="Saving..."
            onClick={() => void saveVehicle()}
          >
            Save Vehicle
          </LoadingActionButton>
        </aside>
      )}

      <style>{`
        .vehicle-allocation-page{max-width:1680px;margin:0 auto;padding:24px;color:#111827}
        .va-header{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;margin-bottom:16px}
        .va-kicker{font-size:11px;font-weight:700;text-transform:uppercase;color:#667085}
        .va-header h1{font-size:28px;font-weight:600;line-height:1.1;margin:4px 0 0}
        .va-search{display:flex;gap:8px;min-width:420px}
        .va-search input,.va-filters input,.va-form input,.va-bulk-panel textarea,.va-bulk-fields input,.va-drawer input,.va-drawer textarea{border:1px solid #cfd6df;background:#fff;color:#111827;border-radius:6px;padding:9px 10px;min-width:0}
        .va-search input{flex:1}
        .vehicle-allocation-page button{border:1px solid #1c69d4;background:#1c69d4;color:white;border-radius:6px;padding:9px 12px;cursor:pointer;font-weight:600}
        .vehicle-allocation-page .btn-secondary,.vehicle-allocation-page .btn-ghost{background:#fff;color:#344054;border-color:#cfd6df}
        .vehicle-allocation-page .btn-secondary:hover,.vehicle-allocation-page .btn-ghost:hover{background:#f8fafc;border-color:#b8c2ce}
        .vehicle-allocation-page .btn-danger{background:#dc2626;color:#fff;border-color:#dc2626}
        .vehicle-allocation-page button:disabled{background:#a8b3c1;border-color:#a8b3c1;cursor:not-allowed}
        .va-message{padding:10px 12px;border-radius:6px;margin-bottom:14px}
        .va-message.is-error{background:#fff1f0;color:#a8071a;border:1px solid #ffa39e}
        .va-message.is-notice{background:#f0f7ff;color:#174ea6;border:1px solid #b7d6ff}
        .va-layout{display:grid;grid-template-columns:330px minmax(0,1fr);gap:16px;align-items:start}
        .va-side,.va-main{display:flex;flex-direction:column;gap:16px;min-width:0}
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
        .va-pi-list{display:grid;gap:8px;margin-top:12px;max-height:280px;overflow:auto}
        .va-pi-list button{background:#fff;color:#111827;border-color:#d8dee6;text-align:left;display:grid;gap:2px}
        .va-pi-list button.is-active{border-color:#1c69d4;background:#eef5ff}
        .va-pi-list small{color:#667085}
        .va-button-row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px}
        .va-filters{display:grid;grid-template-columns:repeat(2,minmax(112px,1fr));gap:10px;padding:12px}
        .va-check{display:flex;align-items:center;gap:6px;min-height:38px;font-size:12px;color:#475467}
        .va-pi-detail{padding:14px}
        .va-pi-metrics{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-start;align-items:center;padding-top:10px;border-top:1px solid #e5eaf0}
        .va-product-text{font-weight:600;filter:brightness(.72)}
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
        .va-line-list{display:grid;gap:6px;margin-top:12px;max-height:45vh;overflow:auto}
        .va-line-row{display:flex;align-items:stretch;border:1px solid #e5eaf0;border-radius:6px;background:#fbfcfe;color:#111827;overflow:hidden}
        .va-line-row.is-active{border-color:#1c69d4;background:#eef5ff}
        .va-line-all{background:#fff}
        .vehicle-allocation-page .va-line-body{display:grid;grid-template-columns:1fr;gap:8px;align-items:center;flex:1;padding:8px;border:none;background:#f8fafc;cursor:pointer;text-align:left;color:#344054;font:inherit}
        .va-line-body strong{font-size:12px}
        .va-line-body span,.va-line-body small{font-size:12px;color:#667085;overflow-wrap:anywhere;white-space:normal}
        .va-line-config{font-weight:600;filter:brightness(.72)}
        .va-table-wrap{overflow:hidden}
        .va-table-head{padding:10px 12px;border-bottom:1px solid #e5eaf0}
        .va-table-head div{display:flex;gap:8px;align-items:center}
        .va-table-head button{padding:6px 10px}
        .va-table-scroll{overflow:auto;max-height:620px}
        .va-table{width:100%;border-collapse:collapse}
        .va-table th{position:sticky;top:0;background:#f8fafc;color:#475467;text-align:left;font-size:12px;border-bottom:1px solid #d8dee6;padding:10px}
        .va-table td{border-bottom:1px solid #eef2f6;padding:10px;font-size:13px;white-space:nowrap}
        .va-table tbody tr{cursor:pointer}
        .va-table tbody tr:hover,.va-table tbody tr.is-selected{background:#eef5ff}
        .va-empty{text-align:center;color:#667085;padding:28px!important}
        .va-status{display:inline-flex;align-items:center;border-radius:999px;padding:3px 8px;background:#eef2f6;color:#344054;font-size:12px}
        .va-status-allocated,.va-status-delivered,.va-status-ready_for_pickup{background:#e8f7ee;color:#16794a}
        .va-status-reserved,.va-status-on_vessel,.va-status-in_production{background:#fff7e6;color:#ad6800}
        .va-status-unallocated,.va-status-pending{background:#eef2f6;color:#475467}
        .va-status-cancelled{background:#fff1f0;color:#a8071a}
        .va-drawer{position:fixed;right:0;top:80px;bottom:0;width:min(520px,100vw);background:#fff;border-left:1px solid #cfd6df;box-shadow:-12px 0 28px rgba(16,24,40,.12);z-index:60;padding:18px;overflow:auto}
        .va-drawer-head{border-bottom:1px solid #e5eaf0;padding-bottom:12px;margin-bottom:12px}
        .va-drawer-head button{background:#fff;color:#111827;border-color:#cfd6df}
        .va-drawer-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
        .va-drawer label,.va-drawer .va-field{display:grid;gap:5px;font-size:12px;font-weight:700;color:#475467}
        .va-drawer textarea{min-height:88px;resize:vertical}
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
        .vehicle-allocation-tool-drawer{top:92px;width:min(520px,calc(100vw - 32px))}
        .vehicle-allocation-tool-panel{width:min(720px,calc(100vw - 32px));height:min(72vh,760px)}
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
          .va-layout{grid-template-columns:minmax(0,1fr)}
          .va-header{align-items:stretch;flex-direction:column}
          .va-search{min-width:0}
          .va-filters{grid-template-columns:repeat(2,minmax(0,1fr))}
          .va-bulk-fields{grid-template-columns:repeat(2,minmax(0,1fr))}
          .va-line-row{grid-template-columns:1fr}
        }
        @media (max-width:640px){
          .vehicle-allocation-page{padding:14px}
          .va-filters,.va-drawer-grid{grid-template-columns:1fr}
          .va-drawer{top:0;width:100%}
        }
      `}</style>
    </div>
  );
}
