import { useEffect, useMemo, useRef, useState, type ChangeEvent, type FormEvent } from "react";
import { api } from "../api/client";
import { CommandSelect, type CommandSelectOption } from "../components/CommandSelect";
import { LoadingActionButton } from "../components/LoadingActionButton";
import {
  buildUnsupportedVehicleImportPreview,
  detectVehicleImportSource,
  parseVehicleImportRowsPayload,
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
import { useAccountCountryOptions } from "../hooks/useAccountCountryOptions";
import type {
  AllocationStatus,
  LogisticsStatus,
  PiOrderDetail,
  PiOrderHeader,
  PiVehicleUnit,
  UpdateVehiclePayload,
  VehicleAllocationFilters,
  VehicleStatusFlowConfig,
  VehicleStatusFlowStep,
  VehicleImportPreview,
} from "../types/orderGeniusVehicle";
import { formatCountryCodeTooltip } from "../utils/jatoCountries";

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

type PiToolTab = "status" | "vin" | "import" | "batch";

const PI_TOOL_TABS: Array<DeckControlTabItem<PiToolTab>> = [
  { key: "status", label: "Status", caption: "Flow board" },
  { key: "vin", label: "VIN paste", caption: "Preview apply" },
  { key: "import", label: "Import", caption: "File digest" },
  { key: "batch", label: "Batch", caption: "Fields" },
];

interface EditableVehicleForm {
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

interface PiForm {
  countryCode: string;
  orderMonth: string;
  orderDate: string;
  officialPiNo: string;
  shipName: string;
  eta: string;
}

interface LineForm {
  materialCode: string;
  bom: string;
  brand: string;
  modelName: string;
  version: string;
  powertrain: string;
  exteriorColorName: string;
  interiorColorName: string;
  quantity: string;
  fobEur: string;
}

interface BulkVehicleForm {
  vinText: string;
  productionDate: string;
  etd: string;
  eta: string;
  readyForPickupDate: string;
  shipName: string;
  allocationStatus: AllocationStatus | "";
  logisticsStatus: LogisticsStatus | "";
}

function currentMonth(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
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

function batchAllocationValue(value: string | undefined): AllocationStatus | "" {
  return ALLOCATION_STATUSES.includes(value as AllocationStatus) ? value as AllocationStatus : "";
}

function batchLogisticsValue(value: string | undefined): LogisticsStatus | "" {
  return LOGISTICS_STATUSES.includes(value as LogisticsStatus) ? value as LogisticsStatus : "";
}

function toEditForm(vehicle: PiVehicleUnit): EditableVehicleForm {
  return {
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

function parseVinText(value: string): string[] {
  return value
    .split(/[\s,;]+/)
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function toBulkFieldPayload(form: BulkVehicleForm): UpdateVehiclePayload {
  const payload: UpdateVehiclePayload = {};
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

export function OrderGeniusVehicleAllocationPage() {
  const { user } = useAuth();
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
  const [selectedPi, setSelectedPi] = useState<PiOrderDetail | null>(null);
  const [deleteConfirmPi, setDeleteConfirmPi] = useState<string | null>(null);
  // Multi-select state
  const [selectedCarCodes, setSelectedCarCodes] = useState<Set<string>>(new Set());
  const [batchForm, setBatchForm] = useState<Record<string, string>>({});
  const [batchSaving, setBatchSaving] = useState(false);
  const [selectedLineCode, setSelectedLineCode] = useState<string | null>(null);
  const [selectedVehicle, setSelectedVehicle] = useState<PiVehicleUnit | null>(null);
  const [editForm, setEditForm] = useState<EditableVehicleForm | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [loading, setLoading] = useState(false);
  const [sideLoading, setSideLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [bulkSaving, setBulkSaving] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [piForm, setPiForm] = useState<PiForm>({
    countryCode: defaultCountry,
    orderMonth: "",
    orderDate: "",
    officialPiNo: "",
    shipName: "",
    eta: "",
  });
  const [lineForm, setLineForm] = useState<LineForm>({
    materialCode: "",
    bom: "",
    brand: "",
    modelName: "",
    version: "",
    powertrain: "",
    exteriorColorName: "",
    interiorColorName: "",
    quantity: "1",
    fobEur: "",
  });
  const [bulkForm, setBulkForm] = useState<BulkVehicleForm>({
    vinText: "",
    productionDate: "",
    etd: "",
    eta: "",
    readyForPickupDate: "",
    shipName: "",
    allocationStatus: "",
    logisticsStatus: "",
  });
  const [importPreview, setImportPreview] = useState<VehicleImportPreview | null>(null);
  const [importBusy, setImportBusy] = useState(false);
  const [toolDrawerOpen, setToolDrawerOpen] = useState(false);
  const [activeToolTab, setActiveToolTab] = useState<PiToolTab>("vin");
  const [vinPasteText, setVinPasteText] = useState("");
  const [vinPasteMessage, setVinPasteMessage] = useState("");
  const [vinPasteApplying, setVinPasteApplying] = useState(false);
  const [vinFileBusy, setVinFileBusy] = useState(false);
  const [statusFlow, setStatusFlow] = useState<VehicleStatusFlowConfig | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const vinFileInputRef = useRef<HTMLInputElement>(null);

  const page = filters.page ?? 1;
  const pageSize = filters.pageSize ?? 100;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
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
    addFallbackCode(piForm.countryCode);
    addFallbackCode(selectedPi?.header.countryCode);
    selectedPi?.header.marketCountryCodes.forEach(addFallbackCode);
    return Array.from(byCode.values()).sort((a, b) => a.value.localeCompare(b.value));
  }, [accountCountryOptions, defaultCountry, filters.country, piForm.countryCode, selectedPi]);
  const vinPasteScopeVehicles = useMemo(() => {
    if (!selectedPi) {
      return [];
    }
    return selectedLineCode
      ? selectedPi.vehicles.filter((vehicle) => vehicle.piLineCode === selectedLineCode)
      : selectedPi.vehicles;
  }, [selectedPi, selectedLineCode]);

  const tableSummary = useMemo(() => {
    const vinMissing = vehicles.filter((item) => !item.vin).length;
    const ready = vehicles.filter((item) => item.logisticsStatus === "ready_for_pickup").length;
    const allocated = vehicles.filter((item) => item.allocationStatus === "allocated").length;
    return { vinMissing, ready, allocated };
  }, [vehicles]);

  useEffect(() => {
    if (!defaultCountry) {
      return;
    }
    setFilters((current) => current.country ? current : { ...current, country: defaultCountry });
    setPiForm((current) => current.countryCode ? current : { ...current, countryCode: defaultCountry });
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
          setError(err instanceof Error ? err.message : "加载车辆失败");
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
    api.getVehicleAllocationPis({
      country: filters.country,
      month: piForm.orderMonth,
      page: 1,
      pageSize: 50,
    })
      .then((res) => {
        if (!cancelled) {
          setPiHeaders(res.items);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setPiHeaders([]);
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
  }, [filters.country, piForm.orderMonth, refreshKey]);

  function updateFilter<K extends keyof VehicleAllocationFilters>(
    key: K,
    value: VehicleAllocationFilters[K],
  ): void {
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
      page: 1,
    }));
    setSelectedVehicle(null);
    setEditForm(null);
  }

  function openPiTool(tab: PiToolTab): void {
    setActiveToolTab(tab);
    setToolDrawerOpen(true);
  }

  async function applyVinPaste(vins: string[]): Promise<void> {
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
      setError(err instanceof Error ? err.message : "VIN 填充失败");
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
    try {
      const result = await api.searchVehicleAllocation(keyword);
      if (result.type === "pi" && isPiDetail(result.item)) {
        const detail = result.item;
        setPiVehicleScope(detail, detail.lines[0]?.piLineCode ?? null);
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
      setNotice("No match");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "搜索失败");
    }
  }

  async function selectPi(piCode: string): Promise<void> {
    setError(null);
    setNotice(null);
    try {
      const detail = await api.getVehicleAllocationPi(piCode);
      setPiVehicleScope(detail, detail.lines[0]?.piLineCode ?? null);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "加载 PI 失败");
    }
  }

  function selectLineScope(lineCode: string | null): void {
    if (!selectedPi) {
      return;
    }
    setSelectedLineCode(lineCode);
    setFilters((current) => ({
      ...current,
      piCode: selectedPi.header.piCode,
      piLineCode: lineCode ?? undefined,
      carCode: undefined,
      page: 1,
    }));
    setSelectedVehicle(null);
    setEditForm(null);
  }

  function selectVehicle(vehicle: PiVehicleUnit): void {
    setSelectedVehicle(vehicle);
    setEditForm(toEditForm(vehicle));
  }

  async function saveVehicle(): Promise<void> {
    if (!selectedVehicle || !editForm) {
      return;
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
      setNotice(`Saved ${next.carCode}`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "保存失败");
    } finally {
      setSaving(false);
    }
  }

  async function createPi(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setSaving(true);
    setError(null);
    try {
      const header = await api.createVehicleAllocationPi({
        countryCode: piForm.countryCode.trim().toUpperCase(),
        orderMonth: piForm.orderMonth,
        orderDate: cleanText(piForm.orderDate),
        officialPiNo: cleanText(piForm.officialPiNo),
        shipName: cleanText(piForm.shipName),
        eta: cleanText(piForm.eta),
      });
      const detail = await api.getVehicleAllocationPi(header.piCode);
      setPiVehicleScope(detail, detail.lines[0]?.piLineCode ?? null);
      setPiForm((current) => ({ ...current, officialPiNo: "", shipName: "", eta: "" }));
      setRefreshKey((key) => key + 1);
      setNotice(`Created ${header.piCode}`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "创建 PI 失败");
    } finally {
      setSaving(false);
    }
  }

  async function generatePiFromSelection(): Promise<void> {
    const [yearText, monthText] = piForm.orderMonth.split("-");
    const orderYear = Number(yearText);
    const orderMonth = Number(monthText);
    if (!piForm.countryCode.trim() || !Number.isFinite(orderYear) || !Number.isFinite(orderMonth)) {
      setError("Country and Month are required");
      return;
    }
    setGenerating(true);
    setError(null);
    try {
      const result = await api.generateVehicleAllocationFromOrderMatrix({
        countryCode: piForm.countryCode.trim().toUpperCase(),
        orderYear,
        orderMonth,
        officialPiNo: cleanText(piForm.officialPiNo),
        orderDate: cleanText(piForm.orderDate),
        shipName: cleanText(piForm.shipName),
        eta: cleanText(piForm.eta),
      });
      const detail = await api.getVehicleAllocationPi(result.piCode);
      setPiVehicleScope(detail, detail.lines[0]?.piLineCode ?? null);
      setRefreshKey((key) => key + 1);
      setNotice(`Generated ${result.piCode}: ${result.lineCount} lines / ${result.vehicleCount} cars`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "生成 PI 失败");
    } finally {
      setGenerating(false);
    }
  }

  async function handleDeletePi(piCode: string): Promise<void> {
    setError(null);
    try {
      await api.deleteVehicleAllocationPi(piCode);
      setDeleteConfirmPi(null);
      setSelectedPi(null);
      setRefreshKey((key) => key + 1);
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Delete failed"); }
  }

  async function handleDeleteLine(piLineCode: string): Promise<void> {
    setError(null);
    try {
      await api.deleteVehicleAllocationLine(piLineCode);
      setRefreshKey((key) => key + 1);
      if (selectedPi) {
        const detail = await api.getVehicleAllocationPi(selectedPi.header.piCode);
        setPiVehicleScope(detail, detail.lines[0]?.piLineCode ?? null);
      }
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Delete line failed"); }
  }

  function toggleVehicleSelect(carCode: string) {
    setSelectedCarCodes((prev) => {
      const next = new Set(prev);
      if (next.has(carCode)) next.delete(carCode); else next.add(carCode);
      return next;
    });
  }

  function toggleSelectAll() {
    const allCodes = vehicles.map((v) => v.carCode);
    if (allCodes.every((c) => selectedCarCodes.has(c))) {
      setSelectedCarCodes(new Set());
    } else {
      setSelectedCarCodes(new Set(allCodes));
    }
  }

  async function applyBatchToSelected(): Promise<void> {
    if (selectedCarCodes.size === 0) return;
    const fields = Object.fromEntries(
      Object.entries(batchForm).filter(([, v]) => v && v !== "Keep"),
    );
    if (Object.keys(fields).length === 0) {
      setError("请填写至少一个批量字段");
      return;
    }
    setBatchSaving(true);
    setError(null);
    try {
      const result = await api.bulkUpdateVehicleAllocationVehicles({
        piCode: selectedPi?.header.piCode ?? undefined,
        piLineCode: selectedLineCode ?? undefined,
        carCodes: Array.from(selectedCarCodes),
        fields,
      });
      setSelectedCarCodes(new Set());
      setBatchForm({});
      const detail = selectedPi ? await api.getVehicleAllocationPi(selectedPi.header.piCode) : null;
      if (detail) setSelectedPi(detail);
      setRefreshKey((key) => key + 1);
      setNotice(`Updated ${result.updatedUnits}/${result.matchedUnits} vehicles`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "批量更新失败");
    } finally {
      setBatchSaving(false);
    }
  }

  async function createLine(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (!selectedPi) {
      setError("先选择 PI");
      return;
    }
    const quantity = Number(lineForm.quantity || 0);
    if (!Number.isFinite(quantity) || quantity < 0) {
      setError("Quantity must be non-negative");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      const createdLine = await api.createVehicleAllocationLine(selectedPi.header.piCode, {
        materialCode: cleanText(lineForm.materialCode),
        bom: cleanText(lineForm.bom),
        brand: cleanText(lineForm.brand),
        modelName: cleanText(lineForm.modelName),
        version: cleanText(lineForm.version),
        powertrain: cleanText(lineForm.powertrain),
        exteriorColorName: cleanText(lineForm.exteriorColorName),
        interiorColorName: cleanText(lineForm.interiorColorName),
        quantity,
        fobEur: cleanText(lineForm.fobEur),
      });
      const detail = await api.getVehicleAllocationPi(selectedPi.header.piCode);
      setPiVehicleScope(detail, createdLine.piLineCode);
      setLineForm((current) => ({
        ...current,
        materialCode: "",
        bom: "",
        brand: "",
        modelName: "",
        version: "",
        powertrain: "",
        exteriorColorName: "",
        interiorColorName: "",
        quantity: "1",
        fobEur: "",
      }));
      setRefreshKey((key) => key + 1);
      setNotice("Line created and Car Codes generated");
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "创建 PI Line 失败");
    } finally {
      setSaving(false);
    }
  }

  async function handleImportFile(event: ChangeEvent<HTMLInputElement>): Promise<void> {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    setImportBusy(true);
    setError(null);
    setImportPreview(null);
    try {
      const sourceKind = detectVehicleImportSource(file);
      if (sourceKind === "parsedRows") {
        const payload = await parseVehicleImportRowsPayload(file);
        const preview = await api.previewVehicleAllocationParsedRows(payload);
        setImportPreview(preview);
        setNotice(`Preview ${preview.totalRows} parsed rows`);
        return;
      }
      if (sourceKind !== "spreadsheet") {
        const preview = buildUnsupportedVehicleImportPreview(file.name, sourceKind);
        setImportPreview(preview);
        setNotice(sourceKind === "image" ? "Image preview needs parser setup" : "Unsupported import file");
        return;
      }
      const preview = await api.previewVehicleAllocationImport(file);
      setImportPreview(preview);
      setNotice(`Preview ${preview.totalRows} rows`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "导入预览失败");
    } finally {
      setImportBusy(false);
      event.target.value = "";
    }
  }

  async function handleVinListFile(event: ChangeEvent<HTMLInputElement>): Promise<void> {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }
    setVinFileBusy(true);
    setError(null);
    setVinPasteMessage("");
    try {
      const result = await api.extractVehicleAllocationVinList(file);
      const nextText = result.vins.join("\n");
      setVinPasteText(nextText);
      setActiveToolTab("vin");
      setToolDrawerOpen(true);
      const message = result.vins.length > 0
        ? `Loaded ${result.vins.length} VIN candidates from ${result.fileName || file.name}`
        : `No VIN candidates found in ${result.fileName || file.name}`;
      setVinPasteMessage(message);
      setNotice(message);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "VIN 文件解析失败");
    } finally {
      setVinFileBusy(false);
      event.target.value = "";
    }
  }

  async function applyImport(): Promise<void> {
    if (!importPreview || importPreview.status === "error") {
      return;
    }
    setImportBusy(true);
    setError(null);
    try {
      const result = await api.applyVehicleAllocationImport(importPreview.importId);
      setNotice(`Imported ${result.createdUnits} new / ${result.updatedUnits} updated`);
      setImportPreview(null);
      setRefreshKey((key) => key + 1);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "导入应用失败");
    } finally {
      setImportBusy(false);
    }
  }

  async function exportCurrentView(): Promise<void> {
    setExporting(true);
    setError(null);
    try {
      const blob = await api.exportVehicleAllocation(filters);
      const country = filters.country || "ALL";
      buildDownload(blob, `Vehicle_Allocation_${country}_${new Date().toISOString().slice(0, 10)}.xlsx`);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "导出失败");
    } finally {
      setExporting(false);
    }
  }

  async function applyBulkUpdate(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (!selectedPi) {
      setError("先选择 PI");
      return;
    }
    const vinList = parseVinText(bulkForm.vinText);
    const fields = toBulkFieldPayload(bulkForm);
    if (vinList.length === 0 && Object.keys(fields).length === 0) {
      setError("请填写批量字段。VIN 请通过 PI Tools 的 VIN paste 预览后应用。");
      return;
    }
    setBulkSaving(true);
    setError(null);
    try {
      const result = await api.bulkUpdateVehicleAllocationVehicles({
        piCode: selectedPi.header.piCode,
        piLineCode: selectedLineCode ?? undefined,
        vinList,
        fields,
      });
      const detail = await api.getVehicleAllocationPi(selectedPi.header.piCode);
      setSelectedPi(detail);
      setSelectedVehicle(null);
      setEditForm(null);
      setRefreshKey((key) => key + 1);
      setBulkForm((current) => ({ ...current, vinText: "" }));
      setNotice(
        `Updated ${result.updatedUnits}/${result.matchedUnits} vehicles`
        + (result.vinAssigned ? ` · ${result.vinAssigned} VINs` : ""),
      );
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "批量更新失败");
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
            placeholder="PI Code / Car Code / VIN"
            title="Search by PI Code, Car Code, or VIN"
          />
          <LoadingActionButton type="submit" title="Load the matching PI or vehicle">
            Search
          </LoadingActionButton>
        </form>
      </div>

      {(error || notice) && (
        <div className={`va-message ${error ? "is-error" : "is-notice"}`}>
          {error || notice}
        </div>
      )}

      <DeckFloatingDrawer
        open={toolDrawerOpen}
        onOpenChange={setToolDrawerOpen}
        triggerPrimary="PI Tools"
        triggerSecondaryOpen="Close tools"
        triggerSecondaryClosed={selectedPi ? `${selectedPi.summary.totalUnits ?? 0} vehicles` : "Open tools"}
        title="Vehicle allocation tools"
        eyebrow="Order Genius"
        ariaLabel="PI vehicle allocation tools"
        className="vehicle-allocation-tool-drawer"
        panelClassName="vehicle-allocation-tool-panel"
      >
        <DeckControlTabs
          tabs={PI_TOOL_TABS}
          activeKey={activeToolTab}
          onChange={setActiveToolTab}
          ariaLabel="PI vehicle allocation tools"
        />
        <div className="va-tool-tab-body">
          {activeToolTab === "status" ? (
            <VehicleStatusBoard
              scopeLabel={activeScopeLabel}
              vehicles={vinPasteScopeVehicles}
              statusFlow={statusFlow}
            />
          ) : null}
          {activeToolTab === "vin" ? (
            <VinPasteDigestPanel
              scopeLabel={activeScopeLabel}
              vehicles={vinPasteScopeVehicles}
              pasteText={vinPasteText}
              applying={vinPasteApplying}
              fileBusy={vinFileBusy}
              applyMessage={vinPasteMessage}
              onPasteTextChange={setVinPasteText}
              onPickFile={() => vinFileInputRef.current?.click()}
              onApply={(vins) => void applyVinPaste(vins)}
            />
          ) : null}
          {activeToolTab === "import" ? (
            <VehicleImportDigestPanel
              preview={importPreview}
              busy={importBusy}
              exporting={exporting}
              onPickFile={() => fileInputRef.current?.click()}
              onApply={() => void applyImport()}
              onExport={() => void exportCurrentView()}
              onClear={() => setImportPreview(null)}
            />
          ) : null}
          {activeToolTab === "batch" ? (
            <section className="va-tool-card">
              <div className="va-tool-card-head">
                <span>Batch maintain</span>
                <strong>{selectedCarCodes.size} selected</strong>
              </div>
              <p>
                Use Bulk Maintain for the current PI or PI line scope. Use the selected-vehicle bar when updates
                must target explicit car codes.
              </p>
              <div className="va-tool-status-grid">
                <div><span>Scope</span><strong>{activeScopeLabel}</strong></div>
                <div><span>Selected</span><strong>{selectedCarCodes.size}</strong></div>
              </div>
            </section>
          ) : null}
        </div>
      </DeckFloatingDrawer>

      <div className="va-layout">
        <aside className="va-side">
          <section className="va-panel">
            <div className="va-panel-head">
              <h2>PI</h2>
              <span>{sideLoading ? "Loading" : `${piHeaders.length}`}</span>
            </div>
            <form className="va-form" onSubmit={createPi}>
              <div className="va-form-row">
                <label>Country</label>
                <CommandSelect
                  value={normalizeCountryCode(piForm.countryCode)}
                  options={countryCommandOptions}
                  placeholder="Country"
                  searchPlaceholder="Search country..."
                  onChange={(value) => setPiForm((current) => ({
                    ...current,
                    countryCode: value,
                  }))}
                />
              </div>
              <div className="va-form-row">
                <label>Month</label>
                <input
                  type="month"
                  value={piForm.orderMonth}
                  onChange={(event) => setPiForm((current) => ({ ...current, orderMonth: event.target.value }))}
                />
              </div>
              <div className="va-form-row">
                <label>Official PI</label>
                <input
                  value={piForm.officialPiNo}
                  onChange={(event) => setPiForm((current) => ({ ...current, officialPiNo: event.target.value }))}
                />
              </div>
              <div className="va-form-row">
                <label>Order Date</label>
                <input
                  type="date"
                  value={piForm.orderDate}
                  onChange={(event) => setPiForm((current) => ({ ...current, orderDate: event.target.value }))}
                />
              </div>
              <div className="va-form-row">
                <label>Ship</label>
                <input
                  value={piForm.shipName}
                  onChange={(event) => setPiForm((current) => ({ ...current, shipName: event.target.value }))}
                />
              </div>
              <div className="va-form-row">
                <label>ETA</label>
                <input
                  type="date"
                  value={piForm.eta}
                  onChange={(event) => setPiForm((current) => ({ ...current, eta: event.target.value }))}
                />
              </div>
              <div className="va-button-row">
                <LoadingActionButton
                  type="submit"
                  loading={saving}
                  loadingLabel="Creating..."
                  disabled={generating || !piForm.countryCode || !piForm.orderMonth}
                >
                  Create PI
                </LoadingActionButton>
                <LoadingActionButton
                  variant="secondary"
                  loading={generating}
                  loadingLabel="Generating..."
                  onClick={() => void generatePiFromSelection()}
                  disabled={saving || !piForm.countryCode || !piForm.orderMonth}
                  title="Generate remaining PI lines and car codes from the selected country/month order matrix"
                >
                  Generate
                </LoadingActionButton>
              </div>
            </form>
            <div className="va-pi-list">
              {piHeaders.map((pi) => (
                <button
                  type="button"
                  key={pi.piCode}
                  className={selectedPi?.header.piCode === pi.piCode ? "is-active" : ""}
                  onClick={() => void selectPi(pi.piCode)}
                  title={`Account ${display(pi.orderingAccountCode)} · Markets ${marketCountriesText(pi)}`}
                >
                  <span>{pi.piCode}</span>
                  <small>{display(pi.officialPiNo)} · {display(pi.orderingAccountCode)} · {marketCountriesText(pi)} · {statusText(pi.status)}</small>
                </button>
              ))}
            </div>
          </section>

          <section className="va-panel">
            <div className="va-panel-head">
              <h2>PI Tools</h2>
              <span>{importBusy ? "Busy" : "Digest ready"}</span>
            </div>
            <input
              ref={fileInputRef}
              type="file"
              accept=".xlsx,.xls,.xlsm,.json,application/json,image/png,image/jpeg,image/webp,image/heic,image/heif"
              onChange={(event) => void handleImportFile(event)}
              hidden
            />
            <input
              ref={vinFileInputRef}
              type="file"
              accept=".xlsx,.xls,.xlsm"
              onChange={(event) => void handleVinListFile(event)}
              hidden
            />
            <div className="va-side-tool-card">
              <p>Use the floating tool drawer for VIN paste, file digest, status board and batch maintain.</p>
              <div className="va-side-tool-actions">
                <LoadingActionButton
                  type="button"
                  variant="secondary"
                  loading={importBusy}
                  loadingLabel="Opening..."
                  onClick={() => openPiTool("import")}
                >
                  Import / Export
                </LoadingActionButton>
                <LoadingActionButton
                  type="button"
                  variant="secondary"
                  onClick={() => openPiTool("vin")}
                >
                  VIN Paste
                </LoadingActionButton>
              </div>
              {importPreview ? (
                <span className="va-side-tool-digest">
                  Preview loaded · {importPreview.totalRows} rows
                </span>
              ) : null}
            </div>
          </section>
        </aside>

        <main className="va-main">
          <section className="va-filters">
            <CommandSelect
              value={normalizeCountryCode(filters.country)}
              options={countryCommandOptions}
              placeholder="Country"
              searchPlaceholder="Search country..."
              allowClear
              onChange={(value) => updateFilter("country", value || undefined)}
            />
            <input
              value={filters.piCode ?? ""}
              onChange={(event) => updateFilter("piCode", event.target.value.toUpperCase())}
              placeholder="PI Code"
              title="Filter vehicles by PI Code"
            />
            <input
              value={filters.carCode ?? ""}
              onChange={(event) => updateFilter("carCode", event.target.value.toUpperCase())}
              placeholder="Car Code"
              title="Filter by generated Car Code"
            />
            <input
              value={filters.vin ?? ""}
              onChange={(event) => updateFilter("vin", event.target.value.toUpperCase())}
              placeholder="VIN"
              title="Filter by VIN"
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
                setFilters({ country: defaultCountry, page: 1, pageSize: 100 });
                setSelectedPi(null);
                setSelectedLineCode(null);
                setSelectedVehicle(null);
                setEditForm(null);
              }}
            >
              Reset
            </LoadingActionButton>
          </section>

          <section className="va-stats">
            <div><span>Total</span><strong>{total}</strong></div>
            <div><span>Allocated</span><strong>{tableSummary.allocated}</strong></div>
            <div><span>VIN Missing</span><strong>{tableSummary.vinMissing}</strong></div>
            <div><span>Ready</span><strong>{tableSummary.ready}</strong></div>
          </section>

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
                  <span>{selectedPi.summary.totalUnits ?? 0} units</span>
                  <span>{selectedPi.summary.vinMissing ?? 0} no VIN</span>
                  <span>{selectedPi.summary.readyForPickup ?? 0} ready</span>
                  {deleteConfirmPi === selectedPi.header.piCode ? (
                    <span style={{ display: "flex", gap: 4, alignItems: "center" }}>
                      <span style={{ fontSize: 12, color: "#dc2626", fontWeight: 600 }}>Delete?</span>
                      <button type="button" className="btn btn-sm btn-primary" style={{ background: "#dc2626", padding: "2px 10px", fontSize: 11 }}
                        onClick={() => handleDeletePi(selectedPi.header.piCode)}>Yes</button>
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
                  )}
                </div>
              </div>
              <form className="va-bulk-panel" onSubmit={(event) => void applyBulkUpdate(event)}>
                <div className="va-bulk-head">
                  <div>
                    <h3>Bulk Maintain</h3>
                    <span title="Current batch edit scope">{activeScopeLabel}</span>
                  </div>
                  <LoadingActionButton
                    type="submit"
                    loading={bulkSaving}
                    loadingLabel="Applying..."
                  >
                    Apply Batch
                  </LoadingActionButton>
                </div>
                <div className="va-bulk-vin-entry">
                  <div>
                    <strong>VIN filling uses preview digest</strong>
                    <span>Paste VINs, check duplicate/overflow rows, then apply to {activeScopeLabel}.</span>
                  </div>
                  <button type="button" onClick={() => openPiTool("vin")}>
                    Open VIN Paste
                  </button>
                </div>
                <div className="va-bulk-fields">
                  <label>Production<input type="date" value={bulkForm.productionDate} onChange={(event) => setBulkForm((current) => ({ ...current, productionDate: event.target.value }))} /></label>
                  <label>ETD<input type="date" value={bulkForm.etd} onChange={(event) => setBulkForm((current) => ({ ...current, etd: event.target.value }))} /></label>
                  <label>ETA<input type="date" value={bulkForm.eta} onChange={(event) => setBulkForm((current) => ({ ...current, eta: event.target.value }))} /></label>
                  <label>Ready Pickup<input type="date" value={bulkForm.readyForPickupDate} onChange={(event) => setBulkForm((current) => ({ ...current, readyForPickupDate: event.target.value }))} /></label>
                  <label>Ship<input value={bulkForm.shipName} onChange={(event) => setBulkForm((current) => ({ ...current, shipName: event.target.value }))} /></label>
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
              <form className="va-line-form" onSubmit={createLine}>
                <input value={lineForm.materialCode} onChange={(event) => setLineForm((current) => ({ ...current, materialCode: event.target.value.toUpperCase() }))} placeholder="Material Code" />
                <input value={lineForm.bom} onChange={(event) => setLineForm((current) => ({ ...current, bom: event.target.value }))} placeholder="BOM" />
                <input value={lineForm.brand} onChange={(event) => setLineForm((current) => ({ ...current, brand: event.target.value }))} placeholder="Brand" />
                <input value={lineForm.modelName} onChange={(event) => setLineForm((current) => ({ ...current, modelName: event.target.value }))} placeholder="Model" />
                <input value={lineForm.version} onChange={(event) => setLineForm((current) => ({ ...current, version: event.target.value }))} placeholder="Version" />
                <input value={lineForm.powertrain} onChange={(event) => setLineForm((current) => ({ ...current, powertrain: event.target.value }))} placeholder="Powertrain" />
                <input value={lineForm.exteriorColorName} onChange={(event) => setLineForm((current) => ({ ...current, exteriorColorName: event.target.value }))} placeholder="Exterior" />
                <input value={lineForm.interiorColorName} onChange={(event) => setLineForm((current) => ({ ...current, interiorColorName: event.target.value }))} placeholder="Interior" />
                <input value={lineForm.fobEur} onChange={(event) => setLineForm((current) => ({ ...current, fobEur: event.target.value }))} placeholder="FOB" inputMode="decimal" />
                <input value={lineForm.quantity} onChange={(event) => setLineForm((current) => ({ ...current, quantity: event.target.value }))} placeholder="Qty" inputMode="numeric" />
                <LoadingActionButton type="submit" loading={saving} loadingLabel="Adding...">
                  Add Line
                </LoadingActionButton>
              </form>
              {selectedPi.lines.length > 0 ? (
                <div className="va-line-list">
                  <div className={`va-line-row va-line-all ${selectedLineCode === null ? "is-active" : ""}`}>
                    <button type="button" className="va-line-body" onClick={() => selectLineScope(null)}
                      title="Use the whole PI as the vehicle table and batch-edit scope">
                      <strong>All PI</strong>
                      <span>{selectedPi.header.piCode} · Qty {selectedPi.summary.totalUnits ?? 0}</span>
                      <small>{selectedPi.summary.vinMissing ?? 0} no VIN</small>
                    </button>
                  </div>
                  {selectedPi.lines.map((line) => (
                    <div
                      key={line.piLineCode}
                      className={`va-line-row ${selectedLineCode === line.piLineCode ? "is-active" : ""}`}
                    >
                      <button type="button" className="va-line-body" onClick={() => selectLineScope(line.piLineCode)}
                        title="Line quantity and market split generated from order matrix allocation">
                        <strong>{line.piLineCode}</strong>
                        <span>{display(line.materialCode)} · {display(line.modelName)} / {display(line.version)} · Qty {line.quantity}</span>
                        <small>
                          {(line.allocations ?? []).length > 0
                            ? (line.allocations ?? []).map((allocation) => `${allocation.marketCountryCode} ${allocation.quantity}`).join(" · ")
                            : `Market ${selectedPi.header.countryCode} ${line.quantity}`}
                        </small>
                      </button>
                      <button
                        type="button"
                        className="va-line-delete"
                        title="Delete this line"
                        onClick={() => void handleDeleteLine(line.piLineCode)}
                      >✕</button>
                    </div>
                  ))}
                </div>
              ) : null}
            </section>
          )}

          {/* ── Batch action bar (visible when vehicles selected) ── */}
          {selectedCarCodes.size > 0 && (
            <div style={{
              display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap",
              padding: "8px 12px", background: "#eff6ff", border: "1px solid #93c5fd",
              borderRadius: 6, marginBottom: 8, fontSize: 13,
            }}>
              <span style={{ fontWeight: 600, color: "#1e40af" }}>{selectedCarCodes.size} selected</span>
              <input type="date" placeholder="ETD" value={batchForm.etd ?? ""}
                onChange={(e) => setBatchForm((f) => ({ ...f, etd: e.target.value }))}
                style={{ padding: "3px 6px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db" }} />
              <input type="date" placeholder="ETA" value={batchForm.eta ?? ""}
                onChange={(e) => setBatchForm((f) => ({ ...f, eta: e.target.value }))}
                style={{ padding: "3px 6px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db" }} />
              <input type="text" placeholder="Ship name" value={batchForm.shipName ?? ""}
                onChange={(e) => setBatchForm((f) => ({ ...f, shipName: e.target.value }))}
                style={{ padding: "3px 6px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db", width: 120 }} />
              <input type="text" placeholder="Dealer code" value={batchForm.dealerCode ?? ""}
                onChange={(e) => setBatchForm((f) => ({ ...f, dealerCode: e.target.value }))}
                style={{ padding: "3px 6px", fontSize: 12, borderRadius: 4, border: "1px solid #d1d5db", width: 100 }} />
              <CommandSelect
                value={batchAllocationValue(batchForm.allocationStatus)}
                options={allocationStatusOptions}
                placeholder="Alloc..."
                searchPlaceholder="Search allocation..."
                allowClear
                compact
                className="va-inline-command-select"
                onChange={(value) => setBatchForm((current) => ({ ...current, allocationStatus: value }))}
              />
              <CommandSelect
                value={batchLogisticsValue(batchForm.logisticsStatus)}
                options={logisticsStatusOptions}
                placeholder="Logi..."
                searchPlaceholder="Search logistics..."
                allowClear
                compact
                className="va-inline-command-select"
                onChange={(value) => setBatchForm((current) => ({ ...current, logisticsStatus: value }))}
              />
              <LoadingActionButton
                size="sm"
                loading={batchSaving}
                loadingLabel="Applying..."
                onClick={applyBatchToSelected}
              >
                Apply to {selectedCarCodes.size}
              </LoadingActionButton>
              <button type="button" className="btn btn-sm btn-ghost"
                onClick={() => { setSelectedCarCodes(new Set()); setBatchForm({}); }} style={{ fontSize: 12 }}>
                Clear
              </button>
            </div>
          )}

          <section className="va-table-wrap">
            <div className="va-table-head">
              <span>{loading ? "Loading vehicles" : selectedPi ? `${vehicles.length} shown · ${activeScopeLabel}` : "Select a PI"}</span>
              <div>
                <button type="button" disabled={page <= 1} onClick={() => updateFilter("page", page - 1)}>Prev</button>
                <span>{page} / {totalPages}</span>
                <button type="button" disabled={page >= totalPages} onClick={() => updateFilter("page", page + 1)}>Next</button>
              </div>
            </div>
            <div className="va-table-scroll">
              <table className="va-table">
                <thead>
                  <tr>
                    <th style={{ width: 32 }}>
                      <input type="checkbox"
                        checked={vehicles.length > 0 && vehicles.every((v) => selectedCarCodes.has(v.carCode))}
                        onChange={toggleSelectAll}
                        style={{ margin: 0 }} />
                    </th>
                    <th>Car Code</th>
                    <th>VIN</th>
                    <th>PI</th>
                    <th>Country</th>
                    <th>Material</th>
                    <th>Config</th>
                    <th>Exterior</th>
                    <th>Interior</th>
                    <th>Allocation</th>
                    <th>Logistics</th>
                    <th>Ship</th>
                    <th>ETA</th>
                    <th>Ready</th>
                  </tr>
                </thead>
                <tbody>
                  {vehicles.map((vehicle) => {
                    const isChecked = selectedCarCodes.has(vehicle.carCode);
                    return (
                    <tr
                      key={vehicle.carCode}
                      className={selectedVehicle?.carCode === vehicle.carCode ? "is-selected" : ""}
                    >
                      <td>
                        <input type="checkbox" checked={isChecked}
                          onChange={() => toggleVehicleSelect(vehicle.carCode)}
                          onClick={(e) => e.stopPropagation()}
                          style={{ margin: 0 }} />
                      </td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{vehicle.carCode}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.vin)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{vehicle.piCode}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{vehicle.countryCode}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.materialCode)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.modelName)} / {display(vehicle.version)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.exteriorColorName)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.interiorColorName)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}><span className={`va-status va-status-${vehicle.allocationStatus}`}>{statusText(vehicle.allocationStatus)}</span></td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}><span className={`va-status va-status-${vehicle.logisticsStatus}`}>{statusText(vehicle.logisticsStatus)}</span></td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.shipName)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.eta)}</td>
                      <td onClick={() => selectVehicle(vehicle)} style={{ cursor: "pointer" }}>{display(vehicle.readyForPickupDate)}</td>
                    </tr>
                  );})}
                  {!loading && vehicles.length === 0 && (
                    <tr>
                      <td colSpan={14} className="va-empty">{selectedPi ? "No vehicles in current scope" : "Select a PI to view vehicles"}</td>
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
            <label className="va-wide">Remark<textarea value={editForm.remark} onChange={(event) => setEditForm((current) => current ? { ...current, remark: event.target.value } : current)} /></label>
          </div>
          <LoadingActionButton
            className="va-save"
            loading={saving}
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
        .va-search input,.va-filters input,.va-form input,.va-line-form input,.va-bulk-panel textarea,.va-bulk-fields input,.va-drawer input,.va-drawer textarea{border:1px solid #cfd6df;background:#fff;color:#111827;border-radius:6px;padding:9px 10px;min-width:0}
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
        .va-side,.va-main{display:flex;flex-direction:column;gap:16px}
        .va-panel,.va-filters,.va-stats,.va-pi-detail,.va-table-wrap{background:#fff;border:1px solid #d8dee6;border-radius:8px}
        .va-panel{padding:14px}
        .va-panel-head,.va-table-head,.va-pi-title,.va-drawer-head{display:flex;align-items:center;justify-content:space-between;gap:12px}
        .va-panel-head h2,.va-pi-title h2,.va-drawer h2{font-size:15px;margin:0}
        .va-panel-head span,.va-table-head span,.va-pi-title span,.va-drawer-head span{color:#667085;font-size:12px}
        .va-form{display:grid;gap:10px;margin-top:12px}
        .va-form-row{display:grid;gap:4px}
        .va-form-row label{font-size:12px;font-weight:700;color:#475467}
        .va-pi-list{display:grid;gap:8px;margin-top:12px;max-height:280px;overflow:auto}
        .va-pi-list button{background:#fff;color:#111827;border-color:#d8dee6;text-align:left;display:grid;gap:2px}
        .va-pi-list button.is-active{border-color:#1c69d4;background:#eef5ff}
        .va-pi-list small{color:#667085}
        .va-button-row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px}
        .va-side-tool-card{display:grid;gap:12px;margin-top:12px;padding:12px;border:1px solid #e2e8f0;border-radius:8px;background:#f8fafc}
        .va-side-tool-card p{margin:0;color:#64748b;font-size:12px;line-height:1.45}
        .va-side-tool-actions{display:grid;grid-template-columns:1fr;gap:8px}
        .va-side-tool-digest{display:inline-flex;align-items:center;width:max-content;border:1px solid #bfdbfe;border-radius:999px;background:#eff6ff;color:#1d4ed8;padding:4px 8px;font-size:11px;font-weight:800}
        .va-import-preview{display:grid;gap:10px;margin-top:12px;border-top:1px solid #e5eaf0;padding-top:12px}
        .va-preview-grid{display:grid;grid-template-columns:1fr auto;gap:6px;font-size:12px}
        .va-preview-grid span{color:#667085}
        .va-preview-error{font-size:12px;color:#a8071a}
        .va-filters{display:grid;grid-template-columns:repeat(6,minmax(112px,1fr));gap:10px;padding:12px}
        .va-check{display:flex;align-items:center;gap:6px;min-height:38px;font-size:12px;color:#475467}
        .va-stats{display:grid;grid-template-columns:repeat(4,1fr)}
        .va-stats div{padding:14px 16px;border-right:1px solid #e5eaf0}
        .va-stats div:last-child{border-right:none}
        .va-stats span{display:block;color:#667085;font-size:12px}
        .va-stats strong{font-size:22px}
        .va-pi-detail{padding:14px}
        .va-pi-metrics{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end}
        .va-pi-metrics span{border:1px solid #d8dee6;border-radius:999px;padding:4px 8px;background:#f8fafc}
        .va-bulk-panel{display:grid;gap:10px;margin-top:12px;border:1px solid #d8dee6;border-radius:8px;background:#fbfcfe;padding:12px}
        .va-bulk-head{display:flex;align-items:center;justify-content:space-between;gap:12px}
        .va-bulk-head h3{font-size:14px;margin:0}
        .va-bulk-head span{font-size:12px;color:#667085}
        .va-bulk-panel textarea{min-height:90px;resize:vertical;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
        .va-bulk-vin-entry{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:12px;border:1px solid #dbe6f4;border-radius:8px;background:#f5f9ff}
        .va-bulk-vin-entry div{display:grid;gap:3px}
        .va-bulk-vin-entry strong{font-size:13px;color:#1e3a8a}
        .va-bulk-vin-entry span{font-size:12px;color:#64748b}
        .va-bulk-vin-entry button{background:#fff;color:#1c69d4;border-color:#bfdbfe;white-space:nowrap}
        .va-bulk-fields{display:grid;grid-template-columns:repeat(7,minmax(112px,1fr));gap:8px}
        .va-bulk-fields label,.va-bulk-fields .va-field{display:grid;gap:5px;font-size:12px;font-weight:700;color:#475467}
        .va-line-form{display:grid;grid-template-columns:repeat(10,minmax(82px,1fr)) auto;gap:8px;margin-top:12px}
        .va-line-list{display:grid;gap:6px;margin-top:12px}
        .va-line-row{display:flex;align-items:stretch;border:1px solid #e5eaf0;border-radius:6px;background:#fbfcfe;color:#111827;overflow:hidden}
        .va-line-row.is-active{border-color:#1c69d4;background:#eef5ff}
        .va-line-all{background:#fff}
        .va-line-body{display:grid;grid-template-columns:170px minmax(0,1fr) minmax(150px,auto);gap:8px;align-items:center;flex:1;padding:8px;border:none;background:none;cursor:pointer;text-align:left;color:inherit;font:inherit}
        .va-line-body strong{font-size:12px}
        .va-line-body span,.va-line-body small{font-size:12px;color:#667085;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .va-line-delete{display:flex;align-items:center;justify-content:center;width:32px;flex-shrink:0;border:none;border-left:1px solid #e5eaf0;background:#f9fafb;color:#d1d5db;cursor:pointer;font-size:14px;padding:0}
        .va-line-delete:hover{background:#fef2f2;color:#ef4444;border-left-color:#fecaca}
        .va-table-wrap{overflow:hidden}
        .va-table-head{padding:10px 12px;border-bottom:1px solid #e5eaf0}
        .va-table-head div{display:flex;gap:8px;align-items:center}
        .va-table-head button{padding:6px 10px}
        .va-table-scroll{overflow:auto;max-height:620px}
        .va-table{width:100%;border-collapse:collapse;min-width:1280px}
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
        .va-inline-command-select{width:112px}
        .va-drawer textarea{min-height:88px;resize:vertical}
        .va-wide{grid-column:1/-1}
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
        .vehicle-import-digest-panel .upload-digest-panel{background:rgba(255,255,255,.86);border-color:rgba(203,213,225,.86);box-shadow:none}
        .vehicle-import-digest-panel.is-compact .upload-digest-panel{padding:12px}
        .vehicle-import-digest-panel.is-compact .upload-digest-metrics{grid-template-columns:repeat(2,minmax(0,1fr))}
        .vehicle-import-digest-panel.is-compact .upload-digest-head p{display:none}
        .vehicle-import-actions{display:flex;align-items:center;justify-content:flex-end;flex-wrap:wrap;gap:8px;width:100%}
        .vehicle-import-preview-table{display:grid;min-width:0;border:1px solid #e2e8f0;border-radius:6px;overflow:hidden}
        .vehicle-import-preview-head,.vehicle-import-preview-row{display:grid;grid-template-columns:54px 78px minmax(138px,1fr) minmax(152px,1.1fr) minmax(128px,1fr) minmax(160px,1.5fr);gap:8px;align-items:center;padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px}
        .vehicle-import-preview-head{background:#f8fafc;color:#475467;font-weight:800;letter-spacing:.06em;text-transform:uppercase}
        .vehicle-import-preview-row strong{text-transform:uppercase;color:#1d4ed8}
        .vehicle-import-preview-row span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
        .vehicle-import-preview-row.is-error{background:#fff7f7;color:#b91c1c}
        .vehicle-import-preview-empty{padding:14px;color:#64748b;font-size:12px;text-align:center}
        @media (max-width:1100px){
          .va-layout{grid-template-columns:1fr}
          .va-header{align-items:stretch;flex-direction:column}
          .va-search{min-width:0}
          .va-filters{grid-template-columns:repeat(2,minmax(0,1fr))}
          .va-line-form,.va-bulk-fields{grid-template-columns:repeat(2,minmax(0,1fr))}
          .va-line-row{grid-template-columns:1fr}
          .va-stats{grid-template-columns:repeat(2,1fr)}
        }
        @media (max-width:640px){
          .vehicle-allocation-page{padding:14px}
          .va-filters,.va-line-form,.va-bulk-fields,.va-drawer-grid{grid-template-columns:1fr}
          .va-stats{grid-template-columns:1fr}
          .va-stats div{border-right:none;border-bottom:1px solid #e5eaf0}
          .va-drawer{top:0;width:100%}
        }
      `}</style>
    </div>
  );
}
