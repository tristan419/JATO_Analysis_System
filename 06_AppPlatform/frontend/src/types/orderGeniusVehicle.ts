export type PiStatus =
  | "draft"
  | "ordered"
  | "in_production"
  | "shipped"
  | "arrived"
  | "ready_for_pickup"
  | "closed"
  | "cancelled";

export type AllocationStatus =
  | "unallocated"
  | "reserved"
  | "allocated"
  | "delivered"
  | "cancelled";

export type LogisticsStatus =
  | "pending"
  | "in_production"
  | "ready_for_shipping"
  | "on_vessel"
  | "arrived_at_port"
  | "in_warehouse"
  | "ready_for_pickup"
  | "delivered";

export interface VehicleStatusFlowStep {
  key: AllocationStatus | LogisticsStatus | string;
  labelEn: string;
  labelZh: string;
  order: number;
  color: string;
  icon: string;
  terminal: boolean;
  allowedTransitions: string[];
}

export interface VehicleStatusFlowConfig {
  countryCode: string | null;
  orderingAccountCode: string | null;
  source: "default" | "country" | "ordering_account" | string;
  logistics: VehicleStatusFlowStep[];
  allocation: VehicleStatusFlowStep[];
}

export interface PiOrderHeader {
  piId: string;
  piCode: string;
  officialPiNo: string | null;
  countryCode: string;
  countryName: string | null;
  orderingAccountCode: string;
  orderingAccountName: string | null;
  marketCountryCodes: string[];
  shipmentBatchCode: string | null;
  portOfDischarge: string | null;
  orderDate: string | null;
  orderMonth: string;
  piSequenceNo: number;
  shippingScheduleUrl: string | null;
  feishuTrackingUrl: string | null;
  shipName: string | null;
  etd: string | null;
  eta: string | null;
  actualDepartureDate: string | null;
  actualArrivalDate: string | null;
  readyForPickupDate: string | null;
  status: PiStatus;
  remark: string | null;
  rowVersion: number;
  createdAtUtc: string | null;
  updatedAtUtc: string | null;
}

export interface PiOrderLine {
  piLineId: string;
  piCode: string;
  piLineCode: string;
  lineSequenceNo: number;
  materialCode: string | null;
  bom: string | null;
  brand: string | null;
  modelName: string | null;
  version: string | null;
  powertrain: string | null;
  exteriorColorName: string | null;
  exteriorColorCode: string | null;
  interiorColorName: string | null;
  interiorColourCode: string | null;
  quantity: number;
  fobEur: number | null;
  amountEur: number | null;
  remark: string | null;
  rowVersion: number;
  allocations?: PiLineAllocation[];
}

export interface PiLineAllocation {
  piLineAllocationId: string;
  piCode: string;
  piLineCode: string;
  marketCountryCode: string;
  orderYear: number;
  orderMonth: number;
  materialCode: string | null;
  quantity: number;
  fobEur: number | null;
}

export interface PiVehicleUnit {
  vehicleUnitId: string;
  piCode: string;
  officialPiNo: string | null;
  orderingAccountCode: string | null;
  orderingAccountName: string | null;
  shipmentBatchCode: string | null;
  portOfDischarge: string | null;
  piLineCode: string;
  carCode: string;
  vin: string | null;
  materialCode: string | null;
  bom: string | null;
  brand: string | null;
  modelName: string | null;
  version: string | null;
  powertrain: string | null;
  exteriorColorName: string | null;
  exteriorColorCode: string | null;
  interiorColorName: string | null;
  interiorColourCode: string | null;
  orderDate: string | null;
  productionDate: string | null;
  etd: string | null;
  eta: string | null;
  actualDepartureDate: string | null;
  actualArrivalDate: string | null;
  readyForPickupDate: string | null;
  shipName: string | null;
  countryCode: string;
  dealerCode: string | null;
  dealerName: string | null;
  customerRef: string | null;
  allocationStatus: AllocationStatus;
  logisticsStatus: LogisticsStatus;
  shippingScheduleUrl: string | null;
  feishuTrackingUrl: string | null;
  remark: string | null;
  rowVersion: number;
}

export interface VehicleAllocationSummary {
  totalUnits: number;
  allocated: number;
  reserved: number;
  unallocated: number;
  vinAssigned: number;
  vinMissing: number;
  onVessel: number;
  arrived: number;
  readyForPickup: number;
}

export interface PiOrderDetail {
  header: PiOrderHeader;
  lines: PiOrderLine[];
  summary: VehicleAllocationSummary;
  vehicles: PiVehicleUnit[];
  vehicleTotal: number;
}

export interface VehicleAllocationListResponse<T> {
  items: T[];
  total: number;
}

export interface VehicleAllocationFilters {
  keyword?: string;
  piCode?: string;
  piLineCode?: string;
  carCode?: string;
  vin?: string;
  materialCode?: string;
  bom?: string;
  country?: string;
  shipName?: string;
  allocationStatus?: AllocationStatus | "";
  logisticsStatus?: LogisticsStatus | "";
  etaFrom?: string;
  etaTo?: string;
  readyFrom?: string;
  readyTo?: string;
  vinMissingOnly?: boolean;
  unallocatedOnly?: boolean;
  page?: number;
  pageSize?: number;
}

export interface PiOrderFilters {
  country?: string;
  month?: string;
  status?: PiStatus | "";
  keyword?: string;
  page?: number;
  pageSize?: number;
}

export interface VehicleAllocationSearchResult {
  type: "empty" | "pi" | "vehicle";
  item: PiOrderDetail | PiVehicleUnit | null;
}

export interface VehicleAllocationLineItem {
  materialCode: string | null;
  quantity: number;
  fobEur: number | null;
  bom?: string | null;
  brand?: string | null;
  modelName?: string | null;
  version?: string | null;
  powertrain?: string | null;
  exteriorColorName?: string | null;
  exteriorColorCode?: string | null;
  interiorColorName?: string | null;
  interiorColourCode?: string | null;
  allocations?: VehicleAllocationLineItemAllocation[];
}

export interface VehicleAllocationLineItemAllocation {
  countryCode: string;
  quantity: number;
  fobEur?: number | null;
}

export interface VehicleAllocationPlanLine {
  materialCode: string | null;
  quantity?: number;
  fobEur?: number | null;
  bom?: string | null;
  brand?: string | null;
  modelName?: string | null;
  version?: string | null;
  powertrain?: string | null;
  exteriorColorName?: string | null;
  exteriorColorCode?: string | null;
  interiorColorName?: string | null;
  interiorColourCode?: string | null;
  selectedQuantity: number;
  generatedQuantity: number;
  generatedVehicleCount: number;
  remainingQuantity: number;
  overGeneratedQuantity: number;
}

export interface VehicleAllocationPlanTotals {
  selectedQuantity: number;
  generatedQuantity: number;
  generatedVehicleCount: number;
  remainingQuantity: number;
  overGeneratedQuantity: number;
}

export interface VehicleAllocationPlan {
  countryCode: string;
  year: number;
  month: number;
  orderMonth: string;
  lineItems: VehicleAllocationPlanLine[];
  selectedLineItems: VehicleAllocationLineItem[];
  remainingLineItems: VehicleAllocationLineItem[];
  existingLines: Array<PiOrderLine | PiLineAllocation>;
  totals: VehicleAllocationPlanTotals;
  status: "complete" | "pending";
}

export interface VehicleImportPreviewRow {
  sourceRow: number;
  action: "create" | "update";
  piCode: string | null;
  carCode: string | null;
  vin: string | null;
  materialCode: string | null;
  warnings: string[];
  errors: string[];
}

export interface VehicleImportPreview {
  importId: string;
  totalRows: number;
  newHeaders: number;
  newLines: number;
  newUnits: number;
  updatedUnits: number;
  warnings: string[];
  errors: string[];
  previewRows: VehicleImportPreviewRow[];
  status: "ok" | "error";
}

export type VehicleImportParsedRow = Record<string, unknown>;

export interface VehicleImportParsedRowsPayload {
  rows: VehicleImportParsedRow[];
  source?: string | null;
}

export interface VehicleImportResult {
  createdUnits: number;
  updatedUnits: number;
  warnings: string[];
}

export interface VehicleVinListExtract {
  fileName: string | null;
  totalRows: number;
  vins: string[];
  uploadedBy: string;
}

export interface UpdateVehiclePayload {
  vin?: string | null;
  productionDate?: string | null;
  etd?: string | null;
  eta?: string | null;
  actualDepartureDate?: string | null;
  actualArrivalDate?: string | null;
  readyForPickupDate?: string | null;
  shipName?: string | null;
  dealerCode?: string | null;
  dealerName?: string | null;
  customerRef?: string | null;
  allocationStatus?: AllocationStatus;
  logisticsStatus?: LogisticsStatus;
  remark?: string | null;
}

export interface BulkVehicleUpdatePayload {
  piCode?: string;
  piLineCode?: string;
  carCodes?: string[];
  vinList?: string[];
  fields?: UpdateVehiclePayload;
}

export interface BulkVehicleUpdateResult {
  piCode: string;
  piLineCode: string | null;
  matchedUnits: number;
  updatedUnits: number;
  vinAssigned: number;
  fieldsUpdated: string[];
}
