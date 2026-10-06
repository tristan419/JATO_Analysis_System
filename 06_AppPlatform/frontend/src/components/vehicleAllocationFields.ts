import type { PiVehicleUnit } from "../types/orderGeniusVehicle";

export type VehicleColumnKey = keyof PiVehicleUnit | "config" | "cocPdf";
export type VehicleTextField = "vin" | "productionDate" | "etd" | "eta" | "actualDepartureDate" | "actualArrivalDate" | "readyForPickupDate" | "shipName" | "dealerCode" | "dealerName" | "customerRef" | "remark";
export interface VehicleColumn {
  key: VehicleColumnKey;
  label: string;
  optional?: boolean;
  kind?: "date" | "number" | "values";
  editOrder?: number;
}
// The actual allocation fields, shared by table, filters, editor and export labels.
export const VEHICLE_COLUMNS: VehicleColumn[] = [
  { key: "carCode", label: "Car Code" }, { key: "vin", label: "VIN", editOrder: 0 },
  { key: "piCode", label: "PI" }, { key: "countryCode", label: "Country", kind: "values" },
  { key: "materialCode", label: "Material" }, { key: "config", label: "Config" },
  { key: "exteriorColorName", label: "Exterior", kind: "values" },
  { key: "interiorColorName", label: "Interior", kind: "values" },
  { key: "fobEur", label: "FOB (EUR)", kind: "number" },
  { key: "freightEur", label: "Freight / 运费 (EUR)", optional: true, kind: "number" },
  { key: "insuranceEur", label: "Insurance / 保费 (EUR)", optional: true, kind: "number" },
  { key: "cocPdf", label: "COC PDF", optional: true, kind: "values" },
  { key: "allocationStatus", label: "Allocation", kind: "values" },
  { key: "logisticsStatus", label: "Logistics", kind: "values" },
  { key: "shipName", label: "Ship", editOrder: 8 },
  { key: "eta", label: "ETA", kind: "date", editOrder: 3 },
  { key: "readyForPickupDate", label: "Ready for pickup / 可提车", kind: "date", editOrder: 6 },
  { key: "productionDate", label: "Production", optional: true, kind: "date", editOrder: 1 },
  { key: "etd", label: "ETD", optional: true, kind: "date", editOrder: 2 },
  { key: "actualDepartureDate", label: "Actual departure", optional: true, kind: "date", editOrder: 4 },
  { key: "actualArrivalDate", label: "Actual arrival", optional: true, kind: "date", editOrder: 5 },
  { key: "dealerCode", label: "Dealer code", optional: true, editOrder: 9 },
  { key: "dealerName", label: "Dealer name", optional: true, editOrder: 10 },
  { key: "customerRef", label: "Customer ref", optional: true, editOrder: 11 },
  { key: "remark", label: "Note / 备注", optional: true, editOrder: 12 },
];
export const COLUMN_GROUPS = [
  { label: "Identity / 车辆", keys: ["carCode", "vin", "piCode", "countryCode", "materialCode", "config", "exteriorColorName", "interiorColorName"] },
  { label: "Price & documents / 价格与文件", keys: ["fobEur", "freightEur", "insuranceEur", "cocPdf", "remark"] },
  { label: "Status & dates / 状态与日期", keys: ["allocationStatus", "logisticsStatus", "shipName", "eta", "readyForPickupDate", "productionDate", "etd", "actualDepartureDate", "actualArrivalDate", "dealerCode", "dealerName", "customerRef"] },
];
export const VEHICLE_TEXT_FIELDS = VEHICLE_COLUMNS.filter((column): column is VehicleColumn & { key: VehicleTextField; editOrder: number } => column.editOrder !== undefined)
  .sort((a, b) => a.editOrder - b.editOrder);
const SEARCH_KEYS: Array<keyof PiVehicleUnit> = ["brand", "bom", "modelName", "version", "powertrain",
  ...VEHICLE_COLUMNS.filter((column): column is VehicleColumn & { key: keyof PiVehicleUnit } => column.key !== "config" && column.key !== "cocPdf" && column.kind !== "number" && column.kind !== "date").map((column) => column.key)];
export function matchesVehicleText(vehicle: PiVehicleUnit, query: string): boolean {
  return SEARCH_KEYS.some((key) => String(vehicle[key] ?? "").toLowerCase().includes(query));
}
