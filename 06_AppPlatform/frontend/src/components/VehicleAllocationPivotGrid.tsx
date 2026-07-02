import { useEffect, useMemo, useState } from "react";
import {
  AllCommunityModule,
  ModuleRegistry,
  themeAlpine,
  type ColDef,
  type ICellRendererParams,
  type RowClickedEvent,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import type { PiVehicleUnit } from "../types/orderGeniusVehicle";

ModuleRegistry.registerModules([AllCommunityModule]);

type PivotRowType = "pi" | "line" | "vehicle";

interface VehicleAllocationPivotRow {
  id: string;
  rowType: PivotRowType;
  piCode: string;
  piLineCode: string;
  carCode: string;
  vin: string;
  countryCode: string;
  materialCode: string;
  config: string;
  modelName: string;
  version: string;
  powertrain: string;
  exteriorColorName: string;
  interiorColorName: string;
  allocationStatus: string;
  logisticsStatus: string;
  shipName: string;
  eta: string;
  orderMonth: string;
  groupLabel: string;
  groupMeta: string;
  childCount: number;
  source?: PiVehicleUnit;
}

interface PivotColumnOption {
  key: keyof VehicleAllocationPivotRow;
  label: string;
}

interface VehicleAllocationPivotGridProps {
  vehicles: PiVehicleUnit[];
  selectedCarCode?: string | null;
  onSelectVehicle: (vehicle: PiVehicleUnit) => void | Promise<void>;
}

const COLUMN_OPTIONS: PivotColumnOption[] = [
  { key: "vin", label: "VIN" },
  { key: "piCode", label: "PI" },
  { key: "piLineCode", label: "PI Line" },
  { key: "countryCode", label: "Country" },
  { key: "materialCode", label: "Material" },
  { key: "config", label: "Config" },
  { key: "exteriorColorName", label: "Exterior" },
  { key: "interiorColorName", label: "Interior" },
  { key: "allocationStatus", label: "Allocation" },
  { key: "logisticsStatus", label: "Logistics" },
  { key: "shipName", label: "Ship" },
  { key: "eta", label: "ETA" },
];

const DEFAULT_HIDDEN_COLUMNS = new Set<keyof VehicleAllocationPivotRow>([
  "piCode",
  "piLineCode",
  "shipName",
  "eta",
]);

function text(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return String(value);
}

function statusLabel(value: string): string {
  return value.replaceAll("_", " ");
}

function vehicleConfig(vehicle: PiVehicleUnit): string {
  return [vehicle.modelName, vehicle.version, vehicle.powertrain]
    .filter(Boolean)
    .join(" / ");
}

function groupVehicles(vehicles: PiVehicleUnit[]): Map<string, Map<string, PiVehicleUnit[]>> {
  const groups = new Map<string, Map<string, PiVehicleUnit[]>>();
  for (const vehicle of vehicles) {
    const lineGroups = groups.get(vehicle.piCode) ?? new Map<string, PiVehicleUnit[]>();
    const rows = lineGroups.get(vehicle.piLineCode) ?? [];
    rows.push(vehicle);
    lineGroups.set(vehicle.piLineCode, rows);
    groups.set(vehicle.piCode, lineGroups);
  }
  return groups;
}

function buildRows(
  vehicles: PiVehicleUnit[],
  expandedGroups: ReadonlySet<string>,
): VehicleAllocationPivotRow[] {
  const grouped = groupVehicles(vehicles);
  const rows: VehicleAllocationPivotRow[] = [];
  for (const [piCode, lineGroups] of grouped) {
    const lineCount = lineGroups.size;
    const vehicleCount = Array.from(lineGroups.values()).reduce((sum, items) => sum + items.length, 0);
    const piKey = `pi:${piCode}`;
    rows.push({
      id: piKey,
      rowType: "pi",
      piCode,
      piLineCode: "",
      carCode: "",
      vin: "",
      countryCode: "",
      materialCode: "",
      config: "",
      modelName: "",
      version: "",
      powertrain: "",
      exteriorColorName: "",
      interiorColorName: "",
      allocationStatus: "",
      logisticsStatus: "",
      shipName: "",
      eta: "",
      orderMonth: "",
      groupLabel: piCode,
      groupMeta: `${lineCount} PI lines · ${vehicleCount} cars`,
      childCount: vehicleCount,
    });
    if (!expandedGroups.has(piKey)) {
      continue;
    }
    for (const [piLineCode, lineVehicles] of lineGroups) {
      const first = lineVehicles[0];
      const lineKey = `line:${piLineCode}`;
      rows.push({
        id: lineKey,
        rowType: "line",
        piCode,
        piLineCode,
        carCode: "",
        vin: "",
        countryCode: first?.countryCode ?? "",
        materialCode: first?.materialCode ?? "",
        config: first ? vehicleConfig(first) : "",
        modelName: first?.modelName ?? "",
        version: first?.version ?? "",
        powertrain: first?.powertrain ?? "",
        exteriorColorName: first?.exteriorColorName ?? "",
        interiorColorName: first?.interiorColorName ?? "",
        allocationStatus: "",
        logisticsStatus: "",
        shipName: first?.shipName ?? "",
        eta: first?.eta ?? "",
        orderMonth: first?.orderMonth ?? "",
        groupLabel: piLineCode,
        groupMeta: `${text(first?.materialCode)} · ${text(first?.modelName)} / ${text(first?.version)} · ${lineVehicles.length} cars`,
        childCount: lineVehicles.length,
      });
      if (!expandedGroups.has(lineKey)) {
        continue;
      }
      for (const vehicle of lineVehicles) {
        rows.push({
          id: `vehicle:${vehicle.carCode}`,
          rowType: "vehicle",
          piCode: vehicle.piCode,
          piLineCode: vehicle.piLineCode,
          carCode: vehicle.carCode,
          vin: vehicle.vin ?? "",
          countryCode: vehicle.countryCode,
          materialCode: vehicle.materialCode ?? "",
          config: vehicleConfig(vehicle),
          modelName: vehicle.modelName ?? "",
          version: vehicle.version ?? "",
          powertrain: vehicle.powertrain ?? "",
          exteriorColorName: vehicle.exteriorColorName ?? "",
          interiorColorName: vehicle.interiorColorName ?? "",
          allocationStatus: vehicle.allocationStatus,
          logisticsStatus: vehicle.logisticsStatus,
          shipName: vehicle.shipName ?? "",
          eta: vehicle.eta ?? "",
          orderMonth: vehicle.orderMonth ?? "",
          groupLabel: vehicle.carCode,
          groupMeta: text(vehicle.vin),
          childCount: 0,
          source: vehicle,
        });
      }
    }
  }
  return rows;
}

function initialExpandedGroups(vehicles: PiVehicleUnit[]): Set<string> {
  const groups = new Set<string>();
  const grouped = groupVehicles(vehicles);
  for (const [piCode, lineGroups] of grouped) {
    groups.add(`pi:${piCode}`);
    for (const piLineCode of lineGroups.keys()) {
      groups.add(`line:${piLineCode}`);
    }
  }
  return groups;
}

export function VehicleAllocationPivotGrid({
  vehicles,
  selectedCarCode,
  onSelectVehicle,
}: VehicleAllocationPivotGridProps) {
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(() => initialExpandedGroups(vehicles));
  const [hiddenColumns, setHiddenColumns] = useState<Set<keyof VehicleAllocationPivotRow>>(() => new Set(DEFAULT_HIDDEN_COLUMNS));

  useEffect(() => {
    setExpandedGroups(initialExpandedGroups(vehicles));
  }, [vehicles]);

  const rowData = useMemo(
    () => buildRows(vehicles, expandedGroups),
    [expandedGroups, vehicles],
  );

  const columnDefs = useMemo<ColDef<VehicleAllocationPivotRow>[]>(() => {
    const cols: ColDef<VehicleAllocationPivotRow>[] = [
      {
        headerName: "Car Code / Group",
        colId: "group",
        pinned: "left",
        minWidth: 240,
        flex: 1,
        cellClass: (params) => params.data?.rowType === "vehicle" ? "va-pivot-car-cell" : "va-pivot-group-cell",
        cellRenderer: (params: ICellRendererParams<VehicleAllocationPivotRow, unknown>) => {
          const row = params.data;
          if (!row) {
            return null;
          }
          if (row.rowType === "vehicle") {
            return (
              <button type="button" className="va-pivot-vehicle-link">
                <strong>{row.carCode}</strong>
                <small>{text(row.vin)}</small>
              </button>
            );
          }
          const expanded = expandedGroups.has(row.id);
          return (
            <button
              type="button"
              className={`va-pivot-group-toggle va-pivot-group-${row.rowType}`}
              onClick={(event) => {
                event.stopPropagation();
                setExpandedGroups((current) => {
                  const next = new Set(current);
                  if (next.has(row.id)) {
                    next.delete(row.id);
                  } else {
                    next.add(row.id);
                  }
                  return next;
                });
              }}
            >
              <span aria-hidden="true">{expanded ? "▾" : "▸"}</span>
              <strong>{row.groupLabel}</strong>
              <small>{row.groupMeta}</small>
            </button>
          );
        },
      },
    ];

    for (const option of COLUMN_OPTIONS) {
      cols.push({
        headerName: option.label,
        field: option.key,
        hide: hiddenColumns.has(option.key),
        minWidth: option.key === "config" ? 240 : 120,
        flex: option.key === "config" ? 1 : undefined,
        valueFormatter: (params) => {
          const value = String(params.value ?? "");
          if (option.key === "allocationStatus" || option.key === "logisticsStatus") {
            return statusLabel(value);
          }
          return text(value);
        },
        cellClass: (params) => {
          if (params.data?.rowType !== "vehicle") {
            return "va-pivot-muted-cell";
          }
          if (option.key === "allocationStatus" || option.key === "logisticsStatus") {
            return "va-pivot-status-cell";
          }
          return undefined;
        },
      });
    }
    return cols;
  }, [expandedGroups, hiddenColumns]);

  function toggleColumn(key: keyof VehicleAllocationPivotRow): void {
    setHiddenColumns((current) => {
      const next = new Set(current);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }

  function handleRowClicked(event: RowClickedEvent<VehicleAllocationPivotRow>): void {
    if (event.data?.rowType === "vehicle" && event.data.source) {
      void onSelectVehicle(event.data.source);
    }
  }

  return (
    <section className={`va-pivot-panel${rowData.length > 0 ? " has-rows" : ""}`}>
      <div className="va-pivot-toolbar">
        <div>
          <strong>PI Search Pivot</strong>
          <span>{vehicles.length} vehicles · PI / PI line collapsible</span>
        </div>
        <div className="va-column-pills" aria-label="Toggle vehicle allocation columns">
          {COLUMN_OPTIONS.map((option) => (
            <button
              key={option.key}
              type="button"
              className={hiddenColumns.has(option.key) ? "" : "is-active"}
              onClick={() => toggleColumn(option.key)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>
      <div className="va-pivot-grid">
        <AgGridReact<VehicleAllocationPivotRow>
          theme={themeAlpine}
          rowData={rowData}
          columnDefs={columnDefs}
          getRowId={(params) => params.data.id}
          onRowClicked={handleRowClicked}
          suppressNoRowsOverlay={rowData.length > 0}
          defaultColDef={{
            resizable: true,
            sortable: true,
            filter: true,
          }}
          rowClassRules={{
            "is-pi-group": (params) => params.data?.rowType === "pi",
            "is-line-group": (params) => params.data?.rowType === "line",
            "is-selected-vehicle": (params) => params.data?.carCode === selectedCarCode,
          }}
          domLayout="normal"
          suppressCellFocus={false}
        />
      </div>
    </section>
  );
}
