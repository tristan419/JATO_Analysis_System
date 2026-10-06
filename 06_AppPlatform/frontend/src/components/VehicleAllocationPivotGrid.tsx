import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { AllCommunityModule, ModuleRegistry, themeAlpine, type ColDef, type ColGroupDef, type GridApi, type ICellRendererParams } from "ag-grid-community";
import { AgGridReact, useGridFilter, type CustomFilterProps } from "ag-grid-react";
import { CommandMultiSelect } from "./CommandSelect";
import type { PiVehicleUnit } from "../types/orderGeniusVehicle";
import { type VehicleColumnKey, type VehicleColumn } from "./vehicleAllocationFields";
import type { PiCocLookup } from "../types/cocLibrary";

ModuleRegistry.registerModules([AllCommunityModule]);
export type { VehicleColumnKey, VehicleColumn } from "./vehicleAllocationFields";
export interface VehicleGridView { vehicles: PiVehicleUnit[]; ordinaryVehicles: PiVehicleUnit[]; columns: string[] }
const DEFAULT_COLUMN: ColDef<PiVehicleUnit> = { sortable: true, resizable: true, minWidth: 100 };
const SELECTION_COLUMN: ColDef<PiVehicleUnit> = { pinned: "left", width: 45, minWidth: 45, maxWidth: 45, suppressMovable: true };
interface Props {
  vehicles: PiVehicleUnit[];
  columns: VehicleColumn[];
  groups: Array<{ label: string; keys: string[] }>;
  visibleKeys: ReadonlySet<VehicleColumnKey>;
  selectedCodes: ReadonlySet<string>;
  busy: boolean;
  resetKey: number;
  filterScope: string;
  cocStatuses: ReadonlyMap<string, PiCocLookup["items"][number]["status"]>;
  renderCell: (vehicle: PiVehicleUnit, key: VehicleColumnKey) => ReactNode;
  onEdit: (vehicle: PiVehicleUnit) => void;
  onSelection: (codes: Set<string>) => void;
  onFilterChange: () => void;
  onViewChange: (view: VehicleGridView) => void;
}
function cellValue(vehicle: PiVehicleUnit, key: VehicleColumnKey, cocStatuses: Props["cocStatuses"]): string | number | null {
  if (key === "config") return [vehicle.modelName, vehicle.version, vehicle.powertrain].filter(Boolean).join(" / ");
  if (key === "cocPdf") return cocStatuses.get(vehicle.carCode) ?? "not_searched";
  const value = vehicle[key];
  return typeof value === "string" || typeof value === "number" ? value : null;
}
function ValueListFilter({ api, getValue, model, onModelChange }: CustomFilterProps<PiVehicleUnit, unknown, string[]>) {
  const [values, setValues] = useState<string[]>([]);
  useEffect(() => {
    function refreshValues(): void {
      const unique = new Set<string>();
      api.forEachNode((node) => unique.add(String(getValue(node) ?? "")));
      const next = [...unique].sort();
      setValues((current) => current.length === next.length && current.every((value, index) => value === next[index]) ? current : next);
    }
    refreshValues();
    api.addEventListener("modelUpdated", refreshValues);
    return () => {
      api.removeEventListener("modelUpdated", refreshValues);
    };
  }, [api, getValue]);
  useGridFilter({ doesFilterPass: ({ node }) => model === null || model.includes(String(getValue(node) ?? "")) });
  return <div style={{ minWidth: 260, padding: 12 }}>
    <CommandMultiSelect selected={model ?? values} options={values.map((value) => ({ value, label: value || "(Blank)" }))} onChange={onModelChange} placeholder="Filter values / 筛选值" />
    <button type="button" onClick={() => onModelChange(null)}>All values / 全部值</button>
  </div>;
}
export function VehicleAllocationGrid({ vehicles, columns, groups, visibleKeys, selectedCodes, busy, resetKey, filterScope, cocStatuses, renderCell, onEdit, onSelection, onFilterChange, onViewChange }: Props) {
  const grid = useRef<AgGridReact<PiVehicleUnit>>(null);
  const syncingSelection = useRef(false);
  const renderer = useRef(renderCell);
  renderer.current = renderCell;
  const cocValues = useRef(cocStatuses);
  cocValues.current = cocStatuses;
  const ordinaryRows = useRef<PiVehicleUnit[]>([]);
  const [showSelected, setShowSelected] = useState(false);
  const [filteredCount, setFilteredCount] = useState(vehicles.length);
  // Stable definitions keep header drag/width/filter state across selection and editing.
  const definitions = useMemo<Array<ColGroupDef<PiVehicleUnit>>>(() => groups.map((group) => ({
    headerName: group.label, groupId: group.label, marryChildren: false, openByDefault: true,
    children: columns.filter((column) => group.keys.includes(column.key)).map((column): ColDef<PiVehicleUnit> => ({
      colId: column.key, headerName: column.label, hide: column.optional ?? false,
      columnGroupShow: column.key === group.keys[0] ? undefined : "open",
      valueGetter: (params) => params.data ? cellValue(params.data, column.key, cocValues.current) : null,
      filter: column.kind === "number" ? "agNumberColumnFilter" : column.kind === "date" ? "agDateColumnFilter" : column.kind === "values" ? ValueListFilter : "agTextColumnFilter",
      filterParams: column.kind === "date" ? { comparator: (filterDate: Date, cell: string | null) => {
        const target = `${filterDate.getFullYear()}-${String(filterDate.getMonth() + 1).padStart(2, "0")}-${String(filterDate.getDate()).padStart(2, "0")}`;
        return (cell ?? "").slice(0, 10).localeCompare(target);
      } } : undefined,
      cellRenderer: (params: ICellRendererParams<PiVehicleUnit>) => params.data ? renderer.current(params.data, column.key) : null,
      width: column.key === "config" || column.key === "carCode" ? 255 : column.key === "materialCode" ? 195 : 170,
    })),
  })), [columns, groups]);
  function rows(api: GridApi<PiVehicleUnit>): PiVehicleUnit[] {
    const result: PiVehicleUnit[] = [];
    api.forEachNodeAfterFilterAndSort((node) => { if (node.data) result.push(node.data); });
    return result;
  }
  function publish(api: GridApi<PiVehicleUnit>): void {
    const result = rows(api);
    if (!showSelected) ordinaryRows.current = result;
    setFilteredCount(result.length);
    onViewChange({ vehicles: result, ordinaryVehicles: ordinaryRows.current, columns: api.getAllDisplayedColumns().map((column) => column.getColId()).filter((key) => key !== "ag-Grid-SelectionColumn") });
  }
  useEffect(() => {
    setShowSelected(false);
  }, [filterScope]);
  useEffect(() => {
    grid.current?.api?.refreshCells({ force: true });
  }, [renderCell]);
  useEffect(() => {
    const api = grid.current?.api;
    if (!api) return;
    api.refreshClientSideRowModel("filter");
    if (api.getFilterModel().cocPdf) { setShowSelected(false); onFilterChange(); }
  }, [cocStatuses]);
  useEffect(() => {
    const api = grid.current?.api;
    if (!api) return;
    syncingSelection.current = true;
    api.forEachNode((node) => node.setSelected(Boolean(node.data && selectedCodes.has(node.data.carCode))));
    syncingSelection.current = false;
  }, [selectedCodes, vehicles, showSelected]);
  useEffect(() => {
    grid.current?.api?.applyColumnState({ state: columns.map((column) => ({ colId: column.key, hide: !visibleKeys.has(column.key) })) });
  }, [columns, visibleKeys]);
  useEffect(() => {
    const api = grid.current?.api;
    if (!api) return;
    api.resetColumnState();
    api.setFilterModel(null);
    api.applyColumnState({ state: columns.map((column) => ({ colId: column.key, hide: !visibleKeys.has(column.key) })) });
  }, [resetKey]); // Explicit reset, not selection/refresh.
  return <section className="va-grid" aria-label="Vehicle details">
    <div className="va-grid-toolbar">
      <span role="status">{filteredCount} filtered / 筛选 · {selectedCodes.size} selected / 已选</span>
      <label><input type="checkbox" checked={showSelected} disabled={busy} onChange={(event) => setShowSelected(event.target.checked)} />Show selected / 只看勾选</label>
      <button type="button" className="btn-secondary" disabled={busy} onClick={() => {
        const scope = showSelected ? ordinaryRows.current : grid.current?.api ? rows(grid.current.api) : [];
        onSelection(new Set(scope.filter((vehicle) => !selectedCodes.has(vehicle.carCode)).map((vehicle) => vehicle.carCode)));
      }}>Invert selection / 反选筛选结果</button>
    </div>
    <div style={{ height: "min(65vh, 660px)", minHeight: 300 }}>
      <AgGridReact<PiVehicleUnit>
        ref={grid}
        theme={themeAlpine}
        rowData={showSelected ? vehicles.filter((vehicle) => selectedCodes.has(vehicle.carCode)) : vehicles}
        columnDefs={definitions}
        defaultColDef={DEFAULT_COLUMN}
        maintainColumnOrder
        getRowId={(params) => params.data.carCode}
        rowSelection={{ mode: "multiRow", selectAll: "filtered", enableClickSelection: false, checkboxes: !busy, headerCheckbox: !busy }}
        selectionColumnDef={SELECTION_COLUMN}
        pagination={!showSelected}
        paginationPageSize={100}
        paginationPageSizeSelector={[50, 100, 200]}
        suppressDragLeaveHidesColumns
        onGridReady={(event) => { event.api.applyColumnState({ state: columns.map((column) => ({ colId: column.key, hide: !visibleKeys.has(column.key) })) }); publish(event.api); }}
        onModelUpdated={(event) => publish(event.api)}
        onDisplayedColumnsChanged={(event) => publish(event.api)}
        onColumnMoved={(event) => { if (event.finished) publish(event.api); }}
        onSortChanged={(event) => publish(event.api)}
        onFilterChanged={() => { setShowSelected(false); onFilterChange(); }}
        onSelectionChanged={(event) => {
          if (!syncingSelection.current && event.source !== "api") onSelection(new Set(event.api.getSelectedRows().map((vehicle) => vehicle.carCode)));
        }}
        onCellClicked={(event) => {
          if (event.data && event.column.getColId() !== "ag-Grid-SelectionColumn" && event.column.getColId() !== "cocPdf") onEdit(event.data);
        }}
        overlayNoRowsTemplate="No vehicles in this view / 当前视图无车辆"
      />
    </div>
  </section>;
}
