import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
  type ForwardedRef,
} from "react";
import {
  ModuleRegistry,
  AllCommunityModule,
  themeAlpine,
  type CellClassParams,
  type CellValueChangedEvent,
  type ColDef,
  type ICellEditorParams,
  type ICellRendererParams,
  type ValueGetterParams,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";

import { parseOrderGeniusColourSwatch } from "../utils/orderGeniusColourSwatch";

ModuleRegistry.registerModules([AllCommunityModule]);

const MONTH_NAMES = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const MONTH_NUMBERS = MONTH_NAMES.map((_, i) => i + 1);
const MODEL_COLUMN_MIN_WIDTH = 320;
const MODEL_COLUMN_CONTENT_PADDING = 88;

export interface OrderGeniusGridRow {
  materialCode: string;
  bomTemplate?: string | null;
  modelName: string;
  version: string;
  colour: string;
  colourCode?: string | null;
  colourTier?: string | null;
  colourHex?: string | null;
  interiorColorName?: string | null;
  fobEur: number | null;
  lifecycleStatus: string;
  editable: boolean;
  remark?: string;
  _countryCode?: string;
  _indent?: boolean;
  __type?: "groupHeader" | "data" | "consolidated_parent" | "summary";
  __groupLabel?: string;
  __groupMeta?: string;
  __groupColor?: string;
  __groupColSpan?: number;
  __groupKey?: string;
  __groupKind?: "trim" | "country" | "bom";
  __groupLevel?: number;
  __expanded?: boolean;
  // Flattened months: month_1..month_12
  [key: `month_${number}`]: number;
  // Precomputed monetary totals for aggregate rows.
  [key: `_amount_${number}`]: number | undefined;
  _ttlAmount?: number;
  // Row versions per month
  _versions: Record<string, number>;
  // Error messages per cell key
  _errors: Record<string, string>;
  // Saving state per cell key
  _saving: Set<string>;
}

export interface OrderGeniusGridProps {
  rows: OrderGeniusGridRow[];
  selectedMonth: number | null;
  selectedRowIds?: ReadonlySet<string>;
  piSelectionSummary?: PiSelectionSummary;
  canEditQuantities: boolean;
  visibleColumns: {
    months: boolean;
    amount: boolean;
    ttlQty: boolean;
    ttlAmount: boolean;
    fob: boolean;
    materialCode: boolean;
    remark: boolean;
  };
  showCountry: boolean;
  onCellValueChanged: (event: CellValueChangedEvent<OrderGeniusGridRow>) => void;
  onGridReady?: (api: any) => void;
  onToggleGroup?: (groupKey: string) => void;
  onTogglePiRow?: (row: OrderGeniusGridRow, selected: boolean) => void;
}

interface OrderGeniusGridContext {
  onToggleGroup?: (groupKey: string) => void;
}

interface PiSelectionSummary {
  selectedCount: number;
  selectableCount: number;
  allSelected: boolean;
  partialSelected: boolean;
  onToggleAll: (selected: boolean) => void;
}

type GroupHeaderRendererProps = ICellRendererParams<OrderGeniusGridRow, string> & {
  context?: OrderGeniusGridContext;
};

function PiSelectHeader({ summary }: { summary?: PiSelectionSummary }) {
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (inputRef.current) {
      inputRef.current.indeterminate = Boolean(summary?.partialSelected);
    }
  }, [summary?.partialSelected]);

  const disabled = !summary || summary.selectableCount === 0;
  return (
    <label
      className="og-pi-select-header"
      title={disabled ? "Select one month with positive quantities first" : `Select all ${summary.selectableCount} visible PI rows`}
    >
      <input
        ref={inputRef}
        type="checkbox"
        checked={summary?.allSelected ?? false}
        disabled={disabled}
        onChange={(event) => summary?.onToggleAll(event.currentTarget.checked)}
        aria-label="Select all PI rows"
      />
      <span>PI</span>
    </label>
  );
}

export function getOrderGeniusRowId(row: OrderGeniusGridRow): string {
  return [
    row._countryCode || "",
    row.materialCode,
    row.__groupKey || "",
    row.lifecycleStatus || "active",
    row.version || "",
    row.colour || "",
    row.interiorColorName || "",
  ].join("|");
}

export function buildOrderGeniusColumnDefs(
  showCountry: boolean,
  selectedMonth: number | null,
  vis: OrderGeniusGridProps["visibleColumns"],
  canEditQuantities: boolean,
  modelColumnWidth: number,
  piSelectionSummary?: PiSelectionSummary,
  isPiRowSelected?: (row: OrderGeniusGridRow) => boolean,
  onTogglePiRow?: (row: OrderGeniusGridRow, selected: boolean) => void,
): ColDef<OrderGeniusGridRow>[] {
  const cols: ColDef<OrderGeniusGridRow>[] = [];

  if (selectedMonth != null && onTogglePiRow) {
    const monthField = `month_${selectedMonth}` as `month_${number}`;
    cols.push({
      colId: "piSelect",
      headerName: "PI",
      headerTooltip: "Tick rows to include their selected-month quantity in PI batch creation.",
      headerComponent: () => <PiSelectHeader summary={piSelectionSummary} />,
      pinned: "left",
      width: 52,
      editable: false,
      sortable: false,
      cellClass: "og-pi-select-cell",
      cellRenderer: (params: ICellRendererParams<OrderGeniusGridRow, unknown>) => {
        const row = params.data;
        if (!row || row.__type === "groupHeader" || row.__type === "consolidated_parent" || row.__type === "summary") {
          return null;
        }
        const quantity = row[monthField] || 0;
        const disabled = quantity <= 0 || row.lifecycleStatus === "historical";
        return (
          <input
            type="checkbox"
            checked={isPiRowSelected?.(row) ?? false}
            disabled={disabled}
            onChange={(event) => onTogglePiRow(row, event.currentTarget.checked)}
            aria-label="Select PI row"
            title={disabled ? "This row has no selectable quantity for the selected month" : "Add this row to PI batch"}
          />
        );
      },
    });
  }

  if (showCountry) {
    cols.push({
      headerName: "Country",
      field: "_countryCode",
      pinned: "left",
      width: 70,
      editable: false,
      cellClass: "og-country-cell",
    });
  }

  cols.push(
    {
      headerName: "Model",
      field: "modelName",
      pinned: "left",
      width: modelColumnWidth,
      minWidth: MODEL_COLUMN_MIN_WIDTH,
      editable: false,
      cellRendererSelector: (p: any) => {
        if (p.data?.__type === "groupHeader") {
          return { component: "groupHeaderRenderer" };
        }
        return undefined;
      },
    },
    { headerName: "Version", field: "version", pinned: "left", width: 130, editable: false },
    {
      headerName: "Colour", field: "colour", pinned: "left", width: 130, editable: false,
      cellRenderer: (p: ICellRendererParams<OrderGeniusGridRow, string>) => {
        const name = String(p.value ?? "");
        if (!name) return null;
        const swatch = parseOrderGeniusColourSwatch(p.data?.colourHex);
        const code = String(p.data?.colourCode ?? "").trim();
        const title = swatch.isMissing
          ? `${name}${code ? ` (${code})` : ""} · Missing swatch`
          : `${name}${code ? ` (${code})` : ""}`;
        return (
          <span style={{ display: "flex", alignItems: "center", gap: 5 }} title={title}>
            <span
              aria-label={swatch.isMissing ? "Missing swatch" : `${name} swatch`}
              style={{
                display: "inline-block",
                width: 14,
                height: 14,
                borderRadius: 3,
                flexShrink: 0,
                border: swatch.isMissing ? "1px dashed #94a3b8" : "1px solid #d1d5db",
                background: swatch.background,
              }}
            />
            <span style={{ fontSize: 11, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>{name}</span>
          </span>
        );
      },
    },
    {
      headerName: "Interior",
      field: "interiorColorName",
      pinned: "left",
      width: 130,
      editable: false,
      cellRenderer: (p: any) => {
        const name = p.value || "";
        const ed = p.data?.editionTag;
        if (!name && !ed) return null;
        return (
          <span style={{ fontSize: 11, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
            {name}{ed ? ` · ${ed}` : ''}
          </span>
        );
      },
    },
  );

  if (vis.materialCode) {
    cols.push({
      headerName: "Material Code",
      field: "materialCode",
      pinned: "left",
      width: 150,
      editable: false,
      cellClass: "og-material-cell",
        valueFormatter: (p) => (p.data?.__type === "groupHeader" || p.data?.__type === "summary" ? "" : String(p.value ?? "")),
    });
  }

  if (vis.remark) {
    cols.push({
      headerName: "Note",
      field: "remark",
      pinned: "left",
      width: 190,
      editable: false,
      cellClass: "og-remark-cell",
      tooltipValueGetter: (p) => (p.value ? String(p.value) : ""),
      cellRenderer: (params: ICellRendererParams<OrderGeniusGridRow, string>) => {
        const value = String(params.value ?? "").trim();
        if (!value) return "";
        return <span className="og-remark-note">{value}</span>;
      },
    });
  }

  if (vis.fob) {
    cols.push({
      headerName: "FOB (EUR)",
      field: "fobEur",
      pinned: "left",
      width: 100,
      editable: false,
      type: "numericColumn",
      valueFormatter: (p) => {
        const value = Number(p.value);
        return Number.isFinite(value) && value > 0 ? value.toLocaleString() : "-";
      },
    });
  }

  const activeMonths = MONTH_NAMES.map((_, i) => i + 1).filter(
    (m) => selectedMonth == null || m === selectedMonth,
  );

  const visibleAmountTotal = (row: OrderGeniusGridRow): number =>
    activeMonths.reduce((sum, month) => {
      const monthField = `month_${month}` as `month_${number}`;
      const amountField = `_amount_${month}` as `_amount_${number}`;
      const precomputed = row[amountField];
      if (precomputed != null) return sum + precomputed;
      return sum + (row[monthField] || 0) * (row.fobEur || 0);
    }, 0);

  for (const m of activeMonths) {
    const field = `month_${m}` as const;
    const amountField = `_amount_${m}` as `_amount_${number}`;
    if (vis.months) {
      cols.push({
        headerName: MONTH_NAMES[m - 1],
        field,
        width: 72,
        type: "numericColumn",
        editable: (params: any) =>
          canEditQuantities
          && params.data != null
          && params.data.__type !== "groupHeader"
          && params.data.editable !== false,
        cellEditor: "agNumberCellEditor",
        cellEditorParams: { min: 0 },
        valueParser: (p) => {
          const parsed = Number(p.newValue);
          return Number.isFinite(parsed) ? Math.max(0, Math.floor(parsed)) : 0;
        },
        valueSetter: (p) => {
          const parsed = Number(p.newValue);
          const nextQuantity = Number.isFinite(parsed) ? Math.max(0, Math.floor(parsed)) : 0;
          const row = p.data;
          if (!row || row[field] === nextQuantity) return false;
          row[field] = nextQuantity;
          row[amountField] = nextQuantity * (row.fobEur ?? 0);
          row._ttlAmount = MONTH_NUMBERS.reduce((sum, month) => {
            const monthQuantity = row[`month_${month}`] ?? 0;
            return sum + monthQuantity * (row.fobEur ?? 0);
          }, 0);
          return true;
        },
        valueFormatter: (p) => (p.value != null ? String(p.value) : "0"),
        cellClassRules: {
          "og-cell-error": (p: CellClassParams) =>
            !!p.data?._errors?.[field as string],
          "og-cell-saving": (p: CellClassParams) =>
            p.data?._saving?.has(field as string) ?? false,
        },
      });
    }
    if (vis.amount) {
      cols.push({
        headerName: `${MONTH_NAMES[m - 1]} €`,
        field: amountField,
        width: 90,
        type: "numericColumn",
        editable: false,
        valueGetter: (p: ValueGetterParams<OrderGeniusGridRow>) => {
          const row = p.data;
          if (!row) return 0;
          const precomputed = row[amountField];
          if (precomputed != null) return precomputed;
          const qty = (row as any)[field] ?? 0;
          const fob = row.fobEur ?? 0;
          return qty * fob;
        },
        valueFormatter: (p) => (p.value != null ? (p.value as number).toLocaleString() : "0"),
      });
    }
  }

  if (vis.ttlQty) {
    cols.push({
      headerName: "TTL",
      field: "_ttl" as any,
      width: 80,
      type: "numericColumn",
      editable: false,
      valueGetter: (p: ValueGetterParams<OrderGeniusGridRow>) => {
        const row = p.data;
        if (!row) return 0;
        let t = 0;
        for (const m of activeMonths) t += (row as any)[`month_${m}`] ?? 0;
        return t;
      },
      valueFormatter: (p) => (p.value != null ? (p.value as number).toLocaleString() : "0"),
      cellClass: "og-ttl-cell",
    });
  }

  if (vis.ttlAmount) {
    cols.push({
      headerName: "TTL €",
      field: "_ttlAmount" as any,
      width: 100,
      type: "numericColumn",
      editable: false,
      valueGetter: (p: ValueGetterParams<OrderGeniusGridRow>) => {
        const row = p.data;
        if (!row) return 0;
        return visibleAmountTotal(row);
      },
      valueFormatter: (p) => (p.value != null ? (p.value as number).toLocaleString() : "0"),
      cellClass: "og-ttl-amount-cell",
    });
  }

  return cols;
}

function estimateGridTextWidth(value: string): number {
  let width = 0;
  for (const char of value) {
    if (/[\u4e00-\u9fff]/.test(char)) {
      width += 14;
    } else if (/[A-Z0-9]/.test(char)) {
      width += 8.4;
    } else if (/[a-z]/.test(char)) {
      width += 7.2;
    } else if (char === " ") {
      width += 4.5;
    } else {
      width += 5.8;
    }
  }
  return width;
}

function getModelColumnDisplayText(row: OrderGeniusGridRow): string {
  if (row.__type === "groupHeader") {
    return [row.__groupLabel, row.__groupMeta].filter(Boolean).join(" ");
  }
  return row.modelName || "";
}

function getModelColumnWidth(rows: OrderGeniusGridRow[]): number {
  const widestText = rows.reduce((width, row) => {
    const text = getModelColumnDisplayText(row);
    return Math.max(width, estimateGridTextWidth(text));
  }, 0);
  return Math.ceil(Math.max(MODEL_COLUMN_MIN_WIDTH, widestText + MODEL_COLUMN_CONTENT_PADDING));
}

/** Inline quantity editor — reads DOM value directly to avoid React batching issues. */
const QuantityCellEditor = forwardRef(
  (props: ICellEditorParams<OrderGeniusGridRow, number>, ref: ForwardedRef<unknown>) => {
    const inputRef = useRef<HTMLInputElement | null>(null);

    useEffect(() => {
      // AG Grid handles focus after mount — just select the text
      const el = inputRef.current;
      if (el) {
        el.focus();
        el.select();
      }
    }, []);

    useImperativeHandle(ref, () => ({
      // Read DOM value directly — React state may be stale due to batching
      getValue: () => {
        const raw = inputRef.current?.value ?? "";
        const n = parseInt(raw, 10);
        return isNaN(n) ? (props.value ?? 0) : n;
      },
      isCancelBeforeStart: () => false,
      isCancelAfterEnd: () => false,
    }));

    const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === "Enter") {
        e.preventDefault();
        props.stopEditing(false);
      } else if (e.key === "Escape") {
        e.preventDefault();
        props.stopEditing(true);
      } else if (e.key === "Tab") {
        // AG Grid handles Tab natively — let it bubble
      }
    };

    return (
      <input
        ref={inputRef}
        type="number"
        min={0}
        defaultValue={props.value ?? 0}
        onKeyDown={handleKeyDown}
        style={{
          width: "100%",
          height: "100%",
          textAlign: "center",
          border: "none",
          outline: "none",
          background: "transparent",
          fontSize: "inherit",
          fontFamily: "inherit",
        }}
      />
    );
  },
);

export function OrderGeniusGrid({
  rows,
  selectedMonth,
  selectedRowIds,
  piSelectionSummary,
  canEditQuantities,
  visibleColumns,
  showCountry,
  onCellValueChanged,
  onGridReady,
  onToggleGroup,
  onTogglePiRow,
}: OrderGeniusGridProps) {
  const localGridApiRef = useRef<any>(null);
  const gridWrapperRef = useRef<HTMLDivElement | null>(null);
  const selectedRowIdsRef = useRef(selectedRowIds);
  selectedRowIdsRef.current = selectedRowIds;
  const isPiRowSelected = useCallback(
    (row: OrderGeniusGridRow): boolean => selectedRowIdsRef.current?.has(getOrderGeniusRowId(row)) ?? false,
    [],
  );
  const modelColumnWidth = useMemo(
    () => getModelColumnWidth(rows),
    [rows],
  );
  const columnDefs = useMemo(
    () => buildOrderGeniusColumnDefs(
      showCountry,
      selectedMonth,
      visibleColumns,
      canEditQuantities,
      modelColumnWidth,
      piSelectionSummary,
      isPiRowSelected,
      onTogglePiRow,
    ),
    [canEditQuantities, showCountry, selectedMonth, visibleColumns, modelColumnWidth, piSelectionSummary, isPiRowSelected, onTogglePiRow],
  );

  const defaultColDef = useMemo<ColDef<OrderGeniusGridRow>>(
    () => ({
      resizable: true,
      sortable: true,
      filter: false,
      suppressHeaderMenuButton: true,
      cellClassRules: {
        "og-historical-row": (p: CellClassParams) =>
          p.data?.lifecycleStatus === "historical",
      },
    }),
    [],
  );

  const getRowId = useCallback(
    (p: { data: OrderGeniusGridRow }) =>
      getOrderGeniusRowId(p.data),
    [],
  );

  const isRowSelectable = useCallback(
    (node: any) => node.data?.__type !== "groupHeader" && node.data?.__type !== "summary",
    [],
  );

  const rowClassRules = useMemo<any>(
    () => ({
      "og-group-header-row": (p: any) => p.data?.__type === "groupHeader",
      "og-group-header-row-country": (p: any) => p.data?.__groupKind === "country",
      "og-group-header-row-bom": (p: any) => p.data?.__groupKind === "bom",
      "og-consolidated-parent": (p: any) => p.data?.__type === "consolidated_parent",
      "og-summary-row": (p: any) => p.data?.__type === "summary",
      "og-historical-row": (p: any) => p.data?.lifecycleStatus === "historical",
    }),
    [],
  );

  const pinnedBottomRowData = useMemo<OrderGeniusGridRow[]>(() => {
    if (rows.length === 0) return [];
    const topLevelHeaders = rows.filter(
      (row) => row.__type === "groupHeader" && (row.__groupLevel ?? 0) === 0,
    );
    const sourceRows = topLevelHeaders.length > 0
      ? topLevelHeaders
      : rows.filter((row) => row.__type !== "groupHeader" && row.__type !== "consolidated_parent" && row.__type !== "summary");
    if (sourceRows.length === 0) return [];

    const summary: OrderGeniusGridRow = {
      materialCode: "__sum__",
      modelName: "SUM",
      version: "",
      colour: "",
      interiorColorName: "",
      fobEur: null,
      lifecycleStatus: "active",
      editable: false,
      remark: "",
      _countryCode: showCountry ? "Σ" : undefined,
      _versions: {},
      _errors: {},
      _saving: new Set(),
      __type: "summary",
    };
    let ttlAmount = 0;
    for (const month of MONTH_NUMBERS) {
      const monthField = `month_${month}` as `month_${number}`;
      const amountField = `_amount_${month}` as `_amount_${number}`;
      const quantity = sourceRows.reduce((sum, row) => sum + (row[monthField] || 0), 0);
      const amount = sourceRows.reduce((sum, row) => {
        const precomputed = row[amountField];
        if (precomputed != null) return sum + precomputed;
        return sum + (row[monthField] || 0) * (row.fobEur || 0);
      }, 0);
      summary[monthField] = quantity;
      summary[amountField] = amount;
      ttlAmount += amount;
    }
    summary._ttlAmount = ttlAmount;
    return [summary];
  }, [rows, showCountry]);

  useEffect(() => {
    if (!localGridApiRef.current) return;
    localGridApiRef.current.refreshCells({ force: true, columns: ["piSelect"] });
  }, [selectedRowIds]);

  const gridContext = useMemo<OrderGeniusGridContext>(
    () => ({ onToggleGroup }),
    [onToggleGroup],
  );

  useEffect(() => {
    const root = gridWrapperRef.current;
    if (!root || !onToggleGroup) return undefined;
    const handleGroupMouseDown = (event: MouseEvent) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const trigger = target.closest<HTMLElement>("[data-og-group-key]");
      if (!trigger || !root.contains(trigger)) return;
      const groupKey = trigger.dataset.ogGroupKey;
      if (!groupKey) return;
      event.preventDefault();
      event.stopPropagation();
      onToggleGroup(groupKey);
    };
    root.addEventListener("mousedown", handleGroupMouseDown, true);
    return () => root.removeEventListener("mousedown", handleGroupMouseDown, true);
  }, [onToggleGroup]);

  const components = useMemo(() => ({
    groupHeaderRenderer: (props: GroupHeaderRendererProps) => {
      const color = props.data?.__groupColor || "#9ca3af";
      const label = props.data?.__groupLabel || "";
      const meta = props.data?.__groupMeta || "";
      const groupKey = props.data?.__groupKey || "";
      const level = props.data?.__groupLevel ?? 0;
      const kind = props.data?.__groupKind ?? "trim";
      const expanded = props.data?.__expanded ?? false;
      const isSubgroup = level > 0;
      const toggleGroup = () => {
        const toggle = props.context?.onToggleGroup ?? onToggleGroup;
        if (groupKey) toggle?.(groupKey);
      };
      return (
        <div
          className={`og-group-header-renderer og-group-header-renderer-${kind}`}
          data-og-group-key={groupKey || undefined}
          style={{
            display: "flex",
            alignItems: "center",
            gap: isSubgroup ? 6 : 8,
            height: "100%",
            fontWeight: 700,
            fontSize: isSubgroup ? 12 : 13,
            paddingLeft: 4 + level * 18,
          }}
        >
          <button
            type="button"
            aria-label={expanded ? "Collapse group" : "Expand group"}
            disabled={!groupKey}
            onKeyDown={(event) => {
              if (event.key !== "Enter" && event.key !== " ") return;
              event.stopPropagation();
              event.preventDefault();
              toggleGroup();
            }}
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              width: 20,
              height: 20,
              borderRadius: 4,
              border: "1px solid #cbd5e1",
              background: "#fff",
              color,
              cursor: groupKey ? "pointer" : "default",
              fontWeight: 800,
              lineHeight: 1,
              flexShrink: 0,
            }}
          >
            {expanded ? "-" : "+"}
          </button>
          <div style={{ width: isSubgroup ? 3 : 4, height: isSubgroup ? 16 : 20, borderRadius: 2, flexShrink: 0, backgroundColor: color }} />
          <span title={label} style={{ color, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{label}</span>
          {meta ? <span className="og-group-header-meta">{meta}</span> : null}
        </div>
      );
    },
  }), [onToggleGroup]);

  return (
    <div ref={gridWrapperRef} className="og-grid-wrapper" style={{ height: "70vh", width: "100%" }}>
      <AgGridReact<OrderGeniusGridRow>
        theme={themeAlpine}
        rowData={rows}
        pinnedBottomRowData={pinnedBottomRowData}
        columnDefs={columnDefs}
        components={components}
        context={gridContext}
        defaultColDef={defaultColDef}
        getRowId={getRowId}
        isRowSelectable={isRowSelectable}
        rowClassRules={rowClassRules}
        onCellValueChanged={onCellValueChanged}
        onGridReady={(p) => {
          localGridApiRef.current = p.api;
          onGridReady?.(p.api);
        }}
        stopEditingWhenCellsLoseFocus={true}
        undoRedoCellEditing={true}
        undoRedoCellEditingLimit={20}
        animateRows={false}
        enableCellTextSelection={true}
        suppressDragLeaveHidesColumns={true}
        rowModelType="clientSide"
        rowBuffer={10}
        headerHeight={32}
        rowHeight={32}
      />
    </div>
  );
}
// force recompile
