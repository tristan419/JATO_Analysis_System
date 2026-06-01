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
  type ValueGetterParams,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";

ModuleRegistry.registerModules([AllCommunityModule]);

const MONTH_NAMES = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

export interface OrderGeniusGridRow {
  materialCode: string;
  modelName: string;
  version: string;
  colour: string;
  interiorColorName?: string | null;
  fobEur: number | null;
  lifecycleStatus: string;
  editable: boolean;
  remark?: string;
  _countryCode?: string;
  _indent?: boolean;
  __type?: "groupHeader" | "data" | "consolidated_parent";
  __groupLabel?: string;
  __groupColor?: string;
  __groupColSpan?: number;
  // Flattened months: month_1..month_12
  [key: `month_${number}`]: number;
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
}

export function buildOrderGeniusColumnDefs(
  showCountry: boolean,
  selectedMonth: number | null,
  vis: OrderGeniusGridProps["visibleColumns"],
): ColDef<OrderGeniusGridRow>[] {
  const cols: ColDef<OrderGeniusGridRow>[] = [];

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
      width: 220,
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
      cellRenderer: (p: any) => {
        const name = p.value || "";
        if (!name) return null;
        const hasAnd = /[&／]/.test(name);
        const parts = name.split(/[&／]/).map((s: string) => s.trim());
        const colourHex = (n: string): string => {
          const map: Record<string, string> = {
            'carbon crystal black':'#1a1a1a','black':'#1a1a1a','khaki white':'#f0ece0','white':'#f5f5f0',
            'moonlight silver':'#d4d0c8','silver':'#c0c0c0','aviation silver':'#c8c0b8',
            'olive gray':'#8a8a7a','gray':'#808080','matte gray':'#5a5a5a','fjord grey':'#6e7a7a',
            'blood red':'#8b0000','aurora green':'#2ecc71','aquatic green':'#1abc9c','alpine green':'#27ae60',
            'mist green':'#82b74b','misty green':'#7daa4a','model green':'#3a7d44','glacier blue':'#5b8db8',
            'phantom gray':'#4a4a4a','tech gray':'#607d8b',
          };
          const n2 = n.toLowerCase();
          if (map[n2]) return map[n2];
          for (const [k, v] of Object.entries(map)) { if (n2.includes(k)) return v; }
          return '#94a3b8';
        };
        const c1 = colourHex(parts[0] || "");
        const c2 = parts.length > 1 ? colourHex(parts[1]) : null;
        const bg = c2 ? `linear-gradient(135deg, ${c1} 50%, ${c2} 50%)` : c1;
        return (
          <span style={{ display: "flex", alignItems: "center", gap: 5 }}>
            <span style={{ display: "inline-block", width: 14, height: 14, borderRadius: 3, flexShrink: 0, border: "1px solid #d1d5db", background: bg }} />
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
      valueFormatter: (p) => (p.value != null ? p.value.toLocaleString() : "-"),
    });
  }

  const activeMonths = MONTH_NAMES.map((_, i) => i + 1).filter(
    (m) => selectedMonth == null || m === selectedMonth,
  );

  for (const m of activeMonths) {
    const field = `month_${m}` as const;
    if (vis.months) {
      cols.push({
        headerName: MONTH_NAMES[m - 1],
        field,
        width: 72,
        type: "numericColumn",
        editable: (params: any) => params.data != null && params.data.__type !== "groupHeader",
        cellEditor: "agNumberCellEditor",
        cellEditorParams: { min: 0 },
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
        field: `_amt_${m}` as any,
        width: 90,
        type: "numericColumn",
        editable: false,
        valueGetter: (p: ValueGetterParams<OrderGeniusGridRow>) => {
          const row = p.data;
          if (!row) return 0;
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
        let t = 0;
        const fob = row.fobEur ?? 0;
        for (const m of activeMonths) t += ((row as any)[`month_${m}`] ?? 0) * fob;
        return t;
      },
      valueFormatter: (p) => (p.value != null ? (p.value as number).toLocaleString() : "0"),
      cellClass: "og-ttl-amount-cell",
    });
  }

  if (vis.remark) {
    cols.push({
      headerName: "Remark",
      field: "remark",
      width: 200,
      editable: false,
      cellClass: "og-remark-cell",
    });
  }

  return cols;
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

export function OrderGeniusGrid({ rows, selectedMonth, visibleColumns, showCountry, onCellValueChanged, onGridReady }: OrderGeniusGridProps) {
  const columnDefs = useMemo(
    () => buildOrderGeniusColumnDefs(showCountry, selectedMonth, visibleColumns),
    [showCountry, selectedMonth, visibleColumns],
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
      (p.data._countryCode || "") + "|" + p.data.materialCode + "|" + (p.data.lifecycleStatus || "active") + "|" + (p.data.version || "") + "|" + (p.data.colour || "") + "|" + (p.data.interiorColorName || ""),
    [],
  );

  const isRowSelectable = useCallback(
    (node: any) => node.data?.__type !== "groupHeader",
    [],
  );

  const rowClassRules = useMemo<any>(
    () => ({
      "og-group-header-row": (p: any) => p.data?.__type === "groupHeader",
      "og-consolidated-parent": (p: any) => p.data?.__type === "consolidated_parent",
      "og-historical-row": (p: any) => p.data?.lifecycleStatus === "historical",
    }),
    [],
  );

  const onFirstDataRendered = useCallback((params: { api: any }) => {
    params.api.autoSizeAllColumns(false);
  }, []);

  const components = useMemo(() => ({
    groupHeaderRenderer: (props: any) => {
      const color = props.data?.__groupColor || "#9ca3af";
      const label = props.data?.__groupLabel || "";
      return (
        <div style={{ display: "flex", alignItems: "center", gap: 8, height: "100%", fontWeight: 700, fontSize: 13, paddingLeft: 4 }}>
          <div style={{ width: 4, height: 20, borderRadius: 2, flexShrink: 0, backgroundColor: color }} />
          <span style={{ color }}>{label}</span>
        </div>
      );
    },
  }), []);

  return (
    <div className="og-grid-wrapper" style={{ height: "70vh", width: "100%" }}>
      <AgGridReact<OrderGeniusGridRow>
        theme={themeAlpine}
        rowData={rows}
        columnDefs={columnDefs}
        components={components}
        defaultColDef={defaultColDef}
        getRowId={getRowId}
        isRowSelectable={isRowSelectable}
        rowClassRules={rowClassRules}
        onCellValueChanged={onCellValueChanged}
        onFirstDataRendered={onFirstDataRendered}
        onGridReady={(p) => onGridReady?.(p.api)}
        stopEditingWhenCellsLoseFocus={true}
        undoRedoCellEditing={true}
        undoRedoCellEditingLimit={20}
        enableCellTextSelection={true}
        suppressDragLeaveHidesColumns={true}
        rowModelType="clientSide"
        rowBuffer={10}
        animateRows={false}
        headerHeight={32}
        rowHeight={32}
      />
    </div>
  );
}
// force recompile
