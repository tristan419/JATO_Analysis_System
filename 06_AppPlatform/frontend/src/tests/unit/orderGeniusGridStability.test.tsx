// @vitest-environment jsdom
import { cleanup, render } from "@testing-library/react";
import type { AgGridReactProps } from "ag-grid-react";
import { afterEach, expect, it, vi } from "vitest";
import { OrderGeniusGrid, type OrderGeniusGridRow } from "../../components/OrderGeniusGrid";

const captured = vi.hoisted(() => new Map<string, AgGridReactProps<OrderGeniusGridRow>>());
vi.mock("ag-grid-react", () => ({ AgGridReact: (props: AgGridReactProps<OrderGeniusGridRow>) => { captured.set("props", props); return <div />; } }));
afterEach(() => { cleanup(); captured.clear(); });
it("does not rebuild columns while quantities, PI eligibility, selection or comparison change", () => {
  const row: OrderGeniusGridRow = { materialCode: "T7150DCLWT1001", modelName: "J7", version: "FWD", colour: "Black", fobEur: 20000, lifecycleStatus: "active", editable: true, _versions: {}, _errors: {}, _saving: new Set(), month_10: 0 };
  const visibleColumns = { months: true, amount: true, ttlQty: true, ttlAmount: true, fob: true, materialCode: true, remark: true };
  const onToggleGroup = vi.fn();
  const view = (quantity: number) => <OrderGeniusGrid rows={[{ ...row, month_10: quantity }]} showCountry={false} selectedMonth={10} visibleColumns={visibleColumns} canEditQuantities onToggleGroup={onToggleGroup} onCellValueChanged={() => undefined}
    selectableRowIds={new Set(quantity ? [row.materialCode] : [])} selectedRowIds={new Set()} onTogglePiRow={() => undefined}
    comparisonMaterials={quantity ? [{ brand: "JAECOO", modelName: "J7", bomTemplate: "T7150D**WT1001" }] : []}
    piSelectionSummary={{ selectableCount: quantity, selectedCount: 0, allSelected: false, partialSelected: false, onToggleAll: () => undefined }} />;
  const result = render(view(0)), original = captured.get("props")?.columnDefs;
  expect(original?.length).toBeGreaterThan(0);
  result.rerender(view(15));
  expect(captured.get("props")?.columnDefs).toBe(original);
  expect(captured.get("props")?.rowData?.[0].month_10).toBe(15);
});
