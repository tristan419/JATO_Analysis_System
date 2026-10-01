import { describe, expect, it } from "vitest";
import type { EditableCallbackParams, ValueGetterParams, ValueSetterParams } from "ag-grid-community";

import {
  buildOrderGeniusColumnDefs,
  getOrderGeniusColumnWidthStorageKey,
  ORDER_GENIUS_MODEL_COLUMN_DEFAULT_WIDTH,
  ORDER_GENIUS_MODEL_COLUMN_MAX_WIDTH,
  ORDER_GENIUS_MODEL_COLUMN_MIN_WIDTH,
  parseOrderGeniusColumnWidths,
  type OrderGeniusGridRow,
} from "../../components/OrderGeniusGrid";

const visibleColumns = {
  months: true,
  amount: true,
  ttlQty: true,
  ttlAmount: true,
  fob: true,
  materialCode: true,
  remark: true,
};

describe("Order Genius grid layout", () => {
  it("keeps Model compact, first, and protected from responsive unpinning", () => {
    const columns = buildOrderGeniusColumnDefs(false, 1, visibleColumns, true);
    const model = columns.find((column) => column.colId === "modelName");
    const version = columns.find((column) => column.colId === "version");

    expect(model?.initialWidth).toBe(ORDER_GENIUS_MODEL_COLUMN_DEFAULT_WIDTH);
    expect(model?.minWidth).toBe(ORDER_GENIUS_MODEL_COLUMN_MIN_WIDTH);
    expect(model?.maxWidth).toBe(ORDER_GENIUS_MODEL_COLUMN_MAX_WIDTH);
    expect(model?.pinned).toBe("left");
    expect(model?.lockPinned).toBe(true);
    expect(model?.lockPosition).toBe("left");
    expect(columns[0]?.colId).toBe("modelName");
    expect(version?.pinned).toBeUndefined();
    expect(columns.some((column) => column.colId === "month_1")).toBe(true);
    expect(columns.some((column) => column.colId === "_amount_1")).toBe(true);
  });

  it("validates saved widths and keeps preferences isolated by account", () => {
    expect(parseOrderGeniusColumnWidths({ modelName: 100, version: "144", bad: -1, nope: "x" })).toEqual({
      modelName: ORDER_GENIUS_MODEL_COLUMN_MIN_WIDTH,
      version: 144,
    });
    expect(parseOrderGeniusColumnWidths({ modelName: 900 })).toEqual({
      modelName: ORDER_GENIUS_MODEL_COLUMN_MAX_WIDTH,
    });
    expect(getOrderGeniusColumnWidthStorageKey("alice@example.com")).not.toBe(
      getOrderGeniusColumnWidthStorageKey("bob@example.com"),
    );
    expect(getOrderGeniusColumnWidthStorageKey("alice@example.com")).toContain("alice_example.com");
  });

  it("locks no-price months, shows the reason, and edits amounts at the month's price", () => {
    const row: OrderGeniusGridRow = {
      materialCode: "D", modelName: "JAECOO", version: "Premium", colour: "Colour", fobEur: 1300,
      lifecycleStatus: "active", editable: true, _versions: {}, _errors: {}, _saving: new Set(),
      month_8: 2, month_9: 3, month_12: 0,
      _months: {
        "8": { quantity: 2, rowVersion: 1, isEditable: true, fobEur: 1500 },
        "9": { quantity: 3, rowVersion: 1, isEditable: false, fobEur: null, reason: "No country FOB" },
        "12": { quantity: 0, rowVersion: 1, isEditable: true, fobEur: 1800 },
      },
    };
    const columns = buildOrderGeniusColumnDefs(false, null, visibleColumns, true);
    const september = columns.find((column) => column.colId === "month_9")!;
    const augustAmount = columns.find((column) => column.colId === "_amount_8")!;
    const septemberAmount = columns.find((column) => column.colId === "_amount_9")!;
    const december = columns.find((column) => column.colId === "month_12")!;
    if (typeof september.editable !== "function" || typeof augustAmount.valueGetter !== "function" || typeof septemberAmount.valueGetter !== "function" || typeof december.valueSetter !== "function") throw new Error("Expected grid callbacks");
    const params = { data: row } as EditableCallbackParams<OrderGeniusGridRow>;
    expect(september.editable(params)).toBe(false);
    expect(september.tooltipValueGetter?.({ ...params, location: "cell" })).toBe("No country FOB");
    expect(augustAmount.valueGetter({ data: row } as ValueGetterParams<OrderGeniusGridRow>)).toBe(3000);
    expect(septemberAmount.valueGetter({ data: row } as ValueGetterParams<OrderGeniusGridRow>)).toBe(0);
    expect(december.valueSetter({ data: row, newValue: 5 } as ValueSetterParams<OrderGeniusGridRow>)).toBe(true);
    expect(row._amount_12).toBe(9000);
    expect(row._ttlAmount).toBe(12000);
  });
});
