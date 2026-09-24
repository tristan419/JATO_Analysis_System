import { describe, expect, it } from "vitest";

import {
  buildOrderGeniusColumnDefs,
  getOrderGeniusColumnWidthStorageKey,
  ORDER_GENIUS_MODEL_COLUMN_DEFAULT_WIDTH,
  ORDER_GENIUS_MODEL_COLUMN_MAX_WIDTH,
  ORDER_GENIUS_MODEL_COLUMN_MIN_WIDTH,
  parseOrderGeniusColumnWidths,
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
});
