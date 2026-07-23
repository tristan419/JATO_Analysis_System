// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { ConfigComparisonTable } from "../../components/ConfigComparisonTable";
import type { CompareResponse, CompareRow } from "../../types/engineeringConfig";

vi.mock("../../api/client", () => ({
  api: {
    exportEngineeringConfigCompareXlsx: vi.fn(),
    exportEngineeringConfigComparePdf: vi.fn(),
  },
}));

const compareData: CompareResponse = {
  trims: [
    {
      trimId: "trim-core",
      fullTrimName: "Volvo XC60 Core",
      brand: "Volvo",
      modelName: "XC60",
      trimName: "Core",
      market: "Germany",
      modelYear: "2026",
      salesVersion: "CORE-SV",
      materialNo: "MAT-CORE",
      dataOrigin: "own_catalog",
      sourceFileName: "own-catalog.xlsx",
      msrp: null,
      targetPrice: null,
    },
    {
      trimId: "trim-ultra",
      fullTrimName: "Volvo XC60 Ultra",
      brand: "Volvo",
      modelName: "XC60",
      trimName: "Ultra",
      market: "Germany",
      modelYear: "2026",
      salesVersion: "ULTRA-SV",
      dataOrigin: "external_or_scraped",
      sourceFileName: "competitor-site.xlsx",
      sourceCreatedBy: "alice",
      msrp: null,
      targetPrice: null,
    },
  ],
  summary: {
    totalFeatures: 3,
    shownFeatures: 3,
    commonSameCount: 0,
    differentValueCount: 1,
    uniqueFeatureCount: 1,
    partialAvailableCount: 0,
    missingOrUnknownCount: 1,
    confirmedDifferenceCount: 3,
    rawConfirmedDifferenceCount: 2,
    inferredDifferenceCount: 1,
    differenceCount: 3,
    differenceCategories: ["Safety", "Wheel", "Infotainment"],
  },
  rows: [
    {
      category: "Safety",
      featureCode: "blind_spot",
      featureName: "Blind Spot Information System",
      comparisonType: "UNIQUE_TO_TRIM",
      uniqueTrimIds: ["trim-ultra"],
      businessNote: "Volvo XC60 Ultra 独有配置",
      values: [
        {
          valueId: "core-blind",
          rawValue: "",
          normalizedValue: null,
          availability: "NOT_AVAILABLE",
          unit: null,
          valueState: "blank",
          displayValue: "不配备*",
          inferred: true,
          inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
          confidence: 0.7,
          source: {
            sheetName: "T19C MY ICE ",
            rowNumber: 128,
            columnNumber: 6,
            columnLetter: "F",
            cell: "F128",
            sourceCell: "F128",
            mergedRange: null,
            inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
            confidence: 0.7,
          },
        },
        {
          valueId: "ultra-blind",
          rawValue: "Yes",
          normalizedValue: "yes",
          availability: "STANDARD",
          unit: null,
          valueState: "marker_value",
          displayValue: "标配",
          inferred: false,
          inferenceReason: null,
          confidence: null,
          source: {
            sheetName: "T19C MY ICE ",
            rowNumber: 128,
            columnNumber: 5,
            columnLetter: "E",
            cell: "E128",
            sourceCell: "E128",
            mergedRange: null,
          },
        },
      ],
    },
    {
      category: "Wheel",
      featureCode: "wheel_size",
      featureName: "Wheel size",
      comparisonType: "DIFFERENT_VALUE",
      uniqueTrimIds: [],
      businessNote: "配置值不同",
      values: [
        {
          valueId: "core-wheel",
          rawValue: "18 inch",
          normalizedValue: "18 inch",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "18 inch",
          inferred: false,
          source: {
            sheetName: "T19C MY ICE ",
            rowNumber: 11,
            columnNumber: 5,
            columnLetter: "E",
            cell: "E11",
            sourceCell: "D11",
            mergedRange: "D11:F11",
          },
        },
        {
          valueId: "ultra-wheel",
          rawValue: "20 inch",
          normalizedValue: "20 inch",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "20 inch",
          inferred: false,
          source: {
            sheetName: "T19C MY ICE ",
            rowNumber: 11,
            columnNumber: 6,
            columnLetter: "F",
            cell: "F11",
            sourceCell: "D11",
            mergedRange: "D11:F11",
          },
        },
      ],
    },
    {
      category: "Infotainment",
      featureCode: "premium_audio",
      featureName: "Premium audio",
      comparisonType: "MISSING_OR_UNKNOWN",
      uniqueTrimIds: [],
      businessNote: "存在缺失或未知数据，需先确认配置源",
      values: [
        null,
        {
          valueId: "ultra-audio",
          rawValue: "Harman Kardon",
          normalizedValue: "harman kardon",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "Harman Kardon",
          inferred: false,
          source: null,
        },
      ],
    },
  ],
  groups: [],
  totalFeatures: 3,
  shownFeatures: 3,
};

const compareDataWithCommonOnlyCategory: CompareResponse = {
  ...compareData,
  rows: [
    ...compareData.rows,
    {
      category: "Comfort",
      featureCode: "shared_speaker",
      featureName: "Shared speaker count",
      comparisonType: "COMMON_SAME",
      uniqueTrimIds: [],
      businessNote: "共同配置",
      values: [
        {
          valueId: "core-shared-speaker",
          rawValue: "6",
          normalizedValue: "6",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "6",
          inferred: false,
          source: null,
        },
        {
          valueId: "ultra-shared-speaker",
          rawValue: "6",
          normalizedValue: "6",
          availability: "VALUE",
          unit: null,
          valueState: "text_value",
          displayValue: "6",
          inferred: false,
          source: null,
        },
      ],
    },
  ],
  totalFeatures: 4,
  shownFeatures: 4,
};

const compareDataWithMiddleTrim: CompareResponse = {
  ...compareData,
  trims: [
    compareData.trims[0],
    {
      trimId: "trim-plus",
      fullTrimName: "Volvo XC60 Plus",
      brand: "Volvo",
      modelName: "XC60",
      trimName: "Plus",
      market: "Germany",
      modelYear: "2026",
      salesVersion: "PLUS-SV",
      msrp: null,
      targetPrice: null,
    },
    compareData.trims[1],
  ],
  rows: compareData.rows.map((row) => {
    const plusValue = row.values[0] ?? row.values[1];
    return {
      ...row,
      values: [
        row.values[0],
        plusValue ? { ...plusValue, valueId: `plus-${row.featureCode}` } : null,
        row.values[1],
      ],
    };
  }),
};

const coreExportHeaderLabel = "Volvo XC60 Core · 本品 · 物料号 MAT-CORE · Germany · 2026 · own-catalog.xlsx";
const ultraExportHeaderLabel = "Volvo XC60 Ultra · 竞品 / 外部 · Sales version ULTRA-SV · Germany · 2026 · competitor-site.xlsx · 来源人 alice";

const compareDataOnlyCommon: CompareResponse = {
  ...compareDataWithCommonOnlyCategory,
  rows: compareDataWithCommonOnlyCategory.rows.filter((row) => row.comparisonType === "COMMON_SAME"),
  totalFeatures: 1,
  shownFeatures: 1,
  summary: {
    ...compareData.summary,
    totalFeatures: 1,
    shownFeatures: 1,
    commonSameCount: 1,
    differentValueCount: 0,
    uniqueFeatureCount: 0,
    partialAvailableCount: 0,
    missingOrUnknownCount: 0,
    confirmedDifferenceCount: 0,
    rawConfirmedDifferenceCount: 0,
    inferredDifferenceCount: 0,
    differenceCount: 0,
    differenceCategories: [],
  },
};

const originalClipboard = navigator.clipboard;
const originalScrollIntoView = HTMLElement.prototype.scrollIntoView;

function buildLargeCompareData(rowCount: number): CompareResponse {
  const rows: CompareRow[] = Array.from({ length: rowCount }, (_, index) => {
    const sourceRow = compareData.rows[index % compareData.rows.length];
    return {
      ...sourceRow,
      category: `Excel Category ${Math.floor(index / 50) + 1}`,
      featureCode: `bulk_feature_${index + 1}`,
      featureName: `Bulk feature ${index + 1}`,
      businessNote: `Excel-style full table row ${index + 1}`,
      values: sourceRow.values.map((value, valueIndex) => value
        ? {
          ...value,
          valueId: `bulk-${index + 1}-${valueIndex + 1}`,
        }
        : null),
    };
  });
  return {
    ...compareData,
    rows,
    totalFeatures: rowCount,
    shownFeatures: rowCount,
  };
}

describe("ConfigComparisonTable", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
    cleanup();
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: originalClipboard,
    });
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: originalScrollIntoView,
    });
  });

  function openSimpleTableControlsIfPresent(): HTMLDetailsElement | null {
    const controls = screen.queryByLabelText("配置表筛选和目标列") as HTMLDetailsElement | null;
    if (!controls || controls.open) return controls;
    controls.open = true;
    fireEvent(controls, new Event("toggle", { bubbles: true }));
    return controls;
  }

  function categoryFilterInput(): HTMLInputElement {
    const visibleInput = screen.queryByRole("combobox", { name: "配置大类" }) as HTMLInputElement | null;
    if (visibleInput) return visibleInput;
    openSimpleTableControlsIfPresent();
    return screen.getByRole("combobox", { name: "配置大类" }) as HTMLInputElement;
  }

  function selectCategoryFilter(category: string): void {
    const input = categoryFilterInput();
    fireEvent.focus(input);
    fireEvent.click(within(screen.getByRole("listbox")).getByText(category));
  }

  function deltaFilterButton(name: RegExp): HTMLButtonElement {
    return within(screen.getByLabelText("差异类型筛选")).getByRole("button", { name }) as HTMLButtonElement;
  }

  function openTableActionsIfPresent(): HTMLElement | null {
    let tableActions = screen.queryByLabelText("配置表操作") as HTMLDetailsElement | null;
    if (!tableActions) {
      openSimpleTableControlsIfPresent();
      tableActions = screen.queryByLabelText("配置表操作") as HTMLDetailsElement | null;
    }
    if (!tableActions) return null;
    if (!tableActions.open) {
      fireEvent.click(within(tableActions).getByText("表格操作"));
    }
    return tableActions;
  }

  function rangeMetricText(label: string): string {
    const status = screen.getByLabelText("配置表范围状态");
    const metric = Array.from(status.querySelectorAll(".comparison-range-status__metric"))
      .find((item) => item.textContent?.includes(label));
    return metric?.textContent ?? "";
  }

  it("exposes the compare table as a focusable anchor target", () => {
    render(<ConfigComparisonTable data={compareData} />);

    const tableRegion = screen.getByLabelText("配置对比表");
    expect(tableRegion.getAttribute("id")).toBe("config-compare-table");
    expect(tableRegion.getAttribute("tabindex")).toBe("-1");
  });

  it("auto-saves editable formal compare cells after 1.2 seconds", async () => {
    const editableData: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode === "wheel_size"
        ? {
            ...row,
            values: row.values.map((value) => value?.valueId === "core-wheel"
              ? { ...value, version: 3 }
              : value),
          }
        : row),
    };
    const onSaveCell = vi.fn().mockResolvedValue({
      valueId: "core-wheel",
      rawValue: "19 inch",
      normalizedValue: "19 inch",
      availability: "VALUE",
      displayValue: "19 inch",
      version: 4,
    });

    render(
      <ConfigComparisonTable
        data={editableData}
        baseTrimId="trim-core"
        cellEvidenceMode="compact"
        columnMode="matrix"
        toolbarMode="simple"
        valuesEditable
        onSaveCell={onSaveCell}
      />,
    );

    vi.useFakeTimers();
    const editStatus = screen.getByLabelText("配置表在线编辑状态");
    expect(editStatus.textContent).toContain("在线编辑已开启");
    expect(editStatus.textContent).toContain("点击配置值进入编辑");
    const editableCell = screen.getByText("18 inch").closest("td") as HTMLTableCellElement;
    expect(within(editableCell).getByText("编辑")).toBeTruthy();
    fireEvent.click(screen.getByText("18 inch"));
    const input = screen.getByLabelText("Volvo XC60 Core Wheel size 配置值，修改后 1.2 秒自动保存");
    fireEvent.change(input, { target: { value: "19 inch" } });

    act(() => {
      vi.advanceTimersByTime(1199);
    });
    expect(onSaveCell).not.toHaveBeenCalled();

    await act(async () => {
      vi.advanceTimersByTime(1);
      await Promise.resolve();
    });

    expect(onSaveCell).toHaveBeenCalledWith(expect.objectContaining({
      valueId: "core-wheel",
      rawValue: "19 inch",
      expectedVersion: 3,
    }));
  });

  it("queues edits typed while an earlier auto-save is still in flight", async () => {
    const editableData: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode === "wheel_size"
        ? {
            ...row,
            values: row.values.map((value) => value?.valueId === "core-wheel"
              ? { ...value, version: 3 }
              : value),
          }
        : row),
    };
    let resolveFirstSave: ((result: {
      valueId: string;
      rawValue: string;
      normalizedValue: string;
      availability: "VALUE";
      displayValue: string;
      version: number;
    }) => void) | undefined;
    const firstSave = new Promise<{
      valueId: string;
      rawValue: string;
      normalizedValue: string;
      availability: "VALUE";
      displayValue: string;
      version: number;
    }>((resolve) => {
      resolveFirstSave = resolve;
    });
    const onSaveCell = vi.fn()
      .mockImplementationOnce(() => firstSave)
      .mockResolvedValueOnce({
        valueId: "core-wheel",
        rawValue: "20 inch",
        normalizedValue: "20 inch",
        availability: "VALUE",
        displayValue: "20 inch",
        version: 5,
      });
    const view = render(
      <ConfigComparisonTable
        data={editableData}
        baseTrimId="trim-core"
        cellEvidenceMode="compact"
        columnMode="matrix"
        toolbarMode="simple"
        valuesEditable
        onSaveCell={onSaveCell}
      />,
    );

    vi.useFakeTimers();
    fireEvent.click(screen.getByText("18 inch"));
    const input = screen.getByLabelText("Volvo XC60 Core Wheel size 配置值，修改后 1.2 秒自动保存") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "19 inch" } });
    await act(async () => {
      vi.advanceTimersByTime(1200);
      await Promise.resolve();
    });
    expect(onSaveCell).toHaveBeenCalledTimes(1);

    fireEvent.change(input, { target: { value: "20 inch" } });
    await act(async () => {
      resolveFirstSave?.({
        valueId: "core-wheel",
        rawValue: "19 inch",
        normalizedValue: "19 inch",
        availability: "VALUE",
        displayValue: "19 inch",
        version: 4,
      });
      await firstSave;
    });

    const refreshedData: CompareResponse = {
      ...editableData,
      rows: editableData.rows.map((row) => row.featureCode === "wheel_size"
        ? {
            ...row,
            values: row.values.map((value) => value?.valueId === "core-wheel"
              ? { ...value, rawValue: "19 inch", displayValue: "19 inch", version: 4 }
              : value),
          }
        : row),
    };
    view.rerender(
      <ConfigComparisonTable
        data={refreshedData}
        baseTrimId="trim-core"
        cellEvidenceMode="compact"
        columnMode="matrix"
        toolbarMode="simple"
        valuesEditable
        onSaveCell={onSaveCell}
      />,
    );
    expect((screen.getByLabelText("Volvo XC60 Core Wheel size 配置值，修改后 1.2 秒自动保存") as HTMLInputElement).value).toBe("20 inch");

    await act(async () => {
      vi.advanceTimersByTime(1200);
      await Promise.resolve();
    });
    expect(onSaveCell).toHaveBeenCalledTimes(2);
    expect(onSaveCell).toHaveBeenLastCalledWith(expect.objectContaining({
      valueId: "core-wheel",
      rawValue: "20 inch",
      expectedVersion: 4,
    }));
  });

  it("clears editable formal compare cells with the keyboard after 1.2 seconds", async () => {
    const editableData: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode === "wheel_size"
        ? {
            ...row,
            values: row.values.map((value) => value?.valueId === "core-wheel"
              ? { ...value, version: 5 }
              : value),
          }
        : row),
    };
    const onSaveCell = vi.fn().mockResolvedValue({
      valueId: "core-wheel",
      rawValue: "",
      normalizedValue: null,
      availability: "UNKNOWN",
      displayValue: "待确认",
      version: 6,
    });

    render(
      <ConfigComparisonTable
        data={editableData}
        baseTrimId="trim-core"
        cellEvidenceMode="compact"
        columnMode="matrix"
        toolbarMode="simple"
        valuesEditable
        onSaveCell={onSaveCell}
      />,
    );

    vi.useFakeTimers();
    const cell = screen.getByText("18 inch").closest("td") as HTMLTableCellElement;
    fireEvent.keyDown(cell, { key: "Delete" });
    const input = screen.getByLabelText("Volvo XC60 Core Wheel size 配置值，修改后 1.2 秒自动保存") as HTMLInputElement;
    expect(input.value).toBe("");

    await act(async () => {
      vi.advanceTimersByTime(1200);
      await Promise.resolve();
    });

    expect(onSaveCell).toHaveBeenCalledWith(expect.objectContaining({
      valueId: "core-wheel",
      rawValue: "",
      expectedVersion: 5,
    }));
  });

  it("creates missing formal compare cells after 1.2 seconds when the row has a feature id", async () => {
    const editableData: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode === "premium_audio"
        ? { ...row, featureId: "feature-premium-audio" }
        : row),
    };
    const onSaveCell = vi.fn().mockResolvedValue({
      valueId: "core-audio-created",
      rawValue: "Standard audio",
      normalizedValue: "standard audio",
      availability: "VALUE",
      displayValue: "Standard audio",
      version: 1,
    });

    render(
      <ConfigComparisonTable
        data={editableData}
        baseTrimId="trim-core"
        cellEvidenceMode="compact"
        columnMode="matrix"
        toolbarMode="simple"
        valuesEditable
        onSaveCell={onSaveCell}
      />,
    );

    vi.useFakeTimers();
    const row = screen.getByRole("row", { name: "Premium audio，待确认配置" });
    fireEvent.click(within(row).getByText("待确认"));
    const input = screen.getByLabelText("Volvo XC60 Core Premium audio 配置值，修改后 1.2 秒自动保存");
    fireEvent.change(input, { target: { value: "Standard audio" } });

    await act(async () => {
      vi.advanceTimersByTime(1200);
      await Promise.resolve();
    });

    expect(onSaveCell).toHaveBeenCalledWith(expect.objectContaining({
      valueId: undefined,
      featureId: "feature-premium-audio",
      rawValue: "Standard audio",
    }));
  });

  it("renders business comparison types and missing values distinctly", () => {
    const { container } = render(<ConfigComparisonTable data={compareData} />);

    expect(screen.getAllByText("Blind Spot Information System").length).toBeGreaterThan(0);
    expect(screen.getByText("独有配置")).toBeTruthy();
    expect(screen.getByText("值不同")).toBeTruthy();
    expect(screen.getByLabelText("当前表格口径").textContent).toContain("目标全部对比对象");
    expect(screen.getByLabelText("当前表格口径").textContent).toContain("范围全部配置");
    expect(screen.getByLabelText("当前表格口径").textContent).toContain("当前3 项配置");
    expect(screen.getByLabelText("配置表范围状态").classList.contains("comparison-range-status--simple")).toBe(false);
    expect(screen.getAllByText("待确认").length).toBeGreaterThan(0);
    expect(screen.getByText("存在缺失或未知数据，需先确认配置源")).toBeTruthy();
    expect(screen.getByText("Harman Kardon")).toBeTruthy();
    expect(container.querySelectorAll(".comparison-table col")).toHaveLength(compareData.trims.length + 4);
    expect(container.querySelector(".comparison-col-feature")).toBeTruthy();
    expect(container.querySelectorAll(".comparison-col-trim")).toHaveLength(compareData.trims.length);
    expect((container.querySelector(".comparison-table") as HTMLTableElement).style.minWidth).toBe("1310px");
    expect(screen.getByRole("row", { name: "Blind Spot Information System，差异配置" }).classList.contains("compare-row-diff")).toBe(true);
    const pendingRow = screen.getByRole("row", { name: "Premium audio，待确认配置" });
    expect(pendingRow.classList.contains("compare-row-pending")).toBe(true);
    expect(pendingRow.classList.contains("compare-row-diff")).toBe(false);
    expect(screen.queryByRole("row", { name: "Premium audio，差异配置" })).toBeNull();
  });

  it("shows a compact legend for value states and source evidence markers", () => {
    render(<ConfigComparisonTable data={compareData} />);

    const legend = screen.getByLabelText("配置值与证据图例");
    expect(legend.tagName.toLowerCase()).toBe("section");
    expect(legend.textContent).toContain("标配");
    expect(legend.textContent).toContain("当前配置列已配置");
    expect(legend.textContent).toContain("选装");
    expect(legend.textContent).toContain("不配备*");
    expect(legend.textContent).toContain("规则推断，不是 Excel 原文");
    expect(legend.textContent).toContain("待确认");
    expect(legend.textContent).toContain("不能直接等同于不配备");
    expect(legend.textContent).toContain("来源");
    expect(legend.textContent).toContain("合并");
    expect(legend.textContent).toContain("来自横向合并格展开");
    expect(legend.textContent).toContain("缺源");
  });

  it("collapses the legend behind a compact summary when requested", () => {
    render(<ConfigComparisonTable data={compareData} legendMode="compact" />);

    const legend = screen.getByLabelText("配置值与证据图例") as HTMLDetailsElement;
    expect(legend.tagName.toLowerCase()).toBe("details");
    expect(legend.open).toBe(false);
    expect(within(legend).getByText("图例 / 证据说明")).toBeTruthy();
    expect(within(legend).getByText("标配、选装、不配备*、待确认与来源标记")).toBeTruthy();
    expect(legend.textContent).toContain("规则推断，不是 Excel 原文");
  });

  it("keeps only primary table filters visible in simple toolbar mode", () => {
    const { container } = render(<ConfigComparisonTable data={compareData} toolbarMode="simple" />);

    openSimpleTableControlsIfPresent();
    const toolbar = container.querySelector(".comparison-toolbar");
    expect(toolbar?.classList.contains("comparison-toolbar--simple")).toBe(true);
    expect(container.querySelector(".comparison-toolbar--simple > .text-muted")).toBeNull();
    const filter = screen.getByLabelText("差异类型筛选");
    const primaryFilter = filter.querySelector(".comparison-type-filter-primary") as HTMLElement;
    expect(within(filter).getByRole("button", { name: /全部配置行 3/ })).toBeTruthy();
    expect(within(filter).getByRole("button", { name: /差异行 3/ })).toBeTruthy();
    expect(within(primaryFilter).queryByRole("button", { name: /新增配置/ })).toBeNull();
    expect(within(filter).queryByRole("button", { name: /待确认行/ })).toBeNull();
    expect(within(filter).queryByRole("button", { name: /共同配置行/ })).toBeNull();
    expect(filter.querySelector(".comparison-advanced-filter-panel")).toBeNull();
    const tableActions = screen.getByLabelText("配置表操作") as HTMLDetailsElement;
    expect(tableActions).toBeTruthy();
    expect(tableActions.open).toBe(false);
    const directToolbarActions = Array.from(toolbar?.children ?? []).filter((item) => item.classList.contains("comparison-copy-scope"));
    expect(directToolbarActions).toHaveLength(0);
  });

  it("shows an Excel-style range status for the full matrix by default", () => {
    render(<ConfigComparisonTable data={compareData} columnMode="matrix" toolbarMode="simple" />);

    const status = screen.getByLabelText("配置表范围状态");

    expect(status.classList.contains("comparison-range-status--simple")).toBe(true);
    expect(within(status).getByText("Excel 配置表")).toBeTruthy();
    expect(within(status).getByText("当前展示全部配置行")).toBeTruthy();
    expect(within(status).getByLabelText("当前表格范围：当前展示 3/3 配置行")).toBeTruthy();
    expect(status.textContent).toContain("范围 全部配置");
    expect(rangeMetricText("总配置行")).toContain("3");
    expect(rangeMetricText("当前展示行")).toContain("3");
    expect(rangeMetricText("差异行")).toContain("3");
    expect(rangeMetricText("待确认行")).toBe("");
    expect(rangeMetricText("共同配置行")).toBe("");
  });

  it("keeps total and current row counts distinct after matrix search filters", () => {
    render(<ConfigComparisonTable data={compareData} columnMode="matrix" toolbarMode="simple" />);

    openSimpleTableControlsIfPresent();
    const searchInput = screen.getByRole("combobox", { name: "搜索配置" });
    fireEvent.focus(searchInput);
    fireEvent.change(searchInput, {
      target: { value: "wheel" },
    });

    expect(rangeMetricText("当前展示行")).toContain("3");
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Wheel size"));

    const status = screen.getByLabelText("配置表范围状态");

    expect(within(status).getByText("当前为筛选视图")).toBeTruthy();
    expect(within(status).getByLabelText("当前表格范围：当前展示 1/3 配置行")).toBeTruthy();
    expect(status.textContent).toContain("搜索 Wheel size");
    expect(rangeMetricText("总配置行")).toContain("3");
    expect(rangeMetricText("当前展示行")).toContain("1");
  });

  it("includes the focused target trim in the matrix range status", () => {
    render(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="matrix"
        targetTrimId="trim-ultra"
        toolbarMode="simple"
      />,
    );

    const status = screen.getByLabelText("配置表范围状态");

    expect(within(status).getByText("当前为目标列视图")).toBeTruthy();
    expect(status.textContent).toContain("目标配置列 Volvo XC60 Ultra");
    expect(rangeMetricText("总配置行")).toContain("3");
    expect(rangeMetricText("当前展示行")).toContain("3");
    expect(within(status).queryByRole("button", { name: "显示总配置行：3，xlsx 原表行数" })).toBeNull();
    expect(screen.getAllByRole("button", { name: "显示全部目标列" })).toHaveLength(1);
    expect(screen.queryByRole("button", { name: "恢复全部配置" })).toBeNull();
  });

  it("marks focused target as a row-filtered view only when the row scope is narrowed", () => {
    render(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="matrix"
        deltaFilter="DIFFERENCE"
        targetTrimId="trim-ultra"
        toolbarMode="simple"
      />,
    );

    const status = screen.getByLabelText("配置表范围状态");

    expect(within(status).getByText("当前为筛选视图")).toBeTruthy();
    expect(within(status).getByLabelText("当前表格范围：当前展示 3/3 差异行")).toBeTruthy();
    expect(status.textContent).toContain("范围 差异行");
    expect(status.textContent).toContain("目标配置列 Volvo XC60 Ultra");
    expect(rangeMetricText("当前展示行")).toContain("3");
    expect(within(status).getByRole("button", { name: "从状态栏恢复全部配置行" })).toBeTruthy();
    expect(screen.getAllByRole("button", { name: "恢复全部配置行" })).toHaveLength(1);
  });

  it("offers target-column quick chips in the simple Excel matrix", () => {
    const onTargetTrimChange = vi.fn();
    const { rerender } = render(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="matrix"
        toolbarMode="simple"
        onTargetTrimChange={onTargetTrimChange}
      />,
    );

    const simpleControls = screen.getByLabelText("配置表筛选和目标列") as HTMLDetailsElement;
    expect(simpleControls.tagName).toBe("DETAILS");
    expect(simpleControls.open).toBe(false);
    expect(simpleControls.textContent).toContain("筛选 / 目标列");
    expect(simpleControls.textContent).toContain("全部配置行 3/3");
    expect(screen.queryByRole("combobox", { name: "搜索配置" })).toBeNull();
    expect(screen.queryByLabelText("Excel 目标列快捷选择")).toBeNull();
    openSimpleTableControlsIfPresent();
    const quickbar = screen.getByLabelText("Excel 目标列快捷选择");
    expect(quickbar.classList.contains("comparison-excel-quickbar--simple")).toBe(true);
    expect(quickbar.textContent).toContain("目标列");
    expect(quickbar.textContent).toContain("当前显示全部目标列；差异行按配置行去重");
    const allTargetsButton = within(quickbar).getByRole("button", {
      name: "显示全部目标列，按目标累计差异 4 行次，表格差异行按配置行去重",
    });
    expect(allTargetsButton.getAttribute("aria-pressed")).toBe("true");
    expect(allTargetsButton.textContent).toContain("全部目标列");
    expect(allTargetsButton.textContent).toContain("按目标累计 4 行次");
    expect(within(quickbar).getByRole("button", { name: "聚焦目标列：Volvo XC60 Plus，差异行 1" })).toBeTruthy();
    expect(within(quickbar).getByRole("button", { name: "聚焦目标列：Volvo XC60 Ultra，差异行 3" })).toBeTruthy();

    fireEvent.click(within(quickbar).getByRole("button", { name: "聚焦目标列：Volvo XC60 Ultra，差异行 3" }));

    expect(onTargetTrimChange).toHaveBeenCalledWith("trim-ultra");

    rerender(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="matrix"
        targetTrimId="trim-ultra"
        toolbarMode="simple"
        onTargetTrimChange={onTargetTrimChange}
      />,
    );

    const focusedSimpleControls = screen.getByLabelText("配置表筛选和目标列") as HTMLDetailsElement;
    expect(focusedSimpleControls.open).toBe(true);
    expect(focusedSimpleControls.textContent).toContain("目标列 Volvo XC60 Ultra");
    const focusedQuickbar = screen.getByLabelText("Excel 目标列快捷选择");
    expect(focusedQuickbar.textContent).toContain("当前只看目标列 Volvo XC60 Ultra；配置行仍保持全量");
    expect(within(focusedQuickbar).getByRole("button", { name: "聚焦目标列：Volvo XC60 Ultra，差异行 3" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.queryByRole("columnheader", { name: "Volvo XC60 Plus" })).toBeNull();

    fireEvent.click(within(focusedQuickbar).getByRole("button", {
      name: "显示全部目标列，按目标累计差异 4 行次，表格差异行按配置行去重",
    }));

    expect(onTargetTrimChange).toHaveBeenLastCalledWith(null);
  });

  it("lets range status metrics switch table scope and restore all configs", () => {
    render(
      <ConfigComparisonTable
        data={compareDataWithCommonOnlyCategory}
        baseTrimId="trim-core"
        columnMode="matrix"
        toolbarMode="simple"
      />,
    );

    const status = screen.getByLabelText("配置表范围状态");
    fireEvent.click(within(status).getByRole("button", { name: "显示差异行：3，当前口径" }));

    openSimpleTableControlsIfPresent();
    expect(deltaFilterButton(/差异行 3/).classList.contains("is-active")).toBe(true);
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.queryByText("Shared speaker count")).toBeNull();
    expect(rangeMetricText("当前展示行")).toContain("3");

    fireEvent.click(within(status).getByRole("button", { name: "显示总配置行：4，xlsx 原表行数" }));

    expect(deltaFilterButton(/全部配置行 4/).classList.contains("is-active")).toBe(true);
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(rangeMetricText("当前展示行")).toContain("4");
  });

  it("renders an Excel-like matrix table when column mode is matrix", () => {
    const { container } = render(<ConfigComparisonTable data={compareData} columnMode="matrix" />);

    expect(container.querySelectorAll(".comparison-table col")).toHaveLength(compareData.trims.length + 1);
    expect(container.querySelector(".comparison-table")?.classList.contains("comparison-table--matrix")).toBe(true);
    expect(container.querySelector(".comparison-table")?.classList.contains("comparison-table--compact-cells")).toBe(false);
    expect((container.querySelector(".comparison-table") as HTMLTableElement).style.minWidth).toBe("720px");
    expect(screen.queryByRole("columnheader", { name: "大类" })).toBeNull();
    expect(screen.queryByRole("columnheader", { name: "差异类型" })).toBeNull();
    expect(screen.queryByRole("columnheader", { name: "业务备注" })).toBeNull();
    expect(screen.getByRole("columnheader", { name: "配置项" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Core" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" })).toBeTruthy();
    expect(screen.getByText("Harman Kardon")).toBeTruthy();
    expect(screen.getByRole("row", { name: "Blind Spot Information System，差异配置" }).classList.contains("compare-row-diff")).toBe(true);
  });

  it("narrows matrix columns to the base and focused target trim", () => {
    const { container } = render(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="matrix"
        targetTrimId="trim-ultra"
      />,
    );

    expect(container.querySelectorAll(".comparison-table col")).toHaveLength(3);
    expect((container.querySelector(".comparison-table") as HTMLTableElement).style.minWidth).toBe("720px");
    expect(screen.getByRole("columnheader", { name: "配置项" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Core" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" })).toBeTruthy();
    expect(screen.queryByRole("columnheader", { name: "Volvo XC60 Plus" })).toBeNull();
    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Core" })).getByText("基准")).toBeTruthy();
    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" })).getByText("目标")).toBeTruthy();
  });

  it("keeps all trim columns in full column mode even when a target is focused", () => {
    const { container } = render(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="full"
        targetTrimId="trim-ultra"
      />,
    );

    expect(container.querySelectorAll(".comparison-table col")).toHaveLength(7);
    expect((container.querySelector(".comparison-table") as HTMLTableElement).style.minWidth).toBe("1530px");
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Core" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Plus" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" })).toBeTruthy();
  });

  it("marks the base column without inventing a target role in the default full matrix", () => {
    render(<ConfigComparisonTable data={compareDataWithMiddleTrim} baseTrimId="trim-core" columnMode="matrix" />);

    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Core" })).getByText("基准")).toBeTruthy();
    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Plus" })).queryByText("目标")).toBeNull();
    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" })).queryByText("目标")).toBeNull();
  });

  it("keeps trim identity anchors visible in sticky matrix headers", () => {
    render(<ConfigComparisonTable data={compareData} columnMode="matrix" />);

    const coreHeader = screen.getByRole("columnheader", { name: "Volvo XC60 Core" });
    const ultraHeader = screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" });

    expect(within(coreHeader).getByText("本品")).toBeTruthy();
    expect(within(coreHeader).getByText("物料号 MAT-CORE")).toBeTruthy();
    expect(within(coreHeader).getByText("Germany · 2026 · own-catalog.xlsx")).toBeTruthy();
    expect(within(ultraHeader).getByText("竞品 / 外部")).toBeTruthy();
    expect(within(ultraHeader).getByText("Sales version ULTRA-SV")).toBeTruthy();
    expect(within(ultraHeader).getByText("Germany · 2026 · competitor-site.xlsx")).toBeTruthy();
    expect(ultraHeader.getAttribute("title")).toContain("来源人 alice");
  });

  it("keeps compact matrix headers focused on Excel identity anchors", () => {
    render(<ConfigComparisonTable data={compareData} columnMode="matrix" cellEvidenceMode="compact" />);

    const coreHeader = screen.getByRole("columnheader", { name: "Volvo XC60 Core" });
    const ultraHeader = screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" });

    expect(within(coreHeader).getByText("本品")).toBeTruthy();
    expect(within(coreHeader).getByText("物料号 MAT-CORE")).toBeTruthy();
    expect(within(coreHeader).queryByText("Germany · 2026 · own-catalog.xlsx")).toBeNull();
    expect(coreHeader.getAttribute("title")).toContain("Germany · 2026 · own-catalog.xlsx");
    expect(within(ultraHeader).getByText("Sales version ULTRA-SV")).toBeTruthy();
    expect(within(ultraHeader).queryByText("Germany · 2026 · competitor-site.xlsx")).toBeNull();
    expect(ultraHeader.getAttribute("title")).toContain("Germany · 2026 · competitor-site.xlsx · 来源人 alice");
  });

  it("keeps compact matrix feature codes in hover context instead of the primary row label", () => {
    render(<ConfigComparisonTable data={compareData} columnMode="matrix" cellEvidenceMode="compact" />);

    const row = screen.getByRole("row", { name: "Blind Spot Information System，差异配置" });
    const featureCell = row.querySelector(".comparison-feature-cell");

    expect(document.querySelector(".comparison-table")?.classList.contains("comparison-table--compact-cells")).toBe(true);
    expect(featureCell?.getAttribute("title")).toBe("Blind Spot Information System · Safety · blind_spot");
    expect(featureCell?.querySelector("small")?.textContent).toBe("blind_spot");
  });

  it("shows compact delta hints in matrix feature cells", () => {
    render(
      <ConfigComparisonTable
        data={compareData}
        baseTrimId="trim-core"
        columnMode="matrix"
        targetTrimId="trim-ultra"
      />,
    );

    const row = screen.getByRole("row", { name: "Blind Spot Information System，差异配置" });
    const hint = within(row).getByLabelText("Blind Spot Information System 差异提示");

    expect(hint.textContent).toContain("新增 1");
    expect(hint.textContent).toContain("规则推断 1");
  });

  it("keeps common matrix rows free of noisy delta hints", () => {
    render(
      <ConfigComparisonTable
        data={compareDataWithCommonOnlyCategory}
        baseTrimId="trim-core"
        columnMode="matrix"
      />,
    );

    const row = screen.getByRole("row", { name: "Shared speaker count，共同配置" });

    expect(within(row).queryByLabelText("Shared speaker count 差异提示")).toBeNull();
  });

  it("does not show trim role badges before a base trim is selected", () => {
    render(<ConfigComparisonTable data={compareDataWithMiddleTrim} columnMode="matrix" />);

    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Core" })).queryByText("基准")).toBeNull();
    expect(within(screen.getByRole("columnheader", { name: "Volvo XC60 Ultra" })).queryByText("目标")).toBeNull();
  });

  it("renders large Excel-style config matrices as one continuous table without pagination", () => {
    render(<ConfigComparisonTable data={buildLargeCompareData(245)} />);

    expect(screen.getByText("当前 245 项配置")).toBeTruthy();
    expect(screen.queryByLabelText("配置项分页")).toBeNull();
    expect(screen.queryByRole("button", { name: "上一页" })).toBeNull();
    expect(screen.queryByRole("button", { name: "下一页" })).toBeNull();
    expect(screen.getByText("Bulk feature 245")).toBeTruthy();
  });

  it("toggles all visible category groups like an Excel outline", () => {
    render(<ConfigComparisonTable data={compareData} />);

    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    const collapseButton = screen.getByRole("button", { name: "折叠当前 3 个配置大类" });

    fireEvent.click(collapseButton);

    expect(screen.queryByText("Blind Spot Information System")).toBeNull();
    expect(screen.getByText("Safety")).toBeTruthy();
    expect(screen.getByRole("button", { name: "展开当前 3 个配置大类" })).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "展开当前 3 个配置大类" }));

    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
  });

  it("jumps from the Excel status strip to the first visible difference row", async () => {
    const scrollIntoView = vi.fn();
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    render(<ConfigComparisonTable data={compareData} columnMode="matrix" toolbarMode="simple" />);

    expect(screen.queryByRole("button", { name: "定位首个差异行" })).toBeNull();
    expect(screen.queryByRole("button", { name: "复制选中行" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: /显示差异行/ }));

    openSimpleTableControlsIfPresent();
    fireEvent.click(screen.getByRole("button", { name: "折叠当前 3 个配置大类" }));

    expect(screen.queryByText("Blind Spot Information System")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "定位首个差异行" }));

    const row = screen.getByRole("row", { name: "Blind Spot Information System，差异配置" });
    expect(row.classList.contains("compare-row-active")).toBe(true);
    expect(row.getAttribute("aria-selected")).toBe("true");
    expect(screen.getByText("差异行 1/3")).toBeTruthy();
    await waitFor(() => {
      expect(scrollIntoView).toHaveBeenCalledWith({ behavior: "smooth", block: "center", inline: "nearest" });
    });

    fireEvent.click(screen.getByRole("button", { name: "下一个差异行" }));

    const nextRow = screen.getByRole("row", { name: "Wheel size，差异配置" });
    expect(nextRow.classList.contains("compare-row-active")).toBe(true);
    expect(screen.getByText("差异行 2/3")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "上一个差异行" }));

    expect(row.classList.contains("compare-row-active")).toBe(true);
    expect(screen.getByText("差异行 1/3")).toBeTruthy();
  });

  it("copies the active row as an Excel-ready TSV line", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" columnMode="matrix" toolbarMode="simple" />);

    expect(screen.queryByRole("button", { name: "复制选中行" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: /显示差异行/ }));
    fireEvent.click(screen.getByRole("button", { name: "定位首个差异行" }));
    fireEvent.click(screen.getByRole("button", { name: "复制选中行" }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });
    const copiedText = writeText.mock.calls[0]?.[0] as string;
    const lines = copiedText.split("\n");

    expect(lines).toHaveLength(2);
    expect(lines[0]).toBe(
      "配置项\t大类\t差异类型\tVolvo XC60 Core · 本品 · 物料号 MAT-CORE · Germany · 2026 · own-catalog.xlsx\tVolvo XC60 Ultra · 竞品 / 外部 · Sales version ULTRA-SV · Germany · 2026 · competitor-site.xlsx · 来源人 alice\t业务备注",
    );
    expect(lines[1]).toContain("Blind Spot Information System\tSafety\t新增 1 / 规则推断 1\t不配备*\t标配\tVolvo XC60 Ultra 独有配置");
    expect((await screen.findByRole("status")).textContent).toContain("已复制选中行：Blind Spot Information System");
  });

  it("keeps the active row context visible while scanning the Excel matrix", async () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" columnMode="matrix" toolbarMode="simple" />);

    expect(screen.queryByLabelText("选中配置行摘要")).toBeNull();
    expect(screen.queryByRole("button", { name: "定位首个差异行" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: /显示差异行/ }));
    fireEvent.click(screen.getByRole("button", { name: "定位首个差异行" }));

    const summary = screen.getByLabelText("选中配置行摘要");
    expect(summary.textContent).toContain("选中配置");
    expect(summary.textContent).toContain("Blind Spot Information System");
    expect(summary.textContent).toContain("Safety · blind_spot");
    expect(screen.getByLabelText("选中行差异类型").textContent).toContain("新增 1");
    expect(screen.getByLabelText("选中行差异类型").textContent).toContain("规则推断 1");
    expect(screen.getByLabelText("选中行当前可见配置值").textContent).toContain("基准列 · Volvo XC60 Core");
    expect(screen.getByLabelText("选中行当前可见配置值").textContent).toContain("不配备*");
    expect(screen.getByLabelText("选中行当前可见配置值").textContent).toContain("Volvo XC60 Ultra");
    expect(screen.getByLabelText("选中行当前可见配置值").textContent).toContain("标配");
    const baseValue = screen.getByRole("button", {
      name: /从选中行摘要查看 Volvo XC60 Core Blind Spot Information System 的配置来源/,
    });
    const targetValue = screen.getByRole("button", {
      name: /从选中行摘要查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });

    expect(baseValue.querySelector(".comparison-active-row-evidence-marker")?.textContent).toBe("*");
    expect(targetValue.querySelector(".comparison-active-row-evidence-marker")).toBeNull();

    fireEvent.click(targetValue);

    expect(await screen.findByRole("dialog", { name: "配置来源证据" })).toBeTruthy();
    expect(screen.getAllByText("选中配置行摘要").length).toBeGreaterThan(0);
    expect(screen.getByText("当前值可追溯到来源文件坐标，可用于解释该配置行在当前配置列下的取值。")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "关闭" }));

    fireEvent.click(screen.getByRole("button", { name: "取消选中" }));

    expect(screen.queryByLabelText("选中配置行摘要")).toBeNull();
    expect(screen.getByRole("row", { name: "Blind Spot Information System，差异配置" }).getAttribute("aria-selected")).toBe("false");
  });

  it("does not show the first-difference jump when the current scope has no differences", () => {
    render(<ConfigComparisonTable data={compareDataOnlyCommon} columnMode="matrix" toolbarMode="simple" />);

    expect(screen.queryByRole("button", { name: "定位首个差异行" })).toBeNull();
    expect(screen.getByText("Shared speaker count")).toBeTruthy();
  });

  it("lets users pin the current row while scanning across trim values", () => {
    render(<ConfigComparisonTable data={compareData} cellEvidenceMode="compact" />);

    const row = screen.getByRole("row", { name: "Blind Spot Information System，差异配置" });

    fireEvent.click(row);

    expect(row.classList.contains("compare-row-active")).toBe(true);
    expect(row.getAttribute("aria-selected")).toBe("true");

    fireEvent.click(row);

    expect(row.classList.contains("compare-row-active")).toBe(false);
    expect(row.getAttribute("aria-selected")).toBe("false");
  });

  it("keeps duplicate feature-code rows as separate Excel rows", () => {
    const duplicateFeatureData: CompareResponse = {
      ...compareData,
      rows: [
        {
          ...compareData.rows[1],
          category: "Cargo",
          featureCode: "trunk_volume",
          featureName: "Trunk Volume V211",
          values: compareData.rows[1].values.map((value) => value ? { ...value, valueId: `${value.valueId}-v211`, rawValue: "370 L", displayValue: "370 L" } : null),
        },
        {
          ...compareData.rows[1],
          category: "Cargo",
          featureCode: "trunk_volume",
          featureName: "Trunk Volume V215",
          values: compareData.rows[1].values.map((value) => value ? { ...value, valueId: `${value.valueId}-v215`, rawValue: "442 L", displayValue: "442 L" } : null),
        },
      ],
      summary: {
        totalFeatures: 2,
        shownFeatures: 2,
        commonSameCount: 0,
        differentValueCount: 2,
        uniqueFeatureCount: 0,
        partialAvailableCount: 0,
        missingOrUnknownCount: 0,
        differenceCount: 2,
        confirmedDifferenceCount: 2,
        rawConfirmedDifferenceCount: 2,
        inferredDifferenceCount: 0,
        differenceCategories: ["Cargo"],
      },
      totalFeatures: 2,
      shownFeatures: 2,
    };
    const { container } = render(<ConfigComparisonTable data={duplicateFeatureData} columnMode="matrix" toolbarMode="simple" />);

    const featureAnchors = Array.from(container.querySelectorAll("[id^='config-feature-']")).map((element) => element.id);
    expect(featureAnchors).toHaveLength(2);
    expect(new Set(featureAnchors).size).toBe(2);
    expect(featureAnchors).not.toContain("config-feature-trunk-volume");

    const v211Row = screen.getByText("Trunk Volume V211").closest("tr") as HTMLTableRowElement;
    const v215Row = screen.getByText("Trunk Volume V215").closest("tr") as HTMLTableRowElement;

    fireEvent.click(v211Row);

    expect(v211Row.getAttribute("aria-selected")).toBe("true");
    expect(v215Row.getAttribute("aria-selected")).toBe("false");
  });

  it("labels the copy action as the current table when no row scope is active", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" columnMode="matrix" toolbarMode="simple" />);

    const tableActions = openTableActionsIfPresent();
    expect(tableActions).toBeTruthy();
    expect(within(tableActions as HTMLElement).getByRole("button", { name: "复制当前表格" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "复制当前筛选" })).toBeNull();

    fireEvent.click(within(tableActions as HTMLElement).getByRole("button", { name: "复制当前表格" }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });
    expect(screen.getByRole("status").textContent).toContain("已复制当前表格：3 配置行");
  });

  it("labels and copies the focused target columns without implying a row filter", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    render(
      <ConfigComparisonTable
        data={compareDataWithMiddleTrim}
        baseTrimId="trim-core"
        columnMode="matrix"
        targetTrimId="trim-ultra"
        toolbarMode="simple"
      />,
    );

    const tableActions = openTableActionsIfPresent();
    expect(tableActions).toBeTruthy();
    expect(within(tableActions as HTMLElement).getByRole("button", { name: "复制当前目标列" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "复制当前筛选" })).toBeNull();

    fireEvent.click(within(tableActions as HTMLElement).getByRole("button", { name: "复制当前目标列" }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });
    const copiedText = writeText.mock.calls[0]?.[0] as string;
    const header = copiedText.split("\n")[0];
    expect(header).toBe(`配置项\t大类\t差异类型\t${coreExportHeaderLabel}\t${ultraExportHeaderLabel}\t业务备注`);
    expect(header).not.toContain("Volvo XC60 Plus");
    expect(screen.getByRole("status").textContent).toContain("已复制当前目标列：3 配置行");
  });

  it("copies the current filtered table scope as Excel-ready TSV", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    render(
      <ConfigComparisonTable
        data={compareData}
        baseTrimId="trim-core"
        businessSummaryExport={[
          {
            targetTrimId: "trim-ultra",
            targetLabel: "Volvo XC60 Ultra",
            headline: "Ultra 相比 Core 的主要升级集中在安全配置。",
            mainUpgrades: ["安全：新增 Blind Spot Information System"],
            replacementsOrReductions: [],
            evidenceStatus: ["1 项来自规则推断，不是 Excel 原文"],
            evidenceRefs: [],
            recommendedUse: "引用前核对 source evidence。",
          },
        ]}
        businessSummaryUsage={{
          provider: "deepseek",
          model: "deepseek-chat",
          status: "ok",
          promptTokens: 100,
          completionTokens: 50,
          totalTokens: 150,
          finishReason: "stop",
        }}
        categoryFilter="Safety"
        searchValue="Blind"
        targetTrimId="trim-ultra"
      />,
    );

    fireEvent.click(deltaFilterButton(/规则推断 1/));
    fireEvent.click(screen.getByRole("button", { name: "复制当前筛选" }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });
    const copiedText = writeText.mock.calls[0]?.[0] as string;
    const lines = copiedText.split("\n");
    expect(lines).toHaveLength(2);
    expect(lines[0]).toBe(`配置项\t大类\t差异类型\t${coreExportHeaderLabel}\t${ultraExportHeaderLabel}\t业务备注`);
    expect(lines[1]).toContain("Blind Spot Information System\tSafety\t新增 1 / 规则推断 1\t不配备*\t标配\tVolvo XC60 Ultra 独有配置");
    expect(screen.getByRole("status").textContent).toContain("已复制当前筛选：1 项差异");
  });

  it("exports the current filtered table scope as xlsx payload", async () => {
    const createObjectURL = vi.fn(() => "blob:config-compare");
    const revokeObjectURL = vi.fn();
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      value: createObjectURL,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      configurable: true,
      value: revokeObjectURL,
    });
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    vi.mocked(api.exportEngineeringConfigCompareXlsx).mockResolvedValue(new Blob(["xlsx"]));

    render(
      <ConfigComparisonTable
        data={compareData}
        baseTrimId="trim-core"
        businessSummaryExport={[
          {
            targetTrimId: "trim-ultra",
            targetLabel: "Volvo XC60 Ultra",
            headline: "Ultra 相比 Core 的主要升级集中在安全配置。",
            mainUpgrades: ["安全：新增 Blind Spot Information System"],
            replacementsOrReductions: [],
            evidenceStatus: ["1 项来自规则推断，不是 Excel 原文"],
            evidenceRefs: [],
            recommendedUse: "引用前核对 source evidence。",
          },
        ]}
        businessSummaryUsage={{
          provider: "deepseek",
          model: "deepseek-chat",
          status: "ok",
          promptTokens: 100,
          completionTokens: 50,
          totalTokens: 150,
          finishReason: "stop",
        }}
        categoryFilter="Safety"
        searchValue="Blind"
        targetTrimId="trim-ultra"
      />,
    );

    fireEvent.click(deltaFilterButton(/规则推断 1/));
    fireEvent.click(screen.getByRole("button", { name: "导出 XLSX" }));

    await waitFor(() => {
      expect(api.exportEngineeringConfigCompareXlsx).toHaveBeenCalledTimes(1);
    });
    const payload = vi.mocked(api.exportEngineeringConfigCompareXlsx).mock.calls[0]?.[0];
    expect(payload).toEqual({
      trimIds: ["trim-core", "trim-ultra"],
      baseTrimId: "trim-core",
      versionScope: "published",
      filters: {
        deltaFilter: "INFERRED",
        category: "Safety",
        search: "Blind",
        targetTrimId: "trim-ultra",
        includeBusinessSummary: true,
      },
    });
    expect(anchorClick).toHaveBeenCalled();
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:config-compare");
    expect(screen.getByRole("status").textContent).toContain("已导出 XLSX：1 项差异");
  });

  it("exports the current filtered table scope as pdf payload", async () => {
    const createObjectURL = vi.fn(() => "blob:config-compare-pdf");
    const revokeObjectURL = vi.fn();
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      value: createObjectURL,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      configurable: true,
      value: revokeObjectURL,
    });
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    vi.mocked(api.exportEngineeringConfigComparePdf).mockResolvedValue(new Blob(["pdf"]));

    render(
      <ConfigComparisonTable
        data={compareData}
        baseTrimId="trim-core"
        businessSummaryExport={[
          {
            targetTrimId: "trim-ultra",
            targetLabel: "Volvo XC60 Ultra",
            headline: "Ultra 相比 Core 的主要升级集中在安全配置。",
            mainUpgrades: ["安全：新增 Blind Spot Information System"],
            replacementsOrReductions: [],
            evidenceStatus: ["1 项来自规则推断，不是 Excel 原文"],
            evidenceRefs: [],
            recommendedUse: "引用前核对 source evidence。",
          },
        ]}
        businessSummaryUsage={{
          provider: "deepseek",
          model: "deepseek-chat",
          status: "ok",
          promptTokens: 100,
          completionTokens: 50,
          totalTokens: 150,
          finishReason: "stop",
        }}
        categoryFilter="Safety"
        searchValue="Blind"
        targetTrimId="trim-ultra"
      />,
    );

    fireEvent.click(deltaFilterButton(/规则推断 1/));
    fireEvent.click(screen.getByRole("button", { name: "导出 PDF" }));

    await waitFor(() => {
      expect(api.exportEngineeringConfigComparePdf).toHaveBeenCalledTimes(1);
    });
    const payload = vi.mocked(api.exportEngineeringConfigComparePdf).mock.calls[0]?.[0];
    expect(payload).toEqual({
      trimIds: ["trim-core", "trim-ultra"],
      baseTrimId: "trim-core",
      versionScope: "published",
      filters: {
        deltaFilter: "INFERRED",
        category: "Safety",
        search: "Blind",
        targetTrimId: "trim-ultra",
        includeBusinessSummary: true,
      },
    });
    expect(anchorClick).toHaveBeenCalled();
    expect(createObjectURL).toHaveBeenCalled();
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:config-compare-pdf");
    expect(screen.getByRole("status").textContent).toContain("已导出 PDF：1 项差异");
  });

  it("keeps source evidence actions discoverable in each value cell", () => {
    const { container } = render(<ConfigComparisonTable data={compareData} />);
    const evidenceButtons = Array.from(container.querySelectorAll(".compare-cell-evidence-button"));

    expect(evidenceButtons.length).toBeGreaterThan(0);
    const inferredButton = screen.getByRole("button", {
      name: /查看 Volvo XC60 Core Blind Spot Information System 的配置来源/,
    });
    const sourceButton = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });
    const mergedButton = screen.getByRole("button", {
      name: /查看 Volvo XC60 Core Wheel size 的配置来源/,
    });
    const missingSourceButton = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Premium audio 的配置来源/,
    });

    expect(inferredButton.textContent).toBe("推断");
    expect(inferredButton.classList.contains("compare-cell-evidence-button--inferred")).toBe(true);
    expect(sourceButton.textContent).toBe("来源");
    expect(sourceButton.classList.contains("compare-cell-evidence-button--source")).toBe(true);
    expect(mergedButton.textContent).toBe("合并");
    expect(mergedButton.classList.contains("compare-cell-evidence-button--merged")).toBe(true);
    expect(missingSourceButton.textContent).toBe("缺源");
    expect(missingSourceButton.classList.contains("compare-cell-evidence-button--missing")).toBe(true);
  });

  it("marks manual overrides as a separate evidence state", async () => {
    const manualData: CompareResponse = {
      ...compareData,
      rows: compareData.rows.map((row) => row.featureCode !== "blind_spot" ? row : {
        ...row,
        values: row.values.map((cell, index) => index !== 1 || !cell ? cell : {
          ...cell,
          manualOverride: true,
          source: null,
        }),
      }),
    };

    render(<ConfigComparisonTable data={manualData} />);

    const manualButton = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });
    expect(manualButton.textContent).toBe("人工");
    expect(manualButton.classList.contains("compare-cell-evidence-button--manual")).toBe(true);

    fireEvent.click(manualButton);
    expect(await screen.findByText("该值为人工覆盖，不是原始文件单元格值。")).toBeTruthy();
  });

  it("uses low-noise evidence markers in compact cell evidence mode", () => {
    const { container } = render(<ConfigComparisonTable data={compareData} cellEvidenceMode="compact" />);

    expect(container.querySelector(".comparison-table")?.classList.contains("comparison-table--compact-cells")).toBe(true);

    const inferredCell = screen.getByRole("button", {
      name: /查看 Volvo XC60 Core Blind Spot Information System 的配置来源/,
    });
    const sourceCell = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });
    const mergedCell = screen.getByRole("button", {
      name: /查看 Volvo XC60 Core Wheel size 的配置来源/,
    });
    const missingSourceCell = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Premium audio 的配置来源/,
    });

    const inferredMarker = inferredCell.querySelector(".compare-cell-evidence-marker");
    const sourceMarker = sourceCell.querySelector(".compare-cell-evidence-marker");
    const mergedMarker = mergedCell.querySelector(".compare-cell-evidence-marker");
    const missingSourceMarker = missingSourceCell.querySelector(".compare-cell-evidence-marker");

    expect(inferredMarker?.textContent).toBe("*");
    expect(sourceMarker?.textContent).toBe("i");
    expect(mergedMarker?.textContent).toBe("M");
    expect(missingSourceMarker?.textContent).toBe("!");
    expect(inferredMarker?.tagName.toLowerCase()).toBe("span");
    expect(inferredMarker?.getAttribute("aria-hidden")).toBe("true");
    expect(inferredCell.classList.contains("compare-cell--evidence-compact")).toBe(true);
    expect(sourceCell.classList.contains("compare-cell--evidence-source")).toBe(true);
    expect(inferredCell.classList.contains("compare-cell--evidence-inferred")).toBe(true);
    expect(mergedCell.classList.contains("compare-cell--evidence-merged")).toBe(true);
    expect(missingSourceCell.classList.contains("compare-cell--evidence-missing")).toBe(true);
    expect(sourceMarker?.classList.contains("compare-cell-evidence-button--source")).toBe(true);
    expect(inferredMarker?.classList.contains("compare-cell-evidence-button--inferred")).toBe(true);
    expect(mergedMarker?.classList.contains("compare-cell-evidence-button--merged")).toBe(true);
    expect(missingSourceMarker?.classList.contains("compare-cell-evidence-button--missing")).toBe(true);

    fireEvent.click(inferredCell);

    expect(screen.getByRole("dialog", { name: "配置来源证据" })).toBeTruthy();
    expect(screen.getByText("该值为规则推断，不是 Excel 原文。")).toBeTruthy();
  });

  it("opens source evidence from the compact value cell itself", async () => {
    render(<ConfigComparisonTable data={compareData} cellEvidenceMode="compact" />);

    const row = screen.getByRole("row", { name: "Blind Spot Information System，差异配置" });
    const sourceCell = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });

    expect(sourceCell).toBeTruthy();
    expect(sourceCell.getAttribute("data-evidence-cell-action")).toBe("open");
    fireEvent.click(sourceCell as HTMLTableCellElement);

    expect(row.classList.contains("compare-row-active")).toBe(false);
    expect(await screen.findByRole("dialog", { name: "配置来源证据" })).toBeTruthy();
    expect(screen.getByText("当前值可追溯到来源文件坐标，可用于解释该配置行在当前配置列下的取值。")).toBeTruthy();
  });

  it("supports keyboard access on compact value cells without changing full mode cells", async () => {
    const { rerender } = render(<ConfigComparisonTable data={compareData} cellEvidenceMode="compact" />);

    const compactCell = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });

    expect(compactCell.getAttribute("role")).toBe("button");
    expect(compactCell.getAttribute("tabindex")).toBe("0");
    fireEvent.keyDown(compactCell as HTMLTableCellElement, { key: "Enter" });

    expect(await screen.findByRole("dialog", { name: "配置来源证据" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "关闭" }));

    rerender(<ConfigComparisonTable data={compareData} cellEvidenceMode="full" />);
    const fullModeSourceButton = screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Blind Spot Information System 的配置来源/,
    });
    const fullModeCell = fullModeSourceButton.closest("td");

    expect(fullModeCell?.getAttribute("role")).toBeNull();
    expect(fullModeCell?.getAttribute("tabindex")).toBeNull();
    fireEvent.keyDown(fullModeCell as HTMLTableCellElement, { key: "Enter" });

    expect(screen.queryByRole("dialog", { name: "配置来源证据" })).toBeNull();
  });

  it("filters rows by comparison type", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.click(deltaFilterButton(/待确认 1/));

    expect(screen.getByText("当前 1 项待确认")).toBeTruthy();
    expect(screen.getAllByText("1 项待确认").length).toBeGreaterThan(0);
    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(screen.getByRole("row", { name: "Premium audio，待确认配置" }).classList.contains("compare-row-pending")).toBe(true);
    expect(screen.queryByRole("row", { name: "Premium audio，差异配置" })).toBeNull();
    expect(screen.queryByText("Blind Spot Information System")).toBeNull();
    expect(screen.queryByText("Wheel size")).toBeNull();
  });

  it("filters rows by base-trim delta type", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.click(deltaFilterButton(/新增配置 1/));

    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.queryByText("Wheel size")).toBeNull();
    expect(screen.queryByText("Premium audio")).toBeNull();

    fireEvent.click(deltaFilterButton(/值变化 1/));

    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.queryByText("Blind Spot Information System")).toBeNull();
  });

  it("filters rows by evidence state", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.click(deltaFilterButton(/合并格 1/));

    expect(screen.getByText("当前 1 项证据")).toBeTruthy();
    expect(screen.getByLabelText("当前表格口径").textContent).toContain("范围合并格展开");
    expect(screen.getByText("1 项合并格")).toBeTruthy();
    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.queryByText("Blind Spot Information System")).toBeNull();
    expect(screen.queryByText("Premium audio")).toBeNull();

    fireEvent.click(deltaFilterButton(/来源问题 1/));

    expect(screen.getByText("当前 1 项证据")).toBeTruthy();
    expect(screen.getByLabelText("当前表格口径").textContent).toContain("范围来源问题");
    expect(screen.getByText("1 项来源问题")).toBeTruthy();
    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(screen.queryByText("Wheel size")).toBeNull();
  });

  it("shows the active base, target, search and filter scope above the table", () => {
    const onCategoryFilterChange = vi.fn();
    const onDeltaFilterChange = vi.fn();
    const onSearchChange = vi.fn();
    const onTargetTrimChange = vi.fn();
    render(
      <ConfigComparisonTable
        data={compareData}
        baseTrimId="trim-core"
        categoryFilter="Safety"
        deltaFilter="ADDED"
        searchValue="Blind"
        targetTrimId="trim-ultra"
        onCategoryFilterChange={onCategoryFilterChange}
        onDeltaFilterChange={onDeltaFilterChange}
        onSearchChange={onSearchChange}
        onTargetTrimChange={onTargetTrimChange}
      />,
    );

    const scope = screen.getByLabelText("当前表格口径");
    expect(scope.textContent).toContain("基准Volvo XC60 Core");
    expect(scope.textContent).toContain("目标Volvo XC60 Ultra");
    expect(scope.textContent).toContain("范围新增配置");
    expect(scope.textContent).toContain("大类Safety");
    expect(scope.textContent).toContain("搜索Blind");
    expect(scope.textContent).toContain("当前1 项差异");

    fireEvent.click(within(scope).getByRole("button", { name: "恢复全部配置" }));

    expect(onSearchChange).toHaveBeenCalledWith("");
    expect(onCategoryFilterChange).toHaveBeenCalledWith(null);
    expect(onDeltaFilterChange).toHaveBeenCalledWith("ALL");
    expect(onTargetTrimChange).not.toHaveBeenCalled();
  });

  it("keeps all rows by default and filters to all differences explicitly", () => {
    render(<ConfigComparisonTable data={compareDataWithCommonOnlyCategory} baseTrimId="trim-core" />);

    expect(screen.getByText("当前 4 项配置")).toBeTruthy();
    expect(screen.getByText("Shared speaker count")).toBeTruthy();
    expect(screen.getByRole("row", { name: "Shared speaker count，共同配置" }).classList.contains("compare-row-same")).toBe(true);

    fireEvent.click(deltaFilterButton(/共同配置 1/));

    expect(screen.getByText("当前 1 项配置")).toBeTruthy();
    expect(screen.getByText("1 项共同配置")).toBeTruthy();
    expect(screen.getByText("Shared speaker count")).toBeTruthy();

    fireEvent.click(deltaFilterButton(/差异项 3/));

    expect(screen.getByText("当前 3 项差异")).toBeTruthy();
    expect(screen.queryByText("Shared speaker count")).toBeNull();
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.getByText("Premium audio")).toBeTruthy();
  });

  it("keeps the category dropdown aligned with the active delta filter", async () => {
    render(<ConfigComparisonTable data={compareDataWithCommonOnlyCategory} baseTrimId="trim-core" />);

    const input = categoryFilterInput();
    fireEvent.focus(input);
    expect(within(screen.getByRole("listbox")).getByText("Comfort")).toBeTruthy();

    fireEvent.click(within(screen.getByRole("listbox")).getByText("Comfort"));
    expect(input.value).toBe("Comfort");
    expect(screen.getByText("Shared speaker count")).toBeTruthy();

    fireEvent.click(deltaFilterButton(/差异项 0/));

    await waitFor(() => {
      expect(screen.getByText("当前 3 项差异")).toBeTruthy();
    });
    expect(input.value).toBe("");
    fireEvent.focus(input);
    expect(within(screen.getByRole("listbox")).queryByText("Comfort")).toBeNull();
    fireEvent.keyDown(input, { key: "Escape" });
    expect(screen.queryByText("Shared speaker count")).toBeNull();
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
  });

  it("renders base-trim delta tags and category difference counts", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    expect(screen.getAllByText("新增 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("规则推断 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("值变化 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("待确认 1").length).toBeGreaterThan(0);
    expect(screen.getAllByText("1 项 / 1 项差异").length).toBeGreaterThan(0);
  });

  it("summarizes delta composition in category rows", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    const safetySummary = screen.getByLabelText("Safety 当前大类差异摘要");
    expect(safetySummary.textContent).toContain("新增 1");
    expect(safetySummary.textContent).toContain("推断 1");

    const wheelSummary = screen.getByLabelText("Wheel 当前大类差异摘要");
    expect(wheelSummary.textContent).toContain("值变化 1");

    const infotainmentSummary = screen.getByLabelText("Infotainment 当前大类差异摘要");
    expect(infotainmentSummary.textContent).toContain("待确认 1");
  });

  it("hides category summary drilldown chips in compact category summary mode", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" categorySummaryMode="compact" />);

    expect(screen.queryByLabelText("Safety 当前大类差异摘要")).toBeNull();
    expect(screen.getAllByText("1 项 / 1 项差异").length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: "折叠 Safety 配置大类" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "查看 Safety 大类摘要：推断项，共 1 项" })).toBeNull();
  });

  it("lets category summary chips drill into that category and delta type", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.click(screen.getByRole("button", { name: "查看 Safety 大类摘要：推断项，共 1 项" }));

    const scope = screen.getByLabelText("当前表格口径");
    expect(scope.textContent).toContain("范围规则推断");
    expect(scope.textContent).toContain("大类Safety");
    expect(scope.textContent).toContain("当前1 项差异");
    expect(deltaFilterButton(/规则推断 1/).classList.contains("is-active")).toBe(true);
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.queryByText("Wheel size")).toBeNull();
    expect(screen.getByRole("button", { name: "折叠 Safety 配置大类" }).getAttribute("aria-expanded")).toBe("true");
  });

  it("expands a collapsed category when its summary chip drills into rows", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.click(screen.getByRole("button", { name: "折叠 Safety 配置大类" }));
    expect(screen.queryByText("Blind Spot Information System")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "查看 Safety 大类摘要：推断项，共 1 项" }));

    expect(screen.getByLabelText("当前表格口径").textContent).toContain("范围规则推断");
    expect(screen.getByLabelText("当前表格口径").textContent).toContain("大类Safety");
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.getByRole("button", { name: "折叠 Safety 配置大类" }).getAttribute("aria-expanded")).toBe("true");
  });

  it("collapses and expands category groups with an explicit toggle button", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    const collapseButton = screen.getByRole("button", { name: "折叠 Safety 配置大类" });
    expect(collapseButton.getAttribute("aria-expanded")).toBe("true");

    fireEvent.click(collapseButton);

    expect(screen.queryByText("Blind Spot Information System")).toBeNull();
    const expandButton = screen.getByRole("button", { name: "展开 Safety 配置大类" });
    expect(expandButton.getAttribute("aria-expanded")).toBe("false");

    fireEvent.click(expandButton);

    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.getByRole("button", { name: "折叠 Safety 配置大类" }).getAttribute("aria-expanded")).toBe("true");
  });

  it("keeps common inferred rows out of inferred-difference filtering", () => {
    const dataWithCommonInferred: CompareResponse = {
      ...compareData,
      rows: [
        ...compareData.rows,
        {
          category: "Safety",
          featureCode: "shared_inferred_not_available",
          featureName: "Shared inferred not available",
          comparisonType: "COMMON_SAME",
          uniqueTrimIds: [],
          businessNote: "双方均按规则推断不配备。",
          values: [
            {
              valueId: "core-shared-inferred",
              rawValue: "",
              normalizedValue: null,
              availability: "NOT_AVAILABLE",
              unit: null,
              valueState: "blank",
              displayValue: "不配备*",
              inferred: true,
              inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
              confidence: 0.7,
              source: null,
            },
            {
              valueId: "ultra-shared-inferred",
              rawValue: "",
              normalizedValue: null,
              availability: "NOT_AVAILABLE",
              unit: null,
              valueState: "blank",
              displayValue: "不配备*",
              inferred: true,
              inferenceReason: "blank_as_not_equipped_by_eu_matrix_policy",
              confidence: 0.7,
              source: null,
            },
          ],
        },
      ],
      totalFeatures: 4,
      shownFeatures: 4,
    };

    render(<ConfigComparisonTable data={dataWithCommonInferred} baseTrimId="trim-core" />);

    expect(screen.getByText("Shared inferred not available")).toBeTruthy();

    fireEvent.click(deltaFilterButton(/规则推断 1/));

    expect(screen.getByText("当前 1 项差异")).toBeTruthy();
    expect(screen.getByText("1 项规则推断")).toBeTruthy();
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.queryByText("Shared inferred not available")).toBeNull();
  });

  it("does not treat matching blank unknown cells as pending differences in base mode", () => {
    const dataWithSharedBlankUnknown: CompareResponse = {
      ...compareData,
      rows: [
        ...compareData.rows,
        {
          category: "Parameters",
          featureCode: "shared_blank_unknown",
          featureName: "Shared blank unknown",
          comparisonType: "MISSING_OR_UNKNOWN",
          uniqueTrimIds: [],
          businessNote: "双方都是空白。",
          values: [
            {
              valueId: "core-shared-blank",
              rawValue: "",
              normalizedValue: null,
              availability: "UNKNOWN",
              unit: null,
              valueState: "blank",
              displayValue: "空白",
              inferred: false,
              source: null,
            },
            {
              valueId: "ultra-shared-blank",
              rawValue: "",
              normalizedValue: null,
              availability: "UNKNOWN",
              unit: null,
              valueState: "blank",
              displayValue: "空白",
              inferred: false,
              source: null,
            },
          ],
        },
      ],
      totalFeatures: 4,
      shownFeatures: 4,
    };
    render(<ConfigComparisonTable data={dataWithSharedBlankUnknown} baseTrimId="trim-core" />);

    fireEvent.click(deltaFilterButton(/待确认 1/));

    expect(screen.getByText("当前 1 项待确认")).toBeTruthy();
    expect(screen.getAllByText("1 项待确认").length).toBeGreaterThan(0);
    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(screen.queryByText("Shared blank unknown")).toBeNull();
  });

  it("filters rows by feature search", () => {
    render(<ConfigComparisonTable data={compareData} />);

    fireEvent.change(screen.getByPlaceholderText("搜索配置项 / 大类 / 值..."), {
      target: { value: "wheel" },
    });
    fireEvent.keyDown(screen.getByRole("combobox", { name: "搜索配置" }), { key: "Escape" });

    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.queryByText("Premium audio")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "清空 搜索配置" }));

    expect(screen.getByText("当前 3 项配置")).toBeTruthy();
    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "清空 搜索配置" })).toBeNull();
  });

  it("can search by selecting a suggested config option", () => {
    render(<ConfigComparisonTable data={compareData} />);

    const searchInput = screen.getByRole("combobox", { name: "搜索配置" });
    fireEvent.focus(searchInput);
    fireEvent.change(searchInput, {
      target: { value: "audio" },
    });
    fireEvent.click(within(screen.getByRole("listbox")).getByText("Premium audio"));

    expect(screen.getAllByText("Premium audio").length).toBeGreaterThan(0);
    expect(screen.queryByText("Wheel size")).toBeNull();
    expect(screen.getByText("当前 1 项配置")).toBeTruthy();
  });

  it("filters rows by cell display and raw values", () => {
    render(<ConfigComparisonTable data={compareData} />);

    fireEvent.change(screen.getByPlaceholderText("搜索配置项 / 大类 / 值..."), {
      target: { value: "Harman Kardon" },
    });
    fireEvent.keyDown(screen.getByRole("combobox", { name: "搜索配置" }), { key: "Escape" });

    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(screen.getAllByText("Harman Kardon").length).toBeGreaterThan(0);
    expect(screen.queryByText("Wheel size")).toBeNull();

    fireEvent.change(screen.getByRole("combobox", { name: "搜索配置" }), {
      target: { value: "标配" },
    });
    fireEvent.keyDown(screen.getByRole("combobox", { name: "搜索配置" }), { key: "Escape" });

    expect(screen.getAllByText("Blind Spot Information System").length).toBeGreaterThan(0);
    expect(screen.queryByText("Premium audio")).toBeNull();
  });

  it("supports controlled table search", () => {
    const onSearchChange = vi.fn();
    render(
      <ConfigComparisonTable
        data={compareData}
        searchValue="wheel"
        onSearchChange={onSearchChange}
      />,
    );

    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.queryByText("Premium audio")).toBeNull();

    fireEvent.change(screen.getByPlaceholderText("搜索配置项 / 大类 / 值..."), {
      target: { value: "blind" },
    });

    expect(onSearchChange).toHaveBeenCalledWith("blind");
  });

  it("matches category search against the normalized display label", () => {
    const dataWithWrappedCategory: CompareResponse = {
      ...compareData,
      rows: [
        {
          ...compareData.rows[0],
          category: "Safety\n Assist",
        },
        ...compareData.rows.slice(1),
      ],
    };
    render(<ConfigComparisonTable data={dataWithWrappedCategory} />);

    fireEvent.change(screen.getByPlaceholderText("搜索配置项 / 大类 / 值..."), {
      target: { value: "safety assist" },
    });
    fireEvent.keyDown(screen.getByRole("combobox", { name: "搜索配置" }), { key: "Escape" });

    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.queryByText("Wheel size")).toBeNull();
  });

  it("filters rows by category while keeping all delta types available by default", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    selectCategoryFilter("Wheel");

    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.queryByText("Blind Spot Information System")).toBeNull();
    expect(screen.queryByText("Premium audio")).toBeNull();
    expect(screen.getByText("当前 1 项配置")).toBeTruthy();
    expect(deltaFilterButton(/全部 1/)).toBeTruthy();
    expect(screen.getAllByText("值变化 1").length).toBeGreaterThan(0);
  });

  it("can restore the full table scope from combined filters", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.change(screen.getByPlaceholderText("搜索配置项 / 大类 / 值..."), {
      target: { value: "wheel" },
    });
    fireEvent.keyDown(screen.getByRole("combobox", { name: "搜索配置" }), { key: "Escape" });
    selectCategoryFilter("Wheel");
    fireEvent.click(deltaFilterButton(/值变化 1/));

    expect(screen.getByText("当前 1 项差异")).toBeTruthy();
    expect(screen.getByText("Wheel size")).toBeTruthy();
    expect(screen.queryByText("Premium audio")).toBeNull();

    fireEvent.click(within(screen.getByLabelText("当前表格口径")).getByRole("button", { name: "恢复全部配置" }));

    expect((screen.getByPlaceholderText("搜索配置项 / 大类 / 值...") as HTMLInputElement).value).toBe("");
    expect(categoryFilterInput().value).toBe("");
    expect(deltaFilterButton(/全部 3/).classList.contains("is-active")).toBe(true);
    expect(screen.getByText("当前 3 项配置")).toBeTruthy();
    expect(screen.getByText("Premium audio")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "恢复全部配置" })).toBeNull();
  });

  it("offers a restore action inside the empty table state", () => {
    render(<ConfigComparisonTable data={compareData} baseTrimId="trim-core" />);

    fireEvent.change(screen.getByPlaceholderText("搜索配置项 / 大类 / 值..."), {
      target: { value: "no matching config" },
    });

    expect(screen.getByText("当前筛选没有配置项")).toBeTruthy();
    expect(screen.getByText("请调整搜索、大类或差异范围；空白 / 待确认项不会被自动当成无配置隐藏。")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "清空筛选并显示全部配置" }));

    expect((screen.getByPlaceholderText("搜索配置项 / 大类 / 值...") as HTMLInputElement).value).toBe("");
    expect(screen.getByText("当前 3 项配置")).toBeTruthy();
    expect(screen.getByText("Blind Spot Information System")).toBeTruthy();
    expect(screen.queryByText("当前筛选没有配置项")).toBeNull();
  });

  it("uses row and dropdown wording in the simple empty table state", () => {
    const onSearchChange = vi.fn();
    render(
      <ConfigComparisonTable
        data={compareData}
        searchValue="no matching config"
        toolbarMode="simple"
        onSearchChange={onSearchChange}
      />,
    );

    expect(screen.getByText("当前筛选没有配置行")).toBeTruthy();
    expect(screen.getByText("请从搜索下拉选择配置项，或调整大类 / 行筛选；空白 / 待确认行不会被自动当成无配置隐藏。")).toBeTruthy();
    expect(screen.queryByText("请调整搜索、大类或差异范围；空白 / 待确认项不会被自动当成无配置隐藏。")).toBeNull();

    const emptyState = screen.getByText("当前筛选没有配置行").closest(".comparison-empty-table-state");
    expect(emptyState).toBeTruthy();
    fireEvent.click(within(emptyState as HTMLElement).getByRole("button", { name: "恢复全部配置行" }));

    expect(onSearchChange).toHaveBeenCalledWith("");
  });

  it("reports controlled category filter changes", () => {
    const onCategoryFilterChange = vi.fn();
    render(
      <ConfigComparisonTable
        data={compareData}
        categoryFilter={null}
        onCategoryFilterChange={onCategoryFilterChange}
      />,
    );

    selectCategoryFilter("Safety");

    expect(onCategoryFilterChange).toHaveBeenCalledWith("Safety");
  });

  it("opens source evidence for inferred values", async () => {
    render(<ConfigComparisonTable data={compareData} />);

    fireEvent.click(screen.getByRole("button", {
      name: /查看 Volvo XC60 Core Blind Spot Information System 的配置来源/,
    }));

    expect(await screen.findByRole("dialog", { name: "配置来源证据" })).toBeTruthy();
    expect(await screen.findByText("该值为规则推断，不是 Excel 原文。")).toBeTruthy();
    expect(screen.getByText("空白")).toBeTruthy();
    expect(screen.getAllByText(/blank_as_not_equipped_by_eu_matrix_policy/).length).toBeGreaterThan(1);
    expect(screen.getAllByText("0.7").length).toBeGreaterThan(0);
    expect(screen.getAllByText("F128").length).toBeGreaterThan(1);
  });

  it("explains merged-cell expansion in source evidence", async () => {
    render(<ConfigComparisonTable data={compareData} />);

    fireEvent.click(screen.getByRole("button", {
      name: /查看 Volvo XC60 Core Wheel size 的配置来源/,
    }));

    expect(await screen.findByText("该值来自合并单元格展开。")).toBeTruthy();
    expect(screen.getAllByText("D11:F11").length).toBeGreaterThan(0);
    expect(screen.getAllByText("D11").length).toBeGreaterThan(0);
    expect(screen.getByText(/当前配置列使用 E11/)).toBeTruthy();
  });

  it("degrades cleanly when published config has no source evidence", async () => {
    render(<ConfigComparisonTable data={compareData} />);

    fireEvent.click(screen.getByRole("button", {
      name: /查看 Volvo XC60 Ultra Premium audio 的配置来源/,
    }));

    expect(await screen.findByText("当前已发布配置暂无来源证据。")).toBeTruthy();
    expect(screen.getAllByText("Harman Kardon").length).toBeGreaterThan(1);
  });
});
