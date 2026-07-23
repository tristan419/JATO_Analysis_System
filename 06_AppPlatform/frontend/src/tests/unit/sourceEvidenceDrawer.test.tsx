// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { SourceEvidenceDrawer, type SourceEvidenceSelection } from "../../components/SourceEvidenceDrawer";

const baseSelection: SourceEvidenceSelection = {
  row: {
    category: "Safety",
    featureCode: "rear_camera",
    featureName: "Rear camera / 倒车影像",
    comparisonType: "UNIQUE_OR_PARTIAL",
    uniqueTrimIds: [],
    businessNote: "配置差异",
    values: [],
  },
  trim: {
    trimId: "trim-basic",
    fullTrimName: "Basic-FWD",
    brand: "OMODA",
    modelName: "T19C MY ICE",
    trimName: "Basic-FWD",
    sourceFileName: "compare-sample.xlsx",
    sourceUploadId: "source-compare-sample",
  },
  cell: {
    valueId: "value-1",
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
      rowNumber: 69,
      columnNumber: 6,
      columnLetter: "F",
      cell: "F69",
      sourceCell: "F69",
      mergedRange: null,
    },
  },
};

const originalClipboard = navigator.clipboard;

describe("SourceEvidenceDrawer", () => {
  afterEach(() => {
    cleanup();
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: originalClipboard,
    });
  });

  it("shows blank raw values as 空白", () => {
    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          selectionReason: "业务摘要优先打开 Basic-FWD 的推断值。",
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("原始值")).toBeTruthy();
    expect(screen.getByText("空白")).toBeTruthy();
    expect(screen.getByText("部分具备 (UNIQUE_OR_PARTIAL)")).toBeTruthy();
    expect(screen.getByText("来源证据")).toBeTruthy();
    expect(screen.getByText("配置行")).toBeTruthy();
    expect(screen.getByText("当前配置列取值")).toBeTruthy();
    expect(screen.getAllByText("来源文件").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("compare-sample.xlsx").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("来源 ID").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("source-compare-sample").length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("原始值类型")).toBeTruthy();
    expect(screen.getByText("配置状态")).toBeTruthy();
    expect(screen.getByText("是否规则推断")).toBeTruthy();
    expect(screen.getByText("推断规则")).toBeTruthy();
    expect(screen.getByText("置信度")).toBeTruthy();
    expect(screen.getByText("空白 (blank)")).toBeTruthy();
    expect(screen.getByText("不配备 (NOT_AVAILABLE)")).toBeTruthy();
    expect(screen.getByText("是")).toBeTruthy();
    expect(screen.getByText("规则推断")).toBeTruthy();
    expect(screen.queryByText("Source Evidence")).toBeNull();
    expect(screen.queryByText("valueState")).toBeNull();
    expect(screen.queryByText("availability")).toBeNull();
    expect(screen.queryByText("inferred")).toBeNull();
    expect(screen.queryByText("inferenceReason")).toBeNull();
    expect(screen.queryByText("confidence")).toBeNull();
    expect(screen.getByText("有来源坐标")).toBeTruthy();
    expect(screen.getByText("当前显示值由规则推断生成，不是 Excel 单元格原文，需要回看推断规则和置信度。")).toBeTruthy();
    expect(screen.getByText("触发来源")).toBeTruthy();
    expect(screen.getByText("证据选择原因")).toBeTruthy();
    expect(screen.getAllByText("业务摘要优先打开 Basic-FWD 的推断值。")).toHaveLength(2);
    expect(screen.getByText("该值为规则推断，不是 Excel 原文。")).toBeTruthy();
  });

  it("copies a structured evidence packet for audit handoff", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          selectionReason: "业务摘要优先打开 Basic-FWD 的推断值。",
        }}
        onClose={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "复制当前配置来源证据" }));

    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copiedText = writeText.mock.calls[0][0];
    expect(copiedText).toContain("Config source evidence");
    expect(copiedText).toContain("Feature: Rear camera / 倒车影像");
    expect(copiedText).toContain("Feature EN: Rear camera");
    expect(copiedText).toContain("Feature ZH: 倒车影像");
    expect(copiedText).toContain("Comparison Type: 部分具备 (UNIQUE_OR_PARTIAL)");
    expect(copiedText).toContain("Business Note: 配置差异");
    expect(copiedText).toContain("Config column: Basic-FWD");
    expect(copiedText).toContain("Source File: compare-sample.xlsx");
    expect(copiedText).toContain("Source ID: source-compare-sample");
    expect(copiedText).toContain("Display Value: 不配备*");
    expect(copiedText).toContain("Raw Value: 空白");
    expect(copiedText).toContain("Availability: 不配备 (NOT_AVAILABLE)");
    expect(copiedText).toContain("Inferred: 是");
    expect(copiedText).toContain("Inference Reason: blank_as_not_equipped_by_eu_matrix_policy");
    expect(copiedText).toContain("Confidence: 0.7");
    expect(copiedText).toContain("Sheet: T19C MY ICE ");
    expect(copiedText).toContain("Source Type: 工作簿 / 表格");
    expect(copiedText).toContain("Page: -");
    expect(copiedText).toContain("OCR Engine: -");
    expect(copiedText).toContain("Cell: F69");
    expect(copiedText).toContain("Source Evidence: available");
    expect(copiedText).toContain("Selection Reason: 业务摘要优先打开 Basic-FWD 的推断值。");
    expect(await screen.findByText("证据包已复制。")).toBeTruthy();
  });

  it("surfaces OCR review notes from the compare row business note", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    const reviewNote = "需核对：OCR 值单元格像配置项文本（seats），可能是特征名换行或单位被切入值列。";

    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          row: {
            ...baseSelection.row,
            featureName: "Number of seats / 座椅",
            businessNote: reviewNote,
          },
          cell: {
            ...baseSelection.cell!,
            rawValue: "seats",
            normalizedValue: "seats",
            displayValue: "seats",
            availability: "VALUE",
            valueState: "text_value",
            inferred: false,
            inferenceReason: null,
            confidence: null,
            source: {
              ...baseSelection.cell!.source!,
              sourceType: "image_ocr",
              pageNumber: 1,
              ocrEngine: "paddleocr",
            },
          },
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("业务备注")).toBeTruthy();
    expect(screen.getAllByText(reviewNote).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("需核对说明")).toBeTruthy();
    expect(screen.getByText("该配置行带需核对说明。")).toBeTruthy();
    expect(screen.getByText("OCR 识别")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "复制当前配置来源证据" }));

    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copiedText = writeText.mock.calls[0][0];
    expect(copiedText).toContain(`Business Note: ${reviewNote}`);
    expect(copiedText).toContain("Source Type: 图片 OCR (image_ocr)");
    expect(copiedText).toContain("OCR Engine: paddleocr");
  });

  it("clears copy feedback when switching to a different evidence selection", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });
    const { rerender } = render(<SourceEvidenceDrawer selection={baseSelection} onClose={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "复制当前配置来源证据" }));

    expect(await screen.findByText("证据包已复制。")).toBeTruthy();

    rerender(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          row: {
            ...baseSelection.row,
            featureCode: "speaker_count",
            featureName: "Speaker count / 扬声器数量",
          },
          cell: {
            ...baseSelection.cell!,
            valueId: "value-speaker-count",
            rawValue: "8",
            normalizedValue: "8",
            availability: "VALUE",
            valueState: "numeric_value",
            displayValue: "8",
            inferred: false,
            inferenceReason: null,
            confidence: null,
          },
        }}
        onClose={vi.fn()}
      />,
    );

    await waitFor(() => expect(screen.queryByText("证据包已复制。")).toBeNull());
    expect(screen.getByText("Speaker count")).toBeTruthy();
    expect(screen.getByText("扬声器数量")).toBeTruthy();
  });

  it("shows a local fallback when evidence copying is unavailable", async () => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: undefined,
    });

    render(<SourceEvidenceDrawer selection={baseSelection} onClose={vi.fn()} />);

    fireEvent.click(screen.getByRole("button", { name: "复制当前配置来源证据" }));

    expect(await screen.findByText("当前浏览器不支持复制，请手动选中证据内容。")).toBeTruthy();
  });

  it("keeps literal dash raw values distinct from blanks", () => {
    const selection: SourceEvidenceSelection = {
      ...baseSelection,
      cell: {
        ...baseSelection.cell!,
        rawValue: "-",
        normalizedValue: "not_available",
        valueState: "marker_value",
        displayValue: "不配备",
        inferred: false,
        inferenceReason: null,
        confidence: null,
      },
    };

    render(<SourceEvidenceDrawer selection={selection} onClose={vi.fn()} />);

    expect(screen.getAllByText("-").length).toBeGreaterThan(0);
    expect(screen.getByText("配置标记 (marker_value)")).toBeTruthy();
    expect(screen.getAllByText("否").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("原始记录")).toBeTruthy();
    expect(screen.queryByText("该值为规则推断，不是 Excel 原文。")).toBeNull();
  });

  it("explains null cells as missing config records instead of not equipped", () => {
    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          cell: null,
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("当前配置列没有配置值记录。")).toBeTruthy();
    expect(screen.getByText("这表示该字段在当前已发布配置中缺失，需要回看来源或重新消化数据；不能直接等同于不配备。")).toBeTruthy();
    expect(screen.getByText("缺配置值")).toBeTruthy();
    expect(screen.getByText("缺来源证据")).toBeTruthy();
    expect(screen.getByText("当前配置列在已发布配置中没有配置值记录，不能直接等同于不配备。")).toBeTruthy();
    expect(screen.getByText("当前已发布配置暂无来源证据。")).toBeTruthy();
    expect(screen.queryByText("该值为规则推断，不是 Excel 原文。")).toBeNull();
  });

  it("falls back to source confidence when the value confidence is missing", () => {
    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          cell: {
            ...baseSelection.cell!,
            confidence: null,
            source: {
              ...baseSelection.cell!.source!,
              confidence: 0.72,
            },
          },
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("0.72")).toBeTruthy();
    expect(screen.getByText("该值为规则推断，不是 Excel 原文。")).toBeTruthy();
  });

  it("shows image OCR source type, page, and engine", () => {
    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          cell: {
            ...baseSelection.cell!,
            rawValue: "●",
            normalizedValue: "standard",
            displayValue: "标配",
            inferred: false,
            inferenceReason: null,
            confidence: null,
            source: {
              ...baseSelection.cell!.source!,
              sourceType: "image_ocr",
              pageNumber: 2,
              ocrEngine: "paddleocr",
            },
          },
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("OCR 识别")).toBeTruthy();
    expect(screen.getByText("来源类型")).toBeTruthy();
    expect(screen.getByText("图片 OCR (image_ocr)")).toBeTruthy();
    expect(screen.getByText("页码")).toBeTruthy();
    expect(screen.getByText("2")).toBeTruthy();
    expect(screen.getByText("OCR 引擎")).toBeTruthy();
    expect(screen.getByText("paddleocr")).toBeTruthy();
    expect(screen.getByText("该值来自 OCR 消化结果。")).toBeTruthy();
    expect(screen.getByText("请结合来源类型、页码和 OCR 引擎回看原始 PDF / 图片，引用到卖点前建议核对原文。")).toBeTruthy();
  });

  it("shows PDF OCR source type, page, engine, and copied evidence fields", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          trim: {
            ...baseSelection.trim,
            sourceFileName: "scan-config.pdf",
            sourceUploadId: "source-scan-config",
          },
          cell: {
            ...baseSelection.cell!,
            rawValue: "O",
            normalizedValue: "optional",
            displayValue: "选装",
            availability: "OPTIONAL",
            valueState: "marker_value",
            inferred: false,
            inferenceReason: null,
            confidence: null,
            source: {
              sheetName: "PDF OCR Page 1",
              rowNumber: 20,
              columnNumber: 4,
              columnLetter: "D",
              cell: "P1OCRR20C4",
              sourceCell: "P1OCRR20C4",
              mergedRange: null,
              sourceType: "pdf_ocr",
              pageNumber: 1,
              ocrEngine: "paddleocr",
            },
          },
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("扫描 PDF OCR (pdf_ocr)")).toBeTruthy();
    expect(screen.getByText("1")).toBeTruthy();
    expect(screen.getByText("paddleocr")).toBeTruthy();
    expect(screen.getAllByText("P1OCRR20C4")).toHaveLength(2);
    expect(screen.getByText("该值来自 OCR 消化结果。")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "复制当前配置来源证据" }));

    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    const copiedText = writeText.mock.calls[0][0];
    expect(copiedText).toContain("Source File: scan-config.pdf");
    expect(copiedText).toContain("Source ID: source-scan-config");
    expect(copiedText).toContain("Source Type: 扫描 PDF OCR (pdf_ocr)");
    expect(copiedText).toContain("Page: 1");
    expect(copiedText).toContain("OCR Engine: paddleocr");
    expect(copiedText).toContain("Cell: P1OCRR20C4");
  });

  it("summarizes merged cell expansion before source details", () => {
    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          row: {
            ...baseSelection.row,
            featureName: "Number of seats / 座椅",
            comparisonType: "DIFFERENT_VALUE",
          },
          cell: {
            ...baseSelection.cell!,
            rawValue: "5",
            normalizedValue: "5",
            availability: "VALUE",
            valueState: "numeric_value",
            displayValue: "5",
            inferred: false,
            inferenceReason: null,
            confidence: null,
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
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("合并格展开")).toBeTruthy();
    expect(screen.getByText("当前显示值来自横向合并格展开，原始值应追溯到原始单元格和合并范围。")).toBeTruthy();
    expect(screen.getByText("原始单元格")).toBeTruthy();
    expect(screen.getByText("合并范围")).toBeTruthy();
    expect(screen.getByText("该值来自合并单元格展开。")).toBeTruthy();
    expect(screen.getByText("D11:F11")).toBeTruthy();
    expect(screen.getByText("D11")).toBeTruthy();
  });

  it("shows a source evidence gap when a published value has no source", () => {
    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          cell: {
            ...baseSelection.cell!,
            rawValue: "Harman Kardon",
            normalizedValue: "harman kardon",
            availability: "VALUE",
            valueState: "text_value",
            displayValue: "Harman Kardon",
            inferred: false,
            inferenceReason: null,
            confidence: null,
            source: null,
          },
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("缺来源证据")).toBeTruthy();
    expect(screen.getByText("当前值存在于已发布配置，但暂无来源证据，无法在页面内追溯到原始文件坐标。")).toBeTruthy();
    expect(screen.getByText("当前已发布配置暂无来源证据。")).toBeTruthy();
  });

  it("labels manual overrides without reusing the original file coordinate", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <SourceEvidenceDrawer
        selection={{
          ...baseSelection,
          cell: {
            ...baseSelection.cell!,
            rawValue: "O",
            normalizedValue: "optional",
            availability: "OPTIONAL",
            valueState: "marker_value",
            displayValue: "选装",
            inferred: false,
            inferenceReason: null,
            confidence: null,
            manualOverride: true,
            source: null,
          },
        }}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByText("人工覆盖")).toBeTruthy();
    expect(screen.getByText("该值为人工覆盖，不是原始文件单元格值。")).toBeTruthy();
    expect(screen.getByText("当前值为人工覆盖，故不使用原始文件坐标作为证据。")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "复制当前配置来源证据" }));
    await waitFor(() => expect(writeText).toHaveBeenCalledTimes(1));
    expect(writeText.mock.calls[0][0]).toContain("Manual Override: 是");
    expect(writeText.mock.calls[0][0]).toContain("Source Evidence: missing");
  });
});
