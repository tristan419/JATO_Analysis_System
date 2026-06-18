// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  EmptyState,
  FileDropzone,
  SheetGroupedPreview,
  StatusMetricCard,
  type SheetGroupedPreviewColumn,
  type SheetGroupedPreviewGroup,
} from "../../components";
import { downloadBlob } from "../../utils/download";
import { formatDateTime } from "../../utils/timeFormatting";

describe("FileDropzone", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("selects a file through the hidden input", () => {
    const onFile = vi.fn();
    render(
      <FileDropzone
        accept=".xlsx"
        label="发运清单 Excel"
        hint="拖拽 / 点击选择发运清单"
        file={null}
        onFile={onFile}
      />,
    );

    const input = screen.getByText("发运清单 Excel")
      .closest(".dropzone")
      ?.querySelector("input[type='file']");
    const file = new File(["hello"], "shipment.xlsx", {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    fireEvent.change(input as HTMLInputElement, { target: { files: [file] } });

    expect(onFile).toHaveBeenCalledWith(file);
  });

  it("accepts dropped files and resets drag styling", () => {
    const onFile = vi.fn();
    render(
      <FileDropzone
        accept=".pdf"
        label="WVTA 关联 PDF"
        hint="拖拽 / 点击选择 PDF"
        file={null}
        onFile={onFile}
      />,
    );

    const dropzone = screen.getByText("WVTA 关联 PDF").closest(".dropzone") as HTMLElement;
    const file = new File(["pdf"], "wvta.pdf", { type: "application/pdf" });

    fireEvent.dragOver(dropzone);
    expect(dropzone.className).toContain("dragover");

    fireEvent.drop(dropzone, { dataTransfer: { files: [file] } });
    expect(onFile).toHaveBeenCalledWith(file);
    expect(dropzone.className).not.toContain("dragover");
  });

  it("clears the selected file without reopening the file picker", () => {
    const onClear = vi.fn();
    const inputClick = vi.spyOn(HTMLInputElement.prototype, "click").mockImplementation(() => undefined);
    render(
      <FileDropzone
        accept=".zip,.rar"
        label="ZIP/RAR 文件包"
        hint="拖拽 / 点击选择压缩包"
        file={new File(["zip"], "cocs.zip", { type: "application/zip" })}
        onFile={() => undefined}
        onClear={onClear}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "清除ZIP/RAR 文件包" }));

    expect(onClear).toHaveBeenCalledTimes(1);
    expect(inputClick).not.toHaveBeenCalled();
  });
});

describe("StatusMetricCard", () => {
  afterEach(() => {
    cleanup();
  });

  it("renders as a button when clickable and calls onClick", () => {
    const onClick = vi.fn();
    render(<StatusMetricCard label="冲突" value={4} tone="warning" active onClick={onClick} />);

    const button = screen.getByRole("button", { name: /冲突\s*4/ });
    fireEvent.click(button);

    expect(onClick).toHaveBeenCalledTimes(1);
    expect(button.style.borderColor).toBe("rgb(37, 99, 235)");
  });

  it("renders as static content when no click handler is provided", () => {
    render(<StatusMetricCard label="填充" value={518} tone="success" />);

    expect(screen.queryByRole("button")).toBeNull();
    expect(screen.getByText("填充")).toBeTruthy();
    expect(screen.getByText("518")).toBeTruthy();
  });
});

describe("SheetGroupedPreview", () => {
  afterEach(() => {
    cleanup();
  });

  type PreviewRow = {
    material: string;
    status: string;
  };

  type PreviewGroup = SheetGroupedPreviewGroup<PreviewRow> & {
    sheetName: string;
  };

  const columns: SheetGroupedPreviewColumn[] = [
    { key: "material", label: "物料号组" },
    { key: "status", label: "状态" },
  ];

  const groups: PreviewGroup[] = [
    {
      key: "job-1:Sheet1",
      sheetName: "Sheet1",
      title: "Sheet1",
      metrics: [{ label: "识别", value: 2 }],
      rows: [{ material: "T7000Z5**MY0013", status: "冲突" }],
    },
    {
      key: "job-1:Sheet3",
      sheetName: "Sheet3",
      title: "Sheet3",
      metrics: [{ label: "识别", value: 1 }],
      rows: [{ material: "T71506J**MH0001", status: "已填充" }],
      truncated: true,
      previewLimit: 1,
    },
  ];

  function renderPreview({
    expandedGroupKeys = new Set<string>(),
    previewTouched = false,
    onToggleGroup = () => undefined,
  }: {
    expandedGroupKeys?: Set<string>;
    previewTouched?: boolean;
    onToggleGroup?: (group: PreviewGroup, expanded: boolean) => void;
  } = {}) {
    render(
      <SheetGroupedPreview<PreviewRow, PreviewGroup>
        title="填充预览"
        groups={groups}
        columns={columns}
        expandedGroupKeys={expandedGroupKeys}
        previewTouched={previewTouched}
        emptyText="当前筛选没有预览记录"
        onToggleGroup={onToggleGroup}
        renderTruncated={(group) => `Sheet ${group.sheetName} limited to ${group.previewLimit} rows`}
        renderRow={(row, index, group) => (
          <tr key={`${group.sheetName}-${index}`}>
            <td>{row.material}</td>
            <td>{row.status}</td>
          </tr>
        )}
        toolbar={<span>全部 · 2 / 2 个 Sheet</span>}
      />,
    );
  }

  it("shows an empty state when there are no groups", () => {
    render(
      <SheetGroupedPreview<PreviewRow, PreviewGroup>
        title="填充预览"
        groups={[]}
        columns={columns}
        expandedGroupKeys={new Set()}
        previewTouched={false}
        emptyText="当前筛选没有预览记录"
        onToggleGroup={() => undefined}
        renderRow={(row) => (
          <tr>
            <td>{row.material}</td>
            <td>{row.status}</td>
          </tr>
        )}
      />,
    );

    expect(screen.getByText("当前筛选没有预览记录")).toBeTruthy();
  });

  it("opens the first group by default until the preview has been touched", () => {
    renderPreview();

    expect(screen.getByText("T7000Z5**MY0013")).toBeTruthy();
    expect(screen.queryByText("T71506J**MH0001")).toBeNull();
  });

  it("uses expandedGroupKeys after the preview has been touched", () => {
    renderPreview({
      expandedGroupKeys: new Set(["job-1:Sheet3"]),
      previewTouched: true,
    });

    expect(screen.queryByText("T7000Z5**MY0013")).toBeNull();
    expect(screen.getByText("T71506J**MH0001")).toBeTruthy();
    expect(screen.getByText("Sheet Sheet3 limited to 1 rows")).toBeTruthy();
  });

  it("reports the current expanded state when a group header is clicked", () => {
    const onToggleGroup = vi.fn();
    renderPreview({ onToggleGroup });

    fireEvent.click(screen.getByRole("button", { name: /Sheet1/ }));
    fireEvent.click(screen.getByRole("button", { name: /Sheet3/ }));

    expect(onToggleGroup).toHaveBeenNthCalledWith(1, groups[0], true);
    expect(onToggleGroup).toHaveBeenNthCalledWith(2, groups[1], false);
  });
});

describe("EmptyState", () => {
  afterEach(() => {
    cleanup();
  });

  it("renders reusable empty-state content", () => {
    render(<EmptyState text="暂无填充记录" />);

    expect(screen.getByText("暂无填充记录")).toBeTruthy();
  });
});

describe("shared workbench utilities", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("formats invalid or missing date values defensively", () => {
    expect(formatDateTime(null)).toBe("-");
    expect(formatDateTime(undefined)).toBe("-");
    expect(formatDateTime("not-a-date")).toBe("-");
    expect(formatDateTime("2026-01-02T03:04:05Z")).toContain("2026");
  });

  it("downloads a blob with the requested filename", () => {
    const blob = new Blob(["report"], { type: "text/plain" });
    const createObjectURL = vi.fn(() => "blob:unit-test");
    const revokeObjectURL = vi.fn();
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: createObjectURL });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: revokeObjectURL });

    downloadBlob(blob, "report.txt");

    expect(createObjectURL).toHaveBeenCalledWith(blob);
    expect(click).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:unit-test");
  });
});
