// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ConfirmDialog } from "../../components/ConfirmDialog";

interface RenderDialogOptions {
  submitting?: boolean;
  error?: {
    title: string;
    message: string;
    details?: Array<{ label: string; value: string }>;
    sourceFeedback?: string;
    retryBlocked?: boolean;
  } | null;
  onCancel?: () => void;
  onConfirm?: () => void;
}

function renderDialog({
  submitting = false,
  error = null,
  onCancel = vi.fn(),
  onConfirm = vi.fn(),
}: RenderDialogOptions = {}) {
  return render(
    <ConfirmDialog
      title="确认生成完整 Candidate"
      description="请检查本批次的历史处理选择。"
      cancelLabel="返回修改"
      confirmLabel="确认并生成 Candidate"
      loadingLabel="正在生成 Candidate"
      submitting={submitting}
      error={error}
      onCancel={onCancel}
      onConfirm={onConfirm}
    >
      <p>整国替换历史，不与 active 累加。</p>
    </ConfirmDialog>,
  );
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("ConfirmDialog", () => {
  it("exposes accessible dialog semantics and initially focuses the safe cancel action", async () => {
    renderDialog();

    const dialog = screen.getByRole("dialog", { name: "确认生成完整 Candidate" });
    const cancelButton = screen.getByRole("button", { name: "返回修改" });

    expect(dialog.getAttribute("aria-modal")).toBe("true");
    expect(dialog.getAttribute("aria-labelledby")).toBeTruthy();
    expect(dialog.getAttribute("aria-describedby")).toBeTruthy();
    await waitFor(() => {
      expect(document.activeElement).toBe(cancelButton);
    });
  });

  it("closes with Escape or a backdrop click when not submitting", () => {
    const onCancel = vi.fn();
    renderDialog({ onCancel });

    fireEvent.keyDown(document, { key: "Escape" });
    expect(onCancel).toHaveBeenCalledTimes(1);

    const overlay = document.querySelector(".confirm-dialog-overlay");
    expect(overlay).toBeTruthy();
    fireEvent.click(overlay as HTMLElement);
    expect(onCancel).toHaveBeenCalledTimes(2);
  });

  it("wraps keyboard focus between the first and last dialog actions", async () => {
    renderDialog();

    const cancelButton = screen.getByRole("button", { name: "返回修改" });
    const confirmButton = screen.getByRole("button", { name: "确认并生成 Candidate" });
    await waitFor(() => {
      expect(document.activeElement).toBe(cancelButton);
    });

    fireEvent.keyDown(document, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(confirmButton);

    fireEvent.keyDown(document, { key: "Tab" });
    expect(document.activeElement).toBe(cancelButton);
  });

  it("restores focus to the trigger after the dialog unmounts", async () => {
    const trigger = document.createElement("button");
    trigger.textContent = "打开确认弹窗";
    document.body.appendChild(trigger);
    trigger.focus();

    const result = renderDialog();
    await waitFor(() => {
      expect(document.activeElement).toBe(screen.getByRole("button", { name: "返回修改" }));
    });

    result.unmount();
    expect(document.activeElement).toBe(trigger);
    trigger.remove();
  });

  it("locks both actions and ignores Escape or backdrop dismissal while submitting", () => {
    const onCancel = vi.fn();
    const onConfirm = vi.fn();
    renderDialog({ submitting: true, onCancel, onConfirm });

    const cancelButton = screen.getByRole("button", { name: "返回修改" }) as HTMLButtonElement;
    const confirmButton = screen.getByRole("button", { name: "正在生成 Candidate" }) as HTMLButtonElement;
    const dialog = screen.getByRole("dialog");
    const overlay = document.querySelector(".confirm-dialog-overlay");

    expect(cancelButton.disabled).toBe(true);
    expect(confirmButton.disabled).toBe(true);
    expect(dialog.getAttribute("aria-busy")).toBe("true");

    fireEvent.click(cancelButton);
    fireEvent.click(confirmButton);
    fireEvent.keyDown(document, { key: "Escape" });
    fireEvent.click(overlay as HTMLElement);

    expect(onCancel).not.toHaveBeenCalled();
    expect(onConfirm).not.toHaveBeenCalled();
  });

  it("renders structured API failure details and source feedback as an alert", () => {
    renderDialog({
      error: {
        title: "Candidate 生成失败",
        message: "上传数据未通过历史分类校验。",
        details: [
          { label: "规则", value: "SC011" },
          { label: "国家", value: "奥地利" },
          { label: "建议处理", value: "核对逐月销量与车型配置唯一性后重新导出。" },
        ],
        sourceFeedback: "奥地利有 5,987 行配置重复，请恢复用于区分 T5 与 T5 EVO 的字段。",
      },
    });

    const alert = screen.getByRole("alert");
    expect(alert.textContent).toContain("Candidate 生成失败");
    expect(alert.textContent).toContain("上传数据未通过历史分类校验。");
    expect(alert.textContent).toContain("规则");
    expect(alert.textContent).toContain("SC011");
    expect(alert.textContent).toContain("国家");
    expect(alert.textContent).toContain("奥地利");
    expect(alert.textContent).toContain("建议处理");
    expect(alert.textContent).toContain("核对逐月销量与车型配置唯一性后重新导出。");
    expect(alert.textContent).toContain("给洗数人员");
    expect(alert.textContent).toContain(
      "奥地利有 5,987 行配置重复，请恢复用于区分 T5 与 T5 EVO 的字段。",
    );
  });

  it("blocks a repeated confirmation after an ambiguous write failure", () => {
    renderDialog({
      error: {
        title: "服务暂时未完成操作",
        message: "状态回读失败，请先刷新任务状态。",
        retryBlocked: true,
      },
    });

    const alert = screen.getByRole("alert");
    const confirmButton = screen.getByRole("button", {
      name: "确认并生成 Candidate",
    }) as HTMLButtonElement;
    const cancelButton = screen.getByRole("button", {
      name: "返回修改",
    }) as HTMLButtonElement;

    expect(alert.textContent).toContain("已锁定再次提交");
    expect(confirmButton.disabled).toBe(true);
    expect(cancelButton.disabled).toBe(false);
  });
});
