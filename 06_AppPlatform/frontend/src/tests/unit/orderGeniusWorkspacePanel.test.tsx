// @vitest-environment jsdom
import { useState } from "react";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { OrderGeniusWorkspacePanel } from "../../components/orderGenius/OrderGeniusWorkspacePanel";
import { ConfirmDialog } from "../../components/ConfirmDialog";

afterEach(cleanup);
describe("Order Genius workspace panel", () => {
  it("closes on Escape/backdrop and restores focus and body scrolling", () => {
    const close = vi.fn(); const button = document.createElement("button"); document.body.append(button); button.focus();
    const view = render(<OrderGeniusWorkspacePanel label="BOM Admin" onClose={close}><button>Close</button></OrderGeniusWorkspacePanel>);
    const dialog = screen.getByRole("dialog"); expect(document.activeElement).toBe(dialog);
    fireEvent.keyDown(dialog, { key: "Escape" }); expect(close).toHaveBeenCalledOnce();
    fireEvent.mouseDown(dialog); expect(close).toHaveBeenCalledOnce();
    if (dialog.parentElement) fireEvent.mouseDown(dialog.parentElement); expect(close).toHaveBeenCalledTimes(2);
    view.unmount(); expect(document.activeElement).toBe(button); expect(document.body.style.overflow).toBe(""); button.remove();
  });
  it("Escape inside a nested confirmation closes only that confirmation", () => {
    const close = vi.fn();
    function Fixture() {
      const [confirming, setConfirming] = useState(true);
      return <OrderGeniusWorkspacePanel label="Material Assets" onClose={close}><button>Upload</button>{confirming ? <ConfirmDialog title="Delete configuration" description="Cannot be undone" cancelLabel="Cancel" confirmLabel="Delete" loadingLabel="Deleting" submitting={false} error={null} onCancel={() => setConfirming(false)} onConfirm={() => undefined} /> : null}</OrderGeniusWorkspacePanel>;
    }
    render(<Fixture />); fireEvent.keyDown(screen.getByRole("button", { name: "Cancel" }), { key: "Escape" });
    expect(screen.queryByRole("dialog", { name: "Delete configuration" })).toBeNull(); expect(close).not.toHaveBeenCalled();
    expect(screen.getByRole("dialog", { name: "Material Assets" })).toBeTruthy(); expect(document.body.style.overflow).toBe("");
  });
  it("wraps keyboard focus over visible controls, excluding collapsed historical content", () => {
    render(<OrderGeniusWorkspacePanel label="Material Assets" onClose={() => undefined}><button>Reload</button><details><summary>Historical</summary><button>Hidden material</button></details></OrderGeniusWorkspacePanel>);
    const first = screen.getByRole<HTMLButtonElement>("button", { name: "Reload" });
    const last = screen.getByText("Historical"), hidden = screen.getByText("Hidden material");
    first.checkVisibility = () => true; last.checkVisibility = () => true; hidden.checkVisibility = () => false;
    last.focus(); fireEvent.keyDown(last, { key: "Tab" }); expect(document.activeElement).toBe(first);
    fireEvent.keyDown(first, { key: "Tab", shiftKey: true }); expect(document.activeElement).toBe(last);
  });
});
