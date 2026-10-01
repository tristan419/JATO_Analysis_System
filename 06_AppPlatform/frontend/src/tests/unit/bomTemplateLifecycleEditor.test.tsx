// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { BomTemplateLifecycleEditor } from "../../components/orderGenius/BomTemplateLifecycleEditor";
import type { BomTemplateLifecycleUpdateResponse } from "../../types/orderGenius";

const preview: BomTemplateLifecycleUpdateResponse = {
  bomTemplate: "T**001", materialCodes: ["S", "D"], effectiveFrom: "2026-03-14",
  effectiveTo: "2026-09-30", affectedPeriods: [], canApply: true,
};
const props = { materialCode: "S", bomTemplate: "T**001", status: "active" as const, effectiveFrom: null, effectiveTo: null, rowVersion: 4 };
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("Template lifecycle date confirmation", () => {
  it("keeps the original status buttons compact and opens optional dates without writing", () => {
    const update = vi.spyOn(api, "updateSkuLifecycle");
    render(<BomTemplateLifecycleEditor {...props} onSaved={vi.fn()} />);
    expect(screen.getByRole("button", { name: "Active" }).getAttribute("aria-pressed")).toBe("true");
    expect(screen.queryByLabelText("First order date")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Dates" }));
    const dialog = screen.getByRole("dialog", { name: "Template lifecycle dates" });
    expect(dialog.parentElement?.parentElement).toBe(document.body);
    expect(screen.getByText("T**001")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Final order date"), { target: { value: "2026-09-30" } });
    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    fireEvent.click(screen.getByRole("button", { name: "Dates" }));
    expect((screen.getByLabelText("Final order date") as HTMLInputElement).value).toBe("");
    expect(update).not.toHaveBeenCalled();
  });

  it("allows status-only confirmation without inventing dates", async () => {
    const update = vi.spyOn(api, "updateSkuLifecycle").mockResolvedValue({ ...preview, effectiveFrom: null, effectiveTo: null });
    render(<BomTemplateLifecycleEditor {...props} onSaved={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Historical" }));
    await act(async () => { fireEvent.click(screen.getByText("Preview template dates")); });
    await act(async () => { fireEvent.click(screen.getByText("Confirm template dates")); });
    expect(update).toHaveBeenLastCalledWith("S", expect.objectContaining({ lifecycleStatus: "historical", effectiveFrom: null, effectiveTo: null, previewOnly: false }));
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("makes dated status read-only and keeps the saved range in calendar hover", () => {
    render(<BomTemplateLifecycleEditor {...props} effectiveFrom="2026-03-14" effectiveTo="2026-09-30" onSaved={vi.fn()} />);
    expect((screen.getByRole("button", { name: "Active" }) as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByRole("button", { name: "Dates · Set" }).title).toContain("2026-03-14 → 2026-09-30");
    fireEvent.click(screen.getByRole("button", { name: "Dates · Set" }));
    expect((screen.getByLabelText("Final order date") as HTMLInputElement).value).toBe("2026-09-30");
  });

  it("clears optional dates and chooses an undated status in one preview/confirmation", async () => {
    const update = vi.spyOn(api, "updateSkuLifecycle").mockResolvedValue({ ...preview, effectiveFrom: null, effectiveTo: null });
    render(<BomTemplateLifecycleEditor {...props} status="phase_out" effectiveFrom="2026-03-14" effectiveTo="2026-09-30" onSaved={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Dates · Set" }));
    expect((screen.getByLabelText("Undated lifecycle status") as HTMLSelectElement).disabled).toBe(true);
    fireEvent.change(screen.getByLabelText("First order date"), { target: { value: "" } });
    fireEvent.change(screen.getByLabelText("Final order date"), { target: { value: "" } });
    fireEvent.change(screen.getByLabelText("Undated lifecycle status"), { target: { value: "active" } });
    await act(async () => { fireEvent.click(screen.getByText("Preview template dates")); });
    await act(async () => { fireEvent.click(screen.getByText("Confirm template dates")); });
    expect(update).toHaveBeenLastCalledWith("S", expect.objectContaining({ lifecycleStatus: "active", effectiveFrom: null, effectiveTo: null, previewOnly: false }));
  });

  it("previews without saving, confirms exact days, and invalidates preview after an edit", async () => {
    const update = vi.spyOn(api, "updateSkuLifecycle").mockResolvedValue(preview);
    const saved = vi.fn();
    render(<BomTemplateLifecycleEditor {...props} onSaved={saved} />);
    fireEvent.click(screen.getByRole("button", { name: "Dates" }));
    fireEvent.change(screen.getByLabelText("First order date"), { target: { value: "2026-03-14" } });
    fireEvent.change(screen.getByLabelText("Final order date"), { target: { value: "2026-09-30" } });
    await act(async () => { fireEvent.click(screen.getByText("Preview template dates")); });
    expect(update).toHaveBeenLastCalledWith("S", { lifecycleStatus: "active", effectiveFrom: "2026-03-14", effectiveTo: "2026-09-30", rowVersion: 4, previewOnly: true });
    expect(saved).not.toHaveBeenCalled();
    fireEvent.change(screen.getByLabelText("Final order date"), { target: { value: "2026-10-31" } });
    expect(screen.queryByText("Confirm template dates")).toBeNull();
    await act(async () => { fireEvent.click(screen.getByText("Preview template dates")); });
    await act(async () => { fireEvent.click(screen.getByText("Confirm template dates")); });
    expect(update).toHaveBeenLastCalledWith("S", expect.objectContaining({ effectiveTo: "2026-10-31", previewOnly: false }));
    expect(saved).toHaveBeenCalledOnce();
  });

  it("shows country suggestions and prevents saving an out-of-bounds period", async () => {
    vi.spyOn(api, "updateSkuLifecycle").mockResolvedValue({ ...preview, canApply: false, affectedPeriods: [{ periodId: "p", countryCode: "CH", validFrom: "2026-08-01", validTo: "2026-12-31", beforeTemplateStart: false, afterTemplateEnd: true, suggestedActions: ["Edit price period", "Extend template lifecycle", "Cancel"] }] });
    render(<BomTemplateLifecycleEditor {...props} onSaved={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Dates" }));
    await act(async () => { fireEvent.click(screen.getByText("Preview template dates")); });
    expect((screen.getByText("Confirm template dates") as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText(/Edit this country's price period/)).toBeTruthy();
  });

  it("locks inputs during a preview and preserves dates after a failed request", async () => {
    let reject!: (failure: Error) => void;
    vi.spyOn(api, "updateSkuLifecycle").mockImplementation(() => new Promise((_, fail) => { reject = fail; }));
    render(<BomTemplateLifecycleEditor {...props} onSaved={vi.fn()} />);
    fireEvent.click(screen.getByRole("button", { name: "Dates" }));
    const input = screen.getByLabelText("Final order date") as HTMLInputElement;
    fireEvent.change(input, { target: { value: "2026-09-30" } });
    fireEvent.click(screen.getByText("Preview template dates"));
    expect(input.disabled).toBe(true);
    await act(async () => { reject(new Error("Changed; refresh and retry")); });
    expect(screen.getByRole("alert").textContent).toContain("Changed");
    expect(input.value).toBe("2026-09-30");
    expect(input.disabled).toBe(false);
  });
});
