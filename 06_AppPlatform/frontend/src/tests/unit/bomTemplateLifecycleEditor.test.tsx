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
const props = { materialCode: "S", status: "active" as const, effectiveFrom: null, effectiveTo: null, rowVersion: 4 };
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

describe("Template lifecycle date confirmation", () => {
  it("previews without saving, confirms exact days, and invalidates preview after an edit", async () => {
    const update = vi.spyOn(api, "updateSkuLifecycle").mockResolvedValue(preview);
    const saved = vi.fn();
    render(<BomTemplateLifecycleEditor {...props} onSaved={saved} />);
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
    await act(async () => { fireEvent.click(screen.getByText("Preview template dates")); });
    expect((screen.getByText("Confirm template dates") as HTMLButtonElement).disabled).toBe(true);
    expect(screen.getByText(/Edit this country's price period/)).toBeTruthy();
  });

  it("locks inputs during a preview and preserves dates after a failed request", async () => {
    let reject!: (failure: Error) => void;
    vi.spyOn(api, "updateSkuLifecycle").mockImplementation(() => new Promise((_, fail) => { reject = fail; }));
    render(<BomTemplateLifecycleEditor {...props} onSaved={vi.fn()} />);
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
