// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { PiInvoiceExportDialog } from "../../components/PiInvoiceExportDialog";
import type { PiInvoiceContext } from "../../types/orderGeniusVehicle";

vi.mock("../../api/client", () => ({ api: { getPiInvoiceContext: vi.fn(), getPiInvoiceTemplates: vi.fn(), exportPiInvoice: vi.fn(), getVehicleAllocationPis: vi.fn() } }));
const context: PiInvoiceContext = {
  piCode: "PI-SE-202609-001", templateKey: "SE_OJ", templateName: "SE OJ", buyerName: "Example Buyer",
  buyerAddress: "Norway", buyerEmail: "", sellerName: "Example Seller", sellerAddress: "Example address",
  bankDetails: "Example bank", priceTerm: "CFR", paymentTerm: "LC 90", unitCount: 120, lineCount: 11,
  missingFreightUnits: 120, missingInsuranceUnits: 120,
  defaults: { invoiceDate: "2026-09-01", referenceNo: "PI-SE-202609-001", portOfShipment: "China", portOfDischarge: "Check destination" },
};
beforeEach(() => {
  vi.mocked(api.getPiInvoiceTemplates).mockResolvedValue({ items: [{ id: "SE_OJ", owner: "System", name: "SE OJ", shared: true, system: true, country: "SE", brands: ["OMODA", "JAECOO"] }], owner: "alice", owners: ["alice"], actor: "alice", admin: false, country: "SE", brands: ["OMODA", "JAECOO"] });
  vi.mocked(api.getPiInvoiceContext).mockResolvedValue(context);
  vi.mocked(api.exportPiInvoice).mockResolvedValue(new Blob(["xlsx"]));
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} });
  vi.stubGlobal("URL", { createObjectURL: vi.fn(() => "blob:test"), revokeObjectURL: vi.fn() });
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
});
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it("blocks missing costs and accepts explicit zero without updating orders", async () => {
  const close = vi.fn();
  render(<PiInvoiceExportDialog piCode={context.piCode} onClose={close} />);
  await screen.findByText("Example Buyer");
  fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  expect(api.exportPiInvoice).not.toHaveBeenCalled();
  fireEvent.change(screen.getByLabelText("Freight / 运费 (EUR / unit)"), { target: { value: "0" } });
  fireEvent.change(screen.getByLabelText("Insurance / 保费 (EUR / unit)"), { target: { value: "0" } });
  fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  await waitFor(() => expect(api.exportPiInvoice).toHaveBeenCalledWith(context.piCode, { ...context.defaults, templateId: "SE_OJ", freightEur: 0, insuranceEur: 0, handlingEur: 0 }));
  await waitFor(() => expect(close).toHaveBeenCalled());
});

it("blank fee inputs preserve saved per-unit costs; a failed download retains the draft", async () => {
  vi.mocked(api.getPiInvoiceContext).mockResolvedValue({ ...context, missingFreightUnits: 0, missingInsuranceUnits: 0 });
  vi.mocked(api.exportPiInvoice).mockRejectedValue(new Error("Download failed"));
  render(<PiInvoiceExportDialog piCode={context.piCode} onClose={vi.fn()} />);
  await screen.findByText("Example Buyer");
  fireEvent.change(screen.getByLabelText("Ref. No / PI 编号"), { target: { value: "Changed reference" } });
  fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  await screen.findByText("Download failed");
  expect(api.exportPiInvoice).toHaveBeenCalledWith(context.piCode, { ...context.defaults, templateId: "SE_OJ", referenceNo: "Changed reference", handlingEur: 0 });
  expect((screen.getByLabelText("Ref. No / PI 编号") as HTMLInputElement).value).toBe("Changed reference");
});

it("closing a preview does not export", async () => {
  const close = vi.fn();
  render(<PiInvoiceExportDialog piCode={context.piCode} onClose={close} />);
  await screen.findByText("Example Buyer");
  fireEvent.click(screen.getByRole("button", { name: "Close" }));
  expect(close).toHaveBeenCalledOnce();
  expect(api.exportPiInvoice).not.toHaveBeenCalled();
});

it("selection chooses saved PIs by country and month, not current matrix rows", async () => {
  vi.mocked(api.getVehicleAllocationPis).mockResolvedValue({ items: [{ piCode: context.piCode } as import("../../types/orderGeniusVehicle").PiOrderHeader], total: 1 });
  render(<PiInvoiceExportDialog countries={["SE"]} month="2026-09" onClose={vi.fn()} />);
  await screen.findByText("Example Buyer");
  expect(api.getVehicleAllocationPis).toHaveBeenCalledWith({ country: "SE", month: "2026-09", pageSize: 200 });
  expect((screen.getByLabelText("Saved PI") as HTMLSelectElement).value).toBe(context.piCode);
});

it("uses the account's top compatible template and offers upload when none exists", async () => {
  vi.mocked(api.getPiInvoiceTemplates).mockResolvedValueOnce({ items: [], owner: "alice", owners: ["alice"], actor: "alice", admin: false, country: "CZ", brands: ["OMODA"] });
  render(<PiInvoiceExportDialog piCode="PI-CZ" onClose={vi.fn()} />);
  await screen.findByText(/No matching template. Upload/);
  expect(api.getPiInvoiceContext).not.toHaveBeenCalled();
  expect((screen.getByRole("button", { name: "Download XLSX" }) as HTMLButtonElement).disabled).toBe(true);
  expect(screen.getByRole("button", { name: "Manage templates / 管理模板" })).toBeTruthy();
});

it("a damaged top template stays selected and never silently falls back", async () => {
  vi.mocked(api.getPiInvoiceTemplates).mockResolvedValueOnce({ items: [
    { id: "first", name: "Broken", owner: "alice", country: "CZ", brands: ["OMODA"], shared: false },
    { id: "second", name: "Valid", owner: "alice", country: "CZ", brands: ["OMODA"], shared: false }],
    owner: "alice", owners: ["alice"], actor: "alice", admin: false, country: "CZ", brands: ["OMODA"] });
  vi.mocked(api.getPiInvoiceContext).mockRejectedValueOnce(new Error("Damaged template"));
  render(<PiInvoiceExportDialog piCode="PI-CZ" onClose={vi.fn()} />);
  await screen.findByText("Damaged template");
  expect(api.getPiInvoiceContext).toHaveBeenCalledTimes(1);
  expect(api.getPiInvoiceContext).toHaveBeenCalledWith("PI-CZ", "first");
  fireEvent.change(screen.getByLabelText("PI template"), { target: { value: "second" } });
  await screen.findByText("Example Buyer");
  expect(api.getPiInvoiceContext).toHaveBeenCalledWith("PI-CZ", "second");
});
