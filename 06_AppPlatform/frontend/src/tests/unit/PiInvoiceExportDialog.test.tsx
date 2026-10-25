// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { PiInvoiceExportDialog } from "../../components/PiInvoiceExportDialog";
import type { PiInvoiceContext, PiTemplateCatalog } from "../../types/orderGeniusVehicle";

vi.mock("../../api/client", () => ({ api: { getMonthlyInvoiceContext: vi.fn(), exportMonthlyInvoice: vi.fn(), getVehicleAllocationPis: vi.fn(), generateVehicleAllocationFromOrderMatrix: vi.fn() } }));
const context: PiInvoiceContext = {
  piCode: "PI-SE-202610", templateKey: "SE_OJ", templateName: "SE OJ September example", buyerName: "Example Buyer",
  buyerAddress: "Norway", buyerEmail: "", sellerName: "Example Seller", sellerAddress: "Example address",
  bankDetails: "Example bank", priceTerm: "CFR", paymentTerm: "LC 90", unitCount: 120, lineCount: 11,
  missingFreightUnits: 120, missingInsuranceUnits: 120,
  defaults: { invoiceDate: "2026-10-25", referenceNo: "PI-SE-202610", portOfShipment: "China", portOfDischarge: "Check destination" },
};
const catalog: PiTemplateCatalog = { items: [{ id: "SE_OJ", owner: "System", name: "SE OJ September example", shared: true, system: true, country: "SE", brands: ["OMODA", "JAECOO"] }],
  owner: "alice", owners: ["alice"], actor: "alice", admin: false, country: "SE", brands: ["OMODA", "JAECOO"] };
beforeEach(() => {
  vi.mocked(api.getMonthlyInvoiceContext).mockResolvedValue({ catalog, context });
  vi.mocked(api.exportMonthlyInvoice).mockResolvedValue(new Blob(["xlsx"]));
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} });
  vi.stubGlobal("URL", { createObjectURL: vi.fn(() => "blob:test"), revokeObjectURL: vi.fn() });
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
});
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

function fillFees(): void {
  fireEvent.change(screen.getByLabelText("Freight / 运费 (EUR / unit)"), { target: { value: "0" } });
  fireEvent.change(screen.getByLabelText("Insurance / 保费 (EUR / unit)"), { target: { value: "0" } });
}

it("exports positive monthly quantities without querying/creating a saved PI; zero costs are explicit", async () => {
  const close = vi.fn();
  render(<PiInvoiceExportDialog countries={["SE"]} month="2026-10" onClose={close} />);
  await screen.findByText("Example Buyer");
  expect(api.getMonthlyInvoiceContext).toHaveBeenCalledWith("SE", "2026-10", "");
  expect(screen.queryByLabelText("Saved PI")).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  expect(api.exportMonthlyInvoice).not.toHaveBeenCalled();
  fillFees(); fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  await waitFor(() => expect(api.exportMonthlyInvoice).toHaveBeenCalledWith("SE", "2026-10", { ...context.defaults, templateId: "SE_OJ", freightEur: 0, insuranceEur: 0, handlingEur: 0 }));
  expect(api.getVehicleAllocationPis).not.toHaveBeenCalled();
  expect(api.generateVehicleAllocationFromOrderMatrix).not.toHaveBeenCalled();
  await waitFor(() => expect(close).toHaveBeenCalledOnce());
});

it("the same September-named template can be reused for October and November", async () => {
  render(<PiInvoiceExportDialog countries={["SE"]} month="2026-10" onClose={vi.fn()} />);
  await screen.findByText("Example Buyer");
  fillFees();
  vi.mocked(api.getMonthlyInvoiceContext).mockResolvedValueOnce({ catalog, context: { ...context, unitCount: 20,
    defaults: { ...context.defaults, referenceNo: "PI-SE-202611" } } });
  fireEvent.change(screen.getByLabelText("Plan month"), { target: { value: "2026-11" } });
  await screen.findByDisplayValue("PI-SE-202611");
  expect(api.getMonthlyInvoiceContext).toHaveBeenLastCalledWith("SE", "2026-11", "");
  expect(screen.getByText(/2026-11.*20 units/)).toBeTruthy();
  expect((screen.getByLabelText("Freight / 运费 (EUR / unit)") as HTMLInputElement).value).toBe("");
  fillFees(); fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  await waitFor(() => expect(api.exportMonthlyInvoice).toHaveBeenCalledWith("SE", "2026-11", expect.objectContaining({ templateId: "SE_OJ", referenceNo: "PI-SE-202611" })));
});

it("failed downloads retain modified fields and closing never writes", async () => {
  vi.mocked(api.exportMonthlyInvoice).mockRejectedValue(new Error("Download failed"));
  const close = vi.fn();
  render(<PiInvoiceExportDialog countries={["SE"]} month="2026-10" onClose={close} />);
  await screen.findByText("Example Buyer"); fillFees();
  fireEvent.change(screen.getByLabelText("Ref. No / PI 编号"), { target: { value: "Changed reference" } });
  fireEvent.click(screen.getByRole("button", { name: "Download XLSX" }));
  await screen.findByText("Download failed");
  expect(screen.getByDisplayValue("Changed reference")).toBeTruthy();
  expect(close).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Close" }));
  expect(close).toHaveBeenCalledOnce();
  expect(api.generateVehicleAllocationFromOrderMatrix).not.toHaveBeenCalled();
});

it("no template offers the existing upload manager without creating an order", async () => {
  vi.mocked(api.getMonthlyInvoiceContext).mockResolvedValueOnce({ catalog: { ...catalog, items: [] }, context: null });
  render(<PiInvoiceExportDialog countries={["SE"]} month="2026-10" onClose={vi.fn()} />);
  await screen.findByText(/No matching template. Upload/);
  expect((screen.getByRole("button", { name: "Download XLSX" }) as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Manage templates / 管理模板" }));
  expect(screen.getByLabelText("Upload PI template")).toBeTruthy();
});

it("requires a month, and a failed/empty plan is not misreported as missing templates", async () => {
  vi.mocked(api.getMonthlyInvoiceContext).mockRejectedValue(new Error("No positive monthly quantities"));
  render(<PiInvoiceExportDialog countries={["SE"]} onClose={vi.fn()} />);
  expect(api.getMonthlyInvoiceContext).not.toHaveBeenCalled();
  fireEvent.change(screen.getByLabelText("Plan month"), { target: { value: "2026-10" } });
  await screen.findByText("No positive monthly quantities");
  expect(screen.queryByText(/No matching template. Upload/)).toBeNull();
  expect((screen.getByRole("button", { name: "Download XLSX" }) as HTMLButtonElement).disabled).toBe(true);
});

it("a late result from the previous country/month cannot replace current scope", async () => {
  let resolveOld!: (value: { catalog: PiTemplateCatalog; context: PiInvoiceContext }) => void;
  vi.mocked(api.getMonthlyInvoiceContext).mockReturnValueOnce(new Promise((resolve) => { resolveOld = resolve; }));
  render(<PiInvoiceExportDialog countries={["SE", "CH"]} month="2026-10" onClose={vi.fn()} />);
  fireEvent.change(screen.getByLabelText("Invoice country"), { target: { value: "CH" } });
  await screen.findByText("Example Buyer");
  resolveOld({ catalog, context: { ...context, buyerName: "Old buyer" } });
  await waitFor(() => expect(screen.queryByText("Old buyer")).toBeNull());
  expect(api.getMonthlyInvoiceContext).toHaveBeenLastCalledWith("CH", "2026-10", "");
});

it("a broken top template reports failure and never silently chooses a fallback", async () => {
  vi.mocked(api.getMonthlyInvoiceContext).mockResolvedValueOnce({ catalog, context: null, error: "Damaged template" });
  render(<PiInvoiceExportDialog countries={["SE"]} month="2026-10" onClose={vi.fn()} />);
  await screen.findByText("Damaged template");
  expect(api.getMonthlyInvoiceContext).toHaveBeenCalledTimes(1);
  expect(api.exportMonthlyInvoice).not.toHaveBeenCalled();
  expect(screen.getByLabelText("PI template")).toBeTruthy();
  expect(screen.getByRole("button", { name: "Manage templates / 管理模板" })).toBeTruthy();
});
