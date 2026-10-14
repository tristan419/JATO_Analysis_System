// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { PiInvoiceTemplateManager } from "../../components/PiInvoiceTemplateManager";
import type { PiTemplateCatalog, PiTemplateConfig } from "../../types/orderGeniusVehicle";

vi.mock("../../api/client", () => ({ api: { getInvoiceTemplates: vi.fn(), previewPiTemplate: vi.fn(), savePiTemplate: vi.fn(), orderPiTemplates: vi.fn(), copyPiTemplate: vi.fn(), deletePiTemplate: vi.fn() } }));
const config: PiTemplateConfig = { sheetName: "Sheet1", headerRow: 10, dataStart: 11, totalRow: 13,
  columns: { modelName: "B", quantity: "D", fobEur: "E", total: "I", freightEur: "F", insuranceEur: "G" }, fields: {
    sellerName: { cell: "A1", value: "Seller", prefix: "" }, buyerName: { cell: "A3", value: "Buyer", prefix: "" },
    buyerAddress: { cell: "A4", value: "Address", prefix: "" }, bankDetails: { cell: "A5", value: "Bank", prefix: "" },
    priceTerm: { cell: "A6", value: "CFR", prefix: "" }, paymentTerm: { cell: "A7", value: "LC", prefix: "" },
    invoiceDate: { cell: "F1", value: "", prefix: "" }, referenceNo: { cell: "F2", value: "", prefix: "" },
    portOfShipment: { cell: "A14", value: "Loading", prefix: "" }, portOfDischarge: { cell: "A15", value: "Discharge", prefix: "" },
  }, charges: [], currency: "EUR" };
const catalog: PiTemplateCatalog = { actor: "alice", owner: "alice", owners: ["alice", "bob"], admin: false, country: "CZ", brands: ["OMODA"],
  items: [{ id: "first", name: "Mine", owner: "alice", country: "CZ", brands: ["OMODA"], shared: false, sha256: "digest", config },
    { id: "second", name: "Shared", owner: "bob", country: "CZ", brands: ["OMODA"], shared: true, sha256: "digest", config }] };
const scope = { country: catalog.country, brands: catalog.brands };
beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn();
  vi.stubGlobal("ResizeObserver", class { observe() {} disconnect() {} });
  vi.mocked(api.getInvoiceTemplates).mockResolvedValue(catalog);
  vi.mocked(api.previewPiTemplate).mockResolvedValue({ ...config, sha256: "digest", sheets: ["Sheet1"], sourceCells: [{ cell: "A3", value: "Buyer" }] });
  vi.mocked(api.savePiTemplate).mockResolvedValue(catalog.items[0]);
  vi.mocked(api.orderPiTemplates).mockResolvedValue({ saved: true });
  vi.mocked(api.copyPiTemplate).mockResolvedValue(catalog.items[0]);
  vi.mocked(api.deletePiTemplate).mockResolvedValue({ deleted: true });
});
afterEach(() => { cleanup(); vi.resetAllMocks(); vi.unstubAllGlobals(); });
function review(): void {
  fireEvent.click(screen.getByLabelText(/I confirm the total calculation/));
  fireEvent.click(screen.getByRole("button", { name: "Review before saving / 保存前核对" }));
}

it("upload is personal until explicit share and cannot save before reviewing", async () => {
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  const file = new File(["xlsx"], "CZ.xlsx");
  fireEvent.change(screen.getByLabelText("Upload PI template"), { target: { files: [file] } });
  await screen.findByRole("button", { name: "Review before saving / 保存前核对" });
  expect((screen.getByLabelText(/Share to country/) as HTMLInputElement).checked).toBe(false);
  review();
  expect(api.savePiTemplate).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Save template / 保存模板" }));
  await waitFor(() => expect(api.savePiTemplate).toHaveBeenCalledWith({ name: "CZ", country: "CZ", brands: ["OMODA"], shared: false, sha256: "digest", config }, file));
});

it("retains a failed save draft and does not retry a saved write on refresh failure", async () => {
  vi.mocked(api.savePiTemplate).mockRejectedValueOnce(new Error("Save failed"));
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: "Edit / 编辑" }));
  fireEvent.change(screen.getByLabelText("Template name"), { target: { value: "Changed" } });
  review();
  fireEvent.click(screen.getByRole("button", { name: "Save template / 保存模板" }));
  await screen.findByText("Save failed");
  expect((screen.getByLabelText("Template name") as HTMLInputElement).value).toBe("Changed");
  vi.mocked(api.getInvoiceTemplates).mockRejectedValueOnce(new Error("Read failed"));
  fireEvent.click(screen.getByRole("button", { name: "Save template / 保存模板" }));
  await screen.findByText(/Saved, but refresh failed/);
  expect(api.savePiTemplate).toHaveBeenCalledTimes(2);
  fireEvent.click(screen.getByRole("button", { name: "Reload list / 重读列表" }));
  await waitFor(() => expect(api.getInvoiceTemplates).toHaveBeenCalledTimes(2));
  expect(api.savePiTemplate).toHaveBeenCalledTimes(2);
});

it("drag reorders locally then saves only the current account's complete list", async () => {
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  const shared = screen.getByText("Shared").closest("li")!;
  const mine = screen.getByText("Mine").closest("li")!;
  fireEvent.dragStart(shared); fireEvent.dragOver(mine); fireEvent.drop(mine);
  expect(api.orderPiTemplates).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Save order / 保存排序" }));
  await waitFor(() => expect(api.orderPiTemplates).toHaveBeenCalledWith(scope, ["second", "first"], "alice"));
  expect(screen.getAllByRole("button", { name: "Edit / 编辑" })).toHaveLength(1);
  expect(screen.queryByLabelText("Manage template owner")).toBeNull();
});

it("admin can explicitly switch to another owner's list", async () => {
  vi.mocked(api.getInvoiceTemplates).mockResolvedValue({ ...catalog, owner: "bob", admin: true });
  render(<PiInvoiceTemplateManager initial={{ ...catalog, admin: true }} onClose={vi.fn()} />);
  fireEvent.change(screen.getByLabelText("Manage template owner"), { target: { value: "bob" } });
  await waitFor(() => expect(api.getInvoiceTemplates).toHaveBeenCalledWith(scope, "bob"));
  await waitFor(() => expect((screen.getByLabelText("Manage template owner") as HTMLSelectElement).value).toBe("bob"));
});

it("requires explicit confirmation to delete only the owned source", async () => {
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  expect(screen.getAllByRole("button", { name: "Delete / 删除" })).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: "Delete / 删除" }));
  expect(api.deletePiTemplate).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  fireEvent.click(screen.getByRole("button", { name: "Delete / 删除" }));
  fireEvent.click(screen.getByRole("button", { name: "Delete template" }));
  await waitFor(() => expect(api.deletePiTemplate).toHaveBeenCalledWith(scope, "first"));
});

it("opens an empty country/brand library and uploads without any saved PI", async () => {
  vi.mocked(api.getInvoiceTemplates).mockResolvedValue({ ...catalog, country: "BG", brands: ["OMODA", "JAECOO"], items: [] });
  render(<PiInvoiceTemplateManager countries={["BG", "CH"]} brands={["OMODA", "JAECOO"]} defaultCountry="BG" onClose={vi.fn()} />);
  const open = screen.getByRole("button", { name: "Open templates / 打开模板" });
  expect((open as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(screen.getByLabelText("OMODA")); fireEvent.click(screen.getByLabelText("JAECOO"));
  fireEvent.click(open);
  await screen.findByLabelText("Upload PI template");
  expect(api.getInvoiceTemplates).toHaveBeenCalledWith({ country: "BG", brands: ["OMODA", "JAECOO"] });
  const file = new File(["xlsx"], "BG.xlsx");
  fireEvent.change(screen.getByLabelText("Upload PI template"), { target: { files: [file] } });
  await screen.findByLabelText("Template name");
  expect(api.previewPiTemplate).toHaveBeenCalledWith({ country: "BG", brands: ["OMODA", "JAECOO"] }, file);
});

it("retains scope after a permission error and does not load without a country", async () => {
  vi.mocked(api.getInvoiceTemplates).mockRejectedValueOnce(new Error("Permission revoked"));
  render(<PiInvoiceTemplateManager countries={["BG"]} brands={["OMODA"]} onClose={vi.fn()} />);
  fireEvent.click(screen.getByLabelText("OMODA"));
  fireEvent.click(screen.getByRole("button", { name: "Open templates / 打开模板" }));
  await screen.findByText("Permission revoked");
  expect((screen.getByLabelText("OMODA") as HTMLInputElement).checked).toBe(true);
  expect(screen.queryByLabelText("Upload PI template")).toBeNull();
});

it("cancelling before opening a scope performs no request or write", () => {
  const close = vi.fn();
  render(<PiInvoiceTemplateManager countries={[]} brands={["OMODA"]} onClose={close} />);
  fireEvent.click(screen.getByLabelText("OMODA"));
  expect((screen.getByRole("button", { name: "Open templates / 打开模板" }) as HTMLButtonElement).disabled).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Close" }));
  expect(close).toHaveBeenCalledOnce();
  expect(api.getInvoiceTemplates).not.toHaveBeenCalled(); expect(api.savePiTemplate).not.toHaveBeenCalled();
});

it("lists every missing field before confirmation and can locate the input", async () => {
  const incomplete = { ...config, fields: { ...config.fields, sellerName: { cell: "", value: "", prefix: "" }, bankDetails: { cell: "", value: "", prefix: "" } } };
  vi.mocked(api.previewPiTemplate).mockResolvedValue({ ...incomplete, sha256: "digest", sheets: ["Sheet1"], sourceCells: [{ cell: "A1", value: "Original seller" }] });
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  fireEvent.change(screen.getByLabelText("Upload PI template"), { target: { files: [new File(["xlsx"], "RO.xlsx")] } });
  await screen.findByLabelText("Template name");
  fireEvent.click(screen.getByRole("button", { name: "Review before saving / 保存前核对" }));
  expect(screen.getByText("Mapping incomplete / 映射未完成")).toBeTruthy();
  expect(screen.getByRole("button", { name: /Bank details.*选择有效/ })).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: /Seller.*填入完整原文/ }));
  expect(document.activeElement).toBe(screen.getByLabelText("sellerName value"));
  expect(api.savePiTemplate).not.toHaveBeenCalled();
  fireEvent.change(screen.getByLabelText("Source sellerName"), { target: { value: "A1" } });
  expect((screen.getByLabelText("sellerName cell") as HTMLTextAreaElement).value).toBe("A1");
  expect((screen.getByLabelText("sellerName value") as HTMLTextAreaElement).value).toBe("Original seller");
  expect(screen.queryByRole("button", { name: /Seller.*填入完整原文/ })).toBeNull();
});

it("requires deliberate FOB-only confirmation and changing charges invalidates it", () => {
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: "Edit / 编辑" }));
  fireEvent.click(screen.getByRole("button", { name: "Review before saving / 保存前核对" }));
  expect(screen.getByRole("button", { name: /明确确认合计费用/ })).toBeTruthy();
  review();
  expect(screen.getByRole("button", { name: "Save template / 保存模板" })).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Change mapping / 返回调整" }));
  expect((screen.getByLabelText("buyerName value") as HTMLTextAreaElement).value).toBe("Buyer");
  fireEvent.click(screen.getByLabelText("freightEur"));
  expect((screen.getByLabelText(/I confirm the total calculation/) as HTMLInputElement).checked).toBe(false);
  expect(screen.getByText(/本次公式.*运费/)).toBeTruthy();
});

it("shows the original formula, rejects overlapping columns and highlights saved template", async () => {
  vi.mocked(api.previewPiTemplate).mockResolvedValue({ ...config, sha256: "digest", sheets: ["Sheet1"], sourceCells: [{ cell: "I11", value: "=(E11+F11+G11)*D11" }] });
  render(<PiInvoiceTemplateManager initial={catalog} onClose={vi.fn()} />);
  fireEvent.change(screen.getByLabelText("Upload PI template"), { target: { files: [new File(["xlsx"], "RO.xlsx")] } });
  await screen.findByText("=(E11+F11+G11)*D11", { selector: "code" });
  fireEvent.change(screen.getByLabelText("Column total"), { target: { value: "E" } });
  review();
  expect(screen.getByRole("button", { name: /Total amount.*不重复/ })).toBeTruthy();
  fireEvent.change(screen.getByLabelText("Column total"), { target: { value: "I" } });
  fireEvent.click(screen.getByRole("button", { name: "Review before saving / 保存前核对" }));
  fireEvent.click(screen.getByRole("button", { name: "Save template / 保存模板" }));
  await screen.findByText(/Saved and refreshed/);
  expect(screen.getByText("Mine").closest("li")?.textContent).toContain("刚保存");
});
