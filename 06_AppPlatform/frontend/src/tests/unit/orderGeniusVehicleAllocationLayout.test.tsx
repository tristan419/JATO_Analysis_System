// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { OrderGeniusVehicleAllocationPage } from "../../pages/OrderGeniusVehicleAllocationPage";
import type { PiOrderDetail, PiOrderLine, PiVehicleUnit, VehicleImportPreview } from "../../types/orderGeniusVehicle";

vi.mock("../../contexts/AuthContext", () => ({ useAuth: () => ({ user: { role: "admin", primaryCountry: "CH" } }) }));
vi.mock("../../hooks/useAccountCountryOptions", () => ({ useAccountCountryOptions: () => ({ countryOptions: [] }) }));
vi.mock("../../components/CommandSelect", () => ({
  CommandSelect: (props: { value: string; placeholder: string; options: Array<{ value: string; label: string }>; onChange: (value: string) => void }) => (
    <select aria-label={props.placeholder} value={props.value} onChange={(event) => props.onChange(event.target.value)}>
      <option value="">{props.placeholder}</option>
      {props.options.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
    </select>
  ),
}));

const PI = "PI-CH-202609-001";
function vehicle(index: number): PiVehicleUnit {
  return {
    fobEur: 15000, vehicleUnitId: `unit-${index}`, piCode: PI, officialPiNo: null, orderingAccountCode: "CH",
    orderingAccountName: null, shipmentBatchCode: null, portOfDischarge: null,
    piLineCode: `${PI}-L${index < 75 ? "01" : "02"}`, carCode: `CAR-${index}`,
    vin: index < 20 ? `LVTDB21B9RD${String(index).padStart(6, "0")}` : null,
    materialCode: index < 75 ? "BOM-ONE" : "BOM-TWO", bom: "BASE", brand: "JAECOO",
    modelName: "JAECOO5 HEV", version: "Select", powertrain: "HEV", exteriorColorName: "Black",
    exteriorColorCode: "CL", interiorColorName: "Black-Black", interiorColourCode: null,
    orderDate: null, orderMonth: "2026-09", productionDate: null, etd: null, eta: null,
    actualDepartureDate: null, actualArrivalDate: null, readyForPickupDate: null, shipName: null,
    countryCode: "CH", dealerCode: null, dealerName: null, customerRef: null,
    allocationStatus: index >= 100 ? "allocated" : "unallocated",
    logisticsStatus: index >= 100 ? "ready_for_pickup" : "pending",
    shippingScheduleUrl: null, feishuTrackingUrl: null, remark: null, rowVersion: 1,
  };
}
function line(number: number): PiOrderLine {
  return {
    piLineId: `line-${number}`, piCode: PI, piLineCode: `${PI}-L0${number}`, lineSequenceNo: number,
    materialCode: number === 1 ? "BOM-ONE" : "BOM-TWO", bom: "BASE", brand: "JAECOO",
    modelName: "JAECOO5 HEV", version: "Select", powertrain: "HEV", exteriorColorName: "Black",
    exteriorColorCode: "CL", interiorColorName: "Black-Black", interiorColourCode: null,
    quantity: 75, fobEur: 15000, amountEur: 1125000, remark: null, rowVersion: 1, allocations: [],
  };
}
function detail(): PiOrderDetail {
  return {
    header: {
      piId: "pi-1", piCode: PI, officialPiNo: null, countryCode: "CH", countryName: "Switzerland",
      orderingAccountCode: "CH", orderingAccountName: null, marketCountryCodes: ["CH"],
      shipmentBatchCode: null, portOfDischarge: null, orderDate: null, orderMonth: "2026-09",
      piSequenceNo: 1, shippingScheduleUrl: null, feishuTrackingUrl: null, shipName: null, etd: null,
      eta: null, actualDepartureDate: null, actualArrivalDate: null, readyForPickupDate: null,
      status: "draft", remark: null, rowVersion: 1, createdAtUtc: null, updatedAtUtc: null,
    },
    lines: [line(1), line(2)], vehicles: Array.from({ length: 150 }, (_, index) => vehicle(index)),
    vehicleTotal: 150,
    summary: { totalUnits: 150, allocated: 50, reserved: 0, unallocated: 100, vinAssigned: 20,
      vinMissing: 130, onVessel: 0, arrived: 0, readyForPickup: 50 },
  };
}

beforeEach(() => {
  window.history.replaceState({}, "", "/product/order-genius/vehicle-allocation");
  vi.spyOn(api, "getVehicleAllocationPis").mockResolvedValue({ items: [detail().header], total: 1 });
  vi.spyOn(api, "getVehicleAllocationPi").mockImplementation(async () => detail());
  vi.spyOn(api, "getVehicleAllocationStatusFlow").mockResolvedValue({ countryCode: "CH", orderingAccountCode: "CH", source: "default", allocation: [], logistics: [] });
  vi.spyOn(api, "listVehicleAllocationVehicles").mockImplementation(async (filters) => ({
    items: detail().vehicles.filter((item) => !filters?.piLineCode || item.piLineCode === filters.piLineCode).slice(0, 5), total: 150,
  }));
  vi.spyOn(api, "bulkUpdateVehicleAllocationVehicles").mockResolvedValue({ piCode: PI, piLineCode: null, matchedUnits: 150, updatedUnits: 150, vinAssigned: 0, fieldsUpdated: ["etd"] });
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

async function selectPi() {
  render(<OrderGeniusVehicleAllocationPage />);
  fireEvent.click(await screen.findByRole("button", { name: new RegExp(`^${PI}`) }));
  await screen.findByText("150 units / 台");
  await waitFor(() => expect(screen.getAllByRole("row").length).toBe(6));
}
function openView() {
  fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
  fireEvent.click(screen.getByRole("tab", { name: /View/ }));
}

function vinPreview(replacing = false): VehicleImportPreview {
  return {
    importId: "vin-preview", mode: "pi_vin_fill", piCode: PI, allowReplacing: replacing,
    totalRows: 2, newHeaders: 0, newLines: 0, newUnits: 0, updatedUnits: 1,
    filledUnits: replacing ? 0 : 1, replacedUnits: replacing ? 1 : 0, skippedUnits: 1, conflictUnits: 0,
    remainingUnits: 129, warnings: [], errors: [], status: "ok",
    previewRows: [
      { sourceRow: 2, action: replacing ? "replace" : "fill", piCode: PI, carCode: "CAR-20", vin: "LVTDB21B9RD123456", materialCode: "BOM-ONE", oldVin: replacing ? "LVTDB21B9RD000001" : null, rowVersion: 1, warnings: [], errors: [] },
      { sourceRow: 3, action: "skip", piCode: PI, carCode: "CAR-0", vin: "LVTDB21B9RD000000", materialCode: "BOM-ONE", oldVin: "LVTDB21B9RD000000", rowVersion: 1, warnings: [], errors: [] },
    ],
    targetVehicles: [
      { carCode: "CAR-20", materialCode: "BOM-ONE", vin: null, countryCode: "CH", piLineCode: `${PI}-L01` },
      { carCode: "CAR-21", materialCode: "BOM-ONE", vin: null, countryCode: "CH", piLineCode: `${PI}-L01` },
    ],
  };
}

async function uploadVinFile() {
  fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
  const file = new File(["mock workbook"], "production.xlsx", { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
  fireEvent.change(screen.getByLabelText("BOM and VIN XLSX"), { target: { files: [file] } });
  return file;
}

describe("PI-scoped BOM + VIN import", () => {
  it("previews selected VIN removal and does not apply a cancelled clear", async () => {
    const removal = { ...vinPreview(), removeVins: true, removedUnits: 1, filledUnits: 0, skippedUnits: 0,
      previewRows: [{ ...vinPreview().previewRows[0], action: "remove" as const }] };
    const preview = vi.spyOn(api, "previewVehicleAllocationParsedRows").mockResolvedValue(removal);
    const apply = vi.spyOn(api, "applyVehicleAllocationImport").mockResolvedValue({ createdUnits: 0, updatedUnits: 1, removedUnits: 1, warnings: [] });
    vi.spyOn(window, "confirm").mockReturnValue(false);
    await selectPi();
    fireEvent.click(screen.getByRole("checkbox", { name: "Select CAR-1" }));
    fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
    fireEvent.click(screen.getByRole("button", { name: /Clear selected VINs/ }));
    await screen.findByText("Clear VIN preview / 清 VIN 预览");
    expect(preview).toHaveBeenCalledWith({ piCode: PI, removeVins: true,
      rows: [{ sourceRow: 1, material_code: "BOM-ONE", car_code: "CAR-1", vin: vehicle(1).vin }] });
    fireEvent.click(screen.getByRole("button", { name: "Apply 1" }));
    expect(apply).not.toHaveBeenCalled();
  });

  it("uploads an original file for removal and confirms the preview before clearing", async () => {
    const preview = vi.spyOn(api, "previewVehicleAllocationImport").mockResolvedValue({ ...vinPreview(), removeVins: true, removedUnits: 1 });
    const apply = vi.spyOn(api, "applyVehicleAllocationImport").mockResolvedValue({ createdUnits: 0, updatedUnits: 1, removedUnits: 1, warnings: [] });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
    fireEvent.click(screen.getByLabelText(/Remove these VINs from file/));
    const file = new File(["mock workbook"], "original.xlsx");
    fireEvent.change(screen.getByLabelText("BOM and VIN XLSX"), { target: { files: [file] } });
    fireEvent.click(await screen.findByRole("button", { name: "Apply 1" }));
    await screen.findByText(/VIN cleared: 1/);
    expect(preview).toHaveBeenCalledWith(file, { piCode: PI, allowReplacing: false, removeVins: true });
    expect(apply).toHaveBeenCalledTimes(1);
  });

  it("uploads into the selected whole PI and applies only the confirmed ready count", async () => {
    const preview = vi.spyOn(api, "previewVehicleAllocationImport").mockResolvedValue(vinPreview());
    const apply = vi.spyOn(api, "applyVehicleAllocationImport").mockResolvedValue({ createdUnits: 0, updatedUnits: 1, skippedUnits: 1, warnings: [] });
    await selectPi();
    const file = await uploadVinFile();
    await screen.findByText("BOM + VIN preview / 匹配预览");
    expect(preview).toHaveBeenCalledWith(file, { piCode: PI, allowReplacing: false });
    expect(screen.getByText("Already imported / 已录跳过")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Apply 1" }));
    await waitFor(() => expect(apply).toHaveBeenCalledWith("vin-preview"));
    await screen.findByText(/VIN saved: 1; already imported: 1/);
  });

  it("repreviews explicit target changes and opt-in replacement before apply", async () => {
    vi.spyOn(api, "previewVehicleAllocationImport").mockResolvedValue(vinPreview());
    const repreview = vi.spyOn(api, "previewVehicleAllocationParsedRows").mockResolvedValue(vinPreview());
    await selectPi(); await uploadVinFile();
    fireEvent.click(await screen.findByRole("button", { name: "Choose target row 2" }));
    fireEvent.change(screen.getByLabelText("Target CarCode row 2"), { target: { value: "CAR-21" } });
    await waitFor(() => expect(repreview).toHaveBeenCalledWith(expect.objectContaining({ piCode: PI, allowReplacing: false,
      rows: expect.arrayContaining([expect.objectContaining({ sourceRow: 2, car_code: "CAR-21", material_code: "BOM-ONE" })]),
    })));
    await screen.findByRole("button", { name: "Apply 1" });
    fireEvent.click(screen.getByLabelText(/Allow replacing existing VINs/));
    await waitFor(() => expect(repreview).toHaveBeenLastCalledWith(expect.objectContaining({ piCode: PI, allowReplacing: true })));
  });

  it("does not call a saved import failed when the subsequent PI refresh fails", async () => {
    vi.spyOn(api, "previewVehicleAllocationImport").mockResolvedValue(vinPreview());
    const apply = vi.spyOn(api, "applyVehicleAllocationImport").mockResolvedValue({ createdUnits: 0, updatedUnits: 1, warnings: [] });
    await selectPi(); await uploadVinFile();
    vi.mocked(api.getVehicleAllocationPi).mockRejectedValueOnce(new Error("refresh unavailable"));
    fireEvent.click(await screen.findByRole("button", { name: "Apply 1" }));
    await screen.findByText(/VIN 已保存，但 PI 页面刷新失败/);
    expect(screen.queryByText(/VIN 导入未完成/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Apply 1" })).toBeNull();
    expect(apply).toHaveBeenCalledTimes(1);
  });

  it("requires replacement confirmation and never applies a cancelled correction", async () => {
    vi.spyOn(api, "previewVehicleAllocationImport").mockResolvedValue(vinPreview(true));
    const apply = vi.spyOn(api, "applyVehicleAllocationImport").mockResolvedValue({ createdUnits: 0, updatedUnits: 1, warnings: [] });
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
    await selectPi(); await uploadVinFile();
    fireEvent.click(await screen.findByRole("button", { name: "Apply 1" }));
    expect(confirm).toHaveBeenCalled(); expect(apply).not.toHaveBeenCalled();
  });

  it("shows bilingual actionable guidance instead of a raw server code", async () => {
    vi.spyOn(api, "previewVehicleAllocationImport").mockRejectedValue(new Error("500 Internal Server Error"));
    await selectPi(); await uploadVinFile();
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("VIN 导入未完成");
    expect(alert.textContent).not.toContain("500");
    expect(within(alert).getByRole("button", { name: /Re-upload/ })).toBeTruthy();
    expect(within(alert).getByRole("link").getAttribute("href")).toBe("/product/order-genius");
  });
});

describe("PI allocation layout and scope", () => {
  it("keeps failed status inputs and offers a read-only retry without exposing server codes", async () => {
    vi.mocked(api.bulkUpdateVehicleAllocationVehicles).mockRejectedValueOnce(new Error("500 Internal Server Error"));
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: /Status \/ 状态/ }));
    fireEvent.change(screen.getByLabelText("ETD"), { target: { value: "2026-10-12" } });
    fireEvent.click(screen.getByRole("button", { name: /^Update status$/ }));
    await screen.findByText(/状态更新未完成，输入已保留/);
    expect(screen.queryByText(/500/)).toBeNull();
    expect((screen.getByLabelText("ETD") as HTMLInputElement).value).toBe("2026-10-12");
    expect(screen.getByRole("link", { name: /Check selection/ }).getAttribute("href")).toBe("/product/order-genius");
    fireEvent.click(screen.getByRole("button", { name: /Re-read PI/ }));
    await waitFor(() => expect(api.getVehicleAllocationPi).toHaveBeenCalledTimes(2));
    expect(api.bulkUpdateVehicleAllocationVehicles).toHaveBeenCalledTimes(1);
  });

  it("blocks individual saves while a VIN preview is running", async () => {
    let finish: (preview: VehicleImportPreview) => void = () => undefined;
    vi.spyOn(api, "previewVehicleAllocationImport").mockImplementation(() => new Promise((resolve) => { finish = resolve; }));
    const save = vi.spyOn(api, "updateVehicleAllocationVehicle").mockResolvedValue(vehicle(1));
    await selectPi();
    fireEvent.click(screen.getByRole("cell", { name: "CAR-1" }));
    await uploadVinFile();
    expect(screen.getByRole("button", { name: "Save Vehicle" }).hasAttribute("disabled")).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Save Vehicle" }));
    expect(save).not.toHaveBeenCalled();
    finish(vinPreview());
    await waitFor(() => expect(screen.getByRole("button", { name: "Save Vehicle" }).hasAttribute("disabled")).toBe(false));
  });

  it("shows confirmed FOB by default and does not infer a missing snapshot", async () => {
    vi.mocked(api.listVehicleAllocationVehicles).mockResolvedValue({ items: [{ ...vehicle(1), fobEur: null }, vehicle(2)], total: 2 });
    render(<OrderGeniusVehicleAllocationPage />);
    fireEvent.click(await screen.findByRole("button", { name: new RegExp(`^${PI}`) }));
    await screen.findByRole("columnheader", { name: "FOB (EUR)" });
    expect(await screen.findByRole("cell", { name: "15,000" })).toBeTruthy();
    expect(screen.getByRole("cell", { name: "—" }).title || screen.getByRole("cell", { name: "—" }).textContent).toBeTruthy();
  });

  it("confirms the whole PI deletion and removes stale scope without touching browse month", async () => {
    const remove = vi.spyOn(api, "deleteVehicleAllocationPi").mockResolvedValue({ pi_code: PI, deleted: true });
    await selectPi();
    fireEvent.change(screen.getByLabelText("Browse Month"), { target: { value: "2026-10" } });
    fireEvent.click(screen.getByRole("button", { name: "Delete PI" }));
    expect(screen.getByText(/Delete 150 units \+ VINs/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Yes" }));
    await screen.findByText(/月需求保留，占用已释放/);
    expect(remove).toHaveBeenCalledWith(PI);
    expect(screen.queryByRole("button", { name: /PI lines/ })).toBeNull();
    expect((screen.getByLabelText("Browse Month") as HTMLInputElement).value).toBe("2026-10");
  });
  it("has no duplicate creation or generic creation-import path", async () => {
    await selectPi();
    expect(screen.queryByRole("button", { name: /^Create PI$/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /^Generate$/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /Add Line/i })).toBeNull();
    expect(screen.getByRole("link", { name: /前往选品/ }).getAttribute("href")).toBe("/product/order-genius");
    fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
    expect(screen.getByRole("button", { name: "Import File" }).hasAttribute("disabled")).toBe(false);
    expect(screen.getByLabelText(/Allow replacing existing VINs/).hasAttribute("checked")).toBe(false);
    expect(screen.getAllByRole("tab").length).toBe(3);
  });

  it("keeps browsing independent and loads the whole PI instead of its first line", async () => {
    render(<OrderGeniusVehicleAllocationPage />);
    fireEvent.change(screen.getByLabelText("Browse Month"), { target: { value: "2026-10" } });
    fireEvent.click(await screen.findByRole("button", { name: new RegExp(`^${PI}`) }));
    await screen.findByText("150 units / 台");
    expect((screen.getByLabelText("Browse Month") as HTMLInputElement).value).toBe("2026-10");
    expect(vi.mocked(api.listVehicleAllocationVehicles).mock.calls.at(-1)?.[0]?.piLineCode).toBeUndefined();
    expect(screen.getByText("130 no VIN / 待录")).toBeTruthy();
    expect(screen.getByRole("button", { name: /50 ready · 50 allocated/ })).toBeTruthy();
    expect(screen.queryByRole("region", { name: "PI lines" })).toBeNull();
  });

  it("closes the line panel on outside click or Escape without clearing its scope", async () => {
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: /PI lines/ }));
    const panel = screen.getByRole("region", { name: "PI lines" });
    fireEvent.click(within(panel).getByRole("button", { name: /L02/ }));
    await screen.findByText("75 units / 台");
    fireEvent.pointerDown(screen.getByRole("heading", { name: "Vehicle Allocation" }));
    expect(screen.queryByRole("region", { name: "PI lines" })).toBeNull();
    expect(vi.mocked(api.listVehicleAllocationVehicles).mock.calls.at(-1)?.[0]?.piLineCode).toBe(`${PI}-L02`);
    fireEvent.click(screen.getByRole("button", { name: /PI lines/ }));
    fireEvent.keyDown(document, { key: "Escape" });
    expect(screen.queryByRole("region", { name: "PI lines" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /PI lines/ }));
    fireEvent.click(within(screen.getByRole("region", { name: "PI lines" })).getByRole("button", { name: /All PI/ }));
    await screen.findByText("150 units / 台");
  });

  it("uses one column list for headers, cells and restore defaults", async () => {
    await selectPi();
    openView();
    const columns = within(screen.getByRole("group", { name: "Columns / 显示列" }));
    const table = screen.getByRole("table");
    const headers = within(table.querySelector("thead")!);
    fireEvent.click(columns.getByRole("checkbox", { name: "Interior" }));
    expect(headers.queryByRole("columnheader", { name: "Interior" })).toBeNull();
    expect(table.querySelectorAll("tbody td").length).toBe(5 * table.querySelectorAll("thead th").length);
    fireEvent.click(columns.getByRole("checkbox", { name: "Production" }));
    expect(headers.getByRole("columnheader", { name: "Production" })).toBeTruthy();
    fireEvent.click(columns.getByRole("button", { name: /Restore default columns/ }));
    expect(headers.getByRole("columnheader", { name: "Interior" })).toBeTruthy();
    expect(headers.queryByRole("columnheader", { name: "Production" })).toBeNull();
  });

  it("updates checked cars using the existing bulk endpoint without sending VINs", async () => {
    await selectPi();
    fireEvent.click(screen.getByRole("checkbox", { name: "Select CAR-1" }));
    fireEvent.click(screen.getByRole("button", { name: /Update selected status/ }));
    expect(screen.getByText("1 selected vehicles / 勾选车辆")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("ETD"), { target: { value: "2026-10-12" } });
    fireEvent.click(screen.getByRole("button", { name: /^Update status$/ }));
    await waitFor(() => expect(api.bulkUpdateVehicleAllocationVehicles).toHaveBeenCalledWith({
      piCode: PI, piLineCode: undefined, carCodes: ["CAR-1"], fields: { etd: "2026-10-12" },
    }));
    await screen.findByText(/车辆状态已更新/);
  });

  it("clears stale checked cars and unsaved status fields on line changes", async () => {
    await selectPi();
    fireEvent.click(screen.getByRole("checkbox", { name: "Select CAR-1" }));
    fireEvent.click(screen.getByRole("button", { name: /Update selected status/ }));
    fireEvent.change(screen.getByLabelText("ETD"), { target: { value: "2026-10-12" } });
    fireEvent.click(screen.getByRole("button", { name: /PI lines/ }));
    fireEvent.click(within(screen.getByRole("region", { name: "PI lines" })).getByRole("button", { name: /L02/ }));
    expect((screen.getByLabelText("ETD") as HTMLInputElement).value).toBe("");
    expect(screen.queryByText("1 selected vehicles / 勾选车辆")).toBeNull();
    expect(screen.getByText(/75 vehicles \/ 全范围/)).toBeTruthy();
    fireEvent.change(screen.getByLabelText("ETD"), { target: { value: "2026-10-13" } });
    fireEvent.click(screen.getByRole("button", { name: /^Update status$/ }));
    await waitFor(() => expect(api.bulkUpdateVehicleAllocationVehicles).toHaveBeenCalledWith({
      piCode: PI, piLineCode: `${PI}-L02`, carCodes: undefined, fields: { etd: "2026-10-13" },
    }));
  });

  it("retains the PI and line scope when resetting view filters", async () => {
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: /PI lines/ }));
    fireEvent.click(within(screen.getByRole("region", { name: "PI lines" })).getByRole("button", { name: /L02/ }));
    openView();
    fireEvent.change(screen.getByPlaceholderText("Material"), { target: { value: "OTHER" } });
    fireEvent.click(screen.getByRole("button", { name: "Reset" }));
    await waitFor(() => expect(vi.mocked(api.listVehicleAllocationVehicles).mock.calls.at(-1)?.[0]).toMatchObject({ piCode: PI, piLineCode: `${PI}-L02`, page: 1 }));
    expect((screen.getByPlaceholderText("Material") as HTMLInputElement).value).toBe("");
    expect(screen.getByText("75 units / 台")).toBeTruthy();
  });

  it("updates the scope summary after saving an individual vehicle", async () => {
    vi.spyOn(api, "updateVehicleAllocationVehicle").mockResolvedValue({ ...vehicle(1), vin: null, logisticsStatus: "ready_for_pickup" });
    await selectPi();
    fireEvent.click(screen.getByRole("cell", { name: "CAR-1" }));
    fireEvent.click(screen.getByRole("button", { name: "Save Vehicle" }));
    await screen.findByText("131 no VIN / 待录");
    expect(screen.getByRole("button", { name: /51 ready · 50 allocated/ })).toBeTruthy();
  });
});
