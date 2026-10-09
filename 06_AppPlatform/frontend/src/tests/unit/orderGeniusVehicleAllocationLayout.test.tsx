// @vitest-environment jsdom
import { useEffect, type ComponentProps } from "react";
import type { VehicleAllocationGrid } from "../../components/VehicleAllocationPivotGrid";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { OrderGeniusVehicleAllocationPage } from "../../pages/OrderGeniusVehicleAllocationPage";
import type { PiOrderDetail, PiOrderLine, PiVehicleUnit, VehicleImportPreview } from "../../types/orderGeniusVehicle";
import { matchesVehicleText, VEHICLE_COLUMNS, VEHICLE_TEXT_FIELDS } from "../../components/vehicleAllocationFields";

// Multi-step full-page workflows exceed 5 seconds on the shared CI runner.
vi.setConfig({ testTimeout: 15_000 });

const testRole = vi.hoisted(() => ({ value: "admin", secondaryCountries: [] as string[] }));
vi.mock("../../contexts/AuthContext", () => ({ useAuth: () => ({ user: { role: testRole.value, brands: ["OMODA", "JAECOO"], primaryCountry: "CH", secondaryCountries: testRole.secondaryCountries } }) }));
vi.mock("../../hooks/useAccountCountryOptions", () => ({ useAccountCountryOptions: () => ({ countryOptions: [] }) }));
vi.mock("../../components/CommandSelect", () => ({
  CommandSelect: (props: { value: string; placeholder: string; options: Array<{ value: string; label: string }>; onChange: (value: string) => void }) => (
    <select aria-label={props.placeholder} value={props.value} onChange={(event) => props.onChange(event.target.value)}>
      <option value="">{props.placeholder}</option>
      {props.options.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
    </select>
  ),
}));

// Page workflows use a small display stub; real Grid interactions are covered by the browser regression.
vi.mock("../../components/VehicleAllocationPivotGrid", () => ({
  VehicleAllocationGrid: (props: ComponentProps<typeof VehicleAllocationGrid>) => {
    const columns = props.columns.filter((column) => props.visibleKeys.has(column.key));
    const visibleRows = [...props.ordinaryVehicles].sort((a, b) => Number(a.carCode.split("-").at(-1)) - Number(b.carCode.split("-").at(-1))).slice(0, 5);
    useEffect(() => props.onViewChange({ vehicles: props.ordinaryVehicles, ordinaryVehicles: props.ordinaryVehicles, columns: columns.map((column) => column.key) }));
    return <><span>{props.ordinaryVehicles.length} filtered</span>
      <button onClick={props.onFilterChange}>Change grid filter</button>
      <table aria-label="Vehicle details"><thead><tr>
        <th><input aria-label="Select filtered vehicles" type="checkbox" onChange={() => props.onSelection(new Set(props.ordinaryVehicles.map((vehicle) => vehicle.carCode)))} /></th>
        {columns.map((column) => <th key={column.key}>{column.label}</th>)}</tr></thead><tbody>
        {visibleRows.map((vehicle) => <tr key={vehicle.carCode}><td><input type="checkbox" aria-label={`Select ${vehicle.carCode}`} checked={props.selectedCodes.has(vehicle.carCode)} onChange={() => { const next = new Set(props.selectedCodes); if (next.has(vehicle.carCode)) next.delete(vehicle.carCode); else next.add(vehicle.carCode); props.onSelection(next); }} /></td>
          {columns.map((column) => <td key={column.key} onClick={() => column.key !== "cocPdf" && props.onEdit(vehicle)}>{props.renderCell(vehicle, column.key)}</td>)}</tr>)}
      </tbody></table></>;
  },
}));

const PI = "PI-CH-202609-001";
function vehicle(index: number): PiVehicleUnit {
  return {
    fobEur: 15000, freightEur: null, insuranceEur: null, vehicleUnitId: `unit-${index}`, piCode: PI, officialPiNo: null, orderingAccountCode: "CH",
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
  // jsdom has no layout observer; real shell resize geometry is covered in Chrome.
  vi.stubGlobal("ResizeObserver", class { observe(): void {} disconnect(): void {} });
  testRole.value = "admin";
  testRole.secondaryCountries = [];
  window.history.replaceState({}, "", "/product/order-genius/vehicle-allocation");
  vi.spyOn(api, "getVehicleAllocationPis").mockResolvedValue({ items: [detail().header], total: 1 });
  vi.spyOn(api, "getVehicleAllocationPiMonths").mockImplementation(async (year) => ({ year,
    items: [{ month: `${year}-09`, piCount: 60, vehicleCount: 450 }],
  }));
  vi.spyOn(api, "getVehicleAllocationPi").mockImplementation(async () => detail());
  vi.spyOn(api, "getVehicleAllocationStatusFlow").mockResolvedValue({ countryCode: "CH", orderingAccountCode: "CH", source: "default", allocation: [], logistics: [] });
  vi.spyOn(api, "listVehicleAllocationVehicles").mockImplementation(async (filters) => ({
    items: detail().vehicles.filter((item) => !filters?.piLineCode || item.piLineCode === filters.piLineCode).slice(0, 5), total: 150,
  }));
  vi.spyOn(api, "bulkUpdateVehicleAllocationVehicles").mockResolvedValue({ piCode: PI, piLineCode: null, matchedUnits: 150, updatedUnits: 150, vinAssigned: 0, fieldsUpdated: ["etd"] });
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

async function selectPi() {
  render(<OrderGeniusVehicleAllocationPage />);
  await waitFor(() => expect(api.getVehicleAllocationPis).toHaveBeenCalled());
  fireEvent.click(await screen.findByRole("button", { name: new RegExp(`^${PI}`) }, { timeout: 5000 }));
  await screen.findByText("150 units / 台");
  await waitFor(() => expect(screen.getAllByRole("row").length).toBe(6));
}

it("updates only the page geometry variable on viewport resize", async () => {
  await selectPi();
  const page = document.querySelector<HTMLElement>(".vehicle-allocation-page");
  expect(page?.style.getPropertyValue("--va-available-height")).toBe(`${window.innerHeight}px`);
  vi.stubGlobal("innerHeight", 900);
  fireEvent(window, new Event("resize"));
  expect(page?.style.getPropertyValue("--va-available-height")).toBe("900px");
  expect(api.bulkUpdateVehicleAllocationVehicles).not.toHaveBeenCalled();
});

async function chooseOctober() {
  fireEvent.change(screen.getByLabelText("Browse Year"), { target: { value: "2026" } });
  await screen.findByRole("button", { name: "Oct 2026" }, { timeout: 5000 });
  await waitFor(() => expect(screen.getByRole("button", { name: "Oct 2026" }).hasAttribute("disabled")).toBe(false), { timeout: 5000 });
  fireEvent.click(screen.getByRole("button", { name: "Oct 2026" }));
}

describe("vehicle detail search and view", () => {
  it("does not clip admin browse queries to the primary country", async () => {
    await selectPi();
    expect(vi.mocked(api.getVehicleAllocationPis).mock.calls.at(-1)?.[0]?.country).toBe("");
    expect(vi.mocked(api.getVehicleAllocationPiMonths).mock.calls.at(-1)?.[1]).toBe("");
  });

  it("keeps filler country options to assigned units, not the shared PI header", async () => {
    testRole.value = "order_filler";
    testRole.secondaryCountries = ["SE"];
    const shared = detail();
    shared.header.countryCode = "CZ";
    shared.header.marketCountryCodes = ["CH", "SE", "CZ"];
    vi.mocked(api.getVehicleAllocationPi).mockResolvedValue(shared);
    await selectPi();
    const options = within(screen.getByRole("combobox", { name: "All accessible" })).getAllByRole("option").map((option) => option.getAttribute("value"));
    expect(options).toContain("CH");
    expect(options).toContain("SE");
    expect(options).not.toContain("CZ");
  });

  it("defaults VIN selection to global matches and permits filtered matches without auto selection", async () => {
    await selectPi(); openView();
    fireEvent.change(screen.getByLabelText("VIN batch search / VIN 批量搜索"), { target: { value: [vehicle(0).vin, vehicle(1).vin].join("\n") } });
    fireEvent.click(screen.getByRole("button", { name: "Search VIN batch / 搜索 VIN" }));
    // A header-filter snapshot is supplied by the real Grid in browser regression.
    expect((screen.getByLabelText("VIN selection scope") as HTMLSelectElement).value).toBe("global");
    expect(screen.queryByText(/2 selected \/ 已勾选/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /Select matches/ }));
    expect(screen.getByText(/2 selected \/ 已勾选/)).toBeTruthy();
    expect(screen.getByText(/Global selected \/ 全局已选 2\/150/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Clear selection/ }));
    fireEvent.change(screen.getByLabelText("VIN selection scope"), { target: { value: "filtered" } });
    expect(screen.queryByText(/2 selected \/ 已勾选/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /Select matches/ }));
    expect(screen.getByText(/2 selected \/ 已勾选/)).toBeTruthy();
  });

  it("blocks export during a PI re-read and after a failed read", async () => {
    const download = vi.spyOn(api, "exportVehicleAllocation").mockResolvedValue(new Blob());
    await selectPi(); openView();
    let rejectRead: (reason: Error) => void = () => {};
    vi.mocked(api.getVehicleAllocationPi).mockImplementationOnce(() => new Promise((_, reject) => { rejectRead = reject; }));
    fireEvent.click(screen.getByRole("button", { name: new RegExp(`^${PI}`) }));
    const button = screen.getByRole("button", { name: "Export current view / 导出所见" });
    expect(button.hasAttribute("disabled")).toBe(true);
    fireEvent.click(button);
    expect(download).not.toHaveBeenCalled();
    rejectRead(new Error("read failed"));
    await waitFor(() => expect(screen.queryByText("Loading PI / 正在读取 PI…")).toBeNull());
    expect(button.hasAttribute("disabled")).toBe(true);
    expect(download).not.toHaveBeenCalled();
  });
  it("uses all authorized PI markets, with summary independent of ordinary filters", async () => {
    const mixed = detail();
    mixed.header.marketCountryCodes = ["CH", "SE"];
    mixed.vehicles.forEach((car, index) => { if (index >= 75) car.countryCode = "SE"; });
    vi.mocked(api.getVehicleAllocationPi).mockResolvedValue(mixed);
    await selectPi();
    expect(screen.getByText("150 filtered")).toBeTruthy();
    expect(screen.getByText("20 VIN assigned / 已录")).toBeTruthy();
    fireEvent.change(screen.getByPlaceholderText("Search PI, Car Code, VIN, material, model, colour…"), { target: { value: "BOM-ONE" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(screen.getByText("75 filtered")).toBeTruthy();
    expect(screen.getByText("150 units / 台")).toBeTruthy();
  });

  it("ignores a slow previous PI and clears the old editor before reading", async () => {
    const second = detail();
    second.header = { ...second.header, piCode: "PI-CH-202609-002" };
    vi.mocked(api.getVehicleAllocationPis).mockResolvedValue({ items: [detail().header, second.header], total: 2 });
    await selectPi();
    fireEvent.click(screen.getByRole("cell", { name: "CAR-1" }));
    let resolveOld: (value: PiOrderDetail) => void = () => {};
    vi.mocked(api.getVehicleAllocationPi).mockImplementationOnce(() => new Promise((resolve) => { resolveOld = resolve; })).mockResolvedValueOnce(second);
    fireEvent.click(screen.getByRole("button", { name: new RegExp(`^${PI}`) }));
    expect(screen.queryByLabelText("Note / 备注")).toBeNull();
    expect(screen.getByRole("button", { name: "Save changes / 保存修改" }).hasAttribute("disabled")).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: /^PI-CH-202609-002/ }));
    await screen.findByRole("heading", { name: "PI-CH-202609-002" });
    resolveOld(detail());
    await waitFor(() => expect(screen.queryByRole("heading", { name: PI })).toBeNull());
  });

  it("opens the same selected editor from the deck status tab", async () => {
    await selectPi();
    fireEvent.click(screen.getByLabelText("Select filtered vehicles"));
    fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
    fireEvent.click(screen.getByRole("tab", { name: /Update Status/i }));
    expect(screen.getByText("150 selected vehicles / 勾选车辆")).toBeTruthy();
    expect(screen.getByLabelText("Note / 备注")).toBeTruthy();
  });

  it("shares editable field metadata and searches only meaningful text", () => {
    const car = { ...vehicle(0), vehicleUnitId: "internal-only", rowVersion: 998877, freightEur: 887766, eta: "2040-12-31" };
    for (const token of ["internal-only", "998877", "887766", "2040-12-31"]) expect(matchesVehicleText(car, token)).toBe(false);
    for (const token of ["jaecoo", "bom-one", "black-black", "hev"]) expect(matchesVehicleText(car, token)).toBe(true);
    expect(VEHICLE_COLUMNS.filter((column) => column.kind === "date").length).toBe(6);
    expect(VEHICLE_TEXT_FIELDS.map((column) => column.key)).toContain("actualArrivalDate");
  });

  it("makes vehicle notes read-only for viewer accounts", async () => {
    testRole.value = "viewer";
    await selectPi();
    fireEvent.click(screen.getByRole("cell", { name: "CAR-1" }));
    expect((screen.getByLabelText("Note / 备注") as HTMLTextAreaElement).readOnly).toBe(true);
    expect(screen.getByRole("button", { name: "Save changes / 保存修改" }).hasAttribute("disabled")).toBe(true);
  });



});

describe("PI month browsing", () => {
  it("uses whole-year counts, preserves selected PI/year independence and can reset to all months", async () => {
    await selectPi();
    expect(screen.getByRole("link", { name: "Create PI in Order Genius" }).getAttribute("href")).toBe("/product/order-genius");
    await chooseOctober();
    expect(screen.getByRole("button", { name: "Sep 2026" }).getAttribute("title")).toBe("60 PIs · 450 vehicles");
    expect(screen.getByRole("button", { name: "Sep 2026" }).textContent).toBe("Sep60");
    expect(vi.mocked(api.getVehicleAllocationPis).mock.calls.at(-1)?.[0]?.month).toBe("2026-10");
    fireEvent.change(screen.getByLabelText("Browse Year"), { target: { value: "2025" } });
    expect(screen.getByText("2026-10", { selector: "summary" })).toBeTruthy();
    await waitFor(() => expect(api.getVehicleAllocationPiMonths).toHaveBeenLastCalledWith(2025, ""));
    expect(screen.getByText("150 units / 台")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "All months" }));
    await waitFor(() => expect(vi.mocked(api.getVehicleAllocationPis).mock.calls.at(-1)?.[0]?.month).toBe(""));
    expect(screen.getByText("All months", { selector: "summary" })).toBeTruthy();
  });

  it("shows retry instead of false zero counts when summary fails", async () => {
    vi.mocked(api.getVehicleAllocationPiMonths).mockRejectedValueOnce(new Error("Network unavailable"));
    render(<OrderGeniusVehicleAllocationPage />);
    await screen.findByRole("button", { name: "Retry months" });
    expect(screen.getByRole("button", { name: `Jan ${new Date().getFullYear()}` }).hasAttribute("disabled")).toBe(true);
    expect(screen.getByRole("button", { name: `Jan ${new Date().getFullYear()}` }).getAttribute("title")).toBe("Counts unavailable");
    fireEvent.click(screen.getByRole("button", { name: "Retry months" }));
    await waitFor(() => expect(screen.getByRole("button", { name: `Jan ${new Date().getFullYear()}` }).hasAttribute("disabled")).toBe(false));
  });

  it("ignores a stale annual response after changing the year", async () => {
    let resolveOld: (value: { year: number; items: [] }) => void = () => {};
    vi.mocked(api.getVehicleAllocationPiMonths).mockImplementationOnce(() => new Promise((resolve) => { resolveOld = resolve; }));
    render(<OrderGeniusVehicleAllocationPage />);
    fireEvent.change(screen.getByLabelText("Browse Year"), { target: { value: "2025" } });
    await waitFor(() => expect(screen.getByRole("button", { name: "Sep 2025" }).textContent).toBe("Sep60"));
    resolveOld({ year: 2026, items: [] });
    await waitFor(() => expect(screen.getByRole("button", { name: "Sep 2025" }).textContent).toBe("Sep60"));
  });
});

describe("COC library lookup and downloads", () => {
  it("searches the whole PI only on request, filters results and downloads selected PDFs", async () => {
    const lookup = vi.spyOn(api, "piCocLookup").mockResolvedValue({ total: 150, available: 1, awaitingVin: 130, missing: 19,
      items: [{ carCode: "CAR-0", vin: vehicle(0).vin, status: "available" }, { carCode: "CAR-1", vin: vehicle(1).vin, status: "missing" }, { carCode: "CAR-20", vin: null, status: "awaiting_vin" }] });
    const download = vi.spyOn(api, "piCocDownload").mockResolvedValue(new Blob(["zip"]));
    const createUrl = vi.fn(() => "blob:zip");
    vi.stubGlobal("URL", class extends URL { static createObjectURL = createUrl; static revokeObjectURL = vi.fn(); });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    await selectPi();
    expect(lookup).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "COC library / 在线库" }));
    fireEvent.click(screen.getByRole("button", { name: "Search library / 在库里查找" }));
    await screen.findByText(/COC PDF 1\/150 · Awaiting/);
    expect(lookup).toHaveBeenCalledWith(PI);
    fireEvent.change(screen.getByLabelText("COC results / 查库结果"), { target: { value: "missing" } });
    expect(screen.getByRole("button", { name: "CAR-1" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "CAR-0" })).toBeNull();
    fireEvent.click(screen.getByLabelText("Select CAR-0"));
    fireEvent.click(screen.getByRole("button", { name: "Download selected COCs / 下载勾选 COC" }));
    await waitFor(() => expect(download).toHaveBeenCalledWith(PI, ["CAR-0"], { "CAR-0": vehicle(0).vin }));
    vi.unstubAllGlobals();
  });

  it("shows actionable guidance when library lookup fails", async () => {
    vi.spyOn(api, "piCocLookup").mockRejectedValue(new Error("503 Service Unavailable"));
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: "COC library / 在线库" }));
    fireEvent.click(screen.getByRole("button", { name: "Search library / 在库里查找" }));
    await screen.findByText(/查库未完成/);
    expect(screen.queryByText(/503/)).toBeNull();
    expect(screen.getByRole("link", { name: "Open COC workbench / 打开 COC 工作台" }).getAttribute("href")).toBe("/product/coc-match");
  });

  it("confirms mixed selection and downloads available PDFs only", async () => {
    vi.spyOn(api, "piCocLookup").mockResolvedValue({ total: 150, available: 1, missing: 19, awaitingVin: 130,
      items: [{ carCode: "CAR-0", vin: vehicle(0).vin, status: "available" }, { carCode: "CAR-1", vin: vehicle(1).vin, status: "missing" }, { carCode: "CAR-2", vin: null, status: "awaiting_vin" }] });
    const download = vi.spyOn(api, "piCocDownload").mockRejectedValue(new Error("409"));
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: "COC library / 在线库" }));
    fireEvent.click(screen.getByRole("button", { name: "Search library / 在库里查找" }));
    await screen.findByText(/COC PDF 1\/150 · Awaiting/);
    for (const code of ["CAR-0", "CAR-1", "CAR-2"]) fireEvent.click(screen.getByLabelText(`Select ${code}`));
    fireEvent.click(screen.getByRole("button", { name: "Download selected COCs / 下载勾选 COC" }));
    expect(download).not.toHaveBeenCalled();
    expect(screen.getByText(/仅下载 1 份可用 PDF/).textContent).toContain("缺 PDF 1 · 待录 VIN 1");
    fireEvent.click(screen.getByRole("button", { name: "Confirm available only / 确认仅下载可用" }));
    await waitFor(() => expect(download).toHaveBeenCalledWith(PI, ["CAR-0"], { "CAR-0": vehicle(0).vin }));
  });

  it("blocks all-missing downloads and invalidates confirmation after selection changes", async () => {
    vi.spyOn(api, "piCocLookup").mockResolvedValue({ total: 150, available: 1, missing: 149, awaitingVin: 0,
      items: [{ carCode: "CAR-0", vin: vehicle(0).vin, status: "available" }, { carCode: "CAR-1", vin: vehicle(1).vin, status: "missing" }] });
    const download = vi.spyOn(api, "piCocDownload").mockResolvedValue(new Blob());
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: "COC library / 在线库" }));
    fireEvent.click(screen.getByRole("button", { name: "Search library / 在库里查找" }));
    await screen.findByText(/COC PDF 1\/150 · Awaiting/);
    fireEvent.click(screen.getByLabelText("Select CAR-1"));
    fireEvent.click(screen.getByRole("button", { name: "Download selected COCs / 下载勾选 COC" }));
    await screen.findByText(/勾选车辆均无可下载 PDF/);
    expect(download).not.toHaveBeenCalled();
    fireEvent.click(screen.getByLabelText("Select CAR-0"));
    fireEvent.click(screen.getByRole("button", { name: "Download selected COCs / 下载勾选 COC" }));
    expect(screen.getByRole("button", { name: "Confirm available only / 确认仅下载可用" })).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Select CAR-0"));
    expect(screen.queryByRole("button", { name: "Confirm available only / 确认仅下载可用" })).toBeNull();
  });

  it("shows row download errors with the tools closed and does not show raw codes", async () => {
    vi.spyOn(api, "piCocLookup").mockResolvedValue({ total: 150, available: 1, missing: 149, awaitingVin: 0,
      items: [{ carCode: "CAR-0", vin: vehicle(0).vin, status: "available" }] });
    vi.spyOn(api, "piCocDownload").mockRejectedValue(new Error("500 Internal Server Error"));
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: "COC library / 在线库" }));
    fireEvent.click(screen.getByRole("button", { name: "Search library / 在库里查找" }));
    await screen.findByText(/COC PDF 1\/150 · Awaiting/);
    fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
    expect(screen.queryByRole("button", { name: "Search library / 在库里查找" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "PDF ↓" }));
    await screen.findByText(/下载未完成/);
    expect(screen.getByRole("button", { name: "Retry / 重试" })).toBeTruthy();
    expect(screen.queryByText(/500 Internal/)).toBeNull();
  });
});
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
    fireEvent.click(await screen.findByRole("button", { name: "Apply 1" }, { timeout: 5000 }));
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
    await screen.findByRole("button", { name: "Apply 1" }, { timeout: 5000 });
    fireEvent.click(screen.getByLabelText(/Allow replacing existing VINs/));
    await waitFor(() => expect(repreview).toHaveBeenLastCalledWith(expect.objectContaining({ piCode: PI, allowReplacing: true })));
  });

  it("does not call a saved import failed when the subsequent PI refresh fails", async () => {
    vi.spyOn(api, "previewVehicleAllocationImport").mockResolvedValue(vinPreview());
    const apply = vi.spyOn(api, "applyVehicleAllocationImport").mockResolvedValue({ createdUnits: 0, updatedUnits: 1, warnings: [] });
    await selectPi(); await uploadVinFile();
    vi.mocked(api.getVehicleAllocationPi).mockRejectedValueOnce(new Error("refresh unavailable"));
    fireEvent.click(await screen.findByRole("button", { name: "Apply 1" }, { timeout: 5000 }));
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
    fireEvent.click(await screen.findByRole("button", { name: "Apply 1" }, { timeout: 5000 }));
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



  it("confirms the whole PI deletion and removes stale scope without touching browse month", async () => {
    const remove = vi.spyOn(api, "deleteVehicleAllocationPi").mockResolvedValue({ pi_code: PI, deleted: true });
    await selectPi();
    await chooseOctober();
    fireEvent.click(screen.getByRole("button", { name: "Delete PI" }));
    expect(screen.getByText(/Delete 150 units \+ VINs/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Yes" }));
    await screen.findByText(/月需求保留，占用已释放/);
    expect(remove).toHaveBeenCalledWith(PI);
    expect(screen.queryByRole("button", { name: /PI lines/ })).toBeNull();
    expect(screen.getByText("2026-10", { selector: "summary" })).toBeTruthy();
  });
  it("has no duplicate creation or generic creation-import path", async () => {
    await selectPi();
    expect(screen.queryByRole("button", { name: /^Create PI$/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /^Generate$/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /Add Line/i })).toBeNull();
    expect(screen.getByRole("link", { name: "Create PI in Order Genius" }).getAttribute("href")).toBe("/product/order-genius");
    fireEvent.click(screen.getByRole("button", { name: /PI Tools/ }));
    expect(screen.getByRole("button", { name: "Import File" }).hasAttribute("disabled")).toBe(false);
    expect(screen.getByLabelText(/Allow replacing existing VINs/).hasAttribute("checked")).toBe(false);
    expect(screen.getAllByRole("tab").length).toBe(3);
  });

  it("keeps browsing independent and loads the whole PI instead of its first line", async () => {
    render(<OrderGeniusVehicleAllocationPage />);
    await chooseOctober();
    fireEvent.click(await screen.findByRole("button", { name: new RegExp(`^${PI}`) }));
    await screen.findByText("150 units / 台");
    expect(screen.getByText("2026-10", { selector: "summary" })).toBeTruthy();
    expect(api.listVehicleAllocationVehicles).not.toHaveBeenCalled();
    expect(screen.getByText("130 no VIN / 待录")).toBeTruthy();
    expect(screen.getByRole("button", { name: /50 ready for pickup · 50 allocated/ })).toBeTruthy();
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
    expect(screen.getByText("75 filtered")).toBeTruthy();
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
    const table = screen.getByRole("table", { name: "Vehicle details" });
    const headers = within(table.querySelector("thead")!);
    fireEvent.click(columns.getByRole("checkbox", { name: "Interior" }));
    expect(headers.queryByRole("columnheader", { name: "Interior" })).toBeNull();
    expect(table.querySelectorAll("tbody td").length).toBe(5 * table.querySelectorAll("thead th").length);
    fireEvent.click(columns.getByRole("checkbox", { name: "Production" }));
    expect(headers.getByRole("columnheader", { name: "Production" })).toBeTruthy();
    fireEvent.click(columns.getByRole("button", { name: /Reset columns & filters/ }));
    expect(headers.getByRole("columnheader", { name: "Interior" })).toBeTruthy();
    expect(headers.queryByRole("columnheader", { name: "Production" })).toBeNull();
  });







});

describe("unified Grid editing contract", () => {
  it("opens month browsing by default without changing the PI month", async () => {
    await selectPi();
    expect(screen.getByText("All months", { selector: "summary" }).parentElement?.hasAttribute("open")).toBe(true);
    expect(api.listVehicleAllocationVehicles).not.toHaveBeenCalled();
  });
  it("filters the complete PI locally and invalidates the old selection/editor", async () => {
    await selectPi();
    fireEvent.click(screen.getByLabelText("Select CAR-1"));
    fireEvent.click(screen.getByRole("button", { name: /Update selected status/ }));
    fireEvent.change(screen.getByLabelText("ETD"), { target: { value: "2026-10-12" } });
    fireEvent.change(screen.getByPlaceholderText(/Search PI, Car Code/), { target: { value: "BOM-TWO" } });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(screen.getByText("75 filtered")).toBeTruthy();
    expect(screen.queryByLabelText("ETD")).toBeNull();
    expect(screen.getByRole("button", { name: "Save changes / 保存修改" }).hasAttribute("disabled")).toBe(true);
  });
  it("never implicitly updates the whole PI with an empty selection", async () => {
    await selectPi();
    fireEvent.click(screen.getByRole("button", { name: /Status \/ 状态/ }));
    expect(screen.queryByLabelText("ETD")).toBeNull();
    expect(screen.getByRole("button", { name: "Save changes / 保存修改" }).hasAttribute("disabled")).toBe(true);
    expect(api.bulkUpdateVehicleAllocationVehicles).not.toHaveBeenCalled();
  });
  it("updates all 150 explicit targets with versions and only changed fields", async () => {
    await selectPi();
    fireEvent.click(screen.getByLabelText("Select filtered vehicles"));
    fireEvent.click(screen.getByRole("button", { name: /Update selected status/ }));
    fireEvent.change(screen.getByLabelText("Freight / 运费 (EUR)"), { target: { value: "0" } });
    fireEvent.change(screen.getByLabelText("Dealer name"), { target: { value: "Dealer" } });
    fireEvent.click(screen.getByRole("button", { name: "Save changes / 保存修改" }));
    await waitFor(() => expect(api.bulkUpdateVehicleAllocationVehicles).toHaveBeenCalled());
    const payload = vi.mocked(api.bulkUpdateVehicleAllocationVehicles).mock.calls.at(-1)?.[0];
    expect(payload?.carCodes).toHaveLength(150);
    expect(payload?.rowVersions).toEqual(Object.fromEntries(detail().vehicles.map((vehicle) => [vehicle.carCode, 1])));
    expect(payload?.fields).toEqual({ freightEur: 0, dealerName: "Dealer" });
    expect(screen.queryByLabelText("VIN")).toBeNull();
  });
  it("single edit ignores other checked rows and preserves unchanged fields", async () => {
    const save = vi.spyOn(api, "updateVehicleAllocationVehicle").mockResolvedValue(vehicle(1));
    await selectPi();
    fireEvent.click(screen.getByLabelText("Select CAR-2"));
    fireEvent.click(screen.getByRole("cell", { name: "CAR-1" }));
    fireEvent.change(screen.getByLabelText("Freight / 运费 (EUR)"), { target: { value: "123.45" } });
    fireEvent.click(screen.getByRole("button", { name: "Save changes / 保存修改" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith("CAR-1", { rowVersion: 1, freightEur: 123.45 }));
    expect(api.bulkUpdateVehicleAllocationVehicles).not.toHaveBeenCalled();
  });
  it("uses explicit clear; an untouched or blank field is not written", async () => {
    const save = vi.spyOn(api, "updateVehicleAllocationVehicle").mockResolvedValue(vehicle(1));
    await selectPi(); fireEvent.click(screen.getByRole("cell", { name: "CAR-1" }));
    const field = screen.getByLabelText("Insurance / 保费 (EUR)").parentElement;
    if (!field) throw Error("Missing cost field");
    fireEvent.click(within(field).getByRole("button", { name: "Clear" }));
    fireEvent.click(screen.getByRole("button", { name: "Save changes / 保存修改" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith("CAR-1", { rowVersion: 1, insuranceEur: null }));
  });
  it("keeps input on version conflicts and clears invalid editor on header filtering", async () => {
    vi.mocked(api.bulkUpdateVehicleAllocationVehicles).mockRejectedValueOnce(new Error("Selection changed; nothing saved / 勾选车辆已变化，未保存"));
    await selectPi(); fireEvent.click(screen.getByLabelText("Select filtered vehicles"));
    fireEvent.click(screen.getByRole("button", { name: /Update selected status/ }));
    fireEvent.change(screen.getByLabelText("ETD"), { target: { value: "2026-10-12" } });
    fireEvent.click(screen.getByRole("button", { name: "Save changes / 保存修改" }));
    await waitFor(() => expect(screen.getAllByText(/勾选车辆已变化/).length).toBeGreaterThan(0));
    expect((screen.getByLabelText("ETD") as HTMLInputElement).value).toBe("2026-10-12");
    fireEvent.click(screen.getByRole("button", { name: "Change grid filter" }));
    expect(screen.queryByLabelText("ETD")).toBeNull();
  });
  it("exports the entire visible view with the same columns, not a display page", async () => {
    const download = vi.spyOn(api, "exportVehicleAllocation").mockResolvedValue(new Blob());
    vi.stubGlobal("URL", class extends URL { static createObjectURL = () => "blob:export"; static revokeObjectURL = vi.fn(); });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    await selectPi(); openView();
    fireEvent.click(screen.getByRole("checkbox", { name: "Interior" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "Note / 备注" }));
    fireEvent.click(screen.getByRole("button", { name: "Export current view / 导出所见" }));
    await waitFor(() => expect(download).toHaveBeenCalled());
    expect(download.mock.calls[0][0]?.carCodes).toHaveLength(150);
    expect(download.mock.calls[0][0]?.columns).not.toContain("interiorColorName");
    expect(download.mock.calls[0][0]?.columns).toContain("remark");
    expect(screen.queryByRole("button", { name: "Move VIN left" })).toBeNull();
  });
});
