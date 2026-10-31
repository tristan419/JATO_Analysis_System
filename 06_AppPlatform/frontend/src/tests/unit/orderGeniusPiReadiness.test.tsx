// @vitest-environment jsdom
import { act, cleanup, fireEvent, render as baseRender, screen, waitFor, within } from "@testing-library/react";
import type { ReactElement } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CellValueChangedEvent } from "ag-grid-community";
import type { OrderGeniusGridProps, OrderGeniusGridRow } from "../../components/OrderGeniusGrid";
import type { VehicleAllocationPlan } from "../../types/orderGeniusVehicle";
import type { QuantityCellResponse } from "../../types/orderGenius";
import { api } from "../../api/client";
import { materialAssets } from "../../api/materialAssets";
import { OrderGeniusPage } from "../../pages/OrderGeniusPage";

const render = (ui: ReactElement) => baseRender(<MemoryRouter>{ui}</MemoryRouter>);

const authState = vi.hoisted(() => ({ user: { role: "admin", primaryCountry: "CH", secondaryCountries: [] as string[], brands: [] as string[] }, refreshUser: vi.fn<() => Promise<boolean>>() }));
vi.mock("../../contexts/AuthContext", async (original) => ({
  ...await original<typeof import("../../contexts/AuthContext")>(),
  useAuth: () => ({ user: authState.user, refreshUser: authState.refreshUser, profileLoaded: true }),
}));
vi.mock("../../components/OrderGeniusGrid", async (original) => ({
  ...await original<typeof import("../../components/OrderGeniusGrid")>(),
  OrderGeniusGrid: (props: Omit<OrderGeniusGridProps, "onCellValueChanged"> & { onCellValueChanged?: (event: Pick<CellValueChangedEvent<OrderGeniusGridRow>, "data" | "colDef" | "newValue" | "oldValue">) => void }) => {
    const row = props.rows.find((item) => item.__type !== "groupHeader");
    return <div>
      <button onClick={() => props.rows.filter((item) => item.__type === "groupHeader").forEach((item) => props.onToggleGroup?.(item.__groupKey || ""))}>Expand rows</button>
      <button disabled={!row} onClick={() => {
        if (!row) return;
        // Fixture supplies exactly the fields read by the AG Grid event adapter.
        props.onCellValueChanged?.({ data: row, colDef: { field: "month_10" }, newValue: 15, oldValue: row.month_10 });
      }}>Save 15</button>
      <button disabled={!row || !props.canEditQuantities} onClick={() => {
        if (row) props.onCellValueChanged?.({ data: row, colDef: { field: "month_10" }, newValue: 80, oldValue: row.month_10 });
      }}>Save 80</button>
      <output data-testid="display-quantity">{row?.month_10}</output>
      <button onClick={() => {
        const nextRow = props.rows.filter((item) => item.__type !== "groupHeader")[1];
        if (nextRow) props.onCellValueChanged?.({ data: nextRow, colDef: { field: "month_10" }, newValue: 5, oldValue: nextRow.month_10 });
      }}>Save next 5</button>
      <output data-testid="eligible-rows">{props.selectableRowIds?.size ?? 0}</output>
      <output data-testid="period-price">{row?.fobEur}</output>
      <output data-testid="group-price">{props.rows.find((item) => item.__type === "groupHeader")?.fobEur}</output>
      <output data-testid="display-row-order">{JSON.stringify(props.rows.map((item) => ({
        kind: item.__type, groupKey: item.__groupKey, materialCode: item.materialCode,
      })))}</output>
      <button disabled={!props.piSelectionSummary?.selectableCount} onClick={() => props.piSelectionSummary?.onToggleAll(true)}>Select candidates</button>
    </div>;
  },
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
function plan(quantity: number): VehicleAllocationPlan {
  return {
    countryCode: "CH", year: new Date().getFullYear(), month: 10, orderMonth: "2026-10", status: "pending",
    lineItems: [{ materialCode: "T6481QNCLLX0003", selectedQuantity: quantity, generatedQuantity: 2,
      generatedVehicleCount: 2, remainingQuantity: Math.max(0, quantity - 2), overGeneratedQuantity: 0 }],
    selectedLineItems: [], remainingLineItems: [], existingLines: [],
    totals: { selectedQuantity: quantity, generatedQuantity: 2, generatedVehicleCount: 2, remainingQuantity: Math.max(0, quantity - 2), overGeneratedQuantity: 0 },
  };
}
const savedQuantity: QuantityCellResponse = { orderQuantityCellId: "q", countryCode: "CH", orderYear: 2026, orderMonth: 10, materialCode: "T6481QNCLLX0003", quantity: 15, fobEur: 29000, rowVersion: 2 };
beforeEach(() => {
  // Isolate notification reads from the quantity request's deliberate 401/403.
  // A delayed unrelated failure must not replace the renewed-session fixture.
  vi.spyOn(api, "getMaterialNotices").mockImplementation(async (country) => ({ countryCode: country, notices: [] }));
  authState.refreshUser.mockReset().mockResolvedValue(true);
  authState.user.role = "admin";
  authState.user.brands = [];
  vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country) => ({
    countryCode: country, today: "2026-10-12", windowFrom: "2026-09-12", windowTo: "2026-11-11",
    nodes: [], links: [], events: [],
  }));
  vi.spyOn(api, "getOrderGeniusCountries").mockResolvedValue({ items: [{ countryCode: "CH", countryName: "Switzerland", paymentTermCode: "TT", paymentMethod: "TT", lcDays: null }] });
  vi.spyOn(api, "getOrderGeniusFobCountries").mockResolvedValue({ countries: ["CH"] });
  vi.spyOn(api, "getOrderGeniusOptions").mockResolvedValue({ countryCode: "CH", paymentTermCode: "TT", brands: [], models: [], powertrains: [], versions: [], colours: [], materialCodes: [] });
  vi.spyOn(api, "getOrderGeniusMatrixBatch").mockResolvedValue({ errors: {}, matrices: { CH: {
    countryCode: "CH", countryName: "Switzerland", paymentTermCode: "TT", year: new Date().getFullYear(), totalRows: 1,
    rows: [{ materialCode: "T6481QNCLLX0003", bomTemplate: "T6481QN**LX0003", brand: "JAECOO", modelName: "JAECOO8 SHS", version: "Premium", colour: "Black", colourCode: "CL", colourTier: "single", powertrain: "PHEV", fobEur: null,
      lifecycleStatus: "active", editable: true, displayStyle: null, remark: null, ttl: 0,
      months: { "10": { quantity: 0, rowVersion: 1, isEditable: true, fobEur: 29000 } } }],
  } } });
  vi.spyOn(api, "getVehicleAllocationPis").mockResolvedValue({ items: [], total: 0 });
  vi.spyOn(api, "getVehicleAllocationOrderMatrixPlan").mockResolvedValue(plan(0));
});
afterEach(() => { cleanup(); localStorage.clear(); vi.unstubAllGlobals(); vi.restoreAllMocks(); });

async function openOctober() {
  render(<OrderGeniusPage />);
  const monthSelect = screen.getAllByRole("combobox").find((element) => element.textContent?.includes("All months"));
  if (!monthSelect) throw new Error("Missing month selector");
  fireEvent.change(monthSelect, { target: { value: "10" } });
  await waitFor(() => expect(screen.getByText("PI ready · 0 units available")).toBeTruthy());
  fireEvent.click(await screen.findByText("Expand rows"));
  await waitFor(() => expect(screen.getByText("Save 15")).toBeTruthy());
}

describe("quantity save → PI readiness", () => {
  it("loads the confirmed country scope once, without speculative matrix/options requests", async () => {
    const countries = deferred<Awaited<ReturnType<typeof api.getOrderGeniusCountries>>>();
    vi.mocked(api.getOrderGeniusCountries).mockReturnValue(countries.promise);
    render(<OrderGeniusPage />);
    expect(api.getOrderGeniusMatrixBatch).not.toHaveBeenCalled();
    expect(api.getOrderGeniusOptions).not.toHaveBeenCalled();
    expect(api.getVehicleAllocationOrderMatrixPlan).not.toHaveBeenCalled();
    await act(async () => countries.resolve({ items: [{ countryCode: "CH", countryName: "Switzerland", paymentTermCode: "TT", paymentMethod: "TT", lcDays: null }] }));
    await waitFor(() => expect(api.getOrderGeniusMatrixBatch).toHaveBeenCalledTimes(1));
    expect(api.getOrderGeniusOptions).toHaveBeenCalledTimes(1);
    expect(vi.mocked(api.getOrderGeniusMatrixBatch).mock.calls[0][0].countries).toEqual(["CH"]);
  });

  it("requests PI batches while the allocation plan is pending, but keeps creation unavailable", async () => {
    const allocation = deferred<VehicleAllocationPlan>();
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockReturnValue(allocation.promise);
    render(<OrderGeniusPage />);
    await waitFor(() => expect(api.getOrderGeniusMatrixBatch).toHaveBeenCalledTimes(1));
    const month = screen.getAllByRole("combobox").find((element) => element.textContent?.includes("All months"));
    if (!month) throw new Error("Missing month selector");
    fireEvent.change(month, { target: { value: "10" } });
    await waitFor(() => expect(api.getVehicleAllocationOrderMatrixPlan).toHaveBeenCalledTimes(1));
    expect(api.getVehicleAllocationPis).toHaveBeenCalledTimes(1);
    expect(screen.queryByText(/PI ready/)).toBeNull();
    await act(async () => allocation.resolve(plan(0)));
    await waitFor(() => expect(screen.getByText("PI ready · 0 units available")).toBeTruthy());
  });

  it("ends initial loading for an empty country scope without fetching a fallback country", async () => {
    vi.mocked(api.getOrderGeniusCountries).mockResolvedValue({ items: [] });
    render(<OrderGeniusPage />);
    await waitFor(() => expect(screen.queryByText("Loading...")).toBeNull());
    expect(api.getOrderGeniusMatrixBatch).not.toHaveBeenCalled();
    expect(api.getOrderGeniusOptions).not.toHaveBeenCalled();
  });

  it("shows a country loading failure without starting dependent requests", async () => {
    vi.mocked(api.getOrderGeniusCountries).mockRejectedValue(new Error("offline"));
    render(<OrderGeniusPage />);
    await screen.findByText("Failed to load countries");
    expect(screen.queryByText("Loading...")).toBeNull();
    expect(api.getOrderGeniusMatrixBatch).not.toHaveBeenCalled();
  });

  it.each([401, 403])("offers a new-tab re-login immediately after %s and preserves the failed quantity for manual retry", async (status) => {
    localStorage.setItem("jato_auth_token", "failed-session");
    await openOctober();
    const fetch = vi.fn(async () => Response.json({ detail: status === 401 ? "Authentication required" : "Forbidden" }, { status }));
    vi.stubGlobal("fetch", fetch);
    const save = vi.spyOn(api, "updateQuantityCell");
    fireEvent.click(screen.getByText("Save 15"));
    const login = await screen.findByRole("link", { name: "Re-login / 重新登录" });
    expect(login.getAttribute("target")).toBe("_blank");
    expect(login.getAttribute("rel")).toBe("noopener noreferrer");
    expect(login.getAttribute("href")).toContain("/login?redirect=");
    expect(save).toHaveBeenCalledTimes(1);
    expect(authState.refreshUser).not.toHaveBeenCalled();
    expect(localStorage.getItem("jato_auth_token")).toBe("failed-session");
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    if (status === 403) {
      expect(screen.getByText(/请求被拒绝（403）/).textContent).toContain("核对国家、品牌及操作权限");
      expect(screen.queryByText(/当前账号没有执行此 BOM 操作的权限/)).toBeNull();
      expect(screen.queryByText(/登录已失效/)).toBeNull();
    }
    fireEvent(window, new Event("focus"));
    expect(authState.refreshUser).not.toHaveBeenCalled();
    localStorage.setItem("jato_auth_token", "renewed-session");
    await act(async () => { fireEvent(window, new Event("focus")); });
    await screen.findByText(/登录身份已更新，请主动重试刚才的操作/);
    expect(authState.refreshUser).toHaveBeenCalledWith({ preserveSession: true });
    expect(save).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    save.mockResolvedValue(savedQuantity);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(plan(15));
    fireEvent.click(screen.getByText("Retry"));
    await screen.findByText("PI ready · 13 units available");
    expect(save.mock.calls[1][0].quantity).toBe(15);
  });

  it.each([false, "network"])("keeps re-login available when the renewed-session check returns %s", async (result) => {
    localStorage.setItem("jato_auth_token", "failed-session");
    await openOctober();
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ detail: "Forbidden" }, { status: 403 })));
    fireEvent.click(screen.getByText("Save 15"));
    await screen.findByRole("link", { name: "Re-login / 重新登录" });
    if (result === false) authState.refreshUser.mockResolvedValue(false);
    else authState.refreshUser.mockRejectedValue(new Error("Offline"));
    localStorage.setItem("jato_auth_token", "renewed-session");
    fireEvent(window, new Event("focus"));
    await waitFor(() => expect(authState.refreshUser).toHaveBeenCalledTimes(1));
    if (result === "network") await screen.findByText(/无法核验登录状态/);
    expect(screen.getByRole("link", { name: "Re-login / 重新登录" })).toBeTruthy();
    expect(screen.queryByText(/登录身份已更新/)).toBeNull();
  });

  it("no saved PI guides directly to PI Batch without changing the month or creating automatically", async () => {
    const create = vi.spyOn(api, "generateVehicleAllocationFromOrderMatrix");
    const preview = vi.spyOn(api, "getMonthlyInvoiceContext").mockRejectedValue(new Error(JSON.stringify({ code: "no_saved_pi",
      message: "No saved PI / 尚无已保存PI", suggestion: "Create a batch / 请先创建批次" })));
    await openOctober();
    fireEvent.click(screen.getByRole("tab", { name: /Import \/ Export/ }));
    fireEvent.click(screen.getByRole("button", { name: "Export PI" }));
    await screen.findByText(/No saved PI.*尚无已保存PI.*Create a batch/);
    expect(preview).toHaveBeenCalledWith("CH", `${new Date().getFullYear()}-10`, "", "single", "", "template");
    expect(screen.queryByRole("button", { name: "Export XLSX" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Export PI data" })).toBeNull();
    expect(screen.getByLabelText("Saved PI")).toBeTruthy();
    expect(create).not.toHaveBeenCalled();
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Go to PI Batch / 前往创建批次" }));
    expect(screen.queryByRole("dialog")).toBeNull();
    fireEvent.click(screen.getByRole("tab", { name: /Filters/i }));
    expect((screen.getByLabelText("Order month") as HTMLSelectElement).value).toBe("10");
    expect(create).not.toHaveBeenCalled();
  });

  it("new batch view edits remaining units, keeps the monthly total and shows zero after allocation", async () => {
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    response.matrices.CH.rows[0].months["10"].quantity = 120;
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue(response);
    const allocatedPlan = (total: number, allocated: number): VehicleAllocationPlan => ({ ...plan(total),
      lineItems: [{ ...plan(total).lineItems[0], generatedQuantity: allocated, remainingQuantity: total - allocated }],
      totals: { ...plan(total).totals, generatedQuantity: allocated, remainingQuantity: total - allocated } });
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(allocatedPlan(120, 120));
    const save = vi.spyOn(api, "updateQuantityCell").mockResolvedValue({ ...savedQuantity, quantity: 200 });
    await openOctober();
    expect(screen.getByTestId("display-quantity").textContent).toBe("120");
    fireEvent.click(screen.getByLabelText("Enter new batch"));
    expect(screen.getByTestId("display-quantity").textContent).toBe("0");
    expect(save).not.toHaveBeenCalled();
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(allocatedPlan(200, 120));
    fireEvent.click(screen.getByText("Save 80"));
    await screen.findByText("PI ready · 80 units available");
    expect(save).toHaveBeenCalledWith(expect.objectContaining({ quantity: 80, expectedAllocatedQuantity: 120 }));
    expect(screen.getByTestId("display-quantity").textContent).toBe("80");
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(allocatedPlan(200, 200));
    const create = vi.spyOn(api, "generateVehicleAllocationFromOrderMatrix").mockResolvedValue({ piCode: "PI-CH-202610-002", lineCount: 1, vehicleCount: 80 });
    fireEvent.click(screen.getByText("Select candidates"));
    fireEvent.click(screen.getByRole("tab", { name: /PI Batch/i }));
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    await waitFor(() => expect(screen.getAllByText("PI ready · 0 units available").length).toBeGreaterThan(0));
    expect(create).toHaveBeenCalledWith(expect.objectContaining({ lineItems: [expect.objectContaining({ quantity: 80 })] }));
    expect(screen.getByTestId("display-quantity").textContent).toBe("0");
    fireEvent.click(screen.getByLabelText("Enter new batch"));
    expect(screen.getByTestId("display-quantity").textContent).toBe("200");
  });

  it("a changed allocation never auto-retries a pending entry and retains the entered remainder", async () => {
    const save = vi.spyOn(api, "updateQuantityCell").mockRejectedValue(Object.assign(new Error(JSON.stringify({ code: "quantity_conflict",
      country: "CH", month: "2026-10", materialCode: "T6481QNCLLX0003", message: "Allocation changed / 占用变化",
      suggestion: "Review before saving / 请核对后重存" })), { status: 409 }));
    await openOctober();
    fireEvent.click(screen.getByLabelText("Enter new batch"));
    fireEvent.click(screen.getByText("Save 80"));
    await screen.findByRole("button", { name: "Locate quantity / 定位数量格" });
    expect(save).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("display-quantity").textContent).toBe("80");
  });

  it("keeps PI templates reachable with no matrix rows, quantities or saved PI", async () => {
    const reactErrors = vi.spyOn(console, "error").mockImplementation(() => {});
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue({ ...response,
      matrices: { CH: { ...response.matrices.CH, rows: [], totalRows: 0 } } });
    render(<OrderGeniusPage />);
    fireEvent.click(await screen.findByRole("tab", { name: /Import \/ Export/ }));
    const button = await screen.findByRole("button", { name: "PI Templates / PI模板" });
    expect((button as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(button);
    expect(await screen.findByLabelText("Template country")).toBeTruthy();
    expect(screen.getByRole("checkbox", { name: "OMODA" })).toBeTruthy();
    expect(screen.getByRole("checkbox", { name: "CHERY" })).toBeTruthy();
    expect(reactErrors.mock.calls.filter((args) => String(args[0]).includes("same key"))).toEqual([]);
  });

  it("keeps a no-brand request actionable without a missing-FOB overlay or BOM entry", async () => {
    authState.user.role = "order_filler";
    vi.mocked(api.getOrderGeniusFobCountries).mockResolvedValue({ countries: [] });
    render(<OrderGeniusPage />);
    fireEvent.click(await screen.findByRole("button", { name: "Request brand access / 申请品牌" }));
    expect(await screen.findByRole("checkbox", { name: "OMODA" })).toBeTruthy();
    expect(screen.queryByText(/selected countries do not have BOM FOB/)).toBeNull();
    expect(screen.queryByRole("button", { name: "Open BOM Admin" })).toBeNull();
    expect(screen.queryByRole("tab", { name: /BOM Admin/ })).toBeNull();
    expect(api.getMaterialTimeline).not.toHaveBeenCalled();
  });
  it("exposes the existing CBU route in the BOM tools", async () => {
    await openOctober();
    fireEvent.click(screen.getByRole("tab", { name: /BOM \/ Assets/i }));
    expect(screen.getByRole("link", { name: "CBU Finance" }).getAttribute("href")).toBe("/product/order-genius/cbu");
  });
  it("keeps the new-batch toggle alongside the title without duplicate filters or subtitle", async () => {
    await openOctober();
    const header = screen.getByRole("heading", { name: "Order Genius" }).closest("header");
    expect(header?.contains(screen.getByLabelText("Enter new batch"))).toBe(true);
    expect(screen.queryByLabelText("Current Order Genius filters")).toBeNull();
    expect(screen.queryByText(/Country order matrix with FOB pricing/)).toBeNull();
    expect(screen.getByRole("tab", { name: /Filters/i })).toBeTruthy();
  });
  it("opens Assets in place and preserves the month, new-batch mode and quantity on close", async () => {
    vi.spyOn(materialAssets, "catalog").mockResolvedValue({ today: "2026-10-31", materials: [], bindings: [], groups: [], versions: [] });
    await openOctober(); fireEvent.click(screen.getByLabelText("Enter new batch"));
    const quantity = screen.getByTestId("display-quantity").textContent;
    fireEvent.click(screen.getByRole("tab", { name: /BOM \/ Assets/ }));
    fireEvent.click(screen.getByRole("button", { name: "Material Assets", exact: true }));
    await screen.findByRole("dialog", { name: "Material Assets" });
    fireEvent.click(await screen.findByRole("button", { name: "Close Material Assets" }));
    expect(screen.queryByRole("dialog", { name: "Material Assets" })).toBeNull();
    expect(screen.getByLabelText<HTMLInputElement>("Enter new batch").checked).toBe(true);
    expect(screen.getByTestId("display-quantity").textContent).toBe(quantity);
    expect(screen.getByText("PI ready · 0 units available")).toBeTruthy();
  });

  it("keeps common toggles outside More filters and applies them without reopening advanced filters", async () => {
    const update = vi.spyOn(api, "updateQuantityCell");
    await openOctober();
    fireEvent.click(screen.getByRole("tab", { name: /Filters/i }));
    const advanced = screen.getByText("More filters").closest("details");
    const grouping = screen.getByRole("checkbox", { name: "Group by product" });
    const hideEmpty = screen.getByRole("checkbox", { name: "Hide empty rows" });
    expect(advanced?.hasAttribute("open")).toBe(false);
    expect(grouping.closest("details")).toBeNull();
    expect(hideEmpty.closest("details")).toBeNull();
    expect((grouping as HTMLInputElement).checked).toBe(true);
    fireEvent.click(grouping);
    await waitFor(() => expect(screen.getByTestId("display-row-order").textContent).not.toContain("groupHeader"));
    fireEvent.click(hideEmpty);
    await waitFor(() => expect(screen.getByTestId("display-row-order").textContent).toBe("[]"));
    fireEvent.click(hideEmpty);
    await waitFor(() => expect(screen.getByTestId("display-row-order").textContent).toContain("T6481QNCLLX0003"));
    expect(advanced?.hasAttribute("open")).toBe(false);
    expect(update).not.toHaveBeenCalled();
  });

  it("uses brand, model number, saved powertrain and version order for product groups", async () => {
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    const base = response.matrices.CH.rows[0];
    const identities = [
      { brand: "JAECOO", modelName: "JAECOO5 HEV", powertrain: "HEV", version: "Alpha" },
      { brand: "OMODA", modelName: "OMODA10 ICE", powertrain: "ICE", version: "Alpha" },
      { brand: "OMODA", modelName: "OMODA5 BEV", powertrain: "BEV", version: "Alpha" },
      { brand: "OMODA", modelName: "OMODA5 HEV", powertrain: "HEV", version: "Zulu" },
      { brand: "OMODA", modelName: "OMODA5 HEV", powertrain: "HEV", version: "Alpha" },
      { brand: "OMODA", modelName: "OMODA5 SHS", powertrain: "PHEV", version: "Alpha" },
      { brand: "OMODA", modelName: "OMODA5 ICE", powertrain: "ICE", version: "Alpha" },
      { brand: "OMODA", modelName: "OMODA7 ICE", powertrain: "ICE", version: "Alpha" },
    ];
    response.matrices.CH.rows = identities.map((identity, index) => ({ ...base, ...identity, materialCode: `CODE${index}` }));
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue(response);
    render(<OrderGeniusPage />);
    const expectedKeys = [
      "OMODA|OMODA5 ICE|Alpha|ICE", "OMODA|OMODA5 HEV|Alpha|HEV", "OMODA|OMODA5 HEV|Zulu|HEV",
      "OMODA|OMODA5 BEV|Alpha|BEV", "OMODA|OMODA5 SHS|Alpha|PHEV", "OMODA|OMODA7 ICE|Alpha|ICE",
      "OMODA|OMODA10 ICE|Alpha|ICE", "JAECOO|JAECOO5 HEV|Alpha|HEV",
    ];
    const groupKeys = (): string[] => {
      const rows: Array<{ groupKey?: string }> = JSON.parse(screen.getByTestId("display-row-order").textContent || "[]");
      return rows.map((row) => row.groupKey || "");
    };
    await waitFor(() => expect(groupKeys()).toEqual(expectedKeys));
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue({ ...response, matrices: {
      CH: { ...response.matrices.CH, rows: [...response.matrices.CH.rows].reverse() },
    } });
    fireEvent.click(screen.getByRole("tab", { name: /Filters/i }));
    const requestsBeforeRefresh = vi.mocked(api.getOrderGeniusMatrixBatch).mock.calls.length;
    fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
    await waitFor(() => expect(api.getOrderGeniusMatrixBatch).toHaveBeenCalledTimes(requestsBeforeRefresh + 1));
    await waitFor(() => expect(groupKeys()).toEqual(expectedKeys));
    expect(response.matrices.CH.rows.map((row) => row.powertrain)).toEqual(identities.map((identity) => identity.powertrain));
  });

  it("waits for two different pending cells before refreshing and reopening PI", async () => {
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    response.matrices.CH.rows.push({ ...response.matrices.CH.rows[0], materialCode: "T6481QNBWLX0003", colour: "White",
      months: { "10": { quantity: 0, rowVersion: 1, isEditable: true, fobEur: 29000 } } });
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue(response);
    await openOctober();
    const first = deferred<QuantityCellResponse>();
    const second = deferred<QuantityCellResponse>();
    vi.spyOn(api, "updateQuantityCell").mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const refreshedPlan = plan(15);
    refreshedPlan.lineItems.push({ materialCode: "T6481QNBWLX0003", selectedQuantity: 5, generatedQuantity: 1,
      generatedVehicleCount: 1, remainingQuantity: 4, overGeneratedQuantity: 0 });
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(refreshedPlan);
    const requestsBefore = vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mock.calls.length;
    fireEvent.click(screen.getByText("Save 15"));
    fireEvent.click(screen.getByText("Save next 5"));
    await act(async () => { first.resolve(savedQuantity); await first.promise; });
    expect(screen.getByText("Saving quantity…")).toBeTruthy();
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    expect(api.getVehicleAllocationOrderMatrixPlan).toHaveBeenCalledTimes(requestsBefore);
    await act(async () => { second.resolve({ ...savedQuantity, materialCode: "T6481QNBWLX0003", quantity: 5 }); await second.promise; });
    await waitFor(() => expect(screen.getByText("PI ready · 17 units available")).toBeTruthy());
    expect(screen.getByTestId("eligible-rows").textContent).toBe("2");
  });

  it("does not apply a previous year's pending save to the newly selected year", async () => {
    await openOctober();
    const save = deferred<QuantityCellResponse>();
    vi.spyOn(api, "updateQuantityCell").mockReturnValue(save.promise);
    fireEvent.click(screen.getByText("Save 15"));
    fireEvent.click(screen.getByRole("tab", { name: /Filters/i }));
    fireEvent.change(screen.getByRole("combobox", { name: "Order year" }), { target: { value: "2027" } });
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue({ ...plan(0), year: 2027, orderMonth: "2027-10" });
    await act(async () => { save.resolve(savedQuantity); await save.promise; });
    await waitFor(() => expect(screen.getByText("PI ready · 0 units available")).toBeTruthy());
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    expect(api.updateQuantityCell).toHaveBeenCalledWith(expect.objectContaining({ orderYear: 2026 }));
  });

  it("makes month primary, keeps Price as of optional, and hides an unavailable empty template", async () => {
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    const row = response.matrices.CH.rows[0];
    row.months["10"] = { quantity: 0, rowVersion: 1, isEditable: false, fobEur: null, reason: "No price this month" };
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue(response);
    await openOctober();
    expect(screen.getByRole("combobox", { name: "Order month" })).toBeTruthy();
    expect(screen.getByText("More filters").closest("details")?.hasAttribute("open")).toBe(false);
    expect(screen.getByLabelText("Price as of").getAttribute("value")).toBe("");
    expect(screen.queryByText("Selection date")).toBeNull();
    expect(screen.getByText("Save 15").hasAttribute("disabled")).toBe(true);
    fireEvent.click(screen.getByRole("tab", { name: /PI Batch/i }));
    expect(screen.getByLabelText("Order date")).toBeTruthy();
  });

  it("keeps unavailable rows with saved quantities or an existing PI", async () => {
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    response.matrices.CH.rows[0].months["10"] = { quantity: 2, rowVersion: 1, isEditable: false, fobEur: null, reason: "No price this month" };
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue(response);
    await openOctober();
    expect(screen.getByText("Save 15").hasAttribute("disabled")).toBe(false);
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    cleanup();
    response.matrices.CH.rows[0].months["10"].quantity = 0;
    const existingPlan = plan(0);
    existingPlan.existingLines = [{ piLineAllocationId: "a", piCode: "CH-202610-001", piLineCode: "L1",
      marketCountryCode: "CH", orderYear: 2026, orderMonth: 10, materialCode: "T6481QNCLLX0003", quantity: 2, fobEur: 29000 }];
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(existingPlan);
    await openOctober();
    expect(screen.getByText("Save 15").hasAttribute("disabled")).toBe(false);
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
  });

  it("blocks stale PI candidates after an allocation refresh failure and supports retry", async () => {
    await openOctober();
    vi.spyOn(api, "updateQuantityCell").mockResolvedValue(savedQuantity);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockRejectedValueOnce(new Error("Plan unavailable"));
    fireEvent.click(screen.getByText("Save 15"));
    await waitFor(() => expect(screen.getByText("PI availability failed: Plan unavailable")).toBeTruthy());
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(plan(15));
    fireEvent.click(screen.getByText("Retry"));
    await waitFor(() => expect(screen.getByText("PI ready · 13 units available")).toBeTruthy());
  });

  it("waits for save and latest Q/P/R; all controls use R and the period price", async () => {
    await openOctober();
    const save = deferred<QuantityCellResponse>();
    const refresh = deferred<VehicleAllocationPlan>();
    vi.spyOn(api, "updateQuantityCell").mockReturnValue(save.promise);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockReturnValue(refresh.promise);
    fireEvent.click(screen.getByText("Save 15"));
    expect(screen.getByText("Saving quantity…")).toBeTruthy();
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    await act(async () => { save.resolve(savedQuantity); await save.promise; });
    expect(screen.getByText("Updating PI availability…")).toBeTruthy();
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    await act(async () => { refresh.resolve(plan(15)); await refresh.promise; });
    expect(screen.getByText("PI ready · 13 units available")).toBeTruthy();
    expect(screen.getByTestId("eligible-rows").textContent).toBe("1");
    expect(screen.getByTestId("period-price").textContent).toBe("29000");
    expect(screen.getByTestId("group-price").textContent).toBe("29000");
    fireEvent.click(screen.getByText("Select candidates"));
    fireEvent.click(screen.getByRole("tab", { name: /PI Batch/i }));
    expect(screen.getByText("1 rows · 13 units · 1 PI · by country")).toBeTruthy();
    expect(screen.getByRole("button", { name: "Create PI" }).hasAttribute("disabled")).toBe(false);
  });

  it("keeps failed quantity as a draft, blocks PI, and retries that quantity", async () => {
    await openOctober();
    const save = vi.spyOn(api, "updateQuantityCell").mockRejectedValueOnce(new Error("Network unavailable"))
      .mockResolvedValue(savedQuantity);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(plan(15));
    fireEvent.click(screen.getByText("Save 15"));
    await waitFor(() => expect(screen.getByText("Quantity not saved: Network unavailable")).toBeTruthy());
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    fireEvent.click(screen.getByText("Retry"));
    await waitFor(() => expect(screen.getByText("PI ready · 13 units available")).toBeTruthy());
    expect(save).toHaveBeenCalledTimes(2);
    expect(save.mock.calls[1][0].quantity).toBe(15);
  });

  it.each(["By country", "Ordering account"])("creates an ordinary PI in %s mode without a historical reason or invented order date", async (mode) => {
    const request = deferred<{ piCode: string; lineCount: number; vehicleCount: number }>();
    const create = vi.spyOn(api, "generateVehicleAllocationFromOrderMatrix").mockReturnValue(request.promise);
    await openOctober();
    vi.spyOn(api, "updateQuantityCell").mockResolvedValue(savedQuantity);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(plan(15));
    fireEvent.click(screen.getByText("Save 15"));
    await waitFor(() => expect(screen.getByText("PI ready · 13 units available")).toBeTruthy());
    fireEvent.click(screen.getByText("Select candidates"));
    fireEvent.click(screen.getByRole("tab", { name: /PI Batch/i }));
    fireEvent.click(screen.getByRole("button", { name: mode }));
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    await waitFor(() => expect(create).toHaveBeenCalledTimes(1));
    const creating = screen.getByRole("button", { name: "Creating…" });
    expect(creating.hasAttribute("disabled")).toBe(true);
    fireEvent.click(creating);
    expect(create).toHaveBeenCalledTimes(1);
    expect(create).toHaveBeenCalledWith(expect.objectContaining({
      countryCode: "CH", orderMonth: 10, orderDate: null, officialPiNo: null, includeHistorical: false,
      lineItems: [expect.objectContaining({ materialCode: "T6481QNCLLX0003", quantity: 13, fobEur: 29000,
        ...(mode === "By country"
          ? { historicalFobOverrideEur: undefined, historicalPriceReason: null }
          : { allocations: [expect.objectContaining({ countryCode: "CH", quantity: 13,
            historicalFobOverrideEur: undefined, historicalPriceReason: null })] }),
      })],
    }));
    const allocated = plan(15);
    allocated.lineItems[0] = { ...allocated.lineItems[0], generatedQuantity: 15, generatedVehicleCount: 15, remainingQuantity: 0 };
    allocated.totals = { ...allocated.totals, generatedQuantity: 15, generatedVehicleCount: 15, remainingQuantity: 0 };
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(allocated);
    await act(async () => {
      request.resolve({ piCode: "CH-202610-003", lineCount: 1, vehicleCount: 13 });
      await request.promise;
    });
    await waitFor(() => expect(screen.getByText("Created CH-202610-003")).toBeTruthy());
    expect(screen.getByRole("link", { name: "Open CH-202610-003" })).toBeTruthy();
    await waitFor(() => expect(screen.getAllByText("PI ready · 0 units available")).toHaveLength(2));
    expect(screen.getByTestId("eligible-rows").textContent).toBe("0");
    expect(screen.getByRole("button", { name: "Create PI" }).hasAttribute("disabled")).toBe(true);
  });

  it("shows date rejection next to Create, keeps the selection and retries after editing the date", async () => {
    const create = vi.spyOn(api, "generateVehicleAllocationFromOrderMatrix")
      .mockRejectedValueOnce(new Error("orderDate must be inside the selected order month"))
      .mockResolvedValue({ piCode: "CH-202610-003", lineCount: 1, vehicleCount: 13 });
    await openOctober();
    vi.spyOn(api, "updateQuantityCell").mockResolvedValue(savedQuantity);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(plan(15));
    fireEvent.click(screen.getByText("Save 15"));
    await waitFor(() => expect(screen.getByText("PI ready · 13 units available")).toBeTruthy());
    fireEvent.click(screen.getByText("Select candidates"));
    fireEvent.click(screen.getByRole("tab", { name: /PI Batch/i }));
    fireEvent.change(screen.getByLabelText("Order date"), { target: { value: "2026-09-30" } });
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("orderDate must be inside the selected order month");
    expect(alert.previousElementSibling?.contains(screen.getByRole("button", { name: "Create PI" }))).toBe(true);
    expect(screen.queryByText(/Price refresh failed/)).toBeNull();
    await waitFor(() => expect(screen.getByRole("button", { name: "Create PI" }).hasAttribute("disabled")).toBe(false));
    expect(screen.getByText("1 rows · 13 units · 1 PI · by country")).toBeTruthy();
    expect((screen.getByLabelText("Order date") as HTMLInputElement).value).toBe("2026-09-30");
    expect(api.updateQuantityCell).toHaveBeenCalledTimes(1);
    fireEvent.change(screen.getByLabelText("Order date"), { target: { value: "2026-10-01" } });
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    await waitFor(() => expect(create).toHaveBeenCalledTimes(2));
    expect(create.mock.calls[1][0].orderDate).toBe("2026-10-01");
    await waitFor(() => expect(screen.getByText("Created CH-202610-003")).toBeTruthy());
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("backfills a Historical material through monthly quantity and the shared PI entry without reactivating it", async () => {
    const response = await api.getOrderGeniusMatrixBatch({ countries: ["CH"], year: 2026 });
    const historicalRow = response.matrices.CH.rows[0];
    historicalRow.lifecycleStatus = "historical";
    historicalRow.historicalBackfill = true;
    historicalRow.priceSource = "undated_default";
    vi.mocked(api.getOrderGeniusMatrixBatch).mockResolvedValue(response);
    const create = vi.spyOn(api, "generateVehicleAllocationFromOrderMatrix").mockResolvedValue({
      piCode: "CH-202610-003", lineCount: 1, vehicleCount: 13,
    });
    await openOctober();
    fireEvent.click(screen.getByRole("checkbox", { name: "Include historical materials" }));
    await waitFor(() => expect(screen.getByText("Save 15").hasAttribute("disabled")).toBe(false));
    const save = vi.spyOn(api, "updateQuantityCell").mockResolvedValue(savedQuantity);
    vi.mocked(api.getVehicleAllocationOrderMatrixPlan).mockResolvedValue(plan(15));
    fireEvent.click(screen.getByText("Save 15"));
    await waitFor(() => expect(screen.getByText("PI ready · 13 units available")).toBeTruthy());
    expect(save).toHaveBeenCalledWith(expect.objectContaining({ includeHistorical: true, quantity: 15 }));
    fireEvent.click(screen.getByText("Select candidates"));
    fireEvent.click(screen.getByRole("tab", { name: /PI Batch/i }));
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    expect(screen.getByRole("alert").textContent).toContain("Confirm that this Historical PI backfill");
    expect(create).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("checkbox", { name: /Confirm Historical PI backfill/ }));
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    expect(screen.getByRole("alert").textContent).toContain("requires an order date");
    fireEvent.change(screen.getByLabelText("Order date"), { target: { value: "2026-10-01" } });
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    expect(screen.getByRole("alert").textContent).toContain("Confirm the undated default FOB");
    expect(create).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("checkbox", { name: /Use the displayed undated default FOB/ }));
    fireEvent.click(screen.getByRole("button", { name: "Create PI" }));
    await waitFor(() => expect(create).toHaveBeenCalledTimes(1));
    expect(create).toHaveBeenCalledWith(expect.objectContaining({
      includeHistorical: true, confirmHistorical: true, confirmUndatedDefaultFob: true, orderDate: "2026-10-01",
      lineItems: [expect.objectContaining({ quantity: 13, historicalPriceReason: null })],
    }));
    await waitFor(() => expect(screen.getByText("Created CH-202610-003")).toBeTruthy());
    expect(historicalRow.lifecycleStatus).toBe("historical");
    expect(save).toHaveBeenCalledTimes(1);
  });
});
