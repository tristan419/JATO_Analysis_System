// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { VehicleAllocationSearch } from "../../components/VehicleAllocationSearch";
import { api } from "../../api/client";
import { matchesVehicleText, parseVehicleSearch } from "../../components/vehicleAllocationFields";
import type { PiVehicleUnit, VehicleAllocationSearchResult } from "../../types/orderGeniusVehicle";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";

const vehicles = [1, 2].map((n) => ({ carCode: `CAR-${n}`, piCode: "PI-CH-202608-001", countryCode: "CH", vin: `LVTDB21B9RD12345${n}` } as PiVehicleUnit));
const result: VehicleAllocationSearchResult = { vehicles, items: [{ piCode: vehicles[0].piCode, countries: ["CH"], orderMonth: "2026-08",
  powertrains: ["BEV"], matched: 2, vehicleTotal: 450, carCodes: vehicles.map((v) => v.carCode) }], total: 1, matched: 2, page: 1, pageSize: 8, missingVins: [] };
function setup(selectedCount = 0) {
  const onApply = vi.fn(), onPreview = vi.fn(), onAdd = vi.fn().mockResolvedValue(undefined);
  render(<VehicleAllocationSearch busy={false} selectedCount={selectedCount} onPreview={onPreview} onApply={onApply} onAdd={onAdd} />);
  return { input: screen.getByRole("combobox", { name: "Search PI, VIN, material / 搜索" }), onApply, onPreview, onAdd };
}
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.useRealTimers(); });

describe("business allocation search", () => {
  it("retains snapshot matching, exact VIN parsing and the shared powertrain palette", () => {
    const v = { countryCode: "CH", orderMonth: "2026-08", modelName: "JAECOO5", powertrain: "BEV" } as PiVehicleUnit;
    expect(matchesVehicleText(v, "CH 2026/08 JAECOO5 BEV")).toBe(true);
    expect(matchesVehicleText(v, "JAECOO5 CL")).toBe(false);
    expect(parseVehicleSearch("LVTDB21B9RD123456\tLVTDB21B9RD123456\nBAD")).toMatchObject({ vins: ["LVTDB21B9RD123456"], duplicates: 1, invalid: ["BAD"] });
    expect(parseVehicleSearch("OOOOOOOOOOOOOOOOO").error).toContain("No valid VIN");
    expect(orderGeniusPowertrainColor("HEV")).toBe("#ca8a04");
  });
  it("waits 100ms after IME, displays results but never selects or mutates implicitly", async () => {
    vi.useFakeTimers(); const search = vi.spyOn(api, "searchVehicleAllocation").mockResolvedValue(result);
    const { input, onPreview, onAdd } = setup();
    expect(screen.queryByLabelText("Search scope / 搜索范围")).toBeNull();
    fireEvent.compositionStart(input); fireEvent.change(input, { target: { value: "CH" } });
    await act(async () => { await vi.advanceTimersByTimeAsync(200); }); expect(search).not.toHaveBeenCalled();
    fireEvent.compositionEnd(input); await act(async () => { await vi.advanceTimersByTimeAsync(99); }); expect(search).not.toHaveBeenCalled();
    await act(async () => { await vi.advanceTimersByTimeAsync(1); }); expect(search).toHaveBeenCalledTimes(1);
    expect(onPreview).toHaveBeenCalledWith(vehicles); expect(onAdd).not.toHaveBeenCalled();
    fireEvent.keyDown(input, { key: "ArrowDown" }); expect(document.activeElement?.textContent).toBe("勾选这 2 台");
    fireEvent.keyDown(input, { key: "Escape" }); expect(screen.queryByRole("region", { name: "Search preview / 搜索预览" })).toBeNull();
  });
  it("invalidates old responses and clear restores viewing without selecting", async () => {
    let finish: (v: VehicleAllocationSearchResult) => void = () => undefined;
    const search = vi.spyOn(api, "searchVehicleAllocation").mockImplementationOnce(() => new Promise((r) => { finish = r; })).mockResolvedValue(result);
    const { input, onPreview, onApply, onAdd } = setup();
    fireEvent.change(input, { target: { value: "OLD" } }); await waitFor(() => expect(search).toHaveBeenCalledTimes(1));
    fireEvent.change(input, { target: { value: "NEW" } }); await screen.findByRole("button", { name: "勾选这 2 台" });
    expect(search.mock.calls[0][3]?.aborted).toBe(true);
    await act(async () => finish({ ...result, vehicles: [] })); expect(onPreview).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole("button", { name: "Clear search / 清空搜索" }));
    expect(onPreview).toHaveBeenLastCalledWith(null); expect(onApply).toHaveBeenLastCalledWith("", null); expect(onAdd).not.toHaveBeenCalled();
  });
  it("summarizes pasted VINs, shows only real exceptions and selects just matching cars", async () => {
    const search = vi.spyOn(api, "searchVehicleAllocation").mockResolvedValue({ ...result, missingVins: ["LVTDB21B9RD123457"] });
    const { input, onAdd } = setup();
    const text = "LVTDB21B9RD123456\nLVTDB21B9RD123456\nLVTDB21B9RD123457\nBAD";
    fireEvent.paste(input, { clipboardData: { getData: () => text } });
    await screen.findByRole("button", { name: "勾选这 2 台" });
    expect(search).toHaveBeenCalledTimes(1); expect(input.getAttribute("readonly")).not.toBeNull();
    expect((input as HTMLInputElement).value).toBe("2 VINs · 查看"); expect(screen.queryByText("追加")).toBeNull();
    expect(screen.getByText("已去重 1 条")).toBeTruthy(); expect(screen.getByText(/异常 2 条/)).toBeTruthy();
    fireEvent.click(input); expect((screen.getByLabelText("VIN source / VIN原文") as HTMLTextAreaElement).value).toBe(text);
    fireEvent.click(screen.getByRole("button", { name: "勾选这 2 台" })); await waitFor(() => expect(onAdd).toHaveBeenCalledWith([vehicles[0].piCode], ["CAR-1", "CAR-2"], text, ["LVTDB21B9RD123456", "LVTDB21B9RD123457"], true));
  });
  it("selects every vehicle beyond eight PIs and only offers append when selection exists", async () => {
    const many = Array.from({ length: 10 }, (_, i) => ({ ...vehicles[0], carCode: `CAR-${i}`, piCode: `PI-${i}` }));
    vi.spyOn(api, "searchVehicleAllocation").mockResolvedValue({ ...result, vehicles: many, total: 10, matched: 10 });
    const { input, onAdd } = setup(3); fireEvent.change(input, { target: { value: "CH" } });
    fireEvent.click(await screen.findByRole("button", { name: "追加" }));
    await waitFor(() => expect(onAdd).toHaveBeenCalledWith(many.map((v) => v.piCode), many.map((v) => v.carCode), "CH", null, false));
  });
  it("requeries the identical batch without clear and supports replacement or Backspace", async () => {
    const search = vi.spyOn(api, "searchVehicleAllocation").mockResolvedValue(result);
    const { input, onAdd, onApply } = setup(); const text = "LVTDB21B9RD123456,LVTDB21B9RD123457";
    fireEvent.change(input, { target: { value: text } }); fireEvent.click(await screen.findByRole("button", { name: "勾选这 2 台" }));
    await waitFor(() => expect(onAdd).toHaveBeenCalledTimes(1));
    fireEvent.paste(input, { clipboardData: { getData: () => text } }); await screen.findByRole("button", { name: "勾选这 2 台" });
    expect(search).toHaveBeenCalledTimes(2); expect(onAdd).toHaveBeenCalledTimes(1); expect(onApply).not.toHaveBeenCalled();
    fireEvent.paste(input, { clipboardData: { getData: () => "LVTDB21B9RD123458" } });
    await waitFor(() => expect(search.mock.calls.at(-1)?.[1]).toEqual(["LVTDB21B9RD123458"]));
    fireEvent.keyDown(input, { key: "Backspace" }); expect(onApply).toHaveBeenLastCalledWith("", null);
  });
  it("distinguishes short input, failure and empty results without an empty dropdown", async () => {
    const search = vi.spyOn(api, "searchVehicleAllocation").mockRejectedValueOnce(new Error("Search unavailable")).mockResolvedValue({ ...result, vehicles: [], matched: 0, total: 0, items: [] });
    const { input, onPreview } = setup(); fireEvent.change(input, { target: { value: "C" } }); expect(search).not.toHaveBeenCalled();
    fireEvent.change(input, { target: { value: "FAIL" } }); await screen.findByText("Search unavailable"); expect(onPreview).not.toHaveBeenCalled();
    fireEvent.change(input, { target: { value: "NONE" } }); await screen.findByText("No matches / 无匹配结果"); expect(onPreview).toHaveBeenCalledWith([]);
    expect(screen.queryByRole("region", { name: "Search preview / 搜索预览" })).toBeNull();
  });
});
