// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { VehicleAllocationSearch } from "../../components/VehicleAllocationSearch";
import { api } from "../../api/client";
import { matchesVehicleText, parseVehicleSearch } from "../../components/vehicleAllocationFields";
import type { PiVehicleUnit, VehicleAllocationSearchResult } from "../../types/orderGeniusVehicle";
import { orderGeniusPowertrainColor } from "../../utils/orderGeniusBomAdmin";

const result: VehicleAllocationSearchResult = { items: [{ piCode: "PI-CH-202608-001", countries: ["CH"], orderMonth: "2026-08",
  powertrains: ["BEV"], matched: 2, vehicleTotal: 450, carCodes: ["CAR-1", "CAR-2"] }], total: 1, matched: 2, page: 1, pageSize: 8, missingVins: [] };
const vehicles: PiVehicleUnit[] = [];
function setup() {
  const onApply = vi.fn(); const onAdd = vi.fn().mockResolvedValue(undefined);
  render(<VehicleAllocationSearch vehicles={vehicles} piCodes={[]} busy={false} applied="old filter" onApply={onApply} onAdd={onAdd} />);
  return { input: screen.getByRole("combobox", { name: "Search PI, VIN, material / 搜索" }), onApply, onAdd };
}
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.useRealTimers(); });

describe("allocation search", () => {
  it("combines words on the same snapshot and normalizes months without numeric/date-field noise", () => {
    const vehicle = { countryCode: "CH", orderMonth: "2026-08", modelName: "JAECOO5", powertrain: "BEV", exteriorColorCode: "BW" } as PiVehicleUnit;
    expect(matchesVehicleText(vehicle, "CH 2026/08 JAECOO5 BEV")).toBe(true);
    expect(matchesVehicleText(vehicle, "CH 202608；SE 202609")).toBe(true);
    expect(matchesVehicleText(vehicle, "JAECOO5 CL")).toBe(false);
    expect(parseVehicleSearch("LVTDB21B9RD123456\tLVTDB21B9RD123456\nINVALID")).toMatchObject({ vins: ["LVTDB21B9RD123456"], duplicates: 1, invalid: ["INVALID"] });
    expect(parseVehicleSearch("OOOOOOOOOOOOOOOOO").error).toContain("No valid VIN");
    expect(orderGeniusPowertrainColor("HEV")).toBe("#ca8a04");
    expect(orderGeniusPowertrainColor("MHEV")).toBe("#d97706");
  });

  it("waits 100ms, pauses IME, and never applies preview implicitly", async () => {
    vi.useFakeTimers(); const search = vi.spyOn(api, "searchVehicleAllocation").mockResolvedValue(result);
    const { input, onApply, onAdd } = setup();
    fireEvent.compositionStart(input); fireEvent.change(input, { target: { value: "CH" } });
    await act(async () => { await vi.advanceTimersByTimeAsync(200); }); expect(search).not.toHaveBeenCalled();
    fireEvent.compositionEnd(input);
    await act(async () => { await vi.advanceTimersByTimeAsync(99); }); expect(search).not.toHaveBeenCalled();
    await act(async () => { await vi.advanceTimersByTimeAsync(1); }); expect(search).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("option", { name: /PI-CH/ })).toBeTruthy(); expect(onApply).not.toHaveBeenCalled(); expect(onAdd).not.toHaveBeenCalled();
    fireEvent.keyDown(input, { key: "ArrowDown" }); fireEvent.keyDown(input, { key: "Enter" });
    expect(screen.getByRole("option", { name: /PI-CH/ }).getAttribute("aria-selected")).toBe("true");
    fireEvent.keyDown(input, { key: "Escape" }); expect(screen.queryByRole("listbox")).toBeNull();
  });

  it("aborts old queries, ignores late results, and clears only the applied search", async () => {
    let oldResolve: (value: VehicleAllocationSearchResult) => void = () => undefined;
    const search = vi.spyOn(api, "searchVehicleAllocation").mockImplementationOnce(() => new Promise((resolve) => { oldResolve = resolve; })).mockResolvedValue(result);
    const { input, onApply, onAdd } = setup();
    fireEvent.change(input, { target: { value: "OLD" } }); await waitFor(() => expect(search).toHaveBeenCalledTimes(1));
    fireEvent.change(input, { target: { value: "NEW" } }); await waitFor(() => expect(search).toHaveBeenCalledTimes(2));
    expect(search.mock.calls[0][3]?.aborted).toBe(true);
    await act(async () => oldResolve({ ...result, items: [{ ...result.items[0], piCode: "OLD" }] }));
    expect(screen.queryByRole("option", { name: /^OLD/ })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Clear search / 清空搜索" }));
    expect(screen.queryByRole("listbox")).toBeNull(); expect(onApply).toHaveBeenLastCalledWith("", null); expect(onAdd).not.toHaveBeenCalled();
  });

  it("submits one deduplicated VIN query and explicitly adds whole PIs with matched selection", async () => {
    const search = vi.spyOn(api, "searchVehicleAllocation").mockResolvedValue({ ...result, missingVins: ["LVTDB21B9RD123457"] });
    const { input, onAdd } = setup();
    fireEvent.paste(input, { clipboardData: { getData: () => "LVTDB21B9RD123456\nLVTDB21B9RD123456\nLVTDB21B9RD123457\nBAD" } });
    await screen.findByRole("option", { name: /PI-CH/ });
    expect(search).toHaveBeenCalledTimes(1); expect(search.mock.calls[0].slice(0, 3)).toEqual(["", ["LVTDB21B9RD123456", "LVTDB21B9RD123457"], 1]);
    expect((input as HTMLTextAreaElement).value).toBe("2 VINs");
    expect(screen.getByText(/1 duplicates/)).toBeTruthy();
    fireEvent.click(screen.getByRole("option", { name: /PI-CH/ }));
    fireEvent.click(screen.getByRole("button", { name: /Add 1 PIs and select 2 vehicles/ }));
    await waitFor(() => expect(onAdd).toHaveBeenCalledWith(["PI-CH-202608-001"], ["CAR-1", "CAR-2"],
      "LVTDB21B9RD123456\nLVTDB21B9RD123456\nLVTDB21B9RD123457\nBAD", ["LVTDB21B9RD123456", "LVTDB21B9RD123457"]));
  });

  it("distinguishes empty, failure, short input and complete pagination", async () => {
    const search = vi.spyOn(api, "searchVehicleAllocation").mockResolvedValueOnce({ ...result, total: 0, matched: 0, items: [] })
      .mockRejectedValueOnce(new Error("Search unavailable"))
      .mockResolvedValueOnce({ ...result, total: 2 }).mockResolvedValueOnce({ ...result, total: 2, page: 2, items: [{ ...result.items[0], piCode: "PI-SE-202609-001" }] });
    const { input } = setup();
    fireEvent.change(input, { target: { value: "C" } }); expect(search).not.toHaveBeenCalled();
    fireEvent.change(input, { target: { value: "NONE" } }); await screen.findByText("No matches / 无匹配结果"); expect(screen.queryByRole("listbox")).toBeNull();
    fireEvent.change(input, { target: { value: "FAIL" } }); await screen.findByText("Search unavailable"); expect(screen.queryByRole("listbox")).toBeNull();
    fireEvent.change(input, { target: { value: "CH" } }); await screen.findByRole("option", { name: /PI-CH/ });
    fireEvent.click(screen.getByRole("button", { name: /Show more/ })); await screen.findByRole("option", { name: /PI-SE/ });
    expect(within(screen.getByRole("listbox")).getAllByRole("option")).toHaveLength(2); expect(search.mock.calls.at(-1)?.[2]).toBe(2);
    fireEvent.pointerDown(document.body); expect(screen.queryByRole("listbox")).toBeNull();
  });
});
