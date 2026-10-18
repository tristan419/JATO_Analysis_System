// @vitest-environment jsdom
import { useState } from "react";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { VehicleColumnSettings } from "../../components/VehicleColumnSettings";
import { VEHICLE_COLUMNS, type VehicleColumnKey } from "../../components/vehicleAllocationFields";
import type { PiVehicleUnit } from "../../types/orderGeniusVehicle";

afterEach(cleanup);
function setup() {
  const targets: PiVehicleUnit[] = [{ carCode: "CAR-A", rowVersion: 3, customValues: { mine: "old" },
    fobEur: 10000, freightEur: null, insuranceEur: null, vehicleUnitId: "A", piCode: "PI-CH-202608-001", piLineCode: "L1",
    officialPiNo: null, orderingAccountCode: null, orderingAccountName: null, shipmentBatchCode: null, portOfDischarge: null,
    vin: null, materialCode: null, bom: null, brand: "OMODA", modelName: null, version: null, powertrain: null,
    exteriorColorName: null, exteriorColorCode: null, interiorColorName: null, interiorColourCode: null,
    orderDate: null, orderMonth: null, productionDate: null, etd: null, eta: null, actualDepartureDate: null,
    actualArrivalDate: null, readyForPickupDate: null, shipName: null, countryCode: "CH", dealerCode: null,
    dealerName: null, customerRef: null, allocationStatus: "unallocated", logisticsStatus: "pending",
    shippingScheduleUrl: null, feishuTrackingUrl: null, remark: null }];
  const onCreate = vi.fn().mockResolvedValue(undefined), onDelete = vi.fn().mockResolvedValue(undefined), onSave = vi.fn().mockResolvedValue(undefined);
  function Panel() {
    const [visible, setVisible] = useState(new Set<VehicleColumnKey>(VEHICLE_COLUMNS.filter((c) => !c.optional).map((c) => c.key)));
    return <VehicleColumnSettings columns={[{ id: "mine", label: "船批号", createdBy: "me", canDelete: true }, { id: "other", label: "提单说明", createdBy: "other", canDelete: false }]}
      visible={visible} targets={targets} busy={false} error="" onRetry={vi.fn()} onVisible={setVisible} onCreate={onCreate} onDelete={onDelete} onSave={onSave} />;
  }
  render(<Panel />); return { targets, onCreate, onDelete, onSave };
}
it("keeps Engine ID optional and exposes deletion only for server-authorized custom columns", async () => {
  const { onDelete, onCreate } = setup();
  fireEvent.click(screen.getByText("系统列"));
  const engine = screen.getByRole("checkbox", { name: "Engine ID / 发动机序号" }) as HTMLInputElement;
  expect(engine.checked).toBe(false); fireEvent.click(engine); expect(engine.checked).toBe(true);
  expect(screen.getAllByRole("button", { name: "删除" })).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: "删除" })); expect(onDelete).not.toHaveBeenCalled();
  const confirmation = screen.getByRole("dialog"); expect(confirmation.textContent).toContain("历史内容保留");
  fireEvent.click(within(confirmation).getByRole("button", { name: "确认删除列" }));
  await waitFor(() => expect(onDelete).toHaveBeenCalledWith("mine"));
  fireEvent.change(screen.getByLabelText("New column name / 新列名"), { target: { value: "  交付批次  " } });
  fireEvent.click(screen.getByRole("button", { name: "新增列" })); await waitFor(() => expect(onCreate).toHaveBeenCalledWith("交付批次"));
});
it("edits shared values against explicit selected vehicles and confirms before saving", async () => {
  const { targets, onSave } = setup();
  fireEvent.click(screen.getAllByRole("button", { name: "填写已勾选 1 台" })[0]);
  expect((screen.getByLabelText("Custom value / 自定义内容") as HTMLTextAreaElement).value).toBe("old");
  fireEvent.change(screen.getByLabelText("Custom value / 自定义内容"), { target: { value: "SHIP-42" } });
  fireEvent.click(screen.getByRole("button", { name: "保存内容" })); expect(onSave).not.toHaveBeenCalled();
  fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "确认保存内容" }));
  await waitFor(() => expect(onSave).toHaveBeenCalledWith("mine", "SHIP-42", targets));
});
