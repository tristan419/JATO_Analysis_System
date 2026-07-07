// @vitest-environment jsdom

import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

import { VehicleStatusBoard } from "../../components/vehicleAllocation";
import type { PiVehicleUnit, VehicleStatusFlowConfig } from "../../types/orderGeniusVehicle";

const STATUS_FLOW: VehicleStatusFlowConfig = {
  countryCode: "DK",
  orderingAccountCode: "NCG",
  source: "ordering_account",
  logistics: [
    {
      key: "pending",
      labelEn: "Ordered",
      labelZh: "已下单",
      order: 10,
      color: "#2563eb",
      icon: "clipboard-list",
      terminal: false,
      allowedTransitions: ["on_vessel"],
    },
    {
      key: "on_vessel",
      labelEn: "In shipping",
      labelZh: "海运途中",
      order: 20,
      color: "#0891b2",
      icon: "ship",
      terminal: false,
      allowedTransitions: ["delivered"],
    },
  ],
  allocation: [
    {
      key: "unallocated",
      labelEn: "Unallocated",
      labelZh: "未分配",
      order: 10,
      color: "#64748b",
      icon: "circle",
      terminal: false,
      allowedTransitions: ["allocated"],
    },
  ],
};

function vehicle(overrides: Partial<PiVehicleUnit>): PiVehicleUnit {
  return {
    vehicleUnitId: "unit-1",
    piCode: "PI-DK-202607-001",
    officialPiNo: null,
    orderingAccountCode: "NCG",
    orderingAccountName: null,
    shipmentBatchCode: null,
    portOfDischarge: null,
    piLineCode: "PI-DK-202607-001-L01",
    carCode: "CAR-DK-2607-001-L01-0001",
    vin: null,
    materialCode: "T000",
    bom: "T000**0001",
    brand: "JAECOO",
    modelName: "JAECOO5",
    version: "Select",
    powertrain: "BEV",
    exteriorColorName: "Black",
    exteriorColorCode: "CL",
    interiorColorName: "Black-Black",
    interiorColourCode: null,
    orderMonth: null,
    orderDate: null,
    productionDate: null,
    etd: null,
    eta: null,
    actualDepartureDate: null,
    actualArrivalDate: null,
    readyForPickupDate: null,
    shipName: null,
    countryCode: "DK",
    dealerCode: null,
    dealerName: null,
    customerRef: null,
    allocationStatus: "unallocated",
    logisticsStatus: "pending",
    shippingScheduleUrl: null,
    feishuTrackingUrl: null,
    remark: null,
    rowVersion: 1,
    ...overrides,
  };
}

afterEach(() => {
  cleanup();
});

describe("VehicleStatusBoard", () => {
  it("renders the full configured status flow including empty statuses", () => {
    render(
      <VehicleStatusBoard
        scopeLabel="PI-DK-202607-001"
        vehicles={[
          vehicle({ carCode: "CAR-DK-2607-001-L01-0001", logisticsStatus: "pending" }),
          vehicle({ carCode: "CAR-DK-2607-001-L01-0002", logisticsStatus: "pending", vin: "LVTDB21B9RD123456" }),
        ]}
        statusFlow={STATUS_FLOW}
      />,
    );

    expect(screen.getByText("PI-DK-202607-001")).toBeTruthy();
    expect(screen.getByText("Flow ordering_account · DK · NCG")).toBeTruthy();
    expect(screen.getByLabelText("Logistics Ordered 2 vehicles")).toBeTruthy();
    expect(screen.getByLabelText("Logistics In shipping 0 vehicles")).toBeTruthy();
    expect(screen.getByLabelText("Allocation Unallocated 2 vehicles")).toBeTruthy();
    expect(screen.getByText("No VIN")).toBeTruthy();
  });

  it("renders an empty state when no status flow is available", () => {
    render(<VehicleStatusBoard scopeLabel="No PI selected" vehicles={[]} statusFlow={null} />);

    expect(screen.getByText("No selected PI scope or status flow config.")).toBeTruthy();
  });
});
