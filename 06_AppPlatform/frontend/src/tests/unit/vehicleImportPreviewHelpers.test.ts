import { describe, expect, it } from "vitest";

import {
  buildUnsupportedVehicleImportPreview,
  detectVehicleImportSource,
  parseVehicleImportRowsPayload,
} from "../../components/vehicleAllocation";

describe("vehicle allocation import preview helpers", () => {
  it("detects spreadsheet files by mime type or extension", () => {
    expect(detectVehicleImportSource({ name: "pi.xlsx", type: "" })).toBe("spreadsheet");
    expect(detectVehicleImportSource({
      name: "pi-upload",
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    })).toBe("spreadsheet");
  });

  it("does not expose CSV as an applyable vehicle allocation import", () => {
    expect(detectVehicleImportSource({ name: "pi.csv", type: "text/csv" })).toBe("unsupported");
  });

  it("detects image files by mime type or extension", () => {
    expect(detectVehicleImportSource({ name: "vin-photo.jpg", type: "" })).toBe("image");
    expect(detectVehicleImportSource({ name: "scan", type: "image/png" })).toBe("image");
    expect(detectVehicleImportSource({ name: "handoff.HEIC", type: "" })).toBe("image");
  });

  it("detects parsed image rows as a structured import source", () => {
    expect(detectVehicleImportSource({ name: "vin-photo-result.json", type: "" })).toBe("parsedRows");
    expect(detectVehicleImportSource({ name: "ocr-output", type: "application/json" })).toBe("parsedRows");
  });

  it("parses structured rows from OCR JSON output", async () => {
    const payload = await parseVehicleImportRowsPayload({
      name: "vin-photo-result.json",
      text: async () => JSON.stringify({
        rows: [
          { pi_code: "PI-SE-202607-001", vin: "LVTDB21B9RD123456" },
        ],
      }),
    });

    expect(payload).toEqual({
      source: "vin-photo-result.json",
      rows: [
        { pi_code: "PI-SE-202607-001", vin: "LVTDB21B9RD123456" },
      ],
    });
  });

  it("builds unsupported image previews in the same digest shape", () => {
    const preview = buildUnsupportedVehicleImportPreview("vin-photo.jpg", "image");

    expect(preview.status).toBe("error");
    expect(preview.totalRows).toBe(0);
    expect(preview.previewRows).toEqual([]);
    expect(preview.errors[0]).toContain("Image import not ready");
    expect(preview.errors[0]).toContain("same digest and apply workflow");
  });
});
