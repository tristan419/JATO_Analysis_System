import { describe, expect, it } from "vitest";

import type { ConfigImportBatch, ConfigVariant } from "../../types";

/**
 * Contract fields that `_config_import_batch_payload()` / API returns.
 * Frontend `ConfigImportBatch` type fields must match these.
 */
const IMPORT_BATCH_CONTRACT_FIELDS = [
  "id",               // configImportBatchId → id on frontend
  "projectId",
  "importStatus",
  "sourceFilePath",
  "sheetName",
  "sourceSchemaVersion",
  "replaceMode",
  "totalRows",
  "importedRows",
  "skippedRows",
  "errorCount",
  "notes",
  "createdAt",
  "finishedAt",
] as const;

const VARIANT_CONTRACT_FIELDS = [
  "id",               // variantId → id on frontend
  "projectId",
  "configImportBatchId",
  "model",
  "trim",
  "marketCountry",
  "isActive",
  "createdAt",
  "updatedAt",
  "metaJson",
] as const;

function makeImportBatch(overrides: Partial<ConfigImportBatch> = {}): ConfigImportBatch {
  return {
    id: "cib-1",
    projectId: "proj-1",
    importStatus: "completed",
    sourceFilePath: "/imports/x5.xlsx",
    sheetName: "Data Export",
    sourceSchemaVersion: "v2",
    replaceMode: "append",
    totalRows: 120,
    importedRows: 115,
    skippedRows: 3,
    errorCount: 2,
    notes: null,
    createdAt: "2026-04-11T10:00:00+00:00",
    finishedAt: "2026-04-11T10:05:00+00:00",
    ...overrides,
  };
}

function makeVariant(overrides: Partial<ConfigVariant> = {}): ConfigVariant {
  return {
    id: "cv-1",
    projectId: "proj-1",
    configImportBatchId: "cib-1",
    model: "X5",
    trim: "xDrive40i",
    marketCountry: "Germany",
    isActive: true,
    createdAt: "2026-04-11T10:00:00+00:00",
    updatedAt: "2026-04-11T10:00:00+00:00",
    metaJson: null,
    ...overrides,
  };
}

describe("ConfigImportBatch contract", () => {
  it("has all contract-required fields from backend serializer", () => {
    const batch = makeImportBatch();
    for (const field of IMPORT_BATCH_CONTRACT_FIELDS) {
      expect(batch).toHaveProperty(field);
    }
  });

  it("importStatus drives badge logic without crashing", () => {
    for (const status of ["completed", "failed", "running", "pending"]) {
      const batch = makeImportBatch({ importStatus: status });
      const badge = batch.importStatus === "completed"
        ? "badge-active"
        : batch.importStatus === "failed"
          ? "badge-danger"
          : "badge-inactive";
      expect(badge).toBeTruthy();
    }
  });

  it("sourceFilePath renders filename via split", () => {
    const batch = makeImportBatch({ sourceFilePath: "/data/imports/bmw_x5.xlsx" });
    const filename = batch.sourceFilePath.split("/").pop();
    expect(filename).toBe("bmw_x5.xlsx");
  });

  it("createdAt is a valid ISO date string for toLocaleDateString", () => {
    const batch = makeImportBatch();
    const parsed = new Date(batch.createdAt);
    expect(Number.isNaN(parsed.getTime())).toBe(false);
  });
});

describe("ConfigVariant contract", () => {
  it("has all contract-required fields from backend serializer", () => {
    const variant = makeVariant();
    for (const field of VARIANT_CONTRACT_FIELDS) {
      expect(variant).toHaveProperty(field);
    }
  });

  it("isActive renders as badge text without crashing", () => {
    const active = makeVariant({ isActive: true });
    const inactive = makeVariant({ isActive: false });
    expect(active.isActive ? "Active" : "Inactive").toBe("Active");
    expect(inactive.isActive ? "Active" : "Inactive").toBe("Inactive");
  });
});
