import { describe, expect, it } from "vitest";

import {
  buildActivityHeatmapColumns,
  formatDataManagementBytes,
  formatDataManagementNumber,
  formatDataManagementTimestamp,
  getDataManagementStatusBadgeClass,
} from "../../utils/dataManagement";

describe("data management helpers", () => {
  it("formats timestamps and numeric values defensively", () => {
    expect(formatDataManagementTimestamp("2026-04-16T00:00:00+00:00")).toBeTruthy();
    expect(formatDataManagementTimestamp(null)).toBe("-");
    expect(formatDataManagementNumber(12345)).toBe("12,345");
    expect(formatDataManagementNumber(undefined)).toBe("-");
    expect(formatDataManagementBytes(5 * 1024 * 1024)).toBe("5.0 MB");
    expect(formatDataManagementBytes(undefined)).toBe("-");
  });

  it("maps statuses to badge classes", () => {
    expect(getDataManagementStatusBadgeClass("ready")).toBe("badge-active");
    expect(getDataManagementStatusBadgeClass("warning")).toBe("badge-warning");
    expect(getDataManagementStatusBadgeClass("inactive")).toBe("badge-inactive");
    expect(getDataManagementStatusBadgeClass("failed")).toBe("badge-danger");
  });

  it("groups activity days into heatmap columns", () => {
    const columns = buildActivityHeatmapColumns([
      { date: "2026-04-02", count: 2, level: 2 },
      { date: "2026-04-01", count: 1, level: 1 },
      { date: "2026-04-04", count: 4, level: 4 },
      { date: "2026-04-03", count: 3, level: 3 },
      { date: "2026-04-05", count: 0, level: 0 },
      { date: "2026-04-06", count: 1, level: 1 },
      { date: "2026-04-07", count: 0, level: 0 },
      { date: "2026-04-08", count: 2, level: 2 },
    ]);

    expect(columns).toHaveLength(2);
    expect(columns[0].map((item) => item.date)).toEqual([
      "2026-04-01",
      "2026-04-02",
      "2026-04-03",
      "2026-04-04",
      "2026-04-05",
      "2026-04-06",
      "2026-04-07",
    ]);
    expect(columns[1].map((item) => item.date)).toEqual(["2026-04-08"]);
  });
});
