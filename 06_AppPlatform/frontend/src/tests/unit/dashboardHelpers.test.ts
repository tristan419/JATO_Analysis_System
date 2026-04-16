import { describe, expect, it } from "vitest";

import {
  formatDashboardSummaryMetric,
  getDashboardLensSummary,
  isDashboardBootstrapping,
} from "../../pages/dashboardHelpers";
import type { OverviewResponse } from "../../types";

const loadedOverview: OverviewResponse = {
  route: "raw",
  kpis: {
    totalRows: 1,
    countryCount: 1,
    brandCount: 1,
    modelCount: 1,
    versionCount: 1,
  },
  monthSeries: [],
  yearSeries: [],
};

describe("dashboardHelpers bootstrapping helpers", () => {
  it("detects cold dashboard bootstrap only before filters are ready", () => {
    expect(isDashboardBootstrapping(false, true, null)).toBe(true);
    expect(isDashboardBootstrapping(true, true, null)).toBe(false);
    expect(isDashboardBootstrapping(false, false, null)).toBe(false);
    expect(isDashboardBootstrapping(false, true, loadedOverview)).toBe(false);
  });

  it("renders placeholders for summary metrics during bootstrap", () => {
    expect(formatDashboardSummaryMetric(0, true)).toBe("...");
    expect(formatDashboardSummaryMetric(12345, false)).toBe("12,345");
    expect(formatDashboardSummaryMetric(undefined, false)).toBe("0");
  });

  it("uses a loading summary for the implicit default lens during bootstrap", () => {
    expect(getDashboardLensSummary("Default powertrain lens", 0, true)).toBe(
      "Loading default powertrain lens...",
    );
    expect(getDashboardLensSummary("国家: Germany", 1, true)).toBe("国家: Germany");
    expect(getDashboardLensSummary("Default powertrain lens", 0, false)).toBe(
      "Default powertrain lens",
    );
  });
});
