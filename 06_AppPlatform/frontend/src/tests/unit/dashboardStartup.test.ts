import { describe, expect, it } from "vitest";

import dashboardApiSource from "../../api/dashboard.ts?raw";
import dashboardSource from "../../pages/DashboardPage.tsx?raw";

describe("Dashboard startup path", () => {
  it("keeps data freshness outside the immediate first paint request path", () => {
    expect(dashboardSource).toContain("const DASHBOARD_DATA_FRESHNESS_DELAY_MS = 10_000;");
    expect(dashboardSource).toContain("scheduleDashboardDelayedIdlePreload(() =>");
    expect(dashboardSource).toContain("dashboardApi.dataFreshness({ signal: controller.signal })");
    expect(dashboardSource).toContain("controller.abort();");
    expect(dashboardApiSource).toContain("dataFreshness: (init?: RequestInit) =>");
    expect(dashboardApiSource).toContain('request<{ items: DataFreshnessItem[] }>("/analysis/data-freshness", init)');
  });
});
