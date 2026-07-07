import { describe, expect, it } from "vitest";

import appSource from "../../App.tsx?raw";
import dashboardApiSource from "../../api/dashboard.ts?raw";
import dashboardSource from "../../pages/DashboardPage.tsx?raw";
import viteConfigSource from "../../../vite.config.ts?raw";

describe("Dashboard startup path", () => {
  it("keeps data freshness outside the immediate first paint request path", () => {
    expect(dashboardSource).toContain("const DASHBOARD_DATA_FRESHNESS_DELAY_MS = 10_000;");
    expect(dashboardSource).toContain("scheduleDashboardDelayedIdlePreload(() =>");
    expect(dashboardSource).toContain("dashboardApi.dataFreshness({ signal: controller.signal })");
    expect(dashboardSource).toContain("controller.abort();");
    expect(dashboardApiSource).toContain("dataFreshness: (init?: RequestInit) =>");
    expect(dashboardApiSource).toContain('request<{ items: DataFreshnessItem[] }>("/analysis/data-freshness", init)');
  });

  it("keeps shared filter scope out of the App startup chunk so the dashboard skeleton can paint first", () => {
    expect(appSource).toContain("const SharedFilterScopeProvider = lazy(() =>");
    expect(appSource).toContain('import("./contexts/SharedFilterScopeContext")');
    expect(appSource).not.toContain('import { SharedFilterScopeProvider } from "./contexts/SharedFilterScopeContext";');
    expect(appSource).toContain("fallback ?? <DashboardRouteSkeleton />");
  });

  it("keeps the small loading fallback out of the dashboard-core chunk", () => {
    expect(viteConfigSource).toContain("function isLoadingSurfaceModule");
    expect(viteConfigSource).toContain('return "loading-surface";');
    expect(viteConfigSource.indexOf("isLoadingSurfaceModule(id)")).toBeLessThan(
      viteConfigSource.indexOf("isDashboardCoreModule(id)"),
    );
    expect(viteConfigSource).not.toContain('"/src/hooks/useResolvedCountry.ts"');
  });

  it("keeps shared chart and export helpers out of the dashboard-core chunk", () => {
    expect(viteConfigSource).not.toContain('"/src/components/LazyPlotlyChart.tsx"');
    expect(viteConfigSource).not.toContain('"/src/components/PageFeedback.tsx"');
    expect(viteConfigSource).not.toContain('"/src/components/SearchSelectFilter.tsx"');
    expect(viteConfigSource).not.toContain('"/src/components/ExportPanelHelpers.ts"');
    expect(viteConfigSource).not.toContain('"/src/components/deckControls/DeckFloatingDrawer.tsx"');
    expect(viteConfigSource).not.toContain('"/src/utils/colors.ts"');
    expect(viteConfigSource).not.toContain('"/src/utils/jatoCountries.ts"');
    expect(viteConfigSource).not.toContain('"/src/utils/plotlyDefaults.ts"');
  });
});
