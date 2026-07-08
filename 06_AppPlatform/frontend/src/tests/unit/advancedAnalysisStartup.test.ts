import { describe, expect, it } from "vitest";

import advancedAnalysisSource from "../../pages/AdvancedAnalysisPage.tsx?raw";

describe("Advanced Analysis startup", () => {
  it("renders transfer results before the competitor analysis finishes", () => {
    const transferCommitIndex = advancedAnalysisSource.indexOf("setData(martResult);");
    const competitorLoadingIndex = advancedAnalysisSource.indexOf("setCompetitorLoading(true);");
    const competitorRequestIndex = advancedAnalysisSource.indexOf("api.post<CompetitorSetResponse>");

    expect(transferCommitIndex).toBeGreaterThan(-1);
    expect(competitorLoadingIndex).toBeGreaterThan(transferCommitIndex);
    expect(competitorRequestIndex).toBeGreaterThan(transferCommitIndex);
    expect(advancedAnalysisSource).toContain("setCompetitorData(null);");
    expect(advancedAnalysisSource).toContain("advanced-analysis-competitor-loading");
  });

  it("keeps the Hero Product grid runtime out of the transfer startup path", () => {
    expect(advancedAnalysisSource).toContain("const HeroProductAnalysisView = lazy(() =>");
    expect(advancedAnalysisSource).toContain("<Suspense fallback={<PageLoadingShell");
    expect(advancedAnalysisSource).not.toContain('import { HeroProductAnalysisView } from "./HeroProductAnalysisView";');
  });

  it("defers Plotly runtime so the transfer result shell paints before heavy chart code", () => {
    expect(advancedAnalysisSource).toContain("const ADVANCED_ANALYSIS_PLOTLY_DEFER_MS = 6_000;");
    expect(advancedAnalysisSource).toContain("return <LazyPlotlyChart {...props} deferMs={ADVANCED_ANALYSIS_PLOTLY_DEFER_MS} />;");
  });

  it("loads export settings only when the export drawer is opened", () => {
    expect(advancedAnalysisSource).toContain("const AdvancedAnalysisExportPanel = lazy(() =>");
    expect(advancedAnalysisSource).toContain('import("../components/ExportPanel")');
    expect(advancedAnalysisSource).toContain('import("../components/ExportPanelHelpers").then((module) =>');
    expect(advancedAnalysisSource).toContain("<AdvancedAnalysisExportPanel value={exportSettings}");
    expect(advancedAnalysisSource).not.toContain("import { DEFAULT_EXPORT, ExportPanel, downloadPng");
    expect(advancedAnalysisSource).not.toContain('from "../components/ExportPanel";');
  });
});
