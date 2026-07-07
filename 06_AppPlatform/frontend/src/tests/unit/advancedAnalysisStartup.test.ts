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
});
