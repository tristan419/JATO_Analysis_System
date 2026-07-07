import { describe, expect, it } from "vitest";

import advancedAnalysisSource from "../../pages/AdvancedAnalysisPage.tsx?raw";

async function readSourceFile(relativePath: string): Promise<string> {
  // @ts-ignore Frontend browser types intentionally omit Node modules; this unit test runs in Vitest.
  const { readFileSync } = await import("node:fs") as {
    readFileSync: (path: URL, encoding: "utf8") => string;
  };
  return readFileSync(new URL(relativePath, import.meta.url), "utf8");
}

describe("Advanced Analysis CSS split", () => {
  it("keeps Advanced Analysis private controls out of the global startup stylesheet", async () => {
    const [globalCss, advancedAnalysisCss] = await Promise.all([
      readSourceFile("../../index.css"),
      readSourceFile("../../pages/AdvancedAnalysisPage.css"),
    ]);

    expect(advancedAnalysisCss).toContain(".aa-target-model-picker");
    expect(advancedAnalysisCss).toContain(".aa-spec-input-grid");
    expect(advancedAnalysisCss).toContain(".aa-mode-ribbon");
    expect(advancedAnalysisSource).toContain('import "./AdvancedAnalysisPage.css";');
    expect(globalCss).not.toContain(".aa-target-model-picker");
    expect(globalCss).not.toContain(".aa-spec-input-grid");
    expect(globalCss).not.toContain(".aa-mode-ribbon");
    expect(globalCss).toContain(".market-scan-field");
  });
});
