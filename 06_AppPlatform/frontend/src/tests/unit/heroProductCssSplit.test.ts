import { describe, expect, it } from "vitest";

import heroProductSource from "../../pages/HeroProductAnalysisView.tsx?raw";

async function readSourceFile(relativePath: string): Promise<string> {
  // @ts-ignore Frontend browser types intentionally omit Node modules; this unit test runs in Vitest.
  const { readFileSync } = await import("node:fs") as {
    readFileSync: (path: URL, encoding: "utf8") => string;
  };
  return readFileSync(new URL(relativePath, import.meta.url), "utf8");
}

describe("Hero Product CSS split", () => {
  it("keeps Hero Product route styles out of the global startup stylesheet", async () => {
    const [globalCss, heroProductCss] = await Promise.all([
      readSourceFile("../../index.css"),
      readSourceFile("../../pages/HeroProductAnalysisView.css"),
    ]);

    expect(heroProductCss).toContain(".hero-product-shell");
    expect(heroProductCss).toContain(".hero-product-slide-frame");
    expect(heroProductSource).toContain('import "./HeroProductAnalysisView.css";');
    expect(globalCss).not.toContain(".hero-product-shell");
    expect(globalCss).not.toContain(".hero-product-slide-frame");
  });
});
