import { describe, expect, it } from "vitest";

import routeDiagnosticsSource from "../../pages/RouteDiagnosticsPage.tsx?raw";

async function readSourceFile(relativePath: string): Promise<string> {
  // @ts-ignore Frontend browser types intentionally omit Node modules; this unit test runs in Vitest.
  const { readFileSync } = await import("node:fs") as {
    readFileSync: (path: URL, encoding: "utf8") => string;
  };
  return readFileSync(new URL(relativePath, import.meta.url), "utf8");
}

describe("Route diagnostics CSS split", () => {
  it("keeps hidden route diagnostics styles out of the global startup stylesheet", async () => {
    const [globalCss, routeDiagnosticsCss] = await Promise.all([
      readSourceFile("../../index.css"),
      readSourceFile("../../pages/RouteDiagnosticsPage.css"),
    ]);

    expect(routeDiagnosticsCss).toContain(".route-diagnostics-shell");
    expect(routeDiagnosticsSource).toContain('import "./RouteDiagnosticsPage.css";');
    expect(globalCss).not.toContain(".route-diagnostics-shell");
  });
});
