import { describe, expect, it } from "vitest";

import routePerfScript from "../../../scripts/measure_route_performance.cjs?raw";

describe("route performance script", () => {
  it("measures Advanced Analysis by the main transfer result, not the background competitor result", () => {
    expect(routePerfScript).toContain('label: "advanced-analysis"');
    expect(routePerfScript).toContain('selector: ".market-scan-page"');
    expect(routePerfScript).toContain('dataPath: "/v1/advanced-analysis/transfer-mart"');
    expect(routePerfScript).not.toContain("/v1/advanced-analysis/competitor-set");
  });

  it("reports backend server cache state separately from edge cache state", () => {
    expect(routePerfScript).toContain('headers["x-jato-edge-cache"]');
    expect(routePerfScript).toContain('headers["x-jato-server-cache"]');
    expect(routePerfScript).toContain("server_memory");
    expect(routePerfScript).toContain("edge=");
    expect(routePerfScript).toContain("server=");
  });
});
