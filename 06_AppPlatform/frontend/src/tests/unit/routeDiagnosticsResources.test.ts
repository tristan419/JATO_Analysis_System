import { describe, expect, it } from "vitest";

import {
  INITIAL_RESOURCE_WINDOW_MS,
  summarizeRouteResources,
  type RouteResourceTiming,
} from "../../pages/RouteDiagnosticsPage";

function resource(
  kind: string,
  transferSize: number,
  startTimeMs: number,
): RouteResourceTiming {
  return {
    label: `${kind}-${startTimeMs}`,
    kind,
    durationMs: 100,
    startTimeMs,
    transferSize,
    encodedBodySize: transferSize,
    cached: transferSize === 0,
  };
}

describe("route diagnostics resource summary", () => {
  it("summarizes initial JS, CSS, and vendor transfer without counting cached bytes", () => {
    const resources = [
      resource("app shell", 1_200, 100),
      resource("dashboard", 25_000, 1_200),
      resource("css", 55_000, 500),
      resource("plotly", 0, 2_000),
      resource("grid", 900_000, INITIAL_RESOURCE_WINDOW_MS + 50),
    ];

    expect(summarizeRouteResources(resources)).toEqual({
      totalTransferBytes: 981_200,
      initialTransferBytes: 81_200,
      initialJsTransferBytes: 26_200,
      initialCssTransferBytes: 55_000,
      initialVendorCount: 1,
      resourceCount: 5,
    });
  });
});
