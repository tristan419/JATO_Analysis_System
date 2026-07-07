import { afterEach, describe, expect, it, vi } from "vitest";

import {
  INITIAL_RESOURCE_WINDOW_MS,
  probeCurrentApiPath,
  summarizeRouteResources,
  type RouteApiProbeSpec,
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

function stubLocalStorage(): void {
  const values = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
    removeItem: (key: string) => values.delete(key),
    clear: () => values.clear(),
  });
}

describe("route diagnostics resource summary", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

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

  it("probes current API paths with auth headers and cache diagnostics", async () => {
    stubLocalStorage();
    localStorage.setItem("jato_auth_token", "token-1");
    localStorage.setItem("jato_user_name", "test-user");
    localStorage.setItem("jato_user_role", "admin");
    const spec: RouteApiProbeSpec = {
      key: "grouped-time-series",
      label: "Grouped time-series default",
      method: "POST",
      path: "/analysis/time-series-grouped",
      body: {
        filters: {},
        grain: "month",
        group_by: "动总规整",
        top_n: 8,
        include_others: true,
      },
    };
    const fetchMock = vi.fn().mockResolvedValue(new Response("{}", {
      status: 200,
      headers: {
        "X-JATO-Server-Cache": "MEMORY",
        "X-JATO-Edge-Cache": "HIT",
      },
    }));
    vi.stubGlobal("fetch", fetchMock);

    const result = await probeCurrentApiPath(spec);

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/v1/analysis/time-series-grouped");
    const init = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined;
    expect(init?.method).toBe("POST");
    expect(init?.cache).toBe("no-store");
    expect(init?.body).toBe(JSON.stringify(spec.body));
    const headers = init?.headers as Headers | undefined;
    expect(headers?.get("X-Auth-Token")).toBe("token-1");
    expect(headers?.get("X-User-Name")).toBe("test-user");
    expect(headers?.get("X-User-Role")).toBe("admin");
    expect(headers?.get("Content-Type")).toBe("application/json");
    expect(result.status).toBe("ok");
    expect(result.statusCode).toBe(200);
    expect(result.serverCache).toBe("MEMORY");
    expect(result.edgeCache).toBe("HIT");
  });
});
