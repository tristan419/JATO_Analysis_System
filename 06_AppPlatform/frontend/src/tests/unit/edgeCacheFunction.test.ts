// @vitest-environment node

import { afterEach, describe, expect, it, vi } from "vitest";

interface EdgeFunctionContext {
  request: Request;
  env: Record<string, string>;
  params: { path: string[] };
  waitUntil: (promise: Promise<unknown>) => void;
}

interface EdgeFunctionModule {
  onRequest: (context: EdgeFunctionContext) => Promise<Response>;
}

interface OriginCall {
  body: BodyInit | null | undefined;
  headers: Headers;
  method: string | undefined;
  url: string;
}

interface EdgeTestRuntime {
  cache: {
    match: ReturnType<typeof vi.fn>;
    put: ReturnType<typeof vi.fn>;
  };
  fetch: ReturnType<typeof vi.fn>;
  originCalls: OriginCall[];
  store: Map<string, Response>;
  waitUntil: ReturnType<typeof vi.fn>;
  waitUntilPromises: Promise<unknown>[];
}

let edgeFunctionModulePromise: Promise<EdgeFunctionModule> | null = null;

async function loadEdgeFunction(): Promise<EdgeFunctionModule> {
  edgeFunctionModulePromise ??= import(
    new URL("../../../functions/v1/[[path]].js", import.meta.url).href
  ) as Promise<EdgeFunctionModule>;
  return edgeFunctionModulePromise;
}

function createRuntime(
  originResponseFactory?: (sequence: number, call: OriginCall) => Response | Promise<Response>,
): EdgeTestRuntime {
  const store = new Map<string, Response>();
  const cache = {
    match: vi.fn(async (request: Request): Promise<Response | undefined> => {
      return store.get(request.url)?.clone();
    }),
    put: vi.fn(async (request: Request, response: Response): Promise<void> => {
      store.set(request.url, response.clone());
    }),
  };
  const originCalls: OriginCall[] = [];
  const fetch = vi.fn(async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const call: OriginCall = {
      body: init?.body,
      headers: new Headers(init?.headers),
      method: init?.method,
      url: String(input),
    };
    originCalls.push(call);
    if (originResponseFactory) {
      return originResponseFactory(originCalls.length, call);
    }
    return Response.json(
      { body: call.body, sequence: originCalls.length, url: call.url },
      { headers: { "content-type": "application/json", "set-cookie": "sid=from-origin" } },
    );
  });
  const waitUntilPromises: Promise<unknown>[] = [];
  const waitUntil = vi.fn((promise: Promise<unknown>): void => {
    waitUntilPromises.push(promise);
  });

  vi.stubGlobal("caches", { default: cache });
  vi.stubGlobal("fetch", fetch);

  return {
    cache,
    fetch,
    originCalls,
    store,
    waitUntil,
    waitUntilPromises,
  };
}

async function flushWaitUntil(runtime: EdgeTestRuntime): Promise<void> {
  await Promise.all(runtime.waitUntilPromises);
  runtime.waitUntilPromises.length = 0;
}

function makeRequest(path: string, init?: RequestInit): Request {
  return new Request(`https://intl.ojeur.cloud/v1/${path}`, init);
}

async function callEdgeFunction(
  runtime: EdgeTestRuntime,
  path: string,
  init?: RequestInit,
  env?: Record<string, string>,
): Promise<Response> {
  const { onRequest } = await loadEdgeFunction();
  return onRequest({
    request: makeRequest(path, init),
    env: {
      API_ORIGIN: "https://origin.example",
      DATA_VERSION: "dataset-a",
      ...env,
    },
    params: { path: path.split("?")[0].split("/") },
    waitUntil: runtime.waitUntil,
  });
}

describe("Cloudflare edge cache function", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("caches readonly analysis responses and strips origin cookies", async () => {
    const runtime = createRuntime();
    const body = JSON.stringify({ country: ["DK"], powertrain: ["BEV"] });
    const requestInit: RequestInit = {
      body,
      headers: {
        "cf-ray": "edge-ray",
        "content-type": "application/json",
        "x-auth-token": "token-a",
        "x-jato-data-version": "dataset-a",
        "x-user-name": "alice",
        "x-user-role": "admin",
      },
      method: "POST",
    };

    const first = await callEdgeFunction(runtime, "analysis/overview?chart=summary", requestInit);
    expect(first.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(first.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/analysis/overview");
    expect(first.headers.get("cache-control")).toBe("public, max-age=0, s-maxage=300");
    expect(first.headers.get("set-cookie")).toBeNull();
    expect(first.headers.get("vary")).toBe("X-User-Role, X-JATO-Data-Version");
    expect(await first.json()).toMatchObject({ sequence: 1 });

    await flushWaitUntil(runtime);

    const second = await callEdgeFunction(runtime, "analysis/overview?chart=summary", requestInit);
    expect(second.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await second.json()).toMatchObject({ sequence: 1 });
    expect(runtime.fetch).toHaveBeenCalledTimes(1);
    expect(runtime.cache.match).toHaveBeenCalledTimes(3);
    expect(runtime.cache.put).toHaveBeenCalledTimes(2);
    const cachedResponse = runtime.cache.put.mock.calls[0]?.[1] as Response | undefined;
    expect(cachedResponse?.headers.get("cache-control")).toBe("public, max-age=300");
    expect(runtime.originCalls[0]?.url).toBe("https://origin.example/v1/analysis/overview?chart=summary");
    expect(runtime.originCalls[0]?.headers.get("cf-ray")).toBeNull();
  });

  it("serves stale readonly responses while refreshing the fresh cache in the background", async () => {
    const runtime = createRuntime();
    const requestInit: RequestInit = {
      body: JSON.stringify({ filters: { 国家: ["丹麦"] }, top_n: 120 }),
      headers: {
        "content-type": "application/json",
        "x-auth-token": "token-a",
        "x-jato-data-version": "dataset-a",
        "x-user-name": "alice",
        "x-user-role": "viewer",
      },
      method: "POST",
    };

    const first = await callEdgeFunction(runtime, "analysis/overview", requestInit);
    expect(first.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(await first.json()).toMatchObject({ sequence: 1 });
    await flushWaitUntil(runtime);

    const keys = [...runtime.store.keys()];
    const freshKey = keys.find((key) => !key.includes("__layer=stale"));
    const staleKey = keys.find((key) => key.includes("__layer=stale"));
    expect(freshKey).toBeTruthy();
    expect(staleKey).toBeTruthy();
    runtime.store.delete(freshKey as string);

    const stale = await callEdgeFunction(runtime, "analysis/overview", requestInit);
    expect(stale.headers.get("x-jato-edge-cache")).toBe("STALE");
    expect(await stale.json()).toMatchObject({ sequence: 1 });
    await flushWaitUntil(runtime);
    expect(runtime.fetch).toHaveBeenCalledTimes(2);

    const refreshed = await callEdgeFunction(runtime, "analysis/overview", requestInit);
    expect(refreshed.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await refreshed.json()).toMatchObject({ sequence: 2 });
  });

  it("separates cached entries by user permission scope and data version", async () => {
    const runtime = createRuntime();
    const body = JSON.stringify({ group_by: ["动总规整", "国家"], grain: "month" });

    const scopedRequest = (
      token: string,
      dataVersion: string,
      userName = "dashboard-user",
      userRole = "viewer",
    ): RequestInit => ({
      body,
      headers: {
        "content-type": "application/json",
        "x-auth-token": token,
        "x-jato-data-version": dataVersion,
        "x-user-name": userName,
        "x-user-role": userRole,
      },
      method: "POST",
    });

    const first = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-a", "dataset-a"),
    );
    expect(first.headers.get("x-jato-edge-cache")).toBe("MISS");
    await flushWaitUntil(runtime);

    const refreshedToken = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-b", "dataset-a"),
    );
    expect(refreshedToken.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await refreshedToken.json()).toMatchObject({ sequence: 1 });

    const otherUserSameRole = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-c", "dataset-a", "other-user"),
    );
    expect(otherUserSameRole.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await otherUserSameRole.json()).toMatchObject({ sequence: 1 });

    const otherRole = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-d", "dataset-a", "dashboard-user", "admin"),
    );
    expect(otherRole.headers.get("x-jato-edge-cache")).toBe("MISS");
    await flushWaitUntil(runtime);

    const otherDataset = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-e", "dataset-b"),
    );
    expect(otherDataset.headers.get("x-jato-edge-cache")).toBe("MISS");
    await flushWaitUntil(runtime);

    const originalScope = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-a", "dataset-a"),
    );
    expect(originalScope.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await originalScope.json()).toMatchObject({ sequence: 1 });
    expect(runtime.fetch).toHaveBeenCalledTimes(3);
    expect(runtime.cache.put).toHaveBeenCalledTimes(6);
  });

  it("synthesizes and caches filter snapshots when the origin endpoint is missing", async () => {
    const runtime = createRuntime((_sequence, call) => {
      if (call.url === "https://origin.example/v1/metadata/filter-snapshot") {
        return new Response("not found", { status: 404 });
      }
      if (call.url === "https://origin.example/v1/metadata/columns") {
        return Response.json({
          items: ["国家", "Body type", "细分市场（按车长）", "动总规整", "Make", "Model"],
        });
      }
      if (call.url === "https://origin.example/v1/filters/options/batch") {
        return Response.json({
          items: [
            { column: "国家", options: ["丹麦", "德国"] },
            { column: "Body type", options: ["SUV"] },
            { column: "细分市场（按车长）", options: ["C"] },
            { column: "动总规整", options: ["ICE", "BEV"] },
          ],
        });
      }
      return new Response("unexpected", { status: 500 });
    });

    const first = await callEdgeFunction(runtime, "metadata/filter-snapshot", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });

    expect(first.status).toBe(200);
    expect(first.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(first.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/metadata/filter-snapshot");
    const payload = await first.json();
    expect(payload).toMatchObject({
      columns: ["国家", "Body type", "细分市场（按车长）", "动总规整", "Make", "Model"],
      options: {
        国家: ["丹麦", "德国"],
        "Body type": ["SUV"],
        "细分市场（按车长）": ["C"],
        动总规整: ["ICE", "BEV"],
      },
      source: "edge-synthesized",
    });
    expect(runtime.originCalls[2]?.method).toBe("POST");
    expect(runtime.originCalls[2]?.headers.get("content-type")).toBe("application/json");

    await flushWaitUntil(runtime);

    const second = await callEdgeFunction(runtime, "metadata/filter-snapshot", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(second.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await second.json()).toMatchObject({ source: "edge-synthesized" });
    expect(runtime.fetch).toHaveBeenCalledTimes(3);
    expect(runtime.cache.put).toHaveBeenCalledTimes(2);
  });

  it("caches readonly data freshness checks", async () => {
    const runtime = createRuntime();

    const first = await callEdgeFunction(runtime, "analysis/data-freshness", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(first.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(first.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/analysis/data-freshness");
    await flushWaitUntil(runtime);

    const second = await callEdgeFunction(runtime, "analysis/data-freshness", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(second.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await second.json()).toMatchObject({ sequence: 1 });
    expect(runtime.fetch).toHaveBeenCalledTimes(1);
    expect(runtime.cache.put).toHaveBeenCalledTimes(2);
  });

  it("caches readonly Advanced Analysis POST endpoints by role scope and request body", async () => {
    const runtime = createRuntime();
    const transferBody = JSON.stringify({
      country: "瑞典",
      fuel_types: [],
      sales_mode: "month",
      scope_filters: [],
      top_n: 25,
    });
    const requestInit: RequestInit = {
      body: transferBody,
      headers: {
        "content-type": "application/json",
        "x-auth-token": "token-a",
        "x-jato-data-version": "dataset-a",
        "x-user-name": "alice",
        "x-user-role": "order_filler",
      },
      method: "POST",
    };

    const firstTransfer = await callEdgeFunction(runtime, "advanced-analysis/transfer-mart", requestInit);
    expect(firstTransfer.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(firstTransfer.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/advanced-analysis/transfer-mart");
    expect(firstTransfer.headers.get("set-cookie")).toBeNull();
    expect(await firstTransfer.json()).toMatchObject({ sequence: 1 });
    await flushWaitUntil(runtime);

    const secondTransfer = await callEdgeFunction(runtime, "advanced-analysis/transfer-mart", requestInit);
    expect(secondTransfer.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await secondTransfer.json()).toMatchObject({ sequence: 1 });

    const adminTransfer = await callEdgeFunction(runtime, "advanced-analysis/transfer-mart", {
      body: transferBody,
      headers: {
        "content-type": "application/json",
        "x-auth-token": "token-a",
        "x-jato-data-version": "dataset-a",
        "x-user-name": "alice",
        "x-user-role": "admin",
      },
      method: "POST",
    });
    expect(adminTransfer.headers.get("x-jato-edge-cache")).toBe("MISS");
    await flushWaitUntil(runtime);

    const competitor = await callEdgeFunction(runtime, "advanced-analysis/competitor-set", {
      ...requestInit,
      body: JSON.stringify({
        country: "瑞典",
        fuel_types: [],
        profile_specs: {},
        sales_mode: "month",
        scope_filters: [],
        top_n: 12,
      }),
    });
    expect(competitor.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(competitor.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/advanced-analysis/competitor-set");
    await flushWaitUntil(runtime);

    const competitorHit = await callEdgeFunction(runtime, "advanced-analysis/competitor-set", {
      ...requestInit,
      body: JSON.stringify({
        country: "瑞典",
        fuel_types: [],
        profile_specs: {},
        sales_mode: "month",
        scope_filters: [],
        top_n: 12,
      }),
    });
    expect(competitorHit.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await competitorHit.json()).toMatchObject({ sequence: 3 });
    expect(runtime.fetch).toHaveBeenCalledTimes(3);
    expect(runtime.cache.put).toHaveBeenCalledTimes(6);
  });

  it("caches stable metadata helpers named by the intl prewarm job", async () => {
    const runtime = createRuntime();

    const firstColumns = await callEdgeFunction(runtime, "metadata/columns", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(firstColumns.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(firstColumns.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/metadata/columns");
    await flushWaitUntil(runtime);

    const secondColumns = await callEdgeFunction(runtime, "metadata/columns", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(secondColumns.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await secondColumns.json()).toMatchObject({ sequence: 1 });

    const firstAssistantMetadata = await callEdgeFunction(runtime, "assistant/country/metadata", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(firstAssistantMetadata.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(firstAssistantMetadata.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/assistant/country/metadata");
    await flushWaitUntil(runtime);

    const secondAssistantMetadata = await callEdgeFunction(runtime, "assistant/country/metadata", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(secondAssistantMetadata.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await secondAssistantMetadata.json()).toMatchObject({ sequence: 2 });

    const firstAdvancedCountries = await callEdgeFunction(runtime, "advanced-analysis/countries", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(firstAdvancedCountries.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(firstAdvancedCountries.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/advanced-analysis/countries");
    await flushWaitUntil(runtime);

    const secondAdvancedCountries = await callEdgeFunction(runtime, "advanced-analysis/countries", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(secondAdvancedCountries.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await secondAdvancedCountries.json()).toMatchObject({ sequence: 3 });

    const firstAdvancedProfile = await callEdgeFunction(runtime, "advanced-analysis/profile-options?country=%E7%91%9E%E5%85%B8", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(firstAdvancedProfile.headers.get("x-jato-edge-cache")).toBe("MISS");
    expect(firstAdvancedProfile.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/advanced-analysis/profile-options");
    await flushWaitUntil(runtime);

    const secondAdvancedProfile = await callEdgeFunction(runtime, "advanced-analysis/profile-options?country=%E7%91%9E%E5%85%B8", {
      headers: {
        "x-user-name": "dashboard-user",
        "x-user-role": "viewer",
      },
      method: "GET",
    });
    expect(secondAdvancedProfile.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await secondAdvancedProfile.json()).toMatchObject({ sequence: 4 });
    expect(runtime.fetch).toHaveBeenCalledTimes(4);
    expect(runtime.cache.put).toHaveBeenCalledTimes(8);
    expect(runtime.originCalls[3]?.url).toBe("https://origin.example/v1/advanced-analysis/profile-options?country=%E7%91%9E%E5%85%B8");
  });

  it("bypasses auth and other non-cacheable endpoints", async () => {
    const runtime = createRuntime();
    const response = await callEdgeFunction(runtime, "auth/login", {
      body: JSON.stringify({ username: "test" }),
      headers: { "content-type": "application/json" },
      method: "POST",
    });

    expect(response.headers.get("x-jato-edge-cache")).toBe("BYPASS");
    expect(response.headers.get("set-cookie")).toBe("sid=from-origin");
    expect(await response.json()).toMatchObject({ sequence: 1 });
    expect(runtime.fetch).toHaveBeenCalledTimes(1);
    expect(runtime.cache.match).not.toHaveBeenCalled();
    expect(runtime.cache.put).not.toHaveBeenCalled();
    expect(runtime.originCalls[0]?.url).toBe("https://origin.example/v1/auth/login");
  });

  it("returns an explicit timeout for hanging non-cacheable origin calls", async () => {
    const timeoutError = Object.assign(new Error("origin timed out"), { name: "AbortError" });
    const runtime = createRuntime(() => {
      throw timeoutError;
    });

    const response = await callEdgeFunction(runtime, "auth/me", {
      headers: { "x-auth-token": "token-a" },
      method: "GET",
    }, {
      API_BYPASS_TIMEOUT_MS: "5",
    });

    expect(response.status).toBe(504);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("x-jato-edge-cache")).toBe("BYPASS_TIMEOUT");
    expect(response.headers.get("x-jato-edge-cache-endpoint")).toBe("/v1/auth/me");
    expect(await response.json()).toMatchObject({
      detail: "Origin request timed out after 5ms.",
      error: "origin_timeout",
      path: "/v1/auth/me",
    });
    expect(runtime.fetch).toHaveBeenCalledTimes(1);
    expect(runtime.cache.match).not.toHaveBeenCalled();
    expect(runtime.cache.put).not.toHaveBeenCalled();
  });

  it("bypasses auth profile, permission management, and write methods", async () => {
    const runtime = createRuntime();

    const authProfile = await callEdgeFunction(runtime, "auth/me", {
      headers: { "x-auth-token": "token-a" },
      method: "GET",
    });
    expect(authProfile.headers.get("x-jato-edge-cache")).toBe("BYPASS");

    const permissionWrite = await callEdgeFunction(runtime, "auth/users/alice/profile", {
      body: JSON.stringify({ role: "admin" }),
      headers: {
        "content-type": "application/json",
        "x-auth-token": "token-a",
        "x-user-role": "admin",
      },
      method: "PATCH",
    });
    expect(permissionWrite.headers.get("x-jato-edge-cache")).toBe("BYPASS");

    const dashboardWrite = await callEdgeFunction(runtime, "filters/options/batch", {
      body: JSON.stringify({ items: [] }),
      headers: {
        "content-type": "application/json",
        "x-auth-token": "token-a",
      },
      method: "PUT",
    });
    expect(dashboardWrite.headers.get("x-jato-edge-cache")).toBe("BYPASS");
    expect(runtime.fetch).toHaveBeenCalledTimes(3);
    expect(runtime.cache.match).not.toHaveBeenCalled();
    expect(runtime.cache.put).not.toHaveBeenCalled();
    expect(runtime.originCalls.map((call) => call.url)).toEqual([
      "https://origin.example/v1/auth/me",
      "https://origin.example/v1/auth/users/alice/profile",
      "https://origin.example/v1/filters/options/batch",
    ]);
  });

  it("does not cache failed origin responses", async () => {
    const runtime = createRuntime(() => new Response("upstream unavailable", { status: 503 }));
    const response = await callEdgeFunction(runtime, "filters/options/batch", {
      body: JSON.stringify({ dimensions: ["国家"] }),
      headers: {
        "content-type": "application/json",
        "x-auth-token": "token-a",
        "x-user-name": "alice",
        "x-user-role": "admin",
      },
      method: "POST",
    });

    expect(response.status).toBe(503);
    expect(response.headers.get("x-jato-edge-cache")).toBe("BYPASS");
    expect(await response.text()).toBe("upstream unavailable");
    expect(runtime.fetch).toHaveBeenCalledTimes(1);
    expect(runtime.cache.match).toHaveBeenCalledTimes(2);
    expect(runtime.cache.put).not.toHaveBeenCalled();
  });
});
