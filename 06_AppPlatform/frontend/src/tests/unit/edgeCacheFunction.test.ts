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
    expect(first.headers.get("vary")).toBe("X-User-Name, X-User-Role, X-JATO-Data-Version");
    expect(await first.json()).toMatchObject({ sequence: 1 });

    await flushWaitUntil(runtime);

    const second = await callEdgeFunction(runtime, "analysis/overview?chart=summary", requestInit);
    expect(second.headers.get("x-jato-edge-cache")).toBe("HIT");
    expect(await second.json()).toMatchObject({ sequence: 1 });
    expect(runtime.fetch).toHaveBeenCalledTimes(1);
    expect(runtime.cache.match).toHaveBeenCalledTimes(2);
    expect(runtime.cache.put).toHaveBeenCalledTimes(1);
    const cachedResponse = runtime.cache.put.mock.calls[0]?.[1] as Response | undefined;
    expect(cachedResponse?.headers.get("cache-control")).toBe("public, max-age=300");
    expect(runtime.originCalls[0]?.url).toBe("https://origin.example/v1/analysis/overview?chart=summary");
    expect(runtime.originCalls[0]?.headers.get("cf-ray")).toBeNull();
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

    const otherUser = await callEdgeFunction(
      runtime,
      "analysis/time-series-grouped",
      scopedRequest("token-c", "dataset-a", "other-user"),
    );
    expect(otherUser.headers.get("x-jato-edge-cache")).toBe("MISS");
    await flushWaitUntil(runtime);

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
    expect(runtime.fetch).toHaveBeenCalledTimes(4);
    expect(runtime.cache.put).toHaveBeenCalledTimes(4);
  });

  it("synthesizes and caches filter snapshots when the origin endpoint is missing", async () => {
    const runtime = createRuntime((_sequence, call) => {
      if (call.url === "https://origin.example/v1/metadata/filter-snapshot") {
        return new Response("not found", { status: 404 });
      }
      if (call.url === "https://origin.example/v1/metadata/columns") {
        return Response.json({
          items: ["国家", "Body type", "细分市场", "动总规整", "Make", "Model"],
        });
      }
      if (call.url === "https://origin.example/v1/filters/options/batch") {
        return Response.json({
          items: [
            { column: "国家", options: ["丹麦", "德国"] },
            { column: "Body type", options: ["SUV"] },
            { column: "细分市场", options: ["C"] },
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
      columns: ["国家", "Body type", "细分市场", "动总规整", "Make", "Model"],
      options: {
        国家: ["丹麦", "德国"],
        "Body type": ["SUV"],
        细分市场: ["C"],
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
    expect(runtime.cache.put).toHaveBeenCalledTimes(1);
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
    expect(runtime.cache.match).toHaveBeenCalledTimes(1);
    expect(runtime.cache.put).not.toHaveBeenCalled();
  });
});
