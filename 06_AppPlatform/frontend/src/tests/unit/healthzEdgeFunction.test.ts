// @vitest-environment node

import { afterEach, describe, expect, it, vi } from "vitest";

interface HealthzFunctionContext {
  request: Request;
  env: Record<string, string>;
}

interface HealthzFunctionModule {
  onRequest: (context: HealthzFunctionContext) => Promise<Response>;
}

let healthzFunctionModulePromise: Promise<HealthzFunctionModule> | null = null;

async function loadHealthzFunction(): Promise<HealthzFunctionModule> {
  healthzFunctionModulePromise ??= import(
    new URL("../../../functions/healthz.js", import.meta.url).href
  ) as Promise<HealthzFunctionModule>;
  return healthzFunctionModulePromise;
}

async function callHealthz(
  method = "GET",
  env: Record<string, string> = {},
): Promise<Response> {
  const { onRequest } = await loadHealthzFunction();
  return onRequest({
    request: new Request("https://intl.ojeur.cloud/healthz", { method }),
    env: { API_ORIGIN: "https://origin.example", ...env },
  });
}

describe("Cloudflare healthz proxy function", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("proxies health to the API origin without forwarding user credentials", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(new Headers(init?.headers).get("accept")).toBe("application/json");
      expect(new Headers(init?.headers).get("cookie")).toBeNull();
      return Response.json(
        { status: "ok" },
        { headers: { "set-cookie": "session=origin" } },
      );
    });
    vi.stubGlobal("fetch", fetch);

    const response = await callHealthz();

    expect(fetch).toHaveBeenCalledWith(
      new URL("https://origin.example/healthz"),
      expect.objectContaining({ method: "GET" }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("set-cookie")).toBeNull();
    expect(response.headers.get("x-jato-edge-proxy")).toBe("healthz");
    expect(await response.json()).toEqual({ status: "ok" });
  });

  it("rejects write methods without contacting the origin", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);

    const response = await callHealthz("POST");

    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET, HEAD");
    expect(response.headers.get("x-jato-edge-proxy")).toBe("REJECTED");
    expect(await response.json()).toEqual({ error: "method_not_allowed" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("proxies HEAD without returning an origin body", async () => {
    const fetch = vi.fn(async () => Response.json({ status: "ok" }));
    vi.stubGlobal("fetch", fetch);

    const response = await callHealthz("HEAD");

    expect(fetch).toHaveBeenCalledWith(
      new URL("https://origin.example/healthz"),
      expect.objectContaining({ method: "HEAD" }),
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("x-jato-edge-proxy")).toBe("healthz");
    expect(await response.text()).toBe("");
  });

  it("returns fail-closed JSON when the API origin times out", async () => {
    const timeoutError = Object.assign(new Error("timed out"), { name: "AbortError" });
    vi.stubGlobal("fetch", vi.fn(async () => {
      throw timeoutError;
    }));

    const response = await callHealthz("GET", { API_ORIGIN_TIMEOUT_MS: "7" });

    expect(response.status).toBe(504);
    expect(response.headers.get("content-type")).toContain("application/json");
    expect(response.headers.get("x-jato-edge-proxy")).toBe("TIMEOUT");
    expect(await response.json()).toEqual({
      detail: "Origin request timed out after 7ms.",
      error: "origin_timeout",
      path: "/healthz",
    });
  });

  it("returns an explicit 502 when the API origin cannot be reached", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => {
      throw new TypeError("network unavailable");
    }));

    const response = await callHealthz();

    expect(response.status).toBe(502);
    expect(response.headers.get("cache-control")).toBe("no-store");
    expect(response.headers.get("x-jato-edge-proxy")).toBe("ERROR");
    expect(await response.json()).toEqual({
      detail: "Origin request failed before returning a response.",
      error: "origin_fetch_failed",
      path: "/healthz",
    });
  });
});
