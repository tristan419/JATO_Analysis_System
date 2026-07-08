import { afterEach, describe, expect, it, vi } from "vitest";

import {
  fetchAuthEndpoint,
  shouldTryDomesticAuthFallback,
} from "../../utils/authFallback";

type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

describe("auth fallback", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("only enables domestic auth fallback from the intl host", () => {
    expect(shouldTryDomesticAuthFallback("intl.ojeur.cloud")).toBe(true);
    expect(shouldTryDomesticAuthFallback("www.ojeur.cloud")).toBe(false);
    expect(shouldTryDomesticAuthFallback("localhost")).toBe(false);
  });

  it("uses the primary auth endpoint when it succeeds", async () => {
    const fetchMock = vi.fn<FetchLike>(async () => Response.json({ token: "primary" }));
    vi.stubGlobal("fetch", fetchMock);

    const response = await fetchAuthEndpoint("/auth/login", {
      body: JSON.stringify({ username: "test" }),
      method: "POST",
    }, {
      hostname: "intl.ojeur.cloud",
    });

    expect(await response.json()).toMatchObject({ token: "primary" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/v1/auth/login");
  });

  it("tries www directly when intl auth returns an origin timeout", async () => {
    const fetchMock = vi.fn<FetchLike>(async (input) => {
      if (String(input) === "/v1/auth/login") {
        return Response.json(
          { error: "origin_timeout" },
          {
            headers: { "x-jato-edge-cache": "BYPASS_TIMEOUT" },
            status: 504,
          },
        );
      }
      return Response.json({ token: "fallback" });
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await fetchAuthEndpoint("/auth/login", {
      body: JSON.stringify({ username: "test" }),
      method: "POST",
    }, {
      fallbackTimeoutMs: 50,
      hostname: "intl.ojeur.cloud",
    });

    expect(await response.json()).toMatchObject({ token: "fallback" });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/v1/auth/login");
    expect(fetchMock.mock.calls[1]?.[0]).toBe("https://www.ojeur.cloud/v1/auth/login");
  });

  it("does not fallback from www when auth returns an origin timeout", async () => {
    const fetchMock = vi.fn<FetchLike>(async () => Response.json(
      { error: "origin_timeout" },
      {
        headers: { "x-jato-edge-cache": "BYPASS_TIMEOUT" },
        status: 504,
      },
    ));
    vi.stubGlobal("fetch", fetchMock);

    const response = await fetchAuthEndpoint("/auth/me", undefined, {
      hostname: "www.ojeur.cloud",
    });

    expect(response.status).toBe(504);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/v1/auth/me");
  });

  it("returns the primary timeout response if the direct www fallback fails", async () => {
    const primary = Response.json(
      { error: "origin_timeout" },
      {
        headers: { "x-jato-edge-cache": "BYPASS_TIMEOUT" },
        status: 504,
      },
    );
    const fetchMock = vi.fn<FetchLike>(async (input) => {
      if (String(input) === "/v1/auth/me") return primary;
      throw new Error("direct www failed");
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await fetchAuthEndpoint("/auth/me", undefined, {
      fallbackTimeoutMs: 50,
      hostname: "intl.ojeur.cloud",
    });

    expect(response).toBe(primary);
    expect(response.status).toBe(504);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("does not fallback when the primary auth request is aborted", async () => {
    const abortError = Object.assign(new Error("aborted"), { name: "AbortError" });
    const fetchMock = vi.fn<FetchLike>(async () => {
      throw abortError;
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchAuthEndpoint("/auth/me", undefined, {
      hostname: "intl.ojeur.cloud",
    })).rejects.toBe(abortError);

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
