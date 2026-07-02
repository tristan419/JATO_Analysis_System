// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { request } from "../../api/core";

function createDeferredResponse(): {
  promise: Promise<Response>;
  resolve: (response: Response) => void;
} {
  let resolveResponse: (response: Response) => void = () => {};
  const promise = new Promise<Response>((resolve) => {
    resolveResponse = resolve;
  });
  return {
    promise,
    resolve: resolveResponse,
  };
}

describe("api core request dedupe", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("dedupes identical non-abortable requests", async () => {
    const deferred = createDeferredResponse();
    const fetchMock = vi.fn(() => deferred.promise);
    vi.stubGlobal("fetch", fetchMock);

    const first = request<{ ok: boolean }>("/metadata/columns");
    const second = request<{ ok: boolean }>("/metadata/columns");

    expect(fetchMock).toHaveBeenCalledTimes(1);
    deferred.resolve(Response.json({ ok: true }));
    await expect(Promise.all([first, second])).resolves.toEqual([
      { ok: true },
      { ok: true },
    ]);
  });

  it("does not dedupe requests with AbortSignal", async () => {
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);

    const firstController = new AbortController();
    const secondController = new AbortController();

    await Promise.all([
      request<{ ok: boolean }>("/analysis/advanced-chart", {
        method: "POST",
        body: JSON.stringify({ chart: "a" }),
        signal: firstController.signal,
      }),
      request<{ ok: boolean }>("/analysis/advanced-chart", {
        method: "POST",
        body: JSON.stringify({ chart: "a" }),
        signal: secondController.signal,
      }),
    ]);

    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
});
