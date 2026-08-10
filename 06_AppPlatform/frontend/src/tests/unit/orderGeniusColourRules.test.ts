import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import {
  buildOrderGeniusColourSwatch,
  MISSING_COLOUR_SWATCH_HEX,
  parseOrderGeniusColourSwatch,
} from "../../utils/orderGeniusColourSwatch";

beforeEach(() => {
  vi.stubGlobal("localStorage", {
    getItem: () => null,
    setItem: () => undefined,
    removeItem: () => undefined,
    clear: () => undefined,
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Order Genius colour swatches", () => {
  it("uses the database single or dual value without guessing from the name", () => {
    expect(parseOrderGeniusColourSwatch("#B6D3FB")).toMatchObject({
      colours: ["#B6D3FB"], isDual: false, isMissing: false,
    });
    expect(parseOrderGeniusColourSwatch("#1A1A1A|#C8C0B8")).toMatchObject({
      colours: ["#1A1A1A", "#C8C0B8"], isDual: true, isMissing: false,
    });
  });

  it("treats an absent or partly invalid database value as missing", () => {
    expect(parseOrderGeniusColourSwatch(null)).toMatchObject({
      background: MISSING_COLOUR_SWATCH_HEX, isMissing: true,
    });
    expect(parseOrderGeniusColourSwatch("#FF0000|bad").isMissing).toBe(true);
    expect(parseOrderGeniusColourSwatch("#FF0000|#00FF00|#0000FF").isMissing).toBe(true);
  });

  it("builds normalized single and dual payloads", () => {
    expect(buildOrderGeniusColourSwatch("#aabbcc", "", false)).toBe("#AABBCC");
    expect(buildOrderGeniusColourSwatch("#aabbcc", "#112233", true)).toBe("#AABBCC|#112233");
  });
});

describe("Order Genius colour rule API", () => {
  it("passes the immutable preview fingerprint into Apply", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => Response.json({
      updated: 1, unchanged: 0, conflicts: 0, missingRules: 0,
      materialCodes: ["T7000NHW3MY0002"], items: [], fingerprint: "abc123",
    }));
    vi.stubGlobal("fetch", fetchMock);

    await api.applyOrderGeniusColourHexRuleFills("abc123", ["T7000NHW3MY0002"]);

    const init = fetchMock.mock.calls[0]?.[1];
    expect(init?.method).toBe("POST");
    expect(JSON.parse(String(init?.body))).toEqual({
      previewFingerprint: "abc123",
      materialCodes: ["T7000NHW3MY0002"],
    });
  });

  it("preserves status 409 so the page can ask for a new preview", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(
      { detail: "colour rule preview is stale" },
      { status: 409 },
    )));

    const error = await api.applyOrderGeniusColourHexRuleFills("old", ["T1"]).catch((cause: unknown) => cause);
    expect((error as { status: number }).status).toBe(409);
    expect((error as Error).message).toContain("preview is stale");
  });
});
