import { afterEach, describe, expect, it, vi } from "vitest";

import {
  clearCachedPageValue,
  getCachedPageValue,
  setCachedPageValue,
} from "../../utils/pageCache";

class SessionStorageMock {
  private readonly values = new Map<string, string>();

  get length(): number {
    return this.values.size;
  }

  clear(): void {
    this.values.clear();
  }

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  key(index: number): string | null {
    return [...this.values.keys()][index] ?? null;
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

function installSessionStorage(): SessionStorageMock {
  const sessionStorage = new SessionStorageMock();
  vi.stubGlobal("window", { sessionStorage });
  return sessionStorage;
}

function storageKey(key: string): string {
  return `jato:page-cache:v2:${key}`;
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("pageCache", () => {
  it("stores and restores BOM Admin filter state", () => {
    const sessionStorage = installSessionStorage();
    const key = "order-genius:bom-admin";
    const value = {
      searchText: "atto2",
      toolsFlipped: true,
      showAddMaterial: false,
      expandedGroups: ["OMODA|OMODA5 EV|BEV"],
      editingBoms: ["T7000SW**MY0001"],
      bulkFobEditors: {
        "T7000SW**MY0001": {
          deltaEur: "200",
          selectedCountries: ["DK", "NL"],
        },
      },
      copyCountryForm: {
        sourceCountryCode: "CZ",
        targetCountryCode: "DK",
        overwriteExisting: true,
      },
      adjustCountryForm: {
        countryCode: "DK",
        deltaEur: "-300",
      },
    };

    setCachedPageValue(key, value, 60_000);

    expect(getCachedPageValue<typeof value>(key)).toEqual(value);
    const rawEnvelope = sessionStorage.getItem(storageKey(key));
    expect(rawEnvelope).toBeTruthy();
    expect(JSON.parse(rawEnvelope || "{}").value).toEqual(value);

    clearCachedPageValue(key);
  });

  it("keeps memory cache bounded with least-recently-used eviction", () => {
    const sessionStorage = installSessionStorage();

    for (let index = 0; index < 65; index += 1) {
      setCachedPageValue(`lru-test-${index}`, { index }, 60_000);
    }

    sessionStorage.removeItem(storageKey("lru-test-0"));
    sessionStorage.removeItem(storageKey("lru-test-1"));

    expect(getCachedPageValue<{ index: number }>("lru-test-0")).toBeNull();
    expect(getCachedPageValue<{ index: number }>("lru-test-1")).toEqual({ index: 1 });

    setCachedPageValue("lru-test-65", { index: 65 }, 60_000);
    sessionStorage.removeItem(storageKey("lru-test-1"));
    sessionStorage.removeItem(storageKey("lru-test-2"));

    expect(getCachedPageValue<{ index: number }>("lru-test-1")).toEqual({ index: 1 });
    expect(getCachedPageValue<{ index: number }>("lru-test-2")).toBeNull();

    for (let index = 1; index <= 65; index += 1) {
      clearCachedPageValue(`lru-test-${index}`);
    }
  });
});
