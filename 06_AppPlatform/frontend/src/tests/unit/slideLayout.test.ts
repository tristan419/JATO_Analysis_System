import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_SLIDE_LAYOUT,
  normalizeSlideLayout,
  readStoredSlideLayouts,
  updateSlideLayout,
  writeStoredSlideLayouts,
} from "../../utils/slideLayout";

describe("normalizeSlideLayout", () => {
  it("fills missing fields with defaults", () => {
    expect(normalizeSlideLayout({ paddingX: 40 })).toEqual({
      ...DEFAULT_SLIDE_LAYOUT,
      paddingX: 40,
    });
  });

  it("clamps values into supported edit ranges", () => {
    expect(normalizeSlideLayout({
      paddingX: 999,
      paddingY: -10,
      frameGap: 99,
      headGap: 0,
      bodyGap: 200,
      contentGap: 2,
    })).toEqual({
      paddingX: 48,
      paddingY: 12,
      frameGap: 24,
      headGap: 8,
      bodyGap: 28,
      contentGap: 8,
    });
  });
});

describe("updateSlideLayout", () => {
  it("merges a patch while keeping the rest of the layout", () => {
    expect(updateSlideLayout(DEFAULT_SLIDE_LAYOUT, { bodyGap: 20, contentGap: 18 })).toEqual({
      ...DEFAULT_SLIDE_LAYOUT,
      bodyGap: 20,
      contentGap: 18,
    });
  });
});

describe("stored slide layouts", () => {
  const storageKey = "market-scan-test";
  const defaults = {
    overview: { ...DEFAULT_SLIDE_LAYOUT },
    segment: { ...DEFAULT_SLIDE_LAYOUT, bodyGap: 14 },
  } as const;
  const storageSlot = "jato:slide-layout:v1:market-scan-test";

  function createStorageMock() {
    const store = new Map<string, string>();
    return {
      getItem(key: string) {
        return store.has(key) ? store.get(key) ?? null : null;
      },
      setItem(key: string, value: string) {
        store.set(key, value);
      },
      removeItem(key: string) {
        store.delete(key);
      },
      clear() {
        store.clear();
      },
    };
  }

  beforeEach(() => {
    const localStorage = createStorageMock();
    vi.stubGlobal("window", { localStorage });
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("reads defaults when storage is empty or invalid", () => {
    window.localStorage.removeItem(storageSlot);
    expect(readStoredSlideLayouts(storageKey, defaults)).toEqual(defaults);

    window.localStorage.setItem(storageSlot, "{bad json");
    expect(readStoredSlideLayouts(storageKey, defaults)).toEqual(defaults);
  });

  it("persists and normalizes stored layouts", () => {
    writeStoredSlideLayouts(storageKey, {
      overview: { ...DEFAULT_SLIDE_LAYOUT, paddingX: 40, bodyGap: 16 },
      segment: { ...DEFAULT_SLIDE_LAYOUT, paddingY: 999, contentGap: 4 },
    });

    expect(readStoredSlideLayouts(storageKey, defaults)).toEqual({
      overview: { ...DEFAULT_SLIDE_LAYOUT, paddingX: 40, bodyGap: 16 },
      segment: { ...DEFAULT_SLIDE_LAYOUT, paddingY: 40, contentGap: 8 },
    });
  });
});
