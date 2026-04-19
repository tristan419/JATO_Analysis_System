export interface SlideLayoutSettings {
  paddingX: number;
  paddingY: number;
  frameGap: number;
  headGap: number;
  bodyGap: number;
  contentGap: number;
}

type SlideLayoutKey = keyof SlideLayoutSettings;
const SLIDE_LAYOUT_STORAGE_PREFIX = "jato:slide-layout:v1:";

export const DEFAULT_SLIDE_LAYOUT: SlideLayoutSettings = {
  paddingX: 28,
  paddingY: 24,
  frameGap: 12,
  headGap: 14,
  bodyGap: 12,
  contentGap: 12,
};

export const SLIDE_LAYOUT_LIMITS: Record<SlideLayoutKey, { min: number; max: number }> = {
  paddingX: { min: 16, max: 48 },
  paddingY: { min: 12, max: 40 },
  frameGap: { min: 6, max: 24 },
  headGap: { min: 8, max: 28 },
  bodyGap: { min: 8, max: 28 },
  contentGap: { min: 8, max: 24 },
};

function normalizeValue(key: SlideLayoutKey, value: number | undefined): number {
  const fallback = DEFAULT_SLIDE_LAYOUT[key];
  const numericValue = typeof value === "number" && Number.isFinite(value) ? value : fallback;
  const { min, max } = SLIDE_LAYOUT_LIMITS[key];
  return Math.min(max, Math.max(min, Math.round(numericValue)));
}

export function normalizeSlideLayout(layout: Partial<SlideLayoutSettings> | undefined): SlideLayoutSettings {
  return {
    paddingX: normalizeValue("paddingX", layout?.paddingX),
    paddingY: normalizeValue("paddingY", layout?.paddingY),
    frameGap: normalizeValue("frameGap", layout?.frameGap),
    headGap: normalizeValue("headGap", layout?.headGap),
    bodyGap: normalizeValue("bodyGap", layout?.bodyGap),
    contentGap: normalizeValue("contentGap", layout?.contentGap),
  };
}

export function updateSlideLayout(
  current: SlideLayoutSettings,
  patch: Partial<SlideLayoutSettings>,
): SlideLayoutSettings {
  return normalizeSlideLayout({
    ...current,
    ...patch,
  });
}

function getStorageKey(key: string): string {
  return `${SLIDE_LAYOUT_STORAGE_PREFIX}${key}`;
}

function canUseLocalStorage(): boolean {
  return typeof window !== "undefined" && typeof window.localStorage !== "undefined";
}

function cloneSlideLayoutMap<T extends string>(defaults: Record<T, SlideLayoutSettings>): Record<T, SlideLayoutSettings> {
  const next = {} as Record<T, SlideLayoutSettings>;
  (Object.keys(defaults) as T[]).forEach((key) => {
    next[key] = normalizeSlideLayout(defaults[key]);
  });
  return next;
}

export function readStoredSlideLayouts<T extends string>(
  key: string,
  defaults: Record<T, SlideLayoutSettings>,
): Record<T, SlideLayoutSettings> {
  const fallback = cloneSlideLayoutMap(defaults);
  if (!canUseLocalStorage()) {
    return fallback;
  }

  try {
    const raw = window.localStorage.getItem(getStorageKey(key));
    if (!raw) {
      return fallback;
    }
    const parsed = JSON.parse(raw) as Record<string, Partial<SlideLayoutSettings> | undefined>;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return fallback;
    }

    const next = cloneSlideLayoutMap(defaults);
    (Object.keys(defaults) as T[]).forEach((pageKey) => {
      next[pageKey] = normalizeSlideLayout(parsed[pageKey]);
    });
    return next;
  } catch {
    return fallback;
  }
}

export function writeStoredSlideLayouts<T extends string>(
  key: string,
  layouts: Record<T, SlideLayoutSettings>,
): void {
  if (!canUseLocalStorage()) {
    return;
  }
  try {
    window.localStorage.setItem(getStorageKey(key), JSON.stringify(layouts));
  } catch {
    // Ignore storage quota / privacy-mode failures; editing still works for the current session.
  }
}
