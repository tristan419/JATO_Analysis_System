const CACHE_PREFIX = "jato:page-cache:v1:";

interface CacheEntry<T> {
  savedAt: number;
  expiresAt: number;
  value: T;
}

const memoryCache = new Map<string, CacheEntry<unknown>>();

export function getCachedPageValue<T>(key: string): T | null {
  const fullKey = CACHE_PREFIX + key;

  const memEntry = memoryCache.get(fullKey) as CacheEntry<T> | undefined;
  if (memEntry) {
    if (Date.now() > memEntry.expiresAt) {
      memoryCache.delete(fullKey);
      try { sessionStorage.removeItem(fullKey); } catch { /* ignore */ }
      return null;
    }
    return memEntry.value;
  }

  try {
    const raw = sessionStorage.getItem(fullKey);
    if (!raw) return null;
    const entry = JSON.parse(raw) as CacheEntry<T>;
    if (Date.now() > entry.expiresAt) {
      sessionStorage.removeItem(fullKey);
      return null;
    }
    memoryCache.set(fullKey, entry as CacheEntry<unknown>);
    return entry.value;
  } catch {
    return null;
  }
}

export function setCachedPageValue<T>(key: string, value: T, ttlMs: number): void {
  const fullKey = CACHE_PREFIX + key;
  const now = Date.now();
  const entry: CacheEntry<T> = { savedAt: now, expiresAt: now + ttlMs, value };
  memoryCache.set(fullKey, entry as CacheEntry<unknown>);
  try { sessionStorage.setItem(fullKey, JSON.stringify(entry)); } catch { /* ignore */ }
}

export function clearCachedPageValue(key: string): void {
  const fullKey = CACHE_PREFIX + key;
  memoryCache.delete(fullKey);
  try { sessionStorage.removeItem(fullKey); } catch { /* ignore */ }
}
