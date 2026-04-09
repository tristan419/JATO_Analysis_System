const CACHE_PREFIX = "jato:page-cache:v1:";

interface CacheEnvelope<T> {
  savedAt: number;
  expiresAt: number;
  value: T;
}

const memoryCache = new Map<string, CacheEnvelope<unknown>>();

function getStorageKey(key: string): string {
  return `${CACHE_PREFIX}${key}`;
}

function canUseSessionStorage(): boolean {
  return typeof window !== "undefined" && typeof window.sessionStorage !== "undefined";
}

function isExpired(entry: CacheEnvelope<unknown>): boolean {
  return entry.expiresAt <= Date.now();
}

function readFromSessionStorage<T>(key: string): CacheEnvelope<T> | null {
  if (!canUseSessionStorage()) return null;
  try {
    const raw = window.sessionStorage.getItem(getStorageKey(key));
    if (!raw) return null;
    const parsed = JSON.parse(raw) as CacheEnvelope<T>;
    if (!parsed || typeof parsed !== "object") return null;
    if (typeof parsed.savedAt !== "number" || typeof parsed.expiresAt !== "number") return null;
    return parsed;
  } catch {
    return null;
  }
}

function writeToSessionStorage<T>(key: string, entry: CacheEnvelope<T>): void {
  if (!canUseSessionStorage()) return;
  try {
    window.sessionStorage.setItem(getStorageKey(key), JSON.stringify(entry));
  } catch {
    // Ignore storage quota failures and keep memory cache only.
  }
}

function removeFromSessionStorage(key: string): void {
  if (!canUseSessionStorage()) return;
  try {
    window.sessionStorage.removeItem(getStorageKey(key));
  } catch {
    // Ignore storage cleanup failures.
  }
}

export function getCachedPageValue<T>(key: string): T | null {
  const memoryEntry = memoryCache.get(key) as CacheEnvelope<T> | undefined;
  if (memoryEntry) {
    if (isExpired(memoryEntry)) {
      memoryCache.delete(key);
      removeFromSessionStorage(key);
    } else {
      return memoryEntry.value;
    }
  }

  const sessionEntry = readFromSessionStorage<T>(key);
  if (!sessionEntry) return null;
  if (isExpired(sessionEntry)) {
    removeFromSessionStorage(key);
    return null;
  }

  memoryCache.set(key, sessionEntry as CacheEnvelope<unknown>);
  return sessionEntry.value;
}

export function setCachedPageValue<T>(key: string, value: T, ttlMs: number): void {
  const now = Date.now();
  const entry: CacheEnvelope<T> = {
    savedAt: now,
    expiresAt: now + ttlMs,
    value,
  };

  memoryCache.set(key, entry as CacheEnvelope<unknown>);
  writeToSessionStorage(key, entry);
}

export function clearCachedPageValue(key: string): void {
  memoryCache.delete(key);
  removeFromSessionStorage(key);
}