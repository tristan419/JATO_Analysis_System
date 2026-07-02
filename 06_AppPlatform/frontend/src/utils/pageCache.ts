const CACHE_PREFIX = "jato:page-cache:v2:";
const MEMORY_CACHE_MAX_ENTRIES = 64;

interface CacheEnvelope<T> {
  savedAt: number;
  expiresAt: number;
  value: T;
}

const memoryCache = new Map<string, CacheEnvelope<unknown>>();

function trimMemoryCache(): void {
  while (memoryCache.size > MEMORY_CACHE_MAX_ENTRIES) {
    const oldest = memoryCache.keys().next();
    if (oldest.done) return;
    memoryCache.delete(oldest.value);
  }
}

function setMemoryEntry<T>(key: string, entry: CacheEnvelope<T>): void {
  memoryCache.delete(key);
  memoryCache.set(key, entry as CacheEnvelope<unknown>);
  trimMemoryCache();
}

function getMemoryEntry<T>(key: string): CacheEnvelope<T> | undefined {
  const entry = memoryCache.get(key) as CacheEnvelope<T> | undefined;
  if (!entry) return undefined;
  memoryCache.delete(key);
  memoryCache.set(key, entry as CacheEnvelope<unknown>);
  return entry;
}

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
  const memoryEntry = getMemoryEntry<T>(key);
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

  setMemoryEntry(key, sessionEntry);
  return sessionEntry.value;
}

export function setCachedPageValue<T>(key: string, value: T, ttlMs: number): void {
  const now = Date.now();
  const entry: CacheEnvelope<T> = {
    savedAt: now,
    expiresAt: now + ttlMs,
    value,
  };

  setMemoryEntry(key, entry);
  writeToSessionStorage(key, entry);
}

export function clearCachedPageValue(key: string): void {
  memoryCache.delete(key);
  removeFromSessionStorage(key);
}
