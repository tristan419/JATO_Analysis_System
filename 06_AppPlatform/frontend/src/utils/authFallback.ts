import { apiUrl } from "../api/core";

export const AUTH_DOMESTIC_FALLBACK_TIMEOUT_MS = 8_000;
const DOMESTIC_AUTH_ORIGIN = "https://www.ojeur.cloud";

interface AuthFallbackOptions {
  fallbackOrigin?: string;
  fallbackTimeoutMs?: number;
  hostname?: string;
}

function currentHostname(): string {
  return typeof window === "undefined" ? "" : window.location.hostname;
}

export function shouldTryDomesticAuthFallback(hostname = currentHostname()): boolean {
  return hostname === "intl.ojeur.cloud";
}

function normalizeApiPath(path: string): string {
  return path.startsWith("/") ? path : `/${path}`;
}

function domesticAuthUrl(path: string, origin: string): string {
  return `${origin.replace(/\/+$/, "")}/v1${normalizeApiPath(path)}`;
}

async function isOriginTimeoutResponse(response: Response): Promise<boolean> {
  if (response.status !== 504) return false;
  if (response.headers.get("x-jato-edge-cache") === "BYPASS_TIMEOUT") return true;
  try {
    const payload = await response.clone().json() as { error?: unknown };
    return payload.error === "origin_timeout";
  } catch {
    return false;
  }
}

function isAbortLikeError(error: unknown): boolean {
  if (error instanceof DOMException) return error.name === "AbortError";
  if (error instanceof Error) return error.name === "AbortError";
  return Boolean(
    error
      && typeof error === "object"
      && "name" in error
      && error.name === "AbortError",
  );
}

function createTimeoutSignal(
  timeoutMs: number,
  parentSignal?: AbortSignal | null,
): { cleanup: () => void; signal: AbortSignal } {
  const controller = new AbortController();
  const abort = () => controller.abort();
  if (parentSignal?.aborted) {
    abort();
  } else {
    parentSignal?.addEventListener("abort", abort, { once: true });
  }
  const timeout = setTimeout(abort, timeoutMs);
  return {
    cleanup: () => {
      clearTimeout(timeout);
      parentSignal?.removeEventListener("abort", abort);
    },
    signal: controller.signal,
  };
}

async function fetchWithTimeout(
  input: string,
  init: RequestInit | undefined,
  timeoutMs: number,
): Promise<Response> {
  const timeoutSignal = createTimeoutSignal(timeoutMs, init?.signal);
  try {
    return await fetch(input, {
      ...init,
      signal: timeoutSignal.signal,
    });
  } finally {
    timeoutSignal.cleanup();
  }
}

export async function fetchAuthEndpoint(
  path: string,
  init?: RequestInit,
  options: AuthFallbackOptions = {},
): Promise<Response> {
  const hostname = options.hostname ?? currentHostname();
  const allowFallback = shouldTryDomesticAuthFallback(hostname);
  let primaryResponse: Response | null = null;
  let primaryError: unknown = null;

  try {
    primaryResponse = await fetch(apiUrl(path), init);
  } catch (error) {
    primaryError = error;
    if (!allowFallback || isAbortLikeError(error)) throw error;
  }

  const shouldFallback = allowFallback && (
    primaryResponse === null
      || await isOriginTimeoutResponse(primaryResponse)
  );
  if (!shouldFallback) {
    if (primaryResponse) return primaryResponse;
    throw primaryError;
  }

  try {
    return await fetchWithTimeout(
      domesticAuthUrl(path, options.fallbackOrigin ?? DOMESTIC_AUTH_ORIGIN),
      init,
      options.fallbackTimeoutMs ?? AUTH_DOMESTIC_FALLBACK_TIMEOUT_MS,
    );
  } catch {
    if (primaryResponse) return primaryResponse;
    throw primaryError;
  }
}
