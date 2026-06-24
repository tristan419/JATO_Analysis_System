export const API_BASE = import.meta.env.VITE_API_BASE ?? "/v1";

export function apiUrl(path: string): string {
  const normalizedBase = API_BASE.endsWith("/") ? API_BASE.slice(0, -1) : API_BASE;
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${normalizedBase}${normalizedPath}`;
}

function getAuthHeaders(): Record<string, string> {
  const token = (
    localStorage.getItem("jato_auth_token")
    || import.meta.env.VITE_AUTH_TOKEN
    || ""
  ).trim();
  const user = (
    localStorage.getItem("jato_user_name")
    || import.meta.env.VITE_USER_NAME
    || "anonymous"
  ).trim();

  return {
    ...(token ? { "X-Auth-Token": token } : {}),
    "X-User-Name": user || "anonymous",
  };
}

function buildHeaders(
  init?: RequestInit,
  options?: { includeJsonContentType?: boolean },
): Headers {
  const headers = new Headers(init?.headers);
  Object.entries(getAuthHeaders()).forEach(([key, value]) => {
    headers.set(key, value);
  });
  if (
    options?.includeJsonContentType
    && !(init?.body instanceof FormData)
    && !headers.has("Content-Type")
  ) {
    headers.set("Content-Type", "application/json");
  }
  return headers;
}

async function readErrorMessage(response: Response): Promise<string> {
  const text = (await response.text()).trim();
  if (!text) return response.statusText || "Request failed";

  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    if (typeof parsed.detail === "string" && parsed.detail.trim()) return parsed.detail;
    if (typeof parsed.detail === "object" && parsed.detail !== null) return JSON.stringify(parsed.detail);
    if (typeof parsed.message === "string" && parsed.message.trim()) return parsed.message;
  } catch {
    // Raw response text is the best error when the origin does not return JSON.
  }

  return text;
}

const inflightRequests = new Map<string, Promise<unknown>>();

function dedupeKey(path: string, init?: RequestInit): string {
  const method = (init?.method ?? "GET").toUpperCase();
  const body = init?.body ? String(init.body) : "";
  return `${method}:${path}:${body}`;
}

function isAbortLikeError(error: unknown): boolean {
  if (error instanceof DOMException) return error.name === "AbortError";
  if (error instanceof Error) {
    if (error.name === "AbortError") return true;
    return /\babort(?:ed)?\b/i.test(error.message);
  }
  if (typeof error === "object" && error !== null) {
    const name = "name" in error ? String(error.name ?? "") : "";
    const message = "message" in error ? String(error.message ?? "") : "";
    return name === "AbortError" || /\babort(?:ed)?\b/i.test(message);
  }
  return false;
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const shouldDedupe = !(init?.body instanceof FormData);
  const key = shouldDedupe ? dedupeKey(path, init) : null;
  const inflight = key
    ? inflightRequests.get(key) as Promise<T> | undefined
    : undefined;
  if (inflight) return inflight;

  const promise = (async () => {
    let response: Response;
    try {
      response = await fetch(apiUrl(path), {
        headers: buildHeaders(init, { includeJsonContentType: true }),
        ...init,
      });
    } catch (error) {
      if (isAbortLikeError(error)) throw error;
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`网络请求失败：${path} (${message})`);
    }
    if (!response.ok) {
      const message = await readErrorMessage(response);
      throw new Error(`${response.status} ${message}`);
    }
    return await response.json() as T;
  })();

  if (key) {
    inflightRequests.set(key, promise);
    promise.then(
      () => inflightRequests.delete(key),
      () => inflightRequests.delete(key),
    );
  }

  return promise;
}
