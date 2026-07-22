const DEFAULT_ORIGIN = "https://www.ojeur.cloud";
const DEFAULT_TIMEOUT_MS = 12_000;

function jsonResponse(payload, status, proxyState) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "cache-control": "no-store",
      "content-type": "application/json; charset=utf-8",
      "x-jato-edge-proxy": proxyState,
    },
  });
}

function resolveOrigin(env) {
  return String(env.API_ORIGIN || DEFAULT_ORIGIN).replace(/\/+$/, "");
}

function timeoutMs(env) {
  const parsed = Number(env.API_ORIGIN_TIMEOUT_MS || "");
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_TIMEOUT_MS;
}

function isAbortError(error) {
  return Boolean(
    error
      && typeof error === "object"
      && (error.name === "AbortError" || error.name === "TimeoutError"),
  );
}

export async function onRequest({ request, env }) {
  const method = request.method.toUpperCase();
  if (method !== "GET" && method !== "HEAD") {
    const response = jsonResponse({ error: "method_not_allowed" }, 405, "REJECTED");
    response.headers.set("allow", "GET, HEAD");
    return response;
  }

  const controller = new AbortController();
  const requestTimeoutMs = timeoutMs(env);
  const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    const response = await fetch(new URL("/healthz", resolveOrigin(env)), {
      method,
      headers: { accept: "application/json" },
      signal: controller.signal,
    });
    const headers = new Headers(response.headers);
    headers.delete("set-cookie");
    headers.set("cache-control", "no-store");
    headers.set("x-jato-edge-proxy", "healthz");
    return new Response(method === "HEAD" ? null : response.body, {
      status: response.status,
      statusText: response.statusText,
      headers,
    });
  } catch (error) {
    const timedOut = isAbortError(error);
    return jsonResponse(
      {
        detail: timedOut
          ? `Origin request timed out after ${requestTimeoutMs}ms.`
          : "Origin request failed before returning a response.",
        error: timedOut ? "origin_timeout" : "origin_fetch_failed",
        path: "/healthz",
      },
      timedOut ? 504 : 502,
      timedOut ? "TIMEOUT" : "ERROR",
    );
  } finally {
    clearTimeout(timeout);
  }
}
