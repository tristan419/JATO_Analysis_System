const DEFAULT_ORIGIN = "https://www.ojeur.cloud";
const DEFAULT_BYPASS_TIMEOUT_MS = 12_000;
const CACHEABLE_ENDPOINTS = new Map([
  ["GET metadata/columns", 3600],
  ["GET metadata/filter-snapshot", 3600],
  ["GET assistant/country/metadata", 3600],
  ["GET advanced-analysis/countries", 3600],
  ["GET advanced-analysis/profile-options", 3600],
  ["POST advanced-analysis/transfer-mart", 300],
  ["POST advanced-analysis/competitor-set", 300],
  ["GET analysis/data-freshness", 300],
  ["POST filters/options", 300],
  ["POST filters/options/batch", 300],
  ["POST analysis/overview", 300],
  ["POST analysis/time-series", 300],
  ["POST analysis/time-series-grouped", 300],
]);
const DEFAULT_STALE_TTL_SECONDS = 24 * 60 * 60;
const FILTER_SNAPSHOT_COLUMNS = [
  ["国家", "country"],
  ["Body type", "body_type", "body type"],
  ["细分市场（按车长）", "细分市场-欧", "细分市场", "segment"],
  ["动总规整", "powertrain"],
];

function resolveOrigin(env) {
  return String(env.API_ORIGIN || DEFAULT_ORIGIN).replace(/\/+$/, "");
}

function getPath(params) {
  const rawPath = Array.isArray(params.path) ? params.path.join("/") : String(params.path || "");
  return rawPath.replace(/^\/+/, "");
}

function cacheTtlSeconds(method, path) {
  return CACHEABLE_ENDPOINTS.get(`${method.toUpperCase()} ${path}`) || 0;
}

function staleTtlSeconds(env) {
  const parsed = Number(env.CACHE_STALE_TTL_SECONDS || env.STALE_CACHE_TTL_SECONDS || "");
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_STALE_TTL_SECONDS;
}

function bypassTimeoutMs(env) {
  const parsed = Number(env.API_BYPASS_TIMEOUT_MS || env.API_ORIGIN_TIMEOUT_MS || "");
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_BYPASS_TIMEOUT_MS;
}

async function withTimeout(timeoutMs, fn) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fn(controller.signal);
  } finally {
    clearTimeout(timeout);
  }
}

function isAbortError(error) {
  return Boolean(
    error
      && typeof error === "object"
      && (
        error.name === "AbortError"
        || error.name === "TimeoutError"
      ),
  );
}

async function sha256Hex(value) {
  const data = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function requestScopeHash(request) {
  const userRole = String(request.headers.get("x-user-role") || "viewer").trim().toLowerCase();
  return sha256Hex(`role:${userRole || "viewer"}`);
}

function dataVersion(request, env) {
  return (
    request.headers.get("x-jato-data-version")
    || env.DATA_VERSION
    || env.JATO_DATA_VERSION
    || "default"
  );
}

function cacheRequestUrl(request, path, bodyHash, scopeHash, version, layer = "fresh") {
  const sourceUrl = new URL(request.url);
  const cacheUrl = new URL(`/_jato_edge_cache/v1/${path}`, sourceUrl.origin);
  cacheUrl.search = sourceUrl.search;
  cacheUrl.searchParams.set("__method", request.method.toUpperCase());
  cacheUrl.searchParams.set("__body", bodyHash);
  cacheUrl.searchParams.set("__scope", scopeHash);
  cacheUrl.searchParams.set("__data", version);
  if (layer !== "fresh") {
    cacheUrl.searchParams.set("__layer", layer);
  }
  return cacheUrl.toString();
}

function sanitizeResponseHeaders(response, ttlSeconds, path, cacheState, cacheControl) {
  const headers = new Headers(response.headers);
  headers.delete("set-cookie");
  headers.set("x-jato-edge-cache", cacheState);
  headers.set("x-jato-edge-cache-endpoint", `/v1/${path}`);
  headers.set("cache-control", cacheControl || `public, max-age=0, s-maxage=${ttlSeconds}`);
  headers.set("vary", "X-User-Role, X-JATO-Data-Version");
  return headers;
}

function bypassResponse(response) {
  const headers = new Headers(response.headers);
  headers.set("x-jato-edge-cache", "BYPASS");
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function originHeaders(request) {
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("cf-connecting-ip");
  headers.delete("cf-ipcountry");
  headers.delete("cf-ray");
  headers.delete("x-forwarded-for");
  headers.delete("x-forwarded-proto");
  return headers;
}

async function originFetchPath(request, origin, path, init = {}) {
  const targetUrl = new URL(`/v1/${path}`, origin);
  if (init.search) {
    targetUrl.search = init.search;
  }
  const method = init.method || request.method;
  const headers = originHeaders(request);
  if (init.contentType) {
    headers.set("content-type", init.contentType);
  }
  return fetch(targetUrl.toString(), {
    method,
    headers,
    body: method === "GET" || method === "HEAD" ? undefined : init.body,
    signal: init.signal,
  });
}

async function originFetch(request, origin, path, body) {
  const sourceUrl = new URL(request.url);
  return originFetchPath(request, origin, path, {
    body,
    method: request.method,
    search: sourceUrl.search,
  });
}

function originFailureResponse(path, timeoutMs, error) {
  const timedOut = isAbortError(error);
  const headers = new Headers({
    "cache-control": "no-store",
    "content-type": "application/json; charset=utf-8",
    "x-jato-edge-cache": timedOut ? "BYPASS_TIMEOUT" : "BYPASS_ERROR",
    "x-jato-edge-cache-endpoint": `/v1/${path}`,
  });
  return new Response(JSON.stringify({
    detail: timedOut
      ? `Origin request timed out after ${timeoutMs}ms.`
      : "Origin request failed before returning a response.",
    error: timedOut ? "origin_timeout" : "origin_fetch_failed",
    path: `/v1/${path}`,
  }), {
    status: timedOut ? 504 : 502,
    headers,
  });
}

async function bypassOriginFetch(request, origin, path, body, timeoutMs) {
  return withTimeout(timeoutMs, (signal) => originFetchPath(request, origin, path, {
    body,
    method: request.method,
    search: new URL(request.url).search,
    signal,
  }));
}

function resolveFilterSnapshotColumns(columns) {
  const normalized = new Map(columns.map((column) => [String(column).trim().toLowerCase(), column]));
  return FILTER_SNAPSHOT_COLUMNS
    .map((aliases) => aliases.map((alias) => normalized.get(alias.toLowerCase())).find(Boolean))
    .filter(Boolean);
}

async function synthesizeFilterSnapshot(request, origin) {
  const columnsResponse = await originFetchPath(request, origin, "metadata/columns", {
    method: "GET",
  });
  if (!columnsResponse.ok) {
    return null;
  }
  const columnsPayload = await columnsResponse.json();
  const columns = Array.isArray(columnsPayload.items) ? columnsPayload.items : [];
  const snapshotColumns = resolveFilterSnapshotColumns(columns);
  if (columns.length === 0 || snapshotColumns.length === 0) {
    return null;
  }
  const optionsResponse = await originFetchPath(request, origin, "filters/options/batch", {
    body: JSON.stringify({
      items: snapshotColumns.map((column) => ({
        column,
        filters: {},
      })),
    }),
    contentType: "application/json",
    method: "POST",
  });
  if (!optionsResponse.ok) {
    return null;
  }
  const optionsPayload = await optionsResponse.json();
  const options = {};
  const items = Array.isArray(optionsPayload.items) ? optionsPayload.items : [];
  items.forEach((item) => {
    if (!item || typeof item !== "object") return;
    const column = typeof item.column === "string" ? item.column : "";
    if (!column) return;
    options[column] = Array.isArray(item.options) ? item.options : [];
  });
  return Response.json({
    columns,
    options,
    source: "edge-synthesized",
  });
}

async function fetchCacheableOriginResponse(request, origin, method, path, bodyText) {
  let originResponse = await originFetch(request, origin, path, bodyText);
  if (!originResponse.ok) {
    const shouldSynthesizeSnapshot =
      method === "GET" && path === "metadata/filter-snapshot" && originResponse.status === 404;
    const synthesizedResponse = shouldSynthesizeSnapshot
      ? await synthesizeFilterSnapshot(request, origin)
      : null;
    if (!synthesizedResponse) {
      return originResponse;
    }
    originResponse = synthesizedResponse;
  }
  return originResponse;
}

async function putCacheableResponse(cache, cacheKey, staleCacheKey, response, ttlSeconds, staleSeconds, path) {
  const body = await response.arrayBuffer();
  const freshHeaders = sanitizeResponseHeaders(
    response,
    ttlSeconds,
    path,
    "MISS",
    `public, max-age=${ttlSeconds}`,
  );
  const staleHeaders = sanitizeResponseHeaders(
    response,
    staleSeconds,
    path,
    "STALE",
    `public, max-age=${staleSeconds}`,
  );
  freshHeaders.delete("vary");
  staleHeaders.delete("vary");
  await Promise.all([
    cache.put(cacheKey, new Response(body.slice(0), {
      status: response.status,
      statusText: response.statusText,
      headers: freshHeaders,
    })),
    cache.put(staleCacheKey, new Response(body.slice(0), {
      status: response.status,
      statusText: response.statusText,
      headers: staleHeaders,
    })),
  ]);
}

async function refreshCacheableResponse({
  bodyText,
  cache,
  cacheKey,
  method,
  origin,
  path,
  request,
  staleCacheKey,
  staleSeconds,
  ttlSeconds,
}) {
  const originResponse = await fetchCacheableOriginResponse(request, origin, method, path, bodyText);
  if (!originResponse.ok) return;
  const responseForCache = new Response(originResponse.body, {
    status: originResponse.status,
    statusText: originResponse.statusText,
    headers: sanitizeResponseHeaders(originResponse, ttlSeconds, path, "MISS"),
  });
  await putCacheableResponse(
    cache,
    cacheKey,
    staleCacheKey,
    responseForCache,
    ttlSeconds,
    staleSeconds,
    path,
  );
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const path = getPath(params);
  const method = request.method.toUpperCase();
  const ttlSeconds = cacheTtlSeconds(method, path);
  const origin = resolveOrigin(env);

  if (!ttlSeconds) {
    const body = method === "GET" || method === "HEAD" ? undefined : request.body;
    const timeoutMs = bypassTimeoutMs(env);
    try {
      return bypassResponse(await bypassOriginFetch(request, origin, path, body, timeoutMs));
    } catch (error) {
      return originFailureResponse(path, timeoutMs, error);
    }
  }

  const bodyText = method === "GET" || method === "HEAD" ? "" : await request.text();
  const [bodyHash, scopeHash] = await Promise.all([
    sha256Hex(bodyText),
    requestScopeHash(request),
  ]);
  const cacheKey = new Request(cacheRequestUrl(
    request,
    path,
    bodyHash,
    scopeHash,
    dataVersion(request, env),
  ), {
    method: "GET",
  });
  const staleCacheKey = new Request(cacheRequestUrl(
    request,
    path,
    bodyHash,
    scopeHash,
    dataVersion(request, env),
    "stale",
  ), {
    method: "GET",
  });
  const cache = caches.default;
  const cached = await cache.match(cacheKey);
  if (cached) {
    const headers = sanitizeResponseHeaders(cached, ttlSeconds, path, "HIT");
    return new Response(cached.body, {
      status: cached.status,
      statusText: cached.statusText,
      headers,
    });
  }

  const staleSeconds = staleTtlSeconds(env);
  const staleCached = await cache.match(staleCacheKey);
  if (staleCached) {
    context.waitUntil(refreshCacheableResponse({
      bodyText,
      cache,
      cacheKey,
      method,
      origin,
      path,
      request,
      staleCacheKey,
      staleSeconds,
      ttlSeconds,
    }).catch(() => undefined));
    const headers = sanitizeResponseHeaders(staleCached, ttlSeconds, path, "STALE");
    return new Response(staleCached.body, {
      status: staleCached.status,
      statusText: staleCached.statusText,
      headers,
    });
  }

  const originResponse = await fetchCacheableOriginResponse(request, origin, method, path, bodyText);
  if (!originResponse.ok) {
    return bypassResponse(originResponse);
  }

  const responseForClient = new Response(originResponse.body, {
    status: originResponse.status,
    statusText: originResponse.statusText,
    headers: sanitizeResponseHeaders(originResponse, ttlSeconds, path, "MISS"),
  });
  const responseForCache = responseForClient.clone();
  await putCacheableResponse(
    cache,
    cacheKey,
    staleCacheKey,
    responseForCache,
    ttlSeconds,
    staleSeconds,
    path,
  );
  return responseForClient;
}
