const DEFAULT_ORIGIN_BASE_URL = "https://www.ojeur.cloud";
const DEFAULT_MAX_CACHE_BODY_BYTES = 8 * 1024 * 1024;

const CACHEABLE_ENDPOINTS = [
  { method: "GET", path: "/v1/metadata/columns", ttlEnv: "CACHE_METADATA_TTL_SECONDS", defaultTtl: 3600 },
  { method: "GET", path: "/v1/metadata/filter-snapshot", ttlEnv: "CACHE_METADATA_TTL_SECONDS", defaultTtl: 3600 },
  { method: "GET", path: "/v1/assistant/country/metadata", ttlEnv: "CACHE_METADATA_TTL_SECONDS", defaultTtl: 3600 },
  { method: "GET", path: "/v1/analysis/data-freshness", ttlEnv: "CACHE_METADATA_TTL_SECONDS", defaultTtl: 300 },
  { method: "POST", path: "/v1/filters/options", ttlEnv: "CACHE_FILTER_OPTIONS_TTL_SECONDS", defaultTtl: 300 },
  { method: "POST", path: "/v1/filters/options/batch", ttlEnv: "CACHE_FILTER_OPTIONS_TTL_SECONDS", defaultTtl: 300 },
  { method: "POST", path: "/v1/analysis/overview", ttlEnv: "CACHE_TIME_SERIES_TTL_SECONDS", defaultTtl: 300 },
  { method: "POST", path: "/v1/analysis/time-series", ttlEnv: "CACHE_TIME_SERIES_TTL_SECONDS", defaultTtl: 300 },
  { method: "POST", path: "/v1/analysis/time-series-grouped", ttlEnv: "CACHE_TIME_SERIES_TTL_SECONDS", defaultTtl: 300 },
];

function textResponse(body, status = 200, headers = {}) {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/plain; charset=utf-8",
      ...headers,
    },
  });
}

function parseOrigins(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function corsHeaders(request, env) {
  const origin = request.headers.get("origin") || "";
  const allowedOrigins = parseOrigins(env.ALLOWED_ORIGINS);
  if (!origin || !allowedOrigins.includes(origin)) return {};
  return {
    "access-control-allow-origin": origin,
    "access-control-allow-credentials": "true",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "Content-Type, X-Auth-Token, X-User-Name, X-User-Role, X-JATO-Data-Version",
    "vary": "Origin",
  };
}

function withCors(response, request, env) {
  const headers = new Headers(response.headers);
  const cors = corsHeaders(request, env);
  Object.entries(cors).forEach(([key, value]) => headers.set(key, value));
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function endpointConfig(requestUrl, method) {
  return CACHEABLE_ENDPOINTS.find((item) => (
    item.method === method && item.path === requestUrl.pathname
  )) || null;
}

function ttlSeconds(env, endpoint) {
  const configured = Number(env[endpoint.ttlEnv]);
  return Number.isFinite(configured) && configured > 0
    ? Math.floor(configured)
    : endpoint.defaultTtl;
}

function maxCacheBodyBytes(env) {
  const configured = Number(env.MAX_CACHE_BODY_BYTES);
  return Number.isFinite(configured) && configured > 0
    ? Math.floor(configured)
    : DEFAULT_MAX_CACHE_BODY_BYTES;
}

async function sha256Hex(input) {
  const bytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

async function bodyHash(request) {
  if (request.method === "GET" || request.method === "HEAD") return "empty";
  const buffer = await request.clone().arrayBuffer();
  if (buffer.byteLength === 0) return "empty";
  return sha256Hex(buffer);
}

async function authScopeHash(request) {
  const userRole = String(request.headers.get("x-user-role") || "viewer").trim().toLowerCase();
  return sha256Hex(`role:${userRole || "viewer"}`);
}

function dataVersion(request, env) {
  return (
    request.headers.get("x-jato-data-version")
    || env.DATA_VERSION
    || "default"
  );
}

async function cacheKeyRequest(request, env) {
  const url = new URL(request.url);
  const keyPayload = {
    method: request.method,
    path: url.pathname,
    search: url.searchParams.toString(),
    body: await bodyHash(request),
    authScope: await authScopeHash(request),
    dataVersion: dataVersion(request, env),
  };
  const keyHash = await sha256Hex(JSON.stringify(keyPayload));
  return new Request(new URL(`/_jato_readonly_api_cache/${keyHash}`, url.origin).toString(), {
    method: "GET",
  });
}

function originUrl(request, env) {
  const sourceUrl = new URL(request.url);
  const origin = new URL(env.ORIGIN_BASE_URL || DEFAULT_ORIGIN_BASE_URL);
  origin.pathname = sourceUrl.pathname;
  origin.search = sourceUrl.search;
  return origin;
}

function shouldBypass(request, endpoint) {
  if (!endpoint) return "not_cacheable_endpoint";
  if (request.headers.get("cache-control")?.includes("no-cache")) return "request_no_cache";
  if (request.method !== "GET" && request.method !== "POST") return "method_not_allowed";
  return "";
}

function cacheableOriginResponse(response) {
  if (response.status !== 200) return false;
  if (response.headers.has("set-cookie")) return false;
  return true;
}

async function fetchOrigin(request, env) {
  const upstream = new Request(originUrl(request, env).toString(), request);
  return fetch(upstream);
}

function responseWithCacheHeaders(body, originResponse, endpoint, ttl, cacheState) {
  const headers = new Headers(originResponse.headers);
  headers.delete("set-cookie");
  headers.set("cache-control", `public, max-age=${ttl}`);
  headers.set("x-jato-edge-cache", cacheState);
  headers.set("x-jato-edge-cache-endpoint", endpoint.path);
  return new Response(body, {
    status: originResponse.status,
    statusText: originResponse.statusText,
    headers,
  });
}

async function cachedReadOnlyResponse(request, env, ctx, endpoint) {
  const cache = caches.default;
  const key = await cacheKeyRequest(request, env);
  const cached = await cache.match(key);
  if (cached) {
    const headers = new Headers(cached.headers);
    headers.set("x-jato-edge-cache", "HIT");
    return withCors(new Response(cached.body, {
      status: cached.status,
      statusText: cached.statusText,
      headers,
    }), request, env);
  }

  const originResponse = await fetchOrigin(request, env);
  if (!cacheableOriginResponse(originResponse)) {
    const headers = new Headers(originResponse.headers);
    headers.set("x-jato-edge-cache", "BYPASS");
    return withCors(new Response(originResponse.body, {
      status: originResponse.status,
      statusText: originResponse.statusText,
      headers,
    }), request, env);
  }

  const body = await originResponse.arrayBuffer();
  const ttl = ttlSeconds(env, endpoint);
  const response = responseWithCacheHeaders(body.slice(0), originResponse, endpoint, ttl, "MISS");
  if (body.byteLength <= maxCacheBodyBytes(env)) {
    const cacheResponse = responseWithCacheHeaders(body.slice(0), originResponse, endpoint, ttl, "HIT");
    cacheResponse.headers.delete("vary");
    await cache.put(key, cacheResponse);
  }
  return withCors(response, request, env);
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders(request, env),
      });
    }

    const requestUrl = new URL(request.url);
    if (requestUrl.pathname === "/healthz") {
      return textResponse("ok", 200, corsHeaders(request, env));
    }

    const endpoint = endpointConfig(requestUrl, request.method);
    const bypassReason = shouldBypass(request, endpoint);
    if (bypassReason) {
      const originResponse = await fetchOrigin(request, env);
      const headers = new Headers(originResponse.headers);
      headers.set("x-jato-edge-cache", `BYPASS; reason=${bypassReason}`);
      return withCors(new Response(originResponse.body, {
        status: originResponse.status,
        statusText: originResponse.statusText,
        headers,
      }), request, env);
    }

    return cachedReadOnlyResponse(request, env, ctx, endpoint);
  },
};
