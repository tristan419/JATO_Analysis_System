const DEFAULT_ORIGIN = "https://www.ojeur.cloud";
const CACHEABLE_ENDPOINTS = new Map([
  ["GET metadata/columns", 3600],
  ["GET metadata/filter-snapshot", 3600],
  ["GET assistant/country/metadata", 3600],
  ["POST filters/options", 300],
  ["POST filters/options/batch", 300],
  ["POST analysis/time-series-grouped", 300],
]);

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

async function sha256Hex(value) {
  const data = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function requestScopeHash(request) {
  const token = request.headers.get("x-auth-token") || "";
  const userName = request.headers.get("x-user-name") || "anonymous";
  const userRole = request.headers.get("x-user-role") || "";
  return sha256Hex(`${userRole}\n${userName}\n${token}`);
}

function dataVersion(request, env) {
  return (
    request.headers.get("x-jato-data-version")
    || env.DATA_VERSION
    || env.JATO_DATA_VERSION
    || "default"
  );
}

function cacheRequestUrl(request, path, bodyHash, scopeHash, version) {
  const sourceUrl = new URL(request.url);
  const cacheUrl = new URL(`https://jato-edge-cache.local/v1/${path}`);
  cacheUrl.search = sourceUrl.search;
  cacheUrl.searchParams.set("__method", request.method.toUpperCase());
  cacheUrl.searchParams.set("__body", bodyHash);
  cacheUrl.searchParams.set("__scope", scopeHash);
  cacheUrl.searchParams.set("__data", version);
  return cacheUrl.toString();
}

function sanitizeResponseHeaders(response, ttlSeconds, path, cacheState) {
  const headers = new Headers(response.headers);
  headers.delete("set-cookie");
  headers.set("x-jato-edge-cache", cacheState);
  headers.set("x-jato-edge-cache-endpoint", `/v1/${path}`);
  headers.set("cache-control", `public, max-age=0, s-maxage=${ttlSeconds}`);
  headers.set("vary", "X-Auth-Token, X-User-Name, X-User-Role, X-JATO-Data-Version");
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

async function originFetch(request, origin, path, body) {
  const sourceUrl = new URL(request.url);
  const targetUrl = new URL(`/v1/${path}${sourceUrl.search}`, origin);
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("cf-connecting-ip");
  headers.delete("cf-ipcountry");
  headers.delete("cf-ray");
  headers.delete("x-forwarded-for");
  headers.delete("x-forwarded-proto");
  return fetch(targetUrl.toString(), {
    method: request.method,
    headers,
    body: request.method === "GET" || request.method === "HEAD" ? undefined : body,
  });
}

export async function onRequest(context) {
  const { request, env, params } = context;
  const path = getPath(params);
  const method = request.method.toUpperCase();
  const ttlSeconds = cacheTtlSeconds(method, path);
  const origin = resolveOrigin(env);

  if (!ttlSeconds) {
    const body = method === "GET" || method === "HEAD" ? undefined : request.body;
    return bypassResponse(await originFetch(request, origin, path, body));
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
  const cache = caches.default;
  const cached = await cache.match(cacheKey);
  if (cached) {
    const headers = new Headers(cached.headers);
    headers.set("x-jato-edge-cache", "HIT");
    return new Response(cached.body, {
      status: cached.status,
      statusText: cached.statusText,
      headers,
    });
  }

  const originResponse = await originFetch(request, origin, path, bodyText);
  if (!originResponse.ok) {
    return bypassResponse(originResponse);
  }

  const responseForCache = new Response(originResponse.body, {
    status: originResponse.status,
    statusText: originResponse.statusText,
    headers: sanitizeResponseHeaders(originResponse, ttlSeconds, path, "MISS"),
  });
  context.waitUntil(cache.put(cacheKey, responseForCache.clone()));
  return responseForCache;
}
