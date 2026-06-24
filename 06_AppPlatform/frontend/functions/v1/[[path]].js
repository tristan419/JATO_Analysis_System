const DEFAULT_ORIGIN = "https://www.ojeur.cloud";
const CACHEABLE_ENDPOINTS = new Map([
  ["GET metadata/columns", 3600],
  ["GET metadata/filter-snapshot", 3600],
  ["GET assistant/country/metadata", 3600],
  ["GET analysis/data-freshness", 300],
  ["POST filters/options", 300],
  ["POST filters/options/batch", 300],
  ["POST analysis/overview", 300],
  ["POST analysis/time-series", 300],
  ["POST analysis/time-series-grouped", 300],
]);
const FILTER_SNAPSHOT_COLUMNS = [
  ["国家", "country"],
  ["Body type", "body_type", "body type"],
  ["细分市场", "segment"],
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

async function sha256Hex(value) {
  const data = new TextEncoder().encode(value);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

async function requestScopeHash(request) {
  const userName = request.headers.get("x-user-name") || "anonymous";
  const userRole = request.headers.get("x-user-role") || "";
  return sha256Hex(`${userRole}\n${userName}`);
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

function sanitizeResponseHeaders(response, ttlSeconds, path, cacheState, cacheControl) {
  const headers = new Headers(response.headers);
  headers.delete("set-cookie");
  headers.set("x-jato-edge-cache", cacheState);
  headers.set("x-jato-edge-cache-endpoint", `/v1/${path}`);
  headers.set("cache-control", cacheControl || `public, max-age=0, s-maxage=${ttlSeconds}`);
  headers.set("vary", "X-User-Name, X-User-Role, X-JATO-Data-Version");
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
    const headers = sanitizeResponseHeaders(cached, ttlSeconds, path, "HIT");
    return new Response(cached.body, {
      status: cached.status,
      statusText: cached.statusText,
      headers,
    });
  }

  let originResponse = await originFetch(request, origin, path, bodyText);
  if (!originResponse.ok) {
    const shouldSynthesizeSnapshot =
      method === "GET" && path === "metadata/filter-snapshot" && originResponse.status === 404;
    const synthesizedResponse = shouldSynthesizeSnapshot
      ? await synthesizeFilterSnapshot(request, origin)
      : null;
    if (!synthesizedResponse) {
      return bypassResponse(originResponse);
    }
    originResponse = synthesizedResponse;
  }

  const responseForClient = new Response(originResponse.body, {
    status: originResponse.status,
    statusText: originResponse.statusText,
    headers: sanitizeResponseHeaders(originResponse, ttlSeconds, path, "MISS"),
  });
  const responseForCache = responseForClient.clone();
  context.waitUntil(cache.put(cacheKey, new Response(responseForCache.body, {
    status: responseForCache.status,
    statusText: responseForCache.statusText,
    headers: sanitizeResponseHeaders(
      responseForCache,
      ttlSeconds,
      path,
      "MISS",
      `public, max-age=${ttlSeconds}`,
    ),
  })));
  return responseForClient;
}
