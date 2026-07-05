#!/usr/bin/env node

const { performance } = require("node:perf_hooks");
const { chromium } = require("playwright");

const HOSTS = {
  cn: { label: "www", origin: "https://www.ojeur.cloud", target: "cn" },
  www: { label: "www", origin: "https://www.ojeur.cloud", target: "cn" },
  intl: { label: "intl", origin: "https://intl.ojeur.cloud", target: "intl" },
};

const WIDE_DASHBOARD_PATH = "/dashboard?country=%E4%B8%B9%E9%BA%A6%2C%E5%85%8B%E7%BD%97%E5%9C%B0%E4%BA%9A%2C%E5%8C%88%E7%89%99%E5%88%A9%2C%E5%A5%A5%E5%9C%B0%E5%88%A9%2C%E5%B8%8C%E8%85%8A%2C%E5%BE%B7%E5%9B%BD%2C%E6%84%8F%E5%A4%A7%E5%88%A9%2C%E6%8C%AA%E5%A8%81%2C%E6%8D%B7%E5%85%8B%2C%E6%96%AF%E6%B4%9B%E4%BC%90%E5%85%8B%2C%E6%96%AF%E6%B4%9B%E6%96%87%E5%B0%BC%E4%BA%9A%2C%E6%AF%94%E5%88%A9%E6%97%B6%2C%E6%B3%95%E5%9B%BD%2C%E6%B3%A2%E5%85%B0%2C%E7%91%9E%E5%85%B8%2C%E7%91%9E%E5%A3%AB%2C%E7%BD%97%E9%A9%AC%E5%B0%BC%E4%BA%9A%2C%E8%8A%AC%E5%85%B0%2C%E8%8D%B7%E5%85%B0%2C%E8%91%A1%E8%90%84%E7%89%99%2C%E8%A5%BF%E7%8F%AD%E7%89%99&powertrain=ICE%2CHEV%2CBEV%2CMHEV%2CPHEV";

const DEFAULT_ROUTES = [
  { label: "dashboard", path: "/dashboard", selector: ".dashboard-layout", waitForOverview: false },
  { label: "dashboard-wide", path: WIDE_DASHBOARD_PATH, selector: ".dashboard-layout" },
];
const DEFAULT_INITIAL_WINDOW_MS = 8_000;

function getArg(name) {
  const prefix = `--${name}=`;
  const value = process.argv.slice(2).find((item) => item.startsWith(prefix));
  return value ? value.slice(prefix.length) : "";
}

function normalizeOrigin(value) {
  const url = new URL(value);
  return url.origin;
}

function normalizeRouteTarget(value) {
  return value === "intl" ? "intl" : "cn";
}

function parsePositiveInteger(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function parseCustomHosts() {
  const raw = process.env.JATO_PERF_ORIGINS_JSON;
  if (!raw) return {};
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("JATO_PERF_ORIGINS_JSON must be an array.");
  }
  const hosts = {};
  for (const item of parsed) {
    if (!item || typeof item !== "object") {
      throw new Error("Each custom origin must be an object.");
    }
    const label = String(item.label || "").trim();
    const origin = String(item.origin || "").trim();
    if (!label || !origin) {
      throw new Error("Each custom origin needs label and origin.");
    }
    hosts[label.toLowerCase()] = {
      label,
      origin: normalizeOrigin(origin),
      target: normalizeRouteTarget(item.target),
    };
  }
  return hosts;
}

function parseInlineHost(raw) {
  const separator = raw.includes("=") ? "=" : raw.includes("@") ? "@" : "";
  if (!separator) return null;
  const [label, originWithTarget] = raw.split(separator);
  const [origin, target = "intl"] = originWithTarget.split("|");
  return {
    label: label.trim(),
    origin: normalizeOrigin(origin.trim()),
    target: normalizeRouteTarget(target.trim()),
  };
}

function parseHosts() {
  const customHosts = parseCustomHosts();
  const raw = getArg("hosts") || process.env.JATO_PERF_HOSTS || "www,intl";
  return raw.split(",")
    .map((item) => item.trim())
    .filter(Boolean)
    .map((item) => {
      const inlineHost = parseInlineHost(item);
      if (inlineHost) return inlineHost;
      const key = item.toLowerCase();
      const host = customHosts[key] || HOSTS[key];
      if (!host) {
        throw new Error(`Unknown host "${item}". Use www, cn, intl, label=https://origin|intl, or JATO_PERF_ORIGINS_JSON.`);
      }
      return host;
    });
}

function parseRoutes() {
  const raw = process.env.JATO_PERF_ROUTES_JSON;
  if (!raw) return DEFAULT_ROUTES;
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error("JATO_PERF_ROUTES_JSON must be an array.");
  }
  return parsed.map((item, index) => {
    if (!item || typeof item !== "object") {
      throw new Error(`Route ${index} is not an object.`);
    }
    const label = String(item.label || `route-${index}`);
    const path = String(item.path || "");
    const selector = String(item.selector || ".dashboard-layout");
    const waitForOverview = item.waitForOverview !== false;
    if (!path.startsWith("/")) {
      throw new Error(`Route ${label} path must start with "/".`);
    }
    return { label, path, selector, waitForOverview };
  });
}

function buildUrl(origin, path) {
  const url = new URL(path, origin);
  url.searchParams.set("route", "stay");
  return url.toString();
}

async function createAuthenticatedContext(browser, host, username, password, timeoutMs) {
  const context = await browser.newContext({ ignoreHTTPSErrors: true });
  const response = await context.request.post(`${host.origin}/v1/auth/login`, {
    data: { username, password },
    timeout: timeoutMs,
  });
  if (!response.ok()) {
    const body = await response.text().catch(() => "");
    throw new Error(`${host.label} login failed: ${response.status()} ${body.slice(0, 160)}`);
  }
  const data = await response.json();
  const token = String(data.token || "");
  if (!token) {
    throw new Error(`${host.label} login did not return a token.`);
  }
  await context.addInitScript(({ routeTarget, tokenValue, userName, userRole }) => {
    localStorage.setItem("jato_auth_token", tokenValue);
    localStorage.setItem("jato_user_name", userName);
    localStorage.setItem("jato_user_role", userRole);
    localStorage.setItem("jato_route_manual_v1", JSON.stringify({
      createdAt: Date.now(),
      expiresAt: Date.now() + 60 * 60 * 1000,
      reason: "performance measurement",
      source: "manual",
      target: routeTarget,
    }));
    localStorage.removeItem("jato_route_decision_v2");
    localStorage.removeItem("jato_route_decision_v1");
    localStorage.removeItem("jato_route_probe_inflight_v1");
  }, {
    routeTarget: host.target,
    tokenValue: token,
    userName: String(data.username || username),
    userRole: String(data.role || "viewer"),
  });
  return context;
}

async function collectBrowserMetrics(page) {
  return page.evaluate(() => {
    const navigation = performance.getEntriesByType("navigation")[0];
    const fcp = performance.getEntriesByName("first-contentful-paint")[0];
    return {
      domContentLoadedMs: navigation ? navigation.domContentLoadedEventEnd : null,
      firstContentfulPaintMs: fcp ? fcp.startTime : null,
      loadMs: navigation && navigation.loadEventEnd > 0 ? navigation.loadEventEnd : null,
      transferSize: navigation && "transferSize" in navigation ? navigation.transferSize : null,
    };
  }).catch(() => ({
    domContentLoadedMs: null,
    firstContentfulPaintMs: null,
    loadMs: null,
    transferSize: null,
  }));
}

function classifyResource(name) {
  if (name.includes("plotly-vendor")) return "plotly";
  if (name.includes("recharts-vendor")) return "recharts";
  if (name.includes("grid-vendor")) return "grid";
  if (name.includes("diagram-vendor")) return "diagram";
  if (name.includes("export-vendor")) return "export";
  if (name.includes("DashboardPage")) return "dashboard";
  if (name.includes("/assets/index-") && name.endsWith(".css")) return "css";
  if (name.includes("/assets/index-") && name.endsWith(".js")) return "app shell";
  if (name.includes("-vendor")) return "vendor";
  if (name.endsWith(".woff2") || name.endsWith(".woff")) return "font";
  if (name.endsWith(".css")) return "css";
  if (name.endsWith(".js")) return "js";
  return "resource";
}

function formatResourceLabel(name) {
  try {
    const url = new URL(name);
    const leaf = url.pathname.split("/").filter(Boolean).pop();
    return leaf || url.hostname;
  } catch {
    return name;
  }
}

const VENDOR_RESOURCE_KINDS = new Set([
  "plotly",
  "recharts",
  "grid",
  "diagram",
  "vendor",
  "export",
]);

function resourceTransferBytes(resource) {
  const value = resource.transferSize ?? 0;
  return value > 0 ? value : 0;
}

function summarizeRouteResources(resources, initialWindowMs) {
  const initialResources = resources.filter((resource) => resource.startTimeMs <= initialWindowMs);
  return {
    initialCssTransferBytes: initialResources
      .filter((resource) => resource.kind === "css")
      .reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialJsTransferBytes: initialResources
      .filter((resource) => (
        resource.kind === "js"
        || resource.kind === "app shell"
        || resource.kind === "dashboard"
        || VENDOR_RESOURCE_KINDS.has(resource.kind)
      ))
      .reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialTransferBytes: initialResources
      .reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialVendorCount: initialResources
      .filter((resource) => VENDOR_RESOURCE_KINDS.has(resource.kind))
      .length,
    resourceCount: resources.length,
    totalTransferBytes: resources
      .reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
  };
}

async function collectRouteResources(page) {
  return page.evaluate(() => {
    const entries = performance.getEntriesByType("resource");
    return entries
      .filter((entry) => (
        entry.name.includes("/assets/")
        || entry.name.includes("/build-meta.json")
        || entry.name.includes("/route-probe.txt")
      ))
      .map((entry) => {
        const timing = entry;
        return {
          durationMs: timing.duration,
          encodedBodySize: Number.isFinite(timing.encodedBodySize) ? timing.encodedBodySize : null,
          name: timing.name,
          startTimeMs: timing.startTime,
          transferSize: Number.isFinite(timing.transferSize) ? timing.transferSize : null,
        };
      });
  }).then((entries) => entries
    .map((entry) => ({
      ...entry,
      kind: classifyResource(entry.name),
      label: formatResourceLabel(entry.name),
    }))
    .sort((a, b) => (
      (b.transferSize ?? b.encodedBodySize ?? 0) - (a.transferSize ?? a.encodedBodySize ?? 0)
      || b.durationMs - a.durationMs
    )),
  ).catch(() => []);
}

async function measureRoute(browser, host, route, credentials, timeoutMs, initialWindowMs) {
  const context = await createAuthenticatedContext(
    browser,
    host,
    credentials.username,
    credentials.password,
    timeoutMs,
  );
  const page = await context.newPage();
  const apiStarts = new Map();
  const apiCalls = [];
  let navigationStartedAt = 0;
  let overviewReadyMs = null;
  let resolveOverview = null;
  const overviewResponsePromise = new Promise((resolve) => {
    resolveOverview = resolve;
  });
  page.on("request", (request) => {
    try {
      const url = new URL(request.url());
      if (url.pathname.startsWith("/v1/")) {
        const requestStartedAt = performance.now();
        apiStarts.set(request, {
          startedAt: requestStartedAt,
          startMs: Math.max(0, requestStartedAt - navigationStartedAt),
        });
      }
    } catch {}
  });
  page.on("response", (response) => {
    const request = response.request();
    const timing = apiStarts.get(request);
    if (timing === undefined) return;
    apiStarts.delete(request);
    const url = new URL(request.url());
    const headers = response.headers();
    apiCalls.push({
      cache: headers["x-jato-edge-cache"] || "",
      endMs: performance.now() - navigationStartedAt,
      method: request.method(),
      ms: performance.now() - timing.startedAt,
      path: url.pathname,
      startMs: timing.startMs,
      status: response.status(),
    });
    if (url.pathname === "/v1/analysis/overview" && overviewReadyMs === null) {
      overviewReadyMs = performance.now() - navigationStartedAt;
      resolveOverview?.();
    }
  });

  const url = buildUrl(host.origin, route.path);
  const startedAt = performance.now();
  navigationStartedAt = startedAt;
  let domContentLoadedMs = null;
  let appReadyMs = null;
  let networkIdleMs = null;
  let error = "";
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: timeoutMs });
    domContentLoadedMs = performance.now() - startedAt;
    await page.waitForSelector(route.selector, { timeout: timeoutMs });
    appReadyMs = performance.now() - startedAt;
    if (route.waitForOverview !== false) {
      await Promise.race([
        overviewResponsePromise,
        new Promise((resolve) => setTimeout(resolve, Math.min(timeoutMs, 12_000))),
      ]);
    }
    try {
      await page.waitForLoadState("networkidle", { timeout: Math.min(timeoutMs, 15_000) });
      networkIdleMs = performance.now() - startedAt;
    } catch {}
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
  }

  const browserMetrics = await collectBrowserMetrics(page);
  const resources = await collectRouteResources(page);
  const resourceSummary = summarizeRouteResources(resources, initialWindowMs);
  await context.close();
  const slowApis = [...apiCalls].sort((a, b) => b.ms - a.ms).slice(0, 5);
  const initialWindowApis = apiCalls
    .filter((api) => api.startMs <= initialWindowMs)
    .sort((a, b) => a.startMs - b.startMs || b.ms - a.ms);
  const initialWindowSlowApis = [...initialWindowApis]
    .sort((a, b) => b.ms - a.ms)
    .slice(0, 5);
  return {
    apiCalls,
    appReadyMs,
    browserMetrics,
    domContentLoadedMs,
    error,
    host: host.label,
    initialWindowApis,
    initialWindowMs,
    initialWindowSlowApis,
    networkIdleMs,
    overviewReadyMs,
    resourceSummary,
    resources,
    route: route.label,
    slowApis,
  };
}

function seconds(value) {
  return typeof value === "number" ? (value / 1000).toFixed(2) : "-";
}

function countCacheState(apiCalls, state) {
  return apiCalls.filter((api) => api.cache.toUpperCase() === state).length;
}

function bytes(value) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "-";
  if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(2)}MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)}KB`;
  return `${value}B`;
}

async function main() {
  const username = process.env.JATO_PERF_USERNAME || "";
  const password = process.env.JATO_PERF_PASSWORD || "";
  if (!username || !password) {
    throw new Error("Set JATO_PERF_USERNAME and JATO_PERF_PASSWORD before running this script.");
  }
  const timeoutMs = Number(getArg("timeout") || process.env.JATO_PERF_TIMEOUT_MS || 30_000);
  const initialWindowMs = parsePositiveInteger(
    getArg("initial-window-ms") || process.env.JATO_PERF_INITIAL_WINDOW_MS,
    DEFAULT_INITIAL_WINDOW_MS,
  );
  const hosts = parseHosts();
  const routes = parseRoutes();
  const browser = await chromium.launch({ headless: true });
  const results = [];
  try {
    for (const host of hosts) {
      for (const route of routes) {
        results.push(await measureRoute(
          browser,
          host,
          route,
          { username, password },
          timeoutMs,
          initialWindowMs,
        ));
      }
    }
  } finally {
    await browser.close();
  }

  console.table(results.map((result) => ({
    host: result.host,
    route: result.route,
    dom_s: seconds(result.domContentLoadedMs),
    app_ready_s: seconds(result.appReadyMs),
    data_ready_s: seconds(result.overviewReadyMs),
    fcp_s: seconds(result.browserMetrics.firstContentfulPaintMs),
    network_idle_s: seconds(result.networkIdleMs),
    api_count: result.apiCalls.length,
    edge_hit: countCacheState(result.apiCalls, "HIT"),
    edge_stale: countCacheState(result.apiCalls, "STALE"),
    edge_miss: countCacheState(result.apiCalls, "MISS"),
    edge_bypass: countCacheState(result.apiCalls, "BYPASS"),
    initial_window_s: seconds(result.initialWindowMs),
    initial_api_count: result.initialWindowApis.length,
    initial_slowest_api_s: seconds(result.initialWindowSlowApis[0]?.ms),
    initial_transfer: bytes(result.resourceSummary.initialTransferBytes),
    initial_js: bytes(result.resourceSummary.initialJsTransferBytes),
    initial_css: bytes(result.resourceSummary.initialCssTransferBytes),
    initial_vendor_count: result.resourceSummary.initialVendorCount,
    slowest_resource_s: seconds(result.resources[0]?.durationMs),
    slowest_resource: result.resources[0]?.label ?? "",
    slowest_api_s: seconds(result.slowApis[0]?.ms),
    error: result.error ? result.error.slice(0, 96) : "",
  })));

  for (const result of results) {
    const slow = result.slowApis
      .map((api) => `${api.method} ${api.path} ${api.status} ${seconds(api.ms)}s ${api.cache || "-"}`)
      .join("; ");
    console.log(`${result.host}/${result.route} slow APIs: ${slow || "-"}`);
    const initialApis = result.initialWindowApis
      .map((api) => `${seconds(api.startMs)}s ${api.method} ${api.path} ${api.status} ${seconds(api.ms)}s ${api.cache || "-"}`)
      .join("; ");
    console.log(`${result.host}/${result.route} first ${seconds(result.initialWindowMs)}s APIs: ${initialApis || "-"}`);
    const resources = result.resources
      .slice(0, 8)
      .map((resource) => `${seconds(resource.startTimeMs)}s ${resource.kind} ${resource.label} ${seconds(resource.durationMs)}s transfer=${bytes(resource.transferSize)} encoded=${bytes(resource.encodedBodySize)}`)
      .join("; ");
    console.log(`${result.host}/${result.route} slow resources: ${resources || "-"}`);
    console.log(
      `${result.host}/${result.route} resource budget: initial_transfer=${bytes(result.resourceSummary.initialTransferBytes)}`
      + ` initial_js=${bytes(result.resourceSummary.initialJsTransferBytes)}`
      + ` initial_css=${bytes(result.resourceSummary.initialCssTransferBytes)}`
      + ` initial_vendor_count=${result.resourceSummary.initialVendorCount}`
      + ` total_transfer=${bytes(result.resourceSummary.totalTransferBytes)}`
      + ` resources=${result.resourceSummary.resourceCount}`,
    );
  }

  if (results.some((result) => result.error)) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
