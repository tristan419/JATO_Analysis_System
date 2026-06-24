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

function getArg(name) {
  const prefix = `--${name}=`;
  const value = process.argv.slice(2).find((item) => item.startsWith(prefix));
  return value ? value.slice(prefix.length) : "";
}

function parseHosts() {
  const raw = getArg("hosts") || process.env.JATO_PERF_HOSTS || "www,intl";
  return raw.split(",")
    .map((item) => item.trim().toLowerCase())
    .filter(Boolean)
    .map((key) => {
      const host = HOSTS[key];
      if (!host) throw new Error(`Unknown host "${key}". Use www, cn, or intl.`);
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

async function measureRoute(browser, host, route, credentials, timeoutMs) {
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
        apiStarts.set(request, performance.now());
      }
    } catch {}
  });
  page.on("response", (response) => {
    const request = response.request();
    const startedAt = apiStarts.get(request);
    if (startedAt === undefined) return;
    apiStarts.delete(request);
    const url = new URL(request.url());
    const headers = response.headers();
    apiCalls.push({
      cache: headers["x-jato-edge-cache"] || "",
      method: request.method(),
      ms: performance.now() - startedAt,
      path: url.pathname,
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
  await context.close();
  const slowApis = [...apiCalls].sort((a, b) => b.ms - a.ms).slice(0, 5);
  return {
    apiCalls,
    appReadyMs,
    browserMetrics,
    domContentLoadedMs,
    error,
    host: host.label,
    networkIdleMs,
    overviewReadyMs,
    route: route.label,
    slowApis,
  };
}

function seconds(value) {
  return typeof value === "number" ? (value / 1000).toFixed(2) : "-";
}

async function main() {
  const username = process.env.JATO_PERF_USERNAME || "";
  const password = process.env.JATO_PERF_PASSWORD || "";
  if (!username || !password) {
    throw new Error("Set JATO_PERF_USERNAME and JATO_PERF_PASSWORD before running this script.");
  }
  const timeoutMs = Number(getArg("timeout") || process.env.JATO_PERF_TIMEOUT_MS || 30_000);
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
    slowest_api_s: seconds(result.slowApis[0]?.ms),
    error: result.error ? result.error.slice(0, 96) : "",
  })));

  for (const result of results) {
    const slow = result.slowApis
      .map((api) => `${api.method} ${api.path} ${api.status} ${seconds(api.ms)}s ${api.cache || "-"}`)
      .join("; ");
    console.log(`${result.host}/${result.route} slow APIs: ${slow || "-"}`);
  }

  if (results.some((result) => result.error)) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
