import { useEffect, useMemo, useState } from "react";

import "./RouteDiagnosticsPage.css";
import {
  DECISION_KEY,
  MANUAL_KEY,
  buildRouteRedirectUrl,
  chooseAutoRoute,
  clearRouteDecisions,
  createManualRouteDecision,
  currentRouteTarget,
  detectClientRouteProfile,
  formatTarget,
  makeInitialProbe,
  probeRoute,
  readRouteDecision,
  routeLabel,
  saveRouteDecision,
  type ProbeResult,
  type RouteDecision,
  type RouteTarget,
} from "../utils/routeDecision";
import { apiUrl } from "../api/core";

export interface RouteResourceTiming {
  label: string;
  kind: string;
  durationMs: number;
  startTimeMs: number;
  transferSize: number | null;
  encodedBodySize: number | null;
  cached: boolean;
}

export interface RouteResourceSummary {
  totalTransferBytes: number;
  initialTransferBytes: number;
  initialJsTransferBytes: number;
  initialCssTransferBytes: number;
  initialVendorCount: number;
  resourceCount: number;
}

type ApiProbeStatus = "idle" | "running" | "ok" | "failed";

export interface RouteApiProbeSpec {
  key: string;
  label: string;
  method: "GET" | "POST";
  path: string;
  body?: Record<string, unknown>;
}

export interface RouteApiProbeResult {
  key: string;
  label: string;
  method: string;
  path: string;
  status: ApiProbeStatus;
  statusCode: number | null;
  durationMs: number | null;
  serverCache: string | null;
  edgeCache: string | null;
  error: string | null;
  checkedAt: string;
}

export const INITIAL_RESOURCE_WINDOW_MS = 8_000;
const RESOURCE_TABLE_LIMIT = 10;
const API_PROBE_SPECS: RouteApiProbeSpec[] = [
  {
    key: "metadata-snapshot",
    label: "Filter metadata snapshot",
    method: "GET",
    path: "/metadata/filter-snapshot",
  },
  {
    key: "auth-profile",
    label: "Auth profile",
    method: "GET",
    path: "/auth/me",
  },
  {
    key: "grouped-time-series",
    label: "Grouped time-series default",
    method: "POST",
    path: "/analysis/time-series-grouped",
    body: {
      filters: {},
      grain: "month",
      group_by: "动总规整",
      top_n: 8,
      include_others: true,
    },
  },
];
const VENDOR_RESOURCE_KINDS = new Set([
  "plotly",
  "recharts",
  "grid",
  "diagram",
  "vendor",
  "export",
]);

function formatDecision(decision: RouteDecision | null): string {
  if (!decision) return "-";
  return `${routeLabel(decision.target)} · ${new Date(decision.expiresAt).toLocaleString()}`;
}

function formatDecisionDetail(decision: RouteDecision | null): string {
  if (!decision) return "No cached decision. The router will probe both hosts, keep China-local browsers on www when www is reachable, and otherwise use the faster measured route.";
  const details: string[] = [];
  if (decision.reason) details.push(decision.reason);
  if (decision.createdAt) {
    details.push(`created ${new Date(decision.createdAt).toLocaleString()}`);
  }
  if (decision.cnMs !== undefined && decision.intlMs !== undefined) {
    details.push(`www ${decision.cnMs} ms / intl ${decision.intlMs} ms`);
  }
  if (decision.marginMs !== undefined) {
    details.push(`margin ${decision.marginMs} ms`);
  }
  return details.join(" · ") || "Cached route decision is active.";
}

function formatProbe(result: ProbeResult): string {
  if (result.status === "running") return "testing";
  if (result.status === "failed") return "failed";
  if (result.ms === null) return "-";
  return `${result.ms} ms`;
}

function formatShortId(value?: string): string {
  return value ? value.slice(0, 12) : "-";
}

function formatBytes(value: number | null): string {
  if (value === null || value <= 0) return value === 0 ? "cached" : "-";
  if (value >= 1024 * 1024) return `${(value / 1024 / 1024).toFixed(2)} MB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${value} B`;
}

function formatMilliseconds(value: number): string {
  return `${Math.round(value)} ms`;
}

function formatNullableMilliseconds(value: number | null): string {
  return value === null ? "-" : formatMilliseconds(value);
}

function formatCacheState(value: string | null): string {
  return value && value.trim() ? value : "-";
}

function buildFingerprint(result: ProbeResult): string {
  return result.frontendBuildId || result.buildCommit || "";
}

function formatBuildParity(results: Record<RouteTarget, ProbeResult>): string {
  const cnBuild = buildFingerprint(results.cn);
  const intlBuild = buildFingerprint(results.intl);
  if (!cnBuild || !intlBuild) return "Testing";
  return cnBuild === intlBuild ? "Same build" : "Different builds";
}

function classifyResource(name: string): string {
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

function formatResourceLabel(name: string): string {
  try {
    const url = new URL(name);
    const leaf = url.pathname.split("/").filter(Boolean).pop();
    return leaf || url.hostname;
  } catch {
    return name;
  }
}

function resourceTransferBytes(resource: RouteResourceTiming): number {
  const value = resource.transferSize ?? 0;
  return value > 0 ? value : 0;
}

export function summarizeRouteResources(
  resources: RouteResourceTiming[],
  initialWindowMs = INITIAL_RESOURCE_WINDOW_MS,
): RouteResourceSummary {
  const initialResources = resources.filter((resource) => resource.startTimeMs <= initialWindowMs);
  return {
    totalTransferBytes: resources.reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialTransferBytes: initialResources.reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialJsTransferBytes: initialResources
      .filter((resource) => (
        resource.kind === "js"
        || resource.kind === "app shell"
        || resource.kind === "dashboard"
        || VENDOR_RESOURCE_KINDS.has(resource.kind)
      ))
      .reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialCssTransferBytes: initialResources
      .filter((resource) => resource.kind === "css")
      .reduce((sum, resource) => sum + resourceTransferBytes(resource), 0),
    initialVendorCount: initialResources
      .filter((resource) => VENDOR_RESOURCE_KINDS.has(resource.kind))
      .length,
    resourceCount: resources.length,
  };
}

export function collectRouteResourceTimings(): RouteResourceTiming[] {
  const entries = performance.getEntriesByType("resource") as PerformanceResourceTiming[];
  return entries
    .filter((entry) => (
      entry.name.includes("/assets/")
      || entry.name.includes("/build-meta.json")
      || entry.name.includes("/route-probe.txt")
    ))
    .map((entry) => ({
      label: formatResourceLabel(entry.name),
      kind: classifyResource(entry.name),
      durationMs: entry.duration,
      startTimeMs: entry.startTime,
      transferSize: Number.isFinite(entry.transferSize) ? entry.transferSize : null,
      encodedBodySize: Number.isFinite(entry.encodedBodySize) ? entry.encodedBodySize : null,
      cached: entry.transferSize === 0 && entry.encodedBodySize > 0,
    }))
    .sort((a, b) => (
      (b.transferSize ?? b.encodedBodySize ?? 0) - (a.transferSize ?? a.encodedBodySize ?? 0)
      || b.durationMs - a.durationMs
    ));
}

function createIdleApiProbeResult(spec: RouteApiProbeSpec): RouteApiProbeResult {
  return {
    key: spec.key,
    label: spec.label,
    method: spec.method,
    path: spec.path,
    status: "idle",
    statusCode: null,
    durationMs: null,
    serverCache: null,
    edgeCache: null,
    error: null,
    checkedAt: "-",
  };
}

function diagnosticAuthHeaders(): Headers {
  const headers = new Headers();
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
  const role = (
    localStorage.getItem("jato_user_role")
    || import.meta.env.VITE_USER_ROLE
    || "viewer"
  ).trim();
  if (token) headers.set("X-Auth-Token", token);
  headers.set("X-User-Name", user || "anonymous");
  headers.set("X-User-Role", role || "viewer");
  return headers;
}

export async function probeCurrentApiPath(spec: RouteApiProbeSpec): Promise<RouteApiProbeResult> {
  const startedAt = performance.now();
  const headers = diagnosticAuthHeaders();
  let response: Response;
  try {
    if (spec.method === "POST") {
      headers.set("Content-Type", "application/json");
    }
    response = await fetch(apiUrl(spec.path), {
      method: spec.method,
      headers,
      body: spec.method === "POST" ? JSON.stringify(spec.body ?? {}) : undefined,
      cache: "no-store",
    });
  } catch (error) {
    return {
      ...createIdleApiProbeResult(spec),
      status: "failed",
      durationMs: performance.now() - startedAt,
      error: error instanceof Error ? error.message : String(error),
      checkedAt: new Date().toLocaleString(),
    };
  }

  return {
    ...createIdleApiProbeResult(spec),
    status: response.ok ? "ok" : "failed",
    statusCode: response.status,
    durationMs: performance.now() - startedAt,
    serverCache: response.headers.get("X-JATO-Server-Cache"),
    edgeCache: response.headers.get("X-JATO-Edge-Cache"),
    error: response.ok ? null : response.statusText || "Request failed",
    checkedAt: new Date().toLocaleString(),
  };
}

export function RouteDiagnosticsPage() {
  const [results, setResults] = useState<Record<RouteTarget, ProbeResult>>({
    cn: makeInitialProbe("cn"),
    intl: makeInitialProbe("intl"),
  });
  const [resourceTimings, setResourceTimings] = useState<RouteResourceTiming[]>([]);
  const [apiProbeResults, setApiProbeResults] = useState<RouteApiProbeResult[]>(
    () => API_PROBE_SPECS.map(createIdleApiProbeResult),
  );
  const [manualDecision, setManualDecision] = useState<RouteDecision | null>(() => readRouteDecision(window.localStorage, MANUAL_KEY));
  const [autoDecision, setAutoDecision] = useState<RouteDecision | null>(() => readRouteDecision(window.localStorage, DECISION_KEY));
  const currentHost = window.location.hostname || "-";
  const currentTarget = currentRouteTarget(currentHost);
  const clientProfile = useMemo(() => detectClientRouteProfile(), []);
  const activeDecision = manualDecision ?? autoDecision;
  const activeTarget = activeDecision?.target ?? currentTarget;
  const fastestTarget = useMemo<RouteTarget | null>(() => {
    const cn = results.cn.status === "ok" ? results.cn.ms : null;
    const intl = results.intl.status === "ok" ? results.intl.ms : null;
    if (cn === null && intl === null) return null;
    if (cn === null) return "intl";
    if (intl === null) return "cn";
    return cn <= intl ? "cn" : "intl";
  }, [results]);
  const autoRecommendation = useMemo(
    () => chooseAutoRoute(results, currentTarget, clientProfile),
    [clientProfile, currentTarget, results],
  );
  const buildParity = useMemo(() => formatBuildParity(results), [results]);
  const largestResource = resourceTimings[0] ?? null;
  const resourceSummary = useMemo(
    () => summarizeRouteResources(resourceTimings),
    [resourceTimings],
  );
  const visibleResourceTimings = useMemo(
    () => resourceTimings.slice(0, RESOURCE_TABLE_LIMIT),
    [resourceTimings],
  );

  function refreshResources() {
    setResourceTimings(collectRouteResourceTimings());
  }

  async function runApiProbes() {
    setApiProbeResults(API_PROBE_SPECS.map((spec) => ({
      ...createIdleApiProbeResult(spec),
      status: "running",
      checkedAt: "testing",
    })));
    const nextResults = await Promise.all(API_PROBE_SPECS.map(probeCurrentApiPath));
    setApiProbeResults(nextResults);
  }

  async function runProbe() {
    setResults({
      cn: { ...makeInitialProbe("cn"), status: "running" },
      intl: { ...makeInitialProbe("intl"), status: "running" },
    });
    const [cnResult, intlResult] = await Promise.all([
      probeRoute("cn"),
      probeRoute("intl"),
    ]);
    setResults({ cn: cnResult, intl: intlResult });
    setManualDecision(readRouteDecision(window.localStorage, MANUAL_KEY));
    setAutoDecision(readRouteDecision(window.localStorage, DECISION_KEY));
    void runApiProbes();
  }

  function lockRoute(target: RouteTarget) {
    const decision = createManualRouteDecision(target);
    saveRouteDecision(window.localStorage, decision);
    window.location.href = buildRouteRedirectUrl(decision, {
      pathname: "/route-diagnostics",
      search: "",
      hash: "",
    });
  }

  function resetAuto() {
    clearRouteDecisions(window.localStorage);
    setManualDecision(null);
    setAutoDecision(null);
    void runProbe();
  }

  useEffect(() => {
    void runProbe();
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(refreshResources, 1_500);
    return () => window.clearTimeout(timer);
  }, []);

  return (
    <div className="route-diagnostics-shell">
      <section className="route-diagnostics-header">
        <div>
          <span className="route-diagnostics-eyebrow">Delivery diagnostics</span>
          <h1>Route Diagnostics</h1>
        </div>
        <div className="route-diagnostics-actions">
          <button className="btn btn-sm btn-primary" type="button" onClick={runProbe}>重新测试</button>
          <button className="btn btn-sm btn-ghost" type="button" onClick={resetAuto}>自动</button>
          <button className="btn btn-sm btn-ghost" type="button" onClick={() => lockRoute("cn")}>锁定 www</button>
          <button className="btn btn-sm btn-ghost" type="button" onClick={() => lockRoute("intl")}>锁定 intl</button>
        </div>
      </section>

      <section className="route-diagnostics-grid">
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Current host</span>
          <strong>{currentHost}</strong>
          <span className="route-diagnostics-muted">当前浏览器正在使用的入口</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Final route</span>
          <strong>{formatTarget(activeTarget)}</strong>
          <span className="route-diagnostics-muted">{activeDecision ? `${activeDecision.source ?? "cached"} decision` : "当前入口"}</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Fastest path</span>
          <strong>{formatTarget(fastestTarget)}</strong>
          <span className="route-diagnostics-muted">基于本次 route-probe 实测</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Manual override</span>
          <strong>{formatDecision(manualDecision)}</strong>
          <span className="route-diagnostics-muted">24 小时有效</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Auto decision</span>
          <strong>{formatDecision(autoDecision)}</strong>
          <span className="route-diagnostics-muted">自动分流缓存</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Browser signal</span>
          <strong>{clientProfile.prefersChinaRoute ? "China-local" : "Neutral"}</strong>
          <span className="route-diagnostics-muted">{clientProfile.reason}; China-local browsers stay on www when www is reachable</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Build parity</span>
          <strong>{buildParity}</strong>
          <span className="route-diagnostics-muted">不同构建时自动分流会保持当前入口，避免旧资源混用</span>
        </article>
        <article className="route-diagnostics-panel">
          <span className="route-diagnostics-label">Largest resource</span>
          <strong>{largestResource ? formatBytes(largestResource.transferSize ?? largestResource.encodedBodySize) : "-"}</strong>
          <span className="route-diagnostics-muted">{largestResource?.label ?? "等待资源采样"}</span>
        </article>
      </section>

      <section className="route-diagnostics-decision">
        <div>
          <span className="route-diagnostics-label">Selection reason</span>
          <p>{formatDecisionDetail(activeDecision)}</p>
        </div>
        <div>
          <span className="route-diagnostics-label">Live recommendation</span>
          <p>
            {autoRecommendation
              ? `${formatTarget(autoRecommendation.target)} · ${autoRecommendation.reason}`
              : "Testing both hosts..."}
          </p>
        </div>
      </section>

      <section className="route-diagnostics-table-wrap">
        <table className="route-diagnostics-table">
          <thead>
            <tr>
              <th>入口</th>
              <th>Host</th>
              <th>状态</th>
              <th>耗时</th>
              <th>Commit</th>
              <th>Frontend build</th>
              <th>测试时间</th>
            </tr>
          </thead>
          <tbody>
            {(["cn", "intl"] as const).map((target) => {
              const result = results[target];
              return (
                <tr key={target}>
                  <td>{target === "cn" ? "www" : "intl"}</td>
                  <td>{result.host}</td>
                  <td>{result.status}</td>
                  <td>{formatProbe(result)}</td>
                  <td>{formatShortId(result.buildCommit)}</td>
                  <td>{formatShortId(result.frontendBuildId)}</td>
                  <td>{result.checkedAt}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>

      <section className="route-diagnostics-table-wrap">
        <div className="route-diagnostics-subheader">
          <div>
            <span className="route-diagnostics-label">API path probes</span>
            <p>当前入口的关键 API 链路耗时，用来区分路由慢、鉴权慢、还是大查询缓存未命中。</p>
          </div>
          <button className="btn btn-sm btn-secondary" type="button" onClick={runApiProbes}>刷新 API</button>
        </div>
        <table className="route-diagnostics-table">
          <thead>
            <tr>
              <th>接口</th>
              <th>方法</th>
              <th>状态</th>
              <th>HTTP</th>
              <th>耗时</th>
              <th>Server cache</th>
              <th>Edge cache</th>
              <th>测试时间</th>
            </tr>
          </thead>
          <tbody>
            {apiProbeResults.map((probe) => (
              <tr key={probe.key}>
                <td>
                  <strong>{probe.label}</strong>
                  <br />
                  <span className="route-diagnostics-muted">{probe.path}</span>
                </td>
                <td>{probe.method}</td>
                <td>{probe.status}</td>
                <td>{probe.statusCode ?? "-"}</td>
                <td>{formatNullableMilliseconds(probe.durationMs)}</td>
                <td>{formatCacheState(probe.serverCache)}</td>
                <td>{formatCacheState(probe.edgeCache)}</td>
                <td>{probe.error ? probe.error : probe.checkedAt}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="route-diagnostics-table-wrap">
        <div className="route-diagnostics-subheader">
          <div>
            <span className="route-diagnostics-label">Current page resources</span>
            <p>首屏窗口按 {INITIAL_RESOURCE_WINDOW_MS / 1000} 秒计算，用来确认是否提前加载 Plotly、Grid、Mermaid 等大资源。</p>
          </div>
          <button className="btn btn-sm btn-secondary" type="button" onClick={refreshResources}>刷新资源</button>
        </div>
        <div className="route-diagnostics-grid route-diagnostics-grid--compact">
          <article className="route-diagnostics-panel">
            <span className="route-diagnostics-label">Initial transfer</span>
            <strong>{formatBytes(resourceSummary.initialTransferBytes)}</strong>
            <span className="route-diagnostics-muted">前 {INITIAL_RESOURCE_WINDOW_MS / 1000} 秒资源传输</span>
          </article>
          <article className="route-diagnostics-panel">
            <span className="route-diagnostics-label">Initial JS</span>
            <strong>{formatBytes(resourceSummary.initialJsTransferBytes)}</strong>
            <span className="route-diagnostics-muted">JS、页面 chunk 与 vendor</span>
          </article>
          <article className="route-diagnostics-panel">
            <span className="route-diagnostics-label">Initial CSS</span>
            <strong>{formatBytes(resourceSummary.initialCssTransferBytes)}</strong>
            <span className="route-diagnostics-muted">首屏样式传输</span>
          </article>
          <article className="route-diagnostics-panel">
            <span className="route-diagnostics-label">Vendor chunks</span>
            <strong>{resourceSummary.initialVendorCount}</strong>
            <span className="route-diagnostics-muted">首屏实际请求的 vendor 数</span>
          </article>
          <article className="route-diagnostics-panel">
            <span className="route-diagnostics-label">Total transfer</span>
            <strong>{formatBytes(resourceSummary.totalTransferBytes)}</strong>
            <span className="route-diagnostics-muted">{resourceSummary.resourceCount} sampled resources</span>
          </article>
        </div>
        <table className="route-diagnostics-table">
          <thead>
            <tr>
              <th>资源</th>
              <th>类型</th>
              <th>传输</th>
              <th>内容大小</th>
              <th>耗时</th>
              <th>开始</th>
              <th>缓存</th>
            </tr>
          </thead>
          <tbody>
            {visibleResourceTimings.length > 0 ? visibleResourceTimings.map((resource) => (
              <tr key={`${resource.label}-${resource.startTimeMs}`}>
                <td>{resource.label}</td>
                <td>{resource.kind}</td>
                <td>{formatBytes(resource.transferSize)}</td>
                <td>{formatBytes(resource.encodedBodySize)}</td>
                <td>{formatMilliseconds(resource.durationMs)}</td>
                <td>{formatMilliseconds(resource.startTimeMs)}</td>
                <td>{resource.cached ? "yes" : "no"}</td>
              </tr>
            )) : (
              <tr>
                <td colSpan={7}>等待资源采样，或点击刷新资源。</td>
              </tr>
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}
