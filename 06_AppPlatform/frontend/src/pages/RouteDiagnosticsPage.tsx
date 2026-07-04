import { useEffect, useMemo, useState } from "react";

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

interface RouteResourceTiming {
  label: string;
  kind: string;
  durationMs: number;
  startTimeMs: number;
  transferSize: number | null;
  encodedBodySize: number | null;
  cached: boolean;
}

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

function collectRouteResourceTimings(): RouteResourceTiming[] {
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
    ))
    .slice(0, 10);
}

export function RouteDiagnosticsPage() {
  const [results, setResults] = useState<Record<RouteTarget, ProbeResult>>({
    cn: makeInitialProbe("cn"),
    intl: makeInitialProbe("intl"),
  });
  const [resourceTimings, setResourceTimings] = useState<RouteResourceTiming[]>([]);
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

  function refreshResources() {
    setResourceTimings(collectRouteResourceTimings());
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
            <span className="route-diagnostics-label">Current page resources</span>
            <p>按传输体积排序，用来确认首屏是否提前加载大 JS/CSS/字体资源。</p>
          </div>
          <button className="btn btn-sm btn-secondary" type="button" onClick={refreshResources}>刷新资源</button>
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
            {resourceTimings.length > 0 ? resourceTimings.map((resource) => (
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
