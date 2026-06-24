import { useEffect, useMemo, useState } from "react";

import {
  DECISION_KEY,
  MANUAL_KEY,
  buildRouteRedirectUrl,
  chooseAutoRoute,
  clearRouteDecisions,
  createManualRouteDecision,
  currentRouteTarget,
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

function formatDecision(decision: RouteDecision | null): string {
  if (!decision) return "-";
  return `${routeLabel(decision.target)} · ${new Date(decision.expiresAt).toLocaleString()}`;
}

function formatDecisionDetail(decision: RouteDecision | null): string {
  if (!decision) return "No cached decision. The router will probe both hosts and prefer www unless intl is clearly faster.";
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

export function RouteDiagnosticsPage() {
  const [results, setResults] = useState<Record<RouteTarget, ProbeResult>>({
    cn: makeInitialProbe("cn"),
    intl: makeInitialProbe("intl"),
  });
  const [manualDecision, setManualDecision] = useState<RouteDecision | null>(() => readRouteDecision(window.localStorage, MANUAL_KEY));
  const [autoDecision, setAutoDecision] = useState<RouteDecision | null>(() => readRouteDecision(window.localStorage, DECISION_KEY));
  const currentHost = window.location.hostname || "-";
  const currentTarget = currentRouteTarget(currentHost);
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
    () => chooseAutoRoute(results, currentTarget),
    [currentTarget, results],
  );

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
                  <td>{result.checkedAt}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </section>
    </div>
  );
}
