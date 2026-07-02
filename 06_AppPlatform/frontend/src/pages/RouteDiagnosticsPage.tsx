import { useEffect, useMemo, useState } from "react";

type RouteTarget = "cn" | "intl";
type ProbeStatus = "idle" | "running" | "ok" | "failed";

interface RouteDecision {
  target: RouteTarget;
  expiresAt: number;
  createdAt?: number;
  source?: "manual" | "auto";
  reason?: string;
  cnOk?: boolean;
  intlOk?: boolean;
  cnMs?: number;
  intlMs?: number;
  marginMs?: number;
}

interface ProbeResult {
  target: RouteTarget;
  host: string;
  status: ProbeStatus;
  ms: number | null;
  checkedAt: string;
}

const ROUTE_HOSTS: Record<RouteTarget, string> = {
  cn: "www.ojeur.cloud",
  intl: "intl.ojeur.cloud",
};
const DECISION_KEY = "jato_route_decision_v1";
const MANUAL_KEY = "jato_route_manual_v1";
const PROBE_TIMEOUT_MS = 1_800;
const REDIRECT_MARGIN_MS = 450;

function routeLabel(target: RouteTarget): string {
  return target === "cn" ? "www" : "intl";
}

function formatTarget(target: RouteTarget | null): string {
  if (!target) return "-";
  return ROUTE_HOSTS[target];
}

function numberOrUndefined(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function booleanOrUndefined(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function readDecision(key: string): RouteDecision | null {
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<RouteDecision>;
    if (parsed.target !== "cn" && parsed.target !== "intl") return null;
    if (typeof parsed.expiresAt !== "number" || parsed.expiresAt < Date.now()) {
      window.localStorage.removeItem(key);
      return null;
    }
    return {
      target: parsed.target,
      expiresAt: parsed.expiresAt,
      createdAt: numberOrUndefined(parsed.createdAt),
      source: parsed.source === "manual" || parsed.source === "auto" ? parsed.source : undefined,
      reason: typeof parsed.reason === "string" ? parsed.reason : undefined,
      cnOk: booleanOrUndefined(parsed.cnOk),
      intlOk: booleanOrUndefined(parsed.intlOk),
      cnMs: numberOrUndefined(parsed.cnMs),
      intlMs: numberOrUndefined(parsed.intlMs),
      marginMs: numberOrUndefined(parsed.marginMs),
    };
  } catch {
    return null;
  }
}

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

async function probeRoute(target: RouteTarget): Promise<ProbeResult> {
  const controller = new AbortController();
  const startedAt = performance.now();
  const timeout = window.setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS);
  const checkedAt = new Date().toLocaleTimeString();
  try {
    await fetch(`https://${ROUTE_HOSTS[target]}/route-probe.txt?ts=${Date.now()}`, {
      cache: "no-store",
      credentials: "omit",
      mode: "no-cors",
      signal: controller.signal,
    });
    return {
      target,
      host: ROUTE_HOSTS[target],
      status: "ok",
      ms: Math.round(performance.now() - startedAt),
      checkedAt,
    };
  } catch {
    return {
      target,
      host: ROUTE_HOSTS[target],
      status: "failed",
      ms: Math.round(performance.now() - startedAt),
      checkedAt,
    };
  } finally {
    window.clearTimeout(timeout);
  }
}

function makeInitialProbe(target: RouteTarget): ProbeResult {
  return {
    target,
    host: ROUTE_HOSTS[target],
    status: "idle",
    ms: null,
    checkedAt: "-",
  };
}

function currentRouteTarget(host: string): RouteTarget | null {
  if (host === ROUTE_HOSTS.cn) return "cn";
  if (host === ROUTE_HOSTS.intl) return "intl";
  return null;
}

function chooseAutoRoute(
  results: Record<RouteTarget, ProbeResult>,
  currentTarget: RouteTarget | null,
): { target: RouteTarget; reason: string } | null {
  const cnOk = results.cn.status === "ok";
  const intlOk = results.intl.status === "ok";
  if (!cnOk && !intlOk) {
    if (results.cn.status === "running" || results.intl.status === "running") return null;
    return {
      target: currentTarget ?? "cn",
      reason: "Both probes failed; stay on the current host.",
    };
  }
  if (cnOk && !intlOk) {
    return {
      target: "cn",
      reason: "intl probe failed and www probe succeeded.",
    };
  }
  if (intlOk && !cnOk) {
    return {
      target: "intl",
      reason: "www probe failed and intl probe succeeded.",
    };
  }
  const cnMs = results.cn.ms ?? PROBE_TIMEOUT_MS;
  const intlMs = results.intl.ms ?? PROBE_TIMEOUT_MS;
  if (intlMs + REDIRECT_MARGIN_MS < cnMs) {
    return {
      target: "intl",
      reason: `intl is faster by ${cnMs - intlMs} ms, above the ${REDIRECT_MARGIN_MS} ms redirect margin.`,
    };
  }
  return {
    target: "cn",
    reason: `www is preferred because intl is not more than ${REDIRECT_MARGIN_MS} ms faster.`,
  };
}

export function RouteDiagnosticsPage() {
  const [results, setResults] = useState<Record<RouteTarget, ProbeResult>>({
    cn: makeInitialProbe("cn"),
    intl: makeInitialProbe("intl"),
  });
  const [manualDecision, setManualDecision] = useState<RouteDecision | null>(() => readDecision(MANUAL_KEY));
  const [autoDecision, setAutoDecision] = useState<RouteDecision | null>(() => readDecision(DECISION_KEY));
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
    setManualDecision(readDecision(MANUAL_KEY));
    setAutoDecision(readDecision(DECISION_KEY));
  }

  function lockRoute(target: RouteTarget) {
    window.localStorage.setItem(
      MANUAL_KEY,
      JSON.stringify({
        target,
        source: "manual",
        reason: "Manual override from route diagnostics",
        createdAt: Date.now(),
        expiresAt: Date.now() + 24 * 60 * 60 * 1000,
      }),
    );
    window.location.href = `https://${ROUTE_HOSTS[target]}/route-diagnostics?routeChecked=1`;
  }

  function resetAuto() {
    window.localStorage.removeItem(MANUAL_KEY);
    window.localStorage.removeItem(DECISION_KEY);
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
