import { useEffect, useMemo, useState } from "react";

type RouteTarget = "cn" | "intl";
type ProbeStatus = "idle" | "running" | "ok" | "failed";

interface RouteDecision {
  target: RouteTarget;
  expiresAt: number;
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
    return { target: parsed.target, expiresAt: parsed.expiresAt };
  } catch {
    return null;
  }
}

function formatDecision(decision: RouteDecision | null): string {
  if (!decision) return "-";
  return `${decision.target === "cn" ? "www" : "intl"} · ${new Date(decision.expiresAt).toLocaleString()}`;
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

export function RouteDiagnosticsPage() {
  const [results, setResults] = useState<Record<RouteTarget, ProbeResult>>({
    cn: makeInitialProbe("cn"),
    intl: makeInitialProbe("intl"),
  });
  const [manualDecision, setManualDecision] = useState<RouteDecision | null>(() => readDecision(MANUAL_KEY));
  const [autoDecision, setAutoDecision] = useState<RouteDecision | null>(() => readDecision(DECISION_KEY));
  const currentHost = window.location.hostname || "-";
  const fastestTarget = useMemo<RouteTarget | null>(() => {
    const cn = results.cn.status === "ok" ? results.cn.ms : null;
    const intl = results.intl.status === "ok" ? results.intl.ms : null;
    if (cn === null && intl === null) return null;
    if (cn === null) return "intl";
    if (intl === null) return "cn";
    return cn <= intl ? "cn" : "intl";
  }, [results]);

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
          <span className="route-diagnostics-label">Fastest path</span>
          <strong>{fastestTarget ? ROUTE_HOSTS[fastestTarget] : "-"}</strong>
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
