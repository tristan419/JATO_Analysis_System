import { Fragment, useEffect, useState, useCallback } from "react";
import { api } from "../api/client";

interface DryrunSource {
  index: number;
  totalInCountry: number;
  sourceCode: string;
  status: "pass" | "empty" | "fail";
  valid: number;
  extracted: number;
  rejected: number;
  elapsedSeconds: number;
  failureReason?: string;
  recommendedStrategy?: string;
  error?: string;
  extractorError?: string;
  sourceUrl?: string;
  finalUrl?: string;
  httpStatus?: number | string;
}

interface DryrunCountry {
  countryCode: string;
  countryLabel: string;
  total: number;
  pass: number;
  empty: number;
  fail: number;
  completed: boolean;
  passRate: number;
  errors?: number;
  status?: string;
  topFailureReason?: string;
  failureBreakdown?: Record<string, number>;
  strategyRecommendations?: Record<string, number>;
  sources: DryrunSource[];
}

interface DryrunCurrent {
  available: boolean;
  running: boolean;
  logFile?: string;
  runId?: string;
  batch?: string;
  gateStatus?: string;
  gateThreshold?: number;
  startedAt?: string;
  finishedAt?: string;
  expectedCountries?: string[];
  observedCountries?: string[];
  missingCountries?: string[];
  duplicateCountries?: string[];
  countries: DryrunCountry[];
  totalSources: number;
  totalPass: number;
  totalEmpty: number;
  totalFail: number;
  overallPassRate: number;
  recentResults: DryrunSource[];
  reason?: string;
}

interface DryrunHistoryCountry {
  countryCode: string;
  countryLabel: string;
  total: number;
  pass: number;
  empty: number;
  fail: number;
  passRate: number;
}

interface DryrunHistoryRun {
  runId?: string;
  batch: string;
  countries: string[];
  total: number;
  pass: number;
  empty: number;
  fail: number;
  errors: number;
  passRate: number;
  timestamp: string;
  file: string;
  gateStatus?: string;
  status?: string;
  countriesDetail?: DryrunHistoryCountry[];
}

interface DryrunDashboard {
  current: DryrunCurrent;
  history: DryrunHistoryRun[];
  selectedRunId?: string | null;
  latestRunId?: string | null;
  serverTime: string;
}

const STATUS_ICON: Record<string, string> = { pass: "✅", empty: "⬚", fail: "❌" };
const STATUS_LABEL: Record<string, string> = { pass: "Pass", empty: "Empty", fail: "Fail" };

function formatTime(iso?: string): string {
  if (!iso) return "-";
  return new Date(iso).toLocaleString();
}

function formatElapsed(s: number): string {
  if (s < 60) return `${s.toFixed(0)}s`;
  return `${(s / 60).toFixed(1)}m`;
}

function sourceFailureTitle(source: DryrunSource): string | undefined {
  const parts = [
    source.recommendedStrategy,
    source.extractorError || source.error,
    source.httpStatus ? `HTTP ${source.httpStatus}` : undefined,
    source.finalUrl || source.sourceUrl,
  ].filter(Boolean);
  return parts.length ? parts.join("\n\n") : undefined;
}

function dedupeByCountryCode(countries: DryrunCountry[]): DryrunCountry[] {
  const seen = new Set<string>();
  return countries.filter((country) => {
    const code = country.countryCode.toLowerCase();
    if (seen.has(code)) return false;
    seen.add(code);
    return true;
  });
}

function ProgressBar({ pct, tone }: { pct: number; tone: "green" | "amber" | "red" }) {
  const colors = { green: "#16a34a", amber: "#d97706", red: "#dc2626" };
  return (
    <span className="dryrun-progress-bar">
      <span className="dryrun-progress-fill" style={{ width: `${Math.max(1, pct)}%`, background: colors[tone] }} />
    </span>
  );
}

export function MsrpDryrunDashboard() {
  const [data, setData] = useState<DryrunDashboard | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [expandedCountry, setExpandedCountry] = useState<string | null>(null);
  const [expandedHistory, setExpandedHistory] = useState<string | null>(null);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const fetchData = useCallback(async () => {
    try {
      const res = await api.getMsrpDryrunDashboard(selectedRunId ?? undefined) as unknown as DryrunDashboard;
      setData(res);
      setError("");
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [selectedRunId]);

  useEffect(() => {
    setLoading(true);
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    if (!autoRefresh) return;
    const id = setInterval(fetchData, 10000);
    return () => clearInterval(id);
  }, [autoRefresh, fetchData]);

  const current = data?.current;
  const history = data?.history ?? [];
  const currentCountries = dedupeByCountryCode(current?.countries ?? []);
  const isHistoricalSelection = Boolean(selectedRunId);

  return (
    <div className="dryrun-dashboard">
      <div className="dryrun-dashboard-head">
        <h3>MSRP Dryrun Monitor</h3>
        <div className="dryrun-dashboard-actions">
          {current?.running && (
            <span className="dryrun-badge dryrun-badge--live">● LIVE</span>
          )}
          <label className="dryrun-auto-refresh">
            <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
            Auto (10s)
          </label>
          {isHistoricalSelection && (
            <button
              type="button"
              className="btn btn-sm btn-secondary"
              onClick={() => {
                setSelectedRunId(null);
                setExpandedCountry(null);
              }}
            >
              Latest
            </button>
          )}
          <button type="button" className="btn btn-sm btn-secondary" onClick={fetchData} disabled={loading}>
            {loading ? "..." : "Refresh"}
          </button>
        </div>
      </div>

      {error && <div className="market-scan-state-card market-scan-state-card--error"><strong>Error</strong><p>{error}</p></div>}

      {/* ── Overall Progress ── */}
      {current?.available && (
        <div className="dryrun-overall">
          <div className="dryrun-overall-bar">
            <div className="dryrun-overall-segments">
              {current.totalPass > 0 && (
                <span className="dryrun-segment is-pass" style={{ flex: current.totalPass }}>
                  {current.totalPass} pass
                </span>
              )}
              {current.totalEmpty > 0 && (
                <span className="dryrun-segment is-empty" style={{ flex: current.totalEmpty }}>
                  {current.totalEmpty} empty
                </span>
              )}
              {current.totalFail > 0 && (
                <span className="dryrun-segment is-fail" style={{ flex: current.totalFail }}>
                  {current.totalFail} fail
                </span>
              )}
            </div>
          </div>
          <div className="dryrun-overall-stats">
            <span><strong>{current.totalSources}</strong> sources</span>
            <span className="is-pass"><strong>{current.totalPass}</strong> pass</span>
            <span className="is-empty"><strong>{current.totalEmpty}</strong> empty</span>
            <span className="is-fail"><strong>{current.totalFail}</strong> fail</span>
            <span><strong>{current.overallPassRate}%</strong> rate</span>
          </div>
          <div className="dryrun-overall-meta">
            {current.runId && <span>Run: {current.runId}</span>}
            {current.batch && <span>Batch: {current.batch}</span>}
            {current.gateStatus && <span>Gate: {current.gateStatus}</span>}
            {current.startedAt && <span>Started: {formatTime(current.startedAt)}</span>}
            {current.logFile && <span>Log: {current.logFile}</span>}
          </div>
        </div>
      )}

      {current && !current.available && (
        <div className="market-scan-empty">{current.reason || "No dryrun data available"}</div>
      )}

      {/* ── Per-Country Grid ── */}
      {currentCountries.length > 0 && (
        <div className="dryrun-countries">
          <h4>Countries ({currentCountries.filter((c) => c.completed).length}/{currentCountries.length} done)</h4>
          <div className="dryrun-country-grid">
            {currentCountries.map((c) => (
              <button
                key={c.countryCode}
                type="button"
                className={`dryrun-country-chip${c.completed ? " is-done" : " is-running"}${expandedCountry === c.countryCode ? " is-expanded" : ""}`}
                onClick={() => setExpandedCountry(expandedCountry === c.countryCode ? null : c.countryCode)}
              >
                <span className="dryrun-country-chip-head">
                  <span className="dryrun-country-flag">{c.completed ? "✅" : "⏳"}</span>
                  <span className="dryrun-country-name">{c.countryLabel}</span>
                  <span className="dryrun-country-rate">{c.passRate}%</span>
                </span>
                <ProgressBar pct={c.passRate} tone={c.passRate >= 50 ? "green" : c.passRate >= 20 ? "amber" : "red"} />
                <span className="dryrun-country-chip-nums">
                  {c.pass}/{c.total} pass · {c.empty} empty · {c.fail} fail
                </span>
                {c.topFailureReason && (
                  <span className="dryrun-country-chip-nums">
                    Top issue: {c.topFailureReason}
                  </span>
                )}

                {/* Expanded source detail panel */}
                {expandedCountry === c.countryCode && (
                  <div className="dryrun-source-panel" onClick={(e) => e.stopPropagation()}>
                    <div className="dryrun-source-panel-head">
                      <span>{c.sources.length} sources</span>
                      <span className="dryrun-source-panel-legend">
                        <span className="is-pass">✅ pass</span>
                        <span className="is-empty">⬚ empty</span>
                        <span className="is-fail">❌ fail</span>
                      </span>
                    </div>
                    <div className="dryrun-source-list">
                      {c.sources.map((s) => {
                        const cleanName = s.sourceCode
                          .replace(/_se_draft_scrapling|_fi_draft_scrapling|_no_draft_scrapling|_dk_draft_scrapling|_hu_draft_scrapling|_hr_draft_scrapling|_at_draft_scrapling|_cz_draft_scrapling|_de_draft_scrapling|_fr_draft_scrapling|_it_draft_scrapling|_pl_draft_scrapling/g, "")
                          .replace(/_/g, " ");
                        const barPct = s.valid > 0 ? Math.min(100, (s.valid / Math.max(s.extracted, 1)) * 100) : 0;
                        return (
                          <div key={s.sourceCode || `${c.countryCode}-${s.index}`} className={`dryrun-source-row is-${s.status}`}>
                            <span className="dryrun-source-icon">{STATUS_ICON[s.status]}</span>
                            <span className="dryrun-source-code" title={s.sourceCode}>{cleanName}</span>
                            <span className="dryrun-source-bar-wrap">
                              <span
                                className={`dryrun-source-bar-fill is-${s.status}`}
                                style={{ width: `${Math.max(2, barPct)}%` }}
                              />
                            </span>
                            <span className="dryrun-source-stat">
                              {s.valid > 0 ? <><strong>{s.valid}</strong> valid</> : <span className="dryrun-source-stat-muted">0</span>}
                            </span>
                            {s.failureReason && (
                              <span className="dryrun-source-elapsed" title={sourceFailureTitle(s)}>{s.failureReason}</span>
                            )}
                            <span className="dryrun-source-elapsed">{formatElapsed(s.elapsedSeconds)}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
              </button>
            ))}
          </div>
        </div>
      )}

      {/* ── History ── */}
      {history.length > 0 && (
        <div className="dryrun-history">
          <h4>History ({history.length} runs, click to expand)</h4>
          <div className="dryrun-history-table-wrap" style={{ maxHeight: expandedHistory ? '60vh' : '240px' }}>
            <table className="dryrun-history-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Batch</th>
                  <th>Sources</th>
                  <th>Pass</th>
                  <th>Empty</th>
                  <th>Fail</th>
                  <th>Rate</th>
                </tr>
              </thead>
              <tbody>
                {history.map((r) => (
                  <Fragment key={r.runId || r.file}>
                    <tr
                      className={`dryrun-history-row${expandedHistory === r.file ? ' is-expanded' : ''}`}
                      onClick={() => {
                        const nextExpanded = expandedHistory === r.file ? null : r.file;
                        setExpandedHistory(nextExpanded);
                        setSelectedRunId(r.runId || null);
                        setExpandedCountry(null);
                      }}
                      style={{ cursor: 'pointer' }}
                    >
                      <td>{formatTime(r.timestamp)}</td>
                      <td>{r.batch}{r.gateStatus ? ` · ${r.gateStatus}` : ""}</td>
                      <td>{r.total}</td>
                      <td className="is-pass">{r.pass}</td>
                      <td className="is-empty">{r.empty}</td>
                      <td className="is-fail">{r.fail}</td>
                      <td><strong>{r.passRate}%</strong></td>
                    </tr>
                    {expandedHistory === r.file && r.countriesDetail && r.countriesDetail.length > 0 && (
                      <tr key={`${r.file}-detail`} className="dryrun-history-detail-row">
                        <td colSpan={7}>
                          <div className="dryrun-history-detail-grid">
                            {r.countriesDetail.map((c) => (
                              <div key={c.countryCode} className="dryrun-history-country-chip">
                                <span className="dryrun-history-country-name">
                                  {c.countryCode.toUpperCase()} {c.countryLabel}
                                </span>
                                <ProgressBar pct={c.passRate} tone={c.passRate >= 50 ? 'green' : c.passRate >= 20 ? 'amber' : 'red'} />
                                <span className="dryrun-history-country-nums">
                                  {c.pass}/{c.total} pass · {c.empty} empty · {c.fail} fail
                                </span>
                              </div>
                            ))}
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
