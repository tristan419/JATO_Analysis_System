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
  runId?: string;
  batch?: string;
  timestamp?: string;
  gateStatus?: string;
  runStatus?: string;
  isLatestRun?: boolean;
  sources: DryrunSource[];
}

interface DryrunCurrent {
  available: boolean;
  running: boolean;
  partial?: boolean;
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

interface StableCoverage {
  gateThreshold: number;
  countryCount: number;
  readyCountryCount: number;
  blockedCountryCount: number;
  stablePassRate: number;
  totalSources: number;
  totalPass: number;
  sourceRowsObserved: number;
  sourceCount: number;
  readySourceCount: number;
  blockedSourceCount: number;
  sourcePassRate: number;
  topFailureReasons: Array<{ reason: string; count: number }>;
  repairSourceSamples: Array<{
    countryCode: string;
    sourceCode: string;
    failureReason?: string;
    recommendedStrategy?: string;
    runId?: string;
  }>;
  probeRegressionCount: number;
  probeRegressionSamples: Array<{
    countryCode: string;
    sourceCode: string;
    activeStatus?: string;
    failureReason?: string;
    recommendedStrategy?: string;
    stableRunId?: string;
    activeRunId?: string;
    lastKnownValid?: number;
  }>;
  latestRunId?: string;
  activeRunId?: string;
  activeRunRunning: boolean;
  activeRunPartial: boolean;
  activeRunPassRate: number;
  probeDiffersFromStableRun: boolean;
  readyCountries: string[];
  blockedCountries: string[];
}

interface DryrunDashboard {
  current: DryrunCurrent;
  allCountries?: DryrunCountry[];
  stableCoverage?: StableCoverage;
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

interface CountryProgressChipProps {
  country: DryrunCountry;
  expanded: boolean;
  onToggle: () => void;
  showRunMeta?: boolean;
}

function CountryProgressChip({
  country,
  expanded,
  onToggle,
  showRunMeta = false,
}: CountryProgressChipProps) {
  const runClass = showRunMeta ? (country.isLatestRun ? " is-latest" : " is-historical") : "";
  return (
    <button
      type="button"
      className={`dryrun-country-chip${country.completed ? " is-done" : " is-running"}${runClass}${expanded ? " is-expanded" : ""}`}
      onClick={onToggle}
    >
      <span className="dryrun-country-chip-head">
        <span className="dryrun-country-flag">{country.completed ? "✅" : "⏳"}</span>
        <span className="dryrun-country-name">{country.countryLabel}</span>
        <span className="dryrun-country-rate">{country.passRate}%</span>
      </span>
      <ProgressBar pct={country.passRate} tone={country.passRate >= 50 ? "green" : country.passRate >= 20 ? "amber" : "red"} />
      <span className="dryrun-country-chip-nums">
        {country.pass}/{country.total} pass · {country.empty} empty · {country.fail} fail
      </span>
      {showRunMeta && (
        <span className="dryrun-country-chip-nums dryrun-country-run-meta">
          {country.isLatestRun ? "Latest run" : "Historical latest"} · {country.batch || country.runId || "-"}
          {country.gateStatus ? ` · gate ${country.gateStatus}` : ""}
          {country.timestamp ? ` · ${formatTime(country.timestamp)}` : ""}
        </span>
      )}
      {country.topFailureReason && (
        <span className="dryrun-country-chip-nums">
          Top issue: {country.topFailureReason}
        </span>
      )}

      {expanded && (
        <div className="dryrun-source-panel" onClick={(event) => event.stopPropagation()}>
          <div className="dryrun-source-panel-head">
            <span>{country.sources.length} sources</span>
            <span className="dryrun-source-panel-legend">
              <span className="is-pass">✅ pass</span>
              <span className="is-empty">⬚ empty</span>
              <span className="is-fail">❌ fail</span>
            </span>
          </div>
          <div className="dryrun-source-list">
            {country.sources.map((source) => {
              const cleanName = source.sourceCode
                .replace(/_[a-z]{2}_draft_scrapling/g, "")
                .replace(/_/g, " ");
              const barPct = source.valid > 0 ? Math.min(100, (source.valid / Math.max(source.extracted, 1)) * 100) : 0;
              return (
                <div key={source.sourceCode || `${country.countryCode}-${source.index}`} className={`dryrun-source-row is-${source.status}`}>
                  <span className="dryrun-source-icon">{STATUS_ICON[source.status]}</span>
                  <span className="dryrun-source-code" title={source.sourceCode}>{cleanName}</span>
                  <span className="dryrun-source-bar-wrap">
                    <span
                      className={`dryrun-source-bar-fill is-${source.status}`}
                      style={{ width: `${Math.max(2, barPct)}%` }}
                    />
                  </span>
                  <span className="dryrun-source-stat">
                    {source.valid > 0 ? <><strong>{source.valid}</strong> valid</> : <span className="dryrun-source-stat-muted">0</span>}
                  </span>
                  {source.failureReason && (
                    <span className="dryrun-source-elapsed" title={sourceFailureTitle(source)}>{source.failureReason}</span>
                  )}
                  <span className="dryrun-source-elapsed">{formatElapsed(source.elapsedSeconds)}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </button>
  );
}

export function MsrpDryrunDashboard() {
  const [data, setData] = useState<DryrunDashboard | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [expandedCountry, setExpandedCountry] = useState<string | null>(null);
  const [expandedAllCountry, setExpandedAllCountry] = useState<string | null>(null);
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
  const allCountries = dedupeByCountryCode(data?.allCountries ?? currentCountries);
  const stableCoverage = data?.stableCoverage;
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
                setExpandedAllCountry(null);
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
          {stableCoverage && stableCoverage.countryCount > 0 && (
            <div className="dryrun-stable-coverage">
              <span><strong>Stable coverage</strong></span>
              <span>
                <strong>{stableCoverage.readyCountryCount}/{stableCoverage.countryCount}</strong> countries &gt;= {stableCoverage.gateThreshold}%
              </span>
              <span><strong>{stableCoverage.stablePassRate}%</strong> stable rate</span>
              {stableCoverage.sourceCount > 0 && (
                <span>
                  Sources: <strong>{stableCoverage.readySourceCount}/{stableCoverage.sourceCount}</strong> pass · {stableCoverage.sourcePassRate}%
                </span>
              )}
              {stableCoverage.topFailureReasons.length > 0 && (
                <span>Top source issue: {stableCoverage.topFailureReasons[0].reason} ({stableCoverage.topFailureReasons[0].count})</span>
              )}
              {stableCoverage.probeRegressionCount > 0 && (
                <span><strong>{stableCoverage.probeRegressionCount}</strong> active probe regressions</span>
              )}
              {stableCoverage.latestRunId && <span>Latest stable: {stableCoverage.latestRunId}</span>}
              {stableCoverage.probeDiffersFromStableRun && stableCoverage.activeRunId && (
                <span>
                  Active probe: {stableCoverage.activeRunId} · {stableCoverage.activeRunPassRate}%
                  {stableCoverage.activeRunPartial ? " · partial" : ""}
                </span>
              )}
            </div>
          )}
        </div>
      )}

      {current && !current.available && (
        <div className="market-scan-empty">{current.reason || "No dryrun data available"}</div>
      )}

      {/* ── Per-Country Grid ── */}
      {currentCountries.length > 0 && (
        <div className="dryrun-countries">
          <h4>Current Run Countries ({currentCountries.filter((c) => c.completed).length}/{currentCountries.length} done)</h4>
          <div className="dryrun-country-grid">
            {currentCountries.map((country) => (
              <CountryProgressChip
                key={country.countryCode}
                country={country}
                expanded={expandedCountry === country.countryCode}
                onToggle={() => setExpandedCountry(expandedCountry === country.countryCode ? null : country.countryCode)}
              />
            ))}
          </div>
        </div>
      )}

      {/* ── All-Country Latest Progress ── */}
      {allCountries.length > 0 && (
        <div className="dryrun-countries dryrun-countries--all">
          <h4>All Country Latest Progress ({allCountries.length} countries)</h4>
          <div className="dryrun-country-grid">
            {allCountries.map((country) => (
              <CountryProgressChip
                key={`all-${country.countryCode}`}
                country={country}
                expanded={expandedAllCountry === country.countryCode}
                onToggle={() => setExpandedAllCountry(expandedAllCountry === country.countryCode ? null : country.countryCode)}
                showRunMeta
              />
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
                        setExpandedAllCountry(null);
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
