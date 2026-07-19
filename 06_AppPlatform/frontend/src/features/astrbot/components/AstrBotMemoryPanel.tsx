import { useEffect, useState } from "react";
import { compareAgentRuns, deleteAgentRun, fetchAgentRuns, fetchAgentMemoryStats } from "../astrbotApi";
import type { AgentRunRecord, AgentMemoryStats } from "../astrbotConfig";

interface MemoryPanelProps {
  skills: { id: string; name: string }[];
  totalRuns: number;
}

type FilterKey = "skillId" | "country" | "mode";

function formatTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch {
    return iso;
  }
}

function truncateQuestion(q: string, max = 60): string {
  return q.length > max ? `${q.slice(0, max)}…` : q;
}

export function AstrBotMemoryPanel({ skills, totalRuns }: MemoryPanelProps) {
  const [runs, setRuns] = useState<AgentRunRecord[]>([]);
  const [runTotal, setRunTotal] = useState(totalRuns);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [stats, setStats] = useState<AgentMemoryStats | null>(null);
  const [filters, setFilters] = useState<Record<FilterKey, string>>({ skillId: "", country: "", mode: "" });
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [compareIds, setCompareIds] = useState<string[]>([]);
  const [compareResult, setCompareResult] = useState<{
    runIds: string[];
    found: string[];
    missing: string[];
    comparison: { field: string; [key: string]: unknown }[];
    runs: AgentRunRecord[];
  } | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);

  const selectedRun = runs.find(r => r.runId === selectedRunId) ?? null;

  async function loadRuns() {
    setLoading(true);
    setError(null);
    try {
      const active: Record<string, string> = {};
      if (filters.skillId) active.skillId = filters.skillId;
      if (filters.country) active.country = filters.country;
      if (filters.mode) active.mode = filters.mode;
      const result = await fetchAgentRuns({ ...active, limit: 30 });
      setRuns(result.items);
      setRunTotal(result.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }

  async function loadStats() {
    try {
      setStats(await fetchAgentMemoryStats());
    } catch {
      // stats are best-effort
    }
  }

  useEffect(() => {
    void loadRuns();
    void loadStats();
  }, []);

  useEffect(() => {
    void loadRuns();
  }, [filters.skillId, filters.country, filters.mode]);

  async function handleDelete(runId: string) {
    setDeleting(runId);
    try {
      await deleteAgentRun(runId);
      if (selectedRunId === runId) setSelectedRunId(null);
      setCompareIds(prev => prev.filter(id => id !== runId));
      setCompareResult(null);
      await loadRuns();
      await loadStats();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setDeleting(null);
    }
  }

  function toggleCompare(runId: string) {
    setCompareIds(prev => {
      if (prev.includes(runId)) return prev.filter(id => id !== runId);
      if (prev.length >= 3) return prev;
      return [...prev, runId];
    });
    setCompareResult(null);
  }

  async function runCompare() {
    if (compareIds.length < 2) return;
    setError(null);
    try {
      setCompareResult(await compareAgentRuns(compareIds));
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  }

  const filterOptions: { key: FilterKey; label: string; options: { value: string; label: string }[] }[] = [
    {
      key: "skillId",
      label: "Skill",
      options: [
        { value: "", label: "All Skills" },
        ...skills.map(s => ({ value: s.id, label: s.name })),
      ],
    },
    {
      key: "country",
      label: "Country",
      options: [
        { value: "", label: "All Countries" },
        ...Object.keys(stats?.byCountry ?? {}).map(c => ({ value: c, label: c })),
      ],
    },
    {
      key: "mode",
      label: "Mode",
      options: [
        { value: "", label: "All Modes" },
        { value: "chart", label: "Chart" },
        { value: "snapshot", label: "Snapshot" },
        { value: "pricing", label: "Pricing" },
        { value: "news", label: "News" },
        { value: "variant", label: "Variant" },
      ],
    },
  ];

  return (
    <div className="astrbot-memory-panel">
      {/* Stats bar */}
      {stats ? (
        <section className="astrbot-memory-stats" aria-label="Memory stats">
          <div>
            <span>Total Runs</span>
            <strong>{stats.totalRuns} / {stats.maxRuns}</strong>
          </div>
          <div>
            <span>Top Skill</span>
            <strong>{Object.entries(stats.bySkill).sort((a, b) => b[1] - a[1])[0]?.[0] ?? "—"}</strong>
          </div>
          <div>
            <span>Top Country</span>
            <strong>{Object.entries(stats.byCountry).sort((a, b) => b[1] - a[1])[0]?.[0] ?? "—"}</strong>
          </div>
          <div>
            <span>Latest Run</span>
            <strong>{stats.latestRunAt ? formatTime(stats.latestRunAt) : "—"}</strong>
          </div>
        </section>
      ) : null}

      {/* Filters */}
      <section className="astrbot-memory-filters" aria-label="Run filters">
        {filterOptions.map(f => (
          <label key={f.key}>
            <span>{f.label}</span>
            <select
              value={filters[f.key]}
              onChange={e => setFilters(prev => ({ ...prev, [f.key]: e.target.value }))}
            >
              {f.options.map(o => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          </label>
        ))}
        <button type="button" className="astrbot-native-toggle" onClick={() => void loadRuns()}>
          Refresh
        </button>
      </section>

      {error ? <div className="astrbot-status-error" role="status">{error}</div> : null}

      {/* Run list */}
      <section className="astrbot-memory-table-shell" aria-label="Agent run history">
        {loading ? (
          <div className="astrbot-table-empty">Loading run history…</div>
        ) : runs.length === 0 ? (
          <div className="astrbot-table-empty">
            No runs recorded yet. Run an agent request from the Agent tab to populate history.
          </div>
        ) : (
          <table className="astrbot-table">
            <thead>
              <tr>
                <th>Compare</th>
                <th>Time</th>
                <th>Skill</th>
                <th>Country</th>
                <th>Tool</th>
                <th>Question</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {runs.map(run => (
                <tr key={run.runId} className={selectedRunId === run.runId ? "is-selected" : ""}>
                  <td>
                    <input
                      type="checkbox"
                      checked={compareIds.includes(run.runId)}
                      onChange={() => toggleCompare(run.runId)}
                    />
                  </td>
                  <td onClick={() => setSelectedRunId(selectedRunId === run.runId ? null : run.runId)}>
                    {formatTime(run.createdAt)}
                  </td>
                  <td>{run.skillName}</td>
                  <td>{run.country}</td>
                  <td><code>{run.selectedTool}</code></td>
                  <td title={run.question}>{truncateQuestion(run.question)}</td>
                  <td>
                    <button
                      type="button"
                      className="astrbot-chip-button"
                      disabled={deleting === run.runId}
                      onClick={() => void handleDelete(run.runId)}
                    >
                      {deleting === run.runId ? "…" : "Del"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      {/* Compare bar */}
      {compareIds.length >= 2 ? (
        <section className="astrbot-memory-compare-bar">
          <span>{compareIds.length} runs selected</span>
          <button type="button" className="astrbot-primary-action" onClick={() => void runCompare()}>
            Compare
          </button>
          <button type="button" className="astrbot-chip-button" onClick={() => { setCompareIds([]); setCompareResult(null); }}>
            Clear
          </button>
        </section>
      ) : null}

      {/* Compare result */}
      {compareResult ? (
        <section className="astrbot-memory-compare-result" aria-label="Comparison result">
          <h3>Comparison</h3>
          {compareResult.missing.length > 0 ? (
            <div className="astrbot-status-error">Missing runs: {compareResult.missing.join(", ")}</div>
          ) : null}
          <div className="astrbot-table-shell">
            <table className="astrbot-table">
              <thead>
                <tr>
                  <th>Field</th>
                  {compareResult.runs.map(r => (
                    <th key={r.runId}>{r.skillName} / {r.country}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {compareResult.comparison.map(row => (
                  <tr key={row.field}>
                    <td><strong>{row.field}</strong></td>
                    {compareResult.runIds.map(rid => (
                      <td key={rid}>{String(row[rid] ?? "—")}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* Run detail */}
      {selectedRun ? (
        <section className="astrbot-memory-detail" aria-label="Run detail">
          <h3>Run Detail</h3>
          <div className="astrbot-agent-card-grid">
            <div className="astrbot-agent-card">
              <span>Run ID</span>
              <strong>{selectedRun.runId}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Time</span>
              <strong>{formatTime(selectedRun.createdAt)}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Skill</span>
              <strong>{selectedRun.skillName}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Country</span>
              <strong>{selectedRun.country}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Mode</span>
              <strong>{selectedRun.mode}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Tool</span>
              <strong>{selectedRun.selectedTool}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Evidence Source</span>
              <strong>{selectedRun.evidenceSource}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Evidence Count</span>
              <strong>{selectedRun.evidenceCount}</strong>
            </div>
          </div>
          <div className="astrbot-memory-detail-question">
            <span>Question</span>
            <p>{selectedRun.question}</p>
          </div>
          <div className="astrbot-memory-detail-summary">
            <span>Result Summary</span>
            <p>{selectedRun.resultSummary}</p>
          </div>
          {selectedRun.displayCards.length > 0 ? (
            <div className="astrbot-agent-card-grid">
              {selectedRun.displayCards.map(card => (
                <div className="astrbot-agent-card" key={card.label}>
                  <span>{card.label}</span>
                  <strong>{card.value}</strong>
                </div>
              ))}
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}
