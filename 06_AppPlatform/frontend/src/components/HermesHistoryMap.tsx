import { useEffect, useState } from "react";

import { api } from "../api/client";
import type {
  HermesHistoryCluster,
  HermesHistoryClustersResponse,
  HermesHistoryLevel,
  HermesHistoryYAxis,
} from "../types/hermes";
import { formatDataManagementTimestamp } from "../utils/dataManagement";

const LEVELS: Array<{ value: HermesHistoryLevel; label: string }> = [
  { value: "epic", label: "Epic" },
  { value: "workstream", label: "Workstream" },
  { value: "feature", label: "Feature" },
  { value: "session", label: "Session" },
  { value: "commit", label: "Commit" },
];

const Y_AXES: Array<{ value: HermesHistoryYAxis; label: string }> = [
  { value: "workstream", label: "Workstream" },
  { value: "phase", label: "Phase" },
  { value: "risk", label: "Risk" },
  { value: "session", label: "Session" },
];

const TIMELINE_CARD_HEIGHT = 112;
const TIMELINE_ROW_GAP = 14;
const TIMELINE_CARD_TOP = 30;

interface TimelineDomain {
  startMs: number;
  endMs: number;
  spanMs: number;
}

interface TimelineTick {
  label: string;
  left: number;
}

interface PositionedCluster {
  cluster: HermesHistoryCluster;
  left: number;
  width: number;
  row: number;
}

function clusterColor(cluster: HermesHistoryCluster): string {
  if (cluster.risk === "blocking" || cluster.risk === "critical" || cluster.risk === "high") return "#dc2626";
  if (cluster.risk === "medium") return "#d97706";
  if (cluster.status === "verified" || cluster.status === "resolved") return "#16a34a";
  if (cluster.status === "ready_for_pr") return "#2563eb";
  return "#64748b";
}

function shortDate(value: string): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function timeMs(value: string): number | null {
  if (!value) return null;
  const date = new Date(value);
  const ms = date.getTime();
  return Number.isNaN(ms) ? null : ms;
}

function buildTimelineDomain(clusters: HermesHistoryCluster[]): TimelineDomain | null {
  const times = clusters.flatMap((cluster) => [
    timeMs(cluster.startAt),
    timeMs(cluster.endAt),
  ]).filter((value): value is number => value !== null);
  if (times.length === 0) return null;
  let startMs = Math.min(...times);
  let endMs = Math.max(...times);
  if (startMs === endMs) {
    startMs -= 12 * 60 * 60 * 1000;
    endMs += 12 * 60 * 60 * 1000;
  }
  const padding = Math.max((endMs - startMs) * 0.08, 60 * 60 * 1000);
  startMs -= padding;
  endMs += padding;
  return { startMs, endMs, spanMs: endMs - startMs };
}

function percentForTime(value: number, domain: TimelineDomain): number {
  return Math.min(100, Math.max(0, ((value - domain.startMs) / domain.spanMs) * 100));
}

function buildTimelineTicks(domain: TimelineDomain): TimelineTick[] {
  return Array.from({ length: 5 }, (_, index) => {
    const left = index * 25;
    const value = domain.startMs + (domain.spanMs * left) / 100;
    return {
      left,
      label: shortDate(new Date(value).toISOString()),
    };
  });
}

function clusterTimelinePosition(cluster: HermesHistoryCluster, domain: TimelineDomain, level: HermesHistoryLevel | string): { left: number; width: number } {
  const start = timeMs(cluster.startAt) ?? domain.startMs;
  const end = timeMs(cluster.endAt) ?? start;
  const minWidth = level === "commit" ? 11 : 18;
  const rawLeft = percentForTime(start, domain);
  const rawRight = percentForTime(Math.max(end, start), domain);
  const width = Math.max(minWidth, rawRight - rawLeft);
  const left = Math.min(Math.max(0, rawLeft), 100 - width);
  return { left, width };
}

function positionLaneClusters(
  clusters: HermesHistoryCluster[],
  domain: TimelineDomain,
  level: HermesHistoryLevel | string,
): PositionedCluster[] {
  const rowsRightEdge: number[] = [];
  return [...clusters]
    .sort((a, b) => (timeMs(a.startAt) ?? 0) - (timeMs(b.startAt) ?? 0))
    .map((cluster) => {
      const position = clusterTimelinePosition(cluster, domain, level);
      const row = rowsRightEdge.findIndex((edge) => position.left >= edge + 1);
      const targetRow = row >= 0 ? row : rowsRightEdge.length;
      rowsRightEdge[targetRow] = position.left + position.width;
      return { cluster, left: position.left, width: position.width, row: targetRow };
    });
}

function laneHeight(positionedClusters: PositionedCluster[]): number {
  const rowCount = positionedClusters.reduce((max, item) => Math.max(max, item.row + 1), 1);
  return TIMELINE_CARD_TOP + rowCount * (TIMELINE_CARD_HEIGHT + TIMELINE_ROW_GAP) + 12;
}

function ClusterDetail({ cluster }: { cluster: HermesHistoryCluster }) {
  const color = clusterColor(cluster);
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 8, padding: 12, background: "#f8fafc" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", marginBottom: 10 }}>
        <div>
          <strong style={{ fontSize: 14, color: "#0f172a" }}>{cluster.title}</strong>
          <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
            {formatDataManagementTimestamp(cluster.startAt)} to {formatDataManagementTimestamp(cluster.endAt)}
          </div>
        </div>
        <span style={{ fontSize: 11, fontWeight: 800, color, textTransform: "uppercase" }}>
          {cluster.status.replace(/_/g, " ")}
        </span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(120px,1fr))", gap: 8, marginBottom: 10 }}>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Events</div>
          <strong>{cluster.eventCount}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Commits</div>
          <strong>{cluster.commitCount}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Tests</div>
          <strong>{cluster.testCount}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Risk</div>
          <strong style={{ color }}>{cluster.risk}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Semantic</div>
          <strong>{typeof cluster.semanticScore === "number" ? cluster.semanticScore.toFixed(2) : "-"}</strong>
        </div>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 10 }}>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Semantic signals</div>
          {(cluster.semanticSignals?.length ? cluster.semanticSignals : ["none"]).map((signal) => (
            <span key={signal} style={{ display: "inline-block", margin: "4px 4px 0 0", fontSize: 10, background: "#dbeafe", color: "#1d4ed8", borderRadius: 4, padding: "2px 6px" }}>
              {signal}
            </span>
          ))}
        </div>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Sources</div>
          {cluster.sources.map((source) => (
            <span key={source} style={{ display: "inline-block", margin: "4px 4px 0 0", fontSize: 10, background: "#e2e8f0", borderRadius: 4, padding: "2px 6px" }}>
              {source}
            </span>
          ))}
        </div>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Events</div>
          {cluster.children.slice(0, 10).map((child) => (
            <div key={child} style={{ fontSize: 11, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {child}
            </div>
          ))}
        </div>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Files</div>
          {(cluster.topFiles.length ? cluster.topFiles : ["none"]).slice(0, 10).map((file) => (
            <div key={file} style={{ fontSize: 11, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {file}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function HermesHistoryMap() {
  const [levelIndex, setLevelIndex] = useState(2);
  const [yAxis, setYAxis] = useState<HermesHistoryYAxis>("workstream");
  const [workstream, setWorkstream] = useState("all");
  const [data, setData] = useState<HermesHistoryClustersResponse | null>(null);
  const [selectedClusterId, setSelectedClusterId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const level = LEVELS[levelIndex]?.value ?? "feature";

  useEffect(() => {
    setLoading(true);
    setError("");
    api.hermesHistoryClusters({
      level,
      yAxis,
      workstream: workstream === "all" ? undefined : workstream,
      limit: 160,
    })
      .then((response) => {
        setData(response);
        setSelectedClusterId((current) => (
          current && response.clusters.some((cluster) => cluster.clusterId === current)
            ? current
            : response.clusters[0]?.clusterId ?? ""
        ));
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, [level, yAxis, workstream]);

  const clusters = data?.clusters ?? [];
  const lanes = data?.summary.lanes?.length ? data.summary.lanes : [...new Set(clusters.map((cluster) => cluster.lane))];
  const selectedCluster = clusters.find((cluster) => cluster.clusterId === selectedClusterId) ?? clusters[0];
  const workstreamOptions = Object.keys(data?.summary.workstreams ?? {}).sort();
  const timelineDomain = buildTimelineDomain(clusters);
  const timelineTicks = timelineDomain ? buildTimelineTicks(timelineDomain) : [];

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div className="card crud-card" style={{ padding: 12 }}>
        <div className="admin-card-header">
          <div>
            <h2>Git History Cluster</h2>
            <p>{data?.summary.clusterCount ?? 0} clusters · {data?.summary.totalEvents ?? 0} events · Y: {yAxis} · {data?.summary.semanticMode ?? "bucket_only"}</p>
          </div>
          <span className="badge">{LEVELS[levelIndex]?.label ?? "Feature"}</span>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(min(100%,180px),1fr))", gap: 10, alignItems: "end", padding: "0 12px 12px" }}>
          <div>
            <label style={{ display: "block", fontSize: 10, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 4 }}>
              Cluster detail
            </label>
            <input
              type="range"
              min={0}
              max={LEVELS.length - 1}
              value={levelIndex}
              onChange={(event) => setLevelIndex(Number(event.target.value))}
              style={{ width: "100%" }}
            />
            <div style={{ display: "flex", justifyContent: "space-between", fontSize: 10, color: "#64748b" }}>
              {LEVELS.map((item) => <span key={item.value}>{item.label}</span>)}
            </div>
          </div>
          <div>
            <label style={{ display: "block", fontSize: 10, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 4 }}>
              Y axis
            </label>
            <select
              value={yAxis}
              onChange={(event) => setYAxis(event.target.value as HermesHistoryYAxis)}
              style={{ width: "100%", fontSize: 12, padding: "7px 8px", border: "1px solid #cbd5e1", borderRadius: 6, background: "#fff" }}
            >
              {Y_AXES.map((axis) => <option key={axis.value} value={axis.value}>{axis.label}</option>)}
            </select>
          </div>
          <div>
            <label style={{ display: "block", fontSize: 10, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 4 }}>
              Workstream
            </label>
            <select
              value={workstream}
              onChange={(event) => setWorkstream(event.target.value)}
              style={{ width: "100%", fontSize: 12, padding: "7px 8px", border: "1px solid #cbd5e1", borderRadius: 6, background: "#fff" }}
            >
              <option value="all">All workstreams</option>
              {workstream !== "all" && !workstreamOptions.includes(workstream) ? <option value={workstream}>{workstream}</option> : null}
              {workstreamOptions.map((option) => <option key={option} value={option}>{option}</option>)}
            </select>
          </div>
        </div>

        {loading ? (
          <div style={{ padding: 16, color: "#64748b", fontSize: 12 }}>Loading history clusters...</div>
        ) : error ? (
          <div style={{ padding: 16, color: "#dc2626", fontSize: 12 }}>{error}</div>
        ) : clusters.length === 0 ? (
          <div style={{ padding: 16, color: "#64748b", fontSize: 12 }}>No history clusters.</div>
        ) : !timelineDomain ? (
          <div style={{ padding: 16, color: "#64748b", fontSize: 12 }}>No timeline timestamps available.</div>
        ) : (
          <div style={{ overflowX: "auto", padding: "0 12px 12px" }} data-testid="hermes-history-timeline">
            <div style={{ display: "grid", gap: 8, minWidth: 980 }}>
              <div style={{ display: "grid", gridTemplateColumns: "150px 1fr", gap: 8, alignItems: "end" }}>
                <div style={{ fontSize: 10, fontWeight: 800, color: "#94a3b8", textTransform: "uppercase" }}>Lane</div>
                <div style={{ position: "relative", height: 28, borderBottom: "1px solid #cbd5e1" }}>
                  {timelineTicks.map((tick) => (
                    <div
                      key={`${tick.left}-${tick.label}`}
                      style={{
                        position: "absolute",
                        left: `${tick.left}%`,
                        top: 0,
                        height: "100%",
                        borderLeft: "1px solid #e2e8f0",
                        paddingLeft: 6,
                        fontSize: 10,
                        color: "#64748b",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {tick.label}
                    </div>
                  ))}
                </div>
              </div>
              {lanes.map((lane) => {
                const laneClusters = clusters.filter((cluster) => cluster.lane === lane);
                if (laneClusters.length === 0) return null;
                const positionedClusters = positionLaneClusters(laneClusters, timelineDomain, level);
                return (
                  <div
                    key={lane}
                    data-testid={`hermes-history-lane-${lane}`}
                    style={{ display: "grid", gridTemplateColumns: "150px 1fr", gap: 8, alignItems: "stretch" }}
                  >
                    <div style={{ fontSize: 11, fontWeight: 800, color: "#475569", textTransform: "uppercase", paddingTop: 10 }}>
                      {lane}
                    </div>
                    <div
                      style={{
                        position: "relative",
                        minHeight: laneHeight(positionedClusters),
                        borderLeft: "1px solid #e2e8f0",
                        borderRight: "1px solid #e2e8f0",
                        background: "linear-gradient(90deg, rgba(226,232,240,0.42) 1px, transparent 1px)",
                        backgroundSize: "25% 100%",
                      }}
                    >
                      <div style={{ position: "absolute", left: 0, right: 0, top: 18, borderTop: "1px solid #cbd5e1" }} />
                      {positionedClusters.map(({ cluster, left, width, row }) => {
                        const color = clusterColor(cluster);
                        const selected = cluster.clusterId === selectedCluster?.clusterId;
                        return (
                          <button
                            key={cluster.clusterId}
                            type="button"
                            onClick={() => setSelectedClusterId(cluster.clusterId)}
                            style={{
                              position: "absolute",
                              left: `${left}%`,
                              top: TIMELINE_CARD_TOP + row * (TIMELINE_CARD_HEIGHT + TIMELINE_ROW_GAP),
                              width: `${width}%`,
                              height: TIMELINE_CARD_HEIGHT,
                              maxWidth: 280,
                              borderLeftWidth: 1,
                              borderLeftStyle: "solid",
                              borderLeftColor: selected ? "#2563eb" : "#e2e8f0",
                              borderRightWidth: 1,
                              borderRightStyle: "solid",
                              borderRightColor: selected ? "#2563eb" : "#e2e8f0",
                              borderBottomWidth: 1,
                              borderBottomStyle: "solid",
                              borderBottomColor: selected ? "#2563eb" : "#e2e8f0",
                              borderTopWidth: 4,
                              borderTopStyle: "solid",
                              borderTopColor: color,
                              borderRadius: 8,
                              background: selected ? "#eff6ff" : "#fff",
                              padding: 10,
                              textAlign: "left",
                              cursor: "pointer",
                              boxShadow: selected ? "0 8px 18px rgba(37,99,235,0.15)" : "0 4px 12px rgba(15,23,42,0.06)",
                              overflow: "hidden",
                            }}
                          >
                            <div style={{ fontSize: 10, color: "#64748b", marginBottom: 4 }}>
                              {shortDate(cluster.startAt)}{cluster.startAt !== cluster.endAt ? ` to ${shortDate(cluster.endAt)}` : ""}
                            </div>
                            <div style={{ fontSize: 12, fontWeight: 800, color: "#0f172a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                              {cluster.title}
                            </div>
                            <div style={{ fontSize: 10, color: "#64748b", marginTop: 5 }}>
                              {cluster.eventCount} ev · {cluster.commitCount} commits · {cluster.testCount} tests
                            </div>
                            {cluster.semanticSignals?.[0] ? (
                              <div style={{ fontSize: 10, color: "#1d4ed8", marginTop: 4, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                {cluster.semanticSignals.slice(0, 3).join(" / ")}
                              </div>
                            ) : null}
                            <div style={{ fontSize: 10, color, marginTop: 4, textTransform: "uppercase", fontWeight: 700 }}>
                              {cluster.status.replace(/_/g, " ")}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
      {selectedCluster ? <ClusterDetail cluster={selectedCluster} /> : null}
    </div>
  );
}
