import { useEffect, useState } from "react";

import { api } from "../api/client";
import type {
  HermesWorkflowCockpitResponse,
  HermesWorkflowSession,
} from "../types/hermes";
import { formatDataManagementTimestamp } from "../utils/dataManagement";

function riskColor(risk: string): string {
  if (risk === "blocking" || risk === "critical" || risk === "high") return "#dc2626";
  if (risk === "medium") return "#d97706";
  return "#2563eb";
}

function statusText(value: string): string {
  return value.replace(/_/g, " ");
}

function compactSha(value: string): string {
  return value ? value.slice(0, 8) : "";
}

function PillList({ items, empty = "none" }: { items: string[]; empty?: string }) {
  const values = items.length ? items : [empty];
  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
      {values.slice(0, 8).map((item) => (
        <span
          key={item}
          style={{
            maxWidth: 180,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
            fontSize: 10,
            color: "#475569",
            background: "#f1f5f9",
            border: "1px solid #e2e8f0",
            borderRadius: 4,
            padding: "2px 6px",
          }}
        >
          {item}
        </span>
      ))}
    </div>
  );
}

function SessionDetail({ session }: { session: HermesWorkflowSession }) {
  const color = riskColor(session.risk);
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 8, background: "#f8fafc", padding: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", marginBottom: 10 }}>
        <div>
          <strong style={{ fontSize: 14, color: "#0f172a" }}>{session.sessionId}</strong>
          <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
            {session.model} · {formatDataManagementTimestamp(session.latestAt)}
          </div>
        </div>
        <span style={{ fontSize: 11, fontWeight: 800, color, textTransform: "uppercase" }}>
          {statusText(session.status)}
        </span>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(110px,1fr))", gap: 8, marginBottom: 12 }}>
        {[
          ["Events", session.eventCount],
          ["Commits", session.commitCount],
          ["Tests", session.testCount],
          ["Evidence", session.evidenceCount],
          ["Gaps", session.gapCount],
        ].map(([label, value]) => (
          <div key={label} style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
            <div style={{ fontSize: 10, color: "#64748b" }}>{label}</div>
            <strong style={{ fontSize: 13 }}>{value}</strong>
          </div>
        ))}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 10, marginBottom: 12 }}>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>Workstreams</div>
          <PillList items={session.workstreams} />
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>Features</div>
          <PillList items={session.featureIds} />
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>Sources</div>
          <PillList items={session.sources} />
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "minmax(260px,1fr) minmax(260px,1fr)", gap: 12 }}>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 6 }}>Recent events</div>
          <div style={{ display: "grid", gap: 6 }}>
            {session.events.map((event) => (
              <div key={event.eventId} style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8 }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8, marginBottom: 3 }}>
                  <strong
                    style={{
                      fontSize: 11,
                      color: "#0f172a",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {event.title}
                  </strong>
                  <span style={{ fontSize: 10, color: "#64748b", whiteSpace: "nowrap" }}>{compactSha(event.commitSha)}</span>
                </div>
                <div style={{ fontSize: 10, color: "#64748b" }}>
                  {event.source} · {event.type} · {event.workstream}
                </div>
              </div>
            ))}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 6 }}>Files</div>
          <div style={{ display: "grid", gap: 4 }}>
            {(session.topFiles.length ? session.topFiles : ["none"]).map((file) => (
              <div
                key={file}
                style={{
                  fontSize: 11,
                  color: "#475569",
                  background: "#fff",
                  border: "1px solid #e2e8f0",
                  borderRadius: 4,
                  padding: "5px 6px",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {file}
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

export function HermesWorkflowView() {
  const [data, setData] = useState<HermesWorkflowCockpitResponse | null>(null);
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    setError("");
    api.hermesWorkflowCockpit()
      .then((response) => {
        setData(response);
        setSelectedSessionId((current) => current || response.sessions[0]?.sessionId || "");
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="card crud-card" style={{ padding: 16, color: "#64748b", fontSize: 12 }}>Loading workflow cockpit...</div>;
  }
  if (error) {
    return <div className="card crud-card" style={{ padding: 16, color: "#dc2626", fontSize: 12 }}>{error}</div>;
  }
  if (!data) {
    return <div className="card crud-card" style={{ padding: 16, color: "#64748b", fontSize: 12 }}>No workflow data.</div>;
  }

  const selectedSession = data.sessions.find((session) => session.sessionId === selectedSessionId) ?? data.sessions[0];

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div className="card crud-card" style={{ padding: 12 }}>
        <div className="admin-card-header">
          <div>
            <h2>Workflow Cockpit</h2>
            <p>{data.summary.sessionCount} sessions · {data.summary.modelCount} models · {data.summary.totalEvents} events</p>
          </div>
          <span className="badge">{data.summary.blockingSessions} blocking</span>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(130px,1fr))", gap: 8, padding: "0 12px 12px" }}>
          {[
            ["Commits", data.summary.commitCount],
            ["Tests", data.summary.testCount],
            ["Sessions", data.summary.sessionCount],
            ["Models", data.summary.modelCount],
          ].map(([label, value]) => (
            <div key={label} style={{ border: "1px solid #e2e8f0", borderRadius: 6, padding: 8, background: "#fff" }}>
              <div style={{ fontSize: 10, color: "#64748b" }}>{label}</div>
              <strong style={{ fontSize: 14 }}>{value}</strong>
            </div>
          ))}
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "minmax(260px,0.8fr) minmax(320px,1.2fr)", gap: 12, padding: "0 12px 12px" }}>
          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase" }}>Models</div>
            {data.models.map((model) => (
              <div key={model.model} style={{ border: "1px solid #e2e8f0", borderRadius: 6, padding: 8, background: "#fff" }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <strong style={{ fontSize: 12, color: "#0f172a" }}>{model.model}</strong>
                  <span style={{ fontSize: 10, color: "#64748b" }}>{model.sessionCount} sessions</span>
                </div>
                <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
                  {model.eventCount} events · {model.commitCount} commits · {model.testCount} tests
                </div>
                <div style={{ marginTop: 6 }}>
                  <PillList items={model.workstreams} />
                </div>
              </div>
            ))}
          </div>

          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase" }}>Sessions</div>
            <div style={{ display: "grid", gap: 6, maxHeight: 360, overflow: "auto" }}>
              {data.sessions.map((session) => {
                const selected = session.sessionId === selectedSession?.sessionId;
                const color = riskColor(session.risk);
                return (
                  <button
                    key={session.sessionId}
                    type="button"
                    onClick={() => setSelectedSessionId(session.sessionId)}
                    style={{
                      border: selected ? "1px solid #2563eb" : "1px solid #e2e8f0",
                      borderLeft: `4px solid ${color}`,
                      borderRadius: 6,
                      background: selected ? "#eff6ff" : "#fff",
                      padding: 8,
                      textAlign: "left",
                      cursor: "pointer",
                    }}
                  >
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                      <strong
                        style={{
                          fontSize: 12,
                          color: "#0f172a",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {session.sessionId}
                      </strong>
                      <span style={{ fontSize: 10, color, textTransform: "uppercase", fontWeight: 700 }}>
                        {statusText(session.status)}
                      </span>
                    </div>
                    <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
                      {session.model} · {session.eventCount} events · {session.commitCount} commits
                    </div>
                    <div
                      style={{
                        fontSize: 10,
                        color: "#64748b",
                        marginTop: 2,
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {session.lastEventTitle}
                    </div>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {data.reviewItems.length > 0 ? (
        <div style={{ border: "1px solid #e2e8f0", borderRadius: 8, background: "#fff", padding: 12 }}>
          <div
            style={{
              fontSize: 10,
              color: "#64748b",
              fontWeight: 800,
              textTransform: "uppercase",
              marginBottom: 8,
            }}
          >
            Top review items
          </div>
          <div style={{ display: "grid", gap: 6 }}>
            {data.reviewItems.map((item) => (
              <div
                key={`${item.kind}-${item.targetId}`}
                style={{ border: "1px solid #e2e8f0", borderRadius: 6, padding: 8, background: "#f8fafc" }}
              >
                <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                  <strong style={{ fontSize: 12, color: "#0f172a" }}>{item.title}</strong>
                  <span
                    style={{
                      fontSize: 10,
                      color: item.priority === "high" ? "#dc2626" : "#d97706",
                      textTransform: "uppercase",
                      fontWeight: 800,
                    }}
                  >
                    {item.priority}
                  </span>
                </div>
                <div style={{ fontSize: 11, color: "#475569", marginTop: 3 }}>{item.reason}</div>
              </div>
            ))}
          </div>
        </div>
      ) : null}

      {selectedSession ? <SessionDetail session={selectedSession} /> : null}
    </div>
  );
}
