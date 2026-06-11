import { useEffect, useState } from "react";

import { api } from "../api/client";
import type {
  HermesProgressFeature,
  HermesProgressPhase,
  HermesProgressSwimlaneResponse,
} from "../types/hermes";
import { formatDataManagementTimestamp } from "../utils/dataManagement";

function phaseColor(status: string): string {
  if (status === "complete") return "#16a34a";
  if (status === "attention") return "#d97706";
  return "#cbd5e1";
}

function riskColor(risk: string): string {
  if (risk === "blocking" || risk === "critical" || risk === "high") return "#dc2626";
  if (risk === "medium") return "#d97706";
  return "#2563eb";
}

function statusLabel(status: string): string {
  return status.replace(/_/g, " ");
}

function ProgressPhaseCell({ phase }: { phase: HermesProgressPhase }) {
  const color = phaseColor(phase.status);
  return (
    <div
      title={`${phase.phase}: ${phase.status}${phase.timestamp ? ` · ${formatDataManagementTimestamp(phase.timestamp)}` : ""}`}
      style={{
        minHeight: 34,
        borderRadius: 6,
        border: `1px solid ${phase.status === "pending" ? "#e2e8f0" : color}`,
        background: phase.status === "complete" ? "#f0fdf4" : phase.status === "attention" ? "#fffbeb" : "#f8fafc",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <span style={{ width: 8, height: 8, borderRadius: "50%", background: color, display: "inline-block" }} />
    </div>
  );
}

function FeatureDetail({ feature }: { feature: HermesProgressFeature }) {
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 8, padding: 12, background: "#f8fafc" }}>
      <div style={{ display: "flex", gap: 12, justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
        <div>
          <strong style={{ fontSize: 14, color: "#0f172a" }}>{feature.title}</strong>
          <div style={{ fontSize: 11, color: "#64748b", marginTop: 2 }}>
            {feature.featureId} · {feature.workstream} · {formatDataManagementTimestamp(feature.lastEventAt)}
          </div>
        </div>
        <span style={{ fontSize: 11, fontWeight: 700, color: riskColor(feature.risk), textTransform: "uppercase" }}>
          {feature.risk}
        </span>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(130px,1fr))", gap: 8, marginBottom: 10 }}>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Phase</div>
          <strong style={{ fontSize: 13 }}>{feature.phase}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Evidence</div>
          <strong style={{ fontSize: 13 }}>{feature.evidenceCount}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Tests / Docs</div>
          <strong style={{ fontSize: 13 }}>{feature.testsCount} / {feature.docsCount}</strong>
        </div>
        <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
          <div style={{ fontSize: 10, color: "#64748b" }}>Open gaps</div>
          <strong style={{ fontSize: 13, color: feature.openGapCount ? "#dc2626" : "#16a34a" }}>{feature.openGapCount}</strong>
        </div>
      </div>
      <div style={{ fontSize: 12, color: "#334155", marginBottom: 8 }}>
        {feature.lastMeaningfulEvent || "No recent event recorded."}
      </div>
      <div style={{ fontSize: 12, color: "#0f172a", marginBottom: 8 }}>
        <strong>Next:</strong> {feature.nextAction}
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 8 }}>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Evidence</div>
          {(feature.evidenceRefs.length ? feature.evidenceRefs : ["none"]).slice(0, 6).map((item) => (
            <div key={`e-${item}`} style={{ fontSize: 11, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {item}
            </div>
          ))}
        </div>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Gaps</div>
          {(feature.gapRefs.length ? feature.gapRefs : ["none"]).slice(0, 6).map((item) => (
            <div key={`g-${item}`} style={{ fontSize: 11, color: feature.gapRefs.length ? "#dc2626" : "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {item}
            </div>
          ))}
        </div>
        <div>
          <div style={{ fontSize: 10, fontWeight: 700, color: "#64748b", textTransform: "uppercase" }}>Files</div>
          {(feature.topFiles.length ? feature.topFiles : ["none"]).slice(0, 6).map((item) => (
            <div key={`f-${item}`} style={{ fontSize: 11, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {item}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function HermesProgressSwimlane() {
  const [data, setData] = useState<HermesProgressSwimlaneResponse | null>(null);
  const [selectedFeatureId, setSelectedFeatureId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    setError("");
    api.hermesProgressSwimlanes()
      .then((response) => {
        setData(response);
        const firstFeature = response.lanes.flatMap((lane) => lane.features)[0];
        setSelectedFeatureId((current) => current || firstFeature?.featureId || "");
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="card crud-card" style={{ padding: 16, color: "#64748b", fontSize: 12 }}>Loading progress swimlanes...</div>;
  }
  if (error) {
    return <div className="card crud-card" style={{ padding: 16, color: "#dc2626", fontSize: 12 }}>{error}</div>;
  }
  if (!data) {
    return <div className="card crud-card" style={{ padding: 16, color: "#64748b", fontSize: 12 }}>No progress data.</div>;
  }

  const features = data.lanes.flatMap((lane) => lane.features);
  const selectedFeature = features.find((feature) => feature.featureId === selectedFeatureId) ?? features[0];

  return (
    <div style={{ display: "grid", gap: 12 }}>
      <div className="card crud-card" style={{ padding: 12 }}>
        <div className="admin-card-header">
          <div>
            <h2>Progress Swimlane</h2>
            <p>{data.summary.total} features · {data.summary.blocking} blocking · {data.summary.readyForPr} ready for PR · {data.summary.verified} verified</p>
          </div>
          <span className="badge">{data.summary.workstreamCount} workstreams</span>
        </div>
        <div style={{ overflowX: "auto", padding: "0 12px 12px" }}>
          <div
            style={{
              display: "grid",
              gridTemplateColumns: `minmax(230px,1.3fr) repeat(${data.phases.length}, minmax(88px, 0.7fr)) minmax(120px,0.7fr)`,
              gap: 6,
              alignItems: "stretch",
              minWidth: 860,
            }}
          >
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 700, textTransform: "uppercase" }}>Feature</div>
            {data.phases.map((phase) => (
              <div key={phase} style={{ fontSize: 10, color: "#64748b", fontWeight: 700, textTransform: "uppercase", textAlign: "center" }}>{phase}</div>
            ))}
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 700, textTransform: "uppercase" }}>Status</div>
            {data.lanes.map((lane) => (
              <div key={lane.workstream} style={{ display: "contents" }}>
                <div style={{ gridColumn: "1 / -1", padding: "10px 0 4px", fontSize: 11, fontWeight: 800, color: "#475569", textTransform: "uppercase" }}>
                  {lane.workstream}
                </div>
                {lane.features.map((feature) => {
                  const color = riskColor(feature.risk);
                  const selected = feature.featureId === selectedFeature?.featureId;
                  return (
                    <div key={feature.featureId} style={{ display: "contents" }}>
                      <button
                        type="button"
                        onClick={() => setSelectedFeatureId(feature.featureId)}
                        style={{
                          border: selected ? "1px solid #2563eb" : "1px solid #e2e8f0",
                          borderLeft: `4px solid ${color}`,
                          borderRadius: 6,
                          background: selected ? "#eff6ff" : "#fff",
                          padding: "8px 10px",
                          textAlign: "left",
                          minWidth: 0,
                          cursor: "pointer",
                        }}
                      >
                        <div style={{ fontSize: 12, fontWeight: 700, color: "#0f172a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {feature.title}
                        </div>
                        <div style={{ fontSize: 10, color: "#64748b", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {feature.featureId}
                        </div>
                      </button>
                      {feature.phases.map((phase) => (
                        <ProgressPhaseCell key={`${feature.featureId}-${phase.phase}`} phase={phase} />
                      ))}
                      <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, padding: "7px 8px", background: "#fff", minWidth: 0 }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color }}>{statusLabel(feature.status)}</div>
                        <div style={{ fontSize: 10, color: "#64748b" }}>{feature.commitCount} commits · {feature.openGapCount} gaps</div>
                      </div>
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
        </div>
      </div>
      {selectedFeature ? <FeatureDetail feature={selectedFeature} /> : null}
    </div>
  );
}
