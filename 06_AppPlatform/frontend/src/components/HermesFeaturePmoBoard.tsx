import { useEffect, useState } from "react";

import { api } from "../api/client";
import type {
  HermesFeatureGoal,
  HermesFeatureGoalChecklistItem,
  HermesFeatureGoalSwimlanesResponse,
  HermesReuseCandidate,
} from "../types/hermes";
import { formatDataManagementTimestamp } from "../utils/dataManagement";

function stateColor(state: string): string {
  if (state === "blocked") return "#dc2626";
  if (state === "ready_for_pr" || state === "in_review") return "#2563eb";
  if (state === "verified" || state === "done") return "#16a34a";
  if (state === "deployed" || state === "tested") return "#0f766e";
  if (state === "implemented" || state === "in_progress") return "#d97706";
  return "#64748b";
}

function stateLabel(state: string): string {
  return state.replace(/_/g, " ");
}

function worktreeStateColor(state: string): string {
  if (state === "dirty") return "#d97706";
  if (state === "clean") return "#16a34a";
  if (state === "missing" || state === "error") return "#dc2626";
  return "#64748b";
}

function scopeStateColor(state: string): string {
  if (state === "out_of_scope" || state === "mixed_scope") return "#dc2626";
  if (state === "unknown") return "#d97706";
  if (state === "in_scope" || state === "clean" || state === "generated_only") return "#16a34a";
  return "#64748b";
}

function scopeStateLabel(state: string): string {
  return state.replace(/_/g, " ");
}

function checkedCount(feature: HermesFeatureGoal): number {
  return feature.checklist.filter((item) => item.checked).length;
}

function ChecklistRow({ item }: { item: HermesFeatureGoalChecklistItem }) {
  const marker = item.checked ? "[x]" : "[ ]";
  const color = item.checked ? "#15803d" : item.declaredChecked ? "#d97706" : "#64748b";
  const source = item.evidenceSources[0] || (item.declaredChecked ? "manual checkbox only" : "missing evidence");
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "34px minmax(0,1fr)",
        gap: 6,
        alignItems: "start",
        borderBottom: "1px solid #e2e8f0",
        padding: "5px 0",
      }}
    >
      <span style={{ fontFamily: "monospace", fontSize: 11, color }}>{marker}</span>
      <div style={{ minWidth: 0 }}>
        <div style={{ fontSize: 11, fontWeight: 700, color: "#0f172a" }}>{item.label}</div>
        <div style={{ fontSize: 10, color, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {source}
        </div>
      </div>
    </div>
  );
}

function ReuseCandidateRow({ candidate }: { candidate: HermesReuseCandidate }) {
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, padding: 8, background: "#fff" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "flex-start" }}>
        <strong style={{ fontSize: 11, color: "#0f172a" }}>{candidate.category}</strong>
        <span style={{ fontSize: 10, color: "#64748b" }}>{candidate.score}</span>
      </div>
      <div style={{ fontSize: 10, color: "#2563eb", marginTop: 3, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {candidate.path}
      </div>
      <div style={{ fontSize: 10, color: "#475569", marginTop: 4 }}>{candidate.reason}</div>
    </div>
  );
}

function FeatureCard({
  feature,
  selected,
  onSelect,
}: {
  feature: HermesFeatureGoal;
  selected: boolean;
  onSelect: () => void;
}) {
  const color = stateColor(feature.state);
  return (
    <button
      type="button"
      onClick={onSelect}
      data-testid={`feature-pmo-card-${feature.featureId}`}
      style={{
        width: "100%",
        borderWidth: 1,
        borderStyle: "solid",
        borderTopColor: selected ? "#2563eb" : "#e2e8f0",
        borderRightColor: selected ? "#2563eb" : "#e2e8f0",
        borderBottomColor: selected ? "#2563eb" : "#e2e8f0",
        borderLeftWidth: 4,
        borderLeftColor: color,
        borderRadius: 6,
        background: selected ? "#eff6ff" : "#fff",
        padding: 9,
        textAlign: "left",
        cursor: "pointer",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
        <strong style={{ fontSize: 12, color: "#0f172a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {feature.title}
        </strong>
        <span style={{ fontSize: 10, color, textTransform: "uppercase", fontWeight: 800, whiteSpace: "nowrap" }}>
          {stateLabel(feature.state)}
        </span>
      </div>
      <div style={{ fontSize: 10, color: "#64748b", marginTop: 4, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
        {feature.featureId}
      </div>
      <div style={{ fontSize: 10, color: "#64748b", marginTop: 5 }}>
        {checkedCount(feature)}/{feature.checklist.length} evidence · {feature.evidenceSummary.openGaps} gaps
      </div>
    </button>
  );
}

function FeatureDetail({ feature }: { feature: HermesFeatureGoal }) {
  const color = stateColor(feature.state);
  const worktreeStatus = feature.worktreeStatus;
  const worktreeColor = worktreeStateColor(worktreeStatus.state);
  const scopeColor = scopeStateColor(worktreeStatus.scopeState);
  const scopeFiles = worktreeStatus.outOfScopeFiles.length > 0
    ? worktreeStatus.outOfScopeFiles
    : worktreeStatus.unknownScopeFiles.length > 0
      ? worktreeStatus.unknownScopeFiles
      : worktreeStatus.generatedFiles;
  const dirtyTotal = worktreeStatus.stagedCount
    + worktreeStatus.modifiedCount
    + worktreeStatus.untrackedCount
    + worktreeStatus.conflictedCount;
  return (
    <div style={{ border: "1px solid #e2e8f0", borderRadius: 8, background: "#f8fafc", padding: 12 }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", marginBottom: 10 }}>
        <div style={{ minWidth: 0 }}>
          <strong style={{ fontSize: 14, color: "#0f172a" }}>{feature.title}</strong>
          <div style={{ fontSize: 11, color: "#64748b", marginTop: 2, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {feature.featureId} · {feature.workstream} · {formatDataManagementTimestamp(feature.lastEventAt)}
          </div>
        </div>
        <span style={{ fontSize: 11, color, fontWeight: 800, textTransform: "uppercase", whiteSpace: "nowrap" }}>
          {stateLabel(feature.state)}
        </span>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(115px,1fr))", gap: 8, marginBottom: 10 }}>
        {[
          ["Events", feature.evidenceSummary.events],
          ["Tests", feature.evidenceSummary.tests],
          ["Evidence", feature.evidenceSummary.evidence],
          ["Gaps", feature.evidenceSummary.openGaps],
          ["Commits", feature.evidenceSummary.commits],
        ].map(([label, value]) => (
          <div key={label} style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8 }}>
            <div style={{ fontSize: 10, color: "#64748b" }}>{label}</div>
            <strong style={{ fontSize: 13 }}>{value}</strong>
          </div>
        ))}
      </div>

      {(feature.branch || feature.linkedWorktree) ? (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 8, marginBottom: 10 }}>
          <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8, minWidth: 0 }}>
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 3 }}>Branch</div>
            <div style={{ fontSize: 11, color: "#0f172a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={feature.branch || "unlinked"}>
              {feature.branch || "unlinked"}
            </div>
          </div>
          <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8, minWidth: 0 }}>
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 3 }}>Worktree</div>
            <div style={{ fontSize: 11, color: feature.linkedWorktree ? "#0f172a" : "#94a3b8", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={feature.linkedWorktree || "No matching local worktree"}>
              {feature.linkedWorktree || "No matching local worktree"}
            </div>
          </div>
          <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8, minWidth: 0 }}>
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 3 }}>Worktree status</div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
              <strong style={{ fontSize: 11, color: worktreeColor, textTransform: "uppercase" }}>{worktreeStatus.state}</strong>
              <span style={{ fontSize: 10, color: "#64748b", whiteSpace: "nowrap" }}>
                {dirtyTotal} dirty
              </span>
            </div>
            <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
              {worktreeStatus.stagedCount} staged · {worktreeStatus.modifiedCount} modified · {worktreeStatus.untrackedCount} untracked
              {worktreeStatus.conflictedCount ? ` · ${worktreeStatus.conflictedCount} conflicts` : ""}
            </div>
            {worktreeStatus.files.length > 0 ? (
              <div style={{ marginTop: 5, display: "grid", gap: 2 }}>
                {worktreeStatus.files.slice(0, 3).map((item) => (
                  <div key={item} style={{ fontSize: 10, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={item}>
                    {item}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
          <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8, minWidth: 0 }}>
            <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 3 }}>Scope</div>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
              <strong style={{ fontSize: 11, color: scopeColor, textTransform: "uppercase" }}>
                {scopeStateLabel(worktreeStatus.scopeState)}
              </strong>
              <span style={{ fontSize: 10, color: "#64748b", whiteSpace: "nowrap" }}>
                {worktreeStatus.scopeWorkstream || "unscoped"}
              </span>
            </div>
            <div style={{ fontSize: 10, color: "#64748b", marginTop: 4 }}>
              {worktreeStatus.inScopeCount} in scope · {worktreeStatus.outOfScopeCount} out of scope · {worktreeStatus.unknownScopeCount} unknown
              {worktreeStatus.generatedCount ? ` · ${worktreeStatus.generatedCount} generated` : ""}
            </div>
            {scopeFiles.length > 0 ? (
              <div style={{ marginTop: 5, display: "grid", gap: 2 }}>
                {scopeFiles.slice(0, 3).map((item) => (
                  <div key={item} style={{ fontSize: 10, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={item}>
                    {item}
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </div>
      ) : null}

      <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#fff", padding: 8, marginBottom: 10 }}>
        <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 3 }}>Next action</div>
        <div style={{ fontSize: 12, color: "#0f172a" }}>{feature.nextAction}</div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(260px,1fr))", gap: 12 }}>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>
            Evidence checklist
          </div>
          <div style={{ background: "#fff", border: "1px solid #e2e8f0", borderRadius: 6, padding: "4px 8px" }}>
            {feature.checklist.map((item) => <ChecklistRow key={item.key} item={item} />)}
          </div>
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>
            Reuse candidates
          </div>
          <div style={{ display: "grid", gap: 6 }}>
            {feature.reuseCandidates.slice(0, 6).map((candidate) => (
              <ReuseCandidateRow key={`${candidate.category}-${candidate.path}`} candidate={candidate} />
            ))}
          </div>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(220px,1fr))", gap: 10, marginTop: 12 }}>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>Docs</div>
          {(feature.sourceDocs.length ? feature.sourceDocs : ["none"]).slice(0, 5).map((item) => (
            <div key={item} style={{ fontSize: 10, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item}</div>
          ))}
        </div>
        <div>
          <div style={{ fontSize: 10, color: "#64748b", fontWeight: 800, textTransform: "uppercase", marginBottom: 4 }}>Files</div>
          {(feature.topFiles.length ? feature.topFiles : ["none"]).slice(0, 5).map((item) => (
            <div key={item} style={{ fontSize: 10, color: "#475569", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{item}</div>
          ))}
        </div>
      </div>
    </div>
  );
}

export function HermesFeaturePmoBoard() {
  const [data, setData] = useState<HermesFeatureGoalSwimlanesResponse | null>(null);
  const [selectedFeatureId, setSelectedFeatureId] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    setLoading(true);
    setError("");
    api.hermesGoalSwimlanes()
      .then((response) => {
        setData(response);
        const first = response.lanes.flatMap((lane) => lane.features)[0] ?? response.features[0];
        setSelectedFeatureId((current) => current || first?.featureId || "");
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="card crud-card" style={{ padding: 16, color: "#64748b", fontSize: 12 }}>Loading Feature PMO Board...</div>;
  }
  if (error) {
    return <div className="card crud-card" style={{ padding: 16, color: "#dc2626", fontSize: 12 }}>{error}</div>;
  }
  if (!data || data.features.length === 0) {
    return (
      <div className="card crud-card" data-testid="hermes-feature-pmo-board" style={{ padding: 16, color: "#64748b", fontSize: 12 }}>
        No Feature PMO goals. Add a feature MD with featureId under Markdown_Readme to start tracking.
      </div>
    );
  }

  const selectedFeature = data.features.find((feature) => feature.featureId === selectedFeatureId) ?? data.features[0];

  return (
    <div data-testid="hermes-feature-pmo-board" style={{ display: "grid", gap: 12 }}>
      <div className="card crud-card" style={{ padding: 12 }}>
        <div className="admin-card-header">
          <div>
            <h2>Feature PMO Board</h2>
            <p>
              {data.summary.total} features · {data.summary.inProgress} in progress · {data.summary.readyForPr} ready for PR · {data.summary.blocked} blocked
            </p>
          </div>
          <span className="badge">{data.summary.workstreamCount} workstreams</span>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit,minmax(min(100%,360px),1fr))", gap: 12, padding: "0 12px 12px", alignItems: "start" }}>
          <div style={{ display: "grid", gap: 10, alignContent: "start", maxHeight: 620, overflow: "auto", paddingRight: 4 }}>
            {data.lanes.map((lane) => (
              <div key={lane.workstream} style={{ display: "grid", gap: 6 }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <strong style={{ fontSize: 10, color: "#64748b", textTransform: "uppercase" }}>{lane.workstream}</strong>
                  <span style={{ fontSize: 10, color: "#64748b" }}>{lane.features.length}</span>
                </div>
                {lane.features.map((feature) => (
                  <FeatureCard
                    key={feature.featureId}
                    feature={feature}
                    selected={feature.featureId === selectedFeature?.featureId}
                    onSelect={() => setSelectedFeatureId(feature.featureId)}
                  />
                ))}
              </div>
            ))}
          </div>
          {selectedFeature ? (
            <div style={{ maxHeight: 620, overflow: "auto", paddingRight: 2 }}>
              <FeatureDetail feature={selectedFeature} />
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
