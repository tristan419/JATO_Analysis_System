import { useEffect, useState, type FormEvent } from "react";

import { PageBannerStack, PageLoadingShell } from "../../components/PageFeedback";
import { useAuth } from "../../contexts/AuthContext";
import { governanceApi } from "./api";
import { GovernanceStatusBadge } from "./components/GovernanceStatusBadge";
import { TargetDetailDeck } from "./components/TargetDetailDeck";
import type {
  MonitoringTargetCreate,
  MonitoringTargetListItem,
  TargetDetailResponse,
  TargetFilters,
} from "./types";
import "./styles.css";


const EMPTY_FILTERS: TargetFilters = {
  country: "",
  brand: "",
  monitoringStatus: "",
  rosterType: "",
};


function formatDate(value: string | null): string {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(parsed);
}


function gateLabel(item: MonitoringTargetListItem, gate: "sourceGate" | "mappingGate"): string {
  return item.gateSummary?.[gate].status ?? "not_evaluated";
}


export function MsrpSourceGovernancePage() {
  const { user } = useAuth();
  const role = user?.role ?? "viewer";
  const canEdit = ["editor", "admin", "developer"].includes(role);
  const [draftFilters, setDraftFilters] = useState<TargetFilters>(EMPTY_FILTERS);
  const [filters, setFilters] = useState<TargetFilters>(EMPTY_FILTERS);
  const [targets, setTargets] = useState<MonitoringTargetListItem[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [selectedTargetId, setSelectedTargetId] = useState<string | null>(null);
  const [detail, setDetail] = useState<TargetDetailResponse | null>(null);
  const [error, setError] = useState("");
  const [notice, setNotice] = useState("");
  const [showCreateTarget, setShowCreateTarget] = useState(false);
  const [createTarget, setCreateTarget] = useState<MonitoringTargetCreate>({
    country: "",
    brand: "",
    model: "",
    rosterType: "manual",
  });

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setError("");
    governanceApi.listTargets(filters, controller.signal)
      .then((response) => {
        setTargets(response.items);
        setTotal(response.total);
      })
      .catch((requestError: unknown) => {
        if (requestError instanceof DOMException && requestError.name === "AbortError") return;
        setError(requestError instanceof Error ? requestError.message : String(requestError));
      })
      .finally(() => setLoading(false));
    return () => controller.abort();
  }, [filters]);

  useEffect(() => {
    if (!selectedTargetId) {
      setDetail(null);
      return;
    }
    const controller = new AbortController();
    setDetailLoading(true);
    governanceApi.getTarget(selectedTargetId, controller.signal)
      .then(setDetail)
      .catch((requestError: unknown) => {
        if (requestError instanceof DOMException && requestError.name === "AbortError") return;
        setError(requestError instanceof Error ? requestError.message : String(requestError));
      })
      .finally(() => setDetailLoading(false));
    return () => controller.abort();
  }, [selectedTargetId]);

  async function refreshTargets() {
    const response = await governanceApi.listTargets(filters);
    setTargets(response.items);
    setTotal(response.total);
  }

  async function refreshDetail() {
    if (!selectedTargetId) return;
    const response = await governanceApi.getTarget(selectedTargetId);
    setDetail(response);
    await refreshTargets();
  }

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFilters({ ...draftFilters });
  }

  function resetFilters() {
    setDraftFilters(EMPTY_FILTERS);
    setFilters(EMPTY_FILTERS);
  }

  function filterByStatus(status: string) {
    const next = { ...draftFilters, monitoringStatus: status };
    setDraftFilters(next);
    setFilters(next);
  }

  function submitTarget(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError("");
    void governanceApi.createTarget({
      ...createTarget,
      country: createTarget.country.toUpperCase(),
    })
      .then(async (target) => {
        setNotice("Monitoring target created in pending state. Add official evidence before activation.");
        setShowCreateTarget(false);
        setCreateTarget({ country: "", brand: "", model: "", rosterType: "manual" });
        await refreshTargets();
        setSelectedTargetId(target.targetId);
      })
      .catch((requestError: unknown) => {
        setError(requestError instanceof Error ? requestError.message : String(requestError));
      });
  }

  const statusCounts = targets.reduce<Record<string, number>>((result, target) => {
    result[target.monitoringStatus] = (result[target.monitoringStatus] ?? 0) + 1;
    return result;
  }, {});
  const proposalReadyCount = targets.filter((target) => target.openCaseCount > 0).length;
  const mappingBlockedCount = targets.filter(
    (target) => gateLabel(target, "mappingGate") === "fail",
  ).length;

  return (
    <main className="msrp-governance-page">
      <header className="msrp-governance-hero page-header">
        <div>
          <span className="msrp-governance-eyebrow">MSRP self-healing governance</span>
          <h1>Source Governance Console</h1>
          <p>Control official evidence, immutable Source Versions, repair Cases and dual-Gate eligibility. Price monitoring remains a separate result surface.</p>
        </div>
        <div className="msrp-governance-hero-actions">
          <span>Role · {role}</span>
          {canEdit ? (
            <button type="button" className="btn btn-primary" onClick={() => setShowCreateTarget((visible) => !visible)}>
              {showCreateTarget ? "Close target form" : "Add target"}
            </button>
          ) : null}
        </div>
      </header>

      <PageBannerStack
        items={[
          { id: "error", tone: "error", title: "Governance request failed", message: error },
          { id: "notice", tone: "success", message: notice },
          {
            id: "integration",
            tone: "info",
            message: "Feature-local Console is ready. Route registration and Hermes dispatch stay reserved for the integration PR.",
          },
        ]}
      />

      {showCreateTarget ? (
        <form className="msrp-governance-create-target" onSubmit={submitTarget}>
          <header><strong>Add a monitoring target</strong><small>New targets start pending and cannot materialize price facts.</small></header>
          <label>Country ISO<input required minLength={2} maxLength={3} value={createTarget.country} onChange={(event) => setCreateTarget({ ...createTarget, country: event.target.value })} /></label>
          <label>Brand<input required value={createTarget.brand} onChange={(event) => setCreateTarget({ ...createTarget, brand: event.target.value })} /></label>
          <label>Model<input required value={createTarget.model} onChange={(event) => setCreateTarget({ ...createTarget, model: event.target.value })} /></label>
          <label>Trim scope<input value={createTarget.trimScope ?? ""} onChange={(event) => setCreateTarget({ ...createTarget, trimScope: event.target.value })} /></label>
          <label>Powertrain scope<input value={createTarget.powertrainScope ?? ""} onChange={(event) => setCreateTarget({ ...createTarget, powertrainScope: event.target.value })} /></label>
          <label>Owner<input value={createTarget.owner ?? ""} onChange={(event) => setCreateTarget({ ...createTarget, owner: event.target.value })} /></label>
          <button type="submit" className="btn btn-primary">Create pending target</button>
        </form>
      ) : null}

      <section className="msrp-governance-summary" aria-label="Governance status summary">
        <button type="button" onClick={() => filterByStatus("active")}><span>Healthy / active</span><strong>{statusCounts.active ?? 0}</strong><small>Published Source Version</small></button>
        <button type="button" onClick={() => filterByStatus("degraded")}><span>Degraded</span><strong>{statusCounts.degraded ?? 0}</strong><small>Last-known-good retained</small></button>
        <button type="button" onClick={() => filterByStatus("manual_evidence_required")}><span>Manual evidence</span><strong>{statusCounts.manual_evidence_required ?? 0}</strong><small>URL or PDF required</small></button>
        <button type="button" onClick={() => filterByStatus("")}><span>Open Cases</span><strong>{proposalReadyCount}</strong><small>Pattern-level repair queue</small></button>
        <button type="button" onClick={() => filterByStatus("")}><span>Mapping blocked</span><strong>{mappingBlockedCount}</strong><small>Deep-link to Matching Review</small></button>
        <div><span>Total targets</span><strong>{total}</strong><small>Roster is independent of runs</small></div>
      </section>

      <form className="msrp-governance-filters" onSubmit={applyFilters}>
        <label>Country<input value={draftFilters.country} onChange={(event) => setDraftFilters({ ...draftFilters, country: event.target.value })} placeholder="SE" /></label>
        <label>Brand<input value={draftFilters.brand} onChange={(event) => setDraftFilters({ ...draftFilters, brand: event.target.value })} placeholder="Volvo" /></label>
        <label>Status<select value={draftFilters.monitoringStatus} onChange={(event) => setDraftFilters({ ...draftFilters, monitoringStatus: event.target.value })}><option value="">All</option><option value="pending">Pending</option><option value="active">Active</option><option value="degraded">Degraded</option><option value="manual_evidence_required">Manual evidence</option><option value="paused">Paused</option></select></label>
        <label>Roster<select value={draftFilters.rosterType} onChange={(event) => setDraftFilters({ ...draftFilters, rosterType: event.target.value })}><option value="">All</option><option value="country_top30">Country Top30</option><option value="manual">Manual</option><option value="future_roster">Future roster</option></select></label>
        <button type="submit" className="btn btn-primary btn-sm">Apply</button>
        <button type="button" className="btn btn-ghost btn-sm" onClick={resetFilters}>Reset</button>
      </form>

      <section className="msrp-governance-targets">
        <header>
          <div><strong>Monitoring targets</strong><small>{targets.length} shown · {total} total</small></div>
          <span>Select a row to open evidence, versions and repair commands.</span>
        </header>
        <div className="table-wrapper msrp-governance-table-wrap">
          <table className="data-table msrp-governance-table">
            <thead>
              <tr>
                <th>Country / rank</th><th>Target</th><th>Health</th><th>Source Gate</th><th>Mapping Gate</th><th>Cases</th><th>Active version</th><th>Owner</th><th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {targets.map((target) => (
                <tr
                  key={target.targetId}
                  className={selectedTargetId === target.targetId ? "is-selected" : ""}
                  onClick={() => setSelectedTargetId(target.targetId)}
                  tabIndex={0}
                  onKeyDown={(event) => { if (event.key === "Enter" || event.key === " ") setSelectedTargetId(target.targetId); }}
                >
                  <td><strong>{target.country}</strong><small>{target.rosterRank ? `#${target.rosterRank}` : target.rosterType}</small></td>
                  <td><strong>{target.brand} {target.model}</strong><small>{[target.trimScope, target.powertrainScope].filter(Boolean).join(" · ") || "All variants"}</small></td>
                  <td><GovernanceStatusBadge value={target.monitoringStatus} /></td>
                  <td><GovernanceStatusBadge value={gateLabel(target, "sourceGate")} /></td>
                  <td><GovernanceStatusBadge value={gateLabel(target, "mappingGate")} /></td>
                  <td><strong>{target.openCaseCount}</strong><small>{target.manualEvidenceCaseCount ? `${target.manualEvidenceCaseCount} manual` : "No manual block"}</small></td>
                  <td title={target.activeSourceVersionId ?? undefined}>{target.activeSourceVersionId ? `${target.activeSourceVersionId.slice(0, 8)}…` : "—"}</td>
                  <td>{target.owner ?? "Unassigned"}</td>
                  <td>{formatDate(target.updatedAtUtc)}</td>
                </tr>
              ))}
              {!loading && targets.length === 0 ? <tr><td colSpan={9} className="msrp-governance-empty">No targets match the current filters.</td></tr> : null}
            </tbody>
          </table>
        </div>
      </section>

      {loading ? <PageLoadingShell kicker="Governance" label="Loading monitoring targets…" /> : null}
      {detailLoading && !detail ? <PageLoadingShell kicker="Governance" label="Loading target detail…" /> : null}
      {detail ? (
        <TargetDetailDeck
          detail={detail}
          role={role}
          onClose={() => setSelectedTargetId(null)}
          onRefresh={refreshDetail}
          onNotice={setNotice}
          onError={setError}
        />
      ) : null}
    </main>
  );
}
