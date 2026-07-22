import { useEffect, useState, type FormEvent } from "react";

import { DeckControlTabs, DeckFloatingDrawer, type DeckControlTabItem } from "../../../components/deckControls";
import { governanceApi } from "../api";
import type {
  GateResult,
  RepairCase,
  RepairCaseDetailResponse,
  RepairDomain,
  SourceVersion,
  TargetDetailResponse,
} from "../types";
import { GovernanceStatusBadge } from "./GovernanceStatusBadge";


type DetailTab = "gates" | "evidence" | "versions" | "repair" | "results";

interface TargetDetailDeckProps {
  detail: TargetDetailResponse;
  role: string;
  onClose: () => void;
  onRefresh: () => Promise<void>;
  onNotice: (message: string) => void;
  onError: (message: string) => void;
}


const TABS: Array<DeckControlTabItem<DetailTab>> = [
  { key: "gates", label: "Gates", caption: "Truth eligibility" },
  { key: "evidence", label: "Evidence", caption: "URL and PDF" },
  { key: "versions", label: "Versions", caption: "Publish / rollback" },
  { key: "repair", label: "Repair", caption: "Cases and Agent" },
  { key: "results", label: "Results", caption: "Correction and FX" },
];

const ALLOWED_TOOLS: Record<RepairDomain, string[]> = {
  source: ["source.url_probe", "source.redirect_check", "source.targeted_dryrun"],
  parser: ["source.targeted_dryrun", "parser.schema_validate", "parser.replay"],
  semantic: ["semantic.classify", "source.targeted_dryrun", "observation.replay"],
  result: ["observation.replay", "gate.evaluate", "result.correction_propose"],
  mapping: ["mapping.candidates", "mapping.shadow_evaluate", "gate.evaluate"],
  fx: ["fx.approved_rate", "fx.recompute", "gate.evaluate"],
  runtime: ["runtime.retry", "runtime.backoff", "source.targeted_dryrun"],
};

const NOT_EVALUATED_GATE: GateResult = {
  status: "fail",
  reasons: ["not_evaluated"],
  policyVersion: "pending-integration",
};


function formatDate(value: string | null | undefined): string {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return new Intl.DateTimeFormat("en-GB", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(parsed);
}


function formatJson(value: Record<string, unknown> | Array<Record<string, unknown>> | null): string {
  if (!value) return "No result recorded";
  return JSON.stringify(value, null, 2);
}


function gateCard(title: string, gate: GateResult | null, purpose: string) {
  return (
    <article className="msrp-governance-gate-card">
      <header>
        <div>
          <span>{title}</span>
          <small>{purpose}</small>
        </div>
        <GovernanceStatusBadge value={gate?.status ?? "not_evaluated"} />
      </header>
      <p>Policy {gate?.policyVersion ?? "not evaluated"}</p>
      {gate?.reasons.length ? (
        <ul>
          {gate.reasons.map((reason) => <li key={reason}>{reason.replaceAll("_", " ")}</li>)}
        </ul>
      ) : (
        <strong>No blocking reasons</strong>
      )}
    </article>
  );
}


function SourceVersionCard({
  version,
  targetRowVersion,
  isAdmin,
  decisionReason,
  working,
  onPublish,
  onRollback,
}: {
  version: SourceVersion;
  targetRowVersion: number;
  isAdmin: boolean;
  decisionReason: string;
  working: boolean;
  onPublish: (version: SourceVersion, targetRowVersion: number, reason: string) => void;
  onRollback: (version: SourceVersion, targetRowVersion: number, reason: string) => void;
}) {
  const canPublish = ["dryrun_passed", "approved"].includes(version.versionStatus);
  const canRollback = version.versionStatus === "published" && Boolean(version.previousVersionId);
  return (
    <article className="msrp-governance-version-card">
      <header>
        <div>
          <strong>Version {version.versionNumber}</strong>
          <small>{version.extractorName} · {version.extractorVersion}</small>
        </div>
        <GovernanceStatusBadge value={version.versionStatus} />
      </header>
      <dl>
        <div><dt>Lane</dt><dd>{version.semanticLane}</dd></div>
        <div><dt>Currency</dt><dd>{version.currency}</dd></div>
        <div><dt>Tax</dt><dd>{version.taxMode}</dd></div>
        <div><dt>Evidence</dt><dd>{version.evidenceRefs.length}</dd></div>
        <div><dt>Published</dt><dd>{formatDate(version.publishedAtUtc)}</dd></div>
        <div><dt>SHA</dt><dd title={version.profileSha256}>{version.profileSha256.slice(0, 12)}…</dd></div>
      </dl>
      <details>
        <summary>Validation and profile</summary>
        <pre>{formatJson(version.validationSummary)}</pre>
        <pre>{formatJson(version.dryrunSummary)}</pre>
        <pre>{version.profileYaml}</pre>
      </details>
      {isAdmin && (canPublish || canRollback) ? (
        <div className="msrp-governance-card-actions">
          {canPublish ? (
            <button
              type="button"
              className="btn btn-primary btn-sm"
              disabled={working || decisionReason.trim().length < 3}
              onClick={() => onPublish(version, targetRowVersion, decisionReason)}
            >
              Publish
            </button>
          ) : null}
          {canRollback ? (
            <button
              type="button"
              className="btn btn-danger btn-sm"
              disabled={working || decisionReason.trim().length < 3}
              onClick={() => onRollback(version, targetRowVersion, decisionReason)}
            >
              Roll back
            </button>
          ) : null}
        </div>
      ) : null}
    </article>
  );
}


export function TargetDetailDeck({
  detail,
  role,
  onClose,
  onRefresh,
  onNotice,
  onError,
}: TargetDetailDeckProps) {
  const [activeTab, setActiveTab] = useState<DetailTab>("gates");
  const [selectedCaseId, setSelectedCaseId] = useState<string | null>(
    detail.repairCases[0]?.caseId ?? null,
  );
  const [caseDetail, setCaseDetail] = useState<RepairCaseDetailResponse | null>(null);
  const [caseLoading, setCaseLoading] = useState(false);
  const [working, setWorking] = useState(false);
  const [urlSource, setUrlSource] = useState("");
  const [officialDomain, setOfficialDomain] = useState("");
  const [sourceType, setSourceType] = useState("official_web");
  const [semanticLane, setSemanticLane] = useState("msrp");
  const [pdfFile, setPdfFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState("");
  const [decisionReason, setDecisionReason] = useState("");

  const canEdit = ["editor", "admin", "developer"].includes(role);
  const isAdmin = ["admin", "developer"].includes(role);

  useEffect(() => {
    const nextCaseId = detail.repairCases.some((item) => item.caseId === selectedCaseId)
      ? selectedCaseId
      : detail.repairCases[0]?.caseId ?? null;
    setSelectedCaseId(nextCaseId);
  }, [detail.item.targetId, detail.repairCases, selectedCaseId]);

  useEffect(() => {
    if (!selectedCaseId) {
      setCaseDetail(null);
      return;
    }
    const controller = new AbortController();
    setCaseLoading(true);
    governanceApi.getCase(selectedCaseId, controller.signal)
      .then(setCaseDetail)
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === "AbortError") return;
        onError(error instanceof Error ? error.message : String(error));
      })
      .finally(() => setCaseLoading(false));
    return () => controller.abort();
  }, [selectedCaseId, onError]);

  async function runMutation(action: () => Promise<void>, successMessage: string) {
    setWorking(true);
    onError("");
    try {
      await action();
      await onRefresh();
      onNotice(successMessage);
    } catch (error) {
      onError(error instanceof Error ? error.message : String(error));
    } finally {
      setWorking(false);
    }
  }

  function submitUrlEvidence(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void runMutation(
      async () => {
        await governanceApi.addUrlEvidence(detail.item.targetId, {
          repairCaseId: selectedCaseId ?? undefined,
          sourceUrl: urlSource,
          officialDomain,
          sourceType,
          semanticLane,
        });
        setUrlSource("");
      },
      "Official URL evidence was recorded as an immutable asset.",
    );
  }

  function submitPdfEvidence(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!pdfFile) return;
    void runMutation(
      async () => {
        await governanceApi.uploadPdfEvidence({
          targetId: detail.item.targetId,
          repairCaseId: selectedCaseId ?? undefined,
          sourceUrl: urlSource,
          officialDomain,
          sourceType: "official_pdf",
          semanticLane,
          file: pdfFile,
          onProgress: (uploaded, total) => setUploadProgress(`${uploaded}/${total} parts`),
        });
        setPdfFile(null);
        setUploadProgress("");
      },
      "Official PDF was verified and stored as immutable evidence.",
    );
  }

  function requestHermes(caseItem: RepairCase) {
    const snapshot = detail.gateSnapshot;
    void runMutation(
      () => governanceApi.requestHermes(caseItem.caseId, {
        sourceGateSnapshot: snapshot?.sourceGate ?? NOT_EVALUATED_GATE,
        mappingGateSnapshot: snapshot?.mappingGate ?? NOT_EVALUATED_GATE,
        fxGateSnapshot: snapshot?.fxGate ?? undefined,
        allowedToolIds: ALLOWED_TOOLS[caseItem.repairDomain],
        authorityPolicyVersion: "msrp-governance-p0",
        composerPolicyVersion: "msrp-composer-p0",
        attemptBudget: 3,
        timeBudgetSeconds: 900,
        tokenBudget: 100000,
        costBudgetUsd: "5.00",
      }),
      "Hermes diagnosis request was created. Dispatch will activate in the integration PR.",
    );
  }

  function publishVersion(version: SourceVersion, rowVersion: number, reason: string) {
    void runMutation(
      () => governanceApi.publishVersion(version.sourceVersionId, rowVersion, reason),
      `Source Version ${version.versionNumber} was published atomically.`,
    );
  }

  function rollbackVersion(version: SourceVersion, rowVersion: number, reason: string) {
    void runMutation(
      () => governanceApi.rollbackVersion(version.sourceVersionId, rowVersion, reason),
      `Source Version ${version.versionNumber} was rolled back to last-known-good.`,
    );
  }

  const target = detail.item;
  const selectedCase = detail.repairCases.find((item) => item.caseId === selectedCaseId) ?? null;
  const gateSnapshot = detail.gateSnapshot;

  return (
    <DeckFloatingDrawer
      open
      onOpenChange={(open) => { if (!open) onClose(); }}
      showTrigger={false}
      triggerPrimary="Governance"
      triggerSecondaryOpen="Close"
      triggerSecondaryClosed="Open"
      eyebrow={`${target.country} · ${target.brand}`}
      title={`${target.model}${target.trimScope ? ` · ${target.trimScope}` : ""}`}
      ariaLabel="MSRP Source Governance target detail"
      className="msrp-governance-detail-drawer"
      panelClassName="msrp-governance-detail-panel"
      bodyClassName="msrp-governance-detail-body"
      footer={
        <div className="msrp-governance-deck-footer">
          <GovernanceStatusBadge value={target.monitoringStatus} />
          <span>Row version {target.rowVersion}</span>
          <span>Updated {formatDate(target.updatedAtUtc)}</span>
        </div>
      }
    >
      <DeckControlTabs
        tabs={TABS}
        activeKey={activeTab}
        onChange={setActiveTab}
        ariaLabel="Governance detail sections"
        className="msrp-governance-tabs"
      />

      {activeTab === "gates" ? (
        <section className="msrp-governance-deck-section">
          <div className="msrp-governance-truth-lanes">
            <article className={gateSnapshot?.eligibleForLocalMaterialization ? "is-pass" : "is-blocked"}>
              <span>Official local MSRP</span>
              <strong>{gateSnapshot?.eligibleForLocalMaterialization ? "Eligible" : "Frozen"}</strong>
              <small>Requires Source + Mapping Gate</small>
            </article>
            <article className={gateSnapshot?.eligibleForNormalizedMaterialization ? "is-pass" : "is-pending"}>
              <span>Derived normalized value</span>
              <strong>{gateSnapshot?.eligibleForNormalizedMaterialization ? "Eligible" : "Pending"}</strong>
              <small>FX never changes the local fact</small>
            </article>
          </div>
          <div className="msrp-governance-gate-grid">
            {gateCard("Source Gate", gateSnapshot?.sourceGate ?? null, "Evidence and deterministic extraction")}
            {gateCard("Mapping Gate", gateSnapshot?.mappingGate ?? null, "Accepted JATO identity")}
            {gateCard("FX Gate", gateSnapshot?.fxGate ?? null, "Derived normalization only")}
          </div>
          <dl className="msrp-governance-target-meta">
            <div><dt>Roster</dt><dd>{target.rosterType} {target.rosterRank ? `#${target.rosterRank}` : ""}</dd></div>
            <div><dt>Owner</dt><dd>{target.owner ?? "Unassigned"}</dd></div>
            <div><dt>Active version</dt><dd>{target.activeSourceVersionId ?? "None"}</dd></div>
            <div><dt>Fallback</dt><dd>{target.fallbackSourceVersionId ?? "None"}</dd></div>
          </dl>
        </section>
      ) : null}

      {activeTab === "evidence" ? (
        <section className="msrp-governance-deck-section">
          {canEdit ? (
            <div className="msrp-governance-evidence-forms">
              <form onSubmit={submitUrlEvidence}>
                <header><strong>Add official URL</strong><small>Creates immutable URL evidence</small></header>
                <label>Official URL<input type="url" required value={urlSource} onChange={(event) => setUrlSource(event.target.value)} /></label>
                <label>Official domain<input required placeholder="volvocars.com" value={officialDomain} onChange={(event) => setOfficialDomain(event.target.value)} /></label>
                <div className="msrp-governance-form-row">
                  <label>Source type<input required value={sourceType} onChange={(event) => setSourceType(event.target.value)} /></label>
                  <label>Semantic lane<input required value={semanticLane} onChange={(event) => setSemanticLane(event.target.value)} /></label>
                </div>
                <button type="submit" className="btn btn-primary btn-sm" disabled={working}>Add URL</button>
              </form>
              <form onSubmit={submitPdfEvidence}>
                <header><strong>Upload official PDF</strong><small>Resumable, MIME and SHA-256 verified</small></header>
                <label>PDF<input type="file" accept="application/pdf,.pdf" required onChange={(event) => setPdfFile(event.target.files?.[0] ?? null)} /></label>
                <small>{uploadProgress || "Uses the URL and domain entered in the URL form."}</small>
                <button type="submit" className="btn btn-secondary btn-sm" disabled={working || !pdfFile || !urlSource || !officialDomain}>Upload PDF</button>
              </form>
            </div>
          ) : null}
          <div className="msrp-governance-stack">
            {detail.evidence.length ? detail.evidence.map((item) => {
              const evidenceUrl = governanceApi.evidenceUrl(item);
              return (
                <article key={item.evidenceAssetId} className="msrp-governance-evidence-card">
                  <header>
                    <div><strong>{item.filename ?? item.evidenceType}</strong><small>{item.sourceType} · {item.semanticLane}</small></div>
                    <GovernanceStatusBadge value={item.officialDomainVerified ? "verified" : "unverified"} />
                  </header>
                  <p title={item.sha256}>SHA-256 {item.sha256.slice(0, 18)}…</p>
                  <small>Captured {formatDate(item.capturedAtUtc)} · {item.sizeBytes ? `${Math.ceil(item.sizeBytes / 1024)} KB` : "URL record"}</small>
                  {evidenceUrl ? <a href={evidenceUrl} target="_blank" rel="noreferrer">Open official evidence</a> : null}
                </article>
              );
            }) : <p className="msrp-governance-empty">No evidence has been recorded.</p>}
          </div>
        </section>
      ) : null}

      {activeTab === "versions" ? (
        <section className="msrp-governance-deck-section">
          {isAdmin ? (
            <label className="msrp-governance-decision-reason">
              Publish / rollback reason
              <input value={decisionReason} onChange={(event) => setDecisionReason(event.target.value)} placeholder="Required for audit" />
            </label>
          ) : null}
          <div className="msrp-governance-stack">
            {detail.sourceVersions.length ? detail.sourceVersions.map((version) => (
              <SourceVersionCard
                key={version.sourceVersionId}
                version={version}
                targetRowVersion={target.rowVersion}
                isAdmin={isAdmin}
                decisionReason={decisionReason}
                working={working}
                onPublish={publishVersion}
                onRollback={rollbackVersion}
              />
            )) : <p className="msrp-governance-empty">No Source Version exists. Evidence and a Dryrun-passed Proposal are required first.</p>}
          </div>
        </section>
      ) : null}

      {activeTab === "repair" ? (
        <section className="msrp-governance-deck-section msrp-governance-repair-layout">
          <div className="msrp-governance-case-list" aria-label="Repair cases">
            {detail.repairCases.length ? detail.repairCases.map((item) => (
              <button
                type="button"
                key={item.caseId}
                className={item.caseId === selectedCaseId ? "is-selected" : ""}
                onClick={() => setSelectedCaseId(item.caseId)}
              >
                <span>{item.repairDomain}</span>
                <strong>{item.caseType}</strong>
                <small>{item.occurrenceCount} occurrences · P{item.priority}</small>
                <GovernanceStatusBadge value={item.caseStatus} />
              </button>
            )) : <p className="msrp-governance-empty">No open repair cases.</p>}
          </div>
          <div className="msrp-governance-case-detail">
            {selectedCase ? (
              <>
                <header>
                  <div><strong>{selectedCase.failureClassifier}</strong><small>Last seen {formatDate(selectedCase.lastSeenAtUtc)}</small></div>
                  <GovernanceStatusBadge value={selectedCase.severity} />
                </header>
                {selectedCase.manualEvidenceRequired ? <p className="msrp-governance-manual-callout">Bounded anti-bot retries stopped. Add an official URL or PDF to continue.</p> : null}
                <div className="msrp-governance-card-actions">
                  {canEdit ? <button type="button" className="btn btn-primary btn-sm" disabled={working} onClick={() => requestHermes(selectedCase)}>Request Hermes</button> : null}
                  {selectedCase.repairDomain === "mapping" ? <a className="btn btn-secondary btn-sm" href={governanceApi.matchingReviewUrl(target)}>Open Matching Review</a> : null}
                </div>
                <dl>
                  <div><dt>Agent runs</dt><dd>{selectedCase.agentRunRefs.length}</dd></div>
                  <div><dt>Evidence</dt><dd>{selectedCase.evidenceRefs.length}</dd></div>
                  <div><dt>Row version</dt><dd>{selectedCase.rowVersion}</dd></div>
                </dl>
                <div className="msrp-governance-proposals">
                  <h4>Proposals {caseLoading ? "· loading" : ""}</h4>
                  {caseDetail?.proposals.length ? caseDetail.proposals.map((proposal) => (
                    <article key={proposal.proposalId}>
                      <header><strong>{proposal.proposalType}</strong><GovernanceStatusBadge value={proposal.proposalStatus} /></header>
                      <p>{proposal.proposalOrigin}{proposal.dpv4Metadata ? " · DPV4 reasoning metadata" : ""}</p>
                      <pre>{formatJson(proposal.fieldDiff)}</pre>
                      {canEdit && proposal.proposalStatus === "dryrun_passed" ? (
                        <button type="button" className="btn btn-primary btn-sm" disabled={working} onClick={() => void runMutation(() => governanceApi.submitProposal(proposal).then(() => undefined), "Proposal submitted for admin approval.")}>Submit Proposal</button>
                      ) : null}
                    </article>
                  )) : <p className="msrp-governance-empty">No Proposal recorded for this Case.</p>}
                </div>
              </>
            ) : <p className="msrp-governance-empty">Select a repair Case.</p>}
          </div>
        </section>
      ) : null}

      {activeTab === "results" ? (
        <section className="msrp-governance-deck-section">
          <div className="msrp-governance-result-rule">
            <strong>Immutable fact rule</strong>
            <span>Result repair creates a correction and controlled rematerialization event. FX repair only recalculates a derived value.</span>
          </div>
          <h4>Result corrections</h4>
          <div className="msrp-governance-stack">
            {detail.resultCorrections.length ? detail.resultCorrections.map((item) => (
              <article key={item.correctionDecisionId} className="msrp-governance-result-card">
                <header><strong>{item.correctionType}</strong><GovernanceStatusBadge value={item.decisionStatus} /></header>
                <p>{item.reason}</p>
                <small>Original observation {item.originalObservationId}</small>
                <small>Replacement {item.replacementObservationId ?? "pending controlled rematerialization"}</small>
              </article>
            )) : <p className="msrp-governance-empty">No result correction decisions.</p>}
          </div>
          <h4>FX normalization</h4>
          <div className="msrp-governance-stack">
            {detail.fxRuns.length ? detail.fxRuns.map((item) => (
              <article key={item.fxRunId} className="msrp-governance-fx-card">
                <header><strong>{item.fxProvider}</strong><GovernanceStatusBadge value={item.runStatus} /></header>
                <div>
                  <span>Official local</span><strong>{item.localValue} {item.localCurrency}</strong>
                  <span>Derived</span><strong>{item.normalizedValue} {item.normalizedCurrency}</strong>
                </div>
                <small>Rate {item.rateToNormalized} · {item.rateEffectiveDate} · policy {item.policyVersion}</small>
              </article>
            )) : <p className="msrp-governance-empty">No FX normalization runs.</p>}
          </div>
        </section>
      ) : null}
    </DeckFloatingDrawer>
  );
}
