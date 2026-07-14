// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { governanceApi } from "../../features/msrp-source-governance/api";
import { MsrpSourceGovernancePage } from "../../features/msrp-source-governance/MsrpSourceGovernancePage";
import type {
  MonitoringTargetListItem,
  PersistedGateDecision,
  RepairCase,
  TargetDetailResponse,
} from "../../features/msrp-source-governance/types";


vi.mock("../../features/msrp-source-governance/api", () => ({
  governanceApi: {
    listTargets: vi.fn(),
    getTarget: vi.fn(),
    createTarget: vi.fn(),
    addUrlEvidence: vi.fn(),
    uploadPdfEvidence: vi.fn(),
    getCase: vi.fn(),
    requestHermes: vi.fn(),
    submitProposal: vi.fn(),
    publishVersion: vi.fn(),
    rollbackVersion: vi.fn(),
    matchingReviewUrl: vi.fn(() => "/data/matching-review?country=SE"),
    evidenceUrl: vi.fn((item: { finalUrl: string | null }) => item.finalUrl),
  },
}));

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: () => ({
    user: { role: "admin" },
  }),
}));


const gateDecision: PersistedGateDecision = {
  schemaVersion: "1.0",
  gateDecisionId: "gate-decision-1",
  targetId: "target-1",
  observationId: "observation-1",
  sourceGate: { status: "pass", reasons: [], policyVersion: "source-v1" },
  mappingGate: { status: "fail", reasons: ["mapping_margin_below_threshold"], policyVersion: "mapping-v1" },
  fxGate: null,
  eligibleForLocalMaterialization: false,
  eligibleForNormalizedMaterialization: false,
  evaluatedAt: "2026-07-14T04:00:00Z",
  evaluationContext: { sourceRunId: "run-1" },
  createdBy: "governance-worker",
  createdAtUtc: "2026-07-14T04:00:00Z",
};

const target: MonitoringTargetListItem = {
  targetId: "target-1",
  targetKey: "SE::volvo::xc60::::",
  country: "SE",
  brand: "Volvo",
  model: "XC60",
  trimScope: null,
  powertrainScope: null,
  rosterType: "country_top30",
  rosterRank: 2,
  monitoringStatus: "manual_evidence_required",
  activeSourceVersionId: null,
  fallbackSourceVersionId: null,
  schedule: null,
  owner: "msrp-ops",
  notes: null,
  rowVersion: 3,
  createdAtUtc: "2026-07-14T03:00:00Z",
  updatedAtUtc: "2026-07-14T04:00:00Z",
  gateSummary: gateDecision,
  openCaseCount: 1,
  manualEvidenceCaseCount: 1,
};

const repairCase: RepairCase = {
  caseId: "case-1",
  repairDomain: "source",
  targetId: "target-1",
  sourceId: null,
  observationId: "observation-1",
  mappingReference: null,
  fxRunId: null,
  caseType: "source_run:failed",
  failureClassifier: "anti_bot",
  severity: "high",
  priority: 80,
  firstSeenAtUtc: "2026-07-14T03:00:00Z",
  lastSeenAtUtc: "2026-07-14T04:00:00Z",
  occurrenceCount: 4,
  recentRunIds: ["run-1"],
  evidenceRefs: [],
  manualEvidenceRequired: true,
  agentRunRefs: [],
  proposalRefs: [],
  caseStatus: "awaiting_evidence",
  resolution: null,
  recurrenceOfCaseId: null,
  owner: null,
  createdBy: "source-repair-service",
  rowVersion: 2,
  createdAtUtc: "2026-07-14T03:00:00Z",
  updatedAtUtc: "2026-07-14T04:00:00Z",
};

const detail: TargetDetailResponse = {
  item: target,
  gateSnapshot: gateDecision,
  evidence: [],
  sourceVersions: [],
  repairCases: [repairCase],
  resultCorrections: [],
  fxRuns: [],
};


describe("MsrpSourceGovernancePage", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(governanceApi.listTargets).mockResolvedValue({
      rows: 1,
      total: 1,
      items: [target],
    });
    vi.mocked(governanceApi.getTarget).mockResolvedValue(detail);
    vi.mocked(governanceApi.getCase).mockResolvedValue({
      item: repairCase,
      proposals: [],
    });
    vi.mocked(governanceApi.addUrlEvidence).mockResolvedValue({
      evidenceAssetId: "evidence-1",
      targetId: "target-1",
      sourceId: null,
      repairCaseId: "case-1",
      evidenceType: "official_url",
      sourceUrl: "https://www.volvocars.com/se/cars/xc60/",
      finalUrl: "https://www.volvocars.com/se/cars/xc60/",
      redirectChain: [],
      officialDomainVerified: true,
      filename: null,
      mimeType: null,
      mimeSignature: null,
      sizeBytes: null,
      storageKey: null,
      sha256: "a".repeat(64),
      capturedAtUtc: "2026-07-14T04:00:00Z",
      documentDate: null,
      validFrom: null,
      validUntil: null,
      pageCount: null,
      contentHash: null,
      textHash: null,
      sourceType: "official_web",
      semanticLane: "msrp",
      lifecycleState: "active",
      createdBy: "admin",
      createdAtUtc: "2026-07-14T04:00:00Z",
    });
    vi.mocked(governanceApi.requestHermes).mockResolvedValue();
  });

  afterEach(() => cleanup());

  it("renders dense Gate and Case state, then opens the selected target deck", async () => {
    render(<MsrpSourceGovernancePage />);

    expect(await screen.findByText("Source Governance Console")).toBeTruthy();
    expect(await screen.findByText("Volvo XC60")).toBeTruthy();
    expect(screen.getAllByText("manual evidence required").length).toBeGreaterThan(0);
    expect(screen.getAllByText("pass").length).toBeGreaterThan(0);
    expect(screen.getAllByText("fail").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("row", { name: /Volvo XC60/ }));

    expect(await screen.findByRole("heading", { name: "XC60" })).toBeTruthy();
    expect(screen.getByText("Official local MSRP")).toBeTruthy();
    expect(screen.getByText("Frozen")).toBeTruthy();
  });

  it("records official URL evidence from the Evidence tab", async () => {
    render(<MsrpSourceGovernancePage />);
    fireEvent.click(await screen.findByRole("row", { name: /Volvo XC60/ }));
    fireEvent.click(await screen.findByRole("tab", { name: /Evidence/ }));

    fireEvent.change(screen.getByLabelText("Official URL"), {
      target: { value: "https://www.volvocars.com/se/cars/xc60/" },
    });
    fireEvent.change(screen.getByLabelText("Official domain"), {
      target: { value: "volvocars.com" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add URL" }));

    await waitFor(() => {
      expect(vi.mocked(governanceApi.addUrlEvidence)).toHaveBeenCalledWith(
        "target-1",
        expect.objectContaining({
          repairCaseId: "case-1",
          officialDomain: "volvocars.com",
          semanticLane: "msrp",
        }),
      );
    });
  });

  it("stops anti-bot automation and creates a bounded Hermes request", async () => {
    render(<MsrpSourceGovernancePage />);
    fireEvent.click(await screen.findByRole("row", { name: /Volvo XC60/ }));
    fireEvent.click(await screen.findByRole("tab", { name: /Repair/ }));

    expect(await screen.findByText(/Bounded anti-bot retries stopped/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Request Hermes" }));

    await waitFor(() => {
      expect(vi.mocked(governanceApi.requestHermes)).toHaveBeenCalledWith(
        "case-1",
        expect.objectContaining({
          allowedToolIds: expect.arrayContaining(["source.url_probe"]),
          authorityPolicyVersion: "msrp-governance-p0",
        }),
      );
    });
  });
});
