import { describe, expect, it } from "vitest";

import type {
  HermesMermaidBlock,
  HermesGap,
  HermesEvidenceLedgerResponse,
  HermesActivityResponse,
  HermesCostResponse,
  HermesOverviewResponse,
  HermesFeatureKanbanResponse,
  HermesArchResponse,
  HermesToolchainResponse,
  HermesDailySummaryResponse,
  HermesPipelineHealthResponse,
  HermesSourceQualityResponse,
  HermesWorkflowCockpitResponse,
} from "../../types/hermes";

// ── Type shape smoke tests ──────────────────────────────────────────
// These verify that the TS interfaces structurally accept payloads
// matching what the backend actually returns.

describe("Hermes type shapes", () => {
  it("HermesMermaidBlock matches backend shape", () => {
    const block: HermesMermaidBlock = {
      file: "Markdown_Readme/Fullstack/WORKFLOWS/ETL.md",
      title: "Monthly pipeline stages",
      diagramIndex: 0,
      raw: "flowchart TD\n  A --> B",
      type: "flowchart",
    };
    expect(block.file).toBeTruthy();
    expect(block.type).toBe("flowchart");
  });

  it("HermesGap accepts open, resolved, in_progress statuses", () => {
    const gap: HermesGap = {
      gapId: "gap.test.x",
      category: "test",
      severity: "medium",
      status: "open",
    };
    expect(gap.status).toBe("open");
    gap.status = "resolved";
    expect(gap.status).toBe("resolved");
    gap.status = "in_progress";
    expect(gap.status).toBe("in_progress");
  });

  it("HermesEvidenceLedgerResponse matches backend shape", () => {
    const resp: HermesEvidenceLedgerResponse = {
      totalCount: 143,
      records: [{ createdAt: "2026-05-15T00:00:00Z", type: "fact", fact: "test" }],
      byType: { fact: 12, event: 3 },
      rangeStart: "2026-05-08T00:00:00Z",
      rangeEnd: "2026-05-15T00:00:00Z",
    };
    expect(resp.totalCount).toBe(143);
    expect(resp.records).toHaveLength(1);
    expect(resp.byType.fact).toBe(12);
    expect(resp.rangeStart).toBeTruthy();
    expect(resp.rangeEnd).toBeTruthy();
  });

  it("HermesEvidenceLedgerResponse empty state", () => {
    const resp: HermesEvidenceLedgerResponse = {
      totalCount: 0,
      records: [],
      byType: {},
      rangeStart: "",
      rangeEnd: "",
    };
    expect(resp.totalCount).toBe(0);
    expect(resp.records).toEqual([]);
    expect(resp.byType).toEqual({});
  });

  it("HermesActivityResponse matches backend shape", () => {
    const resp: HermesActivityResponse = {
      totalRecords: 42,
      days: [{ date: "2026-05-15", count: 3 }],
      byCommand: { "pipeline-audit": 10 },
      lastRun: { command: "pipeline-audit", timestamp: "2026-05-15" },
    };
    expect(resp.days.length).toBeGreaterThan(0);
    expect(resp.byCommand).toBeDefined();
  });

  it("HermesCostResponse handles budget fields", () => {
    const resp: HermesCostResponse = {
      totalCny: 15.5,
      dailyBudgetCny: 20,
      monthlyBudgetCny: 500,
      monthlyStatus: "ok",
      byModelCny: { "deepseek-v4-flash": 10.0 },
      alerts: [],
      days: [{ date: "2026-05-15", costCny: 5.2, overDailyBudget: false }],
    };
    expect(resp.monthlyStatus).toBe("ok");
    expect(resp.alerts).toEqual([]);
  });

  it("HermesOverviewResponse handles all registries", () => {
    const resp: HermesOverviewResponse = {
      registries: { feature: 19, pipeline: 12, source: 7, prompt: 5, artifact: 3 },
      reports: { pipelineHealth: true, sourceQuality: false },
      proposals: { total: 8, implemented: 5, pending: 1, draft: 2 },
      gaps: { total: 13, open: 7, resolved: 4 },
    };
    expect(resp.registries.feature).toBe(19);
    expect(resp.proposals.implemented).toBe(5);
    expect(resp.gaps.open).toBe(7);
  });

  it("HermesFeatureKanbanResponse handles all columns", () => {
    const resp: HermesFeatureKanbanResponse = {
      summary: { total: 19, active: 17, beta: 1, planned: 1, withTests: 5, withIssues: 3 },
      columns: {
        active: { label: "Active", color: "#22c55e", features: [] },
        beta: { label: "Beta", color: "#3b82f6", features: [] },
        planned: { label: "Planned", color: "#f59e0b", features: [] },
        archived: { label: "Archived", color: "#94a3b8", features: [] },
      },
    };
    expect(resp.summary.total).toBe(19);
    expect(resp.columns.active.label).toBe("Active");
  });

  it("HermesArchResponse has modules, deps, routing", () => {
    const resp: HermesArchResponse = {
      modules: [{
        governor: "Code Governor", icon: "code", phase: "Phase 3",
        scripts: ["hermes_code_audit.py"], inputs: ["git diff"],
        outputs: ["audit report"], answers: ["Is this code safe?"], triggers: "on commit",
      }],
      dependencies: [{ from: "Code Governor", to: "Pipeline Governor", what: "audit results" }],
      routing: [{ task: "audit code", ask: "Code Governor", run: "code-audit", gets: "audit report" }],
    };
    expect(resp.modules[0].governor).toBe("Code Governor");
    expect(resp.dependencies[0].what).toBeTruthy();
    expect(resp.routing[0].task).toBeTruthy();
  });

  it("HermesToolchainResponse has workflow steps", () => {
    const resp: HermesToolchainResponse = {
      scripts: [{ name: "hermes_intake.py", path: "03_Scripts/hermes/hermes_intake.py", sizeBytes: 4096 }],
      registries: [{ name: "feature_registry.yaml", path: "hermes/feature_registry.yaml" }],
      reports: [{ name: "pipeline_health.json", path: "hermes/reports/pipeline_health.json" }],
      workflow: [{ step: 1, phase: "Phase 0", script: "asset_map", action: "scan", description: "Scan repo" }],
      scriptCount: 1, registryCount: 1, reportCount: 1,
    };
    expect(resp.workflow[0].step).toBe(1);
    expect(resp.scriptCount).toBe(1);
  });

  it("HermesDailySummaryResponse has cost fields", () => {
    const resp: HermesDailySummaryResponse = {
      date: "2026-05-15",
      activityCount: 5,
      costCny: 3.2,
      dailyBudgetCny: 20,
      monthlyBudgetCny: 500,
      costStatus: "ok",
    };
    expect(resp.costStatus).toBe("ok");
    expect(resp.costCny).toBeLessThan(resp.dailyBudgetCny);
  });

  it("HermesWorkflowCockpitResponse groups model sessions", () => {
    const resp: HermesWorkflowCockpitResponse = {
      summary: {
        totalEvents: 4,
        sessionCount: 2,
        modelCount: 2,
        commitCount: 1,
        testCount: 3,
        blockingSessions: 0,
        latestAt: "2026-06-12T10:00:00Z",
      },
      models: [
        {
          model: "codex",
          sessionCount: 1,
          eventCount: 2,
          commitCount: 1,
          testCount: 3,
          latestAt: "2026-06-12T10:00:00Z",
          workstreams: ["Hermes"],
        },
      ],
      sessions: [
        {
          sessionId: "hermes-session",
          model: "codex",
          status: "ready_for_pr",
          risk: "low",
          latestAt: "2026-06-12T10:00:00Z",
          lastEventTitle: "Workflow cockpit",
          eventCount: 2,
          commitCount: 1,
          testCount: 3,
          evidenceCount: 0,
          gapCount: 0,
          sources: ["git"],
          workstreams: ["Hermes"],
          featureIds: ["proposal.hermes_history_progress_cockpit"],
          topFiles: ["06_AppPlatform/frontend/src/components/HermesWorkflowView.tsx"],
          events: [
            {
              eventId: "git_abc123",
              timestamp: "2026-06-12T10:00:00Z",
              source: "git",
              type: "commit",
              title: "Workflow cockpit",
              featureId: "proposal.hermes_history_progress_cockpit",
              workstream: "Hermes",
              commitSha: "abc123",
            },
          ],
        },
      ],
      reviewItems: [],
    };
    expect(resp.sessions[0].model).toBe("codex");
    expect(resp.summary.sessionCount).toBe(2);
  });

  it("HermesMermaidBlock handles different diagram types", () => {
    const flowchart: HermesMermaidBlock = { file: "a.md", title: "", diagramIndex: 0, raw: "flowchart TD\n  A", type: "flowchart" };
    const seq: HermesMermaidBlock = { file: "b.md", title: "", diagramIndex: 0, raw: "sequenceDiagram\n  A->>B: hi", type: "sequenceDiagram" };
    const gantt: HermesMermaidBlock = { file: "c.md", title: "", diagramIndex: 0, raw: "gantt\n  section A", type: "gantt" };
    expect(flowchart.type).toBe("flowchart");
    expect(seq.type).toBe("sequenceDiagram");
    expect(gantt.type).toBe("gantt");
  });
});
