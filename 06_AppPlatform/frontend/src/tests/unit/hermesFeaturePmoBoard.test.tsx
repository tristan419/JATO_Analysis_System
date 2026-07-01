// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";

import { api } from "../../api/client";
import { HermesFeaturePmoBoard } from "../../components/HermesFeaturePmoBoard";
import type { HermesFeatureGoal, HermesFeatureGoalSwimlanesResponse } from "../../types/hermes";

vi.mock("../../api/client", () => ({
  api: {
    hermesGoalSwimlanes: vi.fn(),
  },
}));

function feature(overrides: Partial<HermesFeatureGoal>): HermesFeatureGoal {
  return {
    featureId: "feature.hermes_feature_pmo_cockpit",
    title: "Hermes Feature PMO Cockpit",
    workstream: "Hermes",
    owner: "codex",
    branch: "codex/hermes-feature-pmo-goal-board",
    state: "ready_for_pr",
    blocked: false,
    risk: "low",
    nextAction: "Open the PR and link it back to the feature document.",
    missingEvidence: ["pr_opened"],
    sourceDocs: ["Markdown_Readme/Fullstack/Hermes/HERMES_FEATURE_PMO_GOAL_2026-06-17.md"],
    linkedPrs: [],
    linkedWorktree: "/Users/litristan/Downloads/JATO_Analysis_System_hermes_history_clean",
    worktreeStatus: {
      path: "/Users/litristan/Downloads/JATO_Analysis_System_hermes_history_clean",
      state: "dirty",
      isDirty: true,
      stagedCount: 1,
      modifiedCount: 2,
      untrackedCount: 1,
      deletedCount: 0,
      conflictedCount: 0,
      files: [
        "06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx",
        "06_AppPlatform/backend/app/services/msrp_scraping_service.py",
      ],
      scopeWorkstream: "Hermes",
      scopeState: "mixed_scope",
      inScopeCount: 1,
      outOfScopeCount: 1,
      unknownScopeCount: 0,
      generatedCount: 1,
      inScopeFiles: ["06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx"],
      outOfScopeFiles: ["06_AppPlatform/backend/app/services/msrp_scraping_service.py"],
      unknownScopeFiles: [],
      generatedFiles: ["hermes/sentinel_notifications.jsonl"],
    },
    lastEventAt: "2026-06-17T08:00:00Z",
    lastMeaningfulEvent: "Feature PMO implementation",
    checklist: [
      {
        key: "prd_md_exists",
        label: "PRD / feature MD exists",
        checked: true,
        declaredChecked: true,
        evidenceSources: ["Markdown_Readme/Fullstack/Hermes/HERMES_FEATURE_PMO_GOAL_2026-06-17.md"],
      },
      {
        key: "unit_tests_added",
        label: "Unit tests added or updated",
        checked: true,
        declaredChecked: false,
        evidenceSources: ["test evidence"],
      },
      {
        key: "pr_opened",
        label: "PR opened",
        checked: false,
        declaredChecked: false,
        evidenceSources: [],
      },
    ],
    declaredChecklist: [],
    reuseCandidates: [
      {
        category: "Hermes backend aggregation",
        path: "06_AppPlatform/backend/app/services/hermes_history_service.py",
        reason: "Reuse normalized history/progress readers.",
        score: 8,
        matchedSignals: ["Hermes"],
      },
    ],
    evidenceSummary: {
      events: 2,
      docs: 1,
      tests: 1,
      evidence: 0,
      openGaps: 0,
      commits: 1,
    },
    topFiles: ["06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx"],
    ...overrides,
  };
}

function response(features: HermesFeatureGoal[]): HermesFeatureGoalSwimlanesResponse {
  return {
    summary: {
      total: features.length,
      blocked: features.filter((item) => item.state === "blocked").length,
      readyForPr: features.filter((item) => item.state === "ready_for_pr").length,
      inProgress: features.filter((item) => item.state === "in_progress").length,
      verified: features.filter((item) => item.state === "verified").length,
      workstreamCount: 1,
    },
    features,
    lanes: [{ workstream: "Hermes", features }],
  };
}

function mockedGoalSwimlanes(): Mock {
  return api.hermesGoalSwimlanes as Mock;
}

describe("HermesFeaturePmoBoard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it("renders active feature state, evidence checklist, and reuse candidates", async () => {
    mockedGoalSwimlanes().mockResolvedValue(response([feature({})]));

    render(<HermesFeaturePmoBoard />);

    expect(await screen.findByTestId("hermes-feature-pmo-board")).toBeTruthy();
    expect(screen.getByText("Feature PMO Board")).toBeTruthy();
    expect(screen.getAllByText("Hermes Feature PMO Cockpit").length).toBeGreaterThan(0);
    expect(screen.getByText("Unit tests added or updated")).toBeTruthy();
    expect(screen.getByText("Hermes backend aggregation")).toBeTruthy();
    expect(screen.getByText("codex/hermes-feature-pmo-goal-board")).toBeTruthy();
    expect(screen.getByText("/Users/litristan/Downloads/JATO_Analysis_System_hermes_history_clean")).toBeTruthy();
    expect(screen.getByText("dirty")).toBeTruthy();
    expect(screen.getByText("mixed scope")).toBeTruthy();
    expect(screen.getByText("1 in scope · 1 out of scope · 0 unknown · 1 generated")).toBeTruthy();
    expect(screen.getByText("1 staged · 2 modified · 1 untracked")).toBeTruthy();
    expect(screen.getAllByText("06_AppPlatform/frontend/src/components/HermesFeaturePmoBoard.tsx").length).toBeGreaterThan(0);
    expect(screen.getAllByText("06_AppPlatform/backend/app/services/msrp_scraping_service.py").length).toBeGreaterThan(0);
    expect(screen.getByText("Open the PR and link it back to the feature document.")).toBeTruthy();
  });

  it("switches the detail panel when a different feature is selected", async () => {
    const blocked = feature({
      featureId: "feature.hermes_blocked_goal",
      title: "Hermes Blocked Goal",
      state: "blocked",
      risk: "high",
      nextAction: "Resolve blocking gaps before advancing this feature.",
      evidenceSummary: {
        events: 1,
        docs: 1,
        tests: 0,
        evidence: 0,
        openGaps: 2,
        commits: 0,
      },
    });
    mockedGoalSwimlanes().mockResolvedValue(response([feature({}), blocked]));

    render(<HermesFeaturePmoBoard />);

    await screen.findByText("Hermes Blocked Goal");
    fireEvent.click(screen.getByTestId("feature-pmo-card-feature.hermes_blocked_goal"));

    await waitFor(() => {
      expect(screen.getByText("Resolve blocking gaps before advancing this feature.")).toBeTruthy();
    });
  });

  it("shows an empty state when no feature goals are tracked", async () => {
    mockedGoalSwimlanes().mockResolvedValue(response([]));

    render(<HermesFeaturePmoBoard />);

    expect(await screen.findByText("No Feature PMO goals. Add a feature MD with featureId under Markdown_Readme to start tracking.")).toBeTruthy();
  });
});
