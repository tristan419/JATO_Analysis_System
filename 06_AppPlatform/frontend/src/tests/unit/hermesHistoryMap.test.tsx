// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";

import { api } from "../../api/client";
import { HermesHistoryMap } from "../../components/HermesHistoryMap";

vi.mock("../../api/client", () => ({
  api: {
    hermesHistoryClusters: vi.fn(),
  },
}));

function mockedHistoryClusters(): Mock {
  return api.hermesHistoryClusters as unknown as Mock;
}

function cluster(overrides: Partial<ReturnType<typeof baseCluster>>) {
  return { ...baseCluster(), ...overrides };
}

function baseCluster() {
  return {
    clusterId: "cluster_base",
    level: "feature",
    yAxis: "workstream",
    lane: "Hermes",
    startAt: "2026-06-10T08:00:00Z",
    endAt: "2026-06-10T10:00:00Z",
    title: "Timeline Build",
    workstream: "Hermes",
    phase: "Implemented",
    risk: "low",
    status: "implemented",
    eventCount: 1,
    commitCount: 1,
    testCount: 0,
    evidenceCount: 0,
    gapCount: 0,
    semanticLabel: "Timeline Build",
    semanticScore: 0.72,
    semanticSignals: ["timeline", "cluster"],
    sources: ["git"],
    children: ["evt_timeline"],
    topFiles: ["06_AppPlatform/frontend/src/components/HermesHistoryMap.tsx"],
  };
}

describe("HermesHistoryMap", () => {
  beforeEach(() => {
    mockedHistoryClusters().mockResolvedValue({
      summary: {
        totalEvents: 3,
        sources: { git: 2, devsync: 1 },
        workstreams: { Hermes: 3 },
        risks: { low: 3 },
        models: { "Manual / Git": 2 },
        level: "feature",
        yAxis: "workstream",
        clusterCount: 2,
        lanes: ["Hermes"],
        semanticMode: "feature_file_title_similarity",
      },
      clusters: [
        cluster({ clusterId: "cluster_a" }),
        cluster({
          clusterId: "cluster_b",
          startAt: "2026-06-12T09:00:00Z",
          endAt: "2026-06-12T12:00:00Z",
          title: "Semantic Merge",
          semanticLabel: "Semantic Merge",
          semanticScore: 0.81,
          semanticSignals: ["semantic", "timeline", "axis"],
          children: ["evt_semantic"],
        }),
      ],
    });
  });

  afterEach(() => {
    cleanup();
    vi.clearAllMocks();
  });

  it("renders clusters on a horizontal timeline with semantic signals", async () => {
    render(<HermesHistoryMap />);

    expect(await screen.findByTestId("hermes-history-timeline")).toBeTruthy();
    expect(screen.getByTestId("hermes-history-lane-Hermes")).toBeTruthy();
    expect(screen.getByText(/feature_file_title_similarity/)).toBeTruthy();
    expect(screen.getByText("timeline / cluster")).toBeTruthy();

    await waitFor(() => {
      expect(mockedHistoryClusters()).toHaveBeenCalledWith({
        level: "feature",
        yAxis: "workstream",
        workstream: undefined,
        limit: 160,
      });
    });
  });

  it("updates the detail panel when a timeline cluster is selected", async () => {
    render(<HermesHistoryMap />);

    fireEvent.click(await screen.findByRole("button", { name: /Semantic Merge/i }));

    expect(screen.getByText("0.81")).toBeTruthy();
    expect(screen.getAllByText("semantic").length).toBeGreaterThan(0);
    expect(screen.getByText("evt_semantic")).toBeTruthy();
  });
});
