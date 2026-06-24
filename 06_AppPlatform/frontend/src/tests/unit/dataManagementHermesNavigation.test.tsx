// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi, type Mock } from "vitest";
import { MemoryRouter, useLocation } from "react-router-dom";

import { api } from "../../api/client";
import { DataManagementPage } from "../../pages/DataManagementPage";

vi.mock("../../api/client", () => ({
  api: {
    getDataManagementOverview: vi.fn(),
    getVocManagementOverview: vi.fn(),
    hermesOverview: vi.fn(),
    hermesArchitecture: vi.fn(),
    hermesToolchain: vi.fn(),
    hermesDailySummary: vi.fn(),
    hermesSentinelStatus: vi.fn(),
    hermesActivityHeatmap: vi.fn(),
    hermesEvidenceLedger: vi.fn(),
    hermesPipelineHealth: vi.fn(),
    hermesPipelineStatuses: vi.fn(),
    hermesSourceQuality: vi.fn(),
    hermesMsrpCountryProgress: vi.fn(),
    hermesMsrpDryrunHistory: vi.fn(),
    hermesCost: vi.fn(),
    hermesCostHeatmap: vi.fn(),
    hermesCommandExecute: vi.fn(),
  },
}));

vi.mock("../../components/HermesHistoryMap", () => ({
  HermesHistoryMap: () => <div data-testid="history-map">Git History Cluster Panel</div>,
}));

vi.mock("../../components/HermesFeaturePmoBoard", () => ({
  HermesFeaturePmoBoard: () => <div data-testid="feature-pmo-board">Feature PMO Board</div>,
}));

vi.mock("../../components/HermesProgressSwimlane", () => ({
  HermesProgressSwimlane: () => <div data-testid="progress-swimlane">Progress Swimlane</div>,
}));

vi.mock("../../components/HermesWorkflowView", () => ({
  HermesWorkflowView: () => <div data-testid="workflow-view">Workflow View</div>,
}));

function mockedApiMethod(name: string): Mock {
  return (api as unknown as Record<string, Mock>)[name];
}

function LocationProbe() {
  const location = useLocation();
  return <div data-testid="location">{`${location.pathname}${location.search}${location.hash}`}</div>;
}

function renderDataManagementRoute(route: string) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <DataManagementPage />
      <LocationProbe />
    </MemoryRouter>,
  );
}

function setupApiMocks() {
  mockedApiMethod("getDataManagementOverview").mockResolvedValue({
    generatedAt: "2026-06-12T00:00:00Z",
    domains: [],
    fileInventory: [],
    databaseTables: [],
    database: { connected: false },
    activity: { days: [] },
    airflow: null,
  });
  mockedApiMethod("getVocManagementOverview").mockResolvedValue({
    selectedCountryCode: "",
    availableCountries: [],
  });
  mockedApiMethod("hermesOverview").mockResolvedValue({
    registries: {},
    reports: {},
    proposals: { total: 0, implemented: 0, pending: 0, draft: 0 },
    gaps: { total: 0, open: 0, resolved: 0 },
  });
  mockedApiMethod("hermesArchitecture").mockResolvedValue({
    modules: [],
    dependencies: [],
    routing: [],
  });
  mockedApiMethod("hermesToolchain").mockResolvedValue({
    scripts: [],
    registries: [],
    reports: [],
    workflow: [],
    scriptCount: 0,
    registryCount: 0,
    reportCount: 0,
  });
  mockedApiMethod("hermesDailySummary").mockResolvedValue({
    date: "2026-06-12",
    activityCount: 0,
    costCny: 0,
    dailyBudgetCny: 20,
    monthlyBudgetCny: 500,
    costStatus: "ok",
  });
  mockedApiMethod("hermesSentinelStatus").mockResolvedValue({
    overall: "ok",
    unreadCount: 0,
    notifications: [],
    probes: [],
  });
  mockedApiMethod("hermesActivityHeatmap").mockResolvedValue({
    totalRecords: 0,
    days: [],
    byCommand: {},
    lastRun: null,
  });
  mockedApiMethod("hermesEvidenceLedger").mockResolvedValue({
    totalCount: 0,
    records: [],
    byType: {},
    rangeStart: "",
    rangeEnd: "",
  });
  mockedApiMethod("hermesPipelineHealth").mockResolvedValue({
    generatedAt: "2026-06-12T00:00:00Z",
    pipelines: [],
    summary: {},
  });
  mockedApiMethod("hermesPipelineStatuses").mockResolvedValue([]);
  mockedApiMethod("hermesSourceQuality").mockResolvedValue({
    generatedAt: "2026-06-12T00:00:00Z",
    sources: [],
    summary: {},
  });
  mockedApiMethod("hermesMsrpCountryProgress").mockResolvedValue({
    countries: [],
    summary: {},
  });
  mockedApiMethod("hermesMsrpDryrunHistory").mockResolvedValue({
    runs: [],
    latestRunId: null,
  });
  mockedApiMethod("hermesCost").mockResolvedValue({
    totalCny: 0,
    dailyBudgetCny: 20,
    monthlyBudgetCny: 500,
    monthlyStatus: "ok",
    byModelCny: {},
    bySourceCny: {},
    alerts: [],
    days: [],
  });
  mockedApiMethod("hermesCostHeatmap").mockResolvedValue({
    totalCny: 0,
    dailyBudgetCny: 20,
    monthlyBudgetCny: 500,
    monthlyStatus: "ok",
    byModelCny: {},
    bySourceCny: {},
    alerts: [],
    days: [],
  });
  mockedApiMethod("hermesCommandExecute").mockResolvedValue({});
}

async function expectHermesSubtabActive(label: string) {
  await waitFor(() => {
    expect(screen.getByRole("button", { name: label }).className).toContain("is-active");
  });
}

describe("DataManagementPage Hermes navigation", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    setupApiMocks();
  });

  afterEach(() => {
    cleanup();
  });

  it("defaults view=hermes to Git History Cluster", async () => {
    renderDataManagementRoute("/data/overview?view=hermes");

    expect(await screen.findByTestId("history-map")).toBeTruthy();
    await expectHermesSubtabActive("Git History Cluster");
  });

  it("opens Hermes subtabs from URL tab query", async () => {
    renderDataManagementRoute("/data/overview?view=hermes&tab=capabilities");
    await expectHermesSubtabActive("Capabilities");
    cleanup();

    setupApiMocks();
    renderDataManagementRoute("/data/overview?view=hermes&tab=activity");
    await expectHermesSubtabActive("Activity");
    cleanup();

    setupApiMocks();
    renderDataManagementRoute("/data/overview?view=hermes&tab=cost");
    await expectHermesSubtabActive("Cost");
  });

  it("writes the Hermes subtab to the URL when clicked", async () => {
    renderDataManagementRoute("/data/overview?view=hermes");
    await expectHermesSubtabActive("Git History Cluster");

    fireEvent.click(screen.getByRole("button", { name: "Cost" }));

    await expectHermesSubtabActive("Cost");
    expect(screen.getByTestId("location").textContent).toBe("/data/overview?view=hermes&tab=cost");
  });
});
