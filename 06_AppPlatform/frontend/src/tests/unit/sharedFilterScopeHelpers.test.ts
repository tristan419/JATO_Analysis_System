import { describe, expect, it } from "vitest";

import { shouldSyncDashboardSearchToLocation } from "../../contexts/SharedFilterScopeContext";

describe("shouldSyncDashboardSearchToLocation", () => {
  it("allows URL sync on dashboard routes that share the global filters", () => {
    expect(shouldSyncDashboardSearchToLocation("/")).toBe(true);
    expect(shouldSyncDashboardSearchToLocation("/specification")).toBe(true);
  });

  it("prevents URL sync on self-managed routes like market scan and country copilot", () => {
    expect(shouldSyncDashboardSearchToLocation("/market-scan")).toBe(false);
    expect(shouldSyncDashboardSearchToLocation("/copilot")).toBe(false);
  });
});
