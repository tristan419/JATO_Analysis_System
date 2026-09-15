import { describe, expect, it } from "vitest";

import authSource from "../../contexts/AuthContext.tsx?raw";

describe("Auth bootstrap", () => {
  it("keeps auth/me refresh outside the first dashboard load window", () => {
    expect(authSource).toContain("AUTH_PROFILE_REFRESH_DELAY_MS = 30_000");
    expect(authSource).toContain("AUTH_PROFILE_REFRESH_IDLE_TIMEOUT_MS = 8_000");
    expect(authSource).toContain("!isCandidatePreviewOrigin(window.location)");
    expect(authSource).toContain("|| !localStorage.getItem(STORAGE_TOKEN)");
    expect(authSource).not.toContain("AUTH_PROFILE_REFRESH_DELAY_MS = 6_000");
  });
});
