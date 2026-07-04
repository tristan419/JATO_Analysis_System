import { describe, expect, it } from "vitest";

import indexHtml from "../../../index.html?raw";

describe("route bootstrap script", () => {
  it("applies cached route decisions before React loads", () => {
    const cachedDecisionBranch = indexHtml.match(
      /var cachedDecision = readDecision\(decisionKey\);[\s\S]*?if \(cachedDecision\) \{[\s\S]*?redirectTo\(cachedDecision\.target, cachedDecision\);[\s\S]*?return;[\s\S]*?\}/,
    );

    expect(cachedDecisionBranch?.[0]).toBeTruthy();
    expect(cachedDecisionBranch?.[0]).not.toContain("targetHost(cachedDecision.target) === host");
  });

  it("does not block cross-host early redirects on build fingerprint mismatch", () => {
    expect(indexHtml).not.toContain("applyBuildGuard");
    expect(indexHtml).not.toContain("not verified against current");
  });
});
