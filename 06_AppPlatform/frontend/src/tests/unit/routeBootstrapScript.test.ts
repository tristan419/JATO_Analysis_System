import { describe, expect, it } from "vitest";

import indexHtml from "../../../index.html?raw";
import mainEntry from "../../main.tsx?raw";

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

  it("keeps China-local browser signals on www whenever the www probe works", () => {
    expect(indexHtml).toContain("if (cn.ok && profile.prefersChinaRoute) {");
    expect(indexHtml).toContain("www probe succeeded and the browser has China-local signals; keep the domestic route");
    expect(indexHtml).not.toContain("intl.ms + marginMs < cn.ms");
  });

  it("does not wait for the intl probe before booting China-local www traffic", () => {
    expect(indexHtml).toContain('var cnProbePromise = probe("cn");');
    expect(indexHtml).toContain('var intlProbePromise = probe("intl");');
    expect(indexHtml).toContain("if (decisionSettled) return;");
    expect(indexHtml).toContain("cnProbePromise.then(function (cn) {");
    expect(indexHtml).toContain("if (profile.prefersChinaRoute && cn.ok) {");
    expect(indexHtml).toContain("completeDecision(cn, null);");
    expect(indexHtml).toContain("Promise.all([cnProbePromise, intlProbePromise]).then(function (results) {");
  });

  it("keeps the app boot blocked while a cross-host redirect is pending", () => {
    expect(indexHtml).toContain("if (nextHost === host) return false;");
    expect(indexHtml).toContain("return true;");
    expect(indexHtml).toContain("var didRedirect = redirectTo(nextTarget, autoPayload);");
    expect(indexHtml).toContain("if (!didRedirect) {");
    expect(indexHtml).toContain("clearProbeInFlight();");
  });

  it("keeps React out of the static entry imports until route selection settles", () => {
    expect(mainEntry).toContain("await waitForInitialRouteProbe();");
    expect(mainEntry).toContain("const PROBE_INFLIGHT_TTL_MS = 2_200;");
    expect(mainEntry).toContain('import("react")');
    expect(mainEntry).toContain('import("react-dom/client")');
    expect(mainEntry).toContain("React.createElement(");
    expect(mainEntry).not.toMatch(/import\s+[^;]+from\s+["']react["']/);
    expect(mainEntry).not.toContain("<React.StrictMode>");
    expect(mainEntry).not.toContain("<App />");
  });

  it("starts CSS and React route resources in parallel after route selection", () => {
    expect(mainEntry).toContain('const cssPromise = import("./index.css");');
    expect(mainEntry).toContain("const appModulesPromise = Promise.all([");
    expect(mainEntry).toContain("cssPromise,");
    expect(mainEntry).toContain("appModulesPromise,");
  });

  it("renders a static bootstrap placeholder before React starts", () => {
    expect(indexHtml).toContain('<div id="root">');
    expect(indexHtml).toContain('class="bootstrap-loading"');
    expect(indexHtml).toContain("Loading workspace");
    expect(indexHtml).toContain("@keyframes bootstrap-loading-bar");
  });
});
