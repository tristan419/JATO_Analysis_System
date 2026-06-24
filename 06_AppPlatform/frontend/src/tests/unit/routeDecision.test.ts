import { describe, expect, it } from "vitest";

import {
  DECISION_KEY,
  MANUAL_KEY,
  buildRouteRedirectUrl,
  chooseAutoRoute,
  consumeRouteDecisionTransfer,
  createAutoRouteDecision,
  createClientRouteProfile,
  createManualRouteDecision,
  isRouteProbeInFlight,
  makeInitialProbe,
  readRouteDecision,
  shouldSkipSmartRoute,
  type ProbeResult,
} from "../../utils/routeDecision";

function createStorage(): Storage {
  const values = new Map<string, string>();
  return {
    get length() {
      return values.size;
    },
    clear() {
      values.clear();
    },
    getItem(key: string) {
      return values.get(key) ?? null;
    },
    key(index: number) {
      return Array.from(values.keys())[index] ?? null;
    },
    removeItem(key: string) {
      values.delete(key);
    },
    setItem(key: string, value: string) {
      values.set(key, value);
    },
  };
}

function okProbe(target: "cn" | "intl", ms: number): ProbeResult {
  return {
    ...makeInitialProbe(target),
    status: "ok",
    ms,
    checkedAt: "10:00:00",
  };
}

describe("route decision helpers", () => {
  it("prefers www unless intl is clearly faster than the redirect margin", () => {
    expect(chooseAutoRoute({
      cn: okProbe("cn", 500),
      intl: okProbe("intl", 120),
    }, "cn")?.target).toBe("cn");

    expect(chooseAutoRoute({
      cn: okProbe("cn", 1_200),
      intl: okProbe("intl", 300),
    }, "cn")?.target).toBe("intl");
  });

  it("raises the intl redirect margin for China-local browser signals", () => {
    const chinaProfile = createClientRouteProfile({
      timeZone: "Asia/Shanghai",
      languages: ["zh-CN"],
    });

    expect(chooseAutoRoute({
      cn: okProbe("cn", 1_200),
      intl: okProbe("intl", 300),
    }, "cn", chinaProfile)?.target).toBe("cn");

    expect(chooseAutoRoute({
      cn: okProbe("cn", 2_200),
      intl: okProbe("intl", 300),
    }, "cn", chinaProfile)?.target).toBe("intl");
  });

  it("builds cross-origin redirect URLs without dropping existing filters", () => {
    const decision = createManualRouteDecision("intl", 1_000);
    const href = buildRouteRedirectUrl(decision, {
      pathname: "/dashboard",
      search: "?country=DK&powertrain=BEV",
      hash: "#chart",
    });
    const url = new URL(href);
    expect(url.origin).toBe("https://intl.ojeur.cloud");
    expect(url.pathname).toBe("/dashboard");
    expect(url.searchParams.get("country")).toBe("DK");
    expect(url.searchParams.get("powertrain")).toBe("BEV");
    expect(url.searchParams.get("jatoRouteTarget")).toBe("intl");
    expect(url.searchParams.get("jatoRouteSource")).toBe("manual");
    expect(url.hash).toBe("#chart");
  });

  it("consumes transferred route decisions on the destination origin", () => {
    const storage = createStorage();
    const result = consumeRouteDecisionTransfer({
      pathname: "/dashboard",
      search: "?country=DK&jatoRouteTarget=intl&jatoRouteExpires=3000&jatoRouteCreated=1000&jatoRouteSource=auto&jatoRouteReason=fast",
      hash: "#chart",
    }, storage, 2_000);

    expect(result.cleanPath).toBe("/dashboard?country=DK#chart");
    expect(result.decision?.target).toBe("intl");
    expect(readRouteDecision(storage, DECISION_KEY, 2_000)?.reason).toBe("fast");
  });

  it("stores manual transfer decisions separately from auto decisions", () => {
    const storage = createStorage();
    const decision = createManualRouteDecision("cn", 1_000);
    const href = buildRouteRedirectUrl(decision, {
      pathname: "/route-diagnostics",
      search: "",
      hash: "",
    });
    const url = new URL(href);
    const result = consumeRouteDecisionTransfer({
      pathname: url.pathname,
      search: url.search,
      hash: url.hash,
    }, storage, 1_500);

    expect(result.decision?.source).toBe("manual");
    expect(readRouteDecision(storage, MANUAL_KEY, 1_500)?.target).toBe("cn");
    expect(readRouteDecision(storage, DECISION_KEY, 1_500)).toBeNull();
  });

  it("skips smart routing on diagnostics, local hosts, and OAuth callback URLs", () => {
    expect(shouldSkipSmartRoute({
      hostname: "www.ojeur.cloud",
      pathname: "/route-diagnostics",
      search: "",
      hash: "",
    })).toBe(true);
    expect(shouldSkipSmartRoute({
      hostname: "localhost",
      pathname: "/dashboard",
      search: "",
      hash: "",
    })).toBe(true);
    expect(shouldSkipSmartRoute({
      hostname: "intl.ojeur.cloud",
      pathname: "/dashboard",
      search: "?token=abc",
      hash: "",
    })).toBe(true);
    expect(shouldSkipSmartRoute({
      hostname: "intl.ojeur.cloud",
      pathname: "/dashboard",
      search: "",
      hash: "",
    })).toBe(false);
  });

  it("creates an expiring auto decision with probe timings", () => {
    const decision = createAutoRouteDecision({
      cn: okProbe("cn", 1_100),
      intl: okProbe("intl", 200),
    }, "cn", null, 1_000);

    expect(decision?.target).toBe("intl");
    expect(decision?.source).toBe("auto");
    expect(decision?.cnMs).toBe(1_100);
    expect(decision?.intlMs).toBe(200);
    expect(decision?.expiresAt).toBeGreaterThan(1_000);
  });

  it("detects and clears stale early route probes", () => {
    const storage = createStorage();
    storage.setItem("jato_route_probe_inflight_v1", "1000");

    expect(isRouteProbeInFlight(storage, 2_000)).toBe(true);
    expect(isRouteProbeInFlight(storage, 10_000)).toBe(false);
    expect(storage.getItem("jato_route_probe_inflight_v1")).toBeNull();
  });
});
