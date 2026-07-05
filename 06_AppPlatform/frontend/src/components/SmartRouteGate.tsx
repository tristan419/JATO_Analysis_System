import { useEffect } from "react";

import {
  DECISION_KEY,
  MANUAL_KEY,
  buildRouteRedirectUrl,
  clearRouteDecisions,
  consumeRouteDecisionTransfer,
  createAutoRouteDecision,
  createCurrentChinaRouteDecision,
  currentRouteTarget,
  detectClientRouteProfile,
  isRouteProbeInFlight,
  probeRoute,
  readRouteDecision,
  saveRouteDecision,
  shouldSkipSmartRoute,
  type RouteDecision,
  type RouteTarget,
} from "../utils/routeDecision";

type RouteProbeWindow = Window & typeof globalThis & {
  __JATO_ROUTE_PROBE_IN_FLIGHT__?: boolean;
};

function redirectIfNeeded(decision: RouteDecision, currentTarget: RouteTarget): void {
  if (decision.target === currentTarget) return;
  window.location.replace(buildRouteRedirectUrl(decision, window.location));
}

export function SmartRouteGate() {
  useEffect(() => {
    const transferred = consumeRouteDecisionTransfer(window.location, window.localStorage);
    if (transferred.cleanPath) {
      window.history.replaceState(window.history.state, "", transferred.cleanPath);
    }

    if (shouldSkipSmartRoute(window.location)) return undefined;
    const currentTarget = currentRouteTarget(window.location.hostname);
    if (!currentTarget) return undefined;

    const manualDecision = readRouteDecision(window.localStorage, MANUAL_KEY);
    if (manualDecision) {
      redirectIfNeeded(manualDecision, currentTarget);
      return undefined;
    }

    const cachedDecision = readRouteDecision(window.localStorage, DECISION_KEY);
    if (cachedDecision) {
      redirectIfNeeded(cachedDecision, currentTarget);
      return undefined;
    }
    const routeProbeWindow = window as RouteProbeWindow;
    if (
      routeProbeWindow.__JATO_ROUTE_PROBE_IN_FLIGHT__
      || isRouteProbeInFlight(window.sessionStorage)
    ) {
      return undefined;
    }

    const clientProfile = detectClientRouteProfile();
    const currentChinaDecision = createCurrentChinaRouteDecision(currentTarget, clientProfile);
    if (currentChinaDecision) {
      saveRouteDecision(window.localStorage, currentChinaDecision);
      return undefined;
    }

    let cancelled = false;
    void (async () => {
      const [cnResult, intlResult] = await Promise.all([
        probeRoute("cn"),
        probeRoute("intl"),
      ]);
      if (cancelled) return;
      const decision = createAutoRouteDecision({
        cn: cnResult,
        intl: intlResult,
      }, currentTarget, clientProfile);
      if (!decision) return;
      clearRouteDecisions(window.localStorage);
      saveRouteDecision(window.localStorage, decision);
      redirectIfNeeded(decision, currentTarget);
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  return null;
}
