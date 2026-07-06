type RouteProbeWindow = Window & typeof globalThis & {
  __JATO_ROUTE_PROBE_IN_FLIGHT__?: boolean;
};

const PROBE_INFLIGHT_KEY = "jato_route_probe_inflight_v1";
const PROBE_INFLIGHT_TTL_MS = 2_200;
const PROBE_CHECK_INTERVAL_MS = 50;

function isInitialRouteProbeInFlight(): boolean {
  const routeWindow = window as RouteProbeWindow;
  if (routeWindow.__JATO_ROUTE_PROBE_IN_FLIGHT__) return true;
  try {
    const raw = window.sessionStorage.getItem(PROBE_INFLIGHT_KEY);
    if (!raw) return false;
    const startedAt = Number(raw);
    if (!Number.isFinite(startedAt) || Date.now() - startedAt > PROBE_INFLIGHT_TTL_MS) {
      window.sessionStorage.removeItem(PROBE_INFLIGHT_KEY);
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

async function waitForInitialRouteProbe(): Promise<void> {
  if (!isInitialRouteProbeInFlight()) return;
  await new Promise<void>((resolve) => {
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      if (!isInitialRouteProbeInFlight() || Date.now() - startedAt > PROBE_INFLIGHT_TTL_MS) {
        window.clearInterval(timer);
        resolve();
      }
    }, PROBE_CHECK_INTERVAL_MS);
  });
}

async function bootApp(): Promise<void> {
  await waitForInitialRouteProbe();
  const cssPromise = import("./index.css");
  const appModulesPromise = Promise.all([
    import("react"),
    import("react-dom/client"),
    import("./App"),
  ]);
  const [, [{ default: React }, ReactDOM, { default: App }]] = await Promise.all([
    cssPromise,
    appModulesPromise,
  ]);
  const root = document.getElementById("root");
  if (!root) return;
  ReactDOM.createRoot(root).render(
    React.createElement(
      React.StrictMode,
      null,
      React.createElement(App),
    ),
  );
}

void bootApp();
