import { Suspense, lazy, Component, useEffect, useState, type ReactNode } from "react";
import { Navigate, createBrowserRouter, RouterProvider, useLocation } from "react-router-dom";
import { AuthProvider } from "./contexts/AuthContext";
import { Layout } from "./components/Layout";
import { RequireRole } from "./components/RequireRole";
import { DashboardRouteSkeleton } from "./components/DashboardRouteSkeleton";
import { LoadingSurface } from "./components/LoadingSurface";
import { SmartRouteGate } from "./components/SmartRouteGate";
import { CandidateEnvironmentBanner } from "./components/CandidateEnvironmentBanner";
import { getOAuthRedirectTarget } from "./utils/oauthRedirect";

/** Consume OAuth token params before any provider mounts, avoiding aborted fetches. */
function OAuthGate({ children }: { children: ReactNode }) {
  // This runs during render, BEFORE any child effects (SharedFilterScope etc.)
  // so we can redirect before those effects fire and get aborted.
  const params = new URLSearchParams(window.location.search);
  const urlToken = params.get("token");
  if (urlToken) {
    const urlUser = params.get("username") || "anonymous";
    const urlRole = params.get("role") || "viewer";
    const isNewUser = params.get("isNewUser") === "true";
    localStorage.setItem("jato_auth_token", urlToken);
    localStorage.setItem("jato_user_name", urlUser);
    localStorage.setItem("jato_user_role", urlRole);
    localStorage.removeItem("shared-filter-scope");
    const target = getOAuthRedirectTarget(window.location, isNewUser);
    // Sync redirect — aborts current render before any child effects run
    window.location.replace(target);
    return null;
  }
  return <>{children}</>;
}

const DataManagementPage = lazy(() => import("./pages/DataManagementPage").then(m => ({ default: m.DataManagementPage })));
const DashboardPage = lazy(() => import("./pages/DashboardPage").then(m => ({ default: m.DashboardPage })));
const EngineeringPage = lazy(() => import("./pages/EngineeringPage").then(m => ({ default: m.EngineeringPage })));
const ReviewCasesPage = lazy(() => import("./pages/ReviewCasesPage").then(m => ({ default: m.ReviewCasesPage })));
const LoginPage = lazy(() => import("./pages/LoginPage").then(m => ({ default: m.LoginPage })));
const MsrpPage = lazy(() => import("./pages/MsrpPage").then(m => ({ default: m.MsrpPage })));
const JatoMonthlyUpdatePage = lazy(() => import("./pages/JatoMonthlyUpdatePage").then(m => ({ default: m.JatoMonthlyUpdatePage })));
const CocMatchPage = lazy(() => import("./pages/CocMatchPage").then(m => ({ default: m.CocMatchPage })));
const PositioningPricingPage = lazy(() => import("./pages/PositioningPricingPage").then(m => ({ default: m.PositioningPricingPage })));
const VersionComparisonPage = lazy(() => import("./pages/VersionComparisonPage").then(m => ({ default: m.VersionComparisonPage })));
const CustomerInsightsPage = lazy(() => import("./pages/CustomerInsightsPage").then(m => ({ default: m.CustomerInsightsPage })));
const CountryChatPageHost = lazy(() => import("./pages/CountryChatPageHost").then(m => ({ default: m.CountryChatPageHost })));
const NotFoundPage = lazy(() => import("./pages/NotFoundPage").then(m => ({ default: m.NotFoundPage })));
const SpecificationPage = lazy(() => import("./pages/SpecificationPage").then(m => ({ default: m.SpecificationPage })));
const EngineeringConfigPage = lazy(() => import("./pages/EngineeringConfigPage").then(m => ({ default: m.EngineeringConfigPage })));
const MarketOverviewPage = lazy(() => import("./pages/MarketOverviewPage").then(m => ({ default: m.MarketOverviewPage })));
const MarketSegmentsPage = lazy(() => import("./pages/MarketSegmentsPage").then(m => ({ default: m.MarketSegmentsPage })));
const MarketBrandRankingPage = lazy(() => import("./pages/MarketBrandRankingPage").then(m => ({ default: m.MarketBrandRankingPage })));
const MarketModelRankingPage = lazy(() => import("./pages/MarketModelRankingPage").then(m => ({ default: m.MarketModelRankingPage })));
const MarketPowertrainPage = lazy(() => import("./pages/MarketPowertrainPage").then(m => ({ default: m.MarketPowertrainPage })));
const MsrpMonitorPage = lazy(() => import("./pages/MsrpMonitorPage").then(m => ({ default: m.MsrpMonitorPage })));
const AdvancedAnalysisPage = lazy(() => import("./pages/AdvancedAnalysisPage").then(m => ({ default: m.AdvancedAnalysisPage })));
const LeaseComparisonPage = lazy(() => import("./pages/LeaseComparisonPage").then(m => ({ default: m.LeaseComparisonPage })));
const OrderGeniusPage = lazy(() => import("./pages/OrderGeniusPage").then(m => ({ default: m.OrderGeniusPage })));
const OrderGeniusCbuPage = lazy(() => import("./pages/OrderGeniusCbuPage").then(m => ({ default: m.OrderGeniusCbuPage })));
const OrderGeniusVehicleAllocationPage = lazy(() => import("./pages/OrderGeniusVehicleAllocationPage").then(m => ({ default: m.OrderGeniusVehicleAllocationPage })));
const AccessControlPage = lazy(() => import("./pages/AccessControlPage").then(m => ({ default: m.AccessControlPage })));
const ProfilePage = lazy(() => import("./pages/ProfilePage").then(m => ({ default: m.ProfilePage })));
const AstrBotPage = lazy(() => import("./pages/AstrBotPage").then(m => ({ default: m.AstrBotPage })));
const RouteDiagnosticsPage = lazy(() => import("./pages/RouteDiagnosticsPage").then(m => ({ default: m.RouteDiagnosticsPage })));
const SharedFilterScopeProvider = lazy(() =>
  import("./contexts/SharedFilterScopeContext").then(m => ({ default: m.SharedFilterScopeProvider })),
);

class ChunkErrorBoundary extends Component<{ children: ReactNode }, { hasError: boolean }> {
  state = { hasError: false };
  static getDerivedStateFromError() { return { hasError: true }; }
  componentDidCatch(error: Error) {
    const msg = error?.message ?? "";
    if (/importing a module script|Failed to fetch dynamically imported module|error loading dynamically imported module/i.test(msg)) {
      window.location.reload();
    }
  }
  render() {
    if (this.state.hasError) {
      return (<div className="app-loading-shell"><LoadingSurface mode="overlay" label="正在重新加载" detail="模块热更新后自动刷新" kicker="Route" /></div>);
    }
    return this.props.children;
  }
}

function withPageLoader(node: ReactNode) {
  return withRouteLoader(node);
}

function withRouteLoader(node: ReactNode, fallback?: ReactNode) {
  return (
    <ChunkErrorBoundary>
      <Suspense fallback={fallback ?? <div className="app-loading-shell"><LoadingSurface mode="overlay" label="正在加载页面" detail="准备下一个工作视图与路由资源" kicker="Route" /></div>}>
        {node}
      </Suspense>
    </ChunkErrorBoundary>
  );
}

function withDashboardLoader(node: ReactNode, fallback?: ReactNode) {
  return withRouteLoader(node, fallback ?? <DashboardRouteSkeleton />);
}

function withSharedFilterScope(node: ReactNode, fallback?: ReactNode) {
  return withRouteLoader(<SharedFilterScopeProvider>{node}</SharedFilterScopeProvider>, fallback ?? <DashboardRouteSkeleton />);
}

function RedirectPreserveSearch({ to }: { to: string }) {
  const location = useLocation();
  return <Navigate to={`${to}${location.search}${location.hash}`} replace />;
}

function getAppEntryScriptFromHtml(html: string): string | null {
  const match = html.match(/<script[^>]+src="([^"]*\/assets\/index-[^"]+\.js)"/i);
  return match?.[1] ?? null;
}

function getCurrentAppEntryScript(): string | null {
  const script = Array.from(document.scripts).find((item) =>
    item.src.includes("/assets/index-") && item.src.endsWith(".js"),
  );
  if (!script) return null;
  try {
    return new URL(script.src).pathname;
  } catch {
    return script.getAttribute("src");
  }
}

const APP_VERSION_INITIAL_CHECK_DELAY_MS = 45_000;
const APP_VERSION_IDLE_TIMEOUT_MS = 5_000;

type AppVersionWindow = Window & typeof globalThis & {
  requestIdleCallback?: (callback: () => void, options?: { timeout?: number }) => number;
  cancelIdleCallback?: (handle: number) => void;
};

function scheduleAppVersionCheck(callback: () => void): () => void {
  const appWindow = window as AppVersionWindow;
  let idleHandle: number | null = null;
  const delayHandle = window.setTimeout(() => {
    if (typeof appWindow.requestIdleCallback === "function") {
      idleHandle = appWindow.requestIdleCallback(callback, {
        timeout: APP_VERSION_IDLE_TIMEOUT_MS,
      });
      return;
    }
    callback();
  }, APP_VERSION_INITIAL_CHECK_DELAY_MS);

  return () => {
    window.clearTimeout(delayHandle);
    if (idleHandle !== null) appWindow.cancelIdleCallback?.(idleHandle);
  };
}

function AppVersionNotice() {
  const [latestEntryScript, setLatestEntryScript] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let listenersAttached = false;
    const currentEntryScript = getCurrentAppEntryScript();
    if (!currentEntryScript) return undefined;

    const checkLatestEntry = async () => {
      try {
        const response = await fetch(`/?version-check=${Date.now()}`, {
          cache: "no-store",
          headers: { "Cache-Control": "no-cache" },
        });
        if (!response.ok) return;
        const html = await response.text();
        const nextEntryScript = getAppEntryScriptFromHtml(html);
        if (!cancelled && nextEntryScript && nextEntryScript !== currentEntryScript) {
          setLatestEntryScript(nextEntryScript);
        }
      } catch {
        // Version checks should never interrupt the active workspace.
      }
    };

    const handleFocus = () => {
      if (document.visibilityState === "hidden") return;
      void checkLatestEntry();
    };
    const attachFocusListeners = () => {
      if (listenersAttached) return;
      listenersAttached = true;
      window.addEventListener("focus", handleFocus);
      document.addEventListener("visibilitychange", handleFocus);
    };
    const cancelInitialCheck = scheduleAppVersionCheck(() => {
      void checkLatestEntry();
      attachFocusListeners();
    });
    const interval = window.setInterval(() => {
      void checkLatestEntry();
    }, 5 * 60 * 1000);

    return () => {
      cancelled = true;
      cancelInitialCheck();
      window.clearInterval(interval);
      if (listenersAttached) {
        window.removeEventListener("focus", handleFocus);
        document.removeEventListener("visibilitychange", handleFocus);
      }
    };
  }, []);

  if (!latestEntryScript) return null;

  return (
    <div
      style={{
        position: "fixed",
        left: 16,
        right: 16,
        bottom: 16,
        zIndex: 10000,
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        gap: 12,
        padding: "12px 14px",
        border: "1px solid #bfdbfe",
        background: "rgba(239,246,255,0.98)",
        boxShadow: "0 18px 45px rgba(15,23,42,0.18)",
        color: "#1e3a8a",
        fontSize: 13,
        fontWeight: 800,
      }}
      role="status"
    >
      <span>New app version is available. Refresh to avoid stale Chrome assets.</span>
      <button
        type="button"
        onClick={() => window.location.reload()}
        style={{
          border: "1px solid #2563eb",
          background: "#2563eb",
          color: "#ffffff",
          padding: "8px 12px",
          fontSize: 12,
          fontWeight: 900,
          letterSpacing: 1,
          textTransform: "uppercase",
          cursor: "pointer",
        }}
      >
        Refresh
      </button>
    </div>
  );
}

const router = createBrowserRouter([
  { path: "/login", element: (<AuthProvider>{withPageLoader(<LoginPage />)}</AuthProvider>) },
  { path: "/", element: (<AuthProvider><OAuthGate><RequireRole><Layout /></RequireRole></OAuthGate></AuthProvider>), children: [
    { index: true, element: withSharedFilterScope(withDashboardLoader(<DashboardPage />)) },
    { path: "dashboard", element: withSharedFilterScope(withDashboardLoader(<DashboardPage />)) },
    { path: "market/overview", element: withPageLoader(<MarketOverviewPage />) },
    { path: "market/segments", element: withPageLoader(<MarketSegmentsPage />) },
    { path: "market/ranking/brand", element: withPageLoader(<MarketBrandRankingPage />) },
    { path: "market/ranking/model", element: withPageLoader(<MarketModelRankingPage />) },
    { path: "market/powertrain", element: withPageLoader(<MarketPowertrainPage />) },
    { path: "market/msrp-monitor", element: withPageLoader(<MsrpMonitorPage />) },
    { path: "market/transfer", element: withPageLoader(<AdvancedAnalysisPage />) },
    { path: "market/advanced-analysis", element: withPageLoader(<AdvancedAnalysisPage />) },
    { path: "product/current-msrp", element: withPageLoader(<MsrpPage />) },
    { path: "product/order-genius", element: withPageLoader(<OrderGeniusPage />) },
    { path: "product/order-genius/cbu", element: withPageLoader(<OrderGeniusCbuPage />) },
    { path: "product/order-genius/vehicle-allocation", element: withPageLoader(<OrderGeniusVehicleAllocationPage />) },
    { path: "product/lease-comparison", element: withPageLoader(<LeaseComparisonPage />) },
    { path: "product/pricing", element: withPageLoader(<PositioningPricingPage />) },
    { path: "product/compare", element: withPageLoader(<VersionComparisonPage />) },
    { path: "product/customer-insight", element: withPageLoader(<CustomerInsightsPage />) },
    { path: "data/spec-detail", element: withSharedFilterScope(withDashboardLoader(
      <SpecificationPage />,
      <DashboardRouteSkeleton
        chartKicker="03 / Specification Grid"
        chartTitle="Specification detail loading"
        heroKicker="01 / Specification Scope"
        title="Specification Page"
      />,
    ), <DashboardRouteSkeleton
      chartKicker="03 / Specification Grid"
      chartTitle="Specification detail loading"
      heroKicker="01 / Specification Scope"
      title="Specification Page"
    />) },
    { path: "data/overview", element: withPageLoader(<DataManagementPage />) },
    { path: "data/config-import", element: withPageLoader(<EngineeringPage />) },
    { path: "data/matching-review", element: withPageLoader(<ReviewCasesPage />) },
    { path: "data/jato-monthly-update", element: withPageLoader(<JatoMonthlyUpdatePage />) },
    { path: "data/order-genius", element: withPageLoader(<OrderGeniusPage />) },
    { path: "admin/access-control", element: withPageLoader(<AccessControlPage />) },
    { path: "account/profile", element: withPageLoader(<ProfilePage />) },
    { path: "route-diagnostics", element: withPageLoader(<RouteDiagnosticsPage />) },
    { path: "product/coc-match", element: withPageLoader(<CocMatchPage />) },
    { path: "copilot", element: withPageLoader(<CountryChatPageHost />) },
    { path: "astrbot/*", element: withPageLoader(<AstrBotPage />) },
    { path: "engineering-config", element: withPageLoader(<EngineeringConfigPage />) },
    { path: "market-scan", element: <RedirectPreserveSearch to="/market/overview" /> },
    { path: "msrp", element: <RedirectPreserveSearch to="/product/current-msrp" /> },
    { path: "msrp/monthly-update", element: <RedirectPreserveSearch to="/data/jato-monthly-update" /> },
    { path: "positioning-pricing", element: <RedirectPreserveSearch to="/product/pricing" /> },
    { path: "version-comparison", element: <RedirectPreserveSearch to="/product/compare" /> },
    { path: "customer-insights", element: <RedirectPreserveSearch to="/product/customer-insight" /> },
    { path: "customer-hev", element: <RedirectPreserveSearch to="/product/customer-insight" /> },
    { path: "specification", element: withSharedFilterScope(withDashboardLoader(
      <SpecificationPage />,
      <DashboardRouteSkeleton
        chartKicker="03 / Specification Grid"
        chartTitle="Specification detail loading"
        heroKicker="01 / Specification Scope"
        title="Specification Page"
      />,
    ), <DashboardRouteSkeleton
      chartKicker="03 / Specification Grid"
      chartTitle="Specification detail loading"
      heroKicker="01 / Specification Scope"
      title="Specification Page"
    />) },
    { path: "data-management", element: withPageLoader(<DataManagementPage />) },
    { path: "engineering", element: <RedirectPreserveSearch to="/data/config-import" /> },
    { path: "review", element: <RedirectPreserveSearch to="/data/matching-review" /> },
    { path: "crud", element: <RedirectPreserveSearch to="/data-management" /> },
    { path: "*", element: withPageLoader(<NotFoundPage />) },
  ]},
]);

const ROUTER_TRANSITION_PROPS = {
  // Commit route changes immediately so a lazy target cannot leave the old page visible.
  unstable_useTransitions: false,
} satisfies Record<string, boolean>;

export default function App() {
  return (
    <>
      <CandidateEnvironmentBanner />
      <SmartRouteGate />
      <RouterProvider router={router} {...ROUTER_TRANSITION_PROPS} />
      <AppVersionNotice />
    </>
  );
}
