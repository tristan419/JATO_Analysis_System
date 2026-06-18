import { Suspense, lazy, Component, type ReactNode } from "react";
import { Navigate, createBrowserRouter, RouterProvider, useLocation } from "react-router-dom";
import { SharedFilterScopeProvider } from "./contexts/SharedFilterScopeContext";
import { CountryChatProvider } from "./contexts/CountryChatContext";
import { AuthProvider } from "./contexts/AuthContext";
import { Layout } from "./components/Layout";
import { RequireRole } from "./components/RequireRole";
import { LoadingSurface } from "./components/LoadingSurface";
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
const CountryChatPage = lazy(() => import("./pages/CountryChatPage").then(m => ({ default: m.CountryChatPage })));
const NotFoundPage = lazy(() => import("./pages/NotFoundPage").then(m => ({ default: m.NotFoundPage })));
const SpecificationPage = lazy(() => import("./pages/SpecificationPage").then(m => ({ default: m.SpecificationPage })));
const EngineeringConfigPage = lazy(() => import("./pages/EngineeringConfigPage").then(m => ({ default: m.EngineeringConfigPage })));
const MarketOverviewPage = lazy(() => import("./pages/MarketOverviewPage").then(m => ({ default: m.MarketOverviewPage })));
const MarketSegmentsPage = lazy(() => import("./pages/MarketSegmentsPage").then(m => ({ default: m.MarketSegmentsPage })));
const MarketBrandRankingPage = lazy(() => import("./pages/MarketBrandRankingPage").then(m => ({ default: m.MarketBrandRankingPage })));
const MarketModelRankingPage = lazy(() => import("./pages/MarketModelRankingPage").then(m => ({ default: m.MarketModelRankingPage })));
const MarketPowertrainPage = lazy(() => import("./pages/MarketPowertrainPage").then(m => ({ default: m.MarketPowertrainPage })));
const AdvancedAnalysisPage = lazy(() => import("./pages/AdvancedAnalysisPage").then(m => ({ default: m.AdvancedAnalysisPage })));
const LeaseComparisonPage = lazy(() => import("./pages/LeaseComparisonPage").then(m => ({ default: m.LeaseComparisonPage })));
const OrderGeniusPage = lazy(() => import("./pages/OrderGeniusPage").then(m => ({ default: m.OrderGeniusPage })));
const OrderGeniusCbuPage = lazy(() => import("./pages/OrderGeniusCbuPage").then(m => ({ default: m.OrderGeniusCbuPage })));
const OrderGeniusVehicleAllocationPage = lazy(() => import("./pages/OrderGeniusVehicleAllocationPage").then(m => ({ default: m.OrderGeniusVehicleAllocationPage })));
const AccessControlPage = lazy(() => import("./pages/AccessControlPage").then(m => ({ default: m.AccessControlPage })));
const ProfilePage = lazy(() => import("./pages/ProfilePage").then(m => ({ default: m.ProfilePage })));

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
  return (
    <ChunkErrorBoundary>
      <Suspense fallback={<div className="app-loading-shell"><LoadingSurface mode="overlay" label="正在加载页面" detail="准备下一个工作视图与路由资源" kicker="Route" /></div>}>
        {node}
      </Suspense>
    </ChunkErrorBoundary>
  );
}

function withSharedFilterScope(node: ReactNode) {
  return <SharedFilterScopeProvider>{node}</SharedFilterScopeProvider>;
}

function RedirectPreserveSearch({ to }: { to: string }) {
  const location = useLocation();
  return <Navigate to={`${to}${location.search}${location.hash}`} replace />;
}

const router = createBrowserRouter([
  { path: "/login", element: (<AuthProvider>{withPageLoader(<LoginPage />)}</AuthProvider>) },
  { path: "/", element: (<AuthProvider><OAuthGate><CountryChatProvider><RequireRole><Layout /></RequireRole></CountryChatProvider></OAuthGate></AuthProvider>), children: [
    { index: true, element: withSharedFilterScope(withPageLoader(<DashboardPage />)) },
    { path: "dashboard", element: withSharedFilterScope(withPageLoader(<DashboardPage />)) },
    { path: "market/overview", element: withPageLoader(<MarketOverviewPage />) },
    { path: "market/segments", element: withPageLoader(<MarketSegmentsPage />) },
    { path: "market/ranking/brand", element: withPageLoader(<MarketBrandRankingPage />) },
    { path: "market/ranking/model", element: withPageLoader(<MarketModelRankingPage />) },
    { path: "market/powertrain", element: withPageLoader(<MarketPowertrainPage />) },
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
    { path: "data/spec-detail", element: withSharedFilterScope(withPageLoader(<SpecificationPage />)) },
    { path: "data/overview", element: withPageLoader(<DataManagementPage />) },
    { path: "data/config-import", element: withPageLoader(<EngineeringPage />) },
    { path: "data/matching-review", element: withPageLoader(<ReviewCasesPage />) },
    { path: "data/jato-monthly-update", element: withPageLoader(<JatoMonthlyUpdatePage />) },
    { path: "data/order-genius", element: withPageLoader(<OrderGeniusPage />) },
    { path: "admin/access-control", element: withPageLoader(<AccessControlPage />) },
    { path: "account/profile", element: withPageLoader(<ProfilePage />) },
    { path: "product/coc-match", element: withPageLoader(<CocMatchPage />) },
    { path: "copilot", element: withPageLoader(<CountryChatPage />) },
    { path: "engineering-config", element: withPageLoader(<EngineeringConfigPage />) },
    { path: "market-scan", element: <RedirectPreserveSearch to="/market/overview" /> },
    { path: "msrp", element: <RedirectPreserveSearch to="/product/current-msrp" /> },
    { path: "msrp/monthly-update", element: <RedirectPreserveSearch to="/data/jato-monthly-update" /> },
    { path: "positioning-pricing", element: <RedirectPreserveSearch to="/product/pricing" /> },
    { path: "version-comparison", element: <RedirectPreserveSearch to="/product/compare" /> },
    { path: "customer-insights", element: <RedirectPreserveSearch to="/product/customer-insight" /> },
    { path: "customer-hev", element: <RedirectPreserveSearch to="/product/customer-insight" /> },
    { path: "specification", element: withSharedFilterScope(withPageLoader(<SpecificationPage />)) },
    { path: "data-management", element: withPageLoader(<DataManagementPage />) },
    { path: "engineering", element: <RedirectPreserveSearch to="/data/config-import" /> },
    { path: "review", element: <RedirectPreserveSearch to="/data/matching-review" /> },
    { path: "crud", element: <RedirectPreserveSearch to="/data-management" /> },
    { path: "*", element: withPageLoader(<NotFoundPage />) },
  ]},
]);

export default function App() { return <RouterProvider router={router} />; }
