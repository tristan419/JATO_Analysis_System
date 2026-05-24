import { Suspense, lazy, type ReactNode } from "react";
import { Navigate, createBrowserRouter, RouterProvider } from "react-router-dom";
import { SharedFilterScopeProvider } from "./contexts/SharedFilterScopeContext";
import { CountryChatProvider } from "./contexts/CountryChatContext";
import { AuthProvider } from "./contexts/AuthContext";
import { Layout } from "./components/Layout";
import { LoadingSurface } from "./components/LoadingSurface";
import { DashboardPage } from "./pages/DashboardPage";
import { LoginPage } from "./pages/LoginPage";

const DataManagementPage = lazy(() => import("./pages/DataManagementPage").then(m => ({ default: m.DataManagementPage })));
const EngineeringPage = lazy(() => import("./pages/EngineeringPage").then(m => ({ default: m.EngineeringPage })));
const ReviewCasesPage = lazy(() => import("./pages/ReviewCasesPage").then(m => ({ default: m.ReviewCasesPage })));
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
const OrderGeniusPage = lazy(() => import("./pages/OrderGeniusPage").then(m => ({ default: m.OrderGeniusPage })));
const AccessControlPage = lazy(() => import("./pages/AccessControlPage").then(m => ({ default: m.AccessControlPage })));

function withPageLoader(node: ReactNode) {
  return (<Suspense fallback={<div className="app-loading-shell"><LoadingSurface mode="overlay" label="正在加载页面" detail="准备下一个工作视图与路由资源" kicker="Route" /></div>}>{node}</Suspense>);
}

const router = createBrowserRouter([
  { path: "/login", element: (<AuthProvider><LoginPage /></AuthProvider>) },
  { path: "/", element: (<AuthProvider><SharedFilterScopeProvider><CountryChatProvider><Layout /></CountryChatProvider></SharedFilterScopeProvider></AuthProvider>), children: [
    { index: true, element: <Navigate to="/dashboard" replace /> },
    { path: "dashboard", element: <DashboardPage /> },
    { path: "market/overview", element: withPageLoader(<MarketOverviewPage />) },
    { path: "market/segments", element: withPageLoader(<MarketSegmentsPage />) },
    { path: "market/ranking/brand", element: withPageLoader(<MarketBrandRankingPage />) },
    { path: "market/ranking/model", element: withPageLoader(<MarketModelRankingPage />) },
    { path: "market/powertrain", element: withPageLoader(<MarketPowertrainPage />) },
    { path: "product/current-msrp", element: withPageLoader(<MsrpPage />) },
    { path: "product/order-genius", element: withPageLoader(<OrderGeniusPage />) },
    { path: "product/pricing", element: withPageLoader(<PositioningPricingPage />) },
    { path: "product/compare", element: withPageLoader(<VersionComparisonPage />) },
    { path: "product/customer-insight", element: withPageLoader(<CustomerInsightsPage />) },
    { path: "data/spec-detail", element: withPageLoader(<SpecificationPage />) },
    { path: "data/overview", element: withPageLoader(<DataManagementPage />) },
    { path: "data/config-import", element: withPageLoader(<EngineeringPage />) },
    { path: "data/matching-review", element: withPageLoader(<ReviewCasesPage />) },
    { path: "data/jato-monthly-update", element: withPageLoader(<JatoMonthlyUpdatePage />) },
    { path: "data/order-genius", element: withPageLoader(<OrderGeniusPage />) },
    { path: "admin/access-control", element: withPageLoader(<AccessControlPage />) },
    { path: "product/coc-match", element: withPageLoader(<CocMatchPage />) },
    { path: "copilot", element: withPageLoader(<CountryChatPage />) },
    { path: "engineering-config", element: withPageLoader(<EngineeringConfigPage />) },
    { path: "market-scan", element: <Navigate to="/market/overview" replace /> },
    { path: "msrp", element: <Navigate to="/product/current-msrp" replace /> },
    { path: "msrp/monthly-update", element: <Navigate to="/data/jato-monthly-update" replace /> },
    { path: "positioning-pricing", element: <Navigate to="/product/pricing" replace /> },
    { path: "version-comparison", element: <Navigate to="/product/compare" replace /> },
    { path: "customer-insights", element: <Navigate to="/product/customer-insight" replace /> },
    { path: "customer-hev", element: <Navigate to="/product/customer-insight" replace /> },
    { path: "specification", element: <Navigate to="/data/spec-detail" replace /> },
    { path: "data-management", element: <Navigate to="/data/overview" replace /> },
    { path: "engineering", element: <Navigate to="/data/config-import" replace /> },
    { path: "review", element: <Navigate to="/data/matching-review" replace /> },
    { path: "crud", element: <Navigate to="/data/overview" replace /> },
    { path: "*", element: withPageLoader(<NotFoundPage />) },
  ]},
]);

export default function App() { return <RouterProvider router={router} />; }
