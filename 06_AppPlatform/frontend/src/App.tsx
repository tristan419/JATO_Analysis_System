import { Suspense, lazy, type ReactNode } from "react";
import { Navigate, createBrowserRouter, RouterProvider } from "react-router-dom";

import { SharedFilterScopeProvider } from "./contexts/SharedFilterScopeContext";
import { CountryChatProvider } from "./contexts/CountryChatContext";
import { Layout } from "./components/Layout";
import { LoadingSurface } from "./components/LoadingSurface";
import { DashboardPage } from "./pages/DashboardPage";

const DataManagementPage = lazy(() =>
  import("./pages/DataManagementPage").then((module) => ({ default: module.DataManagementPage }))
);
const EngineeringPage = lazy(() => import("./pages/EngineeringPage").then((module) => ({ default: module.EngineeringPage })));
const ReviewCasesPage = lazy(() => import("./pages/ReviewCasesPage").then((module) => ({ default: module.ReviewCasesPage })));
const MsrpPage = lazy(() => import("./pages/MsrpPage").then((module) => ({ default: module.MsrpPage })));
const JatoMonthlyUpdatePage = lazy(() =>
  import("./pages/JatoMonthlyUpdatePage").then((module) => ({ default: module.JatoMonthlyUpdatePage }))
);
const MarketScanPage = lazy(() => import("./pages/MarketScanPage").then((module) => ({ default: module.MarketScanPage })));
const PositioningPricingPage = lazy(() =>
  import("./pages/PositioningPricingPage").then((module) => ({ default: module.PositioningPricingPage }))
);
const VersionComparisonPage = lazy(() =>
  import("./pages/VersionComparisonPage").then((module) => ({ default: module.VersionComparisonPage }))
);
const CustomerInsightsPage = lazy(() =>
  import("./pages/CustomerInsightsPage").then((module) => ({ default: module.CustomerInsightsPage }))
);
const CountryChatPage = lazy(() => import("./pages/CountryChatPage").then((module) => ({ default: module.CountryChatPage })));
const NotFoundPage = lazy(() => import("./pages/NotFoundPage").then((module) => ({ default: module.NotFoundPage })));
const SpecificationPage = lazy(() =>
  import("./pages/SpecificationPage").then((module) => ({ default: module.SpecificationPage }))
);

function withPageLoader(node: ReactNode) {
  return (
    <Suspense fallback={
      <div className="app-loading-shell">
        <LoadingSurface
          mode="overlay"
          label="正在加载页面"
          detail="准备下一个工作视图与路由资源"
          kicker="Route"
        />
      </div>
    }>
      {node}
    </Suspense>
  );
}

const router = createBrowserRouter([
  {
    path: "/",
    element: (
      <SharedFilterScopeProvider>
        <CountryChatProvider>
          <Layout />
        </CountryChatProvider>
      </SharedFilterScopeProvider>
    ),
    children: [
      { index: true, element: <DashboardPage /> },
      { path: "specification", element: withPageLoader(<SpecificationPage />) },
      { path: "data-management", element: withPageLoader(<DataManagementPage />) },
      { path: "crud", element: <Navigate to="/data-management" replace /> },
      { path: "engineering", element: withPageLoader(<EngineeringPage />) },
      { path: "review", element: withPageLoader(<ReviewCasesPage />) },
      { path: "msrp", element: withPageLoader(<MsrpPage />) },
      { path: "msrp/monthly-update", element: withPageLoader(<JatoMonthlyUpdatePage />) },
      { path: "market-scan", element: withPageLoader(<MarketScanPage />) },
      { path: "positioning-pricing", element: withPageLoader(<PositioningPricingPage />) },
      { path: "version-comparison", element: withPageLoader(<VersionComparisonPage />) },
      { path: "customer-insights", element: withPageLoader(<CustomerInsightsPage />) },
      { path: "copilot", element: withPageLoader(<CountryChatPage />) },
      { path: "*", element: withPageLoader(<NotFoundPage />) }
    ]
  }
]);

export default function App() {
  return <RouterProvider router={router} />;
}
