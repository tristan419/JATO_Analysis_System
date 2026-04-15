import { Suspense, lazy, type ReactNode } from "react";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { SharedFilterScopeProvider } from "./contexts/SharedFilterScopeContext";
import { CountryChatProvider } from "./contexts/CountryChatContext";
import { Layout } from "./components/Layout";
import { LoadingSurface } from "./components/LoadingSurface";
import { DashboardPage } from "./pages/DashboardPage";

const CrudPage = lazy(() => import("./pages/CrudPage").then((module) => ({ default: module.CrudPage })));
const EngineeringPage = lazy(() => import("./pages/EngineeringPage").then((module) => ({ default: module.EngineeringPage })));
const ReviewCasesPage = lazy(() => import("./pages/ReviewCasesPage").then((module) => ({ default: module.ReviewCasesPage })));
const MsrpPage = lazy(() => import("./pages/MsrpPage").then((module) => ({ default: module.MsrpPage })));
const MarketScanPage = lazy(() => import("./pages/MarketScanPage").then((module) => ({ default: module.MarketScanPage })));
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
      { path: "crud", element: withPageLoader(<CrudPage />) },
      { path: "engineering", element: withPageLoader(<EngineeringPage />) },
      { path: "review", element: withPageLoader(<ReviewCasesPage />) },
      { path: "msrp", element: withPageLoader(<MsrpPage />) },
      { path: "market-scan", element: withPageLoader(<MarketScanPage />) },
      { path: "copilot", element: withPageLoader(<CountryChatPage />) },
      { path: "*", element: withPageLoader(<NotFoundPage />) }
    ]
  }
]);

export default function App() {
  return <RouterProvider router={router} />;
}
