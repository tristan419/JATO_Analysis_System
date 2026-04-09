import { Suspense, lazy, type ReactNode } from "react";
import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { Layout } from "./components/Layout";
import { LoadingSurface } from "./components/LoadingSurface";
import { DashboardPage } from "./pages/DashboardPage";

const CrudPage = lazy(() => import("./pages/CrudPage").then((module) => ({ default: module.CrudPage })));
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
    element: <Layout />,
    children: [
      { index: true, element: <DashboardPage /> },
      { path: "specification", element: withPageLoader(<SpecificationPage />) },
      { path: "crud", element: withPageLoader(<CrudPage />) },
      { path: "*", element: withPageLoader(<NotFoundPage />) }
    ]
  }
]);

export default function App() {
  return <RouterProvider router={router} />;
}
