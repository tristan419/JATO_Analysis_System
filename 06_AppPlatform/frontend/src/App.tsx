import { createBrowserRouter, RouterProvider } from "react-router-dom";

import { Layout } from "./components/Layout";
import { CrudPage } from "./pages/CrudPage";
import { DashboardPage } from "./pages/DashboardPage";
import { NotFoundPage } from "./pages/NotFoundPage";

const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <DashboardPage /> },
      { path: "crud", element: <CrudPage /> },
      { path: "*", element: <NotFoundPage /> }
    ]
  }
]);

export default function App() {
  return <RouterProvider router={router} />;
}
