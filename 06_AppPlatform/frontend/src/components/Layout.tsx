import { Link, Outlet, useLocation } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", icon: "📊" },
  { to: "/crud", label: "数据管理", icon: "📋" },
];

export function Layout() {
  const location = useLocation();

  const isActive = (path: string) =>
    path === "/" ? location.pathname === "/" : location.pathname.startsWith(path);

  return (
    <div className="app-root">
      <header className="top-bar">
        <span className="top-bar-brand">🚗 JATO Analysis Platform</span>
        <nav className="top-bar-nav">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              className={`top-bar-link${isActive(item.to) ? " active" : ""}`}
            >
              <span>{item.icon}</span>
              {item.label}
            </Link>
          ))}
        </nav>
      </header>
      <main className="main-area">
        <Outlet />
      </main>
    </div>
  );
}
