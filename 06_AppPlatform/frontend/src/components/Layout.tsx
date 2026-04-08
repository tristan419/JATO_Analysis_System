import { Link, Outlet, useLocation } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", code: "01", label: "Overview", sublabel: "Dashboard" },
  { to: "/specification", code: "02", label: "Specification", sublabel: "规格明细" },
  { to: "/crud", code: "03", label: "Control", sublabel: "数据管理" },
];

export function Layout() {
  const location = useLocation();

  const isActive = (path: string) =>
    path === "/" ? location.pathname === "/" : location.pathname.startsWith(path);

  return (
    <div className="app-root">
      <header className="top-bar">
        <div className="top-bar-brand">
          <span className="top-bar-brand-eyebrow">JATO Analysis System</span>
          <span className="top-bar-brand-title">Market Intelligence Control Deck</span>
        </div>
        <nav className="top-bar-nav" aria-label="Primary">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.to}
              to={item.to}
              className={`top-bar-link${isActive(item.to) ? " active" : ""}`}
            >
              <span className="top-bar-link-index">{item.code}</span>
              <span className="top-bar-link-copy">
                <span className="top-bar-link-label">{item.label}</span>
                <span className="top-bar-link-sublabel">{item.sublabel}</span>
              </span>
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
