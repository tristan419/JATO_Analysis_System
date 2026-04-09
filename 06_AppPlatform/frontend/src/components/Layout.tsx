import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", code: "01", label: "Overview", sublabel: "Dashboard" },
  { to: "/specification", code: "02", label: "Specification", sublabel: "规格明细" },
  { to: "/crud", code: "03", label: "Control", sublabel: "数据管理" },
];

export function Layout() {
  const location = useLocation();
  const [navOpen, setNavOpen] = useState(false);

  const isActive = (path: string) =>
    path === "/" ? location.pathname === "/" : location.pathname.startsWith(path);

  useEffect(() => {
    setNavOpen(false);
  }, [location.pathname]);

  return (
    <div className="app-root">
      <header className={`top-bar${navOpen ? " is-nav-open" : ""}`}>
        <div className="top-bar-main">
          <div className="top-bar-brand">
            <span className="top-bar-brand-eyebrow">JATO Analysis System</span>
            <span className="top-bar-brand-title">Market Intelligence Control Deck</span>
          </div>
          <button
            type="button"
            className={`top-bar-menu-toggle${navOpen ? " is-open" : ""}`}
            aria-expanded={navOpen}
            aria-controls="primary-navigation"
            aria-label={navOpen ? "收起主导航" : "展开主导航"}
            onClick={() => setNavOpen((current) => !current)}
          >
            <span aria-hidden="true" />
            <span aria-hidden="true" />
            <span aria-hidden="true" />
          </button>
        </div>
        <nav id="primary-navigation" className={`top-bar-nav${navOpen ? " is-open" : ""}`} aria-label="Primary">
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
