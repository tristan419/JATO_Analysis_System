import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "react-router-dom";

import { CountryChatWidget } from "./CountryChatWidget";

const NAV_ITEMS = [
  { to: "/", code: "01", label: "Overview", sublabel: "Dashboard" },
  { to: "/msrp", code: "06", label: "MSRP", sublabel: "当前价格" },
  { to: "/market-scan", code: "07", label: "Scan", sublabel: "市场扫描" },
  { to: "/positioning-pricing", code: "08", label: "Pricing", sublabel: "定位定价" },
  { to: "/version-comparison", code: "09", label: "Compare", sublabel: "版型对比" },
  { to: "/customer-insights", code: "10", label: "Customer", sublabel: "看客户" },
  { to: "/copilot", code: "11", label: "Copilot", sublabel: "国家助手" },
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
      <CountryChatWidget />
    </div>
  );
}
