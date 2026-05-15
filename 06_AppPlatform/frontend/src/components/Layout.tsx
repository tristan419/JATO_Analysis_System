import { useEffect, useState } from "react";
import { Link, Outlet, useLocation } from "react-router-dom";

import { CountryChatWidget } from "./CountryChatWidget";
import { PresenceWidget } from "./PresenceWidget";
import { useAuth } from "../contexts/AuthContext";
import { PAGE_NAV_ITEMS } from "../utils/pageNavigation";

export function Layout() {
  const location = useLocation();
  const [navOpen, setNavOpen] = useState(false);
  const { user, logout } = useAuth();

  const isActive = (path: string) =>
    path === "/"
      ? location.pathname === "/"
      : location.pathname.startsWith(path);

  useEffect(() => {
    setNavOpen(false);
  }, [location.pathname]);

  return (
    <div className="app-root">
      <header className={`top-bar${navOpen ? " is-nav-open" : ""}`}>
        <div className="top-bar-main">
          <div className="top-bar-brand">
            <span className="top-bar-brand-eyebrow">
              JATO Analysis System
            </span>
            <span className="top-bar-brand-title">
              Market Intelligence Control Deck
            </span>
          </div>

          <div style={{ marginLeft: "auto", marginRight: 12, display: "flex", alignItems: "center", gap: 10, fontSize: 11, color: "#94a3b8" }}>
            {user ? (
              <>
                <span style={{ color: "#e2e8f0", fontWeight: 500 }}>{user.username}</span>
                <span style={{ fontSize: 10, color: "#64748b", background: "rgba(255,255,255,0.06)", padding: "1px 6px", borderRadius: 4 }}>{user.role}</span>
                <button type="button" onClick={logout} style={{ background: "none", border: "1px solid rgba(255,255,255,0.08)", color: "#94a3b8", fontSize: 10, padding: "2px 8px", borderRadius: 4, cursor: "pointer" }}>Sign out</button>
              </>
            ) : (
              <Link to="/login" style={{ color: "#94a3b8", textDecoration: "none" }}>Sign in</Link>
            )}
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
        <nav
          id="primary-navigation"
          className={`top-bar-nav${navOpen ? " is-open" : ""}`}
          aria-label="Primary"
        >
          {PAGE_NAV_ITEMS.map((item) => (
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
      <PresenceWidget />
      <CountryChatWidget />
    </div>
  );
}
