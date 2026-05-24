import { useEffect } from "react";
import { Outlet, useLocation, useNavigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import { CountryChatWidget } from "./CountryChatWidget";
import { MegaMenu } from "./MegaMenu";
import { PresenceWidget } from "./PresenceWidget";

export function Layout() {
  const { user, profileLoaded } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    document.documentElement.style.scrollBehavior = "auto";
    return () => { document.documentElement.style.scrollBehavior = ""; };
  }, []);

  useEffect(() => {
    if (!profileLoaded || !user || user.profileComplete) return;
    if (location.pathname === "/account/country-setup") return;
    navigate("/account/country-setup", { replace: true });
  }, [location.pathname, navigate, profileLoaded, user]);

  return (
    <div className="app-root">
      <header className="top-bar">
        <div className="top-bar-main">
          <div className="top-bar-brand">
            <span className="top-bar-brand-eyebrow">JATO Analysis System</span>
            <span className="top-bar-brand-title">Market Intelligence Control Deck</span>
          </div>
          <MegaMenu />
        </div>
      </header>
      <main className="main-area"><Outlet /></main>
      <PresenceWidget />
      <CountryChatWidget />
    </div>
  );
}
