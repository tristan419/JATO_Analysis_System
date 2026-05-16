import { useEffect } from "react";
import { Outlet } from "react-router-dom";
import { CountryChatWidget } from "./CountryChatWidget";
import { MegaMenu } from "./MegaMenu";
import { PresenceWidget } from "./PresenceWidget";

export function Layout() {
  useEffect(() => {
    document.documentElement.style.scrollBehavior = "auto";
    return () => { document.documentElement.style.scrollBehavior = ""; };
  }, []);

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
