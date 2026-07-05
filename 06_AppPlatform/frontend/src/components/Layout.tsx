import { Suspense, lazy, useEffect, useState } from "react";
import { Outlet } from "react-router-dom";
import { MegaMenu } from "./MegaMenu";

const PresenceWidget = lazy(() =>
  import("./PresenceWidget").then((module) => ({ default: module.PresenceWidget }))
);
const CountryChatWidgetHost = lazy(() =>
  import("./CountryChatWidgetHost").then((module) => ({ default: module.CountryChatWidgetHost }))
);

export const AUXILIARY_WIDGET_DELAY_MS = 30_000;
export const AUXILIARY_WIDGET_IDLE_TIMEOUT_MS = 8_000;

type IdleWindow = Window & typeof globalThis & {
  requestIdleCallback?: (callback: () => void, options?: { timeout?: number }) => number;
  cancelIdleCallback?: (handle: number) => void;
};

function scheduleAuxiliaryWidgets(callback: () => void): () => void {
  const idleWindow = window as IdleWindow;
  let idleHandle: number | null = null;
  const timeoutHandle = window.setTimeout(() => {
    if (typeof idleWindow.requestIdleCallback === "function") {
      idleHandle = idleWindow.requestIdleCallback(callback, {
        timeout: AUXILIARY_WIDGET_IDLE_TIMEOUT_MS,
      });
      return;
    }
    callback();
  }, AUXILIARY_WIDGET_DELAY_MS);

  return () => {
    window.clearTimeout(timeoutHandle);
    if (idleHandle !== null) idleWindow.cancelIdleCallback?.(idleHandle);
  };
}

export function Layout() {
  const [showAuxiliaryWidgets, setShowAuxiliaryWidgets] = useState(false);

  useEffect(() => {
    document.documentElement.style.scrollBehavior = "auto";
    return () => { document.documentElement.style.scrollBehavior = ""; };
  }, []);

  useEffect(() => scheduleAuxiliaryWidgets(() => {
    setShowAuxiliaryWidgets(true);
  }), []);

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
      {showAuxiliaryWidgets && (
        <Suspense fallback={null}>
          <PresenceWidget />
          <CountryChatWidgetHost />
        </Suspense>
      )}
    </div>
  );
}
