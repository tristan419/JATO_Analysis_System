import { Navigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import type { ReactNode } from "react";
import { isCandidatePreviewOrigin } from "../utils/candidateRuntime";
import { isKnownAppRoute, isRouteAllowedForRole } from "../utils/pageNavigation";
import { LoadingSurface } from "./LoadingSurface";

export function RequireRole({ children }: { children: ReactNode }) {
  const { user, token, profileLoaded } = useAuth();

  if (!profileLoaded) {
    return (
      <div className="app-loading-shell">
        <LoadingSurface
          mode="overlay"
          label="正在加载账号"
          detail="同步访问权限与页面入口"
          kicker="Auth"
        />
      </div>
    );
  }

  if (isCandidatePreviewOrigin(window.location) && !token) {
    const redirect = `${window.location.pathname}${window.location.search}`;
    return (
      <Navigate
        to={`/login?redirect=${encodeURIComponent(redirect)}`}
        replace
      />
    );
  }

  if (user?.role) {
    const path = window.location.pathname;
    if (isKnownAppRoute(path) && !isRouteAllowedForRole(path, user.role)) {
      return <Navigate to="/dashboard" replace />;
    }
  }

  return <>{children}</>;
}
