import { Navigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import type { ReactNode } from "react";
import { isKnownAppRoute, isRouteAllowedForRole } from "../utils/pageNavigation";
import { LoadingSurface } from "./LoadingSurface";

export function RequireRole({ children }: { children: ReactNode }) {
  const { user, profileLoaded } = useAuth();

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

  if (user?.role) {
    const path = window.location.pathname;
    if (isKnownAppRoute(path) && !isRouteAllowedForRole(path, user.role)) {
      return <Navigate to="/dashboard" replace />;
    }
  }

  return <>{children}</>;
}
