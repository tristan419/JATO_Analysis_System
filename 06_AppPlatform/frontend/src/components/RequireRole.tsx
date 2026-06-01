import { Navigate } from "react-router-dom";
import { useAuth } from "../contexts/AuthContext";
import type { ReactNode } from "react";
import { isRouteAllowedForRole } from "../utils/pageNavigation";

export function RequireRole({ children }: { children: ReactNode }) {
  const { user, profileLoaded } = useAuth();

  if (!profileLoaded) return null;

  if (user?.role) {
    const path = window.location.pathname;
    if (!isRouteAllowedForRole(path, user.role)) {
      return <Navigate to="/dashboard" replace />;
    }
  }

  return <>{children}</>;
}
