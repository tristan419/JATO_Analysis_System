import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { apiUrl } from "../api/core";
import { fetchAuthEndpoint } from "../utils/authFallback";
import {
  CANDIDATE_RUNTIME_IDENTITY,
  isCandidatePreviewOrigin,
} from "../utils/candidateRuntime";

const AUTH_PROFILE_REFRESH_DELAY_MS = 30_000;
const AUTH_PROFILE_REFRESH_IDLE_TIMEOUT_MS = 8_000;

type AuthIdleWindow = Window & typeof globalThis & {
  requestIdleCallback?: (callback: () => void, options?: { timeout?: number }) => number;
  cancelIdleCallback?: (handle: number) => void;
};

function scheduleAuthProfileRefresh(callback: () => void): () => void {
  const idleWindow = window as AuthIdleWindow;
  let idleHandle: number | null = null;
  const delayHandle = window.setTimeout(() => {
    if (typeof idleWindow.requestIdleCallback === "function") {
      idleHandle = idleWindow.requestIdleCallback(callback, {
        timeout: AUTH_PROFILE_REFRESH_IDLE_TIMEOUT_MS,
      });
      return;
    }
    callback();
  }, AUTH_PROFILE_REFRESH_DELAY_MS);

  return () => {
    window.clearTimeout(delayHandle);
    if (idleHandle !== null) {
      idleWindow.cancelIdleCallback?.(idleHandle);
    }
  };
}

export interface User {
  username: string;
  role: string;
  email: string | null;
  oauthProvider: string | null;
  avatarUrl: string | null;
  displayName: string | null;
  primaryCountry: string | null;
  secondaryCountries: string[];
  preferredLandingPage: string | null;
  profileComplete: boolean;
}

interface AuthContextValue {
  user: User | null;
  token: string | null;
  profileLoaded: boolean;
  login: (username: string, password: string) => Promise<void>;
  refreshUser: () => Promise<void>;
  updateProfile: (payload: UserProfileUpdate) => Promise<User>;
  logout: () => void;
}

export interface UserProfileUpdate {
  primaryCountry: string | null;
  secondaryCountries: string[];
  preferredLandingPage?: string | null;
  displayName?: string | null;
}

const AuthContext = createContext<AuthContextValue | null>(null);

const STORAGE_TOKEN = "jato_auth_token";
const STORAGE_USER = "jato_user_name";
const STORAGE_ROLE = "jato_user_role";
const STORAGE_PRIMARY_COUNTRY = "jato_primary_country";
const STORAGE_SECONDARY_COUNTRIES = "jato_secondary_countries";
const STORAGE_PREFERRED_LANDING = "jato_preferred_landing_page";

function loginUrlAfterLogout(): string {
  if (isCandidatePreviewOrigin(window.location)) {
    return `${window.location.origin}/`;
  }
  if (window.location.hostname === "ojeur.cloud" || window.location.hostname === "www.ojeur.cloud") {
    return "https://www.ojeur.cloud/login";
  }
  return `${window.location.origin}/login`;
}

function normalizeUserPayload(data: Record<string, unknown>): User {
  const secondary = Array.isArray(data.secondaryCountries)
    ? data.secondaryCountries.map((item) => String(item)).filter(Boolean)
    : [];
  const primaryCountry = data.primaryCountry
    ? String(data.primaryCountry)
    : null;
  return {
    username: String(data.username ?? ""),
    role: String(data.role ?? "viewer"),
    email: data.email ? String(data.email) : null,
    oauthProvider: data.oauthProvider ? String(data.oauthProvider) : null,
    avatarUrl: data.avatarUrl ? String(data.avatarUrl) : null,
    displayName: data.displayName ? String(data.displayName) : null,
    primaryCountry,
    secondaryCountries: secondary,
    preferredLandingPage: data.preferredLandingPage
      ? String(data.preferredLandingPage)
      : null,
    profileComplete: Boolean(data.profileComplete ?? primaryCountry),
  };
}

function storeUser(user: User): void {
  localStorage.setItem(STORAGE_USER, user.username);
  localStorage.setItem(STORAGE_ROLE, user.role);
  if (user.primaryCountry) {
    localStorage.setItem(STORAGE_PRIMARY_COUNTRY, user.primaryCountry);
  } else {
    localStorage.removeItem(STORAGE_PRIMARY_COUNTRY);
  }
  localStorage.setItem(
    STORAGE_SECONDARY_COUNTRIES,
    JSON.stringify(user.secondaryCountries),
  );
  if (user.preferredLandingPage) {
    localStorage.setItem(STORAGE_PREFERRED_LANDING, user.preferredLandingPage);
  } else {
    localStorage.removeItem(STORAGE_PREFERRED_LANDING);
  }
}

function loadUser(): User | null {
  if (isCandidatePreviewOrigin(window.location)) {
    const candidateUser: User = {
      username: CANDIDATE_RUNTIME_IDENTITY.username,
      role: CANDIDATE_RUNTIME_IDENTITY.role,
      email: null,
      oauthProvider: null,
      avatarUrl: null,
      displayName: null,
      primaryCountry: null,
      secondaryCountries: [],
      preferredLandingPage: null,
      profileComplete: false,
    };
    localStorage.removeItem(STORAGE_TOKEN);
    storeUser(candidateUser);
    return candidateUser;
  }
  const username = localStorage.getItem(STORAGE_USER) || import.meta.env.VITE_USER_NAME || "anonymous";
  const role = localStorage.getItem(STORAGE_ROLE) || "viewer";
  // When running with the dev token (auth disabled), default to admin so
  // the local dev experience matches production admin behavior.
  const effectiveRole = import.meta.env.VITE_AUTH_TOKEN ? "admin" : role;
  let secondaryCountries: string[] = [];
  try {
    const parsed = JSON.parse(
      localStorage.getItem(STORAGE_SECONDARY_COUNTRIES) || "[]",
    );
    if (Array.isArray(parsed)) {
      secondaryCountries = parsed.map((item) => String(item)).filter(Boolean);
    }
  } catch {
    secondaryCountries = [];
  }
  const primaryCountry = localStorage.getItem(STORAGE_PRIMARY_COUNTRY);
  return {
    username,
    role: effectiveRole,
    email: null,
    oauthProvider: null,
    avatarUrl: null,
    displayName: null,
    primaryCountry,
    secondaryCountries,
    preferredLandingPage: localStorage.getItem(STORAGE_PREFERRED_LANDING),
    profileComplete: Boolean(primaryCountry),
  };
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(loadUser);
  const [token, setToken] = useState<string | null>(
    () => localStorage.getItem(STORAGE_TOKEN) || null,
  );
  const [profileLoaded, setProfileLoaded] = useState(true);

  const applyUser = useCallback((nextUser: User) => {
    storeUser(nextUser);
    setUser(nextUser);
  }, []);

  const refreshUser = useCallback(async () => {
    const candidateOrigin = isCandidatePreviewOrigin(window.location);
    const currentToken = candidateOrigin
      ? ""
      : (
        localStorage.getItem(STORAGE_TOKEN)
        || import.meta.env.VITE_AUTH_TOKEN
        || ""
      ).trim();
    if (!candidateOrigin && !currentToken) {
      setProfileLoaded(true);
      return;
    }
    const username = candidateOrigin
      ? CANDIDATE_RUNTIME_IDENTITY.username
      : localStorage.getItem(STORAGE_USER) || import.meta.env.VITE_USER_NAME || "anonymous";
    const res = await fetchAuthEndpoint("/auth/me", {
      headers: {
        ...(currentToken ? { "X-Auth-Token": currentToken } : {}),
        "X-User-Name": username,
      },
    });
    if (!res.ok) {
      setProfileLoaded(true);
      return;
    }
    const data = await res.json();
    applyUser(normalizeUserPayload(data as Record<string, unknown>));
    setProfileLoaded(true);
  }, [applyUser]);

  // Handle OAuth callback (token in URL params from Google / Feishu)
  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (isCandidatePreviewOrigin(window.location)) {
      if (params.has("token") || params.has("username") || params.has("role")) {
        window.history.replaceState({}, "", window.location.pathname);
      }
      return;
    }
    const urlToken = params.get("token");
    const urlUser = params.get("username");
    const urlRole = params.get("role");
    const isNewUser = params.get("isNewUser") === "true";
    if (urlToken && urlUser) {
      localStorage.setItem(STORAGE_TOKEN, urlToken);
      localStorage.removeItem("shared-filter-scope");
      setToken(urlToken);
      applyUser({
        username: urlUser,
        role: urlRole || "viewer",
        email: null,
        oauthProvider: "google",
        avatarUrl: null,
        displayName: null,
        primaryCountry: null,
        secondaryCountries: [],
        preferredLandingPage: null,
        profileComplete: false,
      });
      // Clean URL params and redirect new users to country setup
      if (isNewUser) {
        window.location.replace("/account/profile");
      } else {
        window.history.replaceState({}, "", window.location.pathname);
      }
    }
  }, [applyUser]);

  useEffect(() => {
    if (isCandidatePreviewOrigin(window.location)) {
      void refreshUser();
      return undefined;
    }
    const currentToken = (
      localStorage.getItem(STORAGE_TOKEN)
      || import.meta.env.VITE_AUTH_TOKEN
      || ""
    ).trim();
    if (!currentToken) {
      setProfileLoaded(true);
      return;
    }
    return scheduleAuthProfileRefresh(() => {
      void refreshUser();
    });
  }, [refreshUser, token]);

  const login = useCallback(async (username: string, password: string) => {
    const res = await fetchAuthEndpoint("/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    if (!res.ok) {
      const msg = await res.text();
      throw new Error(msg || "Login failed");
    }
    const data = await res.json();
    const nextUser = normalizeUserPayload(data as Record<string, unknown>);
    localStorage.setItem(STORAGE_TOKEN, data.token);
    localStorage.removeItem("shared-filter-scope");
    localStorage.removeItem("dashboard-cache");
    setToken(data.token);
    applyUser(nextUser);
  }, [applyUser]);

  const updateProfile = useCallback(async (payload: UserProfileUpdate) => {
    const currentToken = localStorage.getItem(STORAGE_TOKEN);
    const res = await fetch(apiUrl("/auth/me/profile"), {
      method: "PATCH",
      headers: {
        "Content-Type": "application/json",
        ...(currentToken ? { "X-Auth-Token": currentToken } : {}),
        "X-User-Name": localStorage.getItem(STORAGE_USER) || "anonymous",
      },
      body: JSON.stringify({ ...payload, displayName: payload.displayName }),
    });
    if (!res.ok) {
      const msg = await res.text();
      throw new Error(msg || "Profile update failed");
    }
    const nextUser = normalizeUserPayload(await res.json() as Record<string, unknown>);
    localStorage.removeItem("shared-filter-scope");
    localStorage.removeItem("dashboard-cache");
    applyUser(nextUser);
    return nextUser;
  }, [applyUser]);

  const logout = useCallback(() => {
    const currentToken = localStorage.getItem(STORAGE_TOKEN);
    if (currentToken) {
      void fetch(apiUrl("/auth/logout"), {
        method: "POST",
        keepalive: true,
        headers: {
          "X-Auth-Token": currentToken,
          "X-User-Name": localStorage.getItem(STORAGE_USER) || "anonymous",
        },
      }).catch(() => undefined);
    }
    localStorage.removeItem(STORAGE_TOKEN);
    localStorage.removeItem(STORAGE_USER);
    localStorage.removeItem(STORAGE_ROLE);
    localStorage.removeItem(STORAGE_PRIMARY_COUNTRY);
    localStorage.removeItem(STORAGE_SECONDARY_COUNTRIES);
    localStorage.removeItem(STORAGE_PREFERRED_LANDING);
    setToken(null);
    setUser(null);
    window.location.assign(loginUrlAfterLogout());
  }, []);

  const value = useMemo(
    () => ({ user, token, profileLoaded, login, refreshUser, updateProfile, logout }),
    [user, token, profileLoaded, login, refreshUser, updateProfile, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
