import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation } from "react-router-dom";

import { apiUrl } from "../api/client";

const HEARTBEAT_INTERVAL_MS = 30_000;
const INITIAL_HEARTBEAT_DELAY_MS = 8_000;
const SESSION_KEY = "jato_presence_session_id";
const USER_NAME_KEY = "jato_user_name";

function isDocumentVisible(): boolean {
  return typeof document === "undefined" || document.visibilityState === "visible";
}

function getSessionId(): string {
  // sessionStorage is per-tab — two tabs = two distinct sessions
  const cached = sessionStorage.getItem(SESSION_KEY);
  if (cached) return cached;
  const id =
    crypto.randomUUID?.() ??
    `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  sessionStorage.setItem(SESSION_KEY, id);
  return id;
}

function getUserName(): string {
  return (
    localStorage.getItem(USER_NAME_KEY)?.trim() ||
    localStorage.getItem("jato_user_name")?.trim() ||
    "anonymous"
  );
}

export interface PresenceUser {
  user_name: string;
  role: string;
  current_page: string;
  last_seen_ago_s: number;
}

export interface PresenceSnapshot {
  online: number;
  samePage: number;
  users: PresenceUser[];
}

export function usePresence(includeUsers = false) {
  const location = useLocation();
  const sessionIdRef = useRef(getSessionId());
  const [snapshot, setSnapshot] = useState<PresenceSnapshot>({
    online: 0,
    samePage: 0,
    users: [],
  });

  const sendHeartbeat = useCallback(() => {
    if (!isDocumentVisible()) return;
    const token =
      localStorage.getItem("jato_auth_token") ||
      import.meta.env.VITE_AUTH_TOKEN;
    const body = {
      session_id: sessionIdRef.current,
      user_name: getUserName(),
      current_page: location.pathname,
    };
    fetch(apiUrl(`/presence/heartbeat?include_users=${includeUsers ? "true" : "false"}`), {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { "X-Auth-Token": token } : {}),
      },
      body: JSON.stringify(body),
    })
      .then((r) => r.json())
      .then((d) => {
        if (typeof d.online === "number") {
          setSnapshot({
            online: d.online,
            samePage: d.same_page ?? 0,
            users: Array.isArray(d.users) ? d.users : [],
          });
        }
      })
      .catch(() => {});
  }, [includeUsers, location.pathname]);

  useEffect(() => {
    let interval: number | null = null;
    const startInterval = () => {
      if (interval !== null || !isDocumentVisible()) return;
      interval = window.setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS);
    };
    const stopInterval = () => {
      if (interval === null) return;
      window.clearInterval(interval);
      interval = null;
    };
    const handleVisibilityChange = () => {
      if (isDocumentVisible()) {
        sendHeartbeat();
        startInterval();
      } else {
        stopInterval();
      }
    };

    const initial = window.setTimeout(() => {
      sendHeartbeat();
      startInterval();
    }, INITIAL_HEARTBEAT_DELAY_MS);
    startInterval();
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      window.clearTimeout(initial);
      stopInterval();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [sendHeartbeat]);

  useEffect(() => {
    if (includeUsers) sendHeartbeat();
  }, [includeUsers, sendHeartbeat]);

  return snapshot;
}
