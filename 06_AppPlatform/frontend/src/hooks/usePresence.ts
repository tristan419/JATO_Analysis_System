import { useCallback, useEffect, useRef, useState } from "react";
import { useLocation } from "react-router-dom";

const HEARTBEAT_INTERVAL_MS = 30_000;
const SESSION_KEY = "jato_presence_session_id";
const USER_NAME_KEY = "jato_user_name";

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
  return localStorage.getItem(USER_NAME_KEY)?.trim() || "operator";
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

export function usePresence() {
  const location = useLocation();
  const sessionIdRef = useRef(getSessionId());
  const [snapshot, setSnapshot] = useState<PresenceSnapshot>({
    online: 0,
    samePage: 0,
    users: [],
  });

  const sendHeartbeat = useCallback(() => {
    const token =
      localStorage.getItem("jato_auth_token") ||
      import.meta.env.VITE_AUTH_TOKEN;
    const body = {
      session_id: sessionIdRef.current,
      user_name: getUserName(),
      current_page: location.pathname,
    };
    fetch("/v1/presence/heartbeat", {
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
            users: d.users ?? [],
          });
        }
      })
      .catch(() => {});
  }, [location.pathname]);

  useEffect(() => {
    sendHeartbeat();
    const interval = setInterval(sendHeartbeat, HEARTBEAT_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [sendHeartbeat]);

  return snapshot;
}
