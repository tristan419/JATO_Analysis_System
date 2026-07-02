"""Presence service — in-memory session tracker. No external infra required."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class Session:
    session_id: str
    user_name: str
    current_page: str
    role: str = "anonymous"
    last_seen: float = field(default_factory=time.time)


class PresenceStore:
    """Thread-safe in-memory session tracker."""

    def __init__(self, ttl_seconds: int = 120) -> None:
        self._sessions: dict[str, Session] = {}
        self._ttl = ttl_seconds
        self._lock = threading.Lock()

    def heartbeat(
        self,
        session_id: str,
        user_name: str,
        current_page: str,
        role: str = "anonymous",
        include_users: bool = True,
    ) -> dict:
        with self._lock:
            self._cleanup()
            session = Session(
                session_id=session_id,
                user_name=user_name,
                current_page=current_page,
                role=role,
            )
            self._sessions[session_id] = session
            return self._snapshot(current_page, include_users=include_users)

    def get_online(
        self,
        current_page: str | None = None,
        include_users: bool = True,
    ) -> dict:
        with self._lock:
            self._cleanup()
            return self._snapshot(current_page, include_users=include_users)

    def _cleanup(self) -> None:
        now = time.time()
        expired = [
            sid
            for sid, s in self._sessions.items()
            if now - s.last_seen > self._ttl
        ]
        for sid in expired:
            del self._sessions[sid]

    def _snapshot(
        self,
        current_page: str | None = None,
        include_users: bool = True,
    ) -> dict:
        now = time.time()
        sessions = list(self._sessions.values())
        same_page = (
            sum(1 for session in sessions if session.current_page == current_page)
            if current_page
            else 0
        )
        snapshot = {
            "online": len(sessions),
            "same_page": same_page,
        }
        if include_users:
            users = [
                {
                    "user_name": s.user_name,
                    "role": s.role,
                    "current_page": s.current_page,
                    "last_seen_ago_s": int(now - s.last_seen),
                }
                for s in sessions
            ]
            snapshot["users"] = sorted(users, key=lambda u: u["user_name"])
        return snapshot


# Module-level singleton — survives between requests, dies on process restart
presence_store = PresenceStore(ttl_seconds=30)
