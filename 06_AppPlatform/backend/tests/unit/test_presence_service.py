from app.services.presence_service import PresenceStore


def test_heartbeat_omits_users_when_not_requested() -> None:
    store = PresenceStore(ttl_seconds=30)

    snapshot = store.heartbeat(
        session_id="session-1",
        user_name="tristan",
        current_page="/dashboard",
        role="editor",
        include_users=False,
    )

    assert snapshot == {
        "online": 1,
        "same_page": 1,
    }


def test_heartbeat_includes_users_when_requested() -> None:
    store = PresenceStore(ttl_seconds=30)

    snapshot = store.heartbeat(
        session_id="session-1",
        user_name="tristan",
        current_page="/dashboard",
        role="editor",
        include_users=True,
    )

    assert snapshot["online"] == 1
    assert snapshot["same_page"] == 1
    assert snapshot["users"] == [
        {
            "user_name": "tristan",
            "role": "editor",
            "current_page": "/dashboard",
            "last_seen_ago_s": 0,
        }
    ]
