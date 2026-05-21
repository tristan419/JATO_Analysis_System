from app.services.market_scan_cache import invalidate_market_scan_deck_cache


class _FakeRedis:
    def __init__(self, keys: list[str]) -> None:
        self.keys = keys
        self.deleted: list[str] = []

    def scan_iter(self, *, match: str, count: int):
        prefix = match.rstrip("*")
        for key in self.keys:
            if key.startswith(prefix):
                yield key

    def delete(self, *keys: str) -> int:
        self.deleted.extend(keys)
        return len(keys)


def test_invalidate_market_scan_deck_cache_deletes_schema_keys() -> None:
    client = _FakeRedis(
        [
            "ms:deck:v4:Sweden:2026-03:default:dtabc",
            "ms:deck:v4:Sweden:2026-03:default:dtabc:lock",
            "ms:deck:v3:Sweden:2026-03:default:dtold",
            "other:key",
        ]
    )

    result = invalidate_market_scan_deck_cache(client, schema_version=4)

    assert result["enabled"] is True
    assert result["pattern"] == "ms:deck:v4:*"
    assert result["deletedCount"] == 2
    assert client.deleted == [
        "ms:deck:v4:Sweden:2026-03:default:dtabc",
        "ms:deck:v4:Sweden:2026-03:default:dtabc:lock",
    ]


def test_invalidate_market_scan_deck_cache_handles_missing_client() -> None:
    result = invalidate_market_scan_deck_cache(None, schema_version=4)

    assert result["enabled"] is False
    assert result["deletedCount"] == 0
