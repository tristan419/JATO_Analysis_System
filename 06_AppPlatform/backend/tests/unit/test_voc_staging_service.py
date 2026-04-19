import json

from app.services import voc_staging_service


def test_load_voc_raw_sync_items_reads_country_payloads(tmp_path) -> None:
    raw_root = tmp_path / "voc"
    se_raw = raw_root / "se" / "raw"
    de_raw = raw_root / "de" / "raw"
    se_raw.mkdir(parents=True, exist_ok=True)
    de_raw.mkdir(parents=True, exist_ok=True)

    (se_raw / "forum.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "se_forum",
                    "country_code": "SE",
                    "country_label": "Sweden / 瑞典",
                },
                "collectedAt": "2026-04-19T09:00:00+00:00",
                "documentCount": 2,
                "documents": [{"url": "https://example.com/thread-1"}],
            }
        ),
        encoding="utf-8",
    )
    (de_raw / "comments.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "de_comments",
                    "country_code": "DE",
                    "country_label": "Germany / 德国",
                },
                "collectedAt": "2026-04-19T10:00:00+00:00",
                "documentCount": 1,
                "documents": [{"url": "https://example.com/thread-2"}],
            }
        ),
        encoding="utf-8",
    )
    (raw_root / "se" / "ignored.json").write_text("{}", encoding="utf-8")

    items = voc_staging_service.load_voc_raw_sync_items(root=raw_root)

    assert [item["countryCode"] for item in items] == ["DE", "SE"]
    assert items[0]["source"]["source_code"] == "de_comments"
    assert items[1]["countryLabel"] == "Sweden / 瑞典"


def test_load_voc_raw_sync_items_applies_country_filter(tmp_path) -> None:
    raw_root = tmp_path / "voc"
    se_raw = raw_root / "se" / "raw"
    se_raw.mkdir(parents=True, exist_ok=True)
    (se_raw / "forum.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "se_forum",
                    "country_code": "SE",
                },
                "collectedAt": "2026-04-19T09:00:00+00:00",
                "documents": [],
            }
        ),
        encoding="utf-8",
    )

    items = voc_staging_service.load_voc_raw_sync_items(
        root=raw_root,
        country_filter={"DE"},
    )

    assert items == []
