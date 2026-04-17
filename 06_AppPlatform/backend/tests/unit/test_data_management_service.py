import json
from types import SimpleNamespace

from app.services import data_management_service


def test_read_data_management_overview_without_database(
    tmp_path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    raw_root = project_root / "01_RAW_DATA"
    baseline_root = raw_root / "baseline"
    patch_root = raw_root / "patches"
    archive_root = raw_root / "historyDataArchive"
    parquet_path = project_root / "04_Processed_data" / "jato_full_archive.parquet"
    partitioned_root = project_root / "04_Processed_data" / "partitioned_dataset_v1"
    jobs_root = project_root / "04_Processed_data" / "ops" / "jato_monthly_update_jobs"
    precomputed_root = project_root / "04_Processed_data" / "precomputed"
    wiki_db_root = project_root / "04_Processed_data" / "wiki"
    wiki_manifest_path = wiki_db_root / "manifest.json"

    baseline_root.mkdir(parents=True, exist_ok=True)
    patch_root.mkdir(parents=True, exist_ok=True)
    archive_root.mkdir(parents=True, exist_ok=True)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    parquet_path.write_bytes(b"parquet")
    partitioned_root.mkdir(parents=True, exist_ok=True)
    jobs_root.mkdir(parents=True, exist_ok=True)
    precomputed_root.mkdir(parents=True, exist_ok=True)
    wiki_db_root.mkdir(parents=True, exist_ok=True)
    wiki_manifest_path.write_text(
        json.dumps(
            {
                "collectionName": "vehicle_wiki",
                "documentCount": 12,
                "sourcePath": "Markdown_Readme/wiki",
            }
        ),
        encoding="utf-8",
    )
    (baseline_root / "JATO-2026.03-full.xlsx").write_bytes(b"baseline")
    (patch_root / "2026-04").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(data_management_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(data_management_service, "RAW_DATA_ROOT", raw_root)
    monkeypatch.setattr(data_management_service, "BASELINE_ROOT", baseline_root)
    monkeypatch.setattr(data_management_service, "PATCH_ROOT", patch_root)
    monkeypatch.setattr(data_management_service, "ARCHIVE_ROOT", archive_root)
    monkeypatch.setattr(data_management_service, "PARQUET_PATH", parquet_path)
    monkeypatch.setattr(data_management_service, "PARTITIONED_PATH", partitioned_root)
    monkeypatch.setattr(data_management_service, "JATO_MONTHLY_UPDATE_JOB_ROOT", jobs_root)
    monkeypatch.setattr(data_management_service, "PRECOMPUTED_DIR", precomputed_root)
    monkeypatch.setattr(data_management_service, "WIKI_MANIFEST_PATH", wiki_manifest_path)
    monkeypatch.setattr(data_management_service, "WIKI_DB_ROOT", wiki_db_root)
    monkeypatch.setattr(
        data_management_service,
        "get_data_freshness",
        lambda: [
            {"country": "China", "latestMonth": "2026 Apr", "monthsInWindow": 12},
            {"country": "Germany", "latestMonth": "2026 Mar", "monthsInWindow": 11},
        ],
    )
    monkeypatch.setattr(
        data_management_service,
        "list_jato_monthly_update_jobs",
        lambda limit=5: {
            "rows": 1,
            "items": [
                {
                    "jobId": "jato-update-1",
                    "month": "2026-04",
                    "status": "success",
                    "createdAt": "2026-04-16T00:00:00+00:00",
                    "updatedAt": "2026-04-16T00:30:00+00:00",
                }
            ],
        },
    )
    monkeypatch.setattr(
        data_management_service,
        "get_country_chat_metadata",
        lambda: {
            "availableCountries": [
                {"label": "China", "value": "China"},
                {"label": "Germany", "value": "Germany"},
            ],
            "availableChatModels": [{"id": "gpt-5.4", "label": "GPT-5.4"}],
            "providerAvailable": True,
        },
    )
    monkeypatch.setattr(
        data_management_service,
        "get_database_health",
        lambda: {"enabled": False, "connected": False, "detail": "disabled"},
    )

    payload = data_management_service.read_data_management_overview()

    assert payload["database"]["connected"] is False
    assert payload["domains"][0]["key"] == "jato"
    assert payload["domains"][0]["metrics"][1]["value"] == "2026 Apr"
    assert payload["activity"]["days"]
    assert any(item["key"] == "wiki-manifest" and item["exists"] for item in payload["fileInventory"])
    assert any(item["key"] == "database" for item in payload["domains"])


def test_collect_recent_snapshot_items_deduplicates_latest_path() -> None:
    class _FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def all(self):
            return self._rows

    class _FakeSession:
        def __init__(self):
            self.calls = 0

        def execute(self, _statement):
            self.calls += 1
            if self.calls == 1:
                return _FakeResult(
                    [
                        SimpleNamespace(
                            country="China",
                            brand="BYD",
                            jato_model="Seal",
                            source_snapshot_path="snapshots/byd/seal-1.html",
                            observed_at_utc=data_management_service.datetime.fromisoformat(
                                "2026-04-15T00:00:00+00:00"
                            ),
                        )
                    ]
                )
            if self.calls == 2:
                return _FakeResult(
                    [
                        SimpleNamespace(
                            country="China",
                            brand="BYD",
                            jato_model="Seal",
                            source_snapshot_path="snapshots/byd/seal-1.html",
                            updated_at_utc=data_management_service.datetime.fromisoformat(
                                "2026-04-16T00:00:00+00:00"
                            ),
                        ),
                        SimpleNamespace(
                            country="Germany",
                            brand="VW",
                            jato_model="ID.7",
                            source_snapshot_path="snapshots/vw/id7-1.html",
                            updated_at_utc=data_management_service.datetime.fromisoformat(
                                "2026-04-14T00:00:00+00:00"
                            ),
                        ),
                    ]
                )
            return _FakeResult(
                [
                    SimpleNamespace(
                        country="France",
                        brand="Renault",
                        jato_model="Megane",
                        source_snapshot_path="snapshots/renault/megane-1.html",
                        updated_at_utc=data_management_service.datetime.fromisoformat(
                            "2026-04-13T00:00:00+00:00"
                        ),
                    )
                ]
            )

    items = data_management_service._collect_recent_snapshot_items(_FakeSession())

    assert [item["value"] for item in items] == [
        "snapshots/byd/seal-1.html",
        "snapshots/vw/id7-1.html",
        "snapshots/renault/megane-1.html",
    ]
    assert items[0]["group"] == "Current Price"
    assert items[0]["updatedAt"] == "2026-04-16T00:00:00+00:00"
