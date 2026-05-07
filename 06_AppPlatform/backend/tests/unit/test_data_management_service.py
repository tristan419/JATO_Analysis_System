import json
from types import SimpleNamespace

from app.services import data_management_service


def test_read_data_management_overview_without_database(
    tmp_path, monkeypatch
) -> None:
    project_root = tmp_path / "project"
    processed_root = project_root / "04_Processed_data"
    raw_root = project_root / "01_RAW_DATA"
    baseline_root = raw_root / "baseline"
    patch_root = raw_root / "patches"
    archive_root = raw_root / "historyDataArchive"
    parquet_path = processed_root / "jato_full_archive.parquet"
    partitioned_root = processed_root / "partitioned_dataset_v1"
    jobs_root = processed_root / "ops" / "jato_monthly_update_jobs"
    precomputed_root = processed_root / "precomputed"
    wiki_db_root = processed_root / "wiki"
    news_raw_root = processed_root / "news" / "raw"
    voc_raw_root = processed_root / "voc"
    voc_se_raw_root = voc_raw_root / "se" / "raw"
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
    news_raw_root.mkdir(parents=True, exist_ok=True)
    voc_se_raw_root.mkdir(parents=True, exist_ok=True)
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
    (news_raw_root / "news_batch_20260418T120000Z.json").write_text(
        json.dumps(
            [
                {
                    "batch_code": "country_news_batch_a",
                    "description": "Demo batch",
                    "country_count": 2,
                    "article_count": 7,
                    "countries": [],
                    "errors": [{"source_code": "fi_demo", "error": "timeout"}],
                }
            ]
        ),
        encoding="utf-8",
    )
    (voc_se_raw_root / "se_demo_source.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "se_demo_source",
                    "country_code": "SE",
                    "country_label": "Sweden / 瑞典",
                },
                "collectedAt": "2026-04-18T10:00:00+00:00",
                "documentCount": 3,
                "documents": [{"url": "https://example.com/thread"}],
                "errors": [],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(data_management_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(data_management_service, "PROCESSED_DATA_ROOT", processed_root)
    monkeypatch.setattr(data_management_service, "RAW_DATA_ROOT", raw_root)
    monkeypatch.setattr(data_management_service, "BASELINE_ROOT", baseline_root)
    monkeypatch.setattr(data_management_service, "PATCH_ROOT", patch_root)
    monkeypatch.setattr(data_management_service, "ARCHIVE_ROOT", archive_root)
    monkeypatch.setattr(data_management_service, "PARQUET_PATH", parquet_path)
    monkeypatch.setattr(data_management_service, "PARTITIONED_PATH", partitioned_root)
    monkeypatch.setattr(data_management_service, "JATO_MONTHLY_UPDATE_JOB_ROOT", jobs_root)
    monkeypatch.setattr(data_management_service, "PRECOMPUTED_DIR", precomputed_root)
    monkeypatch.setattr(data_management_service, "NEWS_RAW_ROOT", news_raw_root)
    monkeypatch.setattr(data_management_service, "VOC_RAW_ROOT", voc_raw_root)
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
    monkeypatch.setattr(
        data_management_service,
        "read_airflow_ops_status",
        lambda: {
            "available": False,
            "mode": "unavailable",
            "detail": "docker missing",
            "uiUrl": "http://127.0.0.1:8080",
            "running": False,
            "runningServices": 0,
            "totalServices": 3,
            "updatedAt": "2026-04-16T00:00:00+00:00",
            "services": [],
            "actions": {
                "canStart": False,
                "canStop": False,
                "canOpenUi": False,
            },
        },
    )

    payload = data_management_service.read_data_management_overview()

    assert payload["database"]["connected"] is False
    domains = {
        item["key"]: item
        for item in payload["domains"]
    }
    assert domains["jato"]["metrics"][1]["value"] == "2026 Apr"
    assert domains["airflow"]["key"] == "airflow"
    assert domains["news-raw"]["metrics"][0]["value"] == 1
    assert domains["news-raw"]["metrics"][2]["value"] == 7
    assert domains["voc-raw"]["metrics"][0]["value"] == 1
    assert domains["voc-raw"]["metrics"][2]["value"] == 3
    assert payload["activity"]["days"]
    assert any(item["key"] == "wiki-manifest" and item["exists"] for item in payload["fileInventory"])
    assert any(item["key"] == "news-raw-root" and item["exists"] for item in payload["fileInventory"])
    assert any(item["key"] == "voc-raw-root" and item["exists"] for item in payload["fileInventory"])
    assert any(item["key"] == "database" for item in payload["domains"])
    assert payload["airflow"]["available"] is False


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


def test_read_airflow_ops_status_without_docker(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management_service,
        "_docker_compose_airflow_base_command",
        lambda: None,
    )

    payload = data_management_service.read_airflow_ops_status()

    assert payload["available"] is False
    assert payload["mode"] == "unavailable"
    assert payload["actions"]["canStart"] is False
    assert payload["runningServices"] == 0


def test_read_airflow_ops_status_with_running_services(monkeypatch) -> None:
    monkeypatch.setattr(
        data_management_service,
        "_docker_compose_airflow_base_command",
        lambda: ["docker", "compose", "--profile", "airflow"],
    )

    def _fake_run(_command, *, timeout):
        return SimpleNamespace(
            returncode=0,
            stdout="\n".join(
                [
                    json.dumps(
                        {
                            "Service": "airflow-postgres",
                            "State": "running",
                            "Status": "Up 2 minutes",
                            "Health": "healthy",
                        }
                    ),
                    json.dumps(
                        {
                            "Service": "airflow-webserver",
                            "State": "running",
                            "Status": "Up 2 minutes",
                            "Publishers": [{"PublishedPort": 8080, "TargetPort": 8080}],
                        }
                    ),
                    json.dumps(
                        {
                            "Service": "airflow-scheduler",
                            "State": "running",
                            "Status": "Up 2 minutes",
                        }
                    ),
                ]
            ),
            stderr="",
        )

    monkeypatch.setattr(data_management_service, "_run_airflow_subprocess", _fake_run)

    payload = data_management_service.read_airflow_ops_status()

    assert payload["available"] is True
    assert payload["running"] is True
    assert payload["actions"]["canOpenUi"] is True
    assert payload["services"][1]["publishedPorts"] == ["8080->8080"]


def test_read_airflow_ops_status_with_json_array_output(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        data_management_service,
        "_docker_compose_airflow_base_command",
        lambda: ["docker", "compose", "--profile", "airflow"],
    )

    def _fake_run(_command, *, timeout):
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                [
                    {
                        "Service": "airflow-postgres",
                        "State": "running",
                        "Status": "Up 2 minutes",
                    },
                    {
                        "Service": "airflow-webserver",
                        "State": "running",
                        "Status": "Up 2 minutes",
                        "Publishers": [
                            {
                                "PublishedPort": 8080,
                                "TargetPort": 8080,
                            }
                        ],
                    },
                    {
                        "Service": "airflow-scheduler",
                        "State": "running",
                        "Status": "Up 2 minutes",
                    },
                ]
            ),
            stderr="",
        )

    monkeypatch.setattr(
        data_management_service,
        "_run_airflow_subprocess",
        _fake_run,
    )

    payload = data_management_service.read_airflow_ops_status()

    assert payload["available"] is True
    assert payload["running"] is True
    assert payload["services"][1]["service"] == "airflow-webserver"
    assert payload["services"][1]["publishedPorts"] == ["8080->8080"]


def test_start_airflow_stack_raises_runtime_error_when_compose_step_fails(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        data_management_service,
        "read_airflow_ops_status",
        lambda: {
            "running": False,
            "actions": {"canStop": False},
        },
    )
    monkeypatch.setattr(
        data_management_service,
        "_require_airflow_compose_base_command",
        lambda: ["docker", "compose", "--profile", "airflow"],
    )

    def _fake_run(command, *, timeout):
        if command[-1] == "airflow-postgres":
            return SimpleNamespace(
                returncode=1,
                stdout="",
                stderr="postgres failed",
            )
        raise AssertionError(f"unexpected command: {command}")

    monkeypatch.setattr(
        data_management_service,
        "_run_airflow_subprocess",
        _fake_run,
    )

    try:
        data_management_service.start_airflow_stack()
        assert False, "expected RuntimeError"
    except RuntimeError as exc:
        assert str(exc) == "postgres failed"


def test_stop_airflow_stack_raises_value_error_when_compose_unavailable(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        data_management_service,
        "read_airflow_ops_status",
        lambda: {
            "available": False,
            "actions": {"canStop": False},
        },
    )

    def _raise_value_error():
        raise ValueError("Docker Compose 不可用，当前环境无法执行本地 Airflow 控制命令。")

    monkeypatch.setattr(
        data_management_service,
        "_require_airflow_compose_base_command",
        _raise_value_error,
    )

    try:
        data_management_service.stop_airflow_stack()
        assert False, "expected ValueError"
    except ValueError as exc:
        assert str(exc) == "Docker Compose 不可用，当前环境无法执行本地 Airflow 控制命令。"


def test_read_voc_management_overview_returns_country_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    project_root = tmp_path / "project"
    voc_root = project_root / "04_Processed_data" / "voc"
    no_root = voc_root / "no"
    raw_root = no_root / "raw"
    enriched_path = no_root / "enriched" / "customer_insight_signals.json"
    deck_path = no_root / "deck" / "customer_insight_deck.json"
    docs_root = project_root / "Markdown_Readme" / "Fullstack" / "02_DataETL"
    toolkit_readme = project_root / "07_ScrapingToolkit" / "README.md"

    raw_root.mkdir(parents=True, exist_ok=True)
    enriched_path.parent.mkdir(parents=True, exist_ok=True)
    deck_path.parent.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    toolkit_readme.parent.mkdir(parents=True, exist_ok=True)

    (raw_root / "no_forum_source.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "no_forum_source",
                    "country_code": "NO",
                    "country_label": "Norway / 挪威",
                    "site_name": "Elbilforum",
                    "site_type": "forum",
                    "language": "no",
                },
                "collectedAt": "2026-04-20T10:00:00+00:00",
                "documentCount": 2,
                "autoReview": {
                    "publishTier": "review",
                    "publishDecision": "publish_ready",
                    "publishReadyCount": 1,
                },
                "documents": [
                    {
                        "url": "https://example.com/thread-1",
                        "textExtraction": {"method": "trafilatura"},
                    },
                    {
                        "url": "https://example.com/thread-2",
                        "textExtraction": {"method": "lxml_xpath"},
                    },
                ],
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    enriched_path.write_text(
        json.dumps(
            {
                "countryCode": "NO",
                "countryLabel": "Norway / 挪威",
                "generatedAt": "2026-04-20T11:00:00+00:00",
                "signalObservationCount": 6,
                "qualityScoreAvg": 0.82,
            }
        ),
        encoding="utf-8",
    )
    deck_path.write_text(
        json.dumps(
            {
                "countryCode": "NO",
                "countryLabel": "Norway / 挪威",
                "generatedAt": "2026-04-20T12:00:00+00:00",
                "observedSections": ["Pain points", "Product signals"],
                "inferredSections": ["Persona cues"],
                "painPoints": [{"label": "Charging", "count": 3, "sharePct": 0.5}],
                "productSignals": [{"label": "OTA", "count": 2, "sharePct": 0.33}],
                "evidenceCards": [
                    {
                        "title": "Charging issues in winter",
                        "url": "https://example.com/thread-1",
                        "siteName": "Elbilforum",
                        "publishTier": "review",
                        "signals": ["Charging"],
                        "evidenceSnippets": ["Cold-weather charging drop reported by owners."],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (docs_root / "VOC_FORUM_IMPLEMENTATION_STATUS_2026-04-19.md").write_text(
        "# status\n",
        encoding="utf-8",
    )
    (docs_root / "VOC_FORUM_SCRAPING_FEASIBILITY_2026-04-17.md").write_text(
        "# feasibility\n",
        encoding="utf-8",
    )
    toolkit_readme.write_text("# toolkit\n", encoding="utf-8")

    monkeypatch.setattr(data_management_service, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(data_management_service, "VOC_RAW_ROOT", voc_root)
    monkeypatch.setattr(
        data_management_service,
        "VOC_IMPLEMENTATION_STATUS_PATH",
        docs_root / "VOC_FORUM_IMPLEMENTATION_STATUS_2026-04-19.md",
    )
    monkeypatch.setattr(
        data_management_service,
        "VOC_FEASIBILITY_PATH",
        docs_root / "VOC_FORUM_SCRAPING_FEASIBILITY_2026-04-17.md",
    )
    monkeypatch.setattr(
        data_management_service,
        "VOC_TOOLKIT_README_PATH",
        toolkit_readme,
    )
    monkeypatch.setattr(
        data_management_service,
        "get_database_health",
        lambda: {"enabled": False, "connected": False, "detail": "disabled"},
    )

    payload = data_management_service.read_voc_management_overview("NO")

    assert payload["selectedCountryCode"] == "NO"
    assert payload["selectedCountryLabel"] == "Norway / 挪威"
    assert payload["availableCountries"][0]["deckReady"] is True
    assert payload["countryMetrics"][0]["value"] == 1
    assert payload["artifacts"][0]["exists"] is True
    assert payload["sourceRuns"][0]["textExtractionMethods"] == [
        "lxml_xpath × 1",
        "trafilatura × 1",
    ]
    assert payload["topPainPoints"][0]["label"] == "Charging"
    assert payload["evidenceCards"][0]["siteName"] == "Elbilforum"
    assert payload["staging"]["databaseConnected"] is False
    assert payload["documentation"][0]["exists"] is True
