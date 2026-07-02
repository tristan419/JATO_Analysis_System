from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from app.db.models import CurrentPrice, MsrpObservation, MsrpSource, PriceHistory
from app.services import msrp_monitoring_service


def _minimal_pdf_bytes(text: str) -> bytes:
    content = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode("latin-1")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        b"<< /Length " + str(len(content)).encode("ascii") + b" >>\nstream\n" + content + b"\nendstream",
    ]
    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, payload in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{index} 0 obj\n".encode("ascii"))
        pdf.extend(payload)
        pdf.extend(b"\nendobj\n")
    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n".encode("ascii")
    )
    return bytes(pdf)


def _observation(
    *,
    observation_id: UUID,
    source_id: UUID,
    scrape_batch_id: UUID,
    observed_at: datetime,
    country: str = "se",
    brand: str = "Volvo",
    jato_model: str = "XC60",
    jato_trim: str = "Ultra",
    jato_powertrain: str = "PHEV",
    msrp_value: str = "65000.00",
    source_msrp_value: str = "747500.00",
    source_currency: str = "SEK",
    match_confidence: str = "0.7600",
    match_status: str = "review_required",
    length_mm: int = 4708,
    dryrun_run_id: str | None = "msrp-dryrun-20260620-010203",
    source_payload_hash: str = "hash-monitoring",
    match_reason_json: dict[str, object] | None = None,
    source_context_json: dict[str, object] | None = None,
) -> MsrpObservation:
    context_json = (
        source_context_json
        if source_context_json is not None
        else {
            "dryrunRunId": dryrun_run_id,
            "vehicle": {"length_mm": length_mm},
        }
    )
    return MsrpObservation(
        observation_id=observation_id,
        scrape_batch_id=scrape_batch_id,
        source_id=source_id,
        country=country,
        brand=brand,
        jato_model=jato_model,
        jato_trim=jato_trim,
        jato_powertrain=jato_powertrain,
        official_model=jato_model,
        official_trim=jato_trim,
        official_edition=None,
        official_powertrain=jato_powertrain,
        msrp_value=Decimal(msrp_value),
        currency="EUR",
        source_msrp_value=Decimal(source_msrp_value),
        source_currency=source_currency,
        fx_rate_to_eur=Decimal("0.08695652"),
        fx_rate_as_of_date=date(2026, 6, 20),
        fx_source="unit-test",
        tax_included=True,
        price_label="List price",
        availability_text=None,
        observed_at_utc=observed_at,
        source_url=f"https://example.test/{country}/{brand}/{jato_model}",
        source_snapshot_path="snapshots/volvo-xc60.html",
        source_payload_hash=source_payload_hash,
        extraction_version="v1",
        match_confidence=Decimal(match_confidence),
        match_status=match_status,
        match_reason_json=match_reason_json if match_reason_json is not None else {"resolver": "unit-test"},
        source_context_json=context_json,
        created_at_utc=observed_at,
        updated_at_utc=observed_at,
    )


def _current_price(
    *,
    current_price_id: UUID,
    observation: MsrpObservation,
    updated_at: datetime,
) -> CurrentPrice:
    return CurrentPrice(
        current_price_id=current_price_id,
        country=observation.country,
        brand=observation.brand,
        jato_model=observation.jato_model,
        jato_trim=observation.jato_trim,
        jato_powertrain=observation.jato_powertrain or "",
        official_model=observation.official_model,
        official_trim=observation.official_trim,
        official_edition=observation.official_edition,
        official_powertrain=observation.official_powertrain,
        effective_observation_id=observation.observation_id,
        current_msrp_value=observation.msrp_value,
        currency=observation.currency,
        source_msrp_value=observation.source_msrp_value,
        source_currency=observation.source_currency,
        fx_rate_to_eur=observation.fx_rate_to_eur,
        fx_rate_as_of_date=observation.fx_rate_as_of_date,
        fx_source=observation.fx_source,
        tax_included=observation.tax_included,
        match_confidence=observation.match_confidence,
        match_status=observation.match_status,
        source_url=observation.source_url,
        source_snapshot_path=observation.source_snapshot_path,
        last_price_change_at_utc=updated_at,
        updated_at_utc=updated_at,
    )


def _source(
    source_id: UUID,
    *,
    country: str = "se",
    brand: str = "Volvo",
    source_code: str = "volvo_xc60_se_draft_scrapling",
    source_type: str = "third_party_reference",
) -> MsrpSource:
    return MsrpSource(
        source_id=source_id,
        source_code=source_code,
        country=country,
        brand=brand,
        source_url="https://www.volvocars.com/se/cars/xc60/",
        source_type=source_type,
        tier=3,
        extractor_name="scrapling_static",
        extractor_version="v1",
        price_semantics="msrp",
        requires_location=False,
        enabled=True,
        notes=None,
    )


def _empty_batch_a_coverage() -> dict[str, object]:
    return {
        "batchCode": "country_msrp_batch_a",
        "countryCount": 0,
        "loadedCountryCount": 0,
        "historicalMonitoringCountryCount": 0,
        "historicalBackfillCountryCount": 0,
        "launchCandidateCountryCount": 0,
        "currentRows": 0,
        "backfillPeriodCount": 0,
        "launchCandidateCount": 0,
        "countries": [],
    }


def _patch_empty_monitoring_addons(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_build_launch_alerts",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_build_batch_a_coverage",
        lambda session: _empty_batch_a_coverage(),
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_attach_market_scan_sales",
        lambda events: None,
    )


@pytest.mark.parametrize(
    "backfill_kind",
    [
        "official_campaign_vs_regular_price",
        "official_campaign_savings_vs_current_price",
    ],
)
def test_country_audit_promotes_official_campaign_backfill_boundary(backfill_kind: str) -> None:
    payload = msrp_monitoring_service._audit_decision_for_country_event(
        {
            "sourceStatus": "confirmed",
            "changePct": -3.28,
            "riskReasons": [],
            "evidence": {
                "backfilled": True,
                "backfillKind": backfill_kind,
            },
        }
    )

    assert payload["auditPriority"] == "priority_audit"
    assert payload["samplingBucket"] == "campaign_promotion_boundary"
    assert "historical_price_backfill" in payload["auditReasons"]
    assert "campaign_promotion_boundary:not_permanent_msrp_cut" in payload["auditReasons"]


def test_country_audit_keeps_large_campaign_savings_as_campaign_boundary() -> None:
    payload = msrp_monitoring_service._audit_decision_for_country_event(
        {
            "sourceStatus": "confirmed",
            "changePct": -14.07,
            "riskReasons": [],
            "evidence": {
                "backfilled": True,
                "backfillKind": "official_campaign_savings_vs_current_price",
            },
        }
    )

    assert payload["auditPriority"] == "priority_audit"
    assert payload["samplingBucket"] == "campaign_promotion_boundary"
    assert "large_price_move:>=5pct" in payload["auditReasons"]
    assert "campaign_promotion_boundary:not_permanent_msrp_cut" in payload["auditReasons"]


def test_country_audit_keeps_official_price_list_backfill_as_sample() -> None:
    payload = msrp_monitoring_service._audit_decision_for_country_event(
        {
            "sourceStatus": "confirmed",
            "changePct": -0.46,
            "riskReasons": [],
            "evidence": {
                "backfilled": True,
                "backfillKind": "official_price_list_pdf",
            },
        }
    )

    assert payload["auditPriority"] == "sample"
    assert payload["samplingBucket"] == "historical_backfill"
    assert payload["auditReasons"] == ["historical_price_backfill"]


def test_summary_payload_counts_campaign_boundary_country_events() -> None:
    payload = msrp_monitoring_service._summary_payload(
        [
            {
                "auditPriority": "priority_audit",
                "timelineEventCount": 2,
                "sourceRiskCount": 0,
                "reviewRequiredCount": 0,
                "outlierCount": 0,
                "lengthMissing": False,
                "countries": [
                    {
                        "country": "瑞典",
                        "samplingBucket": "campaign_promotion_boundary",
                        "auditReasons": [
                            "campaign_promotion_boundary:not_permanent_msrp_cut",
                            "historical_price_backfill",
                        ],
                    },
                    {
                        "country": "挪威",
                        "samplingBucket": "historical_backfill",
                        "auditReasons": ["historical_price_backfill"],
                    },
                ],
            },
            {
                "auditPriority": "sample",
                "timelineEventCount": 1,
                "sourceRiskCount": 0,
                "reviewRequiredCount": 0,
                "outlierCount": 0,
                "lengthMissing": False,
                "countries": [
                    {
                        "country": "丹麦",
                        "samplingBucket": "campaign_promotion_boundary",
                        "auditReasons": [],
                    },
                ],
            },
        ]
    )

    assert payload["eventCount"] == 2
    assert payload["timelineEventCount"] == 3
    assert payload["affectedCountryCount"] == 3
    assert payload["campaignBoundaryCount"] == 2


def test_build_msrp_backfill_snapshot_preview_reads_text_artifact(tmp_path, monkeypatch) -> None:
    artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_backfill" / "sweden_2026"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "volvo_offer.md"
    artifact_path.write_text(
        "# Volvo offer\nOrdinary price 1148800 SEK\nOffer price 1099900 SEK\n" + ("evidence\n" * 200),
        encoding="utf-8",
    )
    monkeypatch.setattr(msrp_monitoring_service, "PROJECT_ROOT", tmp_path)

    payload = msrp_monitoring_service.build_msrp_backfill_snapshot_preview(
        "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volvo_offer.md",
        max_chars=1_000,
    )

    assert payload["exists"] is True
    assert payload["previewable"] is True
    assert payload["status"] == "ok"
    assert payload["fileName"] == "volvo_offer.md"
    assert payload["contentType"] == "text/markdown"
    assert payload["truncated"] is True
    assert str(payload["content"]).startswith("# Volvo offer")
    assert len(str(payload["content"])) == 1_000


def test_build_msrp_backfill_snapshot_preview_extracts_html_text(tmp_path, monkeypatch) -> None:
    artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_backfill" / "sweden_2026"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "skoda_offer.html"
    artifact_path.write_text(
        "<html><head><script>ignore()</script></head><body><h1>Skoda Enyaq</h1><p>Kampanjpris 599500 SEK</p></body></html>",
        encoding="utf-8",
    )
    monkeypatch.setattr(msrp_monitoring_service, "PROJECT_ROOT", tmp_path)

    payload = msrp_monitoring_service.build_msrp_backfill_snapshot_preview(
        "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/skoda_offer.html",
    )

    assert payload["exists"] is True
    assert payload["previewable"] is True
    assert payload["contentType"] == "text/html"
    assert "Skoda Enyaq" in str(payload["content"])
    assert "Kampanjpris 599500 SEK" in str(payload["content"])
    assert "ignore" not in str(payload["content"])


def test_build_msrp_backfill_snapshot_preview_extracts_pdf_text(tmp_path, monkeypatch) -> None:
    artifact_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts" / "msrp_backfill" / "sweden_2026"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "skoda_price_list.pdf"
    artifact_path.write_bytes(
        _minimal_pdf_bytes("Skoda Enyaq Solid Edition 599 500 kr ordinary price 619 800 kr")
    )
    monkeypatch.setattr(msrp_monitoring_service, "PROJECT_ROOT", tmp_path)

    payload = msrp_monitoring_service.build_msrp_backfill_snapshot_preview(
        "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/skoda_price_list.pdf",
    )

    assert payload["exists"] is True
    assert payload["previewable"] is True
    assert payload["status"] == "ok"
    assert payload["contentType"] == "application/pdf"
    assert "Skoda Enyaq Solid Edition" in str(payload["content"])
    assert "599 500 kr" in str(payload["content"])
    assert "619 800 kr" in str(payload["content"])


def test_build_msrp_backfill_snapshot_preview_blocks_paths_outside_backfill_root(tmp_path, monkeypatch) -> None:
    outside_dir = tmp_path / "03_Scripts" / "diagnostics" / "artifacts"
    outside_dir.mkdir(parents=True)
    (outside_dir / "not_backfill.md").write_text("secret", encoding="utf-8")
    monkeypatch.setattr(msrp_monitoring_service, "PROJECT_ROOT", tmp_path)

    payload = msrp_monitoring_service.build_msrp_backfill_snapshot_preview(
        "03_Scripts/diagnostics/artifacts/msrp_backfill/../not_backfill.md",
    )

    assert payload["exists"] is False
    assert payload["previewable"] is False
    assert payload["status"] == "blocked"
    assert payload["content"] is None


def _price_period(
    *,
    price_history_id: UUID,
    observation_id: UUID,
    valid_from: datetime,
    msrp_value: str,
    source_msrp_value: str,
    country: str = "se",
    brand: str = "Volvo",
    jato_model: str = "XC60",
    jato_trim: str = "Ultra",
    jato_powertrain: str = "PHEV",
    source_currency: str = "SEK",
) -> PriceHistory:
    return PriceHistory(
        price_history_id=price_history_id,
        country=country,
        brand=brand,
        jato_model=jato_model,
        jato_trim=jato_trim,
        jato_powertrain=jato_powertrain,
        msrp_value=Decimal(msrp_value),
        currency="EUR",
        source_msrp_value=Decimal(source_msrp_value),
        source_currency=source_currency,
        valid_from_utc=valid_from,
        valid_to_utc=None,
        last_confirmed_at_utc=valid_from,
        started_by_observation_id=observation_id,
        ended_by_observation_id=None,
        last_confirmed_by_observation_id=observation_id,
        created_at_utc=valid_from,
    )


def test_build_msrp_monitoring_events_returns_warning_when_history_missing(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: False,
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        country="se",
        brand="Volvo",
        jato_model="XC60",
        window_days=400,
        threshold_pct=2.5,
        limit=999,
    )

    assert payload["schemaVersion"] == "msrp_monitoring_events_v1"
    assert payload["warnings"] == ["price_history_unavailable"]
    assert payload["filters"] == {
        "country": "se",
        "brand": "Volvo",
        "jatoModel": "XC60",
        "windowDays": 365,
        "fromDate": None,
        "thresholdPct": 2.5,
        "limit": 500,
        "direction": "drops",
    }
    assert payload["summary"]["eventCount"] == 0
    assert payload["summary"]["auditPriorityCounts"] == {
        "auto_pass": 0,
        "sample": 0,
        "priority_audit": 0,
        "block": 0,
    }
    assert payload["summary"]["offerSignalCount"] == 0
    assert payload["events"] == []
    assert payload["launchAlerts"] == []
    assert payload["offerSignals"] == []
    assert payload["coverage"] == {"batchA": None}


def test_build_msrp_monitoring_events_returns_official_offer_signals_without_history(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 24, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: False,
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        country="SE",
        brand="Hyundai",
        window_days=180,
        from_date=date(2026, 1, 1),
    )

    assert payload["events"] == []
    assert payload["warnings"] == ["price_history_unavailable"]
    assert payload["summary"]["offerSignalCount"] == 3
    assert [signal["jatoModel"] for signal in payload["offerSignals"]] == [
        "KONA",
        "TUCSON",
        "INSTER",
    ]
    assert payload["offerSignals"][0]["matchStatus"] == "pending_current_price_match"
    assert payload["offerSignals"][0]["cashDiscountSek"] == 96980
    assert payload["offerSignals"][0]["capturedAtUtc"] == now.isoformat()


def test_build_offer_signals_filters_country_brand_and_model() -> None:
    now = datetime(2026, 6, 24, 8, 0, tzinfo=timezone.utc)

    payload = msrp_monitoring_service._build_offer_signals(
        now,
        country="瑞典",
        brand="BYD",
        jato_model="seal u",
    )

    assert [signal["signalId"] for signal in payload] == [
        "se-2026-byd-seal-u-dmi-official-private-lease"
    ]
    assert "modelAliases" not in payload[0]
    assert payload[0]["monthlyPaymentSek"] == 3995


def test_build_msrp_monitoring_events_filters_by_direction(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    source_id = uuid4()
    observation_id = uuid4()
    current_price_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now - timedelta(days=1),
        msrp_value="70000.00",
        source_msrp_value="805000.00",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    current_price = _current_price(
        current_price_id=current_price_id,
        observation=observation,
        updated_at=now - timedelta(days=1),
    )
    source = _source(source_id, source_type="manufacturer_official")
    current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=1),
        msrp_value="70000.00",
        source_msrp_value="805000.00",
    )
    previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=10),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
    )
    alert_query_kwargs: list[dict[str, object]] = []

    def fake_list_current_price_alerts(*args, **kwargs):
        alert_query_kwargs.append(dict(kwargs))
        return [current_price]

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        fake_list_current_price_alerts,
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda current_prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for item_id in ids
            if UUID(str(item_id)) == observation.observation_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for item_id in ids
            if UUID(str(item_id)) == source.source_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [current_period, previous_period],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda *args, **kwargs: SimpleNamespace(batch_code="msrp-live-20260623-se"),
    )

    drops_payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=0.5,
    )
    increases_payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=0.5,
        direction="increases",
    )

    assert drops_payload["filters"]["direction"] == "drops"
    assert drops_payload["summary"]["eventCount"] == 0
    assert increases_payload["filters"]["direction"] == "increases"
    assert increases_payload["summary"]["eventCount"] == 1
    assert increases_payload["events"][0]["countries"][0]["changePct"] == 7.69
    assert alert_query_kwargs == [
        {"direction": "drops", "changed_since": now - timedelta(days=30), "threshold_pct": 0.5},
        {"direction": "increases", "changed_since": now - timedelta(days=30), "threshold_pct": 0.5},
    ]


def test_timeline_payload_prefers_source_msrp_change_over_eur_fx_noise() -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    source_id = uuid4()
    observation_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now,
        msrp_value="100.00",
        source_msrp_value="900.00",
        source_currency="SEK",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    current_price = _current_price(
        current_price_id=uuid4(),
        observation=observation,
        updated_at=now,
    )
    current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now,
        msrp_value="100.00",
        source_msrp_value="900.00",
        source_currency="SEK",
    )
    previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=7),
        msrp_value="90.00",
        source_msrp_value="1000.00",
        source_currency="SEK",
    )

    drop_payload = msrp_monitoring_service._timeline_payload(
        item=current_price,
        current_period=current_period,
        previous_period=previous_period,
        source=_source(source_id, source_type="manufacturer_official"),
        observation=observation,
        threshold_pct=5.0,
        direction="drops",
    )
    increase_payload = msrp_monitoring_service._timeline_payload(
        item=current_price,
        current_period=current_period,
        previous_period=previous_period,
        source=_source(source_id, source_type="manufacturer_official"),
        observation=observation,
        threshold_pct=5.0,
        direction="increases",
    )

    assert drop_payload is not None
    assert drop_payload["changePct"] == -10.0
    assert drop_payload["changePctBasis"] == "source_msrp"
    assert drop_payload["changeAmountEur"] == 10.0
    assert drop_payload["changeAmountSource"] == -100.0
    assert increase_payload is None


def test_build_msrp_monitoring_events_honors_from_date_window(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    source_id = uuid4()
    observation_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now,
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    current_price = _current_price(
        current_price_id=uuid4(),
        observation=observation,
        updated_at=now,
    )
    current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=datetime(2025, 12, 31, 12, 0, tzinfo=timezone.utc),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
    )
    previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=uuid4(),
        valid_from=datetime(2025, 12, 1, 12, 0, tzinfo=timezone.utc),
        msrp_value="68000.00",
        source_msrp_value="782000.00",
    )

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda current_prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for item_id in ids
            if UUID(str(item_id)) == observation.observation_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [_source(source_id)],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [current_period, previous_period],
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        from_date=date(2026, 1, 1),
        window_days=365,
        threshold_pct=0.0,
    )

    assert payload["filters"]["fromDate"] == "2026-01-01"
    assert payload["summary"]["eventCount"] == 0
    assert payload["events"] == []


def test_build_msrp_monitoring_events_loads_deep_history_for_from_date(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    source_id = uuid4()
    observation_id = uuid4()
    previous_observation_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now,
        msrp_value="65000.00",
        source_msrp_value="747500.00",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    current_price = _current_price(
        current_price_id=uuid4(),
        observation=observation,
        updated_at=now,
    )
    history = [
        _price_period(
            price_history_id=uuid4(),
            observation_id=observation_id,
            valid_from=now - timedelta(days=index * 5),
            msrp_value="65000.00",
            source_msrp_value="747500.00",
        )
        for index in range(25)
    ]
    history.append(
        _price_period(
            price_history_id=uuid4(),
            observation_id=previous_observation_id,
            valid_from=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
            msrp_value="70000.00",
            source_msrp_value="805000.00",
        )
    )
    history_limits: list[int] = []

    def fake_list_price_history(
        session,
        country,
        brand,
        jato_model,
        jato_trim,
        jato_powertrain,
        limit,
    ):
        history_limits.append(limit)
        return history[:limit]

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda current_prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for item_id in ids
            if UUID(str(item_id)) == observation.observation_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            _source(source_id, source_type="manufacturer_official")
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        fake_list_price_history,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda *args, **kwargs: SimpleNamespace(batch_code="msrp-live-20260623-se"),
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        from_date=date(2026, 1, 1),
        window_days=30,
        threshold_pct=0.0,
    )

    assert history_limits == [
        msrp_monitoring_service._monitoring_price_history_limit(
            now,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    ]
    assert history_limits[0] > 20
    assert payload["summary"]["eventCount"] == 1
    assert payload["summary"]["timelineEventCount"] == 1
    assert payload["events"][0]["countries"][0]["changePct"] == -7.14


def test_build_launch_alerts_marks_single_open_period_as_new_baseline(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    scrape_batch_id = uuid4()
    source_id = uuid4()
    observation_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=scrape_batch_id,
        observed_at=now - timedelta(days=1),
        country="瑞典",
        brand="VOLVO",
        jato_model="EX90",
        jato_trim="Single Motor",
        jato_powertrain="BEV",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    current_price = _current_price(
        current_price_id=uuid4(),
        observation=observation,
        updated_at=now - timedelta(days=1),
    )
    source = _source(
        source_id,
        country="瑞典",
        brand="VOLVO",
        source_code="volvo_ex90_se",
        source_type="manufacturer_official",
    )
    launch_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=1),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
        country="瑞典",
        brand="VOLVO",
        jato_model="EX90",
        jato_trim="Single Motor",
        jato_powertrain="BEV",
    )

    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_prices",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [launch_period],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [observation],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [source],
    )

    alerts = msrp_monitoring_service._build_launch_alerts(
        "session",
        country="瑞典",
        brand=None,
        jato_model=None,
        since=now - timedelta(days=30),
        limit=500,
    )

    assert len(alerts) == 1
    alert = alerts[0]
    assert alert["countryLabel"] == "Sweden"
    assert alert["eventType"] == "new_product_launch_price_baseline"
    assert alert["jatoModel"] == "EX90"
    assert alert["jatoTrim"] == "Single Motor"
    assert alert["auditPriority"] == "sample"
    assert alert["currentSourceMsrp"] == 747500.0
    assert alert["sourceStatus"] == "confirmed"


def test_effect_markers_for_event_marks_price_drop_month() -> None:
    event = {
        "timeline": [
            {
                "changedAtUtc": "2026-06-15T10:00:00+00:00",
                "country": "瑞典",
                "countryLabel": "Sweden",
                "jatoTrim": "Solid Edition",
                "changePct": -3.28,
                "changeAmountEur": -1765.0,
            }
        ]
    }

    markers = msrp_monitoring_service._effect_markers_for_event(event)

    assert markers == [
        {
            "period": "2026-06",
            "changedAtUtc": "2026-06-15T10:00:00+00:00",
            "country": "瑞典",
            "countryLabel": "Sweden",
            "jatoTrim": "Solid Edition",
            "changePct": -3.28,
            "changeAmountEur": -1765.0,
            "eventType": "price_drop",
        }
    ]


def test_filter_monitoring_current_prices_removes_codex_smoke_rows() -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    source_id = uuid4()
    real_observation = _observation(
        observation_id=uuid4(),
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now,
        brand="SKODA",
        jato_model="ENYAQ",
        jato_trim="Solid Edition",
        jato_powertrain="BEV",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    smoke_observation = _observation(
        observation_id=uuid4(),
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now,
        brand="CODEX",
        jato_model="SMOKE MODEL smoke_test",
        jato_trim="Smoke",
        jato_powertrain="BEV",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    real_price = _current_price(
        current_price_id=uuid4(),
        observation=real_observation,
        updated_at=now,
    )
    smoke_price = _current_price(
        current_price_id=uuid4(),
        observation=smoke_observation,
        updated_at=now,
    )

    filtered = msrp_monitoring_service._filter_monitoring_current_prices(
        [smoke_price, real_price]
    )

    assert filtered == [real_price]


def test_build_batch_a_coverage_counts_backfill_and_launch_candidates(
    monkeypatch,
) -> None:
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(
        msrp_monitoring_service,
        "BATCH_A_COUNTRIES",
        (
            {"code": "SE", "countryLabel": "Sweden"},
            {"code": "AT", "countryLabel": "Austria"},
        ),
    )
    source_id = uuid4()
    se_current_observation_id = uuid4()
    se_previous_observation_id = uuid4()
    at_observation_id = uuid4()
    se_observation = _observation(
        observation_id=se_current_observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now - timedelta(days=1),
        country="瑞典",
        brand="SKODA",
        jato_model="ENYAQ",
        jato_trim="Solid Edition",
        jato_powertrain="BEV",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    at_observation = _observation(
        observation_id=at_observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now - timedelta(days=1),
        country="奥地利",
        brand="SEAT",
        jato_model="ATECA",
        jato_trim="Style",
        jato_powertrain="ICE",
        match_confidence="0.9600",
        match_status="auto_accepted",
    )
    se_current_price = _current_price(
        current_price_id=uuid4(),
        observation=se_observation,
        updated_at=now - timedelta(days=1),
    )
    at_current_price = _current_price(
        current_price_id=uuid4(),
        observation=at_observation,
        updated_at=now - timedelta(days=1),
    )
    se_current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=se_current_observation_id,
        valid_from=now - timedelta(days=1),
        msrp_value="52130.00",
        source_msrp_value="599500.00",
        country="瑞典",
        brand="SKODA",
        jato_model="ENYAQ",
        jato_trim="Solid Edition",
        jato_powertrain="BEV",
    )
    se_previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=se_previous_observation_id,
        valid_from=now - timedelta(days=2),
        msrp_value="53895.00",
        source_msrp_value="619800.00",
        country="瑞典",
        brand="SKODA",
        jato_model="ENYAQ",
        jato_trim="Solid Edition",
        jato_powertrain="BEV",
    )
    se_previous_period.valid_to_utc = now - timedelta(days=1)
    previous_observation = _observation(
        observation_id=se_previous_observation_id,
        source_id=source_id,
        scrape_batch_id=uuid4(),
        observed_at=now - timedelta(days=2),
        country="瑞典",
        brand="SKODA",
        jato_model="ENYAQ",
        jato_trim="Solid Edition",
        jato_powertrain="BEV",
        match_confidence="0.9600",
        match_status="auto_accepted",
        source_context_json={
            "historicalPriceBackfill": {
                "kind": "official_campaign_vs_regular_price",
                "sourceLabel": "Skoda campaign",
            },
        },
    )
    at_period = _price_period(
        price_history_id=uuid4(),
        observation_id=at_observation_id,
        valid_from=now - timedelta(days=1),
        msrp_value="40000.00",
        source_msrp_value="45990.00",
        country="奥地利",
        brand="SEAT",
        jato_model="ATECA",
        jato_trim="Style",
        jato_powertrain="ICE",
        source_currency="EUR",
    )

    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_prices",
        lambda session, country, *args: [se_current_price] if country == "SE" else [at_current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda session, country, *args: (
            [se_current_period, se_previous_period]
            if country == "瑞典"
            else [at_period]
        ),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            item
            for item in [se_observation, previous_observation, at_observation]
            if item.observation_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )

    coverage = msrp_monitoring_service._build_batch_a_coverage("session")

    assert coverage["loadedCountryCount"] == 2
    assert coverage["historicalBackfillCountryCount"] == 1
    assert coverage["launchCandidateCountryCount"] == 1
    countries = {str(item["code"]): item for item in coverage["countries"]}
    assert countries["SE"]["status"] == "backfilled"
    assert countries["SE"]["backfillPeriodCount"] == 1
    assert countries["AT"]["status"] == "current_only"
    assert countries["AT"]["launchCandidateCount"] == 1


def test_build_msrp_monitoring_events_groups_price_changes_with_evidence(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    scrape_batch_id = uuid4()
    source_id = uuid4()
    observation_id = uuid4()
    current_price_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=scrape_batch_id,
        observed_at=now - timedelta(days=2),
    )
    current_price = _current_price(
        current_price_id=current_price_id,
        observation=observation,
        updated_at=now - timedelta(days=2),
    )
    source = _source(source_id)
    current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=2),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
    )
    previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=uuid4(),
        valid_from=now - timedelta(days=30),
        msrp_value="68000.00",
        source_msrp_value="782000.00",
    )

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda current_prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for item_id in ids
            if UUID(str(item_id)) == observation.observation_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for item_id in ids
            if UUID(str(item_id)) == source.source_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [current_period, previous_period],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda *args, **kwargs: SimpleNamespace(
            batch_code="msrp-dryrun-20260620-010203",
        ),
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=1.0,
    )

    assert payload["warnings"] == []
    assert payload["summary"] == {
        "eventCount": 1,
        "timelineEventCount": 1,
        "affectedCountryCount": 1,
        "sourceRiskCount": 1,
        "reviewRequiredCount": 1,
        "outlierCount": 0,
        "lengthMissingCount": 0,
        "campaignBoundaryCount": 0,
        "auditPriorityCounts": {
            "auto_pass": 0,
            "sample": 0,
            "priority_audit": 1,
            "block": 0,
        },
        "autoPassCount": 0,
        "sampleCount": 0,
        "priorityAuditCount": 1,
        "blockCount": 0,
        "launchAlertCount": 0,
        "offerSignalCount": 10,
        "batchALoadedCountryCount": 0,
        "batchAHistoricalBackfillCountryCount": 0,
    }
    event = payload["events"][0]
    assert event["eventId"] == "Volvo|XC60|PHEV"
    assert event["auditPriority"] == "priority_audit"
    assert event["suggestedAction"] == "priority_audit"
    assert event["samplingBucket"] == "source_risk"
    assert "source_status:review_required" in event["auditReasons"]
    assert event["lengthMm"] == 4708
    assert event["lengthSource"] == "observation_context"
    assert event["confidence"] == "low"
    assert event["medianChangePct"] == -4.41
    assert event["medianOldMsrpEur"] == 68000.0
    assert event["medianCurrentMsrpEur"] == 65000.0
    assert event["riskReasons"] == {
        "match_status:review_required": 1,
        "low_match_confidence": 1,
        "non_official_source:third_party_reference": 1,
    }

    country = event["countries"][0]
    assert country["country"] == "se"
    assert country["countryLabel"] == "Sweden"
    assert country["sourceStatus"] == "review_required"
    assert country["reviewFlag"] is True
    assert country["auditPriority"] == "priority_audit"
    assert country["suggestedAction"] == "priority_audit"
    assert country["samplingBucket"] == "source_risk"
    assert country["outlier"] is False
    assert country["suspectedFalsePositive"] is True
    assert country["changeAmountEur"] == -3000.0
    assert country["changePct"] == -4.41
    assert country["source"]["sourceCode"] == "volvo_xc60_se_draft_scrapling"
    assert country["source"]["sourceType"] == "third_party_reference"
    assert country["evidence"]["sourceSnapshotPath"] == "snapshots/volvo-xc60.html"
    assert country["evidence"]["sourcePayloadHash"] == "hash-monitoring"
    assert country["evidence"]["dryrunRunId"] == "msrp-dryrun-20260620-010203"
    assert country["evidence"]["scrapeBatchCode"] == "msrp-dryrun-20260620-010203"


def test_build_msrp_monitoring_events_marks_historical_backfill_evidence(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    scrape_batch_id = uuid4()
    backfill_batch_id = uuid4()
    source_id = uuid4()
    observation_id = uuid4()
    previous_observation_id = uuid4()
    current_price_id = uuid4()
    observation = _observation(
        observation_id=observation_id,
        source_id=source_id,
        scrape_batch_id=scrape_batch_id,
        observed_at=now - timedelta(days=1),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
        match_confidence="0.9600",
        match_status="auto_accepted",
        dryrun_run_id=None,
        source_payload_hash="hash-current",
    )
    previous_observation = _observation(
        observation_id=previous_observation_id,
        source_id=source_id,
        scrape_batch_id=backfill_batch_id,
        observed_at=datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc),
        msrp_value="65300.00",
        source_msrp_value="750950.00",
        match_confidence="0.9600",
        match_status="auto_accepted",
        dryrun_run_id=None,
        source_payload_hash="hash-backfill",
        source_context_json={
            "historicalPriceBackfill": {
                "kind": "official_price_list_pdf",
                "sourceLabel": "Volvo Sweden official price list 2026-01",
                "effectiveDate": "2026-01-01",
                "evidenceUrl": "https://example.test/volvo-se-price-list-2026-01.pdf",
                "snapshotPath": "snapshots/backfill/volvo-se-price-list-2026-01.pdf",
            },
            "relatedOfficialEvidence": {
                "url": "https://example.test/volvo-se-configurator",
                "label": "Volvo Sweden configurator current price",
                "snapshotPath": "snapshots/backfill/volvo-se-configurator.html",
                "payloadHash": "sha256:related-configurator",
            },
            "officialEvidence": {
                "pdfUrl": "https://example.test/volvo-se-supplemental-price-list.pdf",
                "pdfSnapshotPath": "snapshots/backfill/volvo-se-supplemental-price-list.pdf",
                "pdfPayloadHash": "sha256:official-pdf",
            },
            "vehicle": {"length_mm": 4708},
        },
    )
    current_price = _current_price(
        current_price_id=current_price_id,
        observation=observation,
        updated_at=now - timedelta(days=1),
    )
    source = _source(source_id, source_type="official_price_list_pdf")
    current_period = _price_period(
        price_history_id=uuid4(),
        observation_id=observation_id,
        valid_from=now - timedelta(days=1),
        msrp_value="65000.00",
        source_msrp_value="747500.00",
    )
    previous_period = _price_period(
        price_history_id=uuid4(),
        observation_id=previous_observation_id,
        valid_from=datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc),
        msrp_value="65300.00",
        source_msrp_value="750950.00",
    )
    observations = [observation, previous_observation]

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: [current_price],
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda current_prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            item
            for item in observations
            if item.observation_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for item_id in ids
            if UUID(str(item_id)) == source.source_id
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda *args, **kwargs: [current_period, previous_period],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda session, batch_id: SimpleNamespace(
            batch_code=(
                "msrp-backfill-2026-se"
                if batch_id == backfill_batch_id
                else "msrp-live-20260623-se"
            ),
        ),
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=365,
        threshold_pct=0.0,
    )

    assert payload["summary"]["eventCount"] == 1
    event = payload["events"][0]
    assert event["auditPriority"] == "sample"
    assert event["samplingBucket"] == "historical_backfill"
    assert event["backfilled"] is True
    assert event["backfillEventCount"] == 1
    assert "historical_price_backfill" in event["auditReasons"]

    country = event["countries"][0]
    assert country["changePct"] == -0.46
    assert country["auditPriority"] == "sample"
    assert country["samplingBucket"] == "historical_backfill"
    assert country["evidence"]["backfilled"] is True
    assert country["evidence"]["backfillKind"] == "official_price_list_pdf"
    assert country["evidence"]["backfillEvidenceRole"] == "previous"
    assert country["evidence"]["backfillObservationId"] == str(previous_observation_id)
    assert country["evidence"]["backfillSourceLabel"] == "Volvo Sweden official price list 2026-01"
    assert country["evidence"]["backfillEffectiveDate"] == "2026-01-01"
    assert country["evidence"]["backfillEvidenceUrl"] == "https://example.test/volvo-se-price-list-2026-01.pdf"
    assert country["evidence"]["backfillSnapshotPath"] == "snapshots/backfill/volvo-se-price-list-2026-01.pdf"
    assert country["evidence"]["backfillPayloadHash"] == "hash-backfill"
    assert country["evidence"]["relatedOfficialEvidence"] == [
        {
            "url": "https://example.test/volvo-se-configurator",
            "label": "Volvo Sweden configurator current price",
            "snapshotPath": "snapshots/backfill/volvo-se-configurator.html",
            "payloadHash": "sha256:related-configurator",
        },
        {
            "url": "https://example.test/volvo-se-supplemental-price-list.pdf",
            "label": "Official PDF price list",
            "snapshotPath": "snapshots/backfill/volvo-se-supplemental-price-list.pdf",
            "payloadHash": "sha256:official-pdf",
        }
    ]
    assert country["evidence"]["sourcePayloadHash"] == "hash-current"


def test_build_msrp_monitoring_events_groups_multi_country_sync_and_outlier(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    batch_by_id: dict[UUID, SimpleNamespace] = {}
    observations: list[MsrpObservation] = []
    current_prices: list[CurrentPrice] = []
    sources: list[MsrpSource] = []
    history_by_country: dict[str, list[PriceHistory]] = {}

    rows = [
        ("de", "Tesla", "Model Y", "Long Range", "BEV", "48000.00", "50000.00", "confirmed", "0.9600", "manufacturer_official"),
        ("nl", "Tesla", "Model Y", "Long Range", "BEV", "49400.00", "52000.00", "confirmed", "0.9500", "manufacturer_official"),
        ("dk", "Tesla", "Model Y", "Long Range", "BEV", "49000.00", "70000.00", "review_required", "0.7100", "third_party_reference"),
    ]
    for country, brand, model, trim, powertrain, current_value, previous_value, match_status, confidence, source_type in rows:
        observation_id = uuid4()
        source_id = uuid4()
        batch_id = uuid4()
        batch_by_id[batch_id] = SimpleNamespace(
            batch_code=f"msrp-dryrun-20260620-{country}",
        )
        observation = _observation(
            observation_id=observation_id,
            source_id=source_id,
            scrape_batch_id=batch_id,
            observed_at=now - timedelta(days=1),
            country=country,
            brand=brand,
            jato_model=model,
            jato_trim=trim,
            jato_powertrain=powertrain,
            msrp_value=current_value,
            source_msrp_value=current_value,
            source_currency="EUR",
            match_confidence=confidence,
            match_status=match_status,
            length_mm=4790,
            dryrun_run_id=f"msrp-dryrun-20260620-{country}",
        )
        observations.append(observation)
        current_prices.append(
            _current_price(
                current_price_id=uuid4(),
                observation=observation,
                updated_at=now - timedelta(days=1),
            )
        )
        sources.append(
            _source(
                source_id,
                country=country,
                brand=brand,
                source_code=f"tesla_model_y_{country}",
                source_type=source_type,
            )
        )
        history_by_country[country] = [
            _price_period(
                price_history_id=uuid4(),
                observation_id=observation_id,
                valid_from=now - timedelta(days=1),
                msrp_value=current_value,
                source_msrp_value=current_value,
                country=country,
                brand=brand,
                jato_model=model,
                jato_trim=trim,
                jato_powertrain=powertrain,
                source_currency="EUR",
            ),
            _price_period(
                price_history_id=uuid4(),
                observation_id=uuid4(),
                valid_from=now - timedelta(days=12),
                msrp_value=previous_value,
                source_msrp_value=previous_value,
                country=country,
                brand=brand,
                jato_model=model,
                jato_trim=trim,
                jato_powertrain=powertrain,
                source_currency="EUR",
            ),
        ]

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "has_price_history_table",
        lambda session: True,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_price_alerts",
        lambda *args, **kwargs: current_prices,
    )
    monkeypatch.setattr(
        msrp_monitoring_service,
        "_load_market_scan_length_lookup",
        lambda prices: ({}, {}, None),
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for observation in observations
            if observation.observation_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for source in sources
            if source.source_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_price_history",
        lambda session, country, *args: history_by_country[country],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "get_scrape_batch",
        lambda session, batch_id: batch_by_id[batch_id],
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=1.0,
    )

    assert payload["summary"]["eventCount"] == 1
    assert payload["summary"]["timelineEventCount"] == 3
    assert payload["summary"]["affectedCountryCount"] == 3
    assert payload["summary"]["sourceRiskCount"] == 1
    assert payload["summary"]["outlierCount"] == 1
    assert payload["summary"]["priorityAuditCount"] == 1

    event = payload["events"][0]
    assert event["eventId"] == "Tesla|Model Y|BEV"
    assert event["auditPriority"] == "priority_audit"
    assert event["samplingBucket"] in {"outlier", "source_risk"}
    assert event["powertrainColor"] == "#16a34a"
    assert event["lengthMm"] == 4790
    assert event["affectedCountryCount"] == 3
    assert event["multiCountrySync"] is True
    assert event["medianChangePct"] == -5.0
    assert event["changePctBasis"] == "source_msrp"
    assert event["medianOldMsrpEur"] == 52000.0
    assert event["medianCurrentMsrpEur"] == 49000.0
    assert event["sourceRiskCount"] == 1
    assert event["reviewRequiredCount"] == 1
    assert event["outlierCount"] == 1
    assert event["suspectedFalsePositiveCount"] == 1

    countries = {item["country"]: item for item in event["countries"]}
    assert set(countries) == {"de", "nl", "dk"}
    assert countries["de"]["changePct"] == -4.0
    assert countries["nl"]["changePct"] == -5.0
    assert countries["dk"]["changePct"] == -30.0
    assert countries["dk"]["outlier"] is True
    assert countries["dk"]["reviewFlag"] is True
    assert countries["dk"]["auditPriority"] == "priority_audit"
    assert "outlier_vs_model_country_cluster" in countries["dk"]["auditReasons"]
    assert countries["dk"]["suspectedFalsePositive"] is True
    assert countries["dk"]["evidence"]["dryrunRunId"] == "msrp-dryrun-20260620-dk"


def test_build_msrp_monitoring_events_sweden_demo_uses_current_prices(
    monkeypatch,
) -> None:
    _patch_empty_monitoring_addons(monkeypatch)
    now = datetime(2026, 6, 23, 8, 0, tzinfo=timezone.utc)
    observations: list[MsrpObservation] = []
    current_prices: list[CurrentPrice] = []
    sources: list[MsrpSource] = []
    rows = [
        ("瑞典", "VOLVO", "EX90", "Ultra", "BEV", "94782.61", "1090000.00", "official_price_list"),
        ("瑞典", "VOLKSWAGEN", "TAYRON", "Life", "", "38600.00", "443900.00", "official_configurator"),
    ]
    for country, brand, model, trim, powertrain, current_value, source_value, source_type in rows:
        observation_id = uuid4()
        source_id = uuid4()
        observation = _observation(
            observation_id=observation_id,
            source_id=source_id,
            scrape_batch_id=uuid4(),
            observed_at=now - timedelta(days=1),
            country=country,
            brand=brand,
            jato_model=model,
            jato_trim=trim,
            jato_powertrain=powertrain,
            msrp_value=current_value,
            source_msrp_value=source_value,
            source_currency="SEK",
            match_confidence="0.9600",
            match_status="auto_accepted",
            dryrun_run_id=None,
        )
        observations.append(observation)
        current_prices.append(
            _current_price(
                current_price_id=uuid4(),
                observation=observation,
                updated_at=now - timedelta(days=1),
            )
        )
        sources.append(
            _source(
                source_id,
                country=country,
                brand=brand,
                source_code=f"{brand.lower()}_{model.lower()}_se",
                source_type=source_type,
            )
        )

    monkeypatch.setattr(msrp_monitoring_service, "_utc_now", lambda: now)
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_current_prices",
        lambda session, country, brand, jato_model, limit, offset: current_prices,
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_observations_by_ids",
        lambda session, ids: [
            observation
            for observation in observations
            if observation.observation_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )
    monkeypatch.setattr(
        msrp_monitoring_service.msrp_repo,
        "list_sources_by_ids",
        lambda session, ids: [
            source
            for source in sources
            if source.source_id in {UUID(str(item_id)) for item_id in ids}
        ],
    )

    payload = msrp_monitoring_service.build_msrp_monitoring_events(
        "session",
        window_days=30,
        threshold_pct=0.0,
        mode="sweden_demo",
    )

    assert payload["mode"] == "sweden_demo"
    assert payload["demo"]["enabled"] is True
    assert payload["filters"]["country"] == "瑞典"
    assert payload["summary"]["eventCount"] == 2
    assert payload["summary"]["timelineEventCount"] == 2
    assert payload["summary"]["sourceRiskCount"] == 1
    assert payload["summary"]["priorityAuditCount"] == 1
    assert payload["summary"]["blockCount"] == 1
    assert payload["warnings"] == ["sweden_demo_backfilled_not_written_to_price_history"]

    events = {event["jatoModel"]: event for event in payload["events"]}
    assert events["EX90"]["powertrainColor"] == "#16a34a"
    assert events["EX90"]["lengthMm"] == 5037
    assert events["EX90"]["demo"] is True
    assert events["EX90"]["auditPriority"] == "priority_audit"
    assert "large_price_move:>=5pct" in events["EX90"]["auditReasons"]
    assert events["EX90"]["countries"][0]["evidence"]["demoBackfilled"] is True
    assert events["TAYRON"]["lifecycleStatus"] == "removed_from_configurator"
    assert events["TAYRON"]["auditPriority"] == "block"
    assert "lifecycle_signal:removed_from_configurator" in events["TAYRON"]["auditReasons"]
    assert events["TAYRON"]["reviewRequiredCount"] == 1
    assert events["TAYRON"]["countries"][0]["riskReasons"] == [
        "demo_unavailable_signal",
        "demo_backfilled_price",
    ]
