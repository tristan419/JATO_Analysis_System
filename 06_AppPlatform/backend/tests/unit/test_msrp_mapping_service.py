from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

from app.services import msrp_link_service, msrp_mapping_service


def test_apply_canonical_mapping_uses_override_first(monkeypatch) -> None:
    override = SimpleNamespace(
        override_id=uuid4(),
        official_model="XC60",
        official_trim="Ultra Dark",
        override_reason="Temporary MY26 rename",
    )
    monkeypatch.setattr(
        msrp_mapping_service.review_repository,
        "find_applicable_override",
        lambda *args, **kwargs: override,
    )
    monkeypatch.setattr(
        msrp_mapping_service.msrp_repository,
        "find_active_jato_msrp_link",
        lambda *args, **kwargs: None,
    )
    observation = SimpleNamespace(
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        observed_at_utc=datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
        match_status="review_required",
        match_confidence=Decimal("0.6100"),
        match_reason_json={},
    )

    resolution = msrp_mapping_service.apply_canonical_mapping(
        None,
        observation,
    )

    assert resolution["resolverKind"] == "match_override"
    assert observation.official_trim == "Ultra Dark"
    assert observation.match_status == "override_applied"
    assert observation.match_confidence == Decimal("1.0")
    assert (
        observation.match_reason_json["mappingResolver"]["mismatchCategory"]
        == "timing_mismatch"
    )


def test_apply_canonical_mapping_uses_active_link(monkeypatch) -> None:
    monkeypatch.setattr(
        msrp_mapping_service.review_repository,
        "find_applicable_override",
        lambda *args, **kwargs: None,
    )
    link = SimpleNamespace(
        link_id=uuid4(),
        official_model="XC60",
        official_trim="Ultra Dark",
        official_edition="Launch Edition",
        official_powertrain="Plug-in Hybrid",
        confidence=96,
        link_source="review_decision",
        notes="Approved naming mismatch",
    )
    monkeypatch.setattr(
        msrp_mapping_service.msrp_repository,
        "find_active_jato_msrp_link",
        lambda *args, **kwargs: link,
    )
    observation = SimpleNamespace(
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        observed_at_utc=datetime(2026, 4, 12, 10, 0, tzinfo=timezone.utc),
        match_status="review_required",
        match_confidence=Decimal("0.6100"),
        match_reason_json=None,
    )

    resolution = msrp_mapping_service.apply_canonical_mapping(
        None,
        observation,
    )

    assert resolution["resolverKind"] == "jato_link"
    assert observation.official_trim == "Ultra Dark"
    assert observation.official_edition == "Launch Edition"
    assert observation.official_powertrain == "Plug-in Hybrid"
    assert observation.match_status == "auto_accepted"
    assert observation.match_confidence == Decimal("0.96")
    assert (
        observation.match_reason_json["mappingResolver"]["linkSource"]
        == "review_decision"
    )
    assert (
        observation.match_reason_json["mappingResolver"]["mismatchCategory"]
        == "granularity_mismatch"
    )


def test_upsert_jato_msrp_link_retires_conflicting_links(monkeypatch) -> None:
    retired_link = SimpleNamespace(
        link_id=uuid4(),
        official_model="XC60",
        official_trim="Ultra",
        official_edition=None,
        official_powertrain="PHEV",
        is_active=True,
    )
    created_links = []

    monkeypatch.setattr(
        msrp_link_service.msrp_repository,
        "list_jato_msrp_links_for_key",
        lambda *args, **kwargs: [retired_link],
    )

    def _add_link(_session, link):
        created_links.append(link)
        return link

    monkeypatch.setattr(
        msrp_link_service.msrp_repository,
        "add_jato_msrp_link",
        _add_link,
    )

    link = msrp_link_service.upsert_jato_msrp_link(
        None,
        country="瑞典",
        brand="Volvo",
        jato_model="XC60",
        jato_trim="Ultra",
        jato_powertrain="PHEV",
        official_model="XC60",
        official_trim="Ultra Dark",
        official_edition="Launch Edition",
        official_powertrain="Plug-in Hybrid",
        confidence=97,
        link_source="review_decision",
        notes="[naming_mismatch] reviewer approved",
    )

    assert link in created_links
    assert retired_link.is_active is False
    assert link.is_active is True
    assert link.confidence == 97
