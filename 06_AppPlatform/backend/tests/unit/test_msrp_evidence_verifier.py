from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.services import msrp_evidence_integrity_service as integrity_service
from app.services import msrp_evidence_verifier as verifier


OBSERVED_AT = datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc)


def _case(
    monkeypatch,
    tmp_path: Path,
    *,
    evidence_type: str = "uploaded_pdf",
    source_type: str = "manufacturer_official",
    evidence_source_type: str = "official_price_list_pdf",
    write_object: bool = True,
):
    target_id = uuid4()
    source_id = uuid4()
    source_version_id = uuid4()
    observation_id = uuid4()
    evidence_asset_id = uuid4()
    content = f"official-{evidence_type}".encode()
    sha256 = hashlib.sha256(content).hexdigest()
    extension = {
        "uploaded_pdf": "pdf",
        "downloaded_pdf": "pdf",
        "html_snapshot": "html",
        "api_snapshot": "json",
        "screenshot": "png",
    }.get(evidence_type, "ref")
    storage_key = f"assets/{sha256[:2]}/{sha256}.{extension}"
    asset = SimpleNamespace(
        evidence_asset_id=evidence_asset_id,
        target_id=target_id,
        source_id=source_id,
        evidence_type=evidence_type,
        storage_key=storage_key,
        size_bytes=len(content),
        sha256=sha256,
        captured_at_utc=OBSERVED_AT,
        valid_from=None,
        valid_until=None,
        official_domain_verified=True,
        source_type=evidence_source_type,
        semantic_lane="base_msrp",
        lifecycle_state="active",
    )
    if write_object and evidence_type not in {"official_url"}:
        path = tmp_path / storage_key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    target = SimpleNamespace(
        target_id=target_id,
        country="SE",
        brand="Volvo",
        model="XC60",
        active_source_version_id=source_version_id,
    )
    source = SimpleNamespace(
        source_id=source_id,
        country="SE",
        brand="Volvo",
        source_type=source_type,
        enabled=True,
        price_semantics="base_msrp",
        extractor_name="official-price-extractor",
        extractor_version="v1",
    )
    version = SimpleNamespace(
        source_version_id=source_version_id,
        source_id=source_id,
        target_id=target_id,
        version_status="published",
        evidence_refs_json=[
            {"evidenceAssetId": str(evidence_asset_id), "sha256": sha256}
        ],
        extractor_name=source.extractor_name,
        extractor_type="html",
        extractor_version=source.extractor_version,
        semantic_lane="base_msrp",
        currency="SEK",
        tax_mode="tax_included",
        valid_from=None,
        valid_until=None,
        validation_summary_json={"status": "passed"},
        dryrun_summary_json={"status": "passed"},
        conflict_summary_json=None,
    )
    observation = SimpleNamespace(
        observation_id=observation_id,
        source_id=source_id,
        source_version_id=source_version_id,
        country="se",
        brand="VOLVO",
        jato_model="XC60",
        official_model="XC60",
        source_currency="SEK",
        tax_included=True,
        extraction_version="v1",
        observed_at_utc=OBSERVED_AT,
    )
    link = SimpleNamespace(
        observation_id=observation_id,
        evidence_asset_id=evidence_asset_id,
        source_version_id=source_version_id,
        evidence_role="price_page",
        evidence_sha256=sha256,
        evidence_asset=asset,
    )
    monkeypatch.setattr(
        verifier.governance_repo,
        "get_source_version",
        lambda *_args, **_kwargs: version,
    )
    monkeypatch.setattr(
        verifier.governance_repo,
        "get_target",
        lambda *_args, **_kwargs: target,
    )
    monkeypatch.setattr(
        verifier.governance_repo,
        "list_result_corrections_for_observations",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        verifier.msrp_repository,
        "get_source",
        lambda *_args, **_kwargs: source,
    )
    return SimpleNamespace(
        target_id=target_id,
        source_id=source_id,
        source_version_id=source_version_id,
        observation=observation,
        link=link,
        asset=asset,
        version=version,
        source=source,
        content=content,
        root=tmp_path,
    )


def _verify(case):
    return verifier.verify_observation_evidence(
        object(),
        case.observation,
        target_id=case.target_id,
        evidence_root=case.root,
        links=[case.link],
    )


@pytest.mark.parametrize(
    "evidence_type",
    ["uploaded_pdf", "downloaded_pdf", "html_snapshot", "api_snapshot"],
)
def test_verifier_accepts_replayable_pdf_html_and_api(
    monkeypatch,
    tmp_path: Path,
    evidence_type: str,
) -> None:
    case = _case(monkeypatch, tmp_path, evidence_type=evidence_type)

    result = _verify(case)

    assert result.passed is True
    assert result.source_gate.verified_official_evidence is True
    assert result.evidence_refs == (
        {
            "evidenceAssetId": str(case.asset.evidence_asset_id),
            "sha256": case.asset.sha256,
            "evidenceType": evidence_type,
            "evidenceRole": "price_page",
            "storageKey": case.asset.storage_key,
            "capturedAtUtc": OBSERVED_AT.isoformat(),
        },
    )


@pytest.mark.parametrize("source_type", ["official_pdf", "official_web"])
def test_verifier_accepts_existing_governance_official_aliases(
    monkeypatch,
    tmp_path: Path,
    source_type: str,
) -> None:
    case = _case(
        monkeypatch,
        tmp_path / source_type,
        source_type=source_type,
        evidence_source_type=source_type,
    )

    assert _verify(case).passed is True


def test_verifier_reports_missing_unreadable_non_regular_and_hash_mismatch(
    monkeypatch,
    tmp_path: Path,
) -> None:
    missing = _case(monkeypatch, tmp_path, write_object=False)
    assert "evidence_object_missing" in _verify(missing).reasons

    unreadable = _case(monkeypatch, tmp_path / "unreadable")
    unreadable_path = unreadable.root / unreadable.asset.storage_key
    original_access = integrity_service.os.access
    monkeypatch.setattr(
        integrity_service.os,
        "access",
        lambda path, mode: (
            False if Path(path) == unreadable_path else original_access(path, mode)
        ),
    )
    assert "evidence_object_unreadable" in _verify(unreadable).reasons

    not_regular = _case(
        monkeypatch,
        tmp_path / "not-regular",
        write_object=False,
    )
    (not_regular.root / not_regular.asset.storage_key).mkdir(parents=True)
    assert "evidence_object_not_regular" in _verify(not_regular).reasons

    mismatch = _case(monkeypatch, tmp_path / "mismatch")
    (mismatch.root / mismatch.asset.storage_key).write_bytes(
        b"x" * len(mismatch.content)
    )
    assert "evidence_sha256_mismatch" in _verify(mismatch).reasons


def test_verifier_detects_object_removed_or_tampered_after_valid_link(
    monkeypatch,
    tmp_path: Path,
) -> None:
    case = _case(monkeypatch, tmp_path)
    object_path = case.root / case.asset.storage_key

    assert _verify(case).passed is True

    object_path.unlink()
    assert "evidence_object_missing" in _verify(case).reasons

    object_path.write_bytes(b"x" * len(case.content))
    assert "evidence_sha256_mismatch" in _verify(case).reasons


def test_verifier_rejects_nonofficial_source_and_evidence_type(
    monkeypatch,
    tmp_path: Path,
) -> None:
    source_case = _case(
        monkeypatch,
        tmp_path / "source",
        source_type="reference_catalog",
    )
    source_result = _verify(source_case)
    assert "source_type_not_official" in source_result.reasons
    assert "source_policy_not_approved" in source_result.reasons

    evidence_case = _case(
        monkeypatch,
        tmp_path / "evidence",
        evidence_source_type="reference_catalog",
    )
    assert "evidence_source_type_not_official" in _verify(evidence_case).reasons


def test_verifier_rejects_source_registry_identity_mismatch(
    monkeypatch,
    tmp_path: Path,
) -> None:
    case = _case(monkeypatch, tmp_path)
    case.source.country = "FI"
    case.source.brand = "Polestar"

    result = _verify(case)

    assert "observation_source_registry_mismatch" in result.reasons
    assert "source_registry_target_mismatch" in result.reasons


@pytest.mark.parametrize(
    ("evidence_type", "reason"),
    [
        ("official_url", "official_url_only_not_replayable"),
        ("screenshot", "screenshot_only_not_replayable"),
    ],
)
def test_verifier_rejects_nonreplayable_only_evidence(
    monkeypatch,
    tmp_path: Path,
    evidence_type: str,
    reason: str,
) -> None:
    case = _case(monkeypatch, tmp_path, evidence_type=evidence_type)

    assert reason in _verify(case).reasons


def test_verifier_rejects_stale_invalid_and_mismatched_links(
    monkeypatch,
    tmp_path: Path,
) -> None:
    stale = _case(monkeypatch, tmp_path / "stale")
    stale.asset.captured_at_utc = OBSERVED_AT - timedelta(days=31)
    assert "evidence_capture_window_stale" in _verify(stale).reasons

    invalid = _case(monkeypatch, tmp_path / "invalid")
    invalid.asset.valid_from = date(2026, 7, 1)
    invalid.asset.valid_until = date(2026, 7, 14)
    assert "evidence_validity_window_invalid" in _verify(invalid).reasons

    wrong_version = _case(monkeypatch, tmp_path / "wrong-version")
    wrong_version.link.source_version_id = uuid4()
    assert "evidence_link_source_version_mismatch" in _verify(
        wrong_version
    ).reasons


def test_verifier_requires_source_version_and_observation_link(
    monkeypatch,
    tmp_path: Path,
) -> None:
    case = _case(monkeypatch, tmp_path)
    case.observation.source_version_id = None
    missing_version = verifier.verify_observation_evidence(
        object(),
        case.observation,
        target_id=case.target_id,
        evidence_root=case.root,
        links=[],
    )
    assert missing_version.reasons == ("source_version_missing",)

    case = _case(monkeypatch, tmp_path / "missing-link")
    missing_link = verifier.verify_observation_evidence(
        object(),
        case.observation,
        target_id=case.target_id,
        evidence_root=case.root,
        links=[],
    )
    assert "observation_evidence_link_missing" in missing_link.reasons
    assert "replayable_evidence_missing" in missing_link.reasons
