from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.models import CurrentPrice, MsrpObservation, PriceHistory, ScrapeBatch
from app.db.session import get_session_factory


@dataclass(frozen=True)
class SwedenBackfillRow:
    country: str
    brand: str
    jato_model: str
    jato_trim: str
    jato_powertrain: str
    previous_source_msrp_value: Decimal
    previous_source_currency: str
    current_source_msrp_value: Decimal
    evidence_url: str
    evidence_snapshot_path: str
    evidence_payload_hash: str
    source_label: str
    effective_date: str
    availability_text: str
    pdf_url: str
    pdf_snapshot_path: str
    pdf_payload_hash: str
    evidence_kind: str
    notes: str
    secondary_evidence_url: str = ""
    secondary_evidence_snapshot_path: str = ""
    secondary_evidence_payload_hash: str = ""
    secondary_evidence_label: str = ""


BACKFILL_ROWS = [
    SwedenBackfillRow(
        country="瑞典",
        brand="SKODA",
        jato_model="ENYAQ",
        jato_trim="Solid Edition",
        jato_powertrain="BEV",
        previous_source_msrp_value=Decimal("619800.00"),
        previous_source_currency="SEK",
        current_source_msrp_value=Decimal("599500.00"),
        evidence_url="https://www.skoda.se/erbjudande/kampanj/erbjudande-enyaq",
        evidence_snapshot_path=(
            "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/"
            "skoda_enyaq_solid_edition_offer_2026-06-23.html"
        ),
        evidence_payload_hash=(
            "sha256:ed8913dbfc12456da4fce655053ff6f15b1004e8d65e0b77957699ed7feeff70"
        ),
        source_label=(
            "Skoda Sweden Enyaq 85 Solid Edition campaign page; "
            "Kampanjpris 599500 SEK, Ord.pris 619800 SEK"
        ),
        effective_date="2026-06-17",
        availability_text=(
            "Campaign offer valid until 2026-09-30; current campaign price "
            "599500 SEK; ordinary price 619800 SEK."
        ),
        pdf_url="https://www.skoda.se/_doc/1d23f075-2685-40f9-ad6f-a31b09f3d660",
        pdf_snapshot_path=(
            "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/"
            "skoda_enyaq_solid_edition_prislista_2026-06-17.pdf"
        ),
        pdf_payload_hash=(
            "sha256:c52ac543014eed3f2c235ef870b25279fe073e19e8e154eeef2a9256c2e88dfb"
        ),
        evidence_kind="official_campaign_vs_regular_price",
        notes=(
            "Official Skoda Sweden campaign evidence. This is a campaign price "
            "drop against the official ordinary price, not a permanent MSRP cut."
        ),
    ),
    SwedenBackfillRow(
        country="瑞典",
        brand="VOLVO",
        jato_model="EX90",
        jato_trim="Ultra Pro Edition",
        jato_powertrain="BEV",
        previous_source_msrp_value=Decimal("1148800.00"),
        previous_source_currency="SEK",
        current_source_msrp_value=Decimal("1099900.00"),
        evidence_url="https://www.volvocars.com/se/promotions/",
        evidence_snapshot_path=(
            "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/"
            "volvo_ex90_ultra_pro_edition_offer_2026-06-23.md"
        ),
        evidence_payload_hash=(
            "sha256:bebcf5523ef6cf90387056d164fed9ee46e60559b4414791a3b24a833ac2d4b2"
        ),
        source_label=(
            "Volvo Sweden promotions page; EX90 Ultra Pro Edition recommended "
            "price 1099900 SEK, ordinary price 1148800 SEK"
        ),
        effective_date="2026-06-23",
        availability_text=(
            "Official promotion page states current recommended price 1099900 SEK; "
            "ordinary price 1148800 SEK; offer applies to model year 2027."
        ),
        pdf_url="",
        pdf_snapshot_path="",
        pdf_payload_hash="",
        evidence_kind="official_promotion_vs_ordinary_price",
        notes=(
            "Official Volvo Sweden promotion evidence. Direct curl and Playwright "
            "requests are blocked by Akamai from this environment, so the local "
            "artifact is an extracted evidence note rather than the full source "
            "HTML. This is a promotion price against the official ordinary price, "
            "not a verified permanent MSRP cut."
        ),
    ),
    SwedenBackfillRow(
        country="瑞典",
        brand="VOLKSWAGEN",
        jato_model="TAYRON",
        jato_trim="R-Line SWE Edition",
        jato_powertrain="",
        previous_source_msrp_value=Decimal("711500.00"),
        previous_source_currency="SEK",
        current_source_msrp_value=Decimal("611400.00"),
        evidence_url="https://www.volkswagen.se/sv/kop-en-vw/erbjudanden.html",
        evidence_snapshot_path=(
            "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/"
            "volkswagen_offers_2026-06-24.html"
        ),
        evidence_payload_hash=(
            "sha256:d8a00af25cd34f328b98f68b1a4a4427b4fb346fba69ec01ff9683c01a293af1"
        ),
        source_label=(
            "Volkswagen Sweden offers page; Tayron R-Line SWE Edition saves "
            "up to 100100 SEK, matched to official configurator price 611400 SEK"
        ),
        effective_date="2026-06-24",
        availability_text=(
            "Official Volkswagen Sweden offers page states Tayron R-Line SWE "
            "Edition can save up to 100100 SEK; official configurator lists "
            "R-Line SWE Edition price including VAT at 611400 SEK."
        ),
        pdf_url="",
        pdf_snapshot_path="",
        pdf_payload_hash="",
        evidence_kind="official_campaign_savings_vs_current_price",
        notes=(
            "Official Volkswagen Sweden campaign savings evidence. The previous "
            "baseline is inferred as current official price plus the official "
            "maximum saving amount, so this is a campaign/savings boundary for "
            "spot-check, not a verified permanent MSRP cut."
        ),
        secondary_evidence_url="https://www.volkswagen.se/sv/bygg-din-bil.html/__app/31150.app",
        secondary_evidence_snapshot_path=(
            "03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/"
            "volkswagen_tayron_configurator_2026-06-24.html"
        ),
        secondary_evidence_payload_hash=(
            "sha256:a6ee76a1606f1062660f2d54ed26962e52ca3b3c14ec623e7c543063b8d0f157"
        ),
        secondary_evidence_label=(
            "Volkswagen Sweden Tayron configurator; R-Line SWE Edition price "
            "including VAT 611400 SEK"
        ),
    ),
]


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _powertrain(value: str | None) -> str:
    return str(value or "").strip()


def _batch_code(row: SwedenBackfillRow) -> str:
    safe_brand = row.brand.lower().replace(" ", "-")
    safe_model = row.jato_model.lower().replace(" ", "-")
    safe_trim = row.jato_trim.lower().replace(" ", "-")
    return f"msrp-backfill-se-2026-official-{safe_brand}-{safe_model}-{safe_trim}"


def _find_current_price(session: Session, row: SwedenBackfillRow) -> CurrentPrice:
    stmt = select(CurrentPrice).where(
        CurrentPrice.country == row.country,
        CurrentPrice.brand == row.brand,
        CurrentPrice.jato_model == row.jato_model,
        CurrentPrice.jato_trim == row.jato_trim,
        CurrentPrice.jato_powertrain == row.jato_powertrain,
    )
    current_price = session.execute(stmt).scalar_one_or_none()
    if current_price is None:
        raise RuntimeError(
            f"Current price not found for {row.country}/{row.brand}/{row.jato_model}/{row.jato_trim}"
        )
    return current_price


def _find_current_observation(
    session: Session,
    current_price: CurrentPrice,
) -> MsrpObservation:
    stmt = select(MsrpObservation).where(
        MsrpObservation.observation_id == current_price.effective_observation_id
    )
    observation = session.execute(stmt).scalar_one()
    return observation


def _find_open_period(
    session: Session,
    row: SwedenBackfillRow,
) -> PriceHistory:
    stmt = select(PriceHistory).where(
        PriceHistory.country == row.country,
        PriceHistory.brand == row.brand,
        PriceHistory.jato_model == row.jato_model,
        PriceHistory.jato_trim == row.jato_trim,
        PriceHistory.jato_powertrain == _powertrain(row.jato_powertrain),
        PriceHistory.valid_to_utc.is_(None),
    )
    period = session.execute(stmt).scalar_one_or_none()
    if period is None:
        raise RuntimeError(
            f"Open price period not found for {row.country}/{row.brand}/{row.jato_model}/{row.jato_trim}"
        )
    return period


def _find_existing_backfill_observation(
    session: Session,
    row: SwedenBackfillRow,
) -> MsrpObservation | None:
    stmt = (
        select(MsrpObservation)
        .where(
            MsrpObservation.country == row.country,
            MsrpObservation.brand == row.brand,
            MsrpObservation.jato_model == row.jato_model,
            MsrpObservation.jato_trim == row.jato_trim,
            MsrpObservation.source_payload_hash == row.evidence_payload_hash,
        )
        .limit(1)
    )
    return session.execute(stmt).scalar_one_or_none()


def _find_existing_period_for_observation(
    session: Session,
    observation: MsrpObservation,
) -> PriceHistory | None:
    stmt = select(PriceHistory).where(
        PriceHistory.started_by_observation_id == observation.observation_id
    )
    return session.execute(stmt).scalar_one_or_none()


def _get_or_create_batch(
    session: Session,
    row: SwedenBackfillRow,
    now: datetime,
) -> ScrapeBatch:
    batch_code = _batch_code(row)
    stmt = select(ScrapeBatch).where(ScrapeBatch.batch_code == batch_code)
    batch = session.execute(stmt).scalar_one_or_none()
    if batch is not None:
        return batch
    batch = ScrapeBatch(
        batch_code=batch_code,
        trigger_type="historical_backfill",
        scope_country=row.country,
        scope_brands_json=[row.brand],
        candidate_count=1,
        success_count=1,
        review_required_count=0,
        failed_count=0,
        status="completed",
        started_at_utc=now,
        finished_at_utc=now,
        notes=row.notes,
    )
    session.add(batch)
    session.flush()
    return batch


def _build_observation(
    row: SwedenBackfillRow,
    current_observation: MsrpObservation,
    batch: ScrapeBatch,
    open_period: PriceHistory,
) -> MsrpObservation:
    fx_rate = Decimal(str(current_observation.fx_rate_to_eur))
    previous_msrp_eur = _money(row.previous_source_msrp_value * fx_rate)
    observed_at = open_period.valid_from_utc - timedelta(seconds=1)
    official_evidence = {}
    if row.pdf_url:
        official_evidence = {
            "pdfUrl": row.pdf_url,
            "pdfSnapshotPath": row.pdf_snapshot_path,
            "pdfPayloadHash": row.pdf_payload_hash,
        }
    related_official_evidence = {}
    if row.secondary_evidence_url:
        related_official_evidence = {
            "url": row.secondary_evidence_url,
            "snapshotPath": row.secondary_evidence_snapshot_path,
            "payloadHash": row.secondary_evidence_payload_hash,
            "label": row.secondary_evidence_label,
        }
    return MsrpObservation(
        scrape_batch_id=batch.scrape_batch_id,
        source_id=current_observation.source_id,
        country=row.country,
        brand=row.brand,
        jato_model=row.jato_model,
        jato_trim=row.jato_trim,
        jato_powertrain=row.jato_powertrain,
        official_model=current_observation.official_model,
        official_trim=current_observation.official_trim,
        official_edition=current_observation.official_edition,
        official_powertrain=current_observation.official_powertrain,
        msrp_value=previous_msrp_eur,
        currency=current_observation.currency,
        source_msrp_value=row.previous_source_msrp_value,
        source_currency=row.previous_source_currency,
        fx_rate_to_eur=current_observation.fx_rate_to_eur,
        fx_rate_as_of_date=current_observation.fx_rate_as_of_date,
        fx_source=current_observation.fx_source,
        tax_included=current_observation.tax_included,
        price_label="Official ordinary price baseline",
        availability_text=row.availability_text,
        observed_at_utc=observed_at,
        source_url=row.evidence_url,
        source_snapshot_path=row.evidence_snapshot_path,
        source_payload_hash=row.evidence_payload_hash,
        extraction_version="historical-backfill-v1",
        match_confidence=Decimal("0.9800"),
        match_status="auto_accepted",
        match_reason_json={
            "resolver": "sweden_2026_official_backfill",
            "matchedAgainstCurrentObservationId": str(current_observation.observation_id),
            "matchBasis": (
                "same country/brand/model/trim/powertrain with official Sweden "
                "backfill evidence"
            ),
        },
        source_context_json={
            "historicalPriceBackfill": {
                "enabled": True,
                "kind": row.evidence_kind,
                "sourceLabel": row.source_label,
                "effectiveDate": row.effective_date,
                "evidenceUrl": row.evidence_url,
                "snapshotPath": row.evidence_snapshot_path,
                "capturedAtUtc": datetime.now(timezone.utc).isoformat(),
                "notes": row.notes,
            },
            "pricePeriodBackfill": {
                "validFromStrategy": "one_second_before_current_open_period",
                "validToObservationId": str(open_period.started_by_observation_id),
                "validToUtc": open_period.valid_from_utc.isoformat(),
            },
            "officialEvidence": official_evidence,
            "relatedOfficialEvidence": related_official_evidence,
            "campaignPrice": {
                "sourceMsrpValue": str(row.current_source_msrp_value),
                "sourceCurrency": row.previous_source_currency,
            },
            "ordinaryPrice": {
                "sourceMsrpValue": str(row.previous_source_msrp_value),
                "sourceCurrency": row.previous_source_currency,
            },
        },
        created_at_utc=datetime.now(timezone.utc),
        updated_at_utc=datetime.now(timezone.utc),
    )


def _insert_price_period(
    session: Session,
    row: SwedenBackfillRow,
    observation: MsrpObservation,
    open_period: PriceHistory,
) -> PriceHistory:
    period = PriceHistory(
        country=row.country,
        brand=row.brand,
        jato_model=row.jato_model,
        jato_trim=row.jato_trim,
        jato_powertrain=_powertrain(row.jato_powertrain),
        msrp_value=observation.msrp_value,
        currency=observation.currency,
        source_msrp_value=observation.source_msrp_value,
        source_currency=observation.source_currency,
        valid_from_utc=observation.observed_at_utc,
        valid_to_utc=open_period.valid_from_utc,
        last_confirmed_at_utc=observation.observed_at_utc,
        started_by_observation_id=observation.observation_id,
        ended_by_observation_id=open_period.started_by_observation_id,
        last_confirmed_by_observation_id=observation.observation_id,
        created_at_utc=datetime.now(timezone.utc),
    )
    session.add(period)
    session.flush()
    return period


def apply_row(session: Session, row: SwedenBackfillRow, *, apply: bool) -> dict[str, object]:
    current_price = _find_current_price(session, row)
    current_observation = _find_current_observation(session, current_price)
    open_period = _find_open_period(session, row)
    existing_observation = _find_existing_backfill_observation(session, row)
    if existing_observation is not None:
        existing_period = _find_existing_period_for_observation(
            session,
            existing_observation,
        )
        return {
            "status": "exists",
            "observationId": str(existing_observation.observation_id),
            "priceHistoryId": str(existing_period.price_history_id) if existing_period else None,
            "brand": row.brand,
            "model": row.jato_model,
            "trim": row.jato_trim,
        }

    change_pct = (
        (row.current_source_msrp_value - row.previous_source_msrp_value)
        / row.previous_source_msrp_value
        * Decimal("100")
    )
    if not apply:
        return {
            "status": "dry_run",
            "brand": row.brand,
            "model": row.jato_model,
            "trim": row.jato_trim,
            "previousSourceMsrp": float(row.previous_source_msrp_value),
            "currentSourceMsrp": float(row.current_source_msrp_value),
            "changePct": float(change_pct.quantize(Decimal("0.01"))),
            "currentPriceId": str(current_price.current_price_id),
            "openPriceHistoryId": str(open_period.price_history_id),
        }

    batch = _get_or_create_batch(session, row, datetime.now(timezone.utc))
    observation = _build_observation(row, current_observation, batch, open_period)
    session.add(observation)
    session.flush()
    period = _insert_price_period(session, row, observation, open_period)
    return {
        "status": "inserted",
        "observationId": str(observation.observation_id),
        "priceHistoryId": str(period.price_history_id),
        "brand": row.brand,
        "model": row.jato_model,
        "trim": row.jato_trim,
        "previousSourceMsrp": float(row.previous_source_msrp_value),
        "currentSourceMsrp": float(row.current_source_msrp_value),
        "changePct": float(change_pct.quantize(Decimal("0.01"))),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill verified Sweden 2026 official MSRP evidence.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write observations and price_history periods. Defaults to dry-run.",
    )
    args = parser.parse_args()

    session_factory = get_session_factory()
    with session_factory() as session:
        results = [
            apply_row(session, row, apply=args.apply)
            for row in BACKFILL_ROWS
        ]
        if args.apply:
            session.commit()
        else:
            session.rollback()
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
