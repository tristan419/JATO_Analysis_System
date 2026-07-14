#!/usr/bin/env python3
"""Cleanup MSRP rows for one or more source codes.

Default mode is dry-run. Use --execute to apply changes.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from sqlalchemy import delete, func, select


ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import DATABASE_ENABLED, DATABASE_URL  # noqa: E402
from app.db.models import (  # noqa: E402
    CurrentPrice,
    FinanceObservation,
    MsrpObservation,
    MsrpSource,
    PriceHistory,
    ReviewCase,
    ReviewDecision,
    ScrapeBatch,
)
from app.db.session import get_session_factory  # noqa: E402


@dataclass(frozen=True)
class SourceCleanupPlan:
    source_code: str
    source_id: object
    observation_ids: tuple[object, ...]
    review_case_ids: tuple[object, ...]
    batch_ids: tuple[object, ...]
    current_price_ids: tuple[object, ...]
    finance_observation_ids: tuple[object, ...]
    price_history_ids: tuple[object, ...]
    review_decision_ids: tuple[object, ...]
    orphan_batch_ids: tuple[object, ...]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cleanup MSRP sample data by source code"
    )
    parser.add_argument(
        "--source-code",
        action="append",
        dest="source_codes",
        required=True,
        help="MSRP source_code to cleanup; repeat for multiple sources",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Apply the deletion; default is dry-run",
    )
    parser.add_argument(
        "--delete-source",
        action="append",
        default=[],
        help="Delete the source registry row after deleting its data",
    )
    parser.add_argument(
        "--disable-source",
        action="append",
        default=[],
        help="Disable the source registry row after deleting its data",
    )
    return parser.parse_args()


def _tuple_ids(values: Iterable[object]) -> tuple[object, ...]:
    return tuple(values)


def _collect_plan(session, source_code: str) -> SourceCleanupPlan | None:
    source = session.execute(
        select(MsrpSource).where(MsrpSource.source_code == source_code)
    ).scalar_one_or_none()
    if source is None:
        return None

    observation_ids = _tuple_ids(
        session.execute(
            select(MsrpObservation.observation_id).where(
                MsrpObservation.source_id == source.source_id
            )
        ).scalars()
    )
    review_case_ids = _tuple_ids(
        session.execute(
            select(ReviewCase.review_case_id).where(
                ReviewCase.observation_id.in_(observation_ids)
            )
        ).scalars()
    ) if observation_ids else ()
    batch_ids = _tuple_ids(
        session.execute(
            select(MsrpObservation.scrape_batch_id)
            .where(MsrpObservation.source_id == source.source_id)
            .distinct()
        ).scalars()
    )
    current_price_ids = _tuple_ids(
        session.execute(
            select(CurrentPrice.current_price_id).where(
                CurrentPrice.effective_observation_id.in_(observation_ids)
            )
        ).scalars()
    ) if observation_ids else ()
    finance_observation_ids = _tuple_ids(
        session.execute(
            select(FinanceObservation.finance_observation_id).where(
                FinanceObservation.observation_id.in_(observation_ids)
            )
        ).scalars()
    ) if observation_ids else ()
    price_history_ids = _tuple_ids(
        session.execute(
            select(PriceHistory.price_history_id).where(
                PriceHistory.started_by_observation_id.in_(observation_ids)
                | PriceHistory.ended_by_observation_id.in_(observation_ids)
                | PriceHistory.last_confirmed_by_observation_id.in_(
                    observation_ids
                )
            )
        ).scalars()
    ) if observation_ids else ()
    review_decision_ids = _tuple_ids(
        session.execute(
            select(ReviewDecision.review_decision_id).where(
                ReviewDecision.review_case_id.in_(review_case_ids)
                | ReviewDecision.observation_id.in_(observation_ids)
            )
        ).scalars()
    ) if observation_ids or review_case_ids else ()

    orphan_batch_ids: list[object] = []
    for batch_id in batch_ids:
        remaining = session.execute(
            select(func.count())
            .select_from(MsrpObservation)
            .where(
                MsrpObservation.scrape_batch_id == batch_id,
                MsrpObservation.source_id != source.source_id,
            )
        ).scalar_one()
        if int(remaining or 0) == 0:
            orphan_batch_ids.append(batch_id)

    return SourceCleanupPlan(
        source_code=source_code,
        source_id=source.source_id,
        observation_ids=observation_ids,
        review_case_ids=review_case_ids,
        batch_ids=batch_ids,
        current_price_ids=current_price_ids,
        finance_observation_ids=finance_observation_ids,
        price_history_ids=price_history_ids,
        review_decision_ids=review_decision_ids,
        orphan_batch_ids=tuple(orphan_batch_ids),
    )


def _apply_plan(session, plan: SourceCleanupPlan) -> None:
    if plan.current_price_ids or plan.price_history_ids:
        raise RuntimeError(
            "Direct CurrentPrice/PriceHistory cleanup is disabled; create a "
            "persisted editor compensation approval and execution instead."
        )
    if plan.review_decision_ids:
        session.execute(
            delete(ReviewDecision).where(
                ReviewDecision.review_decision_id.in_(plan.review_decision_ids)
            )
        )
    if plan.finance_observation_ids:
        session.execute(
            delete(FinanceObservation).where(
                FinanceObservation.finance_observation_id.in_(
                    plan.finance_observation_ids
                )
            )
        )
    if plan.review_case_ids:
        session.execute(
            delete(ReviewCase).where(
                ReviewCase.review_case_id.in_(plan.review_case_ids)
            )
        )
    if plan.observation_ids:
        session.execute(
            delete(MsrpObservation).where(
                MsrpObservation.observation_id.in_(plan.observation_ids)
            )
        )
    if plan.orphan_batch_ids:
        session.execute(
            delete(ScrapeBatch).where(
                ScrapeBatch.scrape_batch_id.in_(plan.orphan_batch_ids)
            )
        )


def _delete_empty_batches(session, batch_ids: Iterable[object]) -> None:
    for batch_id in set(batch_ids):
        remaining = session.execute(
            select(func.count())
            .select_from(MsrpObservation)
            .where(MsrpObservation.scrape_batch_id == batch_id)
        ).scalar_one()
        if int(remaining or 0) == 0:
            session.execute(
                delete(ScrapeBatch).where(
                    ScrapeBatch.scrape_batch_id == batch_id
                )
            )


def main() -> int:
    args = _parse_args()
    if not DATABASE_ENABLED or not DATABASE_URL:
        print(
            "[cleanup] database is not configured; "
            "set APP_DATABASE_URL first"
        )
        return 1

    delete_source_codes = set(args.delete_source)
    disable_source_codes = set(args.disable_source)
    overlap = delete_source_codes & disable_source_codes
    if overlap:
        print(f"[cleanup] cannot both delete and disable: {sorted(overlap)}")
        return 1

    session = get_session_factory()()
    try:
        plans: list[SourceCleanupPlan] = []
        for code in args.source_codes:
            plan = _collect_plan(session, code)
            if plan is None:
                print(f"[cleanup] source not found: {code}")
                continue
            plans.append(plan)

        if not plans:
            print("[cleanup] no matching sources found")
            return 1

        print("[cleanup] plan summary")
        for plan in plans:
            print(
                f"- {plan.source_code}: obs={len(plan.observation_ids)} "
                f"reviewCases={len(plan.review_case_ids)} "
                f"reviewDecisions={len(plan.review_decision_ids)} "
                f"currentPrices={len(plan.current_price_ids)} "
                f"financeObs={len(plan.finance_observation_ids)} "
                f"priceHistory={len(plan.price_history_ids)} "
                f"orphanBatches={len(plan.orphan_batch_ids)}"
            )

        if not args.execute:
            print("[cleanup] dry-run only; rerun with --execute to apply")
            session.rollback()
            return 0

        for plan in plans:
            _apply_plan(session, plan)
        _delete_empty_batches(
            session,
            (
                batch_id
                for plan in plans
                for batch_id in plan.batch_ids
            ),
        )

        if disable_source_codes:
            sources = session.execute(
                select(MsrpSource).where(
                    MsrpSource.source_code.in_(disable_source_codes)
                )
            ).scalars().all()
            for source in sources:
                source.enabled = False

        if delete_source_codes:
            session.execute(
                delete(MsrpSource).where(
                    MsrpSource.source_code.in_(delete_source_codes)
                )
            )

        session.commit()
        print("[cleanup] applied successfully")
        return 0
    except Exception as exc:
        session.rollback()
        print(f"[cleanup] failed: {type(exc).__name__}: {exc}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    raise SystemExit(main())
