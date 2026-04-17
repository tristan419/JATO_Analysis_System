#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BACKEND_ROOT = REPO_ROOT / "06_AppPlatform" / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import get_session_factory
from app.services.evkx_import_service import import_evkx_catalog_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import EVKX catalog JSON into MSRP review workflow.",
    )
    parser.add_argument("catalog_path", help="Path to EVKX catalog JSON file")
    parser.add_argument(
        "--target-country",
        help="Override target market country; defaults to catalog metadata.pricingCountry",
    )
    parser.add_argument(
        "--batch-code",
        help="Optional custom scrape batch code",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build payload and summary without writing to the database",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session_factory = get_session_factory()
    session = session_factory()
    try:
        result = import_evkx_catalog_file(
            session,
            args.catalog_path,
            target_country=args.target_country,
            batch_code=args.batch_code,
            dry_run=args.dry_run,
        )
    finally:
        session.close()

    print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
