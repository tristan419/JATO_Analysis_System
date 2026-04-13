from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOLKIT_ROOT = PROJECT_ROOT / "07_ScrapingToolkit"


def _run() -> int:
    if str(TOOLKIT_ROOT) not in sys.path:
        sys.path.insert(0, str(TOOLKIT_ROOT))

    from jato_scraper.source_bootstrap import main

    return main()


if __name__ == "__main__":
    raise SystemExit(_run())
