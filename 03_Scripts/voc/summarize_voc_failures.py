#!/usr/bin/env python3
"""Summarize per-source VOC failures from a raw collection summary."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        return [value]
    return []


def _resolve_output_path(path: str | None, repo_root: Path) -> Path | None:
    if not path:
        return None
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return repo_root / candidate


def _load_raw_payload(path: str | None, repo_root: Path) -> dict[str, Any] | None:
    resolved = _resolve_output_path(path, repo_root)
    if not resolved or not resolved.is_file():
        return None
    try:
        data = json.loads(resolved.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def build_failed_sources(
    summary: Any,
    *,
    repo_root: Path,
    raw_payload_loader: Callable[[str | None, Path], dict[str, Any] | None] = _load_raw_payload,
) -> list[dict[str, Any]]:
    """Extract failed source records from current and legacy VOC summaries."""
    failed_sources: list[dict[str, Any]] = []

    for batch in _items(summary):
        batch_code = batch.get("batch_code") or batch.get("batchCode")

        # Current voc_fetcher summary shape:
        # [{batch_code, countries: [{country_code, sources: [{error_count, ...}]}]}]
        for country in _items(batch.get("countries")):
            country_code = country.get("country_code") or country.get("countryCode")
            for source in _items(country.get("sources")):
                output_path = source.get("output_path") or source.get("outputPath")
                raw_payload = raw_payload_loader(output_path, repo_root) if output_path else None
                raw_errors = []
                if isinstance(raw_payload, dict) and isinstance(raw_payload.get("errors"), list):
                    raw_errors = [
                        item for item in raw_payload["errors"] if isinstance(item, dict)
                    ]

                error_count = _coerce_int(
                    source.get("error_count", source.get("errorCount")),
                )
                if raw_errors and error_count == 0:
                    error_count = len(raw_errors)

                if error_count <= 0:
                    continue

                failed_sources.append(
                    {
                        "batchCode": batch_code,
                        "country": source.get("country_code")
                        or source.get("countryCode")
                        or country_code,
                        "source": source.get("source_code") or source.get("sourceCode"),
                        "siteName": source.get("site_name") or source.get("siteName"),
                        "documentCount": _coerce_int(
                            source.get("document_count", source.get("documentCount")),
                        ),
                        "errorCount": error_count,
                        "errors": raw_errors[:10],
                        "outputPath": output_path,
                    }
                )

        # Legacy flat shape kept for older raw summaries written before voc_fetcher
        # started returning batch/country/source aggregates.
        status = str(batch.get("status") or "").strip().lower()
        if status == "failed" or batch.get("error"):
            failed_sources.append(
                {
                    "batchCode": batch_code,
                    "country": batch.get("country") or batch.get("country_code"),
                    "source": batch.get("source") or batch.get("source_code"),
                    "siteName": batch.get("siteName") or batch.get("site_name"),
                    "documentCount": _coerce_int(batch.get("documentCount")),
                    "errorCount": 1,
                    "errors": [{"error": batch.get("error")}],
                    "outputPath": batch.get("outputPath") or batch.get("output_path"),
                }
            )

    return failed_sources


def build_failure_summary(summary: Any, *, repo_root: Path) -> dict[str, Any]:
    failed_sources = build_failed_sources(summary, repo_root=repo_root)
    return {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "failedCount": len(failed_sources),
        "failedSources": failed_sources,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build voc-failed-sources.json from voc-raw-latest.json.",
    )
    parser.add_argument("summary_path")
    parser.add_argument("--output")
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[2])
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    summary_path = Path(args.summary_path).expanduser()
    if not summary_path.is_absolute():
        summary_path = repo_root / summary_path
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    payload = build_failure_summary(summary, repo_root=repo_root)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)

    if args.output:
        output_path = Path(args.output).expanduser()
        if not output_path.is_absolute():
            output_path = repo_root / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
