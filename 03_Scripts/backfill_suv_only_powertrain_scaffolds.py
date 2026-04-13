from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOLKIT_ROOT = PROJECT_ROOT / "07_ScrapingToolkit"
DEFAULT_REPORT_PATH = (
    PROJECT_ROOT
    / "04_Processed_data"
    / "msrp_candidate_scope"
    / "suv_only"
    / "candidate_scope_report.json"
)
DEFAULT_DRAFT_ROOT = (
    PROJECT_ROOT
    / "07_ScrapingToolkit"
    / "source_drafts"
    / "suv_only_country_model_top30"
)


def _split_header_and_yaml(text: str) -> tuple[str, str]:
    lines = text.splitlines(keepends=True)
    index = 0
    while index < len(lines):
        line = lines[index]
        if line.startswith("#") or not line.strip():
            index += 1
            continue
        break
    return "".join(lines[:index]), "".join(lines[index:])


def _normalize_label(value: object) -> str:
    return " ".join(str(value or "").strip().split()).upper()


def _keyword_is_placeholder(keyword: object) -> bool:
    return str(keyword or "").strip().upper().startswith("TODO")


def _rule_is_placeholder(rule: dict[str, Any]) -> bool:
    key = str(rule.get("key") or "").strip().lower()
    powertrain = str(rule.get("powertrain") or "").strip().upper()
    if key == "powertrain_primary":
        keywords = rule.get("keywords")
        if not isinstance(keywords, list) or not keywords:
            return True
        return all(_keyword_is_placeholder(keyword) for keyword in keywords)
    if powertrain.startswith("TODO") or not powertrain:
        return True
    return False


def _bonus_is_placeholder(rule: dict[str, Any]) -> bool:
    powertrain = str(rule.get("powertrain") or "").strip().upper()
    return not powertrain or powertrain.startswith("TODO")


def _extract_existing_powertrains(profile: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    fixed = _normalize_label(profile.get("fixed_jato_powertrain"))
    if fixed and not fixed.startswith("TODO"):
        values.add(fixed)

    structured = profile.get("structured_fields") or {}
    rules = structured.get("powertrain_rules") or []
    if isinstance(rules, list):
        for rule in rules:
            if isinstance(rule, dict):
                label = _normalize_label(rule.get("powertrain"))
                if label and not label.startswith("TODO"):
                    values.add(label)

    confidence = profile.get("confidence_rules") or {}
    bonuses = confidence.get("powertrain_bonuses") or []
    if isinstance(bonuses, list):
        for rule in bonuses:
            if isinstance(rule, dict):
                label = _normalize_label(rule.get("powertrain"))
                if label and not label.startswith("TODO"):
                    values.add(label)
    return values


def _build_powertrain_rules(
    powertrains: tuple[str, ...],
    source_bootstrap: Any,
) -> list[dict[str, Any]]:
    return [
        {
            "key": f"powertrain_{source_bootstrap._slugify_token(powertrain)}",
            "powertrain": powertrain,
            "keywords": [
                "TODO_"
                f"{source_bootstrap._slugify_token(powertrain).upper()}"
                "_KEYWORD"
            ],
        }
        for powertrain in powertrains
    ]


def _build_powertrain_bonuses(
    powertrains: tuple[str, ...],
    source_bootstrap: Any,
) -> list[dict[str, Any]]:
    return [
        {
            "key": f"powertrain_{source_bootstrap._slugify_token(powertrain)}",
            "label": f"Powertrain matched: {powertrain}",
            "powertrain": powertrain,
            "delta": 0.03,
        }
        for powertrain in powertrains
    ]


def _should_update_rules(profile: dict[str, Any]) -> bool:
    structured = profile.get("structured_fields") or {}
    rules = structured.get("powertrain_rules")
    if not isinstance(rules, list) or not rules:
        return True
    return any(
        isinstance(rule, dict) and _rule_is_placeholder(rule)
        for rule in rules
    )


def _should_update_bonuses(profile: dict[str, Any]) -> bool:
    confidence = profile.get("confidence_rules") or {}
    bonuses = confidence.get("powertrain_bonuses")
    if not isinstance(bonuses, list) or not bonuses:
        return True
    return any(
        isinstance(rule, dict) and _bonus_is_placeholder(rule)
        for rule in bonuses
    )


def _normalize_lookup_key(
    data: dict[str, Any],
    source_bootstrap: Any,
) -> tuple[str, str, str]:
    profile = data.get("profile") or {}
    bootstrap_meta = data.get("bootstrap_meta") or {}
    country = data.get("country")
    brand = data.get("brand")
    model = (
        profile.get("fixed_jato_model")
        or profile.get("fixed_model")
        or bootstrap_meta.get("model")
    )
    return (
        source_bootstrap._normalize_lookup_value(country),
        source_bootstrap._normalize_lookup_value(brand),
        source_bootstrap._normalize_lookup_value(model),
    )


def _dump_yaml(data: dict[str, Any]) -> str:
    return yaml.safe_dump(
        data,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill JATO powertrain scaffold into existing SUV-only "
            "country-model draft YAMLs without overwriting manual research."
        ),
    )
    parser.add_argument("--report-path", default=str(DEFAULT_REPORT_PATH))
    parser.add_argument("--draft-root", default=str(DEFAULT_DRAFT_ROOT))
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    if str(TOOLKIT_ROOT) not in sys.path:
        sys.path.insert(0, str(TOOLKIT_ROOT))

    from jato_scraper import source_bootstrap

    parser = build_arg_parser()
    args = parser.parse_args(argv)

    report = source_bootstrap.load_candidate_scope_report(args.report_path)
    lookup = source_bootstrap.load_jato_powertrain_lookup(report)
    draft_root = Path(args.draft_root).expanduser().resolve()

    updated_paths: list[str] = []
    skipped_paths: list[str] = []
    missing_lookup_paths: list[str] = []

    draft_paths = sorted(path for path in draft_root.rglob("*.yaml"))
    if args.limit > 0:
        draft_paths = draft_paths[: args.limit]

    for path in draft_paths:
        original_text = path.read_text(encoding="utf-8")
        header, body = _split_header_and_yaml(original_text)
        data = yaml.safe_load(body)
        if not isinstance(data, dict):
            skipped_paths.append(str(path.relative_to(draft_root)))
            continue

        lookup_key = _normalize_lookup_key(data, source_bootstrap)
        desired_powertrains = tuple(lookup.get(lookup_key, ()))
        if not desired_powertrains:
            missing_lookup_paths.append(str(path.relative_to(draft_root)))
            continue

        profile = data.setdefault("profile", {})
        structured = profile.setdefault("structured_fields", {})
        confidence = profile.setdefault("confidence_rules", {})

        current_powertrains = _extract_existing_powertrains(profile)
        if (
            current_powertrains
            and current_powertrains == set(desired_powertrains)
        ):
            if (
                not _should_update_rules(profile)
                and not _should_update_bonuses(profile)
            ):
                skipped_paths.append(str(path.relative_to(draft_root)))
                continue

        changed = False
        if _should_update_rules(profile):
            structured["powertrain_rules"] = _build_powertrain_rules(
                desired_powertrains,
                source_bootstrap,
            )
            changed = True

        if _should_update_bonuses(profile):
            confidence["powertrain_bonuses"] = _build_powertrain_bonuses(
                desired_powertrains,
                source_bootstrap,
            )
            changed = True

        if not changed:
            skipped_paths.append(str(path.relative_to(draft_root)))
            continue

        updated_paths.append(str(path.relative_to(draft_root)))
        if args.dry_run:
            continue

        path.write_text(header + _dump_yaml(data), encoding="utf-8")

    print(f"TOTAL={len(draft_paths)}")
    print(f"UPDATED={len(updated_paths)}")
    print(f"SKIPPED={len(skipped_paths)}")
    print(f"MISSING_LOOKUP={len(missing_lookup_paths)}")
    for label, values in (
        ("UPDATED_PATH", updated_paths[:20]),
        ("MISSING_LOOKUP_PATH", missing_lookup_paths[:20]),
    ):
        for value in values:
            print(f"{label}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
