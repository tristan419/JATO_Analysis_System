#!/usr/bin/env python3
"""Fill MSRP source draft powertrain keyword placeholders.

Default mode is dry-run. Use --execute to write YAML files.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from typing import Iterable


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_DRAFT_DIR = (
    ROOT
    / "07_ScrapingToolkit"
    / "source_drafts"
    / "suv_only_country_model_top30"
)

POWERTRAIN_TOKENS = {
    "TODO_BEV_KEYWORD": "BEV",
    "TODO_PHEV_KEYWORD": "PHEV",
    "TODO_HEV_KEYWORD": "HEV",
    "TODO_MHEV_KEYWORD": "MHEV",
    "TODO_ICE_KEYWORD": "ICE",
    "TODO_LPG_KEYWORD": "LPG",
}
TOKEN_LINE_RE = re.compile(
    r"^(?P<indent>\s*)-\s+(?P<token>TODO_(?:BEV|PHEV|HEV|MHEV|ICE|LPG)_KEYWORD)\s*$"
)

COMMON_KEYWORDS: dict[str, tuple[str, ...]] = {
    "BEV": (
        "bev",
        "ev",
        "electric",
        "battery electric",
        "fully electric",
        "electric vehicle",
    ),
    "PHEV": (
        "phev",
        "plug-in hybrid",
        "plug in hybrid",
        "plugin hybrid",
    ),
    "HEV": (
        "hev",
        "full hybrid",
        "self-charging hybrid",
        "self charging hybrid",
        "hybrid",
    ),
    "MHEV": (
        "mhev",
        "mild hybrid",
        "mild-hybrid",
        "48V",
    ),
    "ICE": (
        "ice",
        "petrol",
        "gasoline",
        "diesel",
        "combustion",
    ),
    "LPG": (
        "lpg",
        "autogas",
    ),
}

COUNTRY_KEYWORDS: dict[str, dict[str, tuple[str, ...]]] = {
    "at": {
        "BEV": ("Elektro", "elektrisch", "vollelektrisch", "Elektroauto"),
        "PHEV": ("Plug-in-Hybrid", "eHybrid"),
        "HEV": ("Vollhybrid", "Hybrid"),
        "MHEV": ("Mild-Hybrid",),
        "ICE": ("Benzin", "Diesel", "Verbrenner"),
        "LPG": ("Autogas", "Fluessiggas"),
    },
    "be": {
        "BEV": ("elektrisch", "volledig elektrisch", "electrique", "100% electrique"),
        "PHEV": ("plug-in hybride", "stekkerhybride", "hybride rechargeable"),
        "HEV": ("hybride", "full hybrid", "hybride non rechargeable"),
        "MHEV": ("mild hybrid", "mild-hybrid", "hybride leger"),
        "ICE": ("benzine", "essence", "diesel", "thermique"),
        "LPG": ("LPG", "GPL", "autogas"),
    },
    "ch": {
        "BEV": ("Elektro", "elektrisch", "vollelektrisch", "Elektroauto"),
        "PHEV": ("Plug-in-Hybrid", "eHybrid"),
        "HEV": ("Vollhybrid", "Hybrid"),
        "MHEV": ("Mild-Hybrid",),
        "ICE": ("Benzin", "Diesel", "Verbrenner"),
        "LPG": ("Autogas", "Fluessiggas"),
    },
    "cz": {
        "BEV": ("elektricky", "elektromobil", "plne elektricky"),
        "PHEV": ("plug-in hybrid", "dobijeci hybrid", "iV"),
        "HEV": ("hybrid", "full hybrid"),
        "MHEV": ("mild hybrid", "mild-hybrid", "48V"),
        "ICE": ("benzin", "diesel", "nafta"),
        "LPG": ("LPG",),
    },
    "de": {
        "BEV": ("Elektro", "elektrisch", "vollelektrisch", "Elektroauto"),
        "PHEV": ("Plug-in-Hybrid", "eHybrid"),
        "HEV": ("Vollhybrid", "Hybrid"),
        "MHEV": ("Mild-Hybrid",),
        "ICE": ("Benzin", "Diesel", "Verbrenner"),
        "LPG": ("Autogas", "Fluessiggas"),
    },
    "dk": {
        "BEV": ("elektrisk", "elbil", "fuldelektrisk"),
        "PHEV": ("plug-in hybrid", "opladningshybrid"),
        "HEV": ("hybrid",),
        "MHEV": ("mild hybrid", "48V"),
        "ICE": ("benzin", "diesel"),
        "LPG": ("LPG", "autogas"),
    },
    "es": {
        "BEV": ("electrico", "100% electrico", "vehiculo electrico"),
        "PHEV": ("hibrido enchufable", "plug-in hybrid"),
        "HEV": ("hibrido", "full hybrid"),
        "MHEV": ("microhibrido", "mild hybrid", "48V"),
        "ICE": ("gasolina", "diesel"),
        "LPG": ("GLP", "autogas"),
    },
    "fi": {
        "BEV": ("sahko", "tayssahko", "sahkoauto"),
        "PHEV": ("lataushybridi", "plug-in hybrid"),
        "HEV": ("hybridi", "tayshybridi"),
        "MHEV": ("kevythybridi", "mild hybrid", "48V"),
        "ICE": ("bensiini", "diesel"),
        "LPG": ("LPG",),
    },
    "fr": {
        "BEV": ("electrique", "100% electrique", "vehicule electrique"),
        "PHEV": ("hybride rechargeable", "plug-in hybrid"),
        "HEV": ("hybride", "full hybrid", "hybride non rechargeable"),
        "MHEV": ("micro-hybride", "hybride leger", "mild hybrid"),
        "ICE": ("essence", "diesel", "thermique"),
        "LPG": ("GPL",),
    },
    "gr": {
        "BEV": ("electric", "ilektriko"),
        "PHEV": ("plug-in hybrid", "epanafortizomeno yvridiko"),
        "HEV": ("hybrid", "yvridiko"),
        "MHEV": ("mild hybrid", "48V"),
        "ICE": ("venzini", "diesel", "petreleo"),
        "LPG": ("LPG", "autogas"),
    },
    "hr": {
        "BEV": ("elektricni", "potpuno elektricni"),
        "PHEV": ("plug-in hibrid", "plug-in hybrid"),
        "HEV": ("hibrid", "full hybrid"),
        "MHEV": ("blagi hibrid", "mild hybrid", "48V"),
        "ICE": ("benzin", "dizel"),
        "LPG": ("LPG", "autoplin"),
    },
    "hu": {
        "BEV": ("elektromos", "teljesen elektromos"),
        "PHEV": ("plug-in hibrid", "toltheto hibrid"),
        "HEV": ("hibrid", "full hybrid"),
        "MHEV": ("lagy hibrid", "mild hybrid", "48V"),
        "ICE": ("benzin", "benzines", "dizel", "diesel"),
        "LPG": ("LPG", "autogaz"),
    },
    "it": {
        "BEV": ("elettrico", "100% elettrico"),
        "PHEV": ("ibrida plug-in", "ibrido plug-in", "plug-in hybrid"),
        "HEV": ("ibrida", "ibrido", "full hybrid"),
        "MHEV": ("mild hybrid", "ibrida mild hybrid", "48V"),
        "ICE": ("benzina", "diesel"),
        "LPG": ("GPL", "LPG"),
    },
    "nl": {
        "BEV": ("elektrisch", "volledig elektrisch"),
        "PHEV": ("plug-in hybride", "stekkerhybride"),
        "HEV": ("hybride", "full hybrid"),
        "MHEV": ("mild hybrid", "mild-hybrid", "48V"),
        "ICE": ("benzine", "diesel"),
        "LPG": ("LPG", "autogas"),
    },
    "no": {
        "BEV": ("elektrisk", "elbil", "helelektrisk"),
        "PHEV": ("ladbar hybrid", "plug-in hybrid"),
        "HEV": ("hybrid",),
        "MHEV": ("mild hybrid", "48V"),
        "ICE": ("bensin", "diesel"),
        "LPG": ("LPG", "autogas"),
    },
    "pl": {
        "BEV": ("elektryczny", "w pelni elektryczny", "samochod elektryczny"),
        "PHEV": ("hybryda plug-in", "plug-in hybrid"),
        "HEV": ("hybryda", "pelna hybryda", "full hybrid"),
        "MHEV": ("mild hybrid", "miekka hybryda", "48V"),
        "ICE": ("benzyna", "diesel"),
        "LPG": ("LPG", "autogaz"),
    },
    "pt": {
        "BEV": ("eletrico", "100% eletrico", "veiculo eletrico"),
        "PHEV": ("hibrido plug-in", "plug-in hybrid"),
        "HEV": ("hibrido", "full hybrid"),
        "MHEV": ("mild hybrid", "hibrido ligeiro", "48V"),
        "ICE": ("gasolina", "diesel"),
        "LPG": ("GPL", "autogas"),
    },
    "ro": {
        "BEV": ("electric", "100% electric", "vehicul electric"),
        "PHEV": ("plug-in hybrid", "hibrid plug-in"),
        "HEV": ("hibrid", "full hybrid"),
        "MHEV": ("mild hybrid", "hibrid usor", "48V"),
        "ICE": ("benzina", "diesel", "motorina"),
        "LPG": ("GPL", "LPG"),
    },
    "se": {
        "BEV": ("elbil", "elektrisk", "helelektrisk"),
        "PHEV": ("laddhybrid", "laddbar", "plug-in hybrid"),
        "HEV": ("hybrid", "full hybrid"),
        "MHEV": ("mild hybrid", "eTSI", "48V"),
        "ICE": ("bensin", "diesel"),
        "LPG": ("LPG", "autogas"),
    },
    "si": {
        "BEV": ("elektricni", "elektricno", "elektricno vozilo"),
        "PHEV": ("prikljucni hibrid", "plug-in hybrid"),
        "HEV": ("hibrid", "hibridni", "full hybrid"),
        "MHEV": ("blagi hibrid", "mild hybrid", "48V"),
        "ICE": ("bencin", "dizel"),
        "LPG": ("LPG", "avtoplin"),
    },
    "sk": {
        "BEV": ("elektricky", "elektromobil", "plne elektricky"),
        "PHEV": ("plug-in hybrid", "nabijatelny hybrid"),
        "HEV": ("hybrid", "hybridny", "full hybrid"),
        "MHEV": ("mild hybrid", "mild-hybrid", "48V"),
        "ICE": ("benzin", "diesel", "nafta"),
        "LPG": ("LPG",),
    },
}


@dataclass
class FillReport:
    source_draft_dir: str
    execute: bool
    scanned_file_count: int = 0
    changed_file_count: int = 0
    replacement_count: int = 0
    replacements_by_country: Counter[str] = field(default_factory=Counter)
    replacements_by_token: Counter[str] = field(default_factory=Counter)
    changed_files: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "sourceDraftDir": self.source_draft_dir,
            "execute": self.execute,
            "scannedFileCount": self.scanned_file_count,
            "changedFileCount": self.changed_file_count,
            "replacementCount": self.replacement_count,
            "replacementsByCountry": dict(sorted(self.replacements_by_country.items())),
            "replacementsByToken": dict(sorted(self.replacements_by_token.items())),
            "changedFiles": self.changed_files,
        }


def _dedupe_keywords(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        keyword = " ".join(str(value).strip().split())
        if not keyword:
            continue
        key = keyword.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(keyword)
    return tuple(result)


def build_keywords(country_code: str, powertrain: str) -> tuple[str, ...]:
    country_keywords = COUNTRY_KEYWORDS.get(country_code.lower(), {})
    return _dedupe_keywords(
        (
            *COMMON_KEYWORDS.get(powertrain, ()),
            *country_keywords.get(powertrain, ()),
        )
    )


def replace_placeholders(
    text: str,
    *,
    country_code: str,
) -> tuple[str, Counter[str]]:
    replacements: Counter[str] = Counter()
    output_lines: list[str] = []

    for line in text.splitlines(keepends=True):
        line_body = line[:-1] if line.endswith("\n") else line
        newline = "\n" if line.endswith("\n") else ""
        match = TOKEN_LINE_RE.match(line_body)
        if not match:
            output_lines.append(line)
            continue

        token = match.group("token")
        powertrain = POWERTRAIN_TOKENS[token]
        keywords = build_keywords(country_code, powertrain)
        indent = match.group("indent")
        output_lines.extend(f"{indent}- {keyword}{newline}" for keyword in keywords)
        replacements[token] += 1

    return "".join(output_lines), replacements


def _iter_country_files(
    source_draft_dir: Path,
    countries: tuple[str, ...],
) -> Iterable[tuple[str, Path]]:
    selected = {country.lower() for country in countries}
    for path in sorted(source_draft_dir.rglob("*.yaml")):
        try:
            relative = path.relative_to(source_draft_dir)
        except ValueError:
            continue
        parts = relative.parts
        if not parts or parts[0].startswith("_"):
            continue
        country = parts[0].lower()
        if selected and country not in selected:
            continue
        yield country, path


def fill_source_drafts(
    source_draft_dir: Path,
    *,
    countries: tuple[str, ...] = (),
    execute: bool = False,
) -> FillReport:
    report = FillReport(
        source_draft_dir=str(source_draft_dir),
        execute=execute,
    )

    for country, path in _iter_country_files(source_draft_dir, countries):
        report.scanned_file_count += 1
        original = path.read_text(encoding="utf-8")
        updated, replacements = replace_placeholders(
            original,
            country_code=country,
        )
        if not replacements:
            continue

        relative_path = str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)
        report.changed_file_count += 1
        report.changed_files.append(relative_path)
        replaced_count = sum(replacements.values())
        report.replacement_count += replaced_count
        report.replacements_by_country[country] += replaced_count
        report.replacements_by_token.update(replacements)

        if execute and updated != original:
            path.write_text(updated, encoding="utf-8")

    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-draft-dir",
        type=Path,
        default=DEFAULT_SOURCE_DRAFT_DIR,
        help="Root containing country source draft YAML folders.",
    )
    parser.add_argument(
        "--country",
        action="append",
        default=[],
        help="Limit to one country code; repeat for multiple countries.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write YAML changes; default is dry-run.",
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        help="Optional path for a JSON run report.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    source_draft_dir = args.source_draft_dir.resolve()
    report = fill_source_drafts(
        source_draft_dir,
        countries=tuple(args.country),
        execute=bool(args.execute),
    )
    payload = report.as_dict()

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
