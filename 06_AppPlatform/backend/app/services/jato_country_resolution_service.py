from __future__ import annotations

import re


COUNTRY_MENTION_ALIASES: tuple[tuple[str, str], ...] = (
    ("Czech Republic", "Czech Republic"),
    ("United Kingdom", "UK"),
    ("Great Britain", "UK"),
    ("Magyarország", "Hungary"),
    ("Magyarorszag", "Hungary"),
    ("Sweden", "Sweden"),
    ("Swedish", "Sweden"),
    ("Sverige", "Sweden"),
    ("瑞典", "Sweden"),
    ("Hungary", "Hungary"),
    ("Hungarian", "Hungary"),
    ("匈牙利", "Hungary"),
    ("Germany", "Germany"),
    ("German", "Germany"),
    ("Deutschland", "Germany"),
    ("德国", "Germany"),
    ("Norway", "Norway"),
    ("Norwegian", "Norway"),
    ("Norge", "Norway"),
    ("挪威", "Norway"),
    ("Finland", "Finland"),
    ("Finnish", "Finland"),
    ("Suomi", "Finland"),
    ("芬兰", "Finland"),
    ("Denmark", "Denmark"),
    ("Danish", "Denmark"),
    ("Danmark", "Denmark"),
    ("丹麦", "Denmark"),
    ("Austria", "Austria"),
    ("Österreich", "Austria"),
    ("Osterreich", "Austria"),
    ("奥地利", "Austria"),
    ("Croatia", "Croatia"),
    ("Hrvatska", "Croatia"),
    ("克罗地亚", "Croatia"),
    ("Czechia", "Czech Republic"),
    ("Czech", "Czech Republic"),
    ("Česko", "Czech Republic"),
    ("Cesko", "Czech Republic"),
    ("捷克", "Czech Republic"),
    ("Slovakia", "Slovakia"),
    ("Slovensko", "Slovakia"),
    ("斯洛伐克", "Slovakia"),
    ("Slovenia", "Slovenia"),
    ("Slovenija", "Slovenia"),
    ("斯洛文尼亚", "Slovenia"),
    ("Spain", "Spain"),
    ("Spanish", "Spain"),
    ("España", "Spain"),
    ("Espana", "Spain"),
    ("西班牙", "Spain"),
    ("France", "France"),
    ("法国", "France"),
    ("Italy", "Italy"),
    ("Italia", "Italy"),
    ("意大利", "Italy"),
    ("Netherlands", "Netherlands"),
    ("Dutch", "Netherlands"),
    ("荷兰", "Netherlands"),
    ("Belgium", "Belgium"),
    ("Belgian", "Belgium"),
    ("比利时", "Belgium"),
    ("Switzerland", "Switzerland"),
    ("Schweiz", "Switzerland"),
    ("Suisse", "Switzerland"),
    ("瑞士", "Switzerland"),
    ("Romania", "Romania"),
    ("România", "Romania"),
    ("罗马尼亚", "Romania"),
    ("Greece", "Greece"),
    ("希腊", "Greece"),
)

COUNTRY_CODE_ALIASES: dict[str, str] = {
    "SE": "Sweden",
    "SWE": "Sweden",
    "HU": "Hungary",
    "HUN": "Hungary",
    "DE": "Germany",
    "DEU": "Germany",
    "NO": "Norway",
    "NOR": "Norway",
    "FI": "Finland",
    "FIN": "Finland",
    "DK": "Denmark",
    "DNK": "Denmark",
    "AT": "Austria",
    "AUT": "Austria",
    "CZ": "Czech Republic",
    "CZE": "Czech Republic",
    "SK": "Slovakia",
    "SVK": "Slovakia",
    "HR": "Croatia",
    "HRV": "Croatia",
    "SI": "Slovenia",
    "SVN": "Slovenia",
    "ES": "Spain",
    "ESP": "Spain",
    "FR": "France",
    "FRA": "France",
    "IT": "Italy",
    "ITA": "Italy",
    "NL": "Netherlands",
    "NLD": "Netherlands",
    "BE": "Belgium",
    "BEL": "Belgium",
    "CH": "Switzerland",
    "CHE": "Switzerland",
    "RO": "Romania",
    "ROU": "Romania",
    "GR": "Greece",
    "GRC": "Greece",
    "UK": "UK",
    "GB": "UK",
    "GBR": "UK",
}


def resolve_effective_country(requested_country: str, question: str) -> str:
    """Prefer an explicit country mention in the user question over UI defaults."""
    explicit_country = extract_country_mention(question)
    if explicit_country:
        return explicit_country
    normalized_requested = canonical_country(str(requested_country or "").strip())
    return normalized_requested


def extract_country_mention(question: str) -> str:
    text = str(question or "")
    lower_text = text.lower()
    candidates: list[tuple[int, bool, str]] = []
    for alias, country in COUNTRY_MENTION_ALIASES:
        alias_text = alias.strip()
        if not alias_text:
            continue
        position = _alias_position(alias_text, text, lower_text)
        if position >= 0:
            candidates.append((position, _is_negated_country_mention(text, position), country))
    for code, country in COUNTRY_CODE_ALIASES.items():
        match = re.search(rf"(?<![A-Za-z]){re.escape(code)}(?![A-Za-z])", text, flags=re.IGNORECASE)
        if match:
            candidates.append((match.start(), _is_negated_country_mention(text, match.start()), country))
    if not candidates:
        return ""
    positive_candidates = [candidate for candidate in candidates if not candidate[1]]
    if not positive_candidates:
        return ""
    ordered = sorted(positive_candidates, key=lambda item: item[0])
    return ordered[0][2]


def canonical_country(value: str) -> str:
    if not value:
        return ""
    lowered = value.lower()
    for alias, country in COUNTRY_MENTION_ALIASES:
        if lowered == alias.lower():
            return country
    return COUNTRY_CODE_ALIASES.get(value.upper(), value)


def _alias_position(alias: str, text: str, lower_text: str) -> int:
    if any("\u4e00" <= char <= "\u9fff" for char in alias):
        return text.find(alias)
    if len(alias) <= 3:
        match = re.search(rf"(?<![A-Za-z]){re.escape(alias)}(?![A-Za-z])", text, flags=re.IGNORECASE)
        return match.start() if match else -1
    return lower_text.find(alias.lower())


def _is_negated_country_mention(text: str, start: int) -> bool:
    prefix = text[max(0, start - 24):start].casefold()
    prefix = re.split(r"[，,。.;；!！?？]", prefix)[-1]
    negation_markers = (
        "不要回答",
        "别回答",
        "不是",
        "不要用",
        "别用",
        "not ",
        "not-",
        "do not",
        "don't",
        "dont",
        "not about",
    )
    return any(marker in prefix for marker in negation_markers)
