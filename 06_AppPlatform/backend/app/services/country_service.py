from __future__ import annotations


JATO_BASELINE_COUNTRIES: tuple[str, ...] = (
    "Germany",
    "France",
    "Italy",
    "Spain",
    "Sweden",
    "Norway",
    "Denmark",
    "Finland",
    "Austria",
    "Switzerland",
    "Netherlands",
    "Belgium",
    "Poland",
    "Czechia",
    "Hungary",
    "Croatia",
    "Slovenia",
    "Romania",
    "Slovakia",
    "Greece",
    "Portugal",
)

JATO_BASELINE_COUNTRY_COUNT = len(JATO_BASELINE_COUNTRIES)


COUNTRY_ALIAS_GROUPS: dict[str, set[str]] = {
    "Sweden": {"sweden", "se", "瑞典"},
    "Finland": {"finland", "fi", "芬兰"},
    "Norway": {"norway", "no", "挪威"},
    "Denmark": {"denmark", "dk", "丹麦"},
    "Germany": {"germany", "de", "德国"},
    "France": {"france", "fr", "法国"},
    "Italy": {"italy", "it", "意大利"},
    "Spain": {"spain", "es", "西班牙"},
    "Portugal": {"portugal", "pt", "葡萄牙"},
    "Netherlands": {"netherlands", "nl", "荷兰"},
    "Belgium": {"belgium", "be", "比利时"},
    "Austria": {"austria", "at", "奥地利"},
    "Switzerland": {"switzerland", "ch", "瑞士"},
    "Poland": {"poland", "pl", "波兰"},
    "Czechia": {"czechia", "czech republic", "cz", "捷克"},
    "Hungary": {"hungary", "hu", "匈牙利"},
    "Romania": {"romania", "ro", "罗马尼亚"},
    "Greece": {"greece", "gr", "希腊"},
    "Slovakia": {"slovakia", "sk", "斯洛伐克"},
    "Slovenia": {"slovenia", "si", "斯洛文尼亚"},
    "Croatia": {"croatia", "hr", "克罗地亚"},
    "United States": {"united states", "unitedstates", "usa", "us", "美国"},
    "United Kingdom": {"united kingdom", "uk", "gb", "英国"},
    "Ireland": {"ireland", "ie", "爱尔兰"},
    "Canada": {"canada", "ca", "加拿大"},
    "Australia": {"australia", "au", "澳大利亚"},
    "China": {"china", "cn", "中国"},
}


def _normalize(text: str) -> str:
    return str(text or "").strip().lower()


def to_display_country(country: str) -> str:
    normalized = _normalize(country)
    for canonical, aliases in COUNTRY_ALIAS_GROUPS.items():
        if normalized == _normalize(canonical) or normalized in aliases:
            return canonical
    return str(country or "").strip()


def country_filter_aliases(query: str) -> set[str]:
    normalized = _normalize(query)
    if not normalized:
        return set()

    matched: set[str] = set()
    exact_only = len(normalized) <= 2
    for canonical, aliases in COUNTRY_ALIAS_GROUPS.items():
        all_names = {
            _normalize(canonical),
            *{_normalize(alias) for alias in aliases},
        }
        if exact_only:
            is_match = normalized in all_names
        else:
            is_match = any(
                normalized == name
                or name.startswith(normalized)
                or normalized in name
                for name in all_names
            )
        if is_match:
            matched.update(all_names)

    if matched:
        return matched
    return {normalized}
