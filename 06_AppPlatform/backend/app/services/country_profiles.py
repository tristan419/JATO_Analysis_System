"""Static country market profiles for the country chat assistant.

Each profile provides policy context, market hot-topics, and structural
characteristics that the LLM can reference when analysing chart data.
The dictionary is keyed by **all known aliases** for a country so that
look-ups from the parquet dataset's country column always succeed.
"""

from __future__ import annotations

from typing import Any

# ── profile data ────────────────────────────────────────────────────

_GERMANY: dict[str, Any] = {
    "market_label": "Germany / 德国",
    "market_size_hint": "~2.8M registrations/year, EU No.1",
    "key_policies": [
        "Umweltbonus (EV subsidy) ended abruptly Dec 2023 — BEV demand dropped sharply in 2024",
        "CO₂-based Kfz-Steuer makes high-emission ICE progressively expensive",
        "EU provisional anti-subsidy tariffs on Chinese-made EVs (Oct 2024, 17-36 %)",
        "Deutschlandticket (€49/month transit pass) may shift urban mobility away from cars",
    ],
    "hot_topics": [
        "VW announces 35k job cuts & 3 plant closures (Nov 2024) — structural overcapacity",
        "Chinese brands (BYD, MG, NIO) expanding dealer networks in Germany",
        "PHEV sales surge as buyers seek lower Kfz-Steuer without full-BEV range anxiety",
        "EU 2025 CO₂ fleet target (93.6 g/km) triggers OEM compliance pressure",
    ],
    "market_characteristics": [
        "Premium-heavy: BMW/Mercedes/Audi account for ~30 % of registrations",
        "SUV share >45 %, fastest-growing body type",
        "Corporate/fleet registrations ~65 % of total — policy-sensitive",
        "Diesel still ~18 % but declining YoY",
    ],
    "powertrain_context": "BEV ~18 % share (post-subsidy dip), PHEV ~7 %, HEV rising",
}

_FRANCE: dict[str, Any] = {
    "market_label": "France / 法国",
    "market_size_hint": "~1.8M registrations/year, EU No.2",
    "key_policies": [
        "Bonus écologique €4 000 for BEV <€47k (2024), tightened to EU-made only via eco-score",
        "Malus CO₂ starts at 118 g/km, up to €60k for worst emitters — strong ICE deterrent",
        "Leasing social (€100/month BEV lease for low-income households) launched Jan 2024",
        "EU anti-subsidy tariffs affect Chinese-made models (MG4, Dacia Spring)",
    ],
    "hot_topics": [
        "Stellantis dominates (~30 % share) but faces EV transition cost pressure",
        "Renault R5 E-Tech launch — affordable EU-made BEV targeting mass market",
        "Chinese BEV imports re-routed via non-EU plants to dodge tariffs",
        "Diesel share collapsed from 70 % (2012) to <15 % — fastest transition in EU",
    ],
    "market_characteristics": [
        "Small/medium cars (B/C segment) dominate — price-sensitive market",
        "Domestic brands (Renault, Peugeot, Citroën) hold ~50 % combined",
        "SUV share ~42 %, overtook sedans in 2020",
        "Strong fleet/corporate channel ~55 %",
    ],
    "powertrain_context": "BEV ~17 % share, PHEV ~8 %, HEV ~30 % (full + mild)",
}

_UK: dict[str, Any] = {
    "market_label": "UK / 英国",
    "market_size_hint": "~1.9M registrations/year, Europe No.2 (non-EU)",
    "key_policies": [
        "ZEV Mandate: 22 % of new car sales must be ZEV in 2024, rising to 80 % by 2030",
        "Plug-in car grant ended Jun 2022 — no direct BEV purchase subsidy",
        "Benefit-in-Kind (BiK) tax: BEV 2 % vs ICE up to 37 % — massive fleet incentive",
        "2030 ICE ban confirmed (hybrids allowed to 2035)",
    ],
    "hot_topics": [
        "OEMs face £15k fines per non-ZEV car if mandate shortfall — cross-subsidy pricing",
        "Tesla Model Y dominates BEV sales but Chinese brands (BYD, GWM) entering",
        "Used EV prices dropping fast — new BEV demand affected by residual-value anxiety",
        "Charging infrastructure gap outside London remains a barrier",
    ],
    "market_characteristics": [
        "Fleet/corporate registrations ~55 % — BiK drives BEV adoption",
        "SUV/crossover share ~47 %, highest in Europe",
        "Premium segment strong: BMW, Mercedes, Audi ~25 % combined",
        "Right-hand drive limits model availability vs continental Europe",
    ],
    "powertrain_context": "BEV ~20 % share (ZEV mandate driven), PHEV ~6 %, HEV ~12 %",
}

_ITALY: dict[str, Any] = {
    "market_label": "Italy / 意大利",
    "market_size_hint": "~1.6M registrations/year, EU No.4",
    "key_policies": [
        "Ecobonus: up to €5 000 for BEV <€35k with scrapping (2024 refresh, budget limited)",
        "Incentive fund depleted within hours of each re-opening — demand exceeds budget",
        "IPT (provincial registration tax) varies by region — adds €150-€600",
        "EU anti-subsidy tariffs hit popular models like MG4 and Dacia Spring",
    ],
    "hot_topics": [
        "Fiat 500e and Fiat Grande Panda — Stellantis bets on affordable Italian-made BEVs",
        "Italy's BEV share remains lowest among big-5 EU markets (<5 %)",
        "Government pressures Stellantis to increase Italian production volumes",
        "Chinese OEM (Chery/Dongfeng) considering Italian assembly partnerships",
    ],
    "market_characteristics": [
        "Small-car culture: A/B segment ~40 % of market",
        "Stellantis (Fiat/Alfa/Lancia/Jeep) holds ~30 % share",
        "LPG/CNG bi-fuel powertrain uniquely popular (~8 %)",
        "SUV share ~42 % but smaller crossovers dominate",
    ],
    "powertrain_context": "BEV ~4 % share (lowest big-5), HEV ~37 % (mild+full), diesel ~17 %",
}

_SPAIN: dict[str, Any] = {
    "market_label": "Spain / 西班牙",
    "market_size_hint": "~1.1M registrations/year, EU No.5",
    "key_policies": [
        "MOVES III plan: up to €7 000 BEV subsidy with scrapping (extended to 2024)",
        "Registration tax (IEDMT) based on CO₂ — 0 % for BEV, up to 14.75 % for high emitters",
        "Madrid/Barcelona low-emission zones restrict older ICE access",
        "Spain targets 5.5M EVs on road by 2030 (PNIEC plan)",
    ],
    "hot_topics": [
        "Spain became EU No.2 car producer — Volkswagen, SEAT/CUPRA, Stellantis plants",
        "CUPRA born/Tavascan EV production in Martorell — local BEV manufacturing push",
        "Fleet renewal accelerating: average vehicle age ~13 years (oldest in EU big-5)",
        "Chinese OEMs (Chery, BYD) evaluating Spanish assembly sites",
    ],
    "market_characteristics": [
        "SUV/crossover share ~50 %, fastest-growing body type",
        "SEAT/CUPRA strong domestic brand ~10 % share",
        "Rental/fleet channel ~40 % of market (tourism influence)",
        "Price-sensitive: average transaction price below EU average",
    ],
    "powertrain_context": "BEV ~5 % share, PHEV ~5 %, HEV ~28 %, diesel still ~22 %",
}

_NORWAY: dict[str, Any] = {
    "market_label": "Norway / 挪威",
    "market_size_hint": "~150k registrations/year, small but global BEV benchmark",
    "key_policies": [
        "No VAT (25 % saved), no purchase tax, no toll fees for BEVs — world's strongest incentives",
        "Free municipal parking and bus-lane access for BEVs (being rolled back in some cities)",
        "Weight tax introduced 2023 for cars >500 kg — hits heavy BEVs, benefits small cars",
        "2025 target: 100 % zero-emission new car sales (already ~90 % BEV in 2024)",
    ],
    "hot_topics": [
        "Tesla Model Y is Norway's best-selling car overall — not just BEV segment",
        "Chinese brands (BYD, NIO, XPeng) gained ~10 % combined share rapidly",
        "Used ICE cars near-worthless in resale — accelerating BEV transition",
        "Debate on scaling back BEV incentives as market matures",
    ],
    "market_characteristics": [
        "BEV share ~90 % — world's highest, near-total electrification",
        "Premium/large SUVs popular despite small overall market",
        "Tesla, Toyota, Volvo, BMW, VW are top-5 brands",
        "Cold climate makes real-world range a key purchase factor",
    ],
    "powertrain_context": "BEV ~90 %, PHEV ~5 %, ICE <5 % (near extinction)",
}

_SWEDEN: dict[str, Any] = {
    "market_label": "Sweden / 瑞典",
    "market_size_hint": "~290k registrations/year",
    "key_policies": [
        "Bonus-malus ended Nov 2024 — previously gave SEK 50k bonus for low-emission cars",
        "Company car tax (förmånsvärde) still favors BEV/PHEV — preserves fleet channel demand",
        "Road tax (fordonsskatt) based on CO₂ from 2025 — ICE penalty sharpens",
        "No congestion charge exemption for BEVs in Stockholm/Gothenburg",
    ],
    "hot_topics": [
        "Volvo Cars EX30 — affordable Swedish-designed BEV, produced in China (tariff risk)",
        "Post-bonus-malus BEV sales dip as private buyers wait or switch to HEV",
        "Polestar struggling with profitability despite Swedish roots",
        "Sweden's electricity grid well-positioned for EV charging (hydro/nuclear)",
    ],
    "market_characteristics": [
        "Volvo ~20 % market share — home-market advantage",
        "SUV/estate body types dominate (Nordic conditions)",
        "Corporate/fleet ~55 % of registrations",
        "PHEV historically popular (bonus-malus era), now shifting to full BEV",
    ],
    "powertrain_context": "BEV ~33 %, PHEV ~18 % (declining post-bonus), HEV ~20 %",
}

_FINLAND: dict[str, Any] = {
    "market_label": "Finland / 芬兰",
    "market_size_hint": (
        "~80k-90k registrations/year, Nordic but smaller-volume market"
    ),
    "key_policies": [
        (
            "No direct nationwide BEV purchase bonus since 2023 — "
            "EV demand relies more on tax treatment than cash subsidy"
        ),
        (
            "Company-car taxable value reduction still supports "
            "BEV/PHEV fleet uptake"
        ),
        (
            "Annual vehicle tax and registration economics remain "
            "CO₂-sensitive, pressuring higher-emission ICE"
        ),
        (
            "Cold-climate charging and winter-range practicality "
            "strongly influence EV adoption decisions"
        ),
    ],
    "hot_topics": [
        (
            "Toyota, Skoda and Volvo remain resilient as buyers prioritize "
            "reliability and winter usability"
        ),
        (
            "Tesla and broader BEV demand fluctuate with interest rates "
            "and household affordability pressure"
        ),
        (
            "Plug-in demand increasingly shifts toward practical "
            "crossover/SUV body styles"
        ),
        (
            "Used-car value retention and charging coverage outside major "
            "cities remain major purchase factors"
        ),
    ],
    "market_characteristics": [
        "Nordic market with strong fleet and company-car influence",
        (
            "SUV/crossover mix keeps rising, but practical wagon and "
            "hatchback formats remain relevant"
        ),
        (
            "Winter conditions elevate AWD, efficiency and heat-pump "
            "value propositions"
        ),
        (
            "Japanese and European brands have structural trust advantages "
            "over newer entrants"
        ),
    ],
    "powertrain_context": (
        "HEV remains structurally strong, BEV adoption is meaningful but "
        "more price-sensitive than Norway or Sweden"
    ),
}

_HUNGARY: dict[str, Any] = {
    "market_label": "Hungary / 匈牙利",
    "market_size_hint": "~120k registrations/year, CEE price-sensitive market",
    "key_policies": [
        (
            "No broad mass-market BEV subsidy is permanently available, so "
            "demand depends on targeted business schemes"
        ),
        (
            "Registration and ownership taxes remain favorable to lower-CO₂ "
            "vehicles versus large ICE models"
        ),
        (
            "Company-car and fleet economics matter more than private BEV "
            "cash incentives"
        ),
        (
            "Hungary's battery and EV manufacturing push keeps industrial "
            "policy attention high"
        ),
    ],
    "hot_topics": [
        "BYD, CATL and other battery projects keep Hungary central in EU EV supply chains",
        "Suzuki and Skoda stay strong as value-oriented buyers resist rapid price inflation",
        "BEV uptake remains urban and fleet-led rather than broad household adoption",
        "Chinese entrants gain attention, but affordability still decides conversion",
    ],
    "market_characteristics": [
        "Highly price-sensitive market with strong small and compact-car mix",
        "Fleet channel matters, but private affordability remains the core constraint",
        "Used imports influence residual values and slow premium EV penetration",
        "ICE and HEV still dominate outside Budapest and major cities",
    ],
    "powertrain_context": (
        "ICE and HEV dominate; BEV is growing from a low base and remains "
        "price-sensitive"
    ),
}

_CZECH_REPUBLIC: dict[str, Any] = {
    "market_label": "Czech Republic / 捷克",
    "market_size_hint": "~220k registrations/year, fleet-heavy Skoda home market",
    "key_policies": [
        (
            "No large private-car BEV subsidy base, so fleet taxation and "
            "TCO drive electrification decisions"
        ),
        (
            "EU CO₂ compliance pressure shapes OEM mix even without strong "
            "local purchase grants"
        ),
        (
            "Charging deployment is improving on main corridors but remains "
            "less dense than Nordics"
        ),
        (
            "Industrial policy strongly favors domestic auto production and "
            "supplier competitiveness"
        ),
    ],
    "hot_topics": [
        "Skoda Enyaq and Elroq shape local EV visibility more than imported startups",
        "Corporate fleets lead EV adoption while households remain value-focused",
        "SUV demand keeps rising, but compact cars still anchor market volume",
        "Chinese brands are watched closely, yet local brand trust remains a moat",
    ],
    "market_characteristics": [
        "Skoda has structural home-market strength across private and fleet channels",
        "Company fleets account for a large share of new registrations",
        "Average transaction price is below western Europe, constraining BEV mix",
        "ICE, MHEV and HEV remain the practical mainstream",
    ],
    "powertrain_context": (
        "BEV penetration is still modest; HEV and efficient ICE are more "
        "mainstream in current demand"
    ),
}

_SLOVAKIA: dict[str, Any] = {
    "market_label": "Slovakia / 斯洛伐克",
    "market_size_hint": "~95k registrations/year, small market with major auto production base",
    "key_policies": [
        (
            "Electrification is shaped more by EU fleet rules and company-car "
            "economics than by broad consumer grants"
        ),
        (
            "Lower-emission vehicles benefit from tax logic relative to "
            "high-output ICE models"
        ),
        (
            "Charging availability is improving on corridors, but local density "
            "still limits mass BEV adoption"
        ),
        (
            "Industrial policy is closely tied to VW, Kia and JLR production "
            "footprints"
        ),
    ],
    "hot_topics": [
        "Production footprint matters more than pure consumer incentives in market narratives",
        "Fleet renewal and TCO logic lead electrification ahead of private demand",
        "SUV preference is rising, but compact affordability remains decisive",
        "Chinese EV attention is growing, but dealer depth is still limited",
    ],
    "market_characteristics": [
        "Small market with strong linkage to export-oriented auto manufacturing",
        "Fleet demand is more important than spontaneous private EV uptake",
        "Value sensitivity remains high outside major urban centers",
        "ICE and HEV dominate while BEV stays concentrated in specific niches",
    ],
    "powertrain_context": (
        "BEV is still niche; HEV and efficient ICE remain the broad-market "
        "volume drivers"
    ),
}

_CROATIA: dict[str, Any] = {
    "market_label": "Croatia / 克罗地亚",
    "market_size_hint": "~65k registrations/year, tourism and used-import influenced market",
    "key_policies": [
        (
            "Occasional low-emission purchase schemes can move BEV demand, "
            "but timing and budget visibility matter"
        ),
        (
            "Registration and ownership costs still favor smaller, more "
            "efficient vehicles"
        ),
        (
            "Charging coverage is improving along tourism corridors faster "
            "than inland density"
        ),
        (
            "EU emissions policy shapes brand mix even when local grants are limited"
        ),
    ],
    "hot_topics": [
        "Rental and tourism fleets influence new-car mix and seasonality",
        "Affordable crossovers and compact SUVs keep taking share from classic sedans",
        "Chinese brands get attention mainly when they undercut mainstream pricing",
        "Used imported ICE vehicles still weigh on new EV conversion speed",
    ],
    "market_characteristics": [
        "Smaller-volume market with strong seasonality and rental exposure",
        "Affordability and financing availability dominate household decisions",
        "Imported used cars cap how fast new-car electrification can scale",
        "SUV demand is growing, but low-cost ICE still anchors volume",
    ],
    "powertrain_context": (
        "ICE remains dominant; HEV is the most practical electrified step and "
        "BEV adoption is early-stage"
    ),
}

_SLOVENIA: dict[str, Any] = {
    "market_label": "Slovenia / 斯洛文尼亚",
    "market_size_hint": "~55k registrations/year, small affluent corridor market",
    "key_policies": [
        (
            "Low-emission tax treatment and fleet logic matter more than a "
            "single headline subsidy"
        ),
        (
            "Cross-border price comparison with nearby EU markets affects "
            "consumer timing and brand choice"
        ),
        (
            "Charging along transit corridors supports EV practicality better "
            "than many CEE peers"
        ),
        (
            "EU fleet compliance pressures influence local model availability and mix"
        ),
    ],
    "hot_topics": [
        "Premium compact SUVs and crossovers benefit from higher-income buyers",
        "Tesla, VW Group and Hyundai-Kia stay visible in EV consideration sets",
        "Households compare offers across nearby Austria, Italy and Croatia",
        "Plug-in demand is more rational-TCO driven than purely trend driven",
    ],
    "market_characteristics": [
        "Small but relatively affluent market with cross-border shopping behavior",
        "Fleet and business registrations materially influence new-car demand",
        "SUV/crossover demand keeps rising across compact and midsize classes",
        "Electrified powertrains gain share faster than in lower-income CEE markets",
    ],
    "powertrain_context": (
        "HEV and PHEV are practical bridges; BEV adoption is meaningful but "
        "still selective"
    ),
}

_AUSTRIA: dict[str, Any] = {
    "market_label": "Austria / 奥地利",
    "market_size_hint": "~240k registrations/year, affluent DACH market",
    "key_policies": [
        (
            "NoVA registration tax strongly penalizes higher-CO₂ ICE vehicles "
            "and favors low-emission drivetrains"
        ),
        (
            "Company-car tax treatment remains a powerful BEV/PHEV adoption lever"
        ),
        (
            "Public charging quality is relatively solid, improving EV usability outside Vienna"
        ),
        (
            "Austria follows wider EU tariff and industrial policy debates around Chinese EVs"
        ),
    ],
    "hot_topics": [
        "Tesla, VW Group and BMW compete hard in the premium-leaning EV space",
        "PHEV remains relevant where buyers want alpine usability without full range anxiety",
        "SUV demand stays high, but tax pressure pushes buyers toward lower-emission trims",
        "Chinese brands gain visibility but still face trust and resale-value questions",
    ],
    "market_characteristics": [
        "Higher-income market with strong premium and company-car demand",
        "DACH media and pricing dynamics influence buyer expectations directly",
        "SUV/crossover formats dominate while wagons remain relevant",
        "Electrified powertrains have better structural support than in CEE markets",
    ],
    "powertrain_context": (
        "BEV and PHEV are materially established, though HEV and efficient ICE "
        "still retain broad volume"
    ),
}

_SWITZERLAND: dict[str, Any] = {
    "market_label": "Switzerland / 瑞士",
    "market_size_hint": "~250k registrations/year, high-income premium-heavy market",
    "key_policies": [
        (
            "No single federal BEV subsidy dominates; cantonal tax treatment and "
            "ownership economics vary by region"
        ),
        (
            "High purchasing power supports premium EV uptake even without large cash grants"
        ),
        (
            "CO₂ and fleet pressure still shape OEM mix despite Switzerland being outside the EU"
        ),
        (
            "Winter usability, AWD demand and charging convenience remain central purchase filters"
        ),
    ],
    "hot_topics": [
        "Premium SUVs from BMW, Mercedes, Audi, Tesla and Volvo stay especially competitive",
        "Chinese entrants are watched, but premium-brand loyalty remains strong",
        "PHEV still appeals where alpine driving and long-distance flexibility matter",
        "Residual value, leasing and fleet packages influence EV conversion speed",
    ],
    "market_characteristics": [
        "High-income market with strong premium-brand concentration",
        "SUVs dominate, with AWD and winter practicality carrying real weight",
        "Fleet and leasing channels are influential in premium segments",
        "EV adoption is stronger than in CEE, but still shaped by pragmatic use cases",
    ],
    "powertrain_context": (
        "BEV and PHEV are both relevant; premium buyers still expect range, AWD "
        "and charging convenience"
    ),
}

_ROMANIA: dict[str, Any] = {
    "market_label": "Romania / 罗马尼亚",
    "market_size_hint": "~145k registrations/year, fast-growing but highly price-sensitive",
    "key_policies": [
        (
            "Rabla Plus style incentives have historically been among the region's "
            "strongest EV demand triggers"
        ),
        (
            "Policy continuity matters because subsidy changes can sharply distort timing"
        ),
        (
            "Local taxation still favors smaller, lower-emission vehicles over larger ICE"
        ),
        (
            "Charging rollout is improving, but national density remains uneven outside key cities"
        ),
    ],
    "hot_topics": [
        "Dacia Spring keeps Romania central in affordable EV discussions",
        "Local Dacia/Renault brand strength makes value positioning unusually important",
        "Used imported vehicles remain a major competitive pressure on new-car pricing",
        "SUV demand rises, but affordability ceilings remain low compared with western Europe",
    ],
    "market_characteristics": [
        "Rapidly developing market with strong domestic-brand relevance",
        "Household affordability and subsidy timing drive sharp demand swings",
        "Used-car imports materially affect residual values and new EV adoption",
        "Compact SUVs and value-led models dominate mainstream demand",
    ],
    "powertrain_context": (
        "ICE still leads; HEV grows steadily while BEV can spike when incentives "
        "are generous and visible"
    ),
}

_GREECE: dict[str, Any] = {
    "market_label": "Greece / 希腊",
    "market_size_hint": "~140k registrations/year, recovery market with fleet and tourism influence",
    "key_policies": [
        (
            "Kinoumai Ilektrika-style subsidy programs remain important for BEV and charging decisions"
        ),
        (
            "Company-car and low-emission tax treatment help electrification in urban fleets"
        ),
        (
            "Fuel costs and city-use economics increase interest in HEV and compact EV formats"
        ),
        (
            "Charging buildout is improving but still uneven outside Athens and major islands"
        ),
    ],
    "hot_topics": [
        "Toyota and Hyundai-Kia benefit from strong HEV value propositions",
        "Tourism-related fleets increasingly test electrified compact SUVs",
        "Chinese brands need pricing strength to overcome network and trust gaps",
        "Urban congestion and energy costs support practical electrification over performance-led demand",
    ],
    "market_characteristics": [
        "Price-sensitive market with visible fleet and tourism channel effects",
        "Compact cars and compact SUVs remain the center of volume",
        "HEV practicality resonates more broadly than premium long-range BEV positioning",
        "Urban demand leads the transition while rural charging remains more limited",
    ],
    "powertrain_context": (
        "HEV is the broadest bridge technology; BEV grows selectively where subsidies "
        "and charging are credible"
    ),
}

_DENMARK: dict[str, Any] = {
    "market_label": "Denmark / 丹麦",
    "market_size_hint": "~170k registrations/year, advanced Nordic electrification market",
    "key_policies": [
        (
            "Registration-tax design strongly favors low-emission vehicles and helps "
            "BEV competitiveness"
        ),
        (
            "Company-car and ownership economics remain supportive for EV adoption"
        ),
        (
            "Charging infrastructure is relatively mature, especially across main population corridors"
        ),
        (
            "Danish buyers are sensitive to tax-step changes that alter BEV versus ICE parity"
        ),
    ],
    "hot_topics": [
        "Tesla, VW Group, Skoda and Volvo compete hard in electric crossover volume",
        "Private buyers increasingly accept BEV as mainstream rather than niche",
        "PHEV relevance declines as full-BEV practicality improves",
        "Chinese brands gain attention but must prove resale strength and service depth",
    ],
    "market_characteristics": [
        "Nordic market with relatively high EV readiness and digital buying behavior",
        "Crossovers and compact SUVs dominate mainstream demand",
        "Tax policy has outsized influence on monthly mix and ordering timing",
        "Premium and mainstream EVs both have credible demand pools",
    ],
    "powertrain_context": (
        "BEV has moved into the mainstream; HEV matters, while PHEV is gradually "
        "losing strategic importance"
    ),
}


# ── public API ──────────────────────────────────────────────────────

# Keys include common aliases so look-ups from JATO country columns
# (English name, local name, ISO codes) all resolve.

COUNTRY_PROFILES: dict[str, dict[str, Any]] = {}

_ALIAS_MAP: list[tuple[list[str], dict[str, Any]]] = [
    (["germany", "deutschland", "德国", "de", "deu"], _GERMANY),
    (["france", "法国", "fr", "fra"], _FRANCE),
    (["uk", "united kingdom", "great britain", "英国", "gb", "gbr"], _UK),
    (["italy", "italia", "意大利", "it", "ita"], _ITALY),
    (["spain", "españa", "espana", "西班牙", "es", "esp"], _SPAIN),
    (["norway", "norge", "挪威", "no", "nor"], _NORWAY),
    (["sweden", "sverige", "瑞典", "se", "swe"], _SWEDEN),
    (["finland", "suomi", "芬兰", "fi", "fin"], _FINLAND),
    (["hungary", "magyarorszag", "magyarország", "匈牙利", "hu", "hun"], _HUNGARY),
    (["czech republic", "czechia", "cesko", "česko", "捷克", "cz", "cze"], _CZECH_REPUBLIC),
    (["slovakia", "slovensko", "斯洛伐克", "sk", "svk"], _SLOVAKIA),
    (["croatia", "hrvatska", "克罗地亚", "hr", "hrv"], _CROATIA),
    (["slovenia", "slovenija", "斯洛文尼亚", "si", "svn"], _SLOVENIA),
    (["austria", "osterreich", "österreich", "奥地利", "at", "aut"], _AUSTRIA),
    (["switzerland", "schweiz", "suisse", "svizzera", "瑞士", "ch", "che"], _SWITZERLAND),
    (["romania", "românia", "罗马尼亚", "ro", "rou"], _ROMANIA),
    (["greece", "ellada", "希腊", "gr", "grc"], _GREECE),
    (["denmark", "danmark", "丹麦", "dk", "dnk"], _DENMARK),
]

for _aliases, _profile in _ALIAS_MAP:
    for _alias in _aliases:
        COUNTRY_PROFILES[_alias.lower()] = _profile


def get_country_profile(country: str) -> dict[str, Any] | None:
    """Return the static market profile for *country*, or ``None``."""
    return COUNTRY_PROFILES.get(country.strip().lower())


def get_compact_profile(country: str) -> dict[str, Any] | None:
    """Return a token-efficient slice (policies + topics only)."""
    profile = get_country_profile(country)
    if profile is None:
        return None
    return {
        "market_label": profile["market_label"],
        "key_policies": profile["key_policies"],
        "hot_topics": profile["hot_topics"],
        "powertrain_context": profile["powertrain_context"],
    }
