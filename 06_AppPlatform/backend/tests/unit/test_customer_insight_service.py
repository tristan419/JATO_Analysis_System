import json
from pathlib import Path

import pandas as pd

from app.services import customer_insight_service


def metric_value(payload: dict, label: str) -> float:
    return next(
        float(item["value"])
        for item in payload["page"]["metrics"]
        if item["label"] == label
    )


def test_read_excel_with_fallback_prefers_calamine_then_default(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_read_excel(source_file: object, *, sheet_name: str, engine: str | None = None) -> pd.DataFrame:
        calls.append({
            "source_file": source_file,
            "sheet_name": sheet_name,
            "engine": engine,
        })
        if engine == "calamine":
            raise ImportError("calamine unavailable")
        return pd.DataFrame({"A": [1]})

    monkeypatch.setattr(customer_insight_service.pd, "read_excel", fake_read_excel)

    frame = customer_insight_service._read_excel_with_fallback(
        "dummy.xlsx",
        sheet_name="VOC Data",
    )

    assert list(frame.columns) == ["A"]
    assert calls == [
        {"source_file": "dummy.xlsx", "sheet_name": "VOC Data", "engine": "calamine"},
        {"source_file": "dummy.xlsx", "sheet_name": "VOC Data", "engine": None},
    ]


def test_estimate_weekly_commute_bucket_prefers_explicit_daily_distance() -> None:
    row = pd.Series(
        {
            "Daily Life Pattern": "Commutes 63 km/day by car; evenings family-focused",
            "Driving Scenarios": "Daily urban commute + school run; Mixed city + rural daily use",
            "Usage Frequency": "Every day",
        }
    )

    assert customer_insight_service._estimate_weekly_commute_bucket(row) == "300-500 km/周"


def test_build_factor_items_maps_keywords_to_core_categories() -> None:
    frame = pd.DataFrame(
        {
            "Spending Philosophy": ["Total cost of ownership focus", "Environmental footprint matters most"],
            "Why This Car?": [
                "Subsidy + PHEV fuel savings made the TCO calculation compelling",
                "Top Euro NCAP safety rating – non-negotiable with kids in the car",
            ],
            "Future Car Requirements": [
                "Ultra-fast DC charging (150 kW+)",
                "Integrated trailer management system",
            ],
            "Customer Requirements": [
                "Asks for home charging installation to be bundled with car deal",
                "Requests local-language (Swedish/Finnish) dealer support throughout purchase",
            ],
            "Price Perception": [
                "Slightly above budget – subsidy was essential",
                "Expensive; justified only by brand reputation",
            ],
            "Suggestions": [
                "OTA reliability must improve.",
                "Dealer communication must improve.",
            ],
            "Top 3 Favourite Features": [
                "1. Plug-in charging (PHEV/BEV)  2. Adaptive cruise control / Pilot Assist  3. Third-row seating",
                "1. All-wheel drive (AWD)  2. Hands-free tailgate  3. Over-the-air (OTA) software updates",
            ],
            "Evaluation": [
                "Good value given the Nordic subsidy. Would not pay full price without it.",
                "Winter performance surprised me positively; range loss was manageable.",
            ],
        }
    )

    items = customer_insight_service._build_factor_items(frame)
    labels = {item["label"] for item in items}

    assert "价格 / TCO / 补贴" in labels
    assert "续航 / 充电便利" in labels
    assert "智能化 / OTA / ADAS" in labels
    assert "安全 / 冬季能力" in labels
    assert "拖挂 / 空间实用" in labels
    assert "服务 / 沟通体验" in labels


def test_query_nordic_customer_deck_returns_customer_page_shape(monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "Age": [34, 42, 49],
            "Gender": ["Male", "Female", "Male"],
            "Marital Status": ["Married", "Cohabiting", "Single"],
            "Household Size": [4, 5, 2],
            "Children": ["2 children", "1 child", "No children"],
            "Occupation / Industry": [
                "Software Engineer / Tech",
                "Teacher / Education",
                "Police Officer / Public Service",
            ],
            "Sports / Hobbies": [
                "Hunting; Photography",
                "Swimming; Gardening",
                "Trail running; Hiking / fell walking",
            ],
            "Frequent Locations": [
                "Forest trails; Ferry terminal",
                "Children's school & activities; Sports hall",
                "City centre; Summer cottage",
            ],
            "Spending Philosophy": [
                "Total cost of ownership focus",
                "Environmental footprint matters most",
                "Brand prestige is part of the purchase",
            ],
            "Daily Life Pattern": [
                "Commutes 63 km/day by car; evenings family-focused",
                "Works from home 3 days/week; car used mainly for weekend trips",
                "Semi-rural lifestyle; car is primary and often only transport option",
            ],
            "Why This Car?": [
                "Subsidy + PHEV fuel savings made the TCO calculation compelling",
                "Top Euro NCAP safety rating – non-negotiable with kids in the car",
                "Test drive sealed it – the ride quality on gravel roads was noticeably better",
            ],
            "Driving Scenarios": [
                "Daily motorway commute (60–120 km/day); Weekend ski trips to the mountains",
                "Towing a boat or caravan; Coastal summer holiday touring",
                "Mixed city + rural daily use; Business travel between cities",
            ],
            "Future Car Requirements": [
                "Ultra-fast DC charging (150 kW+)",
                "Integrated trailer management system",
                "Over-the-air updates with rollback option",
            ],
            "Usage Frequency": ["Every day", "Varies seasonally", "5 days/week"],
            "Powertrain Preference": [
                "PHEV (daily EV, petrol on long runs)",
                "Full BEV (500 km+ real-world range)",
                "Open to any if TCO makes sense",
            ],
            "Top 3 Favourite Features": [
                "1. Plug-in charging (PHEV/BEV)  2. Adaptive cruise control / Pilot Assist  3. Third-row seating",
                "1. All-wheel drive (AWD)  2. Hands-free tailgate  3. Over-the-air (OTA) software updates",
                "1. Trailer hitch (tow package)  2. HUD (Head-Up Display)  3. Panoramic sunroof",
            ],
            "Customer Requirements": [
                "Asks for home charging installation to be bundled with car deal",
                "Requests local-language (Swedish/Finnish) dealer support throughout purchase",
                "Requires clear breakdown of PHEV/BEV government subsidy eligibility",
            ],
            "Information Source": [
                "Recommendation from colleague",
                "Dealership test drive event",
                "Manufacturer website",
            ],
            "Price Perception": [
                "Slightly above budget – subsidy was essential",
                "Expensive; justified only by brand reputation",
                "Good value given total cost of ownership",
            ],
            "Suggestions": [
                "OTA reliability must improve.",
                "Dealer communication must improve.",
                "Bundle winter tyre pricing upfront.",
            ],
            "Evaluation": [
                "Good value given the Nordic subsidy. Would not pay full price without it.",
                "Winter performance surprised me positively; range loss was manageable.",
                "Solid choice for the family. Would consider full EV next time.",
            ],
        }
    )

    monkeypatch.setattr(customer_insight_service, "_load_voc_frame", lambda: frame.copy())

    payload = customer_insight_service.query_nordic_customer_deck()

    assert payload["metadata"]["respondentCount"] == 113
    assert payload["page"]["title"] == "看客户"
    assert payload["page"]["methodologyNote"]
    assert len(payload["page"]["metrics"]) == 4
    assert len(payload["page"]["conclusionCards"]) == 4
    assert payload["page"]["profile"]["sampleSources"]
    assert payload["page"]["profile"]["attentionChannels"]
    assert payload["page"]["profile"]["gender"]
    assert payload["page"]["powertrain"]["items"]
    assert payload["page"]["purchaseUses"]["items"]
    assert payload["page"]["decisionFactors"]["items"]
    assert payload["page"]["persona"]["title"] == "典型北欧家庭转电用户"
    assert round(sum(item["sharePct"] for item in payload["page"]["profile"]["sampleSources"]), 4) == 1
    assert round(sum(item["sharePct"] for item in payload["page"]["profile"]["attentionChannels"]), 4) == 1
    assert payload["page"]["profile"]["sampleSources"][0]["label"] == "Bilnytt.se"
    assert payload["page"]["profile"]["attentionChannels"][0]["label"] == "品牌官网"
    assert payload["metadata"]["mode"] == "benchmark"


def test_query_nordic_customer_deck_forum_live_aggregates_available_country_decks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def write_deck(country_code: str, payload: dict) -> None:
        deck_path = tmp_path / country_code.lower() / "deck" / "customer_insight_deck.json"
        deck_path.parent.mkdir(parents=True, exist_ok=True)
        deck_path.write_text(json.dumps(payload), encoding="utf-8")

    def write_enriched(country_code: str, payload: dict) -> None:
        enriched_path = tmp_path / country_code.lower() / "enriched" / "customer_insight_signals.json"
        enriched_path.parent.mkdir(parents=True, exist_ok=True)
        enriched_path.write_text(json.dumps(payload), encoding="utf-8")

    write_deck(
        "SE",
        {
            "countryCode": "SE",
            "countryLabel": "Sweden / 瑞典",
            "generatedAt": "2026-04-19T08:00:00+00:00",
            "metrics": [
                {"label": "Sources", "value": 3},
                {"label": "Documents", "value": 4},
                {"label": "Publish-ready docs", "value": 3},
                {"label": "Signal observations", "value": 2},
                {"label": "Avg quality score", "value": 6.5},
            ],
            "observedSections": ["source_mix", "pain_points", "evidence_cards"],
            "inferredSections": ["demographics"],
            "sourceMix": [
                {"label": "Tesla Club Sweden", "rawLabel": "Tesla Club Sweden", "value": 2, "sharePct": 0.5},
                {"label": "Vi Bilagare", "rawLabel": "Vi Bilagare", "value": 2, "sharePct": 0.5},
            ],
            "siteTypes": [
                {"label": "forum", "rawLabel": "forum", "value": 2, "sharePct": 0.5},
                {"label": "media_comments", "rawLabel": "media_comments", "value": 2, "sharePct": 0.5},
            ],
            "languages": [
                {"label": "sv", "rawLabel": "sv", "value": 4, "sharePct": 1.0},
            ],
            "publishTiers": [
                {"label": "high", "rawLabel": "high", "value": 3, "sharePct": 0.75},
                {"label": "medium", "rawLabel": "medium", "value": 1, "sharePct": 0.25},
            ],
            "sentiment": [
                {"label": "neutral", "rawLabel": "neutral", "value": 3, "sharePct": 0.75},
                {"label": "negative", "rawLabel": "negative", "value": 1, "sharePct": 0.25},
            ],
            "ownershipStages": [
                {"label": "Research / consideration", "rawLabel": "research_consideration", "value": 1, "sharePct": 0.25, "mentionCount": 1},
            ],
            "painPoints": [
                {"label": "Winter range", "rawLabel": "winter_range", "value": 2, "sharePct": 0.5, "mentionCount": 2},
            ],
            "productSignals": [
                {"label": "Charging convenience", "rawLabel": "charging_speed", "value": 1, "sharePct": 0.25, "mentionCount": 1},
            ],
            "powertrains": [
                {"label": "BEV", "rawLabel": "BEV", "value": 3, "sharePct": 0.75, "mentionCount": 3},
            ],
            "decisionFactors": [
                {"label": "Range / charging / winter usability", "rawLabel": "range_charging", "value": 2, "sharePct": 0.5},
            ],
            "evidenceCards": [
                {
                    "title": "Winter range thread",
                    "url": "https://example.com/se/thread-1",
                    "siteName": "Tesla Club Sweden",
                    "siteType": "forum",
                    "publishTier": "high",
                    "sentiment": "negative",
                    "signals": ["Winter range"],
                    "evidenceSnippets": ["Cold-weather range dropped more than expected."],
                }
            ],
        },
    )
    write_enriched(
        "SE",
        {
            "documents": [
                {
                    "sourceCode": "se_forum",
                    "countryCode": "SE",
                    "countryLabel": "Sweden / 瑞典",
                    "siteName": "Tesla Club Sweden",
                    "siteType": "forum",
                    "language": "sv",
                    "url": "https://example.com/se/thread-1",
                    "publishedAt": "2026-04-19T08:00:00+00:00",
                    "collectedAt": "2026-04-19T08:05:00+00:00",
                    "publishDecision": "auto_publish",
                    "qualityScore": 7,
                    "observationCount": 2,
                    "excerpt": "Owners complain about winter range and queueing at public chargers.",
                    "cleanedText": "Owners complain about winter range and queueing at public chargers after the latest software update.",
                    "observations": [
                        {
                            "signalKind": "painPoint",
                            "signalKey": "winter_range",
                            "label": "Winter range",
                            "sentence": "Owners complain about winter range after the latest software update.",
                            "matchedTokens": ["winter range"],
                            "sentiment": "negative",
                        }
                    ],
                }
            ]
        },
    )
    write_deck(
        "NO",
        {
            "countryCode": "NO",
            "countryLabel": "Norway / 挪威",
            "generatedAt": "2026-04-19T09:00:00+00:00",
            "metrics": [
                {"label": "Sources", "value": 2},
                {"label": "Documents", "value": 3},
                {"label": "Publish-ready docs", "value": 3},
                {"label": "Signal observations", "value": 4},
                {"label": "Avg quality score", "value": 7.5},
            ],
            "observedSections": ["source_mix", "product_signals", "evidence_cards"],
            "inferredSections": ["age_distribution"],
            "sourceMix": [
                {"label": "Bil24", "rawLabel": "Bil24", "value": 3, "sharePct": 1.0},
            ],
            "siteTypes": [
                {"label": "media_comments", "rawLabel": "media_comments", "value": 3, "sharePct": 1.0},
            ],
            "languages": [
                {"label": "no", "rawLabel": "no", "value": 3, "sharePct": 1.0},
            ],
            "publishTiers": [
                {"label": "high", "rawLabel": "high", "value": 3, "sharePct": 1.0},
            ],
            "sentiment": [
                {"label": "neutral", "rawLabel": "neutral", "value": 3, "sharePct": 1.0},
            ],
            "ownershipStages": [
                {"label": "Charging / energy", "rawLabel": "charging_energy", "value": 1, "sharePct": 0.3333, "mentionCount": 1},
            ],
            "painPoints": [
                {"label": "Public charging reliability", "rawLabel": "public_charging_reliability", "value": 1, "sharePct": 0.3333, "mentionCount": 1},
            ],
            "productSignals": [
                {"label": "Charging convenience", "rawLabel": "charging_speed", "value": 2, "sharePct": 0.6667, "mentionCount": 2},
            ],
            "powertrains": [
                {"label": "BEV", "rawLabel": "BEV", "value": 1, "sharePct": 0.3333, "mentionCount": 1},
            ],
            "decisionFactors": [
                {"label": "Range / charging / winter usability", "rawLabel": "range_charging", "value": 1, "sharePct": 0.3333},
            ],
            "evidenceCards": [
                {
                    "title": "Charging issue thread",
                    "url": "https://example.com/no/thread-1",
                    "siteName": "Bil24",
                    "siteType": "media_comments",
                    "publishTier": "high",
                    "sentiment": "neutral",
                    "signals": ["Charging convenience"],
                    "evidenceSnippets": ["Public fast chargers were often occupied."],
                }
            ],
        },
    )
    write_enriched(
        "NO",
        {
            "documents": [
                {
                    "sourceCode": "no_comments",
                    "countryCode": "NO",
                    "countryLabel": "Norway / 挪威",
                    "siteName": "Bil24",
                    "siteType": "media_comments",
                    "language": "no",
                    "url": "https://example.com/no/thread-1",
                    "publishedAt": "2026-04-19T09:00:00+00:00",
                    "collectedAt": "2026-04-19T09:05:00+00:00",
                    "publishDecision": "auto_publish",
                    "qualityScore": 8,
                    "observationCount": 2,
                    "excerpt": "Drivers say public fast chargers were often occupied.",
                    "cleanedText": "Drivers say public fast chargers were often occupied and charging convenience remains inconsistent in busy corridors.",
                    "observations": [
                        {
                            "signalKind": "productSignal",
                            "signalKey": "charging_speed",
                            "label": "Charging convenience",
                            "sentence": "Charging convenience remains inconsistent in busy corridors.",
                            "matchedTokens": ["charging"],
                            "sentiment": "neutral",
                        }
                    ],
                }
            ]
        },
    )

    monkeypatch.setattr(customer_insight_service, "VOC_FORUM_ROOT", tmp_path)

    payload = customer_insight_service.query_nordic_customer_deck(mode="forum_live")

    assert payload["metadata"]["mode"] == "forum_live"
    assert payload["metadata"]["sampleUnitLabel"] == "docs"
    assert payload["metadata"]["countryCodes"] == ["SE", "NO"]
    assert payload["page"]["forumLive"]["sourceMix"][0]["label"] == "Bil24"
    assert payload["page"]["forumLive"]["productSignals"][0]["mentionCount"] == 3
    assert payload["page"]["forumLive"]["observedSections"] == [
        "evidence_cards",
        "pain_points",
        "product_signals",
        "source_mix",
    ]
    assert metric_value(payload, "Countries") == 2
    assert metric_value(payload, "Sources") == 5
    assert metric_value(payload, "Documents") == 7
    assert metric_value(payload, "Signal observations") == 6
    assert metric_value(payload, "Avg quality score") == 6.93
    assert payload["page"]["conclusionCards"][0]["headline"] == "Bil24"
    assert payload["page"]["conclusionCards"][2]["headline"] == "Charging convenience"
    first_evidence = payload["page"]["forumLive"]["evidenceCards"][0]
    assert first_evidence["contentPreview"]
    assert first_evidence["countryCode"] == "NO"
    assert first_evidence["observationCount"] == 2
    assert first_evidence["observations"][0]["label"] == "Charging convenience"
    assert first_evidence["excerpt"] == "Drivers say public fast chargers were often occupied."

    norway_only = customer_insight_service.query_nordic_customer_deck(
        mode="forum_live",
        country_codes=["NO"],
    )

    assert norway_only["metadata"]["countryCodes"] == ["NO"]
    assert norway_only["metadata"]["coverageLabel"] == "NO"
    assert metric_value(norway_only, "Countries") == 1
    assert metric_value(norway_only, "Documents") == 3
    assert norway_only["page"]["forumLive"]["sourceMix"] == [
        {"label": "Bil24", "rawLabel": "Bil24", "value": 3, "sharePct": 1.0}
    ]
    assert norway_only["page"]["forumLive"]["evidenceCards"][0]["language"] == "no"


def test_query_nordic_hev_customer_deck_returns_hev_page_shape(monkeypatch) -> None:
    frame = pd.DataFrame(
        {
            "Age": [38, 44, 51],
            "Gender": ["Male", "Female", "Male"],
            "Marital Status": ["Married", "Married", "Cohabiting"],
            "Household Size": [4, 4, 3],
            "Education": ["University", "University", "College"],
            "Region": ["Stockholm", "Göteborg", "Uppsala"],
            "Children": ["2 children", "2 children", "1 child"],
            "Occupation / Industry": [
                "Software Engineer / Tech",
                "Teacher / Education",
                "Construction Project Manager",
            ],
            "Sports / Hobbies": [
                "Trail running; Hiking / fell walking",
                "Cross-country skiing; Gardening",
                "Road cycling; Photography",
            ],
            "Frequent Locations": [
                "Children's school & activities; Summer cottage",
                "Supermarket / ICA / Prisma; Ski resort / slopes",
                "City centre; Hardware store / Bauhaus",
            ],
            "Spending Philosophy": [
                "Family-centred practical buyer",
                "Quality first, price secondary",
                "Value for money – functional over flashy",
            ],
            "Daily Life Pattern": [
                "Commutes 48 km/day by car; evenings revolve around school runs and shopping",
                "Combines office commute with weekend cottage trips in winter",
                "Commutes 36 km/day by car and often drives family gear between activities",
            ],
            "Product Preference": ["SUV", "SUV", "SUV"],
            "Social Circle": ["Family", "Family", "Colleagues"],
            "Personality": ["Pragmatic", "Measured", "Practical"],
            "Car Ownership / Model": [
                "Yes – Toyota RAV4 Hybrid",
                "Yes – Toyota Corolla Cross Hybrid",
                "Yes – Toyota Yaris Cross Hybrid",
            ],
            "Why This Car?": [
                "Toyota reliability, low fuel consumption and family-friendly boot space made the choice easy",
                "Wanted a self-charging hybrid that stays calm in winter and holds value well",
                "Needed a hybrid SUV that feels easy to own and cheap to run over time",
            ],
            "Current Car Pain Points": [
                "Cabin can feel tight with winter gear",
                "Could use quieter tyres on motorways",
                "Rear-seat space is only just enough with child seats",
            ],
            "Dream Car": ["Toyota RAV4 Hybrid Lounge", "Toyota Highlander Hybrid", "Toyota RAV4 Hybrid AWD"],
            "Purchase Type": ["Finance", "Lease", "Finance"],
            "Driving Scenarios": [
                "Daily urban commute + school run; Long-distance family road trips (Germany/Norway)",
                "Mixed city + rural daily use; Weekend ski trips to the mountains",
                "Daily motorway commute (60–120 km/day); Summer cottage",
            ],
            "Future Car Requirements": [
                "No plug needed, lower fuel cost, heated seats and reliable winter traction",
                "Self-charging hybrid, quiet cabin and strong resale value",
                "Low running cost, family boot space and stress-free ownership",
            ],
            "Usage Frequency": ["Every day", "Every day", "5 days/week"],
            "Powertrain Preference": [
                "Mild hybrid (HEV) – no plug needed",
                "Mild hybrid (HEV) – no plug needed",
                "Mild hybrid (HEV) – no plug needed",
            ],
            "Top 3 Favourite Features": [
                "1. Fuel economy  2. Toyota reliability  3. Heated seats",
                "1. Winter traction  2. Quiet cabin  3. Easy ingress for family",
                "1. Low running cost  2. Toyota resale value  3. Simple controls",
            ],
            "Top 3 Complaints": [
                "1. Tyre noise  2. Base audio  3. Rear-seat width",
                "1. Slightly firm ride  2. Limited third-row options  3. Boot lip height",
                "1. Modest acceleration  2. Narrow rear bench  3. Small infotainment icons",
            ],
            "Customer Requirements": [
                "Needs a self-charging SUV without a new charging routine",
                "Wants Toyota dependability, winter tyres bundled and simple dealer support",
                "Prefers no home charger dependency and straightforward maintenance costs",
            ],
            "Suggestions": [
                "Keep winter package simple and clearly priced",
                "Explain Toyota hybrid system benefits in plain Nordic family use cases",
                "Show running-cost math against diesel SUVs",
            ],
            "Information Source": ["Manufacturer website", "Recommendation from colleague", "Dealership test drive event"],
            "Price Perception": [
                "Higher upfront, but justified by reliability and resale value",
                "Feels reasonable if fuel savings and resale stay strong",
                "Good value over the ownership cycle",
            ],
            "Evaluation": [
                "A dependable family hybrid that avoids charging hassle",
                "Feels like the safest winter-proof choice for a family",
                "Would buy again because it keeps life simple",
            ],
            "Closing Remarks": ["Reliable and calm", "Toyota just makes sense", "Hybrid without friction"],
            "Interview Screenshot": ["", "", ""],
        }
    )

    monkeypatch.setattr(customer_insight_service, "_load_hev_voc_frame", lambda: frame.copy())

    payload = customer_insight_service.query_nordic_hev_customer_deck()

    assert payload["metadata"]["datasetLabel"] == "VOC Sweden HEV Owners"
    assert payload["metadata"]["respondentCount"] == 3
    assert payload["page"]["subtitle"] == "瑞典 HEV 车主画像"
    assert payload["page"]["methodologyNote"]
    assert payload["page"]["persona"]["title"] == "典型瑞典 HEV 家庭省心派"
    assert payload["page"]["conclusionCards"][0]["headline"] == "瑞典 HEV 样本里，Toyota hybrid 是最稳定的现实锚点"
    assert next(item["value"] for item in payload["page"]["metrics"] if item["label"] == "Toyota 占比") == "100%"
    assert next(item["value"] for item in payload["page"]["metrics"] if item["label"] == "HEV 偏好") == "100%"
    assert payload["page"]["powertrain"]["items"][0]["label"] == "HEV 不插电"


def test_hev_preference_heuristics_treat_hev_first_phrasing_as_no_plug() -> None:
    frame = pd.DataFrame(
        {
            "Powertrain Preference": [
                "HEV for now, open to BEV in 5 years",
                "Prefer HEV until BEV winter range exceeds 500 km in real use",
                "HEV and no BEV until home charging becomes practical",
            ]
        }
    )

    assert customer_insight_service._count_hev_preferences(frame) == 3
    assert customer_insight_service._count_no_plug_hev_preferences(frame) == 3
