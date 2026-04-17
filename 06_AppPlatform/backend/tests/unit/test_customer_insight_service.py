import pandas as pd

from app.services import customer_insight_service


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
