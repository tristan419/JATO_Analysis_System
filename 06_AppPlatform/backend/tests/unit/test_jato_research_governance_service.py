from __future__ import annotations

from app.services.jato_research_governance_service import apply_research_governance
from app.services.jato_research_governance_service import classify_source_category
from app.services.jato_research_governance_service import dedupe_research_sources
from app.services.jato_research_governance_service import get_research_policy
from app.services.jato_research_governance_service import infer_research_intent
from app.services.jato_research_governance_service import research_mode_config
from app.services.jato_research_governance_service import standardize_jato_cross_check


def test_research_policy_requires_official_source_for_policy() -> None:
    policy = get_research_policy("news_policy_search")

    assert policy["minSources"] == 3
    assert policy["requireOfficialSource"] is True
    assert policy["requirePublishDate"] is True
    assert infer_research_intent("瑞典 2026 年 BEV 补贴有什么影响？") == "news_policy_search"


def test_research_intent_prioritizes_voc_over_configuration_terms() -> None:
    question = "瑞典用户对 OMODA/JAECOO 最容易吐槽哪些配置或使用场景？"

    assert infer_research_intent(question) == "voc_analysis"
    assert get_research_policy(infer_research_intent(question))["intent"] == "voc_analysis"


def test_source_category_and_dedupe_prefers_official_source() -> None:
    assert classify_source_category(url="https://www.transportstyrelsen.se/policy", source="") == "official"
    deduped, rejected = dedupe_research_sources([
        {
            "title": "Sweden EV subsidy update",
            "url": "https://news.example.com/sweden-ev-subsidy",
            "source": "news.example.com",
            "sourceScore": 88,
        },
        {
            "title": "Sweden EV subsidy update",
            "url": "https://www.transportstyrelsen.se/sweden-ev-subsidy",
            "source": "transportstyrelsen.se",
            "sourceScore": 72,
        },
    ])

    assert len(deduped) == 1
    assert deduped[0]["sourceCategory"] == "official"
    assert rejected[0]["reason"] == "duplicate_replaced_by_better_source"


def test_cross_check_status_standardization() -> None:
    matched = standardize_jato_cross_check(
        {"status": "matched", "checks": [{"name": "kpis"}, {"name": "models"}], "summary": "ok"},
        intent="pricing_analysis",
        question="J7 HEV pricing in Sweden",
    )
    policy = standardize_jato_cross_check(
        {"status": "matched", "checks": [{"name": "kpis"}, {"name": "models"}], "summary": "ok"},
        intent="news_policy_search",
        question="瑞典 2026 年 BEV 补贴政策会影响什么？",
    )

    assert matched["status"] == "matched"
    assert policy["status"] == "not_applicable"


def test_apply_research_governance_adds_claim_mapping_cost_and_insight_cards() -> None:
    data = {
        "items": [
            {
                "title": "Sweden EV policy update",
                "url": "https://www.transportstyrelsen.se/policy",
                "source": "transportstyrelsen.se",
                "publishedAt": "2026-05-01",
                "snippet": "Sweden updated EV policy eligibility in 2026. The policy has vehicle price constraints.",
                "sourceScore": 90,
                "sourceTier": "high",
            },
            {
                "title": "Sweden EV policy update",
                "url": "https://www.transportstyrelsen.se/policy?utm=copy",
                "source": "transportstyrelsen.se",
                "publishedAt": "2026-05-01",
                "snippet": "Duplicate policy copy.",
                "sourceScore": 80,
                "sourceTier": "high",
            },
            {
                "title": "Industry comment on Sweden EV policy",
                "url": "https://www.reuters.com/business/autos/sweden-ev-policy",
                "source": "reuters.com",
                "publishedAt": "2026-05-02",
                "snippet": "Industry analysts expect small BEV SUVs to benefit from the policy.",
                "sourceScore": 74,
                "sourceTier": "high",
            },
            {
                "title": "Mobility Sweden EV market note",
                "url": "https://mobilitysweden.se/ev-market-note",
                "source": "mobilitysweden.se",
                "publishedAt": "2026-05-03",
                "snippet": "Mobility Sweden reported BEV mix changes.",
                "sourceScore": 70,
                "sourceTier": "medium",
            },
        ],
        "sourceCoverage": {"queryCount": 2, "provider": "tavily"},
        "queriesRun": ["q1", "q2"],
        "jatoCrossCheck": {"status": "not_applicable", "checks": []},
        "researchPlan": [],
        "limitations": [],
    }

    governed = apply_research_governance(
        data,
        intent="news_policy_search",
        question="瑞典 2026 年 BEV 补贴会如何影响 O5 BEV？",
        research_mode="standard",
        latency_ms=123,
    )

    assert governed["researchGovernance"]["policyStatus"] == "passed"
    assert governed["researchGovernance"]["metrics"]["queryCount"] == 2
    assert governed["researchGovernance"]["metrics"]["sourcesUsed"] == 3
    assert governed["sourceCoverage"]["officialSourceCount"] == 1
    assert governed["citations"][0]["supportedClaim"].startswith("Sweden updated EV policy")
    assert governed["citations"][0]["claimType"] == "fact"
    assert governed["insightCards"][0]["citations"] == ["R1"]
    assert governed["researchGovernance"]["rejectedSources"]


def test_research_governance_rejects_clearly_irrelevant_policy_sources() -> None:
    data = {
        "items": [
            {
                "title": "EVERY angle of Alexander Isak scores Sweden's second goal against Tunisia",
                "url": "https://www.fifa.com/world-cup/sweden-tunisia",
                "source": "fifa.com",
                "publishedAt": "2026-06-01",
                "snippet": "Alexander Isak scored as Sweden extended its lead over Tunisia at the 2026 FIFA World Cup.",
                "sourceScore": 99,
            },
            {
                "title": "Sweden EV subsidy price cap update",
                "url": "https://www.electrive.com/sweden-ev-subsidy-price-cap",
                "source": "electrive.com",
                "publishedAt": "2026-05-02",
                "snippet": "Sweden EV subsidy policy may include price cap rules for battery-electric vehicles.",
                "sourceScore": 74,
            },
        ],
        "jatoCrossCheck": {"status": "not_applicable", "checks": []},
        "limitations": [],
    }

    governed = apply_research_governance(
        data,
        intent="news_policy_search",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        research_mode="standard",
        latency_ms=50,
    )

    assert governed["sourceCoverage"]["sourcesReturned"] == 2
    assert governed["sourceCoverage"]["sourcesUsed"] == 1
    assert governed["citations"][0]["source"] == "electrive.com"
    assert "fifa" not in str(governed["citations"]).lower()
    rejected = governed["researchGovernance"]["rejectedSources"]
    assert any(item["reason"] == "low_question_relevance" and "fifa" in str(item["url"]) for item in rejected)


def test_research_governance_rejects_weak_wiki_style_sources_as_citations() -> None:
    data = {
        "items": [
            {
                "title": "Hungary EV incentives and legislation",
                "url": "https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/hungary/incentives-legislations",
                "source": "alternative-fuels-observatory.ec.europa.eu",
                "publishedAt": "2026-04-01",
                "snippet": "Hungary has electric vehicle incentive policy details for company BEV purchases.",
                "sourceScore": 87,
            },
            {
                "title": "Plug-in electric vehicles in Hungary — Grokipedia",
                "url": "https://grokipedia.com/page/plug_in_electric_vehicles_in_hungary",
                "source": "grokipedia.com",
                "snippet": "Hungary BEV market share and purchase subsidy background.",
                "sourceScore": 75,
            },
        ],
        "jatoCrossCheck": {"status": "not_applicable", "checks": []},
        "limitations": [],
    }

    governed = apply_research_governance(
        data,
        intent="market_overview",
        question="匈牙利 J7 HEV 是否值得继续验证？",
        research_mode="standard",
        latency_ms=50,
    )

    assert governed["sourceCoverage"]["sourcesUsed"] == 1
    assert governed["citations"][0]["source"] == "alternative-fuels-observatory.ec.europa.eu"
    assert "grokipedia" not in str(governed["citations"]).lower()
    rejected = governed["researchGovernance"]["rejectedSources"]
    assert any(item["reason"] == "weak_citation_source" and "grokipedia" in str(item["url"]) for item in rejected)


def test_research_governance_ignores_privacy_policy_footer_for_policy_relevance() -> None:
    data = {
        "items": [
            {
                "title": "EVERY ANGLE of Sweden vs Tunisia - FOX Sports",
                "url": "https://www.foxsports.com/watch/fmc-jv0ovc0r1moa8dtn",
                "source": "foxsports.com",
                "publishedAt": "2026-06-15",
                "snippet": "FIFA World Cup highlights. Use of this website constitutes acceptance of Terms of Use and Privacy Policy.",
                "sourceScore": 99,
            },
            {
                "title": "Sweden EV subsidy policy price cap update",
                "url": "https://www.electrive.com/sweden-ev-subsidy-price-cap",
                "source": "electrive.com",
                "publishedAt": "2026-05-02",
                "snippet": "Sweden EV subsidy policy may affect battery-electric vehicles under a price cap.",
                "sourceScore": 74,
            },
        ],
        "jatoCrossCheck": {"status": "not_applicable", "checks": []},
        "limitations": [],
    }

    governed = apply_research_governance(
        data,
        intent="news_policy_search",
        question="Elbilspremien 2026 会影响哪些车型？",
        research_mode="standard",
        latency_ms=50,
    )

    assert governed["sourceCoverage"]["sourcesUsed"] == 1
    assert governed["citations"][0]["source"] == "electrive.com"
    assert "foxsports" not in str(governed["citations"]).lower()


def test_report_generation_policy_research_rejects_generic_ev_news_for_named_policy() -> None:
    data = {
        "items": [
            {
                "title": "EV maker Polestar's quarterly sales volumes slide amid US market ban - Reuters",
                "url": "https://www.reuters.com/business/autos-transportation/ev-maker-polestars-quarterly-sales-volumes-slide-amid-us-market-ban-2026-07-09/",
                "source": "reuters.com",
                "publishedAt": "2026-07-09",
                "snippet": "Polestar quarterly sales volumes declined amid a US market ban.",
                "sourceScore": 82,
            },
            {
                "title": "Elbilspremien 2026 eligibility and price cap",
                "url": "https://example.se/elbilspremien-2026",
                "source": "example.se",
                "publishedAt": "2026-03-01",
                "snippet": "Elbilspremien policy source for eligible elbil models, price cap and private buyer subsidy rules.",
                "sourceScore": 76,
            },
        ],
        "jatoCrossCheck": {"status": "not_applicable", "checks": []},
        "limitations": [],
    }

    governed = apply_research_governance(
        data,
        intent="report_generation",
        question="Elbilspremien 2026 会影响哪些车型？请给出来源、JATO 数据交叉验证和一页汇报结构。",
        research_mode="standard",
        latency_ms=50,
    )

    assert governed["sourceCoverage"]["sourcesReturned"] == 2
    assert governed["sourceCoverage"]["sourcesUsed"] == 1
    assert governed["citations"][0]["source"] == "example.se"
    assert "polestar" not in str(governed["citations"]).casefold()
    rejected = governed["researchGovernance"]["rejectedSources"]
    assert any(item["reason"] == "low_question_relevance" and "reuters" in str(item["url"]) for item in rejected)


def test_research_governance_infers_publish_date_from_dated_url() -> None:
    data = {
        "items": [
            {
                "title": "Sweden EV incentive scheme approved",
                "url": "https://www.electrive.com/2025/12/15/green-light-for-swedens-ev-incentive-scheme",
                "source": "electrive.com",
                "snippet": "Sweden EV incentive policy includes monthly support for battery-electric vehicles.",
                "sourceScore": 82,
            },
            {
                "title": "Mobility Sweden EV market policy note",
                "url": "https://mobilitysweden.se/2025/12/16/ev-market-policy-note",
                "source": "mobilitysweden.se",
                "snippet": "Mobility Sweden discusses EV policy and market effects.",
                "sourceScore": 70,
            },
            {
                "title": "ChargeNode explains Sweden EV premium",
                "url": "https://chargenode.eu/en/blogg/2025/12/17/elbilspremien",
                "source": "chargenode.eu",
                "snippet": "The EV premium may affect BEV purchase decisions.",
                "sourceScore": 65,
            },
        ],
        "jatoCrossCheck": {"status": "not_applicable", "checks": []},
        "limitations": [],
    }

    governed = apply_research_governance(
        data,
        intent="news_policy_search",
        question="BEV 补贴价格上限对 O5 BEV 定价有什么影响？",
        research_mode="standard",
        latency_ms=50,
    )

    by_url = {item["url"]: item for item in governed["items"]}
    citation_by_url = {item["url"]: item for item in governed["citations"]}
    assert by_url["https://www.electrive.com/2025/12/15/green-light-for-swedens-ev-incentive-scheme"]["publishedAt"] == "2025-12-15"
    assert citation_by_url["https://www.electrive.com/2025/12/15/green-light-for-swedens-ev-incentive-scheme"]["publishedAt"] == "2025-12-15"
    assert "publish_date_missing" not in governed["researchGovernance"]["policyWarnings"]
    assert all(item["name"] != "published_date" for item in governed["researchGovernance"]["missingEvidence"])


def test_research_mode_config_cost_guard() -> None:
    assert research_mode_config("quick")["sourceLimit"] == 3
    assert research_mode_config("standard")["queryLimit"] == 2
    assert research_mode_config("deep")["sourceLimit"] == 8
