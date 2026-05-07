import json
from pathlib import Path

from jato_scraper import voc_enricher


def _write_country_raw_payload(root: Path) -> None:
    raw_root = root / "se" / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    (raw_root / "se_demo_forum.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "se_demo_forum",
                    "country_code": "SE",
                    "country_label": "Sweden / 瑞典",
                    "site_name": "Demo Forum",
                    "site_type": "ev_community",
                    "language": "sv",
                },
                "taxonomyProfile": "nordic_core",
                "collectedAt": "2026-04-19T10:00:00+00:00",
                "autoReview": {
                    "publishReadyCount": 3,
                    "publishTier": "high",
                    "publishDecision": "auto_publish",
                },
                "documentCount": 3,
                "documents": [
                    {
                        "sourceCode": "se_demo_forum",
                        "countryCode": "SE",
                        "countryLabel": "Sweden / 瑞典",
                        "siteName": "Demo Forum",
                        "siteType": "ev_community",
                        "language": "sv",
                        "url": "https://example.com/thread-1",
                        "title": "Winter range and charging queue after software update",
                        "publishedAt": "2026-04-18T12:00:00Z",
                        "summary": "Owners discuss charger queues and software issues.",
                        "excerpt": "Winter range dropped sharply and the public charging queue was painful.",
                        "rawText": (
                            "After the winter trip, our Tesla Model Y showed obvious range loss and the public charging queue was painful. "
                            "A recent software update fixed some bugs, but dealer service wait times are still long compared with the Volvo EX30 and the lease price still feels expensive."
                        ),
                        "replyPosts": [
                            {
                                "unitId": "thread-1-reply-1",
                                "unitType": "reply_post",
                                "author": "owner_1",
                                "publishedAt": "2026-04-18T12:15:00Z",
                                "text": "My winter commute still shows weak range and charging queues make the trip planning worse.",
                            },
                            {
                                "unitId": "thread-1-reply-2",
                                "unitType": "reply_post",
                                "author": "owner_2",
                                "publishedAt": "2026-04-18T12:20:00Z",
                                "text": "Lease value matters too, so range and price are linked in the shortlist.",
                            },
                        ],
                        "collectedAt": "2026-04-19T10:00:00+00:00",
                        "autoReview": {
                            "score": 7,
                            "publishTier": "high",
                            "publishDecision": "auto_publish",
                        },
                    },
                    {
                        "sourceCode": "se_demo_forum",
                        "countryCode": "SE",
                        "countryLabel": "Sweden / 瑞典",
                        "siteName": "Demo Forum",
                        "siteType": "ev_community",
                        "language": "sv",
                        "url": "https://example.com/thread-2",
                        "title": "Family test drive points to a new PHEV lease",
                        "publishedAt": "2026-04-18T13:00:00Z",
                        "summary": "Cross-shopping family SUVs and lease offers.",
                        "excerpt": "We are comparing PHEV offers and need space for a caravan.",
                        "rawText": (
                            "We are cross-shopping a new Skoda Enyaq lease after a strong test drive. "
                            "Family space, trailer use, and overall price value matter most compared with the Volkswagen ID.4."
                        ),
                        "collectedAt": "2026-04-19T10:00:00+00:00",
                        "autoReview": {
                            "score": 6,
                            "publishTier": "medium",
                            "publishDecision": "candidate_publish",
                        },
                    },
                    {
                        "sourceCode": "se_demo_forum",
                        "countryCode": "SE",
                        "countryLabel": "Sweden / 瑞典",
                        "siteName": "Demo Forum",
                        "siteType": "ev_community",
                        "language": "sv",
                        "url": "https://example.com/thread-3",
                        "title": "Real-world range still matters more than premium trim",
                        "publishedAt": "2026-04-18T14:00:00Z",
                        "summary": "Commuters compare range and lease economics.",
                        "excerpt": "Range, commute confidence, and monthly lease cost decide the shortlist.",
                        "rawText": (
                            "For our daily commute we care most about real-world range and overall lease cost. "
                            "The Tesla Model Y and Skoda Enyaq both work, but value for money wins."
                        ),
                        "collectedAt": "2026-04-19T10:00:00+00:00",
                        "autoReview": {
                            "score": 7,
                            "publishTier": "high",
                            "publishDecision": "auto_publish",
                        },
                    },
                ],
                "errors": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (raw_root / "se_alt_forum.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_code": "se_alt_forum",
                    "country_code": "SE",
                    "country_label": "Sweden / 瑞典",
                    "site_name": "Alt Owners Forum",
                    "site_type": "forum",
                    "language": "sv",
                },
                "taxonomyProfile": "nordic_core",
                "collectedAt": "2026-05-02T10:00:00+00:00",
                "autoReview": {
                    "publishReadyCount": 1,
                    "publishTier": "high",
                    "publishDecision": "auto_publish",
                },
                "documentCount": 1,
                "documents": [
                    {
                        "sourceCode": "se_alt_forum",
                        "countryCode": "SE",
                        "countryLabel": "Sweden / 瑞典",
                        "siteName": "Alt Owners Forum",
                        "siteType": "forum",
                        "language": "sv",
                        "url": "https://example.com/thread-4",
                        "title": "Lease value and range are still the shortlist anchors",
                        "publishedAt": "2026-05-01T08:00:00Z",
                        "summary": "Forum users compare range and monthly cost.",
                        "excerpt": "Real-world range and lease value keep deciding the shortlist.",
                        "rawText": (
                            "On our forum shortlist, real-world range and monthly lease value matter most. "
                            "The Tesla Model Y still competes with the Skoda Enyaq because price value and range stay linked."
                        ),
                        "collectedAt": "2026-05-02T10:00:00+00:00",
                        "autoReview": {
                            "score": 8,
                            "publishTier": "high",
                            "publishDecision": "auto_publish",
                        },
                    }
                ],
                "errors": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def test_build_country_voc_enrichment_extracts_signals(tmp_path: Path) -> None:
    _write_country_raw_payload(tmp_path)

    payload = voc_enricher.build_country_voc_enrichment(tmp_path / "se")

    assert payload["countryCode"] == "SE"
    assert payload["documentCount"] == 4
    assert payload["publishReadyDocumentCount"] == 4
    assert payload["sourceCount"] == 2
    assert payload["analysisProfile"]["themeTags"]
    assert payload["analysisProfile"]["personaCohorts"]
    assert payload["analysisProfile"]["productCatalog"]
    assert payload["analysisProfile"]["associationMethodology"]["transactionUnit"] == "content_unit"
    assert payload["analysisUnitCount"] > payload["documentCount"]
    first_document = next(
        document for document in payload["documents"] if document["url"] == "https://example.com/thread-1"
    )
    assert first_document["sentiment"] == "negative"
    assert "winter_range" in first_document["painPoints"]
    assert "public_charging_reliability" in first_document["painPoints"]
    assert "service_wait_time" in first_document["painPoints"]
    assert "charging_speed" in first_document["productSignals"]
    assert "winter_usability" in first_document["themeTags"]
    assert first_document["personaTags"]
    assert first_document["productMentions"]
    assert first_document["primaryProduct"]
    assert first_document["analysisUnits"]
    assert any(unit["unitSource"] == "explicit" for unit in first_document["analysisUnits"])
    assert any(unit["unitType"] == "reply_post" for unit in first_document["analysisUnits"])
    assert first_document["autoScores"]["overallScore"] > 0
    pain_point_labels = {item["label"] for item in payload["aggregates"]["painPoints"]}
    assert "Winter range" in pain_point_labels
    assert "Public charging reliability" in pain_point_labels
    assert payload["aggregates"]["evidenceCards"]
    assert payload["aggregates"]["evidenceCards"][0]["contentPreview"]
    assert payload["aggregates"]["evidenceCards"][0]["observations"]
    assert payload["aggregates"]["themeTags"]
    assert payload["aggregates"]["personaCohorts"]
    assert payload["aggregates"]["matchedProducts"]
    assert payload["aggregates"]["scoreBands"]
    assert payload["aggregates"]["crossAnalysis"]["productPainPoints"]
    assert payload["aggregates"]["associationGraph"]
    assert payload["aggregates"]["synergyMatrix"]
    assert payload["aggregates"]["associationRecommendations"]
    assert payload["aggregates"]["filterSuggestions"]
    assert any(
        item["leftKey"] == "range_efficiency" and item["rightKey"] == "price_tco"
        or item["leftKey"] == "price_tco" and item["rightKey"] == "range_efficiency"
        for item in payload["aggregates"]["synergyMatrix"]
    )
    pair = next(
        item
        for item in payload["aggregates"]["associationGraph"]
        if (
            item["leftKey"] == "range_efficiency" and item["rightKey"] == "price_tco"
        ) or (
            item["leftKey"] == "price_tco" and item["rightKey"] == "range_efficiency"
        )
    )
    assert "confidenceForwardPct" in pair
    assert "confidenceReversePct" in pair
    assert "jaccard" in pair
    assert "npmi" in pair
    assert "phiCoefficient" in pair
    assert "expectedCount" in pair
    assert pair["sourceCount"] >= 2
    assert pair["siteTypeCount"] >= 2
    assert pair["monthBucketCount"] >= 2
    assert pair["validation"]["replicatedAcrossSources"] is True
    assert pair["validation"]["replicatedAcrossSiteTypes"] is True
    assert pair["validation"]["replicatedAcrossMonths"] is True
    assert payload["aggregates"]["personaSummaries"]
    assert first_document["observationCount"] > 0
    assert first_document["observations"][0]["signalKind"]
    assert payload["signalObservationCount"] >= first_document["observationCount"]


def test_clean_document_text_removes_forum_chrome() -> None:
    cleaned = voc_enricher._clean_document_text(
        {
            "title": "Bästa privatleasing erbjudanden för stunden",
            "summary": None,
            "rawText": (
                "Tesla Club Sweden Hoppa till sida: Hoppa till sida: Alla tidsangivelser är UTC+02:00 Europe/Stockholm | "
                "Vi jämför privatleasing och prisnivåer för nya PHEV-erbjudanden."
            ),
            "excerpt": "",
        }
    )

    assert "Hoppa till sida" not in cleaned
    assert "Europe/Stockholm" not in cleaned
    assert "privatleasing" in cleaned


def test_find_matches_uses_token_boundaries() -> None:
    matches, _ = voc_enricher._find_matches(
        "Toyota bZ4X blir bedre i kaldt vær.",
        {"software_bug": ("ota",), "winter_range": ("kaldt",)},
    )

    assert "software_bug" not in matches
    assert "winter_range" in matches


def test_build_voc_enriched_collection_writes_country_artifacts(tmp_path: Path) -> None:
    _write_country_raw_payload(tmp_path)

    summary = voc_enricher.build_voc_enriched_collection(output_root=tmp_path)

    assert summary["country_count"] == 1
    assert summary["document_count"] == 4
    assert summary["publish_ready_document_count"] == 4
    country_item = summary["countries"][0]
    enriched_path = Path(country_item["enriched_output_path"])
    deck_path = Path(country_item["deck_output_path"])
    assert enriched_path.exists()
    assert deck_path.exists()

    deck = json.loads(deck_path.read_text(encoding="utf-8"))
    assert deck["sourceMode"] == "forum_voc"
    assert deck["metrics"][0]["label"] == "Sources"
    assert any(item["label"] == "Analysis units" for item in deck["metrics"])
    assert any(item["label"] == "Signal observations" for item in deck["metrics"])
    assert any(item["label"] == "Avg overall score" for item in deck["metrics"])
    assert deck["methodologyNote"]
    assert deck["evidenceCards"]
    assert deck["evidenceCards"][0]["contentPreview"]
    assert deck["evidenceCards"][0]["observations"][0]["label"]
    assert deck["matchedProducts"]
    assert deck["personaCohorts"]
    assert deck["crossAnalysis"]["productPainPoints"]
    assert deck["associationGraph"]
    assert deck["synergyMatrix"]
    assert deck["associationRecommendations"]
    assert deck["filterSuggestions"]
    assert deck["associationMethodology"]["statisticalValidation"]["multipleTestingCorrection"] == "benjamini_hochberg_fdr"
    assert any(card["label"] == "Top co-consideration" for card in deck["conclusionCards"])
