from app.services import web_search_service


def test_market_news_queries_fallback_to_model_generation_terms() -> None:
    queries = web_search_service._build_market_news_queries(
        country="瑞典",
        question=(
            "瑞典 SUV-A 的 HEV 月度销量如果出现下跌，"
            "结合最近 Toyota RAV4 第六代换代新闻，可能是什么原因？"
        ),
    )

    assert queries[0] == "SUV-A Toyota RAV4 Sweden Sverige 2026"
    assert "Toyota RAV4 new generation Europe 2026" in queries
    assert "Toyota RAV4 sixth generation Europe 2026" in queries


def test_market_news_scoring_penalizes_stale_results() -> None:
    stale = web_search_service.WebSearchResult(
        title="The Toyota RAV4 Fails Moose Test in Sweden",
        url="https://example.test/old",
        snippet="Old Sweden result",
        source="Auto123",
        publishedAt="2019-09-13",
        provider="test",
    )
    fresh = web_search_service.WebSearchResult(
        title="Toyota launches sixth-generation RAV4 across Europe",
        url="https://example.test/new",
        snippet="All-new Toyota RAV4 launches in Europe",
        source="Automotive World",
        publishedAt="2026-04-08",
        provider="test",
    )

    assert web_search_service._score_result_relevance(
        fresh,
        "Toyota RAV4 new generation Europe 2026",
    ) > web_search_service._score_result_relevance(
        stale,
        "Toyota RAV4 Sweden Sverige 2026",
    )
