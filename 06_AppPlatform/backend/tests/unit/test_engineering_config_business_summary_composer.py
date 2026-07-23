from __future__ import annotations

import json
import subprocess
from typing import Any

import pytest

from app.services import engineering_config_business_summary_composer as composer


@pytest.fixture(autouse=True)
def clear_business_summary_cache() -> None:
    composer.clear_engineering_config_business_summary_cache()


def _payload() -> dict[str, Any]:
    return {
        "baseTrim": {"trimId": "basic", "label": "两驱基本型 Basic-FWD"},
        "targets": [
            {
                "targetTrimId": "comfort",
                "targetLabel": "两驱舒适型 Comfort-FWD",
                "differenceCounts": {"inferred": 2, "totalDifference": 8},
                "upgradeSignals": [
                    {
                        "dimension": "座椅面料",
                        "from": "fabric seat",
                        "to": "Artificial leather seat",
                        "fromEvidenceKey": "comfort:REMOVED:seat_fabric",
                        "toEvidenceKey": "comfort:ADDED:seat_leather",
                    }
                ],
                "evidenceFacts": [
                    {
                        "evidenceKey": "comfort:ADDED:mirror_heat",
                        "featureCode": "mirror_heat",
                        "featureName": "Door mirror heating / 外后视镜加热",
                    },
                    {
                        "evidenceKey": "comfort:ADDED:seat_leather",
                        "featureCode": "seat_leather",
                        "featureName": "Artificial leather seat / 仿皮座椅",
                    },
                ],
                "sourceEvidenceSummary": {
                    "differenceCount": 8,
                    "withSourceEvidenceCount": 6,
                    "missingSourceEvidenceCount": 2,
                    "inferredCount": 2,
                    "unknownCount": 0,
                    "mergedCellExpandedCount": 1,
                    "sourceSheetNames": ["T19C MY ICE"],
                },
                "evidence": {
                    "inferredCount": 2,
                    "warning": "不配备* 是规则推断，不是 Excel 原文；引用到卖点前需要点开 source evidence 核对。",
                },
            },
            {
                "targetTrimId": "premium",
                "targetLabel": "两驱尊贵型 Premium-FWD",
                "differenceCounts": {"inferred": 0, "totalDifference": 12},
                "sourceEvidenceSummary": {
                    "differenceCount": 12,
                    "withSourceEvidenceCount": 12,
                    "missingSourceEvidenceCount": 0,
                    "inferredCount": 0,
                    "unknownCount": 0,
                    "mergedCellExpandedCount": 2,
                    "sourceSheetNames": ["T19C MY ICE", "T19C BEV"],
                },
                "evidence": {
                    "inferredCount": 0,
                    "warning": "当前差异可继续从单元格追溯来源。",
                },
            },
        ],
        "context": {
            "deltaFilter": "DIFFERENCE",
            "compareScope": {
                "sourceScope": "multi_source",
                "sourceReviewHints": [
                    "同国家同年款多来源：T19C MY ICE · Germany · 2026 存在 2 个来源 / 2 个上传人，AI 摘要需保留来源前提。"
                ],
                "sourceGroups": [
                    {
                        "label": "T19C MY ICE · Germany · 2026",
                        "sourceCount": 2,
                        "ownerCount": 2,
                        "sources": ["dealer-config.xlsx", "brand-site.html"],
                        "owners": ["alice", "bob"],
                    }
                ],
            },
        },
    }


def _provider_response(content: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "choices": [
            {
                "message": {"content": json.dumps(content, ensure_ascii=False)},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 8,
            "total_tokens": 20,
            "prompt_cache_hit_tokens": 3,
            "prompt_cache_miss_tokens": 9,
        },
    }


def test_system_prompts_preserve_evidence_review_notes() -> None:
    system_prompt = composer._system_prompt()
    single_target_prompt = composer._single_target_system_prompt()

    assert "requiresReview=true" in system_prompt
    assert "businessNote" in system_prompt
    assert "sourceEvidenceSummary" in system_prompt
    assert "sourceReviewHints" in system_prompt
    assert "sourceGroups" in system_prompt
    assert "mergedCellExpandedCount" in system_prompt
    assert "missingSourceEvidenceCount" in system_prompt
    assert "evidenceStatus" in system_prompt
    assert "requiresReview=true" in single_target_prompt
    assert "businessNote" in single_target_prompt
    assert "targets[0].sourceEvidenceSummary" in single_target_prompt
    assert "sourceReviewHints" in single_target_prompt
    assert "sourceGroups" in single_target_prompt
    assert "mergedCellExpandedCount" in single_target_prompt


def test_compact_payload_preserves_rich_business_facts_for_llm() -> None:
    payload = {
        "targets": [
            {
                "targetTrimId": f"target-{target_index}",
                "evidenceFacts": [
                    {"evidenceKey": f"target-{target_index}:fact-{fact_index}", "featureName": f"Feature {fact_index}"}
                    for fact_index in range(45)
                ],
                "categoryFacts": [{"category": f"Category {index}"} for index in range(14)],
                "businessFocusGroups": [{"label": f"Focus {index}"} for index in range(10)],
                "upgradeSignals": [{"dimension": f"Signal {index}"} for index in range(10)],
                "addedFeatures": [f"Added {index}" for index in range(18)],
                "removedFeatures": [f"Removed {index}" for index in range(18)],
                "changedFeatures": [{"feature": f"Changed {index}"} for index in range(18)],
            }
            for target_index in range(7)
        ],
        "context": {
            "genericNotes": [f"Note {index}" for index in range(20)],
        },
    }

    compact = composer._compact_payload(payload)
    target = compact["targets"][0]

    assert len(compact["targets"]) == 6
    assert len(target["evidenceFacts"]) == 40
    assert target["evidenceFacts"][-1]["evidenceKey"] == "target-0:fact-39"
    assert len(target["categoryFacts"]) == 12
    assert len(target["businessFocusGroups"]) == 8
    assert len(target["upgradeSignals"]) == 8
    assert len(target["addedFeatures"]) == 16
    assert len(target["removedFeatures"]) == 16
    assert len(target["changedFeatures"]) == 16
    assert len(compact["context"]["genericNotes"]) == 12


def test_compose_engineering_config_business_summary_reports_missing_provider_key(monkeypatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert result["summaries"] == []
    assert result["usage"]["status"] == "missing_key"
    assert result["usage"]["provider"] == "deepseek"
    assert "$" not in result["usage"]["fallbackReason"]


def test_engineering_config_business_summary_readiness_reports_provider_without_calling_llm(monkeypatch) -> None:
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    readiness = composer.get_engineering_config_business_summary_readiness()

    assert readiness["ready"] is True
    assert readiness["status"] == "ready"
    assert readiness["provider"] == "deepseek"
    assert readiness["model"]
    assert readiness["apiBase"]
    assert readiness["keySource"] == "DEEPSEEK_API_KEY"
    assert readiness["providerConfigured"] is True
    assert readiness["runtimeUrl"] == ""
    assert readiness["runtimeUsed"] is False
    assert readiness["runtimeStatus"] == "not_used_by_compare_runtime_compose"
    assert readiness["liveCheck"] == "not_performed"
    assert readiness["pipeline"] == "compare_runtime_compose"
    assert readiness["persisted"] is False
    assert readiness["cacheSize"] == 0
    assert "Source Digest upload stores source files" in readiness["notes"][0]


def test_engineering_config_business_summary_readiness_reports_missing_key(monkeypatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    readiness = composer.get_engineering_config_business_summary_readiness()

    assert readiness["ready"] is False
    assert readiness["status"] == "missing_key"
    assert "DEEPSEEK_API_KEY is not configured" in readiness["message"]
    assert "$" not in readiness["message"]


def test_compose_engineering_config_business_summary_uses_config_provider(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_post_chat_completion(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["外后视镜加热", "座椅面料升级"],
                        "replacementsOrReductions": ["手动后视镜被电动折叠替代"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                        "evidenceRefs": [
                            {
                                "section": "mainUpgrades",
                                "itemIndex": 0,
                                "evidenceKey": "comfort:ADDED:mirror_heat",
                                "featureCode": "mirror_heat",
                                "category": "舒适便利",
                                "reason": "外后视镜加热来自配置差异事实。",
                            },
                            {
                                "section": "mainUpgrades",
                                "itemIndex": 1,
                                "evidenceKey": "comfort:ADDED:seat_leather",
                                "featureCode": "seat_leather",
                                "category": "内饰",
                                "reason": "座椅面料升级引用新增座椅面料配置。",
                            },
                            {
                                "section": "mainUpgrades",
                                "itemIndex": 8,
                                "evidenceKey": "comfort:ADDED:mirror_heat",
                                "reason": "越界引用应被丢弃。",
                            },
                                {
                                    "section": "replacementsOrReductions",
                                    "itemIndex": 0,
                                    "evidenceKey": "comfort:ADDED:mirror_heat",
                                    "reason": "合法 key 由服务端按 canonical facts 校验。",
                            },
                        ],
                        "recommendedUse": "引用前点开 evidence 核对。",
                    },
                    {
                        "targetTrimId": "premium",
                        "targetLabel": "两驱尊贵型 Premium-FWD",
                        "headline": "Premium 相比 Basic 主要升级泊车辅助和音响。",
                        "mainUpgrades": ["360 全景影像", "SONY 8 扬声器"],
                        "replacementsOrReductions": [],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                        "evidenceRefs": [
                            {
                                "section": "evidenceStatus",
                                "itemIndex": 0,
                                "evidenceKey": "warning",
                                "reason": "无输入证据 key 的引用应被丢弃。",
                            }
                        ],
                        "recommendedUse": "适合做配置亮点摘要。",
                    },
                ]
            }
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert captured["api_key"] == "test-key"
    assert captured["response_format"] == {"type": "json_object"}
    assert captured["messages"][0]["role"] == "system"
    assert "evidenceRefs" in captured["messages"][0]["content"]
    assert "evidenceKey 必须来自输入" in captured["messages"][0]["content"]
    assert "headline 必须使用“{target} 相比 {base}" in captured["messages"][0]["content"]
    assert "优先按业务维度归纳" in captured["messages"][0]["content"]
    assert "优先读取 categoryFacts" in captured["messages"][0]["content"]
    assert "倒车影像升级为 360 全景影像" in captured["messages"][0]["content"]
    assert "single_target_summary_rescue" in captured["messages"][0]["content"]
    assert "必须读取 context.compareScope" in captured["messages"][0]["content"]
    assert "sourceReviewHints" in captured["messages"][0]["content"]
    assert "sourceGroups" in captured["messages"][0]["content"]
    assert "cross_market" in captured["messages"][0]["content"]
    assert "multi_source" in captured["messages"][0]["content"]
    assert "own_vs_competitor" in captured["messages"][0]["content"]
    assert "missingSourceEvidenceCount" in captured["messages"][0]["content"]
    assert "sourceEvidenceSummary" in captured["messages"][0]["content"]
    assert "mergedCellExpandedCount" in captured["messages"][0]["content"]
    user_payload = json.loads(captured["messages"][1]["content"])
    assert user_payload["context"]["compareScope"]["sourceReviewHints"] == [
        "同国家同年款多来源：T19C MY ICE · Germany · 2026 存在 2 个来源 / 2 个上传人，AI 摘要需保留来源前提。"
    ]
    assert user_payload["context"]["compareScope"]["sourceGroups"][0]["sources"] == ["dealer-config.xlsx", "brand-site.html"]
    assert result["usage"] == {
        "provider": "deepseek",
        "model": "deepseek-chat",
        "status": "ok",
        "promptTokens": 12,
        "completionTokens": 8,
        "totalTokens": 20,
        "promptCacheHitTokens": 3,
        "promptCacheMissTokens": 9,
        "finishReason": "stop",
        "estimated": False,
    }
    assert [item["targetTrimId"] for item in result["summaries"]] == ["comfort", "premium"]
    assert result["summaries"][1]["mainUpgrades"] == ["360 全景影像", "SONY 8 扬声器"]
    assert result["summaries"][0]["evidenceRefs"] == [
        {
            "section": "mainUpgrades",
            "itemIndex": 0,
            "evidenceKey": "comfort:ADDED:mirror_heat",
            "featureCode": "mirror_heat",
            "category": "",
            "reason": "外后视镜加热来自配置差异事实。",
        },
        {
            "section": "mainUpgrades",
            "itemIndex": 1,
            "evidenceKey": "comfort:ADDED:seat_leather",
            "featureCode": "seat_leather",
            "category": "座椅面料",
            "reason": "座椅面料升级引用新增座椅面料配置。",
        },
        {
            "section": "replacementsOrReductions",
            "itemIndex": 0,
            "evidenceKey": "comfort:ADDED:mirror_heat",
            "featureCode": "mirror_heat",
            "category": "",
            "reason": "合法 key 由服务端按 canonical facts 校验。",
        },
    ]
    assert result["summaries"][1]["evidenceRefs"] == []
    assert result["summaries"][0]["evidenceBoundClaimCount"] == 3
    assert result["summaries"][0]["unsupportedEvidenceCount"] == 0
    assert result["summaries"][1]["evidenceBoundClaimCount"] == 0
    assert result["summaries"][1]["unsupportedEvidenceCount"] == 2
    assert "2 条 AI 结论未匹配到配置证据，不可直接引用。" in result["summaries"][1]["evidenceStatus"]


def test_compose_engineering_config_business_summary_does_not_infer_missing_evidence_refs(monkeypatch) -> None:
    payload = _payload()
    payload["targets"][0]["evidenceFacts"].append({
        "evidenceKey": "comfort:ADDED:wireless_charging",
        "featureCode": "wireless_charging",
        "featureName": "手机无线充电（50W）",
        "category": "舒适便利",
    })
    payload["targets"][1]["upgradeSignals"] = [
        {
            "dimension": "泊车辅助",
            "from": "倒车影像",
            "to": "360度高清全景影像",
            "fromEvidenceKey": "premium:REMOVED:rear_camera",
            "toEvidenceKey": "premium:ADDED:360_camera",
        }
    ]

    def fake_post_chat_completion(**_kwargs: Any) -> dict[str, Any]:
        return _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["增加手机无线充电（50W）"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                    },
                    {
                        "targetTrimId": "premium",
                        "targetLabel": "两驱尊贵型 Premium-FWD",
                        "headline": "Premium 相比 Basic 主要升级泊车辅助。",
                        "mainUpgrades": ["泊车辅助从倒车影像升级为360度高清全景影像"],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                    },
                ]
            }
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    result = composer.compose_engineering_config_business_summary(payload)

    assert result["summaries"][0]["evidenceRefs"] == []
    assert result["summaries"][1]["evidenceRefs"] == []
    assert result["summaries"][0]["unsupportedEvidenceCount"] == 1
    assert result["summaries"][1]["unsupportedEvidenceCount"] == 1
    assert "1 条 AI 结论未匹配到配置证据，不可直接引用。" in result["summaries"][0]["evidenceStatus"]
    assert "1 条 AI 结论未匹配到配置证据，不可直接引用。" in result["summaries"][1]["evidenceStatus"]


def test_compose_engineering_config_business_summary_falls_back_to_curl_transport(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def failing_post_chat_completion(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("python ssl transport failed for test-key")

    def fake_curl_run(
        args: list[str],
        *,
        input: str,
        text: bool,
        capture_output: bool,
        timeout: int,
        check: bool,
    ) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        captured["config"] = input
        captured["text"] = text
        captured["capture_output"] = capture_output
        captured["timeout"] = timeout
        captured["check"] = check
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps({
                "choices": [
                    {
                        "message": {
                            "content": json.dumps({
                                "summaries": [
                                    {
                                        "targetTrimId": "comfort",
                                        "targetLabel": "两驱舒适型 Comfort-FWD",
                                        "headline": "Comfort 相比 Basic 的主要升级来自后备 transport。",
                                        "mainUpgrades": ["外后视镜加热"],
                                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                                    },
                                    {
                                        "targetTrimId": "premium",
                                        "targetLabel": "两驱尊贵型 Premium-FWD",
                                        "headline": "Premium 相比 Basic 的主要升级来自后备 transport。",
                                        "mainUpgrades": ["360 全景影像"],
                                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                                    },
                                ]
                            }, ensure_ascii=False)
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 21,
                    "completion_tokens": 13,
                    "total_tokens": 34,
                },
            }, ensure_ascii=False),
            stderr="",
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", failing_post_chat_completion)
    monkeypatch.setattr(composer.subprocess, "run", fake_curl_run)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert "test-key" not in " ".join(captured["args"])
    assert "Authorization: Bearer test-key" in captured["config"]
    assert "data-binary" in captured["config"]
    assert captured["text"] is True
    assert captured["capture_output"] is True
    assert captured["check"] is False
    assert result["usage"]["status"] == "ok"
    assert result["usage"]["transportFallback"] == "curl"
    assert result["usage"]["promptTokens"] == 21
    assert result["usage"]["completionTokens"] == 13
    assert result["usage"]["totalTokens"] == 34
    assert result["summaries"][0]["headline"] == "Comfort 相比 Basic 的主要升级来自后备 transport。"


def test_compose_engineering_config_business_summary_does_not_substitute_rule_text_when_provider_fails(monkeypatch) -> None:
    def failing_post_chat_completion(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("provider unavailable for test-key")

    def failing_curl_run(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=["curl"], returncode=35, stdout="", stderr="ssl eof")

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", failing_post_chat_completion)
    monkeypatch.setattr(composer.subprocess, "run", failing_curl_run)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert result["usage"]["status"] == "failed"
    assert result["usage"]["providerStatus"] == "failed"
    assert "test-key" not in result["usage"]["fallbackReason"]
    assert result["summaries"] == []


def test_compose_engineering_config_business_summary_backfills_required_evidence_status(monkeypatch) -> None:
    def fake_post_chat_completion(**_kwargs: Any) -> dict[str, Any]:
        return _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["外后视镜加热"],
                        "evidenceStatus": ["当前摘要可作为配置差异初稿。"],
                    },
                    {
                        "targetTrimId": "premium",
                        "targetLabel": "两驱尊贵型 Premium-FWD",
                        "headline": "Premium 相比 Basic 主要升级泊车辅助和音响。",
                        "mainUpgrades": ["360 全景影像"],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源。"],
                    },
                ]
            }
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    result = composer.compose_engineering_config_business_summary(_payload())

    comfort_status = result["summaries"][0]["evidenceStatus"]
    premium_status = result["summaries"][1]["evidenceStatus"]
    assert comfort_status == [
        "2 项来自规则推断，不是 Excel 原文。",
        "2 项缺 source evidence，不能直接引用为确定卖点。",
        "1 项来自合并格展开，可引用但需保留来源边界。",
        "1 条 AI 结论未匹配到配置证据，不可直接引用。",
    ]
    assert premium_status == [
        "当前差异可继续从单元格追溯来源。",
        "2 项来自合并格展开，可引用但需保留来源边界。",
        "差异涉及多个 sheet/source，引用前需核对来源口径。",
        "1 条 AI 结论未匹配到配置证据，不可直接引用。",
    ]


def test_compose_engineering_config_business_summary_reuses_cached_payload(monkeypatch) -> None:
    call_count = 0

    def fake_post_chat_completion(**_kwargs: Any) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["外后视镜加热"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                    },
                    {
                        "targetTrimId": "premium",
                        "targetLabel": "两驱尊贵型 Premium-FWD",
                        "headline": "Premium 相比 Basic 主要升级泊车辅助和音响。",
                        "mainUpgrades": ["360 全景影像"],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                    },
                ]
            }
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    first = composer.compose_engineering_config_business_summary(_payload())
    second = composer.compose_engineering_config_business_summary(_payload())

    assert call_count == 1
    assert first["usage"].get("cacheHit") is None
    assert second["usage"]["cacheHit"] is True
    assert second["summaries"][1]["headline"] == "Premium 相比 Basic 主要升级泊车辅助和音响。"


def test_compose_engineering_config_business_summary_force_refresh_bypasses_cache(monkeypatch) -> None:
    captured_payloads: list[dict[str, Any]] = []

    def fake_post_chat_completion(**kwargs: Any) -> dict[str, Any]:
        captured_payloads.append(json.loads(kwargs["messages"][1]["content"]))
        call_number = len(captured_payloads)
        return _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": f"Comfort refresh {call_number}",
                        "mainUpgrades": ["外后视镜加热"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                    },
                    {
                        "targetTrimId": "premium",
                        "targetLabel": "两驱尊贵型 Premium-FWD",
                        "headline": f"Premium refresh {call_number}",
                        "mainUpgrades": ["360 全景影像"],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                    },
                ]
            }
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    first = composer.compose_engineering_config_business_summary(_payload())
    refresh_payload = {
        **_payload(),
        "context": {
            **_payload()["context"],
            "cacheControl": {"forceRefresh": True},
        },
    }
    refreshed = composer.compose_engineering_config_business_summary(refresh_payload)

    assert len(captured_payloads) == 2
    assert "cacheControl" not in captured_payloads[1]["context"]
    assert first["summaries"][1]["headline"] == "Premium refresh 1"
    assert refreshed["summaries"][1]["headline"] == "Premium refresh 2"


def test_compose_engineering_config_business_summary_keeps_missing_targets_visible(monkeypatch) -> None:
    def fake_post_chat_completion(**_kwargs: Any) -> dict[str, Any]:
        return _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["外后视镜加热"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                    }
                ]
            }
        )

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert [item["targetTrimId"] for item in result["summaries"]] == ["comfort", "premium"]
    assert result["summaries"][1]["targetLabel"] == "两驱尊贵型 Premium-FWD"
    assert result["summaries"][1]["headline"] == "两驱尊贵型 Premium-FWD 的 AI 摘要暂未返回，请以配置表和 source evidence 为准。"
    assert result["summaries"][1]["mainUpgrades"] == []
    assert result["summaries"][1]["evidenceRefs"] == []
    assert "LLM 未返回该目标摘要" in result["summaries"][1]["evidenceStatus"][0]


def test_compose_engineering_config_business_summary_retries_missing_targets(monkeypatch) -> None:
    captured_targets: list[list[str]] = []
    responses = [
        _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["外后视镜加热"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                    }
                ]
            }
        ),
        _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "premium",
                        "targetLabel": "两驱尊贵型 Premium-FWD",
                        "headline": "Premium 相比 Basic 主要升级泊车辅助和音响。",
                        "mainUpgrades": ["360 全景影像", "SONY 8 扬声器"],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                        "recommendedUse": "适合做配置亮点摘要。",
                    }
                ]
            }
        ),
    ]

    def fake_post_chat_completion(**kwargs: Any) -> dict[str, Any]:
        user_payload = json.loads(kwargs["messages"][1]["content"])
        captured_targets.append([target["targetTrimId"] for target in user_payload["targets"]])
        return responses.pop(0)

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert captured_targets == [["comfort", "premium"], ["premium"]]
    assert [item["targetTrimId"] for item in result["summaries"]] == ["comfort", "premium"]
    assert result["summaries"][1]["headline"] == "Premium 相比 Basic 主要升级泊车辅助和音响。"
    assert result["summaries"][1]["mainUpgrades"] == ["360 全景影像", "SONY 8 扬声器"]
    assert result["usage"]["promptTokens"] == 24
    assert result["usage"]["completionTokens"] == 16
    assert result["usage"]["totalTokens"] == 40


def test_compose_engineering_config_business_summary_rescues_single_missing_target(monkeypatch) -> None:
    captured_targets: list[list[str]] = []
    captured_reasons: list[str] = []
    captured_payloads: list[dict[str, Any]] = []
    captured_system_prompts: list[str] = []
    responses = [
        _provider_response(
            {
                "summaries": [
                    {
                        "targetTrimId": "comfort",
                        "targetLabel": "两驱舒适型 Comfort-FWD",
                        "headline": "Comfort 相比 Basic 主要增加舒适便利配置。",
                        "mainUpgrades": ["外后视镜加热"],
                        "evidenceStatus": ["2 项来自规则推断，不是 Excel 原文"],
                    }
                ]
            }
        ),
        _provider_response({"summaries": []}),
        _provider_response(
            {
                "summaries": [
                    {
                        "headline": "Premium 相比 Basic 主要升级泊车辅助和音响。",
                        "mainUpgrades": ["360 全景影像", "SONY 8 扬声器"],
                        "evidenceStatus": ["当前差异可继续从单元格追溯来源"],
                        "recommendedUse": "适合做配置亮点摘要。",
                    }
                ]
            }
        ),
    ]

    def fake_post_chat_completion(**kwargs: Any) -> dict[str, Any]:
        captured_system_prompts.append(str(kwargs["messages"][0]["content"]))
        user_payload = json.loads(kwargs["messages"][1]["content"])
        captured_payloads.append(user_payload)
        captured_targets.append([target["targetTrimId"] for target in user_payload["targets"]])
        captured_reasons.append(str(user_payload.get("context", {}).get("retryReason") or ""))
        return responses.pop(0)

    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setattr(composer, "_post_chat_completion", fake_post_chat_completion)

    result = composer.compose_engineering_config_business_summary(_payload())

    assert captured_targets == [["comfort", "premium"], ["premium"], ["premium"]]
    assert captured_reasons == ["", "previous_llm_response_omitted_targets", "single_target_summary_rescue"]
    assert "单目标业务摘要 composer" in captured_system_prompts[2]
    assert "summaries 必须且只能有 1 项" in captured_system_prompts[2]
    assert captured_payloads[2]["targets"] == [
        {
            "targetTrimId": "premium",
            "targetLabel": "两驱尊贵型 Premium-FWD",
            "differenceCounts": {"inferred": 0, "totalDifference": 12},
            "sourceEvidenceSummary": {
                "differenceCount": 12,
                "withSourceEvidenceCount": 12,
                "missingSourceEvidenceCount": 0,
                "inferredCount": 0,
                "unknownCount": 0,
                "mergedCellExpandedCount": 2,
                "sourceSheetNames": ["T19C MY ICE", "T19C BEV"],
            },
            "evidence": {
                "inferredCount": 0,
                "warning": "当前差异可继续从单元格追溯来源。",
            },
        }
    ]
    assert [item["targetTrimId"] for item in result["summaries"]] == ["comfort", "premium"]
    assert result["summaries"][1]["targetLabel"] == "两驱尊贵型 Premium-FWD"
    assert result["summaries"][1]["headline"] == "Premium 相比 Basic 主要升级泊车辅助和音响。"
    assert result["summaries"][1]["mainUpgrades"] == ["360 全景影像", "SONY 8 扬声器"]
    assert result["usage"]["promptTokens"] == 36
    assert result["usage"]["completionTokens"] == 24
    assert result["usage"]["totalTokens"] == 60
