import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { CountryChatGroundedAnswer } from "../../components/CountryChatGroundedAnswer";
import { CountryChatPendingMessage } from "../../components/CountryChatPendingMessage";
import type { CountryChatTranscriptMessage } from "../../contexts/CountryChatContext";

function buildAssistantMessage(): CountryChatTranscriptMessage {
  return {
    id: "assistant-1",
    role: "assistant",
    country: "瑞典",
    content: "RAV4 HEV 目前主销仍然集中在中高配。\n\n价格带主要落在 42-46 万 SEK。",
    answerMode: "grounded-direct",
    intentRoute: "precise-lookup",
    extractedParams: {
      model: "RAV4",
      powertrain: "HEV",
    },
    grounding: {
      strategyLabel: "CurrentPrice 直查",
      summary: "这次回答优先读取当前价格样本与国家快照。",
      answerPath: {
        routeTrigger: "参数线索推导：RAV4 + HEV + 价格条件已经收敛到当前版型查询。",
        evidenceUsed: ["Snapshot", "CurrentPrice"],
        steps: [
          "先锁定具体车型 / trim / 价格条件。",
          "参数线索推导：RAV4 + HEV + 价格条件已经收敛到当前版型查询。",
          "读取 Snapshot / CurrentPrice。",
          "在已验证证据上直接组装结论。",
        ],
      },
      reasoningNotes: [
        "参数线索推导：RAV4 + HEV + 价格条件已经收敛到当前版型查询。",
      ],
      layers: [
        {
          kind: "snapshot",
          label: "Snapshot",
          detail: "Market base map",
          freshness: "2026-04-19T10:00:00+00:00",
        },
        {
          kind: "dynamic",
          label: "CurrentPrice",
          detail: "Current trim rows",
        },
      ],
      keyFindings: [
        "中高配仍然是主销版本。",
        "价格带集中在 42-46 万 SEK。",
      ],
      evidenceTables: [
        {
          title: "Trim Price",
          columns: ["Trim", "MSRP"],
          rows: [["Active", "429,900 SEK"]],
        },
        {
          title: "Feature Diff",
          columns: ["Feature", "Delta", "链接"],
          rows: [["HUD", "Optional", "https://example.test/rav4"]],
        },
        {
          title: "Should Be Hidden In Compact",
          columns: ["A"],
          rows: [["B"]],
        },
      ],
      trust: {
        confidence: "high",
        evidenceSufficiency: "strong",
        evidenceScore: 92,
        routeRationale: "当前价格类精确查询必须命中 DB price state，不能只靠模型概述。",
        missingFacts: [],
        sourceCoverage: {
          requiredReady: 3,
          requiredTotal: 3,
          prefetchedCount: 1,
        },
      },
    },
    executionPlan: {
      orchestrationMode: "prefetch-first",
      answerStrategy: "snapshot-first",
      prefetchedToolNames: ["query_local_wiki"],
      allowedToolNames: ["query_local_wiki", "query_news_wiki"],
      sourcePlan: [
        {
          key: "snapshot-core",
          label: "Country snapshot",
          required: true,
          status: "ready",
          reason: "先锁国家、周期、基础销量结构。",
        },
        {
          key: "db-price-state",
          label: "DB current price state",
          required: true,
          status: "ready",
          reason: "当前价格类精确查询必须命中 DB price state。",
        },
        {
          key: "vehicle-wiki",
          label: "Vehicle wiki facts",
          required: false,
          status: "prefetched",
          toolName: "query_local_wiki",
          reason: "点名车型的精确查询优先预取本地 wiki。",
        },
      ],
    },
  };
}

describe("CountryChatGroundedAnswer", () => {
  it("renders the answer-first grounded structure in compact mode", () => {
    const markup = renderToStaticMarkup(
      <CountryChatGroundedAnswer message={buildAssistantMessage()} compact />,
    );

    expect(markup).toContain("copilot-grounded-answer is-compact");
    expect(markup).toContain("直接回答");
    expect(markup).toContain("关键结论");
    expect(markup).toContain("回答路径");
    expect(markup).toContain("可信度");
    expect(markup).toContain("执行计划");
    expect(markup).toContain("高可信");
    expect(markup).toContain("证据充分");
    expect(markup).toContain("已预取 query_local_wiki");
    expect(markup).toContain("数据依据");
    expect(markup).toContain("数据来源层");
    expect(markup).toContain("瑞典");
    expect(markup).toContain("RAV4");
    expect(markup).toContain("HEV");
    expect(markup).toContain("CurrentPrice 直查");
    expect(markup).toContain("参数线索推导：RAV4 + HEV + 价格条件已经收敛到当前版型查询。");
    expect(markup).toContain("在已验证证据上直接组装结论。");
    expect(markup).toContain("Trim Price");
    expect(markup).toContain("Feature Diff");
    expect(markup).toContain("href=\"https://example.test/rav4\"");
    expect(markup).toContain("打开来源");
    expect(markup).not.toContain("Should Be Hidden In Compact");
  });

  it("falls back to the plain body for non-grounded user turns", () => {
    const markup = renderToStaticMarkup(
      <CountryChatGroundedAnswer
        message={{
          id: "user-1",
          role: "user",
          content: "帮我看一下 RAV4 HEV。",
        }}
      />,
    );

    expect(markup).toContain("copilot-message-body");
    expect(markup).toContain("帮我看一下 RAV4 HEV。");
    expect(markup).not.toContain("copilot-grounded-answer");
  });
});

describe("CountryChatPendingMessage", () => {
  it("renders the loading plan for model-performance questions", () => {
    const markup = renderToStaticMarkup(
      <CountryChatPendingMessage question="EX40为什么卖得好" compact />,
    );

    expect(markup).toContain("copilot-loading is-compact");
    expect(markup).toContain("准备数据中");
    expect(markup).toContain("翻阅销量档案中...");
    expect(markup).toContain("copilot-loading-step");
  });
});
