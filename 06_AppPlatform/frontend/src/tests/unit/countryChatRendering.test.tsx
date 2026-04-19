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
          columns: ["Feature", "Delta"],
          rows: [["HUD", "Optional"]],
        },
        {
          title: "Should Be Hidden In Compact",
          columns: ["A"],
          rows: [["B"]],
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
    expect(markup).toContain("思考链");
    expect(markup).toContain("数据依据");
    expect(markup).toContain("数据来源层");
    expect(markup).toContain("瑞典");
    expect(markup).toContain("RAV4");
    expect(markup).toContain("HEV");
    expect(markup).toContain("CurrentPrice 直查");
    expect(markup).toContain("Trim Price");
    expect(markup).toContain("Feature Diff");
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
    expect(markup).toContain("正在做车型胜因分析");
    expect(markup).toContain("1. 锁定车型与细分页 scope");
    expect(markup).toContain("2. 读取 Market Scan 排名与份额");
  });
});