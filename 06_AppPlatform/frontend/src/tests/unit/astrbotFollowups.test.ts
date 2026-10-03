import { describe, expect, it } from "vitest";

import { normalizeAgentFollowUps, readEvidencePackage, readQualityScore } from "../../features/astrbot/AstrBotWorkbenchPage";

describe("normalizeAgentFollowUps", () => {
  it("keeps legacy string follow-ups clickable", () => {
    const result = normalizeAgentFollowUps([
      "继续看瑞典 BEV 车型排名。",
      "生成一页汇报框架。",
    ]);

    expect(result).toHaveLength(2);
    expect(result[0].label).toBe("继续看瑞典 BEV 车型排名。");
    expect(result[0].question).toBe("继续看瑞典 BEV 车型排名。");
    expect(result[0].intent).toBe("legacy");
  });

  it("reads structured follow-ups and sorts by priority", () => {
    const result = normalizeAgentFollowUps([
      {
        id: "fu_report",
        label: "生成汇报",
        question: "生成一页定位定价汇报框架。",
        intent: "report",
        expectedTools: ["build_market_chart"],
        expectedOutput: "report",
        priority: 2,
      },
      {
        id: "fu_compare",
        label: "看竞品价格走廊",
        question: "对比 J7 HEV 和核心竞品 MSRP。",
        intent: "compare",
        reason: "验证价格带是否合理。",
        expectedTools: ["query_msrp_pricing"],
        expectedOutput: "table",
        priority: 1,
      },
    ]);

    expect(result).toHaveLength(2);
    expect(result[0].id).toBe("fu_compare");
    expect(result[0].label).toBe("看竞品价格走廊");
    expect(result[0].question).toContain("J7 HEV");
    expect(result[0].expectedTools).toEqual(["query_msrp_pricing"]);
    expect(result[1].expectedOutput).toBe("report");
  });
});

describe("AstrBot quality loop parsers", () => {
  it("reads evidence packages for the analysis path panel", () => {
    const result = readEvidencePackage({
      evidenceId: "evpkg_test",
      intent: "pricing_analysis",
      country: "Sweden",
      confidence: "medium",
      toolResults: [
        {
          toolName: "query_msrp_pricing",
          success: true,
          rowCount: 2,
          sourceType: "postgres",
          summary: "pricing rows",
          keyFindings: ["MSRP: 300000"],
          evidenceRefs: [{ refId: "ev_1", label: "MSRP", value: 300000, unit: "currency" }],
        },
      ],
      missingEvidence: [{ name: "leasing_monthly", reason: "not queried", impact: "weakens_answer" }],
    });

    expect(result?.evidenceId).toBe("evpkg_test");
    expect(result?.toolResults[0].sourceType).toBe("postgres");
    expect(result?.toolResults[0].evidenceRefs[0].value).toBe(300000);
    expect(result?.missingEvidence[0].impact).toBe("weakens_answer");
  });

  it("reads deterministic quality scores", () => {
    const score = readQualityScore({
      intentScore: 1,
      toolScore: 0.5,
      groundingScore: 1,
      followUpScore: 1,
      safetyScore: 1,
      totalScore: 0.875,
      failures: ["missing_required_tools:compare_competitive_set"],
    });

    expect(score?.totalScore).toBe(0.875);
    expect(score?.failures[0]).toContain("missing_required_tools");
  });
});
