// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  AnalysisPathPanel,
  AgentMessageStatus,
  AstrBotMarkdownAnswer,
  AnswerLimitationsPanel,
  AnswerCopyActions,
  ChartFallbackCard,
  EvidenceBackedLead,
  EvidenceGapPanel,
  QuickActionCards,
  StreamingArtifactPreview,
  VisualArtifactsDeck,
  buildAstrBotCitationKey,
  formatAstrBotAnswerRefinementStatus,
  formatAstrBotAnswerStreamStatus,
  formatSessionDisplayMeta,
  formatSessionDisplayTitle,
  formatAstrBotTokenStreamStatus,
  formatUserFacingAnswerText,
  inferAstrBotStreamPhase,
  buildPptBlockCopyText,
  buildChartFallbackCard,
  buildToolCoverageSummary,
  formatAstrBotStreamingPlaceholder,
  getAstrBotQuickActionQuestion,
  inferAstrBotQuestionCountry,
  resolveAstrBotRequestCountry,
  readEvidencePackage,
  parseAstrBotSseFrames,
  selectAstrBotUserTakeaways,
  selectUniqueAstrBotCitations,
  type ChartFallbackCardData,
  type VisualArtifact,
} from "../../features/astrbot/AstrBotWorkbenchPage";
import {
  buildBusinessReadinessGate,
  buildBusinessBaselineActionPlan,
  buildBusinessReadinessHandoffText,
  buildBusinessReviewWorkbench,
  buildBusinessScoringSheetText,
  buildBusinessArtifactPreviews,
  buildBlockingReadinessItems,
  buildCodexDraftScorePrefill,
  buildCodexDraftTriage,
  buildEvidenceRepairDisplayState,
  buildEvidenceRepairQueue,
  buildEvidenceRepairOverview,
  buildEvidenceRepairPlanText,
  buildSourceRepairBacklogPlanText,
  buildJudgeEnvTemplate,
  canSaveBusinessScore,
  businessReviewPriorityReason,
  isBusinessScoreReadyRecord,
  evidenceRepairReasonLines,
  failureTagBreakdown,
  findNextReviewComparisonId,
  filterBusinessReviewRecords,
  draftRecordsForCodexAcceptance,
  needsBusinessDecisionReview,
  normalizeEvidenceRepairQueue,
  normalizeSourceRepairBacklog,
  parseBusinessScoringSheetDrafts,
  parseReferenceJudgeScoreDrafts,
  recordsForImportedManualSave,
  scoreProgressText,
  scoreCompletionText,
  acceptedCodexDraftNotes,
  BUSINESS_SCORE_RUBRIC,
  BUSINESS_SCORE_GUIDE_SUMMARY,
  BUSINESS_QUICK_VERDICTS,
  BUSINESS_QUICK_FAILURE_TAGS,
  businessDiagnosticLabel,
  businessScoreRubricLabel,
  businessScoreShortLabel,
} from "../../features/astrbot/components/AstrBotEvalPanel";
import type { EvalCodexReviewNote, EvalCodexReviewNotesResponse, EvalSideBySideRecord, EvalSideBySideSummary } from "../../features/astrbot/astrbotConfig";

afterEach(cleanup);

function buildEvidencePackage(): NonNullable<Parameters<typeof buildChartFallbackCard>[2]> {
  return {
    evidenceId: "evpkg_chart",
    intent: "market_overview",
    country: "Sweden",
    confidence: "high",
    missingEvidence: [{ name: "monthly trend series", reason: "not returned", impact: "weakens_answer" }],
    toolResults: [
      {
        toolName: "query_country_snapshot",
        success: true,
        rowCount: 3,
        sourceType: "jato_parquet",
        summary: "snapshot",
        keyFindings: ["BEV is largest"],
        evidenceRefs: [
          { refId: "bev", label: "BEV", value: 25235, unit: "units" },
          { refId: "phev", label: "PHEV", value: 15028, unit: "units" },
          { refId: "ice", label: "ICE", value: 8129, unit: "units" },
        ],
      },
    ],
    insightCards: [],
  };
}

describe("AstrBot UX alignment helpers", () => {
  function buildReadinessSummary(failureTagCounts: Record<string, number>): EvalSideBySideSummary {
    return {
      count: 21,
      pendingHumanScoring: 21,
      pendingBaselineScoring: 21,
      pendingReplacementBaselineScoring: 21,
      scoredCount: 0,
      baselineScoredCount: 0,
      replacementBaselineScoredCount: 0,
      baselineSourceCounts: {},
      replacementBaselineSourceCounts: {},
      astrbotErrorCount: 0,
      countryCopilotErrorCount: 0,
      avgAstrBotComposite: 0.76,
      astrbotWinRate: 0,
      failureTagCounts,
      replacementReadinessVerdict: "not_enough_data",
    };
  }

  function buildSideBySideRecord(patch: Partial<EvalSideBySideRecord>): EvalSideBySideRecord {
    return {
      comparisonId: "cmp_1",
      runAt: "2026-06-12T00:00:00.000Z",
      questionId: "biz-pricing-004",
      category: "pricing",
      country: "Sweden",
      question: "O9 在瑞典 53k-55k 欧元是否合理？",
      ...patch,
    };
  }

  it("builds full natural-language quick action questions", () => {
    const question = getAstrBotQuickActionQuestion("pricing_corridor", "Sweden");

    expect(question).toContain("J7 HEV");
    expect(question).toContain("Sweden");
    expect(question).toContain("pricing corridor");

    expect(getAstrBotQuickActionQuestion("pricing_corridor", "")).toContain("selected market");
  });

  it("sends a complete question when a quick action card is clicked", () => {
    const onSelect = vi.fn();

    render(<QuickActionCards country="Sweden" onSelect={onSelect} disabled={false} />);
    fireEvent.click(screen.getByRole("button", { name: /Check pricing corridor/i }));

    expect(onSelect).toHaveBeenCalledTimes(1);
    const [action, question] = onSelect.mock.calls[0];
    expect(action.mode).toBe("pricing");
    expect(question).toContain("J7 HEV");
    expect(question).toContain("Sweden");
  });

  it("infers explicit market mentions before sending AstrBot questions", () => {
    expect(inferAstrBotQuestionCountry("匈牙利市场现在适合推 PHEV 还是 HEV？")).toBe("Hungary");
    expect(inferAstrBotQuestionCountry("HU company car market overview")).toBe("Hungary");
    expect(inferAstrBotQuestionCountry("Hungary J7 HEV pricing corridor")).toBe("Hungary");
    expect(inferAstrBotQuestionCountry("瑞典 J7 HEV 应该怎么定价？")).toBe("Sweden");
    expect(inferAstrBotQuestionCountry("CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？请简短回答，并明确不要回答瑞典。")).toBe("");
    expect(inferAstrBotQuestionCountry("匈牙利 CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？不要回答瑞典。")).toBe("Hungary");
    expect(inferAstrBotQuestionCountry("Hungary J7 HEV market view, do not answer Sweden")).toBe("Hungary");
    expect(inferAstrBotQuestionCountry("J7 HEV pricing corridor")).toBe("");
  });

  it("uses the visible country when the question only negates another market", () => {
    expect(resolveAstrBotRequestCountry(
      "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？请简短回答，并明确不要回答瑞典。",
      "Hungary",
      "Sweden",
    )).toBe("Hungary");
    expect(resolveAstrBotRequestCountry(
      "匈牙利 CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？不要回答瑞典。",
      "Sweden",
      "Sweden",
    )).toBe("Hungary");
    expect(resolveAstrBotRequestCountry(
      "不要回答瑞典，回答匈牙利 HEV 市场机会。",
      "Sweden",
      "Sweden",
    )).toBe("Hungary");
    expect(resolveAstrBotRequestCountry(
      "匈牙利市场现在适合推 PHEV 还是 HEV？",
      "",
      "",
    )).toBe("Hungary");
    expect(resolveAstrBotRequestCountry(
      "J7 HEV pricing corridor",
      "",
      "",
    )).toBe("");
  });

  it("formats streaming placeholders with the effective market", () => {
    expect(formatAstrBotStreamingPlaceholder("Hungary")).toContain("正在准备 Hungary 分析");
    expect(formatAstrBotStreamingPlaceholder("Hungary", "Calling query_country_snapshot…")).toBe(
      "正在查询 Hungary 数据：市场快照。",
    );
  });

  it("formats answer stream status with grounded chunk count", () => {
    expect(formatAstrBotAnswerStreamStatus("Hungary", 12)).toBe("Writing Hungary answer in 12 grounded chunks…");
    expect(formatAstrBotAnswerStreamStatus("", 0)).toBe("Writing selected market grounded answer…");
    expect(formatAstrBotAnswerRefinementStatus("Sweden", 6)).toBe("Refining Sweden final answer in 6 grounded chunks…");
    expect(formatAstrBotTokenStreamStatus("Hungary", 3)).toBe("Streaming Hungary answer · 3 chunks received…");
  });

  it("labels visible SSE phases for user chat", () => {
    expect(inferAstrBotStreamPhase(true, "Calling query_country_snapshot…")).toBe("正在查数据");
    expect(inferAstrBotStreamPhase(true, "Writing Hungary answer in 4 grounded chunks…")).toBe("生成业务结论");
    expect(inferAstrBotStreamPhase(true, "Refining Sweden final answer in 6 grounded chunks…")).toBe("完善最终结论");
    expect(inferAstrBotStreamPhase(true, "Streaming Hungary answer · 3 chunks received…")).toBe("正在输出结论");
    expect(inferAstrBotStreamPhase(true, "Still checking Hungary evidence · 9s · waiting for tools or first answer chunk…")).toBe("等待首段答案");
    expect(inferAstrBotStreamPhase(false, "Analysis complete")).toBe("证据路径");
  });

  it("keeps a compact evidence path visible after streamed answers complete", () => {
    render(
      <AgentMessageStatus
        isStreaming={false}
        country="Sweden"
        toolCalls={["query_country_snapshot"]}
      />,
    );

    expect(screen.getByText("证据路径")).toBeTruthy();
    expect(screen.getByText("完成")).toBeTruthy();
    expect(screen.getByText(/Sweden · 使用 1 个工具/)).toBeTruthy();
  });

  it("renders a compact data basis between answer and artifacts", () => {
    render(
      <EvidenceBackedLead text="已查数据：匈牙利 HEV share = 18.2%。业务判断：先转成动力路线和产品动作。" />,
    );

    expect(screen.getByLabelText("Evidence-backed lead")).toBeTruthy();
    expect(screen.getByText("数据依据")).toBeTruthy();
    expect(screen.getByText(/匈牙利 HEV share = 18.2%/)).toBeTruthy();
  });

  it("removes repeated conclusion labels from user-facing answer text", () => {
    expect(formatUserFacingAnswerText("直接结论：匈牙利 HEV 产品线机会入口已有证据支撑。")).toBe("匈牙利 HEV 产品线机会入口已有证据支撑。");
    expect(formatUserFacingAnswerText("结论：先验证 HEV + SUV A0/A。")).toBe("先验证 HEV + SUV A0/A。");
  });

  it("turns composer sections into readable markdown paragraphs", () => {
    const formatted = formatUserFacingAnswerText(
      "J7 HEV 应保持更强价格吸引力。 市场结构证据：HEV 2WD 85.9%。 价格证据：当前 MSRP 待补。 下一步执行：建立价格矩阵。",
    );

    expect(formatted).toContain("\n\n**市场结构证据：** HEV 2WD 85.9%。");
    expect(formatted).toContain("\n\n**价格证据：** 当前 MSRP 待补。");
    expect(formatted).toContain("\n\n**下一步执行：** 建立价格矩阵。");
  });

  it("renders streamed AstrBot answers as safe markdown prose without an answer card", () => {
    const { container, rerender } = render(
      <AstrBotMarkdownAnswer
        text=""
        isStreaming
        streamPlaceholder="正在查询 Hungary 数据：市场快照。"
      />,
    );

    const answer = container.querySelector(".astrbot-markdown-answer");
    expect(answer).toBeTruthy();
    expect(answer?.classList.contains("is-streaming")).toBe(true);
    expect(screen.getByText("正在查询 Hungary 数据：市场快照。")).toBeTruthy();
    expect(container.querySelector(".astrbot-executive-answer")).toBeNull();

    rerender(
      <AstrBotMarkdownAnswer
        text={"## 结论\n\n**2WD** 应作为主销。\n\n- HEV 2WD：89.5%\n- HEV 4WD：9.9%"}
      />,
    );

    expect(screen.getByRole("heading", { name: "结论" })).toBeTruthy();
    expect(screen.getByText("2WD", { selector: "strong" })).toBeTruthy();
    expect(screen.getByText(/HEV 2WD：89.5%/)).toBeTruthy();
    expect(container.querySelector(".astrbot-executive-answer")).toBeNull();
  });

  it("shows the evidence-answer-artifact preview during user-mode streaming", () => {
    render(
      <StreamingArtifactPreview
        isStreaming
        statusText="Streaming Hungary answer · 3 chunks received…"
        toolCalls={["query_country_snapshot", "build_market_chart"]}
      />,
    );

    expect(screen.getByLabelText("Streaming output preview")).toBeTruthy();
    expect(screen.getByText("证据")).toBeTruthy();
    expect(screen.getByText("已收到 2 个工具结果")).toBeTruthy();
    expect(screen.getByText("结论")).toBeTruthy();
    expect(screen.getByText("正在输出有证据支撑的结论")).toBeTruthy();
    expect(screen.getByText("图表/表格")).toBeTruthy();
  });

  it("renders visual artifact summary cards as jump links into the deck", () => {
    const artifacts: VisualArtifact[] = [
      {
        id: "artifact_pricing_corridor_table",
        type: "table",
        title: "Pricing corridor table",
        data: { rows: [{ model: "J7 HEV", price: "34,720 EUR" }] },
        spec: { columns: ["model", "price"] },
        sourceEvidenceRefs: ["ev_price_1"],
      },
      {
        id: "artifact_report_block",
        type: "report_block",
        title: "PPT-ready block",
        data: { title: "J7 HEV pricing", keyMessage: "Core corridor", evidence: ["MSRP needs validation"], productImplication: "High trim push", nextAction: "Validate MSRP" },
        sourceEvidenceRefs: ["ev_report_1"],
      },
    ];

    render(<VisualArtifactsDeck artifacts={artifacts} deckId="msg_1" />);

    const pricingLink = screen.getByRole("link", { name: "跳转到定价走廊表" });
    expect(pricingLink.getAttribute("href")).toBe("#astrbot-artifact-msg_1-artifact_pricing_corridor_table");
    expect(document.getElementById("astrbot-artifact-msg_1-artifact_pricing_corridor_table")).toBeTruthy();
    expect(screen.getByText("可复用图表、表格和汇报块")).toBeTruthy();
  });

  it("keeps the primary user artifacts visible and collapses supplemental output", () => {
    const artifacts: VisualArtifact[] = [
      {
        id: "artifact_metric_cards",
        type: "metric_cards",
        title: "Key metrics",
        data: { rows: [{ label: "HEV", value: 2687, unit: "units" }] },
        sourceEvidenceRefs: ["ev_hev"],
      },
      {
        id: "artifact_market_structure_chart",
        type: "chart",
        title: "Market structure chart",
        data: [{ label: "SUV A", value: 3535 }],
        spec: { chartType: "bar", xField: "label", yField: "value" },
        sourceEvidenceRefs: ["ev_segment"],
      },
      {
        id: "artifact_report_block",
        type: "report_block",
        title: "PPT-ready block",
        data: { title: "Hungary J7", keyMessage: "Validate HEV entry", evidence: [] },
        sourceEvidenceRefs: ["ev_report"],
      },
    ];

    render(<VisualArtifactsDeck artifacts={artifacts} compact />);

    expect(screen.getByLabelText("关键指标")).toBeTruthy();
    expect(screen.getByLabelText("市场结构图")).toBeTruthy();
    expect(screen.getByLabelText("PPT 汇报块")).toBeTruthy();
    const supplemental = screen.getByText("更多证据输出").closest("details") as HTMLDetailsElement | null;
    expect(supplemental?.open).toBe(false);
  });

  it("formats readable conversation session labels instead of only technical ids", () => {
    const session = {
      sessionId: "sess_hungary_context",
      startedAt: "2026-06-19T10:00:00.000Z",
      lastActivityAt: "2026-06-19T10:05:00.000Z",
      turnCount: 2,
      country: "Hungary",
      latestQuestion: "匈牙利 J7 HEV 是否值得继续验证？请简短回答。",
      answerStatus: "answered",
      confidence: "high",
      toolCalls: ["query_country_snapshot", "external_research"],
    };

    expect(formatSessionDisplayTitle(session)).toBe("Hungary · 匈牙利 J7 HEV 是否值得继续验证？请简短回答。");
    expect(formatSessionDisplayMeta(session)).toContain("2 turns");
    expect(formatSessionDisplayMeta(session)).toContain("answered · high");
    expect(formatSessionDisplayMeta(session)).toContain("2 tools");
  });

  it("parses AstrBot SSE frames while preserving incomplete rest", () => {
    const first = parseAstrBotSseFrames([
      "data: {\"_event\":\"thinking\",\"message\":\"Planning\"}",
      "",
      "data: {\"_event\":\"token\",\"text\":\"Hello\"}",
      "",
      "data: {\"_event\":\"token\",",
    ].join("\n"));

    expect(first.events).toEqual([
      { _event: "thinking", message: "Planning" },
      { _event: "token", text: "Hello" },
    ]);
    expect(first.rest).toBe("data: {\"_event\":\"token\",");

    const second = parseAstrBotSseFrames(`${first.rest}"text":" world"}\n\n`);
    expect(second.events).toEqual([{ _event: "token", text: " world" }]);
    expect(second.rest).toBe("");
  });

  it("prioritizes compact key takeaways over PPT report bullets in chat view", () => {
    expect(selectAstrBotUserTakeaways({
      keyTakeaways: ["compact market judgment"],
      reportReadyBullets: ["long PPT paragraph"],
      answerBullets: ["fallback bullet"],
    })).toEqual(["compact market judgment"]);
    expect(selectAstrBotUserTakeaways({
      keyTakeaways: [],
      reportReadyBullets: ["report bullet"],
      answerBullets: ["fallback bullet"],
    })).toEqual(["fallback bullet"]);
  });

  it("localizes backend takeaway labels before showing them in user chat", () => {
    expect(selectAstrBotUserTakeaways({
      keyTakeaways: [
        "Key metrics：HEV 2,687 units / SUV A0 7,303 units",
        "Powertrain mix：HEV 2WD 89.5% / HEV 4WD 9.9%",
        "Top models：当前工具未返回匈牙利市场车型级销量/价格记录",
      ],
      reportReadyBullets: [],
      answerBullets: [],
    })).toEqual([
      "关键指标：HEV 2,687 units / SUV A0 7,303 units",
      "动力结构：HEV 2WD 89.5% / HEV 4WD 9.9%",
      "车型证据：当前工具未返回匈牙利市场车型级销量/价格记录",
    ]);
  });

  it("filters evidence ref noise out of user takeaways", () => {
    expect(selectAstrBotUserTakeaways({
      keyTakeaways: [
        "Key metrics：Tax increases and regulatory changes in the automotive market from 2026 | Ayvens Hungary.claim / Tax increases and regulatory changes in the automotive market from 2026 | Ayvens Hungary.rank",
        "Top models：Incentives and Legislation | European Alternative Fuels Observatory.claim",
      ],
      reportReadyBullets: ["Key message：Hungary should stay in validation mode before a J7 HEV launch decision."],
      answerBullets: ["证据有限但可推进：先补 HEV 份额和 SUV A0/A 结构。"],
    })).toEqual(["证据有限但可推进：先补 HEV 份额和 SUV A0/A 结构。"]);
  });

  it("prioritizes evidence-limited business bullets when no compact takeaways exist", () => {
    expect(selectAstrBotUserTakeaways({
      keyTakeaways: [],
      reportReadyBullets: [],
      answerBullets: [
        "结论：O9 target price needs validation.",
        "证据：已有部分价格走廊。",
        "产品经理判断：先看竞品走廊和配置价值。",
        "证据有限但可推进：当前可先做目标价/价格走廊的场景判断。",
        "下一步动作：补齐官方 MSRP 和月供。",
      ],
    })).toEqual([
      "证据有限但可推进：当前可先做目标价/价格走廊的场景判断。",
      "产品经理判断：先看竞品走廊和配置价值。",
      "下一步动作：补齐官方 MSRP 和月供。",
    ]);
  });

  it("filters repeated lead text and user instructions out of key takeaways", () => {
    expect(selectAstrBotUserTakeaways({
      keyTakeaways: [
        "直接结论：匈牙利的业务分析现在还不能给确定数字，因为缺少内部市场快照、HEV 销量/份额和车型结构证据。请简短回答。",
        "分析对象：匈牙利 J7 HEV 是否值得继续验证？请简短回答。",
      ],
      reportReadyBullets: ["Key message：Hungary stays in validation mode until HEV SUV evidence is available."],
      answerBullets: ["下一步动作：补齐 HEV 销量、SUV A0/A 结构和竞品价格。"],
    })).toEqual(["下一步动作：补齐 HEV 销量、SUV A0/A 结构和竞品价格。"]);
  });

  it("keeps Analysis Path collapsed until the user expands it", () => {
    render(
      <AnalysisPathPanel
        evidencePlan={{
          intent: "market_overview",
          requiredTools: ["query_country_snapshot"],
          allowedTools: ["query_country_snapshot"],
          mustHaveEvidence: ["market_snapshot"],
        }}
        evidencePackage={buildEvidencePackage()}
        qualityScore={{
          intentScore: 1,
          toolScore: 1,
          groundingScore: 1,
          followUpScore: 1,
          safetyScore: 1,
          executiveConclusionScore: 1,
          businessImplicationScore: 1,
          actionabilityScore: 1,
          evidenceAlignmentScore: 1,
          reportReadinessScore: 1,
          businessSynthesisScore: 1,
          totalScore: 1,
          failures: [],
        }}
        toolCalls={["query_country_snapshot"]}
      />,
    );

    const details = screen.getByText("Analysis Path").closest("details") as HTMLDetailsElement | null;
    expect(details?.open).toBe(false);

    fireEvent.click(screen.getByText("Analysis Path"));
    expect(details?.open).toBe(true);
    expect(screen.getAllByText("query_country_snapshot").length).toBeGreaterThan(0);
    expect(screen.getByText("Tool Coverage")).toBeTruthy();
    expect(screen.getByText("covered")).toBeTruthy();
  });

  it("shows parallel evidence periods and same-scope conflicts in Analysis Path", () => {
    const evidencePackage = readEvidencePackage({
      ...buildEvidencePackage(),
      toolResults: [
        {
          ...buildEvidencePackage().toolResults[0],
          evidenceRefs: [
            {
              refId: "hev_month",
              label: "contextSnapshot.crossTabs.driveByFuel.HEV.sales",
              value: 1946,
              unit: "units",
              periodType: "month",
              periodLabel: "2026-03 当月",
              periodStart: "2026-03",
              periodEnd: "2026-03",
            },
          ],
        },
      ],
      scopeDiagnostics: {
        parallelScopes: [
          {
            metric: "powertrain:HEV:sales",
            scopes: [
              {
                periodType: "month",
                periodLabel: "2026-03 当月",
                periodStart: "2026-03",
                periodEnd: "2026-03",
                values: [1946],
                refIds: ["hev_month"],
              },
              {
                periodType: "ytd",
                periodLabel: "2026 YTD（截至 2026-03）",
                periodStart: "2026-01",
                periodEnd: "2026-03",
                values: [5051],
                refIds: ["hev_ytd"],
              },
            ],
          },
        ],
        conflicts: [],
        hasBlockingConflict: false,
      },
    });

    expect(evidencePackage?.toolResults[0].evidenceRefs[0].periodLabel).toBe("2026-03 当月");
    expect(evidencePackage?.scopeDiagnostics?.parallelScopes).toHaveLength(1);

    render(
      <AnalysisPathPanel
        evidencePackage={evidencePackage}
        toolCalls={["build_market_chart"]}
      />,
    );

    fireEvent.click(screen.getByText("Analysis Path"));
    expect(screen.getByText("Evidence Scope")).toBeTruthy();
    expect(screen.getByText("1 parallel · 0 conflicts")).toBeTruthy();
  });

  it("renders duplicate external evidence refs without React key warnings", () => {
    const evidencePackage = buildEvidencePackage();
    evidencePackage.toolResults[0].keyFindings = [
      "Incentives and Legislation | European Alternative Fuels Observatory",
      "Incentives and Legislation | European Alternative Fuels Observatory",
    ];
    evidencePackage.toolResults[0].evidenceRefs = [
      {
        refId: "external_research-[R1] Incentives and Legislation | European Alternative Fuels Observatory",
        label: "External source",
        value: "policy background",
      },
      {
        refId: "external_research-[R1] Incentives and Legislation | European Alternative Fuels Observatory",
        label: "External source",
        value: "policy background",
      },
    ];
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      render(
        <AnalysisPathPanel
          evidencePlan={{
            intent: "news_policy_search",
            requiredTools: ["external_research"],
            allowedTools: ["external_research"],
          }}
          evidencePackage={evidencePackage}
          toolCalls={["external_research"]}
        />,
      );

      const duplicateKeyWarnings = errorSpy.mock.calls.filter(call =>
        String(call[0]).includes("same key"),
      );
      expect(duplicateKeyWarnings).toHaveLength(0);
    } finally {
      errorSpy.mockRestore();
    }
  });

  it("builds unique citation keys for repeated external source labels", () => {
    const first = buildAstrBotCitationKey({
      tool: "external_research",
      citationId: "",
      label: "[R1] Incentives and Legislation | European Alternative Fuels Observatory",
      url: "",
    }, 0);
    const second = buildAstrBotCitationKey({
      tool: "external_research",
      citationId: "",
      label: "[R1] Incentives and Legislation | European Alternative Fuels Observatory",
      url: "",
    }, 1);

    expect(first).not.toBe(second);
    expect(first).toContain("external_research");
  });

  it("deduplicates repeated external citations by URL before rendering source cards", () => {
    const citations = selectUniqueAstrBotCitations([
      {
        label: "[R1] Incentives and Legislation | European Alternative Fuels Observatory",
        source: "alternative-fuels-observatory.ec.europa.eu",
        tool: "external_research",
        url: "https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/hungary/incentives-legislations",
        citationId: "R1",
        sourceTitle: "Incentives and Legislation | European Alternative Fuels Observatory",
      },
      {
        label: "[R1] Incentives and Legislation | European Alternative Fuels Observatory",
        source: "alternative-fuels-observatory.ec.europa.eu",
        tool: "external_research",
        url: "https://alternative-fuels-observatory.ec.europa.eu/transport-mode/road/hungary/incentives-legislations/",
        citationId: "R1",
        sourceTitle: "Incentives and Legislation | European Alternative Fuels Observatory",
      },
      {
        label: "[R2] Autovista Hungary EV market",
        source: "autovista24.autovistagroup.com",
        tool: "external_research",
        url: "https://autovista24.autovistagroup.com/news/will-hungary-see-automotive-growth-in-2026",
        citationId: "R2",
        sourceTitle: "Will Hungary see automotive growth in 2026?",
      },
    ]);

    expect(citations).toHaveLength(2);
    expect(citations.map(item => item.citationId)).toEqual(["R1", "R2"]);
  });

  it("summarizes missing required tool coverage from evidence package gaps", () => {
    const summary = buildToolCoverageSummary(
      ["query_msrp_pricing", "compare_competitive_set"],
      ["compare_competitive_set"],
      [{ name: "missing_required_tool:query_msrp_pricing", reason: "not executed", impact: "blocking" }],
    );

    expect(summary.status).toBe("gap");
    expect(summary.label).toBe("1 missing");
    expect(summary.missing).toEqual(["query_msrp_pricing"]);
    expect(summary.satisfied).toEqual(["compare_competitive_set"]);
  });

  it("shows missing required tools in Analysis Path when coverage is incomplete", () => {
    render(
      <AnalysisPathPanel
        evidencePlan={{
          intent: "pricing_analysis",
          requiredTools: ["query_msrp_pricing", "compare_competitive_set"],
          allowedTools: ["query_msrp_pricing", "compare_competitive_set"],
          mustHaveEvidence: ["own_model_price"],
        }}
        evidencePackage={{
          ...buildEvidencePackage(),
          intent: "pricing_analysis",
          confidence: "low",
          missingEvidence: [
            { name: "missing_required_tool:query_msrp_pricing", reason: "tool not executed", impact: "blocking" },
          ],
        }}
        qualityScore={{
          intentScore: 1,
          toolScore: 0.5,
          groundingScore: 0,
          followUpScore: 1,
          safetyScore: 1,
          executiveConclusionScore: 0.5,
          businessImplicationScore: 0.5,
          actionabilityScore: 0.5,
          evidenceAlignmentScore: 0.5,
          reportReadinessScore: 0.5,
          businessSynthesisScore: 0.5,
          totalScore: 0.58,
          failures: ["missing_required_tools:query_msrp_pricing"],
        }}
        toolCalls={["compare_competitive_set"]}
      />,
    );

    fireEvent.click(screen.getByText("Analysis Path"));
    expect(screen.getByText("1 missing")).toBeTruthy();
    expect(screen.getByText("Missing: query_msrp_pricing")).toBeTruthy();
  });

  it("keeps evidence gaps collapsed by default for user chat", () => {
    render(
      <EvidenceGapPanel
        evidencePackage={{
          ...buildEvidencePackage(),
          confidence: "low",
          missingEvidence: [
            { name: "current_msrp", reason: "price tool returned no rows", impact: "blocking" },
            { name: "price_corridor", reason: "competitor corridor unavailable", impact: "weakens_answer" },
          ],
        }}
        actions={[{
          action: "补齐竞品 MSRP",
          rationale: "缺少当前官方价格会影响定价结论",
          priority: "P0",
          evidenceRefs: [],
          citationIds: [],
        }]}
      />,
    );

    const panel = screen.getByLabelText("Evidence gaps") as HTMLDetailsElement;
    expect(panel.open).toBe(false);
    expect(screen.getByText("不能给确定数字结论")).toBeTruthy();
    expect(screen.getByText("current msrp")).toBeTruthy();
    expect(screen.getByText(/补齐竞品 MSRP/)).toBeTruthy();
  });

  it("can keep evidence gaps open for developer mode", () => {
    render(
      <EvidenceGapPanel
        evidencePackage={{
          ...buildEvidencePackage(),
          confidence: "low",
          missingEvidence: [
            { name: "missing_required_tool:query_msrp_pricing", reason: "tool not executed", impact: "blocking" },
          ],
        }}
        actions={[]}
        defaultOpen
      />,
    );

    const panel = screen.getByLabelText("Evidence gaps") as HTMLDetailsElement;
    expect(panel.open).toBe(true);
    expect(screen.getByText("required tool: query msrp pricing")).toBeTruthy();
  });

  it("collapses answer limitations by default and can open them for developer mode", () => {
    const limitations = ["Missing official source.", "Published date needs verification."];

    const { rerender } = render(<AnswerLimitationsPanel limitations={limitations} />);
    let panel = screen.getByLabelText("Answer limitations") as HTMLDetailsElement;
    expect(panel.open).toBe(false);
    expect(screen.getByText("2 checks need attention")).toBeTruthy();

    rerender(<AnswerLimitationsPanel limitations={limitations} defaultOpen />);
    panel = screen.getByLabelText("Answer limitations") as HTMLDetailsElement;
    expect(panel.open).toBe(true);
    expect(screen.getByText("Missing official source.")).toBeTruthy();
  });

  it("describes manual scoring as one total score per answer", () => {
    expect(scoreProgressText(0, 0, 8, 0, 0)).toBe("Pick one 1-5 total for AstrBot and Copilot");
    expect(scoreProgressText(8, 0, 8, 5, 0)).toBe("AstrBot total 5 selected · pick Copilot total");
    expect(scoreProgressText(8, 8, 8, 5, 3)).toBe("Total scores ready: AstrBot 5 · Copilot 3");
    expect(scoreCompletionText(8, 0, 8)).toBe("Total scores: AstrBot selected · Copilot pending");
    expect(scoreCompletionText(0, 0, 8)).toBe("Total scores: AstrBot pending · Copilot pending");
    expect(scoreCompletionText(0, 0, 0)).toBe("No dimensions");
  });

  it("requires complete scores for both answers before saving a business baseline", () => {
    expect(canSaveBusinessScore(8, 8, 8)).toBe(true);
    expect(canSaveBusinessScore(8, 0, 8)).toBe(false);
    expect(canSaveBusinessScore(1, 1, 8)).toBe(false);
    expect(canSaveBusinessScore(0, 0, 0)).toBe(false);
  });

  it("keeps the one-click business score rubric explicit", () => {
    expect(BUSINESS_SCORE_GUIDE_SUMMARY).toBe("Score guide · 5 Ready · 3 Tie · 1 Risky");
    expect(BUSINESS_SCORE_RUBRIC.map(item => item.score)).toEqual([5, 4, 3, 2, 1]);
    expect(BUSINESS_SCORE_RUBRIC.map(item => item.shortLabel)).toEqual(["Ready", "Better", "Tie", "Weak", "Risky"]);
    expect(businessScoreRubricLabel(5)).toBe("5 = Replace-ready");
    expect(businessScoreRubricLabel(3)).toBe("3 = Tie / usable");
    expect(businessScoreRubricLabel(0)).toBe("Use 1-5 total");
    expect(businessScoreShortLabel(5)).toBe("Ready");
    expect(businessScoreShortLabel(1)).toBe("Risky");
    expect(businessScoreShortLabel(0)).toBe("");
  });

  it("offers quick verdict presets for slight and clear wins on both sides", () => {
    expect(BUSINESS_QUICK_VERDICTS.map(item => item.label)).toEqual([
      "Copilot +1",
      "Copilot +2",
      "Tie",
      "AstrBot +1",
      "AstrBot +2",
      "Both weak",
    ]);
    expect(BUSINESS_QUICK_VERDICTS.map(item => `${item.astrbotScore}/${item.countryCopilotScore}`)).toEqual([
      "4/5",
      "3/5",
      "4/4",
      "5/4",
      "5/3",
      "2/2",
    ]);
  });

  it("keeps common business failure tags available in the main scoring flow", () => {
    expect(BUSINESS_QUICK_FAILURE_TAGS).toContain("tool_missing");
    expect(BUSINESS_QUICK_FAILURE_TAGS).toContain("evidence_missing");
    expect(BUSINESS_QUICK_FAILURE_TAGS).toContain("pm_insight_weak");
    expect(BUSINESS_QUICK_FAILURE_TAGS).toContain("followup_low_value");
  });

  it("renders business diagnostic keys as user-readable labels", () => {
    expect(businessDiagnosticLabel("external_research_claims_unavailable")).toBe("外部来源结论不足");
    expect(businessDiagnosticLabel("published_date")).toBe("来源发布日期");
    expect(businessDiagnosticLabel("coverage_diagnostic:no_current_prices_for_requested_models")).toBe("请求车型当前价格缺口");
    expect(businessDiagnosticLabel("missing_required_tool:query_msrp_pricing")).toBe("缺少工具：query msrp pricing");
  });

  it("builds a copy-safe judge env template separate from runtime provider keys", () => {
    const template = buildJudgeEnvTemplate({
      ready: false,
      enabled: false,
      missingKey: true,
      liveCheck: false,
      status: "disabled",
      reason: "disabled",
      provider: {
        provider: "openai",
        model: "gpt-5.5",
        apiBase: "https://api.openai.com/v1",
        keySource: "OPENAI_API_KEY",
      },
    });

    expect(template).toContain("Runtime DPV4/DeepSeek key stays separate");
    expect(template).toContain("APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED=true");
    expect(template).toContain("APP_ASTRBOT_JUDGE_KEY_ENV=OPENAI_API_KEY");
    expect(template).toContain("OPENAI_API_KEY=<paste judge provider key here>");
    expect(template).toContain("APP_ASTRBOT_JUDGE_MODEL=gpt-5.5");
    expect(template).toContain("APP_ASTRBOT_JUDGE_API_BASE=https://api.openai.com/v1");
    expect(template).not.toContain("sk-");
  });

  it("builds a spreadsheet-friendly business scoring sheet without saving scores", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
    ];
    const records = [
      buildSideBySideRecord({
        comparisonId: "cmp_sheet",
        questionId: "biz-pricing-001",
        scoreSchema,
        question: "瑞典\tJ7 HEV\n应该怎么定价？",
        failureTags: ["answer_too_generic"],
        astrbot: {
          status: "ok",
          answerStatus: "answered",
          selectedTool: "query_msrp_pricing",
          evidenceRefCount: 3,
          missingEvidence: [{ name: "current_msrp", reason: "Needs official source.", impact: "weakens_answer" }],
          answerPreview: "AstrBot answer\nwith business implications.",
        },
        countryCopilot: {
          status: "ok",
          answerMode: "grounded-model",
          sourceCount: 4,
          answerPreview: "CountryCopilot answer\twith long context.",
        },
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          source: "manual",
          winner: "astrbot",
          dimensions: ["intentAccuracy", "grounding"],
          astrbotScores: { intentAccuracy: 5, grounding: 5 },
          countryCopilotScores: { intentAccuracy: 3, grounding: 3 },
          scoreTotals: { astrbot: 5, countryCopilot: 3, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
    ];
    const notesByQuestionId: Record<string, EvalCodexReviewNote> = {
      "biz-pricing-001": {
        questionId: "biz-pricing-001",
        uiStatus: "pass",
        suggestedWinner: "astrbot",
        suggestedScores: {
          astrbot: { intentAccuracy: 5, grounding: 4 },
          countryCopilot: { intentAccuracy: 3, grounding: 3 },
        },
        suggestedFailureTags: ["answer_too_generic"],
        reviewNotes: "AstrBot is stronger but needs human confirmation.",
        screenshots: [],
        createdAt: "2026-06-13T01:00:00.000Z",
        source: "codex_review",
      },
    };

    const text = buildBusinessScoringSheetText(records, { notesByQuestionId });
    const lines = text.split("\n");

    expect(lines).toHaveLength(2);
    expect(lines[0]).toContain("astrbot_total_1_to_5");
    expect(lines[0]).toContain("copilot_total_1_to_5");
    expect(lines[1]).toContain("biz-pricing-001");
    expect(lines[1]).toContain("codex_draft:pass");
    expect(lines[1]).toContain("query_msrp_pricing");
    expect(lines[1]).toContain("current_msrp");
    expect(lines[1]).toContain("answer_too_generic");
    expect(lines[1]).toContain("AstrBot answer with business implications.");
    expect(lines[1]).toContain("CountryCopilot answer with long context.");
    expect(lines[1]).not.toContain("\n");

    const cells = lines[1].split("\t");
    expect(cells).toHaveLength(lines[0].split("\t").length);
    expect(cells[12]).toBe("");
    expect(cells[13]).toBe("");
  });

  it("imports filled scoring sheet rows as local drafts without overwriting saved scores", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
    ];
    const records = [
      buildSideBySideRecord({
        comparisonId: "cmp_import",
        questionId: "biz-pricing-001",
        scoreSchema,
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          source: "manual",
          winner: "astrbot",
          dimensions: ["intentAccuracy", "grounding"],
          astrbotScores: { intentAccuracy: 5, grounding: 5 },
          countryCopilotScores: { intentAccuracy: 3, grounding: 3 },
          scoreTotals: { astrbot: 5, countryCopilot: 3, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
    ];
    const sheet = [
      "question_id\tastrbot_total_1_to_5\tcopilot_total_1_to_5\twinner\tnotes\tfailure_tags",
      "biz-pricing-001\t5\t3\tastrbot\tAstrBot is more actionable.\tpm_insight_weak, table_not_readable",
      "biz-saved\t2\t5\tcountryCopilot\tShould not overwrite.\ttool_missing",
      "biz-missing\t4\t4\ttie\tUnknown row.\t",
    ].join("\n");

    const result = parseBusinessScoringSheetDrafts(records, sheet);

    expect(result.matchedCount).toBe(2);
    expect(result.appliedCount).toBe(1);
    expect(result.skippedCount).toBe(2);
    expect(result.firstComparisonId).toBe("cmp_import");
    expect(result.errors.join(" ")).toContain("already saved");
    expect(result.errors.join(" ")).toContain("unknown question_id biz-missing");
    expect(result.drafts.cmp_import).toMatchObject({
      status: "scored",
      winner: "astrbot",
      notes: "AstrBot is more actionable.",
      failureTags: ["pm_insight_weak", "table_not_readable"],
      astrbotScores: { intentAccuracy: 5, grounding: 5 },
      countryCopilotScores: { intentAccuracy: 3, grounding: 3 },
    });
    expect(result.drafts.cmp_saved).toBeUndefined();
    expect(recordsForImportedManualSave(records, result.drafts, ["cmp_import", "cmp_saved"]))
      .toEqual([records[0]]);
    expect(recordsForImportedManualSave(records, result.drafts, []))
      .toEqual([]);
  });

  it("imports reference judge JSON as local scored drafts without overwriting confirmed scores", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "toolSelection", label: "Tool" },
      { key: "grounding", label: "Grounding" },
      { key: "pmInsight", label: "PM Insight" },
      { key: "actionability", label: "Action" },
      { key: "artifactQuality", label: "Artifacts" },
      { key: "followUpValue", label: "Follow-up" },
      { key: "presentationReadiness", label: "Presentation" },
    ];
    const records = [
      buildSideBySideRecord({
        comparisonId: "cmp_judge",
        questionId: "biz-pricing-001",
        scoreSchema,
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          source: "manual",
          winner: "astrbot",
          dimensions: scoreSchema.map(item => item.key),
          astrbotScores: {
            intentAccuracy: 5,
            toolSelection: 5,
            grounding: 5,
            pmInsight: 5,
            actionability: 5,
            artifactQuality: 5,
            followUpValue: 5,
            presentationReadiness: 5,
          },
          countryCopilotScores: {
            intentAccuracy: 3,
            toolSelection: 3,
            grounding: 3,
            pmInsight: 3,
            actionability: 3,
            artifactQuality: 3,
            followUpValue: 3,
            presentationReadiness: 3,
          },
          scoreTotals: { astrbot: 5, countryCopilot: 3, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
    ];
    const judgeOutput = JSON.stringify({
      source: "gpt5_5_reference_judge",
      pathId: "gpt5_5",
      label: "GPT5.5 / GPT Judge",
      provider: "openai",
      model: "gpt-5.5",
      apiBase: "https://api.openai.com/v1",
      keySource: "OPENAI_API_KEY",
      records: [
        {
          questionId: "biz-pricing-001",
          winner: "astrbot",
          astrbotScores: {
            intentAccuracy: "5",
            toolSelection: 5,
            grounding: 5,
            pmInsight: 4,
            actionability: 4,
            artifactQuality: 4,
            followUpValue: 5,
            presentationReadiness: 4,
          },
          countryCopilotScores: {
            intentAccuracy: 3,
            toolSelection: 2,
            grounding: 3,
            pmInsight: 3,
            actionability: 3,
            artifactQuality: 2,
            followUpValue: 1,
            presentationReadiness: 3,
          },
          failureTags: ["table_not_readable"],
          notes: "AstrBot is more actionable and grounded.",
        },
        {
          questionId: "biz-saved",
          winner: "countryCopilot",
          astrbotScores: { intentAccuracy: 1 },
          countryCopilotScores: { intentAccuracy: 5 },
          notes: "Should not overwrite saved manual review.",
        },
      ],
    });

    const result = parseReferenceJudgeScoreDrafts(records, judgeOutput);

    expect(result.importedSourceLabel).toBe("gpt5_5_reference_judge");
    expect(result.matchedCount).toBe(2);
    expect(result.appliedCount).toBe(1);
    expect(result.skippedCount).toBe(1);
    expect(result.errors.join(" ")).toContain("already saved");
    expect(result.drafts.cmp_judge).toMatchObject({
      status: "scored",
      winner: "astrbot",
      failureTags: ["table_not_readable"],
      astrbotScores: {
        intentAccuracy: 5,
        toolSelection: 5,
        grounding: 5,
        pmInsight: 4,
        actionability: 4,
        artifactQuality: 4,
        followUpValue: 5,
        presentationReadiness: 4,
      },
      countryCopilotScores: {
        intentAccuracy: 3,
        toolSelection: 2,
        grounding: 3,
        pmInsight: 3,
        actionability: 3,
        artifactQuality: 2,
        followUpValue: 1,
        presentationReadiness: 3,
      },
    });
    expect(result.drafts.cmp_judge.notes).toContain("[Accepted reference judge draft]");
    expect(result.drafts.cmp_judge.notes).toContain("source=gpt5_5_reference_judge");
    expect(result.drafts.cmp_judge.notes).toContain("AstrBot is more actionable");
    expect(result.drafts.cmp_judge.judgeProvider).toEqual({
      source: "gpt5_5_reference_judge",
      pathId: "gpt5_5",
      label: "GPT5.5 / GPT Judge",
      provider: "openai",
      model: "gpt-5.5",
      apiBase: "https://api.openai.com/v1",
      keySource: "OPENAI_API_KEY",
    });
    expect(result.drafts.cmp_saved).toBeUndefined();
    expect(recordsForImportedManualSave(records, result.drafts, ["cmp_judge", "cmp_saved"]))
      .toEqual([records[0]]);
  });

  it("imports reference judge JSON total scores as complete dimension scores", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
      { key: "pmInsight", label: "PM Insight" },
    ];
    const records = [
      buildSideBySideRecord({
        comparisonId: "cmp_total_judge",
        questionId: "biz-total-score",
        scoreSchema,
      }),
    ];
    const judgeOutput = JSON.stringify({
      source: "gpt5_5_reference_judge",
      records: [
        {
          questionId: "biz-total-score",
          astrbotTotal: 5,
          countryCopilotTotal: 3,
          winner: "astrbot",
          notes: "AstrBot is more useful for PM review.",
        },
      ],
    });

    const result = parseReferenceJudgeScoreDrafts(records, judgeOutput);

    expect(result.appliedCount).toBe(1);
    expect(result.skippedCount).toBe(0);
    expect(result.drafts.cmp_total_judge).toMatchObject({
      status: "scored",
      winner: "astrbot",
      astrbotScores: {
        intentAccuracy: 5,
        grounding: 5,
        pmInsight: 5,
      },
      countryCopilotScores: {
        intentAccuracy: 3,
        grounding: 3,
        pmInsight: 3,
      },
    });
    expect(recordsForImportedManualSave(records, result.drafts, ["cmp_total_judge"]))
      .toEqual([records[0]]);
  });

  it("summarizes Codex draft review coverage without changing saved scoring", () => {
    const records = [
      buildSideBySideRecord({
        questionId: "biz-pricing-001",
        comparisonId: "cmp_pricing",
        category: "pricing",
        astrbot: { status: "ok", evidenceRefCount: 11 },
      }),
      buildSideBySideRecord({
        questionId: "biz-report-001",
        comparisonId: "cmp_report",
        category: "report_generation",
        astrbot: { status: "ok", evidenceRefCount: 1 },
      }),
      buildSideBySideRecord({ questionId: "biz-policy-001", comparisonId: "cmp_policy" }),
    ];
    const notes: EvalCodexReviewNotesResponse = {
      items: [],
      total: 2,
      limit: 100,
      latestByQuestionId: {
        "biz-pricing-001": {
          questionId: "biz-pricing-001",
          uiStatus: "pass",
          suggestedWinner: "astrbot",
          suggestedScores: {
            astrbot: { intentAccuracy: 5, grounding: 4 },
            countryCopilot: { intentAccuracy: 3, grounding: 3 },
          },
          suggestedFailureTags: [],
          reviewNotes: "AstrBot is more grounded.",
          screenshots: [],
          createdAt: "2026-06-13T00:00:00.000Z",
          source: "codex_review",
        },
        "biz-report-001": {
          questionId: "biz-report-001",
          uiStatus: "warning",
          suggestedWinner: "tie",
          suggestedScores: {
            astrbot: { intentAccuracy: 4, grounding: 4 },
            copilot: { intentAccuracy: 4, grounding: 3 },
          },
          suggestedFailureTags: ["presentation_not_ready"],
          reviewNotes: "Needs human check.",
          screenshots: [],
          createdAt: "2026-06-13T00:05:00.000Z",
          source: "codex_review",
        },
      },
    };

    const triage = buildCodexDraftTriage(notes, records);

    expect(triage.totalRecords).toBe(3);
    expect(triage.draftCount).toBe(2);
    expect(triage.coverage).toBeCloseTo(2 / 3);
    expect(triage.readyDraftCount).toBe(2);
    expect(triage.avgAstrBotScore).toBe(4.25);
    expect(triage.avgCountryCopilotScore).toBe(3.25);
    expect(triage.suggestedWins).toMatchObject({ astrbot: 1, tie: 1 });
    expect(triage.uiStatuses).toMatchObject({ pass: 1, warning: 1 });
    expect(triage.tieCount).toBe(1);
    expect(triage.thinEvidenceCount).toBe(1);
    expect(triage.researchGapCount).toBe(0);
    expect(triage.lowAstrBotScoreCount).toBe(0);
    expect(triage.gapClusters).toEqual([expect.objectContaining({
      category: "report_generation",
      count: 1,
      tieCount: 1,
      thinEvidenceCount: 1,
      researchGapCount: 0,
      avgAstrBotScore: 4,
      avgCountryCopilotScore: 3.5,
      exampleQuestionIds: ["biz-report-001"],
      priority: "P1",
    })]);
    expect(triage.latestAt).toBe("2026-06-13T00:05:00.000Z");
  });

  it("converts Codex draft review into a complete prefill without saving it", () => {
    const dimensions = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
      { key: "pmInsight", label: "PM Insight" },
    ];
    const note: EvalCodexReviewNote = {
      questionId: "biz-pricing-001",
      uiStatus: "pass",
      suggestedWinner: "astrbot",
      suggestedScores: {
        astrbot: { intentAccuracy: 5, grounding: 4 },
        copilot: { intentAccuracy: 3, grounding: 3 },
      },
      suggestedFailureTags: ["pm_insight_weak"],
      reviewNotes: "AstrBot is more actionable, but human should confirm before saving.",
      screenshots: [],
      createdAt: "2026-06-13T00:10:00.000Z",
      source: "codex_review",
    };

    const prefill = buildCodexDraftScorePrefill(note, dimensions);

    expect(prefill.status).toBe("pending");
    expect(prefill.winner).toBe("astrbot");
    expect(prefill.complete).toBe(true);
    expect(prefill.astrbotScores).toMatchObject({ intentAccuracy: 5, grounding: 4, pmInsight: 5 });
    expect(prefill.countryCopilotScores).toMatchObject({ intentAccuracy: 3, grounding: 3, pmInsight: 3 });
    expect(prefill.failureTags).toEqual(["pm_insight_weak"]);
    expect(prefill.notes).toContain("human should confirm");
  });

  it("builds an audit note when a reviewer accepts a Codex draft", () => {
    const note: EvalCodexReviewNote = {
      questionId: "biz-pricing-001",
      uiStatus: "pass",
      suggestedWinner: "astrbot",
      suggestedScores: {
        astrbot: { intentAccuracy: 5, grounding: 4 },
        countryCopilot: { intentAccuracy: 3, grounding: 3 },
      },
      suggestedFailureTags: [],
      reviewNotes: "AstrBot has stronger evidence and actionability.",
      screenshots: [],
      createdAt: "2026-06-13T00:55:00.000Z",
      source: "codex_review",
    };

    const notes = acceptedCodexDraftNotes(note);

    expect(notes).toContain("[Accepted Codex draft review]");
    expect(notes).toContain("uiStatus=pass");
    expect(notes).toContain("suggestedWinner=astrbot");
    expect(notes).toContain("createdAt=2026-06-13T00:55:00.000Z");
    expect(notes).toContain("stronger evidence");
  });

  it("summarizes business review progress and prioritizes draft-ready unscored rows", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
    ];
    const records = [
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          winner: "tie",
          notes: "already reviewed",
          dimensions: ["intentAccuracy", "grounding"],
          astrbotScores: { intentAccuracy: 4, grounding: 4 },
          countryCopilotScores: { intentAccuracy: 4, grounding: 4 },
          scoreTotals: { astrbot: 4, countryCopilot: 4, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_draft",
        questionId: "biz-draft",
        scoreSchema,
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_unscored",
        questionId: "biz-unscored",
        scoreSchema,
      }),
    ];
    const notesByQuestionId: Record<string, EvalCodexReviewNote> = {
      "biz-draft": {
        questionId: "biz-draft",
        uiStatus: "pass",
        suggestedWinner: "astrbot",
        suggestedScores: {
          astrbot: { intentAccuracy: 5, grounding: 4 },
          countryCopilot: { intentAccuracy: 3, grounding: 3 },
        },
        suggestedFailureTags: [],
        reviewNotes: "draft is ready",
        screenshots: [],
        createdAt: "2026-06-13T00:20:00.000Z",
        source: "codex_review",
      },
    };

    const workbench = buildBusinessReviewWorkbench(records, notesByQuestionId, 2, "draft_ready");

    expect(workbench.totalRecords).toBe(3);
    expect(workbench.visibleRecords).toBe(1);
    expect(workbench.scoredCount).toBe(1);
    expect(workbench.unscoredCount).toBe(2);
    expect(workbench.draftReadyUnscoredCount).toBe(1);
    expect(workbench.neededForReviewTarget).toBe(1);
    expect(workbench.nextComparisonId).toBe("cmp_draft");
    expect(workbench.nextQuestionId).toBe("biz-draft");
  });

  it("builds a baseline action plan that prioritizes human-confirmed draft review", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
    ];
    const records = [
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          source: "manual",
          winner: "tie",
          dimensions: ["intentAccuracy", "grounding"],
          astrbotScores: { intentAccuracy: 4, grounding: 4 },
          countryCopilotScores: { intentAccuracy: 4, grounding: 4 },
          scoreTotals: { astrbot: 4, countryCopilot: 4, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_decision",
        questionId: "biz-decision",
        question: "Does AstrBot win?",
        scoreSchema,
        category: "policy_news",
        astrbot: { status: "ok", evidenceRefCount: 1 },
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_draft",
        questionId: "biz-draft",
        scoreSchema,
        astrbot: {
          status: "ok",
          evidenceRefCount: 12,
          missingEvidence: [
            {
              name: "external_research_claims_unavailable",
              reason: "External research returned no citation-ready source-backed claim evidence.",
              impact: "weakens_answer",
            },
          ],
        },
      }),
    ];
    const notes: EvalCodexReviewNotesResponse = {
      items: [],
      total: 2,
      limit: 100,
      latestByQuestionId: {
        "biz-decision": {
          questionId: "biz-decision",
          uiStatus: "warning",
          suggestedWinner: "tie",
          suggestedScores: {
            astrbot: { intentAccuracy: 4, grounding: 3 },
            countryCopilot: { intentAccuracy: 3, grounding: 3 },
          },
          suggestedFailureTags: [],
          reviewNotes: "Tie and thin evidence should be manually reviewed first.",
          screenshots: [],
          createdAt: "2026-06-13T00:19:00.000Z",
          source: "codex_review",
        },
        "biz-draft": {
          questionId: "biz-draft",
          uiStatus: "pass",
          suggestedWinner: "astrbot",
          suggestedScores: {
            astrbot: { intentAccuracy: 5, grounding: 4 },
            countryCopilot: { intentAccuracy: 3, grounding: 3 },
          },
          suggestedFailureTags: [],
          reviewNotes: "Draft requires human confirmation before it counts.",
          screenshots: [],
          createdAt: "2026-06-13T00:20:00.000Z",
          source: "codex_review",
        },
      },
    };
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      count: 3,
      pendingHumanScoring: 2,
      pendingBaselineScoring: 2,
      pendingReplacementBaselineScoring: 2,
      scoredCount: 1,
      baselineScoredCount: 1,
      replacementBaselineScoredCount: 1,
      baselineSourceCounts: { manual: 1 },
      replacementBaselineSourceCounts: { manual: 1 },
    }, 3);
    const workbench = buildBusinessReviewWorkbench(records, notes.latestByQuestionId, readiness.minBusinessScores, "all");
    const triage = buildCodexDraftTriage(notes, records);

    const plan = buildBusinessBaselineActionPlan(readiness, workbench, triage, false);

    expect(plan.tone).toBe("pending");
    expect(plan.remainingToMinimum).toBe(2);
    expect(needsBusinessDecisionReview(records[1], notes.latestByQuestionId)).toBe(true);
    expect(needsBusinessDecisionReview(records[2], notes.latestByQuestionId)).toBe(true);
    expect(filterBusinessReviewRecords(records, notes.latestByQuestionId, "needs_decision").map(record => record.comparisonId)).toEqual(["cmp_decision", "cmp_draft"]);
    expect(filterBusinessReviewRecords(records, notes.latestByQuestionId, "draft_ready").map(record => record.comparisonId)).toEqual(["cmp_decision", "cmp_draft"]);
    expect(filterBusinessReviewRecords(records, notes.latestByQuestionId, "score_ready").map(record => record.comparisonId)).toEqual(["cmp_decision"]);
    expect(filterBusinessReviewRecords(records, notes.latestByQuestionId, "repair_first").map(record => record.comparisonId)).toEqual(["cmp_draft"]);
    expect(isBusinessScoreReadyRecord(records[1])).toBe(true);
    expect(isBusinessScoreReadyRecord(records[2])).toBe(false);
    expect(workbench.decisionNeededCount).toBe(2);
    expect(workbench.scoreReadyUnscoredCount).toBe(1);
    expect(workbench.repairFirstUnscoredCount).toBe(1);
    expect(workbench.nextComparisonId).toBe("cmp_decision");
    expect(workbench.nextQuestionId).toBe("biz-decision");
    expect(workbench.nextQuestion).toBe("Does AstrBot win?");
    expect(plan.recommendedFilter).toBe("score_ready");
    expect(plan.reviewButtonLabel).toBe("Review next score-ready row");
    expect(plan.sourceLabel).toBe("Manual 1");
    expect(plan.decisionLabel).toContain("2 decision rows");
    expect(plan.draftLabel).toContain("2 Codex drafts");
    expect(plan.judgeLabel).toBe("GPT judge not configured");
    expect(plan.description).toContain("human-reviewed manual scores or GPT judge scores count");
    expect(businessReviewPriorityReason(records[1], notes.latestByQuestionId)).toContain("Decision row first");
    expect(businessReviewPriorityReason(records[2], notes.latestByQuestionId)).toContain("Repair-first row");

    const scoreReadyWorkbench = buildBusinessReviewWorkbench(records, notes.latestByQuestionId, readiness.minBusinessScores, "score_ready");
    const repairFirstWorkbench = buildBusinessReviewWorkbench(records, notes.latestByQuestionId, readiness.minBusinessScores, "repair_first");
    expect(scoreReadyWorkbench.visibleRecords).toBe(1);
    expect(repairFirstWorkbench.visibleRecords).toBe(1);
  });

  it("keeps the baseline action plan focused on saved records once the minimum is reached", () => {
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      count: 8,
      pendingHumanScoring: 0,
      pendingBaselineScoring: 0,
      pendingReplacementBaselineScoring: 0,
      scoredCount: 8,
      baselineScoredCount: 8,
      replacementBaselineScoredCount: 8,
      baselineSourceCounts: { manual: 5, llm_judge: 3 },
      replacementBaselineSourceCounts: { manual: 5, llm_judge: 3 },
      replacementReadinessVerdict: "not_enough_data",
    }, 8);
    const workbench = buildBusinessReviewWorkbench([], {}, readiness.minBusinessScores, "all");
    const triage = buildCodexDraftTriage(null, []);

    const plan = buildBusinessBaselineActionPlan(readiness, workbench, triage, true);

    expect(plan.tone).toBe("ready");
    expect(plan.remainingToMinimum).toBe(0);
    expect(plan.recommendedFilter).toBe("saved");
    expect(plan.reviewButtonLabel).toBe("Review saved baseline");
    expect(plan.sourceLabel).toBe("Manual 5 · GPT judge 3");
    expect(plan.judgeLabel).toBe("GPT judge ready");
  });

  it("selects only unscored complete Codex drafts for codex_review audit saving", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
    ];
    const records = [
      buildSideBySideRecord({ comparisonId: "cmp_a", questionId: "biz-a", scoreSchema }),
      buildSideBySideRecord({ comparisonId: "cmp_b", questionId: "biz-b", scoreSchema }),
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          winner: "astrbot",
          dimensions: ["intentAccuracy", "grounding"],
          astrbotScores: { intentAccuracy: 5, grounding: 5 },
          countryCopilotScores: { intentAccuracy: 3, grounding: 3 },
          scoreTotals: { astrbot: 5, countryCopilot: 3, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
      buildSideBySideRecord({ comparisonId: "cmp_fail", questionId: "biz-fail", scoreSchema }),
      buildSideBySideRecord({ comparisonId: "cmp_partial", questionId: "biz-partial", scoreSchema }),
    ];
    const readyNote = (questionId: string): EvalCodexReviewNote => ({
      questionId,
      uiStatus: "pass",
      suggestedWinner: "astrbot",
      suggestedScores: {
        astrbot: { intentAccuracy: 5, grounding: 4 },
        countryCopilot: { intentAccuracy: 3, grounding: 3 },
      },
      suggestedFailureTags: [],
      reviewNotes: "draft ready",
      screenshots: [],
      createdAt: "2026-06-13T01:10:00.000Z",
      source: "codex_review",
    });
    const notesByQuestionId: Record<string, EvalCodexReviewNote> = {
      "biz-a": readyNote("biz-a"),
      "biz-b": readyNote("biz-b"),
      "biz-saved": readyNote("biz-saved"),
      "biz-fail": { ...readyNote("biz-fail"), uiStatus: "fail" },
      "biz-partial": {
        ...readyNote("biz-partial"),
        suggestedScores: {
          astrbot: { intentAccuracy: 5 },
          countryCopilot: {},
        },
      },
    };

    const selected = draftRecordsForCodexAcceptance(records, notesByQuestionId, 1);
    const selectedAllNeeded = draftRecordsForCodexAcceptance(records, notesByQuestionId, 5);

    expect(selected.map(record => record.comparisonId)).toEqual(["cmp_a"]);
    expect(selectedAllNeeded.map(record => record.comparisonId)).toEqual(["cmp_a", "cmp_b"]);
  });

  it("finds the next matching review row for save and next", () => {
    const scoreSchema = [
      { key: "intentAccuracy", label: "Intent" },
      { key: "grounding", label: "Grounding" },
    ];
    const records = [
      buildSideBySideRecord({ comparisonId: "cmp_a", questionId: "biz-a", scoreSchema }),
      buildSideBySideRecord({ comparisonId: "cmp_b", questionId: "biz-b", scoreSchema }),
      buildSideBySideRecord({
        comparisonId: "cmp_saved",
        questionId: "biz-saved",
        scoreSchema,
        humanScoring: {
          status: "scored",
          winner: "astrbot",
          dimensions: ["intentAccuracy", "grounding"],
          astrbotScores: { intentAccuracy: 5, grounding: 5 },
          countryCopilotScores: { intentAccuracy: 3, grounding: 3 },
          scoreTotals: { astrbot: 5, countryCopilot: 3, astrbotComplete: true, countryCopilotComplete: true, complete: true },
        },
      }),
      buildSideBySideRecord({ comparisonId: "cmp_c", questionId: "biz-c", scoreSchema }),
    ];
    const noteFor = (questionId: string): EvalCodexReviewNote => ({
      questionId,
      uiStatus: "pass",
      suggestedWinner: "astrbot",
      suggestedScores: {
        astrbot: { intentAccuracy: 5, grounding: 4 },
        countryCopilot: { intentAccuracy: 3, grounding: 3 },
      },
      suggestedFailureTags: [],
      reviewNotes: "draft ready",
      screenshots: [],
      createdAt: "2026-06-13T00:25:00.000Z",
      source: "codex_review",
    });
    const notesByQuestionId: Record<string, EvalCodexReviewNote> = {
      "biz-a": noteFor("biz-a"),
      "biz-b": noteFor("biz-b"),
      "biz-c": noteFor("biz-c"),
    };

    expect(findNextReviewComparisonId(records, notesByQuestionId, "cmp_a", "draft_ready")).toBe("cmp_b");
    expect(findNextReviewComparisonId(records, notesByQuestionId, "cmp_c", "draft_ready")).toBe("cmp_a");
  });

  it("treats evidence_missing as a data gate instead of an engineering block", () => {
    const summary = buildReadinessSummary({ evidence_missing: 1 });
    const breakdown = failureTagBreakdown(summary);
    const readiness = buildBusinessReadinessGate(summary, 21);

    expect(breakdown).toEqual({ total: 1, engineeringTotal: 0, evidenceGapTotal: 1 });
    expect(readiness.status).toBe("data_blocked");
    expect(readiness.engineeringClean).toBe(true);
    expect(readiness.evidenceReady).toBe(false);
    expect(readiness.replacementReady).toBe(false);
  });

  it("treats repair gaps as data blockers before manual or GPT scoring exists", () => {
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      repairGapCounts: {
        "coverage_diagnostic:no_current_prices_for_requested_models": 6,
      },
      topRepairGaps: [
        {
          gap: "coverage_diagnostic:no_current_prices_for_requested_models",
          tag: "coverage_diagnostic:no_current_prices_for_requested_models",
          count: 6,
        },
      ],
    }, 30);

    expect(readiness.status).toBe("data_blocked");
    expect(readiness.engineeringClean).toBe(true);
    expect(readiness.evidenceReady).toBe(false);
    expect(readiness.evidenceGapTotal).toBe(6);
    expect(readiness.failureTagTotal).toBe(0);
    expect(readiness.replacementReady).toBe(false);
  });

  it("keeps tool_missing as an engineering/business blocker", () => {
    const readiness = buildBusinessReadinessGate(buildReadinessSummary({ tool_missing: 1 }), 21);

    expect(readiness.status).toBe("blocked");
    expect(readiness.engineeringClean).toBe(false);
    expect(readiness.evidenceReady).toBe(true);
    expect(readiness.replacementReady).toBe(false);
  });

  it("uses manual or GPT judge scoring counts for the replacement baseline gate", () => {
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      pendingHumanScoring: 21,
      pendingBaselineScoring: 0,
      pendingReplacementBaselineScoring: 0,
      scoredCount: 0,
      baselineScoredCount: 21,
      replacementBaselineScoredCount: 21,
      baselineSourceCounts: { llm_judge: 21 },
      replacementBaselineSourceCounts: { llm_judge: 21 },
      judgeCalibration: {
        gptJudgedCount: 21,
        humanReviewedCount: 0,
        matchCount: 0,
        partialCount: 0,
        mismatchCount: 0,
        agreementRate: 0,
        weightedAgreementRate: 0,
        needsHumanReviewCount: 21,
        mismatchExamples: [],
        items: [],
      },
    }, 21);

    expect(readiness.businessBaselineReady).toBe(true);
    expect(readiness.scoredBaseline).toBe(21);
    expect(readiness.pendingBaselineScoring).toBe(0);
    expect(readiness.baselineSourceCounts).toEqual({ llm_judge: 21 });
  });

  it("uses backend replacement readiness summary as the business gate source of truth", () => {
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      replacementReadinessVerdict: "ready_for_limited_default_trial",
      replacementReadiness: {
        status: "ready",
        verdict: "ready_for_limited_default_trial",
        replacementReady: true,
        businessBaselineReady: true,
        winRateReady: true,
        executionClean: true,
        hallucinationClean: true,
        totalQuestions: 30,
        minimumRequiredScores: 21,
        scoredCount: 21,
        pendingCount: 9,
        sourceCounts: { llm_judge: 21 },
        astrbotWinRate: 0.74,
        avgAstrBotScore: 4.2,
        avgCountryCopilotScore: 3.8,
        astrbotErrorCount: 0,
        countryCopilotErrorCount: 0,
        hallucinationRiskCount: 0,
        failureTagTotal: 0,
        reasons: [],
        recommendedNextAction: "AstrBot can enter a limited default-route trial behind a feature flag.",
      },
    }, 30);

    expect(readiness.replacementReady).toBe(true);
    expect(readiness.verdict).toBe("ready_for_limited_default_trial");
    expect(readiness.scoredBaseline).toBe(21);
    expect(readiness.pendingBaselineScoring).toBe(9);
    expect(readiness.minBusinessScores).toBe(21);
    expect(readiness.baselineSourceCounts).toEqual({ llm_judge: 21 });
    expect(readiness.nextAction).toContain("limited default-route trial");
  });

  it("does not let accepted Codex draft scores unlock the replacement baseline gate", () => {
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      pendingHumanScoring: 0,
      pendingBaselineScoring: 0,
      pendingReplacementBaselineScoring: 21,
      scoredCount: 21,
      baselineScoredCount: 21,
      replacementBaselineScoredCount: 0,
      baselineSourceCounts: { codex_review: 21 },
      replacementBaselineSourceCounts: {},
      astrbotWinRate: 1,
      replacementAstrbotWinRate: 0,
      replacementReadinessVerdict: "not_enough_human_scores",
    }, 21);

    expect(readiness.businessBaselineReady).toBe(false);
    expect(readiness.scoredBaseline).toBe(0);
    expect(readiness.pendingBaselineScoring).toBe(21);
    expect(readiness.baselineSourceCounts).toEqual({});
    expect(readiness.replacementReady).toBe(false);
    expect(readiness.nextAction).toContain("manually or with GPT judge");
  });

  it("builds a copyable business readiness handoff without treating Codex drafts as replacement baseline", () => {
    const readiness = buildBusinessReadinessGate({
      ...buildReadinessSummary({}),
      pendingBaselineScoring: 30,
      pendingReplacementBaselineScoring: 30,
      baselineScoredCount: 0,
      replacementBaselineScoredCount: 0,
      baselineSourceCounts: {},
      replacementBaselineSourceCounts: {},
      replacementReadinessVerdict: "not_enough_human_scores",
    }, 30);
    const workbench = {
      totalRecords: 30,
      visibleRecords: 28,
      scoredCount: 0,
      unscoredCount: 30,
      decisionNeededCount: 2,
      draftReadyUnscoredCount: 30,
      scoreReadyUnscoredCount: 28,
      repairFirstUnscoredCount: 2,
      savedCount: 0,
      neededForReviewTarget: 21,
      nextComparisonId: "cmp_1",
      nextQuestionId: "biz-policy-003",
      nextQuestion: "CO₂ 0-75g/km 税率阶梯对 PHEV 是否有利？",
      nextCategory: "policy_news",
      nextCountry: "Hungary",
    };
    const codexTriage = {
      totalRecords: 30,
      draftCount: 30,
      coverage: 1,
      readyDraftCount: 30,
      avgAstrBotScore: 4.35,
      avgCountryCopilotScore: 2.95,
      suggestedWins: { astrbot: 24, countryCopilot: 0, tie: 6, unclear: 0 },
      uiStatuses: { pass: 30 },
      tieCount: 6,
      thinEvidenceCount: 0,
      researchGapCount: 0,
      lowAstrBotScoreCount: 0,
      gapClusters: [],
      latestAt: "2026-06-19T00:00:00.000Z",
    };
    const plan = buildBusinessBaselineActionPlan(readiness, workbench, codexTriage, false);

    const handoff = buildBusinessReadinessHandoffText({
      readiness,
      actionPlan: plan,
      workbench,
      codexTriage,
      judgePreflight: {
        status: "missing_key",
        ready: false,
        enabled: true,
        missingKey: true,
        liveCheck: false,
        reason: "missing_key",
        provider: {
          provider: "openai",
          model: "gpt-5.5",
          apiBase: "https://api.openai.com/v1",
          keySource: "OPENAI_API_KEY",
        },
      },
      visibleRecordCount: 28,
    });

    expect(handoff).toContain("Replacement baseline: 0/21 scored, 30 pending, 21 more needed");
    expect(handoff).toContain("Replacement baseline only counts saved `manual` or `llm_judge` scores.");
    expect(handoff).toContain("Codex draft / `codex_review` rows are self-test and triage evidence only.");
    expect(handoff).toContain("biz-policy-003");
    expect(handoff).toContain("Hungary");
    expect(handoff).toContain("not ready · missing_key");
  });

  it("builds an actionable evidence repair queue from business report records", () => {
    const queue = buildEvidenceRepairQueue([
      buildSideBySideRecord({
        failureTags: ["evidence_missing"],
        astrbot: {
          status: "ok",
          answerStatus: "partially_answered",
          selectedTool: "query_msrp_pricing",
          missingEvidence: [
            { name: "current_msrp", reason: "No current price rows.", impact: "blocking" },
            { name: "price_corridor", reason: "No competitor corridor.", impact: "weakens_answer" },
          ],
          recommendedActions: [{ action: "补齐当前 MSRP 和竞品价格走廊", priority: "P0" }],
        },
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_clean",
        questionId: "biz-report-003",
        category: "report_generation",
        question: "生成汇报结构。",
        failureTags: [],
        astrbot: { status: "ok", answerStatus: "answered", selectedTool: "query_country_snapshot", missingEvidence: [] },
      }),
      buildSideBySideRecord({
        comparisonId: "cmp_hardening",
        questionId: "biz-compare-001",
        category: "competitor_compare",
        question: "J7 HEV 的核心竞品是谁？",
        failureTags: [],
        astrbot: {
          status: "ok",
          answerStatus: "partially_answered",
          selectedTool: "compare_competitive_set",
          missingEvidence: [{ name: "configuration_delta", reason: "No trim delta rows.", impact: "blocking" }],
        },
      }),
    ]);

    expect(queue).toHaveLength(2);
    expect(queue[0].priority).toBe("P0");
    expect(queue[0].questionId).toBe("biz-pricing-004");
    expect(queue[0].missingEvidence.map(item => item.name)).toEqual(["current_msrp", "price_corridor"]);
    expect(queue[0].recommendedActions[0].action).toContain("MSRP");
    expect(queue[0].primaryGap).toBe("current_msrp");
    expect(queue[0].repairSummary?.primaryGap).toBe("current_msrp");
    expect(queue[0].repairSummary?.missingEvidenceCount).toBe(2);
    expect(queue[0].repairSummary?.blockingEvidenceCount).toBe(1);
    expect(queue[1].priority).toBe("P1");
    expect(queue[1].questionId).toBe("biz-compare-001");
  });

  it("normalizes grouped source repair backlog from the business report", () => {
    const backlog = normalizeSourceRepairBacklog([
      {
        priority: "P1",
        sourceType: "external_research_source",
        label: "VOC OMODA JAECOO Sweden owner review",
        sourceSearchQuery: "OMODA JAECOO Sweden owner review complaint forum",
        affectedCount: 1,
        questionIds: ["biz-voc-003"],
        categories: ["voc"],
        countries: ["Sweden"],
        primaryGaps: ["external_research_claims_unavailable"],
        failureTags: [],
        recommendedAction: "验证 VOC/媒体/论坛来源，保留标题、URL、发布日期和可引用原文要点。",
      },
      {
        priority: "P0",
        sourceType: "msrp_current_price_source",
        label: "J7 HEV",
        candidateDomain: "jaecoo.se",
        sourceDraftPath: "se/13_kia_sportage_se.yaml",
        sourceSearchQuery: "Sweden J7 HEV official price MSRP",
        affectedCount: 2,
        questionIds: ["biz-pricing-002", "biz-report-001"],
        categories: ["pricing", "report_generation"],
        countries: ["Sweden"],
        primaryGaps: ["coverage_diagnostic:no_current_prices_for_requested_models"],
        failureTags: ["evidence_missing"],
        recommendedAction: "验证官方价格来源，补齐版本/配置、币种、发布日期并生成 current_price 行。",
      },
    ]);

    expect(backlog).toHaveLength(2);
    expect(backlog[0].priority).toBe("P0");
    expect(backlog[0].sourceType).toBe("msrp_current_price_source");
    expect(backlog[0].sourceSearchQuery).toBe("Sweden J7 HEV official price MSRP");
    expect(backlog[0].affectedCount).toBe(2);
    expect(backlog[0].questionIds).toEqual(["biz-pricing-002", "biz-report-001"]);
    expect(backlog[1].sourceType).toBe("external_research_source");
  });

  it("sorts source repair backlog by priority and affected question count", () => {
    const backlog = normalizeSourceRepairBacklog([
      {
        priority: "P0",
        sourceType: "external_research_source",
        label: "VOC OMODA JAECOO Sweden owner review",
        sourceSearchQuery: "OMODA JAECOO Sweden owner review complaint forum",
        affectedCount: 1,
        questionIds: ["biz-voc-003"],
        categories: ["voc"],
        countries: ["Sweden"],
        primaryGaps: ["external_research_claims_unavailable"],
        failureTags: ["evidence_missing"],
        recommendedAction: "验证 VOC/媒体/论坛来源。",
      },
      {
        priority: "P0",
        sourceType: "msrp_current_price_source",
        label: "J7 HEV",
        sourceSearchQuery: "Sweden J7 HEV official price MSRP",
        affectedCount: 2,
        questionIds: ["biz-pricing-002", "biz-report-001"],
        categories: ["pricing", "report_generation"],
        countries: ["Sweden"],
        primaryGaps: ["coverage_diagnostic:no_current_prices_for_requested_models"],
        failureTags: ["evidence_missing"],
        recommendedAction: "验证官方价格来源并生成 current_price 行。",
      },
      {
        priority: "P1",
        sourceType: "msrp_current_price_source",
        label: "O9",
        sourceSearchQuery: "Sweden O9 official price MSRP",
        affectedCount: 4,
        questionIds: ["biz-pricing-004"],
        categories: ["pricing"],
        countries: ["Sweden"],
        primaryGaps: ["coverage_diagnostic:no_current_prices_for_requested_models"],
        failureTags: [],
        recommendedAction: "验证官方价格来源。",
      },
    ]);

    expect(backlog.map(item => item.label)).toEqual(["J7 HEV", "VOC OMODA JAECOO Sweden owner review", "O9"]);
  });

  it("builds a copyable TSV source repair backlog plan", () => {
    const backlog = normalizeSourceRepairBacklog([
      {
        priority: "P1",
        sourceType: "msrp_current_price_source",
        label: "J7 HEV",
        candidateDomain: "jaecoo.se",
        sourceDraftPath: "se/13_kia_sportage_se.yaml",
        sourceSearchQuery: "Sweden J7 HEV official price MSRP",
        affectedCount: 2,
        questionIds: ["biz-pricing-002", "biz-report-001"],
        categories: ["pricing", "report_generation"],
        countries: ["Sweden"],
        primaryGaps: ["coverage_diagnostic:no_current_prices_for_requested_models"],
        failureTags: [],
        recommendedAction: "验证官方价格来源，补齐版本/配置、币种、发布日期并生成 current_price 行。",
      },
    ]);

    const text = buildSourceRepairBacklogPlanText(backlog);

    expect(text).toContain("AstrBot Source Repair Backlog");
    expect(text).toContain("Priority\tSource Type\tLabel\tDraft Path\tSearch Query / URL\tAffected Questions");
    expect(text).toContain("P1\tMSRP current price\tJ7 HEV\tse/13_kia_sportage_se.yaml\tSweden J7 HEV official price MSRP\t2");
    expect(text).toContain("biz-pricing-002, biz-report-001");
    expect(text).toContain("请求车型当前价格缺口");
    expect(text).toContain("current_price");
  });

  it("separates readiness blockers from evidence hardening work", () => {
    const queue = normalizeEvidenceRepairQueue([
      {
        questionId: "biz-pricing-004",
        category: "pricing",
        country: "Sweden",
        question: "O9 在瑞典 53k-55k 欧元是否合理？",
        priority: "P0",
        primaryGap: "current_msrp",
        answerStatus: "partially_answered",
        selectedTool: "query_msrp_pricing",
        failureTags: ["evidence_missing"],
        missingEvidence: [
          { name: "current_msrp", reason: "No O9 own-model current price rows.", impact: "blocking" },
        ],
        recommendedActions: [{ action: "Create O9 own-model MSRP source, then rerun.", rationale: "", priority: "P0" }],
        repairTasks: [],
      },
      {
        questionId: "biz-config-003",
        category: "configuration",
        country: "Sweden",
        question: "北欧市场冬季包应该包含什么？",
        priority: "P1",
        answerStatus: "answered",
        selectedTool: "compare_vehicle_variants",
        failureTags: [],
        missingEvidence: [
          { name: "configuration_delta", reason: "Need richer trim evidence.", impact: "weakens_answer" },
        ],
        recommendedActions: [],
        repairTasks: [],
      },
    ], []);

    const blockers = buildBlockingReadinessItems(queue);

    expect(blockers).toHaveLength(1);
    expect(blockers[0].questionId).toBe("biz-pricing-004");
    expect(blockers[0].primaryGap).toBe("current_msrp");
    expect(blockers[0].reason).toContain("No O9 本车型当前价格 rows");
    expect(blockers[0].action).toContain("Create O9 own-model MSRP source");
  });

  it("summarizes evidence repair reasons without expanding every missing item", () => {
    const [item] = buildEvidenceRepairQueue([
      buildSideBySideRecord({
        failureTags: ["evidence_missing"],
        astrbot: {
          status: "ok",
          answerStatus: "partially_answered",
          selectedTool: "query_msrp_pricing",
          missingEvidence: [
            {
              name: "coverage_diagnostic:no_current_prices_for_country",
              reason: "Add current price observations for Sweden; the local current_prices table has no rows for this market.",
              impact: "blocking",
            },
            { name: "price_corridor", reason: "No competitor corridor.", impact: "weakens_answer" },
            { name: "own_model_price", reason: "No O9 own-model price.", impact: "blocking" },
          ],
        },
      }),
    ]);

    expect(evidenceRepairReasonLines(item)).toEqual([
      "国家当前价格表缺口: Add current price observations for Sweden; the local 当前价格表 has no rows for this market. (阻断结论)",
      "Price corridor: No competitor corridor. (会削弱结论)",
    ]);
  });

  it("uses backend evidence repair queue before local record inference", () => {
    const queue = normalizeEvidenceRepairQueue([
      {
        questionId: "biz-backend-001",
        comparisonId: "cmp_backend",
        category: "pricing",
        country: "Sweden",
        question: "Backend queue item",
        priority: "P0",
        primaryGap: "coverage_diagnostic:no_current_prices_for_country",
        commandHint: "Use Data Ops MSRP source workflow, then rerun biz-backend-001.",
        answerStatus: "partially_answered",
        selectedTool: "query_msrp_pricing",
        failureTags: ["evidence_missing"],
        missingEvidence: [
          {
            name: "coverage_diagnostic:no_current_prices_for_country",
            reason: "No current prices for Sweden.",
            impact: "blocking",
          },
        ],
        recommendedActions: [],
        sourceRepairCandidates: {
          dataStatus: "source_draft_only_not_price_evidence",
          missingOwnModelSource: true,
          materializedCandidateCount: 1,
          candidateCount: 2,
          ownModel: [],
          competitorCorridor: [
            {
              sourceCode: "volvo_xc90_se_draft_scrapling",
              brand: "VOLVO",
              model: "XC90",
              sourceUrl: "https://www.volvocars.com/se/build/xc90-hybrid/",
              relativePath: "se/26_volvo_xc90_se.yaml",
            },
            {
              sourceCode: "kia_ev9_se_draft_scrapling",
              brand: "KIA",
              model: "EV9",
              sourceUrl: "https://www.kia.com/se/nya-bilar/ev9/",
              relativePath: "se/21_kia_ev9_se.yaml",
            },
          ],
        },
        repairSummary: {
          primaryGap: "coverage_diagnostic:no_current_prices_for_country",
          missingEvidenceCount: 1,
          blockingEvidenceCount: 1,
          weakEvidenceCount: 0,
          sourceCandidateCount: 2,
          competitorCandidateCount: 2,
          materializedCandidateCount: 1,
          missingOwnModelSource: true,
          sourceSummary: "1/2 source candidates materialized; own-model source missing",
        },
        repairTasks: [
          {
            taskId: "biz_backend_001_1_own_model_msrp_source",
            taskType: "own_model_msrp_source",
            title: "Create own-model MSRP source",
            input: "country=Sweden; category=pricing",
            output: "Own-model current price evidenceRefs with source dates.",
            owner: "Data/Ops",
            priority: "P0",
            status: "todo",
            sourceCandidates: ["VOLVO XC90", "KIA EV9"],
          },
        ],
        repairAction: "补齐 Sweden current price observations 后重跑。",
      },
    ], [
      buildSideBySideRecord({
        questionId: "biz-local-001",
        failureTags: ["tool_missing"],
      }),
    ]);

    expect(queue).toHaveLength(1);
    expect(queue[0].questionId).toBe("biz-backend-001");
    expect(queue[0].primaryGap).toBe("coverage_diagnostic:no_current_prices_for_country");
    expect(queue[0].commandHint).toContain("Data Ops MSRP 来源流程");
    expect(queue[0].repairAction).toContain("Sweden current price");
    expect(queue[0].repairSummary?.primaryGap).toBe("coverage_diagnostic:no_current_prices_for_country");
    expect(queue[0].repairSummary?.sourceSummary).toContain("1/2 个来源候选已生成价格行");
    expect(queue[0].sourceRepairCandidates?.competitorCorridor?.map(item => item.model)).toEqual(["XC90", "EV9"]);
    expect(queue[0].repairTasks[0].title).toBe("Create own-model MSRP source");
    expect(queue[0].repairTasks[0].sourceCandidates).toEqual(["VOLVO XC90", "KIA EV9"]);
  });

  it("builds a copyable evidence repair plan for developers", () => {
    const queue = normalizeEvidenceRepairQueue([
      {
        questionId: "biz-pricing-004",
        comparisonId: "cmp_pricing_004",
        category: "pricing",
        country: "Sweden",
        question: "O9 在瑞典 53k-55k 欧元是否合理？",
        priority: "P0",
        primaryGap: "coverage_diagnostic:no_current_prices_for_country",
        commandHint: "Use Data Ops MSRP source workflow, then rerun biz-pricing-004.",
        answerStatus: "partially_answered",
        selectedTool: "query_msrp_pricing",
        failureTags: ["evidence_missing"],
        missingEvidence: [
          {
            name: "coverage_diagnostic:no_current_prices_for_country",
            reason: "No current prices for Sweden.",
            impact: "blocking",
          },
        ],
        recommendedActions: [],
        sourceRepairCandidates: {
          dataStatus: "source_draft_only_not_price_evidence",
          missingOwnModelSource: true,
          materializedCandidateCount: 0,
          candidateCount: 2,
          ownModel: [],
          competitorCorridor: [
            {
              sourceCode: "volvo_xc90_se_draft_scrapling",
              brand: "VOLVO",
              model: "XC90",
              sourceUrl: "https://www.volvocars.com/se/build/xc90-hybrid/",
              relativePath: "se/26_volvo_xc90_se.yaml",
            },
            {
              sourceCode: "kia_ev9_se_draft_scrapling",
              brand: "KIA",
              model: "EV9",
              sourceUrl: "https://www.kia.com/se/nya-bilar/ev9/",
              relativePath: "se/21_kia_ev9_se.yaml",
            },
          ],
        },
        repairTasks: [
          {
            taskId: "biz_pricing_004_1_own_model_msrp_source",
            taskType: "own_model_msrp_source",
            title: "Create own-model MSRP source",
            input: "country=Sweden; category=pricing; selectedTool=query_msrp_pricing",
            output: "Own-model current price evidenceRefs with model, trim/version, MSRP, currency, source, and retrievedAt.",
            owner: "Data/Ops",
            priority: "P0",
            status: "todo",
            evidenceName: "own_model_current_msrp",
            sourceCandidates: ["VOLVO XC90", "KIA EV9"],
            commandHint: "Use Data Ops MSRP source workflow, then rerun biz-pricing-004.",
          },
        ],
        repairAction: "补齐 Sweden current price observations 后重跑 biz-pricing-004。",
      },
    ], []);

    const text = buildEvidenceRepairPlanText(queue);

    expect(text).toContain("AstrBot Evidence Repair Plan");
    expect(text).toContain("[P0] biz-pricing-004");
    expect(text).toContain("国家当前价格表缺口 (阻断结论)");
    expect(text).toContain("No current prices for Sweden.");
    expect(text).toContain("Repair summary: primary gap=国家当前价格表缺口; missing=1; blocking=1; source=0/2 个来源候选已生成价格行; 本车型来源缺失; 来源草稿尚未转成价格证据");
    expect(text).toContain("Command hint: 用 Data Ops MSRP 来源流程, 然后重跑 biz-pricing-004.");
    expect(text).toContain("Source drafts:");
    expect(text).toContain("VOLVO XC90 · se/26_volvo_xc90_se.yaml");
    expect(text).toContain("KIA EV9 · se/21_kia_ev9_se.yaml");
    expect(text).toContain("Repair tasks:");
    expect(text).toContain("[P0] Create own-model MSRP source (Data/Ops)");
    expect(text).toContain("output: 本车型当前价格 可引用证据");
    expect(text).toContain("hint: 用 Data Ops MSRP 来源流程");
    expect(text).toContain("sources: VOLVO XC90, KIA EV9");
    expect(text).toContain("补齐 Sweden current price observations 后重跑 biz-pricing-004。");
  });

  it("summarizes evidence repair debt for the developer overview strip", () => {
    const queue = normalizeEvidenceRepairQueue([
      {
        questionId: "biz-pricing-001",
        category: "pricing",
        question: "瑞典 J7 HEV 应该怎么定价？",
        priority: "P1",
        answerStatus: "answered",
        selectedTool: "query_msrp_pricing",
        failureTags: [],
        missingEvidence: [
          { name: "current_official_msrp_cross_check", reason: "Need official MSRP.", impact: "weakens_answer" },
        ],
        recommendedActions: [],
        sourceRepairCandidates: {
          candidateCount: 5,
          materializedCandidateCount: 2,
          missingOwnModelSource: true,
          ownModel: [],
          competitorCorridor: [],
        },
        repairTasks: [
          {
            taskId: "pricing_own_source",
            taskType: "own_model_msrp_source",
            title: "Create own-model MSRP source",
            input: "",
            output: "",
            owner: "Data/Ops",
            priority: "P1",
            status: "todo",
          },
          {
            taskId: "pricing_competitor",
            taskType: "competitor_price_corridor",
            title: "Validate competitor price corridor",
            input: "",
            output: "",
            owner: "Data/Ops",
            priority: "P1",
            status: "todo",
          },
        ],
      },
      {
        questionId: "biz-policy-003",
        category: "policy_news",
        question: "CO2 税率阶梯对 PHEV 是否有利？",
        priority: "P0",
        answerStatus: "partially_answered",
        selectedTool: "external_research",
        failureTags: ["evidence_missing"],
        missingEvidence: [
          { name: "published_date", reason: "Need dated source.", impact: "blocking" },
        ],
        recommendedActions: [],
        repairTasks: [
          {
            taskId: "policy_date",
            taskType: "source_date_evidence",
            title: "Attach dated external source evidence",
            input: "",
            output: "",
            owner: "Research",
            priority: "P0",
            status: "todo",
          },
          {
            taskId: "policy_rerun",
            taskType: "rerun_business_validation",
            title: "Rerun affected validation item",
            input: "",
            output: "",
            owner: "AstrBot Eval",
            priority: "P1",
            status: "todo",
          },
        ],
      },
      {
        questionId: "biz-compare-004",
        category: "competitor_compare",
        question: "O9 和 XC60 / EX60 的定位差异是什么？",
        priority: "P1",
        answerStatus: "partially_answered",
        selectedTool: "compare_competitive_set",
        failureTags: [],
        missingEvidence: [
          { name: "configuration_delta", reason: "Need config delta.", impact: "weakens_answer" },
        ],
        recommendedActions: [],
        repairTasks: [
          {
            taskId: "config_gap",
            taskType: "config_gap_evidence",
            title: "Map configuration/BOM gap evidence",
            input: "",
            output: "",
            owner: "Product/PM",
            priority: "P1",
            status: "todo",
          },
        ],
      },
    ], []);

    const overview = buildEvidenceRepairOverview(queue);

    expect(overview.total).toBe(3);
    expect(overview.p0Count).toBe(1);
    expect(overview.p1Count).toBe(2);
    expect(overview.answeredCount).toBe(1);
    expect(overview.partialCount).toBe(2);
    expect(overview.taskCount).toBe(5);
    expect(overview.pricingSourceTaskCount).toBe(2);
    expect(overview.configGapTaskCount).toBe(1);
    expect(overview.sourceDateTaskCount).toBe(1);
    expect(overview.rerunTaskCount).toBe(1);
    expect(overview.materializedCandidateCount).toBe(2);
    expect(overview.sourceCandidateCount).toBe(5);
    expect(overview.missingOwnModelSourceCount).toBe(1);
    expect(overview.topOwners[0]).toEqual({ owner: "Data/Ops", count: 2 });
  });

  it("shows a bounded evidence repair list with an explicit show-all affordance", () => {
    const records = Array.from({ length: 8 }, (_, index) => buildSideBySideRecord({
      questionId: `biz-repair-${index + 1}`,
      question: `Repair item ${index + 1}`,
      failureTags: ["evidence_missing"],
      astrbot: {
        status: "ok",
        answerStatus: "partially_answered",
        selectedTool: "query_msrp_pricing",
        missingEvidence: [
          {
            name: `missing_evidence_${index + 1}`,
            reason: "Needs repair.",
            impact: "weakens_answer",
          },
        ],
      },
    }));
    const queue = buildEvidenceRepairQueue(records);

    const collapsed = buildEvidenceRepairDisplayState(queue, false, 6);
    const expanded = buildEvidenceRepairDisplayState(queue, true, 6);

    expect(collapsed.visibleItems).toHaveLength(6);
    expect(collapsed.hiddenCount).toBe(2);
    expect(collapsed.statusText).toBe("Showing 6 of 8");
    expect(collapsed.toggleLabel).toBe("Show all 8");
    expect(expanded.visibleItems).toHaveLength(8);
    expect(expanded.hiddenCount).toBe(2);
    expect(expanded.statusText).toBe("Showing 8 of 8");
    expect(expanded.toggleLabel).toBe("Show top 6");
  });

  it("creates and renders a snapshot fallback when a trend chart is unavailable", () => {
    const card = buildChartFallbackCard("Show Sweden BEV sales trend with a chart", undefined, buildEvidencePackage());

    expect(card?.missingEvidence).toBe("monthly trend series");
    expect(card?.rows[0].label).toBe("BEV");

    render(<ChartFallbackCard card={card as ChartFallbackCardData} />);

    expect(screen.getByText("Trend series unavailable; showing current evidence snapshot instead.")).toBeTruthy();
    expect(screen.getByText("BEV")).toBeTruthy();
    expect(screen.getByText("25,235 units")).toBeTruthy();
  });

  it("renders metric, chart, table and report visual artifacts", () => {
    const artifacts: VisualArtifact[] = [
      {
        id: "metrics",
        type: "metric_cards",
        title: "Key metrics",
        data: {
          rows: [{ label: "BEV", value: 25235, unit: "units", source: "JATO" }],
          intentAnalysis: { template: "market_overview", productImplication: "Prioritize BEV opportunity." },
        },
        sourceEvidenceRefs: ["ev_1"],
      },
      {
        id: "chart",
        type: "chart",
        title: "Current evidence snapshot",
        fallbackReason: "monthly trend series missing",
        data: [{ label: "BEV", value: 25235 }],
        spec: { chartType: "bar", xField: "label", yField: "value", data: [{ label: "BEV", value: 25235 }] },
        sourceEvidenceRefs: ["ev_1"],
      },
      {
        id: "table",
        type: "table",
        title: "Pricing evidence table",
        data: {
          rows: [{ model: "J7 HEV", powertrain: "HEV", msrp: "34,720 EUR", monthlyPayment: "待补", rv: "待补", pricePosition: "core corridor", action: "Build price matrix.", evidenceRef: "ev_2" }],
          intentAnalysis: { template: "pricing_analysis", recommendation: "Build price matrix." },
        },
        spec: {
          columns: ["model", "powertrain", "msrp", "monthlyPayment", "rv", "pricePosition", "action"],
          columnPolicy: "Main table is capped at seven business columns.",
        },
        sourceEvidenceRefs: ["ev_2"],
      },
      {
        id: "report",
        type: "report_block",
        title: "PPT-ready block",
        data: {
          title: "J7 HEV pricing",
          keyMessage: "Use core corridor plus high-trim push.",
          evidence: ["MSRP 34,720 EUR"],
          productImplication: "高配主推。",
          nextAction: "Build price matrix.",
        },
        sourceEvidenceRefs: ["ev_2"],
      },
    ];

    render(<VisualArtifactsDeck artifacts={artifacts} />);

    expect(screen.getByLabelText("证据展示摘要")).toBeTruthy();
    expect(screen.getAllByText("指标").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("图表").length).toBeGreaterThanOrEqual(1);
    expect(screen.getAllByText("表格").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("汇报块").length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText("1 条证据").length).toBeGreaterThanOrEqual(4);
    expect(screen.getAllByText("关键指标").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("25,235 units")).toBeTruthy();
    expect(screen.getAllByText("当前证据快照").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("缺少月度趋势序列")).toBeTruthy();
    expect(screen.getAllByText("价格证据表").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("月供/租赁")).toBeTruthy();
    expect(screen.getByText("价格位置")).toBeTruthy();
    expect(screen.getByText("主表最多展示 7 个业务字段。")).toBeTruthy();
    expect(screen.getByText("J7 HEV pricing")).toBeTruthy();
    expect(screen.getAllByText("生成价格矩阵。").length).toBeGreaterThanOrEqual(1);
  });

  it("localizes generated visual artifact structure for user mode", () => {
    const artifacts: VisualArtifact[] = [
      {
        id: "artifact_market_structure_chart",
        type: "chart",
        title: "Market structure chart",
        data: [{ label: "SUV A", value: 7544 }],
        spec: {
          chartType: "bar",
          xField: "label",
          yField: "value",
          note: "Cross-tab bars show evidence-backed sales signals by dimension; they are not additive market totals.",
          data: [{ label: "SUV A", value: 7544 }],
        },
        sourceEvidenceRefs: ["ev_market"],
      },
      {
        id: "artifact_market_overview_table",
        type: "table",
        title: "Market decision table",
        data: {
          rows: [
            {
              dimension: "Segment structure",
              signal: "SUV A",
              evidence: "7,544 units",
              businessImplication: "Prioritize SUV A price band and winter package.",
              recommendedAction: "Build price matrix.",
              confidence: "high",
            },
          ],
        },
        spec: {
          columns: ["dimension", "signal", "evidence", "businessImplication", "recommendedAction", "confidence"],
          businessExplanation: "Market table converts snapshot evidence into business implications and next product actions.",
        },
        sourceEvidenceRefs: ["ev_market"],
      },
    ];

    render(<VisualArtifactsDeck artifacts={artifacts} />);

    expect(screen.getByText("交叉表柱状图按维度展示有证据支撑的销售信号，不代表可相加的市场总量。")).toBeTruthy();
    expect(screen.getByText("市场表把快照证据转成业务含义和下一步产品动作。")).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "维度" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "业务含义" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "建议动作" })).toBeTruthy();
    expect(screen.getByText("优先验证 SUV A 价格带和冬季包。")).toBeTruthy();
    expect(screen.getByText("生成价格矩阵。")).toBeTruthy();
    expect(screen.getByText("高")).toBeTruthy();
    expect(screen.queryByText("Business Implication")).toBeNull();
    expect(screen.queryByText("Recommended Action")).toBeNull();
  });

  it("renders HEV/PHEV route table as compact decision cards", () => {
    const artifacts: VisualArtifact[] = [
      {
        id: "artifact_powertrain_route_table",
        type: "table",
        title: "HEV / PHEV route comparison table",
        subtitle: "Evidence-backed route table.",
        data: {
          rows: [
            {
              powertrain: "PHEV",
              sales: "969 units",
              share: "待补",
              twoWheelDrive: "52%",
              fourWheelDrive: "46.9%",
              routeRole: "公司车/TCO 验证线",
              productAction: "补月供、残值/RV、税费 benefit、里程和充电条件后再决定是否主推。",
            },
            {
              powertrain: "HEV",
              sales: "2,687 units",
              share: "待补",
              twoWheelDrive: "89.5%",
              fourWheelDrive: "9.9%",
              routeRole: "低风险主线",
              productAction: "验证价格敏感、无稳定充电和低使用风险场景；继续补车型级价格/竞品池。",
            },
          ],
        },
        spec: {
          columns: ["powertrain", "sales", "share", "twoWheelDrive", "fourWheelDrive", "routeRole", "productAction"],
          businessExplanation: "This table consolidates powertrain evidence into product-route decisions.",
        },
        sourceEvidenceRefs: ["ev_phev_sales", "ev_hev_sales"],
      },
    ];

    render(<VisualArtifactsDeck artifacts={artifacts} />);

    expect(screen.getByLabelText("HEV / PHEV 路线决策表")).toBeTruthy();
    expect(screen.getByText("公司车/TCO 验证线")).toBeTruthy();
    expect(screen.getByText("低风险主线")).toBeTruthy();
    expect(screen.getByText("969 units")).toBeTruthy();
    expect(screen.getByText("2,687 units")).toBeTruthy();
    expect(screen.getByText("46.9%")).toBeTruthy();
    expect(screen.queryByRole("columnheader", { name: "Powertrain" })).toBeNull();
  });

  it("builds compact business artifact previews for eval answer cards", () => {
    const previews = buildBusinessArtifactPreviews([
      {
        id: "table",
        type: "table",
        title: "Market decision table",
        data: {
          rows: [
            {
              dimension: "Segment structure",
              signal: "SUV A",
              evidence: "7,544 units",
              businessImplication: "Prioritize SUV A price band and winter package.",
            },
          ],
        },
        sourceEvidenceRefs: ["ev_market"],
      },
      {
        id: "report",
        type: "report_block",
        title: "PPT-ready block",
        data: {
          keyMessage: "J7 HEV enters through practical SUV A0/A use cases.",
          productImplication: "Use warranty, visible high-spec and price anchor logic.",
          nextAction: "Build competitor matrix.",
        },
        sourceEvidenceRefs: ["ev_report"],
      },
      {
        id: "metrics",
        type: "metric_cards",
        title: "Key metrics",
        data: {
          rows: [{ label: "BEV", value: 25235, unit: "units", source: "JATO" }],
        },
        sourceEvidenceRefs: ["ev_metric"],
      },
    ]);

    expect(previews.map(item => item.title)).toEqual(["Market decision table", "PPT-ready block", "Key metrics"]);
    expect(previews[0].lines[0]).toContain("Segment structure");
    expect(previews[0].lines[0]).toContain("7,544 units");
    expect(previews[1].lines).toContain("J7 HEV enters through practical SUV A0/A use cases.");
    expect(previews[2].lines[0]).toContain("BEV");
    expect(previews[2].lines[0]).toContain("25,235");
  });

  it("prioritizes business fields for real pricing, configuration and MSRP repair artifact previews", () => {
    const previews = buildBusinessArtifactPreviews([
      {
        id: "pricing",
        type: "table",
        title: "Pricing evidence table",
        data: {
          rows: [
            {
              model: "J7 HEV",
              powertrain: "HEV",
              msrp: "34,720 EUR",
              monthlyPayment: "待补月供/租赁方案",
              rv: "待补残值/RV",
              pricePosition: "本车型价格锚点；价差 3,230 EUR；PVA 118 %",
              action: "对齐竞品走廊、月供/RV 和可感知配置价值后确认主销版本",
            },
          ],
        },
        sourceEvidenceRefs: ["ev_price"],
      },
      {
        id: "config",
        type: "table",
        title: "Configuration validation matrix",
        data: {
          rows: [
            {
              feature: "市场场景证据 · SUV A BEV 渗透率",
              targetModel: "A0 SUV",
              validationData: "SUV A BEV 渗透率 = 40 %",
              sourceOrTool: "jato_country_chart_deck",
              acceptanceCriteria: "支持 A0/A SUV BEV 需求和长续航版本继续验证",
              currentStatus: "已有 可引用证据: ev_3_11",
              priority: "P1",
            },
          ],
        },
        sourceEvidenceRefs: ["ev_config"],
      },
      {
        id: "source_repair",
        type: "table",
        title: "MSRP source validation table",
        data: {
          rows: [
            {
              candidateRole: "请求车型",
              model: "KIA EV3",
              sourceType: "source_draft",
              sourceStatus: "来源草稿待审核",
              draftPath: "se/04_kia_ev3_se.yaml",
              searchQuery: "https://www.kia.com/se/nya-bilar/ev3/upptack/",
            },
          ],
        },
        sourceEvidenceRefs: ["ev_source"],
      },
    ]);

    expect(previews[0].lines[0]).toContain("J7 HEV");
    expect(previews[0].lines[0]).toContain("34,720 EUR");
    expect(previews[0].lines[0]).toContain("PVA 118 %");
    expect(previews[0].lines[0]).not.toContain("monthlyPayment");
    expect(previews[1].lines[0]).toContain("市场场景证据");
    expect(previews[1].lines[0]).toContain("SUV A BEV 渗透率 = 40 %");
    expect(previews[1].lines[0]).toContain("已有 可引用证据");
    expect(previews[2].lines[0]).toContain("请求车型");
    expect(previews[2].lines[0]).toContain("KIA EV3");
    expect(previews[2].lines[0]).toContain("来源草稿待审核");
  });

  it("builds PPT copy from a report block artifact", () => {
    const text = buildPptBlockCopyText(
      {
        id: "report",
        type: "report_block",
        title: "PPT-ready block",
        data: {
          title: "J7 HEV pricing",
          keyMessage: "Core corridor plus high trim.",
          evidence: ["MSRP 34,720 EUR", "PVA coverage 118%"],
          productImplication: "高配主推。",
          nextAction: "Build competitor matrix.",
        },
        sourceEvidenceRefs: ["ev_1"],
      },
      { title: "", summary: "", evidence: [], pmInsight: "", nextAction: "" },
    );

    expect(text).toContain("Title: J7 HEV pricing");
    expect(text).toContain("Evidence: MSRP 34,720 EUR / PVA coverage 118%");
    expect(text).toContain("Next action: Build competitor matrix.");
  });

  it("copies answer summary and PPT block", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText },
    });

    render(
      <AnswerCopyActions
        title="J7 HEV pricing"
        summary="Use core corridor."
        takeaways={["MSRP 34,720 EUR"]}
        pmInsight="高配主推。"
        visualArtifacts={[]}
        nextAction="Build competitor matrix."
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "Copy summary" }));
    fireEvent.click(screen.getByRole("button", { name: "Copy PPT block" }));

    expect(writeText).toHaveBeenCalledTimes(2);
    expect(writeText.mock.calls[0][0]).toContain("Use core corridor.");
    expect(writeText.mock.calls[1][0]).toContain("Next action: Build competitor matrix.");
  });
});
