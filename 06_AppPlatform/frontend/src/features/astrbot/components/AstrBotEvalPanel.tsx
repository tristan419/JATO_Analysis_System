import { Fragment, useEffect, useState } from "react";
import {
  fetchBusinessValidationQuestions,
  fetchBusinessValidationReport,
  fetchCodexReviewNotes,
  fetchEvalQuestions,
  fetchEvalJudgePreflight,
  fetchEvalResults,
  fetchEvalSideBySideResults,
  fetchEvalSummary,
  fetchEvalUsage,
  fetchLatestCodexReviewScoringArtifacts,
  judgeExistingBusinessValidationRecords,
  runBusinessValidationAll,
  runBusinessValidationCategory,
  runBusinessValidationQuestion,
  runEvalCategory,
  runEvalFull,
  runEvalQuestion,
  runEvalSideBySideCategory,
  runEvalSideBySideQuestion,
  updateEvalSideBySideHumanScore,
  type EvalUsageSummary,
} from "../astrbotApi";
import type {
  EvalBusinessQuestionsResponse,
  EvalBusinessJudgeExistingResponse,
  EvalBusinessReportResponse,
  EvalCodexReviewNote,
  EvalCodexReviewNotesResponse,
  EvalEvidenceRepairSummary,
  EvalEvidenceRepairItem,
  EvalEvidenceRepairTask,
  EvalJudgePreflightResponse,
  EvalQuestion,
  EvalReferenceJudgePath,
  EvalResult,
  EvalScoreDimension,
  EvalSideBySideRecord,
  EvalSideBySideResultsResponse,
  EvalSideBySideSummary,
  EvalSourceRepairBacklogItem,
  EvalSourceRepairCandidate,
  EvalSourceRepairCandidates,
  EvalSummary,
} from "../astrbotConfig";

type TabKey = "overview" | "results" | "questions" | "run" | "compare" | "business";
export type ScoringDraft = {
  status: string;
  winner: string;
  notes: string;
  failureTags: string[];
  astrbotScores: Record<string, number>;
  countryCopilotScores: Record<string, number>;
  judgeProvider?: {
    source?: string;
    pathId?: string;
    label?: string;
    provider?: string;
    model?: string;
    apiBase?: string;
    keySource?: string;
  };
};
type CodexDraftScorePrefill = ScoringDraft & {
  complete: boolean;
  astrbotAverage: number;
  countryCopilotAverage: number;
};
type BusinessPanelMode = "review" | "questions" | "calibration";
type BusinessReviewFilter = "all" | "score_ready" | "repair_first" | "needs_score" | "needs_decision" | "draft_ready" | "saved";
export type EvidenceRepairItem = {
  questionId: string;
  comparisonId?: string;
  category: string;
  country?: string;
  question: string;
  answerStatus: string;
  selectedTool: string;
  failureTags: string[];
  missingEvidence: Array<{ name: string; reason: string; impact: string }>;
  recommendedActions: Array<{ action: string; rationale: string; priority: string }>;
  sourceRepairCandidates?: EvalSourceRepairCandidates;
  repairSummary?: EvalEvidenceRepairSummary;
  priority: "P0" | "P1" | string;
  primaryGap?: string;
  commandHint?: string;
  repairAction?: string;
  repairTasks: EvalEvidenceRepairTask[];
};
export type EvidenceRepairOverview = {
  total: number;
  p0Count: number;
  p1Count: number;
  answeredCount: number;
  partialCount: number;
  taskCount: number;
  pricingSourceTaskCount: number;
  configGapTaskCount: number;
  sourceDateTaskCount: number;
  rerunTaskCount: number;
  materializedCandidateCount: number;
  sourceCandidateCount: number;
  missingOwnModelSourceCount: number;
  topOwners: Array<{ owner: string; count: number }>;
};
export type EvidenceRepairDisplayState = {
  visibleItems: EvidenceRepairItem[];
  hiddenCount: number;
  statusText: string;
  toggleLabel: string;
};
export type BlockingReadinessItem = {
  questionId: string;
  category: string;
  question: string;
  primaryGap: string;
  reason: string;
  action: string;
  selectedTool: string;
};

const CATEGORY_LABELS: Record<string, string> = {
  structured: "Structured",
  long_doc: "Long Doc",
  multi_hop: "Multi-Hop",
  fragmented: "Fragmented",
  mixed: "Mixed",
};

const BUSINESS_CATEGORY_LABELS: Record<string, string> = {
  pricing: "Pricing",
  competitor_compare: "Competitor",
  market_overview: "Market",
  policy_news: "Policy / News",
  configuration: "Configuration",
  inventory_bom: "Inventory / BOM",
  voc: "VOC",
  report_generation: "Report",
};

const BUSINESS_SCORE_LABELS: Record<string, string> = {
  intentAccuracy: "Intent",
  toolSelection: "Tool",
  grounding: "Grounding",
  pmInsight: "PM Insight",
  actionability: "Action",
  artifactQuality: "Artifacts",
  followUpValue: "Follow-up",
  presentationReadiness: "Presentation",
};

const BUSINESS_DIAGNOSTIC_LABELS: Record<string, string> = {
  answer_too_conservative: "回答过保守",
  answer_too_generic: "回答过泛",
  chart_not_useful: "图表价值弱",
  table_not_readable: "表格不可读",
  tool_missing: "工具缺失",
  evidence_missing: "证据缺口",
  pm_insight_weak: "产品经理洞察弱",
  followup_low_value: "追问价值低",
  presentation_not_ready: "不适合汇报",
  hallucination_risk: "幻觉风险",
  intent_wrong: "意图错误",
  coverage_diagnostic: "价格覆盖诊断",
  "coverage_diagnostic:no_current_prices_for_requested_models": "请求车型当前价格缺口",
  "coverage_diagnostic:no_current_prices_for_country": "国家当前价格表缺口",
  current_msrp: "当前 MSRP",
  own_model_price: "本车型价格",
  competitor_price_range: "竞品价格走廊",
  competitive_or_configuration_data_unavailable: "竞品/配置证据缺口",
  configuration_delta: "配置差异证据",
  published_date: "来源发布日期",
  external_research_claims_unavailable: "外部来源结论不足",
  monthly_trend_series: "月度趋势序列",
  query_competitive_landscape_weak_evidence_refs: "竞品格局证据弱",
  query_with_filters_weak_evidence_refs: "筛选查询证据弱",
  source_repair_candidates: "价格来源修复候选",
  decision_boundary: "决策边界",
  weakens_answer: "会削弱结论",
  blocking: "阻断结论",
  optional: "可选补强",
  "Pricing corridor playbook": "定价走廊方法",
  "Competitor positioning playbook": "竞品定位方法",
  "Market opportunity playbook": "市场机会方法",
  "Policy impact playbook": "政策影响方法",
  "VOC evidence playbook": "VOC 证据方法",
  "PPT-ready report playbook": "汇报生成方法",
  "Configuration value playbook": "配置价值方法",
  "Inventory / BOM playbook": "库存/BOM 方法",
};

export const BUSINESS_SCORE_RUBRIC = [
  { score: 5, label: "Replace-ready", shortLabel: "Ready", description: "Clearly better, grounded, actionable." },
  { score: 4, label: "Better", shortLabel: "Better", description: "Useful with minor gaps." },
  { score: 3, label: "Tie / usable", shortLabel: "Tie", description: "Acceptable but not clearly better." },
  { score: 2, label: "Weak", shortLabel: "Weak", description: "Generic, incomplete, or hard to use." },
  { score: 1, label: "Wrong / risky", shortLabel: "Risky", description: "Wrong intent, bad evidence, or unsafe claim." },
] as const;

export const BUSINESS_SCORE_GUIDE_SUMMARY = "Score guide · 5 Ready · 3 Tie · 1 Risky";

export const BUSINESS_QUICK_VERDICTS = [
  { winner: "countryCopilot", label: "Copilot +1", description: "Copilot slightly better", astrbotScore: 4, countryCopilotScore: 5 },
  { winner: "countryCopilot", label: "Copilot +2", description: "Copilot clearly better", astrbotScore: 3, countryCopilotScore: 5 },
  { winner: "tie", label: "Tie", description: "Both usable", astrbotScore: 4, countryCopilotScore: 4 },
  { winner: "astrbot", label: "AstrBot +1", description: "AstrBot slightly better", astrbotScore: 5, countryCopilotScore: 4 },
  { winner: "astrbot", label: "AstrBot +2", description: "AstrBot clearly better", astrbotScore: 5, countryCopilotScore: 3 },
  { winner: "unclear", label: "Both weak", description: "Not enough business value", astrbotScore: 2, countryCopilotScore: 2 },
] as const;

export const BUSINESS_QUICK_FAILURE_TAGS = [
  "tool_missing",
  "evidence_missing",
  "answer_too_conservative",
  "answer_too_generic",
  "pm_insight_weak",
  "followup_low_value",
  "presentation_not_ready",
  "table_not_readable",
] as const;

const SCORE_COLORS: Record<string, string> = {
  composite: "#4f46e5",
  retrievalPathCorrectness: "#2563eb",
  evidenceTraceability: "#0f766e",
  citationCoverage: "#d97706",
  chartCorrectness: "#7c3aed",
  toolSelectionRelevance: "#dc2626",
};

function formatScore(n: number): string {
  if (!Number.isFinite(n)) return "—";
  return (n * 100).toFixed(0) + "%";
}

function formatOptionalScore(n?: number): string {
  return typeof n === "number" ? formatScore(n) : "—";
}

function formatManualScore(n?: number): string {
  if (typeof n !== "number" || n <= 0) return "—";
  return Number.isInteger(n) ? String(n) : n.toFixed(1);
}

export function businessScoreRubricLabel(score: number): string {
  const item = BUSINESS_SCORE_RUBRIC.find(entry => entry.score === score);
  return item ? `${item.score} = ${item.label}` : "Use 1-5 total";
}

export function businessScoreShortLabel(score: number): string {
  const item = BUSINESS_SCORE_RUBRIC.find(entry => entry.score === score);
  return item?.shortLabel ?? "";
}

export function businessDiagnosticLabel(value?: string): string {
  const raw = String(value ?? "").trim();
  if (!raw) return "—";
  if (BUSINESS_DIAGNOSTIC_LABELS[raw]) return BUSINESS_DIAGNOSTIC_LABELS[raw];
  if (raw.startsWith("missing_required_tool:")) {
    return `缺少工具：${raw.slice("missing_required_tool:".length).replace(/_/g, " ")}`;
  }
  if (raw.startsWith("coverage_diagnostic:")) {
    return `价格覆盖诊断：${raw.slice("coverage_diagnostic:".length).replace(/_/g, " ")}`;
  }
  return readableStatusLabel(raw);
}

function businessDiagnosticListText(values: string[], fallback = "none"): string {
  const labels = values.map(value => businessDiagnosticLabel(value)).filter(label => label !== "—");
  return labels.length > 0 ? labels.join(", ") : fallback;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function businessDiagnosticDisplayText(value: string): string {
  const orderedEntries = Object.entries(BUSINESS_DIAGNOSTIC_LABELS)
    .sort(([left], [right]) => right.length - left.length);
  const labeled = orderedEntries.reduce(
    (text, [raw, label]) => text.replace(new RegExp(escapeRegExp(raw), "g"), label),
    value,
  );
  return labeled
    .replace(/业务答案生成：\s*evidence alignment is\s*(部分对齐|证据一致|证据冲突|证据不足)\.?/g, "证据对齐：$1。")
    .replace(/Business Composer:\s*evidence alignment is\s*(部分对齐|aligned|conflicting|insufficient)\.?/g, "证据对齐：$1。")
    .replace(/evidence alignment is\s*(部分对齐|aligned|conflicting|insufficient)\.?/g, "证据对齐：$1。");
}

export function buildJudgeEnvTemplate(preflight?: EvalJudgePreflightResponse | null): string {
  const provider = preflight?.provider;
  const keySource = provider?.keySource?.trim() || "OPENAI_API_KEY";
  const model = provider?.model?.trim() || "gpt-5.5";
  const apiBase = provider?.apiBase?.trim() || "https://api.openai.com/v1";
  return [
    "# AstrBot side-by-side GPT judge",
    "# Runtime DPV4/DeepSeek key stays separate. This key only scores /astrbot vs /copilot baseline records.",
    "APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED=true",
    `APP_ASTRBOT_JUDGE_KEY_ENV=${keySource}`,
    `${keySource}=<paste judge provider key here>`,
    `APP_ASTRBOT_JUDGE_MODEL=${model}`,
    `APP_ASTRBOT_JUDGE_API_BASE=${apiBase}`,
    "# Restart the backend after editing .env.",
  ].join("\n");
}

function referenceJudgePathTone(path: EvalReferenceJudgePath): string {
  if (path.readinessStatus === "ready") return "is-good";
  if (path.status === "implemented" || path.status === "configured" || path.readinessStatus === "configured_inactive") {
    return "is-ok";
  }
  return "is-low";
}

function scoreTone(n: number): string {
  if (!Number.isFinite(n)) return "";
  if (n >= 0.8) return "is-good";
  if (n >= 0.5) return "is-ok";
  return "is-low";
}

function statusTone(status?: string, error?: string): string {
  if (error || status === "failed") return "is-low";
  if (status === "ok") return "is-good";
  return "is-ok";
}

function tabLabel(tabKey: TabKey): string {
  switch (tabKey) {
    case "overview":
      return "Overview";
    case "results":
      return "Results";
    case "questions":
      return "Questions";
    case "run":
      return "Run";
    case "compare":
      return "Compare";
    case "business":
      return "Business";
  }
}

function categoryLabel(category: string): string {
  return CATEGORY_LABELS[category] ?? BUSINESS_CATEGORY_LABELS[category] ?? category;
}

function readableStatusLabel(value?: string): string {
  if (!value) return "—";
  const text = value.replace(/[_-]+/g, " ").trim();
  if (!text) return "—";
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function readableScoreSourceLabel(value?: string): string {
  switch (value) {
    case "manual":
      return "Manual";
    case "llm_judge":
      return "GPT judge";
    case "codex_review":
      return "Codex accepted";
    case "codex_review_draft":
      return "Codex draft";
    default:
      return readableStatusLabel(value);
  }
}

function formatLocalDateTime(value?: string): string {
  if (!value) return "—";
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return "—";
  return new Date(parsed).toLocaleString();
}

function countBreakdownText(counts: Record<string, number>, fallback = "—"): string {
  const parts = Object.entries(counts)
    .filter(([, count]) => count > 0)
    .map(([key, count]) => `${readableScoreSourceLabel(key)} ${count}`);
  return parts.length > 0 ? parts.join(" · ") : fallback;
}

function truncateText(text: string | undefined, maxLength: number): string {
  if (!text) return "—";
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}…`;
}

function formatFullRunOutput(totalRun: number, byCategory: Record<string, { avgComposite: number; count: number }>): string {
  const categoryLines = Object.entries(byCategory)
    .map(([cat, s]) => `${CATEGORY_LABELS[cat] ?? cat}: ${formatScore(s.avgComposite)} (${s.count} runs)`);
  return [`Total: ${totalRun} runs`, ...categoryLines].join("\n");
}

function formatSideBySideRunOutput(total: number, summary: EvalSideBySideSummary): string {
  return [
    `Total: ${total} side-by-side comparisons`,
    `Pending human scoring: ${summary.pendingHumanScoring}`,
    `AstrBot avg composite: ${formatScore(summary.avgAstrBotComposite)}`,
    `Errors: AstrBot ${summary.astrbotErrorCount}, CountryCopilot ${summary.countryCopilotErrorCount}`,
  ].join("\n");
}

function formatBusinessRunOutput(total: number, summary: EvalSideBySideSummary): string {
  return [
    `Total: ${total} business comparisons`,
    `Baseline scored: ${summary.baselineScoredCount ?? summary.scoredCount ?? 0}`,
    `Replacement baseline scored: ${summary.replacementBaselineScoredCount ?? summary.baselineScoredCount ?? summary.scoredCount ?? 0}`,
    `Pending baseline scoring: ${summary.pendingBaselineScoring ?? summary.pendingHumanScoring}`,
    `Pending replacement baseline scoring: ${summary.pendingReplacementBaselineScoring ?? summary.pendingBaselineScoring ?? summary.pendingHumanScoring}`,
    `Pending human scoring: ${summary.pendingHumanScoring}`,
    `Baseline sources: ${countBreakdownText(summary.baselineSourceCounts ?? summary.humanScoreSourceCounts ?? {}, "none")}`,
    `Replacement sources: ${countBreakdownText(summary.replacementBaselineSourceCounts ?? summary.baselineSourceCounts ?? summary.humanScoreSourceCounts ?? {}, "none")}`,
    `AstrBot win rate: ${Math.round((summary.astrbotWinRate ?? 0) * 100)}%`,
    `AstrBot replacement win rate: ${Math.round((summary.replacementAstrbotWinRate ?? summary.astrbotWinRate ?? 0) * 100)}%`,
    `AstrBot wins: ${summary.humanWins?.astrbot ?? 0}`,
    `CountryCopilot wins: ${summary.humanWins?.countryCopilot ?? 0}`,
    `Readiness: ${summary.replacementReadinessVerdict ?? "not_enough_data"}`,
  ].join("\n");
}

function formatBusinessJudgeExistingOutput(result: EvalBusinessJudgeExistingResponse): string {
  const summary = result.summary;
  return [
    `Judge existing status: ${readableStatusLabel(result.status)}`,
    `Attempted: ${result.attemptedCount} · Saved baseline: ${result.savedCount} · Failed: ${result.failedCount}`,
    `Candidates: ${result.candidateCount} · Skipped: ${result.skippedCount}`,
    `Judge statuses: ${countBreakdownText(result.statusCounts, "none")}`,
    `Replacement baseline scored: ${summary.replacementBaselineScoredCount ?? summary.baselineScoredCount ?? 0}`,
    `Pending replacement baseline scoring: ${summary.pendingReplacementBaselineScoring ?? summary.pendingBaselineScoring ?? summary.pendingHumanScoring}`,
    `Replacement sources: ${countBreakdownText(summary.replacementBaselineSourceCounts ?? summary.baselineSourceCounts ?? summary.humanScoreSourceCounts ?? {}, "none")}`,
    `Readiness: ${summary.replacementReadinessVerdict ?? "not_enough_data"}`,
  ].join("\n");
}

type BusinessReadinessGate = {
  status: "ready" | "pending" | "data_blocked" | "blocked";
  verdict: string;
  replacementReady: boolean;
  engineeringClean: boolean;
  evidenceReady: boolean;
  businessBaselineReady: boolean;
  winRateReady: boolean;
  scoredBaseline: number;
  totalQuestions: number;
  minBusinessScores: number;
  humanScored: number;
  gptJudged: number;
  pendingBaselineScoring: number;
  baselineSourceCounts: Record<string, number>;
  failureTagTotal: number;
  engineeringFailureTagTotal: number;
  evidenceGapTotal: number;
  errorTotal: number;
  winRate: number;
  nextAction: string;
};

type CodexDraftTriage = {
  totalRecords: number;
  draftCount: number;
  coverage: number;
  readyDraftCount: number;
  avgAstrBotScore: number;
  avgCountryCopilotScore: number;
  suggestedWins: Record<string, number>;
  uiStatuses: Record<string, number>;
  tieCount: number;
  thinEvidenceCount: number;
  researchGapCount: number;
  lowAstrBotScoreCount: number;
  gapClusters: CodexDraftGapCluster[];
  latestAt: string;
};
type CodexDraftGapCluster = {
  category: string;
  count: number;
  tieCount: number;
  thinEvidenceCount: number;
  researchGapCount: number;
  lowAstrBotScoreCount: number;
  avgAstrBotScore: number;
  avgCountryCopilotScore: number;
  exampleQuestionIds: string[];
  reason: string;
  priority: "P1" | "P2";
};
type BusinessReviewWorkbench = {
  totalRecords: number;
  visibleRecords: number;
  scoredCount: number;
  unscoredCount: number;
  decisionNeededCount: number;
  draftReadyUnscoredCount: number;
  scoreReadyUnscoredCount: number;
  repairFirstUnscoredCount: number;
  savedCount: number;
  neededForReviewTarget: number;
  nextComparisonId: string;
  nextQuestionId: string;
  nextQuestion: string;
  nextCategory: string;
  nextCountry: string;
};
type BusinessBaselineActionPlan = {
  tone: "ready" | "pending";
  title: string;
  description: string;
  progressLabel: string;
  progressPercent: number;
  recommendedFilter: BusinessReviewFilter;
  reviewButtonLabel: string;
  sourceLabel: string;
  draftLabel: string;
  decisionLabel: string;
  judgeLabel: string;
  remainingToMinimum: number;
};
type BusinessScoringSheetOptions = {
  notesByQuestionId?: Record<string, EvalCodexReviewNote>;
  maxRecords?: number;
  includeSaved?: boolean;
};
type BusinessReadinessHandoffOptions = {
  readiness: BusinessReadinessGate;
  actionPlan: BusinessBaselineActionPlan;
  workbench: BusinessReviewWorkbench;
  codexTriage: CodexDraftTriage;
  judgePreflight?: EvalJudgePreflightResponse | null;
  visibleRecordCount: number;
};
export type BusinessArtifactPreview = {
  id: string;
  type: string;
  title: string;
  lines: string[];
  meta: string;
};
export type BusinessScoringSheetImportResult = {
  drafts: Record<string, ScoringDraft>;
  matchedCount: number;
  appliedCount: number;
  skippedCount: number;
  firstComparisonId: string;
  errors: string[];
};
type ReferenceJudgeScoreImportResult = BusinessScoringSheetImportResult & {
  importedSourceLabel: string;
};

type FailureTagBreakdown = {
  total: number;
  engineeringTotal: number;
  evidenceGapTotal: number;
};

const DATA_GAP_FAILURE_TAGS = new Set(["evidence_missing"]);

function readyVerdictAllowsSwitch(verdict: string): boolean {
  return [
    "ready",
    "ready_to_replace",
    "replacement_ready",
    "ready_to_consider_switch",
    "ready_for_limited_default_trial",
  ].includes(verdict);
}

function finiteTagCount(count: number): number {
  return Number.isFinite(count) && count > 0 ? count : 0;
}

export function failureTagBreakdown(summary?: EvalSideBySideSummary): FailureTagBreakdown {
  const counts = summary?.failureTagCounts ?? {};
  return Object.entries(counts).reduce<FailureTagBreakdown>((acc, [tag, count]) => {
    const value = finiteTagCount(count);
    acc.total += value;
    if (DATA_GAP_FAILURE_TAGS.has(tag)) {
      acc.evidenceGapTotal += value;
    } else {
      acc.engineeringTotal += value;
    }
    return acc;
  }, { total: 0, engineeringTotal: 0, evidenceGapTotal: 0 });
}

function repairGapTotal(summary?: EvalSideBySideSummary): number {
  const counts = summary?.repairGapCounts ?? {};
  return Object.values(counts).reduce((total, count) => total + finiteTagCount(count), 0);
}

export function buildBusinessReadinessGate(summary?: EvalSideBySideSummary, total = 0): BusinessReadinessGate {
  const backendReadiness = summary?.replacementReadiness;
  const verdict = backendReadiness?.verdict ?? summary?.replacementReadinessVerdict ?? "not_enough_data";
  const totalQuestions = total > 0 ? total : backendReadiness?.totalQuestions ?? summary?.count ?? 0;
  const minBusinessScores = totalQuestions > 0
    ? backendReadiness?.minimumRequiredScores ?? Math.min(totalQuestions, Math.max(8, Math.ceil(totalQuestions * 0.7)))
    : 8;
  const humanScored = summary?.scoredCount ?? 0;
  const gptJudged = summary?.judgeCalibration?.gptJudgedCount ?? 0;
  const scoredBaseline = backendReadiness?.scoredCount
    ?? summary?.replacementBaselineScoredCount
    ?? summary?.baselineScoredCount
    ?? Math.max(humanScored, gptJudged);
  const pendingBaselineScoring = backendReadiness?.pendingCount
    ?? summary?.pendingReplacementBaselineScoring
    ?? summary?.pendingBaselineScoring
    ?? Math.max(0, totalQuestions - scoredBaseline);
  const baselineSourceCounts = backendReadiness?.sourceCounts
    ?? summary?.replacementBaselineSourceCounts
    ?? summary?.baselineSourceCounts
    ?? summary?.humanScoreSourceCounts
    ?? {};
  const tagBreakdown = failureTagBreakdown(summary);
  const evidenceRepairGapTotal = repairGapTotal(summary);
  const errorTotal = (summary?.astrbotErrorCount ?? 0) + (summary?.countryCopilotErrorCount ?? 0);
  const winRate = backendReadiness?.astrbotWinRate ?? summary?.replacementAstrbotWinRate ?? summary?.astrbotWinRate ?? 0;
  const engineeringClean = tagBreakdown.engineeringTotal === 0 && errorTotal === 0;
  const evidenceGapTotal = Math.max(tagBreakdown.evidenceGapTotal, evidenceRepairGapTotal);
  const evidenceReady = evidenceGapTotal === 0;
  const businessBaselineReady = backendReadiness?.businessBaselineReady ?? scoredBaseline >= minBusinessScores;
  const winRateReady = backendReadiness?.winRateReady ?? (businessBaselineReady && winRate >= 0.7);
  const replacementReady = Boolean(backendReadiness?.replacementReady ?? readyVerdictAllowsSwitch(verdict))
    && engineeringClean
    && evidenceReady
    && businessBaselineReady
    && winRateReady;
  const status = replacementReady
    ? "ready"
    : engineeringClean && !evidenceReady
      ? "data_blocked"
      : engineeringClean
        ? "pending"
        : "blocked";
  const nextAction = backendReadiness?.recommendedNextAction || (!engineeringClean
    ? "Fix engineering failure tags or execution errors before scoring more samples."
    : !evidenceReady
      ? "Fill the named evidence/data gap shown in the Evidence Repair Queue before treating AstrBot as replacement-ready."
      : !businessBaselineReady
        ? `Score ${Math.max(0, minBusinessScores - scoredBaseline)} more business records manually or with GPT judge.`
        : !winRateReady
          ? "Review losing categories before switching default traffic."
          : "Backend verdict is still holding the replacement gate; review report details before switching.");
  return {
    status,
    verdict,
    replacementReady,
    engineeringClean,
    evidenceReady,
    businessBaselineReady,
    winRateReady,
    scoredBaseline,
    totalQuestions,
    minBusinessScores,
    humanScored,
    gptJudged,
    pendingBaselineScoring,
    baselineSourceCounts,
    failureTagTotal: tagBreakdown.total,
    engineeringFailureTagTotal: tagBreakdown.engineeringTotal,
    evidenceGapTotal,
    errorTotal,
    winRate,
    nextAction,
  };
}

function averageScoreMap(scores?: Record<string, number>): number {
  if (!scores) return 0;
  const values = Object.values(scores).filter(value => Number.isFinite(value) && value > 0);
  if (values.length === 0) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(2));
}

function averageNumbers(values: number[]): number {
  const validValues = values.filter(value => Number.isFinite(value) && value > 0);
  if (validValues.length === 0) return 0;
  return Number((validValues.reduce((sum, value) => sum + value, 0) / validValues.length).toFixed(2));
}

function averageAllNumbers(values: number[]): number {
  const validValues = values.filter(value => Number.isFinite(value));
  if (validValues.length === 0) return 0;
  return Number((validValues.reduce((sum, value) => sum + value, 0) / validValues.length).toFixed(2));
}

function newerDate(left: string, right: string): string {
  if (!left) return right;
  if (!right) return left;
  return Date.parse(right) > Date.parse(left) ? right : left;
}

export function buildCodexDraftTriage(
  notes: EvalCodexReviewNotesResponse | null | undefined,
  records: EvalSideBySideRecord[] = [],
): CodexDraftTriage {
  const latestByQuestionId = notes?.latestByQuestionId ?? {};
  const recordByQuestionId = new Map(records.map(record => [record.questionId, record]));
  const relevantNotes = records.length > 0
    ? records
      .map(record => latestByQuestionId[record.questionId])
      .filter((note): note is EvalCodexReviewNote => Boolean(note))
    : Object.values(latestByQuestionId);
  const totalRecords = records.length > 0 ? records.length : Object.keys(latestByQuestionId).length;
  const suggestedWins: Record<string, number> = { astrbot: 0, countryCopilot: 0, tie: 0, unclear: 0 };
  const uiStatuses: Record<string, number> = {};
  const astrbotScores: number[] = [];
  const countryCopilotScores: number[] = [];
  const gapBuckets = new Map<string, {
    category: string;
    count: number;
    tieCount: number;
    thinEvidenceCount: number;
    researchGapCount: number;
    lowAstrBotScoreCount: number;
    astrbotScores: number[];
    countryCopilotScores: number[];
    exampleQuestionIds: string[];
  }>();
  let readyDraftCount = 0;
  let tieCount = 0;
  let thinEvidenceCount = 0;
  let researchGapCount = 0;
  let lowAstrBotScoreCount = 0;
  let latestAt = "";

  for (const note of relevantNotes) {
    const winner = note.suggestedWinner || "unclear";
    const record = recordByQuestionId.get(note.questionId);
    const category = record?.category || "uncategorized";
    suggestedWins[winner] = (suggestedWins[winner] ?? 0) + 1;
    uiStatuses[note.uiStatus] = (uiStatuses[note.uiStatus] ?? 0) + 1;
    latestAt = newerDate(latestAt, note.createdAt);

    const astrbotAverage = averageScoreMap(note.suggestedScores.astrbot);
    const countryAverage = averageScoreMap(note.suggestedScores.countryCopilot ?? note.suggestedScores.copilot);
    const evidenceRefCount = typeof record?.astrbot?.evidenceRefCount === "number"
      ? record.astrbot.evidenceRefCount
      : typeof record?.astrbot?.evidenceCount === "number"
        ? record.astrbot.evidenceCount
        : null;
    const isTie = winner === "tie";
    const isThinEvidence = evidenceRefCount !== null && evidenceRefCount <= 1;
    const hasResearchGap = researchSourceGapCount(record) > 0;
    const isLowAstrBotScore = astrbotAverage > 0 && astrbotAverage < 4;
    if (astrbotAverage > 0) astrbotScores.push(astrbotAverage);
    if (countryAverage > 0) countryCopilotScores.push(countryAverage);
    if (astrbotAverage > 0 && countryAverage > 0 && note.uiStatus !== "fail") {
      readyDraftCount += 1;
    }
    if (isTie) tieCount += 1;
    if (isThinEvidence) thinEvidenceCount += 1;
    if (hasResearchGap) researchGapCount += 1;
    if (isLowAstrBotScore) lowAstrBotScoreCount += 1;

    if (isTie || isThinEvidence || hasResearchGap || isLowAstrBotScore) {
      const bucket = gapBuckets.get(category) ?? {
        category,
        count: 0,
        tieCount: 0,
        thinEvidenceCount: 0,
        researchGapCount: 0,
        lowAstrBotScoreCount: 0,
        astrbotScores: [],
        countryCopilotScores: [],
        exampleQuestionIds: [],
      };
      bucket.count += 1;
      if (isTie) bucket.tieCount += 1;
      if (isThinEvidence) bucket.thinEvidenceCount += 1;
      if (hasResearchGap) bucket.researchGapCount += 1;
      if (isLowAstrBotScore) bucket.lowAstrBotScoreCount += 1;
      if (astrbotAverage > 0) bucket.astrbotScores.push(astrbotAverage);
      if (countryAverage > 0) bucket.countryCopilotScores.push(countryAverage);
      if (bucket.exampleQuestionIds.length < 3) bucket.exampleQuestionIds.push(note.questionId);
      gapBuckets.set(category, bucket);
    }
  }

  const gapClusters = [...gapBuckets.values()]
    .map(bucket => {
      const reasonParts = [
        bucket.tieCount > 0 ? `${bucket.tieCount} draft tie${bucket.tieCount === 1 ? "" : "s"}` : "",
        bucket.thinEvidenceCount > 0 ? `${bucket.thinEvidenceCount} thin-evidence row${bucket.thinEvidenceCount === 1 ? "" : "s"}` : "",
        bucket.researchGapCount > 0 ? `${bucket.researchGapCount} research-source gap${bucket.researchGapCount === 1 ? "" : "s"}` : "",
        bucket.lowAstrBotScoreCount > 0 ? `${bucket.lowAstrBotScoreCount} AstrBot score below 4` : "",
      ].filter(Boolean);
      return {
        category: bucket.category,
        count: bucket.count,
        tieCount: bucket.tieCount,
        thinEvidenceCount: bucket.thinEvidenceCount,
        researchGapCount: bucket.researchGapCount,
        lowAstrBotScoreCount: bucket.lowAstrBotScoreCount,
        avgAstrBotScore: averageAllNumbers(bucket.astrbotScores),
        avgCountryCopilotScore: averageAllNumbers(bucket.countryCopilotScores),
        exampleQuestionIds: bucket.exampleQuestionIds,
        reason: reasonParts.join(" · ") || "Needs review",
        priority: bucket.thinEvidenceCount > 0 || bucket.researchGapCount > 0 || bucket.lowAstrBotScoreCount > 0 ? "P1" : "P2",
      } satisfies CodexDraftGapCluster;
    })
    .sort((left, right) => (
      right.thinEvidenceCount - left.thinEvidenceCount
      || right.researchGapCount - left.researchGapCount
      || right.tieCount - left.tieCount
      || left.avgAstrBotScore - right.avgAstrBotScore
      || left.category.localeCompare(right.category)
    ));

  return {
    totalRecords,
    draftCount: relevantNotes.length,
    coverage: totalRecords > 0 ? relevantNotes.length / totalRecords : 0,
    readyDraftCount,
    avgAstrBotScore: averageNumbers(astrbotScores),
    avgCountryCopilotScore: averageNumbers(countryCopilotScores),
    suggestedWins,
    uiStatuses,
    tieCount,
    thinEvidenceCount,
    researchGapCount,
    lowAstrBotScoreCount,
    gapClusters,
    latestAt,
  };
}

function scoreDimensionsForRecord(record: EvalSideBySideRecord): EvalScoreDimension[] {
  if (record.scoreSchema?.length) return record.scoreSchema;
  const dimensions = record.humanScoring?.dimensions ?? [];
  return dimensions.map(key => ({ key, label: BUSINESS_SCORE_LABELS[key] ?? key }));
}

function defaultScoreMap(dimensions: EvalScoreDimension[], source?: Record<string, number>): Record<string, number> {
  const result: Record<string, number> = {};
  for (const dimension of dimensions) {
    const raw = source?.[dimension.key];
    if (typeof raw === "number" && raw > 0) {
      result[dimension.key] = raw;
    }
  }
  return result;
}

function completedScoreCount(dimensions: EvalScoreDimension[], scores: Record<string, number>): number {
  return dimensions.filter(dimension => {
    const value = scores[dimension.key];
    return Number.isFinite(value) && value >= 1 && value <= 5;
  }).length;
}

function completeAverage(dimensions: EvalScoreDimension[], scores: Record<string, number>): number {
  if (completedScoreCount(dimensions, scores) !== dimensions.length || dimensions.length === 0) return 0;
  const sum = dimensions.reduce((total, dimension) => total + (scores[dimension.key] ?? 0), 0);
  return Number((sum / dimensions.length).toFixed(2));
}

function scoreInputValue(scores: Record<string, number>, key: string): string {
  const value = scores[key];
  return Number.isFinite(value) && value >= 1 && value <= 5 ? String(value) : "";
}

function nextScoreMap(scores: Record<string, number>, key: string, raw: string): Record<string, number> {
  const next = { ...scores };
  const trimmed = raw.trim();
  if (!trimmed) {
    delete next[key];
    return next;
  }
  const numeric = Number(trimmed);
  if (!Number.isFinite(numeric) || numeric < 1) {
    delete next[key];
    return next;
  }
  next[key] = Math.min(5, Math.round(numeric));
  return next;
}

function filledScoreMap(dimensions: EvalScoreDimension[], score: number): Record<string, number> {
  const bounded = Math.max(1, Math.min(5, Math.round(score)));
  return Object.fromEntries(dimensions.map(dimension => [dimension.key, bounded]));
}

function boundedManualScore(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 1) return null;
  return Math.max(1, Math.min(5, Math.round(value)));
}

function codexScoreMapForDimensions(
  dimensions: EvalScoreDimension[],
  source?: Record<string, number>,
): Record<string, number> {
  const validValues = Object.values(source ?? {})
    .map(boundedManualScore)
    .filter((value): value is number => value !== null);
  const fallback = validValues.length > 0
    ? Math.max(1, Math.min(5, Math.round(validValues.reduce((sum, value) => sum + value, 0) / validValues.length)))
    : null;

  const result: Record<string, number> = {};
  for (const dimension of dimensions) {
    const direct = boundedManualScore(source?.[dimension.key]);
    const score = direct ?? fallback;
    if (score !== null) result[dimension.key] = score;
  }
  return result;
}

export function acceptedCodexDraftNotes(note: EvalCodexReviewNote): string {
  const marker = "[Accepted Codex draft review]";
  const meta = [
    `uiStatus=${note.uiStatus || "unknown"}`,
    `suggestedWinner=${note.suggestedWinner || "unclear"}`,
    `createdAt=${note.createdAt || "unknown"}`,
  ].join("; ");
  const body = note.reviewNotes.trim();
  return body ? `${marker}\n${meta}\n\n${body}` : `${marker}\n${meta}`;
}

export function buildCodexDraftScorePrefill(
  note: EvalCodexReviewNote,
  dimensions: EvalScoreDimension[],
): CodexDraftScorePrefill {
  const countrySource = note.suggestedScores.countryCopilot ?? note.suggestedScores.copilot;
  const astrbotScores = codexScoreMapForDimensions(dimensions, note.suggestedScores.astrbot);
  const countryCopilotScores = codexScoreMapForDimensions(dimensions, countrySource);
  const astrbotAverage = completeAverage(dimensions, astrbotScores) || averageScoreMap(astrbotScores);
  const countryCopilotAverage = completeAverage(dimensions, countryCopilotScores) || averageScoreMap(countryCopilotScores);
  const complete = dimensions.length > 0
    && completedScoreCount(dimensions, astrbotScores) === dimensions.length
    && completedScoreCount(dimensions, countryCopilotScores) === dimensions.length;

  return {
    status: "pending",
    winner: note.suggestedWinner,
    notes: note.reviewNotes,
    failureTags: note.suggestedFailureTags,
    astrbotScores,
    countryCopilotScores,
    complete,
    astrbotAverage,
    countryCopilotAverage,
  };
}

function hasSavedHumanScore(record: EvalSideBySideRecord): boolean {
  if (record.humanScoring?.status !== "scored") return false;
  return record.humanScoring.scoreTotals?.complete !== false;
}

function hasReadyCodexDraft(record: EvalSideBySideRecord, notesByQuestionId: Record<string, EvalCodexReviewNote>): boolean {
  const note = notesByQuestionId[record.questionId];
  if (!note || note.uiStatus === "fail") return false;
  const dimensions = scoreDimensionsForRecord(record);
  return buildCodexDraftScorePrefill(note, dimensions).complete;
}

function evidenceRefCount(record: EvalSideBySideRecord): number {
  const value = record.astrbot?.evidenceRefCount ?? record.astrbot?.evidenceCount;
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function researchSourceGapCount(record: EvalSideBySideRecord | undefined): number {
  const missing = missingEvidenceItems(record?.astrbot?.missingEvidence);
  return missing.filter(item => {
    const name = item.name.toLowerCase();
    return name === "external_research_claims_unavailable"
      || name === "published_date"
      || name === "fresh_external_signal";
  }).length;
}

const REPAIR_FIRST_MISSING_EVIDENCE = new Set([
  "competitive_or_configuration_data_unavailable",
  "current_msrp",
  "external_research_claims_unavailable",
  "monthly_trend_series",
  "own_model_price",
  "published_date",
]);

function isRepairFirstEvidenceName(name: string): boolean {
  const value = String(name || "").trim();
  if (!value) return false;
  return REPAIR_FIRST_MISSING_EVIDENCE.has(value)
    || value.startsWith("coverage_diagnostic:")
    || value.endsWith("_weak_evidence_refs");
}

export function isBusinessScoreReadyRecord(record: EvalSideBySideRecord): boolean {
  if (hasSavedHumanScore(record)) return false;
  if (record.comparison?.bothReturned === false) return false;
  const missing = missingEvidenceItems(record.astrbot?.missingEvidence);
  return !missing.some(item => isRepairFirstEvidenceName(item.name));
}

export function needsBusinessDecisionReview(
  record: EvalSideBySideRecord,
  notesByQuestionId: Record<string, EvalCodexReviewNote>,
): boolean {
  if (hasSavedHumanScore(record)) return false;
  const note = notesByQuestionId[record.questionId];
  const dimensions = scoreDimensionsForRecord(record);
  const prefill = note ? buildCodexDraftScorePrefill(note, dimensions) : null;
  const suggestedWinner = note?.suggestedWinner;
  const hasAstrBotSide = Boolean(record.astrbot);
  return stringList(record.failureTags).length > 0
    || suggestedWinner === "tie"
    || suggestedWinner === "unclear"
    || (prefill !== null && prefill.astrbotAverage > 0 && prefill.astrbotAverage < 4)
    || (hasAstrBotSide && evidenceRefCount(record) <= 1)
    || researchSourceGapCount(record) > 0;
}

export function businessReviewPriorityReason(
  record: EvalSideBySideRecord | undefined,
  notesByQuestionId: Record<string, EvalCodexReviewNote>,
): string {
  if (!record) return "No review row is currently available.";
  if (hasSavedHumanScore(record)) {
    return "Saved baseline row: audit the winner, score source, and notes.";
  }
  if (!isBusinessScoreReadyRecord(record)) {
    return "Repair-first row: fix data/source evidence before treating this as a replacement baseline sample.";
  }
  if (needsBusinessDecisionReview(record, notesByQuestionId)) {
    return "Decision row first: tied score, thin evidence, low AstrBot score, or research gap needs review.";
  }
  if (hasReadyCodexDraft(record, notesByQuestionId)) {
    return "Draft-ready row: confirm or adjust the Codex draft into a manual/GPT baseline score.";
  }
  return "Unscored row: choose one 1-5 total score for each answer.";
}

export function filterBusinessReviewRecords(
  records: EvalSideBySideRecord[],
  notesByQuestionId: Record<string, EvalCodexReviewNote>,
  filter: BusinessReviewFilter,
): EvalSideBySideRecord[] {
  switch (filter) {
    case "score_ready":
      return records.filter(isBusinessScoreReadyRecord);
    case "repair_first":
      return records.filter(record => !hasSavedHumanScore(record) && !isBusinessScoreReadyRecord(record));
    case "needs_score":
      return records.filter(record => !hasSavedHumanScore(record));
    case "needs_decision":
      return records.filter(record => needsBusinessDecisionReview(record, notesByQuestionId));
    case "draft_ready":
      return records.filter(record => !hasSavedHumanScore(record) && hasReadyCodexDraft(record, notesByQuestionId));
    case "saved":
      return records.filter(hasSavedHumanScore);
    default:
      return records;
  }
}

export function buildBusinessReviewWorkbench(
  records: EvalSideBySideRecord[],
  notesByQuestionId: Record<string, EvalCodexReviewNote>,
  minBusinessScores: number,
  filter: BusinessReviewFilter = "all",
): BusinessReviewWorkbench {
  const visibleRecords = filterBusinessReviewRecords(records, notesByQuestionId, filter);
  const scoredCount = records.filter(hasSavedHumanScore).length;
  const unscoredRecords = records.filter(record => !hasSavedHumanScore(record));
  const decisionNeededRecords = records.filter(record => needsBusinessDecisionReview(record, notesByQuestionId));
  const scoreReadyUnscoredRecords = unscoredRecords.filter(isBusinessScoreReadyRecord);
  const repairFirstUnscoredRecords = unscoredRecords.filter(record => !isBusinessScoreReadyRecord(record));
  const scoreReadyDecisionRecords = scoreReadyUnscoredRecords.filter(record => needsBusinessDecisionReview(record, notesByQuestionId));
  const draftReadyRecords = unscoredRecords.filter(record => hasReadyCodexDraft(record, notesByQuestionId));
  const scoreReadyDraftRecords = scoreReadyUnscoredRecords.filter(record => hasReadyCodexDraft(record, notesByQuestionId));
  const nextRecord = scoreReadyDecisionRecords[0]
    ?? scoreReadyDraftRecords[0]
    ?? scoreReadyUnscoredRecords[0]
    ?? decisionNeededRecords[0]
    ?? draftReadyRecords[0]
    ?? unscoredRecords[0]
    ?? records[0];

  return {
    totalRecords: records.length,
    visibleRecords: visibleRecords.length,
    scoredCount,
    unscoredCount: unscoredRecords.length,
    decisionNeededCount: decisionNeededRecords.length,
    draftReadyUnscoredCount: draftReadyRecords.length,
    scoreReadyUnscoredCount: scoreReadyUnscoredRecords.length,
    repairFirstUnscoredCount: repairFirstUnscoredRecords.length,
    savedCount: scoredCount,
    neededForReviewTarget: Math.max(0, minBusinessScores - scoredCount),
    nextComparisonId: nextRecord?.comparisonId ?? "",
    nextQuestionId: nextRecord?.questionId ?? "",
    nextQuestion: nextRecord?.question ?? "",
    nextCategory: nextRecord?.category ?? "",
    nextCountry: nextRecord?.country ?? "",
  };
}

export function buildBusinessBaselineActionPlan(
  readiness: BusinessReadinessGate,
  workbench: BusinessReviewWorkbench,
  codexTriage: CodexDraftTriage,
  judgeReady = false,
): BusinessBaselineActionPlan {
  const remainingToMinimum = Math.max(0, readiness.minBusinessScores - readiness.scoredBaseline);
  const progressBase = Math.max(1, readiness.minBusinessScores);
  const progressPercent = Math.max(0, Math.min(100, Math.round((readiness.scoredBaseline / progressBase) * 100)));
  const recommendedFilter: BusinessReviewFilter = workbench.scoreReadyUnscoredCount > 0
    ? "score_ready"
    : workbench.decisionNeededCount > 0
      ? "needs_decision"
      : workbench.draftReadyUnscoredCount > 0
      ? "draft_ready"
      : "needs_score";
  const sourceLabel = countBreakdownText(readiness.baselineSourceCounts, "manual/GPT none");
  const decisionLabel = workbench.decisionNeededCount > 0
    ? `${workbench.decisionNeededCount} decision row${workbench.decisionNeededCount === 1 ? "" : "s"} should be reviewed first`
    : "No tie/thin-evidence/research-source rows in the current review set";
  const draftLabel = codexTriage.readyDraftCount > 0
    ? `${codexTriage.readyDraftCount} Codex draft${codexTriage.readyDraftCount === 1 ? "" : "s"} ready for human confirmation`
    : "No Codex draft rows ready; score manually or run review harness";
  const judgeLabel = judgeReady ? "GPT judge ready" : "GPT judge not configured";
  if (readiness.businessBaselineReady) {
    return {
      tone: "ready",
      title: "Replacement baseline has enough scored records",
      description: "Business scoring coverage is sufficient. Review win rate and losing categories before any controlled default-route trial.",
      progressLabel: `${readiness.scoredBaseline}/${readiness.minBusinessScores} minimum scored`,
      progressPercent,
      recommendedFilter: "saved",
      reviewButtonLabel: "Review saved baseline",
      sourceLabel,
      draftLabel,
      decisionLabel,
      judgeLabel,
      remainingToMinimum: 0,
    };
  }
  return {
    tone: "pending",
    title: `Score ${remainingToMinimum} more manual/GPT baseline record${remainingToMinimum === 1 ? "" : "s"}`,
    description: "Codex drafts can prefill and triage, but only human-reviewed manual scores or GPT judge scores count toward replacement readiness.",
    progressLabel: `${readiness.scoredBaseline}/${readiness.minBusinessScores} minimum scored · ${readiness.pendingBaselineScoring} pending`,
    progressPercent,
    recommendedFilter,
    reviewButtonLabel: workbench.decisionNeededCount > 0
      ? "Review next score-ready row"
      : workbench.decisionNeededCount > 0
        ? "Review next decision row"
        : workbench.draftReadyUnscoredCount > 0
          ? "Review next draft row"
          : "Review next unscored row",
    sourceLabel,
    draftLabel,
    decisionLabel,
    judgeLabel,
    remainingToMinimum,
  };
}

export function buildBusinessReadinessHandoffText(options: BusinessReadinessHandoffOptions): string {
  const { readiness, actionPlan, workbench, codexTriage, judgePreflight, visibleRecordCount } = options;
  const remaining = Math.max(0, readiness.minBusinessScores - readiness.scoredBaseline);
  const judgeStatus = judgePreflight?.ready
    ? `ready · ${judgePreflight.provider.model}`
    : `not ready · ${judgePreflight?.reason || "preflight not loaded"}`;
  const nextRow = workbench.nextQuestionId
    ? `${workbench.nextQuestionId} · ${categoryLabel(workbench.nextCategory)} · ${workbench.nextCountry || "Market"}`
    : "none selected";
  return [
    "# AstrBot Business Readiness Handoff",
    "",
    "## Current Gate",
    `- Replacement ready: ${readiness.replacementReady ? "YES" : "NO"}`,
    `- Backend verdict: ${readiness.verdict}`,
    `- Engineering ready: ${readiness.engineeringClean ? "YES" : "NO"} (${readiness.errorTotal} execution errors, ${readiness.engineeringFailureTagTotal} engineering failure tags)`,
    `- Evidence ready: ${readiness.evidenceReady ? "YES" : "NO"} (${readiness.evidenceGapTotal} evidence/data gaps)`,
    `- Replacement baseline: ${readiness.scoredBaseline}/${readiness.minBusinessScores} scored, ${readiness.pendingBaselineScoring} pending, ${remaining} more needed`,
    `- Baseline sources: ${countBreakdownText(readiness.baselineSourceCounts, "manual/GPT none")}`,
    `- AstrBot win rate: ${Math.round(readiness.winRate * 100)}%`,
    "",
    "## Non-Negotiable Rule",
    "- Replacement baseline only counts saved `manual` or `llm_judge` scores.",
    "- Codex draft / `codex_review` rows are self-test and triage evidence only. They must not unlock replacement readiness by themselves.",
    "",
    "## Recommended Next Action",
    `- ${readiness.nextAction}`,
    `- UI action: ${actionPlan.reviewButtonLabel}`,
    `- Queue filter: ${actionPlan.recommendedFilter}`,
    `- Next row: ${nextRow}`,
    workbench.nextQuestion ? `- Question: ${workbench.nextQuestion}` : "- Question: n/a",
    "",
    "## Review Queue State",
    `- Visible records: ${visibleRecordCount}/${workbench.totalRecords}`,
    `- Score-ready unscored: ${workbench.scoreReadyUnscoredCount}`,
    `- Repair-first unscored: ${workbench.repairFirstUnscoredCount}`,
    `- Decision-first rows: ${workbench.decisionNeededCount}`,
    `- Codex drafts ready for confirmation: ${codexTriage.readyDraftCount}`,
    `- Draft average: AstrBot ${formatManualScore(codexTriage.avgAstrBotScore)} / CountryCopilot ${formatManualScore(codexTriage.avgCountryCopilotScore)}`,
    "",
    "## Judge Provider",
    `- Status: ${judgeStatus}`,
    `- Provider key source: ${judgePreflight?.provider.keySource || "OPENAI_API_KEY"}`,
    `- If not ready: copy judge setup, configure the key in backend .env, restart backend, run a 2-record smoke, then judge score-ready records.`,
  ].join("\n");
}

function tsvCell(value: string | number | undefined): string {
  return String(value ?? "")
    .replace(/\t/g, " ")
    .replace(/\r?\n/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function savedTotalText(record: EvalSideBySideRecord, side: "astrbot" | "countryCopilot"): string {
  const totals = record.humanScoring?.scoreTotals;
  if (!totals) return "";
  if (side === "astrbot") return totals.astrbotComplete ? formatManualScore(totals.astrbot) : "";
  return totals.countryCopilotComplete ? formatManualScore(totals.countryCopilot) : "";
}

export function buildBusinessScoringSheetText(
  records: EvalSideBySideRecord[],
  options: BusinessScoringSheetOptions = {},
): string {
  const notesByQuestionId = options.notesByQuestionId ?? {};
  const maxRecords = Math.max(1, options.maxRecords ?? (records.length || 1));
  const visibleRecords = (options.includeSaved ? records : records.filter(record => !hasSavedHumanScore(record)))
    .slice(0, maxRecords);
  const headers = [
    "question_id",
    "category",
    "country",
    "question",
    "review_status",
    "draft_winner",
    "draft_astrbot",
    "draft_copilot",
    "saved_source",
    "saved_winner",
    "saved_astrbot",
    "saved_copilot",
    "astrbot_total_1_to_5",
    "copilot_total_1_to_5",
    "winner",
    "notes",
    "failure_tags",
    "astrbot_evidence",
    "astrbot_answer_preview",
    "countrycopilot_answer_preview",
  ];
  const rows = visibleRecords.map(record => {
    const dimensions = scoreDimensionsForRecord(record);
    const note = notesByQuestionId[record.questionId];
    const codexDraft = note ? buildCodexDraftScorePrefill(note, dimensions) : null;
    const saved = hasSavedHumanScore(record);
    const reviewStatus = saved
      ? `saved:${record.humanScoring?.source || "manual"}`
      : codexDraft?.complete
        ? `codex_draft:${note?.uiStatus || "ready"}`
        : "needs_manual";
    const evidenceSummary = [
      `tool=${record.astrbot?.selectedTool || "n/a"}`,
      `status=${record.astrbot?.answerStatus || record.astrbot?.status || "n/a"}`,
      `refs=${record.astrbot?.evidenceRefCount ?? 0}`,
      `missing=${listPreview(record.astrbot?.missingEvidence, ["name", "reason", "impact"], "none", 3)}`,
    ].join("; ");
    return [
      record.questionId,
      categoryLabel(record.category),
      record.country || "",
      record.question,
      reviewStatus,
      codexDraft?.winner || "",
      codexDraft ? formatManualScore(codexDraft.astrbotAverage) : "",
      codexDraft ? formatManualScore(codexDraft.countryCopilotAverage) : "",
      record.humanScoring?.source || "",
      record.humanScoring?.winner || "",
      savedTotalText(record, "astrbot"),
      savedTotalText(record, "countryCopilot"),
      "",
      "",
      "",
      "",
      stringList(record.failureTags).join(", "),
      evidenceSummary,
      truncateText(answerPreviewText(record.astrbot?.answerPreview || record.astrbotAnswer || record.astrbot?.error, ""), 520),
      truncateText(answerPreviewText(record.countryCopilot?.answerPreview || record.copilotAnswer || record.countryCopilot?.error, ""), 520),
    ].map(tsvCell).join("\t");
  });
  return [headers.join("\t"), ...rows].join("\n");
}

function headerIndex(headers: string[], key: string): number {
  return headers.findIndex(header => header.trim().toLowerCase() === key);
}

function tsvScoreValue(value: string | undefined): number | null {
  const numeric = Number(String(value ?? "").trim());
  if (!Number.isFinite(numeric) || numeric < 1 || numeric > 5) return null;
  return Math.round(numeric);
}

function scoreValueFromUnknown(value: unknown): number | null {
  const numeric = typeof value === "number" ? value : Number(String(value ?? "").trim());
  if (!Number.isFinite(numeric) || numeric < 1 || numeric > 5) return null;
  return Math.round(numeric);
}

function stringFromUnknown(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function scoreMapFromUnknown(dimensions: EvalScoreDimension[], value: unknown): Record<string, number> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  const source = value as Record<string, unknown>;
  const directScores: Record<string, number> = {};
  const allScores: number[] = [];
  for (const [key, raw] of Object.entries(source)) {
    const score = scoreValueFromUnknown(raw);
    if (score === null) continue;
    allScores.push(score);
    directScores[key] = score;
  }
  const fallback = allScores.length > 0
    ? Math.round(allScores.reduce((sum, score) => sum + score, 0) / allScores.length)
    : null;
  const result: Record<string, number> = {};
  for (const dimension of dimensions) {
    const direct = scoreValueFromUnknown(source[dimension.key]);
    const score = direct ?? fallback;
    if (score !== null) result[dimension.key] = score;
  }
  return result;
}

function scoreValueFromRecord(source: Record<string, unknown>, keys: string[]): number | null {
  for (const key of keys) {
    const score = scoreValueFromUnknown(source[key]);
    if (score !== null) return score;
  }
  return null;
}

function scoreMapFromTotalOrUnknown(
  dimensions: EvalScoreDimension[],
  source: Record<string, unknown>,
  totalKeys: string[],
  mapValue: unknown,
): Record<string, number> {
  const totalScore = scoreValueFromRecord(source, totalKeys);
  return totalScore !== null ? filledScoreMap(dimensions, totalScore) : scoreMapFromUnknown(dimensions, mapValue);
}

function referenceJudgeNotes(record: Record<string, unknown>, sourceLabel: string): string {
  const body = stringFromUnknown(record.notes)
    || stringFromUnknown(record.humanNotes)
    || stringFromUnknown(record.rationale)
    || stringFromUnknown(record.reason);
  const reviewer = stringFromUnknown(record.reviewer)
    || stringFromUnknown(record.judge)
    || sourceLabel;
  const marker = "[Accepted reference judge draft]";
  const meta = `source=${sourceLabel}; reviewer=${reviewer || "unknown"}`;
  return body ? `${marker}\n${meta}\n\n${body}` : `${marker}\n${meta}`;
}

function referenceJudgeProviderMetadata(
  root: Record<string, unknown>,
  record?: Record<string, unknown>,
): ScoringDraft["judgeProvider"] {
  const rowProvider = record?.judgeProvider && typeof record.judgeProvider === "object" && !Array.isArray(record.judgeProvider)
    ? record.judgeProvider as Record<string, unknown>
    : {};
  const rootProvider = root.judgeProvider && typeof root.judgeProvider === "object" && !Array.isArray(root.judgeProvider)
    ? root.judgeProvider as Record<string, unknown>
    : {};
  const source = stringFromUnknown(rowProvider.source)
    || stringFromUnknown(record?.source)
    || stringFromUnknown(rootProvider.source)
    || stringFromUnknown(root.source)
    || stringFromUnknown(root.judge)
    || "reference_judge";
  return {
    source,
    pathId: stringFromUnknown(rowProvider.pathId)
      || stringFromUnknown(record?.pathId)
      || stringFromUnknown(rootProvider.pathId)
      || stringFromUnknown(root.pathId)
      || stringFromUnknown(root.referencePathId),
    label: stringFromUnknown(rowProvider.label)
      || stringFromUnknown(record?.label)
      || stringFromUnknown(rootProvider.label)
      || stringFromUnknown(root.label),
    provider: stringFromUnknown(rowProvider.provider)
      || stringFromUnknown(record?.provider)
      || stringFromUnknown(rootProvider.provider)
      || stringFromUnknown(root.provider),
    model: stringFromUnknown(rowProvider.model)
      || stringFromUnknown(record?.model)
      || stringFromUnknown(rootProvider.model)
      || stringFromUnknown(root.model),
    apiBase: stringFromUnknown(rowProvider.apiBase)
      || stringFromUnknown(record?.apiBase)
      || stringFromUnknown(rootProvider.apiBase)
      || stringFromUnknown(root.apiBase),
    keySource: stringFromUnknown(rowProvider.keySource)
      || stringFromUnknown(record?.keySource)
      || stringFromUnknown(rootProvider.keySource)
      || stringFromUnknown(root.keySource),
  };
}

export function parseReferenceJudgeScoreDrafts(
  records: EvalSideBySideRecord[],
  text: string,
): ReferenceJudgeScoreImportResult {
  const trimmed = text.trim();
  if (!trimmed) {
    return {
      drafts: {},
      matchedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      firstComparisonId: "",
      errors: ["Paste the reference judge JSON output first."],
      importedSourceLabel: "",
    };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return {
      drafts: {},
      matchedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      firstComparisonId: "",
      errors: ["Reference judge output must be valid JSON."],
      importedSourceLabel: "",
    };
  }

  const root = parsed && typeof parsed === "object" && !Array.isArray(parsed)
    ? parsed as Record<string, unknown>
    : {};
  const rawRows = Array.isArray(parsed)
    ? parsed
    : Array.isArray(root.records)
      ? root.records
      : [];
  const importedSourceLabel = stringFromUnknown(root.source)
    || stringFromUnknown(root.model)
    || stringFromUnknown(root.judge)
    || "reference_judge";
  if (rawRows.length === 0) {
    return {
      drafts: {},
      matchedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      firstComparisonId: "",
      errors: ["Reference judge JSON needs a records array."],
      importedSourceLabel,
    };
  }

  const recordsByQuestionId = new Map(records.map(record => [record.questionId, record]));
  const drafts: Record<string, ScoringDraft> = {};
  const errors: string[] = [];
  let matchedCount = 0;
  let skippedCount = 0;
  let firstComparisonId = "";

  for (const [index, row] of rawRows.entries()) {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
      skippedCount += 1;
      errors.push(`Record ${index + 1}: expected an object.`);
      continue;
    }
    const item = row as Record<string, unknown>;
    const questionId = stringFromUnknown(item.questionId) || stringFromUnknown(item.question_id);
    const record = recordsByQuestionId.get(questionId);
    if (!record) {
      skippedCount += 1;
      errors.push(`Record ${index + 1}: unknown questionId ${questionId || "(blank)"}.`);
      continue;
    }
    matchedCount += 1;
    if (hasSavedHumanScore(record)) {
      skippedCount += 1;
      errors.push(`Record ${index + 1}: ${questionId} is already saved; skipped to avoid overwriting a confirmed score.`);
      continue;
    }
    const dimensions = scoreDimensionsForRecord(record);
    if (dimensions.length === 0) {
      skippedCount += 1;
      errors.push(`Record ${index + 1}: ${questionId} has no score schema.`);
      continue;
    }
    const astrbotScores = scoreMapFromTotalOrUnknown(
      dimensions,
      item,
      ["astrbotTotal", "astrbot_total", "astrbotScore", "astrbot_score"],
      item.astrbotScores,
    );
    const countryCopilotScores = scoreMapFromTotalOrUnknown(
      dimensions,
      item,
      [
        "countryCopilotTotal",
        "country_copilot_total",
        "countryCopilotScore",
        "country_copilot_score",
        "copilotTotal",
        "copilot_total",
        "copilotScore",
        "copilot_score",
      ],
      item.countryCopilotScores ?? item.copilotScores ?? item.country_copilot_scores,
    );
    const astrbotAverage = completeAverage(dimensions, astrbotScores) || averageScoreMap(astrbotScores);
    const countryAverage = completeAverage(dimensions, countryCopilotScores) || averageScoreMap(countryCopilotScores);
    if (
      completedScoreCount(dimensions, astrbotScores) !== dimensions.length
      || completedScoreCount(dimensions, countryCopilotScores) !== dimensions.length
    ) {
      skippedCount += 1;
      errors.push(`Record ${index + 1}: ${questionId} needs complete AstrBot and CountryCopilot scores.`);
      continue;
    }
    const winner = stringFromUnknown(item.winner) || inferredWinnerFromScores(astrbotAverage, countryAverage);
    drafts[record.comparisonId] = {
      status: "scored",
      winner,
      notes: referenceJudgeNotes(item, importedSourceLabel),
      failureTags: Array.isArray(item.failureTags)
        ? stringList(item.failureTags)
        : commaStringToList(stringFromUnknown(item.failureTags)),
      astrbotScores,
      countryCopilotScores,
      judgeProvider: referenceJudgeProviderMetadata(root, item),
    };
    if (!firstComparisonId) firstComparisonId = record.comparisonId;
  }

  return {
    drafts,
    matchedCount,
    appliedCount: Object.keys(drafts).length,
    skippedCount,
    firstComparisonId,
    errors,
    importedSourceLabel,
  };
}

export function parseBusinessScoringSheetDrafts(
  records: EvalSideBySideRecord[],
  text: string,
): BusinessScoringSheetImportResult {
  const lines = text
    .split(/\r?\n/)
    .map(line => line.trimEnd())
    .filter(line => line.trim().length > 0);
  if (lines.length < 2) {
    return {
      drafts: {},
      matchedCount: 0,
      appliedCount: 0,
      skippedCount: 0,
      firstComparisonId: "",
      errors: ["Paste a TSV sheet with a header and at least one scored row."],
    };
  }

  const headers = lines[0].split("\t").map(header => header.trim().toLowerCase());
  const questionIndex = headerIndex(headers, "question_id");
  const astrbotIndex = headerIndex(headers, "astrbot_total_1_to_5");
  const copilotIndex = headerIndex(headers, "copilot_total_1_to_5");
  const winnerIndex = headerIndex(headers, "winner");
  const notesIndex = headerIndex(headers, "notes");
  const tagsIndex = headerIndex(headers, "failure_tags");
  const requiredMissing = [
    { key: "question_id", index: questionIndex },
    { key: "astrbot_total_1_to_5", index: astrbotIndex },
    { key: "copilot_total_1_to_5", index: copilotIndex },
  ].filter(item => item.index < 0).map(item => item.key);
  if (requiredMissing.length > 0) {
    return {
      drafts: {},
      matchedCount: 0,
      appliedCount: 0,
      skippedCount: lines.length - 1,
      firstComparisonId: "",
      errors: [`Missing required column(s): ${requiredMissing.join(", ")}`],
    };
  }

  const recordsByQuestionId = new Map(records.map(record => [record.questionId, record]));
  const drafts: Record<string, ScoringDraft> = {};
  const errors: string[] = [];
  let matchedCount = 0;
  let skippedCount = 0;
  let firstComparisonId = "";

  for (const [rowIndex, line] of lines.slice(1).entries()) {
    const cells = line.split("\t");
    const questionId = cells[questionIndex]?.trim() ?? "";
    const record = recordsByQuestionId.get(questionId);
    if (!record) {
      skippedCount += 1;
      if (questionId) errors.push(`Row ${rowIndex + 2}: unknown question_id ${questionId}.`);
      continue;
    }
    matchedCount += 1;
    if (hasSavedHumanScore(record)) {
      skippedCount += 1;
      errors.push(`Row ${rowIndex + 2}: ${questionId} is already saved; skipped to avoid overwriting a confirmed score.`);
      continue;
    }
    const astrbotScore = tsvScoreValue(cells[astrbotIndex]);
    const copilotScore = tsvScoreValue(cells[copilotIndex]);
    if (astrbotScore === null || copilotScore === null) {
      skippedCount += 1;
      errors.push(`Row ${rowIndex + 2}: ${questionId} needs both AstrBot and Copilot totals between 1 and 5.`);
      continue;
    }
    const dimensions = scoreDimensionsForRecord(record);
    if (dimensions.length === 0) {
      skippedCount += 1;
      errors.push(`Row ${rowIndex + 2}: ${questionId} has no score schema.`);
      continue;
    }
    const winner = cells[winnerIndex]?.trim() || inferredWinnerFromScores(astrbotScore, copilotScore);
    drafts[record.comparisonId] = {
      status: "scored",
      winner,
      notes: cells[notesIndex]?.trim() ?? "",
      failureTags: commaStringToList(cells[tagsIndex] ?? ""),
      astrbotScores: filledScoreMap(dimensions, astrbotScore),
      countryCopilotScores: filledScoreMap(dimensions, copilotScore),
    };
    if (!firstComparisonId) firstComparisonId = record.comparisonId;
  }

  return {
    drafts,
    matchedCount,
    appliedCount: Object.keys(drafts).length,
    skippedCount,
    firstComparisonId,
    errors,
  };
}

export function recordsForImportedManualSave(
  records: EvalSideBySideRecord[],
  drafts: Record<string, ScoringDraft>,
  importedComparisonIds: string[],
): EvalSideBySideRecord[] {
  const importedIds = new Set(importedComparisonIds);
  return records.filter(record => {
    if (!importedIds.has(record.comparisonId) || hasSavedHumanScore(record)) return false;
    const draft = drafts[record.comparisonId];
    if (!draft) return false;
    const dimensions = scoreDimensionsForRecord(record);
    if (dimensions.length === 0) return false;
    return completedScoreCount(dimensions, draft.astrbotScores) === dimensions.length
      && completedScoreCount(dimensions, draft.countryCopilotScores) === dimensions.length;
  });
}

export function findNextReviewComparisonId(
  records: EvalSideBySideRecord[],
  notesByQuestionId: Record<string, EvalCodexReviewNote>,
  currentComparisonId: string,
  filter: BusinessReviewFilter = "all",
): string {
  const currentIndex = records.findIndex(record => record.comparisonId === currentComparisonId);
  const candidates = filterBusinessReviewRecords(records, notesByQuestionId, filter)
    .filter(record => record.comparisonId !== currentComparisonId);
  if (candidates.length === 0) return "";
  if (currentIndex < 0) return candidates[0]?.comparisonId ?? "";
  const nextAfterCurrent = candidates.find(record => records.findIndex(item => item.comparisonId === record.comparisonId) > currentIndex);
  return nextAfterCurrent?.comparisonId ?? candidates[0]?.comparisonId ?? "";
}

export function draftRecordsForCodexAcceptance(
  records: EvalSideBySideRecord[],
  notesByQuestionId: Record<string, EvalCodexReviewNote>,
  neededForReviewTarget: number,
): EvalSideBySideRecord[] {
  if (neededForReviewTarget <= 0) return [];
  return records
    .filter(record => !hasSavedHumanScore(record) && hasReadyCodexDraft(record, notesByQuestionId))
    .slice(0, neededForReviewTarget);
}

function winnerText(winner: string | undefined): string {
  switch (winner) {
    case "astrbot":
      return "AstrBot wins";
    case "countryCopilot":
      return "CountryCopilot wins";
    case "tie":
      return "Tie";
    case "unclear":
      return "Unclear";
    default:
      return "Pending";
  }
}

function inferredWinnerFromScores(astrbotScore: number, countryCopilotScore: number): string {
  if (astrbotScore <= 0 || countryCopilotScore <= 0) return "";
  if (astrbotScore > countryCopilotScore) return "astrbot";
  if (countryCopilotScore > astrbotScore) return "countryCopilot";
  return "tie";
}

function effectiveWinner(scoring: ScoringDraft, astrbotScore: number, countryCopilotScore: number, complete: boolean): string {
  if (!complete) return scoring.winner || "";
  return scoring.winner || inferredWinnerFromScores(astrbotScore, countryCopilotScore);
}

function effectiveStatus(scoring: ScoringDraft, complete: boolean): string {
  if (scoring.status === "skipped") return "skipped";
  return complete ? "scored" : "pending";
}

function scoreDecisionText(status: string, winner: string, complete: boolean): string {
  if (status === "skipped") return "Skipped";
  if (!complete) return "Needs scores";
  return winnerText(winner);
}

export function scoreProgressText(
  astrbotCompleted: number,
  countryCompleted: number,
  required: number,
  astrbotAverage: number,
  countryAverage: number,
): string {
  if (required <= 0) return "No scoring schema";
  const astrbotReady = required > 0 && astrbotCompleted === required;
  const countryReady = required > 0 && countryCompleted === required;
  if (astrbotReady && countryReady) {
    return `Total scores ready: AstrBot ${formatManualScore(astrbotAverage)} · Copilot ${formatManualScore(countryAverage)}`;
  }
  if (astrbotReady) {
    return `AstrBot total ${formatManualScore(astrbotAverage)} selected · pick Copilot total`;
  }
  if (countryReady) {
    return `Pick AstrBot total · Copilot total ${formatManualScore(countryAverage)} selected`;
  }
  return "Pick one 1-5 total for AstrBot and Copilot";
}

export function scoreCompletionText(astrbotCompleted: number, countryCompleted: number, required: number): string {
  if (required <= 0) return "No dimensions";
  const astrbotReady = astrbotCompleted === required;
  const countryReady = countryCompleted === required;
  return `Total scores: AstrBot ${astrbotReady ? "selected" : "pending"} · Copilot ${countryReady ? "selected" : "pending"}`;
}

export function canSaveBusinessScore(astrbotCompleted: number, countryCompleted: number, required: number): boolean {
  return required > 0 && astrbotCompleted === required && countryCompleted === required;
}

function initialScoringDraft(record: EvalSideBySideRecord): ScoringDraft {
  const dimensions = scoreDimensionsForRecord(record);
  const savedStatus = record.humanScoring?.status ?? "pending";
  const complete = record.humanScoring?.scoreTotals?.complete;
  return {
    status: savedStatus === "scored" && complete === false ? "pending" : savedStatus,
    winner: complete === false ? "" : record.humanScoring?.winner ?? "",
    notes: record.humanScoring?.notes ?? "",
    failureTags: stringList(record.failureTags),
    astrbotScores: defaultScoreMap(dimensions, record.humanScoring?.astrbotScores),
    countryCopilotScores: defaultScoreMap(dimensions, record.humanScoring?.countryCopilotScores ?? record.humanScoring?.copilotScores),
  };
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

async function writeClipboardText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    // Fall back to the textarea path below for local/dev browsers with stricter clipboard permissions.
  }
  if (typeof document === "undefined" || !document.body) return false;
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "true");
  textarea.style.position = "fixed";
  textarea.style.left = "-9999px";
  textarea.style.top = "0";
  document.body.appendChild(textarea);
  textarea.focus();
  textarea.select();
  try {
    return document.execCommand("copy");
  } catch {
    return false;
  } finally {
    document.body.removeChild(textarea);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textFromRecord(value: unknown, keys: string[]): string {
  if (!isRecord(value)) return "";
  for (const key of keys) {
    const raw = value[key];
    if (typeof raw === "string" && raw.trim()) return raw.trim();
    if (typeof raw === "number") return String(raw);
  }
  return "";
}

function numberFromRecord(value: unknown, keys: string[]): number {
  if (!isRecord(value)) return 0;
  for (const key of keys) {
    const raw = value[key];
    if (typeof raw === "number" && Number.isFinite(raw)) return raw;
    if (typeof raw === "string" && raw.trim()) {
      const numeric = Number(raw);
      if (Number.isFinite(numeric)) return numeric;
    }
  }
  return 0;
}

function listPreview(value: unknown, keys: string[], fallback = "—", maxItems = 4): string {
  if (!Array.isArray(value) || value.length === 0) return fallback;
  const labels = value
    .slice(0, maxItems)
    .map(item => {
      if (typeof item === "string") return item.trim();
      if (isRecord(item)) return textFromRecord(item, keys);
      return "";
    })
    .filter(Boolean);
  return labels.length > 0 ? labels.join(", ") : fallback;
}

function artifactDataRows(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) {
    return value.filter(isRecord);
  }
  if (isRecord(value) && Array.isArray(value.rows)) {
    return value.rows.filter(isRecord);
  }
  return [];
}

function artifactValueText(value: unknown): string {
  if (typeof value === "number") {
    return Number.isInteger(value) ? value.toLocaleString() : value.toLocaleString(undefined, { maximumFractionDigits: 1 });
  }
  if (typeof value === "string") return businessDiagnosticDisplayText(value.trim());
  return "";
}

function artifactRowPreview(row: Record<string, unknown>): string {
  const preferredPairs = [
    ["dimension", "signal", "evidence", "businessImplication"],
    ["model", "segment", "keyAdvantage", "gapVsOj"],
    ["model", "msrp", "pricePosition", "action"],
    ["model", "trim", "localMsrp", "decisionUse"],
    ["feature", "validationData", "currentStatus", "priority"],
    ["feature", "targetModel", "gap", "customerValue"],
    ["section", "evidence", "businessUse", "nextAction"],
    ["label", "value", "unit", "source"],
    ["candidateRole", "model", "sourceStatus", "searchQuery"],
    ["candidateRole", "model", "sourceScope", "searchQuery"],
    ["businessVariant", "materialCode", "lifecycle", "action"],
  ];
  for (const keys of preferredPairs) {
    const parts = keys
      .map(key => artifactValueText(row[key]))
      .filter(Boolean);
    if (parts.length >= 2) return parts.slice(0, 4).join(" · ");
  }
  return Object.entries(row)
    .filter(([key]) => !["evidenceRef", "sourceRaw"].includes(key))
    .map(([key, value]) => `${key}: ${artifactValueText(value)}`)
    .filter(item => !item.endsWith(": "))
    .slice(0, 4)
    .join(" · ");
}

export function buildBusinessArtifactPreviews(value: unknown): BusinessArtifactPreview[] {
  if (!Array.isArray(value)) return [];
  return value
    .filter(isRecord)
    .map((artifact, index): BusinessArtifactPreview | null => {
      const type = textFromRecord(artifact, ["type"]) || "artifact";
      const title = textFromRecord(artifact, ["title", "id"]) || `${type} ${index + 1}`;
      const data = artifact.data;
      let lines: string[] = [];
      if (type === "report_block" && isRecord(data)) {
        lines = [
          textFromRecord(data, ["keyMessage"]),
          textFromRecord(data, ["productImplication"]),
          textFromRecord(data, ["nextAction"]),
        ].filter(Boolean);
      } else {
        lines = artifactDataRows(data)
          .map(artifactRowPreview)
          .filter(Boolean)
          .slice(0, 3);
      }
      if (lines.length === 0) {
        const subtitle = textFromRecord(artifact, ["subtitle", "fallbackReason"]);
        if (subtitle) lines = [businessDiagnosticDisplayText(subtitle)];
      }
      if (lines.length === 0) return null;
      const sourceRefs = Array.isArray(artifact.sourceEvidenceRefs) ? artifact.sourceEvidenceRefs.length : 0;
      return {
        id: textFromRecord(artifact, ["id"]) || `${type}_${index}`,
        type,
        title,
        lines,
        meta: sourceRefs > 0 ? `${sourceRefs} refs` : readableStatusLabel(type),
      };
    })
    .filter((item): item is BusinessArtifactPreview => item !== null)
    .slice(0, 4);
}

function missingEvidenceItems(value: unknown): EvidenceRepairItem["missingEvidence"] {
  if (!Array.isArray(value)) return [];
  return value
    .map(item => {
      if (typeof item === "string" && item.trim()) {
        return { name: item.trim(), reason: "", impact: "weakens_answer" };
      }
      if (!isRecord(item)) return null;
      const name = textFromRecord(item, ["name", "label", "id"]);
      if (!name) return null;
      return {
        name,
        reason: textFromRecord(item, ["reason", "description", "message"]),
        impact: textFromRecord(item, ["impact", "severity"]) || "weakens_answer",
      };
    })
    .filter((item): item is EvidenceRepairItem["missingEvidence"][number] => item !== null);
}

function recommendedActionItems(value: unknown): EvidenceRepairItem["recommendedActions"] {
  if (!Array.isArray(value)) return [];
  return value
    .map(item => {
      if (typeof item === "string" && item.trim()) {
        return { action: evidenceRepairDisplayText(item), rationale: "", priority: "P1" };
      }
      if (!isRecord(item)) return null;
      const action = textFromRecord(item, ["action", "recommendation", "label", "nextAction"]);
      if (!action) return null;
      return {
        action: evidenceRepairDisplayText(action),
        rationale: evidenceRepairDisplayText(textFromRecord(item, ["rationale", "reason", "description"])),
        priority: textFromRecord(item, ["priority"]) || "P1",
      };
    })
    .filter((item): item is EvidenceRepairItem["recommendedActions"][number] => item !== null);
}

function evidenceRepairTaskItems(value: unknown): EvalEvidenceRepairTask[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item, index): EvalEvidenceRepairTask | null => {
      if (!isRecord(item)) return null;
      const title = textFromRecord(item, ["title", "name", "label"]);
      if (!title) return null;
      const taskType = textFromRecord(item, ["taskType", "type"]) || "repair_task";
      return {
        taskId: textFromRecord(item, ["taskId", "id"]) || `repair_task_${index + 1}`,
        taskType,
        title: evidenceRepairDisplayText(title),
        input: evidenceRepairDisplayText(textFromRecord(item, ["input", "inputText", "description"])),
        output: evidenceRepairDisplayText(textFromRecord(item, ["output", "expectedOutput", "result"])),
        owner: textFromRecord(item, ["owner"]) || "AstrBot Eval",
        priority: textFromRecord(item, ["priority"]) || "P1",
        status: textFromRecord(item, ["status"]) || "todo",
        evidenceName: textFromRecord(item, ["evidenceName", "evidence"]),
        sourceCandidates: stringList(item.sourceCandidates),
        commandHint: evidenceRepairDisplayText(textFromRecord(item, ["commandHint", "command"])),
      };
    })
    .filter((item): item is EvalEvidenceRepairTask => item !== null);
}

function sourceRepairCandidateItems(value: unknown): EvalSourceRepairCandidate[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item): EvalSourceRepairCandidate | null => {
      if (!isRecord(item)) return null;
      const sourceCode = textFromRecord(item, ["sourceCode", "source_code"]);
      const brand = textFromRecord(item, ["brand"]);
      const model = textFromRecord(item, ["model", "jatoModel", "jato_model"]);
      if (!sourceCode && !brand && !model) return null;
      return {
        sourceCode,
        brand,
        model,
        sourceUrl: textFromRecord(item, ["sourceUrl", "source_url", "url"]),
        relativePath: textFromRecord(item, ["relativePath", "relative_path", "path"]),
        draftStatus: textFromRecord(item, ["draftStatus", "draft_status", "dataStatus"]),
        currentPriceRows: numberFromRecord(item, ["currentPriceRows", "current_price_rows"]),
        candidateSourceType: textFromRecord(item, ["candidateSourceType", "candidate_source_type"]),
        candidateDomain: textFromRecord(item, ["candidateDomain", "candidate_domain", "domain"]),
        sourceSearchQuery: textFromRecord(item, ["sourceSearchQuery", "source_search_query", "query"]),
      };
    })
    .filter((item): item is EvalSourceRepairCandidate => item !== null);
}

function evidenceRepairDisplayText(value: string): string {
  return value
    .replace(/Use Data Ops MSRP source workflow/g, "用 Data Ops MSRP 来源流程")
    .replace(/then rerun/gi, "然后重跑")
    .replace(/Own-model current price/gi, "本车型当前价格")
    .replace(/这题需要先给业务立场，再展开证据；当前判断是 /g, "")
    .replace(/这题需要先给业务立场/g, "需要先给出明确业务立场")
    .replace(/materialize current_price rows/gi, "生成当前价格行")
    .replace(/current_price rows/gi, "当前价格行")
    .replace(/current_prices table/gi, "当前价格表")
    .replace(/current_prices/gi, "当前价格表")
    .replace(/source candidates materialized/gi, "个来源候选已生成价格行")
    .replace(/own-model source missing/gi, "本车型来源缺失")
    .replace(/competitor current price available own model missing/gi, "已有竞品当前价格，本车型来源缺失")
    .replace(/source draft only not price evidence/gi, "来源草稿尚未转成价格证据")
    .replace(/external policy source candidates/gi, "政策官方来源候选")
    .replace(/candidate search query/gi, "候选搜索查询")
    .replace(/Next:\s*/g, "下一步：")
    .replace(/evidenceRefs?/g, "可引用证据")
    .replace(/EvidenceRefs?/g, "可引用证据")
    .replace(/partially_aligned/g, "部分对齐")
    .replace(/confidence high/g, "置信度高")
    .replace(/confidence medium/g, "置信度中")
    .replace(/confidence low/g, "置信度低")
    .replace(/置信度 high/g, "置信度高")
    .replace(/置信度 medium/g, "置信度中")
    .replace(/置信度 low/g, "置信度低")
    .replace(/\s+/g, " ")
    .trim();
}

function sourceRepairCandidatesFromValue(value: unknown): EvalSourceRepairCandidates | undefined {
  if (!isRecord(value)) return undefined;
  const ownModel = sourceRepairCandidateItems(value.ownModel);
  const competitorCorridor = sourceRepairCandidateItems(value.competitorCorridor);
  const candidateCount = numberFromRecord(value, ["candidateCount"]) || ownModel.length + competitorCorridor.length;
  const dataStatus = textFromRecord(value, ["dataStatus"]);
  const missingOwnModelSource = typeof value.missingOwnModelSource === "boolean"
    ? value.missingOwnModelSource
    : undefined;
  if (candidateCount === 0 && !dataStatus && missingOwnModelSource === undefined) return undefined;
  return {
    dataStatus,
    missingOwnModelSource,
    candidateCount,
    materializedCandidateCount: numberFromRecord(value, ["materializedCandidateCount", "materialized_candidate_count"]),
    ownModel,
    competitorCorridor,
  };
}

function sourceRepairSummaryText(candidates: EvalSourceRepairCandidates | undefined): string {
  if (!candidates) return "";
  const candidateCount = candidates.candidateCount ?? (candidates.ownModel?.length ?? 0) + (candidates.competitorCorridor?.length ?? 0);
  const materialized = candidates.materializedCandidateCount ?? 0;
  const parts = [];
  if (candidateCount > 0) {
    parts.push(
      isPolicySourceRepairCandidates(candidates)
        ? `${materialized}/${candidateCount} 个政策来源候选已确认`
        : `${materialized}/${candidateCount} 个来源候选已生成价格行`,
    );
  }
  if (candidates.missingOwnModelSource) parts.push("本车型来源缺失");
  if (candidates.dataStatus) parts.push(evidenceRepairDisplayText(candidates.dataStatus.replace(/_/g, " ")));
  return evidenceRepairDisplayText(parts.join("; "));
}

function isPolicySourceRepairCandidates(candidates: EvalSourceRepairCandidates | undefined): boolean {
  return candidates?.dataStatus === "external_policy_source_candidates";
}

function repairSummaryFromValue(
  value: unknown,
  missingEvidence: EvidenceRepairItem["missingEvidence"],
  sourceRepairCandidates: EvalSourceRepairCandidates | undefined,
  repairAction: string | undefined,
  failureTags: string[],
): EvalEvidenceRepairSummary {
  const blockingEvidenceCount = missingEvidence.filter(item => item.impact === "blocking").length;
  const weakEvidenceCount = missingEvidence.filter(item => item.impact === "weakens_answer").length;
  const firstBlocking = missingEvidence.find(item => item.impact === "blocking");
  const fallbackPrimaryGap = firstBlocking?.name ?? missingEvidence[0]?.name ?? failureTags[0] ?? "";
  const sourceCandidateCount = sourceRepairCandidates?.candidateCount
    ?? (sourceRepairCandidates?.ownModel?.length ?? 0) + (sourceRepairCandidates?.competitorCorridor?.length ?? 0);
  const fallback: EvalEvidenceRepairSummary = {
    primaryGap: fallbackPrimaryGap,
    missingEvidenceCount: missingEvidence.length,
    blockingEvidenceCount,
    weakEvidenceCount,
    sourceCandidateCount,
    ownModelCandidateCount: sourceRepairCandidates?.ownModel?.length ?? 0,
    competitorCandidateCount: sourceRepairCandidates?.competitorCorridor?.length ?? 0,
    materializedCandidateCount: sourceRepairCandidates?.materializedCandidateCount ?? 0,
    missingOwnModelSource: sourceRepairCandidates?.missingOwnModelSource,
    dataStatus: sourceRepairCandidates?.dataStatus,
    sourceSummary: sourceRepairSummaryText(sourceRepairCandidates),
    nextStep: repairAction,
  };
  if (!isRecord(value)) return fallback;
  return {
    primaryGap: textFromRecord(value, ["primaryGap"]) || fallback.primaryGap,
    missingEvidenceCount: numberFromRecord(value, ["missingEvidenceCount"]) || fallback.missingEvidenceCount,
    blockingEvidenceCount: numberFromRecord(value, ["blockingEvidenceCount"]) || fallback.blockingEvidenceCount,
    weakEvidenceCount: numberFromRecord(value, ["weakEvidenceCount"]) || fallback.weakEvidenceCount,
    sourceCandidateCount: numberFromRecord(value, ["sourceCandidateCount"]) || fallback.sourceCandidateCount,
    ownModelCandidateCount: numberFromRecord(value, ["ownModelCandidateCount"]) || fallback.ownModelCandidateCount,
    competitorCandidateCount: numberFromRecord(value, ["competitorCandidateCount"]) || fallback.competitorCandidateCount,
    materializedCandidateCount: numberFromRecord(value, ["materializedCandidateCount"]) || fallback.materializedCandidateCount,
    missingOwnModelSource: typeof value.missingOwnModelSource === "boolean" ? value.missingOwnModelSource : fallback.missingOwnModelSource,
    dataStatus: textFromRecord(value, ["dataStatus"]) || fallback.dataStatus,
    sourceSummary: evidenceRepairDisplayText(textFromRecord(value, ["sourceSummary"]) || fallback.sourceSummary || ""),
    nextStep: evidenceRepairDisplayText(textFromRecord(value, ["nextStep"]) || fallback.nextStep || ""),
  };
}

function sourceRepairCandidateLabel(candidate: EvalSourceRepairCandidate): string {
  return [candidate.brand, candidate.model].filter(Boolean).join(" ").trim()
    || candidate.sourceCode
    || "source draft";
}

function visibleSourceRepairCandidates(
  candidates: EvalSourceRepairCandidates | undefined,
  maxItems = 4,
): EvalSourceRepairCandidate[] {
  if (!candidates) return [];
  const seen = new Set<string>();
  const ordered = [
    ...(candidates.ownModel ?? []),
    ...(candidates.competitorCorridor ?? []),
  ];
  return ordered.filter(candidate => {
    const key = candidate.sourceCode || `${candidate.brand ?? ""}:${candidate.model ?? ""}`;
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, Math.max(1, maxItems));
}

function sourceRepairCandidateLines(
  candidates: EvalSourceRepairCandidates | undefined,
  maxItems = 4,
): string[] {
  return visibleSourceRepairCandidates(candidates, maxItems).map(candidate => {
    const label = sourceRepairCandidateLabel(candidate);
    const path = candidate.relativePath || candidate.sourceCode;
    return `${label}${path ? ` · ${path}` : ""}`;
  });
}

function sortEvidenceRepairQueue(items: EvidenceRepairItem[]): EvidenceRepairItem[] {
  return [...items].sort((left, right) => (
    (left.priority === "P0" ? 0 : 1) - (right.priority === "P0" ? 0 : 1)
    || right.missingEvidence.length - left.missingEvidence.length
    || left.questionId.localeCompare(right.questionId)
  ));
}

function normalizeBackendEvidenceRepairItem(item: EvalEvidenceRepairItem): EvidenceRepairItem | null {
  if (!item.questionId || !item.question) return null;
  const missingEvidence = missingEvidenceItems(item.missingEvidence);
  const recommendedActions = recommendedActionItems(item.recommendedActions);
  const sourceRepairCandidates = sourceRepairCandidatesFromValue(item.sourceRepairCandidates);
  const failureTags = stringList(item.failureTags);
  return {
    questionId: item.questionId,
    comparisonId: item.comparisonId,
    category: item.category || "unknown",
    country: item.country,
    question: item.question,
    answerStatus: item.answerStatus || "unknown",
    selectedTool: item.selectedTool || "—",
    failureTags,
    missingEvidence,
    recommendedActions,
    sourceRepairCandidates,
    repairSummary: repairSummaryFromValue(item.repairSummary, missingEvidence, sourceRepairCandidates, item.repairAction, failureTags),
    priority: item.priority || "P1",
    primaryGap: item.primaryGap,
    commandHint: item.commandHint ? evidenceRepairDisplayText(item.commandHint) : undefined,
    repairAction: item.repairAction ? evidenceRepairDisplayText(item.repairAction) : undefined,
    repairTasks: evidenceRepairTaskItems(item.repairTasks),
  };
}

export function normalizeEvidenceRepairQueue(
  backendQueue: EvalBusinessReportResponse["evidenceRepairQueue"] | undefined,
  fallbackRecords: EvalSideBySideRecord[],
): EvidenceRepairItem[] {
  const normalizedBackend = Array.isArray(backendQueue)
    ? backendQueue
      .map(normalizeBackendEvidenceRepairItem)
      .filter((item): item is EvidenceRepairItem => item !== null)
    : [];
  if (normalizedBackend.length > 0) return sortEvidenceRepairQueue(normalizedBackend);
  return buildEvidenceRepairQueue(fallbackRecords);
}

export function normalizeSourceRepairBacklog(value: unknown): EvalSourceRepairBacklogItem[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item): EvalSourceRepairBacklogItem | null => {
      if (!isRecord(item)) return null;
      const label = textFromRecord(item, ["label", "model", "sourceSearchQuery", "sourceUrl"]);
      const sourceType = textFromRecord(item, ["sourceType", "type"]) || "source_repair";
      const query = textFromRecord(item, ["sourceSearchQuery", "source_search_query", "query"]);
      const sourceUrl = textFromRecord(item, ["sourceUrl", "source_url", "url"]);
      const domain = textFromRecord(item, ["candidateDomain", "candidate_domain", "domain"]);
      const sourceDraftPath = textFromRecord(item, ["sourceDraftPath", "source_draft_path"]);
      const relativePath = textFromRecord(item, ["relativePath", "relative_path"]);
      const questionIds = stringList(item.questionIds);
      if (!label && !query && !sourceUrl && !domain) return null;
      return {
        priority: textFromRecord(item, ["priority"]) || "P1",
        sourceType,
        role: textFromRecord(item, ["role"]),
        label: label || query || sourceUrl || domain,
        brand: textFromRecord(item, ["brand"]),
        model: textFromRecord(item, ["model"]),
        candidateSourceType: textFromRecord(item, ["candidateSourceType", "candidate_source_type"]),
        candidateDomain: domain,
        sourceDraftPath,
        relativePath,
        sourceSearchQuery: query,
        sourceUrl,
        affectedCount: numberFromRecord(item, ["affectedCount", "affected_count"]) || questionIds.length || 1,
        questionIds,
        categories: stringList(item.categories),
        countries: stringList(item.countries),
        primaryGaps: stringList(item.primaryGaps),
        failureTags: stringList(item.failureTags),
        recommendedAction: evidenceRepairDisplayText(textFromRecord(item, ["recommendedAction", "action", "nextStep"])),
      };
    })
    .filter((item): item is EvalSourceRepairBacklogItem => item !== null)
    .sort((left, right) => (
      (left.priority === "P0" ? 0 : 1) - (right.priority === "P0" ? 0 : 1)
      || right.affectedCount - left.affectedCount
      || left.sourceType.localeCompare(right.sourceType)
      || left.label.localeCompare(right.label)
    ));
}

function sourceRepairBacklogSearchText(item: EvalSourceRepairBacklogItem): string {
  return item.sourceSearchQuery || item.sourceUrl || item.candidateDomain || "source lookup pending";
}

function sourceRepairBacklogTypeLabel(sourceType: string): string {
  if (sourceType === "msrp_current_price_source") return "MSRP current price";
  if (sourceType === "external_research_source") return "External research";
  if (sourceType === "policy_news_source") return "Policy / news";
  return evidenceRepairDisplayText(sourceType.replace(/_/g, " "));
}

export function buildSourceRepairBacklogPlanText(items: EvalSourceRepairBacklogItem[], maxItems = 30): string {
  const visibleItems = items.slice(0, Math.max(1, maxItems));
  if (visibleItems.length === 0) {
    return "AstrBot Source Repair Backlog: no source repair items.";
  }
  const lines = [
    "AstrBot Source Repair Backlog",
    `Total grouped source items: ${items.length}`,
    "Use this list to validate missing MSRP, VOC, policy, or news sources before rerunning Business Validation.",
    "",
    [
      "Priority",
      "Source Type",
      "Label",
      "Draft Path",
      "Search Query / URL",
      "Affected Questions",
      "Question IDs",
      "Categories",
      "Countries",
      "Primary Gaps",
      "Action",
    ].join("\t"),
  ];
  visibleItems.forEach(item => {
    lines.push([
      item.priority,
      sourceRepairBacklogTypeLabel(item.sourceType),
      item.label,
      item.sourceDraftPath || item.relativePath || "",
      sourceRepairBacklogSearchText(item),
      item.affectedCount,
      item.questionIds.join(", "),
      item.categories.map(categoryLabel).join(", "),
      item.countries.join(", "),
      item.primaryGaps.map(businessDiagnosticLabel).join(", "),
      item.recommendedAction,
    ].map(tsvCell).join("\t"));
  });
  if (items.length > visibleItems.length) {
    lines.push("", `... ${items.length - visibleItems.length} more source repair item(s) not shown.`);
  }
  return lines.join("\n").trim();
}

export function buildEvidenceRepairQueue(records: EvalSideBySideRecord[]): EvidenceRepairItem[] {
  return sortEvidenceRepairQueue(records
    .map((record): EvidenceRepairItem | null => {
      const failureTags = stringList(record.failureTags);
      const missingEvidence = missingEvidenceItems(record.astrbot?.missingEvidence);
      const recommendedActions = recommendedActionItems(record.astrbot?.recommendedActions);
      const sourceRepairCandidates = sourceRepairCandidatesFromValue(record.astrbot?.sourceRepairCandidates);
      if (failureTags.length === 0 && missingEvidence.length === 0) return null;
      const failureTagged = failureTags.length > 0;
      return {
        questionId: record.questionId,
        category: record.category,
        question: record.question,
        answerStatus: record.astrbot?.answerStatus || record.astrbot?.status || "unknown",
        selectedTool: record.astrbot?.selectedTool || "—",
        failureTags,
        missingEvidence,
        recommendedActions,
        sourceRepairCandidates,
        repairSummary: repairSummaryFromValue(
          isRecord(record.astrbot) ? record.astrbot.repairSummary : undefined,
          missingEvidence,
          sourceRepairCandidates,
          recommendedActions[0]?.action,
          failureTags,
        ),
        priority: failureTagged ? "P0" : "P1",
        primaryGap: missingEvidence.find(item => item.impact === "blocking")?.name ?? missingEvidence[0]?.name ?? failureTags[0],
        repairTasks: [],
      };
    })
    .filter((item): item is EvidenceRepairItem => item !== null));
}

export function evidenceRepairReasonLines(item: EvidenceRepairItem, maxItems = 2): string[] {
  return item.missingEvidence
    .map(missing => {
      const reason = evidenceRepairDisplayText(missing.reason.trim());
      const impact = missing.impact.trim();
      if (!reason) return "";
      const name = businessDiagnosticLabel(missing.name);
      return impact ? `${name}: ${reason} (${businessDiagnosticLabel(impact)})` : `${name}: ${reason}`;
    })
    .filter(Boolean)
    .slice(0, Math.max(1, maxItems));
}

export function buildEvidenceRepairPlanText(items: EvidenceRepairItem[], maxItems = 10): string {
  const visibleItems = items.slice(0, Math.max(1, maxItems));
  if (visibleItems.length === 0) {
    return "Evidence Repair Queue: no repair items.";
  }
  const lines = [
    "AstrBot Evidence Repair Plan",
    `Total repair items: ${items.length}`,
    "",
  ];
  visibleItems.forEach((item, index) => {
    const missing = item.missingEvidence
      .map(entry => {
        const impact = entry.impact ? ` (${businessDiagnosticLabel(entry.impact)})` : "";
        return `${businessDiagnosticLabel(entry.name)}${impact}`;
      })
      .join(", ") || "none";
    const reasons = evidenceRepairReasonLines(item, 3);
    const action = item.repairAction
      ?? item.recommendedActions[0]?.action
      ?? "补齐缺失证据后再重跑 Business Validation。";
    lines.push(
      `${index + 1}. [${item.priority}] ${item.questionId} · ${categoryLabel(item.category)}`,
      `Question: ${item.question}`,
      `Status: ${item.answerStatus}`,
      `Tool: ${item.selectedTool || "—"}`,
      `Failure tags: ${businessDiagnosticListText(item.failureTags)}`,
      `Missing evidence: ${missing}`,
    );
    if (reasons.length > 0) {
      lines.push("Why blocked:");
      reasons.forEach(reason => lines.push(`- ${reason}`));
    }
    const summary = item.repairSummary;
    if (summary) {
      const sourceSummary = evidenceRepairDisplayText(summary.sourceSummary || sourceRepairSummaryText(item.sourceRepairCandidates));
      lines.push(
        `Repair summary: primary gap=${businessDiagnosticLabel(item.primaryGap || summary.primaryGap || "n/a")}; missing=${summary.missingEvidenceCount ?? item.missingEvidence.length}; blocking=${summary.blockingEvidenceCount ?? 0}; source=${sourceSummary || "n/a"}`,
      );
    }
    if (item.commandHint) {
      lines.push(`Command hint: ${evidenceRepairDisplayText(item.commandHint)}`);
    }
    const sourceLines = sourceRepairCandidateLines(item.sourceRepairCandidates, 6);
    if (sourceLines.length > 0) {
      lines.push("Source drafts:");
      sourceLines.forEach(line => lines.push(`- ${line}`));
    }
    if (item.repairTasks.length > 0) {
      lines.push("Repair tasks:");
      item.repairTasks.slice(0, 5).forEach(task => {
        const sourceCandidates = task.sourceCandidates ?? [];
        lines.push(`- [${task.priority}] ${task.title} (${task.owner})`);
        if (task.input) lines.push(`  input: ${task.input}`);
        if (task.output) lines.push(`  output: ${task.output}`);
        if (task.commandHint) lines.push(`  hint: ${task.commandHint}`);
        if (sourceCandidates.length > 0) {
          lines.push(`  sources: ${sourceCandidates.slice(0, 4).join(", ")}`);
        }
      });
    }
    lines.push(`Repair action: ${evidenceRepairDisplayText(action)}`, "");
  });
  if (items.length > visibleItems.length) {
    lines.push(`... ${items.length - visibleItems.length} more repair item(s) not shown.`);
  }
  return lines.join("\n").trim();
}

export function buildEvidenceRepairOverview(items: EvidenceRepairItem[]): EvidenceRepairOverview {
  const owners = new Map<string, number>();
  let p0Count = 0;
  let p1Count = 0;
  let answeredCount = 0;
  let partialCount = 0;
  let taskCount = 0;
  let pricingSourceTaskCount = 0;
  let configGapTaskCount = 0;
  let sourceDateTaskCount = 0;
  let rerunTaskCount = 0;
  let materializedCandidateCount = 0;
  let sourceCandidateCount = 0;
  let missingOwnModelSourceCount = 0;

  items.forEach(item => {
    if (item.priority === "P0") p0Count += 1;
    else p1Count += 1;
    const status = item.answerStatus.toLowerCase();
    if (status === "answered") answeredCount += 1;
    if (status.includes("partial") || status.includes("insufficient")) partialCount += 1;

    const summary = item.repairSummary;
    const candidateCount = summary?.sourceCandidateCount
      ?? item.sourceRepairCandidates?.candidateCount
      ?? (item.sourceRepairCandidates?.ownModel?.length ?? 0) + (item.sourceRepairCandidates?.competitorCorridor?.length ?? 0);
    const materializedCount = summary?.materializedCandidateCount
      ?? item.sourceRepairCandidates?.materializedCandidateCount
      ?? 0;
    sourceCandidateCount += candidateCount;
    materializedCandidateCount += materializedCount;
    if (summary?.missingOwnModelSource ?? item.sourceRepairCandidates?.missingOwnModelSource) {
      missingOwnModelSourceCount += 1;
    }

    item.repairTasks.forEach(task => {
      taskCount += 1;
      const owner = task.owner || "AstrBot Eval";
      owners.set(owner, (owners.get(owner) ?? 0) + 1);
      if (
        task.taskType === "own_model_msrp_source"
        || task.taskType === "promote_own_model_source_draft"
        || task.taskType === "competitor_price_corridor"
      ) {
        pricingSourceTaskCount += 1;
      }
      if (task.taskType === "config_gap_evidence") configGapTaskCount += 1;
      if (task.taskType === "source_date_evidence") sourceDateTaskCount += 1;
      if (task.taskType === "rerun_business_validation") rerunTaskCount += 1;
    });
  });

  return {
    total: items.length,
    p0Count,
    p1Count,
    answeredCount,
    partialCount,
    taskCount,
    pricingSourceTaskCount,
    configGapTaskCount,
    sourceDateTaskCount,
    rerunTaskCount,
    materializedCandidateCount,
    sourceCandidateCount,
    missingOwnModelSourceCount,
    topOwners: [...owners.entries()]
      .map(([owner, count]) => ({ owner, count }))
      .sort((left, right) => right.count - left.count || left.owner.localeCompare(right.owner))
      .slice(0, 3),
  };
}

export function buildEvidenceRepairDisplayState(
  items: EvidenceRepairItem[],
  showAll: boolean,
  limit = 6,
): EvidenceRepairDisplayState {
  const safeLimit = Math.max(1, limit);
  const hiddenCount = Math.max(0, items.length - safeLimit);
  const visibleItems = showAll ? items : items.slice(0, safeLimit);
  return {
    visibleItems,
    hiddenCount,
    statusText: `Showing ${visibleItems.length} of ${items.length}`,
    toggleLabel: showAll ? `Show top ${safeLimit}` : `Show all${hiddenCount > 0 ? ` ${items.length}` : ""}`,
  };
}

export function buildBlockingReadinessItems(items: EvidenceRepairItem[], maxItems = 3): BlockingReadinessItem[] {
  return items
    .filter(item => item.priority === "P0" || item.failureTags.includes("evidence_missing"))
    .map(item => {
      const blockingEvidence = item.missingEvidence.find(missing => missing.impact === "blocking")
        ?? item.missingEvidence[0];
      const summary = item.repairSummary;
      const primaryGap = item.primaryGap || summary?.primaryGap || blockingEvidence?.name || item.failureTags[0] || "unknown_gap";
      const action = item.repairAction
        ?? summary?.nextStep
        ?? item.recommendedActions[0]?.action
        ?? "补齐阻断证据后重跑 Business Validation。";
      return {
        questionId: item.questionId,
        category: item.category,
        question: item.question,
        primaryGap,
        reason: blockingEvidence?.reason ? evidenceRepairDisplayText(blockingEvidence.reason) : "缺少可引用证据，不能进入替换评估。",
        action: evidenceRepairDisplayText(action),
        selectedTool: item.selectedTool || "—",
      };
    })
    .slice(0, Math.max(1, maxItems));
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => typeof item === "string" && item.trim().length > 0);
}

function commaStringToList(value: string): string[] {
  const seen = new Set<string>();
  return value
    .split(",")
    .map(item => item.trim())
    .filter(item => {
      if (!item || seen.has(item)) return false;
      seen.add(item);
      return true;
    });
}

function agreementTone(status: string): string {
  if (status === "match") return "is-good";
  if (status === "partial" || status === "pending") return "is-ok";
  return "is-low";
}

function playbookTitle(record: EvalSideBySideRecord): string {
  const playbook = record.businessPlaybook;
  if (!isRecord(playbook)) return "—";
  const title = textFromRecord(playbook, ["title", "id"]) || "—";
  return businessDiagnosticDisplayText(title);
}

function playbookSections(record: EvalSideBySideRecord): string {
  const playbook = record.businessPlaybook;
  if (!isRecord(playbook)) return "—";
  return listPreview(playbook.requiredSections, ["label", "name"], "—", 5);
}

function extractJsonStringField(text: string, field: string): string {
  const escapedField = field.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = text.match(new RegExp(`"${escapedField}"\\s*:\\s*"([^"]*)"`));
  return match?.[1]
    ?.replace(/\\"/g, "\"")
    .replace(/\\n/g, "\n")
    .replace(/\\\\/g, "\\")
    .trim() ?? "";
}

function answerPreviewText(text: string | undefined, fallback: string): string {
  if (!text?.trim()) return fallback;
  const trimmed = text.trim();
  if (!trimmed.startsWith("{")) return businessDiagnosticDisplayText(trimmed);
  try {
    const parsed: unknown = JSON.parse(trimmed);
    if (!isRecord(parsed)) return businessDiagnosticDisplayText(trimmed);
    const lines: string[] = [];
    const title = textFromRecord(parsed, ["title"]);
    const direct = textFromRecord(parsed, ["direct", "answer"]);
    if (title) lines.push(title);
    if (direct) lines.push(direct);
    const bullets = parsed.bullets;
    if (Array.isArray(bullets)) {
      for (const item of bullets.slice(0, 6)) {
        if (typeof item === "string" && item.trim()) lines.push(`- ${item.trim()}`);
      }
    }
    const limitations = parsed.limitations;
    if (Array.isArray(limitations) && limitations.length > 0) {
      lines.push(`Limitations: ${listPreview(limitations, ["text", "reason"], "none", 3)}`);
    }
    return businessDiagnosticDisplayText(lines.length > 0 ? lines.join("\n") : trimmed);
  } catch {
    const title = extractJsonStringField(trimmed, "title");
    const direct = extractJsonStringField(trimmed, "direct");
    const lines = [title, direct].filter(Boolean);
    if (lines.length > 0) return businessDiagnosticDisplayText(lines.join("\n"));
    return businessDiagnosticDisplayText(trimmed);
  }
}

export function AstrBotEvalPanel() {
  const [tab, setTab] = useState<TabKey>("business");
  const [summary, setSummary] = useState<EvalSummary | null>(null);
  const [results, setResults] = useState<EvalResult[]>([]);
  const [questions, setQuestions] = useState<EvalQuestion[]>([]);
  const [businessQuestions, setBusinessQuestions] = useState<EvalBusinessQuestionsResponse | null>(null);
  const [businessReport, setBusinessReport] = useState<EvalBusinessReportResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [loadWarning, setLoadWarning] = useState<string | null>(null);
  const [running, setRunning] = useState<string | null>(null);
  const [runOutput, setRunOutput] = useState<string>("");
  const [resultsCategory, setResultsCategory] = useState("");
  const [compareCategory, setCompareCategory] = useState("");
  const [compareOutput, setCompareOutput] = useState("");
  const [businessCategory, setBusinessCategory] = useState("");
  const [businessOutput, setBusinessOutput] = useState("");
  const [businessMode, setBusinessMode] = useState<BusinessPanelMode>("review");
  const [businessReviewFilter, setBusinessReviewFilter] = useState<BusinessReviewFilter>("score_ready");
  const [calibrationOnlyMismatches, setCalibrationOnlyMismatches] = useState(false);
  const [sideBySide, setSideBySide] = useState<EvalSideBySideResultsResponse | null>(null);
  const [codexReviewNotes, setCodexReviewNotes] = useState<EvalCodexReviewNotesResponse | null>(null);
  const [judgePreflight, setJudgePreflight] = useState<EvalJudgePreflightResponse | null>(null);
  const [scoringDrafts, setScoringDrafts] = useState<Record<string, ScoringDraft>>({});
  const [expandedComparisonId, setExpandedComparisonId] = useState<string | null>(null);
  const [usage, setUsage] = useState<EvalUsageSummary | null>(null);
  const [repairPlanCopyState, setRepairPlanCopyState] = useState("");
  const [sourceBacklogPlanText, setSourceBacklogPlanText] = useState("");
  const [scoreSheetCopyState, setScoreSheetCopyState] = useState("");
  const [judgeTemplateCopyState, setJudgeTemplateCopyState] = useState("");
  const [readinessHandoffCopyState, setReadinessHandoffCopyState] = useState("");
  const [scoreSheetText, setScoreSheetText] = useState("");
  const [scoreSheetDraftText, setScoreSheetDraftText] = useState("");
  const [scoreSheetImportState, setScoreSheetImportState] = useState("");
  const [importedDraftComparisonIds, setImportedDraftComparisonIds] = useState<string[]>([]);
  const [referenceJudgeDraftText, setReferenceJudgeDraftText] = useState("");
  const [referenceJudgeImportState, setReferenceJudgeImportState] = useState("");
  const [referenceJudgeDraftComparisonIds, setReferenceJudgeDraftComparisonIds] = useState<string[]>([]);
  const [showAllEvidenceRepairItems, setShowAllEvidenceRepairItems] = useState(false);

  async function loadAll() {
    setLoading(true);
    setError(null);
    setLoadWarning(null);
    try {
      const [
        summaryResult,
        questionsResult,
        usageResult,
        businessQuestionsResult,
        businessReportResult,
        codexReviewNotesResult,
        judgePreflightResult,
      ] = await Promise.allSettled([
        fetchEvalSummary(),
        fetchEvalQuestions(),
        fetchEvalUsage(),
        fetchBusinessValidationQuestions(),
        fetchBusinessValidationReport({ limit: 100 }),
        fetchCodexReviewNotes(),
        fetchEvalJudgePreflight(false),
      ]);

      const warnings: string[] = [];

      if (summaryResult.status === "fulfilled") {
        setSummary(summaryResult.value);
      } else {
        setSummary(null);
        setError(`Eval summary unavailable: ${errorMessage(summaryResult.reason)}`);
      }

      if (questionsResult.status === "fulfilled") {
        setQuestions(questionsResult.value.items ?? []);
      } else {
        setQuestions([]);
        warnings.push(`questions unavailable: ${errorMessage(questionsResult.reason)}`);
      }

      if (usageResult.status === "fulfilled") {
        setUsage(usageResult.value);
      } else {
        setUsage(null);
        warnings.push(`usage unavailable: ${errorMessage(usageResult.reason)}`);
      }

      if (businessQuestionsResult.status === "fulfilled") {
        setBusinessQuestions(businessQuestionsResult.value);
      } else {
        setBusinessQuestions(null);
        warnings.push(`business validation unavailable: ${errorMessage(businessQuestionsResult.reason)}`);
      }

      if (businessReportResult.status === "fulfilled") {
        setBusinessReport(businessReportResult.value);
      } else {
        setBusinessReport(null);
        warnings.push(`business report unavailable: ${errorMessage(businessReportResult.reason)}`);
      }

      if (codexReviewNotesResult.status === "fulfilled") {
        setCodexReviewNotes(codexReviewNotesResult.value);
      } else {
        setCodexReviewNotes(null);
        warnings.push(`codex review notes unavailable: ${errorMessage(codexReviewNotesResult.reason)}`);
      }

      if (judgePreflightResult.status === "fulfilled") {
        setJudgePreflight(judgePreflightResult.value);
      } else {
        setJudgePreflight(null);
        warnings.push(`judge preflight unavailable: ${errorMessage(judgePreflightResult.reason)}`);
      }

      setLoadWarning(warnings.length > 0 ? `Partial eval data loaded; ${warnings.join("; ")}` : null);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setLoading(false);
    }
  }

  async function loadResults(cat?: string) {
    try {
      const r = await fetchEvalResults({ category: cat || undefined, limit: 50 });
      setResults(r.items ?? []);
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  async function loadSideBySideResults(cat?: string) {
    try {
      const r = await fetchEvalSideBySideResults({ category: cat || undefined, limit: 30, latestPerQuestion: true });
      setSideBySide(r);
      setExpandedComparisonId(previous => (
        r.items.some(item => item.comparisonId === previous)
          ? previous
          : null
      ));
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  async function loadBusinessReport(cat?: string) {
    try {
      const r = await fetchBusinessValidationReport({ category: cat || undefined, limit: 100 });
      setBusinessReport(r);
      return r;
    } catch (err) {
      setError(errorMessage(err));
      return null;
    }
  }

  async function loadCodexReviewNotes() {
    try {
      setCodexReviewNotes(await fetchCodexReviewNotes());
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  async function loadJudgePreflight() {
    try {
      setJudgePreflight(await fetchEvalJudgePreflight(false));
    } catch (err) {
      setError(errorMessage(err));
    }
  }

  useEffect(() => { void loadAll(); }, []);

  async function runSingle(qId: string, label: string) {
    setRunning(label);
    setError(null);
    setRunOutput(`Running ${label}…`);
    try {
      const r = await runEvalQuestion(qId);
      setRunOutput(JSON.stringify(r.scores, null, 2));
      await loadAll();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runCat(cat: string) {
    setRunning(`category: ${cat}`);
    setError(null);
    setRunOutput(`Running all ${cat} questions…`);
    try {
      const r = await runEvalCategory(cat, 5);
      setRunOutput(`${r.total} results\nAvg composite: ${formatScore(r.summary.avgComposite)}`);
      await loadAll();
      await loadResults(cat);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runSmokeSuite() {
    setRunning("smoke suite");
    setError(null);
    setRunOutput("Running 5 questions per category (25 total)…");
    try {
      const r = await runEvalFull(5);
      setRunOutput(formatFullRunOutput(r.totalRun, r.byCategory));
      await loadAll();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runFullSuite() {
    setRunning("full suite");
    setError(null);
    setRunOutput("Running 20 questions per category (100 total)…");
    try {
      const r = await runEvalFull(20);
      setRunOutput(formatFullRunOutput(r.totalRun, r.byCategory));
      await loadAll();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runCompareQuestion(qId: string, label: string) {
    setRunning(`compare-question: ${qId}`);
    setError(null);
    setCompareOutput(`Running side-by-side comparison for ${label}…`);
    try {
      const r = await runEvalSideBySideQuestion(qId);
      setCompareCategory(r.category);
      setCompareOutput(formatSideBySideRunOutput(1, {
        count: 1,
        pendingHumanScoring: r.humanScoring?.status === "scored" ? 0 : 1,
        astrbotErrorCount: r.astrbot?.error ? 1 : 0,
        countryCopilotErrorCount: r.countryCopilot?.error ? 1 : 0,
        avgAstrBotComposite: r.astrbot?.scores?.composite ?? 0,
      }));
      await loadSideBySideResults(r.category);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runCompareCategory(cat: string) {
    setRunning(`compare-category: ${cat}`);
    setError(null);
    setCompareCategory(cat);
    setCompareOutput(`Running one ${CATEGORY_LABELS[cat] ?? cat} question through AstrBot and CountryCopilot…`);
    try {
      const r = await runEvalSideBySideCategory(cat, 1);
      setCompareOutput(formatSideBySideRunOutput(r.total, r.summary));
      await loadSideBySideResults(cat);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runBusinessQuestion(qId: string) {
    setRunning(`business-question: ${qId}`);
    setError(null);
    setBusinessOutput(`Running business side-by-side comparison for ${qId}…`);
    try {
      const r = await runBusinessValidationQuestion(qId);
      setBusinessCategory(r.category);
      setBusinessOutput(formatSideBySideRunOutput(1, {
        count: 1,
        pendingHumanScoring: r.humanScoring?.status === "scored" ? 0 : 1,
        scoredCount: r.humanScoring?.status === "scored" ? 1 : 0,
        astrbotErrorCount: r.astrbot?.error ? 1 : 0,
        countryCopilotErrorCount: r.countryCopilot?.error ? 1 : 0,
        avgAstrBotComposite: r.astrbot?.scores?.composite ?? 0,
        humanWins: { astrbot: 0, countryCopilot: 0, tie: 0, unclear: 0 },
      }));
      await loadBusinessReport(r.category);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runBusinessCategory(cat: string, limit: number) {
    setRunning(`business-category: ${cat}:${limit}`);
    setError(null);
    setBusinessCategory(cat);
    setBusinessOutput(`Running ${limit} ${BUSINESS_CATEGORY_LABELS[cat] ?? cat} business comparisons…`);
    try {
      const r = await runBusinessValidationCategory(cat, limit);
      setBusinessOutput(formatBusinessRunOutput(r.total, r.summary));
      await loadBusinessReport(cat);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runBusinessAll() {
    setRunning("business-all");
    setError(null);
    setBusinessOutput("Running all 30 business validation questions through AstrBot and CountryCopilot…");
    try {
      const r = await runBusinessValidationAll(30);
      setBusinessOutput(formatBusinessRunOutput(r.total, r.summary));
      await loadBusinessReport(businessCategory || undefined);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function runJudgeExistingBaseline(limit: number, scoreReadyOnly = false) {
    if (!judgePreflight?.ready) {
      setBusinessOutput("Judge provider is not ready. Enable the judge provider and refresh preflight before scoring existing baseline records.");
      return;
    }
    setRunning(scoreReadyOnly ? "judge-baseline-score-ready" : limit >= 30 ? "judge-baseline-full" : "judge-baseline-smoke");
    setError(null);
    setBusinessOutput(
      scoreReadyOnly
        ? `Judging up to ${limit} score-ready business validation records with the GPT judge…`
        : `Judging ${limit} existing business validation records with the GPT judge…`,
    );
    try {
      const r = await judgeExistingBusinessValidationRecords({
        category: businessCategory || undefined,
        limit,
        latestPerQuestion: true,
        scoreReadyOnly,
      });
      setBusinessOutput(formatBusinessJudgeExistingOutput(r));
      await loadBusinessReport(businessCategory || undefined);
      await loadJudgePreflight();
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  function readScoringDraft(record: EvalSideBySideRecord): ScoringDraft {
    return scoringDrafts[record.comparisonId] ?? initialScoringDraft(record);
  }

  function updateScoringDraft(record: EvalSideBySideRecord, patch: Partial<ScoringDraft>) {
    setScoringDrafts(prev => ({
      ...prev,
      [record.comparisonId]: {
        ...(prev[record.comparisonId] ?? initialScoringDraft(record)),
        ...patch,
      },
    }));
  }

  function updateOverallScore(record: EvalSideBySideRecord, side: "astrbot" | "countryCopilot", score: number) {
    const dimensions = scoreDimensionsForRecord(record);
    const scores = filledScoreMap(dimensions, score);
    updateScoringDraft(
      record,
      side === "astrbot"
        ? { astrbotScores: scores, status: "pending", winner: "" }
        : { countryCopilotScores: scores, status: "pending", winner: "" },
    );
  }

  function applyScorePreset(
    record: EvalSideBySideRecord,
    winner: string,
    astrbotScore: number,
    countryCopilotScore: number,
  ) {
    const dimensions = scoreDimensionsForRecord(record);
    updateScoringDraft(record, {
      status: "scored",
      winner,
      astrbotScores: filledScoreMap(dimensions, astrbotScore),
      countryCopilotScores: filledScoreMap(dimensions, countryCopilotScore),
    });
  }

  function applyCodexReviewDraft(record: EvalSideBySideRecord, note: EvalCodexReviewNote) {
    const dimensions = scoreDimensionsForRecord(record);
    const prefill = buildCodexDraftScorePrefill(note, dimensions);
    updateScoringDraft(record, {
      status: "pending",
      winner: prefill.winner,
      notes: prefill.notes,
      failureTags: prefill.failureTags,
      astrbotScores: prefill.astrbotScores,
      countryCopilotScores: prefill.countryCopilotScores,
    });
  }

  function clearScoringDraft(record: EvalSideBySideRecord) {
    updateScoringDraft(record, {
      status: "pending",
      winner: "",
      notes: "",
      failureTags: [],
      astrbotScores: {},
      countryCopilotScores: {},
    });
  }

  function toggleFailureTag(record: EvalSideBySideRecord, tag: string) {
    const current = readScoringDraft(record).failureTags;
    const next = current.includes(tag)
      ? current.filter(value => value !== tag)
      : [...current, tag];
    updateScoringDraft(record, { failureTags: next });
  }

  async function saveHumanScore(
    record: EvalSideBySideRecord,
    options: { advanceAfterSave?: boolean; draftOverride?: ScoringDraft; runningPrefix?: string; source?: string } = {},
  ) {
    const draft = options.draftOverride ?? readScoringDraft(record);
    const dimensions = scoreDimensionsForRecord(record);
    const astrbotCompleted = completedScoreCount(dimensions, draft.astrbotScores);
    const countryCompleted = completedScoreCount(dimensions, draft.countryCopilotScores);
    const scoreIsComplete = canSaveBusinessScore(astrbotCompleted, countryCompleted, dimensions.length);
    const astrbotAverage = completeAverage(dimensions, draft.astrbotScores);
    const countryAverage = completeAverage(dimensions, draft.countryCopilotScores);
    const status = effectiveStatus(draft, scoreIsComplete);
    const winner = status === "scored" ? effectiveWinner(draft, astrbotAverage, countryAverage, scoreIsComplete) : "";
    const advanceAfterSave = Boolean(options.advanceAfterSave);
    if (!scoreIsComplete) {
      setError("Pick one total score for both AstrBot and CountryCopilot before saving this baseline score.");
      return;
    }
    const nextComparisonId = advanceAfterSave
      ? findNextReviewComparisonId(businessRecords, codexReviewByQuestionId, record.comparisonId, businessReviewFilter)
      : "";
    const runningPrefix = options.runningPrefix ?? (advanceAfterSave ? "score-next" : "score");
    setRunning(`${runningPrefix}: ${record.comparisonId}`);
    setError(null);
    try {
      const updated = await updateEvalSideBySideHumanScore(record.comparisonId, {
        status,
        source: options.source ?? "manual",
        winner,
        notes: draft.notes,
        dimensions: dimensions.map(dimension => dimension.key),
        astrbotTotal: astrbotAverage,
        countryCopilotTotal: countryAverage,
        astrbotScores: draft.astrbotScores,
        countryCopilotScores: draft.countryCopilotScores,
        failureTags: draft.failureTags,
      });
      setCompareOutput(`Saved human score for ${updated.questionId}: ${winnerText(updated.humanScoring?.winner)}`);
      setScoringDrafts(prev => {
        const next = { ...prev };
        delete next[record.comparisonId];
        return next;
      });
      await loadSideBySideResults(compareCategory);
      if (record.validationType === "business") {
        await loadBusinessReport(businessCategory || undefined);
      }
      if (advanceAfterSave) {
        setBusinessMode("review");
        setExpandedComparisonId(nextComparisonId || null);
        setBusinessOutput(nextComparisonId
          ? `Saved ${updated.questionId}. Opened next review item.`
          : `Saved ${updated.questionId}. No more matching review items.`);
        if (nextComparisonId) {
          window.setTimeout(() => {
            document
              .querySelector(`[data-comparison-id="${nextComparisonId}"]`)
              ?.scrollIntoView({ block: "start" });
          }, 0);
        }
      }
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function acceptCodexDraftAndNext(record: EvalSideBySideRecord, note: EvalCodexReviewNote) {
    const dimensions = scoreDimensionsForRecord(record);
    const prefill = buildCodexDraftScorePrefill(note, dimensions);
    if (!prefill.complete) {
      setError("Codex draft is partial. Prefill it first, then finish both totals before saving.");
      return;
    }
    await saveHumanScore(record, {
      advanceAfterSave: true,
      runningPrefix: "accept-draft-next",
      source: "codex_review",
      draftOverride: {
        status: "scored",
        winner: prefill.winner,
        notes: acceptedCodexDraftNotes(note),
        failureTags: prefill.failureTags,
        astrbotScores: prefill.astrbotScores,
        countryCopilotScores: prefill.countryCopilotScores,
      },
    });
  }

  async function copyEvidenceRepairPlan(items: EvidenceRepairItem[]) {
    const text = buildEvidenceRepairPlanText(items);
    if (await writeClipboardText(text)) {
      setRepairPlanCopyState("Repair plan copied");
    } else {
      setRepairPlanCopyState("Copy unavailable");
    }
  }

  async function copySourceRepairBacklogPlan(items: EvalSourceRepairBacklogItem[]) {
    const text = buildSourceRepairBacklogPlanText(items);
    setSourceBacklogPlanText(text);
    if (await writeClipboardText(text)) {
      setRepairPlanCopyState("Source backlog copied");
    } else {
      setRepairPlanCopyState("Copy blocked; TSV ready below");
      setBusinessOutput(text);
    }
  }

  async function copyBusinessScoringSheet(records: EvalSideBySideRecord[]) {
    const text = buildBusinessScoringSheetText(records, { notesByQuestionId: codexReviewByQuestionId });
    setScoreSheetText(text);
    setScoreSheetDraftText(text);
    if (await writeClipboardText(text)) {
      const rowCount = Math.max(0, text.split("\n").length - 1);
      setScoreSheetCopyState(`Copied ${rowCount} row${rowCount === 1 ? "" : "s"}`);
    } else {
      setScoreSheetCopyState("Copy unavailable; sheet shown below");
    }
  }

  async function copyJudgeEnvTemplate() {
    const text = buildJudgeEnvTemplate(judgePreflight);
    if (await writeClipboardText(text)) {
      setJudgeTemplateCopyState("Judge .env template copied");
    } else {
      setJudgeTemplateCopyState("Copy unavailable; open the template below");
    }
  }

  async function copyBusinessReadinessHandoff() {
    const text = buildBusinessReadinessHandoffText({
      readiness: businessReadiness,
      actionPlan: baselineActionPlan,
      workbench: businessReviewWorkbench,
      codexTriage: codexDraftTriage,
      judgePreflight,
      visibleRecordCount: businessReviewRecords.length,
    });
    if (await writeClipboardText(text)) {
      setReadinessHandoffCopyState("Readiness handoff copied");
    } else {
      setReadinessHandoffCopyState("Copy unavailable; handoff shown below");
      setBusinessOutput(text);
    }
  }

  function applyBusinessScoringSheetDrafts(records: EvalSideBySideRecord[]) {
    const result = parseBusinessScoringSheetDrafts(records, scoreSheetDraftText);
    if (result.appliedCount > 0) {
      const importedIds = Object.keys(result.drafts);
      setScoringDrafts(prev => ({ ...prev, ...result.drafts }));
      setImportedDraftComparisonIds(prev => Array.from(new Set([...prev, ...importedIds])));
      setExpandedComparisonId(result.firstComparisonId || expandedComparisonId);
    }
    const errorSuffix = result.errors.length > 0
      ? ` · ${result.errors.slice(0, 2).join(" ")}${result.errors.length > 2 ? ` (+${result.errors.length - 2} more)` : ""}`
      : "";
    setScoreSheetImportState(
      `Applied ${result.appliedCount}/${result.matchedCount} matched row${result.matchedCount === 1 ? "" : "s"}`
      + (result.skippedCount > 0 ? ` · skipped ${result.skippedCount}` : "")
      + errorSuffix,
    );
  }

  function applyReferenceJudgeScoreDrafts(records: EvalSideBySideRecord[]) {
    const result = parseReferenceJudgeScoreDrafts(records, referenceJudgeDraftText);
    if (result.appliedCount > 0) {
      const importedIds = Object.keys(result.drafts);
      setScoringDrafts(prev => ({ ...prev, ...result.drafts }));
      setReferenceJudgeDraftComparisonIds(prev => Array.from(new Set([...prev, ...importedIds])));
      setExpandedComparisonId(result.firstComparisonId || expandedComparisonId);
    }
    const source = result.importedSourceLabel ? ` · source ${result.importedSourceLabel}` : "";
    const errorSuffix = result.errors.length > 0
      ? ` · ${result.errors.slice(0, 2).join(" ")}${result.errors.length > 2 ? ` (+${result.errors.length - 2} more)` : ""}`
      : "";
    setReferenceJudgeImportState(
      `Applied ${result.appliedCount}/${result.matchedCount} judged row${result.matchedCount === 1 ? "" : "s"}`
      + (result.skippedCount > 0 ? ` · skipped ${result.skippedCount}` : "")
      + source
      + errorSuffix,
    );
  }

  async function loadLatestCodexScoringSheetDraft() {
    setRunning("load-latest-codex-scoring-sheet");
    setError(null);
    try {
      const artifact = await fetchLatestCodexReviewScoringArtifacts();
      const sheet = artifact.codexDraftSheetText?.trim()
        ? artifact.codexDraftSheetText
        : artifact.manualTemplateText ?? "";
      if (!artifact.available || !sheet.trim()) {
        setScoreSheetImportState(artifact.reason || "No Codex scoring TSV artifact is available. Run review:astrbot first.");
        return;
      }
      setScoreSheetDraftText(sheet);
      setScoreSheetImportState(
        `Loaded ${artifact.rowCount ?? 0} row${artifact.rowCount === 1 ? "" : "s"} from ${artifact.runId || "latest Codex review"}. Review before saving as manual scores.`,
      );
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function loadLatestReferenceJudgePacket() {
    setRunning("load-latest-reference-judge-packet");
    setError(null);
    try {
      const artifact = await fetchLatestCodexReviewScoringArtifacts();
      const packet = artifact.referenceJudgePacketMdText?.trim()
        ? artifact.referenceJudgePacketMdText
        : artifact.referenceJudgePacketJsonText ?? "";
      if (!artifact.available || !packet.trim()) {
        setReferenceJudgeImportState(artifact.reason || "No reference judge packet is available. Run review:astrbot first.");
        return;
      }
      setReferenceJudgeDraftText(packet);
      if (await writeClipboardText(packet)) {
        setReferenceJudgeImportState(
          `Loaded and copied reference judge packet from ${artifact.runId || "latest Codex review"}. Send it to GPT/Opus/Fable, then paste returned JSON here.`,
        );
      } else {
        setReferenceJudgeImportState(
          `Loaded reference judge packet from ${artifact.runId || "latest Codex review"}. Copy unavailable; use the textarea content.`,
        );
      }
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function saveImportedManualScores() {
    const recordsToSave = recordsForImportedManualSave(businessRecords, scoringDrafts, importedDraftComparisonIds);
    if (recordsToSave.length === 0) {
      setScoreSheetImportState("No complete imported drafts are ready to save.");
      return;
    }
    const confirmed = window.confirm(
      `Save ${recordsToSave.length} imported sheet draft${recordsToSave.length === 1 ? "" : "s"} as manual scores? Only continue if the TSV was reviewed by a human.`,
    );
    if (!confirmed) return;

    setRunning("save-imported-manual-scores");
    setError(null);
    setBusinessOutput(`Saving ${recordsToSave.length} imported manual score${recordsToSave.length === 1 ? "" : "s"}...`);
    const savedIds = new Set<string>();
    try {
      for (const record of recordsToSave) {
        const draft = scoringDrafts[record.comparisonId];
        if (!draft) continue;
        const dimensions = scoreDimensionsForRecord(record);
        const astrbotAverage = completeAverage(dimensions, draft.astrbotScores);
        const countryAverage = completeAverage(dimensions, draft.countryCopilotScores);
        const winner = effectiveWinner(draft, astrbotAverage, countryAverage, true);
        await updateEvalSideBySideHumanScore(record.comparisonId, {
          status: "scored",
          source: "manual",
          winner,
          notes: draft.notes,
          dimensions: dimensions.map(dimension => dimension.key),
          astrbotTotal: astrbotAverage,
          countryCopilotTotal: countryAverage,
          astrbotScores: draft.astrbotScores,
          countryCopilotScores: draft.countryCopilotScores,
          failureTags: draft.failureTags,
        });
        savedIds.add(record.comparisonId);
      }
      setScoringDrafts(prev => {
        const next = { ...prev };
        savedIds.forEach(id => { delete next[id]; });
        return next;
      });
      setImportedDraftComparisonIds(prev => prev.filter(id => !savedIds.has(id)));
      setScoreSheetImportState(`Saved ${savedIds.size} imported manual score${savedIds.size === 1 ? "" : "s"}.`);
      setBusinessOutput(`Saved ${savedIds.size} imported manual score${savedIds.size === 1 ? "" : "s"}. Refreshing replacement baseline...`);
      await loadSideBySideResults(compareCategory);
      await loadBusinessReport(businessCategory || undefined);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function saveImportedReferenceJudgeScores() {
    const recordsToSave = recordsForImportedManualSave(businessRecords, scoringDrafts, referenceJudgeDraftComparisonIds);
    if (recordsToSave.length === 0) {
      setReferenceJudgeImportState("No complete reference judge drafts are ready to save.");
      return;
    }
    const confirmed = window.confirm(
      `Save ${recordsToSave.length} imported reference judge draft${recordsToSave.length === 1 ? "" : "s"} as llm_judge scores? Only continue if the GPT/Opus/Fable/manual judge output has been reviewed and accepted.`,
    );
    if (!confirmed) return;

    setRunning("save-reference-judge-scores");
    setError(null);
    setBusinessOutput(`Saving ${recordsToSave.length} imported reference judge score${recordsToSave.length === 1 ? "" : "s"} as llm_judge...`);
    const savedIds = new Set<string>();
    try {
      for (const record of recordsToSave) {
        const draft = scoringDrafts[record.comparisonId];
        if (!draft) continue;
        const dimensions = scoreDimensionsForRecord(record);
        const astrbotAverage = completeAverage(dimensions, draft.astrbotScores);
        const countryAverage = completeAverage(dimensions, draft.countryCopilotScores);
        const winner = effectiveWinner(draft, astrbotAverage, countryAverage, true);
        await updateEvalSideBySideHumanScore(record.comparisonId, {
          status: "scored",
          source: "llm_judge",
          judgeProvider: draft.judgeProvider,
          winner,
          notes: draft.notes,
          dimensions: dimensions.map(dimension => dimension.key),
          astrbotTotal: astrbotAverage,
          countryCopilotTotal: countryAverage,
          astrbotScores: draft.astrbotScores,
          countryCopilotScores: draft.countryCopilotScores,
          failureTags: draft.failureTags,
        });
        savedIds.add(record.comparisonId);
      }
      setScoringDrafts(prev => {
        const next = { ...prev };
        savedIds.forEach(id => { delete next[id]; });
        return next;
      });
      setReferenceJudgeDraftComparisonIds(prev => prev.filter(id => !savedIds.has(id)));
      setReferenceJudgeImportState(`Saved ${savedIds.size} reference judge score${savedIds.size === 1 ? "" : "s"} as llm_judge.`);
      setBusinessOutput(`Saved ${savedIds.size} reference judge score${savedIds.size === 1 ? "" : "s"} as llm_judge. Refreshing replacement baseline...`);
      await loadSideBySideResults(compareCategory);
      await loadBusinessReport(businessCategory || undefined);
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  async function openManualScoreQueue() {
    setTab("business");
    setBusinessMode("review");
    setBusinessReviewFilter("needs_score");
    const report = await loadBusinessReport(businessCategory || undefined);
    const readiness = buildBusinessReadinessGate(report?.summary, report?.total ?? businessQuestions?.total ?? 0);
    const workbench = buildBusinessReviewWorkbench(
      report?.items ?? [],
      codexReviewNotes?.latestByQuestionId ?? {},
      readiness.minBusinessScores,
      "needs_score",
    );
    setExpandedComparisonId(workbench.nextComparisonId || null);
    window.setTimeout(() => {
      const selector = workbench.nextComparisonId
        ? `[data-comparison-id="${workbench.nextComparisonId}"]`
        : ".astrbot-business-review-queue";
      document.querySelector(selector)?.scrollIntoView({ block: "start" });
    }, 0);
  }

  function openHotspotReviewQueue(category: string) {
    setTab("business");
    setBusinessMode("review");
    setBusinessCategory(category);
    setBusinessReviewFilter("draft_ready");
    setExpandedComparisonId(null);
    void loadBusinessReport(category).finally(() => {
      window.setTimeout(() => {
        document.querySelector(".astrbot-business-review-queue")?.scrollIntoView({ block: "start" });
      }, 0);
    });
  }

  const calibration = businessReport?.summary.judgeCalibration;
  const calibrationItems = calibration?.items ?? [];
  const visibleCalibrationItems = calibrationOnlyMismatches
    ? calibrationItems.filter(item => item.agreementStatus === "mismatch")
    : calibrationItems;
  const recommendedNextActions = businessReport?.summary.recommendedNextActions ?? [];
  const topRepairGapEntry = businessReport?.summary.topRepairGaps?.[0];
  const topFailureTag = businessReport?.summary.topFailureTags?.[0]?.tag ?? "";
  const topRepairGap = topRepairGapEntry?.gap
    ?? topRepairGapEntry?.tag
    ?? "";
  const topFailureOrRepairLabel = topFailureTag ? "Top Failure" : "Top Repair Gap";
  const topFailureOrRepairValue = topFailureTag || topRepairGap || "—";
  const topFailureOrRepairDisplayValue = businessDiagnosticLabel(topFailureOrRepairValue);
  const topRepairGapNote = topRepairGap && topRepairGap !== topFailureOrRepairValue ? topRepairGap : "";
  const topRepairGapNoteValue = topRepairGapNote ? businessDiagnosticLabel(topRepairGapNote) : "";
  const topRepairGapSampleIds = (topRepairGapEntry?.sampleQuestionIds?.length
    ? topRepairGapEntry.sampleQuestionIds
    : topRepairGapEntry?.sampleQuestions?.map(item => item.questionId).filter(Boolean)) ?? [];
  const businessReadiness = buildBusinessReadinessGate(businessReport?.summary, businessReport?.total ?? businessQuestions?.total ?? 0);
  const selfTestBaseline = businessReport?.summary.selfTestBaseline;
  const evidenceRepairQueue = normalizeEvidenceRepairQueue(businessReport?.evidenceRepairQueue, businessReport?.items ?? []);
  const sourceRepairBacklog = normalizeSourceRepairBacklog(businessReport?.sourceRepairBacklog);
  const visibleSourceRepairBacklog = sourceRepairBacklog.slice(0, 6);
  const evidenceRepairOverview = buildEvidenceRepairOverview(evidenceRepairQueue);
  const evidenceRepairDisplay = buildEvidenceRepairDisplayState(evidenceRepairQueue, showAllEvidenceRepairItems);
  const blockingReadinessItems = buildBlockingReadinessItems(evidenceRepairQueue);
  const codexReviewByQuestionId = codexReviewNotes?.latestByQuestionId ?? {};
  const codexDraftTriage = buildCodexDraftTriage(codexReviewNotes, businessReport?.items ?? []);
  const businessRecords = businessReport?.items ?? [];
  const businessReviewWorkbench = buildBusinessReviewWorkbench(
    businessRecords,
    codexReviewByQuestionId,
    businessReadiness.minBusinessScores,
    businessReviewFilter,
  );
  const baselineActionPlan = buildBusinessBaselineActionPlan(
    businessReadiness,
    businessReviewWorkbench,
    codexDraftTriage,
    Boolean(judgePreflight?.ready),
  );
  const referenceJudgePaths = judgePreflight?.referenceJudgePaths?.paths
    ?? businessReport?.summary.referenceJudgePaths?.paths
    ?? [];
  const baselineActionWorkbench = buildBusinessReviewWorkbench(
    businessRecords,
    codexReviewByQuestionId,
    businessReadiness.minBusinessScores,
    baselineActionPlan.recommendedFilter,
  );
  const baselineActionRecords = filterBusinessReviewRecords(
    businessRecords,
    codexReviewByQuestionId,
    baselineActionPlan.recommendedFilter,
  );
  const baselineNextReviewRecord = businessRecords.find(
    record => record.comparisonId === baselineActionWorkbench.nextComparisonId,
  );
  const baselineNextReviewReason = businessReviewPriorityReason(
    baselineNextReviewRecord,
    codexReviewByQuestionId,
  );
  const businessReviewRecords = filterBusinessReviewRecords(
    businessRecords,
    codexReviewByQuestionId,
    businessReviewFilter,
  );
  const codexDraftAcceptanceRecords = draftRecordsForCodexAcceptance(
    businessRecords,
    codexReviewByQuestionId,
    businessReviewWorkbench.neededForReviewTarget,
  );
  const importedManualSaveRecords = recordsForImportedManualSave(
    businessRecords,
    scoringDrafts,
    importedDraftComparisonIds,
  );
  const importedReferenceJudgeSaveRecords = recordsForImportedManualSave(
    businessRecords,
    scoringDrafts,
    referenceJudgeDraftComparisonIds,
  );

  function openNextBusinessReview() {
    if (!businessReviewWorkbench.nextComparisonId) return;
    setBusinessMode("review");
    setExpandedComparisonId(businessReviewWorkbench.nextComparisonId);
    window.setTimeout(() => {
      document
        .querySelector(`[data-comparison-id="${businessReviewWorkbench.nextComparisonId}"]`)
        ?.scrollIntoView({ block: "start" });
    }, 0);
  }

  function openBaselineActionReview() {
    if (!baselineActionWorkbench.nextComparisonId) return;
    setBusinessMode("review");
    setBusinessReviewFilter(baselineActionPlan.recommendedFilter);
    setExpandedComparisonId(baselineActionWorkbench.nextComparisonId);
    window.setTimeout(() => {
      document
        .querySelector(`[data-comparison-id="${baselineActionWorkbench.nextComparisonId}"]`)
        ?.scrollIntoView({ block: "start" });
    }, 0);
  }

  async function acceptCodexDraftsAsReviewSource() {
    const recordsToAccept = codexDraftAcceptanceRecords;
    if (recordsToAccept.length === 0) return;
    const confirmed = window.confirm(
      `Save ${recordsToAccept.length} Codex draft review${recordsToAccept.length === 1 ? "" : "s"} as codex_review scores? These drafts are audit/triage evidence and do not count toward the manual/GPT replacement baseline.`,
    );
    if (!confirmed) return;

    setRunning("accept-codex-drafts");
    setError(null);
    setBusinessOutput(`Saving ${recordsToAccept.length} Codex draft review${recordsToAccept.length === 1 ? "" : "s"} as codex_review source...`);
    const acceptedIds = new Set(recordsToAccept.map(record => record.comparisonId));
    try {
      for (const record of recordsToAccept) {
        const note = codexReviewByQuestionId[record.questionId];
        if (!note) continue;
        const dimensions = scoreDimensionsForRecord(record);
        const prefill = buildCodexDraftScorePrefill(note, dimensions);
        if (!prefill.complete) continue;
        await updateEvalSideBySideHumanScore(record.comparisonId, {
          status: "scored",
          source: "codex_review",
          winner: prefill.winner,
          notes: acceptedCodexDraftNotes(note),
          dimensions: dimensions.map(dimension => dimension.key),
          astrbotScores: prefill.astrbotScores,
          countryCopilotScores: prefill.countryCopilotScores,
          failureTags: prefill.failureTags,
        });
      }

      setScoringDrafts(prev => {
        const next = { ...prev };
        for (const id of acceptedIds) delete next[id];
        return next;
      });
      const nextRecord = draftRecordsForCodexAcceptance(
        businessRecords.filter(record => !acceptedIds.has(record.comparisonId)),
        codexReviewByQuestionId,
        1,
      )[0];
      setExpandedComparisonId(nextRecord?.comparisonId ?? null);
      setBusinessOutput(`Saved ${recordsToAccept.length} Codex draft review${recordsToAccept.length === 1 ? "" : "s"} as codex_review scores. Replacement baseline still requires manual or GPT judge scores.`);
      await loadSideBySideResults(compareCategory);
      await loadBusinessReport(businessCategory || undefined);
      if (nextRecord) {
        window.setTimeout(() => {
          document
            .querySelector(`[data-comparison-id="${nextRecord.comparisonId}"]`)
            ?.scrollIntoView({ block: "start" });
        }, 0);
      }
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setRunning(null);
    }
  }

  function renderComparisonDetail(record: EvalSideBySideRecord) {
    const scoring = readScoringDraft(record);
    const dimensions = scoreDimensionsForRecord(record);
    const astrbotCompleted = completedScoreCount(dimensions, scoring.astrbotScores);
    const countryCompleted = completedScoreCount(dimensions, scoring.countryCopilotScores);
    const astrbotDraftAverage = completeAverage(dimensions, scoring.astrbotScores);
    const countryDraftAverage = completeAverage(dimensions, scoring.countryCopilotScores);
    const draftComplete = canSaveBusinessScore(astrbotCompleted, countryCompleted, dimensions.length);
    const draftWinner = effectiveWinner(scoring, astrbotDraftAverage, countryDraftAverage, draftComplete);
    const draftStatus = effectiveStatus(scoring, draftComplete);
    const draftDecision = scoreDecisionText(draftStatus, draftWinner, draftComplete);
    const draftProgress = scoreProgressText(
      astrbotCompleted,
      countryCompleted,
      dimensions.length,
      astrbotDraftAverage,
      countryDraftAverage,
    );
    const astrbotMissingEvidence = missingEvidenceItems(record.astrbot?.missingEvidence)
      .slice(0, 3)
      .map(item => businessDiagnosticLabel(item.name))
      .join(", ") || "none";
    const astrbotFollowUps = listPreview(
      record.astrbot?.followUps,
      ["label", "question", "intent"],
      "none",
      4,
    );
    const astrbotEvidenceDigest = stringList(record.astrbot?.evidenceDigest ?? record.astrbotEvidenceDigest).slice(0, 4);
    const astrbotDisplayPlan = (
      record.astrbot?.displayPlan?.trim()
      || record.astrbotDisplayPlan?.trim()
      || ""
    );
    const astrbotArtifactPreviews = buildBusinessArtifactPreviews(
      record.astrbot?.visualArtifacts ?? record.astrbotVisualArtifacts,
    );
    const astrbotArtifactPreview = astrbotArtifactPreviews.length > 0
      ? astrbotArtifactPreviews.map(item => item.title).join(", ")
      : listPreview(
        record.astrbot?.visualArtifacts ?? record.astrbotVisualArtifacts,
        ["title", "type"],
        "none",
        3,
      );
    const codexReviewNote = codexReviewByQuestionId[record.questionId];
    const codexDraftPrefill = codexReviewNote
      ? buildCodexDraftScorePrefill(codexReviewNote, dimensions)
      : null;

    return (
      <div className="astrbot-compare-detail-grid astrbot-review-detail-deck">
        <article className="astrbot-answer-panel astrbot-review-answer-card is-astrbot-answer">
          <header>
            <strong>AstrBot Answer</strong>
            <span>
              {record.astrbot?.answerStatus || record.astrbot?.status || "—"}
              {" · "}
              {record.astrbot?.confidence || record.astrbot?.evidenceConfidence || "confidence n/a"}
            </span>
          </header>
          <p className="astrbot-answer-preview">{answerPreviewText(
            record.astrbot?.answerPreview || record.astrbot?.error,
            "No AstrBot answer preview stored for this comparison.",
          )}</p>
          {astrbotEvidenceDigest.length > 0 || astrbotDisplayPlan ? (
            <div className="astrbot-answer-evidence-strip" aria-label="AstrBot evidence and display plan">
              {astrbotEvidenceDigest.length > 0 ? (
                <div>
                  <span>Evidence digest</span>
                  <ul>
                    {astrbotEvidenceDigest.map(item => (
                      <li key={item}>{businessDiagnosticDisplayText(item)}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
              {astrbotDisplayPlan ? (
                <div>
                  <span>Display plan</span>
                  <strong>{businessDiagnosticDisplayText(astrbotDisplayPlan)}</strong>
                  <small>Artifacts: {astrbotArtifactPreview}</small>
                </div>
              ) : null}
            </div>
          ) : null}
          {astrbotArtifactPreviews.length > 0 ? (
            <div className="astrbot-artifact-preview-strip" aria-label="AstrBot visual artifact previews">
              {astrbotArtifactPreviews.map(artifact => (
                <section key={artifact.id}>
                  <header>
                    <span>{artifact.type.replace(/_/g, " ")}</span>
                    <small>{artifact.meta}</small>
                  </header>
                  <strong>{artifact.title}</strong>
                  <ul>
                    {artifact.lines.map(line => <li key={line}>{line}</li>)}
                  </ul>
                </section>
              ))}
            </div>
          ) : null}
          <div className="astrbot-answer-meta">
            <span>Refs: {record.astrbot?.evidenceRefCount ?? 0}</span>
            <span>Missing: {astrbotMissingEvidence}</span>
            <span>Follow-ups: {astrbotFollowUps}</span>
            <span>Playbook: {playbookTitle(record)}</span>
            <span>Sections: {playbookSections(record)}</span>
          </div>
        </article>
        <article className="astrbot-answer-panel astrbot-review-answer-card is-copilot-answer">
          <header>
            <strong>CountryCopilot Answer</strong>
            <span>
              {record.countryCopilot?.answerMode || record.countryCopilot?.status || "—"}
              {" · "}
              {record.countryCopilot?.confidence || "confidence n/a"}
            </span>
          </header>
          <p className="astrbot-answer-preview">{answerPreviewText(
            record.countryCopilot?.answerPreview || record.countryCopilot?.error,
            "No CountryCopilot answer preview stored for this comparison.",
          )}</p>
          <div className="astrbot-answer-meta">
            <span>Route: {record.countryCopilot?.intentRoute || "—"}</span>
            <span>Sources: {record.countryCopilot?.sourceCount ?? 0}</span>
            <span>Chars: {record.comparison?.countryCopilotAnswerChars ?? 0}</span>
          </div>
        </article>
        <article className="astrbot-score-panel astrbot-review-score-panel">
          <header>
            <strong>Quick total score / 业务评分（点一个总分即可）</strong>
            <span>先给两边各点一个 1-5 总分；失败标签和备注只记录为什么。</span>
          </header>
          {record.humanScoring?.source === "llm_judge" ? (
            <p className="astrbot-score-helper">
              GPT judge draft from {String(record.humanScoring.judgeProvider?.model ?? "configured judge model")}. Review or override before treating it as final business acceptance.
            </p>
          ) : null}
          <div className={`astrbot-score-decision ${draftComplete ? "is-complete" : "is-incomplete"}`}>
            <strong>{draftDecision}</strong>
            <span>{draftComplete ? `Ready to save · ${draftProgress}` : draftProgress}</span>
            <small>{scoreCompletionText(astrbotCompleted, countryCompleted, dimensions.length)}</small>
          </div>
          <div className="astrbot-score-preset-strip" aria-label={`Quick verdict score presets for ${record.questionId}`}>
            <span>1. One-click pair 快速结论</span>
            <div>
              {BUSINESS_QUICK_VERDICTS.map(verdict => (
                <button
                  key={verdict.label}
                  type="button"
                  className={astrbotDraftAverage === verdict.astrbotScore && countryDraftAverage === verdict.countryCopilotScore ? "is-active" : ""}
                  onClick={() => applyScorePreset(record, verdict.winner, verdict.astrbotScore, verdict.countryCopilotScore)}
                >
                  <strong>{verdict.label}</strong>
                  <small>
                    {verdict.astrbotScore}
                    {" / "}
                    {verdict.countryCopilotScore}
                    {" · "}
                    {verdict.description}
                  </small>
                </button>
              ))}
            </div>
          </div>
          {codexReviewNote && codexDraftPrefill ? (
            <div className={`astrbot-codex-score-banner ${codexReviewNote.uiStatus === "fail" ? "is-low" : codexReviewNote.uiStatus === "pass" ? "is-good" : "is-ok"}`}>
              <div>
                <span>Codex draft available</span>
                <strong>
                  {formatManualScore(codexDraftPrefill.astrbotAverage)}
                  {" / "}
                  {formatManualScore(codexDraftPrefill.countryCopilotAverage)}
                  {" · "}
                  {winnerText(codexDraftPrefill.winner)}
                </strong>
                <small>
                  {codexReviewNote.uiStatus}
                  {" · "}
                  {codexDraftPrefill.complete ? "ready to prefill" : "partial draft"}
                  {" · "}
                  writes audit note, not baseline
                </small>
              </div>
              <div className="astrbot-codex-score-actions">
                <button
                  type="button"
                  className="astrbot-chip-button"
                  onClick={() => applyCodexReviewDraft(record, codexReviewNote)}
                >
                  Prefill draft scores
                </button>
                <button
                  type="button"
                  className="astrbot-chip-button is-active"
                  onClick={() => void acceptCodexDraftAndNext(record, codexReviewNote)}
                  disabled={running !== null || !codexDraftPrefill.complete}
                  title={codexDraftPrefill.complete
                    ? "Save this Codex draft as codex_review audit evidence and open the next draft. It does not count toward the manual/GPT replacement baseline."
                    : "Codex draft is partial; prefill and finish both totals before saving"}
                >
                  {running === `accept-draft-next: ${record.comparisonId}` ? "Saving…" : "Save draft & next"}
                </button>
              </div>
            </div>
          ) : null}
          <div className="astrbot-score-primary-grid">
            <section className="astrbot-score-side-card" aria-label={`AstrBot total score for ${record.questionId}`}>
              <div>
                <span>2. AstrBot 总分</span>
                <strong>{formatManualScore(astrbotDraftAverage)}</strong>
                <small>点 5 就是总分 5，自动填满所有维度</small>
              </div>
              <div className="astrbot-score-button-row">
                {[1, 2, 3, 4, 5].map(score => (
                  <button
                    key={`astrbot-${score}`}
                    type="button"
                    className={astrbotDraftAverage === score ? "is-active" : ""}
                    title={businessScoreRubricLabel(score)}
                    aria-label={`AstrBot score ${businessScoreRubricLabel(score)}`}
                    onClick={() => updateOverallScore(record, "astrbot", score)}
                  >
                    <span>{score}</span>
                    <small>{businessScoreShortLabel(score)}</small>
                  </button>
                ))}
              </div>
            </section>
            <section className="astrbot-score-side-card" aria-label={`CountryCopilot total score for ${record.questionId}`}>
              <div>
                <span>3. Copilot 总分</span>
                <strong>{formatManualScore(countryDraftAverage)}</strong>
                <small>点 5 就是总分 5，自动填满所有维度</small>
              </div>
              <div className="astrbot-score-button-row">
                {[1, 2, 3, 4, 5].map(score => (
                  <button
                    key={`copilot-${score}`}
                    type="button"
                    className={countryDraftAverage === score ? "is-active" : ""}
                    title={businessScoreRubricLabel(score)}
                    aria-label={`CountryCopilot score ${businessScoreRubricLabel(score)}`}
                    onClick={() => updateOverallScore(record, "countryCopilot", score)}
                  >
                    <span>{score}</span>
                    <small>{businessScoreShortLabel(score)}</small>
                  </button>
                ))}
              </div>
            </section>
          </div>
          <section className={`astrbot-score-save-row ${draftComplete ? "is-complete" : "is-incomplete"}`}>
            <div>
              <span>4. 保存 baseline</span>
              <strong>
                {draftComplete
                  ? `AstrBot ${formatManualScore(astrbotDraftAverage)} / Copilot ${formatManualScore(countryDraftAverage)}`
                  : "先给 AstrBot 和 Copilot 各点一个总分"}
              </strong>
              <small>
                {draftComplete
                  ? `${winnerText(draftWinner)} · 总分已自动填满 ${dimensions.length} 个维度`
                  : scoreCompletionText(astrbotCompleted, countryCompleted, dimensions.length)}
              </small>
            </div>
            <div className="astrbot-score-actions">
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => clearScoringDraft(record)}
                disabled={running !== null}
              >
                Clear
              </button>
              <button
                type="button"
                className="astrbot-chip-button is-active"
                onClick={() => void saveHumanScore(record)}
                disabled={running !== null || !draftComplete}
                title={draftComplete ? "Save this confirmed baseline score" : "Pick one 1-5 total for AstrBot and Copilot before saving"}
              >
                {running === `score: ${record.comparisonId}` ? "Saving…" : "Save score"}
              </button>
              <button
                type="button"
                className="astrbot-chip-button is-active"
                onClick={() => void saveHumanScore(record, { advanceAfterSave: true })}
                disabled={running !== null || !draftComplete}
                title={draftComplete ? "Save this confirmed score and open the next matching review row" : "Pick one 1-5 total for AstrBot and Copilot before saving and advancing"}
              >
                {running === `score-next: ${record.comparisonId}` ? "Saving…" : "Save & next"}
              </button>
            </div>
          </section>
          <div className="astrbot-score-feedback-row">
            <section className="astrbot-score-tag-panel" aria-label={`Failure tags for ${record.questionId}`}>
              <div>
                <span>可选：失败标签</span>
                <strong>{scoring.failureTags.length > 0 ? `${scoring.failureTags.length} selected` : "optional"}</strong>
              </div>
              <div className="astrbot-score-tag-chips">
                {BUSINESS_QUICK_FAILURE_TAGS.map(tag => (
                  <button
                    key={tag}
                    type="button"
                    className={scoring.failureTags.includes(tag) ? "is-active" : ""}
                    onClick={() => toggleFailureTag(record, tag)}
                    title={tag}
                  >
                    {businessDiagnosticLabel(tag)}
                  </button>
                ))}
              </div>
            </section>
            <label className="astrbot-score-notes-panel">
              <span>Review note</span>
              <textarea
                className="astrbot-score-notes"
                value={scoring.notes}
                onChange={event => updateScoringDraft(record, { notes: event.target.value })}
                placeholder="why this side wins, or what must improve before replacement"
                aria-label={`Human score notes for ${record.questionId}`}
              />
            </label>
          </div>
          <div className="astrbot-score-controls">
            <details className="astrbot-score-details astrbot-score-rubric-details">
              <summary>{BUSINESS_SCORE_GUIDE_SUMMARY}</summary>
              <div className="astrbot-score-rubric-strip" aria-label={`Business score rubric for ${record.questionId}`}>
                {BUSINESS_SCORE_RUBRIC.map(item => (
                  <span key={item.score} title={`${item.label}: ${item.description}`}>
                    <strong>{item.score}</strong>
                    <em>{item.shortLabel}</em>
                    <small>{item.description}</small>
                  </span>
                ))}
              </div>
            </details>
            {codexReviewNote ? (
              <details className="astrbot-score-details astrbot-codex-review-details">
                <summary>Draft rationale</summary>
                <div className={`astrbot-codex-review-card ${codexReviewNote.uiStatus === "fail" ? "is-low" : codexReviewNote.uiStatus === "pass" ? "is-good" : "is-ok"}`}>
                  <div>
                    <span>Codex Review Draft</span>
                    <strong>{winnerText(codexReviewNote.suggestedWinner)}</strong>
                  </div>
                  <p>{codexReviewNote.reviewNotes}</p>
                  <small>
                    Tags: {codexReviewNote.suggestedFailureTags.slice(0, 4).join(", ") || "none"}
                    {" · "}
                    Screenshots: {codexReviewNote.screenshots.length}
                  </small>
                </div>
              </details>
            ) : null}
            <details className="astrbot-score-details astrbot-score-advanced-details">
              <summary>高级：只在个别维度偏离总分时展开</summary>
              <p className="astrbot-score-advanced-note">
                总分按钮已经把所有维度填成同一分；这里只在某个维度明显偏离总分时手动修正。
              </p>
              <div className="astrbot-score-advanced-row">
                <label>
                  <span>Winner override</span>
                  <select
                    value={scoring.winner}
                    onChange={event => updateScoringDraft(record, { winner: event.target.value })}
                    aria-label={`Human score winner override for ${record.questionId}`}
                  >
                    <option value="">Auto from scores</option>
                    <option value="astrbot">AstrBot</option>
                    <option value="countryCopilot">CountryCopilot</option>
                    <option value="tie">Tie</option>
                    <option value="unclear">Unclear</option>
                  </select>
                </label>
                <div className="astrbot-score-saved-state">
                  <span>Saved</span>
                  <strong>
                    {record.humanScoring?.scoreTotals?.astrbotComplete ? formatManualScore(record.humanScoring?.scoreTotals?.astrbot) : "—"}
                    {" / "}
                    {record.humanScoring?.scoreTotals?.countryCopilotComplete ? formatManualScore(record.humanScoring?.scoreTotals?.countryCopilot) : "—"}
                  </strong>
                </div>
              </div>
              <label className="astrbot-score-tag-editor">
                <span>Failure tags override</span>
                <input
                  type="text"
                  value={scoring.failureTags.join(", ")}
                  onChange={event => updateScoringDraft(record, { failureTags: commaStringToList(event.target.value) })}
                  placeholder="pm_insight_weak, tool_missing"
                  aria-label={`Failure tags override for ${record.questionId}`}
                />
              </label>
              <div className="astrbot-score-matrix-header">
                <span>Dimension</span>
                <span>AstrBot</span>
                <span>Copilot</span>
              </div>
              <div className="astrbot-score-dimension-grid">
                {dimensions.map(dimension => (
                  <label key={dimension.key}>
                    <span>{dimension.label}</span>
                    <input
                      type="number"
                      min={1}
                      max={5}
                      value={scoreInputValue(scoring.astrbotScores, dimension.key)}
                      onChange={event => updateScoringDraft(record, {
                        astrbotScores: nextScoreMap(scoring.astrbotScores, dimension.key, event.target.value),
                      })}
                      aria-label={`AstrBot ${dimension.label} score for ${record.questionId}`}
                    />
                    <input
                      type="number"
                      min={1}
                      max={5}
                      value={scoreInputValue(scoring.countryCopilotScores, dimension.key)}
                      onChange={event => updateScoringDraft(record, {
                        countryCopilotScores: nextScoreMap(scoring.countryCopilotScores, dimension.key, event.target.value),
                      })}
                      aria-label={`CountryCopilot ${dimension.label} score for ${record.questionId}`}
                    />
                  </label>
                ))}
              </div>
            </details>
          </div>
        </article>
      </div>
    );
  }

  return (
    <div className="astrbot-eval-panel">
      {/* Tab bar */}
      <nav className="astrbot-eval-tabs">
        {(["overview", "results", "questions", "run", "compare", "business"] as TabKey[]).map(t => (
          <button
            key={t}
            type="button"
            className={`astrbot-chip-button ${tab === t ? "is-active" : ""}`}
            onClick={() => {
              setTab(t);
              if (t === "results") void loadResults(resultsCategory);
              if (t === "compare") {
                void loadSideBySideResults(compareCategory);
                void loadCodexReviewNotes();
              }
              if (t === "business") void loadBusinessReport(businessCategory);
            }}
          >
            {tabLabel(t)}
          </button>
        ))}
      </nav>

      {error ? <div className="astrbot-status-error" role="status">{error}</div> : null}
      {loadWarning ? <div className="astrbot-status-warning" role="status">{loadWarning}</div> : null}
      {loading ? <div className="astrbot-table-empty">Loading eval data…</div> : null}

      {/* Overview */}
      {tab === "overview" && summary ? (
        <section className="astrbot-eval-overview">
          <div className="astrbot-agent-card-grid">
            <div className="astrbot-agent-card">
              <span>Total Runs</span>
              <strong>{summary.totalRuns}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Overall Composite</span>
              <strong className={scoreTone(summary.overallScores.avgComposite)}>
                {formatScore(summary.overallScores.avgComposite)}
              </strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Avg Evidence</span>
              <strong>{formatScore(summary.overallScores.avgEvidence)}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Avg Citation</span>
              <strong>{formatScore(summary.overallScores.avgCitation)}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Latest Run</span>
              <strong>{summary.latestRunAt ? new Date(summary.latestRunAt).toLocaleDateString() : "—"}</strong>
            </div>
          </div>

          {usage && usage.totalRuns > 0 ? (
            <>
              <h4>Usage &amp; Cost (Hermes-integrated)</h4>
              <div className="astrbot-agent-card-grid">
                <div className="astrbot-agent-card">
                  <span>Total Tokens</span>
                  <strong>{(usage.totalTokens ?? 0).toLocaleString()}</strong>
                </div>
                <div className="astrbot-agent-card">
                  <span>Input / Output</span>
                  <strong>{(usage.totalInputTokens ?? 0).toLocaleString()} / {(usage.totalOutputTokens ?? 0).toLocaleString()}</strong>
                </div>
                <div className="astrbot-agent-card">
                  <span>Est. Cost</span>
                  <strong>{usage.currency} {(usage.totalCostCny ?? 0).toFixed(4)}</strong>
                </div>
                <div className="astrbot-agent-card">
                  <span>Avg / Run</span>
                  <strong>{usage.currency} {(usage.avgCostPerRunCny ?? 0).toFixed(6)}</strong>
                </div>
              </div>
              <p style={{fontSize:11,color:'var(--c-text-muted)',marginTop:4}}>
                Usage data auto-written to <code>hermes/eval/eval_usage.jsonl</code> and <code>hermes/answer_audit.jsonl</code>. Hermes cost report picks up AstrBot records automatically.
              </p>
            </>
          ) : null}

          <h4>By Category</h4>
          <div className="astrbot-table-shell">
	              <table className="astrbot-table astrbot-compare-table">
              <thead>
	                      <tr className="astrbot-compare-summary-row">
                  <th>Category</th>
                  <th>Runs</th>
                  <th>Composite</th>
                  <th>Evidence</th>
                  <th>Citation</th>
                  <th>Tool</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(summary.byCategory).map(([cat, s]) => (
                  <tr key={cat}>
                    <td><strong>{CATEGORY_LABELS[cat] ?? cat}</strong></td>
                    <td>{s.count}</td>
                    <td className={scoreTone(s.avgComposite)}>{formatScore(s.avgComposite)}</td>
                    <td>{formatScore(s.avgEvidence)}</td>
                    <td>{formatScore(s.avgCitation)}</td>
                    <td>{formatScore(s.avgTool)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* Results */}
      {tab === "results" ? (
        <section className="astrbot-eval-results">
          <div className="astrbot-memory-filters">
            <label>
              <span>Category</span>
              <select value={resultsCategory} onChange={e => { setResultsCategory(e.target.value); void loadResults(e.target.value || undefined); }}>
                <option value="">All</option>
                {[...Object.keys(CATEGORY_LABELS), ...Object.keys(BUSINESS_CATEGORY_LABELS)].map(c => (
                  <option key={c} value={c}>{categoryLabel(c)}</option>
                ))}
              </select>
            </label>
          </div>
          <div className="astrbot-table-shell">
            <table className="astrbot-table">
              <thead>
                <tr>
                  <th>Question</th>
                  <th>Category</th>
                  <th>Expected</th>
                  <th>Actual</th>
                  <th>Composite</th>
                </tr>
              </thead>
              <tbody>
                {results.length === 0 ? (
                  <tr><td colSpan={5} className="astrbot-table-empty">No results yet. Run an eval to populate.</td></tr>
                ) : results.map(r => (
                  <tr key={r.evalId}>
                    <td title={r.question}>{r.question.slice(0, 50)}…</td>
                    <td>{CATEGORY_LABELS[r.category] ?? r.category}</td>
                    <td><code>{r.expectedRetrievalPath}</code></td>
                    <td><code>{r.actualRetrievalPath}</code></td>
                    <td className={scoreTone(r.scores.composite)}><strong>{formatScore(r.scores.composite)}</strong></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* Questions */}
      {tab === "questions" ? (
        <section className="astrbot-eval-questions">
          <div className="astrbot-agent-card-grid">
            {Object.entries(CATEGORY_LABELS).map(([cat, label]) => {
              const count = questions.filter(q => q.category === cat).length;
              return (
                <div className="astrbot-agent-card" key={cat}>
                  <span>{label}</span>
                  <strong>{count} questions</strong>
                </div>
              );
            })}
          </div>
          <div className="astrbot-table-shell">
            <table className="astrbot-table">
              <thead>
                <tr><th>ID</th><th>Category</th><th>Country</th><th>Question</th><th>Difficulty</th><th>Action</th></tr>
              </thead>
              <tbody>
                {questions.slice(0, 30).map(q => (
                  <tr key={q.id}>
                    <td><code>{q.id}</code></td>
                    <td>{CATEGORY_LABELS[q.category] ?? q.category}</td>
                    <td>{q.country}</td>
                    <td>{q.question.slice(0, 60)}…</td>
                    <td>{q.difficulty}</td>
                    <td>
                      <div className="astrbot-inline-actions">
                        <button type="button" className="astrbot-chip-button" onClick={() => void runSingle(q.id, q.id)} disabled={running !== null}>
                          {running === q.id ? "Running…" : "Run"}
                        </button>
                        <button type="button" className="astrbot-chip-button" onClick={() => void runCompareQuestion(q.id, q.id)} disabled={running !== null}>
                          {running === `compare-question: ${q.id}` ? "Comparing…" : "Compare"}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* Run */}
      {tab === "run" ? (
        <section className="astrbot-eval-run">
          <div className="astrbot-agent-actions">
            <button type="button" className="astrbot-chip-button" onClick={() => void runSmokeSuite()} disabled={running !== null}>
              {running === "smoke suite" ? "Running…" : "Run Smoke (25 Qs)"}
            </button>
            <button type="button" className="astrbot-primary-action" onClick={() => void runFullSuite()} disabled={running !== null}>
              {running === "full suite" ? "Running…" : "Run Full (100 Qs)"}
            </button>
            {Object.keys(CATEGORY_LABELS).map(cat => (
              <button key={cat} type="button" className="astrbot-chip-button" onClick={() => void runCat(cat)} disabled={running !== null}>
                {running === `category: ${cat}` ? "…" : CATEGORY_LABELS[cat]}
              </button>
            ))}
          </div>
          {runOutput ? (
            <pre className="astrbot-tool-result">{runOutput}</pre>
          ) : (
            <div className="astrbot-table-empty">
              Select a category, smoke suite, or full suite to execute eval questions through the JATO Agent.
              Results are auto-scored on retrieval path correctness, evidence traceability, citation coverage, chart correctness, and tool selection.
            </div>
          )}
        </section>
      ) : null}

      {/* Compare */}
      {tab === "compare" ? (
        <section className="astrbot-eval-compare">
          <div className="astrbot-memory-filters">
            <label>
              <span>Category</span>
              <select value={compareCategory} onChange={e => { setCompareCategory(e.target.value); void loadSideBySideResults(e.target.value || undefined); }}>
                <option value="">All</option>
                {[...Object.keys(CATEGORY_LABELS), ...Object.keys(BUSINESS_CATEGORY_LABELS)].map(c => (
                  <option key={c} value={c}>{categoryLabel(c)}</option>
                ))}
              </select>
            </label>
            <button type="button" className="astrbot-chip-button" onClick={() => void loadSideBySideResults(compareCategory)} disabled={running !== null}>
              Refresh
            </button>
          </div>

          <div className="astrbot-agent-actions">
            {Object.keys(CATEGORY_LABELS).map(cat => (
              <button key={cat} type="button" className="astrbot-chip-button" onClick={() => void runCompareCategory(cat)} disabled={running !== null}>
                {running === `compare-category: ${cat}` ? "Comparing…" : `Compare 1 ${CATEGORY_LABELS[cat]}`}
              </button>
            ))}
          </div>

          {sideBySide ? (
            <div className="astrbot-agent-card-grid">
              <div className="astrbot-agent-card">
                <span>Comparisons</span>
                <strong>{sideBySide.total}</strong>
              </div>
              <div className="astrbot-agent-card">
                <span>Pending Human Score</span>
                <strong>{sideBySide.summary.pendingHumanScoring}</strong>
              </div>
              <div className="astrbot-agent-card">
                <span>Avg AstrBot Composite</span>
                <strong className={scoreTone(sideBySide.summary.avgAstrBotComposite)}>{formatScore(sideBySide.summary.avgAstrBotComposite)}</strong>
              </div>
              <div className="astrbot-agent-card">
                <span>Errors</span>
                <strong>{sideBySide.summary.astrbotErrorCount} / {sideBySide.summary.countryCopilotErrorCount}</strong>
              </div>
            </div>
          ) : null}

          {compareOutput ? <pre className="astrbot-tool-result">{compareOutput}</pre> : null}

          <div className="astrbot-table-shell astrbot-review-table-shell">
            <table className="astrbot-table astrbot-compare-table astrbot-review-table">
              <thead>
                <tr>
                  <th>Question</th>
                  <th>Category</th>
                  <th>AstrBot</th>
                  <th>CountryCopilot</th>
                  <th>Both</th>
                  <th>Human</th>
                </tr>
              </thead>
              <tbody>
                {(sideBySide?.items ?? []).length === 0 ? (
                  <tr><td colSpan={6} className="astrbot-table-empty">No side-by-side records yet. Run a comparison to populate this table.</td></tr>
	                ) : (sideBySide?.items ?? []).map(record => {
	                  const isExpanded = expandedComparisonId === record.comparisonId;
	                  const scoring = readScoringDraft(record);
	                  const dimensions = scoreDimensionsForRecord(record);
                  const astrbotCompleted = completedScoreCount(dimensions, scoring.astrbotScores);
                  const countryCompleted = completedScoreCount(dimensions, scoring.countryCopilotScores);
                  const astrbotDraftAverage = completeAverage(dimensions, scoring.astrbotScores);
                  const countryDraftAverage = completeAverage(dimensions, scoring.countryCopilotScores);
                  const draftComplete = dimensions.length > 0 && astrbotCompleted === dimensions.length && countryCompleted === dimensions.length;
                  const draftWinner = effectiveWinner(scoring, astrbotDraftAverage, countryDraftAverage, draftComplete);
                  const failureTags = stringList(record.failureTags);
                  return (
                    <Fragment key={record.comparisonId}>
	                      <tr className={isExpanded ? "astrbot-compare-row is-expanded" : "astrbot-compare-row"}>
	                        <td title={record.question}>
	                          <div className="astrbot-question-cell">
	                            <span>{truncateText(record.question, 72)}</span>
	                            {failureTags.length > 0 ? (
	                              <span className="astrbot-failure-tags">
	                                {failureTags.slice(0, 3).map(tag => <em key={tag} title={tag}>{businessDiagnosticLabel(tag)}</em>)}
	                              </span>
	                            ) : null}
	                            <button
	                              type="button"
	                              className="astrbot-row-toggle"
	                              aria-expanded={isExpanded}
	                              onClick={() => setExpandedComparisonId(isExpanded ? null : record.comparisonId)}
	                            >
	                              {isExpanded ? "Hide review" : "Review"}
	                            </button>
	                          </div>
	                        </td>
                        <td>{categoryLabel(record.category)}</td>
                        <td>
                          <div className="astrbot-side-cell">
                            <strong className={statusTone(record.astrbot?.status, record.astrbot?.error)}>
                              {record.astrbot?.selectedTool || record.astrbot?.status || "—"}
                            </strong>
                            <span>{formatOptionalScore(record.astrbot?.scores?.composite)} · {record.astrbot?.retrievalPath || "—"}</span>
                          </div>
                        </td>
                        <td>
                          <div className="astrbot-side-cell">
                            <strong className={statusTone(record.countryCopilot?.status, record.countryCopilot?.error)}>
                              {record.countryCopilot?.answerMode || record.countryCopilot?.status || "—"}
                            </strong>
                            <span>{record.countryCopilot?.intentRoute || "—"} · {record.countryCopilot?.sourceCount ?? 0} sources</span>
                          </div>
                        </td>
                        <td>
                          <div className="astrbot-side-cell">
                            <strong className={record.comparison?.bothReturned ? "is-good" : "is-low"}>
                              {record.comparison?.bothReturned ? "Yes" : "No"}
                            </strong>
                            <span>{record.comparison?.errorCount ?? 0} errors</span>
                          </div>
                        </td>
		                        <td>
		                          <div className={`astrbot-human-summary ${draftComplete ? "is-complete" : "is-incomplete"}`}>
		                            <strong>
                                  {draftComplete
                                    ? `${formatManualScore(astrbotDraftAverage)} / ${formatManualScore(countryDraftAverage)}`
                                    : "Needs totals"}
                                </strong>
		                            <span>{draftComplete ? winnerText(draftWinner) : scoreCompletionText(astrbotCompleted, countryCompleted, dimensions.length)}</span>
		                          </div>
			                        </td>
		                      </tr>
		                      {isExpanded ? (
		                      <tr className="astrbot-compare-detail-row">
			                        <td colSpan={6}>
			                          {renderComparisonDetail(record)}
		                        </td>
		                      </tr>
		                      ) : null}
	                    </Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {/* Business Validation */}
      {tab === "business" ? (
        <section className="astrbot-eval-business">
          <div className="astrbot-memory-filters">
            <label>
              <span>Business Category</span>
              <select
                value={businessCategory}
                onChange={event => {
                  setBusinessCategory(event.target.value);
                  void loadBusinessReport(event.target.value || undefined);
                }}
              >
                <option value="">All</option>
                {Object.entries(BUSINESS_CATEGORY_LABELS).map(([cat, label]) => (
                  <option key={cat} value={cat}>{label}</option>
                ))}
              </select>
            </label>
            <button
              type="button"
              className="astrbot-chip-button"
              onClick={() => {
                void loadBusinessReport(businessCategory);
                void loadJudgePreflight();
              }}
              disabled={running !== null}
            >
              Refresh Report
            </button>
          </div>

          <div className="astrbot-eval-mode-switch" role="group" aria-label="Business validation mode">
            <button
              type="button"
              className={`astrbot-chip-button ${businessMode === "review" ? "is-active" : ""}`}
              onClick={() => setBusinessMode("review")}
            >
              Review Queue
            </button>
            <button
              type="button"
              className={`astrbot-chip-button ${businessMode === "questions" ? "is-active" : ""}`}
              onClick={() => setBusinessMode("questions")}
            >
              Questions / Run
            </button>
            <button
              type="button"
              className={`astrbot-chip-button ${businessMode === "calibration" ? "is-active" : ""}`}
              onClick={() => setBusinessMode("calibration")}
            >
              Judge Calibration
            </button>
          </div>

          <div className="astrbot-agent-card-grid astrbot-business-kpi-grid">
            <div className="astrbot-agent-card">
              <span>Manual Questions</span>
              <strong>{businessQuestions?.total ?? 0}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Business Comparisons</span>
              <strong>{businessReport?.total ?? 0}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Baseline Scored</span>
              <strong>{businessReport?.summary.baselineScoredCount ?? businessReport?.summary.scoredCount ?? 0}</strong>
              <small>{countBreakdownText(businessReport?.summary.baselineSourceCounts ?? businessReport?.summary.humanScoreSourceCounts ?? {}, "sources none")}</small>
            </div>
            <div className="astrbot-agent-card">
              <span>Codex Self-test</span>
              <strong>
                {selfTestBaseline?.scoredCount ?? 0}/{selfTestBaseline?.totalQuestions ?? businessReport?.total ?? 0}
              </strong>
              <small>
                Win {Math.round((selfTestBaseline?.astrbotWinRate ?? 0) * 100)}%
                {" · "}
                {formatManualScore(selfTestBaseline?.avgAstrBotScore)}
                {" / "}
                {formatManualScore(selfTestBaseline?.avgCountryCopilotScore)}
              </small>
            </div>
            <div className="astrbot-agent-card">
              <span>AstrBot Wins</span>
              <strong>{businessReport?.summary.humanWins?.astrbot ?? 0}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>AstrBot Win Rate</span>
              <strong>{Math.round((businessReport?.summary.astrbotWinRate ?? 0) * 100)}%</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Avg Human Score</span>
              <strong>
                {formatManualScore(businessReport?.summary.avgAstrBotHumanScore)}
                {" / "}
                {formatManualScore(businessReport?.summary.avgCountryCopilotHumanScore)}
              </strong>
            </div>
            <div className="astrbot-agent-card astrbot-agent-card--wide">
              <span>Readiness</span>
              <strong title={businessReport?.summary.replacementReadinessVerdict ?? "not_enough_data"}>
                {readableStatusLabel(businessReport?.summary.replacementReadinessVerdict ?? "not_enough_data")}
              </strong>
            </div>
            <div className="astrbot-agent-card astrbot-agent-card--wide">
              <span>{topFailureOrRepairLabel}</span>
              <strong title={topFailureOrRepairValue}>
                {topFailureOrRepairDisplayValue}
              </strong>
              {topRepairGapNoteValue ? <small title={topRepairGapNote}>Repair: {topRepairGapNoteValue}</small> : null}
              {topRepairGapSampleIds.length > 0 ? (
                <small title={topRepairGapSampleIds.join(", ")}>
                  Samples: {topRepairGapSampleIds.slice(0, 3).join(", ")}
                </small>
              ) : null}
            </div>
            <div className="astrbot-agent-card">
              <span>GPT-Human Agreement</span>
              <strong>{Math.round((calibration?.agreementRate ?? 0) * 100)}%</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Judge Review Gap</span>
              <strong>{calibration?.needsHumanReviewCount ?? 0}</strong>
            </div>
          </div>

          <section className={`astrbot-readiness-gate is-${businessReadiness.status}`} aria-label="AstrBot replacement readiness">
            <div className="astrbot-readiness-summary">
              <span>Replacement Gate</span>
              <strong>{businessReadiness.replacementReady ? "Ready to consider switch" : "Not ready to replace /copilot"}</strong>
              <p>{businessReadiness.nextAction}</p>
            </div>
            <div className="astrbot-readiness-metrics">
              <article className={businessReadiness.engineeringClean ? "is-good" : "is-low"}>
                <span>Engineering</span>
                <strong>{businessReadiness.engineeringClean ? "Clean" : "Blocked"}</strong>
                <small>{businessReadiness.engineeringFailureTagTotal} engineering tags · {businessReadiness.errorTotal} errors</small>
              </article>
              <article className={businessReadiness.evidenceReady ? "is-good" : "is-ok"}>
                <span>Evidence / Data</span>
                <strong>{businessReadiness.evidenceReady ? "Ready" : "Data gap"}</strong>
                <small>{businessReadiness.evidenceGapTotal} evidence gaps · {businessReadiness.failureTagTotal} total tags</small>
              </article>
              <article className={businessReadiness.businessBaselineReady ? "is-good" : "is-ok"}>
                <span>Replacement baseline</span>
                <strong>{businessReadiness.scoredBaseline}/{businessReadiness.totalQuestions}</strong>
                <small>
                  Manual/GPT only · Need {businessReadiness.minBusinessScores} · Pending {businessReadiness.pendingBaselineScoring}
                  {" · "}
                  {countBreakdownText(businessReadiness.baselineSourceCounts, "sources none")}
                </small>
              </article>
              <article className={selfTestBaseline?.selfTestReady ? "is-good" : "is-ok"}>
                <span>Self-test baseline</span>
                <strong>
                  {selfTestBaseline?.scoredCount ?? businessReadiness.humanScored}/{selfTestBaseline?.totalQuestions ?? businessReadiness.totalQuestions}
                </strong>
                <small>
                  Codex/manual/GPT · Need {selfTestBaseline?.minimumRequiredScores ?? businessReadiness.minBusinessScores}
                  {" · "}
                  Codex {selfTestBaseline?.codexReviewedCount ?? 0}
                  {" · "}
                  {countBreakdownText(selfTestBaseline?.sourceCounts ?? businessReport?.summary.baselineSourceCounts ?? {}, "sources none")}
                </small>
                <p>{selfTestBaseline?.recommendedNextAction ?? "Run review harness or score records to build a self-test baseline."}</p>
              </article>
              <article className={judgePreflight?.ready ? "is-good" : "is-low"}>
                <span>Judge provider</span>
                <strong>{judgePreflight?.status ?? "unknown"}</strong>
                <small>{judgePreflight?.provider.model ?? "model n/a"} · {judgePreflight?.provider.keySource ?? "key source n/a"}</small>
                <p>{judgePreflight?.reason ?? "Judge preflight has not loaded."}</p>
              </article>
              <article className={businessReadiness.winRateReady ? "is-good" : "is-ok"}>
                <span>AstrBot win rate</span>
                <strong>{Math.round(businessReadiness.winRate * 100)}%</strong>
                <small>Target 70% after scoring</small>
              </article>
              <article className={businessReadiness.replacementReady ? "is-good" : "is-low"}>
                <span>Backend verdict</span>
                <strong title={businessReadiness.verdict}>{readableStatusLabel(businessReadiness.verdict)}</strong>
                <small>Default route stays /copilot</small>
              </article>
            </div>
          </section>

          <section className={`astrbot-baseline-action-panel is-${baselineActionPlan.tone}`} aria-label="Replacement baseline next step">
            <div className="astrbot-baseline-action-copy">
              <span>Baseline next step</span>
              <strong>{baselineActionPlan.title}</strong>
              <p>{baselineActionPlan.description}</p>
              <div
                className="astrbot-baseline-progress"
                aria-label={`Replacement baseline progress ${baselineActionPlan.progressLabel}`}
              >
                <div>
                  <span style={{ width: `${baselineActionPlan.progressPercent}%` }} />
                </div>
                <small>{baselineActionPlan.progressLabel}</small>
              </div>
              <div className="astrbot-baseline-next-card" aria-label="Recommended next baseline review row">
                <span>Next review row</span>
                <strong>
                  {baselineNextReviewRecord
                    ? `${categoryLabel(baselineNextReviewRecord.category)} · ${baselineNextReviewRecord.country || "Market"} · ${baselineNextReviewRecord.questionId}`
                    : "No row selected"}
                </strong>
                <p>{baselineNextReviewRecord?.question || "Run or refresh Business Validation before scoring."}</p>
                <small>{baselineNextReviewReason}</small>
              </div>
            </div>
            <div className="astrbot-baseline-action-metrics" aria-label="Baseline scoring shortcuts">
              <article>
                <span>Sources</span>
                <strong>{baselineActionPlan.sourceLabel}</strong>
                <small>Manual/GPT only counts</small>
              </article>
              <article>
                <span>Score ready</span>
                <strong>{businessReviewWorkbench.scoreReadyUnscoredCount}</strong>
                <small>prioritized for baseline</small>
              </article>
              <article>
                <span>Repair first</span>
                <strong>{businessReviewWorkbench.repairFirstUnscoredCount}</strong>
                <small>data/source gate</small>
              </article>
              <article>
                <span>Decision first</span>
                <strong>{businessReviewWorkbench.decisionNeededCount}</strong>
                  <small>{baselineActionPlan.decisionLabel}</small>
              </article>
              <article>
                <span>Draft help</span>
                <strong>{businessReviewWorkbench.draftReadyUnscoredCount}</strong>
                <small>{baselineActionPlan.draftLabel}</small>
              </article>
              <article>
                <span>Judge</span>
                <strong>{baselineActionPlan.judgeLabel}</strong>
                <small>{judgePreflight?.provider.model ?? "model n/a"}</small>
              </article>
            </div>
            <div className="astrbot-baseline-action-buttons">
              <button
                type="button"
                className="astrbot-primary-action"
                onClick={openBaselineActionReview}
                disabled={running !== null || !baselineActionWorkbench.nextComparisonId}
              >
                {baselineActionPlan.reviewButtonLabel}
              </button>
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => void copyBusinessScoringSheet(baselineActionRecords.length > 0 ? baselineActionRecords : businessRecords)}
                disabled={running !== null || businessRecords.length === 0}
                title="Copy the recommended baseline queue as TSV for spreadsheet scoring. This does not save scores."
              >
                Copy TSV for scoring
              </button>
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => void copyJudgeEnvTemplate()}
                disabled={running !== null}
              >
                Copy judge setup
              </button>
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => void copyBusinessReadinessHandoff()}
                disabled={running !== null}
                title="Copy the current engineering/evidence/baseline gate state and next review action as Markdown."
              >
                Copy readiness handoff
              </button>
              <button
                type="button"
                className="astrbot-primary-action"
                onClick={() => void runJudgeExistingBaseline(Math.max(1, Math.min(30, businessReviewWorkbench.scoreReadyUnscoredCount || 14)), true)}
                disabled={running !== null || !judgePreflight?.ready || businessReviewWorkbench.scoreReadyUnscoredCount === 0}
                title={judgePreflight?.ready ? "Judge score-ready existing records only, skipping repair-first data/source gaps" : "Judge provider must be ready before scoring score-ready baseline records"}
              >
                {running === "judge-baseline-score-ready" ? "Judging…" : `Judge Score-ready (${businessReviewWorkbench.scoreReadyUnscoredCount})`}
              </button>
              {readinessHandoffCopyState ? <span className="astrbot-review-copy-state">{readinessHandoffCopyState}</span> : null}
            </div>
          </section>

          <details className="astrbot-judge-action-panel" aria-label="Judge baseline actions">
            <summary>
              <span>Judge setup details</span>
              <strong>{judgePreflight?.ready ? "GPT judge ready" : "GPT judge not ready"}</strong>
              <small>{judgePreflight?.reason ?? "Judge preflight has not loaded."}</small>
            </summary>
            <div className="astrbot-judge-action-copy">
              <span>Judge Setup</span>
              <strong>{judgePreflight?.ready ? "GPT judge can score baseline records" : "GPT judge baseline is not ready"}</strong>
              <p>{judgePreflight?.reason ?? "Judge preflight has not loaded."}</p>
              <p className="astrbot-judge-boundary-note">
                DPV4/DeepSeek is the runtime answer provider. The judge key is a separate evaluator for side-by-side
                baseline scoring only; it does not change user-facing /astrbot answers.
              </p>
              <div className="astrbot-judge-env-list" aria-label="Judge environment requirements">
                <code>APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED=true</code>
                <code>APP_ASTRBOT_JUDGE_KEY_ENV={judgePreflight?.provider.keySource ?? "OPENAI_API_KEY"}</code>
                <code>{judgePreflight?.provider.keySource ?? "OPENAI_API_KEY"}=configured</code>
                <code>APP_ASTRBOT_JUDGE_MODEL={judgePreflight?.provider.model ?? "gpt-5.5"}</code>
              </div>
              {referenceJudgePaths.length > 0 ? (
                <div className="astrbot-reference-judge-grid" aria-label="Reference judge paths">
                  {referenceJudgePaths.map(path => (
                    <article key={path.id} className={referenceJudgePathTone(path)}>
                      <span>{path.label}</span>
                      <strong>{readableStatusLabel(path.status)} · {readableStatusLabel(path.readinessStatus)}</strong>
                      <small>
                        {path.model || "model n/a"}
                        {" · "}
                        {path.keySource || "key source n/a"}
                        {path.active ? " · active" : ""}
                      </small>
                      <div className="astrbot-reference-judge-envs" aria-label={`${path.label} env hooks`}>
                        <code>{path.env.model}</code>
                        <code>{path.env.apiBase}</code>
                        <code>{path.env.keySource}</code>
                      </div>
                      <p>{path.nextAction}</p>
                    </article>
                  ))}
                </div>
              ) : null}
              <details className="astrbot-judge-template-details">
                <summary>Judge .env template</summary>
                <pre>{buildJudgeEnvTemplate(judgePreflight)}</pre>
              </details>
            </div>
            <div className="astrbot-judge-action-buttons">
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => void copyJudgeEnvTemplate()}
                disabled={running !== null}
              >
                Copy judge .env
              </button>
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => void loadJudgePreflight()}
                disabled={running !== null}
              >
                Refresh Judge
              </button>
              <button
                type="button"
                className="astrbot-primary-action"
                onClick={() => void runJudgeExistingBaseline(Math.max(1, Math.min(30, businessReviewWorkbench.scoreReadyUnscoredCount || 14)), true)}
                disabled={running !== null || !judgePreflight?.ready || businessReviewWorkbench.scoreReadyUnscoredCount === 0}
                title={judgePreflight?.ready ? "Judge score-ready existing records only, skipping repair-first data/source gaps" : "Judge provider must be ready before scoring existing baseline records"}
              >
                {running === "judge-baseline-score-ready" ? "Judging…" : `Judge Score-ready (${businessReviewWorkbench.scoreReadyUnscoredCount})`}
              </button>
              <button
                type="button"
                className="astrbot-primary-action"
                onClick={() => void runJudgeExistingBaseline(2)}
                disabled={running !== null || !judgePreflight?.ready}
                title={judgePreflight?.ready ? "Judge 2 existing business validation records without rerunning answers" : "Judge provider must be ready before scoring existing baseline records"}
              >
                {running === "judge-baseline-smoke" ? "Judging…" : "Run 2 judge smoke"}
              </button>
              <button
                type="button"
                className="astrbot-primary-action"
                onClick={() => void runJudgeExistingBaseline(30)}
                disabled={running !== null || !judgePreflight?.ready}
                title={judgePreflight?.ready ? "Judge up to 30 existing business validation records without rerunning answers" : "Judge provider must be ready before scoring existing baseline records"}
              >
                {running === "judge-baseline-full" ? "Judging…" : "Judge 30 Existing"}
              </button>
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={openManualScoreQueue}
                disabled={running !== null}
              >
                Manual Score Queue
              </button>
              {judgeTemplateCopyState ? <span>{judgeTemplateCopyState}</span> : null}
            </div>
          </details>

          <section className="astrbot-codex-triage-panel" aria-label="Codex draft triage">
            <div className="astrbot-codex-triage-copy">
              <span>Codex Draft Triage</span>
              <strong>
                {codexDraftTriage.draftCount}/{codexDraftTriage.totalRecords || 0} review drafts available
              </strong>
              <p>
                Draft only: use this to prioritize manual review. It does not change human scores,
                win rate, or replacement readiness until a reviewer opens a row and saves a score.
              </p>
            </div>
            <div className="astrbot-codex-triage-metrics">
              <article>
                <span>Coverage</span>
                <strong>{formatScore(codexDraftTriage.coverage)}</strong>
                <small>{codexDraftTriage.readyDraftCount} ready to inspect</small>
              </article>
              <article>
                <span>Draft avg</span>
                <strong>
                  {formatManualScore(codexDraftTriage.avgAstrBotScore)}
                  {" / "}
                  {formatManualScore(codexDraftTriage.avgCountryCopilotScore)}
                </strong>
                <small>AstrBot / Copilot</small>
              </article>
              <article>
                <span>Suggested wins</span>
                <strong>{codexDraftTriage.suggestedWins.astrbot} / {codexDraftTriage.suggestedWins.countryCopilot}</strong>
                <small>{countBreakdownText(codexDraftTriage.suggestedWins)}</small>
              </article>
              <article>
                <span>UI status</span>
                <strong>{countBreakdownText(codexDraftTriage.uiStatuses)}</strong>
                <small>Latest {formatLocalDateTime(codexDraftTriage.latestAt)}</small>
              </article>
              <article>
                <span>Hotspots</span>
                <strong>{codexDraftTriage.gapClusters.length}</strong>
                <small>
                  Draft tie / thin evidence / research gaps ·
                  {codexDraftTriage.tieCount} ties
                  {" · "}
                  {codexDraftTriage.thinEvidenceCount} thin evidence
                  {" · "}
                  {codexDraftTriage.researchGapCount} research gaps
                </small>
              </article>
            </div>
            {codexDraftTriage.gapClusters.length > 0 ? (
              <div className="astrbot-codex-hotspot-list" aria-label="Codex draft improvement hotspots">
                {codexDraftTriage.gapClusters.slice(0, 4).map(cluster => (
                  <article key={cluster.category}>
                    <span>{cluster.priority} · {categoryLabel(cluster.category)}</span>
                    <strong>
                      {formatManualScore(cluster.avgAstrBotScore)}
                      {" / "}
                      {formatManualScore(cluster.avgCountryCopilotScore)}
                    </strong>
                    <p>{cluster.reason}</p>
                    <small>Examples: {cluster.exampleQuestionIds.join(", ")}</small>
                    <button
                      type="button"
                      className="astrbot-chip-button"
                      onClick={() => openHotspotReviewQueue(cluster.category)}
                      disabled={running !== null}
                    >
                      Review hotspot
                    </button>
                  </article>
                ))}
              </div>
            ) : null}
            <div className="astrbot-codex-triage-actions">
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={() => void loadCodexReviewNotes()}
                disabled={running !== null}
              >
                Refresh Draft Notes
              </button>
              <button
                type="button"
                className="astrbot-chip-button"
                onClick={openManualScoreQueue}
                disabled={running !== null}
              >
                Review Draft Rows
              </button>
            </div>
          </section>

          {!businessReadiness.evidenceReady && blockingReadinessItems.length > 0 ? (
            <section className="astrbot-blocking-gap-panel" aria-label="Blocking readiness gaps">
              <div className="astrbot-blocking-gap-head">
                <div>
                  <span>Readiness Blocker</span>
                  <strong>{blockingReadinessItems.length} blocking data gap{blockingReadinessItems.length === 1 ? "" : "s"}</strong>
                  <p>These are the items preventing /astrbot from being treated as replacement-ready. Enhancement tasks stay in the full repair queue below.</p>
                </div>
                <button
                  type="button"
                  className="astrbot-chip-button"
                  onClick={() => void copyEvidenceRepairPlan(blockingReadinessItems.map(blocker => (
                    evidenceRepairQueue.find(item => item.questionId === blocker.questionId)
                  )).filter((item): item is EvidenceRepairItem => Boolean(item)))}
                >
                  Copy Blocker Plan
                </button>
              </div>
              <div className="astrbot-blocking-gap-list">
                {blockingReadinessItems.map(item => (
                  <article key={item.questionId}>
                    <header>
                      <span>{item.questionId} · {categoryLabel(item.category)}</span>
                      <strong>{businessDiagnosticLabel(item.primaryGap)}</strong>
                    </header>
                    <p>{item.question}</p>
                    <dl>
                      <div>
                        <dt>Why blocked</dt>
                        <dd>{item.reason}</dd>
                      </div>
                      <div>
                        <dt>Tool</dt>
                        <dd>{item.selectedTool}</dd>
                      </div>
                      <div>
                        <dt>Next action</dt>
                        <dd>{item.action}</dd>
                      </div>
                    </dl>
                  </article>
                ))}
              </div>
            </section>
          ) : null}

          {sourceRepairBacklog.length > 0 ? (
            <section className="astrbot-source-repair-backlog-panel" aria-label="Source repair backlog">
              <div className="astrbot-source-repair-backlog-head">
                <div>
                  <span>Source Repair Backlog</span>
                  <strong>{sourceRepairBacklog.length} grouped source item{sourceRepairBacklog.length === 1 ? "" : "s"}</strong>
                  <p>Batch these missing MSRP, VOC, or policy sources first, then rerun Business Validation to see whether AstrBot gains from real evidence coverage.</p>
                </div>
                <div className="astrbot-source-repair-backlog-actions">
                  <div>
                    <span>{businessReport?.summary.sourceRepairBacklogCount ?? sourceRepairBacklog.length}</span>
                    <small>report items</small>
                  </div>
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => void copySourceRepairBacklogPlan(sourceRepairBacklog)}
                  >
                    Copy Source Backlog
                  </button>
                  {repairPlanCopyState ? <small>{repairPlanCopyState}</small> : null}
                </div>
              </div>
              <div className="astrbot-source-repair-backlog-list">
                {visibleSourceRepairBacklog.map(item => {
                  const searchText = sourceRepairBacklogSearchText(item);
                  const key = `${item.sourceType}-${item.label}-${searchText}`;
                  return (
                    <article key={key}>
                      <header>
                        <span>{item.priority} · {sourceRepairBacklogTypeLabel(item.sourceType)}</span>
                        <strong>{item.label}</strong>
                      </header>
                      <p>{searchText}</p>
                      <div className="astrbot-source-repair-backlog-meta">
                        <span>{item.affectedCount} affected</span>
                        {item.sourceDraftPath || item.relativePath ? <span>{item.sourceDraftPath || item.relativePath}</span> : null}
                        {item.candidateDomain ? <span>{item.candidateDomain}</span> : null}
                        {item.categories.length > 0 ? <span>{item.categories.map(categoryLabel).join(" · ")}</span> : null}
                      </div>
                      {item.questionIds.length > 0 ? (
                        <small>{item.questionIds.slice(0, 4).join(", ")}{item.questionIds.length > 4 ? ` +${item.questionIds.length - 4}` : ""}</small>
                      ) : null}
                      {item.primaryGaps.length > 0 ? (
                        <small>{item.primaryGaps.map(businessDiagnosticLabel).slice(0, 3).join(" · ")}</small>
                      ) : null}
                      {item.recommendedAction ? <p>{item.recommendedAction}</p> : null}
                    </article>
                  );
                })}
              </div>
              {sourceBacklogPlanText ? (
                <details className="astrbot-source-repair-backlog-export">
                  <summary>Source backlog TSV</summary>
                  <textarea
                    readOnly
                    value={sourceBacklogPlanText}
                    aria-label="Source repair backlog TSV"
                    onFocus={event => event.currentTarget.select()}
                  />
                </details>
              ) : null}
              {sourceRepairBacklog.length > visibleSourceRepairBacklog.length ? (
                <p className="astrbot-source-repair-backlog-more">
                  Showing top {visibleSourceRepairBacklog.length}; click Copy Source Backlog to expose the full TSV in this panel.
                </p>
              ) : null}
            </section>
          ) : null}

          {evidenceRepairQueue.length > 0 ? (
            <section className="astrbot-evidence-repair-panel" aria-label="Evidence repair queue">
              <div className="astrbot-evidence-repair-head">
                <div>
                  <span>Evidence Repair Queue</span>
                  <strong>{evidenceRepairQueue.length} evidence task{evidenceRepairQueue.length === 1 ? "" : "s"} found</strong>
                  <p>P0 items block readiness through failure tags; P1 items are evidence hardening before manual review.</p>
                  <p>Why blocked: coverage_diagnostic, current_prices table, or no current price source is missing for the affected answer.</p>
                </div>
                <div className="astrbot-evidence-repair-actions">
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => void copyEvidenceRepairPlan(evidenceRepairQueue)}
                  >
                    Copy Repair Plan
                  </button>
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={openManualScoreQueue}
                    disabled={running !== null}
                  >
                    Open Review Queue
                  </button>
                  {repairPlanCopyState ? <span>{repairPlanCopyState}</span> : null}
                </div>
              </div>
              <div className="astrbot-evidence-repair-overview" aria-label="Evidence repair overview">
                <span className={evidenceRepairOverview.p0Count > 0 ? "is-low" : "is-good"}>
                  <small>Blockers</small>
                  <strong>{evidenceRepairOverview.p0Count}</strong>
                  <em>{evidenceRepairOverview.p1Count} P1 hardening</em>
                </span>
                <span>
                  <small>Source progress</small>
                  <strong>{evidenceRepairOverview.materializedCandidateCount}/{evidenceRepairOverview.sourceCandidateCount}</strong>
                  <em>{evidenceRepairOverview.missingOwnModelSourceCount} own-model gaps</em>
                </span>
                <span>
                  <small>Pricing source work</small>
                  <strong>{evidenceRepairOverview.pricingSourceTaskCount}</strong>
                  <em>{evidenceRepairOverview.taskCount} total tasks</em>
                </span>
                <span>
                  <small>Config / date gaps</small>
                  <strong>{evidenceRepairOverview.configGapTaskCount + evidenceRepairOverview.sourceDateTaskCount}</strong>
                  <em>{evidenceRepairOverview.configGapTaskCount} config · {evidenceRepairOverview.sourceDateTaskCount} date</em>
                </span>
                <span>
                  <small>Owner split</small>
                  <strong>{evidenceRepairOverview.topOwners[0]?.owner ?? "n/a"}</strong>
                  <em>{evidenceRepairOverview.topOwners.map(owner => `${owner.owner} ${owner.count}`).join(" · ") || "no tasks"}</em>
                </span>
              </div>
              <div className="astrbot-evidence-repair-list-toolbar" aria-label="Evidence repair list display">
                <span>{evidenceRepairDisplay.statusText}</span>
                {evidenceRepairDisplay.hiddenCount > 0 ? (
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => setShowAllEvidenceRepairItems(prev => !prev)}
                  >
                    {evidenceRepairDisplay.toggleLabel}
                  </button>
                ) : null}
              </div>
              <div className="astrbot-evidence-repair-list">
                {evidenceRepairDisplay.visibleItems.map(item => {
                  const reasonLines = evidenceRepairReasonLines(item);
                  const sourceDrafts = visibleSourceRepairCandidates(item.sourceRepairCandidates);
                  const repairSummary = item.repairSummary;
                  const sourceSummary = repairSummary?.sourceSummary || sourceRepairSummaryText(item.sourceRepairCandidates);
                  const sourceProgressLabel = isPolicySourceRepairCandidates(item.sourceRepairCandidates) ? "confirmed" : "materialized";
                  const sourceDraftsLabel = isPolicySourceRepairCandidates(item.sourceRepairCandidates) ? "Source candidates" : "Source drafts";
                  return (
                    <article key={item.questionId}>
                      <header>
                        <span>{item.priority} · {categoryLabel(item.category)} · {item.answerStatus}</span>
                        <strong>{item.questionId}</strong>
                      </header>
                      <p>{item.question}</p>
                      <div className="astrbot-evidence-repair-meta">
                        <span>Tool: {item.selectedTool}</span>
                        <span>Tags: {businessDiagnosticListText(item.failureTags)}</span>
                      </div>
                      {repairSummary ? (
                        <div className="astrbot-evidence-repair-summary" aria-label={`Repair summary for ${item.questionId}`}>
                          <span>
                            Primary gap
                            <strong title={item.primaryGap || repairSummary.primaryGap || item.missingEvidence[0]?.name || "n/a"}>
                              {businessDiagnosticLabel(item.primaryGap || repairSummary.primaryGap || item.missingEvidence[0]?.name || "n/a")}
                            </strong>
                          </span>
                          <span>
                            Evidence
                            <strong>
                              {repairSummary.missingEvidenceCount ?? item.missingEvidence.length} missing
                              {" · "}
                              {repairSummary.blockingEvidenceCount ?? 0} blocking
                            </strong>
                          </span>
                          <span>
                            Sources
                            <strong>
                              {repairSummary.materializedCandidateCount ?? item.sourceRepairCandidates?.materializedCandidateCount ?? 0}
                              /
                              {repairSummary.sourceCandidateCount ?? item.sourceRepairCandidates?.candidateCount ?? 0} {sourceProgressLabel}
                            </strong>
                          </span>
                          <span>
                            Own model
                            <strong>{repairSummary.missingOwnModelSource ? "missing source" : "source present or n/a"}</strong>
                          </span>
                          {sourceSummary ? (
                            <span className="is-wide">
                              Source status
                              <strong>{sourceSummary}</strong>
                            </span>
                          ) : null}
                        </div>
                      ) : null}
                      {item.commandHint ? (
                        <p className="astrbot-evidence-repair-command-hint">
                          <strong>Next command</strong>
                          <span>{item.commandHint}</span>
                        </p>
                      ) : null}
                      <div className="astrbot-evidence-repair-chips" aria-label={`Missing evidence for ${item.questionId}`}>
                        {item.missingEvidence.slice(0, 5).map(missing => (
                          <span key={`${item.questionId}-${missing.name}`}>
                            {businessDiagnosticLabel(missing.name)}
                            <small>{businessDiagnosticLabel(missing.impact)}</small>
                          </span>
                        ))}
                      </div>
                      {reasonLines.length > 0 ? (
                        <div className="astrbot-evidence-repair-reasons" aria-label={`Repair reasons for ${item.questionId}`}>
                          <span>Why blocked</span>
                          {reasonLines.map(reason => (
                            <p key={`${item.questionId}-${reason}`}>{reason}</p>
                          ))}
                        </div>
                      ) : null}
                      {sourceDrafts.length > 0 ? (
                        <div className="astrbot-evidence-source-drafts" aria-label={`Source repair candidates for ${item.questionId}`}>
                          <span>
                            {sourceDraftsLabel}
                            {item.sourceRepairCandidates?.dataStatus ? (
                              <small>
                                {evidenceRepairDisplayText(item.sourceRepairCandidates.dataStatus.replace(/_/g, " "))}
                                {item.sourceRepairCandidates.materializedCandidateCount
                                  ? ` · ${item.sourceRepairCandidates.materializedCandidateCount} materialized`
                                  : ""}
                              </small>
                            ) : null}
                          </span>
                          <div>
                            {sourceDrafts.map(candidate => {
                              const label = sourceRepairCandidateLabel(candidate);
                              const description = candidate.currentPriceRows && candidate.currentPriceRows > 0
                                ? `current price materialized · ${candidate.currentPriceRows} rows`
                                : candidate.relativePath || candidate.sourceCode || candidate.draftStatus || "source draft";
                              const key = candidate.sourceCode || `${candidate.brand ?? ""}-${candidate.model ?? ""}-${candidate.sourceUrl ?? ""}`;
                              const content = (
                                <Fragment>
                                  <strong>{label}</strong>
                                  <small>{description}</small>
                                </Fragment>
                              );
                              return candidate.sourceUrl ? (
                                <a key={key} href={candidate.sourceUrl} target="_blank" rel="noreferrer">
                                  {content}
                                </a>
                              ) : (
                                <span key={key}>
                                  {content}
                                </span>
                              );
                            })}
                          </div>
                        </div>
                      ) : null}
                      {item.repairTasks.length > 0 ? (
                        <div className="astrbot-evidence-repair-tasks" aria-label={`Repair tasks for ${item.questionId}`}>
                          <span>Task pack</span>
                          <div>
                            {item.repairTasks.slice(0, 4).map(task => (
                              <div key={task.taskId} className="astrbot-evidence-repair-task">
                                <header>
                                  <strong>{task.title}</strong>
                                  <small>{task.priority} · {task.owner}</small>
                                </header>
                                {task.output ? <p>{task.output}</p> : null}
                                {task.commandHint ? (
                                  <p className="astrbot-evidence-repair-task-hint">
                                    Hint: {task.commandHint}
                                  </p>
                                ) : null}
                                {(task.sourceCandidates ?? []).length > 0 ? (
                                  <small>{(task.sourceCandidates ?? []).slice(0, 3).join(", ")}</small>
                                ) : null}
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                      <p className="astrbot-evidence-repair-action">
                        {item.repairAction
                          ?? item.recommendedActions[0]?.action
                          ?? "补齐当前 MSRP、竞品价格走廊和来源可追溯证据后再重跑 Business Validation。"}
                      </p>
                    </article>
                  );
                })}
              </div>
            </section>
          ) : null}

          {businessMode === "review" ? (
            <section className="astrbot-business-review-queue" aria-label="Business review queue">
              <div className="astrbot-business-review-head">
                <div>
                  <span>Review Queue</span>
                  <strong>{businessReport?.total ?? 0} business comparisons</strong>
                  <p>Read both answers, then click one 1-5 total for AstrBot and one for CountryCopilot. That total fills every dimension; Advanced is only for exceptions.</p>
                </div>
                <div>
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => void loadBusinessReport(businessCategory || undefined)}
                    disabled={running !== null}
                  >
                    Refresh Queue
                  </button>
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => setBusinessMode("questions")}
                    disabled={running !== null}
                  >
                    Run More
                  </button>
                </div>
              </div>
              <section className="astrbot-review-workbench" aria-label="Business review workbench">
                <div className="astrbot-review-workbench-copy">
                  <span>Review Workbench</span>
                  <strong>
                    Need {businessReviewWorkbench.neededForReviewTarget} more saved score{businessReviewWorkbench.neededForReviewTarget === 1 ? "" : "s"} for review coverage
                  </strong>
                  <p>
                    {businessReviewWorkbench.scoreReadyUnscoredCount} unscored rows are score-ready;
                    {businessReviewWorkbench.repairFirstUnscoredCount} should go through evidence repair first.
                    Replacement baseline is manual/GPT only.
                  </p>
                </div>
                <div className="astrbot-review-workbench-metrics">
                  <article>
                    <span>Visible</span>
                    <strong>{businessReviewWorkbench.visibleRecords}/{businessReviewWorkbench.totalRecords}</strong>
                    <small>{businessReviewFilter.replace(/_/g, " ")}</small>
                  </article>
                  <article>
                    <span>Baseline saved</span>
                    <strong>{businessReviewWorkbench.savedCount}</strong>
                    <small>{countBreakdownText(businessReport?.summary.baselineSourceCounts ?? businessReport?.summary.humanScoreSourceCounts ?? {}, "sources none")}</small>
                  </article>
                  <article>
                    <span>Score ready</span>
                    <strong>{businessReviewWorkbench.scoreReadyUnscoredCount}</strong>
                    <small>no obvious data repair gate</small>
                  </article>
                  <article>
                    <span>Repair first</span>
                    <strong>{businessReviewWorkbench.repairFirstUnscoredCount}</strong>
                    <small>fix evidence before baseline</small>
                  </article>
                  <article>
                    <span>Needs decision</span>
                    <strong>{businessReviewWorkbench.decisionNeededCount}</strong>
                    <small>Decision first · Tie / thin / research first</small>
                  </article>
                  <article>
                    <span>Draft ready</span>
                    <strong>{businessReviewWorkbench.draftReadyUnscoredCount}</strong>
                    <small>can prefill</small>
                  </article>
                </div>
                <div className="astrbot-review-workbench-actions">
                  <button
                    type="button"
                    className="astrbot-primary-action"
                    onClick={openNextBusinessReview}
                    disabled={running !== null || !businessReviewWorkbench.nextComparisonId}
                  >
                    Open next draft
                  </button>
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => void copyBusinessScoringSheet(businessReviewRecords.length > 0 ? businessReviewRecords : businessRecords)}
                    disabled={running !== null || businessRecords.length === 0}
                    title="Copy the current review queue as TSV for spreadsheet scoring. This does not save scores."
                  >
                    Copy scoring sheet
                  </button>
                  {scoreSheetCopyState ? <span className="astrbot-review-copy-state">{scoreSheetCopyState}</span> : null}
                  <div className="astrbot-review-filter-tabs" role="group" aria-label="Business review queue filter">
                    {([
                      ["score_ready", "Score ready"],
                      ["repair_first", "Repair first"],
                      ["needs_decision", "Needs decision"],
                      ["draft_ready", "Draft ready"],
                      ["needs_score", "Needs score"],
                      ["saved", "Saved"],
                      ["all", "All"],
                    ] as Array<[BusinessReviewFilter, string]>).map(([value, label]) => (
                      <button
                        key={value}
                        type="button"
                        className={businessReviewFilter === value ? "is-active" : ""}
                        onClick={() => setBusinessReviewFilter(value)}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                </div>
                {scoreSheetText ? (
                  <textarea
                    className="astrbot-review-sheet-fallback"
                    readOnly
                    value={scoreSheetText}
                    aria-label="Business scoring sheet TSV"
                    title="TSV scoring sheet. Select all and copy if browser clipboard is unavailable."
                  />
                ) : null}
                <details className="astrbot-review-sheet-import">
                  <summary>Import filled sheet draft</summary>
                  <p>Paste the filled TSV here to prefill local review drafts. Nothing is saved until each row is reviewed and saved.</p>
                  <textarea
                    value={scoreSheetDraftText}
                    onChange={event => setScoreSheetDraftText(event.target.value)}
                    placeholder="Paste TSV with question_id, astrbot_total_1_to_5, copilot_total_1_to_5, winner, notes, failure_tags..."
                    aria-label="Paste filled business scoring sheet"
                  />
                  <div>
                    <button
                      type="button"
                      className="astrbot-chip-button"
                      onClick={() => void loadLatestCodexScoringSheetDraft()}
                      disabled={running !== null}
                      title="Load the latest Codex-generated scoring TSV into this textarea. It still needs human review before saving."
                    >
                      {running === "load-latest-codex-scoring-sheet" ? "Loading…" : "Load latest Codex TSV"}
                    </button>
                    <button
                      type="button"
                      className="astrbot-chip-button is-active"
                      onClick={() => applyBusinessScoringSheetDrafts(businessRecords)}
                      disabled={running !== null || businessRecords.length === 0 || !scoreSheetDraftText.trim()}
                    >
                      Apply sheet draft
                    </button>
                    <button
                      type="button"
                      className="astrbot-chip-button"
                      onClick={() => void saveImportedManualScores()}
                      disabled={running !== null || importedManualSaveRecords.length === 0}
                      title="Save only complete rows imported from a human-reviewed TSV as manual scores."
                    >
                      Save imported manual scores ({importedManualSaveRecords.length})
                    </button>
                    {scoreSheetImportState ? <span className="astrbot-review-copy-state">{scoreSheetImportState}</span> : null}
                  </div>
                </details>
                <details className="astrbot-review-reference-import">
                  <summary>Import reference judge JSON</summary>
                  <p>
                    Paste the strict JSON returned from the reference judge packet. This prefills local drafts first; accepted saves are written as <code>llm_judge</code> scores.
                    The imported result stays a local llm_judge draft until the reviewer explicitly saves it.
                  </p>
                  <textarea
                    value={referenceJudgeDraftText}
                    onChange={event => setReferenceJudgeDraftText(event.target.value)}
                    placeholder={'{"records":[{"questionId":"biz-pricing-001","winner":"astrbot","astrbotScores":{"intentAccuracy":5},"countryCopilotScores":{"intentAccuracy":3},"failureTags":[],"notes":"..."}]}'}
                    aria-label="Paste reference judge JSON output"
                  />
                  <div>
                    <button
                      type="button"
                      className="astrbot-chip-button"
                      onClick={() => void loadLatestReferenceJudgePacket()}
                      disabled={running !== null}
                      title="Load and copy the latest reference judge packet. Send it to GPT/Opus/Fable, then paste the returned JSON back here."
                    >
                      {running === "load-latest-reference-judge-packet" ? "Loading…" : "Load latest judge packet"}
                    </button>
                    <button
                      type="button"
                      className="astrbot-chip-button is-active"
                      onClick={() => applyReferenceJudgeScoreDrafts(businessRecords)}
                      disabled={running !== null || businessRecords.length === 0 || !referenceJudgeDraftText.trim()}
                    >
                      Apply judge JSON
                    </button>
                    <button
                      type="button"
                      className="astrbot-chip-button"
                      onClick={() => void saveImportedReferenceJudgeScores()}
                      disabled={running !== null || importedReferenceJudgeSaveRecords.length === 0}
                      title="Save complete accepted reference judge drafts as llm_judge scores."
                    >
                      Save reference judge scores ({importedReferenceJudgeSaveRecords.length})
                    </button>
                    {referenceJudgeImportState ? <span className="astrbot-review-copy-state">{referenceJudgeImportState}</span> : null}
                  </div>
                </details>
                <details className="astrbot-review-audit-actions">
                  <summary>Audit-only: Save Codex drafts</summary>
                  <p>
                    This writes Codex draft notes as <code>codex_review</code> evidence only. It is useful for traceability,
                    but it does not count toward the manual/GPT replacement baseline.
                  </p>
                  <button
                    type="button"
                    className="astrbot-chip-button"
                    onClick={() => void acceptCodexDraftsAsReviewSource()}
                    disabled={running !== null || codexDraftAcceptanceRecords.length === 0}
                    title={codexDraftAcceptanceRecords.length > 0
                      ? `Save ${codexDraftAcceptanceRecords.length} complete Codex drafts with audit notes as codex_review source`
                      : "No complete Codex drafts are ready to save"}
                  >
                    {running === "accept-codex-drafts"
                      ? "Saving..."
                      : codexDraftAcceptanceRecords.length > 0
                        ? `Save Codex drafts (${codexDraftAcceptanceRecords.length})`
                        : "Save Codex drafts"}
                  </button>
                </details>
              </section>
              <div className="astrbot-review-card-list">
                {businessRecords.length === 0 ? (
                  <div className="astrbot-empty-action-panel">
                    <div>
                      <span>No comparison records loaded</span>
                      <strong>Run Business Validation to show side-by-side answers here.</strong>
                      <p>
                        Codex draft triage can exist without live comparison rows. Run the 30-question pack from
                        this AstrBot worktree, then this queue will show AstrBot and CountryCopilot answers for scoring.
                      </p>
                    </div>
                    <div>
                      <button
                        type="button"
                        className="astrbot-primary-action"
                        onClick={() => void runBusinessAll()}
                        disabled={running !== null}
                      >
                        {running === "business-all" ? "Running…" : "Run All 30"}
                      </button>
                      <button
                        type="button"
                        className="astrbot-chip-button"
                        onClick={() => setBusinessMode("questions")}
                        disabled={running !== null}
                      >
                        Questions / Run
                      </button>
                      <button
                        type="button"
                        className="astrbot-chip-button"
                        onClick={() => void loadBusinessReport(businessCategory || undefined)}
                        disabled={running !== null}
                      >
                        Refresh report
                      </button>
                    </div>
                  </div>
                ) : businessReviewRecords.length === 0 ? (
                  <div className="astrbot-empty-action-panel">
                    <div>
                      <span>No rows in this filter</span>
                      <strong>{businessReviewFilter.replace(/_/g, " ")} has no matching comparison rows.</strong>
                      <p>Switch back to All to inspect answers, or run more questions if the category is empty.</p>
                    </div>
                    <div>
                      <button
                        type="button"
                        className="astrbot-primary-action"
                        onClick={() => setBusinessReviewFilter("all")}
                        disabled={running !== null}
                      >
                        Show all rows
                      </button>
                      <button
                        type="button"
                        className="astrbot-chip-button"
                        onClick={() => setBusinessMode("questions")}
                        disabled={running !== null}
                      >
                        Run more
                      </button>
                    </div>
                  </div>
                ) : businessReviewRecords.map(record => {
                      const isExpanded = expandedComparisonId === record.comparisonId;
                      const scoring = readScoringDraft(record);
                      const dimensions = scoreDimensionsForRecord(record);
                      const astrbotCompleted = completedScoreCount(dimensions, scoring.astrbotScores);
                      const countryCompleted = completedScoreCount(dimensions, scoring.countryCopilotScores);
                      const astrbotDraftAverage = completeAverage(dimensions, scoring.astrbotScores);
                      const countryDraftAverage = completeAverage(dimensions, scoring.countryCopilotScores);
                      const draftComplete = canSaveBusinessScore(astrbotCompleted, countryCompleted, dimensions.length);
                      const draftWinner = effectiveWinner(scoring, astrbotDraftAverage, countryDraftAverage, draftComplete);
                      const failureTags = stringList(record.failureTags);

                      return (
                        <article
                          key={`business-${record.comparisonId}`}
                          className={isExpanded ? "astrbot-review-card is-expanded" : "astrbot-review-card"}
                          data-comparison-id={record.comparisonId}
                        >
                          <div className="astrbot-review-card-summary">
                            <div className="astrbot-review-card-question" title={record.question}>
                              <span>{categoryLabel(record.category)} · {record.country || "Market"}</span>
                              <strong>{record.question}</strong>
                              {failureTags.length > 0 ? (
                                <span className="astrbot-failure-tags">
                                  {failureTags.slice(0, 3).map(tag => <em key={tag} title={tag}>{businessDiagnosticLabel(tag)}</em>)}
                                </span>
                              ) : null}
                            </div>
                            <div className="astrbot-review-card-metrics" aria-label={`Review summary for ${record.questionId}`}>
                              <div className="astrbot-side-cell">
                                <span>AstrBot</span>
                                <strong className={statusTone(record.astrbot?.status, record.astrbot?.error)}>
                                  {record.astrbot?.selectedTool || record.astrbot?.status || "—"}
                                </strong>
                                <small>{formatOptionalScore(record.astrbot?.scores?.composite)} · {record.astrbot?.retrievalPath || "—"}</small>
                              </div>
                              <div className="astrbot-side-cell">
                                <span>CountryCopilot</span>
                                <strong className={statusTone(record.countryCopilot?.status, record.countryCopilot?.error)}>
                                  {record.countryCopilot?.answerMode || record.countryCopilot?.status || "—"}
                                </strong>
                                <small>{record.countryCopilot?.intentRoute || "—"} · {record.countryCopilot?.sourceCount ?? 0} sources</small>
                              </div>
                              <div className="astrbot-side-cell">
                                <span>Both</span>
                                <strong className={record.comparison?.bothReturned ? "is-good" : "is-low"}>
                                  {record.comparison?.bothReturned ? "Yes" : "No"}
                                </strong>
                                <small>{record.comparison?.errorCount ?? 0} errors</small>
                              </div>
                              <div className={`astrbot-human-summary ${draftComplete ? "is-complete" : "is-incomplete"}`}>
                                <span>Baseline</span>
                                <strong>
                                  {draftComplete
                                    ? `${formatManualScore(astrbotDraftAverage)} / ${formatManualScore(countryDraftAverage)}`
                                    : "Needs totals"}
                                </strong>
                                <small>{draftComplete ? winnerText(draftWinner) : scoreCompletionText(astrbotCompleted, countryCompleted, dimensions.length)}</small>
                              </div>
                            </div>
                            <button
                              type="button"
                              className="astrbot-row-toggle"
                              aria-expanded={isExpanded}
                              onClick={() => setExpandedComparisonId(isExpanded ? null : record.comparisonId)}
                            >
                              {isExpanded ? "Hide review" : "Review"}
                            </button>
                          </div>
                          {isExpanded ? (
                            <div className="astrbot-compare-detail-row">
                              {renderComparisonDetail(record)}
                            </div>
                          ) : null}
                        </article>
                      );
                    })}
              </div>
            </section>
          ) : null}

          {businessMode === "questions" ? (
            <div className="astrbot-agent-actions">
            {Object.keys(BUSINESS_CATEGORY_LABELS).map(cat => (
              <button key={cat} type="button" className="astrbot-chip-button" onClick={() => void runBusinessCategory(cat, 1)} disabled={running !== null}>
                {running === `business-category: ${cat}:1` ? "Running…" : `Run 1 ${BUSINESS_CATEGORY_LABELS[cat]}`}
              </button>
            ))}
            {businessCategory ? (
              <button type="button" className="astrbot-primary-action" onClick={() => void runBusinessCategory(businessCategory, 5)} disabled={running !== null}>
                {running === `business-category: ${businessCategory}:5` ? "Running…" : `Run 5 ${BUSINESS_CATEGORY_LABELS[businessCategory] ?? businessCategory}`}
              </button>
            ) : null}
            <button type="button" className="astrbot-primary-action" onClick={() => void runBusinessAll()} disabled={running !== null}>
              {running === "business-all" ? "Running…" : "Run All 30"}
            </button>
            </div>
          ) : null}

          {businessOutput ? <pre className="astrbot-tool-result">{businessOutput}</pre> : null}

          {businessMode === "questions" ? (
            <div className="astrbot-table-shell">
            <table className="astrbot-table">
              <thead>
                <tr><th>ID</th><th>Category</th><th>Question</th><th>Expected</th><th>Action</th></tr>
              </thead>
              <tbody>
                {(businessQuestions?.items ?? []).map(question => (
                  <tr key={question.id}>
                    <td><code>{question.id}</code></td>
                    <td>{categoryLabel(question.category)}</td>
                    <td>{question.question}</td>
                    <td>
                      <div className="astrbot-side-cell">
                        <strong>{question.expectedIntent ?? "—"}</strong>
                        <span>{question.expectedTools.join(", ")}</span>
                      </div>
                    </td>
                    <td>
                      <button type="button" className="astrbot-chip-button" onClick={() => void runBusinessQuestion(question.id)} disabled={running !== null}>
                        {running === `business-question: ${question.id}` ? "Running…" : "Run Compare"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            </div>
          ) : null}

          {businessMode === "calibration" ? (
            <section className="astrbot-calibration-panel" aria-label="Judge calibration">
              <div className="astrbot-calibration-toolbar">
                <div>
                  <strong>Judge Calibration</strong>
                  <span>
                    GPT judged {calibration?.gptJudgedCount ?? 0} · human reviewed {calibration?.humanReviewedCount ?? 0} · mismatches {calibration?.mismatchCount ?? 0}
                  </span>
                </div>
                <label>
                  <input
                    type="checkbox"
                    checked={calibrationOnlyMismatches}
                    onChange={event => setCalibrationOnlyMismatches(event.target.checked)}
                  />
                  <span>Mismatch only</span>
                </label>
              </div>
              <div className="astrbot-agent-card-grid">
                <div className="astrbot-agent-card">
                  <span>Strict Agreement</span>
                  <strong>{Math.round((calibration?.agreementRate ?? 0) * 100)}%</strong>
                </div>
                <div className="astrbot-agent-card">
                  <span>Weighted Agreement</span>
                  <strong>{Math.round((calibration?.weightedAgreementRate ?? 0) * 100)}%</strong>
                </div>
                <div className="astrbot-agent-card">
                  <span>Match / Partial / Mismatch</span>
                  <strong>{calibration?.matchCount ?? 0} / {calibration?.partialCount ?? 0} / {calibration?.mismatchCount ?? 0}</strong>
                </div>
              </div>
              <div className="astrbot-table-shell">
                <table className="astrbot-table">
                  <thead>
                    <tr>
                      <th>Question</th>
                      <th>Category</th>
                      <th>GPT</th>
                      <th>Human</th>
                      <th>Agreement</th>
                      <th>Tags</th>
                      <th>Action</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleCalibrationItems.length === 0 ? (
                      <tr><td colSpan={7} className="astrbot-table-empty">No GPT-judged calibration records yet.</td></tr>
                    ) : visibleCalibrationItems.map(item => (
                      <tr key={`${item.questionId}-${item.gptJudgeWinner}-${item.humanWinner || "pending"}`}>
                        <td title={item.question}>{truncateText(item.question, 70)}</td>
                        <td>{categoryLabel(item.category)}</td>
                        <td>{winnerText(item.gptJudgeWinner)}</td>
                        <td>{item.humanWinner ? winnerText(item.humanWinner) : "Needs review"}</td>
                        <td className={agreementTone(item.agreementStatus)}><strong>{item.agreementStatus}</strong></td>
                        <td>{[...item.gptFailureTags, ...item.humanFailureTags].slice(0, 3).join(", ") || "—"}</td>
                        <td>
                          <button
                            type="button"
                            className="astrbot-chip-button"
                            onClick={() => {
                              setTab("compare");
                              setCompareCategory(item.category);
                              void loadSideBySideResults(item.category);
                            }}
                          >
                            Review
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {recommendedNextActions.length > 0 ? (
                <div className="astrbot-recommendation-grid">
                  {recommendedNextActions.slice(0, 6).map(action => (
                    <article key={`${action.tag}-${action.module}`}>
                      <span>{action.priority} · {action.tag} · {action.count}</span>
                      <strong>{action.module}</strong>
                      <p>{action.recommendation}</p>
                    </article>
                  ))}
                </div>
              ) : null}
            </section>
          ) : null}

          <div className="astrbot-business-report-grid">
            <section>
              <h4>Business Report</h4>
              <pre className="astrbot-tool-result">
                {businessReport?.markdown
                  ? businessDiagnosticDisplayText(businessReport.markdown)
                  : "No business report yet. Run a business comparison or refresh the report."}
              </pre>
            </section>
            <section>
              <h4>Scoring Dimensions</h4>
              <div className="astrbot-score-dimension-list">
                {(businessQuestions?.scoreDimensions ?? []).map(dimension => (
                  <span key={dimension.key}>{dimension.label}</span>
                ))}
              </div>
            </section>
          </div>
        </section>
      ) : null}
    </div>
  );
}
