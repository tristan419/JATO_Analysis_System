import { chromium } from "playwright";
import { mkdir, writeFile, appendFile, readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SCORE_DIMENSIONS = [
  "intentAccuracy",
  "toolSelection",
  "grounding",
  "pmInsight",
  "actionability",
  "artifactQuality",
  "followUpValue",
  "presentationReadiness",
];
const SCORE_RUBRIC = [
  { key: "intentAccuracy", label: "意图识别是否准确", anchor: "1 wrong intent, 3 partly right, 5 exact business intent." },
  { key: "toolSelection", label: "工具选择是否正确", anchor: "1 wrong/no tools, 3 partial tool use, 5 all needed evidence tools." },
  { key: "grounding", label: "数字是否可信", anchor: "1 unsupported numbers, 3 partial sources, 5 traceable evidence and caveats." },
  { key: "pmInsight", label: "是否有产品经理视角", anchor: "1 generic text, 3 some insight, 5 clear automotive PM judgement." },
  { key: "actionability", label: "是否能转成业务动作", anchor: "1 no action, 3 broad next steps, 5 concrete executable actions." },
  { key: "artifactQuality", label: "图表/表格/证据产物是否有用", anchor: "1 absent/useless, 3 usable but thin, 5 directly reviewable artifact." },
  { key: "followUpValue", label: "follow-up 是否有价值", anchor: "1 generic follow-up, 3 related, 5 pushes next analysis path." },
  { key: "presentationReadiness", label: "表达是否适合汇报", anchor: "1 not reportable, 3 needs rewrite, 5 can go into a deck." },
];
const FAILURE_TAXONOMY = [
  "intent_wrong",
  "tool_missing",
  "evidence_missing",
  "answer_too_conservative",
  "answer_too_generic",
  "chart_not_useful",
  "table_not_readable",
  "pm_insight_weak",
  "followup_low_value",
  "presentation_not_ready",
  "hallucination_risk",
];

const DEFAULT_LIMIT = 5;
const DATA_GAP_FAILURE_TAGS = new Set(["evidence_missing"]);
const INTERNAL_REVIEW_MARKERS = [
  "Use this source",
  "Next:",
  "partially_aligned",
  "evidenceRefs",
  "evidenceRef",
  "置信度 high",
  "confidence high",
  "这题需要先给业务立场",
];
const TOOL_COVERAGE_ALIASES = {
  query_country_snapshot: [
    "query_country_snapshot",
    "build_market_chart",
    "analyze_market_dynamics",
    "analyze_model_performance",
    "query_with_filters",
  ],
  external_research: [
    "external_research",
    "search_market_news",
    "read_web_page",
    "browser_snapshot",
    "pageindex_search_documents",
    "minirag_query_graph",
  ],
  search_market_news: [
    "search_market_news",
    "external_research",
    "pageindex_search_documents",
    "read_web_page",
    "browser_snapshot",
  ],
  pageindex_search_documents: [
    "pageindex_search_documents",
    "external_research",
    "search_market_news",
    "read_web_page",
  ],
  build_market_chart: ["build_market_chart"],
  query_msrp_pricing: ["query_msrp_pricing"],
  compare_competitive_set: ["compare_competitive_set", "analyze_model_performance"],
  compare_vehicle_variants: ["compare_vehicle_variants"],
  query_with_filters: ["query_with_filters", "query_country_snapshot"],
};

const REFERENCE_MODEL_PATHS = [
  {
    label: "GPT5.5 / GPT Judge",
    status: "implemented",
    readinessStatus: "not_checked",
    role: "Scalable Judge / Teacher Loop for side-by-side business scoring.",
    evidence: "Independent judge provider via APP_ASTRBOT_SIDE_BY_SIDE_LLM_JUDGE_ENABLED, APP_ASTRBOT_JUDGE_MODEL, and APP_ASTRBOT_JUDGE_API_BASE.",
    nextAction: "Configure a valid judge key, run a 2-record smoke, then generate a 30-record GPT judged baseline.",
  },
  {
    label: "Opus 4.8",
    status: "reference_missing",
    readinessStatus: "not_configured",
    role: "External implementation path requested by user, but no local spec text was found in repo docs or Codex attachments.",
    evidence: "No local rg hit for opus4.8 / opus 4.8.",
    nextAction: "Provide the Opus 4.8 path/spec or map it to the existing judge-provider interface before claiming alignment.",
  },
  {
    label: "Fable 5",
    status: "reference_missing",
    readinessStatus: "not_configured",
    role: "External implementation path requested by user, but no local spec text was found in repo docs or Codex attachments.",
    evidence: "No local rg hit for fable5 / fable 5.",
    nextAction: "Provide the Fable 5 path/spec or map it to the existing judge-provider interface before claiming alignment.",
  },
];

function parseArgs(argv) {
  const result = {
    limit: DEFAULT_LIMIT,
    baseUrl: process.env.ASTRBOT_REVIEW_BASE_URL || "http://127.0.0.1:5176",
    apiBase: process.env.ASTRBOT_REVIEW_API_BASE || process.env.VITE_API_BASE || "http://127.0.0.1:8002/v1",
    headed: false,
  };
  for (const arg of argv) {
    if (arg.startsWith("--limit=")) {
      result.limit = Math.max(1, Math.min(Number(arg.slice("--limit=".length)) || DEFAULT_LIMIT, 30));
    } else if (arg.startsWith("--base-url=")) {
      result.baseUrl = arg.slice("--base-url=".length).replace(/\/+$/, "");
    } else if (arg.startsWith("--api-base=")) {
      result.apiBase = arg.slice("--api-base=".length).replace(/\/+$/, "");
    } else if (arg === "--headed") {
      result.headed = true;
    }
  }
  return result;
}

function nowStamp() {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

function safeName(value) {
  return String(value || "record")
    .trim()
    .replace(/[^0-9A-Za-z\u4e00-\u9fff_-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80) || "record";
}

function mdCell(value) {
  return String(value ?? "")
    .replace(/\|/g, "\\|")
    .replace(/\r?\n/g, " ")
    .trim();
}

function tsvCell(value) {
  return String(value ?? "")
    .replace(/\r?\n/g, " ")
    .replace(/\t/g, " ")
    .trim();
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      "X-User-Name": "codex-review",
      "Accept": "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await response.text()}`);
  }
  return response.json();
}

function numberValue(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}

function objectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function sourceCountsLabel(counts) {
  const entries = Object.entries(objectValue(counts))
    .filter(([, count]) => numberValue(count) > 0)
    .sort(([left], [right]) => left.localeCompare(right));
  if (entries.length === 0) return "sources none";
  return entries.map(([source, count]) => `${source} ${numberValue(count)}`).join(" · ");
}

function filledScoringSheetSmokeRow(sheet) {
  const lines = String(sheet || "").split(/\r?\n/).filter(line => line.trim());
  if (lines.length < 2) return "";
  const headers = lines[0].split("\t").map(header => header.trim());
  const row = lines[1].split("\t");
  const setCell = (name, value) => {
    const index = headers.indexOf(name);
    if (index >= 0) row[index] = value;
  };
  setCell("astrbot_total_1_to_5", "5");
  setCell("copilot_total_1_to_5", "3");
  setCell("winner", "astrbot");
  setCell("notes", "Playwright import smoke: AstrBot more actionable.");
  setCell("failure_tags", "pm_insight_weak");
  return [lines[0], row.join("\t")].join("\n");
}

function listValue(value) {
  return Array.isArray(value) ? value.filter(item => typeof item === "string" && item.trim()) : [];
}

function artifactCount(record) {
  const artifacts = record?.astrbot?.visualArtifacts;
  return Array.isArray(artifacts) ? artifacts.length : 0;
}

function followUpCount(record) {
  const followUps = record?.astrbot?.followUps;
  return Array.isArray(followUps) ? followUps.length : 0;
}

function fillScores(score) {
  return Object.fromEntries(SCORE_DIMENSIONS.map(key => [key, score]));
}

function averageScores(scores) {
  const values = SCORE_DIMENSIONS
    .map(key => Number(scores?.[key] || 0))
    .filter(value => Number.isFinite(value) && value > 0);
  if (values.length === 0) return 0;
  return Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(2));
}

function scoreWinner(astrbotAvg, copilotAvg) {
  if (astrbotAvg <= 0 || copilotAvg <= 0) return "unclear";
  // Codex draft scoring is intentionally conservative: it is a self-test signal,
  // not a replacement for human calibration or GPT judge. Small scoring gaps stay tied.
  if (Math.abs(astrbotAvg - copilotAvg) <= 1) return "tie";
  return astrbotAvg > copilotAvg ? "astrbot" : "countryCopilot";
}

function clampScore(value) {
  if (!Number.isFinite(value)) return 1;
  return Math.max(1, Math.min(5, Math.round(value)));
}

function textIncludesAny(text, terms) {
  const normalized = String(text || "").toLowerCase();
  return terms.some(term => normalized.includes(String(term).toLowerCase()));
}

function countTextHits(text, terms) {
  const normalized = String(text || "").toLowerCase();
  return terms.reduce((count, term) => (
    normalized.includes(String(term).toLowerCase()) ? count + 1 : count
  ), 0);
}

function recordText(record, side) {
  const payload = side === "astrbot" ? record?.astrbot : record?.countryCopilot;
  return String(payload?.answerPreview || payload?.error || "");
}

function expectedTools(record) {
  return Array.isArray(record?.expectedTools)
    ? record.expectedTools.filter(tool => typeof tool === "string" && tool.trim())
    : [];
}

function astrbotTools(record) {
  const all = Array.isArray(record?.astrbot?.allRetrievalPaths) ? record.astrbot.allRetrievalPaths : [];
  return [
    record?.astrbot?.selectedTool,
    record?.astrbot?.retrievalPath,
    ...all,
  ].filter(item => typeof item === "string" && item.trim());
}

function normalizedToolName(value) {
  return String(value || "").trim().toLowerCase();
}

function toolAliasSet(value) {
  const normalized = normalizedToolName(value);
  if (!normalized) return new Set();
  return new Set([normalized, ...(TOOL_COVERAGE_ALIASES[normalized] || []).map(normalizedToolName)]);
}

function singleToolMatches(expected, actual) {
  const expectedAliases = toolAliasSet(expected);
  const actualAliases = toolAliasSet(actual);
  if (expectedAliases.size === 0 || actualAliases.size === 0) return false;
  for (const expectedAlias of expectedAliases) {
    for (const actualAlias of actualAliases) {
      if (actualAlias === expectedAlias || actualAlias.includes(expectedAlias) || expectedAlias.includes(actualAlias)) {
        return true;
      }
    }
  }
  return false;
}

function hasToolMatch(expected, actual) {
  if (expected.length === 0 || actual.length === 0) return false;
  return expected.some(tool => actual.some(item => singleToolMatches(tool, item)));
}

function evidenceRefCount(record) {
  return numberValue(record?.astrbot?.evidenceRefCount)
    || numberValue(record?.astrbot?.evidenceCount);
}

function astrbotMissingEvidence(record) {
  return Array.isArray(record?.astrbot?.missingEvidence) ? record.astrbot.missingEvidence : [];
}

function externalResearchGapItems(record) {
  return astrbotMissingEvidence(record).filter(item => {
    const name = String(item?.name || "").toLowerCase();
    return name === "external_research_claims_unavailable"
      || name === "published_date"
      || name === "fresh_external_signal";
  });
}

function businessTermScore(text) {
  return countTextHits(text, [
    "定位",
    "定价",
    "竞品",
    "价格",
    "配置",
    "渠道",
    "用户",
    "场景",
    "风险",
    "action",
    "next action",
    "implication",
    "recommendation",
    "corridor",
    "evidence",
  ]);
}

function sectionScore(text) {
  return countTextHits(text, [
    "核心发现",
    "数据证据",
    "业务建议",
    "next action",
    "key message",
    "evidence",
    "product implication",
    "risk",
  ]);
}

function wordLikeLength(text) {
  return String(text || "").replace(/\s+/g, "").length;
}

function usefulAnswerStructureScore(text) {
  return countTextHits(text, [
    "verdict",
    "why",
    "so what",
    "action",
    "risk",
    "结论",
    "为什么",
    "意味着",
    "建议",
    "风险",
    "下一步",
    "核心发现",
    "业务建议",
    "产品启示",
  ]);
}

function businessSpecificityScore(text) {
  return countTextHits(text, [
    "msrp",
    "月供",
    "leasing",
    "rv",
    "残值",
    "价格走廊",
    "竞品",
    "配置差异",
    "公司车",
    "补贴",
    "政策",
    "suv",
    "hev",
    "phev",
    "bev",
    "j7",
    "o5",
    "o9",
    "rav4",
    "sportage",
    "corolla",
    "ev3",
    "ex30",
  ]);
}

function cappedScore(value, cap) {
  return clampScore(Math.min(value, cap));
}

function draftScoresForRecord(record) {
  const expected = expectedTools(record);
  const actualTools = astrbotTools(record);
  const toolMatched = hasToolMatch(expected, actualTools);
  const astrbotText = recordText(record, "astrbot");
  const copilotText = recordText(record, "countryCopilot");
  const astrbotStatus = String(record?.astrbot?.status || "");
  const copilotStatus = String(record?.countryCopilot?.status || "");
  const refs = evidenceRefCount(record);
  const sourceCount = numberValue(record?.countryCopilot?.sourceCount);
  const artifacts = artifactCount(record);
  const followUps = followUpCount(record);
  const missingEvidence = astrbotMissingEvidence(record);
  const blockingMissing = missingEvidence.some(item => item && String(item.impact || "").includes("blocking"));
  const externalResearchGaps = externalResearchGapItems(record);
  const astrbotTerms = businessTermScore(astrbotText);
  const copilotTerms = businessTermScore(copilotText);
  const astrbotSections = sectionScore(astrbotText);
  const copilotSections = sectionScore(copilotText);
  const copilotTables = numberValue(record?.countryCopilot?.evidenceTableCount);
  const copilotCharts = numberValue(record?.countryCopilot?.chartLinkCount);
  const astrbotLength = wordLikeLength(astrbotText);
  const copilotLength = wordLikeLength(copilotText);
  const astrbotStructure = usefulAnswerStructureScore(astrbotText);
  const copilotStructure = usefulAnswerStructureScore(copilotText);
  const astrbotSpecificity = businessSpecificityScore(astrbotText);
  const copilotSpecificity = businessSpecificityScore(copilotText);
  const thinAstrbotEvidence = refs <= 1 && /pricing|compare|market|policy|configuration|inventory|bom|voc|report/i.test(String(record?.category || ""));
  const thinEvidenceCap = thinAstrbotEvidence ? 3 : 5;
  const shortGenericAstrbot = astrbotLength < 360 && astrbotSpecificity < 4;
  const structureWeakCap = astrbotStructure < 2 ? 3 : 5;

  const astrbotScores = {
    intentAccuracy: clampScore(astrbotStatus === "ok" ? (toolMatched ? 5 : actualTools.length > 0 ? 4 : 3) : 2),
    toolSelection: clampScore(toolMatched ? 5 : actualTools.length >= 2 ? 4 : actualTools.length === 1 ? 3 : 2),
    grounding: cappedScore(
      blockingMissing ? 2 : refs >= 12 ? 5 : refs >= 6 ? 4 : refs >= 2 ? 3 : 2,
      externalResearchGaps.length > 0 ? 3 : 5,
    ),
    pmInsight: cappedScore(
      astrbotTerms >= 8 && astrbotSpecificity >= 6 ? 5 : astrbotTerms >= 5 && astrbotSpecificity >= 4 ? 4 : astrbotTerms >= 3 ? 3 : 2,
      Math.min(thinEvidenceCap, structureWeakCap, shortGenericAstrbot ? 3 : 5),
    ),
    actionability: cappedScore(
      textIncludesAny(astrbotText, ["下一步", "next action", "建议", "action", "补齐", "生成"])
        ? 4 + (Array.isArray(record?.astrbot?.recommendedActions) && record.astrbot.recommendedActions.length > 0 ? 1 : 0)
        : 2,
      Math.min(thinEvidenceCap, shortGenericAstrbot ? 3 : 5),
    ),
    artifactQuality: cappedScore(artifacts >= 3 ? 5 : artifacts === 2 ? 4 : artifacts === 1 ? 3 : 2, thinEvidenceCap),
    followUpValue: cappedScore(followUps >= 4 ? 5 : followUps === 3 ? 4 : followUps > 0 ? 3 : 1, shortGenericAstrbot ? 4 : 5),
    presentationReadiness: cappedScore(
      astrbotSections >= 4 || textIncludesAny(astrbotText, ["ppt", "key message", "report", "汇报"])
        ? 4 + (Array.isArray(record?.astrbot?.reportReadyBullets) && record.astrbot.reportReadyBullets.length > 0 ? 1 : 0)
        : 2,
      Math.min(thinEvidenceCap, structureWeakCap, shortGenericAstrbot ? 3 : 5),
    ),
  };

  const copilotScores = {
    intentAccuracy: clampScore(copilotStatus === "ok" ? 4 : 2),
    toolSelection: clampScore(sourceCount >= 3 ? 4 : sourceCount > 0 ? 3 : 2),
    grounding: clampScore(sourceCount >= 4 ? 4 : sourceCount >= 2 ? 3 : sourceCount > 0 ? 2 : 1),
    pmInsight: clampScore(
      copilotTerms >= 8 && copilotSpecificity >= 6 ? 5 : copilotTerms >= 5 && copilotSpecificity >= 4 ? 4 : copilotTerms >= 3 ? 3 : 2,
    ),
    actionability: clampScore(textIncludesAny(copilotText, ["建议", "动作", "下一步", "策略", "应"]) ? 4 : 2),
    artifactQuality: clampScore(copilotTables + copilotCharts >= 2 ? 4 : copilotTables + copilotCharts === 1 ? 3 : 2),
    followUpValue: 1,
    presentationReadiness: clampScore(copilotSections >= 3 || copilotStructure >= 3 ? 4 : copilotLength >= 800 ? 3 : 2),
  };

  const astrbotAverage = averageScores(astrbotScores);
  const copilotAverage = averageScores(copilotScores);
  const winner = scoreWinner(astrbotAverage, copilotAverage);
  const rationale = [
    toolMatched ? "AstrBot used an expected tool path." : "AstrBot expected-tool match is incomplete.",
    refs > 0 ? `AstrBot has ${refs} evidence refs.` : "AstrBot has no evidence refs in the record.",
    artifacts > 0 ? `AstrBot has ${artifacts} visual artifact(s).` : "AstrBot has no visual artifact in the record.",
    followUps > 0 ? `AstrBot has ${followUps} follow-up(s).` : "AstrBot has no follow-ups.",
    sourceCount > 0 ? `CountryCopilot has ${sourceCount} source(s).` : "CountryCopilot has no source count.",
    blockingMissing ? "AstrBot still has blocking missing evidence." : "No blocking missing evidence is visible.",
    externalResearchGaps.length > 0
      ? `AstrBot has research source gap(s): ${externalResearchGaps.map(item => item?.name).filter(Boolean).slice(0, 3).join(", ")}.`
      : "No external research source gap is visible.",
    thinAstrbotEvidence ? "AstrBot evidence is thin; do not treat artifacts/follow-ups as proof of business quality." : "AstrBot evidence depth is not thin by the current draft heuristic.",
    shortGenericAstrbot ? "AstrBot answer is short/generic by the current draft heuristic." : "AstrBot answer has enough text-specificity for draft review.",
  ];

  return {
    questionId: String(record?.questionId || ""),
    comparisonId: String(record?.comparisonId || ""),
    category: String(record?.category || ""),
    question: String(record?.question || ""),
    source: "codex_score_draft",
    persisted: false,
    winner,
    astrbotAverage,
    countryCopilotAverage: copilotAverage,
    delta: Number((astrbotAverage - copilotAverage).toFixed(2)),
    astrbotScores,
    countryCopilotScores: copilotScores,
    failureTags: listValue(record?.failureTags),
    rationale,
  };
}

function buildDraftScoreBaseline(records) {
  const items = records.map(draftScoresForRecord);
  const scored = items.length;
  const wins = { astrbot: 0, countryCopilot: 0, tie: 0, unclear: 0 };
  const categoryRows = new Map();
  for (const item of items) {
    if (wins[item.winner] !== undefined) wins[item.winner] += 1;
    const row = categoryRows.get(item.category) || {
      category: item.category,
      count: 0,
      astrbotTotal: 0,
      copilotTotal: 0,
      wins: { astrbot: 0, countryCopilot: 0, tie: 0, unclear: 0 },
    };
    row.count += 1;
    row.astrbotTotal += item.astrbotAverage;
    row.copilotTotal += item.countryCopilotAverage;
    row.wins[item.winner] += 1;
    categoryRows.set(item.category, row);
  }
  const categoryLevel = [...categoryRows.values()]
    .sort((left, right) => left.category.localeCompare(right.category))
    .map(row => ({
      category: row.category,
      count: row.count,
      avgAstrBot: Number((row.astrbotTotal / row.count).toFixed(2)),
      avgCountryCopilot: Number((row.copilotTotal / row.count).toFixed(2)),
      wins: row.wins,
    }));
  return {
    source: "codex_score_draft",
    persisted: false,
    warning: "Draft self-review only. It does not replace human calibration or GPT judge baseline.",
    count: scored,
    avgAstrBot: scored ? Number((items.reduce((sum, item) => sum + item.astrbotAverage, 0) / scored).toFixed(2)) : 0,
    avgCountryCopilot: scored ? Number((items.reduce((sum, item) => sum + item.countryCopilotAverage, 0) / scored).toFixed(2)) : 0,
    astrbotWinRate: scored ? Number((wins.astrbot / scored).toFixed(3)) : 0,
    wins,
    categoryLevel,
    items,
  };
}

function isReplacementBaselineScore(record) {
  const scoring = record?.humanScoring;
  if (!scoring || scoring.status !== "scored") return false;
  if (scoring.scoreTotals?.complete === false) return false;
  return ["manual", "llm_judge"].includes(String(scoring.source || ""));
}

function blockingMissingEvidenceCount(record) {
  const missingEvidence = astrbotMissingEvidence(record);
  return missingEvidence.filter(item => String(item?.impact || "") === "blocking").length;
}

function humanConfirmationReasons(record, draft) {
  const reasons = [];
  const tags = listValue(record?.failureTags);
  const refs = evidenceRefCount(record);
  const blockingCount = blockingMissingEvidenceCount(record);
  const researchGaps = externalResearchGapItems(record);

  if (!draft) {
    reasons.push("no Codex draft score available");
  } else {
    if (["tie", "unclear"].includes(draft.winner)) reasons.push(`draft winner ${draft.winner}`);
    if (draft.astrbotAverage > 0 && draft.astrbotAverage < 4) reasons.push(`AstrBot draft avg ${draft.astrbotAverage}`);
    if (draft.countryCopilotAverage > draft.astrbotAverage) {
      reasons.push(`CountryCopilot draft higher by ${(draft.countryCopilotAverage - draft.astrbotAverage).toFixed(2)}`);
    }
  }
  if (tags.length > 0) reasons.push(`failure tags: ${tags.slice(0, 3).join(", ")}`);
  if (blockingCount > 0) reasons.push(`${blockingCount} blocking evidence gap${blockingCount === 1 ? "" : "s"}`);
  if (researchGaps.length > 0) {
    reasons.push(`external research gap: ${researchGaps.map(item => item?.name).filter(Boolean).slice(0, 3).join(", ")}`);
  }
  if (record?.astrbot && refs <= 1) reasons.push(`thin AstrBot evidence (${refs} refs)`);

  return reasons.length > 0 ? reasons : ["unscored replacement-baseline row"];
}

function humanConfirmationPriority(reasons) {
  if (reasons.some(reason => /failure tags|blocking evidence|CountryCopilot draft higher/i.test(reason))) return "P0";
  if (reasons.some(reason => /tie|unclear|AstrBot draft avg|thin AstrBot evidence|external research gap/i.test(reason))) return "P1";
  return "P2";
}

function buildHumanConfirmationQueue(records, draftScoreBaseline) {
  const draftByQuestionId = new Map(draftScoreBaseline.items.map(item => [item.questionId, item]));
  const priorityRank = { P0: 0, P1: 1, P2: 2 };
  const rows = records
    .filter(record => !isReplacementBaselineScore(record))
    .map(record => {
      const questionId = String(record?.questionId || "");
      const draft = draftByQuestionId.get(questionId);
      const reasons = humanConfirmationReasons(record, draft);
      const priority = humanConfirmationPriority(reasons);
      return {
        questionId,
        category: String(record?.category || ""),
        country: String(record?.country || ""),
        question: String(record?.question || ""),
        priority,
        reasons,
        suggestedWinner: draft?.winner || "unclear",
        astrbotDraftAverage: draft?.astrbotAverage || 0,
        countryCopilotDraftAverage: draft?.countryCopilotAverage || 0,
        evidenceRefCount: evidenceRefCount(record),
        externalResearchGapCount: externalResearchGapItems(record).length,
        expectedTools: expectedTools(record),
        actualTools: astrbotTools(record),
        failureTags: listValue(record?.failureTags),
      };
    })
    .sort((left, right) => (
      (priorityRank[left.priority] ?? 9) - (priorityRank[right.priority] ?? 9)
      || left.astrbotDraftAverage - right.astrbotDraftAverage
      || left.category.localeCompare(right.category)
      || left.questionId.localeCompare(right.questionId)
    ));

  return {
    source: "codex_review_human_confirmation_queue",
    persisted: false,
    warning: "Queue only. Confirm scores manually or with the configured judge before replacement readiness.",
    count: rows.length,
    priorityCounts: rows.reduce((counts, row) => {
      counts[row.priority] = (counts[row.priority] || 0) + 1;
      return counts;
    }, {}),
    rows,
  };
}

function buildHumanConfirmationQueueTsv(queue) {
  const headers = [
    "priority",
    "question_id",
    "category",
    "country",
    "suggested_winner",
    "astrbot_draft_avg",
    "copilot_draft_avg",
    "evidence_refs",
    "external_research_gaps",
    "reasons",
    "expected_tools",
    "actual_tools",
    "failure_tags",
    "question",
  ];
  const rows = queue.rows.map(row => [
    row.priority,
    row.questionId,
    row.category,
    row.country,
    row.suggestedWinner,
    row.astrbotDraftAverage,
    row.countryCopilotDraftAverage,
    row.evidenceRefCount,
    row.externalResearchGapCount,
    row.reasons.join("; "),
    row.expectedTools.join(", "),
    row.actualTools.join(", "),
    row.failureTags.join(", "),
    row.question,
  ].map(tsvCell).join("\t"));
  return [headers.join("\t"), ...rows].join("\n");
}

function truncateText(value, maxLength = 6000) {
  const text = String(value ?? "").trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, maxLength)}\n[truncated ${text.length - maxLength} chars]`;
}

function compactEvidenceForJudge(record) {
  const evidencePackage = record?.astrbot?.evidencePackage || record?.astrbotEvidencePackage || {};
  const toolResults = Array.isArray(evidencePackage.toolResults) ? evidencePackage.toolResults : [];
  const missingEvidence = Array.isArray(evidencePackage.missingEvidence) ? evidencePackage.missingEvidence : [];
  return {
    evidenceId: evidencePackage.evidenceId || "",
    intent: evidencePackage.intent || record?.expectedIntent || "",
    country: evidencePackage.country || record?.country || "",
    confidence: evidencePackage.confidence || record?.astrbot?.evidenceConfidence || "",
    evidenceRefCount: evidenceRefCount(record),
    tools: toolResults.slice(0, 6).map(tool => ({
      toolName: tool?.toolName || "",
      success: tool?.success,
      sourceType: tool?.sourceType || "",
      rowCount: tool?.rowCount,
      summary: truncateText(tool?.summary || "", 500),
      keyFindings: Array.isArray(tool?.keyFindings)
        ? tool.keyFindings.slice(0, 6)
        : truncateText(tool?.keyFindings || "", 500),
      evidenceRefs: Array.isArray(tool?.evidenceRefs)
        ? tool.evidenceRefs.slice(0, 8).map(ref => ({
          refId: ref?.refId || "",
          label: ref?.label || "",
          value: ref?.value,
          unit: ref?.unit || "",
          source: ref?.source || "",
          table: ref?.table || "",
        }))
        : [],
    })),
    missingEvidence: missingEvidence.slice(0, 8).map(item => ({
      name: item?.name || "",
      impact: item?.impact || "",
      reason: item?.reason || "",
    })),
  };
}

function compactVisualArtifactsForJudge(record) {
  const artifacts = Array.isArray(record?.astrbot?.visualArtifacts)
    ? record.astrbot.visualArtifacts
    : Array.isArray(record?.astrbotVisualArtifacts)
      ? record.astrbotVisualArtifacts
      : [];
  return artifacts.slice(0, 8).map(artifact => ({
    id: artifact?.id || "",
    type: artifact?.type || "",
    title: artifact?.title || "",
    subtitle: artifact?.subtitle || "",
    fallbackReason: artifact?.fallbackReason || "",
    sourceEvidenceRefs: Array.isArray(artifact?.sourceEvidenceRefs) ? artifact.sourceEvidenceRefs.slice(0, 8) : [],
  }));
}

function compactFollowUpsForJudge(record) {
  const followUps = Array.isArray(record?.astrbot?.followUps)
    ? record.astrbot.followUps
    : Array.isArray(record?.astrbotFollowUps)
      ? record.astrbotFollowUps
      : [];
  return followUps.slice(0, 5).map(item => ({
    label: item?.label || "",
    question: item?.question || "",
    intent: item?.intent || "",
    expectedTools: Array.isArray(item?.expectedTools) ? item.expectedTools : [],
    reason: item?.reason || "",
  }));
}

function referenceJudgeSystemPrompt() {
  return [
    "You are a senior automotive-market analysis evaluator.",
    "Score each side-by-side record for whether AstrBot is ready to replace CountryCopilot for that business question.",
    "Use only the supplied answers, evidence summaries, artifacts, follow-ups, expected intent, and expected tools.",
    "Do not reward length by itself. Prefer reliable, evidence-aware, actionable automotive PM analysis.",
    "If a side invents unsupported numbers, lower grounding and add hallucination_risk.",
    "If a side is safe but too generic or only describes a template, lower pmInsight, actionability, and presentationReadiness.",
    "Return strict JSON only. Do not include markdown outside JSON.",
  ].join(" ");
}

function buildReferenceJudgeSetupChecklist(referenceModelPaths) {
  return referenceModelPaths.map(pathItem => {
    const env = objectValue(pathItem.env);
    const hooks = [
      env.provider ? `${env.provider}=<provider-id>` : "",
      env.model ? `${env.model}=<model-id>` : "",
      env.apiBase ? `${env.apiBase}=<api-base-url>` : "",
      env.keySource ? `${env.keySource}=<secret-env-name>` : "",
    ].filter(Boolean);
    return {
      label: String(pathItem.label || "Unknown path"),
      status: String(pathItem.status || "unknown"),
      readinessStatus: String(pathItem.readinessStatus || "unknown"),
      envHooks: hooks,
      safeSetup: [
        hooks.length > 0 ? `Set ${hooks.join(", ")}.` : "No env hooks exposed for this path yet.",
        env.keySource ? "Configure the secret value in the env var named by the key-source hook; do not paste secrets into review artifacts." : "",
        "Restart the backend on 127.0.0.1:8000 after changing env.",
        "Run judge preflight with liveCheck=false first, then run a 2-record smoke before a 30-record baseline.",
      ].filter(Boolean).join(" "),
      preflight: "PYTHONPATH=. ../../.venv/bin/python -c \"from app.services.jato_agent_llm_judge_service import preflight_judge_provider; print(preflight_judge_provider(live_check=False))\"",
    };
  });
}

function buildReferenceJudgePacket({ records, referenceModelPaths, readinessGate, draftScoreBaseline }) {
  const items = records.map(record => ({
    questionId: String(record?.questionId || ""),
    comparisonId: String(record?.comparisonId || ""),
    category: String(record?.category || ""),
    country: String(record?.country || ""),
    question: String(record?.question || ""),
    expectedIntent: String(record?.expectedIntent || ""),
    expectedTools: expectedTools(record),
    expectedFollowUpTypes: Array.isArray(record?.expectedFollowUpTypes) ? record.expectedFollowUpTypes : [],
    astrbot: {
      answer: truncateText(record?.astrbot?.answerPreview || record?.astrbotAnswer || "", 9000),
      selectedTool: record?.astrbot?.selectedTool || "",
      retrievalPath: record?.astrbot?.retrievalPath || "",
      allRetrievalPaths: Array.isArray(record?.astrbot?.allRetrievalPaths) ? record.astrbot.allRetrievalPaths : [],
      evidence: compactEvidenceForJudge(record),
      visualArtifacts: compactVisualArtifactsForJudge(record),
      followUps: compactFollowUpsForJudge(record),
      qualityScore: record?.astrbot?.qualityScore || record?.astrbotQualityScore || {},
    },
    countryCopilot: {
      answer: truncateText(record?.countryCopilot?.answerPreview || record?.copilotAnswer || "", 9000),
      mode: record?.countryCopilot?.mode || "",
      sourceCount: record?.countryCopilot?.sourceCount || 0,
      evidenceTableCount: record?.countryCopilot?.evidenceTableCount || 0,
      chartLinkCount: record?.countryCopilot?.chartLinkCount || 0,
    },
    existingFailureTags: listValue(record?.failureTags),
    codexDraft: draftScoresForRecord(record),
  }));
  return {
    source: "astrbot_reference_judge_packet",
    createdAt: new Date().toISOString(),
    warning: "For GPT5.5/Opus/Fable/manual judging only. Import scores only after a reviewer accepts them.",
    systemPrompt: referenceJudgeSystemPrompt(),
    referenceModelPaths,
    referenceJudgeSetupChecklist: buildReferenceJudgeSetupChecklist(referenceModelPaths),
    readinessGate: {
      status: readinessGate.status,
      replacementReady: readinessGate.replacementReady,
      scoredBaseline: readinessGate.scoredBaseline,
      pendingBaselineScoring: readinessGate.pendingBaselineScoring,
      minBusinessScores: readinessGate.minBusinessScores,
      baselineSourceCounts: readinessGate.baselineSourceCounts,
    },
    rubric: {
      dimensions: SCORE_RUBRIC,
      scoreScale: {
        1: "risky / wrong / not usable",
        2: "weak",
        3: "tie or acceptable but not clearly better",
        4: "better than current baseline",
        5: "ready to use for this business question",
      },
      winnerValues: ["astrbot", "countryCopilot", "tie", "unclear"],
      failureTaxonomy: FAILURE_TAXONOMY,
    },
    outputSchema: {
      records: [{
        questionId: "string",
        winner: "astrbot | countryCopilot | tie | unclear",
        astrbotScores: Object.fromEntries(SCORE_DIMENSIONS.map(key => [key, "1-5"])),
        countryCopilotScores: Object.fromEntries(SCORE_DIMENSIONS.map(key => [key, "1-5"])),
        failureTags: FAILURE_TAXONOMY,
        notes: "short reason for the score",
      }],
    },
    summary: {
      recordCount: items.length,
      categories: [...new Set(items.map(item => item.category))].sort(),
    },
    records: items,
  };
}

function markdownReferenceJudgePacket(packet) {
  const lines = [
    "# AstrBot Reference Judge Packet",
    "",
    `- Created at: ${packet.createdAt}`,
    `- Records: ${packet.summary.recordCount}`,
    `- Warning: ${packet.warning}`,
    "",
    "## Judge Prompt",
    "",
    packet.systemPrompt,
    "",
    "## Reference Paths",
    "",
    "| Path | Status | Readiness | Next action |",
    "| --- | --- | --- | --- |",
    ...packet.referenceModelPaths.map(item => (
      `| ${mdCell(item.label)} | ${mdCell(item.status)} | ${mdCell(item.readinessStatus || "")} | ${mdCell(item.nextAction)} |`
    )),
    "",
    "## Reference Judge Setup Checklist",
    "",
    "| Path | Status | Env hooks | Safe setup |",
    "| --- | --- | --- | --- |",
    ...packet.referenceJudgeSetupChecklist.map(item => (
      `| ${mdCell(item.label)} | ${mdCell(`${item.status}/${item.readinessStatus}`)} | ${mdCell(item.envHooks.join("; ") || "none")} | ${mdCell(item.safeSetup)} |`
    )),
    "",
    "## Rubric",
    "",
    "| Dimension | Label | Anchor |",
    "| --- | --- | --- |",
    ...packet.rubric.dimensions.map(item => (
      `| ${mdCell(item.key)} | ${mdCell(item.label)} | ${mdCell(item.anchor)} |`
    )),
    "",
    "## Output JSON Shape",
    "",
    "```json",
    JSON.stringify(packet.outputSchema, null, 2),
    "```",
    "",
    "## Records",
    "",
  ];
  for (const item of packet.records) {
    lines.push(
      `### ${item.questionId}`,
      "",
      `- Category: ${item.category}`,
      `- Country: ${item.country}`,
      `- Question: ${item.question}`,
      `- Expected intent: ${item.expectedIntent}`,
      `- Expected tools: ${item.expectedTools.join(", ") || "none"}`,
      `- Codex draft: ${item.codexDraft.winner} · AstrBot ${item.codexDraft.astrbotAverage} / Copilot ${item.codexDraft.countryCopilotAverage}`,
      `- AstrBot evidence refs: ${item.astrbot.evidence.evidenceRefCount}`,
      `- AstrBot tools: ${item.astrbot.selectedTool || item.astrbot.retrievalPath || "none"}`,
      "",
      "**AstrBot Answer**",
      "",
      item.astrbot.answer || "(empty)",
      "",
      "**CountryCopilot Answer**",
      "",
      item.countryCopilot.answer || "(empty)",
      "",
      "**AstrBot Evidence Summary**",
      "",
      `- Confidence: ${item.astrbot.evidence.confidence || "unknown"}`,
      `- Missing evidence: ${item.astrbot.evidence.missingEvidence.map(missing => `${missing.name}:${missing.impact}`).join(", ") || "none"}`,
      `- Visual artifacts: ${item.astrbot.visualArtifacts.map(artifact => `${artifact.type}:${artifact.title}`).join(", ") || "none"}`,
      `- Follow-ups: ${item.astrbot.followUps.map(follow => `${follow.intent}:${follow.label}`).join(", ") || "none"}`,
      "",
    );
  }
  return lines.join("\n");
}

function answerSnippet(record, side) {
  return String(
    side === "astrbot"
      ? record?.astrbot?.answerPreview || record?.astrbotAnswer || record?.astrbot?.error || ""
      : record?.countryCopilot?.answerPreview || record?.copilotAnswer || record?.countryCopilot?.error || "",
  ).replace(/\s+/g, " ").slice(0, 720);
}

function evidenceSummaryForSheet(record) {
  const expected = expectedTools(record).join(", ");
  const used = astrbotTools(record).join(", ");
  const refs = evidenceRefCount(record);
  const missing = Array.isArray(record?.astrbot?.missingEvidence)
    ? record.astrbot.missingEvidence
      .slice(0, 4)
      .map(item => [item?.name, item?.impact].filter(Boolean).join(":"))
      .filter(Boolean)
      .join(", ")
    : "";
  return [
    expected ? `expected=${expected}` : "",
    used ? `used=${used}` : "",
    `refs=${refs}`,
    missing ? `missing=${missing}` : "",
  ].filter(Boolean).join(" | ");
}

function draftTotalScore(value) {
  return String(clampScore(Number(value) || 0));
}

function buildHumanScoringSheet(records, draftScoreBaseline, { prefillDraft = false } = {}) {
  const draftByQuestionId = new Map(draftScoreBaseline.items.map(item => [item.questionId, item]));
  const headers = [
    "question_id",
    "category",
    "country",
    "question",
    "codex_suggested_winner",
    "codex_astrbot_avg",
    "codex_copilot_avg",
    "astrbot_total_1_to_5",
    "copilot_total_1_to_5",
    "winner",
    "notes",
    "failure_tags",
    "evidence_summary",
    "astrbot_answer_preview",
    "copilot_answer_preview",
  ];
  const rows = records.map(record => {
    const draft = draftByQuestionId.get(String(record?.questionId || ""));
    const notes = draft
      ? `[Codex draft - human confirm before import] ${draft.rationale.slice(0, 3).join(" ")}`
      : "";
    return [
      record?.questionId,
      record?.category,
      record?.country || "",
      record?.question,
      draft?.winner || "",
      draft?.astrbotAverage || "",
      draft?.countryCopilotAverage || "",
      prefillDraft && draft ? draftTotalScore(draft.astrbotAverage) : "",
      prefillDraft && draft ? draftTotalScore(draft.countryCopilotAverage) : "",
      prefillDraft && draft ? draft.winner : "",
      prefillDraft && draft ? notes : "",
      prefillDraft && draft ? listValue(draft.failureTags).join(", ") : "",
      evidenceSummaryForSheet(record),
      answerSnippet(record, "astrbot"),
      answerSnippet(record, "countryCopilot"),
    ].map(tsvCell).join("\t");
  });
  return [headers.join("\t"), ...rows].join("\n");
}

function suggestedWinner(record) {
  const astrbotStatus = record?.astrbot?.status || "";
  const copilotStatus = record?.countryCopilot?.status || "";
  const failureTags = new Set(listValue(record?.failureTags));
  const astrbotRefs = numberValue(record?.astrbot?.evidenceRefCount);
  const copilotSources = numberValue(record?.countryCopilot?.sourceCount);
  const astrbotChars = numberValue(record?.comparison?.astrbotAnswerChars);
  const copilotChars = numberValue(record?.comparison?.countryCopilotAnswerChars);

  if (astrbotStatus === "failed" && copilotStatus !== "failed") return "countryCopilot";
  if (copilotStatus === "failed" && astrbotStatus !== "failed") return "astrbot";
  if (failureTags.has("answer_too_conservative") || failureTags.has("answer_too_generic")) {
    return copilotChars > astrbotChars * 1.6 ? "countryCopilot" : "unclear";
  }
  if (astrbotRefs > 0 && copilotSources === 0) return "astrbot";
  return "unclear";
}

function suggestedScoresForWinner(winner) {
  if (winner === "astrbot") {
    return { astrbot: fillScores(4), countryCopilot: fillScores(3) };
  }
  if (winner === "countryCopilot") {
    return { astrbot: fillScores(3), countryCopilot: fillScores(4) };
  }
  return { astrbot: fillScores(3), countryCopilot: fillScores(3) };
}

function inferUiStatus(record, uiIssues) {
  if (record?.errors && Object.keys(record.errors).length > 0) return "fail";
  if (uiIssues.length > 0) return "warning";
  if (listValue(record?.failureTags).length > 0) return "warning";
  return "pass";
}

function buildReviewNote(record, screenshots, globalUiIssues) {
  const tags = listValue(record?.failureTags);
  const localIssues = [];
  if (artifactCount(record) === 0 && /pricing|compare|market|report|policy|configuration/i.test(String(record?.category || ""))) {
    localIssues.push("No AstrBot visualArtifacts stored for this side-by-side record.");
  }
  if (followUpCount(record) === 0) {
    localIssues.push("No AstrBot followUps stored for this side-by-side record.");
  }
  const allIssues = [...globalUiIssues, ...localIssues];
  const draft = draftScoresForRecord(record);
  const winner = draft.winner;
  const scores = {
    astrbot: draft.astrbotScores,
    countryCopilot: draft.countryCopilotScores,
  };
  const questionId = String(record?.questionId || "");
  const reviewNotes = [
    `Codex draft suggests ${winner || "unclear"} for ${questionId || "this record"}.`,
    `Draft averages: AstrBot ${draft.astrbotAverage} / CountryCopilot ${draft.countryCopilotAverage}.`,
    tags.length > 0 ? `Failure tags already visible: ${tags.join(", ")}.` : "No stored failure tags were visible.",
    localIssues.length > 0 ? `UI/data issues: ${localIssues.join(" ")}` : "Side-by-side panel appears reviewable.",
    `Rationale: ${draft.rationale.join(" ")}`,
    "This is a draft note only; it should not be treated as human calibration until a user applies and saves it.",
  ].join(" ");

  return {
    questionId,
    uiStatus: inferUiStatus(record, allIssues),
    suggestedWinner: winner,
    suggestedScores: scores,
    suggestedFailureTags: tags,
    reviewNotes,
    uiIssues: allIssues,
    screenshots,
    createdAt: new Date().toISOString(),
    source: "codex_review",
  };
}

function tagCountsFromNotes(notes) {
  const tagCounts = new Map();
  for (const note of notes) {
    for (const tag of listValue(note?.suggestedFailureTags)) {
      tagCounts.set(tag, (tagCounts.get(tag) || 0) + 1);
    }
  }
  return tagCounts;
}

function sortedTagRows(tagCounts, limit = 8) {
  return [...tagCounts.entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, limit);
}

function tagCountTotal(tagCounts) {
  return [...tagCounts.values()].reduce((sum, count) => sum + count, 0);
}

function reviewStatsFromReport(report, notes) {
  const summary = report?.summary && typeof report.summary === "object" ? report.summary : {};
  const baselineScored = numberValue(summary.baselineScoredCount) || numberValue(summary.scoredCount);
  const pendingBaseline = Number.isFinite(summary.pendingBaselineScoring)
    ? numberValue(summary.pendingBaselineScoring)
    : Math.max(0, (numberValue(summary.count) || numberValue(report?.total)) - baselineScored);
  const replacementBaselineScored = Number.isFinite(summary.replacementBaselineScoredCount)
    ? numberValue(summary.replacementBaselineScoredCount)
    : baselineScored;
  const pendingReplacementBaseline = Number.isFinite(summary.pendingReplacementBaselineScoring)
    ? numberValue(summary.pendingReplacementBaselineScoring)
    : Math.max(0, (numberValue(summary.count) || numberValue(report?.total)) - replacementBaselineScored);
  return {
    checkedRecords: notes.length,
    humanScored: numberValue(summary.scoredCount),
    pendingHumanScoring: numberValue(summary.pendingHumanScoring),
    baselineScored,
    pendingBaselineScoring: pendingBaseline,
    baselineSourceCounts: objectValue(summary.baselineSourceCounts ?? summary.humanScoreSourceCounts),
    replacementBaselineScored,
    pendingReplacementBaselineScoring: pendingReplacementBaseline,
    replacementBaselineSourceCounts: objectValue(summary.replacementBaselineSourceCounts ?? summary.baselineSourceCounts ?? summary.humanScoreSourceCounts),
    recordsNeedingHumanConfirmation: notes.filter(note => (
      note?.uiStatus !== "pass"
      || note?.suggestedWinner !== "unclear"
      || listValue(note?.suggestedFailureTags).length > 0
    )).length,
  };
}

function failureTagRowsFromReport(report) {
  const summary = report?.summary && typeof report.summary === "object" ? report.summary : {};
  const counts = summary.failureTagCounts && typeof summary.failureTagCounts === "object"
    ? summary.failureTagCounts
    : {};
  return Object.entries(counts)
    .map(([tag, count]) => [tag, numberValue(count)])
    .filter(([, count]) => count > 0)
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
}

function reportFailureTagTotal(report) {
  return failureTagRowsFromReport(report).reduce((sum, [, count]) => sum + count, 0);
}

function reportEngineeringFailureTagTotal(report) {
  return failureTagRowsFromReport(report)
    .filter(([tag]) => !DATA_GAP_FAILURE_TAGS.has(tag))
    .reduce((sum, [, count]) => sum + count, 0);
}

function reportEvidenceGapTotal(report) {
  return failureTagRowsFromReport(report)
    .filter(([tag]) => DATA_GAP_FAILURE_TAGS.has(tag))
    .reduce((sum, [, count]) => sum + count, 0);
}

function repairQueueFromReport(report) {
  return Array.isArray(report?.evidenceRepairQueue) ? report.evidenceRepairQueue : [];
}

function repairQueueExpectedFromReport(report) {
  if (reportEvidenceGapTotal(report) > 0) return true;
  const records = Array.isArray(report?.items) ? report.items : [];
  return records.some(record => (
    listValue(record?.failureTags).length > 0
    || (Array.isArray(record?.astrbot?.missingEvidence) && record.astrbot.missingEvidence.length > 0)
  ));
}

function repairQueueHasAction(report) {
  return repairQueueFromReport(report).some(item => (
    typeof item?.repairAction === "string"
    && item.repairAction.trim().length > 0
  ));
}

function topEvidenceRepairAction(report) {
  const repairQueue = repairQueueFromReport(report);
  const candidate = repairQueue.find(item => String(item?.priority || "").toUpperCase() === "P0")
    || repairQueue[0];
  if (!candidate || typeof candidate !== "object") {
    return {
      label: "the named evidence/data gap",
      action: "Fill the named evidence/data gap shown in the Evidence Repair Queue, then rerun business validation.",
    };
  }
  const primaryGap = String(candidate.primaryGap || candidate?.repairSummary?.primaryGap || "").trim();
  const questionId = String(candidate.questionId || "").trim();
  const repairAction = String(candidate.repairAction || candidate.commandHint || "").trim();
  const label = [
    primaryGap || "evidence/data gap",
    questionId ? `for ${questionId}` : "",
  ].filter(Boolean).join(" ");
  return {
    label,
    action: repairAction
      ? `${repairAction}${questionId ? `, then rerun ${questionId}.` : ", then rerun business validation."}`
      : `Fill ${label}, then rerun business validation.`,
  };
}

function businessReportDisplayTexts(report) {
  const records = Array.isArray(report?.items) ? report.items : [];
  const repairQueue = repairQueueFromReport(report);
  const texts = [];
  for (const record of records) {
    if (!record || typeof record !== "object") continue;
    const astrbot = objectValue(record.astrbot);
    const countryCopilot = objectValue(record.countryCopilot);
    texts.push(
      record.question,
      record.astrbotAnswer,
      astrbot.answerPreview,
      astrbot.answer,
      countryCopilot.answerPreview,
      countryCopilot.answer,
    );
  }
  for (const item of repairQueue) {
    if (!item || typeof item !== "object") continue;
    const summary = objectValue(item.repairSummary);
    texts.push(item.repairAction, item.commandHint, summary.sourceSummary, summary.nextStep);
    const missing = Array.isArray(item.missingEvidence) ? item.missingEvidence : [];
    for (const entry of missing) {
      if (entry && typeof entry === "object") texts.push(entry.reason, entry.message);
    }
    const actions = Array.isArray(item.recommendedActions) ? item.recommendedActions : [];
    for (const action of actions) {
      if (action && typeof action === "object") texts.push(action.action, action.rationale);
    }
    const tasks = Array.isArray(item.repairTasks) ? item.repairTasks : [];
    for (const task of tasks) {
      if (task && typeof task === "object") texts.push(task.title, task.input, task.output, task.commandHint);
    }
  }
  if (typeof report?.markdown === "string") texts.push(report.markdown);
  return texts.filter(value => typeof value === "string" && value.trim()).join("\n");
}

function buildReadinessGate({ report, notes, uiChecks, reviewStats }) {
  const summary = report?.summary && typeof report.summary === "object" ? report.summary : {};
  const backendReadiness = objectValue(summary.replacementReadiness);
  const judgeCalibration = summary.judgeCalibration && typeof summary.judgeCalibration === "object"
    ? summary.judgeCalibration
    : {};
  const totalQuestions = numberValue(backendReadiness.totalQuestions) || numberValue(summary.count) || numberValue(report?.total) || notes.length;
  const minBusinessScores = totalQuestions > 0
    ? numberValue(backendReadiness.minimumRequiredScores) || Math.min(totalQuestions, Math.max(8, Math.ceil(totalQuestions * 0.7)))
    : 8;
  const humanScored = numberValue(reviewStats.humanScored);
  const gptJudged = numberValue(judgeCalibration.gptJudgedCount);
  const scoredBaseline = Number.isFinite(backendReadiness.scoredCount)
    ? numberValue(backendReadiness.scoredCount)
    : Number.isFinite(summary.replacementBaselineScoredCount)
    ? numberValue(summary.replacementBaselineScoredCount)
    : numberValue(summary.baselineScoredCount) || Math.max(humanScored, gptJudged);
  const pendingBaselineScoring = Number.isFinite(backendReadiness.pendingCount)
    ? numberValue(backendReadiness.pendingCount)
    : Number.isFinite(summary.pendingReplacementBaselineScoring)
    ? numberValue(summary.pendingReplacementBaselineScoring)
    : Number.isFinite(summary.pendingBaselineScoring)
    ? numberValue(summary.pendingBaselineScoring)
    : Math.max(0, totalQuestions - scoredBaseline);
  const baselineSourceCounts = objectValue(backendReadiness.sourceCounts ?? summary.replacementBaselineSourceCounts ?? summary.baselineSourceCounts ?? summary.humanScoreSourceCounts);
  const uiPassCount = uiChecks.filter(item => item.ok).length;
  const failureTagTotal = reportFailureTagTotal(report);
  const engineeringFailureTagTotal = reportEngineeringFailureTagTotal(report);
  const evidenceGapTotal = reportEvidenceGapTotal(report);
  const backendVerdict = String(backendReadiness.verdict || summary.replacementReadinessVerdict || "unknown");
  const backendAllowsReplacement = [
    "ready",
    "ready_to_replace",
    "replacement_ready",
    "ready_to_consider_switch",
    "ready_for_limited_default_trial",
  ].includes(backendVerdict);

  const engineeringReady = uiPassCount === uiChecks.length
    && engineeringFailureTagTotal === 0
    && numberValue(summary.astrbotErrorCount) === 0
    && numberValue(summary.countryCopilotErrorCount) === 0;
  const evidenceReady = evidenceGapTotal === 0;
  const evidenceRepairAction = topEvidenceRepairAction(report);
  const businessBaselineReady = scoredBaseline >= minBusinessScores;
  const replacementReady = engineeringReady && evidenceReady && businessBaselineReady && backendAllowsReplacement;

  const reasons = [];
  if (!engineeringReady) {
    if (uiPassCount !== uiChecks.length) reasons.push(`UI checks passing ${uiPassCount}/${uiChecks.length}.`);
    if (engineeringFailureTagTotal > 0) reasons.push(`Current business report still has ${engineeringFailureTagTotal} engineering/business failure tags.`);
    if (numberValue(summary.astrbotErrorCount) > 0) reasons.push(`AstrBot execution errors: ${summary.astrbotErrorCount}.`);
    if (numberValue(summary.countryCopilotErrorCount) > 0) reasons.push(`CountryCopilot execution errors: ${summary.countryCopilotErrorCount}.`);
  }
  if (!evidenceReady) {
    reasons.push(`Current business report still has ${evidenceGapTotal} evidence/data gap tags; fill ${evidenceRepairAction.label} before replacing /copilot.`);
  }
  if (!businessBaselineReady) {
    reasons.push(`Business scoring baseline is incomplete: baseline ${scoredBaseline}/${totalQuestions}, pending ${pendingBaselineScoring}; minimum required is ${minBusinessScores}. Sources: ${sourceCountsLabel(baselineSourceCounts)}.`);
  }
  if (!backendAllowsReplacement) {
    reasons.push(`Backend replacement verdict is ${backendVerdict}.`);
  }
  if (replacementReady) {
    reasons.push("Engineering checks, score baseline, and backend verdict all allow considering a controlled switch.");
  }

  return {
    status: replacementReady
      ? "ready_to_consider_switch"
      : engineeringReady && !evidenceReady
        ? "evidence_data_blocked"
        : engineeringReady
        ? "engineering_passed_business_scores_pending"
        : "engineering_blocked",
    replacementReady,
    engineeringReady,
    evidenceReady,
    businessBaselineReady,
    backendVerdict,
    uiPassCount,
    uiCheckCount: uiChecks.length,
    failureTagTotal,
    engineeringFailureTagTotal,
    evidenceGapTotal,
    humanScored,
    gptJudged,
    scoredBaseline,
    pendingBaselineScoring,
    baselineSourceCounts,
    totalQuestions,
    minBusinessScores,
    astrbotWinRate: numberValue(backendReadiness.astrbotWinRate ?? summary.replacementAstrbotWinRate ?? summary.astrbotWinRate),
    avgAstrBotComposite: numberValue(summary.avgAstrBotComposite),
    reasons,
    recommendedNextAction: replacementReady
      ? "Run a controlled feature-flag switch trial; keep /copilot available for rollback."
      : !evidenceReady
        ? evidenceRepairAction.action
      : businessBaselineReady
        ? "Review failed gates and fix the remaining engineering or verdict blockers."
        : "Generate GPT judged records or manually score at least the minimum business sample before changing default traffic.",
  };
}

function referenceModelPathsFromReport(report) {
  const summary = objectValue(report?.summary);
  const matrix = objectValue(summary.referenceJudgePaths ?? report?.referenceJudgePaths);
  const paths = Array.isArray(matrix.paths) ? matrix.paths : [];
  if (paths.length === 0) return REFERENCE_MODEL_PATHS;
  return paths.map(pathItem => {
    const env = objectValue(pathItem.env);
    const keySource = String(pathItem.keySource || "").trim();
    const model = String(pathItem.model || "").trim();
    const active = Boolean(pathItem.active);
    const configuredLabel = keySource
      ? `${keySource}${pathItem.keyConfigured ? " configured" : " missing"}`
      : "no key env";
    const runtimeLabel = [model, active ? "active" : ""].filter(Boolean).join(" · ");
    return {
      label: String(pathItem.label || pathItem.id || "Unknown path"),
      status: String(pathItem.status || "unknown"),
      readinessStatus: String(pathItem.readinessStatus || "unknown"),
      role: String(pathItem.role || ""),
      evidence: [
        String(pathItem.evidence || ""),
        runtimeLabel ? `Runtime: ${runtimeLabel}.` : "",
        configuredLabel ? `Key: ${configuredLabel}.` : "",
        [env.model, env.apiBase, env.keySource].some(Boolean)
          ? `Env hooks: ${[env.model, env.apiBase, env.keySource].filter(Boolean).join(", ")}.`
          : "",
      ].filter(Boolean).join(" "),
      nextAction: String(pathItem.nextAction || ""),
      env: {
        provider: String(env.provider || ""),
        model: String(env.model || ""),
        apiBase: String(env.apiBase || ""),
        keySource: String(env.keySource || ""),
      },
    };
  });
}

async function readJsonIfExists(filePath) {
  try {
    return JSON.parse(await readFile(filePath, "utf-8"));
  } catch {
    return null;
  }
}

async function latestPreviousReview(frontendRoot, currentRunId) {
  const reviewRoot = path.join(frontendRoot, "artifacts", "astrbot-review");
  const entries = await readdir(reviewRoot, { withFileTypes: true }).catch(() => []);
  const candidates = entries
    .filter(entry => entry.isDirectory() && entry.name !== currentRunId)
    .map(entry => entry.name)
    .sort()
    .reverse();
  for (const runId of candidates) {
    const dir = path.join(reviewRoot, runId);
    const notes = await readJsonIfExists(path.join(dir, "codex_review_notes.json"));
    if (!Array.isArray(notes)) continue;
    const summary = await readJsonIfExists(path.join(dir, "codex_review_summary.json"));
    return {
      runId,
      notes,
      stats: summary?.reviewStats && typeof summary.reviewStats === "object"
        ? summary.reviewStats
        : {
          checkedRecords: notes.length,
          humanScored: 0,
          pendingHumanScoring: 0,
          recordsNeedingHumanConfirmation: notes.length,
        },
      tagCounts: tagCountsFromNotes(notes),
    };
  }
  return null;
}

async function screenshotIfVisible(locator, screenshotPath) {
  const count = await locator.count();
  if (count === 0) return false;
  await locator.scrollIntoViewIfNeeded();
  await locator.screenshot({ path: screenshotPath });
  return true;
}

function markdownReport({
  baseUrl,
  apiBase,
  report,
  notes,
  screenshots,
  uiChecks,
  previousReview,
  reviewStats,
  readinessGate,
  referenceModelPaths,
  draftScoreBaseline,
  humanConfirmationQueue,
  referenceJudgePacket,
  scoringArtifacts,
}) {
  const tagCounts = tagCountsFromNotes(notes);
  const topTags = sortedTagRows(tagCounts);
  const reportTopTags = failureTagRowsFromReport(report).slice(0, 8);
  const previousTagRows = previousReview ? sortedTagRows(previousReview.tagCounts) : [];
  const previousFailureTotal = previousReview ? tagCountTotal(previousReview.tagCounts) : 0;
  const currentFailureTotal = tagCountTotal(tagCounts);
  const failureDelta = previousReview ? previousFailureTotal - currentFailureTotal : 0;
  const referenceJudgeSetupChecklist = buildReferenceJudgeSetupChecklist(referenceModelPaths);
  const comparedTags = sortedTagRows(new Map([
    ...previousTagRows.map(([tag]) => [tag, 0]),
    ...topTags.map(([tag]) => [tag, 0]),
  ]), 12).map(([tag]) => {
    const before = previousReview?.tagCounts.get(tag) || 0;
    const after = tagCounts.get(tag) || 0;
    const diff = after - before;
    return `- ${tag}: ${before} → ${after}${diff === 0 ? "" : diff > 0 ? ` (+${diff})` : ` (${diff})`}`;
  });
  const lines = [
    "# AstrBot Codex Review Report",
    "",
    `- Created at: ${new Date().toISOString()}`,
    `- App URL: ${baseUrl}/astrbot/eval`,
    `- API base: ${apiBase}`,
    `- Checked records: ${notes.length}`,
    `- Business comparisons available: ${report.total ?? 0}`,
    `- Baseline scored records: ${reviewStats.baselineScored}`,
    `- Pending baseline scoring: ${reviewStats.pendingBaselineScoring}`,
    `- Baseline score sources: ${sourceCountsLabel(reviewStats.baselineSourceCounts)}`,
    `- Replacement baseline scored records: ${reviewStats.replacementBaselineScored}`,
    `- Pending replacement baseline scoring: ${reviewStats.pendingReplacementBaselineScoring}`,
    `- Replacement baseline sources: ${sourceCountsLabel(reviewStats.replacementBaselineSourceCounts)}`,
    `- Human scored records: ${reviewStats.humanScored}`,
    `- Pending human scoring: ${reviewStats.pendingHumanScoring}`,
    "",
    "## Readiness Gate",
    "",
    `- Status: ${readinessGate.status}`,
    `- Replacement ready: ${readinessGate.replacementReady ? "YES" : "NO"}`,
    `- Engineering ready: ${readinessGate.engineeringReady ? "YES" : "NO"} (${readinessGate.uiPassCount}/${readinessGate.uiCheckCount} UI checks, ${readinessGate.engineeringFailureTagTotal} engineering failure tags, ${readinessGate.failureTagTotal} total report tags)`,
    `- Evidence/data ready: ${readinessGate.evidenceReady ? "YES" : "NO"} (${readinessGate.evidenceGapTotal} evidence/data gap tags)`,
    `- Business score baseline: ${readinessGate.businessBaselineReady ? "YES" : "NO"} (baseline ${readinessGate.scoredBaseline}/${readinessGate.totalQuestions}, pending ${readinessGate.pendingBaselineScoring}; minimum ${readinessGate.minBusinessScores}; sources ${sourceCountsLabel(readinessGate.baselineSourceCounts)})`,
    `- Backend replacement verdict: ${readinessGate.backendVerdict}`,
    `- AstrBot human win rate: ${Math.round(readinessGate.astrbotWinRate * 100)}%`,
    `- Avg AstrBot composite: ${Math.round(readinessGate.avgAstrBotComposite * 100)}%`,
    `- Recommended next action: ${readinessGate.recommendedNextAction}`,
    "",
    ...(readinessGate.reasons.length > 0 ? readinessGate.reasons.map(reason => `- ${reason}`) : ["- none"]),
    "",
    "## Reference Model Paths",
    "",
    "| Path | Status | Readiness | Role | Evidence | Next action |",
    "| --- | --- | --- | --- | --- | --- |",
    ...referenceModelPaths.map(pathItem => (
      `| ${mdCell(pathItem.label)} | ${mdCell(pathItem.status)} | ${mdCell(pathItem.readinessStatus || "")} | ${mdCell(pathItem.role)} | ${mdCell(pathItem.evidence)} | ${mdCell(pathItem.nextAction)} |`
    )),
    "",
    "## Reference Judge Setup Checklist",
    "",
    "| Path | Status | Env hooks | Safe setup |",
    "| --- | --- | --- | --- |",
    ...referenceJudgeSetupChecklist.map(pathItem => (
      `| ${mdCell(pathItem.label)} | ${mdCell(`${pathItem.status}/${pathItem.readinessStatus}`)} | ${mdCell(pathItem.envHooks.join("; ") || "none")} | ${mdCell(pathItem.safeSetup)} |`
    )),
    "",
    "## Human Scoring Artifacts",
    "",
    "- These files are review aids only. They do not write app scores until a reviewer imports and confirms them in `/astrbot/eval`.",
    `- Blank manual scoring TSV: ${scoringArtifacts.manualTemplatePath}`,
    `- Codex prefilled draft TSV: ${scoringArtifacts.codexDraftSheetPath}`,
    `- Human confirmation queue JSON: ${scoringArtifacts.humanQueueJsonPath}`,
    `- Human confirmation queue TSV: ${scoringArtifacts.humanQueueTsvPath}`,
    `- Reference judge packet JSON: ${scoringArtifacts.referenceJudgePacketJsonPath}`,
    `- Reference judge packet Markdown: ${scoringArtifacts.referenceJudgePacketMdPath}`,
    `- Rows exported: ${scoringArtifacts.rowCount}`,
    "- Safe workflow: open the TSV, review/edit totals and notes, paste into `Import filled sheet draft`, then use `Save imported manual scores (n)` only after human confirmation.",
    "",
    "## Reference Judge Packet",
    "",
    `- Source: ${referenceJudgePacket.source}`,
    `- Warning: ${referenceJudgePacket.warning}`,
    `- Records: ${referenceJudgePacket.summary.recordCount}`,
    `- Categories: ${referenceJudgePacket.summary.categories.join(", ")}`,
    "- Use this packet when a GPT5.5, Opus 4.8, Fable 5, or manual reviewer needs to judge the same side-by-side records with the same rubric.",
    "- Output must follow the packet JSON schema before importing or saving any scores.",
    "",
    "## Human Confirmation Queue",
    "",
    `- Source: ${humanConfirmationQueue.source}`,
    `- Persisted to app scoring: ${humanConfirmationQueue.persisted ? "YES" : "NO"}`,
    `- Warning: ${humanConfirmationQueue.warning}`,
    `- Rows needing confirmation: ${humanConfirmationQueue.count}`,
    `- Priority split: P0 ${humanConfirmationQueue.priorityCounts.P0 || 0}, P1 ${humanConfirmationQueue.priorityCounts.P1 || 0}, P2 ${humanConfirmationQueue.priorityCounts.P2 || 0}`,
    "",
    "| Priority | Question | Category | Suggested | Draft A/C | Evidence refs | Research gaps | Why review first |",
    "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ...humanConfirmationQueue.rows.slice(0, 12).map(row => (
      `| ${row.priority} | ${mdCell(row.question)} | ${mdCell(row.category)} | ${mdCell(row.suggestedWinner)} | ${row.astrbotDraftAverage} / ${row.countryCopilotDraftAverage} | ${row.evidenceRefCount} | ${row.externalResearchGapCount} | ${mdCell(row.reasons.join("; "))} |`
    )),
    "",
    "## Codex Draft Score Baseline",
    "",
    `- Source: ${draftScoreBaseline.source}`,
    `- Persisted to app scoring: ${draftScoreBaseline.persisted ? "YES" : "NO"}`,
    `- Warning: ${draftScoreBaseline.warning}`,
    `- Draft scored records: ${draftScoreBaseline.count}`,
    `- Draft average: AstrBot ${draftScoreBaseline.avgAstrBot} / CountryCopilot ${draftScoreBaseline.avgCountryCopilot}`,
    `- Draft AstrBot win rate: ${Math.round(draftScoreBaseline.astrbotWinRate * 100)}%`,
    `- Draft wins: AstrBot ${draftScoreBaseline.wins.astrbot}, CountryCopilot ${draftScoreBaseline.wins.countryCopilot}, Tie ${draftScoreBaseline.wins.tie}, Unclear ${draftScoreBaseline.wins.unclear}`,
    "",
    "### Draft Category Scores",
    "",
    "| Category | Count | AstrBot | CountryCopilot | Wins |",
    "| --- | ---: | ---: | ---: | --- |",
    ...draftScoreBaseline.categoryLevel.map(row => (
      `| ${row.category} | ${row.count} | ${row.avgAstrBot} | ${row.avgCountryCopilot} | A ${row.wins.astrbot} / C ${row.wins.countryCopilot} / T ${row.wins.tie} / U ${row.wins.unclear} |`
    )),
    "",
    "### Draft Record Scores",
    "",
    "| Question | Category | Winner | AstrBot | CountryCopilot | Notes |",
    "| --- | --- | --- | ---: | ---: | --- |",
    ...draftScoreBaseline.items.map(item => (
      `| ${mdCell(item.question)} | ${mdCell(item.category)} | ${mdCell(item.winner)} | ${item.astrbotAverage} | ${item.countryCopilotAverage} | ${mdCell(item.rationale.slice(0, 2).join(" "))} |`
    )),
    "",
    "## UI Checks",
    "",
    ...uiChecks.map(item => `- ${item.ok ? "PASS" : "FAIL"}: ${item.label}${item.detail ? ` — ${item.detail}` : ""}`),
    "",
    "## Before / After Snapshot",
    "",
    ...(previousReview ? [
      `- Previous run: ${previousReview.runId}`,
      `- Current run records needing human confirmation: ${reviewStats.recordsNeedingHumanConfirmation}`,
      `- Previous run records needing human confirmation: ${previousReview.stats.recordsNeedingHumanConfirmation ?? previousReview.notes.length}`,
      `- Failure tag total delta: ${previousFailureTotal} → ${currentFailureTotal}${failureDelta === 0 ? "" : failureDelta > 0 ? ` (reduced ${failureDelta})` : ` (increased ${Math.abs(failureDelta)})`}`,
      "",
      "### Failure Tag Delta",
      "",
      ...(comparedTags.length > 0 ? comparedTags : ["- none"]),
    ] : [
      "- No previous Codex review artifact found.",
    ]),
    "",
    "## Suggested Top Failures",
    "",
    ...(topTags.length > 0 ? topTags.map(([tag, count]) => `- ${tag}: ${count}`) : ["- none"]),
    "",
    "## Current Report Failure Tags",
    "",
    ...(reportTopTags.length > 0 ? reportTopTags.map(([tag, count]) => `- ${tag}: ${count}`) : ["- none"]),
    "",
    "## Records Needing Human Confirmation",
    "",
    ...notes.map(note => [
      `### ${note.questionId || "unknown"}`,
      `- UI status: ${note.uiStatus}`,
      `- Suggested winner: ${note.suggestedWinner}`,
      `- Suggested tags: ${note.suggestedFailureTags.join(", ") || "none"}`,
      `- Screenshots: ${note.screenshots.join(", ") || "none"}`,
      `- Notes: ${note.reviewNotes}`,
      "",
    ].join("\n")),
    "## Screenshot Index",
    "",
    ...screenshots.map(item => `- ${item.label}: ${item.path}`),
    "",
  ];
  return lines.join("\n");
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const scriptPath = fileURLToPath(import.meta.url);
  const frontendRoot = path.resolve(path.dirname(scriptPath), "..");
  const projectRoot = path.resolve(frontendRoot, "..", "..");
  const runId = nowStamp();
  const artifactDir = path.join(frontendRoot, "artifacts", "astrbot-review", runId);
  const hermesEvalDir = path.join(projectRoot, "hermes", "eval");
  const hermesNotesPath = path.join(hermesEvalDir, "codex_review_notes.jsonl");
  await mkdir(artifactDir, { recursive: true });
  await mkdir(hermesEvalDir, { recursive: true });

  const reportUrl = `${options.apiBase}/astrbot/eval/business/report?limit=${encodeURIComponent(String(options.limit))}`;
  const report = await fetchJson(reportUrl);
  const records = Array.isArray(report.items) ? report.items.slice(0, options.limit) : [];
  const draftScoreBaseline = buildDraftScoreBaseline(records);
  const repairQueueExpected = repairQueueExpectedFromReport(report);
  const repairQueue = repairQueueFromReport(report);
  const reportSummary = report?.summary && typeof report.summary === "object" ? report.summary : {};
  const reportTotalQuestions = numberValue(reportSummary.count) || numberValue(report?.total);
  const reportBaselineScored = numberValue(reportSummary.baselineScoredCount) || numberValue(reportSummary.scoredCount);
  const reportPendingBaseline = Number.isFinite(reportSummary.pendingBaselineScoring)
    ? numberValue(reportSummary.pendingBaselineScoring)
    : Math.max(0, reportTotalQuestions - reportBaselineScored);
  const reportReplacementBaselineScored = Number.isFinite(reportSummary.replacementBaselineScoredCount)
    ? numberValue(reportSummary.replacementBaselineScoredCount)
    : reportBaselineScored;
  const reportPendingReplacementBaseline = Number.isFinite(reportSummary.pendingReplacementBaselineScoring)
    ? numberValue(reportSummary.pendingReplacementBaselineScoring)
    : Math.max(0, reportTotalQuestions - reportReplacementBaselineScored);
  const reportMinBusinessScores = reportTotalQuestions > 0
    ? Math.min(reportTotalQuestions, Math.max(8, Math.ceil(reportTotalQuestions * 0.7)))
    : 8;
  const reportBaselineSourceLabel = sourceCountsLabel(reportSummary.baselineSourceCounts ?? reportSummary.humanScoreSourceCounts);
  const reportReplacementSourceLabel = sourceCountsLabel(reportSummary.replacementBaselineSourceCounts ?? reportSummary.baselineSourceCounts ?? reportSummary.humanScoreSourceCounts);

  const browser = await chromium.launch({ headless: !options.headed });
  const page = await browser.newPage({ viewport: { width: 1440, height: 980 } });
  const screenshots = [];
  const uiChecks = [];
  const globalUiIssues = [];
  const apiRepairQueueOk = repairQueueExpected
    ? repairQueue.length > 0 && repairQueueHasAction(report)
    : Array.isArray(report.evidenceRepairQueue);
  uiChecks.push({
    label: "Business report API exposes evidence repair queue",
    ok: apiRepairQueueOk,
    detail: repairQueueExpected ? `${repairQueue.length} items` : "not needed",
  });
  if (!apiRepairQueueOk) {
    globalUiIssues.push("Business report API did not expose evidenceRepairQueue with actionable repair items.");
  }
  const displayText = businessReportDisplayTexts(report);
  const leakedInternalMarkers = INTERNAL_REVIEW_MARKERS.filter(marker => displayText.includes(marker));
  uiChecks.push({
    label: "Business report display text hides internal review prose",
    ok: leakedInternalMarkers.length === 0,
    detail: leakedInternalMarkers.length > 0 ? leakedInternalMarkers.join(", ") : "clean",
  });
  if (leakedInternalMarkers.length > 0) {
    globalUiIssues.push(`Business report display text leaked internal review prose: ${leakedInternalMarkers.join(", ")}.`);
  }
  const apiBaselineSourceOk = Number.isFinite(reportSummary.baselineScoredCount)
    && Number.isFinite(reportSummary.pendingBaselineScoring)
    && objectValue(reportSummary.baselineSourceCounts) === reportSummary.baselineSourceCounts
    && Number.isFinite(reportSummary.replacementBaselineScoredCount)
    && Number.isFinite(reportSummary.pendingReplacementBaselineScoring)
    && objectValue(reportSummary.replacementBaselineSourceCounts) === reportSummary.replacementBaselineSourceCounts;
  uiChecks.push({
    label: "Business report API exposes baseline source counts",
    ok: apiBaselineSourceOk,
    detail: `baseline ${reportBaselineScored}/${reportTotalQuestions}; replacement ${reportReplacementBaselineScored}/${reportTotalQuestions}; pending ${reportPendingReplacementBaseline}; ${reportReplacementSourceLabel}`,
  });
  if (!apiBaselineSourceOk) {
    globalUiIssues.push("Business report API missing baseline/replacement source-count fields.");
  }
  const apiReadiness = objectValue(reportSummary.replacementReadiness);
  const apiReadinessOk = typeof apiReadiness.status === "string"
    && typeof apiReadiness.verdict === "string"
    && typeof apiReadiness.replacementReady === "boolean"
    && typeof apiReadiness.businessBaselineReady === "boolean"
    && Number.isFinite(apiReadiness.totalQuestions)
    && Number.isFinite(apiReadiness.minimumRequiredScores)
    && Number.isFinite(apiReadiness.scoredCount)
    && Number.isFinite(apiReadiness.pendingCount)
    && objectValue(apiReadiness.sourceCounts) === apiReadiness.sourceCounts
    && typeof apiReadiness.recommendedNextAction === "string";
  uiChecks.push({
    label: "Business report API exposes replacement readiness object",
    ok: apiReadinessOk,
    detail: apiReadinessOk
      ? `${apiReadiness.status}; ${apiReadiness.scoredCount}/${apiReadiness.totalQuestions}; need ${apiReadiness.minimumRequiredScores}`
      : "missing replacementReadiness fields",
  });
  if (!apiReadinessOk) {
    globalUiIssues.push("Business report API missing replacementReadiness object fields.");
  }
  const apiReferenceMatrix = objectValue(reportSummary.referenceJudgePaths);
  const apiReferencePaths = Array.isArray(apiReferenceMatrix.paths) ? apiReferenceMatrix.paths : [];
  const apiReferenceLabels = apiReferencePaths.map(item => String(item?.label || item?.id || ""));
  const apiReferencePathsOk = apiReferencePaths.length >= 3
    && apiReferenceLabels.some(label => label.includes("GPT5.5"))
    && apiReferenceLabels.some(label => label.includes("Opus"))
    && apiReferenceLabels.some(label => label.includes("Fable"));
  uiChecks.push({
    label: "Business report API exposes reference judge paths",
    ok: apiReferencePathsOk,
    detail: apiReferencePathsOk
      ? apiReferencePaths.map(item => `${item.id || item.label}:${item.status || "unknown"}/${item.readinessStatus || "unknown"}`).join("; ")
      : "missing GPT/Opus/Fable reference path matrix",
  });
  if (!apiReferencePathsOk) {
    globalUiIssues.push("Business report API missing GPT/Opus/Fable reference judge path matrix.");
  }

  try {
    await page.goto(`${options.baseUrl}/astrbot/eval`, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
    const bodyText = await page.locator("body").innerText({ timeout: 10000 });
    const has404 = bodyText.includes("404 Not Found");
    uiChecks.push({ label: "/astrbot/eval is not 404", ok: !has404 });
    if (has404) globalUiIssues.push("/astrbot/eval showed 404.");

    const overflow = await page.evaluate(() => ({
      scrollWidth: document.documentElement.scrollWidth,
      clientWidth: document.documentElement.clientWidth,
    }));
    const noOverflow = overflow.scrollWidth <= overflow.clientWidth + 4;
    uiChecks.push({
      label: "No horizontal overflow on eval page",
      ok: noOverflow,
      detail: `${overflow.scrollWidth}/${overflow.clientWidth}`,
    });
    if (!noOverflow) globalUiIssues.push(`Horizontal overflow detected: ${overflow.scrollWidth}/${overflow.clientWidth}.`);

    const evalPath = path.join(artifactDir, "01_eval_overview.png");
    await page.screenshot({ path: evalPath, fullPage: false });
    screenshots.push({ label: "Eval overview", path: evalPath });

    const businessButton = page.getByRole("button", { name: "Business", exact: true });
    await businessButton.click({ timeout: 10000 });
    await page.getByText("Business Category", { exact: true }).waitFor({ timeout: 10000 });
    if (repairQueueExpected) {
      await page.getByText("Evidence Repair Queue", { exact: true }).waitFor({ timeout: 15000 }).catch(() => undefined);
    }
    const businessText = await page.locator("body").innerText();
    const normalizedBusinessText = businessText.toLowerCase();
    const hasBusinessStats = normalizedBusinessText.includes("business comparisons")
      && normalizedBusinessText.includes("gpt-human agreement");
    uiChecks.push({ label: "Business tab shows comparisons and judge summary", ok: hasBusinessStats });
    if (!hasBusinessStats) globalUiIssues.push("Business tab missing comparison or GPT-Human summary stats.");
    const staleActionMarkers = [
      "next: confirm official source",
      "下一步：确认官方来源",
      "补齐竞品 msrp",
    ].filter(marker => normalizedBusinessText.includes(marker));
    uiChecks.push({
      label: "Business report avoids stale action copy",
      ok: staleActionMarkers.length === 0,
      detail: staleActionMarkers.length > 0 ? staleActionMarkers.join(", ") : "clean",
    });
    if (staleActionMarkers.length > 0) {
      globalUiIssues.push(`Business report still shows stale action copy: ${staleActionMarkers.join(", ")}.`);
    }
    const hasBaselineSourceText = normalizedBusinessText.includes("baseline scored")
      && normalizedBusinessText.includes("replacement baseline")
      && normalizedBusinessText.includes("manual/gpt only")
      && normalizedBusinessText.includes(`need ${reportMinBusinessScores}`)
      && normalizedBusinessText.includes(`pending ${reportPendingReplacementBaseline}`)
      && normalizedBusinessText.includes(reportReplacementSourceLabel.toLowerCase());
    uiChecks.push({
      label: "Business tab shows baseline source and pending counts",
      ok: hasBaselineSourceText,
      detail: `need ${reportMinBusinessScores}; pending ${reportPendingReplacementBaseline}; ${reportReplacementSourceLabel}`,
    });
    if (!hasBaselineSourceText) {
      globalUiIssues.push("Business tab missing replacement baseline, pending baseline, or source-count copy.");
    }
    const selfTestBaseline = reportSummary.selfTestBaseline || {};
    const expectedSelfTestCount = Number(selfTestBaseline.scoredCount || 0);
    const expectedSelfTestTotal = Number(selfTestBaseline.totalQuestions || report.total || 0);
    const hasSelfTestKpi = normalizedBusinessText.includes("codex self-test")
      && normalizedBusinessText.includes(`${expectedSelfTestCount}/${expectedSelfTestTotal}`)
      && normalizedBusinessText.includes("self-test baseline");
    uiChecks.push({
      label: "Business tab separates Codex self-test from replacement baseline",
      ok: hasSelfTestKpi,
      detail: `self-test ${expectedSelfTestCount}/${expectedSelfTestTotal}`,
    });
    if (!hasSelfTestKpi) {
      globalUiIssues.push("Business tab missing Codex self-test KPI or readiness copy separating draft review from replacement baseline.");
    }
    const hasBaselineActionPanel = normalizedBusinessText.includes("baseline next step")
      && normalizedBusinessText.includes("copy tsv for scoring")
      && normalizedBusinessText.includes("codex drafts can prefill");
    uiChecks.push({
      label: "Business tab exposes a baseline next-step action panel",
      ok: hasBaselineActionPanel,
      detail: hasBaselineActionPanel ? "primary action visible" : "missing baseline action copy or shortcut",
    });
    if (!hasBaselineActionPanel) {
      globalUiIssues.push("Business tab missing Baseline next step action panel or scoring shortcuts.");
    }
    const hasDecisionPriorityQueue = normalizedBusinessText.includes("needs decision")
      && normalizedBusinessText.includes("tie / thin / research first")
      && normalizedBusinessText.includes("decision first");
    uiChecks.push({
      label: "Business review exposes decision-priority scoring queue",
      ok: hasDecisionPriorityQueue,
      detail: hasDecisionPriorityQueue ? "needs-decision filter visible" : "missing needs-decision filter or tie/thin/research copy",
    });
    if (!hasDecisionPriorityQueue) {
      globalUiIssues.push("Business review missing decision-priority queue for tie/thin/research-source records.");
    }
    const businessKpiLayout = await page.evaluate(() => {
      const items = [...document.querySelectorAll(".astrbot-business-kpi-grid .astrbot-agent-card--wide strong")];
      const metrics = items.map(element => {
        const parent = element.closest(".astrbot-agent-card");
        const rect = element.getBoundingClientRect();
        const parentRect = parent?.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return {
          text: element.textContent?.trim() || "",
          width: Math.round(rect.width),
          parentWidth: Math.round(parentRect?.width || 0),
          whiteSpace: style.whiteSpace,
          textOverflow: style.textOverflow,
          overflowX: style.overflowX,
          scrollWidth: element.scrollWidth,
          clientWidth: element.clientWidth,
        };
      });
      return {
        count: metrics.length,
        metrics,
        readable: metrics.length >= 2 && metrics.every(item => (
          item.whiteSpace !== "nowrap"
          && item.textOverflow !== "ellipsis"
          && item.width <= item.parentWidth + 2
          && item.scrollWidth <= item.clientWidth + 2
        )),
      };
    });
    uiChecks.push({
      label: "Business KPI long values are readable",
      ok: businessKpiLayout.readable,
      detail: businessKpiLayout.metrics.map(item => `${item.text}:${item.width}/${item.parentWidth}/${item.whiteSpace}`).join("; "),
    });
    if (!businessKpiLayout.readable) globalUiIssues.push("Business KPI long values are still truncated or constrained to a single line.");
    const hasReadinessGate = normalizedBusinessText.includes("replacement gate")
      && normalizedBusinessText.includes("not ready to replace /copilot")
      && normalizedBusinessText.includes("replacement baseline")
      && normalizedBusinessText.includes("judge provider");
    uiChecks.push({ label: "Business tab shows replacement readiness gate", ok: hasReadinessGate });
    if (!hasReadinessGate) globalUiIssues.push("Business tab missing replacement readiness gate.");
    const readinessMetricLayout = await page.evaluate(() => {
      const items = [...document.querySelectorAll(".astrbot-readiness-metrics article strong")]
        .filter(element => (element.textContent?.trim().length || 0) >= 10);
      const metrics = items.map(element => {
        const parent = element.closest("article");
        const rect = element.getBoundingClientRect();
        const parentRect = parent?.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return {
          text: element.textContent?.trim() || "",
          width: Math.round(rect.width),
          parentWidth: Math.round(parentRect?.width || 0),
          whiteSpace: style.whiteSpace,
          textOverflow: style.textOverflow,
          scrollWidth: element.scrollWidth,
          clientWidth: element.clientWidth,
        };
      });
      return {
        count: metrics.length,
        metrics,
        readable: metrics.every(item => (
          item.whiteSpace !== "nowrap"
          && item.textOverflow !== "ellipsis"
          && item.width <= item.parentWidth + 2
          && item.scrollWidth <= item.clientWidth + 2
        )),
      };
    });
    uiChecks.push({
      label: "Readiness gate long verdicts are readable",
      ok: readinessMetricLayout.readable,
      detail: readinessMetricLayout.metrics.map(item => `${item.text}:${item.width}/${item.parentWidth}/${item.whiteSpace}`).join("; ") || "no long verdicts",
    });
    if (!readinessMetricLayout.readable) globalUiIssues.push("Readiness gate long verdicts are still truncated or constrained to a single line.");
    const judgeDetailsState = await page.evaluate(() => {
      const details = document.querySelector(".astrbot-judge-action-panel");
      const summary = details?.querySelector("summary");
      return {
        exists: Boolean(details),
        openBefore: Boolean(details?.hasAttribute("open")),
        summaryText: summary?.textContent?.toLowerCase() || "",
      };
    });
    if (judgeDetailsState.exists) {
      await page.locator(".astrbot-judge-action-panel > summary").click({ timeout: 5000 }).catch(() => undefined);
    }
    const expandedJudgeText = judgeDetailsState.exists
      ? (await page.locator(".astrbot-judge-action-panel").innerText({ timeout: 5000 }).catch(() => "")).toLowerCase()
      : "";
    const hasJudgeActions = judgeDetailsState.exists
      && !judgeDetailsState.openBefore
      && judgeDetailsState.summaryText.includes("judge setup details")
      && expandedJudgeText.includes("dpv4/deepseek is the runtime answer provider")
      && expandedJudgeText.includes("copy judge .env")
      && expandedJudgeText.includes("refresh judge")
      && expandedJudgeText.includes("run 2 judge smoke")
      && expandedJudgeText.includes("manual score queue");
    uiChecks.push({
      label: "Business tab shows collapsed judge baseline actions",
      ok: hasJudgeActions,
      detail: hasJudgeActions ? "summary collapsed by default; actions available after expand" : "missing judge details summary or expanded actions",
    });
    if (!hasJudgeActions) globalUiIssues.push("Business tab missing collapsed judge setup details or baseline actions.");
    const referenceJudgePathLayout = judgeDetailsState.exists
      ? await page.evaluate(() => {
        const grid = document.querySelector(".astrbot-reference-judge-grid");
        const cards = [...document.querySelectorAll(".astrbot-reference-judge-grid article")];
        const rect = grid?.getBoundingClientRect();
        return {
          exists: Boolean(grid),
          cardCount: cards.length,
          text: grid?.textContent?.toLowerCase() || "",
          width: Math.round(rect?.width || 0),
          scrollWidth: grid?.scrollWidth || 0,
          clientWidth: grid?.clientWidth || 0,
        };
      })
      : { exists: false, cardCount: 0, text: "", width: 0, scrollWidth: 0, clientWidth: 0 };
    const hasReferenceJudgePaths = referenceJudgePathLayout.exists
      && referenceJudgePathLayout.cardCount >= 3
      && referenceJudgePathLayout.text.includes("gpt5.5")
      && referenceJudgePathLayout.text.includes("opus 4.8")
      && referenceJudgePathLayout.text.includes("fable 5")
      && referenceJudgePathLayout.text.includes("app_astrbot_judge_model")
      && referenceJudgePathLayout.text.includes("app_astrbot_opus48_judge_model")
      && referenceJudgePathLayout.text.includes("app_astrbot_fable5_judge_model")
      && referenceJudgePathLayout.scrollWidth <= referenceJudgePathLayout.clientWidth + 2;
    uiChecks.push({
      label: "Judge setup shows GPT/Opus/Fable reference paths and env hooks",
      ok: hasReferenceJudgePaths,
      detail: hasReferenceJudgePaths
        ? `cards=${referenceJudgePathLayout.cardCount}; width=${referenceJudgePathLayout.scrollWidth}/${referenceJudgePathLayout.clientWidth}`
        : "missing reference path cards, env hooks, or horizontal overflow",
    });
    if (!hasReferenceJudgePaths) {
      globalUiIssues.push("Judge setup did not show readable GPT/Opus/Fable reference path cards with env hooks.");
    }
    const businessTopFlow = await page.evaluate(() => {
      const rectFor = (selector) => {
        const element = document.querySelector(selector);
        if (!element) return null;
        const rect = element.getBoundingClientRect();
        return { y: Math.round(rect.y), height: Math.round(rect.height) };
      };
      return {
        kpi: rectFor(".astrbot-business-kpi-grid"),
        readiness: rectFor(".astrbot-readiness-gate"),
        judge: rectFor(".astrbot-judge-action-panel"),
        triage: rectFor(".astrbot-codex-triage-panel"),
        review: rectFor(".astrbot-business-review-queue"),
      };
    });
    const businessTopFlowOk = Boolean(
      businessTopFlow.kpi
      && businessTopFlow.readiness
      && businessTopFlow.judge
      && businessTopFlow.triage
      && businessTopFlow.review
      && businessTopFlow.review.y < businessTopFlow.kpi.y
      && businessTopFlow.kpi.y < businessTopFlow.readiness.y
      && businessTopFlow.readiness.y < businessTopFlow.judge.y
      && businessTopFlow.judge.y < businessTopFlow.triage.y,
    );
    uiChecks.push({
      label: "Business tab prioritizes review rows before deep governance panels",
      ok: businessTopFlowOk,
      detail: JSON.stringify(businessTopFlow),
    });
    if (!businessTopFlowOk) {
      globalUiIssues.push("Business tab should show the review queue before KPI/readiness/judge/triage governance details.");
    }
    const hasCodexDraftTriage = normalizedBusinessText.includes("codex draft triage")
      && normalizedBusinessText.includes("review drafts available")
      && normalizedBusinessText.includes("draft only");
    uiChecks.push({ label: "Business tab shows Codex draft triage", ok: hasCodexDraftTriage });
    if (!hasCodexDraftTriage) globalUiIssues.push("Business tab missing Codex Draft Triage guidance.");
    const hotspotExpected = draftScoreBaseline.wins.tie > 0
      || draftScoreBaseline.items.some(item => item.astrbotAverage > 0 && item.astrbotAverage < 4);
    const hasCodexHotspots = !hotspotExpected || (
      normalizedBusinessText.includes("hotspots")
      && normalizedBusinessText.includes("thin evidence")
      && normalizedBusinessText.includes("draft tie")
      && normalizedBusinessText.includes("review hotspot")
    );
    uiChecks.push({
      label: "Business tab shows Codex draft improvement hotspots",
      ok: hasCodexHotspots,
      detail: hotspotExpected ? `ties=${draftScoreBaseline.wins.tie}` : "not needed",
    });
    if (!hasCodexHotspots) {
      globalUiIssues.push("Business tab has draft ties or weak draft scores but no Codex draft improvement hotspot panel.");
    }
    const codexDraftCount = await page.evaluate(() => {
      const text = document.querySelector(".astrbot-codex-triage-copy strong")?.textContent || "";
      const match = text.match(/(\d+)\s*\//);
      return match ? Number(match[1]) : 0;
    });
    const hasReviewWorkbench = normalizedBusinessText.includes("review workbench")
      && normalizedBusinessText.includes("open next draft")
      && normalizedBusinessText.includes("draft ready")
      && normalizedBusinessText.includes("needs score");
    uiChecks.push({ label: "Business tab shows draft-assisted review workbench", ok: hasReviewWorkbench });
    if (!hasReviewWorkbench) globalUiIssues.push("Business tab missing draft-assisted Review Workbench controls.");
    const hasCodexDraftSaveAction = normalizedBusinessText.includes("save codex drafts")
      && normalizedBusinessText.includes("replacement baseline is manual/gpt only");
    uiChecks.push({
      label: "Business tab exposes explicit Codex draft audit save action",
      ok: hasCodexDraftSaveAction,
    });
    if (!hasCodexDraftSaveAction) globalUiIssues.push("Business tab missing explicit Save Codex drafts action or manual/GPT-only replacement copy.");
    const codexAuditPlacement = await page.evaluate(() => {
      const auditDetails = document.querySelector(".astrbot-review-audit-actions");
      const primaryActionTexts = [...document.querySelectorAll(".astrbot-review-workbench-actions button")]
        .map(button => button.textContent?.trim().toLowerCase() || "");
      return {
        hasAuditDetails: Boolean(auditDetails),
        auditDetailsOpen: auditDetails instanceof HTMLDetailsElement ? auditDetails.open : null,
        primaryHasCodexSave: primaryActionTexts.some(text => text.includes("save codex drafts")),
      };
    });
    uiChecks.push({
      label: "Business review keeps Codex save as collapsed audit-only action",
      ok: codexAuditPlacement.hasAuditDetails
        && codexAuditPlacement.auditDetailsOpen === false
        && !codexAuditPlacement.primaryHasCodexSave,
      detail: JSON.stringify(codexAuditPlacement),
    });
    if (!codexAuditPlacement.hasAuditDetails || codexAuditPlacement.primaryHasCodexSave) {
      globalUiIssues.push("Codex draft save should stay in a collapsed audit-only area, not the primary review actions.");
    }
    const hasScoringSheetExport = normalizedBusinessText.includes("copy scoring sheet")
      && normalizedBusinessText.includes("review workbench");
    uiChecks.push({
      label: "Business review exposes copyable scoring sheet",
      ok: hasScoringSheetExport,
    });
    if (!hasScoringSheetExport) globalUiIssues.push("Business tab missing copyable scoring sheet action for manual review.");
    let scoringSheetImportOk = false;
    let scoringSheetImportDetail = "not checked";
    const scoringSheetButton = page.getByRole("button", { name: "Copy scoring sheet", exact: true });
    if (await scoringSheetButton.count() === 1) {
      await scoringSheetButton.click({ timeout: 10000 });
      const sheetFallback = page.locator(".astrbot-review-sheet-fallback");
      await sheetFallback.waitFor({ state: "attached", timeout: 5000 }).catch(() => undefined);
      const rawSheet = await sheetFallback.inputValue({ timeout: 5000 }).catch(() => "");
      const filledSheet = filledScoringSheetSmokeRow(rawSheet);
      const importSummary = page.locator(".astrbot-review-sheet-import summary");
      const importTextarea = page.locator(".astrbot-review-sheet-import textarea");
      if (filledSheet && await importSummary.count() === 1 && await importTextarea.count() === 1) {
        await importSummary.click({ timeout: 10000 });
        const applySheetButton = page.getByRole("button", { name: "Apply sheet draft", exact: true });
        await applySheetButton.waitFor({ state: "visible", timeout: 5000 }).catch(() => undefined);
        const applySheetButtonCount = await applySheetButton.count();
        if (applySheetButtonCount !== 1) {
          scoringSheetImportDetail = `sheet=true summary=1 textarea=1 apply=${applySheetButtonCount}`;
        } else {
        await importTextarea.fill(filledSheet, { timeout: 10000 });
        await applySheetButton.click({ timeout: 10000 });
        const afterImportText = await page.locator("body").innerText({ timeout: 10000 });
        scoringSheetImportOk = afterImportText.includes("Applied 1/1 matched row")
          && afterImportText.includes("Load latest Codex TSV")
          && afterImportText.includes("AstrBot 5")
          && afterImportText.includes("Copilot 3")
          && afterImportText.includes("AstrBot wins")
          && afterImportText.includes("Save imported manual scores (1)");
        scoringSheetImportDetail = scoringSheetImportOk ? "imported one local draft and exposed confirm-gated batch save" : "draft status or batch-save text missing";
        }
      } else {
        scoringSheetImportDetail = `sheet=${Boolean(filledSheet)} summary=${await importSummary.count()} textarea=${await importTextarea.count()}`;
      }
    } else {
      scoringSheetImportDetail = "Copy scoring sheet button missing";
    }
    uiChecks.push({
      label: "Business review imports filled scoring sheet as local draft",
      ok: scoringSheetImportOk,
      detail: scoringSheetImportDetail,
    });
    if (!scoringSheetImportOk) globalUiIssues.push("Business review could not import a filled scoring sheet as a local draft.");
    let referenceJudgeImportOk = false;
    let referenceJudgeImportDetail = "not checked";
    const referenceJudgeSummary = page.locator(".astrbot-review-reference-import summary");
    const referenceJudgeTextarea = page.locator(".astrbot-review-reference-import textarea");
    const judgeImportRecord = records.find((record, index) => {
      const scoring = record?.humanScoring || {};
      return index > 0 && (scoring.status !== "scored" || !["manual", "llm_judge"].includes(String(scoring.source || "")));
    }) || records[0];
    if (judgeImportRecord && await referenceJudgeSummary.count() === 1 && await referenceJudgeTextarea.count() === 1) {
      await referenceJudgeSummary.click({ timeout: 10000 });
      const applyJudgeButton = page.getByRole("button", { name: "Apply judge JSON", exact: true });
      const loadJudgePacketButton = page.getByRole("button", { name: "Load latest judge packet", exact: true });
      await applyJudgeButton.waitFor({ state: "visible", timeout: 5000 }).catch(() => undefined);
      const applyJudgeButtonCount = await applyJudgeButton.count();
      const loadJudgePacketButtonCount = await loadJudgePacketButton.count();
      if (applyJudgeButtonCount !== 1 || loadJudgePacketButtonCount !== 1) {
        referenceJudgeImportDetail = `summary=1 textarea=1 load=${loadJudgePacketButtonCount} apply=${applyJudgeButtonCount}`;
      } else {
        await loadJudgePacketButton.click({ timeout: 10000 });
        await page.waitForFunction(() => {
          const textarea = document.querySelector(".astrbot-review-reference-import textarea");
          return textarea instanceof HTMLTextAreaElement
            && textarea.value.includes("AstrBot Reference Judge Packet");
        }, undefined, { timeout: 10000 }).catch(() => undefined);
        const loadedPacketText = await referenceJudgeTextarea.inputValue({ timeout: 10000 }).catch(() => "");
        const judgeJson = JSON.stringify({
          source: "gpt5_5_reference_judge_smoke",
          records: [{
            questionId: judgeImportRecord.questionId,
            winner: "astrbot",
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
              toolSelection: 3,
              grounding: 3,
              pmInsight: 3,
              actionability: 3,
              artifactQuality: 2,
              followUpValue: 2,
              presentationReadiness: 3,
            },
            failureTags: [],
            notes: "Smoke import only; do not save without reviewer acceptance.",
          }],
        }, null, 2);
        await referenceJudgeTextarea.fill(judgeJson, { timeout: 10000 });
        await applyJudgeButton.click({ timeout: 10000 });
        await page.waitForFunction(() => document.body.innerText.includes("Applied 1/1 judged row"), undefined, { timeout: 10000 }).catch(() => undefined);
        const afterJudgeImportText = await page.locator("body").innerText({ timeout: 10000 });
        referenceJudgeImportOk = loadedPacketText.includes("AstrBot Reference Judge Packet")
          && afterJudgeImportText.includes("Applied 1/1 judged row")
          && afterJudgeImportText.includes("source gpt5_5_reference_judge_smoke")
          && afterJudgeImportText.includes("Save reference judge scores (1)");
        referenceJudgeImportDetail = referenceJudgeImportOk ? "loaded latest packet, then imported one reference judge local draft and exposed confirm-gated llm_judge save" : "packet load, judge draft status, or batch-save text missing";
      }
    } else {
      referenceJudgeImportDetail = `record=${Boolean(judgeImportRecord)} summary=${await referenceJudgeSummary.count()} textarea=${await referenceJudgeTextarea.count()}`;
    }
    uiChecks.push({
      label: "Business review imports reference judge JSON as local llm_judge draft",
      ok: referenceJudgeImportOk,
      detail: referenceJudgeImportDetail,
    });
    if (!referenceJudgeImportOk) globalUiIssues.push("Business review could not import reference judge JSON as a local llm_judge draft.");
    const hasRepairQueue = normalizedBusinessText.includes("evidence repair queue");
    uiChecks.push({
      label: "Business tab shows evidence repair queue when data gaps exist",
      ok: repairQueueExpected ? hasRepairQueue : true,
      detail: repairQueueExpected ? (hasRepairQueue ? "visible" : "missing") : "not needed",
    });
    if (repairQueueExpected && !hasRepairQueue) globalUiIssues.push("Business tab missing evidence repair queue despite current evidence gaps.");
    const reasonExpected = records.some(record => (
      Array.isArray(record?.astrbot?.missingEvidence)
      && record.astrbot.missingEvidence.some(item => item && typeof item.reason === "string" && item.reason.trim())
    ));
    const hasRepairReasons = normalizedBusinessText.includes("why blocked")
      && (
        normalizedBusinessText.includes("coverage_diagnostic")
        || normalizedBusinessText.includes("current_prices table")
        || normalizedBusinessText.includes("no current price")
      );
    uiChecks.push({
      label: "Evidence repair queue explains why records are blocked",
      ok: reasonExpected ? hasRepairReasons : true,
      detail: reasonExpected ? (hasRepairReasons ? "visible" : "missing") : "not needed",
    });
    if (reasonExpected && !hasRepairReasons) globalUiIssues.push("Evidence repair queue missing repair reasons despite missingEvidence reason text.");
    const hasCopyRepairPlan = normalizedBusinessText.includes("copy repair plan");
    uiChecks.push({
      label: "Evidence repair queue exposes copyable repair plan",
      ok: repairQueueExpected ? hasCopyRepairPlan : true,
      detail: repairQueueExpected ? (hasCopyRepairPlan ? "visible" : "missing") : "not needed",
    });
    if (repairQueueExpected && !hasCopyRepairPlan) globalUiIssues.push("Evidence repair queue missing Copy Repair Plan action.");
    const businessPath = path.join(artifactDir, "02_business_tab.png");
    await page.screenshot({ path: businessPath, fullPage: false });
    screenshots.push({ label: "Business tab", path: businessPath });

    const openNextDraftButton = page.getByRole("button", { name: "Open next draft", exact: true });
    const openNextDraftCount = await openNextDraftButton.count();
    const businessReviewButtons = page.locator(".astrbot-business-review-queue .astrbot-row-toggle");
    const businessReviewButtonCount = await businessReviewButtons.count();
    if (openNextDraftCount === 1) {
      await openNextDraftButton.click({ timeout: 10000 });
    } else if (businessReviewButtonCount > 0) {
      await businessReviewButtons.first().click({ timeout: 10000 });
    }
    const businessDetailOpened = openNextDraftCount === 1 || businessReviewButtonCount > 0;
    const businessReviewLayout = businessDetailOpened
      ? await page.evaluate(() => {
        const scorePanel = document.querySelector(".astrbot-business-review-queue .astrbot-score-panel");
        const firstAnswer = document.querySelector(".astrbot-business-review-queue .astrbot-answer-panel");
        const advanced = document.querySelector(".astrbot-business-review-queue .astrbot-score-advanced-details");
        const presetStrip = document.querySelector(".astrbot-business-review-queue .astrbot-score-preset-strip");
        const scoreHeaderText = scorePanel?.querySelector("header")?.textContent || "";
        const sideScoreCards = [...document.querySelectorAll(".astrbot-business-review-queue .astrbot-score-side-card")];
        const scoreButtons = [...document.querySelectorAll(".astrbot-business-review-queue .astrbot-score-side-card button")];
        const visibleAdvancedInputs = advanced
          ? [...advanced.querySelectorAll("input, select, textarea")].filter(element => {
            const rect = element.getBoundingClientRect();
            const style = window.getComputedStyle(element);
            return rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden";
          }).length
          : 0;
        const rectFor = (element) => {
          if (!element) return null;
          const rect = element.getBoundingClientRect();
          return {
            x: Math.round(rect.x),
            y: Math.round(rect.y),
            width: Math.round(rect.width),
            height: Math.round(rect.height),
          };
        };
        return {
          score: rectFor(scorePanel),
          firstAnswer: rectFor(firstAnswer),
          sideScoreCardCount: sideScoreCards.length,
          scoreButtonCount: scoreButtons.length,
          scoreHeaderText,
          presetStripVisible: Boolean(presetStrip),
          presetStripText: presetStrip?.textContent || "",
          advancedDetailsPresent: Boolean(advanced),
          advancedDetailsOpen: Boolean(advanced?.hasAttribute("open")),
          visibleAdvancedInputs,
        };
      })
      : null;
    const businessOneClickScoringOk = Boolean(
      businessReviewLayout
      && businessReviewLayout.sideScoreCardCount >= 2
      && businessReviewLayout.scoreButtonCount >= 10
      && businessReviewLayout.score
      && businessReviewLayout.firstAnswer
      && businessReviewLayout.firstAnswer.y < businessReviewLayout.score.y
      && (businessReviewLayout.scoreHeaderText.includes("Business score") || businessReviewLayout.scoreHeaderText.includes("Quick total score"))
      && businessReviewLayout.scoreHeaderText.includes("总分")
      && businessReviewLayout.presetStripVisible
      && businessReviewLayout.presetStripText.includes("One-click pair")
    );
    uiChecks.push({
      label: "Business review detail shows answers before one-click scoring",
      ok: businessOneClickScoringOk,
      detail: businessReviewLayout
        ? `cards=${businessReviewLayout.sideScoreCardCount}; buttons=${businessReviewLayout.scoreButtonCount}; preset=${businessReviewLayout.presetStripVisible}; answer@${businessReviewLayout.firstAnswer?.y ?? "n/a"}; score@${businessReviewLayout.score?.y ?? "n/a"}; header=${businessReviewLayout.scoreHeaderText.replace(/\s+/g, " ").trim()}`
        : "no review detail opened",
    });
    if (!businessOneClickScoringOk) {
      globalUiIssues.push("Business review detail did not show answer content before the one-click score controls.");
    }
    const businessAdvancedClosedOk = Boolean(
      businessReviewLayout
      && businessReviewLayout.advancedDetailsPresent
      && !businessReviewLayout.advancedDetailsOpen
      && businessReviewLayout.visibleAdvancedInputs === 0
    );
    uiChecks.push({
      label: "Business review advanced dimensions stay collapsed by default",
      ok: businessAdvancedClosedOk,
      detail: businessReviewLayout
        ? `open=${businessReviewLayout.advancedDetailsOpen}; visibleInputs=${businessReviewLayout.visibleAdvancedInputs}`
        : "no review detail opened",
    });
    if (!businessAdvancedClosedOk) {
      globalUiIssues.push("Business review advanced dimension inputs are visible before the reviewer opens Advanced dimension overrides.");
    }
    await page.setViewportSize({ width: 390, height: 844 });
    await page.waitForTimeout(300);
    const businessMobileReviewLayout = await page.evaluate(() => {
      const bodyText = document.body.innerText || "";
      const viewportWidth = document.documentElement.clientWidth;
      const scoreButtons = [...document.querySelectorAll(".astrbot-business-review-queue .astrbot-score-side-card button")]
        .map(button => (button.textContent || "").replace(/\s+/g, "").trim())
        .filter(text => /^(1Risky|2Weak|3Tie|4Better|5Ready)$/.test(text));
      const rectFor = (selector) => {
        const element = document.querySelector(selector);
        if (!element) return null;
        const rect = element.getBoundingClientRect();
        return {
          x: Math.round(rect.x),
          right: Math.round(rect.right),
          width: Math.round(rect.width),
        };
      };
      const elements = [
        [".astrbot-eval-business", "business"],
        [".astrbot-business-review-queue", "reviewQueue"],
        [".astrbot-business-review-queue .astrbot-compare-table", "table"],
        [".astrbot-business-review-queue .astrbot-compare-detail-row", "detail"],
        [".astrbot-business-review-queue .astrbot-answer-panel", "answer"],
        [".astrbot-business-review-queue .astrbot-score-panel", "score"],
      ].map(([selector, label]) => ({ label, rect: rectFor(selector) }));
      const overflowing = elements.filter(item => (
        item.rect && (item.rect.right > viewportWidth + 4 || item.rect.width > viewportWidth + 4)
      ));
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: viewportWidth,
        bodyScrollWidth: document.body.scrollWidth,
        bodyClientWidth: document.body.clientWidth,
        hasAstrBotAnswer: bodyText.includes("AstrBot Answer"),
        hasCountryCopilotAnswer: bodyText.includes("CountryCopilot Answer"),
        hasRepairHint: bodyText.includes("Hint:"),
        scoreButtonCount: scoreButtons.length,
        elements,
        overflowing,
      };
    });
    const businessMobileReviewFits = businessDetailOpened
      && businessMobileReviewLayout.scrollWidth <= businessMobileReviewLayout.clientWidth + 4
      && businessMobileReviewLayout.bodyScrollWidth <= businessMobileReviewLayout.bodyClientWidth + 4
      && businessMobileReviewLayout.overflowing.length === 0
      && businessMobileReviewLayout.hasAstrBotAnswer
      && businessMobileReviewLayout.hasCountryCopilotAnswer
      && businessMobileReviewLayout.hasRepairHint
      && businessMobileReviewLayout.scoreButtonCount >= 10;
    uiChecks.push({
      label: "Mobile Business review detail and repair hint fit viewport",
      ok: businessMobileReviewFits,
      detail: `${businessMobileReviewLayout.scrollWidth}/${businessMobileReviewLayout.clientWidth}; body=${businessMobileReviewLayout.bodyScrollWidth}/${businessMobileReviewLayout.bodyClientWidth}; buttons=${businessMobileReviewLayout.scoreButtonCount}; hint=${businessMobileReviewLayout.hasRepairHint}`,
    });
    if (!businessMobileReviewFits) {
      const overflowLabels = businessMobileReviewLayout.overflowing.map(item => item.label).join(", ") || "missing answer, hint, or score controls";
      globalUiIssues.push(`Mobile Business review detail overflow detected: ${overflowLabels}.`);
    }
    const businessMobileReviewPath = path.join(artifactDir, "02b_business_mobile_review.png");
    await page.screenshot({ path: businessMobileReviewPath, fullPage: false });
    screenshots.push({ label: "Business mobile review", path: businessMobileReviewPath });
    await page.setViewportSize({ width: 1440, height: 980 });
    await page.waitForTimeout(300);

    const calibrationButton = page.getByRole("button", { name: "Judge Calibration", exact: true });
    await calibrationButton.click({ timeout: 10000 });
    await page.getByText("Mismatch only", { exact: true }).waitFor({ timeout: 10000 });
    const calibrationText = await page.locator("body").innerText();
    const normalizedCalibrationText = calibrationText.toLowerCase();
    const hasCalibration = normalizedCalibrationText.includes("strict agreement")
      && normalizedCalibrationText.includes("weighted agreement");
    uiChecks.push({ label: "Judge Calibration tab opens", ok: hasCalibration });
    if (!hasCalibration) globalUiIssues.push("Judge Calibration tab did not show agreement cards.");
    const calibrationPath = path.join(artifactDir, "03_judge_calibration.png");
    await page.screenshot({ path: calibrationPath, fullPage: false });
    screenshots.push({ label: "Judge Calibration", path: calibrationPath });

	    await page.goto(`${options.baseUrl}/astrbot/eval`, { waitUntil: "domcontentloaded", timeout: 30000 });
	    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
	    const compareButton = page.getByRole("button", { name: "Compare", exact: true });
	    await compareButton.click({ timeout: 10000 });
	    await page.getByText("Comparisons", { exact: true }).waitFor({ timeout: 10000 }).catch(() => undefined);
	    const detailRows = page.locator(".astrbot-compare-detail-row");
	    const initialDetailCount = await detailRows.count();
	    uiChecks.push({
	      label: "Side-by-side starts with no expanded review rows",
	      ok: initialDetailCount === 0,
	      detail: String(initialDetailCount),
	    });
	    if (initialDetailCount !== 0) {
	      globalUiIssues.push(`Compare tab opened with ${initialDetailCount} expanded detail rows; the review queue should stay compact until a row is selected.`);
	    }
	    const reviewButtons = page.locator(".astrbot-row-toggle");
	    const reviewButtonCount = await reviewButtons.count();
	    if (reviewButtonCount > 0) {
	      await reviewButtons.first().click({ timeout: 10000 });
	    }
	    const detailCount = await detailRows.count();
	    uiChecks.push({
	      label: "Side-by-side opens one focused detail row on demand",
	      ok: reviewButtonCount > 0 && detailCount === 1,
	      detail: `${detailCount}/${reviewButtonCount} review buttons`,
	    });
	    if (reviewButtonCount === 0) globalUiIssues.push("No Review buttons were visible in Compare tab.");
	    if (reviewButtonCount > 0 && detailCount !== 1) globalUiIssues.push(`Compare tab opened ${detailCount} detail rows after selecting one Review button.`);
		    const reviewLayout = await page.evaluate(() => {
		      const score = document.querySelector(".astrbot-score-panel");
		      const firstAnswer = document.querySelector(".astrbot-answer-panel");
		      const secondAnswer = document.querySelectorAll(".astrbot-answer-panel")[1];
		      const presetStrip = document.querySelector(".astrbot-score-preset-strip");
		      const codexButtons = [...document.querySelectorAll(".astrbot-codex-score-banner button")]
		        .map(button => button.textContent?.toLowerCase() || "");
		      const codexBannerText = document.querySelector(".astrbot-codex-score-banner")?.textContent?.toLowerCase() || "";
		      const codexBannerVisible = Boolean(codexBannerText.trim());
		      const codexPrefillVisible = codexButtons.some(text => text.includes("prefill"));
		      const codexAcceptVisible = codexButtons.some(text => text.includes("save draft"));
		      const codexAuditHintVisible = codexBannerText.includes("audit note");
		      const saveNext = [...document.querySelectorAll(".astrbot-score-actions button")]
		        .some(button => button.textContent?.toLowerCase().includes("save & next"));
		      const detailChildren = [...document.querySelectorAll(".astrbot-compare-detail-grid > *")];
			      const rectFor = (element) => {
		        if (!element) return null;
		        const rect = element.getBoundingClientRect();
        return {
          x: Math.round(rect.x),
          y: Math.round(rect.y),
          width: Math.round(rect.width),
          height: Math.round(rect.height),
        };
      };
		      return {
		        score: rectFor(score),
		        firstAnswer: rectFor(firstAnswer),
		        secondAnswer: rectFor(secondAnswer),
		        presetStripVisible: Boolean(presetStrip),
		        presetStripText: presetStrip?.textContent || "",
		        codexBannerVisible,
		        codexPrefillVisible,
		        codexAcceptVisible,
			        codexAuditHintVisible,
			        saveNextVisible: saveNext,
			        detailOverflowX: document.documentElement.scrollWidth > document.documentElement.clientWidth,
			      };
			    });
			    const answerFirstScoreBarOk = Boolean(
			      reviewLayout.score
			      && reviewLayout.firstAnswer
			      && reviewLayout.secondAnswer
			      && reviewLayout.firstAnswer.y < reviewLayout.score.y
			      && Math.abs(reviewLayout.firstAnswer.y - reviewLayout.secondAnswer.y) <= 24
			      && reviewLayout.secondAnswer.x > reviewLayout.firstAnswer.x
			      && reviewLayout.score.width >= (reviewLayout.firstAnswer.width + reviewLayout.secondAnswer.width) * 0.92
			      && reviewLayout.presetStripVisible
			      && reviewLayout.presetStripText.includes("One-click pair")
			      && !reviewLayout.detailOverflowX,
			    );
			    uiChecks.push({
			      label: "Review detail shows side-by-side answers before compact score bar",
			      ok: answerFirstScoreBarOk,
			      detail: reviewLayout.score && reviewLayout.firstAnswer
			        ? `answer=${reviewLayout.firstAnswer.x},${reviewLayout.firstAnswer.y},${reviewLayout.firstAnswer.width}; second=${reviewLayout.secondAnswer?.x},${reviewLayout.secondAnswer?.y},${reviewLayout.secondAnswer?.width}; score=${reviewLayout.score.x},${reviewLayout.score.y},${reviewLayout.score.width}; preset=${reviewLayout.presetStripVisible}; overflow=${reviewLayout.detailOverflowX}`
			        : "missing score or answer panel",
			    });
		    if (!answerFirstScoreBarOk) {
		      globalUiIssues.push("Compare review detail did not show side-by-side answer panels before the compact score controls.");
		    }
	    const currentRowHasCodexDraft = Boolean(reviewLayout.codexBannerVisible);
	    const codexPrefillOk = codexDraftCount === 0 || !currentRowHasCodexDraft || reviewLayout.codexPrefillVisible;
	    uiChecks.push({
	      label: "Review detail exposes Codex draft prefill when the opened row has a draft",
	      ok: codexPrefillOk,
	      detail: `drafts=${codexDraftCount}; rowDraft=${currentRowHasCodexDraft}; prefill=${reviewLayout.codexPrefillVisible}`,
	    });
	    if (!codexPrefillOk) {
	      globalUiIssues.push("Review detail has a Codex draft for the opened row but no visible prefill draft scores action.");
	    }
	    const codexAcceptOk = codexDraftCount === 0 || !currentRowHasCodexDraft || reviewLayout.codexAcceptVisible;
	    uiChecks.push({
	      label: "Review detail exposes explicit Save draft & next action when the opened row has a draft",
	      ok: codexAcceptOk,
	      detail: `drafts=${codexDraftCount}; rowDraft=${currentRowHasCodexDraft}; accept=${reviewLayout.codexAcceptVisible}`,
	    });
	    if (!codexAcceptOk) {
	      globalUiIssues.push("Review detail has a Codex draft for the opened row but no explicit Save draft & next action.");
	    }
	    const codexAuditHintOk = codexDraftCount === 0 || !currentRowHasCodexDraft || reviewLayout.codexAuditHintVisible;
	    uiChecks.push({
	      label: "Review detail explains accepted drafts write an audit note when the opened row has a draft",
	      ok: codexAuditHintOk,
	      detail: `drafts=${codexDraftCount}; rowDraft=${currentRowHasCodexDraft}; auditHint=${reviewLayout.codexAuditHintVisible}`,
	    });
	    if (!codexAuditHintOk) {
	      globalUiIssues.push("Review detail has a Save draft action for the opened row but does not explain that saved drafts write an audit note.");
	    }
	    uiChecks.push({
	      label: "Review detail exposes Save & next for manual scoring flow",
	      ok: reviewLayout.saveNextVisible,
	      detail: `saveNext=${reviewLayout.saveNextVisible}`,
	    });
	    if (!reviewLayout.saveNextVisible) {
	      globalUiIssues.push("Review detail missing Save & next action for efficient manual scoring.");
	    }

    const notes = [];
    for (let index = 0; index < records.length; index += 1) {
      const record = records[index];
      const recordScreenshots = [];
      const detailPath = path.join(artifactDir, `record_${String(index + 1).padStart(2, "0")}_${safeName(record.questionId)}.png`);
      const captured = index < detailCount
        ? await screenshotIfVisible(detailRows.nth(index), detailPath)
        : false;
      if (captured) {
        recordScreenshots.push(detailPath);
        screenshots.push({ label: `Record ${index + 1} ${record.questionId}`, path: detailPath });
      }
      notes.push(buildReviewNote(record, recordScreenshots, globalUiIssues));
    }

    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${options.baseUrl}/astrbot/eval`, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
    await page.getByRole("button", { name: "Compare", exact: true }).click({ timeout: 10000 });
    await page.getByText("Comparisons", { exact: true }).waitFor({ timeout: 10000 }).catch(() => undefined);
    const mobileReviewButtons = page.locator(".astrbot-row-toggle");
    const mobileReviewButtonCount = await mobileReviewButtons.count();
    if (mobileReviewButtonCount > 0) {
      await mobileReviewButtons.first().click({ timeout: 10000 });
    }
    const mobileReviewLayout = await page.evaluate(() => {
      const viewportWidth = document.documentElement.clientWidth;
      const rectFor = (selector) => {
        const element = document.querySelector(selector);
        if (!element) return null;
        const rect = element.getBoundingClientRect();
        return {
          x: Math.round(rect.x),
          right: Math.round(rect.right),
          width: Math.round(rect.width),
        };
      };
      const elements = [
        [".astrbot-compare-table", "table"],
        [".astrbot-compare-detail-row", "detail"],
        [".astrbot-compare-detail-grid", "grid"],
        [".astrbot-answer-panel", "answer"],
        [".astrbot-score-panel", "score"],
      ].map(([selector, label]) => ({ label, rect: rectFor(selector) }));
      const overflowing = elements.filter(item => (
        item.rect && (item.rect.right > viewportWidth + 4 || item.rect.width > viewportWidth + 4)
      ));
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: viewportWidth,
        elements,
        overflowing,
      };
    });
    const mobileReviewFits = mobileReviewButtonCount > 0
      && mobileReviewLayout.scrollWidth <= mobileReviewLayout.clientWidth + 4
      && mobileReviewLayout.overflowing.length === 0
      && mobileReviewLayout.elements.every(item => item.rect && item.rect.width > 0);
    uiChecks.push({
      label: "Mobile side-by-side review detail fits viewport",
      ok: mobileReviewFits,
      detail: `${mobileReviewLayout.scrollWidth}/${mobileReviewLayout.clientWidth}; ${mobileReviewLayout.elements.map(item => `${item.label}=${item.rect?.width || 0}`).join(", ")}`,
    });
    if (!mobileReviewFits) {
      const overflowLabels = mobileReviewLayout.overflowing.map(item => item.label).join(", ") || "missing or oversized review panels";
      globalUiIssues.push(`Mobile side-by-side review detail overflow detected: ${overflowLabels}.`);
    }
    const mobileComparePath = path.join(artifactDir, "04_mobile_compare_review.png");
    await page.screenshot({ path: mobileComparePath, fullPage: false });
    screenshots.push({ label: "Mobile compare review", path: mobileComparePath });

    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`${options.baseUrl}/astrbot`, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
    const userDesktop = await page.evaluate(() => {
      const bodyText = document.body.innerText || "";
      const rectFor = (selector) => {
        const element = document.querySelector(selector);
        if (!element) return null;
        const rect = element.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return {
          x: Math.round(rect.x),
          y: Math.round(rect.y),
          right: Math.round(rect.right),
          bottom: Math.round(rect.bottom),
          width: Math.round(rect.width),
          height: Math.round(rect.height),
          position: style.position,
          display: style.display,
          visible: rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden",
        };
      };
      return {
        has404: /404\s*not\s*found/i.test(bodyText),
        hasUserTitle: bodyText.includes("JATO AstrBot"),
        hasDeveloperTitle: bodyText.includes("Developer AstrBot Mode"),
        hasQualityLoop: bodyText.includes("QUALITY LOOP") || bodyText.includes("Quality Loop"),
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
        chatThread: rectFor(".astrbot-agent-surface.is-user .astrbot-chat-thread"),
        input: rectFor(".astrbot-agent-surface.is-user .astrbot-agent-input"),
        textarea: rectFor(".astrbot-agent-surface.is-user textarea"),
        send: rectFor(".astrbot-agent-surface.is-user .astrbot-primary-action"),
        sessions: rectFor(".astrbot-user-sessions"),
        context: rectFor(".astrbot-user-context-bar"),
        statusStrip: rectFor(".astrbot-status-strip"),
      };
    });
    const userDesktopNoOverflow = userDesktop.scrollWidth <= userDesktop.clientWidth + 4;
    const userDesktopComposerVisible = Boolean(
      userDesktop.input?.visible
      && userDesktop.textarea?.visible
      && userDesktop.send?.visible
      && userDesktop.input.position !== "fixed"
      && userDesktop.input.bottom <= 900 + 4
    );
    const userDesktopChatShellVisible = Boolean(
      userDesktop.chatThread?.visible
      && userDesktop.chatThread.height >= 360
      && userDesktop.sessions?.visible
      && userDesktop.context?.visible
      && !userDesktop.statusStrip?.visible
    );
    const userDesktopProductMode = Boolean(
      !userDesktop.has404
      && userDesktop.hasUserTitle
      && !userDesktop.hasDeveloperTitle
      && !userDesktop.hasQualityLoop
    );
    uiChecks.push({
      label: "User /astrbot desktop opens as chat product, not debug console",
      ok: userDesktopNoOverflow && userDesktopComposerVisible && userDesktopChatShellVisible && userDesktopProductMode,
      detail: `${userDesktop.scrollWidth}/${userDesktop.clientWidth}; chat=${userDesktop.chatThread?.width || 0}x${userDesktop.chatThread?.height || 0}; composer=${userDesktop.input?.position || "missing"}@${userDesktop.input?.y || 0}+${userDesktop.input?.height || 0}`,
    });
    if (!userDesktopNoOverflow) globalUiIssues.push(`User /astrbot desktop overflow detected: ${userDesktop.scrollWidth}/${userDesktop.clientWidth}.`);
    if (!userDesktopComposerVisible) globalUiIssues.push("User /astrbot desktop composer was not visible in the chat shell.");
    if (!userDesktopChatShellVisible) globalUiIssues.push("User /astrbot desktop did not show the expected chat thread, session rail, and context bar without developer status cards.");
    if (!userDesktopProductMode) globalUiIssues.push("User /astrbot desktop still exposed developer/debug-console chrome.");
    const userDesktopPath = path.join(artifactDir, "05_user_desktop_chat.png");
    await page.screenshot({ path: userDesktopPath, fullPage: false });
    screenshots.push({ label: "User desktop chat", path: userDesktopPath });

    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(`${options.baseUrl}/astrbot`, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
    const userMobile = await page.evaluate(() => {
      const rectFor = (selector) => {
        const element = document.querySelector(selector);
        if (!element) return null;
        const rect = element.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return {
          x: Math.round(rect.x),
          y: Math.round(rect.y),
          width: Math.round(rect.width),
          height: Math.round(rect.height),
          position: style.position,
          display: style.display,
          visible: rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden",
        };
      };
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
        sessions: rectFor(".astrbot-user-sessions"),
        context: rectFor(".astrbot-user-context-bar"),
        input: rectFor(".astrbot-agent-surface.is-user .astrbot-agent-input"),
        textarea: rectFor(".astrbot-agent-surface.is-user textarea"),
        send: rectFor(".astrbot-agent-surface.is-user .astrbot-primary-action"),
      };
    });
    const userMobileNoOverflow = userMobile.scrollWidth <= userMobile.clientWidth + 4;
    const userMobileComposerVisible = Boolean(
      userMobile.input?.visible
      && userMobile.textarea?.visible
      && userMobile.send?.visible
      && userMobile.input.position === "fixed"
      && userMobile.input.y + userMobile.input.height <= 844 + 4
      && userMobile.input.y >= 560,
    );
    const userMobileSessionsContained = Boolean(
      userMobile.sessions?.visible
      && userMobile.sessions.height <= 260
      && userMobile.context?.visible,
    );
    uiChecks.push({
      label: "User /astrbot mobile composer stays visible",
      ok: userMobileNoOverflow && userMobileComposerVisible && userMobileSessionsContained,
      detail: `${userMobile.scrollWidth}/${userMobile.clientWidth}; composer=${userMobile.input?.position || "missing"}@${userMobile.input?.y || 0}+${userMobile.input?.height || 0}; sessions=${userMobile.sessions?.height || 0}`,
    });
    if (!userMobileNoOverflow) globalUiIssues.push(`User /astrbot mobile overflow detected: ${userMobile.scrollWidth}/${userMobile.clientWidth}.`);
    if (!userMobileComposerVisible) globalUiIssues.push("User /astrbot mobile composer was not fixed and visible.");
    if (!userMobileSessionsContained) globalUiIssues.push("User /astrbot mobile conversation list was too tall or context bar was hidden.");
    const userMobilePath = path.join(artifactDir, "06_user_mobile_chat.png");
    await page.screenshot({ path: userMobilePath, fullPage: false });
    screenshots.push({ label: "User mobile chat", path: userMobilePath });

    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto(`${options.baseUrl}/astrbot/dev`, { waitUntil: "domcontentloaded", timeout: 30000 });
    await page.waitForLoadState("networkidle", { timeout: 10000 }).catch(() => undefined);
    const devMode = await page.evaluate(() => {
      const bodyText = document.body.innerText || "";
      const rectFor = (selector) => {
        const element = document.querySelector(selector);
        if (!element) return null;
        const rect = element.getBoundingClientRect();
        const style = window.getComputedStyle(element);
        return {
          x: Math.round(rect.x),
          y: Math.round(rect.y),
          width: Math.round(rect.width),
          height: Math.round(rect.height),
          display: style.display,
          visible: rect.width > 0 && rect.height > 0 && style.display !== "none" && style.visibility !== "hidden",
        };
      };
      return {
        has404: /404\s*not\s*found/i.test(bodyText),
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
        labels: {
          runtime: bodyText.includes("AGENT RUNTIME") || bodyText.includes("Agent Runtime"),
          tools: bodyText.includes("JATO TOOLS") || bodyText.includes("JATO Tools"),
          provider: bodyText.includes("PROVIDER") || bodyText.includes("Provider"),
          profile: bodyText.includes("PROFILE") || bodyText.includes("Profile"),
          boundary: bodyText.includes("DATA BOUNDARY") || bodyText.includes("Data Boundary"),
          quality: bodyText.includes("QUALITY LOOP") || bodyText.includes("Quality Loop"),
        },
        statusStrip: rectFor(".astrbot-status-strip"),
        qualityDeck: rectFor(".astrbot-dev-quality-deck"),
        chatThread: rectFor(".astrbot-chat-thread"),
        input: rectFor(".astrbot-agent-input"),
      };
    });
    const devNoOverflow = devMode.scrollWidth <= devMode.clientWidth + 4;
    const devHasCoreLabels = Object.values(devMode.labels).every(Boolean);
    const devCorePanelsVisible = Boolean(
      devMode.statusStrip?.visible
      && devMode.qualityDeck?.visible
      && devMode.chatThread?.visible
      && devMode.input?.visible,
    );
    uiChecks.push({
      label: "Developer /astrbot/dev shows quality cockpit",
      ok: !devMode.has404 && devNoOverflow && devHasCoreLabels && devCorePanelsVisible,
      detail: `${devMode.scrollWidth}/${devMode.clientWidth}; labels=${Object.entries(devMode.labels).filter(([, ok]) => ok).length}/6`,
    });
    if (devMode.has404) globalUiIssues.push("/astrbot/dev showed 404.");
    if (!devNoOverflow) globalUiIssues.push(`Developer /astrbot/dev overflow detected: ${devMode.scrollWidth}/${devMode.clientWidth}.`);
    if (!devHasCoreLabels) globalUiIssues.push("Developer /astrbot/dev was missing core quality cockpit labels.");
    if (!devCorePanelsVisible) globalUiIssues.push("Developer /astrbot/dev was missing status, quality, chat, or input panels.");
    const devModePath = path.join(artifactDir, "07_developer_mode.png");
    await page.screenshot({ path: devModePath, fullPage: false });
    screenshots.push({ label: "Developer mode", path: devModePath });

    const notesJsonPath = path.join(artifactDir, "codex_review_notes.json");
    const reportMdPath = path.join(artifactDir, "codex_review_report.md");
    const summaryJsonPath = path.join(artifactDir, "codex_review_summary.json");
    const manualTemplatePath = path.join(artifactDir, "manual_scoring_template.tsv");
    const codexDraftSheetPath = path.join(artifactDir, "codex_draft_scoring_sheet.tsv");
    const humanQueueJsonPath = path.join(artifactDir, "human_confirmation_queue.json");
    const humanQueueTsvPath = path.join(artifactDir, "human_confirmation_queue.tsv");
    const referenceJudgePacketJsonPath = path.join(artifactDir, "reference_judge_packet.json");
    const referenceJudgePacketMdPath = path.join(artifactDir, "reference_judge_packet.md");
    const previousReview = await latestPreviousReview(frontendRoot, runId);
    const reviewStats = reviewStatsFromReport(report, notes);
    const topFailureTags = sortedTagRows(tagCountsFromNotes(notes)).map(([tag, count]) => ({ tag, count }));
    const reportFailureTags = failureTagRowsFromReport(report).map(([tag, count]) => ({ tag, count }));
    const readinessGate = buildReadinessGate({ report, notes, uiChecks, reviewStats });
    const referenceModelPaths = referenceModelPathsFromReport(report);
    const humanConfirmationQueue = buildHumanConfirmationQueue(records, draftScoreBaseline);
    const referenceJudgePacket = buildReferenceJudgePacket({
      records,
      referenceModelPaths,
      readinessGate,
      draftScoreBaseline,
    });
    const scoringArtifacts = {
      manualTemplatePath,
      codexDraftSheetPath,
      humanQueueJsonPath,
      humanQueueTsvPath,
      referenceJudgePacketJsonPath,
      referenceJudgePacketMdPath,
      rowCount: records.length,
    };
    await writeFile(notesJsonPath, JSON.stringify(notes, null, 2), "utf-8");
    await writeFile(manualTemplatePath, buildHumanScoringSheet(records, draftScoreBaseline), "utf-8");
    await writeFile(codexDraftSheetPath, buildHumanScoringSheet(records, draftScoreBaseline, { prefillDraft: true }), "utf-8");
    await writeFile(humanQueueJsonPath, JSON.stringify(humanConfirmationQueue, null, 2), "utf-8");
    await writeFile(humanQueueTsvPath, buildHumanConfirmationQueueTsv(humanConfirmationQueue), "utf-8");
    await writeFile(referenceJudgePacketJsonPath, JSON.stringify(referenceJudgePacket, null, 2), "utf-8");
    await writeFile(referenceJudgePacketMdPath, markdownReferenceJudgePacket(referenceJudgePacket), "utf-8");
    await appendFile(hermesNotesPath, notes.map(note => JSON.stringify(note)).join("\n") + (notes.length > 0 ? "\n" : ""), "utf-8");
    await writeFile(
      reportMdPath,
      markdownReport({
        baseUrl: options.baseUrl,
        apiBase: options.apiBase,
        report,
        notes,
        screenshots,
        uiChecks,
        previousReview,
        reviewStats,
        readinessGate,
        referenceModelPaths,
        draftScoreBaseline,
        humanConfirmationQueue,
        referenceJudgePacket,
        scoringArtifacts,
      }),
      "utf-8",
    );
    await writeFile(
      summaryJsonPath,
      JSON.stringify({
        createdAt: new Date().toISOString(),
        checkedRecords: notes.length,
        artifactDir,
        notesJsonPath,
        reportMdPath,
        hermesNotesPath,
        uiChecks,
        reviewStats,
        readinessGate,
        referenceModelPaths,
        draftScoreBaseline,
        humanConfirmationQueue,
        referenceJudgePacket: {
          source: referenceJudgePacket.source,
          warning: referenceJudgePacket.warning,
          summary: referenceJudgePacket.summary,
          jsonPath: referenceJudgePacketJsonPath,
          markdownPath: referenceJudgePacketMdPath,
        },
        scoringArtifacts,
        previousRunId: previousReview?.runId || "",
        topFailureTags,
        reportFailureTags,
      }, null, 2),
      "utf-8",
    );

    console.log(JSON.stringify({
      checkedRecords: notes.length,
      artifactDir,
      reportMdPath,
      notesJsonPath,
      manualTemplatePath,
      codexDraftSheetPath,
      humanQueueJsonPath,
      humanQueueTsvPath,
      referenceJudgePacketJsonPath,
      referenceJudgePacketMdPath,
      hermesNotesPath,
      uiChecks,
      readinessGate,
    }, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
