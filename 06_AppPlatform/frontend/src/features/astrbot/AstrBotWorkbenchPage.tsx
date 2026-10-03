import { type ReactNode, useEffect, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { apiUrl } from "../../api/client";
import { renderMarkdown } from "../../contexts/countryChatHelpers";
import {
  callAstrBotTool,
  fetchAgentConversationHistory,
  fetchAgentConversationSessions,
  fetchAgentUsageSummary,
  fetchAstrBotRuntimeStatus,
  type AgentUsageRecord,
  type AgentUsageSummary,
} from "./astrbotApi";
import { AstrBotNativeFrame } from "./AstrBotNativeFrame";
import { AstrBotMemoryPanel } from "./components/AstrBotMemoryPanel";
import { AstrBotEvalPanel } from "./components/AstrBotEvalPanel";
import { LazyPlotlyChart } from "../../components/LazyPlotlyChart";
import type { PlotlyChartProps } from "../../components/PlotlyChart";
import {
  ASTRBOT_MCP_ENDPOINT,
  ASTRBOT_PROVIDER,
  ASTRBOT_VIEW_CONFIGS,
  buildAstrBotNativeUrl,
  resolveAstrBotView,
  type AstrBotProviderStatus,
  type AstrBotAgentProfile,
  type AstrBotAgentSkill,
  type AgentConversationHistory,
  type AgentConversationSession,
  type AgentConversationTurn,
  type AstrBotRuntimeStatus,
  type AstrBotRetrievalDependencyStatus,
  type AstrBotToolCallResponse,
  type AstrBotToolDefinition,
  type AstrBotView,
} from "./astrbotConfig";

type RuntimeTone = "online" | "offline" | "warning" | "loading";
type AgentMode = "auto" | "chart" | "snapshot" | "pricing" | "news" | "research" | "variant";
type ResearchDepth = "quick" | "standard" | "deep";

export interface AstrBotQuickAction {
  id: string;
  label: string;
  description: string;
  mode: AgentMode;
  questionTemplate: (country: string) => string;
}

interface RuntimeRow {
  label: string;
  value: string;
  meta: string;
  tone: RuntimeTone;
}

interface ThinkingStep {
  type: "thinking" | "tool_call" | "tool_result" | "tool_error";
  message?: string;
  tool?: string;
  reason?: string;
  summary?: Record<string, unknown>;
  error?: string;
  round?: number;
}

interface ChatChart {
  chartId: string;
  chartType: string;
  title: string;
  data: Record<string, unknown>[];
  layout: Record<string, unknown>;
}

type VisualArtifactType = "chart" | "table" | "metric_cards" | "report_block";

export interface VisualArtifact {
  id: string;
  type: VisualArtifactType;
  title: string;
  subtitle?: string;
  data: unknown;
  spec?: Record<string, unknown>;
  fallbackReason?: string;
  sourceEvidenceRefs: string[];
}

export interface ChartFallbackRow {
  label: string;
  value: number;
  unit: string;
  sharePct: number;
}

export interface ChartFallbackCardData {
  title: string;
  notice: string;
  missingEvidence: string;
  rows: ChartFallbackRow[];
}

export interface AgentFollowUp {
  id: string;
  label: string;
  question: string;
  intent: string;
  reason: string;
  expectedTools: string[];
  expectedOutput?: string;
  priority: number;
  risk?: string;
}

interface EvidenceRef {
  refId: string;
  label: string;
  value?: string | number;
  unit?: string;
  source?: string;
  table?: string;
  rowCount?: number;
  retrievedAt?: string;
  scopeKey?: string;
  periodType?: string;
  periodLabel?: string;
  periodStart?: string;
  periodEnd?: string;
}

interface EvidenceScopeDetail {
  periodType: string;
  periodLabel: string;
  periodStart: string;
  periodEnd: string;
  values: Array<string | number>;
  refIds: string[];
}

interface EvidenceScopeDiagnostic {
  metric: string;
  periodType?: string;
  periodLabel?: string;
  periodStart?: string;
  periodEnd?: string;
  values: Array<string | number>;
  refIds: string[];
  scopes: EvidenceScopeDetail[];
}

interface EvidenceScopeDiagnostics {
  parallelScopes: EvidenceScopeDiagnostic[];
  conflicts: EvidenceScopeDiagnostic[];
  hasBlockingConflict: boolean;
}

interface ToolEvidence {
  toolName: string;
  success: boolean;
  rowCount: number;
  sourceType: string;
  summary: string;
  keyFindings: string[];
  evidenceRefs: EvidenceRef[];
}

interface MissingEvidence {
  name: string;
  reason: string;
  impact: string;
}

interface ResearchPolicy {
  intent: string;
  minSources: number;
  preferredSourceTiers: string[];
  requireOfficialSource: boolean;
  requirePublishDate: boolean;
  requireJatoCrossCheck: boolean;
  allowAnswerWithoutExternalSource: boolean;
}

interface ResearchModeSummary {
  mode: string;
  queryLimit: number;
  sourceLimit: number;
  description: string;
}

interface ResearchMetrics {
  queryCount: number;
  sourcesReturned: number;
  sourcesUsed: number;
  avgSourceScore: number;
  latencyMs: number;
  estimatedCost: number;
}

interface ResearchGovernance {
  policy?: ResearchPolicy;
  policyStatus: string;
  policyWarnings: string[];
  missingEvidence: MissingEvidence[];
  mode?: ResearchModeSummary;
  metrics?: ResearchMetrics;
  rejectedSources: Record<string, unknown>[];
}

interface JatoCrossCheck {
  status: string;
  summary: string;
  rawStatus?: string;
  checkedAt?: string;
}

interface InsightCard {
  title: string;
  claim: string;
  evidence: string[];
  implication: string;
  recommendedAction: string;
  citations: string[];
  confidence: string;
}

interface EvidenceAlignment {
  status: string;
  summary: string;
  internalSignal: string;
  externalSignal: string;
}

interface RecommendedAction {
  action: string;
  rationale: string;
  priority: string;
  evidenceRefs: string[];
  citationIds: string[];
}

interface BusinessRisk {
  name: string;
  impact: string;
  mitigation: string;
}

interface MethodKeySlide {
  slideId: string;
  title: string;
  relevance: string;
  summary: string;
}

interface FeatureValueClaim {
  featureName: string;
  customerValue: string;
  businessUse: string;
  supportsTrim: string;
  evidenceRef: string;
}

interface DataQualityWarning {
  code: string;
  severity: string;
  message: string;
  evidence: string;
  impact: string;
  mitigation: string;
}

interface PriceCorridor {
  positioning: string;
  coreCorridor: string;
  anchorPrice: string;
  mainTrimPrice: string;
  priceGap: string;
  basis: string;
}

interface VersionStrategy {
  lowTrimRole: string;
  mainTrimRole: string;
  priceGap: string;
  pvaCoverage: string;
  salesTalk: string[];
}

interface PricingMethodPlaybook {
  market_window: string;
  competitor_corridor: string;
  product_value_delta: string;
  price_anchor: string;
  main_trim_strategy: string;
  pva_validation: string;
  sales_talk_track: string[];
  risks_and_support: string[];
}

interface GoldenAnswerSpec {
  expectedMustMention: string[];
  answerQualityRubric: Record<string, string>;
}

interface BusinessMethodDistillation {
  deckId: string;
  deckTitle: string;
  sourceName: string;
  market: string;
  model: string;
  methodType: string;
  keySlides: MethodKeySlide[];
  analysisFlow: string[];
  coreClaims: string[];
  competitorPool: string[];
  priceCorridor: PriceCorridor;
  featureValueClaims: FeatureValueClaim[];
  versionStrategy: VersionStrategy;
  risksAndSupportNeeds: string[];
  dataQualityWarnings: DataQualityWarning[];
  pricingPlaybook: PricingMethodPlaybook;
  goldenAnswer: GoldenAnswerSpec;
}

interface BusinessSynthesisPlan {
  intent: string;
  country: string;
  executiveConclusion: string;
  internalEvidenceSummary: string;
  externalEvidenceSummary: string;
  evidenceAlignment: EvidenceAlignment;
  businessImplications: string[];
  recommendedActions: RecommendedAction[];
  risksAndMissingEvidence: BusinessRisk[];
  reportReadyBullets: string[];
  insightCardIds: string[];
  methodDistillation?: BusinessMethodDistillation;
}

interface EvidencePackage {
  evidenceId: string;
  intent: string;
  country: string;
  confidence: string;
  toolResults: ToolEvidence[];
  missingEvidence: MissingEvidence[];
  researchGovernance?: ResearchGovernance;
  jatoCrossCheck?: JatoCrossCheck;
  insightCards: InsightCard[];
  scopeDiagnostics?: EvidenceScopeDiagnostics;
}

interface QualityScore {
  intentScore: number;
  toolScore: number;
  groundingScore: number;
  followUpScore: number;
  safetyScore: number;
  executiveConclusionScore: number;
  businessImplicationScore: number;
  actionabilityScore: number;
  evidenceAlignmentScore: number;
  reportReadinessScore: number;
  businessSynthesisScore: number;
  totalScore: number;
  failures: string[];
}

interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  text: string;
  result?: AstrBotToolCallResponse;
  thinking?: ThinkingStep[];
  isStreaming?: boolean;
  streamStatus?: string;
  streamStartedAt?: number;
  activeCountry?: string;
  answerTitle?: string;
  answerSummary?: string;
  answerEvidenceLead?: string;
  answerBullets?: string[];
  keyTakeaways?: string[];
  pmInsight?: string;
  answerLimitations?: string[];
  answerFollowUps?: AgentFollowUp[];
  answerCitations?: AgentAnswerCitation[];
  visualArtifacts?: VisualArtifact[];
  toolCalls?: string[];
  charts?: ChatChart[];
  developerTrace?: Record<string, unknown>;
  evidencePlan?: Record<string, unknown>;
  evidencePackage?: EvidencePackage;
  qualityScore?: QualityScore;
  businessSynthesisPlan?: BusinessSynthesisPlan;
  methodDistillation?: BusinessMethodDistillation;
  recommendedActions?: RecommendedAction[];
  reportReadyBullets?: string[];
  sessionId?: string;
}

interface AgentAnswerCitation {
  label: string;
  source: string;
  tool: string;
  url?: string;
  citationId?: string;
  sourceScore?: number;
  sourceTier?: string;
  sourceTitle?: string;
  sourceCategory?: string;
  supportedClaim?: string;
  claimType?: string;
}

interface AgentAnswer {
  title: string;
  direct: string;
  evidenceBackedLead?: string;
  bullets: string[];
  citations: AgentAnswerCitation[];
  limitations: string[];
  followUps: AgentFollowUp[];
  confidence: string;
  status: string;
  answerStatus: string;
  grounding?: Record<string, unknown>;
  businessSynthesisPlan?: BusinessSynthesisPlan;
  methodDistillation?: BusinessMethodDistillation;
  recommendedActions: RecommendedAction[];
  reportReadyBullets: string[];
  businessImplications: string[];
  retrievalPaths: string[];
  sourceCount: number;
  tool: string;
}

interface AgentModelUsage {
  provider: string;
  model: string;
  pricingModel: string;
  status: string;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  estimatedCostCny: number;
  currency: string;
  usageId: string;
  finishReason: string;
  fallbackReason: string;
}

type JsonPreview = string | number | boolean | null | JsonPreview[] | { [key: string]: JsonPreview };

const AGENT_SESSION_STORAGE_KEY = "jato_agent_session_id";
const localAstrBotBackendPorts: Record<string, string> = {
  "5173": "8000",
  "5174": "8001",
  "5176": "8002",
};

const extensionRows = [
  {
    name: "JATO MCP bridge",
    status: "Active",
    boundary: "Required for every data-aware agent answer.",
  },
  {
    name: "Chart artifact bridge",
    status: "Planned",
    boundary: "Returns chart specs or rendered artifacts from JATO data sources.",
  },
  {
    name: "Community extensions",
    status: "Review",
    boundary: "Enable only after credential, network and data access checks.",
  },
];

const agentModeOptions: { value: AgentMode; label: string }[] = [
  { value: "auto", label: "Auto" },
  { value: "chart", label: "Chart" },
  { value: "snapshot", label: "Snapshot" },
  { value: "pricing", label: "Pricing" },
  { value: "news", label: "News" },
  { value: "research", label: "Research" },
  { value: "variant", label: "Variant" },
];

export const ASTRBOT_QUICK_ACTIONS: AstrBotQuickAction[] = [
  {
    id: "market_trend",
    label: "Analyze market trend",
    description: "看市场结构、动力变化和机会区间。",
    mode: "snapshot",
    questionTemplate: country => `Analyze the ${country} BEV / HEV / PHEV market trend, show the best available chart, and explain the product opportunity.`,
  },
  {
    id: "competitor_compare",
    label: "Compare competitors",
    description: "锁定竞品池，比较价格、配置和定位。",
    mode: "variant",
    questionTemplate: country => `Compare the key competitors in ${country} for J7 HEV, including positioning, configuration value and pricing implications.`,
  },
  {
    id: "pricing_corridor",
    label: "Check pricing corridor",
    description: "验证 MSRP、价格走廊和主销高配逻辑。",
    mode: "pricing",
    questionTemplate: country => `Build a pricing corridor for J7 HEV in ${country}, including competitor anchors, main-trim strategy and evidence gaps.`,
  },
  {
    id: "report_slide",
    label: "Generate report slide",
    description: "把当前问题整理成一页汇报结构。",
    mode: "research",
    questionTemplate: country => `Generate a one-page product manager report structure for a ${country} market analysis, with conclusion, evidence, risks and next actions.`,
  },
];

const usageLimitOptions = [10, 30, 100] as const;

type UsageLimit = typeof usageLimitOptions[number];

interface UsageBucketMetric {
  runs: number;
  tokens: number;
  costCny: number;
}

interface BrowserPlanAction {
  actionId: string;
  actionType: "click" | "type";
  targetType: string;
  label: string;
  selectorHint?: string;
  targetUrl?: string;
  confirmationToken: string;
  expiresAt: number;
  risk: string;
  requiresUserApproval: boolean;
}

interface BrowserActionPlan {
  status: string;
  url: string;
  browserEngine: string;
  title: string;
  actionGoal: string;
  actions: BrowserPlanAction[];
  approvalInstructions: string;
  limitations: string[];
}

interface BrowserActionResult {
  status: string;
  action: string;
  actionId: string;
  url: string;
  resultUrl: string;
  title: string;
  textPreview: string;
  typedCharacters: number;
  limitations: string[];
}

function renderTabLink(view: AstrBotView, activeView: AstrBotView) {
  const config = ASTRBOT_VIEW_CONFIGS[view];
  return (
    <Link
      key={view}
      to={config.to}
      className={`astrbot-tab${activeView === view ? " is-active" : ""}`}
      aria-current={activeView === view ? "page" : undefined}
    >
      {config.label}
    </Link>
  );
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function statusTone(status: string | undefined, fallback: RuntimeTone = "warning"): RuntimeTone {
  if (status === "online" || status === "configured") {
    return "online";
  }
  if (status === "offline" || status === "missing_key") {
    return "offline";
  }
  return fallback;
}

function buildRuntimeRows(status: AstrBotRuntimeStatus | null, loading: boolean): RuntimeRow[] {
  if (!status) {
    const tone: RuntimeTone = loading ? "loading" : "warning";
    return [
      { label: "Agent Runtime", value: loading ? "Checking" : "Unknown", meta: "shared service", tone },
      { label: "JATO Tools", value: loading ? "Checking" : "Unknown", meta: "tools pending", tone },
      { label: "Provider", value: ASTRBOT_PROVIDER.model, meta: "JATO env key", tone },
      { label: "Profile", value: "pm_coder_market_assistant", meta: "default persona", tone: "online" },
      { label: "Data Boundary", value: "MCP only", meta: "no direct DB secret", tone: "online" },
    ];
  }

  return [
    {
      label: "Agent Runtime",
      value: status.runtime.status,
      meta: `${status.runtime.httpStatus ?? "n/a"} · ${status.runtime.latencyMs}ms`,
      tone: statusTone(status.runtime.status),
    },
    {
      label: "JATO Tools",
      value: status.mcp.name,
      meta: `${status.mcp.toolCount} tools · ${status.mcp.transport}`,
      tone: statusTone(status.mcp.status),
    },
    {
      label: "Provider",
      value: status.provider.model,
      meta: status.provider.keyConfigured ? "key configured" : "key missing",
      tone: statusTone(status.provider.status),
    },
    {
      label: "Profile",
      value: status.profile.id,
      meta: status.profile.shortId,
      tone: "online",
    },
    {
      label: "Data Boundary",
      value: status.dataBoundary.mode === "mcp_only" ? "MCP only" : status.dataBoundary.mode,
      meta: status.dataBoundary.directDatabaseAccess ? "direct DB enabled" : "no direct DB secret",
      tone: status.dataBoundary.directDatabaseAccess ? "warning" : "online",
    },
  ];
}

function formatRequiredFields(tool: AstrBotToolDefinition): string {
  return tool.required.length > 0 ? tool.required.join(", ") : "none";
}

function summarizeJson(value: unknown, depth = 0): JsonPreview {
  if (value === null || typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return value;
  }
  if (Array.isArray(value)) {
    if (depth >= 2) {
      return [`${value.length} items`];
    }
    return value.slice(0, 3).map(item => summarizeJson(item, depth + 1));
  }
  if (typeof value === "object") {
    const source = value as Record<string, unknown>;
    const entries = Object.entries(source).slice(0, 10);
    const result: { [key: string]: JsonPreview } = {};
    entries.forEach(([key, entryValue]) => {
      result[key] = depth >= 2 ? String(entryValue) : summarizeJson(entryValue, depth + 1);
    });
    const remaining = Object.keys(source).length - entries.length;
    if (remaining > 0) {
      result._remainingKeys = remaining;
    }
    return result;
  }
  return String(value);
}

function buildProbePreview(result: AstrBotToolCallResponse): JsonPreview {
  return {
    tool: result.tool,
    metadata: summarizeJson(result.metadata),
    data: summarizeJson(result.data),
  };
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function readText(value: unknown, fallback = "n/a"): string {
  return typeof value === "string" && value.trim() ? value : fallback;
}

function buildAstrBotAgentEndpoint(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const explicitBase = String(import.meta.env.VITE_ASTRBOT_AGENT_API_BASE || "").trim();
  if (explicitBase) {
    return `${explicitBase.replace(/\/+$/, "")}${normalizedPath}`;
  }
  if (typeof window !== "undefined") {
    const host = window.location.hostname;
    const backendPort = localAstrBotBackendPorts[window.location.port];
    if ((host === "127.0.0.1" || host === "localhost") && backendPort) {
      return `${window.location.protocol}//${host}:${backendPort}/v1/astrbot/agent${normalizedPath}`;
    }
  }
  return apiUrl(`/astrbot/agent${normalizedPath}`);
}

function readNumber(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function readStringList(value: unknown): string[] {
  return Array.isArray(value)
    ? value.map(item => String(item || "").trim()).filter(Boolean)
    : [];
}

export interface AstrBotSseFrameParseResult {
  events: Record<string, unknown>[];
  rest: string;
}

export function parseAstrBotSseFrames(buffer: string): AstrBotSseFrameParseResult {
  const normalized = buffer.replace(/\r\n/g, "\n");
  const frames = normalized.split("\n\n");
  const rest = frames.pop() ?? "";
  const events: Record<string, unknown>[] = [];
  for (const frame of frames) {
    const dataText = frame
      .split("\n")
      .filter(line => line.startsWith("data:"))
      .map(line => line.slice(5).trimStart())
      .join("\n")
      .trim();
    if (!dataText || dataText === "[DONE]") {
      continue;
    }
    try {
      const parsed = JSON.parse(dataText);
      const record = asRecord(parsed);
      if (record) {
        events.push(record);
      }
    } catch {
      // Ignore malformed/incomplete SSE frames; the next read will carry the rest.
    }
  }
  return { events, rest };
}

export function normalizeAgentFollowUps(value: unknown): AgentFollowUp[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item, index): AgentFollowUp | null => {
      if (typeof item === "string") {
        const text = item.trim();
        if (!text) return null;
        return {
          id: `legacy_${index}_${text.slice(0, 24)}`,
          label: text.length > 34 ? `${text.slice(0, 34)}...` : text,
          question: text,
          intent: "legacy",
          reason: "Legacy follow-up question.",
          expectedTools: [],
          expectedOutput: "summary",
          priority: index + 1,
        };
      }
      const record = asRecord(item);
      if (!record) return null;
      const question = readText(record.question, readText(record.label, ""));
      if (!question) return null;
      const label = readText(record.label, question.length > 34 ? `${question.slice(0, 34)}...` : question);
      return {
        id: readText(record.id, `followup_${index}_${question.slice(0, 24)}`),
        label,
        question,
        intent: readText(record.intent, "drilldown"),
        reason: readText(record.reason, ""),
        expectedTools: readStringList(record.expectedTools),
        expectedOutput: readText(record.expectedOutput, "summary"),
        priority: readNumber(record.priority, index + 1),
        risk: typeof record.risk === "string" && record.risk.trim() ? record.risk : undefined,
      };
    })
    .filter((item): item is AgentFollowUp => item !== null)
    .sort((a, b) => a.priority - b.priority)
    .slice(0, 4);
}

function isBrowserActionType(value: string): value is BrowserPlanAction["actionType"] {
  return value === "click" || value === "type";
}

function readBrowserPlanActions(value: unknown): BrowserPlanAction[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item, index) => {
      const record = asRecord(item);
      if (!record) {
        return null;
      }
      const actionTypeText = readText(record.actionType, "click");
      const actionType = isBrowserActionType(actionTypeText) ? actionTypeText : "click";
      const confirmationToken = readText(record.confirmationToken, "");
      if (!confirmationToken) {
        return null;
      }
      const action: BrowserPlanAction = {
        actionId: readText(record.actionId, `action_${index + 1}`),
        actionType,
        targetType: readText(record.targetType, "element"),
        label: readText(record.label, `Action ${index + 1}`),
        selectorHint: typeof record.selectorHint === "string" && record.selectorHint.trim() ? record.selectorHint : undefined,
        targetUrl: typeof record.targetUrl === "string" && record.targetUrl.trim() ? record.targetUrl : undefined,
        confirmationToken,
        expiresAt: readNumber(record.expiresAt),
        risk: readText(record.risk, "unknown"),
        requiresUserApproval: record.requiresUserApproval === true,
      };
      return action;
    })
    .filter((action): action is BrowserPlanAction => action !== null);
}

function readBrowserActionPlan(result: AstrBotToolCallResponse | null): BrowserActionPlan | null {
  const data = asRecord(result?.data);
  if (!data) {
    return null;
  }
  return {
    status: readText(data.status, "unknown"),
    url: readText(data.url, ""),
    browserEngine: readText(data.browserEngine, "unknown"),
    title: readText(data.title, "Untitled page"),
    actionGoal: readText(data.actionGoal, ""),
    actions: readBrowserPlanActions(data.actions),
    approvalInstructions: readText(data.approvalInstructions, ""),
    limitations: readStringList(data.limitations),
  };
}

function readBrowserActionResult(result: AstrBotToolCallResponse | null): BrowserActionResult | null {
  const data = asRecord(result?.data);
  if (!data) {
    return null;
  }
  return {
    status: readText(data.status, "unknown"),
    action: readText(data.action, readText(result?.tool, "browser action")),
    actionId: readText(data.actionId, ""),
    url: readText(data.url, ""),
    resultUrl: readText(data.resultUrl, ""),
    title: readText(data.title, "Browser result"),
    textPreview: readText(data.textPreview, ""),
    typedCharacters: readNumber(data.typedCharacters),
    limitations: readStringList(data.limitations),
  };
}

function formatBrowserExpiry(expiresAt: number): string {
  if (expiresAt <= 0) {
    return "n/a";
  }
  const timestamp = new Date(expiresAt * 1000);
  return Number.isNaN(timestamp.getTime()) ? "n/a" : timestamp.toLocaleTimeString();
}

function readChatCharts(value: unknown): ChatChart[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item, index) => ({
      chartId: readText(item.chartId, `history_chart_${index}`),
      chartType: readText(item.chartType, "chart"),
      title: readText(item.title, `Chart ${index + 1}`),
      data: Array.isArray(item.data) ? item.data as Record<string, unknown>[] : [],
      layout: asRecord(item.layout) ?? {},
    }))
    .filter(chart => chart.data.length > 0);
}

function isVisualArtifactType(value: string): value is VisualArtifactType {
  return value === "chart" || value === "table" || value === "metric_cards" || value === "report_block";
}

export function readVisualArtifacts(value: unknown): VisualArtifact[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((item, index): VisualArtifact | null => {
      const record = asRecord(item);
      if (!record) {
        return null;
      }
      const rawType = readText(record.type, "");
      if (!isVisualArtifactType(rawType)) {
        return null;
      }
      const title = readText(record.title, rawType);
      return {
        id: readText(record.id, `visual_artifact_${index + 1}`),
        type: rawType,
        title,
        subtitle: typeof record.subtitle === "string" && record.subtitle.trim() ? record.subtitle : undefined,
        data: record.data,
        spec: asRecord(record.spec) ?? undefined,
        fallbackReason: typeof record.fallbackReason === "string" && record.fallbackReason.trim() ? record.fallbackReason : undefined,
        sourceEvidenceRefs: readStringList(record.sourceEvidenceRefs),
      };
    })
    .filter((item): item is VisualArtifact => item !== null)
    .slice(0, 8);
}

function readDeveloperTrace(value: unknown): Record<string, unknown> | undefined {
  const trace = asRecord(value);
  if (!trace || Object.keys(trace).length === 0) {
    return undefined;
  }
  return trace;
}

export function readEvidencePackage(value: unknown): EvidencePackage | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  const toolResults = Array.isArray(record.toolResults)
    ? record.toolResults
      .map(item => asRecord(item))
      .filter((item): item is Record<string, unknown> => Boolean(item))
      .map((item): ToolEvidence => ({
        toolName: readText(item.toolName, "tool"),
        success: item.success !== false,
        rowCount: readNumber(item.rowCount),
        sourceType: readText(item.sourceType, "jato_parquet"),
        summary: readText(item.summary, ""),
        keyFindings: readStringList(item.keyFindings),
        evidenceRefs: readEvidenceRefs(item.evidenceRefs),
      }))
    : [];
  const missingEvidence = Array.isArray(record.missingEvidence)
    ? record.missingEvidence
      .map(item => asRecord(item))
      .filter((item): item is Record<string, unknown> => Boolean(item))
      .map((item): MissingEvidence => ({
        name: readText(item.name, "missing_evidence"),
        reason: readText(item.reason, ""),
        impact: readText(item.impact, "weakens_answer"),
      }))
    : [];
  return {
    evidenceId: readText(record.evidenceId, ""),
    intent: readText(record.intent, ""),
    country: readText(record.country, ""),
    confidence: readText(record.confidence, "low"),
    toolResults,
    missingEvidence,
    researchGovernance: readResearchGovernance(record.researchGovernance),
    jatoCrossCheck: readJatoCrossCheck(record.jatoCrossCheck),
    insightCards: readInsightCards(record.insightCards),
    scopeDiagnostics: readEvidenceScopeDiagnostics(record.scopeDiagnostics),
  };
}

function readEvidenceScopeDiagnostics(value: unknown): EvidenceScopeDiagnostics | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  return {
    parallelScopes: readEvidenceScopeDiagnosticList(record.parallelScopes),
    conflicts: readEvidenceScopeDiagnosticList(record.conflicts),
    hasBlockingConflict: record.hasBlockingConflict === true,
  };
}

function readEvidenceScopeDiagnosticList(value: unknown): EvidenceScopeDiagnostic[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map(item => ({
      metric: readText(item.metric, "market_metric"),
      periodType: readOptionalText(item.periodType),
      periodLabel: readOptionalText(item.periodLabel),
      periodStart: readOptionalText(item.periodStart),
      periodEnd: readOptionalText(item.periodEnd),
      values: readEvidenceScopeValues(item.values),
      refIds: readStringList(item.refIds),
      scopes: readEvidenceScopeDetails(item.scopes),
    }));
}

function readEvidenceScopeDetails(value: unknown): EvidenceScopeDetail[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map(item => ({
      periodType: readText(item.periodType, "unknown"),
      periodLabel: readText(item.periodLabel, "时间范围未标注"),
      periodStart: readText(item.periodStart, ""),
      periodEnd: readText(item.periodEnd, ""),
      values: readEvidenceScopeValues(item.values),
      refIds: readStringList(item.refIds),
    }));
}

function readEvidenceScopeValues(value: unknown): Array<string | number> {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string | number => typeof item === "string" || typeof item === "number");
}

function readOptionalText(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readResearchGovernance(value: unknown): ResearchGovernance | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  const policyRecord = asRecord(record.policy);
  const metricsRecord = asRecord(record.metrics);
  const modeRecord = asRecord(record.mode);
  const missingEvidence = Array.isArray(record.missingEvidence)
    ? record.missingEvidence
      .map(item => asRecord(item))
      .filter((item): item is Record<string, unknown> => Boolean(item))
      .map((item): MissingEvidence => ({
        name: readText(item.name, "missing_evidence"),
        reason: readText(item.reason, ""),
        impact: readText(item.impact, "weakens_answer"),
      }))
    : [];
  return {
    policy: policyRecord ? {
      intent: readText(policyRecord.intent, ""),
      minSources: readNumber(policyRecord.minSources),
      preferredSourceTiers: readStringList(policyRecord.preferredSourceTiers),
      requireOfficialSource: policyRecord.requireOfficialSource === true,
      requirePublishDate: policyRecord.requirePublishDate === true,
      requireJatoCrossCheck: policyRecord.requireJatoCrossCheck === true,
      allowAnswerWithoutExternalSource: policyRecord.allowAnswerWithoutExternalSource === true,
    } : undefined,
    policyStatus: readText(record.policyStatus, "unknown"),
    policyWarnings: readStringList(record.policyWarnings),
    missingEvidence,
    mode: modeRecord ? {
      mode: readText(modeRecord.mode, "standard"),
      queryLimit: readNumber(modeRecord.queryLimit),
      sourceLimit: readNumber(modeRecord.sourceLimit),
      description: readText(modeRecord.description, ""),
    } : undefined,
    metrics: metricsRecord ? {
      queryCount: readNumber(metricsRecord.queryCount),
      sourcesReturned: readNumber(metricsRecord.sourcesReturned),
      sourcesUsed: readNumber(metricsRecord.sourcesUsed),
      avgSourceScore: readNumber(metricsRecord.avgSourceScore),
      latencyMs: readNumber(metricsRecord.latencyMs),
      estimatedCost: readNumber(metricsRecord.estimatedCost),
    } : undefined,
    rejectedSources: Array.isArray(record.rejectedSources)
      ? record.rejectedSources.map(item => asRecord(item)).filter((item): item is Record<string, unknown> => Boolean(item))
      : [],
  };
}

function readJatoCrossCheck(value: unknown): JatoCrossCheck | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  return {
    status: readText(record.status, "unknown"),
    summary: readText(record.summary, ""),
    rawStatus: typeof record.rawStatus === "string" && record.rawStatus.trim() ? record.rawStatus : undefined,
    checkedAt: typeof record.checkedAt === "string" && record.checkedAt.trim() ? record.checkedAt : undefined,
  };
}

function readInsightCards(value: unknown): InsightCard[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): InsightCard => ({
      title: readText(item.title, "Insight"),
      claim: readText(item.claim, ""),
      evidence: readStringList(item.evidence),
      implication: readText(item.implication, ""),
      recommendedAction: readText(item.recommendedAction, ""),
      citations: readStringList(item.citations),
      confidence: readText(item.confidence, "medium"),
    }))
    .filter(card => card.claim || card.title);
}

function readRecommendedActions(value: unknown): RecommendedAction[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): RecommendedAction => ({
      action: readText(item.action, ""),
      rationale: readText(item.rationale, ""),
      priority: readText(item.priority, "P1"),
      evidenceRefs: readStringList(item.evidenceRefs),
      citationIds: readStringList(item.citationIds),
    }))
    .filter(item => item.action);
}

function readBusinessRisks(value: unknown): BusinessRisk[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): BusinessRisk => ({
      name: readText(item.name, "risk"),
      impact: readText(item.impact, ""),
      mitigation: readText(item.mitigation, ""),
    }))
    .filter(item => item.impact || item.mitigation);
}

function readMethodKeySlides(value: unknown): MethodKeySlide[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): MethodKeySlide => ({
      slideId: readText(item.slideId, ""),
      title: readText(item.title, "Slide"),
      relevance: readText(item.relevance, ""),
      summary: readText(item.summary, ""),
    }))
    .filter(item => item.slideId || item.summary);
}

function readFeatureValueClaims(value: unknown): FeatureValueClaim[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): FeatureValueClaim => ({
      featureName: readText(item.featureName, ""),
      customerValue: readText(item.customerValue, ""),
      businessUse: readText(item.businessUse, ""),
      supportsTrim: readText(item.supportsTrim, ""),
      evidenceRef: readText(item.evidenceRef, ""),
    }))
    .filter(item => item.featureName);
}

function readDataQualityWarnings(value: unknown): DataQualityWarning[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): DataQualityWarning => ({
      code: readText(item.code, "warning"),
      severity: readText(item.severity, "info"),
      message: readText(item.message, ""),
      evidence: readText(item.evidence, ""),
      impact: readText(item.impact, ""),
      mitigation: readText(item.mitigation, ""),
    }))
    .filter(item => item.message || item.impact);
}

function readPriceCorridor(value: unknown): PriceCorridor {
  const record = asRecord(value) ?? {};
  return {
    positioning: readText(record.positioning, ""),
    coreCorridor: readText(record.coreCorridor, ""),
    anchorPrice: readText(record.anchorPrice, ""),
    mainTrimPrice: readText(record.mainTrimPrice, ""),
    priceGap: readText(record.priceGap, ""),
    basis: readText(record.basis, ""),
  };
}

function readVersionStrategy(value: unknown): VersionStrategy {
  const record = asRecord(value) ?? {};
  return {
    lowTrimRole: readText(record.lowTrimRole, ""),
    mainTrimRole: readText(record.mainTrimRole, ""),
    priceGap: readText(record.priceGap, ""),
    pvaCoverage: readText(record.pvaCoverage, ""),
    salesTalk: readStringList(record.salesTalk),
  };
}

function readPricingMethodPlaybook(value: unknown): PricingMethodPlaybook {
  const record = asRecord(value) ?? {};
  return {
    market_window: readText(record.market_window, ""),
    competitor_corridor: readText(record.competitor_corridor, ""),
    product_value_delta: readText(record.product_value_delta, ""),
    price_anchor: readText(record.price_anchor, ""),
    main_trim_strategy: readText(record.main_trim_strategy, ""),
    pva_validation: readText(record.pva_validation, ""),
    sales_talk_track: readStringList(record.sales_talk_track),
    risks_and_support: readStringList(record.risks_and_support),
  };
}

function readGoldenAnswerSpec(value: unknown): GoldenAnswerSpec {
  const record = asRecord(value) ?? {};
  const rubricRecord = asRecord(record.answerQualityRubric) ?? {};
  const answerQualityRubric = Object.fromEntries(
    Object.entries(rubricRecord)
      .filter(([, item]) => typeof item === "string")
      .map(([key, item]) => [key, item as string]),
  );
  return {
    expectedMustMention: readStringList(record.expectedMustMention),
    answerQualityRubric,
  };
}

function readBusinessMethodDistillation(value: unknown): BusinessMethodDistillation | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  const methodType = readText(record.methodType, "");
  const deckTitle = readText(record.deckTitle, "");
  if (!methodType && !deckTitle) {
    return undefined;
  }
  return {
    deckId: readText(record.deckId, ""),
    deckTitle: deckTitle || "Business method",
    sourceName: readText(record.sourceName, ""),
    market: readText(record.market, ""),
    model: readText(record.model, ""),
    methodType,
    keySlides: readMethodKeySlides(record.keySlides),
    analysisFlow: readStringList(record.analysisFlow),
    coreClaims: readStringList(record.coreClaims),
    competitorPool: readStringList(record.competitorPool),
    priceCorridor: readPriceCorridor(record.priceCorridor),
    featureValueClaims: readFeatureValueClaims(record.featureValueClaims),
    versionStrategy: readVersionStrategy(record.versionStrategy),
    risksAndSupportNeeds: readStringList(record.risksAndSupportNeeds),
    dataQualityWarnings: readDataQualityWarnings(record.dataQualityWarnings),
    pricingPlaybook: readPricingMethodPlaybook(record.pricingPlaybook),
    goldenAnswer: readGoldenAnswerSpec(record.goldenAnswer),
  };
}

function readBusinessSynthesisPlan(value: unknown): BusinessSynthesisPlan | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  const alignment = asRecord(record.evidenceAlignment);
  return {
    intent: readText(record.intent, ""),
    country: readText(record.country, ""),
    executiveConclusion: readText(record.executiveConclusion, ""),
    internalEvidenceSummary: readText(record.internalEvidenceSummary, ""),
    externalEvidenceSummary: readText(record.externalEvidenceSummary, ""),
    evidenceAlignment: {
      status: readText(alignment?.status, "unknown"),
      summary: readText(alignment?.summary, ""),
      internalSignal: readText(alignment?.internalSignal, ""),
      externalSignal: readText(alignment?.externalSignal, ""),
    },
    businessImplications: readStringList(record.businessImplications),
    recommendedActions: readRecommendedActions(record.recommendedActions),
    risksAndMissingEvidence: readBusinessRisks(record.risksAndMissingEvidence),
    reportReadyBullets: readStringList(record.reportReadyBullets),
    insightCardIds: readStringList(record.insightCardIds),
    methodDistillation: readBusinessMethodDistillation(record.methodDistillation),
  };
}

function readEvidenceRefs(value: unknown): EvidenceRef[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map((item): EvidenceRef => {
      const rawValue = item.value;
      return {
        refId: readText(item.refId, ""),
        label: readText(item.label, "evidence"),
        value: typeof rawValue === "number" || typeof rawValue === "string" ? rawValue : undefined,
        unit: typeof item.unit === "string" && item.unit.trim() ? item.unit : undefined,
        source: typeof item.source === "string" && item.source.trim() ? item.source : undefined,
        table: typeof item.table === "string" && item.table.trim() ? item.table : undefined,
        rowCount: typeof item.rowCount === "number" ? item.rowCount : undefined,
        retrievedAt: typeof item.retrievedAt === "string" && item.retrievedAt.trim() ? item.retrievedAt : undefined,
        scopeKey: readOptionalText(item.scopeKey),
        periodType: readOptionalText(item.periodType),
        periodLabel: readOptionalText(item.periodLabel),
        periodStart: readOptionalText(item.periodStart),
        periodEnd: readOptionalText(item.periodEnd),
      };
    });
}

export function readQualityScore(value: unknown): QualityScore | undefined {
  const record = asRecord(value);
  if (!record) {
    return undefined;
  }
  return {
    intentScore: readNumber(record.intentScore),
    toolScore: readNumber(record.toolScore),
    groundingScore: readNumber(record.groundingScore),
    followUpScore: readNumber(record.followUpScore),
    safetyScore: readNumber(record.safetyScore),
    executiveConclusionScore: readNumber(record.executiveConclusionScore),
    businessImplicationScore: readNumber(record.businessImplicationScore),
    actionabilityScore: readNumber(record.actionabilityScore),
    evidenceAlignmentScore: readNumber(record.evidenceAlignmentScore),
    reportReadinessScore: readNumber(record.reportReadinessScore),
    businessSynthesisScore: readNumber(record.businessSynthesisScore),
    totalScore: readNumber(record.totalScore),
    failures: readStringList(record.failures),
  };
}

function readAgentCitations(value: unknown): AgentAnswerCitation[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item))
    .map(item => ({
      label: readText(item.label, readText(item.title, readText(item.source, "Source"))),
      source: readText(item.source, readText(item.provider, "jato")),
      tool: readText(item.tool, "tool"),
      url: typeof item.url === "string" && item.url.trim() ? item.url : undefined,
      citationId: typeof item.citationId === "string" && item.citationId.trim() ? item.citationId : undefined,
      sourceScore: typeof item.sourceScore === "number" ? item.sourceScore : undefined,
      sourceTier: typeof item.sourceTier === "string" && item.sourceTier.trim() ? item.sourceTier : undefined,
      sourceTitle: typeof item.sourceTitle === "string" && item.sourceTitle.trim() ? item.sourceTitle : undefined,
      sourceCategory: typeof item.sourceCategory === "string" && item.sourceCategory.trim() ? item.sourceCategory : undefined,
      supportedClaim: typeof item.supportedClaim === "string" && item.supportedClaim.trim() ? item.supportedClaim : undefined,
      claimType: typeof item.claimType === "string" && item.claimType.trim() ? item.claimType : undefined,
    }));
}

function readAgentAnswer(result: AstrBotToolCallResponse | null): AgentAnswer | null {
  const data = asRecord(result?.data);
  const answer = asRecord(data?.answer);
  if (!answer) {
    return null;
  }
  const citations = readAgentCitations(answer.citations);

  return {
    title: readText(answer.title, "Grounded answer"),
    direct: readText(answer.direct, ""),
    evidenceBackedLead: readText(answer.evidenceBackedLead, "") || undefined,
    bullets: readStringList(answer.bullets),
    citations,
    limitations: readStringList(answer.limitations),
    followUps: normalizeAgentFollowUps(answer.followUps),
    confidence: readText(answer.confidence, "medium"),
    status: readText(answer.status, readText(answer.answerStatus, "answered")),
    answerStatus: readText(answer.answerStatus, readText(answer.status, "answered")),
    grounding: asRecord(answer.grounding) ?? undefined,
    businessSynthesisPlan: readBusinessSynthesisPlan(answer.businessSynthesisPlan),
    methodDistillation: readBusinessMethodDistillation(answer.methodDistillation ?? asRecord(answer.businessSynthesisPlan)?.methodDistillation),
    recommendedActions: readRecommendedActions(answer.recommendedActions),
    reportReadyBullets: readStringList(answer.reportReadyBullets),
    businessImplications: readStringList(answer.businessImplications),
    retrievalPaths: readStringList(answer.retrievalPaths),
    sourceCount: typeof answer.sourceCount === "number" ? answer.sourceCount : 0,
    tool: readText(answer.tool, readText(result?.metadata.selectedTool, "tool")),
  };
}

function readModelUsage(result: AstrBotToolCallResponse | null): AgentModelUsage | null {
  const data = asRecord(result?.data);
  const usage = asRecord(data?.modelUsage);
  if (!usage) {
    return null;
  }
  return {
    provider: readText(usage.provider, "deepseek"),
    model: readText(usage.model, "deepseek-chat"),
    pricingModel: readText(usage.pricingModel, readText(usage.model, "deepseek-chat")),
    status: readText(usage.status, "unknown"),
    promptTokens: readNumber(usage.promptTokens),
    completionTokens: readNumber(usage.completionTokens),
    totalTokens: readNumber(usage.totalTokens),
    estimatedCostCny: readNumber(usage.estimatedCostCny),
    currency: readText(usage.currency, "CNY"),
    usageId: readText(usage.usageId, ""),
    finishReason: readText(usage.finishReason, ""),
    fallbackReason: readText(usage.fallbackReason, ""),
  };
}

function formatTokens(value: number): string {
  return Math.max(0, Math.round(value)).toLocaleString();
}

function formatCost(value: number, currency = "CNY"): string {
  return `${currency} ${value.toFixed(6)}`;
}

function formatDateTime(value: string): string {
  const timestamp = new Date(value);
  return Number.isNaN(timestamp.getTime()) ? "n/a" : timestamp.toLocaleString();
}

function formatUsageBarWidth(value: number, maxValue: number): string {
  if (value <= 0 || maxValue <= 0) {
    return "0%";
  }
  return `${Math.min(100, Math.max(4, (value / maxValue) * 100)).toFixed(1)}%`;
}

function makeMessageId(role: ChatMessage["role"]): string {
  return `${role}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
}

function createAgentSessionId(): string {
  return `sess_${Math.random().toString(36).slice(2, 14)}`;
}

function readInitialAgentSessionId(): string {
  const stored = localStorage.getItem(AGENT_SESSION_STORAGE_KEY);
  if (stored) {
    return stored;
  }
  const sessionId = createAgentSessionId();
  localStorage.setItem(AGENT_SESSION_STORAGE_KEY, sessionId);
  return sessionId;
}

function conversationTurnToMessage(turn: AgentConversationTurn, index: number): ChatMessage {
  const role: ChatMessage["role"] = turn.role === "assistant" ? "assistant" : "user";
  const metadata = asRecord(turn.metadata) ?? {};
  const toolCalls = role === "assistant" ? readStringList(metadata.toolCalls) : [];
  const charts = role === "assistant" ? readChatCharts(metadata.charts) : [];
  const bullets = role === "assistant" ? readStringList(metadata.bullets) : [];
  const keyTakeaways = role === "assistant" ? readStringList(metadata.keyTakeaways) : [];
  const limitations = role === "assistant" ? readStringList(metadata.limitations) : [];
  const evidenceBackedLead = role === "assistant" ? readText(metadata.evidenceBackedLead, "") : "";
  const followUps = role === "assistant" ? normalizeAgentFollowUps(metadata.structuredFollowUps ?? metadata.followUps) : [];
  const citations = role === "assistant" ? readAgentCitations(metadata.citations) : [];
  const visualArtifacts = role === "assistant" ? readVisualArtifacts(metadata.visualArtifacts) : [];
  const developerTrace = role === "assistant" ? readDeveloperTrace(metadata.developerTrace) : undefined;
  const evidencePlan = role === "assistant" ? asRecord(metadata.evidencePlan) ?? undefined : undefined;
  const evidencePackage = role === "assistant" ? readEvidencePackage(metadata.evidencePackage) : undefined;
  const qualityScore = role === "assistant" ? readQualityScore(metadata.qualityScore) : undefined;
  const businessSynthesisPlan = role === "assistant" ? readBusinessSynthesisPlan(metadata.businessSynthesisPlan) : undefined;
  const methodDistillation = role === "assistant"
    ? readBusinessMethodDistillation(metadata.methodDistillation ?? businessSynthesisPlan?.methodDistillation)
    : undefined;
  const recommendedActions = role === "assistant" ? readRecommendedActions(metadata.recommendedActions) : [];
  const reportReadyBullets = role === "assistant" ? readStringList(metadata.reportReadyBullets) : [];
  return {
    id: turn.turnId || `history_${index}`,
    role,
    text: turn.text,
    toolCalls: toolCalls.length > 0 ? toolCalls : undefined,
    activeCountry: role === "assistant" ? readText(metadata.country, "") || undefined : undefined,
    answerTitle: role === "assistant" ? readText(metadata.answerTitle, "") || undefined : undefined,
    answerSummary: role === "assistant" ? readText(metadata.summary, "") || undefined : undefined,
    answerEvidenceLead: evidenceBackedLead || undefined,
    answerBullets: bullets.length > 0 ? bullets : undefined,
    keyTakeaways: keyTakeaways.length > 0 ? keyTakeaways : undefined,
    pmInsight: role === "assistant" ? readText(metadata.pmInsight, "") || undefined : undefined,
    answerLimitations: limitations.length > 0 ? limitations : undefined,
    answerFollowUps: followUps.length > 0 ? followUps : undefined,
    answerCitations: citations.length > 0 ? citations : undefined,
    visualArtifacts: visualArtifacts.length > 0 ? visualArtifacts : undefined,
    charts: charts.length > 0 ? charts : undefined,
    developerTrace,
    evidencePlan,
    evidencePackage,
    qualityScore,
    businessSynthesisPlan,
    methodDistillation,
    recommendedActions: recommendedActions.length > 0 ? recommendedActions : undefined,
    reportReadyBullets: reportReadyBullets.length > 0 ? reportReadyBullets : undefined,
    sessionId: turn.sessionId,
  };
}

function conversationHistoryToMessages(history: AgentConversationHistory): ChatMessage[] {
  return history.turns
    .map((turn, index) => conversationTurnToMessage(turn, index))
    .filter(message => message.text.trim().length > 0);
}

function latestResolvedCountryFromMessages(messages: ChatMessage[]): string {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message.role === "assistant") {
      const activeCountry = message.activeCountry?.trim();
      if (activeCountry) {
        return activeCountry;
      }
      const evidenceCountry = message.evidencePackage?.country?.trim();
      if (evidenceCountry) {
        return evidenceCountry;
      }
    }
    const inferredCountry = inferAstrBotQuestionCountry(message.text);
    if (inferredCountry) {
      return inferredCountry;
    }
  }
  return "";
}

function mergeSessionOptions(activeSessionId: string, sessions: AgentConversationSession[]): AgentConversationSession[] {
  const seen = new Set<string>();
  const merged: AgentConversationSession[] = [];
  for (const session of sessions) {
    if (seen.has(session.sessionId)) continue;
    seen.add(session.sessionId);
    merged.push(session);
  }
  if (!seen.has(activeSessionId)) {
    merged.unshift({
      sessionId: activeSessionId,
      startedAt: "",
      lastActivityAt: "",
      turnCount: 0,
    });
  }
  return merged.slice(0, 20);
}

function formatSessionOption(session: AgentConversationSession): string {
  const compactId = session.sessionId.replace(/^sess_/, "").slice(0, 12) || session.sessionId;
  return session.turnCount > 0 ? `${compactId} (${session.turnCount})` : compactId;
}

function truncateSessionText(value: string | undefined, limit: number): string {
  const text = String(value || "").trim().replace(/\s+/g, " ");
  if (!text) return "";
  if (text.length <= limit) return text;
  return `${text.slice(0, Math.max(0, limit - 1)).trim()}…`;
}

export function formatSessionDisplayTitle(session: AgentConversationSession): string {
  const latestQuestion = truncateSessionText(session.latestQuestion, 54);
  if (latestQuestion) {
    return session.country ? `${session.country} · ${latestQuestion}` : latestQuestion;
  }
  const latestAnswerTitle = truncateSessionText(session.latestAnswerTitle, 54);
  if (latestAnswerTitle) {
    return session.country ? `${session.country} · ${latestAnswerTitle}` : latestAnswerTitle;
  }
  return session.turnCount > 0 ? formatSessionOption(session) : "Current draft";
}

export function formatSessionDisplayMeta(session: AgentConversationSession): string {
  const parts = [
    `${formatSessionTime(session.lastActivityAt)} · ${session.turnCount} turn${session.turnCount === 1 ? "" : "s"}`,
  ];
  const status = truncateSessionText(session.answerStatus, 28);
  const confidence = truncateSessionText(session.confidence, 18);
  if (status || confidence) {
    parts.push([status, confidence].filter(Boolean).join(" · "));
  }
  const toolCount = session.toolCalls?.length ?? 0;
  if (toolCount > 0) {
    parts.push(`${toolCount} tool${toolCount === 1 ? "" : "s"}`);
  }
  return parts.join(" · ");
}

function formatSessionTime(value: string): string {
  if (!value) {
    return "New";
  }
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) {
    return "Recent";
  }
  return timestamp.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

const ASTRBOT_COUNTRY_MENTION_ALIASES: ReadonlyArray<readonly [string, string]> = [
  ["Czech Republic", "Czech Republic"],
  ["United Kingdom", "UK"],
  ["Great Britain", "UK"],
  ["Magyarország", "Hungary"],
  ["Magyarorszag", "Hungary"],
  ["Hungary", "Hungary"],
  ["Hungarian", "Hungary"],
  ["匈牙利", "Hungary"],
  ["Sweden", "Sweden"],
  ["Swedish", "Sweden"],
  ["Sverige", "Sweden"],
  ["瑞典", "Sweden"],
  ["Germany", "Germany"],
  ["German", "Germany"],
  ["Deutschland", "Germany"],
  ["德国", "Germany"],
  ["Norway", "Norway"],
  ["Norwegian", "Norway"],
  ["Norge", "Norway"],
  ["挪威", "Norway"],
  ["Finland", "Finland"],
  ["Finnish", "Finland"],
  ["Suomi", "Finland"],
  ["芬兰", "Finland"],
  ["Denmark", "Denmark"],
  ["Danish", "Denmark"],
  ["Danmark", "Denmark"],
  ["丹麦", "Denmark"],
  ["Austria", "Austria"],
  ["Österreich", "Austria"],
  ["Osterreich", "Austria"],
  ["奥地利", "Austria"],
  ["Croatia", "Croatia"],
  ["Hrvatska", "Croatia"],
  ["克罗地亚", "Croatia"],
  ["Czechia", "Czech Republic"],
  ["Czech", "Czech Republic"],
  ["Česko", "Czech Republic"],
  ["Cesko", "Czech Republic"],
  ["捷克", "Czech Republic"],
  ["Slovakia", "Slovakia"],
  ["Slovensko", "Slovakia"],
  ["斯洛伐克", "Slovakia"],
  ["Spain", "Spain"],
  ["Spanish", "Spain"],
  ["España", "Spain"],
  ["Espana", "Spain"],
  ["西班牙", "Spain"],
  ["France", "France"],
  ["法国", "France"],
  ["Italy", "Italy"],
  ["Italia", "Italy"],
  ["意大利", "Italy"],
  ["Netherlands", "Netherlands"],
  ["Dutch", "Netherlands"],
  ["荷兰", "Netherlands"],
  ["Belgium", "Belgium"],
  ["Belgian", "Belgium"],
  ["比利时", "Belgium"],
];

const ASTRBOT_COUNTRY_CODE_ALIASES: Record<string, string> = {
  SE: "Sweden",
  SWE: "Sweden",
  HU: "Hungary",
  HUN: "Hungary",
  DE: "Germany",
  DEU: "Germany",
  NO: "Norway",
  NOR: "Norway",
  FI: "Finland",
  FIN: "Finland",
  DK: "Denmark",
  DNK: "Denmark",
  AT: "Austria",
  AUT: "Austria",
  CZ: "Czech Republic",
  CZE: "Czech Republic",
  HR: "Croatia",
  HRV: "Croatia",
  ES: "Spain",
  ESP: "Spain",
  FR: "France",
  FRA: "France",
  IT: "Italy",
  ITA: "Italy",
  NL: "Netherlands",
  NLD: "Netherlands",
  BE: "Belgium",
  BEL: "Belgium",
  UK: "UK",
  GB: "UK",
  GBR: "UK",
};

function aliasAppearsInText(alias: string, text: string, lowerText: string): boolean {
  if (/[\u4e00-\u9fff]/.test(alias)) return text.includes(alias);
  if (alias.length <= 3) {
    return new RegExp(`(^|[^A-Za-z])${alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}([^A-Za-z]|$)`, "i").test(text);
  }
  return lowerText.includes(alias.toLowerCase());
}

function isNegatedCountryMention(alias: string, text: string, lowerText: string): boolean {
  const aliasLower = alias.toLowerCase();
  const aliasIndex = lowerText.indexOf(aliasLower);
  const textIndex = text.indexOf(alias);
  const mentionIndex = aliasIndex >= 0 ? aliasIndex : textIndex;
  if (mentionIndex < 0) return false;
  const rawPrefix = text.slice(Math.max(0, mentionIndex - 24), mentionIndex).toLowerCase();
  const prefix = rawPrefix.split(/[，,。.;；!！?？]/).pop() ?? rawPrefix;
  if (/不要回答|别回答|不要用|别用|不要按|别按|不是|非/.test(prefix)) return true;
  if (/\b(no|not|never|without|instead of|rather than)\s*$/i.test(prefix)) return true;
  if (/\b(do not|don't|dont|don’t)\s+(answer|use|analyze|analyse|reply with|respond with)\s*$/i.test(prefix)) return true;
  return false;
}

export function inferAstrBotQuestionCountry(questionText: string): string {
  const text = questionText.trim();
  if (!text) return "";
  const lowerText = text.toLowerCase();
  for (const [alias, countryName] of ASTRBOT_COUNTRY_MENTION_ALIASES) {
    if (aliasAppearsInText(alias, text, lowerText) && !isNegatedCountryMention(alias, text, lowerText)) return countryName;
  }
  for (const [code, countryName] of Object.entries(ASTRBOT_COUNTRY_CODE_ALIASES)) {
    if (
      new RegExp(`(^|[^A-Za-z])${code}([^A-Za-z]|$)`, "i").test(text)
      && !isNegatedCountryMention(code, text, lowerText)
    ) return countryName;
  }
  return "";
}

function isCountryNegatedInQuestion(countryName: string, questionText: string): boolean {
  const text = questionText.trim();
  if (!countryName || !text) return false;
  const lowerText = text.toLowerCase();
  for (const [alias, aliasCountry] of ASTRBOT_COUNTRY_MENTION_ALIASES) {
    if (aliasCountry === countryName && aliasAppearsInText(alias, text, lowerText) && isNegatedCountryMention(alias, text, lowerText)) {
      return true;
    }
  }
  for (const [code, aliasCountry] of Object.entries(ASTRBOT_COUNTRY_CODE_ALIASES)) {
    if (
      aliasCountry === countryName
      && new RegExp(`(^|[^A-Za-z])${code}([^A-Za-z]|$)`, "i").test(text)
      && isNegatedCountryMention(code, text, lowerText)
    ) {
      return true;
    }
  }
  return false;
}

export function resolveAstrBotRequestCountry(
  questionText: string,
  visibleCountry: string,
  fallbackCountry: string,
): string {
  const inferredCountry = inferAstrBotQuestionCountry(questionText);
  const safeInferredCountry = inferredCountry && !isCountryNegatedInQuestion(inferredCountry, questionText)
    ? inferredCountry
    : "";
  return safeInferredCountry || visibleCountry.trim() || fallbackCountry.trim();
}

export function getAstrBotQuickActionQuestion(actionId: string, country: string): string {
  const action = ASTRBOT_QUICK_ACTIONS.find(item => item.id === actionId) ?? ASTRBOT_QUICK_ACTIONS[0];
  return action.questionTemplate(country.trim() || "selected market");
}

export function formatAstrBotStreamingPlaceholder(country: string, streamStatus?: string): string {
  const market = country.trim() || "selected market";
  const status = streamStatus?.trim();
  if (status) {
    return formatAstrBotVisibleStreamStatus(market, status);
  }
  return `正在准备 ${market} 分析：识别问题、规划证据并选择首个数据工具。`;
}

export function formatAstrBotVisibleStreamStatus(country: string, streamStatus: string): string {
  const market = country.trim() || "selected market";
  const status = streamStatus.trim();
  const normalized = status.toLowerCase();
  if (normalized.includes("calling")) {
    return `正在查询 ${market} 数据：${formatAstrBotStreamToolLabel(status)}。`;
  }
  if (normalized.includes("returned")) {
    return `${market} 数据已返回，正在整理证据和业务结论。`;
  }
  if (normalized.includes("streaming") && normalized.includes("chunk")) {
    const chunkCount = status.match(/(\d+)\s+chunk/i)?.[1];
    return chunkCount ? `正在输出 ${market} 结论，已收到 ${chunkCount} 段内容。` : `正在输出 ${market} 结论。`;
  }
  if (normalized.includes("refining")) {
    return `正在完善 ${market} 最终结论和展示内容。`;
  }
  if (normalized.includes("writing")) {
    return `正在生成 ${market} 业务结论。`;
  }
  if (normalized.includes("waiting") || normalized.includes("still checking")) {
    return `仍在核对 ${market} 证据，等待工具结果或首段答案。`;
  }
  if (normalized.includes("resolved current market")) {
    return `已识别为 ${market} 市场，正在规划证据路径。`;
  }
  return status;
}

function formatAstrBotStreamToolLabel(status: string): string {
  const toolName = status.match(/calling\s+([a-zA-Z0-9_:-]+)/i)?.[1] ?? "";
  const labels: Record<string, string> = {
    query_country_snapshot: "市场快照",
    build_market_chart: "市场结构图",
    query_segment_breakdown: "细分市场拆解",
    query_with_filters: "筛选后的市场数据",
    query_msrp_pricing: "MSRP 价格",
    query_price_positioning: "价格定位",
    compare_competitive_set: "竞品集合",
    compare_vehicle_variants: "车型配置对比",
    external_research: "外部来源检索",
    search_market_news: "市场新闻/政策",
  };
  return labels[toolName] ?? (toolName.replace(/_/g, " ") || "数据工具");
}

export function formatAstrBotAnswerStreamStatus(country: string, chunkCount: number): string {
  const market = country.trim() || "selected market";
  const count = Number.isFinite(chunkCount) && chunkCount > 0 ? Math.round(chunkCount) : 0;
  return count > 1
    ? `Writing ${market} answer in ${count} grounded chunks…`
    : `Writing ${market} grounded answer…`;
}

export function formatAstrBotAnswerRefinementStatus(country: string, chunkCount: number): string {
  const market = country.trim() || "selected market";
  const count = Number.isFinite(chunkCount) && chunkCount > 0 ? Math.round(chunkCount) : 0;
  return count > 1
    ? `Refining ${market} final answer in ${count} grounded chunks…`
    : `Refining ${market} final answer…`;
}

export function formatAstrBotTokenStreamStatus(country: string, chunkCount: number): string {
  const market = country.trim() || "selected market";
  const count = Number.isFinite(chunkCount) && chunkCount > 0 ? Math.round(chunkCount) : 0;
  return count > 0
    ? `Streaming ${market} answer · ${count} chunk${count === 1 ? "" : "s"} received…`
    : `Streaming ${market} answer…`;
}

export function inferAstrBotStreamPhase(isStreaming: boolean, statusText: string): string {
  if (!isStreaming) {
    return "证据路径";
  }
  const normalized = statusText.toLowerCase();
  if (normalized.includes("streaming") && normalized.includes("chunk")) {
    return "正在输出结论";
  }
  if (normalized.includes("refining")) {
    return "完善最终结论";
  }
  if (normalized.includes("writing")) {
    return "生成业务结论";
  }
  if (normalized.includes("calling")) {
    return "正在查数据";
  }
  if (normalized.includes("returned")) {
    return "已收到证据";
  }
  if (normalized.includes("waiting") || normalized.includes("still checking")) {
    return "等待首段答案";
  }
  return "规划分析路径";
}

function streamProgressStage(statusText: string, toolCallCount: number): number {
  const normalized = statusText.toLowerCase();
  if (normalized.includes("streaming") && normalized.includes("chunk")) {
    return 3;
  }
  if (normalized.includes("refining")) {
    return 2;
  }
  if (normalized.includes("writing")) {
    return 2;
  }
  if (toolCallCount > 0 || normalized.includes("calling") || normalized.includes("returned")) {
    return 1;
  }
  return 0;
}

function streamProgressLabels(stage: number): { label: string; state: "done" | "active" | "pending" }[] {
  return [
    { label: "查证据", state: stage > 1 ? "done" : stage === 1 ? "active" : "pending" },
    { label: "写结论", state: stage > 2 ? "done" : stage === 2 ? "active" : "pending" },
    { label: "准备图表", state: stage >= 3 ? "active" : "pending" },
  ];
}

export function selectAstrBotUserTakeaways(message: {
  keyTakeaways?: string[];
  reportReadyBullets?: string[];
  answerBullets?: string[];
}): string[] {
  const keyTakeaways = normalizeAstrBotUserTakeawayList(message.keyTakeaways)
    .filter(item => !isEvidenceReferenceNoiseTakeaway(item))
    .filter(item => !isLeadRepeatTakeaway(item))
    .slice(0, 5);
  if (keyTakeaways.length > 0) {
    return keyTakeaways;
  }
  const answerBullets = normalizeAstrBotUserTakeawayList(message.answerBullets)
    .filter(item => !isEvidenceReferenceNoiseTakeaway(item))
    .filter(item => !isLeadRepeatTakeaway(item));
  const prioritizedAnswerBullets = prioritizeAstrBotAnswerBullets(answerBullets);
  if (prioritizedAnswerBullets.length > 0) {
    return prioritizedAnswerBullets;
  }
  return normalizeAstrBotUserTakeawayList(message.reportReadyBullets)
    .filter(item => !isReportTitleOnlyTakeaway(item))
    .slice(0, 5);
}

function normalizeAstrBotUserTakeawayList(values?: string[]): string[] {
  return Array.from(new Set(
    (values ?? [])
      .map(item => item.replace(/\s+/g, " ").trim())
      .map(localizeAstrBotUserTakeawayLabel)
      .filter(Boolean),
  ));
}

function localizeAstrBotUserTakeawayLabel(value: string): string {
  const text = value.trim();
  const replacements: ReadonlyArray<readonly [RegExp, string]> = [
    [/^Key metrics\s*[:：]\s*/i, "关键指标："],
    [/^Powertrain mix\s*[:：]\s*/i, "动力结构："],
    [/^Top models\s*[:：]\s*/i, "车型证据："],
    [/^Competitor table\s*[:：]\s*/i, "竞品表："],
    [/^Feature delta\s*[:：]\s*/i, "配置差异："],
    [/^Evidence\s*[:：]\s*/i, "证据："],
  ];
  for (const [pattern, label] of replacements) {
    if (pattern.test(text)) {
      return text.replace(pattern, label);
    }
  }
  return text;
}

function isEvidenceReferenceNoiseTakeaway(value: string): boolean {
  const text = value.trim();
  if (!text) {
    return true;
  }
  const lower = text.toLowerCase();
  const hasRefSuffix = /\.(claim|rank|rankseed|source|table|value)\b/.test(lower);
  const hasManyRefSeparators = (text.match(/\s\/\s/g) ?? []).length >= 1 && text.includes("|");
  const evidenceLabelPrefix = /^(key metrics|powertrain mix|top models|competitor table|feature delta|evidence|关键指标|动力结构|车型证据|竞品表|配置差异|证据)[:：]/i.test(text);
  return evidenceLabelPrefix && (hasRefSuffix || hasManyRefSeparators);
}

function isReportTitleOnlyTakeaway(value: string): boolean {
  return /^title[:：]/i.test(value.trim());
}

function isLeadRepeatTakeaway(value: string): boolean {
  const text = value.trim();
  if (!text) {
    return true;
  }
  if (/^直接结论[:：]/.test(text) || /^executive answer[:：]/i.test(text)) {
    return true;
  }
  if (/请简短回答|please answer briefly/i.test(text)) {
    return true;
  }
  return text.length > 240 && /分析对象|直接结论|executive answer/i.test(text);
}

function prioritizeAstrBotAnswerBullets(values: string[]): string[] {
  if (values.length <= 3) {
    return values;
  }
  const priorityPrefixes = [
    "证据有限但可推进",
    "产品经理判断",
    "下一步动作",
    "风险边界",
    "结论",
    "证据",
  ];
  const ordered: string[] = [];
  for (const prefix of priorityPrefixes) {
    const match = values.find(item => item.startsWith(prefix));
    if (match && !ordered.includes(match)) {
      ordered.push(match);
    }
  }
  for (const item of values) {
    if (!ordered.includes(item)) {
      ordered.push(item);
    }
  }
  return ordered.slice(0, 3);
}

function isChartLikeQuestion(questionText: string): boolean {
  const normalized = questionText.toLowerCase();
  return /chart|trend|plot|visual|graph|趋势|图表|折线|柱状|可视化/.test(normalized);
}

function readEvidenceNumber(value: string | number | undefined): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value !== "string") {
    return null;
  }
  const normalized = value.replace(/,/g, "").match(/-?\d+(\.\d+)?/);
  if (!normalized) {
    return null;
  }
  const parsed = Number(normalized[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

export function buildChartFallbackCard(
  questionText: string,
  charts: ChatChart[] | undefined,
  evidencePackage: EvidencePackage | undefined,
): ChartFallbackCardData | null {
  if (!isChartLikeQuestion(questionText) || (charts?.length ?? 0) > 0 || !evidencePackage) {
    return null;
  }

  const rowsByLabel = new Map<string, { value: number; unit: string }>();
  evidencePackage.toolResults.forEach(tool => {
    tool.evidenceRefs.forEach(ref => {
      const value = readEvidenceNumber(ref.value);
      const label = ref.label.trim();
      if (!label || value === null || value <= 0 || rowsByLabel.has(label)) {
        return;
      }
      rowsByLabel.set(label, { value, unit: ref.unit ?? "" });
    });
  });

  const rows = Array.from(rowsByLabel.entries())
    .map(([label, item]) => ({ label, value: item.value, unit: item.unit }))
    .sort((first, second) => second.value - first.value)
    .slice(0, 5);
  if (rows.length === 0) {
    return null;
  }

  const maxValue = Math.max(...rows.map(row => row.value), 1);
  return {
    title: "Snapshot fallback chart",
    notice: "Trend series unavailable; showing current evidence snapshot instead.",
    missingEvidence: isChartLikeQuestion(questionText) ? "monthly trend series" : "plottable chart series",
    rows: rows.map(row => ({
      ...row,
      sharePct: Math.max(6, Math.round((row.value / maxValue) * 100)),
    })),
  };
}

function ProfileSummary({ profile }: { profile: AstrBotAgentProfile | null }) {
  if (!profile) {
    return null;
  }
  return (
    <section className="astrbot-profile-summary" aria-label="Active agent profile">
      <div>
        <span>Active Profile</span>
        <strong>{profile.id}</strong>
      </div>
      <p>{profile.positioning}</p>
    </section>
  );
}

function UserSessionList({
  sessions,
  activeSessionId,
  onSelect,
  onNew,
  disabled,
}: {
  sessions: AgentConversationSession[];
  activeSessionId: string;
  onSelect: (sessionId: string) => void;
  onNew: () => void;
  disabled: boolean;
}) {
  return (
    <aside className="astrbot-user-sessions" aria-label="AstrBot conversations">
      <div className="astrbot-user-sessions-head">
        <div>
          <span>Conversations</span>
          <strong>AstrBot</strong>
        </div>
        <button type="button" className="astrbot-session-new" onClick={onNew} disabled={disabled} aria-label="New AstrBot conversation">
          +
        </button>
      </div>
      <div className="astrbot-user-session-group">
        <span>Recent</span>
        {sessions.slice(0, 12).map(session => (
          <button
            key={session.sessionId}
            type="button"
            className={`astrbot-user-session-row${session.sessionId === activeSessionId ? " is-active" : ""}`}
            onClick={() => onSelect(session.sessionId)}
            disabled={disabled}
          >
            <span>
              <strong>{formatSessionDisplayTitle(session)}</strong>
              {session.sessionId === activeSessionId ? <em>Current</em> : null}
            </span>
            <small>{formatSessionDisplayMeta(session)}</small>
          </button>
        ))}
      </div>
    </aside>
  );
}

export function QuickActionCards({
  country,
  onSelect,
  disabled,
}: {
  country: string;
  onSelect: (action: AstrBotQuickAction, question: string) => void;
  disabled: boolean;
}) {
  return (
    <div className="astrbot-quick-actions" aria-label="AstrBot quick actions">
      {ASTRBOT_QUICK_ACTIONS.map(action => {
        const nextQuestion = action.questionTemplate(country.trim() || "selected market");
        return (
          <button
            key={action.id}
            type="button"
            className="astrbot-quick-action-card"
            onClick={() => onSelect(action, nextQuestion)}
            disabled={disabled}
          >
            <strong>{action.label}</strong>
            <span>{action.description}</span>
          </button>
        );
      })}
    </div>
  );
}

export function ChartFallbackCard({ card }: { card: ChartFallbackCardData }) {
  return (
    <section className="astrbot-chart-fallback-card" aria-label="Chart fallback">
      <div className="astrbot-chart-fallback-heading">
        <div>
          <span>Fallback chart</span>
          <strong>{card.title}</strong>
        </div>
        <small>Missing: {card.missingEvidence}</small>
      </div>
      <p>{card.notice}</p>
      <div className="astrbot-chart-fallback-bars">
        {card.rows.map(row => (
          <div className="astrbot-chart-fallback-row" key={row.label}>
            <span>{row.label}</span>
            <div className="astrbot-chart-fallback-track" aria-hidden="true">
              <i style={{ width: `${row.sharePct}%` }} />
            </div>
            <strong>{row.value.toLocaleString()}{row.unit ? ` ${row.unit}` : ""}</strong>
          </div>
        ))}
      </div>
    </section>
  );
}

function readArtifactRows(value: unknown): Record<string, unknown>[] {
  if (Array.isArray(value)) {
    return value.map(item => asRecord(item)).filter((item): item is Record<string, unknown> => Boolean(item));
  }
  const record = asRecord(value);
  if (Array.isArray(record?.rows)) {
    return record.rows.map(item => asRecord(item)).filter((item): item is Record<string, unknown> => Boolean(item));
  }
  return [];
}

function formatArtifactValue(value: unknown, unit?: string): string {
  const suffix = unit ? ` ${unit}` : "";
  if (typeof value === "number" && Number.isFinite(value)) {
    return localizeArtifactDisplayText(`${value.toLocaleString()}${suffix}`);
  }
  if (typeof value === "string" && value.trim()) {
    return localizeArtifactDisplayText(`${value}${suffix}`);
  }
  return `n/a${suffix}`;
}

function localizeArtifactDisplayText(value: string): string {
  const text = value.trim();
  const exactLabels: Record<string, string> = {
    "Market table converts snapshot evidence into business implications and next product actions.": "市场表把快照证据转成业务含义和下一步产品动作。",
    "Cross-tab sales signals from the market snapshot.": "来自市场快照的交叉表销售信号。",
    "Cross-tab bars show evidence-backed sales signals by dimension; they are not additive market totals.": "交叉表柱状图按维度展示有证据支撑的销售信号，不代表可相加的市场总量。",
    "Visual artifact generated by the agent tool chain.": "由 Agent 工具链基于证据生成。",
    "Evidence-backed route table.": "基于证据的动力路线决策表。",
    "This table consolidates powertrain evidence into product-route decisions.": "把动力证据收敛成产品路线判断。",
    "This table consolidates powertrain evidence into product-route decisions instead of scattering HEV/PHEV metrics across generic evidence rows.": "把 HEV/PHEV 证据收敛成产品路线判断，避免散落在普通证据行里。",
    "Main table is capped at seven business columns.": "主表最多展示 7 个业务字段。",
    "Main table is capped at seven business columns; raw redundant fields remain in evidencePackage / Analysis Path.": "主表最多展示 7 个业务字段，冗余原始字段保留在证据包和分析路径中。",
    "Market structure support table; it does not replace official MSRP, monthly payment, RV or configuration-delta validation.": "这是市场结构支持表，不能替代官方 MSRP、月供、残值或配置差异验证。",
    "Supplemental policy-market table; official policy sources remain in the primary policy table.": "补充政策市场表；官方政策来源仍以主政策表为准。",
    "Supplemental policy-pricing table; official policy sources remain in the primary policy table.": "补充政策定价表；官方政策来源仍以主政策表为准。",
    "Supplemental report-pricing table; it supports the PPT block but does not replace official MSRP repair requirements.": "补充报告定价表；可支撑 PPT 汇报块，但不能替代官方 MSRP 修复。",
    "Pending official-source MSRP observations that can guide review and table layout, but cannot support final price claims yet.": "待审核的官方 MSRP 观察项可用于审核和排版，但暂不能支撑最终价格结论。",
    "Pending observations are not accepted current price evidence until human approval or deterministic override is applied.": "待审核观察项在人工确认或确定性规则通过前，不能作为当前价格证据。",
    "Validation matrix for turning search queries or source candidates into citation-ready VOC/news/configuration evidence.": "用于把搜索线索或候选来源转成可引用 VOC、新闻或配置证据的验证矩阵。",
    "Search queries and source candidates are displayed as validation tasks, not as evidence-backed facts.": "搜索线索和候选来源只作为验证任务展示，不能当作已证实事实。",
    "TCO validation table separates market/channel background from the missing monthly, residual-value, tax and usage evidence needed for company-car or leasing conclusions.": "TCO 验证表把市场/渠道背景与公司车或租赁结论所需的月供、残值、税费和使用场景证据分开。",
    "Candidate validation requirements are not numeric evidence until source/tool rows are materialized in EvidencePackage.": "候选验证要求在工具或来源生成证据引用前，不是可用数字证据。",
    "BOM/entity validation table separates inventory/BOM mapping requirements from ordinary market metrics so material-code answers are traceable and actionable.": "BOM 实体映射表把库存/BOM 关系与普通市场指标分开，让物料号回答可追溯、可执行。",
    "Rows are validation requirements until the matching source/tool produces evidenceRefs for the entity relationship.": "在匹配来源或工具产出实体关系证据引用前，这些行都是验证要求。",
    "Multi-model report coverage table keeps the target model and benchmark vehicles in one view, so partial evidence does not become a full competitor conclusion.": "多车型覆盖表把目标车型和基准车型放在同一视图，避免把部分证据误写成完整竞品结论。",
    "Coverage rows show what is proven now and which fields must be repaired before a PPT claim becomes presentation-ready.": "覆盖行显示当前已证实内容，以及 PPT 结论发布前需要修复的字段。",
    "Build price matrix.": "生成价格矩阵。",
    "Prioritize SUV A price band and winter package.": "优先验证 SUV A 价格带和冬季包。",
    "Use core corridor plus high-trim push.": "采用核心价格带中段 + 高配主推逻辑。",
    "core corridor": "核心价格带",
    high: "高",
    medium: "中",
    low: "低",
  };
  return exactLabels[text] ?? text;
}

function isLongArtifactColumn(key: string): boolean {
  return [
    "action",
    "recommendedAction",
    "businessImplication",
    "productImplication",
    "evidenceStatus",
    "nextAction",
    "decisionUse",
    "risk",
    "gap",
    "customerValue",
  ].includes(key);
}

function artifactTableCellClassName(key: string, value: string): string {
  const classes = ["astrbot-artifact-table-cell"];
  if (isLongArtifactColumn(key) || value.length > 96) {
    classes.push("is-long");
  }
  if (value === "n/a") {
    classes.push("is-empty");
  }
  return classes.join(" ");
}

function formatArtifactColumnLabel(key: string): string {
  const labels: Record<string, string> = {
    model: "车型",
    evidenceStatus: "证据状态",
    powertrain: "动力",
    msrp: "MSRP",
    price: "价格",
    monthlyPayment: "月供/租赁",
    rv: "残值/RV",
    pricePosition: "价格位置",
    action: "动作",
    segment: "细分市场",
    keyAdvantage: "关键优势",
    gapVsOj: "与 OJ 差距",
    businessImplication: "业务含义",
    recommendedAction: "建议动作",
    productImplication: "产品含义",
    feature: "配置项",
    targetModel: "目标车型",
    competitor: "竞品",
    gap: "缺口",
    customerValue: "用户价值",
    priority: "优先级",
    market: "市场",
    version: "版本",
    colorSpec: "内外饰",
    materialCode: "物料号",
    availableUnits: "可用数量",
    risk: "风险",
    candidateRole: "证据角色",
    sourceType: "来源类型",
    sourceStatus: "来源状态",
    reviewPendingRows: "待审观察",
    readiness: "可用状态",
    reviewStatus: "审核状态",
    requiredFields: "待补字段",
    sourceScope: "来源范围",
    searchQuery: "来源线索",
    nextStep: "下一步",
    decisionUse: "决策用途",
    localMsrp: "本币 MSRP",
    eurMsrp: "EUR MSRP",
    source: "来源",
    routeRole: "路线角色",
    productAction: "产品动作",
    twoWheelDrive: "2WD",
    fourWheelDrive: "4WD",
    sales: "销量",
    share: "份额",
    dimension: "维度",
    signal: "信号",
    evidence: "证据",
    confidence: "置信度",
    theme: "主题",
    evidenceSignal: "证据信号",
    validationStatus: "验证状态",
    businessUse: "业务用途",
    section: "章节",
    value: "数值",
    label: "指标",
    unit: "单位",
  };
  if (labels[key]) {
    return labels[key];
  }
  return key
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, value => value.toUpperCase());
}

function buildArtifactChartPlotly(artifact: VisualArtifact): { data: PlotlyChartProps["data"]; layout: PlotlyChartProps["layout"] } | null {
  const spec = artifact.spec ?? {};
  const plotlyData = Array.isArray(spec.plotlyData) ? spec.plotlyData as PlotlyChartProps["data"] : null;
  const plotlyLayout = asRecord(spec.plotlyLayout) as PlotlyChartProps["layout"] | null;
  if (plotlyData && plotlyData.length > 0) {
    return {
      data: plotlyData,
      layout: plotlyLayout ?? { title: { text: artifactDisplayTitle(artifact) }, height: 360 },
    };
  }

  const rows = readArtifactRows(spec.data ?? artifact.data);
  if (rows.length === 0) {
    return null;
  }
  const chartType = readText(spec.chartType, "bar");
  const xField = readText(spec.xField, "label");
  const yField = readText(spec.yField, "value");
  const labels = rows.map(row => readText(row[xField], readText(row.label, readText(row.x, ""))));
  const values = rows.map(row => readNumber(row[yField], readNumber(row.value, readNumber(row.y, 0))));
  if (!labels.some(Boolean) || !values.some(value => value !== 0)) {
    return null;
  }
  if (chartType === "line") {
    return {
      data: [{ x: labels, y: values, type: "scatter", mode: "lines+markers", line: { color: "#1c69d4", width: 2 } }] as PlotlyChartProps["data"],
      layout: { title: { text: artifactDisplayTitle(artifact) }, height: 340, margin: { l: 48, r: 18, t: 42, b: 48 } },
    };
  }
  return {
    data: [{ x: values, y: labels, type: "bar", orientation: "h", marker: { color: "#1c69d4" } }] as PlotlyChartProps["data"],
    layout: { title: { text: artifactDisplayTitle(artifact) }, height: Math.max(300, labels.length * 32), margin: { l: 140, r: 18, t: 42, b: 48 }, yaxis: { autorange: "reversed" } },
  };
}

export function VisualArtifactsDeck({
  artifacts,
  deckId = "",
  compact = false,
}: {
  artifacts: VisualArtifact[];
  deckId?: string;
  compact?: boolean;
}) {
  if (artifacts.length === 0) {
    return null;
  }
  const visibleArtifacts = compact ? selectPrimaryVisualArtifacts(artifacts) : artifacts;
  const visibleIds = new Set(visibleArtifacts.map(artifact => artifact.id));
  const supplementalArtifacts = compact
    ? artifacts.filter(artifact => !visibleIds.has(artifact.id))
    : [];
  return (
    <div className={compact ? "astrbot-visual-artifacts is-compact" : "astrbot-visual-artifacts"} aria-label="证据展示">
      <div className="astrbot-visual-artifacts-head">
        <div>
          <span>证据展示</span>
          <strong>可复用图表、表格和汇报块</strong>
        </div>
        <small>{artifacts.length} 个输出</small>
      </div>
      <ArtifactSummaryStrip artifacts={visibleArtifacts} deckId={deckId} />
      {visibleArtifacts.map(artifact => (
        <VisualArtifactCard artifact={artifact} deckId={deckId} key={artifact.id} />
      ))}
      {supplementalArtifacts.length > 0 ? (
        <details className="astrbot-supplemental-artifacts">
          <summary>
            <span>更多证据输出</span>
            <strong>{supplementalArtifacts.length} 个补充图表、表格或汇报块</strong>
          </summary>
          <div>
            {supplementalArtifacts.map(artifact => (
              <VisualArtifactCard artifact={artifact} deckId={deckId} key={artifact.id} />
            ))}
          </div>
        </details>
      ) : null}
    </div>
  );
}

function selectPrimaryVisualArtifacts(artifacts: VisualArtifact[]): VisualArtifact[] {
  const metricCards = artifacts.find(artifact => artifact.type === "metric_cards");
  const decisionTable = artifacts.find(artifact => (
    artifact.type === "table"
    && /competitor|pricing|configuration|powertrain/.test(artifact.id)
  ));
  const firstChart = artifacts.find(artifact => artifact.type === "chart");
  const firstTable = artifacts.find(artifact => artifact.type === "table");
  const primaryDecisionArtifact = decisionTable ?? firstChart ?? firstTable ?? artifacts[0];
  return Array.from(new Map(
    [metricCards, primaryDecisionArtifact]
      .filter((artifact): artifact is VisualArtifact => Boolean(artifact))
      .map(artifact => [artifact.id, artifact]),
  ).values());
}

function ArtifactSummaryStrip({ artifacts, deckId }: { artifacts: VisualArtifact[]; deckId: string }) {
  return (
    <div className="astrbot-visual-summary-strip" aria-label="证据展示摘要">
      {artifacts.slice(0, 4).map(artifact => (
        <a aria-label={`跳转到${artifactDisplayTitle(artifact)}`} href={`#${artifactDomId(artifact, deckId)}`} key={`summary-${artifact.id}`}>
          <span>{artifactTypeLabel(artifact.type)}</span>
          <strong>{artifactDisplayTitle(artifact)}</strong>
          <small>{artifactEvidenceLabel(artifact)}</small>
        </a>
      ))}
    </div>
  );
}

function artifactDomId(artifact: VisualArtifact, deckId = ""): string {
  const safeId = artifact.id
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  const safeDeckId = deckId
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `astrbot-artifact-${safeDeckId ? `${safeDeckId}-` : ""}${safeId || artifact.type}`;
}

function artifactTypeLabel(type: VisualArtifactType): string {
  if (type === "metric_cards") return "指标";
  if (type === "report_block") return "汇报块";
  if (type === "chart") return "图表";
  return "表格";
}

function artifactDisplayTitle(artifact: VisualArtifact): string {
  const id = artifact.id.trim();
  const rawTitle = artifact.title.trim();
  const idTitles: Record<string, string> = {
    artifact_metric_cards: "关键指标",
    artifact_market_structure_chart: "市场结构图",
    artifact_market_powertrain_mix_chart: "动力结构图",
    artifact_pricing_corridor_chart: rawTitle.toLowerCase().includes("reference sample") ? "价格参考样本图" : "定价走廊图",
    artifact_pricing_corridor_table: "定价走廊表",
    artifact_competitor_evidence_chart: "竞品证据图",
    artifact_pending_msrp_review_chart: "待审核 MSRP 阶梯图",
    top_ranking: "Top 车型排行",
    artifact_competitor_compare_table: "竞品对比表",
    artifact_pricing_analysis_table: "价格证据表",
    artifact_pricing_market_structure_table: "价格相关市场结构表",
    artifact_msrp_source_repair_table: "MSRP 来源验证表",
    artifact_pending_msrp_review_table: "待审核 MSRP 表",
    artifact_external_source_repair_table: "外部来源验证表",
    artifact_powertrain_route_table: "HEV / PHEV 路线决策表",
    artifact_market_overview_table: "市场决策表",
    artifact_tco_validation_table: "TCO 验证表",
    artifact_bom_entity_validation_table: "BOM 实体映射验证表",
    artifact_inventory_analysis_table: "库存 / BOM 证据表",
    artifact_configuration_validation_table: "配置验证矩阵",
    artifact_report_block: "PPT 汇报块",
  };
  if (idTitles[id]) return idTitles[id];
  const titleMap: Record<string, string> = {
    "Key metrics": "关键指标",
    "Market structure chart": "市场结构图",
    "Powertrain mix chart": "动力结构图",
    "Pricing corridor chart": "定价走廊图",
    "Pricing corridor table": "定价走廊表",
    "Pricing reference sample chart": "价格参考样本图",
    "Pricing evidence table": "价格证据表",
    "Pricing market structure evidence table": "价格相关市场结构表",
    "Competitor comparison table": "竞品对比表",
    "Market decision table": "市场决策表",
    "HEV / PHEV route comparison table": "HEV / PHEV 路线决策表",
    "PPT-ready block": "PPT 汇报块",
    "Current evidence snapshot": "当前证据快照",
    "Top Models": "Top 车型排行",
  };
  return titleMap[rawTitle] ?? rawTitle;
}

function artifactSubtitleLabel(value: string): string {
  const text = value.trim();
  const labels: Record<string, string> = {
    "Evidence-backed route table.": "基于证据的动力路线决策表。",
    "Top rows from the latest available evidence.": "来自当前可用证据的重点行。",
    "This table consolidates powertrain evidence into product-route decisions.": "把动力证据收敛成产品路线判断。",
    "Main table is capped at seven business columns.": "主表最多展示 7 个业务字段。",
  };
  return labels[text] ?? localizeArtifactDisplayText(text);
}

function artifactFallbackLabel(value: string): string {
  const text = value.trim();
  const labels: Record<string, string> = {
    monthly_trend_series: "缺少月度趋势序列",
    "monthly trend series missing": "缺少月度趋势序列",
    "monthly trend series unavailable": "缺少月度趋势序列",
    source_repair_candidates: "需要先验证来源",
    external_source_repair_candidates: "需要先验证外部来源",
  };
  return labels[text] ?? text.replace(/_/g, " ");
}

function artifactEvidenceLabel(artifact: VisualArtifact): string {
  const evidenceCount = artifact.sourceEvidenceRefs.length;
  if (artifact.fallbackReason) {
    return `备用展示 · ${artifactFallbackLabel(artifact.fallbackReason)}`;
  }
  if (evidenceCount > 0) {
    return `${evidenceCount} 条证据`;
  }
  return "证据待补";
}

function VisualArtifactCard({ artifact, deckId }: { artifact: VisualArtifact; deckId: string }) {
  if (artifact.type === "metric_cards") {
    return <MetricCardsArtifact artifact={artifact} deckId={deckId} />;
  }
  if (artifact.type === "chart") {
    return <ChartArtifact artifact={artifact} deckId={deckId} />;
  }
  if (artifact.type === "table") {
    if (isPowertrainRouteArtifact(artifact)) {
      return <PowertrainRouteArtifact artifact={artifact} deckId={deckId} />;
    }
    return <TableArtifact artifact={artifact} deckId={deckId} />;
  }
  return <ReportBlockArtifact artifact={artifact} deckId={deckId} />;
}

function isPowertrainRouteArtifact(artifact: VisualArtifact): boolean {
  return artifact.id === "artifact_powertrain_route_table";
}

function ArtifactHeader({ artifact }: { artifact: VisualArtifact }) {
  const displayTitle = artifactDisplayTitle(artifact);
  return (
    <header>
      <div>
        <span>{artifactTypeLabel(artifact.type)}</span>
        <strong>{displayTitle}</strong>
      </div>
      <em>{artifactEvidenceLabel(artifact)}</em>
      {artifact.fallbackReason ? <small>{artifactFallbackLabel(artifact.fallbackReason)}</small> : artifact.subtitle ? <small>{artifactSubtitleLabel(artifact.subtitle)}</small> : null}
    </header>
  );
}

function MetricCardsArtifact({ artifact, deckId }: { artifact: VisualArtifact; deckId: string }) {
  const rows = readArtifactRows(artifact.data);
  return (
    <section className="astrbot-visual-card astrbot-visual-metrics" id={artifactDomId(artifact, deckId)} aria-label={artifactDisplayTitle(artifact)}>
      <ArtifactHeader artifact={artifact} />
      <div className="astrbot-visual-metric-grid">
        {rows.slice(0, 6).map(row => (
          <div key={`${readText(row.label, "metric")}-${readText(row.sourceEvidenceRef, "")}`}>
            <span>{localizeArtifactDisplayText(readText(row.label, "Metric"))}</span>
            <strong>{formatArtifactValue(row.value, readText(row.unit, ""))}</strong>
            {row.source ? <small>{readText(row.source, "")}</small> : null}
          </div>
        ))}
      </div>
    </section>
  );
}

function ChartArtifact({ artifact, deckId }: { artifact: VisualArtifact; deckId: string }) {
  const plotly = buildArtifactChartPlotly(artifact);
  return (
    <section className="astrbot-visual-card astrbot-visual-chart" id={artifactDomId(artifact, deckId)} aria-label={artifactDisplayTitle(artifact)}>
      <ArtifactHeader artifact={artifact} />
      {artifact.spec?.note ? <p>{artifactSubtitleLabel(readText(artifact.spec.note, ""))}</p> : null}
      {plotly ? (
        <LazyPlotlyChart data={plotly.data} layout={plotly.layout} height={Math.min(420, readNumber(asRecord(plotly.layout)?.height, 360))} />
      ) : (
        <div className="astrbot-table-empty">图表数据暂不可用。</div>
      )}
    </section>
  );
}

function PowertrainRouteArtifact({ artifact, deckId }: { artifact: VisualArtifact; deckId: string }) {
  const rows = readArtifactRows(artifact.data);
  if (rows.length === 0) {
    return null;
  }
  return (
    <section className="astrbot-visual-card astrbot-powertrain-route-card" id={artifactDomId(artifact, deckId)} aria-label={artifactDisplayTitle(artifact)}>
      <ArtifactHeader artifact={artifact} />
      {typeof artifact.spec?.businessExplanation === "string" ? <p>{artifactSubtitleLabel(artifact.spec.businessExplanation)}</p> : null}
      <div className="astrbot-powertrain-route-grid">
        {rows.slice(0, 3).map(row => {
          const powertrain = readText(row.powertrain, "路线");
          const role = readText(row.routeRole, "路线角色待判断");
          const action = readText(row.productAction, "下一步动作待补");
          return (
            <article className="astrbot-powertrain-route-item" key={`${powertrain}-${role}`}>
              <div className="astrbot-powertrain-route-heading">
                <span>{powertrain}</span>
                <strong>{role}</strong>
              </div>
              <div className="astrbot-powertrain-route-metrics">
                <RouteMetric label="销量" value={row.sales} />
                <RouteMetric label="份额" value={row.share} />
                <RouteMetric label="2WD" value={row.twoWheelDrive} />
                <RouteMetric label="4WD" value={row.fourWheelDrive} />
              </div>
              <p>{action}</p>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function RouteMetric({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{formatArtifactValue(value)}</strong>
    </div>
  );
}

function TableArtifact({ artifact, deckId }: { artifact: VisualArtifact; deckId: string }) {
  const rows = readArtifactRows(artifact.data);
  if (rows.length === 0) {
    return null;
  }
  const schemaColumns = readStringList(artifact.spec?.columns)
    .filter(key => rows.some(row => Object.prototype.hasOwnProperty.call(row, key)))
    .slice(0, 8);
  const rowKeys = Array.from(new Set(rows.flatMap(row => Object.keys(row)))).filter(key => key !== "evidenceRef" && key !== "source").slice(0, 8);
  const keys = schemaColumns.length > 0 ? schemaColumns : rowKeys;
  return (
    <section className="astrbot-visual-card astrbot-visual-table" id={artifactDomId(artifact, deckId)} aria-label={artifactDisplayTitle(artifact)}>
      <ArtifactHeader artifact={artifact} />
      {typeof artifact.spec?.businessExplanation === "string" ? <p>{artifactSubtitleLabel(artifact.spec.businessExplanation)}</p> : null}
      {typeof artifact.spec?.columnPolicy === "string" ? <small className="astrbot-visual-table-policy">{artifactSubtitleLabel(artifact.spec.columnPolicy)}</small> : null}
      <div className="astrbot-table-shell">
        <table className="astrbot-table">
          <thead>
            <tr>
              {keys.map(key => <th key={key}>{formatArtifactColumnLabel(key)}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.slice(0, 8).map((row, index) => (
              <tr key={`${artifact.id}-${index}`}>
                {keys.map(key => {
          const cellText = formatArtifactValue(row[key]);
                  return (
                    <td className={artifactTableCellClassName(key, cellText)} key={key}>
                      <span title={cellText}>{cellText}</span>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ReportBlockArtifact({ artifact, deckId }: { artifact: VisualArtifact; deckId: string }) {
  const data = asRecord(artifact.data) ?? {};
  const evidence = readStringList(data.evidence);
  return (
    <section className="astrbot-visual-card astrbot-report-block" id={artifactDomId(artifact, deckId)} aria-label={artifactDisplayTitle(artifact)}>
      <ArtifactHeader artifact={artifact} />
      <div className="astrbot-report-block-grid">
        <div>
          <span>标题</span>
          <strong>{localizeArtifactDisplayText(readText(data.title, artifact.title))}</strong>
        </div>
        <div>
          <span>核心结论</span>
          <p>{localizeArtifactDisplayText(readText(data.keyMessage, ""))}</p>
        </div>
        <div>
          <span>证据</span>
          <ul>{evidence.slice(0, 4).map(item => <li key={item}>{localizeArtifactDisplayText(item)}</li>)}</ul>
        </div>
        <div>
          <span>产品含义</span>
          <p>{localizeArtifactDisplayText(readText(data.productImplication, ""))}</p>
        </div>
        <div>
          <span>下一步动作</span>
          <p>{localizeArtifactDisplayText(readText(data.nextAction, ""))}</p>
        </div>
      </div>
    </section>
  );
}

export function buildSummaryCopyText({
  title,
  summary,
  takeaways,
  pmInsight,
}: {
  title: string;
  summary: string;
  takeaways: string[];
  pmInsight: string;
}): string {
  const lines = [title, "", summary];
  if (takeaways.length > 0) {
    lines.push("", "Key takeaways:", ...takeaways.slice(0, 5).map(item => `- ${item}`));
  }
  if (pmInsight) {
    lines.push("", `Product implication: ${pmInsight}`);
  }
  return lines.filter((line, index, source) => line !== "" || source[index - 1] !== "").join("\n").trim();
}

export function buildPptBlockCopyText(artifact: VisualArtifact | undefined, fallback: { title: string; summary: string; evidence: string[]; pmInsight: string; nextAction: string }): string {
  const data = artifact ? asRecord(artifact.data) ?? {} : {};
  const evidence = readStringList(data.evidence);
  return [
    `Title: ${readText(data.title, fallback.title)}`,
    `Key message: ${readText(data.keyMessage, fallback.summary)}`,
    `Evidence: ${(evidence.length ? evidence : fallback.evidence).slice(0, 3).join(" / ") || "n/a"}`,
    `Product implication: ${readText(data.productImplication, fallback.pmInsight) || "n/a"}`,
    `Next action: ${readText(data.nextAction, fallback.nextAction) || "n/a"}`,
  ].join("\n");
}

export function AnswerCopyActions({
  title,
  summary,
  takeaways,
  pmInsight,
  visualArtifacts,
  nextAction,
}: {
  title: string;
  summary: string;
  takeaways: string[];
  pmInsight: string;
  visualArtifacts: VisualArtifact[];
  nextAction: string;
}) {
  const [copyState, setCopyState] = useState("");
  const reportArtifact = visualArtifacts.find(item => item.type === "report_block");

  async function copy(label: string, text: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(text);
      setCopyState(`${label} copied`);
    } catch {
      setCopyState("Copy unavailable");
    }
  }

  const summaryText = buildSummaryCopyText({ title, summary, takeaways, pmInsight });
  const pptText = buildPptBlockCopyText(reportArtifact, {
    title,
    summary,
    evidence: takeaways,
    pmInsight,
    nextAction,
  });
  return (
    <div className="astrbot-answer-copy-actions" aria-label="Answer copy actions">
      <button type="button" onClick={() => void copy("Summary", summaryText)}>Copy summary</button>
      <button type="button" onClick={() => void copy("PPT block", pptText)}>Copy PPT block</button>
      {copyState ? <span>{copyState}</span> : null}
    </div>
  );
}

interface ExecutiveAnswerBlockProps {
  text: string;
  isStreaming?: boolean;
  streamPlaceholder?: string;
  takeaways?: string[];
  pmInsight?: string;
  actions?: RecommendedAction[];
}

function ExecutiveAnswerBlock({
  text,
  isStreaming = false,
  streamPlaceholder = "",
  takeaways = [],
  pmInsight = "",
  actions = [],
}: ExecutiveAnswerBlockProps) {
  const visibleTakeaways = takeaways.filter(Boolean).slice(0, 3);
  const visibleActions = actions
    .filter(action => action.action.trim())
    .sort(compareExecutiveActions)
    .slice(0, 2);
  const hasSupport = visibleTakeaways.length > 0 || Boolean(pmInsight.trim()) || visibleActions.length > 0;
  const displayText = formatUserFacingAnswerText(text);

  return (
    <section className="astrbot-executive-answer" aria-label="业务结论">
      <div className="astrbot-executive-answer-lead">
        <span>业务结论</span>
        <p className={isStreaming ? "is-streaming-text" : ""}>
          {displayText || (isStreaming ? streamPlaceholder || "正在整理证据结论…" : "")}
        </p>
      </div>
      {hasSupport ? (
        <div className="astrbot-executive-answer-grid">
          {visibleTakeaways.length > 0 ? (
            <div>
              <strong>关键证据</strong>
              <ul>
                {visibleTakeaways.map(item => <li key={item}>{item}</li>)}
              </ul>
            </div>
          ) : null}
          {pmInsight.trim() ? (
            <div>
              <strong>产品判断</strong>
              <p>{pmInsight}</p>
            </div>
          ) : null}
          {visibleActions.length > 0 ? (
            <div>
              <strong>下一步动作</strong>
              <ol>
                {visibleActions.map(action => (
                  <li key={`${action.priority}-${action.action}`}>
                    <span>{action.priority}</span>
                    {action.action}
                  </li>
                ))}
              </ol>
            </div>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}

export function AstrBotMarkdownAnswer({
  text,
  isStreaming = false,
  streamPlaceholder = "",
}: {
  text: string;
  isStreaming?: boolean;
  streamPlaceholder?: string;
}) {
  const visibleText = formatUserFacingAnswerText(text)
    || (isStreaming ? streamPlaceholder || "正在整理证据结论…" : "");
  return (
    <div
      className={isStreaming ? "astrbot-markdown-answer is-streaming" : "astrbot-markdown-answer"}
      aria-busy={isStreaming}
      aria-live="polite"
      dangerouslySetInnerHTML={{ __html: renderMarkdown(visibleText) }}
    />
  );
}

export function formatUserFacingAnswerText(value: string): string {
  const cleaned = value
    .replace(/^\s*直接结论[:：]\s*/, "")
    .replace(/^\s*结论[:：]\s*/, "")
    .trim();
  if (!cleaned || cleaned.includes("\n\n")) {
    return cleaned;
  }
  return cleaned.replace(
    /\s+(市场结构证据|关键证据|价格证据|配置价值|价值解释|业务含义|数据边界|证据边界|风险边界|价差边界|补源状态|待审核价格状态|下一步执行)[:：]\s*/g,
    "\n\n**$1：** ",
  );
}

function compareExecutiveActions(first: RecommendedAction, second: RecommendedAction): number {
  const firstRank = executiveActionRank(first);
  const secondRank = executiveActionRank(second);
  if (firstRank !== secondRank) {
    return firstRank - secondRank;
  }
  return priorityRank(first.priority) - priorityRank(second.priority);
}

function executiveActionRank(action: RecommendedAction): number {
  const text = `${action.action} ${action.rationale}`.toLowerCase();
  if (
    text.includes("source repair")
    || text.includes("repair candidate")
    || text.includes("repair queue")
    || text.includes("来源修复")
    || text.includes("补数清单")
  ) {
    return 2;
  }
  if (text.includes("ppt") || text.includes("汇报") || text.includes("主推") || text.includes("价格矩阵") || text.includes("配置")) {
    return 0;
  }
  return 1;
}

function priorityRank(priority: string): number {
  const normalized = priority.trim().toUpperCase();
  if (normalized === "P0") {
    return 0;
  }
  if (normalized === "P1") {
    return 1;
  }
  if (normalized === "P2") {
    return 2;
  }
  return 3;
}

export function AgentMessageStatus({
  isStreaming,
  streamStatus,
  country = "",
  toolCalls,
}: {
  isStreaming?: boolean;
  streamStatus?: string;
  country?: string;
  toolCalls?: string[];
}) {
  if (!isStreaming && (!toolCalls || toolCalls.length === 0)) {
    return null;
  }
  const toolCallCount = toolCalls?.length ?? 0;
  const statusText = isStreaming ? streamStatus || "Analyzing with governed tools…" : "分析完成，证据路径可展开查看。";
  const visibleStatusText = isStreaming
    ? formatAstrBotVisibleStreamStatus(country, statusText)
    : statusText;
  const phaseLabel = inferAstrBotStreamPhase(Boolean(isStreaming), statusText);
  const market = country.trim();
  const stage = streamProgressStage(statusText, toolCallCount);
  const progressText = isStreaming
    ? toolCallCount > 0 ? `已收到 ${toolCallCount} 个工具信号` : "等待首个数据工具"
    : `使用 ${toolCallCount} 个工具`;
  return (
    <div className={isStreaming ? "astrbot-message-status-card is-streaming" : "astrbot-message-status-card"} aria-live={isStreaming ? "polite" : undefined}>
      <span aria-hidden="true" />
      <div>
        <strong>{phaseLabel}</strong>
        <p>{visibleStatusText}</p>
      </div>
      <small>
        <span className="astrbot-stream-badge">{isStreaming ? "实时" : "完成"}</span>
        {market ? `${market} · ${progressText}` : progressText}
      </small>
      {isStreaming ? (
        <div className="astrbot-stream-progress" aria-label="Streaming progress">
          {streamProgressLabels(stage).map(item => (
            <span className={`is-${item.state}`} key={item.label}>{item.label}</span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function StreamingArtifactPreview({
  isStreaming,
  toolCalls,
  statusText,
}: {
  isStreaming?: boolean;
  toolCalls?: string[];
  statusText?: string;
}) {
  if (!isStreaming) {
    return null;
  }
  const toolCount = toolCalls?.length ?? 0;
  const stage = streamProgressStage(statusText ?? "", toolCount);
  const items = [
    {
      title: "证据",
      body: toolCount > 0 ? `已收到 ${toolCount} 个工具结果` : "等待首个工具结果",
      state: stage >= 1 ? "active" : "pending",
    },
    {
      title: "结论",
      body: stage >= 2 ? "正在输出有证据支撑的结论" : "正在准备业务判断",
      state: stage >= 2 ? "active" : "pending",
    },
    {
      title: "图表/表格",
      body: stage >= 3 ? "完成后附上图表、表格和汇报块" : "证据包完成后生成",
      state: stage >= 3 ? "active" : "pending",
    },
  ];
  return (
    <div className="astrbot-streaming-artifact-preview" aria-label="Streaming output preview">
      {items.map(item => (
        <section className={`is-${item.state}`} key={item.title}>
          <span>{item.title}</span>
          <strong>{item.body}</strong>
        </section>
      ))}
    </div>
  );
}

function streamStatusForThinkingStep(step: ThinkingStep): string {
  if (step.type === "tool_call") {
    return step.tool ? `Calling ${step.tool}…` : "Calling selected tool…";
  }
  if (step.type === "tool_result") {
    return step.tool ? `${step.tool} returned evidence.` : "Tool returned evidence.";
  }
  if (step.type === "tool_error") {
    return step.tool ? `${step.tool} failed, checking fallback path…` : "Tool failed, checking fallback path…";
  }
  return step.message || "Analyzing with governed tools…";
}

function modeFromSkill(skill: AstrBotAgentSkill | null): AgentMode {
  const routeMode = skill?.routeMode;
  if (routeMode === "chart" || routeMode === "snapshot" || routeMode === "pricing" || routeMode === "news" || routeMode === "research" || routeMode === "variant") {
    return routeMode;
  }
  return "auto";
}

function AgentPanel({
  profile,
  skills,
  developerMode,
}: {
  profile: AstrBotAgentProfile | null;
  skills: AstrBotAgentSkill[];
  developerMode: boolean;
}) {
  const defaultSkill = skills[0] ?? null;
  const [country, setCountry] = useState(developerMode ? "Sweden" : "");
  const [skillId, setSkillId] = useState(defaultSkill?.id ?? "auto_route");
  const [mode, setMode] = useState<AgentMode>("auto");
  const [researchMode, setResearchMode] = useState<ResearchDepth>("standard");
  const [question, setQuestion] = useState(developerMode ? "Draw a 2025 BEV market trend chart and explain the key movement." : "");
  const countryInputRef = useRef<HTMLInputElement | null>(null);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [sessionId, setSessionId] = useState(readInitialAgentSessionId);
  const [sessionOptions, setSessionOptions] = useState<AgentConversationSession[]>(() => mergeSessionOptions(readInitialAgentSessionId(), []));
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [sessionListError, setSessionListError] = useState<string | null>(null);
  const [runLoading, setRunLoading] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [runResult, setRunResult] = useState<AstrBotToolCallResponse | null>(null);
  const chatThreadRef = useRef<HTMLDivElement | null>(null);
  const data = asRecord(runResult?.data);
  const route = asRecord(data?.route);
  const display = asRecord(data?.display);
  const displayCards = (Array.isArray(display?.cards) ? display.cards : [])
    .map(card => asRecord(card))
    .filter((card): card is Record<string, unknown> => Boolean(card));
  const evidencePack = asRecord(data?.evidencePack);
  const evidencePlan = asRecord(data?.evidencePlan) ?? undefined;
  const evidencePackage = readEvidencePackage(data?.evidencePackage);
  const qualityScore = readQualityScore(data?.qualityScore);
  const evidenceItems = Array.isArray(evidencePack?.items) ? evidencePack.items : [];
  const evidenceSources = Array.isArray(evidencePack?.sources) ? evidencePack.sources as string[] : [];
  const evidencePathsContributed = (Array.isArray(evidencePack?.pathsContributed) ? evidencePack.pathsContributed : []) as string[];
  const primaryResult = asRecord(data?.primaryResult);
  const retrievalClassification = asRecord(data?.retrievalClassification);
  const retrievalToolPlan = asRecord(data?.retrievalToolPlan);
  const retrievalSteps = (Array.isArray(retrievalToolPlan?.steps) ? retrievalToolPlan.steps : [])
    .map(step => asRecord(step))
    .filter((step): step is Record<string, unknown> => Boolean(step));
  const retrievalPrimaryLabel = typeof retrievalClassification?.primaryLabel === 'string' ? retrievalClassification.primaryLabel : undefined;
  const retrievalPrimaryPath = typeof retrievalClassification?.primaryPath === 'string' ? retrievalClassification.primaryPath : undefined;
  const retrievalPrimaryConfidence = typeof retrievalClassification?.primaryConfidence === 'string' ? retrievalClassification.primaryConfidence : undefined;
  const retrievalAllPaths = (Array.isArray(retrievalClassification?.allPaths) ? retrievalClassification.allPaths : []) as string[];
  const retrievalSecondaryPaths = (Array.isArray(retrievalClassification?.secondaryPaths) ? retrievalClassification.secondaryPaths : []) as string[];
  const routeSource = readText(data?.routeSource, "legacy");
  // Phase 3: chart specs from primary result
  const primaryResultData = asRecord(primaryResult?.data);
  const chartSpecsResult = asRecord(primaryResultData?.chartSpecs);
  const chartSpecs = (Array.isArray(chartSpecsResult?.charts) ? chartSpecsResult.charts : []) as Record<string, unknown>[];
  const selectedSkill = skills.find(skill => skill.id === skillId) ?? defaultSkill;
  const selectedTool = readText(route?.tool, readText(runResult?.metadata.selectedTool));
  const hasCharts = chartSpecs.length > 0 && selectedTool === "build_market_chart";
  const displaySummary = readText(display?.summary, "The request has been routed to a JATO MCP tool.");
  const answer = readAgentAnswer(runResult);
  const modelUsage = readModelUsage(runResult);
  const canRun = question.trim().length > 0
    && resolveAstrBotRequestCountry(question, country, country).trim().length > 0
    && !runLoading;
  const historyStatus = historyLoading
    ? "Loading"
    : messages.length > 0
      ? `${messages.length} turn${messages.length === 1 ? "" : "s"}`
      : "New session";
  const sessionListStatus = sessionListError
    ? "Unavailable"
    : `${sessionOptions.length} session${sessionOptions.length === 1 ? "" : "s"}`;
  const latestAssistantMessage = messages.slice().reverse().find(message => message.role === "assistant" && !message.isStreaming);
  const resolvedConversationCountry = latestResolvedCountryFromMessages(messages);
  const displayCountry = messages.length > 0
    ? resolvedConversationCountry || country || "selected market"
    : "New conversation";
  const deckCitations = answer?.citations.length ? answer.citations : latestAssistantMessage?.answerCitations ?? [];
  const deckToolCalls = answer?.tool ? [answer.tool] : latestAssistantMessage?.toolCalls ?? [];
  const deckEvidencePlan = evidencePlan ?? latestAssistantMessage?.evidencePlan;
  const deckEvidencePackage = evidencePackage ?? latestAssistantMessage?.evidencePackage;
  const deckQualityScore = qualityScore ?? latestAssistantMessage?.qualityScore;
  const deckBusinessSynthesis = answer?.businessSynthesisPlan ?? latestAssistantMessage?.businessSynthesisPlan;
  const deckMethodDistillation = answer?.methodDistillation
    ?? latestAssistantMessage?.methodDistillation
    ?? deckBusinessSynthesis?.methodDistillation;

  function refreshSessionOptions(activeSessionId: string): void {
    void fetchAgentConversationSessions(20)
      .then(response => {
        setSessionOptions(mergeSessionOptions(activeSessionId, response.items));
        setSessionListError(null);
      })
      .catch(error => {
        setSessionOptions(current => mergeSessionOptions(activeSessionId, current));
        setSessionListError(error instanceof Error ? error.message : String(error));
      });
  }

  useEffect(() => {
    let active = true;
    setHistoryLoading(true);
    setHistoryError(null);
    void fetchAgentConversationHistory(sessionId)
      .then(history => {
        if (!active) return;
        const restoredMessages = conversationHistoryToMessages(history);
        const restoredCountry = latestResolvedCountryFromMessages(restoredMessages);
        if (restoredCountry) {
          setCountry(restoredCountry);
        }
        setMessages(current => current.some(message => message.isStreaming) ? current : restoredMessages);
      })
      .catch(error => {
        if (!active) return;
        setHistoryError(error instanceof Error ? error.message : String(error));
        setMessages(current => current.some(message => message.isStreaming) ? current : []);
      })
      .finally(() => {
        if (active) {
          setHistoryLoading(false);
        }
      });
    refreshSessionOptions(sessionId);
    return () => {
      active = false;
    };
  }, [sessionId]);

  useEffect(() => {
    const thread = chatThreadRef.current;
    if (!thread) return;
    if (messages.some(message => message.isStreaming)) {
      thread.scrollTop = thread.scrollHeight;
      return;
    }
    const assistantMessages = thread.querySelectorAll<HTMLElement>(".astrbot-chat-message.is-assistant");
    const latestAssistantMessage = assistantMessages.item(assistantMessages.length - 1);
    if (latestAssistantMessage) {
      thread.scrollTop = Math.max(0, latestAssistantMessage.offsetTop - thread.offsetTop - 12);
      return;
    }
    thread.scrollTop = 0;
  }, [messages]);

  useEffect(() => {
    if (!runLoading) return undefined;
    const timer = window.setInterval(() => {
      setMessages(current => current.map(message => {
        if (message.role !== "assistant" || !message.isStreaming || message.text.trim()) {
          return message;
        }
        const startedAt = message.streamStartedAt ?? Date.now();
        const elapsedSeconds = Math.max(1, Math.round((Date.now() - startedAt) / 1000));
        if (elapsedSeconds < 8) {
          return message;
        }
        const market = message.activeCountry?.trim() || country.trim() || "selected market";
        return {
          ...message,
          streamStartedAt: startedAt,
          streamStatus: `Still checking ${market} evidence · ${elapsedSeconds}s · waiting for tools or first answer chunk…`,
        };
      }));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [country, runLoading]);

  function startNewConversation(): void {
    if (runLoading) return;
    const nextSessionId = createAgentSessionId();
    localStorage.setItem(AGENT_SESSION_STORAGE_KEY, nextSessionId);
    setSessionId(nextSessionId);
    setMessages([]);
    setRunResult(null);
    setRunError(null);
    setHistoryError(null);
    setSessionListError(null);
    setSessionOptions(current => mergeSessionOptions(nextSessionId, current));
  }

  function selectConversationSession(nextSessionId: string): void {
    if (runLoading || nextSessionId === sessionId) return;
    localStorage.setItem(AGENT_SESSION_STORAGE_KEY, nextSessionId);
    setSessionId(nextSessionId);
    setRunResult(null);
    setRunError(null);
    setHistoryError(null);
  }

  function buildAgentHeaders(): Record<string, string> {
    const token = (localStorage.getItem("jato_auth_token") || import.meta.env.VITE_AUTH_TOKEN || "").trim();
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (token) headers["X-Auth-Token"] = token;
    headers["X-User-Name"] = localStorage.getItem("jato_user_name") || "anonymous";
    return headers;
  }

  function logFollowUpClick(followUp: AgentFollowUp, sourceQuestion: string, eventCountry: string): void {
    void fetch(buildAstrBotAgentEndpoint("/followups/click"), {
      method: "POST",
      headers: buildAgentHeaders(),
      body: JSON.stringify({
        session_id: sessionId,
        country: eventCountry,
        source_question: sourceQuestion,
        follow_up: followUp,
      }),
    }).catch(() => undefined);
  }

  async function sendQuestion(nextQuestion?: string, sourceFollowUp?: AgentFollowUp, sourceQuestion = ""): Promise<void> {
    const questionText = (nextQuestion ?? question).trim();
    const visibleCountry = countryInputRef.current?.value.trim() || country.trim();
    const requestCountry = resolveAstrBotRequestCountry(questionText, visibleCountry, country);
    if (!requestCountry || !questionText || runLoading) return;
    if (nextQuestion) {
      setQuestion(questionText);
    }
    if (requestCountry !== country.trim()) {
      setCountry(requestCountry);
    }
    if (sourceFollowUp) {
      logFollowUpClick(sourceFollowUp, sourceQuestion, requestCountry);
    }
    const assistantId = makeMessageId("assistant");

    setMessages(current => [
      ...current,
      { id: makeMessageId("user"), role: "user", text: questionText },
      {
        id: assistantId,
        role: "assistant",
        text: "",
        thinking: [],
        isStreaming: true,
        streamStartedAt: Date.now(),
        activeCountry: requestCountry,
        streamStatus: `Starting ${requestCountry} analysis…`,
      },
    ]);
    setRunLoading(true);
    setRunError(null);

    const thinkingLog: ThinkingStep[] = [];
    const pushThink = (step: ThinkingStep, nextCountry = "") => {
      thinkingLog.push(step);
      const streamStatus = streamStatusForThinkingStep(step);
      const resolvedCountry = nextCountry.trim();
      setMessages(current => current.map(m => (
        m.id === assistantId ? {
          ...m,
          thinking: [...thinkingLog],
          streamStatus,
          activeCountry: resolvedCountry || m.activeCountry || requestCountry,
        } : m
      )));
    };

    try {
      const headers = buildAgentHeaders();

      const response = await fetch(buildAstrBotAgentEndpoint("/stream"), {
        method: "POST",
        headers,
        body: JSON.stringify({
          country: requestCountry,
          question: questionText,
          max_rounds: 3,
          session_id: sessionId,
          skill_id: skillId,
          mode,
          research_mode: researchMode,
          source_followup: sourceFollowUp ?? {},
        }),
      });

      if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);

      const reader = response.body?.getReader();
      if (!reader) throw new Error("No response body");

      const decoder = new TextDecoder();
      let buffer = "";
      let streamedText = "";
      let streamedChunkCount = 0;
      let answerStreamPass = 0;
      let doneData: Record<string, unknown> | null = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const parsedFrames = parseAstrBotSseFrames(buffer);
        buffer = parsedFrames.rest;

        for (const data of parsedFrames.events) {
            switch (readText(data._event, "")) {
              case "thinking":
                pushThink(
                  { type: "thinking", message: readText(data.message, ""), round: readNumber(data.round) },
                  readText(data.country, ""),
                );
                break;
              case "tool_call":
                pushThink({ type: "tool_call", tool: readText(data.tool, ""), reason: readText(data.reason, ""), round: readNumber(data.round) });
                break;
              case "tool_result":
                pushThink({ type: "tool_result", tool: readText(data.tool, ""), summary: asRecord(data.summary) ?? {}, round: readNumber(data.round) });
                break;
              case "tool_error":
                pushThink({ type: "tool_error", tool: readText(data.tool, ""), error: readText(data.error, "") });
                break;
              case "answer_start":
                answerStreamPass += 1;
                streamedChunkCount = 0;
                streamedText = "";
                setMessages(current => current.map(m => (
                  m.id === assistantId ? {
                    ...m,
                    text: answerStreamPass === 1 ? "" : m.text,
                    streamStatus: answerStreamPass === 1
                      ? formatAstrBotAnswerStreamStatus(requestCountry, readNumber(data.chunkCount, 0))
                      : formatAstrBotAnswerRefinementStatus(requestCountry, readNumber(data.chunkCount, 0)),
                  } : m
                )));
                break;
              case "token":
                streamedChunkCount += 1;
                streamedText += readText(data.text, "");
                setMessages(current => current.map(m => (
                  m.id === assistantId ? {
                    ...m,
                    text: streamedText,
                    streamStatus: formatAstrBotTokenStreamStatus(requestCountry, streamedChunkCount),
                  } : m
                )));
                break;
              case "error":
                throw new Error(readText(data.message, "AstrBot stream failed"));
              case "done":
                doneData = data as Record<string, unknown>;
                break;
            }
        }
      }

      // Finalize message
      const charts = (Array.isArray(doneData?.charts) ? doneData.charts : []) as ChatChart[];
      const developerTrace = readDeveloperTrace(doneData?.developerTrace);
      const doneEvidencePlan = asRecord(doneData?.evidencePlan) ?? undefined;
      const doneEvidencePackage = readEvidencePackage(doneData?.evidencePackage);
      const doneQualityScore = readQualityScore(doneData?.qualityScore);
      const doneBusinessSynthesisPlan = readBusinessSynthesisPlan(doneData?.businessSynthesisPlan);
      const doneMethodDistillation = readBusinessMethodDistillation(doneData?.methodDistillation ?? doneBusinessSynthesisPlan?.methodDistillation);
      const doneRecommendedActions = readRecommendedActions(doneData?.recommendedActions);
      const doneReportReadyBullets = readStringList(doneData?.reportReadyBullets);
      const followUps = normalizeAgentFollowUps(doneData?.structuredFollowUps ?? doneData?.followUps);
      const citations = readAgentCitations(doneData?.citations);
      const doneVisualArtifacts = readVisualArtifacts(doneData?.visualArtifacts);
      const doneKeyTakeaways = readStringList(doneData?.keyTakeaways);
      const donePmInsight = readText(doneData?.pmInsight, "");
      const doneEvidenceBackedLead = readText(doneData?.evidenceBackedLead, "");
      if (doneData?.sessionId) {
        setSessionId(doneData.sessionId as string);
        localStorage.setItem(AGENT_SESSION_STORAGE_KEY, doneData.sessionId as string);
      }
      const effectiveCountry = readText(doneData?.country, "");
      if (effectiveCountry && effectiveCountry !== country.trim()) {
        setCountry(effectiveCountry);
      }
      refreshSessionOptions(doneData?.sessionId as string || sessionId);
      setMessages(current => current.map(m => {
        if (m.id !== assistantId) return m;
        return {
          ...m,
          isStreaming: false,
          streamStatus: undefined,
          activeCountry: effectiveCountry || requestCountry,
          text: doneData?.direct as string || streamedText || "Analysis complete.",
          answerTitle: doneData?.title as string,
          answerSummary: readText(doneData?.summary, ""),
          answerEvidenceLead: doneEvidenceBackedLead || undefined,
          answerBullets: doneData?.bullets as string[],
          keyTakeaways: doneKeyTakeaways.length > 0 ? doneKeyTakeaways : undefined,
          pmInsight: donePmInsight || undefined,
          answerLimitations: doneData?.limitations as string[],
          answerFollowUps: followUps,
          answerCitations: citations,
          visualArtifacts: doneVisualArtifacts.length > 0 ? doneVisualArtifacts : undefined,
          toolCalls: doneData?.toolCalls as string[],
          charts: charts.length > 0 ? charts : undefined,
          developerTrace,
          evidencePlan: doneEvidencePlan,
          evidencePackage: doneEvidencePackage,
          qualityScore: doneQualityScore,
          businessSynthesisPlan: doneBusinessSynthesisPlan,
          methodDistillation: doneMethodDistillation,
          recommendedActions: doneRecommendedActions.length > 0 ? doneRecommendedActions : undefined,
          reportReadyBullets: doneReportReadyBullets.length > 0 ? doneReportReadyBullets : undefined,
          sessionId: doneData?.sessionId as string,
        };
      }));

      // Also set runResult for backward compat (chart rendering etc.)
      if (doneData) {
        setRunResult({
          tool: "agent_stream",
          metadata: { toolCalls: doneData.toolCalls, toolCount: doneData.toolCount, developerTrace, qualityScore: doneQualityScore },
          data: {
            answer: {
              title: doneData.title,
              direct: doneData.direct,
              evidenceBackedLead: doneEvidenceBackedLead,
              bullets: doneData.bullets,
              limitations: doneData.limitations,
              citations,
              followUps,
              visualArtifacts: doneVisualArtifacts,
              summary: doneData.summary,
              keyTakeaways: doneKeyTakeaways,
              pmInsight: donePmInsight,
              confidence: doneData.confidence,
              status: doneData.status ?? doneData.answerStatus,
              answerStatus: doneData.answerStatus ?? doneData.status,
              grounding: doneData.grounding,
              businessSynthesisPlan: doneBusinessSynthesisPlan,
              methodDistillation: doneMethodDistillation,
              recommendedActions: doneRecommendedActions,
              reportReadyBullets: doneReportReadyBullets,
              businessImplications: readStringList(doneData.businessImplications),
              sourceCount: doneData.sourceCount,
              tool: doneData.tool,
            },
            developerTrace: developerTrace ?? {},
            evidencePlan: doneEvidencePlan ?? {},
            evidencePackage: doneEvidencePackage ?? {},
            qualityScore: doneQualityScore ?? {},
            methodDistillation: doneMethodDistillation ?? {},
          },
        } as AstrBotToolCallResponse);
      }
    } catch (error) {
      setRunError(error instanceof Error ? error.message : String(error));
      setMessages(current => current.map(m => m.id === assistantId ? { ...m, isStreaming: false, text: `Error: ${error instanceof Error ? error.message : String(error)}` } : m));
    } finally {
      setRunLoading(false);
    }
  }

  return (
    <div className={`astrbot-agent-surface ${developerMode ? "is-developer" : "is-user"}`}>
      {!developerMode ? (
        <UserSessionList
          sessions={sessionOptions}
          activeSessionId={sessionId}
          onSelect={selectConversationSession}
          onNew={startNewConversation}
          disabled={runLoading}
        />
      ) : null}
      {developerMode ? <ProfileSummary profile={profile} /> : null}

      {developerMode ? (
        <section className="astrbot-session-bar" aria-label="Conversation session">
          <label>
            <span>Session</span>
            <select value={sessionId} onChange={event => selectConversationSession(event.target.value)} disabled={runLoading}>
              {sessionOptions.map(session => (
                <option key={session.sessionId} value={session.sessionId}>
                  {formatSessionOption(session)}
                </option>
              ))}
            </select>
          </label>
          <div>
            <span>History</span>
            <strong>{historyStatus}</strong>
          </div>
          <div>
            <span>Recent</span>
            <strong title={sessionListError ?? sessionListStatus}>{sessionListStatus}</strong>
          </div>
          <button type="button" className="astrbot-chip-button" onClick={startNewConversation} disabled={runLoading}>
            New conversation
          </button>
        </section>
      ) : (
        <section className="astrbot-user-context-bar" aria-label="AstrBot context">
          <div>
            <span>JATO AstrBot</span>
            <strong>{displayCountry} · {selectedSkill?.name ?? "Auto Route"}</strong>
          </div>
          <div>
            <span>Research</span>
            <strong>{researchMode}</strong>
          </div>
          <button type="button" className="astrbot-chip-button" onClick={startNewConversation} disabled={runLoading}>
            New chat
          </button>
        </section>
      )}

      {historyError ? <div className="astrbot-status-error" role="status">History unavailable: {historyError}</div> : null}

      {developerMode ? (
            <AgentQualityDeck
              answer={answer}
              evidencePlan={deckEvidencePlan}
              evidencePackage={deckEvidencePackage}
              qualityScore={deckQualityScore}
              businessSynthesis={deckBusinessSynthesis}
              methodDistillation={deckMethodDistillation}
              toolCalls={deckToolCalls}
              citations={deckCitations}
            />
      ) : null}

      <section ref={chatThreadRef} className="astrbot-chat-thread" aria-label="Agent conversation">
        {messages.length === 0 ? (
          <div className="astrbot-chat-empty">
            <span>{selectedSkill?.name ?? "Auto Route"}</span>
            <strong>{developerMode ? selectedSkill?.description ?? "Ready" : "What should we analyze?"}</strong>
            {!developerMode ? (
              <>
                <p>Ask about market structure, pricing, competitors, policy, configuration or report output.</p>
                <QuickActionCards
                  country={country}
                  onSelect={(action, nextQuestion) => {
                    setMode(action.mode);
                    void sendQuestion(nextQuestion);
                  }}
                  disabled={runLoading}
                />
              </>
            ) : null}
          </div>
        ) : (
          messages.map(message => (
            <article className={`astrbot-chat-message is-${message.role}${message.isStreaming ? " is-streaming" : ""}`} key={message.id}>
              <span className="astrbot-chat-role-label">{message.role === "user" ? "You" : developerMode ? "Hermes Agent" : "AstrBot"}</span>

              {/* Thinking chain */}
              {developerMode && message.thinking && message.thinking.length > 0 ? (
                <details className="astrbot-thinking-chain" open={message.isStreaming}>
                  <summary>
                    {message.isStreaming ? "Thinking…" : `Used ${message.thinking.filter(t => t.type === "tool_call").length} tools`}
                    {message.toolCalls ? ` (${message.toolCalls.join(", ")})` : ""}
                  </summary>
                  <div className="astrbot-thinking-steps">
                    {message.thinking.map((step, i) => (
                      <div key={i} className={`astrbot-thinking-step is-${step.type}`}>
                        {step.type === "thinking" ? (
                          <span>🧠 {step.message}</span>
                        ) : step.type === "tool_call" ? (
                          <span>🔧 Calling <code>{step.tool}</code> — {step.reason}</span>
                        ) : step.type === "tool_result" ? (
                          <span>✅ <code>{step.tool}</code> returned data{step.summary ? ` (${Object.keys(step.summary).length} fields)` : ""}</span>
                        ) : step.type === "tool_error" ? (
                          <span>❌ <code>{step.tool}</code> failed: {step.error}</span>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </details>
              ) : null}
              {/* Streaming/answer text */}
              {message.role === "assistant" ? (
                <div className="astrbot-chat-answer">
                  {developerMode && message.answerTitle ? <h4>{message.answerTitle}</h4> : null}
                  {developerMode ? (
                    <ExecutiveAnswerBlock
                      text={message.text}
                      isStreaming={message.isStreaming}
                      streamPlaceholder={formatAstrBotStreamingPlaceholder(
                        message.activeCountry ?? country,
                        message.streamStatus,
                      )}
                      takeaways={selectAstrBotUserTakeaways(message)}
                      pmInsight={message.pmInsight ?? ""}
                      actions={message.recommendedActions ?? []}
                    />
                  ) : (
                    <AstrBotMarkdownAnswer
                      text={message.text}
                      isStreaming={message.isStreaming}
                      streamPlaceholder={formatAstrBotStreamingPlaceholder(
                        message.activeCountry ?? country,
                        message.streamStatus,
                      )}
                    />
                  )}
                  {message.answerEvidenceLead && (developerMode || !message.visualArtifacts?.length) ? (
                    <EvidenceBackedLead text={message.answerEvidenceLead} />
                  ) : null}
                  {message.visualArtifacts?.length ? (
                    <VisualArtifactsDeck artifacts={message.visualArtifacts} deckId={message.id} compact={!developerMode} />
                  ) : null}
                  {(() => {
                    if (message.visualArtifacts?.length) {
                      return null;
                    }
                    const fallback = buildChartFallbackCard(`${question} ${message.text}`, message.charts, message.evidencePackage);
                    return fallback ? <ChartFallbackCard card={fallback} /> : null;
                  })()}
                  {developerMode && message.isStreaming ? (
                    <StreamingArtifactPreview
                      isStreaming={message.isStreaming}
                      statusText={message.streamStatus}
                      toolCalls={
                        message.toolCalls
                        ?? message.thinking
                          ?.flatMap(step => step.type === "tool_call" && step.tool ? [step.tool] : [])
                      }
                    />
                  ) : null}
                  {developerMode && message.answerBullets?.length ? (
                    <ul className="astrbot-debug-answer-bullets">{message.answerBullets.map((b, i) => <li key={i}>{b}</li>)}</ul>
                  ) : null}
                  {message.answerCitations?.length ? (
                    <CitationStrip citations={message.answerCitations} />
                  ) : null}
                  <EvidenceGapPanel
                    evidencePackage={message.evidencePackage}
                    actions={message.recommendedActions ?? []}
                    defaultOpen={developerMode}
                  />
                  {developerMode && message.pmInsight ? (
                    <section className="astrbot-pm-insight" aria-label="Product manager insight">
                      <span>Product manager insight</span>
                      <p>{message.pmInsight}</p>
                    </section>
                  ) : null}
                  {developerMode ? (
                    <BusinessComposerPanel
                      actions={message.recommendedActions ?? []}
                      reportBullets={message.reportReadyBullets ?? []}
                      synthesis={message.businessSynthesisPlan}
                    />
                  ) : null}
                  {message.answerLimitations?.length ? (
                    <AnswerLimitationsPanel limitations={message.answerLimitations} defaultOpen={developerMode} />
                  ) : null}
                  {message.answerFollowUps?.length ? (
                    <FollowUpChips
                      items={message.answerFollowUps}
                      onSelect={item => void sendQuestion(item.question, item, message.text)}
                      disabled={runLoading}
                    />
                  ) : null}
                  <AnalysisPathPanel
                    evidencePlan={message.evidencePlan}
                    evidencePackage={message.evidencePackage}
                    qualityScore={message.qualityScore}
                    businessSynthesis={message.businessSynthesisPlan}
                    methodDistillation={message.methodDistillation ?? message.businessSynthesisPlan?.methodDistillation}
                    toolCalls={message.toolCalls ?? []}
                  />
                  {!message.isStreaming ? (
                    <AnswerCopyActions
                      title={message.answerTitle ?? "AstrBot answer"}
                      summary={message.answerSummary || message.text}
                      takeaways={selectAstrBotUserTakeaways(message)}
                      pmInsight={message.pmInsight ?? ""}
                      visualArtifacts={message.visualArtifacts ?? []}
                      nextAction={message.answerFollowUps?.[0]?.question ?? ""}
                    />
                  ) : null}
                  {developerMode && message.developerTrace ? (
                    <MessageDeveloperTrace trace={message.developerTrace} />
                  ) : null}
                  {!message.visualArtifacts?.length && message.charts?.length ? (
                    <div className="astrbot-chat-charts">
                      {message.charts.map((chart, i) => (
                        <div key={`${chart.chartId}-${i}`} className="astrbot-chart-artifact-card">
                          <div className="astrbot-chart-artifact-header">
                            <code>{chart.chartType}</code>
                            <span>{chart.title}</span>
                          </div>
                          <LazyPlotlyChart
                            data={chart.data as PlotlyChartProps["data"]}
                            layout={chart.layout as PlotlyChartProps["layout"]}
                            height={Math.min(380, (chart.layout.height as number) ?? 350)}
                          />
                        </div>
                      ))}
                    </div>
                  ) : null}
                </div>
              ) : (
                <p>{message.text}</p>
              )}
            </article>
          ))
        )}
      </section>

      <section className="astrbot-agent-input" aria-label="Agent request">
        {!developerMode ? (
          <div className="astrbot-agent-command-strip" aria-label="Quick analysis modes">
            {[
              { label: "Research sources", mode: "research" as AgentMode },
              { label: "Market trend", mode: "snapshot" as AgentMode },
              { label: "Competitor compare", mode: "variant" as AgentMode },
              { label: "Pricing corridor", mode: "pricing" as AgentMode },
            ].map(item => (
              <button
                key={item.mode}
                type="button"
                className={`astrbot-agent-command-button ${mode === item.mode ? "is-active" : ""}`}
                onClick={() => setMode(item.mode)}
              >
                {item.label}
              </button>
            ))}
          </div>
        ) : null}
        <div className="astrbot-agent-fields">
          <label>
            <span>Skill</span>
            <select
              value={skillId}
              onChange={event => {
                const nextSkill = skills.find(skill => skill.id === event.target.value) ?? null;
                setSkillId(event.target.value);
                setMode(modeFromSkill(nextSkill));
                if (nextSkill?.defaultCountry) {
                  setCountry(nextSkill.defaultCountry);
                }
                if (nextSkill?.defaultQuestion) {
                  setQuestion(nextSkill.defaultQuestion);
                }
              }}
            >
              {skills.map(skill => (
                <option key={skill.id} value={skill.id}>{skill.name}</option>
              ))}
            </select>
          </label>
          <label>
            <span>Country</span>
            <input ref={countryInputRef} value={country} onChange={event => setCountry(event.target.value)} />
          </label>
          {developerMode ? (
            <label>
              <span>Mode</span>
              <select value={mode} onChange={event => setMode(event.target.value as AgentMode)}>
                {agentModeOptions.map(option => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
          ) : null}
          <label>
            <span>Research</span>
            <select value={researchMode} onChange={event => setResearchMode(event.target.value as ResearchDepth)}>
              <option value="quick">Quick</option>
              <option value="standard">Standard</option>
              <option value="deep">Deep</option>
            </select>
          </label>
          <label>
            <span>Question</span>
            <textarea
              value={question}
              onChange={event => setQuestion(event.target.value)}
              rows={3}
              placeholder={developerMode ? undefined : "Ask JATO AstrBot about market, pricing, competitors, policy, configuration or reports..."}
            />
          </label>
        </div>
        {selectedSkill ? (
          <div className="astrbot-skill-summary">
            <span>{selectedSkill.domain}</span>
            <strong>{selectedSkill.description}</strong>
          </div>
        ) : null}
        <div className="astrbot-agent-actions">
          {developerMode ? skills.filter(skill => skill.id !== "auto_route").slice(0, 5).map(skill => (
            <button
              key={skill.id}
              type="button"
              className="astrbot-chip-button"
              onClick={() => {
                setSkillId(skill.id);
                setMode(modeFromSkill(skill));
                setCountry(skill.defaultCountry);
                setQuestion(skill.defaultQuestion);
              }}
            >
              {skill.name}
            </button>
          )) : null}
          <button type="button" className="astrbot-primary-action" onClick={() => void sendQuestion()} disabled={!canRun}>
            {runLoading ? "Running" : "Send"}
          </button>
        </div>
      </section>

      {runError ? <div className="astrbot-status-error" role="status">{runError}</div> : null}

      {developerMode ? (
        runResult ? (
          <section className="astrbot-agent-result" aria-label="Agent route result">
            <AgentAnswerPanel
              answer={answer}
              fallbackSummary={displaySummary}
              onFollowUp={item => void sendQuestion(item.question, item, answer?.direct ?? displaySummary)}
              followUpDisabled={runLoading}
              showBusinessComposer={false}
            />
            {!latestAssistantMessage ? (
            <AnalysisPathPanel
              evidencePlan={evidencePlan}
              evidencePackage={evidencePackage}
              qualityScore={qualityScore}
              businessSynthesis={answer?.businessSynthesisPlan}
              methodDistillation={answer?.methodDistillation ?? answer?.businessSynthesisPlan?.methodDistillation}
              toolCalls={answer?.tool ? [answer.tool] : []}
            />
          ) : null}

          {/* ── Phase 3: Chart artifacts ── */}
          {hasCharts ? (
            <div className="astrbot-chart-artifacts" aria-label="Chart artifacts">
              <div className="astrbot-chart-artifacts-heading">
                <span className="page-kicker">Charts</span>
                <h4>{chartSpecs.length} chart artifact{chartSpecs.length === 1 ? "" : "s"}</h4>
              </div>
              {chartSpecs.map((spec, idx) => {
                const data = Array.isArray(spec.data) ? spec.data : [];
                const layout = (spec.layout ?? {}) as Record<string, unknown>;
                const chartType = readText(spec.chartType, "line");
                const title = readText(spec.title, `Chart ${idx + 1}`);
                if (data.length === 0) return null;
                return (
                  <div key={`${readText(spec.chartId)}-${idx}`} className="astrbot-chart-artifact-card">
                    <div className="astrbot-chart-artifact-header">
                      <code>{chartType}</code>
                      <span>{title}</span>
                    </div>
                    <LazyPlotlyChart
                      data={data as PlotlyChartProps["data"]}
                      layout={layout as PlotlyChartProps["layout"]}
                      height={Math.min(420, (layout.height as number) ?? 400)}
                    />
                  </div>
                );
              })}
            </div>
          ) : null}

          {/* ── Phase 6: Retrieval Router trace ── */}
          {developerMode && retrievalClassification ? (
            <div className="astrbot-retrieval-trace" aria-label="Retrieval route trace">
              <div className="astrbot-retrieval-trace-heading">
                <span>Retrieval Router</span>
                <strong>{readText(retrievalPrimaryLabel, retrievalPrimaryPath)}</strong>
                <small>{readText(retrievalPrimaryConfidence, "medium")} confidence · {routeSource === "retrieval_router" ? "auto-routed" : "skill override"}</small>
              </div>
              <div className="astrbot-retrieval-paths">
                {retrievalAllPaths.map((path, i) => {
                  const isPrimary = i === 0;
                  const contributed = evidencePathsContributed.includes(path);
                  const planned = !contributed && (retrievalSecondaryPaths.includes(path) || (!isPrimary && path !== retrievalClassification?.primaryPath));
                  const prefix = contributed ? "✓ " : isPrimary ? "▼ " : "▸ ";
                  return (
                    <span key={path} className={`astrbot-retrieval-path-chip ${isPrimary ? "is-primary" : ""} ${contributed ? "is-executed" : ""} ${planned && !isPrimary ? "is-planned" : ""}`}>
                      {prefix}{path}
                    </span>
                  );
                })}
              </div>
              {retrievalSteps.length > 0 ? (
                <div className="astrbot-retrieval-steps">
                  {retrievalSteps.map(step => {
                    const executed = step.executed === true;
                    const planned = readText(step.status) === "planned";
                    return (
                      <div key={`${readText(step.step)}-${readText(step.tool)}`} className={`astrbot-retrieval-step ${executed ? "is-executed" : ""} ${planned ? "is-planned" : ""}`}>
                        <span className="astrbot-retrieval-step-num">{executed ? "✓" : planned ? "○" : "·"}</span>
                        <code>{readText(step.tool)}</code>
                        <span>{readText(step.pathLabel)}</span>
                        <small>{readText(step.confidence)}</small>
                      </div>
                    );
                  })}
                </div>
              ) : null}
            </div>
          ) : null}

          {developerMode ? (
            <>
              <div className="astrbot-agent-readable">
                <div className="astrbot-agent-readable-heading">
                  <span className="page-kicker">Developer Result</span>
                  <h3>{readText(display?.title, "Agent route ready")}</h3>
                  <p>{displaySummary}</p>
                </div>
                <div className="astrbot-agent-card-grid">
                  {displayCards.map(card => (
                    <div className="astrbot-agent-card" key={`${readText(card.label)}-${readText(card.value)}`}>
                      <span>{readText(card.label)}</span>
                      <strong>{readText(card.value)}</strong>
                    </div>
                  ))}
                </div>
              </div>
              <div className="astrbot-agent-route-grid">
                <div>
                  <span>Selected Tool</span>
                  <strong>{selectedTool}</strong>
                </div>
                <div>
                  <span>Retrieval Path</span>
                  <strong>{readText(retrievalPrimaryPath, String(route?.mode ?? ""))}</strong>
                </div>
                <div>
                  <span>Evidence</span>
                  <strong>{evidenceItems.length} item{evidenceItems.length === 1 ? "" : "s"} · {evidenceSources.length} source{evidenceSources.length === 1 ? "" : "s"}</strong>
                </div>
                <div>
                  <span>Total Paths</span>
                  <strong>{retrievalAllPaths.length > 0 ? `${retrievalAllPaths.length} path${retrievalAllPaths.length === 1 ? "" : "s"}` : "1 path"}</strong>
                </div>
              </div>
              <ModelUsagePanel usage={modelUsage} />
              <details className="astrbot-debug-details">
                <summary>Debug payload</summary>
                <pre className="astrbot-tool-result">{JSON.stringify(buildProbePreview(runResult), null, 2)}</pre>
              </details>
            </>
          ) : null}
          </section>
        ) : (
          <section className="astrbot-agent-empty" aria-label="Agent route state">
          <div>
            <span>Route</span>
            <strong>Waiting</strong>
          </div>
          <div>
            <span>Evidence Pack</span>
            <strong>Not built</strong>
          </div>
          <div>
            <span>Native Runtime</span>
            <strong>{developerMode ? "Fallback only" : "Developer only"}</strong>
          </div>
        </section>
        )
      ) : null}
    </div>
  );
}

function FollowUpChips({
  items,
  onSelect,
  disabled,
}: {
  items: AgentFollowUp[];
  onSelect: (item: AgentFollowUp) => void;
  disabled: boolean;
}) {
  if (items.length === 0) {
    return null;
  }
  return (
    <div className="astrbot-followups" aria-label="Follow-up suggestions">
      {items.slice(0, 4).map(item => (
        <button
          key={item.id}
          type="button"
          className={`astrbot-followup-chip is-${item.intent}`}
          onClick={() => onSelect(item)}
          title={buildFollowUpTitle(item)}
          disabled={disabled}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}

function buildFollowUpTitle(item: AgentFollowUp): string {
  const lines = [
    item.question,
    item.reason ? `Reason: ${item.reason}` : "",
    item.expectedTools.length > 0 ? `Tools: ${item.expectedTools.join(", ")}` : "",
    item.expectedOutput ? `Output: ${item.expectedOutput}` : "",
    item.risk ? `Risk: ${item.risk}` : "",
  ];
  return lines.filter(Boolean).join("\n");
}

function MessageDeveloperTrace({ trace }: { trace: Record<string, unknown> }) {
  const tools = (Array.isArray(trace.tools) ? trace.tools : [])
    .map(item => asRecord(item))
    .filter((item): item is Record<string, unknown> => Boolean(item));
  const followUps = normalizeAgentFollowUps(trace.followUps);
  const toolCount = readNumber(trace.toolCount, tools.length);
  const dataBoundary = readText(trace.dataBoundary, "JATO MCP summaries");
  const parser = readText(trace.answerParser, "answer parser");
  return (
    <details className="astrbot-message-dev-trace">
      <summary>Developer trace</summary>
      <div className="astrbot-message-dev-grid">
        <div>
          <span>Tools</span>
          <strong>{toolCount}</strong>
        </div>
        <div>
          <span>Boundary</span>
          <strong>{dataBoundary}</strong>
        </div>
        <div>
          <span>Parser</span>
          <strong>{parser}</strong>
        </div>
        <div>
          <span>Follow-ups</span>
          <strong>{followUps.length}</strong>
        </div>
      </div>
      {tools.length > 0 ? (
        <div className="astrbot-message-dev-tools" aria-label="Developer tool trace">
          {tools.map((tool, index) => {
            const summary = asRecord(tool.summary);
            return (
              <section key={`${readText(tool.tool, "tool")}-${index}`} className="astrbot-message-dev-tool">
                <div>
                  <code>{readText(tool.tool, "tool")}</code>
                  <span>round {readNumber(tool.round, index + 1)} · {readText(tool.status, "ok")}</span>
                </div>
                <p>{readText(tool.reason, "agent-selected")}</p>
                {summary ? (
                  <pre>{JSON.stringify(summarizeJson(summary), null, 2)}</pre>
                ) : null}
                {tool.error ? <p className="astrbot-message-dev-error">{readText(tool.error)}</p> : null}
              </section>
            );
          })}
        </div>
      ) : null}
      {followUps.length > 0 ? (
        <div className="astrbot-message-dev-followups">
          {followUps.map(item => <span key={item.id}>{item.label}</span>)}
        </div>
      ) : null}
      <details className="astrbot-message-dev-json">
        <summary>Raw trace preview</summary>
        <pre>{JSON.stringify(summarizeJson(trace), null, 2)}</pre>
      </details>
    </details>
  );
}

function citationTone(citation: AgentAnswerCitation): string {
  if (citation.sourceTier === "high" || (citation.sourceScore ?? 0) >= 72) return "is-high";
  if (citation.sourceTier === "medium" || (citation.sourceScore ?? 0) >= 45) return "is-medium";
  return "is-low";
}

function citationScoreLabel(citation: AgentAnswerCitation): string {
  return typeof citation.sourceScore === "number" ? `${Math.round(citation.sourceScore)}/100` : citation.sourceTier ?? "source";
}

function averageCitationScore(citations: AgentAnswerCitation[]): string {
  const scores = citations
    .map(item => item.sourceScore)
    .filter((score): score is number => typeof score === "number");
  if (scores.length === 0) return "n/a";
  return `${Math.round(scores.reduce((sum, score) => sum + score, 0) / scores.length)}/100`;
}

export function selectUniqueAstrBotCitations(citations: AgentAnswerCitation[], limit = 8): AgentAnswerCitation[] {
  const seen = new Set<string>();
  const result: AgentAnswerCitation[] = [];
  for (const citation of citations) {
    const key = buildAstrBotCitationIdentity(citation);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    result.push(citation);
    if (result.length >= limit) {
      break;
    }
  }
  return result;
}

function buildAstrBotCitationIdentity(citation: AgentAnswerCitation): string {
  const url = normalizeAstrBotCitationText(citation.url ?? "").replace(/\/+$/, "");
  if (url) {
    return `url:${url}`;
  }
  const title = normalizeAstrBotCitationText(citation.sourceTitle ?? citation.label);
  const claim = normalizeAstrBotCitationText(citation.supportedClaim ?? "");
  if (citation.citationId) {
    return `id:${normalizeAstrBotCitationText(citation.citationId)}|${title || claim}`;
  }
  return `text:${title}|${claim}|${normalizeAstrBotCitationText(citation.source)}`;
}

function normalizeAstrBotCitationText(value: string): string {
  return value.trim().replace(/\s+/g, " ").toLowerCase();
}

function qualityPercent(score?: QualityScore): string {
  return score ? `${Math.round(score.totalScore * 100)}%` : "n/a";
}

function CitationStrip({ citations }: { citations: AgentAnswerCitation[] }) {
  const visibleCitations = selectUniqueAstrBotCitations(citations);
  if (visibleCitations.length === 0) {
    return null;
  }
  return (
    <div className="astrbot-answer-citations" aria-label="Answer citations">
      {visibleCitations.map((citation, index) => {
        const claimType = citation.claimType ? citation.claimType : "source";
        const content = (
          <>
            <span>{citation.citationId ? `[${citation.citationId}] ` : ""}{citation.sourceCategory ?? citation.source}</span>
            <strong>{citation.sourceTitle ?? citation.label}</strong>
            <em className={`astrbot-citation-score ${citationTone(citation)}`}>
              {claimType} · relevance {citationScoreLabel(citation)}
            </em>
            {citation.supportedClaim ? (
              <p>Supports: {citation.supportedClaim}</p>
            ) : null}
          </>
        );
        return citation.url ? (
          <a key={buildAstrBotCitationKey(citation, index)} href={citation.url} target="_blank" rel="noreferrer">
            {content}
          </a>
        ) : (
          <div key={buildAstrBotCitationKey(citation, index)}>
            {content}
          </div>
        );
      })}
    </div>
  );
}

export function buildAstrBotCitationKey(citation: Pick<AgentAnswerCitation, "tool" | "citationId" | "label" | "url">, index: number): string {
  const stableLabel = citation.citationId || citation.label || citation.url || "citation";
  return `${citation.tool || "source"}-${stableLabel}-${index}`;
}

function AgentQualityDeck({
  answer,
  evidencePlan,
  evidencePackage,
  qualityScore,
  businessSynthesis,
  methodDistillation,
  toolCalls,
  citations,
}: {
  answer: AgentAnswer | null;
  evidencePlan?: Record<string, unknown>;
  evidencePackage?: EvidencePackage;
  qualityScore?: QualityScore;
  businessSynthesis?: BusinessSynthesisPlan;
  methodDistillation?: BusinessMethodDistillation;
  toolCalls: string[];
  citations: AgentAnswerCitation[];
}) {
  const intent = readText(evidencePlan?.intent, evidencePackage?.intent || "waiting");
  const confidence = evidencePackage?.confidence ?? answer?.confidence ?? "n/a";
  const refs = evidencePackage?.toolResults.flatMap(item => item.evidenceRefs) ?? [];
  const missing = evidencePackage?.missingEvidence ?? [];
  const failures = qualityScore?.failures ?? [];
  const governance = evidencePackage?.researchGovernance;
  const metrics = governance?.metrics;
  const crossCheck = evidencePackage?.jatoCrossCheck;
  const improvementTags = [
    ...failures,
    ...missing.slice(0, 3).map(item => `${item.name}:${item.impact}`),
    ...(governance?.policyWarnings ?? []).slice(0, 3),
  ];
  const uniqueTools = Array.from(new Set(toolCalls.filter(Boolean)));
  const uniqueCitations = selectUniqueAstrBotCitations(citations);
  return (
    <section className="astrbot-dev-quality-deck" aria-label="Developer quality deck">
      <div className="astrbot-dev-quality-heading">
        <span>Quality Loop</span>
        <strong>{answer?.answerStatus ?? "waiting"}</strong>
        <small>{intent} · {confidence}</small>
      </div>
      <div className="astrbot-dev-quality-grid">
        <div>
          <span>Score</span>
          <strong>{qualityPercent(qualityScore)}</strong>
        </div>
        <div>
          <span>Tools</span>
          <strong>{uniqueTools.length ? uniqueTools.join(", ") : "none"}</strong>
        </div>
        <div>
          <span>Evidence</span>
          <strong>{refs.length} refs · {missing.length} missing</strong>
        </div>
        <div>
          <span>Sources</span>
          <strong>{uniqueCitations.length} cited · relevance avg {averageCitationScore(uniqueCitations)}</strong>
        </div>
        <div>
          <span>Research</span>
          <strong>{governance?.mode?.mode ?? "n/a"} · {metrics ? `${metrics.queryCount}q/${metrics.sourcesUsed}s` : "no metrics"}</strong>
        </div>
        <div>
          <span>Cost / Latency</span>
          <strong>{metrics ? `$${metrics.estimatedCost.toFixed(4)} · ${metrics.latencyMs}ms` : "n/a"}</strong>
        </div>
        <div>
          <span>Policy</span>
          <strong>{governance?.policyStatus ?? "n/a"}</strong>
        </div>
        <div>
          <span>JATO Check</span>
          <strong>{crossCheck?.status ?? "n/a"}</strong>
        </div>
        <div>
          <span>Business</span>
          <strong>{businessSynthesis?.evidenceAlignment.status ?? "n/a"} · {qualityScore ? `${Math.round(qualityScore.businessSynthesisScore * 100)}%` : "n/a"}</strong>
        </div>
        <div>
          <span>Method</span>
          <strong>{methodDistillation ? `${methodDistillation.methodType} · ${methodDistillation.dataQualityWarnings.length} warnings` : "n/a"}</strong>
        </div>
      </div>
      <div className="astrbot-dev-quality-actions">
        {improvementTags.length ? (
          improvementTags.slice(0, 6).map(tag => <span key={tag}>{tag}</span>)
        ) : (
          <span>No blocking quality gaps</span>
        )}
      </div>
    </section>
  );
}

function BusinessComposerPanel({
  actions,
  reportBullets,
  synthesis,
}: {
  actions: RecommendedAction[];
  reportBullets: string[];
  synthesis?: BusinessSynthesisPlan;
}) {
  if (actions.length === 0 && reportBullets.length === 0 && !synthesis) {
    return null;
  }
  const visibleActions = actions.slice(0, 3);
  const visibleBullets = reportBullets.slice(0, 4);
  return (
    <section className="astrbot-business-composer" aria-label="Business composer output">
      {synthesis ? (
        <div className="astrbot-business-composer-status">
          <span>Business synthesis</span>
          <strong>{synthesis.evidenceAlignment.status}</strong>
          <p>{synthesis.evidenceAlignment.summary}</p>
        </div>
      ) : null}
      {visibleActions.length > 0 ? (
        <div className="astrbot-business-action-grid">
          {visibleActions.map(action => (
            <article key={`${action.priority}-${action.action}`}>
              <span>{action.priority}</span>
              <strong>{action.action}</strong>
              {action.rationale ? <p>{action.rationale}</p> : null}
              {action.evidenceRefs.length > 0 || action.citationIds.length > 0 ? (
                <small>
                  {[...action.evidenceRefs.slice(0, 2), ...action.citationIds.slice(0, 2)].join(" · ")}
                </small>
              ) : null}
            </article>
          ))}
        </div>
      ) : null}
      {visibleBullets.length > 0 ? (
        <div className="astrbot-report-ready">
          <span>Report-ready bullets</span>
          <ul>
            {visibleBullets.map(item => <li key={item}>{item}</li>)}
          </ul>
        </div>
      ) : null}
    </section>
  );
}

function evidenceGapLabel(name: string): string {
  const cleaned = name
    .replace(/^missing_required_tool:/, "required tool: ")
    .replace(/_/g, " ")
    .trim();
  return cleaned || "missing evidence";
}

function evidenceGapAction(actions: RecommendedAction[]): string {
  const primaryAction = actions.find(action => action.action.trim().length > 0);
  if (primaryAction) {
    return primaryAction.rationale.trim()
      ? `${primaryAction.action} — ${primaryAction.rationale}`
      : primaryAction.action;
  }
  return "补齐上述证据后再给确定数字结论。";
}

export function buildToolCoverageSummary(
  requiredTools: string[],
  usedTools: string[],
  missingEvidence: MissingEvidence[] = [],
) {
  const required = Array.from(new Set(requiredTools.map(item => item.trim()).filter(Boolean)));
  const used = Array.from(new Set(usedTools.map(item => item.trim()).filter(Boolean)));
  const missingFromEvidence = missingEvidence
    .map(item => item.name.trim())
    .filter(name => name.startsWith("missing_required_tool:"))
    .map(name => name.replace(/^missing_required_tool:/, "").trim())
    .filter(Boolean);
  const missing = Array.from(new Set([
    ...required.filter(tool => !used.includes(tool)),
    ...missingFromEvidence,
  ]));
  const satisfied = required.filter(tool => !missing.includes(tool));
  const status = missing.length > 0 ? "gap" : required.length > 0 ? "covered" : "not_required";
  return {
    required,
    used,
    missing,
    satisfied,
    status,
    label: status === "gap"
      ? `${missing.length} missing`
      : status === "covered"
        ? "covered"
        : "not required",
  };
}

export function EvidenceGapPanel({
  evidencePackage,
  actions,
  defaultOpen = false,
}: {
  evidencePackage?: EvidencePackage;
  actions: RecommendedAction[];
  defaultOpen?: boolean;
}) {
  const missing = evidencePackage?.missingEvidence.filter(item => item.name.trim().length > 0) ?? [];
  if (missing.length === 0) {
    return null;
  }
  const blocking = missing.filter(item => item.impact === "blocking" || item.name.startsWith("missing_required_tool:"));
  const visible = (blocking.length > 0 ? blocking : missing).slice(0, 3);
  const confidence = evidencePackage?.confidence ?? "unknown";
  const heading = blocking.length > 0 ? "不能给确定数字结论" : "结论需要更强证据";
  const modeLabel = blocking.length > 0 ? "Evidence gap" : "Evidence check";
  return (
    <details
      className={`astrbot-evidence-gap-panel ${blocking.length > 0 ? "is-blocking" : "is-soft"}`}
      aria-label="Evidence gaps"
      open={defaultOpen}
    >
      <summary className="astrbot-evidence-gap-summary">
        <span>{modeLabel}</span>
        <strong>{heading}</strong>
        <small>{visible.length}/{missing.length} shown · {confidence} confidence</small>
      </summary>
      <div className="astrbot-evidence-gap-content">
        <div className="astrbot-evidence-gap-list">
          {visible.map(item => (
            <span key={`${item.name}-${item.impact}`} title={item.reason}>
              <strong>{evidenceGapLabel(item.name)}</strong>
              <small>{item.impact}</small>
            </span>
          ))}
        </div>
        <p className="astrbot-evidence-gap-action">{evidenceGapAction(actions)}</p>
      </div>
    </details>
  );
}

export function AnswerLimitationsPanel({
  limitations,
  defaultOpen = false,
}: {
  limitations: string[];
  defaultOpen?: boolean;
}) {
  const cleanLimitations = limitations.map(item => item.trim()).filter(Boolean);
  if (cleanLimitations.length === 0) {
    return null;
  }
  return (
    <details className="astrbot-answer-limitations" aria-label="Answer limitations" open={defaultOpen}>
      <summary className="astrbot-answer-limitations-summary">
        <span>Evidence limits</span>
        <strong>{cleanLimitations.length} check{cleanLimitations.length === 1 ? "" : "s"} need attention</strong>
      </summary>
      <div className="astrbot-answer-limitations-list">
        {cleanLimitations.map(item => <span key={item}>{item}</span>)}
      </div>
    </details>
  );
}

export function EvidenceBackedLead({ text }: { text?: string }) {
  const value = readText(text, "");
  if (!value) {
    return null;
  }
  return (
    <section className="astrbot-evidence-backed-lead" aria-label="Evidence-backed lead">
      <span>数据依据</span>
      <p>{value}</p>
    </section>
  );
}

function AgentAnswerPanel({
  answer,
  fallbackSummary,
  onFollowUp,
  followUpDisabled,
  showBusinessComposer = true,
}: {
  answer: AgentAnswer | null;
  fallbackSummary: string;
  onFollowUp: (item: AgentFollowUp) => void;
  followUpDisabled: boolean;
  showBusinessComposer?: boolean;
}) {
  const direct = formatUserFacingAnswerText(answer?.direct || fallbackSummary);
  return (
    <div className="astrbot-answer-panel">
      <div className="astrbot-answer-heading">
        <span className="page-kicker">{answer?.confidence ?? "Grounded"}</span>
        <h3>{answer?.title ?? "Grounded answer"}</h3>
        <p>{direct}</p>
      </div>
      {answer?.evidenceBackedLead ? (
        <EvidenceBackedLead text={answer.evidenceBackedLead} />
      ) : null}
      {answer?.bullets.length ? (
        <ul className="astrbot-answer-bullets">
          {answer.bullets.map(item => <li key={item}>{item}</li>)}
        </ul>
      ) : null}
      {answer?.citations.length ? (
        <CitationStrip citations={answer.citations} />
      ) : null}
      {answer && showBusinessComposer ? (
        <BusinessComposerPanel
          actions={answer.recommendedActions}
          reportBullets={answer.reportReadyBullets}
          synthesis={answer.businessSynthesisPlan}
        />
      ) : null}
      {answer?.limitations.length ? (
        <AnswerLimitationsPanel limitations={answer.limitations} />
      ) : null}
      {answer?.followUps.length ? (
        <FollowUpChips items={answer.followUps} onSelect={onFollowUp} disabled={followUpDisabled} />
      ) : null}
    </div>
  );
}

function EvidenceSourceFilter({
  toolResults,
  refs,
}: {
  toolResults: ToolEvidence[];
  refs: EvidenceRef[];
}) {
  const [query, setQuery] = useState("");
  const [disabledTypes, setDisabledTypes] = useState<string[]>([]);
  const sourceTypes = Array.from(new Set(toolResults.map(item => item.sourceType).filter(Boolean)));
  const loweredQuery = query.trim().toLowerCase();
  const visibleTools = toolResults.filter(item => {
    const typeEnabled = !disabledTypes.includes(item.sourceType);
    const text = `${item.toolName} ${item.sourceType} ${item.summary} ${item.keyFindings.join(" ")}`.toLowerCase();
    return typeEnabled && (!loweredQuery || text.includes(loweredQuery));
  });
  const visibleRefs = refs.filter(ref => {
    const text = `${ref.label} ${ref.source ?? ""} ${ref.table ?? ""} ${ref.value ?? ""} ${ref.periodLabel ?? ""}`.toLowerCase();
    return !loweredQuery || text.includes(loweredQuery);
  });

  function toggleSourceType(sourceType: string): void {
    setDisabledTypes(current => current.includes(sourceType)
      ? current.filter(item => item !== sourceType)
      : [...current, sourceType]);
  }

  if (toolResults.length === 0 && refs.length === 0) {
    return null;
  }
  return (
    <details className="astrbot-source-filter">
      <summary>
        <span>Evidence filter</span>
        <strong>{visibleTools.length} tools · {visibleRefs.length} refs</strong>
      </summary>
      <div className="astrbot-source-filter-controls">
        <label>
          <span>Search</span>
          <input value={query} onChange={event => setQuery(event.target.value)} placeholder="tool, ref, source…" />
        </label>
        {sourceTypes.length > 0 ? (
          <div className="astrbot-source-type-checks" aria-label="Source type filters">
            {sourceTypes.map(sourceType => (
              <label key={sourceType}>
                <input
                  type="checkbox"
                  checked={!disabledTypes.includes(sourceType)}
                  onChange={() => toggleSourceType(sourceType)}
                />
                <span>{sourceType}</span>
              </label>
            ))}
          </div>
        ) : null}
      </div>
      {visibleTools.length > 0 ? (
        <div className="astrbot-analysis-tools">
          {visibleTools.slice(0, 4).map((item, index) => (
            <section key={`${item.toolName}-${item.sourceType}-${index}`} className="astrbot-analysis-tool">
              <div>
                <code>{item.toolName}</code>
                <span>{item.sourceType} · {item.success ? "success" : "failed"} · {item.rowCount} rows</span>
              </div>
              <p>{item.summary}</p>
              {item.keyFindings.length > 0 ? (
                <ul>
                  {item.keyFindings.slice(0, 3).map((finding, findingIndex) => (
                    <li key={`${finding}-${findingIndex}`}>{finding}</li>
                  ))}
                </ul>
              ) : null}
            </section>
          ))}
        </div>
      ) : null}
      {visibleRefs.length > 0 ? (
        <div className="astrbot-analysis-refs" aria-label="Evidence references">
          {visibleRefs.slice(0, 12).map((ref, index) => (
            <span key={`${ref.refId || `${ref.label}-${ref.value}`}-${index}`}>
              {ref.label}: <strong>{String(ref.value ?? "n/a")}{ref.unit ? ` ${ref.unit}` : ""}</strong>
              {ref.periodLabel ? <small> · {ref.periodLabel}</small> : null}
            </span>
          ))}
        </div>
      ) : null}
    </details>
  );
}

function InsightCards({ cards }: { cards: InsightCard[] }) {
  if (cards.length === 0) {
    return null;
  }
  return (
    <div className="astrbot-insight-cards" aria-label="Research insight cards">
      {cards.slice(0, 4).map(card => (
        <article key={`${card.title}-${card.claim}`}>
          <header>
            <span>{card.confidence}</span>
            <strong>{card.title}</strong>
          </header>
          <p>{card.claim}</p>
          {card.evidence.length > 0 ? (
            <div className="astrbot-insight-evidence">
              {card.evidence.slice(0, 3).map(item => <span key={item}>{item}</span>)}
            </div>
          ) : null}
          {card.implication ? <small>{card.implication}</small> : null}
          {card.recommendedAction ? <em>{card.recommendedAction}</em> : null}
        </article>
      ))}
    </div>
  );
}

function MethodDistillationPanel({ method }: { method?: BusinessMethodDistillation }) {
  if (!method) {
    return null;
  }
  const playbook = method.pricingPlaybook;
  return (
    <section className="astrbot-method-distillation" aria-label="Business method distillation">
      <header>
        <div>
          <span>Method Distillation</span>
          <strong>{method.deckTitle}</strong>
        </div>
        <small>{method.methodType} · {method.market || "market"} · {method.model || "model"}</small>
      </header>
      <div className="astrbot-method-grid">
        <article>
          <span>Analysis flow</span>
          <div className="astrbot-analysis-path-chips">
            {method.analysisFlow.slice(0, 8).map(item => <span key={item}>{item}</span>)}
          </div>
        </article>
        <article>
          <span>Competitor pool</span>
          <strong>{method.competitorPool.length ? method.competitorPool.join(", ") : "not detected"}</strong>
          {method.priceCorridor.positioning ? <p>{method.priceCorridor.positioning} · {method.priceCorridor.coreCorridor}</p> : null}
        </article>
        <article>
          <span>Playbook</span>
          <p>{playbook.market_window}</p>
          <p>{playbook.main_trim_strategy}</p>
        </article>
        <article>
          <span>Feature value</span>
          <ul>
            {method.featureValueClaims.slice(0, 5).map(item => (
              <li key={item.evidenceRef || item.featureName}>
                <strong>{item.featureName}</strong>: {item.customerValue}
              </li>
            ))}
          </ul>
        </article>
      </div>
      {method.dataQualityWarnings.length > 0 ? (
        <div className="astrbot-method-warnings" aria-label="Method data quality warnings">
          {method.dataQualityWarnings.slice(0, 5).map(warning => (
            <span key={warning.code} title={warning.mitigation}>
              {warning.severity}: {warning.code}
            </span>
          ))}
        </div>
      ) : null}
    </section>
  );
}

export function AnalysisPathPanel({
  evidencePlan,
  evidencePackage,
  qualityScore,
  businessSynthesis,
  methodDistillation,
  toolCalls,
}: {
  evidencePlan?: Record<string, unknown>;
  evidencePackage?: EvidencePackage;
  qualityScore?: QualityScore;
  businessSynthesis?: BusinessSynthesisPlan;
  methodDistillation?: BusinessMethodDistillation;
  toolCalls: string[];
}) {
  if (!evidencePlan && !evidencePackage && !qualityScore && !businessSynthesis && !methodDistillation) {
    return null;
  }
  const intent = readText(evidencePlan?.intent, evidencePackage?.intent || "unknown");
  const requiredTools = readStringList(evidencePlan?.requiredTools);
  const allowedTools = readStringList(evidencePlan?.allowedTools);
  const mustHaveEvidence = readStringList(evidencePlan?.mustHaveEvidence);
  const confidence = evidencePackage?.confidence ?? "unknown";
  const usedTools = toolCalls.length > 0 ? toolCalls : evidencePackage?.toolResults.map(item => item.toolName) ?? [];
  const toolCoverage = buildToolCoverageSummary(requiredTools, usedTools, evidencePackage?.missingEvidence ?? []);
  const score = qualityScore ? `${Math.round(qualityScore.totalScore * 100)}%` : "n/a";
  const refs = evidencePackage?.toolResults.flatMap(item => item.evidenceRefs).slice(0, 8) ?? [];
  const toolResults = evidencePackage?.toolResults ?? [];
  const governance = evidencePackage?.researchGovernance;
  const metrics = governance?.metrics;
  const crossCheck = evidencePackage?.jatoCrossCheck;
  const insightCards = evidencePackage?.insightCards ?? [];
  const scopeDiagnostics = evidencePackage?.scopeDiagnostics;
  return (
    <details className="astrbot-analysis-path">
      <summary>
        <span>Analysis Path</span>
        <strong>{intent}</strong>
        <small>{confidence} confidence · score {score}</small>
      </summary>
      <div className="astrbot-analysis-path-grid">
        <div>
          <span>Intent</span>
          <strong>{intent}</strong>
        </div>
        <div>
          <span>Used Tools</span>
          <strong>{usedTools.length ? usedTools.join(", ") : "none"}</strong>
        </div>
        <div>
          <span>Required Tools</span>
          <strong>{requiredTools.length ? requiredTools.join(", ") : "none"}</strong>
        </div>
        <div className={`astrbot-tool-coverage-card is-${toolCoverage.status}`}>
          <span>Tool Coverage</span>
          <strong>{toolCoverage.label}</strong>
          <small>
            {toolCoverage.missing.length
              ? `Missing: ${toolCoverage.missing.join(", ")}`
              : toolCoverage.satisfied.length
                ? `Satisfied: ${toolCoverage.satisfied.join(", ")}`
                : "No required tool for this intent"}
          </small>
        </div>
        <div>
          <span>Evidence</span>
          <strong>{refs.length} refs · {evidencePackage?.missingEvidence.length ?? 0} missing</strong>
        </div>
        <div>
          <span>Evidence Scope</span>
          <strong>
            {scopeDiagnostics
              ? `${scopeDiagnostics.parallelScopes.length} parallel · ${scopeDiagnostics.conflicts.length} conflicts`
              : "n/a"}
          </strong>
          {scopeDiagnostics?.hasBlockingConflict ? <small>Composer blocked on conflicting same-scope values</small> : null}
        </div>
        <div>
          <span>Research Policy</span>
          <strong>{governance?.policyStatus ?? "n/a"}</strong>
        </div>
        <div>
          <span>Research Mode</span>
          <strong>{governance?.mode?.mode ?? "n/a"}{metrics ? ` · ${metrics.queryCount}q/${metrics.sourcesUsed}s` : ""}</strong>
        </div>
        <div>
          <span>Cost / Latency</span>
          <strong>{metrics ? `$${metrics.estimatedCost.toFixed(4)} · ${metrics.latencyMs}ms` : "n/a"}</strong>
        </div>
        <div>
          <span>JATO Cross-check</span>
          <strong>{crossCheck?.status ?? "n/a"}</strong>
        </div>
        <div>
          <span>Business Synthesis</span>
          <strong>{businessSynthesis?.evidenceAlignment.status ?? "n/a"}</strong>
        </div>
        <div>
          <span>Method Distillation</span>
          <strong>{methodDistillation ? `${methodDistillation.methodType} · ${methodDistillation.dataQualityWarnings.length} warnings` : "n/a"}</strong>
        </div>
      </div>
      {businessSynthesis?.executiveConclusion ? (
        <p className="astrbot-policy-summary">{businessSynthesis.executiveConclusion}</p>
      ) : null}
      {businessSynthesis?.businessImplications.length ? (
        <div className="astrbot-analysis-path-chips" aria-label="Business implications">
          {businessSynthesis.businessImplications.slice(0, 4).map(item => <span key={item}>{item}</span>)}
        </div>
      ) : null}
      {crossCheck?.summary ? (
        <p className="astrbot-policy-summary">{crossCheck.summary}</p>
      ) : null}
      {governance?.policy?.preferredSourceTiers.length ? (
        <div className="astrbot-analysis-path-chips" aria-label="Preferred source tiers">
          {governance.policy.preferredSourceTiers.map(item => <span key={item}>{item}</span>)}
        </div>
      ) : null}
      {mustHaveEvidence.length > 0 ? (
        <div className="astrbot-analysis-path-chips" aria-label="Must-have evidence">
          {mustHaveEvidence.map(item => <span key={item}>{item}</span>)}
        </div>
      ) : null}
      <InsightCards cards={insightCards} />
      <MethodDistillationPanel method={methodDistillation} />
      <EvidenceSourceFilter toolResults={toolResults} refs={refs} />
      {evidencePackage?.missingEvidence.length ? (
        <div className="astrbot-analysis-missing" aria-label="Missing evidence">
          {evidencePackage.missingEvidence.slice(0, 5).map(item => (
            <span key={`${item.name}-${item.impact}`}>{item.name} · {item.impact}</span>
          ))}
        </div>
      ) : null}
      {qualityScore?.failures.length ? (
        <div className="astrbot-analysis-missing" aria-label="Quality failures">
          {qualityScore.failures.map(item => <span key={item}>{item}</span>)}
        </div>
      ) : null}
      {allowedTools.length > 0 ? (
        <p className="astrbot-analysis-allowed">Allowed tools: {allowedTools.join(", ")}</p>
      ) : null}
    </details>
  );
}

function ModelUsagePanel({ usage }: { usage: AgentModelUsage | null }) {
  if (!usage) {
    return null;
  }
  const tokenTotal = usage.totalTokens || usage.promptTokens + usage.completionTokens;
  return (
    <section className="astrbot-model-usage" aria-label="Model usage">
      <div className="astrbot-model-usage-heading">
        <span className="page-kicker">Model Usage</span>
        <h3>{usage.status === "ok" ? "DPV4 final composer" : "Composer fallback"}</h3>
        <p>{usage.provider} · {usage.model} · {usage.status}</p>
      </div>
      <div className="astrbot-agent-card-grid">
        <div className="astrbot-agent-card">
          <span>Total Tokens</span>
          <strong>{formatTokens(tokenTotal)}</strong>
        </div>
        <div className="astrbot-agent-card">
          <span>Input / Output</span>
          <strong>{formatTokens(usage.promptTokens)} / {formatTokens(usage.completionTokens)}</strong>
        </div>
        <div className="astrbot-agent-card">
          <span>Est. Cost</span>
          <strong>{formatCost(usage.estimatedCostCny, usage.currency)}</strong>
        </div>
        <div className="astrbot-agent-card">
          <span>Pricing Model</span>
          <strong>{usage.pricingModel}</strong>
        </div>
      </div>
      {usage.usageId || usage.finishReason || usage.fallbackReason ? (
        <div className="astrbot-model-usage-meta">
          {usage.usageId ? <code>{usage.usageId}</code> : null}
          {usage.finishReason ? <span>finish: {usage.finishReason}</span> : null}
          {usage.fallbackReason ? <span>{usage.fallbackReason}</span> : null}
        </div>
      ) : null}
    </section>
  );
}

function ProfilePanel({ profile }: { profile: AstrBotAgentProfile | null }) {
  const [copyState, setCopyState] = useState<string | null>(null);
  if (!profile) {
    return <div className="astrbot-table-empty">Profile metadata unavailable</div>;
  }

  const fullConfig = JSON.stringify(profile, null, 2);

  async function copyText(label: string, value: string): Promise<void> {
    try {
      await navigator.clipboard.writeText(value);
      setCopyState(`${label} copied`);
    } catch {
      setCopyState("Copy failed");
    }
  }

  return (
    <div className="astrbot-profile-panel">
      <section className="astrbot-profile-hero">
        <div>
          <span className="page-kicker">Default Persona</span>
          <h3>{profile.id}</h3>
          <p>{profile.positioning}</p>
        </div>
        <div className="astrbot-profile-actions">
          <button type="button" className="astrbot-native-toggle" onClick={() => void copyText("System prompt", profile.systemPrompt)}>
            Copy Prompt
          </button>
          <button type="button" className="astrbot-native-toggle" onClick={() => void copyText("Full config", fullConfig)}>
            Copy Config
          </button>
        </div>
      </section>

      {copyState ? <div className="astrbot-status-pill">{copyState}</div> : null}

      <section className="astrbot-profile-grid" aria-label="Profile metadata">
        <div>
          <span>Name</span>
          <strong>{profile.name}</strong>
        </div>
        <div>
          <span>Short ID</span>
          <strong>{profile.shortId}</strong>
        </div>
        <div>
          <span>Language</span>
          <strong>{profile.defaultLanguage}</strong>
        </div>
        <div>
          <span>Failure Message</span>
          <strong>{profile.modelFailureMessage}</strong>
        </div>
      </section>

      <section className="astrbot-profile-columns">
        <ProfileList title="Core capabilities" items={profile.coreCapabilities} />
        <ProfileList title="Suggested tools" items={profile.suggestedTools} />
        <ProfileList title="Suggested skills" items={profile.suggestedSkills} />
        <ProfileList title="Safety rules" items={profile.safetyRules} />
      </section>

      <section className="astrbot-profile-prompt">
        <div className="astrbot-tool-probe-heading">
          <span className="page-kicker">System Prompt</span>
          <h3>AstrBot Persona Prompt</h3>
        </div>
        <textarea readOnly value={profile.systemPrompt} rows={18} />
      </section>

      <section className="astrbot-profile-presets" aria-label="Preset dialogs">
        {profile.presetDialogs.map(dialog => (
          <article key={dialog.title}>
            <span>{dialog.title}</span>
            <strong>用户：{dialog.user}</strong>
            <p>助手：{dialog.assistant}</p>
          </article>
        ))}
      </section>
    </div>
  );
}

function ProfileList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="astrbot-profile-list">
      <span>{title}</span>
      <ul>
        {items.map(item => <li key={item}>{item}</li>)}
      </ul>
    </div>
  );
}

function BrowserAssistPanel() {
  const [browserUrl, setBrowserUrl] = useState("https://example.com/");
  const [actionGoal, setActionGoal] = useState("Find the market report or dashboard link.");
  const [planLoading, setPlanLoading] = useState(false);
  const [planError, setPlanError] = useState<string | null>(null);
  const [planResult, setPlanResult] = useState<AstrBotToolCallResponse | null>(null);
  const [actionTextById, setActionTextById] = useState<Record<string, string>>({});
  const [actionLoadingId, setActionLoadingId] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [confirmedResult, setConfirmedResult] = useState<AstrBotToolCallResponse | null>(null);
  const plan = readBrowserActionPlan(planResult);
  const confirmed = readBrowserActionResult(confirmedResult);
  const canPlan = browserUrl.trim().length > 0 && !planLoading;

  async function generatePlan(): Promise<void> {
    if (!canPlan) {
      return;
    }
    setPlanLoading(true);
    setPlanError(null);
    setActionError(null);
    setConfirmedResult(null);
    setActionTextById({});
    try {
      setPlanResult(await callAstrBotTool("browser_interaction_plan", {
        url: browserUrl.trim(),
        action_goal: actionGoal.trim(),
        max_actions: 8,
      }));
    } catch (error) {
      setPlanResult(null);
      setPlanError(error instanceof Error ? error.message : String(error));
    } finally {
      setPlanLoading(false);
    }
  }

  async function confirmAction(action: BrowserPlanAction): Promise<void> {
    const typedText = (actionTextById[action.actionId] ?? "").trim();
    if (action.actionType === "type" && !typedText) {
      setActionError("Type actions require text before confirmation.");
      return;
    }

    setActionLoadingId(action.actionId);
    setActionError(null);
    setConfirmedResult(null);
    try {
      const payload: Record<string, unknown> = {
        url: plan?.url || browserUrl.trim(),
        action_id: action.actionId,
        confirmation_token: action.confirmationToken,
      };
      if (action.actionType === "type") {
        payload.text = typedText;
      }
      setConfirmedResult(await callAstrBotTool(
        action.actionType === "type" ? "browser_type_confirmed" : "browser_click_confirmed",
        payload,
      ));
    } catch (error) {
      setActionError(error instanceof Error ? error.message : String(error));
    } finally {
      setActionLoadingId(null);
    }
  }

  return (
    <section className="astrbot-browser-assist" aria-label="Browser assisted interaction">
      <div className="astrbot-tool-probe-heading">
        <span className="page-kicker">Browser Level 3</span>
        <h3>Confirmed Browser Assist</h3>
        <p>Generate a public-page action plan, then confirm one click or type action at a time.</p>
      </div>
      <div className="astrbot-browser-assist-controls">
        <label>
          <span>URL</span>
          <input
            type="url"
            value={browserUrl}
            onChange={event => setBrowserUrl(event.target.value)}
            placeholder="https://example.com/"
          />
        </label>
        <label>
          <span>Goal</span>
          <input
            value={actionGoal}
            onChange={event => setActionGoal(event.target.value)}
            placeholder="Find a public report link"
          />
        </label>
        <button type="button" className="astrbot-native-toggle" onClick={() => void generatePlan()} disabled={!canPlan}>
          {planLoading ? "Planning" : "Generate Plan"}
        </button>
      </div>

      {planError ? <div className="astrbot-status-error" role="status">{planError}</div> : null}
      {actionError ? <div className="astrbot-status-error" role="status">{actionError}</div> : null}

      {plan ? (
        <div className="astrbot-browser-plan" aria-label="Browser action plan">
          <div className="astrbot-browser-plan-summary">
            <div>
              <span>Status</span>
              <strong>{plan.status}</strong>
            </div>
            <div>
              <span>Engine</span>
              <strong>{plan.browserEngine}</strong>
            </div>
            <div>
              <span>Actions</span>
              <strong>{plan.actions.length}</strong>
            </div>
            <div>
              <span>Page</span>
              <strong>{plan.title}</strong>
            </div>
          </div>

          {plan.approvalInstructions ? (
            <p className="astrbot-browser-approval">{plan.approvalInstructions}</p>
          ) : null}

          {plan.limitations.length > 0 ? (
            <div className="astrbot-browser-limitations" aria-label="Browser plan limitations">
              {plan.limitations.map(item => <span key={item}>{item}</span>)}
            </div>
          ) : null}

          <div className="astrbot-browser-action-list">
            {plan.actions.length > 0 ? plan.actions.map(action => {
              const typedText = actionTextById[action.actionId] ?? "";
              const typeActionBlocked = action.actionType === "type" && !typedText.trim();
              const runningThisAction = actionLoadingId === action.actionId;
              const buttonLabel = runningThisAction
                ? "Confirming"
                : action.actionType === "type"
                  ? "Confirm Type"
                  : "Confirm Click";
              return (
                <article key={action.actionId} className="astrbot-browser-action-card">
                  <div className="astrbot-browser-action-head">
                    <div>
                      <span className="page-kicker">{action.actionType}</span>
                      <h4>{action.label}</h4>
                    </div>
                    <strong>{action.risk}</strong>
                  </div>
                  <div className="astrbot-browser-action-meta">
                    <span>{action.actionId}</span>
                    <span>{action.targetType}</span>
                    <span>expires {formatBrowserExpiry(action.expiresAt)}</span>
                    {action.targetUrl ? <a href={action.targetUrl} target="_blank" rel="noreferrer">target</a> : null}
                    {action.selectorHint ? <code>{action.selectorHint}</code> : null}
                  </div>
                  <div className="astrbot-browser-action-controls">
                    {action.actionType === "type" ? (
                      <input
                        value={typedText}
                        onChange={event => setActionTextById(current => ({
                          ...current,
                          [action.actionId]: event.target.value,
                        }))}
                        maxLength={500}
                        placeholder="Text to type"
                      />
                    ) : null}
                    <button
                      type="button"
                      className="astrbot-native-toggle"
                      onClick={() => void confirmAction(action)}
                      disabled={Boolean(actionLoadingId) || !action.requiresUserApproval || typeActionBlocked}
                    >
                      {buttonLabel}
                    </button>
                  </div>
                </article>
              );
            }) : (
              <div className="astrbot-table-empty">No confirmable actions found for this page.</div>
            )}
          </div>
        </div>
      ) : null}

      {confirmed ? (
        <section className="astrbot-browser-result" aria-label="Confirmed browser action result">
          <div>
            <span className="page-kicker">{confirmed.status}</span>
            <h4>{confirmed.title}</h4>
            <code>{confirmed.resultUrl || confirmed.url}</code>
          </div>
          {confirmed.textPreview ? <p>{confirmed.textPreview}</p> : null}
          <div className="astrbot-browser-action-meta">
            <span>{confirmed.action}</span>
            <span>{confirmed.actionId}</span>
            {confirmed.typedCharacters > 0 ? <span>{confirmed.typedCharacters} chars typed</span> : null}
          </div>
          {confirmed.limitations.length > 0 ? (
            <div className="astrbot-browser-limitations">
              {confirmed.limitations.map(item => <span key={item}>{item}</span>)}
            </div>
          ) : null}
        </section>
      ) : null}
    </section>
  );
}

function McpPanel({ tools }: { tools: AstrBotToolDefinition[] }) {
  const [probeTool, setProbeTool] = useState("query_country_snapshot");
  const [probeCountry, setProbeCountry] = useState("Sweden");
  const [probeQuestion, setProbeQuestion] = useState("2025 BEV market summary");
  const [probeLoading, setProbeLoading] = useState(false);
  const [probeError, setProbeError] = useState<string | null>(null);
  const [probeResult, setProbeResult] = useState<AstrBotToolCallResponse | null>(null);
  const selectedTool = tools.find(tool => tool.name === probeTool) ?? tools[0] ?? null;
  const missingCountry = Boolean(selectedTool?.required.includes("country")) && !probeCountry.trim();
  const missingQuestion = Boolean(selectedTool?.required.includes("question")) && !probeQuestion.trim();
  const canRunProbe = Boolean(selectedTool) && !missingCountry && !missingQuestion && !probeLoading;

  useEffect(() => {
    if (tools.length > 0 && !tools.some(tool => tool.name === probeTool)) {
      setProbeTool(tools[0].name);
    }
  }, [probeTool, tools]);

  async function runProbe(): Promise<void> {
    if (!selectedTool || !canRunProbe) {
      return;
    }
    const argumentsPayload: Record<string, unknown> = {};
    if (probeCountry.trim()) {
      argumentsPayload.country = probeCountry.trim();
    }
    if (probeQuestion.trim()) {
      argumentsPayload.question = probeQuestion.trim();
    }
    setProbeLoading(true);
    setProbeError(null);
    try {
      setProbeResult(await callAstrBotTool(selectedTool.name, argumentsPayload));
    } catch (error) {
      setProbeError(error instanceof Error ? error.message : String(error));
    } finally {
      setProbeLoading(false);
    }
  }

  return (
    <>
      <div className="astrbot-table-shell">
        <table className="astrbot-table">
          <thead>
            <tr>
              <th>Tool</th>
              <th>Description</th>
              <th>Required</th>
            </tr>
          </thead>
          <tbody>
            {tools.length > 0 ? tools.map(tool => (
              <tr key={tool.name}>
                <td><code>{tool.name}</code></td>
                <td>{tool.description}</td>
                <td>{formatRequiredFields(tool)}</td>
              </tr>
            )) : (
              <tr>
                <td colSpan={3} className="astrbot-table-empty">Tool metadata unavailable</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <section className="astrbot-tool-probe" aria-label="JATO MCP tool probe">
        <div className="astrbot-tool-probe-heading">
          <span className="page-kicker">Probe</span>
          <h3>Tool Probe</h3>
        </div>
        <div className="astrbot-tool-probe-controls">
          <label>
            <span>Tool</span>
            <select value={selectedTool?.name ?? ""} onChange={event => setProbeTool(event.target.value)}>
              {tools.map(tool => (
                <option key={tool.name} value={tool.name}>{tool.name}</option>
              ))}
            </select>
          </label>
          <label>
            <span>Country</span>
            <input value={probeCountry} onChange={event => setProbeCountry(event.target.value)} />
          </label>
          <label>
            <span>Question</span>
            <input value={probeQuestion} onChange={event => setProbeQuestion(event.target.value)} />
          </label>
          <button type="button" className="astrbot-native-toggle" onClick={() => void runProbe()} disabled={!canRunProbe}>
            {probeLoading ? "Running" : "Run Probe"}
          </button>
        </div>
        {probeError ? <div className="astrbot-status-error" role="status">{probeError}</div> : null}
        {probeResult ? (
          <pre className="astrbot-tool-result">{JSON.stringify(buildProbePreview(probeResult), null, 2)}</pre>
        ) : null}
      </section>
      <BrowserAssistPanel />
    </>
  );
}

function ProvidersPanel({
  provider,
  retrieval,
}: {
  provider: AstrBotProviderStatus | null;
  retrieval: AstrBotRetrievalDependencyStatus | null;
}) {
  const visibleProvider = provider ?? {
    ...ASTRBOT_PROVIDER,
    keyConfigured: false,
    status: "unknown",
  };
  const [usage, setUsage] = useState<AgentUsageSummary | null>(null);
  const [usageError, setUsageError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function loadUsage(): Promise<void> {
      try {
        const summary = await fetchAgentUsageSummary(10);
        if (!cancelled) {
          setUsage(summary);
          setUsageError(null);
        }
      } catch (error) {
        if (!cancelled) {
          setUsageError(error instanceof Error ? error.message : String(error));
        }
      }
    }
    void loadUsage();
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <>
      <div className="astrbot-provider-grid">
        <div className="astrbot-provider-row">
          <span>Provider ID</span>
          <code>{visibleProvider.providerId}</code>
        </div>
        <div className="astrbot-provider-row">
          <span>Source ID</span>
          <code>{visibleProvider.sourceId}</code>
        </div>
        <div className="astrbot-provider-row">
          <span>Model</span>
          <code>{visibleProvider.model}</code>
        </div>
        <div className="astrbot-provider-row">
          <span>API Base</span>
          <code>{visibleProvider.apiBase}</code>
        </div>
        <div className="astrbot-provider-row">
          <span>Key Source</span>
          <code>{visibleProvider.keySource}</code>
        </div>
        <div className="astrbot-provider-row">
          <span>Key Status</span>
          <code>{visibleProvider.keyConfigured ? "configured" : "missing"}</code>
        </div>
      </div>
      {retrieval ? (
        <section className="astrbot-provider-grid" aria-label="Retrieval dependency status">
          <div className="astrbot-provider-row">
            <span>PageIndex</span>
            <code>{retrieval.pageIndex.status} · {retrieval.pageIndex.keySource}</code>
          </div>
          <div className="astrbot-provider-row">
            <span>PageIndex MCP</span>
            <code>{retrieval.pageIndex.mcpUrl}</code>
          </div>
          <div className="astrbot-provider-row">
            <span>PageIndex Fallback</span>
            <code>{retrieval.pageIndex.fallback}</code>
          </div>
          <div className="astrbot-provider-row">
            <span>MiniRAG</span>
            <code>{retrieval.miniRag.status} · library {retrieval.miniRag.libraryInstalled ? "installed" : "missing"} · API {retrieval.miniRag.apiConfigured ? "configured" : "not configured"}</code>
          </div>
          <div className="astrbot-provider-row">
            <span>MiniRAG Storage</span>
            <code>{retrieval.miniRag.workingDir} · corpus {retrieval.miniRag.corpusStatus}</code>
          </div>
          <div className="astrbot-provider-row">
            <span>MiniRAG Fallback</span>
            <code>{retrieval.miniRag.fallback}</code>
          </div>
        </section>
      ) : null}
      <AgentUsageSummaryPanel usage={usage} error={usageError} />
    </>
  );
}

function AgentUsageSummaryPanel({
  usage,
  error,
}: {
  usage: AgentUsageSummary | null;
  error: string | null;
}) {
  if (error) {
    return <div className="astrbot-status-error" role="status">Usage unavailable: {error}</div>;
  }
  if (!usage) {
    return <div className="astrbot-table-empty">Loading agent usage…</div>;
  }
  return (
    <section className="astrbot-usage-summary" aria-label="Agent usage summary">
      <div className="astrbot-model-usage-heading">
        <span className="page-kicker">Usage &amp; Cost</span>
        <h3>Agent Chat Usage</h3>
        <p>Normal chat answers written to Hermes agent usage and answer audit logs.</p>
      </div>
      <div className="astrbot-agent-card-grid">
        <div className="astrbot-agent-card">
          <span>Total Runs</span>
          <strong>{usage.totalRuns.toLocaleString()}</strong>
        </div>
        <div className="astrbot-agent-card">
          <span>Total Tokens</span>
          <strong>{formatTokens(usage.totalTokens)}</strong>
        </div>
        <div className="astrbot-agent-card">
          <span>Input / Output</span>
          <strong>{formatTokens(usage.totalInputTokens)} / {formatTokens(usage.totalOutputTokens)}</strong>
        </div>
        <div className="astrbot-agent-card">
          <span>Total Cost</span>
          <strong>{formatCost(usage.totalCostCny, usage.currency)}</strong>
        </div>
      </div>
      <div className="astrbot-usage-split-grid">
        <UsageBucketList title="By Model" buckets={usage.byModel} currency={usage.currency} />
        <UsageBucketList title="By Tool" buckets={usage.byTool} currency={usage.currency} />
      </div>
      {usage.recent.length > 0 ? (
        <div className="astrbot-table-shell">
          <table className="astrbot-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Model</th>
                <th>Tool</th>
                <th>Tokens</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {usage.recent.map(record => (
                <tr key={record.usageId}>
                  <td>{new Date(record.recordedAt).toLocaleString()}</td>
                  <td><code>{record.pricingModel || record.model}</code></td>
                  <td><code>{record.selectedTool}</code></td>
                  <td>{formatTokens(record.totalTokens)}</td>
                  <td>{formatCost(record.estimatedCostCny, record.currency || usage.currency)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="astrbot-table-empty">No DPV4 chat usage recorded yet.</div>
      )}
    </section>
  );
}

function UsageBucketList({
  title,
  buckets,
  currency,
}: {
  title: string;
  buckets: Record<string, UsageBucketMetric>;
  currency: string;
}) {
  const entries = Object.entries(buckets);
  return (
    <div className="astrbot-usage-bucket">
      <h4>{title}</h4>
      {entries.length > 0 ? entries.map(([key, value]) => (
        <div key={key}>
          <code>{key}</code>
          <span>{value.runs} runs · {formatTokens(value.tokens)} tokens · {formatCost(value.costCny, currency)}</span>
        </div>
      )) : <span>No records</span>}
    </div>
  );
}

interface UsageDashboardData {
  usage: AgentUsageSummary;
  sessions: AgentConversationSession[];
}

async function fetchUsageDashboard(limit: UsageLimit): Promise<UsageDashboardData> {
  const [usage, sessionResponse] = await Promise.all([
    fetchAgentUsageSummary(limit),
    fetchAgentConversationSessions(20),
  ]);
  return {
    usage,
    sessions: sessionResponse.items,
  };
}

function AstrBotUsagePanel() {
  const [usageLimit, setUsageLimit] = useState<UsageLimit>(30);
  const [usage, setUsage] = useState<AgentUsageSummary | null>(null);
  const [sessions, setSessions] = useState<AgentConversationSession[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    void fetchUsageDashboard(usageLimit)
      .then(data => {
        if (!active) return;
        setUsage(data.usage);
        setSessions(data.sessions);
      })
      .catch(fetchError => {
        if (!active) return;
        setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, [usageLimit]);

  async function refreshUsageDashboard(): Promise<void> {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchUsageDashboard(usageLimit);
      setUsage(data.usage);
      setSessions(data.sessions);
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
    } finally {
      setLoading(false);
    }
  }

  return (
    <section className="astrbot-usage-dashboard" aria-label="Conversation usage dashboard">
      <div className="astrbot-usage-toolbar">
        <div className="astrbot-model-usage-heading">
          <span className="page-kicker">Conversation Usage</span>
          <h3>Session, Token and Cost Monitor</h3>
          <p>DPV4 answer composition, routed tools and persisted conversation sessions.</p>
        </div>
        <div className="astrbot-usage-controls" aria-label="Usage dashboard controls">
          <div className="astrbot-usage-limit-group" role="group" aria-label="Recent usage records">
            {usageLimitOptions.map(option => (
              <button
                key={option}
                type="button"
                className={`astrbot-usage-limit${usageLimit === option ? " is-active" : ""}`}
                onClick={() => setUsageLimit(option)}
                aria-pressed={usageLimit === option}
              >
                {option}
              </button>
            ))}
          </div>
          <button
            type="button"
            className="astrbot-native-toggle"
            onClick={() => void refreshUsageDashboard()}
            disabled={loading}
          >
            {loading ? "Refreshing" : "Refresh"}
          </button>
        </div>
      </div>

      {error ? <div className="astrbot-status-error" role="status">Usage dashboard unavailable: {error}</div> : null}
      {loading && !usage ? <div className="astrbot-table-empty">Loading usage dashboard…</div> : null}

      {usage ? (
        <>
          <div className="astrbot-agent-card-grid astrbot-usage-kpis">
            <div className="astrbot-agent-card">
              <span>Recorded Runs</span>
              <strong>{usage.totalRuns.toLocaleString()}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Recent Sessions</span>
              <strong>{sessions.length.toLocaleString()}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Total Tokens</span>
              <strong>{formatTokens(usage.totalTokens)}</strong>
            </div>
            <div className="astrbot-agent-card">
              <span>Avg Cost / Run</span>
              <strong>{formatCost(usage.avgCostPerRunCny, usage.currency)}</strong>
            </div>
          </div>

          <UsageTokenMeter usage={usage} />

          <div className="astrbot-usage-visual-grid">
            <UsageBucketBars title="Model Mix" subtitle="Runs by pricing model" buckets={usage.byModel} currency={usage.currency} />
            <UsageBucketBars title="Tool Mix" subtitle="Runs by selected tool" buckets={usage.byTool} currency={usage.currency} />
            <UsageStatusBars statuses={usage.byStatus} totalRuns={usage.totalRuns} />
          </div>

          <div className="astrbot-usage-detail-grid">
            <RecentUsageTable records={usage.recent} currency={usage.currency} />
            <UsageSessionList sessions={sessions} />
          </div>
        </>
      ) : null}
    </section>
  );
}

function UsageTokenMeter({ usage }: { usage: AgentUsageSummary }) {
  const tokenTotal = usage.totalInputTokens + usage.totalOutputTokens;
  const inputShare = tokenTotal > 0 ? Math.round((usage.totalInputTokens / tokenTotal) * 100) : 0;
  const outputShare = tokenTotal > 0 ? 100 - inputShare : 0;
  return (
    <section className="astrbot-usage-meter" aria-label="Token split">
      <div className="astrbot-usage-meter-heading">
        <div>
          <span>Input Tokens</span>
          <strong>{formatTokens(usage.totalInputTokens)}</strong>
        </div>
        <div>
          <span>Output Tokens</span>
          <strong>{formatTokens(usage.totalOutputTokens)}</strong>
        </div>
        <div>
          <span>Total Cost</span>
          <strong>{formatCost(usage.totalCostCny, usage.currency)}</strong>
        </div>
      </div>
      <div className="astrbot-usage-meter-track" aria-hidden="true">
        <span className="astrbot-usage-meter-fill is-input" style={{ width: `${inputShare}%` }} />
        <span className="astrbot-usage-meter-fill is-output" style={{ width: `${outputShare}%` }} />
      </div>
      <div className="astrbot-usage-meter-legend">
        <span>Input {inputShare}%</span>
        <span>Output {outputShare}%</span>
      </div>
    </section>
  );
}

function UsageBucketBars({
  title,
  subtitle,
  buckets,
  currency,
}: {
  title: string;
  subtitle: string;
  buckets: Record<string, UsageBucketMetric>;
  currency: string;
}) {
  const entries = Object.entries(buckets)
    .sort(([, first], [, second]) => second.runs - first.runs || second.tokens - first.tokens)
    .slice(0, 6);
  const maxRuns = entries.reduce((currentMax, [, value]) => Math.max(currentMax, value.runs), 0);
  return (
    <section className="astrbot-usage-bar-card" aria-label={title}>
      <div className="astrbot-usage-bar-heading">
        <h4>{title}</h4>
        <span>{subtitle}</span>
      </div>
      <div className="astrbot-usage-bar-list">
        {entries.length > 0 ? entries.map(([key, value]) => (
          <div className="astrbot-usage-bar-row" key={key}>
            <div className="astrbot-usage-bar-copy">
              <code>{key}</code>
              <span>{value.runs} runs · {formatTokens(value.tokens)} tokens · {formatCost(value.costCny, currency)}</span>
            </div>
            <div className="astrbot-usage-bar-track" aria-hidden="true">
              <span style={{ width: formatUsageBarWidth(value.runs, maxRuns) }} />
            </div>
          </div>
        )) : <div className="astrbot-table-empty">No records</div>}
      </div>
    </section>
  );
}

function UsageStatusBars({ statuses, totalRuns }: { statuses: Record<string, number>; totalRuns: number }) {
  const entries = Object.entries(statuses).sort(([, first], [, second]) => second - first);
  const maxCount = entries.reduce((currentMax, [, value]) => Math.max(currentMax, value), 0);
  return (
    <section className="astrbot-usage-bar-card" aria-label="Status mix">
      <div className="astrbot-usage-bar-heading">
        <h4>Status Mix</h4>
        <span>Completion result by run</span>
      </div>
      <div className="astrbot-usage-bar-list">
        {entries.length > 0 ? entries.map(([status, count]) => {
          const share = totalRuns > 0 ? Math.round((count / totalRuns) * 100) : 0;
          return (
            <div className="astrbot-usage-bar-row" key={status}>
              <div className="astrbot-usage-bar-copy">
                <code>{status}</code>
                <span>{count} runs · {share}%</span>
              </div>
              <div className="astrbot-usage-bar-track" aria-hidden="true">
                <span style={{ width: formatUsageBarWidth(count, maxCount) }} />
              </div>
            </div>
          );
        }) : <div className="astrbot-table-empty">No records</div>}
      </div>
    </section>
  );
}

function RecentUsageTable({ records, currency }: { records: AgentUsageRecord[]; currency: string }) {
  return (
    <section className="astrbot-usage-table-panel" aria-label="Recent usage records">
      <div className="astrbot-usage-bar-heading">
        <h4>Recent Calls</h4>
        <span>{records.length} records</span>
      </div>
      {records.length > 0 ? (
        <div className="astrbot-table-shell">
          <table className="astrbot-table astrbot-usage-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Request</th>
                <th>Route</th>
                <th>Tokens</th>
                <th>Cost</th>
              </tr>
            </thead>
            <tbody>
              {records.map(record => (
                <tr key={record.usageId}>
                  <td>{formatDateTime(record.recordedAt)}</td>
                  <td>
                    <div className="astrbot-usage-table-question">
                      <strong>{record.country}</strong>
                      <span>{record.question}</span>
                    </div>
                  </td>
                  <td>
                    <div className="astrbot-usage-route-cell">
                      <code>{record.selectedTool}</code>
                      <span>{record.pricingModel || record.model} · {record.status}</span>
                    </div>
                  </td>
                  <td>{formatTokens(record.totalTokens)}</td>
                  <td>{formatCost(record.estimatedCostCny, record.currency || currency)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="astrbot-table-empty">No usage records yet.</div>
      )}
    </section>
  );
}

function UsageSessionList({ sessions }: { sessions: AgentConversationSession[] }) {
  const totalTurns = sessions.reduce((sum, session) => sum + session.turnCount, 0);
  return (
    <section className="astrbot-usage-sessions" aria-label="Recent conversation sessions">
      <div className="astrbot-usage-bar-heading">
        <h4>Recent Sessions</h4>
        <span>{sessions.length} sessions · {totalTurns} turns</span>
      </div>
      {sessions.length > 0 ? (
        <div className="astrbot-usage-session-list">
          {sessions.slice(0, 12).map(session => (
            <div className="astrbot-usage-session-row" key={session.sessionId}>
              <div>
                <code>{session.sessionId}</code>
                <span>Last active {formatDateTime(session.lastActivityAt)}</span>
              </div>
              <strong>{session.turnCount} turns</strong>
            </div>
          ))}
        </div>
      ) : (
        <div className="astrbot-table-empty">No conversation sessions yet.</div>
      )}
    </section>
  );
}

function ExtensionsPanel() {
  return (
    <div className="astrbot-extension-list">
      {extensionRows.map(row => (
        <div className="astrbot-extension-row" key={row.name}>
          <div>
            <strong>{row.name}</strong>
            <span>{row.boundary}</span>
          </div>
          <span className="astrbot-status-pill">{row.status}</span>
        </div>
      ))}
    </div>
  );
}

function renderActivePanel(view: AstrBotView, status: AstrBotRuntimeStatus | null, developerMode: boolean) {
  const skills = status?.skills.items ?? [];
  if (view === "agent") {
    return <AgentPanel profile={status?.profile ?? null} skills={skills} developerMode={developerMode} />;
  }
  if (view === "profile") {
    return <ProfilePanel profile={status?.profile ?? null} />;
  }
  if (view === "mcp") {
    return <McpPanel tools={status?.mcp.tools ?? []} />;
  }
  if (view === "providers") {
    return <ProvidersPanel provider={status?.provider ?? null} retrieval={status?.retrieval ?? null} />;
  }
  if (view === "extensions") {
    return <ExtensionsPanel />;
  }
  if (view === "memory") {
    return (
      <AstrBotMemoryPanel
        skills={skills.map(s => ({ id: s.id, name: s.name }))}
        totalRuns={status?.memory?.totalRuns ?? 0}
      />
    );
  }
  if (view === "usage") {
    return <AstrBotUsagePanel />;
  }
  if (view === "eval") {
    return <AstrBotEvalPanel />;
  }
  return <AgentPanel profile={status?.profile ?? null} skills={skills} developerMode={developerMode} />;
}

export function AstrBotWorkbenchPage() {
  const location = useLocation();
  const activeView = resolveAstrBotView(location.pathname);
  const activeConfig = ASTRBOT_VIEW_CONFIGS[activeView];
  const nativeUrl = buildAstrBotNativeUrl(activeView);
  const developerMode = location.pathname.endsWith("/dev") || activeView !== "agent";
  const [showNativeConsole, setShowNativeConsole] = useState(false);
  const [runtimeStatus, setRuntimeStatus] = useState<AstrBotRuntimeStatus | null>(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const runtimeRows = buildRuntimeRows(runtimeStatus, statusLoading);

  async function refreshRuntimeStatus(signal?: AbortSignal): Promise<void> {
    setStatusLoading(true);
    setStatusError(null);
    try {
      setRuntimeStatus(await fetchAstrBotRuntimeStatus(signal));
    } catch (error) {
      if (isAbortError(error)) {
        return;
      }
      setStatusError(error instanceof Error ? error.message : String(error));
    } finally {
      if (!signal?.aborted) {
        setStatusLoading(false);
      }
    }
  }

  useEffect(() => {
    setShowNativeConsole(false);
  }, [activeView]);

  useEffect(() => {
    const controller = new AbortController();
    void refreshRuntimeStatus(controller.signal);
    return () => controller.abort();
  }, []);

  return (
    <main className={`astrbot-workbench ${developerMode ? "is-developer" : "is-user"} is-view-${activeView}`}>
      <header className="astrbot-header">
        <div className="astrbot-title-block">
          <span className="page-kicker">{developerMode ? "Hermes Steward / Developer" : "Automotive Market Agent"}</span>
          <h1>{developerMode ? "Developer AstrBot Mode" : "JATO AstrBot"}</h1>
        </div>
        {developerMode ? (
          <nav className="astrbot-tabs" aria-label="Developer agent views">
            {(Object.keys(ASTRBOT_VIEW_CONFIGS) as AstrBotView[]).map(view => renderTabLink(view, activeView))}
          </nav>
        ) : null}
        <div className="astrbot-header-actions">
          {developerMode ? (
            <button
              type="button"
              className="astrbot-refresh-button"
              onClick={() => void refreshRuntimeStatus()}
              disabled={statusLoading}
            >
              {statusLoading ? "Checking" : "Refresh"}
            </button>
          ) : null}
          {developerMode ? (
            <Link className="astrbot-open-link" to="/astrbot">
              User Mode
            </Link>
          ) : (
            <Link className="astrbot-native-toggle" to="/astrbot/dev">
              Developer Mode
            </Link>
          )}
          {developerMode ? (
            <a className="astrbot-open-link" href={nativeUrl} target="_blank" rel="noreferrer">
              Open Native
            </a>
          ) : null}
          {developerMode ? (
            <button
              type="button"
              className="astrbot-native-toggle"
              onClick={() => setShowNativeConsole(current => !current)}
            >
              {showNativeConsole ? "Hide Native" : "Embed Native"}
            </button>
          ) : null}
        </div>
      </header>

      {developerMode ? (
        <section className="astrbot-status-strip" aria-label="Agent runtime status">
          {runtimeRows.map(row => (
            <div className={`astrbot-status-item is-${row.tone}`} key={row.label}>
              <span>{row.label}</span>
              <strong>
                <i className="astrbot-live-dot" aria-hidden="true" />
                {row.value}
              </strong>
              <small>{row.meta}</small>
            </div>
          ))}
        </section>
      ) : null}
      {statusError && developerMode ? (
        <div className="astrbot-status-error" role="status">
          Runtime status degraded: {statusError}
        </div>
      ) : null}

      <section className={`astrbot-content-grid${developerMode ? " is-developer" : " is-user"}`}>
        <section className="astrbot-main-panel">
          <div className="astrbot-panel-heading">
            <span className="page-kicker">{activeConfig.kicker}</span>
            <h2>{activeConfig.title}</h2>
            {developerMode ? <p>{activeConfig.summary}</p> : null}
          </div>
          {renderActivePanel(activeView, runtimeStatus, developerMode)}
        </section>

        {developerMode ? (
        <aside className="astrbot-side-panel" aria-label="JATO agent boundaries">
          <div className="astrbot-side-heading">
            <span className="page-kicker">{developerMode ? "Boundary" : "Agent State"}</span>
            <h2>{developerMode ? "JATO Ownership" : "JATO Agent"}</h2>
          </div>
          <dl className="astrbot-boundary-list">
            <div>
              <dt>Identity</dt>
              <dd>OJEUR/JATO login gates access before agent state.</dd>
            </div>
            <div>
              <dt>Data</dt>
              <dd>Database, Parquet and news access stay behind JATO MCP tools.</dd>
            </div>
            <div>
              <dt>Provider</dt>
              <dd>DP V4 credentials come from the CountryCopilot environment source.</dd>
            </div>
            <div>
              <dt>MCP Endpoint</dt>
              <dd><code>{runtimeStatus?.mcp.url ?? ASTRBOT_MCP_ENDPOINT}</code></dd>
            </div>
          </dl>
        </aside>
        ) : null}
      </section>

      {developerMode && showNativeConsole ? (
        <section className="astrbot-native-panel">
          <div className="astrbot-native-heading">
            <div>
              <span className="page-kicker">Optional Native Console</span>
              <h2>Native AstrBot Console</h2>
              <p>Embedded native AstrBot is a fallback view. If it is unavailable, /astrbot evaluation and chat still use the JATO backend above.</p>
            </div>
            <button
              type="button"
              className="astrbot-native-toggle"
              onClick={() => setShowNativeConsole(current => !current)}
            >
              {showNativeConsole ? "Hide Native" : "Load Native"}
            </button>
          </div>
          {showNativeConsole ? (
            <AstrBotNativeFrame src={nativeUrl} title={`Native AstrBot ${activeConfig.label}`} />
          ) : null}
        </section>
      ) : null}
    </main>
  );
}
