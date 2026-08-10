import { apiUrl as sharedApiUrl } from "../../api/client";
import type {
  AgentCompareResponse,
  AgentConversationHistory,
  AgentConversationSessionsResponse,
  AgentMemoryStats,
  AgentRunListResponse,
  AgentRunRecord,
  AstrBotRuntimeStatus,
  AstrBotToolCallResponse,
} from "./astrbotConfig";

const localAstrBotBackendPorts: Record<string, string> = {
  "5173": "8000",
  "5174": "8001",
  "5176": "8002",
};

export function astrbotApiUrl(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const explicitApiBase = String(import.meta.env.VITE_API_BASE || "").trim();
  if (!explicitApiBase && normalizedPath.startsWith("/astrbot") && typeof window !== "undefined") {
    const host = window.location.hostname;
    const backendPort = localAstrBotBackendPorts[window.location.port];
    if ((host === "127.0.0.1" || host === "localhost") && backendPort) {
      return `${window.location.protocol}//${host}:${backendPort}/v1${normalizedPath}`;
    }
  }
  return sharedApiUrl(normalizedPath);
}

function readAstrBotAuthHeaders(): Headers {
  const headers = new Headers();
  const token = (
    localStorage.getItem("jato_auth_token")
    || import.meta.env.VITE_AUTH_TOKEN
    || ""
  ).trim();
  const userName = (
    localStorage.getItem("jato_user_name")
    || import.meta.env.VITE_USER_NAME
    || "anonymous"
  ).trim();

  if (token) {
    headers.set("X-Auth-Token", token);
  }
  headers.set("X-User-Name", userName || "anonymous");
  return headers;
}

async function readResponseError(response: Response): Promise<string> {
  const text = (await response.text()).trim();
  if (!text) {
    return response.statusText || "Request failed";
  }
  try {
    const payload = JSON.parse(text) as { detail?: unknown; message?: unknown };
    if (typeof payload.detail === "string" && payload.detail.trim()) {
      return payload.detail;
    }
    if (typeof payload.message === "string" && payload.message.trim()) {
      return payload.message;
    }
  } catch {
    return text;
  }
  return text;
}

export async function fetchAstrBotRuntimeStatus(
  signal?: AbortSignal,
): Promise<AstrBotRuntimeStatus> {
  const response = await fetch(astrbotApiUrl("/astrbot/tools/status"), {
    headers: readAstrBotAuthHeaders(),
    signal,
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AstrBotRuntimeStatus;
}

export async function callAstrBotTool(
  name: string,
  argumentsPayload: Record<string, unknown>,
): Promise<AstrBotToolCallResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/tools/call"), {
    method: "POST",
    headers,
    body: JSON.stringify({ name, arguments: argumentsPayload }),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AstrBotToolCallResponse;
}

// ── Memory API ──

export async function fetchAgentRuns(params?: {
  skillId?: string;
  country?: string;
  mode?: string;
  selectedTool?: string;
  limit?: number;
  offset?: number;
}): Promise<AgentRunListResponse> {
  const searchParams = new URLSearchParams();
  if (params?.skillId) searchParams.set("skill_id", params.skillId);
  if (params?.country) searchParams.set("country", params.country);
  if (params?.mode) searchParams.set("mode", params.mode);
  if (params?.selectedTool) searchParams.set("selected_tool", params.selectedTool);
  if (params?.limit !== undefined) searchParams.set("limit", String(params.limit));
  if (params?.offset !== undefined) searchParams.set("offset", String(params.offset));
  const qs = searchParams.toString();
  const response = await fetch(astrbotApiUrl(`/astrbot/memory/runs${qs ? `?${qs}` : ""}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentRunListResponse;
}

export async function fetchAgentRun(runId: string): Promise<AgentRunRecord> {
  const response = await fetch(astrbotApiUrl(`/astrbot/memory/runs/${encodeURIComponent(runId)}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentRunRecord;
}

export async function deleteAgentRun(runId: string): Promise<{ runId: string; deleted: boolean }> {
  const response = await fetch(astrbotApiUrl(`/astrbot/memory/runs/${encodeURIComponent(runId)}`), {
    method: "DELETE",
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as { runId: string; deleted: boolean };
}

export async function fetchAgentMemoryStats(): Promise<AgentMemoryStats> {
  const response = await fetch(astrbotApiUrl("/astrbot/memory/stats"), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentMemoryStats;
}

export async function compareAgentRuns(runIds: string[]): Promise<AgentCompareResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/memory/compare"), {
    method: "POST",
    headers,
    body: JSON.stringify({ run_ids: runIds }),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentCompareResponse;
}

export async function fetchAgentConversationHistory(
  sessionId: string,
  limit = 20,
): Promise<AgentConversationHistory> {
  const response = await fetch(
    astrbotApiUrl(`/astrbot/agent/sessions/${encodeURIComponent(sessionId)}?limit=${encodeURIComponent(String(limit))}`),
    { headers: readAstrBotAuthHeaders() },
  );
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentConversationHistory;
}

export async function fetchAgentConversationSessions(
  limit = 20,
): Promise<AgentConversationSessionsResponse> {
  const response = await fetch(astrbotApiUrl(`/astrbot/agent/sessions?limit=${encodeURIComponent(String(limit))}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentConversationSessionsResponse;
}

// ── Agent Usage ──

export interface AgentUsageRecord {
  usageId: string;
  recordedAt: string;
  country: string;
  question: string;
  provider: string;
  model: string;
  pricingModel: string;
  status: string;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  estimatedCostCny: number;
  currency: string;
  selectedTool: string;
  toolsUsed: string[];
  retrievalPaths: string[];
}

export interface AgentUsageSummary {
  totalRuns: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  totalTokens: number;
  totalCostCny: number;
  avgCostPerRunCny: number;
  currency: string;
  byTool: Record<string, { runs: number; tokens: number; costCny: number }>;
  byModel: Record<string, { runs: number; tokens: number; costCny: number }>;
  byStatus: Record<string, number>;
  recent: AgentUsageRecord[];
}

export async function fetchAgentUsageSummary(limit = 20): Promise<AgentUsageSummary> {
  const response = await fetch(astrbotApiUrl(`/astrbot/usage/summary?limit=${encodeURIComponent(String(limit))}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${await readResponseError(response)}`);
  }
  return (await response.json()) as AgentUsageSummary;
}

// ── Phase 7: Eval API ──

import type {
  EvalBusinessJudgeExistingResponse,
  EvalBusinessQuestionsResponse,
  EvalBusinessReportResponse,
  EvalBusinessRunResponse,
  EvalCategoryRunResponse,
  EvalCodexReviewNotesResponse,
  EvalCodexReviewScoringArtifactsResponse,
  EvalFullRunResponse,
  EvalJudgePreflightResponse,
  EvalQuestionsResponse,
  EvalResult,
  EvalResultsResponse,
  EvalSideBySideCategoryRunResponse,
  EvalSideBySideRecord,
  EvalSideBySideResultsResponse,
  EvalSummary,
} from "./astrbotConfig";

export async function fetchEvalQuestions(): Promise<EvalQuestionsResponse> {
  const response = await fetch(astrbotApiUrl("/astrbot/eval/questions"), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalQuestionsResponse;
}

export async function fetchEvalSummary(): Promise<EvalSummary> {
  const response = await fetch(astrbotApiUrl("/astrbot/eval/summary"), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalSummary;
}

export async function fetchEvalResults(params?: {
  category?: string;
  limit?: number;
  offset?: number;
}): Promise<EvalResultsResponse> {
  const sp = new URLSearchParams();
  if (params?.category) sp.set("category", params.category);
  if (params?.limit !== undefined) sp.set("limit", String(params.limit));
  if (params?.offset !== undefined) sp.set("offset", String(params.offset));
  const qs = sp.toString();
  const response = await fetch(astrbotApiUrl(`/astrbot/eval/results${qs ? `?${qs}` : ""}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalResultsResponse;
}

export async function runEvalQuestion(questionId: string): Promise<EvalResult> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/run/question"), {
    method: "POST",
    headers,
    body: JSON.stringify({ question_id: questionId }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalResult;
}

export async function runEvalCategory(category: string, limit = 10): Promise<EvalCategoryRunResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/run/category"), {
    method: "POST",
    headers,
    body: JSON.stringify({ category, limit }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalCategoryRunResponse;
}

export async function runEvalFull(questionsPerCategory = 5): Promise<EvalFullRunResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/run/full"), {
    method: "POST",
    headers,
    body: JSON.stringify({ questions_per_category: questionsPerCategory }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalFullRunResponse;
}

export async function fetchEvalSideBySideResults(params?: {
  category?: string;
  limit?: number;
  offset?: number;
  latestPerQuestion?: boolean;
}): Promise<EvalSideBySideResultsResponse> {
  const sp = new URLSearchParams();
  if (params?.category) sp.set("category", params.category);
  if (params?.limit !== undefined) sp.set("limit", String(params.limit));
  if (params?.offset !== undefined) sp.set("offset", String(params.offset));
  if (params?.latestPerQuestion !== undefined) sp.set("latestPerQuestion", String(params.latestPerQuestion));
  const qs = sp.toString();
  const response = await fetch(astrbotApiUrl(`/astrbot/eval/side-by-side/results${qs ? `?${qs}` : ""}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalSideBySideResultsResponse;
}

export async function fetchBusinessValidationQuestions(): Promise<EvalBusinessQuestionsResponse> {
  const response = await fetch(astrbotApiUrl("/astrbot/eval/business/questions"), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalBusinessQuestionsResponse;
}

export async function fetchBusinessValidationReport(params?: {
  category?: string;
  limit?: number;
}): Promise<EvalBusinessReportResponse> {
  const sp = new URLSearchParams();
  if (params?.category) sp.set("category", params.category);
  if (params?.limit !== undefined) sp.set("limit", String(params.limit));
  const qs = sp.toString();
  const response = await fetch(astrbotApiUrl(`/astrbot/eval/business/report${qs ? `?${qs}` : ""}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalBusinessReportResponse;
}

export async function fetchCodexReviewNotes(limit = 100): Promise<EvalCodexReviewNotesResponse> {
  const response = await fetch(astrbotApiUrl(`/astrbot/eval/codex-review/notes?limit=${encodeURIComponent(String(limit))}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalCodexReviewNotesResponse;
}

export async function fetchLatestCodexReviewScoringArtifacts(): Promise<EvalCodexReviewScoringArtifactsResponse> {
  const response = await fetch(astrbotApiUrl("/astrbot/eval/codex-review/scoring-artifacts/latest"), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalCodexReviewScoringArtifactsResponse;
}

export async function fetchEvalJudgePreflight(liveCheck = false): Promise<EvalJudgePreflightResponse> {
  const response = await fetch(astrbotApiUrl(`/astrbot/eval/judge/preflight?liveCheck=${encodeURIComponent(String(liveCheck))}`), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalJudgePreflightResponse;
}

export async function runEvalSideBySideQuestion(questionId: string): Promise<EvalSideBySideRecord> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/side-by-side/run/question"), {
    method: "POST",
    headers,
    body: JSON.stringify({ question_id: questionId }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalSideBySideRecord;
}

export async function runEvalSideBySideCategory(
  category: string,
  limit = 1,
): Promise<EvalSideBySideCategoryRunResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/side-by-side/run/category"), {
    method: "POST",
    headers,
    body: JSON.stringify({ category, limit }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalSideBySideCategoryRunResponse;
}

export async function runBusinessValidationQuestion(questionId: string): Promise<EvalSideBySideRecord> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/business/run/question"), {
    method: "POST",
    headers,
    body: JSON.stringify({ question_id: questionId }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalSideBySideRecord;
}

export async function runBusinessValidationCategory(category: string, limit = 5): Promise<EvalBusinessRunResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/business/run/category"), {
    method: "POST",
    headers,
    body: JSON.stringify({ category, limit }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalBusinessRunResponse;
}

export async function runBusinessValidationAll(limit = 30): Promise<EvalBusinessRunResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/business/run/all"), {
    method: "POST",
    headers,
    body: JSON.stringify({ limit }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalBusinessRunResponse;
}

export async function judgeExistingBusinessValidationRecords(params?: {
  category?: string;
  limit?: number;
  latestPerQuestion?: boolean;
  scoreReadyOnly?: boolean;
}): Promise<EvalBusinessJudgeExistingResponse> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(astrbotApiUrl("/astrbot/eval/business/judge-existing"), {
    method: "POST",
    headers,
    body: JSON.stringify({
      category: params?.category,
      limit: params?.limit ?? 30,
      latestPerQuestion: params?.latestPerQuestion ?? true,
      scoreReadyOnly: params?.scoreReadyOnly ?? false,
    }),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalBusinessJudgeExistingResponse;
}

export interface EvalSideBySideHumanScorePayload {
  status?: string;
  source?: string;
  judgeProvider?: {
    source?: string;
    pathId?: string;
    label?: string;
    provider?: string;
    model?: string;
    apiBase?: string;
    keySource?: string;
  };
  winner?: string;
  notes?: string;
  dimensions?: string[];
  astrbotTotal?: number;
  countryCopilotTotal?: number;
  copilotTotal?: number;
  astrbotScores?: Record<string, number>;
  countryCopilotScores?: Record<string, number>;
  copilotScores?: Record<string, number>;
  failureTags?: string[];
}

export async function updateEvalSideBySideHumanScore(
  comparisonId: string,
  payload: EvalSideBySideHumanScorePayload,
): Promise<EvalSideBySideRecord> {
  const headers = readAstrBotAuthHeaders();
  headers.set("Content-Type", "application/json");
  const response = await fetch(
    astrbotApiUrl(`/astrbot/eval/side-by-side/results/${encodeURIComponent(comparisonId)}/human-score`),
    {
      method: "PATCH",
      headers,
      body: JSON.stringify(payload),
    },
  );
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalSideBySideRecord;
}

// ── Eval Usage ──

export interface EvalUsageSummary {
  totalRuns: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  totalTokens: number;
  totalCostCny: number;
  avgCostPerRunCny: number;
  currency: string;
  byCategory: Record<string, { runs: number; tokens: number; costCny: number }>;
  byModel: Record<string, { runs: number; tokens: number; costCny: number }>;
}

export async function fetchEvalUsage(): Promise<EvalUsageSummary> {
  const response = await fetch(astrbotApiUrl("/astrbot/eval/usage"), {
    headers: readAstrBotAuthHeaders(),
  });
  if (!response.ok) throw new Error(`${response.status} ${await readResponseError(response)}`);
  return (await response.json()) as EvalUsageSummary;
}
