import type {
  AdvancedChartResponse,
  AnalysisQuery,
  CocMatchJob,
  ConfigImportBatch,
  ConfigVariant,
  CustomerInsightDeckResponse,
  CustomerInsightMode,
  CrudListResponse,
  CrudItem,
  CurrentPrice,
  DataFreshnessItem,
  MsrpObservationRecord,
  PriceHistoryEntry,
  DetailResponse,
  GroupedTimeSeriesResponse,
  JatoMonthlyUpdateCleanupResult,
  JatoMonthlyUpdateArtifacts,
  JatoMonthlyUpdateJob,
  JatoMonthlyUpdatePlan,
  JatoMonthlyUpdateConflictSample,
  JatoMonthlyUpdateCountryCoverageSummary,
  JatoMonthlyUpdateCountryFreshnessSummary,
  JatoMonthlyUpdateCountryMonthlySalesRow,
  JatoMonthlyUpdateCountryMonthlySalesSummary,
  JatoMonthlyUpdateBaselinePromotionResult,
  JatoMonthlyUpdatePublication,
  JatoMonthlyUpdateOverlapChangeSummary,
  JatoMonthlyUpdateMaintenanceStatus,
  JatoMonthlyUpdateRawCompareSummary,
  JatoMonthlyUpdateReviewBundle,
  JatoMonthlyUpdateReviewFinding,
  JatoMonthlyUpdateRefreshSummary,
  JatoMonthlyUpdateSmartMergeSummary,
  JatoMonthlyUpdateSummaries,
  JatoMonthlyUpdateStorageMetric,
  JatoMonthlyUpdateUploadProgress,
  JatoMonthlyUpdateUploadSession,
  JatoMonthlyUpdateUpload,
  MarketScanDeckRequest,
  MarketScanDeckResponse,
  RankingTrendResponse,
  ModelVersionsResponse,
  OverviewResponse,
  PositioningPricingDeckRequest,
  PositioningPricingDeckResponse,
  PositioningMapResponse,
  ReviewCase,
  ReviewCandidateMatch,
  ReviewCaseDetail,
  ReviewBacklogOpportunity,
  ReviewDecision,
  ReviewScopeCountrySummary,
  ReviewWorkbench,
  RvFinanceResponse,
  RvFinanceVehicle,
  VersionComparisonDeckRequest,
  VersionComparisonDeckResponse,
} from "../types";
import type {
  ConfigProject,
  DataManagementAirflowActionResponse,
  DataManagementAirflowStatus,
  DataManagementOverviewResponse,
  DataManagementVocOverviewResponse,
  DataManagementVocSyncResponse,
  MatchOverride,
  MsrpSource,
} from "../types/dataManagement";
import type {
  HermesActivityResponse,
  HermesArchResponse,
  HermesChatResponse,
  HermesChatRequest,
  HermesChatSession,
  HermesChatSessionDetail,
  HermesCommand,
  HermesCommandExecuteRequest,
  HermesCommandExecuteResponse,
  HermesCostResponse,
  HermesDailySummaryResponse,
  HermesEvidenceLedgerResponse,
  HermesFeatureKanbanResponse,
  HermesFullDesignDocumentResponse,
  HermesGap,
  HermesMermaidBlock,
  HermesOverviewResponse,
  HermesPipelineHealthResponse,
  HermesDeployStatusResponse,
  HermesSentinelMailboxStatus,
  HermesSentinelNotification,
  HermesSentinelStatusResponse,
  HermesMsrpCountryProgressResponse,
  HermesMsrpDryrunHistoryResponse,
  HermesSourceQualityResponse,
  HermesToolchainResponse,
} from "../types/hermes";
import type {
  CountryChatDeckResponse,
  CountryChatMetadataResponse,
  CountryChatNewsOpsStatus,
  CountryChatNewsRefreshResponse,
  CountryChatResponse,
  CountryChatTurn,
} from "../types/countryChat";
import type { FilterOptionsPayload } from "../utils/filterOptions";
import {
  buildMonthlyUpdateUploadResumeKey,
  getMonthlyUpdateRetryDelayMs,
} from "../utils/jatoMonthlyUpdate";

const API_BASE = import.meta.env.VITE_API_BASE ?? "/v1";
const MONTHLY_UPDATE_RESUME_PROBE_BYTES = 1024 * 1024;
const MONTHLY_UPDATE_UPLOAD_SESSION_STORAGE_PREFIX = "jato_monthly_update_upload_session:";
const MONTHLY_UPDATE_UPLOAD_MAX_ATTEMPTS = 4;

function getAuthHeaders(): Record<string, string> {
  const token = (
    localStorage.getItem("jato_auth_token")
    || import.meta.env.VITE_AUTH_TOKEN
    || ""
  ).trim();
  const user = (
    localStorage.getItem("jato_user_name")
    || import.meta.env.VITE_USER_NAME
    || "anonymous"
  ).trim();

  return {
    ...(token ? { "X-Auth-Token": token } : {}),
    "X-User-Name": user || "anonymous"
  };
}

function buildHeaders(
  init?: RequestInit,
  options?: { includeJsonContentType?: boolean }
): Headers {
  const headers = new Headers(init?.headers);
  const authHeaders = getAuthHeaders();
  Object.entries(authHeaders).forEach(([key, value]) => {
    headers.set(key, value);
  });
  if (
    options?.includeJsonContentType
    && !(init?.body instanceof FormData)
    && !headers.has("Content-Type")
  ) {
    headers.set("Content-Type", "application/json");
  }
  return headers;
}

async function readErrorMessage(response: Response): Promise<string> {
  const text = (await response.text()).trim();
  if (!text) {
    return response.statusText || "Request failed";
  }

  try {
    const parsed = JSON.parse(text) as Record<string, unknown>;
    if (typeof parsed.detail === "string" && parsed.detail.trim()) {
      return parsed.detail;
    }
    if (typeof parsed.detail === "object" && parsed.detail !== null) {
      return JSON.stringify(parsed.detail);
    }
    if (typeof parsed.message === "string" && parsed.message.trim()) {
      return parsed.message;
    }
  } catch {
    // Fall back to the raw response body when it is not JSON.
  }

  return text;
}

/* ── in-flight request deduplication ────────────────── */
const inflightRequests = new Map<string, Promise<unknown>>();

function dedupeKey(path: string, init?: RequestInit): string {
  const method = (init?.method ?? "GET").toUpperCase();
  const body = init?.body ? String(init.body) : "";
  return `${method}:${path}:${body}`;
}

function isAbortLikeError(error: unknown): boolean {
  if (error instanceof DOMException) {
    return error.name === "AbortError";
  }

  if (error instanceof Error) {
    if (error.name === "AbortError") {
      return true;
    }
    return /\babort(?:ed)?\b/i.test(error.message);
  }

  if (typeof error === "object" && error !== null) {
    const name = "name" in error ? String(error.name ?? "") : "";
    const message = "message" in error ? String(error.message ?? "") : "";
    return name === "AbortError" || /\babort(?:ed)?\b/i.test(message);
  }

  return false;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const shouldDedupe = !(init?.body instanceof FormData);
  const key = shouldDedupe ? dedupeKey(path, init) : null;
  const inflight = key
    ? inflightRequests.get(key) as Promise<T> | undefined
    : undefined;
  if (inflight) return inflight;

  const promise = (async () => {
    let response: Response;
    try {
      response = await fetch(`${API_BASE}${path}`, {
        headers: buildHeaders(init, { includeJsonContentType: true }),
        ...init
      });
    } catch (error) {
      if (isAbortLikeError(error)) {
        throw error;
      }
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`网络请求失败：${path} (${message})`);
    }
    if (!response.ok) {
      const message = await readErrorMessage(response);
      throw new Error(`${response.status} ${message}`);
    }
    return (await response.json()) as T;
  })();

  if (key) {
    inflightRequests.set(key, promise);
    promise.then(
      () => {
        inflightRequests.delete(key);
      },
      () => {
        inflightRequests.delete(key);
      },
    );
  }
  return promise;
}

async function requestBlob(path: string, init?: RequestInit): Promise<Blob> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      headers: buildHeaders(init),
      ...init
    });
  } catch (error) {
    if (isAbortLikeError(error)) {
      throw error;
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`网络请求失败：${path} (${message})`);
  }
  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`${response.status} ${message}`);
  }
  return response.blob();
}

function toHex(buffer: ArrayBuffer): string {
  return Array.from(new Uint8Array(buffer))
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256ForBlob(blob: Blob): Promise<string> {
  if (!globalThis.crypto?.subtle) {
    throw new Error("当前浏览器环境不支持 SHA-256 校验，请使用 HTTPS 或 localhost 访问。");
  }
  const buffer = await blob.arrayBuffer();
  return toHex(await crypto.subtle.digest("SHA-256", buffer));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function getMonthlyUpdateUploadSessionStorageKey(resumeKey: string): string {
  return `${MONTHLY_UPDATE_UPLOAD_SESSION_STORAGE_PREFIX}${resumeKey}`;
}

function readStoredMonthlyUpdateUploadId(resumeKey: string): string | null {
  try {
    const value = localStorage.getItem(getMonthlyUpdateUploadSessionStorageKey(resumeKey));
    return value && value.trim() ? value : null;
  } catch {
    return null;
  }
}

function writeStoredMonthlyUpdateUploadId(resumeKey: string, uploadId: string): void {
  try {
    localStorage.setItem(getMonthlyUpdateUploadSessionStorageKey(resumeKey), uploadId);
  } catch {
    // Ignore storage quota / privacy mode failures; upload can still proceed without resume persistence.
  }
}

function clearStoredMonthlyUpdateUploadId(resumeKey: string): void {
  try {
    localStorage.removeItem(getMonthlyUpdateUploadSessionStorageKey(resumeKey));
  } catch {
    // Ignore storage failures when clearing local resume state.
  }
}

function mapReviewCase(raw: Record<string, unknown>): ReviewCase {
  return {
    id: String(raw.reviewCaseId ?? raw.id ?? ""),
    observationId: String(raw.observationId ?? ""),
    reviewStatus: String(raw.reviewStatus ?? ""),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    sourceCode: raw.sourceCode === undefined ? undefined : String(raw.sourceCode),
    sourceRegistryUrl: raw.sourceRegistryUrl === undefined ? undefined : String(raw.sourceRegistryUrl),
    sourceType: raw.sourceType === undefined ? undefined : String(raw.sourceType),
    extractorName: raw.extractorName === undefined ? undefined : String(raw.extractorName),
    extractorVersion: raw.extractorVersion === undefined ? undefined : String(raw.extractorVersion),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: raw.jatoPowertrain === undefined || raw.jatoPowertrain === null ? null : String(raw.jatoPowertrain),
    officialModel: String(raw.officialModel ?? ""),
    officialTrim: String(raw.officialTrim ?? ""),
    officialEdition: raw.officialEdition === undefined || raw.officialEdition === null ? null : String(raw.officialEdition),
    officialPowertrain: raw.officialPowertrain === undefined || raw.officialPowertrain === null ? null : String(raw.officialPowertrain),
    msrpValue: raw.msrpValue === undefined ? undefined : Number(raw.msrpValue),
    currency: raw.currency === undefined ? undefined : String(raw.currency),
    sourceMsrpValue: raw.sourceMsrpValue === undefined ? undefined : Number(raw.sourceMsrpValue),
    sourceCurrency: raw.sourceCurrency === undefined ? undefined : String(raw.sourceCurrency),
    fxRateToEur: raw.fxRateToEur === undefined ? undefined : Number(raw.fxRateToEur),
    fxRateAsOfDate: raw.fxRateAsOfDate === undefined ? undefined : String(raw.fxRateAsOfDate),
    fxSource: raw.fxSource === undefined ? undefined : String(raw.fxSource),
    priceLabel: raw.priceLabel === undefined ? undefined : String(raw.priceLabel),
    observedAtUtc: raw.observedAtUtc === undefined ? undefined : String(raw.observedAtUtc),
    sourceUrl: raw.sourceUrl === undefined ? undefined : String(raw.sourceUrl),
    sourceSnapshotPath: raw.sourceSnapshotPath === undefined ? null : (raw.sourceSnapshotPath as string | null),
    matchConfidence: Number(raw.matchConfidence ?? 0),
    matchReason: raw.matchReason === undefined || raw.matchReason === null
      ? null
      : (raw.matchReason as Record<string, unknown>),
    candidateMatches: Array.isArray(raw.candidateMatches)
      ? raw.candidateMatches.map((item) => mapReviewCandidateMatch(item as Record<string, unknown>))
      : null,
    currentAssignee: raw.currentAssignee === undefined || raw.currentAssignee === null ? null : String(raw.currentAssignee),
    createdAt: String(raw.createdAtUtc ?? raw.createdAt ?? ""),
    updatedAt: String(raw.updatedAtUtc ?? raw.updatedAt ?? ""),
  };
}

function mapReviewCandidateMatch(raw: Record<string, unknown>): ReviewCandidateMatch {
  return {
    currentPriceId: raw.currentPriceId === undefined || raw.currentPriceId === null ? undefined : String(raw.currentPriceId),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: raw.jatoPowertrain === undefined || raw.jatoPowertrain === null || String(raw.jatoPowertrain).trim() === ""
      ? null
      : String(raw.jatoPowertrain),
    officialModel: String(raw.officialModel ?? ""),
    officialTrim: String(raw.officialTrim ?? ""),
    officialEdition: raw.officialEdition === undefined || raw.officialEdition === null ? null : String(raw.officialEdition),
    officialPowertrain: raw.officialPowertrain === undefined || raw.officialPowertrain === null ? null : String(raw.officialPowertrain),
    currentMsrpValue: raw.currentMsrpValue === undefined ? undefined : Number(raw.currentMsrpValue),
    currency: raw.currency === undefined ? undefined : String(raw.currency),
    score: Number(raw.score ?? 0),
    reason: raw.reason === undefined || raw.reason === null ? null : (raw.reason as Record<string, unknown>),
  };
}

function mapMsrpObservation(raw: Record<string, unknown>): MsrpObservationRecord {
  return {
    observationId: String(raw.observationId ?? ""),
    scrapeBatchId: String(raw.scrapeBatchId ?? ""),
    sourceId: String(raw.sourceId ?? ""),
    sourceCode: raw.sourceCode === undefined ? undefined : String(raw.sourceCode),
    sourceType: raw.sourceType === undefined ? undefined : String(raw.sourceType),
    extractorName: raw.extractorName === undefined ? undefined : String(raw.extractorName),
    extractorVersion: raw.extractorVersion === undefined ? undefined : String(raw.extractorVersion),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: raw.jatoPowertrain === undefined || raw.jatoPowertrain === null || String(raw.jatoPowertrain).trim() === ""
      ? null
      : String(raw.jatoPowertrain),
    officialModel: String(raw.officialModel ?? ""),
    officialTrim: String(raw.officialTrim ?? ""),
    officialEdition: raw.officialEdition === undefined || raw.officialEdition === null ? null : String(raw.officialEdition),
    officialPowertrain: raw.officialPowertrain === undefined || raw.officialPowertrain === null ? null : String(raw.officialPowertrain),
    msrpValue: Number(raw.msrpValue ?? 0),
    currency: String(raw.currency ?? ""),
    sourceMsrpValue: Number(raw.sourceMsrpValue ?? 0),
    sourceCurrency: String(raw.sourceCurrency ?? ""),
    fxRateToEur: Number(raw.fxRateToEur ?? 0),
    fxRateAsOfDate: String(raw.fxRateAsOfDate ?? ""),
    fxSource: String(raw.fxSource ?? ""),
    taxIncluded: Boolean(raw.taxIncluded),
    priceLabel: String(raw.priceLabel ?? ""),
    availabilityText: raw.availabilityText === undefined || raw.availabilityText === null ? null : String(raw.availabilityText),
    observedAtUtc: String(raw.observedAtUtc ?? ""),
    sourceUrl: String(raw.sourceUrl ?? ""),
    sourceSnapshotPath: raw.sourceSnapshotPath === undefined ? null : (raw.sourceSnapshotPath as string | null),
    sourcePayloadHash: raw.sourcePayloadHash === undefined || raw.sourcePayloadHash === null ? null : String(raw.sourcePayloadHash),
    extractionVersion: String(raw.extractionVersion ?? ""),
    matchConfidence: Number(raw.matchConfidence ?? 0),
    matchStatus: String(raw.matchStatus ?? ""),
    matchReason: raw.matchReason === undefined || raw.matchReason === null ? null : (raw.matchReason as Record<string, unknown>),
    sourceContext: raw.sourceContext === undefined || raw.sourceContext === null ? null : (raw.sourceContext as Record<string, unknown>),
    createdAtUtc: String(raw.createdAtUtc ?? ""),
    updatedAtUtc: String(raw.updatedAtUtc ?? ""),
  };
}

function mapReviewDecision(raw: Record<string, unknown>): ReviewDecision {
  return {
    id: String(raw.reviewDecisionId ?? raw.id ?? ""),
    reviewCaseId: String(raw.reviewCaseId ?? ""),
    decision: String(raw.decision ?? ""),
    decidedOfficialModel: raw.decidedOfficialModel === undefined || raw.decidedOfficialModel === null ? null : String(raw.decidedOfficialModel),
    decidedOfficialTrim: raw.decidedOfficialTrim === undefined || raw.decidedOfficialTrim === null ? null : String(raw.decidedOfficialTrim),
    note: raw.note === undefined || raw.note === null ? null : String(raw.note),
    decidedBy: String(raw.decidedBy ?? ""),
    decidedAt: String(raw.decidedAtUtc ?? raw.decidedAt ?? ""),
  };
}

function mapReviewCaseDetail(raw: Record<string, unknown>): ReviewCaseDetail {
  const reviewCaseRaw = (raw.reviewCase as Record<string, unknown> | undefined) ?? raw;
  return {
    ...mapReviewCase(reviewCaseRaw),
    observation: raw.observation && typeof raw.observation === "object"
      ? mapMsrpObservation(raw.observation as Record<string, unknown>)
      : null,
    decisions: Array.isArray(raw.decisions)
      ? raw.decisions.map((item) => mapReviewDecision(item as Record<string, unknown>))
      : [],
    currentPrice: raw.currentPrice && typeof raw.currentPrice === "object"
      ? mapCurrentPrice(raw.currentPrice as Record<string, unknown>)
      : null,
  };
}

function mapCurrentPrice(raw: Record<string, unknown>): CurrentPrice {
  return {
    id: String(raw.currentPriceId ?? raw.id ?? ""),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    sourceCode: raw.sourceCode === undefined ? undefined : String(raw.sourceCode),
    sourceType: raw.sourceType === undefined ? undefined : String(raw.sourceType),
    extractorName: raw.extractorName === undefined ? undefined : String(raw.extractorName),
    extractorVersion: raw.extractorVersion === undefined ? undefined : String(raw.extractorVersion),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: raw.jatoPowertrain === undefined || raw.jatoPowertrain === null ? null : String(raw.jatoPowertrain),
    officialModel: String(raw.officialModel ?? ""),
    officialTrim: String(raw.officialTrim ?? ""),
    officialEdition: raw.officialEdition === undefined || raw.officialEdition === null ? null : String(raw.officialEdition),
    officialPowertrain: raw.officialPowertrain === undefined || raw.officialPowertrain === null ? null : String(raw.officialPowertrain),
    effectiveObservationId: String(raw.effectiveObservationId ?? ""),
    currentMsrpValue: Number(raw.currentMsrpValue ?? 0),
    currency: String(raw.currency ?? ""),
    sourceMsrpValue: raw.sourceMsrpValue === undefined ? undefined : Number(raw.sourceMsrpValue),
    sourceCurrency: raw.sourceCurrency === undefined ? undefined : String(raw.sourceCurrency),
    fxRateToEur: raw.fxRateToEur === undefined ? undefined : Number(raw.fxRateToEur),
    fxRateAsOfDate: raw.fxRateAsOfDate === undefined ? undefined : String(raw.fxRateAsOfDate),
    fxSource: raw.fxSource === undefined ? undefined : String(raw.fxSource),
    taxIncluded: Boolean(raw.taxIncluded),
    matchConfidence: Number(raw.matchConfidence ?? 0),
    matchStatus: String(raw.matchStatus ?? ""),
    sourceUrl: String(raw.sourceUrl ?? ""),
    sourceSnapshotPath: raw.sourceSnapshotPath === undefined ? null : (raw.sourceSnapshotPath as string | null),
    lastPriceChangeAtUtc: raw.lastPriceChangeAtUtc === undefined || raw.lastPriceChangeAtUtc === null ? null : String(raw.lastPriceChangeAtUtc),
    updatedAtUtc: String(raw.updatedAtUtc ?? ""),
  };
}

function mapPriceHistory(raw: Record<string, unknown>): PriceHistoryEntry {
  return {
    id: String(raw.priceHistoryId ?? raw.id ?? ""),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: raw.jatoPowertrain === undefined || raw.jatoPowertrain === null || String(raw.jatoPowertrain).trim() === ""
      ? null
      : String(raw.jatoPowertrain),
    msrpValue: Number(raw.msrpValue ?? 0),
    currency: String(raw.currency ?? ""),
    sourceMsrpValue: Number(raw.sourceMsrpValue ?? 0),
    sourceCurrency: String(raw.sourceCurrency ?? ""),
    validFromUtc: String(raw.validFromUtc ?? ""),
    validToUtc: raw.validToUtc === undefined || raw.validToUtc === null
      ? null
      : String(raw.validToUtc),
    lastConfirmedAtUtc: String(raw.lastConfirmedAtUtc ?? raw.validFromUtc ?? ""),
    startedByObservationId: String(raw.startedByObservationId ?? ""),
    endedByObservationId: raw.endedByObservationId === undefined || raw.endedByObservationId === null
      ? null
      : String(raw.endedByObservationId),
    lastConfirmedByObservationId: String(raw.lastConfirmedByObservationId ?? raw.startedByObservationId ?? ""),
    createdAtUtc: String(raw.createdAtUtc ?? ""),
  };
}

function mapConfigProject(raw: Record<string, unknown>): ConfigProject {
  return {
    id: String(raw.projectId ?? raw.id ?? ""),
    projectCode: String(raw.projectCode ?? ""),
    brand: String(raw.brand ?? ""),
    model: String(raw.model ?? ""),
    marketCountry: String(raw.marketCountry ?? ""),
    displayName: String(raw.displayName ?? ""),
    status: String(raw.status ?? ""),
    createdAt: String(raw.createdAtUtc ?? raw.createdAt ?? ""),
    updatedAt: String(raw.updatedAtUtc ?? raw.updatedAt ?? ""),
  };
}

function mapMsrpSource(raw: Record<string, unknown>): MsrpSource {
  return {
    id: String(raw.sourceId ?? raw.id ?? ""),
    sourceCode: String(raw.sourceCode ?? ""),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    sourceUrl: String(raw.sourceUrl ?? ""),
    sourceType: String(raw.sourceType ?? ""),
    tier: Number(raw.tier ?? 3),
    extractorName: String(raw.extractorName ?? ""),
    extractorVersion: String(raw.extractorVersion ?? ""),
    priceSemantics: String(raw.priceSemantics ?? ""),
    requiresLocation: Boolean(raw.requiresLocation),
    enabled: Boolean(raw.enabled),
    notes: raw.notes === undefined || raw.notes === null ? null : String(raw.notes),
    createdAt: String(raw.createdAtUtc ?? raw.createdAt ?? ""),
    updatedAt: String(raw.updatedAtUtc ?? raw.updatedAt ?? ""),
  };
}

function mapMatchOverride(raw: Record<string, unknown>): MatchOverride {
  return {
    id: String(raw.overrideId ?? raw.id ?? ""),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: raw.jatoPowertrain === undefined || raw.jatoPowertrain === null || String(raw.jatoPowertrain).trim() === ""
      ? null
      : String(raw.jatoPowertrain),
    officialModel: String(raw.officialModel ?? ""),
    officialTrim: String(raw.officialTrim ?? ""),
    validFromDate: String(raw.validFromDate ?? ""),
    validToDate: raw.validToDate === undefined || raw.validToDate === null ? null : String(raw.validToDate),
    overrideReason: String(raw.overrideReason ?? ""),
    createdBy: String(raw.createdBy ?? ""),
    createdAt: String(raw.createdAtUtc ?? raw.createdAt ?? ""),
    updatedAt: String(raw.updatedAtUtc ?? raw.updatedAt ?? ""),
  };
}

function mapJatoMonthlyUpdateRawCompareSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateRawCompareSummary {
  return {
    compareId: String(raw.compareId ?? ""),
    decisionSuggestion: String(raw.decisionSuggestion ?? ""),
    compareKeyMode: String(raw.compareKeyMode ?? ""),
    compareKeyColumns: Array.isArray(raw.compareKeyColumns)
      ? raw.compareKeyColumns.map((item) => String(item))
      : [],
    blockerCount: Number(raw.blockerCount ?? 0),
    reviewCount: Number(raw.reviewCount ?? 0),
    infoCount: Number(raw.infoCount ?? 0),
    advancedCountryCount: Number(raw.advancedCountryCount ?? 0),
    regressedCountryCount: Number(raw.regressedCountryCount ?? 0),
    newCountryCount: Number(raw.newCountryCount ?? 0),
    missingCountryCount: Number(raw.missingCountryCount ?? 0),
    addedCountryCount: Number(raw.addedCountryCount ?? 0),
    removedCountryCount: Number(raw.removedCountryCount ?? 0)
  };
}

function mapJatoMonthlyUpdateRefreshSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateRefreshSummary {
  return {
    jobStatus: String(raw.jobStatus ?? ""),
    jobElapsedSeconds: Number(raw.jobElapsedSeconds ?? 0),
    rowCount: Number(raw.rowCount ?? 0),
    columnCount: Number(raw.columnCount ?? 0),
    partitionCount: Number(raw.partitionCount ?? 0),
    changedRows: Number(raw.changedRows ?? 0),
    changedCountryCount: Number(raw.changedCountryCount ?? 0),
    fingerprintMatched: Boolean(raw.fingerprintMatched),
    fingerprintUpdated: Boolean(raw.fingerprintUpdated),
    conflictGroupCount: Number(raw.conflictGroupCount ?? 0),
    conflictRowCount: Number(raw.conflictRowCount ?? 0)
  };
}

function mapJatoMonthlyUpdateSmartMergeSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateSmartMergeSummary {
  return {
    mergedAt: String(raw.mergedAt ?? raw.mergedAt ?? ""),
    regressedCountryCount: Number(raw.regressedCountryCount ?? 0),
    regressedCountries: Array.isArray(raw.regressedCountries)
      ? raw.regressedCountries.map((c) => String(c))
      : [],
    totalRowCount: Number(raw.totalRowCount ?? 0),
  };
}

function mapJatoMonthlyUpdatePublication(
  raw: Record<string, unknown>
): JatoMonthlyUpdatePublication {
  return {
    publishedAt: raw.publishedAt === undefined || raw.publishedAt === null ? null : String(raw.publishedAt),
    publishedBy: raw.publishedBy === undefined || raw.publishedBy === null ? null : String(raw.publishedBy),
    backupDir: raw.backupDir === undefined || raw.backupDir === null ? null : String(raw.backupDir),
    activeParquetPath: raw.activeParquetPath === undefined || raw.activeParquetPath === null ? null : String(raw.activeParquetPath),
    activeManifestPath: raw.activeManifestPath === undefined || raw.activeManifestPath === null ? null : String(raw.activeManifestPath),
    activePartitionPath: raw.activePartitionPath === undefined || raw.activePartitionPath === null ? null : String(raw.activePartitionPath),
    activeFingerprintPath: raw.activeFingerprintPath === undefined || raw.activeFingerprintPath === null ? null : String(raw.activeFingerprintPath),
    activeRefreshReportPath: raw.activeRefreshReportPath === undefined || raw.activeRefreshReportPath === null ? null : String(raw.activeRefreshReportPath),
    rolledBackAt: raw.rolledBackAt === undefined || raw.rolledBackAt === null ? null : String(raw.rolledBackAt),
    rolledBackBy: raw.rolledBackBy === undefined || raw.rolledBackBy === null ? null : String(raw.rolledBackBy),
    rollbackBackupDir: raw.rollbackBackupDir === undefined || raw.rollbackBackupDir === null ? null : String(raw.rollbackBackupDir)
  };
}

function mapCocMatchJob(raw: Record<string, unknown>): CocMatchJob {
  return {
    jobId: String(raw.jobId ?? ""),
    status: String(raw.status ?? ""),
    country: String(raw.country ?? ""),
    month: String(raw.month ?? ""),
    fileExt: String(raw.fileExt ?? ""),
    excelFilename: String(raw.excelFilename ?? ""),
    archiveFilename: String(raw.archiveFilename ?? ""),
    totalRows: raw.totalRows === undefined ? undefined : Number(raw.totalRows),
    matchedCount: raw.matchedCount === undefined ? undefined : Number(raw.matchedCount),
    missingCount: raw.missingCount === undefined ? undefined : Number(raw.missingCount),
    coverageRate: raw.coverageRate === undefined ? undefined : Number(raw.coverageRate),
    previousRun: raw.previousRun && typeof raw.previousRun === "object"
      ? (raw.previousRun as { month: string; matched: number; total: number })
      : null,
    diffSummary: raw.diffSummary && typeof raw.diffSummary === "object"
      ? (raw.diffSummary as { gained: number; lost: number; newEntries: number })
      : null,
    triggeredBy: String(raw.triggeredBy ?? ""),
    error: raw.error === undefined ? null : String(raw.error),
    createdAt: String(raw.createdAt ?? ""),
    startedAt: raw.startedAt === undefined ? null : String(raw.startedAt),
    finishedAt: raw.finishedAt === undefined ? null : String(raw.finishedAt),
  };
}

function mapJatoMonthlyUpdateJob(raw: Record<string, unknown>): JatoMonthlyUpdateJob {
  const uploadRaw = raw.upload && typeof raw.upload === "object"
    ? raw.upload as Record<string, unknown>
    : null;
  const planRaw = raw.plan && typeof raw.plan === "object"
    ? raw.plan as Record<string, unknown>
    : null;
  const artifactsRaw = raw.artifacts && typeof raw.artifacts === "object"
    ? raw.artifacts as Record<string, unknown>
    : null;
  const summariesRaw = raw.summaries && typeof raw.summaries === "object"
    ? raw.summaries as Record<string, unknown>
    : null;

  const upload: JatoMonthlyUpdateUpload | null = uploadRaw ? {
    originalFilename: String(uploadRaw.originalFilename ?? ""),
    storedPath: uploadRaw.storedPath === undefined || uploadRaw.storedPath === null
      ? null
      : String(uploadRaw.storedPath),
    sizeBytes: uploadRaw.sizeBytes === undefined ? undefined : Number(uploadRaw.sizeBytes),
    sha256: uploadRaw.sha256 === undefined || uploadRaw.sha256 === null
      ? null
      : String(uploadRaw.sha256),
  } : null;

  const plan: JatoMonthlyUpdatePlan | null = planRaw ? {
    path: planRaw.path === undefined || planRaw.path === null ? null : String(planRaw.path),
    batchId: planRaw.batchId === undefined || planRaw.batchId === null ? null : String(planRaw.batchId),
    compareId: planRaw.compareId === undefined || planRaw.compareId === null ? null : String(planRaw.compareId),
    compareCommand: planRaw.compareCommand === undefined || planRaw.compareCommand === null
      ? null
      : String(planRaw.compareCommand),
    refreshCommand: planRaw.refreshCommand === undefined || planRaw.refreshCommand === null
      ? null
      : String(planRaw.refreshCommand)
  } : null;

  const artifacts: JatoMonthlyUpdateArtifacts | null = artifactsRaw ? {
    jobDir: artifactsRaw.jobDir === undefined || artifactsRaw.jobDir === null ? null : String(artifactsRaw.jobDir),
    logPath: artifactsRaw.logPath === undefined || artifactsRaw.logPath === null ? null : String(artifactsRaw.logPath),
    baselinePath: artifactsRaw.baselinePath === undefined || artifactsRaw.baselinePath === null ? null : String(artifactsRaw.baselinePath),
    stagedPatchPath: artifactsRaw.stagedPatchPath === undefined || artifactsRaw.stagedPatchPath === null ? null : String(artifactsRaw.stagedPatchPath),
    supplementParquetPath: artifactsRaw.supplementParquetPath === undefined || artifactsRaw.supplementParquetPath === null
      ? null
      : String(artifactsRaw.supplementParquetPath),
    planPath: artifactsRaw.planPath === undefined || artifactsRaw.planPath === null ? null : String(artifactsRaw.planPath),
    reviewDir: artifactsRaw.reviewDir === undefined || artifactsRaw.reviewDir === null ? null : String(artifactsRaw.reviewDir),
    rawCompareReportPath: artifactsRaw.rawCompareReportPath === undefined || artifactsRaw.rawCompareReportPath === null
      ? null
      : String(artifactsRaw.rawCompareReportPath),
    stagingOutputPath: artifactsRaw.stagingOutputPath === undefined || artifactsRaw.stagingOutputPath === null
      ? null
      : String(artifactsRaw.stagingOutputPath),
    manifestPath: artifactsRaw.manifestPath === undefined || artifactsRaw.manifestPath === null
      ? null
      : String(artifactsRaw.manifestPath),
    partitionOutputPath: artifactsRaw.partitionOutputPath === undefined || artifactsRaw.partitionOutputPath === null
      ? null
      : String(artifactsRaw.partitionOutputPath),
    refreshReportPath: artifactsRaw.refreshReportPath === undefined || artifactsRaw.refreshReportPath === null
      ? null
      : String(artifactsRaw.refreshReportPath),
    fingerprintPath: artifactsRaw.fingerprintPath === undefined || artifactsRaw.fingerprintPath === null
      ? null
      : String(artifactsRaw.fingerprintPath)
  } : null;

  const summaries: JatoMonthlyUpdateSummaries | null = summariesRaw ? {
    rawCompare: summariesRaw.rawCompare && typeof summariesRaw.rawCompare === "object"
      ? mapJatoMonthlyUpdateRawCompareSummary(summariesRaw.rawCompare as Record<string, unknown>)
      : undefined,
    refresh: summariesRaw.refresh && typeof summariesRaw.refresh === "object"
      ? mapJatoMonthlyUpdateRefreshSummary(summariesRaw.refresh as Record<string, unknown>)
      : undefined,
    smartMerge: summariesRaw.smartMerge && typeof summariesRaw.smartMerge === "object"
      ? mapJatoMonthlyUpdateSmartMergeSummary(summariesRaw.smartMerge as Record<string, unknown>)
      : undefined,
  } : null;
  const publicationRaw = raw.publication && typeof raw.publication === "object"
    ? raw.publication as Record<string, unknown>
    : null;

  return {
    jobId: String(raw.jobId ?? ""),
    month: String(raw.month ?? ""),
    batchId: raw.batchId === undefined || raw.batchId === null ? null : String(raw.batchId),
    status: String(raw.status ?? ""),
    phase: String(raw.phase ?? ""),
    triggeredBy: String(raw.triggeredBy ?? ""),
    createdAt: String(raw.createdAt ?? ""),
    updatedAt: String(raw.updatedAt ?? ""),
    startedAt: raw.startedAt === undefined || raw.startedAt === null ? null : String(raw.startedAt),
    finishedAt: raw.finishedAt === undefined || raw.finishedAt === null ? null : String(raw.finishedAt),
    error: raw.error === undefined || raw.error === null ? null : String(raw.error),
    upload,
    plan,
    artifacts,
    summaries,
    publication: publicationRaw ? mapJatoMonthlyUpdatePublication(publicationRaw) : null,
    logPath: raw.logPath === undefined || raw.logPath === null ? null : String(raw.logPath),
    logTail: raw.logTail === undefined || raw.logTail === null ? null : String(raw.logTail)
  };
}

function mapJatoMonthlyUpdateReviewFinding(
  raw: Record<string, unknown>
): JatoMonthlyUpdateReviewFinding {
  return {
    severity: String(raw.severity ?? ""),
    scope: String(raw.scope ?? ""),
    target: String(raw.target ?? ""),
    ruleId: String(raw.ruleId ?? ""),
    message: String(raw.message ?? ""),
    metrics: raw.metrics && typeof raw.metrics === "object"
      ? raw.metrics as Record<string, unknown>
      : {},
    suggestedAction: String(raw.suggestedAction ?? "")
  };
}

function mapJatoMonthlyUpdateConflictSample(
  raw: Record<string, unknown>
): JatoMonthlyUpdateConflictSample {
  return {
    country: String(raw.country ?? ""),
    businessKey: raw.businessKey && typeof raw.businessKey === "object"
      ? raw.businessKey as Record<string, unknown>
      : {},
    oldValueDigest: raw.oldValueDigest === undefined || raw.oldValueDigest === null
      ? null
      : String(raw.oldValueDigest),
    newValueDigest: raw.newValueDigest === undefined || raw.newValueDigest === null
      ? null
      : String(raw.newValueDigest),
    changedFields: Array.isArray(raw.changedFields)
      ? raw.changedFields.map((item) => String(item))
      : []
  };
}

function mapJatoMonthlyUpdateOverlapChangeSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateOverlapChangeSummary {
  return {
    country: String(raw.country ?? ""),
    compareMonths: Array.isArray(raw.compareMonths)
      ? raw.compareMonths.map((item) => String(item))
      : [],
    compareKeyColumns: Array.isArray(raw.compareKeyColumns)
      ? raw.compareKeyColumns.map((item) => String(item))
      : [],
    addedRecordCount: Number(raw.addedRecordCount ?? 0),
    removedRecordCount: Number(raw.removedRecordCount ?? 0),
    changedRecordCount: Number(raw.changedRecordCount ?? 0),
    unchangedRecordCount: Number(raw.unchangedRecordCount ?? 0),
    changeRate: Number(raw.changeRate ?? 0),
    sampleAddedKeys: Array.isArray(raw.sampleAddedKeys)
      ? raw.sampleAddedKeys.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
      : [],
    sampleRemovedKeys: Array.isArray(raw.sampleRemovedKeys)
      ? raw.sampleRemovedKeys.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
      : [],
    sampleChangedKeys: Array.isArray(raw.sampleChangedKeys)
      ? raw.sampleChangedKeys.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
      : []
  };
}

function mapJatoMonthlyUpdateCountryFreshnessSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateCountryFreshnessSummary {
  return {
    country: String(raw.country ?? ""),
    oldLatestMonth: raw.oldLatestMonth === undefined || raw.oldLatestMonth === null
      ? null
      : String(raw.oldLatestMonth),
    newLatestMonth: raw.newLatestMonth === undefined || raw.newLatestMonth === null
      ? null
      : String(raw.newLatestMonth),
    freshnessStatus: String(raw.freshnessStatus ?? ""),
    rowDelta: Number(raw.rowDelta ?? 0)
  };
}

function mapJatoMonthlyUpdateCountryCoverageSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateCountryCoverageSummary {
  return {
    country: String(raw.country ?? ""),
    oldMonths: Array.isArray(raw.oldMonths)
      ? raw.oldMonths.map((item) => String(item))
      : [],
    newMonths: Array.isArray(raw.newMonths)
      ? raw.newMonths.map((item) => String(item))
      : [],
    addedMonths: Array.isArray(raw.addedMonths)
      ? raw.addedMonths.map((item) => String(item))
      : [],
    removedMonths: Array.isArray(raw.removedMonths)
      ? raw.removedMonths.map((item) => String(item))
      : [],
    overlappingMonths: Array.isArray(raw.overlappingMonths)
      ? raw.overlappingMonths.map((item) => String(item))
      : [],
    coverageStatus: String(raw.coverageStatus ?? "")
  };
}

function mapJatoMonthlyUpdateCountryMonthlySalesRow(
  raw: Record<string, unknown>
): JatoMonthlyUpdateCountryMonthlySalesRow {
  return {
    month: String(raw.month ?? ""),
    referenceSales: raw.referenceSales === undefined || raw.referenceSales === null
      ? null
      : Number(raw.referenceSales),
    candidateSales: raw.candidateSales === undefined || raw.candidateSales === null
      ? null
      : Number(raw.candidateSales),
    deltaSales: raw.deltaSales === undefined || raw.deltaSales === null
      ? null
      : Number(raw.deltaSales),
    changeStatus: String(raw.changeStatus ?? "")
  };
}

function mapJatoMonthlyUpdateCountryMonthlySalesSummary(
  raw: Record<string, unknown>
): JatoMonthlyUpdateCountryMonthlySalesSummary {
  return {
    country: String(raw.country ?? ""),
    rows: Array.isArray(raw.rows)
      ? raw.rows.map((item) => mapJatoMonthlyUpdateCountryMonthlySalesRow(item as Record<string, unknown>))
      : []
  };
}

function mapJatoMonthlyUpdateReviewBundle(
  raw: Record<string, unknown>
): JatoMonthlyUpdateReviewBundle {
  return {
    jobId: String(raw.jobId ?? ""),
    reviewDir: raw.reviewDir === undefined || raw.reviewDir === null ? null : String(raw.reviewDir),
    compareId: String(raw.compareId ?? ""),
    decisionSuggestion: String(raw.decisionSuggestion ?? ""),
    compareKeyColumns: Array.isArray(raw.compareKeyColumns)
      ? raw.compareKeyColumns.map((item) => String(item))
      : [],
    checklistMarkdown: raw.checklistMarkdown === undefined || raw.checklistMarkdown === null
      ? null
      : String(raw.checklistMarkdown),
    reviewFindings: Array.isArray(raw.reviewFindings)
      ? raw.reviewFindings.map((item) => mapJatoMonthlyUpdateReviewFinding(item as Record<string, unknown>))
      : [],
    sampledCountries: Array.isArray(raw.sampledCountries)
      ? raw.sampledCountries.map((item) => String(item))
      : [],
    conflictSampleCount: Number(raw.conflictSampleCount ?? 0),
    conflictSamples: Array.isArray(raw.conflictSamples)
      ? raw.conflictSamples.map((item) => mapJatoMonthlyUpdateConflictSample(item as Record<string, unknown>))
      : [],
    overlapChangeSummary: Array.isArray(raw.overlapChangeSummary)
      ? raw.overlapChangeSummary.map((item) => mapJatoMonthlyUpdateOverlapChangeSummary(item as Record<string, unknown>))
      : [],
    countryFreshnessSummary: Array.isArray(raw.countryFreshnessSummary)
      ? raw.countryFreshnessSummary.map((item) => mapJatoMonthlyUpdateCountryFreshnessSummary(item as Record<string, unknown>))
      : [],
    countryCoverageSummary: Array.isArray(raw.countryCoverageSummary)
      ? raw.countryCoverageSummary.map((item) => mapJatoMonthlyUpdateCountryCoverageSummary(item as Record<string, unknown>))
      : [],
    countrySalesReferenceLabel: String(raw.countrySalesReferenceLabel ?? ""),
    countryMonthlySalesSummary: Array.isArray(raw.countryMonthlySalesSummary)
      ? raw.countryMonthlySalesSummary.map((item) => mapJatoMonthlyUpdateCountryMonthlySalesSummary(item as Record<string, unknown>))
      : [],
    countryMonthlySalesError: raw.countryMonthlySalesError === undefined || raw.countryMonthlySalesError === null
      ? null
      : String(raw.countryMonthlySalesError),
    timeAxisCheck: raw.timeAxisCheck && typeof raw.timeAxisCheck === "object"
      ? raw.timeAxisCheck as Record<string, unknown>
      : {},
    countryScopeSummary: raw.countryScopeSummary && typeof raw.countryScopeSummary === "object"
      ? raw.countryScopeSummary as Record<string, unknown>
      : {},
    refreshSummary: raw.refreshSummary && typeof raw.refreshSummary === "object"
      ? mapJatoMonthlyUpdateRefreshSummary(raw.refreshSummary as Record<string, unknown>)
      : null
  };
}

function mapJatoMonthlyUpdateCleanupResult(
  raw: Record<string, unknown>
): JatoMonthlyUpdateCleanupResult {
  return {
    cleanedAt: String(raw.cleanedAt ?? ""),
    triggeredBy: String(raw.triggeredBy ?? ""),
    cleanupTier: String(raw.cleanupTier ?? "safe") as "safe" | "cautious",
    activeBaselinePath:
      raw.activeBaselinePath === undefined || raw.activeBaselinePath === null
        ? null
        : String(raw.activeBaselinePath),
    activePatchMonth:
      raw.activePatchMonth === undefined || raw.activePatchMonth === null
        ? null
        : String(raw.activePatchMonth),
    freedBytes: Number(raw.freedBytes ?? 0),
    archivedBaselineCount: Number(raw.archivedBaselineCount ?? 0),
    archivedBaselines: Array.isArray(raw.archivedBaselines)
      ? raw.archivedBaselines.map((item) => String(item))
      : [],
    archivedPatchDirCount: Number(raw.archivedPatchDirCount ?? 0),
    archivedPatchDirs: Array.isArray(raw.archivedPatchDirs)
      ? raw.archivedPatchDirs.map((item) => String(item))
      : [],
    removedUploadSessionDirCount: Number(raw.removedUploadSessionDirCount ?? 0),
    removedUploadSessionDirs: Array.isArray(raw.removedUploadSessionDirs)
      ? raw.removedUploadSessionDirs.map((item) => String(item))
      : [],
    removedJobUploadDirCount: Number(raw.removedJobUploadDirCount ?? 0),
    removedJobUploadDirs: Array.isArray(raw.removedJobUploadDirs)
      ? raw.removedJobUploadDirs.map((item) => String(item))
      : [],
    deletedReviewDirCount: Number(raw.deletedReviewDirCount ?? 0),
    deletedReviewDirs: Array.isArray(raw.deletedReviewDirs)
      ? raw.deletedReviewDirs.map((item) => String(item))
      : [],
    deletedStagingDirCount: Number(raw.deletedStagingDirCount ?? 0),
    deletedStagingDirs: Array.isArray(raw.deletedStagingDirs)
      ? raw.deletedStagingDirs.map((item) => String(item))
      : [],
    deletedRefreshBackupDirCount: Number(raw.deletedRefreshBackupDirCount ?? 0),
    deletedRefreshBackupDirs: Array.isArray(raw.deletedRefreshBackupDirs)
      ? raw.deletedRefreshBackupDirs.map((item) => String(item))
      : [],
    deletedArchivedBaselineCount: Number(raw.deletedArchivedBaselineCount ?? 0),
    deletedArchivedBaselines: Array.isArray(raw.deletedArchivedBaselines)
      ? raw.deletedArchivedBaselines.map((item) => String(item))
      : [],
    deletedArchivedPatchDirCount: Number(raw.deletedArchivedPatchDirCount ?? 0),
    deletedArchivedPatchDirs: Array.isArray(raw.deletedArchivedPatchDirs)
      ? raw.deletedArchivedPatchDirs.map((item) => String(item))
      : [],
  };
}

function mapJatoMonthlyUpdateStorageMetric(
  raw: Record<string, unknown>
): JatoMonthlyUpdateStorageMetric {
  return {
    key: String(raw.key ?? ""),
    label: String(raw.label ?? ""),
    bytes: Number(raw.bytes ?? 0),
    fileCount: Number(raw.fileCount ?? 0),
    dirCount: Number(raw.dirCount ?? 0),
    paths: Array.isArray(raw.paths)
      ? raw.paths.map((item) => String(item))
      : [],
    cleanupTier: String(raw.cleanupTier ?? "protected") as "safe" | "cautious" | "protected",
  };
}

function mapJatoMonthlyUpdateMaintenanceStatus(
  raw: Record<string, unknown>
): JatoMonthlyUpdateMaintenanceStatus {
  return {
    checkedAt: String(raw.checkedAt ?? ""),
    activeBaselinePath:
      raw.activeBaselinePath === undefined || raw.activeBaselinePath === null
        ? null
        : String(raw.activeBaselinePath),
    activeBaselineSource:
      raw.activeBaselineSource === undefined || raw.activeBaselineSource === null
        ? null
        : String(raw.activeBaselineSource),
    latestPatchBatch:
      raw.latestPatchBatch === undefined || raw.latestPatchBatch === null
        ? null
        : String(raw.latestPatchBatch),
    jobCount: Number(raw.jobCount ?? 0),
    uploadSessionCount: Number(raw.uploadSessionCount ?? 0),
    trackedStorageBytes: Number(raw.trackedStorageBytes ?? 0),
    storageMetrics: Array.isArray(raw.storageMetrics)
      ? raw.storageMetrics.map((item) => mapJatoMonthlyUpdateStorageMetric(item as Record<string, unknown>))
      : [],
  };
}

function mapJatoMonthlyUpdateBaselinePromotionResult(
  raw: Record<string, unknown>
): JatoMonthlyUpdateBaselinePromotionResult {
  return {
    promotedAt: String(raw.promotedAt ?? ""),
    triggeredBy: String(raw.triggeredBy ?? ""),
    sourceParquetPath:
      raw.sourceParquetPath === undefined || raw.sourceParquetPath === null
        ? null
        : String(raw.sourceParquetPath),
    baselinePath:
      raw.baselinePath === undefined || raw.baselinePath === null
        ? null
        : String(raw.baselinePath),
    detectedLatestMonth:
      raw.detectedLatestMonth === undefined || raw.detectedLatestMonth === null
        ? null
        : String(raw.detectedLatestMonth),
    countryCount: Number(raw.countryCount ?? 0),
    rowCount: Number(raw.rowCount ?? 0),
    archivedBaselineCount: Number(raw.archivedBaselineCount ?? 0),
    archivedBaselines: Array.isArray(raw.archivedBaselines)
      ? raw.archivedBaselines.map((item) => String(item))
      : [],
  };
}

function mapJatoMonthlyUpdateUploadSession(
  raw: Record<string, unknown>
): JatoMonthlyUpdateUploadSession {
  return {
    uploadId: String(raw.uploadId ?? ""),
    filename: String(raw.filename ?? ""),
    sizeBytes: Number(raw.sizeBytes ?? 0),
    chunkSize: Number(raw.chunkSize ?? 0),
    totalChunks: Number(raw.totalChunks ?? 0),
    receivedChunkCount: Number(raw.receivedChunkCount ?? 0),
    receivedChunks: Array.isArray(raw.receivedChunks)
      ? raw.receivedChunks.map((item) => Number(item))
      : [],
    uploadedBytes: Number(raw.uploadedBytes ?? 0),
    status: String(raw.status ?? ""),
    createdAt:
      raw.createdAt === undefined || raw.createdAt === null
        ? null
        : String(raw.createdAt),
    updatedAt:
      raw.updatedAt === undefined || raw.updatedAt === null
        ? null
        : String(raw.updatedAt),
    completedAt:
      raw.completedAt === undefined || raw.completedAt === null
        ? null
        : String(raw.completedAt),
    assembledPath:
      raw.assembledPath === undefined || raw.assembledPath === null
        ? null
        : String(raw.assembledPath),
    resumeKey:
      raw.resumeKey === undefined || raw.resumeKey === null
        ? null
        : String(raw.resumeKey),
    fileSha256:
      raw.fileSha256 === undefined || raw.fileSha256 === null
        ? null
        : String(raw.fileSha256),
    triggeredBy:
      raw.triggeredBy === undefined || raw.triggeredBy === null
        ? null
        : String(raw.triggeredBy),
  };
}

async function requestJatoMonthlyUpdateUploadChunk(
  uploadId: string,
  partNumber: number,
  chunk: Blob,
  chunkSha256: string
): Promise<JatoMonthlyUpdateUploadSession> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}/msrp/monthly-update-uploads/${uploadId}/parts/${partNumber}`, {
      method: "PUT",
      headers: new Headers({
        ...getAuthHeaders(),
        "Content-Type": "application/octet-stream",
        "X-Chunk-SHA256": chunkSha256
      }),
      body: chunk
    });
  } catch (error) {
    if (isAbortLikeError(error)) {
      throw error;
    }
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `网络请求失败：/msrp/monthly-update-uploads/${uploadId}/parts/${partNumber} (${message})`
    );
  }
  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`${response.status} ${message}`);
  }
  const payload = await response.json() as { item: Record<string, unknown> };
  return mapJatoMonthlyUpdateUploadSession(payload.item);
}

async function getJatoMonthlyUpdateUploadSession(
  uploadId: string
): Promise<JatoMonthlyUpdateUploadSession> {
  return request<{ item: Record<string, unknown> }>(
    `/msrp/monthly-update-uploads/${uploadId}`
  ).then((res) => mapJatoMonthlyUpdateUploadSession(res.item));
}

async function initiateJatoMonthlyUpdateUploadSession(
  file: File,
  resumeKey: string
): Promise<JatoMonthlyUpdateUploadSession> {
  return request<{ item: Record<string, unknown> }>("/msrp/monthly-update-uploads/initiate", {
    method: "POST",
    body: JSON.stringify({
      filename: file.name,
      sizeBytes: file.size,
      resumeKey,
    })
  }).then((res) => mapJatoMonthlyUpdateUploadSession(res.item));
}

async function completeJatoMonthlyUpdateUploadSession(
  uploadId: string
): Promise<JatoMonthlyUpdateUploadSession> {
  return request<{ item: Record<string, unknown> }>(
    `/msrp/monthly-update-uploads/${uploadId}/complete`,
    { method: "POST" }
  ).then((res) => mapJatoMonthlyUpdateUploadSession(res.item));
}

function mapReviewScopeCountrySummary(raw: Record<string, unknown>): ReviewScopeCountrySummary {
  return {
    country: String(raw.country ?? ""),
    latestMonth: String(raw.latestMonth ?? ""),
    windowStartMonth: String(raw.windowStartMonth ?? ""),
    windowEndMonth: String(raw.windowEndMonth ?? ""),
    candidateCount: Number(raw.candidateCount ?? 0),
    missingCount: Number(raw.missingCount ?? 0),
    topMissingBrands: Array.isArray(raw.topMissingBrands)
      ? raw.topMissingBrands.map((item) => String(item))
      : [],
    topMissingModels: Array.isArray(raw.topMissingModels)
      ? raw.topMissingModels.map((item) => String(item))
      : [],
  };
}

function mapReviewBacklogOpportunity(raw: Record<string, unknown>): ReviewBacklogOpportunity {
  return {
    priorityRank: Number(raw.priorityRank ?? 0),
    country: String(raw.country ?? ""),
    countryCode: String(raw.countryCode ?? ""),
    brand: String(raw.brand ?? ""),
    brandSlug: String(raw.brandSlug ?? ""),
    candidateModelCount: Number(raw.candidateModelCount ?? 0),
    sales12mSum: Number(raw.sales12mSum ?? 0),
    topModels: Array.isArray(raw.topModels)
      ? raw.topModels.map((item) => String(item))
      : [],
    sourceCode: String(raw.sourceCode ?? ""),
    fileName: String(raw.fileName ?? ""),
    relativePath: String(raw.relativePath ?? ""),
  };
}

function mapReviewWorkbench(raw: Record<string, unknown>): ReviewWorkbench {
  const coverageSummary = (raw.coverageSummary as Record<string, unknown> | undefined) ?? {};
  return {
    candidateScopeAvailable: Boolean(raw.candidateScopeAvailable),
    backlogAvailable: Boolean(raw.backlogAvailable),
    generatedAtUtc: raw.generatedAtUtc === undefined || raw.generatedAtUtc === null
      ? null
      : String(raw.generatedAtUtc),
    reportTopN: Number(raw.reportTopN ?? 0),
    countryCount: Number(raw.countryCount ?? 0),
    candidateCount: Number(raw.candidateCount ?? 0),
    coverageSummary: {
      modelSource: Number(coverageSummary.modelSource ?? 0),
      brandSource: Number(coverageSummary.brandSource ?? 0),
      missingSource: Number(coverageSummary.missingSource ?? 0),
    },
    countryScope: Array.isArray(raw.countryScope)
      ? raw.countryScope.map((item) => mapReviewScopeCountrySummary(item as Record<string, unknown>))
      : [],
    backlog: Array.isArray(raw.backlog)
      ? raw.backlog.map((item) => mapReviewBacklogOpportunity(item as Record<string, unknown>))
      : [],
  };
}

export const api = {
  columns: () => request<{ items: string[] }>("/metadata/columns"),
  filterOptions: (payload: FilterOptionsPayload, init?: RequestInit) =>
    request<{ column: string; options: string[] }>(
      "/filters/options",
      { method: "POST", body: JSON.stringify(payload), ...init }
    ),
  analysis: (payload: AnalysisQuery) =>
    request<{ route: string; rows: number; items: Record<string, unknown>[] }>(
      "/analysis/query",
      { method: "POST", body: JSON.stringify(payload) }
    ),
  timeSeries: (payload: {
    filters: Record<string, string[]>;
    grain: "month" | "year";
    top_n: number;
  }) =>
    request<{ grain: string; rows: number; items: { time: string; value: number }[] }>(
      "/analysis/time-series",
      { method: "POST", body: JSON.stringify(payload) }
    ),
  overview: (payload: {
    filters: Record<string, string[]>;
    prefer_precomputed: boolean;
    top_n: number;
  }) =>
    request<OverviewResponse>("/analysis/overview", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  dataFreshness: () =>
    request<{ items: DataFreshnessItem[] }>("/analysis/data-freshness"),
  countryChatMetadata: () =>
    request<CountryChatMetadataResponse>("/assistant/country/metadata"),
  countryChatStream: async (
    payload: {
      country: string;
      question: string;
      history: CountryChatTurn[];
      model?: string;
    },
    onStatus: (text: string) => void,
    onToken: (token: string) => void,
    onMeta: (meta: Record<string, unknown>) => void,
    onDone: (suggestions: string[]) => void,
    onError: (error: string) => void,
  ): Promise<void> => {
    const API_BASE = "/v1";
    const response = await fetch(`${API_BASE}/assistant/country/chat/stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      onError(`Stream error: ${response.status}`);
      return;
    }
    const reader = response.body?.getReader();
    if (!reader) { onError("No stream reader"); return; }
    const decoder = new TextDecoder();
    let buffer = "";
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");
      buffer = lines.pop() || "";
      let eventType = "";
      for (const line of lines) {
        if (line.startsWith("event: ")) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith("data: ")) {
          try {
            const data = JSON.parse(line.slice(6));
            if (eventType === "status") {
              onStatus(data.text || "");
            } else if (eventType === "token") {
              onToken(data.text || "");
            } else if (eventType === "meta") {
              onMeta(data);
            } else if (eventType === "done") {
              onDone(data.suggestedPrompts || []);
            }
          } catch { /* skip malformed */ }
          eventType = "";
        }
      }
    }
  },
  countryChat: (payload: {
    country: string;
    question: string;
    history: CountryChatTurn[];
    refresh_news?: boolean;
    model?: string;
  }) =>
    request<CountryChatResponse>("/assistant/country/chat", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  countryChatNewsStatus: (country: string) =>
    request<CountryChatNewsOpsStatus>(
      `/assistant/country/news/status?country=${encodeURIComponent(country)}`
    ),
  countryChatNewsRefresh: (payload: {
    country: string;
    limit?: number;
    persist?: boolean;
    enrich_with_gemini?: boolean | null;
  }) =>
    request<CountryChatNewsRefreshResponse>("/assistant/country/news/refresh", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  countryChatDeck: (payload: {
    country: string;
    question?: string;
    intents?: string[];
    extracted_params?: Record<string, unknown>;
    selected_year?: number;
    selected_model?: string;
    model_top_n?: number;
  }) =>
    request<CountryChatDeckResponse>("/assistant/country/chart-deck", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  detail: (payload: {
    filters: Record<string, string[]>;
    columns: string[];
    page: number;
    page_size: number;
    exclude_zero_sales?: boolean;
  }) =>
    request<DetailResponse>("/analysis/detail", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  detailCsv: (payload: {
    filters: Record<string, string[]>;
    columns: string[];
    max_rows: number;
    exclude_zero_sales?: boolean;
  }) =>
    requestBlob("/analysis/detail-csv", {
      method: "POST",
      body: JSON.stringify(payload),
      headers: {
        "Content-Type": "application/json"
      }
    }),
  advancedChart: (payload: {
    group: string;
    chart: string;
    filters: Record<string, string[]>;
    top_n: number;
    options?: Record<string, unknown>;
    time_range?: { start: string; end: string };
  }) =>
    request<AdvancedChartResponse>("/analysis/advanced-chart", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  groupedTimeSeries: (payload: {
    filters: Record<string, string[]>;
    grain: "year" | "month";
    group_by: string | null;
    share_split_by?: "segment" | "powertrain";
    top_n: number;
    include_others: boolean;
    time_range?: { start: string; end: string };
  }) =>
    request<GroupedTimeSeriesResponse>("/analysis/time-series-grouped", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  modelVersions: (payload: {
    filters: Record<string, string[]>;
    model_name: string;
    top_n?: number;
    time_range?: { start: string; end: string };
  }) =>
    request<ModelVersionsResponse>("/analysis/model-versions", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  positioningMap: (payload: {
    filters: Record<string, string[]>;
    target_length?: number | null;
    target_msrp?: number | null;
    length_range?: number;
    manual_competitors?: string[];
    top_n?: number;
    n_clusters?: number;
    time_range?: { start: string; end: string };
  }) =>
    request<PositioningMapResponse>("/analysis/positioning-map", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  rvFinance: (payload: {
    vehicles: RvFinanceVehicle[];
    currency?: string;
    fx_rate?: number;
    sensitivity_vehicle_idx?: number;
  }) =>
    request<RvFinanceResponse>("/analysis/rv-finance", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  marketScanDeck: (payload: MarketScanDeckRequest = {}, signal?: AbortSignal) =>
    request<MarketScanDeckResponse>("/market-scan/deck", {
      method: "POST",
      body: JSON.stringify(payload),
      signal,
    }),
  rankingTrend: (params: Record<string, string>) => {
    const qs = new URLSearchParams(params).toString();
    return request<RankingTrendResponse>(`/market-scan/ranking-trend?${qs}`);
  },
  positioningPricingDeck: (payload: PositioningPricingDeckRequest = {}) =>
    request<PositioningPricingDeckResponse>("/market-scan/positioning-pricing-deck", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  versionComparisonDeck: (payload: VersionComparisonDeckRequest = {}) =>
    request<VersionComparisonDeckResponse>("/market-scan/version-comparison-deck", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  nordicCustomerDeck: (mode: CustomerInsightMode = "benchmark", countries?: string[]) => {
    const search = new URLSearchParams();
    search.set("mode", mode);
    (countries ?? []).forEach((country) => {
      if (country.trim()) {
        search.append("countries", country.trim().toUpperCase());
      }
    });
    return request<CustomerInsightDeckResponse>(`/market-scan/nordic-customer-deck?${search.toString()}`);
  },
  nordicHevCustomerDeck: (mode: CustomerInsightMode = "benchmark", countries?: string[]) => {
    const search = new URLSearchParams();
    search.set("mode", mode);
    (countries ?? []).forEach((country) => {
      if (country.trim()) {
        search.append("countries", country.trim().toUpperCase());
      }
    });
    return request<CustomerInsightDeckResponse>(`/market-scan/nordic-hev-customer-deck?${search.toString()}`);
  },
  nordicPhevCustomerDeck: (mode: CustomerInsightMode = "benchmark", countries?: string[]) => {
    const search = new URLSearchParams();
    search.set("mode", mode);
    (countries ?? []).forEach((country) => { if (country.trim()) { search.append("countries", country.trim().toUpperCase()); } });
    return request<CustomerInsightDeckResponse>(`/market-scan/nordic-phev-customer-deck?${search.toString()}`);
  },
  nordicBevCustomerDeck: (mode: CustomerInsightMode = "benchmark", countries?: string[]) => {
    const search = new URLSearchParams();
    search.set("mode", mode);
    (countries ?? []).forEach((country) => { if (country.trim()) { search.append("countries", country.trim().toUpperCase()); } });
    return request<CustomerInsightDeckResponse>(`/market-scan/nordic-bev-customer-deck?${search.toString()}`);
  },
  listItems: (params?: {
    page?: number;
    page_size?: number;
    sort_by?: "code" | "name" | "status" | "created" | "updated";
    sort_order?: "asc" | "desc";
    query?: string;
  }) => {
    const search = new URLSearchParams();
    if (params?.page) search.set("page", String(params.page));
    if (params?.page_size) search.set("page_size", String(params.page_size));
    if (params?.sort_by) search.set("sort_by", params.sort_by);
    if (params?.sort_order) search.set("sort_order", params.sort_order);
    if (params?.query) search.set("query", params.query);
    const suffix = search.toString() ? `?${search.toString()}` : "";
    return request<CrudListResponse>(`/crud/items${suffix}`);
  },
  createItem: (payload: Omit<CrudItem, "id">) =>
    request<{ item: CrudItem }>("/crud/items", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  getDataManagementOverview: () =>
    request<{ item: DataManagementOverviewResponse }>("/data-management/overview")
      .then((response) => response.item),
  getVocManagementOverview: (country?: string) => {
    const search = new URLSearchParams();
    if (country) {
      search.set("country", country);
    }
    const suffix = search.toString() ? `?${search.toString()}` : "";
    return request<{ item: DataManagementVocOverviewResponse }>(`/data-management/voc/overview${suffix}`)
      .then((response) => response.item);
  },
  getAirflowStatus: () =>
    request<{ item: DataManagementAirflowStatus }>("/data-management/airflow/status")
      .then((response) => response.item),
  startAirflow: () =>
    request<{ item: DataManagementAirflowActionResponse }>("/data-management/airflow/start", {
      method: "POST"
    }).then((response) => response.item),
  stopAirflow: () =>
    request<{ item: DataManagementAirflowActionResponse }>("/data-management/airflow/stop", {
      method: "POST"
    }).then((response) => response.item),
  syncVocRawToStore: () =>
    request<{ item: DataManagementVocSyncResponse }>("/data-management/voc/sync", {
      method: "POST"
    }).then((response) => response.item),

  /* ── Hermes Governance ──────────────────────────── */
  hermesOverview: () =>
    request<HermesOverviewResponse>("/hermes/overview"),
  hermesPipelineHealth: () =>
    request<HermesPipelineHealthResponse>("/hermes/pipeline-health"),
  hermesSourceQuality: () =>
    request<HermesSourceQualityResponse>("/hermes/source-quality"),
  hermesMsrpCountryProgress: (runId?: string) =>
    request<HermesMsrpCountryProgressResponse>(
      runId
        ? `/hermes/msrp-country-progress?run_id=${encodeURIComponent(runId)}`
        : "/hermes/msrp-country-progress"
    ),
  hermesMsrpDryrunHistory: () =>
    request<HermesMsrpDryrunHistoryResponse>("/hermes/msrp-dryrun-history"),
  hermesCost: () =>
    request<HermesCostResponse>("/hermes/cost"),
  hermesProposals: (status?: string) => {
    const q = status ? `?status=${encodeURIComponent(status)}` : "";
    return request<Record<string, unknown>[]>(`/hermes/proposals${q ? `?${q}` : ""}`);
  },
  hermesFeatures: () =>
    request<Record<string, unknown>[]>("/hermes/features"),
  hermesGaps: (status?: string, category?: string) => {
    const params = new URLSearchParams();
    if (status) params.set("status", status);
    if (category) params.set("category", category);
    const q = params.toString();
    return request<HermesGap[]>(`/hermes/gaps${q ? `?${q}` : ""}`);
  },
  hermesMarkdownDiagrams: (fileFilter?: string) => {
    const q = fileFilter ? `?file_filter=${encodeURIComponent(fileFilter)}` : "";
    return request<HermesMermaidBlock[]>(`/hermes/markdown-diagrams${q}`);
  },
  hermesToolchain: () =>
    request<HermesToolchainResponse>("/hermes/toolchain"),
  hermesArchitecture: () =>
    request<HermesArchResponse>("/hermes/architecture"),
  hermesRun: (command: string) =>
    request<Record<string, unknown>>(`/hermes/run/${command}`, { method: "POST" }),
  hermesListCommands: () =>
    request<Record<string, unknown>>("/hermes/run"),
  hermesSourceDetail: (sourceId: string) =>
    request<Record<string, unknown>>(`/hermes/source/${encodeURIComponent(sourceId)}`),
  hermesSourceHealthHistory: (sourceId: string) =>
    request<Record<string, unknown>>(`/hermes/source/${encodeURIComponent(sourceId)}/health-history`),
  hermesActivityHeatmap: (days?: number) =>
    request<HermesActivityResponse>(`/hermes/activity-heatmap?days=${days || 30}`),
  hermesCostHeatmap: (days?: number) =>
    request<HermesCostResponse>(`/hermes/cost-heatmap?days=${days || 30}`),
  hermesDailySummary: () =>
    request<HermesDailySummaryResponse>("/hermes/daily-summary"),
  hermesFeatureKanban: () =>
    request<HermesFeatureKanbanResponse>("/hermes/feature-kanban"),
  hermesEvidenceLedger: (days = 7, limit = 50) =>
    request<HermesEvidenceLedgerResponse>(`/hermes/evidence-ledger?days=${days}&limit=${limit}`),
  hermesSentinelStatus: () =>
    request<HermesSentinelStatusResponse>("/hermes/sentinel/status"),
  hermesSentinelNotifications: (status?: HermesSentinelMailboxStatus | "all", limit = 100) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (status && status !== "all") params.set("status", status);
    return request<HermesSentinelNotification[]>(`/hermes/sentinel/notifications?${params.toString()}`);
  },
  hermesSetSentinelNotificationStatus: (notificationId: string, status: HermesSentinelMailboxStatus) =>
    request<HermesSentinelNotification>(`/hermes/sentinel/notifications/${encodeURIComponent(notificationId)}/status`, {
      method: "POST",
      body: JSON.stringify({ status }),
    }),
  hermesDeployStatus: () =>
    request<HermesDeployStatusResponse>("/hermes/deploy/status"),
  hermesFullDesignDocument: () =>
    request<HermesFullDesignDocumentResponse>("/hermes/reports/full-design-document"),

  /* ── Hermes Chat ──────────────────────────────── */
  hermesChat: (payload: HermesChatRequest) =>
    request<HermesChatResponse>("/hermes/chat", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  hermesChatSessions: (limit = 20) =>
    request<HermesChatSession[]>(`/hermes/chat/sessions?limit=${limit}`),
  hermesChatSession: (sessionId: string) =>
    request<HermesChatSessionDetail>(`/hermes/chat/sessions/${encodeURIComponent(sessionId)}`),
  hermesCommands: () =>
    request<HermesCommand[]>("/hermes/commands"),
  hermesCommandExecute: (payload: HermesCommandExecuteRequest) =>
    request<HermesCommandExecuteResponse>("/hermes/commands/execute", {
      method: "POST",
      body: JSON.stringify(payload),
    }),

  patchItem: (id: string, payload: Partial<Omit<CrudItem, "id">>) =>
    request<{ item: CrudItem }>(`/crud/items/${id}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    }),
  deleteItem: (id: string) =>
    request<{ deleted: boolean }>(`/crud/items/${id}`, { method: "DELETE" }),

  /* ── Engineering ────────────────────────────────── */
  listProjects: (params?: {
    status?: string;
    brand?: string;
    market_country?: string;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.status) sp.set("status", params.status);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.market_country) sp.set("market_country", params.market_country);
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: Record<string, unknown>[] }>(
      `/engineering/projects${q ? `?${q}` : ""}`
    ).then((res) => ({
      items: res.items.map((item) => mapConfigProject(item))
    }));
  },
  createProject: (payload: {
    project_code: string;
    brand: string;
    model: string;
    market_country: string;
    display_name: string;
    status?: string;
  }) =>
    request<{ item: Record<string, unknown> }>("/engineering/projects", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapConfigProject(res.item) })),
  patchProject: (projectId: string, payload: {
    brand?: string;
    model?: string;
    market_country?: string;
    display_name?: string;
    status?: string;
  }) =>
    request<{ item: Record<string, unknown> }>(`/engineering/projects/${projectId}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapConfigProject(res.item) })),
  deleteProject: (projectId: string) =>
    request<{ item: Record<string, unknown> }>(`/engineering/projects/${projectId}`, {
      method: "DELETE"
    }).then((res) => ({ item: mapConfigProject(res.item) })),
  listImportBatches: (params?: {
    project_id?: string;
    import_status?: string;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.project_id) sp.set("project_id", params.project_id);
    if (params?.import_status) sp.set("import_status", params.import_status);
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: ConfigImportBatch[] }>(
      `/engineering/projects/imports${q ? `?${q}` : ""}`
    );
  },
  getImportPageData: (batchId: string, sampleLimit = 20) =>
    request<{ item: Record<string, unknown> }>(
      `/engineering/projects/imports/${batchId}/page-data?sample_limit=${sampleLimit}`
    ),
  listVariants: (params?: {
    project_id?: string;
    config_import_batch_id?: string;
    model?: string;
    market_country?: string;
    is_active?: boolean;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.project_id) sp.set("project_id", params.project_id);
    if (params?.config_import_batch_id) sp.set("config_import_batch_id", params.config_import_batch_id);
    if (params?.model) sp.set("model", params.model);
    if (params?.market_country) sp.set("market_country", params.market_country);
    if (params?.is_active !== undefined) sp.set("is_active", String(params.is_active));
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: ConfigVariant[] }>(
      `/engineering/projects/variants${q ? `?${q}` : ""}`
    );
  },

  /* ── Review Cases ──────────────────────────────── */
  listMatchOverrides: (params?: {
    country?: string;
    brand?: string;
    jato_model?: string;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: Record<string, unknown>[] }>(
      `/review/overrides${q ? `?${q}` : ""}`
    ).then((res) => ({
      items: res.items.map((item) => mapMatchOverride(item))
    }));
  },
  createMatchOverride: (payload: {
    country: string;
    brand: string;
    jato_model: string;
    jato_trim: string;
    jato_powertrain?: string | null;
    official_model: string;
    official_trim: string;
    valid_from_date: string;
    valid_to_date?: string | null;
    override_reason: string;
    created_by: string;
  }) =>
    request<{ item: Record<string, unknown> }>("/review/overrides", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapMatchOverride(res.item) })),
  patchMatchOverride: (overrideId: string, payload: {
    official_model?: string;
    official_trim?: string;
    jato_powertrain?: string | null;
    valid_from_date?: string;
    valid_to_date?: string | null;
    override_reason?: string;
    created_by?: string;
  }) =>
    request<{ item: Record<string, unknown> }>(`/review/overrides/${overrideId}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapMatchOverride(res.item) })),
  deleteMatchOverride: (overrideId: string) =>
    request<{ item: Record<string, unknown> }>(`/review/overrides/${overrideId}`, {
      method: "DELETE"
    }).then((res) => ({ item: mapMatchOverride(res.item) })),
  listReviewCases: (params?: {
    review_status?: string;
    country?: string;
    brand?: string;
    model?: string;
    current_assignee?: string;
    limit?: number;
    offset?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.review_status) sp.set("review_status", params.review_status);
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.model) sp.set("model", params.model);
    if (params?.current_assignee) sp.set("current_assignee", params.current_assignee);
    if (params?.limit) sp.set("limit", String(params.limit));
    if (params?.offset !== undefined) sp.set("offset", String(params.offset));
    const q = sp.toString();
    return request<{
      rows: number;
      total?: number;
      limit?: number;
      offset?: number;
      items: Record<string, unknown>[];
    }>(
      `/review/cases${q ? `?${q}` : ""}`
    ).then((res) => ({
      rows: Number(res.rows ?? res.items.length),
      total: Number(res.total ?? res.items.length),
      limit: Number(res.limit ?? params?.limit ?? res.items.length),
      offset: Number(res.offset ?? params?.offset ?? 0),
      items: res.items.map((item) => mapReviewCase(item))
    }));
  },
  getReviewCasesStats: () =>
    request<{ totalCountries: number; jatoCountries: number }>("/review/cases/stats"),
  getReviewCaseDetail: (id: string) =>
    request<{ item: Record<string, unknown> }>(`/review/cases/${id}`).then((res) => ({
      item: mapReviewCaseDetail(res.item)
    })),
  getReviewWorkbench: (params?: {
    country?: string;
    brand?: string;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    const q = sp.toString();
    return request<{ item: Record<string, unknown> }>(
      `/review/cases/workbench${q ? `?${q}` : ""}`
    ).then((res) => ({
      item: mapReviewWorkbench(res.item)
    }));
  },
  getMsrpDryrunDashboard: () =>
    request<Record<string, unknown>>("/msrp-dryrun/dashboard"),
  createReviewDecision: (caseId: string, payload: {
    decision: "approve" | "reject" | "remap";
    decided_official_model?: string;
    decided_official_trim?: string;
    note?: string;
    decided_by: string;
    persist_override?: boolean;
    override_reason?: string;
  }) =>
    request<{ item: ReviewDecision }>(`/review/cases/${caseId}/decisions`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),

  /* ── MSRP Current Prices ───────────────────────── */
  listMsrpSources: (params?: {
    source_code?: string;
    country?: string;
    brand?: string;
    source_type?: string;
    enabled?: boolean;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.source_code) sp.set("source_code", params.source_code);
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.source_type) sp.set("source_type", params.source_type);
    if (params?.enabled !== undefined) sp.set("enabled", String(params.enabled));
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: Record<string, unknown>[] }>(
      `/msrp/sources${q ? `?${q}` : ""}`
    ).then((res) => ({
      items: res.items.map((item) => mapMsrpSource(item))
    }));
  },
  createMsrpSource: (payload: {
    source_code: string;
    country: string;
    brand: string;
    source_url: string;
    source_type: string;
    tier?: number;
    extractor_name: string;
    extractor_version: string;
    price_semantics: string;
    requires_location?: boolean;
    enabled?: boolean;
    notes?: string | null;
  }) =>
    request<{ item: Record<string, unknown> }>("/msrp/sources", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapMsrpSource(res.item) })),
  patchMsrpSource: (sourceId: string, payload: {
    country?: string;
    brand?: string;
    source_url?: string;
    source_type?: string;
    tier?: number;
    extractor_name?: string;
    extractor_version?: string;
    price_semantics?: string;
    requires_location?: boolean;
    enabled?: boolean;
    notes?: string | null;
  }) =>
    request<{ item: Record<string, unknown> }>(`/msrp/sources/${sourceId}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapMsrpSource(res.item) })),
  deleteMsrpSource: (sourceId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/sources/${sourceId}`, {
      method: "DELETE"
    }).then((res) => ({ item: mapMsrpSource(res.item) })),
  listMsrpObservations: (params?: {
    scrape_batch_id?: string;
    country?: string;
    brand?: string;
    jato_model?: string;
    match_status?: string;
    source_code?: string;
    source_type?: string;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.scrape_batch_id) sp.set("scrape_batch_id", params.scrape_batch_id);
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.match_status) sp.set("match_status", params.match_status);
    if (params?.source_code) sp.set("source_code", params.source_code);
    if (params?.source_type) sp.set("source_type", params.source_type);
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: Record<string, unknown>[] }>(
      `/msrp/sources/observations${q ? `?${q}` : ""}`
    ).then((res) => ({
      items: res.items.map((item) => mapMsrpObservation(item))
    }));
  },
  createMsrpObservation: (payload: {
    source_id: string;
    country: string;
    brand: string;
    jato_model: string;
    jato_trim: string;
    jato_powertrain?: string | null;
    official_model: string;
    official_trim: string;
    official_edition?: string | null;
    official_powertrain?: string | null;
    msrp_value: number;
    currency: string;
    tax_included: boolean;
    price_label: string;
    availability_text?: string | null;
    observed_at_utc: string;
    source_url: string;
    source_snapshot_path?: string | null;
    source_payload_hash?: string | null;
    extraction_version: string;
    match_confidence: number;
    match_status: "auto_accepted" | "review_required" | "human_approved" | "rejected";
    match_reason_json?: Record<string, unknown> | null;
    source_context_json?: Record<string, unknown> | null;
    candidate_matches_json?: Record<string, unknown>[] | null;
  }) =>
    request<{ item: Record<string, unknown> }>("/msrp/sources/observations", {
      method: "POST",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapMsrpObservation(res.item) })),
  patchMsrpObservation: (observationId: string, payload: {
    source_id?: string;
    country?: string;
    brand?: string;
    jato_model?: string;
    jato_trim?: string;
    jato_powertrain?: string | null;
    official_model?: string;
    official_trim?: string;
    official_edition?: string | null;
    official_powertrain?: string | null;
    msrp_value?: number;
    currency?: string;
    tax_included?: boolean;
    price_label?: string;
    availability_text?: string | null;
    observed_at_utc?: string;
    source_url?: string;
    source_snapshot_path?: string | null;
    source_payload_hash?: string | null;
    extraction_version?: string;
    match_confidence?: number;
    match_status?: "auto_accepted" | "review_required" | "human_approved" | "rejected" | "override_applied";
    match_reason_json?: Record<string, unknown> | null;
    source_context_json?: Record<string, unknown> | null;
    candidate_matches_json?: Record<string, unknown>[] | null;
  }) =>
    request<{ item: Record<string, unknown> }>(`/msrp/sources/observations/${observationId}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    }).then((res) => ({ item: mapMsrpObservation(res.item) })),
  deleteMsrpObservation: (observationId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/sources/observations/${observationId}`, {
      method: "DELETE"
    }).then((res) => ({ item: mapMsrpObservation(res.item) })),
  listCurrentPrices: (params?: {
    country?: string;
    brand?: string;
    jato_model?: string;
    limit?: number;
    offset?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.limit) sp.set("limit", String(params.limit));
    if (params?.offset !== undefined) sp.set("offset", String(params.offset));
    const q = sp.toString();
    return request<{
      rows: number;
      total?: number;
      limit?: number;
      offset?: number;
      priceAlertCount?: number;
      items: Record<string, unknown>[];
    }>(
      `/msrp/current-prices${q ? `?${q}` : ""}`
    ).then((res) => ({
      rows: Number(res.rows ?? res.items.length),
      total: Number(res.total ?? res.items.length),
      limit: Number(res.limit ?? params?.limit ?? res.items.length),
      offset: Number(res.offset ?? params?.offset ?? 0),
      priceAlertCount: Number(res.priceAlertCount ?? 0),
      items: res.items.map((item) => mapCurrentPrice(item))
    }));
  },
  listPriceHistory: (params?: {
    country?: string;
    brand?: string;
    jato_model?: string;
    jato_trim?: string;
    jato_powertrain?: string;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.jato_trim) sp.set("jato_trim", params.jato_trim);
    if (params?.jato_powertrain) sp.set("jato_powertrain", params.jato_powertrain);
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ items: Record<string, unknown>[] }>(
      `/msrp/price-history${q ? `?${q}` : ""}`
    ).then((res) => ({
      items: res.items.map((item) => mapPriceHistory(item))
    }));
  },
  materializeCurrentPrices: (payload: {
    country?: string;
    brand?: string;
    jato_model?: string;
    limit?: number;
  }) =>
    request<{ item: Record<string, unknown> }>("/msrp/current-prices/materialize", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  remapCurrentPrice: (currentPriceId: string, payload: {
    decided_by: string;
    note?: string;
  }) =>
    request<{ item: Record<string, unknown> }>(`/msrp/current-prices/${currentPriceId}/remap`, {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  createJatoMonthlyUpdateJob: async (
    file: File,
    onProgress?: (progress: JatoMonthlyUpdateUploadProgress) => void,
    month?: string,
  ) => {
    const probeSha256 = await sha256ForBlob(
      file.slice(0, Math.min(file.size, MONTHLY_UPDATE_RESUME_PROBE_BYTES))
    );
    const resumeKey = buildMonthlyUpdateUploadResumeKey({
      filename: file.name,
      sizeBytes: file.size,
      lastModified: file.lastModified,
      probeSha256,
    });
    onProgress?.({
      stage: "initiating",
      uploadedBytes: 0,
      totalBytes: file.size,
      uploadedChunks: 0,
      totalChunks: 0,
      chunkSize: 0,
      detail: "准备上传会话与续传信息",
    });

    let session: JatoMonthlyUpdateUploadSession | null = null;
    const storedUploadId = readStoredMonthlyUpdateUploadId(resumeKey);
    if (storedUploadId) {
      try {
        session = await getJatoMonthlyUpdateUploadSession(storedUploadId);
      } catch {
        clearStoredMonthlyUpdateUploadId(resumeKey);
      }
    }

    if (!session) {
      session = await initiateJatoMonthlyUpdateUploadSession(file, resumeKey);
    }
    writeStoredMonthlyUpdateUploadId(resumeKey, session.uploadId);

    onProgress?.({
      stage: session.receivedChunkCount > 0 ? "resuming" : "uploading",
      uploadedBytes: session.uploadedBytes,
      totalBytes: file.size,
      uploadedChunks: session.receivedChunkCount,
      totalChunks: session.totalChunks,
      chunkSize: session.chunkSize,
      detail: session.receivedChunkCount > 0
        ? `已恢复 ${session.receivedChunkCount}/${session.totalChunks} 个分片`
        : "开始分片上传",
    });

    const receivedChunks = new Set(session.receivedChunks);
    for (let chunkIndex = 0; chunkIndex < session.totalChunks; chunkIndex += 1) {
      const partNumber = chunkIndex + 1;
      if (receivedChunks.has(partNumber)) {
        continue;
      }
      const start = chunkIndex * session.chunkSize;
      const end = Math.min(file.size, start + session.chunkSize);
      const chunk = file.slice(start, end);
      const chunkSha256 = await sha256ForBlob(chunk);
      let uploaded = false;
      for (let attempt = 1; attempt <= MONTHLY_UPDATE_UPLOAD_MAX_ATTEMPTS; attempt += 1) {
        try {
          session = await requestJatoMonthlyUpdateUploadChunk(
            session.uploadId,
            partNumber,
            chunk,
            chunkSha256
          );
          uploaded = true;
          break;
        } catch (error) {
          if (attempt >= MONTHLY_UPDATE_UPLOAD_MAX_ATTEMPTS) {
            throw error;
          }
          onProgress?.({
            stage: "retrying",
            uploadedBytes: session.uploadedBytes,
            totalBytes: file.size,
            uploadedChunks: session.receivedChunkCount,
            totalChunks: session.totalChunks,
            chunkSize: session.chunkSize,
            detail: `分片 ${partNumber}/${session.totalChunks} 上传失败，正在第 ${attempt + 1} 次重试`,
          });
          await sleep(getMonthlyUpdateRetryDelayMs(attempt));
        }
      }
      if (!uploaded) {
        throw new Error(`分片 ${partNumber} 上传失败。`);
      }
      onProgress?.({
        stage: "uploading",
        uploadedBytes: session.uploadedBytes,
        totalBytes: file.size,
        uploadedChunks: session.receivedChunkCount,
        totalChunks: session.totalChunks,
        chunkSize: session.chunkSize,
        detail: `分片 ${partNumber}/${session.totalChunks} 已完成`,
      });
    }

    if (session.status !== "completed") {
      onProgress?.({
        stage: "assembling",
        uploadedBytes: file.size,
        totalBytes: file.size,
        uploadedChunks: session.totalChunks,
        totalChunks: session.totalChunks,
        chunkSize: session.chunkSize,
        detail: "服务端正在校验分片并组装整文件",
      });
      session = await completeJatoMonthlyUpdateUploadSession(session.uploadId);
    }

    onProgress?.({
      stage: "creating_job",
      uploadedBytes: file.size,
      totalBytes: file.size,
      uploadedChunks: session.totalChunks,
      totalChunks: session.totalChunks,
      chunkSize: session.chunkSize,
      detail: session.fileSha256
        ? `文件 SHA-256 ${session.fileSha256.slice(0, 12)}...`
        : "准备创建月更任务",
    });

    const item = await request<{ item: Record<string, unknown> }>("/msrp/monthly-update-jobs/from-upload", {
      method: "POST",
      body: JSON.stringify({
        uploadId: session.uploadId,
        month: month || undefined,
      }),
    }).then((res) => mapJatoMonthlyUpdateJob(res.item));
    clearStoredMonthlyUpdateUploadId(resumeKey);

    onProgress?.({
      stage: "queued",
      uploadedBytes: file.size,
      totalBytes: file.size,
      uploadedChunks: session.totalChunks,
      totalChunks: session.totalChunks,
      chunkSize: session.chunkSize,
      detail: item.upload?.sha256
        ? `已入队，文件指纹 ${item.upload.sha256.slice(0, 12)}...`
        : "已入队，后台开始执行脚本",
    });

    return { item };
  },
  listJatoMonthlyUpdateJobs: (limit = 20) =>
    request<{ rows: number; items: Record<string, unknown>[] }>(
      `/msrp/monthly-update-jobs?limit=${limit}`
    ).then((res) => ({
      rows: Number(res.rows ?? res.items.length),
      items: res.items.map((item) => mapJatoMonthlyUpdateJob(item))
    })),
  getJatoMonthlyUpdateJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}`).then((res) => ({
      item: mapJatoMonthlyUpdateJob(res.item)
    })),
  getJatoMonthlyUpdateReview: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/review`).then((res) => ({
      item: mapJatoMonthlyUpdateReviewBundle(res.item)
    })),
  retryFailedJatoMonthlyUpdateJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/retry`, {
      method: "POST"
    }).then((res) => ({
      item: mapJatoMonthlyUpdateJob(res.item)
    })),
  publishJatoMonthlyUpdateJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/publish`, {
      method: "POST"
    }).then((res) => ({
      item: mapJatoMonthlyUpdateJob(res.item)
    })),
  rollbackJatoMonthlyUpdateJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/rollback`, {
      method: "POST"
    }).then((res) => ({
      item: mapJatoMonthlyUpdateJob(res.item)
    })),
  smartMergeJatoMonthlyUpdateCandidate: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/smart-merge`, {
      method: "POST"
    }).then((res) => ({
      item: mapJatoMonthlyUpdateJob(res.item)
    })),
  getJatoMonthlyUpdateMaintenanceStatus: () =>
    request<{ item: Record<string, unknown> }>("/msrp/monthly-update-maintenance/status").then((res) => ({
      item: mapJatoMonthlyUpdateMaintenanceStatus(res.item)
    })),
  promoteCurrentActiveToJatoBaseline: () =>
    request<{ item: Record<string, unknown> }>("/msrp/monthly-update-maintenance/promote-baseline", {
      method: "POST",
    }).then((res) => ({
      item: mapJatoMonthlyUpdateBaselinePromotionResult(res.item)
    })),
  runJatoMonthlyUpdateCleanup: (cleanupTier: "safe" | "cautious") =>
    request<{ item: Record<string, unknown> }>("/msrp/monthly-update-maintenance/cleanup", {
      method: "POST",
      body: JSON.stringify({ cleanupTier }),
    }).then((res) => ({
      item: mapJatoMonthlyUpdateCleanupResult(res.item)
    })),

  /* ── Engineering Config ────────────────────────── */

  listEngineeringConfigFeatureCatalog: (params?: { category?: string; is_active?: boolean; limit?: number }) => {
    const sp = new URLSearchParams();
    if (params?.category) sp.set("category", params.category);
    if (params?.is_active !== undefined) sp.set("is_active", String(params.is_active));
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{ rows: number; items: Record<string, unknown>[] }>(
      `/engineering-config/feature-catalog${q ? `?${q}` : ""}`
    );
  },

  initiateEngineeringConfigUpload: (fileName: string, totalSize: number, chunkSize?: number) => {
    const sp = new URLSearchParams();
    sp.set("file_name", fileName);
    sp.set("total_size", String(totalSize));
    if (chunkSize) sp.set("chunk_size", String(chunkSize));
    return request<{ uploadId: string; totalChunks: number }>(
      `/engineering-config/matrix/upload/initiate?${sp.toString()}`,
      { method: "POST" }
    );
  },

  uploadEngineeringConfigChunk: (uploadId: string, partNumber: number, chunk: Blob) =>
    request<{ uploadId: string; partNumber: number; receivedBytes: number }>(
      `/engineering-config/matrix/upload/${uploadId}/parts/${partNumber}`,
      { method: "PUT", body: chunk, headers: { "Content-Type": "application/octet-stream" } }
    ),

  completeEngineeringConfigUpload: (uploadId: string) =>
    request<Record<string, unknown>>(
      `/engineering-config/matrix/upload/${uploadId}/complete`,
      { method: "POST" }
    ),

  parseEngineeringConfigUpload: (uploadId: string) =>
    request<Record<string, unknown>>(
      `/engineering-config/matrix/upload/${uploadId}/parse`,
      { method: "POST" }
    ) as Promise<Record<string, unknown>>,

  importEngineeringConfigMatrix: (uploadId: string) =>
    request<Record<string, unknown>>(
      `/engineering-config/matrix/upload/${uploadId}/import`,
      { method: "POST" }
    ),

  matchEngineeringConfigUpload: (uploadId: string) =>
    request<Record<string, unknown>>(`/engineering-config/matrix/upload/${uploadId}/match`, { method: "POST" }),

  getEngineeringConfigUploadPreview: (uploadId: string) =>
    request<Record<string, unknown>>(`/engineering-config/matrix/upload/${uploadId}/preview`),

  confirmEngineeringConfigUpload: (uploadId: string) =>
    request<Record<string, unknown>>(`/engineering-config/matrix/upload/${uploadId}/confirm`, { method: "POST" }),

  publishEngineeringConfigVersion: (versionId: string) =>
    request<Record<string, unknown>>(`/engineering-config/versions/${versionId}/publish`, { method: "POST" }),

  createEngineeringConfigFeatureValue: (payload: { trim_id: string; feature_id: string; raw_value: string; updated_by?: string }) =>
    request<Record<string, unknown>>("/engineering-config/values", { method: "POST", body: JSON.stringify(payload) }),

  deleteEngineeringConfigFeatureValue: (valueId: string) =>
    request<Record<string, unknown>>(`/engineering-config/values/${valueId}`, { method: "DELETE" }),

  updateEngineeringConfigTrim: (trimId: string, payload: { brand?: string; model_name?: string; trim_name?: string; energy_type?: string; drivetrain?: string; engine?: string; model_year?: string; status?: string }) =>
    request<Record<string, unknown>>(`/engineering-config/trims/${trimId}`, { method: "PATCH", body: JSON.stringify(payload) }),

  listEngineeringConfigTrims: (params?: { brand?: string; model_name?: string; status?: string; limit?: number }) => {
    const sp = new URLSearchParams();
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.model_name) sp.set("model_name", params.model_name);
    if (params?.status) sp.set("status", params.status);
    if (params?.limit) sp.set("limit", String(params.limit ?? 200));
    const q = sp.toString();
    return request<{ rows: number; items: Record<string, unknown>[] }>(
      `/engineering-config/trims${q ? `?${q}` : ""}`
    );
  },

  getEngineeringConfigTrimDetail: (trimId: string) =>
    request<Record<string, unknown>>(`/engineering-config/trims/${trimId}`),

  compareEngineeringConfigTrims: (trimIds: string[], differencesOnly?: boolean) => {
    const sp = new URLSearchParams();
    sp.set("trim_ids", trimIds.join(","));
    if (differencesOnly) sp.set("differences_only", "true");
    return request<Record<string, unknown>>(
      `/engineering-config/compare?${sp.toString()}`
    );
  },

  updateEngineeringConfigFeatureValue: (valueId: string, payload: {
    raw_value?: string;
    updated_by?: string;
    expected_version: number;
    comment?: string;
  }) =>
    request<Record<string, unknown>>(`/engineering-config/values/${valueId}`, {
      method: "PATCH",
      body: JSON.stringify(payload),
    }),

  listEngineeringConfigAuditLog: (params?: { entity_type?: string; entity_id?: string; limit?: number }) => {
    const sp = new URLSearchParams();
    if (params?.entity_type) sp.set("entity_type", params.entity_type);
    if (params?.entity_id) sp.set("entity_id", params.entity_id);
    if (params?.limit) sp.set("limit", String(params.limit ?? 200));
    const q = sp.toString();
    return request<{ rows: number; items: Record<string, unknown>[] }>(
      `/engineering-config/audit-log${q ? `?${q}` : ""}`
    );
  },

  /* ── Role Upgrade ──────────────────────────── */

  requestRoleUpgrade: (payload: { requested_role: string; reason?: string }) =>
    request<Record<string, unknown>>("/auth/role-upgrade/request", {
      method: "POST", body: JSON.stringify(payload),
    }),

  listRoleUpgradeRequests: (params?: { status?: string }) => {
    const sp = new URLSearchParams();
    if (params?.status) sp.set("status", params.status);
    const q = sp.toString();
    return request<Record<string, unknown>>(
      `/auth/role-upgrade/requests${q ? `?${q}` : ""}`
    );
  },

  reviewRoleUpgradeRequest: (requestId: string, payload: { status: string }) =>
    request<Record<string, unknown>>(
      `/auth/role-upgrade/requests/${requestId}`,
      { method: "PATCH", body: JSON.stringify(payload) }
    ),

  /* ── COC Match ────────────────────────────── */

  /** Simple POST — both files < 50 MB. */
  cocMatchCreateJob: (
    excel: File,
    archive: File,
    country: string,
    fileExt: string,
    month?: string,
  ) => {
    const fd = new FormData();
    fd.append("excel", excel);
    fd.append("archive", archive);
    fd.append("country", country);
    fd.append("file_ext", fileExt);
    if (month) fd.append("month", month);
    return request<{ item: Record<string, unknown> }>("/coc-match/jobs", {
      method: "POST",
      body: fd,
    }).then((res) => ({ item: mapCocMatchJob(res.item) }));
  },

  /** Initiate a chunked upload session for a large file. */
  cocMatchInitiateUpload: (
    filename: string,
    sizeBytes: number,
    resumeKey?: string,
  ) =>
    request<{ item: Record<string, unknown> }>(
      "/coc-match/upload-sessions/initiate",
      {
        method: "POST",
        body: JSON.stringify({
          filename,
          sizeBytes,
          resumeKey: resumeKey || undefined,
        }),
      }
    ).then((res) => res.item),

  /** Upload a single chunk. Returns the updated session state. */
  cocMatchUploadChunk: async (
    uploadId: string,
    partNumber: number,
    blob: Blob,
  ): Promise<Record<string, unknown>> => {
    const sha256 = await crypto.subtle.digest("SHA-256", await blob.arrayBuffer())
      .then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    return request<{ item: Record<string, unknown> }>(
      `/coc-match/upload-sessions/${uploadId}/parts/${partNumber}`,
      {
        method: "PUT",
        body: blob,
        headers: { "X-Chunk-SHA256": sha256, "Content-Type": "application/octet-stream" },
      }
    ).then((res) => res.item);
  },

  /** Complete a chunked upload session. */
  cocMatchCompleteUpload: (uploadId: string) =>
    request<{ item: Record<string, unknown> }>(
      `/coc-match/upload-sessions/${uploadId}/complete`,
      { method: "POST" }
    ).then((res) => res.item),

  /** Create a COC match job from two completed chunked upload sessions. */
  cocMatchCreateJobFromUpload: (
    excelUploadId: string,
    archiveUploadId: string,
    excelFilename: string,
    archiveFilename: string,
    country: string,
    fileExt: string,
    month?: string,
  ) => {
    const body: Record<string, unknown> = {
      excelUploadId,
      archiveUploadId,
      excelFilename,
      archiveFilename,
      country,
      fileExt,
    };
    if (month) body.month = month;
    return request<{ item: Record<string, unknown> }>("/coc-match/jobs/batch", {
      method: "POST",
      body: JSON.stringify(body),
    }).then((res) => ({ item: mapCocMatchJob(res.item) }));
  },

  /**
   * Smart upload: auto-decides simple POST vs chunked based on file size.
   * Files >= 50 MB are uploaded via chunked sessions; smaller files use direct POST.
   */
  cocMatchUploadAndCreateJob: async (
    excel: File,
    archive: File,
    country: string,
    fileExt: string,
    month?: string,
  ): Promise<{ item: CocMatchJob }> => {
    const CHUNK_THRESHOLD = 50 * 1024 * 1024; // 50 MB
    const CHUNK_SIZE = 8 * 1024 * 1024; // 8 MB

    const needsChunkedExcel = excel.size >= CHUNK_THRESHOLD;
    const needsChunkedArchive = archive.size >= CHUNK_THRESHOLD;

    // Small files: simple POST
    if (!needsChunkedExcel && !needsChunkedArchive) {
      return api.cocMatchCreateJob(excel, archive, country, fileExt, month);
    }

    // Large files: chunked upload per file
    const uploadFile = async (file: File): Promise<string> => {
      const session = await api.cocMatchInitiateUpload(
        file.name,
        file.size,
        `coc-resume-${file.name}-${file.size}`,
      );
      const uploadId = String(session.uploadId ?? session.uploadId);
      const received: number[] = (session.receivedChunks as number[]) || [];
      const totalChunks = Number(session.totalChunks ?? 1);

      for (let i = 1; i <= totalChunks; i++) {
        if (received.includes(i)) continue;
        const start = (i - 1) * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        const blob = file.slice(start, end);
        await api.cocMatchUploadChunk(uploadId, i, blob);
      }

      await api.cocMatchCompleteUpload(uploadId);
      return uploadId;
    };

    const excelUploadId = await uploadFile(excel);
    const archiveUploadId = await uploadFile(archive);

    return api.cocMatchCreateJobFromUpload(
      excelUploadId,
      archiveUploadId,
      excel.name,
      archive.name,
      country,
      fileExt,
      month,
    );
  },

  cocMatchListJobs: (limit = 20) =>
    request<{ items: Record<string, unknown>[] }>(
      `/coc-match/jobs?limit=${limit}`
    ),

  cocMatchGetJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/coc-match/jobs/${jobId}`)
      .then((res) => ({ item: mapCocMatchJob(res.item) })),

  cocMatchRetryJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(
      `/coc-match/jobs/${jobId}/retry`,
      { method: "POST" }
    ).then((res) => ({ item: mapCocMatchJob(res.item) })),
};
