import type {
  AdvancedChartResponse,
  AnalysisQuery,
  CocFillJob,
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
  HeroProductDeckRequest,
  HeroProductDeckResponse,
  HeroProductPriceOverridePayload,
  HeroProductSpecOverridePayload,
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
  MsrpFinanceObservation,
  MsrpFinanceObservationSummary,
  MsrpFinanceObservationsResponse,
  MsrpReconciliationItem,
  MsrpReconciliationResponse,
  MsrpReconciliationReviewQueueResponse,
  MsrpReconciliationReviewQueueSummary,
  MsrpReconciliationQueuedConflict,
  MsrpReconciliationSourceObservation,
  MsrpReconciliationSummary,
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
  HermesFeatureGoal,
  HermesFeatureGoalsResponse,
  HermesFeatureGoalSwimlanesResponse,
  HermesFeatureKanbanResponse,
  HermesFullDesignDocumentResponse,
  HermesGap,
  HermesHistoryClustersResponse,
  HermesHistoryEventsResponse,
  HermesHistoryLevel,
  HermesHistoryYAxis,
  HermesMermaidBlock,
  HermesOverviewResponse,
  HermesPipelineHealthResponse,
  HermesPipelineStatusRecord,
  HermesProgressFeature,
  HermesProgressSwimlaneResponse,
  HermesReuseCandidatesResponse,
  HermesDeployStatusResponse,
  HermesSentinelMailboxStatus,
  HermesSentinelNotification,
  HermesSentinelStatusResponse,
  HermesMsrpCountryProgressResponse,
  HermesMsrpDryrunHistoryResponse,
  HermesSourceQualityResponse,
  HermesToolchainResponse,
  HermesWorkflowCockpitResponse,
} from "../types/hermes";
import type {
  BaselineVersion,
  ColourHexRule,
  ColourSurchargeRule,
  CountryMaterialFinanceHistoryItem,
  CountryMaterialFinanceImportPreview,
  CountryMaterialFinanceImportRow,
  CountryMaterialFinanceRow,
  CountryMaterialFinanceUpdate,
  CountryPaymentTerm,
  MaterialUploadPreview,
  MaterialUploadPreviewRow,
  MaterialUploadSession,
  MatrixBatchResponse,
  MatrixResponse,
  OrderGeniusOptions,
  PaymentTermRule,
  PublishBaselineResponse,
  QuantityCellResponse,
  QuantityCellUpdate,
  QuantityImportPreview,
  QuantityImportResult,
  RemarkResponse,
  RemarkUpdate,
} from "../types/orderGenius";
import type {
  BulkVehicleUpdatePayload,
  BulkVehicleUpdateResult,
  PiOrderDetail,
  PiOrderFilters,
  PiOrderHeader,
  PiOrderLine,
  PiVehicleUnit,
  UpdateVehiclePayload,
  VehicleAllocationFilters,
  VehicleAllocationListResponse,
  VehicleAllocationPlan,
  VehicleAllocationSearchResult,
  VehicleImportPreview,
  VehicleImportResult,
} from "../types/orderGeniusVehicle";
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

export const API_BASE = import.meta.env.VITE_API_BASE ?? "/v1";
const MONTHLY_UPDATE_RESUME_PROBE_BYTES = 1024 * 1024;
const MONTHLY_UPDATE_UPLOAD_SESSION_STORAGE_PREFIX = "jato_monthly_update_upload_session:";
const MONTHLY_UPDATE_UPLOAD_MAX_ATTEMPTS = 4;

export function apiUrl(path: string): string {
  const normalizedBase = API_BASE.endsWith("/") ? API_BASE.slice(0, -1) : API_BASE;
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${normalizedBase}${normalizedPath}`;
}

interface FilterOptionsResponse {
  column: string;
  options: string[];
}

interface FilterOptionsBatchResponse {
  items: FilterOptionsResponse[];
}

interface FilterMetadataSnapshotResponse {
  columns: string[];
  options: Record<string, string[]>;
}

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
  const role = (
    localStorage.getItem("jato_user_role")
    || import.meta.env.VITE_USER_ROLE
    || "viewer"
  ).trim();

  return {
    ...(token ? { "X-Auth-Token": token } : {}),
    "X-User-Name": user || "anonymous",
    "X-User-Role": role || "viewer"
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
  const shouldDedupe = !(init?.body instanceof FormData) && !init?.signal;
  const key = shouldDedupe ? dedupeKey(path, init) : null;
  const inflight = key
    ? inflightRequests.get(key) as Promise<T> | undefined
    : undefined;
  if (inflight) return inflight;

  const promise = (async () => {
    let response: Response;
    try {
      response = await fetch(apiUrl(path), {
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

function normalizeQuantityCellResponse(raw: Record<string, unknown>): QuantityCellResponse {
  return {
    orderQuantityCellId: String(raw.orderQuantityCellId ?? raw.order_quantity_cell_id ?? ""),
    countryCode: String(raw.countryCode ?? raw.country_code ?? ""),
    orderYear: Number(raw.orderYear ?? raw.order_year ?? 0),
    orderMonth: Number(raw.orderMonth ?? raw.order_month ?? 0),
    materialCode: String(raw.materialCode ?? raw.material_code ?? ""),
    quantity: Number(raw.quantity ?? 0),
    fobEur: Number(raw.fobEur ?? raw.fob_eur ?? 0),
    rowVersion: Number(raw.rowVersion ?? raw.row_version ?? 0),
  };
}

async function requestBlob(path: string, init?: RequestInit): Promise<Blob> {
  let response: Response;
  try {
    response = await fetch(apiUrl(path), {
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

function appendSearchParam(
  params: URLSearchParams,
  key: string,
  value: string | number | boolean | null | undefined,
): void {
  if (value === null || value === undefined || value === "") {
    return;
  }
  params.set(key, String(value));
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
    candidateType: raw.candidateType === undefined || raw.candidateType === null ? undefined : String(raw.candidateType),
    reconciliationStatus: raw.reconciliationStatus === undefined || raw.reconciliationStatus === null ? undefined : String(raw.reconciliationStatus),
    recommendedAction: raw.recommendedAction === undefined || raw.recommendedAction === null ? undefined : String(raw.recommendedAction),
    thresholdPct: raw.thresholdPct === undefined || raw.thresholdPct === null ? undefined : Number(raw.thresholdPct),
    spreadPct: nullableNumber(raw.spreadPct),
    spreadValue: nullableNumber(raw.spreadValue),
    sourceRank: raw.sourceRank === undefined || raw.sourceRank === null ? undefined : Number(raw.sourceRank),
    observationId: raw.observationId === undefined || raw.observationId === null ? undefined : String(raw.observationId),
    sourceId: raw.sourceId === undefined || raw.sourceId === null ? undefined : String(raw.sourceId),
    sourceCode: raw.sourceCode === undefined || raw.sourceCode === null ? null : String(raw.sourceCode),
    sourceType: raw.sourceType === undefined || raw.sourceType === null ? null : String(raw.sourceType),
    sourceMsrpValue: raw.sourceMsrpValue === undefined || raw.sourceMsrpValue === null ? undefined : Number(raw.sourceMsrpValue),
    sourceCurrency: raw.sourceCurrency === undefined || raw.sourceCurrency === null ? undefined : String(raw.sourceCurrency),
    msrpValue: raw.msrpValue === undefined || raw.msrpValue === null ? undefined : Number(raw.msrpValue),
    observedAtUtc: raw.observedAtUtc === undefined || raw.observedAtUtc === null ? undefined : String(raw.observedAtUtc),
    sourceUrl: raw.sourceUrl === undefined || raw.sourceUrl === null ? undefined : String(raw.sourceUrl),
    matchStatus: raw.matchStatus === undefined || raw.matchStatus === null ? undefined : String(raw.matchStatus),
    matchConfidence: raw.matchConfidence === undefined || raw.matchConfidence === null ? undefined : Number(raw.matchConfidence),
    sourcePayloadHash: raw.sourcePayloadHash === undefined || raw.sourcePayloadHash === null ? null : String(raw.sourcePayloadHash),
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

function nullableNumber(value: unknown): number | null {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function nullableString(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  const parsed = String(value);
  return parsed.trim() ? parsed : null;
}

function mapMsrpFinanceObservation(raw: Record<string, unknown>): MsrpFinanceObservation {
  return {
    financeObservationId: String(raw.financeObservationId ?? ""),
    observationId: String(raw.observationId ?? ""),
    scrapeBatchId: String(raw.scrapeBatchId ?? ""),
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: nullableString(raw.jatoPowertrain),
    officialModel: String(raw.officialModel ?? ""),
    officialTrim: String(raw.officialTrim ?? ""),
    officialEdition: nullableString(raw.officialEdition),
    officialPowertrain: nullableString(raw.officialPowertrain),
    priceSemantics: String(raw.priceSemantics ?? ""),
    financeType: nullableString(raw.financeType),
    monthlyPayment: nullableNumber(raw.monthlyPayment),
    monthlyPaymentEur: nullableNumber(raw.monthlyPaymentEur),
    downPayment: nullableNumber(raw.downPayment),
    downPaymentEur: nullableNumber(raw.downPaymentEur),
    downPaymentPct: nullableNumber(raw.downPaymentPct),
    termMonths: nullableNumber(raw.termMonths),
    apr: nullableNumber(raw.apr),
    effectiveApr: nullableNumber(raw.effectiveApr),
    balloonPayment: nullableNumber(raw.balloonPayment),
    balloonPaymentEur: nullableNumber(raw.balloonPaymentEur),
    totalCreditCost: nullableNumber(raw.totalCreditCost),
    totalCreditCostEur: nullableNumber(raw.totalCreditCostEur),
    totalAmountPayable: nullableNumber(raw.totalAmountPayable),
    totalAmountPayableEur: nullableNumber(raw.totalAmountPayableEur),
    annualMileageLimit: nullableNumber(raw.annualMileageLimit),
    offerValidUntil: nullableString(raw.offerValidUntil),
    subsidyAmount: nullableNumber(raw.subsidyAmount),
    subsidyAmountEur: nullableNumber(raw.subsidyAmountEur),
    netPriceAfterSubsidy: nullableNumber(raw.netPriceAfterSubsidy),
    netPriceAfterSubsidyEur: nullableNumber(raw.netPriceAfterSubsidyEur),
    currency: String(raw.currency ?? ""),
    sourceUrl: String(raw.sourceUrl ?? ""),
    observedAtUtc: String(raw.observedAtUtc ?? ""),
    financeContext: raw.financeContext === undefined || raw.financeContext === null
      ? null
      : raw.financeContext as Record<string, unknown>,
    createdAtUtc: String(raw.createdAtUtc ?? ""),
    updatedAtUtc: String(raw.updatedAtUtc ?? ""),
  };
}

function mapMsrpFinanceSummary(
  raw: Record<string, unknown> | undefined,
): MsrpFinanceObservationSummary {
  const priceSemanticsCounts = (
    raw?.priceSemanticsCounts && typeof raw.priceSemanticsCounts === "object"
      ? raw.priceSemanticsCounts
      : {}
  ) as Record<string, number>;
  const financeTypeCounts = (
    raw?.financeTypeCounts && typeof raw.financeTypeCounts === "object"
      ? raw.financeTypeCounts
      : {}
  ) as Record<string, number>;
  return {
    priceSemanticsCounts,
    financeTypeCounts,
    monthlyPaymentCount: Number(raw?.monthlyPaymentCount ?? 0),
    monthlyPaymentEurMin: nullableNumber(raw?.monthlyPaymentEurMin),
    monthlyPaymentEurMax: nullableNumber(raw?.monthlyPaymentEurMax),
    netPriceAfterSubsidyCount: Number(raw?.netPriceAfterSubsidyCount ?? 0),
    netPriceAfterSubsidyEurMin: nullableNumber(raw?.netPriceAfterSubsidyEurMin),
    netPriceAfterSubsidyEurMax: nullableNumber(raw?.netPriceAfterSubsidyEurMax),
    subsidyObservationCount: Number(raw?.subsidyObservationCount ?? 0),
  };
}

function mapMsrpReconciliationSourceObservation(
  raw: Record<string, unknown>,
): MsrpReconciliationSourceObservation {
  return {
    observationId: String(raw.observationId ?? ""),
    sourceId: String(raw.sourceId ?? ""),
    sourceCode: nullableString(raw.sourceCode),
    sourceType: nullableString(raw.sourceType),
    sourceMsrpValue: Number(raw.sourceMsrpValue ?? 0),
    sourceCurrency: String(raw.sourceCurrency ?? ""),
    msrpValue: Number(raw.msrpValue ?? 0),
    currency: String(raw.currency ?? ""),
    observedAtUtc: String(raw.observedAtUtc ?? ""),
    sourceUrl: String(raw.sourceUrl ?? ""),
    matchStatus: String(raw.matchStatus ?? ""),
    matchConfidence: Number(raw.matchConfidence ?? 0),
    sourcePayloadHash: nullableString(raw.sourcePayloadHash),
  };
}

function mapMsrpReconciliationItem(
  raw: Record<string, unknown>,
): MsrpReconciliationItem {
  return {
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: nullableString(raw.jatoPowertrain),
    status: String(raw.status ?? ""),
    recommendedAction: String(raw.recommendedAction ?? ""),
    sourceCount: Number(raw.sourceCount ?? 0),
    observationCount: Number(raw.observationCount ?? 0),
    minMsrpValue: nullableNumber(raw.minMsrpValue),
    maxMsrpValue: nullableNumber(raw.maxMsrpValue),
    avgMsrpValue: nullableNumber(raw.avgMsrpValue),
    spreadValue: nullableNumber(raw.spreadValue),
    spreadPct: nullableNumber(raw.spreadPct),
    thresholdPct: Number(raw.thresholdPct ?? 0),
    currentPrice: raw.currentPrice && typeof raw.currentPrice === "object"
      ? mapCurrentPrice(raw.currentPrice as Record<string, unknown>)
      : null,
    sourceObservations: Array.isArray(raw.sourceObservations)
      ? raw.sourceObservations.map((item) => mapMsrpReconciliationSourceObservation(item as Record<string, unknown>))
      : [],
  };
}

function mapMsrpReconciliationSummary(
  raw: Record<string, unknown> | undefined,
): MsrpReconciliationSummary {
  const statusCounts = (
    raw?.statusCounts && typeof raw.statusCounts === "object"
      ? raw.statusCounts
      : {}
  ) as Record<string, number>;
  return {
    observationRows: Number(raw?.observationRows ?? 0),
    reconciliationGroupCount: Number(raw?.reconciliationGroupCount ?? 0),
    statusCounts,
    limit: Number(raw?.limit ?? 0),
  };
}

function mapMsrpQueuedConflict(
  raw: Record<string, unknown>,
): MsrpReconciliationQueuedConflict {
  return {
    country: String(raw.country ?? ""),
    brand: String(raw.brand ?? ""),
    jatoModel: String(raw.jatoModel ?? ""),
    jatoTrim: String(raw.jatoTrim ?? ""),
    jatoPowertrain: nullableString(raw.jatoPowertrain),
    sourceCount: Number(raw.sourceCount ?? 0),
    spreadPct: nullableNumber(raw.spreadPct),
    spreadValue: nullableNumber(raw.spreadValue),
    reviewObservationId: String(raw.reviewObservationId ?? ""),
  };
}

function mapMsrpReviewQueueSummary(
  raw: Record<string, unknown> | undefined,
): MsrpReconciliationReviewQueueSummary {
  return {
    observationRows: Number(raw?.observationRows ?? 0),
    reconciliationGroupCount: Number(raw?.reconciliationGroupCount ?? 0),
    conflictGroupCount: Number(raw?.conflictGroupCount ?? 0),
    reviewCasesQueued: Number(raw?.reviewCasesQueued ?? 0),
    reviewCasesCreated: Number(raw?.reviewCasesCreated ?? 0),
    reviewCasesReused: Number(raw?.reviewCasesReused ?? 0),
    limit: Number(raw?.limit ?? 0),
  };
}

function mapNullableFilterText(raw: Record<string, unknown>, key: string): string | null {
  return raw[key] === undefined || raw[key] === null ? null : String(raw[key]);
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
    extraFileCount: raw.extraFileCount === undefined ? undefined : Number(raw.extraFileCount),
    differenceType: raw.differenceType === undefined || raw.differenceType === null ? null : String(raw.differenceType),
    hasBidirectionalMismatch: Boolean(raw.hasBidirectionalMismatch),
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

function mapCocFillRecord(raw: Record<string, unknown>) {
  return {
    materialGroup: String(raw.materialGroup ?? raw.material_group ?? ""),
    wvtaNo: String(raw.wvtaNo ?? raw.wvta_no ?? ""),
    cocNo: String(raw.cocNo ?? raw.coc_no ?? ""),
    brand: nullableString(raw.brand),
    model: nullableString(raw.model),
    powertrain: nullableString(raw.powertrain),
    version: nullableString(raw.version),
    salesName: nullableString(raw.salesName ?? raw.sales_name),
    validFrom: nullableString(raw.validFrom ?? raw.valid_from),
    validTo: nullableString(raw.validTo ?? raw.valid_to),
    comments: nullableString(raw.comments),
    pageNumber: Number(raw.pageNumber ?? raw.page_number ?? 0),
    tableRowNumber: Number(raw.tableRowNumber ?? raw.table_row_number ?? 0),
  };
}

function mapCocFillDecision(raw: Record<string, unknown>) {
  const selectedRecordRaw = raw.selectedRecord ?? raw.selected_record;
  const candidateRecordsRaw = raw.candidateRecords ?? raw.candidate_records;
  return {
    materialGroup: String(raw.materialGroup ?? raw.material_group ?? ""),
    sheetName: String(raw.sheetName ?? raw.sheet_name ?? ""),
    rowNumber: Number(raw.rowNumber ?? raw.row_number ?? 0),
    status: String(raw.status ?? ""),
    candidateCount: Number(raw.candidateCount ?? raw.candidate_count ?? 0),
    reason: String(raw.reason ?? ""),
    confidence: Number(raw.confidence ?? 0),
    selectedRecord: selectedRecordRaw && typeof selectedRecordRaw === "object" && !Array.isArray(selectedRecordRaw)
      ? mapCocFillRecord(selectedRecordRaw as Record<string, unknown>)
      : null,
    candidateRecords: Array.isArray(candidateRecordsRaw)
      ? candidateRecordsRaw
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item))
        .map(mapCocFillRecord)
      : [],
    writtenWvta: nullableString(raw.writtenWvta ?? raw.written_wvta),
    writtenCoc: nullableString(raw.writtenCoc ?? raw.written_coc),
  };
}

function mapCocFillPreviewGroup(raw: Record<string, unknown>) {
  const statusCountsRaw = raw.statusCounts ?? raw.status_counts;
  const decisionsRaw = raw.decisions;
  const statusCounts: Record<string, number> = {};
  if (statusCountsRaw && typeof statusCountsRaw === "object" && !Array.isArray(statusCountsRaw)) {
    for (const [key, value] of Object.entries(statusCountsRaw)) {
      statusCounts[key] = Number(value);
    }
  }
  return {
    sheetName: String(raw.sheetName ?? raw.sheet_name ?? ""),
    totalRows: Number(raw.totalRows ?? raw.total_rows ?? 0),
    filledCount: Number(raw.filledCount ?? raw.filled_count ?? 0),
    notFoundCount: Number(raw.notFoundCount ?? raw.not_found_count ?? 0),
    ambiguousCount: Number(raw.ambiguousCount ?? raw.ambiguous_count ?? 0),
    skippedExistingCount: Number(raw.skippedExistingCount ?? raw.skipped_existing_count ?? 0),
    invalidSourceCount: Number(raw.invalidSourceCount ?? raw.invalid_source_count ?? 0),
    statusCounts,
    decisions: Array.isArray(decisionsRaw)
      ? decisionsRaw
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item))
        .map(mapCocFillDecision)
      : [],
    previewLimit: nullableNumber(raw.previewLimit ?? raw.preview_limit) ?? undefined,
    truncated: Boolean(raw.truncated),
  };
}

function mapCocFillJob(raw: Record<string, unknown>): CocFillJob {
  const statusCountsRaw = raw.statusCounts ?? raw.status_counts;
  const decisionsRaw = raw.decisions;
  const previewGroupsRaw = raw.previewGroups ?? raw.preview_groups;
  const sheetNamesRaw = raw.sheetNames ?? raw.sheet_names;
  const statusCounts: Record<string, number> = {};
  if (statusCountsRaw && typeof statusCountsRaw === "object" && !Array.isArray(statusCountsRaw)) {
    for (const [key, value] of Object.entries(statusCountsRaw)) {
      statusCounts[key] = Number(value);
    }
  }
  return {
    jobId: String(raw.jobId ?? raw.job_id ?? ""),
    jobType: String(raw.jobType ?? raw.job_type ?? "fill"),
    status: String(raw.status ?? ""),
    phase: String(raw.phase ?? ""),
    excelFilename: String(raw.excelFilename ?? raw.excel_filename ?? ""),
    pdfFilename: String(raw.pdfFilename ?? raw.pdf_filename ?? ""),
    overwriteExisting: Boolean(raw.overwriteExisting ?? raw.overwrite_existing),
    conflictStrategy: String(raw.conflictStrategy ?? raw.conflict_strategy ?? "strict"),
    includeResultSheet: Boolean(raw.includeResultSheet ?? raw.include_result_sheet),
    sheetNames: Array.isArray(sheetNamesRaw) ? sheetNamesRaw.map((item) => String(item)).filter(Boolean) : [],
    totalRows: nullableNumber(raw.totalRows ?? raw.total_rows),
    uniqueMaterialCount: nullableNumber(raw.uniqueMaterialCount ?? raw.unique_material_count),
    pdfRecordCount: nullableNumber(raw.pdfRecordCount ?? raw.pdf_record_count),
    filledCount: nullableNumber(raw.filledCount ?? raw.filled_count),
    notFoundCount: nullableNumber(raw.notFoundCount ?? raw.not_found_count),
    ambiguousCount: nullableNumber(raw.ambiguousCount ?? raw.ambiguous_count),
    skippedExistingCount: nullableNumber(raw.skippedExistingCount ?? raw.skipped_existing_count),
    invalidSourceCount: nullableNumber(raw.invalidSourceCount ?? raw.invalid_source_count),
    sheetCount: nullableNumber(raw.sheetCount ?? raw.sheet_count),
    statusCounts,
    decisions: Array.isArray(decisionsRaw)
      ? decisionsRaw
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item))
        .map(mapCocFillDecision)
      : [],
    previewGroups: Array.isArray(previewGroupsRaw)
      ? previewGroupsRaw
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object" && !Array.isArray(item))
        .map(mapCocFillPreviewGroup)
      : [],
    outputFilename: nullableString(raw.outputFilename ?? raw.output_filename),
    triggeredBy: String(raw.triggeredBy ?? raw.triggered_by ?? ""),
    error: raw.error === undefined || raw.error === null ? null : String(raw.error),
    createdAt: String(raw.createdAt ?? raw.created_at ?? ""),
    startedAt: nullableString(raw.startedAt ?? raw.started_at),
    finishedAt: nullableString(raw.finishedAt ?? raw.finished_at),
  };
}

function mapMaterialUploadSession(raw: Record<string, unknown>): MaterialUploadSession {
  const uploadedRaw = raw.uploadedChunks ?? raw.uploaded_chunks;
  return {
    uploadId: String(raw.uploadId ?? raw.upload_id ?? ""),
    fileName: String(raw.fileName ?? raw.file_name ?? ""),
    totalSize: Number(raw.totalSize ?? raw.total_size ?? 0),
    chunkSize: Number(raw.chunkSize ?? raw.chunk_size ?? 0),
    totalChunks: Number(raw.totalChunks ?? raw.total_chunks ?? 0),
    uploadedChunks: Array.isArray(uploadedRaw)
      ? uploadedRaw.map((item) => Number(item)).filter((item) => Number.isFinite(item))
      : [],
    status: String(raw.status ?? ""),
  };
}

function mapMaterialUploadPreviewRow(raw: Record<string, unknown>): MaterialUploadPreviewRow {
  const warningsRaw = raw.warnings;
  const interiorColorRaw = raw.interiorColorName ?? raw.interior_color_name;
  const bomTemplateRaw = raw.bomTemplate ?? raw.bom_template;
  const baseFobRaw = raw.baseFobEur ?? raw.base_fob_eur;
  return {
    rowIndex: Number(raw.rowIndex ?? raw.row_index ?? 0),
    sheetName: String(raw.sheetName ?? raw.sheet_name ?? ""),
    brand: String(raw.brand ?? ""),
    modelName: String(raw.modelName ?? raw.model_name ?? ""),
    version: String(raw.version ?? ""),
    exteriorColorName: String(raw.exteriorColorName ?? raw.exterior_color_name ?? ""),
    exteriorColorCode: String(raw.exteriorColorCode ?? raw.exterior_color_code ?? ""),
    exteriorColorType: String(raw.exteriorColorType ?? raw.exterior_color_type ?? ""),
    interiorColorName: interiorColorRaw === undefined || interiorColorRaw === null
      ? null
      : String(interiorColorRaw),
    bomTemplate: bomTemplateRaw === undefined || bomTemplateRaw === null
      ? null
      : String(bomTemplateRaw),
    materialCode: String(raw.materialCode ?? raw.material_code ?? ""),
    baseFobEur: baseFobRaw === undefined || baseFobRaw === null
      ? null
      : Number(baseFobRaw),
    powertrain: raw.powertrain === undefined || raw.powertrain === null ? null : String(raw.powertrain),
    warnings: Array.isArray(warningsRaw) ? warningsRaw.map((item) => String(item)) : [],
  };
}

function mapMaterialUploadPreview(raw: Record<string, unknown>): MaterialUploadPreview {
  const rowsRaw = raw.rows;
  const warningsRaw = raw.warnings;
  const sheetNamesRaw = raw.sheetNames ?? raw.sheet_names;
  return {
    uploadId: String(raw.uploadId ?? raw.upload_id ?? ""),
    totalRows: Number(raw.totalRows ?? raw.total_rows ?? 0),
    newSkus: Number(raw.newSkus ?? raw.new_skus ?? 0),
    existingSkus: Number(raw.existingSkus ?? raw.existing_skus ?? 0),
    sheetNames: Array.isArray(sheetNamesRaw) ? sheetNamesRaw.map((item) => String(item)) : [],
    rows: Array.isArray(rowsRaw)
      ? rowsRaw
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
        .map(mapMaterialUploadPreviewRow)
      : [],
    warnings: Array.isArray(warningsRaw) ? warningsRaw.map((item) => String(item)) : [],
  };
}

function mapCountryMaterialFinanceRow(raw: Record<string, unknown>): CountryMaterialFinanceRow {
  const sourcePayloadRaw = raw.sourcePayload ?? raw.source_payload;
  return {
    financeId: nullableString(raw.financeId ?? raw.finance_id),
    countryCode: String(raw.countryCode ?? raw.country_code ?? ""),
    materialCode: String(raw.materialCode ?? raw.material_code ?? ""),
    brand: String(raw.brand ?? ""),
    modelName: String(raw.modelName ?? raw.model_name ?? ""),
    version: String(raw.version ?? ""),
    powertrain: nullableString(raw.powertrain),
    colour: String(raw.colour ?? ""),
    colourCode: String(raw.colourCode ?? raw.colour_code ?? ""),
    bomTemplate: nullableString(raw.bomTemplate ?? raw.bom_template),
    bomFobEur: nullableNumber(raw.bomFobEur ?? raw.bom_fob_eur),
    fobEur: nullableNumber(raw.fobEur ?? raw.fob_eur),
    retailPriceEur: nullableNumber(raw.retailPriceEur ?? raw.retail_price_eur),
    wholesalePriceEur: nullableNumber(raw.wholesalePriceEur ?? raw.wholesale_price_eur),
    dealerPriceEur: nullableNumber(raw.dealerPriceEur ?? raw.dealer_price_eur),
    costEur: nullableNumber(raw.costEur ?? raw.cost_eur),
    marginEur: nullableNumber(raw.marginEur ?? raw.margin_eur),
    marginRate: nullableNumber(raw.marginRate ?? raw.margin_rate),
    vehicleMarginEur: nullableNumber(raw.vehicleMarginEur ?? raw.vehicle_margin_eur),
    vehicleMarginRate: nullableNumber(raw.vehicleMarginRate ?? raw.vehicle_margin_rate),
    vehicleProfitEur: nullableNumber(raw.vehicleProfitEur ?? raw.vehicle_profit_eur),
    vehicleProfitRate: nullableNumber(raw.vehicleProfitRate ?? raw.vehicle_profit_rate),
    fobDeltaEur: nullableNumber(raw.fobDeltaEur ?? raw.fob_delta_eur),
    marginDeltaEur: nullableNumber(raw.marginDeltaEur ?? raw.margin_delta_eur),
    memo: nullableString(raw.memo),
    sourceMode: nullableString(raw.sourceMode ?? raw.source_mode),
    sourcePayload: sourcePayloadRaw && typeof sourcePayloadRaw === "object" && !Array.isArray(sourcePayloadRaw)
      ? sourcePayloadRaw as Record<string, unknown>
      : null,
    updatedBy: nullableString(raw.updatedBy ?? raw.updated_by),
    updatedAtUtc: nullableString(raw.updatedAtUtc ?? raw.updated_at_utc),
  };
}

function mapCountryMaterialFinanceImportRow(raw: Record<string, unknown>): CountryMaterialFinanceImportRow {
  const updateRaw = raw.update;
  return {
    lineNumber: Number(raw.lineNumber ?? raw.line_number ?? 0),
    materialCode: String(raw.materialCode ?? raw.material_code ?? ""),
    update: updateRaw && typeof updateRaw === "object" && !Array.isArray(updateRaw)
      ? updateRaw as CountryMaterialFinanceUpdate
      : null,
    error: String(raw.error ?? ""),
  };
}

function mapCountryMaterialFinanceImportPreview(raw: Record<string, unknown>): CountryMaterialFinanceImportPreview {
  const rowsRaw = raw.rows;
  const warningsRaw = raw.warnings;
  return {
    rows: Array.isArray(rowsRaw)
      ? rowsRaw
        .filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object")
        .map(mapCountryMaterialFinanceImportRow)
      : [],
    warnings: Array.isArray(warningsRaw) ? warningsRaw.map((item) => String(item)) : [],
  };
}

function mapCountryMaterialFinanceHistoryItem(raw: Record<string, unknown>): CountryMaterialFinanceHistoryItem {
  const oldValuesRaw = raw.oldValues ?? raw.old_values;
  const newValuesRaw = raw.newValues ?? raw.new_values;
  const changedFieldsRaw = raw.changedFields ?? raw.changed_fields;
  const sourcePayloadRaw = raw.sourcePayload ?? raw.source_payload;
  return {
    historyId: String(raw.historyId ?? raw.history_id ?? ""),
    financeId: nullableString(raw.financeId ?? raw.finance_id),
    countryCode: String(raw.countryCode ?? raw.country_code ?? ""),
    materialCode: String(raw.materialCode ?? raw.material_code ?? ""),
    oldValues: oldValuesRaw && typeof oldValuesRaw === "object" && !Array.isArray(oldValuesRaw)
      ? oldValuesRaw as Record<string, unknown>
      : null,
    newValues: newValuesRaw && typeof newValuesRaw === "object" && !Array.isArray(newValuesRaw)
      ? newValuesRaw as Record<string, unknown>
      : {},
    changedFields: Array.isArray(changedFieldsRaw) ? changedFieldsRaw.map((item) => String(item)) : [],
    sourceMode: nullableString(raw.sourceMode ?? raw.source_mode),
    sourcePayload: sourcePayloadRaw && typeof sourcePayloadRaw === "object" && !Array.isArray(sourcePayloadRaw)
      ? sourcePayloadRaw as Record<string, unknown>
      : null,
    changedBy: nullableString(raw.changedBy ?? raw.changed_by),
    changedAtUtc: nullableString(raw.changedAtUtc ?? raw.changed_at_utc),
  };
}

function mapPublishBaselineResponse(raw: Record<string, unknown>): PublishBaselineResponse {
  return {
    baselineVersionId: String(raw.baselineVersionId ?? raw.baseline_version_id ?? ""),
    baselineName: String(raw.baselineName ?? raw.baseline_name ?? ""),
    skuCount: Number(raw.skuCount ?? raw.sku_count ?? 0),
    fobCount: Number(raw.fobCount ?? raw.fob_count ?? 0),
    status: String(raw.status ?? ""),
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
  const currentProcessRaw = raw.currentProcess && typeof raw.currentProcess === "object"
    ? raw.currentProcess as Record<string, unknown>
    : null;
  const runtimeCheckRaw = raw.runtimeCheck && typeof raw.runtimeCheck === "object"
    ? raw.runtimeCheck as Record<string, unknown>
    : null;
  const cancellationRaw = raw.cancellation && typeof raw.cancellation === "object"
    ? raw.cancellation as Record<string, unknown>
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
    currentProcess: currentProcessRaw ? {
      pid: Number(currentProcessRaw.pid ?? 0),
      label: String(currentProcessRaw.label ?? ""),
      command: String(currentProcessRaw.command ?? ""),
      startedAt: String(currentProcessRaw.startedAt ?? ""),
      lastHeartbeatAt: String(currentProcessRaw.lastHeartbeatAt ?? ""),
    } : null,
    runtimeCheck: runtimeCheckRaw ? {
      checkedAt: String(runtimeCheckRaw.checkedAt ?? ""),
      statusAtCheck: runtimeCheckRaw.statusAtCheck === undefined ? undefined : String(runtimeCheckRaw.statusAtCheck),
      phaseAtCheck: runtimeCheckRaw.phaseAtCheck === undefined ? undefined : String(runtimeCheckRaw.phaseAtCheck),
      threadAlive: runtimeCheckRaw.threadAlive === undefined ? undefined : Boolean(runtimeCheckRaw.threadAlive),
      processPid: runtimeCheckRaw.processPid === undefined || runtimeCheckRaw.processPid === null
        ? null
        : Number(runtimeCheckRaw.processPid),
      processAlive: runtimeCheckRaw.processAlive === undefined ? undefined : Boolean(runtimeCheckRaw.processAlive),
      log: runtimeCheckRaw.log && typeof runtimeCheckRaw.log === "object"
        ? runtimeCheckRaw.log as Record<string, unknown>
        : undefined,
      artifacts: Array.isArray(runtimeCheckRaw.artifacts)
        ? runtimeCheckRaw.artifacts.filter((item): item is Record<string, unknown> => (
            typeof item === "object" && item !== null
          ))
        : undefined,
      resolvedAs: runtimeCheckRaw.resolvedAs === undefined ? undefined : String(runtimeCheckRaw.resolvedAs),
      resolvedBy: runtimeCheckRaw.resolvedBy === undefined ? undefined : String(runtimeCheckRaw.resolvedBy),
      resolvedAt: runtimeCheckRaw.resolvedAt === undefined ? undefined : String(runtimeCheckRaw.resolvedAt),
    } : null,
    cancellation: cancellationRaw ? {
      cancelledAt: String(cancellationRaw.cancelledAt ?? ""),
      cancelledBy: String(cancellationRaw.cancelledBy ?? ""),
      phaseAtCancel: String(cancellationRaw.phaseAtCancel ?? ""),
      termination: cancellationRaw.termination && typeof cancellationRaw.termination === "object"
        ? cancellationRaw.termination as Record<string, unknown>
        : undefined,
    } : null,
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
    response = await fetch(
      apiUrl(`/msrp/monthly-update-uploads/${uploadId}/parts/${partNumber}`),
      {
        method: "PUT",
        headers: new Headers({
          ...getAuthHeaders(),
          "Content-Type": "application/octet-stream",
          "X-Chunk-SHA256": chunkSha256
        }),
        body: chunk
      },
    );
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
  // Generic typed HTTP helpers — use for endpoints without dedicated methods
  get: <T>(path: string, init?: RequestInit) => request<T>(path, { method: "GET", ...init }),
  post: <T>(path: string, body?: unknown, init?: RequestInit) =>
    request<T>(path, { method: "POST", body: body != null ? JSON.stringify(body) : undefined, ...init }),
  patch: <T>(path: string, body?: unknown, init?: RequestInit) =>
    request<T>(path, { method: "PATCH", body: body != null ? JSON.stringify(body) : undefined, ...init }),
  delete: <T>(path: string, init?: RequestInit) =>
    request<T>(path, { method: "DELETE", ...init }),

  columns: (init?: RequestInit) => request<{ items: string[] }>("/metadata/columns", init),
  filterMetadataSnapshot: (init?: RequestInit) =>
    request<FilterMetadataSnapshotResponse>("/metadata/filter-snapshot", init),
  filterOptions: (payload: FilterOptionsPayload, init?: RequestInit) =>
    request<FilterOptionsResponse>(
      "/filters/options",
      { method: "POST", body: JSON.stringify(payload), ...init }
    ),
  filterOptionsBatch: (items: FilterOptionsPayload[], init?: RequestInit) =>
    request<FilterOptionsBatchResponse>(
      "/filters/options/batch",
      { method: "POST", body: JSON.stringify({ items }), ...init }
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
  }, init?: RequestInit) =>
    request<OverviewResponse>("/analysis/overview", {
      method: "POST",
      body: JSON.stringify(payload),
      ...init,
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
    const response = await fetch(apiUrl("/assistant/country/chat/stream"), {
      method: "POST",
      headers: buildHeaders(
        { headers: { Accept: "text/event-stream" } },
        { includeJsonContentType: true }
      ),
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
  }, init?: RequestInit) =>
    request<GroupedTimeSeriesResponse>("/analysis/time-series-grouped", {
      method: "POST",
      body: JSON.stringify(payload),
      ...init,
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
  heroProductDeck: (payload: HeroProductDeckRequest = {}, signal?: AbortSignal) =>
    request<HeroProductDeckResponse>("/market-scan/hero-product-deck", {
      method: "POST",
      body: JSON.stringify(payload),
      signal,
    }),
  patchHeroProductPrice: (payload: HeroProductPriceOverridePayload) =>
    request<{ item: Record<string, unknown> }>("/market-scan/hero-product-price", {
      method: "PATCH",
      body: JSON.stringify(payload),
    }),
  patchHeroProductSpec: (payload: HeroProductSpecOverridePayload) =>
    request<{ item: Record<string, unknown> }>("/market-scan/hero-product-spec", {
      method: "PATCH",
      body: JSON.stringify(payload),
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
  hermesPipelineStatuses: () =>
    request<HermesPipelineStatusRecord[]>("/hermes/pipeline/status"),
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
    const params = new URLSearchParams();
    if (status) params.set("status", status);
    const q = params.toString();
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
  hermesHistoryEvents: (params?: {
    source?: string;
    workstream?: string;
    model?: string;
    limit?: number;
  }) => {
    const search = new URLSearchParams();
    if (params?.source) search.set("source", params.source);
    if (params?.workstream) search.set("workstream", params.workstream);
    if (params?.model) search.set("model", params.model);
    if (params?.limit) search.set("limit", String(params.limit));
    const q = search.toString();
    return request<HermesHistoryEventsResponse>(`/hermes/history/events${q ? `?${q}` : ""}`);
  },
  hermesHistoryClusters: (params?: {
    level?: HermesHistoryLevel;
    yAxis?: HermesHistoryYAxis;
    workstream?: string;
    limit?: number;
  }) => {
    const search = new URLSearchParams();
    if (params?.level) search.set("level", params.level);
    if (params?.yAxis) search.set("yAxis", params.yAxis);
    if (params?.workstream) search.set("workstream", params.workstream);
    if (params?.limit) search.set("limit", String(params.limit));
    const q = search.toString();
    return request<HermesHistoryClustersResponse>(`/hermes/history/clusters${q ? `?${q}` : ""}`);
  },
  hermesProgressFeatures: () =>
    request<HermesProgressFeature[]>("/hermes/progress/features"),
  hermesProgressSwimlanes: () =>
    request<HermesProgressSwimlaneResponse>("/hermes/progress/swimlanes"),
  hermesWorkflowCockpit: () =>
    request<HermesWorkflowCockpitResponse>("/hermes/workflow/cockpit"),
  hermesGoalFeatures: () =>
    request<HermesFeatureGoalsResponse>("/hermes/goals/features"),
  hermesGoalFeature: (featureId: string) =>
    request<HermesFeatureGoal>(`/hermes/goals/features/${encodeURIComponent(featureId)}`),
  hermesGoalSwimlanes: () =>
    request<HermesFeatureGoalSwimlanesResponse>("/hermes/goals/swimlanes"),
  hermesReuseCandidates: (featureId: string) =>
    request<HermesReuseCandidatesResponse>(`/hermes/reuse/candidates?featureId=${encodeURIComponent(featureId)}`),

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
  getMsrpDryrunDashboard: (runId?: string) =>
    request<Record<string, unknown>>(
      runId
        ? `/msrp-dryrun/dashboard?run_id=${encodeURIComponent(runId)}`
        : "/msrp-dryrun/dashboard"
    ),
  createReviewDecision: (caseId: string, payload: {
    decision: "approve" | "reject" | "remap";
    accepted_observation_id?: string;
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
  listMsrpFinanceObservations: (params?: {
    country?: string;
    brand?: string;
    jato_model?: string;
    price_semantics?: string;
    finance_type?: string;
    has_monthly_payment?: boolean;
    has_subsidy?: boolean;
    has_net_price_after_subsidy?: boolean;
    limit?: number;
    offset?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.price_semantics) sp.set("price_semantics", params.price_semantics);
    if (params?.finance_type) sp.set("finance_type", params.finance_type);
    if (params?.has_monthly_payment !== undefined) {
      sp.set("has_monthly_payment", String(params.has_monthly_payment));
    }
    if (params?.has_subsidy !== undefined) sp.set("has_subsidy", String(params.has_subsidy));
    if (params?.has_net_price_after_subsidy !== undefined) {
      sp.set("has_net_price_after_subsidy", String(params.has_net_price_after_subsidy));
    }
    if (params?.limit) sp.set("limit", String(params.limit));
    if (params?.offset !== undefined) sp.set("offset", String(params.offset));
    const q = sp.toString();
    return request<{
      rows?: unknown;
      total?: unknown;
      limit?: unknown;
      offset?: unknown;
      summary?: Record<string, unknown>;
      items?: Record<string, unknown>[];
      warning?: unknown;
    }>(`/msrp/finance-observations${q ? `?${q}` : ""}`).then((res) => ({
      rows: Number(res.rows ?? 0),
      total: Number(res.total ?? 0),
      limit: Number(res.limit ?? 0),
      offset: Number(res.offset ?? 0),
      summary: mapMsrpFinanceSummary(res.summary),
      items: (res.items ?? []).map((item) => mapMsrpFinanceObservation(item)),
      ...(typeof res.warning === "string" && res.warning ? { warning: res.warning } : {}),
    }));
  },
  listMsrpReconciliation: (params?: {
    country?: string;
    brand?: string;
    jato_model?: string;
    threshold_pct?: number;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.threshold_pct !== undefined) sp.set("threshold_pct", String(params.threshold_pct));
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{
      schemaVersion?: unknown;
      generatedAtUtc?: unknown;
      filters?: Record<string, unknown>;
      thresholdPct?: unknown;
      summary?: Record<string, unknown>;
      items?: Record<string, unknown>[];
    }>(`/msrp/reconciliation${q ? `?${q}` : ""}`).then((res) => {
      const filters = res.filters ?? {};
      return {
        schemaVersion: String(res.schemaVersion ?? ""),
        generatedAtUtc: String(res.generatedAtUtc ?? ""),
        filters: {
          country: mapNullableFilterText(filters, "country"),
          brand: mapNullableFilterText(filters, "brand"),
          jatoModel: mapNullableFilterText(filters, "jatoModel"),
        },
        thresholdPct: Number(res.thresholdPct ?? 0),
        summary: mapMsrpReconciliationSummary(res.summary),
        items: (res.items ?? []).map((item) => mapMsrpReconciliationItem(item)),
      };
    });
  },
  queueMsrpReconciliationReviewCases: (params?: {
    country?: string;
    brand?: string;
    jato_model?: string;
    threshold_pct?: number;
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.threshold_pct !== undefined) sp.set("threshold_pct", String(params.threshold_pct));
    if (params?.limit) sp.set("limit", String(params.limit));
    const q = sp.toString();
    return request<{
      item: {
        schemaVersion?: unknown;
        generatedAtUtc?: unknown;
        filters?: Record<string, unknown>;
        thresholdPct?: unknown;
        summary?: Record<string, unknown>;
        sampleConflicts?: Record<string, unknown>[];
        sampleReviewCases?: Record<string, unknown>[];
      };
    }>(`/msrp/reconciliation/review-cases${q ? `?${q}` : ""}`, {
      method: "POST",
    }).then((res) => {
      const item = res.item;
      const filters = item.filters ?? {};
      return {
        schemaVersion: String(item.schemaVersion ?? ""),
        generatedAtUtc: String(item.generatedAtUtc ?? ""),
        filters: {
          country: mapNullableFilterText(filters, "country"),
          brand: mapNullableFilterText(filters, "brand"),
          jatoModel: mapNullableFilterText(filters, "jatoModel"),
        },
        thresholdPct: Number(item.thresholdPct ?? 0),
        summary: mapMsrpReviewQueueSummary(item.summary),
        sampleConflicts: (item.sampleConflicts ?? []).map((conflict) => (
          mapMsrpQueuedConflict(conflict)
        )),
        sampleReviewCases: (item.sampleReviewCases ?? []).map((reviewCase) => (
          mapReviewCase(reviewCase)
        )),
      };
    });
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
  recheckJatoMonthlyUpdateJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/recheck`, {
      method: "POST"
    }).then((res) => ({
      item: mapJatoMonthlyUpdateJob(res.item)
    })),
  cancelJatoMonthlyUpdateJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/msrp/monthly-update-jobs/${jobId}/cancel`, {
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

  cocMatchListJobs: (limit = 20, country?: string) => {
    const qs = new URLSearchParams({ limit: String(limit) });
    const normalizedCountry = country?.trim().toUpperCase();
    if (normalizedCountry) qs.set("country", normalizedCountry);
    return request<{ items: Record<string, unknown>[] }>(
      `/coc-match/jobs?${qs.toString()}`
    ).then((res) => ({ items: res.items.map(mapCocMatchJob) }));
  },

  cocMatchGetJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/coc-match/jobs/${jobId}`)
      .then((res) => ({ item: mapCocMatchJob(res.item) })),

  cocMatchGetReport: (jobId: string, download = false) => {
    const qs = download ? "?download=1" : "";
    return requestBlob(`/coc-match/jobs/${encodeURIComponent(jobId)}/report${qs}`);
  },

  cocMatchRetryJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(
      `/coc-match/jobs/${jobId}/retry`,
      { method: "POST" }
    ).then((res) => ({ item: mapCocMatchJob(res.item) })),

  cocFillCreateJob: (
    excel: File,
    pdf: File,
    options?: { overwriteExisting?: boolean; conflictStrategy?: string; includeResultSheet?: boolean; sheetNames?: string[] },
  ) => {
    const fd = new FormData();
    fd.append("excel", excel);
    fd.append("pdf", pdf);
    fd.append("overwrite_existing", String(Boolean(options?.overwriteExisting)));
    fd.append("conflict_strategy", options?.conflictStrategy || "strict");
    fd.append("include_result_sheet", String(Boolean(options?.includeResultSheet)));
    if (options?.sheetNames?.length) {
      fd.append("sheet_names", options.sheetNames.join(","));
    }
    return request<{ item: Record<string, unknown> }>("/coc-match/fill/jobs", {
      method: "POST",
      body: fd,
    }).then((res) => ({ item: mapCocFillJob(res.item) }));
  },

  cocFillInitiateUpload: (
    filename: string,
    sizeBytes: number,
    resumeKey?: string,
  ) =>
    request<{ item: Record<string, unknown> }>(
      "/coc-match/fill/upload-sessions/initiate",
      {
        method: "POST",
        body: JSON.stringify({
          filename,
          sizeBytes,
          resumeKey: resumeKey || undefined,
        }),
      }
    ).then((res) => res.item),

  cocFillUploadChunk: async (
    uploadId: string,
    partNumber: number,
    blob: Blob,
  ): Promise<Record<string, unknown>> => {
    const sha256 = await crypto.subtle.digest("SHA-256", await blob.arrayBuffer())
      .then((buf) => Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join(""));
    return request<{ item: Record<string, unknown> }>(
      `/coc-match/fill/upload-sessions/${uploadId}/parts/${partNumber}`,
      {
        method: "PUT",
        body: blob,
        headers: { "X-Chunk-SHA256": sha256, "Content-Type": "application/octet-stream" },
      }
    ).then((res) => res.item);
  },

  cocFillCompleteUpload: (uploadId: string) =>
    request<{ item: Record<string, unknown> }>(
      `/coc-match/fill/upload-sessions/${uploadId}/complete`,
      { method: "POST" }
    ).then((res) => res.item),

  cocFillCreateJobFromUpload: (
    excelUploadId: string,
    pdfUploadId: string,
    excelFilename: string,
    pdfFilename: string,
    options?: { overwriteExisting?: boolean; conflictStrategy?: string; includeResultSheet?: boolean; sheetNames?: string[] },
  ) => request<{ item: Record<string, unknown> }>("/coc-match/fill/jobs/batch", {
    method: "POST",
    body: JSON.stringify({
      excelUploadId,
      pdfUploadId,
      excelFilename,
      pdfFilename,
      overwriteExisting: Boolean(options?.overwriteExisting),
      conflictStrategy: options?.conflictStrategy || "strict",
      includeResultSheet: Boolean(options?.includeResultSheet),
      sheetNames: options?.sheetNames || [],
    }),
  }).then((res) => ({ item: mapCocFillJob(res.item) })),

  cocFillUploadAndCreateJob: async (
    excel: File,
    pdf: File,
    options?: { overwriteExisting?: boolean; conflictStrategy?: string; includeResultSheet?: boolean; sheetNames?: string[] },
  ): Promise<{ item: CocFillJob }> => {
    const CHUNK_THRESHOLD = 50 * 1024 * 1024;
    const CHUNK_SIZE = 8 * 1024 * 1024;
    if (excel.size < CHUNK_THRESHOLD && pdf.size < CHUNK_THRESHOLD) {
      return api.cocFillCreateJob(excel, pdf, options);
    }

    const uploadFile = async (file: File): Promise<string> => {
      const session = await api.cocFillInitiateUpload(
        file.name,
        file.size,
        `coc-fill-resume-${file.name}-${file.size}`,
      );
      const uploadId = String(session.uploadId ?? "");
      const received: number[] = Array.isArray(session.receivedChunks)
        ? session.receivedChunks.map((item) => Number(item))
        : [];
      const totalChunks = Number(session.totalChunks ?? 1);
      for (let i = 1; i <= totalChunks; i++) {
        if (received.includes(i)) continue;
        const start = (i - 1) * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, file.size);
        await api.cocFillUploadChunk(uploadId, i, file.slice(start, end));
      }
      await api.cocFillCompleteUpload(uploadId);
      return uploadId;
    };

    const excelUploadId = await uploadFile(excel);
    const pdfUploadId = await uploadFile(pdf);
    return api.cocFillCreateJobFromUpload(
      excelUploadId,
      pdfUploadId,
      excel.name,
      pdf.name,
      options,
    );
  },

  cocFillListJobs: (limit = 50) =>
    request<{ items: Record<string, unknown>[] }>(
      `/coc-match/fill/jobs?limit=${encodeURIComponent(String(limit))}`
    ).then((res) => ({ items: res.items.map(mapCocFillJob) })),

  cocFillGetJob: (jobId: string) =>
    request<{ item: Record<string, unknown> }>(`/coc-match/fill/jobs/${jobId}`)
      .then((res) => ({ item: mapCocFillJob(res.item) })),

  cocFillApplyOverrides: (
    jobId: string,
    overrides: Array<{
      sheetName: string;
      rowNumber: number;
      materialGroup: string;
      wvtaNo: string;
      cocNo: string;
      pageNumber?: number;
      tableRowNumber?: number;
    }>,
  ) =>
    request<{ item: Record<string, unknown> }>(
      `/coc-match/fill/jobs/${encodeURIComponent(jobId)}/overrides`,
      {
        method: "POST",
        body: JSON.stringify({ overrides }),
      },
    ).then((res) => ({ item: mapCocFillJob(res.item) })),

  cocFillRevertOverrides: (
    jobId: string,
    overrides: Array<{
      sheetName: string;
      rowNumber: number;
      materialGroup: string;
    }>,
  ) =>
    request<{ item: Record<string, unknown> }>(
      `/coc-match/fill/jobs/${encodeURIComponent(jobId)}/overrides/revert`,
      {
        method: "POST",
        body: JSON.stringify({ overrides }),
      },
    ).then((res) => ({ item: mapCocFillJob(res.item) })),

  cocFillGetWorkbook: (jobId: string) =>
    requestBlob(`/coc-match/fill/jobs/${encodeURIComponent(jobId)}/workbook`),

  // ── Order Genius ────────────────────────────────────────────────

  initiateMaterialMasterUpload: (fileName: string, totalSize: number) =>
    request<Record<string, unknown>>(
      `/order-genius/material-master-uploads/initiate?file_name=${encodeURIComponent(fileName)}&total_size=${totalSize}`,
      { method: "POST" }
    ).then(mapMaterialUploadSession),

  uploadMaterialMasterChunk: async (
    uploadId: string,
    partNumber: number,
    blob: Blob,
  ) =>
    request<Record<string, unknown>>(
      `/order-genius/material-master-uploads/${uploadId}/parts/${partNumber}`,
      {
        method: "PUT",
        body: blob,
        headers: { "Content-Type": "application/octet-stream" },
      },
    ),

  completeMaterialMasterUpload: (uploadId: string) =>
    request<Record<string, unknown>>(
      `/order-genius/material-master-uploads/${uploadId}/complete`,
      { method: "POST" },
    ),

  parseMaterialMasterUpload: (uploadId: string) =>
    request<Record<string, unknown>>(
      `/order-genius/material-master-uploads/${uploadId}/parse`,
      { method: "POST" },
    ),

  getMaterialMasterPreview: (uploadId: string) =>
    request<Record<string, unknown>>(
      `/order-genius/material-master-uploads/${uploadId}/preview`,
    ).then(mapMaterialUploadPreview),

  publishMaterialMaster: (uploadId: string, notes?: string) =>
    request<Record<string, unknown>>(
      `/order-genius/material-master-uploads/${uploadId}/publish`,
      { method: "POST", body: JSON.stringify({ notes }) },
    ).then(mapPublishBaselineResponse),

  getOrderGeniusOptions: (params: {
    country: string;
    brand?: string;
    model?: string;
    powertrain?: string;
    version?: string;
    colour?: string;
  }) => {
    const qs = new URLSearchParams({ country: params.country });
    if (params.brand) qs.set("brand", params.brand);
    if (params.model) qs.set("model", params.model);
    if (params.powertrain) qs.set("powertrain", params.powertrain);
    if (params.version) qs.set("version", params.version);
    if (params.colour) qs.set("colour", params.colour);
    return request<OrderGeniusOptions>(
      `/order-genius/options?${qs.toString()}`,
    );
  },

  getOrderGeniusMatrix: (params: {
    country: string;
    year: number;
    brand?: string;
    model?: string;
    powertrain?: string;
    version?: string;
    colour?: string;
    materialCodeSearch?: string;
  }) => {
    const qs = new URLSearchParams();
    qs.set("country", params.country);
    qs.set("year", String(params.year));
    if (params.brand) qs.set("brand", params.brand);
    if (params.model) qs.set("model", params.model);
    if (params.powertrain) qs.set("powertrain", params.powertrain);
    if (params.version) qs.set("version", params.version);
    if (params.colour) qs.set("colour", params.colour);
    if (params.materialCodeSearch)
      qs.set("material_code_search", params.materialCodeSearch);
    return request<MatrixResponse>(
      `/order-genius/matrix?${qs.toString()}`,
    );
  },

  getOrderGeniusMatrixBatch: (params: {
    countries: string[];
    year: number;
    brand?: string;
    model?: string;
    powertrain?: string;
    version?: string;
    colour?: string;
    materialCodeSearch?: string;
  }) =>
    request<MatrixBatchResponse>("/order-genius/matrix/batch", {
      method: "POST",
      body: JSON.stringify({
        countries: params.countries,
        year: params.year,
        brand: params.brand,
        model: params.model,
        powertrain: params.powertrain,
        version: params.version,
        colour: params.colour,
        materialCodeSearch: params.materialCodeSearch,
      }),
    }),

  updateQuantityCell: async (payload: QuantityCellUpdate) =>
    normalizeQuantityCellResponse(await request<Record<string, unknown>>("/order-genius/quantity-cell", {
      method: "PATCH",
      body: JSON.stringify(payload),
    })),

  updateSkuRemark: (materialCode: string, payload: RemarkUpdate) =>
    request<RemarkResponse>(
      `/order-genius/material-skus/${encodeURIComponent(materialCode)}/remark`,
      { method: "PATCH", body: JSON.stringify(payload) },
    ),

  getSkuFob: (materialCode: string, country: string) =>
    request<Record<string, unknown>>(
      `/order-genius/material-skus/${encodeURIComponent(materialCode)}/fob?country=${encodeURIComponent(country)}`,
    ),

  listCountryMaterialFinance: (params: {
    country: string;
    brand?: string;
    model?: string;
    powertrain?: string;
    version?: string;
    materialCodes?: string[];
  }) => {
    const qs = new URLSearchParams({ country: params.country });
    if (params.brand) qs.set("brand", params.brand);
    if (params.model) qs.set("model", params.model);
    if (params.powertrain) qs.set("powertrain", params.powertrain);
    if (params.version) qs.set("version", params.version);
    for (const materialCode of params.materialCodes || []) {
      qs.append("material_code", materialCode);
    }
    return request<{ items: Record<string, unknown>[] }>(
      `/order-genius/country-material-finance?${qs.toString()}`,
    ).then((response) => ({
      items: response.items.map(mapCountryMaterialFinanceRow),
    }));
  },

  getCountryMaterialFinanceOptions: (params: {
    country: string;
    brand?: string;
    model?: string;
    powertrain?: string;
    version?: string;
  }) => {
    const qs = new URLSearchParams({ country: params.country });
    if (params.brand) qs.set("brand", params.brand);
    if (params.model) qs.set("model", params.model);
    if (params.powertrain) qs.set("powertrain", params.powertrain);
    if (params.version) qs.set("version", params.version);
    return request<OrderGeniusOptions>(
      `/order-genius/country-material-finance/options?${qs.toString()}`,
    );
  },

  listCountryMaterialFinanceHistory: (params: {
    country: string;
    materialCode: string;
    limit?: number;
  }) => {
    const qs = new URLSearchParams({
      country: params.country,
      material_code: params.materialCode,
    });
    if (params.limit) qs.set("limit", String(params.limit));
    return request<{ items: Record<string, unknown>[] }>(
      `/order-genius/country-material-finance/history?${qs.toString()}`,
    ).then((response) => ({
      items: response.items.map(mapCountryMaterialFinanceHistoryItem),
    }));
  },

  previewCountryMaterialFinanceImport: (country: string, payload: { file?: File; text?: string }) => {
    const formData = new FormData();
    formData.set("country", country);
    if (payload.file) formData.set("file", payload.file);
    if (payload.text) formData.set("text", payload.text);
    return request<Record<string, unknown>>(
      "/order-genius/country-material-finance/import-preview",
      {
        method: "POST",
        body: formData,
      },
    ).then(mapCountryMaterialFinanceImportPreview);
  },

  getMaterialCountryFinance: (materialCode: string, country: string) =>
    request<Record<string, unknown>>(
      `/order-genius/material-skus/${encodeURIComponent(materialCode)}/country-finance?country=${encodeURIComponent(country)}`,
    ).then(mapCountryMaterialFinanceRow),

  updateMaterialCountryFinance: (materialCode: string, payload: CountryMaterialFinanceUpdate) =>
    request<Record<string, unknown>>(
      `/order-genius/material-skus/${encodeURIComponent(materialCode)}/country-finance`,
      {
        method: "PATCH",
        body: JSON.stringify(payload),
      },
    ).then(mapCountryMaterialFinanceRow),

  exportOrderGenius: (country: string, year: number, opts?: { brand?: string; model?: string; powertrain?: string; version?: string; colour?: string; materialCodeSearch?: string; selectedMonth?: number; hideEmptyRows?: boolean; quantitiesOnly?: boolean }) =>
    requestBlob("/order-genius/export", {
      method: "POST",
      body: JSON.stringify({ country, year, ...opts }),
      headers: { "Content-Type": "application/json" },
    }),

  exportOrderGeniusPi: (country: string, year: number, opts?: { brand?: string; model?: string; powertrain?: string; version?: string; colour?: string; materialCodeSearch?: string; selectedMonth?: number; hideEmptyRows?: boolean }) =>
    requestBlob("/order-genius/export-pi", {
      method: "POST",
      body: JSON.stringify({ country, year, ...opts }),
      headers: { "Content-Type": "application/json" },
    }),

  getOrderGeniusPaymentTerms: () =>
    request<{ items: PaymentTermRule[] }>("/order-genius/payment-terms"),

  getOrderGeniusColourSurcharges: () =>
    request<{ items: ColourSurchargeRule[] }>(
      "/order-genius/colour-surcharges",
    ),

  updateOrderGeniusColourSurcharge: (body: { brand: string; colourType: string; surchargeEur: number }) =>
    request<ColourSurchargeRule>("/order-genius/colour-surcharges", {
      method: "PATCH",
      body: JSON.stringify(body),
    }),

  getOrderGeniusColourHexRules: () =>
    request<{ items: ColourHexRule[] }>("/order-genius/colour-hex-rules"),

  setOrderGeniusColourHexRuleStandard: (body: { brand: string; colourCode: string; colourName: string; colourHex: string }) =>
    request<{ brand: string; colourCode: string; colourName: string; normalizedColourName: string; colourHex: string; updated: number; materialCodes: string[] }>(
      "/order-genius/colour-hex-rules/standard",
      { method: "PATCH", body: JSON.stringify(body) },
    ),

  getOrderGeniusCountries: () =>
    request<{ items: CountryPaymentTerm[] }>("/order-genius/countries"),

  getAccountCountryOptions: () =>
    request<{ items: CountryPaymentTerm[] }>("/order-genius/account-country-options"),

  getOrderGeniusFobCountries: () =>
    request<{ countries: string[] }>("/order-genius/fob-countries"),

  getOrderGeniusBaselines: () =>
    request<{ items: BaselineVersion[] }>("/order-genius/baselines"),

  previewOrderQuantityImport: (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return request<QuantityImportPreview>(
      "/order-genius/import-quantities/preview",
      { method: "POST", body: form },
    );
  },

  applyOrderQuantityImport: (importId: string) =>
    request<QuantityImportResult>(
      `/order-genius/import-quantities/${encodeURIComponent(importId)}/apply`,
      { method: "POST" },
    ),

  getVehicleAllocationPis: (params: PiOrderFilters = {}) => {
    const qs = new URLSearchParams();
    appendSearchParam(qs, "country", params.country);
    appendSearchParam(qs, "month", params.month);
    appendSearchParam(qs, "status", params.status);
    appendSearchParam(qs, "keyword", params.keyword);
    appendSearchParam(qs, "page", params.page);
    appendSearchParam(qs, "page_size", params.pageSize);
    const suffix = qs.toString();
    return request<VehicleAllocationListResponse<PiOrderHeader>>(
      `/order-genius/vehicle-allocation/pi${suffix ? `?${suffix}` : ""}`,
    );
  },

  getVehicleAllocationPi: (piCode: string) =>
    request<PiOrderDetail>(
      `/order-genius/vehicle-allocation/pi/${encodeURIComponent(piCode)}`,
    ),

  createVehicleAllocationPi: (body: Record<string, unknown>) =>
    request<PiOrderHeader>("/order-genius/vehicle-allocation/pi", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  updateVehicleAllocationPi: (piCode: string, body: Record<string, unknown>) =>
    request<PiOrderHeader>(
      `/order-genius/vehicle-allocation/pi/${encodeURIComponent(piCode)}`,
      { method: "PATCH", body: JSON.stringify(body) },
    ),

  createVehicleAllocationLine: (piCode: string, body: Record<string, unknown>) =>
    request<PiOrderLine>(
      `/order-genius/vehicle-allocation/pi/${encodeURIComponent(piCode)}/lines`,
      { method: "POST", body: JSON.stringify(body) },
    ),

  updateVehicleAllocationLine: (piLineCode: string, body: Record<string, unknown>) =>
    request<PiOrderLine>(
      `/order-genius/vehicle-allocation/lines/${encodeURIComponent(piLineCode)}`,
      { method: "PATCH", body: JSON.stringify(body) },
    ),

  deleteVehicleAllocationPi: (piCode: string) =>
    request<{ pi_code: string; deleted: boolean }>(
      `/order-genius/vehicle-allocation/pi/${encodeURIComponent(piCode)}`,
      { method: "DELETE" },
    ),

  deleteVehicleAllocationLine: (piLineCode: string) =>
    request<{ pi_line_code: string; deleted: boolean }>(
      `/order-genius/vehicle-allocation/lines/${encodeURIComponent(piLineCode)}`,
      { method: "DELETE" },
    ),

  listVehicleAllocationVehicles: (params: VehicleAllocationFilters = {}) => {
    const qs = new URLSearchParams();
    appendSearchParam(qs, "keyword", params.keyword);
    appendSearchParam(qs, "pi_code", params.piCode);
    appendSearchParam(qs, "pi_line_code", params.piLineCode);
    appendSearchParam(qs, "car_code", params.carCode);
    appendSearchParam(qs, "vin", params.vin);
    appendSearchParam(qs, "material_code", params.materialCode);
    appendSearchParam(qs, "bom", params.bom);
    appendSearchParam(qs, "country", params.country);
    appendSearchParam(qs, "ship_name", params.shipName);
    appendSearchParam(qs, "allocation_status", params.allocationStatus);
    appendSearchParam(qs, "logistics_status", params.logisticsStatus);
    appendSearchParam(qs, "eta_from", params.etaFrom);
    appendSearchParam(qs, "eta_to", params.etaTo);
    appendSearchParam(qs, "ready_from", params.readyFrom);
    appendSearchParam(qs, "ready_to", params.readyTo);
    appendSearchParam(qs, "vin_missing_only", params.vinMissingOnly);
    appendSearchParam(qs, "unallocated_only", params.unallocatedOnly);
    appendSearchParam(qs, "page", params.page);
    appendSearchParam(qs, "page_size", params.pageSize);
    const suffix = qs.toString();
    return request<VehicleAllocationListResponse<PiVehicleUnit>>(
      `/order-genius/vehicle-allocation/vehicles${suffix ? `?${suffix}` : ""}`,
    );
  },

  getVehicleAllocationVehicle: (carCode: string) =>
    request<PiVehicleUnit>(
      `/order-genius/vehicle-allocation/vehicles/${encodeURIComponent(carCode)}`,
    ),

  updateVehicleAllocationVehicle: (carCode: string, body: UpdateVehiclePayload) =>
    request<PiVehicleUnit>(
      `/order-genius/vehicle-allocation/vehicles/${encodeURIComponent(carCode)}`,
      { method: "PATCH", body: JSON.stringify(body) },
    ),

  bulkUpdateVehicleAllocationVehicles: (body: BulkVehicleUpdatePayload) =>
    request<BulkVehicleUpdateResult>(
      "/order-genius/vehicle-allocation/vehicles/bulk-update",
      { method: "POST", body: JSON.stringify(body) },
    ),

  searchVehicleAllocation: (keyword: string) => {
    const qs = new URLSearchParams({ keyword });
    return request<VehicleAllocationSearchResult>(
      `/order-genius/vehicle-allocation/search?${qs.toString()}`,
    );
  },

  getVehicleAllocationOrderMatrixPlan: (country: string, year: number, month: number) => {
    const qs = new URLSearchParams();
    appendSearchParam(qs, "country", country);
    appendSearchParam(qs, "year", year);
    appendSearchParam(qs, "month", month);
    return request<VehicleAllocationPlan>(
      `/order-genius/vehicle-allocation/order-matrix-plan?${qs.toString()}`,
    );
  },

  generateVehicleAllocationFromOrderMatrix: (body: Record<string, unknown>) =>
    request<{ piCode: string; lineCount: number; vehicleCount: number }>(
      "/order-genius/vehicle-allocation/generate-from-order-matrix",
      { method: "POST", body: JSON.stringify(body) },
    ),

  previewVehicleAllocationImport: (file: File) => {
    const form = new FormData();
    form.append("file", file);
    return request<VehicleImportPreview>(
      "/order-genius/vehicle-allocation/import/preview",
      { method: "POST", body: form },
    );
  },

  applyVehicleAllocationImport: (importId: string) =>
    request<VehicleImportResult>(
      `/order-genius/vehicle-allocation/import/${encodeURIComponent(importId)}/apply`,
      { method: "POST" },
    ),

  exportVehicleAllocation: (params: VehicleAllocationFilters = {}) =>
    requestBlob("/order-genius/vehicle-allocation/export", {
      method: "POST",
      body: JSON.stringify(params),
      headers: { "Content-Type": "application/json" },
    }),

  // BOM Admin
  getBomAdmin: (params?: { brand?: string; search?: string; country?: string }) => {
    const qs = params ? new URLSearchParams(Object.entries(params).filter(([_,v]) => v != null) as any).toString() : "";
    return request<{ items: any[]; countries: string[]; activeFobCountries?: string[] }>("/order-genius/bom-admin" + (qs ? "?" + qs : ""));
  },

  updateSkuLifecycle: (materialCode: string, body: { lifecycleStatus: string; effectiveFrom?: string; effectiveTo?: string; rowVersion: number }) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/lifecycle`, { method: "PATCH", body: JSON.stringify(body) }),

  updateSkuFob: (materialCode: string, body: { countryCode: string; finalFobEur?: number | null; paymentTermCode?: string }) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/fob`, { method: "PATCH", body: JSON.stringify(body) }),

  getSkuFobDetail: (materialCode: string, country: string) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/fob?country=${encodeURIComponent(country)}`),

  copyCountryFobs: (body: { sourceCountryCode: string; targetCountryCode: string; overwriteExisting?: boolean }) =>
    request<{ sourceCountryCode: string; targetCountryCode: string; sourceRows: number; created: number; updated: number; skipped: number; unchanged: number; targetPaymentTermCode: string | null }>(
      "/order-genius/countries/copy-fobs",
      { method: "POST", body: JSON.stringify(body) },
    ),

  adjustCountryFobs: (body: { countryCode: string; deltaEur: number }) =>
    request<{ countryCode: string; deltaEur: number; rows: number; adjusted: number; skippedNegative: number; unchanged: number }>(
      "/order-genius/countries/adjust-fobs",
      { method: "POST", body: JSON.stringify(body) },
    ),

  createPaymentTerm: (body: { countryCode: string; countryName: string; paymentTermCode: string; paymentMethod: string; lcDays: number }) =>
    request<any>("/order-genius/payment-terms/countries", { method: "POST", body: JSON.stringify(body) }),

  createMaterialSku: (body: { materialCode: string; brand?: string; modelName?: string; version?: string; colour?: string; colourCode?: string; colourHex?: string | null; colourType?: string; powertrain?: string; bomTemplate?: string; sourceBomTemplate?: string }) =>
    request<any>("/order-genius/material-skus", { method: "POST", body: JSON.stringify(body) }),

  updateSkuMetadata: (materialCode: string, body: { materialCodes?: string[]; brand?: string; modelName?: string; version?: string; powertrain?: string }) =>
    request<{ materialCodes: string[]; updated: number }>(
      `/order-genius/material-skus/${encodeURIComponent(materialCode)}/metadata`,
      { method: "PATCH", body: JSON.stringify(body) },
    ),

  updateColourHex: (materialCode: string, colourHex: string | null) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/colour-hex`, { method: "PATCH", body: JSON.stringify({ colourHex }) }),

  confirmColourCode: (materialCode: string) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/confirm-colour-code`, { method: "PATCH" }),

  updateColourCode: (materialCode: string, colourCode: string) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/colour-code`, { method: "PATCH", body: JSON.stringify({ colourCode }) }),

  updateMaterialCode: (oldCode: string, newCode: string) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(oldCode)}/material-code`, { method: "PATCH", body: JSON.stringify({ materialCode: newCode }) }),

  updateBomTemplateMaterialCode: (materialCodes: string[], bomTemplate: string) =>
    request<{ bomTemplate: string; materialCodes: string[]; updated: number }>(
      "/order-genius/bom-templates/material-code",
      { method: "PATCH", body: JSON.stringify({ materialCodes, bomTemplate }) },
    ),

  updateColourTier: (materialCode: string, colourTier: string) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/colour-tier`, { method: "PATCH", body: JSON.stringify({ colourTier }) }),

  deleteMaterialSku: (materialCode: string) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}`, { method: "DELETE" }),

  updateSkuInterior: (materialCode: string, body: { interiorColorName?: string | null; editionTag?: string | null; interiorColourCode?: string | null }) =>
    request<any>(`/order-genius/material-skus/${encodeURIComponent(materialCode)}/interior`, { method: "PATCH", body: JSON.stringify(body) }),
};
