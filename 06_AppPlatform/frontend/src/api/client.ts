import type {
  AdvancedChartResponse,
  AnalysisQuery,
  ConfigImportBatch,
  ConfigProject,
  ConfigVariant,
  CountryChatDeckResponse,
  CountryChatMetadataResponse,
  CountryChatNewsOpsStatus,
  CountryChatNewsRefreshResponse,
  CountryChatResponse,
  CountryChatTurn,
  CrudListResponse,
  CrudItem,
  CurrentPrice,
  DataFreshnessItem,
  PriceHistoryEntry,
  DetailResponse,
  GroupedTimeSeriesResponse,
  MarketScanDeckRequest,
  MarketScanDeckResponse,
  ModelVersionsResponse,
  OverviewResponse,
  PositioningMapResponse,
  ReviewCase,
  ReviewCaseDetail,
  ReviewBacklogOpportunity,
  ReviewDecision,
  ReviewScopeCountrySummary,
  ReviewWorkbench,
  RvFinanceResponse,
  RvFinanceVehicle,
} from "../types";
import type { FilterOptionsPayload } from "../utils/filterOptions";

const API_BASE = import.meta.env.VITE_API_BASE ?? "/v1";

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

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const key = dedupeKey(path, init);
  const inflight = inflightRequests.get(key) as Promise<T> | undefined;
  if (inflight) return inflight;

  const promise = (async () => {
    let response: Response;
    try {
      response = await fetch(`${API_BASE}${path}`, {
        headers: {
          "Content-Type": "application/json",
          ...getAuthHeaders(),
          ...(init?.headers ?? {})
        },
        ...init
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`网络请求失败：${path} (${message})`);
    }
    if (!response.ok) {
      const message = await readErrorMessage(response);
      throw new Error(`${response.status} ${message}`);
    }
    return (await response.json()) as T;
  })();

  inflightRequests.set(key, promise);
  promise.finally(() => inflightRequests.delete(key));
  return promise;
}

async function requestBlob(path: string, init?: RequestInit): Promise<Blob> {
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      headers: {
        ...getAuthHeaders(),
        ...(init?.headers ?? {})
      },
      ...init
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`网络请求失败：${path} (${message})`);
  }
  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`${response.status} ${message}`);
  }
  return response.blob();
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
    currentAssignee: raw.currentAssignee === undefined || raw.currentAssignee === null ? null : String(raw.currentAssignee),
    createdAt: String(raw.createdAtUtc ?? raw.createdAt ?? ""),
    updatedAt: String(raw.updatedAtUtc ?? raw.updatedAt ?? ""),
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
    observation: (raw.observation as Record<string, unknown> | null | undefined) ?? null,
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
  countryChat: (payload: {
    country: string;
    question: string;
    history: CountryChatTurn[];
    refresh_news?: boolean;
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
  }) =>
    request<AdvancedChartResponse>("/analysis/advanced-chart", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  groupedTimeSeries: (payload: {
    filters: Record<string, string[]>;
    grain: "year" | "month";
    group_by: string | null;
    top_n: number;
    include_others: boolean;
  }) =>
    request<GroupedTimeSeriesResponse>("/analysis/time-series-grouped", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  modelVersions: (payload: {
    filters: Record<string, string[]>;
    model_name: string;
    top_n?: number;
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
  marketScanDeck: (payload: MarketScanDeckRequest = {}) =>
    request<MarketScanDeckResponse>("/market-scan/deck", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
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
    return request<{ items: ConfigProject[] }>(
      `/engineering/projects${q ? `?${q}` : ""}`
    );
  },
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
    limit?: number;
  }) => {
    const sp = new URLSearchParams();
    if (params?.country) sp.set("country", params.country);
    if (params?.brand) sp.set("brand", params.brand);
    if (params?.jato_model) sp.set("jato_model", params.jato_model);
    if (params?.jato_trim) sp.set("jato_trim", params.jato_trim);
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
};
