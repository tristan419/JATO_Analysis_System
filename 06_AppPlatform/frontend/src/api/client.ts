import type {
  AdvancedChartResponse,
  AnalysisQuery,
  CrudListResponse,
  CrudItem,
  DetailResponse,
  GroupedTimeSeriesResponse,
  ModelVersionsResponse,
  OverviewResponse,
  PositioningMapResponse,
  RvFinanceResponse,
  RvFinanceVehicle,
} from "../types";

const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:8000/v1";

function getAuthHeaders(): Record<string, string> {
  const token = (
    localStorage.getItem("jato_auth_token")
    || import.meta.env.VITE_AUTH_TOKEN
    || ""
  ).trim();
  const role = (
    localStorage.getItem("jato_user_role")
    || import.meta.env.VITE_USER_ROLE
    || "viewer"
  ).trim();
  const user = (
    localStorage.getItem("jato_user_name")
    || import.meta.env.VITE_USER_NAME
    || "anonymous"
  ).trim();

  return {
    ...(token ? { "X-Auth-Token": token } : {}),
    "X-User-Role": role || "viewer",
    "X-User-Name": user || "anonymous"
  };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...getAuthHeaders(),
      ...(init?.headers ?? {})
    },
    ...init
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${text}`);
  }
  return (await response.json()) as T;
}

async function requestBlob(path: string, init?: RequestInit): Promise<Blob> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      ...getAuthHeaders(),
      ...(init?.headers ?? {})
    },
    ...init
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${text}`);
  }
  return response.blob();
}

export const api = {
  columns: () => request<{ items: string[] }>("/metadata/columns"),
  filterOptions: (payload: { column: string; filters: Record<string, string[]> }) =>
    request<{ column: string; options: string[]; rowCount: number }>(
      "/filters/options",
      { method: "POST", body: JSON.stringify(payload) }
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
    request<{ deleted: boolean }>(`/crud/items/${id}`, { method: "DELETE" })
};
