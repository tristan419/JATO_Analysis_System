import type {
  AdvancedChartResponse,
  DataFreshnessItem,
  GroupedTimeSeriesResponse,
  ModelVersionsResponse,
  OverviewResponse,
  PositioningMapResponse,
} from "../types";
import type { FilterOptionsPayload } from "../utils/filterOptions";
import { request } from "./core";

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

export const dashboardApi = {
  columns: (init?: RequestInit) =>
    request<{ items: string[] }>("/metadata/columns", init),
  filterMetadataSnapshot: (init?: RequestInit) =>
    request<FilterMetadataSnapshotResponse>("/metadata/filter-snapshot", init),
  filterOptionsBatch: (items: FilterOptionsPayload[], init?: RequestInit) =>
    request<FilterOptionsBatchResponse>(
      "/filters/options/batch",
      { method: "POST", body: JSON.stringify({ items }), ...init },
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
      body: JSON.stringify(payload),
    }),
  modelVersions: (payload: {
    filters: Record<string, string[]>;
    model_name: string;
    top_n?: number;
    time_range?: { start: string; end: string };
  }) =>
    request<ModelVersionsResponse>("/analysis/model-versions", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  positioningMap: (payload: {
    filters: Record<string, string[]>;
    target_length?: number | null;
    target_msrp?: number | null;
    length_range: number;
    manual_competitors: string[];
    top_n: number;
    n_clusters: number;
    time_range?: { start: string; end: string };
  }) =>
    request<PositioningMapResponse>("/analysis/positioning-map", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
};
