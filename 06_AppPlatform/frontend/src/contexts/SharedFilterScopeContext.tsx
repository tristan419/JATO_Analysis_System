import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useLocation, useNavigate } from "react-router-dom";

import { api } from "../api/client";
import {
  DIM,
  FILTER_KEYS,
  FILTER_ORDER,
  buildSearchFromSelections,
  createEmptySelections,
  getDefaultPowertrainValues,
  hasSelections,
  readSelectionsFromSearch,
  resolve,
  type FilterKey,
  type FilterSelections,
} from "../dashboardFilters";
import type { OverviewResponse, TimeSeriesPoint } from "../types";
import { getCachedPageValue, setCachedPageValue } from "../utils/pageCache";
import {
  FILTER_OPTIONS_CACHE_TTL_MS,
  buildFilterOptionsCacheKey,
  fetchOnDemandCascadedOptions,
  isAbortError,
  type FilterOptionsPayload,
} from "../utils/filterOptions";

const SHARED_FILTER_SCOPE_CACHE_KEY = "shared-filter-scope";
const PAGE_CACHE_TTL_MS = 30 * 60 * 1000;

export type ResolvedFilterColumns = Record<FilterKey, string | null>;

export const TOP_LEVEL_FILTER_KEYS = [
  "country",
  "segment",
  "powertrain",
] as const satisfies readonly FilterKey[];

interface SharedFilterScopeCache {
  search: string;
  columns: string[];
  selections: FilterSelections;
  optionsMap: Partial<Record<FilterKey, string[]>>;
  overview: OverviewResponse | null;
  filteredRowCount: number | null;
  yearSeries: TimeSeriesPoint[];
  monthSeries: TimeSeriesPoint[];
  filtersReady: boolean;
  heroCollapsed: boolean;
  sidebarCollapsed: boolean;
}

export interface SharedFilterScopeValue {
  columns: string[];
  selections: FilterSelections;
  optionsMap: Partial<Record<FilterKey, string[]>>;
  overview: OverviewResponse | null;
  filteredRowCount: number | null;
  yearSeries: TimeSeriesPoint[];
  monthSeries: TimeSeriesPoint[];
  filtersReady: boolean;
  loading: boolean;
  optionsSyncPending: boolean;
  error: string;
  activeFilters: typeof FILTER_ORDER;
  activeFilterSummary: string;
  dashboardSearch: string;
  specificationHref: string;
  dashboardHref: string;
  heroCollapsed: boolean;
  sidebarCollapsed: boolean;
  setHeroCollapsed: React.Dispatch<React.SetStateAction<boolean>>;
  setSidebarCollapsed: React.Dispatch<React.SetStateAction<boolean>>;
  buildFilterPayload: () => Record<string, string[]>;
  filterPayloadStr: string;
  applySelections: (next: FilterSelections, dimKey: FilterKey) => Promise<void>;
  onFilterChange: (dimKey: FilterKey, newVals: string[]) => Promise<void>;
  resetFilters: () => void;
}

const SharedFilterScopeContext = createContext<SharedFilterScopeValue | null>(null);

export function shouldSyncDashboardSearchToLocation(pathname: string): boolean {
  return pathname === "/" || pathname === "/dashboard" || pathname === "/specification";
}

export function createSharedSelections(
  source?: Record<string, string[]>,
): FilterSelections {
  const base = createEmptySelections();
  return {
    country: [...(source?.country ?? base.country)],
    segment: [...(source?.segment ?? base.segment)],
    powertrain: [...(source?.powertrain ?? base.powertrain)],
    make: [...(source?.make ?? base.make)],
    model: [...(source?.model ?? base.model)],
    version: [...(source?.version ?? base.version)],
  };
}

export function resolveFilterColumns(columns: string[]): ResolvedFilterColumns {
  return FILTER_KEYS.reduce<ResolvedFilterColumns>(
    (resolved, key) => {
      resolved[key] = resolve(columns, DIM[key]);
      return resolved;
    },
    {
      country: null,
      segment: null,
      powertrain: null,
      make: null,
      model: null,
      version: null,
    },
  );
}

export function buildFilterPayloadFromResolved(
  resolved: ResolvedFilterColumns,
  selections: FilterSelections,
): Record<string, string[]> {
  const payload: Record<string, string[]> = {};
  for (const { key } of FILTER_ORDER) {
    const column = resolved[key];
    const values = selections[key];
    if (column && values.length > 0) payload[column] = values;
  }
  return payload;
}

export function sanitizeTopLevelSelections(
  selections: FilterSelections,
  topLevelOptions: Partial<Record<FilterKey, string[]>>,
): FilterSelections {
  const next = createSharedSelections(selections);
  for (const key of TOP_LEVEL_FILTER_KEYS) {
    const available = topLevelOptions[key] ?? [];
    next[key] = next[key].filter((value) => available.includes(value));
  }
  return next;
}

function summarizeScopeValues(values: string[]): string {
  if (values.length === 0) return "-";
  if (values.length <= 2) return values.join(" · ");
  return `${values.slice(0, 2).join(" · ")} +${values.length - 2}`;
}

export function SharedFilterScopeProvider({ children }: { children: ReactNode }) {
  const location = useLocation();
  const navigate = useNavigate();
  const currentSearch = location.search;
  const cachedScopeRef = useRef<SharedFilterScopeCache | null>(null);

  if (cachedScopeRef.current === null) {
    const cached = getCachedPageValue<SharedFilterScopeCache>(
      SHARED_FILTER_SCOPE_CACHE_KEY,
    );
    cachedScopeRef.current = cached && cached.search === currentSearch ? cached : null;
  }

  const cachedScope = cachedScopeRef.current;
  const [columns, setColumns] = useState<string[]>(() => cachedScope?.columns ?? []);
  const [selections, setSelections] = useState<FilterSelections>(() =>
    createSharedSelections(cachedScope?.selections),
  );
  const [optionsMap, setOptionsMap] = useState<
    Partial<Record<FilterKey, string[]>>
  >(() => cachedScope?.optionsMap ?? {});
  const [overview, setOverview] = useState<OverviewResponse | null>(
    () => cachedScope?.overview ?? null,
  );
  const [filteredRowCount, setFilteredRowCount] = useState<number | null>(
    () => cachedScope?.filteredRowCount ?? null,
  );
  const [yearSeries, setYearSeries] = useState<TimeSeriesPoint[]>(
    () => cachedScope?.yearSeries ?? [],
  );
  const [monthSeries, setMonthSeries] = useState<TimeSeriesPoint[]>(
    () => cachedScope?.monthSeries ?? [],
  );
  const [filtersReady, setFiltersReady] = useState(
    () => cachedScope?.filtersReady ?? false,
  );
  const [loading, setLoading] = useState(() => !cachedScope);
  const [optionsSyncPending, setOptionsSyncPending] = useState(false);
  const [error, setError] = useState("");
  const [heroCollapsed, setHeroCollapsed] = useState(
    () => cachedScope?.heroCollapsed ?? false,
  );
  const [sidebarCollapsed, setSidebarCollapsed] = useState(
    () => cachedScope?.sidebarCollapsed ?? false,
  );
  const bootDone = useRef(false);
  const bootCompleted = useRef(Boolean(cachedScope));
  const optionsCacheRef = useRef(
    new Map<string, { expiresAt: number; options: string[] }>(),
  );
  const bootOptionsAbortRef = useRef<AbortController | null>(null);
  const syncOptionsAbortRef = useRef<AbortController | null>(null);
  const prevPayloadRef = useRef("");

  const res = useMemo<ResolvedFilterColumns>(
    () => resolveFilterColumns(columns),
    [columns],
  );
  const buildFilterPayload = useCallback(
    () => buildFilterPayloadFromResolved(res, selections),
    [res, selections],
  );
  const filterPayloadStr = useMemo(
    () => JSON.stringify(buildFilterPayload()),
    [buildFilterPayload],
  );
  const dashboardSearch = useMemo(
    () => buildSearchFromSelections(selections),
    [selections],
  );
  const activeFilters = useMemo(
    () => FILTER_ORDER.filter(({ key }) => selections[key].length > 0),
    [selections],
  );
  const activeFilterSummary = useMemo(() => {
    if (activeFilters.length === 0) return "Default powertrain lens";
    return activeFilters
      .map(({ key, label }) => `${label}: ${summarizeScopeValues(selections[key])}`)
      .join(" · ");
  }, [activeFilters, selections]);

  const loadFilterOptions = useCallback(
    async (
      payload: FilterOptionsPayload,
      signal?: AbortSignal,
    ): Promise<string[]> => {
      const cacheKey = buildFilterOptionsCacheKey(payload);
      const now = Date.now();
      const cached = optionsCacheRef.current.get(cacheKey);
      if (cached && cached.expiresAt > now) {
        return cached.options;
      }

      const response = await api.filterOptions(payload, { signal });
      optionsCacheRef.current.set(cacheKey, {
        expiresAt: now + FILTER_OPTIONS_CACHE_TTL_MS,
        options: response.options,
      });
      return response.options;
    },
    [],
  );

  const loadOverview = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const overviewResponse = await api.overview({
        filters: buildFilterPayload(),
        prefer_precomputed: true,
        top_n: 120,
      });
      setOverview(overviewResponse);
      setYearSeries(overviewResponse.yearSeries ?? []);
      setMonthSeries(overviewResponse.monthSeries ?? []);
      setFilteredRowCount(overviewResponse.kpis.totalRows);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  }, [buildFilterPayload]);

  useEffect(() => {
    if (bootDone.current) return;
    bootDone.current = true;
    if (cachedScope) {
      bootCompleted.current = true;
      prevPayloadRef.current = JSON.stringify(
        buildFilterPayloadFromResolved(
          resolveFilterColumns(cachedScope.columns),
          cachedScope.selections,
        ),
      );
      return;
    }

    const controller = new AbortController();
    bootOptionsAbortRef.current = controller;
    bootCompleted.current = false;

    (async () => {
      setLoading(true);
      setError("");
      try {
        const { items } = await api.columns();
        const resolvedColumns = resolveFilterColumns(items);
        const topLevelOptions = (
          await Promise.all(
            TOP_LEVEL_FILTER_KEYS.map(async (key) => {
              const column = resolvedColumns[key];
              if (!column) return [key, [] as string[]] as const;
              const options = await loadFilterOptions(
                { column, filters: {} },
                controller.signal,
              );
              return [key, options] as const;
            }),
          )
        ).reduce<Partial<Record<FilterKey, string[]>>>(
          (accumulator, [key, options]) => {
            accumulator[key] = options;
            return accumulator;
          },
          {},
        );

        const initialFromSearch = sanitizeTopLevelSelections(
          readSelectionsFromSearch(currentSearch),
          topLevelOptions,
        );
        const initialSelections = hasSelections(initialFromSearch)
          ? initialFromSearch
          : createSharedSelections({
              country: topLevelOptions.country ?? [],
              powertrain: getDefaultPowertrainValues(
                topLevelOptions.powertrain ?? [],
              ),
            });

        const {
          optionsMap: cascadedOptions,
          selections: syncedSelections,
        } = await fetchOnDemandCascadedOptions(
          resolvedColumns,
          initialSelections,
          3,
          loadFilterOptions,
          controller.signal,
        );

        const initialFilters = buildFilterPayloadFromResolved(
          resolvedColumns,
          syncedSelections,
        );
        const overviewResponse = await api.overview({
          filters: initialFilters,
          prefer_precomputed: true,
          top_n: 120,
        });

        prevPayloadRef.current = JSON.stringify(initialFilters);
        setColumns(items);
        setSelections(syncedSelections);
        setOptionsMap({ ...topLevelOptions, ...cascadedOptions });
        setOverview(overviewResponse);
        setYearSeries(overviewResponse.yearSeries ?? []);
        setMonthSeries(overviewResponse.monthSeries ?? []);
        setFilteredRowCount(overviewResponse.kpis.totalRows);
        setFiltersReady(true);
        bootCompleted.current = true;
      } catch (err) {
        if (!isAbortError(err)) setError((err as Error).message);
      } finally {
        if (bootOptionsAbortRef.current === controller) {
          bootOptionsAbortRef.current = null;
        }
        setLoading(false);
      }
    })();

    return () => {
      controller.abort();
      if (!bootCompleted.current) {
        bootDone.current = false;
      }
      if (bootOptionsAbortRef.current === controller) {
        bootOptionsAbortRef.current = null;
      }
    };
  }, [cachedScope, currentSearch, loadFilterOptions]);

  useEffect(() => {
    return () => {
      bootOptionsAbortRef.current?.abort();
      syncOptionsAbortRef.current?.abort();
    };
  }, []);

  useEffect(() => {
    if (!filtersReady || columns.length === 0) return;
    if (!shouldSyncDashboardSearchToLocation(location.pathname)) return;
    const nextSearch = dashboardSearch;
    if (nextSearch === location.search) return;
    navigate(`${location.pathname}${nextSearch}`, { replace: true });
  }, [columns.length, dashboardSearch, filtersReady, location.pathname, location.search, navigate]);

  useEffect(() => {
    if (!filtersReady || columns.length === 0 || optionsSyncPending) return;
    if (prevPayloadRef.current === filterPayloadStr) return;
    prevPayloadRef.current = filterPayloadStr;
    void loadOverview();
  }, [columns.length, filterPayloadStr, filtersReady, loadOverview, optionsSyncPending]);

  const scopeCacheSnapshot = useMemo<SharedFilterScopeCache>(
    () => ({
      search: dashboardSearch,
      columns,
      selections,
      optionsMap,
      overview,
      filteredRowCount,
      yearSeries,
      monthSeries,
      filtersReady,
      heroCollapsed,
      sidebarCollapsed,
    }),
    [
      columns,
      dashboardSearch,
      filteredRowCount,
      filtersReady,
      heroCollapsed,
      monthSeries,
      optionsMap,
      overview,
      selections,
      sidebarCollapsed,
      yearSeries,
    ],
  );

  useEffect(() => {
    if (!filtersReady || columns.length === 0) return;
    setCachedPageValue(
      SHARED_FILTER_SCOPE_CACHE_KEY,
      scopeCacheSnapshot,
      PAGE_CACHE_TTL_MS,
    );
  }, [columns.length, filtersReady, scopeCacheSnapshot]);

  const applySelections = useCallback(
    async (next: FilterSelections, dimKey: FilterKey) => {
      const index = FILTER_ORDER.findIndex((filter) => filter.key === dimKey);
      if (index === -1) return;

      const cascadeStartIndex = index < 3 ? 3 : index;
      syncOptionsAbortRef.current?.abort();
      const controller = new AbortController();
      syncOptionsAbortRef.current = controller;

      setOptionsSyncPending(true);
      setSelections(next);
      setError("");
      try {
        const {
          optionsMap: cascadedOptions,
          selections: syncedSelections,
        } = await fetchOnDemandCascadedOptions(
          res,
          next,
          cascadeStartIndex,
          loadFilterOptions,
          controller.signal,
        );
        if (syncOptionsAbortRef.current !== controller) return;

        setSelections(syncedSelections);
        setOptionsMap((previous) => ({ ...previous, ...cascadedOptions }));
        setFiltersReady(true);
      } catch (err) {
        if (!isAbortError(err)) setError((err as Error).message);
      } finally {
        if (syncOptionsAbortRef.current === controller) {
          syncOptionsAbortRef.current = null;
          setOptionsSyncPending(false);
        }
      }
    },
    [loadFilterOptions, res],
  );

  const onFilterChange = useCallback(
    async (dimKey: FilterKey, newVals: string[]) => {
      const nextSelections: FilterSelections = {
        ...selections,
        [dimKey]: newVals,
      };
      await applySelections(nextSelections, dimKey);
    },
    [applySelections, selections],
  );

  const resetFilters = useCallback(() => {
    const defaults = getDefaultPowertrainValues(optionsMap.powertrain ?? []);
    const nextSelections = createSharedSelections({ powertrain: defaults });
    void applySelections(nextSelections, "powertrain");
  }, [applySelections, optionsMap.powertrain]);

  const value = useMemo<SharedFilterScopeValue>(
    () => ({
      columns,
      selections,
      optionsMap,
      overview,
      filteredRowCount,
      yearSeries,
      monthSeries,
      filtersReady,
      loading,
      optionsSyncPending,
      error,
      activeFilters,
      activeFilterSummary,
      dashboardSearch,
      specificationHref: `/specification${dashboardSearch}`,
      dashboardHref: dashboardSearch ? `/${dashboardSearch}` : "/",
      heroCollapsed,
      sidebarCollapsed,
      setHeroCollapsed,
      setSidebarCollapsed,
      buildFilterPayload,
      filterPayloadStr,
      applySelections,
      onFilterChange,
      resetFilters,
    }),
    [
      activeFilterSummary,
      activeFilters,
      applySelections,
      buildFilterPayload,
      columns,
      dashboardSearch,
      error,
      filterPayloadStr,
      filteredRowCount,
      filtersReady,
      heroCollapsed,
      loading,
      monthSeries,
      onFilterChange,
      optionsMap,
      optionsSyncPending,
      overview,
      resetFilters,
      selections,
      sidebarCollapsed,
      yearSeries,
    ],
  );

  return (
    <SharedFilterScopeContext.Provider value={value}>
      {children}
    </SharedFilterScopeContext.Provider>
  );
}

export function useSharedFilterScope(): SharedFilterScopeValue {
  const context = useContext(SharedFilterScopeContext);
  if (!context) {
    throw new Error("useSharedFilterScope must be used within SharedFilterScopeProvider");
  }
  return context;
}
