import { FILTER_ORDER, type FilterKey, type FilterSelections } from "../dashboardFilters";

export interface FilterOptionsPayload {
  column: string;
  filters: Record<string, string[]>;
}

export const FILTER_OPTIONS_CACHE_TTL_MS = 30_000;

export function buildFilterOptionsCacheKey(payload: FilterOptionsPayload): string {
  const normalizedFilters = Object.entries(payload.filters)
    .filter(([column, values]) => Boolean(column) && values.length > 0)
    .map(([column, values]) => [
      column,
      Array.from(new Set(values.map((value) => value.trim()).filter(Boolean))).sort(),
    ] as const)
    .sort(([left], [right]) => left.localeCompare(right));

  return JSON.stringify({ column: payload.column, filters: normalizedFilters });
}

export function isAbortError(error: unknown): boolean {
  if (error instanceof DOMException) {
    return error.name === "AbortError";
  }

  if (error instanceof Error) {
    return error.name === "AbortError" || /\babort(?:ed)?\b/i.test(error.message);
  }

  if (typeof error === "object" && error !== null) {
    const name = "name" in error ? String(error.name ?? "") : "";
    const message = "message" in error ? String(error.message ?? "") : "";
    return name === "AbortError" || /\babort(?:ed)?\b/i.test(message);
  }

  if (typeof error === "string") {
    return /\babort(?:ed)?\b/i.test(error);
  }

  return false;
}

export function cloneFilterSelections(source: FilterSelections): FilterSelections {
  return FILTER_ORDER.reduce<FilterSelections>((next, { key }) => {
    next[key] = [...source[key]];
    return next;
  }, {
    country: [],
    segment: [],
    powertrain: [],
    make: [],
    model: [],
    version: [],
  });
}

export async function fetchOnDemandCascadedOptions(
  resolved: Record<FilterKey, string | null>,
  sourceSelections: FilterSelections,
  startIndex: number,
  fetchOptions: (payload: FilterOptionsPayload, signal?: AbortSignal) => Promise<string[]>,
  signal?: AbortSignal,
): Promise<{
  optionsMap: Partial<Record<FilterKey, string[]>>;
  selections: FilterSelections;
}> {
  const selections = cloneFilterSelections(sourceSelections);
  const optionsMap: Partial<Record<FilterKey, string[]>> = {};

  if (startIndex >= FILTER_ORDER.length) {
    return { optionsMap, selections };
  }

  const prefixFilters: Record<string, string[]> = {};
  for (let index = 0; index < startIndex; index += 1) {
    const key = FILTER_ORDER[index].key;
    const column = resolved[key];
    if (column && selections[key].length > 0) {
      prefixFilters[column] = selections[key];
    }
  }

  for (let index = startIndex; index < FILTER_ORDER.length; index += 1) {
    const key = FILTER_ORDER[index].key;
    const column = resolved[key];
    if (!column) {
      selections[key] = [];
      optionsMap[key] = [];
      continue;
    }

    const options = await fetchOptions({ column, filters: prefixFilters }, signal);
    optionsMap[key] = options;

    const validSelections = selections[key].filter((value) => options.includes(value));
    selections[key] = validSelections;

    if (validSelections.length === 0) {
      for (let remainder = index + 1; remainder < FILTER_ORDER.length; remainder += 1) {
        const remainderKey = FILTER_ORDER[remainder].key;
        selections[remainderKey] = [];
        optionsMap[remainderKey] = [];
      }
      break;
    }

    prefixFilters[column] = validSelections;
  }

  return { optionsMap, selections };
}