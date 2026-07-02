export const FILTER_KEYS = [
  "country",
  "body_type",
  "segment",
  "powertrain",
  "origin",
  "make",
  "model",
  "version",
] as const;

export type FilterKey = (typeof FILTER_KEYS)[number];
export type FilterSelections = Record<FilterKey, string[]>;

export const DIM: Record<FilterKey, string[]> = {
  country: ["国家", "Country", "country"],
  body_type: ["Body type", "Body Type", "body type", "车身形式"],
  segment: ["细分市场（按车长）", "细分市场", "segment"],
  powertrain: ["动总规整", "powertrain", "Powertrain"],
  origin: ["车系", "Origin", "Series", "origin"],
  make: ["Make", "品牌", "make"],
  model: ["Model", "model"],
  version: ["Version name", "version name", "Version Name"],
};

export const FILTER_ORDER: { key: FilterKey; label: string }[] = [
  { key: "country", label: "国家" },
  { key: "body_type", label: "车身形式" },
  { key: "segment", label: "细分市场" },
  { key: "powertrain", label: "动总规整" },
  { key: "origin", label: "品牌阵营" },
  { key: "make", label: "品牌" },
  { key: "model", label: "Model" },
  { key: "version", label: "Version name" },
];

const DEFAULT_POWERTRAINS = ["ICE", "HEV", "BEV", "MHEV", "PHEV"] as const;

function normalizePowertrainName(value: string): string {
  return value.trim().toUpperCase();
}

export function createEmptySelections(): FilterSelections {
  return {
    country: [],
    body_type: [],
    segment: [],
    powertrain: [],
    origin: [],
    make: [],
    model: [],
    version: [],
  };
}

export function hasSelections(selections: FilterSelections): boolean {
  return FILTER_ORDER.some(({ key }) => selections[key].length > 0);
}

export function resolve(cols: string[], keys: string[]): string | null {
  const map = new Map(cols.map((column) => [column.toLowerCase().trim(), column]));
  for (const key of keys) {
    const hit = map.get(key.toLowerCase().trim());
    if (hit) return hit;
  }
  return null;
}

export function getDefaultPowertrainValues(options: string[]): string[] {
  const optionMap = new Map(options.map((option) => [normalizePowertrainName(option), option]));
  return DEFAULT_POWERTRAINS
    .map((name) => optionMap.get(name))
    .filter((value): value is string => Boolean(value));
}

export function readSelectionsFromSearch(search: string): FilterSelections {
  const params = new URLSearchParams(search);
  const initial = createEmptySelections();
  for (const { key } of FILTER_ORDER) {
    const value = params.get(key);
    if (value) initial[key] = value.split(",").filter(Boolean);
  }
  return initial;
}

export function buildSearchFromSelections(selections: FilterSelections): string {
  const params = new URLSearchParams();
  for (const { key } of FILTER_ORDER) {
    const values = selections[key];
    if (values.length) params.set(key, values.join(","));
  }
  const queryString = params.toString();
  return queryString ? `?${queryString}` : "";
}
