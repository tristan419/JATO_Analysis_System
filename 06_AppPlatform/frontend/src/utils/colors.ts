/**
 * Shared color constants for powertrain types, fuel types, and origin groups.
 *
 * These palettes are used across DashboardPage, MarketScanPage, and ExportPanel
 * to ensure visual consistency.
 */

/** Powertrain fixed palette — all chart views use the same mapping */
export const POWERTRAIN_COLORS: Record<string, string> = {
  ICE:  "#6b7280",
  MHEV: "#f97316",
  HEV:  "#eab308",
  PHEV: "#3b82f6",
  BEV:  "#22c55e",
};

/** Canonical fuel-type palette (MarketScan uses the same colors plus LPG) */
export const FUEL_COLORS: Record<string, string> = {
  ...POWERTRAIN_COLORS,
  LPG: "#b91c1c",
};

/** Origin-group palette for market scan origin analysis */
export const ORIGIN_COLORS: Record<string, string> = {
  欧系: "#0f766e",
  日系: "#d97706",
  韩系: "#ef4444",
  美系: "#2563eb",
  中系: "#16a34a",
  其他: "#6b7280",
};

/** Default ordered powertrain list */
export const DEFAULT_POWERTRAINS = ["ICE", "HEV", "BEV", "MHEV", "PHEV"] as const;

/** Categorical hue cycle for unnamed series */
export const SERIES_COLORS = [
  "#2563eb","#16a34a","#f59e0b","#ef4444","#8b5cf6","#ec4899",
  "#14b8a6","#f97316","#6366f1","#0ea5e9","#84cc16","#e11d48",
];

export function normalizePowertrainName(value: string): string {
  return value.trim().toUpperCase();
}

/** Return powertrain color if name matches, else fallback */
export function ptColor(name: string, fallback: string): string {
  return POWERTRAIN_COLORS[name] ?? POWERTRAIN_COLORS[name.toUpperCase()] ?? fallback;
}

/** Assign color to a series: powertrain-aware when `isPowertrain` is true */
export function seriesColor(name: string, idx: number, palette: string[], isPowertrain: boolean): string {
  if (isPowertrain) return ptColor(name, palette[idx % palette.length]);
  return palette[idx % palette.length];
}

export function fuelColor(fuel: string): string {
  return FUEL_COLORS[fuel] ?? "#94a3b8";
}

export function originColor(origin: string): string {
  return ORIGIN_COLORS[origin] ?? "#64748b";
}
