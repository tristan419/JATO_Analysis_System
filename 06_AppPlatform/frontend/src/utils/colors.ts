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

/** Assign color to a series: powertrain-aware when `isPowertrain` is true */
export function seriesColor(name: string, idx: number, palette: string[], isPowertrain: boolean): string {
  if (isPowertrain) return ptColor(name, palette[idx % palette.length]);
  return palette[idx % palette.length];
}

// Module-level color overrides — set by pages that have export color customization
let _fuelOverrides: Record<string, string> = {};
let _originOverrides: Record<string, string> = {};

export function setFuelColorOverrides(overrides: Record<string, string>): void {
  _fuelOverrides = { ...overrides };
}

export function setOriginColorOverrides(overrides: Record<string, string>): void {
  _originOverrides = { ...overrides };
}

export function fuelColor(fuel: string): string {
  if (_fuelOverrides[fuel]) return _fuelOverrides[fuel];
  return FUEL_COLORS[fuel] ?? "#94a3b8";
}

export function originColor(origin: string): string {
  if (_originOverrides[origin]) return _originOverrides[origin];
  return ORIGIN_COLORS[origin] ?? "#64748b";
}

export function ptColor(name: string, fallback: string): string {
  if (_fuelOverrides[name]) return _fuelOverrides[name];
  return POWERTRAIN_COLORS[name] ?? POWERTRAIN_COLORS[name.toUpperCase()] ?? fallback;
}

function hexToRgb(color: string): [number, number, number] | null {
  const trimmed = color.trim().replace("#", "");
  const expanded = trimmed.length === 3
    ? trimmed.split("").map((part) => part + part).join("")
    : trimmed;
  if (!/^[0-9a-fA-F]{6}$/.test(expanded)) {
    return null;
  }
  return [
    Number.parseInt(expanded.slice(0, 2), 16),
    Number.parseInt(expanded.slice(2, 4), 16),
    Number.parseInt(expanded.slice(4, 6), 16),
  ];
}

function mixHexColor(color: string, ratio: number, target: number): string {
  const rgb = hexToRgb(color);
  if (!rgb) return color;
  const mixed = rgb.map((channel) => Math.round(channel + (target - channel) * ratio));
  return `#${mixed.map((channel) => channel.toString(16).padStart(2, "0")).join("")}`;
}

function shadeHexColor(color: string, amount: number): string {
  if (amount === 0) return color;
  return amount > 0
    ? mixHexColor(color, Math.min(amount, 0.55), 255)
    : mixHexColor(color, Math.min(Math.abs(amount), 0.45), 0);
}

export function fuelFamilyColor(fuel: string, idx: number, total: number): string {
  const baseColor = fuelColor(normalizePowertrainName(fuel));
  if (total <= 1) return baseColor;
  const shadeOffsets = [0.28, 0.18, 0.08, 0, -0.1, -0.2, -0.3, -0.38, -0.44, -0.5];
  const safeIndex = Math.max(0, idx);
  const shadeAmount = shadeOffsets[Math.min(safeIndex, shadeOffsets.length - 1)];
  return shadeHexColor(baseColor, shadeAmount);
}
