// Order Genius business palette; do not substitute Market Scan's palette.
export const ORDER_GENIUS_POWERTRAIN_COLORS: Record<string, string> = {
  EV: "#16a34a", BEV: "#16a34a", HEV: "#ca8a04", PHEV: "#2563eb", SHS: "#2563eb",
  MHEV: "#d97706", ICE: "#4b5563", LPG: "#6b7280", REEV: "#0d9488", FCV: "#0891b2",
};
export function orderGeniusPowertrainColor(powertrain: string | null): string {
  return ORDER_GENIUS_POWERTRAIN_COLORS[powertrain?.trim().toUpperCase() ?? ""] ?? "#9ca3af";
}

export function buildBomEditScopeKey(
  modelGroupKey: string,
  versionKey: string,
  bomTemplate: string,
): string {
  return `${modelGroupKey}|${versionKey}|${bomTemplate}`;
}

export type BomAdminColourTier = "single" | "dual" | "special";

interface BomAdminColourTierFields {
  colour?: string | null;
  colourType?: string | null;
  colourTier?: string | null;
}

function explicitBomAdminColourTier(value: unknown): BomAdminColourTier | null {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "single" || normalized === "dual" || normalized === "special"
    ? normalized
    : null;
}

export function resolveBomAdminColourTier(sku: BomAdminColourTierFields): BomAdminColourTier {
  const explicitTier = explicitBomAdminColourTier(sku.colourTier);
  if (explicitTier) return explicitTier;

  const colourType = String(sku.colourType ?? "").trim().toLowerCase().replace("_", "-");
  if (["special", "matte", "pearl", "metallic", "black edition"].includes(colourType)) {
    return "special";
  }
  if (["dual", "two-tone", "dual-tone", "dual tone", "bi-color", "bi-colour"].includes(colourType)) {
    return "dual";
  }
  return "single";
}
