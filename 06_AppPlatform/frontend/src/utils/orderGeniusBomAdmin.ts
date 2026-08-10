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
