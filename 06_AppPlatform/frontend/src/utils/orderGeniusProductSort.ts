function brandDisplayRank(brand: string): number {
  const upper = brand.toUpperCase();
  return upper.includes("OMODA") ? 0 : upper.includes("JAECOO") ? 1 : 2;
}

function firstModelNumber(value: string): number {
  const match = value.match(/\d+/);
  return match ? Number(match[0]) : Number.MAX_SAFE_INTEGER;
}

function powertrainDisplayRank(value: string): number {
  const upper = value.toUpperCase();
  return upper === "ICE" ? 0 : upper === "HEV" ? 1 : upper === "BEV" ? 2 : upper === "PHEV" || upper.includes("SHS") ? 3 : 9;
}

export function compareProductModels(
  brandA: string, modelA: string, ptA: string,
  brandB: string, modelB: string, ptB: string,
): number {
  return brandDisplayRank(brandA) - brandDisplayRank(brandB)
    || brandA.localeCompare(brandB)
    || firstModelNumber(modelA) - firstModelNumber(modelB)
    || powertrainDisplayRank(ptA) - powertrainDisplayRank(ptB)
    || ptA.localeCompare(ptB) || modelA.localeCompare(modelB);
}
