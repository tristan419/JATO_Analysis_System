import type { CSSProperties, ReactNode } from "react";
import type { MaterialIdentity } from "../../types/materialAssets";

export const materialIdentityKey = (material: MaterialIdentity): string => JSON.stringify([material.brand, material.modelName, material.bomTemplate]);
export function comparisonIndex(material: MaterialIdentity, selected: readonly MaterialIdentity[]): number {
  return selected.findIndex(item => materialIdentityKey(item) === materialIdentityKey(material));
}
export function comparisonStyle(index: number): CSSProperties | undefined {
  return index < 0 ? undefined : { backgroundColor: `hsl(265 76% ${96 - index * 3}%)`, color: "#38205c", borderColor: `hsl(265 50% ${84 - index * 3}%)` };
}
export function MaterialCompareLabel({ index, children }: { index: number; children: ReactNode }) {
  return <span className={index < 0 ? "" : "mw-compare-label"} style={comparisonStyle(index)}>{index >= 0 ? <b>{index + 1}</b> : null}{children}</span>;
}
