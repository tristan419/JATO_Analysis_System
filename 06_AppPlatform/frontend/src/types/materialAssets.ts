export type AssetKind = "config" | "exterior" | "interior";
export interface MaterialAssetRequest {
  materials: Array<{ brand: string; modelName: string; bomTemplate: string }>;
  compare: boolean;
}
export interface AssetColour { key: string; code: string | null; name: string | null; hex: string | null }
export interface AssetMaterial {
  key: string; brand: string; modelName: string; bomTemplate: string; version: string;
  powertrain: string | null; effectiveFrom: string | null; effectiveTo: string | null;
  historical: boolean; colours: AssetColour[]; interiors: AssetColour[]; notes: string[]; issues: string[];
}
export interface AssetFile { id: string; filename: string; contentType: string; sha256: string }
export interface AssetGroup {
  id: string; brand: string; modelName: string; kind: "exterior" | "interior"; colourKey: string;
  name: string; rowVersion: number; references: string[]; files: AssetFile[];
}
export interface AssetVersion {
  id: string; brand: string; modelName: string; bomTemplate: string; number: number;
  parentId: string | null; createdBy: string | null; createdAt: string; source: string | null;
  references: string[]; files: AssetFile[];
}
export interface AssetBinding {
  id: string; materialKey: string; kind: AssetKind; colourKey: string;
  groupId: string | null; versionId: string | null; rowVersion: number;
}
export interface AssetCatalog {
  today: string; materials: AssetMaterial[]; bindings: AssetBinding[]; groups: AssetGroup[]; versions: AssetVersion[];
}
export interface AssetLinkChange {
  materialKey: string; kind: AssetKind; colourKey: string; groupId: string | null; versionId: string | null; rowVersion: number;
}
export interface ConfigCell {
  category: string; feature_name: string; raw_value: string | null; normalized_value: string | null; availability: string | null;
  source_row: number; source_column: string;
}
export interface ConfigPreview { columns: Array<{ column: number; name: string }>; warnings: string[] }
export interface ConfigDetails { id: string; source: string | null; values: ConfigCell[] }
export interface ConfigComparison {
  materials: Array<{ key: string; bomTemplate: string; modelName: string; versionId: string; version: number }>;
  rows: Array<{ category: string; featureName: string; values: Array<ConfigCell | null> }>;
}
