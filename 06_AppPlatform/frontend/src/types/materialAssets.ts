export type AssetKind = "config" | "exterior" | "interior";
export interface MaterialIdentity { brand: string; modelName: string; bomTemplate: string }
export interface MaterialAssetRequest {
  materials: MaterialIdentity[];
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
  feature_name_zh?: string; feature_name_en?: string;
  source_row: number; source_column: string; source_sheet?: string; unit?: string | null;
}
export interface ConfigPreview {
  columns: Array<{ column: number; sheet: string; name: string; materials: string[]; hidden: boolean; issues: string[]; matchesTarget: boolean;
    featureCount: number; filledCount: number; sample: ConfigCell[] }>;
  warnings: string[]; suggestedColumn: number | null; suggestedSheet: string | null; materialHeaders: boolean;
}
export interface ConfigDetails { id: string; source: string | null; values: ConfigCell[] }
export interface ConfigComparison {
  materials: Array<{ key: string; bomTemplate: string; modelName: string; versionId: string; version: number }>;
  rows: Array<{ category: string; featureName: string; featureNameEn?: string | null; values: Array<ConfigCell | null> }>;
}
export interface AssetUploadTarget { materialKey: string; kind: AssetKind; colourKey: string; groupId: string | null; rowVersion: number }
export interface AssetUploadState { uploadId: string; chunkSize: number; totalChunks: number; receivedChunks: number[]; uploadedBytes: number }
export interface AssetTrashEntry extends AssetFile { folder: "photos" | "configurations"; modelName: string; label: string; bytes: number; deletedAt: string; deletedBy: string }
export interface AssetTrash { usedBytes: number; maxBytes: number; entries: AssetTrashEntry[] }
export interface AssetDownloadGroup {
  key: string; kind: AssetKind; label: string; colourKey: string; bindingVersion: number; sourceId: string | null;
  files: Array<AssetFile & { path: string; bytes: number }>;
}
export interface AssetDownloadMaterial extends MaterialIdentity {
  key: string; version: string; effectiveFrom: string | null; effectiveTo: string | null;
  issues: string[]; groups: AssetDownloadGroup[];
}
export interface AssetDownloadPreview {
  referenceMonth: string; signature: string; materials: AssetDownloadMaterial[];
  totalFiles: number; totalBytes: number; maxBytes: number;
}
