export interface CocLibrarySource {
  id: string;
  filename: string;
  status: "indexing" | "review" | "active" | "failed";
  pdfCount: number;
  job: { status?: string; pdfCount?: number; invalidCount?: number; error?: string };
  resources?: { status?: string; rssBytes?: number | null; peakRssBytes?: number | null; rssWarningBytes?: number; rssLimitBytes?: number; terminationReason?: string | null };
}
export interface CocLibraryState {
  configured: boolean;
  vinCount: number;
  items: CocLibrarySource[];
  libraryBytes?: number;
  diskFreeBytes?: number;
}
export interface CocSourcePreview {
  sourceId: string;
  fingerprint: string;
  affectedPis: string[];
  affectedVehicles: number;
  items: Array<{ vin: string; status: "new" | "duplicate" | "conflict" | "invalid"; oldSha: string | null; newSha: string }>;
}
export interface CocDeletePreview {
  sourceId: string;
  fingerprint: string;
  lostCount: number;
  affectedPis: string[];
  affectedVehicles: number;
}
export interface PiCocLookup {
  total: number;
  available: number;
  awaitingVin: number;
  missing: number;
  items: Array<{ carCode: string; vin: string | null; status: "awaiting_vin" | "available" | "missing" }>;
}
