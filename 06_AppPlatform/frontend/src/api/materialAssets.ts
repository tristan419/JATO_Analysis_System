import { request, requestBlob } from "./core";
import type { AssetCatalog, AssetLinkChange, AssetUploadState, AssetUploadTarget, AssetTrash, ConfigComparison, ConfigDetails, ConfigPreview } from "../types/materialAssets";
const root = "/order-genius/material-assets";
export const materialAssets = {
  catalog: (signal?: AbortSignal): Promise<AssetCatalog> => request(root, { signal }),
  bind: (changes: AssetLinkChange[]): Promise<{ saved: boolean }> => request(`${root}/bindings`, { method: "PUT", body: JSON.stringify({ changes }) }),
  deleteGroup: (id: string, rowVersion: number): Promise<{ deleted: boolean }> => request(`${root}/groups/${id}`, { method: "DELETE", body: JSON.stringify({ confirmed: true, rowVersion }) }),
  deletePhoto: (id: string, rowVersion: number): Promise<{ deleted: boolean }> => request(`${root}/files/${id}`, { method: "DELETE", body: JSON.stringify({ confirmed: true, rowVersion }) }),
  deleteConfig: (id: string): Promise<{ deleted: boolean }> => request(`${root}/configurations/${id}?confirmed=true`, { method: "DELETE" }),
  config: (id: string, signal?: AbortSignal): Promise<ConfigDetails> => request(`${root}/configurations/${id}`, { signal }),
  compare: (keys: string[], signal?: AbortSignal): Promise<ConfigComparison> => request(`${root}/compare?${new URLSearchParams({ materialKeys: keys.join(",") })}`, { signal }),
  photo: (id: string, signal?: AbortSignal, variant: "thumbnail" | "display" = "thumbnail"): Promise<Blob> => requestBlob(`${root}/files/${id}?variant=${variant}`, { signal }),
  download: (ids: string[]): Promise<Blob> => requestBlob(`${root}/download?${new URLSearchParams({ fileIds: ids.join(",") })}`),
  trash: (signal?: AbortSignal): Promise<AssetTrash> => request(`${root}/trash`, { signal }),
  restore: (id: string): Promise<{ restored: boolean }> => request(`${root}/trash/${id}/restore`, { method: "POST" }),
  purge: (id: string): Promise<{ deleted: boolean }> => request(`${root}/trash/${id}?confirmed=true`, { method: "DELETE" }),
  startUpload: (target: AssetUploadTarget, file: File, fileFingerprint: string, signal: AbortSignal): Promise<AssetUploadState> => request(`${root}/uploads`, { method: "POST", signal, body: JSON.stringify({ ...target, filename: file.name, sizeBytes: file.size, fileFingerprint }) }),
  uploadPart: (id: string, part: number, body: Blob, sha256: string, signal: AbortSignal): Promise<AssetUploadState> => request(`${root}/uploads/${id}/parts/${part}`, { method: "PUT", body, signal, headers: { "Content-Type": "application/octet-stream", "X-Chunk-SHA256": sha256 } }),
  previewUpload: (id: string, signal: AbortSignal): Promise<ConfigPreview> => request(`${root}/uploads/${id}/preview`, { method: "POST", signal }),
  saveUpload: (id: string, signal: AbortSignal, sourceColumn?: number): Promise<{ saved: boolean; groupId: string | null; rowVersion: number | null }> => request(`${root}/uploads/${id}/save`, { method: "POST", signal, body: JSON.stringify({ sourceColumn }) }),
  cancelUpload: (id: string): Promise<{ cancelled: boolean; saved?: boolean }> => request(`${root}/uploads/${id}`, { method: "DELETE" }),
};
export function saveAssetBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a"); link.href = url; link.download = filename; link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
