import { request, requestBlob } from "./core";
import type { AssetCatalog, AssetLinkChange, ConfigComparison, ConfigDetails, ConfigPreview } from "../types/materialAssets";
const root = "/order-genius/material-assets";
export const materialAssets = {
  catalog: (signal?: AbortSignal): Promise<AssetCatalog> => request(root, { signal }),
  bind: (changes: AssetLinkChange[]): Promise<{ saved: boolean }> => request(`${root}/bindings`, { method: "PUT", body: JSON.stringify({ changes }) }),
  createGroup: (body: FormData): Promise<{ groupId: string }> => request(`${root}/groups`, { method: "POST", body }),
  addPhotos: (id: string, body: FormData): Promise<{ saved: boolean }> => request(`${root}/groups/${id}/files`, { method: "POST", body }),
  deleteGroup: (id: string, rowVersion: number): Promise<{ deleted: boolean }> => request(`${root}/groups/${id}`, { method: "DELETE", body: JSON.stringify({ confirmed: true, rowVersion }) }),
  deletePhoto: (id: string, rowVersion: number): Promise<{ deleted: boolean }> => request(`${root}/files/${id}`, { method: "DELETE", body: JSON.stringify({ confirmed: true, rowVersion }) }),
  previewConfig: (body: FormData): Promise<ConfigPreview> => request(`${root}/config-preview`, { method: "POST", body }),
  uploadConfig: (body: FormData): Promise<{ versionId: string }> => request(`${root}/configurations`, { method: "POST", body }),
  deleteConfig: (id: string): Promise<{ deleted: boolean }> => request(`${root}/configurations/${id}?confirmed=true`, { method: "DELETE" }),
  config: (id: string, signal?: AbortSignal): Promise<ConfigDetails> => request(`${root}/configurations/${id}`, { signal }),
  compare: (keys: string[], signal?: AbortSignal): Promise<ConfigComparison> => request(`${root}/compare?${new URLSearchParams({ materialKeys: keys.join(",") })}`, { signal }),
  photo: (id: string, signal?: AbortSignal): Promise<Blob> => requestBlob(`${root}/files/${id}`, { signal }),
  download: (ids: string[]): Promise<Blob> => requestBlob(`${root}/download?${new URLSearchParams({ fileIds: ids.join(",") })}`),
};
export function saveAssetBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a"); link.href = url; link.download = filename; link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}
