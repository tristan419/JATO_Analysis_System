// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { materialAssets } from "../../api/materialAssets";
import { MaterialAssetsPage } from "../../pages/MaterialAssetsPage";
import { MaterialAssetInspector } from "../../components/orderGenius/MaterialAssetInspector";
import { MaterialAssetUpload } from "../../components/orderGenius/MaterialAssetUpload";
import { MaterialAssetDownload } from "../../components/orderGenius/MaterialAssetDownload";
import * as assetApi from "../../api/materialAssets";
import type { AssetCatalog, AssetDownloadPreview, AssetMaterial } from "../../types/materialAssets";
const auth = vi.hoisted(() => ({ role: "editor", brands: ["JAECOO"] }));
vi.mock("../../contexts/AuthContext", () => ({ useAuth: () => ({ user: { username: "user", role: auth.role, primaryCountry: "CH", secondaryCountries: [], brands: auth.brands }, token: "test", profileLoaded: true }) }));
const t = (en: string): string => en;
const base = (key: string, model = "J7", historical = false): AssetMaterial => ({ key, brand: "JAECOO", modelName: model, bomTemplate: `T7150D**${key}`, version: "Large battery FWD", powertrain: "PHEV", effectiveFrom: "2026-01-01", effectiveTo: null, historical, colours: [{ key: "code:CL", code: "CL", name: "Black", hex: "#000000" }], interiors: [{ key: "name:BLACK", code: null, name: "Black", hex: null }], notes: [], issues: [] });
function catalog(): AssetCatalog {
  return { today: "2026-10-31", materials: [base("A"), base("B"), base("C", "J8"), base("H", "J7", true)], bindings: [{ id: "link", materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "album", versionId: null, rowVersion: 1 }], groups: [{ id: "album", brand: "JAECOO", modelName: "J7", kind: "exterior", colourKey: "code:CL", name: "Shared black", rowVersion: 1, references: ["A"], files: [{ id: "photo", filename: "black.png", contentType: "image/png", sha256: "hash" }] }], versions: [] };
}
const blobArrayBuffer = Object.getOwnPropertyDescriptor(Blob.prototype, "arrayBuffer");
function downloadPreview(month = "2026-10"): AssetDownloadPreview {
  return { referenceMonth: month, signature: "a".repeat(64), totalFiles: 2, totalBytes: 2000, maxBytes: 512 * 1024 * 1024,
    materials: ["A", "B"].map(key => ({ key, brand: "JAECOO", modelName: "J7", bomTemplate: `T7150D**${key}`, version: key === "A" ? "FWD" : "AWD", effectiveFrom: "2026-10-12", effectiveTo: null, issues: [],
      groups: [{ key: `${key}:exterior:CL`, kind: "exterior", label: "CL", colourKey: "CL", bindingVersion: 1, sourceId: "album", files: [{ id: "photo", filename: "black.png", path: `${key}/Exterior/CL/black.png`, bytes: 1000, contentType: "image/png", sha256: "hash" }] },
        { key: `${key}:config:`, kind: "config", label: "Configuration", colourKey: "", bindingVersion: 0, sourceId: null, files: [] }] })) };
}
beforeEach(() => {
  auth.role = "editor"; localStorage.clear(); vi.spyOn(materialAssets, "catalog").mockResolvedValue(catalog());
  Object.defineProperty(Blob.prototype, "arrayBuffer", { configurable: true, value: async () => new ArrayBuffer(8) });
  vi.stubGlobal("crypto", { subtle: { digest: async () => new Uint8Array(32).buffer } });
  vi.spyOn(materialAssets, "startUpload").mockResolvedValue({ uploadId: "upload-test", chunkSize: 1024 * 1024, totalChunks: 1, receivedChunks: [], uploadedBytes: 0 });
  vi.spyOn(materialAssets, "uploadPart").mockResolvedValue({ uploadId: "upload-test", chunkSize: 1024 * 1024, totalChunks: 1, receivedChunks: [1], uploadedBytes: 4 });
});
afterEach(() => {
  cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals();
  if (blobArrayBuffer) Object.defineProperty(Blob.prototype, "arrayBuffer", blobArrayBuffer); else Reflect.deleteProperty(Blob.prototype, "arrayBuffer");
});
describe("Material Assets", () => {
  it("sends monthly ZIP parameters as JSON through the actual binary client", async () => {
    const fetch = vi.fn().mockResolvedValue(new Response("zip", { status: 200, headers: { "Content-Type": "application/zip" } }));
    vi.stubGlobal("fetch", fetch);
    await materialAssets.downloadMonth("2026-10", "a".repeat(64), [], new AbortController().signal);
    const options = fetch.mock.calls[0][1] as RequestInit;
    expect(new Headers(options.headers).get("Content-Type")).toBe("application/json");
    expect(JSON.parse(String(options.body))).toEqual({ referenceMonth: "2026-10", signature: "a".repeat(64), selected: [] });
  });
  it("exposes monthly downloads to Order Filler without adding write permissions", async () => {
    auth.role = "order_filler";
    vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue(downloadPreview());
    render(<MemoryRouter><MaterialAssetsPage /></MemoryRouter>);
    await screen.findByRole("heading", { name: "T7150D**A" });
    fireEvent.click(screen.getByRole("button", { name: "Download" }));
    await screen.findByRole("button", { name: "Download all 2" });
    expect(screen.queryByRole("button", { name: "Upload" })).toBeNull();
    expect(screen.queryByRole("button", { name: "Recycle bin" })).toBeNull();
    expect(materialAssets.downloadPreview).toHaveBeenCalledWith("2026-10", expect.any(AbortSignal));
  });
  it("downloads all with zero selection and only explicit material groups when selected", async () => {
    vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue(downloadPreview());
    const download = vi.spyOn(materialAssets, "downloadMonth").mockResolvedValue(new Blob(["zip"]));
    const save = vi.spyOn(assetApi, "saveAssetBlob").mockImplementation(() => undefined);
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    fireEvent.click(await screen.findByRole("button", { name: "Download all 2" }));
    await waitFor(() => expect(save).toHaveBeenCalledOnce());
    expect(download).toHaveBeenLastCalledWith("2026-10", "a".repeat(64), [], expect.any(AbortSignal));
    fireEvent.click(screen.getByLabelText("Select material T7150D**A"));
    fireEvent.click(screen.getByRole("button", { name: "Download selected 1" }));
    await waitFor(() => expect(save).toHaveBeenCalledTimes(2));
    expect(download).toHaveBeenLastCalledWith("2026-10", "a".repeat(64), ["A:exterior:CL"], expect.any(AbortSignal));
    fireEvent.click(screen.getByRole("button", { name: "Clear selection" }));
    expect(screen.getByRole("button", { name: "Download all 2" })).toBeTruthy();
    expect(materialAssets.startUpload).not.toHaveBeenCalled();
  });
  it("selects an entire model independently of Compare selection", async () => {
    vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue(downloadPreview());
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    fireEvent.click(await screen.findByRole("checkbox", { name: "Select model JAECOO · J7" }));
    expect(screen.getByRole<HTMLInputElement>("checkbox", { name: "Select material T7150D**A" }).checked).toBe(true);
    expect(screen.getByRole("button", { name: "Download selected 2" })).toBeTruthy();
  });
  it("selects configuration separately and shows partial parent selection", async () => {
    const data = downloadPreview();
    data.materials[0].groups[1].files = [{ id: "config", filename: "config.xlsx", path: "A/Configuration/config.xlsx", bytes: 1000, contentType: "xlsx", sha256: "config" }];
    vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue(data);
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    await screen.findByRole("button", { name: "Download all 3" });
    fireEvent.click(screen.getByText("FWD"));
    fireEvent.click(screen.getByLabelText("Select Configuration Configuration T7150D**A"));
    expect(screen.getByRole("button", { name: "Download selected 1" })).toBeTruthy();
    expect(screen.getByLabelText<HTMLInputElement>("Select material T7150D**A").indeterminate).toBe(true);
    expect(screen.getByLabelText<HTMLInputElement>("Select Configuration Configuration T7150D**B").disabled).toBe(true);
  });
  it("immediately clears month selection and ignores a late old-month response", async () => {
    let old: ((data: AssetDownloadPreview) => void) | undefined;
    vi.spyOn(materialAssets, "downloadPreview").mockImplementation(month => month === "2026-10" ? new Promise(resolve => { old = resolve; }) : Promise.resolve(downloadPreview(month)));
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    fireEvent.change(screen.getByLabelText("Month"), { target: { value: "2026-11" } });
    await screen.findByRole("button", { name: "Download all 2" });
    fireEvent.click(screen.getByLabelText("Select material T7150D**A"));
    old?.({ ...downloadPreview(), totalFiles: 99, materials: [] });
    await waitFor(() => expect(screen.getByRole("button", { name: "Download selected 1" })).toBeTruthy());
    fireEvent.change(screen.getByLabelText("Month"), { target: { value: "2026-12" } });
    expect(screen.getByRole<HTMLButtonElement>("button", { name: "Download all 0" }).disabled).toBe(true);
    await screen.findByRole("button", { name: "Download all 2" });
  });
  it("keeps empty months empty, rather than falling back to the whole catalog", async () => {
    vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue({ ...downloadPreview(), materials: [], totalFiles: 0, totalBytes: 0 });
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    await screen.findByText("No effective materials this month");
    expect(screen.getByRole<HTMLButtonElement>("button", { name: "Download all 0" }).disabled).toBe(true);
  });
  it("counts repeated package bytes and requires grouping rather than truncating", async () => {
    vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue({ ...downloadPreview(), maxBytes: 1500 });
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    await screen.findByRole("alert");
    expect(screen.getByRole<HTMLButtonElement>("button", { name: "Download all 2" }).disabled).toBe(true);
    fireEvent.click(screen.getByLabelText("Select material T7150D**B"));
    expect(screen.getByRole<HTMLButtonElement>("button", { name: "Download selected 1" }).disabled).toBe(false);
  });
  it("preserves failures as errors and offers a direct reload of stale preview", async () => {
    const preview = vi.spyOn(materialAssets, "downloadPreview").mockResolvedValue(downloadPreview());
    vi.spyOn(materialAssets, "downloadMonth").mockRejectedValue(new Error("409 Assets changed; reload preview"));
    render(<MaterialAssetDownload initialMonth="2026-10" t={t} onClose={vi.fn()} />);
    fireEvent.click(await screen.findByRole("button", { name: "Download all 2" }));
    fireEvent.click(await screen.findByRole("button", { name: "Reload preview" }));
    await waitFor(() => expect(preview).toHaveBeenCalledTimes(2));
  });
  it("opens the requested material inside the selection page and exposes Close", async () => {
    const close = vi.fn();
    render(<MemoryRouter><MaterialAssetsPage request={{ materials: [{ brand: "JAECOO", modelName: "J8", bomTemplate: "T7150D**C" }], compare: false }} onClose={close} /></MemoryRouter>);
    await screen.findByRole("heading", { name: "T7150D**C" });
    expect(document.querySelector(".mw-workspace.is-embedded")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Close Material Assets" })); expect(close).toHaveBeenCalledOnce();
  });
  it("defaults EN, groups bases by model, keeps historical collapsed and has no BOM edit jump", async () => {
    render(<MemoryRouter><MaterialAssetsPage /></MemoryRouter>); await screen.findByRole("heading", { name: "T7150D**A" });
    expect(document.querySelector<HTMLDetailsElement>(".mw-historical")?.open).toBe(false);
    expect(screen.queryByText(/Edit.*BOM/)).toBeNull(); expect(screen.getByRole("button", { name: "Upload" })).toBeTruthy();
  });
  it("Order Filler can compare but never sees mutation controls", async () => {
    auth.role = "order_filler"; render(<MemoryRouter><MaterialAssetsPage /></MemoryRouter>); await screen.findByRole("heading", { name: "T7150D**A" });
    expect(screen.queryByRole("button", { name: "Upload" })).toBeNull(); expect(screen.queryByText("Add photos")).toBeNull();
    fireEvent.click(screen.getByLabelText("Compare T7150D**A")); fireEvent.click(screen.getByLabelText("Compare T7150D**C"));
    expect(screen.getByRole<HTMLButtonElement>("button", { name: "Compare 2" }).disabled).toBe(false);
  });
  it("reuses same-model colour references in one explicit atomic request", async () => {
    const bind = vi.spyOn(materialAssets, "bind").mockResolvedValue({ saved: true });
    const data = catalog(); render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Reuse…" }));
    const dialog = screen.getByRole("dialog"); expect(dialog.textContent).not.toContain("T7150D**C");
    const labels = [...dialog.querySelectorAll("label")]; const target = labels.find(l => l.textContent?.includes("T7150D**B"))?.querySelector("input"); expect(target).toBeTruthy();
    if (target) fireEvent.click(target); expect(bind).not.toHaveBeenCalled(); fireEvent.click(screen.getByRole("button", { name: "Apply to 1" }));
    await waitFor(() => expect(bind).toHaveBeenCalledWith([{ materialKey: "B", kind: "exterior", colourKey: "code:CL", groupId: "album", versionId: null, rowVersion: 0 }]));
  });
  it("requires explicit config column, sends the original file and selected base version", async () => {
    vi.spyOn(materialAssets, "previewUpload").mockResolvedValue({ columns: [{ column: 26, name: "Large battery" }], warnings: [] });
    const upload = vi.spyOn(materialAssets, "saveUpload").mockResolvedValue({ saved: true, groupId: null, rowVersion: null }); const data = catalog();
    render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Upload" }));
    const fileInput = screen.getByRole("dialog").querySelector<HTMLInputElement>('input[type="file"]'); const file = new File(["xlsx"], "source.xlsx");
    if (fileInput) fireEvent.change(fileInput, { target: { files: [file] } });
    const select = await screen.findByLabelText("Configuration column"); expect(screen.getByRole<HTMLButtonElement>("button", { name: "Save version" }).disabled).toBe(true);
    fireEvent.change(select, { target: { value: "26" } }); fireEvent.click(screen.getByRole("button", { name: "Save version" }));
    await waitFor(() => expect(upload).toHaveBeenCalledOnce()); expect(upload.mock.calls[0][0]).toBe("upload-test"); expect(upload.mock.calls[0][2]).toBe(26);
    expect(materialAssets.startUpload).toHaveBeenCalledWith(expect.objectContaining({ materialKey: "A", kind: "config", rowVersion: 0 }), file, expect.any(String), expect.any(AbortSignal));
  });
  it("failed upload retains input for retry and never claims saved", async () => {
    vi.spyOn(materialAssets, "previewUpload").mockRejectedValue(new Error("422 matrix unavailable")); const data = catalog();
    render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Upload" })); const input = screen.getByRole("dialog").querySelector<HTMLInputElement>('input[type="file"]');
    if (input) fireEvent.change(input, { target: { files: [new File(["bad"], "bad.xlsx")] } });
    await screen.findByText("422 matrix unavailable"); expect(screen.getByRole("button", { name: "Resume" })).toBeTruthy();
  });
  it("exposes the recycle bin only to maintainers and separates the two folders", async () => {
    vi.spyOn(materialAssets, "trash").mockResolvedValue({ usedBytes: 12, maxBytes: 2e9, entries: [{ id: "deleted", filename: "old.xlsx", contentType: "x", sha256: "s", folder: "configurations", modelName: "J7", label: "T7150D**A", bytes: 12, deletedAt: "2026-11-01", deletedBy: "e" }] });
    render(<MemoryRouter><MaterialAssetsPage /></MemoryRouter>); await screen.findByRole("heading", { name: "T7150D**A" });
    fireEvent.click(screen.getByRole("button", { name: "Recycle bin" })); await screen.findByRole("tab", { name: "Configurations 1" });
    expect(screen.queryByText("old.xlsx")).toBeNull(); fireEvent.click(screen.getByRole("tab", { name: "Configurations 1" })); expect(screen.getByText("old.xlsx")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Delete permanently" })); expect(screen.getByText(/cannot be undone/)).toBeTruthy();
  });
  it("shows corner removal as a recoverable shared-photo action", () => {
    const data = catalog(); render(<MaterialAssetInspector material={data.materials[0]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    const button = screen.getByRole("button", { name: "Move to recycle bin black.png" }); expect(button.textContent).toBe("×");
    fireEvent.click(button); expect(screen.getByText(/1 material links are affected/)).toBeTruthy(); expect(screen.getByText(/Move this shared photo to the recycle bin/)).toBeTruthy();
  });
  it("reuses an authorized same-model configuration by UUID without uploading another file", async () => {
    const data = catalog();
    data.versions = [{ id: "shared-config", brand: "JAECOO", modelName: "J7", bomTemplate: data.materials[0].bomTemplate, number: 1, parentId: null, createdBy: "e", createdAt: "2026-11-01", source: "source.xlsx", references: ["A"], files: [{ id: "source", filename: "source.xlsx", contentType: "xlsx", sha256: "hash" }] }];
    const bind = vi.spyOn(materialAssets, "bind").mockResolvedValue({ saved: true });
    render(<MaterialAssetInspector material={data.materials[1]} catalog={data} editor t={t} scope="s" onReload={async () => undefined} />);
    fireEvent.click(screen.getByRole("button", { name: "Reuse configuration" })); fireEvent.change(screen.getByLabelText("Source version"), { target: { value: "shared-config" } });
    fireEvent.click(screen.getByRole("button", { name: "Use configuration" }));
    await waitFor(() => expect(bind).toHaveBeenCalledWith([{ materialKey: "B", kind: "config", colourKey: "", groupId: null, versionId: "shared-config", rowVersion: 0 }]));
    expect(materialAssets.startUpload).not.toHaveBeenCalled();
  });
  it("does not claim cancellation when the server confirms that an ambiguous save already completed", async () => {
    vi.mocked(materialAssets.uploadPart).mockRejectedValue(new Error("connection interrupted"));
    vi.spyOn(materialAssets, "cancelUpload").mockResolvedValue({ cancelled: false, saved: true }); const done = vi.fn();
    render(<MaterialAssetUpload label="T7150D**A" target={{ materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "album", rowVersion: 1 }} files={[new File(["data"], "photo.png")]} t={t} onDone={done} />);
    await screen.findByRole("button", { name: "Resume" }); fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
    await waitFor(() => expect(done).toHaveBeenCalledWith(1, false));
  });
  it("cancels an active chunk request and never saves the upload", async () => {
    vi.mocked(materialAssets.uploadPart).mockImplementation((_id, _part, _body, _hash, signal) => new Promise((_resolve, reject) => signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")))));
    const cancel = vi.spyOn(materialAssets, "cancelUpload").mockResolvedValue({ cancelled: true }); const done = vi.fn(); const save = vi.spyOn(materialAssets, "saveUpload");
    render(<MaterialAssetUpload label="T7150D**A" target={{ materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "album", rowVersion: 1 }} files={[new File(["data"], "photo.png")]} t={t} onDone={done} />);
    await waitFor(() => expect(materialAssets.uploadPart).toHaveBeenCalledOnce()); fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
    await waitFor(() => expect(done).toHaveBeenCalledWith(0, true)); expect(cancel).toHaveBeenCalledWith("upload-test"); expect(save).not.toHaveBeenCalled();
  });
  it("does not re-enable Resume while cancellation is still deleting partial chunks", async () => {
    vi.mocked(materialAssets.uploadPart).mockRejectedValue(new Error("offline"));
    let finish: ((value: { cancelled: boolean }) => void) | undefined;
    vi.spyOn(materialAssets, "cancelUpload").mockImplementation(() => new Promise(resolve => { finish = resolve; })); const done = vi.fn();
    render(<MaterialAssetUpload label="T7150D**A" target={{ materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "album", rowVersion: 1 }} files={[new File(["data"], "photo.png")]} t={t} onDone={done} />);
    await screen.findByRole("button", { name: "Resume" }); fireEvent.click(screen.getByRole("button", { name: "Cancel upload" }));
    await waitFor(() => expect(materialAssets.cancelUpload).toHaveBeenCalledOnce());
    expect(screen.queryByRole("button", { name: "Resume" })).toBeNull();
    finish?.({ cancelled: true }); await waitFor(() => expect(done).toHaveBeenCalledWith(0, true));
  });
  it("pauses and resumes through the existing received-chunk manifest", async () => {
    vi.mocked(materialAssets.uploadPart).mockImplementationOnce((_id, _part, _body, _hash, signal) => new Promise((_resolve, reject) => signal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")))));
    vi.spyOn(materialAssets, "saveUpload").mockResolvedValue({ saved: true, groupId: "album", rowVersion: 2 }); const done = vi.fn();
    render(<MaterialAssetUpload label="T7150D**A" target={{ materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "album", rowVersion: 1 }} files={[new File(["data"], "photo.png")]} t={t} onDone={done} />);
    await waitFor(() => expect(materialAssets.uploadPart).toHaveBeenCalledOnce()); fireEvent.click(screen.getByRole("button", { name: "Pause" }));
    await screen.findByRole("button", { name: "Resume" }); fireEvent.click(screen.getByRole("button", { name: "Resume" }));
    await waitFor(() => expect(done).toHaveBeenCalledWith(1, false)); expect(materialAssets.startUpload).toHaveBeenCalledTimes(2);
  });
});
