// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { CocLibraryPanel } from "../../components/CocLibraryPanel";
import type { CocSourcePreview } from "../../types/cocLibrary";

const preview: CocSourcePreview = { sourceId: "s", fingerprint: "f", affectedPis: ["PI-CH-1"], affectedVehicles: 2,
  items: [{ vin: "LVUGTB220TDE99425", status: "conflict", oldSha: "old", newSha: "new" }] };
beforeEach(() => {
  vi.spyOn(api, "cocLibrary").mockResolvedValue({ configured: true, vinCount: 1, items: [{ id: "s", filename: "source.zip", status: "review", pdfCount: 1, job: { invalidCount: 2 } }] });
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); });

it("requires explicit replacement selection and preview before append", async () => {
  vi.spyOn(api, "cocLibraryPreview").mockResolvedValue(preview);
  const activate = vi.spyOn(api, "cocLibraryActivate").mockResolvedValue({ status: "active" });
  render(<CocLibraryPanel />);
  fireEvent.click(await screen.findByRole("button", { name: "Preview / 预览" }));
  expect(screen.getByText(/2 invalid VIN filenames/)).toBeTruthy();
  await screen.findByText(/PI-CH-1/);
  expect(activate).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("checkbox"));
  fireEvent.click(screen.getByRole("button", { name: /Confirm append/ }));
  await waitFor(() => expect(activate).toHaveBeenCalledWith(preview, ["LVUGTB220TDE99425"]));
});

it("shows deletion impact and allows cancelling without mutation", async () => {
  vi.spyOn(api, "cocLibraryDeletePreview").mockResolvedValue({ sourceId: "s", fingerprint: "f", lostCount: 10, affectedVehicles: 2, affectedPis: ["PI-CH-1"] });
  const remove = vi.spyOn(api, "cocLibraryDelete").mockResolvedValue({ deleted: true });
  render(<CocLibraryPanel />);
  fireEvent.click(await screen.findByRole("button", { name: "Delete source / 删除来源" }));
  await screen.findByText(/10 VINs will lose/);
  fireEvent.click(screen.getByRole("button", { name: "Cancel / 取消" }));
  expect(remove).not.toHaveBeenCalled();
});

it("does not expose an import action when persistent storage is not configured", async () => {
  vi.mocked(api.cocLibrary).mockResolvedValue({ configured: false, vinCount: 0, items: [] });
  render(<CocLibraryPanel />);
  await screen.findByText(/在线库持久目录未配置/);
  expect(screen.getByRole("button", { name: "Upload & index / 上传并索引" }).hasAttribute("disabled")).toBe(true);
});

it("shows worker RAM, peak, limit and disk without claiming unavailable readings are zero", async () => {
  vi.mocked(api.cocLibrary).mockResolvedValue({ configured: true, vinCount: 1, libraryBytes: 1024 ** 2, diskFreeBytes: 2 * 1024 ** 3,
    items: [{ id: "s", filename: "source.zip", status: "indexing", pdfCount: 1, job: { status: "running" },
      resources: { status: "running", rssBytes: null, peakRssBytes: 128 * 1024 ** 2, rssLimitBytes: 1536 * 1024 ** 2 } }] });
  render(<CocLibraryPanel />);
  await screen.findByText(/Index RAM \/ 索引内存 Unavailable/);
  expect(screen.getByText(/Peak \/ 峰值 128.0 MiB/).textContent).toContain("Limit / 上限 1536.0 MiB");
  expect(screen.getByText(/Library disk \/ 库磁盘/).textContent).toContain("2048.0 MiB");
});

it("explains explicit re-upload review while reusing the existing source", async () => {
  vi.spyOn(api, "cocUploadFile").mockResolvedValue("upload");
  vi.spyOn(api, "cocLibraryImport").mockResolvedValue({ sourceId: "s", duplicate: true, needsReview: true });
  const { container } = render(<CocLibraryPanel />);
  await screen.findByRole("button", { name: "Preview / 预览" });
  const input = container.querySelector<HTMLInputElement>('input[type="file"]');
  expect(input).not.toBeNull();
  fireEvent.change(input!, { target: { files: [new File(["zip"], "source.zip")] } });
  fireEvent.click(screen.getByRole("button", { name: "Upload & index / 上传并索引" }));
  await screen.findByText(/已复用原包及缓存/);
});
