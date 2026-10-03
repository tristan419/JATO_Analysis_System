// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { CocMatchPage } from "../../pages/CocMatchPage";
import type { CocMatchJob } from "../../types";

vi.mock("../../contexts/AuthContext", () => ({ useAuth: () => ({ user: { role: "admin" } }) }));
vi.mock("../../components/CocLibraryPanel", () => ({ CocLibraryPanel: () => null }));
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it("downloads a completed one-off job PDF package without a library write", async () => {
  const job: CocMatchJob = { jobId: "coc-match-1234abcd", status: "success", country: "CH", month: "2026-09",
    fileExt: ".pdf", excelFilename: "vin.xlsx", archiveFilename: "nested.zip", pdfDownloadCount: 2,
    totalRows: 3, matchedCount: 2, missingCount: 1, triggeredBy: "admin", createdAt: "2026-10-04T00:00:00Z" };
  vi.spyOn(api, "cocMatchListJobs").mockResolvedValue({ items: [job] });
  vi.spyOn(api, "cocFillListJobs").mockResolvedValue({ items: [] });
  const download = vi.spyOn(api, "cocMatchGetPdfPackage").mockResolvedValue(new Blob(["pdf zip"]));
  const importSource = vi.spyOn(api, "cocLibraryImport");
  vi.stubGlobal("URL", { createObjectURL: vi.fn(() => "blob:test"), revokeObjectURL: vi.fn() });
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
  render(<CocMatchPage />);
  fireEvent.click(screen.getByRole("tab", { name: /COC 比对/ }));
  fireEvent.click((await screen.findAllByRole("button", { name: "Download PDFs / 下载PDF (2)" }))[0]);
  await waitFor(() => expect(download).toHaveBeenCalledWith(job.jobId));
  expect(importSource).not.toHaveBeenCalled();
  expect(screen.getByText(/PDF包含 2 个VIN，缺失 1 个/)).toBeTruthy();
  download.mockRejectedValueOnce(new Error("500 Internal Server Error"));
  await waitFor(() => expect(screen.getAllByRole("button", { name: "Download PDFs / 下载PDF (2)" })[0].hasAttribute("disabled")).toBe(false));
  fireEvent.click(screen.getAllByRole("button", { name: "Download PDFs / 下载PDF (2)" })[0]);
  await screen.findByText(/PDF包暂不可下载，请重新运行PDF比对/);
  expect(screen.queryByText(/500 Internal/)).toBeNull();
});
