// @vitest-environment jsdom
import { afterEach, expect, it, vi } from "vitest";
import { api } from "../../api/client";

afterEach(() => vi.restoreAllMocks());

it("starts fresh sessions for newly selected files even with equal names and sizes", async () => {
  const initiate = vi.spyOn(api, "cocMatchInitiateUpload")
    .mockResolvedValueOnce({ uploadId: "old", totalChunks: 1, receivedChunks: [] })
    .mockResolvedValueOnce({ uploadId: "new", totalChunks: 1, receivedChunks: [] });
  const chunk = vi.spyOn(api, "cocMatchUploadChunk").mockResolvedValue({});
  vi.spyOn(api, "cocMatchCompleteUpload").mockResolvedValue({});
  expect(await api.cocUploadFile(new File(["old"], "same.zip"))).toBe("old");
  expect(await api.cocUploadFile(new File(["new"], "same.zip"))).toBe("new");
  expect(initiate.mock.calls).toEqual([["same.zip", 3], ["same.zip", 3]]);
  expect(chunk.mock.calls.map(([id]) => id)).toEqual(["old", "new"]);
});

it("discards the owned partial session when a transfer fails", async () => {
  vi.spyOn(api, "cocMatchInitiateUpload").mockResolvedValue({ uploadId: "partial", totalChunks: 1, receivedChunks: [] });
  vi.spyOn(api, "cocMatchUploadChunk").mockRejectedValue(new Error("transfer failed"));
  const discard = vi.spyOn(api, "cocDiscardUpload").mockResolvedValue({ deleted: true });
  await expect(api.cocUploadFile(new File(["zip"], "a.zip"))).rejects.toThrow("transfer failed");
  expect(discard).toHaveBeenCalledWith("partial");
});
