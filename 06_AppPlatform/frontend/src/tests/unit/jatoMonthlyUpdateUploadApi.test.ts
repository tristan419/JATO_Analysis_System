// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

const SHA256_OF_TEST_FILE = "0".repeat(64);

function uploadSession(status: "ready" | "consumed" | "abandoned") {
  return {
    uploadId: "upload-123",
    filename: "patch.xlsx",
    sizeBytes: 4,
    chunkSize: 4,
    totalChunks: 1,
    receivedChunkCount: 1,
    receivedChunks: [1],
    chunkDigests: { "1": SHA256_OF_TEST_FILE },
    uploadedBytes: 4,
    status,
    consumedJobId: status === "consumed" ? "jato-update-existing" : null,
  };
}

describe("JATO monthly update upload job creation", () => {
  afterEach(() => {
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it("does not repeat the create-job POST after a generic server error", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/msrp/monthly-update-uploads/initiate")) {
        return Response.json({ item: uploadSession("ready") });
      }
      if (url.endsWith("/msrp/monthly-update-jobs/from-upload")) {
        return new Response("gateway unavailable", { status: 502 });
      }
      if (url.endsWith("/msrp/monthly-update-uploads/upload-123") && !init?.method) {
        return Response.json({ item: uploadSession("ready") });
      }
      throw new Error(`Unexpected request: ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("crypto", {
      subtle: {
        digest: vi.fn(async () => new Uint8Array(32).buffer),
      },
    });

    await expect(
      api.createJatoMonthlyUpdateJob(new File([new Uint8Array([1, 2, 3, 4])], "patch.xlsx"))
    ).rejects.toThrow("502 gateway unavailable");

    const createRequests = fetchMock.mock.calls.filter(([input, init]) => (
      String(input).endsWith("/msrp/monthly-update-jobs/from-upload")
      && init?.method === "POST"
    ));
    expect(createRequests).toHaveLength(1);
  });

  it("reads the existing job only after the upload session is confirmed consumed", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/msrp/monthly-update-uploads/initiate")) {
        return Response.json({ item: uploadSession("ready") });
      }
      if (url.endsWith("/msrp/monthly-update-jobs/from-upload")) {
        return new Response("gateway unavailable", { status: 502 });
      }
      if (url.endsWith("/msrp/monthly-update-uploads/upload-123") && !init?.method) {
        return Response.json({ item: uploadSession("consumed") });
      }
      if (url.endsWith("/msrp/monthly-update-jobs/jato-update-existing") && !init?.method) {
        return Response.json({ item: { jobId: "jato-update-existing", status: "running" } });
      }
      throw new Error(`Unexpected request: ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("crypto", {
      subtle: {
        digest: vi.fn(async () => new Uint8Array(32).buffer),
      },
    });

    const response = await api.createJatoMonthlyUpdateJob(
      new File([new Uint8Array([1, 2, 3, 4])], "patch.xlsx")
    );

    expect(response.item.jobId).toBe("jato-update-existing");
    expect(fetchMock.mock.calls.filter(([input, init]) => (
      String(input).endsWith("/msrp/monthly-update-jobs/from-upload")
      && init?.method === "POST"
    ))).toHaveLength(1);
    expect(fetchMock.mock.calls.some(([input, init]) => (
      String(input).endsWith("/msrp/monthly-update-jobs/jato-update-existing")
      && !init?.method
    ))).toBe(true);
  });

  it("abandons an unconsumed upload and clears every matching local resume key", async () => {
    localStorage.setItem("jato_monthly_update_upload_session:resume-a", "upload-123");
    localStorage.setItem("jato_monthly_update_upload_session:resume-b", "another-upload");
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      expect(String(input)).toMatch(/\/msrp\/monthly-update-uploads\/upload-123\/abandon$/);
      expect(init?.method).toBe("POST");
      return Response.json({ item: uploadSession("abandoned") });
    });
    vi.stubGlobal("fetch", fetchMock);

    const session = await api.abandonJatoMonthlyUpdateUpload("upload-123");

    expect(session.status).toBe("abandoned");
    expect(localStorage.getItem("jato_monthly_update_upload_session:resume-a")).toBeNull();
    expect(localStorage.getItem("jato_monthly_update_upload_session:resume-b")).toBe("another-upload");
  });
});
