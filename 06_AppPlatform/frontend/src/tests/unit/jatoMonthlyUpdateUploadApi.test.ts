// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

const SHA256_OF_TEST_FILE = "0".repeat(64);

function uploadSession(
  status: "ready" | "invalid" | "consumed" | "abandoned",
  failureCode = "DIGEST_TIMEOUT",
) {
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
    assembledPath: status === "invalid" ? "uploads/assembled/upload-safe.xlsx" : null,
    fileSha256: status === "invalid" ? SHA256_OF_TEST_FILE : null,
    failureDigest: status === "invalid"
      ? {
        code: failureCode,
        category: "resource",
        phase: "digesting",
        retryable: true,
        message: "digest timeout",
        sourceFeedback: null,
        technicalDetail: null,
        nextAction: "retry_digest",
      }
      : null,
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
        expect(JSON.parse(String(init?.body))).toEqual({ uploadId: "upload-123" });
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

  it.each([
    "DIGEST_TIMEOUT",
    "DIGEST_WORKER_LOST",
    "DIGEST_WORKER_SIGNALLED",
    "DIGEST_WORKER_EXITED",
    "DIGEST_RESULT_MISSING",
    "DIGEST_WORKER_UNAVAILABLE",
  ])("retries retryable digest failure %s without uploading file chunks again", async (failureCode) => {
    const progressStages: string[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/msrp/monthly-update-uploads/initiate")) {
        return Response.json({ item: uploadSession("invalid", failureCode) });
      }
      if (url.endsWith("/msrp/monthly-update-uploads/upload-123/retry-digest")) {
        expect(init?.method).toBe("POST");
        return Response.json({ item: uploadSession("ready") });
      }
      if (url.endsWith("/msrp/monthly-update-jobs/from-upload")) {
        return Response.json({
          item: {
            jobId: "jato-update-recovered",
            status: "queued",
          },
        });
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
      new File([new Uint8Array([1, 2, 3, 4])], "patch.xlsx"),
      (progress) => progressStages.push(progress.stage),
    );

    expect(response.item.jobId).toBe("jato-update-recovered");
    expect(progressStages).toContain("retrying");
    expect(fetchMock.mock.calls.filter(([_input, init]) => init?.method === "PUT")).toHaveLength(0);
    expect(fetchMock.mock.calls.filter(([input, init]) => (
      String(input).endsWith("/msrp/monthly-update-uploads/upload-123/retry-digest")
      && init?.method === "POST"
    ))).toHaveLength(1);
  });

  it("stops before job creation when the digest resource is quarantined", async () => {
    const quarantinedSession = {
      ...uploadSession("ready"),
      status: "digesting",
      digestPid: 24680,
      failureDigest: {
        code: "RESOURCE_QUARANTINED",
        category: "resource",
        phase: "digesting",
        retryable: false,
        message: "旧 digest worker 状态无法确认，请联系管理员。",
        sourceFeedback: null,
        technicalDetail: { digestPid: 24680 },
        nextAction: "contact_admin_verify_digest_process",
      },
    };
    const progressStages: string[] = [];
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/msrp/monthly-update-uploads/initiate")) {
        return Response.json({ item: quarantinedSession });
      }
      throw new Error(`Unexpected request: ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    vi.stubGlobal("crypto", {
      subtle: {
        digest: vi.fn(async () => new Uint8Array(32).buffer),
      },
    });

    await expect(api.createJatoMonthlyUpdateJob(
      new File([new Uint8Array([1, 2, 3, 4])], "patch.xlsx"),
      (progress) => progressStages.push(progress.stage),
    )).rejects.toThrow("旧 digest worker 状态无法确认");

    expect(progressStages).toContain("invalid");
    expect(fetchMock.mock.calls.some(([input]) => (
      String(input).endsWith("/msrp/monthly-update-jobs/from-upload")
    ))).toBe(false);
    expect(fetchMock.mock.calls.some(([input]) => (
      String(input).includes("/retry-digest")
    ))).toBe(false);
  });

  it("keeps the resume key when abandon returns resource quarantine", async () => {
    localStorage.setItem("jato_monthly_update_upload_session:resume-a", "upload-123");
    const fetchMock = vi.fn(async () => Response.json(
      {
        detail: {
          code: "RESOURCE_QUARANTINED",
          message: "旧 digest worker 仍存活。",
        },
      },
      { status: 409 },
    ));
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      api.abandonJatoMonthlyUpdateUpload("upload-123")
    ).rejects.toThrow("RESOURCE_QUARANTINED");
    expect(
      localStorage.getItem("jato_monthly_update_upload_session:resume-a")
    ).toBe("upload-123");
  });
});
