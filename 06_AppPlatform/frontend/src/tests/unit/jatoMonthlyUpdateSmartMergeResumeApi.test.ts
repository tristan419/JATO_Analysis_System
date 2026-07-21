// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

describe("JATO Smart Merge resume API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("posts every immutable seal once and maps the recovery operation", async () => {
    const request = {
      requestId: "5ebd4dba-09cb-4dfa-8bcc-d6d465caad0e",
      expectedSourceCandidateFingerprint: "a".repeat(64),
      expectedActiveFingerprint: "b".repeat(64),
      expectedReportFingerprint: "c".repeat(64),
      expectedResolutionFingerprint: "d".repeat(64),
    };
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.method).toBe("POST");
      expect(JSON.parse(String(init?.body))).toEqual(request);
      return Response.json({
        item: {
          jobId: "jato-update-171a8b20",
          status: "failed",
          phase: "smart_merge_failed",
          smartMergeRecovery: {
            canResume: false,
            sourceCandidateFingerprint: request.expectedSourceCandidateFingerprint,
            activeBaseFingerprint: request.expectedActiveFingerprint,
            reportFingerprint: request.expectedReportFingerprint,
            resolutionFingerprint: request.expectedResolutionFingerprint,
          },
          pendingOperation: {
            operationId: "jato-smart_merge_resume-1",
            type: "smart_merge_resume",
            status: "queued",
            phase: "queued",
            requestId: request.requestId,
            requestedAt: "2026-07-22T00:00:00+00:00",
            requestedBy: "admin",
            startedAt: null,
            finishedAt: null,
            error: null,
            failureDigest: null,
            expectedSourceCandidateFingerprint: request.expectedSourceCandidateFingerprint,
            expectedActiveFingerprint: request.expectedActiveFingerprint,
            expectedReportFingerprint: request.expectedReportFingerprint,
            expectedResolutionFingerprint: request.expectedResolutionFingerprint,
          },
        },
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await api.resumeJatoMonthlyUpdateSmartMerge(
      "jato-update-171a8b20",
      request,
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(String(fetchMock.mock.calls[0]?.[0])).toMatch(
      /\/msrp\/monthly-update-jobs\/jato-update-171a8b20\/smart-merge-resume$/
    );
    expect(response.item.smartMergeRecovery).toEqual({
      canResume: false,
      sourceCandidateFingerprint: request.expectedSourceCandidateFingerprint,
      activeBaseFingerprint: request.expectedActiveFingerprint,
      reportFingerprint: request.expectedReportFingerprint,
      resolutionFingerprint: request.expectedResolutionFingerprint,
    });
    expect(response.item.pendingOperation).toMatchObject({
      type: "smart_merge_resume",
      status: "queued",
      requestId: request.requestId,
      expectedSourceCandidateFingerprint: request.expectedSourceCandidateFingerprint,
      expectedActiveFingerprint: request.expectedActiveFingerprint,
      expectedReportFingerprint: request.expectedReportFingerprint,
      expectedResolutionFingerprint: request.expectedResolutionFingerprint,
    });
  });
});
