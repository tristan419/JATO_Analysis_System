// @vitest-environment jsdom

import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import type { JatoMonthlyUpdateReviewIssue } from "../../types";

function issueDetail(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    blockerType: "review_bundle_stale",
    reason: "legacy_stat_signature",
    rebuildBlockerReason: null,
    message: "Review bundle 使用旧版签名。",
    canRebuild: true,
    candidateFingerprint: "a".repeat(64),
    activeBaseFingerprint: "b".repeat(64),
    currentActiveFingerprint: "b".repeat(64),
    reviewRefresh: null,
    ...overrides,
  };
}

describe("JATO Review refresh API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("preserves the structured stale Review contract on a 409", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      detail: issueDetail(),
    }, { status: 409 })));

    let issue: JatoMonthlyUpdateReviewIssue | undefined;
    try {
      await api.getJatoMonthlyUpdateReview("jato-review-1");
    } catch (error) {
      issue = (error as Error & {
        reviewIssue?: JatoMonthlyUpdateReviewIssue;
      }).reviewIssue;
    }

    expect(issue).toEqual({
      blockerType: "review_bundle_stale",
      reason: "legacy_stat_signature",
      rebuildBlockerReason: null,
      message: "Review bundle 使用旧版签名。",
      canRebuild: true,
      candidateFingerprint: "a".repeat(64),
      activeBaseFingerprint: "b".repeat(64),
      currentActiveFingerprint: "b".repeat(64),
      reviewRefresh: null,
    });
  });

  it.each([
    ["missing reason", { reason: undefined }],
    ["invalid fingerprint", { candidateFingerprint: "not-a-sha" }],
    ["wrong operation type", {
      reviewRefresh: {
        operationId: "jato-publish-1",
        type: "publish",
        status: "queued",
      },
    }],
  ])("does not expose a rebuild action for %s", async (_label, override) => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      detail: issueDetail(override),
    }, { status: 409 })));

    let issue: JatoMonthlyUpdateReviewIssue | undefined;
    await api.getJatoMonthlyUpdateReview("jato-review-1").catch((error) => {
      issue = (error as Error & {
        reviewIssue?: JatoMonthlyUpdateReviewIssue;
      }).reviewIssue;
    });

    expect(issue).toBeUndefined();
  });

  it("submits one idempotency key and maps the shared pending operation", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.method).toBe("POST");
      expect(JSON.parse(String(init?.body))).toEqual({
        requestId: "review-request-123",
        expectedCandidateFingerprint: "a".repeat(64),
      });
      return Response.json({
        item: {
          jobId: "jato-review-1",
          status: "success",
          phase: "completed",
          pendingOperation: {
            operationId: "jato-review_refresh-1",
            type: "review_refresh",
            status: "queued",
            phase: "queued",
            requestId: "review-request-123",
            requestedAt: "2026-07-21T00:00:00+00:00",
            requestedBy: "admin",
            startedAt: null,
            finishedAt: null,
            error: null,
            failureDigest: null,
            expectedCandidateFingerprint: "a".repeat(64),
            expectedActiveFingerprint: "b".repeat(64),
          },
        },
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await api.refreshJatoMonthlyUpdateReview(
      "jato-review-1",
      "review-request-123",
      "a".repeat(64),
    );

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(response.item.pendingOperation).toMatchObject({
      type: "review_refresh",
      status: "queued",
      requestId: "review-request-123",
      expectedCandidateFingerprint: "a".repeat(64),
      expectedActiveFingerprint: "b".repeat(64),
    });
  });
});
