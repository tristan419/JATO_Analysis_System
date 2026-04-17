import { describe, expect, it } from "vitest";

import type {
  CurrentPrice,
  ReviewCase,
  ReviewCaseDetail,
  ReviewDecision,
} from "../../types";
import {
  extractReviewTrimTokens,
  formatReviewTrimSummary,
  summarizeReviewTrimCollection,
} from "../../utils/reviewCaseDisplay";

/**
 * Contract fields that `review_case_payload()` returns from the backend.
 * Frontend `ReviewCase` type fields must match these.
 */
const REVIEW_CASE_CONTRACT_FIELDS = [
  "id",             // reviewCaseId → id on frontend
  "observationId",
  "country",
  "brand",
  "jatoModel",
  "jatoTrim",
  "jatoPowertrain",
  "officialModel",
  "officialTrim",
  "officialEdition",
  "officialPowertrain",
  "matchConfidence",
  "matchReason",
  "candidateMatches",
  "reviewStatus",
  "sourceUrl",
  "currentAssignee",
  "createdAt",      // createdAtUtc → createdAt on frontend
  "updatedAt",      // updatedAtUtc → updatedAt on frontend
] as const;

const REVIEW_DECISION_CONTRACT_FIELDS = [
  "id",             // reviewDecisionId → id on frontend
  "reviewCaseId",
  "decision",
  "decidedOfficialModel",
  "decidedOfficialTrim",
  "note",
  "decidedBy",
  "decidedAt",      // decidedAtUtc → decidedAt on frontend
] as const;

function makeReviewCase(overrides: Partial<ReviewCase> = {}): ReviewCase {
  return {
    id: "rc-1",
    observationId: "obs-1",
    country: "Germany",
    brand: "BMW",
    sourceUrl: "https://example.com/review-case",
    jatoModel: "X5",
    jatoTrim: "xDrive40i",
    jatoPowertrain: "PHEV",
    officialModel: "X5",
    officialTrim: "xDrive40i",
    officialEdition: null,
    officialPowertrain: "PHEV",
    matchConfidence: 0.98,
    matchReason: { strategy: "unit-test" },
    candidateMatches: null,
    reviewStatus: "open",
    currentAssignee: "analyst-1",
    createdAt: "2026-04-11T10:00:00+00:00",
    updatedAt: "2026-04-11T10:00:00+00:00",
    ...overrides,
  };
}

function makeReviewDecision(overrides: Partial<ReviewDecision> = {}): ReviewDecision {
  return {
    id: "rd-1",
    reviewCaseId: "rc-1",
    decision: "approve",
    decidedOfficialModel: null,
    decidedOfficialTrim: null,
    note: "OK",
    decidedBy: "analyst-1",
    decidedAt: "2026-04-11T11:00:00+00:00",
    ...overrides,
  };
}

function makeCurrentPrice(overrides: Partial<CurrentPrice> = {}): CurrentPrice {
  return {
    id: "cp-1",
    country: "Germany",
    brand: "BMW",
    jatoModel: "X5",
    jatoTrim: "xDrive40i",
    jatoPowertrain: "PHEV",
    officialModel: "X5",
    officialTrim: "xDrive40i",
    officialEdition: null,
    officialPowertrain: "PHEV",
    effectiveObservationId: "obs-1",
    currentMsrpValue: 78000,
    currency: "EUR",
    sourceMsrpValue: 78000,
    sourceCurrency: "EUR",
    fxRateToEur: 1,
    fxRateAsOfDate: "2026-04-11",
    fxSource: "ECB",
    taxIncluded: true,
    matchConfidence: 0.98,
    matchStatus: "human_approved",
    sourceUrl: "https://example.com/current-price",
    sourceSnapshotPath: null,
    lastPriceChangeAtUtc: "2026-04-10T10:00:00+00:00",
    updatedAtUtc: "2026-04-11T10:00:00+00:00",
    ...overrides,
  };
}

function makeReviewCaseDetail(
  overrides: Partial<ReviewCaseDetail> = {},
): ReviewCaseDetail {
  return {
    ...makeReviewCase(),
    observation: null,
    decisions: [],
    currentPrice: null,
    ...overrides,
  };
}

describe("ReviewCase contract", () => {
  it("has all contract-required fields from backend serializer", () => {
    const rc = makeReviewCase();
    for (const field of REVIEW_CASE_CONTRACT_FIELDS) {
      expect(rc).toHaveProperty(field);
    }
  });

  it("reviewStatus drives table badge logic without crashing", () => {
    for (const status of ["open", "review_required", "approved", "rejected", "remapped", "unknown"]) {
      const rc = makeReviewCase({ reviewStatus: status });
      const badge = rc.reviewStatus === "approved"
        ? "badge-active"
        : rc.reviewStatus === "rejected"
          ? "badge-danger"
          : rc.reviewStatus === "open" || rc.reviewStatus === "review_required"
            ? "badge-warning"
            : "badge-inactive";
      expect(badge).toBeTruthy();
    }
  });

  it("matchConfidence renders as percentage without NaN", () => {
    const rc = makeReviewCase({ matchConfidence: 0.965 });
    const display = (rc.matchConfidence * 100).toFixed(0);
    expect(display).toBe("97");
    expect(Number.isNaN(Number(display))).toBe(false);
  });

  it("preserves structured edition and powertrain fields", () => {
    const rc = makeReviewCase({
      officialEdition: "Black Edition",
      officialPowertrain: "PHEV",
      jatoPowertrain: "PHEV",
    });
    expect(rc.officialEdition).toBe("Black Edition");
    expect(rc.officialPowertrain).toBe("PHEV");
    expect(rc.jatoPowertrain).toBe("PHEV");
  });

  it("detail contract can carry a linked current price snapshot", () => {
    const detail = makeReviewCaseDetail({
      currentPrice: makeCurrentPrice(),
      decisions: [makeReviewDecision()],
    });
    expect(detail.currentPrice?.sourceUrl).toBe("https://example.com/current-price");
    expect(detail.decisions).toHaveLength(1);
  });
});

describe("ReviewDecision contract", () => {
  it("has all contract-required fields from backend serializer", () => {
    const rd = makeReviewDecision();
    for (const field of REVIEW_DECISION_CONTRACT_FIELDS) {
      expect(rd).toHaveProperty(field);
    }
  });

  it("decidedAt field is a valid ISO date string", () => {
    const rd = makeReviewDecision();
    const parsed = new Date(rd.decidedAt);
    expect(Number.isNaN(parsed.getTime())).toBe(false);
  });
});

describe("review trim display", () => {
  it("parses Python-style trim list strings into trim tokens", () => {
    expect(
      extractReviewTrimTokens("['Life', 'Life Edition', 'R-Line']"),
    ).toEqual(["Life", "Life Edition", "R-Line"]);
  });

  it("formats stringified trim arrays as readable summaries", () => {
    const reviewCase = makeReviewCase({
      officialTrim: "['Life', 'Life Edition', 'R-Line', 'R-Line Edition', 'Style']",
    });

    expect(formatReviewTrimSummary(reviewCase)).toBe(
      "Life / Life Edition / R-Line +2",
    );
  });

  it("summarizes trim tokens across grouped review cases", () => {
    const firstCase = makeReviewCase({
      officialTrim: "['Life', 'R-Line']",
    });
    const secondCase = makeReviewCase({
      id: "rc-2",
      officialTrim: "['Style', 'R-Line Edition']",
    });

    expect(summarizeReviewTrimCollection([firstCase, secondCase])).toBe(
      "Life / R-Line / Style +1",
    );
  });
});
