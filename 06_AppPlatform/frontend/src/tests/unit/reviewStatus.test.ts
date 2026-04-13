import { describe, expect, it } from "vitest";

import {
  REVIEW_STATUS_FILTER_OPTIONS,
  getCurrentPriceMatchStatusBadgeClass,
  getCurrentPriceMatchStatusLabel,
  getReviewStatusBadgeClass,
  getReviewStatusLabel,
} from "../../utils/reviewStatus";

describe("reviewStatus helpers", () => {
  it("exposes English review filter labels", () => {
    expect(REVIEW_STATUS_FILTER_OPTIONS).toEqual([
      { value: "open", label: "Open" },
      { value: "approved", label: "Approved" },
      { value: "rejected", label: "Rejected" },
    ]);
  });

  it("maps review statuses to readable labels and badges", () => {
    expect(getReviewStatusLabel("open")).toBe("Open");
    expect(getReviewStatusLabel("review_required")).toBe("Needs Review");
    expect(getReviewStatusLabel("remapped")).toBe("Remapped");
    expect(getReviewStatusBadgeClass("approved")).toBe("badge-active");
    expect(getReviewStatusBadgeClass("rejected")).toBe("badge-danger");
    expect(getReviewStatusBadgeClass("review_required")).toBe("badge-warning");
  });

  it("maps current price statuses to readable labels and badges", () => {
    expect(getCurrentPriceMatchStatusLabel("auto_accepted")).toBe("Auto Accepted");
    expect(getCurrentPriceMatchStatusLabel("human_approved")).toBe("Human Approved");
    expect(getCurrentPriceMatchStatusLabel("override_applied")).toBe("Override Applied");
    expect(getCurrentPriceMatchStatusLabel("review_required")).toBe("Needs Review");
    expect(getCurrentPriceMatchStatusLabel("matched")).toBe("Matched");
    expect(getCurrentPriceMatchStatusBadgeClass("human_approved")).toBe("badge-active");
    expect(getCurrentPriceMatchStatusBadgeClass("override_applied")).toBe("badge-active");
    expect(getCurrentPriceMatchStatusBadgeClass("review_required")).toBe("badge-warning");
    expect(getCurrentPriceMatchStatusBadgeClass("rejected")).toBe("badge-danger");
  });

  it("formats unknown status tokens into readable English", () => {
    expect(getReviewStatusLabel("manual_override_needed")).toBe("Manual Override Needed");
    expect(getCurrentPriceMatchStatusLabel("queued_for_review")).toBe("Queued For Review");
  });
});