function normalizeStatus(status?: string) {
  return String(status || "").trim().toLowerCase();
}

function toReadableLabel(value?: string, fallback = "Unknown") {
  const normalized = normalizeStatus(value);
  if (!normalized) {
    return fallback;
  }

  return normalized
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");
}

export const REVIEW_STATUS_FILTER_OPTIONS = [
  { value: "open", label: "Open" },
  { value: "approved", label: "Approved" },
  { value: "rejected", label: "Rejected" },
] as const;

export function getReviewStatusLabel(status?: string) {
  switch (normalizeStatus(status)) {
    case "open":
      return "Open";
    case "review_required":
      return "Needs Review";
    case "approved":
      return "Approved";
    case "rejected":
      return "Rejected";
    case "remapped":
      return "Remapped";
    default:
      return toReadableLabel(status, "Unknown Status");
  }
}

export function getReviewStatusBadgeClass(status?: string) {
  switch (normalizeStatus(status)) {
    case "approved":
      return "badge-active";
    case "rejected":
      return "badge-danger";
    case "open":
    case "review_required":
      return "badge-warning";
    default:
      return "badge-inactive";
  }
}

export function getCurrentPriceMatchStatusLabel(status?: string) {
  switch (normalizeStatus(status)) {
    case "auto_accepted":
      return "Auto Accepted";
    case "human_approved":
      return "Human Approved";
    case "override_applied":
      return "Override Applied";
    case "review_required":
      return "Needs Review";
    case "matched":
      return "Matched";
    case "rejected":
      return "Rejected";
    default:
      return toReadableLabel(status, "Unknown Match Status");
  }
}

export function getCurrentPriceMatchStatusBadgeClass(status?: string) {
  const normalized = normalizeStatus(status);
  if (
    normalized === "matched"
    || normalized === "auto_accepted"
    || normalized === "human_approved"
    || normalized === "override_applied"
    || normalized.includes("approve")
  ) {
    return "badge-active";
  }
  if (normalized === "review_required" || normalized.includes("review")) {
    return "badge-warning";
  }
  if (normalized === "rejected" || normalized.includes("reject")) {
    return "badge-danger";
  }
  return "badge-inactive";
}