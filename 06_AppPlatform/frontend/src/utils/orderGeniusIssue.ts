export interface OrderGeniusIssue {
  code: string;
  message: string;
  suggestion: string;
  country?: string;
  month?: string;
  materialCode?: string;
}

/** Decode the existing API error detail, without classifying arbitrary 409 text. */
export function orderGeniusIssue(cause: unknown): OrderGeniusIssue | null {
  let value: unknown = cause instanceof Error ? cause.message : cause;
  if (typeof value === "string") {
    try { value = JSON.parse(value) as unknown; } catch { return null; }
  }
  if (!value || typeof value !== "object" || !("code" in value) || !("message" in value) || !("suggestion" in value)
    || typeof value.code !== "string" || typeof value.message !== "string" || typeof value.suggestion !== "string") return null;
  return { code: value.code, message: value.message, suggestion: value.suggestion,
    country: "country" in value && typeof value.country === "string" ? value.country : undefined,
    month: "month" in value && typeof value.month === "string" ? value.month : undefined,
    materialCode: "materialCode" in value && typeof value.materialCode === "string" ? value.materialCode : undefined };
}
