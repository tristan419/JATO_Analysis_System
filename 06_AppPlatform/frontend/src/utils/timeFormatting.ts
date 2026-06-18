/**
 * Shared time-label parsing, ordinal conversion, and comparison utilities.
 *
 * Extracted from DashboardPage / TimeAxis to eliminate duplication.
 */

export const MONTH_INDEX: Record<string, number> = {
  Jan: 1, Feb: 2, Mar: 3, Apr: 4, May: 5, Jun: 6,
  Jul: 7, Aug: 8, Sep: 9, Oct: 10, Nov: 11, Dec: 12,
};

/**
 * Parse a time label like "2024 Jan", "24/3", "2024-03" into { year, month }.
 */
export function parseMonthLabel(label: string): { year: number; month: number } | null {
  const text = label.trim();
  const monthNameMatch = text.match(/^(\d{4})\s+([A-Za-z]{3})$/);
  if (monthNameMatch) {
    return { year: Number(monthNameMatch[1]), month: MONTH_INDEX[monthNameMatch[2]] ?? 1 };
  }
  const shortYearMatch = text.match(/^(\d{2})[.\/-](\d{1,2})$/);
  if (shortYearMatch) {
    return { year: 2000 + Number(shortYearMatch[1]), month: Number(shortYearMatch[2]) };
  }
  const numericMatch = text.match(/^(\d{4})[-\/.](\d{1,2})$/);
  if (numericMatch) {
    return { year: Number(numericMatch[1]), month: Number(numericMatch[2]) };
  }
  return null;
}

/**
 * Convert a time label to a sortable ordinal (YYYYMM).
 * Supports: "2024" (year), "2024 Jan" (month name), "24/3" (short), "2024-Q2" (quarter).
 */
export function toTimeOrdinal(label: string): number | null {
  const text = label.trim();
  if (/^\d{4}$/.test(text)) return Number(text) * 100 + 12;
  const month = parseMonthLabel(text);
  if (month) return month.year * 100 + month.month;
  const quarter = text.match(/^(\d{4})-Q([1-4])$/);
  if (quarter) return Number(quarter[1]) * 100 + Number(quarter[2]) * 3;
  return null;
}

/**
 * Compare two time labels chronologically.
 */
export function compareTimeLabels(a: string, b: string): number {
  const ao = toTimeOrdinal(a);
  const bo = toTimeOrdinal(b);
  if (ao !== null && bo !== null && ao !== bo) return ao - bo;
  return a.localeCompare(b);
}

export function formatDateTime(value: string | null | undefined, locale = "zh-CN"): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString(locale, { hour12: false });
}
