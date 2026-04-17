import type { DataManagementActivityDay } from "../types";

export function formatDataManagementTimestamp(value?: string | null): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

export function formatDataManagementNumber(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toLocaleString();
}

export function formatDataManagementBytes(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value) || value < 0) {
    return "-";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  if (value < 1024 * 1024 * 1024) {
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

export function getDataManagementStatusBadgeClass(status: string): string {
  switch (status) {
    case "ready":
    case "active":
      return "badge-active";
    case "warning":
      return "badge-warning";
    case "inactive":
      return "badge-inactive";
    default:
      return "badge-danger";
  }
}

export function buildActivityHeatmapColumns(
  days: DataManagementActivityDay[]
): DataManagementActivityDay[][] {
  const normalized = [...days].sort((left, right) => left.date.localeCompare(right.date));
  const columns: DataManagementActivityDay[][] = [];
  for (let index = 0; index < normalized.length; index += 7) {
    columns.push(normalized.slice(index, index + 7));
  }
  return columns;
}
