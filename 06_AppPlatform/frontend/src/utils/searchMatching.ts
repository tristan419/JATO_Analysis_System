export function compactSearchText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fff]+/g, "");
}

export function matchesCompactSearch(value: string, query: string): boolean {
  const normalizedQuery = query.toLowerCase().trim();
  if (!normalizedQuery) return true;
  if (value.toLowerCase().includes(normalizedQuery)) return true;
  const compactQuery = compactSearchText(query);
  return compactQuery !== "" && compactSearchText(value).includes(compactQuery);
}

export function optionMatchesCompactSearch(
  option: { label: string; value: string },
  query: string,
): boolean {
  return matchesCompactSearch(option.label, query) || matchesCompactSearch(option.value, query);
}
