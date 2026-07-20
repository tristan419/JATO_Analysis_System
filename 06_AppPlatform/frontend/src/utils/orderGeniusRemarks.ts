export function resolveCommonParentMaterialRemark<T>(
  rows: readonly T[],
  getTemplate: (row: T) => string,
  getRemark: (row: T) => string | undefined,
): string | undefined {
  const templates = new Set(
    rows.map((row) => getTemplate(row).trim().toUpperCase()),
  );
  if (templates.size !== 1 || templates.has("")) return undefined;

  for (const row of rows) {
    const remark = String(getRemark(row) || "").trim();
    if (remark) return remark;
  }
  return undefined;
}
