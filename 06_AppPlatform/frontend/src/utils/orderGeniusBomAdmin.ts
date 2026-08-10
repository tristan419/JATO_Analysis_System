export function buildBomEditScopeKey(
  modelGroupKey: string,
  versionKey: string,
  bomTemplate: string,
): string {
  return `${modelGroupKey}|${versionKey}|${bomTemplate}`;
}
