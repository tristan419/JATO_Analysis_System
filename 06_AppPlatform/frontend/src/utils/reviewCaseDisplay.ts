interface ReviewTrimCandidate {
  officialTrim?: string | null;
  jatoTrim?: string | null;
}

function uniqueStrings(values: string[]) {
  return Array.from(new Set(values.map((value) => value.trim()).filter(Boolean)));
}

export function extractReviewTrimTokens(value?: string | null): string[] {
  const normalizedValue = String(value ?? "").trim();
  if (!normalizedValue) {
    return [];
  }

  if (normalizedValue.startsWith("[") && normalizedValue.endsWith("]")) {
    const quotedMatches = Array.from(
      normalizedValue.matchAll(/'([^']+)'|"([^"]+)"/g),
    ).map((match) => match[1] ?? match[2] ?? "");

    if (quotedMatches.length > 0) {
      return uniqueStrings(quotedMatches);
    }

    const fallbackTokens = normalizedValue
      .slice(1, -1)
      .split(",")
      .map((token) => token.replace(/^['"]|['"]$/g, "").trim());

    return uniqueStrings(fallbackTokens);
  }

  return [normalizedValue];
}

export function resolveReviewTrimTokens(candidate: ReviewTrimCandidate): string[] {
  const officialTokens = extractReviewTrimTokens(candidate.officialTrim);
  if (officialTokens.length > 0) {
    return officialTokens;
  }
  return extractReviewTrimTokens(candidate.jatoTrim);
}

export function formatReviewTrimSummary(
  candidate: ReviewTrimCandidate,
  maxItems = 3,
) {
  const tokens = resolveReviewTrimTokens(candidate);
  if (tokens.length === 0) {
    return "-";
  }

  if (!Number.isFinite(maxItems) || tokens.length <= maxItems) {
    return tokens.join(" / ");
  }

  return `${tokens.slice(0, maxItems).join(" / ")} +${tokens.length - maxItems}`;
}

export function summarizeReviewTrimCollection(
  candidates: ReviewTrimCandidate[],
  maxItems = 3,
) {
  const tokens = uniqueStrings(
    candidates.flatMap((candidate) => resolveReviewTrimTokens(candidate)),
  );

  if (tokens.length === 0) {
    return "未命名 Trim";
  }

  if (!Number.isFinite(maxItems) || tokens.length <= maxItems) {
    return tokens.join(" / ");
  }

  return `${tokens.slice(0, maxItems).join(" / ")} +${tokens.length - maxItems}`;
}