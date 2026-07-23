import type {
  EngineeringConfigOcrCandidate,
  EngineeringConfigOcrEvaluation,
} from "../types/engineeringConfig";

const OCR_SEMANTIC_STRATEGIES = new Set(["highest_config_semantic_score", "highest_table_score"]);

export type EngineeringConfigOcrDigestLike = {
  ocrEngine?: string | null;
  ocrEngineCandidates?: EngineeringConfigOcrCandidate[];
  ocrEvaluation?: EngineeringConfigOcrEvaluation | null;
};

export function isOcrSemanticStrategy(value: string | null | undefined): boolean {
  return Boolean(value && OCR_SEMANTIC_STRATEGIES.has(value));
}

function ocrCandidateFeatureCount(candidate: EngineeringConfigOcrCandidate): number {
  return candidate.score?.featureCount ?? candidate.score?.totalFeatureCount ?? 0;
}

function ocrCandidateTrimCount(candidate: EngineeringConfigOcrCandidate): number {
  return candidate.score?.candidateTrimCount ?? candidate.score?.totalCandidateTrimCount ?? 0;
}

function ocrCandidateDifferenceCount(candidate: EngineeringConfigOcrCandidate): number {
  return candidate.score?.differenceCount ?? candidate.score?.totalDifferenceCount ?? 0;
}

function ocrCandidateRankScore(candidate: EngineeringConfigOcrCandidate): number {
  const score = candidate.score;
  if (!score) return candidate.comparableTableDetected ? 1 : 0;
  return [
    candidate.comparableTableDetected ? 1_000_000_000 : 0,
    (score.semanticScore ?? 0) * 1_000_000,
    ocrCandidateFeatureCount(candidate) * 100_000,
    ocrCandidateTrimCount(candidate) * 10_000,
    ocrCandidateDifferenceCount(candidate) * 1_000,
    score.tableShapeScore ?? 0,
    score.nonEmptyCount ?? 0,
  ].reduce((total, value) => total + value, 0);
}

function ocrSelectionStrategyLabel(digest: EngineeringConfigOcrDigestLike): string {
  if (isOcrSemanticStrategy(digest.ocrEvaluation?.reason) || isOcrSemanticStrategy(digest.ocrEvaluation?.strategy)) {
    return "配置表语义评分";
  }
  if (digest.ocrEvaluation?.reason === "no_comparable_table_detected") return "未识别可比表时的保守策略";
  if (digest.ocrEvaluation?.strategy === "highest_table_score") return "表格形状评分";
  return "OCR 评分";
}

export function engineeringConfigOcrComparisonText(digest: EngineeringConfigOcrDigestLike): string | null {
  const candidates = digest.ocrEngineCandidates ?? [];
  if (candidates.length === 0) return null;
  const selectedEngine = digest.ocrEvaluation?.selectedEngine || digest.ocrEngine;
  const selectedCandidate = candidates.find((item) => item.selected)
    ?? candidates.find((item) => selectedEngine && item.engine === selectedEngine)
    ?? null;
  if (!selectedCandidate) return null;
  if (candidates.length === 1) {
    return `OCR 对比：仅有 ${selectedCandidate.engine} 候选；未形成 PaddleOCR vs legacy/custom OCR 横向对比，引用前需抽查来源。`;
  }
  const alternative = candidates
    .filter((item) => item !== selectedCandidate)
    .sort((a, b) => ocrCandidateRankScore(b) - ocrCandidateRankScore(a))[0];
  if (!alternative) return null;
  const selectedScore = selectedCandidate.score;
  const alternativeScore = alternative.score;
  const reasons: string[] = [];
  if (selectedCandidate.comparableTableDetected && !alternative.comparableTableDetected) {
    reasons.push(`${alternative.engine} 未识别可比表`);
  }
  const relativeReasons: string[] = [];
  const featureDelta = ocrCandidateFeatureCount(selectedCandidate) - ocrCandidateFeatureCount(alternative);
  if (featureDelta > 0) relativeReasons.push(`多识别 ${featureDelta} 个配置项`);
  const trimDelta = ocrCandidateTrimCount(selectedCandidate) - ocrCandidateTrimCount(alternative);
  if (trimDelta > 0) relativeReasons.push(`多识别 ${trimDelta} 个配置列`);
  const differenceDelta = ocrCandidateDifferenceCount(selectedCandidate) - ocrCandidateDifferenceCount(alternative);
  if (differenceDelta > 0) relativeReasons.push(`多识别 ${differenceDelta} 个差异`);
  const nonEmptyDelta = (selectedScore?.nonEmptyCount ?? 0) - (alternativeScore?.nonEmptyCount ?? 0);
  if (nonEmptyDelta > 0) relativeReasons.push(`多识别 ${nonEmptyDelta} 个非空单元`);
  const tableShapeDelta = (selectedScore?.tableShapeScore ?? 0) - (alternativeScore?.tableShapeScore ?? 0);
  if (tableShapeDelta > 0) relativeReasons.push(`表格结构分 +${tableShapeDelta}`);
  if (relativeReasons.length > 0) {
    reasons.push(`相对 ${alternative.engine} ${relativeReasons.slice(0, 3).join("，")}`);
  }
  if (reasons.length > 0) {
    return `OCR 对比：${selectedCandidate.engine} 胜出；${reasons.slice(0, 3).join("；")}。`;
  }
  return `OCR 对比：选用 ${selectedCandidate.engine}；${alternative.engine} 分数接近，按${ocrSelectionStrategyLabel(digest)}排序，建议抽查来源。`;
}
