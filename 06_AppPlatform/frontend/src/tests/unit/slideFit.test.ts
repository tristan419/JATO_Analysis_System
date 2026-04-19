import { describe, expect, it } from "vitest";

import { assessSlideFit, measureLongestLabel } from "../../utils/slideFit";

describe("measureLongestLabel", () => {
  it("ignores empty values and returns the longest trimmed label length", () => {
    expect(measureLongestLabel(["  Alpha  ", null, "Longest Label Here", undefined])).toBe(18);
  });
});

describe("assessSlideFit", () => {
  it("marks balanced slides as safe", () => {
    const assessment = assessSlideFit({
      chartCount: 2,
      metricCount: 4,
      primaryItemCount: 10,
      secondaryItemCount: 12,
      seriesCount: 4,
      labelCount: 12,
      longestLabelLength: 14,
      exportWidth: 1920,
      exportHeight: 1080,
    });

    expect(assessment.status).toBe("safe");
    expect(assessment.splitSlides).toBe(1);
    expect(assessment.recommendedActions).toHaveLength(0);
  });

  it("asks for trimming when counts exceed soft budgets", () => {
    const assessment = assessSlideFit({
      chartCount: 3,
      metricCount: 6,
      primaryItemCount: 14,
      seriesCount: 6,
      labelCount: 18,
      longestLabelLength: 24,
      exportWidth: 1920,
      exportHeight: 1080,
    });

    expect(assessment.status).toBe("compress");
    expect(assessment.recommendedActions).toContain("同页系列建议控制在 5 条以内，更多系列优先拆页或切标签。");
    expect(assessment.recommendedActions).toContain("长标签建议缩写到 18 字以内，或拆到独立页面。");
  });

  it("asks for split when hard limits are exceeded", () => {
    const assessment = assessSlideFit({
      chartCount: 5,
      primaryItemCount: 26,
      secondaryItemCount: 28,
      seriesCount: 8,
      labelCount: 28,
      exportWidth: 1920,
      exportHeight: 1080,
    });

    expect(assessment.status).toBe("split");
    expect(assessment.splitSlides).toBeGreaterThan(1);
    expect(assessment.summary).toContain("建议至少拆成");
  });
});
