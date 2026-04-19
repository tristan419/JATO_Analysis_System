export type SlideFitStatus = "safe" | "compress" | "split";

export interface SlideFitInput {
  chartCount?: number;
  metricCount?: number;
  narrativeCount?: number;
  primaryItemCount?: number;
  secondaryItemCount?: number;
  seriesCount?: number;
  labelCount?: number;
  longestLabelLength?: number;
  exportWidth?: number;
  exportHeight?: number;
}

export interface SlideFitIssue {
  key: string;
  severity: Exclude<SlideFitStatus, "safe">;
  message: string;
  recommendation: string;
  splitSlides?: number;
}

export interface SlideFitAssessment {
  status: SlideFitStatus;
  score: number;
  summary: string;
  splitSlides: number;
  issues: SlideFitIssue[];
  recommendedActions: string[];
}

interface SlideFitBudget {
  soft: number;
  hard: number;
  message: (value: number) => string;
  recommend: (value: number) => string;
  splitBase?: number;
}

const FIT_BUDGETS: Record<Exclude<keyof SlideFitInput, "exportWidth" | "exportHeight">, SlideFitBudget> = {
  chartCount: {
    soft: 3,
    hard: 4,
    message: (value) => `当前页承载 ${value} 个图表/榜单面板，首屏会偏拥挤。`,
    recommend: () => "单页图表建议控制在 2-3 个，超出时拆页。",
    splitBase: 2,
  },
  metricCount: {
    soft: 6,
    hard: 8,
    message: (value) => `头部指标卡 ${value} 个，抢占正文高度。`,
    recommend: () => "头部指标建议压到 6 个以内，次要指标移到下一页。",
    splitBase: 6,
  },
  narrativeCount: {
    soft: 2,
    hard: 3,
    message: (value) => `说明文案分成 ${value} 段，易挤压图表空间。`,
    recommend: () => "文案建议保留 1-2 段，超出部分拆到备注页。",
    splitBase: 2,
  },
  primaryItemCount: {
    soft: 12,
    hard: 18,
    message: (value) => `主榜单/主类目达到 ${value} 项，阅读密度过高。`,
    recommend: (value) => `主榜单建议压到 ${Math.min(12, value)} 项左右，超限时拆成两页。`,
    splitBase: 12,
  },
  secondaryItemCount: {
    soft: 16,
    hard: 24,
    message: (value) => `辅助类目 ${value} 项，标签与图例会变密。`,
    recommend: () => "辅助类目建议控制在 16 项以内，必要时合并为 Others。",
    splitBase: 16,
  },
  seriesCount: {
    soft: 5,
    hard: 7,
    message: (value) => `同页系列数 ${value} 条，图例和颜色辨识度会下降。`,
    recommend: () => "同页系列建议控制在 5 条以内，更多系列优先拆页或切标签。",
    splitBase: 5,
  },
  labelCount: {
    soft: 16,
    hard: 24,
    message: (value) => `页面涉及 ${value} 个标签，轴标签和榜单会显得拥挤。`,
    recommend: () => "标签数建议控制在 16 个以内，超限时先减类目再导出。",
    splitBase: 16,
  },
  longestLabelLength: {
    soft: 18,
    hard: 28,
    message: (value) => `最长标签 ${value} 个字符，容易挤占坐标轴与榜单行高。`,
    recommend: () => "长标签建议缩写到 18 字以内，或拆到独立页面。",
    splitBase: 18,
  },
};

function normalizeMetric(value: number | undefined): number {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return 0;
  }
  return Math.max(0, Math.round(value));
}

function buildIssue(key: keyof typeof FIT_BUDGETS, value: number): SlideFitIssue | null {
  const budget = FIT_BUDGETS[key];
  if (value <= budget.soft) {
    return null;
  }

  const severity: SlideFitIssue["severity"] = value > budget.hard ? "split" : "compress";
  return {
    key,
    severity,
    message: budget.message(value),
    recommendation: budget.recommend(value),
    splitSlides: severity === "split" && budget.splitBase
      ? Math.max(2, Math.ceil(value / budget.splitBase))
      : undefined,
  };
}

function buildDimensionIssue(exportWidth: number, exportHeight: number): SlideFitIssue | null {
  if (!exportWidth || !exportHeight) {
    return null;
  }
  const ratio = exportWidth / exportHeight;
  if (Math.abs(ratio - (16 / 9)) <= 0.02) {
    return null;
  }
  return {
    key: "dimensions",
    severity: "compress",
    message: `当前导出比例约为 ${(ratio).toFixed(2)}，不是标准 16:9 画布。`,
    recommendation: "固定汇报页建议保持 16:9，例如 1920×1080。",
  };
}

export function measureLongestLabel(labels: Array<string | null | undefined>): number {
  return labels.reduce((max, label) => {
    const text = typeof label === "string" ? label.trim() : "";
    return Math.max(max, text.length);
  }, 0);
}

export function assessSlideFit(input: SlideFitInput): SlideFitAssessment {
  const issues: SlideFitIssue[] = [];
  let score = 0;

  (Object.keys(FIT_BUDGETS) as Array<keyof typeof FIT_BUDGETS>).forEach((key) => {
    const value = normalizeMetric(input[key]);
    const issue = buildIssue(key, value);
    if (!issue) {
      return;
    }
    issues.push(issue);
    score += issue.severity === "split" ? 28 : 14;
  });

  const dimensionIssue = buildDimensionIssue(
    normalizeMetric(input.exportWidth),
    normalizeMetric(input.exportHeight),
  );
  if (dimensionIssue) {
    issues.push(dimensionIssue);
    score += 10;
  }

  const status: SlideFitStatus = issues.some((issue) => issue.severity === "split")
    ? "split"
    : issues.length > 0
      ? "compress"
      : "safe";
  const splitSlides = Math.max(1, ...issues.map((issue) => issue.splitSlides ?? 1));
  const recommendedActions = Array.from(new Set(issues.map((issue) => issue.recommendation)));

  const summary = status === "safe"
    ? "当前页密度适合固定 1920×1080 导出。"
    : status === "compress"
      ? "当前页偏密，建议先压缩类目/系列/标签后再导出。"
      : `当前页已超出推荐密度，建议至少拆成 ${splitSlides} 页。`;

  return {
    status,
    score,
    summary,
    splitSlides,
    issues,
    recommendedActions,
  };
}
