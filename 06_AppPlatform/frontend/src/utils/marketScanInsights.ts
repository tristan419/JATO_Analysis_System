import type {
  MarketScanBodyShareTrendItem,
  MarketScanDelta,
  MarketScanDrilldownPage,
  MarketScanFuelPanel,
  MarketScanFuelTrendItem,
  MarketScanMatrix,
  MarketScanMetricCell,
  MarketScanOriginBrandGroup,
  MarketScanOriginPage,
  MarketScanOverviewPage,
  MarketScanOverviewTrendItem,
  MarketScanRankingItem,
  MarketScanSegmentPage,
} from "../types";

export type InsightTone = "positive" | "negative" | "neutral";

export interface MarketInsightCard {
  label: string;
  value: string;
  detail: string;
  tone: InsightTone;
}

export interface MarketInsightSnapshot {
  headline: string;
  summary: string;
  tone: InsightTone;
  cards: MarketInsightCard[];
}

interface RollingTrendRead {
  value: string;
  detail: string;
  tone: InsightTone;
  changeRatio: number | null;
}

interface StructureRead {
  value: string;
  detail: string;
  tone: InsightTone;
  driverFuel: string | null;
  driverShare: number | null;
}

function deltaValue(delta: MarketScanDelta | undefined): number | null {
  return typeof delta?.value === "number" && Number.isFinite(delta.value) ? delta.value : null;
}

function average(values: number[]): number {
  if (values.length === 0) {
    return 0;
  }
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function safeShare(numerator: number, denominator: number): number {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator <= 0) {
    return 0;
  }
  return numerator / denominator;
}

function formatVolume(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return Number(value).toLocaleString("en-US");
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function formatSignedPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  const sign = value > 0 ? "+" : value < 0 ? "" : "";
  return `${sign}${(value * 100).toFixed(digits)}%`;
}

function formatSignedPctPoints(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  const sign = value > 0 ? "+" : value < 0 ? "" : "";
  return `${sign}${(value * 100).toFixed(digits)} 个百分点`;
}

function formatSignedVolume(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  const sign = value > 0 ? "+" : value < 0 ? "" : "";
  return `${sign}${Math.abs(value).toLocaleString("en-US")}`;
}

function shiftPeriod(period: string, deltaMonths: number): string {
  const [yearText, monthText] = period.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    return period;
  }
  const shifted = new Date(Date.UTC(year, month - 1 + deltaMonths, 1));
  return `${shifted.getUTCFullYear()}-${String(shifted.getUTCMonth() + 1).padStart(2, "0")}`;
}

function rankingItemLabel(item: Pick<MarketScanRankingItem, "brand" | "model">): string {
  return item.brand ?? item.model ?? "-";
}

function orderedTrendItems(items: MarketScanOverviewTrendItem[]): MarketScanOverviewTrendItem[] {
  return [...items].sort((left, right) => left.period.localeCompare(right.period));
}

function matrixRow(matrix: MarketScanMatrix, metricKey: string) {
  return matrix.rows.find((row) => row.metricKey === metricKey);
}

function topMatrixCell(row: { cells: MarketScanMetricCell[] } | undefined): MarketScanMetricCell | null {
  if (!row || row.cells.length === 0) {
    return null;
  }
  return row.cells.reduce<MarketScanMetricCell | null>((winner, cell) => {
    if (!winner || (cell.value ?? Number.NEGATIVE_INFINITY) > (winner.value ?? Number.NEGATIVE_INFINITY)) {
      return cell;
    }
    return winner;
  }, null);
}

function totalMatrixRow(row: { cells: MarketScanMetricCell[] } | undefined): number {
  return row?.cells.reduce((sum, cell) => sum + Number(cell.value ?? 0), 0) ?? 0;
}

function topPositiveMatrixCell(row: { cells: MarketScanMetricCell[] } | undefined): MarketScanMetricCell | null {
  if (!row || row.cells.length === 0) {
    return null;
  }
  const ranked = [...row.cells]
    .filter((cell) => typeof cell.value === "number")
    .sort((left, right) => Number(right.value ?? 0) - Number(left.value ?? 0));
  return ranked[0] ?? null;
}

function lastItem<T>(items: T[]): T | undefined {
  return items[items.length - 1];
}

function buildRollingTrendRead(items: MarketScanOverviewTrendItem[]): RollingTrendRead {
  const ordered = orderedTrendItems(items);
  const recent = ordered.slice(-6);
  if (recent.length < 4) {
    return {
      value: "趋势样本不足",
      detail: "最近月份不足，先结合当月与累计同比判断方向。",
      tone: "neutral",
      changeRatio: null,
    };
  }

  const splitIndex = Math.ceil(recent.length / 2);
  const earlyValues = recent.slice(0, splitIndex).map((item) => item.totalVolume);
  const lateValues = recent.slice(splitIndex).map((item) => item.totalVolume);
  const earlyAverage = average(earlyValues);
  const lateAverage = average(lateValues);
  const changeRatio = safeShare(lateAverage - earlyAverage, Math.max(earlyAverage, 1));

  if (changeRatio >= 0.06) {
    return {
      value: "Rolling 12M 持续抬升",
      detail: `近半年均值较前段 ${formatSignedPercent(changeRatio)}，修复斜率明确向上。`,
      tone: "positive",
      changeRatio,
    };
  }
  if (changeRatio <= -0.06) {
    return {
      value: "Rolling 12M 明显回落",
      detail: `近半年均值较前段 ${formatSignedPercent(changeRatio)}，总量趋势转弱。`,
      tone: "negative",
      changeRatio,
    };
  }
  return {
    value: "Rolling 12M 基本走平",
    detail: `近半年均值仅 ${formatSignedPercent(changeRatio)} 波动，市场仍在平台区间。`,
    tone: "neutral",
    changeRatio,
  };
}

function buildStructureRead(items: MarketScanOverviewTrendItem[]): StructureRead {
  const ordered = orderedTrendItems(items);
  const latest = ordered[ordered.length - 1];
  if (!latest) {
    return {
      value: "缺少结构样本",
      detail: "当前没有可用的动总结构数据。",
      tone: "neutral",
      driverFuel: null,
      driverShare: null,
    };
  }

  const yearAgo = ordered.find((item) => item.period === shiftPeriod(latest.period, -12));
  const previous = ordered[ordered.length - 2];
  const compareItem = yearAgo ?? previous;
  const compareLabel = yearAgo ? "去年同期" : "上月";
  const fuelKeys = Object.keys(latest.fuelMix ?? {});
  if (fuelKeys.length === 0) {
    return {
      value: "缺少结构样本",
      detail: "当前没有可用的动总结构数据。",
      tone: "neutral",
      driverFuel: null,
      driverShare: null,
    };
  }

  const ranked = fuelKeys
    .map((fuel) => {
      const currentVolume = Number(latest.fuelMix?.[fuel] ?? 0);
      const compareVolume = Number(compareItem?.fuelMix?.[fuel] ?? 0);
      const currentShare = safeShare(currentVolume, latest.totalVolume);
      const compareShare = safeShare(compareVolume, compareItem?.totalVolume ?? 0);
      return {
        fuel,
        currentVolume,
        currentShare,
        deltaVolume: currentVolume - compareVolume,
        shareDelta: currentShare - compareShare,
      };
    })
    .sort((left, right) => (
      right.deltaVolume - left.deltaVolume
      || right.shareDelta - left.shareDelta
      || right.currentVolume - left.currentVolume
    ));

  const leader = ranked[0];
  if (!leader) {
    return {
      value: "缺少结构样本",
      detail: "当前没有可用的动总结构数据。",
      tone: "neutral",
      driverFuel: null,
      driverShare: null,
    };
  }

  if (leader.deltaVolume > 0) {
    return {
      value: `${leader.fuel} 拉动增量`,
      detail: `较${compareLabel} ${formatSignedVolume(leader.deltaVolume)} 台，份额 ${formatSignedPctPoints(leader.shareDelta)}。`,
      tone: leader.shareDelta > 0 ? "positive" : "neutral",
      driverFuel: leader.fuel,
      driverShare: leader.currentShare,
    };
  }

  return {
    value: `${leader.fuel} 仍是最大盘子`,
    detail: `${leader.fuel} 当前占比 ${formatPercent(leader.currentShare)}，但暂无单一动力显著放大增量。`,
    tone: "neutral",
    driverFuel: leader.fuel,
    driverShare: leader.currentShare,
  };
}

function buildOverviewSignalCard(page: MarketScanOverviewPage, latest?: MarketScanOverviewTrendItem): MarketInsightCard {
  const currentYoY = deltaValue(page.summary.currentMonthYoY);
  const latestMoM = deltaValue(latest?.mom);
  const ytdYoY = deltaValue(page.summary.ytdYoY);
  const positiveCount = [currentYoY, latestMoM, ytdYoY].filter((value) => (value ?? 0) > 0).length;

  if (positiveCount === 3) {
    return {
      label: "方向信号",
      value: "单月 / 环比 / 累计三线共振",
      detail: `当月 YoY ${page.summary.currentMonthYoY.display}、MoM ${latest?.mom.display ?? "-"}、累计 YoY ${page.summary.ytdYoY.display}。`,
      tone: "positive",
    };
  }
  if (positiveCount === 2) {
    return {
      label: "方向信号",
      value: "修复仍在，但节奏不完全一致",
      detail: `当月 YoY ${page.summary.currentMonthYoY.display}、MoM ${latest?.mom.display ?? "-"}、累计 YoY ${page.summary.ytdYoY.display}。`,
      tone: "neutral",
    };
  }
  return {
    label: "方向信号",
    value: "市场信号偏弱",
    detail: `当月 YoY ${page.summary.currentMonthYoY.display}、MoM ${latest?.mom.display ?? "-"}、累计 YoY ${page.summary.ytdYoY.display}。`,
    tone: "negative",
  };
}

function buildOverviewLeaderCard(page: MarketScanOverviewPage): MarketInsightCard {
  const monthlyLeader = page.monthlyBrandRanking.items[0];
  const ytdLeader = page.ytdBrandRanking.items[0];
  if (!monthlyLeader && !ytdLeader) {
    return {
      label: "头部品牌",
      value: "暂无头部样本",
      detail: "当前筛选下没有可用的品牌排名数据。",
      tone: "neutral",
    };
  }
  if (monthlyLeader && ytdLeader && rankingItemLabel(monthlyLeader) === rankingItemLabel(ytdLeader)) {
    return {
      label: "头部品牌",
      value: `${rankingItemLabel(monthlyLeader)} 月榜 / 累计双第一`,
      detail: `当月份额 ${monthlyLeader.shareDisplay ?? formatPercent(monthlyLeader.sharePct)}，累计份额 ${ytdLeader.shareDisplay ?? formatPercent(ytdLeader.sharePct)}。`,
      tone: "positive",
    };
  }
  if (monthlyLeader && ytdLeader) {
    return {
      label: "头部品牌",
      value: `${rankingItemLabel(monthlyLeader)} 抢下月榜，${rankingItemLabel(ytdLeader)} 仍守住累计`,
      detail: `短期冲量与全年格局并未完全重合，说明竞争仍在重排。`,
      tone: "neutral",
    };
  }
  const leader = monthlyLeader ?? ytdLeader;
  return {
    label: "头部品牌",
    value: `${rankingItemLabel(leader!)} 领跑`,
    detail: `当前份额 ${leader?.shareDisplay ?? formatPercent(leader?.sharePct)}。`,
    tone: "neutral",
  };
}

function competitionCard(page: MarketScanOverviewPage): MarketInsightCard {
  const monthlyItems = page.monthlyBrandRanking.items;
  const ytdItems = page.ytdBrandRanking.items;
  if (monthlyItems.length === 0 || ytdItems.length === 0) {
    return {
      label: "竞争格局",
      value: "暂无品牌榜数据",
      detail: "当前筛选下没有足够的品牌排名样本。",
      tone: "neutral",
    };
  }

  const monthlyLeader = rankingItemLabel(monthlyItems[0]);
  const ytdLeader = rankingItemLabel(ytdItems[0]);
  const monthlyTop3Share = monthlyItems.slice(0, 3).reduce((sum, item) => sum + (item.sharePct ?? 0), 0);

  if (monthlyLeader === ytdLeader && monthlyTop3Share >= 0.5) {
    return {
      label: "竞争格局",
      value: "头部稳固且集中",
      detail: `${monthlyLeader} 同时领跑月榜与 YTD，月榜 Top3 占 ${formatPercent(monthlyTop3Share)}。`,
      tone: "positive",
    };
  }
  if (monthlyLeader === ytdLeader) {
    return {
      label: "竞争格局",
      value: "头部稳定，竞争仍有空间",
      detail: `${monthlyLeader} 同时领跑月榜与 YTD，但月榜 Top3 占比仅 ${formatPercent(monthlyTop3Share)}。`,
      tone: "neutral",
    };
  }
  if (monthlyTop3Share < 0.35) {
    return {
      label: "竞争格局",
      value: "月榜更分散，短期换位频繁",
      detail: `${monthlyLeader} 拿下月榜，但 YTD 仍由 ${ytdLeader} 领跑；月榜 Top3 仅占 ${formatPercent(monthlyTop3Share)}。`,
      tone: "neutral",
    };
  }
  return {
    label: "竞争格局",
    value: "月榜冲量与累计格局分化",
    detail: `${monthlyLeader} 拿下月榜，但 YTD 仍由 ${ytdLeader} 领跑；短期冲量强于长期换挡。`,
    tone: "neutral",
  };
}

function watchoutCard(
  page: MarketScanOverviewPage,
  structure: StructureRead,
  rolling: RollingTrendRead,
): MarketInsightCard {
  const monthlyItems = page.monthlyBrandRanking.items;
  const monthlyLeader = monthlyItems[0];
  const monthlyLeaderName = monthlyLeader ? rankingItemLabel(monthlyLeader) : "头部品牌";
  const monthlyLeaderShare = monthlyLeader?.sharePct ?? 0;
  const monthlyTop3Share = monthlyItems.slice(0, 3).reduce((sum, item) => sum + (item.sharePct ?? 0), 0);

  const currentYoY = deltaValue(page.summary.currentMonthYoY);
  const latest = orderedTrendItems(page.trend.items).slice(-1)[0];
  const latestMoM = deltaValue(latest?.mom);
  const ytdYoY = deltaValue(page.summary.ytdYoY);
  const conflictingSignals = (
    currentYoY !== null
    && latestMoM !== null
    && ((currentYoY > 0 && latestMoM < 0) || (currentYoY < 0 && latestMoM > 0))
  ) || (
    currentYoY !== null
    && ytdYoY !== null
    && ((currentYoY > 0 && ytdYoY < 0) || (currentYoY < 0 && ytdYoY > 0))
  );

  if (monthlyLeaderShare >= 0.18 || monthlyTop3Share >= 0.55) {
    return {
      label: "下月观察",
      value: "关注头部冲量透支",
      detail: `${monthlyLeaderName} 月榜份额 ${formatPercent(monthlyLeaderShare)}，Top3 已占 ${formatPercent(monthlyTop3Share)}。`,
      tone: "negative",
    };
  }
  if (structure.driverFuel && (structure.driverShare ?? 0) >= 0.35) {
    return {
      label: "下月观察",
      value: `关注 ${structure.driverFuel} 延续性`,
      detail: `本轮结构变化主要依赖 ${structure.driverFuel}，当前占比已达 ${formatPercent(structure.driverShare)}。`,
      tone: "negative",
    };
  }
  if (conflictingSignals) {
    return {
      label: "下月观察",
      value: "观察单月与累计是否再分化",
      detail: `当前 YoY ${page.summary.currentMonthYoY.display}、MoM ${latest?.mom.display ?? "-"}、累计 YoY ${page.summary.ytdYoY.display}，仍需下月验证。`,
      tone: "neutral",
    };
  }
  if (rolling.tone === "negative") {
    return {
      label: "下月观察",
      value: "关注修复斜率是否企稳",
      detail: "Rolling 12M 已转弱，若下月仍未企稳，全年节奏可能继续放缓。",
      tone: "negative",
    };
  }
  return {
    label: "下月观察",
    value: "关注修复斜率能否延续",
    detail: "若 Rolling 12M 下月继续抬升，市场修复将从结构性改善走向更稳的总量修复。",
    tone: "neutral",
  };
}

export function buildOverviewInsight(page: MarketScanOverviewPage): MarketInsightSnapshot {
  const ordered = orderedTrendItems(page.trend.items);
  const latest = ordered[ordered.length - 1];
  const currentYoY = deltaValue(page.summary.currentMonthYoY);
  const latestMoM = deltaValue(latest?.mom);
  const ytdYoY = deltaValue(page.summary.ytdYoY);
  const rolling = buildRollingTrendRead(page.trend.items);
  const structure = buildStructureRead(page.trend.items);
  const competition = competitionCard(page);
  const watchout = watchoutCard(page, structure, rolling);

  let headline = "市场仍在震荡";
  let tone: InsightTone = "neutral";
  if ((currentYoY ?? 0) > 0 && (latestMoM ?? 0) > 0 && (ytdYoY ?? 0) > 0) {
    headline = "市场进入上行通道";
    tone = "positive";
  } else if ((currentYoY ?? 0) > 0 && (ytdYoY ?? 0) > 0 && (latestMoM ?? 0) < 0) {
    headline = "市场延续修复，但短期回踩";
    tone = "neutral";
  } else if ((currentYoY ?? 0) > 0 && (ytdYoY ?? 0) > 0) {
    headline = "市场延续修复";
    tone = "positive";
  } else if ((currentYoY ?? 0) < 0 && (ytdYoY ?? 0) > 0) {
    headline = "累计改善仍在，但单月承压";
    tone = "neutral";
  } else if ((currentYoY ?? 0) < 0 && (ytdYoY ?? 0) < 0) {
    headline = "市场整体偏弱";
    tone = "negative";
  }

  return {
    headline,
    summary: `${page.summary.headline}；${page.summary.subheadline}；${rolling.value}。`,
    tone,
    cards: [
      buildOverviewSignalCard(page, latest),
      {
        label: "规模趋势",
        value: rolling.tone === "positive" ? "修复斜率继续抬升" : rolling.tone === "negative" ? "总量斜率正在转弱" : "总量仍处平台区间",
        detail: `当月 YoY ${page.summary.currentMonthYoY.display}，MoM ${latest?.mom.display ?? "-"}，累计 YoY ${page.summary.ytdYoY.display}；${rolling.detail}`,
        tone: rolling.tone === "neutral" ? tone : rolling.tone,
      },
      {
        label: "结构驱动",
        value: structure.value,
        detail: structure.detail,
        tone: structure.tone,
      },
      buildOverviewLeaderCard(page),
      competition,
      watchout,
    ],
  };
}

function buildOriginBrandRead(group: MarketScanOriginBrandGroup | undefined): MarketInsightCard {
  if (!group || group.series.length === 0) {
    return {
      label: "品牌牵引",
      value: "缺少品牌走势样本",
      detail: "当前没有足够的车系内品牌走势数据。",
      tone: "neutral",
    };
  }
  const rankedBrands = [...group.series]
    .map((series) => ({
      brand: series.brand,
      latestPoint: lastItem(series.points),
    }))
    .filter((item): item is { brand: string; latestPoint: NonNullable<typeof item.latestPoint> } => Boolean(item.latestPoint))
    .sort((left, right) => (right.latestPoint.volume ?? 0) - (left.latestPoint.volume ?? 0));
  const leader = rankedBrands[0];
  const runnerUp = rankedBrands[1];
  if (!leader) {
    return {
      label: "品牌牵引",
      value: "缺少品牌走势样本",
      detail: "当前没有足够的车系内品牌走势数据。",
      tone: "neutral",
    };
  }
  return {
    label: "品牌牵引",
    value: `${leader.brand} 是 ${group.origin} 当前核心支点`,
    detail: runnerUp
      ? `最新月 ${formatVolume(leader.latestPoint.volume)} 台，领先第二名 ${runnerUp.brand} ${formatVolume((leader.latestPoint.volume ?? 0) - (runnerUp.latestPoint.volume ?? 0))} 台。`
      : `最新月贡献 ${formatVolume(leader.latestPoint.volume)} 台。`,
    tone: "neutral",
  };
}

export function buildOriginInsight(page: MarketScanOriginPage): MarketInsightSnapshot {
  const currentRow = matrixRow(page.matrix, "current_volume");
  const yoyRow = matrixRow(page.matrix, "yoy");
  const ytdRow = matrixRow(page.matrix, "ytd");
  const ytdYoYRow = matrixRow(page.matrix, "ytd_yoy");
  const currentLeader = topMatrixCell(currentRow);
  const yoyLeader = topPositiveMatrixCell(yoyRow);
  const ytdLeader = topMatrixCell(ytdRow);
  const ytdYoYLeader = topPositiveMatrixCell(ytdYoYRow);
  const latestShares = page.trend.series
    .map((series) => ({ origin: series.origin, point: lastItem(series.points) }))
    .filter((entry): entry is { origin: string; point: NonNullable<typeof entry.point> } => Boolean(entry.point))
    .sort((left, right) => (right.point.volume ?? 0) - (left.point.volume ?? 0));
  const latestLeader = latestShares[0];
  const runnerUp = latestShares[1];
  const shareGap = (latestLeader?.point.sharePct ?? 0) - (runnerUp?.point.sharePct ?? 0);
  const currentTotal = totalMatrixRow(currentRow);
  const currentLeaderShare = safeShare(Number(currentLeader?.value ?? 0), currentTotal);
  const brandCard = buildOriginBrandRead(
    page.brandTrend.groups.find((group) => group.origin === currentLeader?.key) ?? page.brandTrend.groups[0],
  );

  let headline = "车系格局保持轮动";
  let tone: InsightTone = "neutral";
  if (currentLeader?.key && ytdLeader?.key && currentLeader.key === ytdLeader.key) {
    headline = `${currentLeader.key} 继续主导车系格局`;
    tone = "positive";
  } else if (currentLeader?.key) {
    headline = `${currentLeader.key} 月度抬头，车系格局重排`;
    tone = "neutral";
  }

  return {
    headline,
    summary: `${page.summaryText}${yoyLeader ? ` ${yoyLeader.key} 是当前同比弹性最高的车系。` : ""}`,
    tone,
    cards: [
      {
        label: "主导车系",
        value: currentLeader?.key ? `${currentLeader.key} 当月领先` : "暂无主导车系",
        detail: currentLeader?.key
          ? `当月份额 ${formatPercent(currentLeaderShare)}，领先第二名 ${formatSignedPctPoints(shareGap)}。`
          : "当前没有足够的车系销量数据。",
        tone: shareGap >= 0.03 ? "positive" : "neutral",
      },
      {
        label: "增长亮点",
        value: yoyLeader?.key ? `${yoyLeader.key} 同比领跑` : "暂无明显增速亮点",
        detail: yoyLeader?.key
          ? `当月同比 ${yoyLeader.display}，说明该车系在当前窗口内弹性更强。`
          : "当前没有可用的同比变化样本。",
        tone: (yoyLeader?.value ?? 0) > 0 ? "positive" : "neutral",
      },
      {
        label: "累计格局",
        value: ytdLeader?.key ? `${ytdLeader.key} 仍居 YTD 第一` : "暂无累计格局样本",
        detail: ytdLeader?.key
          ? `累计表现由 ${ytdLeader.key} 领跑${ytdYoYLeader?.key ? `，其中 ${ytdYoYLeader.key} 的累计弹性更突出。` : "。"}`
          : "当前没有足够的累计车系数据。",
        tone: currentLeader?.key && ytdLeader?.key && currentLeader.key === ytdLeader.key ? "positive" : "neutral",
      },
      brandCard,
      {
        label: "下月观察",
        value: currentLeader?.key && ytdLeader?.key && currentLeader.key !== ytdLeader.key
          ? "关注月度抬头能否传导到累计"
          : yoyLeader?.key && yoyLeader.key !== currentLeader?.key
            ? `关注 ${yoyLeader.key} 份额扩张`
            : "关注头部车系份额是否继续集中",
        detail: currentLeader?.key && ytdLeader?.key && currentLeader.key !== ytdLeader.key
          ? `${currentLeader.key} 已领先当月，但全年仍由 ${ytdLeader.key} 把持，需要继续验证趋势强度。`
          : yoyLeader?.key && yoyLeader.key !== currentLeader?.key
            ? `${yoyLeader.key} 已表现出更高弹性，若连续扩张会改写现有车系排序。`
            : "若头部车系继续扩大份额，后续品牌趋势会进一步向少数车系集中。",
        tone: currentLeader?.key && ytdLeader?.key && currentLeader.key !== ytdLeader.key ? "neutral" : "negative",
      },
    ],
  };
}

function compareBodyShare(
  latest: MarketScanBodyShareTrendItem | undefined,
  compare: MarketScanBodyShareTrendItem | undefined,
): { body: "SUV" | "Sedan"; latestShare: number; delta: number } {
  const suvShare = latest?.suvSharePct ?? 0;
  const sedanShare = latest?.sedanSharePct ?? 0;
  const leader = suvShare >= sedanShare ? "SUV" : "Sedan";
  if (leader === "SUV") {
    return { body: leader, latestShare: suvShare, delta: suvShare - (compare?.suvSharePct ?? 0) };
  }
  return { body: leader, latestShare: sedanShare, delta: sedanShare - (compare?.sedanSharePct ?? 0) };
}

export function buildSegmentInsight(page: MarketScanSegmentPage): MarketInsightSnapshot {
  const latestBody = lastItem(page.bodyShareTrend.items);
  const compareBody = page.bodyShareTrend.items.length >= 6
    ? page.bodyShareTrend.items[page.bodyShareTrend.items.length - 6]
    : page.bodyShareTrend.items[page.bodyShareTrend.items.length - 2];
  const bodyRead = compareBodyShare(latestBody, compareBody);
  const latestSuvSplit = lastItem(page.suvSegmentShareTrend.items);
  const rankedSuvBuckets = Object.entries(latestSuvSplit?.segmentSharePct ?? {})
    .sort((left, right) => right[1] - left[1]);
  const topSuvBucket = rankedSuvBuckets[0];

  const currentRow = matrixRow(page.matrix, "current_volume");
  const yoyRow = matrixRow(page.matrix, "yoy");
  const ytdRow = matrixRow(page.matrix, "ytd");
  const currentLeader = topMatrixCell(currentRow);
  const yoyLeader = topPositiveMatrixCell(yoyRow);
  const ytdLeader = topMatrixCell(ytdRow);
  const currentTotal = totalMatrixRow(currentRow);
  const currentLeaderShare = safeShare(Number(currentLeader?.value ?? 0), currentTotal);

  let headline = "车身结构保持均衡";
  let tone: InsightTone = "neutral";
  if (bodyRead.body === "SUV" && bodyRead.latestShare >= 0.55) {
    headline = "SUV 继续主导车身结构";
    tone = "positive";
  } else if (bodyRead.body === "Sedan" && bodyRead.latestShare >= 0.5) {
    headline = "Sedan 占比明显回升";
    tone = "positive";
  }

  return {
    headline,
    summary: `${page.summaryText} ${bodyRead.body} 近期份额变化 ${formatSignedPctPoints(bodyRead.delta)}。`,
    tone,
    cards: [
      {
        label: "车身结构",
        value: `${bodyRead.body} 占比 ${formatPercent(bodyRead.latestShare)}`,
        detail: `${bodyRead.body} 相比观察窗口前段变化 ${formatSignedPctPoints(bodyRead.delta)}。`,
        tone: bodyRead.delta > 0.01 ? "positive" : bodyRead.delta < -0.01 ? "negative" : "neutral",
      },
      {
        label: "长度级别",
        value: currentLeader?.key ? `${currentLeader.key} 当月容量最大` : "暂无长度级别样本",
        detail: currentLeader?.key
          ? `当前占全部长度桶 ${formatPercent(currentLeaderShare)}，销量 ${formatVolume(currentLeader.value)} 台。`
          : "当前没有足够的长度级别销量数据。",
        tone: "neutral",
      },
      {
        label: "增速亮点",
        value: yoyLeader?.key ? `${yoyLeader.key} 同比弹性最高` : "暂无同比亮点",
        detail: yoyLeader?.key
          ? `同比 ${yoyLeader.display}，是当前结构切换中最活跃的长度级别。`
          : "当前没有可用的同比变化样本。",
        tone: (yoyLeader?.value ?? 0) > 0 ? "positive" : "neutral",
      },
      {
        label: "SUV 内部拆分",
        value: topSuvBucket ? `${topSuvBucket[0]} 是 SUV 内部第一层` : "暂无 SUV 内部拆分样本",
        detail: topSuvBucket
          ? `当前在整体市场中的占比约 ${formatPercent(topSuvBucket[1])}。`
          : "当前没有足够的 SUV 内部结构样本。",
        tone: "neutral",
      },
      {
        label: "下月观察",
        value: currentLeader?.key && ytdLeader?.key && currentLeader.key !== ytdLeader.key
          ? "关注当月与累计领先级别是否切换"
          : topSuvBucket
            ? `关注 ${topSuvBucket[0]} 是否继续放大`
            : "关注结构切换是否延续",
        detail: currentLeader?.key && ytdLeader?.key && currentLeader.key !== ytdLeader.key
          ? `当月由 ${currentLeader.key} 领跑，但累计第一仍是 ${ytdLeader.key}，说明结构变化仍在过渡期。`
          : topSuvBucket
            ? `${topSuvBucket[0]} 已成为 SUV 份额核心层，若继续扩大将改变整体长度结构。`
            : "若 SUV / Sedan 份额继续偏移，后续长度级别矩阵会更快重排。",
        tone: "neutral",
      },
    ],
  };
}

function buildFuelDominanceRead(items: MarketScanFuelTrendItem[]) {
  const latest = lastItem(items);
  const prior = items.length >= 2 ? items[items.length - 2] : undefined;
  const ranked = Object.entries(latest?.fuelMix ?? {}).sort((left, right) => right[1] - left[1]);
  const dominant = ranked[0];
  if (!latest || !dominant) {
    return null;
  }
  const dominantShare = safeShare(dominant[1], latest.totalVolume);
  const priorShare = safeShare(Number(prior?.fuelMix?.[dominant[0]] ?? 0), prior?.totalVolume ?? 0);
  return {
    fuel: dominant[0],
    share: dominantShare,
    shareDelta: dominantShare - priorShare,
    volume: dominant[1],
  };
}

function buildFuelPanelLeaderRead(panel: MarketScanFuelPanel | undefined) {
  const ytdLeader = panel?.ytdRanking[0];
  const monthLeader = panel?.monthRanking[0];
  if (!panel || !ytdLeader) {
    return null;
  }
  return {
    fuelType: panel.fuelType,
    ytdLeader,
    monthLeader,
  };
}

export function buildDrilldownInsight(page: MarketScanDrilldownPage): MarketInsightSnapshot {
  const totalLeader = page.totalRanking.items[0];
  const totalTop3Share = page.totalRanking.items.slice(0, 3).reduce((sum, item) => sum + (item.sharePct ?? 0), 0);
  const dominantFuel = buildFuelDominanceRead(page.ytdFuelTrend.items);
  const strongestFuelPanel = [...page.fuelPanels]
    .map((panel) => buildFuelPanelLeaderRead(panel))
    .filter((entry): entry is NonNullable<typeof entry> => Boolean(entry))
    .sort((left, right) => (right.ytdLeader.sharePct ?? 0) - (left.ytdLeader.sharePct ?? 0))[0];

  let headline = `${page.segmentLabel} 头部格局仍在重排`;
  let tone: InsightTone = "neutral";
  if (totalLeader?.model) {
    headline = `${totalLeader.model} 继续领跑 ${page.segmentLabel}`;
    tone = (totalLeader.yoy.value ?? 0) > 0 ? "positive" : "neutral";
  }

  return {
    headline,
    summary: `${page.summaryText}${dominantFuel ? ` ${dominantFuel.fuel} 当前占累计结构 ${formatPercent(dominantFuel.share)}。` : ""}`,
    tone,
    cards: [
      {
        label: "榜首车型",
        value: totalLeader?.model ? `${totalLeader.model} 位居 YTD 第一` : "暂无榜首车型",
        detail: totalLeader?.model
          ? `当前份额 ${totalLeader.shareDisplay ?? formatPercent(totalLeader.sharePct)}，同比 ${totalLeader.yoy.display}。`
          : "当前筛选下没有足够的车型排行数据。",
        tone: (totalLeader?.yoy.value ?? 0) > 0 ? "positive" : "neutral",
      },
      {
        label: "动力主线",
        value: dominantFuel ? `${dominantFuel.fuel} 是当前累计主线` : "暂无动力主线样本",
        detail: dominantFuel
          ? `占累计结构 ${formatPercent(dominantFuel.share)}，较上一累计窗口 ${formatSignedPctPoints(dominantFuel.shareDelta)}。`
          : "当前没有足够的累计动力结构数据。",
        tone: dominantFuel && dominantFuel.shareDelta > 0 ? "positive" : "neutral",
      },
      {
        label: "集中度",
        value: `Top3 占 ${formatPercent(totalTop3Share)}`,
        detail: totalTop3Share >= 0.4
          ? "头部车型集中度偏高，榜首波动会直接影响细分市场走势。"
          : "头部集中度仍可控，细分市场内部仍有换位空间。",
        tone: totalTop3Share >= 0.4 ? "negative" : "neutral",
      },
      {
        label: "细分亮点",
        value: strongestFuelPanel ? `${strongestFuelPanel.fuelType} 头部车型最强` : "暂无燃料亮点样本",
        detail: strongestFuelPanel
          ? `${rankingItemLabel(strongestFuelPanel.ytdLeader)} 是 ${strongestFuelPanel.fuelType} 当前累计第一${
            strongestFuelPanel.monthLeader && rankingItemLabel(strongestFuelPanel.monthLeader) !== rankingItemLabel(strongestFuelPanel.ytdLeader)
              ? `，但月榜已被 ${rankingItemLabel(strongestFuelPanel.monthLeader)} 挑战`
              : ""
          }。`
          : "当前没有足够的燃料面板排行数据。",
        tone: "neutral",
      },
      {
        label: "下月观察",
        value: strongestFuelPanel?.monthLeader && rankingItemLabel(strongestFuelPanel.monthLeader) !== rankingItemLabel(strongestFuelPanel.ytdLeader)
          ? `关注 ${strongestFuelPanel.fuelType} 月榜冲量能否传导到累计`
          : dominantFuel && dominantFuel.share >= 0.5
            ? `关注 ${dominantFuel.fuel} 是否继续单线扩张`
            : "关注头部车型是否继续集中",
        detail: strongestFuelPanel?.monthLeader && rankingItemLabel(strongestFuelPanel.monthLeader) !== rankingItemLabel(strongestFuelPanel.ytdLeader)
          ? `${strongestFuelPanel.fuelType} 月榜已出现新领跑者，若连续两月延续，累计榜将更快改写。`
          : dominantFuel && dominantFuel.share >= 0.5
            ? `${dominantFuel.fuel} 已占据较高结构权重，后续细分市场波动会更依赖该动力路线。`
            : "若 Top3 份额继续提升，细分市场会从多车型竞争转向少数车型主导。",
        tone: dominantFuel && dominantFuel.share >= 0.5 ? "negative" : "neutral",
      },
    ],
  };
}
