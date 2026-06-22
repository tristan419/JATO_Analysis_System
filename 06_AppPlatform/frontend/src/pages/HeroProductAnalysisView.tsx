import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { CSSProperties, ReactNode } from "react";
import {
  AllCommunityModule,
  ModuleRegistry,
  themeAlpine,
  type CellValueChangedEvent,
  type ColDef,
} from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { useSearchParams } from "react-router-dom";

import { api } from "../api/client";
import { DeckExportDrawer, DeckFloatingDrawer } from "../components/deckControls";
import { DeckSubpageNav } from "../components/DeckSubpageNav";
import { DEFAULT_EXPORT, ExportPanel, type ExportSettings } from "../components/ExportPanel";
import { SlideLayoutEditor } from "../components/SlideLayoutEditor";
import { useAuth } from "../contexts/AuthContext";
import type {
  HeroProductDeckResponse,
  HeroProductModelRow,
  HeroProductPriceSource,
  HeroProductSalesMode,
  HeroProductTrendSeries,
  MarketScanCountryOption,
} from "../types";
import { useFixedCanvasPreview } from "../utils/useFixedCanvasPreview";
import {
  DEFAULT_SLIDE_LAYOUT,
  readStoredSlideLayouts,
  updateSlideLayout,
  writeStoredSlideLayouts,
  type SlideLayoutSettings,
} from "../utils/slideLayout";

ModuleRegistry.registerModules([AllCommunityModule]);

type HeroProductPageKey =
  | "benchmark"
  | "benchmarkWithChannel"
  | "topTrend"
  | "topDistribution"
  | "heroTrend"
  | "heroDistribution";

type HeroProductScopeMode = "all" | "price";
type HeroProductDistributionLayout = "ranked" | "aligned";
type HeroProductInsightTone = "positive" | "negative" | "neutral" | "new";
type HeroProductSpecEditableField =
  | "brand"
  | "model"
  | "rangeKm"
  | "batteryKwh"
  | "consumptionKwh100km"
  | "accelerationSec"
  | "chargingText";
type HeroProductSystemSpecColumnKey =
  | "brand"
  | "model"
  | "sales"
  | "sharePct"
  | "yoy"
  | "fourWd"
  | "business"
  | "rangeKm"
  | "batteryKwh"
  | "consumptionKwh100km"
  | "accelerationSec"
  | "chargingText"
  | "price";
type HeroProductCustomSpecColumnKey = `custom:${string}`;
type HeroProductSpecColumnKey = HeroProductSystemSpecColumnKey | HeroProductCustomSpecColumnKey;

interface HeroProductSpecColumnOption {
  key: HeroProductSpecColumnKey;
  label: string;
  isCustom?: boolean;
}

interface HeroProductInsightCard {
  label: string;
  value: string;
  detail: string;
  tone?: HeroProductInsightTone;
}

interface HeroProductInsight {
  eyebrow: string;
  headline: string;
  summary: string;
  cards: HeroProductInsightCard[];
}

type HeroProductPriceEditableRow = Pick<HeroProductModelRow, "brand" | "model" | "sourceBrand" | "sourceModel" | "price">;

interface HeroProductPriceEditorBinding {
  canEdit: boolean;
  priceDrafts: Record<string, string>;
  savingPriceKey: string;
  savingSpecKey: string;
  saveMessage: string;
  onDraftChange: (row: HeroProductPriceEditableRow, value: string) => void;
  onSavePrice: (row: HeroProductPriceEditableRow) => void;
  onSavePriceValue: (row: HeroProductPriceEditableRow, value: string) => Promise<void>;
  onSaveSpecValue: (row: HeroProductPriceEditableRow, fieldName: HeroProductSpecEditableField | HeroProductCustomSpecColumnKey, value: string) => Promise<void>;
}

interface HeroProductSpecGridRow {
  [key: string]: unknown;
  rowId: string;
  brand: string;
  model: string;
  sourceBrand?: string;
  sourceModel?: string;
  pricePayload: HeroProductModelRow["price"];
  sales: string;
  sharePct: string;
  yoy: string;
  yoyTone: string;
  fourWd: string;
  business: string;
  rangeKm: string;
  batteryKwh: string;
  consumptionKwh100km: string;
  accelerationSec: string;
  chargingText: string;
  price: string;
}

interface HeroProductPricePanelGridRow {
  rowId: string;
  brand: string;
  model: string;
  sourceBrand?: string;
  sourceModel?: string;
  origin: string;
  pricePayload: HeroProductModelRow["price"];
  raw: string;
  source: string;
  saved: string;
  status: string;
  isMissing: boolean;
}

const HERO_PAGE_ITEMS: Array<{ key: HeroProductPageKey; code: string; label: string; sublabel: string }> = [
  { key: "benchmark", code: "01", label: "BEV Benchmark", sublabel: "动总对标" },
  { key: "benchmarkWithChannel", code: "02", label: "Channel Benchmark", sublabel: "渠道对标" },
  { key: "topTrend", code: "03", label: "Top Trend", sublabel: "Top 趋势" },
  { key: "topDistribution", code: "04", label: "Top Markets", sublabel: "Top 分布" },
  { key: "heroTrend", code: "05", label: "Hero Trend", sublabel: "固定车型趋势" },
  { key: "heroDistribution", code: "06", label: "Hero Markets", sublabel: "固定车型分布" },
];

const DEFAULT_HERO_EXPORT: ExportSettings = {
  ...DEFAULT_EXPORT,
  exportWidth: 1920,
  exportHeight: 1080,
  paperBg: "#ffffff",
  plotBg: "#ffffff",
  dataLabelMode: "value",
  fontSize: 12,
};
const DEFAULT_HERO_LAYOUTS: Record<HeroProductPageKey, SlideLayoutSettings> = {
  benchmark: DEFAULT_SLIDE_LAYOUT,
  benchmarkWithChannel: DEFAULT_SLIDE_LAYOUT,
  topTrend: DEFAULT_SLIDE_LAYOUT,
  topDistribution: DEFAULT_SLIDE_LAYOUT,
  heroTrend: DEFAULT_SLIDE_LAYOUT,
  heroDistribution: DEFAULT_SLIDE_LAYOUT,
};
const SALES_MODES: Array<{ value: HeroProductSalesMode; label: string }> = [
  { value: "month", label: "当月" },
  { value: "ytd", label: "YTD" },
  { value: "rolling12", label: "近12月" },
];
const PRICE_SOURCES: Array<{ value: HeroProductPriceSource; label: string }> = [
  { value: "msrp", label: "MSRP 抓取价" },
  { value: "jato", label: "JATO 价格" },
];
const DISTRIBUTION_LAYOUTS: Array<{ value: HeroProductDistributionLayout; label: string }> = [
  { value: "ranked", label: "独立排序" },
  { value: "aligned", label: "国家对齐" },
];
const HERO_PRODUCT_SPEC_COLUMN_OPTIONS: Array<{ key: HeroProductSystemSpecColumnKey; label: string }> = [
  { key: "brand", label: "品牌" },
  { key: "model", label: "车型" },
  { key: "sales", label: "销量" },
  { key: "sharePct", label: "份额" },
  { key: "yoy", label: "YoY" },
  { key: "fourWd", label: "4WD" },
  { key: "business", label: "Business" },
  { key: "rangeKm", label: "续航" },
  { key: "batteryKwh", label: "电池" },
  { key: "consumptionKwh100km", label: "电耗" },
  { key: "accelerationSec", label: "零百" },
  { key: "chargingText", label: "充电" },
  { key: "price", label: "价格" },
];
const ALL_HERO_PRODUCT_SPEC_COLUMNS = HERO_PRODUCT_SPEC_COLUMN_OPTIONS.map((option) => option.key);
const DEFAULT_HERO_PRODUCT_SPEC_COLUMNS: HeroProductSystemSpecColumnKey[] = [
  "brand",
  "model",
  "rangeKm",
  "batteryKwh",
  "consumptionKwh100km",
  "accelerationSec",
  "chargingText",
  "price",
];
const HERO_PRODUCT_CUSTOM_SPEC_STORAGE_KEY = "jato.hero-product.customSpecColumns.v1";
const LINE_COLORS = ["#2563eb", "#f97316", "#16a34a", "#7c3aed", "#0ea5e9", "#dc2626", "#64748b", "#ca8a04", "#059669", "#db2777"];
const HERO_PRODUCT_CHANNEL_ORDER = ["Business", "Private", "Other"] as const;
const HERO_PRODUCT_CHANNEL_META: Record<(typeof HERO_PRODUCT_CHANNEL_ORDER)[number], { label: string; color: string; textColor: string }> = {
  Business: { label: "Business", color: "#9ca3af", textColor: "#111827" },
  Private: { label: "Private", color: "#b7d5ed", textColor: "#0f172a" },
  Other: { label: "Other", color: "#e5e7eb", textColor: "#475569" },
};
const HERO_PRODUCT_DRIVE_META = {
  front: { label: "front", color: "#11a8dd" },
  rear: { label: "rear", color: "#f27a2e" },
  "4x4": { label: "4x4", color: "#a3a3a3" },
} as const;

function isHeroProductPageKey(value: string | null): value is HeroProductPageKey {
  return HERO_PAGE_ITEMS.some((item) => item.key === value);
}

function isSalesMode(value: string | null): value is HeroProductSalesMode {
  return value === "month" || value === "ytd" || value === "rolling12";
}

function isPriceSource(value: string | null): value is HeroProductPriceSource {
  return value === "msrp" || value === "jato";
}

function isDistributionLayout(value: string | null): value is HeroProductDistributionLayout {
  return value === "ranked" || value === "aligned";
}

function normalizeExportDimension(value: number, fallback: number, min: number): number {
  return Number.isFinite(value) ? Math.max(min, Math.round(value)) : fallback;
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return Math.round(value).toLocaleString();
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return `${(value * 100).toFixed(1)}%`;
}

function formatWholePercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "-";
  return `${Math.round(value * 100)}%`;
}

function formatPrice(value: number | null | undefined, currency: string): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "";
  return `${Math.round(value).toLocaleString()} ${currency || "EUR"}`;
}

function formatPriceNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "";
  return Math.round(value).toLocaleString();
}

function editableRowFromSpecGridRow(row: HeroProductSpecGridRow): HeroProductPriceEditableRow {
  return {
    brand: row.brand,
    model: row.model,
    sourceBrand: row.sourceBrand,
    sourceModel: row.sourceModel,
    price: row.pricePayload,
  };
}

function editableRowFromPriceGridRow(row: HeroProductPricePanelGridRow): HeroProductPriceEditableRow {
  return {
    brand: row.brand,
    model: row.model,
    sourceBrand: row.sourceBrand,
    sourceModel: row.sourceModel,
    price: row.pricePayload,
  };
}

function cleanPriceInput(value: unknown): string {
  return String(value ?? "").replace(/[^\d.-]/g, "").trim();
}

function isCustomSpecColumnKey(value: string | undefined): value is HeroProductCustomSpecColumnKey {
  return typeof value === "string" && value.startsWith("custom:") && value.length > "custom:".length;
}

function isEditableSpecGridField(field: string | undefined): field is HeroProductSpecEditableField | HeroProductCustomSpecColumnKey | "price" {
  return field === "price"
    || field === "brand"
    || field === "model"
    || field === "rangeKm"
    || field === "batteryKwh"
    || field === "consumptionKwh100km"
    || field === "accelerationSec"
    || field === "chargingText"
    || isCustomSpecColumnKey(field);
}

function salesModeColumnPrefix(mode: HeroProductSalesMode): string {
  if (mode === "month") return "当月";
  if (mode === "rolling12") return "近12月";
  return "YTD";
}

function salesModeLabel(mode: HeroProductSalesMode): string {
  return SALES_MODES.find((item) => item.value === mode)?.label ?? "YTD";
}

function sanitizeCustomSpecColumnLabel(value: string): string {
  return value
    .replace(/[.:|]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 32);
}

function customSpecColumnLabelFromKey(key: HeroProductCustomSpecColumnKey): string {
  return sanitizeCustomSpecColumnLabel(key.replace(/^custom:/, "")) || "自定义列";
}

function makeCustomSpecColumn(label: string, existingKeys: Iterable<HeroProductSpecColumnKey>): HeroProductSpecColumnOption | null {
  const baseLabel = sanitizeCustomSpecColumnLabel(label);
  if (!baseLabel) return null;
  const used = new Set(existingKeys);
  let nextLabel = baseLabel;
  let suffix = 2;
  while (used.has(`custom:${nextLabel}` as HeroProductCustomSpecColumnKey)) {
    nextLabel = `${baseLabel} ${suffix}`;
    suffix += 1;
  }
  return { key: `custom:${nextLabel}` as HeroProductCustomSpecColumnKey, label: nextLabel, isCustom: true };
}

function readStoredCustomSpecColumns(): HeroProductSpecColumnOption[] {
  if (typeof window === "undefined" || typeof window.localStorage === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(HERO_PRODUCT_CUSTOM_SPEC_STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.flatMap((item) => {
      if (!item || typeof item !== "object") return [];
      const record = item as Record<string, unknown>;
      const key = typeof record.key === "string" && isCustomSpecColumnKey(record.key) ? record.key : null;
      if (!key) return [];
      return [{ key, label: sanitizeCustomSpecColumnLabel(String(record.label ?? "")) || customSpecColumnLabelFromKey(key), isCustom: true }];
    });
  } catch {
    return [];
  }
}

function writeStoredCustomSpecColumns(columns: HeroProductSpecColumnOption[]): void {
  if (typeof window === "undefined" || typeof window.localStorage === "undefined") return;
  try {
    window.localStorage.setItem(
      HERO_PRODUCT_CUSTOM_SPEC_STORAGE_KEY,
      JSON.stringify(columns.flatMap((column) => {
        if (!isCustomSpecColumnKey(column.key)) return [];
        return [{
          key: column.key,
          label: sanitizeCustomSpecColumnLabel(column.label) || customSpecColumnLabelFromKey(column.key),
        }];
      })),
    );
  } catch {
    // Ignore localStorage failures; custom columns still work for the current session.
  }
}

function parseModelList(value: string): string[] {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function parseCountryLimit(value: string): number {
  const trimmed = value.trim();
  if (!trimmed) return 0;
  const numeric = Number(trimmed);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(80, Math.round(numeric)));
}

function sanitizeFileNameSegment(value: string): string {
  return value.trim().replace(/[^0-9a-zA-Z\u4e00-\u9fa5_-]+/g, "-").replace(/-+/g, "-") || "hero-product";
}

function canEditPrices(role: string | undefined): boolean {
  return role === "editor" || role === "admin" || role === "developer";
}

function specList(values: number[] | undefined, suffix: string): string {
  if (!values || values.length === 0) return "";
  return values.map((value) => `${Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1)}${suffix}`).join(" / ");
}

function specDisplay(values: number[] | undefined, suffix: string): string {
  return specList(values, suffix) || "-";
}

function maxSpecValue(values: number[] | undefined): number | null {
  if (!values || values.length === 0) return null;
  const finiteValues = values.filter(Number.isFinite);
  return finiteValues.length > 0 ? Math.max(...finiteValues) : null;
}

function minSpecValue(values: number[] | undefined): number | null {
  if (!values || values.length === 0) return null;
  const finiteValues = values.filter(Number.isFinite);
  return finiteValues.length > 0 ? Math.min(...finiteValues) : null;
}

function selectedPricePayload(price: HeroProductModelRow["price"], source: HeroProductPriceSource) {
  return price.sources[source] ?? price.selected;
}

function selectedPrice(row: HeroProductModelRow, source: HeroProductPriceSource) {
  return selectedPricePayload(row.price, source);
}

function sourceBrand(row: Pick<HeroProductModelRow, "brand" | "sourceBrand">): string {
  return row.sourceBrand || row.brand;
}

function sourceModel(row: Pick<HeroProductModelRow, "model" | "sourceModel">): string {
  return row.sourceModel || row.model;
}

function rowKey(row: Pick<HeroProductModelRow, "brand" | "model">, source: HeroProductPriceSource): string {
  const stable = row as Pick<HeroProductModelRow, "brand" | "model" | "sourceBrand" | "sourceModel">;
  return `${source}:${sourceBrand(stable)}:${sourceModel(stable)}`;
}

function priceStatusLabel(status: string): string {
  if (status === "manual_override") return "manual";
  if (status === "missing") return "待录入";
  return "source";
}

function getRowsForPage(deck: HeroProductDeckResponse | null, pageKey: HeroProductPageKey): HeroProductModelRow[] {
  if (!deck) return [];
  if (pageKey === "benchmark" || pageKey === "benchmarkWithChannel") {
    return deck.pages[pageKey].productRows;
  }
  if (pageKey === "topTrend") return deck.pages.topTrend.models;
  if (pageKey === "topDistribution") return deck.pages.topDistribution.models;
  if (pageKey === "heroTrend") return deck.pages.heroTrend.models;
  return deck.pages.heroDistribution.models;
}

function countryValue(options: MarketScanCountryOption[], value: string): string {
  return options.find((option) => option.value === value || option.label === value)?.value ?? value;
}

function clampShare(value: number | null | undefined): number {
  if (value === null || value === undefined || !Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function toneClassName(tone?: string): string {
  if (tone === "positive") return "is-positive";
  if (tone === "negative") return "is-negative";
  if (tone === "new") return "is-new";
  return "is-neutral";
}

function channelShare(row: HeroProductModelRow, channel: (typeof HERO_PRODUCT_CHANNEL_ORDER)[number]): number {
  const direct = row.channelSharePct[channel];
  if (Number.isFinite(direct)) return clampShare(direct);
  const total = Object.values(row.channelMix ?? {}).reduce((sum, value) => sum + Math.max(0, Number(value) || 0), 0);
  if (total <= 0) return 0;
  return clampShare(Number(row.channelMix[channel] ?? 0) / total);
}

function businessShare(row: HeroProductModelRow): number {
  return channelShare(row, "Business");
}

function channelMixText(row: HeroProductModelRow): string {
  const parts = HERO_PRODUCT_CHANNEL_ORDER.map((channel) => {
    const share = channelShare(row, channel);
    return share > 0 ? `${HERO_PRODUCT_CHANNEL_META[channel].label} ${formatWholePercent(share)}` : null;
  }).filter((value): value is string => Boolean(value));
  return parts.length > 0 ? parts.join(" · ") : "渠道暂无";
}

function driveTotal(row: HeroProductModelRow): number {
  return Object.values(row.driveMix ?? {}).reduce((sum, value) => sum + Math.max(0, Number(value) || 0), 0);
}

function driveVolume(row: HeroProductModelRow, key: keyof typeof HERO_PRODUCT_DRIVE_META): number {
  return Math.max(0, Number(row.driveMix?.[key] ?? 0) || 0);
}

function driveShare(row: HeroProductModelRow, key: keyof typeof HERO_PRODUCT_DRIVE_META): number {
  const total = driveTotal(row);
  if (total <= 0) return 0;
  return clampShare(driveVolume(row, key) / total);
}

function fourWheelShare(row: HeroProductModelRow): number {
  return driveShare(row, "4x4");
}

function specTone(value: number | null, values: number[], higherBetter: boolean): string {
  if (value === null || values.length < 2) return "";
  const finiteValues = values.filter(Number.isFinite);
  if (finiteValues.length < 2) return "";
  const min = Math.min(...finiteValues);
  const max = Math.max(...finiteValues);
  if (max <= min) return "";
  const score = higherBetter ? (value - min) / (max - min) : (max - value) / (max - min);
  if (score >= 0.72) return "is-good";
  if (score <= 0.28) return "is-watch";
  return "";
}

function isFiniteNumber(value: number | null | undefined): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function maxBy<T>(items: T[], pickValue: (item: T) => number | null | undefined): T | null {
  let bestItem: T | null = null;
  let bestValue = -Infinity;
  items.forEach((item) => {
    const value = pickValue(item);
    if (isFiniteNumber(value) && value > bestValue) {
      bestItem = item;
      bestValue = value;
    }
  });
  return bestItem;
}

function minBy<T>(items: T[], pickValue: (item: T) => number | null | undefined): T | null {
  let bestItem: T | null = null;
  let bestValue = Infinity;
  items.forEach((item) => {
    const value = pickValue(item);
    if (isFiniteNumber(value) && value < bestValue) {
      bestItem = item;
      bestValue = value;
    }
  });
  return bestItem;
}

function formatSignedNumber(value: number | null | undefined): string {
  if (!isFiniteNumber(value)) return "-";
  const rounded = Math.round(value);
  return `${rounded > 0 ? "+" : ""}${rounded.toLocaleString()}`;
}

function firstTrendVolume(series: HeroProductTrendSeries): number | null {
  return series.points[0]?.volume ?? null;
}

function lastTrendVolume(series: HeroProductTrendSeries): number | null {
  return series.points[series.points.length - 1]?.volume ?? null;
}

function trendDelta(series: HeroProductTrendSeries): number | null {
  const first = firstTrendVolume(series);
  const last = lastTrendVolume(series);
  return isFiniteNumber(first) && isFiniteNumber(last) ? last - first : null;
}

function modelLabel(row: Pick<HeroProductModelRow, "brand" | "model"> | null | undefined): string {
  if (!row) return "-";
  return row.brand ? `${row.brand} ${row.model}` : row.model;
}

function seriesLabel(row: Pick<HeroProductTrendSeries, "brand" | "model"> | null | undefined): string {
  if (!row) return "-";
  return row.brand ? `${row.brand} ${row.model}` : row.model;
}

function countryTotals(items: HeroProductDeckResponse["pages"]["topDistribution"]["distribution"]["items"]) {
  const totals = new Map<string, number>();
  items.forEach((item) => {
    item.countries.forEach((country) => {
      totals.set(country.country, (totals.get(country.country) ?? 0) + Math.max(0, country.sales));
    });
  });
  return Array.from(totals.entries()).map(([country, sales]) => ({ country, sales }));
}

function distributionFourWheelShare(item: HeroProductDeckResponse["pages"]["topDistribution"]["distribution"]["items"][number]): number {
  const fourWheelVolume = item.countries.reduce((sum, country) => sum + Math.max(0, Number(country.driveMix["4x4"] ?? 0) || 0), 0);
  return item.totalSales > 0 ? clampShare(fourWheelVolume / item.totalSales) : 0;
}

function buildBenchmarkInsight(rows: HeroProductModelRow[], productRows: HeroProductModelRow[], priceSource: HeroProductPriceSource, showChannel: boolean): HeroProductInsight {
  const salesLeader = rows[0] ?? null;
  const rangeLeader = maxBy(productRows, (row) => maxSpecValue(row.specs.rangeKm));
  const batteryLeader = maxBy(productRows, (row) => maxSpecValue(row.specs.batteryKwh));
  const accelerationLeader = minBy(productRows, (row) => minSpecValue(row.specs.accelerationSec));
  const priceLeader = minBy(productRows, (row) => selectedPrice(row, priceSource).value);
  const businessLeader = maxBy(rows, businessShare);
  const privateLeader = maxBy(rows, (row) => channelShare(row, "Private"));
  const fourWheelLeader = maxBy(rows, fourWheelShare);
  const missingSpecs = [
    rangeLeader ? null : "续航",
    accelerationLeader ? null : "零百",
    priceLeader ? null : `${priceSource.toUpperCase()}价格`,
  ].filter((value): value is string => Boolean(value));
  const benchmarkHeadline = missingSpecs.length > 0
    ? `${batteryLeader?.model ?? salesLeader?.model ?? "固定车型"}已有电池/销量基准，${missingSpecs.join("、")}仍待补齐；先用现有规格表定位对标缺口。`
    : `${rangeLeader?.model ?? "续航领先车型"}续航领先，${accelerationLeader?.model ?? "性能领先车型"}零百最快；规格表用于定位动总、能耗和价格短板。`;
  const rangeOrBatteryCard = rangeLeader
    ? { label: "Range", value: modelLabel(rangeLeader), detail: `${specDisplay(rangeLeader.specs.rangeKm, "km")}`, tone: "positive" as const }
    : { label: "Battery", value: modelLabel(batteryLeader), detail: batteryLeader ? `${specDisplay(batteryLeader.specs.batteryKwh, "kWh")}` : "待补齐", tone: batteryLeader ? "positive" as const : "new" as const };

  if (showChannel) {
    return {
      eyebrow: "Channel judgement",
      headline: `${privateLeader?.model ?? "Private主力"}零售占比${formatWholePercent(privateLeader ? channelShare(privateLeader, "Private") : null)}，${businessLeader?.model ?? "Business主力"}商务占比${formatWholePercent(businessLeader ? businessShare(businessLeader) : null)}。`,
      summary: `左侧保留 TOP20 销量排序和渠道结构，右侧用固定车型规格表复核续航、性能、充电和 ${priceSource.toUpperCase()} 价格，判断渠道放量是否有产品力支撑。`,
      cards: [
        { label: "Private Leader", value: modelLabel(privateLeader), detail: `Private ${formatWholePercent(privateLeader ? channelShare(privateLeader, "Private") : null)}`, tone: "positive" },
        { label: "Business Leader", value: modelLabel(businessLeader), detail: `Business ${formatWholePercent(businessLeader ? businessShare(businessLeader) : null)}` },
        { label: "4WD Exposure", value: modelLabel(fourWheelLeader), detail: `4WD ${formatWholePercent(fourWheelLeader ? fourWheelShare(fourWheelLeader) : null)}`, tone: "new" },
        { label: "Sales Leader", value: modelLabel(salesLeader), detail: `${formatNumber(salesLeader?.sales)} units · MS ${formatPercent(salesLeader?.sharePct)}` },
      ],
    };
  }

  return {
    eyebrow: "Powertrain benchmark",
    headline: benchmarkHeadline,
    summary: `左侧复刻 TOP20 销量基准，右侧固定车型下沉到续航、电池、电耗、零百、充电和 ${priceSource.toUpperCase()} 价格，先看结论再看单项差距。`,
    cards: [
      rangeOrBatteryCard,
      { label: "Acceleration", value: modelLabel(accelerationLeader), detail: accelerationLeader ? `${specDisplay(accelerationLeader.specs.accelerationSec, "s")}` : "待补齐", tone: accelerationLeader ? "positive" : "new" },
      { label: "Price Floor", value: modelLabel(priceLeader), detail: priceLeader ? (formatPrice(selectedPrice(priceLeader, priceSource).value, selectedPrice(priceLeader, priceSource).currency) || "空白") : "空白" },
      { label: "Sales Leader", value: modelLabel(salesLeader), detail: `${formatNumber(salesLeader?.sales)} units · MS ${formatPercent(salesLeader?.sharePct)}` },
    ],
  };
}

function buildTrendInsight(page: HeroProductDeckResponse["pages"]["topTrend"], priceSource: HeroProductPriceSource, variant: "top" | "hero"): HeroProductInsight {
  const endLeader = maxBy(page.series, lastTrendVolume);
  const risingSeries = maxBy(page.series, trendDelta);
  const fallingSeries = minBy(page.series, trendDelta);
  const priceLeader = minBy(page.priceRows, (row) => selectedPricePayload(row.price, priceSource).value);
  const missingPriceCount = page.priceRows.filter((row) => {
    const price = selectedPricePayload(row.price, priceSource);
    return price.value == null || price.status === "missing";
  }).length;
  const priceLeaderValue = priceLeader ? selectedPricePayload(priceLeader.price, priceSource) : null;
  const risingDelta = risingSeries ? trendDelta(risingSeries) : null;
  const fallingDelta = fallingSeries ? trendDelta(fallingSeries) : null;
  const headline = variant === "hero"
    ? `${seriesLabel(risingSeries)}固定车型拉升最明显，${seriesLabel(fallingSeries)}承压；价格变化需要和销量曲线同步解释。`
    : `${seriesLabel(endLeader)}当前领先，${seriesLabel(risingSeries)}近窗口增量最大；政策退坡后重点看渠道和价格承接。`;

  return {
    eyebrow: variant === "hero" ? "Hero model trend" : "Top model trend",
    headline,
    summary: `上方用 Business / Private 渠道条解释结构差异，中间趋势线保留 TOP1 share 参照，右侧价格列可在 MSRP 抓取价和 JATO 价格之间切换。`,
    cards: [
      { label: "Current Leader", value: seriesLabel(endLeader), detail: `${formatNumber(endLeader ? lastTrendVolume(endLeader) : null)} units`, tone: "positive" },
      { label: "Biggest Rise", value: seriesLabel(risingSeries), detail: `${formatSignedNumber(risingDelta)} units`, tone: "positive" },
      { label: "Biggest Drop", value: seriesLabel(fallingSeries), detail: `${formatSignedNumber(fallingDelta)} units`, tone: "negative" },
      { label: "Price Coverage", value: missingPriceCount > 0 ? `${missingPriceCount} 空白` : (priceLeader?.model ?? "-"), detail: missingPriceCount > 0 ? "等待 Editor 补录" : `最低 ${priceLeaderValue ? formatPrice(priceLeaderValue.value, priceLeaderValue.currency) : "-"}`, tone: missingPriceCount > 0 ? "new" : "neutral" },
    ],
  };
}

function buildDistributionInsight(page: HeroProductDeckResponse["pages"]["topDistribution"], variant: "top" | "hero"): HeroProductInsight {
  const items = page.distribution.items;
  const topCountry = maxBy(countryTotals(items), (country) => country.sales);
  const salesLeader = maxBy(items, (item) => item.totalSales);
  const coverageLeader = maxBy(items, (item) => item.countries.filter((country) => country.sales > 0).length);
  const fourWheelLeader = maxBy(items, distributionFourWheelShare);
  const headline = variant === "hero"
    ? `${topCountry?.country ?? "核心市场"}是固定车型放量核心，${salesLeader?.model ?? "主力车型"}总量最高；同级判断要看国家覆盖和驱动结构。`
    : `${topCountry?.country ?? "核心市场"}是 TOP 车型核心市场，${salesLeader?.model ?? "主力车型"}总量最高；优先识别 Western Europe 的前驱/4WD机会。`;

  return {
    eyebrow: variant === "hero" ? "Hero market split" : "Top market split",
    headline,
    summary: "每列对应一个车型，每行下沉到国家销量，并用 front / rear / 4x4 堆叠条解释市场分布，避免只看全市场均值。",
    cards: [
      { label: "Core Market", value: topCountry?.country ?? "-", detail: `${formatNumber(topCountry?.sales)} units`, tone: "positive" },
      { label: "Volume Leader", value: salesLeader?.model ?? "-", detail: `${formatNumber(salesLeader?.totalSales)} units`, tone: "positive" },
      { label: "Coverage", value: coverageLeader?.model ?? "-", detail: `${coverageLeader?.countries.filter((country) => country.sales > 0).length ?? 0} markets` },
      { label: "4WD Mix", value: fourWheelLeader?.model ?? "-", detail: `4WD ${formatWholePercent(fourWheelLeader ? distributionFourWheelShare(fourWheelLeader) : null)}`, tone: "new" },
    ],
  };
}

export function HeroProductAnalysisView({ onSwitchToTransfer }: { onSwitchToTransfer: () => void }) {
  const { user } = useAuth();
  const [searchParams, setSearchParams] = useSearchParams();
  const [activePage, setActivePage] = useState<HeroProductPageKey>(() => (
    isHeroProductPageKey(searchParams.get("heroPage")) ? searchParams.get("heroPage") as HeroProductPageKey : "benchmark"
  ));
  const [priceCountry, setPriceCountry] = useState(() => searchParams.get("priceCountry") || "");
  const [period, setPeriod] = useState(() => searchParams.get("period") || "");
  const [salesMode, setSalesMode] = useState<HeroProductSalesMode>(() => (
    isSalesMode(searchParams.get("salesMode")) ? searchParams.get("salesMode") as HeroProductSalesMode : "ytd"
  ));
  const [priceSource, setPriceSource] = useState<HeroProductPriceSource>(() => (
    isPriceSource(searchParams.get("priceSource")) ? searchParams.get("priceSource") as HeroProductPriceSource : "msrp"
  ));
  const [scopeMode, setScopeMode] = useState<HeroProductScopeMode>(() => (
    searchParams.get("scope") === "price" ? "price" : "all"
  ));
  const [distributionLayout, setDistributionLayout] = useState<HeroProductDistributionLayout>(() => (
    isDistributionLayout(searchParams.get("distributionLayout")) ? searchParams.get("distributionLayout") as HeroProductDistributionLayout : "aligned"
  ));
  const [topModelText, setTopModelText] = useState(() => searchParams.get("topModels") || "");
  const [heroModelText, setHeroModelText] = useState(() => searchParams.get("heroModels") || "");
  const [countryLimitText, setCountryLimitText] = useState(() => {
    const initialLimit = parseCountryLimit(searchParams.get("countryLimit") || "");
    return initialLimit > 0 ? String(initialLimit) : "";
  });
  const [deck, setDeck] = useState<HeroProductDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [reloadToken, setReloadToken] = useState(0);
  const [controlOpen, setControlOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportError, setExportError] = useState("");
  const [slideEditMode, setSlideEditMode] = useState(false);
  const [saveMessage, setSaveMessage] = useState("");
  const [savingPriceKey, setSavingPriceKey] = useState("");
  const [savingSpecKey, setSavingSpecKey] = useState("");
  const [priceDrafts, setPriceDrafts] = useState<Record<string, string>>({});
  const [specColumns, setSpecColumns] = useState<HeroProductSpecColumnKey[]>(DEFAULT_HERO_PRODUCT_SPEC_COLUMNS);
  const [customSpecColumns, setCustomSpecColumns] = useState<HeroProductSpecColumnOption[]>(readStoredCustomSpecColumns);
  const [hiddenCustomSpecColumns, setHiddenCustomSpecColumns] = useState<HeroProductCustomSpecColumnKey[]>([]);
  const [customColumnDraft, setCustomColumnDraft] = useState("");
  const [exportSettings, setExportSettings] = useState<ExportSettings>(DEFAULT_HERO_EXPORT);
  const [slideLayouts, setSlideLayouts] = useState<Record<HeroProductPageKey, SlideLayoutSettings>>(
    () => readStoredSlideLayouts("hero-product-analysis", DEFAULT_HERO_LAYOUTS),
  );
  const slideRef = useRef<HTMLDivElement | null>(null);
  const canEdit = canEditPrices(user?.role);
  const activeTab = HERO_PAGE_ITEMS.find((item) => item.key === activePage) ?? HERO_PAGE_ITEMS[0];
  const countryLimit = parseCountryLimit(countryLimitText);
  const currentPriceCountry = priceCountry || deck?.metadata.selectedPriceCountry.value || "";
  const selectedCountries = scopeMode === "price" && currentPriceCountry
    ? [countryValue(deck?.metadata.availableCountries ?? [], currentPriceCountry)]
    : [];
  const discoveredCustomSpecColumns = useMemo<HeroProductSpecColumnOption[]>(() => {
    const byKey = new Map<HeroProductCustomSpecColumnKey, HeroProductSpecColumnOption>();
    const storedLabels = new Map(customSpecColumns.filter((column) => isCustomSpecColumnKey(column.key)).map((column) => [column.key, column.label]));
    const rowsToInspect = [
      ...(deck?.pages.benchmark.productRows ?? []),
      ...(deck?.pages.benchmarkWithChannel.productRows ?? []),
    ];
    for (const row of rowsToInspect) {
      for (const key of Object.keys(row.specs.overrides ?? {})) {
        if (!isCustomSpecColumnKey(key) || hiddenCustomSpecColumns.includes(key)) continue;
        byKey.set(key, {
          key,
          label: sanitizeCustomSpecColumnLabel(storedLabels.get(key) ?? "") || customSpecColumnLabelFromKey(key),
          isCustom: true,
        });
      }
    }
    return Array.from(byKey.values());
  }, [customSpecColumns, deck, hiddenCustomSpecColumns]);
  const customSpecColumnOptions = useMemo<HeroProductSpecColumnOption[]>(() => {
    const byKey = new Map<HeroProductSpecColumnKey, HeroProductSpecColumnOption>();
    for (const column of customSpecColumns) {
      if (isCustomSpecColumnKey(column.key) && !hiddenCustomSpecColumns.includes(column.key)) {
        byKey.set(column.key, { ...column, label: sanitizeCustomSpecColumnLabel(column.label) || customSpecColumnLabelFromKey(column.key), isCustom: true });
      }
    }
    for (const column of discoveredCustomSpecColumns) {
      if (!byKey.has(column.key)) byKey.set(column.key, column);
    }
    return Array.from(byKey.values());
  }, [customSpecColumns, discoveredCustomSpecColumns, hiddenCustomSpecColumns]);
  const specColumnOptions = useMemo<HeroProductSpecColumnOption[]>(() => [
    ...HERO_PRODUCT_SPEC_COLUMN_OPTIONS,
    ...customSpecColumnOptions,
  ], [customSpecColumnOptions]);
  const allSpecColumnKeys = useMemo<HeroProductSpecColumnKey[]>(() => specColumnOptions.map((column) => column.key), [specColumnOptions]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError("");
    api.heroProductDeck({
      countries: selectedCountries,
      price_country: priceCountry || undefined,
      target_period: period || undefined,
      sales_mode: salesMode,
      segment: "SUV A0",
      fuel_type: "BEV",
      price_source: priceSource,
      top_n: 10,
      ranking_limit: 20,
      country_limit: countryLimit,
      trend_window_months: 16,
      top_models: parseModelList(topModelText),
      hero_models: parseModelList(heroModelText),
    })
      .then((response) => {
        if (cancelled) return;
        setDeck(response);
        if (!priceCountry) setPriceCountry(response.metadata.selectedPriceCountry.value);
        if (!period) setPeriod(response.metadata.resolvedPeriod);
      })
      .catch((reason: Error) => {
        if (cancelled) return;
        if (reason.name !== "AbortError") setError(reason.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [countryLimit, heroModelText, period, priceCountry, priceSource, reloadToken, salesMode, scopeMode, topModelText]);

  useEffect(() => {
    const params = new URLSearchParams();
    params.set("mode", "hero-product");
    if (activePage !== "benchmark") params.set("heroPage", activePage);
    if (priceCountry) params.set("priceCountry", priceCountry);
    if (period) params.set("period", period);
    if (salesMode !== "ytd") params.set("salesMode", salesMode);
    if (priceSource !== "msrp") params.set("priceSource", priceSource);
    if (scopeMode !== "all") params.set("scope", scopeMode);
    if (distributionLayout !== "aligned") params.set("distributionLayout", distributionLayout);
    if (countryLimit > 0) params.set("countryLimit", String(countryLimit));
    if (topModelText.trim()) params.set("topModels", topModelText.trim());
    if (heroModelText.trim()) params.set("heroModels", heroModelText.trim());
    if (params.toString() !== searchParams.toString()) {
      setSearchParams(params, { replace: true });
    }
  }, [activePage, countryLimit, distributionLayout, heroModelText, period, priceCountry, priceSource, salesMode, scopeMode, searchParams, setSearchParams, topModelText]);

  useEffect(() => {
    writeStoredSlideLayouts("hero-product-analysis", slideLayouts);
  }, [slideLayouts]);

  useEffect(() => {
    writeStoredCustomSpecColumns(customSpecColumns);
  }, [customSpecColumns]);

  const previewWidth = normalizeExportDimension(exportSettings.exportWidth, 1920, 400);
  const previewHeight = normalizeExportDimension(exportSettings.exportHeight, 1080, 300);
  const slidePreview = useFixedCanvasPreview({
    width: previewWidth,
    height: previewHeight,
    exporting: exportingSlide,
  });
  const activeLayout = slideLayouts[activePage] ?? DEFAULT_SLIDE_LAYOUT;
  const slideFrameStyle: CSSProperties = {
    ...slidePreview.frameStyle,
    background: exportSettings.paperBg,
    "--market-scan-slide-pad-x": `${activeLayout.paddingX}px`,
    "--market-scan-slide-pad-y": `${activeLayout.paddingY}px`,
    "--market-scan-slide-frame-gap": `${activeLayout.frameGap}px`,
    "--market-scan-slide-head-gap": `${activeLayout.headGap}px`,
    "--market-scan-slide-body-gap": `${activeLayout.bodyGap}px`,
    "--market-scan-slide-content-gap": `${activeLayout.contentGap}px`,
  } as CSSProperties;
  const editableRows = useMemo(
    () => getRowsForPage(deck, activePage).slice(0, 12),
    [activePage, deck],
  );

  function setActiveLayoutPatch(patch: Partial<SlideLayoutSettings>): void {
    setSlideLayouts((current) => ({
      ...current,
      [activePage]: updateSlideLayout(current[activePage] ?? DEFAULT_SLIDE_LAYOUT, patch),
    }));
  }

  function resetActiveLayout(): void {
    setSlideLayouts((current) => ({
      ...current,
      [activePage]: DEFAULT_SLIDE_LAYOUT,
    }));
  }

  function handleDrawerOpen(open: boolean): void {
    setControlOpen(open);
    if (open) setExportOpen(false);
  }

  function handleExportOpen(open: boolean): void {
    setExportOpen(open);
    if (open) setControlOpen(false);
  }

  function toggleSpecColumn(column: HeroProductSpecColumnKey): void {
    setSpecColumns((current) => {
      if (current.includes(column)) {
        const next = current.filter((item) => item !== column);
        return next.length > 0 ? next : current;
      }
      return allSpecColumnKeys.filter((item) => item === column || current.includes(item));
    });
  }

  function handleAddCustomSpecColumn(): void {
    const column = makeCustomSpecColumn(customColumnDraft, allSpecColumnKeys);
    if (!column || !isCustomSpecColumnKey(column.key)) return;
    setCustomSpecColumns((current) => [...current, column]);
    setHiddenCustomSpecColumns((current) => current.filter((key) => key !== column.key));
    setSpecColumns((current) => (current.includes(column.key) ? current : [...current, column.key]));
    setCustomColumnDraft("");
  }

  function handleCustomSpecColumnLabelChange(column: HeroProductSpecColumnOption, label: string): void {
    if (!isCustomSpecColumnKey(column.key)) return;
    const nextLabel = label.slice(0, 32);
    setCustomSpecColumns((current) => {
      const existing = current.find((item) => item.key === column.key);
      if (existing) {
        return current.map((item) => (item.key === column.key ? { ...item, label: nextLabel, isCustom: true } : item));
      }
      return [...current, { key: column.key, label: nextLabel, isCustom: true }];
    });
  }

  function handleRemoveCustomSpecColumn(column: HeroProductSpecColumnOption): void {
    if (!isCustomSpecColumnKey(column.key)) return;
    const customKey = column.key;
    setSpecColumns((current) => current.filter((key) => key !== customKey));
    setCustomSpecColumns((current) => current.filter((item) => item.key !== customKey));
    setHiddenCustomSpecColumns((current) => (current.includes(customKey) ? current : [...current, customKey]));
  }

  async function handleExportSlide(): Promise<void> {
    if (!slideRef.current || !deck) return;
    try {
      setExportError("");
      setExportingSlide(true);
      if ("fonts" in document) await document.fonts.ready;
      await new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
      const { toPng } = await import("html-to-image");
      const dataUrl = await toPng(slideRef.current, {
        cacheBust: true,
        pixelRatio: 2,
        backgroundColor: exportSettings.paperBg || "#ffffff",
        width: previewWidth,
        height: previewHeight,
        canvasWidth: previewWidth,
        canvasHeight: previewHeight,
        style: { width: `${previewWidth}px`, height: `${previewHeight}px` },
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = [
        "hero-product",
        sanitizeFileNameSegment(deck.metadata.selectedFuelType),
        sanitizeFileNameSegment(deck.metadata.selectedSegment),
        deck.metadata.resolvedPeriod,
        activePage,
      ].join("-") + ".png";
      link.click();
    } catch (reason) {
      setExportError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setExportingSlide(false);
    }
  }

  function handlePriceDraftChange(row: HeroProductPriceEditableRow, value: string): void {
    const key = rowKey(row, priceSource);
    setPriceDrafts((current) => ({ ...current, [key]: value }));
  }

  async function handleSavePriceValue(row: HeroProductPriceEditableRow, valueText: string): Promise<void> {
    if (!deck || !canEdit) return;
    const key = rowKey(row, priceSource);
    const trimmed = valueText.trim();
    const nextValue = trimmed ? Number(trimmed) : null;
    if (trimmed && (!Number.isFinite(nextValue) || Number(nextValue) < 0)) {
      setSaveMessage("价格必须是大于等于 0 的数字。");
      throw new Error("价格必须是大于等于 0 的数字。");
    }
    const current = selectedPricePayload(row.price, priceSource);
    try {
      setSavingPriceKey(key);
      setSaveMessage("");
      await api.patchHeroProductPrice({
        country: deck.metadata.selectedPriceCountry.value,
        price_period: deck.metadata.resolvedPeriod,
        price_source: priceSource,
        brand: sourceBrand(row),
        model: sourceModel(row),
        price_value: nextValue,
        currency: current.currency || "EUR",
      });
      setPriceDrafts((currentDrafts) => {
        const next = { ...currentDrafts };
        delete next[key];
        return next;
      });
      setSaveMessage(nextValue === null ? "价格补录已清除。" : "价格已保存，所有人刷新后可见。");
      setReloadToken((value) => value + 1);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : String(reason);
      setSaveMessage(message);
      throw reason;
    } finally {
      setSavingPriceKey("");
    }
  }

  async function handleSavePrice(row: HeroProductPriceEditableRow): Promise<void> {
    const current = selectedPricePayload(row.price, priceSource);
    const draft = priceDrafts[rowKey(row, priceSource)] ?? (current.value == null ? "" : String(Math.round(current.value)));
    await handleSavePriceValue(row, draft);
  }

  async function handleSaveSpecValue(row: HeroProductPriceEditableRow, fieldName: HeroProductSpecEditableField | HeroProductCustomSpecColumnKey, valueText: string): Promise<void> {
    if (!deck || !canEdit) return;
    const key = `${sourceBrand(row)}:${sourceModel(row)}:${fieldName}`;
    const trimmed = valueText.trim();
    try {
      setSavingSpecKey(key);
      setSaveMessage("");
      await api.patchHeroProductSpec({
        country: deck.metadata.selectedPriceCountry.value,
        price_period: deck.metadata.resolvedPeriod,
        brand: sourceBrand(row),
        model: sourceModel(row),
        field_name: fieldName,
        field_value: trimmed || null,
      });
      setSaveMessage(trimmed ? "规格已保存，所有人刷新后可见。" : "规格补录已清除。");
      setReloadToken((value) => value + 1);
    } catch (reason) {
      const message = reason instanceof Error ? reason.message : String(reason);
      setSaveMessage(message);
      throw reason;
    } finally {
      setSavingSpecKey("");
    }
  }

  const slideTitle = deck?.pages[activePage].title ?? activeTab.label;
  const narrative = deck
    ? `${deck.metadata.labels.marketScopeLabel} · ${deck.metadata.labels.periodLabel} · 价格市场 ${deck.metadata.selectedPriceCountry.label}`
    : "按 BEV / SUV A0 生成 Hero Product 六页分析。";
  const priceEditor: HeroProductPriceEditorBinding = {
    canEdit,
    priceDrafts,
    savingPriceKey,
    savingSpecKey,
    saveMessage,
    onDraftChange: handlePriceDraftChange,
    onSavePrice: (row) => { void handleSavePrice(row); },
    onSavePriceValue: handleSavePriceValue,
    onSaveSpecValue: handleSaveSpecValue,
  };
  const activeSalesModeLabel = salesModeLabel(salesMode);

  return (
    <div className="market-scan-shell hero-product-shell">
      <div className="market-scan-main">
        <section className="header-card dashboard-hero market-scan-hero hero-product-hero">
          <div className="dashboard-hero-head">
            <div className="dashboard-hero-copy market-scan-hero-copy">
              <span className="page-kicker">Advanced Analysis</span>
              <h1>Hero Product 分析</h1>
              <p>从 SUV A0 BEV 动总下钻到固定车型，复刻六页产品、趋势、价格和市场分布分析。</p>
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip">Mode Hero Product</span>
                <span className="market-scan-hero-chip">{deck?.metadata.labels.marketScopeLabel ?? "全部市场"}</span>
                <span className="market-scan-hero-chip">Price {priceSource.toUpperCase()}</span>
                <button type="button" className="btn btn-ghost btn-sm hero-product-mode-btn" onClick={onSwitchToTransfer}>
                  Share Transfer
                </button>
              </div>
            </div>
          </div>
        </section>

        <DeckFloatingDrawer
          open={controlOpen}
          onOpenChange={handleDrawerOpen}
          triggerPrimary="Hero Product 控制"
          triggerSecondaryOpen="收起"
          triggerSecondaryClosed="打开"
          eyebrow="Controls"
          title="分析与价格编辑"
          ariaLabel="Hero Product controls"
          footer={(
            <>
              <span className="market-scan-toolbar-chip">{deck?.metadata.selectedFuelType ?? "BEV"}</span>
              <span className="market-scan-toolbar-chip">{deck?.metadata.selectedSegment ?? "SUV A0"}</span>
              <span className="market-scan-toolbar-chip">价格 {priceSource.toUpperCase()}</span>
              <span className="market-scan-toolbar-chip">{canEdit ? "Editor" : "Read only"}</span>
            </>
          )}
        >
          <div className="deck-panel-grid">
            <label className="market-scan-field">
              <span>Price Market</span>
              <select value={currentPriceCountry} onChange={(event) => setPriceCountry(event.target.value)}>
                {(deck?.metadata.availableCountries ?? []).map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Period</span>
              <select value={period || deck?.metadata.resolvedPeriod || ""} onChange={(event) => setPeriod(event.target.value)}>
                {(deck?.metadata.availablePeriods ?? []).map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
            <div className="market-scan-field">
              <span>Sales Mode</span>
              <div className="btn-group">
                {SALES_MODES.map((mode) => (
                  <button key={mode.value} type="button" className={`btn btn-sm ${salesMode === mode.value ? "btn-primary" : "btn-ghost"}`} onClick={() => setSalesMode(mode.value)}>
                    {mode.label}
                  </button>
                ))}
              </div>
            </div>
            <div className="market-scan-field">
              <span>Price Source</span>
              <div className="btn-group">
                {PRICE_SOURCES.map((source) => (
                  <button key={source.value} type="button" className={`btn btn-sm ${priceSource === source.value ? "btn-primary" : "btn-ghost"}`} onClick={() => setPriceSource(source.value)}>
                    {source.label}
                  </button>
                ))}
              </div>
            </div>
            <div className="market-scan-field deck-panel-grid__wide">
              <span>Market Layout</span>
              <div className="btn-group">
                {DISTRIBUTION_LAYOUTS.map((layout) => (
                  <button key={layout.value} type="button" className={`btn btn-sm ${distributionLayout === layout.value ? "btn-primary" : "btn-ghost"}`} onClick={() => setDistributionLayout(layout.value)}>
                    {layout.label}
                  </button>
                ))}
              </div>
              <small className="hero-product-control-hint">国家对齐 = 第 4/6 页按第一列车型国家顺序对齐；缺失国家保留空行。</small>
            </div>
            <div className="market-scan-field deck-panel-grid__wide hero-product-spec-column-control">
              <div className="hero-product-column-control-head">
                <span>Spec Columns</span>
                <div className="btn-group">
                  <button type="button" className="btn btn-ghost btn-sm" onClick={() => setSpecColumns(DEFAULT_HERO_PRODUCT_SPEC_COLUMNS)}>默认</button>
                  <button type="button" className="btn btn-ghost btn-sm" onClick={() => setSpecColumns(allSpecColumnKeys)}>全部列</button>
                </div>
              </div>
              <div className="hero-product-column-toggle-grid">
                {specColumnOptions.map((column) => (
                  <label key={column.key} className={`hero-product-column-toggle${specColumns.includes(column.key) ? " is-active" : ""}`}>
                    <input
                      type="checkbox"
                      value={column.key}
                      checked={specColumns.includes(column.key)}
                      onChange={() => toggleSpecColumn(column.key)}
                    />
                    <span>{column.label}</span>
                  </label>
                ))}
              </div>
              <div className="hero-product-custom-column-add">
                <input
                  value={customColumnDraft}
                  maxLength={32}
                  placeholder="新增自定义列名"
                  onChange={(event) => setCustomColumnDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      handleAddCustomSpecColumn();
                    }
                  }}
                />
                <button type="button" className="btn btn-secondary btn-sm" onClick={handleAddCustomSpecColumn}>新增列</button>
              </div>
              {customSpecColumnOptions.length > 0 ? (
                <div className="hero-product-custom-column-list" aria-label="Custom spec columns">
                  {customSpecColumnOptions.map((column) => (
                    <div key={column.key} className="hero-product-custom-column-edit">
                      <input
                        value={column.label}
                        maxLength={32}
                        aria-label={`${column.label} 自定义列名`}
                        onChange={(event) => handleCustomSpecColumnLabelChange(column, event.target.value)}
                      />
                      <button type="button" className="btn btn-ghost btn-sm" onClick={() => handleRemoveCustomSpecColumn(column)}>移除</button>
                    </div>
                  ))}
                </div>
              ) : null}
              <small className="hero-product-control-hint">系统指标列跟随 Sales Mode 切换口径；自定义列可直接在表格内编辑保存。</small>
            </div>
            <div className="market-scan-field deck-panel-grid__wide">
              <span>Market Scope</span>
              <div className="btn-group">
                <button type="button" className={`btn btn-sm ${scopeMode === "all" ? "btn-primary" : "btn-ghost"}`} onClick={() => setScopeMode("all")}>全部市场</button>
                <button type="button" className={`btn btn-sm ${scopeMode === "price" ? "btn-primary" : "btn-ghost"}`} onClick={() => setScopeMode("price")}>只看价格市场</button>
              </div>
            </div>
            <label className="market-scan-field deck-panel-grid__wide">
              <span>Countries per model</span>
              <input
                type="number"
                min={0}
                max={80}
                step={1}
                inputMode="numeric"
                value={countryLimitText}
                placeholder="全部国家"
                onChange={(event) => setCountryLimitText(event.target.value)}
              />
              <small className="hero-product-control-hint">空白或 0 = 每个车型展示全部有销量国家；输入数字则按销量截取前 N 个国家。</small>
            </label>
            <label className="market-scan-field deck-panel-grid__wide">
              <span>Top Models</span>
              <input value={topModelText} onChange={(event) => setTopModelText(event.target.value)} placeholder="可选，逗号分隔车型名" />
            </label>
            <label className="market-scan-field deck-panel-grid__wide">
              <span>Hero Models</span>
              <input value={heroModelText} onChange={(event) => setHeroModelText(event.target.value)} placeholder="可选，逗号分隔固定车型" />
            </label>
            <div className="market-scan-field market-scan-field-actions deck-panel-grid__wide">
              <span>Deck</span>
              <div className="btn-group">
                <button type="button" className="btn btn-secondary btn-sm" onClick={() => setReloadToken((value) => value + 1)}>Refresh</button>
                <button type="button" className="btn btn-ghost btn-sm" onClick={() => { setTopModelText(""); setHeroModelText(""); setCountryLimitText(""); setScopeMode("all"); setSalesMode("ytd"); setPriceSource("msrp"); setDistributionLayout("aligned"); setSpecColumns(DEFAULT_HERO_PRODUCT_SPEC_COLUMNS); }}>Reset</button>
              </div>
            </div>
            <div className="hero-product-price-editor deck-panel-grid__wide">
              <div className="hero-product-price-editor-head">
                <strong>价格补录</strong>
                <span>{canEdit ? "Editor 及以上可保存" : "当前只读"}</span>
              </div>
              {editableRows.map((row) => {
                return (
                  <div key={rowKey(row, priceSource)} className="hero-product-price-edit-row">
                    <span>{row.model}</span>
                    <InlinePriceEditor row={row} priceSource={priceSource} binding={priceEditor} variant="drawer" />
                  </div>
                );
              })}
              {saveMessage ? <small className="hero-product-save-message">{saveMessage}</small> : null}
            </div>
          </div>
        </DeckFloatingDrawer>

        <DeckSubpageNav
          items={HERO_PAGE_ITEMS}
          activeKey={activePage}
          onSelect={setActivePage}
          ariaLabel="Hero Product pages"
          tabsClassName="market-scan-tab-strip hero-product-tab-strip"
        />

        {exportError ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>PNG 导出失败</strong>
            <p>{exportError}</p>
          </section>
        ) : null}

        {!deck ? (
          <HeroProductDeckFallback
            loading={loading}
            error={error}
            onRetry={() => setReloadToken((value) => value + 1)}
          />
        ) : (
          <div className="market-scan-content" aria-busy={loading}>
            <div className="market-scan-slide-shell-actions">
              <div className="market-scan-slide-shell-meta">
                <span className={`market-scan-toolbar-chip slide-edit-shell-chip${slideEditMode ? " is-active" : ""}`}>
                  {slideEditMode ? "Edit Mode" : "Preview"}
                </span>
                <span className="market-scan-slide-shell-note">当前页为固定 16:9 画布，导出复用 MarketScan PNG 流程。</span>
              </div>
              <button type="button" className={`btn btn-sm ${slideEditMode ? "btn-secondary" : "btn-primary"}`} onClick={() => { setSlideEditMode((value) => !value); setExportOpen(true); setControlOpen(false); }}>
                {slideEditMode ? "返回 Preview" : "一键 Edit"}
              </button>
            </div>
            <div ref={slidePreview.shellRef} className="market-scan-slide-shell">
              <div className="market-scan-slide-scale-box" style={slidePreview.scaleBoxStyle}>
                <div
                  ref={slideRef}
                  className={`market-scan-slide-frame hero-product-slide-frame hero-product-slide-frame--${activePage}${exportingSlide ? " is-exporting" : ""}${slideEditMode && !exportingSlide ? " is-editing" : ""}`}
                  style={slideFrameStyle}
                >
                  <header className="market-scan-slide-head hero-product-slide-head">
                    <div className="market-scan-slide-copy">
                      <span className="market-scan-slide-kicker">{activeTab.code} {activeTab.label}</span>
                      <h2>{slideTitle}</h2>
                      <p>{narrative}</p>
                    </div>
                    <div className="market-scan-slide-meta">
                      <span className="market-scan-slide-tag">{deck.metadata.selectedFuelType}</span>
                      <span className="market-scan-slide-tag">{deck.metadata.selectedSegment}</span>
                      <span className="market-scan-slide-tag">{activeSalesModeLabel}</span>
                      <span className="market-scan-slide-tag">{deck.metadata.labels.currentMonthShort}</span>
                      <span className="market-scan-slide-tag">{priceSource.toUpperCase()}</span>
                      {loading ? <span className="market-scan-slide-tag">Updating</span> : null}
                    </div>
                  </header>
                  <div className="market-scan-slide-body hero-product-slide-body">
                    <div className="market-scan-slide-content hero-product-slide-content">
                      <HeroProductPageContent deck={deck} pageKey={activePage} priceSource={priceSource} priceEditor={priceEditor} salesMode={salesMode} distributionLayout={distributionLayout} specColumns={specColumns} customColumns={customSpecColumnOptions} />
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
        <DeckExportDrawer
          open={exportOpen}
          onOpenChange={handleExportOpen}
          triggerPrimary="导出当前页 / 导出图设置"
          triggerSecondaryOpen="收起"
          triggerSecondaryClosed="展开"
          eyebrow="Export"
          title="导出与版式"
          ariaLabel="Hero Product export settings"
          footer={(
            <>
              <span className="market-scan-toolbar-chip">{previewWidth} x {previewHeight}</span>
              <span className="market-scan-toolbar-chip">{deck ? activeTab.label : "等待数据"}</span>
            </>
          )}
        >
          <button type="button" className={`btn btn-primary btn-liquid deck-export-primary${exportingSlide ? " is-loading" : ""}`} onClick={() => { void handleExportSlide(); }} disabled={!deck || exportingSlide}>
            <span className="btn-liquid-label">{exportingSlide ? "正在导出 PNG..." : deck ? "导出当前页 PNG" : "等待 deck 数据"}</span>
            {exportingSlide ? <span className="btn-liquid-loader" aria-hidden="true" /> : null}
          </button>
          <div className="deck-export-quick-grid">
            <button type="button" className={`btn btn-sm ${slideEditMode ? "btn-secondary" : "btn-ghost"}`} onClick={() => setSlideEditMode((value) => !value)}>
              {slideEditMode ? "退出 Edit" : "一键 Edit"}
            </button>
          </div>
          {slideEditMode ? (
            <SlideLayoutEditor value={activeLayout} onChange={setActiveLayoutPatch} onReset={resetActiveLayout} />
          ) : null}
          <ExportPanel value={exportSettings} onChange={setExportSettings} showExportButton={false} collapsible={false} />
        </DeckExportDrawer>
      </div>
    </div>
  );
}

function HeroProductDeckFallback({ loading, error, onRetry }: { loading: boolean; error: string; onRetry: () => void }) {
  return (
    <div className="market-scan-content hero-product-fallback">
      <div className="market-scan-slide-shell">
        <section className={`hero-product-fallback-frame${error ? " is-error" : ""}`}>
          <div className="hero-product-fallback-copy">
            <span>{loading ? "Loading" : error ? "Request failed" : "Waiting"}</span>
            <h2>{loading ? "正在生成 Hero Product deck" : error ? "Hero Product 数据没有加载成功" : "等待 Hero Product 数据"}</h2>
            <p>
              {loading
                ? "正在从 JATO 销量数据和价格源合成六页分析。"
                : error
                  ? "请确认当前前端代理连接的是包含 Hero Product API 的后端服务。"
                  : "页面结构已就绪，数据返回后会自动渲染 16:9 分析页。"}
            </p>
            {error ? <code>{error}</code> : null}
            {error ? (
              <button type="button" className="btn btn-secondary btn-sm" onClick={onRetry}>
                Retry
              </button>
            ) : null}
          </div>
          <div className="hero-product-fallback-grid" aria-hidden="true">
            {Array.from({ length: 6 }).map((_, index) => (
              <div key={index} className="hero-product-fallback-panel">
                <i />
                <i />
                <i />
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}

function HeroProductPanel({ eyebrow, title, subtitle, actions, className, children }: { eyebrow?: string; title: string; subtitle?: string; actions?: ReactNode; className?: string; children: ReactNode }) {
  return (
    <section className={`market-scan-panel${className ? ` ${className}` : ""}`}>
      <header className="market-scan-panel-head">
        <div>
          {eyebrow ? <span className="market-scan-panel-eyebrow">{eyebrow}</span> : null}
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        {actions ? <div className="market-scan-panel-actions">{actions}</div> : null}
      </header>
      <div className="market-scan-panel-body">{children}</div>
    </section>
  );
}

function HeroProductInsightCallout({ insight }: { insight: HeroProductInsight }) {
  return (
    <section className="market-scan-callout hero-product-insight-callout">
      <div className="market-scan-insight hero-product-insight-content">
        <div className="market-scan-insight-hero hero-product-insight-hero">
          <span className="market-scan-panel-eyebrow">{insight.eyebrow}</span>
          <strong className="market-scan-insight-headline">{insight.headline}</strong>
          <p className="market-scan-insight-summary">{insight.summary}</p>
        </div>
        <div className="market-scan-insight-grid hero-product-insight-grid">
          {insight.cards.map((card) => (
            <article key={card.label} className={`market-scan-insight-card ${toneClassName(card.tone)}`}>
              <span className="market-scan-insight-card-label">{card.label}</span>
              <strong className="market-scan-insight-card-value">{card.value}</strong>
              <span className="market-scan-insight-card-detail">{card.detail}</span>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

function HeroProductPageContent({ deck, pageKey, priceSource, priceEditor, salesMode, distributionLayout, specColumns, customColumns }: { deck: HeroProductDeckResponse; pageKey: HeroProductPageKey; priceSource: HeroProductPriceSource; priceEditor: HeroProductPriceEditorBinding; salesMode: HeroProductSalesMode; distributionLayout: HeroProductDistributionLayout; specColumns: HeroProductSpecColumnKey[]; customColumns: HeroProductSpecColumnOption[] }) {
  if (pageKey === "benchmark") {
    return <BenchmarkSlide rows={deck.pages.benchmark.ranking} productRows={deck.pages.benchmark.productRows} priceSource={priceSource} priceEditor={priceEditor} salesMode={salesMode} specColumns={specColumns} customColumns={customColumns} showChannel={false} />;
  }
  if (pageKey === "benchmarkWithChannel") {
    return <BenchmarkSlide rows={deck.pages.benchmarkWithChannel.ranking} productRows={deck.pages.benchmarkWithChannel.productRows} priceSource={priceSource} priceEditor={priceEditor} salesMode={salesMode} specColumns={specColumns} customColumns={customColumns} showChannel />;
  }
  if (pageKey === "topTrend") {
    return <TrendSlide page={deck.pages.topTrend} priceSource={priceSource} priceEditor={priceEditor} variant="top" />;
  }
  if (pageKey === "topDistribution") {
    return <DistributionSlide page={deck.pages.topDistribution} variant="top" salesMode={salesMode} layout={distributionLayout} />;
  }
  if (pageKey === "heroTrend") {
    return <TrendSlide page={deck.pages.heroTrend} priceSource={priceSource} priceEditor={priceEditor} variant="hero" />;
  }
  return <DistributionSlide page={deck.pages.heroDistribution} variant="hero" salesMode={salesMode} layout={distributionLayout} />;
}

function BenchmarkSlide({ rows, productRows, priceSource, priceEditor, salesMode, specColumns, customColumns, showChannel }: { rows: HeroProductModelRow[]; productRows: HeroProductModelRow[]; priceSource: HeroProductPriceSource; priceEditor: HeroProductPriceEditorBinding; salesMode: HeroProductSalesMode; specColumns: HeroProductSpecColumnKey[]; customColumns: HeroProductSpecColumnOption[]; showChannel: boolean }) {
  const insight = buildBenchmarkInsight(rows, productRows, priceSource, showChannel);

  return (
    <div className="hero-product-page-stack hero-product-page-stack--benchmark">
      <HeroProductInsightCallout insight={insight} />
      <div className="market-scan-grid market-scan-grid--two-wide hero-product-benchmark-grid">
        <HeroProductPanel
          eyebrow={showChannel ? "Ranking · Channel" : "Ranking · Volume"}
          title="SUV A0 BEV TOP20 销量"
          subtitle={showChannel ? "用 Business / Private 与 4WD 标记解释排行背后的渠道结构。" : "用销量、份额、YoY 和 4WD 暴露度建立动总对标基准。"}
          className="hero-product-ranking-panel"
        >
          <RankingList rows={rows.slice(0, 10)} />
        </HeroProductPanel>
        <HeroProductPanel
          eyebrow="Product · Spec"
          title="续航、性能、技术、价格对标"
          subtitle={`固定车型下沉到核心规格，并用 ${priceSource.toUpperCase()} 价格源对齐。`}
          className="hero-product-spec-panel"
        >
          <ProductSpecTable rows={productRows} priceSource={priceSource} priceEditor={priceEditor} salesMode={salesMode} visibleColumns={specColumns} customColumns={customColumns} />
        </HeroProductPanel>
      </div>
    </div>
  );
}

function RankingList({ rows }: { rows: HeroProductModelRow[] }) {
  if (rows.length === 0) {
    return <div className="market-scan-empty">暂无排行数据。</div>;
  }

  return (
    <div className="market-scan-ranking-list market-scan-ranking-list--fuel hero-product-ranking-list hero-product-share-ranking-list">
      {rows.map((row) => {
        const fourWd = fourWheelShare(row);
        const business = businessShare(row);
        const hasDrive = driveTotal(row) > 0;
        const hoverTitle = [
          `MS ${formatPercent(row.sharePct)}`,
          `Sales ${formatNumber(row.sales)}`,
          hasDrive ? `4WD ${formatWholePercent(fourWd)}` : null,
          channelMixText(row),
        ].filter((value): value is string => Boolean(value)).join(" · ");

        return (
          <article key={`${row.brand}-${row.model}-${row.rank}`} className="market-scan-ranking-row market-scan-ranking-row--fuel hero-product-ranking-row hero-product-share-ranking-row">
            <div className="market-scan-ranking-row-rank">{String(row.rank).padStart(2, "0")}</div>
            <div className="market-scan-ranking-row-info">
              <div className="market-scan-ranking-row-head">
                <span className="market-scan-ranking-row-name">{row.model}</span>
                <div className="market-scan-ranking-row-nums">
                  <span>{formatNumber(row.sales)}</span>
                  <span className="market-scan-tag">MS {formatPercent(row.sharePct)}</span>
                </div>
              </div>
              <div className="market-scan-ranking-row-bar" title={hoverTitle}>
                <span
                  className="market-scan-ranking-row-bar-fill hero-product-ranking-bar-fill"
                  style={{ width: `${Math.max(1, row.barPct * 100)}%` }}
                >
                  {hasDrive ? <span className="market-scan-4wd-fill hero-product-4wd-fill" style={{ width: `${fourWd * 100}%` }} /> : null}
                  <span
                    className="market-scan-business-marker hero-product-channel-marker"
                    style={{ left: `${Math.max(0, Math.min(100, business * 100))}%` }}
                    title={`Business ${formatWholePercent(business)}`}
                  />
                </span>
              </div>
            </div>
            <div className="market-scan-ranking-row-side">
              <span className={`market-scan-tone-text ${toneClassName(row.yoy.tone)}`}>
                YoY {row.yoy.tone === "positive" ? "▲ " : row.yoy.tone === "negative" ? "▼ " : ""}{row.yoy.display}
              </span>
              {hasDrive ? <DriveShareChip row={row} /> : null}
            </div>
          </article>
        );
      })}
    </div>
  );
}

function ProductSpecTable({ rows, priceSource, priceEditor, salesMode, visibleColumns, customColumns }: { rows: HeroProductModelRow[]; priceSource: HeroProductPriceSource; priceEditor: HeroProductPriceEditorBinding; salesMode: HeroProductSalesMode; visibleColumns: HeroProductSpecColumnKey[]; customColumns: HeroProductSpecColumnOption[] }) {
  const revertingRef = useRef(false);
  const gridRows = useMemo<HeroProductSpecGridRow[]>(() => rows.map((row) => {
    const price = selectedPrice(row, priceSource);
    const hasDrive = driveTotal(row) > 0;
    const hasChannel = Object.values(row.channelMix ?? {}).some((value) => Number(value) > 0)
      || Object.values(row.channelSharePct ?? {}).some((value) => Number.isFinite(value));
    const yoyPrefix = row.yoy.tone === "positive" ? "▲ " : row.yoy.tone === "negative" ? "▼ " : "";
    const gridRow: HeroProductSpecGridRow = {
      rowId: `${sourceBrand(row)}:${sourceModel(row)}`,
      brand: row.brand,
      model: row.model,
      sourceBrand: row.sourceBrand,
      sourceModel: row.sourceModel,
      pricePayload: row.price,
      sales: formatNumber(row.sales),
      sharePct: formatPercent(row.sharePct),
      yoy: `${yoyPrefix}${row.yoy.display}`,
      yoyTone: row.yoy.tone,
      fourWd: hasDrive ? formatWholePercent(fourWheelShare(row)) : "-",
      business: hasChannel ? formatWholePercent(businessShare(row)) : "-",
      rangeKm: specDisplay(row.specs.rangeKm, ""),
      batteryKwh: specDisplay(row.specs.batteryKwh, ""),
      consumptionKwh100km: specDisplay(row.specs.consumptionKwh100km, ""),
      accelerationSec: specDisplay(row.specs.accelerationSec, "s"),
      chargingText: row.specs.chargingText.join(" / ") || specDisplay(row.specs.chargingKw, "kW"),
      price: price.value == null ? "" : String(Math.round(price.value)),
    };
    for (const column of customColumns) {
      if (isCustomSpecColumnKey(column.key)) {
        gridRow[column.key] = row.specs.overrides?.[column.key]?.value ?? "";
      }
    }
    return gridRow;
  }), [customColumns, priceSource, rows]);

  const numericValue = useCallback((value: unknown, mode: "min" | "max"): number | null => {
    const values = String(value ?? "")
      .replace(/[^\d.,/ -]/g, "")
      .split(/[\/,，|]+/)
      .map((part) => Number(part.trim()))
      .filter((value): value is number => Number.isFinite(value));
    if (values.length === 0) return null;
    return mode === "min" ? Math.min(...values) : Math.max(...values);
  }, []);

  const rangeValues = useMemo(() => gridRows.map((row) => numericValue(row.rangeKm, "max")).filter((value): value is number => value !== null), [gridRows, numericValue]);
  const batteryValues = useMemo(() => gridRows.map((row) => numericValue(row.batteryKwh, "max")).filter((value): value is number => value !== null), [gridRows, numericValue]);
  const consumptionValues = useMemo(() => gridRows.map((row) => numericValue(row.consumptionKwh100km, "min")).filter((value): value is number => value !== null), [gridRows, numericValue]);
  const accelerationValues = useMemo(() => gridRows.map((row) => numericValue(row.accelerationSec, "min")).filter((value): value is number => value !== null), [gridRows, numericValue]);
  const priceValues = useMemo(() => gridRows.map((row) => numericValue(row.price, "min")).filter((value): value is number => value !== null), [gridRows, numericValue]);

  const editable = useCallback(() => priceEditor.canEdit, [priceEditor.canEdit]);
  const cellTone = useCallback((field: string, value: unknown): string => {
    if (field === "rangeKm") return specTone(numericValue(value, "max"), rangeValues, true);
    if (field === "batteryKwh") return specTone(numericValue(value, "max"), batteryValues, true);
    if (field === "consumptionKwh100km") return specTone(numericValue(value, "min"), consumptionValues, false);
    if (field === "accelerationSec") return specTone(numericValue(value, "min"), accelerationValues, false);
    if (field === "price") return numericValue(value, "min") == null ? "is-missing" : specTone(numericValue(value, "min"), priceValues, false);
    return "";
  }, [accelerationValues, batteryValues, consumptionValues, numericValue, priceValues, rangeValues]);

  const salesPrefix = salesModeColumnPrefix(salesMode);

  const allColumnDefs = useMemo<Map<HeroProductSpecColumnKey, ColDef<HeroProductSpecGridRow>>>(() => {
    const entries: Array<[HeroProductSpecColumnKey, ColDef<HeroProductSpecGridRow>]> = [
      ["brand", { headerName: "品牌", field: "brand", width: 112, editable, cellClass: "hero-product-ag-brand-cell" }],
      ["model", { headerName: "车型", field: "model", width: 136, editable, cellClass: "hero-product-ag-model-cell" }],
      ["sales", { headerName: `${salesPrefix}销量`, field: "sales", width: 106, editable: false, cellClass: "hero-product-ag-metric-cell" }],
      ["sharePct", { headerName: `${salesPrefix}份额`, field: "sharePct", width: 102, editable: false, cellClass: "hero-product-ag-metric-cell" }],
      ["yoy", {
        headerName: `${salesPrefix} YoY`,
        field: "yoy",
        width: 106,
        editable: false,
        cellClass: (params) => `hero-product-ag-metric-cell ${toneClassName(params.data?.yoyTone)}`,
      }],
      ["fourWd", { headerName: `${salesPrefix} 4WD`, field: "fourWd", width: 96, editable: false, cellClass: "hero-product-ag-metric-cell" }],
      ["business", { headerName: `${salesPrefix} Business`, field: "business", width: 126, editable: false, cellClass: "hero-product-ag-metric-cell" }],
      ["rangeKm", { headerName: "续航 km", field: "rangeKm", width: 118, editable, cellClass: (params) => cellTone("rangeKm", params.value) }],
      ["batteryKwh", { headerName: "电池 kWh", field: "batteryKwh", width: 124, editable, cellClass: (params) => cellTone("batteryKwh", params.value) }],
      ["consumptionKwh100km", { headerName: "电耗", field: "consumptionKwh100km", width: 110, editable, cellClass: (params) => cellTone("consumptionKwh100km", params.value) }],
      ["accelerationSec", { headerName: "零百", field: "accelerationSec", width: 104, editable, cellClass: (params) => cellTone("accelerationSec", params.value) }],
      ["chargingText", { headerName: "充电", field: "chargingText", width: 126, editable }],
      ["price", { headerName: "价格", field: "price", width: 116, editable, cellClass: (params) => cellTone("price", params.value) }],
    ];
    for (const column of customColumns) {
      if (isCustomSpecColumnKey(column.key)) {
        entries.push([
          column.key,
          {
            headerName: sanitizeCustomSpecColumnLabel(column.label) || customSpecColumnLabelFromKey(column.key),
            field: column.key,
            width: 118,
            editable,
            cellClass: "hero-product-ag-custom-cell",
          },
        ]);
      }
    }
    return new Map(entries);
  }, [cellTone, customColumns, editable, salesPrefix]);

  const columnDefs = useMemo<ColDef<HeroProductSpecGridRow>[]>(() => (
    visibleColumns.map((column) => allColumnDefs.get(column)).filter((column): column is ColDef<HeroProductSpecGridRow> => Boolean(column))
  ), [allColumnDefs, visibleColumns]);

  const defaultColDef = useMemo<ColDef<HeroProductSpecGridRow>>(() => ({
    resizable: true,
    sortable: false,
    filter: false,
    suppressHeaderMenuButton: true,
    singleClickEdit: true,
  }), []);

  const onCellValueChanged = useCallback((event: CellValueChangedEvent<HeroProductSpecGridRow>) => {
    if (revertingRef.current || !event.data) return;
    const field = event.colDef.field;
    if (!isEditableSpecGridField(field)) return;
    const oldValue = String(event.oldValue ?? "");
    const nextValue = String(event.newValue ?? "");
    if (oldValue === nextValue) return;
    const save = field === "price"
      ? priceEditor.onSavePriceValue(editableRowFromSpecGridRow(event.data), cleanPriceInput(nextValue))
      : priceEditor.onSaveSpecValue(editableRowFromSpecGridRow(event.data), field, nextValue);
    void save.catch(() => {
      revertingRef.current = true;
      event.node.setDataValue(field, event.oldValue);
      event.api.refreshCells({ rowNodes: [event.node], columns: [field], force: true });
      revertingRef.current = false;
    });
  }, [priceEditor]);

  return (
    <div className="hero-product-spec-grid-wrap">
      <AgGridReact<HeroProductSpecGridRow>
        key={visibleColumns.join("|")}
        theme={themeAlpine}
        rowData={gridRows}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        getRowId={(params) => params.data.rowId}
        onCellValueChanged={onCellValueChanged}
        stopEditingWhenCellsLoseFocus
        undoRedoCellEditing
        undoRedoCellEditingLimit={20}
        enableCellTextSelection
        rowHeight={48}
        headerHeight={36}
        suppressDragLeaveHidesColumns
      />
      {priceEditor.saveMessage ? <small className="hero-product-inline-save-message">{priceEditor.saveMessage}</small> : null}
    </div>
  );
}

function InlinePriceEditor({ row, priceSource, binding, variant }: { row: HeroProductPriceEditableRow; priceSource: HeroProductPriceSource; binding: HeroProductPriceEditorBinding; variant: "table" | "panel" | "drawer" }) {
  const price = selectedPricePayload(row.price, priceSource);
  const key = rowKey(row, priceSource);
  const draft = binding.priceDrafts[key] ?? (price.value == null ? "" : String(Math.round(price.value)));
  const isSaving = binding.savingPriceKey === key;
  const isMissing = price.value == null || price.status === "missing";
  const disabled = !binding.canEdit || isSaving;
  const status = priceStatusLabel(price.status);

  return (
    <div className={`hero-product-inline-price-editor is-${variant}${isMissing ? " is-missing" : ""}${!binding.canEdit ? " is-readonly" : ""}`}>
      <input
        type="number"
        min={0}
        inputMode="decimal"
        value={draft}
        disabled={disabled}
        placeholder="空白"
        aria-label={`${row.model} ${priceSource.toUpperCase()} 价格`}
        title={binding.canEdit ? "输入价格后保存" : "Editor 及以上可编辑"}
        onChange={(event) => binding.onDraftChange(row, event.target.value)}
        onKeyDown={(event) => {
          if (event.key === "Enter" && !disabled) {
            event.preventDefault();
            binding.onSavePrice(row);
          }
        }}
      />
      <button type="button" className="btn btn-secondary btn-sm" disabled={disabled} onClick={() => binding.onSavePrice(row)}>
        {isSaving ? "保存中" : "保存"}
      </button>
      <small title={`${price.currency || "EUR"} · ${status}`}>{status}</small>
    </div>
  );
}

function TrendSlide({ page, priceSource, priceEditor, variant }: { page: HeroProductDeckResponse["pages"]["topTrend"]; priceSource: HeroProductPriceSource; priceEditor: HeroProductPriceEditorBinding; variant: "top" | "hero" }) {
  const insight = buildTrendInsight(page, priceSource, variant);
  const channelRows = page.models.slice(0, variant === "hero" ? 6 : 10);

  return (
    <div className="hero-product-page-stack hero-product-page-stack--trend">
      <HeroProductInsightCallout insight={insight} />
      <div className="market-scan-grid hero-product-trend-grid">
        <HeroProductPanel
          eyebrow={variant === "hero" ? "Trend · Fixed Models" : "Trend · Top Models"}
          title={page.title}
          subtitle="顶部用 Business / Private 饼图解释渠道结构，折线补充最近周期销量趋势。"
          className="hero-product-trend-panel"
        >
          <ChannelMixStrip rows={channelRows} />
          <TrendSvg series={page.series} />
        </HeroProductPanel>
        <PricePanel rows={page.priceRows} priceSource={priceSource} priceEditor={priceEditor} />
      </div>
    </div>
  );
}

function TrendSvg({ series }: { series: HeroProductTrendSeries[] }) {
  const periods = series.reduce<HeroProductTrendSeries["points"]>((longest, item) => (
    item.points.length > longest.length ? item.points : longest
  ), []);
  if (periods.length === 0 || series.length === 0) {
    return <div className="market-scan-empty">暂无趋势数据。</div>;
  }

  const maxValue = Math.max(1, ...series.flatMap((item) => item.points.map((point) => point.volume)));
  const width = 1000;
  const height = 420;
  const padLeft = 48;
  const padRight = 168;
  const padTop = 36;
  const padBottom = 48;
  const chartHeight = height - padTop - padBottom;
  const x = (index: number) => padLeft + (index / Math.max(1, periods.length - 1)) * (width - padLeft - padRight);
  const y = (value: number) => height - padBottom - (value / maxValue) * chartHeight;
  const buildPath = (points: Array<{ x: number; y: number }>): string => {
    if (points.length === 0) return "";
    if (points.length === 1) return `M${points[0].x},${points[0].y}`;
    return points.slice(1).reduce((path, point, index) => {
      const previous = points[index];
      const midX = (previous.x + point.x) / 2;
      return `${path} C${midX},${previous.y} ${midX},${point.y} ${point.x},${point.y}`;
    }, `M${points[0].x},${points[0].y}`);
  };
  const totalsByIndex = periods.map((_, periodIndex) => series.reduce((sum, item) => sum + Math.max(0, item.points[periodIndex]?.volume ?? 0), 0));
  const topSharePoints = (series[0]?.points ?? []).map((point, index) => {
    const share = totalsByIndex[index] > 0 ? point.volume / totalsByIndex[index] : 0;
    return { x: x(index), y: height - padBottom - clampShare(share) * chartHeight };
  });
  const labelKey = (item: HeroProductTrendSeries) => `${item.brand}-${item.model}`;
  const labelTop = padTop + 8;
  const labelBottom = height - padBottom - 8;
  const minLabelGap = 24;
  const labelRows = series.map((item) => {
    const lastPoint = item.points[item.points.length - 1];
    return lastPoint ? { key: labelKey(item), y: y(lastPoint.volume) + 4 } : null;
  }).filter((item): item is { key: string; y: number } => Boolean(item)).sort((a, b) => a.y - b.y);
  let nextLabelY = labelTop;
  labelRows.forEach((item) => {
    item.y = Math.max(item.y, nextLabelY);
    nextLabelY = item.y + minLabelGap;
  });
  let previousLabelY = labelBottom;
  [...labelRows].reverse().forEach((item) => {
    item.y = Math.min(item.y, previousLabelY);
    previousLabelY = item.y - minLabelGap;
  });
  const labelYBySeries = new Map(labelRows.map((item) => [item.key, item.y]));

  return (
    <svg className="hero-product-trend-svg" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="销量趋势">
      {[0, 0.25, 0.5, 0.75, 1].map((ratio) => (
        <g key={ratio}>
          <line className="hero-product-trend-gridline" x1={padLeft} y1={height - padBottom - ratio * chartHeight} x2={width - padRight} y2={height - padBottom - ratio * chartHeight} />
          <text className="hero-product-trend-axis-label" x={12} y={height - padBottom - ratio * chartHeight + 4}>{formatNumber(maxValue * ratio)}</text>
        </g>
      ))}
      <path className="hero-product-trend-share-line" d={buildPath(topSharePoints)} />
      <text className="hero-product-trend-share-label" x={width - padRight + 36} y={topSharePoints[topSharePoints.length - 1]?.y ?? padTop} textAnchor="start">TOP1 share</text>
      {periods.map((point, index) => (
        <text key={point.period} x={x(index)} y={height - 12} textAnchor="middle">{point.label}</text>
      ))}
      {series.map((item, seriesIndex) => {
        const color = LINE_COLORS[seriesIndex % LINE_COLORS.length];
        const coordinates = item.points.map((point, index) => ({ x: x(index), y: y(point.volume) }));
        const path = buildPath(coordinates);
        const lastPoint = item.points[item.points.length - 1];
        const labelY = labelYBySeries.get(labelKey(item)) ?? (lastPoint ? y(lastPoint.volume) + 4 : padTop);
        return (
          <g key={`${item.brand}-${item.model}`}>
            <path d={path} style={{ stroke: color }} />
            {item.points.map((point, index) => (
              <circle key={point.period} cx={x(index)} cy={y(point.volume)} r={3.2} style={{ fill: color }} />
            ))}
            {lastPoint ? (
              <text x={width - padRight + 10} y={labelY} style={{ fill: color }}>{item.model} {formatNumber(lastPoint.volume)}</text>
            ) : null}
          </g>
        );
      })}
    </svg>
  );
}

function PricePanel({ rows, priceSource, priceEditor }: { rows: HeroProductDeckResponse["pages"]["topTrend"]["priceRows"]; priceSource: HeroProductPriceSource; priceEditor: HeroProductPriceEditorBinding }) {
  const revertingRef = useRef(false);
  const gridRows = useMemo<HeroProductPricePanelGridRow[]>(() => rows.map((row) => {
    const price = selectedPricePayload(row.price, priceSource);
    const sourceValue = price.rawValue ?? null;
    const savedValue = price.value ?? null;
    const isMissing = savedValue == null || price.status === "missing";
    return {
      rowId: rowKey(row, priceSource),
      brand: row.brand,
      model: row.model,
      sourceBrand: row.sourceBrand,
      sourceModel: row.sourceModel,
      origin: row.origin,
      pricePayload: row.price,
      raw: formatPriceNumber(sourceValue),
      source: formatPriceNumber(sourceValue ?? savedValue),
      saved: savedValue == null ? "" : String(Math.round(savedValue)),
      status: priceStatusLabel(price.status),
      isMissing,
    };
  }), [priceSource, rows]);

  const columnDefs = useMemo<ColDef<HeroProductPricePanelGridRow>[]>(() => [
    {
      headerName: "RAW",
      field: "model",
      minWidth: 116,
      flex: 1.35,
      cellClass: "hero-product-price-grid-model",
      cellRenderer: (params: { data?: HeroProductPricePanelGridRow; value?: string }) => {
        if (!params.data) return params.value ?? "";
        return (
          <span className="hero-product-price-grid-model-cell">
            <strong>{params.data.model}</strong>
            <small>{params.data.origin}</small>
          </span>
        );
      },
    },
    {
      headerName: "SOURCE",
      field: "source",
      minWidth: 92,
      flex: 0.95,
      cellClass: "hero-product-price-grid-source",
      cellRenderer: (params: { value?: string }) => (
        <span className="hero-product-price-grid-value">{params.value || "空白"}</span>
      ),
    },
    {
      headerName: "SAVED",
      field: "saved",
      minWidth: 100,
      flex: 1,
      editable: () => priceEditor.canEdit,
      cellEditor: "agTextCellEditor",
      cellClass: (params) => {
        const data = params.data;
        return `hero-product-price-grid-saved${data?.isMissing ? " is-missing" : ""}${priceEditor.canEdit ? " is-editable" : " is-readonly"}`;
      },
      cellRenderer: (params: { data?: HeroProductPricePanelGridRow; value?: string }) => (
        <span className="hero-product-price-grid-saved-cell">
          <strong>{params.value || "空白"}</strong>
          <small>{params.data?.status || "source"}</small>
        </span>
      ),
      tooltipValueGetter: (params) => {
        const status = params.data?.status || "source";
        return `${status} · ${priceSource.toUpperCase()}`;
      },
    },
  ], [priceEditor.canEdit, priceSource]);

  const defaultColDef = useMemo<ColDef<HeroProductPricePanelGridRow>>(() => ({
    resizable: true,
    sortable: false,
    filter: false,
    suppressHeaderMenuButton: true,
    singleClickEdit: true,
  }), []);

  const onCellValueChanged = useCallback((event: CellValueChangedEvent<HeroProductPricePanelGridRow>) => {
    if (revertingRef.current || !event.data || event.colDef.field !== "saved") return;
    const oldValue = String(event.oldValue ?? "");
    const nextValue = String(event.newValue ?? "");
    if (oldValue === nextValue) return;
    void priceEditor
      .onSavePriceValue(editableRowFromPriceGridRow(event.data), cleanPriceInput(nextValue))
      .catch(() => {
        revertingRef.current = true;
        event.node.setDataValue("saved", oldValue);
        event.api.refreshCells({ rowNodes: [event.node], columns: ["saved"], force: true });
        revertingRef.current = false;
      });
  }, [priceEditor]);

  return (
    <HeroProductPanel
      eyebrow="Price"
      title="价格"
      subtitle={`${priceSource.toUpperCase()} 可切换；空白项等待 Editor 补录。`}
      className="hero-product-price-panel"
    >
      <div className="hero-product-price-grid-wrap">
        <AgGridReact<HeroProductPricePanelGridRow>
          theme={themeAlpine}
          rowData={gridRows}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          getRowId={(params) => params.data.rowId}
          onCellValueChanged={onCellValueChanged}
          domLayout="autoHeight"
          stopEditingWhenCellsLoseFocus
          undoRedoCellEditing
          undoRedoCellEditingLimit={20}
          enableCellTextSelection
          rowHeight={58}
          headerHeight={36}
          suppressDragLeaveHidesColumns
        />
      </div>
      {priceEditor.saveMessage ? <small className="hero-product-inline-save-message">{priceEditor.saveMessage}</small> : null}
    </HeroProductPanel>
  );
}

type HeroProductDistributionCountryRow = HeroProductDeckResponse["pages"]["topDistribution"]["distribution"]["items"][number]["countries"][number] & {
  isAlignedEmpty?: boolean;
};

function emptyDistributionCountry(country: string): HeroProductDistributionCountryRow {
  return {
    country,
    sales: 0,
    isAlignedEmpty: true,
    driveMix: {
      front: 0,
      rear: 0,
      "4x4": 0,
      other: 0,
    },
  };
}

function alignedCountryOrder(items: HeroProductDeckResponse["pages"]["topDistribution"]["distribution"]["items"], fallbackCountries: string[]): string[] {
  const firstColumnCountries = items[0]?.countries.map((country) => country.country).filter(Boolean) ?? [];
  return firstColumnCountries.length > 0 ? firstColumnCountries : fallbackCountries;
}

function countryRowsForDistributionItem(
  item: HeroProductDeckResponse["pages"]["topDistribution"]["distribution"]["items"][number],
  layout: HeroProductDistributionLayout,
  countryOrder: string[],
): HeroProductDistributionCountryRow[] {
  if (layout === "ranked") return item.countries;
  const byCountry = new Map(item.countries.map((country) => [country.country, country]));
  return countryOrder.map((country) => byCountry.get(country) ?? emptyDistributionCountry(country));
}

function DistributionSlide({ page, variant, salesMode, layout }: { page: HeroProductDeckResponse["pages"]["topDistribution"]; variant: "top" | "hero"; salesMode: HeroProductSalesMode; layout: HeroProductDistributionLayout }) {
  const insight = buildDistributionInsight(page, variant);
  const visibleItems = page.distribution.items.slice(0, variant === "top" ? 10 : 6);
  const countryOrder = alignedCountryOrder(visibleItems, page.distribution.countries);
  const maxCountryRows = Math.max(1, ...visibleItems.map((item) => (
    layout === "aligned" ? countryOrder.length : item.countries.length
  )));
  const densityClass = maxCountryRows > 34 ? " is-very-dense" : maxCountryRows > 24 ? " is-dense" : "";
  const matrixStyle = {
    "--hero-product-distribution-cols": String(Math.max(1, visibleItems.length)),
  } as CSSProperties;
  const modeLabel = salesModeLabel(salesMode);
  const layoutLabel = layout === "aligned" ? "国家对齐" : "独立排序";

  return (
    <div className="hero-product-page-stack hero-product-page-stack--distribution">
      <HeroProductInsightCallout insight={insight} />
      <HeroProductPanel
        eyebrow={variant === "hero" ? "Market · Fixed Models" : "Market · Top Models"}
        title={page.title}
        subtitle={`${modeLabel}口径 · ${layoutLabel}；${layout === "aligned" ? "按第一列车型国家顺序统一行轴，缺失国家保留空行。" : "每个车型独立成列，按国家销量排序。"}`}
        className={`hero-product-distribution-panel hero-product-distribution-panel--${variant}${layout === "aligned" ? " is-country-aligned" : ""}${densityClass}`}
      >
        <div className="hero-product-distribution-wrap">
          <div className="hero-product-distribution-legend">
            {Object.entries(HERO_PRODUCT_DRIVE_META).map(([key, meta]) => (
              <span key={key}><i style={{ background: meta.color }} />{meta.label}</span>
            ))}
          </div>
          <div className="hero-product-distribution-grid" style={matrixStyle}>
        {visibleItems.map((item) => {
          const countryRows = countryRowsForDistributionItem(item, layout, countryOrder);
          const modelMaxCountrySales = Math.max(1, ...countryRows.map((country) => country.sales));
          return (
            <section key={`${item.brand}-${item.model}`} className="hero-product-distribution-card">
              <header><strong title={item.model}>{item.model}</strong><span>总计 {formatNumber(item.totalSales)}</span></header>
              <div className="hero-product-country-list">
                {countryRows.map((country) => (
                  <CountryDriveRow key={country.country} country={country} maxSales={modelMaxCountrySales} />
                ))}
              </div>
              <footer className="hero-product-distribution-total">
                <span>总计</span>
                <strong>{formatNumber(item.totalSales)}</strong>
              </footer>
            </section>
          );
        })}
          </div>
        </div>
      </HeroProductPanel>
    </div>
  );
}

function CountryDriveRow({ country, maxSales }: { country: HeroProductDistributionCountryRow; maxSales: number }) {
  const isEmpty = Boolean(country.isAlignedEmpty) || country.sales <= 0;
  const total = Math.max(1, country.sales);
  const frontShare = clampShare(Number(country.driveMix.front ?? 0) / total);
  const rearShare = clampShare(Number(country.driveMix.rear ?? 0) / total);
  const fourShare = clampShare(Number(country.driveMix["4x4"] ?? 0) / total);
  const barWidth = isEmpty ? 0 : Math.max(1, Math.min(100, (country.sales / Math.max(1, maxSales)) * 100));

  return (
    <div className={`hero-product-country-row${isEmpty ? " is-empty" : ""}`}>
      <span>{country.country}</span>
      <div className="hero-product-country-bar-shell">
        <div className="hero-product-drive-stack" style={{ width: `${barWidth}%` }}>
          <i className="is-front" style={{ width: `${frontShare * 100}%` }} />
          <i className="is-rear" style={{ width: `${rearShare * 100}%` }} />
          <i className="is-4x4" style={{ width: `${fourShare * 100}%` }} />
        </div>
      </div>
      <strong>{isEmpty ? "-" : formatNumber(country.sales)}</strong>
    </div>
  );
}

function ChannelMixStrip({ rows }: { rows: HeroProductModelRow[] }) {
  return (
    <div className="hero-product-channel-strip hero-product-channel-strip--pies">
      <div className="hero-product-channel-legend">
        {HERO_PRODUCT_CHANNEL_ORDER.map((channel) => (
          <span key={channel}><i style={{ background: HERO_PRODUCT_CHANNEL_META[channel].color }} />{HERO_PRODUCT_CHANNEL_META[channel].label}</span>
        ))}
      </div>
      {rows.map((row) => <ChannelMixPie key={`${row.brand}-${row.model}`} row={row} />)}
    </div>
  );
}

function ChannelMixPie({ row }: { row: HeroProductModelRow }) {
  const business = businessShare(row);
  const privateShare = channelShare(row, "Private");
  const other = Math.max(0, 1 - business - privateShare);
  const businessEnd = business * 360;
  const privateEnd = (business + privateShare) * 360;
  const background = `conic-gradient(${HERO_PRODUCT_CHANNEL_META.Business.color} 0deg ${businessEnd}deg, ${HERO_PRODUCT_CHANNEL_META.Private.color} ${businessEnd}deg ${privateEnd}deg, ${HERO_PRODUCT_CHANNEL_META.Other.color} ${privateEnd}deg 360deg)`;

  return (
    <div className="hero-product-channel-pie-card" title={`${row.model} ${channelMixText(row)}`}>
      <strong>{row.model}</strong>
      <div className="hero-product-channel-pie" style={{ background }}>
        {privateShare > 0.06 ? <span className="hero-product-channel-pie-label is-private">{formatWholePercent(privateShare)}</span> : null}
        {business > 0.06 ? <span className="hero-product-channel-pie-label is-business">{formatWholePercent(business)}</span> : null}
        {other > 0.06 ? <span className="hero-product-channel-pie-label is-other">{formatWholePercent(other)}</span> : null}
      </div>
    </div>
  );
}

function ChannelMixBar({ row, compact = false }: { row: HeroProductModelRow; compact?: boolean }) {
  const business = businessShare(row);
  const privateShare = channelShare(row, "Private");
  const other = Math.max(0, 1 - business - privateShare);
  const shares: Array<[(typeof HERO_PRODUCT_CHANNEL_ORDER)[number], number]> = [
    ["Business", business],
    ["Private", privateShare],
    ["Other", other],
  ];

  return (
    <div className={`hero-product-channel-bar-card${compact ? " is-compact" : ""}`} title={`${row.model} ${channelMixText(row)}`}>
      {!compact ? <span className="hero-product-channel-model">{row.model}</span> : null}
      <div className="hero-product-channel-bar">
        {shares.map(([channel, share]) => (
          <i
            key={channel}
            style={{
              width: `${Math.max(share > 0 ? 2 : 0, share * 100)}%`,
              background: HERO_PRODUCT_CHANNEL_META[channel].color,
              color: HERO_PRODUCT_CHANNEL_META[channel].textColor,
            }}
          >
            {!compact && share >= 0.18 ? formatWholePercent(share) : ""}
          </i>
        ))}
        <span className="hero-product-channel-bar-marker" style={{ left: `${Math.max(0, Math.min(100, business * 100))}%` }} />
      </div>
      {!compact ? <strong>{formatWholePercent(privateShare)}</strong> : null}
    </div>
  );
}

function DriveShareChip({ row }: { row: HeroProductModelRow }) {
  const share = fourWheelShare(row);
  return (
    <span className="market-scan-ranking-row-drive-chip hero-product-drive-chip" title={`${row.model} 4WD ${formatWholePercent(share)}`}>
      <span className="hero-product-drive-chip-marker" />
      <span className="market-scan-ranking-row-drive-chip-label">4WD</span>
      <strong className="market-scan-ranking-row-drive-chip-value">{formatWholePercent(share)}</strong>
      <span className="hero-product-drive-chip-track"><i style={{ width: `${share * 100}%` }} /></span>
    </span>
  );
}
