import { useEffect, useMemo, useRef, useState } from "react";

import { api } from "../api/client";
import { DeckControlTabs, DeckFloatingDrawer, type DeckControlTabItem } from "../components/deckControls";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  MsrpAuditPriority,
  MsrpBackfillSnapshotPreview,
  MsrpBatchACountryCoverage,
  MsrpLaunchAlert,
  MsrpMonitoringModelEvent,
  MsrpMonitoringResponse,
  MsrpMonitoringTimelineEvent,
  MsrpOfferSignal,
} from "../types";

interface WindowOption {
  id: string;
  days: number;
  label: string;
  fromDate?: string;
}

const WINDOW_OPTIONS: WindowOption[] = [
  { id: "7d", days: 7, label: "7D" },
  { id: "30d", days: 30, label: "30D" },
  { id: "90d", days: 90, label: "90D" },
  { id: "180d", days: 180, label: "180D" },
];

const THRESHOLD_OPTIONS = [
  { value: 0, label: "Any" },
  { value: 1, label: ">= 1%" },
  { value: 3, label: ">= 3%" },
  { value: 5, label: ">= 5%" },
] as const;

const REFRESH_OPTIONS = [
  { value: 0, label: "Off" },
  { value: 30, label: "30s" },
  { value: 60, label: "60s" },
  { value: 300, label: "5m" },
] as const;

const MODE_OPTIONS = [
  { value: "live", label: "Live" },
  { value: "sweden_demo", label: "Sweden demo" },
  { value: "sweden_swiss_demo", label: "Sweden + Swiss demo" },
] as const;

const AUDIT_OPTIONS = [
  { value: "all", label: "All audit" },
  { value: "block", label: "Block" },
  { value: "priority_audit", label: "Priority" },
  { value: "sample", label: "Sample" },
  { value: "auto_pass", label: "Auto pass" },
] as const;

const DIRECTION_OPTIONS = [
  { value: "drops", label: "Drops" },
  { value: "all", label: "All moves" },
  { value: "increases", label: "Increases" },
] as const;

const EVIDENCE_OPTIONS = [
  { value: "all", label: "All evidence" },
  { value: "official_backfill", label: "Official backfill" },
  { value: "campaign_promotion", label: "Campaign/promotion" },
  { value: "no_backfill", label: "No backfill" },
  { value: "demo_backfill", label: "Demo backfill" },
] as const;

const CHART_WIDTH = 920;
const CHART_HEIGHT = 560;
const CHART_MARGIN = { top: 38, right: 36, bottom: 54, left: 78 } as const;
const TIMELINE_CHART_WIDTH = 560;
const TIMELINE_CHART_HEIGHT = 380;
const TIMELINE_CHART_MARGIN = { top: 28, right: 18, bottom: 96, left: 70 } as const;
const SALES_CHART_WIDTH = 560;
const SALES_CHART_HEIGHT = 260;
const SALES_CHART_MARGIN = { top: 26, right: 20, bottom: 42, left: 58 } as const;
const IMPACT_CHART_WIDTH = 560;
const IMPACT_CHART_HEIGHT = 168;
const IMPACT_CHART_MARGIN = { top: 18, right: 16, bottom: 30, left: 58 } as const;
const TIMELINE_SERIES_COLORS = ["#2563eb", "#16a34a", "#f97316", "#7c3aed", "#0891b2", "#b45309"];
const SPOT_CHECK_QUEUE_LIMIT = 8;
const PRICE_ACTION_BOARD_LIMIT = 12;
const MAX_CHART_MOVEMENT_SEGMENTS = 4;

type DeckTab = "filters" | "overview" | "countries" | "timeline" | "offers" | "source";
type MonitorMode = typeof MODE_OPTIONS[number]["value"];
type AuditFilter = typeof AUDIT_OPTIONS[number]["value"];
type DirectionFilter = typeof DIRECTION_OPTIONS[number]["value"];
type EvidenceFilter = typeof EVIDENCE_OPTIONS[number]["value"];
type OfferSignalColumnKey = "cash" | "finance" | "lease" | "benefit" | "gap";

const DEFAULT_SWEDEN_SWISS_DEMO_COUNTRY = "CH";

interface TimelineSeries {
  key: string;
  label: string;
  color: string;
  events: MsrpMonitoringTimelineEvent[];
}

interface ChartMovementSegment {
  key: string;
  oldY: number;
  currentY: number;
  xOffset: number;
  opacity: number;
  className: string;
  label: string;
}

interface ChartPointHoverDetail {
  scopeLabel: string;
  trimLabel: string;
  changeLabel: string;
  priceLabel: string;
  salesLabel: string;
  salesSourceLabel: string;
  salesUseLabel: string;
  evidenceLabel: string;
  sampleLabel: string;
}

interface SpotCheckQueueItem {
  key: string;
  eventId: string;
  modelLabel: string;
  item: MsrpMonitoringTimelineEvent;
  decision: SpotCheckDecision;
  effectivePriority: MsrpAuditPriority | string;
  effectiveActionLabel: string;
  priorityRank: number;
  absChangePct: number;
}

type PriceActionKind = "msrp_move" | "offer_signal";

interface PriceActionBase {
  key: string;
  kind: PriceActionKind;
  modelLabel: string;
  countryLabel: string;
  trimLabel: string;
  actionTypeLabel: string;
  primaryMetric: string;
  secondaryMetric: string;
  evidenceLabel: string;
  statusLabel: string;
  statusDetail: string;
  auditPriority: MsrpAuditPriority | string;
  changedAtLabel: string;
  sortRank: number;
  sortMagnitude: number;
}

interface PriceActionMoveItem extends PriceActionBase {
  kind: "msrp_move";
  queueItem: SpotCheckQueueItem;
}

interface PriceActionOfferItem extends PriceActionBase {
  kind: "offer_signal";
  signal: MsrpOfferSignal;
}

type PriceActionItem = PriceActionMoveItem | PriceActionOfferItem;

interface SpotCheckDecisionSummary {
  key: string;
  label: string;
  className: string;
  count: number;
  detail: string;
  firstItemKey: string;
}

interface EffectiveAudit {
  priority: MsrpAuditPriority | string;
  actionLabel: string;
  bucket: string;
  rank: number;
}

interface OfferSignalColumn {
  key: OfferSignalColumnKey;
  label: string;
}

type EvidenceChecklistStatus = "pass" | "warn" | "pending";
type CopyStatus = "idle" | "copied" | "failed";

interface ImpactSeriesPoint {
  label: string;
  actual: number;
  expected: number;
  effect: number;
  cumulative: number;
}

interface PriceImpactModel {
  title: string;
  subtitle: string;
  metricLabel: string;
  valueKind: "eur" | "local" | "count";
  valueCurrency?: string;
  effectLabel: string;
  effectPctLabel: string;
  rangeLabel: string;
  avgPerDayLabel: string;
  durationLabel: string;
  verdictLabel: string;
  baselineLabel: string;
  actualLabel: string;
  expectedLabel: string;
  notes: string;
  points: ImpactSeriesPoint[];
}

interface EvidenceChecklistItem {
  key: string;
  label: string;
  status: EvidenceChecklistStatus;
  detail: string;
}

interface SpotCheckBrief {
  decision: SpotCheckDecision;
  statusLabel: string;
  statusClassName: string;
  matchedCount: number;
  totalCount: number;
  warningLabels: string[];
  lines: string[];
}

interface SpotCheckDecision {
  label: string;
  className: string;
  detail: string;
  actions: string[];
}

interface SelectedMonitoringItem {
  eventId: string | null;
  countryKey: string | null;
}

const EMPTY_SELECTION: SelectedMonitoringItem = {
  eventId: null,
  countryKey: null,
};

const OFFER_SIGNAL_COLUMNS: OfferSignalColumn[] = [
  { key: "cash", label: "Cash" },
  { key: "finance", label: "Finance" },
  { key: "lease", label: "Lease" },
  { key: "benefit", label: "Benefit" },
  { key: "gap", label: "Gap" },
];

const DECK_TABS: Array<DeckControlTabItem<DeckTab>> = [
  { key: "filters", label: "Filters", caption: "scope" },
  { key: "overview", label: "Overview", caption: "event" },
  { key: "countries", label: "Countries", caption: "trim" },
  { key: "timeline", label: "Timeline", caption: "history" },
  { key: "offers", label: "Offers", caption: "signals" },
  { key: "source", label: "Source", caption: "evidence" },
];

function formatNumber(value: number | null | undefined, digits = 0): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return value.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "EUR",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatSek(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return `${formatNumber(value)} SEK`;
}

function formatBytes(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function currentYearWindowOption(now = new Date()): WindowOption {
  const year = now.getFullYear();
  const startOfYear = new Date(year, 0, 1);
  const elapsedMs = now.getTime() - startOfYear.getTime();
  const elapsedDays = Math.max(1, Math.ceil(elapsedMs / 86_400_000));
  return {
    id: `${year}-ytd`,
    days: Math.min(365, elapsedDays + 1),
    label: `${year} YTD`,
    fromDate: `${year}-01-01`,
  };
}

function monitoringWindowOptions(): WindowOption[] {
  const ytdOption = currentYearWindowOption();
  return [...WINDOW_OPTIONS, ytdOption];
}

function formatTime(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) {
    return "Not synced";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function eventLabel(event: MsrpMonitoringModelEvent): string {
  return `${event.brand} ${event.jatoModel}`;
}

function countryKey(item: MsrpMonitoringTimelineEvent): string {
  return `${item.country}|${item.jatoTrim}|${item.changedAtUtc ?? ""}`;
}

function riskLabel(event: MsrpMonitoringModelEvent): string {
  if (event.suspectedFalsePositiveCount > 0) {
    return `${event.suspectedFalsePositiveCount} risk`;
  }
  if (event.sourceRiskCount > 0) {
    return `${event.sourceRiskCount} source`;
  }
  return "clean";
}

function auditLabel(priority: MsrpAuditPriority | string | null | undefined): string {
  switch (priority) {
    case "block":
      return "Block";
    case "priority_audit":
      return "Priority audit";
    case "sample":
      return "Sample";
    case "auto_pass":
      return "Auto pass";
    default:
      return "Review";
  }
}

function auditFilterLabel(filter: AuditFilter): string {
  return AUDIT_OPTIONS.find((option) => option.value === filter)?.label ?? auditLabel(filter);
}

function samplingBucketLabel(bucket: string | null | undefined): string {
  switch (bucket) {
    case "campaign_promotion_boundary":
      return "Campaign boundary";
    case "historical_backfill":
      return "Historical backfill";
    case "routine_price_move":
      return "Routine price move";
    case "single_trim_market_move":
      return "Single trim move";
    case "large_price_move":
      return "Large price move";
    case "source_risk":
      return "Source risk";
    case "currency_change":
      return "Currency change";
    case "outlier":
      return "Outlier";
    case "demo_backfill":
      return "Demo backfill";
    case "new_launch_price_baseline":
      return "Launch baseline";
    case "official_offer_signal":
      return "Official offer";
    case "official_offer_signal_gap":
      return "Source gap";
    case "clean_confirmed":
      return "Clean confirmed";
    default:
      return bucket ? bucket.replace(/_/g, " ") : "-";
  }
}

function auditReasonLabel(reason: string): string {
  switch (reason) {
    case "campaign_promotion_boundary:not_permanent_msrp_cut":
      return "Campaign boundary";
    case "historical_price_backfill":
      return "Historical backfill";
    case "price_move:>=1pct":
      return "Price move >=1%";
    case "single_trim_market_move:>=3pct":
      return "Single trim >=3%";
    case "large_price_move:>=5pct":
      return "Large move >=5%";
    case "source_currency_changed":
      return "Currency changed";
    case "outlier_vs_model_country_cluster":
      return "Cluster outlier";
    case "confirmed_low_variance":
      return "Low variance";
    case "demo_backfilled_scenario":
      return "Demo backfill";
    default:
      return reason.replace(/_/g, " ");
  }
}

function directionFilterLabel(filter: DirectionFilter): string {
  return DIRECTION_OPTIONS.find((option) => option.value === filter)?.label ?? "Moves";
}

function evidenceFilterLabel(filter: EvidenceFilter): string {
  return EVIDENCE_OPTIONS.find((option) => option.value === filter)?.label ?? "Evidence";
}

function offerTypeLabel(type: string): string {
  switch (type) {
    case "cash_discount":
      return "Cash";
    case "finance_offer":
      return "Finance";
    case "lease_offer":
      return "Lease";
    case "purchase_benefit":
      return "Benefit";
    case "coverage_gap":
      return "Gap";
    default:
      return type.replace(/_/g, " ");
  }
}

function offerSignalMatchStatusLabel(status: string): string {
  switch (status) {
    case "pending_current_price_match":
      return "Pending MSRP match";
    case "source_coverage_gap":
      return "Coverage gap";
    default:
      return status.replace(/_/g, " ");
  }
}

function offerSignalMatchesEvidence(signal: MsrpOfferSignal, evidence: EvidenceFilter): boolean {
  switch (evidence) {
    case "all":
      return true;
    case "campaign_promotion":
      return signal.primaryType !== "coverage_gap";
    case "official_backfill":
      return signal.matchStatus === "pending_current_price_match";
    case "no_backfill":
    case "demo_backfill":
      return false;
    default:
      return true;
  }
}

function offerSignalMatchesAudit(signal: MsrpOfferSignal, audit: AuditFilter): boolean {
  return audit === "all" || signal.auditPriority === audit;
}

function offerSignalMatchesDirection(signal: MsrpOfferSignal, direction: DirectionFilter): boolean {
  if (direction === "increases") {
    return false;
  }
  return direction === "all" || signal.primaryType !== "coverage_gap";
}

function offerSignalMatchesFilters(
  signal: MsrpOfferSignal,
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): boolean {
  return offerSignalMatchesDirection(signal, direction)
    && offerSignalMatchesEvidence(signal, evidence)
    && offerSignalMatchesAudit(signal, audit);
}

function offerSignalMetric(signal: MsrpOfferSignal): string {
  const cashDiscount = offerSignalCashDiscount(signal);
  const monthlyPayment = offerSignalMonthlyPayment(signal);
  if (cashDiscount !== null) {
    return formatSourcePrice(cashDiscount, offerSignalLocalCurrency(signal));
  }
  if (signal.interestRatePct !== null) {
    return `${formatNumber(signal.interestRatePct, 1)}% interest`;
  }
  if (monthlyPayment !== null) {
    return `${formatSourcePrice(monthlyPayment, offerSignalLocalCurrency(signal))}/mo`;
  }
  return offerSignalMatchStatusLabel(signal.matchStatus);
}

function offerSignalLocalCurrency(signal: MsrpOfferSignal): string {
  return signal.localCurrency || (signal.country === "CH" ? "CHF" : "SEK");
}

function offerSignalCashDiscount(signal: MsrpOfferSignal): number | null {
  return signal.cashDiscountLocal ?? signal.cashDiscountSek;
}

function offerSignalMonthlyPayment(signal: MsrpOfferSignal): number | null {
  return signal.monthlyPaymentLocal ?? signal.monthlyPaymentSek;
}

function offerSignalColumnActive(signal: MsrpOfferSignal, column: OfferSignalColumnKey): boolean {
  switch (column) {
    case "cash":
      return offerSignalCashDiscount(signal) !== null;
    case "finance":
      return signal.interestRatePct !== null || signal.offerTypes.includes("finance_offer");
    case "lease":
      return offerSignalMonthlyPayment(signal) !== null || signal.offerTypes.includes("lease_offer");
    case "benefit":
      return signal.benefitLabels.length > 0 || signal.offerTypes.includes("purchase_benefit");
    case "gap":
      return signal.primaryType === "coverage_gap" || signal.matchStatus === "source_coverage_gap";
    default:
      return false;
  }
}

function offerSignalColumnLabel(signal: MsrpOfferSignal, column: OfferSignalColumnKey): string {
  if (!offerSignalColumnActive(signal, column)) {
    return "-";
  }
  switch (column) {
    case "cash":
      return formatSourcePrice(offerSignalCashDiscount(signal), offerSignalLocalCurrency(signal));
    case "finance":
      return signal.interestRatePct !== null ? `${formatNumber(signal.interestRatePct, 1)}%` : "Finance";
    case "lease":
      return offerSignalMonthlyPayment(signal) !== null
        ? `${formatSourcePrice(offerSignalMonthlyPayment(signal), offerSignalLocalCurrency(signal))}/mo`
        : "Lease";
    case "benefit":
      return signal.benefitLabels.length ? `${signal.benefitLabels.length} item${signal.benefitLabels.length === 1 ? "" : "s"}` : "Benefit";
    case "gap":
      return "Source gap";
    default:
      return "-";
  }
}

function offerSignalColumnRatio(
  signal: MsrpOfferSignal,
  column: OfferSignalColumnKey,
  maxCashDiscount: number,
  maxMonthlyPayment: number,
): number {
  if (!offerSignalColumnActive(signal, column)) {
    return 0;
  }
  switch (column) {
    case "cash":
      return Math.max(0.1, Math.min(1, Number(offerSignalCashDiscount(signal) ?? 0) / Math.max(1, maxCashDiscount)));
    case "lease":
      return Math.max(0.1, Math.min(1, Number(offerSignalMonthlyPayment(signal) ?? 0) / Math.max(1, maxMonthlyPayment)));
    case "benefit":
      return Math.max(0.32, Math.min(1, signal.benefitLabels.length / 3));
    case "finance":
    case "gap":
      return 1;
    default:
      return 0;
  }
}

function offerSignalSortRank(signal: MsrpOfferSignal): number {
  return auditPriorityRank(signal.auditPriority);
}

function offerSignalSortValue(signal: MsrpOfferSignal): number {
  return Math.max(Number(offerSignalCashDiscount(signal) ?? 0), Number(offerSignalMonthlyPayment(signal) ?? 0));
}

function offerSignalValidUntilLabel(signal: MsrpOfferSignal): string {
  return signal.offerValidUntil ? formatTime(signal.offerValidUntil) : "Not captured";
}

function offerSignalEurNormalizedLabel(signal: MsrpOfferSignal): string {
  return signal.matchStatus === "pending_current_price_match" ? "Pending MSRP match" : "Not normalized";
}

function offerSignalLocalCurrencyLabel(signal: MsrpOfferSignal): string {
  const parts = [
    offerSignalCashDiscount(signal) !== null
      ? `Cash ${formatSourcePrice(offerSignalCashDiscount(signal), offerSignalLocalCurrency(signal))}`
      : null,
    signal.interestRatePct !== null ? `Finance ${formatNumber(signal.interestRatePct, 1)}%` : null,
    offerSignalMonthlyPayment(signal) !== null
      ? `Lease ${formatSourcePrice(offerSignalMonthlyPayment(signal), offerSignalLocalCurrency(signal))}/mo`
      : null,
    signal.benefitLabels.length > 0 ? `${signal.benefitLabels.length} benefits` : null,
  ].filter((item): item is string => Boolean(item));
  return parts.length > 0 ? parts.join(" · ") : signal.valueLabel;
}

function countryScopeLabel(
  mode: MonitorMode,
  countryFilter: string,
  countryOptions: Array<[string, string]>,
): string {
  if (mode === "sweden_demo") return "Sweden demo";
  if (mode === "sweden_swiss_demo" && countryFilter === "all") return "Sweden + Swiss demo";
  if (countryFilter === "all") return "All countries";
  return countryOptions.find(([value]) => value === countryFilter)?.[1] ?? countryFilter;
}

function monitorModeLabel(mode: MonitorMode): string {
  return MODE_OPTIONS.find((option) => option.value === mode)?.label ?? "Live";
}

function demoScopeLabel(data: MsrpMonitoringResponse | null): string | null {
  const scope = data?.demo?.scope;
  if (!scope) return null;
  const topN = scope.topN ? `top${scope.topN}` : "top30";
  const segment = scope.segmentFilter || "SUV";
  const latest = scope.sourceLatestMonth ? ` · latest ${scope.sourceLatestMonth}` : "";
  return `Rolling 12M ${segment} ${topN}${latest}`;
}

function isSwedenValue(value: string | null | undefined): boolean {
  const normalized = String(value ?? "").trim().toLowerCase();
  return normalized === "se" || normalized === "sweden" || normalized === "瑞典";
}

function isSwedenTimelineItem(item: MsrpMonitoringTimelineEvent): boolean {
  return isSwedenValue(item.country) || isSwedenValue(item.countryLabel);
}

function isSwedenLaunchAlert(alert: MsrpLaunchAlert): boolean {
  return isSwedenValue(alert.country) || isSwedenValue(alert.countryLabel);
}

function isSwedenOfferSignal(signal: MsrpOfferSignal): boolean {
  return isSwedenValue(signal.country) || isSwedenValue(signal.countryLabel);
}

function isOfficialDropSignal(item: MsrpMonitoringTimelineEvent): boolean {
  return Number(item.changePct ?? 0) < 0
    && Boolean(item.evidence.backfilled)
    && String(item.evidence.backfillKind ?? "").startsWith("official_");
}

function brandScopeLabel(brandFilter: string): string {
  return brandFilter === "all" ? "All brands" : brandFilter;
}

function windowScopeLabel(option: WindowOption): string {
  return option.fromDate ? `${option.label} · from ${option.fromDate}` : option.label;
}

function coverageStatusLabel(status: string): string {
  switch (status) {
    case "backfilled":
      return "Backfilled";
    case "history_without_backfill":
      return "History only";
    case "current_only":
      return "Current only";
    case "not_loaded":
      return "Not loaded";
    default:
      return status || "Unknown";
  }
}

function coverageStatusClass(status: string): string {
  return `is-${String(status || "unknown").replace(/_/g, "-")}`;
}

function auditClass(priority: MsrpAuditPriority | string | null | undefined): string {
  return `is-${String(priority || "unknown").replace(/_/g, "-")}`;
}

function auditPriorityRank(priority: MsrpAuditPriority | string | null | undefined): number {
  switch (priority) {
    case "block":
      return 0;
    case "priority_audit":
      return 1;
    case "sample":
      return 2;
    case "auto_pass":
      return 3;
    default:
      return 4;
  }
}

function higherAuditPriority(
  event: MsrpMonitoringModelEvent,
  item: MsrpMonitoringTimelineEvent,
): EffectiveAudit {
  const eventRank = auditPriorityRank(event.auditPriority);
  const itemRank = auditPriorityRank(item.auditPriority);
  if (eventRank <= itemRank) {
    return {
      priority: event.auditPriority,
      actionLabel: event.auditActionLabel,
      bucket: event.samplingBucket,
      rank: eventRank,
    };
  }
  return {
    priority: item.auditPriority,
    actionLabel: item.auditActionLabel,
    bucket: item.samplingBucket,
    rank: itemRank,
  };
}

function backfillKindLabel(kind: string | null | undefined): string {
  switch (kind) {
    case "official_campaign_vs_regular_price":
      return "Official campaign vs ordinary price";
    case "official_campaign_savings_vs_current_price":
      return "Official campaign savings boundary";
    case "official_promotion_vs_ordinary_price":
      return "Official promotion vs ordinary price";
    case "official_price_list_pdf":
      return "Official historical price list";
    case "official_offer_boundary":
      return "Official offer boundary";
    case "official_offer_boundary_expired":
      return "Expired official offer boundary";
    case "official_generation_transition_baseline":
      return "Official generation transition";
    default:
      return kind ? kind.replace(/_/g, " ") : "Historical backfill";
  }
}

function isCampaignPromotionBackfillKind(kind: string | null | undefined): boolean {
  return (
    kind === "official_campaign_vs_regular_price"
    || kind === "official_campaign_savings_vs_current_price"
    || kind === "official_promotion_vs_ordinary_price"
    || kind === "official_offer_boundary"
    || kind === "official_offer_boundary_expired"
  );
}

function isCampaignSavingsBackfillKind(kind: string | null | undefined): boolean {
  return kind === "official_campaign_savings_vs_current_price";
}

function changePctBasisLabel(basis: string | null | undefined): string {
  if (basis === "source_msrp") return "Local source MSRP";
  if (basis === "eur_normalized") return "EUR normalized";
  if (basis === "mixed") return "Mixed basis";
  return basis ?? "EUR normalized";
}

function changePctBasisShortLabel(basis: string | null | undefined): string {
  if (basis === "source_msrp") return "Local MSRP";
  if (basis === "eur_normalized") return "EUR";
  if (basis === "mixed") return "Mixed";
  return basis ?? "EUR";
}

function isCampaignBoundarySpotCheckItem(item: SpotCheckQueueItem): boolean {
  return isCampaignPromotionBackfillKind(item.item.evidence.backfillKind);
}

function backfillBoundaryLabel(item: MsrpMonitoringTimelineEvent): string {
  const kind = item.evidence.backfillKind ?? "";
  if (isCampaignPromotionBackfillKind(kind)) {
    return "Official price-drop signal; not verified permanent MSRP cut.";
  }
  if (kind === "official_price_list_pdf") {
    return "Official dated price-list evidence.";
  }
  if (kind === "official_generation_transition_baseline") {
    return "Generation-transition evidence; review before treating it as a same-trim price drop.";
  }
  return "Backfilled evidence; spot-check the source before accepting.";
}

function relatedOfficialEvidenceRoleLabel(item: MsrpMonitoringTimelineEvent): string {
  if (isCampaignSavingsBackfillKind(item.evidence.backfillKind)) {
    return "Role: current-price evidence for campaign-savings boundary.";
  }
  return "Role: supporting official evidence.";
}

function snapshotPreviewTitle(snapshotPreviewPathOverride: string | null): string {
  return snapshotPreviewPathOverride ? "Related snapshot preview" : "Primary snapshot preview";
}

function normalizeEvidenceText(value: string): string {
  return value.replace(/\u00a0/g, " ").toLowerCase();
}

function priceEvidenceTokens(value: number | null | undefined): string[] {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return [];
  }
  const rounded = Math.round(value);
  const plain = String(rounded);
  const comma = rounded.toLocaleString("en-US", { maximumFractionDigits: 0 });
  const space = comma.replace(/,/g, " ");
  const dot = comma.replace(/,/g, ".");
  return Array.from(new Set([plain, comma, space, dot]));
}

function evidenceTextHasPrice(text: string, value: number | null | undefined): boolean {
  const normalized = normalizeEvidenceText(text);
  return priceEvidenceTokens(value).some((token) => normalized.includes(token.toLowerCase()));
}

function relatedOfficialEvidenceSummary(item: MsrpMonitoringTimelineEvent): string | null {
  const relatedEvidence = item.evidence.relatedOfficialEvidence ?? [];
  if (relatedEvidence.length === 0) {
    return null;
  }
  return relatedEvidence
    .map((evidence) => {
      const label = evidence.label ?? "Official evidence";
      const source = evidence.url ?? evidence.snapshotPath ?? evidence.payloadHash ?? "";
      return source ? `${label} · ${source}` : label;
    })
    .join(" | ");
}

function relatedOfficialEvidenceArtifactSummary(item: MsrpMonitoringTimelineEvent): string | null {
  const relatedEvidence = item.evidence.relatedOfficialEvidence ?? [];
  const artifacts = relatedEvidence
    .map((evidence) => {
      const label = evidence.label ?? "Official evidence";
      const parts = [evidence.snapshotPath, evidence.payloadHash].filter(Boolean);
      return parts.length > 0 ? `${label} · ${parts.join(" · ")}` : null;
    })
    .filter((value): value is string => Boolean(value));
  return artifacts.length > 0 ? artifacts.join(" | ") : null;
}

function buildEvidenceChecklist(
  item: MsrpMonitoringTimelineEvent,
  preview: MsrpBackfillSnapshotPreview | null,
  loading: boolean,
): EvidenceChecklistItem[] {
  const content = preview?.content ?? "";
  const hasPreviewContent = Boolean(preview?.previewable && content);
  const currentPriceMatched = hasPreviewContent && evidenceTextHasPrice(content, item.currentSourceMsrp);
  const previousPriceMatched = hasPreviewContent && evidenceTextHasPrice(content, item.oldSourceMsrp);
  const savingsAmount = item.changeAmountSource !== null && item.changeAmountSource !== undefined
    ? Math.abs(item.changeAmountSource)
    : null;
  const savingsAmountMatched = hasPreviewContent && evidenceTextHasPrice(content, savingsAmount);
  const campaignSavingsBoundary = isCampaignSavingsBackfillKind(item.evidence.backfillKind);
  const hasRelatedOfficialEvidence = Boolean(item.evidence.relatedOfficialEvidence?.length);
  const currentPriceSupported = currentPriceMatched || (campaignSavingsBoundary && hasRelatedOfficialEvidence);
  const previousBoundarySupported = campaignSavingsBoundary ? savingsAmountMatched : previousPriceMatched;
  const officialKind = Boolean(item.evidence.backfillKind?.startsWith("official_"));
  const boundaryKnown = isCampaignPromotionBackfillKind(item.evidence.backfillKind)
    || item.evidence.backfillKind === "official_price_list_pdf";
  const currentPriceStatus: EvidenceChecklistStatus = loading
    ? "pending"
    : currentPriceSupported
      ? "pass"
      : "warn";

  return [
    {
      key: "snapshot",
      label: "Primary snapshot readable",
      status: loading ? "pending" : hasPreviewContent ? "pass" : "warn",
      detail: loading ? "Loading snapshot preview" : preview?.message ?? "No snapshot preview loaded",
    },
    {
      key: "current-price",
      label: campaignSavingsBoundary ? "Current price source" : "Current price found",
      status: currentPriceStatus,
      detail: campaignSavingsBoundary && hasRelatedOfficialEvidence && !currentPriceMatched
        ? `Related official evidence attached for ${formatNumber(item.currentSourceMsrp)} ${item.sourceCurrency}`
        : `${formatNumber(item.currentSourceMsrp)} ${item.sourceCurrency}`,
    },
    {
      key: "previous-price",
      label: campaignSavingsBoundary ? "Savings amount found" : "Previous price found",
      status: loading ? "pending" : previousBoundarySupported ? "pass" : "warn",
      detail: campaignSavingsBoundary
        ? `${formatNumber(savingsAmount)} ${item.previousSourceCurrency || item.sourceCurrency}`
        : `${formatNumber(item.oldSourceMsrp)} ${item.previousSourceCurrency || item.sourceCurrency}`,
    },
    {
      key: "official-evidence",
      label: "Official evidence",
      status: officialKind && (Boolean(item.evidence.backfillEvidenceUrl) || hasRelatedOfficialEvidence) ? "pass" : "warn",
      detail: item.evidence.backfillEvidenceUrl
        ?? relatedOfficialEvidenceSummary(item)
        ?? item.evidence.backfillKind
        ?? "Missing official evidence URL",
    },
    {
      key: "boundary",
      label: "Price boundary",
      status: boundaryKnown ? "pass" : "warn",
      detail: backfillBoundaryLabel(item),
    },
  ];
}

function buildSpotCheckDecision(
  item: MsrpMonitoringTimelineEvent,
  checklist: EvidenceChecklistItem[],
  loading: boolean,
): SpotCheckDecision {
  const warningLabels = checklist.filter((check) => check.status === "warn").map((check) => check.label);
  const pendingCount = checklist.filter((check) => check.status === "pending").length;
  const kind = item.evidence.backfillKind ?? "";
  const campaignSavingsBoundary = isCampaignSavingsBackfillKind(kind);

  if (loading || pendingCount > 0) {
    return {
      label: "Wait for evidence preview",
      className: "is-pending",
      detail: "Snapshot evidence is still loading, so the movement is not ready for acceptance.",
      actions: [
        "Wait for snapshot preview",
        "Confirm old and current price tokens both match",
      ],
    };
  }

  if (warningLabels.length > 0) {
    return {
      label: "Source check required",
      className: "is-warn",
      detail: `Resolve weak evidence before accepting: ${warningLabels.join(", ")}.`,
      actions: [
        "Open evidence URL or snapshot",
        "Verify trim, currency, old price and current price",
      ],
    };
  }

  if (isCampaignPromotionBackfillKind(kind)) {
    return {
      label: "Campaign boundary",
      className: "is-warn",
      detail: "Official price-drop signal; keep it as campaign/promotion evidence until a dated price list proves a permanent MSRP cut.",
      actions: campaignSavingsBoundary
        ? [
          "Confirm saving amount in the offer source",
          "Confirm current price in related official evidence",
          "Keep classification as campaign/promotion boundary",
        ]
        : [
          "Confirm ordinary price and campaign price in the official source",
          "Keep classification as campaign/promotion boundary",
          "Escalate only dated price-list evidence as permanent MSRP cut",
        ],
    };
  }

  if (kind === "official_price_list_pdf") {
    return {
      label: "Accept official movement",
      className: "is-pass",
      detail: "Checklist is clean and the evidence boundary is a dated official price list.",
      actions: [
        "Spot-check dated price list",
        "Accept movement if trim and currency match",
      ],
    };
  }

  if (item.evidence.backfilled) {
    return {
      label: "Backfill boundary check",
      className: "is-warn",
      detail: "Backfilled evidence is present, but the price boundary needs a human source check.",
      actions: [
        "Verify evidence boundary",
        "Confirm old and current price context",
      ],
    };
  }

  return {
    label: "Needs historical backfill",
    className: "is-warn",
    detail: "Live scrape gives the current price only; historical evidence is required before calling this a real 2026 drop.",
    actions: [
      "Find official historical price evidence",
      "Mark as current observation until backfill exists",
    ],
  };
}

function spotCheckOutcomeLine(
  item: MsrpMonitoringTimelineEvent,
  decision: SpotCheckDecision,
): string {
  const kind = item.evidence.backfillKind ?? "";
  if (item.evidence.demoBackfilled && item.evidence.backfilled && isCampaignPromotionBackfillKind(kind)) {
    return "Outcome: official evidence boundary in demo mode; keep out of permanent MSRP-cut conclusions until production backfill is accepted.";
  }
  if (item.evidence.demoBackfilled) {
    return "Outcome: demo monitor only; do not use as a real market conclusion.";
  }
  if (isCampaignPromotionBackfillKind(kind)) {
    return "Outcome: keep as campaign/promotion boundary; do not accept as permanent MSRP cut without dated official price-list evidence.";
  }
  if (kind === "official_price_list_pdf") {
    return "Outcome: acceptable official movement after trim, currency and date spot-check.";
  }
  if (item.evidence.backfilled) {
    return "Outcome: keep in source-check queue until the historical price boundary is verified.";
  }
  return `Outcome: ${decision.detail}`;
}

function buildSpotCheckQueueDecision(item: MsrpMonitoringTimelineEvent): SpotCheckDecision {
  const kind = item.evidence.backfillKind ?? "";

  if (item.evidence.demoBackfilled && item.evidence.backfilled && isCampaignPromotionBackfillKind(kind)) {
    return {
      label: "Demo boundary",
      className: "is-warn",
      detail: "Official evidence-backed demo boundary; source-check before treating it as a permanent MSRP cut.",
      actions: ["Open Source", "Verify official boundary"],
    };
  }

  if (item.evidence.demoBackfilled) {
    return {
      label: "Demo only",
      className: "is-warn",
      detail: "Demo monitor only; replace with accepted production backfill before market reporting.",
      actions: ["Open Source", "Replace with official evidence"],
    };
  }

  if (isCampaignPromotionBackfillKind(kind)) {
    return {
      label: "Campaign boundary",
      className: "is-warn",
      detail: "Official campaign/promotion price-drop signal; verify the boundary before calling it a permanent MSRP cut.",
      actions: ["Open Source", "Check campaign boundary"],
    };
  }

  if (kind === "official_price_list_pdf") {
    return {
      label: "Official movement",
      className: "is-pass",
      detail: "Dated official price-list evidence; spot-check trim and currency before accepting.",
      actions: ["Open Source", "Confirm trim and currency"],
    };
  }

  if (item.evidence.backfilled) {
    return {
      label: "Boundary check",
      className: "is-warn",
      detail: "Historical evidence exists, but the price boundary still needs a source check.",
      actions: ["Open Source", "Verify boundary"],
    };
  }

  return {
    label: "Needs backfill",
    className: "is-warn",
    detail: "No historical evidence attached; keep as current observation until backfilled.",
    actions: ["Find official history", "Open Source"],
  };
}

function buildSpotCheckBrief(
  event: MsrpMonitoringModelEvent,
  item: MsrpMonitoringTimelineEvent,
  audit: EffectiveAudit | null,
  checklist: EvidenceChecklistItem[],
  preview: MsrpBackfillSnapshotPreview | null,
  loading: boolean,
): SpotCheckBrief {
  const matchedCount = checklist.filter((check) => check.status === "pass").length;
  const warningLabels = checklist.filter((check) => check.status === "warn").map((check) => check.label);
  const pendingCount = checklist.filter((check) => check.status === "pending").length;
  const decision = buildSpotCheckDecision(item, checklist, loading);
  const outcomeLine = spotCheckOutcomeLine(item, decision);
  const cleanDecisionNeedsReview = warningLabels.length === 0 && decision.className === "is-warn";
  const statusLabel = pendingCount > 0
    ? "Evidence loading"
    : warningLabels.length > 0
      ? "Needs source check"
      : cleanDecisionNeedsReview
        ? `${decision.label} review`
        : "Ready for spot-check";
  const statusClassName = pendingCount > 0
    ? "is-pending"
    : warningLabels.length > 0 || cleanDecisionNeedsReview
      ? "is-warn"
      : "is-pass";
  const sourceUrl = item.evidence.backfillEvidenceUrl
    ?? item.evidence.observationSourceUrl
    ?? item.evidence.sourceUrl
    ?? "-";
  const relatedEvidenceSummary = relatedOfficialEvidenceSummary(item);
  const relatedEvidenceArtifactSummary = relatedOfficialEvidenceArtifactSummary(item);
  const backfillArtifactSummary = item.evidence.backfillSnapshotPath
    ? `${item.evidence.backfillSnapshotPath}${item.evidence.backfillPayloadHash ? ` · ${item.evidence.backfillPayloadHash}` : ""}`
    : item.evidence.backfillPayloadHash ?? null;
  const snapshotStatus = item.evidence.backfillSnapshotPath
    ? loading
      ? "loading"
      : preview
        ? `${preview.fileName} (${preview.status}${preview.truncated ? ", truncated" : ""})`
        : item.evidence.backfillSnapshotPath
    : "-";
  const validUntilLine = item.evidence.backfillValidUntil ? ` · Valid until: ${item.evidence.backfillValidUntil}` : "";
  const lines = [
    `Model: ${eventLabel(event)} · ${item.jatoTrim || "trim"} · ${item.countryLabel}`,
    `Change: ${formatNumber(item.oldSourceMsrp)} -> ${formatNumber(item.currentSourceMsrp)} ${item.sourceCurrency} (${formatPct(item.changePct)} · ${changePctBasisLabel(item.changePctBasis)})`,
    `EUR normalized: ${formatCurrency(item.oldMsrpEur)} -> ${formatCurrency(item.currentMsrpEur)}`,
    `Observed: ${formatDateTime(item.evidence.observedAtUtc)} · Effective: ${item.evidence.backfillEffectiveDate ?? formatTime(item.changedAtUtc)}${validUntilLine}`,
    `Audit: ${auditLabel(audit?.priority ?? item.auditPriority)} · ${audit?.actionLabel ?? item.auditActionLabel}`,
    `Evidence: ${backfillKindLabel(item.evidence.backfillKind)} · ${sourceUrl}`,
    ...(backfillArtifactSummary ? [`Backfill artifact: ${backfillArtifactSummary}`] : []),
    ...(relatedEvidenceSummary ? [`Related official evidence: ${relatedEvidenceSummary}`] : []),
    ...(relatedEvidenceArtifactSummary ? [`Related evidence artifacts: ${relatedEvidenceArtifactSummary}`] : []),
    `Primary snapshot: ${snapshotStatus}`,
    `Checklist: ${matchedCount}/${checklist.length} passed${warningLabels.length > 0 ? ` · check ${warningLabels.join(", ")}` : ""}`,
    `Decision: ${decision.label} - ${decision.detail}`,
    outcomeLine,
    `Next: ${decision.actions.join(" | ")}`,
    `Boundary: ${backfillBoundaryLabel(item)}`,
  ];

  return {
    decision,
    statusLabel,
    statusClassName,
    matchedCount,
    totalCount: checklist.length,
    warningLabels,
    lines,
  };
}

async function copyTextToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "true");
    textarea.style.position = "fixed";
    textarea.style.left = "-9999px";
    textarea.style.top = "0";
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    try {
      return document.execCommand("copy");
    } finally {
      document.body.removeChild(textarea);
    }
  }
}

function domain(values: number[], fallback: [number, number], paddingRatio = 0.08): [number, number] {
  if (values.length === 0) {
    return fallback;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = Math.max(1, max - min);
  return [min - span * paddingRatio, max + span * paddingRatio];
}

function niceTicks(min: number, max: number, count: number): number[] {
  const span = Math.max(1, max - min);
  const rawStep = span / Math.max(1, count - 1);
  const power = 10 ** Math.floor(Math.log10(rawStep));
  const ratio = rawStep / power;
  const step = (ratio <= 1 ? 1 : ratio <= 2 ? 2 : ratio <= 5 ? 5 : 10) * power;
  const start = Math.ceil(min / step) * step;
  const ticks: number[] = [];
  for (let value = start; value <= max + step * 0.5; value += step) {
    ticks.push(Math.round(value));
  }
  return ticks;
}

function timelineItemMatchesDirection(item: MsrpMonitoringTimelineEvent, direction: DirectionFilter): boolean {
  if (direction === "all") {
    return true;
  }
  const change = item.changePct ?? 0;
  return direction === "drops" ? change < 0 : change > 0;
}

function timelineItemMatchesEvidence(item: MsrpMonitoringTimelineEvent, evidence: EvidenceFilter): boolean {
  const kind = item.evidence.backfillKind ?? "";
  switch (evidence) {
    case "all":
      return true;
    case "official_backfill":
      return Boolean(item.evidence.backfilled && kind.startsWith("official_"));
    case "campaign_promotion":
      return isCampaignPromotionBackfillKind(kind);
    case "no_backfill":
      return !item.evidence.backfilled && !item.evidence.demoBackfilled;
    case "demo_backfill":
      return Boolean(item.evidence.demoBackfilled);
    default:
      return true;
  }
}

function timelineItemMatchesAudit(
  event: MsrpMonitoringModelEvent,
  item: MsrpMonitoringTimelineEvent,
  audit: AuditFilter,
): boolean {
  return audit === "all" || higherAuditPriority(event, item).priority === audit;
}

function timelineItemMatchesFilters(
  event: MsrpMonitoringModelEvent,
  item: MsrpMonitoringTimelineEvent,
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): boolean {
  return timelineItemMatchesDirection(item, direction)
    && timelineItemMatchesEvidence(item, evidence)
    && timelineItemMatchesAudit(event, item, audit);
}

function eventMatchesFilters(
  event: MsrpMonitoringModelEvent,
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): boolean {
  return event.timeline.some((item) => timelineItemMatchesFilters(event, item, direction, evidence, audit));
}

function filteredTimelineItems(
  events: MsrpMonitoringModelEvent[],
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): MsrpMonitoringTimelineEvent[] {
  return events
    .flatMap((event) => event.timeline
      .filter((item) => timelineItemMatchesFilters(event, item, direction, evidence, audit))
      .map((item) => ({ ...item, brand: event.brand, jatoModel: event.jatoModel })))
    .sort((left, right) => String(left.changedAtUtc ?? "").localeCompare(String(right.changedAtUtc ?? "")));
}

function buildSpotCheckQueueCandidates(
  events: MsrpMonitoringModelEvent[],
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): SpotCheckQueueItem[] {
  return events
    .flatMap((event) => event.timeline
      .filter((item) => timelineItemMatchesFilters(event, item, direction, evidence, audit))
      .map((item) => {
        const effectiveAudit = higherAuditPriority(event, item);
        return {
          key: `${event.eventId}|${countryKey(item)}`,
          eventId: event.eventId,
          modelLabel: eventLabel(event),
          item,
          decision: buildSpotCheckQueueDecision(item),
          effectivePriority: effectiveAudit.priority,
          effectiveActionLabel: effectiveAudit.actionLabel,
          priorityRank: effectiveAudit.rank,
          absChangePct: Math.abs(item.changePct ?? 0),
        };
      }))
    .sort((left, right) => (
      left.priorityRank - right.priorityRank
      || Number(right.item.evidence.backfilled) - Number(left.item.evidence.backfilled)
      || right.absChangePct - left.absChangePct
      || String(right.item.changedAtUtc ?? "").localeCompare(String(left.item.changedAtUtc ?? ""))
      || left.modelLabel.localeCompare(right.modelLabel)
    ));
}

function visibleSpotCheckQueue(items: SpotCheckQueueItem[]): SpotCheckQueueItem[] {
  return items.slice(0, SPOT_CHECK_QUEUE_LIMIT);
}

function buildSpotCheckDecisionSummary(items: SpotCheckQueueItem[]): SpotCheckDecisionSummary[] {
  const grouped = new Map<string, SpotCheckDecisionSummary>();
  items.forEach((queueItem) => {
    const key = `${queueItem.decision.className}|${queueItem.decision.label}`;
    const existing = grouped.get(key);
    if (existing) {
      existing.count += 1;
      return;
    }
    grouped.set(key, {
      key,
      label: queueItem.decision.label,
      className: queueItem.decision.className,
      count: 1,
      detail: queueItem.decision.detail,
      firstItemKey: queueItem.key,
    });
  });
  return Array.from(grouped.values()).sort((left, right) => (
    right.count - left.count
    || left.label.localeCompare(right.label)
  ));
}

function isChartDrawableEvent(event: MsrpMonitoringModelEvent): boolean {
  return event.lengthMm !== null
    && event.medianCurrentMsrpEur !== null
    && event.medianOldMsrpEur !== null;
}

function isPriceChartEvent(event: MsrpMonitoringModelEvent): boolean {
  return event.medianCurrentMsrpEur !== null
    && event.medianOldMsrpEur !== null;
}

function defaultSelectedModelEvent(events: MsrpMonitoringModelEvent[]): MsrpMonitoringModelEvent | null {
  return events.find(isChartDrawableEvent) ?? events.find(isPriceChartEvent) ?? events[0] ?? null;
}

function eventEvidenceLabel(item: MsrpMonitoringTimelineEvent): string {
  if (item.evidence.backfilled) {
    return item.evidence.backfillSourceLabel ?? item.evidence.backfillKind ?? "Historical backfill";
  }
  if (item.evidence.demoBackfilled) {
    return item.evidence.demoScenario ?? "Demo backfill";
  }
  return item.evidence.dryrunRunId ?? item.evidence.scrapeBatchCode ?? item.currentObservationId;
}

function formatSourcePrice(value: number | null | undefined, currency: string | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return `${formatNumber(value)} ${currency || ""}`.trim();
}

function sourcePriceTransitionLabel(item: MsrpMonitoringTimelineEvent): string {
  if (item.oldSourceMsrp !== null || item.currentSourceMsrp !== null) {
    return `${formatSourcePrice(item.oldSourceMsrp, item.previousSourceCurrency || item.sourceCurrency)} -> ${formatSourcePrice(item.currentSourceMsrp, item.sourceCurrency)}`;
  }
  return `${formatCurrency(item.oldMsrpEur)} -> ${formatCurrency(item.currentMsrpEur)}`;
}

function launchAlertLabel(alert: MsrpLaunchAlert): string {
  return `${alert.brand} ${alert.jatoModel}`;
}

function priceActionMoveTypeLabel(item: MsrpMonitoringTimelineEvent): string {
  if ((item.changePct ?? 0) < 0) {
    return "MSRP drop";
  }
  if ((item.changePct ?? 0) > 0) {
    return "MSRP increase";
  }
  return "MSRP move";
}

function buildPriceActionItems(queueItems: SpotCheckQueueItem[]): PriceActionItem[] {
  const moveItems: PriceActionMoveItem[] = queueItems.map((queueItem) => ({
    key: `move:${queueItem.key}`,
    kind: "msrp_move",
    modelLabel: queueItem.modelLabel,
    countryLabel: queueItem.item.countryLabel,
    trimLabel: queueItem.item.jatoTrim || "trim",
    actionTypeLabel: priceActionMoveTypeLabel(queueItem.item),
    primaryMetric: formatPct(queueItem.item.changePct),
    secondaryMetric: sourcePriceTransitionLabel(queueItem.item),
    evidenceLabel: eventEvidenceLabel(queueItem.item),
    statusLabel: queueItem.decision.label,
    statusDetail: queueItem.decision.detail,
    auditPriority: queueItem.effectivePriority,
    changedAtLabel: formatTime(queueItem.item.changedAtUtc),
    sortRank: queueItem.priorityRank,
    sortMagnitude: Math.max(
      Math.abs(queueItem.item.changeAmountSource ?? 0),
      Math.abs(queueItem.item.changeAmountEur ?? 0),
      queueItem.absChangePct,
    ),
    queueItem,
  }));
  return moveItems.sort((left, right) => (
    left.sortRank - right.sortRank
    || right.sortMagnitude - left.sortMagnitude
    || left.modelLabel.localeCompare(right.modelLabel)
    || left.countryLabel.localeCompare(right.countryLabel)
  ));
}

function impactDateLabel(value: string | null | undefined): string {
  return formatTime(value);
}

function impactDurationDays(startValue: string | null | undefined): number {
  if (!startValue) {
    return 1;
  }
  const start = new Date(startValue);
  if (Number.isNaN(start.getTime())) {
    return 1;
  }
  const now = new Date();
  return Math.max(1, Math.ceil((now.getTime() - start.getTime()) / 86_400_000));
}

function formatImpactValue(value: number, valueKind: PriceImpactModel["valueKind"], valueCurrency?: string): string {
  if (valueKind === "local") {
    return formatSourcePrice(value, valueCurrency);
  }
  if (valueKind === "eur") {
    return formatCurrency(value);
  }
  return formatNumber(value, 1);
}

function impactRangeLabel(effect: number): string {
  const lower = effect * 0.82;
  const upper = effect * 1.18;
  return `${formatNumber(lower, 0)} to ${formatNumber(upper, 0)}`;
}

function buildImpactPoints(expected: number, observed: number, durationDays: number): ImpactSeriesPoint[] {
  const effect = expected - observed;
  const dayLabels = ["Pre", "Launch", "Mid", "Latest"];
  const cumulativeSteps = [0, 0.34, 0.68, 1];
  return dayLabels.map((label, index) => {
    const postLaunch = index > 0;
    const actual = postLaunch ? observed : expected;
    const pointEffect = postLaunch ? effect : 0;
    return {
      label,
      actual,
      expected,
      effect: pointEffect,
      cumulative: effect * durationDays * cumulativeSteps[index],
    };
  });
}

function buildMoveImpactModel(action: PriceActionMoveItem): PriceImpactModel {
  const item = action.queueItem.item;
  const expected = Number(item.oldSourceMsrp ?? item.oldMsrpEur ?? 0);
  const actual = Number(item.currentSourceMsrp ?? item.currentMsrpEur ?? expected);
  const valueKind: PriceImpactModel["valueKind"] = item.oldSourceMsrp !== null || item.currentSourceMsrp !== null ? "local" : "eur";
  const valueCurrency = valueKind === "local" ? item.sourceCurrency : undefined;
  const durationDays = impactDurationDays(item.changedAtUtc);
  const effect = expected - actual;
  const avgPerDay = effect;

  return {
    title: "Price impact view",
    subtitle: `${action.modelLabel} · ${item.countryLabel} · ${impactDateLabel(item.changedAtUtc)}`,
    metricLabel: valueKind === "local" ? `Local MSRP (${item.sourceCurrency})` : "MSRP EUR normalized",
    valueKind,
    valueCurrency,
    effectLabel: formatImpactValue(effect, valueKind, valueCurrency),
    effectPctLabel: formatPct(item.changePct),
    rangeLabel: impactRangeLabel(effect),
    avgPerDayLabel: formatImpactValue(avgPerDay, valueKind, valueCurrency),
    durationLabel: `${durationDays} days`,
    verdictLabel: action.queueItem.decision.label,
    baselineLabel: "Expected = previous official price baseline",
    actualLabel: "Actual = latest observed price",
    expectedLabel: "Evidence baseline, not causal forecast",
    notes: `${action.queueItem.decision.detail} ${backfillBoundaryLabel(item)}`,
    points: buildImpactPoints(expected, actual, durationDays),
  };
}

function buildOfferImpactModel(action: PriceActionOfferItem): PriceImpactModel {
  const signal = action.signal;
  const cashDiscount = offerSignalCashDiscount(signal);
  const monthlyPayment = offerSignalMonthlyPayment(signal);
  const effect = Number(cashDiscount ?? monthlyPayment ?? signal.benefitLabels.length);
  const durationDays = impactDurationDays(signal.capturedAtUtc || signal.sourceObservedDate);
  const valueKind: PriceImpactModel["valueKind"] = cashDiscount !== null || monthlyPayment !== null ? "local" : "count";
  const valueCurrency = valueKind === "local" ? offerSignalLocalCurrency(signal) : undefined;

  return {
    title: "Offer impact view",
    subtitle: `${action.modelLabel} · ${signal.countryLabel} · ${signal.sourceObservedDate}`,
    metricLabel: cashDiscount !== null ? "Price gap vs no-offer baseline" : monthlyPayment !== null ? "Monthly offer gap vs baseline" : "Benefit count",
    valueKind,
    valueCurrency,
    effectLabel: formatImpactValue(effect, valueKind, valueCurrency),
    effectPctLabel: offerSignalMatchStatusLabel(signal.matchStatus),
    rangeLabel: impactRangeLabel(effect),
    avgPerDayLabel: formatImpactValue(effect, valueKind, valueCurrency),
    durationLabel: `${durationDays} days`,
    verdictLabel: auditLabel(signal.auditPriority),
    baselineLabel: "Expected = no official incentive signal",
    actualLabel: "Actual = observed official offer signal",
    expectedLabel: "Offer baseline, pending MSRP match",
    notes: signal.notes,
    points: buildImpactPoints(0, -effect, durationDays),
  };
}

interface MsrpOfferSignalVisualProps {
  signals: MsrpOfferSignal[];
  selectedSignalId: string | null;
  onSelect: (signal: MsrpOfferSignal) => void;
}

function MsrpOfferSignalVisual({ signals, selectedSignalId, onSelect }: MsrpOfferSignalVisualProps) {
  const sortedSignals = [...signals].sort((left, right) => (
    offerSignalSortRank(left) - offerSignalSortRank(right)
    || offerSignalSortValue(right) - offerSignalSortValue(left)
    || left.brand.localeCompare(right.brand)
    || left.jatoModel.localeCompare(right.jatoModel)
  ));
  const maxCashDiscount = Math.max(0, ...sortedSignals.map((signal) => Number(offerSignalCashDiscount(signal) ?? 0)));
  const maxMonthlyPayment = Math.max(0, ...sortedSignals.map((signal) => Number(offerSignalMonthlyPayment(signal) ?? 0)));
  const cashCount = sortedSignals.filter((signal) => offerSignalColumnActive(signal, "cash")).length;
  const financeCount = sortedSignals.filter((signal) => offerSignalColumnActive(signal, "finance")).length;
  const leaseCount = sortedSignals.filter((signal) => offerSignalColumnActive(signal, "lease")).length;
  const gapCount = sortedSignals.filter((signal) => offerSignalColumnActive(signal, "gap")).length;

  return (
    <section className="msrp-monitor-offer-visual" aria-label="Official offer signal visualization">
      <header>
        <div>
          <h2>Offer signal map</h2>
          <p>Official incentives are leads pending MSRP match; they are not counted as spot-check price movements.</p>
        </div>
        <div className="msrp-monitor-offer-visual-kpis">
          <span><strong>{cashCount}</strong> cash</span>
          <span><strong>{financeCount}</strong> finance</span>
          <span><strong>{leaseCount}</strong> lease</span>
          <span><strong>{gapCount}</strong> gaps</span>
        </div>
      </header>
      {sortedSignals.length > 0 ? (
        <div className="msrp-monitor-offer-heatmap">
          <div className="msrp-monitor-offer-heatmap-head" aria-hidden="true">
            <span>Model</span>
            {OFFER_SIGNAL_COLUMNS.map((column) => <span key={column.key}>{column.label}</span>)}
          </div>
          <div className="msrp-monitor-offer-heatmap-body">
            {sortedSignals.map((signal) => (
              <button
                key={signal.signalId}
                type="button"
                className={`msrp-monitor-offer-heatmap-row ${auditClass(signal.auditPriority)}${signal.signalId === selectedSignalId ? " is-selected" : ""}`}
                onClick={() => onSelect(signal)}
              >
                <span className="msrp-monitor-offer-heatmap-model">
                  <strong>{signal.brand} {signal.jatoModel}</strong>
                  <small>{offerSignalMetric(signal)} · {offerSignalMatchStatusLabel(signal.matchStatus)}</small>
                </span>
                {OFFER_SIGNAL_COLUMNS.map((column) => {
                  const active = offerSignalColumnActive(signal, column.key);
                  const ratio = offerSignalColumnRatio(signal, column.key, maxCashDiscount, maxMonthlyPayment);
                  return (
                    <span
                      key={`${signal.signalId}-${column.key}`}
                      className={`msrp-monitor-offer-heatmap-cell is-${column.key}${active ? " is-active" : ""}`}
                      title={`${column.label}: ${offerSignalColumnLabel(signal, column.key)}`}
                    >
                      <i style={{ width: `${Math.round(ratio * 100)}%` }} />
                      <b>{offerSignalColumnLabel(signal, column.key)}</b>
                    </span>
                  );
                })}
              </button>
            ))}
          </div>
        </div>
      ) : (
        <div className="msrp-monitor-empty-block">No official offer signals match the current filter scope.</div>
      )}
    </section>
  );
}

interface MsrpOfferSignalCardProps {
  signal: MsrpOfferSignal;
  selected: boolean;
  onSelect: () => void;
}

function MsrpOfferSignalCard({ signal, selected, onSelect }: MsrpOfferSignalCardProps) {
  return (
    <button
      type="button"
      className={`msrp-monitor-offer-card ${auditClass(signal.auditPriority)}${selected ? " is-selected" : ""}`}
      onClick={onSelect}
    >
      <span>
        <strong>{signal.brand} {signal.jatoModel}</strong>
        <small>{signal.countryLabel} · {signal.jatoTrim}</small>
      </span>
      <b>
        {offerSignalMetric(signal)}
        <small>{offerSignalMatchStatusLabel(signal.matchStatus)}</small>
      </b>
      <em>{signal.offerTypes.map(offerTypeLabel).join(" · ")}</em>
      <i>{auditLabel(signal.auditPriority)}</i>
      {signal.benefitLabels.length > 0 ? (
        <small>{signal.benefitLabels.map(offerTypeLabel).join(" · ")}</small>
      ) : null}
    </button>
  );
}

function MsrpOfferSignalSource({ signal }: { signal: MsrpOfferSignal }) {
  return (
    <div className={`msrp-monitor-offer-source ${auditClass(signal.auditPriority)}`}>
      <header>
        <div>
          <strong>{signal.headline}</strong>
          <span>{signal.brand} {signal.jatoModel} · {signal.countryLabel} · {samplingBucketLabel(signal.samplingBucket)}</span>
        </div>
        <b>{offerSignalMetric(signal)}</b>
      </header>
      <div className="msrp-monitor-offer-source-tags">
        {signal.offerTypes.map((type) => <span key={type}>{offerTypeLabel(type)}</span>)}
        <span>{auditLabel(signal.auditPriority)}</span>
        <span>{offerSignalMatchStatusLabel(signal.matchStatus)}</span>
      </div>
      <dl>
        <dt>Value</dt><dd>{signal.valueLabel}</dd>
        <dt>Cash discount</dt><dd>{formatSourcePrice(offerSignalCashDiscount(signal), offerSignalLocalCurrency(signal))}</dd>
        <dt>Interest</dt><dd>{signal.interestRatePct !== null ? `${formatNumber(signal.interestRatePct, 1)}%` : "-"}</dd>
        <dt>Monthly</dt><dd>{offerSignalMonthlyPayment(signal) !== null ? `${formatSourcePrice(offerSignalMonthlyPayment(signal), offerSignalLocalCurrency(signal))}/month` : "-"}</dd>
        <dt>Benefits</dt><dd>{signal.benefitLabels.length ? signal.benefitLabels.join(", ") : "-"}</dd>
        <dt>Observed</dt><dd>{signal.sourceObservedDate}</dd>
        <dt>Valid until</dt><dd>{offerSignalValidUntilLabel(signal)}</dd>
        <dt>Status</dt><dd>{offerSignalMatchStatusLabel(signal.matchStatus)}</dd>
        <dt>Source</dt>
        <dd><a href={signal.sourceUrl} target="_blank" rel="noreferrer">{signal.sourceLabel}</a></dd>
      </dl>
      <p>{signal.notes}</p>
    </div>
  );
}

interface MsrpOfferDeckProps {
  signals: MsrpOfferSignal[];
  selectedSignalId: string | null;
  countryValue: string;
  countryOptions: Array<[string, string]>;
  onCountryChange: (value: string) => void;
  onSelect: (signal: MsrpOfferSignal) => void;
}

function MsrpOfferDeck({
  signals,
  selectedSignalId,
  countryValue,
  countryOptions,
  onCountryChange,
  onSelect,
}: MsrpOfferDeckProps) {
  const selectedSignal = signals.find((signal) => signal.signalId === selectedSignalId) ?? signals[0] ?? null;

  return (
    <div className="msrp-monitor-offer-deck">
      <div className="msrp-monitor-offer-deck-toolbar">
        <label>
          <span>Country</span>
          <select value={countryValue} onChange={(event) => onCountryChange(event.target.value)}>
            <option value="all">All</option>
            {countryOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
          </select>
        </label>
        <strong>{signals.length} offer leads</strong>
      </div>
      {signals.length > 0 ? (
        <div className="msrp-monitor-offer-deck-list">
          {signals.map((signal) => (
            <button
              key={signal.signalId}
              type="button"
              className={`${auditClass(signal.auditPriority)}${signal.signalId === selectedSignal?.signalId ? " is-selected" : ""}`}
              onClick={() => onSelect(signal)}
            >
              <span>
                <strong>{signal.brand} {signal.jatoModel}</strong>
                <small>{signal.countryLabel} · valid until {offerSignalValidUntilLabel(signal)}</small>
              </span>
              <b>{offerSignalMetric(signal)}</b>
            </button>
          ))}
        </div>
      ) : (
        <div className="msrp-monitor-empty-block">No offer signals match the current country and filter scope.</div>
      )}
      {selectedSignal ? (
        <div className="msrp-monitor-offer-drilldown">
          <header>
            <div>
              <h3>{selectedSignal.brand} {selectedSignal.jatoModel}</h3>
              <span>{selectedSignal.countryLabel} · {selectedSignal.jatoTrim}</span>
            </div>
            <strong>{offerSignalMetric(selectedSignal)}</strong>
          </header>
          <div className="msrp-monitor-offer-drilldown-grid">
            <div>
              <span>EUR NORMALIZED</span>
              <strong>{offerSignalEurNormalizedLabel(selectedSignal)}</strong>
              <small>{offerSignalMatchStatusLabel(selectedSignal.matchStatus)}</small>
            </div>
            <div>
              <span>LOCAL CURRENCY</span>
              <strong>{offerSignalLocalCurrencyLabel(selectedSignal)}</strong>
              <small>{selectedSignal.valueLabel}</small>
            </div>
            <div>
              <span>SOURCE</span>
              <strong>{selectedSignal.sourceLabel}</strong>
              <small>{selectedSignal.sourceObservedDate}</small>
            </div>
            <div>
              <span>EVIDENCE</span>
              <strong>{auditLabel(selectedSignal.auditPriority)}</strong>
              <small>{selectedSignal.notes}</small>
            </div>
          </div>
          <div className="msrp-monitor-offer-timeline" aria-label="Offer signal timeline">
            <span>
              <b>Observed</b>
              <strong>{formatTime(selectedSignal.sourceObservedDate)}</strong>
            </span>
            <span>
              <b>Captured</b>
              <strong>{formatTime(selectedSignal.capturedAtUtc)}</strong>
            </span>
            <span className={selectedSignal.offerValidUntil ? "is-active" : ""}>
              <b>Valid until</b>
              <strong>{offerSignalValidUntilLabel(selectedSignal)}</strong>
            </span>
          </div>
          <MsrpOfferSignalSource signal={selectedSignal} />
        </div>
      ) : null}
    </div>
  );
}

interface PriceActionBoardProps {
  items: PriceActionItem[];
  totalCount: number;
  selectedKey: string | null;
  onSelect: (item: PriceActionItem) => void;
}

function PriceActionBoard({ items, totalCount, selectedKey, onSelect }: PriceActionBoardProps) {
  const hiddenCount = Math.max(0, totalCount - items.length);

  return (
    <section className="msrp-monitor-action-board" aria-label="Price drop and offer action board">
      <header>
        <div>
          <h2>Price drop action board</h2>
          <p>MSRP movement only; offers stay in the offer signal map until matched to a price event.</p>
        </div>
        <strong>{items.length}/{totalCount}</strong>
      </header>
      {items.length > 0 ? (
        <div className="msrp-monitor-action-table">
          <div className="msrp-monitor-action-table-head" aria-hidden="true">
            <span>Priority</span>
            <span>Model</span>
            <span>Movement</span>
            <span>Evidence / status</span>
          </div>
          <div className="msrp-monitor-action-list">
            {items.map((item) => (
              <button
                key={item.key}
                type="button"
                className={`msrp-monitor-action-row ${auditClass(item.auditPriority)} is-${item.kind.replace(/_/g, "-")}${item.key === selectedKey ? " is-selected" : ""}`}
                onClick={() => onSelect(item)}
              >
                <span className="msrp-monitor-action-priority">
                  <b>{item.statusLabel}</b>
                  <small>{item.actionTypeLabel}</small>
                </span>
                <span className="msrp-monitor-action-model">
                  <strong>{item.modelLabel}</strong>
                  <small>{item.countryLabel} · {item.trimLabel} · {item.changedAtLabel}</small>
                </span>
                <span className="msrp-monitor-action-metric">
                  <strong>{item.primaryMetric}</strong>
                  <small>{item.secondaryMetric}</small>
                </span>
                <span className="msrp-monitor-action-evidence">
                  <strong>{item.evidenceLabel}</strong>
                  <small>{item.kind === "offer_signal" ? "Official source" : "Price history"}</small>
                  <small className="msrp-monitor-action-status-line">{item.statusDetail} · {auditLabel(item.auditPriority)}</small>
                </span>
              </button>
            ))}
          </div>
          {hiddenCount > 0 ? (
            <div className="msrp-monitor-action-overflow">
              {hiddenCount} more action item{hiddenCount === 1 ? "" : "s"} available in the lower detail sections.
            </div>
          ) : null}
        </div>
      ) : (
        <div className="msrp-monitor-empty-block">No MSRP movement actions match the current filters.</div>
      )}
    </section>
  );
}

function ImpactMiniChart({
  model,
  mode,
}: {
  model: PriceImpactModel;
  mode: "actual_expected" | "daily_effect" | "cumulative";
}) {
  const values = model.points.flatMap((point) => {
    if (mode === "actual_expected") {
      return [point.actual, point.expected];
    }
    if (mode === "daily_effect") {
      return [point.effect, 0];
    }
    return [point.cumulative, 0];
  });
  const [yMin, yMax] = domain(values, [-1, 1], 0.16);
  const innerWidth = IMPACT_CHART_WIDTH - IMPACT_CHART_MARGIN.left - IMPACT_CHART_MARGIN.right;
  const innerHeight = IMPACT_CHART_HEIGHT - IMPACT_CHART_MARGIN.top - IMPACT_CHART_MARGIN.bottom;
  const step = innerWidth / Math.max(1, model.points.length - 1);
  const scaleX = (index: number) => IMPACT_CHART_MARGIN.left + index * step;
  const scaleY = (value: number) => IMPACT_CHART_MARGIN.top + (1 - (value - yMin) / Math.max(1, yMax - yMin)) * innerHeight;
  const yTicks = niceTicks(yMin, yMax, 3);
  const launchX = scaleX(Math.min(1, model.points.length - 1));
  const actualPath = model.points.map((point, index) => `${index === 0 ? "M" : "L"} ${scaleX(index)} ${scaleY(point.actual)}`).join(" ");
  const expectedPath = model.points.map((point, index) => `${index === 0 ? "M" : "L"} ${scaleX(index)} ${scaleY(point.expected)}`).join(" ");
  const effectPath = model.points.map((point, index) => {
    const value = mode === "daily_effect" ? point.effect : point.cumulative;
    return `${index === 0 ? "M" : "L"} ${scaleX(index)} ${scaleY(value)}`;
  }).join(" ");
  const title = mode === "actual_expected"
    ? "Actual vs expected"
    : mode === "daily_effect"
      ? "Effect per observation"
      : "Cumulative exposure";
  const metricLabel = mode === "actual_expected"
    ? model.metricLabel
    : mode === "daily_effect"
      ? "Expected - Actual"
      : "Cumulative gap";

  return (
    <div className={`msrp-monitor-impact-chart is-${mode.replace("_", "-")}`}>
      <header>
        <strong>{title}</strong>
        <span>{metricLabel}</span>
      </header>
      <svg viewBox={`0 0 ${IMPACT_CHART_WIDTH} ${IMPACT_CHART_HEIGHT}`} role="img" aria-label={`${model.title} ${title}`}>
        <g className="msrp-monitor-impact-window">
          <rect
            x={launchX}
            y={IMPACT_CHART_MARGIN.top}
            width={Math.max(0, IMPACT_CHART_MARGIN.left + innerWidth - launchX)}
            height={innerHeight}
          />
          <line x1={launchX} x2={launchX} y1={IMPACT_CHART_MARGIN.top} y2={IMPACT_CHART_MARGIN.top + innerHeight} />
          {mode === "actual_expected" ? (
            <text x={launchX + 7} y={IMPACT_CHART_MARGIN.top + 14}>Launch</text>
          ) : null}
        </g>
        <g className="msrp-monitor-impact-grid">
          {yTicks.map((tick) => (
            <line key={`impact-y-${mode}-${tick}`} x1={IMPACT_CHART_MARGIN.left} x2={IMPACT_CHART_MARGIN.left + innerWidth} y1={scaleY(tick)} y2={scaleY(tick)} />
          ))}
          {model.points.map((point, index) => (
            <line key={`impact-x-${mode}-${point.label}`} x1={scaleX(index)} x2={scaleX(index)} y1={IMPACT_CHART_MARGIN.top} y2={IMPACT_CHART_MARGIN.top + innerHeight} />
          ))}
        </g>
        <g className="msrp-monitor-impact-axis">
          <line x1={IMPACT_CHART_MARGIN.left} x2={IMPACT_CHART_MARGIN.left + innerWidth} y1={scaleY(0)} y2={scaleY(0)} />
          {yTicks.map((tick) => (
            <text key={`impact-yt-${mode}-${tick}`} x={IMPACT_CHART_MARGIN.left - 8} y={scaleY(tick) + 4} textAnchor="end">
              {formatImpactValue(tick, model.valueKind, model.valueCurrency)}
            </text>
          ))}
          {model.points.map((point, index) => (
            <text key={`impact-xt-${mode}-${point.label}`} x={scaleX(index)} y={IMPACT_CHART_HEIGHT - 8} textAnchor="middle">
              {point.label}
            </text>
          ))}
        </g>
        {mode === "actual_expected" ? (
          <g className="msrp-monitor-impact-lines">
            <path className="is-expected" d={expectedPath} />
            <path className="is-actual" d={actualPath} />
            {model.points.map((point, index) => (
              <line key={`impact-gap-${point.label}`} className="is-gap" x1={scaleX(index)} x2={scaleX(index)} y1={scaleY(point.expected)} y2={scaleY(point.actual)} />
            ))}
          </g>
        ) : (
          <g className="msrp-monitor-impact-lines">
            <path className={mode === "daily_effect" ? "is-effect" : "is-cumulative"} d={effectPath} />
          </g>
        )}
      </svg>
    </div>
  );
}

function PriceImpactView({ model }: { model: PriceImpactModel }) {
  return (
    <div className="msrp-monitor-impact-view">
      <div className="msrp-monitor-impact-copy">
        <strong>{model.title}</strong>
        <span>{model.subtitle}</span>
        <p>{model.notes}</p>
      </div>
      <div className="msrp-monitor-impact-table" aria-label="Price impact verdict">
        <div><span>Metric</span><strong>{model.metricLabel}</strong></div>
        <div><span>Effect</span><strong>{model.effectLabel}</strong><small>{model.effectPctLabel}</small></div>
        <div><span>Range</span><strong>{model.rangeLabel}</strong><small>{model.expectedLabel}</small></div>
        <div><span>Avg / day</span><strong>{model.avgPerDayLabel}</strong><small>{model.durationLabel}</small></div>
        <div><span>Verdict</span><strong>{model.verdictLabel}</strong><small>{model.baselineLabel}</small></div>
      </div>
      <ImpactMiniChart model={model} mode="actual_expected" />
      <div className="msrp-monitor-impact-breakdown-label">Breakdown of the gap shown above</div>
      <div className="msrp-monitor-impact-breakdown-grid">
        <ImpactMiniChart model={model} mode="daily_effect" />
        <ImpactMiniChart model={model} mode="cumulative" />
      </div>
      <div className="msrp-monitor-impact-legend">
        <span><i className="is-actual" /> Actual</span>
        <span><i className="is-expected" /> Expected baseline</span>
        <span><i className="is-effect" /> Effect</span>
        <span><i className="is-cumulative" /> Cumulative</span>
      </div>
    </div>
  );
}

function OfferSignalStoryRail({ action }: { action: PriceActionOfferItem }) {
  const signal = action.signal;

  return (
    <div className="msrp-monitor-offer-story">
      <div className={`msrp-monitor-offer-story-verdict ${auditClass(signal.auditPriority)}`}>
        <strong>{offerSignalMatchStatusLabel(signal.matchStatus)}</strong>
        <span>{signal.notes}</span>
      </div>
      <div className="msrp-monitor-offer-story-grid" aria-label="Selected official offer signal">
        {OFFER_SIGNAL_COLUMNS.map((column) => {
          const active = offerSignalColumnActive(signal, column.key);
          return (
            <span key={column.key} className={`is-${column.key}${active ? " is-active" : ""}`}>
              <b>{column.label}</b>
              <strong>{offerSignalColumnLabel(signal, column.key)}</strong>
            </span>
          );
        })}
      </div>
      <div className="msrp-monitor-price-story-evidence">
        <span>{signal.sourceObservedDate}</span>
        <span>{signal.sourceLabel}</span>
        <a href={signal.sourceUrl} target="_blank" rel="noreferrer">Open official source</a>
      </div>
    </div>
  );
}

interface PriceActionStoryPanelProps {
  item: PriceActionItem | null;
  events: MsrpMonitoringModelEvent[];
  directionFilter: DirectionFilter;
  evidenceFilter: EvidenceFilter;
  onSelectTimelineItem: (item: MsrpMonitoringTimelineEvent) => void;
}

function PriceActionStoryPanel({
  item,
  events,
  directionFilter,
  evidenceFilter,
  onSelectTimelineItem,
}: PriceActionStoryPanelProps) {
  if (!item) {
    return (
      <section className="msrp-monitor-action-story">
        <div className="msrp-monitor-empty-block">Select an action item to inspect the price timeline.</div>
      </section>
    );
  }

  if (item.kind === "offer_signal") {
    return (
      <section className="msrp-monitor-action-story">
        <header>
          <div>
            <h2>Selected offer impact</h2>
            <p>{item.modelLabel} · {item.countryLabel} · {item.trimLabel}</p>
          </div>
          <strong>{item.primaryMetric}</strong>
        </header>
        <PriceImpactView model={buildOfferImpactModel(item)} />
        <OfferSignalStoryRail action={item} />
      </section>
    );
  }

  const selectedEvent = events.find((event) => event.eventId === item.queueItem.eventId) ?? null;
  const selectedKey = countryKey(item.queueItem.item);

  return (
      <section className="msrp-monitor-action-story">
      <header>
        <div>
          <h2>Selected price impact</h2>
          <p>{item.modelLabel} · {item.countryLabel} · {item.trimLabel}</p>
        </div>
        <strong>{item.primaryMetric}</strong>
      </header>
      <PriceImpactView model={buildMoveImpactModel(item)} />
      <MsrpTimelinePriceView
        event={selectedEvent}
        selectedKey={selectedKey}
        directionFilter={directionFilter}
        evidenceFilter={evidenceFilter}
        onSelect={onSelectTimelineItem}
      />
    </section>
  );
}

function selectedCountry(
  event: MsrpMonitoringModelEvent | null,
  selectedKey: string | null,
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): MsrpMonitoringTimelineEvent | null {
  if (!event) {
    return null;
  }
  const matchesFilters = (item: MsrpMonitoringTimelineEvent): boolean => (
    timelineItemMatchesDirection(item, direction)
    && timelineItemMatchesEvidence(item, evidence)
    && timelineItemMatchesAudit(event, item, audit)
  );
  const countryKeys = new Set(event.countries.map(countryKey));
  const candidates = [
    ...event.countries,
    ...event.timeline.filter((item) => !countryKeys.has(countryKey(item))),
  ];
  const selected = candidates.find((item) => countryKey(item) === selectedKey);
  if (selected && matchesFilters(selected)) {
    return selected;
  }
  return candidates.find(matchesFilters) ?? null;
}

function resolveMonitoringSelection(
  events: MsrpMonitoringModelEvent[],
  current: SelectedMonitoringItem,
  direction: DirectionFilter,
  evidence: EvidenceFilter,
  audit: AuditFilter,
): SelectedMonitoringItem {
  const retainedEvent = current.eventId
    ? events.find((event) => event.eventId === current.eventId) ?? null
    : null;
  const nextEvent = retainedEvent ?? defaultSelectedModelEvent(events);
  const nextCountry = selectedCountry(nextEvent, current.countryKey, direction, evidence, audit);
  const nextSelection = nextEvent && nextCountry
    ? { eventId: nextEvent.eventId, countryKey: countryKey(nextCountry) }
    : EMPTY_SELECTION;

  return current.eventId === nextSelection.eventId && current.countryKey === nextSelection.countryKey
    ? current
    : nextSelection;
}

function timelineTimeMs(item: MsrpMonitoringTimelineEvent): number | null {
  if (!item.changedAtUtc) {
    return null;
  }
  const time = new Date(item.changedAtUtc).getTime();
  return Number.isFinite(time) ? time : null;
}

function timelineSeriesKey(item: MsrpMonitoringTimelineEvent): string {
  return `${item.country}|${item.jatoTrim || "trim"}|${item.jatoPowertrain || "powertrain"}`;
}

function timelineSeriesLabel(item: MsrpMonitoringTimelineEvent): string {
  return `${item.countryLabel} · ${item.jatoTrim || "trim"}`;
}

function timelineDirectionClass(item: MsrpMonitoringTimelineEvent): string {
  const change = item.changePct ?? 0;
  if (change > 0) return "is-rise";
  if (change < 0) return "is-drop";
  return "is-flat";
}

function salesVolume(event: MsrpMonitoringModelEvent): number {
  const total = event.sales?.totalSales ?? 0;
  return Number.isFinite(total) ? Math.max(0, total) : 0;
}

function salesBubbleRadius(event: MsrpMonitoringModelEvent, maxSales: number): number {
  const sales = salesVolume(event);
  if (sales <= 0 || maxSales <= 0) {
    return 0;
  }
  return 7 + Math.sqrt(sales / maxSales) * 22;
}

function eventMovementClass(event: MsrpMonitoringModelEvent): string {
  const oldPrice = Number(event.medianOldMsrpEur ?? 0);
  const currentPrice = Number(event.medianCurrentMsrpEur ?? oldPrice);
  if (currentPrice < oldPrice) return "is-drop";
  if (currentPrice > oldPrice) return "is-rise";
  return "is-flat";
}

function formatCompactNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  const absolute = Math.abs(value);
  if (absolute >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(absolute >= 10_000_000 ? 0 : 1)}m`;
  }
  if (absolute >= 1_000) {
    return `${(value / 1_000).toFixed(absolute >= 10_000 ? 0 : 1)}k`;
  }
  return formatNumber(value);
}

function chartSalesLabel(event: MsrpMonitoringModelEvent): string {
  const sales = salesVolume(event);
  if (sales <= 0) {
    return "JATO rolling 12M not matched";
  }
  const coverage = event.sales?.countryLabels.join(", ") || "matched market";
  return `${formatNumber(sales)} JATO rolling 12M units · ${coverage}`;
}

function chartSalesSourceLabel(event: MsrpMonitoringModelEvent): string {
  const sales = event.sales;
  if (!sales || salesVolume(event) <= 0) {
    return "No current-country JATO sales match";
  }
  if (sales.source === "sweden_swiss_top30_rolling12") {
    return `JATO top30 snapshot · ${sales.latestSalesLabel ?? "rolling 12M"}`;
  }
  if (sales.source === "market_scan") {
    return `JATO market_scan · ${sales.latestSalesLabel ?? "latest"} rolling window`;
  }
  return `${sales.source || "JATO sales"} · ${sales.latestSalesLabel ?? "rolling 12M"}`;
}

function chartMovementScopeShortLabel(event: MsrpMonitoringModelEvent): string {
  const trimCount = uniqueChartTrims(event).length;
  const scope = chartMovementScopeLabel(event, trimCount);
  if (scope === "Single trim movement") {
    return "single trim";
  }
  if (scope === "Multi-trim movement") {
    return "multi trim";
  }
  return "model-wide";
}

function chartSmartLabelNote(event: MsrpMonitoringModelEvent, showSalesLayer: boolean): string {
  if (!showSalesLayer) {
    return chartMovementScopeShortLabel(event);
  }
  const sales = salesVolume(event);
  const salesLabel = sales > 0 ? `12M ${formatCompactNumber(sales)}` : "12M n/a";
  return `${salesLabel} · ${chartMovementScopeShortLabel(event)}`;
}

function chartSmartLabelScore(event: MsrpMonitoringModelEvent, maxSales: number, showSalesLayer: boolean): number {
  const sales = salesVolume(event);
  const salesScore = showSalesLayer && maxSales > 0 ? (sales / maxSales) * 56 : 0;
  const auditScore = Math.max(0, 4 - auditPriorityRank(event.auditPriority)) * 16;
  const changeScore = Math.min(28, Math.max(Math.abs(event.minChangePct ?? 0), Math.abs(event.maxChangePct ?? 0)) * 1.8);
  const trimScore = event.trimChangeCount > 1 || event.timelineEventCount > 1 ? 14 : 0;
  const syncScore = event.multiCountrySync ? 8 : 0;
  return salesScore + auditScore + changeScore + trimScore + syncScore;
}

function smartChartLabelEventIds(events: MsrpMonitoringModelEvent[], maxSales: number, showSalesLayer: boolean): Set<string> {
  const labelBudget = Math.max(4, Math.min(7, Math.ceil(events.length * 0.42)));
  return new Set(
    [...events]
      .sort((left, right) => (
        chartSmartLabelScore(right, maxSales, showSalesLayer) - chartSmartLabelScore(left, maxSales, showSalesLayer)
        || (showSalesLayer ? salesVolume(right) - salesVolume(left) : 0)
        || eventLabel(left).localeCompare(eventLabel(right))
      ))
      .slice(0, labelBudget)
      .map((event) => event.eventId),
  );
}

function buildChartMovementSegments(
  event: MsrpMonitoringModelEvent,
  scaleY: (value: number) => number,
): ChartMovementSegment[] {
  const pricedTimeline = event.timeline
    .filter((item) => item.oldMsrpEur !== null && item.currentMsrpEur !== null && timelineTimeMs(item) !== null)
    .sort((left, right) => (timelineTimeMs(left) ?? 0) - (timelineTimeMs(right) ?? 0));
  const sourceSegments = pricedTimeline.length > 0
    ? pricedTimeline
    : [{
      priceHistoryId: event.eventId,
      oldMsrpEur: event.medianOldMsrpEur,
      currentMsrpEur: event.medianCurrentMsrpEur,
      changedAtUtc: null,
      changePct: event.minChangePct,
    }];

  const pricedSegments = sourceSegments
    .filter((item) => item.oldMsrpEur !== null && item.currentMsrpEur !== null)
    .map((item, sourceIndex) => ({ item, sourceIndex }));
  const visibleSegments = pricedSegments.length <= MAX_CHART_MOVEMENT_SEGMENTS
    ? pricedSegments
    : (() => {
      const largestMove = pricedSegments.reduce((best, candidate) => {
        const bestDelta = Math.abs(Number(best.item.currentMsrpEur) - Number(best.item.oldMsrpEur));
        const candidateDelta = Math.abs(Number(candidate.item.currentMsrpEur) - Number(candidate.item.oldMsrpEur));
        return candidateDelta > bestDelta ? candidate : best;
      }, pricedSegments[0]);
      return Array.from(new Map([
        [0, pricedSegments[0]],
        [largestMove.sourceIndex, largestMove],
        [Math.max(0, pricedSegments.length - 2), pricedSegments[Math.max(0, pricedSegments.length - 2)]],
        [pricedSegments.length - 1, pricedSegments[pricedSegments.length - 1]],
      ]).values()).sort((left, right) => left.sourceIndex - right.sourceIndex);
    })();

  return visibleSegments
    .map(({ item, sourceIndex }, index, allItems) => {
      const change = Number(item.currentMsrpEur) - Number(item.oldMsrpEur);
      const spreadIndex = index - (allItems.length - 1) / 2;
      const recency = pricedSegments.length <= 1 ? 1 : (sourceIndex + 1) / pricedSegments.length;
      const collapsedLabel = pricedSegments.length > visibleSegments.length
        ? ` · showing ${visibleSegments.length} of ${pricedSegments.length} moves`
        : "";
      return {
        key: `${event.eventId}-${item.priceHistoryId ?? index}`,
        oldY: scaleY(Number(item.oldMsrpEur)),
        currentY: scaleY(Number(item.currentMsrpEur)),
        xOffset: spreadIndex * 4,
        opacity: 0.22 + recency * 0.48,
        className: change < 0 ? "is-drop" : change > 0 ? "is-rise" : "is-flat",
        label: `${formatTime(item.changedAtUtc)} · ${formatPct(item.changePct)}${collapsedLabel}`,
      };
    });
}

function latestTimelineEvent(event: MsrpMonitoringModelEvent): MsrpMonitoringTimelineEvent | null {
  return event.timeline
    .map((item) => ({ item, time: timelineTimeMs(item) ?? 0 }))
    .sort((left, right) => right.time - left.time)[0]?.item ?? event.timeline[0] ?? null;
}

function uniqueChartTrims(event: MsrpMonitoringModelEvent): string[] {
  return Array.from(new Set(event.timeline.map((item) => item.jatoTrim.trim()).filter(Boolean)));
}

function chartMovementScopeLabel(event: MsrpMonitoringModelEvent, trimCount: number): string {
  const effectiveTrimCount = Math.max(event.trimChangeCount, trimCount);
  if (effectiveTrimCount <= 1) {
    return "Single trim movement";
  }
  if (effectiveTrimCount >= 3 || event.timelineEventCount >= 3) {
    return "Model-wide candidate";
  }
  return "Multi-trim movement";
}

function chartTrimLabel(trims: string[]): string {
  if (trims.length === 0) {
    return "Trim pending";
  }
  const visible = trims.slice(0, 3);
  return trims.length > visible.length ? `${visible.join(", ")} +${trims.length - visible.length}` : visible.join(", ");
}

function chartEvidenceLabel(item: MsrpMonitoringTimelineEvent | null): string {
  if (!item) {
    return "Evidence pending";
  }
  if (item.evidence.backfilled && item.evidence.backfillKind) {
    return backfillKindLabel(item.evidence.backfillKind);
  }
  if (item.evidence.backfillSourceLabel) {
    return item.evidence.backfillSourceLabel;
  }
  return item.evidence.sourceUrl || item.sourceStatus || "Live MSRP source";
}

function chartPointHoverDetail(event: MsrpMonitoringModelEvent): ChartPointHoverDetail {
  const trims = uniqueChartTrims(event);
  const latest = latestTimelineEvent(event);
  const minChange = formatPct(event.minChangePct);
  const maxChange = formatPct(event.maxChangePct);
  const changeLabel = minChange === maxChange ? minChange : `${minChange} to ${maxChange}`;
  const sampleParts = [
    latest?.countryLabel,
    latest?.jatoTrim || trims[0],
    latest?.changedAtUtc ? formatTime(latest.changedAtUtc) : null,
  ].filter((item): item is string => Boolean(item));
  return {
    scopeLabel: chartMovementScopeLabel(event, trims.length),
    trimLabel: chartTrimLabel(trims),
    changeLabel,
    priceLabel: `${formatCurrency(event.medianOldMsrpEur)} -> ${formatCurrency(event.medianCurrentMsrpEur)}`,
    salesLabel: chartSalesLabel(event),
    salesSourceLabel: chartSalesSourceLabel(event),
    salesUseLabel: "Visual layer only; not price evidence",
    evidenceLabel: chartEvidenceLabel(latest),
    sampleLabel: sampleParts.join(" · ") || `${event.affectedCountryCount} market signal`,
  };
}

function monthTimeMs(period: string | null | undefined): number | null {
  if (!period || !/^\d{4}-\d{2}$/.test(period)) {
    return null;
  }
  const time = new Date(`${period}-01T00:00:00Z`).getTime();
  return Number.isFinite(time) ? time : null;
}

function markerDirectionClass(eventType: string): string {
  if (eventType === "price_drop") return "is-drop";
  if (eventType === "price_increase") return "is-rise";
  return "is-flat";
}

function salesEffectCoverageLabel(status: string): string {
  switch (status) {
    case "covered":
      return "Effect covered";
    case "post_sales_pending":
      return "Post-sales pending";
    case "no_effect_markers":
      return "No price marker";
    case "no_sales_match":
      return "No sales match";
    default:
      return status || "Unknown coverage";
  }
}

function salesEffectCoverageClass(status: string): string {
  return `is-${String(status || "unknown").replace(/_/g, "-")}`;
}

function ChartPoint({
  event,
  x,
  oldY,
  currentY,
  radius,
  movementClass,
  segments,
  labelSide = "right",
  unanchored = false,
  selected,
  showLabel,
  showSalesLayer,
  onSelect,
}: {
  event: MsrpMonitoringModelEvent;
  x: number;
  oldY: number;
  currentY: number;
  radius: number;
  movementClass: string;
  segments: ChartMovementSegment[];
  labelSide?: "left" | "right";
  unanchored?: boolean;
  selected: boolean;
  showLabel: boolean;
  showSalesLayer: boolean;
  onSelect: () => void;
}) {
  const [hovered, setHovered] = useState(false);
  const renderSalesBubble = showSalesLayer && salesVolume(event) > 0;
  const bubbleRadius = renderSalesBubble ? radius : 0;
  const dotRadius = renderSalesBubble ? Math.max(5.5, Math.min(9.5, radius * 0.42)) : 7;
  const anchorRadius = renderSalesBubble ? Math.max(bubbleRadius, dotRadius) : dotRadius;
  const hitTop = Math.min(oldY, currentY) - 22;
  const hitHeight = Math.abs(currentY - oldY) + 44;
  const hitWidth = Math.max(30, anchorRadius * 2 + 20);
  const labelX = labelSide === "left" ? x - anchorRadius - 8 : x + anchorRadius + 8;
  const hoverDetail = chartPointHoverDetail(event);
  const smartLabelNote = chartSmartLabelNote(event, renderSalesBubble);
  const tooltipWidth = 306;
  const tooltipHeight = 188;
  const tooltipX = labelSide === "left" || x > CHART_WIDTH - tooltipWidth - 24
    ? Math.max(8, x - tooltipWidth - anchorRadius - 16)
    : Math.min(CHART_WIDTH - tooltipWidth - 8, x + anchorRadius + 16);
  const tooltipY = Math.max(10, Math.min(CHART_HEIGHT - tooltipHeight - 10, Math.min(oldY, currentY) - 22));
  return (
    <g
      className={`msrp-monitor-point ${movementClass} ${auditClass(event.auditPriority)}${unanchored ? " is-unanchored" : ""}${hovered ? " is-hovered" : ""}${showLabel || selected ? " is-label-visible" : ""}`}
      onClick={onSelect}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocus={() => setHovered(true)}
      onBlur={() => setHovered(false)}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect();
        }
      }}
      tabIndex={0}
      role="button"
      aria-label={`${eventLabel(event)} ${hoverDetail.scopeLabel}`}
    >
      <rect className="msrp-monitor-hit-target" x={x - hitWidth / 2} y={hitTop} width={hitWidth} height={hitHeight} rx={8} />
      <circle className="msrp-monitor-hit-target" cx={x} cy={currentY} r={Math.max(24, anchorRadius + 10)} />
      <g className="msrp-monitor-price-trail">
        {segments.map((segment) => (
          <line
            key={segment.key}
            className={segment.className}
            x1={x + segment.xOffset}
            x2={x + segment.xOffset}
            y1={segment.oldY}
            y2={segment.currentY}
            opacity={segment.opacity}
          >
            <title>{segment.label}</title>
          </line>
        ))}
      </g>
      <line className={`msrp-monitor-drop-line ${movementClass}`} x1={x} x2={x} y1={oldY} y2={currentY} />
      <circle className="msrp-monitor-old-dot" cx={x} cy={oldY} r={Math.max(4, dotRadius * 0.75)} />
      {renderSalesBubble ? (
        <circle
          className="msrp-monitor-sales-bubble"
          cx={x}
          cy={currentY}
          r={bubbleRadius}
          fill={event.powertrainColor}
          stroke={event.powertrainColor}
        />
      ) : null}
      <line
        className="msrp-monitor-current-line"
        x1={x - dotRadius * 1.45}
        x2={x + dotRadius * 1.45}
        y1={currentY}
        y2={currentY}
        stroke={event.powertrainColor}
      />
      <circle
        className={`msrp-monitor-current-dot${selected ? " is-selected" : ""}`}
        cx={x}
        cy={currentY}
        r={dotRadius}
        fill={event.powertrainColor}
      />
      <title>{`${eventLabel(event)} · ${formatCurrency(event.medianOldMsrpEur)} to ${formatCurrency(event.medianCurrentMsrpEur)} · ${chartSalesLabel(event)} · ${event.affectedCountryCount} countries`}</title>
      <g className="msrp-monitor-point-label-group" aria-hidden="true">
        <text className="msrp-monitor-point-label" x={labelX} y={currentY + 2} textAnchor={labelSide === "left" ? "end" : "start"}>
          {eventLabel(event)}
        </text>
        <text className="msrp-monitor-point-label-note" x={labelX} y={currentY + 15} textAnchor={labelSide === "left" ? "end" : "start"}>
          {smartLabelNote}
        </text>
      </g>
      {hovered ? (
        <foreignObject
          className="msrp-monitor-hover-card-wrap"
          x={tooltipX}
          y={tooltipY}
          width={tooltipWidth}
          height={tooltipHeight}
        >
          <div className="msrp-monitor-hover-card">
            <header>
              <strong>{eventLabel(event)}</strong>
              <span>{hoverDetail.scopeLabel}</span>
            </header>
            <dl>
              <div><dt>Trim</dt><dd>{hoverDetail.trimLabel}</dd></div>
              <div><dt>Change</dt><dd>{hoverDetail.changeLabel} · {changePctBasisShortLabel(event.changePctBasis)}</dd></div>
              <div><dt>MSRP</dt><dd>{hoverDetail.priceLabel}</dd></div>
              <div><dt>Sales</dt><dd>{hoverDetail.salesLabel}</dd></div>
              <div><dt>Source</dt><dd>{hoverDetail.salesSourceLabel}</dd></div>
              <div><dt>Use</dt><dd>{hoverDetail.salesUseLabel}</dd></div>
              <div><dt>Evidence</dt><dd>{hoverDetail.evidenceLabel}</dd></div>
              <div><dt>Sample</dt><dd>{hoverDetail.sampleLabel}</dd></div>
            </dl>
          </div>
        </foreignObject>
      ) : null}
    </g>
  );
}

function MsrpSalesEffectChart({ event }: { event: MsrpMonitoringModelEvent | null }) {
  const sales = event?.sales ?? null;
  const series = (sales?.monthlySeries ?? [])
    .map((item) => ({
      ...item,
      time: monthTimeMs(item.period),
      sales: Number(item.sales),
    }))
    .filter((item): item is typeof item & { time: number } => item.time !== null && Number.isFinite(item.sales));

  if (!event || !sales || series.length === 0 || series.every((item) => item.sales <= 0)) {
    return (
      <div className="msrp-monitor-sales-chart-panel">
        <div className="msrp-monitor-empty-block">No JATO monthly sales match for this MSRP event.</div>
      </div>
    );
  }

  const salesByPeriod = new Map(series.map((item) => [item.period, item.sales]));
  const latestSalesTime = monthTimeMs(sales.latestSalesPeriod);
  const markerTimes = (sales.effectMarkers ?? [])
    .map((item) => ({ ...item, time: monthTimeMs(item.period) }))
    .filter((item): item is typeof item & { time: number } => item.time !== null);
  const rawTimes = [...series.map((item) => item.time), ...markerTimes.map((item) => item.time)];
  const minTime = Math.min(...rawTimes);
  const maxTime = Math.max(...rawTimes);
  const timeSpan = Math.max(30 * 24 * 60 * 60 * 1000, maxTime - minTime);
  const xMin = minTime - timeSpan * 0.03;
  const xMax = maxTime + timeSpan * 0.03;
  const yMax = Math.max(1, ...series.map((item) => item.sales));
  const yTicks = niceTicks(0, yMax, 4);
  const xTicks = Array.from(new Set([minTime, Math.round((minTime + maxTime) / 2), maxTime]));
  const innerWidth = SALES_CHART_WIDTH - SALES_CHART_MARGIN.left - SALES_CHART_MARGIN.right;
  const innerHeight = SALES_CHART_HEIGHT - SALES_CHART_MARGIN.top - SALES_CHART_MARGIN.bottom;
  const scaleX = (value: number) => SALES_CHART_MARGIN.left + ((value - xMin) / Math.max(1, xMax - xMin)) * innerWidth;
  const scaleY = (value: number) => SALES_CHART_MARGIN.top + (1 - value / Math.max(1, yMax)) * innerHeight;
  const linePath = series
    .map((item, index) => `${index === 0 ? "M" : "L"} ${scaleX(item.time)} ${scaleY(item.sales)}`)
    .join(" ");
  const areaPath = `${linePath} L ${scaleX(series[series.length - 1].time)} ${scaleY(0)} L ${scaleX(series[0].time)} ${scaleY(0)} Z`;
  const nearestSalesAt = (time: number) => {
    let value = series[0]?.sales ?? 0;
    series.forEach((item) => {
      if (item.time <= time) {
        value = item.sales;
      }
    });
    return value;
  };
  const markers = markerTimes
    .map((item) => ({
      ...item,
      sales: salesByPeriod.get(item.period) ?? nearestSalesAt(item.time),
      pendingSales: latestSalesTime === null || item.time > latestSalesTime,
    }))
    .filter((item): item is typeof item & { time: number; sales: number; pendingSales: boolean } => Number.isFinite(item.sales));

  return (
    <div className="msrp-monitor-sales-chart-panel">
      <div className="msrp-monitor-sales-chart-head">
        <div>
          <strong>JATO sales effect</strong>
          <span>{sales.countryLabels.join(", ") || "Matched market"} · {formatNumber(sales.totalSales)} rolling 12M units</span>
          <em className={`msrp-monitor-sales-coverage ${salesEffectCoverageClass(sales.effectCoverageStatus)}`}>
            {salesEffectCoverageLabel(sales.effectCoverageStatus)}
            {sales.pendingEffectMarkerCount > 0 ? ` · ${sales.pendingEffectMarkerCount} pending` : ""}
          </em>
        </div>
        <div>
          <b>{formatNumber(sales.currentMonthSales)}</b>
          <small>{sales.latestSalesLabel ? `${sales.latestSalesLabel} latest` : "latest month"}</small>
        </div>
      </div>
      <svg
        className="msrp-monitor-sales-chart"
        viewBox={`0 0 ${SALES_CHART_WIDTH} ${SALES_CHART_HEIGHT}`}
        role="img"
        aria-label={`${eventLabel(event)} JATO monthly sales`}
      >
        <g className="msrp-monitor-sales-grid">
          {yTicks.map((tick) => (
            <line key={`sales-y-${tick}`} x1={SALES_CHART_MARGIN.left} x2={SALES_CHART_MARGIN.left + innerWidth} y1={scaleY(tick)} y2={scaleY(tick)} />
          ))}
          {xTicks.map((tick) => (
            <line key={`sales-x-${tick}`} x1={scaleX(tick)} x2={scaleX(tick)} y1={SALES_CHART_MARGIN.top} y2={SALES_CHART_MARGIN.top + innerHeight} />
          ))}
        </g>
        <g className="msrp-monitor-sales-axis">
          <line x1={SALES_CHART_MARGIN.left} x2={SALES_CHART_MARGIN.left + innerWidth} y1={SALES_CHART_MARGIN.top + innerHeight} y2={SALES_CHART_MARGIN.top + innerHeight} />
          <line x1={SALES_CHART_MARGIN.left} x2={SALES_CHART_MARGIN.left} y1={SALES_CHART_MARGIN.top} y2={SALES_CHART_MARGIN.top + innerHeight} />
          {yTicks.map((tick) => (
            <text key={`sales-yt-${tick}`} x={SALES_CHART_MARGIN.left - 10} y={scaleY(tick) + 4} textAnchor="end">{formatNumber(tick)}</text>
          ))}
          {xTicks.map((tick) => (
            <text key={`sales-xt-${tick}`} x={scaleX(tick)} y={SALES_CHART_MARGIN.top + innerHeight + 24} textAnchor="middle">
              {new Date(tick).toLocaleDateString(undefined, { month: "short", year: "2-digit" })}
            </text>
          ))}
        </g>
        <path className="msrp-monitor-sales-area" d={areaPath} />
        <path className="msrp-monitor-sales-line" d={linePath} />
        <g className="msrp-monitor-sales-markers">
          {markers.map((marker) => (
            <g
              key={`${marker.period}-${marker.countryLabel}-${marker.jatoTrim}`}
              className={`msrp-monitor-sales-marker ${markerDirectionClass(marker.eventType)}${marker.pendingSales ? " is-pending" : ""}`}
            >
              <line x1={scaleX(marker.time)} x2={scaleX(marker.time)} y1={SALES_CHART_MARGIN.top} y2={SALES_CHART_MARGIN.top + innerHeight} />
              <circle cx={scaleX(marker.time)} cy={marker.pendingSales ? scaleY(0) : scaleY(marker.sales)} r={5}>
                <title>{`${marker.period} · ${marker.countryLabel ?? "market"} · ${formatPct(marker.changePct)}${marker.pendingSales ? " · post-sales pending" : ""}`}</title>
              </circle>
            </g>
          ))}
        </g>
      </svg>
      {sales.warnings.length > 0 ? (
        <div className="msrp-monitor-sales-warning">
          {sales.warnings.map((warning) => <span key={warning}>{warning}</span>)}
        </div>
      ) : null}
    </div>
  );
}

function MsrpTimelinePriceView({
  event,
  selectedKey,
  directionFilter,
  evidenceFilter,
  onSelect,
}: {
  event: MsrpMonitoringModelEvent | null;
  selectedKey: string | null;
  directionFilter: DirectionFilter;
  evidenceFilter: EvidenceFilter;
  onSelect: (item: MsrpMonitoringTimelineEvent) => void;
}) {
  if (!event || event.timeline.length === 0) {
    return <div className="msrp-monitor-empty-block">No timeline price movements for the selected model.</div>;
  }

  const timelineEvents = event.timeline
    .filter((item) => (
      timelineTimeMs(item) !== null
      && item.oldMsrpEur !== null
      && item.currentMsrpEur !== null
      && timelineItemMatchesDirection(item, directionFilter)
      && timelineItemMatchesEvidence(item, evidenceFilter)
    ))
    .sort((left, right) => (timelineTimeMs(left) ?? 0) - (timelineTimeMs(right) ?? 0));

  if (timelineEvents.length === 0) {
    return <div className="msrp-monitor-empty-block">No priced timeline events are available for this model.</div>;
  }

  const seriesMap = new Map<string, TimelineSeries>();
  timelineEvents.forEach((item) => {
    const key = timelineSeriesKey(item);
    const existing = seriesMap.get(key);
    if (existing) {
      existing.events.push(item);
      return;
    }
    seriesMap.set(key, {
      key,
      label: timelineSeriesLabel(item),
      color: TIMELINE_SERIES_COLORS[seriesMap.size % TIMELINE_SERIES_COLORS.length],
      events: [item],
    });
  });
  const series = Array.from(seriesMap.values()).map((item) => ({
    ...item,
    events: item.events.sort((left, right) => (timelineTimeMs(left) ?? 0) - (timelineTimeMs(right) ?? 0)),
  }));
  const eventStripLaneCount = Math.min(Math.max(series.length, 1), 6);

  const rawTimes = timelineEvents.map((item) => timelineTimeMs(item)).filter((value): value is number => value !== null);
  const minTime = Math.min(...rawTimes);
  const maxTime = Math.max(...rawTimes);
  const timeSpan = Math.max(24 * 60 * 60 * 1000, maxTime - minTime);
  const xMin = minTime - timeSpan * 0.18;
  const xMax = maxTime + timeSpan * 0.28;
  const priceValues = timelineEvents.flatMap((item) => [Number(item.oldMsrpEur), Number(item.currentMsrpEur)]);
  const [yMin, yMax] = domain(priceValues, [20000, 90000], 0.12);
  const innerWidth = TIMELINE_CHART_WIDTH - TIMELINE_CHART_MARGIN.left - TIMELINE_CHART_MARGIN.right;
  const innerHeight = TIMELINE_CHART_HEIGHT - TIMELINE_CHART_MARGIN.top - TIMELINE_CHART_MARGIN.bottom;
  const stripTop = TIMELINE_CHART_MARGIN.top + innerHeight + 30;
  const scaleX = (value: number) => TIMELINE_CHART_MARGIN.left + ((value - xMin) / Math.max(1, xMax - xMin)) * innerWidth;
  const scaleY = (value: number) => TIMELINE_CHART_MARGIN.top + (1 - (value - yMin) / Math.max(1, yMax - yMin)) * innerHeight;
  const yTicks = niceTicks(yMin, yMax, 4);
  const xTicks = Array.from(new Set([minTime, Math.round((minTime + maxTime) / 2), maxTime]));

  function seriesPath(item: TimelineSeries): string {
    const first = item.events[0];
    if (!first || first.oldMsrpEur === null || first.currentMsrpEur === null) {
      return "";
    }
    const commands = [`M ${scaleX(xMin)} ${scaleY(Number(first.oldMsrpEur))}`];
    item.events.forEach((timelineItem) => {
      const time = timelineTimeMs(timelineItem);
      if (time === null || timelineItem.oldMsrpEur === null || timelineItem.currentMsrpEur === null) {
        return;
      }
      commands.push(`H ${scaleX(time)}`);
      commands.push(`V ${scaleY(Number(timelineItem.oldMsrpEur))}`);
      commands.push(`V ${scaleY(Number(timelineItem.currentMsrpEur))}`);
    });
    commands.push(`H ${scaleX(xMax)}`);
    return commands.join(" ");
  }

  return (
    <div className="msrp-monitor-timeline-chart-panel">
      <div className="msrp-monitor-timeline-chart-head">
        <div>
          <strong>Price history</strong>
          <span>{eventLabel(event)} · {event.timelineEventCount} movements</span>
        </div>
        <div className="msrp-monitor-timeline-chart-legend">
          {series.slice(0, 4).map((item) => <span key={item.key}><i style={{ background: item.color }} />{item.label}</span>)}
        </div>
      </div>
      <svg
        className="msrp-monitor-timeline-chart"
        viewBox={`0 0 ${TIMELINE_CHART_WIDTH} ${TIMELINE_CHART_HEIGHT}`}
        role="img"
        aria-label={`${eventLabel(event)} MSRP timeline`}
      >
        <g className="msrp-monitor-timeline-grid">
          {yTicks.map((tick) => (
            <line key={`timeline-y-${tick}`} x1={TIMELINE_CHART_MARGIN.left} x2={TIMELINE_CHART_MARGIN.left + innerWidth} y1={scaleY(tick)} y2={scaleY(tick)} />
          ))}
          {xTicks.map((tick) => (
            <line key={`timeline-x-${tick}`} x1={scaleX(tick)} x2={scaleX(tick)} y1={TIMELINE_CHART_MARGIN.top} y2={stripTop + 44} />
          ))}
        </g>
        <g className="msrp-monitor-timeline-axis">
          <line x1={TIMELINE_CHART_MARGIN.left} x2={TIMELINE_CHART_MARGIN.left + innerWidth} y1={TIMELINE_CHART_MARGIN.top + innerHeight} y2={TIMELINE_CHART_MARGIN.top + innerHeight} />
          <line x1={TIMELINE_CHART_MARGIN.left} x2={TIMELINE_CHART_MARGIN.left} y1={TIMELINE_CHART_MARGIN.top} y2={TIMELINE_CHART_MARGIN.top + innerHeight} />
          {yTicks.map((tick) => (
            <text key={`timeline-yt-${tick}`} x={TIMELINE_CHART_MARGIN.left - 10} y={scaleY(tick) + 4} textAnchor="end">{formatCurrency(tick)}</text>
          ))}
          {xTicks.map((tick) => (
            <text key={`timeline-xt-${tick}`} x={scaleX(tick)} y={stripTop + 62} textAnchor="middle">{formatTime(new Date(tick).toISOString())}</text>
          ))}
          <text x={18} y={TIMELINE_CHART_MARGIN.top + innerHeight / 2} textAnchor="middle" transform={`rotate(-90 18 ${TIMELINE_CHART_MARGIN.top + innerHeight / 2})`}>MSRP EUR</text>
        </g>
        <g className="msrp-monitor-timeline-lines">
          {series.map((item) => (
            <path key={item.key} d={seriesPath(item)} stroke={item.color} />
          ))}
        </g>
        <g className="msrp-monitor-timeline-events">
          {timelineEvents.map((item) => {
            const time = timelineTimeMs(item);
            if (time === null || item.oldMsrpEur === null || item.currentMsrpEur === null) {
              return null;
            }
            const key = countryKey(item);
            const effectiveAudit = higherAuditPriority(event, item);
            const matchingSeries = seriesMap.get(timelineSeriesKey(item));
            const seriesIndex = series.findIndex((candidate) => candidate.key === matchingSeries?.key);
            const stripY = stripTop + 10 + (Math.max(0, seriesIndex) % eventStripLaneCount) * 8;
            return (
              <g key={item.priceHistoryId} className="msrp-monitor-timeline-event">
                <line className="msrp-monitor-timeline-change-line" x1={scaleX(time)} x2={scaleX(time)} y1={scaleY(Number(item.oldMsrpEur))} y2={scaleY(Number(item.currentMsrpEur))} />
                <circle
                  className={`msrp-monitor-timeline-dot ${timelineDirectionClass(item)} ${auditClass(effectiveAudit.priority)}${selectedKey === key ? " is-selected" : ""}`}
                  cx={scaleX(time)}
                  cy={scaleY(Number(item.currentMsrpEur))}
                  r={selectedKey === key ? 6 : 5}
                  onClick={() => onSelect(item)}
                >
                  <title>{`${timelineSeriesLabel(item)} · ${formatPct(item.changePct)} · ${changePctBasisLabel(item.changePctBasis)} · ${formatCurrency(item.oldMsrpEur)} to ${formatCurrency(item.currentMsrpEur)}`}</title>
                </circle>
                <circle
                  className={`msrp-monitor-timeline-strip-dot ${timelineDirectionClass(item)} ${auditClass(effectiveAudit.priority)}${selectedKey === key ? " is-selected" : ""}`}
                  cx={scaleX(time)}
                  cy={stripY}
                  r={selectedKey === key ? 5 : 4}
                  onClick={() => onSelect(item)}
                >
                  <title>{`${formatTime(item.changedAtUtc)} · ${timelineSeriesLabel(item)} · ${auditLabel(effectiveAudit.priority)}`}</title>
                </circle>
              </g>
            );
          })}
        </g>
        <text className="msrp-monitor-timeline-strip-label" x={TIMELINE_CHART_MARGIN.left} y={stripTop - 6}>Event strip</text>
      </svg>
    </div>
  );
}

function MsrpEventChart({
  events,
  selectedEventId,
  onSelect,
}: {
  events: MsrpMonitoringModelEvent[];
  selectedEventId: string | null;
  onSelect: (eventId: string) => void;
}) {
  const [showSalesLayer, setShowSalesLayer] = useState(false);
  const priceChartEvents = events.filter(isPriceChartEvent);
  const drawableEvents = priceChartEvents.filter(isChartDrawableEvent);
  const missingLengthEvents = priceChartEvents.filter((event) => !isChartDrawableEvent(event));
  const hiddenLengthCount = missingLengthEvents.length;
  const sortedRailEvents = [...events].sort((left, right) => (
    auditPriorityRank(left.auditPriority) - auditPriorityRank(right.auditPriority)
    || Math.max(Math.abs(right.minChangePct ?? 0), Math.abs(right.maxChangePct ?? 0))
      - Math.max(Math.abs(left.minChangePct ?? 0), Math.abs(left.maxChangePct ?? 0))
    || eventLabel(left).localeCompare(eventLabel(right))
  ));
  const xValues = drawableEvents.map((event) => Number(event.lengthMm));
  const yValues = drawableEvents.flatMap((event) => [
    Number(event.medianOldMsrpEur),
    Number(event.medianCurrentMsrpEur),
  ]);
  const priceYValues = priceChartEvents.flatMap((event) => [
    Number(event.medianOldMsrpEur),
    Number(event.medianCurrentMsrpEur),
  ]);
  const [xMin, xMax] = domain(xValues, [4000, 5000]);
  const [yMin, yMax] = domain(priceYValues.length > 0 ? priceYValues : yValues, [20000, 80000], 0.12);
  const innerWidth = CHART_WIDTH - CHART_MARGIN.left - CHART_MARGIN.right;
  const innerHeight = CHART_HEIGHT - CHART_MARGIN.top - CHART_MARGIN.bottom;
  const hasPendingLengthLane = missingLengthEvents.length > 0;
  const anchoredInnerWidth = hasPendingLengthLane ? innerWidth * 0.82 : innerWidth;
  const pendingLaneStart = CHART_MARGIN.left + anchoredInnerWidth + 14;
  const pendingLaneWidth = Math.max(64, innerWidth - anchoredInnerWidth - 14);
  const pendingLaneX = pendingLaneStart + pendingLaneWidth * 0.52;
  const scaleX = (value: number) => CHART_MARGIN.left + ((value - xMin) / Math.max(1, xMax - xMin)) * anchoredInnerWidth;
  const scaleY = (value: number) => CHART_MARGIN.top + (1 - (value - yMin) / Math.max(1, yMax - yMin)) * innerHeight;
  const xTicks = niceTicks(xMin, xMax, 6);
  const yTicks = niceTicks(yMin, yMax, 6);
  const maxSales = Math.max(0, ...priceChartEvents.map(salesVolume));
  const smartLabelIds = smartChartLabelEventIds(priceChartEvents, maxSales, showSalesLayer);

  return (
    <div className="msrp-monitor-chart-shell">
      <div className="msrp-monitor-chart-head">
        <div>
          <h2>Length x MSRP movement map</h2>
          <p>Vehicle length anchors each model; vertical trails encode old price, current price and every observed move.</p>
        </div>
        <div className="msrp-monitor-legend">
          <span><i className="old" /> Old MSRP</span>
          <span><i className="line" /> Movement trail</span>
          <span><i className="current" /> Current MSRP</span>
          <button
            type="button"
            className={`msrp-monitor-legend-toggle${showSalesLayer ? " is-active" : ""}`}
            aria-pressed={showSalesLayer}
            onClick={() => setShowSalesLayer((current) => !current)}
          >
            <i className="bubble" /> JATO rolling 12M sales
          </button>
          <span><i className="smart" /> Smart label</span>
        </div>
      </div>
      <svg viewBox={`0 0 ${CHART_WIDTH} ${CHART_HEIGHT}`} className="msrp-monitor-chart" role="img" aria-label="MSRP monitor model event chart">
        <g className="msrp-monitor-grid">
          {xTicks.map((tick) => (
            <line key={`x-${tick}`} x1={scaleX(tick)} x2={scaleX(tick)} y1={CHART_MARGIN.top} y2={CHART_MARGIN.top + innerHeight} />
          ))}
          {yTicks.map((tick) => (
            <line key={`y-${tick}`} x1={CHART_MARGIN.left} x2={CHART_MARGIN.left + innerWidth} y1={scaleY(tick)} y2={scaleY(tick)} />
          ))}
        </g>
        <g className="msrp-monitor-axis">
          <line x1={CHART_MARGIN.left} x2={CHART_MARGIN.left + innerWidth} y1={CHART_MARGIN.top + innerHeight} y2={CHART_MARGIN.top + innerHeight} />
          <line x1={CHART_MARGIN.left} x2={CHART_MARGIN.left} y1={CHART_MARGIN.top} y2={CHART_MARGIN.top + innerHeight} />
          {xTicks.map((tick) => (
            <g key={`xt-${tick}`}>
              <text x={scaleX(tick)} y={CHART_MARGIN.top + innerHeight + 26} textAnchor="middle">{formatNumber(tick)} mm</text>
            </g>
          ))}
          {yTicks.map((tick) => (
            <g key={`yt-${tick}`}>
              <text x={CHART_MARGIN.left - 12} y={scaleY(tick) + 4} textAnchor="end">{formatCurrency(tick)}</text>
            </g>
          ))}
          <text x={CHART_MARGIN.left + innerWidth / 2} y={CHART_HEIGHT - 12} textAnchor="middle">Vehicle length</text>
          <text x={18} y={CHART_MARGIN.top + innerHeight / 2} textAnchor="middle" transform={`rotate(-90 18 ${CHART_MARGIN.top + innerHeight / 2})`}>MSRP EUR normalized</text>
        </g>
        {hasPendingLengthLane ? (
          <g className="msrp-monitor-length-pending-lane">
            <rect
              x={pendingLaneStart}
              y={CHART_MARGIN.top}
              width={pendingLaneWidth}
              height={innerHeight}
              rx={8}
            />
            <line x1={pendingLaneStart} x2={pendingLaneStart} y1={CHART_MARGIN.top} y2={CHART_MARGIN.top + innerHeight} />
            <text x={pendingLaneX} y={CHART_MARGIN.top + innerHeight + 26} textAnchor="middle">Length pending</text>
          </g>
        ) : null}
        {drawableEvents.map((event) => (
          <ChartPoint
            key={event.eventId}
            event={event}
            x={scaleX(Number(event.lengthMm))}
            oldY={scaleY(Number(event.medianOldMsrpEur))}
            currentY={scaleY(Number(event.medianCurrentMsrpEur))}
            radius={salesBubbleRadius(event, maxSales)}
            movementClass={eventMovementClass(event)}
            segments={buildChartMovementSegments(event, scaleY)}
            selected={event.eventId === selectedEventId}
            showLabel={smartLabelIds.has(event.eventId)}
            showSalesLayer={showSalesLayer}
            onSelect={() => onSelect(event.eventId)}
          />
        ))}
        {missingLengthEvents.map((event, index) => {
          const offset = (index - (missingLengthEvents.length - 1) / 2) * 18;
          return (
            <ChartPoint
              key={event.eventId}
              event={event}
              x={pendingLaneX + offset}
              oldY={scaleY(Number(event.medianOldMsrpEur))}
              currentY={scaleY(Number(event.medianCurrentMsrpEur))}
              radius={salesBubbleRadius(event, maxSales)}
              movementClass={eventMovementClass(event)}
              segments={buildChartMovementSegments(event, scaleY)}
              labelSide="left"
              unanchored
              selected={event.eventId === selectedEventId}
              showLabel={smartLabelIds.has(event.eventId)}
              showSalesLayer={showSalesLayer}
              onSelect={() => onSelect(event.eventId)}
            />
          );
        })}
      </svg>
      {priceChartEvents.length === 0 ? (
        <div className="msrp-monitor-empty">No priced model events are available in this time window.</div>
      ) : null}
      <div className="msrp-monitor-chart-coverage" aria-label="Length MSRP movement map coverage">
        <span><strong>{priceChartEvents.length}</strong> plotted</span>
        <span><strong>{drawableEvents.length}</strong> length anchored</span>
        <span><strong>{hiddenLengthCount}</strong> length pending</span>
        <span><strong>{events.length}</strong> filtered models</span>
        <span><strong>{priceChartEvents.reduce((total, event) => total + Math.max(1, event.timelineEventCount), 0)}</strong> move segments</span>
      </div>
      {sortedRailEvents.length > 0 ? (
        <div className="msrp-monitor-chart-model-rail">
          <header>
            <strong>All filtered models</strong>
            <span>{hiddenLengthCount > 0 ? `${hiddenLengthCount} kept outside the scatter because length is missing` : "Every model is plotted"}</span>
          </header>
          <div>
            {sortedRailEvents.map((event) => (
              <button
                key={event.eventId}
                type="button"
                className={`${auditClass(event.auditPriority)}${event.eventId === selectedEventId ? " is-selected" : ""}${isChartDrawableEvent(event) ? "" : " is-missing-length"}`}
                onClick={() => onSelect(event.eventId)}
              >
                <span>
                  <strong>{eventLabel(event)}</strong>
                  <small>{event.jatoPowertrain} · {event.affectedCountryCount} countries</small>
                </span>
                <b>{formatPct(event.minChangePct)} / {formatPct(event.maxChangePct)}</b>
                <i>{isChartDrawableEvent(event) ? `${event.lengthMm} mm` : "No length"}</i>
              </button>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function MsrpMonitorPage() {
  const [mode, setMode] = useState<MonitorMode>("sweden_swiss_demo");
  const [windowOptionId, setWindowOptionId] = useState(() => currentYearWindowOption().id);
  const [thresholdPct, setThresholdPct] = useState(0);
  const [refreshIntervalSeconds, setRefreshIntervalSeconds] = useState(60);
  const [refreshTick, setRefreshTick] = useState(0);
  const [auditFilter, setAuditFilter] = useState<AuditFilter>("all");
  const [directionFilter, setDirectionFilter] = useState<DirectionFilter>("all");
  const [evidenceFilter, setEvidenceFilter] = useState<EvidenceFilter>("all");
  const [countryFilter, setCountryFilter] = useState(DEFAULT_SWEDEN_SWISS_DEMO_COUNTRY);
  const [brandFilter, setBrandFilter] = useState("all");
  const [data, setData] = useState<MsrpMonitoringResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [lastUpdatedAt, setLastUpdatedAt] = useState<string | null>(null);
  const [selectedItem, setSelectedItem] = useState<SelectedMonitoringItem>(EMPTY_SELECTION);
  const [selectedLaunchAlertId, setSelectedLaunchAlertId] = useState<string | null>(null);
  const [selectedOfferSignalId, setSelectedOfferSignalId] = useState<string | null>(null);
  const [selectedActionKey, setSelectedActionKey] = useState<string | null>(null);
  const [backfillSnapshotPreviews, setBackfillSnapshotPreviews] = useState<Record<string, MsrpBackfillSnapshotPreview>>({});
  const [backfillSnapshotLoadingPath, setBackfillSnapshotLoadingPath] = useState<string | null>(null);
  const [backfillSnapshotErrorByPath, setBackfillSnapshotErrorByPath] = useState<Record<string, string>>({});
  const [snapshotPreviewPathOverride, setSnapshotPreviewPathOverride] = useState<string | null>(null);
  const [spotCheckBriefCopyStatus, setSpotCheckBriefCopyStatus] = useState<CopyStatus>("idle");
  const [pendingCampaignBoundaryFocus, setPendingCampaignBoundaryFocus] = useState(false);
  const [deckOpen, setDeckOpen] = useState(false);
  const [deckTab, setDeckTab] = useState<DeckTab>("overview");
  const [timelineIndex, setTimelineIndex] = useState(0);
  const monitoringRequestSequenceRef = useRef(0);
  const windowOptions = monitoringWindowOptions();
  const selectedWindowOption = windowOptions.find((option) => option.id === windowOptionId) ?? WINDOW_OPTIONS[1];
  const selectedEventId = selectedItem.eventId;
  const selectedCountryKey = selectedItem.countryKey;

  useEffect(() => {
    let active = true;
    let timerId: ReturnType<typeof window.setInterval> | null = null;

    function loadEvents(): void {
      const requestSequence = monitoringRequestSequenceRef.current + 1;
      monitoringRequestSequenceRef.current = requestSequence;
      setLoading(true);
      setError("");
      api.getMsrpMonitoringEvents({
        country: mode === "sweden_demo" || countryFilter === "all" ? undefined : countryFilter,
        brand: brandFilter === "all" ? undefined : brandFilter,
        window_days: selectedWindowOption.days,
        from_date: selectedWindowOption.fromDate,
        threshold_pct: thresholdPct,
        direction: directionFilter,
        limit: 500,
        mode,
      }).then((response) => {
        if (!active || monitoringRequestSequenceRef.current !== requestSequence) return;
        setData(response);
        setLastUpdatedAt(response.generatedAtUtc);
      }).catch((err: unknown) => {
        if (!active || monitoringRequestSequenceRef.current !== requestSequence) return;
        setError(err instanceof Error ? err.message : String(err));
        setData(null);
      }).finally(() => {
        if (active && monitoringRequestSequenceRef.current === requestSequence) setLoading(false);
      });
    }

    loadEvents();
    if (refreshIntervalSeconds > 0) {
      timerId = window.setInterval(loadEvents, refreshIntervalSeconds * 1000);
    }

    return () => {
      active = false;
      if (timerId !== null) {
        window.clearInterval(timerId);
      }
    };
  }, [
    brandFilter,
    countryFilter,
    directionFilter,
    mode,
    refreshIntervalSeconds,
    refreshTick,
    selectedWindowOption.days,
    selectedWindowOption.fromDate,
    thresholdPct,
  ]);

  const rawEvents = data?.events ?? [];
  const launchAlerts = data?.launchAlerts ?? [];
  const rawOfferSignals = data?.offerSignals ?? [];
  const batchACoverage = data?.coverage?.batchA ?? null;
  const events = useMemo(
    () => rawEvents.filter((event) => eventMatchesFilters(event, directionFilter, evidenceFilter, auditFilter)),
    [auditFilter, directionFilter, evidenceFilter, rawEvents],
  );
  const offerSignals = useMemo(
    () => rawOfferSignals.filter((signal) => offerSignalMatchesFilters(signal, directionFilter, evidenceFilter, auditFilter)),
    [auditFilter, directionFilter, evidenceFilter, rawOfferSignals],
  );
  const selectedOfferSignal = offerSignals.find((signal) => signal.signalId === selectedOfferSignalId) ?? offerSignals[0] ?? null;

  useEffect(() => {
    setSelectedItem((current) => resolveMonitoringSelection(
      events,
      current,
      directionFilter,
      evidenceFilter,
      auditFilter,
    ));
  }, [auditFilter, directionFilter, evidenceFilter, events]);

  useEffect(() => {
    setSelectedOfferSignalId((current) => {
      if (current && offerSignals.some((signal) => signal.signalId === current)) {
        return current;
      }
      return offerSignals[0]?.signalId ?? null;
    });
  }, [offerSignals]);

  const selectedEvent = events.find((event) => event.eventId === selectedEventId) ?? defaultSelectedModelEvent(events);
  const selectedCountryEvent = selectedCountry(selectedEvent, selectedCountryKey, directionFilter, evidenceFilter, auditFilter);
  const selectedCountryAudit = selectedEvent && selectedCountryEvent
    ? higherAuditPriority(selectedEvent, selectedCountryEvent)
    : null;
  const selectedBackfillSnapshotPath = selectedCountryEvent?.evidence.backfillSnapshotPath ?? null;
  const selectedRelatedSnapshotPaths = selectedCountryEvent?.evidence.relatedOfficialEvidence
    ?.map((item) => item.snapshotPath)
    .filter((path): path is string => Boolean(path)) ?? [];
  const activeSnapshotPreviewPathOverride = snapshotPreviewPathOverride && selectedRelatedSnapshotPaths.includes(snapshotPreviewPathOverride)
    ? snapshotPreviewPathOverride
    : null;
  const activeBackfillSnapshotPath = activeSnapshotPreviewPathOverride ?? selectedBackfillSnapshotPath;
  const currentBackfillSnapshotPreview = selectedBackfillSnapshotPath
    ? backfillSnapshotPreviews[selectedBackfillSnapshotPath] ?? null
    : null;
  const currentBackfillSnapshotError = selectedBackfillSnapshotPath
    ? backfillSnapshotErrorByPath[selectedBackfillSnapshotPath] ?? ""
    : "";
  const currentBackfillSnapshotLoading = Boolean(selectedBackfillSnapshotPath)
    && (
      backfillSnapshotLoadingPath === selectedBackfillSnapshotPath
      || (!currentBackfillSnapshotPreview && !currentBackfillSnapshotError)
    );
  const activeBackfillSnapshotPreview = activeBackfillSnapshotPath
    ? backfillSnapshotPreviews[activeBackfillSnapshotPath] ?? null
    : null;
  const activeBackfillSnapshotError = activeBackfillSnapshotPath
    ? backfillSnapshotErrorByPath[activeBackfillSnapshotPath] ?? ""
    : "";
  const activeBackfillSnapshotLoading = Boolean(activeBackfillSnapshotPath)
    && (
      backfillSnapshotLoadingPath === activeBackfillSnapshotPath
      || (!activeBackfillSnapshotPreview && !activeBackfillSnapshotError)
    );
  const evidenceChecklist = selectedCountryEvent
    ? buildEvidenceChecklist(selectedCountryEvent, currentBackfillSnapshotPreview, currentBackfillSnapshotLoading)
    : [];
  const spotCheckBrief = selectedEvent && selectedCountryEvent
    ? buildSpotCheckBrief(
        selectedEvent,
        selectedCountryEvent,
        selectedCountryAudit,
        evidenceChecklist,
        currentBackfillSnapshotPreview,
        currentBackfillSnapshotLoading,
      )
    : null;

  useEffect(() => {
    let active = true;
    if (!activeBackfillSnapshotPath) {
      setBackfillSnapshotLoadingPath(null);
      return () => {
        active = false;
      };
    }
    if (backfillSnapshotPreviews[activeBackfillSnapshotPath] || backfillSnapshotErrorByPath[activeBackfillSnapshotPath]) {
      return () => {
        active = false;
      };
    }
    setBackfillSnapshotLoadingPath(activeBackfillSnapshotPath);
    setBackfillSnapshotErrorByPath((current) => {
      if (!(activeBackfillSnapshotPath in current)) {
        return current;
      }
      const next = { ...current };
      delete next[activeBackfillSnapshotPath];
      return next;
    });
    api.getMsrpBackfillSnapshot(activeBackfillSnapshotPath)
      .then((preview) => {
        if (!active) return;
        setBackfillSnapshotPreviews((current) => ({
          ...current,
          [preview.path]: preview,
        }));
      })
      .catch((err: unknown) => {
        if (!active) return;
        setBackfillSnapshotErrorByPath((current) => ({
          ...current,
          [activeBackfillSnapshotPath]: err instanceof Error ? err.message : String(err),
        }));
      })
      .finally(() => {
        setBackfillSnapshotLoadingPath((current) => (current === activeBackfillSnapshotPath ? null : current));
      });
    return () => {
      active = false;
    };
  }, [activeBackfillSnapshotPath, backfillSnapshotErrorByPath, backfillSnapshotPreviews]);

  useEffect(() => {
    setSpotCheckBriefCopyStatus("idle");
    setSnapshotPreviewPathOverride(null);
  }, [selectedCountryKey, selectedEventId]);
  const selectedCountries = (selectedEvent?.countries ?? []).filter((item) => (
    selectedEvent ? timelineItemMatchesFilters(selectedEvent, item, directionFilter, evidenceFilter, auditFilter) : false
  ));
  const timeline = useMemo(
    () => filteredTimelineItems(events, directionFilter, evidenceFilter, auditFilter),
    [auditFilter, directionFilter, evidenceFilter, events],
  );
  const filteredTimelineCount = events.reduce(
    (sum, event) => sum + event.timeline.filter((item) => timelineItemMatchesFilters(event, item, directionFilter, evidenceFilter, auditFilter)).length,
    0,
  );
  const filteredBackfillCount = events.reduce(
    (sum, event) => sum + event.timeline.filter((item) => (
      timelineItemMatchesFilters(event, item, directionFilter, evidenceFilter, auditFilter)
      && item.evidence.backfilled
    )).length,
    0,
  );
  const filteredPriorityAuditCount = events.reduce(
    (sum, event) => sum + event.timeline.filter((item) => (
      timelineItemMatchesDirection(item, directionFilter)
      && timelineItemMatchesEvidence(item, evidenceFilter)
      && timelineItemMatchesAudit(event, item, "priority_audit")
    )).length,
    0,
  );
  const filteredBlockCount = events.reduce(
    (sum, event) => sum + event.timeline.filter((item) => (
      timelineItemMatchesDirection(item, directionFilter)
      && timelineItemMatchesEvidence(item, evidenceFilter)
      && timelineItemMatchesAudit(event, item, "block")
    )).length,
    0,
  );
  const filteredSampleCount = events.reduce(
    (sum, event) => sum + event.timeline.filter((item) => (
      timelineItemMatchesDirection(item, directionFilter)
      && timelineItemMatchesEvidence(item, evidenceFilter)
      && timelineItemMatchesAudit(event, item, "sample")
    )).length,
    0,
  );
  const spotCheckQueueCandidates = useMemo(
    () => buildSpotCheckQueueCandidates(events, directionFilter, evidenceFilter, auditFilter),
    [auditFilter, directionFilter, evidenceFilter, events],
  );
  const spotCheckQueue = useMemo(
    () => visibleSpotCheckQueue(spotCheckQueueCandidates),
    [spotCheckQueueCandidates],
  );
  const priceActionItems = useMemo(
    () => buildPriceActionItems(spotCheckQueueCandidates),
    [spotCheckQueueCandidates],
  );
  const visiblePriceActionItems = priceActionItems.slice(0, PRICE_ACTION_BOARD_LIMIT);
  const selectedPriceActionItem = priceActionItems.find((item) => item.key === selectedActionKey)
    ?? visiblePriceActionItems[0]
    ?? null;
  const resolvedSelectedActionKey = selectedPriceActionItem?.key ?? null;
  const selectedActionIsOffer = selectedActionKey?.startsWith("offer:") ?? false;
  const spotCheckQueueLimitLabel = Math.min(SPOT_CHECK_QUEUE_LIMIT, spotCheckQueueCandidates.length);
  const spotCheckQueueOverflowCount = Math.max(0, spotCheckQueueCandidates.length - spotCheckQueue.length);
  const filteredCampaignBoundaryCount = spotCheckQueueCandidates.filter(isCampaignBoundarySpotCheckItem).length;
  const spotCheckDecisionSummary = buildSpotCheckDecisionSummary(spotCheckQueueCandidates);
  const selectedSpotCheckKey = selectedEventId && selectedCountryKey ? `${selectedEventId}|${selectedCountryKey}` : null;
  const selectedSpotCheckIndex = spotCheckQueueCandidates.findIndex((item) => item.key === selectedSpotCheckKey);
  const selectedSpotCheckQueueItem = selectedSpotCheckIndex >= 0 ? spotCheckQueueCandidates[selectedSpotCheckIndex] : null;
  const previousSpotCheckQueueItem = selectedSpotCheckIndex > 0 ? spotCheckQueueCandidates[selectedSpotCheckIndex - 1] : null;
  const nextSpotCheckQueueItem = selectedSpotCheckIndex >= 0 && selectedSpotCheckIndex < spotCheckQueueCandidates.length - 1
    ? spotCheckQueueCandidates[selectedSpotCheckIndex + 1]
    : null;
  const campaignBoundaryFocusFromDate = currentYearWindowOption().fromDate ?? null;
  const campaignBoundaryFocusReady = Boolean(data)
    && (data?.mode ?? "live") === "live"
    && data?.filters.country === null
    && data?.filters.brand === null
    && data?.filters.fromDate === campaignBoundaryFocusFromDate
    && data?.filters.thresholdPct === 0;
  const campaignBoundaryFiltersReady = mode === "live"
    && selectedWindowOption.fromDate === campaignBoundaryFocusFromDate
    && directionFilter === "drops"
    && evidenceFilter === "campaign_promotion"
    && auditFilter === "priority_audit"
    && countryFilter === "all"
    && brandFilter === "all"
    && thresholdPct === 0;

  useEffect(() => {
    if (!pendingCampaignBoundaryFocus) return;
    if (!campaignBoundaryFiltersReady) return;
    if (!campaignBoundaryFocusReady) return;
    const firstBoundaryItem = spotCheckQueueCandidates.find(isCampaignBoundarySpotCheckItem);
    if (firstBoundaryItem) {
      selectSpotCheckQueueItem(firstBoundaryItem);
      setPendingCampaignBoundaryFocus(false);
      return;
    }
    if (!loading && data) {
      setPendingCampaignBoundaryFocus(false);
    }
  }, [campaignBoundaryFiltersReady, campaignBoundaryFocusReady, data, loading, pendingCampaignBoundaryFocus, spotCheckQueueCandidates]);

  const showLaunchAlerts = directionFilter === "all" && evidenceFilter === "all";
  const countryOptions = useMemo(() => {
    const countries = new Map<string, string>();
    const labels = new Set<string>();
    batchACoverage?.countries.forEach((item) => {
      countries.set(item.code, item.countryLabel);
      labels.add(item.countryLabel);
    });
    rawEvents.forEach((event) => event.countries.forEach((item) => {
      if (!labels.has(item.countryLabel)) {
        countries.set(item.country, item.countryLabel);
        labels.add(item.countryLabel);
      }
    }));
    if (showLaunchAlerts) {
      launchAlerts.forEach((item) => {
        if (!labels.has(item.countryLabel)) {
          countries.set(item.country, item.countryLabel);
          labels.add(item.countryLabel);
        }
      });
    }
    rawOfferSignals.forEach((item) => {
      if (!labels.has(item.countryLabel)) {
        countries.set(item.country, item.countryLabel);
        labels.add(item.countryLabel);
      }
    });
    return Array.from(countries.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  }, [batchACoverage, launchAlerts, rawEvents, rawOfferSignals, showLaunchAlerts]);
  const brandOptions = useMemo(() => {
    const brands = new Set(rawEvents.map((event) => event.brand));
    if (showLaunchAlerts) {
      launchAlerts.forEach((item) => brands.add(item.brand));
    }
    rawOfferSignals.forEach((item) => brands.add(item.brand));
    return Array.from(brands).sort();
  }, [launchAlerts, rawEvents, rawOfferSignals, showLaunchAlerts]);
  const activeCountryScopeLabel = countryScopeLabel(mode, countryFilter, countryOptions);
  const activeBrandScopeLabel = brandScopeLabel(brandFilter);
  const activeWindowScopeLabel = windowScopeLabel(selectedWindowOption);
  const activeDemoScopeLabel = demoScopeLabel(data);
  const missingLengthEvents = events.filter((event) => event.lengthMissing);
  const swedenTimelineItems = rawEvents.flatMap((event) => event.timeline.filter(isSwedenTimelineItem));
  const swedenOfficialDropSignalCount = swedenTimelineItems.filter(isOfficialDropSignal).length;
  const swedenLaunchAlerts = launchAlerts.filter(isSwedenLaunchAlert);
  const swedenOfferSignalCount = rawOfferSignals.filter(isSwedenOfferSignal).length;
  const swedenCoverage = batchACoverage?.countries.find((item) => isSwedenValue(item.code) || isSwedenValue(item.countryLabel)) ?? null;
  const swedenLaunchBaselineCount = swedenCoverage?.launchCandidateCount ?? swedenLaunchAlerts.length;
  const swedenBackfillPeriodCount = swedenCoverage?.backfillPeriodCount ?? swedenOfficialDropSignalCount;
  const swedenCurrentRows = swedenCoverage?.currentRows ?? null;
  const swedenStatusVisible = Boolean(data)
    && (mode === "sweden_demo" || isSwedenValue(countryFilter) || isSwedenValue(activeCountryScopeLabel) || isSwedenValue(data?.filters.country));
  const launchSummaryCount = swedenStatusVisible
    ? swedenLaunchBaselineCount
    : data?.summary.launchAlertCount ?? launchAlerts.length;
  const displayedLaunchAlerts = showLaunchAlerts
    ? (
        swedenLaunchAlerts.length > 0
          ? [...swedenLaunchAlerts, ...launchAlerts.filter((item) => !swedenLaunchAlerts.includes(item))]
          : launchAlerts
      )
    : [];

  function selectEvent(eventId: string): void {
    const nextEvent = events.find((event) => event.eventId === eventId) ?? null;
    const nextCountry = nextEvent?.countries.find((item) => (
      timelineItemMatchesDirection(item, directionFilter)
      && timelineItemMatchesEvidence(item, evidenceFilter)
    )) ?? nextEvent?.countries[0] ?? null;
    setSelectedItem({
      eventId,
      countryKey: nextCountry ? countryKey(nextCountry) : null,
    });
    setDeckOpen(true);
    setDeckTab("overview");
  }

  function selectTimeline(index: number): void {
    const safeIndex = Math.max(0, Math.min(index, timeline.length - 1));
    const item = timeline[safeIndex];
    if (!item) return;
    selectTimelineItem(item);
    setTimelineIndex(safeIndex);
  }

  function selectTimelineItem(item: MsrpMonitoringTimelineEvent): void {
    const event = events.find((candidate) => (
      candidate.brand === item.brand
      && candidate.jatoModel === item.jatoModel
      && candidate.jatoPowertrain === item.jatoPowertrain
    ));
    if (event) {
      setSelectedActionKey(`move:${event.eventId}|${countryKey(item)}`);
      setSelectedItem({
        eventId: event.eventId,
        countryKey: countryKey(item),
      });
      setDeckTab("timeline");
      setDeckOpen(true);
    }
  }

  function selectSpotCheckQueueItem(queueItem: SpotCheckQueueItem): void {
    setSelectedActionKey(`move:${queueItem.key}`);
    setSelectedItem({
      eventId: queueItem.eventId,
      countryKey: countryKey(queueItem.item),
    });
    setDeckTab("source");
    setDeckOpen(true);
  }

  function selectCountrySourceItem(item: MsrpMonitoringTimelineEvent): void {
    const event = events.find((candidate) => (
      candidate.brand === item.brand
      && candidate.jatoModel === item.jatoModel
      && candidate.jatoPowertrain === item.jatoPowertrain
    ));
    if (event) {
      setSelectedActionKey(`move:${event.eventId}|${countryKey(item)}`);
    }
    setSelectedItem({
      eventId: event?.eventId ?? selectedEventId,
      countryKey: countryKey(item),
    });
    setDeckTab("source");
    setDeckOpen(true);
  }

  function selectCoverageCountry(item: MsrpBatchACountryCoverage): void {
    setMode("live");
    setCountryFilter(item.code);
    setRefreshTick((value) => value + 1);
  }

  function selectLaunchAlert(alert: MsrpLaunchAlert): void {
    setSelectedLaunchAlertId(alert.alertId);
    const event = events.find((candidate) => (
      candidate.brand === alert.brand
      && candidate.jatoModel === alert.jatoModel
      && candidate.jatoPowertrain === alert.jatoPowertrain
    ));
    if (event) {
      selectEvent(event.eventId);
    }
  }

  function selectOfferSignal(signal: MsrpOfferSignal): void {
    setSelectedActionKey(`offer:${signal.signalId}`);
    setSelectedOfferSignalId(signal.signalId);
    setDeckTab("offers");
    setDeckOpen(true);
  }

  function selectTopCountryScope(value: string): void {
    if (mode === "sweden_swiss_demo") {
      setCountryFilter(value);
      return;
    }
    setMode("live");
    setCountryFilter(value);
  }

  function selectPriceActionItem(item: PriceActionItem): void {
    setSelectedActionKey(item.key);
    if (item.kind === "msrp_move") {
      selectSpotCheckQueueItem(item.queueItem);
      return;
    }
    selectOfferSignal(item.signal);
  }

  function applySwedenYtdBackfillPreset(): void {
    const ytdOption = currentYearWindowOption();
    setMode("live");
    setWindowOptionId(ytdOption.id);
    setDirectionFilter("drops");
    setEvidenceFilter("campaign_promotion");
    setAuditFilter("priority_audit");
    setCountryFilter("SE");
    setBrandFilter("all");
    setThresholdPct(0);
    setDeckTab("source");
    setDeckOpen(true);
    setRefreshTick((value) => value + 1);
  }

  function viewSwedenLaunchBaselines(): void {
    const ytdOption = currentYearWindowOption();
    setMode("live");
    setWindowOptionId(ytdOption.id);
    setDirectionFilter("all");
    setEvidenceFilter("all");
    setAuditFilter("all");
    setCountryFilter("SE");
    setBrandFilter("all");
    setThresholdPct(0);
    setDeckTab("filters");
    setDeckOpen(true);
    setRefreshTick((value) => value + 1);
  }

  function applySwedenSwissDemoPreset(): void {
    const ytdOption = currentYearWindowOption();
    setMode("sweden_swiss_demo");
    setWindowOptionId(ytdOption.id);
    setDirectionFilter("all");
    setEvidenceFilter("all");
    setAuditFilter("all");
    setCountryFilter(DEFAULT_SWEDEN_SWISS_DEMO_COUNTRY);
    setBrandFilter("all");
    setThresholdPct(0);
    setDeckTab("overview");
    setDeckOpen(true);
    setRefreshTick((value) => value + 1);
  }

  function focusCampaignBoundarySpotChecks(): void {
    const ytdOption = currentYearWindowOption();
    setMode("live");
    setWindowOptionId(ytdOption.id);
    setDirectionFilter("drops");
    setEvidenceFilter("campaign_promotion");
    setAuditFilter("priority_audit");
    setCountryFilter("all");
    setBrandFilter("all");
    setThresholdPct(0);
    setDeckTab("filters");
    setDeckOpen(true);
    setPendingCampaignBoundaryFocus(true);
    setRefreshTick((value) => value + 1);
  }

  function applyAllLiveDropsPreset(): void {
    setMode("live");
    setWindowOptionId("30d");
    setDirectionFilter("drops");
    setEvidenceFilter("all");
    setAuditFilter("all");
    setCountryFilter("all");
    setBrandFilter("all");
    setThresholdPct(0);
    setRefreshTick((value) => value + 1);
  }

  function spotCheckBriefCopyLabel(): string {
    if (spotCheckBriefCopyStatus === "copied") return "Copied";
    if (spotCheckBriefCopyStatus === "failed") return "Failed";
    return "Copy";
  }

  function copySpotCheckBrief(): void {
    if (!spotCheckBrief) return;
    void copyTextToClipboard(spotCheckBrief.lines.join("\n")).then((copied) => {
      setSpotCheckBriefCopyStatus(copied ? "copied" : "failed");
      window.setTimeout(() => setSpotCheckBriefCopyStatus("idle"), 1800);
    });
  }

  return (
    <section className="msrp-monitor-page">
      <header className="msrp-monitor-topbar">
        <div>
          <p className="msrp-monitor-kicker">Market Monitor / 市场监控</p>
          <h1>MSRP监控</h1>
          <p className="msrp-monitor-subtitle">默认聚焦 price drop，跨国家车型调价聚合、国家展开、trim/source evidence 和时间轴追踪。</p>
        </div>
        <div className="msrp-monitor-header-meta">
          <span>{monitorModeLabel(mode)}</span>
          <span>{activeCountryScopeLabel}</span>
          <span>{activeBrandScopeLabel}</span>
          <span>{directionFilterLabel(directionFilter)}</span>
          <span>{activeWindowScopeLabel}</span>
          {activeDemoScopeLabel ? <span>{activeDemoScopeLabel}</span> : null}
          <span>{auditFilterLabel(auditFilter)}</span>
          <span>{evidenceFilterLabel(evidenceFilter)}</span>
          <span>{loading && data ? "Updating..." : `Synced ${formatDateTime(lastUpdatedAt)}`}</span>
        </div>
      </header>

      <div className="msrp-monitor-scope-bar" aria-label="MSRP monitor top filters">
        <div className="msrp-monitor-window-strip" aria-label="Window selector">
          <span>Window</span>
          {windowOptions.map((option) => (
            <button
              key={option.id}
              type="button"
              className={option.id === selectedWindowOption.id ? "is-active" : ""}
              onClick={() => setWindowOptionId(option.id)}
            >
              {option.label}
            </button>
          ))}
          <button type="button" className="is-refresh" onClick={() => setRefreshTick((value) => value + 1)} disabled={loading}>
            {loading && data ? "Updating" : "Refresh"}
          </button>
        </div>
        <div className="msrp-monitor-scope-row">
          <label htmlFor="msrp-monitor-country-scope">
            <span>Country</span>
            <select id="msrp-monitor-country-scope" value={mode === "sweden_demo" ? "SE" : countryFilter} onChange={(event) => selectTopCountryScope(event.target.value)}>
              <option value="all">All countries</option>
              {countryOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
            </select>
          </label>
          <label htmlFor="msrp-monitor-brand-scope">
            <span>Brand</span>
            <select id="msrp-monitor-brand-scope" value={brandFilter} onChange={(event) => setBrandFilter(event.target.value)}>
              <option value="all">All brands</option>
              {brandOptions.map((value) => <option key={value} value={value}>{value}</option>)}
            </select>
          </label>
          <label htmlFor="msrp-monitor-direction-scope">
            <span>Direction</span>
            <select id="msrp-monitor-direction-scope" value={directionFilter} onChange={(event) => setDirectionFilter(event.target.value as DirectionFilter)}>
              {DIRECTION_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label htmlFor="msrp-monitor-evidence-scope">
            <span>Evidence</span>
            <select id="msrp-monitor-evidence-scope" value={evidenceFilter} onChange={(event) => setEvidenceFilter(event.target.value as EvidenceFilter)}>
              {EVIDENCE_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label htmlFor="msrp-monitor-audit-scope">
            <span>Audit</span>
            <select id="msrp-monitor-audit-scope" value={auditFilter} onChange={(event) => setAuditFilter(event.target.value as AuditFilter)}>
              {AUDIT_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
        </div>
      </div>

      <DeckFloatingDrawer
        open={deckOpen}
        onOpenChange={setDeckOpen}
        triggerPrimary="MSRP Deck"
        triggerSecondaryOpen="收起"
        triggerSecondaryClosed="打开"
        eyebrow={mode === "live" ? "Live monitor" : monitorModeLabel(mode)}
        title={selectedEvent ? eventLabel(selectedEvent) : "MSRP controls"}
        ariaLabel="MSRP monitoring floating deck"
        className="msrp-monitor-floating-drawer"
        panelClassName="msrp-monitor-floating-panel"
        footer={(
          <div className="msrp-monitor-floating-footer">
            <span className="market-scan-toolbar-chip">{events.length}/{data?.summary.eventCount ?? 0} events</span>
            <span className="market-scan-toolbar-chip">{activeCountryScopeLabel}</span>
            <span className="market-scan-toolbar-chip">{directionFilterLabel(directionFilter)}</span>
            <span className="market-scan-toolbar-chip">{auditFilterLabel(auditFilter)}</span>
            <span className="market-scan-toolbar-chip">{evidenceFilterLabel(evidenceFilter)}</span>
            <span className="market-scan-toolbar-chip">{formatDateTime(lastUpdatedAt)}</span>
          </div>
        )}
      >
        <DeckControlTabs
          tabs={DECK_TABS}
          activeKey={deckTab}
          onChange={setDeckTab}
          ariaLabel="MSRP monitoring deck tabs"
        />

        {deckTab === "filters" ? (
          <div className="deck-panel-grid msrp-monitor-filter-grid">
            <label className="market-scan-field">
              <span>Mode</span>
              <select value={mode} onChange={(event) => setMode(event.target.value as MonitorMode)}>
                {MODE_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Window</span>
              <select value={selectedWindowOption.id} onChange={(event) => setWindowOptionId(event.target.value)}>
                {windowOptions.map((option) => <option key={option.id} value={option.id}>{option.label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Change</span>
              <select value={thresholdPct} onChange={(event) => setThresholdPct(Number(event.target.value))}>
                {THRESHOLD_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Direction</span>
              <select value={directionFilter} onChange={(event) => setDirectionFilter(event.target.value as DirectionFilter)}>
                {DIRECTION_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Audit</span>
              <select value={auditFilter} onChange={(event) => setAuditFilter(event.target.value as AuditFilter)}>
                {AUDIT_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Evidence</span>
              <select value={evidenceFilter} onChange={(event) => setEvidenceFilter(event.target.value as EvidenceFilter)}>
                {EVIDENCE_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Country</span>
              <select value={countryFilter} onChange={(event) => setCountryFilter(event.target.value)} disabled={mode === "sweden_demo"}>
                <option value="all">All</option>
                {countryOptions.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Brand</span>
              <select value={brandFilter} onChange={(event) => setBrandFilter(event.target.value)}>
                <option value="all">All</option>
                {brandOptions.map((value) => <option key={value} value={value}>{value}</option>)}
              </select>
            </label>
            <label className="market-scan-field">
              <span>Refresh</span>
              <select value={refreshIntervalSeconds} onChange={(event) => setRefreshIntervalSeconds(Number(event.target.value))}>
                {REFRESH_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
            </label>
            <div className="market-scan-field market-scan-field-actions">
              <span>Sync</span>
              <button type="button" className="btn btn-primary btn-sm" onClick={() => setRefreshTick((value) => value + 1)} disabled={loading}>
                Refresh
              </button>
              <small className="msrp-monitor-filter-sync">{loading && data ? "Updating..." : `Synced ${formatDateTime(lastUpdatedAt)}`}</small>
            </div>
            <div className="market-scan-field msrp-monitor-preset-actions">
              <span>Preset</span>
              <button type="button" className="btn btn-ghost btn-sm" onClick={applySwedenYtdBackfillPreset}>
                Sweden 2026 official drops
              </button>
              <button type="button" className="btn btn-ghost btn-sm" onClick={applyAllLiveDropsPreset}>
                All live drops
              </button>
            </div>
          </div>
        ) : null}

        {deckTab === "overview" && selectedEvent ? (
          <div className="msrp-monitor-deck-section">
            <div className="msrp-monitor-deck-stats">
              <div className={`msrp-monitor-audit-card ${auditClass(selectedEvent.auditPriority)}`}>
                <span>Audit</span>
                <strong>{auditLabel(selectedEvent.auditPriority)}</strong>
                <small>{samplingBucketLabel(selectedEvent.samplingBucket)}</small>
              </div>
              <div>
                <span>Drop range</span>
                <strong>{formatPct(selectedEvent.minChangePct)} / {formatPct(selectedEvent.maxChangePct)}</strong>
                <small>{changePctBasisLabel(selectedEvent.changePctBasis)}</small>
              </div>
              <div><span>Current median</span><strong>{formatCurrency(selectedEvent.medianCurrentMsrpEur)}</strong></div>
              <div>
                <span>Length</span>
                <strong>{selectedEvent.lengthMm ? `${selectedEvent.lengthMm} mm` : "Missing"}</strong>
                <small>{selectedEvent.lengthSource ?? "no length source"}</small>
              </div>
              <div><span>Confidence</span><strong>{selectedEvent.confidence}</strong></div>
              <div>
                <span>JATO sales</span>
                <strong>{formatNumber(selectedEvent.sales?.totalSales ?? null)}</strong>
                <small>{selectedEvent.sales ? `${selectedEvent.sales.matchedRowCount} rows · ${selectedEvent.sales.matchedCountryCount} markets` : "not matched"}</small>
              </div>
            </div>
            <div className="msrp-monitor-signal-list">
              <span className={`msrp-monitor-audit-pill ${auditClass(selectedEvent.auditPriority)}`}>{selectedEvent.auditActionLabel}</span>
              <span className={selectedEvent.multiCountrySync ? "is-good" : ""}>Multi-country sync: {selectedEvent.multiCountrySync ? "yes" : "no"}</span>
              <span>Review flags: {selectedEvent.reviewRequiredCount}</span>
              <span>Potential false positives: {selectedEvent.suspectedFalsePositiveCount}</span>
              <span>Risk: {riskLabel(selectedEvent)}</span>
              <span>Lifecycle: {selectedEvent.lifecycleStatus ?? "active"}</span>
              {selectedEvent.backfilled ? <span>Historical backfill: {selectedEvent.backfillEventCount ?? 0}</span> : null}
              {selectedCountryEvent?.evidence.backfilled ? (
                <span className="is-backfill-signal">Evidence: {backfillKindLabel(selectedCountryEvent.evidence.backfillKind)}</span>
              ) : null}
              {selectedCountryEvent?.evidence.backfilled ? (
                <span className="is-boundary">{backfillBoundaryLabel(selectedCountryEvent)}</span>
              ) : null}
              {selectedCountryEvent?.evidence.backfillValidUntil ? (
                <span>Valid until: {selectedCountryEvent.evidence.backfillValidUntil}</span>
              ) : null}
              {selectedEvent.auditReasons.slice(0, 4).map((reason) => <span key={reason} title={reason}>{auditReasonLabel(reason)}</span>)}
            </div>
            <MsrpSalesEffectChart event={selectedEvent} />
            {missingLengthEvents.length > 0 ? (
              <div className="msrp-monitor-missing-length">
                <strong>Length missing</strong>
                {missingLengthEvents.slice(0, 5).map((event) => (
                  <button key={event.eventId} type="button" onClick={() => selectEvent(event.eventId)}>
                    {eventLabel(event)} · {event.jatoPowertrain}
                  </button>
                ))}
              </div>
            ) : null}
          </div>
        ) : null}

        {deckTab === "countries" && selectedEvent ? (
          <div className="msrp-monitor-country-list">
            {selectedCountries.map((item) => {
              const effectiveAudit = higherAuditPriority(selectedEvent, item);
              return (
                <button
                  key={countryKey(item)}
                  type="button"
                  className={countryKey(item) === selectedCountryKey ? "is-selected" : ""}
                  onClick={() => setSelectedItem({
                    eventId: selectedEvent.eventId,
                    countryKey: countryKey(item),
                  })}
                >
                  <span><strong>{item.countryLabel}</strong><small>{item.jatoTrim}</small></span>
                  <b title={changePctBasisLabel(item.changePctBasis)}>
                    {formatPct(item.changePct)}
                    <small>{auditLabel(effectiveAudit.priority)} · {changePctBasisShortLabel(item.changePctBasis)}</small>
                  </b>
                </button>
              );
            })}
          </div>
        ) : null}

        {deckTab === "timeline" ? (
          <div className="msrp-monitor-timeline">
            <MsrpTimelinePriceView
              event={selectedEvent}
              selectedKey={selectedCountryKey}
              directionFilter={directionFilter}
              evidenceFilter={evidenceFilter}
              onSelect={selectTimelineItem}
            />
            <input
              type="range"
              min={0}
              max={Math.max(0, timeline.length - 1)}
              value={Math.min(timelineIndex, Math.max(0, timeline.length - 1))}
              onChange={(event) => selectTimeline(Number(event.target.value))}
            />
            <div className="msrp-monitor-timeline-list">
              {timeline.slice(-12).map((item, index, list) => {
                const globalIndex = timeline.length - list.length + index;
                return (
                  <button key={`${item.priceHistoryId}-${globalIndex}`} type="button" onClick={() => selectTimeline(globalIndex)}>
                          <span>{formatTime(item.changedAtUtc)}</span>
                          <strong>{item.brand} {item.jatoModel}</strong>
                          <small>{item.countryLabel} · {item.jatoTrim || "trim"} · {formatPct(item.changePct)} · {changePctBasisShortLabel(item.changePctBasis)}</small>
                        </button>
                      );
                    })}
            </div>
          </div>
        ) : null}

        {deckTab === "offers" ? (
          <MsrpOfferDeck
            signals={offerSignals}
            selectedSignalId={selectedOfferSignalId}
            countryValue={mode === "sweden_demo" ? "SE" : countryFilter}
            countryOptions={countryOptions}
            onCountryChange={selectTopCountryScope}
            onSelect={selectOfferSignal}
          />
        ) : null}

        {deckTab === "source" && selectedCountryEvent ? (
          <div className="msrp-monitor-source-panel">
            {selectedActionIsOffer && selectedOfferSignal ? <MsrpOfferSignalSource signal={selectedOfferSignal} /> : null}
            {spotCheckQueueCandidates.length > 1 && selectedSpotCheckQueueItem ? (
              <div className="msrp-monitor-source-queue-nav">
                <button
                  type="button"
                  disabled={!previousSpotCheckQueueItem}
                  aria-label="Previous spot-check item"
                  title="Previous spot-check item"
                  onClick={() => {
                    if (previousSpotCheckQueueItem) selectSpotCheckQueueItem(previousSpotCheckQueueItem);
                  }}
                >
                  ‹
                </button>
                <span>
                  <strong>Spot-check item {selectedSpotCheckIndex + 1}/{spotCheckQueueCandidates.length}</strong>
                  <small>{selectedSpotCheckQueueItem.modelLabel} · {selectedSpotCheckQueueItem.item.jatoTrim || "trim"}</small>
                  <small>
                    {spotCheckQueueOverflowCount > 0
                      ? `Full filtered queue · top ${spotCheckQueueLimitLabel} shown as cards · ${spotCheckQueueOverflowCount} beyond cards`
                      : `Full filtered queue · all ${spotCheckQueueCandidates.length} shown as cards`}
                  </small>
                </span>
                <button
                  type="button"
                  disabled={!nextSpotCheckQueueItem}
                  aria-label="Next spot-check item"
                  title="Next spot-check item"
                  onClick={() => {
                    if (nextSpotCheckQueueItem) selectSpotCheckQueueItem(nextSpotCheckQueueItem);
                  }}
                >
                  ›
                </button>
              </div>
            ) : null}
            {spotCheckBrief ? (
              <div className={`msrp-monitor-source-verdict ${spotCheckBrief.decision.className}`}>
                <strong>{spotCheckBrief.decision.label}</strong>
                <span>{spotCheckBrief.decision.detail}</span>
              </div>
            ) : null}
            <div className="msrp-monitor-source-actions" aria-label="Source evidence actions">
              {selectedCountryEvent.evidence.backfillEvidenceUrl ? (
                <a href={selectedCountryEvent.evidence.backfillEvidenceUrl} target="_blank" rel="noreferrer">
                  Open backfill evidence
                </a>
              ) : null}
              {selectedCountryEvent.evidence.observationSourceUrl || selectedCountryEvent.evidence.sourceUrl ? (
                <a
                  href={selectedCountryEvent.evidence.observationSourceUrl ?? selectedCountryEvent.evidence.sourceUrl ?? undefined}
                  target="_blank"
                  rel="noreferrer"
                >
                  Open observed source
                </a>
              ) : null}
              {selectedCountryEvent.evidence.backfillSnapshotPath ? (
                <button type="button" onClick={() => setSnapshotPreviewPathOverride(null)}>
                  Preview primary snapshot
                </button>
              ) : null}
              {selectedCountryEvent.evidence.backfillPayloadHash ? (
                <span title={selectedCountryEvent.evidence.backfillPayloadHash}>
                  {selectedCountryEvent.evidence.backfillPayloadHash}
                </span>
              ) : null}
            </div>
            {selectedCountryEvent.evidence.backfilled ? (
              <div className="msrp-monitor-source-boundary">
                <strong>{backfillKindLabel(selectedCountryEvent.evidence.backfillKind)}</strong>
                <span>{backfillBoundaryLabel(selectedCountryEvent)}</span>
              </div>
            ) : null}
            <dl>
              <dt>Source status</dt><dd>{selectedCountryEvent.sourceStatus}</dd>
              <dt>Audit</dt><dd>{selectedCountryAudit ? auditLabel(selectedCountryAudit.priority) : auditLabel(selectedCountryEvent.auditPriority)}</dd>
              <dt>Action</dt><dd>{selectedCountryAudit?.actionLabel ?? selectedCountryEvent.auditActionLabel}</dd>
              <dt>Bucket</dt><dd>{samplingBucketLabel(selectedCountryAudit?.bucket ?? selectedCountryEvent.samplingBucket)}</dd>
              <dt>Review flag</dt><dd>{selectedCountryEvent.reviewFlag ? "Yes" : "No"}</dd>
              <dt>Observed at</dt><dd>{formatDateTime(selectedCountryEvent.evidence.observedAtUtc)}</dd>
              <dt>Dryrun run</dt><dd>{selectedCountryEvent.evidence.dryrunRunId ?? "-"}</dd>
              <dt>Batch</dt><dd>{selectedCountryEvent.evidence.scrapeBatchCode ?? "-"}</dd>
              <dt>Source code</dt><dd>{selectedCountryEvent.source.sourceCode ?? "-"}</dd>
              <dt>Change basis</dt><dd>{changePctBasisLabel(selectedCountryEvent.changePctBasis)}</dd>
              <dt>Source URL</dt>
              <dd>
                {selectedCountryEvent.evidence.observationSourceUrl || selectedCountryEvent.evidence.sourceUrl ? (
                  <a href={selectedCountryEvent.evidence.observationSourceUrl ?? selectedCountryEvent.evidence.sourceUrl ?? undefined} target="_blank" rel="noreferrer">
                    {selectedCountryEvent.evidence.observationSourceUrl ?? selectedCountryEvent.evidence.sourceUrl}
                  </a>
                ) : "-"}
              </dd>
              <dt>Payload hash</dt><dd>{selectedCountryEvent.evidence.sourcePayloadHash ?? "-"}</dd>
              <dt>Lifecycle</dt><dd>{selectedCountryEvent.lifecycleStatus ?? "active"}</dd>
              <dt>Backfill</dt>
              <dd>
                {selectedCountryEvent.evidence.backfilled
                  ? selectedCountryEvent.evidence.backfillSourceLabel ?? selectedCountryEvent.evidence.backfillKind ?? "Historical backfill"
                  : "-"}
              </dd>
              <dt>Backfill kind</dt><dd>{selectedCountryEvent.evidence.backfilled ? backfillKindLabel(selectedCountryEvent.evidence.backfillKind) : "-"}</dd>
              <dt>Backfill date</dt><dd>{selectedCountryEvent.evidence.backfillEffectiveDate ?? "-"}</dd>
              <dt>Valid until</dt><dd>{selectedCountryEvent.evidence.backfillValidUntil ?? "-"}</dd>
              <dt>Backfill role</dt><dd>{selectedCountryEvent.evidence.backfillEvidenceRole ?? "-"}</dd>
              <dt>Backfill URL</dt>
              <dd>
                {selectedCountryEvent.evidence.backfillEvidenceUrl ? (
                  <a href={selectedCountryEvent.evidence.backfillEvidenceUrl} target="_blank" rel="noreferrer">
                    {selectedCountryEvent.evidence.backfillEvidenceUrl}
                  </a>
                ) : "-"}
              </dd>
              <dt>Backfill snapshot</dt><dd>{selectedCountryEvent.evidence.backfillSnapshotPath ?? "-"}</dd>
              <dt>Backfill hash</dt><dd>{selectedCountryEvent.evidence.backfillPayloadHash ?? "-"}</dd>
              <dt>Demo</dt><dd>{selectedCountryEvent.evidence.demoBackfilled ? selectedCountryEvent.evidence.demoScenario ?? "Backfilled" : "-"}</dd>
            </dl>
            {selectedCountryEvent.evidence.backfilled ? (
              <div className="msrp-monitor-backfill-note">
                <strong>Historical evidence</strong>
                <span>{selectedCountryEvent.evidence.backfillNotes ?? "Backfilled price evidence is included in this monitoring event for targeted spot-checking."}</span>
              </div>
            ) : null}
            {selectedCountryEvent.evidence.relatedOfficialEvidence?.length ? (
              <div className="msrp-monitor-related-evidence">
                <header>
                  <strong>Related official evidence</strong>
                  <span>{selectedCountryEvent.evidence.relatedOfficialEvidence.length} source{selectedCountryEvent.evidence.relatedOfficialEvidence.length === 1 ? "" : "s"}</span>
                </header>
                <div>
                  {selectedCountryEvent.evidence.relatedOfficialEvidence.map((relatedEvidence, index) => (
                    <section key={`${relatedEvidence.url ?? relatedEvidence.snapshotPath ?? relatedEvidence.label ?? "related"}-${index}`}>
                      <strong>
                        {relatedEvidence.url ? (
                          <a href={relatedEvidence.url} target="_blank" rel="noreferrer">
                            {relatedEvidence.label ?? relatedEvidence.url}
                          </a>
                        ) : (
                          relatedEvidence.label ?? "Official evidence"
                        )}
                      </strong>
                      <em>{relatedOfficialEvidenceRoleLabel(selectedCountryEvent)}</em>
                      {relatedEvidence.snapshotPath ? (
                        <span>
                          {relatedEvidence.snapshotPath}
                          <button
                            type="button"
                            className={activeSnapshotPreviewPathOverride === relatedEvidence.snapshotPath ? "is-active" : ""}
                            onClick={() => setSnapshotPreviewPathOverride(relatedEvidence.snapshotPath ?? null)}
                          >
                            Preview snapshot
                          </button>
                        </span>
                      ) : null}
                      {relatedEvidence.payloadHash ? <small>{relatedEvidence.payloadHash}</small> : null}
                    </section>
                  ))}
                </div>
              </div>
            ) : null}
            {selectedCountryEvent.evidence.backfilled ? (
              <div className="msrp-monitor-evidence-checklist">
                <header>
                  <strong>Evidence checklist</strong>
                  <span>{evidenceChecklist.filter((item) => item.status === "pass").length}/{evidenceChecklist.length} passed</span>
                </header>
                <div>
                  {evidenceChecklist.map((item) => (
                    <span key={item.key} className={`is-${item.status}`} title={item.detail}>
                      <b>{item.status}</b>
                      {item.label}
                    </span>
                  ))}
                </div>
              </div>
            ) : null}
            {spotCheckBrief ? (
              <div className="msrp-monitor-spotcheck-brief">
                <header>
                  <div>
                    <strong>Spot-check brief</strong>
                    <span className={spotCheckBrief.statusClassName}>
                      {spotCheckBrief.statusLabel} · {spotCheckBrief.matchedCount}/{spotCheckBrief.totalCount}
                    </span>
                  </div>
                  <button type="button" className={`is-${spotCheckBriefCopyStatus}`} onClick={copySpotCheckBrief}>
                    {spotCheckBriefCopyLabel()}
                  </button>
                </header>
                <div className={`msrp-monitor-spotcheck-decision ${spotCheckBrief.decision.className}`}>
                  <strong>{spotCheckBrief.decision.label}</strong>
                  <span>{spotCheckBrief.decision.detail}</span>
                  <ul>
                    {spotCheckBrief.decision.actions.map((action) => <li key={action}>{action}</li>)}
                  </ul>
                </div>
                {spotCheckBrief.warningLabels.length > 0 ? (
                  <div className="msrp-monitor-spotcheck-brief-warnings">
                    {spotCheckBrief.warningLabels.map((label) => <span key={label}>{label}</span>)}
                  </div>
                ) : null}
                <pre>{spotCheckBrief.lines.join("\n")}</pre>
              </div>
            ) : null}
            {activeBackfillSnapshotPath ? (
              <div className="msrp-monitor-snapshot-preview">
                <header>
                  <div>
                    <strong>{snapshotPreviewTitle(activeSnapshotPreviewPathOverride)}</strong>
                    <span>{activeBackfillSnapshotPreview?.fileName ?? activeBackfillSnapshotPath}</span>
                  </div>
                  <div className="msrp-monitor-snapshot-preview-actions">
                    {activeSnapshotPreviewPathOverride ? (
                      <button type="button" onClick={() => setSnapshotPreviewPathOverride(null)}>
                        Primary snapshot
                      </button>
                    ) : null}
                    <small>
                      {activeBackfillSnapshotLoading
                        ? "Loading"
                        : activeBackfillSnapshotPreview
                          ? `${activeBackfillSnapshotPreview.status}${activeBackfillSnapshotPreview.sizeBytes !== null ? ` · ${formatBytes(activeBackfillSnapshotPreview.sizeBytes)}` : ""}${activeBackfillSnapshotPreview.truncated ? " · truncated" : ""}`
                        : "Not loaded"}
                    </small>
                  </div>
                </header>
                {activeBackfillSnapshotError ? <p>{activeBackfillSnapshotError}</p> : null}
                {!activeBackfillSnapshotError && activeBackfillSnapshotPreview && !activeBackfillSnapshotPreview.previewable ? <p>{activeBackfillSnapshotPreview.message}</p> : null}
                {!activeBackfillSnapshotError && activeBackfillSnapshotPreview?.content ? <pre>{activeBackfillSnapshotPreview.content}</pre> : null}
              </div>
            ) : null}
            {selectedCountryEvent.riskReasons.length > 0 ? (
              <div className="msrp-monitor-risk-reasons">
                {selectedCountryEvent.riskReasons.map((reason) => <span key={reason}>{reason}</span>)}
              </div>
            ) : null}
          </div>
        ) : null}

        {deckTab === "source" && !selectedCountryEvent && selectedOfferSignal ? (
          <div className="msrp-monitor-source-panel">
            <MsrpOfferSignalSource signal={selectedOfferSignal} />
          </div>
        ) : null}
      </DeckFloatingDrawer>

      {loading && !data ? <LoadingSurface mode="inline" label="加载 MSRP 监控" detail="读取 price history、source evidence 与调价事件" kicker="MSRP" /> : null}
      {error ? <div className="market-scan-state-card market-scan-state-card--error"><strong>Error</strong><p>{error}</p></div> : null}
      {data?.warnings.length ? (
        <div className="msrp-monitor-warning-list">
          {data.warnings.map((warning) => <span key={warning}>{warning}</span>)}
        </div>
      ) : null}
      {data?.demo?.enabled ? (
        <div className="msrp-monitor-demo-banner">
          <strong>{data.demo.country} demo</strong>
          <span>{data.demo.description}</span>
          <small>Backfilled scenario only; no synthetic price history was written to the database.</small>
        </div>
      ) : null}

      {data ? (
        <>
          {swedenStatusVisible ? (
            <section className="msrp-monitor-sweden-status" aria-label="Sweden 2026 MSRP monitor status">
              <header>
                <div>
                  <span>Sweden 2026</span>
                  <h2>Monitor status</h2>
                </div>
                <p>Official campaign/promotion drops are separated from launch baselines; this is monitored evidence, not a full-market absence proof.</p>
                <div className="msrp-monitor-sweden-status-actions">
                  <button type="button" onClick={applySwedenYtdBackfillPreset}>Review drop signals</button>
                  <button type="button" onClick={viewSwedenLaunchBaselines}>View launch baselines</button>
                </div>
              </header>
              <div className="msrp-monitor-sweden-status-grid">
                <div>
                  <span>Official drop signals</span>
                  <strong>{swedenOfficialDropSignalCount}</strong>
                  <small>{swedenBackfillPeriodCount} backfilled periods · campaign/promotion</small>
                </div>
                <div>
                  <span>Launch baselines</span>
                  <strong>{swedenLaunchBaselineCount}</strong>
                  <small>{showLaunchAlerts ? "Listed in All moves" : "Coverage count; switch All moves to list"}</small>
                </div>
                <div>
                  <span>Offer signals</span>
                  <strong>{swedenOfferSignalCount}</strong>
                  <small>Cash / finance / lease / benefit</small>
                </div>
                <div>
                  <span>Current rows</span>
                  <strong>{swedenCurrentRows ?? "-"}</strong>
                  <small>{swedenCoverage?.status ? coverageStatusLabel(swedenCoverage.status) : "Current official scope"}</small>
                </div>
              </div>
            </section>
          ) : null}

          <div className="msrp-monitor-summary">
            <div><span>Movement signals</span><strong>{events.length}/{data.summary.eventCount}</strong></div>
            <div><span>Price moves</span><strong>{filteredTimelineCount}/{data.summary.timelineEventCount}</strong></div>
            <div><span>Priority audit</span><strong>{filteredPriorityAuditCount}</strong></div>
            <button
              type="button"
              title="Use Sweden, current-year YTD, price drops and campaign/promotion evidence for official backfill spot-checking"
              onClick={applySwedenYtdBackfillPreset}
            >
              <span>Sweden 2026</span>
              <strong>YTD</strong>
              <small>Official drops</small>
            </button>
            <button
              type="button"
              data-testid="msrp-monitor-sweden-swiss-demo-preset"
              title="Open the Sweden and Swiss demo, defaulting to Switzerland"
              onClick={applySwedenSwissDemoPreset}
            >
              <span>Sweden + Swiss</span>
              <strong>Demo</strong>
              <small>Rolling 12M top30</small>
            </button>
            <button
              type="button"
              className={pendingCampaignBoundaryFocus ? "is-pending" : ""}
              title="Use current-year YTD, clear country and brand filters, then focus campaign/promotion boundary spot-checks"
              onClick={focusCampaignBoundarySpotChecks}
            >
              <span>Campaign boundary</span><strong>{filteredCampaignBoundaryCount}/{data.summary.campaignBoundaryCount}</strong>
              {pendingCampaignBoundaryFocus ? (
                <small>Opening source...</small>
              ) : data.summary.campaignBoundaryCount > SPOT_CHECK_QUEUE_LIMIT ? (
                <small>Showing top {SPOT_CHECK_QUEUE_LIMIT}</small>
              ) : null}
            </button>
            <div><span>Blocks</span><strong>{filteredBlockCount}</strong></div>
            <div><span>Samples</span><strong>{filteredSampleCount}</strong></div>
            <div><span>Backfilled signals</span><strong>{filteredBackfillCount}</strong></div>
            <div><span>Official offers</span><strong>{offerSignals.length}/{data.summary.offerSignalCount ?? rawOfferSignals.length}</strong></div>
            {showLaunchAlerts || swedenStatusVisible ? <div><span>Launch baselines</span><strong>{launchSummaryCount}</strong></div> : null}
            <div><span>Batch A backfill</span><strong>{batchACoverage ? `${batchACoverage.historicalBackfillCountryCount}/${batchACoverage.countryCount}` : "-"}</strong></div>
          </div>

          <div className="msrp-monitor-visual-grid">
            <MsrpEventChart events={events} selectedEventId={selectedEvent?.eventId ?? null} onSelect={selectEvent} />
            {rawOfferSignals.length > 0 ? (
              <MsrpOfferSignalVisual
                signals={offerSignals}
                selectedSignalId={selectedOfferSignalId}
                onSelect={selectOfferSignal}
              />
            ) : null}
          </div>

          <div className="msrp-monitor-action-layout">
            <PriceActionBoard
              items={visiblePriceActionItems}
              totalCount={priceActionItems.length}
              selectedKey={resolvedSelectedActionKey}
              onSelect={selectPriceActionItem}
            />
            <PriceActionStoryPanel
              item={selectedPriceActionItem}
              events={events}
              directionFilter={directionFilter}
              evidenceFilter={evidenceFilter}
              onSelectTimelineItem={selectTimelineItem}
            />
          </div>

          <div className="msrp-monitor-powertrain-legend">
            {Object.entries(data.powertrainColors).map(([powertrain, color]) => (
              <span key={powertrain}><i style={{ background: color }} />{powertrain}</span>
            ))}
          </div>

          {batchACoverage ? (
            <section className="msrp-monitor-coverage-panel">
              <header>
                <div>
                  <h2>Batch A monitoring coverage</h2>
                  <span>{batchACoverage.loadedCountryCount}/{batchACoverage.countryCount} loaded · {batchACoverage.historicalBackfillCountryCount} with historical backfill · {batchACoverage.launchCandidateCount} launch baselines</span>
                </div>
                <strong>{batchACoverage.batchCode}</strong>
              </header>
              <div className="msrp-monitor-coverage-grid">
                {batchACoverage.countries.map((item) => (
                  <button
                    key={item.code}
                    type="button"
                    className={`msrp-monitor-coverage-chip ${coverageStatusClass(item.status)}`}
                    onClick={() => selectCoverageCountry(item)}
                  >
                    <span><strong>{item.code}</strong>{item.countryLabel}</span>
                    <b>{coverageStatusLabel(item.status)}</b>
                    <small>{item.currentRows} rows · {item.backfillPeriodCount} backfill · {item.launchCandidateCount} launch</small>
                  </button>
                ))}
              </div>
            </section>
          ) : null}

          {displayedLaunchAlerts.length > 0 ? (
            <section className="msrp-monitor-launch-panel">
              <header>
                <div>
                  <h2>New launch price alerts</h2>
                  <span>{displayedLaunchAlerts.length} launch baselines in current window · {launchAlerts.length} total</span>
                </div>
                <strong>{formatDateTime(data.generatedAtUtc)}</strong>
              </header>
              <div className="msrp-monitor-launch-list">
                {displayedLaunchAlerts.map((alert) => (
                  <button
                    key={alert.alertId}
                    type="button"
                    className={`msrp-monitor-launch-card ${auditClass(alert.auditPriority)}${alert.alertId === selectedLaunchAlertId ? " is-selected" : ""}`}
                    onClick={() => selectLaunchAlert(alert)}
                  >
                    <span>
                      <strong>{launchAlertLabel(alert)}</strong>
                      <small>{alert.countryLabel} · {alert.jatoTrim || "trim"} · {formatTime(alert.launchedAtUtc)}</small>
                    </span>
                    <b>{formatNumber(alert.currentSourceMsrp)} {alert.sourceCurrency}<small>{formatCurrency(alert.currentMsrpEur)}</small></b>
                    <em>{auditLabel(alert.auditPriority)}</em>
                  </button>
                ))}
              </div>
            </section>
          ) : null}

          <section className="msrp-monitor-detail">
            <header>
              <h2>Country drilldown</h2>
              <span>{selectedEvent ? eventLabel(selectedEvent) : "-"}</span>
            </header>
            <div className="msrp-monitor-table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Country</th>
                    <th>Trim</th>
                    <th>Change</th>
                    <th>Audit</th>
                    <th>EUR normalized</th>
                    <th>Local currency</th>
                    <th>Source</th>
                    <th>Evidence</th>
                  </tr>
                </thead>
                <tbody>
                  {selectedEvent ? selectedCountries.map((item) => {
                    const effectiveAudit = higherAuditPriority(selectedEvent, item);
                    return (
                      <tr
                        key={countryKey(item)}
                        className={countryKey(item) === selectedCountryKey ? "is-selected" : ""}
                        tabIndex={0}
                        title="Open Source evidence"
                        onClick={() => selectCountrySourceItem(item)}
                        onKeyDown={(event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            selectCountrySourceItem(item);
                          }
                        }}
                      >
                        <td>{item.countryLabel}</td>
                        <td>{item.jatoTrim || "-"}</td>
                        <td><strong>{formatPct(item.changePct)}</strong><small>{formatTime(item.changedAtUtc)} · {changePctBasisShortLabel(item.changePctBasis)}</small></td>
                        <td>{auditLabel(effectiveAudit.priority)}<small title={effectiveAudit.bucket}>{samplingBucketLabel(effectiveAudit.bucket)}</small></td>
                        <td>{formatCurrency(item.oldMsrpEur)} → {formatCurrency(item.currentMsrpEur)}</td>
                        <td>{formatNumber(item.oldSourceMsrp)} → {formatNumber(item.currentSourceMsrp)} {item.sourceCurrency}</td>
                        <td>{item.sourceStatus}<small>{item.lifecycleStatus ?? item.source.sourceType ?? "-"}</small></td>
                        <td>
                          {eventEvidenceLabel(item)}
                          <small>
                            {item.evidence.backfilled
                              ? [item.evidence.backfillEffectiveDate, item.evidence.backfillValidUntil ? `until ${item.evidence.backfillValidUntil}` : null]
                                .filter(Boolean)
                                .join(" · ") || item.evidence.backfillEvidenceRole || "backfilled"
                              : item.evidence.scrapeBatchCode ?? "-"}
                          </small>
                        </td>
                      </tr>
                    );
                  }) : null}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </section>
  );
}
