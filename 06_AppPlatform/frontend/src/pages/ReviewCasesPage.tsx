import { ChangeEvent, FormEvent, Fragment, useEffect, useRef, useState } from "react";

import { api } from "../api/client";
import { AdminToolsNav } from "../components/AdminToolsNav";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LoadingSurface } from "../components/LoadingSurface";
import { LoopingCountStrip } from "../components/LoopingCountStrip";
import { RollingTickerCard } from "../components/RollingTickerCard";
import { ReviewDeliveryPanel } from "../components/ReviewDeliveryPanel";
import {
  TextSearchFilters,
  useTextSearchFilters,
} from "../components/TextSearchFilters";
import type { ReviewCandidateMatch, ReviewCase, ReviewCaseDetail, ReviewWorkbench } from "../types";
import {
  formatReviewTrimSummary,
  summarizeReviewTrimCollection,
} from "../utils/reviewCaseDisplay";
import {
  REVIEW_STATUS_FILTER_OPTIONS,
  getCurrentPriceMatchStatusBadgeClass,
  getCurrentPriceMatchStatusLabel,
  getReviewStatusBadgeClass,
  getReviewStatusLabel,
} from "../utils/reviewStatus";


interface ReviewCaseGroup {
  key: string;
  country: string;
  brand: string;
  model: string;
  items: ReviewCase[];
}

type MatchReasonPayload = Record<string, unknown>;

interface ConfidenceRuleComponent {
  key: string;
  label: string;
  applied: boolean;
  delta: number;
  evidence?: unknown;
}

interface ConfidenceRuleSummary {
  mode: string;
  base: number | null;
  total: number | null;
  components: ConfidenceRuleComponent[];
}


function formatMsrp(value?: number, currency = "EUR") {
  if (value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${new Intl.NumberFormat("en-IE", {
    maximumFractionDigits: 0,
    minimumFractionDigits: 0,
  }).format(value)} ${currency}`;
}

function formatSourceLink(url?: string) {
  if (!url) {
    return "-";
  }
  try {
    const parsed = new URL(url);
    const path = parsed.pathname === "/"
      ? ""
      : parsed.pathname.length > 24
        ? `${parsed.pathname.slice(0, 24)}…`
        : parsed.pathname;
    return `${parsed.hostname}${path}`;
  } catch {
    return url.length > 36 ? `${url.slice(0, 36)}…` : url;
  }
}

function formatDateTime(value?: string | null) {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}

function getReviewerName() {
  return (localStorage.getItem("jato_user_name") || "anonymous").trim() || "anonymous";
}

function isActionableStatus(status: string) {
  return status === "open" || status === "review_required";
}

function resolveReviewCaseModel(reviewCase: ReviewCase) {
  return reviewCase.officialModel || reviewCase.jatoModel || "待映射";
}

function buildReviewCaseGroupKey(reviewCase: ReviewCase) {
  return [reviewCase.country, reviewCase.brand, resolveReviewCaseModel(reviewCase)].join("::");
}

function groupReviewCases(reviewCases: ReviewCase[]) {
  const groups = new Map<string, ReviewCaseGroup>();

  reviewCases.forEach((reviewCase) => {
    const key = buildReviewCaseGroupKey(reviewCase);
    const existing = groups.get(key);

    if (existing) {
      existing.items.push(reviewCase);
      return;
    }

    groups.set(key, {
      key,
      country: reviewCase.country,
      brand: reviewCase.brand,
      model: resolveReviewCaseModel(reviewCase),
      items: [reviewCase],
    });
  });

  return Array.from(groups.values());
}

function summarizeReviewCaseTrims(reviewCases: ReviewCase[]) {
  return summarizeReviewTrimCollection(reviewCases);
}

function formatConfidenceRange(reviewCases: ReviewCase[]) {
  const values = reviewCases.map((reviewCase) => reviewCase.matchConfidence);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const minLabel = `${(min * 100).toFixed(0)}%`;
  const maxLabel = `${(max * 100).toFixed(0)}%`;

  return min === max ? minLabel : `${minLabel} - ${maxLabel}`;
}

function asRecord(value: unknown): MatchReasonPayload | null {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as MatchReasonPayload;
  }
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function formatConfidenceValue(value: number | null | undefined, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function formatEvidenceValue(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (Array.isArray(value)) {
    return value.map((item) => String(item)).join(" / ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

const EVKX_SPEC_LABELS: Record<string, string> = {
  bodyType: "Body Type",
  modelYear: "Model Year",
  peakPower: "Peak Power",
  torque: "Torque",
  topSpeed: "Top Speed",
  batteryNet: "Battery Net",
  batteryGross: "Battery Gross",
  maxDcCharging: "Max DC Charging",
  range: "Range",
  consumption: "Consumption",
  trunkCapacity: "Trunk Capacity",
  trailerWeight: "Trailer Weight",
  chargeport: "Chargeport",
};

function formatEvkxFieldLabel(key: string) {
  return EVKX_SPEC_LABELS[key]
    ?? key.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/_/g, " ");
}

function getEvkxSourceContext(
  reviewCase: ReviewCaseDetail | null,
) {
  const context = asRecord(reviewCase?.observation?.sourceContext ?? null);
  if (!context) {
    return null;
  }
  return String(context.source ?? "").toUpperCase() === "EVKX" ? context : null;
}

function formatCandidateScore(match: ReviewCandidateMatch) {
  return `${(match.score * 100).toFixed(1)}%`;
}

function getReviewCaseMatchReason(reviewCase: ReviewCase | ReviewCaseDetail): MatchReasonPayload | null {
  const directPayload = asRecord(reviewCase.matchReason);
  if (directPayload) {
    return directPayload;
  }
  if (!("observation" in reviewCase)) {
    return null;
  }
  const observationPayload = asRecord(reviewCase.observation);
  return observationPayload ? asRecord(observationPayload["matchReason"]) : null;
}

function getConfidenceRuleSummary(matchReason: MatchReasonPayload | null): ConfidenceRuleSummary | null {
  const confidenceRule = asRecord(matchReason ? matchReason["confidenceRule"] : null);
  if (!confidenceRule) {
    return null;
  }

  const rawComponents = confidenceRule["components"];
  const components = Array.isArray(rawComponents)
    ? rawComponents.flatMap((component, index) => {
      const record = asRecord(component);
      if (!record) {
        return [];
      }
      return [{
        key: String(record["key"] ?? `component-${index}`),
        label: String(record["label"] ?? record["key"] ?? "Unnamed component"),
        applied: Boolean(record["applied"]),
        delta: asNumber(record["delta"]) ?? 0,
        evidence: record["evidence"],
      }];
    })
    : [];

  return {
    mode: String(confidenceRule["mode"] ?? "weighted_profile"),
    base: asNumber(confidenceRule["base"]),
    total: asNumber(confidenceRule["total"]),
    components,
  };
}

function getMatchEvidenceEntries(matchReason: MatchReasonPayload | null) {
  const evidence = asRecord(matchReason ? matchReason["evidence"] : null);
  if (!evidence) {
    return [] as Array<{ key: string; value: string }>;
  }
  return Object.entries(evidence)
    .filter(([, value]) => value !== null && value !== undefined && value !== "")
    .map(([key, value]) => ({ key, value: formatEvidenceValue(value) }));
}

function formatRuleToken(token: string) {
  const upperToken = token.toUpperCase();
  if (["BEV", "PHEV", "HEV", "MHEV", "ICE"].includes(upperToken)) {
    return upperToken;
  }
  return token
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getAppliedRuleTokens(
  matchReason: MatchReasonPayload | null,
  options?: { includeTrim?: boolean },
) {
  const includeTrim = options?.includeTrim ?? true;
  const summary = getConfidenceRuleSummary(matchReason);
  if (!summary) {
    return [] as string[];
  }

  const tokens: string[] = [];
  summary.components.forEach((component) => {
    if (!component.applied) {
      return;
    }
    if (component.key.startsWith("trim_keyword_") && includeTrim) {
      tokens.push(formatRuleToken(component.key.replace("trim_keyword_", "")));
      return;
    }
    if (component.key.startsWith("price_band_")) {
      tokens.push(formatRuleToken(component.key.replace("price_band_", "")));
      return;
    }
    if (component.key.startsWith("powertrain_keyword_")) {
      tokens.push(formatRuleToken(component.key.replace("powertrain_keyword_", "")));
      return;
    }
    if (component.key.startsWith("powertrain_")) {
      tokens.push(formatRuleToken(component.key.replace("powertrain_", "")));
    }
  });

  return Array.from(new Set(tokens));
}

function getReviewCaseReasonBrief(reviewCase: ReviewCase) {
  const tokens = getAppliedRuleTokens(
    getReviewCaseMatchReason(reviewCase),
    { includeTrim: false },
  );
  return tokens.slice(0, 2).join(" · ");
}

function getGroupReasonBrief(reviewCases: ReviewCase[]) {
  const categories = new Set<string>();
  reviewCases.forEach((reviewCase) => {
    const summary = getConfidenceRuleSummary(getReviewCaseMatchReason(reviewCase));
    summary?.components.forEach((component) => {
      if (!component.applied) {
        return;
      }
      if (component.key.startsWith("trim_keyword_")) {
        categories.add("trim");
      } else if (component.key.startsWith("price_band_")) {
        categories.add("price");
      } else if (
        component.key.startsWith("powertrain_keyword_")
        || component.key.startsWith("powertrain_")
      ) {
        categories.add("powertrain");
      }
    });
  });
  return categories.size > 0 ? `依据 ${Array.from(categories).join(" / ")}` : "";
}

export function ReviewCasesPage() {
  const reviewCasePageSize = 500;

  /* ── list state ─────────────────────────────────── */
  const [cases, setCases] = useState<ReviewCase[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const [expandedGroups, setExpandedGroups] = useState<Record<string, boolean>>({});
  const refreshCasesRequestRef = useRef(0);

  /* filters */
  const [statusFilter, setStatusFilter] = useState("");
  const {
    applied: appliedTextFilters,
    commitDraft: commitTextFilters,
    draft: draftTextFilters,
    isPending: textFiltersPending,
    reset: resetTextFilters,
    setField: setTextFilterField,
  } = useTextSearchFilters();
  const countryFilter = appliedTextFilters.country;
  const brandFilter = appliedTextFilters.brand;
  const modelFilter = appliedTextFilters.model;

  /* country stats */
  const [countryStats, setCountryStats] = useState<{ totalCountries: number; jatoCountries: number } | null>(null);

  /* workbench snapshot for Gantt phase progress */
  const [workbench, setWorkbench] = useState<ReviewWorkbench | null>(null);

  /* detail + decision */
  const [detail, setDetail] = useState<ReviewCaseDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailCollapsed, setDetailCollapsed] = useState(false);
  const [decisionType, setDecisionType] = useState<"approve" | "reject" | "remap">("approve");
  const [decisionNote, setDecisionNote] = useState("");
  const [decisionModel, setDecisionModel] = useState("");
  const [decisionTrim, setDecisionTrim] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [rowActionId, setRowActionId] = useState<string | null>(null);
  const [groupActionState, setGroupActionState] = useState<{
    key: string;
    decision: "approve" | "reject";
  } | null>(null);

  /* ── fetchers ───────────────────────────────────── */
  async function refreshCases(filters = appliedTextFilters) {
    const requestId = ++refreshCasesRequestRef.current;
    setLoading(true);
    setError("");
    try {
      const nextCases: ReviewCase[] = [];
      let offset = 0;
      let total: number | null = null;

      while (total === null || offset < total) {
        const res = await api.listReviewCases({
          review_status: statusFilter || undefined,
          country: filters.country || undefined,
          brand: filters.brand || undefined,
          model: filters.model || undefined,
          limit: reviewCasePageSize,
          offset,
        });
        total = res.total;
        nextCases.push(...res.items);
        if (res.items.length === 0) {
          break;
        }
        offset += res.items.length;
      }

      if (requestId !== refreshCasesRequestRef.current) {
        return;
      }

      setCases(nextCases);
    } catch (err) {
      if (requestId !== refreshCasesRequestRef.current) {
        return;
      }

      setError((err as Error).message);
      setCases([]);
    } finally {
      if (requestId === refreshCasesRequestRef.current) {
        setLoading(false);
      }
    }
  }

  async function refreshStats() {
    try {
      const stats = await api.getReviewCasesStats();
      setCountryStats(stats);
    } catch {
      /* non-critical */
    }
  }

  async function refreshWorkbench() {
    try {
      const { item } = await api.getReviewWorkbench();
      setWorkbench(item);
    } catch {
      /* non-critical — Gantt will fall back gracefully */
    }
  }

  async function openDetail(id: string) {
    setDetailLoading(true);
    setDetailCollapsed(false);
    setError("");
    try {
      const res = await api.getReviewCaseDetail(id);
      setDetail(res.item);
      setExpandedGroups((current) => ({
        ...current,
        [buildReviewCaseGroupKey(res.item)]: true,
      }));
      setDecisionType("approve");
      setDecisionNote("");
      setDecisionModel(res.item.officialModel ?? "");
      setDecisionTrim(res.item.officialTrim ?? "");
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setDetailLoading(false);
    }
  }

  async function submitDecision(e: FormEvent) {
    e.preventDefault();
    if (!detail) return;
    setSubmitting(true);
    setError("");
    try {
      await api.createReviewDecision(detail.id, {
        decision: decisionType,
        decided_official_model: decisionType === "remap" ? decisionModel : undefined,
        decided_official_trim: decisionType === "remap" ? decisionTrim : undefined,
        note: decisionNote || undefined,
        decided_by: getReviewerName(),
      });
      await openDetail(detail.id);
      await refreshCases(appliedTextFilters);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSubmitting(false);
    }
  }

  async function submitInlineDecision(reviewCase: ReviewCase, decision: "approve" | "reject") {
    setRowActionId(reviewCase.id);
    setError("");
    try {
      await api.createReviewDecision(reviewCase.id, {
        decision,
        decided_by: getReviewerName(),
      });
      await refreshCases(appliedTextFilters);
      if (detail?.id === reviewCase.id) {
        await openDetail(reviewCase.id);
      }
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setRowActionId(null);
    }
  }

  async function submitGroupDecision(group: ReviewCaseGroup, decision: "approve" | "reject") {
    const actionableCases = group.items.filter((reviewCase) => isActionableStatus(reviewCase.reviewStatus));
    if (actionableCases.length === 0) {
      return;
    }

    setGroupActionState({ key: group.key, decision });
    setError("");
    try {
      for (const reviewCase of actionableCases) {
        await api.createReviewDecision(reviewCase.id, {
          decision,
          decided_by: getReviewerName(),
        });
      }
      await refreshCases(appliedTextFilters);
      if (detail && actionableCases.some((reviewCase) => reviewCase.id === detail.id)) {
        await openDetail(detail.id);
      }
    } catch (err) {
      setError((err as Error).message);
      await refreshCases(appliedTextFilters);
    } finally {
      setGroupActionState(null);
    }
  }

  useEffect(() => {
    void refreshCases();
  }, [appliedTextFilters, statusFilter]);

  useEffect(() => {
    void refreshStats();
    void refreshWorkbench();
  }, []);

  /* ── derived ────────────────────────────────────── */
  const pendingCount = cases.filter((c) => c.reviewStatus === "open" || c.reviewStatus === "review_required").length;
  const approvedCount = cases.filter((c) => c.reviewStatus === "approved").length;
  const rejectedCount = cases.filter((c) => c.reviewStatus === "rejected").length;
  const caseGroups = groupReviewCases(cases);
  const reviewCountries = Array.from(new Set(cases.map((c) => c.country)))
    .sort((left, right) => left.localeCompare(right));
  const filteredCountries = new Set(cases.map((c) => c.country)).size;
  const showLoadingOverlay = loading && cases.length === 0;
  const detailMatchReason = detail ? getReviewCaseMatchReason(detail) : null;
  const detailConfidenceRule = getConfidenceRuleSummary(detailMatchReason);
  const detailEvidence = getMatchEvidenceEntries(detailMatchReason);
  const detailEvkxContext = getEvkxSourceContext(detail);
  const detailEvkxMarketPrice = asRecord(detailEvkxContext ? detailEvkxContext.selectedMarketPrice : null);
  const detailEvkxSpecHighlights = asRecord(detailEvkxContext ? detailEvkxContext.specHighlights : null);
  const detailEvkxSpecEntries = detailEvkxSpecHighlights
    ? Object.entries(detailEvkxSpecHighlights)
      .filter(([, value]) => value !== null && value !== undefined && value !== "")
    : [];
  const detailCandidateMatches = detail?.candidateMatches ?? [];
  const currentPrice = detail?.currentPrice ?? null;
  const statusFilterLabel = statusFilter
    ? getReviewStatusLabel(statusFilter)
    : "All review statuses";

  function isGroupExpanded(group: ReviewCaseGroup) {
    return expandedGroups[group.key] ?? group.items.length <= 1;
  }

  function toggleGroup(group: ReviewCaseGroup) {
    const expanded = isGroupExpanded(group);
    setExpandedGroups((current) => ({
      ...current,
      [group.key]: !expanded,
    }));
  }

  function setAllGroupsExpanded(nextExpanded: boolean) {
    setExpandedGroups(
      caseGroups.reduce<Record<string, boolean>>((accumulator, group) => {
        accumulator[group.key] = nextExpanded;
        return accumulator;
      }, {}),
    );
  }

  return (
    <section className={`crud-shell review-cases-shell${detail ? " has-detail-dock" : ""}${detail && detailCollapsed ? " is-detail-dock-collapsed" : ""}`}>
      <CollapsibleDeckHero
        collapsed={heroCollapsed}
        onToggle={() => setHeroCollapsed((c) => !c)}
        expandedLabel="展开审核概览"
        collapsedLabel="收起审核概览"
        expandedTitle="Expand review overview"
        collapsedTitle="Collapse review overview"
        className="header-card dashboard-hero crud-hero"
        head={(
          <>
            <div className="dashboard-hero-copy crud-hero-copy">
              <span className="page-kicker">05 / Review</span>
              <h1>Review Cases Deck</h1>
              <p>匹配审核工作台 review open case| approve / reject / remap </p>
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Current filter</span>
                <span className="selection-ribbon-value">
                  {statusFilterLabel} · {countryFilter || "All countries"} · {brandFilter || "All brands"} · {modelFilter || "All models"}
                </span>
              </div>
            </div>
            <div className="dashboard-hero-actions crud-hero-actions">
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Total cases</span>
                <strong className="hero-meta-value">{cases.length}</strong>
                <span className="hero-meta-subvalue">当前筛选全量 backlog</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Pending</span>
                <strong className="hero-meta-value">{pendingCount}</strong>
                <span className="hero-meta-subvalue">Open queue</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Approved</span>
                <strong className="hero-meta-value">{approvedCount}</strong>
                <span className="hero-meta-subvalue">Approved</span>
              </div>
              <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                <span className="hero-meta-label">Rejected</span>
                <strong className="hero-meta-value">{rejectedCount}</strong>
                <span className="hero-meta-subvalue">Rejected</span>
              </div>
            </div>
          </>
        )}
        body={(
          <div className="dashboard-hero-rail">
            <div className="dashboard-hero-chip-row">
              <span className="dashboard-hero-chip">{statusFilterLabel}</span>
              <span className="dashboard-hero-chip">{countryFilter || "All countries"}</span>
              <span className="dashboard-hero-chip">{brandFilter || "All brands"}</span>
              <span className="dashboard-hero-chip">{modelFilter || "All models"}</span>
            </div>
            <div className="dashboard-hero-rail-actions">
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                onClick={() => {
                  resetTextFilters();
                  setStatusFilter("");
                }}
              >
                重置
              </button>
              <button
                type="button"
                className="btn btn-sm btn-secondary"
                onClick={() => {
                  if (textFiltersPending) {
                    commitTextFilters();
                    return;
                  }
                  void refreshCases(appliedTextFilters);
                }}
              >
                刷新
              </button>
            </div>
          </div>
        )}
      />

      {error && <div className="alert alert-error">{error}</div>}

      {/* ── Filters ─────────────────────────────── */}
      <div className="card crud-card">
        <div className="detail-section-head">
          <div><div className="card-title">Filter</div></div>
        </div>
        <div className="crud-toolbar-grid">
          <TextSearchFilters
            value={draftTextFilters}
            onChange={setTextFilterField}
            leading={(
              <div className="filter-group">
                <label>Review Status</label>
                <select
                  value={statusFilter}
                  onChange={(event: ChangeEvent<HTMLSelectElement>) => {
                    if (textFiltersPending) {
                      commitTextFilters();
                    }
                    setStatusFilter(event.target.value);
                  }}
                >
                  <option value="">All</option>
                  {REVIEW_STATUS_FILTER_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </div>
            )}
          />
        </div>
      </div>

      {/* ── Cases table ─────────────────────────── */}
      <div className="card crud-table-card">
        <div className="detail-section-head">
          <div>
            <div className="card-title">Review Cases</div>
            <p className="section-note">按 country / brand / model 收纳。低风险决策可直接在行内 approve / reject，只有 remap 或追溯时再打开详情。{loading && cases.length > 0 ? " 正在后台同步最新筛选…" : ""}</p>
          </div>
          <div className="crud-table-toolbar">
            <div className="btn-group">
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                disabled={caseGroups.length === 0}
                onClick={() => setAllGroupsExpanded(true)}
              >
                全部展开
              </button>
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                disabled={caseGroups.length === 0}
                onClick={() => setAllGroupsExpanded(false)}
              >
                全部收起
              </button>
            </div>
            <div className="review-table-toolbar-status">
              <LoopingCountStrip
                current={filteredCountries}
                total={countryStats?.totalCountries ?? filteredCountries}
                label="Alive / All"
                meta={countryStats ? `JATO ${countryStats.jatoCountries}` : "Syncing"}
                pauseMs={2000}
              />
              <RollingTickerCard
                title="Live Markets"
                items={reviewCountries}
                emptyLabel="No active market"
                pauseMs={1000}
                variant="reel-only"
              />
            </div>
          </div>
        </div>

        {showLoadingOverlay && (
          <LoadingSurface mode="overlay" label="正在加载" detail="同步审核案例" kicker="Review" />
        )}

        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Status</th>
                <th>Country</th>
                <th>Brand</th>
                <th>Source</th>
                <th>JATO Model</th>
                <th>Official Model</th>
                <th>Official Trim</th>
                <th>Scraped MSRP</th>
                <th>Confidence</th>
                <th>Assignee</th>
                <th style={{ width: 240 }}>操作</th>
              </tr>
            </thead>
            <tbody>
              {caseGroups.map((group) => {
                const expanded = isGroupExpanded(group);
                const actionableCases = group.items.filter((reviewCase) => isActionableStatus(reviewCase.reviewStatus));
                const pendingInGroup = group.items.filter((reviewCase) => isActionableStatus(reviewCase.reviewStatus)).length;
                const approvedInGroup = group.items.filter((reviewCase) => reviewCase.reviewStatus === "approved").length;
                const rejectedInGroup = group.items.filter((reviewCase) => reviewCase.reviewStatus === "rejected").length;
                const containsSelection = group.items.some((reviewCase) => detail?.id === reviewCase.id);
                const groupSubmitting = groupActionState?.key === group.key;
                const approvingGroup = groupSubmitting && groupActionState?.decision === "approve";
                const rejectingGroup = groupSubmitting && groupActionState?.decision === "reject";
                const groupReasonBrief = getGroupReasonBrief(group.items);

                return (
                  <Fragment key={group.key}>
                    <tr className={`data-table-group-row${expanded ? " is-expanded" : ""}${containsSelection ? " contains-selection" : ""}`}>
                      <td colSpan={11}>
                        <div className="data-table-group-cell">
                          <button
                            type="button"
                            className="data-table-group-toggle"
                            aria-expanded={expanded}
                            onClick={() => toggleGroup(group)}
                          >
                            <span className="data-table-group-toggle-mark">{expanded ? "-" : "+"}</span>
                            <span className="data-table-group-copy">
                              <span className="data-table-group-title">{group.country} / {group.brand} / {group.model}</span>
                              <span className="data-table-group-subtitle">
                                {group.items.length} trims · {summarizeReviewCaseTrims(group.items)}{groupReasonBrief ? ` · ${groupReasonBrief}` : ""}
                              </span>
                            </span>
                          </button>
                          <div className="data-table-group-meta">
                            {actionableCases.length > 0 && (
                              <div className="data-table-group-actions">
                                <button
                                  type="button"
                                  className="btn btn-sm btn-primary"
                                  disabled={groupSubmitting}
                                  onClick={() => void submitGroupDecision(group, "approve")}
                                >
                                  {approvingGroup ? "处理中…" : "一键 Approve"}
                                </button>
                                <button
                                  type="button"
                                  className="btn btn-sm btn-danger"
                                  disabled={groupSubmitting}
                                  onClick={() => void submitGroupDecision(group, "reject")}
                                >
                                  {rejectingGroup ? "处理中…" : "一键 Reject"}
                                </button>
                              </div>
                            )}
                            <span className="data-table-group-pill">{pendingInGroup} pending</span>
                            {approvedInGroup > 0 && <span className="data-table-group-pill">{approvedInGroup} approved</span>}
                            {rejectedInGroup > 0 && <span className="data-table-group-pill">{rejectedInGroup} rejected</span>}
                            <span className="data-table-group-pill">Confidence {formatConfidenceRange(group.items)}</span>
                          </div>
                        </div>
                      </td>
                    </tr>
                    {expanded && group.items.map((c) => {
                      const actionable = isActionableStatus(c.reviewStatus);
                      const rowSubmitting = rowActionId === c.id;
                      const rowReasonBrief = getReviewCaseReasonBrief(c);

                      return (
                        <tr key={c.id} className={detail?.id === c.id ? "is-selected" : ""}>
                          <td>
                            <span className={`badge ${getReviewStatusBadgeClass(c.reviewStatus)}`}>
                              {getReviewStatusLabel(c.reviewStatus)}
                            </span>
                          </td>
                          <td>{c.country}</td>
                          <td>{c.brand}</td>
                          <td className="review-table-meta-cell">
                            <div className="review-table-meta">
                              <strong>{c.sourceCode || c.extractorName || "-"}</strong>
                              {c.sourceUrl ? (
                                <a href={c.sourceUrl} target="_blank" rel="noreferrer" className="review-table-link">
                                  {formatSourceLink(c.sourceUrl)}
                                </a>
                              ) : (
                                <span className="review-table-muted">No official URL</span>
                              )}
                            </div>
                          </td>
                          <td><strong>{c.jatoModel || "待映射"}</strong></td>
                          <td>{c.officialModel}</td>
                          <td>
                            <div className="review-table-meta">
                              <strong>{formatReviewTrimSummary(c)}</strong>
                              {rowReasonBrief && <span className="review-table-match-brief">{rowReasonBrief}</span>}
                            </div>
                          </td>
                          <td><strong>{formatMsrp(c.msrpValue, c.currency || "EUR")}</strong></td>
                          <td>{(c.matchConfidence * 100).toFixed(0)}%</td>
                          <td>{c.currentAssignee || "未分配"}</td>
                          <td>
                            <div className="crud-row-actions review-row-actions">
                              {actionable && (
                                <>
                                  <button
                                    type="button"
                                    className="btn btn-sm btn-primary"
                                    disabled={rowSubmitting || groupActionState !== null}
                                    onClick={() => void submitInlineDecision(c, "approve")}
                                  >
                                    {rowSubmitting ? "处理中…" : "Approve"}
                                  </button>
                                  <button
                                    type="button"
                                    className="btn btn-sm btn-danger"
                                    disabled={rowSubmitting || groupActionState !== null}
                                    onClick={() => void submitInlineDecision(c, "reject")}
                                  >
                                    Reject
                                  </button>
                                </>
                              )}
                              <button type="button" className="btn btn-sm btn-secondary" onClick={() => void openDetail(c.id)}>
                                Detail / Remap
                              </button>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </Fragment>
                );
              })}
              {cases.length === 0 && !loading && (
                <tr><td colSpan={11}><div className="crud-empty-state">暂无审核案例</div></td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <ReviewDeliveryPanel
        totalCases={cases.length}
        pendingCount={pendingCount}
        approvedCount={approvedCount}
        rejectedCount={rejectedCount}
        groupCount={caseGroups.length}
        countryStats={countryStats}
        workbenchCoverage={workbench?.coverageSummary ?? null}
      />

      {/* ── Detail + Decision Panel ─────────────── */}
      {detail && (
        <div className={`card crud-card admin-detail-drawer review-detail-dock${detailCollapsed ? " is-collapsed" : ""}`}>
          <div className="detail-section-head review-detail-dock-head">
            <div>
              <div className="card-title">Case Detail</div>
              <p className="section-note">{detail.brand} {detail.jatoModel} — {detail.country}</p>
            </div>
            <div className="review-detail-dock-actions">
              {detail.sourceUrl && (
                <a href={detail.sourceUrl} target="_blank" rel="noreferrer" className="btn btn-sm btn-ghost">
                  Case URL
                </a>
              )}
              {currentPrice?.sourceUrl && (
                <a href={currentPrice.sourceUrl} target="_blank" rel="noreferrer" className="btn btn-sm btn-ghost">
                  Current Price URL
                </a>
              )}
              <button
                type="button"
                className="btn btn-sm btn-secondary"
                onClick={() => setDetailCollapsed((current) => !current)}
              >
                {detailCollapsed ? "展开" : "收起"}
              </button>
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                onClick={() => {
                  setDetail(null);
                  setDetailCollapsed(false);
                }}
              >
                关闭
              </button>
            </div>
          </div>
          {detailLoading && (
            <LoadingSurface mode="inline" label="正在加载详情" detail="" kicker="Detail" />
          )}
          {!detailCollapsed && (
            <div className="review-detail-dock-body">
              <div className="admin-detail-grid">
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Source Code</span>
                  <span className="admin-detail-value">{detail.sourceCode || "-"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Extractor</span>
                  <span className="admin-detail-value">{detail.extractorName || "-"} {detail.extractorVersion || ""}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">JATO Model</span>
                  <span className="admin-detail-value">{detail.jatoModel || "待映射"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Official Model / Trim</span>
                  <span className="admin-detail-value">{detail.officialModel} / {formatReviewTrimSummary(detail, Number.POSITIVE_INFINITY)}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Official Edition</span>
                  <span className="admin-detail-value">{detail.officialEdition || "-"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Official Powertrain</span>
                  <span className="admin-detail-value">{detail.officialPowertrain || "-"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">JATO Powertrain</span>
                  <span className="admin-detail-value">{detail.jatoPowertrain || "-"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Scraped MSRP (EUR)</span>
                  <span className="admin-detail-value">{formatMsrp(detail.msrpValue, detail.currency || "EUR")}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Source MSRP</span>
                  <span className="admin-detail-value">{formatMsrp(detail.sourceMsrpValue, detail.sourceCurrency || "")}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Match Confidence</span>
                  <span className="admin-detail-value">{(detail.matchConfidence * 100).toFixed(1)}%</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Status</span>
                  <span className={`badge ${getReviewStatusBadgeClass(detail.reviewStatus)}`}>
                    {getReviewStatusLabel(detail.reviewStatus)}
                  </span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">FX Rate</span>
                  <span className="admin-detail-value">{detail.fxRateToEur ? `${detail.fxRateToEur.toFixed(6)} (${detail.fxSource || "-"})` : "-"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">FX Date</span>
                  <span className="admin-detail-value">{detail.fxRateAsOfDate || "-"}</span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Source URL</span>
                  <span className="admin-detail-value">
                    {detail.sourceUrl ? <a href={detail.sourceUrl} target="_blank" rel="noreferrer">打开来源</a> : "-"}
                  </span>
                </div>
                <div className="admin-detail-item">
                  <span className="admin-detail-label">Registry URL</span>
                  <span className="admin-detail-value">
                    {detail.sourceRegistryUrl ? <a href={detail.sourceRegistryUrl} target="_blank" rel="noreferrer">打开 source registry</a> : "-"}
                  </span>
                </div>
              </div>

              <div className="review-current-price-panel">
                <div className="detail-section-head review-inline-section-head">
                  <div>
                    <div className="card-title">Current Price Snapshot</div>
                    <p className="section-note">如果这条 case 已经物化为 current price，这里直接给出最终价格和来源链接。</p>
                  </div>
                  {currentPrice?.sourceUrl && (
                    <a href={currentPrice.sourceUrl} target="_blank" rel="noreferrer" className="review-table-link">
                      {formatSourceLink(currentPrice.sourceUrl)}
                    </a>
                  )}
                </div>
                {currentPrice ? (
                  <div className="admin-detail-grid">
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Current MSRP</span>
                      <span className="admin-detail-value">{formatMsrp(currentPrice.currentMsrpValue, currentPrice.currency)}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Source MSRP</span>
                      <span className="admin-detail-value">{formatMsrp(currentPrice.sourceMsrpValue, currentPrice.sourceCurrency || "")}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Match Status</span>
                      <span className={`badge ${getCurrentPriceMatchStatusBadgeClass(currentPrice.matchStatus)}`}>
                        {getCurrentPriceMatchStatusLabel(currentPrice.matchStatus)}
                      </span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Match Confidence</span>
                      <span className="admin-detail-value">{(currentPrice.matchConfidence * 100).toFixed(1)}%</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Last Price Change</span>
                      <span className="admin-detail-value">{formatDateTime(currentPrice.lastPriceChangeAtUtc)}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Updated</span>
                      <span className="admin-detail-value">{formatDateTime(currentPrice.updatedAtUtc)}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Source URL</span>
                      <span className="admin-detail-value">
                        {currentPrice.sourceUrl ? <a href={currentPrice.sourceUrl} target="_blank" rel="noreferrer">打开 current price 来源</a> : "-"}
                      </span>
                    </div>
                  </div>
                ) : (
                  <p className="section-note">当前 case 还没有对应的 current price 物化记录。</p>
                )}
              </div>

              <div className="review-current-price-panel">
                <div className="detail-section-head review-inline-section-head">
                  <div>
                    <div className="card-title">Candidate Matches</div>
                    <p className="section-note">导入侧给出的候选映射会在这里展开，方便人工快速判断是否要 remap。</p>
                  </div>
                </div>
                {detailCandidateMatches.length > 0 ? (
                  <div className="admin-match-component-list">
                    {detailCandidateMatches.map((candidate, index) => (
                      <div key={`${candidate.currentPriceId || candidate.jatoModel}-${candidate.jatoTrim}-${index}`} className="admin-match-component is-applied">
                        <div className="admin-match-component-head">
                          <span className="admin-match-component-label">
                            {candidate.officialModel || candidate.jatoModel} / {candidate.officialTrim || candidate.jatoTrim}
                          </span>
                          <span className="admin-match-component-delta">{formatCandidateScore(candidate)}</span>
                        </div>
                        <div className="admin-match-component-evidence">
                          {[
                            candidate.jatoTrim || null,
                            candidate.jatoPowertrain || candidate.officialPowertrain || null,
                            candidate.currentMsrpValue !== undefined
                              ? formatMsrp(candidate.currentMsrpValue, candidate.currency || detail.currency || "EUR")
                              : null,
                          ].filter(Boolean).join(" / ")}
                        </div>
                        {candidate.reason && (
                          <div className="admin-match-component-evidence">
                            {Object.entries(candidate.reason)
                              .map(([key, value]) => `${formatEvkxFieldLabel(key)}: ${formatEvidenceValue(value)}`)
                              .join(" · ")}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="section-note">当前没有可展示的候选映射；这通常说明该市场还没有同国别 current price 基线。</p>
                )}
              </div>

              {detailEvkxContext && (
                <div className="review-current-price-panel">
                  <div className="detail-section-head review-inline-section-head">
                    <div>
                      <div className="card-title">EVKX Variant / Spec Diff</div>
                      <p className="section-note">这里集中展示 EVKX 变体身份、目标市场价格和关键规格，辅助 reviewer 判断这个 variant 应该映射到哪条 JATO 车型。</p>
                    </div>
                    {Boolean(detailEvkxContext.infoUrl) && (
                      <a href={String(detailEvkxContext.infoUrl)} target="_blank" rel="noreferrer" className="review-table-link">
                        {formatSourceLink(String(detailEvkxContext.infoUrl))}
                      </a>
                    )}
                  </div>
                  <div className="admin-detail-grid">
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">EVKX Variant</span>
                      <span className="admin-detail-value">{String(detailEvkxContext.vehicleName ?? "-")}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">EV ID</span>
                      <span className="admin-detail-value">{String(detailEvkxContext.evId ?? "-")}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Target Market</span>
                      <span className="admin-detail-value">{String(detailEvkxContext.targetCountry ?? detail.country)}</span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Selected Market Price</span>
                      <span className="admin-detail-value">
                        {detailEvkxMarketPrice
                          ? formatMsrp(
                            asNumber(detailEvkxMarketPrice.amount) ?? undefined,
                            String(detailEvkxMarketPrice.currency ?? detail.sourceCurrency ?? ""),
                          )
                          : "-"}
                      </span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Mapped Target</span>
                      <span className="admin-detail-value">
                        {detail.officialModel} / {formatReviewTrimSummary(detail, Number.POSITIVE_INFINITY)}
                      </span>
                    </div>
                    <div className="admin-detail-item">
                      <span className="admin-detail-label">Mapped Powertrain</span>
                      <span className="admin-detail-value">{detail.officialPowertrain || detail.jatoPowertrain || "-"}</span>
                    </div>
                  </div>
                  {detailEvkxSpecEntries.length > 0 ? (
                    <div className="admin-match-evidence-list" style={{ marginTop: 12 }}>
                      {detailEvkxSpecEntries.map(([key, value]) => (
                        <div key={key} className="admin-match-evidence-pill">
                          <span className="admin-match-evidence-key">{formatEvkxFieldLabel(key)}</span>
                          <span className="admin-match-evidence-value">{formatEvidenceValue(value)}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="section-note">当前 EVKX 记录没有提取到可对比的关键规格。</p>
                  )}
                </div>
              )}

              <div className="admin-match-reason">
                <h3 className="admin-subsection-title">Match Reason</h3>
                {!detailMatchReason ? (
                  <p className="section-note">当前案例没有结构化 match reason，通常说明它是在旧版 confidence 逻辑下生成的。</p>
                ) : (
                  <>
                    {detailConfidenceRule && (
                      <>
                        <div className="admin-match-rule-grid">
                          <div className="admin-match-rule-card">
                            <span className="admin-detail-label">Rule Mode</span>
                            <span className="admin-detail-value">{detailConfidenceRule.mode}</span>
                          </div>
                          <div className="admin-match-rule-card">
                            <span className="admin-detail-label">Base Score</span>
                            <span className="admin-detail-value">{formatConfidenceValue(detailConfidenceRule.base, 0)}</span>
                          </div>
                          <div className="admin-match-rule-card">
                            <span className="admin-detail-label">Displayed Confidence</span>
                            <span className="admin-detail-value">{formatConfidenceValue(detailConfidenceRule.total ?? detail.matchConfidence, 1)}</span>
                          </div>
                        </div>

                        {detailConfidenceRule.components.length > 0 && (
                          <div className="admin-match-component-list">
                            {detailConfidenceRule.components.map((component) => (
                              <div
                                key={component.key}
                                className={`admin-match-component ${component.applied ? "is-applied" : "is-skipped"}`}
                              >
                                <div className="admin-match-component-head">
                                  <span className="admin-match-component-label">{component.label}</span>
                                  <span className="admin-match-component-delta">
                                    {component.delta > 0 ? "+" : ""}
                                    {formatConfidenceValue(component.delta, 0)}
                                  </span>
                                </div>
                                {component.evidence !== undefined && component.evidence !== null && component.evidence !== "" && (
                                  <div className="admin-match-component-evidence">{formatEvidenceValue(component.evidence)}</div>
                                )}
                              </div>
                            ))}
                          </div>
                        )}
                      </>
                    )}

                    {detailEvidence.length > 0 && (
                      <div className="admin-match-evidence-list">
                        {detailEvidence.map((entry) => (
                          <div key={entry.key} className="admin-match-evidence-pill">
                            <span className="admin-match-evidence-key">{entry.key}</span>
                            <span className="admin-match-evidence-value">{entry.value}</span>
                          </div>
                        ))}
                      </div>
                    )}

                    <div className="admin-json-preview">
                      <pre>{JSON.stringify(detailMatchReason, null, 2)}</pre>
                    </div>
                  </>
                )}
              </div>

              {/* Decision history */}
              {detail.decisions && detail.decisions.length > 0 && (
                <div className="admin-decision-history">
                  <h3 className="admin-subsection-title">Decision History</h3>
                  {detail.decisions.map((d) => (
                    <div key={d.id} className="admin-decision-row">
                      <span className={`badge ${d.decision === "approve" ? "badge-active" : d.decision === "reject" ? "badge-danger" : "badge-inactive"}`}>
                        {d.decision}
                      </span>
                      <span>{d.decidedBy}</span>
                      <span>{new Date(d.decidedAt).toLocaleString()}</span>
                      {d.note && <span className="admin-decision-note">{d.note}</span>}
                    </div>
                  ))}
                </div>
              )}

              {/* Decision form */}
              <form onSubmit={submitDecision} className="admin-decision-form">
                <h3 className="admin-subsection-title">New Decision</h3>
                <div className="crud-toolbar-grid">
                  <div className="filter-group">
                    <label>Decision</label>
                    <select value={decisionType} onChange={(e: ChangeEvent<HTMLSelectElement>) => setDecisionType(e.target.value as typeof decisionType)}>
                      <option value="approve">Approve</option>
                      <option value="reject">Reject</option>
                      <option value="remap">Remap</option>
                    </select>
                  </div>
                  {decisionType === "remap" && (
                    <>
                      <div className="filter-group">
                        <label>Official Model</label>
                        <input type="text" value={decisionModel} onChange={(e: ChangeEvent<HTMLInputElement>) => setDecisionModel(e.target.value)} required />
                      </div>
                      <div className="filter-group">
                        <label>Official Trim</label>
                        <input type="text" value={decisionTrim} onChange={(e: ChangeEvent<HTMLInputElement>) => setDecisionTrim(e.target.value)} required />
                      </div>
                    </>
                  )}
                  <div className="filter-group">
                    <label>Note</label>
                    <input type="text" value={decisionNote} onChange={(e: ChangeEvent<HTMLInputElement>) => setDecisionNote(e.target.value)} placeholder="可选备注" />
                  </div>
                </div>
                <div className="crud-form-actions" style={{ marginTop: 12 }}>
                  <button type="submit" className="btn btn-primary" disabled={submitting}>
                    {submitting ? "提交中…" : "提交决策"}
                  </button>
                </div>
              </form>
            </div>
          )}
        </div>
      )}
      <AdminToolsNav />
    </section>
  );
}
