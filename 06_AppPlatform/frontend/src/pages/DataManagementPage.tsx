import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link, useLocation } from "react-router-dom";

import { api } from "../api/client";
import { HermesAskResponseCard } from "../components/HermesAskResponseCard";
import { HermesHistoryMap } from "../components/HermesHistoryMap";
import { HermesMermaidBlock } from "../components/HermesMermaidBlock";
import { HermesProgressSwimlane } from "../components/HermesProgressSwimlane";
import { LoadingSurface } from "../components/LoadingSurface";
import { MsrpDryrunDashboard } from "../components/MsrpDryrunDashboard";
import { MsrpFinanceObservationsPanel } from "../components/MsrpFinanceObservationsPanel";
import { MsrpReconciliationPanel } from "../components/MsrpReconciliationPanel";
import type {
  DataManagementAirflowStatus,
  DataManagementDomain,
  DataManagementOverviewResponse,
  DataManagementVocOverviewResponse,
  ConfigProject,
  MatchOverride,
  MsrpSource,
} from "../types/dataManagement";
import type {
  HermesActivityResponse,
  HermesArchResponse,
  HermesChatResponse,
  HermesChatSuggestedAction,
  HermesCostResponse,
  HermesDailySummaryResponse,
  HermesEvidenceLedgerResponse,
  HermesFeatureKanbanResponse,
  HermesFullDesignDocumentResponse,
  HermesGap,
  HermesMermaidBlock as HermesMermaidBlockType,
  HermesOverviewResponse,
  HermesPipelineHealthResponse,
  HermesPipelineStatusRecord,
  HermesReplyType,
  HermesSentinelMailboxStatus,
  HermesSentinelNotification,
  HermesSentinelStatusResponse,
  HermesMsrpCountryProgressCountry,
  HermesMsrpCountryProgressResponse,
  HermesMsrpDryrunHistoryResponse,
  HermesMsrpDryrunHistoryRun,
  HermesMsrpSourceRepairBacklogGroup,
  HermesSourceQualityResponse,
  HermesToolchainResponse,
} from "../types/hermes";
import {
  buildActivityHeatmapColumns,
  formatDataManagementBytes,
  formatDataManagementNumber,
  formatDataManagementTimestamp,
  getDataManagementStatusBadgeClass,
} from "../utils/dataManagement";

type CrudEntityTab = "msrp-sources" | "engineering-projects" | "review-overrides";
type DataSubpage = "overview" | "hermes" | "features" | "voc" | "admin" | "dryrun" | "order-genius" | "material-master";
type HermesSubtab = "capabilities" | "progress" | "history" | "activity" | "cost" | "roadmap" | "diagrams";
type SentinelInboxFilter = "new" | "read" | "archived" | "all";
const DEFAULT_RECENT_ITEMS_VISIBLE = 6;

const DATA_SUBPAGES: DataSubpage[] = ["overview", "hermes", "features", "voc", "admin", "dryrun", "order-genius", "material-master"];
const HERMES_SUBTABS: HermesSubtab[] = ["capabilities", "activity", "cost", "roadmap", "diagrams"];

function resolveDataSubpageFromLocation(search: string, hash: string, pathname = ""): DataSubpage {
  const params = new URLSearchParams(search);
  const candidate = (params.get("view") || params.get("tab") || hash.replace(/^#/, "")).toLowerCase();
  if (DATA_SUBPAGES.includes(candidate as DataSubpage)) return candidate as DataSubpage;
  if (pathname === "/data-management") return "admin";
  return "overview";
}

function resolveHermesSubtabFromLocation(search: string): HermesSubtab {
  const params = new URLSearchParams(search);
  const candidate = (params.get("hermesTab") || params.get("hermesSubtab") || "").toLowerCase();
  if (HERMES_SUBTABS.includes(candidate as HermesSubtab)) return candidate as HermesSubtab;
  return params.get("view")?.toLowerCase() === "hermes" ? "activity" : "capabilities";
}

const HERMES_SCRIPTS_MAP: Record<string, string> = {
  "pipeline-audit": "pipeline audit",
  "source-quality": "source quality",
  "cost-report": "cost report",
  "code-audit": "code audit",
  "evidence": "evidence writer",
  "answer-audit": "answer audit",
};

type ReusableBuildingBlockLayer = "Frontend" | "Backend" | "Utility";

interface ReusableBuildingBlock {
  name: string;
  layer: ReusableBuildingBlockLayer;
  path: string;
  usage: string;
  owner: string;
}

const REUSABLE_BUILDING_BLOCKS: ReusableBuildingBlock[] = [
  {
    name: "FileDropzone",
    layer: "Frontend",
    path: "src/components/upload/FileDropzone.tsx",
    usage: "Excel、PDF、ZIP/RAR 上传入口，支持拖拽、点击选择和清除文件。",
    owner: "COC 工作台 / 月更上传 / 后续导入页",
  },
  {
    name: "StatusMetricCard",
    layer: "Frontend",
    path: "src/components/workbench/StatusMetricCard.tsx",
    usage: "状态数字卡片，支持 tone、active 和点击筛选。",
    owner: "COC 状态 / 导入预览 / 工作台摘要",
  },
  {
    name: "SheetGroupedPreview",
    layer: "Frontend",
    path: "src/components/workbench/SheetGroupedPreview.tsx",
    usage: "按 Sheet 分组展开的表格预览，业务行操作通过 renderRow 插槽注入。",
    owner: "COC 填充 / Engineering Config / Excel digest",
  },
  {
    name: "UploadDigestPanel",
    layer: "Frontend",
    path: "src/components/UploadDigestPanel.tsx",
    usage: "上传解析后的指标、错误、预览和 apply/cancel 操作面板。",
    owner: "Material Master / CBU / 后续上传 digest",
  },
  {
    name: "workbook_table_scanner",
    layer: "Backend",
    path: "app/services/workbook_table_scanner.py",
    usage: "多 sheet 表头扫描、物料号组推断、目标列创建和 source row/cell 抽取。",
    owner: "COC 填充 / Excel 回写型任务",
  },
  {
    name: "downloadBlob + formatDateTime",
    layer: "Utility",
    path: "src/utils/download.ts, src/utils/timeFormatting.ts",
    usage: "浏览器下载 blob 与统一时间显示，避免页面内重复小工具函数。",
    owner: "所有下载和历史记录页面",
  },
];

interface HermesPipelineDisplayRow {
  key: string;
  label: string;
  status: string;
  statusColor: string;
  meta: string;
  lastRunAt: string;
}

function getPipelineStatusColor(status: string, fallbackRisk = ""): string {
  const normalized = status.toLowerCase();
  const risk = fallbackRisk.toLowerCase();
  if (normalized === "failed" || normalized === "missing" || risk === "high") return "#ef4444";
  if (normalized === "degraded" || normalized === "unknown" || risk === "medium") return "#f59e0b";
  if (normalized === "success" || normalized === "ok") return "#22c55e";
  return "#64748b";
}

function formatPipelineLastRun(value?: string | null): string {
  if (!value) return "never";
  return formatDataManagementTimestamp(value);
}

function buildPipelineDisplayRows(
  statuses: HermesPipelineStatusRecord[],
  health: HermesPipelineHealthResponse | null,
): HermesPipelineDisplayRow[] {
  if (statuses.length > 0) {
    return statuses.map((item) => {
      const failed = item.failedCount ?? 0;
      const warnings = item.warningCount ?? 0;
      const metaParts = [
        `${item.recordsProcessed ?? 0} records`,
        failed > 0 ? `${failed} failed` : "",
        warnings > 0 ? `${warnings} warnings` : "",
      ].filter(Boolean);
      return {
        key: item.pipelineId,
        label: item.pipelineId,
        status: item.status,
        statusColor: getPipelineStatusColor(item.status),
        meta: metaParts.join(" · "),
        lastRunAt: formatPipelineLastRun(item.lastRunAt),
      };
    });
  }
  return (health?.allPipelines ?? []).map((item) => {
    const key = String(item.pipelineId || item.name || "pipeline");
    const status = String(item.status || item.risk || item.riskLevel || "unknown");
    return {
      key,
      label: String(item.name || item.pipelineId || key),
      status,
      statusColor: getPipelineStatusColor(status, String(item.risk || item.riskLevel || "")),
      meta: String(item.type || item.role || ""),
      lastRunAt: "",
    };
  });
}

const SENTINEL_FILTERS: Array<{ key: SentinelInboxFilter; label: string }> = [
  { key: "new", label: "Unread" },
  { key: "read", label: "Read" },
  { key: "archived", label: "Archived" },
  { key: "all", label: "All" },
];

function getSentinelSeverityColor(severity: string): string {
  if (severity === "critical" || severity === "high") return "#dc2626";
  if (severity === "medium") return "#d97706";
  return "#2563eb";
}

function getMsrpProgressColor(passPct: number): string {
  if (passPct >= 90) return "#16a34a";
  if (passPct >= 70) return "#ca8a04";
  if (passPct >= 50) return "#ea580c";
  return "#dc2626";
}

function formatHermesRunLabel(run: HermesMsrpDryrunHistoryRun): string {
  const finished = run.finishedAt ? new Date(run.finishedAt).toLocaleString() : run.runId;
  return `${finished} · ${run.passPct}% · ${run.gateStatus}`;
}

function normalizeSentinelMailboxStatus(status: string): SentinelInboxFilter {
  if (status === "archived" || status === "resolved") return "archived";
  if (status === "read" || status === "acked") return "read";
  return "new";
}

function matchesSentinelFilter(notification: HermesSentinelNotification, filter: SentinelInboxFilter, search: string): boolean {
  const normalizedStatus = normalizeSentinelMailboxStatus(String(notification.status || "new"));
  if (filter !== "all" && normalizedStatus !== filter) return false;
  const query = search.trim().toLowerCase();
  if (!query) return true;
  return [
    notification.title,
    notification.body,
    notification.source,
    notification.recommendedAction ?? "",
    String(notification.severity || ""),
  ].some((value) => value.toLowerCase().includes(query));
}

function readSentinelContextString(context: Record<string, unknown> | undefined, key: string): string {
  const value = context?.[key];
  return typeof value === "string" ? value : "";
}

function readSentinelContextCount(context: Record<string, unknown> | undefined, key: string): string {
  const value = context?.[key];
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "string" && value.trim()) return value.trim();
  return "";
}

function getSentinelPipelineDetails(notification: HermesSentinelNotification): Array<{ label: string; value: string }> {
  const context = notification.context;
  const details = [
    { label: "Pipeline", value: readSentinelContextString(context, "pipeline") },
    { label: "Status", value: readSentinelContextString(context, "pipelineStatus") },
    { label: "Last run", value: readSentinelContextString(context, "lastRunAt") },
    { label: "Failed", value: readSentinelContextCount(context, "failedCount") },
    { label: "Warnings", value: readSentinelContextCount(context, "warningCount") },
  ];
  return details.filter((item) => item.value);
}

function getSentinelArtifactRefs(notification: HermesSentinelNotification): string[] {
  const refs = notification.context?.artifactRefs;
  if (!Array.isArray(refs)) return [];
  return refs.filter((item): item is string => typeof item === "string" && item.trim().length > 0).slice(0, 3);
}

interface SourceFilters {
  country: string;
  brand: string;
  enabled: "all" | "true" | "false";
}

interface ProjectFilters {
  status: string;
  brand: string;
  marketCountry: string;
}

interface OverrideFilters {
  country: string;
  brand: string;
  jatoModel: string;
}

interface SourceFormState {
  sourceCode: string;
  country: string;
  brand: string;
  sourceUrl: string;
  sourceType: string;
  tier: number;
  extractorName: string;
  extractorVersion: string;
  priceSemantics: string;
  requiresLocation: boolean;
  enabled: boolean;
  notes: string;
}

interface ProjectFormState {
  projectCode: string;
  brand: string;
  model: string;
  marketCountry: string;
  displayName: string;
  status: string;
}

interface OverrideFormState {
  country: string;
  brand: string;
  jatoModel: string;
  jatoTrim: string;
  officialModel: string;
  officialTrim: string;
  validFromDate: string;
  validToDate: string;
  overrideReason: string;
  createdBy: string;
}

function getAdminUserName() {
  return (localStorage.getItem("jato_user_name") || "anonymous").trim() || "anonymous";
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function defaultSourceFilters(): SourceFilters {
  return {
    country: "",
    brand: "",
    enabled: "all",
  };
}

function defaultProjectFilters(): ProjectFilters {
  return {
    status: "",
    brand: "",
    marketCountry: "",
  };
}

function defaultOverrideFilters(): OverrideFilters {
  return {
    country: "",
    brand: "",
    jatoModel: "",
  };
}

function defaultSourceForm(): SourceFormState {
  return {
    sourceCode: "",
    country: "",
    brand: "",
    sourceUrl: "",
    sourceType: "official_site",
    tier: 1,
    extractorName: "manual",
    extractorVersion: "v1",
    priceSemantics: "msrp",
    requiresLocation: false,
    enabled: true,
    notes: "",
  };
}

function defaultProjectForm(): ProjectFormState {
  return {
    projectCode: "",
    brand: "",
    model: "",
    marketCountry: "",
    displayName: "",
    status: "active",
  };
}

function defaultOverrideForm(): OverrideFormState {
  return {
    country: "",
    brand: "",
    jatoModel: "",
    jatoTrim: "",
    officialModel: "",
    officialTrim: "",
    validFromDate: todayIsoDate(),
    validToDate: "",
    overrideReason: "",
    createdBy: getAdminUserName(),
  };
}

function formatMetricValue(value: string | number): string {
  return typeof value === "number" ? value.toLocaleString() : value;
}

function formatSharePct(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

function renderDomainRecentItems(
  domain: DataManagementDomain,
  expanded: boolean,
  onToggle: (domainKey: string) => void,
) {
  if (domain.recentItems.length === 0) {
    return <div className="crud-empty-state">暂无近期记录</div>;
  }
  const visibleItems = expanded
    ? domain.recentItems
    : domain.recentItems.slice(0, DEFAULT_RECENT_ITEMS_VISIBLE);
  const hiddenCount = Math.max(0, domain.recentItems.length - visibleItems.length);
  return (
    <div className="data-management-recent-list">
      {visibleItems.map((item, index) => (
        <article key={`${domain.key}-${index}`} className="data-management-recent-item">
          <div>
            <strong>{item.label}</strong>
            <span>{formatMetricValue(item.value)}</span>
          </div>
          <time>{formatDataManagementTimestamp(item.updatedAt)}</time>
        </article>
      ))}
      {domain.recentItems.length > DEFAULT_RECENT_ITEMS_VISIBLE ? (
        <button
          type="button"
          className="btn btn-sm btn-secondary"
          onClick={() => onToggle(domain.key)}
        >
          {expanded ? "收起明细" : `查看全部 ${domain.recentItems.length} 项${hiddenCount > 0 ? `（还有 ${hiddenCount} 项）` : ""}`}
        </button>
      ) : null}
    </div>
  );
}

export function DataManagementPage() {
  const location = useLocation();
  const [overview, setOverview] = useState<DataManagementOverviewResponse | null>(null);
  const [vocOverview, setVocOverview] = useState<DataManagementVocOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [vocOverviewLoading, setVocOverviewLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [vocOverviewError, setVocOverviewError] = useState("");
  const [expandedDomains, setExpandedDomains] = useState<Record<string, boolean>>({});
  const [vocCountry, setVocCountry] = useState("");

  const [crudTab, setCrudTab] = useState<CrudEntityTab>("msrp-sources");
  const [subpage, setSubpage] = useState<DataSubpage>(() => resolveDataSubpageFromLocation(location.search, location.hash, location.pathname));
  const [hermesSubtab, setHermesSubtab] = useState<HermesSubtab>(() => resolveHermesSubtabFromLocation(location.search));
  const [hermesOverview, setHermesOverview] = useState<HermesOverviewResponse | null>(null);
  const [hermesPipelines, setHermesPipelines] = useState<HermesPipelineHealthResponse | null>(null);
  const [hermesPipelineStatuses, setHermesPipelineStatuses] = useState<HermesPipelineStatusRecord[]>([]);
  const [hermesSources, setHermesSources] = useState<HermesSourceQualityResponse | null>(null);
  const [hermesMsrpProgress, setHermesMsrpProgress] = useState<HermesMsrpCountryProgressResponse | null>(null);
  const [hermesMsrpHistory, setHermesMsrpHistory] = useState<HermesMsrpDryrunHistoryResponse | null>(null);
  const [hermesMsrpRunId, setHermesMsrpRunId] = useState("");
  const [hermesCost, setHermesCost] = useState<HermesCostResponse | null>(null);
  const [hermesProposals, setHermesProposals] = useState<Record<string, unknown>[]>([]);
  const [hermesFeatures, setHermesFeatures] = useState<Record<string, unknown>[]>([]);
  const [hermesToolchain, setHermesToolchain] = useState<HermesToolchainResponse | null>(null);
  const [hermesArch, setHermesArch] = useState<HermesArchResponse | null>(null);
  const [hermesActivity, setHermesActivity] = useState<HermesActivityResponse | null>(null);
  const [hermesCostHeatmap, setHermesCostHeatmap] = useState<HermesCostResponse | null>(null);
  const [hermesDaily, setHermesDaily] = useState<HermesDailySummaryResponse | null>(null);
  const [featureKanban, setFeatureKanban] = useState<HermesFeatureKanbanResponse | null>(null);
  const [hermesEvidence, setHermesEvidence] = useState<HermesEvidenceLedgerResponse | null>(null);
  const [hermesGaps, setHermesGaps] = useState<HermesGap[]>([]);
  const [hermesDiagrams, setHermesDiagrams] = useState<HermesMermaidBlockType[]>([]);
  const [selectedSource, setSelectedSource] = useState<Record<string, unknown> | null>(null);
  const [sourceDetail, setSourceDetail] = useState<Record<string, unknown> | null>(null);
  const [sourceDetailOpen, setSourceDetailOpen] = useState(false);
  const [hermesLoading, setHermesLoading] = useState(false);
  const [hermesTabError, setHermesTabError] = useState("");
  const [sentinelStatus, setSentinelStatus] = useState<HermesSentinelStatusResponse | null>(null);
  const [sentinelFilter, setSentinelFilter] = useState<SentinelInboxFilter>("new");
  const [sentinelSearch, setSentinelSearch] = useState("");
  const [sentinelBusyId, setSentinelBusyId] = useState<string | null>(null);
  const [diagramModal, setDiagramModal] = useState<HermesMermaidBlockType | null>(null);
  const [diagramSearch, setDiagramSearch] = useState("");
  const [diagramFileFilter, setDiagramFileFilter] = useState("all");
  const [diagramCategoryFilter, setDiagramCategoryFilter] = useState("all");
  const [fullDesignDocOpen, setFullDesignDocOpen] = useState(false);
  const [hermesDesignDoc, setHermesDesignDoc] = useState<HermesFullDesignDocumentResponse | null>(null);
  const [hermesDesignDocError, setHermesDesignDocError] = useState("");

  // Chat state
  const [askDraft, setAskDraft] = useState("");
  const [askSending, setAskSending] = useState(false);
  const [askResponse, setAskResponse] = useState<HermesChatResponse | null>(null);
  const [askError, setAskError] = useState("");
  const [askSessionId, setAskSessionId] = useState("");

  async function sendAskMessage() {
    const msg = askDraft.trim();
    if (!msg || askSending) return;
    setAskSending(true);
    setAskError("");
    setAskResponse(null);
    try {
      const resp = await api.hermesChat({
        message: msg,
        sessionId: askSessionId || undefined,
        context: { userRole: "admin" },
      });
      setAskResponse(resp);
      if (resp.sessionId && !askSessionId) setAskSessionId(resp.sessionId);
      setAskDraft("");
    } catch (e: unknown) {
      setAskError((e as Error).message || String(e));
    } finally {
      setAskSending(false);
    }
  }

  function handleAskKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendAskMessage();
    }
  }

  function handleSuggestedPrompt(prompt: string) {
    setAskDraft(prompt);
  }

  async function updateSentinelNotificationStatus(notificationId: string, status: HermesSentinelMailboxStatus) {
    setSentinelBusyId(notificationId);
    try {
      await api.hermesSetSentinelNotificationStatus(notificationId, status);
      const nextStatus = await api.hermesSentinelStatus();
      setSentinelStatus(nextStatus);
    } catch (e: unknown) {
      setHermesTabError((e as Error).message || String(e));
    } finally {
      setSentinelBusyId(null);
    }
  }
  const [crudLoading, setCrudLoading] = useState(false);
  const [crudError, setCrudError] = useState("");
  const [crudNotice, setCrudNotice] = useState("");
  const [airflowBusyAction, setAirflowBusyAction] = useState<"start" | "stop" | null>(null);
  const [airflowError, setAirflowError] = useState("");
  const [airflowNotice, setAirflowNotice] = useState("");
  const [vocSyncBusy, setVocSyncBusy] = useState(false);
  const [vocSyncError, setVocSyncError] = useState("");
  const [vocSyncNotice, setVocSyncNotice] = useState("");

  const [sourceFilters, setSourceFilters] = useState<SourceFilters>(defaultSourceFilters);
  const [projectFilters, setProjectFilters] = useState<ProjectFilters>(defaultProjectFilters);
  const [overrideFilters, setOverrideFilters] = useState<OverrideFilters>(defaultOverrideFilters);

  const [sources, setSources] = useState<MsrpSource[]>([]);
  const [projects, setProjects] = useState<ConfigProject[]>([]);
  const [overrides, setOverrides] = useState<MatchOverride[]>([]);

  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [editingProjectId, setEditingProjectId] = useState<string | null>(null);
  const [editingOverrideId, setEditingOverrideId] = useState<string | null>(null);

  const [sourceForm, setSourceForm] = useState<SourceFormState>(defaultSourceForm);
  const [projectForm, setProjectForm] = useState<ProjectFormState>(defaultProjectForm);
  const [overrideForm, setOverrideForm] = useState<OverrideFormState>(defaultOverrideForm);

  async function loadOverview(options?: { silent?: boolean }) {
    if (options?.silent) {
      setRefreshing(true);
    } else {
      setLoading(true);
    }
    setError("");
    try {
      setOverview(await api.getDataManagementOverview());
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }

  async function loadVocOverview(country?: string, options?: { silent?: boolean }) {
    if (!options?.silent) {
      setVocOverviewLoading(true);
    }
    setVocOverviewError("");
    try {
      const result = await api.getVocManagementOverview(country);
      setVocOverview(result);
      if (!country && result.selectedCountryCode) {
        setVocCountry((current) => current || result.selectedCountryCode);
      }
    } catch (err) {
      setVocOverviewError((err as Error).message);
    } finally {
      setVocOverviewLoading(false);
    }
  }

  function toggleDomainRecentItems(domainKey: string) {
    setExpandedDomains((current) => ({
      ...current,
      [domainKey]: !current[domainKey],
    }));
  }

  async function loadCrudData(tab: CrudEntityTab = crudTab) {
    setCrudLoading(true);
    setCrudError("");
    try {
      if (tab === "msrp-sources") {
        const res = await api.listMsrpSources({
          country: sourceFilters.country || undefined,
          brand: sourceFilters.brand || undefined,
          enabled: sourceFilters.enabled === "all" ? undefined : sourceFilters.enabled === "true",
          limit: 80,
        });
        setSources(res.items);
        return;
      }
      if (tab === "engineering-projects") {
        const res = await api.listProjects({
          status: projectFilters.status || undefined,
          brand: projectFilters.brand || undefined,
          market_country: projectFilters.marketCountry || undefined,
          limit: 80,
        });
        setProjects(res.items);
        return;
      }
      const res = await api.listMatchOverrides({
        country: overrideFilters.country || undefined,
        brand: overrideFilters.brand || undefined,
        jato_model: overrideFilters.jatoModel || undefined,
        limit: 80,
      });
      setOverrides(res.items);
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  useEffect(() => {
    const nextSubpage = resolveDataSubpageFromLocation(location.search, location.hash, location.pathname);
    setSubpage((current) => (current === nextSubpage ? current : nextSubpage));
  }, [location.hash, location.pathname, location.search]);

  useEffect(() => {
    const nextHermesSubtab = resolveHermesSubtabFromLocation(location.search);
    setHermesSubtab((current) => (current === nextHermesSubtab ? current : nextHermesSubtab));
  }, [location.search]);

  useEffect(() => {
    void loadOverview();
    void loadVocOverview();
  }, []);

  useEffect(() => {
    if (overview?.database.connected) {
      void loadCrudData(crudTab);
    }
  }, [crudTab, overview?.database.connected]);

  // Eager-load lightweight essentials for Hermes header
  useEffect(() => {
    if ((subpage !== "hermes" && subpage !== "overview") || hermesOverview) return;
    setHermesLoading(true);
    Promise.allSettled([
      api.hermesOverview().then(setHermesOverview),
      api.hermesArchitecture().then(setHermesArch),
      api.hermesToolchain().then(setHermesToolchain),
      api.hermesDailySummary().then(setHermesDaily),
    ]).finally(() => setHermesLoading(false));
  }, [subpage, hermesOverview]);

  // Lazy-load per-subtab data
  useEffect(() => {
    if (subpage !== "hermes") return;
    setHermesTabError("");
    if (hermesSubtab === "activity" && !hermesActivity) {
      api.hermesActivityHeatmap().then(setHermesActivity).catch((e: Error) => setHermesTabError(e.message));
      api.hermesEvidenceLedger().then(setHermesEvidence).catch((e: Error) => setHermesTabError(e.message));
    }
    if (hermesSubtab === "cost" && !hermesCost) {
      api.hermesCost().then(setHermesCost).catch((e: Error) => setHermesTabError(e.message));
      api.hermesCostHeatmap().then(setHermesCostHeatmap).catch((e: Error) => setHermesTabError(e.message));
    }
    if (hermesSubtab === "activity" || hermesSubtab === "roadmap") {
      if (!hermesPipelines) api.hermesPipelineHealth().then(setHermesPipelines).catch((e: Error) => setHermesTabError(e.message));
      if (hermesPipelineStatuses.length === 0) api.hermesPipelineStatuses().then(setHermesPipelineStatuses).catch((e: Error) => setHermesTabError(e.message));
      if (!hermesSources) api.hermesSourceQuality().then(setHermesSources).catch((e: Error) => setHermesTabError(e.message));
    }
    if (hermesSubtab === "roadmap" && hermesProposals.length === 0) {
      api.hermesProposals().then(setHermesProposals).catch((e: Error) => setHermesTabError(e.message));
      api.hermesGaps().then(setHermesGaps).catch((e: Error) => setHermesTabError(e.message));
      if (!featureKanban) api.hermesFeatureKanban().then(setFeatureKanban).catch((e: Error) => setHermesTabError(e.message));
    }
    if (hermesSubtab === "diagrams" && hermesDiagrams.length === 0) {
      api.hermesMarkdownDiagrams().then(setHermesDiagrams).catch((e: Error) => setHermesTabError(e.message));
    }
  }, [featureKanban, hermesActivity, hermesCost, hermesCostHeatmap, hermesDiagrams.length, hermesEvidence, hermesGaps.length, hermesPipelineStatuses.length, hermesPipelines, hermesProposals.length, hermesSources, hermesSubtab, subpage]);

  useEffect(() => {
    if (subpage !== "hermes" || (hermesSubtab !== "activity" && hermesSubtab !== "roadmap")) return;
    api.hermesMsrpCountryProgress(hermesMsrpRunId || undefined)
      .then(setHermesMsrpProgress)
      .catch(() => {});
  }, [hermesMsrpRunId, hermesSubtab, subpage]);

  useEffect(() => {
    if (subpage !== "hermes" || (hermesSubtab !== "activity" && hermesSubtab !== "roadmap") || hermesMsrpHistory) return;
    api.hermesMsrpDryrunHistory()
      .then(setHermesMsrpHistory)
      .catch(() => {});
  }, [hermesMsrpHistory, hermesSubtab, subpage]);

  useEffect(() => {
    if (subpage !== "features" || featureKanban) return;
    api.hermesFeatureKanban().then(setFeatureKanban).catch(() => {});
  }, [subpage, featureKanban]);

  // Sentinel polling
  useEffect(() => {
    if (subpage !== "hermes" && subpage !== "overview") return;
    const poll = () => {
      api.hermesSentinelStatus().then(setSentinelStatus).catch(() => {});
    };
    poll();
    const iv = setInterval(poll, 10000);
    return () => clearInterval(iv);
  }, [subpage]);

  useEffect(() => {
    if (subpage !== "hermes" || !fullDesignDocOpen || hermesDesignDoc) return;
    setHermesDesignDocError("");
    api.hermesFullDesignDocument()
      .then(setHermesDesignDoc)
      .catch((e: Error) => setHermesDesignDocError(e.message));
  }, [fullDesignDocOpen, hermesDesignDoc, subpage]);

  const activityColumns = useMemo(
    () => buildActivityHeatmapColumns(overview?.activity.days ?? []),
    [overview]
  );
  const pipelineDisplayRows = buildPipelineDisplayRows(hermesPipelineStatuses, hermesPipelines);

  const airflowStatus: DataManagementAirflowStatus | null = overview?.airflow ?? null;
  const selectedVocCountryStatus = vocOverview?.availableCountries.find(
    (item) => item.code === vocOverview.selectedCountryCode,
  )?.status ?? "warning";

  function startEditSource(item: MsrpSource) {
    setEditingSourceId(item.id);
    setSourceForm({
      sourceCode: item.sourceCode,
      country: item.country,
      brand: item.brand,
      sourceUrl: item.sourceUrl,
      sourceType: item.sourceType,
      tier: item.tier,
      extractorName: item.extractorName,
      extractorVersion: item.extractorVersion,
      priceSemantics: item.priceSemantics,
      requiresLocation: item.requiresLocation,
      enabled: item.enabled,
      notes: item.notes ?? "",
    });
  }

  function startEditProject(item: ConfigProject) {
    setEditingProjectId(item.id);
    setProjectForm({
      projectCode: item.projectCode,
      brand: item.brand,
      model: item.model,
      marketCountry: item.marketCountry,
      displayName: item.displayName,
      status: item.status,
    });
  }

  function startEditOverride(item: MatchOverride) {
    setEditingOverrideId(item.id);
    setOverrideForm({
      country: item.country,
      brand: item.brand,
      jatoModel: item.jatoModel,
      jatoTrim: item.jatoTrim,
      officialModel: item.officialModel,
      officialTrim: item.officialTrim,
      validFromDate: item.validFromDate,
      validToDate: item.validToDate ?? "",
      overrideReason: item.overrideReason,
      createdBy: item.createdBy,
    });
  }

  function resetSourceEditor() {
    setEditingSourceId(null);
    setSourceForm(defaultSourceForm());
  }

  function resetProjectEditor() {
    setEditingProjectId(null);
    setProjectForm(defaultProjectForm());
  }

  function resetOverrideEditor() {
    setEditingOverrideId(null);
    setOverrideForm(defaultOverrideForm());
  }

  async function handleSourceSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setCrudError("");
    setCrudNotice("");
    setCrudLoading(true);
    try {
      if (editingSourceId) {
        await api.patchMsrpSource(editingSourceId, {
          country: sourceForm.country,
          brand: sourceForm.brand,
          source_url: sourceForm.sourceUrl,
          source_type: sourceForm.sourceType,
          tier: sourceForm.tier,
          extractor_name: sourceForm.extractorName,
          extractor_version: sourceForm.extractorVersion,
          price_semantics: sourceForm.priceSemantics,
          requires_location: sourceForm.requiresLocation,
          enabled: sourceForm.enabled,
          notes: sourceForm.notes.trim() || null,
        });
        setCrudNotice("MSRP source 已更新。");
      } else {
        await api.createMsrpSource({
          source_code: sourceForm.sourceCode,
          country: sourceForm.country,
          brand: sourceForm.brand,
          source_url: sourceForm.sourceUrl,
          source_type: sourceForm.sourceType,
          tier: sourceForm.tier,
          extractor_name: sourceForm.extractorName,
          extractor_version: sourceForm.extractorVersion,
          price_semantics: sourceForm.priceSemantics,
          requires_location: sourceForm.requiresLocation,
          enabled: sourceForm.enabled,
          notes: sourceForm.notes.trim() || null,
        });
        setCrudNotice("MSRP source 已创建。");
      }
      resetSourceEditor();
      await loadCrudData("msrp-sources");
      await loadOverview({ silent: true });
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  async function handleProjectSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setCrudError("");
    setCrudNotice("");
    setCrudLoading(true);
    try {
      if (editingProjectId) {
        await api.patchProject(editingProjectId, {
          brand: projectForm.brand,
          model: projectForm.model,
          market_country: projectForm.marketCountry,
          display_name: projectForm.displayName,
          status: projectForm.status,
        });
        setCrudNotice("Engineering project 已更新。");
      } else {
        await api.createProject({
          project_code: projectForm.projectCode,
          brand: projectForm.brand,
          model: projectForm.model,
          market_country: projectForm.marketCountry,
          display_name: projectForm.displayName,
          status: projectForm.status,
        });
        setCrudNotice("Engineering project 已创建。");
      }
      resetProjectEditor();
      await loadCrudData("engineering-projects");
      await loadOverview({ silent: true });
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  async function handleOverrideSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setCrudError("");
    setCrudNotice("");
    setCrudLoading(true);
    try {
      if (editingOverrideId) {
        await api.patchMatchOverride(editingOverrideId, {
          official_model: overrideForm.officialModel,
          official_trim: overrideForm.officialTrim,
          valid_from_date: overrideForm.validFromDate,
          valid_to_date: overrideForm.validToDate.trim() || null,
          override_reason: overrideForm.overrideReason,
          created_by: overrideForm.createdBy,
        });
        setCrudNotice("Review override 已更新。");
      } else {
        await api.createMatchOverride({
          country: overrideForm.country,
          brand: overrideForm.brand,
          jato_model: overrideForm.jatoModel,
          jato_trim: overrideForm.jatoTrim,
          official_model: overrideForm.officialModel,
          official_trim: overrideForm.officialTrim,
          valid_from_date: overrideForm.validFromDate,
          valid_to_date: overrideForm.validToDate.trim() || null,
          override_reason: overrideForm.overrideReason,
          created_by: overrideForm.createdBy,
        });
        setCrudNotice("Review override 已创建。");
      }
      resetOverrideEditor();
      await loadCrudData("review-overrides");
      await loadOverview({ silent: true });
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  async function handleDeactivateSource(sourceId: string) {
    if (!window.confirm("确认停用这个 MSRP source 吗？")) {
      return;
    }
    setCrudLoading(true);
    setCrudError("");
    setCrudNotice("");
    try {
      await api.deleteMsrpSource(sourceId);
      if (editingSourceId === sourceId) {
        resetSourceEditor();
      }
      setCrudNotice("MSRP source 已停用。");
      await loadCrudData("msrp-sources");
      await loadOverview({ silent: true });
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  async function handleArchiveProject(projectId: string) {
    if (!window.confirm("确认归档这个 Engineering project 吗？")) {
      return;
    }
    setCrudLoading(true);
    setCrudError("");
    setCrudNotice("");
    try {
      await api.deleteProject(projectId);
      if (editingProjectId === projectId) {
        resetProjectEditor();
      }
      setCrudNotice("Engineering project 已归档，关联 variants 已停用。");
      await loadCrudData("engineering-projects");
      await loadOverview({ silent: true });
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  async function handleDeleteOverride(overrideId: string) {
    if (!window.confirm("确认删除这个 review override 吗？")) {
      return;
    }
    setCrudLoading(true);
    setCrudError("");
    setCrudNotice("");
    try {
      await api.deleteMatchOverride(overrideId);
      if (editingOverrideId === overrideId) {
        resetOverrideEditor();
      }
      setCrudNotice("Review override 已删除。");
      await loadCrudData("review-overrides");
      await loadOverview({ silent: true });
    } catch (err) {
      setCrudError((err as Error).message);
    } finally {
      setCrudLoading(false);
    }
  }

  async function handleAirflowAction(action: "start" | "stop") {
    setAirflowBusyAction(action);
    setAirflowError("");
    setAirflowNotice("");
    try {
      const result = action === "start"
        ? await api.startAirflow()
        : await api.stopAirflow();
      setAirflowNotice(result.detail);
      await loadOverview({ silent: true });
    } catch (err) {
      setAirflowError((err as Error).message);
    } finally {
      setAirflowBusyAction(null);
    }
  }

  function handleOpenAirflowUi(status: DataManagementAirflowStatus) {
    window.open(status.uiUrl, "_blank", "noopener,noreferrer");
  }

  async function handleVocSync() {
    setVocSyncBusy(true);
    setVocSyncError("");
    setVocSyncNotice("");
    try {
      const result = await api.syncVocRawToStore();
      setVocSyncNotice(
        `已同步 ${result.countryCount} 个国家 / ${result.sourceRunCount} 个 source runs / ${result.documentCount} 篇文档到 PostgreSQL。`,
      );
      await loadOverview({ silent: true });
      await loadVocOverview(vocCountry || undefined, { silent: true });
    } catch (err) {
      setVocSyncError((err as Error).message);
    } finally {
      setVocSyncBusy(false);
    }
  }

  return (
    <section className="crud-shell data-management-shell">
      <header className="header-card dashboard-hero crud-hero">
        <div className="dashboard-hero-grid">
          <div className="dashboard-hero-copy crud-hero-copy">
            <span className="dashboard-kicker">Admin · Data Ops</span>
            <h1>数据总览</h1>
            <p>
              统一查看 JATO、Country Assistant、News、Wiki、MSRP、Engineering
              的文件与数据库状态。现在数据库侧也支持一轮基础 CRUD，先覆盖管理价值最高的 Source / Project / Override。
            </p>
            <div className="dashboard-hero-actions crud-hero-actions">
              <Link to="/msrp">返回 MSRP</Link>
              <button type="button" onClick={() => void loadOverview({ silent: true })} disabled={refreshing}>
                {refreshing ? "刷新中…" : "刷新概览"}
              </button>
            </div>
          </div>
          <div className="dashboard-hero-metrics">
            <div className="metric-chip">
              <span>Domains</span>
              <strong>{overview?.domains.length ?? "-"}</strong>
            </div>
            <div className="metric-chip">
              <span>Files</span>
              <strong>{overview?.fileInventory.length ?? "-"}</strong>
            </div>
            <div className="metric-chip">
              <span>Tables</span>
              <strong>{overview?.databaseTables.length ?? "-"}</strong>
            </div>
            <div className="metric-chip">
              <span>Updated</span>
              <strong>{formatDataManagementTimestamp(overview?.generatedAt)}</strong>
            </div>
          </div>
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <div className="admin-tabs" style={{ marginBottom: 16 }}>
        <button type="button" className={`admin-tab${subpage === "overview" ? " is-active" : ""}`} onClick={() => setSubpage("overview")}>Overview</button>
        <button type="button" className={`admin-tab${subpage === "hermes" ? " is-active" : ""}`} onClick={() => setSubpage("hermes")}>Hermes</button>
        <button type="button" className={`admin-tab${subpage === "features" ? " is-active" : ""}`} onClick={() => setSubpage("features")}>Features</button>
        <button type="button" className={`admin-tab${subpage === "voc" ? " is-active" : ""}`} onClick={() => setSubpage("voc")}>VOC</button>
        <button type="button" className={`admin-tab${subpage === "admin" ? " is-active" : ""}`} onClick={() => setSubpage("admin")}>Admin</button>
        <button type="button" className={`admin-tab${subpage === "dryrun" ? " is-active" : ""}`} onClick={() => setSubpage("dryrun")}>MSRP Dryrun</button>
        <button type="button" className={`admin-tab${subpage === "order-genius" ? " is-active" : ""}`} onClick={() => setSubpage("order-genius")}>Order Genius</button>
        <button type="button" className={`admin-tab${subpage === "material-master" ? " is-active" : ""}`} onClick={() => setSubpage("material-master")}>Material Master</button>
      </div>

      {subpage === "voc" ? (
        <div className="card crud-card">
          <div className="admin-card-header"><div><h2>VOC Operations</h2></div></div>
          <div style={{ padding: 16 }}>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <button type="button" className="btn btn-sm btn-primary"
                disabled={vocSyncBusy}
                onClick={() => {
                  setVocSyncBusy(true); setVocSyncError(""); setVocSyncNotice("");
                  api.syncVocRawToStore().then((res) => {
                    setVocSyncNotice(`Synced: ${(res as unknown as Record<string,unknown>).countryCount || "?"} countries, ${(res as unknown as Record<string,unknown>).documentCount || "?"} documents`);
                  }).catch((e) => setVocSyncError(String(e))).finally(() => setVocSyncBusy(false));
                }}>
                {vocSyncBusy ? "Syncing..." : "Sync VOC to PostgreSQL"}
              </button>
              <button type="button" className="btn btn-sm btn-ghost"
                onClick={() => { setVocCountry(""); loadVocOverview(undefined, { silent: true }); }}>
                Refresh VOC Overview
              </button>
              <span style={{ fontSize: 12, color: "#64748b" }}>
                VOC timer: daily 01:45 UTC. Last run: check journalctl on server.
              </span>
            </div>
            {vocSyncError ? <div className="alert alert-error" style={{marginTop:8}}>{vocSyncError}</div> : null}
            {vocSyncNotice ? <div className="alert alert-success" style={{marginTop:8}}>{vocSyncNotice}</div> : null}
          </div>
        </div>
      ) : null}

      {subpage === "features" ? (
        <>
          <div className="card crud-card">
            <div className="admin-card-header"><div><h2>Feature Development Kanban</h2></div></div>
            <div style={{ padding: 16 }}>
              {featureKanban ? (
                <>
                  <div style={{display:"flex",gap:12,marginBottom:16,fontSize:12,color:"#64748b"}}>
                    <span>Total: {(featureKanban.summary as Record<string,number>)?.total} features</span>
                    <span>Active: {(featureKanban.summary as Record<string,number>)?.active}</span>
                    <span>Beta: {(featureKanban.summary as Record<string,number>)?.beta}</span>
                    <span>Planned: {(featureKanban.summary as Record<string,number>)?.planned}</span>
                    <span>With tests: {(featureKanban.summary as Record<string,number>)?.withTests}</span>
                    <span>With issues: {(featureKanban.summary as Record<string,number>)?.withIssues}</span>
                  </div>
                  <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12}}>
                    {(["planned","beta","active","archived"] as string[]).map((col) => {
                      const column = (featureKanban.columns as Record<string,unknown>)[col] as Record<string,unknown>;
                      const allFeatures = (column?.features as unknown[]) || [];
                      const showAll = allFeatures.length <= 10;
                      const visibleFeatures = showAll ? allFeatures : allFeatures.slice(0, 10);
                      return (
                        <div key={col} style={{background:"#f8fafc",borderRadius:8,padding:12}}>
                          <div style={{fontWeight:700,fontSize:13,marginBottom:10,color:column?.color as string,display:"flex",justifyContent:"space-between"}}>
                            <span>{column?.label as string}</span>
                            <span style={{fontSize:11,background:"#e2e8f0",borderRadius:10,padding:"0 8px"}}>{allFeatures.length}</span>
                          </div>
                          <div style={{maxHeight:520,overflowY:"auto",paddingRight:4}}>
                          {visibleFeatures.map((f: unknown, idx: number) => {
                            const feat = f as Record<string,unknown>;
                            const risk = String(feat.riskLevel || "low");
                            const riskColor = risk === "high" ? "#ef4444" : risk === "medium" ? "#f59e0b" : "#22c55e";
                            const hasTests = (feat.tests as unknown[])?.length > 0;
                            const hasDocs = (feat.docs as unknown[])?.length > 0;
                            const hasIssues = (feat.knownIssues as unknown[])?.length > 0;
                            const deps = (feat.dependencies as string[]) || [];
                            return (
                              <div key={String(feat.featureId)}>
                                {/* Dependency connector dot + line */}
                                {deps.length > 0 && (
                                  <div style={{display:"flex",alignItems:"center",gap:4,marginBottom:2,paddingLeft:8}}>
                                    <div style={{width:6,height:6,borderRadius:"50%",background:"#94a3b8"}} />
                                    <div style={{fontSize:9,color:"#94a3b8"}}>{deps.map((d: string) => d.replace("feature.","")).join(", ")}</div>
                                  </div>
                                )}
                                <div style={{
                                  background:"#fff",borderRadius:6,padding:"10px 12px",marginBottom:8,
                                  border:"1px solid #e2e8f0",borderLeft:`3px solid ${feat.color || "#94a3b8"}`,fontSize:12,
                                  marginLeft: deps.length > 0 ? 12 : 0,
                                }}>
                                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:4}}>
                                    <span style={{fontWeight:600,fontSize:11}}>{String(feat.name)}</span>
                                    <span style={{fontSize:8,background:feat.implementationStatus==="implemented"?"#dcfce7":feat.implementationStatus==="partial"?"#dbeafe":"#fef3c7",color:feat.implementationStatus==="implemented"?"#166534":feat.implementationStatus==="partial"?"#1e40af":"#92400e",padding:"1px 5px",borderRadius:3,fontWeight:600,whiteSpace:"nowrap"}}>{String(feat.phase||feat.implementationStatus||"")}</span>
                                  </div>
                                  <div style={{display:"flex",gap:4,flexWrap:"wrap",marginBottom:3}}>
                                    {(feat.routes as unknown[])?.length > 0 && <span style={{fontSize:9,background:"#e0f2fe",color:"#0369a1",padding:"0 5px",borderRadius:2}}>{(feat.routes as string[]).join(" ")}</span>}
                                    {(feat.backendApis as unknown[])?.length > 0 && <span style={{fontSize:9,background:"#fef3c7",color:"#92400e",padding:"0 5px",borderRadius:2}}>{(feat.backendApis as string[]).length} APIs</span>}
                                  </div>
                                  <div style={{display:"flex",gap:6,fontSize:9,color:"#64748b"}}>
                                    {hasTests && <span style={{color:"#22c55e"}}>Tests</span>}
                                    {hasDocs && <span style={{color:"#3b82f6"}}>Docs</span>}
                                    {hasIssues && <span style={{color:"#ef4444"}}>Issues</span>}
                                    <span style={{color:riskColor,fontWeight:600}}>{risk}</span>
                                  </div>
                                </div>
                                {/* Connector line between cards with deps */}
                                {idx < visibleFeatures.length - 1 && deps.length > 0 && (
                                  <div style={{width:2,height:4,background:"#e2e8f0",marginLeft:10}} />
                                )}
                              </div>
                            );
                          })}
                          </div>
                          {allFeatures.length > 10 && (
                            <button type="button" className="btn btn-sm btn-ghost" style={{width:"100%",marginTop:8,fontSize:11}}
                              onClick={() => {}}>
                              +{allFeatures.length - 10} more
                            </button>
                          )}
                          {allFeatures.length === 0 && <div style={{color:"#94a3b8",fontSize:11,textAlign:"center",padding:20}}>—</div>}
                        </div>
                      );
                    })}
                  </div>
                </>
              ) : <LoadingSurface mode="inline" label="Loading feature kanban..." kicker="Features" />}
            </div>
          </div>
          <div className="card crud-card" style={{ marginTop: 16 }}>
            <div className="admin-card-header">
              <div>
                <h2>Reusable Building Blocks</h2>
                <p style={{ margin: "4px 0 0", color: "#64748b", fontSize: 12 }}>
                  Frontend components, utilities, and backend scanners worth reusing before writing another upload workflow.
                </p>
              </div>
            </div>
            <div style={{ padding: 16 }}>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(240px, 1fr))", gap: 12 }}>
                {REUSABLE_BUILDING_BLOCKS.map((block) => (
                  <div key={block.name} style={{ border: "1px solid #e2e8f0", borderRadius: 8, background: "#ffffff", padding: 12, display: "grid", gap: 8 }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: 8 }}>
                      <strong style={{ fontSize: 13, color: "#0f172a" }}>{block.name}</strong>
                      <span style={{ border: "1px solid #cbd5e1", borderRadius: 999, color: "#475569", fontSize: 10, fontWeight: 700, padding: "2px 8px" }}>
                        {block.layer}
                      </span>
                    </div>
                    <div style={{ color: "#2563eb", fontSize: 11, overflowWrap: "anywhere" }}>{block.path}</div>
                    <div style={{ color: "#475569", fontSize: 12, lineHeight: 1.45 }}>{block.usage}</div>
                    <div style={{ color: "#64748b", fontSize: 11 }}>Used by: {block.owner}</div>
                  </div>
                ))}
              </div>
              <div style={{ marginTop: 12, color: "#64748b", fontSize: 12 }}>
                Full notes live in <code>src/components/REUSABLE_COMPONENTS.md</code>.
              </div>
            </div>
          </div>
        </>
      ) : null}

      {subpage === "hermes" ? (
        hermesLoading ? (
          <LoadingSurface mode="inline" label="Loading Hermes..." kicker="Hermes" />
        ) : (
          <>
            {/* Error banner */}
            {hermesTabError && (
              <div style={{marginBottom:12,padding:"8px 12px",background:"#fef2f2",borderRadius:6,fontSize:12,color:"#ef4444",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                <span>Data load error: {hermesTabError}</span>
                <button className="btn btn-sm btn-ghost" style={{fontSize:11,color:"#ef4444"}} onClick={() => {setHermesTabError(""); setHermesSubtab(hermesSubtab);}}>Retry</button>
              </div>
            )}

            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(min(100%,360px),1fr))",gap:12,alignItems:"stretch",marginBottom:16}}>
              <div className="card crud-card" style={{height:440,padding:12,display:"grid",gridTemplateRows:"auto 1fr auto",gap:10,overflow:"hidden"}}>
                <div>
                  <div style={{fontSize:11,fontWeight:800,letterSpacing:"0.08em",textTransform:"uppercase",color:"#64748b"}}>Hermes 小管家</div>
                  <strong style={{fontSize:16,color:"#0f172a"}}>{askSending ? "正在查询" : askResponse ? "已返回回答" : "可以继续开发"}</strong>
                </div>
                <div style={{overflowY:"auto",paddingRight:2}}>
                  {askError && (
                    <div style={{padding:"8px 12px",background:"#fef2f2",borderRadius:6,fontSize:12,color:"#ef4444"}}>
                      {askError}
                    </div>
                  )}
                  {askResponse ? (
                    <HermesAskResponseCard
                      response={askResponse}
                      onDismiss={() => setAskResponse(null)}
                      onSuggestedAction={(action: HermesChatSuggestedAction) => {
                        if (action.intent) setAskDraft(action.intent.replace(/_/g, " "));
                        if (action.command) {
                          api.hermesCommandExecute({ commandId: action.command }).catch((e: Error) => setAskError(e.message));
                        }
                      }}
                    />
                  ) : (
                    <div style={{height:"100%",minHeight:180,display:"flex",alignItems:"center",justifyContent:"center",border:"1px solid #e2e8f0",borderRadius:6,color:"#64748b",fontSize:12}}>
                      选择一个快捷问题，或直接输入要 Hermes 检查的事项。
                    </div>
                  )}
                </div>
                <div style={{display:"grid",gap:8}}>
                  <div className="hermes-suggested-prompts" style={{display:"flex",flexWrap:"wrap",gap:6}}>
                    {[
                      "Show open governance gaps",
                      "Run source audit",
                      "Recent activity",
                      "Evidence ledger past 7 days",
                      "Cost status",
                    ].map((p) => (
                      <button
                        key={p}
                        className="btn btn-sm btn-ghost"
                        style={{fontSize:10,background:"#f1f5f9",borderRadius:4,padding:"3px 10px"}}
                        onClick={() => handleSuggestedPrompt(p)}
                      >
                        {p}
                      </button>
                    ))}
                  </div>
                  <div className="hermes-ask-bar">
                    <input
                      type="text"
                      className="hermes-ask-input"
                      placeholder="Ask Hermes anything about this system..."
                      value={askDraft}
                      onChange={(e) => setAskDraft(e.target.value)}
                      onKeyDown={handleAskKeyDown}
                      disabled={askSending}
                    />
                    <button
                      className="btn btn-sm btn-primary"
                      onClick={sendAskMessage}
                      disabled={askSending || !askDraft.trim()}
                      style={{padding:"6px 14px",borderRadius:"0 8px 8px 0"}}
                    >
                      {askSending ? "..." : "Ask"}
                    </button>
                  </div>
                </div>
              </div>

              {sentinelStatus ? (() => {
                const notifications = sentinelStatus.notifications ?? [];
                const filteredNotifications = notifications.filter((notification) =>
                  matchesSentinelFilter(notification, sentinelFilter, sentinelSearch)
                );
                const countFor = (filter: SentinelInboxFilter) =>
                  notifications.filter((notification) => matchesSentinelFilter(notification, filter, "")).length;
                return (
                  <div className="card crud-card" style={{height:440,padding:12,display:"grid",gridTemplateRows:"auto auto 1fr auto",gap:10,overflow:"hidden"}}>
                    <div>
                      <strong style={{fontSize:13}}>SENTINEL INBOX</strong>
                      <div style={{fontSize:11,color:"#64748b"}}>{sentinelStatus.overall === "ok" ? "Clear" : String(sentinelStatus.overall).toUpperCase()} · {sentinelStatus.unreadCount ?? 0} unread</div>
                    </div>
                    <div style={{display:"grid",gap:8}}>
                      <input
                        type="search"
                        value={sentinelSearch}
                        onChange={(e) => setSentinelSearch(e.target.value)}
                        placeholder="Search alerts..."
                        style={{width:"100%",fontSize:12,padding:"7px 10px",border:"1px solid #cbd5e1",borderRadius:6}}
                      />
                      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:4}}>
                        {SENTINEL_FILTERS.map((filter) => (
                          <button
                            key={filter.key}
                            type="button"
                            className={`btn btn-sm${sentinelFilter === filter.key ? " btn-primary" : " btn-ghost"}`}
                            style={{fontSize:11,borderRadius:6,padding:"5px 8px"}}
                            onClick={() => setSentinelFilter(filter.key)}
                          >
                            {filter.label} <span style={{marginLeft:4,color:sentinelFilter === filter.key ? "inherit" : "#64748b"}}>{countFor(filter.key)}</span>
                          </button>
                        ))}
                      </div>
                    </div>
                    <div style={{overflowY:"auto",display:"grid",alignContent:"start",gap:8,paddingRight:2}}>
                      {filteredNotifications.length === 0 ? (
                        <div style={{border:"1px solid #e2e8f0",borderRadius:6,padding:"14px 12px",color:"#64748b",fontSize:12}}>
                          No inbox items.
                        </div>
                      ) : filteredNotifications.map((notification, notificationIndex) => {
                        const severity = String(notification.severity || "low");
                        const color = getSentinelSeverityColor(severity);
                        const mailboxStatus = normalizeSentinelMailboxStatus(String(notification.status || "new"));
                        const pipelineDetails = getSentinelPipelineDetails(notification);
                        const artifactRefs = getSentinelArtifactRefs(notification);
                        return (
                          <div key={`${notification.id}-${notificationIndex}`} style={{border:"1px solid #e2e8f0",borderLeft:`4px solid ${color}`,borderRadius:6,padding:"9px 10px",background:mailboxStatus === "new" ? "#ffffff" : "#f8fafc",fontSize:12}}>
                            <div style={{display:"flex",justifyContent:"space-between",gap:8,alignItems:"flex-start"}}>
                              <div>
                                <div style={{fontWeight:700,color,letterSpacing:0}}>{notification.title}</div>
                                <div style={{fontSize:10,color:"#64748b",marginTop:1}}>{notification.source} · {severity.toUpperCase()} · {mailboxStatus}</div>
                              </div>
                              <div style={{display:"flex",gap:4,flexShrink:0}}>
                                {mailboxStatus === "new" ? (
                                  <button className="btn btn-sm btn-ghost" style={{fontSize:10,padding:"2px 6px"}} disabled={sentinelBusyId === notification.id} onClick={() => updateSentinelNotificationStatus(notification.id, "read")}>Read</button>
                                ) : (
                                  <button className="btn btn-sm btn-ghost" style={{fontSize:10,padding:"2px 6px"}} disabled={sentinelBusyId === notification.id} onClick={() => updateSentinelNotificationStatus(notification.id, "new")}>Unread</button>
                                )}
                                {mailboxStatus !== "archived" && (
                                  <button className="btn btn-sm btn-ghost" style={{fontSize:10,padding:"2px 6px"}} disabled={sentinelBusyId === notification.id} onClick={() => updateSentinelNotificationStatus(notification.id, "archived")}>Archive</button>
                                )}
                              </div>
                            </div>
                            <div style={{color:"#334155",fontSize:12,marginTop:6,lineHeight:1.45}}>{notification.body}</div>
                            {notification.recommendedAction && (
                              <div style={{fontSize:11,color:"#475569",marginTop:6}}>Action: {notification.recommendedAction}</div>
                            )}
                            {pipelineDetails.length > 0 && (
                              <div style={{display:"grid",gridTemplateColumns:"repeat(2,minmax(0,1fr))",gap:6,marginTop:8}}>
                                {pipelineDetails.map((detail) => (
                                  <div key={`${notification.id}-${detail.label}`} style={{border:"1px solid #e2e8f0",borderRadius:4,padding:"5px 6px",minWidth:0}}>
                                    <div style={{fontSize:9,color:"#64748b",textTransform:"uppercase"}}>{detail.label}</div>
                                    <div style={{fontSize:11,color:"#0f172a",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={detail.value}>{detail.value}</div>
                                  </div>
                                ))}
                              </div>
                            )}
                            {artifactRefs.length > 0 && (
                              <div style={{fontSize:10,color:"#64748b",marginTop:6,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={artifactRefs.join(" · ")}>
                                Artifacts: {artifactRefs.join(" · ")}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                    <div style={{display:"flex",gap:8,flexWrap:"wrap",borderTop:"1px solid #e2e8f0",paddingTop:8}}>
                      {(sentinelStatus.probes ?? []).map((probe) => {
                        const status = String(probe.overall || "ok");
                        const dot = status === "critical" ? "#dc2626" : status === "warning" ? "#d97706" : "#16a34a";
                        return (
                          <span key={probe.probe} style={{fontSize:10,display:"flex",alignItems:"center",gap:4}}>
                            <span style={{width:6,height:6,borderRadius:"50%",background:dot,display:"inline-block"}} />
                            {probe.probe}
                          </span>
                        );
                      })}
                    </div>
                  </div>
                );
              })() : (
                <div className="card crud-card" style={{height:440,padding:12,display:"flex",alignItems:"center",justifyContent:"center",color:"#64748b",fontSize:12}}>
                  Loading Sentinel inbox...
                </div>
              )}
            </div>

            {/* Hermes summary bar */}
            <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12,marginBottom:16}}>
              <div className="card crud-card" style={{padding:12,textAlign:"center"}}>
                <div style={{fontSize:22,fontWeight:800,color:"#3b82f6"}}>{hermesOverview?.registries?.feature ?? "-"}</div>
                <div style={{fontSize:10,color:"#64748b"}}>Features</div>
              </div>
              <div className="card crud-card" style={{padding:12,textAlign:"center"}}>
                <div style={{fontSize:22,fontWeight:800,color:"#8b5cf6"}}>{hermesOverview?.registries?.pipeline ?? "-"}</div>
                <div style={{fontSize:10,color:"#64748b"}}>Pipelines</div>
              </div>
              <div className="card crud-card" style={{padding:12,textAlign:"center"}}>
                <div style={{fontSize:22,fontWeight:800,color: (hermesDaily?.costStatus === "over_daily" ? "#ef4444" : "#22c55e")}}>{hermesDaily?.costCny?.toFixed(1) ?? "-"}</div>
                <div style={{fontSize:10,color:"#64748b"}}>CNY today / {hermesDaily?.dailyBudgetCny ?? 20} day</div>
              </div>
              <div className="card crud-card" style={{padding:12,textAlign:"center"}}>
                <div style={{fontSize:22,fontWeight:800,color:"#f59e0b"}}>{hermesOverview?.gaps?.open ?? "-"}</div>
                <div style={{fontSize:10,color:"#64748b"}}>Open Gaps</div>
              </div>
            </div>

            {/* Hermes sub-tabs with group labels */}
            <div className="admin-tabs" style={{marginBottom:12,display:"flex",alignItems:"center",gap:4,flexWrap:"wrap"}}>
              <span className="hermes-subtab-group-label">Can</span>
              {(["capabilities"] as HermesSubtab[]).map((st) => (
                <button key={st} type="button" className={`admin-tab${hermesSubtab===st?" is-active":""}`} onClick={()=>setHermesSubtab(st)}>{st.charAt(0).toUpperCase()+st.slice(1)}</button>
              ))}
              <span className="hermes-subtab-group-label" style={{marginLeft:8}}>Understands</span>
              {(["progress","history"] as HermesSubtab[]).map((st) => (
                <button key={st} type="button" className={`admin-tab${hermesSubtab===st?" is-active":""}`} onClick={()=>setHermesSubtab(st)}>{st === "history" ? "History Map" : "Progress"}</button>
              ))}
              <span className="hermes-subtab-group-label" style={{marginLeft:8}}>Does</span>
              {(["activity","cost"] as HermesSubtab[]).map((st) => (
                <button key={st} type="button" className={`admin-tab${hermesSubtab===st?" is-active":""}`} onClick={()=>setHermesSubtab(st)}>{st.charAt(0).toUpperCase()+st.slice(1)}</button>
              ))}
              <span className="hermes-subtab-group-label" style={{marginLeft:8}}>Will</span>
              {(["roadmap"] as HermesSubtab[]).map((st) => (
                <button key={st} type="button" className={`admin-tab${hermesSubtab===st?" is-active":""}`} onClick={()=>setHermesSubtab(st)}>Roadmap</button>
              ))}
              <span className="hermes-subtab-group-label" style={{marginLeft:8}}>Docs</span>
              {(["diagrams"] as HermesSubtab[]).map((st) => (
                <button key={st} type="button" className={`admin-tab${hermesSubtab===st?" is-active":""}`} onClick={()=>setHermesSubtab(st)}>Diagrams</button>
              ))}
            </div>

            {/* ── Capabilities sub-tab (能为我干什么) ── */}
            {hermesSubtab === "capabilities" && (
              <div style={{display:"grid",gap:16}}>
                {/* 4 Governors */}
                <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(280px,1fr))",gap:12}}>
                  {(hermesArch?.modules || []).map((mod) => (
                    <div key={mod.governor} className="card crud-card" style={{padding:14}}>
                      <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:8}}>
                        <span style={{fontSize:20}}>{mod.icon}</span>
                        <div>
                          <strong style={{fontSize:14}}>{mod.governor}</strong>
                          <div style={{fontSize:10,color:"#94a3b8"}}>{mod.phase}</div>
                        </div>
                      </div>
                      <div style={{fontSize:11,color:"#475569",marginBottom:6}}>
                        <strong>Answers:</strong>
                        <ul style={{margin:"4px 0 0 14px",padding:0}}>
                          {mod.answers.slice(0,3).map((a,i)=><li key={i}>{a}</li>)}
                        </ul>
                      </div>
                      <div style={{fontSize:10,color:"#94a3b8"}}>
                        Scripts: {mod.scripts.join(", ")}
                      </div>
                    </div>
                  ))}
                </div>
                {/* Routing Guide */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>What to ask which Governor</h2></div></div>
                  <div style={{padding:12,maxHeight:300,overflowY:"auto"}}>
                    <table style={{width:"100%",fontSize:12,borderCollapse:"collapse"}}>
                      <thead><tr style={{borderBottom:"1px solid #e2e8f0"}}><th style={{textAlign:"left",padding:4}}>Task</th><th style={{textAlign:"left",padding:4}}>Ask</th><th style={{textAlign:"left",padding:4}}>Run</th></tr></thead>
                      <tbody>
                        {(hermesArch?.routing || []).map((r,i) => (
                          <tr key={i} style={{borderBottom:"1px solid #f1f5f9"}}>
                            <td style={{padding:4,fontWeight:500}}>{r.task}</td>
                            <td style={{padding:4,color:"#3b82f6"}}>{r.ask}</td>
                            <td style={{padding:4,fontSize:10,fontFamily:"monospace"}}>{r.run}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
                {/* Toolchain workflow */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Development Workflow (11 steps)</h2></div></div>
                  <div style={{padding:12,display:"flex",gap:8,overflowX:"auto"}}>
                    {(hermesToolchain?.workflow || []).map((w) => (
                      <div key={w.step} style={{minWidth:150,background:"#f8fafc",borderRadius:8,padding:10,border:"1px solid #e2e8f0",fontSize:11}}>
                        <div style={{fontWeight:700,color:"#3b82f6",marginBottom:4}}>Step {w.step}</div>
                        <div style={{fontSize:10,color:"#94a3b8",marginBottom:2}}>{w.phase}</div>
                        <div style={{fontWeight:500,marginBottom:2}}>{w.script}</div>
                        <div style={{color:"#64748b"}}>{w.action}</div>
                      </div>
                    ))}
                  </div>
                </div>
                {/* Operations */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Run Hermes Scripts</h2></div></div>
                  <div style={{padding:12}}>
                    <div style={{display:"flex",flexWrap:"wrap",gap:6,marginBottom:8}}>
                      {Object.entries(HERMES_SCRIPTS_MAP).map(([cmd,label])=>(<button key={cmd} className="btn btn-sm btn-primary" style={{fontSize:11}} onClick={()=>{api.hermesCommandExecute({commandId:cmd}).then((res)=>{const el=document.getElementById(`hout-${cmd}`);if(el)el.textContent=`[${res.status}] exit=${res.exitCode} runId=${res.runId}\n${res.stdout||res.stderr||""}`;if(cmd==="pipeline-audit"){api.hermesPipelineHealth().then(setHermesPipelines);api.hermesPipelineStatuses().then(setHermesPipelineStatuses);}if(cmd==="source-quality")api.hermesSourceQuality().then(setHermesSources);if(cmd==="cost-report")api.hermesCost().then(setHermesCost);}).catch((e)=>{const el=document.getElementById(`hout-${cmd}`);if(el)el.textContent=String(e);});}}>Run {label}</button>))}
                    </div>
                    <div style={{background:"#1e293b",color:"#e2e8f0",borderRadius:6,padding:10,fontFamily:"monospace",fontSize:10,maxHeight:120,overflow:"auto",whiteSpace:"pre-wrap"}}>
                      {Object.keys(HERMES_SCRIPTS_MAP).map(cmd=>(<div key={cmd} id={`hout-${cmd}`} style={{display:"none"}} />))}
                      <span style={{color:"#64748b"}}>Click Run. Output appears here.</span>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* ── Progress sub-tab (feature lifecycle) ── */}
            {hermesSubtab === "progress" && (
              <HermesProgressSwimlane />
            )}

            {/* ── History Map sub-tab (clustered project timeline) ── */}
            {hermesSubtab === "history" && (
              <HermesHistoryMap />
            )}

            {/* ── Activity sub-tab (干了什么) ── */}
            {hermesSubtab === "activity" && (
              <div style={{display:"grid",gap:16}}>
                {/* Activity heatmap */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Activity Heatmap (30 days)</h2></div></div>
                  <div style={{padding:12}}>
                    {hermesActivity ? (
                      <>
                        <div style={{marginBottom:8,fontSize:12,color:"#64748b"}}>{hermesActivity.totalRecords} total records · last run: {hermesActivity.lastRun ? String((hermesActivity.lastRun as Record<string,unknown>).command || "-") : "none"}</div>
                        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(22px,1fr))",gap:2}}>
                          {(hermesActivity.days || []).map((d) => {
                            const count = d.count || 0;
                            const c = count===0?"#f1f5f9":count===1?"#93c5fd":count<=3?"#3b82f6":count<=6?"#1d4ed8":"#1e3a5f";
                            return <div key={d.date} title={`${d.date}: ${count}`} style={{aspectRatio:"1",background:c,borderRadius:2}} />;
                          })}
                        </div>
                        {/* Command breakdown */}
                        {Object.keys(hermesActivity.byCommand).length > 0 && (
                          <div style={{marginTop:12}}>
                            <div style={{fontSize:11,fontWeight:600,marginBottom:4}}>By Command</div>
                            <div style={{display:"flex",flexWrap:"wrap",gap:6}}>
                              {Object.entries(hermesActivity.byCommand).map(([cmd,count]) => (
                                <span key={cmd} style={{fontSize:10,background:"#e2e8f0",borderRadius:4,padding:"2px 6px"}}>{cmd}: {count}</span>
                              ))}
                            </div>
                          </div>
                        )}
                      </>
                    ) : <span style={{color:"#94a3b8",fontSize:11}}>No activity data yet</span>}
                  </div>
                </div>
                {/* Evidence ledger */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Evidence Ledger</h2></div></div>
                  <div style={{padding:12,maxHeight:400,overflowY:"auto"}}>
                    {hermesEvidence && hermesEvidence.records.length > 0 ? (
                      <>
                        <div style={{fontSize:11,color:"#64748b",marginBottom:8}}>
                          {hermesEvidence.totalCount} total · {Object.entries(hermesEvidence.byType).map(([t,c])=>`${t}: ${c}`).join(" · ")}
                        </div>
                        {hermesEvidence.records.map((rec,i) => (
                          <div key={i} style={{padding:"6px 10px",marginBottom:4,background:"#f8fafc",borderRadius:6,border:"1px solid #e2e8f0",fontSize:11}}>
                            <div style={{display:"flex",justifyContent:"space-between",marginBottom:2}}>
                              <span style={{fontWeight:600,color:"#3b82f6"}}>{rec.type || "evidence"}</span>
                              <span style={{color:"#94a3b8",fontSize:10}}>{rec.createdAt || rec.timestamp || ""}</span>
                            </div>
                            <div style={{color:"#475569"}}>{rec.fact || rec.event || rec.quote || JSON.stringify(rec).slice(0,100)}</div>
                          </div>
                        ))}
                      </>
                    ) : <span style={{color:"#94a3b8",fontSize:11}}>No evidence records yet</span>}
                  </div>
                </div>
                {/* Pipeline + Source summary */}
                <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
                  <div className="card crud-card">
                    <div className="admin-card-header"><div><h2>Pipeline Health</h2></div></div>
                    <div style={{padding:12}}>
                      {pipelineDisplayRows.length ? (
                        <div style={{display:"grid",gap:6,maxHeight:260,overflowY:"auto"}}>
                          {pipelineDisplayRows.slice(0,10).map((pipe) => {
                            return (
                              <div key={pipe.key} style={{display:"grid",gridTemplateColumns:"8px minmax(0,1fr) auto",alignItems:"center",gap:10,padding:"6px 10px",background:"#fff",borderRadius:6,border:"1px solid #e2e8f0",fontSize:12}}>
                                <div style={{width:8,height:8,borderRadius:"50%",background:pipe.statusColor}} title={pipe.status} />
                                <div style={{minWidth:0}}>
                                  <div style={{fontWeight:500,whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}} title={pipe.label}>{pipe.label}</div>
                                  <div style={{fontSize:10,color:"#64748b",whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis"}}>{pipe.meta || pipe.status}</div>
                                </div>
                                <div style={{fontSize:10,color:"#64748b",textAlign:"right",whiteSpace:"nowrap"}}>{pipe.lastRunAt || pipe.status}</div>
                              </div>
                            );
                          })}
                        </div>
                      ) : <span style={{color:"#94a3b8",fontSize:11}}>Run pipeline audit to populate</span>}
                    </div>
                  </div>
                  <div className="card crud-card">
                    <div className="admin-card-header"><div><h2>Source Quality</h2></div></div>
                    <div style={{padding:12}}>
                      {hermesSources?.sources?.length ? (
                        <div style={{display:"grid",gap:6,maxHeight:260,overflowY:"auto"}}>
                          {hermesSources.sources.slice(0,8).map((src) => {
                            const qs = src.qualityScore || 0;
                            const barColor = qs<40?"#ef4444":qs<70?"#f59e0b":"#22c55e";
                            return (
                              <div key={src.sourceId} style={{padding:"6px 10px",background:"#fff",borderRadius:6,border:"1px solid #e2e8f0",fontSize:12}}>
                                <div style={{display:"flex",justifyContent:"space-between",marginBottom:4}}>
                                  <span style={{fontWeight:500}}>{String(src.name || src.sourceId).slice(0,30)}</span>
                                  <span style={{fontSize:11,fontWeight:600,color:barColor}}>{qs}</span>
                                </div>
                                <div style={{height:4,background:"#e2e8f0",borderRadius:2,overflow:"hidden"}}>
                                  <div style={{width:`${qs}%`,height:"100%",background:barColor,borderRadius:2}} />
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      ) : <span style={{color:"#94a3b8",fontSize:11}}>Run source quality to populate</span>}
                    </div>
                  </div>
                  {/* MSRP Governance Center */}
                  <div className="card crud-card">
                    <div className="admin-card-header">
                      <div><h2>MSRP Governance Center</h2></div>
                      <select
                        value={hermesMsrpRunId}
                        onChange={(event) => setHermesMsrpRunId(event.target.value)}
                        style={{minWidth:170,fontSize:11,padding:"5px 8px",border:"1px solid #cbd5e1",borderRadius:6,background:"#fff"}}
                      >
                        <option value="">Latest</option>
                        {(hermesMsrpHistory?.runs ?? []).slice(0,30).map((run: HermesMsrpDryrunHistoryRun) => (
                          <option key={run.runId} value={run.runId}>{formatHermesRunLabel(run)}</option>
                        ))}
                      </select>
                    </div>
                    <div style={{padding:12}}>
                      {hermesMsrpProgress ? (() => {
                        const progress = hermesMsrpProgress;
                        const status = progress.status;
                        const countries = progress.countries ?? [];
                        const latestCountries = progress.allCountriesLatest?.length
                          ? progress.allCountriesLatest
                          : countries;
                        if (!countries.length && !latestCountries.length) {
                          return <span style={{color:"#94a3b8",fontSize:11}}>Run dryrun to populate</span>;
                        }
                        const stableCoverage = progress.stableCoverage;
                        const currentCountryCount = status?.expectedCountries?.length ?? countries.length;
                        const currentObservedCount = status?.observedCountries?.length ?? countries.length;
                        const latestCountryCount = stableCoverage?.countryCount ?? latestCountries.length;
                        const latestReadyCountryCount = stableCoverage?.readyCountryCount ?? latestCountries.filter((country) => (country.passPct ?? 0) >= 70).length;
                        const blockers = progress.topBlockingCountries ?? [];
                        const failureReasons = progress.topFailureReasons ?? [];
                        const backlogGroups = progress.sourceRepairBacklog?.groups ?? [];
                        const issueCount = progress.sourceRepairBacklog?.totalIssueCount ?? 0;
                        const recheckCount = progress.sourceRepairBacklog?.transientRegressionCount ?? 0;
                        const sourceRepairIssueCount = progress.sourceRepairBacklog?.sourceRepairIssueCount ?? issueCount;
                        const gateColor = status?.gateStatus === "blocked" ? "#dc2626" : "#16a34a";
                        return (
                          <div style={{display:"grid",gap:10}}>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(4,minmax(0,1fr))",gap:6}}>
                              <div style={{padding:"8px 10px",background:"#fff",border:"1px solid #e2e8f0",borderRadius:6}}>
                                <div style={{fontSize:10,color:"#64748b"}}>Pass</div>
                                <div style={{fontSize:16,fontWeight:700,color:getMsrpProgressColor(status?.overallPassPct ?? 0)}}>{status?.overallPassPct ?? 0}%</div>
                                {stableCoverage?.stablePassRate !== undefined && (
                                  <div style={{fontSize:10,color:"#64748b",whiteSpace:"nowrap"}}>stable {stableCoverage.stablePassRate}%</div>
                                )}
                              </div>
                              <div style={{padding:"8px 10px",background:"#fff",border:"1px solid #e2e8f0",borderRadius:6}}>
                                <div style={{fontSize:10,color:"#64748b"}}>Gate</div>
                                <div style={{fontSize:13,fontWeight:700,color:gateColor}}>{status?.gateStatus ?? "unknown"}</div>
                              </div>
                              <div style={{padding:"8px 10px",background:"#fff",border:"1px solid #e2e8f0",borderRadius:6}}>
                                <div style={{fontSize:10,color:"#64748b"}}>Countries</div>
                                <div style={{fontSize:13,fontWeight:700}}>{latestReadyCountryCount}/{latestCountryCount}</div>
                                <div style={{fontSize:10,color:"#64748b",whiteSpace:"nowrap"}}>current {currentObservedCount}/{currentCountryCount}</div>
                              </div>
                              <div style={{padding:"8px 10px",background:"#fff",border:"1px solid #e2e8f0",borderRadius:6}}>
                                <div style={{fontSize:10,color:"#64748b"}}>Fix Queue</div>
                                <div style={{fontSize:13,fontWeight:700,color:sourceRepairIssueCount > 0 ? "#ea580c" : "#16a34a"}}>{sourceRepairIssueCount}</div>
                                <div style={{fontSize:10,color:"#64748b",whiteSpace:"nowrap"}}>{recheckCount} recheck · {issueCount} total</div>
                              </div>
                            </div>

                            <div>
                              <div style={{fontSize:11,fontWeight:700,marginBottom:5}}>All Country Latest Progress</div>
                              <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(92px,1fr))",gap:6,maxHeight:138,overflowY:"auto"}}>
                                {latestCountries.map((country: HermesMsrpCountryProgressCountry) => {
                                  const pct = country.passPct ?? 0;
                                  const color = getMsrpProgressColor(pct);
                                  return (
                                    <div key={`${country.countryCode}-${country.runId ?? "latest"}`} style={{padding:"6px 8px",background:"#fff",border:"1px solid #e2e8f0",borderRadius:6,fontSize:11}}>
                                      <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>
                                        <span style={{width:6,height:6,borderRadius:"50%",background:color,flexShrink:0}} />
                                        <span style={{fontWeight:700}}>{country.countryCode.toUpperCase()}</span>
                                        <span style={{marginLeft:"auto",color}}>{pct}%</span>
                                      </div>
                                      <div style={{height:4,background:"#f1f5f9",borderRadius:2,overflow:"hidden"}}>
                                        <div style={{height:"100%",width:`${Math.min(pct,100)}%`,background:color,borderRadius:2}} />
                                      </div>
                                      <div style={{marginTop:4,color:"#64748b"}}>{country.pass}/{country.total} pass</div>
                                      {country.runId && <div style={{marginTop:2,color:"#94a3b8",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{country.isLatestRun ? "latest" : "hist"} · {country.runId}</div>}
                                    </div>
                                  );
                                })}
                              </div>
                            </div>

                            {status?.partial && countries.length > 0 && (
                              <div>
                                <div style={{fontSize:11,fontWeight:700,marginBottom:5}}>Current Run Snapshot</div>
                                <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(92px,1fr))",gap:6,maxHeight:104,overflowY:"auto"}}>
                                  {countries.map((country: HermesMsrpCountryProgressCountry) => {
                                    const pct = country.passPct ?? 0;
                                    const color = getMsrpProgressColor(pct);
                                    return (
                                      <div key={`current-${country.countryCode}`} style={{padding:"6px 8px",background:"#fff",border:"1px solid #e2e8f0",borderRadius:6,fontSize:11}}>
                                        <div style={{display:"flex",alignItems:"center",gap:6,marginBottom:4}}>
                                          <span style={{width:6,height:6,borderRadius:"50%",background:color,flexShrink:0}} />
                                          <span style={{fontWeight:700}}>{country.countryCode.toUpperCase()}</span>
                                          <span style={{marginLeft:"auto",color}}>{pct}%</span>
                                        </div>
                                        <div style={{height:4,background:"#f1f5f9",borderRadius:2,overflow:"hidden"}}>
                                          <div style={{height:"100%",width:`${Math.min(pct,100)}%`,background:color,borderRadius:2}} />
                                        </div>
                                        <div style={{marginTop:4,color:"#64748b"}}>{country.pass}/{country.total} pass · {country.status}</div>
                                      </div>
                                    );
                                  })}
                                </div>
                              </div>
                            )}

                            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
                              <div style={{background:"#fff",border:"1px solid #e2e8f0",borderRadius:6,padding:8}}>
                                <div style={{fontSize:11,fontWeight:700,marginBottom:5}}>Blocking Countries</div>
                                {blockers.length ? blockers.slice(0,5).map((item) => (
                                  <div key={item.countryCode} style={{fontSize:11,display:"flex",justifyContent:"space-between",gap:8,marginBottom:4}}>
                                    <span style={{fontWeight:700}}>{item.countryCode.toUpperCase()} {item.passPct}%</span>
                                    <span style={{color:"#64748b",textAlign:"right"}}>{item.reason}</span>
                                  </div>
                                )) : <span style={{fontSize:11,color:"#94a3b8"}}>No blocking countries</span>}
                              </div>
                              <div style={{background:"#fff",border:"1px solid #e2e8f0",borderRadius:6,padding:8}}>
                                <div style={{fontSize:11,fontWeight:700,marginBottom:5}}>Failure Reasons</div>
                                {failureReasons.length ? failureReasons.slice(0,5).map((item) => (
                                  <div key={item.reason} style={{fontSize:11,display:"flex",justifyContent:"space-between",gap:8,marginBottom:4}}>
                                    <span style={{color:"#475569"}}>{item.reason}</span>
                                    <strong>{item.count}</strong>
                                  </div>
                                )) : <span style={{fontSize:11,color:"#94a3b8"}}>No failure reasons</span>}
                              </div>
                            </div>

                            <div style={{background:"#fff",border:"1px solid #e2e8f0",borderRadius:6,padding:8}}>
                              <div style={{fontSize:11,fontWeight:700,marginBottom:5}}>Source Repair Backlog</div>
                              {backlogGroups.length ? (
                                <div style={{fontSize:10,color:"#64748b",display:"grid",gridTemplateColumns:"minmax(0,1.1fr) 54px 42px 48px minmax(0,1.1fr)",gap:8,marginBottom:4}}>
                                  <span>Reason</span>
                                  <span style={{textAlign:"right"}}>Priority</span>
                                  <span style={{textAlign:"right"}}>Repair</span>
                                  <span style={{textAlign:"right"}}>Recheck</span>
                                  <span>Strategy</span>
                                </div>
                              ) : null}
                              {backlogGroups.length ? backlogGroups.slice(0,5).map((group: HermesMsrpSourceRepairBacklogGroup) => (
                                <div key={group.failureReason} style={{fontSize:11,display:"grid",gridTemplateColumns:"minmax(0,1.1fr) 54px 42px 48px minmax(0,1.1fr)",gap:8,alignItems:"center",marginBottom:5}}>
                                  <span style={{fontWeight:600,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={group.failureReason}>{group.failureReason}</span>
                                  <strong
                                    style={{textAlign:"right",color:group.priorityBand === "recheck" ? "#2563eb" : "#475569"}}
                                    title={group.reviewAssist?.reason ?? group.priorityBand}
                                  >
                                    {group.priorityBand ?? "n/a"} {group.priorityScore ?? 0}
                                  </strong>
                                  <strong style={{textAlign:"right",color:(group.sourceRepairIssueCount ?? group.count) > 0 ? "#ea580c" : "#16a34a"}}>{group.sourceRepairIssueCount ?? group.count}</strong>
                                  <strong style={{textAlign:"right",color:(group.transientRegressionCount ?? 0) > 0 ? "#2563eb" : "#94a3b8"}}>{group.transientRegressionCount ?? 0}</strong>
                                  <span style={{color:"#64748b",overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}} title={group.recommendedStrategy}>{group.recommendedStrategy}</span>
                                </div>
                              )) : <span style={{fontSize:11,color:"#94a3b8"}}>No source repair backlog</span>}
                            </div>

                            {(hermesMsrpHistory?.runs ?? []).length > 0 && (
                              <div style={{display:"flex",gap:6,overflowX:"auto",paddingBottom:2}}>
                                {(hermesMsrpHistory?.runs ?? []).slice(0,8).map((run: HermesMsrpDryrunHistoryRun) => (
                                  <button
                                    key={run.runId}
                                    type="button"
                                    className="btn btn-sm btn-secondary"
                                    style={{whiteSpace:"nowrap",fontSize:11,borderColor:hermesMsrpRunId === run.runId ? "#2563eb" : "#cbd5e1"}}
                                    onClick={() => setHermesMsrpRunId(run.runId)}
                                  >
                                    {run.passPct}% · {run.gateStatus}
                                  </button>
                                ))}
                              </div>
                            )}
                          </div>
                        );
                      })() : <span style={{color:"#94a3b8",fontSize:11}}>Run dryrun to populate</span>}
                    </div>
                  </div>
                  <div className="card crud-card" style={{gridColumn:"1 / -1"}}>
                    <MsrpReconciliationPanel />
                  </div>
                  <div className="card crud-card" style={{gridColumn:"1 / -1"}}>
                    <MsrpFinanceObservationsPanel />
                  </div>
                </div>
              </div>
            )}

            {/* ── Cost sub-tab (干了什么 - cost focus) ── */}
            {hermesSubtab === "cost" && (
              <div style={{display:"grid",gap:16}}>
                {/* Cost heatmap */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Cost Heatmap (30d) · Budget: {hermesCostHeatmap?.dailyBudgetCny ?? 20}/day · {hermesCostHeatmap?.monthlyBudgetCny ?? 500}/mo</h2></div></div>
                  <div style={{padding:12}}>
                    {hermesCostHeatmap ? (
                      <>
                        <div style={{display:"flex",gap:16,marginBottom:8}}>
                          <div style={{fontWeight:700,fontSize:14,color:(hermesCostHeatmap.alerts||[]).length>0?"#ef4444":"#22c55e"}}>{(hermesCostHeatmap.totalCny ?? 0).toFixed(2)} CNY total</div>
                          <div style={{fontSize:12,color:"#64748b"}}>Status: <span style={{fontWeight:600,color:hermesCostHeatmap.monthlyStatus==="ok"?"#22c55e":hermesCostHeatmap.monthlyStatus==="warning"?"#f59e0b":"#ef4444"}}>{hermesCostHeatmap.monthlyStatus || "ok"}</span></div>
                        </div>
                        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(22px,1fr))",gap:2}}>
                          {(hermesCostHeatmap.days || []).map((d) => {
                            const cost = d.costCny || 0;
                            const c = cost===0?"#f1f5f9":cost<5?"#bbf7d0":cost<10?"#4ade80":cost<20?"#f59e0b":"#ef4444";
                            return <div key={d.date} title={`${d.date}: ${cost.toFixed(1)} CNY`} style={{aspectRatio:"1",background:c,borderRadius:2,border:d.overDailyBudget?"2px solid #ef4444":"none"}} />;
                          })}
                        </div>
                        {/* Source breakdown */}
                        {(hermesCostHeatmap as Record<string,unknown>).bySourceCny && Object.keys((hermesCostHeatmap as Record<string,unknown>).bySourceCny as Record<string,unknown>).length > 0 && (
                          <div style={{marginTop:12}}>
                            <div style={{fontSize:11,fontWeight:600,marginBottom:4}}>By Source</div>
                            <div style={{display:"flex",flexWrap:"wrap",gap:8}}>
                              {Object.entries((hermesCostHeatmap as Record<string,unknown>).bySourceCny as Record<string,unknown>).map(([source,cost]) => (
                                <span key={source} style={{fontSize:10,fontWeight:600,
                                  background:source==="country_copilot"?"#fef3c7":source==="hermes"?"#dbeafe":"#f1f5f9",
                                  borderRadius:4,padding:"4px 10px"}}>
                                  {source==="country_copilot"?"Country Copilot":source==="hermes"?"Hermes":source}: {Number(cost).toFixed(2)} CNY
                                </span>
                              ))}
                            </div>
                          </div>
                        )}
                        {/* Model breakdown */}
                        {hermesCostHeatmap.byModelCny && Object.keys(hermesCostHeatmap.byModelCny).length > 0 && (
                          <div style={{marginTop:12}}>
                            <div style={{fontSize:11,fontWeight:600,marginBottom:4}}>By Model</div>
                            <div style={{display:"flex",flexWrap:"wrap",gap:8}}>
                              {Object.entries(hermesCostHeatmap.byModelCny).map(([model,cost]) => (
                                <span key={model} style={{fontSize:10,background:model.includes("pro")?"#ede9fe":"#dbeafe",borderRadius:4,padding:"3px 8px"}}>{model}: {Number(cost).toFixed(2)} CNY</span>
                              ))}
                            </div>
                          </div>
                        )}
                        {hermesCostHeatmap.alerts && hermesCostHeatmap.alerts.length > 0 && (
                          <div style={{marginTop:8,padding:"6px 10px",background:"#fef2f2",borderRadius:6,fontSize:11,color:"#ef4444"}}>
                            {hermesCostHeatmap.alerts.map((a,i)=><div key={i}>{a}</div>)}
                          </div>
                        )}
                      </>
                    ) : <span style={{color:"#94a3b8",fontSize:11}}>No cost data yet</span>}
                  </div>
                </div>
                {/* Cost by model detail */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Cost Detail</h2></div></div>
                  <div style={{padding:16}}>
                    {hermesCost && (hermesCost as Record<string,unknown>).byModel ? (
                      <div style={{display:"grid",gap:12}}>
                        {Object.entries((hermesCost as Record<string,unknown>).byModel as Record<string,unknown>).map(([model,data]) => {
                          const d = data as Record<string,unknown>;
                          const cost = (d.estimatedCostCny as number) || 0;
                          const maxCost = Math.max(...Object.values((hermesCost as Record<string,unknown>).byModel as Record<string,unknown>).map((v:unknown) => ((v as Record<string,unknown>).estimatedCostCny as number) || 0), 1);
                          const pct = Math.round(cost / Math.max(maxCost, 0.001) * 100);
                          return (
                            <div key={model}>
                              <div style={{display:"flex",justifyContent:"space-between",marginBottom:4,fontSize:13}}>
                                <span style={{fontWeight:600}}>{model}</span>
                                <span>{cost.toFixed(4)} CNY · {String(d.records || 0)} calls</span>
                              </div>
                              <div style={{height:8,background:"#e2e8f0",borderRadius:4,overflow:"hidden"}}>
                                <div style={{width:`${pct}%`,height:"100%",background:model.includes("pro")?"#8b5cf6":"#3b82f6",borderRadius:4}} />
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    ) : <span style={{color:"#94a3b8",fontSize:11}}>Run cost report to populate</span>}
                  </div>
                </div>
              </div>
            )}

            {/* ── Roadmap sub-tab (要干什么) ── */}
            {hermesSubtab === "roadmap" && (
              <div style={{display:"grid",gap:16}}>
                {/* Proposals pipeline */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Proposals ({hermesOverview?.proposals?.total || 0} total · {hermesOverview?.proposals?.implemented || 0} done)</h2></div></div>
                  <div style={{padding:12}}>
                    <div style={{height:6,background:"#e2e8f0",borderRadius:3,overflow:"hidden",marginBottom:10}}>
                      <div style={{width:`${(((hermesOverview?.proposals?.implemented || 0) / Math.max((hermesOverview?.proposals?.total || 1), 1)) * 100)}%`,height:"100%",background:"#22c55e",borderRadius:3}} />
                    </div>
                    <div style={{maxHeight:300,overflowY:"auto"}}>
                      {hermesProposals.map((p: Record<string,unknown>) => {
                        const st = String(p.status || "");
                        const c = st === "implemented" ? "#22c55e" : st === "pending_review" ? "#f59e0b" : "#94a3b8";
                        return (
                          <div key={String(p.proposalId)} style={{display:"flex",alignItems:"center",gap:8,padding:"5px 0",fontSize:12}}>
                            <div style={{width:8,height:8,borderRadius:"50%",background:c,flexShrink:0}} />
                            <span style={{flex:1}}>{String(p.title || "").slice(0,70)}</span>
                            <span style={{color:c,fontWeight:600,fontSize:10}}>{st}</span>
                          </div>
                        );
                      })}
                      {hermesProposals.length === 0 && <span style={{color:"#94a3b8",fontSize:11}}>No proposals</span>}
                    </div>
                  </div>
                </div>
                {/* Governance Gaps */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Governance Gaps ({hermesGaps.length} total · {hermesGaps.filter(g=>g.status==="open").length} open)</h2></div></div>
                  <div style={{padding:12,maxHeight:400,overflowY:"auto"}}>
                    {hermesGaps.length > 0 ? (
                      <div style={{display:"grid",gap:6}}>
                        {hermesGaps.map((gap) => {
                          const sev = gap.severity || "low";
                          const sevColor = sev === "high" ? "#ef4444" : sev === "medium" ? "#f59e0b" : "#22c55e";
                          const sevBg = sev === "high" ? "#fef2f2" : sev === "medium" ? "#fffbeb" : "#f0fdf4";
                          const stColor = gap.status === "resolved" ? "#22c55e" : gap.status === "in_progress" ? "#3b82f6" : "#94a3b8";
                          return (
                            <div key={gap.gapId} style={{display:"flex",alignItems:"flex-start",gap:10,padding:"8px 12px",background:"#fff",borderRadius:6,border:"1px solid #e2e8f0",fontSize:12}}>
                              <span style={{padding:"1px 6px",borderRadius:4,fontSize:10,fontWeight:600,background:sevBg,color:sevColor,flexShrink:0}}>{sev}</span>
                              <div style={{flex:1}}>
                                <div style={{fontWeight:500}}>{gap.title || gap.name || gap.gapId}</div>
                                <div style={{fontSize:10,color:"#64748b"}}>{gap.category} · {gap.recommendedAction || gap.notes || ""}</div>
                              </div>
                              <span style={{fontSize:10,color:stColor,fontWeight:600,flexShrink:0}}>{gap.status}</span>
                            </div>
                          );
                        })}
                      </div>
                    ) : <span style={{color:"#94a3b8",fontSize:11}}>No gaps loaded</span>}
                  </div>
                </div>
                {/* Planned Features (from kanban) */}
                <div className="card crud-card">
                  <div className="admin-card-header"><div><h2>Planned & Beta Features</h2></div></div>
                  <div style={{padding:12}}>
                    {featureKanban ? (
                      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
                        {(["planned","beta"] as const).map((col) => {
                          const features = featureKanban.columns[col]?.features || [];
                          return (
                            <div key={col}>
                              <div style={{fontWeight:600,fontSize:12,marginBottom:6,color:col==="planned"?"#f59e0b":"#3b82f6"}}>{col === "planned" ? "Planned" : "Beta"} ({features.length})</div>
                              {features.slice(0,6).map((f) => (
                                <div key={f.featureId} style={{padding:"5px 8px",marginBottom:4,background:"#f8fafc",borderRadius:4,fontSize:11,borderLeft:`3px solid ${f.color || "#94a3b8"}`}}>
                                  <div style={{fontWeight:500}}>{f.name}</div>
                                  <div style={{color:"#64748b",fontSize:10}}>{f.riskLevel} risk · {(f.dependencies || []).length} deps</div>
                                </div>
                              ))}
                              {features.length === 0 && <div style={{color:"#94a3b8",fontSize:10}}>None</div>}
                            </div>
                          );
                        })}
                      </div>
                    ) : <span style={{color:"#94a3b8",fontSize:11}}>Loading...</span>}
                  </div>
                </div>
              </div>
            )}

            {/* ── Diagrams sub-tab (Doc visualization) ── */}
            {hermesSubtab === "diagrams" && (
              <div>
                {/* Filter bar */}
                <div style={{display:"flex",gap:8,marginBottom:12,alignItems:"center"}}>
                  <select value={diagramCategoryFilter} onChange={(e) => setDiagramCategoryFilter(e.target.value)} style={{fontSize:12,padding:"4px 8px",borderRadius:4,border:"1px solid #e2e8f0"}}>
                    <option value="all">All categories</option>
                    {[...new Map(hermesDiagrams.map((d) => [d.category ?? "other", d.categoryLabel ?? "Other"])).entries()]
                      .map(([category, label]) => (<option key={category} value={category}>{label}</option>))}
                  </select>
                  <select value={diagramFileFilter} onChange={(e) => setDiagramFileFilter(e.target.value)} style={{fontSize:12,padding:"4px 8px",borderRadius:4,border:"1px solid #e2e8f0"}}>
                    <option value="all">All files ({hermesDiagrams.length} diagrams)</option>
                    {[...new Set(hermesDiagrams.map(d=>d.file))].map(f=>(<option key={f} value={f}>{f.split("/").pop()}</option>))}
                  </select>
                  <input type="text" placeholder="Search diagrams..." value={diagramSearch} onChange={(e) => setDiagramSearch(e.target.value)} style={{flex:1,fontSize:12,padding:"4px 8px",borderRadius:4,border:"1px solid #e2e8f0"}} />
                </div>
                {hermesDiagrams.length === 0 ? (
                  <div className="card crud-card" style={{padding:24,textAlign:"center"}}>
                    {hermesTabError ? (
                      <span style={{color:"#ef4444"}}>Failed to load diagrams: {hermesTabError}</span>
                    ) : (
                      <span style={{color:"#94a3b8"}}>Scanning markdown files for diagrams...</span>
                    )}
                  </div>
                ) : (
                  /* Diagram gallery */
                  (() => {
                    const filtered = hermesDiagrams.filter(d => {
                      if (diagramCategoryFilter !== "all" && (d.category ?? "other") !== diagramCategoryFilter) return false;
                      if (diagramFileFilter !== "all" && d.file !== diagramFileFilter) return false;
                      if (diagramSearch && !d.title.toLowerCase().includes(diagramSearch.toLowerCase()) && !d.file.toLowerCase().includes(diagramSearch.toLowerCase()) && !(d.categoryLabel ?? "").toLowerCase().includes(diagramSearch.toLowerCase())) return false;
                      return true;
                    });
                    if (filtered.length === 0) return <div className="card crud-card" style={{padding:24,textAlign:"center",color:"#94a3b8"}}>No diagrams match filter</div>;
                    const grouped = filtered.reduce<Record<string, HermesMermaidBlockType[]>>((acc, block) => {
                      const group = block.categoryLabel ?? "Other";
                      acc[group] = acc[group] ?? [];
                      acc[group].push(block);
                      return acc;
                    }, {});
                    return (
                      <div style={{display:"grid",gap:18}}>
                        {Object.entries(grouped).map(([group, blocks]) => (
                          <div key={group}>
                            <div style={{fontSize:11,fontWeight:700,color:"#475569",textTransform:"uppercase",letterSpacing:0,marginBottom:8}}>{group}</div>
                            <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(420px,1fr))",gap:16}}>
                              {blocks.map((block) => (
                                <div key={`${block.file}-${block.diagramIndex}`} className="hermes-diagram-card">
                                  <div className="hermes-diagram-card-header">
                                    <div>
                                      <span style={{fontWeight:600}}>{block.title || "Diagram " + (block.diagramIndex + 1)}</span>
                                      <span style={{marginLeft:8,color:"#94a3b8",fontSize:10}}>{block.file.split("/").pop()}</span>
                                    </div>
                                    <div style={{display:"flex",gap:4}}>
                                      <button className="btn btn-sm btn-ghost" style={{fontSize:10}} onClick={() => setDiagramModal(block)}>Expand</button>
                                    </div>
                                  </div>
                                  <div className="hermes-diagram-card-body">
                                    <HermesMermaidBlock block={block} maxHeight={400} />
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    );
                  })()
                )}
                {/* Fullscreen modal */}
                {diagramModal && (
                  <div className="hermes-modal-overlay" onClick={() => setDiagramModal(null)}>
                    <div className="hermes-modal-content" onClick={(e) => e.stopPropagation()} style={{width:"90vw",maxHeight:"90vh"}}>
                      <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
                        <div>
                          <strong>{diagramModal.title || "Diagram " + (diagramModal.diagramIndex + 1)}</strong>
                          <div style={{fontSize:11,color:"#64748b"}}>{diagramModal.file} · {diagramModal.categoryLabel ?? "Other"} · type: {diagramModal.type}</div>
                        </div>
                        <button className="btn btn-sm btn-ghost" onClick={() => setDiagramModal(null)}>Close</button>
                      </div>
                      <HermesMermaidBlock block={diagramModal} maxHeight={700} />
                      <details style={{marginTop:12}}>
                        <summary style={{fontSize:11,color:"#64748b",cursor:"pointer"}}>Raw source</summary>
                        <pre style={{marginTop:8,background:"#1e293b",color:"#e2e8f0",padding:12,borderRadius:6,fontSize:11,whiteSpace:"pre-wrap",maxHeight:200,overflow:"auto"}}>{diagramModal.raw}</pre>
                      </details>
                    </div>
                  </div>
                )}
              </div>
            )}

            <div className="card crud-card" style={{marginTop:16,padding:12}}>
              <button
                type="button"
                className="btn btn-sm btn-ghost"
                style={{width:"100%",justifyContent:"space-between",fontSize:12,fontWeight:700}}
                onClick={() => setFullDesignDocOpen((value) => !value)}
              >
                <span>Hermes Full Design Document</span>
                <span>{fullDesignDocOpen ? "Collapse" : "Expand"}</span>
              </button>
              {fullDesignDocOpen && (
                <div style={{marginTop:10,borderTop:"1px solid #e2e8f0",paddingTop:10}}>
                  {hermesDesignDocError ? (
                    <div style={{fontSize:12,color:"#dc2626"}}>{hermesDesignDocError}</div>
                  ) : !hermesDesignDoc ? (
                    <div style={{fontSize:12,color:"#64748b"}}>Loading document...</div>
                  ) : !hermesDesignDoc.exists ? (
                    <div style={{fontSize:12,color:"#64748b"}}>Document not found: {hermesDesignDoc.path}</div>
                  ) : (
                    <>
                      <div style={{fontSize:11,color:"#64748b",marginBottom:8}}>{hermesDesignDoc.path}{hermesDesignDoc.updatedAt ? ` · ${formatDataManagementTimestamp(hermesDesignDoc.updatedAt)}` : ""}</div>
                      <pre style={{maxHeight:520,overflow:"auto",whiteSpace:"pre-wrap",fontSize:12,lineHeight:1.55,color:"#1e293b",background:"#f8fafc",border:"1px solid #e2e8f0",borderRadius:6,padding:12}}>
                        {hermesDesignDoc.content}
                      </pre>
                    </>
                  )}
                </div>
              )}
            </div>
          </>
        )
      ) : null}

      {subpage !== "hermes" && loading && !overview ? (
        <LoadingSurface
          mode="overlay"
          label="正在读取数据总览"
          detail="拉取文件清单、数据库表摘要与活跃度数据"
          kicker="Data Ops"
        />
      ) : null}

      {subpage !== "hermes" && subpage !== "admin" && overview ? (
        <>
          {/* Hermes summary strip — Overview only */}
          {subpage === "overview" && (
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Hermes Governance Snapshot <span style={{fontSize:12,fontWeight:400,color:"#64748b"}}>— full details in Hermes tab</span></h2></div></div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 12, padding: 16 }}>
                <div className="metric-chip"><span>Pipelines</span><strong style={{color:"#3b82f6"}}>{hermesOverview?.registries?.pipeline ?? "?"}</strong></div>
                <div className="metric-chip"><span>Sources</span><strong style={{color:"#3b82f6"}}>{hermesOverview?.registries?.source ?? "?"}</strong></div>
                <div className="metric-chip"><span>Features</span><strong>{hermesOverview?.registries?.feature ?? "?"}</strong></div>
                <div className="metric-chip"><span>Proposals Done</span><strong style={{color:"#22c55e"}}>{hermesOverview?.proposals?.implemented ?? 0}/{hermesOverview?.proposals?.total ?? 0}</strong></div>
                <div className="metric-chip"><span>Gaps Open</span><strong style={{color: (hermesOverview?.gaps?.open ?? 0) > 0 ? "#ef4444" : "#22c55e"}}>{hermesOverview?.gaps?.open ?? 0}</strong></div>
                <div className="metric-chip"><span>Cost</span><strong style={{color: hermesDaily?.costStatus === "over_daily" ? "#ef4444" : "#22c55e"}}>{hermesDaily?.costCny?.toFixed(2) ?? "0"} CNY</strong></div>
              </div>
            </div>
          )}

          <div className="card crud-card">
            <div className="admin-card-header">
              <div>
                <h2>入库活跃度</h2>
                <p>
                  最近 {overview.activity.days.length} 天的写入/更新热力图，
                  便于快速判断近期是不是有数据真正进来。
                </p>
              </div>
              <span className={`badge ${getDataManagementStatusBadgeClass(overview.database.connected ? "ready" : "warning")}`}>
                DB {overview.database.connected ? "connected" : "offline"}
              </span>
            </div>
            <div className="data-management-activity-shell">
              <div className="data-management-activity-grid" role="img" aria-label="数据入库活跃度热力图">
                {activityColumns.map((column, columnIndex) => (
                  <div key={`week-${columnIndex}`} className="data-management-activity-column">
                    {column.map((day) => (
                      <div
                        key={day.date}
                        className={`data-management-activity-cell level-${day.level}`}
                        title={`${day.date} · ${day.count} events`}
                      />
                    ))}
                  </div>
                ))}
              </div>
              <div className="data-management-activity-meta">
                <div className="data-management-source-chips">
                  {overview.activity.sourceCounts.map((item) => (
                    <span key={item.label} className="data-management-source-chip">
                      {item.label}: {item.count.toLocaleString()}
                    </span>
                  ))}
                </div>
                <p>
                  Total {overview.activity.totalCount.toLocaleString()} events ·
                  Peak {overview.activity.maxCount.toLocaleString()} / day ·
                  {overview.activity.rangeStart} to {overview.activity.rangeEnd}
                </p>
              </div>
            </div>
          </div>
        </>
      ) : null}

      {/* ═══════════ ADMIN TAB: domains, CRUD, Airflow, files, VOC sync ═══════════ */}
      {subpage === "admin" && overview ? (
        <>
          <div className="data-management-domain-grid">
            {overview.domains.map((domain) => (
              <article key={domain.key} className="card crud-card data-management-domain-card">
                <div className="data-management-card-header">
                  <div>
                    <strong>{domain.label}</strong>
                    <p>{domain.summary}</p>
                  </div>
                  <span className={`badge ${getDataManagementStatusBadgeClass(domain.status)}`}>
                    {domain.status}
                  </span>
                </div>
                <div className="data-management-metric-grid">
                  {domain.metrics.map((metric, index) => (
                    <div key={`${domain.key}-metric-${index}`} className="data-management-metric">
                      <span>{metric.label}</span>
                      <strong>{formatMetricValue(metric.value)}</strong>
                    </div>
                  ))}
                </div>
                <div className="data-management-domain-footer">
                  <time>{formatDataManagementTimestamp(domain.updatedAt)}</time>
                  <span>{domain.storage}</span>
                </div>
                {domain.key === "airflow" && airflowStatus ? (
                  <div className="data-management-airflow-panel">
                    <p className="data-management-airflow-detail">
                      {airflowStatus.detail}
                    </p>
                    <div className="data-management-airflow-meta">
                      <span>Mode: {airflowStatus.mode}</span>
                      <span>
                        Web UI: {airflowStatus.actions.canOpenUi ? airflowStatus.uiUrl : "未运行"}
                      </span>
                    </div>
                    {airflowError ? <div className="alert alert-error">{airflowError}</div> : null}
                    {airflowNotice ? <div className="alert alert-success">{airflowNotice}</div> : null}
                    <div className="data-management-inline-actions">
                      <button
                        type="button"
                        className="btn btn-sm btn-primary"
                        onClick={() => void handleAirflowAction("start")}
                        disabled={!airflowStatus.actions.canStart || airflowBusyAction !== null}
                      >
                        {airflowBusyAction === "start" ? "启动中…" : "启动 Airflow"}
                      </button>
                      <button
                        type="button"
                        className="btn btn-sm btn-secondary"
                        onClick={() => void handleAirflowAction("stop")}
                        disabled={!airflowStatus.actions.canStop || airflowBusyAction !== null}
                      >
                        {airflowBusyAction === "stop" ? "暂停中…" : "暂停 Airflow"}
                      </button>
                      <button
                        type="button"
                        className="btn btn-sm btn-secondary"
                        onClick={() => handleOpenAirflowUi(airflowStatus)}
                        disabled={!airflowStatus.actions.canOpenUi || airflowBusyAction !== null}
                      >
                        打开 Airflow UI
                      </button>
                    </div>
                  </div>
                ) : null}
                {domain.key === "voc" ? (
                  <div className="data-management-airflow-panel">
                    <p className="data-management-airflow-detail">
                      将 `04_Processed_data/voc/**/raw/*.json` 同步到 PostgreSQL staging，
                      供后续统计、过滤与 Assistant 消费。
                    </p>
                    {vocSyncError ? <div className="alert alert-error">{vocSyncError}</div> : null}
                    {vocSyncNotice ? <div className="alert alert-success">{vocSyncNotice}</div> : null}
                    <div className="data-management-inline-actions">
                      <button
                        type="button"
                        className="btn btn-sm btn-primary"
                        onClick={() => void handleVocSync()}
                        disabled={!overview.database.connected || vocSyncBusy}
                      >
                        {vocSyncBusy ? "同步中…" : "同步 VOC 到 PostgreSQL"}
                      </button>
                    </div>

                    {/* VOC source traceability — clickable source cards */}
                    <div style={{marginTop:16}}>
                      <strong style={{fontSize:13,display:"block",marginBottom:8}}>VOC Sources — click to trace</strong>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(240px,1fr))",gap:8}}>
                        {((hermesSources as Record<string,unknown>)?.sources as unknown[])
                          ?.filter((s: unknown) => String((s as Record<string,unknown>).sourceType || "").includes("voc") || String((s as Record<string,unknown>).sourceId || "").includes("voc"))
                          .map((s: unknown) => {
                            const src = s as Record<string,unknown>;
                            const status = String(src.status || "?");
                            const color = status === "degraded" ? "#ef4444" : status === "watch" ? "#f59e0b" : "#22c55e";
                            return (
                              <div key={String(src.sourceId)} style={{padding:"10px 12px",borderRadius:8,border:"1px solid #e2e8f0",cursor:"pointer",borderLeft:`3px solid ${color}`}}
                                onClick={() => {
                                  setSelectedSource(src);
                                  setSourceDetailOpen(true);
                                  api.hermesSourceDetail(String(src.sourceId || "")).then(setSourceDetail).catch(() => setSourceDetail(null));
                                }}>
                                <div style={{fontWeight:600,fontSize:13,marginBottom:4}}>{String(src.name).slice(0,40)}</div>
                                <div style={{display:"flex",gap:8,fontSize:11,color:"#64748b"}}>
                                  <span>Score: <strong style={{color}}>{String(src.qualityScore)}</strong></span>
                                  <span>{String(src.country || "").slice(0,25)}</span>
                                  <span style={{color}}>{status}</span>
                                </div>
                              </div>
                            );
                          })}
                        {!((hermesSources as Record<string,unknown>)?.sources as unknown[])?.filter((s: unknown) => String((s as Record<string,unknown>).sourceType || "").includes("voc")).length && (
                          <span style={{fontSize:11,color:"#94a3b8"}}>No VOC sources in registry. Run hermes_source_quality.py.</span>
                        )}
                      </div>
                    </div>

                    {/* Source Detail Panel (shared with Hermes tab logic) */}
                    {sourceDetailOpen && selectedSource ? (
                      <div style={{marginTop:12,background:"#f8fafc",borderRadius:8,padding:14,border:"1px solid #e2e8f0"}}>
                        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:8}}>
                          <strong>{String(selectedSource.name || selectedSource.sourceId)}</strong>
                          <button type="button" className="btn btn-sm btn-ghost" onClick={() => {setSourceDetailOpen(false);setSourceDetail(null);}}>Close</button>
                        </div>
                        {sourceDetail ? (
                          <div style={{fontSize:12}}>
                            <div><strong>Path:</strong> <span style={{fontFamily:"monospace",fontSize:11}}>{String(selectedSource.path || "?")}</span></div>
                            <div><strong>Evidence:</strong> {(sourceDetail.linkedEvidenceCount as number) || 0} records | <strong>Pipelines:</strong> {(sourceDetail.linkedPipelines as unknown[])?.length || 0}</div>
                            {(selectedSource.knownIssues as unknown[])?.length > 0 && (
                              <div style={{color:"#ef4444",marginTop:4}}>{(selectedSource.knownIssues as unknown[]).map((issue: unknown, i: number) => <div key={i}>- {String(issue).slice(0,120)}</div>)}</div>
                            )}
                          </div>
                        ) : <span style={{color:"#64748b"}}>Loading...</span>}
                      </div>
                    ) : null}
                  </div>
                ) : null}
                {renderDomainRecentItems(
                  domain,
                  Boolean(expandedDomains[domain.key]),
                  toggleDomainRecentItems,
                )}
              </article>
            ))}
          </div>

          <div className="card crud-card">
            <div className="admin-card-header">
              <div>
                <h2>VOC 观察台</h2>
                <p>
                  在数据管理页直接查看 VOC 抓取产物、国家切换、source runs、PostgreSQL staging 状态，以及对应设计文档路径。
                </p>
              </div>
              <span className={`badge ${getDataManagementStatusBadgeClass(selectedVocCountryStatus)}`}>
                {vocOverview?.selectedCountryCode || "VOC"}
              </span>
            </div>

            {vocOverviewError ? <div className="alert alert-error">{vocOverviewError}</div> : null}
            {vocSyncError ? <div className="alert alert-error">{vocSyncError}</div> : null}
            {vocSyncNotice ? <div className="alert alert-success">{vocSyncNotice}</div> : null}

            <div className="crud-toolbar-grid">
              <div className="filter-group">
                <label>Country</label>
                <select
                  value={vocCountry}
                  onChange={(event) => {
                    const nextCountry = event.target.value;
                    setVocCountry(nextCountry);
                    void loadVocOverview(nextCountry, { silent: true });
                  }}
                  disabled={vocOverviewLoading && !vocOverview}
                >
                  {vocOverview?.availableCountries.map((item) => (
                    <option key={item.code} value={item.code}>
                      {item.code} · {item.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className="filter-group">
                <label>Selected deck</label>
                <input value={vocOverview?.selectedCountryLabel ?? "-"} disabled />
              </div>
              <div className="data-management-inline-actions">
                <button
                  type="button"
                  className="btn btn-sm btn-secondary"
                  onClick={() => void loadVocOverview(vocCountry || undefined, { silent: true })}
                  disabled={vocOverviewLoading}
                >
                  {vocOverviewLoading ? "刷新中…" : "刷新 VOC"}
                </button>
                <button
                  type="button"
                  className="btn btn-sm btn-primary"
                  onClick={() => void handleVocSync()}
                  disabled={!overview.database.connected || vocSyncBusy}
                >
                  {vocSyncBusy ? "同步中…" : "同步 VOC 到 PostgreSQL"}
                </button>
                <Link className="btn btn-sm btn-ghost" to="/customer-insights">
                  打开 Customer Insights
                </Link>
                <Link className="btn btn-sm btn-ghost" to="/customer-hev">
                  打开 Nordic HEV
                </Link>
              </div>
            </div>

            {vocOverviewLoading && !vocOverview ? (
              <LoadingSurface
                mode="inline"
                label="正在读取 VOC 观察台"
                detail="汇总 raw / enriched / deck / staging 状态"
                kicker="VOC"
              />
            ) : null}

            {vocOverview ? (
              <>
                <div className="data-management-card-header">
                  <div>
                    <strong>全局抓取概览</strong>
                    <p>当前 VOC 目录下可见国家与已生成 deck 的整体覆盖。</p>
                  </div>
                  <time>{formatDataManagementTimestamp(vocOverview.generatedAt)}</time>
                </div>
                <div className="data-management-metric-grid">
                  {vocOverview.overallMetrics.map((metric, index) => (
                    <div key={`voc-overall-${index}`} className="data-management-metric">
                      <span>{metric.label}</span>
                      <strong>{formatMetricValue(metric.value)}</strong>
                    </div>
                  ))}
                </div>

                <div className="data-management-card-header">
                  <div>
                    <strong>{vocOverview.selectedCountryCode} 国家明细</strong>
                    <p>聚焦当前国家的 raw 抓取、enrichment、deck 和 staging 入库状态。</p>
                  </div>
                  <span>{vocOverview.selectedCountryLabel}</span>
                </div>
                <div className="data-management-metric-grid">
                  {vocOverview.countryMetrics.map((metric, index) => (
                    <div key={`voc-country-${index}`} className="data-management-metric">
                      <span>{metric.label}</span>
                      <strong>{formatMetricValue(metric.value)}</strong>
                    </div>
                  ))}
                  <div className="data-management-metric">
                    <span>PG source runs</span>
                    <strong>{formatDataManagementNumber(vocOverview.staging.sourceRunCount)}</strong>
                  </div>
                  <div className="data-management-metric">
                    <span>PG documents</span>
                    <strong>{formatDataManagementNumber(vocOverview.staging.documentCount)}</strong>
                  </div>
                  <div className="data-management-metric">
                    <span>PG publish-ready</span>
                    <strong>{formatDataManagementNumber(vocOverview.staging.publishReadyCount)}</strong>
                  </div>
                  <div className="data-management-metric">
                    <span>PG latest sync</span>
                    <strong>{formatDataManagementTimestamp(vocOverview.staging.latestCollectedAt)}</strong>
                  </div>
                </div>

                <div className="data-management-domain-grid">
                  <article className="card crud-card data-management-domain-card">
                    <div className="data-management-card-header">
                      <div>
                        <strong>Artifacts</strong>
                        <p>当前国家对应的 raw 目录、enriched 输出和 deck 文件。</p>
                      </div>
                    </div>
                    <div className="data-management-recent-list">
                      {vocOverview.artifacts.map((item) => (
                        <article key={item.key} className="data-management-recent-item">
                          <div>
                            <strong>{item.label}</strong>
                            <span className="data-management-path-cell">{item.path}</span>
                          </div>
                          <div className="data-management-card-meta">
                            <span>{item.exists ? (item.isDir ? `${formatDataManagementNumber(item.fileCount)} files` : formatDataManagementBytes(item.sizeBytes)) : "missing"}</span>
                            <time>{formatDataManagementTimestamp(item.updatedAt)}</time>
                          </div>
                        </article>
                      ))}
                    </div>
                  </article>

                  <article className="card crud-card data-management-domain-card">
                    <div className="data-management-card-header">
                      <div>
                        <strong>Country switchboard</strong>
                        <p>每个国家的 raw/doc/deck 就绪情况，帮助快速切换观察。</p>
                      </div>
                    </div>
                    <div className="data-management-recent-list">
                      {vocOverview.availableCountries.map((item) => (
                        <article key={item.code} className="data-management-recent-item">
                          <div>
                            <strong>{item.code} · {item.label}</strong>
                            <span>
                              {formatDataManagementNumber(item.rawSourceCount)} sources · {formatDataManagementNumber(item.rawDocumentCount)} docs · {formatDataManagementNumber(item.signalObservationCount)} observations
                            </span>
                          </div>
                          <div className="data-management-card-meta">
                            <span className={`badge ${getDataManagementStatusBadgeClass(item.status)}`}>
                              {item.deckReady ? "deck ready" : item.status}
                            </span>
                            <time>{formatDataManagementTimestamp(item.updatedAt)}</time>
                          </div>
                        </article>
                      ))}
                    </div>
                  </article>
                </div>

                <div className="data-management-domain-grid">
                  <article className="card crud-card data-management-domain-card">
                    <div className="data-management-card-header">
                      <div>
                        <strong>Source runs</strong>
                        <p>当前国家 raw 采集批次与 text extraction 方法分布。</p>
                      </div>
                    </div>
                    <div className="data-management-recent-list">
                      {vocOverview.sourceRuns.length > 0 ? vocOverview.sourceRuns.map((item) => (
                        <article key={`${item.sourceCode}-${item.path}`} className="data-management-recent-item">
                          <div>
                            <strong>{item.siteName}</strong>
                            <span>
                              {item.sourceCode} · {item.siteType}
                              {item.language ? ` · ${item.language}` : ""}
                              {item.publishTier ? ` · ${item.publishTier}` : ""}
                            </span>
                            <span className="data-management-path-cell">{item.path}</span>
                          </div>
                          <div className="data-management-card-meta">
                            <span>
                              {formatDataManagementNumber(item.documentCount)} docs / {formatDataManagementNumber(item.publishReadyCount)} ready / {formatDataManagementNumber(item.errorCount)} errors
                            </span>
                            <time>{formatDataManagementTimestamp(item.updatedAt)}</time>
                          </div>
                          {item.textExtractionMethods.length > 0 ? (
                            <div className="data-management-source-chips">
                              {item.textExtractionMethods.map((method) => (
                                <span key={`${item.sourceCode}-${method}`} className="data-management-source-chip">
                                  {method}
                                </span>
                              ))}
                            </div>
                          ) : null}
                        </article>
                      )) : <div className="crud-empty-state">当前国家还没有 raw source run。</div>}
                    </div>
                  </article>

                  <article className="card crud-card data-management-domain-card">
                    <div className="data-management-card-header">
                      <div>
                        <strong>Deck signals</strong>
                        <p>如果当前国家已经生成 deck，这里直接显示 observed / inferred 结构和高频信号。</p>
                      </div>
                    </div>
                    <div className="data-management-activity-meta">
                      <p>Observed sections</p>
                      <div className="data-management-source-chips">
                        {vocOverview.observedSections.length > 0 ? vocOverview.observedSections.map((item) => (
                          <span key={item} className="data-management-source-chip">{item}</span>
                        )) : <span className="data-management-source-chip">暂无</span>}
                      </div>
                      <p>Inferred sections</p>
                      <div className="data-management-source-chips">
                        {vocOverview.inferredSections.length > 0 ? vocOverview.inferredSections.map((item) => (
                          <span key={item} className="data-management-source-chip">{item}</span>
                        )) : <span className="data-management-source-chip">暂无</span>}
                      </div>
                      <p>Top pain points</p>
                      <div className="data-management-source-chips">
                        {vocOverview.topPainPoints.length > 0 ? vocOverview.topPainPoints.map((item) => (
                          <span key={`pain-${item.label}`} className="data-management-source-chip">
                            {item.label} · {formatSharePct(item.sharePct)}
                          </span>
                        )) : <span className="data-management-source-chip">暂无</span>}
                      </div>
                      <p>Top product signals</p>
                      <div className="data-management-source-chips">
                        {vocOverview.topProductSignals.length > 0 ? vocOverview.topProductSignals.map((item) => (
                          <span key={`signal-${item.label}`} className="data-management-source-chip">
                            {item.label} · {formatSharePct(item.sharePct)}
                          </span>
                        )) : <span className="data-management-source-chip">暂无</span>}
                      </div>
                    </div>
                  </article>
                </div>

                <div className="data-management-domain-grid">
                  <article className="card crud-card data-management-domain-card">
                    <div className="data-management-card-header">
                      <div>
                        <strong>Evidence cards</strong>
                        <p>从 country deck 中抽样展示当前国家的代表性证据卡片。</p>
                      </div>
                    </div>
                    <div className="data-management-recent-list">
                      {vocOverview.evidenceCards.length > 0 ? vocOverview.evidenceCards.map((item) => (
                        <article key={`${item.url}-${item.title}`} className="data-management-recent-item">
                          <div>
                            <strong>{item.title || item.siteName}</strong>
                            <span>{item.siteName}{item.publishTier ? ` · ${item.publishTier}` : ""}</span>
                            {item.snippet ? <span>{item.snippet}</span> : null}
                            <span className="data-management-path-cell">{item.url}</span>
                          </div>
                          <div className="data-management-source-chips">
                            {item.signals.map((signal) => (
                              <span key={`${item.url}-${signal}`} className="data-management-source-chip">
                                {signal}
                              </span>
                            ))}
                          </div>
                        </article>
                      )) : <div className="crud-empty-state">当前国家还没有可展示的 evidence cards。</div>}
                    </div>
                  </article>

                  <article className="card crud-card data-management-domain-card">
                    <div className="data-management-card-header">
                      <div>
                        <strong>VOC docs</strong>
                        <p>本次 VOC 方案、实现状态与抓取设计文档都在这里留路径，方便继续追踪。</p>
                      </div>
                    </div>
                    <div className="data-management-recent-list">
                      {vocOverview.documentation.map((item) => (
                        <article key={item.path} className="data-management-recent-item">
                          <div>
                            <strong>{item.label}</strong>
                            <span className="data-management-path-cell">{item.path}</span>
                          </div>
                          <time>{formatDataManagementTimestamp(item.updatedAt)}</time>
                        </article>
                      ))}
                    </div>
                  </article>
                </div>
              </>
            ) : null}
          </div>

          {overview.database.connected ? (
            <div className="card crud-card">
              <div className="admin-card-header">
                <div>
                  <h2>数据库 CRUD</h2>
                  <p>先覆盖数据库侧最适合管理员直管的三个实体：MSRP Sources、Engineering Projects、Review Overrides。</p>
                </div>
                <button type="button" className="btn btn-sm btn-secondary" onClick={() => void loadCrudData()}>
                  {crudLoading ? "刷新中…" : "刷新列表"}
                </button>
              </div>

              {crudError ? <div className="alert alert-error">{crudError}</div> : null}
              {crudNotice ? <div className="alert alert-success">{crudNotice}</div> : null}

              <div className="admin-tabs">
                <button
                  type="button"
                  className={`admin-tab${crudTab === "msrp-sources" ? " is-active" : ""}`}
                  onClick={() => setCrudTab("msrp-sources")}
                >
                  MSRP Sources
                </button>
                <button
                  type="button"
                  className={`admin-tab${crudTab === "engineering-projects" ? " is-active" : ""}`}
                  onClick={() => setCrudTab("engineering-projects")}
                >
                  Engineering Projects
                </button>
                <button
                  type="button"
                  className={`admin-tab${crudTab === "review-overrides" ? " is-active" : ""}`}
                  onClick={() => setCrudTab("review-overrides")}
                >
                  Review Overrides
                </button>
              </div>

              {crudTab === "msrp-sources" ? (
                <>
                  <div className="crud-toolbar-grid">
                    <div className="filter-group">
                      <label>Country</label>
                      <input
                        value={sourceFilters.country}
                        onChange={(event) => setSourceFilters((prev) => ({ ...prev, country: event.target.value }))}
                        placeholder="China"
                      />
                    </div>
                    <div className="filter-group">
                      <label>Brand</label>
                      <input
                        value={sourceFilters.brand}
                        onChange={(event) => setSourceFilters((prev) => ({ ...prev, brand: event.target.value }))}
                        placeholder="BYD"
                      />
                    </div>
                    <div className="filter-group">
                      <label>Enabled</label>
                      <select
                        value={sourceFilters.enabled}
                        onChange={(event) => setSourceFilters((prev) => ({ ...prev, enabled: event.target.value as SourceFilters["enabled"] }))}
                      >
                        <option value="all">All</option>
                        <option value="true">Enabled</option>
                        <option value="false">Disabled</option>
                      </select>
                    </div>
                    <div className="data-management-inline-actions">
                      <button type="button" className="btn btn-sm btn-secondary" onClick={() => void loadCrudData("msrp-sources")}>查询</button>
                      <button type="button" className="btn btn-sm btn-ghost" onClick={() => setSourceFilters(defaultSourceFilters())}>重置</button>
                    </div>
                  </div>

                  <form className="data-management-crud-form" onSubmit={handleSourceSubmit}>
                    <div className="crud-toolbar-grid">
                      <div className="filter-group"><label>Source Code</label><input value={sourceForm.sourceCode} disabled={Boolean(editingSourceId)} onChange={(event) => setSourceForm((prev) => ({ ...prev, sourceCode: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Country</label><input value={sourceForm.country} onChange={(event) => setSourceForm((prev) => ({ ...prev, country: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Brand</label><input value={sourceForm.brand} onChange={(event) => setSourceForm((prev) => ({ ...prev, brand: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Source URL</label><input value={sourceForm.sourceUrl} onChange={(event) => setSourceForm((prev) => ({ ...prev, sourceUrl: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Source Type</label><input value={sourceForm.sourceType} onChange={(event) => setSourceForm((prev) => ({ ...prev, sourceType: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Tier</label><select value={sourceForm.tier} onChange={(event) => setSourceForm((prev) => ({ ...prev, tier: Number(event.target.value) || 3 }))}><option value={1}>1 · Official</option><option value={2}>2 · Reference / Dealer</option><option value={3}>3 · Curated</option><option value={4}>4 · Third-party</option><option value={5}>5 · Experimental</option></select></div>
                      <div className="filter-group"><label>Extractor</label><input value={sourceForm.extractorName} onChange={(event) => setSourceForm((prev) => ({ ...prev, extractorName: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Extractor Version</label><input value={sourceForm.extractorVersion} onChange={(event) => setSourceForm((prev) => ({ ...prev, extractorVersion: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Price Semantics</label><input value={sourceForm.priceSemantics} onChange={(event) => setSourceForm((prev) => ({ ...prev, priceSemantics: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Notes</label><input value={sourceForm.notes} onChange={(event) => setSourceForm((prev) => ({ ...prev, notes: event.target.value }))} /></div>
                      <label className="data-management-checkbox"><input type="checkbox" checked={sourceForm.requiresLocation} onChange={(event) => setSourceForm((prev) => ({ ...prev, requiresLocation: event.target.checked }))} />Requires Location</label>
                      <label className="data-management-checkbox"><input type="checkbox" checked={sourceForm.enabled} onChange={(event) => setSourceForm((prev) => ({ ...prev, enabled: event.target.checked }))} />Enabled</label>
                    </div>
                    <div className="data-management-inline-actions">
                      <button type="submit" className="btn btn-sm btn-secondary">{editingSourceId ? "保存修改" : "新建 Source"}</button>
                      {editingSourceId ? <button type="button" className="btn btn-sm btn-ghost" onClick={resetSourceEditor}>取消编辑</button> : null}
                    </div>
                  </form>

                  <div className="crud-table-wrapper">
                    <table className="crud-table">
                      <thead>
                        <tr>
                          <th>Code</th>
                          <th>Country / Brand</th>
                          <th>Tier / Extractor</th>
                          <th>Extractor</th>
                          <th>Status</th>
                          <th>Updated</th>
                          <th>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {sources.map((item) => (
                          <tr key={item.id}>
                            <td><strong>{item.sourceCode}</strong><div className="data-management-table-subtle">{item.sourceType}</div></td>
                            <td>{item.country}<div className="data-management-table-subtle">{item.brand}</div></td>
                            <td><strong>T{item.tier}</strong><div className="data-management-table-subtle">{item.extractorName}</div></td>
                            <td>{item.extractorVersion}<div className="data-management-table-subtle">{item.priceSemantics}</div></td>
                            <td><span className={`badge ${getDataManagementStatusBadgeClass(item.enabled ? "ready" : "inactive")}`}>{item.enabled ? "enabled" : "disabled"}</span></td>
                            <td>{formatDataManagementTimestamp(item.updatedAt)}</td>
                            <td>
                              <div className="data-management-inline-actions">
                                <button type="button" className="btn btn-sm btn-ghost" onClick={() => startEditSource(item)}>编辑</button>
                                <button type="button" className="btn btn-sm btn-ghost" onClick={() => void handleDeactivateSource(item.id)}>停用</button>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {sources.length === 0 ? <div className="crud-empty-state">暂无 MSRP source 结果。</div> : null}
                  </div>
                </>
              ) : null}

              {crudTab === "engineering-projects" ? (
                <>
                  <div className="crud-toolbar-grid">
                    <div className="filter-group">
                      <label>Status</label>
                      <input
                        value={projectFilters.status}
                        onChange={(event) => setProjectFilters((prev) => ({ ...prev, status: event.target.value }))}
                        placeholder="active"
                      />
                    </div>
                    <div className="filter-group">
                      <label>Brand</label>
                      <input
                        value={projectFilters.brand}
                        onChange={(event) => setProjectFilters((prev) => ({ ...prev, brand: event.target.value }))}
                        placeholder="BMW"
                      />
                    </div>
                    <div className="filter-group">
                      <label>Market</label>
                      <input
                        value={projectFilters.marketCountry}
                        onChange={(event) => setProjectFilters((prev) => ({ ...prev, marketCountry: event.target.value }))}
                        placeholder="Germany"
                      />
                    </div>
                    <div className="data-management-inline-actions">
                      <button type="button" className="btn btn-sm btn-secondary" onClick={() => void loadCrudData("engineering-projects")}>查询</button>
                      <button type="button" className="btn btn-sm btn-ghost" onClick={() => setProjectFilters(defaultProjectFilters())}>重置</button>
                    </div>
                  </div>

                  <form className="data-management-crud-form" onSubmit={handleProjectSubmit}>
                    <div className="crud-toolbar-grid">
                      <div className="filter-group"><label>Project Code</label><input value={projectForm.projectCode} disabled={Boolean(editingProjectId)} onChange={(event) => setProjectForm((prev) => ({ ...prev, projectCode: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Brand</label><input value={projectForm.brand} onChange={(event) => setProjectForm((prev) => ({ ...prev, brand: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Model</label><input value={projectForm.model} onChange={(event) => setProjectForm((prev) => ({ ...prev, model: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Market Country</label><input value={projectForm.marketCountry} onChange={(event) => setProjectForm((prev) => ({ ...prev, marketCountry: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Display Name</label><input value={projectForm.displayName} onChange={(event) => setProjectForm((prev) => ({ ...prev, displayName: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Status</label><input value={projectForm.status} onChange={(event) => setProjectForm((prev) => ({ ...prev, status: event.target.value }))} required /></div>
                    </div>
                    <div className="data-management-inline-actions">
                      <button type="submit" className="btn btn-sm btn-secondary">{editingProjectId ? "保存修改" : "新建 Project"}</button>
                      {editingProjectId ? <button type="button" className="btn btn-sm btn-ghost" onClick={resetProjectEditor}>取消编辑</button> : null}
                    </div>
                  </form>

                  <div className="crud-table-wrapper">
                    <table className="crud-table">
                      <thead>
                        <tr>
                          <th>Code</th>
                          <th>Brand / Model</th>
                          <th>Market</th>
                          <th>Status</th>
                          <th>Updated</th>
                          <th>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {projects.map((item) => (
                          <tr key={item.id}>
                            <td><strong>{item.projectCode}</strong><div className="data-management-table-subtle">{item.displayName}</div></td>
                            <td>{item.brand}<div className="data-management-table-subtle">{item.model}</div></td>
                            <td>{item.marketCountry}</td>
                            <td><span className={`badge ${getDataManagementStatusBadgeClass(item.status === "archived" ? "inactive" : "ready")}`}>{item.status}</span></td>
                            <td>{formatDataManagementTimestamp(item.updatedAt)}</td>
                            <td>
                              <div className="data-management-inline-actions">
                                <button type="button" className="btn btn-sm btn-ghost" onClick={() => startEditProject(item)}>编辑</button>
                                <button type="button" className="btn btn-sm btn-ghost" onClick={() => void handleArchiveProject(item.id)}>归档</button>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {projects.length === 0 ? <div className="crud-empty-state">暂无 Engineering project 结果。</div> : null}
                  </div>
                </>
              ) : null}

              {crudTab === "review-overrides" ? (
                <>
                  <div className="crud-toolbar-grid">
                    <div className="filter-group">
                      <label>Country</label>
                      <input
                        value={overrideFilters.country}
                        onChange={(event) => setOverrideFilters((prev) => ({ ...prev, country: event.target.value }))}
                        placeholder="China"
                      />
                    </div>
                    <div className="filter-group">
                      <label>Brand</label>
                      <input
                        value={overrideFilters.brand}
                        onChange={(event) => setOverrideFilters((prev) => ({ ...prev, brand: event.target.value }))}
                        placeholder="BYD"
                      />
                    </div>
                    <div className="filter-group">
                      <label>JATO Model</label>
                      <input
                        value={overrideFilters.jatoModel}
                        onChange={(event) => setOverrideFilters((prev) => ({ ...prev, jatoModel: event.target.value }))}
                        placeholder="Seal"
                      />
                    </div>
                    <div className="data-management-inline-actions">
                      <button type="button" className="btn btn-sm btn-secondary" onClick={() => void loadCrudData("review-overrides")}>查询</button>
                      <button type="button" className="btn btn-sm btn-ghost" onClick={() => setOverrideFilters(defaultOverrideFilters())}>重置</button>
                    </div>
                  </div>

                  <form className="data-management-crud-form" onSubmit={handleOverrideSubmit}>
                    <div className="crud-toolbar-grid">
                      <div className="filter-group"><label>Country</label><input value={overrideForm.country} disabled={Boolean(editingOverrideId)} onChange={(event) => setOverrideForm((prev) => ({ ...prev, country: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Brand</label><input value={overrideForm.brand} disabled={Boolean(editingOverrideId)} onChange={(event) => setOverrideForm((prev) => ({ ...prev, brand: event.target.value }))} required /></div>
                      <div className="filter-group"><label>JATO Model</label><input value={overrideForm.jatoModel} disabled={Boolean(editingOverrideId)} onChange={(event) => setOverrideForm((prev) => ({ ...prev, jatoModel: event.target.value }))} required /></div>
                      <div className="filter-group"><label>JATO Trim</label><input value={overrideForm.jatoTrim} disabled={Boolean(editingOverrideId)} onChange={(event) => setOverrideForm((prev) => ({ ...prev, jatoTrim: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Official Model</label><input value={overrideForm.officialModel} onChange={(event) => setOverrideForm((prev) => ({ ...prev, officialModel: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Official Trim</label><input value={overrideForm.officialTrim} onChange={(event) => setOverrideForm((prev) => ({ ...prev, officialTrim: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Valid From</label><input type="date" value={overrideForm.validFromDate} onChange={(event) => setOverrideForm((prev) => ({ ...prev, validFromDate: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Valid To</label><input type="date" value={overrideForm.validToDate} onChange={(event) => setOverrideForm((prev) => ({ ...prev, validToDate: event.target.value }))} /></div>
                      <div className="filter-group"><label>Reason</label><input value={overrideForm.overrideReason} onChange={(event) => setOverrideForm((prev) => ({ ...prev, overrideReason: event.target.value }))} required /></div>
                      <div className="filter-group"><label>Created By</label><input value={overrideForm.createdBy} onChange={(event) => setOverrideForm((prev) => ({ ...prev, createdBy: event.target.value }))} required /></div>
                    </div>
                    <div className="data-management-inline-actions">
                      <button type="submit" className="btn btn-sm btn-secondary">{editingOverrideId ? "保存修改" : "新建 Override"}</button>
                      {editingOverrideId ? <button type="button" className="btn btn-sm btn-ghost" onClick={resetOverrideEditor}>取消编辑</button> : null}
                    </div>
                  </form>

                  <div className="crud-table-wrapper">
                    <table className="crud-table">
                      <thead>
                        <tr>
                          <th>Key</th>
                          <th>Official Mapping</th>
                          <th>Valid Window</th>
                          <th>Updated</th>
                          <th>Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {overrides.map((item) => (
                          <tr key={item.id}>
                            <td><strong>{item.country} · {item.brand}</strong><div className="data-management-table-subtle">{item.jatoModel} / {item.jatoTrim}</div></td>
                            <td>{item.officialModel}<div className="data-management-table-subtle">{item.officialTrim}</div></td>
                            <td>{item.validFromDate} → {item.validToDate ?? "open"}</td>
                            <td>{formatDataManagementTimestamp(item.updatedAt)}</td>
                            <td>
                              <div className="data-management-inline-actions">
                                <button type="button" className="btn btn-sm btn-ghost" onClick={() => startEditOverride(item)}>编辑</button>
                                <button type="button" className="btn btn-sm btn-ghost" onClick={() => void handleDeleteOverride(item.id)}>删除</button>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {overrides.length === 0 ? <div className="crud-empty-state">暂无 Review override 结果。</div> : null}
                  </div>
                </>
              ) : null}
            </div>
          ) : (
            <div className="card crud-card">
              <div className="crud-empty-state">数据库当前未连接，CRUD 工作台暂不可用。</div>
            </div>
          )}

          <div className="card crud-table-card">
            <div className="admin-card-header">
              <div>
                <h2>文件盘点</h2>
                <p>JATO、wiki、月更作业等关键路径的存在性、体量和更新时间。</p>
              </div>
            </div>
            <div className="crud-table-wrapper">
              <table className="crud-table">
                <thead>
                  <tr>
                    <th>资源</th>
                    <th>路径</th>
                    <th>状态</th>
                    <th>体量</th>
                    <th>更新时间</th>
                  </tr>
                </thead>
                <tbody>
                  {overview.fileInventory.map((item) => (
                    <tr key={item.key}>
                      <td>
                        <strong>{item.label}</strong>
                        <div className="data-management-table-subtle">{item.kind}</div>
                      </td>
                      <td className="data-management-path-cell">{item.path}</td>
                      <td>
                        <span className={`badge ${getDataManagementStatusBadgeClass(item.exists ? "ready" : "warning")}`}>
                          {item.exists ? "present" : "missing"}
                        </span>
                      </td>
                      <td>
                        {item.isDir
                          ? `${formatDataManagementNumber(item.fileCount)} items`
                          : formatDataManagementBytes(item.sizeBytes)}
                      </td>
                      <td>{formatDataManagementTimestamp(item.updatedAt)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="card crud-table-card">
            <div className="admin-card-header">
              <div>
                <h2>数据库表</h2>
                <p>按域汇总核心业务表的行数和最近事件时间；数据库不可用时这里会自动降级为空。</p>
              </div>
            </div>
            {overview.databaseTables.length === 0 ? (
              <div className="crud-empty-state">当前没有可展示的数据库表摘要。</div>
            ) : (
              <div className="crud-table-wrapper">
                <table className="crud-table">
                  <thead>
                    <tr>
                      <th>表</th>
                      <th>域</th>
                      <th>Rows</th>
                      <th>Last event</th>
                      <th>状态</th>
                    </tr>
                  </thead>
                  <tbody>
                    {overview.databaseTables.map((item) => (
                      <tr key={item.key}>
                        <td>
                          <strong>{item.label}</strong>
                          <div className="data-management-table-subtle">{item.schema}.{item.table}</div>
                        </td>
                        <td>{item.domain}</td>
                        <td>{item.rowCount.toLocaleString()}</td>
                        <td>{formatDataManagementTimestamp(item.lastEventAt)}</td>
                        <td>
                          <span className={`badge ${getDataManagementStatusBadgeClass(item.status)}`}>
                            {item.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      ) : null}

      {subpage === "dryrun" ? (
        <div className="card crud-card" style={{ padding: 16 }}>
          <MsrpDryrunDashboard />
        </div>
      ) : null}

      {subpage === "order-genius" ? (
        <div className="card crud-card" style={{ padding: 24 }}>
          <h2>Order Genius</h2>
          <p style={{color:"#64748b"}}>Country order matrix, monthly quantity editing, and Excel export.</p>
          <Link to="/data/order-genius" className="btn btn-sm btn-primary" style={{marginTop:12,display:"inline-block"}}>
            Open Order Genius
          </Link>
        </div>
      ) : null}

      {subpage === "material-master" ? (
        <div className="card crud-card" style={{ padding: 24 }}>
          <h2>Material Master Upload</h2>
          <p style={{color:"#64748b"}}>Upload OMODA &amp; JAECOO Material Master XLSX files to publish new SKU baselines.</p>
          <Link to="/data/order-genius" className="btn btn-sm btn-primary" style={{marginTop:12,display:"inline-block"}}>
            Open Material Master
          </Link>
        </div>
      ) : null}

    </section>
  );
}
