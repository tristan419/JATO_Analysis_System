import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { AdminToolsNav } from "../components/AdminToolsNav";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  DataManagementAirflowStatus,
  DataManagementDomain,
  DataManagementOverviewResponse,
  DataManagementVocOverviewResponse,
  ConfigProject,
  MatchOverride,
  MsrpSource,
} from "../types/dataManagement";
import {
  buildActivityHeatmapColumns,
  formatDataManagementBytes,
  formatDataManagementNumber,
  formatDataManagementTimestamp,
  getDataManagementStatusBadgeClass,
} from "../utils/dataManagement";

type CrudEntityTab = "msrp-sources" | "engineering-projects" | "review-overrides";
type DataSubpage = "overview" | "hermes" | "voc";
const DEFAULT_RECENT_ITEMS_VISIBLE = 6;

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
  const [subpage, setSubpage] = useState<DataSubpage>("overview");
  const [hermesOverview, setHermesOverview] = useState<Record<string, unknown> | null>(null);
  const [hermesPipelines, setHermesPipelines] = useState<Record<string, unknown> | null>(null);
  const [hermesSources, setHermesSources] = useState<Record<string, unknown> | null>(null);
  const [hermesCost, setHermesCost] = useState<Record<string, unknown> | null>(null);
  const [hermesProposals, setHermesProposals] = useState<unknown[]>([]);
  const [hermesFeatures, setHermesFeatures] = useState<unknown[]>([]);
  const [hermesToolchain, setHermesToolchain] = useState<Record<string, unknown> | null>(null);
  const [hermesArch, setHermesArch] = useState<Record<string, unknown> | null>(null);
  const [hermesActivity, setHermesActivity] = useState<Record<string, unknown> | null>(null);
  const [hermesCostHeatmap, setHermesCostHeatmap] = useState<Record<string, unknown> | null>(null);
  const [hermesDaily, setHermesDaily] = useState<Record<string, unknown> | null>(null);
  const [selectedSource, setSelectedSource] = useState<Record<string, unknown> | null>(null);
  const [sourceDetail, setSourceDetail] = useState<Record<string, unknown> | null>(null);
  const [sourceDetailOpen, setSourceDetailOpen] = useState(false);
  const [hermesLoading, setHermesLoading] = useState(false);
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
    void loadOverview();
    void loadVocOverview();
  }, []);

  useEffect(() => {
    if (overview?.database.connected) {
      void loadCrudData(crudTab);
    }
  }, [crudTab, overview?.database.connected]);

  useEffect(() => {
    if ((subpage !== "hermes" && subpage !== "overview") || hermesOverview) return;
    setHermesLoading(true);
    Promise.allSettled([
      api.hermesOverview().then(setHermesOverview),
      api.hermesPipelineHealth().then(setHermesPipelines),
      api.hermesSourceQuality().then(setHermesSources),
      api.hermesCost().then(setHermesCost),
      api.hermesProposals().then(setHermesProposals),
      api.hermesFeatures().then(setHermesFeatures),
      api.hermesToolchain().then(setHermesToolchain),
      api.hermesArchitecture().then(setHermesArch),
      api.hermesActivityHeatmap().then(setHermesActivity),
      api.hermesCostHeatmap().then(setHermesCostHeatmap),
      api.hermesDailySummary().then(setHermesDaily),
    ]).finally(() => setHermesLoading(false));
  }, [subpage, hermesOverview]);

  const activityColumns = useMemo(
    () => buildActivityHeatmapColumns(overview?.activity.days ?? []),
    [overview]
  );

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
        <button type="button" className={`admin-tab${subpage === "hermes" ? " is-active" : ""}`} onClick={() => setSubpage("hermes")}>Hermes Governance</button>
        <button type="button" className={`admin-tab${subpage === "voc" ? " is-active" : ""}`} onClick={() => setSubpage("voc")}>VOC 观察台</button>
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
                    setVocSyncNotice(`Synced: ${(res as Record<string,unknown>).countryCount || "?"} countries, ${(res as Record<string,unknown>).documentCount || "?"} documents`);
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

      {subpage === "hermes" ? (
        hermesLoading ? (
          <LoadingSurface mode="inline" label="Loading Hermes governance data..." kicker="Hermes" />
        ) : (
          <>
            {/* ---------- Tool Chain: How Hermes Works ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>How Hermes Works</h2></div></div>
              <div style={{ padding: 16 }}>
                {(hermesToolchain?.workflow as unknown[])?.length > 0 ? (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 20, alignItems: "flex-start" }}>
                    {(hermesToolchain.workflow as unknown[]).map((step: unknown) => {
                      const s = step as Record<string,unknown>;
                      return (
                        <div key={Number(s.step)} style={{
                          flex: "0 0 180px", background: "#f8fafc", borderRadius: 8,
                          padding: "10px 12px", borderLeft: "3px solid #3b82f6", fontSize: 12,
                        }}>
                          <div style={{fontWeight:700,color:"#3b82f6",marginBottom:2}}>{s.phase}</div>
                          <div style={{fontWeight:600,marginBottom:4}}>{s.action}</div>
                          <div style={{color:"#64748b",fontSize:11}}>{s.description}</div>
                          <div style={{color:"#94a3b8",fontSize:10,marginTop:4,fontFamily:"monospace"}}>{s.script}</div>
                        </div>
                      );
                    })}
                  </div>
                ) : null}

                {/* Scripts + Registries + Reports in 3 columns */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, fontSize: 13 }}>
                  <div>
                    <strong style={{display:"block",marginBottom:8}}>Scripts ({(hermesToolchain?.scriptCount as number) || 0})</strong>
                    {((hermesToolchain?.scripts as unknown[]) || []).map((s: unknown) => {
                      const sc = s as Record<string,unknown>;
                      return <div key={String(sc.name)} style={{padding:"3px 0",fontFamily:"monospace",fontSize:11,color:"#475569"}}>{String(sc.name)}</div>;
                    })}
                  </div>
                  <div>
                    <strong style={{display:"block",marginBottom:8}}>Registries ({(hermesToolchain?.registryCount as number) || 0})</strong>
                    {((hermesToolchain?.registries as unknown[]) || []).map((r: unknown) => {
                      const reg = r as Record<string,unknown>;
                      return <div key={String(reg.name)} style={{padding:"3px 0",fontFamily:"monospace",fontSize:11,color:"#475569"}}>{String(reg.name)}</div>;
                    })}
                  </div>
                  <div>
                    <strong style={{display:"block",marginBottom:8}}>Reports ({(hermesToolchain?.reportCount as number) || 0})</strong>
                    {((hermesToolchain?.reports as unknown[]) || []).map((r: unknown) => {
                      const rep = r as Record<string,unknown>;
                      return <div key={String(rep.name)} style={{padding:"3px 0",fontFamily:"monospace",fontSize:11,color:"#475569"}}>{String(rep.name)}</div>;
                    })}
                  </div>
                </div>
              </div>
            </div>

            {/* ---------- 4 Governors: Who Does What ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>4 Hermes Governors — Who Should I Ask?</h2></div></div>
              <div style={{ padding: 16 }}>
                {(hermesArch?.modules as unknown[])?.length > 0 ? (
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(2,1fr)", gap: 12, marginBottom: 16 }}>
                    {(hermesArch.modules as unknown[]).map((m: unknown) => {
                      const mod = m as Record<string,unknown>;
                      const scripts = (mod.scripts as string[]) || [];
                      const answers = (mod.answers as string[]) || [];
                      const icon = String(mod.icon || "");
                      const colors: Record<string,string> = {code:"#3b82f6",pipeline:"#f59e0b",intelligence:"#8b5cf6",knowledge:"#22c55e"};
                      return (
                        <div key={String(mod.governor)} style={{
                          background: "#f8fafc", borderRadius: 8, padding: "14px 16px",
                          borderLeft: `4px solid ${colors[icon] || "#94a3b8"}`,
                        }}>
                          <div style={{fontWeight:700,fontSize:15,marginBottom:2,color:colors[icon]}}>{mod.governor}</div>
                          <div style={{color:"#64748b",fontSize:11,marginBottom:8}}>{mod.phase} · {scripts.join(", ")}</div>
                          <div style={{fontSize:12,color:"#334155",marginBottom:4,fontWeight:600}}>Answers:</div>
                          {answers.map((a: string, i: number) => (
                            <div key={i} style={{fontSize:11,color:"#475569",padding:"1px 0",paddingLeft:8,borderLeft:"2px solid #e2e8f0",marginBottom:2}}>{a}</div>
                          ))}
                          <div style={{marginTop:8,fontSize:10,color:"#94a3b8"}}>Trigger: {mod.triggers}</div>
                        </div>
                      );
                    })}
                  </div>
                ) : null}

                {/* Work Routing Table */}
                {(hermesArch?.routing as unknown[])?.length > 0 ? (
                  <div>
                    <strong style={{display:"block",marginBottom:8,fontSize:14}}>Work Routing Guide</strong>
                    <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                      <thead><tr style={{background:"#f1f5f9"}}>
                        <th style={{padding:"6px 10px",textAlign:"left"}}>I want to...</th>
                        <th style={{padding:"6px 10px",textAlign:"left"}}>Ask</th>
                        <th style={{padding:"6px 10px",textAlign:"left",fontFamily:"monospace",fontSize:11}}>Command</th>
                      </tr></thead>
                      <tbody>
                        {(hermesArch.routing as unknown[]).slice(0, 10).map((r: unknown, i: number) => {
                          const row = r as Record<string,unknown>;
                          return (
                            <tr key={i} style={{borderTop:"1px solid #e2e8f0"}}>
                              <td style={{padding:"6px 10px",fontWeight:500}}>{String(row.task)}</td>
                              <td style={{padding:"6px 10px",color:"#3b82f6",fontWeight:600,fontSize:11}}>{String(row.ask)}</td>
                              <td style={{padding:"6px 10px",fontFamily:"monospace",fontSize:11,color:"#475569"}}>{String(row.run)}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                ) : null}
              </div>
            </div>

            {/* ---------- Dependency Graph ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>How They Connect</h2></div></div>
              <div style={{ padding: 16 }}>
                {((hermesArch?.dependencies as unknown[]) || []).map((d: unknown, i: number) => {
                  const dep = d as Record<string,unknown>;
                  return (
                    <div key={i} style={{display:"flex",alignItems:"center",gap:10,padding:"5px 0",fontSize:12,borderBottom:"1px solid #f1f5f9"}}>
                      <span style={{fontFamily:"monospace",fontWeight:600,color:"#3b82f6",minWidth:180}}>{String(dep.from)}</span>
                      <span style={{color:"#94a3b8"}}>→</span>
                      <span style={{fontFamily:"monospace",fontWeight:600,color:"#f59e0b",minWidth:140}}>{String(dep.to)}</span>
                      <span style={{color:"#64748b",fontSize:11}}>{String(dep.what)}</span>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* ---------- Operations: Run Hermes Scripts ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Operations — Run Hermes Scripts from UI</h2></div></div>
              <div style={{ padding: 16 }}>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 8, marginBottom: 16 }}>
                  {["pipeline-audit","source-quality","cost-report","code-audit","evidence","answer-audit"].map((cmd) => (
                    <button key={cmd} type="button" className="btn btn-sm btn-primary"
                      style={{ fontSize: 12 }}
                      onClick={() => {
                        setHermesLoading(true);
                        api.hermesRun(cmd).then((res) => {
                          const el = document.getElementById(`hermes-run-output-${cmd}`);
                          if (el) el.textContent = JSON.stringify(res, null, 2);
                          // Refresh related data
                          if (cmd === "pipeline-audit") api.hermesPipelineHealth().then(setHermesPipelines);
                          if (cmd === "source-quality") api.hermesSourceQuality().then(setHermesSources);
                          if (cmd === "cost-report") api.hermesCost().then(setHermesCost);
                        }).catch((e) => {
                          const el = document.getElementById(`hermes-run-output-${cmd}`);
                          if (el) el.textContent = String(e);
                        }).finally(() => setHermesLoading(false));
                      }}
                    >
                      Run {cmd.replace("-"," ")}
                    </button>
                  ))}
                </div>
                <div style={{ background: "#1e293b", color: "#e2e8f0", borderRadius: 8, padding: 12, fontFamily: "monospace", fontSize: 11, maxHeight: 200, overflow: "auto", whiteSpace: "pre-wrap" }}>
                  {(["pipeline-audit","source-quality","cost-report","code-audit","evidence","answer-audit"] as string[]).map((cmd) => (
                    <div key={cmd} id={`hermes-run-output-${cmd}`} style={{ display: "none" }} />
                  ))}
                  <span style={{ color: "#64748b" }}>Click a Run button above. Output appears here.</span>
                </div>
              </div>
            </div>

            {/* ---------- Activity & Cost Heatmaps ---------- */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div className="card crud-card">
                <div className="admin-card-header"><div><h2>Activity Heatmap (30d) — Hermes Script Runs</h2></div></div>
                <div style={{ padding: 16 }}>
                  {hermesActivity ? (
                    <>
                      <div style={{display:"flex",gap:8,marginBottom:12,fontSize:12,color:"#64748b"}}>
                        <span>Total: {(hermesActivity.totalRecords as number) || 0} runs</span>
                        <span>Last: {String((hermesActivity.lastRun as Record<string,unknown>)?.timestamp || "never").slice(0,16)}</span>
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(28px,1fr))",gap:3}}>
                        {((hermesActivity.days as unknown[]) || []).map((d: unknown) => {
                          const day = d as Record<string,unknown>;
                          const count = (day.count as number) || 0;
                          const intensity = count === 0 ? "#f1f5f9" : count === 1 ? "#93c5fd" : count <= 3 ? "#3b82f6" : count <= 6 ? "#1d4ed8" : "#1e3a5f";
                          return <div key={String(day.date)} title={`${String(day.date)}: ${count} runs`}
                            style={{aspectRatio:"1",background:intensity,borderRadius:3,minWidth:24}} />;
                        })}
                      </div>
                    </>
                  ) : <span style={{color:"#94a3b8",fontSize:12}}>No activity data yet. Run a Hermes script to populate.</span>}
                </div>
              </div>

              <div className="card crud-card">
                <div className="admin-card-header"><div><h2>Cost Heatmap (30d) — 20 CNY/day, 500 CNY/month</h2></div></div>
                <div style={{ padding: 16 }}>
                  {hermesCostHeatmap ? (
                    <>
                      <div style={{display:"flex",gap:8,marginBottom:12,fontSize:12}}>
                        <span style={{fontWeight:600,color: (hermesCostHeatmap.monthlyStatus === "exceeded" || (hermesCostHeatmap.alerts as unknown[])?.length > 0) ? "#ef4444" : "#22c55e"}}>
                          Total: {(hermesCostHeatmap.totalCny as number)?.toFixed(2)} CNY
                        </span>
                        <span style={{color:"#64748b"}}>Budget: {String(hermesCostHeatmap.monthlyBudgetCny)} CNY/mo</span>
                        {hermesCostHeatmap.emailSent ? <span style={{color:"#f59e0b"}}>Alert emailed to {(hermesCostHeatmap.alertEmail as string)}</span> : null}
                      </div>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(28px,1fr))",gap:3}}>
                        {((hermesCostHeatmap.days as unknown[]) || []).map((d: unknown) => {
                          const day = d as Record<string,unknown>;
                          const cost = (day.costCny as number) || 0;
                          const over = day.overDailyBudget;
                          const intensity = cost === 0 ? "#f1f5f9" : cost < 5 ? "#bbf7d0" : cost < 10 ? "#4ade80" : cost < 20 ? "#f59e0b" : "#ef4444";
                          return <div key={String(day.date)} title={`${String(day.date)}: ${cost.toFixed(2)} CNY${over ? " OVER DAILY BUDGET" : ""}`}
                            style={{aspectRatio:"1",background:intensity,borderRadius:3,minWidth:24,border: over ? "2px solid #ef4444" : "none"}} />;
                        })}
                      </div>
                      <div style={{display:"flex",gap:12,marginTop:8,fontSize:11,color:"#64748b"}}>
                        <span>0 CNY</span><span style={{flex:1,background:"linear-gradient(to right,#f1f5f9,#bbf7d0,#4ade80,#f59e0b,#ef4444)",height:8,borderRadius:4}} />
                        <span>20+ CNY</span>
                      </div>
                      {(hermesCostHeatmap.alerts as unknown[])?.length > 0 && (
                        <div style={{marginTop:8,padding:"8px 12px",background:"#fef2f2",borderRadius:6,fontSize:12,color:"#ef4444"}}>
                          {(hermesCostHeatmap.alerts as unknown[]).map((a: unknown, i: number) => <div key={i}>{String(a)}</div>)}
                        </div>
                      )}
                    </>
                  ) : <span style={{color:"#94a3b8",fontSize:12}}>No cost data yet.</span>}
                </div>
              </div>
            </div>

            {/* ---------- 当前状态 ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Current Status</h2></div></div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, padding: 16 }}>
                <div className="metric-chip"><span>Pipelines Registered</span><strong>{(hermesOverview?.registries as Record<string,number>)?.pipeline || 0}</strong></div>
                <div className="metric-chip"><span>Features</span><strong>{(hermesOverview?.registries as Record<string,number>)?.feature || 0}</strong></div>
                <div className="metric-chip"><span>Sources Tracked</span><strong>{(hermesOverview?.registries as Record<string,number>)?.source || 0}</strong></div>
                <div className="metric-chip"><span>Reports Available</span><strong>{Object.values(hermesOverview?.reports as Record<string,boolean> || {}).filter(Boolean).length}</strong></div>
              </div>
            </div>

            {/* ---------- Pipeline Health ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Pipeline Health</h2></div></div>
              <div style={{ padding: 16 }}>
                {(hermesPipelines as Record<string,unknown>)?.summary ? (
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, marginBottom: 12 }}>
                    <div className="metric-chip"><span>Registered</span><strong>{String((hermesPipelines as Record<string,unknown>).summary?.registeredPipelines || 0)}</strong></div>
                    <div className="metric-chip"><span>Duplicate Risks</span><strong style={{color: (hermesPipelines as Record<string,unknown>).summary?.duplicateSchedulingRisks > 0 ? "#ef4444" : "#22c55e"}}>{String((hermesPipelines as Record<string,unknown>).summary?.duplicateSchedulingRisks || 0)}</strong></div>
                    <div className="metric-chip"><span>High Risk</span><strong style={{color: (hermesPipelines as Record<string,unknown>).summary?.highRiskFindings > 0 ? "#ef4444" : "#22c55e"}}>{String((hermesPipelines as Record<string,unknown>).summary?.highRiskFindings || 0)}</strong></div>
                    <div className="metric-chip"><span>Status Gaps</span><strong>{String((hermesPipelines as Record<string,unknown>).summary?.statusCoverageGaps || 0)}</strong></div>
                  </div>
                ) : null}
                {((hermesPipelines as Record<string,unknown>)?.allPipelines as unknown[])?.length > 0 ? (
                  <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
                    <thead><tr style={{background:"#f8fafc"}}><th style={{padding:"6px 10px",textAlign:"left"}}>Pipeline</th><th style={{padding:"6px 10px"}}>Type</th><th style={{padding:"6px 10px"}}>Role</th><th style={{padding:"6px 10px"}}>Risk</th></tr></thead>
                    <tbody>
                      {((hermesPipelines as Record<string,unknown>)?.allPipelines as unknown[]).slice(0, 12).map((p: unknown) => {
                        const pipe = p as Record<string,unknown>;
                        const risk = String(pipe.risk || "low");
                        const color = risk === "high" ? "#ef4444" : risk === "medium" ? "#f59e0b" : "#22c55e";
                        return (
                          <tr key={String(pipe.pipelineId || "")} style={{borderTop:"1px solid #e2e8f0"}}>
                            <td style={{padding:"6px 10px",fontWeight:500}}>{String(pipe.name || pipe.pipelineId || "")}</td>
                            <td style={{padding:"6px 10px",color:"#64748b",fontSize:11}}>{String(pipe.type || "")}</td>
                            <td style={{padding:"6px 10px",color:"#64748b",fontSize:11}}>{String(pipe.role || pipe.registryStatus || "")}</td>
                            <td style={{padding:"6px 10px",color,fontWeight:600}}>{risk}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                ) : <p style={{color:"#64748b",padding:"0 16px 16px"}}>Run hermes_pipeline_audit.py to generate pipeline health data.</p>}
              </div>
            </div>

            {/* ---------- Source Quality ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Source Quality</h2></div></div>
              <div style={{ padding: 16 }}>
                {(hermesSources as Record<string,unknown>)?.summary ? (
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, marginBottom: 12 }}>
                    <div className="metric-chip"><span>Total</span><strong>{String((hermesSources as Record<string,unknown>).summary?.totalSources || 0)}</strong></div>
                    <div className="metric-chip"><span>Watch</span><strong style={{color:"#f59e0b"}}>{String((hermesSources as Record<string,unknown>).summary?.watch || 0)}</strong></div>
                    <div className="metric-chip"><span>Degraded</span><strong style={{color:"#ef4444"}}>{String((hermesSources as Record<string,unknown>).summary?.degraded || 0)}</strong></div>
                    <div className="metric-chip"><span>High Risk</span><strong style={{color:"#ef4444"}}>{String((hermesSources as Record<string,unknown>).summary?.highRisk || 0)}</strong></div>
                  </div>
                ) : null}
                {((hermesSources as Record<string,unknown>)?.sources as unknown[])?.length > 0 ? (
                  <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
                    <thead><tr style={{background:"#f8fafc"}}><th style={{padding:"6px 10px",textAlign:"left"}}>Source</th><th style={{padding:"6px 10px"}}>Type</th><th style={{padding:"6px 10px"}}>Status</th><th style={{padding:"6px 10px"}}>Score</th></tr></thead>
                    <tbody>
                      {((hermesSources as Record<string,unknown>)?.sources as unknown[]).slice(0, 8).map((s: unknown) => {
                        const src = s as Record<string,unknown>;
                        const status = String(src.status || "?");
                        const color = status === "degraded" ? "#ef4444" : status === "watch" ? "#f59e0b" : "#22c55e";
                        return (
                          <tr key={String(src.sourceId || "")} style={{borderTop:"1px solid #e2e8f0",cursor:"pointer"}}
                            onClick={() => {
                              setSelectedSource(src);
                              setSourceDetailOpen(true);
                              api.hermesSourceDetail(String(src.sourceId || "")).then(setSourceDetail).catch(() => setSourceDetail(null));
                            }}>
                            <td style={{padding:"6px 10px",fontWeight:500,color:"#3b82f6"}}>{String(src.name || src.sourceId || "")}</td>
                            <td style={{padding:"6px 10px",color:"#64748b",fontSize:11}}>{String(src.sourceType || "")}</td>
                            <td style={{padding:"6px 10px",color,fontWeight:600}}>{status}</td>
                            <td style={{padding:"6px 10px",color: (src.qualityScore as number) < 40 ? "#ef4444" : (src.qualityScore as number) < 70 ? "#f59e0b" : "#22c55e",fontWeight:600}}>{String(src.qualityScore || "?")}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                ) : <p style={{color:"#64748b",padding:"0 16px 16px"}}>Run hermes_source_quality.py to generate source quality data.</p>}

                {/* Source Detail Panel */}
                {sourceDetailOpen && selectedSource ? (
                  <div style={{margin:"0 16px 16px",background:"#f8fafc",borderRadius:8,padding:16,border:"1px solid #e2e8f0"}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:12}}>
                      <strong style={{fontSize:14}}>{String(selectedSource.name || selectedSource.sourceId)}</strong>
                      <button type="button" className="btn btn-sm btn-ghost" onClick={() => {setSourceDetailOpen(false);setSourceDetail(null);}}>Close</button>
                    </div>
                    {sourceDetail ? (
                      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,fontSize:12}}>
                        <div>
                          <div style={{marginBottom:8}}><strong>Type:</strong> {String(selectedSource.sourceType)} | <strong>Country:</strong> {String(selectedSource.country || "?")}</div>
                          <div style={{marginBottom:8}}><strong>Status:</strong> {String(selectedSource.status)} | <strong>Governance:</strong> {String(selectedSource.governanceStatus)}</div>
                          <div style={{marginBottom:8}}><strong>Path:</strong> <span style={{fontFamily:"monospace",fontSize:11}}>{String(selectedSource.path || "?")}</span></div>
                          {String((selectedSource as Record<string,unknown>).notes || "") && <div style={{marginBottom:8,color:"#64748b"}}><strong>Notes:</strong> {String((selectedSource as Record<string,unknown>).notes)}</div>}
                          <div style={{marginBottom:8}}><strong>Linked Evidence:</strong> {(sourceDetail.linkedEvidenceCount as number) || 0} records</div>
                          {((sourceDetail.linkedPipelines as unknown[]) || []).length > 0 && (
                            <div style={{marginBottom:8}}><strong>Pipelines:</strong> {(sourceDetail.linkedPipelines as unknown[]).map((p: unknown) => String((p as Record<string,unknown>).name || (p as Record<string,unknown>).pipelineId)).join(", ")}</div>
                          )}
                        </div>
                        <div>
                          {(selectedSource.knownIssues as unknown[])?.length > 0 && (
                            <div style={{marginBottom:8}}>
                              <strong style={{color:"#ef4444"}}>Known Issues:</strong>
                              {(selectedSource.knownIssues as unknown[]).map((issue: unknown, i: number) => (
                                <div key={i} style={{color:"#ef4444",fontSize:11,marginTop:2}}>{String(issue)}</div>
                              ))}
                            </div>
                          )}
                          {((sourceDetail.linkedEvidence as unknown[]) || []).length > 0 && (
                            <div>
                              <strong>Evidence Records:</strong>
                              {(sourceDetail.linkedEvidence as unknown[]).slice(0, 3).map((e: unknown, i: number) => {
                                const ev = e as Record<string,unknown>;
                                return <div key={i} style={{fontSize:11,color:"#475569",marginTop:4,padding:"4px 8px",background:"#fff",borderRadius:4}}>{String(ev.claim || ev.evidenceType || "").slice(0, 100)}</div>;
                              })}
                            </div>
                          )}
                        </div>
                      </div>
                    ) : <p style={{color:"#64748b"}}>Loading source details...</p>}
                  </div>
                ) : null}
              </div>
            </div>

            {/* ---------- Cost ---------- */}
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Cost Report</h2></div></div>
              <div style={{ padding: 16 }}>
                {(hermesCost as Record<string,unknown>)?.summary ? (
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 12, marginBottom: 12 }}>
                    <div className="metric-chip"><span>Total Cost</span><strong>{(hermesCost as Record<string,unknown>).summary?.totalEstimatedCostCny as number ?? 0} CNY</strong></div>
                    <div className="metric-chip"><span>Budget</span><strong>{(hermesCost as Record<string,unknown>).summary?.budgetCny as number ?? 500} CNY</strong></div>
                    <div className="metric-chip"><span>Status</span><strong style={{color: (hermesCost as Record<string,unknown>).summary?.budgetStatus === "exceeded" ? "#ef4444" : (hermesCost as Record<string,unknown>).summary?.budgetStatus === "warning" ? "#f59e0b" : "#22c55e"}}>{String((hermesCost as Record<string,unknown>).summary?.budgetStatus || "ok")}</strong></div>
                    <div className="metric-chip"><span>Cache Hit</span><strong>{String(Math.round(((hermesCost as Record<string,unknown>).summary?.cacheHitRatio as number || 0) * 100))}%</strong></div>
                  </div>
                ) : null}
                {((hermesCost as Record<string,unknown>)?.byModel as Record<string,unknown>) ? (
                  <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
                    <thead><tr style={{background:"#f8fafc"}}><th style={{padding:"6px 10px",textAlign:"left"}}>Model</th><th style={{padding:"6px 10px"}}>Records</th><th style={{padding:"6px 10px"}}>Input Tokens</th><th style={{padding:"6px 10px"}}>Cost (CNY)</th></tr></thead>
                    <tbody>
                      {Object.entries((hermesCost as Record<string,unknown>).byModel as Record<string,unknown>).map(([model, data]) => {
                        const d = data as Record<string,unknown>;
                        return (
                          <tr key={model} style={{borderTop:"1px solid #e2e8f0"}}>
                            <td style={{padding:"6px 10px",fontWeight:500}}>{model}</td>
                            <td style={{padding:"6px 10px"}}>{String(d.records || 0)}</td>
                            <td style={{padding:"6px 10px"}}>{Number(d.inputTokens || 0).toLocaleString()}</td>
                            <td style={{padding:"6px 10px",fontWeight:600}}>{Number(d.estimatedCostCny || 0).toFixed(4)}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                ) : <p style={{color:"#64748b",padding:"0 16px 16px"}}>Run hermes_cost_report.py to generate cost data.</p>}
              </div>
            </div>

            {/* ---------- 已完成 (Proposals + Gaps) ---------- */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
              <div className="card crud-card">
                <div className="admin-card-header"><div><h2>Proposals (已实施 vs 待处理)</h2></div></div>
                <div style={{ padding: 16 }}>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(2,1fr)", gap: 12, marginBottom: 12 }}>
                    <div className="metric-chip"><span>Total</span><strong>{(hermesOverview?.proposals as Record<string,number>)?.total || 0}</strong></div>
                    <div className="metric-chip"><span style={{color:"#22c55e"}}>Implemented</span><strong style={{color:"#22c55e"}}>{(hermesOverview?.proposals as Record<string,number>)?.implemented || 0}</strong></div>
                    <div className="metric-chip"><span style={{color:"#f59e0b"}}>Pending Review</span><strong style={{color:"#f59e0b"}}>{(hermesOverview?.proposals as Record<string,number>)?.pending || 0}</strong></div>
                    <div className="metric-chip"><span>Draft</span><strong>{(hermesOverview?.proposals as Record<string,number>)?.draft || 0}</strong></div>
                  </div>
                  {hermesProposals.length > 0 ? (
                    <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                      <thead><tr style={{background:"#f8fafc"}}><th style={{padding:"4px 8px",textAlign:"left"}}>Proposal</th><th style={{padding:"4px 8px"}}>Status</th></tr></thead>
                      <tbody>
                        {hermesProposals.slice(0, 10).map((p: unknown) => {
                          const prop = p as Record<string,unknown>;
                          const st = String(prop.status || "");
                          const color = st === "implemented" ? "#22c55e" : st === "pending_review" ? "#f59e0b" : "#94a3b8";
                          return (
                            <tr key={String(prop.proposalId || "")} style={{borderTop:"1px solid #e2e8f0"}}>
                              <td style={{padding:"4px 8px"}}>{String(prop.title || prop.proposalId || "").slice(0, 60)}</td>
                              <td style={{padding:"4px 8px",color,fontWeight:600,fontSize:11}}>{st}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  ) : null}
                </div>
              </div>

              <div className="card crud-card">
                <div className="admin-card-header"><div><h2>Governance Gaps (已解决 vs 未解决)</h2></div></div>
                <div style={{ padding: 16 }}>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(2,1fr)", gap: 12, marginBottom: 12 }}>
                    <div className="metric-chip"><span>Total</span><strong>{(hermesOverview?.gaps as Record<string,number>)?.total || 0}</strong></div>
                    <div className="metric-chip"><span style={{color:"#22c55e"}}>Resolved</span><strong style={{color:"#22c55e"}}>{(hermesOverview?.gaps as Record<string,number>)?.resolved || 0}</strong></div>
                    <div className="metric-chip"><span style={{color:"#ef4444"}}>Open</span><strong style={{color:"#ef4444"}}>{(hermesOverview?.gaps as Record<string,number>)?.open || 0}</strong></div>
                    <div className="metric-chip"><span>In Progress</span><strong>{(hermesOverview?.gaps as Record<string,number>)?.total - (hermesOverview?.gaps as Record<string,number>)?.open - (hermesOverview?.gaps as Record<string,number>)?.resolved || 0}</strong></div>
                  </div>
                  {hermesFeatures.length > 0 ? (
                    <table style={{ width: "100%", fontSize: 12, borderCollapse: "collapse" }}>
                      <thead><tr style={{background:"#f8fafc"}}><th style={{padding:"4px 8px",textAlign:"left"}}>Feature</th><th style={{padding:"4px 8px"}}>Status</th></tr></thead>
                      <tbody>
                        {hermesFeatures.slice(0, 10).map((f: unknown) => {
                          const feat = f as Record<string,unknown>;
                          const st = String(feat.status || "");
                          const color = st === "active" ? "#22c55e" : st === "beta" ? "#3b82f6" : st === "archived" ? "#94a3b8" : "#f59e0b";
                          return (
                            <tr key={String(feat.featureId || "")} style={{borderTop:"1px solid #e2e8f0"}}>
                              <td style={{padding:"4px 8px"}}>{String(feat.name || feat.featureId || "").slice(0, 55)}</td>
                              <td style={{padding:"4px 8px",color,fontWeight:600,fontSize:11}}>{st}</td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  ) : null}
                </div>
              </div>
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

      {subpage !== "hermes" && overview ? (
        <>
          {/* Hermes summary strip — always visible on Overview & VOC */}
          {subpage === "overview" && (
            <div className="card crud-card">
              <div className="admin-card-header"><div><h2>Hermes Governance Snapshot <span style={{fontSize:12,fontWeight:400,color:"#64748b"}}>— full details in Hermes tab</span></h2></div></div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(6,1fr)", gap: 12, padding: 16 }}>
                <div className="metric-chip"><span>Pipelines</span><strong style={{color: (hermesPipelines as Record<string,unknown>)?.summary?.highRiskFindings > 0 ? "#ef4444" : "#22c55e"}}>{(hermesOverview?.registries as Record<string,number>)?.pipeline || "?"}</strong></div>
                <div className="metric-chip"><span>Sources</span><strong style={{color: (hermesSources as Record<string,unknown>)?.summary?.degraded > 0 ? "#ef4444" : "#22c55e"}}>{(hermesOverview?.registries as Record<string,number>)?.source || "?"}</strong></div>
                <div className="metric-chip"><span>Features</span><strong>{(hermesOverview?.registries as Record<string,number>)?.feature || "?"}</strong></div>
                <div className="metric-chip"><span>Proposals Done</span><strong style={{color:"#22c55e"}}>{(hermesOverview?.proposals as Record<string,number>)?.implemented || 0}/{(hermesOverview?.proposals as Record<string,number>)?.total || 0}</strong></div>
                <div className="metric-chip"><span>Gaps Open</span><strong style={{color: (hermesOverview?.gaps as Record<string,number>)?.open > 0 ? "#ef4444" : "#22c55e"}}>{(hermesOverview?.gaps as Record<string,number>)?.open || 0}</strong></div>
                <div className="metric-chip"><span>Cost</span><strong style={{color: (hermesCost as Record<string,unknown>)?.summary?.budgetStatus === "exceeded" ? "#ef4444" : "#22c55e"}}>{(hermesCost as Record<string,unknown>)?.summary?.totalEstimatedCostCny as number ?? 0} CNY</strong></div>
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
          <AdminToolsNav />
        </>
      ) : null}
    </section>
  );
}
