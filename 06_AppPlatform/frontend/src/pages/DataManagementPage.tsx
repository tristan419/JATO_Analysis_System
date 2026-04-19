import { useEffect, useMemo, useState, type FormEvent } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { AdminToolsNav } from "../components/AdminToolsNav";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  DataManagementAirflowStatus,
  DataManagementDomain,
  DataManagementOverviewResponse,
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

function renderDomainRecentItems(domain: DataManagementDomain) {
  if (domain.recentItems.length === 0) {
    return <div className="crud-empty-state">暂无近期记录</div>;
  }
  return (
    <div className="data-management-recent-list">
      {domain.recentItems.map((item, index) => (
        <article key={`${domain.key}-${index}`} className="data-management-recent-item">
          <div>
            <strong>{item.label}</strong>
            <span>{formatMetricValue(item.value)}</span>
          </div>
          <time>{formatDataManagementTimestamp(item.updatedAt)}</time>
        </article>
      ))}
    </div>
  );
}

export function DataManagementPage() {
  const [overview, setOverview] = useState<DataManagementOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");

  const [crudTab, setCrudTab] = useState<CrudEntityTab>("msrp-sources");
  const [crudLoading, setCrudLoading] = useState(false);
  const [crudError, setCrudError] = useState("");
  const [crudNotice, setCrudNotice] = useState("");
  const [airflowBusyAction, setAirflowBusyAction] = useState<"start" | "stop" | null>(null);
  const [airflowError, setAirflowError] = useState("");
  const [airflowNotice, setAirflowNotice] = useState("");

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
  }, []);

  useEffect(() => {
    if (overview?.database.connected) {
      void loadCrudData(crudTab);
    }
  }, [crudTab, overview?.database.connected]);

  const activityColumns = useMemo(
    () => buildActivityHeatmapColumns(overview?.activity.days ?? []),
    [overview]
  );

  const airflowStatus: DataManagementAirflowStatus | null = overview?.airflow ?? null;

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

      {loading && !overview ? (
        <LoadingSurface
          mode="overlay"
          label="正在读取数据总览"
          detail="拉取文件清单、数据库表摘要与活跃度数据"
          kicker="Data Ops"
        />
      ) : null}

      {overview ? (
        <>
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
                {renderDomainRecentItems(domain)}
              </article>
            ))}
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
