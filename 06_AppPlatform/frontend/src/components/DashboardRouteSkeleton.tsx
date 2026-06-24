const FILTER_LABELS = ["Country", "Body type", "Segment", "Powertrain", "Make"];
const KPI_LABELS = ["Total sales", "Version count"];

interface DashboardRouteSkeletonProps {
  chartKicker?: string;
  chartTitle?: string;
  heroKicker?: string;
  title?: string;
}

export function DashboardRouteSkeleton({
  chartKicker = "03 / Time Series",
  chartTitle = "Sales trend loading",
  heroKicker = "01 / Market Overview",
  title = "Dashboard Control View",
}: DashboardRouteSkeletonProps) {
  return (
    <div className="dashboard-layout dashboard-route-skeleton" role="status" aria-live="polite">
      <aside className="filter-sidebar dashboard-route-skeleton-sidebar">
        <div className="filter-sidebar-rail">
          <div className="filter-sidebar-rail-copy">
            <span className="page-kicker">01 / Filter Stack</span>
            <strong className="filter-sidebar-rail-title">全维度筛选</strong>
            <span className="dashboard-sidebar-caption">同步筛选元数据与默认市场范围</span>
          </div>
        </div>
        <div className="filter-card filter-summary-card dashboard-route-skeleton-summary">
          <div className="kpi-card">
            <span className="kpi-label">筛选后记录数</span>
            <span className="dashboard-skeleton-line dashboard-skeleton-line--value" />
          </div>
          <div className="kpi-card">
            <span className="kpi-label">品牌数</span>
            <span className="dashboard-skeleton-line dashboard-skeleton-line--value" />
          </div>
        </div>
        {FILTER_LABELS.map((label) => (
          <div key={label} className="dashboard-route-skeleton-filter">
            <span className="kpi-label">{label}</span>
            <span className="dashboard-skeleton-line" />
          </div>
        ))}
      </aside>

      <section className="dashboard-main dashboard-route-skeleton-main">
        <div className="header-card dashboard-hero dashboard-route-skeleton-hero">
          <div className="dashboard-hero-head">
            <div className="dashboard-hero-copy">
              <span className="page-kicker">{heroKicker}</span>
              <h1>{title}</h1>
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Active lens</span>
                <span className="selection-ribbon-value">Loading default powertrain scope...</span>
              </div>
            </div>
            <div className="dashboard-hero-actions">
              {KPI_LABELS.map((label) => (
                <div key={label} className="hero-meta-block hero-meta-block-immersive is-loading">
                  <span className="hero-meta-label">{label}</span>
                  <span className="dashboard-skeleton-line dashboard-skeleton-line--hero" />
                  <span className="hero-meta-subvalue">Edge cache warm path</span>
                </div>
              ))}
            </div>
          </div>
          <div className="dashboard-hero-rail">
            <div className="dashboard-hero-chip-row">
              {["Country", "Powertrain", "Time axis"].map((label) => (
                <span key={label} className="dashboard-hero-chip">{label}</span>
              ))}
            </div>
          </div>
        </div>

        <div className="card dashboard-route-skeleton-card">
          <div className="dashboard-route-skeleton-card-head">
            <div>
              <span className="panel-kicker">02 / Global Time Axis</span>
              <h3>Global Time Axis</h3>
            </div>
            <span className="dashboard-skeleton-line dashboard-skeleton-line--short" />
          </div>
          <span className="dashboard-skeleton-line dashboard-skeleton-line--wide" />
        </div>

        <div className="card dashboard-route-skeleton-chart">
          <div className="chart-header">
            <div>
              <span className="panel-kicker">{chartKicker}</span>
              <h3>{chartTitle}</h3>
            </div>
          </div>
          <div className="dashboard-route-skeleton-chart-box">
            <span className="dashboard-skeleton-line dashboard-skeleton-line--chart" />
          </div>
        </div>
      </section>
    </div>
  );
}
