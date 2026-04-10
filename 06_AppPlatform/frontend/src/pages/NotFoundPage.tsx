import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <section className="crud-shell not-found-shell">
      <div className="header-card dashboard-hero not-found-card">
        <div className="dashboard-hero-head">
          <div className="dashboard-hero-copy">
            <span className="page-kicker">04 / Route Fallback</span>
            <h1>Page Not Found</h1>
            <p>当前地址没有映射到可用工作视图。保留在同一套应用 shell 中恢复，而不是落到白页或孤立错误页。</p>
            <div className="dashboard-hero-inline-summary">
              <span className="selection-ribbon-label">Route state</span>
              <span className="selection-ribbon-value">404 handled inside app shell</span>
            </div>
          </div>
          <div className="dashboard-hero-actions">
            <div className="hero-meta-block hero-meta-block-immersive">
              <span className="hero-meta-label">Route state</span>
              <strong className="hero-meta-value">404</strong>
              <span className="hero-meta-subvalue">无匹配工作视图</span>
            </div>
            <div className="hero-meta-block hero-meta-block-immersive">
              <span className="hero-meta-label">Available views</span>
              <strong className="hero-meta-value">03</strong>
              <span className="hero-meta-subvalue">Dashboard / Specification / CRUD</span>
            </div>
          </div>
        </div>

        <div className="dashboard-hero-body">
          <div className="dashboard-hero-body-inner">
            <div className="dashboard-hero-rail not-found-actions">
              <div className="dashboard-hero-chip-row">
                <span className="dashboard-hero-chip">React Router fallback</span>
                <span className="dashboard-hero-chip">Shell-safe route recovery</span>
                <span className="dashboard-hero-chip">No blank page</span>
              </div>
              <div className="dashboard-hero-rail-actions">
                <Link className="btn btn-sm btn-primary" to="/">返回 Dashboard</Link>
                <Link className="btn btn-sm btn-secondary" to="/specification">打开 Specification</Link>
                <Link className="btn btn-sm btn-ghost" to="/crud">打开 CRUD</Link>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
