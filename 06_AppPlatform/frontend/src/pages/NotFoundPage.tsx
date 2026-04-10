import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <section className="crud-shell not-found-shell">
      <div className="card analysis-deck-card not-found-card">
        <div className="analysis-deck-head">
          <div className="analysis-deck-copy">
            <span className="panel-kicker">04 / Route Fallback</span>
            <h1>Page Not Found</h1>
            <p>当前地址没有映射到可用工作视图。使用下面的入口返回 Dashboard、Specification 或 CRUD。</p>
            <div className="analysis-chip-row">
              <span className="analysis-chip">React Router fallback</span>
              <span className="analysis-chip">404 handled in shell</span>
            </div>
          </div>
          <div className="analysis-deck-meta">
            <div className="analysis-deck-stat">
              <span className="analysis-deck-stat-label">Route State</span>
              <strong className="analysis-deck-stat-value">404</strong>
              <span className="analysis-deck-stat-subvalue">无匹配工作视图</span>
            </div>
          </div>
        </div>

        <div className="analysis-chart-block analysis-chart-block--compact not-found-actions">
          <Link className="btn btn-primary" to="/">返回 Dashboard</Link>
          <Link className="btn btn-secondary" to="/specification">打开 Specification</Link>
          <Link className="btn btn-ghost" to="/crud">打开 CRUD</Link>
        </div>
      </div>
    </section>
  );
}
