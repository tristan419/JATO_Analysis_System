import {
  useEffect,
  useRef,
} from "react";
import { Link } from "react-router-dom";

import { CountryChatAnalysisDeck } from "../components/CountryChatAnalysisDeck";
import { CountryChatModelSelect } from "../components/CountryChatModelSelect";
import { LoadingSurface } from "../components/LoadingSurface";
import { useCountryChat } from "../contexts/CountryChatContext";

function formatNumber(value: number | undefined): string {
  if (value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toLocaleString("en-US");
}

function formatDateTime(value: string | null | undefined): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "-";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

export function CountryChatPage() {
  const {
    draft,
    error,
    latestResponse,
    loadingMetadata,
    loadingNewsStatus,
    messages,
    metadata,
    newsStatus,
    promptSuggestions,
    providerSummary,
    refreshingNews,
    refreshCountryNews,
    retryLatestQuestionWithFreshNews,
    selectedCountry,
    sending,
    setDraft,
    setSelectedCountry,
    sendQuestion,
  } = useCountryChat();
  const transcriptEndRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    transcriptEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, sending]);

  if (loadingMetadata && !metadata) {
    return (
      <div className="dashboard-shell copilot-shell">
        <LoadingSurface
          mode="overlay"
          kicker="Copilot"
          label="正在准备国家聊天助手"
          detail="读取国家列表、提供方状态与默认提示"
        />
      </div>
    );
  }

  const snapshot = latestResponse?.contextSnapshot ?? null;
  const countryOptions = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];
  return (
    <div className="dashboard-shell copilot-shell">
      <section className="content copilot-content">
        <div className="header-card dashboard-hero copilot-hero">
            <div className="dashboard-hero-head">
              <div className="dashboard-hero-copy">
                <span className="page-kicker">08 / Country Copilot</span>
                <h1>国家数据聊天助手</h1>
              <div className="dashboard-hero-inline-summary">
                <span className="selection-ribbon-label">Current mode</span>
                <span className="selection-ribbon-value">按国家读取快照，理解用户问题并回答</span>
              </div>
            </div>
            <div className="dashboard-hero-actions copilot-hero-actions">
              <span className={`copilot-status-badge${metadata?.providerAvailable ? " is-ready" : " is-fallback"}`}>
                {providerSummary}
              </span>
            </div>
          </div>
            <div className="dashboard-hero-rail">
              <div className="dashboard-hero-chip-row">
                <span className="dashboard-hero-chip">国家维度回答</span>
                <span className="dashboard-hero-chip">复用 overview 聚合数据</span>
                <span className="dashboard-hero-chip">自动轮换聊天模型</span>
              </div>
            </div>
          </div>

        <div className="copilot-grid">
          <div className="card copilot-chat-card">
            <div className="copilot-toolbar">
              <div className="copilot-toolbar-controls">
                <label className="copilot-field">
                  <span>国家</span>
                  <select
                    value={selectedCountry}
                    onChange={(event) => setSelectedCountry(event.target.value)}
                    disabled={sending}
                  >
                    {countryOptions.map((item) => (
                      <option key={item.value} value={item.value}>
                        {item.label}
                      </option>
                    ))}
                  </select>
                </label>
                <CountryChatModelSelect />
              </div>
              <div className="copilot-toolbar-session">
                <div className="copilot-toolbar-note">
                  当前页面与右下角悬浮助手共享同一套会话，切换页面后会按国家和聊天模型恢复历史记录。
                </div>
                <div className="copilot-toolbar-meta">
                  <span className="copilot-ops-pill">Page + Widget Shared</span>
                  <span className="copilot-ops-pill">{providerSummary}</span>
                </div>
              </div>
            </div>

            <div className="copilot-suggestion-row">
              {promptSuggestions.map((prompt) => (
                <button
                  key={prompt}
                  type="button"
                  className="btn btn-sm btn-secondary"
                  onClick={() => setDraft(prompt)}
                >
                  {prompt}
                </button>
              ))}
            </div>

            <div className="copilot-transcript">
              {messages.length === 0 ? (
                <div className="copilot-empty-state">
                  <h3>还没有对话</h3>
                  <p>
                    先选一个国家，再问类似“这个国家最近几年销量趋势怎么样？”的问题。
                  </p>
                </div>
              ) : (
                messages.map((message) => (
                  <article
                    key={message.id}
                    className={`copilot-message copilot-message--${message.role}`}
                  >
                    <div className="copilot-message-meta">
                      <span>{message.role === "user" ? "你" : "助手"}</span>
                      {message.provider ? (
                        <span>
                          {[message.provider, message.model].filter(Boolean).join(" · ")}
                        </span>
                      ) : null}
                    </div>
                    <div className="copilot-message-body">
                      {message.content}
                    </div>
                    {message.contextSnapshot ? (
                      <CountryChatAnalysisDeck
                        message={message}
                        defaultExpanded={false}
                      />
                    ) : null}
                    {message.providerReason ? (
                      <div className="copilot-message-note">{message.providerReason}</div>
                    ) : null}
                  </article>
                ))
              )}
              {sending ? (
                <article className="copilot-message copilot-message--assistant is-pending">
                  <div className="copilot-message-meta">
                    <span>助手</span>
                    <span>thinking</span>
                  </div>
                  <div className="copilot-message-body">正在读取国家快照并整理回答…</div>
                </article>
              ) : null}
              <div ref={transcriptEndRef} />
            </div>

            <div className="copilot-composer">
              <label className="copilot-field copilot-field--stacked">
                <span>你的问题</span>
                <textarea
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  placeholder="例如：这个国家的动力结构有什么特点？"
                  rows={4}
                  disabled={sending || !selectedCountry}
                />
              </label>
              <div className="copilot-composer-actions">
                {error ? <span className="copilot-error">{error}</span> : null}
                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={() => {
                    void sendQuestion();
                  }}
                  disabled={sending || !selectedCountry || !draft.trim()}
                >
                  {sending ? "发送中…" : "发送问题"}
                </button>
              </div>
            </div>
          </div>

          <aside className="copilot-side-panel">
            <div className="card copilot-side-card">
              <h3>当前上下文</h3>
              <p>
                {snapshot
                  ? `${snapshot.country} 的聚合快照已经就绪。`
                  : "发送第一条消息后，这里会展示当前国家摘要。"}
              </p>
              {snapshot ? (
                <div className="copilot-kpi-grid">
                  <div>
                    <span>品牌数</span>
                    <strong>{formatNumber(snapshot.kpis.brandCount)}</strong>
                  </div>
                  <div>
                    <span>车型数</span>
                    <strong>{formatNumber(snapshot.kpis.modelCount)}</strong>
                  </div>
                  <div>
                    <span>版本数</span>
                    <strong>{formatNumber(snapshot.kpis.versionCount)}</strong>
                  </div>
                  <div>
                    <span>累计销量</span>
                    <strong>{formatNumber(snapshot.kpis.cumulativeSales)}</strong>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="card copilot-side-card">
              <h3>新闻运维状态</h3>
              {loadingNewsStatus ? (
                <p>正在读取当前国家的新闻同步状态…</p>
              ) : newsStatus ? (
                <>
                  <div className="copilot-kpi-grid copilot-ops-grid">
                    <div>
                      <span>快照状态</span>
                      <strong>{newsStatus.hasSnapshot ? "已就绪" : "尚无快照"}</strong>
                    </div>
                    <div>
                      <span>同步时间</span>
                      <strong>{formatDateTime(newsStatus.syncTimestamp)}</strong>
                    </div>
                    <div>
                      <span>摘要 Provider</span>
                      <strong>{newsStatus.summaryProvider ?? "rss-fallback"}</strong>
                    </div>
                    <div>
                      <span>Stale</span>
                      <strong>{newsStatus.stale ? "是" : "否"}</strong>
                    </div>
                    <div>
                      <span>文章数</span>
                      <strong>{formatNumber(newsStatus.articleCount)}</strong>
                    </div>
                    <div>
                      <span>Source feeds</span>
                      <strong>{formatNumber(newsStatus.feedCount)}</strong>
                    </div>
                  </div>

                  <div className="copilot-ops-actions">
                    <button
                      type="button"
                      className="btn btn-sm btn-secondary"
                      onClick={() => {
                        void refreshCountryNews();
                      }}
                      disabled={refreshingNews || !selectedCountry}
                    >
                      {refreshingNews ? "在线刷新中…" : "在线刷新新闻快照"}
                    </button>
                    <button
                      type="button"
                      className="btn btn-sm btn-primary"
                      onClick={() => {
                        void retryLatestQuestionWithFreshNews();
                      }}
                      disabled={refreshingNews || sending || !latestResponse}
                    >
                      不满意时刷新后重答
                    </button>
                  </div>

                  <p className="copilot-toolbar-note">
                    默认问答优先读数据库快照；如果结果不满意，可以临时在线抓取最新新闻，
                    再由当前选中的聊天模型基于更新后的上下文重答。新闻层仍然保持
                    RSS/Atom 抓取，Gemini 可用于新闻摘要增强。
                  </p>

                  {newsStatus.providerRoles &&
                  newsStatus.providerRoles.length > 0 ? (
                    <ul className="copilot-ops-list">
                      {newsStatus.providerRoles.map((role) => (
                        <li key={`${role.capability}-${role.provider}`}>
                          <strong>{role.capability}</strong>
                          <span>
                            {role.provider}
                            {role.model ? ` · ${role.model}` : ""}
                            {role.mode ? ` · ${role.mode}` : ""}
                          </span>
                        </li>
                      ))}
                    </ul>
                  ) : null}
                </>
              ) : (
                <p>当前国家还没有新闻同步信息。</p>
              )}
            </div>

            {snapshot?.insightCards && snapshot.insightCards.length > 0 ? (
              <div className="card copilot-side-card">
                <h3>分析洞察</h3>
                <ul className="copilot-insight-list">
                  {snapshot.insightCards.map((card) => (
                    <li key={card.title} className="copilot-insight-item">
                      <span className={`copilot-insight-tone copilot-insight-tone--${card.tone}`} />
                      <div className="copilot-insight-body">
                        <strong>{card.title}</strong>
                        <p>{card.conclusion}</p>
                        {card.relatedChartLink ? (
                          <Link to={card.relatedChartLink} className="copilot-insight-link">
                            查看图表 →
                          </Link>
                        ) : null}
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </aside>
        </div>
      </section>
    </div>
  );
}
