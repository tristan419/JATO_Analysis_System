import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { Link } from "react-router-dom";

import { CountryChatAnalysisDeck } from "../components/CountryChatAnalysisDeck";
import { CountryChatGroundedAnswer } from "../components/CountryChatGroundedAnswer";
import { CountryChatPendingMessage } from "../components/CountryChatPendingMessage";
import { ChatInlineCharts } from "../components/ChatInlineCharts";
import { CountryChatModelSelect } from "../components/CountryChatModelSelect";
import { LoadingSurface } from "../components/LoadingSurface";
import {
  buildCountryChatHandoffSearch,
  isCountryChatMobileAccess,
} from "../contexts/countryChatHelpers";
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

function formatProviderMeta(provider: string | null | undefined, model: string | null | undefined): string {
  const normalizedProvider = String(provider ?? "").trim();
  const providerLabel = {
    fallback: "证据兜底",
    "external-search": "检索证据",
    snapshot: "快照证据",
    deepseek: "DeepSeek",
    gemini: "Gemini",
    nvidia: "NVIDIA",
  }[normalizedProvider] ?? normalizedProvider;
  return [providerLabel, model].filter(Boolean).join(" · ");
}

function useIsMobileAccess(maxWidth = 720) {
  const [isMobileAccess, setIsMobileAccess] = useState(
    () => isCountryChatMobileAccess(
      window.innerWidth,
      window.matchMedia("(pointer: coarse)").matches,
      maxWidth,
    ),
  );

  useEffect(() => {
    const coarsePointerMedia = window.matchMedia("(pointer: coarse)");
    function handleResize() {
      setIsMobileAccess(
        isCountryChatMobileAccess(
          window.innerWidth,
          coarsePointerMedia.matches,
          maxWidth,
        ),
      );
    }

    handleResize();
    window.addEventListener("resize", handleResize);
    coarsePointerMedia.addEventListener("change", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
      coarsePointerMedia.removeEventListener("change", handleResize);
    };
  }, [maxWidth]);

  return isMobileAccess;
}

function CopilotSideSection({
  title,
  mobile,
  defaultOpen = false,
  children,
}: {
  title: string;
  mobile: boolean;
  defaultOpen?: boolean;
  children: ReactNode;
}) {
  if (!mobile) {
    return (
      <div className="card copilot-side-card">
        <h3>{title}</h3>
        {children}
      </div>
    );
  }

  return (
    <details className="card copilot-side-card copilot-mobile-section" open={defaultOpen}>
      <summary className="copilot-mobile-section-summary">
        <span>{title}</span>
        <span>展开</span>
      </summary>
      <div className="copilot-mobile-section-body">{children}</div>
    </details>
  );
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
    selectedChatModel,
    sending,
    setDraft,
    setSelectedCountry,
    sendQuestion,
  } = useCountryChat();
  const transcriptEndRef = useRef<HTMLDivElement | null>(null);
  const isMobileAccess = useIsMobileAccess();
  const [handoffFeedback, setHandoffFeedback] = useState("");

  useEffect(() => {
    transcriptEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, sending]);

  const snapshot = latestResponse?.contextSnapshot ?? null;
  const pendingQuestion = sending
    ? [...messages]
      .reverse()
      .find((message) => message.role === "user")
      ?.content
      ?? draft
    : "";
  const countryOptions = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];
  const lastUserQuestion = useMemo(
    () => [...messages]
      .reverse()
      .find((message) => message.role === "user")
      ?.content
      ?? "",
    [messages],
  );
  const handoffQuestion = String(
    draft.trim()
    || latestResponse?.question
    || lastUserQuestion,
  ).trim();
  const handoffUrl = useMemo(() => {
    const search = buildCountryChatHandoffSearch({
      country: selectedCountry,
      chatModel: selectedChatModel,
      question: handoffQuestion,
    });
    return `${window.location.origin}/copilot${search}`;
  }, [handoffQuestion, selectedChatModel, selectedCountry]);
  const hasConversation = messages.length > 0 || sending;
  const visiblePromptSuggestions = isMobileAccess
    ? promptSuggestions.slice(0, 4)
    : promptSuggestions;
  const showPromptSuggestions = visiblePromptSuggestions.length > 0 && (!isMobileAccess || !hasConversation);
  const showMobileQuickActions = isMobileAccess && Boolean(handoffQuestion);

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

  async function copyDesktopHandoffLink() {
    if (!handoffQuestion || !navigator.clipboard?.writeText) {
      setHandoffFeedback("当前浏览器不支持复制，请直接把地址栏链接发到桌面。");
      return;
    }
    try {
      await navigator.clipboard.writeText(handoffUrl);
      setHandoffFeedback("桌面接力链接已复制。");
    } catch (reason: unknown) {
      setHandoffFeedback(
        reason instanceof Error
          ? reason.message
          : "复制失败，请直接把地址栏链接发到桌面。",
      );
    }
  }

  const contextPanel = (
    <>
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
    </>
  );

  const newsPanel = (
    <>
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
    </>
  );

  const insightsPanel = snapshot?.insightCards && snapshot.insightCards.length > 0 ? (
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
  ) : null;

  return (
    <div className={`dashboard-shell copilot-shell${isMobileAccess ? " is-mobile-access" : ""}`}>
      <section className="content copilot-content">
        {isMobileAccess ? (
          <div className="header-card dashboard-hero copilot-hero copilot-hero--mobile">
            <div className="copilot-mobile-hero-head">
              <div className="copilot-mobile-hero-copy">
                <span className="page-kicker">Country Copilot</span>
                <h1>国家助手</h1>
                <p>先给结论，再补证据，需要时再接力桌面。</p>
              </div>
              <span className={`copilot-status-badge${metadata?.providerAvailable ? " is-ready" : " is-fallback"}`}>
                {providerSummary}
              </span>
            </div>
            <div className="copilot-mobile-hero-meta">
              <span className="copilot-ops-pill">{selectedCountry || "选择国家"}</span>
              <span className="copilot-ops-pill">Answer first</span>
              <span className="copilot-ops-pill">Desktop handoff</span>
            </div>
          </div>
        ) : (
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
        )}

        <div className="copilot-grid">
          <div className={`card copilot-chat-card${isMobileAccess ? " is-mobile" : ""}`}>
            <div className="copilot-toolbar">
              {isMobileAccess ? (
                <>
                  <div className="copilot-mobile-toolbar-controls">
                    <select
                      className="ccw-country-select"
                      value={selectedCountry}
                      onChange={(event) => setSelectedCountry(event.target.value)}
                      disabled={sending}
                      aria-label="选择国家"
                    >
                      {countryOptions.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                    <CountryChatModelSelect compact />
                  </div>
                  <details className="copilot-mobile-disclosure">
                    <summary className="copilot-mobile-disclosure-summary">
                      手机模式说明
                    </summary>
                    <div className="copilot-mobile-disclosure-body">
                      手机端优先看答案；需要完整图表或处理台时，再把当前问题接力到桌面。
                    </div>
                  </details>
                </>
              ) : (
                <>
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
                </>
              )}
            </div>

            {showPromptSuggestions ? (
              <div className={`copilot-prompt-block${isMobileAccess ? " is-mobile" : ""}`}>
                {isMobileAccess ? (
                  <div className="copilot-prompt-block-head">
                    <strong>试试这样问</strong>
                    <span>高频手机入口</span>
                  </div>
                ) : null}
                <div className="copilot-suggestion-row">
                  {visiblePromptSuggestions.map((prompt) => (
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
              </div>
            ) : null}

            {showMobileQuickActions ? (
              <div className="copilot-mobile-quick-actions">
                <button
                  type="button"
                  className="btn btn-sm btn-primary"
                  onClick={() => {
                    void copyDesktopHandoffLink();
                  }}
                  disabled={!handoffQuestion}
                >
                  复制桌面接力
                </button>
                <a
                  href={handoffUrl}
                  className="btn btn-sm btn-secondary"
                  target="_blank"
                  rel="noreferrer"
                >
                  桌面工作台
                </a>
              </div>
            ) : null}
            {handoffFeedback ? (
              <div className="copilot-handoff-feedback">{handoffFeedback}</div>
            ) : null}

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
                        <span>{formatProviderMeta(message.provider, message.model)}</span>
                      ) : null}
                    </div>
                    <CountryChatGroundedAnswer message={message} compact={isMobileAccess} />
                    {message.contextSnapshot ? (
                      <>
                        <ChatInlineCharts
                          snapshot={message.contextSnapshot}
                          intents={message.focusedIntents ?? message.intents}
                          renderHints={message.renderHints}
                          compact={isMobileAccess}
                        />
                        <CountryChatAnalysisDeck
                          message={message}
                          compact={isMobileAccess}
                          defaultExpanded={false}
                        />
                      </>
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
                    <span>working</span>
                  </div>
                  <CountryChatPendingMessage question={pendingQuestion} compact={isMobileAccess} />
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
                  onKeyDown={(event) => {
                    if (!isMobileAccess) {
                      return;
                    }
                    if (
                      event.key === "Enter"
                      && !event.shiftKey
                      && !event.nativeEvent.isComposing
                    ) {
                      event.preventDefault();
                      void sendQuestion();
                    }
                  }}
                  placeholder={isMobileAccess ? "直接输入问题…" : "例如：这个国家的动力结构有什么特点？"}
                  rows={isMobileAccess ? 2 : 4}
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

          {isMobileAccess ? (
            <div className="copilot-mobile-panels">
              <CopilotSideSection title="当前上下文" mobile defaultOpen={Boolean(snapshot) && messages.length === 0}>
                {contextPanel}
              </CopilotSideSection>
              <CopilotSideSection title="新闻运维状态" mobile>
                {newsPanel}
              </CopilotSideSection>
              {insightsPanel ? (
                <CopilotSideSection title="分析洞察" mobile>
                  {insightsPanel}
                </CopilotSideSection>
              ) : null}
            </div>
          ) : (
            <aside className="copilot-side-panel">
              <CopilotSideSection title="当前上下文" mobile={false}>
                {contextPanel}
              </CopilotSideSection>
              <CopilotSideSection title="新闻运维状态" mobile={false}>
                {newsPanel}
              </CopilotSideSection>
              {insightsPanel ? (
                <CopilotSideSection title="分析洞察" mobile={false}>
                  {insightsPanel}
                </CopilotSideSection>
              ) : null}
            </aside>
          )}
        </div>
      </section>
    </div>
  );
}
