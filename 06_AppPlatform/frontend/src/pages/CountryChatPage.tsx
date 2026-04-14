import {
  useEffect,
  useMemo,
  useRef,
  useState,
  startTransition,
} from "react";

import { api } from "../api/client";
import { LoadingSurface } from "../components/LoadingSurface";
import { useSharedFilterScope } from "../contexts/SharedFilterScopeContext";
import type {
  CountryChatMetadataResponse,
  CountryChatResponse,
  CountryChatTurn,
} from "../types";

interface TranscriptMessage extends CountryChatTurn {
  id: string;
  provider?: string;
  providerReason?: string | null;
}

function formatNumber(value: number | undefined): string {
  if (value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toLocaleString("en-US");
}

export function CountryChatPage() {
  const { selections } = useSharedFilterScope();
  const preferredCountry = selections.country[0] ?? "";
  const [metadata, setMetadata] =
    useState<CountryChatMetadataResponse | null>(null);
  const [selectedCountry, setSelectedCountry] = useState("");
  const [draft, setDraft] = useState("");
  const [messages, setMessages] = useState<TranscriptMessage[]>([]);
  const [latestResponse, setLatestResponse] =
    useState<CountryChatResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState("");
  const transcriptEndRef = useRef<HTMLDivElement | null>(null);
  const previousCountryRef = useRef("");

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api.countryChatMetadata()
      .then((response) => {
        if (cancelled) {
          return;
        }
        setMetadata(response);
      })
      .catch((reason: unknown) => {
        if (cancelled) {
          return;
        }
        setError(
          reason instanceof Error
            ? reason.message
            : String(reason),
        );
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!metadata || selectedCountry) {
      return;
    }
    const matchedCountry = metadata.availableCountries.find(
      (item) => item.value === preferredCountry,
    );
    setSelectedCountry(
      matchedCountry?.value
        ?? metadata.availableCountries[0]?.value
        ?? "",
    );
  }, [metadata, preferredCountry, selectedCountry]);

  useEffect(() => {
    if (!selectedCountry) {
      return;
    }
    if (!previousCountryRef.current) {
      previousCountryRef.current = selectedCountry;
      return;
    }
    if (previousCountryRef.current === selectedCountry) {
      return;
    }
    previousCountryRef.current = selectedCountry;
    setMessages([]);
    setLatestResponse(null);
    setError("");
  }, [selectedCountry]);

  useEffect(() => {
    transcriptEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, sending]);

  const promptSuggestions = useMemo(
    () => latestResponse?.suggestedPrompts ?? metadata?.suggestedPrompts ?? [],
    [latestResponse, metadata],
  );

  async function handleSend() {
    const question = draft.trim();
    if (!question || !selectedCountry || sending) {
      return;
    }

    const userMessage: TranscriptMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: question,
    };
    const history = messages.map<CountryChatTurn>((message) => ({
      role: message.role,
      content: message.content,
    }));

    setDraft("");
    setError("");
    setSending(true);
    setMessages((current) => [...current, userMessage]);

    try {
      const response = await api.countryChat({
        country: selectedCountry,
        question,
        history,
      });
      startTransition(() => {
        setLatestResponse(response);
        setMessages((current) => [
          ...current,
          {
            id: `assistant-${Date.now()}`,
            role: "assistant",
            content: response.answer,
            provider: response.provider,
            providerReason: response.providerReason,
          },
        ]);
      });
    } catch (reason: unknown) {
      setError(
        reason instanceof Error
          ? reason.message
          : String(reason),
      );
    } finally {
      setSending(false);
    }
  }

  if (loading && !metadata) {
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
  const providerSummary = metadata?.providerAvailable
    ? `NVIDIA · ${metadata.defaultModel ?? "default model"}`
    : (metadata?.providerReason ?? "当前使用本地降级回答");

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
              <span className="dashboard-hero-chip">缺 key 自动降级</span>
            </div>
          </div>
        </div>

        <div className="copilot-grid">
          <div className="card copilot-chat-card">
            <div className="copilot-toolbar">
              <label className="copilot-field">
                <span>国家</span>
                <select
                  value={selectedCountry}
                  onChange={(event) => setSelectedCountry(event.target.value)}
                  disabled={sending}
                >
                  {metadata?.availableCountries.map((item) => (
                    <option key={item.value} value={item.value}>
                      {item.label}
                    </option>
                  ))}
                </select>
              </label>
              <div className="copilot-toolbar-note">
                优先读取当前国家的 overview、品牌、车型和动力结构摘要，榜单按累计销量排序。
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
                        <span>{message.provider}</span>
                      ) : null}
                    </div>
                    <div className="copilot-message-body">
                      {message.content}
                    </div>
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
                  onClick={handleSend}
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
              <h3>头部品牌（销量）</h3>
              <ul className="copilot-rank-list">
                {(snapshot?.topBrands ?? []).map((item) => (
                  <li key={`brand-${item.label}`}>
                    <span>{item.label}</span>
                    <strong>{formatNumber(item.value)}</strong>
                  </li>
                ))}
              </ul>
            </div>

            <div className="card copilot-side-card">
              <h3>动力结构（销量）</h3>
              <ul className="copilot-rank-list">
                {(snapshot?.powertrainMix ?? []).map((item) => (
                  <li key={`pt-${item.label}`}>
                    <span>{item.label}</span>
                    <strong>{formatNumber(item.value)}</strong>
                  </li>
                ))}
              </ul>
            </div>
          </aside>
        </div>
      </section>
    </div>
  );
}