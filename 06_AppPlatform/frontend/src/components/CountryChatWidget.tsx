import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import { useCountryChatOptional } from "../contexts/CountryChatContext";
import { CatMascot } from "./CatMascot";
import { CountryChatAnalysisDeck } from "./CountryChatAnalysisDeck";
import { CountryChatGroundedAnswer } from "./CountryChatGroundedAnswer";
import { CountryChatPendingMessage } from "./CountryChatPendingMessage";
import { CountryChatModelSelect } from "./CountryChatModelSelect";
import { ChatInlineCharts } from "./ChatInlineCharts";

/* ------------------------------------------------------------------ */
/*  Drag helpers                                                      */
/* ------------------------------------------------------------------ */

interface ResizeState {
  startX: number;
  startY: number;
  originWidth: number;
  originHeight: number;
}

function clampPosition(
  x: number,
  y: number,
  size: number,
): { x: number; y: number } {
  const margin = 8;
  const maxX = window.innerWidth - size - margin;
  const maxY = window.innerHeight - size - margin;
  return {
    x: Math.max(margin, Math.min(x, maxX)),
    y: Math.max(margin, Math.min(y, maxY)),
  };
}

function snapToEdge(
  x: number,
  y: number,
  size: number,
): { x: number; y: number } {
  const midX = window.innerWidth / 2;
  const margin = 16;
  const snappedX =
    x + size / 2 < midX ? margin : window.innerWidth - size - margin;
  return { x: snappedX, y: Math.max(8, Math.min(y, window.innerHeight - size - 8)) };
}

function clampWidgetSize(width: number, height: number): { width: number; height: number } {
  const maxWidth = Math.max(320, Math.min(760, window.innerWidth - 32));
  const maxHeight = Math.max(380, Math.min(880, window.innerHeight - 32));
  return {
    width: Math.max(320, Math.min(width, maxWidth)),
    height: Math.max(380, Math.min(height, maxHeight)),
  };
}

/* ------------------------------------------------------------------ */
/*  Component                                                         */
/* ------------------------------------------------------------------ */

const FAB_SIZE = 72;
const INITIAL_RIGHT = 20;
const INITIAL_BOTTOM = 24;
const WIDGET_SIZE_PRESETS = [
  { id: "compact", label: "紧凑", width: 360, height: 500 },
  { id: "default", label: "标准", width: 420, height: 620 },
  { id: "expanded", label: "展开", width: 520, height: 760 },
] as const;

type WidgetSizePresetId = typeof WIDGET_SIZE_PRESETS[number]["id"];

function formatOpsTime(value: string | null | undefined): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "未同步";
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

function nearestWidgetPresetId(
  width: number,
  height: number,
): WidgetSizePresetId {
  return WIDGET_SIZE_PRESETS.reduce(
    (best, preset) => {
      const distance = Math.abs(preset.width - width) + Math.abs(preset.height - height);
      if (distance < best.distance) {
        return { id: preset.id, distance };
      }
      return best;
    },
    { id: "default" as WidgetSizePresetId, distance: Number.POSITIVE_INFINITY },
  ).id;
}

export function CountryChatWidget() {
  const countryChat = useCountryChatOptional();
  const location = useLocation();

  // Keep the shell stable during hot reloads or transient boot states.
  if (!countryChat) {
    return null;
  }

  const {
    draft,
    error,
    latestResponse,
    loadingMetadata,
    newsStatus,
    messages,
    metadata,
    promptSuggestions,
    providerSummary,
    refreshingNews,
    refreshCountryNews,
    retryLatestQuestionWithFreshNews,
    selectedCountry,
    sending,
    setDraft,
    setSelectedCountry,
    setWidgetExpanded,
    sendQuestion,
    widgetExpanded,
    widgetWidth,
    widgetHeight,
    setWidgetSize,
  } = countryChat;

  const transcriptEndRef = useRef<HTMLDivElement | null>(null);
  const resizeRef = useRef<ResizeState | null>(null);
  const activeWidgetPreset = useMemo(
    () => nearestWidgetPresetId(widgetWidth, widgetHeight),
    [widgetHeight, widgetWidth],
  );
  const pendingQuestion = sending
    ? [...messages]
      .reverse()
      .find((message) => message.role === "user")
      ?.content
      ?? draft
    : "";

  /* --- Drag state --- */
  const [fabPos, setFabPos] = useState<{ x: number; y: number }>(() => ({
    x: window.innerWidth - FAB_SIZE - INITIAL_RIGHT,
    y: window.innerHeight - FAB_SIZE - INITIAL_BOTTOM,
  }));
  const [dragging, setDragging] = useState(false);
  const [resizing, setResizing] = useState(false);
  const dragRef = useRef<{
    startX: number;
    startY: number;
    originX: number;
    originY: number;
    moved: boolean;
  } | null>(null);

  const onPointerDown = useCallback(
    (e: React.PointerEvent) => {
      if (widgetExpanded) return;
      dragRef.current = {
        startX: e.clientX,
        startY: e.clientY,
        originX: fabPos.x,
        originY: fabPos.y,
        moved: false,
      };
      setDragging(true);
      (e.target as HTMLElement).setPointerCapture(e.pointerId);
    },
    [fabPos, widgetExpanded],
  );

  const onPointerMove = useCallback(
    (e: React.PointerEvent) => {
      if (!dragRef.current) return;
      const dx = e.clientX - dragRef.current.startX;
      const dy = e.clientY - dragRef.current.startY;
      if (Math.abs(dx) > 3 || Math.abs(dy) > 3) {
        dragRef.current.moved = true;
      }
      const next = clampPosition(
        dragRef.current.originX + dx,
        dragRef.current.originY + dy,
        FAB_SIZE,
      );
      setFabPos(next);
    },
    [],
  );

  const onPointerUp = useCallback(
    (e: React.PointerEvent) => {
      if (!dragRef.current) return;
      const wasDrag = dragRef.current.moved;
      const finalPos = snapToEdge(fabPos.x, fabPos.y, FAB_SIZE);
      setFabPos(finalPos);
      dragRef.current = null;
      setDragging(false);
      (e.target as HTMLElement).releasePointerCapture(e.pointerId);
      if (!wasDrag) {
        setWidgetExpanded(true);
      }
    },
    [fabPos, setWidgetExpanded],
  );

  const applyWidgetSize = useCallback(
    (nextWidth: number, nextHeight: number) => {
      const next = clampWidgetSize(nextWidth, nextHeight);
      setWidgetSize(next.width, next.height);
    },
    [setWidgetSize],
  );

  const onResizePointerDown = useCallback(
    (event: React.PointerEvent<HTMLButtonElement>) => {
      resizeRef.current = {
        startX: event.clientX,
        startY: event.clientY,
        originWidth: widgetWidth,
        originHeight: widgetHeight,
      };
      setResizing(true);
      event.currentTarget.setPointerCapture(event.pointerId);
      event.preventDefault();
    },
    [widgetHeight, widgetWidth],
  );

  const onResizePointerMove = useCallback(
    (event: React.PointerEvent<HTMLButtonElement>) => {
      if (!resizeRef.current) {
        return;
      }
      const dx = event.clientX - resizeRef.current.startX;
      const dy = event.clientY - resizeRef.current.startY;
      applyWidgetSize(
        resizeRef.current.originWidth - dx,
        resizeRef.current.originHeight - dy,
      );
    },
    [applyWidgetSize],
  );

  const onResizePointerUp = useCallback(
    (event: React.PointerEvent<HTMLButtonElement>) => {
      resizeRef.current = null;
      setResizing(false);
      event.currentTarget.releasePointerCapture(event.pointerId);
    },
    [],
  );

  const applyWidgetPreset = useCallback(
    (presetId: WidgetSizePresetId) => {
      const preset = WIDGET_SIZE_PRESETS.find((item) => item.id === presetId);
      if (!preset) {
        return;
      }
      applyWidgetSize(preset.width, preset.height);
    },
    [applyWidgetSize],
  );

  useEffect(() => {
    transcriptEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, sending, widgetExpanded]);

  useEffect(() => {
    function handleViewportResize() {
      setFabPos((current) => clampPosition(current.x, current.y, FAB_SIZE));
      const next = clampWidgetSize(widgetWidth, widgetHeight);
      if (next.width !== widgetWidth || next.height !== widgetHeight) {
        setWidgetSize(next.width, next.height);
      }
    }

    window.addEventListener("resize", handleViewportResize);
    return () => window.removeEventListener("resize", handleViewportResize);
  }, [setWidgetSize, widgetHeight, widgetWidth]);

  /* Hide on /copilot full-page */
  if (location.pathname === "/copilot") {
    return null;
  }

  const countryOptions = Array.isArray(metadata?.availableCountries)
    ? metadata.availableCountries
    : [];
  const unreadCount = messages.filter((m) => m.role === "assistant").length;

  /* ---- Collapsed: FAB bubble ---- */
  if (!widgetExpanded) {
    return (
      <button
        type="button"
        className={`ccw-fab${dragging ? " is-dragging" : ""}`}
        style={{ left: fabPos.x, top: fabPos.y }}
        onPointerDown={onPointerDown}
        onPointerMove={onPointerMove}
        onPointerUp={onPointerUp}
        aria-label="打开国家助手"
      >
        <CatMascot chatOpen={false} size={FAB_SIZE - 8} />
        {unreadCount > 0 ? (
          <span className="ccw-fab-badge">{unreadCount > 99 ? "99+" : unreadCount}</span>
        ) : null}
      </button>
    );
  }

  /* ---- Expanded: chat popup ---- */
  const popupRight = Math.max(16, window.innerWidth - fabPos.x - FAB_SIZE);
  const popupBottom = Math.max(16, window.innerHeight - fabPos.y - FAB_SIZE);

  return (
    <aside
      className={`ccw-popup${resizing ? " is-resizing" : ""}`}
      style={{
        right: Math.min(popupRight, window.innerWidth - (widgetWidth + 40)),
        bottom: popupBottom,
        width: widgetWidth,
        height: widgetHeight,
      }}
    >
      {/* header */}
      <header className="ccw-popup-header">
        <div className="ccw-popup-header-main">
          <button
            type="button"
            className="ccw-resize-handle"
            aria-label="从左上角细调助手窗口"
            title="从左上角细调窗口"
            onPointerDown={onResizePointerDown}
            onPointerMove={onResizePointerMove}
            onPointerUp={onResizePointerUp}
          />
          <div className="ccw-popup-header-info">
            <strong>国家助手</strong>
            <span className="ccw-popup-provider">{providerSummary}</span>
          </div>
          <div className="ccw-popup-header-actions">
            <Link to="/copilot" className="btn btn-sm btn-secondary" title="全屏工作台">
              ⛶
            </Link>
            <button
              type="button"
              className="ccw-close-btn"
              onClick={() => setWidgetExpanded(false)}
              aria-label="关闭助手"
            >
              ✕
            </button>
          </div>
        </div>
        <div className="ccw-popup-toolbar">
          <select
            className="ccw-country-select"
            value={selectedCountry}
            onChange={(e) => setSelectedCountry(e.target.value)}
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
          <div className="ccw-size-control" aria-label="窗口大小">
            {WIDGET_SIZE_PRESETS.map((preset) => (
              <button
                key={preset.id}
                type="button"
                className={`ccw-size-btn${activeWidgetPreset === preset.id ? " is-active" : ""}`}
                onClick={() => applyWidgetPreset(preset.id)}
                disabled={sending}
                aria-label={`切换到${preset.label}窗口`}
                title={preset.label}
              >
                {preset.label}
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* quick prompts */}
      {promptSuggestions.length > 0 && messages.length === 0 ? (
        <div className="ccw-suggestions">
          {promptSuggestions.slice(0, 3).map((prompt) => (
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
      ) : null}

      {/* transcript */}
      <div className="ccw-transcript">
        {loadingMetadata && !metadata ? (
          <div className="ccw-empty">
            <p>正在准备助手…</p>
          </div>
        ) : messages.length === 0 ? (
          <div className="ccw-empty">
            <p>从当前国家直接问市场趋势、定价、续航或竞品。</p>
          </div>
        ) : (
          messages.map((message) => (
            <article
              key={message.id}
              className={`ccw-msg ccw-msg--${message.role}`}
            >
              <div className="ccw-msg-body">
                <CountryChatGroundedAnswer message={message} compact />
              </div>
              {message.contextSnapshot ? (
                <>
                  <ChatInlineCharts
                    snapshot={message.contextSnapshot}
                    intents={message.focusedIntents ?? message.intents}
                    renderHints={message.renderHints}
                  />
                  <CountryChatAnalysisDeck message={message} compact />
                </>
              ) : null}
            </article>
          ))
        )}
        {sending ? (
          <article className="ccw-msg ccw-msg--assistant ccw-msg--pending">
            <CountryChatPendingMessage question={pendingQuestion} compact />
          </article>
        ) : null}
        <div ref={transcriptEndRef} />
      </div>

      {selectedCountry ? (
        <div className="ccw-ops-row">
          <span className={`copilot-ops-pill${newsStatus?.stale ? " is-stale" : ""}`}>
            {newsStatus?.hasSnapshot
              ? `${newsStatus.summaryProvider ?? "snapshot"} · ${formatOpsTime(newsStatus.syncTimestamp)}`
              : "尚无新闻快照"}
          </span>
          <button
            type="button"
            className="btn btn-sm btn-secondary"
            onClick={() => {
              void refreshCountryNews();
            }}
            disabled={refreshingNews || sending}
          >
            {refreshingNews ? "刷新中…" : "在线刷新新闻"}
          </button>
          {latestResponse ? (
            <button
              type="button"
              className="btn btn-sm btn-primary"
              onClick={() => {
                void retryLatestQuestionWithFreshNews();
              }}
              disabled={refreshingNews || sending}
            >
              刷新后重答
            </button>
          ) : null}
        </div>
      ) : null}

      {/* composer */}
      <div className="ccw-composer">
        {error ? <span className="copilot-error ccw-error">{error}</span> : null}
        <div className="ccw-composer-row">
          <textarea
            className="ccw-input"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                void sendQuestion();
              }
            }}
            placeholder="输入问题…"
            rows={2}
            disabled={sending || !selectedCountry}
          />
          <button
            type="button"
            className="ccw-send-btn"
            onClick={() => {
              void sendQuestion();
            }}
            disabled={sending || !selectedCountry || !draft.trim()}
            aria-label="发送"
          >
            {sending ? (
              <span className="ccw-spinner" />
            ) : (
              <svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor">
                <path d="M2.01 21L23 12 2.01 3 2 10l15 2-15 2z" />
              </svg>
            )}
          </button>
        </div>
      </div>
    </aside>
  );
}
