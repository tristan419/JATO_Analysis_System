import { useEffect, useMemo, useRef, useState } from "react";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  CustomerInsightDeckResponse,
  CustomerInsightEvidenceCard,
  CustomerInsightMode,
  CustomerInsightShareItem,
  PositioningPricingMetric,
} from "../types";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import { useFixedCanvasPreview } from "../utils/useFixedCanvasPreview";

const EXPORT_PRESETS = [
  { key: "hd+", label: "1600 x 900", width: 1600, height: 900 },
  { key: "fhd", label: "1920 x 1080", width: 1920, height: 1080 },
  { key: "qhd", label: "2560 x 1440", width: 2560, height: 1440 },
] as const;
const LIVE_COUNTRY_LABELS: Record<string, string> = {
  SE: "SE / Sweden",
  FI: "FI / Finland",
  NO: "NO / Norway",
  DK: "DK / Denmark",
};
const DEFAULT_MODE_OPTIONS: CustomerInsightMode[] = ["benchmark", "forum_live"];

type BenchmarkSectionCopy = {
  chipLabel: string;
  loadingLabel: string;
  loadingDetail: string;
  occupationSubtitle: string;
  powertrainSubtitle: string;
  philosophySubtitle: string;
  useCasesSubtitle: string;
  decisionFactorsSubtitle: string;
  personaSubtitle: string;
};

type CustomerInsightsPageProps = {
  deckLoader?: (mode: CustomerInsightMode, countries?: string[]) => Promise<CustomerInsightDeckResponse>;
  modeOptions?: CustomerInsightMode[];
  slideCode?: string;
  exportFilePrefix?: string;
  errorTitle?: string;
  benchmarkCopy?: Partial<BenchmarkSectionCopy>;
};

const DEFAULT_BENCHMARK_COPY: BenchmarkSectionCopy = {
  chipLabel: "Curated benchmark sample",
  loadingLabel: "正在整理北欧用户调研",
  loadingDetail: "从 VOC Nordic 用户深访中聚合家庭画像、购车用途与典型 persona。",
  occupationSubtitle: "职业主要集中在专业白领、教育、医疗与公共服务。",
  powertrainSubtitle: "不是排斥转电，而是按冬季补能、家庭长途和 TCO 做现实权衡。",
  philosophySubtitle: "用户在品牌、低碳、TCO 和科技体验之间做现实取舍。",
  useCasesSubtitle: "家庭长途、滑雪、拖挂与通勤并存，强调全季节适应性。",
  decisionFactorsSubtitle: "价格体系、补能便利、冬季能力和智能化都在前排。",
  personaSubtitle: "把北欧家庭转电用户翻译成可直接用于产品和营销讨论的画像。",
};

function formatMetricValue(value: number | string): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString("en-US");
  }
  return String(value ?? "-");
}

function formatShare(sharePct: number): string {
  return `${Math.round(sharePct * 100)}%`;
}

function formatEvidenceTimestamp(value?: string | null): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString();
}

function takeTopItems(items: CustomerInsightShareItem[] | undefined, limit: number): CustomerInsightShareItem[] {
  return (items ?? []).slice(0, limit);
}

function sanitizeFileNameSegment(value: string): string {
  return value
    .trim()
    .replace(/[\\/:*?"<>|]+/g, "-")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function buildSourceMetaLabel(items: CustomerInsightShareItem[] | undefined): string {
  return items && items.length > 0 ? "来源 瑞典 / 北欧常看汽车论坛" : "来源 瑞典 / 北欧车主讨论网站";
}

function buildDeckSourceMetaLabel(deck: CustomerInsightDeckResponse | null): string {
  if (deck?.metadata.mode === "forum_live") {
    return "来源 北欧公开论坛 / 评论页";
  }
  return buildSourceMetaLabel(deck?.page.profile.sampleSources);
}

function buildDeckSourceDetail(deck: CustomerInsightDeckResponse | null): string {
  if (deck?.metadata.mode === "forum_live") {
    return `覆盖 ${deck.metadata.coverageLabel}；只展示 observed forum VOC sections。`;
  }
  const sampleSources = deck?.page.profile.sampleSources;
  const attentionChannels = deck?.page.profile.attentionChannels;
  if ((sampleSources?.length ?? 0) === 0 && (attentionChannels?.length ?? 0) === 0) {
    return "样本来源与购车信息渠道拆分展示。";
  }
  return "样本来源与购车信息渠道拆分展示。";
}

function Panel({
  eyebrow,
  title,
  subtitle,
  children,
}: {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="market-scan-panel">
      <header className="market-scan-panel-head">
        <div>
          {eyebrow ? <span className="market-scan-panel-eyebrow">{eyebrow}</span> : null}
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </header>
      <div className="market-scan-panel-body">{children}</div>
    </section>
  );
}

function MetricCard({ metric }: { metric: PositioningPricingMetric }) {
  return (
    <article className="market-scan-metric-card">
      <span className="market-scan-metric-label">{metric.label}</span>
      <strong className="market-scan-metric-value">{formatMetricValue(metric.value)}</strong>
      <span className="market-scan-metric-detail">{metric.detail}</span>
    </article>
  );
}

function ShareList({ items, variant = "default" }: { items: CustomerInsightShareItem[]; variant?: "default" | "profile" }) {
  return (
    <div className={`customer-insight-share-list${variant === "profile" ? " customer-insight-share-list--profile" : ""}`}>
      {items.map((item) => (
        <div
          key={`${item.label}-${item.value}`}
          className={`customer-insight-share-item${variant === "profile" ? " customer-insight-share-item--profile" : ""}`}
        >
          <span className="customer-insight-share-label" title={item.rawLabel || item.label}>{item.label}</span>
          <div className="customer-insight-share-bar">
            <span style={{ width: `${Math.max(item.sharePct * 100, 4)}%` }} />
          </div>
          <span className="customer-insight-share-meta">{formatShare(item.sharePct)}</span>
        </div>
      ))}
    </div>
  );
}

function ProfileBlock({ title, items, wide = false }: { title: string; items: CustomerInsightShareItem[]; wide?: boolean }) {
  return (
    <section className={`customer-insight-profile-block${wide ? " customer-insight-profile-block--wide" : ""}`}>
      <header>
        <h3>{title}</h3>
      </header>
      {items.length > 0 ? (
        <ShareList items={items} variant="profile" />
      ) : (
        <p className="version-comparison-empty">暂无可展示项</p>
      )}
    </section>
  );
}

function TagCloud({ items }: { items: string[] }) {
  if (items.length === 0) {
    return <p className="version-comparison-empty">暂无可展示项</p>;
  }
  return (
    <div className="customer-insight-tag-cloud">
      {items.map((item) => (
        <span key={item} className="customer-insight-tag">{item}</span>
      ))}
    </div>
  );
}

function EvidenceCardList({ cards }: { cards: CustomerInsightEvidenceCard[] }) {
  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({});

  useEffect(() => {
    setExpandedCards({});
  }, [cards]);

  if (cards.length === 0) {
    return <p className="version-comparison-empty">暂无可回放的证据卡片</p>;
  }
  return (
    <div className="customer-insight-evidence-grid">
      {cards.map((card) => (
        <article key={card.url} className="customer-insight-evidence-card">
          <div className="customer-insight-evidence-meta">
            <span>{card.siteName || "Source"}</span>
            <span>{card.publishTier || "n/a"}</span>
            <span>{card.sentiment || "neutral"}</span>
            {card.countryCode ? <span>{card.countryCode}</span> : null}
            {card.language ? <span>{card.language}</span> : null}
          </div>
          <a
            className="customer-insight-evidence-title"
            href={card.url}
            target="_blank"
            rel="noreferrer"
          >
            {card.title}
          </a>
          <div className="customer-insight-tag-cloud">
            {card.signals.map((signal) => (
              <span key={`${card.url}-${signal}`} className="customer-insight-tag">
                {signal}
              </span>
            ))}
          </div>
          <div className="customer-insight-note-list">
            {card.evidenceSnippets.slice(0, 2).map((snippet) => (
              <p key={`${card.url}-${snippet}`} className="customer-insight-note">{snippet}</p>
            ))}
          </div>
          {card.excerpt || card.contentPreview || card.observations.length > 0 ? (
            <>
              <button
                type="button"
                className="btn btn-sm btn-ghost customer-insight-evidence-toggle"
                onClick={() => {
                  setExpandedCards((current) => ({
                    ...current,
                    [card.url]: !current[card.url],
                  }));
                }}
              >
                {expandedCards[card.url] ? "收起抓取内容" : "查看抓取内容"}
              </button>
              {expandedCards[card.url] ? (
                <div className="customer-insight-evidence-detail">
                  <div className="customer-insight-evidence-detail-meta">
                    <span>发布时间：{formatEvidenceTimestamp(card.publishedAt)}</span>
                    <span>抓取时间：{formatEvidenceTimestamp(card.collectedAt)}</span>
                    {card.qualityScore ? <span>质量分：{card.qualityScore}</span> : null}
                    {card.observationCount ? <span>Observation：{card.observationCount}</span> : null}
                  </div>
                  {card.excerpt ? (
                    <div className="customer-insight-evidence-copy">
                      <strong>页面摘录</strong>
                      <p>{card.excerpt}</p>
                    </div>
                  ) : null}
                  {card.contentPreview ? (
                    <div className="customer-insight-evidence-copy">
                      <strong>抓取正文预览{card.contentTruncated ? "（已截断）" : ""}</strong>
                      <p>{card.contentPreview}</p>
                    </div>
                  ) : null}
                  {card.observations.length > 0 ? (
                    <div className="customer-insight-evidence-copy">
                      <strong>命中的观察句</strong>
                      <div className="customer-insight-evidence-observations">
                        {card.observations.map((observation, index) => (
                          <article
                            key={`${card.url}-${observation.label}-${index}`}
                            className="customer-insight-evidence-observation"
                          >
                            <div className="customer-insight-evidence-meta">
                              <span>{observation.label}</span>
                              <span>{observation.signalKind || "signal"}</span>
                              <span>{observation.sentiment || "neutral"}</span>
                            </div>
                            <p>{observation.sentence}</p>
                            {observation.matchedTokens.length > 0 ? (
                              <div className="customer-insight-tag-cloud">
                                {observation.matchedTokens.map((token) => (
                                  <span key={`${card.url}-${observation.label}-${token}`} className="customer-insight-tag">
                                    {token}
                                  </span>
                                ))}
                              </div>
                            ) : null}
                          </article>
                        ))}
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : null}
            </>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function buildHorizontalBarTrace(items: CustomerInsightShareItem[], color: string): Data[] {
  return [
    {
      type: "bar",
      orientation: "h",
      x: items.map((item) => item.sharePct * 100),
      y: items.map((item) => item.label),
      text: items.map((item) => formatShare(item.sharePct)),
      textposition: "outside",
      cliponaxis: false,
      marker: {
        color,
        opacity: 0.9,
      },
      hovertemplate: "%{y}<br>占比: %{x:.0f}%<extra></extra>",
    } as Data,
  ];
}

function horizontalBarLayout(maxValue: number): Partial<PlotlyLayout> {
  return {
    ...CHART_LAYOUT,
    margin: { l: 168, r: 22, t: 4, b: 18 },
    showlegend: false,
    xaxis: {
      range: [0, Math.max(30, Math.ceil(maxValue * 100 / 5) * 5 + 5)],
      ticksuffix: "%",
      zeroline: false,
      gridcolor: "rgba(148, 163, 184, 0.15)",
    },
    yaxis: {
      automargin: true,
      tickfont: { size: 10 },
      zeroline: false,
    },
  };
}

export function CustomerInsightsPage({
  deckLoader = api.nordicCustomerDeck,
  modeOptions = DEFAULT_MODE_OPTIONS,
  slideCode = "10",
  exportFilePrefix = "customer-insights",
  errorTitle = "北欧用户调研加载失败",
  benchmarkCopy: benchmarkCopyOverrides,
}: CustomerInsightsPageProps = {}) {
  const availableModes = modeOptions.length > 0 ? modeOptions : DEFAULT_MODE_OPTIONS;
  const defaultMode = availableModes[0] ?? "benchmark";
  const benchmarkCopy = { ...DEFAULT_BENCHMARK_COPY, ...benchmarkCopyOverrides };
  const [deck, setDeck] = useState<CustomerInsightDeckResponse | null>(null);
  const [mode, setMode] = useState<CustomerInsightMode>(defaultMode);
  const [forumCountryFilter, setForumCountryFilter] = useState("ALL");
  const [forumCountryOptions, setForumCountryOptions] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportPresetKey, setExportPresetKey] = useState<(typeof EXPORT_PRESETS)[number]["key"]>("fhd");
  const slideRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    preloadPlotlyChartRuntime().catch(() => undefined);
  }, []);

  useEffect(() => {
    if (availableModes.includes(mode)) {
      return;
    }
    setMode(defaultMode);
    setForumCountryFilter("ALL");
  }, [availableModes, defaultMode, mode]);

  useEffect(() => {
    let active = true;
    setDeck(null);
    setLoading(true);
    setError("");
    const requestedCountries = mode === "forum_live" && forumCountryFilter !== "ALL"
      ? [forumCountryFilter]
      : undefined;
    deckLoader(mode, requestedCountries)
      .then((response) => {
        if (!active) {
          return;
        }
        if (response.metadata.mode === "forum_live") {
          setForumCountryOptions((current) => {
            const merged = new Set<string>([...current, ...response.metadata.countryCodes]);
            return Array.from(merged);
          });
        }
        setDeck(response);
      })
      .catch((reason: Error) => {
        if (!active) {
          return;
        }
        setError(reason.message);
      })
      .finally(() => {
        if (active) {
          setLoading(false);
        }
      });
    return () => {
      active = false;
    };
  }, [deckLoader, mode, forumCountryFilter]);

  const page = deck?.page;
  const isForumLive = deck?.metadata.mode === "forum_live";
  const compactProfile = useMemo(() => ({
    sampleSources: page?.profile.sampleSources ?? [],
    attentionChannels: page?.profile.attentionChannels ?? [],
    gender: page?.profile.gender ?? [],
    age: page?.profile.age ?? [],
    household: page?.profile.household ?? [],
    weeklyCommute: page?.profile.weeklyCommute ?? [],
  }), [page]);
  const compactOccupation = useMemo(() => page?.occupation.items ?? [], [page]);
  const compactPowertrain = useMemo(() => page?.powertrain.items ?? [], [page]);
  const compactPhilosophy = useMemo(() => page?.philosophy.items ?? [], [page]);
  const compactPurchaseUses = useMemo(() => page?.purchaseUses.items ?? [], [page]);
  const compactDecisionFactors = useMemo(() => page?.decisionFactors.items ?? [], [page]);
  const compactPersonaFacts = useMemo(() => page?.persona.facts ?? [], [page]);
  const compactPersonaNotes = useMemo(() => page?.persona.notes ?? [], [page]);
  const usageTraces = useMemo(
    () => (compactPurchaseUses.length > 0 ? buildHorizontalBarTrace(compactPurchaseUses, "#2563eb") : []),
    [compactPurchaseUses],
  );
  const factorTraces = useMemo(
    () => (compactDecisionFactors.length > 0 ? buildHorizontalBarTrace(compactDecisionFactors, "#0f766e") : []),
    [compactDecisionFactors],
  );
  const usageMax = Math.max(...compactPurchaseUses.map((item) => item.sharePct), 0);
  const factorMax = Math.max(...compactDecisionFactors.map((item) => item.sharePct), 0);
  const exportPreset = EXPORT_PRESETS.find((item) => item.key === exportPresetKey) ?? EXPORT_PRESETS[1];
  const slidePreview = useFixedCanvasPreview({
    width: exportPreset.width,
    height: exportPreset.height,
    exporting: exportingSlide,
  });
  const sourceMetaLabel = useMemo(() => buildDeckSourceMetaLabel(deck), [deck]);
  const sourceDetail = useMemo(() => buildDeckSourceDetail(deck), [deck]);
  const sampleUnitLabel = deck?.metadata.sampleUnitLabel === "docs" ? "文档" : "样本";
  const forumLive = page?.forumLive;
  const forumSourceMix = forumLive?.sourceMix ?? [];
  const forumSiteTypes = forumLive?.siteTypes ?? [];
  const forumLanguages = forumLive?.languages ?? [];
  const forumPublishTiers = forumLive?.publishTiers ?? [];
  const forumSentiment = forumLive?.sentiment ?? [];
  const forumOwnershipStages = forumLive?.ownershipStages ?? [];
  const forumPainPoints = forumLive?.painPoints ?? [];
  const forumProductSignals = forumLive?.productSignals ?? [];
  const forumDecisionFactors = forumLive?.decisionFactors ?? [];
  const forumEvidenceCards = forumLive?.evidenceCards ?? [];
  const forumObservedSections = forumLive?.observedSections ?? [];
  const forumInferredSections = forumLive?.inferredSections ?? [];

  async function handleExportSlide() {
    if (!slideRef.current || !deck || !page) {
      return;
    }
    try {
      setExportError("");
      setExportingSlide(true);
      if ("fonts" in document) {
        await document.fonts.ready;
      }
      await new Promise<void>((resolve) => {
        requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
      });
      const { toPng } = await import("html-to-image");
      const dataUrl = await toPng(slideRef.current, {
        cacheBust: true,
        pixelRatio: 2,
        backgroundColor: "#eef4f7",
        width: exportPreset.width,
        height: exportPreset.height,
        canvasWidth: exportPreset.width,
        canvasHeight: exportPreset.height,
        style: {
          width: `${exportPreset.width}px`,
          height: `${exportPreset.height}px`,
        },
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = [
        exportFilePrefix,
        sanitizeFileNameSegment(page.subtitle),
        String(deck.metadata.respondentCount),
      ].join("-") + ".png";
      link.click();
    } catch (reason) {
      setExportError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setExportingSlide(false);
    }
  }

  return (
    <div className="positioning-pricing-shell customer-insight-shell">
      <div className="positioning-pricing-main customer-insight-main">
        <section className="market-scan-toolbar customer-insight-mode-toolbar">
          <div className="market-scan-toolbar-group market-scan-toolbar-group--settings">
            {availableModes.length > 1 ? (
              <label className="market-scan-field">
                <span>数据模式</span>
                <select
                  value={mode}
                  onChange={(event) => {
                    const nextMode = event.target.value as CustomerInsightMode;
                    setMode(nextMode);
                    if (nextMode !== "forum_live") {
                      setForumCountryFilter("ALL");
                    }
                  }}
                >
                  {availableModes.includes("benchmark") ? <option value="benchmark">Benchmark Excel</option> : null}
                  {availableModes.includes("forum_live") ? <option value="forum_live">Forum VOC Live</option> : null}
                </select>
              </label>
            ) : null}
            {mode === "forum_live" ? (
              <label className="market-scan-field">
                <span>live 国家</span>
                <select
                  value={forumCountryFilter}
                  onChange={(event) => setForumCountryFilter(event.target.value)}
                >
                  <option value="ALL">All live countries</option>
                  {forumCountryOptions.map((countryCode) => (
                    <option key={countryCode} value={countryCode}>
                      {LIVE_COUNTRY_LABELS[countryCode] ?? countryCode}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
          </div>
          <div className="market-scan-toolbar-group">
            <span className="market-scan-toolbar-chip">
              {mode === "benchmark" ? benchmarkCopy.chipLabel : "Observed-only live forum deck"}
            </span>
            {mode === "forum_live" && deck?.metadata.coverageLabel ? (
              <span className="market-scan-toolbar-chip">覆盖 {deck.metadata.coverageLabel}</span>
            ) : null}
          </div>
        </section>

        {error ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>{errorTitle}</strong>
            <p>{error}</p>
          </section>
        ) : null}

        {loading && !deck ? (
          <section className="market-scan-state-card">
            <LoadingSurface
              mode="inline"
              kicker="VOC"
              label={mode === "benchmark" ? benchmarkCopy.loadingLabel : "正在整理 live forum VOC"}
              detail={
                mode === "benchmark"
                  ? benchmarkCopy.loadingDetail
                  : "从已生成的公开论坛 / 评论页 deck 中聚合 observed evidence、痛点与决策因素。"
              }
            />
          </section>
        ) : null}

        {exportError ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>PNG 导出失败</strong>
            <p>{exportError}</p>
          </section>
        ) : null}

        {deck && page ? (
          <>
            <div ref={slidePreview.shellRef} className="market-scan-slide-shell">
              <div className="market-scan-slide-scale-box" style={slidePreview.scaleBoxStyle}>
                <div
                  ref={slideRef}
                  className={`market-scan-slide-frame customer-insight-slide-frame${exportingSlide ? " is-exporting" : ""}`}
                  style={slidePreview.frameStyle}
                >
              <header className="market-scan-slide-head">
                <div className="market-scan-slide-copy">
                  <span className="market-scan-slide-kicker">{slideCode} {page.title}</span>
                  <h2>{page.subtitle}</h2>
                  <p>{page.summaryText}</p>
                </div>
                <div className="market-scan-slide-meta">
                  <span className="market-scan-slide-tag">{sampleUnitLabel} {deck.metadata.respondentCount}</span>
                  <span className="market-scan-slide-tag">数据源 {deck.metadata.datasetLabel}</span>
                  <span className="market-scan-slide-tag">{deck.metadata.modeLabel}</span>
                  <span className="market-scan-slide-tag">{sourceMetaLabel}</span>
                </div>
              </header>

              <div className="market-scan-slide-body customer-insight-slide-body">
                <div className="market-scan-metric-grid market-scan-metric-grid--slide">
                  {page.metrics.map((metric) => (
                    <MetricCard key={`${metric.label}-${metric.detail}`} metric={metric} />
                  ))}
                </div>

                <section className="market-scan-callout customer-insight-callout">
                  <div className="customer-insight-callout-head">
                    <span className="market-scan-panel-eyebrow">{isForumLive ? "Observed Forum VOC" : "Core Target User"}</span>
                    <strong>{isForumLive ? "公开论坛 live VOC 结论" : "核心目标用户结论"}</strong>
                  </div>
                  <div className="customer-insight-conclusion-grid">
                    {page.conclusionCards.map((card) => (
                      <article key={card.label} className="customer-insight-conclusion-card">
                        <span>{card.label}</span>
                        <strong>{card.headline}</strong>
                        <p>{card.detail}</p>
                      </article>
                    ))}
                  </div>
                  <p className="customer-insight-methodology-note">{page.methodologyNote}</p>
                </section>

                {isForumLive ? (
                  <>
                    <div className="market-scan-grid customer-insight-grid customer-insight-grid--upper">
                      <Panel
                        eyebrow="Coverage"
                        title="来源与覆盖"
                        subtitle={sourceDetail}
                      >
                        <div className="customer-insight-profile-grid">
                          <ProfileBlock title="来源站点" items={forumSourceMix} wide />
                          <ProfileBlock title="站点类型" items={forumSiteTypes} wide />
                          <ProfileBlock title="语言" items={forumLanguages} />
                          <ProfileBlock title="发布层级" items={forumPublishTiers} />
                        </div>
                      </Panel>

                      <Panel
                        eyebrow="Sentiment"
                        title="情绪分布"
                        subtitle="当前为 document-level 主情绪，只作为粗粒度背景层。"
                      >
                        {forumSentiment.length > 0 ? (
                          <ShareList items={forumSentiment} />
                        ) : (
                          <p className="version-comparison-empty">暂无可展示项</p>
                        )}
                      </Panel>

                      <Panel
                        eyebrow="Ownership"
                        title="使用阶段 / 关注位"
                        subtitle="forum live 模式里保留 heuristic ownership / energy-stage hits。"
                      >
                        {forumOwnershipStages.length > 0 ? (
                          <ShareList items={forumOwnershipStages} />
                        ) : (
                          <p className="version-comparison-empty">暂无可展示项</p>
                        )}
                      </Panel>

                      <Panel
                        eyebrow="Boundaries"
                        title="Observed vs inferred"
                        subtitle="live 模式只展示 public VOC 里可直接观测到的 section。"
                      >
                        <div className="customer-insight-forum-sections">
                          <div>
                            <h3>Observed</h3>
                            <TagCloud items={forumObservedSections} />
                          </div>
                          <div>
                            <h3>Excluded / inferred-only</h3>
                            <TagCloud items={forumInferredSections} />
                          </div>
                        </div>
                      </Panel>
                    </div>

                    <div className="market-scan-grid customer-insight-grid customer-insight-grid--lower">
                      <Panel
                        eyebrow="Pain Points"
                        title="高频痛点"
                        subtitle="来自 publish-ready 文档的 observation / mention 聚合。"
                      >
                        {forumPainPoints.length > 0 ? (
                          <ShareList items={forumPainPoints} />
                        ) : (
                          <p className="version-comparison-empty">暂无可展示项</p>
                        )}
                      </Panel>

                      <Panel
                        eyebrow="Product Signals"
                        title="产品信号"
                        subtitle="当前 live 讨论里最稳定出现的产品维度。"
                      >
                        {forumProductSignals.length > 0 ? (
                          <ShareList items={forumProductSignals} />
                        ) : (
                          <p className="version-comparison-empty">暂无可展示项</p>
                        )}
                      </Panel>

                      <Panel
                        eyebrow="Decision Factors"
                        title="决策因素"
                        subtitle="从产品信号与痛点反推出的高层 reason cluster。"
                      >
                        {forumDecisionFactors.length > 0 ? (
                          <ShareList items={forumDecisionFactors} />
                        ) : (
                          <p className="version-comparison-empty">暂无可展示项</p>
                        )}
                      </Panel>
                    </div>

                    <Panel
                      eyebrow="Evidence Cards"
                      title="可回放证据"
                      subtitle="保留来源、信号、snippet，并可直接展开查看抓取到的正文预览与命中观察句。"
                    >
                      <EvidenceCardList cards={forumEvidenceCards} />
                    </Panel>
                  </>
                ) : (
                  <>
                    <div className="market-scan-grid customer-insight-grid customer-insight-grid--upper">
                      <Panel
                        eyebrow="Profile Mix"
                        title="基础画像"
                        subtitle={sourceDetail}
                      >
                        <div className="customer-insight-profile-grid">
                          <ProfileBlock title="样本来源" items={compactProfile.sampleSources} wide />
                          <ProfileBlock title="关注渠道" items={compactProfile.attentionChannels} wide />
                          <ProfileBlock title="性别" items={compactProfile.gender} />
                          <ProfileBlock title="年龄" items={compactProfile.age} />
                          <ProfileBlock title="家庭" items={compactProfile.household} />
                          <ProfileBlock title="周通勤" items={compactProfile.weeklyCommute} />
                        </div>
                      </Panel>

                      <Panel
                        eyebrow="Occupation"
                        title="工作领域"
                        subtitle={benchmarkCopy.occupationSubtitle}
                      >
                        <ShareList items={compactOccupation} />
                      </Panel>

                      <Panel
                        eyebrow="Powertrain"
                        title="动力 / 转电取向"
                        subtitle={benchmarkCopy.powertrainSubtitle}
                      >
                        <ShareList items={compactPowertrain} />
                      </Panel>

                      <Panel
                        eyebrow="Philosophy"
                        title="购车判断逻辑"
                        subtitle={benchmarkCopy.philosophySubtitle}
                      >
                        <ShareList items={compactPhilosophy} />
                      </Panel>
                    </div>

                    <div className="market-scan-grid customer-insight-grid customer-insight-grid--lower">
                      <Panel
                        eyebrow="Use Cases"
                        title="购车用途"
                        subtitle={benchmarkCopy.useCasesSubtitle}
                      >
                        <div className="customer-insight-chart">
                          <PlotlyChart
                            data={usageTraces}
                            layout={horizontalBarLayout(usageMax)}
                            height={188}
                          />
                        </div>
                      </Panel>

                      <Panel
                        eyebrow="Decision Factors"
                        title="购车关注因素"
                        subtitle={benchmarkCopy.decisionFactorsSubtitle}
                      >
                        <div className="customer-insight-chart">
                          <PlotlyChart
                            data={factorTraces}
                            layout={horizontalBarLayout(factorMax)}
                            height={188}
                          />
                        </div>
                      </Panel>

                      <Panel
                        eyebrow="Persona"
                        title={page.persona.title}
                        subtitle={benchmarkCopy.personaSubtitle}
                      >
                        <div className="customer-insight-persona">
                          <p className="customer-insight-persona-summary">{page.persona.summary}</p>
                          <div className="customer-insight-persona-grid">
                            {compactPersonaFacts.map((fact) => (
                              <article key={fact.label} className="customer-insight-persona-fact">
                                <span>{fact.label}</span>
                                <strong>{fact.value}</strong>
                              </article>
                            ))}
                          </div>
                          <div className="customer-insight-note-list">
                            {compactPersonaNotes.map((note) => (
                              <p key={note} className="customer-insight-note">{note}</p>
                            ))}
                          </div>
                        </div>
                      </Panel>
                    </div>
                  </>
                )}
              </div>
                </div>
              </div>
            </div>

            <section className="market-scan-toolbar market-scan-toolbar--bottom customer-insight-export-toolbar">
              <div className="market-scan-toolbar-group market-scan-toolbar-group--settings">
                <label className="market-scan-field">
                  <span>导出尺寸</span>
                  <select
                    value={exportPresetKey}
                    onChange={(event) => setExportPresetKey(event.target.value as (typeof EXPORT_PRESETS)[number]["key"])}
                  >
                    {EXPORT_PRESETS.map((preset) => (
                      <option key={preset.key} value={preset.key}>
                        {preset.label}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <div className="market-scan-toolbar-group">
                <button
                  type="button"
                  className="btn btn-primary btn-sm"
                  onClick={() => { void handleExportSlide(); }}
                  disabled={exportingSlide}
                >
                  {exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}
                </button>
                <span className="market-scan-toolbar-chip">{exportPreset.width} x {exportPreset.height}</span>
                <span className="market-scan-toolbar-chip">{deck.metadata.respondentCount} {deck.metadata.sampleUnitLabel}</span>
              </div>
            </section>
          </>
        ) : null}
      </div>
    </div>
  );
}
