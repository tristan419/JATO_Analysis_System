import { useEffect, useMemo, useRef, useState } from "react";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  CustomerInsightDeckResponse,
  CustomerInsightShareItem,
  PositioningPricingMetric,
} from "../types";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";

const EXPORT_PRESETS = [
  { key: "hd+", label: "1600 x 900", width: 1600, height: 900 },
  { key: "fhd", label: "1920 x 1080", width: 1920, height: 1080 },
  { key: "qhd", label: "2560 x 1440", width: 2560, height: 1440 },
] as const;

function formatMetricValue(value: number | string): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString("en-US");
  }
  return String(value ?? "-");
}

function formatShare(sharePct: number): string {
  return `${Math.round(sharePct * 100)}%`;
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

function buildSourceDetail(
  sampleSources: CustomerInsightShareItem[] | undefined,
  attentionChannels: CustomerInsightShareItem[] | undefined,
): string {
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
      <ShareList items={items} variant="profile" />
    </section>
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

export function CustomerInsightsPage() {
  const [deck, setDeck] = useState<CustomerInsightDeckResponse | null>(null);
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
    let active = true;
    setLoading(true);
    setError("");
    api.nordicCustomerDeck()
      .then((response) => {
        if (!active) {
          return;
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
  }, []);

  const page = deck?.page;
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
  const sourceMetaLabel = useMemo(() => buildSourceMetaLabel(page?.profile.sampleSources), [page]);
  const sourceDetail = useMemo(
    () => buildSourceDetail(page?.profile.sampleSources, page?.profile.attentionChannels),
    [page],
  );

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
        "customer-insights",
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
        {error ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>北欧用户调研加载失败</strong>
            <p>{error}</p>
          </section>
        ) : null}

        {loading && !deck ? (
          <section className="market-scan-state-card">
            <LoadingSurface
              mode="inline"
              kicker="VOC"
              label="正在整理北欧用户调研"
              detail="从 VOC Nordic 用户深访中聚合家庭画像、购车用途与典型 persona。"
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
            <div className="market-scan-slide-shell">
              <div
                ref={slideRef}
                className={`market-scan-slide-frame customer-insight-slide-frame${exportingSlide ? " is-exporting" : ""}`}
                style={{
                  width: exportingSlide ? `${exportPreset.width}px` : undefined,
                  height: exportingSlide ? `${exportPreset.height}px` : undefined,
                  aspectRatio: exportingSlide ? "auto" : undefined,
                }}
              >
              <header className="market-scan-slide-head">
                <div className="market-scan-slide-copy">
                  <span className="market-scan-slide-kicker">10 {page.title}</span>
                  <h2>{page.subtitle}</h2>
                  <p>{page.summaryText}</p>
                </div>
                <div className="market-scan-slide-meta">
                  <span className="market-scan-slide-tag">样本 {deck.metadata.respondentCount}</span>
                  <span className="market-scan-slide-tag">数据源 {deck.metadata.datasetLabel}</span>
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
                    <span className="market-scan-panel-eyebrow">Core Target User</span>
                    <strong>核心目标用户结论</strong>
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
                    subtitle="职业主要集中在专业白领、教育、医疗与公共服务。"
                  >
                    <ShareList items={compactOccupation} />
                  </Panel>

                  <Panel
                    eyebrow="Powertrain"
                    title="动力 / 转电取向"
                    subtitle="不是排斥转电，而是按冬季补能、家庭长途和 TCO 做现实权衡。"
                  >
                    <ShareList items={compactPowertrain} />
                  </Panel>

                  <Panel
                    eyebrow="Philosophy"
                    title="购车判断逻辑"
                    subtitle="用户在品牌、低碳、TCO 和科技体验之间做现实取舍。"
                  >
                    <ShareList items={compactPhilosophy} />
                  </Panel>
                </div>

                <div className="market-scan-grid customer-insight-grid customer-insight-grid--lower">
                  <Panel
                    eyebrow="Use Cases"
                    title="购车用途"
                    subtitle="家庭长途、滑雪、拖挂与通勤并存，强调全季节适应性。"
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
                    subtitle="价格体系、补能便利、冬季能力和智能化都在前排。"
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
                    subtitle="把北欧家庭转电用户翻译成可直接用于产品和营销讨论的画像。"
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
                <span className="market-scan-toolbar-chip">{deck.metadata.respondentCount} Samples</span>
              </div>
            </section>
          </>
        ) : null}
      </div>
    </div>
  );
}
