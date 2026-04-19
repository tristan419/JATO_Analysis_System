# Business Pipeline Workflows

This document explains the system from a **business workflow** perspective instead of a repository/module perspective.

The goal is to answer:

- How JATO monthly import becomes analysis-ready data
- How MSRP scraping becomes pricing intelligence
- How sales + prices become positioning/pricing analysis
- How monthly scan and competitor monitoring are implemented
- How country scan and Copilot are grounded by data, prices, and news

---

## 1. Business capability to pipeline map

```mermaid
flowchart LR
  subgraph DataFoundation[Data foundation]
    A1[JATO monthly Excel / raw source files]
    A2[Sales parquet / processed aggregates]
    A3[MSRP source configs + scrapers]
    A4[Country news sources]
    A5[App DB]
  end

  subgraph Pipelines[Operational pipelines]
    B1[Monthly update job\nprepare -> compare -> refresh]
    B2[MSRP scraping\n dry-run -> ingest -> review -> materialize]
    B3[Country news refresh\n digest/article persistence]
    B4[Analysis APIs\n overview + advanced charts]
    B5[Country snapshot builder]
  end

  subgraph BusinessOutputs[Business-facing outputs]
    C1[定位定价\nPositioning & Pricing]
    C2[月度扫描\nMonthly Market Scan]
    C3[竞品监控\nCompetitor Monitoring]
    C4[国家扫描\nCountry Scan]
    C5[Country Copilot]
  end

  A1 --> B1
  B1 --> A2

  A3 --> B2
  B2 --> A5

  A4 --> B3
  B3 --> A5

  A2 --> B4
  A5 --> B4
  A2 --> B5
  A5 --> B5

  B4 --> C1
  B4 --> C2
  B4 --> C3
  B5 --> C4
  B5 --> C5
  B2 --> C1
  B2 --> C3
  B3 --> C4
  B3 --> C5
```

---

## 2. Business work decomposition table

| Business work | Core business question | Main data inputs | Pipeline implementation | Product surface |
| --- | --- | --- | --- | --- |
| JATO monthly import | How do we refresh the sales/market baseline for the latest month? | Monthly JATO Excel, raw source files | Upload -> monthly update job -> prepare / compare / refresh -> processed dataset refresh | Monthly update page, downstream dashboard/chart pages |
| Positioning & pricing | Where are we positioned by segment/brand/model/version and what should the pricing logic look like? | Processed sales data, current MSRP, review/materialized price history | Analysis APIs + MSRP workflow + advanced chart / deck generation | Positioning/Pricing pages, Market Scan, Version Comparison |
| Monthly market scan | What changed this month in market structure, pricing, versions, and trends? | Refreshed processed dataset, current price state | Monthly import refresh + overview/advanced APIs + market-scan page logic | Dashboard, Market Scan, Customer Insights |
| Competitor monitoring | What are competitors doing on price, lineup, and signal/news level? | MSRP scraping, review cases, current prices, country news digests | Scrape -> ingest -> review/materialize + news refresh + analysis pages | MSRP, Review, Dashboard, Copilot |
| Country scan | What is happening in one country across market, prices, and current news? | Processed country snapshot, DB prices, country digests/articles | Country snapshot builder + optional news refresh + grounded country chat | Country Copilot, country-level analysis pages |

---

## 3. JATO import -> monthly scan

```mermaid
flowchart TD
  A[JATO monthly Excel upload] --> B[monthly update job]
  B --> C[prepare]
  C --> D[compare]
  D --> E[refresh]
  E --> F[04_Processed_data refresh]
  F --> G[analysis overview APIs]
  G --> H[dashboard / specification]
  G --> I[market scan]
  G --> J[positioning / version comparison / customer insights]
```

Business meaning:

- This is the baseline refresh path.
- Once the monthly dataset is refreshed, all downstream scan and comparison work becomes updated.

---

## 4. MSRP -> pricing intelligence

```mermaid
flowchart TD
  A[MSRP source YAML + scraper config] --> B[dry-run]
  B --> C[batch logs / dryrun report]
  B --> D[live ingest]
  D --> E[ensure MSRP source]
  E --> F[POST /msrp/batches]
  F --> G[scrape_batch + observations]
  G --> H[review cases / overrides]
  G --> I[CurrentPrice + PriceHistory]
  H --> I
  I --> J[positioning & pricing analysis]
  I --> K[competitor monitoring]
  I --> L[Country Copilot]
```

Business meaning:

- MSRP is not useful at scrape time alone.
- It becomes business-usable only after ingest, review/override, and materialization into current/historical price state.

---

## 5. Positioning & pricing workflow

```mermaid
flowchart TD
  A[Processed sales data] --> B[analysis overview]
  A --> C[advanced chart / deck APIs]
  D[CurrentPrice + PriceHistory] --> C
  B --> E[segment / trend / version baseline]
  C --> F[positioning map]
  C --> G[market scan deck]
  C --> H[version comparison]
  E --> I[pricing hypothesis]
  F --> I
  G --> I
  H --> I
  I --> J[business decision: target position / target price / variant strategy]
```

Business meaning:

- Sales data provides market structure.
- MSRP provides the live/historical pricing layer.
- Advanced analysis pages convert those inputs into an actionable positioning/pricing decision.

---

## 6. Competitor monitoring workflow

```mermaid
flowchart TD
  A[MSRP scraping] --> B[price observations]
  B --> C[review + materialize]
  C --> D[current competitor price state]

  E[country news refresh] --> F[digest/articles]

  D --> G[dashboard / market scan]
  D --> H[Country Copilot]
  F --> G
  F --> H

  G --> I[competitor watchlist / movement tracking]
  H --> I
```

Business meaning:

- Competitor monitoring is not one page.
- It is the combination of **current price state** plus **fresh news signal** plus **analytical views**.

---

## 7. Country scan and Copilot workflow

```mermaid
flowchart TD
  A[Processed country-level analysis data] --> B[country snapshot]
  C[CurrentPrice + PriceHistory] --> B
  D[Country digests / articles] --> B
  E[optional live news refresh] --> D
  B --> F[Country Copilot answer]
  B --> G[country scan narrative]
  F --> H[business interpretation]
  G --> H
```

Business meaning:

- Country Copilot is grounded on structured market data first.
- News and price state enrich the scan so it can answer practical country-level questions instead of generic chat.

---

## 8. Practical reading order for business users

1. Start with **JATO monthly import -> monthly scan**
2. Then read **MSRP -> pricing intelligence**
3. Then read **Positioning & pricing workflow**
4. Then read **Competitor monitoring workflow**
5. Finish with **Country scan and Copilot workflow**

That order matches the business logic:

1. refresh baseline
2. refresh price layer
3. analyze position/pricing
4. watch competitors
5. explain one country deeply

