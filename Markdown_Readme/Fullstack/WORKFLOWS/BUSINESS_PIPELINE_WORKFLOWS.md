# Business Pipeline Workflows

This document explains the system from a **business workflow** perspective instead of a repository/module perspective.

The goal is to answer:

- How JATO monthly import becomes analysis-ready data
- How MSRP scraping becomes pricing intelligence
- How sales + prices become positioning/pricing analysis
- How monthly scan and competitor monitoring are implemented
- How country scan and Copilot are grounded by data, prices, and news
- How vehicle engineering configurations are managed and compared

---

## 1. Business capability to pipeline map

```mermaid
flowchart LR
  subgraph DataFoundation[Data foundation]
    A1[JATO monthly Excel\nraw source files]
    A2[Sales parquet\nprocessed aggregates]
    A3[MSRP source configs\n+ scrapers]
    A4[Country news sources]
    A5[App DB]
    A6[Eng config xlsx\nfield mapping + matrix]
  end

  subgraph Pipelines[Operational pipelines]
    B1[Monthly update job\nprepare → compare → refresh]
    B2[MSRP scraping\ndry-run → ingest → review]
    B3[Country news refresh\ndigest + article persistence]
    B4[Analysis APIs\noverview + advanced charts]
    B5[Country snapshot builder]
    B6[Eng Config Pipeline\nparse → match → diff → publish]
  end

  subgraph BusinessOutputs[Business-facing outputs]
    C1[Positioning & Pricing]
    C2[Monthly Market Scan]
    C3[Competitor Monitoring]
    C4[Country Scan]
    C5[Country Copilot]
    C6[Config Comparison\nDongchedi-style]
    C7[Config Matrix Editor\nExcel-like CRUD]
  end

  A1 --> B1
  B1 --> A2
  A3 --> B2
  B2 --> A5
  A4 --> B3
  B3 --> A5
  A6 --> B6
  B6 --> A5

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
  B6 --> C6
  B6 --> C7
```

---

## 2. Business work decomposition table

| Business work | Core business question | Main data inputs | Pipeline implementation | Product surface |
| --- | --- | --- | --- | --- |
| JATO monthly import | How do we refresh the sales/market baseline? | Monthly JATO Excel, raw source files | Upload → monthly update job → prepare/compare/refresh | Monthly update page, dashboard |
| Positioning & pricing | Where are we positioned and what should pricing be? | Processed sales data, current MSRP, price history | Analysis APIs + MSRP workflow + advanced chart/deck | Positioning/Pricing, Market Scan, Version Comparison |
| Monthly market scan | What changed this month in market structure? | Refreshed dataset, current price state | Monthly import refresh + overview/advanced APIs | Dashboard, Market Scan, Customer Insights |
| Competitor monitoring | What are competitors doing on price and lineup? | MSRP scraping, review cases, news digests | Scrape → ingest → review + news refresh + analysis | MSRP, Review, Dashboard, Copilot |
| Country scan | What is happening in one country? | Country snapshot, DB prices, news digests | Snapshot builder + news refresh + grounded chat | Country Copilot |
| **Eng Config Mgmt** | **What features does each trim have?** | **Config xlsx, field mapping, DB state** | **Parse → identity match → diff → draft → publish** | **Config page: trims/compare/matrix/diff** |

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
  G --> J[positioning / version comparison]
```

Business meaning:

- This is the baseline refresh path.
- Once the monthly dataset is refreshed, all downstream scan and comparison work becomes updated.

---

## 4. MSRP -> pricing intelligence

```mermaid
flowchart TD
  A[MSRP source YAML\n+ scraper config] --> B[dry-run]
  B --> C[batch logs / dryrun report]
  B --> D[live ingest]
  D --> E[ensure MSRP source]
  E --> F[POST /msrp/batches]
  F --> G[scrape_batch + observations]
  G --> H[review cases / overrides]
  G --> I[CurrentPrice + PriceHistory]
  H --> I
  I --> J[positioning & pricing]
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
  I --> J[business decision\ntarget position / price / strategy]
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

  E[country news refresh] --> F[digest / articles]

  D --> G[dashboard / market scan]
  D --> H[Country Copilot]
  F --> G
  F --> H

  G --> I[competitor watchlist]
  H --> I
```

Business meaning:

- Competitor monitoring is not one page.
- It is the combination of **current price state** plus **fresh news signal** plus **analytical views**.

---

## 7. Country scan and Copilot workflow

```mermaid
flowchart TD
  A[Processed country-level\ndata] --> B[country snapshot]
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
- News and price state enrich the scan so it can answer practical country-level questions.

---

## 8. Engineering Configuration Management

```mermaid
flowchart TD
  U[Upload config xlsx] --> P1[FieldMappingParser\n308 features x 10 categories]
  U --> P2[MatrixParser\nfeature x trim matrix]
  P2 --> P3[availability classify\nSTANDARD / OPTIONAL / VALUE...]
  P3 --> P4[Upload session\nchunk → assemble → parse]
  P4 --> P5{identity_key\nmatch?}
  P5 -->|New| P6[New Resource Preview]
  P5 -->|Existing| P7[Diff Preview\nCHANGED / NEW / UNCHANGED]
  P5 -->|Conflict| P8[Draft Conflict Warning]
  P6 --> P9[User confirms → Draft]
  P7 --> P9
  P9 --> P10[Admin publishes\n→ Published]
  P10 --> P11[Viewers see:\ntrims / compare / detail]
```

**Identity key:** `material_no|vehicle_code|market|model_year|trim_name`

**Version state machine:** `draft → published → archived`

**Frontend tabs:** Trims → Compare → Matrix Editor → Upload → Diff History

**Key rules:**
- Never overwrite Published directly — always go through Draft
- Optimistic locking on cell edits (`version` field)
- Every change logged in `ConfigAuditLog`
- Rollback writes old_value back, records source audit_log_id

---

## 9. Practical reading order for business users

1. Start with **JATO monthly import -> monthly scan**
2. Then read **MSRP -> pricing intelligence**
3. Then read **Positioning & pricing workflow**
4. Then read **Competitor monitoring workflow**
5. Then read **Engineering Configuration Management**
6. Finish with **Country scan and Copilot workflow**

That order matches the business logic:

1. refresh baseline
2. refresh price layer
3. analyze position/pricing
4. watch competitors
5. manage vehicle configurations
6. explain one country deeply
