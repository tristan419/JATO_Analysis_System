# Repository System Workflow

This is the detailed assembled technical map for the repository.

It is split into three layers:

1. Business/data execution chain
2. AppPlatform runtime interaction chain
3. GitNexus independent code-intelligence chain

## 1. Business/data execution chain

```mermaid
flowchart TD
  subgraph Inputs[Source inputs and storage]
    A1[01_RAW_DATA baseline / patches / archives]
    A2[04_Processed_data parquet / precomputed aggregates / candidate scope]
    A3[02_Config_MetaData]
    A4[App DB]
  end

  subgraph DraftPrep[Draft generation and helper scripts]
    B1[candidate_scope.py]
    B2[source_bootstrap.py]
    B3[source_drafts/*.yaml]
    B4[03_Scripts/batch_dryrun.py]
    B5[03_Scripts/batch_ingest.py]
    B6[03_Scripts/run_msrp_low_concurrency.sh]
    B7[03_Scripts/run_scraping_tool.sh]
    B8[03_Scripts/import_evkx_catalog.py]
    B9[dryrun_report.json + batch logs]
  end

  subgraph Toolkit[07_ScrapingToolkit]
    C1[sources/*.yaml promoted MSRP sources]
    C2[news_sources/*.yaml]
    C3[voc_sources/*.yaml]
    C4[run.py / jato-scrape]
    C5[run_news.py]
    C6[jato-voc-plan]
    C7[run_evkx.py]
    C8[config_loader + presets]
    C9[extractors: scrapling / playwright / http_json]
    C10[validation + EUR conversion]
    C11[dry-run summary JSON]
    C12[ensure source + live MSRP ingest]
    C13[EVKX JSON output]
    C14[news batch payload]
    C15[VOC plan paths]
  end

  subgraph Airflow[airflow DAG orchestration]
    D1[jato_msrp_low_concurrency]
    D2[dryrun_country_batch]
    D3[ingest_country_batch]
    D4[jato_country_news_sync]
    D5[jato_scraping_toolkit_manual]
  end

  subgraph Platform[06_AppPlatform business system]
    E1[Data Management page]
    E2[Local Airflow controls]
    E3[MSRP sources CRUD]
    E4[POST /msrp/batches]
    E5[scrape_batch + observations]
    E6[review cases + overrides]
    E7[CurrentPrice + PriceHistory]
    E8[country news refresh endpoint]
    E9[CountryNewsDigest + CountryNewsArticle]
    E10[monthly update upload + jobs]
    E11[analytics overview APIs]
    E12[advanced chart / deck pages]
    E13[Country Copilot]
    E14[projects / overrides / ops state]
    E15[EVKX import review flow]
  end

  subgraph Legacy[05_DashBoard legacy viewer]
    F1[Streamlit dashboard]
  end

  A1 --> A2
  A2 --> B1
  A2 --> B2
  B1 --> B3
  B2 --> B3

  B3 --> B4
  B3 --> B5
  B6 --> B4
  B6 --> B5

  C1 --> C4
  C2 --> C5
  C3 --> C6
  C4 --> C8
  C8 --> C9
  C9 --> C10
  C10 --> C11
  C10 --> C12
  C7 --> C13
  C5 --> C14
  C6 --> C15

  B4 --> C4
  B5 --> C4
  B7 --> C4
  B8 --> C13

  D1 --> D2
  D2 --> B6
  D1 --> D3
  D3 --> B6
  D4 --> E8
  D5 --> B7

  C12 --> E3
  C12 --> E4
  E3 --> E4
  E4 --> E5
  E5 --> E6
  E5 --> E7

  C14 --> E8
  E8 --> E9

  C13 --> B8
  B8 --> E15

  C13 --> A2
  C15 --> A2
  A2 --> E11
  A2 --> E12
  A2 --> E13
  A2 --> F1

  A3 --> E11
  A3 --> E12
  A3 --> E13

  A4 --> E1
  A4 --> E5
  A4 --> E6
  A4 --> E7
  A4 --> E9
  A4 --> E10
  A4 --> E13
  A4 --> E14
  A4 --> E15

  E11 --> E12
  E11 --> E13
  E1 --> E2
  E1 --> E3
  E1 --> E14
  E1 --> D1
  E1 --> D4
  E1 --> D5

  classDef business fill:#fef3c7,stroke:#d97706,color:#111827,stroke-width:2px;
  classDef orchestration fill:#dbeafe,stroke:#2563eb,color:#111827,stroke-width:1.5px;
  classDef store fill:#dcfce7,stroke:#16a34a,color:#111827,stroke-width:1.5px;
  classDef legacy fill:#f3e8ff,stroke:#7c3aed,color:#111827,stroke-width:1.5px;

  class A1,A2,A3,A4,B1,B2,B3,B4,B5,B6,B7,B8,B9,C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13,C14,C15,E1,E2,E3,E4,E5,E6,E7,E8,E9,E10,E11,E12,E13,E14,E15 business;
  class D1,D2,D3,D4,D5 orchestration;
  class A2,A4,B9,C11,C13,C14,C15,E5,E6,E7,E9,E10,E14,E15 store;
  class F1 legacy;
```

Legend:

- Yellow = main business layer
- Green = important data/output landing points
- Blue = orchestration
- Purple = legacy

## 2. AppPlatform runtime interaction chain

```mermaid
sequenceDiagram
  actor U as User
  participant FE as React SPA
  participant BE as FastAPI
  participant DS as Processed sales data
  participant DB as App DB
  participant AF as Local Airflow / runners
  participant EXT as News / wiki / external refresh

  U->>FE: Open Dashboard / Specification
  FE->>BE: columns + filter options + /analysis/overview
  BE->>DS: read parquet / distincts / precomputed country aggregates
  BE-->>FE: KPI cards + trend data + filter state

  U->>FE: Open Market Scan / Positioning / Version Comparison / Customer Insights
  FE->>BE: advanced chart / grouped series / positioning / market-scan APIs
  BE->>DS: compute derived analytical views
  BE-->>FE: charts / deck-ready page state

  U->>FE: Open Country Copilot
  FE->>BE: /assistant/country/chat
  BE->>DS: build country snapshot from analysis data
  BE->>DB: optional MSRP lookup
  BE->>EXT: optional news refresh and wiki/news enrichment
  BE-->>FE: grounded response + snapshot + chart/deck payload

  U->>FE: Open Data Management
  FE->>BE: /data-management/overview + CRUD actions
  BE->>DB: sources / projects / overrides / ops state
  BE->>AF: start/stop/open local Airflow when requested
  BE-->>FE: system status + admin state

  U->>FE: Review MSRP workflow
  FE->>BE: list current prices / review cases / materialize actions
  BE->>DB: observations -> review cases -> current price / history
  BE-->>FE: refreshed review / price state

  U->>FE: Upload monthly JATO Excel
  FE->>BE: resumable upload + create monthly-update job
  BE->>AF: prepare -> compare -> refresh
  BE->>DB: persist job state and outputs
  BE-->>FE: job progress / result / cleanup state
```

### 2.1 Country Copilot current architecture

```mermaid
flowchart TD
  U[User question] --> P1[Extract params + infer intents]
  P1 --> P2[Route planner<br/>market-scan / positioning / precise-lookup / market-context]
  P2 --> P3[Build country snapshot]
  P3 --> P4[Lazy enrich by route]
  P4 --> P5{Snapshot-first answer?}

  P5 -->|Yes| D1[Direct answer builder]
  D1 --> D2[Grounding layers + evidence tables + render hints]
  D2 --> R[Response]

  P5 -->|No| M1[Execution plan / source plan]
  M1 --> M2[Planner prefetch<br/>news wiki / local wiki]
  M2 --> M3[Model path<br/>NVIDIA or Gemini]
  M3 --> M4[Grounded answer]
  M4 --> D2
```

**What this means now**

- The current assistant is already **scope-first**, not free-form chat-first.
- `market-scan-scope`, `positioning-focus`, and `precise-lookup` are answered from the correct page/data scope first.
- A new explicit **execution plan** now sits between routing and model answering:
  - it lists the planned evidence sources
  - marks which ones are already ready vs need prefetch
  - prefetches `news_wiki` / `local_wiki` before the model path when needed

### 2.2 Target top-tier internal knowledge assistant architecture

```mermaid
flowchart LR
  Q[User query] --> A[Query understanding]
  A --> B[Planner / orchestrator]
  B --> C1[Dashboard / structured data]
  B --> C2[Internal wiki / vector retrieval]
  B --> C3[News / external intelligence]
  B --> C4[SQL / app DB]
  B --> C5[Permissions / ACL filter]

  C1 --> D[Evidence packer / reranker]
  C2 --> D
  C3 --> D
  C4 --> D
  C5 --> D

  D --> E[LLM synthesis]
  E --> F[Answer + citations + confidence + next actions]
```

**Gap view**

1. Current Country Copilot already has strong **route + scope + structured evidence**.
2. The first top-tier upgrade is the **planner/orchestrator** layer.
3. The planner should decide, before generation:
   - whether the answer is direct or model-grounded
   - which source stack to use
   - which evidence to prefetch
   - which tools are still allowed after prefetch

### 2.3 Planner-first upgrade now implemented

```mermaid
flowchart TD
  A[Route detected] --> B[Build executionPlan]
  B --> C[sourcePlan:<br/>snapshot core / page scope / trim-sales / msrp / news-wiki / vehicle-wiki]
  C --> D{Need model path?}
  D -->|No| E[Use snapshot-first direct answer]
  D -->|Yes| F[Prefetch planner evidence]
  F --> G[Pass executionPlan + prefetchedEvidence to model]
  G --> H[Return grounded answer + executionPlan]
```

**Planner-first principle**

- The model is no longer the first component deciding where to look.
- The backend planner now decides the evidence stack first, then gives the model a bounded plan.
- This is the first step from **analytics copilot** toward a **top-tier internal knowledge assistant**.

### 2.4 Trust layer + hybrid retrieval now implemented

```mermaid
flowchart TD
  A[executionPlan] --> B[sourcePlan<br/>snapshot / dashboard / scope / DB / wiki / news]
  B --> C[Planner evidence packs]
  C --> D[Model or direct answer]
  D --> E[Grounding]
  E --> F[Trust layer<br/>confidence / sufficiency / missing facts / coverage]
  E --> G[Execution plan visualization<br/>source status / orchestration mode / prefetched tools]
```

**What changed in this step**

- Country Copilot now carries a visible **trust layer** in grounding, not just hidden backend routing.
- Planner context is now **hybrid retrieval** oriented:
  - dashboard analytics
  - page/scope evidence
  - DB current price state
  - prefetched wiki/news facts
- The frontend can now show **how the answer was assembled**, including:
  - orchestration mode
  - source readiness / prefetch state
  - missing facts when evidence is still thin
- This closes the next four gaps after planner-first:
  1. trust layer
  2. hybrid retrieval packaging
  3. execution-plan visualization
  4. regression coverage for the new planner contract

## 3. GitNexus independent code-intelligence chain

```mermaid
sequenceDiagram
  actor U as User
  participant CLI as gitnexus CLI/backend
  participant WEB as gitnexus-web
  participant WORKER as analyze-worker
  participant STORE as .gitnexus / registry / LadybugDB

  U->>CLI: gitnexus serve
  CLI->>CLI: createServer() + LocalBackend.init()
  WEB->>CLI: probe backend + list repos

  alt Existing indexed repo
    WEB->>CLI: GET /api/repo
    WEB->>CLI: GET /api/graph stream=true
  else Analyze new repo
    WEB->>CLI: POST /api/analyze
    CLI->>WORKER: fork analyze-worker
    WORKER->>WORKER: scan -> parse -> imports/calls/heritage -> communities/processes
    WORKER->>STORE: write LadybugDB + meta.json + global registry
    WEB->>CLI: SSE progress stream
    WEB->>CLI: GET /api/repo + GET /api/graph
  end

  WEB->>WEB: build in-memory KnowledgeGraph
  WEB->>WEB: initialize agent + embeddings

  U->>WEB: search / inspect / focus / process view / AI chat
  WEB->>CLI: /api/search
  WEB->>CLI: /api/query
  WEB->>CLI: /api/file
  WEB->>CLI: scoped /api/graph
  CLI-->>WEB: search hits + query rows + code slices + focused subgraphs
```

## Interpretation

### Main business chain

`01_RAW_DATA / 04_Processed_data / 03_Scripts / 07_ScrapingToolkit / airflow / 06_AppPlatform`

### Supporting but non-mainline chains

- `05_DashBoard` = legacy Streamlit consumer of processed data
- `08_GitNexus` = separate code-intelligence product
