# Diagrams Index

Categorized catalog of all Mermaid architecture diagrams in the JATO Analysis System.

---

## Category 1: System Architecture

### 1.1 Full Repository Execution Chain
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#1-businessdata-execution-chain)
```
flowchart TD — 7 subgraphs, ~60 nodes
Inputs → Draft Prep → ScrapingToolkit → Airflow → Platform → Legacy
Includes: Engineering Config Pipeline (pink), 5-table schema
```

### 1.2 AppPlatform Runtime Sequence
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#2-appplatform-runtime-interaction-chain)
```
sequenceDiagram — User / React SPA / FastAPI / Data / DB / Airflow
Covers: Dashboard, Copilot, Data Mgmt, MSRP, JATO upload, Eng Config
```

### 1.3 Business Capability → Pipeline Map
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#1-business-capability-to-pipeline-map)
```
flowchart LR — 3 subgraphs
Data Foundation → Pipelines → Business Outputs
```

### 1.4 GitNexus Code-Intelligence Chain
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#3-gitnexus-independent-code-intelligence-chain)
```
sequenceDiagram — CLI / Web / Worker / Store
Scan → index → graph → search/chat
```

---

## Category 2: Data & ETL Pipelines

### 2.1 JATO Monthly Import → Monthly Scan
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#3-jato-import---monthly-scan)
```
flowchart TD — Upload → prepare → compare → refresh → dashboard
```

### 2.2 MSRP → Pricing Intelligence
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#4-msrp---pricing-intelligence)
```
flowchart TD — YAML → dry-run → ingest → review → CurrentPrice
```

### 2.3 ETL Monthly Update Pipeline
**File:** [../02_DataETL/ETL.md](../02_DataETL/ETL.md)
```
flowchart TD — upload → prepare → compare → candidate → review → publish → baseline
```

### 2.4 Full MSRP Extraction Pipeline
**File:** [../MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md](../MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md)
```
flowchart TD — YAML → scraping strategies → observations → review → price history
```

### 2.5 JATO Monthly Update Publish Guards
**File:** [../04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md](../04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md)
```
flowchart TD — Upload → Prepare/Compare/Refresh → 7 publish precheck gates
  (missing artifacts, country regression → Resolution Panel,
  sales doubling → Critical Warning, partition inconsistency) →
  Publish → Backup → Replace active → Dataset token → Dashboard/MarketScan
```

---

## Category 3: Business Workflows

### 3.1 Positioning & Pricing
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#5-positioning--pricing-workflow)
```
flowchart TD — Sales + MSRP → analysis → positioning map → pricing decision
```

### 3.2 Competitor Monitoring
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#6-competitor-monitoring-workflow)
```
flowchart TD — MSRP scraping + news refresh → price state + signal → watchlist
```

### 3.3 Country Scan & Copilot
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#7-country-scan-and-copilot-workflow)
```
flowchart TD — Sales data + prices + news → snapshot → Copilot answer
```

### 3.4 Business Presentation Deck (6 diagrams)
**File:** [BUSINESS_PRESENTATION_DECK.md](BUSINESS_PRESENTATION_DECK.md)
- Slide 1: Pipeline overview (LR)
- Slide 3: Inputs → Pipelines → Outputs (LR)
- Slide 4: Monthly scan flow (TD)
- Slide 5: Positioning/pricing flow (TD)
- Slide 6: Competitor monitoring (TD)
- Slide 7: Country scan + Copilot (TD)

---

## Category 4: AI / Copilot Architecture

### 4.1 Country Copilot Current Architecture
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#21-country-copilot-current-architecture)
```
flowchart TD — Route planner → snapshot → direct/model answer
```

### 4.2 Target Top-Tier Knowledge Assistant
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#22-target-top-tier-internal-knowledge-assistant-architecture)
```
flowchart LR — Query → planner → evidence packer → LLM → answer
```

### 4.3 Planner-First Upgrade
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#23-planner-first-upgrade-now-implemented)
```
flowchart TD — Route → executionPlan → sourcePlan → prefetch → model
```

### 4.4 Trust Layer + Hybrid Retrieval
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#24-trust-layer--hybrid-retrieval-now-implemented)
```
flowchart TD — executionPlan → evidence → grounding → trust + visualization
```

---

## Category 5: Engineering Configuration Management

### 5.1 Config Pipeline (in main architecture)
**File:** [REPOSITORY_SYSTEM_WORKFLOW.md](REPOSITORY_SYSTEM_WORKFLOW.md#1-businessdata-execution-chain)
```
flowchart TD — EngConfig subgraph: 5 nodes, pink
FieldMappingParser → MatrixParser → availability → upload flow → DB schema
```

### 5.2 Config Upload Flow
**File:** [BUSINESS_PIPELINE_WORKFLOWS.md](BUSINESS_PIPELINE_WORKFLOWS.md#8-engineering-configuration-management)
```
flowchart TD — Upload → parse → match → diff/new preview → confirm → publish
Identity key: material_no|vehicle_code|market|model_year|trim_name
Version states: draft → published → archived
```

### 5.3 Hermes DevSync Contract
**File:** [../../Hermes/HERMES_CLAUDE_CODE_DEVSYNC_CONTRACT.md](../../Hermes/HERMES_CLAUDE_CODE_DEVSYNC_CONTRACT.md)
```
flowchart TD — Claude Code → Dev Event → Git Commit → CI → Hermes API
```

---

## Quick Reference: Which Diagram to Read

| Question | Go To |
|---|---|
| How does the whole system fit together? | 1.1 Full Repository Execution Chain |
| What happens when a user opens a page? | 1.2 AppPlatform Runtime Sequence |
| How does JATO data get refreshed? | 2.1 JATO Monthly Import |
| How does MSRP scraping work end-to-end? | 2.2 MSRP → Pricing Intelligence |
| How does positioning/pricing analysis work? | 3.1 Positioning & Pricing |
| How does competitor monitoring work? | 3.2 Competitor Monitoring |
| How does Country Copilot think? | 4.1 Country Copilot Architecture |
| How does Engineering Config work? | 5.1 + 5.2 |
| Where are all diagrams listed? | This page |
