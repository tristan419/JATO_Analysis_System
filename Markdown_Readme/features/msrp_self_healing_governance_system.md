# MSRP Self-Healing Governance System

## Document status

- Status: confirmed architecture; implementation split across owned sessions
- Date: 2026-07-14
- Implementation authorization: granted on 2026-07-14 within each session's owned scope
- Purpose: define the product boundary shared by Source Governance, Hermes Self-Iterating Agent, automatic Mapping, Matching Review, and MSRP Monitor

Related feature specifications:

- [MSRP Source Governance Console](./feature.msrp_source_governance_console.md)
- [MSRP Hermes Self-Iterating Agent](./feature.msrp_hermes_self_iterating_agent.md)
- [MSRP Multi-Session Ownership and Integration Boundary](./msrp_multi_session_integration_boundary.md)

## 1. Executive decision

The complete product is one MSRP self-healing governance system with two new features:

1. Source Governance Console is the database-backed control plane, evidence store, approval surface, and rollback surface.
2. MSRP Hermes Self-Iterating Agent is the continuously running observation, reasoning, action, evaluation, and improvement plane. Composer is its runtime planning core, not the limit of what may evolve.

DPV4 is not an Agent and is not a feature workflow. It is a stateless LLM API provider accessed by Hermes with a server-side API key.

Self-iteration does not mean letting an LLM learn or invent prices. Hermes may improve Composer policies, playbooks, memory, diagnosis, prompts, model routing, tool definitions, parser templates, Mapping rules, evaluation suites, and bounded code-change proposals from verified outcomes. Every capability has an authority level and promotion Gate.

Official local-currency MSRP remains deterministic and evidence-backed.

## 2. Product architecture

~~~mermaid
flowchart LR
    P0["P0 source fill session<br/>initial official URL/PDF candidates"] --> G["Source Governance Console<br/>targets, evidence, versions, cases"]
    G --> H["MSRP Hermes Self-Iterating Agent"]

    H --> C["Composer / Planner"]
    C -->|"deterministic condition"| T["Repair Tool Registry"]
    C -->|"semantic or structural ambiguity"| D["DPV4 API Provider"]
    D -->|"typed JSON only"| C

    T --> R["Targeted Dryrun / Replay"]
    C --> R
    R --> SG["Source Gate"]

    H --> M["Automatic Mapping Engine"]
    M --> MG["Mapping Gate"]
    M -->|"abstain or conflict"| MR["Matching Review<br/>clustered exceptions"]
    MR -->|"scoped reusable rule"| M

    SG --> E["Materialization Eligibility"]
    MG --> E
    E --> CP["CurrentPrice / PriceHistory"]
    CP --> MM["MSRP Monitor"]
    MM -->|"source/result anomaly"| G
~~~

## 3. Feature and service boundaries

| Component | Owns | Does not own |
|---|---|---|
| P0 source-fill session | initial official URL/PDF discovery and draft configuration | long-term monitoring, repair cases, version publishing |
| Source Governance Console | targets, evidence, source versions, repair cases, proposals, approvals, audit, rollback | Agent planning, LLM routing, extractor implementation |
| Hermes Self-Iterating Agent | observation, diagnosis, planning, tools, Dryrun/replay, evaluation, memory, capability and code proposals | price truth, direct fact writes, self-merge or production deployment |
| DPV4 API Provider | bounded language and structure reasoning through typed responses | workflow state, tools, approval, database writes |
| Scraping Toolkit | deterministic fetch, extract, normalize, Dryrun, ingest | governance approval and Agent policy |
| Automatic Mapping Engine | candidate generation, hard constraints, scoring, margin, abstention, versioned Mapping policy execution | source repair and Agent scheduling |
| Matching Review | low-confidence Mapping exception clusters and reusable scoped decisions | source, evidence, semantic, result, or FX conflicts |
| Current MSRP | accepted current facts and history inspection | source configuration editing |
| MSRP Monitor | market movements, offers, evidence drilldown, anomaly feedback | source publication and Mapping approval |

## 4. Repair domains

Hermes must compose repair plans across explicit domains. Each domain has different truth and publication rules.

### 4.1 Source repair

Examples:

- official URL changed, redirected, expired, or disappeared;
- official PDF was replaced;
- canonical official source is missing or conflicted;
- evidence is stale;
- server access is blocked.

Allowed outcomes:

- retry or validate a known official URL;
- discover a same-domain redirect through deterministic checks;
- attach a new immutable official evidence asset;
- create a candidate Source Version;
- request an editor to enter an official URL or upload an official PDF.

An anti-bot response is not something Hermes learns around indefinitely. After bounded retries it becomes manual_evidence_required. An editor supplies an official URL or PDF, and Hermes resumes validation from that evidence.

### 4.2 Parser repair

Examples:

- selector drift;
- JSON path or API shape changed;
- PDF table moved to different pages;
- regex or locale number parsing changed;
- a page now requires a different existing extractor type.

Allowed outcomes:

- apply a known versioned extractor template;
- propose a selector, regex, JSON path, or PDF region;
- validate a typed profile;
- run targeted Dryrun and replay;
- produce a new Source Version proposal.

Parser repair never promotes a value merely because DPV4 described it plausibly.

### 4.3 Semantic repair

Examples:

- base MSRP was confused with campaign cash;
- a monthly finance amount was parsed as a vehicle price;
- tax or on-the-road semantics are ambiguous;
- price validity dates changed.

Allowed outcomes:

- reclassify a candidate into the correct semantic lane after evidence validation;
- freeze materialization while ambiguity remains;
- create a human decision task;
- reprocess the evidence using a corrected semantic profile.

Semantically different prices never overwrite one another.

### 4.4 Result repair

Result repair corrects a derived observation or materialization decision. It never edits CurrentPrice or PriceHistory in place.

Examples:

- normalization used the wrong decimal or thousands separator;
- a rejected row should be reprocessed after parser repair;
- an observation was assigned the wrong semantic lane;
- a previously materialized observation is invalidated by stronger evidence.

The repair must:

1. preserve the original evidence and observation;
2. create a correction decision with reason and policy version;
3. produce a new corrected observation or rematerialization event;
4. supersede or close the old effective period through the existing history service;
5. retain a complete audit link between old and new results.

### 4.5 Mapping repair

Examples:

- model, trim, powertrain, edition, battery, drive, or model-year alias drift;
- Top1 and Top2 candidates are too close;
- a hard vehicle attribute conflicts.

The Automatic Mapping Engine executes versioned rules. Hermes can compose Mapping jobs and propose aliases. Matching Review handles abstentions and creates scoped reusable rules. DPV4 may explain language ambiguity but cannot approve a Mapping.

### 4.6 FX repair

Official local-currency price is immutable source truth. EUR normalization is a derived lane.

FX repair may correct:

- FX provider or rate date;
- missing currency code;
- stale rate;
- failed EUR-normalized recomputation.

It must never change the observed local MSRP. A corrected FX rate creates a new normalization calculation with provider, rate, effective date, retrieval time, and policy version.

### 4.7 Runtime repair

Examples:

- timeout, DNS, temporary 403/429/5xx;
- browser start failure;
- concurrency or rate-limit pressure;
- an interrupted worker.

These receive bounded retry, backoff, host circuit breaking, and resumable execution. Persistent anti-bot behavior leaves the runtime lane and becomes manual source evidence work.

## 5. Hermes self-iteration and authority levels

Hermes is not limited to Composer and Playbook changes. Its ability to iterate is broad, while its ability to activate a change is risk-based.

### 5.1 Level A: runtime adaptation

Hermes may automatically improve or select:

- playbook and tool order;
- retry, timeout, concurrency, cache, and evidence-pack budgets;
- whether DPV4 is justified;
- Dryrun/replay scope;
- escalation and abstention timing;
- retrieval of similar verified Repair Episodes.

These changes remain inside an active bounded run and cannot weaken a Gate.

### 5.2 Level B: versioned knowledge and policy assets

Hermes may create candidate versions of:

- failure taxonomy and diagnostic rules;
- Source/parser templates;
- Mapping aliases and scoped rules;
- prompts and Flash/Pro/no-LLM routing;
- Composer policies and repair playbooks;
- evaluation fixtures, regression cases, and success criteria;
- cost, freshness, and priority policies.

Candidates require replay/shadow evaluation. Low-risk candidates may later auto-promote under an approved policy; high-risk policy remains human-approved.

### 5.3 Level C: tool and code evolution

When no existing tool can solve a verified recurring pattern, Hermes may:

- generate a new extractor or tool proposal;
- create deterministic test fixtures from immutable evidence;
- draft code patches, migration designs, and documentation;
- open a reviewable PR proposal with evaluation evidence.

Hermes cannot self-merge, self-deploy, change secrets, or grant itself new authority. Code and schema changes follow normal review, CI, migration, and deployment controls.

### 5.4 Level D: product-contract proposals

Hermes may identify and propose changes to:

- semantic lanes;
- evidence contracts;
- Source or Mapping hard constraints;
- canonical-source priority;
- FX provider/rate policy;
- new repair domains or Case taxonomies.

These are architecture proposals only and always require explicit human approval because they redefine business truth or data contracts.

### 5.5 Non-negotiable invariants

No iteration level may:

- infer an MSRP without official evidence;
- optimize toward a desired price;
- modify accepted local-currency facts in place;
- bypass Source Gate, Mapping Gate, schema validation, audit, or rollback;
- expose secrets;
- self-approve a protected policy, merge, migration, or deployment.

No neural-network training is required for this design. If a future model-training proposal ever appears, it is a separate architecture decision and cannot silently enter this Feature.

## 6. DPV4 provider boundary

DPV4 is configured as server infrastructure:

~~~text
DEEPSEEK_API_KEY=<secret>
MSRP_HERMES_LLM_PROVIDER=dpv4
MSRP_HERMES_LLM_MODEL=deepseek-v4-flash
MSRP_HERMES_LLM_BASE_URL=https://api.deepseek.com
~~~

The key is stored only in the deployment secret environment. It is never written to a Source Version, Repair Case, prompt log, frontend payload, or audit body.

Every invocation records:

- provider and API model;
- prompt/template version;
- bounded evidence hashes, not unrestricted page dumps;
- input/output token usage and cost;
- latency, retry, and error status;
- typed response schema version;
- Agent Run and Agent Step references.

DPV4 output is advisory typed JSON. Hermes decides which deterministic validation tool to run next.

## 7. Truth and Gate invariants

### 7.1 Local price facts

An observation can materialize into local-currency CurrentPrice/PriceHistory only when:

- Source Gate passes;
- Mapping Gate passes.

### 7.2 Source Gate

Source Gate requires:

- published Source Version;
- immutable official evidence;
- successful deterministic extraction;
- compatible country, currency, tax mode, validity, and semantic lane;
- no blocking source conflict;
- no unresolved result correction affecting the candidate.

### 7.3 Mapping Gate

Mapping Gate requires:

- an accepted exact link, override, or versioned automatic Mapping decision;
- no hard attribute contradiction;
- threshold and Top1/Top2 margin pass;
- accepted Mapping policy version;
- otherwise explicit human approval.

### 7.4 FX Gate

FX Gate controls derived EUR-normalized fields only. If it fails, local MSRP remains valid and the normalized field is marked pending or stale. FX failure must never freeze or alter official local-currency history.

### 7.5 Central eligibility service

All ingest, auto-review, manual review, remap, repair, replay, and rematerialization paths must reuse one central materialization eligibility service. No caller may duplicate or weaken the Gate logic.

## 8. Current baseline and realistic automation target

The current 2026-07-08 dryrun artifact contains 611 source targets:

| Current category | Count | Share | Intended route |
|---|---:|---:|---|
| complete pass | 199 | 32.6% | Source Gate candidate now |
| network/timeout/DNS/HTTP transient | 230 | 37.6% | deterministic Composer retry |
| last-known-good structural or URL drift | 21 | 3.4% | diff and known playbook |
| configured structural repair | 38 | 6.2% | parser/source playbook, optional DPV4 |
| anti-bot | 8 | 1.3% | bounded retry then manual URL/PDF |
| placeholder source URL | 115 | 18.8% | P0 source-fill session |

Engineering estimate, not yet production proof:

- current direct source pass: 199 of 611;
- deterministic Composer recovery: approximately 360 to 406 of 611;
- after versioned playbook iteration: approximately 394 to 455 of 611;
- after P0 fills the 115 placeholder targets and repeated verification runs: a 75% to 89% Source Gate range is a reasonable planning target.

The estimate must be replaced by measured rolling metrics after the database-backed Agent is running.

For new Mapping patterns, the existing CZ run produced 146 Review Cases and zero automatic approvals. Future coverage targets are:

- initial deterministic Mapping Engine: 50% to 65% coverage at at least 99.5% audited precision;
- after clustered reusable decisions: 75% to 85%;
- mature playbooks and scoped aliases: 85% to 92%;
- remaining ambiguous cases deliberately abstain.

These are acceptance targets, not evidence that the current implementation already achieves them.

## 9. Human interaction model

The operator should manage exceptions, not individual rows.

- Source Governance presents a dense target table and a right-side detail deck.
- An editor requests Hermes diagnosis rather than invoking DPV4 directly.
- An anti-bot Case presents Enter Official URL and Upload Official PDF as the primary actions.
- Source, parser, semantic, result, and FX Cases display different evidence and commands.
- Mapping exceptions deep-link to clustered Matching Review.
- One Mapping decision creates a scoped reusable rule and resolves the matching cluster.
- MSRP Monitor remains the business read surface and deep-links anomalies to Governance.

## 10. Rollout

### P0

- database-backed governance state;
- Source and Mapping Gate enforcement;
- Self-Iterating Agent runs with deterministic-first tools and versioned capability proposals;
- optional DPV4 provider for ambiguity;
- Source, parser, result, Mapping, FX, and runtime Case routing;
- targeted Dryrun/replay;
- high-confidence Mapping auto-accept;
- human Source Version and Composer policy publication.

### P1

- allowlisted low-risk source/parser actions can auto-publish after replay;
- versioned Composer policy promotion;
- repeated successful plan sequences become playbooks;
- clustered Mapping rules and threshold shadow evaluation;
- automatic rollback on measured regression.

### P2

- broader rosters;
- brand-family and country playbook libraries;
- stronger outcome comparison and cost routing;
- cross-country reuse where evidence and page-family fingerprints prove compatibility.
- reviewable extractor/tool/code PR proposals with generated regression fixtures.

No planned phase requires neural-network training or makes DPV4 the price judge. Any future training capability would require a separate explicit architecture and governance decision.

## 11. Implementation authorization

The user authorized implementation on 2026-07-14. Authorization follows the multi-session ownership contract: Governance, Source Repair, Monitor, and Hermes may change only their owned paths; shared registration and cross-module call sites remain integration-PR work.
