# MSRP Hermes Self-Iterating Agent

## Document status

- Status: confirmed specification; implementation owned by the Hermes session
- Date: 2026-07-14
- Implementation authorization: granted on 2026-07-14 within the Hermes-owned worktree
- Parent system: [MSRP Self-Healing Governance System](./msrp_self_healing_governance_system.md)
- Control-plane feature: [MSRP Source Governance Console](./feature.msrp_source_governance_console.md)
- Integration boundary: [MSRP Multi-Session Ownership and Integration Boundary](./msrp_multi_session_integration_boundary.md)

## 1. Executive decision

Hermes is the self-iterating Agent.

DPV4 is a stateless API-key-based LLM Provider used by Hermes. DPV4 is not a repair Agent, does not own workflow state, does not select production actions, and does not approve results.

Composer is Hermes' runtime planning core, but self-iteration is not limited to Composer, Playbook, or routing changes.

Hermes may improve, at different authority levels:

- episodic, semantic, and procedural memory;
- diagnosis taxonomy and failure classifiers;
- Composer policies and repair playbooks;
- prompts, evidence-pack composition, and model routing;
- deterministic tools, parser templates, and Mapping rules;
- evaluation fixtures, regression suites, and promotion criteria;
- bounded extractor/tool/code-change and schema-change proposals.

The boundary is authority, not component type. A low-risk runtime decision may execute automatically; a policy or tool candidate requires replay/shadow evaluation; code, schema, semantic-contract, hard-Gate, and deployment changes require normal human review.

No neural network is required. DPV4 remains a replaceable reasoning Provider rather than the owner of Hermes' learning.

## 2. Relationship to generic Hermes

Existing generic Hermes is a read-only-first governance layer with registries, ledgers, audits, cost monitoring, Sentinel, and a gated Ops Runner.

The MSRP Agent is a domain-scoped Hermes profile. It reuses generic Hermes primitives but receives only bounded MSRP repair authority through typed domain services.

Generic Hermes remains read-only by default. The MSRP Agent must not turn global Hermes into an unrestricted production operator.

Reusable existing capabilities:

- activity and evidence ledger patterns;
- DPV4 usage and cost accounting;
- model routing configuration;
- Sentinel and pipeline health findings;
- role, policy, feature-flag, lock, redaction, and allowlist patterns;
- source quality, Dryrun, and country progress artifacts;
- existing systemd scheduling and low-concurrency runner.

The existing generic Ops Runner is not the long-running Agent loop. Shared execution, lock, redaction, and audit primitives may be extracted or reused by a dedicated MSRP worker.

## 3. Agent loop

~~~mermaid
flowchart LR
    O["Observe<br/>health, evidence, cases, gates"] --> D["Diagnose<br/>deterministic classifier"]
    D --> M["Retrieve Memory<br/>playbooks, templates, outcomes"]
    M --> C["Composer / Planner"]

    C -->|"known deterministic issue"| T["Repair Tool Registry"]
    C -->|"semantic or structural ambiguity"| L["DPV4 API Provider"]
    L -->|"typed JSON"| C

    T --> V["Targeted Dryrun / Replay"]
    C --> V
    V --> E["Evaluate<br/>schema, evidence, Gate, regression"]
    E -->|"safe and policy-authorized"| A["Apply or propose"]
    E -->|"uncertain, anti-bot, high risk"| H["Human escalation"]
    A --> R["Record Repair Episode"]
    H --> R
    R --> P["Propose memory / policy / tool / evaluation improvement"]
    P --> S["Shadow replay"]
    S -->|"passes promotion policy"| M
~~~

Each loop is finite, resumable, idempotent, and auditable. The Agent must have explicit stop conditions and may not recursively call itself without a bounded run budget.

## 4. Composer contract

The Composer builds a typed plan from:

- target, source, country, brand, model, and semantic lane;
- current and last-known-good Source Versions;
- immutable evidence references;
- failure classifier output;
- recent run attempts;
- applicable playbooks and their success history;
- available tool schemas and risk classes;
- Source, Mapping, and optional FX Gate results;
- current Composer Policy Version;
- cost, time, and attempt budget.

Example plan:

~~~json
{
  "planSchemaVersion": "msrp-hermes-plan-v1",
  "repairDomain": "parser",
  "policyVersion": "composer-policy-12",
  "steps": [
    {
      "tool": "source.capture_official_evidence",
      "risk": "low",
      "inputRefs": ["evidence:current-url"],
      "successCondition": "http_200_and_official_domain"
    },
    {
      "tool": "parser.apply_known_template",
      "risk": "medium",
      "templateRef": "template:brand-family-v5",
      "successCondition": "typed_profile_valid"
    },
    {
      "tool": "validation.targeted_dryrun",
      "risk": "low",
      "successCondition": "source_gate_candidate"
    }
  ],
  "stopConditions": [
    "anti_bot_after_bounded_retry",
    "semantic_ambiguity_unresolved",
    "budget_exhausted",
    "blocking_gate_failure"
  ],
  "requiresHumanPublication": true
}
~~~

The Composer output is validated by Pydantic before execution. Unknown tools, free-form shell commands, unbounded URLs, and missing stop conditions are rejected.

## 5. Deterministic-first routing

Hermes must not call DPV4 for every failed source.

### 5.1 No LLM required

- network retry, exponential backoff, rate-limit handling;
- DNS and host health recheck;
- same official-domain redirect validation;
- known Source Template application;
- schema validation;
- known PDF/text/JSON extractor execution;
- known locale and currency normalization;
- exact Mapping link or override;
- Source/Mapping/FX Gate evaluation;
- replay and result comparison;
- case deduplication.

### 5.2 DPV4 may be useful

- ambiguous MSRP versus campaign/finance/lease semantics;
- previously unseen page structure;
- changed PDF table layout;
- selector, regex, JSON path, or page-region proposal;
- multilingual model/trim/powertrain alias proposal;
- explanation of why deterministic evidence is insufficient.

### 5.3 Human evidence required

- persistent anti-bot or login wall;
- no official price source can be reached;
- official source does not publish MSRP;
- model appears discontinued;
- canonical official sources conflict without stronger evidence;
- legal/business meaning cannot be proven;
- proposed action would change a high-risk policy.

In the anti-bot lane, Hermes stops automated retries and asks the editor to enter an official URL or upload an official PDF. A screenshot may support diagnosis, but a screenshot alone is not sufficient canonical price evidence unless the evidence policy explicitly validates its official origin, capture time, and extracted text.

## 6. Repair Tool Registry

Every tool has a typed input/output schema, risk class, idempotency behavior, timeout, evidence requirements, and allowed actor.

### 6.1 Source tools

- source.retry_fetch
- source.validate_official_url
- source.follow_same_domain_redirect
- source.capture_official_evidence
- source.attach_uploaded_pdf
- source.compare_last_known_good
- source.mark_manual_evidence_required
- source.create_version_proposal

### 6.2 Parser tools

- parser.inspect_deterministic_signals
- parser.apply_known_template
- parser.validate_selector
- parser.validate_regex
- parser.validate_json_path
- parser.extract_pdf_pages
- parser.validate_typed_profile
- parser.serialize_profile_yaml

### 6.3 Semantic and result tools

- result.classify_semantic_lane
- result.reprocess_evidence
- result.compare_observation_set
- result.create_correction_decision
- result.invalidate_materialization_candidate
- result.rematerialize_through_gate

These tools preserve original observations. They never update a fact row in place.

### 6.4 Mapping tools

- mapping.generate_candidates
- mapping.apply_hard_constraints
- mapping.score_candidates
- mapping.evaluate_margin
- mapping.apply_scoped_rule
- mapping.abstain_to_review_cluster
- mapping.replay_policy

DPV4 can propose an alias but cannot call mapping.apply_scoped_rule as an approver.

### 6.5 FX tools

- fx.fetch_approved_rate
- fx.validate_rate_date
- fx.compare_provider_result
- fx.recompute_eur_normalized
- fx.mark_normalization_pending

FX tools never modify official local MSRP.

### 6.6 Validation tools

- validation.targeted_dryrun
- validation.replay_historical_evidence
- validation.evaluate_source_gate
- validation.evaluate_mapping_gate
- validation.evaluate_fx_gate
- validation.compare_champion_challenger
- validation.detect_regression

### 6.7 Governance tools

- governance.open_or_update_case
- governance.attach_evidence
- governance.create_proposal
- governance.submit_for_approval
- governance.record_human_escalation
- governance.rollback_authorized_version

P0 does not expose a general production publish tool to the Agent.

### 6.8 Knowledge and evaluation tools

- knowledge.retrieve_similar_episodes
- knowledge.propose_failure_taxonomy
- knowledge.propose_scope_fingerprint
- evaluation.create_regression_fixture
- evaluation.extend_failure_corpus
- evaluation.compare_policy_versions
- evaluation.score_capability_proposal
- evaluation.detect_blind_spot

Hermes may add evidence-backed evaluation cases automatically. Changing a blocking acceptance criterion or removing a regression case is protected and requires human approval.

### 6.9 Development proposal tools

- development.propose_extractor
- development.propose_tool_schema
- development.generate_deterministic_tests
- development.generate_code_patch
- development.generate_migration_plan
- development.prepare_pr_evidence

These tools produce reviewable artifacts only. They cannot edit the active runtime, merge a branch, apply a migration, deploy, or expand their own permissions.

## 7. DPV4 API Provider

### 7.1 Runtime configuration

DPV4 is configured through server secrets and model routing:

~~~text
DEEPSEEK_API_KEY=<secret>
MSRP_HERMES_LLM_PROVIDER=dpv4
MSRP_HERMES_LLM_MODEL=deepseek-v4-flash
MSRP_HERMES_LLM_PRO_MODEL=deepseek-v4-pro
MSRP_HERMES_LLM_BASE_URL=https://api.deepseek.com
~~~

The existing OpenAI-compatible chat client should be reused through a Dpv4ChatClient adapter. The existing DPV4 usage ledger and model-pricing configuration remain the cost source.

### 7.2 Model routing

- Flash is the default for classification and profile hints.
- Pro is allowed only for policy-approved complex ambiguity, such as multi-source semantic conflict.
- deterministic tasks use no LLM;
- a provider failure never blocks manual or known-template repair;
- daily and monthly budgets, concurrency, timeout, and retry are enforced before the call.

### 7.3 Evidence boundary

DPV4 receives:

- official-domain evidence only;
- sanitized excerpts with page/DOM references;
- content hashes;
- current typed profile and allowed schema;
- deterministic findings;
- last-known-good diff;
- bounded candidate roster for Mapping assistance.

It does not receive unrestricted database exports, API keys, cookies, arbitrary third-party browsing, or executable credentials.

### 7.4 Output contract

DPV4 returns typed JSON such as:

~~~json
{
  "taskType": "parser_patch_proposal",
  "semanticLane": "base_msrp",
  "profilePatch": {},
  "mappingAliasProposals": [],
  "evidenceReferences": [],
  "assumptions": [],
  "unresolvedQuestions": [],
  "riskFlags": [],
  "confidence": 0.0
}
~~~

Hermes validates this output and composes deterministic verification steps. A high DPV4 confidence value is never itself a publication Gate.

## 8. Repair Episode memory

The unit of self-iteration is a verified Repair Episode:

| Field group | Contents |
|---|---|
| context | country, brand, host, page/PDF fingerprint, extractor, semantic lane, failure class |
| plan | Composer Policy Version, playbook, ordered steps, budgets |
| calls | tool inputs/outputs, DPV4 invocation metadata, evidence hashes |
| evaluation | schema, Dryrun, replay, Source/Mapping/FX Gate results |
| decision | automatic low-risk action, human publication, rejection, rollback |
| outcome | next-run health, recurrence, extracted count, false-positive/regression flags |

Episodes are immutable. A later observation appends outcome evidence rather than rewriting the original episode.

## 9. Layered self-iteration

Hermes may evolve multiple capability layers. Every candidate records its scope, evidence, evaluator, authority class, and rollback path.

### 9.1 Memory and knowledge

Hermes may improve:

- episodic memory of what happened in each Repair Episode;
- semantic memory of source families, failure taxonomies, aliases, and evidence patterns;
- procedural memory of successful tool sequences;
- recurrence links and last-known-good comparisons;
- retrieval scope and context fingerprints.

Memory can influence planning but cannot become price evidence by itself.

### 9.2 Diagnosis and planning

Hermes may improve:

- failure classification;
- classifier-to-playbook routing;
- Composer plan structure;
- tool order;
- retry/timeout/host-circuit budget;
- escalation and abstention;
- Dryrun and replay scope.

### 9.3 Prompt and model routing

Hermes may propose and evaluate:

- prompt versions and typed-output instructions;
- evidence-pack selection and compression;
- Flash versus Pro versus no-LLM routing;
- LLM retry, cache, timeout, and cost policy;
- critic/verifier passes for structurally ambiguous proposals.

A prompt or model change is promoted by replay and schema/Gate outcomes, not by subjective response quality alone.

### 9.4 Templates, Mapping, and deterministic tools

Hermes may create candidate versions of:

- Source and parser templates;
- selector/regex/JSON/PDF extraction patterns;
- locale/normalization rules;
- scoped Mapping aliases and rules;
- FX normalization procedures under an approved provider policy;
- tool schemas, preconditions, stop conditions, and success conditions.

The candidate is inactive until its authority-specific validation and approval complete.

### 9.5 Evaluation-system iteration

Hermes should improve the system that evaluates itself:

- convert every verified failure into a regression fixture;
- generate replay suites by country, brand, host/page family, and repair domain;
- identify evaluation blind spots;
- propose stronger success criteria;
- compare champion/challenger policies and tools;
- monitor delayed outcomes, recurrence, and rollback.

Hermes may add a regression case automatically. Weakening a blocking Gate, deleting a failing fixture, or changing business truth requires human approval.

### 9.6 Tool and code evolution

When repeated verified Cases cannot be solved by current tools, Hermes may produce:

- a new extractor or repair-tool design;
- deterministic fixtures from immutable evidence;
- code and test patches;
- a migration proposal when persistent schema is needed;
- documentation and a PR evidence package.

This is still self-iteration, but activation follows the normal development path. Hermes cannot merge, migrate, deploy, modify production secrets, or grant itself a tool.

### 9.7 Contract and product-policy proposals

Hermes may detect that a new semantic lane, repair domain, evidence contract, Mapping hard constraint, canonical-source policy, or FX policy is needed.

It may produce an impact report and proposal. These changes redefine business or data truth and always require explicit human architecture approval.

### 9.8 Promotion process

~~~text
observed episodes and failures
  -> capability candidate
  -> typed/schema validation
  -> historical replay
  -> shadow execution
  -> champion/challenger comparison
  -> authority-specific approval
  -> versioned activation or normal code PR path
  -> delayed outcome monitoring
  -> automatic rollback or human revert
~~~

Suggested default for a reusable low-risk candidate:

- at least three successful comparable Episodes;
- at least two distinct targets when claiming reusable scope;
- 100% pass on blocking Gate replay;
- no loss of evidence traceability;
- no increase in wrong-semantic or wrong-Mapping decisions;
- cost and runtime remain within policy;
- scope is explicit by country, brand, host/page family, and semantic lane.

These defaults are policy, not universal constants. A candidate's risk and blast radius determine stronger requirements.

### 9.9 Authority boundary

Hermes may create a proposal for almost any capability layer. It may activate only what an already approved authority policy allows.

Protected actions always require humans:

- weakening evidence requirements or a blocking Gate;
- editing accepted local MSRP/history in place;
- canonical-source, semantic-lane, Mapping-hard-constraint, or FX-provider policy;
- new tool permission;
- schema migration;
- code merge or deployment;
- secret or credential changes.

### 9.10 No required neural-network learning

This design has no model-training requirement, online gradient update, embeddings-only truth, or hidden weight mutation.

Improvement initially comes from:

- exact/scoped rules and versioned templates;
- verified memory retrieval;
- success-rate and outcome comparison;
- prompt/model routing evaluation;
- deterministic replay;
- new tool and code proposals when existing capability is insufficient.

If neural-network training is proposed later, it is a separate explicit architecture decision with its own data, evaluation, privacy, and rollback design.

## 10. Risk classification and authority

### Low risk: automatic in P0

- retry/backoff;
- host circuit break and resume;
- same official-domain redirect validation;
- evidence freshness check;
- known-template Dryrun;
- case deduplication;
- exact accepted Mapping reuse;
- high-confidence Mapping decision that passes hard constraints and policy;
- FX recomputation using an already approved provider/rate policy.

### Medium risk: automatic proposal and verification

- selector, regex, JSON path, or PDF page changes;
- known extractor-type switch;
- semantic reclassification proposal;
- new scoped alias;
- result correction;
- candidate Composer playbook change.
- prompt/model-routing candidate;
- deterministic tool/template candidate;
- new regression fixture or evaluation-suite candidate;
- extractor/tool/code patch proposal without activation.

P0 produces a validated proposal and awaits publication or approval.

### High risk: human decision

- canonical source priority;
- new official domain;
- base MSRP versus offer/finance ambiguity;
- material result invalidation affecting published history;
- new Mapping hard constraint;
- FX provider or rate policy change;
- broader Agent auto-action authority;
- evidence, semantic, or Gate contract change;
- new tool permission;
- code merge, migration, or deployment.

## 11. Data model

Recommended PostgreSQL entities:

### msrp.hermes_agent_runs

- run ID, trigger, target/case references;
- status and current step;
- Composer Policy Version;
- attempt, time, token, and cost budgets;
- started/completed timestamps;
- final outcome and escalation reason.

### msrp.hermes_agent_steps

- run and sequence;
- tool or DPV4 invocation;
- typed input/output references;
- risk, idempotency key, status, duration;
- evidence and evaluation references.

### msrp.repair_episodes

- immutable context, plan, evaluation, decision, and outcome snapshots;
- recurrence and regression links;
- reusable-scope fingerprint.

### msrp.composer_policy_versions

- immutable classifier routing, budgets, LLM rules, promotion policy;
- draft, shadow, active, rejected, superseded, rolled_back states;
- creator, approver, activation, and rollback links.

### msrp.repair_playbook_versions

- repair domain and scope;
- ordered typed steps;
- preconditions, stop conditions, success conditions;
- evidence requirements;
- performance counters derived from Episodes.

### msrp.llm_invocations

- Agent Run/Step;
- provider/model and prompt version;
- evidence hashes;
- tokens, cost, latency, status;
- response schema/version and output hash.

### msrp.capability_proposals

- capability type: memory, taxonomy, prompt, policy, playbook, template, tool, evaluation, code, schema, or contract;
- proposed scope and authority class;
- source Episodes and evidence;
- typed diff or artifact references;
- replay/shadow/evaluation result;
- approval, activation, PR, rollback, or rejection state.

### msrp.evaluation_suite_versions

- immutable regression fixtures and scope;
- required Source/Mapping/FX Gate assertions;
- added/removed fixture audit;
- champion/challenger comparison result.

### msrp.prompt_policy_versions

- prompt templates, evidence-pack rules, DPV4 routing, schema version, and budgets;
- draft, shadow, active, superseded, rejected, rolled_back states.

Code and migration proposals may remain repository artifacts linked from capability_proposals rather than executable database content.

The Source Governance service owns Source Versions, Evidence Assets, Cases, and Proposals. Agent tables reference those IDs instead of duplicating governance state.

## 12. Backend stack and reuse

Use the existing FastAPI, Pydantic, SQLAlchemy, PostgreSQL, Alembic, Python Scraping Toolkit, and systemd deployment stack.

Reuse:

- existing MsrpSource, MsrpObservation, CurrentPrice, PriceHistory, JatoMsrpLink, and MatchOverride objects;
- source loader and extractor implementations;
- dryrun aggregation and source audits;
- canonical Mapping resolver and link service;
- DPV4 usage monitor and model-pricing registry;
- activity/evidence ledger patterns;
- existing source-health scheduling and low-concurrency execution.

Add a dedicated worker with database-backed runs and idempotent steps. Do not create a second unrelated scheduler and do not duplicate materialization rules in Agent code.

## 13. API boundary

Proposed prefix: /v1/msrp/hermes-agent

### Query

| Method | Path | Purpose |
|---|---|---|
| GET | /runs | filter Agent Runs |
| GET | /runs/{run_id} | plan, steps, evidence, evaluation, outcome |
| GET | /episodes | repair history and recurrence |
| GET | /policies | Composer Policy Versions |
| GET | /playbooks | active and candidate playbooks |
| GET | /capabilities | memory/prompt/tool/evaluation/code proposals |
| GET | /evaluation-suites | regression fixtures and version results |
| GET | /metrics | automation, repair success, cost, regression |
| GET | /provider-status | DPV4 availability and budget without exposing a key |

### Commands

| Method | Path | Minimum actor | Purpose |
|---|---|---|---|
| POST | /runs | editor/service | request a bounded diagnosis/repair run |
| POST | /runs/{run_id}/cancel | editor/admin | stop future steps |
| POST | /runs/{run_id}/resume | editor/admin | resume from a safe checkpoint |
| POST | /policies/{id}/shadow | admin | start replay/shadow evaluation |
| POST | /policies/{id}/activate | admin | activate a validated policy |
| POST | /policies/{id}/rollback | admin | restore the prior policy |
| POST | /playbooks/{id}/approve | admin | approve a candidate playbook |
| POST | /capabilities/{id}/shadow | admin | evaluate a capability candidate |
| POST | /capabilities/{id}/approve | admin | approve authority-eligible activation or PR handoff |
| POST | /capabilities/{id}/reject | admin | reject with evidence/reason |

Source publication remains in Source Governance APIs.

The integration dispatcher submits Governance `AgentRunRequest v1` to `POST /runs`. Hermes creates typed repair Proposals through the registered Governance Case and posts terminal `AgentRunResult v1` to `/v1/msrp/source-governance/cases/{caseId}/agent-run-results`. Governance rejects a run or Proposal reference not already associated with that Case. Agent plans, steps, LLM invocations, token usage, and DPV4 Provider state remain persisted and queried from this Hermes API rather than duplicated in Governance.

## 14. Scheduling and concurrency

Triggers:

- scheduled source health findings;
- repeated comparable failures;
- evidence or content hash changes;
- Mapping review feedback;
- Monitor anomaly feedback;
- manual editor request;
- scheduled outcome evaluation for previous Episodes.

Execution requirements:

- database-backed queue or outbox;
- host-aware low concurrency;
- per-run and per-tool timeout;
- distributed/idempotent lock;
- resume from a safe step;
- cancellation between steps;
- DPV4 concurrency and budget guard;
- no blocking sleep loop;
- all errors mapped to typed Agent outcomes.

## 15. Console interaction

The Agent has no standalone end-user page in P0. Source Governance is its control surface.

The right-side detail deck adds:

- Agent Runs: plan, current step, stop condition, retry budget;
- Repair Episodes: previous attempts and later outcome;
- Learned Playbooks: active scope, success count, replay result;
- Composer Policy: active/challenger version and change diff;
- Capabilities: memory, prompt, template, tool, evaluation, and code proposals;
- Evaluation Suites: new failure fixtures and champion/challenger result;
- DPV4 Usage: provider/model, tokens, cost, status;
- Dryrun/Replay: extracted/rejected rows and Gate comparison.

The primary command is Request Hermes Diagnosis. There is no Run DPV4 button. When manual evidence is required, the UI surfaces Enter Official URL and Upload Official PDF.

## 16. Metrics

Operational metrics:

- source automatic pass and recovery rate;
- repair first-attempt and eventual success;
- recurrence and reopen rate;
- mean steps and time to resolution;
- percentage solved without DPV4;
- DPV4 calls, tokens, cost, latency, schema failures;
- manual evidence requirement rate;
- Source and Mapping Gate coverage;
- clustered Mapping review rate;
- Composer policy promotion and rollback count;
- capability proposals by type, approval, activation, PR, and regression;
- evaluation blind spots found and regression fixtures added;
- prompt/model/tool version success and rollback rate;
- wrong-semantic, wrong-Mapping, and result-correction regression rate.

The system optimizes safe coverage and human minutes saved, not maximum automatic approval.

## 17. Rollout

### P0: observable self-iterating Agent

- persistent Agent Run, Step, Episode, Policy, Playbook, and LLM invocation records;
- deterministic failure routing;
- Source/parser/result/Mapping/FX/runtime tool registry;
- DPV4 Provider adapter and budget;
- targeted Dryrun and historical replay;
- human publication of Source Versions and Composer policies;
- low-risk retry and high-confidence Mapping automation.
- versioned memory, prompt, evaluation, and capability proposal records.

### P1: bounded self-iteration

- candidate playbooks from repeated successful Episodes;
- shadow replay and champion/challenger comparison;
- allowlisted low-risk policy auto-promotion;
- automatic rollback on regression;
- scheduled outcome evaluation;
- reduced repetitive Mapping Review through scoped rules.
- prompt/model-routing, template, taxonomy, and evaluation-suite iteration.

### P2: capability evolution

- broader brand/page-family playbooks;
- country and host-specific orchestration;
- cost-aware Flash/Pro/no-LLM routing;
- cross-country playbook reuse only after fingerprint and replay validation.
- new extractor/tool/code PR proposals with generated deterministic fixtures;
- contract and schema impact proposals that remain human-approved.

No planned phase requires neural-network training. Any future training proposal is a separate explicit architecture decision.

## 18. Acceptance criteria

P0 is accepted when:

1. Hermes runs a finite typed Composer plan without an open Codex/ChatGPT session.
2. deterministic failures do not consume DPV4 tokens.
3. semantic or structural ambiguity can call DPV4 through a server-side API key and store typed metadata.
4. DPV4 cannot write a Source Version, Mapping decision, CurrentPrice, or PriceHistory.
5. all tool calls are typed, idempotent where applicable, auditable, and bounded.
6. source, parser, semantic, result, Mapping, FX, and runtime repair domains route to distinct playbooks.
7. persistent anti-bot failures stop and request official URL/PDF evidence.
8. Result repair preserves the old observation and uses Gate-controlled reprocessing.
9. FX repair changes only derived normalization, never official local MSRP.
10. each completed run produces an immutable Repair Episode and later outcome evaluation.
11. a candidate policy can be replayed, compared, approved, activated, and rolled back.
12. Hermes can propose improvements across memory, diagnosis, prompts, policies, templates, tools, evaluation, code, and contracts, while authority policy prevents protected changes from self-activation.
13. Source and Mapping Gate remain central and cannot be bypassed by an Agent tool.
14. Console users can inspect every plan, DPV4 call, Dryrun, decision, and rollback.
15. a recurring unsupported parser pattern can produce a reviewable extractor/tool/code proposal with deterministic fixtures, without self-merge or deployment.

## 19. Test strategy

### Unit and schema

- Composer plan validation and unknown-tool rejection;
- deterministic-versus-DPV4 routing;
- stop condition and budget enforcement;
- Repair Episode immutability;
- Composer Policy and Playbook state transitions;
- capability proposal authority and promotion transitions;
- evaluation-suite append/protected-removal behavior;
- prompt/model-routing replay;
- DPV4 typed response and secret-redaction tests;
- result and FX repair invariants.

### Integration

- transient failure to automatic recovery;
- parser drift to proposal and Dryrun;
- anti-bot to manual_evidence_required;
- uploaded official PDF to resumed Agent validation;
- result correction without in-place fact mutation;
- FX recomputation preserving local price;
- Mapping abstention to Review and scoped-rule replay;
- policy shadow comparison and rollback.
- extractor/tool/code proposal generation without activation.

### Safety

- DPV4 outage falls back to deterministic/manual flow;
- prompt injection inside official page text cannot select tools;
- arbitrary URL, shell, code-deploy, and direct database-write attempts are rejected;
- high-risk changes cannot auto-publish in P0;
- a failed or cancelled run leaves last-known-good state unchanged.

## 20. Non-goals

- neural-network training, fine-tuning, or online weight updates in this Feature;
- an autonomous price judge;
- unrestricted browser exploration;
- bypassing anti-bot through uncontrolled techniques;
- self-merge, automatic migration, automatic production code activation, or deployment;
- direct CurrentPrice/PriceHistory edits;
- replacing official evidence with model confidence;
- replacing Source Governance or Matching Review.

## 21. Implementation authorization

The user authorized development on 2026-07-14. Hermes implementation remains owned by its separate worktree/PR and consumes the frozen Governance contracts; the Governance branch does not modify Composer, Playbooks, DPV4 provider code, or Agent runtime registries.
