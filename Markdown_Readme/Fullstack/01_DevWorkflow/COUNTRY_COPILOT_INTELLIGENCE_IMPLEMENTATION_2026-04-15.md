# Country Copilot Intelligence Implementation 2026-04-15

Status: Active

## Current State

- Country chat now has **direct-answer routes** for `positioning-focus`, `segment-fuel-focus`, and `precise-lookup`, instead of dropping most narrow questions into a broad market overview.
- Backend responses now expose **`answerMode + grounding + contextSnapshot`**, and the frontend renders the grounded answer first on both the page and widget surfaces.
- `/copilot` now acts as the **mobile-web primary entry**: the top nav exposes Copilot directly, the floating widget is hidden on phone-sized coarse-pointer access, and the full page reuses compact grounded-answer / compact deck rendering without changing the desktop page into mobile mode.
- Mobile handoff is now explicit at the page layer through a **desktop continuation deep link** that carries country, chat model, and the latest question into `/copilot` on another device/browser session.
- Phone view now further compresses the **pre-transcript stack**: mobile hero/header is shorter, prompts collapse to the empty state, quick actions focus on desktop handoff, pending/loading uses compact rendering, and inline charts are capped more aggressively on phone only.
- Grounded answers now expose a **visible answer path** from existing metadata: route label, resolved focus tags, truth-layer read path, and output mode are rendered as a concise user-facing reasoning chain without exposing raw model chain-of-thought.
- Assistant replies now share one **answer-first scaffold** across page + widget: lead answer first, then key findings, then a visible thinking chain, then explanation/evidence/data layers, so every turn feels structured instead of dumping one long body.
- NVIDIA tool-first routes now **degrade cleanly** when function-calling does not converge in time: the backend raises into the normal provider fallback path, so users get a grounded local snapshot answer instead of seeing a raw “analysis interrupted / tool depth exceeded” message.
- NVIDIA tool execution is now **two-stage instead of recursive**: the model gets one tool-planning pass to request all needed tools, backend executes them in parallel-like batch, then a second no-tool pass forces the model to synthesize the final answer from the gathered evidence.
- `msrp_lookup_service.py` provides deterministic **CurrentPrice-based MSRP lookup** with source metadata and tier summaries.
- `engineering_variant_diff_service.py` provides deterministic **trim/version diff** from engineering normalized variants (`ConfigBaseVariant` + `ConfigMarketFeatureOverride`).
- Related news now acts as **question-scoped evidence** instead of always appearing as a standalone market snapshot block.
- Backend already has a **country news cache / digest layer**, but toolkit-side news freshness, PG persistence, and vector sync are still not one fully closed pipeline.

## Target State

- Country chat uses the existing deterministic routes as the default path for narrow asks, and only falls back to broad model composition when the question truly needs it.
- CurrentPrice lookup, variant diff, policy/news evidence, and future sales-truth joins converge into a narrower canonical tool layer.
- News / policy freshness closes the loop through persistent sinks and stable retrieval, not just toolkit-side collection.
- Grounding becomes more explicit about source tier, observed time, and official vs third-party provenance.
- Diff answers keep expanding from “A vs B trim” into powertrain, entry-vs-top, and cross-country comparisons.
- Mobile handoff eventually becomes a **true cross-device session continuation**, not only a deep link that preloads country/model/question.

## Affected Files

| File | Change Type | Dependencies |
|------|-------------|--------------|
| 06_AppPlatform/backend/app/infra/parquet_repository.py | modify | blocks latest-month trend fix |
| 06_AppPlatform/frontend/src/components/CountryChatAnalysisDeck.tsx | modify | depends on colors.ts palette helpers |
| 06_AppPlatform/frontend/src/utils/colors.ts | reference only | canonical powertrain palette source |
| 06_AppPlatform/backend/app/services/country_profiles.py | modify | blocks Finland market-context coverage |
| 06_AppPlatform/backend/tests/unit/test_country_chat_service.py | modify | validates Finland profile aliases |
| 06_AppPlatform/backend/tests/unit/test_parquet_repository.py | create | validates latest-12 month behavior |
| 07_ScrapingToolkit/jato_scraper/news_base.py | create | news batch/article types |
| 07_ScrapingToolkit/jato_scraper/news_config_loader.py | create | loads batch YAML configs |
| 07_ScrapingToolkit/jato_scraper/news_runner.py | create | RSS/Atom batch fetch runner |
| 07_ScrapingToolkit/news_sources/batch_a.yaml | create | first-batch country news feeds |
| 07_ScrapingToolkit/news_sources/batch_b.yaml | create | remaining data-country news feeds |
| 07_ScrapingToolkit/tests/test_news_runner.py | create | validates config loading and RSS parsing |

## Execution Plan

### Phase 1: Immediate Fixes

- [x] Step 1.1: Write down implementation plan in this markdown document.
- [x] Step 1.2: Fix monthly trend selection to use the latest rolling 12 months for filtered country snapshots.
- [x] Step 1.3: Fix CountryChatAnalysisDeck powertrain colors to use canonical mappings instead of positional palette order.
- [x] Step 1.4: Add Finland market profile so Finland can use policy/hot-topic context immediately.
- [x] Verify: backend unit tests + frontend type/build checks.

### Phase 2: First Country Batch Knowledge Expansion

- [x] Step 2.1: Extend country_profiles.py to the remaining user-prioritized batch A countries.
- [x] Step 2.2: Add alias coverage tests for FI/HU/CZ/SK/HR/SI/AT/CH/RO/GR/DK.
- [x] Verify: country chat unit tests and manual snapshot inspection for covered countries.

### Phase 3: News Intake Foundation

- [x] Step 3.1: Add a news observation schema and a lightweight batch runner under 07_ScrapingToolkit.
- [x] Step 3.2: Define batch A and batch B country source configuration for news ingestion.
- [ ] Step 3.3: Persist normalized news results for the app platform to consume.
- [x] Verify: config loading and RSS parsing tests pass for the new toolkit foundation.

### Phase 4: Data Completeness and UI Fallbacks

- [ ] Step 4.1: Audit empty cards in CountryChatAnalysisDeck by data dependency.
- [x] Step 4.2: Hide empty sections or render explicit no-data reasons instead of silent gaps.
- [ ] Step 4.3: Align PM workbench charts with current-year and selected-model scope rules.
- [x] Verify: frontend type checks, unit tests, and build pass after section-level empty-state changes.

### Phase 5: Grounded direct answers (completed 2026-04-17 ~ 2026-04-18)

- [x] Step 5.1: Add grounded answer payloads (`answerMode + grounding`) so answers render before the chart deck.
- [x] Step 5.2: Add route-aware deck narrowing and related-news evidence for narrow questions.
- [x] Step 5.3: Support follow-up inheritance for `segment + fuel + ranking` questions.
- [x] Verify: backend and frontend regression suites cover grounded answer rendering and narrowed deck behavior.

### Phase 6: Deterministic entity answers (completed 2026-04-18)

- [x] Step 6.1: Add CurrentPrice-backed direct MSRP lookup for named models / powertrains.
- [x] Step 6.2: Add engineering normalized variant diff so the assistant can answer trim/version comparison questions directly.
- [x] Step 6.3: Feed variant diff results back into grounding as `variantDiff`.
- [x] Verify: targeted backend tests cover MSRP lookup, compare-subject parsing, and engineering diff outputs.

### Phase 7: Next gaps (not yet complete)

- [ ] Step 7.1: Persist normalized news/policy data so Live retrieval stops depending on toolkit-only outputs.
- [ ] Step 7.2: Add version reconcile above `JatoMsrpLink + MatchOverride` so official / third-party / historical trims produce one reviewable current truth.
- [ ] Step 7.3: Join sales truth into diff answers so the assistant can say which JATO sales version each difference maps to.
- [ ] Step 7.4: Add review/workbench UI for low-confidence / 1-to-many / many-to-1 trim conflicts.

## Rollback Plan

If something fails:

1. Revert parquet_repository.py latest-month selection to the prior slice behavior.
2. Revert CountryChatAnalysisDeck.tsx to the existing PALETTE-only rendering path.
3. Remove newly added country profiles if any profile content causes unexpected regressions.

## Risks

- Precomputed yearMonth summaries may still need chronological normalization outside filtered country snapshots.
- Some workbench charts remain empty because source MSRP fields are genuinely missing, not because of rendering bugs.
- News scraping needs a separate normalization schema; MSRP extractors cannot be reused unchanged.

## Started Changes

- 2026-04-15: Started implementation with latest-month trend fix, fixed powertrain palette usage, and Finland profile coverage.
- 2026-04-15: Expanded batch A market profiles, added toolkit RSS/Atom news foundation with batch configs, and changed CountryChat deck empty sections into explicit reason panels.
- 2026-04-17 ~ 2026-04-18: Added grounded answers, route-aware evidence, follow-up inheritance, deterministic CurrentPrice MSRP lookup, and engineering-based variant diff direct answers.
- 2026-04-18: Promoted `/copilot` to a mobile-web entry surface with nav access, compact mobile rendering, collapsible secondary panels, and desktop-handoff deep links.
- 2026-04-19: Tightened phone-only density again by compressing the mobile hero/toolbar stack, hiding prompt chips after the conversation starts, using compact pending/loading, and capping inline chart count on phone without changing desktop behavior.
- 2026-04-19: Added a visible answer-path layer to grounded answers so users can see which route was chosen, what scope was resolved, which truth layers were read, and whether the answer was direct/model-assisted/fallback.
- 2026-04-19: Reorganized every grounded assistant reply into a stable answer-first order: direct answer → key findings → visible thinking chain → explanation → evidence tables → source layers, with the same structure shared by `/copilot` and the desktop widget.
- 2026-04-19: Changed NVIDIA tool-depth overflow from a user-visible interruption string into a normal fallback signal, and normalized provider error copy so Country Copilot falls back to local grounded answers without exposing raw internal failure text.
- 2026-04-19: Reworked NVIDIA tool-first execution into a bounded two-pass flow (`tool request -> tool results -> forced final answer`), so precise-lookup / positioning routes still use the LLM for synthesis but no longer recurse through repeated tool-calling rounds.
