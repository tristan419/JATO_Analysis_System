# AstrBot Composer Special-Case Inventory

## 1. Purpose

This inventory supports the current AstrBot goal: replace evaluation-question and model-specific answer branches with reusable intent/evidence composition.

Model names may remain inputs to entity extraction and retrieval. They must not select a hard-coded final answer.

## 2. Baseline

AST inspection of `jato_business_composer_service.py` on 2026-07-14 found:

- File size: 13,993 lines before the first dead-code removal.
- Functions: 571.
- Functions containing one or more tracked model literals: 51.
- Approximate code inside those functions: 3,001 lines.
- Tracked literals: J7, J8, O5, O9, Sorento, Sportage, EV3, EX30, RAV4, XC60.

This does not mean all 51 functions are invalid. The main distinction is whether a model literal is used for retrieval/entity handling or for selecting business conclusions.

## 3. Category A: Final-Answer Branches To Replace

These functions directly select conclusions, implications, actions, bullets, or report text from model/question matches. They are the highest-priority migration targets.

This is the historical baseline list. The 2026-07-14 migration results in sections 11-15 supersede the original status below.

| Function | Current role | Migration target |
|---|---|---|
| `_question_specific_executive_conclusion` | Large question/model switch for final conclusions | Intent-level conclusion composer driven by EvidencePackage signals |
| `_question_specific_business_implications` | Adds model/question-specific implications | Generic implication rules from evidence type, delta, confidence, and missing evidence |
| `_question_specific_recommended_actions` | Adds model/question-specific next actions | Tool/missing-evidence action registry |
| `_question_specific_policy_bullets` | O5 policy-specific report bullets | Generic policy applicability/status/effect/action block |
| `_question_specific_market_overview_bullets` | J7 market-fit bullets | Generic market opportunity block from segment/powertrain/channel evidence |
| `_compose_pricing_direct_answer` | Mixes generic pricing with O5/O9 branches | Generic relative/absolute pricing composer |
| `_pricing_o5_ev3_delta_verdict` | O5 vs EV3 fixed scenario verdict | Relative-price scenario evaluator using entities and price/config refs |
| `_pricing_o5_ev3_delta_phrase` | Fixed O5/EV3 wording | Generic target/competitor/delta phrase builder |
| `_compose_policy_direct_answer` | Contains O5 subsidy-cap branch | Generic policy evidence composer |
| `_compose_report_generation_direct_answer` | Contains O5/EX30/EV3 report branch | AnswerMode/report schema driven composer |
| `_report_generation_bullets` | Contains model-specific report structure | Generic report sections from conclusion/evidence/implication/action |
| `_report_ready_bullets` | Mixes intent output with model branches | Generic report projection from synthesis plan |
| `_market_fit_gap_conclusion` | J7-specific market-fit gap | Generic model opportunity gap from missing model evidence |
| `_market_hev_opportunity_cross_tab_conclusion` | HEV/J7 opportunity conclusion | Generic powertrain/segment opportunity composer |
| `_o5_bev_bonus_ended_recommended_actions` | O5-specific policy actions | Generic policy-status + pricing impact actions |

## 4. Category B: Scope Predicates To Remove

These predicates turn model/question strings into branch selectors. After migration, final composition must not call them.

- `_is_o5_ex30_ev3_scope`
- `_is_o5_ex30_ev3_report`
- `_is_o5_bev_subsidy_cap_question`
- `_is_j7_hev_market_fit_question`
- `_is_j7_hev_market_fit_public_plan`

Replacement inputs already exist or should be added to structured schemas:

- `EvidencePlan.intent`
- `EvidencePlan.entities.models`
- `EvidencePlan.entities.competitors`
- `EvidencePlan.entities.powertrains`
- `EvidencePackage.toolResults`
- `EvidencePackage.missingEvidence`
- `EvidencePackage.scopeDiagnostics`
- `answerMode`

## 5. Category C: Knowledge To Move Out Of Python

These functions encode product roles, competitor roles, or model material assumptions. Their content belongs in versioned Playbook/Method Card data or retrievable source material.

- `_o5_ex30_ev3_evidence_boundary`
- `_o5_ex30_ev3_role_line`
- `_o5_ex30_ev3_direct_evidence_by_model`
- `_o5_ex30_ev3_model_evidence_phrase`
- `_competitor_market_context_metric_specs`
- `_competitor_market_context_scenario_reason`
- `_market_fit_target_label`
- `_market_fit_gap_action`
- `_market_fit_matrix_action`

Required destination shape:

```text
MethodCard
  id
  version
  applicableIntent
  entitySelectors
  evidenceRequirements
  reasoningSteps
  outputSections
  sourceRefs
```

The Method Card can describe an analysis method. It must not store an unverified final verdict.

## 6. Category D: Temporary Entity/Display Helpers

These helpers do not necessarily generate a verdict. They may remain temporarily while entity extraction moves to structured planner output.

- `_pricing_model_markers_from_text`
- `_source_candidate_priority_tokens`
- `_user_title_from_plan`
- `_pricing_model_price_presence_line`
- `_relative_pricing_pair`

Constraints while retained:

- They may identify or display entities.
- They may not decide whether the target should win, be cheaper, enter a market, or use a specific product strategy.
- Prefer EvidencePlan entities over scanning question strings.
- Unknown models must work without adding their names to Python lists.

## 7. Category E: String-Based Evidence Classification

The following functions infer evidence role or source from J7/material naming. Replace these checks with explicit evidence metadata.

- `_relative_pricing_ref_is_user_material`
- `_is_pricing_user_material_ref`
- `_is_business_method_material_ref`
- `_pricing_user_material_ref_matches_scope`
- model-specific branches inside `_public_evidence_ref_label`
- model-specific branches inside `_public_evidence_ref_source`

Target EvidenceRef fields:

```text
evidenceStatus
sourceType
entityIds
claimType
scopeKey
periodType
periodStart
periodEnd
```

## 8. Dead Code Removed

`_j7_sportage_pricing_market_context_note` had no in-module callers and no repository references. It was removed as the first no-behavior-change cleanup.

## 9. Migration Order

### Step 1: Generic pricing pilot

Replace O5/EV3 and J7/Sportage final-answer selection with one relative-pricing path driven by:

- target and competitor entities;
- direction/delta supplied by the user;
- official target/competitor price refs;
- monthly/RV/TCO refs;
- configuration-value refs;
- scoped market context;
- explicit missing evidence.

Acceptance requirement: the same path must work for a blind model pair not named in current code.

### Step 2: Generic market opportunity

Replace J7/HEV market-fit branches with one powertrain/segment/channel opportunity composer.

Status: completed for direct answers, evidence-rich opportunity conclusions, and insufficient-evidence market-fit conclusions.

### Step 3: Generic competitor comparison

Move J8/Sorento and O5/EX30/EV3 roles to evidence or Method Cards.

Status: completed for generic competitor/report role selection and structural seven-seat/AWD scenario reasoning. Known-model fixtures remain as regression inputs, but model names no longer select the conclusion path.

### Step 4: Generic policy and report composition

Remove O5-specific policy/report predicates and generate report sections from structured synthesis output.

Status: report composition and the BEV subsidy price-cap branch are completed. Other policy question families remain separate generic policy work and must not reintroduce model-name selectors.

### Step 5: Remove string-based evidence classification

Use explicit EvidenceRef metadata instead of source/model substrings.

Status: completed for business-method material. New refs carry `sourceType`, `claimType`, `entityIds`, `country`, and `evidenceStatus`; legacy records use the registered Method Card scope instead of a model filename branch.

## 10. Guardrail

New product code must fail review if it uses a model name or golden-question phrase to select final conclusions, recommendations, report bullets, or business implications.

Allowed uses are limited to entity extraction, retrieval queries, source matching, and display labels.

## 11. Generic Pricing Pilot Result

Completed on 2026-07-14:

- Removed both O5/EV3 delta-question interception branches.
- Removed `_pricing_o5_ev3_delta_verdict` and `_pricing_o5_ev3_delta_phrase`.
- Reused `_generic_relative_pricing_direct_answer` for all relative-price questions.
- Structured `User supplied price-delta direction` now controls direction when question wording is ambiguous.
- A numeric delta appears in the conclusion only when `User supplied relative price delta` exists in the EvidencePackage.
- A blind `Aster Q HEV` / `Boreal One HEV` case follows the same code path and renders accepted MSRP evidence without adding either model to product code.
- A source-inspection guard test rejects J7/O5/EV3/Sportage/Tucson and blind-model literals inside the generic relative-pricing core.

Verification:

- Business Composer: `171 passed` after the pilot guard test.
- EvidencePackage + Business Composer + SSE: `252 passed` before the final guard-only test was added.

Remaining pricing-specific debt:

- O9 target-price wording and Method Card EvidenceRef classification were completed in the later generic-target-price and metadata migrations.
- Current price, monthly/RV/TCO, and configuration evidence coverage remain data/tool work rather than Composer branch work.

## 12. Generic Market-Opportunity Pilot Result

Completed on 2026-07-14:

- Replaced `_market_hev_opportunity_cross_tab_conclusion` with `_market_powertrain_opportunity_cross_tab_conclusion`.
- Removed the J7-only evidence-rich fallback conclusion.
- Replaced `_is_j7_hev_market_fit_question` and the J7-only public-plan predicate with generic market-fit predicates.
- Market opportunity now reads target model and powertrain from EvidencePackage entities, with a generic alphanumeric model-code fallback for incomplete legacy plans.
- Evidence-rich and evidence-missing paths both use the selected powertrain rather than assuming HEV.
- Removed model-specific entry wording such as warranty/high-trim/price-anchor claims from the insufficient-evidence conclusion.
- Blind `Lumen X BEV` tests cover both usable JATO cross-tabs and missing internal-market evidence.
- Source-inspection guard tests reject known and blind model names inside the generic market-opportunity core.

Verification:

- Business Composer: `174 passed`.
- EvidencePackage + Business Composer + SSE: `256 passed`.

Inventory after the pricing and market-opportunity pilots:

- File size: 13,997 lines.
- Functions: 573.
- Functions containing tracked model literals: 38, down from 51.
- Approximate code inside those functions: 2,355 lines, down from 3,001.

The largest remaining concentration is competitor/report/policy composition, plus string-based evidence classification.

## 13. Generic Competitor And Report Result

Completed on 2026-07-14:

- Removed the O5/EX30/EV3 report predicates and dedicated role/action helpers.
- Report composition now derives the subject and comparison set from EvidencePackage entities.
- Roles are described from available price, sales, configuration, segment, powertrain, channel, and missing-evidence signals; the code does not assign a named model as the fixed primary benchmark.
- Replaced J8/Sorento-specific structural reasoning with reusable seven-seat, AWD, electrified-powertrain, family/company-car, and high-trim scenario dimensions.
- Blind `Nova Prime / Atlas E / Vector Z` and `Orion Max / Titan Seven` tests use the same generic paths.
- Source-inspection guards reject known and blind model literals from the generic competitor/report and structural-competitor core.

## 14. Generic BEV Subsidy Price-Cap Result

Completed on 2026-07-14:

- Replaced `_is_o5_bev_subsidy_cap_question` with an intent/question-shape predicate that does not contain a model name.
- Replaced `_o5_bev_bonus_ended_recommended_actions` with target-entity-driven actions.
- The policy answer resolves its target from `EvidencePackage.entities.models` and handles active/applicable, ended/not-applicable, and unconfirmed-new-plan states without product-specific templates.
- Blind `Aurora E` coverage confirms that policy boundaries, implications, and next actions are generated without adding the model to product code.
- A source-inspection guard rejects O5/J7/EV3 and blind-model literals from the generic policy-cap core.

Verification after competitor/report, structural-competitor, and policy-cap migrations:

- Business Composer: `179 passed`.
- EvidencePackage + Business Composer + SSE: `261 passed`.
- File size: 13,993 lines.
- Functions: 576.
- Functions containing tracked model literals: 16, down from 51.
- Approximate code inside those functions: 1,062 lines, down from 3,001.

Remaining highest-priority debt:

1. Real MSRP, configuration, monthly/RV/TCO, BOM, VOC, and policy-source data coverage.
2. End-to-end blind questions that exercise planner, tools, EvidencePackage, SSE, and artifacts together.
3. Entity/retrieval alias centralization; this is maintenance work and must not reintroduce output branches.

## 15. Generic Target Price And Evidence Metadata Result

Completed on 2026-07-14:

- Removed the O9-specific target-range answer and its fixed `53k-55k` implication text.
- Generic target-price composition now reads target model, user range, price sample statistics, evidence gaps, and action placeholders from structured inputs.
- Blind `Nimbus E` target pricing preserves target/competitor separation and produces the same title, range-position, risk, and next-action structure.
- Business-method EvidenceRefs now use dynamic model labels and explicit `sourceType`, `claimType`, `entityIds`, `country`, and `evidenceStatus` metadata.
- Legacy method refs are scoped through their parent ToolEvidence and registered Method Card; country mismatches remain blocked without a model-specific filename condition.
- Removed remaining model literals from user-facing titles, implications, action summaries, executive conclusions, method competitor conclusions, and drivetrain conclusions.
- Blind `Aurora HEV` Method Card and drivetrain tests verify generic generation and entity rendering.

Final Composer inventory for this migration:

- File size: 14,106 lines.
- Functions: 582.
- Functions containing tracked model literals: 2, down from 51.
- Approximate code inside those functions: 43 lines, down from 3,001.
- Remaining functions: `_pricing_model_markers_from_text` and `_source_candidate_priority_tokens`; both are entity/retrieval helpers, not answer selectors.

Verification:

- Business Composer: `184 passed`.
- EvidencePackage + Business Composer + SSE: `267 passed`.
- Broad AstrBot/JATO Agent regression: `798 passed`.
- Source guards reject known and blind model names in the generic user-output core.
- The MCP route keeps the existing `直接结论` presentation contract by prefix-normalizing provider output without changing its evidence-backed business content.
