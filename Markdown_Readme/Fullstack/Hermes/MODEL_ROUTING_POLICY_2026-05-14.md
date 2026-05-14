# Hermes Model Routing Policy

> Created: 2026-05-14
> Based on: DeepSeek V4 Flash / Pro pricing (user-provided)
> Config: `hermes/model_pricing.yaml`

## 1. Purpose

Define when to use DeepSeek V4 Flash and when to use DeepSeek V4 Pro, based on task complexity, evidence availability, and cost.

Principle: **Flash first, Pro on demand.**

---

## 2. Model Summary

| Model | Input (cache miss) | Input (cache hit) | Output | Best For | Avoid For |
|---|---|---|---|---|---|
| **Flash** | 1 CNY/M | 0.02 CNY/M | 2 CNY/M | Direct lookup, short answer, VOC tagging, News summary, audit scoring, PRD intake | Strategic synthesis requiring deep cross-source reasoning |
| **Pro** | 3 CNY/M (discounted) | 0.025 CNY/M | 6 CNY/M (discounted) | Deep report, multi-source conflict, strategy judgement, executive synthesis | Simple lookups, speculative answers, insufficient-evidence cases |
| **Pro (original)** | 12 CNY/M | 0.1 CNY/M | 24 CNY/M | _(same)_ | After discount expires 2026-05-31, Pro is ~4x more expensive |

### Flash Pricing (standard, no discount period)

| | CNY per 1M tokens |
|---|---|
| Input (cache hit) | 0.02 |
| Input (cache miss) | 1.00 |
| Output | 2.00 |

### Pro Pricing (2.5折 discount until 2026-05-31)

| | Discounted CNY/M | Original CNY/M |
|---|---|---|
| Input (cache hit) | 0.025 | 0.10 |
| Input (cache miss) | 3.00 | 12.00 |
| Output | 6.00 | 24.00 |

**Note:** After discount expires, Pro cost increases ~4x. Review routing policy before 2026-05-31.

---

## 3. Country Assistant Routing

| Answer Mode | Default | Pro Allowed? | Condition for Pro |
|---|---|---|---|
| `direct_lookup` | Flash | No | — |
| `short_answer` | Flash | No | — |
| `grounded_analysis` | Flash | Yes | Multi-source conflict or strategy judgement |
| `deep_report` | **Pro** | Yes | Default to Pro for complex synthesis |
| `hypothesis` | Flash | No | Keep cost low for speculative answers |
| `insufficient_evidence` | Flash | No | Should not consume Pro budget |

### Hard Rules

- `direct_lookup` must not use Pro
- `insufficient_evidence` must not use Pro
- `deep_report` defaults to Pro
- `grounded_analysis` starts Flash, escalates to Pro only on multi-source conflict

---

## 4. Crawler & Pipeline LLM Tasks

| Task | Model | Reason |
|---|---|---|
| VOC tagging (sentiment/theme) | Flash | Classification task. Flash sufficient. |
| News summary (per country) | Flash | RSS summarization. High volume, keep cost low. |
| MSRP extraction | None (rules) | CSS/JSON extraction does not need LLM. |
| Cross-source strategy analysis | Pro | Multi-source strategic reasoning. |
| JATO fact explanation | Flash | Simple data narration. |

---

## 5. Hermes Internal Tasks (No LLM Required)

| Task | Model | Reason |
|---|---|---|
| PRD Intake | None | Deterministic keyword matching + registry cross-reference |
| Code Audit | None | Deterministic git diff scanning |
| Pipeline Audit | None | Deterministic registry cross-reference |
| Source Quality | None | Deterministic scoring rules |
| Evidence Extraction | None | Deterministic artifact parsing. No LLM for claim generation. |
| Answer Audit | None | Deterministic scoring formulas |
| Proposal Explanation | Flash (optional) | Light summary of structured proposal data |

---

## 6. Cost Control Rules

1. **Flash first, Pro on demand.** All tasks default to Flash unless explicitly routed to Pro.
2. **Pro gets top evidence only.** Do not send full raw text to Pro. Pre-filter evidence.
3. **Cache aggressively.** Use `promptVersion + inputHash` for cache key. Track cache hit/miss tokens.
4. **Record cost in answer audit.** Every Country Assistant answer must log `inputTokens`, `outputTokens`, `modelUsed`.
5. **Monthly budget:** 500 CNY. Warning at 75% (375 CNY).
6. **Pro discount expires 2026-05-31.** Re-evaluate Pro usage before then. Original Pro pricing is ~4x current.

---

## 7. Budget Tracking

| Threshold | CNY | Status |
|---|---|---|
| < 375 | — | OK |
| 375–500 | — | WARNING — review Pro usage |
| > 500 | — | EXCEEDED — all Pro requests must be justified |

Budget tracked via `hermes_cost_report.py` → `hermes/reports/cost_report.json`.

---

## 8. Cache Optimization

Cache hit vs miss has massive cost impact:

| Model | Cache Miss / Hit Ratio |
|---|---|
| Flash | 50x (1.0 vs 0.02 CNY/M) |
| Pro (discounted) | 120x (3.0 vs 0.025 CNY/M) |
| Pro (original) | 120x (12.0 vs 0.1 CNY/M) |

**Strategy:**
- Version all prompts (prompt version → cache key)
- Hash input data (snapshot fingerprint → cache key)
- Track `cacheHitInputTokens` vs `cacheMissInputTokens` per answer
- Target >50% cache hit ratio for Flash, >70% for Pro

---

## 9. Review Schedule

| Date | Action |
|---|---|
| 2026-05-25 | Review Pro discount expiration. Plan Pro usage for June. |
| 2026-06-01 | If discount expired, switch to original pricing in `model_pricing.yaml` and update routing defaults. |
| Monthly | Run `hermes_cost_report.py` and review budget status. |
