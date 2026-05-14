# Hermes Cost Report

**Generated:** 2026-05-14T15:50:45Z

## 1. Summary

| Metric | Value |
|---|---|
| Currency | CNY |
| Total estimated cost | 0.2638 CNY |
| Total input tokens | 97,000 |
| Total output tokens | 17,400 |
| Cache-hit input tokens | 0 |
| Cache-miss input tokens | 97,000 |
| Cache hit ratio | 0.0% |
| Budget | 500 CNY |
| Budget status | ok |

## 2. Cost by Model

| Model | Records | Input Tokens | Output Tokens | Estimated Cost (CNY) | Discount |
|---|---:|---:|---:|---:|
| deepseek-v4-flash | 10 | 47,000 | 9,400 | 0.0658 | none |
| deepseek-v4-pro | 2 | 50,000 | 8,000 | 0.1980 | active |

## 3. Cost by Answer Mode

| Answer Mode | Records | Model Mix | Estimated Cost (CNY) |
|---|---:|---:|---:|
| direct_lookup | 4 | deepseek-v4-flash | 0.0148 |
| grounded_analysis | 2 | deepseek-v4-flash | 0.0320 |
| deep_report | 2 | deepseek-v4-pro | 0.1980 |
| hypothesis | 2 | deepseek-v4-flash | 0.0148 |
| insufficient_evidence | 2 | deepseek-v4-flash | 0.0042 |

## 4. Top Expensive Records

| Answer ID | Mode | Model | Input | Output | Cost (CNY) |
|---|---|---:|---:|---:|
| `...0260514140512.102d26` | deep_report | deepseek-v4-pro | 25,000 | 4,000 | 0.099000 |
| `...0260514155045.3979f6` | deep_report | deepseek-v4-pro | 25,000 | 4,000 | 0.099000 |
| `...0260514140512.8ace5b` | grounded_analysis | deepseek-v4-flash | 12,000 | 2,000 | 0.016000 |
| `...0260514155045.03d0b8` | grounded_analysis | deepseek-v4-flash | 12,000 | 2,000 | 0.016000 |
| `...0260514140512.cc3369` | hypothesis | deepseek-v4-flash | 5,000 | 1,200 | 0.007400 |
| `...0260514155045.2e7660` | hypothesis | deepseek-v4-flash | 5,000 | 1,200 | 0.007400 |
| `...0260514140512.8efe77` | direct_lookup | deepseek-v4-flash | 3,000 | 800 | 0.004600 |
| `...0260514155045.b027ea` | direct_lookup | deepseek-v4-flash | 3,000 | 800 | 0.004600 |
| `...0260514140512.779f4a` | direct_lookup | deepseek-v4-flash | 2,000 | 400 | 0.002800 |
| `...0260514155045.a942a1` | direct_lookup | deepseek-v4-flash | 2,000 | 400 | 0.002800 |

## 6. Notes

- Pricing loaded from `hermes/model_pricing.yaml`. Verify against DeepSeek billing console.
- Cache split fields (cacheHitInputTokens/cacheMissInputTokens) not yet present in audit records — assuming all cache-miss.
- Pro discount (2.5折) is time-limited. Review cost estimates before 2026-05-31.
