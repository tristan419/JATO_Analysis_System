# Hermes Answer Audit Report

**Generated:** 2026-05-14T14:05:12Z

## 1. Summary

| Metric | Value |
|---|---|
| Total audit records | 6 |
| Average groundedness | 0.72 |
| Average hallucination risk | 0.32 |
| Pro usage count | 1 |
| Flash usage count | 5 |
| Total estimated cost | \$0.0620 |

## 2. Sample Audits

| Answer ID | Mode | Model | Evidence | Groundedness | Hallucination Risk | Cost |
|---|---|---:|---:|---:|---:|
| `...0260514140512.8efe77` | direct_lookup | deepseek-v4-flash | 1 | 1.0 | 0.3 | \$0.0006 |
| `...0260514140512.8ace5b` | grounded_analysis | deepseek-v4-flash | 3 | 0.9 | 0.3 | \$0.0022 |
| `...0260514140512.102d26` | deep_report | deepseek-v4-pro | 3 | 0.9 | 0.3 | \$0.0574 |
| `...0260514140512.cc3369` | hypothesis | deepseek-v4-flash | 0 | 0.5 | 0.3 | \$0.0010 |
| `...0260514140512.779f4a` | direct_lookup | deepseek-v4-flash | 2 | 1.0 | 0.3 | \$0.0004 |
| `...0260514140512.60a2e7` | insufficient_evidence | deepseek-v4-flash | 0 | 0.0 | 0.4 | \$0.0003 |

## 3. Recommendations

- [ ] 1 Pro model usages — verify if Flash would suffice for lower-cost alternatives
- [ ] 1 answers with low groundedness — insufficient evidence or no tools used
- [ ] 4 answers could be cached to reduce API cost
