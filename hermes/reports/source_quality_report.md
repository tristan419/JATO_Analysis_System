# Hermes Source Quality Report

**Generated:** 2026-05-14T14:05:12Z

## 1. Summary

| Metric | Value |
|---|---|
| Total sources | 7 |
| Healthy | 0 |
| Watch | 4 |
| Degraded | 3 |
| Disabled candidate | 0 |
| High risk | 1 |
| Unstructured failures | 2 |

## 2. Source Health

| Source ID | Type | Country | Status | Score | Risk | Recommendation |
|---|---|---|---:|---|---|
| `source.msrp.batch_a` | msrp | SE,FI,NO,DK,HU,HR,AT,CZ | degraded | 30 | high | Consider degrading or reducing frequency. Issues: repeated failures (failedCount |
| `source.voc.batch_a` | forum | SE,FI,NO,DK,AT,CZ,HR,HU | degraded | 40 | medium | Consider degrading or reducing frequency. Issues: repeated failures (failedCount |
| `source.news.batch_b` | news | DE,FR,IT,ES,BE,NL,PL,PT,RO,SI,SK,GR | degraded | 55 | medium | Consider degrading or reducing frequency. Issues: no recent success recorded; go |
| `source.news.batch_a` | news | SE,FI,HU,NO,DK,AT,CZ,HR | watch | 70 | low | Watch. Issues: incomplete metadata (3/3 quality fields null); blocking issue: Go |
| `source.msrp.production` | msrp | SE,DE | watch | 70 | low | Watch. Issues: no recent success recorded; incomplete metadata (3/3 quality fiel |
| `source.msrp.evkx` | official | multi | watch | 70 | low | Watch. Issues: no recent success recorded; incomplete metadata (3/3 quality fiel |
| `source.msrp.drafts_suv_top30` | msrp | SE,FI,NO,DK,HU,HR,AT,CZ | watch | 70 | low | Watch. Issues: no recent success recorded; incomplete metadata (3/3 quality fiel |

## 3. Unstructured Failures

- **[source.voc.batch_a]** 8 failures without per-source structured tracking (sourceId, url, error type, retryable)
  - Recommendation: Add per-source failed_sources.json output to the crawler. Track source code, error type, and timestamp per failure.
- **[source.msrp.batch_a]** 117 failures without per-source structured tracking (sourceId, url, error type, retryable)
  - Recommendation: Add per-source failed_sources.json output to the crawler. Track source code, error type, and timestamp per failure.

## 4. Registry Update Suggestions

- [ ] Update `source.msrp.batch_a` status in source_registry.yaml (degraded)
- [ ] Update `source.voc.batch_a` status in source_registry.yaml (degraded)
- [ ] Update `source.news.batch_b` status in source_registry.yaml (degraded)
- [ ] Add per-source failure tracking to Governance Gaps
- [ ] Create proposal for source quality scoring automation
