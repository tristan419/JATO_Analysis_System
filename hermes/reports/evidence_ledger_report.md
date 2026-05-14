# Hermes Evidence Ledger Report

**Generated:** 2026-05-14T15:50:45Z

## 1. Summary

| Metric | Value |
|---|---|
| Evidence records written | 2 |
| jato_fact | 1 |
| msrp_fact | 1 |
| Skipped artifacts | 4 |

## 2. Skipped Artifacts

| Artifact | Reason |
|---|---|
| `artifact.voc.raw` | VOC raw artifact directory not found locally (server-only) |
| `artifact.news.digest` | News digest stored in PostgreSQL — needs DB connection for extraction |
| `artifact.msrp.observations` | MSRP observations in PostgreSQL — needs DB connection for extraction |
| `artifact.msrp.current_prices` | Current prices in PostgreSQL — needs DB connection for extraction |

## 3. Evidence Samples

| Evidence ID | Type | Claim | Source |
|---|---|---|---|
| `evidence.20260514155045.2235f8` | msrp_fact | Scraped price observations with match status, MSRP, FX rates, confidence scoring | artifact registry metadata |
| `evidence.20260514155045.2eabfe` | jato_fact | JATO full archive parquet available at 04_Processed_data/jato_full_archive.parqu | 04_Processed_data/jato_full_archive.parq |

## 4. Recommendations

- [ ] 3 artifact(s) in PostgreSQL — add DB connection support in Phase 6
- [ ] 1 artifact(s) server-only — run evidence extraction on server
- [ ] Define artifact extraction schema for each artifact type
