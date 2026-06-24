# MSRP Source Repair Backlog

Generated: 2026-06-24T14:26:40Z
Run ID: msrp-dryrun-20260624-142640
Transient regressions: 7
Business resolutions: 0
Source repair issues: 1

| Failure reason | Priority | Count | Recheck | Business | Source repair | Recommended strategy | Reference assist | Affected countries |
|---|---:|---:|---:|---:|---:|---|---|---|
| anti_bot_access_denied | medium 19.0 | 1 | 0 | 0 | 1 | manual_review_or_proxy_required | EVKX reference_only_review_required | FR |
| http_timeout | recheck 25.5 | 7 | 7 | 0 | 0 | retry_or_reduce_concurrency | - | FR |

## Source Repair Queue

| Country | Source | Brand | Host | Failure reason | Strategy |
|---|---|---|---|---|---|
| FR | `tesla_model_y_fr_draft_scrapling` | TESLA | tesla.com | anti_bot_access_denied | manual_review_or_proxy_required |

## Transient Recheck Queue

| Country | Source | Failure reason | Last known good | Strategy |
|---|---|---|---|---|
| FR | `hyundai_tucson_fr_draft_scrapling` | http_timeout | - | retry_or_reduce_concurrency |
| FR | `kia_sportage_fr_draft_scrapling` | http_timeout | msrp-dryrun-20260623-131042 | retry_or_reduce_concurrency |
| FR | `nissan_juke_fr_draft_scrapling` | http_timeout | msrp-dryrun-20260623-132635 | retry_or_reduce_concurrency |
| FR | `peugeot_2008_fr_draft_scrapling` | http_timeout | msrp-dryrun-20260623-132635 | retry_or_reduce_concurrency |
| FR | `peugeot_3008_fr_draft_scrapling` | http_timeout | msrp-dryrun-20260623-131042 | retry_or_reduce_concurrency |
| FR | `peugeot_5008_fr_draft_scrapling` | http_timeout | - | retry_or_reduce_concurrency |
| FR | `toyota_yaris_cross_fr_draft_scrapling` | http_timeout | - | retry_or_reduce_concurrency |
