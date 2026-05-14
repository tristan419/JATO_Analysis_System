# Pipeline Scheduler Decision for Hermes Governance

> Created: 2026-05-14
> Based on: Hermes Phase 4 Pipeline Audit (`hermes_pipeline_audit.py`)
> Status: Decision recorded. Schedules not yet modified.

---

## 1. Context

Hermes Phase 4 identified overlapping or duplicate scheduling risks across three orchestration systems:

| System | Role | Location |
|---|---|---|
| systemd timers | Production cron on Tencent Cloud server | `/etc/systemd/system/jato-*.timer` |
| Airflow DAGs | Orchestration with web UI, manual trigger | `airflow/dags/` (Docker Compose) |
| GitHub Actions | CI/CD, diagnostics, workflow_dispatch | `.github/workflows/` |

Three pipelines have scheduler overlap:

| Pipeline | systemd | Airflow | GitHub Actions | Risk |
|---|---|---|---|---|
| Country News Sync | yes (23:15 UTC) | yes (06:15 UTC) | yes (workflow_dispatch only) | **HIGH — double ingestion** |
| MSRP | yes (03:30 / Sat 05:30) | yes (manual only) | none | LOW |
| VOC Forum Sync | yes (01:45 UTC) | none | none | — |

---

## 2. Scheduler Roles

### 2.1 systemd timer — Production Scheduler

- **Role:** Primary production scheduler for all data pipelines
- **Why:** Runs on the same server as PostgreSQL, has direct DB access via `/etc/jato-fullstack/backend.env`, no network hop
- **Scope:** News, VOC, MSRP dry-run, MSRP ingest

### 2.2 Airflow DAG — Manual Fallback / Experimental

- **Role:** Manual trigger alternative, orchestration candidate, experimental scheduler
- **Why:** Provides web UI, DAG visualization, dependency management. Docker Compose stack may not always be running
- **Scope:** Manual re-run of pipelines, MSRP low-concurrency manual trigger, scraping toolkit on-demand

### 2.3 GitHub Actions — CI / Diagnostics / Manual Dispatch

- **Role:** Code quality gate, performance monitoring, manual dispatch for dry-run diagnostics
- **Why:** GitHub runner does not have direct access to production DB. Should not be production DB writer
- **Scope:** CI checks, nightly performance, deploy, manual workflow_dispatch for diagnostics

---

## 3. Country News Sync Decision

**Production scheduler:** `systemd timer` (`jato-country-news-sync.timer`, 23:15 UTC daily)

**Decision rationale:**
- Systemd timer runs on server with direct PostgreSQL access
- Systemd timer already has success history (last success: 2026-05-13 23:18 CST)
- Airflow DAG calls backend API (circular: Airflow → Backend → DB), adding a network hop and failure point
- GitHub Actions schedule was already removed 2026-05-14 (converted to workflow_dispatch only)

**Airflow DAG role:** Manual fallback / experimental. Should be set to `schedule=None` (manual-only) if not already.

**GitHub Actions role:** workflow_dispatch only (confirmed 2026-05-14). Used for dry-run diagnostics when manually triggered.

**Risk if unresolved:**
- Double ingestion into `ops.country_news_digest` table
- Inconsistent `scheduled_fetch_status.json` state
- Wasted API calls to RSS sources
- Confusion during debugging ("which run produced this data?")

**Required follow-up:**
- [ ] Verify Airflow DAG `jato_country_news_sync` schedule status on production Airflow instance
- [ ] If Airflow DAG schedule is active, pause it or change to `None`
- [ ] Document the Airflow DAG as manual fallback in Airflow UI description
- [ ] Add scheduler role note to `hermes/pipeline_registry.yaml`

---

## 4. MSRP Decision

**Production scheduler:** `systemd timer` (dry-run daily 03:30 UTC, ingest weekly Sat 05:30 UTC)

**Airflow DAG role:** Manual fallback. Airflow DAG (`jato_msrp_low_concurrency`) is already `schedule=None`.

**Risk:** LOW — no schedule conflict. Airflow DAG provides useful manual trigger for ad-hoc MSRP scraping.

**Required follow-up:**
- [ ] Ensure Airflow MSRP DAG outputs do not overwrite production observations unexpectedly
- [ ] Document the dual-trigger design (systemd production + Airflow manual) in pipeline registry

---

## 5. VOC Decision

**Production scheduler:** `systemd timer` (`jato-voc-forum-sync.timer`, 01:45 UTC daily)

**No overlap:** VOC has no Airflow DAG or GitHub Actions schedule.

**Required follow-up:**
- [ ] Add per-source failure tracking (sourceId, url, error type, retryable flag)
- [ ] Expand `scheduled_fetch_status.json` with structured failed source details
- [ ] Phase 5 Intelligence Governor should add source quality scoring

---

## 6. GitHub Actions Decision

**Role:** CI, diagnostics, manual workflow_dispatch, deploy. **Not a production DB writer.**

**Current state (2026-05-14):**
- `ci.yml` — push/PR trigger ✅
- `deploy-fullstack-tencent.yml` — push to main trigger ✅
- `country-news-sync.yml` — workflow_dispatch only (schedule removed) ✅
- `nightly-performance.yml` — daily 01:30 UTC schedule ✅ (read-only, no DB writes)
- `deploy-ec2-auto-update.yml` — push to main + workflow_dispatch (legacy Streamlit)
- `deploy-aws-ecs.yml` — workflow_dispatch only

**Decision:** All GitHub Actions schedules are acceptable. No action needed beyond confirming `country-news-sync.yml` has no active cron schedule.

---

## 7. Future: Scheduled Fetch Status JSON Coverage

Current `scheduled_fetch_status.json` only covers `voc`. Should be expanded to:

```json
{
  "news": { "lastRunAt": "...", "status": "success", "successCount": 21, "failedCount": 3 },
  "voc": { "lastRunAt": "...", "status": "success", "successCount": 40, "failedCount": 8 },
  "msrp_dryrun": { "lastRunAt": "...", "status": "success", "successCount": 92, "failedCount": 117 },
  "msrp_ingest": { "lastRunAt": "...", "status": "success", "successCount": 0, "failedCount": 0 },
  "jato_etl": { "lastRunAt": null, "status": "manual_only", "successCount": 0, "failedCount": 0 }
}
```

This is a Phase 5+ task.

---

## 8. Registry Updates Summary

| Registry | Change |
|---|---|
| `pipeline_registry.yaml` | Add `role` and `schedulerDecision` fields to News (systemd, Airflow, GH), MSRP, VOC entries |
| `governance_gaps.yaml` | Update `gap.pipeline.duplicate_news_scheduling` status to `in_progress` with decision reference |
| `proposal_registry.yaml` | Update `proposal.pipeline.news_dedup` status to `pending_review` with decision summary |

---

## 9. Acceptance Criteria

- [x] Country News duplicate scheduling is documented with decision rationale
- [x] Production scheduler is identified for News (systemd), MSRP (systemd), VOC (systemd)
- [x] Airflow fallback role is documented
- [x] GitHub Actions role is documented
- [ ] Airflow DAG schedule confirmed as manual-only or paused (requires server access)
- [ ] `scheduled_fetch_status.json` expanded to cover all pipelines (Phase 5+)
- [ ] Per-source VOC failure tracking implemented (Phase 5+)
