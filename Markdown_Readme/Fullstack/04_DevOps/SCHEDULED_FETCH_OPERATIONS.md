# Scheduled Fetch Operations Runbook

This runbook covers recovery for scheduled fetch jobs that feed Country Copilot,
VOC, and MSRP workflows. It is written for Tencent Cloud production first, with
local launchd notes where the same scripts are used on macOS.

## Scope

| Pipeline | Production unit | Runner |
| --- | --- | --- |
| Country news sync | `jato-country-news-sync.timer` / `.service` | `03_Scripts/news/run_country_news_sync.sh` |
| VOC forum sync | `jato-voc-forum-sync.timer` / `.service` | `03_Scripts/voc/run_voc_forum_sync.sh` |
| MSRP dry run | `jato-msrp-dryrun.timer` / `jato-msrp-sync@dryrun.service` | `03_Scripts/run_msrp_low_concurrency.sh` |
| MSRP ingest | `jato-msrp-ingest.timer` / `jato-msrp-sync@ingest.service` | `03_Scripts/run_msrp_low_concurrency.sh` |

## Quick Triage

Run these first on Tencent Cloud:

```bash
cd /opt/JATO_Analysis_System-main
sudo systemctl list-timers 'jato-*' --all --no-pager
sudo systemctl status jato-country-news-sync.timer jato-voc-forum-sync.timer jato-msrp-dryrun.timer jato-msrp-ingest.timer --no-pager
sudo systemctl status jato-fullstack-backend@8000 --no-pager
curl --noproxy '*' -fsS http://127.0.0.1:8000/healthz
```

If the backend health check fails, fix the backend first. Scheduled fetchers may
depend on `/v1` APIs or PostgreSQL credentials loaded from the backend env file.

## Logs And Artifacts

| Pipeline | Latest log | Failure/status artifact |
| --- | --- | --- |
| Country news | `03_Scripts/logs/country-news-sync-latest.log` | `03_Scripts/logs/country-news-sync-last-failure.txt`, `03_Scripts/logs/scheduled_fetch_status.json` |
| VOC forum | `03_Scripts/logs/voc-forum-sync-latest.log` | `03_Scripts/logs/voc-raw-latest.json`, `03_Scripts/logs/voc-enriched-latest.json`, `03_Scripts/logs/scheduled_fetch_status.json` |
| MSRP dry run | `03_Scripts/logs/msrp-dryrun-*.log` | per-country logs `03_Scripts/logs/msrp-dryrun-<country>-*.log` |
| MSRP ingest | `03_Scripts/logs/msrp-ingest-*.log` | per-country logs `03_Scripts/logs/msrp-ingest-<country>-*.log` |

Systemd journal commands:

```bash
sudo journalctl -u jato-country-news-sync.service -n 160 --no-pager
sudo journalctl -u jato-voc-forum-sync.service -n 160 --no-pager
sudo journalctl -u jato-msrp-sync@dryrun.service -n 160 --no-pager
sudo journalctl -u jato-msrp-sync@ingest.service -n 160 --no-pager
```

## Manual Recovery Commands

Run one job manually before restarting timers.

```bash
cd /opt/JATO_Analysis_System-main

# Country news
sudo -E bash 03_Scripts/news/run_country_news_sync.sh

# VOC raw + enrichment
sudo -E bash 03_Scripts/voc/run_voc_forum_sync.sh

# MSRP dry run, safe validation mode
sudo -E JATO_MSRP_MODE=dryrun JATO_MSRP_COUNTRIES=batch_a bash 03_Scripts/run_msrp_low_concurrency.sh

# MSRP ingest, only after dry run succeeds
sudo -E JATO_MSRP_MODE=ingest JATO_MSRP_COUNTRIES=batch_a bash 03_Scripts/run_msrp_low_concurrency.sh
```

For a single MSRP country:

```bash
sudo -E JATO_MSRP_MODE=dryrun JATO_MSRP_COUNTRIES=se bash 03_Scripts/run_msrp_low_concurrency.sh
```

## Common Failures

| Symptom | Likely cause | Recovery |
| --- | --- | --- |
| `Another ... job already holds lock` | Prior run still active or stale lock | Check `ps aux`, then remove only stale `/tmp/jato-*.lock` files. |
| Database connection failure | `/etc/jato-fullstack/backend.env` missing or stale `APP_DATABASE_URL` | Verify env file, then restart backend and rerun the job. |
| News or VOC HTTP timeout | Source site slow or blocking | Rerun with lower workers or smaller country/source scope; keep the failure in source quality notes. |
| MSRP 404 / selector miss | Source page changed | Keep dry run failed, do not ingest; update source draft selector or mark source degraded. |
| Backend API 500 during MSRP | Backend service or data contract failure | Check backend journal, fix API first, then rerun dry run. |

## Timer Recovery

After a manual run succeeds:

```bash
sudo systemctl daemon-reload
sudo systemctl restart jato-country-news-sync.timer
sudo systemctl restart jato-voc-forum-sync.timer
sudo systemctl restart jato-msrp-dryrun.timer
sudo systemctl restart jato-msrp-ingest.timer
sudo systemctl list-timers 'jato-*' --all --no-pager
```

To run a timer-backed service immediately:

```bash
sudo systemctl start jato-country-news-sync.service
sudo systemctl start jato-voc-forum-sync.service
sudo systemctl start jato-msrp-sync@dryrun.service
```

Do not manually start `jato-msrp-sync@ingest.service` until the dry run succeeds
for the same country scope.

## Local macOS Launchd Notes

Local launchd installers:

```bash
bash 03_Scripts/news/install_local_country_news_sync_launchd.sh 6 15
bash 03_Scripts/ops/install_local_msrp_sync_launchd.sh 3 20
```

Launchd logs are under `~/Library/Logs/JATO_Analysis_System/`. If the repo is
under `~/Downloads`, the installers may use Terminal bridge mode so macOS
permissions do not block the scheduled process.

## When To Escalate

Escalate instead of retrying when:

- MSRP ingest would write production observations after a failed dry run.
- A source has repeated selector failures across two consecutive dry runs.
- The same pipeline fails after backend health and env files are confirmed.
- A run creates partial database writes that need reconciliation.

Record the outcome in Hermes evidence or a dev event with the exact command,
job time, affected countries, log path, and recovery action.
