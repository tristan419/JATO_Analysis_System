# Server Artifact Sync Policy

> Hermes Phase 6.5
> Created: 2026-05-14

## 1. Principle

| Direction | What | How |
|---|---|---|
| GitHub → Server | Code, registries, scripts | `deploy-fullstack-tencent` workflow |
| Server → Local | Runtime reports, lightweight snapshots | `hermes_sync_server_snapshot.sh` |
| Never | Secrets, env files, DB dumps, large raw data | Explicitly excluded |

## 2. What to Sync (Always)

- `hermes/reports/*.json` — all Hermes JSON reports
- `hermes/reports/*.md` — all Hermes markdown reports
- `hermes/evidence_ledger.jsonl` — evidence records
- `hermes/answer_audit.jsonl` — answer audit records
- `hermes/*.yaml` — registry files (server may have updates)
- `03_Scripts/logs/scheduled_fetch_status.json` — runtime fetch status

## 3. What to Sync (Optional, --include-optional)

- `04_Processed_data/**/deck/*.json` — VOC deck artifacts
- `04_Processed_data/**/enriched/*.json` — VOC enriched signals
- `04_Processed_data/**/summary*.json` — lightweight summary files

## 4. What NEVER to Sync

```
/etc/jato-fullstack/*.env
*.env / backend.env / country-news.env / voc.env / msrp.env
*.key / *.pem
node_modules/
.venv/
.hermes_server_snapshot/ (except via this script)
PostgreSQL dumps
01_RAW_DATA/
04_Processed_data/*.parquet (large data)
.git/ (server git is stale — SKIP_GIT_SYNC)
```

## 5. Local Usage

```bash
# Pull server runtime snapshot
bash 03_Scripts/hermes/hermes_sync_server_snapshot.sh --host <IP> --user ubuntu

# Include optional VOC deck artifacts
bash 03_Scripts/hermes/hermes_sync_server_snapshot.sh --host <IP> --include-optional

# Read summary
cat .hermes_server_snapshot/SNAPSHOT_SUMMARY.md
```

## 6. Safety Rules

1. Do NOT use this script in GitHub Actions — it's a developer debugging tool.
2. Do NOT commit `.hermes_server_snapshot/` to git.
3. The snapshot directory is gitignored.
4. Local source code is never overwritten.
5. Server secrets are never pulled.
6. Large raw data is never pulled by default.

## 7. Future Integration

Phase 6+ can add a Data Management UI button to trigger server snapshot refresh, but must respect the same safety boundaries documented here.
