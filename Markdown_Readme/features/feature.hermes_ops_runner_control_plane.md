# Hermes Ops Runner Control Plane

## Purpose

Hermes Ops Runner is the controlled execution surface for manual Hermes audit
commands. It keeps Chat, Dashboard, and DevSync passive while allowing an admin
to trigger known governance scripts through a single audited service.

## Runtime Boundary

- Only allowlisted commands in `hermes_ops_runner_service.py` can run.
- `HERMES_RUN_ENABLED=false` disables `/hermes/run/*` and
  `/hermes/commands/execute`.
- A lock file prevents concurrent runs.
- stdout and stderr are redacted before returning to the browser.
- Every run writes `hermes/activity_log.jsonl` and a matching
  `hermes/evidence_ledger.jsonl` record.

## API Surface

- `GET /hermes/run`
- `GET /hermes/run/{command}/help`
- `POST /hermes/run/{command}`
- `POST /hermes/commands/execute`

## Verification

- `test_hermes_ops_runner_service.py`
- `test_hermes_routes.py`
- `test_hermes_chat.py`

Latest focused result: 71 passed.

## Operational Note

Production should explicitly choose the `HERMES_RUN_ENABLED` value. Use `false`
when Hermes should remain a read-only governance dashboard.
