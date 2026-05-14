# Hermes Code Audit Report

## Summary

- **Base:** `main~5`
- **Head:** `HEAD`
- **Files changed:** 18
- **Risk Score:** 60/100 (NEEDS_REVIEW)
- **Generated at:** 2026-05-14T13:31:41Z
- **Blockers:** 0 | **Needs Review:** 1 | **Warnings:** 0 | **Info:** 0

## Findings

| Severity | Area | Finding | Suggested Fix |
|---|---|---|---|
| 🟠 NEEDS_REVIEW | Scheduling | New schedule added. Verify it does not duplicate existing schedules in Pipeline Registry. | Check hermes/pipeline_registry.yaml for duplicate scheduling. See gap.pipeline.duplicate_news_scheduling for an existing |

## Required Actions

- [ ] **[NEEDS_REVIEW]** Scheduling: Check hermes/pipeline_registry.yaml for duplicate scheduling. See gap.pipeline.duplicate_news_scheduling for an existing example.
