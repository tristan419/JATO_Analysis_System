# Hermes Code Audit Report

## Summary

- **Base:** `main~5`
- **Head:** `HEAD`
- **Files changed:** 7
- **Risk Score:** 60/100 (NEEDS_REVIEW)
- **Generated at:** 2026-05-14T15:50:45Z
- **Blockers:** 0 | **Needs Review:** 1 | **Warnings:** 1 | **Info:** 1

## Findings

| Severity | Area | Finding | Suggested Fix |
|---|---|---|---|
| 🟠 NEEDS_REVIEW | API Contract | Backend API routes or schemas changed but frontend types/index.ts was not updated | Verify frontend types match the updated backend serializers. |
| 🟡 WARNING | Registry Gap | Backend API routes changed but Feature Registry was not updated | Update hermes/feature_registry.yaml with any new or modified features. |
| 🔵 INFO | Documentation | Code changes detected but no documentation files were updated | Review if Markdown_Readme/ docs need updating for the changes. |

## Required Actions

- [ ] **[NEEDS_REVIEW]** API Contract: Verify frontend types match the updated backend serializers.

### Warnings (1)

- [ ] [Registry Gap] Update hermes/feature_registry.yaml with any new or modified features.

### Info (1)

- [Documentation] Code changes detected but no documentation files were updated
