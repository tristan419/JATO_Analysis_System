# AstrBot Real Data Coverage Audit

Date: 2026-07-14

## Purpose

This audit separates three different states that must not be conflated:

1. a tool is registered;
2. a tool can execute;
3. a tool returns citation-ready data for the requested business scope.

AstrBot may claim a numeric conclusion only in state 3.

## Current Coverage

| Data area | Tool path | Live result | Current verdict |
|---|---|---|---|
| Hungary market structure | `query_country_snapshot`, `query_segment_breakdown`, `build_market_chart` | Current cross-tabs returned usable HEV/SUV structure evidence | Usable for current structure, not automatically for trend |
| Monthly market trend | `query_time_series` | Hungary HEV monthly query returned 0 points | Missing; do not infer growth from one-month cross-tabs |
| Sweden current MSRP | `query_msrp_pricing` | 4 rows, currently covering ENYAQ and TAYRON | Partially usable; requested-model coverage remains narrow |
| Leasing / monthly / RV / TCO | `query_leasing_offers` | `leasing.lease_offers` contains 0 rows | Tool ready, data unavailable |
| Engineering configuration | `compare_vehicle_variants` | ENYAQ/TAYRON comparison returned 0 subjects | Mapping or source ingestion unavailable |

## Implemented Data Path

`query_leasing_offers` now follows the existing governed stack:

```text
Intent Router
-> Evidence Plan
-> FastMCP / JATO MCP tool
-> SQLAlchemy session factory
-> lease_comparison_service.list_offers
-> EvidencePackage
-> pricing / TCO artifacts
-> SSE answer
```

The MCP layer reuses existing lease offer objects and does not duplicate lease calculations or database models. When the store is empty or unavailable, it returns `coverageDiagnostics` and no fabricated values.

## Blind Validation

The fixture-backed blind pair `Nimbus E BEV / Solaris One BEV` verifies generic behavior:

- multi-word model entities are preserved;
- country prefixes are not absorbed into model names;
- leasing questions require `query_leasing_offers`;
- monthly payment and RV become PostgreSQL EvidenceRefs with units;
- SSE emits `answer_start` and `token` events;
- the pricing artifact renders monthly payment and RV from those refs.

The live Hungary market check verifies the real runtime path:

- resolved country: Hungary;
- intent: `market_overview`;
- tools: `query_country_snapshot`, `build_market_chart`, `query_segment_breakdown`, `query_time_series`;
- evidence refs: 34;
- artifacts: market structure chart, snapshot fallback chart, market table, report block, metric cards;
- result: `partially_answered`, because monthly series is absent.
- SSE lifecycle: `answer_start`, incremental `token`, and `done` events were all observed.
- generic-subject guard: no brand/model was requested, so the answer used `目标产品组合`; it did not leak a default OMODA/JAECOO subject.

## P0 Data Work Remaining

1. Materialize monthly time-series data with explicit country, powertrain, segment, month, and metric scope.
2. Import current lease offers with source URL, effective/expiry dates, term, annual mileage, monthly payment, RV, inclusions, and total contract cost.
3. Repair engineering model/variant mapping so requested models return subjects and feature deltas.
4. Expand current official MSRP beyond the current narrow Sweden sample and retain trim, currency, source URL, and retrieval date.
5. Repeat live blind checks after each ingestion change; do not compensate for missing data with Composer templates.

## Verification

- AstrBot/JATO Agent regression: `805 passed`.
- Existing warning: shared virtual environment `requests` dependency-version warning.
- Redis was unavailable during the live check; caching was disabled, but the read-only JATO tool path still completed.
