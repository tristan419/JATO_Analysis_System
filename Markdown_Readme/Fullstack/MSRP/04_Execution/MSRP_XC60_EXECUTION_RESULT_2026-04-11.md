# Sweden XC60 Execution Result

Date: 2026-04-11
Purpose: record the executed replacement of the BMW Germany local sample with the real Volvo Sweden XC60 pilot source and the follow-up UI validation.

## Final State

- Real XC60 source has been added to source registry and is enabled.
- BMW Germany sample rows have been cleaned from local review data.
- BMW source remains in registry but is disabled.
- Fake placeholder Volvo source has been removed.
- XC60 trim group UI is now collapsible and supports group-level approve / reject actions.

## Source Registry Status

Verified against local API:

- `volvo_se_xc60_build_scrapling`
  - enabled: `true`
  - country: `Sweden`
  - brand: `Volvo`
  - sourceUrl: `https://www.volvocars.com/se/build/xc60-hybrid/`
  - extractorVersion: `0.4.0-scrapling`
- `bmw_de_scrapling`
  - enabled: `false`
  - retained only as a disabled source row

## XC60 Review Case Status

Verified against local API:

- total Volvo Sweden review cases: `3`
- all three are still `open`
- all three come from source `volvo_se_xc60_build_scrapling`
- all three use fixed `jatoModel = XC60`
- all three now carry structured `matchReason.confidenceRule`

Observed rows:

| officialModel | officialTrim | sourceMsrpValue | sourceCurrency | msrpValue(EUR) | matchConfidence | reviewStatus |
| --- | --- | ---: | --- | ---: | ---: | --- |
| XC60 | Core Nordic Edition | 569900 | SEK | 49556.52 | 0.88 | open |
| XC60 | Plus Nordic Edition | 599900 | SEK | 52165.22 | 0.88 | open |
| XC60 | Ultra | 773000 | SEK | 67217.39 | 0.88 | open |

FX fields currently stored on these rows:

- `fxRateToEur = 0.08695652`
- `fxRateAsOfDate = 2026-04-11`
- `fxSource = static-fallback`

## Confidence Rule

Current XC60 confidence is not derived from a ranking or fuzzy matcher.

It is now computed through a declarative weighted rule profile in the source YAML for this pilot:

- source file: `07_ScrapingToolkit/sources/volvo_se_xc60.yaml`
- configured rule mode: `confidence_rules -> weighted_profile_v1`
- current total for the XC60 trim cards: `0.88`

Meaning:

- The source is using a controlled declarative extraction rule.
- We fixed `officialModel = XC60` and `jatoModel = XC60`.
- We copy the selected trim title into `jatoTrim`.
- The displayed `0.88` is the sum of rule components, not a magic constant.
- Current component breakdown is: `0.22 base + 0.18 fixed_model + 0.12 fixed_jato_model + 0.12 trim_present + 0.10 copy_trim_to_jato_trim + 0.04 exclude_price_prefixes + 0.04 exclude_if_selector + 0.03 parsed_price_text + 0.01 currency + 0.02 price_label = 0.88`.
- The rows are still emitted as `review_required`, so this confidence does not auto-approve anything.

## UI Validation

Validated after the latest frontend patch:

- Review Cases page groups by `country / brand / model`
- same-model trims can be expanded and collapsed together
- the `MODEL GROUPS` summary chip now uses the shorter hero-stat style height
- group headers expose:
  - one-click approve
  - one-click reject
  - pending count
  - confidence range
- Review detail now shows `Match Reason` directly, including rule mode, base score, component deltas, evidence, and raw JSON fallback
- Current Prices page uses the same compact model-group summary chip style

Frontend verification completed:

- `npm run check:types` passed
- `npm run build` passed

## Related Files

- `07_ScrapingToolkit/sources/volvo_se_xc60.yaml`
- `07_ScrapingToolkit/jato_scraper/extractors/scrapling_web.py`
- `03_Scripts/cleanup_msrp_source_data.py`
- `06_AppPlatform/frontend/src/pages/ReviewCasesPage.tsx`
- `06_AppPlatform/frontend/src/pages/MsrpPage.tsx`
- `06_AppPlatform/frontend/src/index.css`