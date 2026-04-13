# Sweden XC60 Scrape Preview

Date: 2026-04-11
Purpose: let the operator review the real XC60 scrape quality before replacing the local BMW Germany sample.

## Current Status

- No real XC60 source has been added to source registry in this step.
- No real XC60 batch has been ingested in this step.
- BMW Germany review cases have not been deleted yet.

## Important Existing Local Residue

There is already one Volvo placeholder source in local DB:

- sourceCode: `se-volvo-configurator`
- sourceRegistryUrl: `https://example.com/volvo/se/configurator`
- extractorVersion: `e2e-test-1`

This is not a real official source. It also already produced one fake local observation, one approved review case, and one current price row. It should be cleaned together with BMW replacement work, but it was left untouched in this preview step.

## Live Evidence: Volvo Sweden Model Page

URL:

- `https://www.volvocars.com/se/cars/xc60-hybrid/`

Real stealth fetch result:

- HTTP 200
- JSON-LD extraction returned 4 objects
- Only 1 object contains a real MSRP

Usable extracted row:

| official_model | official_trim | msrp_value | currency | interpretation |
| --- | --- | ---: | --- | --- |
| XC60 | Dynamisk, ansvarsfull och smartare än någonsin. Det här är vår mellanstora och uppkopplade SUV-laddhybrid för en mångsidig körupplevelse. | 569900 | SEK | Good for base MSRP only |

Conclusion:

- The model page is stable for one base price.
- The model page is not suitable for trim-level ingest because `official_trim` degenerates into marketing description text.

## Live Evidence: Volvo Sweden Build Page

URL:

- `https://www.volvocars.com/se/build/xc60-hybrid/`

Real stealth fetch result:

- HTTP 200
- HTML length about 1.66 MB
- No simple JSON-LD vehicle rows usable directly
- Visible DOM contains real title / price pairs

Observed title / price pairs from live DOM:

| observed_title | observed_price | likely meaning |
| --- | --- | --- |
| XC60 | Från 569 900 kr | model / edition family |
| XC60 Black Edition | Från 619 900 kr | edition family |
| Core Nordic Edition | 569 900 kr | equipment level candidate |
| Plus Nordic Edition | 599 900 kr | equipment level candidate |
| Ultra | 773 000 kr | equipment level candidate |
| T6 AWD Laddhybrid | 569 900 kr | powertrain candidate |

Additional signal from DOM attributes:

- `XC60` and `XC60 Black Edition` come from a title source combining `EDITION` and `MODEL`.
- `Core Nordic Edition`, `Plus Nordic Edition`, and `Ultra` expose price source `totalPriceInclTax`.
- `T6 AWD Laddhybrid` also exposes `totalPriceInclTax`, but includes a `valueDescription` hint (`Fyrhjulsdrift`), which indicates it belongs to powertrain and should not be treated as trim directly.

Conclusion:

- The build page is good enough to support trim-level extraction.
- But it should not be ingested with a naive selector, because the page mixes:
  - model / edition choices
  - trim / sales level choices
  - powertrain choices

## Recommended Candidate Rows If We Proceed

The safest first real XC60 ingest would be to emit trim-level rows from the build page for the sales-level options only, under the currently selected XC60 plug-in hybrid configuration.

Candidate preview rows:

| official_model | official_trim | source_msrp_value | currency | source_url |
| --- | --- | ---: | --- | --- |
| XC60 | Core Nordic Edition | 569900 | SEK | https://www.volvocars.com/se/build/xc60-hybrid/ |
| XC60 | Plus Nordic Edition | 599900 | SEK | https://www.volvocars.com/se/build/xc60-hybrid/ |
| XC60 | Ultra | 773000 | SEK | https://www.volvocars.com/se/build/xc60-hybrid/ |

Operational note:

- These rows are preview rows only at this stage.
- They have not been written into local DB.
- If approved, the next implementation should add a dedicated Volvo build-page selector that filters out model-family and powertrain buttons before registry ingest.

## Replace Decision Gate

Before replacing BMW Germany local sample data, review these questions:

1. Is `Core Nordic Edition / Plus Nordic Edition / Ultra` the right truth source for the trim dimension you want?
2. Do you want the first Sweden pilot to ingest only these 3 sales-level rows?
3. Should the existing fake local Volvo sample (`example.com`) be deleted together with BMW cleanup?

## Proposed Next Step After Approval

If approved, the next step should be:

1. Add a real Volvo Sweden XC60 source config using the build page.
2. Register it in source registry.
3. Ingest one manual XC60 batch.
4. Show the resulting review cases / observations.
5. Delete BMW Germany local sample rows and the fake Volvo placeholder rows.