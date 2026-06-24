# Sweden 2026 Official MSRP Backfill Audit

Generated: 2026-06-24

## Current Coverage

The live Sweden MSRP table currently has 18 rows. Three verified official ordinary-price, promotion-price, or campaign-savings backfills have been inserted and are visible in the existing MSRP monitor page.

| Status | Brand | Model | Trim | Evidence | Backfilled movement |
| --- | --- | --- | --- | --- | --- |
| inserted | SKODA | ENYAQ | Solid Edition | Skoda Sweden official campaign page plus official PDF price list | 619,800 SEK -> 599,500 SEK (-3.28%) |
| inserted | VOLVO | EX90 | Ultra Pro Edition | Volvo Sweden official promotions page evidence excerpt | 1,148,800 SEK -> 1,099,900 SEK (-4.26%) |
| inserted | VOLKSWAGEN | TAYRON | R-Line SWE Edition | Volkswagen Sweden official offers page plus official configurator page | 711,500 SEK -> 611,400 SEK (-14.07%) |

## Inserted Evidence

### Skoda ENYAQ Solid Edition

- Official campaign page: https://www.skoda.se/erbjudande/kampanj/erbjudande-enyaq
- Official PDF: https://www.skoda.se/_doc/1d23f075-2685-40f9-ad6f-a31b09f3d660
- Local HTML snapshot: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/skoda_enyaq_solid_edition_offer_2026-06-23.html`
- Local PDF snapshot: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/skoda_enyaq_solid_edition_prislista_2026-06-17.pdf`
- Backfill observation id: `63440677-44a2-4296-abee-fb887b2f87cf`
- Backfill price history id: `78df9a01-ea2d-425e-beb3-dda6f59413eb`
- Evidence classification: `official_campaign_vs_regular_price`

This is a campaign price drop against Skoda's official ordinary price, not a verified permanent MSRP cut.

### Volvo EX90 Ultra Pro Edition

- Official promotions page: https://www.volvocars.com/se/promotions/
- Local evidence excerpt: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volvo_ex90_ultra_pro_edition_offer_2026-06-23.md`
- Local evidence excerpt SHA256: `bebcf5523ef6cf90387056d164fed9ee46e60559b4414791a3b24a833ac2d4b2`
- Backfill observation id: `9e2da606-bb96-4a2f-9ae3-77f3f49e5963`
- Backfill price history id: `ce30eceb-fed9-446e-a130-9141cadd2011`
- Evidence classification: `official_promotion_vs_ordinary_price`

The Volvo page is official and aligns with the current scraped `VOLVO / EX90 / Ultra Pro Edition / BEV` row. Direct curl and Playwright requests from this environment return Akamai Access Denied, so the local artifact is an extracted evidence note rather than the full HTML source snapshot. This is a promotion price against Volvo's official ordinary recommended price, not a verified permanent MSRP cut.

### Volkswagen Tayron R-Line SWE Edition

- Official offers page: https://www.volkswagen.se/sv/kop-en-vw/erbjudanden.html
- Official configurator page: https://www.volkswagen.se/sv/bygg-din-bil.html/__app/31150.app
- Local offers HTML snapshot: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_offers_2026-06-24.html`
- Local offers text extract: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_offers_2026-06-24.txt`
- Local configurator HTML snapshot: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_tayron_configurator_2026-06-24.html`
- Local configurator text extract: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_tayron_configurator_2026-06-24.txt`
- Backfill observation id: `de702780-2b8a-460a-b809-c5c3fb329d68`
- Backfill price history id: `06507ebe-fa9f-457d-a316-fcac4dee1aea`
- Evidence classification: `official_campaign_savings_vs_current_price`

The official offers page states that Tayron R-Line SWE Edition can save up to 100,100 SEK. The official configurator lists the same trim at 611,400 SEK including VAT. The inserted previous baseline is therefore inferred as 611,400 SEK + 100,100 SEK = 711,500 SEK. This is a campaign savings boundary for targeted spot-check, not a verified permanent MSRP cut.

## Still Open

| Brand | Current rows | Official historical evidence status |
| --- | ---: | --- |
| VOLKSWAGEN | 6 remaining Tayron rows | One Tayron R-Line SWE Edition campaign-savings boundary was inserted. Current official page and official `model-structure.mofa*.json` files were saved. Other Tayron rows still lack same-trim ordinary/historical price fields. No additional Tayron backfill inserted. |
| VOLVO | 9 remaining EX90/XC90 rows | One EX90 Ultra Pro Edition backfill was inserted. Other Volvo current rows still lack same-trim official ordinary/historical price evidence. Volvo build URLs and promotions page are blocked by Akamai from direct local curl/Playwright. No additional Volvo backfill inserted. |

## Network/Search Notes

- Wayback CDX and `archive.org/wayback/available` requests timed out from this environment.
- DuckDuckGo HTML searches for Volvo/VW official price PDFs timed out from this environment.
- Volkswagen current official page snapshot was saved as reference only: `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_tayron_current_2026-06-23.html`.
- Volkswagen official current JSON snapshots were saved as reference only:
  - `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_model_structure_mofa_2026-06-23.json`
  - `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volkswagen_model_structure_mofa_cardata_2026-06-23.json`
- Volvo direct access-denied captures were saved as troubleshooting artifacts only:
  - `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volvo_promotions_2026-06-23.html`
  - `03_Scripts/diagnostics/artifacts/msrp_backfill/sweden_2026/volvo_promotions_rendered_2026-06-23.html`

## Verification Commands

```bash
curl -sS 'http://127.0.0.1:8012/v1/msrp/monitoring/events?country=%E7%91%9E%E5%85%B8&brand=SKODA&jato_model=ENYAQ&window_days=365&threshold_pct=0&limit=50&mode=live'
curl -sS 'http://127.0.0.1:8012/v1/msrp/monitoring/events?country=%E7%91%9E%E5%85%B8&brand=VOLVO&jato_model=EX90&window_days=365&threshold_pct=0&limit=50&mode=live'
```

Expected evidence highlights:

- `eventId`: `SKODA|ENYAQ|BEV`, `medianChangePct`: `-3.28`
- `eventId`: `VOLVO|EX90|BEV`, `medianChangePct`: `-4.26`
- `eventId`: `VOLKSWAGEN|TAYRON|UNKNOWN`, `medianChangePct`: `-14.07`
- `backfilled`: `true`
- `backfillKind`: `official_campaign_vs_regular_price`, `official_promotion_vs_ordinary_price`, or `official_campaign_savings_vs_current_price`
- `backfillEvidenceRole`: `previous`

## 2026-06-24 Recheck

The local evidence artifacts and live API were rechecked from the MSRP worktree.

| Item | Result |
| --- | --- |
| Skoda campaign HTML snapshot | SHA256 `ed8913dbfc12456da4fce655053ff6f15b1004e8d65e0b77957699ed7feeff70`; extracted text contains `Enyaq 85 Solid Edition`, `Kampanjpris från 599 500 kr`, and `Ord.pris 619 800kr`. |
| Skoda official PDF snapshot | SHA256 `c52ac543014eed3f2c235ef870b25279fe073e19e8e154eeef2a9256c2e88dfb`; extracted text contains `Škoda Enyaq Solid Edition`, `Prislista 17 juni, 2026`, `599 500 kr`, and validity text through `2026-09-30`. |
| Volvo extracted evidence note | SHA256 `bebcf5523ef6cf90387056d164fed9ee46e60559b4414791a3b24a833ac2d4b2`; contains EX90 Ultra Pro Edition current recommended price `1,099,900 SEK` and ordinary price `1,148,800 SEK`. |
| Volkswagen offers HTML snapshot | SHA256 `d8a00af25cd34f328b98f68b1a4a4427b4fb346fba69ec01ff9683c01a293af1`; extracted text contains `Tayron R-Line SWE Edition` and `spara upp till 100 100` kronor. |
| Volkswagen configurator HTML snapshot | SHA256 `a6ee76a1606f1062660f2d54ed26962e52ca3b3c14ec623e7c543063b8d0f157`; extracted text contains `R-Line SWE Edition`, `Pris inkl. moms`, and `611 400 kr`. |
| Live monitoring API | `country=瑞典&window_days=365&threshold_pct=0` returns three backfilled price drops: `VOLKSWAGEN|TAYRON|UNKNOWN` at `-14.07%`, `VOLVO|EX90|BEV` at `-4.26%`, and `SKODA|ENYAQ|BEV` at `-3.28%`. |
| Wayback/CDX retry | Direct CDX calls for Skoda model page, Skoda PDF URL, and Volvo EX90 build page timed out after 60 seconds each from this environment. |

Interpretation: the current backfill is real official Sweden 2026 evidence, but it is still promotion, campaign, or campaign-savings evidence against an official ordinary price or inferred official savings baseline. It should be monitored as an official price-drop signal for targeted spot-check, not labeled as a verified permanent MSRP cut unless a dated historical configurator snapshot or price list confirms that lifecycle.
