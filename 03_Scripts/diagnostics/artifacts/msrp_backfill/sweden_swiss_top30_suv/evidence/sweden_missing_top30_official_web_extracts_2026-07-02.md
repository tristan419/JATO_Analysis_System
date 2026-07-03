# Sweden missing top30 official web extracts

- Captured: 2026-07-02
- Capture note: these official pages either returned 403/503 during deterministic local HTML fetch or are rendered dynamically. This file preserves the official page text exposed through searchable official-page extracts for review-only demo evidence.

## Tesla Sweden Model Y

- Source: https://www.tesla.com/sv_SE/modely/design
- Model Y Bakhjulsdrift 490 810 kr
- Pearl White included
- Evidence lane: official current baseline

## Audi Sweden Q4 e-tron

- Source: https://www.audi.se/sv/models/q4-e-tron/q4-e-tron/
- Q4 e-tron från 570 000 kr inkl. moms
- Evidence lane: official current baseline

## Volvo Sweden XC40

- Source: https://www.volvocars.com/se/cars/xc40/
- XC40 Rek. pris från 430 000 kr
- Mildhybrid / Motor
- SUV / Kategori
- Modellår 2027
- Evidence lane: official current baseline

## Volvo Sweden XC90

- Source: https://www.volvocars.com/se/cars/xc90-hybrid/
- XC90 Rek. pris från 994 000 kr
- Leasing från 12 995 kr/mån
- Laddhybrid / Motor
- SUV / Kategori
- Modellår 2027
- Evidence lane: official current baseline

## Mercedes-Benz Sweden EQA

- Captured: 2026-07-03
- Source: https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html
- Page title: EQA | Priser & specifikationer | Mercedes-Benz
- Browser-rendered navigation data tied to the EQA model URL:
  - `"aria":"EQA","label":"EQA"`
  - `"url":"https://www.mercedes-benz.se/passengercars/models/suv/eqa/overview.html"`
  - `"price":"Från 529 000 kr"`
  - `"text":"Pris inkl. moms"`
- Official model-page terms exposed by browser render:
  - EQA 250+ Special Edition
  - EQA 300 4MATIC Special Edition
  - Laddningsvoucher ingår
- Official store product pages inspected:
  - https://www.mercedes-benz.se/passengercars/buy/new-car/product.html/MBT53VJ20
  - https://www.mercedes-benz.se/passengercars/buy/new-car/product.html/MBXAGMERE
  - https://www.mercedes-benz.se/passengercars/buy/new-car/product.html/MB2VRDPGT
- API note: the official Mercedes-Benz PLS price endpoint returned `INVALID_CONSUMER_ID` without a valid `consumer-id` header. The browser-rendered product pages confirmed model/store presence but did not expose a current MSRP amount.
- Follow-up note: the official model page embeds the current `Från 529 000 kr` MSRP in the SUV navigation object for the EQA URL. This is usable as a current model baseline, but not as a price-drop conclusion because no previous official same-trim MSRP is captured here.
- Evidence lane: official current baseline
- MSRP monitor classification: baseline_only_no_movement; keep separate from price-drop movement.
