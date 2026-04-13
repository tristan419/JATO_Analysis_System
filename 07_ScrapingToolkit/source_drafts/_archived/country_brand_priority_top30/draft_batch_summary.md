# MSRP Source Draft Batch

Source report: /Users/litristan/Downloads/JATO_Analysis_System/04_Processed_data/partitioned_dataset_v1
Candidate report top_n: 20
Draft count: 30
Ranking: country×brand priority score = sales_12m_sum + model_count*1000

| Global Rank | Rollout Batch | Country Rank | Country | Brand | Scope | Missing Models | Sales 12M Sum | Draft Path |
| ---: | ---: | ---: | --- | --- | --- | ---: | ---: | --- |
| 1 | 1 | 1 | 德国 | VOLKSWAGEN | brand_family | 9 | 419,012 | batch_01/de/01_volkswagen_de.yaml |
| 2 | 1 | 1 | 法国 | RENAULT | brand_family | 5 | 229,152 | batch_01/fr/01_renault_fr.yaml |
| 3 | 1 | 2 | 法国 | PEUGEOT | brand_family | 5 | 211,332 | batch_01/fr/02_peugeot_fr.yaml |
| 4 | 1 | 2 | 德国 | SKODA | brand_family | 4 | 131,844 | batch_01/de/02_skoda_de.yaml |
| 5 | 1 | 3 | 法国 | DACIA | brand_family | 3 | 115,093 | batch_01/fr/03_dacia_fr.yaml |
| 6 | 1 | 1 | 意大利 | FIAT | brand_family | 2 | 107,090 | batch_01/it/01_fiat_it.yaml |
| 7 | 1 | 4 | 法国 | CITROEN | brand_family | 3 | 95,748 | batch_01/fr/04_citroen_fr.yaml |
| 8 | 1 | 2 | 意大利 | TOYOTA | brand_family | 3 | 95,648 | batch_01/it/02_toyota_it.yaml |
| 9 | 1 | 1 | 捷克 | SKODA | brand_family | 8 | 81,578 | batch_01/cz/01_skoda_cz.yaml |
| 10 | 1 | 3 | 德国 | OPEL | brand_family | 2 | 84,612 | batch_01/de/03_opel_de.yaml |
| 11 | 2 | 3 | 意大利 | DACIA | brand_family | 2 | 77,028 | batch_02/it/03_dacia_it.yaml |
| 12 | 2 | 1 | 西班牙 | TOYOTA | brand_family | 4 | 74,803 | batch_02/es/01_toyota_es.yaml |
| 13 | 2 | 4 | 意大利 | VOLKSWAGEN | brand_family | 3 | 73,248 | batch_02/it/04_volkswagen_it.yaml |
| 14 | 2 | 5 | 法国 | TOYOTA | brand_family | 2 | 64,481 | batch_02/fr/05_toyota_fr.yaml |
| 15 | 2 | 2 | 西班牙 | SEAT | brand_family | 3 | 61,308 | batch_02/es/02_seat_es.yaml |
| 16 | 2 | 4 | 德国 | AUDI | brand_family | 2 | 61,277 | batch_02/de/04_audi_de.yaml |
| 17 | 2 | 5 | 意大利 | RENAULT | brand_family | 2 | 53,585 | batch_02/it/05_renault_it.yaml |
| 18 | 2 | 6 | 意大利 | JEEP | single_model | 1 | 48,786 | batch_02/it/06_jeep_it.yaml |
| 19 | 2 | 3 | 西班牙 | RENAULT | brand_family | 2 | 45,608 | batch_02/es/03_renault_es.yaml |
| 20 | 2 | 1 | 罗马尼亚 | DACIA | brand_family | 5 | 42,152 | batch_02/ro/01_dacia_ro.yaml |
| 21 | 3 | 4 | 西班牙 | PEUGEOT | brand_family | 2 | 41,841 | batch_03/es/04_peugeot_es.yaml |
| 22 | 3 | 7 | 意大利 | CITROEN | single_model | 1 | 37,509 | batch_03/it/07_citroen_it.yaml |
| 23 | 3 | 5 | 西班牙 | DACIA | single_model | 1 | 37,378 | batch_03/es/05_dacia_es.yaml |
| 24 | 3 | 6 | 西班牙 | HYUNDAI | brand_family | 2 | 35,698 | batch_03/es/06_hyundai_es.yaml |
| 25 | 3 | 1 | 挪威 | TESLA | brand_family | 2 | 33,626 | batch_03/no/01_tesla_no.yaml |
| 26 | 3 | 7 | 西班牙 | VOLKSWAGEN | brand_family | 2 | 33,201 | batch_03/es/07_volkswagen_es.yaml |
| 27 | 3 | 1 | 瑞典 | VOLKSWAGEN | brand_family | 6 | 27,756 | batch_03/se/01_volkswagen_se.yaml |
| 28 | 3 | 5 | 德国 | FIAT | single_model | 1 | 31,712 | batch_03/de/05_fiat_de.yaml |
| 29 | 3 | 8 | 意大利 | PEUGEOT | single_model | 1 | 30,158 | batch_03/it/08_peugeot_it.yaml |
| 30 | 3 | 1 | 波兰 | HYUNDAI | brand_family | 3 | 27,297 | batch_03/pl/01_hyundai_pl.yaml |

## Notes

- These are draft scaffolds only and are not loaded by the current runner.
- Promote a draft into 07_ScrapingToolkit/sources only after URL and selector verification.
- Draft files are grouped by rollout batch first, then by country, so the top30 backlog can be executed in three batches of ten.
- Every draft now includes placeholders for fixed_model, fixed_jato_model, fixed_jato_powertrain, copy_trim_to_jato_trim, edition_rules, powertrain_rules, and price_band_bonuses.
- The current production candidate coverage report remains unchanged until promotion.
