# MSRP Source Draft Batch

Source report: /Users/litristan/Downloads/JATO_Analysis_System/04_Processed_data/partitioned_dataset_v1
Candidate report top_n: 30
Report vehicle category: SUV
Draft count: 626
Selection unit: country×model candidates from the report.

## Dry-run 进度（2026-04-12）

| 市场 | Keyword | Dry-run | 通过率 |
|------|---------|---------|-------|
| SE 瑞典 | ✅ | ✅ 21/29 | 72.4% |
| HR 克罗地亚 | ✅ | ✅ 5/30 | 16.7% |
| HU 匈牙利 | ✅ | ✅ 16/31 | 51.6% |
| NO 挪威 | ✅ | ✅ 13/30 | 43.3% |
| AT 奥地利 | ✅ | ✅ 8/31 | 25.8% |
| CZ 捷克 | ✅ | ✅ 19/30 | 63.3% |
| CH 瑞士 | ✅ | ✅ 15/31 | 48.4% |
| 其余 14 国 | ⬜ | ⬜ | — |
| **合计** | **33%** | **97/212** | **45.8%** |

---

> **流程图已统一维护在**: `Markdown_Readme/Fullstack/MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` §5.0

## PDF 价目表发现（VW Group AT）

| 品牌 | 型号 | PDF URL | CDN |
|------|------|---------|-----|
| Audi | Q8 | `2145_audi_q8_pa_katalog_inkl__preisliste.pdf` | gpt-live.porsche.co.at |
| Audi | Q3 | `2503_q3_preisliste_final.pdf` | gpt-live.porsche.co.at |
| SEAT | Arona | `arona_neu_dupl.pdf` | gpt-live.porsche.co.at |
| SEAT | Ateca | `ateca_dupl.pdf` | porschegpt-prod.etn.cz |

**CDN 模式**: `https://gpt-live.porsche.co.at/at/brand/{A=Audi|S=SEAT}/pricelist/{filename}.pdf`
- Porsche Holding = VW Group 奥地利分销商，同一 CDN 承载 Audi / SEAT / Skoda / CUPRA / VW 价目表
- 品牌代码: A=Audi, S=SEAT, V=VW(?), C=CUPRA(?), K=Skoda(?)
- 后续可系统性探测所有 VW Group 品牌×市场组合

---

## 丹麦

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | SKODA | ELROQ | 11,389 | dk/01_skoda_elroq_dk.yaml |
| 2 | VOLKSWAGEN | ID.4 | 8,428 | dk/02_volkswagen_id_4_dk.yaml |
| 3 | TESLA | MODEL Y | 7,849 | dk/03_tesla_model_y_dk.yaml |
| 4 | SKODA | ENYAQ | 5,234 | dk/04_skoda_enyaq_dk.yaml |
| 5 | AUDI | Q4 E-TRON | 4,632 | dk/05_audi_q4_e_tron_dk.yaml |
| 6 | VOLKSWAGEN | T-ROC | 3,471 | dk/06_volkswagen_t_roc_dk.yaml |
| 7 | TOYOTA | BZ4X | 3,319 | dk/07_toyota_bz4x_dk.yaml |
| 8 | MERCEDES | EQA | 3,054 | dk/08_mercedes_eqa_dk.yaml |
| 9 | CUPRA | FORMENTOR | 3,020 | dk/09_cupra_formentor_dk.yaml |
| 10 | BMW | IX1 | 3,018 | dk/10_bmw_ix1_dk.yaml |
| 11 | KIA | EV3 | 2,975 | dk/11_kia_ev3_dk.yaml |
| 12 | MERCEDES | EQB | 2,828 | dk/12_mercedes_eqb_dk.yaml |
| 13 | CUPRA | TAVASCAN | 2,539 | dk/13_cupra_tavascan_dk.yaml |
| 14 | XPENG | G6 | 2,496 | dk/14_xpeng_g6_dk.yaml |
| 15 | BMW | X1 | 2,333 | dk/15_bmw_x1_dk.yaml |
| 16 | RENAULT | SCENIC | 2,267 | dk/16_renault_scenic_dk.yaml |
| 17 | TOYOTA | YARIS CROSS | 2,071 | dk/17_toyota_yaris_cross_dk.yaml |
| 18 | VOLVO | EX40 | 2,045 | dk/18_volvo_ex40_dk.yaml |
| 19 | TOYOTA | AYGO X | 1,975 | dk/19_toyota_aygo_x_dk.yaml |
| 20 | CUPRA | TERRAMAR | 1,723 | dk/20_cupra_terramar_dk.yaml |
| 21 | FORD | EXPLORER EV | 1,709 | dk/21_ford_explorer_ev_dk.yaml |
| 22 | NISSAN | QASHQAI | 1,666 | dk/22_nissan_qashqai_dk.yaml |
| 23 | DFSK | E5 | 1,608 | dk/23_dfsk_e5_dk.yaml |
| 24 | HYUNDAI | IONIQ 5 | 1,517 | dk/24_hyundai_ioniq_5_dk.yaml |
| 25 | AUDI | Q6 E-TRON | 1,461 | dk/25_audi_q6_e_tron_dk.yaml |
| 26 | KIA | EV6 | 1,433 | dk/26_kia_ev6_dk.yaml |
| 27 | MG | ZS | 1,433 | dk/27_mg_zs_dk.yaml |
| 28 | PEUGEOT | 3008 | 1,433 | dk/28_peugeot_3008_dk.yaml |
| 29 | CITROEN | C3 AIRCROSS | 1,355 | dk/29_citroen_c3_aircross_dk.yaml |
| 30 | PEUGEOT | 2008 | 1,355 | dk/30_peugeot_2008_dk.yaml |

## 克罗地亚 — ✅ Dry-run 5/30 (16.7%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | VOLKSWAGEN | T-CROSS | 3,237 | hr/01_volkswagen_t_cross_hr.yaml |
| 2 | OPEL | MOKKA | 2,143 | hr/02_opel_mokka_hr.yaml |
| 3 | SUZUKI | VITARA | 2,029 | hr/03_suzuki_vitara_hr.yaml |
| 4 | RENAULT | CAPTUR | 1,884 | hr/04_renault_captur_hr.yaml |
| 5 | DACIA | DUSTER | 1,643 | hr/05_dacia_duster_hr.yaml |
| 6 | SKODA | KAMIQ | 1,278 | hr/06_skoda_kamiq_hr.yaml |
| 7 | VOLKSWAGEN | TIGUAN | 1,236 | hr/07_volkswagen_tiguan_hr.yaml |
| 8 | SUZUKI | S-CROSS | 1,191 | hr/08_suzuki_s_cross_hr.yaml |
| 9 | HYUNDAI | TUCSON | 1,068 | hr/09_hyundai_tucson_hr.yaml |
| 10 | SKODA | KODIAQ | 1,053 | hr/10_skoda_kodiaq_hr.yaml |
| 11 | KIA | STONIC | 956 | hr/11_kia_stonic_hr.yaml |
| 12 | TOYOTA | YARIS CROSS | 944 | hr/12_toyota_yaris_cross_hr.yaml |
| 13 | VOLKSWAGEN | T-ROC | 926 | hr/13_volkswagen_t_roc_hr.yaml |
| 14 | VOLKSWAGEN | TAIGO | 862 | hr/14_volkswagen_taigo_hr.yaml |
| 15 | KIA | SPORTAGE | 821 | hr/15_kia_sportage_hr.yaml |
| 16 | SKODA | KAROQ | 685 | hr/16_skoda_karoq_hr.yaml |
| 17 | TOYOTA | C-HR | 678 | hr/17_toyota_c_hr_hr.yaml |
| 18 | VOLKSWAGEN | TAYRON | 640 | hr/18_volkswagen_tayron_hr.yaml |
| 19 | NISSAN | QASHQAI | 583 | hr/19_nissan_qashqai_hr.yaml |
| 20 | MAZDA | CX-30 | 564 | hr/20_mazda_cx_30_hr.yaml |
| 21 | GEELY | COOLRAY | 560 | hr/21_geely_coolray_hr.yaml |
| 22 | MG | ZS | 523 | hr/22_mg_zs_hr.yaml |
| 23 | CUPRA | TERRAMAR | 519 | hr/23_cupra_terramar_hr.yaml |
| 24 | FORD | KUGA | 517 | hr/24_ford_kuga_hr.yaml |
| 25 | PEUGEOT | 2008 | 511 | hr/25_peugeot_2008_hr.yaml |
| 26 | BMW | X1 | 481 | hr/26_bmw_x1_hr.yaml |
| 27 | AUDI | Q3 | 436 | hr/27_audi_q3_hr.yaml |
| 28 | TOYOTA | RAV4 | 409 | hr/28_toyota_rav4_hr.yaml |
| 29 | KIA | XCEED | 399 | hr/29_kia_xceed_hr.yaml |
| 30 | OPEL | FRONTERA | 398 | hr/30_opel_frontera_hr.yaml |

## 匈牙利 — ✅ Dry-run 15/30 (50.0%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | SUZUKI | S-CROSS | 5,892 | hu/01_suzuki_s_cross_hu.yaml |
| 2 | SUZUKI | VITARA | 5,200 | hu/02_suzuki_vitara_hu.yaml |
| 3 | NISSAN | QASHQAI | 3,251 | hu/03_nissan_qashqai_hu.yaml |
| 4 | TOYOTA | YARIS CROSS | 3,136 | hu/04_toyota_yaris_cross_hu.yaml |
| 5 | DACIA | DUSTER | 3,042 | hu/05_dacia_duster_hu.yaml |
| 6 | TOYOTA | C-HR | 2,430 | hu/06_toyota_c_hr_hu.yaml |
| 7 | KIA | SPORTAGE | 2,251 | hu/07_kia_sportage_hu.yaml |
| 8 | HYUNDAI | TUCSON | 2,161 | hu/08_hyundai_tucson_hu.yaml |
| 9 | TOYOTA | COROLLA CROSS | 1,831 | hu/09_toyota_corolla_cross_hu.yaml |
| 10 | FORD | KUGA | 1,684 | hu/10_ford_kuga_hu.yaml |
| 11 | FORD | PUMA | 1,589 | hu/11_ford_puma_hu.yaml |
| 12 | VOLKSWAGEN | T-ROC | 1,400 | hu/12_volkswagen_t_roc_hu.yaml |
| 13 | SKODA | KODIAQ | 1,366 | hu/13_skoda_kodiaq_hu.yaml |
| 14 | MG | ZS | 1,362 | hu/14_mg_zs_hu.yaml |
| 15 | TESLA | MODEL Y | 1,210 | hu/15_tesla_model_y_hu.yaml |
| 16 | VOLKSWAGEN | TIGUAN | 1,203 | hu/16_volkswagen_tiguan_hu.yaml |
| 17 | TOYOTA | RAV4 | 1,184 | hu/17_toyota_rav4_hu.yaml |
| 18 | NISSAN | X-TRAIL | 1,099 | hu/18_nissan_x_trail_hu.yaml |
| 19 | RENAULT | CAPTUR | 1,034 | hu/19_renault_captur_hu.yaml |
| 20 | KGM | KORANDO | 1,026 | hu/20_kgm_korando_hu.yaml |
| 21 | VOLVO | XC60 | 956 | hu/21_volvo_xc60_hu.yaml |
| 22 | PEUGEOT | 3008 | 939 | hu/22_peugeot_3008_hu.yaml |
| 23 | JAECOO | 7 | 936 | hu/23_jaecoo_7_hu.yaml |
| 24 | BMW | X5 | 925 | hu/24_bmw_x5_hu.yaml |
| 25 | OMODA | 5 | 882 | hu/25_omoda_5_hu.yaml |
| 26 | VOLVO | XC40 | 881 | hu/26_volvo_xc40_hu.yaml |
| 27 | OPEL | FRONTERA | 832 | hu/27_opel_frontera_hu.yaml |
| 28 | PEUGEOT | 2008 | 784 | hu/28_peugeot_2008_hu.yaml |
| 29 | TOYOTA | AYGO X | 780 | hu/29_toyota_aygo_x_hu.yaml |
| 30 | DACIA | BIGSTER | 779 | hu/30_dacia_bigster_hu.yaml |

## 奥地利 — ✅ Dry-run 7/30 (23.3%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | TESLA | MODEL Y | 4,649 | at/01_tesla_model_y_at.yaml |
| 2 | VOLKSWAGEN | TIGUAN | 4,429 | at/02_volkswagen_tiguan_at.yaml |
| 3 | SKODA | KAROQ | 4,285 | at/03_skoda_karoq_at.yaml |
| 4 | DACIA | BIGSTER | 3,595 | at/04_dacia_bigster_at.yaml |
| 5 | DACIA | DUSTER | 3,578 | at/05_dacia_duster_at.yaml |
| 6 | SKODA | ELROQ | 3,432 | at/06_skoda_elroq_at.yaml |
| 7 | MG | ZS | 3,093 | at/07_mg_zs_at.yaml |
| 8 | HYUNDAI | TUCSON | 3,090 | at/08_hyundai_tucson_at.yaml |
| 9 | CUPRA | TERRAMAR | 2,854 | at/09_cupra_terramar_at.yaml |
| 10 | VOLKSWAGEN | T-CROSS | 2,853 | at/10_volkswagen_t_cross_at.yaml |
| 11 | MAZDA | CX-30 | 2,848 | at/11_mazda_cx_30_at.yaml |
| 12 | BMW | X1 | 2,845 | at/12_bmw_x1_at.yaml |
| 13 | VOLKSWAGEN | T-ROC | 2,771 | at/13_volkswagen_t_roc_at.yaml |
| 14 | TOYOTA | YARIS CROSS | 2,619 | at/14_toyota_yaris_cross_at.yaml |
| 15 | BYD | SEAL U | 2,600 | at/15_byd_seal_u_at.yaml |
| 16 | SKODA | ENYAQ | 2,525 | at/16_skoda_enyaq_at.yaml |
| 17 | SKODA | KODIAQ | 2,424 | at/17_skoda_kodiaq_at.yaml |
| 18 | SEAT | ARONA | 2,391 | at/18_seat_arona_at.yaml |
| 19 | BMW | IX1 | 2,383 | at/19_bmw_ix1_at.yaml |
| 20 | SKODA | KAMIQ | 2,356 | at/20_skoda_kamiq_at.yaml |
| 21 | BMW | X3 | 2,219 | at/21_bmw_x3_at.yaml |
| 22 | BYD | SEALION 7 | 2,168 | at/22_byd_sealion_7_at.yaml |
| 23 | HYUNDAI | KONA | 2,065 | at/23_hyundai_kona_at.yaml |
| 24 | CUPRA | FORMENTOR | 1,991 | at/24_cupra_formentor_at.yaml |
| 25 | AUDI | Q8 | 1,946 | at/25_audi_q8_at.yaml |
| 26 | MERCEDES | GLC | 1,889 | at/26_mercedes_glc_at.yaml |
| 27 | SEAT | ATECA | 1,859 | at/27_seat_ateca_at.yaml |
| 28 | PEUGEOT | 3008 | 1,810 | at/28_peugeot_3008_at.yaml |
| 29 | VOLKSWAGEN | ID.4 | 1,763 | at/29_volkswagen_id_4_at.yaml |
| 30 | FORD | PUMA | 1,645 | at/30_ford_puma_at.yaml |

## 希腊

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | TOYOTA | YARIS CROSS | 7,956 | gr/01_toyota_yaris_cross_gr.yaml |
| 2 | PEUGEOT | 2008 | 6,296 | gr/02_peugeot_2008_gr.yaml |
| 3 | SUZUKI | VITARA | 4,398 | gr/03_suzuki_vitara_gr.yaml |
| 4 | TOYOTA | C-HR | 3,471 | gr/04_toyota_c_hr_gr.yaml |
| 5 | MG | ZS | 2,932 | gr/05_mg_zs_gr.yaml |
| 6 | BMW | X1 | 2,576 | gr/06_bmw_x1_gr.yaml |
| 7 | OPEL | MOKKA | 2,488 | gr/07_opel_mokka_gr.yaml |
| 8 | TOYOTA | AYGO X | 2,339 | gr/08_toyota_aygo_x_gr.yaml |
| 9 | DACIA | DUSTER | 2,253 | gr/09_dacia_duster_gr.yaml |
| 10 | TOYOTA | COROLLA CROSS | 1,937 | gr/10_toyota_corolla_cross_gr.yaml |
| 11 | FORD | PUMA | 1,908 | gr/11_ford_puma_gr.yaml |
| 12 | NISSAN | JUKE | 1,849 | gr/12_nissan_juke_gr.yaml |
| 13 | NISSAN | QASHQAI | 1,753 | gr/13_nissan_qashqai_gr.yaml |
| 14 | HYUNDAI | KONA | 1,534 | gr/14_hyundai_kona_gr.yaml |
| 15 | VOLKSWAGEN | T-CROSS | 1,478 | gr/15_volkswagen_t_cross_gr.yaml |
| 16 | JEEP | AVENGER | 1,452 | gr/16_jeep_avenger_gr.yaml |
| 17 | MERCEDES | GLA | 1,442 | gr/17_mercedes_gla_gr.yaml |
| 18 | VOLKSWAGEN | T-ROC | 1,425 | gr/18_volkswagen_t_roc_gr.yaml |
| 19 | PEUGEOT | 3008 | 1,409 | gr/19_peugeot_3008_gr.yaml |
| 20 | CITROEN | C3 AIRCROSS | 1,264 | gr/20_citroen_c3_aircross_gr.yaml |
| 21 | VOLKSWAGEN | TIGUAN | 1,247 | gr/21_volkswagen_tiguan_gr.yaml |
| 22 | OPEL | FRONTERA | 1,239 | gr/22_opel_frontera_gr.yaml |
| 23 | MINI | COUNTRYMAN | 1,228 | gr/23_mini_countryman_gr.yaml |
| 24 | SUZUKI | S-CROSS | 1,193 | gr/24_suzuki_s_cross_gr.yaml |
| 25 | BMW | X2 | 1,147 | gr/25_bmw_x2_gr.yaml |
| 26 | RENAULT | CAPTUR | 1,042 | gr/26_renault_captur_gr.yaml |
| 27 | HYUNDAI | TUCSON | 1,030 | gr/27_hyundai_tucson_gr.yaml |
| 28 | KIA | STONIC | 971 | gr/28_kia_stonic_gr.yaml |
| 29 | SKODA | KAMIQ | 851 | gr/29_skoda_kamiq_gr.yaml |
| 30 | FORD | KUGA | 836 | gr/30_ford_kuga_gr.yaml |

## 德国

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | VOLKSWAGEN | T-ROC | 75,681 | de/01_volkswagen_t_roc_de.yaml |
| 2 | VOLKSWAGEN | TIGUAN | 58,447 | de/02_volkswagen_tiguan_de.yaml |
| 3 | VOLKSWAGEN | T-CROSS | 30,942 | de/03_volkswagen_t_cross_de.yaml |
| 4 | SKODA | KODIAQ | 30,379 | de/04_skoda_kodiaq_de.yaml |
| 5 | VOLKSWAGEN | TAYRON | 29,876 | de/05_volkswagen_tayron_de.yaml |
| 6 | SKODA | KAROQ | 28,374 | de/06_skoda_karoq_de.yaml |
| 7 | SKODA | ELROQ | 28,284 | de/07_skoda_elroq_de.yaml |
| 9 | MERCEDES | GLC | 27,452 | de/09_mercedes_glc_de.yaml |
| 11 | CUPRA | FORMENTOR | 24,115 | de/11_cupra_formentor_de.yaml |
| 12 | VOLVO | XC60 | 22,390 | de/12_volvo_xc60_de.yaml |
| 13 | SKODA | KAMIQ | 22,251 | de/13_skoda_kamiq_de.yaml |
| 14 | HYUNDAI | TUCSON | 20,979 | de/14_hyundai_tucson_de.yaml |
| 15 | SKODA | ENYAQ | 20,443 | de/15_skoda_enyaq_de.yaml |
| 16 | VOLKSWAGEN | ID.4 | 20,366 | de/16_volkswagen_id_4_de.yaml |
| 17 | OPEL | MOKKA | 20,226 | de/17_opel_mokka_de.yaml |
| 18 | TOYOTA | AYGO X | 19,938 | de/18_toyota_aygo_x_de.yaml |
| 19 | VOLKSWAGEN | TAIGO | 19,712 | de/19_volkswagen_taigo_de.yaml |
| 21 | OPEL | GRANDLAND | 19,169 | de/21_opel_grandland_de.yaml |
| 22 | CUPRA | TERRAMAR | 17,725 | de/22_cupra_terramar_de.yaml |
| 23 | FORD | PUMA | 16,644 | de/23_ford_puma_de.yaml |
| 24 | FORD | KUGA | 15,967 | de/24_ford_kuga_de.yaml |
| 25 | SEAT | ARONA | 15,860 | de/25_seat_arona_de.yaml |
| 26 | TOYOTA | YARIS CROSS | 15,668 | de/26_toyota_yaris_cross_de.yaml |
| 27 | NISSAN | QASHQAI | 14,826 | de/27_nissan_qashqai_de.yaml |
| 28 | DACIA | BIGSTER | 14,681 | de/28_dacia_bigster_de.yaml |
| 29 | HYUNDAI | KONA | 14,601 | de/29_hyundai_kona_de.yaml |
| 30 | DACIA | DUSTER | 14,520 | de/30_dacia_duster_de.yaml |

## 意大利

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | JEEP | AVENGER | 48,786 | it/01_jeep_avenger_it.yaml |
| 2 | TOYOTA | YARIS CROSS | 36,246 | it/02_toyota_yaris_cross_it.yaml |
| 3 | DACIA | DUSTER | 30,268 | it/03_dacia_duster_it.yaml |
| 4 | MG | ZS | 28,666 | it/04_mg_zs_it.yaml |
| 5 | VOLKSWAGEN | T-ROC | 27,899 | it/05_volkswagen_t_roc_it.yaml |
| 6 | FORD | PUMA | 26,503 | it/06_ford_puma_it.yaml |
| 7 | TOYOTA | AYGO X | 25,984 | it/07_toyota_aygo_x_it.yaml |
| 8 | RENAULT | CAPTUR | 24,978 | it/08_renault_captur_it.yaml |
| 9 | VOLKSWAGEN | TIGUAN | 24,172 | it/09_volkswagen_tiguan_it.yaml |
| 10 | BMW | X1 | 22,534 | it/10_bmw_x1_it.yaml |
| 11 | VOLKSWAGEN | T-CROSS | 21,177 | it/11_volkswagen_t_cross_it.yaml |
| 12 | FIAT | 600 | 20,297 | it/12_fiat_600_it.yaml |
| 13 | KIA | SPORTAGE | 17,925 | it/13_kia_sportage_it.yaml |
| 14 | PEUGEOT | 2008 | 17,672 | it/14_peugeot_2008_it.yaml |
| 15 | PEUGEOT | 3008 | 16,818 | it/15_peugeot_3008_it.yaml |
| 16 | NISSAN | QASHQAI | 16,673 | it/16_nissan_qashqai_it.yaml |
| 17 | BYD | SEAL U | 16,013 | it/17_byd_seal_u_it.yaml |
| 18 | FIAT | PANDINA | 15,752 | it/18_fiat_pandina_it.yaml |
| 19 | TOYOTA | C-HR | 15,630 | it/19_toyota_c_hr_it.yaml |
| 20 | FIAT | GRANDE PANDA | 14,455 | it/20_fiat_grande_panda_it.yaml |
| 21 | ALFA ROMEO | JUNIOR | 14,320 | it/21_alfa_romeo_junior_it.yaml |
| 22 | HYUNDAI | TUCSON | 13,072 | it/22_hyundai_tucson_it.yaml |
| 23 | MERCEDES | GLA | 12,919 | it/23_mercedes_gla_it.yaml |
| 24 | NISSAN | JUKE | 11,005 | it/24_nissan_juke_it.yaml |
| 25 | SKODA | KAMIQ | 10,606 | it/25_skoda_kamiq_it.yaml |
| 26 | SUZUKI | VITARA | 10,431 | it/26_suzuki_vitara_it.yaml |
| 27 | ALFA ROMEO | TONALE | 10,058 | it/27_alfa_romeo_tonale_it.yaml |
| 28 | FORD | KUGA | 9,806 | it/28_ford_kuga_it.yaml |
| 29 | MERCEDES | GLC | 9,618 | it/29_mercedes_glc_it.yaml |
| 30 | BMW | X3 | 9,280 | it/30_bmw_x3_it.yaml |

## 挪威 — ✅ Dry-run 13/30 (43.3%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | TESLA | MODEL Y | 27,322 | no/01_tesla_model_y_no.yaml |
| 2 | VOLKSWAGEN | ID.4 | 8,041 | no/02_volkswagen_id_4_no.yaml |
| 3 | TOYOTA | BZ4X | 6,270 | no/03_toyota_bz4x_no.yaml |
| 4 | VOLVO | EX40 | 5,196 | no/04_volvo_ex40_no.yaml |
| 5 | VOLVO | EX30 | 5,017 | no/05_volvo_ex30_no.yaml |
| 6 | SKODA | ENYAQ | 4,717 | no/06_skoda_enyaq_no.yaml |
| 7 | BYD | SEALION 7 | 4,252 | no/07_byd_sealion_7_no.yaml |
| 8 | NISSAN | ARIYA | 4,038 | no/08_nissan_ariya_no.yaml |
| 9 | FORD | EXPLORER EV | 3,992 | no/09_ford_explorer_ev_no.yaml |
| 10 | SKODA | ELROQ | 3,972 | no/10_skoda_elroq_no.yaml |
| 11 | BMW | IX1 | 3,340 | no/11_bmw_ix1_no.yaml |
| 12 | XPENG | G6 | 2,838 | no/12_xpeng_g6_no.yaml |
| 13 | AUDI | Q6 E-TRON | 2,721 | no/13_audi_q6_e_tron_no.yaml |
| 14 | AUDI | Q4 E-TRON | 2,575 | no/14_audi_q4_e_tron_no.yaml |
| 15 | KIA | EV3 | 2,461 | no/15_kia_ev3_no.yaml |
| 16 | HYUNDAI | IONIQ 5 | 2,234 | no/16_hyundai_ioniq_5_no.yaml |
| 17 | POLESTAR | 4 | 2,102 | no/17_polestar_4_no.yaml |
| 18 | HYUNDAI | KONA | 1,960 | no/18_hyundai_kona_no.yaml |
| 19 | VOLVO | EX90 | 1,949 | no/19_volvo_ex90_no.yaml |
| 20 | MERCEDES | EQB | 1,927 | no/20_mercedes_eqb_no.yaml |
| 21 | MERCEDES | EQA | 1,875 | no/21_mercedes_eqa_no.yaml |
| 22 | BMW | IX | 1,660 | no/22_bmw_ix_no.yaml |
| 23 | MG | S5 | 1,575 | no/23_mg_s5_no.yaml |
| 24 | FORD | CAPRI | 1,545 | no/24_ford_capri_no.yaml |
| 25 | XPENG | G9 | 1,447 | no/25_xpeng_g9_no.yaml |
| 26 | TOYOTA | YARIS CROSS | 1,259 | no/26_toyota_yaris_cross_no.yaml |
| 27 | PORSCHE | MACAN | 1,212 | no/27_porsche_macan_no.yaml |
| 28 | PEUGEOT | 5008 | 1,181 | no/28_peugeot_5008_no.yaml |
| 29 | BMW | IX2 | 1,033 | no/29_bmw_ix2_no.yaml |
| 30 | POLESTAR | 3 | 891 | no/30_polestar_3_no.yaml |

## 捷克 — ✅ Dry-run 19/30 (63.3%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | SKODA | KAMIQ | 11,466 | cz/01_skoda_kamiq_cz.yaml |
| 2 | SKODA | KAROQ | 11,433 | cz/02_skoda_karoq_cz.yaml |
| 3 | SKODA | KODIAQ | 11,031 | cz/03_skoda_kodiaq_cz.yaml |
| 4 | HYUNDAI | TUCSON | 5,799 | cz/04_hyundai_tucson_cz.yaml |
| 5 | DACIA | DUSTER | 4,216 | cz/05_dacia_duster_cz.yaml |
| 6 | MG | ZS | 2,985 | cz/06_mg_zs_cz.yaml |
| 7 | SKODA | ELROQ | 2,883 | cz/07_skoda_elroq_cz.yaml |
| 8 | TOYOTA | RAV4 | 2,675 | cz/08_toyota_rav4_cz.yaml |
| 9 | DACIA | BIGSTER | 2,424 | cz/09_dacia_bigster_cz.yaml |
| 10 | VOLKSWAGEN | TIGUAN | 2,359 | cz/10_volkswagen_tiguan_cz.yaml |
| 11 | KIA | SPORTAGE | 2,318 | cz/11_kia_sportage_cz.yaml |
| 12 | RENAULT | CAPTUR | 1,959 | cz/12_renault_captur_cz.yaml |
| 13 | TOYOTA | C-HR | 1,858 | cz/13_toyota_c_hr_cz.yaml |
| 14 | SKODA | ENYAQ | 1,832 | cz/14_skoda_enyaq_cz.yaml |
| 15 | FORD | KUGA | 1,823 | cz/15_ford_kuga_cz.yaml |
| 16 | TOYOTA | YARIS CROSS | 1,767 | cz/16_toyota_yaris_cross_cz.yaml |
| 17 | PEUGEOT | 2008 | 1,755 | cz/17_peugeot_2008_cz.yaml |
| 18 | SUZUKI | S-CROSS | 1,508 | cz/18_suzuki_s_cross_cz.yaml |
| 19 | KGM | KORANDO | 1,486 | cz/19_kgm_korando_cz.yaml |
| 20 | FORD | PUMA | 1,444 | cz/20_ford_puma_cz.yaml |
| 21 | TESLA | MODEL Y | 1,419 | cz/21_tesla_model_y_cz.yaml |
| 22 | CUPRA | FORMENTOR | 1,397 | cz/22_cupra_formentor_cz.yaml |
| 23 | HYUNDAI | KONA | 1,338 | cz/23_hyundai_kona_cz.yaml |
| 24 | VOLKSWAGEN | TAYRON | 1,325 | cz/24_volkswagen_tayron_cz.yaml |
| 25 | JAECOO | 7 | 1,289 | cz/25_jaecoo_7_cz.yaml |
| 26 | NISSAN | QASHQAI | 1,288 | cz/26_nissan_qashqai_cz.yaml |
| 27 | VOLVO | XC90 | 1,239 | cz/27_volvo_xc90_cz.yaml |
| 28 | VOLVO | XC60 | 1,173 | cz/28_volvo_xc60_cz.yaml |
| 29 | KIA | STONIC | 1,156 | cz/29_kia_stonic_cz.yaml |
| 30 | SUZUKI | VITARA | 1,156 | cz/30_suzuki_vitara_cz.yaml |

## 斯洛伐克

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | SKODA | KAROQ | 2,540 | sk/01_skoda_karoq_sk.yaml |
| 2 | KIA | SPORTAGE | 2,517 | sk/02_kia_sportage_sk.yaml |
| 3 | SKODA | KODIAQ | 2,058 | sk/03_skoda_kodiaq_sk.yaml |
| 4 | HYUNDAI | TUCSON | 1,871 | sk/04_hyundai_tucson_sk.yaml |
| 5 | SKODA | KAMIQ | 1,857 | sk/05_skoda_kamiq_sk.yaml |
| 6 | TOYOTA | YARIS CROSS | 1,308 | sk/06_toyota_yaris_cross_sk.yaml |
| 7 | DACIA | DUSTER | 1,273 | sk/07_dacia_duster_sk.yaml |
| 8 | VOLKSWAGEN | T-CROSS | 1,140 | sk/08_volkswagen_t_cross_sk.yaml |
| 9 | VOLKSWAGEN | TIGUAN | 1,118 | sk/09_volkswagen_tiguan_sk.yaml |
| 10 | TOYOTA | RAV4 | 1,106 | sk/10_toyota_rav4_sk.yaml |
| 11 | TOYOTA | C-HR | 1,043 | sk/11_toyota_c_hr_sk.yaml |
| 12 | SUZUKI | S-CROSS | 1,033 | sk/12_suzuki_s_cross_sk.yaml |
| 13 | MG | ZS | 871 | sk/13_mg_zs_sk.yaml |
| 14 | TOYOTA | COROLLA CROSS | 839 | sk/14_toyota_corolla_cross_sk.yaml |
| 15 | SUZUKI | VITARA | 835 | sk/15_suzuki_vitara_sk.yaml |
| 16 | NISSAN | QASHQAI | 834 | sk/16_nissan_qashqai_sk.yaml |
| 17 | FORD | KUGA | 833 | sk/17_ford_kuga_sk.yaml |
| 18 | VOLKSWAGEN | TAIGO | 814 | sk/18_volkswagen_taigo_sk.yaml |
| 19 | SKODA | ELROQ | 810 | sk/19_skoda_elroq_sk.yaml |
| 20 | VOLKSWAGEN | T-ROC | 723 | sk/20_volkswagen_t_roc_sk.yaml |
| 21 | DACIA | BIGSTER | 722 | sk/21_dacia_bigster_sk.yaml |
| 22 | KIA | STONIC | 692 | sk/22_kia_stonic_sk.yaml |
| 23 | KGM | KORANDO | 614 | sk/23_kgm_korando_sk.yaml |
| 24 | VOLKSWAGEN | TAYRON | 588 | sk/24_volkswagen_tayron_sk.yaml |
| 25 | VOLKSWAGEN | TOUAREG | 588 | sk/25_volkswagen_touareg_sk.yaml |
| 26 | KIA | XCEED | 576 | sk/26_kia_xceed_sk.yaml |
| 27 | PEUGEOT | 2008 | 532 | sk/27_peugeot_2008_sk.yaml |
| 28 | HYUNDAI | KONA | 524 | sk/28_hyundai_kona_sk.yaml |
| 29 | PEUGEOT | 3008 | 503 | sk/29_peugeot_3008_sk.yaml |
| 30 | RENAULT | CAPTUR | 495 | sk/30_renault_captur_sk.yaml |

## 斯洛文尼亚

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | RENAULT | CAPTUR | 1,696 | si/01_renault_captur_si.yaml |
| 2 | SKODA | KODIAQ | 1,274 | si/02_skoda_kodiaq_si.yaml |
| 3 | SKODA | KAMIQ | 1,254 | si/03_skoda_kamiq_si.yaml |
| 4 | VOLKSWAGEN | TIGUAN | 1,182 | si/04_volkswagen_tiguan_si.yaml |
| 5 | VOLKSWAGEN | T-ROC | 1,165 | si/05_volkswagen_t_roc_si.yaml |
| 6 | HYUNDAI | TUCSON | 1,125 | si/06_hyundai_tucson_si.yaml |
| 7 | PEUGEOT | 2008 | 1,058 | si/07_peugeot_2008_si.yaml |
| 8 | VOLKSWAGEN | T-CROSS | 974 | si/08_volkswagen_t_cross_si.yaml |
| 9 | DACIA | DUSTER | 880 | si/09_dacia_duster_si.yaml |
| 10 | TOYOTA | YARIS CROSS | 793 | si/10_toyota_yaris_cross_si.yaml |
| 11 | VOLKSWAGEN | TAIGO | 769 | si/11_volkswagen_taigo_si.yaml |
| 12 | NISSAN | QASHQAI | 760 | si/12_nissan_qashqai_si.yaml |
| 13 | DONGFENG-FENGXING | T5 EVO | 718 | si/13_dongfeng_fengxing_t5_evo_si.yaml |
| 14 | KIA | SPORTAGE | 660 | si/14_kia_sportage_si.yaml |
| 15 | TESLA | MODEL Y | 644 | si/15_tesla_model_y_si.yaml |
| 16 | FORD | KUGA | 600 | si/16_ford_kuga_si.yaml |
| 17 | MG | ZS | 561 | si/17_mg_zs_si.yaml |
| 18 | OPEL | MOKKA | 549 | si/18_opel_mokka_si.yaml |
| 19 | HYUNDAI | KONA | 541 | si/19_hyundai_kona_si.yaml |
| 20 | KIA | STONIC | 531 | si/20_kia_stonic_si.yaml |
| 21 | SKODA | KAROQ | 492 | si/21_skoda_karoq_si.yaml |
| 22 | JEEP | AVENGER | 477 | si/22_jeep_avenger_si.yaml |
| 23 | MAZDA | CX-30 | 475 | si/23_mazda_cx_30_si.yaml |
| 24 | RENAULT | AUSTRAL | 460 | si/24_renault_austral_si.yaml |
| 25 | SUZUKI | VITARA | 420 | si/25_suzuki_vitara_si.yaml |
| 26 | TOYOTA | C-HR | 419 | si/26_toyota_c_hr_si.yaml |
| 27 | VOLKSWAGEN | TAYRON | 397 | si/27_volkswagen_tayron_si.yaml |
| 28 | PEUGEOT | 3008 | 360 | si/28_peugeot_3008_si.yaml |
| 29 | RENAULT | SYMBIOZ | 355 | si/29_renault_symbioz_si.yaml |
| 30 | TOYOTA | RAV4 | 354 | si/30_toyota_rav4_si.yaml |

## 比利时

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | BMW | IX1 | 8,974 | be/01_bmw_ix1_be.yaml |
| 2 | PEUGEOT | 2008 | 6,621 | be/02_peugeot_2008_be.yaml |
| 3 | DACIA | DUSTER | 6,315 | be/03_dacia_duster_be.yaml |
| 4 | TESLA | MODEL Y | 6,261 | be/04_tesla_model_y_be.yaml |
| 5 | AUDI | Q6 E-TRON | 5,193 | be/05_audi_q6_e_tron_be.yaml |
| 6 | BMW | X1 | 5,167 | be/06_bmw_x1_be.yaml |
| 7 | VOLKSWAGEN | ID.4 | 5,161 | be/07_volkswagen_id_4_be.yaml |
| 8 | MERCEDES | EQB | 5,151 | be/08_mercedes_eqb_be.yaml |
| 9 | PEUGEOT | 3008 | 4,998 | be/09_peugeot_3008_be.yaml |
| 10 | VOLKSWAGEN | T-ROC | 4,979 | be/10_volkswagen_t_roc_be.yaml |
| 11 | HYUNDAI | TUCSON | 4,850 | be/11_hyundai_tucson_be.yaml |
| 12 | RENAULT | CAPTUR | 4,831 | be/12_renault_captur_be.yaml |
| 13 | SKODA | ELROQ | 4,563 | be/13_skoda_elroq_be.yaml |
| 14 | KIA | EV3 | 4,087 | be/14_kia_ev3_be.yaml |
| 15 | SKODA | ENYAQ | 3,907 | be/15_skoda_enyaq_be.yaml |
| 16 | AUDI | Q4 E-TRON | 3,811 | be/16_audi_q4_e_tron_be.yaml |
| 17 | DACIA | BIGSTER | 3,685 | be/17_dacia_bigster_be.yaml |
| 18 | BMW | IX2 | 3,649 | be/18_bmw_ix2_be.yaml |
| 19 | PEUGEOT | 5008 | 3,616 | be/19_peugeot_5008_be.yaml |
| 20 | VOLVO | EX30 | 3,536 | be/20_volvo_ex30_be.yaml |
| 21 | FORD | PUMA | 3,458 | be/21_ford_puma_be.yaml |
| 22 | TOYOTA | YARIS CROSS | 3,452 | be/22_toyota_yaris_cross_be.yaml |
| 23 | MERCEDES | EQA | 3,401 | be/23_mercedes_eqa_be.yaml |
| 24 | CITROEN | C3 AIRCROSS | 3,233 | be/24_citroen_c3_aircross_be.yaml |
| 25 | VOLKSWAGEN | TIGUAN | 3,187 | be/25_volkswagen_tiguan_be.yaml |
| 26 | MERCEDES | GLA | 3,020 | be/26_mercedes_gla_be.yaml |
| 27 | VOLVO | XC40 | 2,994 | be/27_volvo_xc40_be.yaml |
| 28 | NISSAN | QASHQAI | 2,988 | be/28_nissan_qashqai_be.yaml |
| 29 | VOLVO | EX40 | 2,963 | be/29_volvo_ex40_be.yaml |
| 30 | NISSAN | JUKE | 2,752 | be/30_nissan_juke_be.yaml |

## 法国

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | PEUGEOT | 2008 | 53,922 | fr/01_peugeot_2008_fr.yaml |
| 2 | PEUGEOT | 3008 | 37,438 | fr/02_peugeot_3008_fr.yaml |
| 3 | RENAULT | CAPTUR | 37,327 | fr/03_renault_captur_fr.yaml |
| 4 | DACIA | DUSTER | 35,357 | fr/04_dacia_duster_fr.yaml |
| 5 | TOYOTA | YARIS CROSS | 30,813 | fr/05_toyota_yaris_cross_fr.yaml |
| 6 | RENAULT | SYMBIOZ | 30,023 | fr/06_renault_symbioz_fr.yaml |
| 7 | CITROEN | C5 AIRCROSS | 20,961 | fr/07_citroen_c5_aircross_fr.yaml |
| 8 | TESLA | MODEL Y | 19,170 | fr/08_tesla_model_y_fr.yaml |
| 9 | CITROEN | C3 AIRCROSS | 19,164 | fr/09_citroen_c3_aircross_fr.yaml |
| 10 | RENAULT | AUSTRAL | 18,794 | fr/10_renault_austral_fr.yaml |
| 11 | PEUGEOT | 5008 | 17,485 | fr/11_peugeot_5008_fr.yaml |
| 12 | DACIA | BIGSTER | 17,430 | fr/12_dacia_bigster_fr.yaml |
| 13 | RENAULT | SCENIC | 16,896 | fr/13_renault_scenic_fr.yaml |
| 14 | FORD | PUMA | 16,837 | fr/14_ford_puma_fr.yaml |
| 15 | VOLKSWAGEN | T-ROC | 16,476 | fr/15_volkswagen_t_roc_fr.yaml |
| 16 | TOYOTA | C-HR | 15,111 | fr/16_toyota_c_hr_fr.yaml |
| 17 | HYUNDAI | TUCSON | 14,901 | fr/17_hyundai_tucson_fr.yaml |
| 18 | VOLKSWAGEN | TIGUAN | 14,806 | fr/18_volkswagen_tiguan_fr.yaml |
| 19 | NISSAN | QASHQAI | 13,591 | fr/19_nissan_qashqai_fr.yaml |
| 20 | VOLKSWAGEN | T-CROSS | 13,124 | fr/20_volkswagen_t_cross_fr.yaml |
| 21 | MG | ZS | 12,629 | fr/21_mg_zs_fr.yaml |
| 22 | HYUNDAI | KONA | 12,285 | fr/22_hyundai_kona_fr.yaml |
| 23 | TOYOTA | AYGO X | 12,013 | fr/23_toyota_aygo_x_fr.yaml |
| 24 | FORD | KUGA | 11,709 | fr/24_ford_kuga_fr.yaml |
| 25 | BMW | X1 | 11,135 | fr/25_bmw_x1_fr.yaml |
| 26 | MERCEDES | GLA | 11,018 | fr/26_mercedes_gla_fr.yaml |
| 27 | NISSAN | JUKE | 9,936 | fr/27_nissan_juke_fr.yaml |
| 28 | KIA | SPORTAGE | 9,338 | fr/28_kia_sportage_fr.yaml |
| 29 | BMW | IX1 | 9,151 | fr/29_bmw_ix1_fr.yaml |
| 30 | SKODA | ELROQ | 8,801 | fr/30_skoda_elroq_fr.yaml |

## 波兰

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | HYUNDAI | TUCSON | 14,224 | pl/01_hyundai_tucson_pl.yaml |
| 2 | CITROEN | C5 AIRCROSS | 13,131 | pl/02_citroen_c5_aircross_pl.yaml |
| 3 | LAND ROVER | DEFENDER | 12,312 | pl/03_land_rover_defender_pl.yaml |
| 4 | LAND ROVER | RANGE ROVER EVOQUE | 10,046 | pl/04_land_rover_range_rover_evoque_pl.yaml |
| 5 | AUDI | Q3 | 9,810 | pl/05_audi_q3_pl.yaml |
| 6 | PEUGEOT | 3008 | 8,919 | pl/06_peugeot_3008_pl.yaml |
| 7 | TOYOTA | RAV4 | 8,092 | pl/07_toyota_rav4_pl.yaml |
| 8 | PEUGEOT | 5008 | 7,167 | pl/08_peugeot_5008_pl.yaml |
| 9 | HYUNDAI | KONA | 6,059 | pl/09_hyundai_kona_pl.yaml |
| 10 | VOLKSWAGEN | TIGUAN | 5,679 | pl/10_volkswagen_tiguan_pl.yaml |
| 11 | SEAT | ATECA | 5,676 | pl/11_seat_ateca_pl.yaml |
| 12 | VOLKSWAGEN | TOUAREG | 5,637 | pl/12_volkswagen_touareg_pl.yaml |
| 13 | OPEL | MOKKA | 5,507 | pl/13_opel_mokka_pl.yaml |
| 14 | TOYOTA | COROLLA CROSS | 5,423 | pl/14_toyota_corolla_cross_pl.yaml |
| 15 | JEEP | COMPASS | 5,076 | pl/15_jeep_compass_pl.yaml |
| 16 | OPEL | GRANDLAND | 4,911 | pl/16_opel_grandland_pl.yaml |
| 17 | KIA | SPORTAGE | 4,777 | pl/17_kia_sportage_pl.yaml |
| 18 | VOLKSWAGEN | TIGUAN ALLSPACE | 4,769 | pl/18_volkswagen_tiguan_allspace_pl.yaml |
| 19 | VOLKSWAGEN | T-ROC | 4,604 | pl/19_volkswagen_t_roc_pl.yaml |
| 20 | SKODA | KAMIQ | 4,577 | pl/20_skoda_kamiq_pl.yaml |
| 21 | SKODA | KAROQ | 4,566 | pl/21_skoda_karoq_pl.yaml |
| 22 | VOLVO | XC40 | 4,557 | pl/22_volvo_xc40_pl.yaml |
| 23 | AUDI | Q7 | 4,357 | pl/23_audi_q7_pl.yaml |
| 24 | VOLVO | XC60 | 4,293 | pl/24_volvo_xc60_pl.yaml |
| 25 | SUZUKI | VITARA | 4,139 | pl/25_suzuki_vitara_pl.yaml |
| 26 | HYUNDAI | SANTA FE | 3,992 | pl/26_hyundai_santa_fe_pl.yaml |
| 27 | CUPRA | FORMENTOR | 3,690 | pl/27_cupra_formentor_pl.yaml |
| 28 | PORSCHE | CAYENNE COUPE | 3,663 | pl/28_porsche_cayenne_coupe_pl.yaml |
| 29 | KIA | EV6 | 3,545 | pl/29_kia_ev6_pl.yaml |
| 30 | KIA | SORENTO | 3,267 | pl/30_kia_sorento_pl.yaml |

## 瑞典 — ✅ Dry-run 20/29 (69.0%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 2 | VOLVO | EX40 | 9,271 | se/02_volvo_ex40_se.yaml |
| 3 | TESLA | MODEL Y | 6,024 | se/03_tesla_model_y_se.yaml |
| 4 | KIA | EV3 | 5,543 | se/04_kia_ev3_se.yaml |
| 5 | VOLVO | EX30 | 5,231 | se/05_volvo_ex30_se.yaml |
| 6 | SKODA | KODIAQ | 5,154 | se/06_skoda_kodiaq_se.yaml |
| 7 | TOYOTA | RAV4 | 4,860 | se/07_toyota_rav4_se.yaml |
| 8 | POLESTAR | 4 | 4,744 | se/08_polestar_4_se.yaml |
| 9 | VOLKSWAGEN | TIGUAN | 4,603 | se/09_volkswagen_tiguan_se.yaml |
| 10 | VOLKSWAGEN | ID.4 | 4,546 | se/10_volkswagen_id_4_se.yaml |
| 11 | TOYOTA | YARIS CROSS | 3,924 | se/11_toyota_yaris_cross_se.yaml |
| 12 | VOLKSWAGEN | T-ROC | 3,651 | se/12_volkswagen_t_roc_se.yaml |
| 13 | KIA | SPORTAGE | 3,404 | se/13_kia_sportage_se.yaml |
| 14 | SKODA | ENYAQ | 3,365 | se/14_skoda_enyaq_se.yaml |
| 15 | PEUGEOT | 3008 | 2,944 | se/15_peugeot_3008_se.yaml |
| 16 | TOYOTA | COROLLA CROSS | 2,779 | se/16_toyota_corolla_cross_se.yaml |
| 17 | PEUGEOT | 2008 | 2,611 | se/17_peugeot_2008_se.yaml |
| 18 | TOYOTA | C-HR | 2,449 | se/18_toyota_c_hr_se.yaml |
| 19 | VOLVO | EC40 | 2,277 | se/19_volvo_ec40_se.yaml |
| 20 | CUPRA | TERRAMAR | 2,266 | se/20_cupra_terramar_se.yaml |
| 21 | KIA | EV9 | 2,233 | se/21_kia_ev9_se.yaml |
| 22 | PEUGEOT | 5008 | 2,181 | se/22_peugeot_5008_se.yaml |
| 23 | VOLKSWAGEN | TAYRON | 2,071 | se/23_volkswagen_tayron_se.yaml |
| 24 | VOLVO | XC40 | 2,069 | se/24_volvo_xc40_se.yaml |
| 25 | KIA | EV6 | 2,063 | se/25_kia_ev6_se.yaml |
| 26 | VOLVO | XC90 | 1,965 | se/26_volvo_xc90_se.yaml |
| 27 | BMW | IX1 | 1,949 | se/27_bmw_ix1_se.yaml |
| 28 | MERCEDES | EQA | 1,740 | se/28_mercedes_eqa_se.yaml |
| 29 | AUDI | Q4 E-TRON | 1,667 | se/29_audi_q4_e_tron_se.yaml |
| 30 | VOLVO | EX90 | 1,649 | se/30_volvo_ex90_se.yaml |

## 瑞士 — ✅ Dry-run 13/30 (43.3%)

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | VOLKSWAGEN | TIGUAN | 5,084 | ch/01_volkswagen_tiguan_ch.yaml |
| 2 | TESLA | MODEL Y | 4,991 | ch/02_tesla_model_y_ch.yaml |
| 3 | SKODA | KODIAQ | 3,857 | ch/03_skoda_kodiaq_ch.yaml |
| 4 | SKODA | ELROQ | 3,646 | ch/04_skoda_elroq_ch.yaml |
| 5 | MERCEDES | GLC | 3,533 | ch/05_mercedes_glc_ch.yaml |
| 6 | BMW | X1 | 3,465 | ch/06_bmw_x1_ch.yaml |
| 7 | SKODA | KAROQ | 3,390 | ch/07_skoda_karoq_ch.yaml |
| 8 | BMW | X3 | 2,839 | ch/08_bmw_x3_ch.yaml |
| 9 | SKODA | ENYAQ | 2,785 | ch/09_skoda_enyaq_ch.yaml |
| 10 | VOLVO | XC60 | 2,647 | ch/10_volvo_xc60_ch.yaml |
| 11 | DACIA | DUSTER | 2,274 | ch/11_dacia_duster_ch.yaml |
| 12 | VOLVO | EX30 | 2,178 | ch/12_volvo_ex30_ch.yaml |
| 13 | KIA | SPORTAGE | 2,104 | ch/13_kia_sportage_ch.yaml |
| 14 | CUPRA | TERRAMAR | 2,097 | ch/14_cupra_terramar_ch.yaml |
| 15 | MERCEDES | GLA | 2,054 | ch/15_mercedes_gla_ch.yaml |
| 16 | HYUNDAI | TUCSON | 2,005 | ch/16_hyundai_tucson_ch.yaml |
| 17 | AUDI | Q3 SPORTBACK | 1,962 | ch/17_audi_q3_sportback_ch.yaml |
| 18 | BMW | X5 | 1,962 | ch/18_bmw_x5_ch.yaml |
| 19 | DACIA | BIGSTER | 1,912 | ch/19_dacia_bigster_ch.yaml |
| 20 | TOYOTA | YARIS CROSS | 1,911 | ch/20_toyota_yaris_cross_ch.yaml |
| 21 | VOLKSWAGEN | T-ROC | 1,771 | ch/21_volkswagen_t_roc_ch.yaml |
| 22 | AUDI | Q5 | 1,638 | ch/22_audi_q5_ch.yaml |
| 23 | AUDI | Q4 E-TRON | 1,577 | ch/23_audi_q4_e_tron_ch.yaml |
| 24 | BMW | IX1 | 1,566 | ch/24_bmw_ix1_ch.yaml |
| 25 | AUDI | Q3 | 1,513 | ch/25_audi_q3_ch.yaml |
| 26 | MERCEDES | GLE | 1,482 | ch/26_mercedes_gle_ch.yaml |
| 27 | HYUNDAI | KONA | 1,397 | ch/27_hyundai_kona_ch.yaml |
| 28 | VOLKSWAGEN | TAYRON | 1,380 | ch/28_volkswagen_tayron_ch.yaml |
| 29 | CUPRA | FORMENTOR | 1,351 | ch/29_cupra_formentor_ch.yaml |
| 30 | TOYOTA | RAV4 | 1,332 | ch/30_toyota_rav4_ch.yaml |

## 罗马尼亚

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | DACIA | DUSTER | 13,597 | ro/01_dacia_duster_ro.yaml |
| 2 | DACIA | BIGSTER | 3,737 | ro/02_dacia_bigster_ro.yaml |
| 3 | HYUNDAI | TUCSON | 3,591 | ro/03_hyundai_tucson_ro.yaml |
| 4 | TOYOTA | RAV4 | 2,956 | ro/04_toyota_rav4_ro.yaml |
| 5 | TOYOTA | YARIS CROSS | 2,925 | ro/05_toyota_yaris_cross_ro.yaml |
| 6 | FORD | KUGA | 2,728 | ro/06_ford_kuga_ro.yaml |
| 7 | FORD | PUMA | 2,222 | ro/07_ford_puma_ro.yaml |
| 8 | HYUNDAI | KONA | 2,003 | ro/08_hyundai_kona_ro.yaml |
| 9 | VOLKSWAGEN | TIGUAN | 1,889 | ro/09_volkswagen_tiguan_ro.yaml |
| 10 | KIA | SPORTAGE | 1,650 | ro/10_kia_sportage_ro.yaml |
| 11 | TOYOTA | C-HR | 1,611 | ro/11_toyota_c_hr_ro.yaml |
| 12 | SUZUKI | VITARA | 1,610 | ro/12_suzuki_vitara_ro.yaml |
| 13 | SUZUKI | S-CROSS | 1,552 | ro/13_suzuki_s_cross_ro.yaml |
| 14 | RENAULT | CAPTUR | 1,516 | ro/14_renault_captur_ro.yaml |
| 15 | MG | ZS | 1,504 | ro/15_mg_zs_ro.yaml |
| 16 | SKODA | KODIAQ | 1,436 | ro/16_skoda_kodiaq_ro.yaml |
| 17 | VOLKSWAGEN | TAIGO | 1,424 | ro/17_volkswagen_taigo_ro.yaml |
| 18 | TOYOTA | COROLLA CROSS | 1,358 | ro/18_toyota_corolla_cross_ro.yaml |
| 19 | KGM | KORANDO | 1,324 | ro/19_kgm_korando_ro.yaml |
| 20 | MG | HS | 1,242 | ro/20_mg_hs_ro.yaml |
| 21 | RENAULT | ARKANA | 1,024 | ro/21_renault_arkana_ro.yaml |
| 22 | SKODA | KAMIQ | 938 | ro/22_skoda_kamiq_ro.yaml |
| 23 | RENAULT | AUSTRAL | 895 | ro/23_renault_austral_ro.yaml |
| 24 | SKODA | KAROQ | 883 | ro/24_skoda_karoq_ro.yaml |
| 25 | MAZDA | CX-30 | 856 | ro/25_mazda_cx_30_ro.yaml |
| 26 | VOLKSWAGEN | TOUAREG | 776 | ro/26_volkswagen_touareg_ro.yaml |
| 27 | VOLKSWAGEN | T-CROSS | 745 | ro/27_volkswagen_t_cross_ro.yaml |
| 28 | PEUGEOT | 2008 | 684 | ro/28_peugeot_2008_ro.yaml |
| 29 | PEUGEOT | 3008 | 681 | ro/29_peugeot_3008_ro.yaml |
| 30 | BMW | X5 | 674 | ro/30_bmw_x5_ro.yaml |

## 芬兰

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | TOYOTA | YARIS CROSS | 2,865 | fi/01_toyota_yaris_cross_fi.yaml |
| 2 | TESLA | MODEL Y | 2,051 | fi/02_tesla_model_y_fi.yaml |
| 3 | VOLKSWAGEN | ID.4 | 1,771 | fi/03_volkswagen_id_4_fi.yaml |
| 4 | TOYOTA | RAV4 | 1,714 | fi/04_toyota_rav4_fi.yaml |
| 5 | VOLVO | XC60 | 1,710 | fi/05_volvo_xc60_fi.yaml |
| 6 | SKODA | ELROQ | 1,629 | fi/06_skoda_elroq_fi.yaml |
| 7 | SKODA | ENYAQ | 1,447 | fi/07_skoda_enyaq_fi.yaml |
| 8 | NISSAN | QASHQAI | 1,437 | fi/08_nissan_qashqai_fi.yaml |
| 9 | TOYOTA | C-HR | 1,392 | fi/09_toyota_c_hr_fi.yaml |
| 10 | KIA | EV3 | 1,050 | fi/10_kia_ev3_fi.yaml |
| 11 | VOLVO | EX40 | 1,012 | fi/11_volvo_ex40_fi.yaml |
| 12 | VOLKSWAGEN | TIGUAN | 853 | fi/12_volkswagen_tiguan_fi.yaml |
| 13 | KIA | STONIC | 790 | fi/13_kia_stonic_fi.yaml |
| 14 | POLESTAR | 4 | 776 | fi/14_polestar_4_fi.yaml |
| 15 | VOLKSWAGEN | T-CROSS | 744 | fi/15_volkswagen_t_cross_fi.yaml |
| 16 | VOLKSWAGEN | TAIGO | 662 | fi/16_volkswagen_taigo_fi.yaml |
| 17 | AUDI | Q4 E-TRON | 633 | fi/17_audi_q4_e_tron_fi.yaml |
| 18 | SKODA | KODIAQ | 626 | fi/18_skoda_kodiaq_fi.yaml |
| 19 | VOLVO | EX30 | 616 | fi/19_volvo_ex30_fi.yaml |
| 20 | BMW | X1 | 600 | fi/20_bmw_x1_fi.yaml |
| 21 | TOYOTA | BZ4X | 594 | fi/21_toyota_bz4x_fi.yaml |
| 22 | KIA | NIRO | 578 | fi/22_kia_niro_fi.yaml |
| 23 | FORD | EXPLORER EV | 514 | fi/23_ford_explorer_ev_fi.yaml |
| 24 | SKODA | KAMIQ | 508 | fi/24_skoda_kamiq_fi.yaml |
| 25 | AUDI | Q6 E-TRON | 498 | fi/25_audi_q6_e_tron_fi.yaml |
| 26 | MERCEDES | GLC | 481 | fi/26_mercedes_glc_fi.yaml |
| 27 | VOLVO | EC40 | 475 | fi/27_volvo_ec40_fi.yaml |
| 28 | BMW | IX1 | 456 | fi/28_bmw_ix1_fi.yaml |
| 29 | MERCEDES | EQA | 425 | fi/29_mercedes_eqa_fi.yaml |
| 30 | MERCEDES | EQB | 401 | fi/30_mercedes_eqb_fi.yaml |

## 荷兰

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | SKODA | ELROQ | 12,085 | nl/01_skoda_elroq_nl.yaml |
| 2 | TESLA | MODEL Y | 10,600 | nl/02_tesla_model_y_nl.yaml |
| 3 | KIA | EV3 | 9,528 | nl/03_kia_ev3_nl.yaml |
| 4 | TOYOTA | AYGO X | 7,029 | nl/04_toyota_aygo_x_nl.yaml |
| 5 | TOYOTA | YARIS CROSS | 6,729 | nl/05_toyota_yaris_cross_nl.yaml |
| 6 | KIA | NIRO | 6,091 | nl/06_kia_niro_nl.yaml |
| 7 | VOLKSWAGEN | TIGUAN | 5,212 | nl/07_volkswagen_tiguan_nl.yaml |
| 8 | FORD | KUGA | 5,164 | nl/08_ford_kuga_nl.yaml |
| 9 | HYUNDAI | KONA | 5,011 | nl/09_hyundai_kona_nl.yaml |
| 10 | SKODA | KODIAQ | 4,971 | nl/10_skoda_kodiaq_nl.yaml |
| 11 | VOLVO | EX30 | 4,820 | nl/11_volvo_ex30_nl.yaml |
| 12 | BMW | IX1 | 4,171 | nl/12_bmw_ix1_nl.yaml |
| 13 | VOLVO | EX40 | 4,016 | nl/13_volvo_ex40_nl.yaml |
| 14 | RENAULT | CAPTUR | 3,752 | nl/14_renault_captur_nl.yaml |
| 15 | VOLKSWAGEN | ID.4 | 3,619 | nl/15_volkswagen_id_4_nl.yaml |
| 16 | HYUNDAI | INSTER | 3,552 | nl/16_hyundai_inster_nl.yaml |
| 17 | FORD | PUMA | 3,379 | nl/17_ford_puma_nl.yaml |
| 18 | AUDI | Q4 E-TRON | 3,244 | nl/18_audi_q4_e_tron_nl.yaml |
| 19 | KIA | SPORTAGE | 3,219 | nl/19_kia_sportage_nl.yaml |
| 20 | PEUGEOT | 2008 | 3,216 | nl/20_peugeot_2008_nl.yaml |
| 21 | VOLVO | XC60 | 3,137 | nl/21_volvo_xc60_nl.yaml |
| 22 | RENAULT | SCENIC | 3,027 | nl/22_renault_scenic_nl.yaml |
| 23 | FORD | EXPLORER EV | 2,906 | nl/23_ford_explorer_ev_nl.yaml |
| 24 | SKODA | ENYAQ | 2,876 | nl/24_skoda_enyaq_nl.yaml |
| 25 | BYD | SEAL U | 2,600 | nl/25_byd_seal_u_nl.yaml |
| 26 | RENAULT | SYMBIOZ | 2,505 | nl/26_renault_symbioz_nl.yaml |
| 27 | PEUGEOT | 3008 | 2,444 | nl/27_peugeot_3008_nl.yaml |
| 28 | KIA | STONIC | 2,372 | nl/28_kia_stonic_nl.yaml |
| 29 | TOYOTA | C-HR | 2,351 | nl/29_toyota_c_hr_nl.yaml |
| 30 | VOLKSWAGEN | TAYRON | 2,303 | nl/30_volkswagen_tayron_nl.yaml |

## 葡萄牙

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | RENAULT | ARKANA | 5,937 | pt/01_renault_arkana_pt.yaml |
| 2 | PEUGEOT | 2008 | 3,376 | pt/02_peugeot_2008_pt.yaml |
| 3 | MERCEDES | EQE | 3,211 | pt/03_mercedes_eqe_pt.yaml |
| 4 | DACIA | DUSTER | 3,182 | pt/04_dacia_duster_pt.yaml |
| 5 | FORD | PUMA | 3,027 | pt/05_ford_puma_pt.yaml |
| 6 | HYUNDAI | TUCSON | 2,664 | pt/06_hyundai_tucson_pt.yaml |
| 7 | NISSAN | JUKE | 2,659 | pt/07_nissan_juke_pt.yaml |
| 8 | ALFA ROMEO | TONALE | 2,591 | pt/08_alfa_romeo_tonale_pt.yaml |
| 9 | AUDI | Q2 | 2,395 | pt/09_audi_q2_pt.yaml |
| 10 | VOLKSWAGEN | TIGUAN | 2,380 | pt/10_volkswagen_tiguan_pt.yaml |
| 11 | CITROEN | C5 AIRCROSS | 2,336 | pt/11_citroen_c5_aircross_pt.yaml |
| 12 | RENAULT | CAPTUR | 2,148 | pt/12_renault_captur_pt.yaml |
| 13 | HYUNDAI | KAUAI | 2,139 | pt/13_hyundai_kauai_pt.yaml |
| 14 | PEUGEOT | 3008 | 1,964 | pt/14_peugeot_3008_pt.yaml |
| 15 | VOLVO | EX30 | 1,843 | pt/15_volvo_ex30_pt.yaml |
| 16 | VOLVO | XC40 | 1,586 | pt/16_volvo_xc40_pt.yaml |
| 17 | JEEP | RENEGADE | 1,461 | pt/17_jeep_renegade_pt.yaml |
| 18 | OPEL | GRANDLAND | 1,435 | pt/18_opel_grandland_pt.yaml |
| 19 | MERCEDES | GLE | 1,321 | pt/19_mercedes_gle_pt.yaml |
| 20 | PEUGEOT | 5008 | 1,307 | pt/20_peugeot_5008_pt.yaml |
| 21 | TOYOTA | COROLLA CROSS | 1,208 | pt/21_toyota_corolla_cross_pt.yaml |
| 22 | KIA | STONIC | 1,185 | pt/22_kia_stonic_pt.yaml |
| 23 | MG | HS | 1,156 | pt/23_mg_hs_pt.yaml |
| 24 | SKODA | KAROQ | 1,142 | pt/24_skoda_karoq_pt.yaml |
| 25 | CITROEN | C3 AIRCROSS | 1,117 | pt/25_citroen_c3_aircross_pt.yaml |
| 26 | VOLKSWAGEN | TAIGO | 1,103 | pt/26_volkswagen_taigo_pt.yaml |
| 27 | AUDI | Q4 SPORTBACK E-TRON | 1,099 | pt/27_audi_q4_sportback_e_tron_pt.yaml |
| 28 | TOYOTA | C-HR | 1,093 | pt/28_toyota_c_hr_pt.yaml |
| 29 | SKODA | KAMIQ | 1,019 | pt/29_skoda_kamiq_pt.yaml |
| 30 | DS | DS 7 | 1,018 | pt/30_ds_ds_7_pt.yaml |

## 西班牙

| Rank | Brand | Model | Sales 12M | Draft Path |
| ---: | --- | --- | ---: | --- |
| 1 | MG | ZS | 22,978 | es/01_mg_zs_es.yaml |
| 2 | SEAT | ARONA | 21,009 | es/02_seat_arona_es.yaml |
| 3 | PEUGEOT | 2008 | 20,770 | es/03_peugeot_2008_es.yaml |
| 4 | HYUNDAI | TUCSON | 20,631 | es/04_hyundai_tucson_es.yaml |
| 5 | TOYOTA | C-HR | 19,517 | es/05_toyota_c_hr_es.yaml |
| 6 | NISSAN | QASHQAI | 19,327 | es/06_nissan_qashqai_es.yaml |
| 7 | TOYOTA | YARIS CROSS | 18,930 | es/07_toyota_yaris_cross_es.yaml |
| 8 | VOLKSWAGEN | T-ROC | 18,717 | es/08_volkswagen_t_roc_es.yaml |
| 9 | RENAULT | CAPTUR | 18,431 | es/09_renault_captur_es.yaml |
| 10 | KIA | SPORTAGE | 15,346 | es/10_kia_sportage_es.yaml |
| 11 | HYUNDAI | KONA | 15,067 | es/11_hyundai_kona_es.yaml |
| 12 | VOLKSWAGEN | TAIGO | 14,484 | es/12_volkswagen_taigo_es.yaml |
| 13 | KIA | STONIC | 14,151 | es/13_kia_stonic_es.yaml |
| 14 | DACIA | DUSTER | 14,107 | es/14_dacia_duster_es.yaml |
| 15 | VOLKSWAGEN | TIGUAN | 13,695 | es/15_volkswagen_tiguan_es.yaml |
| 16 | TOYOTA | RAV4 | 13,016 | es/16_toyota_rav4_es.yaml |
| 17 | BMW | X1 | 12,273 | es/17_bmw_x1_es.yaml |
| 18 | VOLKSWAGEN | T-CROSS | 11,645 | es/18_volkswagen_t_cross_es.yaml |
| 19 | RENAULT | AUSTRAL | 11,439 | es/19_renault_austral_es.yaml |
| 20 | SKODA | KAMIQ | 11,235 | es/20_skoda_kamiq_es.yaml |
| 21 | KIA | NIRO | 10,794 | es/21_kia_niro_es.yaml |
| 22 | MG | HS | 10,787 | es/22_mg_hs_es.yaml |
| 23 | BYD | SEAL U | 10,658 | es/23_byd_seal_u_es.yaml |
| 24 | FORD | PUMA | 10,414 | es/24_ford_puma_es.yaml |
| 25 | FORD | KUGA | 10,385 | es/25_ford_kuga_es.yaml |
| 26 | OMODA | 5 | 10,067 | es/26_omoda_5_es.yaml |
| 27 | CUPRA | FORMENTOR | 10,040 | es/27_cupra_formentor_es.yaml |
| 28 | NISSAN | JUKE | 9,668 | es/28_nissan_juke_es.yaml |
| 29 | MERCEDES | GLA | 9,167 | es/29_mercedes_gla_es.yaml |
| 30 | JAECOO | 7 | 8,571 | es/30_jaecoo_7_es.yaml |

## Notes

- These are draft scaffolds. Promote a draft into `07_ScrapingToolkit/sources/` only after dry-run verification passes.
- Draft files are grouped by country and mirror the per-country top_n candidate ranking from the report.
- Batch 1+2（SE/HR/HU/NO/AT/CZ/CH）keyword filling 已完成，dry-run 结果见上方进度表。
- 详细执行计划见 `Markdown_Readme/Fullstack/MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md`。
