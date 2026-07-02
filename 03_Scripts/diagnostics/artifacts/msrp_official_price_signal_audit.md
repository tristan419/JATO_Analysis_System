# MSRP Official Price Signal Audit

Generated: 2026-06-22T05:34:05Z
Backlog run: msrp-dryrun-20260618-110029
Policy: price signals create dryrun candidates only; official ingest still requires validation/review

| Metric | Value |
|---|---:|
| Inspected sources | 13 |
| Candidate URLs | 14 |
| Dryrun candidate eligible | 8 |
| Official ingest eligible | 0 |
| No price signal | 0 |
| Access blocked | 2 |

| Country | Source | Candidate | HTTP | Signal | Recommended action | URL |
|---|---|---|---:|---|---|---|
| AT | `mg_zs_at_draft_scrapling` | registered_source | - | tls_handshake_failed | try_official_alternative_url_or_proxy | https://www.mgmotor.at/modelle/mg-zs |
| AT | `mg_zs_at_draft_scrapling` | candidate_url | 200 | campaign_or_net_price_signal | route_to_campaign_or_net_price_pipeline_not_base_msrp | https://www.mgmotor.eu/de-AT/configurator/zs-hev |
| AT | `tesla_model_y_at_draft_scrapling` | registered_source | 403 | access_blocked | official_proxy_or_configurator_api | https://www.tesla.com/de_at/modely |
| AT | `peugeot_3008_at_draft_scrapling` | registered_source | 403 | access_blocked | official_proxy_or_configurator_api | https://www.peugeot.at/unsere-modelle/neuer-3008.html |
| AT | `skoda_elroq_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.skoda.at/elroq/elroq/overlay-elroq-preisliste |
| AT | `skoda_enyaq_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.skoda.at/enyaq/enyaq/overlay-enyaq-preisliste |
| AT | `skoda_kamiq_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.skoda.at/kamiq/kamiq/overlay-kamiq-preise-technische-daten |
| AT | `skoda_karoq_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.skoda.at/karoq/karoq/overlay-karoq-preise-technische-daten |
| AT | `skoda_kodiaq_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.skoda.at/kodiaq/kodiaq/overlay-kodiaq-preise-technische-daten |
| AT | `volkswagen_id_4_at_draft_scrapling` | registered_source | 200 | campaign_or_net_price_signal | route_to_campaign_or_net_price_pipeline_not_base_msrp | https://www.volkswagen.at/id4/id4 |
| AT | `volkswagen_t_cross_at_draft_scrapling` | registered_source | 200 | campaign_or_net_price_signal | route_to_campaign_or_net_price_pipeline_not_base_msrp | https://www.volkswagen.at/t-cross/t-cross |
| AT | `seat_arona_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.seat.at/angebote-und-produkte/kataloge-preislisten/arona |
| AT | `seat_ateca_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.seat.at/angebote-und-produkte/kataloge-preislisten/ateca |
| AT | `toyota_yaris_cross_at_draft_scrapling` | registered_source | 200 | price_signal_present | repair_selector_and_run_dryrun | https://www.toyota.at/neuwagen/brochures |
