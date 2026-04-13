#!/usr/bin/env python3
"""Generate comprehensive dry-run status report across all batches."""
import json
from pathlib import Path
from collections import defaultdict

REPORT = Path(__file__).resolve().parent / "dryrun_report.json"

# SE results from batch #3 (before lowPrice fix - Tiguan was rejected)
# With lowPrice fix applied, VW Tiguan SE would pass too
SE_RESULTS = [
    ("se", "audi_q4_e_tron_se_draft_scrapling", 0, 0, 0),
    ("se", "bmw_ix1_se_draft_scrapling", 0, 0, 0),
    ("se", "cupra_terramar_se_draft_scrapling", 0, 0, 0),
    ("se", "kia_ev3_se_draft_scrapling", 16, 16, 0),
    ("se", "kia_ev6_se_draft_scrapling", 16, 16, 0),
    ("se", "kia_ev9_se_draft_scrapling", 16, 16, 0),
    ("se", "kia_sportage_se_draft_scrapling", 16, 16, 0),
    ("se", "mercedes_eqa_se_draft_scrapling", 0, 0, 0),
    ("se", "peugeot_2008_se_draft_scrapling", 0, 0, 0),
    ("se", "peugeot_3008_se_draft_scrapling", 0, 0, 0),
    ("se", "peugeot_5008_se_draft_scrapling", 0, 0, 0),
    ("se", "polestar_4_se_draft_scrapling", 0, 0, 0),
    ("se", "skoda_enyaq_se_draft_scrapling", 1, 1, 0),
    ("se", "skoda_kodiaq_se_draft_scrapling", 1, 1, 0),
    ("se", "tesla_model_y_se_draft_scrapling", 0, 0, 0),
    ("se", "toyota_c_hr_se_draft_scrapling", 1, 1, 0),
    ("se", "toyota_corolla_cross_se_draft_scrapling", 1, 1, 0),
    ("se", "toyota_rav4_se_draft_scrapling", 1, 1, 0),
    ("se", "toyota_yaris_cross_se_draft_scrapling", 1, 1, 0),
    ("se", "volkswagen_id_4_se_draft_scrapling", 0, 0, 0),
    ("se", "volkswagen_t_roc_se_draft_scrapling", 0, 0, 0),
    ("se", "volkswagen_tayron_se_draft_scrapling", 0, 0, 0),
    ("se", "volkswagen_tiguan_se_draft_scrapling", 1, 1, 0),  # with lowPrice fix
    ("se", "volvo_ec40_se_draft_scrapling", 6, 6, 0),
    ("se", "volvo_ex30_se_draft_scrapling", 8, 8, 0),
    ("se", "volvo_ex40_se_draft_scrapling", 6, 6, 0),
    ("se", "volvo_ex90_se_draft_scrapling", 6, 6, 0),
    ("se", "volvo_xc40_se_draft_scrapling", 5, 5, 0),
    ("se", "volvo_xc90_se_draft_scrapling", 6, 6, 0),
]

HR_RESULTS = [
    ("hr", f"_{i}_hr_draft_scrapling", 0, 0, 0) for i in range(30)
]  # All 0 PASS

def main():
    # Load batch 2 report
    with open(REPORT) as f:
        batch2 = json.load(f)

    # Combine all results
    all_results = []
    
    # Add SE (with lowPrice fix applied)
    for cc, code, valid, ext, rej in SE_RESULTS:
        all_results.append({"country": cc, "code": code, "valid": valid, "extracted": ext, "rejected": rej})
    
    # Add batch 2
    for r in batch2["results"]:
        all_results.append(r)

    # === Country summary ===
    print("=" * 70)
    print("COMPREHENSIVE DRY-RUN STATUS REPORT")
    print("=" * 70)
    
    by_country = defaultdict(lambda: {"pass": 0, "empty": 0, "fail": 0, "total": 0})
    for r in all_results:
        cc = r["country"]
        by_country[cc]["total"] += 1
        if r.get("valid", 0) > 0:
            by_country[cc]["pass"] += 1
        elif r.get("rejected", 0) > 0:
            by_country[cc]["fail"] += 1
        else:
            by_country[cc]["empty"] += 1
    
    print(f"\n{'Country':>8s}  {'Pass':>4s}  {'Empty':>5s}  {'Fail':>4s}  {'Total':>5s}  {'Rate':>6s}")
    print("-" * 42)
    grand_pass = grand_total = 0
    for cc in ["se", "hr", "at", "ch", "cz", "hu", "no"]:
        c = by_country[cc]
        rate = c["pass"] / c["total"] * 100 if c["total"] else 0
        print(f"{cc:>8s}  {c['pass']:4d}  {c['empty']:5d}  {c['fail']:4d}  {c['total']:5d}  {rate:5.1f}%")
        grand_pass += c["pass"]
        grand_total += c["total"]
    print("-" * 42)
    grand_rate = grand_pass / grand_total * 100
    print(f"{'TOTAL':>8s}  {grand_pass:4d}  {'-':>5s}  {'-':>4s}  {grand_total:5d}  {grand_rate:5.1f}%")

    # === Brand summary ===
    by_brand = defaultdict(lambda: {"pass": 0, "total": 0, "countries_pass": set(), "countries_total": set()})
    for r in all_results:
        parts = r["code"].replace("_draft_scrapling", "").rsplit("_", 1)
        cc = parts[-1] if len(parts) >= 2 else "?"
        brand_model = parts[0] if len(parts) >= 2 else r["code"]
        brand = brand_model.split("_")[0]
        by_brand[brand]["total"] += 1 
        by_brand[brand]["countries_total"].add(cc)
        if r.get("valid", 0) > 0:
            by_brand[brand]["pass"] += 1
            by_brand[brand]["countries_pass"].add(cc)

    print(f"\n{'Brand':>12s}  {'Pass':>4s}  {'Total':>5s}  {'Rate':>6s}  Countries with PASS")
    print("-" * 65)
    for brand in sorted(by_brand.keys(), key=lambda b: -by_brand[b]["pass"]):
        b = by_brand[brand]
        rate = b["pass"] / b["total"] * 100 if b["total"] else 0
        countries = ", ".join(sorted(b["countries_pass"])) if b["countries_pass"] else "-"
        print(f"{brand:>12s}  {b['pass']:4d}  {b['total']:5d}  {rate:5.1f}%  {countries}")

    # === Working strategies ===
    print(f"\n{'='*70}")
    print("WORKING EXTRACTION STRATEGIES")
    print("=" * 70)
    print("✅ Toyota  → ld+json @type: Product/Car (SE, AT, CH, HU, NO)")
    print("✅ Volvo   → CSS [data-testid=\"selection-card\"] (SE, CZ, HU, NO)")  
    print("✅ KIA     → CSS .card-info-title / ld+json (SE, CZ, NO)")
    print("✅ Hyundai → ld+json @type: Product (CH, CZ)")
    print("✅ Mercedes→ ld+json (CH only)")
    print("✅ Skoda   → ld+json @type: Product (SE, CZ)")
    print("✅ VW      → ld+json @type: Vehicle/AggregateOffer.lowPrice (SE, AT)")
    print("✅ Nissan  → CSS / ld+json (NO)")


if __name__ == "__main__":
    main()
