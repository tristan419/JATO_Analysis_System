import re

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/backend/app/services/market_scan_service.py', 'r', encoding='utf8') as f:
    content = f.read()

target = '''        items.append(
            {
                "model": str(model),
                "volume": current_volume,
                "sharePct": _safe_share(current_volume, segment_total),
                "shareDisplay": f"{_safe_share(current_volume, segment_total) * 100:.1f}%",
                "yoy": _delta_payload(current_volume, prior_volume),
            }
        )'''

replacement = '''        
        current_4wd_volume = float(_series_sum(group[group["__drive_type"] == "4WD"], current_columns).sum())
        current_2wd_volume = float(_series_sum(group[group["__drive_type"] == "2WD"], current_columns).sum())
        current_other_volume = float(_series_sum(group[group["__drive_type"] == "OTHER"], current_columns).sum())
        
        drive_share_pct = float(current_4wd_volume / current_volume) if current_volume > 0 else 0.0
        
        items.append(
            {
                "model": str(model),
                "volume": current_volume,
                "sharePct": _safe_share(current_volume, segment_total),
                "shareDisplay": f"{_safe_share(current_volume, segment_total) * 100:.1f}%",
                "yoy": _delta_payload(current_volume, prior_volume),
                "driveSharePct": drive_share_pct,
                "driveMix": {
                    "4WD": current_4wd_volume,
                    "2WD": current_2wd_volume,
                    "OTHER": current_other_volume,
                }
            }
        )'''

new_content = content.replace(target, replacement)
if new_content == content:
    print("Failed to replace!")
else:
    with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/backend/app/services/market_scan_service.py', 'w', encoding='utf8') as f:
        f.write(new_content)
    print("Replaced successfully!")
