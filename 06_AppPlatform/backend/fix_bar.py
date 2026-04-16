with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/pages/MarketScanPage.tsx', 'r', encoding='utf8') as f:
    c = f.read()

target = "span style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor, position: 'relative', overflow: 'hidden' }}"
replacement = "span style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor, overflow: 'hidden' }}"

c = c.replace(target, replacement)

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/pages/MarketScanPage.tsx', 'w', encoding='utf8') as f:
    f.write(c)
print("Done")
