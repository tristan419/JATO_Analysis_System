with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/pages/MarketScanPage.tsx', 'r', encoding='utf8') as f:
    c = f.read()

target = """                {item.mom ? (
                  <span className={`market-scan-tone-text ${toneClassName(item.mom.tone)}`}>
                    (环 {item.mom.tone === "positive" ? "▲ " : item.mom.tone === "negative" ? "▼ " : ""}{item.mom.display})
                  </span>
                          {item.mom.display})
                  </span>
                ) : null}"""

replacement = """                {item.mom ? (
                  <span className={`market-scan-tone-text ${toneClassName(item.mom.tone)}`}>
                    (环 {item.mom.tone === "positive" ? "▲ " : item.mom.tone === "negative" ? "▼ " : ""}{item.mom.display})
                  </span>
                ) : null}"""

c = c.replace(target, replacement)

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/pages/MarketScanPage.tsx', 'w', encoding='utf8') as f:
    f.write(c)
print("Done")
