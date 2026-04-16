with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/index.css', 'r', encoding='utf8') as f:
    c = f.read()

target = """.market-scan-ranking-scrollable {
  max-height: 520px;
  overflow-y: auto;
  scrollbar-width: thin;
}"""

replacement = """.market-scan-ranking-scrollable {
  /* removed nested scroll to prevent double scrollbar confusion */
}"""

c = c.replace(target, replacement)

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/index.css', 'w', encoding='utf8') as f:
    f.write(c)
print("Done CSS")
