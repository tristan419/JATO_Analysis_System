import re

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/pages/MarketScanPage.tsx', 'r', encoding='utf8') as f:
    content = f.read()

target = '''                          {item.mom.display})
                  </span>
                ) : null}
              </div>
            </div>
            <div className="market-scan-ranking-row-bar">
              <span style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor }} />
            </div>
          </article>'''

replacement = '''                          {item.mom.display})
                  </span>
                ) : null}
              </div>
            </div>
            <div className="market-scan-ranking-row-bar" title={item.driveSharePct ? `4WD Share: ${(item.driveSharePct * 100).toFixed(1)}%` : undefined}>
              <span style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor, position: 'relative', overflow: 'hidden' }}>
                {(item.driveSharePct && item.driveSharePct > 0) ? (
                  <span
                    className="market-scan-4wd-fill"
                    style={{
                      position: 'absolute',
                      top: 0,
                      left: 0,
                      height: '100%',
                      width: `${item.driveSharePct * 100}%`,
                    }}
                  />
                ) : null}
              </span>
            </div>
          </article>'''

new_content = content.replace(target, replacement)

# Do the same for non-compact ranking
target2 = '''                ) : null}
              </div>
            </div>
            <div className="market-scan-ranking-row-bar">
              <span style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor }} />
            </div>
          </article>'''
# Actually non-compact uses the exact same layout below. So we can just `.replace(target, replacement)` again.
new_content2 = new_content.replace(target2, replacement)

# We also add the 4WD text to the row.
target_nums = '''                <span className="market-scan-ranking-row-nums">
                  {formatVolume(item.volume)} 台
                  {item.shareDisplay ? ` · ${item.shareDisplay}` : ""}
                </span>'''

replacement_nums = '''                <span className="market-scan-ranking-row-nums" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
                  <span>{formatVolume(item.volume)} 台</span>
                  {item.shareDisplay ? <span className="market-scan-tag">{item.shareDisplay} 份额</span> : null}
                  {(item.driveSharePct && item.driveSharePct > 0) ? <span className="market-scan-tag-animated">4WD {(item.driveSharePct * 100).toFixed(1)}%</span> : null}
                </span>'''

new_content3 = new_content2.replace(target_nums, replacement_nums)

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/frontend/src/pages/MarketScanPage.tsx', 'w', encoding='utf8') as f:
    f.write(new_content3)
