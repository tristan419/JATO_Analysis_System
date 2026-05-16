#!/usr/bin/env python3
"""Run anytime Mega Menu/Dashboard fixes get reverted. Usage: python3 fix_all.py"""
import os

ROOT = os.path.dirname(os.path.abspath(__file__))
F = os.path.join(ROOT, '06_AppPlatform/frontend/src')
B = os.path.join(ROOT, '06_AppPlatform/backend/app')

def fix(path, old, new, desc):
    with open(path) as f: c = f.read()
    if old in c:
        c = c.replace(old, new)
        with open(path, 'w') as f: f.write(c)
        print(f'  OK {desc}')

def writef(path, content, desc):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f: f.write(content)
    print(f'  OK {desc}')

# === Backend ===
ms = os.path.join(B, 'services/market_scan_service.py')
with open(ms) as f: c = f.read()
if 'logger = logging.getLogger' not in c:
    c = c.replace('from __future__ import annotations\n\nimport re',
                  'from __future__ import annotations\n\nimport logging\nimport re')
    c = c.replace('\n_DECK_CACHE_TTL_SECONDS',
                  '\nlogger = logging.getLogger(__name__)\n\n_DECK_CACHE_TTL_SECONDS')
    print('  OK market_scan: logger')

if '_get_redis_client_safe' not in c:
    stub_block = '''_deck_cache_lock = threading.Lock()

def _get_redis_client_safe():
    return None
def _build_deck_cache_key(*_a, **_kw):
    return ""
def _get_cached_deck(_c, _k):
    return None
def _acquire_compute_lock(_c, _k):
    return True
def _wait_for_cache(_c, _k):
    return None
def _set_cached_deck(_c, _k, _r):
    pass
def _release_compute_lock(_c, _k):
    pass'''
    c = c.replace('_deck_cache_lock = threading.Lock()', stub_block)
    for old_n, new_n in [
        ('get_redis_client()', '_get_redis_client_safe()'),
        ('build_deck_cache_key(', '_build_deck_cache_key('),
        ('get_cached_deck(', '_get_cached_deck('),
        ('acquire_compute_lock(', '_acquire_compute_lock('),
        ('wait_for_cache(', '_wait_for_cache('),
        ('set_cached_deck(', '_set_cached_deck('),
        ('release_compute_lock(', '_release_compute_lock('),
    ]:
        c = c.replace(old_n, new_n)
    print('  OK market_scan: redis stubs')

with open(ms, 'w') as f: f.write(c)

fix(os.path.join(B, 'core/config.py'),
    '_parse_bool_env("APP_AUTH_ENABLED", True)',
    '_parse_bool_env("APP_AUTH_ENABLED", False)',
    'AUTH_ENABLED=False')

# === Frontend ===
writef(os.path.join(F, 'components/Layout.tsx'), '''import { useEffect } from "react";
import { Outlet } from "react-router-dom";
import { CountryChatWidget } from "./CountryChatWidget";
import { MegaMenu } from "./MegaMenu";
import { PresenceWidget } from "./PresenceWidget";

export function Layout() {
  useEffect(() => {
    document.documentElement.style.scrollBehavior = "auto";
    return () => { document.documentElement.style.scrollBehavior = ""; };
  }, []);

  return (
    <div className="app-root">
      <header className="top-bar">
        <div className="top-bar-main">
          <div className="top-bar-brand">
            <span className="top-bar-brand-eyebrow">JATO Analysis System</span>
            <span className="top-bar-brand-title">Market Intelligence Control Deck</span>
          </div>
          <MegaMenu />
        </div>
      </header>
      <main className="main-area"><Outlet /></main>
      <PresenceWidget />
      <CountryChatWidget />
    </div>
  );
}
''', 'Layout.tsx')

# SharedFilterScopeContext
fix(os.path.join(F, 'contexts/SharedFilterScopeContext.tsx'),
    'return pathname === "/" || pathname === "/specification";',
    'return pathname === "/" || pathname === "/dashboard" || pathname === "/specification";',
    '/dashboard URL sync')

fix(os.path.join(F, 'contexts/SharedFilterScopeContext.tsx'),
    ': createSharedSelections({\n              powertrain: getDefaultPowertrainValues(',
    ': createSharedSelections({\n              country: topLevelOptions.country ?? [],\n              powertrain: getDefaultPowertrainValues(',
    'country defaults')

# Cleanup
for p in [os.path.join(F, 'components/AdminToolsNav.tsx'),
          os.path.join(F, 'pages/NordicHevInsightsPage.tsx')]:
    if os.path.exists(p):
        os.remove(p)
        print(f'  OK Deleted {os.path.basename(p)}')

for fn in ['DataManagementPage', 'EngineeringConfigPage', 'SpecificationPage',
           'MsrpPage', 'JatoMonthlyUpdatePage', 'ReviewCasesPage', 'CrudPage',
           'EngineeringPage']:
    p = os.path.join(F, 'pages', f'{fn}.tsx')
    if os.path.exists(p):
        lines = [l for l in open(p).readlines() if 'AdminToolsNav' not in l]
        with open(p, 'w') as f: f.writelines(lines)

print('\nALL DONE. Refresh the browser.')
