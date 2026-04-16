import re

with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/backend/app/services/market_scan_service.py', 'r', encoding='utf8') as f:
    content = f.read()

target = '''    if any(token in text for token in ("fwd", "rwd", "2wd", "front wheel", "rear wheel", "two wheel", "sdrive")):'''
replacement = '''    if any(token in text for token in ("fwd", "rwd", "2wd", "front wheel", "rear wheel", "two wheel", "sdrive", "front", "rear")):'''

target2 = '''    if any(token in text for token in ("awd", "4wd", "4x4", "all wheel", "four wheel", "quattro", "4-matic", "4matic", "xdrive")):'''
replacement2 = '''    if any(token in text for token in ("awd", "4wd", "4x4", "all wheel", "four wheel", "quattro", "4-matic", "4matic", "xdrive", "all", "four")):'''

new_content = content.replace(target, replacement).replace(target2, replacement2)
with open('/Users/litristan/Downloads/JATO_Analysis_System/06_AppPlatform/backend/app/services/market_scan_service.py', 'w', encoding='utf8') as f:
    f.write(new_content)
