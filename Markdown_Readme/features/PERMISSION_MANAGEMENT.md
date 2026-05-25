# Permission Management — 集中权限管理方案

> 2026-05-25 | 状态: 已设计，待实施

## 问题

当前权限配置散落在 10+ 个路由文件中，每个路由手动指定 `require_min_role("XXX")`。改一个功能的权限需要逐文件查找替换，容易遗漏，无法一眼看清"谁能做什么"。

## 方案

加一个配置文件集中管理，不改框架。

### 新文件: `app/core/permissions.py`

```python
"""Centralised function → minimum role mapping."""

from app.core.security import require_min_role

FUNCTION_PERMISSIONS: dict[str, str] = {
    "dashboard.view":            "viewer",
    "market_scan.view":          "viewer",
    "coc_match.upload":          "viewer",
    "coc_match.view":            "viewer",
    "coc_match.download":        "viewer",
    "order_genius.view":         "viewer",
    "order_genius.edit_quantity": "editor",
    "order_genius.upload":       "editor",
    "order_genius.publish":      "admin",
    "payment_terms.edit":        "admin",
    "bom.edit":                  "admin",
    "lifecycle.edit":            "admin",
    "fob.edit":                  "admin",
    "user.create":               "admin",
    "user.edit_role":            "admin",
    "role_upgrade.request":      "viewer",
    "role_upgrade.review":       "admin",
    "engineering.edit":          "editor",
    "jato_monthly.upload":       "editor",
    "jato_monthly.publish":      "admin",
    "msrp.edit":                 "editor",
    "hermes.manage":             "admin",
}

def require_permission(feature: str):
    min_role = FUNCTION_PERMISSIONS.get(feature, "admin")
    return require_min_role(min_role)
```

### 路由文件改动

`require_min_role("X")` → `require_permission("feature.name")`，纯机械替换。

| 文件 | 改动数 |
|------|--------|
| `coc_match.py` | 9 |
| `order_genius.py` | ~15 |
| `auth.py` | ~5 |

### 权限矩阵

| 功能 | viewer | editor | admin |
|------|--------|--------|-------|
| Dashboard / MarketScan | ✅ | ✅ | ✅ |
| COC 上传/对比/下载 | ✅ | ✅ | ✅ |
| Order Genius 查看 | ✅ | ✅ | ✅ |
| Order 数量编辑 | — | ✅ | ✅ |
| Material 上传 | — | ✅ | ✅ |
| Publish Baseline | — | — | ✅ |
| Payment Term 勘误 | — | — | ✅ |
| BOM/Lifecycle/FOB | — | — | ✅ |
| 用户管理 | — | — | ✅ |

## 为什么足够

只有 3 个角色，10 个人用。一张静态表覆盖所有场景。以后需要更细粒度时升级为 DB 表，接口不变。
