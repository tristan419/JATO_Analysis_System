# Permission Management — 实际实施方案

> 2026-06-01 | 状态: **已实施** | 取代 2026-05-25 的设计方案

## 实际架构

权限管理通过三个机制实现：

### 1. 角色层级 (`app/core/security.py`)

```python
ROLE_LEVEL = {
    "order_filler": 1,   # NEW — 受限订单填报
    "viewer": 1,
    "editor": 2,
    "admin": 3,
}
```

### 2. 两个依赖函数

| 函数 | 用途 | 示例 |
|------|------|------|
| `require_min_role("X")` | 层级检查，level >= X 即可 | 大部分 viewer/editor/admin 端点 |
| `require_roles("A","B","C")` | 显式白名单，忽略层级 | 排除 order_filler: `require_roles("viewer","editor","admin")` |
| `validate_country_access()` | 国家隔离，仅 order_filler 触发 | matrix/options/export/quantity-cell |

### 3. 前端

| 机制 | 文件 |
|------|------|
| Menu 过滤 | `pageNavigation.ts` — `ROLE_LEVEL: {order_filler:0, viewer:1, editor:2, admin:3}` |
| 路由守卫 | `RequireRole.tsx` — order_filler 只能访问 12 条路径 |
| 页面内权限 | `isAdmin = role === "admin"` 控制 BOM/PaymentTerm 按钮 |

## order_filler 角色

**用途：** 订单填报专用账号。只能看自己国家的选品表，可以填数量、导入导出，但不能访问 BOM Admin、Payment Terms、COC Match、Data Ops 等。

**国家隔离：**
- 一主多副（`primary_country_code` + `secondary_country_codes`）
- 只有 admin 能修改，自己改不了（`update_my_profile` 403）
- 后端强制校验（`validate_country_access` 查 DB）
- 前端国家列表过滤（`GET /countries` 只返回已分配国家）

## 权限矩阵（实际）

| 功能 | order_filler | viewer | editor | admin |
|------|:--:|:--:|:--:|:--:|
| Dashboard / Market Scan 查看 | ✅ | ✅ | ✅ | ✅ |
| Order Genius 查看（仅本人国家）| ✅ | ✅ | ✅ | ✅ |
| Order 数量编辑 / 导入导出 | ✅ | — | ✅ | ✅ |
| Material 上传 | — | — | ✅ | ✅ |
| Publish Material Baseline | — | — | — | ✅ |
| Payment Term 勘误 | — | — | — | ✅ |
| BOM 底表编辑 | — | — | — | ✅ |
| 用户管理 / 权限审批 | — | — | — | ✅ |
| 修改本人国家分配 | — | ✅ | ✅ | ✅ |
| 工程配置管理 | — | ✅ | ✅ | ✅ |
| MSRP 价格管理 | — | ✅ | ✅ | ✅ |

## 关键文件

| 文件 | 作用 |
|------|------|
| `app/core/security.py` | `require_roles`, `validate_country_access`, dev mode token resolution |
| `app/api/routes/auth.py` | role validation, country-change block, delete user, reset password |
| `app/api/routes/order_genius.py` | country enforcement, role gating on payment-terms/bom-admin |
| `frontend/src/components/RequireRole.tsx` | 路由守卫 |
| `frontend/src/utils/pageNavigation.ts` | Menu 角色过滤 |
| `frontend/src/pages/AccessControlPage.tsx` | 用户管理 UI（CRUD + 筛选 + 国家多选）|
| `frontend/src/pages/ProfilePage.tsx` | order_filler 禁止自改国家 |

## 与设计方案的差异

原设计方案（`app/core/permissions.py` + `FUNCTION_PERMISSIONS` dict）未实施。实际采用更轻量的方案：
- 不需要新文件
- `require_roles` 只在需要排除 order_filler 的端点上使用（~4 个端点）
- `validate_country_access` 只在需要国家隔离的端点上调用（~6 个端点）
- 其余沿用 `require_min_role`
