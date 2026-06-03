export interface PageNavigationItem {
  to: string;
  code: string;
  label: string;
  sublabel: string;
}

export const PAGE_NAV_ITEMS: PageNavigationItem[] = [
  { to: "/", code: "01", label: "Overview", sublabel: "Dashboard" },
  { to: "/msrp", code: "06", label: "MSRP", sublabel: "当前价格" },
  { to: "/market-scan", code: "07", label: "Scan", sublabel: "市场扫描" },
  { to: "/positioning-pricing", code: "08", label: "Pricing", sublabel: "定位定价" },
  { to: "/version-comparison", code: "09", label: "Compare", sublabel: "版型对比" },
  { to: "/customer-insights", code: "10", label: "Customer", sublabel: "看客户" },
  { to: "/customer-hev", code: "11", label: "Hybrid", sublabel: "看HEV" },
  { to: "/copilot", code: "12", label: "Copilot", sublabel: "国家助手" },
];

function matchesNavPath(pathname: string, path: string): boolean {
  return path === "/"
    ? pathname === "/"
    : pathname === path || pathname.startsWith(`${path}/`);
}

export function getActivePageIndex(pathname: string): number {
  return PAGE_NAV_ITEMS.findIndex((item) => matchesNavPath(pathname, item.to));
}

export function getAdjacentPage(
  pathname: string,
  direction: -1 | 1,
): PageNavigationItem | null {
  const activeIndex = getActivePageIndex(pathname);
  if (activeIndex < 0) {
    return null;
  }
  return PAGE_NAV_ITEMS[activeIndex + direction] ?? null;
}

export function getAdjacentKeyedItem<T extends { key: string }>(
  items: T[],
  activeKey: string,
  direction: -1 | 1,
): T | null {
  const activeIndex = items.findIndex((item) => item.key === activeKey);
  if (activeIndex < 0) {
    return null;
  }
  return items[activeIndex + direction] ?? null;
}

export function getAdjacentValuedItem<T extends { value: string }>(
  items: T[],
  activeValue: string,
  direction: -1 | 1,
): T | null {
  const activeIndex = items.findIndex((item) => item.value === activeValue);
  if (activeIndex < 0) {
    return null;
  }
  return items[activeIndex + direction] ?? null;
}

export function getHorizontalNavigationDirectionFromKey(
  key: string,
): -1 | 1 | null {
  if (key === "ArrowLeft") {
    return -1;
  }
  if (key === "ArrowRight") {
    return 1;
  }
  return null;
}

export function getVerticalNavigationDirectionFromKey(
  key: string,
): -1 | 1 | null {
  if (key === "ArrowUp") {
    return -1;
  }
  if (key === "ArrowDown") {
    return 1;
  }
  return null;
}

export function shouldIgnorePageNavigationTarget(
  target: EventTarget | null,
): boolean {
  if (!(target instanceof HTMLElement)) {
    return false;
  }
  if (target.closest("input, textarea, select, button, [role='textbox'], [role='combobox'], [role='slider'], [data-page-nav-ignore='true']")) {
    return true;
  }
  let current: HTMLElement | null = target;
  while (current) {
    if (current.isContentEditable) {
      return true;
    }
    current = current.parentElement;
  }
  return false;
}

/* ── Mega Menu data types ─────────────────────────────── */

export type MenuRole = "viewer" | "order_filler" | "editor" | "admin";

const ROLE_LEVEL: Record<MenuRole, number> = { viewer: 0, order_filler: 1, editor: 2, admin: 3 };

export interface MegaMenuSubItem {
  label: string;
  sublabel: string;
  to: string;
  minRole?: MenuRole;
}

export interface MegaMenuGroup {
  title: string;
  items: MegaMenuSubItem[];
}

export type MegaMenuItem =
  | { id: string; label: string; sublabel: string; type: "link"; to: string; minRole?: MenuRole }
  | { id: string; label: string; sublabel: string; type: "dropdown"; items: MegaMenuSubItem[]; minRole?: MenuRole }
  | { id: string; label: string; sublabel: string; type: "mega"; groups: MegaMenuGroup[]; minRole?: MenuRole };

export function filterMenuByRole(items: MegaMenuItem[], userRole: string): MegaMenuItem[] {
  const level = ROLE_LEVEL[userRole as MenuRole] ?? 0;
  return items
    .filter((item) => {
      const min = ROLE_LEVEL[item.minRole ?? "viewer"];
      return level >= min;
    })
    .map((item) => {
      if (item.type === "dropdown") {
        return {
          ...item,
          items: item.items.filter((sub) => {
            const subMin = ROLE_LEVEL[sub.minRole ?? "viewer"];
            return level >= subMin;
          }),
        };
      }
      if (item.type === "mega") {
        return {
          ...item,
          groups: item.groups.map((group) => ({
            ...group,
            items: group.items.filter((sub) => {
              const subMin = ROLE_LEVEL[sub.minRole ?? "viewer"];
              return level >= subMin;
            }),
          })),
        };
      }
      return item;
    });
}

function menuPathname(to: string): string {
  return to.split("?")[0] || "/";
}

function collectMenuPaths(items: MegaMenuItem[]): string[] {
  const paths: string[] = [];
  for (const item of items) {
    if (item.type === "link") {
      paths.push(menuPathname(item.to));
    } else if (item.type === "dropdown") {
      paths.push(...item.items.map((subItem) => menuPathname(subItem.to)));
    } else {
      for (const group of item.groups) {
        paths.push(...group.items.map((subItem) => menuPathname(subItem.to)));
      }
    }
  }
  return [...new Set(paths)];
}

export function getMenuPathsForRole(userRole: string): string[] {
  return collectMenuPaths(filterMenuByRole(MEGA_MENU_ITEMS, userRole));
}

const ROUTE_ROLE_OVERRIDES: Record<string, MenuRole> = {
  "/copilot": "viewer",
  "/market/segments": "viewer",
  "/market/ranking/brand": "viewer",
  "/market/ranking/model": "viewer",
  "/market/powertrain": "viewer",
  "/market/transfer": "viewer",
  "/market-scan": "viewer",
  "/msrp": "viewer",
  "/msrp/monthly-update": "editor",
  "/positioning-pricing": "viewer",
  "/version-comparison": "viewer",
  "/customer-insights": "viewer",
  "/customer-hev": "viewer",
  "/specification": "viewer",
  "/data-management": "viewer",
  "/data/order-genius": "order_filler",
  "/product/order-genius/vehicle-allocation": "order_filler",
  "/engineering": "editor",
  "/review": "editor",
  "/crud": "viewer",
};

export function isRouteAllowedForRole(pathname: string, userRole: string): boolean {
  if (pathname === "/" || pathname.startsWith("/login") || pathname === "/account/profile") {
    return true;
  }
  const level = ROLE_LEVEL[userRole as MenuRole] ?? ROLE_LEVEL.viewer;
  const override = Object.entries(ROUTE_ROLE_OVERRIDES).sort(
    ([a], [b]) => b.length - a.length,
  ).find(([path]) => matchesNavPath(pathname, path));
  if (override) {
    return level >= ROLE_LEVEL[override[1]];
  }
  return getMenuPathsForRole(userRole).some((path) => matchesNavPath(pathname, path));
}

export const MEGA_MENU_ITEMS: MegaMenuItem[] = [
  {
    id: "dashboard",
    label: "Dashboard",
    sublabel: "JATO看板",
    type: "mega",
    minRole: "viewer",
    groups: [
      {
        title: "JATO Board / JATO 看板",
        items: [
          { label: "Dashboard", sublabel: "JATO 总览", to: "/dashboard", minRole: "viewer" },
          { label: "Spec Detail", sublabel: "规格明细", to: "/data/spec-detail", minRole: "viewer" },
        ],
      },
    ],
  },
  {
    id: "market-scan",
    label: "Market Scan",
    sublabel: "市场扫描",
    type: "mega",
    minRole: "viewer",
    groups: [
      {
        title: "Market Analysis / 市场分析",
        items: [
          { label: "Overview", sublabel: "市场总览", to: "/market/overview", minRole: "viewer" },
          { label: "Advanced Analysis", sublabel: "高级分析", to: "/market/advanced-analysis", minRole: "viewer" },
        ],
      },
    ],
  },
  {
    id: "product-deck",
    label: "Product Deck",
    sublabel: "产品平台",
    type: "mega",
    minRole: "viewer",
    groups: [
      {
        title: "Pricing & Positioning / 价格与定位",
        items: [
          { label: "Pricing", sublabel: "定位定价", to: "/product/pricing" },
          { label: "Compare", sublabel: "版型对比", to: "/product/compare" },
          { label: "Customer Insight", sublabel: "看客户", to: "/product/customer-insight" },
          { label: "Current MSRP", sublabel: "当前价格", to: "/product/current-msrp" },
        ],
      },
      {
        title: "Product Toolkit / 产品工具包",
        items: [
          { label: "Order Genius", sublabel: "订单矩阵", to: "/product/order-genius", minRole: "order_filler" },
          { label: "Vehicle Allocation", sublabel: "PI 分车", to: "/product/order-genius/vehicle-allocation", minRole: "order_filler" },
          { label: "COC Match", sublabel: "COC 比对", to: "/product/coc-match", minRole: "viewer" },
        ],
      },
    ],
  },
  {
    id: "data-ops",
    label: "Data Ops",
    sublabel: "数据运维",
    type: "mega",
    groups: [
      {
        title: "Data View / 数据查看",
        items: [
          { label: "Hermes Steward", sublabel: "Hermes 小管家", to: "/data/overview?view=hermes" },
          { label: "Data Overview", sublabel: "数据总览", to: "/data/overview" },
        ],
      },
      {
        title: "Data Workflow / 数据流程",
        items: [
          { label: "Config Import", sublabel: "配置导入", to: "/data/config-import", minRole: "editor" },
          { label: "Matching Review", sublabel: "匹配审核", to: "/data/matching-review", minRole: "editor" },
          { label: "JATO Monthly Update", sublabel: "JATO 月更", to: "/data/jato-monthly-update", minRole: "editor" },
          { label: "Eng Config", sublabel: "工程配置", to: "/engineering-config", minRole: "viewer" },
          { label: "Access Control", sublabel: "权限管理", to: "/admin/access-control", minRole: "admin" },
        ],
      },
    ],
  },
];

const MEGA_MENU_ROUTE_MAP: Record<string, string> = {
  "/dashboard": "dashboard",
  "/market": "market-scan",
  "/product": "product-deck",
  "/data": "data-ops",
  "/admin": "data-ops",
};

export function getActiveMegaMenuId(pathname: string): string | null {
  if (pathname === "/" || pathname === "/dashboard") return "dashboard";
  if (pathname === "/data/spec-detail") return "dashboard";
  for (const [prefix, id] of Object.entries(MEGA_MENU_ROUTE_MAP)) {
    if (pathname === prefix || pathname.startsWith(`${prefix}/`)) return id;
  }
  return null;
}
