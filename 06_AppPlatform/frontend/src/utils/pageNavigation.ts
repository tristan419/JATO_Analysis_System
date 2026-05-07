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
