import { describe, expect, it } from "vitest";

import {
  MEGA_MENU_ITEMS,
  filterMenuByRole,
  getActiveMegaMenuId,
  isKnownAppRoute,
  isRouteAllowedForRole,
  type MegaMenuItem,
} from "../../utils/pageNavigation";

function collectMenuPaths(items: MegaMenuItem[]): string[] {
  const paths: string[] = [];
  for (const item of items) {
    if (item.type === "link") {
      paths.push(item.to);
    } else if (item.type === "dropdown") {
      paths.push(...item.items.map((subItem) => subItem.to));
    } else {
      for (const group of item.groups) {
        paths.push(...group.items.map((subItem) => subItem.to));
      }
    }
  }
  return paths;
}

describe("order filler navigation", () => {
  it("exposes viewer-visible routes plus order filler tools", () => {
    const paths = collectMenuPaths(
      filterMenuByRole(MEGA_MENU_ITEMS, "order_filler"),
    );

    expect(paths).toContain("/dashboard");
    expect(paths).toContain("/market/overview");
    expect(paths).toContain("/market/advanced-analysis");
    expect(paths).toContain("/astrbot");
    expect(paths).toContain("/market/advanced-analysis?mode=hero-product");
    expect(paths).toContain("/product/order-genius");
    expect(paths).toContain("/product/order-genius/vehicle-allocation");
    expect(paths).toContain("/data/spec-detail");
    expect(paths).toContain("/product/pricing");
    expect(paths).toContain("/product/compare");
    expect(paths).toContain("/product/customer-insight");
    expect(paths).toContain("/product/current-msrp");
    expect(paths).toContain("/data/overview");
    expect(paths).toContain("/engineering-config");

    for (const path of [
      "/product/coc-match",
      "/product/order-genius/cbu",
      "/data/config-import",
      "/data/matching-review",
      "/data/jato-monthly-update",
      "/admin/access-control",
    ]) {
      expect(paths).not.toContain(path);
    }
  });

  it("keeps AstrBot under Market Analysis and highlights Market Scan", () => {
    const marketMenu = MEGA_MENU_ITEMS.find((item) => item.id === "market-scan");
    expect(marketMenu?.type).toBe("mega");
    if (!marketMenu || marketMenu.type !== "mega") {
      throw new Error("Market Scan menu is missing");
    }

    const marketAnalysis = marketMenu.groups.find(
      (group) => group.title === "Market Analysis / 市场分析",
    );
    const paths = marketAnalysis?.items.map((item) => item.to) ?? [];
    expect(paths).toContain("/astrbot");
    expect(paths.indexOf("/astrbot")).toBeGreaterThan(
      paths.indexOf("/market/advanced-analysis"),
    );
    expect(getActiveMegaMenuId("/astrbot")).toBe("market-scan");
    expect(getActiveMegaMenuId("/astrbot/eval")).toBe("market-scan");
  });

  it("uses the same role policy in the route guard", () => {
    expect(isRouteAllowedForRole("/product/pricing", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/data/spec-detail", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/market/transfer", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/astrbot", "viewer")).toBe(true);
    expect(isRouteAllowedForRole("/astrbot/eval", "viewer")).toBe(true);
    expect(isRouteAllowedForRole("/data/order-genius", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/product/order-genius/vehicle-allocation", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/product/order-genius/cbu", "editor")).toBe(true);
    expect(isRouteAllowedForRole("/product/order-genius/cbu", "order_filler")).toBe(false);
    expect(isRouteAllowedForRole("/product/coc-match", "editor")).toBe(true);
    expect(isRouteAllowedForRole("/product/coc-match", "order_filler")).toBe(false);
    expect(isRouteAllowedForRole("/product/order-genius/vehicle-allocation", "viewer")).toBe(false);
    expect(isRouteAllowedForRole("/data/config-import", "order_filler")).toBe(false);
    expect(isRouteAllowedForRole("/admin/access-control", "order_filler")).toBe(false);
  });

  it("distinguishes unknown routes from protected known routes", () => {
    expect(isKnownAppRoute("/dashboard")).toBe(true);
    expect(isKnownAppRoute("/product/order-genius")).toBe(true);
    expect(isKnownAppRoute("/route-that-does-not-exist")).toBe(false);
  });
});
