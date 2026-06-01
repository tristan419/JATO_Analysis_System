import { describe, expect, it } from "vitest";

import {
  MEGA_MENU_ITEMS,
  filterMenuByRole,
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
    expect(paths).toContain("/product/order-genius");
    expect(paths).toContain("/data/spec-detail");
    expect(paths).toContain("/product/pricing");
    expect(paths).toContain("/product/compare");
    expect(paths).toContain("/product/customer-insight");
    expect(paths).toContain("/product/current-msrp");
    expect(paths).toContain("/product/coc-match");
    expect(paths).toContain("/data/overview");
    expect(paths).toContain("/engineering-config");

    for (const path of [
      "/data/config-import",
      "/data/matching-review",
      "/data/jato-monthly-update",
      "/admin/access-control",
    ]) {
      expect(paths).not.toContain(path);
    }
  });

  it("uses the same role policy in the route guard", () => {
    expect(isRouteAllowedForRole("/product/pricing", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/data/spec-detail", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/market/transfer", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/data/order-genius", "order_filler")).toBe(true);
    expect(isRouteAllowedForRole("/data/config-import", "order_filler")).toBe(false);
    expect(isRouteAllowedForRole("/admin/access-control", "order_filler")).toBe(false);
  });
});
