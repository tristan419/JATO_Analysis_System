// @vitest-environment jsdom
import { createElement } from "react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { BomAdminPanel } from "../../pages/OrderGeniusPage";
import type { ColourHexRule } from "../../types/orderGenius";
import { clearCachedPageValue } from "../../utils/pageCache";
import gridSource from "../../components/OrderGeniusGrid.tsx?raw";
import pageSource from "../../pages/OrderGeniusPage.tsx?raw";

describe("Order Genius colour rule page contract", () => {
  it("keeps all five rule categories, detail view, and preview/apply states visible", () => {
    for (const status of ["fillable", "missing", "name_conflict", "swatch_conflict", "complete"]) {
      expect(pageSource).toContain(`status: "${status}"`);
    }
    expect(pageSource).toContain("selectedColourRuleDetails");
    expect(pageSource).toContain("loadingColourRulePreview");
    expect(pageSource).toContain("colourRuleActionError");
    expect(pageSource).toContain("colourRuleApplyResult");
    expect(pageSource).toContain("colourRulePreview.fingerprint");
  });

  it("guards async Add/Edit lookup and preserves manual edits", () => {
    expect(pageSource).toContain("colourCodeLookupRequestRef.current !== requestId");
    expect(pageSource).toContain("addColourLookupRequestRef.current !== requestId");
    expect(pageSource).toContain("colourNameTouched: true");
    expect(pageSource).toContain("colourHexTouched: true");
    expect(pageSource).toContain("Manual values will create a rule difference");
    expect(pageSource).toContain("conflict: not auto-filled. Enter values manually");
    expect(pageSource).toContain("Several existing colours match this name. Choose one below");
    expect(pageSource).toContain("Name match selected one existing colour");
    expect(pageSource).toContain("Approximate swatch generated from the colour name");
    expect(pageSource).toContain('"generated_from_name"');
    expect(pageSource).toContain("nameCandidates.map");
    expect(pageSource).toContain("No reusable Brand + Code rule");
    expect(pageSource).toContain("Wait for the Brand + Code rule check to finish.");
    expect(pageSource).toContain("This code cannot be auto-filled: enter a colour name before saving it.");
    expect(pageSource).toContain("const BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS = 1200;");
    expect(pageSource).toContain("}, BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS);");
    expect(pageSource).toContain("colourCodeEditorTargetKey");
    expect(pageSource).toContain("addColourEditorTargetKey");
    expect(pageSource).toContain("lookupOrderGeniusColourHexRule(brand, colourCode, colourName)");
    expect(pageSource).toContain("getBomColourCodeEditorTargetKey(current) !== targetKey");
    expect(pageSource).toContain("getBomAddColourEditorTargetKey(current) !== targetKey");
    expect(pageSource).toContain("setColourCodeRuleLookup(null);");
    expect(pageSource).toContain("setAddColourRuleLookup(null);");
    expect(pageSource).not.toContain("}, [colourCodeEditor]);");
    expect(pageSource).not.toContain("}, [addColourEditor]);");
  });

  it("refreshes BOM data with the applied search and preserves an explicit clear", () => {
    expect(pageSource).toContain("const appliedBomSearchRef = useRef(cachedSearchText.trim());");
    expect(pageSource).toContain("requestedSearch === undefined");
    expect(pageSource).toContain("latestLoadKeyRef.current = loadKey;");
    expect(pageSource).toContain("pendingLoadKeyRef.current = loadKey;");
    expect(pageSource.includes("if (latestLoadKeyRef.current === loadKey) {")).toBe(true);
    expect(pageSource.includes("loaded = await load(pendingLoadKey);")).toBe(true);
    expect(pageSource).toContain("void load(nextSearch);");
    expect(pageSource).toContain("await load(\"\");");
    expect(pageSource).toContain("load(debouncedSearch);");
  });

  it("passes Matrix colour fields through and renders the database swatch", () => {
    for (const field of ["colourCode: r.colourCode", "colourTier: r.colourTier", "colourHex: r.colourHex"]) {
      expect(pageSource).toContain(field);
    }
    expect(gridSource).toContain("parseOrderGeniusColourSwatch(p.data?.colourHex)");
    expect(gridSource).not.toContain("'carbon crystal black'");
  });

  it("surfaces incomplete identities and keeps BOM/Matrix colour edits shared", () => {
    expect(pageSource).toContain("invalidIdentitySkuCount");
    expect(pageSource).toContain("invalidIdentitySampleMaterialCodes");
    expect(pageSource).toContain("setOrderGeniusColourHexRuleStandard");
    expect(pageSource).toContain("Updated shared");
    expect(pageSource).toContain("Confirm Brand + Code standards");
    expect(pageSource).toContain("Confirm ${colourRulePreview.rules.length} shared standards");
    expect(pageSource).toContain("No conflict-free standards to confirm");
    expect(pageSource).toContain("conflict groups excluded from batch");
    expect(pageSource).not.toContain("selected most-used name");
    expect(pageSource).toContain('if (hexes.length === 0) hexes.push("");');
    expect(pageSource).toContain("openColourRuleStandardEditor(rule, choice.colourName, choice.colourHex)");
    expect(pageSource).toContain("saved PI snapshots stay unchanged");
  });

  it("uses a neutral swatch border while retaining keyboard focus", () => {
    expect(pageSource).toContain("className=\"bom-colour-swatch-button\"");
    expect(pageSource).not.toContain("2px solid #3b82f6");
    expect(pageSource).toContain("1px solid #d1d5db");
  });

  it("keeps tier repricing as a separate backend-derived report", () => {
    expect(pageSource).toContain("setColourTierReview({ previousTier, nextTier: tierName, report: result.reprice })");
    expect(pageSource).toContain("colourTierReview.report.details.map");
    expect(pageSource).toContain("manual FOB skipped");
    expect(pageSource).toContain("missing Single base");
  });

  it("surfaces protected-request auth failures without discarding quantity drafts", () => {
    expect(pageSource).toContain("AUTH_FAILURE_EVENT");
    expect(pageSource).toContain("登录已失效，未保存的 BOM/订单输入仍保留在当前页面");
    expect(pageSource).toContain("当前账号没有执行此 BOM 操作的权限");
    expect(pageSource).toContain("refreshUser({ preserveSession: true })");
    expect(pageSource).toContain("登录已恢复，请主动重试刚才的保存。");
    expect(pageSource).toContain('window.open(loginUrl, "_blank", "noopener,noreferrer")');
    expect(pageSource).toContain("const authStatus = getErrorStatus(err)");
    expect(pageSource).toContain("if (authStatus === 401 || authStatus === 403)");
  });

  it("requires an explicit current-or-past Historical backfill flow", () => {
    expect(pageSource).toContain("Include historical materials");
    expect(pageSource).toContain("selectedOrderMonthIsFuture");
    expect(pageSource).toContain("row.historicalBackfill === true");
    expect(pageSource).toContain("includeHistorical: data.historicalBackfill === true");
    expect(pageSource).toContain("This will not reactivate the material");
    expect(pageSource).toContain("confirmHistorical: confirmHistoricalPi");
    expect(pageSource).toContain("confirmUndatedDefaultFob: confirmUndatedHistoricalFob");
    expect(pageSource).toContain("confirmHistoricalSurcharge");
    expect(pageSource).toContain("historicalFobOverrideEur");
    expect(pageSource).toContain("Historical surcharge: saved");
    expect(pageSource).toContain("Override reason");
    expect(pageSource).toContain("Open BOM Admin");
  });
});

describe("Shared colour standard confirmation interactions", () => {
  const conflict: ColourHexRule = {
    brand: "OMODA", colourCode: "SY", colourName: null, normalizedColourName: null,
    standardColourName: null, standardColourHex: null, status: "name_conflict",
    skuCount: 3, fillableSkuCount: 0, placeholderNameSkuCount: 0, missingSwatchSkuCount: 3,
    sampleMaterialCodes: ["A", "B", "C"], hasNameConflict: true, hasSwatchConflict: false,
    nameOptions: [
      { colourName: "Mist Green", normalizedColourName: "mist green", skuCount: 2 },
      { colourName: "Misty Green", normalizedColourName: "misty green", skuCount: 1 },
    ], hexOptions: [],
  };
  beforeEach(() => {
    clearCachedPageValue("order-genius:bom-admin");
    vi.spyOn(api, "getBomAdmin").mockResolvedValue({ items: [], countries: ["NL"] });
    vi.spyOn(api, "getOrderGeniusColourSurcharges").mockResolvedValue({ items: [] });
    vi.spyOn(api, "getOrderGeniusSpecialColourSurcharges").mockResolvedValue({ items: [] });
    vi.spyOn(api, "getOrderGeniusColourHexRules").mockResolvedValue({
      items: [conflict],
      summary: { totalRules: 1, fillable: 0, missing: 0, nameConflict: 1, swatchConflict: 0,
        complete: 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] },
    });
  });
  afterEach(() => { cleanup(); vi.restoreAllMocks(); });

  async function openConflict() {
    render(createElement(BomAdminPanel));
    fireEvent.click(await screen.findByRole("button", { name: "Edit tools" }));
    fireEvent.click(await screen.findByRole("button", { name: /Name conflict/ }));
    await screen.findByRole("dialog", { name: "Colour rule details" });
  }

  it("opens missing-HEX conflicts without immediately saving or inventing grey", async () => {
    const save = vi.spyOn(api, "setOrderGeniusColourHexRuleStandard");
    await openConflict();
    fireEvent.click(screen.getByRole("button", { name: /Misty Green · Enter HEX/ }));
    expect(screen.queryByRole("dialog", { name: "Colour rule details" })).toBeNull();
    expect(screen.getByLabelText("Shared colour name").getAttribute("value")).toBe("Misty Green");
    expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("");
    expect(screen.getByRole("button", { name: "Save shared colour standard" }).hasAttribute("disabled")).toBe(true);
    expect(save).not.toHaveBeenCalled();
  });

  it("confirms the shared range and cancellation writes nothing", async () => {
    const confirm = vi.spyOn(window, "confirm").mockReturnValue(false);
    const save = vi.spyOn(api, "setOrderGeniusColourHexRuleStandard").mockResolvedValue({
      brand: "OMODA", colourCode: "SY", colourName: "Misty Green", normalizedColourName: "misty green",
      colourHex: "#8BA99A", updated: 3, materialCodes: ["A", "B", "C"],
    });
    await openConflict();
    fireEvent.click(screen.getByRole("button", { name: /Misty Green · Enter HEX/ }));
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#8BA99A" } });
    fireEvent.click(screen.getByRole("button", { name: "Save shared colour standard" }));
    expect(save).not.toHaveBeenCalled();
    expect(confirm).toHaveBeenCalledWith(expect.stringContaining("3 active SKUs"));
    confirm.mockReturnValue(true);
    fireEvent.click(screen.getByRole("button", { name: "Save shared colour standard" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({
      brand: "OMODA", colourCode: "SY", colourName: "Misty Green", colourHex: "#8BA99A",
    }));
    await waitFor(() => expect(screen.queryByLabelText("Shared colour name")).toBeNull());
    expect(api.getBomAdmin).toHaveBeenCalledTimes(2);
  });

  it("does not report all confirmed when conflicts are excluded from batch", async () => {
    vi.spyOn(api, "previewOrderGeniusColourHexRuleFills").mockResolvedValue({
      rules: [], items: [], total: 0, ruleCount: 0, generatedRuleCount: 0,
      unresolvedRuleCount: 0, unresolvedConflictCount: 1, fingerprint: "conflicts-only",
    });
    const apply = vi.spyOn(api, "applyOrderGeniusColourHexRuleFills");
    render(createElement(BomAdminPanel));
    fireEvent.click(await screen.findByRole("button", { name: "Edit tools" }));
    const preview = await screen.findByRole("button", { name: "Preview shared swatch standards" });
    await waitFor(() => expect(preview.hasAttribute("disabled")).toBe(false));
    fireEvent.click(preview);
    await screen.findByText(/1 conflict groups excluded from batch/);
    expect(screen.queryByRole("button", { name: /Confirm .* shared standards/ })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /Review name conflicts/ }));
    await screen.findByRole("dialog", { name: "Colour rule details" });
    expect(apply).not.toHaveBeenCalled();
  });
});
