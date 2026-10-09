// @vitest-environment jsdom
import { createElement } from "react";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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
    expect(pageSource).toContain("Existing same-name swatches: choose Use this swatch to adopt");
    expect(pageSource).not.toContain('"generated_from_name"');
    expect(pageSource).not.toContain('"name_candidate"');
    expect(pageSource).toContain("nameCandidates.map");
    expect(pageSource).toContain("No reusable Brand + Code rule");
    expect(pageSource).toContain("Wait for the Brand + Code rule check to finish.");
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
    expect(pageSource).toContain("Saved shared");
    expect(pageSource).toContain("Confirm Brand + Code standards");
    expect(pageSource).toContain("Confirm ${colourRulePreview.rules.length} shared standards");
    expect(pageSource).toContain("No conflict-free standards to confirm");
    expect(pageSource).toContain("conflict groups excluded from batch");
    expect(pageSource).not.toContain("selected most-used name");
    expect(pageSource).toContain('if (hexes.length === 0) hexes.push("");');
    expect(pageSource).toContain("openColourRuleStandardEditor(rule, choice.colourName, choice.colourHex, true)");
    expect(pageSource).toContain("saved material codes, BOM, descriptions and prices stay unchanged");
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
    vi.spyOn(api, "lookupOrderGeniusColourHexRule").mockResolvedValue({ brand: "OMODA", colourCode: "SY", status: "none", colourName: null, colourHex: null, source: "none", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] });
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
    expect(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }).hasAttribute("disabled")).toBe(false);
    expect(save).not.toHaveBeenCalled();
  });

  it("saves a confirmed name without submitting a placeholder HEX", async () => {
    const save = vi.spyOn(api, "setOrderGeniusColourHexRuleStandard").mockResolvedValue({
      brand: "OMODA", colourCode: "SY", colourName: "Misty Green", normalizedColourName: "misty green",
      colourHex: null, updated: 3, materialCodes: ["A", "B", "C"],
    });
    await openConflict();
    fireEvent.click(screen.getByRole("button", { name: /Misty Green · Enter HEX/ }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({
      brand: "OMODA", colourCode: "SY", colourName: "Misty Green", colourHex: undefined,
    }));
    await waitFor(() => expect(screen.queryByLabelText("Shared colour name")).toBeNull());
    expect(api.getBomAdmin).toHaveBeenCalledTimes(2);
  });

  it("rejects invalid or incomplete dual HEX instead of saving half a swatch", async () => {
    const save = vi.spyOn(api, "setOrderGeniusColourHexRuleStandard");
    await openConflict();
    fireEvent.click(screen.getByRole("button", { name: /Misty Green · Enter HEX/ }));
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#12345" } });
    expect(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }).hasAttribute("disabled")).toBe(true);
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#112233" } });
    fireEvent.click(screen.getByRole("checkbox", { name: /Dual swatch/ }));
    expect(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }).hasAttribute("disabled")).toBe(true);
    expect(save).not.toHaveBeenCalled();
  });

  it("confirms the shared range and cancellation writes nothing", async () => {
    const confirm = vi.spyOn(window, "confirm");
    const save = vi.spyOn(api, "setOrderGeniusColourHexRuleStandard").mockResolvedValue({
      brand: "OMODA", colourCode: "SY", colourName: "Misty Green", normalizedColourName: "misty green",
      colourHex: "#8BA99A", updated: 3, materialCodes: ["A", "B", "C"],
    });
    await openConflict();
    fireEvent.click(screen.getByRole("button", { name: /Misty Green · Enter HEX/ }));
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#8BA99A" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(save).not.toHaveBeenCalled();
    expect(screen.getByText(/Affects 3 materials/)).toBeTruthy();
    expect(confirm).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Back / 返回编辑" }));
    expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("#8BA99A");
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({
      brand: "OMODA", colourCode: "SY", colourName: "Misty Green", colourHex: "#8BA99A",
    }));
    await waitFor(() => expect(screen.queryByLabelText("Shared colour name")).toBeNull());
    expect(api.getBomAdmin).toHaveBeenCalledTimes(2);
  });

  it("counts missing HEX independently of mutually exclusive rule status", async () => {
    vi.spyOn(api, "getOrderGeniusColourHexRules").mockResolvedValue({
      items: [conflict, { ...conflict, colourCode: "CL", hasNameConflict: false, status: "missing",
        missingSwatchSkuCount: 0, placeholderNameSkuCount: 3, nameOptions: [],
        standardColourHex: "#111111", hexOptions: [{ colourHex: "#111111", skuCount: 3 }] }],
      summary: { totalRules: 2, fillable: 0, missing: 1, nameConflict: 1, swatchConflict: 0,
        complete: 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] },
    });
    render(createElement(BomAdminPanel));
    fireEvent.click(await screen.findByRole("button", { name: "Edit tools" }));
    const missingStandard = await screen.findByRole("button", { name: /Missing standard/ });
    await waitFor(() => expect(within(missingStandard).getByText("1")).toBeTruthy());
    const missingHex = screen.getByRole("button", { name: /Missing HEX/ });
    expect(within(missingHex).getByText("1")).toBeTruthy();
    fireEvent.click(missingHex);
    await screen.findByRole("dialog", { name: "Colour rule details" });
    expect(screen.getByText("OMODA · SY")).toBeTruthy();
    expect(screen.queryByText("OMODA · CL")).toBeNull();
    expect(screen.getByRole("button", { name: /Misty Green · Enter HEX/ })).toBeTruthy();
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

  it("previews name-only fill with null HEX and applies the exact missing targets", async () => {
    vi.spyOn(api, "previewOrderGeniusColourHexRuleFills").mockResolvedValue({
      rules: [{ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: null, source: "persistent_rule", skuCount: 2, hasNameConflict: false, hasSwatchConflict: false, nameOptions: [] }],
      items: [{ materialCode: "B", brand: "OMODA", colourCode: "BX", oldColourName: "BX", newColourName: "Khaki white", oldColourHex: null, newColourHex: null }],
      total: 1, ruleCount: 1, generatedRuleCount: 0, unresolvedRuleCount: 0, unresolvedConflictCount: 0, fingerprint: "name-only",
    });
    const apply = vi.spyOn(api, "applyOrderGeniusColourHexRuleFills").mockResolvedValue({ updated: 1, unchanged: 1, rulesCreated: 0, generatedRules: 0, conflicts: 0, missingRules: 0, materialCodes: ["B"], items: [], fingerprint: "name-only" });
    render(createElement(BomAdminPanel));
    fireEvent.click(await screen.findByRole("button", { name: "Edit tools" }));
    fireEvent.click(await screen.findByRole("button", { name: "Preview shared swatch standards" }));
    const preview = await screen.findByRole("dialog", { name: "Colour rule fill preview" });
    expect(within(preview).getByText("1 names / 名称 · 0 swatches / 色卡")).toBeTruthy();
    expect(within(preview).getByText(/Keep missing/)).toBeTruthy();
    fireEvent.click(within(preview).getByRole("button", { name: "Confirm 1 shared standards" }));
    await waitFor(() => expect(apply).toHaveBeenCalledWith("name-only", ["B"]));
    await screen.findByText(/Tables refreshed/);
  });

  const material = { materialCode: "T6480J1BXLX0017", bomTemplate: "T6480J1**LX0017", brand: "OMODA", modelName: "OMODA9 SHS", version: "Exclusive-AWD", powertrain: "PHEV", colour: "Khaki white", colourCode: "BX", colourHex: null, colourTier: "single", interiorColorName: "Black-Red", lifecycleStatus: "active", fobByCountry: { NL: { finalFobEur: 25400 } }, rowVersion: 1 };
  const bxRule: ColourHexRule = { ...conflict, colourCode: "BX", colourName: "Khaki white", standardColourName: "Khaki white", normalizedColourName: "khaki white", skuCount: 2, missingSwatchSkuCount: 2, status: "missing", hasNameConflict: false, nameOptions: [{ colourName: "Khaki white", normalizedColourName: "khaki white", skuCount: 2 }] };

  async function openMaterial(onFobChanged = vi.fn().mockResolvedValue(undefined), colourHex: string | null = null, rulePatch: Partial<ColourHexRule> = {}, storedColourHex: string | null = colourHex, materialPatch: { colour?: string; colourCodeConfirmed?: boolean } = {}) {
    vi.spyOn(api, "getBomAdmin").mockResolvedValue({ items: [{ ...material, colourHex, storedColourHex, ...materialPatch }], countries: ["NL"] });
    vi.spyOn(api, "getOrderGeniusColourHexRules").mockResolvedValue({ items: [{ ...bxRule, ...rulePatch }], summary: { totalRules: 1, fillable: 0, missing: 1, nameConflict: 0, swatchConflict: 0, complete: 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] } });
    render(createElement(BomAdminPanel, { onFobChanged }));
    fireEvent.click(await screen.findByText("OMODA OMODA9 SHS"));
    fireEvent.click(await screen.findByRole("button", { name: /Edit swatch rule for OMODA BX/ }));
    return onFobChanged;
  }

  function mockSaved() {
    return vi.spyOn(api, "setOrderGeniusColourHexRuleStandard").mockResolvedValue({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", normalizedColourName: "khaki white", colourHex: "#F2F4F8", updated: 2, materialCodes: [material.materialCode, "T6480J1BXLX0018"] });
  }

  function nameSuggestion() {
    vi.mocked(api.lookupOrderGeniusColourHexRule).mockResolvedValue({ brand: "OMODA", colourCode: "BX", status: "missing", colourName: null, colourHex: null, source: "name_candidates", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [{ brand: "OMODA", colourCode: "BW", colourName: "Khaki white", colourHex: "#F2F4F8", status: "complete", hasNameConflict: false, hasSwatchConflict: false }] });
  }

  it("missing names open from both entries without writes", async () => {
    const save = mockSaved();
    await openMaterial(undefined, null, {}, null, { colour: "", colourCodeConfirmed: false });
    expect(screen.getByRole("dialog", { name: "Edit colour · OMODA · BX" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    fireEvent.click(screen.getByTitle("Unconfirmed colour code — click to edit and confirm"));
    expect(screen.getByRole("dialog", { name: "Edit colour · OMODA · BX" })).toBeTruthy();
    expect(save).not.toHaveBeenCalled();
  });

  it("unchanged unconfirmed material confirms only itself", async () => {
    const save = mockSaved();
    const confirm = vi.spyOn(api, "confirmColourCode").mockResolvedValue({ materialCode: material.materialCode, colourCodeConfirmed: true });
    await openMaterial(undefined, null, {}, null, { colourCodeConfirmed: false });
    fireEvent.click(screen.getByRole("button", { name: "Confirm code" }));
    expect(screen.getAllByText(/Confirm this material only/).length).toBeGreaterThan(0);
    fireEvent.click(screen.getByRole("button", { name: "Confirm code" }));
    await waitFor(() => expect(confirm).toHaveBeenCalledWith(material.materialCode));
    expect(save).not.toHaveBeenCalled();
  });

  it("changed unconfirmed material saves and confirms in one request", async () => {
    const save = mockSaved();
    const confirm = vi.spyOn(api, "confirmColourCode");
    await openMaterial(undefined, null, {}, null, { colourCodeConfirmed: false });
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    fireEvent.click(screen.getByRole("button", { name: "Save & confirm code" }));
    fireEvent.click(screen.getByRole("button", { name: "Save & confirm code" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Reviewed white", colourHex: undefined, confirmMaterialCode: material.materialCode }));
    expect(confirm).not.toHaveBeenCalled();
  });

  it("unique same-code name and HEX can be saved from a missing draft without touching the picker", async () => {
    vi.mocked(api.lookupOrderGeniusColourHexRule).mockResolvedValue({ brand: "OMODA", colourCode: "BX", status: "complete", colourName: "Khaki white", colourHex: "#112233", source: "persistent_rule", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] });
    const save = mockSaved();
    await openMaterial(undefined, null, {}, null, { colour: "", colourCodeConfirmed: false });
    await waitFor(() => expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("#112233"), { timeout: 3000 });
    expect(screen.getByLabelText("Shared colour name").getAttribute("value")).toBe("Khaki white");
    fireEvent.click(screen.getByRole("button", { name: "Save & confirm code" }));
    fireEvent.click(screen.getByRole("button", { name: "Save & confirm code" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#112233", confirmMaterialCode: material.materialCode }));
  });

  it("ordinary rule-list name-only editing does not adopt an already loaded swatch", async () => {
    const save = mockSaved();
    vi.mocked(api.getOrderGeniusColourHexRules).mockResolvedValue({ items: [{ ...bxRule,
      status: "fillable", standardColourHex: "#112233", fillableSkuCount: 1,
    }], summary: { totalRules: 1, fillable: 1, missing: 0, nameConflict: 0, swatchConflict: 0, complete: 0, fillableSkus: 1, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] } });
    render(createElement(BomAdminPanel));
    fireEvent.click(await screen.findByRole("button", { name: "Edit tools" }));
    fireEvent.click(await screen.findByRole("button", { name: /Can fill/ }));
    fireEvent.click(within(await screen.findByRole("dialog", { name: "Colour rule details" })).getByRole("button", { name: "Edit colour" }));
    expect(screen.queryByRole("button", { name: "Correct code" })).toBeNull();
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByText(/Keep each material's existing swatch/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Reviewed white", colourHex: undefined }));
  });

  it("explicit conflict candidate selection still adopts its swatch", async () => {
    const save = mockSaved();
    vi.mocked(api.getOrderGeniusColourHexRules).mockResolvedValue({ items: [{ ...bxRule,
      status: "swatch_conflict", hasSwatchConflict: true, hexOptions: [{ colourHex: "#112233", skuCount: 1 }, { colourHex: "#445566", skuCount: 1 }],
    }], summary: { totalRules: 1, fillable: 0, missing: 0, nameConflict: 0, swatchConflict: 1, complete: 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] } });
    render(createElement(BomAdminPanel));
    fireEvent.click(await screen.findByRole("button", { name: "Edit tools" }));
    fireEvent.click(await screen.findByRole("button", { name: /Swatch conflict/ }));
    fireEvent.click(within(await screen.findByRole("dialog", { name: "Colour rule details" })).getByRole("button", { name: "Khaki white · #112233" }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#112233" }));
  });

  it.each([null, "#445566"])("correction Keep previews the actual stored HEX %s rather than the shared display", async storedHex => {
    nameSuggestion();
    const save = vi.spyOn(api, "updateColourCode").mockResolvedValue({ materialCode: "T6480J1ZZLX0017", colourCode: "ZZ", colourName: "Khaki white", colourHex: storedHex });
    await openMaterial(undefined, "#112233", {}, storedHex);
    fireEvent.click(screen.getByRole("button", { name: "Correct code" }));
    fireEvent.change(screen.getByLabelText("Colour code"), { target: { value: "ZZ" } });
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Khaki white" } });
    await screen.findByRole("button", { name: "Use this swatch" }, { timeout: 3000 });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    const after = screen.getByText("After / 修改后").parentElement;
    if (!after) throw new Error("Missing confirmation comparison");
    expect(within(after).getByText(storedHex ?? "Missing HEX / 缺色卡")).toBeTruthy();
    expect(screen.getByText(/Current shared display differs/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith(material.materialCode, { colourCode: "ZZ", colourName: "Khaki white", colourHex: undefined }));
  });

  it("opens the same compact editor from swatch and colour text, keeping code read-only", async () => {
    await openMaterial();
    expect(screen.getByRole("dialog", { name: "Edit colour · OMODA · BX" })).toBeTruthy();
    expect(screen.queryByLabelText("Colour code")).toBeNull();
    expect(screen.getByRole("button", { name: "Correct code" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    fireEvent.click(screen.getByText("BX", { exact: true }));
    expect(screen.getByRole("dialog", { name: "Edit colour · OMODA · BX" })).toBeTruthy();
    expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("");
  });

  it("does not adopt a same-name different-code swatch unless explicitly chosen", async () => {
    nameSuggestion();
    const save = mockSaved();
    await openMaterial();
    await screen.findByText("Suggested from OMODA · BW", {}, { timeout: 3000 });
    expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("");
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Reviewed white", colourHex: undefined }));
  });

  it("adopts BW HEX into BX draft, confirmation and request without touching the picker", async () => {
    nameSuggestion();
    const save = mockSaved();
    const move = vi.spyOn(api, "updateColourCode");
    const refreshSelection = await openMaterial();
    fireEvent.click(await screen.findByRole("button", { name: "Use this swatch" }, { timeout: 3000 }));
    expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("#F2F4F8");
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByText("Suggested from OMODA · BW")).toBeTruthy();
    expect(screen.getByText("#F2F4F8")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#F2F4F8" }));
    await waitFor(() => expect(refreshSelection).toHaveBeenCalledTimes(1));
    expect(move).not.toHaveBeenCalled();
  });

  it("submits authoritative same-code HEX auto-filled into the actual draft", async () => {
    vi.mocked(api.lookupOrderGeniusColourHexRule).mockResolvedValue({ brand: "OMODA", colourCode: "BX", status: "complete", colourName: "Khaki white", colourHex: "#ECEEF4", source: "persistent_rule", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] });
    const save = mockSaved();
    await openMaterial();
    await waitFor(() => expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("#ECEEF4"), { timeout: 3000 });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#ECEEF4" }));
  });

  it.each(["name", "swatch", "fillable"])("name-only save preserves HEX even with a %s group condition", async condition => {
    const save = mockSaved();
    await openMaterial(undefined, "#123456", { hasNameConflict: condition === "name", hasSwatchConflict: condition === "swatch", fillableSkuCount: condition === "fillable" ? 1 : 0 });
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByText(/Keep each material's existing swatch/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Reviewed white", colourHex: undefined }));
  });

  it("does not turn name-only editing into auto-loaded HEX synchronization", async () => {
    vi.mocked(api.lookupOrderGeniusColourHexRule).mockResolvedValue({ brand: "OMODA", colourCode: "BX", status: "complete", colourName: "Khaki white", colourHex: "#ECEEF4", source: "persistent_rule", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] });
    const save = mockSaved();
    await openMaterial();
    await waitFor(() => expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("#ECEEF4"), { timeout: 3000 });
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByText(/Keep each material's existing swatch/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Reviewed white", colourHex: undefined }));
  });

  it("explicit adoption synchronizes even when the accepted HEX equals this row", async () => {
    nameSuggestion();
    const save = mockSaved();
    await openMaterial(undefined, "#F2F4F8", { fillableSkuCount: 1 });
    fireEvent.click(await screen.findByRole("button", { name: "Use this swatch" }, { timeout: 3000 }));
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.queryByText(/Keep each material's existing swatch/)).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#F2F4F8" }));
  });

  it("code correction without adopted suggestion confirms and submits Keep", async () => {
    nameSuggestion();
    const move = vi.spyOn(api, "updateColourCode").mockResolvedValue({ materialCode: "T6480J1ZZLX0017", colourCode: "ZZ", colourName: "Reviewed white", colourHex: "#123456", colourCodeConfirmed: true });
    await openMaterial(undefined, "#123456");
    fireEvent.click(screen.getByRole("button", { name: "Correct code" }));
    fireEvent.change(screen.getByLabelText("Colour code"), { target: { value: "ZZ" } });
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    await screen.findByText("Suggested from OMODA · BW", {}, { timeout: 3000 });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByText(/Keep each material's existing swatch/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(move).toHaveBeenCalledWith(material.materialCode, { colourCode: "ZZ", colourName: "Reviewed white", colourHex: undefined }));
  });

  it("keeps unchanged saves read-only and exposes code/material preview only after Correct code", async () => {
    const save = mockSaved();
    const move = vi.spyOn(api, "updateColourCode");
    await openMaterial();
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByText(/No actual changes/)).toBeTruthy();
    expect(save).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: /Edit swatch rule for OMODA BX/ }));
    fireEvent.click(screen.getByRole("button", { name: "Correct code" }));
    fireEvent.change(screen.getByLabelText("Colour code"), { target: { value: "ZZ" } });
    fireEvent.change(screen.getByLabelText("Shared colour name"), { target: { value: "Reviewed white" } });
    await waitFor(() => expect(screen.queryByText(/Checking Brand \+ Code/)).toBeNull(), { timeout: 3000 });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    expect(screen.getByRole("dialog", { name: /Confirm code correction/ })).toBeTruthy();
    expect(screen.getByText("BX → ZZ")).toBeTruthy();
    expect(screen.getByText("T6480J1BXLX0017 → T6480J1ZZLX0017")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Back / 返回编辑" }));
    expect(move).not.toHaveBeenCalled();
  });

  it("retains failed-save input and prevents duplicate confirmation while saving", async () => {
    let rejectSave: (reason: Error) => void = () => undefined;
    const save = vi.spyOn(api, "setOrderGeniusColourHexRuleStandard").mockImplementation(() => new Promise((_resolve, reject) => { rejectSave = reject; }));
    await openMaterial();
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#112233" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    const confirm = screen.getByRole("button", { name: "Confirm save / 确认保存" });
    fireEvent.click(confirm); fireEvent.click(confirm);
    expect(save).toHaveBeenCalledTimes(1);
    rejectSave(new Error("Denied"));
    await screen.findByText(/Save failed; draft kept/);
    fireEvent.click(screen.getByRole("button", { name: "Back / 返回编辑" }));
    expect(screen.getByLabelText("Primary HEX").getAttribute("value")).toBe("#112233");
  });

  it("reports saved-but-refresh-failed and retries only reads including selection", async () => {
    const refreshSelection = vi.fn().mockRejectedValueOnce(new Error("Selection unavailable")).mockResolvedValue(undefined);
    const save = mockSaved();
    await openMaterial(refreshSelection);
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#112233" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await screen.findByText(/Saved, but refresh failed/);
    expect(screen.queryByLabelText("Primary HEX")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: /Re-read saved colour/ }));
    await screen.findByText(/Tables refreshed/);
    expect(save).toHaveBeenCalledTimes(1);
    expect(refreshSelection).toHaveBeenCalledTimes(2);
    expect(api.getBomAdmin).toHaveBeenCalledTimes(3);
    expect(api.getOrderGeniusColourHexRules).toHaveBeenCalledTimes(3);
  });

  it.each(["BOM", "rules"])("treats a failed %s re-read as saved and never repeats its write", async (failedRead) => {
    const save = mockSaved();
    await openMaterial();
    if (failedRead === "BOM") vi.mocked(api.getBomAdmin).mockRejectedValueOnce(new Error("BOM unavailable"));
    else vi.mocked(api.getOrderGeniusColourHexRules).mockRejectedValueOnce(new Error("Rules unavailable"));
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#112233" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await screen.findByText(/Saved, but refresh failed/);
    fireEvent.click(screen.getByRole("button", { name: /Re-read saved colour/ }));
    await screen.findByText(/Tables refreshed/);
    expect(save).toHaveBeenCalledTimes(1);
  });

  it("ignores a late authoritative lookup after the user has entered confirmation", async () => {
    let resolveLookup: (value: Awaited<ReturnType<typeof api.lookupOrderGeniusColourHexRule>>) => void = () => undefined;
    vi.mocked(api.lookupOrderGeniusColourHexRule).mockImplementation(() => new Promise(resolve => { resolveLookup = resolve; }));
    const save = mockSaved();
    await openMaterial();
    await waitFor(() => expect(api.lookupOrderGeniusColourHexRule).toHaveBeenCalled(), { timeout: 3000 });
    fireEvent.change(screen.getByLabelText("Primary HEX"), { target: { value: "#112233" } });
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: "Save" }));
    resolveLookup({ brand: "OMODA", colourCode: "BX", status: "complete", colourName: "Late name", colourHex: "#EEEEEE", source: "persistent_rule", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] });
    await waitFor(() => expect(screen.queryByText("#EEEEEE")).toBeNull());
    fireEvent.click(screen.getByRole("button", { name: "Confirm save / 确认保存" }));
    await waitFor(() => expect(save).toHaveBeenCalledWith({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#112233" }));
  });
});
