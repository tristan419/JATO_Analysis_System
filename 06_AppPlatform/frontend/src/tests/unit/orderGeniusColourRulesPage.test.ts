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
    expect(pageSource).toContain("Several existing colours match this name. Choose one below");
    expect(pageSource).toContain("Name match selected one existing colour");
    expect(pageSource).toContain("Approximate swatch generated from the colour name");
    expect(pageSource).toContain('"generated_from_name"');
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

  const material = { materialCode: "T6480J1BXLX0017", bomTemplate: "T6480J1**LX0017", brand: "OMODA", modelName: "OMODA9 SHS", version: "Exclusive-AWD", powertrain: "PHEV", colour: "Khaki white", colourCode: "BX", colourHex: null, colourTier: "single", interiorColorName: "Black-Red", lifecycleStatus: "active", fobByCountry: { NL: { finalFobEur: 25400 } }, rowVersion: 1 };
  const bxRule: ColourHexRule = { ...conflict, colourCode: "BX", colourName: "Khaki white", standardColourName: "Khaki white", normalizedColourName: "khaki white", skuCount: 2, missingSwatchSkuCount: 2, status: "missing", hasNameConflict: false, nameOptions: [{ colourName: "Khaki white", normalizedColourName: "khaki white", skuCount: 2 }] };

  async function openMaterial(onFobChanged = vi.fn().mockResolvedValue(undefined)) {
    vi.spyOn(api, "getBomAdmin").mockResolvedValue({ items: [material], countries: ["NL"] });
    vi.spyOn(api, "getOrderGeniusColourHexRules").mockResolvedValue({ items: [bxRule], summary: { totalRules: 1, fillable: 0, missing: 1, nameConflict: 0, swatchConflict: 0, complete: 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] } });
    render(createElement(BomAdminPanel, { onFobChanged }));
    fireEvent.click(await screen.findByText("OMODA OMODA9 SHS"));
    fireEvent.click(await screen.findByRole("button", { name: /Edit swatch rule for OMODA BX/ }));
    return onFobChanged;
  }

  function mockSaved() {
    return vi.spyOn(api, "setOrderGeniusColourHexRuleStandard").mockResolvedValue({ brand: "OMODA", colourCode: "BX", colourName: "Khaki white", normalizedColourName: "khaki white", colourHex: "#F2F4F8", updated: 2, materialCodes: [material.materialCode, "T6480J1BXLX0018"] });
  }

  function nameSuggestion() {
    vi.mocked(api.lookupOrderGeniusColourHexRule).mockResolvedValue({ brand: "OMODA", colourCode: "BX", status: "complete", colourName: "Khaki white", colourHex: "#F2F4F8", source: "name_candidate", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [{ brand: "OMODA", colourCode: "BW", colourName: "Khaki white", colourHex: "#F2F4F8", status: "complete", hasNameConflict: false, hasSwatchConflict: false }] });
  }

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
