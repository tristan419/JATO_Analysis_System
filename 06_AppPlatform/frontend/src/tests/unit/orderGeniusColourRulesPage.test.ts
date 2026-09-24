import { describe, expect, it } from "vitest";
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
    expect(pageSource).toContain("No reusable Brand + Code rule");
    expect(pageSource).toContain("Wait for the Brand + Code rule check to finish.");
    expect(pageSource).toContain("This code cannot be auto-filled: enter a colour name before saving it.");
    expect(pageSource).toContain("const BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS = 1200;");
    expect(pageSource).toContain("}, BOM_ADMIN_COLOUR_LOOKUP_DELAY_MS);");
    expect(pageSource).toContain("colourCodeEditorTargetKey");
    expect(pageSource).toContain("addColourEditorTargetKey");
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
    expect(pageSource).toContain("if (latestLoadKeyRef.current !== loadKey) return;");
    expect(pageSource).toContain("void load(pendingLoadKey);");
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
});
