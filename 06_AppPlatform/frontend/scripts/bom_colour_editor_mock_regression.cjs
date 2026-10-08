const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const base = process.env.JATO_REGRESSION_BASE_URL || "http://127.0.0.1:4199";
const material = {
  materialCode: "T6480J1BXLX0017", bomTemplate: "T6480J1**LX0017", brand: "OMODA",
  modelName: "OMODA9 SHS", version: "Exclusive-AWD", powertrain: "PHEV", colour: "Khaki white",
  colourCode: "BX", colourHex: null, colourTier: "single", interiorColorName: "Black-Red",
  lifecycleStatus: "active", isActive: true, rowVersion: 1, fobByCountry: { NL: { finalFobEur: 25400 } },
};
const skus = [material, { ...material, materialCode: "T6480J1BXLX0018", bomTemplate: "T6480J1**LX0018", interiorColorName: "Black-Black" }];
const rule = () => ({ brand: "OMODA", colourCode: "BX", colourName: material.colour,
  normalizedColourName: "khaki white", standardColourName: material.colour, standardColourHex: material.colourHex,
  status: material.colourHex ? "complete" : "missing", skuCount: 2, fillableSkuCount: 0,
  placeholderNameSkuCount: 0, missingSwatchSkuCount: material.colourHex ? 0 : 2,
  sampleMaterialCodes: skus.map(sku => sku.materialCode), hasNameConflict: false, hasSwatchConflict: false,
  nameOptions: [{ colourName: material.colour, normalizedColourName: "khaki white", skuCount: 2 }],
  hexOptions: material.colourHex ? [{ colourHex: material.colourHex, skuCount: 2 }] : [],
});
const matrix = () => ({ countryCode: "NL", countryName: "Netherlands", paymentTermCode: "TT", year: 2026,
  totalRows: 2, rows: skus.map(sku => ({ ...sku, fobEur: 25400, editable: true, ttl: 0,
    months: Object.fromEntries(Array.from({ length: 12 }, (_, i) => [String(i + 1), { quantity: 1, rowVersion: 1, isEditable: true, fobEur: 25400 }])) })) });
const writes = [], errors = [], checks = [];
let bomReads = 0, matrixReads = 0, ruleReads = 0, failSave = false, failMatrix = false;
const screenshot = (page, name) => process.env.JATO_REGRESSION_ARTIFACT_DIR
  ? page.screenshot({ path: process.env.JATO_REGRESSION_ARTIFACT_DIR + "/" + name }) : Promise.resolve();
(async () => {
  const browser = await chromium.launch({ channel: "chrome", headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1920, height: 1080 } });
    await page.addInitScript(() => {
      localStorage.setItem("jato_auth_token", "mock-colour-editor");
      localStorage.setItem("jato_user_role", "admin");
      localStorage.setItem("jato_user_name", "mock");
      localStorage.setItem("jato_primary_country", "NL");
    });
    page.on("pageerror", error => errors.push(error.message));
    page.on("dialog", dialog => { errors.push("Unexpected native " + dialog.type()); void dialog.dismiss(); });
    await page.route("**/v1/**", async route => {
      const req = route.request(), path = new URL(req.url()).pathname;
      let body = { items: [], total: 0 };
      if (path.endsWith("/auth/me")) body = { username: "mock", role: "admin", primaryCountry: "NL", secondaryCountries: [], countryCodes: ["NL"] };
      else if (path.endsWith("/bom-admin")) { bomReads++; body = { items: skus, countries: ["NL"] }; }
      else if (path.endsWith("/colour-hex-rules/standard")) {
        const payload = req.postDataJSON(); writes.push({ path, payload });
        if (failSave) { failSave = false; return route.fulfill({ status: 409, contentType: "application/json", body: JSON.stringify({ detail: "Mock save rejected" }) }); }
        assert.equal(payload.colourCode, "BX");
        for (const sku of skus) Object.assign(sku, { colour: payload.colourName, ...(payload.colourHex ? { colourHex: payload.colourHex } : {}) });
        body = { ...payload, updated: 2, materialCodes: skus.map(sku => sku.materialCode) };
      }
      else if (path.endsWith("/colour-hex-rules/lookup")) body = { brand: "OMODA", colourCode: "BX", status: "suggested", colourName: "Khaki white", colourHex: "#F2F4F8",
        source: "name_candidate", hasNameConflict: false, hasSwatchConflict: false,
        nameCandidates: [{ brand: "OMODA", colourCode: "BW", colourName: "Khaki white", colourHex: "#F2F4F8", hasNameConflict: false, hasSwatchConflict: false }] };
      else if (path.endsWith("/colour-hex-rules")) { ruleReads++; body = { items: [rule()], summary: { totalRules: 1, fillable: 0, missing: material.colourHex ? 0 : 1, nameConflict: 0, swatchConflict: 0, complete: material.colourHex ? 1 : 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] } }; }
      else if (path.endsWith("/countries") || path.endsWith("/account-country-options")) body = { items: [{ countryCode: "NL", countryName: "Netherlands", paymentTermCode: "TT", paymentMethod: "TT", lcDays: null }] };
      else if (path.endsWith("/fob-countries")) body = { countries: ["NL"] };
      else if (path.endsWith("/options")) body = { countryCode: "NL", paymentTermCode: "TT", brands: ["OMODA"], models: ["OMODA9 SHS"], powertrains: ["PHEV"], versions: ["Exclusive-AWD"], colours: [material.colour], materialCodes: skus.map(sku => sku.materialCode) };
      else if (path.endsWith("/matrix/batch")) {
        matrixReads++;
        if (failMatrix) return route.fulfill({ status: 500, contentType: "application/json", body: JSON.stringify({ detail: "Mock selection refresh failed" }) });
        body = { errors: {}, matrices: { NL: matrix() } };
      }
      else if (path.endsWith("/order-matrix-plan")) body = { countryCode: "NL", lineItems: [], selectedLineItems: [], remainingLineItems: [], existingLines: [], totals: { selectedQuantity: 0, generatedQuantity: 0, generatedVehicleCount: 0, remainingQuantity: 0, overGeneratedQuantity: 0 } };
      else if (!["GET", "HEAD", "OPTIONS"].includes(req.method())) {
        // Matrix/lookup are read-only POSTs; all other writes are unexpected.
        assert.fail("Unexpected write: " + req.method() + " " + path);
      }
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
    });
    await page.goto(base + "/product/order-genius");
    const trigger = page.getByRole("button", { name: /Filters & Actions/ }).first();
    if (await trigger.getAttribute("aria-expanded") !== "true") await trigger.click();
    await page.getByRole("checkbox", { name: "Group by product", exact: true }).uncheck();
    await page.getByRole("checkbox", { name: "Hide empty rows", exact: true }).uncheck();
    const missingMatrix = page.locator('.ag-cell[col-id="colour"] [aria-label="Missing swatch"]').first();
    await missingMatrix.waitFor();
    assert((await missingMatrix.evaluate(el => el.style.background)).includes("repeating-conic-gradient"));
    await page.getByRole("tab", { name: /BOM ADMIN/i }).click();
    await page.getByText("OMODA OMODA9 SHS", { exact: true }).click();
    const swatch = page.getByRole("button", { name: /Edit swatch rule for/ }).first();
    assert((await swatch.evaluate(el => el.style.background)).includes("repeating-conic-gradient"));
    await swatch.click();
    const editor = page.getByRole("dialog", { name: "Edit colour · OMODA · BX" });
    await editor.waitFor();
    assert.equal(await editor.getByLabel("Primary HEX", { exact: true }).inputValue(), "");
    assert.equal(await editor.getByRole("textbox", { name: "Colour code", exact: true }).count(), 0);
    assert(await editor.getByText(/Affects 2 materials/).isVisible());
    const center = await editor.boundingBox();
    assert(await editor.evaluate((el, pos) => el.contains(document.elementFromPoint(pos.x + pos.width / 2, pos.y + 50)), center), "editor is above outer BOM overlay");
    await editor.getByRole("button", { name: "Use this swatch", exact: true }).waitFor();
    assert.equal(await editor.getByLabel("Primary HEX", { exact: true }).inputValue(), "", "suggestion never silently adopts");
    await screenshot(page, "colour-editor-desktop.png");
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
    assert.equal(writes.length, 0);
    checks.push("Missing checkerboard; explicit suggestion; overlay above BOM; cancel no writes");

    await swatch.click();
    await editor.getByRole("button", { name: "Use this swatch", exact: true }).click();
    assert.equal(await editor.getByLabel("Primary HEX", { exact: true }).inputValue(), "#F2F4F8");
    await editor.getByRole("button", { name: "Save", exact: true }).click();
    const confirmation = page.getByRole("dialog", { name: "Confirm shared colour / 确认共享颜色" });
    await confirmation.waitFor();
    assert(await confirmation.getByText("Suggested from OMODA · BW", { exact: true }).isVisible());
    const beforeReads = { bomReads, matrixReads, ruleReads };
    await confirmation.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await confirmation.waitFor({ state: "hidden" });
    await page.getByText(/Tables refreshed/).waitFor();
    assert.deepEqual(writes[0].payload, { brand: "OMODA", colourCode: "BX", colourName: "Khaki white", colourHex: "#F2F4F8" });
    assert(bomReads > beforeReads.bomReads && matrixReads > beforeReads.matrixReads && ruleReads > beforeReads.ruleReads);
    assert.equal(await swatch.evaluate(el => el.style.background), "rgb(242, 244, 248)");
    const matrixSwatch = page.locator('.ag-cell[col-id="colour"] [aria-label="Khaki white swatch"]').first();
    await matrixSwatch.waitFor({ state: "attached" });
    assert.equal(await matrixSwatch.evaluate(el => el.style.background), "rgb(242, 244, 248)", "selection uses fresh saved HEX too");
    checks.push("Adopt BW HEX without picker; save BX only; BOM and selection show re-read HEX");

    // Colour text and swatch share the exact same editor; code correction is opt-in.
    await page.getByText("BX", { exact: true }).first().click();
    await editor.waitFor();
    await editor.getByRole("button", { name: "Correct code", exact: true }).click();
    await editor.getByRole("textbox", { name: "Colour code", exact: true }).fill("BP");
    await editor.getByLabel("Shared colour name", { exact: true }).fill("Test correction");
    await editor.getByText(/Checking Brand \+ Code/).waitFor({ state: "hidden" });
    await editor.getByRole("button", { name: "Save", exact: true }).click();
    const correction = page.getByRole("dialog", { name: "Confirm code correction / 确认色码纠错" });
    await correction.waitFor();
    assert(await correction.getByText("BX → BP", { exact: true }).isVisible());
    assert(await correction.getByText("T6480J1BXLX0017 → T6480J1BPLX0017", { exact: true }).isVisible());
    await correction.getByRole("button", { name: "Back / 返回编辑", exact: true }).click();
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
    assert.equal(writes.length, 1);
    checks.push("Both entries same editor; explicit code/material preview; correction cancel no write");

    await page.setViewportSize({ width: 390, height: 640 });
    await swatch.click();
    const box = await editor.boundingBox();
    assert(box.x >= 0 && box.x + box.width <= 390 && box.y >= 0 && box.y + box.height <= 640);
    await editor.getByRole("checkbox", { name: /Dual swatch/ }).check();
    await editor.getByLabel("Second HEX", { exact: true }).fill("#222222");
    await editor.getByRole("button", { name: "Use this swatch", exact: true }).waitFor();
    await editor.locator(".confirm-dialog-body").evaluate(el => { el.scrollTop = el.scrollHeight; });
    const save = editor.getByRole("button", { name: "Save", exact: true });
    const saveBox = await save.boundingBox();
    assert(saveBox.y + saveBox.height <= 640, "Save remains in viewport after body scroll");
    await screenshot(page, "colour-editor-phone.png");
    await editor.getByLabel("Primary HEX", { exact: true }).fill("bad");
    assert(await save.isDisabled());
    await editor.getByLabel("Primary HEX", { exact: true }).fill("#F2F4F8");
    await editor.getByLabel("Shared colour name", { exact: true }).fill("New name");
    failSave = true;
    await save.click();
    await confirmation.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await confirmation.getByText(/Save failed; draft kept/).waitFor();
    await confirmation.getByRole("button", { name: "Back / 返回编辑", exact: true }).click();
    assert.equal(await editor.getByLabel("Shared colour name", { exact: true }).inputValue(), "New name");
    assert.equal(await editor.getByLabel("Second HEX", { exact: true }).inputValue(), "#222222");
    checks.push("390x640 fixed footer; body scroll; invalid HEX blocks; rejected save retains dual draft");
    failMatrix = true;
    await save.click();
    await confirmation.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await page.getByText(/Saved, but refresh failed/).waitFor();
    const writeCount = writes.length;
    failMatrix = false;
    await page.getByRole("button", { name: /Re-read saved colour/ }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    assert.equal(writes.length, writeCount, "read retry never repeats write");
    checks.push("Saved+refresh failed distinguished; Retry only re-reads");
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ status: "ok", checks, pageErrors: errors.length, mockWrites: writes.length, realBusinessWrites: 0 }));
  } finally { await browser.close(); }
})().catch(error => { console.error(error.stack); process.exitCode = 1; });
