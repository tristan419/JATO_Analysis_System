const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const base = process.env.JATO_REGRESSION_BASE_URL || "http://127.0.0.1:4199";
const material = {
  materialCode: "T6480J1BXLX0017", bomTemplate: "T6480J1**LX0017", brand: "OMODA",
  modelName: "OMODA9 SHS", version: "Exclusive-AWD", powertrain: "PHEV", colour: "Khaki white",
  colourCode: "BX", colourHex: null, storedColourHex: null, colourTier: "single", interiorColorName: "Black-Red",
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
let ruleOverrides = {}, fillPreview = null, lookupResponse = null;
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
        for (const sku of skus) Object.assign(sku, { colour: payload.colourName, ...(payload.colourHex ? { colourHex: payload.colourHex, storedColourHex: payload.colourHex } : {}) });
        if (payload.confirmMaterialCode) skus.find(sku => sku.materialCode === payload.confirmMaterialCode).colourCodeConfirmed = true;
        body = { ...payload, updated: 2, materialCodes: skus.map(sku => sku.materialCode) };
      }
      else if (path.endsWith("/colour-hex-rules/lookup")) body = lookupResponse ?? { brand: "OMODA", colourCode: "BX", status: "missing", colourName: null, colourHex: null,
        source: "name_candidates", hasNameConflict: false, hasSwatchConflict: false,
        nameCandidates: [{ brand: "OMODA", colourCode: "BW", colourName: "Khaki white", colourHex: "#F2F4F8", hasNameConflict: false, hasSwatchConflict: false }] };
      else if (path.endsWith("/colour-hex-rules/preview")) body = fillPreview;
      else if (path.endsWith("/colour-hex-rules/apply")) {
        const payload = req.postDataJSON(); writes.push({ path, payload });
        assert.deepEqual(payload.materialCodes, [skus[1].materialCode]);
        Object.assign(skus[1], { colour: "Reviewed white", colourHex: "#123456", storedColourHex: "#123456" });
        ruleOverrides = {};
        body = { updated: 1, unchanged: 1, rulesCreated: 0, generatedRules: 0, conflicts: 0, missingRules: 0, materialCodes: payload.materialCodes, items: fillPreview.items, fingerprint: fillPreview.fingerprint };
      }
      else if (/\/material-skus\/[^/]+\/confirm-colour-code$/.test(path)) {
        writes.push({ path, payload: null });
        skus.find(sku => path.includes(sku.materialCode)).colourCodeConfirmed = true;
        body = { materialCode: material.materialCode, colourCodeConfirmed: true };
      }
      else if (/\/material-skus\/[^/]+\/colour-code$/.test(path)) {
        const payload = req.postDataJSON(); writes.push({ path, payload });
        assert.equal(payload.colourCode, "ZZ");
        assert(!("colourHex" in payload), "unadopted correction suggestion is never sent");
        Object.assign(material, { materialCode: "T6480J1ZZLX0017", colour: payload.colourName, colourCode: "ZZ", colourHex: material.storedColourHex });
        body = { materialCode: material.materialCode, colourName: payload.colourName, colourCode: "ZZ", colourHex: material.storedColourHex, colourCodeConfirmed: true };
      }
      else if (path.endsWith("/colour-hex-rules")) { ruleReads++; body = { items: [{ ...rule(), ...ruleOverrides }], summary: { totalRules: 1, fillable: 0, missing: material.colourHex ? 0 : 1, nameConflict: 0, swatchConflict: 0, complete: material.colourHex ? 1 : 0, fillableSkus: 0, invalidIdentitySkuCount: 0, invalidIdentitySampleMaterialCodes: [] } }; }
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
    await page.evaluate(() => {
      document.querySelectorAll(".candidate-environment-banner").forEach(el => el.remove());
      const banner = document.createElement("aside");
      banner.className = "candidate-environment-banner";
      banner.style.height = "120px";
      banner.textContent = "Mock multi-line Candidate banner";
      document.body.appendChild(banner);
    });
    await swatch.click();
    const box = await editor.boundingBox();
    assert(box.x >= 0 && box.x + box.width <= 390 && box.y >= 120 && box.y + box.height <= 640);
    await page.locator(".candidate-environment-banner").evaluate(el => { el.style.height = "160px"; });
    await page.waitForFunction(() => document.querySelector(".bom-colour-confirmation")?.style.getPropertyValue("--confirm-banner-height") === "160px");
    const title = editor.locator("h2");
    const titleBox = await title.boundingBox();
    assert(titleBox.y >= 160);
    assert(await title.evaluate(el => { const r = el.getBoundingClientRect(); return el.contains(document.elementFromPoint(r.x + 10, r.y + 5)); }));
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
    checks.push("390x640 tracks 120→160px Candidate banner; uncovered title; fixed footer; body scroll; invalid HEX blocks; rejected save retains dual draft");
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

    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.locator(".candidate-environment-banner").evaluate(el => el.remove());
    Object.assign(material, { colour: "Khaki white", colourHex: "#123456", storedColourHex: "#123456" });
    Object.assign(skus[1], { colour: "Khaki white", colourHex: "#654321", storedColourHex: "#654321" });
    ruleOverrides = { hasSwatchConflict: true, status: "swatch_conflict", standardColourHex: null };
    // Reload uses mock business reads only; no real API writes.
    await page.reload();
    const reopenedTrigger = page.getByRole("button", { name: /Filters & Actions/ }).first();
    if (await reopenedTrigger.getAttribute("aria-expanded") !== "true") await reopenedTrigger.click();
    await page.getByRole("tab", { name: /BOM ADMIN/i }).click();
    await page.getByText("OMODA OMODA9 SHS", { exact: true }).waitFor();
    if (await swatch.count() === 0) await page.getByText("OMODA OMODA9 SHS", { exact: true }).click();
    await swatch.click();
    await editor.getByLabel("Shared colour name", { exact: true }).fill("Reviewed white");
    await editor.getByRole("button", { name: "Save", exact: true }).click();
    await confirmation.getByText(/Keep each material's existing swatch/).waitFor();
    await confirmation.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    assert(!("colourHex" in writes.at(-1).payload));
    assert.equal(skus[1].colourHex, "#654321");
    checks.push("Name-only in HEX conflict omits HEX and preserves both raw swatches");

    ruleOverrides = {};
    await swatch.click();
    await editor.getByRole("button", { name: "Correct code", exact: true }).click();
    await editor.getByLabel("Colour code", { exact: true }).fill("ZZ");
    await editor.getByLabel("Shared colour name", { exact: true }).fill("Khaki white");
    await editor.getByRole("button", { name: "Use this swatch", exact: true }).waitFor();
    assert.equal(await editor.getByLabel("Primary HEX", { exact: true }).inputValue(), "");
    await editor.getByRole("button", { name: "Save", exact: true }).click();
    await correction.getByText(/Keep each material's existing swatch/).waitFor();
    await correction.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    Object.assign(material, { materialCode: "T6480J1BXLX0017", colourCode: "BX" });
    checks.push("Correction without adoption confirms Keep and submits no HEX");

    Object.assign(skus[1], { colour: "BX", colourHex: null });
    fillPreview = { rules: [{ brand: "OMODA", colourCode: "BX", colourName: "Reviewed white", colourHex: "#123456", source: "persistent_rule", skuCount: 2, nameOptions: [] }],
      items: [{ materialCode: skus[1].materialCode, brand: "OMODA", colourCode: "BX", oldColourName: "BX", newColourName: "Reviewed white", oldColourHex: null, newColourHex: "#123456" }],
      total: 1, ruleCount: 1, generatedRuleCount: 0, unresolvedRuleCount: 0, unresolvedConflictCount: 0, fingerprint: "mock-fill" };
    await page.getByRole("button", { name: "Edit tools", exact: true }).click();
    await page.getByRole("button", { name: "Preview shared swatch standards", exact: true }).click();
    const preview = page.getByRole("dialog", { name: "Colour rule fill preview" });
    await preview.getByText(/1 names \/ 名称 · 1 swatches/).waitFor();
    assert(await preview.getByText(/Confirmed same-code standard/).isVisible());
    assert(await preview.getByText(/Missing HEX.*#123456/).isVisible());
    await screenshot(page, "colour-fill-preview.png");
    await preview.getByRole("button", { name: "Confirm 1 shared standards", exact: true }).click();
    await preview.getByText(/1 materials filled/).waitFor();
    assert.equal(skus[0].colourHex, "#123456");
    assert.equal(skus[1].colourHex, "#123456");
    checks.push("Existing Preview shows per-material missing name+HEX, source and counts; Apply exact targets; donor unchanged");
    await preview.getByRole("button", { name: "Close", exact: true }).click();

    Object.assign(material, { colour: "Khaki white", colourHex: "#112233", storedColourHex: "#112233" });
    Object.assign(skus[1], { colour: "Khaki white", colourHex: null, storedColourHex: null });
    ruleOverrides = { status: "fillable", fillableSkuCount: 1, missingSwatchSkuCount: 1 };
    await page.reload();
    const showBom = async () => {
      const deck = page.getByRole("button", { name: /Filters & Actions/ }).first();
      if (await deck.getAttribute("aria-expanded") !== "true") await deck.click();
      await page.getByRole("tab", { name: /BOM ADMIN/i }).click();
      await page.getByText("OMODA OMODA9 SHS", { exact: true }).waitFor();
      if (await swatch.count() === 0) await page.getByText("OMODA OMODA9 SHS", { exact: true }).click();
    };
    await showBom();
    // BOM tools restore their open state after reload.
    if (await page.locator(".bom-admin-toolbar.is-tools-open").count() === 0) {
      await page.getByRole("button", { name: "Edit tools", exact: true }).click();
    }
    await page.getByRole("button", { name: /Can fill/ }).click();
    await page.getByRole("dialog", { name: "Colour rule details" }).getByRole("button", { name: "Edit colour", exact: true }).click();
    await editor.getByLabel("Shared colour name", { exact: true }).fill("List-only rename");
    await editor.getByRole("button", { name: "Save", exact: true }).click();
    await confirmation.getByText(/Keep each material's existing swatch/).waitFor();
    await confirmation.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    assert(!("colourHex" in writes.at(-1).payload));
    assert.equal(skus[1].storedColourHex, null);
    checks.push("Ordinary rule-list name-only save omits HEX; other missing rows remain missing");

    Object.assign(material, { colour: "Khaki white", colourHex: null, storedColourHex: null });
    Object.assign(skus[1], { colour: "Khaki white", colourHex: "#112233", storedColourHex: "#112233" });
    ruleOverrides = { status: "fillable", fillableSkuCount: 1, standardColourHex: "#112233" };
    lookupResponse = { brand: "OMODA", colourCode: "BX", status: "complete", colourName: "Khaki white", colourHex: "#112233", source: "persistent_rule", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] };
    await page.reload(); await showBom(); await swatch.click();
    await editor.getByLabel("Primary HEX", { exact: true }).waitFor();
    await page.waitForFunction(() => document.querySelector('[aria-label="Primary HEX"]').value === "#112233");
    await editor.getByRole("button", { name: "Save", exact: true }).click();
    await confirmation.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    assert.equal(writes.at(-1).payload.colourHex, "#112233");
    checks.push("Unique same-code HEX loads and saves without touching picker");
    lookupResponse = null;

    for (const storedHex of [null, "#445566"]) {
      Object.assign(material, { materialCode: "T6480J1BXLX0017", colourCode: "BX", colour: "Khaki white", colourHex: "#112233", storedColourHex: storedHex });
      ruleOverrides = {};
      await page.setViewportSize({ width: storedHex ? 1920 : 390, height: storedHex ? 1080 : 640 });
      await page.reload(); await showBom(); await swatch.click();
      await editor.getByRole("button", { name: "Correct code", exact: true }).click();
      await editor.getByLabel("Colour code", { exact: true }).fill("ZZ");
      await editor.getByLabel("Shared colour name", { exact: true }).fill("Khaki white");
      await editor.getByRole("button", { name: "Use this swatch", exact: true }).waitFor();
      await editor.getByRole("button", { name: "Save", exact: true }).click();
      const after = correction.locator('.bom-colour-edit-comparison > div').nth(1);
      assert.equal(await after.locator("code").textContent(), storedHex ?? "Missing HEX / 缺色卡");
      await correction.getByText(/Current shared display differs/).waitFor();
      await screenshot(page, storedHex ? "correction-stored-hex-desktop.png" : "correction-missing-hex-phone.png");
      await correction.getByRole("button", { name: "Confirm save / 确认保存", exact: true }).click();
      await page.getByText(/Tables refreshed/).waitFor();
      assert(!("colourHex" in writes.at(-1).payload));
      assert.equal(material.colourHex, storedHex);
    }
    checks.push("Correction Keep preview matches reread stored HEX, including null; desktop and narrow screen");

    Object.assign(material, { materialCode: "T6480J1BXLX0017", colourCode: "BX", colour: "", colourHex: null, storedColourHex: null, colourCodeConfirmed: false });
    skus[1].colourCodeConfirmed = false;
    ruleOverrides = {};
    lookupResponse = { brand: "OMODA", colourCode: "BX", status: "missing", colourName: null, colourHex: null, source: "none", hasNameConflict: false, hasSwatchConflict: false, nameCandidates: [] };
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.reload(); await showBom(); await swatch.click();
    assert.equal(await editor.getByLabel("Shared colour name", { exact: true }).inputValue(), "");
    await editor.getByRole("button", { name: "Cancel", exact: true }).click();
    await page.getByTitle("Unconfirmed colour code — click to edit and confirm", { exact: true }).first().click();
    await editor.waitFor();
    await screenshot(page, "missing-name-confirm-code.png");
    const countBeforeConfirm = writes.length;
    await editor.getByRole("button", { name: "Confirm code", exact: true }).click();
    const codeConfirmation = page.getByRole("dialog", { name: "Confirm code / 确认色码" });
    await codeConfirmation.getByRole("button", { name: "Confirm code", exact: true }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    assert.equal(writes.length, countBeforeConfirm + 1);
    assert(writes.at(-1).path.endsWith("/confirm-colour-code"));
    assert.equal(material.colourCodeConfirmed, true);
    assert.equal(skus[1].colourCodeConfirmed, false);
    assert.equal(material.colour, "");
    checks.push("Missing-name chip/text both open; unchanged Confirm code only confirms current material, no shared write");

    material.colourCodeConfirmed = false;
    await page.reload(); await showBom(); await swatch.click();
    await editor.getByLabel("Shared colour name", { exact: true }).fill("Reviewed white");
    await editor.getByLabel("Primary HEX", { exact: true }).fill("#334455");
    const beforeAtomic = writes.length;
    await editor.getByRole("button", { name: "Save & confirm code", exact: true }).click();
    await page.getByRole("dialog", { name: "Confirm shared colour / 确认共享颜色" }).getByRole("button", { name: "Save & confirm code", exact: true }).click();
    await page.getByText(/Tables refreshed/).waitFor();
    assert.equal(writes.length, beforeAtomic + 1);
    assert(writes.at(-1).path.endsWith("/colour-hex-rules/standard"));
    assert.equal(writes.at(-1).payload.confirmMaterialCode, material.materialCode);
    assert.equal(material.colourCodeConfirmed, true);
    assert.equal(skus[1].colourCodeConfirmed, false);
    checks.push("Changed unconfirmed Save & confirm code uses one shared request with current material identity");
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ status: "ok", checks, pageErrors: errors.length, mockWrites: writes.length, realBusinessWrites: 0 }));
  } finally { await browser.close(); }
})().catch(error => { console.error(error.stack); process.exitCode = 1; });
