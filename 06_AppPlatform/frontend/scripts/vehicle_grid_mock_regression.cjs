const { chromium } = require("playwright");
const assert = require("node:assert/strict");
const base = process.env.JATO_REGRESSION_BASE_URL || "http://127.0.0.1:4175";
const PI = "PI-CH-202609-001";
const vehicles = Array.from({ length: 450 }, (_, i) => ({
  vehicleUnitId: String(i), carCode: `CAR-CH-2609-001-L01-${String(i + 1).padStart(4, "0")}`,
  piCode: PI, piLineCode: PI + "-L01", countryCode: "CH", vin: i < 200 ? "LVTDB21B9RD" + String(i).padStart(6, "0") : null,
  brand: "OMODA", modelName: "OMODA5", version: "Comfort-FWD", powertrain: i < 150 ? "ICE" : i < 300 ? "HEV" : "BEV",
  materialCode: "T71506JCLMH" + (i < 150 ? "0007" : i < 300 ? "0008" : "0009"),
  bom: "T71506J**MH" + (i < 150 ? "0007" : i < 300 ? "0008" : "0009"),
  exteriorColorName: "White", interiorColorName: "Black-Black",
  eta: i < 150 ? "2026-10-01" : i < 300 ? "2026-10-02" : null,
  fobEur: 15000, freightEur: null, insuranceEur: null,
  allocationStatus: "unallocated", logisticsStatus: "pending", rowVersion: 1,
}));
const header = { piCode: PI, countryCode: "CH", orderingAccountCode: "CH", marketCountryCodes: ["CH"], status: "draft", orderMonth: "2026-09", rowVersion: 1 };
const detail = () => ({ header, lines: [{ piCode: PI, piLineCode: PI + "-L01", materialCode: "T71506JCLMH0008", quantity: 450, allocations: [] }], vehicles,
  summary: { totalUnits: 450, vinAssigned: 200, vinMissing: 250, readyForPickup: 0, allocated: 0 } });
const saves = [], viewExports = [], errors = [];
let rejectNextSave = true;
const checks = [];
const screenshot = (page, name) => process.env.JATO_REGRESSION_ARTIFACT_DIR
  ? page.screenshot({ path: process.env.JATO_REGRESSION_ARTIFACT_DIR + "/" + name }) : Promise.resolve();
(async () => {
  const browser = await chromium.launch({ channel: "chrome", headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1550, height: 1050 } });
    await page.addInitScript(() => {
      localStorage.setItem("jato_auth_token", "mock-grid-regression");
      localStorage.setItem("jato_user_role", "admin");
      localStorage.setItem("jato_user_name", "mock");
      localStorage.setItem("jato_primary_country", "CH");
    });
    page.on("pageerror", (error) => errors.push(error.message));
    await page.route("**/v1/**", async (route) => {
      const req = route.request(), path = new URL(req.url()).pathname;
      let body = {};
      if (path.endsWith("/auth/me")) body = { username: "mock", role: "admin", primaryCountry: "CH", secondaryCountries: [], countryCodes: ["CH"] };
      else if (path.includes("/account-country-options")) body = { items: [{ countryCode: "CH", countryName: "Switzerland", countryNameZh: "瑞士" }] };
      else if (path.endsWith("/pi-months")) body = { year: 2026, items: [{ month: "2026-09", piCount: 1, vehicleCount: 450 }] };
      else if (path.endsWith("/status-flow")) body = { countryCode: "CH", source: "default", allocation: [], logistics: [] };
      else if (path.endsWith("/coc-library")) body = { total: 450, available: 100, missing: 100, awaitingVin: 250,
        items: vehicles.map((car, index) => ({ carCode: car.carCode, vin: car.vin, status: index < 100 ? "available" : index < 200 ? "missing" : "awaiting_vin" })) };
      else if (path.endsWith("/export")) { viewExports.push(req.postDataJSON()); return route.fulfill({ status: 200, contentType: "application/octet-stream", body: "mock-xlsx" }); }
      else if (path.endsWith("/bulk-update")) {
        const payload = req.postDataJSON();
        if (rejectNextSave) {
          rejectNextSave = false;
          return route.fulfill({ status: 409, contentType: "application/json", body: JSON.stringify({ detail: "Selection changed; nothing saved. Re-read PI / 勾选车辆已变化，未保存，请重新读取 PI" }) });
        }
        saves.push(payload);
        assert.equal(payload.carCodes.length, Object.keys(payload.rowVersions).length);
        for (const car of vehicles.filter((car) => payload.carCodes.includes(car.carCode))) Object.assign(car, payload.fields, { rowVersion: car.rowVersion + 1 });
        body = { updatedUnits: payload.carCodes.length, matchedUnits: payload.carCodes.length };
      }
      else if (path.includes("/vehicles/") && req.method() === "PATCH") { saves.push(req.postDataJSON()); body = vehicles[0]; }
      else if (path.endsWith("/pi/" + PI)) body = detail();
      else if (path.endsWith("/pi")) body = { items: [header], total: 1 };
      else body = { items: [], total: 0, columns: [] };
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
    });
    await page.goto(base + "/product/order-genius/vehicle-allocation");
    await page.getByRole("button", { name: new RegExp("^" + PI) }).click();
    await page.locator('.ag-row').first().waitFor();
    assert(await page.locator(".va-month-browser").evaluate((element) => element.open));
    checks.push("Month open; full PI scope");
    await screenshot(page, "pi-grid-desktop.png");
    const all = page.locator(".ag-header-select-all input");
    await all.check({ force: true });
    await page.getByText("450 selected / 已选", { exact: false }).waitFor();
    await page.getByRole("button", { name: /PI Tools/ }).click();
    await page.getByRole("tab", { name: /Update status/i }).click();
    await page.getByText("450 selected vehicles / 勾选车辆", { exact: true }).waitFor();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    checks.push("Deck status tab and main selected action share explicit targets");
    await page.locator('[aria-label="Next Page"]').click();
    await page.getByText("450 selected / 已选", { exact: false }).waitFor();
    checks.push("Header selection includes 450 rows and survives pagination");
    // Real header drag and resize, not Grid API calls.
    const vinHeader = page.locator('.ag-header-cell[col-id="vin"]');
    const carHeader = page.locator('.ag-header-cell[col-id="carCode"]');
    const vinBox = await vinHeader.locator(".ag-header-cell-text").boundingBox(), carBox = await carHeader.boundingBox();
    await page.mouse.move(vinBox.x + vinBox.width / 2, vinBox.y + vinBox.height / 2);
    await page.mouse.down();
    await page.mouse.move(vinBox.x - 8, vinBox.y + vinBox.height / 2, { steps: 5 });
    await page.waitForTimeout(200);
    await page.mouse.move(carBox.x + 80, vinBox.y + vinBox.height / 2, { steps: 30 });
    await page.waitForTimeout(250); await page.mouse.up();
    const edge = carHeader.locator(".ag-header-cell-resize");
    const edgeBox = await edge.boundingBox();
    await page.mouse.move(edgeBox.x + edgeBox.width / 2, edgeBox.y + 12);
    await page.mouse.down(); await page.mouse.move(edgeBox.x + 60, edgeBox.y + 12, { steps: 12 }); await page.mouse.up();
    assert((await carHeader.boundingBox()).width > carBox.width + 30);
    await page.getByText("450 selected / 已选", { exact: false }).waitFor();
    checks.push("Native drag/resize preserves selection");
    await page.getByLabel("Show selected / 只看勾选").check();
    await page.locator(".ag-paging-panel").waitFor({ state: "hidden" });
    await page.getByRole("button", { name: /Invert selection/ }).click();
    await page.getByText("0 selected / 已选", { exact: false }).waitFor();
    await page.locator(".ag-row").first().waitFor({ state: "hidden" });
    await page.getByRole("button", { name: /Invert selection/ }).click();
    await page.getByText("450 selected / 已选", { exact: false }).waitFor();
    checks.push("Selected-only is continuous, empty does not fallback; invert uses original scope");
    await page.getByLabel("Show selected / 只看勾选").uncheck();
    // Use a real header filter on the complete data set.
    const material = page.locator('.ag-header-cell[col-id="materialCode"]');
    await material.locator(".ag-header-cell-filter-button").click({ force: true });
    await page.locator('.ag-filter-body input[type="text"]').first().fill("0008");
    await page.getByText("150 filtered / 筛选", { exact: false }).waitFor();
    await page.getByText("0 selected / 已选", { exact: false }).waitFor();
    await page.keyboard.press("Escape");
    await all.check({ force: true });
    await page.getByText("150 selected / 已选", { exact: false }).waitFor();
    checks.push("Column filter covers full PI and clears old selection");
    await page.getByRole("button", { name: /Update selected status/ }).click();
    await page.getByLabel("Freight / 运费 (EUR)", { exact: true }).fill("0");
    await page.getByLabel("Actual departure", { exact: true }).fill("2026-10-01");
    await page.getByLabel("Dealer name", { exact: true }).fill("Mock dealer");
    const save = page.getByRole("button", { name: "Save changes / 保存修改" });
    assert(await save.isVisible());
    await screenshot(page, "pi-grid-editor-desktop.png");
    await save.click();
    await page.getByText(/Selection changed; nothing saved/).first().waitFor();
    assert.equal(await page.getByLabel("Freight / 运费 (EUR)", { exact: true }).inputValue(), "0");
    assert.equal(await page.getByLabel("Dealer name", { exact: true }).inputValue(), "Mock dealer");
    await page.getByText("150 selected / 已选", { exact: false }).waitFor();
    checks.push("Version rejection and busy controls preserve checked targets and unsaved inputs");
    await page.getByRole("button", { name: "Re-read PI", exact: true }).click();
    await page.getByText("0 selected / 已选", { exact: false }).waitFor();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await all.check({ force: true });
    await page.getByText("150 selected / 已选", { exact: false }).waitFor();
    await page.getByRole("button", { name: /Update selected status/ }).click();
    await page.getByLabel("Freight / 运费 (EUR)", { exact: true }).fill("0");
    await page.getByLabel("Actual departure", { exact: true }).fill("2026-10-01");
    await page.getByLabel("Dealer name", { exact: true }).fill("Mock dealer");
    await save.click();
    await page.getByText("Updated 150 vehicles", { exact: false }).waitFor();
    assert.equal(saves[0].carCodes.length, 150);
    assert.deepEqual(Object.keys(saves[0].fields).sort(), ["actualDepartureDate", "dealerName", "freightEur"]);
    assert(!saves[0].fields.vin);
    assert((await carHeader.boundingBox()).width > carBox.width + 30);
    checks.push("Fixed footer saves exact checked targets, versions and dirty fields");
    await page.getByRole("tab", { name: /View/ }).click();
    await page.getByRole("checkbox", { name: "Interior", exact: true }).uncheck();
    await Promise.all([
      page.waitForResponse((response) => response.url().endsWith("/export")),
      page.getByRole("button", { name: /Export current view/ }).click(),
    ]);
    assert.equal(viewExports[0].carCodes.length, 150);
    assert(!viewExports[0].columns.includes("interiorColorName"));
    assert(viewExports[0].columns.indexOf("vin") < viewExports[0].columns.indexOf("carCode"));
    checks.push("Export covers complete filtered set with dragged order and hidden column");
    await page.getByRole("button", { name: "Close", exact: true }).click();
    // A subset, not the 100-row page or whole filtered PI, is exported in selected-only mode.
    await page.locator('.ag-row .ag-selection-checkbox input').first().check({ force: true });
    await page.getByLabel("Show selected / 只看勾选").check();
    await page.getByText("1 filtered / 筛选", { exact: false }).waitFor();
    await page.getByRole("button", { name: /PI Tools/ }).click();
    await page.getByRole("tab", { name: /View/ }).click();
    await Promise.all([page.waitForResponse((response) => response.url().endsWith("/export")), page.getByRole("button", { name: /Export current view/ }).click()]);
    assert.equal(viewExports[1].carCodes.length, 1);
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByLabel("Show selected / 只看勾选").uncheck();
    await page.locator('.ag-header-expand-icon-expanded:visible').first().click();
    await vinHeader.waitFor({ state: "hidden" });
    await page.getByRole("button", { name: /PI Tools/ }).click();
    await page.getByRole("tab", { name: /View/ }).click();
    await Promise.all([page.waitForResponse((response) => response.url().endsWith("/export")), page.getByRole("button", { name: /Export current view/ }).click()]);
    assert.equal(viewExports[2].carCodes.length, 150);
    assert(!viewExports[2].columns.includes("vin"));
    assert(viewExports[2].columns.includes("carCode"));
    checks.push("Selected-only exports exact subset; collapsed group omits hidden children");
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await page.locator('.ag-header-expand-icon-collapsed:visible').first().click();
    await page.locator('.ag-header-cell[col-id="countryCode"] .ag-header-cell-filter-button').click({ force: true });
    await page.locator(".ag-filter .command-select-trigger").click();
    await page.getByRole("option", { name: /CH/ }).click();
    await page.getByText("0 filtered / 筛选", { exact: false }).waitFor();
    await page.getByText("0 selected / 已选", { exact: false }).waitFor();
    await page.locator(".ag-filter .command-select-trigger").click();
    await page.getByRole("button", { name: /All values/ }).click();
    await page.getByText("150 filtered / 筛选", { exact: false }).waitFor();
    await page.keyboard.press("Escape");
    checks.push("Community value-list filter reuses existing selector and clears old targets");
    const scroll = page.locator(".ag-body-horizontal-scroll-viewport");
    await scroll.evaluate((element) => { element.scrollLeft = 500; });
    await page.locator(".ag-body-viewport").evaluate((element) => { element.scrollTop = 850; });
    await screenshot(page, "pi-grid-scrolled.png");
    // Column header and body cannot paint into each other.
    const headerBottom = await page.locator(".ag-header").evaluate((element) => element.getBoundingClientRect().bottom);
    const bodyTop = await page.locator(".ag-body").evaluate((element) => element.getBoundingClientRect().top);
    assert(bodyTop >= headerBottom - 1);
    await scroll.evaluate((element) => { element.scrollLeft = 0; });
    await page.setViewportSize({ width: 700, height: 950 });
    await screenshot(page, "pi-grid-narrow.png");
    await page.locator('.ag-row .ag-cell[col-id="carCode"]').first().click();
    await page.getByRole("button", { name: "Save changes / 保存修改" }).waitFor();
    assert(await page.getByLabel("VIN", { exact: true }).isVisible());
    const panel = page.locator(".vehicle-allocation-tool-panel");
    assert((await panel.boundingBox()).height <= 950);
    await screenshot(page, "pi-grid-editor-narrow.png");
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await page.setViewportSize({ width: 1550, height: 1050 });
    async function resetView() {
      await page.getByRole("button", { name: /PI Tools/ }).click();
      await page.getByRole("tab", { name: /View/ }).click();
      await page.getByRole("button", { name: /Reset columns & filters/ }).click();
      await page.getByText("450 filtered / 筛选", { exact: false }).waitFor();
    }
    await resetView();
    await page.getByLabel("VIN batch search / VIN 批量搜索").fill(vehicles.slice(0, 2).map((car) => car.vin).join("\n"));
    await page.getByRole("button", { name: "Search VIN batch / 搜索 VIN" }).click();
    await page.getByText("2 filtered / 筛选", { exact: false }).waitFor();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await carHeader.locator(".ag-header-cell-filter-button").click({ force: true });
    await page.locator('.ag-filter-body input[type="text"]').first().fill("0001");
    await page.getByText("1 filtered / 筛选", { exact: false }).waitFor();
    await page.keyboard.press("Escape");
    await page.getByRole("button", { name: /PI Tools/ }).click();
    await page.getByRole("button", { name: /Select matches/ }).click();
    await page.getByText("1 selected / 已选", { exact: false }).waitFor();
    await page.getByText(/2 matched · 1 selected · 1 excluded by filters/).waitFor();
    checks.push("Select VIN matches intersects ordinary header filters, not all matches");
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await resetView();
    await page.getByLabel("VIN batch search / VIN 批量搜索").fill(vehicles.slice(0, 200).map((car) => car.vin).join("\n"));
    await page.getByRole("button", { name: "Search VIN batch / 搜索 VIN" }).click();
    await page.getByText("200 filtered / 筛选", { exact: false }).waitFor();
    await page.getByRole("button", { name: /Select matches/ }).click();
    await page.getByText("200 selected / 已选", { exact: false }).waitFor();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByLabel("Show selected / 只看勾选").check();
    await page.getByRole("button", { name: /Invert selection/ }).click();
    await page.getByText("0 selected / 已选", { exact: false }).waitFor();
    await page.getByRole("button", { name: /PI Tools/ }).click();
    await page.getByRole("button", { name: /Select matches/ }).click();
    await page.getByText("200 selected / 已选", { exact: false }).waitFor();
    checks.push("Visible VIN matches select across pages; selected-only does not shrink original scope");
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await page.getByLabel("Show selected / 只看勾选").uncheck();
    await resetView();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await scroll.evaluate((element) => { element.scrollLeft = 2600; });
    await page.locator('.ag-header-cell[col-id="eta"] .ag-header-cell-filter-button').click({ force: true });
    await page.locator('.ag-filter-body input[type="date"]').first().fill("2026-10-02");
    await page.getByText("150 filtered / 筛选", { exact: false }).waitFor();
    await page.getByLabel("Filtering operator", { exact: true }).first().click();
    await page.getByRole("option", { name: "Between", exact: true }).click();
    await page.locator('.ag-filter-body input[type="date"]').nth(0).fill("2026-09-30");
    await page.locator('.ag-filter-body input[type="date"]').nth(1).fill("2026-10-03");
    await page.getByText("300 filtered / 筛选", { exact: false }).waitFor();
    await page.getByLabel("Filtering operator", { exact: true }).first().click();
    await page.getByRole("option", { name: "Blank", exact: true }).click();
    await page.getByText("150 filtered / 筛选", { exact: false }).waitFor();
    await page.keyboard.press("Escape");
    await page.getByText("450 units / 台", { exact: true }).waitFor();
    checks.push("Native date comparison, range and blank filters; whole-PI summary unchanged");
    await resetView();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await all.check({ force: true });
    await page.getByRole("button", { name: "COC library / 在线库", exact: true }).click();
    await page.getByRole("button", { name: "Search library / 在库里查找" }).click();
    await page.getByText(/COC PDF 100\/450/).first().waitFor();
    await page.getByText("450 selected / 已选", { exact: false }).waitFor();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await scroll.evaluate((element) => { element.scrollLeft = 1500; });
    await page.locator('.ag-header-cell[col-id="cocPdf"] .ag-header-cell-filter-button').click({ force: true });
    await page.locator(".ag-filter .command-select-trigger").click();
    await page.getByRole("option", { name: /available/ }).click();
    await page.getByRole("option", { name: /awaiting_vin/ }).click();
    await page.getByText("100 filtered / 筛选", { exact: false }).waitFor();
    await page.keyboard.press("Escape"); await page.keyboard.press("Escape");
    await page.getByRole("button", { name: "PDF ↓", exact: true }).first().waitFor({ state: "hidden" });
    assert((await page.locator('.ag-cell[col-id="cocPdf"]').first().innerText()).includes("Missing"));
    checks.push("COC native filter uses real library status, matching visible PDF/missing cells");
    await resetView();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    vehicles.forEach((car, index) => { if (index >= 225) car.countryCode = "SE"; });
    header.marketCountryCodes = ["CH", "SE"];
    await page.getByRole("button", { name: new RegExp("^" + PI) }).click();
    await page.getByText("Loading PI / 正在读取 PI…", { exact: true }).waitFor({ state: "hidden" });
    await page.getByText("450 filtered / 筛选", { exact: false }).waitFor();
    await page.getByText("450 units / 台", { exact: true }).waitFor();
    // Let Grid's 150 ms scroll-partner debounce settle after hiding the COC column.
    await page.waitForTimeout(200);
    await page.locator(".ag-center-cols-viewport").evaluate((element) => { element.scrollLeft = 0; });
    await screenshot(page, "pi-grid-mixed-market.png");
    await page.locator('.ag-header-cell[col-id="countryCode"] .ag-header-cell-filter-button').click({ force: true });
    await page.locator(".ag-filter .command-select-trigger").click();
    await page.getByRole("option", { name: /SE/ }).click();
    await page.getByText("225 filtered / 筛选", { exact: false }).waitFor();
    await page.getByText("450 units / 台", { exact: true }).waitFor();
    checks.push("Mixed-market PI defaults to all 450 rows; ordinary country filter narrows view, not summary");
    assert.equal(errors.length, 0, errors.join("\n"));
    checks.push("Horizontal/vertical scroll and narrow viewport; no browser errors");
    console.log(JSON.stringify({ passed: checks, saves: saves.length, viewExports: viewExports.length, browserErrors: errors.length }, null, 2));
  } finally { await browser.close(); }
})().catch((error) => { console.error(error.stack); process.exitCode = 1; });
