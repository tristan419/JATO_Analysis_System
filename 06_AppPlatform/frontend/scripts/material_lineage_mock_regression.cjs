// Synthetic APIs: layout/interaction evidence, not live authorization or BOM verification.
const { chromium } = require('playwright');
const assert = require('node:assert/strict');
const path = require('node:path');
const fs = require('node:fs');
const base = process.env.JATO_BASE_URL || 'http://127.0.0.1:5183';
const output = process.env.JATO_LINEAGE_OUTPUT;
const node = (bomTemplate, from, to) => ({ bomTemplate, brand: 'JAECOO', modelName: 'JAECOO5 BEV', version: 'Select-FWD', powertrain: 'BEV', materialCodes: [bomTemplate + '-BW'], lifecycleStatus: 'active', effectiveFrom: from, effectiveTo: to, notes: ['BOM note'], issues: [], quoteDate: '2026-10-13', quotedSkuCount: 1, quoteReasons: [] });
const nodes = [node('OLD-A', '2026-09-01', '2026-10-29'), node('NEW-A', '2026-10-30', null), node('OLD-B', '2026-09-01', '2026-10-31'), node('NEW-B', '2026-10-15', '2026-12-01'), node('UNLINKED', null, null)];
const links = [{ from: 'OLD-A', to: 'NEW-A', note: 'Battery upgrade' }, { from: 'OLD-B', to: 'NEW-B' }];
(async () => {
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  const checks = [];
  try {
    for (const [name, width, height] of [['1080p', 1920, 1080], ['4k', 3840, 2160], ['narrow', 390, 844]]) {
      const page = await browser.newPage({ viewport: { width, height } });
      const errors = [], writes = [];
      const notices = ['CH', 'SE'].flatMap(country => ['OLD-A', 'OLD-B'].map(code => ({ key: country + code, countryCode: country, brand: 'JAECOO', modelName: 'JAECOO5 BEV', bomTemplate: code, date: '2026-10-30', kind: 'start', target: null, note: '', read: false })));
      page.on('pageerror', error => errors.push(error.message));
      await page.addInitScript(() => { for (const [key, value] of Object.entries({ jato_auth_token: 'mock', jato_user_role: 'order_filler', jato_user_name: 'mock', jato_primary_country: 'CH', jato_secondary_countries: '["SE"]', jato_brands: '["JAECOO"]' })) localStorage.setItem(key, value); });
      await page.route('**/v1/**', async route => {
        const request = route.request(), url = new URL(request.url()), endpoint = url.pathname, country = url.searchParams.get('country') || 'CH';
        let body = { items: [], total: 0, columns: [] };
        if (endpoint.endsWith('/auth/me')) body = { username: 'mock', role: 'order_filler', primaryCountry: 'CH', secondaryCountries: ['SE'], brands: ['JAECOO'], profileComplete: true };
        else if (endpoint.endsWith('/account-country-options') || endpoint.endsWith('/countries')) body = { items: ['CH', 'SE'].map(code => ({ countryCode: code, countryName: code, paymentTermCode: 'TT' })) };
        else if (endpoint.endsWith('/fob-countries')) body = { countries: ['CH', 'SE'] };
        else if (endpoint.endsWith('/material-timeline')) body = { countryCode: country, today: '2026-10-13', windowFrom: '2026-09-13', windowTo: '2026-11-12', nodes, links, events: [] };
        else if (endpoint.endsWith('/material-notices')) body = { countryCode: country, notices: notices.filter(item => item.countryCode === country) };
        else if (endpoint.endsWith('/read') && request.method() === 'PUT') { const key = decodeURIComponent(endpoint.split('/').at(-2)); const notice = notices.find(item => item.key === key); assert(notice); notice.read = request.postDataJSON().read; writes.push(key); body = { read: notice.read }; }
        else if (endpoint.endsWith('/options')) body = { countryCode: country, brands: ['JAECOO'], models: ['JAECOO5 BEV'], powertrains: ['BEV'], versions: ['Select-FWD'], colours: [], materialCodes: [] };
        else if (endpoint.endsWith('/matrix/batch')) body = { errors: {}, matrices: Object.fromEntries(['CH', 'SE'].map(code => [code, { countryCode: code, countryName: code, paymentTermCode: 'TT', year: 2026, totalRows: 0, rows: [] }])) };
        else if (!['GET', 'HEAD'].includes(request.method())) throw new Error('Unexpected write: ' + endpoint);
        await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
      });
      await page.goto(base + '/product/order-genius');
      const board = page.getByRole('region', { name: 'Material changes / 物料变化', exact: true });
      await board.getByRole('status').filter({ hasText: 'models with recent changes' }).waitFor();
      const drawer = page.locator('.order-genius-control-drawer.is-open');
      if (await drawer.count()) await drawer.getByRole('button', { name: 'Close', exact: true }).click();
      await board.getByRole('button', { name: 'View timeline / 查看时间轴' }).click();
      const model = board.locator('.og-timeline-model').first();
      await model.locator(':scope > summary').click();
      assert.equal(await model.locator('.og-lineage-axis').count(), 1);
      assert.equal(await model.locator('.og-lineage-branch').count(), 2);
      const connector = await model.locator('.og-lineage-root').evaluate(element => ({ content: getComputedStyle(element, '::after').content, width: getComputedStyle(element, '::after').width }));
      assert.equal(connector.content, '""'); assert.equal(connector.width, '28px');
      assert.equal(await model.locator('.og-lineage-pending').evaluate(element => element.open), false);
      await model.getByRole('button', { name: 'OLD-A ends 2026-10-29', exact: true }).click();
      await board.getByRole('region', { name: 'Material change detail' }).filter({ hasText: 'OLD-A' }).waitFor();
      await model.getByRole('button', { name: 'NEW-A starts 2026-10-30', exact: true }).click();
      await board.getByRole('region', { name: 'Material change detail' }).filter({ hasText: 'NEW-A' }).waitFor();
      assert.equal(await model.getByRole('button', { name: /^NEW-A ends/ }).count(), 0);
      await model.getByRole('button', { name: 'OLD-A → NEW-A · Note / 备注', exact: true }).click();
      await board.getByRole('region', { name: 'Material change detail' }).filter({ hasText: 'Battery upgrade' }).waitFor();
      assert.equal(await board.getByRole('button', { name: /Link versions/ }).count(), 0);
      await board.getByRole('combobox', { name: 'Time zoom' }).selectOption('2');
      assert.equal(await model.locator('.og-lineage-map').evaluate(element => element.style.minWidth), '1600px');
      const reminders = board.locator('.og-material-notices');
      await reminders.locator(':scope > summary').filter({ hasText: '1 unread changes' }).waitFor();
      await reminders.locator(':scope > details > summary').click();
      assert.equal(await reminders.locator('.og-material-notice').count(), 1);
      await reminders.getByRole('combobox').selectOption('SE');
      await reminders.getByRole('button', { name: 'Mark read / 标为已读', exact: true }).click();
      await reminders.locator(':scope > summary').filter({ hasText: '0 unread changes' }).waitFor();
      assert.equal(writes.length, 4);
      assert.equal(await reminders.locator(':scope > details').evaluate(element => element.open), true);
      assert.equal(await reminders.getByRole('combobox').inputValue(), 'SE');
      assert.deepEqual(errors, []);
      if (output) { fs.mkdirSync(output, { recursive: true }); await board.screenshot({ path: path.join(output, 'lineage-' + name + '.png') }); }
      checks.push({ viewport: name, twoBranches: true, sharedRuler: true, pendingCollapsed: true, noteVisible: true, oneEvent: true, fourAuthorizedReads: true, noBusinessWrites: true, errors });
      await page.close();
    }
    console.log(JSON.stringify(checks));
  } finally { await browser.close(); }
})().catch(error => { console.error(error.message); process.exitCode = 1; });
