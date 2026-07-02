const { chromium } = require('playwright');
const assert = require('node:assert/strict');

const baseUrl = process.env.JATO_REGRESSION_BASE_URL || 'http://127.0.0.1:4173';

const columns = [
  '国家',
  '细分市场（按车长）',
  '动总规整',
  'Make',
  'Model',
  'Version name',
  'Length',
  'MSRP',
  '2024',
  '2025',
];

const optionUniverse = {
  '国家': ['德国', '法国'],
  '细分市场（按车长）': ['SUV', 'Sedan'],
  '动总规整': ['BEV', 'PHEV', 'ICE'],
  'Make': ['BMW', 'Audi'],
  'Model': ['iX1', 'X3', 'Q4'],
  'Version name': ['iX1 xDrive30', 'X3 xDrive20', 'Q4 45 e-tron'],
};

const counters = { columns: 0, options: 0, overview: 0, detail: 0, crudList: 0, dataOverview: 0 };

let crudItems = [
  {
    id: 'src-1',
    sourceId: 'src-1',
    sourceCode: 'DE-BMW-OFFICIAL',
    country: 'Germany',
    brand: 'BMW',
    sourceUrl: 'https://www.bmw.de/configure',
    sourceType: 'official_configurator',
    tier: 1,
    extractorName: 'bmw_de_extractor',
    extractorVersion: 'v2',
    priceSemantics: 'msrp_incl_vat',
    requiresLocation: false,
    enabled: true,
    notes: 'Regression seed row',
    createdAtUtc: '2026-04-17T08:00:00Z',
    updatedAtUtc: '2026-04-17T08:00:00Z',
  },
  {
    id: 'src-2',
    sourceId: 'src-2',
    sourceCode: 'DE-AUDI-MEDIA',
    country: 'Germany',
    brand: 'Audi',
    sourceUrl: 'https://example.com/audi-q4',
    sourceType: 'automotive_media',
    tier: 4,
    extractorName: 'manual',
    extractorVersion: 'v1',
    priceSemantics: 'retail_price',
    requiresLocation: false,
    enabled: false,
    notes: 'Secondary mock row',
    createdAtUtc: '2026-04-16T08:00:00Z',
    updatedAtUtc: '2026-04-16T08:00:00Z',
  },
];

function buildDataManagementOverview() {
  return {
    item: {
      generatedAt: '2026-04-17T08:00:00Z',
      database: {
        enabled: true,
        connected: true,
        detail: 'ok',
      },
      domains: [
        {
          key: 'msrp',
          label: 'MSRP',
          status: 'ready',
          storage: 'postgres',
          updatedAt: '2026-04-17T08:00:00Z',
          summary: 'Regression mock for data-management entry.',
          metrics: [
            { label: 'Sources', value: 2 },
            { label: 'Current Prices', value: 12 },
          ],
          recentItems: [
            { label: 'DE-BMW-OFFICIAL', value: 'updated', updatedAt: '2026-04-17T08:00:00Z' },
          ],
        },
      ],
      fileInventory: [],
      databaseTables: [],
      activity: {
        days: [],
        maxCount: 0,
        totalCount: 0,
        rangeStart: '2026-04-01',
        rangeEnd: '2026-04-17',
        sourceCounts: [],
        databaseConnected: true,
      },
    },
  };
}

function optionsFor(column, filters = {}) {
  if (column === 'Make') {
    if ((filters['动总规整'] || []).includes('BEV')) return ['BMW'];
    return optionUniverse[column];
  }
  if (column === 'Model') {
    if ((filters.Make || []).includes('BMW')) return ['iX1', 'X3'];
    return optionUniverse[column];
  }
  if (column === 'Version name') {
    if ((filters.Model || []).includes('iX1')) return ['iX1 xDrive30'];
    if ((filters.Model || []).includes('X3')) return ['X3 xDrive20'];
    return optionUniverse[column];
  }
  return optionUniverse[column] || [];
}

function buildDetailItems() {
  return [
    {
      '国家': '德国',
      '细分市场（按车长）': 'SUV',
      '动总规整': 'BEV',
      'Make': 'BMW',
      'Model': 'iX1',
      'Version name': 'iX1 xDrive30',
      'Length': 4616,
      'MSRP': 48990,
      '2024': 1200,
      '2025': 1450,
    },
    {
      '国家': '德国',
      '细分市场（按车长）': 'SUV',
      '动总规整': 'BEV',
      'Make': 'BMW',
      'Model': 'iX1',
      'Version name': 'iX1 eDrive20',
      'Length': 4616,
      'MSRP': 44990,
      '2024': 980,
      '2025': 1180,
    },
  ];
}

function buildAdvancedChartItems(chart) {
  if (chart === 'powertrain_bubble') {
    return [
      {
        Powertrain: 'BEV',
        Length: 4616,
        MSRP: 48990,
        Sales: 1450,
        Model: 'iX1',
        Version: 'iX1 xDrive30',
      },
    ];
  }
  return [];
}

function buildCrudListResponse(url) {
  const page = Number(url.searchParams.get('page') || 1);
  const pageSize = Number(url.searchParams.get('page_size') || 20);
  const sortBy = url.searchParams.get('sort_by') || 'code';
  const sortOrder = url.searchParams.get('sort_order') || 'asc';
  const query = (url.searchParams.get('query') || '').trim().toLowerCase();

  let filtered = crudItems.filter((item) => {
    if (!query) return true;
    return [item.code, item.name, item.status, item.notes]
      .filter(Boolean)
      .some((value) => String(value).toLowerCase().includes(query));
  });

  filtered = [...filtered].sort((left, right) => {
    const leftValue = String(left[sortBy] ?? '');
    const rightValue = String(right[sortBy] ?? '');
    const compareResult = leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: 'base' });
    return sortOrder === 'desc' ? compareResult * -1 : compareResult;
  });

  const start = Math.max(0, (page - 1) * pageSize);
  const items = filtered.slice(start, start + pageSize);

  return {
    page,
    pageSize,
    total: filtered.length,
    items,
  };
}

function json(route, body) {
  return route.fulfill({
    status: 200,
    contentType: 'application/json',
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type,X-Auth-Token,X-User-Role,X-User-Name',
    },
    body: JSON.stringify(body),
  });
}

function snapshot() {
  return { ...counters };
}

function assertSharedBootStable(before, after, label) {
  assert.equal(after.columns, before.columns, `${label}: metadata/columns should not rerun`);
  assert.equal(after.options, before.options, `${label}: filters/options should not rerun`);
  assert.ok(
    after.overview <= Math.max(before.overview, 1),
    `${label}: analysis/overview should only run as the deferred dashboard boot request`,
  );
}

async function waitForCondition(check, label, timeoutMs = 5000) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (check()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function launchBrowser() {
  const attempts = [
    { channel: 'chrome', headless: true },
    { channel: 'msedge', headless: true },
    { headless: true },
  ];
  let lastError;
  for (const options of attempts) {
    try {
      return await chromium.launch(options);
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

async function main() {
  const browser = await launchBrowser();
  const page = await browser.newPage();

  page.on('console', (message) => {
    const text = message.text();
    if (/spline|prod\.spline\.design|deserialize|KDA\.load|Failed to fetch|Failed to load resource|ERR_FAILED/i.test(text)) {
      return;
    }
    if (message.type() === 'error' || message.type() === 'warning') {
      console.error(`BROWSER_${message.type().toUpperCase()}: ${text}`);
    }
  });

  page.on('pageerror', (error) => {
    if (/spline|prod\.spline\.design|deserialize|KDA\.load|Failed to fetch|Failed to load resource|ERR_FAILED/i.test(error.stack || error.message || '')) {
      return;
    }
    console.error(`PAGEERROR: ${error.stack || error.message}`);
  });

  await page.route('**/prod.spline.design/**', async (route) => {
    await route.abort();
  });

  if (process.env.JATO_REGRESSION_DEBUG === '1') {
    page.on('request', (request) => {
      if (request.url().includes('/v1/')) {
        console.log(`REQUEST: ${request.method()} ${request.url()}`);
      }
    });
  }

  await page.route('**/v1/**', async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;

    if (request.method() === 'OPTIONS') {
      return route.fulfill({
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type,X-Auth-Token,X-User-Role,X-User-Name',
        },
      });
    }

    if (path.endsWith('/metadata/columns')) {
      counters.columns += 1;
      return json(route, { items: columns });
    }

    if (path.endsWith('/filters/options/batch')) {
      counters.options += 1;
      const payload = request.postDataJSON() || {};
      const items = Array.isArray(payload.items) ? payload.items : [];
      return json(route, {
        items: items.map((item) => ({
          column: item.column,
          options: optionsFor(item.column, item.filters || {}),
        })),
      });
    }

    if (path.endsWith('/filters/options')) {
      counters.options += 1;
      const payload = request.postDataJSON() || {};
      if (process.env.JATO_REGRESSION_DEBUG === '1') {
        console.log(`OPTIONS_REQUEST: ${JSON.stringify(payload)}`);
      }
      return json(route, {
        column: payload.column,
        options: optionsFor(payload.column, payload.filters || {}),
      });
    }

    if (path.endsWith('/analysis/overview')) {
      counters.overview += 1;
      return json(route, {
        route: 'mock-overview',
        kpis: {
          totalRows: 2380,
          countryCount: 1,
          brandCount: 1,
          modelCount: 1,
          versionCount: 2,
          cumulativeSales: 2630,
          avgMsrp: 46990,
        },
        yearSeries: [
          { time: '2024', value: 1200 },
          { time: '2025', value: 1450 },
        ],
        monthSeries: [
          { time: '2025-01', value: 110 },
          { time: '2025-02', value: 140 },
        ],
      });
    }

    if (path.endsWith('/analysis/detail')) {
      counters.detail += 1;
      const payload = request.postDataJSON() || {};
      const items = buildDetailItems().map((item) => {
        const projected = {};
        (payload.columns || columns).forEach((column) => {
          projected[column] = item[column] ?? '';
        });
        return projected;
      });
      return json(route, {
        page: payload.page || 1,
        pageSize: payload.page_size || 100,
        total: items.length,
        items,
      });
    }

    if (path.endsWith('/analysis/advanced-chart')) {
      const payload = request.postDataJSON() || {};
      return json(route, {
        group: payload.group || 'market_structure',
        chart: payload.chart || 'powertrain_bubble',
        rows: 1,
        items: buildAdvancedChartItems(payload.chart),
        meta: {},
      });
    }

    if (path.endsWith('/crud/items') && request.method() === 'GET') {
      counters.crudList += 1;
      return json(route, buildCrudListResponse(url));
    }

    if (path.endsWith('/data-management/overview') && request.method() === 'GET') {
      counters.dataOverview += 1;
      return json(route, buildDataManagementOverview());
    }

    if (path.endsWith('/msrp/sources') && request.method() === 'GET') {
      counters.crudList += 1;
      return json(route, { items: crudItems });
    }

    if (path.endsWith('/crud/items') && request.method() === 'POST') {
      const payload = request.postDataJSON() || {};
      const item = {
        id: `crud-${crudItems.length + 1}`,
        code: payload.code || 'NEW-ITEM',
        name: payload.name || 'New Item',
        status: payload.status || 'active',
        notes: payload.notes || '',
      };
      crudItems = [item, ...crudItems];
      return json(route, { item });
    }

    if (/\/crud\/items\/[^/]+$/.test(path) && request.method() === 'DELETE') {
      const id = path.split('/').pop();
      crudItems = crudItems.filter((item) => item.id !== id);
      return json(route, { deleted: true });
    }

    return json(route, {});
  });

  const powertrainCheckbox = () => page
    .locator('.filter-card')
    .filter({ has: page.locator('.filter-card-title', { hasText: '动总规整' }) })
    .first()
    .locator('label')
    .filter({ hasText: 'BEV' })
    .locator('input');

  try {
    const dashboardUrl = `${baseUrl}/?powertrain=BEV&make=BMW&model=iX1`;
    await page.goto(dashboardUrl, { waitUntil: 'networkidle' });
    if (process.env.JATO_REGRESSION_DEBUG === '1') {
      console.log(`COUNTERS_AFTER_DASHBOARD_GOTO: ${JSON.stringify(snapshot())}`);
    }
    await page.getByRole('heading', { name: 'Dashboard Control View' }).waitFor();
    assert.equal(await powertrainCheckbox().isChecked(), true, 'Dashboard should hydrate query-selected powertrain');

    const specHref = await page.getByRole('link', { name: '打开 Specification Page' }).getAttribute('href');
    assert.equal(specHref, '/specification?powertrain=BEV&make=BMW&model=iX1', 'Dashboard share link should keep active query');

    const bootSnapshot = snapshot();
    assert.ok(bootSnapshot.columns >= 1, 'Initial dashboard load should boot columns at least once');
    assert.ok(bootSnapshot.options > 0, 'Initial dashboard load should request filter options');
    assert.ok(bootSnapshot.overview <= 1, 'Initial dashboard load should not repeat deferred overview');

    await page.getByRole('link', { name: '打开 Specification Page' }).click();
    await page.waitForURL('**/specification?powertrain=BEV&make=BMW&model=iX1');
    await page.getByRole('heading', { name: 'Specification / Detail Explorer' }).waitFor();
    assert.equal(await powertrainCheckbox().isChecked(), true, 'Specification should reuse selected powertrain');
    await waitForCondition(() => counters.detail >= 1, 'specification detail request');
    await page.locator('table.data-table tbody').filter({ hasText: '德国' }).waitFor();

    const afterSpecification = snapshot();
    assertSharedBootStable(bootSnapshot, afterSpecification, 'Dashboard -> Specification');
    assert.ok(afterSpecification.detail >= 1, 'Specification should request detail rows');

    await page.reload({ waitUntil: 'networkidle' });
    await page.getByRole('heading', { name: 'Specification / Detail Explorer' }).waitFor();
    assert.equal(await powertrainCheckbox().isChecked(), true, 'Specification refresh should preserve selected powertrain');
    await page.locator('table.data-table tbody').filter({ hasText: '德国' }).waitFor();
    assert.equal(new URL(page.url()).search, '?powertrain=BEV&make=BMW&model=iX1', 'Specification refresh should preserve query');

    const afterSpecificationRefresh = snapshot();

    await page.goBack({ waitUntil: 'networkidle' });
    await page.waitForURL('**/?powertrain=BEV&make=BMW&model=iX1');
    await page.getByRole('heading', { name: 'Dashboard Control View' }).waitFor();
    const afterBack = snapshot();
    assertSharedBootStable(afterSpecificationRefresh, afterBack, 'Specification refresh -> browser back');

    const beforeCrud = snapshot();
    await page.evaluate(() => {
      window.history.pushState({}, '', '/crud');
      window.dispatchEvent(new PopStateEvent('popstate'));
    });
    await page.waitForURL((url) => url.pathname === '/data-management');
    await page.getByRole('heading', { name: '数据总览' }).waitFor();
    await page.getByRole('button', { name: 'MSRP Sources' }).waitFor();
    await page.locator('table.crud-table tbody').filter({ hasText: 'DE-BMW-OFFICIAL' }).waitFor();

    const afterCrud = snapshot();
    assertSharedBootStable(beforeCrud, afterCrud, 'Dashboard -> Data Management redirect');
    assert.ok(afterCrud.dataOverview >= beforeCrud.dataOverview + 1, 'Data management route should request overview');
    assert.ok(afterCrud.crudList >= beforeCrud.crudList + 1, 'Data management route should request MSRP sources');

    await page.goto(`${baseUrl}/specification?powertrain=BEV&make=BMW&model=iX1`, { waitUntil: 'networkidle' });
    await page.getByRole('heading', { name: 'Specification / Detail Explorer' }).waitFor();
    assert.equal(await powertrainCheckbox().isChecked(), true, 'Direct specification share link should hydrate query');
    await page.locator('table.data-table tbody').filter({ hasText: '德国' }).waitFor();

    const directSpecificationSnapshot = snapshot();

    const dashboardBackHref = await page.getByRole('link', { name: '返回 Dashboard' }).first().getAttribute('href');
    assert.equal(dashboardBackHref, '/?powertrain=BEV&make=BMW&model=iX1', 'Specification back link should preserve query');

    await page.getByRole('link', { name: '返回 Dashboard' }).first().click();
    await page.waitForURL('**/?powertrain=BEV&make=BMW&model=iX1');
    await page.getByRole('heading', { name: 'Dashboard Control View' }).waitFor();
    const afterDirectSpecBack = snapshot();
    assertSharedBootStable(directSpecificationSnapshot, afterDirectSpecBack, 'Direct specification share link -> Dashboard');

    await page.goto(`${baseUrl}/route-that-does-not-exist`, { waitUntil: 'networkidle' });
    await page.getByRole('heading', { name: 'Page Not Found' }).waitFor();
    await page.getByRole('link', { name: '返回 Dashboard' }).click();
    await page.waitForURL((url) => url.pathname === '/');
    await page.getByRole('heading', { name: 'Dashboard Control View' }).waitFor();

    console.log(JSON.stringify({
      status: 'ok',
      counters,
      checks: [
        'dashboard_to_spec_query_preserved',
        'specification_refresh_hydrates_query',
        'browser_back_query_preserved',
        'crud_redirect_to_data_management',
        'spec_share_link_back_query_preserved',
        'shared_filter_boot_not_repeated_on_route_switch',
        'not_found_route_renders_shell',
      ],
    }, null, 2));
  } catch (error) {
    console.error(`CURRENT_URL: ${page.url()}`);
    console.error(`BODY_SNIPPET: ${await page.locator('body').innerText().catch(() => '')}`);
    throw error;
  } finally {
    await browser.close();
  }
}

main().catch(async (error) => {
  console.error(error);
  process.exitCode = 1;
});
