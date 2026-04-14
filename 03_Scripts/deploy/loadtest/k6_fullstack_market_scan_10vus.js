import http from 'k6/http';
import { check, sleep } from 'k6';

const APP_BASE_URL = (__ENV.APP_BASE_URL || __ENV.BASE_URL || 'http://127.0.0.1').replace(/\/$/, '');
const API_BASE_URL = (__ENV.API_BASE_URL || APP_BASE_URL).replace(/\/$/, '');
const AUTH_TOKEN = __ENV.AUTH_TOKEN || 'change-me';
const USER_ROLE = __ENV.USER_ROLE || 'viewer';
const USER_NAME = __ENV.USER_NAME || 'k6-market-scan';
const APP_HOST_HEADER = __ENV.APP_HOST_HEADER || '';
const API_HOST_HEADER = __ENV.API_HOST_HEADER || '';
const TARGET_PERIOD = __ENV.TARGET_PERIOD || undefined;
const COUNTRY = __ENV.COUNTRY || undefined;
const DRILLDOWN_SEGMENT = __ENV.DRILLDOWN_SEGMENT || 'SUV A0';

const PAGE_HEADERS = APP_HOST_HEADER ? { Host: APP_HOST_HEADER } : {};

const READ_HEADERS = {
  'X-Auth-Token': AUTH_TOKEN,
  'X-User-Role': USER_ROLE,
  'X-User-Name': USER_NAME,
  ...(API_HOST_HEADER ? { Host: API_HOST_HEADER } : {}),
};

const JSON_HEADERS = {
  ...READ_HEADERS,
  'Content-Type': 'application/json',
};

const marketScanPayload = JSON.stringify({
  country: COUNTRY,
  target_period: TARGET_PERIOD,
  fuel_types: ['ICE', 'MHEV', 'HEV', 'PHEV', 'BEV', 'LPG'],
  trend_window_months: 24,
  origin_window_months: 24,
  body_window_months: 24,
  ranking_limit: 6,
  drilldown_segment: DRILLDOWN_SEGMENT,
});

export const options = {
  stages: [
    { duration: '30s', target: 5 },
    { duration: '60s', target: 10 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    'http_req_duration{endpoint:market-scan-page}': ['p(95)<4000'],
    'http_req_duration{endpoint:market-scan-deck}': ['p(95)<15000'],
    'http_req_duration{endpoint:overview}': ['p(95)<5000'],
    'http_req_duration{endpoint:detail}': ['p(95)<7000'],
  },
};

export function setup() {
  const columnsResponse = http.get(`${API_BASE_URL}/v1/metadata/columns`, {
    headers: READ_HEADERS,
    tags: { endpoint: 'columns' },
  });
  check(columnsResponse, {
    'columns status is 200': (r) => r.status === 200,
  });
  const body = columnsResponse.status === 200 ? JSON.parse(columnsResponse.body) : { items: [] };
  return {
    detailColumns: Array.isArray(body.items) ? body.items.slice(0, 5) : [],
  };
}

export default function (data) {
  const pageResponses = http.batch([
    ['GET', `${APP_BASE_URL}/`, null, { headers: PAGE_HEADERS, tags: { endpoint: 'root-page' } }],
    ['GET', `${APP_BASE_URL}/specification`, null, { headers: PAGE_HEADERS, tags: { endpoint: 'specification-page' } }],
    ['GET', `${APP_BASE_URL}/market-scan`, null, { headers: PAGE_HEADERS, tags: { endpoint: 'market-scan-page' } }],
    ['GET', `${API_BASE_URL}/healthz`, null, { tags: { endpoint: 'healthz' } }],
  ]);

  check(pageResponses[0], { 'root page is 200': (r) => r.status === 200 });
  check(pageResponses[1], { 'specification page is 200': (r) => r.status === 200 });
  check(pageResponses[2], { 'market-scan page is 200': (r) => r.status === 200 });
  check(pageResponses[3], { 'healthz is 200': (r) => r.status === 200 });

  const apiResponses = http.batch([
    [
      'POST',
      `${API_BASE_URL}/v1/analysis/overview`,
      JSON.stringify({ filters: {}, prefer_precomputed: true, top_n: 24 }),
      { headers: JSON_HEADERS, tags: { endpoint: 'overview' } },
    ],
    [
      'POST',
      `${API_BASE_URL}/v1/analysis/detail`,
      JSON.stringify({
        filters: {},
        columns: data.detailColumns,
        page: 1,
        page_size: 50,
        exclude_zero_sales: true,
      }),
      { headers: JSON_HEADERS, tags: { endpoint: 'detail' } },
    ],
    [
      'POST',
      `${API_BASE_URL}/v1/market-scan/deck`,
      marketScanPayload,
      { headers: JSON_HEADERS, tags: { endpoint: 'market-scan-deck' } },
    ],
  ]);

  check(apiResponses[0], { 'overview is 200': (r) => r.status === 200 });
  check(apiResponses[1], { 'detail is 200': (r) => r.status === 200 });
  check(apiResponses[2], { 'market-scan deck is 200': (r) => r.status === 200 });

  sleep(0.5 + Math.random() * 1.0);
}