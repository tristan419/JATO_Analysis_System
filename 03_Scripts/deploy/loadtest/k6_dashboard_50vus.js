import http from 'k6/http';
import { check, sleep } from 'k6';

const APP_BASE_URL = __ENV.APP_BASE_URL || __ENV.BASE_URL || 'http://127.0.0.1';
const API_BASE_URL = __ENV.API_BASE_URL || APP_BASE_URL;
const AUTH_TOKEN = __ENV.AUTH_TOKEN || 'change-me';
const USER_ROLE = __ENV.USER_ROLE || 'viewer';
const USER_NAME = __ENV.USER_NAME || 'k6-loadtest';

const READ_HEADERS = {
  'X-Auth-Token': AUTH_TOKEN,
  'X-User-Role': USER_ROLE,
  'X-User-Name': USER_NAME,
};

const JSON_HEADERS = {
  ...READ_HEADERS,
  'Content-Type': 'application/json',
};

export const options = {
  stages: [
    { duration: '2m', target: 20 },
    { duration: '3m', target: 50 },
    { duration: '5m', target: 50 },
    { duration: '2m', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<8000', 'p(99)<12000'],
  },
};

export function setup() {
  const columnsResponse = http.get(`${API_BASE_URL}/v1/metadata/columns`, {
    headers: READ_HEADERS,
    tags: { endpoint: 'columns' },
  });
  check(columnsResponse, {
    'metadata columns status is 200': (r) => r.status === 200,
  });

  const body = columnsResponse.status === 200 ? JSON.parse(columnsResponse.body) : { items: [] };
  return {
    detailColumns: Array.isArray(body.items) ? body.items.slice(0, 5) : [],
  };
}

export default function (data) {
  const pageResponses = http.batch([
    ['GET', `${APP_BASE_URL}/`, null, { tags: { endpoint: 'root' } }],
    ['GET', `${APP_BASE_URL}/specification`, null, { tags: { endpoint: 'specification-page' } }],
    ['GET', `${APP_BASE_URL}/crud`, null, { tags: { endpoint: 'crud-page' } }],
    ['GET', `${API_BASE_URL}/healthz`, null, { tags: { endpoint: 'healthz' } }],
  ]);

  check(pageResponses[0], {
    'root status is 200': (r) => r.status === 200,
  });
  check(pageResponses[1], {
    'specification page status is 200': (r) => r.status === 200,
  });
  check(pageResponses[2], {
    'crud page status is 200': (r) => r.status === 200,
  });
  check(pageResponses[3], {
    'health status is 200': (r) => r.status === 200,
  });

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
        exclude_zero_sales: false,
      }),
      { headers: JSON_HEADERS, tags: { endpoint: 'detail' } },
    ],
    [
      'GET',
      `${API_BASE_URL}/v1/crud/items?page=1&page_size=20&sort_by=code&sort_order=asc`,
      null,
      { headers: READ_HEADERS, tags: { endpoint: 'crud-list' } },
    ],
  ]);

  check(apiResponses[0], {
    'overview status is 200': (r) => r.status === 200,
  });
  check(apiResponses[1], {
    'detail status is 200': (r) => r.status === 200,
  });
  check(apiResponses[2], {
    'crud list status is 200': (r) => r.status === 200,
  });

  sleep(0.5 + Math.random() * 1.5);
}
