import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1';

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

export default function () {
  const root = http.get(`${BASE_URL}/`, { tags: { endpoint: 'root' } });
  check(root, {
    'root status is 200': (r) => r.status === 200,
  });

  const health = http.get(`${BASE_URL}/healthz`, {
    tags: { endpoint: 'healthz' },
  });
  check(health, {
    'health status is 200': (r) => r.status === 200,
  });

  sleep(1 + Math.random() * 2);
}
