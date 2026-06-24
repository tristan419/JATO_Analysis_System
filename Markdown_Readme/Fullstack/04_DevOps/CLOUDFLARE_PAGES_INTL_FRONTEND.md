# Cloudflare Pages Intl Frontend

Goal: keep China traffic on Tencent Cloud at `https://www.ojeur.cloud`, and serve an overseas static frontend at `https://intl.ojeur.cloud` with the API still calling Tencent Cloud.

## Cloudflare Pages

Create a Pages project from the GitHub repo:

- Root directory: `06_AppPlatform/frontend`
- Build command: `npm ci && npm run build`
- Build output directory: `dist`
- Production environment variables:

```bash
VITE_API_BASE=/v1
API_ORIGIN=https://www.ojeur.cloud
VITE_USER_ROLE=viewer
VITE_USER_NAME=anonymous
```

`VITE_API_BASE=/v1` is required for the intl frontend. It makes browser API
traffic hit the same-origin Cloudflare Pages Function at `/v1/*`, where
read-only metadata, filter, overview, and grouped time-series requests can be
cached near overseas users before the Function falls back to Tencent Cloud.

Do not set the Pages build variable to
`VITE_API_BASE=https://www.ojeur.cloud/v1`. That hard-codes the Tencent Cloud
origin into the built JavaScript and bypasses the Pages Function cache. The
frontend build includes a Cloudflare Pages guard that fails this specific
misconfiguration.

The repo includes:

- `public/_redirects`: React Router SPA fallback for routes like `/login`.
- `public/_headers`: long cache for hashed assets and no-cache for build metadata.
- `functions/v1/[[path]].js`: same-origin read-only API cache facade for intl.

## Custom Domain

In Cloudflare Pages, add:

```text
intl.ojeur.cloud
```

Keep `www.ojeur.cloud` on Tencent Cloud for China traffic. Do not move the whole apex domain if China speed is still required and there is no budget for paid traffic steering.

Do not force `www.ojeur.cloud` page routes to `intl.ojeur.cloud` with IP-based nginx 302 rules. Some users run a PAC/configuration proxy instead of a full global proxy, so their browser may still reach `www.ojeur.cloud` through the China network path. Keep `www.ojeur.cloud` as the domestic entry and use `intl.ojeur.cloud` only when users explicitly open the overseas entry.

If you test the temporary Cloudflare `*.pages.dev` domain before binding `intl.ojeur.cloud`, append that exact origin to both `APP_FRONTEND_ORIGINS` and `APP_CORS_ORIGINS` during the test.

## Tencent API Env

The backend must allow both frontends:

```bash
APP_FRONTEND_ORIGIN=https://www.ojeur.cloud
APP_FRONTEND_ORIGINS=https://www.ojeur.cloud,https://intl.ojeur.cloud
APP_CORS_ORIGINS=https://www.ojeur.cloud,https://intl.ojeur.cloud,http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000
APP_GOOGLE_REDIRECT_URI=https://www.ojeur.cloud/v1/auth/google/callback
```

## Read-only API Edge Cache

The Cloudflare Pages deployment should use the same-origin Pages Function API
cache:

```bash
VITE_API_BASE=/v1
API_ORIGIN=https://www.ojeur.cloud
```

Quick cache check:

```bash
curl -sS -D - -X POST 'https://intl.ojeur.cloud/v1/analysis/overview' \
  -H 'content-type: application/json' \
  -H 'x-user-name: test' \
  --data '{"filters":{},"prefer_precomputed":true,"top_n":5}' \
  -o /dev/null | grep -i 'x-jato-edge-cache'
```

Run the same command twice. The first response should be `MISS`, and the second
response should be `HIT` when Cloudflare has accepted the cached object.
The same-origin Function cache key uses method, URL, request body hash,
`X-User-Name`, `X-User-Role`, and data version. It intentionally does not use
`X-Auth-Token`, because refreshed login tokens would otherwise split identical
read-only Dashboard cache entries.
The same-origin Function currently caches metadata, filter option batches,
Dashboard overview, time-series reads, grouped time-series reads, and data
freshness. If Tencent Cloud has not deployed `/v1/metadata/filter-snapshot`
yet, the Function synthesizes that snapshot from `/v1/metadata/columns` plus
`/v1/filters/options/batch`, then caches the synthesized response at the edge.

If a separate API domain is required later, the older standalone Worker facade
is still available in:

```text
03_Scripts/deploy/cloudflare/jato-readonly-api-cache
```

Recommended route:

```text
https://api-intl.ojeur.cloud/*
```

Then set the Cloudflare Pages intl frontend API base to that Worker domain:

```bash
VITE_API_BASE=https://api-intl.ojeur.cloud/v1
```

The Worker only caches explicit read-only endpoints such as metadata, filter
options, and grouped time-series. Auth, profile, admin, and write APIs keep
going to the Tencent Cloud origin. Keep `www.ojeur.cloud` on the Tencent Cloud
backend for domestic users.

Google OAuth callback can stay on `www.ojeur.cloud`; the backend stores the initiating frontend origin in OAuth state and redirects back to `intl.ojeur.cloud` only when it is in `APP_FRONTEND_ORIGINS`.

Do not register `intl.ojeur.cloud` as a second Google callback unless the backend is also changed to accept that callback. The intended chain is:

```text
intl.ojeur.cloud page -> www.ojeur.cloud /v1/auth/google/auth-url
Google login -> www.ojeur.cloud /v1/auth/google/callback
backend state -> intl.ojeur.cloud original page
```

## Quick Check

After both deployments:

```bash
curl -I https://intl.ojeur.cloud/login
curl -sS \
  -H 'Origin: https://intl.ojeur.cloud' \
  'https://www.ojeur.cloud/v1/auth/google/auth-url?redirect=%2Fproduct%2Forder-genius'
curl -i -X OPTIONS \
  -H 'Origin: https://intl.ojeur.cloud' \
  -H 'Access-Control-Request-Method: GET' \
  -H 'Access-Control-Request-Headers: X-Auth-Token, Content-Type' \
  https://www.ojeur.cloud/v1/auth/me
```

The auth-url response should contain a Google URL whose encoded `state` stores `frontend_origin=https://intl.ojeur.cloud` and `redirect=/product/order-genius`. The OPTIONS response should include an `access-control-allow-origin: https://intl.ojeur.cloud` response header.
