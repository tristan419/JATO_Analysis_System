# JATO Read-only API Cache Worker

This Worker is an optional edge cache for overseas users. It is intended for
`intl.ojeur.cloud` to call a Cloudflare-hosted API facade while the origin
FastAPI backend remains on Tencent Cloud.

It only caches explicit read-only endpoints:

- `GET /v1/metadata/columns`
- `GET /v1/assistant/country/metadata`
- `POST /v1/filters/options`
- `POST /v1/filters/options/batch`
- `POST /v1/analysis/time-series-grouped`

Auth, login, write APIs, profile updates, and admin mutation APIs are always
proxied to origin and are not cached.

## Cache Key

The Worker builds a synthetic cache key from:

- HTTP method
- path and query string
- request body SHA-256 hash
- hashed `X-Auth-Token`, `X-User-Name`, and `X-User-Role`
- `X-JATO-Data-Version` request header, or `DATA_VERSION` from Worker env

This keeps cached payloads scoped to the user/auth context and lets a data
publish invalidate old entries by changing `DATA_VERSION`.

## Deploy

```bash
cd 03_Scripts/deploy/cloudflare/jato-readonly-api-cache
npx wrangler deploy
```

After DNS is ready, bind a route such as:

```toml
routes = [
  { pattern = "api-intl.ojeur.cloud/*", zone_name = "ojeur.cloud" },
]
```

Then point the Cloudflare Pages intl frontend at the Worker facade:

```bash
VITE_API_BASE=https://api-intl.ojeur.cloud/v1
```

Keep `www.ojeur.cloud` on the Tencent Cloud backend for domestic users.

## Smoke Checks

```bash
curl -i https://api-intl.ojeur.cloud/healthz

curl -i \
  -H 'Origin: https://intl.ojeur.cloud' \
  -H 'X-Auth-Token: test' \
  -H 'X-User-Name: test' \
  https://api-intl.ojeur.cloud/v1/metadata/columns
```

The first cacheable origin response should return:

```text
x-jato-edge-cache: MISS
```

The repeated request with the same auth scope and payload should return:

```text
x-jato-edge-cache: HIT
```

## Operational Notes

- Increase `DATA_VERSION` after JATO data publish if the source dataset changes.
- Keep `MAX_CACHE_BODY_BYTES` conservative; large uncached responses still proxy.
- If an endpoint returns `Set-Cookie` or a non-200 status, the Worker bypasses
  cache and forwards the origin response.
