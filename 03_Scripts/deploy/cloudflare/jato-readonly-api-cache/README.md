# JATO Read-only API Cache Worker

This Worker is an optional edge cache for overseas users. It is intended for
`intl.ojeur.cloud` to call a Cloudflare-hosted API facade while the origin
FastAPI backend remains on Tencent Cloud.

It only caches explicit read-only endpoints:

- `GET /v1/metadata/columns`
- `GET /v1/metadata/filter-snapshot`
- `GET /v1/assistant/country/metadata`
- `GET /v1/analysis/data-freshness`
- `POST /v1/filters/options`
- `POST /v1/filters/options/batch`
- `POST /v1/analysis/overview`
- `POST /v1/analysis/time-series`
- `POST /v1/analysis/time-series-grouped`

Auth, login, write APIs, profile updates, and admin mutation APIs are always
proxied to origin and are not cached.

## Cache Key

The Worker builds a synthetic cache key from:

- HTTP method
- path and query string
- request body SHA-256 hash
- hashed `X-User-Role`
- `X-JATO-Data-Version` request header, or `DATA_VERSION` from Worker env

This keeps cached payloads scoped to the permission role, avoids splitting the
same read-only dashboard payload by refreshed login tokens, and lets a data
publish invalidate old entries by changing `DATA_VERSION`.

## Prewarm Coverage

`06_AppPlatform/frontend/scripts/prewarm_intl_edge_cache.cjs` seeds the same
read-only cache used by `intl.ojeur.cloud/v1/*`. The default Dashboard warmup
covers the top-level filter snapshot/batch requests, `POST /v1/analysis/overview`,
and grouped time-series combinations for:

- `动总规整`
- `国家`
- `四驱占比`
- `Business/Private 占比`

The share-only grouped lenses also prewarm the `segment` and `powertrain`
`share_split_by` variants. Override these for a targeted warmup with:

```bash
JATO_PREWARM_GROUP_BY=国家,四驱占比 \
JATO_PREWARM_SHARE_SPLIT_BY=segment \
npm run perf:prewarm-edge
```

## Deploy

```bash
cd 03_Scripts/deploy/cloudflare/jato-readonly-api-cache
npx wrangler deploy
```

After DNS is ready, bind a route such as:

```toml
[[routes]]
pattern = "api-intl.ojeur.cloud"
custom_domain = true
```

The Worker deployment can report the custom domain as attached before public DNS
is actually ready. Verify DNS before switching the frontend:

```bash
nslookup api-intl.ojeur.cloud
curl -i https://api-intl.ojeur.cloud/healthz
```

If `nslookup` returns `NXDOMAIN`, add a proxied DNS record in Cloudflare DNS:

```text
Type: CNAME
Name: api-intl
Target: jato-readonly-api-cache.tristanlyk.workers.dev
Proxy status: Proxied
```

If `ojeur.cloud` is managed by Cloudflare DNS, point the Cloudflare Pages intl
frontend at the Worker facade:

```bash
VITE_API_BASE=https://api-intl.ojeur.cloud/v1
```

Keep `www.ojeur.cloud` on the Tencent Cloud backend for domestic users.
Do not use the `*.workers.dev` URL as the production API base; direct access to
`workers.dev` can be unreliable from domestic networks. Use
`https://api-intl.ojeur.cloud/v1` for the intl Pages deployment.

If `ojeur.cloud` is still managed by DNSPod or another external DNS provider,
the Worker custom domain will not become publicly resolvable until the DNS
record or delegation is added there. In that setup, keep the intl Pages frontend
on the same-origin Pages Function API:

```bash
VITE_API_BASE=/v1
```

The same-origin path is `https://intl.ojeur.cloud/v1/*`, backed by
`06_AppPlatform/frontend/functions/v1/[[path]].js`.

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
