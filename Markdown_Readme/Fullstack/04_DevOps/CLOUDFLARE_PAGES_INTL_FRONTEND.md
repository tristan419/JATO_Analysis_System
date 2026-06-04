# Cloudflare Pages Intl Frontend

Goal: keep China traffic on Tencent Cloud at `https://www.ojeur.cloud`, and serve an overseas static frontend at `https://intl.ojeur.cloud` with the API still calling Tencent Cloud.

## Cloudflare Pages

Create a Pages project from the GitHub repo:

- Root directory: `06_AppPlatform/frontend`
- Build command: `npm ci && npm run build`
- Build output directory: `dist`
- Production environment variables:

```bash
VITE_API_BASE=https://www.ojeur.cloud/v1
VITE_USER_ROLE=viewer
VITE_USER_NAME=anonymous
```

The repo includes:

- `public/_redirects`: React Router SPA fallback for routes like `/login`.
- `public/_headers`: long cache for hashed assets and no-cache for build metadata.

## Custom Domain

In Cloudflare Pages, add:

```text
intl.ojeur.cloud
```

Keep `www.ojeur.cloud` on Tencent Cloud for China traffic. Do not move the whole apex domain if China speed is still required and there is no budget for paid traffic steering.

If you test the temporary Cloudflare `*.pages.dev` domain before binding `intl.ojeur.cloud`, append that exact origin to both `APP_FRONTEND_ORIGINS` and `APP_CORS_ORIGINS` during the test.

## Tencent API Env

The backend must allow both frontends:

```bash
APP_FRONTEND_ORIGIN=https://www.ojeur.cloud
APP_FRONTEND_ORIGINS=https://www.ojeur.cloud,https://intl.ojeur.cloud
APP_CORS_ORIGINS=https://www.ojeur.cloud,https://intl.ojeur.cloud,http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000
APP_GOOGLE_REDIRECT_URI=https://www.ojeur.cloud/v1/auth/google/callback
```

Google OAuth callback can stay on `www.ojeur.cloud`; the backend stores the initiating frontend origin in OAuth state and redirects back to `intl.ojeur.cloud` only when it is in `APP_FRONTEND_ORIGINS`.

## Quick Check

After both deployments:

```bash
curl -I https://intl.ojeur.cloud/login
curl -i -X OPTIONS \
  -H 'Origin: https://intl.ojeur.cloud' \
  -H 'Access-Control-Request-Method: GET' \
  -H 'Access-Control-Request-Headers: X-Auth-Token, Content-Type' \
  https://www.ojeur.cloud/v1/auth/me
```

The second command should include an `access-control-allow-origin: https://intl.ojeur.cloud` response header.
