# Intl Page Load Matrix - 2026-06-19

Scope: first-screen route readiness for `www.ojeur.cloud` and `intl.ojeur.cloud` after PR #20 (`0e73c381`). Measurements used Playwright with a fresh browser context per sample and the `test` / `order_filler` login token for authenticated pages.

Readiness means the route rendered the expected app shell/page text. It is not a full data-complete metric for every chart.

## Direct Network

| Origin | Page | First | Second | Median | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| www | Login | timeout | 3.07s | 3.07s | One cold timeout at 60s. |
| www | Dashboard large params | 9.96s | 7.69s | 8.83s | Large country/powertrain query. |
| www | Order Genius | 50.21s | 11.04s | 30.62s | Slow, measured only as comparison. |
| www | Market Scan | timeout | 3.49s | 3.49s | Redirected to `/market/overview`. |
| www | Advanced Analysis | 3.06s | 5.28s | 4.17s | Stable but slower than intl. |
| intl | Login | 1.22s | 0.74s | 0.98s | Stable. |
| intl | Dashboard large params | 0.49s | 0.54s | 0.52s | Stable first screen. |
| intl | Order Genius | 0.50s | 0.56s | 0.53s | Static shell fast; not a business-code target here. |
| intl | Market Scan | 0.60s | 0.56s | 0.58s | Stable. |
| intl | Advanced Analysis | 0.49s | 0.49s | 0.49s | Stable. |

## Proxy `127.0.0.1:7890`

| Origin | Page | First | Second | Median | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| www | Login | 5.24s | 14.34s | 9.79s | Proxy-to-domestic path is volatile. |
| www | Dashboard large params | 2.40s | 4.54s | 3.47s | Faster than direct in this run, still volatile. |
| www | Order Genius | 4.48s | 10.92s | 7.70s | Slow, measured only as comparison. |
| www | Market Scan | timeout | 30.78s | 30.78s | Unstable under proxy. |
| www | Advanced Analysis | 12.97s | 35.75s | 24.36s | Unstable under proxy. |
| intl | Login | 0.82s | 0.83s | 0.82s | Stable. |
| intl | Dashboard large params | 0.59s | 0.57s | 0.58s | Stable. |
| intl | Order Genius | 0.57s | 0.50s | 0.54s | Static shell fast; not a business-code target here. |
| intl | Market Scan | 0.54s | 0.61s | 0.58s | Stable. |
| intl | Advanced Analysis | 0.51s | 0.53s | 0.52s | Stable. |

## Conclusion

- `intl.ojeur.cloud` is the correct overseas/proxy entry. It is consistently under ~1.3s for first-screen route readiness in this run.
- `www.ojeur.cloud` should remain the domestic entry but should not be forced for proxy users. Under proxy, `www` had large variance and timeouts.
- Further frontend performance work should focus on data-complete timing for `Dashboard` and `Advanced Analysis`, especially non-cached API chains after the static shell is visible.
- Do not use Order Genius results to justify BOM Admin code changes in this performance line; it was included only as a cross-page comparison point.
