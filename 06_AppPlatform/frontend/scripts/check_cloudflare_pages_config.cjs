#!/usr/bin/env node

const directTencentApiBase = /^https:\/\/www\.ojeur\.cloud\/v1\/?$/i;
const apiBase = String(process.env.VITE_API_BASE || "").trim();
const isCloudflarePages = process.env.CF_PAGES === "1" || process.env.CF_PAGES === "true";

if (!isCloudflarePages) {
  process.exit(0);
}

if (directTencentApiBase.test(apiBase)) {
  console.error([
    "Cloudflare Pages intl build is misconfigured.",
    "VITE_API_BASE must not be https://www.ojeur.cloud/v1 because that bypasses the same-origin Pages Function cache.",
    "Use VITE_API_BASE=/v1 and set the Pages Function runtime variable API_ORIGIN=https://www.ojeur.cloud.",
  ].join("\n"));
  process.exit(1);
}
