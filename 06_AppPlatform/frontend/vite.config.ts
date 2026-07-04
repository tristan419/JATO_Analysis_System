import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

const DASHBOARD_CORE_MODULES = [
  "/src/components/CollapsibleDeckHero.tsx",
  "/src/components/CollapsibleFilterSidebar.tsx",
  "/src/components/LazyPlotlyChart.tsx",
  "/src/components/LoadingActionButton.tsx",
  "/src/components/PageFeedback.tsx",
  "/src/components/SearchSelectFilter.tsx",
  "/src/components/TimeAxis.tsx",
  "/src/components/deckControls/DebouncedNumberInput.tsx",
  "/src/components/deckControls/DeckExportDrawer.tsx",
  "/src/components/deckControls/DeckFloatingDrawer.tsx",
  "/src/components/deckControls/DeckControlTabs.tsx",
  "/src/components/ExportPanelHelpers.ts",
  "/src/hooks/useResolvedCountry.ts",
  "/src/pages/dashboardHelpers.ts",
  "/src/utils/bubbleSizing.ts",
  "/src/utils/colors.ts",
  "/src/utils/filterOptions.ts",
  "/src/utils/jatoCountries.ts",
  "/src/utils/pageCache.ts",
  "/src/utils/plotlyDefaults.ts",
  "/src/utils/timeFormatting.ts",
];

function isDashboardCoreModule(id: string): boolean {
  const normalizedId = id.replace(/\\/g, "/");
  return DASHBOARD_CORE_MODULES.some((modulePath) => normalizedId.endsWith(modulePath));
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const devProxyTarget =
    env.VITE_DEV_PROXY_TARGET?.trim()
    || env.VITE_API_ORIGIN?.trim()
    || "http://127.0.0.1:8000";
  const rawAssetBaseUrl = env.VITE_ASSET_BASE_URL?.trim() || "";
  const assetBaseUrl = rawAssetBaseUrl
    ? rawAssetBaseUrl.endsWith("/") ? rawAssetBaseUrl : `${rawAssetBaseUrl}/`
    : "/";

  return {
    base: assetBaseUrl,
    plugins: [react()],
    build: {
      modulePreload: {
        resolveDependencies(_url, deps, context) {
          if (context.hostType === "html") {
            return deps.filter((dep) => !dep.includes("react-vendor"));
          }
          return deps;
        },
      },
      rollupOptions: {
        output: {
          manualChunks(id) {
            if (id.includes("vite/preload-helper")) {
              return "vite-runtime";
            }
            if (isDashboardCoreModule(id)) {
              return "dashboard-core";
            }
            if (!id.includes("node_modules")) {
              return undefined;
            }
            // Keep feature-only libraries out of the first app shell on low-bandwidth links.
            if (
              id.includes("react-plotly.js")
              || id.includes("plotly.js-cartesian-dist-min")
              || id.includes("plotly.js")
            ) {
              return "plotly-vendor";
            }
            if (id.includes("recharts")) {
              return "recharts-vendor";
            }
            if (id.includes("ag-grid")) {
              return "grid-vendor";
            }
            if (id.includes("mermaid")) {
              return "diagram-vendor";
            }
            if (id.includes("html-to-image")) {
              return "export-vendor";
            }
            if (id.includes("animejs")) {
              return "animation-vendor";
            }
            if (id.includes("/d3") || id.includes("d3-")) {
              return "d3-vendor";
            }
            if (id.includes("/buffer/") || id.includes("node_modules/buffer")) {
              return "node-polyfills";
            }
            if (id.includes("react-router")) {
              return "router-vendor";
            }
            if (
              id.includes("scheduler")
              || id.includes("react-dom")
              || id.includes("/react/")
            ) {
              return "react-vendor";
            }
            return undefined;
          },
        },
      },
    },
    resolve: {
      alias: {
        "buffer/": "buffer",
      },
    },
    server: {
      host: "0.0.0.0",
      port: 5173,
      proxy: {
        "/v1": devProxyTarget,
        "/metadata": devProxyTarget,
        "/healthz": devProxyTarget,
      },
    },
  };
});
