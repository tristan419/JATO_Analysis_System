import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const devProxyTarget =
    env.VITE_DEV_PROXY_TARGET?.trim()
    || env.VITE_API_ORIGIN?.trim()
    || "http://127.0.0.1:8000";

  return {
    plugins: [react()],
    build: {
      rollupOptions: {
        output: {
          manualChunks(id) {
            if (!id.includes("node_modules")) {
              return undefined;
            }
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
            return "vendor";
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
