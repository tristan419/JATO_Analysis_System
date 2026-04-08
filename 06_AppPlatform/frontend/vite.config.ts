import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
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
          if (id.includes("react-dom") || id.includes("/react/")) {
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
    port: 5173
  }
});
