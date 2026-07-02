/// <reference types="vite/client" />

declare module "*.cjs" {
  const moduleValue: unknown;
  export default moduleValue;
}

declare module "plotly.js-cartesian-dist-min";
