const { execSync } = require("child_process");
const fs = require("fs");
const path = require("path");

const dist = path.resolve(__dirname, "..", "dist");
const meta = {
  commit: "unknown",
  builtAt: new Date().toISOString(),
  nodeVersion: process.version,
};

try {
  meta.commit = execSync("git rev-parse HEAD", { encoding: "utf-8" }).trim();
} catch {
  // not a git repo or no git available
}

fs.writeFileSync(
  path.join(dist, "build-meta.json"),
  JSON.stringify(meta, null, 2) + "\n",
);

console.log(`build-meta.json written: commit=${meta.commit.slice(0, 7)} builtAt=${meta.builtAt}`);
