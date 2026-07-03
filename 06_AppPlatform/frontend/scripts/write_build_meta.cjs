const { execSync } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const dist = path.resolve(__dirname, "..", "dist");

function walkFiles(root) {
  const items = fs.readdirSync(root, { withFileTypes: true });
  const files = [];
  for (const item of items) {
    const itemPath = path.join(root, item.name);
    if (item.isDirectory()) {
      files.push(...walkFiles(itemPath));
    } else if (item.isFile() && item.name !== "build-meta.json") {
      files.push(itemPath);
    }
  }
  return files;
}

function frontendBuildId(root) {
  const hash = crypto.createHash("sha256");
  const files = walkFiles(root).sort();
  for (const file of files) {
    const relativePath = path.relative(root, file).replaceAll(path.sep, "/");
    hash.update(relativePath);
    hash.update("\0");
    hash.update(fs.readFileSync(file));
    hash.update("\0");
  }
  return hash.digest("hex");
}

const meta = {
  commit: "unknown",
  builtAt: new Date().toISOString(),
  nodeVersion: process.version,
  frontendBuildId: frontendBuildId(dist),
};

try {
  meta.commit =
    process.env.DEPLOY_COMMIT_SHA ||
    process.env.GITHUB_SHA ||
    execSync("git rev-parse HEAD", { encoding: "utf-8" }).trim();
} catch {
  // not a git repo or no git available
}

fs.writeFileSync(
  path.join(dist, "build-meta.json"),
  JSON.stringify(meta, null, 2) + "\n",
);

console.log(`build-meta.json written: commit=${meta.commit.slice(0, 7)} frontendBuildId=${meta.frontendBuildId.slice(0, 12)} builtAt=${meta.builtAt}`);
