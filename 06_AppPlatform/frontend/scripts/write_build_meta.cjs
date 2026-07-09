const { execFileSync } = require("child_process");
const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const dist = path.resolve(__dirname, "..", "dist");

function git(args) {
  return execFileSync("git", args, { encoding: "utf-8" }).trim();
}

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

function currentDeployCommit() {
  return (
    process.env.DEPLOY_COMMIT_SHA ||
    process.env.GITHUB_SHA ||
    process.env.CF_PAGES_COMMIT_SHA ||
    git(["rev-parse", "HEAD"])
  );
}

function changedFilesForCommit(commit) {
  try {
    return git(["diff-tree", "--no-commit-id", "--name-only", "-r", commit])
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);
  } catch {
    return [];
  }
}

function subjectForCommit(commit) {
  try {
    return git(["log", "-1", "--format=%s", commit]);
  } catch {
    return "";
  }
}

function parentCommit(commit) {
  try {
    return git(["rev-parse", `${commit}^`]);
  } catch {
    return "";
  }
}

function isHermesDevEventOnly(files) {
  return files.length > 0 && files.every((file) => file === "hermes/dev_events/dev_events.jsonl" || file.startsWith("hermes/dev_events/"));
}

function applicationCommitFromHermesSubject(commit) {
  const subject = subjectForCommit(commit);
  const match = subject.match(/^hermes: auto dev event from push ([0-9a-f]{7,40})$/i);
  if (!match) {
    return "";
  }
  const candidate = match[1];
  if (candidate.length >= 40) {
    return candidate;
  }
  try {
    return git(["rev-parse", candidate]);
  } catch {
    return "";
  }
}

function resolveApplicationCommit(commit) {
  let cursor = commit;
  for (let i = 0; i < 20 && cursor; i += 1) {
    const subjectApplicationCommit = applicationCommitFromHermesSubject(cursor);
    if (subjectApplicationCommit) {
      return subjectApplicationCommit;
    }
    const files = changedFilesForCommit(cursor);
    if (!isHermesDevEventOnly(files)) {
      return cursor;
    }
    cursor = parentCommit(cursor);
  }
  return commit;
}

const meta = {
  commit: "unknown",
  deployCommit: "unknown",
  commitMode: "deploy",
  builtAt: new Date().toISOString(),
  nodeVersion: process.version,
  frontendBuildId: frontendBuildId(dist),
};

try {
  meta.deployCommit = currentDeployCommit();
  meta.commit = resolveApplicationCommit(meta.deployCommit);
  meta.commitMode = meta.commit === meta.deployCommit ? "deploy" : "application";
} catch {
  // not a git repo or no git available
}

fs.writeFileSync(
  path.join(dist, "build-meta.json"),
  JSON.stringify(meta, null, 2) + "\n",
);

console.log(`build-meta.json written: commit=${meta.commit.slice(0, 7)} deployCommit=${meta.deployCommit.slice(0, 7)} frontendBuildId=${meta.frontendBuildId.slice(0, 12)} builtAt=${meta.builtAt}`);
