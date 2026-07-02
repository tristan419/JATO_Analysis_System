#!/usr/bin/env python3
"""Hermes Phase 3 — Code Audit.

Scan git diff for governance issues after Claude Code development.

Usage:
  python 03_Scripts/hermes/hermes_code_audit.py
  python 03_Scripts/hermes/hermes_code_audit.py --base main --head HEAD
  python 03_Scripts/hermes/hermes_code_audit.py --base HEAD~3 --head HEAD

Output:
  hermes/reports/hermes_code_audit_report.md
  hermes/reports/hermes_code_audit_report.json
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Config ───────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[2]

SEVERITY = {
    "INFO": 0,
    "WARNING": 1,
    "NEEDS_REVIEW": 2,
    "BLOCKER": 3,
}

SECRET_PATTERNS = [
    (r'postgresql\+asyncpg://[a-zA-Z0-9_]+:[^@\s\'"]+@', "DB connection string with password"),
    (r"sk-[a-zA-Z0-9]{20,}", "OpenAI/DeepSeek API key pattern"),
    (r"AIza[0-9A-Za-z\-_]{35}", "Google API key pattern"),
    (r"-----BEGIN (RSA|EC|OPENSSH|DSA) PRIVATE KEY-----", "Private key in code"),
    (r'api_key\s*=\s*"[^"]{20,}"', 'Hardcoded api_key="..."'),
    (r'password\s*=\s*"[^"]{8,}"', 'Hardcoded password="..."'),
]

# Directories to skip entirely
SKIP_DIRS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".claude",
    "04_Processed_data",
    "01_RAW_DATA",
}

# Files that should never contain secrets but are safe to scan
CHECK_PATTERNS_IN = {".py", ".ts", ".tsx", ".yml", ".yaml", ".sh", ".env", ".json", ".js"}

# Registry files
REGISTRY_FILES = {
    "hermes/source_registry.yaml": "Source Registry",
    "hermes/pipeline_registry.yaml": "Pipeline Registry",
    "hermes/feature_registry.yaml": "Feature Registry",
    "hermes/prompt_registry.yaml": "Prompt Registry",
    "hermes/artifact_registry.yaml": "Artifact Registry",
}

EXAMPLE_ENV_FILE = "03_Scripts/deploy/systemd/jato-fullstack-backend.env.example"
VOC_ENV_EXAMPLE = "03_Scripts/deploy/systemd/jato-voc.env.example"
NEWS_ENV_EXAMPLE = "03_Scripts/deploy/systemd/jato-country-news.env.example"
MSRP_ENV_EXAMPLE = "03_Scripts/deploy/systemd/jato-msrp.env.example"

FRONTEND_TYPES_FILE = "06_AppPlatform/frontend/src/types/index.ts"

# Env var patterns
ENV_VAR_PATTERN = re.compile(
    r'\b(?:os\.getenv|os\.environ|process\.env)\s*[\[\(]\s*["\']([A-Z][A-Z0-9_]+)["\']',
)
ENV_VAR_BACKEND = re.compile(
    r'(?:os\.getenv|getenv)\s*\(\s*["\']([A-Z][A-Z0-9_]+)["\']',
)

SYSTEMD_TIMER_PATTERN = re.compile(r"OnCalendar=")
AIRFLOW_SCHEDULE_PATTERN = re.compile(r'schedule\s*=\s*["\']')
CRAWLER_ADD_PATTERNS = [
    re.compile(r"class\s+\w+Extractor\b"),
    re.compile(r"class\s+\w+Fetcher\b"),
    re.compile(r"@register_extractor"),
]


def _run_git_diff(base: str, head: str) -> str:
    """Get git diff between two refs."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-status", base, head],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        return result.stdout
    except Exception as exc:
        print(f"[ERROR] git diff failed: {exc}")
        sys.exit(1)


def _run_git_diff_full(base: str, head: str) -> str:
    """Get full git diff with content."""
    try:
        result = subprocess.run(
            ["git", "diff", base, head],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
        return result.stdout
    except Exception as exc:
        print(f"[ERROR] git diff failed: {exc}")
        sys.exit(1)


def _parse_diff_name_status(output: str) -> list[dict]:
    """Parse 'git diff --name-status' into file change list."""
    changes = []
    for line in output.strip().split("\n"):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) >= 2:
            status = parts[0][0]  # M, A, D, R
            path = parts[-1]
        else:
            status = "?"
            path = line.strip()
        changes.append({"status": status, "path": path})
    return changes


# ── Check Functions ──────────────────────────────────────────────────


def check_secret_leaks(full_diff: str, files: list[str]) -> list[dict]:
    """Check diff for secret/credential patterns."""
    findings = []
    for pattern, desc in SECRET_PATTERNS:
        for match in re.finditer(pattern, full_diff):
            # Only flag if the line was ADDED (starts with +)
            line_start = max(0, match.start() - 200)
            line_context = full_diff[line_start: match.end()]
            if any(l.startswith("+") and match.group() in l for l in line_context.split("\n")):
                findings.append(
                    {
                        "severity": "BLOCKER",
                        "area": "Secret Leak",
                        "finding": f"Potential {desc} in diff",
                        "pattern": pattern,
                        "file": "diff",
                        "suggestedFix": "Remove secret from code. Use environment variables or GitHub Secrets.",
                    }
                )
    return findings


def check_api_frontend_consistency(files: list[str], full_diff: str) -> list[dict]:
    """Check if backend API routes changed but frontend types were not updated."""
    findings = []
    backend_routes_changed = any(
        "routes/" in f or "schemas.py" in f for f in files if f.startswith("06_AppPlatform/backend")
    )
    frontend_types_changed = any(FRONTEND_TYPES_FILE in f for f in files)

    if backend_routes_changed and not frontend_types_changed:
        findings.append(
            {
                "severity": "NEEDS_REVIEW",
                "area": "API Contract",
                "finding": "Backend API routes or schemas changed but frontend types/index.ts was not updated",
                "suggestedFix": "Verify frontend types match the updated backend serializers.",
            }
        )
    return findings


def check_db_schema_migration(files: list[str], full_diff: str) -> list[dict]:
    """Check if DB models changed but no migration file was added."""
    findings = []
    models_changed = any("models.py" in f and f.startswith("06_AppPlatform/backend") for f in files)
    migration_added = any("alembic" in f.lower() and "migration" in f.lower() for f in files)

    if models_changed and not migration_added:
        findings.append(
            {
                "severity": "BLOCKER",
                "area": "Database Schema",
                "finding": "SQLAlchemy models changed but no Alembic migration file was added",
                "suggestedFix": "Generate an Alembic migration: alembic revision --autogenerate -m 'description'",
            }
        )
    return findings


def check_env_var_documentation(files: list[str], full_diff: str) -> list[dict]:
    """Check if new env vars were added but example env file was not updated."""
    findings = []

    # Find new env var references in added/changed backend files
    new_env_vars = set()
    for line in full_diff.split("\n"):
        if line.startswith("+") and not line.startswith("+++"):
            for match in ENV_VAR_BACKEND.finditer(line):
                new_env_vars.add(match.group(1))

    if new_env_vars and EXAMPLE_ENV_FILE not in files:
        findings.append(
            {
                "severity": "WARNING",
                "area": "Environment Variables",
                "finding": (
                    f"New env var(s) referenced but {EXAMPLE_ENV_FILE} was not updated: "
                    f"{', '.join(sorted(new_env_vars)[:5])}"
                ),
                "suggestedFix": (
                    f"Add the new env vars to {EXAMPLE_ENV_FILE} with safe defaults "
                    "(commented out if secret)."
                ),
            }
        )
    return findings


def check_registry_updates(files: list[str], full_diff: str) -> list[dict]:
    """Check if code changes should trigger registry updates."""
    findings = []

    # Crawler/scraper changes → Source/Pipeline Registry
    scraper_changed = any(f.startswith("07_ScrapingToolkit/") for f in files)
    if scraper_changed:
        for reg_file, reg_name in REGISTRY_FILES.items():
            if reg_file not in files:
                findings.append(
                    {
                        "severity": "WARNING",
                        "area": "Registry Gap",
                        "finding": f"Scraping Toolkit changed but {reg_name} ({reg_file}) was not updated",
                        "suggestedFix": (
                            f"Review and update {reg_file} if new sources, pipelines, "
                            "or artifacts were added."
                        ),
                    }
                )

    # Airflow DAG changes → Pipeline Registry
    airflow_changed = any("airflow/dags/" in f for f in files)
    if airflow_changed and "hermes/pipeline_registry.yaml" not in files:
        findings.append(
            {
                "severity": "WARNING",
                "area": "Registry Gap",
                "finding": "Airflow DAG changed but Pipeline Registry was not updated",
                "suggestedFix": "Update hermes/pipeline_registry.yaml with any new or modified DAGs.",
            }
        )

    # Systemd file changes → Pipeline Registry
    systemd_changed = any("systemd" in f and (f.endswith(".service") or f.endswith(".timer")) for f in files)
    if systemd_changed and "hermes/pipeline_registry.yaml" not in files:
        findings.append(
            {
                "severity": "WARNING",
                "area": "Registry Gap",
                "finding": "systemd unit files changed but Pipeline Registry was not updated",
                "suggestedFix": "Update hermes/pipeline_registry.yaml with timer schedule changes.",
            }
        )

    # New API routes → Feature Registry
    new_routes = any(
        f.startswith("06_AppPlatform/backend/app/api/routes/") and any(f.endswith(ext) for ext in [".py"])
        for f in files
    )
    if new_routes and "hermes/feature_registry.yaml" not in files:
        findings.append(
            {
                "severity": "WARNING",
                "area": "Registry Gap",
                "finding": "Backend API routes changed but Feature Registry was not updated",
                "suggestedFix": "Update hermes/feature_registry.yaml with any new or modified features.",
            }
        )

    return findings


def check_duplicate_scheduling(files: list[str], full_diff: str) -> list[dict]:
    """Check if new schedule was added that duplicates existing ones."""
    findings = []

    # Check if diff adds a schedule
    has_new_schedule = False
    for line in full_diff.split("\n"):
        if line.startswith("+") and not line.startswith("+++"):
            if SYSTEMD_TIMER_PATTERN.search(line) or AIRFLOW_SCHEDULE_PATTERN.search(line):
                has_new_schedule = True
                break

    if has_new_schedule:
        findings.append(
            {
                "severity": "NEEDS_REVIEW",
                "area": "Scheduling",
                "finding": "New schedule added. Verify it does not duplicate existing schedules in Pipeline Registry.",
                "suggestedFix": (
                    "Check hermes/pipeline_registry.yaml for duplicate scheduling. "
                    "See gap.pipeline.duplicate_news_scheduling for an existing example."
                ),
            }
        )

    return findings


def check_prompt_changes(files: list[str], full_diff: str) -> list[dict]:
    """Check if prompts changed but version/history was not updated."""
    findings = []
    prompt_files = [
        "country_chat_service.py",
        "assistant.py",
        "news_digest_service.py",
        "voc_enricher.py",
    ]

    prompt_roots = ("06_AppPlatform/backend", "07_ScrapingToolkit")
    prompt_changed = any(
        any(pf in f for pf in prompt_files) and f.startswith(prompt_roots) for f in files
    )

    if prompt_changed:
        findings.append(
            {
                "severity": "WARNING",
                "area": "Prompt Versioning",
                "finding": (
                    "A file containing LLM prompts was modified. Prompt version should be bumped "
                    "if content changed."
                ),
                "suggestedFix": "Update prompt version in hermes/prompt_registry.yaml if prompt content was modified.",
            }
        )

    return findings


def check_frontend_type_consistency(files: list[str], full_diff: str) -> list[dict]:
    """Check if frontend types changed but backend schemas were not checked."""
    findings = []
    types_changed = FRONTEND_TYPES_FILE in files

    if types_changed:
        findings.append(
            {
                "severity": "INFO",
                "area": "Type Consistency",
                "finding": (
                    "frontend/src/types/index.ts was modified. Verify backend serializers "
                    "produce matching shapes."
                ),
                "suggestedFix": "Run backend tests and verify contract test coverage.",
            }
        )
    return findings


def check_artifact_deletion(changes: list[dict]) -> list[dict]:
    """Check if any core artifact or script was deleted."""
    findings = []
    deleted = [c["path"] for c in changes if c["status"] == "D"]
    critical_paths = [
        "03_Scripts/",
        "07_ScrapingToolkit/",
        "06_AppPlatform/",
        "airflow/",
        ".github/workflows/",
    ]
    critical_deletes = [d for d in deleted if any(d.startswith(p) for p in critical_paths)]
    if critical_deletes:
        findings.append(
            {
                "severity": "NEEDS_REVIEW",
                "area": "Artifact Deletion",
                "finding": f"Critical path files deleted: {', '.join(critical_deletes[:5])}",
                "suggestedFix": "Verify the deletion is intentional and downstream consumers are updated.",
            }
        )
    return findings


def check_docs_updated(files: list[str]) -> list[dict]:
    """Check if code changes have corresponding doc updates."""
    findings = []
    has_code_changes = any(
        f.startswith(("06_AppPlatform/", "07_ScrapingToolkit/", "03_Scripts/", "airflow/")) for f in files
    )
    has_doc_changes = any(f.startswith("Markdown_Readme/") for f in files)

    if has_code_changes and not has_doc_changes:
        findings.append(
            {
                "severity": "INFO",
                "area": "Documentation",
                "finding": "Code changes detected but no documentation files were updated",
                "suggestedFix": "Review if Markdown_Readme/ docs need updating for the changes.",
            }
        )
    return findings


# ── Main ─────────────────────────────────────────────────────────────


def run_audit(base: str, head: str) -> dict:
    """Run all audit checks and return structured results."""
    print(f"[Hermes Code Audit] base={base} head={head}")

    # Get file list and full diff
    name_status = _run_git_diff(base, head)
    all_files = _parse_diff_name_status(name_status)
    changed_files = [f["path"] for f in all_files]

    print(f"[Hermes Code Audit] {len(changed_files)} files changed")
    for f in changed_files[:20]:
        print(f"  {f}")
    if len(changed_files) > 20:
        print(f"  ... and {len(changed_files) - 20} more")

    full_diff = _run_git_diff_full(base, head)

    # Run all checks
    all_findings: list[dict] = []
    all_findings.extend(check_secret_leaks(full_diff, changed_files))
    all_findings.extend(check_api_frontend_consistency(changed_files, full_diff))
    all_findings.extend(check_db_schema_migration(changed_files, full_diff))
    all_findings.extend(check_env_var_documentation(changed_files, full_diff))
    all_findings.extend(check_registry_updates(changed_files, full_diff))
    all_findings.extend(check_duplicate_scheduling(changed_files, full_diff))
    all_findings.extend(check_prompt_changes(changed_files, full_diff))
    all_findings.extend(check_frontend_type_consistency(changed_files, full_diff))
    all_findings.extend(check_artifact_deletion(all_files))
    all_findings.extend(check_docs_updated(changed_files))

    # Count by severity
    blockers = [f for f in all_findings if f["severity"] == "BLOCKER"]
    needs_review = [f for f in all_findings if f["severity"] == "NEEDS_REVIEW"]
    warnings = [f for f in all_findings if f["severity"] == "WARNING"]
    infos = [f for f in all_findings if f["severity"] == "INFO"]

    # Overall risk
    if blockers:
        risk = "BLOCKER"
        risk_score = 100
    elif needs_review:
        risk = "NEEDS_REVIEW"
        risk_score = 60
    elif warnings:
        risk = "WARNING"
        risk_score = 30
    else:
        risk = "LOW"
        risk_score = 5

    return {
        "base": base,
        "head": head,
        "filesChanged": len(changed_files),
        "findings": all_findings,
        "summary": {
            "riskLevel": risk,
            "riskScore": risk_score,
            "blockerCount": len(blockers),
            "needsReviewCount": len(needs_review),
            "warningCount": len(warnings),
            "infoCount": len(infos),
        },
    }


def _generate_report(results: dict) -> str:
    """Generate markdown audit report."""
    s = results["summary"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines: list[str] = []
    lines.append(f"# Hermes Code Audit Report\n")
    lines.append(f"## Summary\n")
    lines.append(f"- **Base:** `{results['base']}`")
    lines.append(f"- **Head:** `{results['head']}`")
    lines.append(f"- **Files changed:** {results['filesChanged']}")
    lines.append(f"- **Risk Score:** {s['riskScore']}/100 ({s['riskLevel']})")
    lines.append(f"- **Generated at:** {now}")
    lines.append(
        f"- **Blockers:** {s['blockerCount']} | **Needs Review:** {s['needsReviewCount']} | "
        f"**Warnings:** {s['warningCount']} | **Info:** {s['infoCount']}"
    )
    lines.append("")

    if not results["findings"]:
        lines.append("_No findings. Audit passed clean._\n")
        return "\n".join(lines)

    # Findings table
    lines.append(f"## Findings\n")
    lines.append(f"| Severity | Area | Finding | Suggested Fix |")
    lines.append(f"|---|---|---|---|")
    for f in results["findings"]:
        sev = (
            f"🔴 {f['severity']}"
            if f["severity"] == "BLOCKER"
            else (
                f"🟠 {f['severity']}"
                if f["severity"] == "NEEDS_REVIEW"
                else (f"🟡 {f['severity']}" if f["severity"] == "WARNING" else f"🔵 {f['severity']}")
            )
        )
        finding = f["finding"][:120]
        fix = f["suggestedFix"][:120]
        lines.append(f"| {sev} | {f['area']} | {finding} | {fix} |")
    lines.append("")

    # Required Actions
    lines.append(f"## Required Actions\n")
    for f in results["findings"]:
        if f["severity"] in ("BLOCKER", "NEEDS_REVIEW"):
            lines.append(f"- [ ] **[{f['severity']}]** {f['area']}: {f['suggestedFix']}")
    warnings_only = [f for f in results["findings"] if f["severity"] == "WARNING"]
    infos_only = [f for f in results["findings"] if f["severity"] == "INFO"]
    if not any(f["severity"] in ("BLOCKER", "NEEDS_REVIEW") for f in results["findings"]):
        lines.append("_No blockers or review items required._")
    lines.append("")

    if warnings_only:
        lines.append(f"### Warnings ({len(warnings_only)})\n")
        for f in warnings_only:
            lines.append(f"- [ ] [{f['area']}] {f['suggestedFix']}")
        lines.append("")

    if infos_only:
        lines.append(f"### Info ({len(infos_only)})\n")
        for f in infos_only:
            lines.append(f"- [{f['area']}] {f['finding']}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hermes Code Audit — scan git diff for governance issues",
    )
    parser.add_argument("--base", default="main", help="Base ref (default: main)")
    parser.add_argument("--head", default="HEAD", help="Head ref (default: HEAD)")
    parser.add_argument("--out", default="hermes/reports/hermes_code_audit_report.md")
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()

    out_md = REPO_ROOT / args.out
    out_md.parent.mkdir(parents=True, exist_ok=True)

    if args.json_out:
        out_json = REPO_ROOT / args.json_out
    else:
        out_json = out_md.parent / out_md.name.replace(".md", ".json")

    results = run_audit(args.base, args.head)

    # Write markdown
    report = _generate_report(results)
    out_md.write_text(report, encoding="utf-8")
    print(f"[Hermes Code Audit] Report: {out_md}")

    # Write JSON
    out_json.write_text(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    print(f"[Hermes Code Audit] JSON: {out_json}")

    # Exit code
    if results["summary"]["blockerCount"] > 0:
        print(f"[Hermes Code Audit] BLOCKERS found ({results['summary']['blockerCount']}). Review required.")
        sys.exit(1)
    else:
        print(f"[Hermes Code Audit] Risk: {results['summary']['riskLevel']} — no blockers.")


if __name__ == "__main__":
    main()
