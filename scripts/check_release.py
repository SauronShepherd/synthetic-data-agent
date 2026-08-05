"""Fail fast when a release tree contains local or sensitive artifacts."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

FORBIDDEN_NAMES = {
    ".env",
    ".databrickscfg",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    ".venv",
    "venv",
}
SKIP_DIRECTORIES = {
    ".git",
    ".databricks",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    ".testdeps",
    ".release-check",
    "__pycache__",
    "*.egg-info",
}
GENERATED_DIRECTORIES = {"build", "dist"}
FORBIDDEN_SUFFIXES = {".pyc", ".pyo"}
SENSITIVE_PATTERNS = (
    re.compile(r"(?i)(?:DATABRICKS_TOKEN|access[_-]?token)\s*[:=]\s*['\"][A-Za-z0-9_\-]{20,}"),
    re.compile(r"(?i)C:\\\\Users\\\\[^\\\"']+\\\\\.cache\\\\codex"),
)


def violations(root: Path) -> list[str]:
    findings: list[str] = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if any(part in SKIP_DIRECTORIES or part.endswith(".egg-info") for part in relative.parts):
            continue
        if any(part in GENERATED_DIRECTORIES for part in relative.parts):
            findings.append(f"generated release artifact: {relative}")
            continue
        if any(part in FORBIDDEN_NAMES for part in relative.parts):
            findings.append(f"forbidden path: {relative}")
        if path.is_file() and path.suffix.lower() in FORBIDDEN_SUFFIXES:
            findings.append(f"compiled file: {relative}")
        if path.is_file() and path.name != "check_release.py" and path.stat().st_size < 2_000_000:
            text = path.read_text(encoding="utf-8", errors="ignore")
            if any(pattern.search(text) for pattern in SENSITIVE_PATTERNS):
                findings.append(f"sensitive-looking content: {relative}")
    return sorted(set(findings))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", default=".")
    args = parser.parse_args()
    findings = violations(Path(args.root).resolve())
    if findings:
        print("Release hygiene failed:", file=sys.stderr)
        print("\n".join(f"- {finding}" for finding in findings), file=sys.stderr)
        return 1
    print("Release hygiene: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
