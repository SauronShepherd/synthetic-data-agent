"""Stable relationship evidence row builders shared by job entrypoints."""

from __future__ import annotations

from typing import Any

from sda.artifacts.fingerprint import fingerprint


def relationship_analysis_id(identity: dict[str, Any]) -> str:
    reusable = {
        key: value
        for key, value in identity.items()
        if key not in {"run_id", "started_at", "completed_at"}
    }
    return "relationship_analysis_" + fingerprint(reusable)


def relationship_row(
    *, identity: dict[str, Any], evidence: dict[str, Any], policy_version: str, environment: str
) -> dict[str, Any]:
    return {
        "analysis_id": relationship_analysis_id(identity),
        "environment": environment,
        "policy_version": policy_version,
        "evidence_json": evidence,
        **{key: value for key, value in identity.items() if key != "run_id"},
    }
