"""Executable architecture contract for SDA-09's Lakebase decision.

The decision is deliberately workload-specific: Lakebase owns small, mutable
coordination state and Delta remains the governed evidence plane. Keeping this
boundary in code gives tests a stable contract to compare with the ADR and the
article instead of relying on prose alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class StoragePlane(StrEnum):
    OPERATIONAL_STATE = "operational_state"
    ANALYTICAL_EVIDENCE = "analytical_evidence"


@dataclass(frozen=True, slots=True)
class StorageBoundary:
    """Responsibilities that must remain separate in the SDA architecture."""

    operational_store: str
    analytical_store: str
    operational_records: tuple[str, ...]
    analytical_records: tuple[str, ...]
    forbidden_operational_payloads: tuple[str, ...]

    def owner_for(self, record_type: str) -> StoragePlane:
        if record_type in self.operational_records:
            return StoragePlane.OPERATIONAL_STATE
        if record_type in self.analytical_records:
            return StoragePlane.ANALYTICAL_EVIDENCE
        raise ValueError(f"unknown SDA record type: {record_type}")

    def validate(self) -> None:
        overlap = set(self.operational_records) & set(self.analytical_records)
        if overlap:
            raise ValueError(f"record types have two owners: {sorted(overlap)}")
        if set(self.forbidden_operational_payloads) & set(self.operational_records):
            raise ValueError("forbidden payloads cannot be operational records")


@dataclass(frozen=True, slots=True)
class DecisionCandidate:
    name: str
    workload_fit: str
    integration_fit: str
    operational_tradeoff: str
    portability_tradeoff: str


@dataclass(frozen=True, slots=True)
class ArchitectureDecision:
    """The narrow decision that SDA-09 records and tests."""

    decision_id: str
    chosen_operational_store: str
    analytical_store: str
    criteria: tuple[str, ...]
    candidates: tuple[DecisionCandidate, ...]
    non_goals: tuple[str, ...]
    revisit_triggers: tuple[str, ...]
    boundary: StorageBoundary

    def validate(self) -> None:
        if not self.decision_id.strip():
            raise ValueError("decision ID must not be empty")
        if not self.criteria or not self.candidates:
            raise ValueError("a decision needs criteria and candidates")
        names = {candidate.name for candidate in self.candidates}
        if self.chosen_operational_store not in names:
            raise ValueError("chosen operational store is not a candidate")
        if self.boundary.operational_store != self.chosen_operational_store:
            raise ValueError("operational boundary and decision disagree")
        if self.boundary.analytical_store != self.analytical_store:
            raise ValueError("analytical boundary and decision disagree")
        self.boundary.validate()


SDA09_DECISION = ArchitectureDecision(
    decision_id="SDA-09",
    chosen_operational_store="Lakebase Postgres Autoscaling",
    analytical_store="Unity Catalog and Delta",
    criteria=(
        "transactional correctness",
        "point lookup latency and concurrency",
        "Databricks integration",
        "governance and least privilege",
        "delivery and operational ownership",
        "portability and exit cost",
    ),
    candidates=(
        DecisionCandidate(
            "Lakebase Postgres Autoscaling",
            "PostgreSQL transactions, constraints, indexes, and short writes fit active state",
            "Databricks identities, bundles, branches, and Apps stay close to the workflow",
            "Managed platform still needs database roles, migrations, reconnects, and recovery checks",
            "Standard PostgreSQL clients reduce migration cost, but Databricks OAuth and branching add coupling",
        ),
        DecisionCandidate(
            "Delta tables",
            "ACID and multi-statement transactions exist, but key constraints are informational and the workload is analytical",
            "Deepest Unity Catalog and Spark integration",
            "Fewest infrastructure names, but less natural application-state semantics",
            "Strong analytical portability, weak fit for this operational boundary",
        ),
        DecisionCandidate(
            "External managed PostgreSQL",
            "Mature PostgreSQL semantics and tooling fit the state workload",
            "Requires separate networking, identities, delivery, monitoring, and ownership",
            "A strong choice where an established database platform already exists",
            "Most platform-independent option",
        ),
    ),
    non_goals=(
        "replacing the lakehouse",
        "storing complete profiles, distributions, or generated datasets in Lakebase",
        "granting the agent unrestricted SQL access",
        "treating Unity Catalog registration as write authorization",
    ),
    revisit_triggers=(
        "Lakebase is unavailable in the required region",
        "a required extension, quota, network control, or recovery objective is unsupported",
        "cross-region resilience or independent database ownership becomes mandatory",
        "measured latency, availability, RTO, or RPO is not met",
        "most consumers move outside Databricks",
        "material cost or connection limits make the service unsuitable",
    ),
    boundary=StorageBoundary(
        operational_store="Lakebase Postgres Autoscaling",
        analytical_store="Unity Catalog and Delta",
        operational_records=(
            "requests",
            "runs",
            "plans",
            "steps",
            "leases",
            "approvals",
            "feedback",
            "transition history",
            "audit events",
            "artifact references",
        ),
        analytical_records=(
            "profiles",
            "distributions",
            "relationship evidence",
            "pattern evidence",
            "validation detail",
            "generated datasets",
            "historical quality and cost analytics",
        ),
        forbidden_operational_payloads=(
            "raw source rows",
            "complete histograms",
            "generated synthetic tables",
        ),
    ),
)


SDA09_DECISION.validate()
