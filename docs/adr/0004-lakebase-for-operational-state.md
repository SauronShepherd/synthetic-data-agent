# ADR 0004 Lakebase for operational state

Status: accepted for the SDA-09 dev acceptance boundary

Date: 2026-11-13

## Decision

Use Lakebase Postgres Autoscaling as the Synthetic Data Agent's operational
control plane. Keep complete analytical evidence and generated datasets in
Unity Catalog-governed Delta tables.

Lakebase stores the small, mutable records that coordinate work: requests,
runs, plan versions, steps, leases, approvals, feedback, transition history,
audit events, and references to larger artifacts. It does not become a second
lakehouse and it does not receive raw source rows, complete distributions, or
generated synthetic tables.

The machine-readable form of this boundary is
`src/sda/lakebase_decision.py`. The PostgreSQL/Lakebase repository implements
the operational side in `src/sda/state_postgres.py`; the schema is in
`schema/lakebase/001_state_schema.sql`.

## Context

The SDA has two storage workloads with different failure and access patterns.
The control plane performs short transactions, indexed point lookups,
idempotent creation, conditional updates, leases, and concurrent workflow
coordination. The evidence plane contains profiles, distributions,
relationships, patterns, validation detail, historical metrics, and generated
tables that are larger and processed through Spark and SQL.

The decision is therefore workload-specific. It is not a claim that Lakebase
is the best database for every organization or every SDA artifact.

## Criteria

The candidates were evaluated against:

1. transactional correctness;
2. point-lookup latency and concurrency;
3. Databricks integration;
4. governance and least privilege;
5. delivery and operational ownership; and
6. portability and exit cost.

The benchmark is a viability gate for the first two criteria. It is not a
substitute for the complete architecture decision. The reproducible probe is
`scripts/benchmark_state_store.py`; it measures fresh connection plus a small
request insert/select/delete transaction and removes its disposable row.

## Options considered

### Delta tables for everything

Delta remains the right home for analytical evidence. Databricks supports ACID
transactions and, for qualifying Unity Catalog managed tables with catalog
commits, transactions spanning multiple statements and tables. However, the
transaction feature has compute and table requirements, DDL and metadata
limitations, and interactive transactions use conservative conflict detection.
Primary-key, foreign-key, and unique constraints in Databricks are
informational rather than enforced. See [Databricks transactions](https://docs.databricks.com/aws/en/transactions),
[catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits),
and [constraints](https://docs.databricks.com/aws/en/tables/constraints).

This is not an argument that Delta is non-transactional. It is an argument
that the SDA's active run store has a more natural relational application
boundary in PostgreSQL.

### External managed PostgreSQL

External PostgreSQL is a technically valid alternative and may be the better
choice where an established database platform already owns networking,
identity, migrations, monitoring, recovery testing, and incidents. It loses
for this project only when those additional boundaries add more operational
work than value.

No external provider was provisioned for this acceptance. The comparison is
therefore qualitative rather than an apples-to-apples latency benchmark.

### Lakebase Postgres Autoscaling

Lakebase supplies standard PostgreSQL access for low-latency transactional
applications and agent state while keeping projects, branches, endpoints,
roles, and databases close to the Databricks delivery model. The repository
uses parameterized PostgreSQL operations, enforced relational constraints from
the state schema, and a lazy OAuth token provider that is called again when a
new connection is established.

Lakebase resources are declared in `bundle/lakebase.yml` and validated with
the Databricks bundle contract. The bundle creates an isolated non-expiring
dev branch, a read/write endpoint, an OAuth role, the state database, and a
snapshot schedule for the protected root branch.

The product-specific caveats are part of the decision. Scale-to-zero closes
idle connections and discards session state such as temporary tables,
prepared statements, advisory locks, and `LISTEN`/`NOTIFY` registrations. The
connection path must therefore tolerate reconnects and must not rely on
session state. See [Postgres compatibility](https://docs.databricks.com/aws/en/oltp/projects/compatibility).

Databricks Apps inject connection settings such as `PGHOST`, `PGDATABASE`,
`PGUSER`, and `PGSSLMODE` for a Lakebase resource. The code can construct the
non-secret settings from those variables with
`LakebaseConnectionConfig.from_environment`; token acquisition remains lazy.
See [Lakebase resources for Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakebase).

## Governance boundary

Registering a Lakebase database in Unity Catalog creates a read-only catalog
for discovery, permissions, lineage, audit, and cross-source queries. It does
not turn direct PostgreSQL writes into Unity Catalog writes. Direct database
access still depends on PostgreSQL roles and grants, while project permissions
control infrastructure operations. See [Register a Lakebase database in Unity Catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc).

The SDA does not require catalog registration to run its operational repository.
Registration remains an optional read-only analytics integration and is not
claimed as part of this branch's live acceptance.

## Consequences

Positive consequences:

- requests, retries, approvals, leases, and checkpoints have ordinary
  relational semantics;
- the existing Databricks bundle owns the dev infrastructure and environment
  isolation;
- short-lived OAuth credentials can be refreshed on reconnect;
- analytical evidence remains scalable and governed in Delta;
- PostgreSQL clients and SQL keep the exit path legible.

Costs and risks:

- Lakebase still requires schema migrations, grants, token rotation,
  connection-pool discipline, recovery rehearsals, and monitoring;
- Unity Catalog registration does not synchronize permissions with PostgreSQL;
- scale-to-zero adds cold-start and reconnect behavior to test;
- external PostgreSQL may win for organizations with a mature independent
  database platform;
- the dev acceptance does not prove production service-principal permissions,
  region availability, high availability, or cross-region recovery.

## Revisit triggers

Reopen this ADR if Lakebase is unavailable in the required region, a required
extension or network control is unsupported, cross-region resilience or an
independent database lifecycle becomes mandatory, the measured latency or
availability objectives fail, the recovery objectives are not met, most
consumers move outside Databricks, or cost/connection/storage limits become
material.

## Evidence

- Decision and boundary contract: `src/sda/lakebase_decision.py` and
  `tests/test_lakebase_decision.py`.
- Repository and reconnect behavior: `src/sda/state_postgres.py` and
  `tests/test_state_postgres*.py`.
- Schema constraints: `schema/lakebase/001_state_schema.sql` and
  `scripts/check_state_schema.py`.
- Bundle resources: `bundle/lakebase.yml`, `tests/test_bundle_contract.py`, and
  `scripts/check_bundle_contract.py`.
- Workload probe: `scripts/benchmark_state_store.py` and
  `tests/test_benchmark.py`.
- Full disposable lifecycle: `scripts/run_sda09_live_lifecycle.py`.
- SDA-09 acceptance sequence and live evidence: `docs/sda09-build-and-test-plan.md`.
