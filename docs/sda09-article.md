# 🤖 SDA 09: Why We Picked Lakebase — Benchmarks Were Not the Whole Story

In the [previous article](https://angelic-articles.com/sda/08-durable-agent-memory/), we gave the Synthetic Data Agent durable memory. Requests survive conversations, failed runs leave checkpoints, plans keep their approval history, and reusable profiles remain tied to the source versions that produced them.

Lakebase became the operational store behind that workflow. Unity Catalog and Delta remained the home of profiles, relationships, patterns, validation evidence, and generated data.

That left one question open:

Why Lakebase?

The answer is not simply that it was fast enough. A latency graph tells us what happened under one test condition. It does not tell us who will operate the database, how applications authenticate, how schema changes ship, whether constraints are actually enforced, or how the choice fits the rest of the system.

So this article records the decision as an architecture contract. We compare Lakebase with Delta tables and an external PostgreSQL service, run a small workload-specific probe against the real Lakebase dev endpoint, and make the boundary between operational state and analytical evidence executable in the repository.

The result is deliberately narrower than “Lakebase is the best database.” Lakebase is the best fit for the SDA’s operational control plane in the architecture we have built.

## Start with the workload, not the database

### The SDA needs an operational control plane

The state store is not another analytical repository. It coordinates requests, runs, plan versions, workflow steps, checkpoints, leases, approvals, artifact references, warnings, failures, and retries.

Those records are small, but they change frequently while the application, Databricks jobs, and deterministic tools coordinate the same workflow. The useful operations are ordinary application-database operations:

- find one request or run by identifier;
- create a request exactly once when a client retries;
- update a state projection only if its version is still current;
- acquire a lease before a worker executes a step;
- renew that lease without letting another worker take the same work;
- record a failed attempt without erasing it when a retry starts;
- approve a plan without rewriting the earlier decision; and
- commit a step checkpoint together with its artifact references.

That workload wants short transactions, indexed point lookups, enforced relationships, conditional writes, and safe concurrency. It looks like an operational PostgreSQL workload.

The SDA does not need another place to store synthetic tables. It needs a database that can safely decide what the system is allowed to do next.

### Analytical evidence has a different shape

Profiles, histograms, categorical distributions, relationship metrics, pattern evidence, validation details, generated datasets, and long-term quality histories are different artifacts. They are larger, mostly immutable or append-oriented, and naturally processed through Spark and SQL.

Those artifacts remain in Unity Catalog and Delta. The state store keeps their operational index: an artifact ID, source snapshot, configuration fingerprint, location, status, and compact summary. It does not copy every histogram bucket into a run table.

This is the boundary introduced in Article 8:

Lakebase remembers what the workflow is doing and which evidence it is allowed to use. Delta preserves the evidence of what the system learned and produced.

The boundary is now encoded in `src/sda/lakebase_decision.py`. The code gives operational ownership to requests, runs, plans, steps, leases, approvals, feedback, transition history, audit events, and artifact references. It gives analytical ownership to profiles, distributions, relationship evidence, pattern evidence, validation detail, generated datasets, and historical quality and cost analytics.

It also names three things that do not belong in the operational store: raw source rows, complete histograms, and generated synthetic tables.

## Turn requirements into decision criteria

Before comparing products, we need criteria that describe the work. Otherwise, the decision matrix becomes a retrospective justification for a choice already made.

### Transactional correctness

The state store must be able to:

- create each logical request once;
- start a run against a specific plan version;
- acquire a lease before executing a step;
- reject an update based on a stale version;
- preserve a rejected or superseded plan;
- complete a step and register its output references together; and
- preserve failed attempts instead of resetting their history.

These operations require transactions, constraints, conditional writes, and concurrency controls. Low query latency is useful only if the result is correct.

### Latency and concurrency

The relevant benchmark is not a maximum scan-throughput contest. The questions are smaller and more specific:

- How quickly can the application retrieve one state record?
- How quickly can it complete a short state transaction?
- What happens when two workers claim the same stage?
- What happens when a connection is interrupted?
- What latency appears when suspended compute resumes?

Lakebase scale-to-zero closes idle connections and discards session state such as temporary tables, prepared statements, advisory locks, and `LISTEN`/`NOTIFY` registrations. The application must tolerate a new session rather than quietly depending on the old one. Databricks documents this behavior in [Postgres compatibility](https://docs.databricks.com/aws/en/oltp/projects/compatibility).

### Databricks integration

The comparison also needs to include the surrounding platform:

- Databricks Apps, jobs, and notebooks;
- users, groups, service principals, and OAuth;
- Unity Catalog and Delta;
- Declarative Automation Bundles; and
- isolated development, staging, and production environments.

Every additional credential flow, network path, firewall rule, synchronization process, and deployment system is another component to secure and operate.

### Governance and security

We need identity mapping, least-privilege roles, infrastructure permissions, auditability, encryption, private connectivity, and a clear separation between operational state and sensitive analytical evidence.

Lakebase has separate authorization layers. Project permissions control infrastructure operations such as branches and compute. PostgreSQL roles and grants control direct database access. A Unity Catalog registration governs the read-only catalog representation. These layers complement one another; they are not automatically synchronized.

The [Lakebase Unity Catalog registration documentation](https://docs.databricks.com/aws/en/oltp/projects/register-uc) is explicit about the boundary: registration creates a read-only catalog. Direct writes still go through PostgreSQL access, where PostgreSQL roles remain authoritative.

### Delivery and operational ownership

The decision includes the full operating model:

- infrastructure as code and schema migrations;
- environment isolation;
- backup and recovery testing;
- monitoring and connection management;
- upgrades and incident response; and
- cost ownership and attribution.

A good benchmark result can still produce a poor architecture if the database requires a second delivery platform, duplicated monitoring, bespoke connectivity, and an unclear owner.

### Portability and exit cost

Platform integration has value, but it also creates coupling. We need to know how much of the application remains ordinary PostgreSQL: SQL, drivers, schemas, migrations, and tools. We also need to name the Databricks-specific dependencies: OAuth roles, branches, bundle resources, and optional Unity Catalog registration.

“PostgreSQL compatible” is not a reason to skip testing product-specific behavior. It is a reason to make the exit path understandable and to test the differences that matter.

## Candidate one: put everything in Delta

Delta was an attractive option because it already holds the SDA’s evidence plane. It provides ACID transactions, schema enforcement, table history, time travel, Unity Catalog governance, and scalable processing through Spark and SQL.

For full profiles, relationship graphs, pattern registries, validation detail, historical metrics, and generated synthetic tables, Delta remains the right choice.

We should not reject Delta using the old claim that it lacks transactions. Databricks now supports transactions spanning multiple statements and tables for qualifying Unity Catalog managed Delta or Iceberg tables with catalog commits enabled. The feature has requirements: supported table types, supported compute, and transaction-safe operations. DDL and some metadata operations remain outside the transaction boundary, and interactive transactions use more conservative conflict detection. See [Databricks transactions](https://docs.databricks.com/aws/en/transactions) and [catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits).

The more relevant distinction is integrity. Databricks enforces `NOT NULL` and `CHECK` constraints, but primary-key, foreign-key, and unique constraints are informational rather than enforced. That is documented in [Constraints on Databricks](https://docs.databricks.com/aws/en/tables/constraints).

None of this makes Delta inferior. It makes the boundary clearer. A Delta table can represent many state-machine patterns with `MERGE`, optimistic concurrency, and transactions. The question is whether those patterns are the most natural way to implement a small application run store with enforced relational identity, point updates, leases, and retries.

For the SDA, Delta was retained for the work it handles best. Using it for every layer would remove one technology name while making the operational repository less natural and more constrained.

## Candidate two: use external managed PostgreSQL

External PostgreSQL is not a weak alternative. PostgreSQL provides mature transaction and concurrency semantics, standard client interfaces, extensive operational tooling, and familiar patterns for monitoring, backup, high availability, and replication.

It would be the right choice in an organization that already has a managed PostgreSQL platform, approved extensions, standard network patterns, and a database team that owns migrations and incidents. It would also be a good choice if most applications and users lived outside Databricks.

The cost is architectural rather than merely financial. An external service adds another platform boundary:

- separate provisioning and deployment;
- independent networking and private endpoints;
- another identity, credential, and secret lifecycle;
- database permissions outside the Databricks model;
- separate monitoring, backup testing, and incident response; and
- another ownership agreement between the application, data platform, security, and database teams.

None of that is inherently bad. Mature organizations operate these controls successfully every day. For this project, however, the additional boundary would add work without improving profiling, generation, or validation.

External PostgreSQL remains a credible alternative. It did not lose because it is a worse database. It lost because the SDA team does not already have a separate database platform whose existing value outweighs the integration cost.

## Candidate three: Lakebase Postgres Autoscaling

Lakebase supplies a managed PostgreSQL service integrated with Databricks. That makes it a natural home for the SDA’s operational state: short transactions, constraints, indexed lookups, workflow records, and concurrent state changes.

This is the fundamental reason for the choice. Autoscaling, scale-to-zero, branching, OAuth roles, and bundle resources support the decision. They do not replace workload fit.

### Less application plumbing

Databricks Apps can add a Lakebase resource. The App uses its service principal; Databricks creates a matching PostgreSQL role and injects connection settings such as `PGHOST`, `PGDATABASE`, `PGUSER`, and `PGSSLMODE`. The [Lakebase Apps documentation](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakebase) describes the resource boundary and the permissions it grants.

The SDA’s `LakebaseConnectionConfig.from_environment` consumes those non-secret settings. Token acquisition remains lazy, so a new connection can ask for a fresh credential instead of holding a token forever.

The same adapter can use a standard PostgreSQL DSN for jobs and local acceptance. `connect_lakebase` calls the token provider on each connection attempt, uses TLS, applies bounded exponential backoff, and retries connection establishment only. It never retries a repository mutation. Retrying a connection is one problem; repeating an approval insert or state transition is another.

### Close to Unity Catalog without becoming Delta

A Lakebase database can be registered in Unity Catalog as a read-only catalog. That enables discovery, governed read access, lineage, and cross-source queries beside Delta data. It does not turn the registration into the write path.

That gives the SDA a useful hybrid architecture:

- Lakebase handles active requests, runs, approvals, leases, and checkpoints.
- A registered catalog can expose selected operational state for analysis.
- Unity Catalog and Delta retain full profiles, validation evidence, and synthetic outputs.
- Direct PostgreSQL writes continue to use database roles and grants.

Registration is optional for the state repository and was not claimed as a live acceptance result for this branch.

### It fits the deployment model

The SDA has used Declarative Automation Bundles since Article 3. Lakebase projects, branches, endpoints, roles, and databases are declared in `bundle/lakebase.yml` alongside the jobs and applications. Databricks documents Lakebase bundle support in [Manage Lakebase with Declarative Automation Bundles](https://docs.databricks.com/aws/en/oltp/projects/manage-with-bundles).

The dev target uses an isolated, non-expiring `dev` branch, a read/write primary endpoint, an OAuth role, and the `sda_state` database. The root `production` branch is protected, and the bundle declares a daily snapshot schedule with seven-day retention.

This does not replace migrations. The versioned schema in `schema/lakebase/001_state_schema.sql` is applied explicitly by `scripts/apply_state_schema.py`, not when the application starts. The schema runner hashes the filename and SQL content, records the result in `sda_schema_versions`, and makes the second application a no-op.

## The benchmark established viability, not the decision

The repository now contains `scripts/benchmark_state_store.py`. It is intentionally small and disposable. It measures the two operations that justify an operational state store:

1. `connect_and_ping`: open a fresh PostgreSQL connection and run `SELECT 1`;
2. `request_insert_select_delete`: insert one request, read it back, and delete it in one short transaction.

The probe uses a unique request marker and deletes it in a `finally` block. It never prints the DSN or credential. The benchmark is not a synthetic-data throughput benchmark, and it is not a controlled three-way comparison because no external PostgreSQL service or Delta control-plane implementation was provisioned for the test.

On 13 November 2026, against the deployed `dev` endpoint with a 0.5–1.0 CU autoscaling range and a 300-second suspend timeout, 20 measured samples followed three warm-up samples:

| Operation | Samples | p50 | p95 | Maximum |
|---|---:|---:|---:|---:|
| Fresh connection plus `SELECT 1` | 20 | 1,079.9 ms | 1,135.6 ms | 1,139.6 ms |
| Request insert/select/delete transaction | 20 | 1,321.1 ms | 1,381.2 ms | 1,519.2 ms |

Those figures are observations of one workspace, endpoint state, network path, and run. They are not an SLA, and they should not be compared with a number measured on another platform under different conditions.

They answer the first question: Lakebase is viable for this batch-oriented control plane. They do not answer who should own it, how it should be governed, or whether another platform would be better for a different organization.

## The decision matrix without the spreadsheet

| Criterion | Lakebase Postgres | Delta tables | External PostgreSQL |
|---|---|---|---|
| Transactional state | Best fit for short PostgreSQL transactions, enforced relational constraints, leases, and point updates | ACID is available, but the application-state model is more constrained and key constraints are informational | Strong fit with mature PostgreSQL semantics |
| Analytical scale | Not the destination for complete evidence or generated data | Best fit for profiles, distributions, validation, history, and generated tables | Possible, but would duplicate or complicate the evidence plane |
| Databricks integration | Strong: identities, bundles, branches, endpoints, and Apps stay close to the workflow | Native Unity Catalog and Spark integration | Requires separate networking, credentials, deployment, and monitoring |
| Governance | PostgreSQL roles for direct access; optional read-only Unity Catalog registration; project permissions remain separate | Deepest native Unity Catalog governance for analytical assets | Separate authorization model, with federation if needed |
| Operational ownership | One platform boundary for the SDA team, with explicit database engineering still required | Least infrastructure, but a less natural control-plane workload | Best where a mature database team already exists |
| Portability | Standard PostgreSQL clients reduce exit cost; Databricks OAuth and branches add coupling | Strong analytical portability, but not the chosen state-store semantics | Highest platform independence |

The overall decision is therefore:

Use Lakebase Postgres Autoscaling for the SDA’s operational control plane.

Use Unity Catalog-governed Delta tables for complete analytical artifacts, generated datasets, validation history, quality reporting, cost analysis, and audit datasets.

The benchmark says Lakebase is fast enough for the workload we measured. The architecture says why it is useful in the system we actually have.

## Make the decision an architectural contract

An architecture decision is more useful when it can be tested. SDA-09 adds five concrete contracts around the decision.

### The storage boundary is executable

`src/sda/lakebase_decision.py` names the two storage planes, their record types, their non-goals, and the conditions that should reopen the decision. Tests reject overlapping ownership and verify that raw rows, complete histograms, and generated tables are not operational payloads.

### The connection path is reconnect-safe

`LakebaseConnectionConfig.from_environment` reads non-secret connection settings from the variables Databricks Apps expose. `environment_token_provider` remains lazy. `connect_lakebase` requests a token for each connection attempt and never logs it.

The behavior is covered by unit tests and by the live connection probe. The endpoint was observed in `IDLE` before the fresh-connection cycle and `ACTIVE` after it. The application did not depend on a permanent session.

### The state schema is versioned and relational

The schema contains 11 `sda_*` tables, including schema versions, requests, runs, plans, attempts, approvals, feedback, audit events, transition history, run steps, and artifact decisions. Foreign keys and uniqueness constraints are part of the schema rather than comments in an ADR.

Applying the schema twice on the live dev branch returned no new schema file on either pass. The live check reported:

```text
sda_tables=11
schema_versions=1
disposable_rows=0
```

### The full lifecycle is disposable and repeatable

`scripts/run_sda09_live_lifecycle.py` exercises the part of the contract a benchmark cannot see:

- request creation;
- run creation and a legal transition;
- immutable plan creation;
- audit event and approval persistence;
- a failed step followed by a successful retry;
- a reused artifact decision;
- lease acquisition and renewal; and
- successful attempt completion.

The live lifecycle completed with `live_repository_lifecycle=OK`. The cleanup deletes child history before parent rows, including transition events. The final state check returned zero disposable request rows.

### The generation boundary is bounded and testable

SDA-09 does not move generated rows into Lakebase, but it does make the existing Delta generation boundary executable. `src/sda/generation.py` enforces approved plans, deterministic seeds, row-count budgets, schema contracts, receipts, and generation manifests. The Databricks bundle deploys `src/sda/job_entrypoints/generate_spark.py` as the standalone Spark writer and `src/sda/job_entrypoints/validate_generated_spark.py` as its bounded technical gate. The writer reconstructs the approved plan, verifies its fingerprint, writes a Delta table, and emits a manifest with the run, plan, source snapshot, artifact, receipt, and output fingerprint. The validation job checks the required schema and expected row count, then persists a versioned validation report in Delta.

On 14 November 2026, the `sda` dev acceptance ran that path with an approved three-row plan bounded by five rows. The generator completed with `status=complete` and `generation-manifest-v1`; the validator completed with `technical_disposition=PASS`, observing 3 rows against an expected 3 and the required `run_id` column. The two temporary Delta tables were then dropped through the SQL API, and Unity Catalog confirmed `sda09_disposable_tables=0`. This is a deployment and contract smoke test, not a production-scale capacity claim or a full Article 10 relational-generation scenario.

## Honest caveats

Native integration is not the same as correctness. Lakebase still has product-specific behavior and limits that must be checked against production requirements.

This branch does not claim:

- a measured external PostgreSQL competitor result;
- a measured Delta control-plane competitor result;
- Unity Catalog registration of the Lakebase database;
- production service-principal permissions;
- high availability or cross-region recovery;
- region availability for every future deployment; or
- a production latency or cost SLA.

Those are not hidden failures. They are explicit revisit conditions in [ADR 0004](https://github.com/SauronShepherd/synthetic-data-agent/blob/SDA-09/docs/adr/0004-lakebase-for-operational-state.md): unavailable region, unsupported extension or network control, mandatory independent ownership, failed latency/availability objectives, unmet recovery objectives, consumers moving outside Databricks, or material cost and connection limits.

External PostgreSQL should win if those constraints matter more than platform integration. Delta should continue to win for analytical evidence. The decision is not a logo preference and it is not permanent by ceremony.

## Conclusion: the fastest component is not always the simplest system

The SDA needed more than durable storage. It needed a transactional control plane that could coordinate concurrent tools, prevent duplicate work, preserve approvals, recover from failures, and expose state through ordinary application patterns.

Lakebase matched that workload while staying close to the Databricks applications and workflows already using it. Delta remained the right home for profiles, validation evidence, historical analytics, and generated tables. External PostgreSQL remained technically sound, but it would have introduced more identity, networking, deployment, and operational boundaries for this project.

The benchmark mattered because Lakebase still had to meet the workload’s latency and reconnect requirements. Speed established viability. The architecture established usefulness.

The agent now has governed evidence, durable memory, and a database selected for the work rather than the logo. The bounded generator and validator are deployed and verified; Article 10 can extend that path into full relational generation while preserving the distributions and null behavior that make the source data believable.
