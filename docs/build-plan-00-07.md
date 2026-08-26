# Synthetic Data Agent Articles 00–07: Complete Build and Execution Plan

This plan is the executable scope for the series charter (Article 00/index) and
Articles 01–07. It distinguishes article instructions from implementation
requirements and records the evidence required to claim completion.

## Completion rule

An item is complete only when the implementation, contract, negative tests, and
release evidence named in the item exist. A local unit test cannot claim governed
Unity Catalog or Databricks execution. Raw source values must not be persisted in
metadata, profile, relationship, or pattern artifacts unless an explicitly
approved vocabulary is part of the contract.

## 00 — Series charter and capability boundary

- [x] Define intended uses: development, QA, demonstrations, performance testing,
  analytics, and AI experimentation.
- [x] Define realism layers: schema/types, distributions, nulls, dependencies,
  relationships, temporal behavior, topology, and controlled defects.
- [x] State the privacy caveat: synthetic does not automatically mean anonymous,
  private, unbiased, or safe.
- [x] Identify the article progression and map it to repository modules.
- [ ] Add a generated capability matrix that links every charter claim to an
  implementation module, test file, and release gate.

## 01 — Why build the agent on Databricks

- [x] Keep Unity Catalog as the governed source boundary.
- [x] Keep Delta as the analytical artifact boundary.
- [x] Keep deterministic tools responsible for measurement and execution.
- [x] Keep the agent responsible for intent interpretation, coordination, and
  explanation rather than generation or governance.
- [x] Document that metadata and evidence are not proof of business correctness.
- [ ] Add a runnable end-to-end charter smoke test showing evidence → plan →
  generation → validation without source-row serialization.

## 02 — System architecture and contracts

- [x] Define request, scope, metadata, profile, relationship, pattern, plan,
  approval, attempt, artifact, validation, and publication models.
- [x] Define immutable, versioned generation plans with fingerprints.
- [x] Define explicit workflow states, rejection, cancellation, retry, expiry,
  and approval boundaries.
- [x] Separate Delta evidence from operational workflow state.
- [x] Enforce fail-closed approvals and plan-bound receipts/manifests.
- [x] Reject unsafe identifiers and prevent raw values in durable artifacts.
- [ ] Add a contract matrix test covering every state transition and every stage's
  required input/output artifact references.
- [ ] Add idempotency and retry tests for every durable write path.

## 03 — Declarative Automation Bundles

- [x] Maintain reusable package code and thin Spark job entrypoints.
- [x] Define development, staging, and production targets and variables.
- [x] Wire metadata, profiling, relationship, pattern, generation, and validation
  task dependencies in the bundle.
- [x] Validate bundle syntax and controlled-target preflight behavior.
- [x] Pin quality-tool versions so CI is reproducible.
- [x] Build source distributions and wheels and inspect release archives.
- [ ] Add a bundle smoke test that asserts all upstream artifact IDs reach the
  pattern task and all output locations are target-scoped.
- [ ] Add governed deployment evidence using a service principal and least-
  privilege Unity Catalog grants.

## 04 — Unity Catalog metadata reader

- [x] Normalize catalogs, schemas, tables, views, columns, types, and nullability.
- [x] Capture owners, comments, tags, declared keys, foreign keys, and visibility.
- [x] Record sensitivity/classification signals, warnings, provenance, and schema
  versions.
- [x] Distinguish declared metadata from validated evidence.
- [x] Reject unsafe or ambiguous qualified names.
- [ ] Add live controlled Unity Catalog integration tests for visible and hidden
  objects, tags, constraints, views, and permission-denied behavior.
- [ ] Add schema migration/compatibility tests for every persisted metadata field.

## 05 — Source profiling

- [x] Implement numeric summaries, quantiles, point masses, tails, and outliers.
- [x] Implement categorical frequencies, rare values, and long-tail indicators.
- [x] Implement string lengths, format signatures, dates, timestamps, blanks,
  sentinels, nulls, conditional nulls, and freshness.
- [x] Record exact/approximate/sampled/metadata-derived provenance and budgets.
- [x] Reuse profile artifacts without rescanning when fingerprints match.
- [x] Avoid unrestricted raw examples and use safe signatures/fingerprints.
- [ ] Add Spark parity tests for every profile metric, including null semantics,
  skew, empty inputs, singleton inputs, and partition independence.
- [ ] Add controlled Delta read tests proving profile persistence is idempotent.

## 06 — Relationship detection

- [x] Validate declared primary, unique, and foreign-key constraints.
- [x] Infer and score single-column and composite key candidates.
- [x] Measure distinct-key coverage, orphan rates, parent coverage, and fan-out.
- [x] Support nullable and composite foreign keys and bridge-table evidence.
- [x] Produce deterministic dependency graphs with explicit evidence lifecycle.
- [x] Detect and fail closed on unresolved self-reference and cycles.
- [x] Persist raw-value-free relationship receipts and lineage.
- [ ] Add hand-computable fixtures for one-to-many, many-to-many, composite,
  optional, bridge, self-referential, cyclic, and empty relations.
- [ ] Add Spark scale tests proving no production key-domain collection to driver.
- [ ] Add controlled Unity Catalog/Delta integration tests for declared constraints.

## 07 — Cross-column patterns and business rules

- [x] Detect numeric association and preserve support/population evidence.
- [x] Detect conditional categorical distributions and segment behavior.
- [x] Detect conditional missingness and lifecycle/state transitions.
- [x] Detect event ordering and temporal lag distributions.
- [x] Detect candidate rules and preserve declared/user/domain-approved rule
  provenance separately from observed patterns.
- [x] Persist support, confidence, stability, violations, method, population,
  sampling, warnings, limitations, and proposed generation/validation actions.
- [x] Provide bounded Spark adapters for correlation, conditionals, missingness,
  ordering, lag, transitions, and fan-out with actionable unsupported results.
- [x] Enforce deterministic fingerprints, tie handling, immutable inputs, and
  duplicate upstream-reference rejection.
- [ ] Add Spark integration tests for every supported metric on real partitions,
  skewed keys, nulls, unsupported types, and bounded scale.
- [x] Add no-raw-value tests for every detector and persistence path.
- [x] Add conflict-resolution fixtures for overlapping and contradictory rules.
- [x] Add bundle smoke coverage proving all SDA 04–06 artifacts are consumed.

## Cross-article acceptance gates

- [x] `python scripts/check_release.py` passes on a clean release tree.
- [x] `python scripts/check_state_schema.py` passes.
- [x] Ruff format, Ruff lint, and mypy pass.
- [x] Non-Spark unit/contract suite passes.
- [x] Bundle contract and controlled-target preflight pass.
- [x] PostgreSQL state integration passes with the checked-in schema.
- [ ] Spark integration suite passes on supported Python/Spark versions.
- [ ] Controlled Databricks Free Edition smoke proves metadata → profile →
  relationship → pattern artifact handoff and persistence.
- [ ] Security review proves no secrets, unsafe identifiers, raw sensitive values,
  or cross-tenant references enter durable artifacts or logs.
- [ ] Production readiness is not claimed until governed Databricks evidence exists.

## Execution order

1. Close the unchecked SDA 07 Spark and bundle tests.
2. Complete metadata/profile/relationship controlled integration coverage.
3. Add the charter end-to-end smoke and cross-stage contract matrix.
4. Run the full acceptance gates and record skipped tests with reasons.
5. Only then mark this plan complete; unresolved governed-environment tests remain
   explicit blockers rather than being inferred from local tests.
