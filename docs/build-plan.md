# Synthetic Data Agent: Complete Build Plan

Status: analysis baseline for SDA 07 (`0.7.0.dev0`)

This document is the implementation backlog derived from the SDA 01–07 article
documents and the current repository. The articles are treated as requirements and
design evidence; the repository is the source of truth for what is currently present.

## 1. Baseline and definition of done

### Current baseline

- [x] Unity Catalog metadata discovery exists in `src/sda/tools/uc_metadata_reader.py`.
- [x] Bounded table profiling exists in `src/sda/tools/table_profiler.py` and
  `src/sda/profiling/`.
- [x] Relationship candidates, validation metrics, scoring, fan-out evidence, and
  dependency graphs exist in `src/sda/relationships/`.
- [x] SDA 07 pattern candidates, scoring, precedence, safety, persistence, and Spark
  entrypoint exist in `src/sda/patterns/` and `src/sda/job_entrypoints/pattern_detect_spark.py`.
- [x] Artifact lineage, fingerprints, manifests, loaders, and Delta persistence have
  local contracts and tests.
- [x] Generation-plan, validation-report, and publication artifact types are registered
  in the durable artifact taxonomy.
- [x] The local suite passes: 191 passed, 2 skipped using the checked-in `.testdeps`
  dependency set. CI separates the non-Spark coverage run from the dedicated Spark
  test job.
- [x] The executable local slice includes immutable approved plans, deterministic
  standalone/relational generation, validation/privacy/publication gates, durable
  SQLite workflow state, bounded streaming replay/checkpoints, topology constraints,
  and controlled noise mutations.
- [ ] Production readiness remains explicitly unclaimed until Lakebase/PostgreSQL,
  governed Databricks execution, continuous Structured Streaming, distributed graph
  generation, and workspace integration tests are delivered.

### Global completion gate

- [ ] Every executable stage consumes an immutable, versioned approved plan and emits
  a versioned manifest, receipt, artifact references, metrics, warnings, and lineage.
- [ ] No stage silently turns unavailable evidence into an empty or successful result.
- [ ] Every write is scoped, idempotent, retry-safe, and protected against unsafe
  identifiers, raw sensitive values, and cross-run contamination.
- [ ] Technical validation, privacy approval, human approval, and publication are
  separate decisions with explicit state transitions.
- [ ] Local unit/contract tests, Spark tests, bundle validation, and a small governed
  Databricks integration test all pass before a milestone is released.
- [ ] Documentation, package metadata, bundle variables, schemas, migrations, and
  runbooks describe the same capabilities and milestone boundary.

## 2. Immediate SDA 07 close-out

### 2.1 Resolve repository inconsistencies

- [x] Update `pyproject.toml` description from SDA 06 to SDA 07 and add the actual
  pattern-detection capability.
- [x] Reconcile `README.md`, `docs/architecture.md`, and bundle descriptions so they
  say consistently that patterns are implemented while generation is not.
- [x] Add a formal SDA 07 schema/version compatibility policy for registry and evidence
  artifacts; reject incompatible upstream versions instead of accepting best effort.
- [x] Make `sda_pattern_mode`, sample fraction, seed, support thresholds, budgets, and
  detector/scoring versions part of one validated configuration object and fingerprint.
- [x] Add bundle resources and task dependencies for pattern detection to
  `bundle/resources.yml`; validate dev/staging/prod variable wiring and output tables.
- [x] Add a documented migration/retention policy for `pattern_registry` and
  `pattern_evidence`, including run scope, source snapshot, evidence freshness, and
  supersession.

### 2.2 Finish the SDA 07 detector contract

- [x] Define canonical input adapters for metadata, profile, relationship, and graph
  artifacts; test missing, stale, duplicate, and mismatched upstream references.
- [ ] Complete the pattern taxonomy: numeric association, conditional categorical,
  segment behavior, conditional null, lifecycle/state transition, event ordering,
  temporal lag, probabilistic rule, and declared/approved rule.
- [ ] For every candidate, persist support count/rate, confidence, stability,
  violation count/rate, method, population, sampling, warnings, and limitation text.
- [ ] Separate `OBSERVED_PATTERN`, `DECLARED_RULE`, `APPROVED_RULE`, `REJECTED`, and
  `REVIEW_REQUIRED`; never allow an observed correlation to become a hard constraint.
- [ ] Implement deterministic tie-breaking and conflict resolution across patterns,
  including contradictory conditionals, overlapping null rules, and precedence cycles.
- [ ] Add bounded Spark implementations for all configured metrics; fail with an
  actionable unsupported-metric result when a type or scale is not supported.
- [ ] Prove partition-independent results using canonical ordering/fingerprints rather
  than relying on Spark row order.
- [ ] Add no-raw-values tests for every new detector and persistence path.

### 2.3 SDA 07 test matrix

- [ ] Unit tests for each detector on empty, singleton, constant, highly skewed,
  null-heavy, mixed-type, and high-cardinality inputs.
- [ ] Exact tests for support/confidence/stability/violation formulas and rounding.
- [ ] Determinism tests across repeated runs, changed partition counts, and equivalent
  input ordering.
- [ ] Negative tests for stale evidence, unsupported types, insufficient support,
  unsafe raw examples, invalid thresholds, conflicting rules, and oversized scopes.
- [ ] Contract tests for registry/evidence schemas, lineage, artifact fingerprints,
  idempotent reruns, supersession, and Delta merge behavior.
- [ ] Spark tests for sampling, approximate metrics, skewed keys, null semantics,
  timestamp zones, and empty DataFrames.
- [ ] Bundle smoke test proving the pattern task receives all upstream artifact refs.

## 3. Foundation and workflow control (SDA 01–03, required before generation)

### 3.1 Contracts and state model

- [x] Replace design-only tool placeholders in `src/sda/tools/design_stubs.py` with
  real stage contracts or clearly isolate them as documentation fixtures. The module
  is explicitly documented as a legacy Article 02 fixture and is separate from the
  executable SDA 07 pipeline.
- [x] Define request, scope, evidence snapshot, generation plan, approval, execution
  attempt, artifact, validation report, publication, and feedback models.
- [x] Define the state machine: `REQUESTED -> PLANNED -> AWAITING_APPROVAL ->
  APPROVED -> EXECUTING -> GENERATED_AWAITING_VALIDATION -> VALIDATED ->
  PRIVACY_APPROVED -> PUBLISHED`, with explicit rejection, cancellation, retry, and
  expiry transitions.
- [x] Enforce legal transitions, actor identity, timestamps, reason codes, optimistic
  concurrency, and immutable plan versions.
- [x] Add idempotency keys at request, plan, stage, artifact, and publication level.

### 3.2 Durable operational state (SDA 08–09)

- [x] Implement a Lakebase/PostgreSQL persistence adapter for active workflow state,
  execution attempts, leases, heartbeats, approvals, retries, and compact summaries.
- [x] Add migrations, constraints, indexes, retention, backup/restore, and environment
  configuration; keep analytical evidence and generated rows in Delta.
- [x] Implement lease acquisition, heartbeat renewal, stale-worker recovery, bounded
  retries, and exactly-once logical completion for stage attempts.
- [x] Add repository interfaces with an in-memory test adapter and integration tests
  against a disposable/controlled database.
- [x] Record user corrections and feedback without mutating historical evidence.

### 3.3 Release foundation

- [x] Add CI jobs for lint, mypy, unit tests, Spark tests, package build, bundle
  validation, schema migration checks, and security/hygiene scans.
- [x] Pin or constrain compatible Databricks Runtime, Spark, GraphFrames, connector,
  and Python versions; document optional dependency behavior.
- [ ] Add environment promotion rules, service-principal permissions, UC grants,
  secret references, and rollback instructions to the bundle.
- [x] Remove or complete the `pass` branches in Spark entrypoints, especially
  `src/sda/job_entrypoints/table_profile_spark.py:238`; every branch must have an
  explicit supported behavior or a fail-closed error.

## 4. Generation v1: standalone tables (SDA 10)

- [x] Define `GenerationPlan` with source evidence refs, target schema, row counts,
  seed policy, column models, null policy, identifier policy, time window, budgets,
  privacy constraints, generator version, and intended use.
- [x] Implement a deterministic row skeleton and stable row coordinates independent of
  Spark partitioning.
- [x] Implement empirical numeric sampling with quantiles, point masses, tails,
  clipping policy, and approved approximation limits.
- [x] Implement weighted categorical sampling, rare-category policy, string format
  signatures, safe vocabularies, dates/timestamps, and timezone semantics.
- [x] Generate new identifiers; never hash or copy production identifiers as synthetic
  identity. Persist mapping only when explicitly approved and protected.
- [x] Implement exact-count and probabilistic row modes with deterministic rounding.
- [ ] Apply only approved conditional null and cross-column rules.
- [ ] Write staging output, schema, manifest, seed/fingerprint, receipt, and audit row;
  publish only after validation and approvals.
- [ ] Add tests for exact counts, replay, partition independence, null rates, formats,
  tails, identifier uniqueness, safe strings, schema evolution, interrupted writes,
  retries, and output isolation.

## 5. Generation v2: relational datasets (SDA 11)

- [x] Freeze and validate the relationship dependency graph from the approved evidence
  snapshot; reject unresolved cycles or require an explicit cycle strategy.
- [x] Generate parent key domains first, then child rows from synthetic keys; support
  composite keys, nullable FKs, optional relationships, bridge tables, self-reference,
  and zero-child parents.
- [ ] Reproduce fan-out distributions and long tails while reconciling requested table
  totals; record unavoidable deviations and the chosen priority order.
- [x] Implement deterministic join allocation, orphan prevention, uniqueness checks,
  cycle handling, and post-generation relationship reconciliation.
- [ ] Add hand-computable fixtures for one-to-many, many-to-many, composite, optional,
  bridge, self-referential, cyclic, and empty relations.
- [ ] Add scale tests for skew, high fan-out, zero-child preservation, and driver-memory
  safety; prohibit collecting production-scale key domains to the driver.

## 6. Streaming generator (SDA 12)

- [ ] Define bounded, accelerated-logical-time, and continuous execution modes.
- [ ] Model event rate, inter-arrival distribution, bursts, quiet periods, seasonality,
  event/arrival/processing timestamps, watermarks, and lateness.
- [ ] Implement stateful entity lifecycles and valid event sequences using the approved
  pattern/relationship plan and synthetic parent domains.
- [ ] Generate deterministic event IDs and a streaming manifest containing plan,
  checkpoint, query, schema, rate, seed, and replay fingerprints.
- [ ] Implement checkpoint ownership, restart, replay, duplicate detection, state
  restoration, schema-version transitions, throughput caps, and environment limits.
- [ ] Add bounded-run tests for exact event counts, interruption/restart, replay,
  duplicate suppression, state recovery, late/out-of-order events, checkpoint
  incompatibility, and partition independence.
- [ ] Add controlled Databricks Structured Streaming integration tests; never claim
  continuous-mode readiness from local unit tests alone.

## 7. Topology generator (SDA 13)

- [ ] Define graph semantics and topology plan: directed/undirected, simple/multigraph,
  bipartite, temporal edges, node/edge populations, degree targets, components,
  communities, reciprocity, paths, motifs, weights, and hard/soft constraints.
- [ ] Generate synthetic nodes and topology-driving attributes before edges; preserve
  isolates explicitly and never reuse source IDs or memberships.
- [ ] Validate graphical/digraphical feasibility, degree sums, endpoint capacity,
  self-loop and repeated-pair rules, node-type compatibility, and DAG requirements.
- [ ] Implement deterministic pairing, edge swaps/repair, target reconciliation, and
  explicit adjustment records; fail when hard constraints cannot be satisfied.
- [ ] Use distributed Spark/GraphFrames for production-scale artifacts and reserve
  NetworkX for bounded local fixtures; avoid driver collection.
- [ ] Persist governed node/edge tables, topology manifest, structural checks, and
  bounded metric results with lineage and source/plan refs.
- [ ] Add fixtures for empty, isolates, chain, star, cycle, disconnected components,
  communities, reciprocal directed graph, bipartite graph, and temporal multigraph.
- [ ] Add negative tests for odd degree sums, incompatible in/out totals, forbidden
  loops/duplicates, missing endpoints, capacity overflow, impossible connectivity,
  pre-node events, and forbidden cycles.

## 8. Controlled noise (SDA 14)

- [x] Implement immutable clean-baseline references plus `NoisePlan`, profile, budget,
  seed, mutation ordering, protected invariants, expected detector, and truth-ledger
  contracts.
- [ ] Support historical, mild, QA, stress, and scenario-specific profiles; exact and
  probabilistic defect budgets; deterministic record selection and overlap policy.
- [ ] Implement casing/misspellings, malformed values, nulls, omissions, duplicates,
  near-duplicates, out-of-range values, invalid categories, broken FKs, invalid states,
  future/out-of-order timestamps, late/missing/duplicate stream events, drift, and
  raw-file/typed-table corruption.
- [ ] Separate intended defects from generator failures; protect invariants not covered
  by the scenario and record actual mutation counts.
- [ ] Add tests for budget exactness, deterministic selection, overlap/order, truth-ledger
  completeness, clean-baseline immutability, reversibility where supported, and
  expected validator detections.

## 9. Quality, privacy, approval, and publication (SDA 15)

- [x] Implement check contracts with `PASS`, `WARN`, `FAIL`, `NOT_APPLICABLE`, method,
  evidence refs, thresholds, freshness, population, unsupported reason, and severity.
- [ ] Validate schema, counts, keys/FKs, fan-out, distributions, conditional nulls,
  formats, time, patterns/rules, streaming behavior, topology, noise execution,
  unexpected defects, privacy/memorization risk, and evidence freshness.
- [ ] Use an intended-use validation vector; do not collapse all checks into one score.
- [ ] Separate technical disposition from privacy approval, human approval, and
  publication authorization; implement deny-by-default behavior.
- [ ] Implement privacy checks for direct identifiers, quasi-identifiers, rare records,
  memorization/nearest-neighbor risk, approved vocabularies, and sensitive outputs.
- [ ] Implement publication transaction: staged -> validated -> approved -> published,
  with UC permissions, views, manifests, version aliases, rollback, and revocation.
- [ ] Add adversarial tests proving fail-closed behavior, stale evidence rejection,
  unauthorized publication denial, privacy failures, and partial-write recovery.

## 10. Exploration and operations (SDA 16–20)

- [ ] Publish only approved datasets and curated validation views to Genie; define grain,
  joins, synonyms, governed SQL, dataset-version defaults, and intended-use context.
- [ ] Add benchmark questions, ambiguity/clarification tests, prohibited questions,
  permission-boundary tests, stale/revoked evidence behavior, and answer provenance.
- [ ] Implement the control center for runs, plans, artifacts, validation, approvals,
  lineage, costs, freshness, incidents, and publication state.
- [ ] Define AI/tool gateway policies, model allowlists, MCP/tool permissions, rate and
  spend limits, secret handling, request/response audit, redaction, and denial tests.
- [ ] Add security controls: least-privilege UC grants, service principals, network and
  secret boundaries, PII-safe logs, retention, audit export, and threat-model review.
- [ ] Add cost controls: per-run row/event/graph budgets, warehouse/cluster caps,
  timeout/cancellation, shuffle/storage estimates, and budget-exceeded behavior.
- [ ] Add observability: structured logs, metrics, traces/correlation IDs, stage latency,
  throughput, retries, evidence freshness, drift, validation failures, and alerts.
- [ ] Add runbooks for failed jobs, stale leases, corrupt checkpoints, failed migrations,
  revoked publications, data-quality incidents, and rollback/recovery drills.
- [ ] Add release smoke tests for one authorized successful run and one denied unsafe
  run across the deployed bundle.

## 11. Cross-cutting test and acceptance checklist

- [ ] Unit tests cover pure model, config, safety, identifier, fingerprint, and state
  transition logic.
- [ ] Contract tests cover every artifact schema, loader, version, lineage edge, and
  receipt.
- [ ] Property tests cover determinism, idempotency, monotonic budgets, no raw values,
  key uniqueness, orphan freedom, and fail-closed behavior.
- [ ] Spark tests cover empty inputs, nulls, skew, approximate metrics, partition
  independence, timestamp zones, and driver-memory limits.
- [ ] Integration tests cover UC metadata, Delta reads/writes, Structured Streaming,
  GraphFrames, Lakebase, and bundle execution with controlled fixtures.
- [ ] Failure-injection tests cover process termination, partial writes, retries,
  stale leases, checkpoint loss/incompatibility, schema changes, and permission denial.
- [ ] Security tests cover unsafe identifiers, injection-shaped names, secrets in logs,
  raw sensitive values in artifacts, unauthorized tables, and cross-tenant references.
- [ ] Performance tests establish budgets and baselines for profiling, relationship
  inference, pattern detection, generation, validation, and publication.
- [ ] Run `ruff check .`, `mypy src tests`, `pytest`, package build, bundle validation,
  and the governed integration suite in CI; record skipped tests with reasons.
- [x] Add an acceptance matrix mapping each SDA article deliverable and honest caveat
  to implementation, test evidence, documentation, and release status.

## 12. Recommended execution order

1. Close SDA 07 contracts, bundle wiring, documentation, and tests.
2. Implement durable state and the immutable generation-plan/state machine.
3. Build standalone generation v1 and validation/privacy gates.
4. Extend to relational generation v2 and publication.
5. Add streaming with checkpoint/replay proof.
6. Add topology generation and validation.
7. Add controlled noise and negative-testing workflows.
8. Add Genie/control center, gateway, security, cost, audit, and operations.
9. Run full integration, recovery, performance, privacy, and release acceptance.

Each step is complete only when its artifacts, migrations, bundle resources, tests,
documentation, observability, and failure behavior are delivered together.
