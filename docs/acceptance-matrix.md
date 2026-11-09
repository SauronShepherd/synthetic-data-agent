# SDA 00–08 acceptance matrix

This matrix is the release boundary for the current `0.8.0.dev0` milestone.
“Verified” means covered by local contracts/tests; it does not imply governed
Databricks production readiness.

| Article | Deliverable | Repository evidence | Status / caveat |
|---|---|---|---|
| SDA 01 | Agent scope, safety boundary, intended use | `src/sda/models.py`, `src/sda/security.py`, `docs/architecture.md` | Verified locally; production UC grants remain deployment work |
| SDA 02 | Tool-oriented orchestration and plan handoff | `src/sda/tools/`, `src/sda/orchestrator.py`, `src/sda/planning.py` | Verified locally; legacy design fixtures are explicitly isolated |
| SDA 03 | Declarative bundle bootstrap | `databricks.yml`, `bundle/`, `scripts/check_bundle_contract.py` | Static contract and live `sda` deployment verified; production permissions remain target-specific |
| SDA 04 | Unity Catalog metadata reader | `src/sda/tools/uc_metadata_reader.py`, metadata tests | Local contracts and live one-run governed metadata inventory verified |
| SDA 05 | Table profiling and privacy-safe evidence | `src/sda/tools/table_profiler.py`, `src/sda/profiling/` | Local bounded paths and live Databricks profile verified; production-scale performance remains a capacity exercise |
| SDA 06 | PK/FK relationships, fan-out, dependency graph | `src/sda/relationships/`, `src/sda/relational.py`, `src/sda/topology.py` | Local contracts and live relationship/dependency output verified |
| SDA 07 | Cross-column patterns and business rules | `src/sda/patterns/`, `src/sda/job_entrypoints/pattern_detect_spark.py` | Registry/evidence schemas, compatibility, budgets, lineage, detectors, and live quick-mode run verified |
| SDA 08 | Durable requests, plans, runs, steps, leases, audit history, and artifact decisions | `src/sda/state.py`, `src/sda/state_sqlite.py`, `src/sda/state_postgres.py`, `sql/lakebase_state_schema.sql`, `migrations/versions/`, `bundle/lakebase.yml`, `src/sda/workflows/analyze_scope.py` | Local repository contracts plus live Lakebase migration, OAuth CRUD, bundle resources, and restart-safe state verified; backup/restore and post-suspension observation remain operational follow-ups |

## Release gates

- Local release: full pytest suite, mypy, Ruff, static bundle contract, and
  package hygiene must pass.
- Spark release: marked Spark tests must pass on the supported Python/Spark
  combination.
- Governed release: Databricks bundle validation, UC/Delta integration tests,
  service-principal permissions, and production runbook checks are required.

The current branch satisfies the local release gates and the dev workspace
integration gates recorded in `docs/sda08-build-and-test-plan.md`. It is not a
production readiness sign-off: service-principal permissions, backup/restore,
and the post-suspension observation still require the target platform owner.
