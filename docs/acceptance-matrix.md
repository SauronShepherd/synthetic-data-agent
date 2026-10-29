# SDA 00–08 acceptance matrix

This matrix is the release boundary for the current `0.8.0.dev0` milestone.
“Verified” means covered by local contracts/tests; it does not imply governed
Databricks production readiness.

| Article | Deliverable | Repository evidence | Status / caveat |
|---|---|---|---|
| SDA 01 | Agent scope, safety boundary, intended use | `src/sda/models.py`, `src/sda/security.py`, `docs/architecture.md` | Verified locally; production UC grants remain deployment work |
| SDA 02 | Tool-oriented orchestration and plan handoff | `src/sda/tools/`, `src/sda/orchestrator.py`, `src/sda/planning.py` | Verified locally; legacy design fixtures are explicitly isolated |
| SDA 03 | Declarative bundle bootstrap | `databricks.yml`, `bundle/`, `scripts/check_bundle_contract.py` | Static wiring verified; live workspace validation requires integration credentials |
| SDA 04 | Unity Catalog metadata reader | `src/sda/tools/uc_metadata_reader.py`, metadata tests | Local SQL/Spark contracts verified; governed workspace test pending |
| SDA 05 | Table profiling and privacy-safe evidence | `src/sda/tools/table_profiler.py`, `src/sda/profiling/` | Bounded local/Spark paths verified; production-scale performance pending |
| SDA 06 | PK/FK relationships, fan-out, dependency graph | `src/sda/relationships/`, `src/sda/relational.py`, `src/sda/topology.py` | Local contracts verified; distributed graph generation pending |
| SDA 07 | Cross-column patterns and business rules | `src/sda/patterns/`, `src/sda/job_entrypoints/pattern_detect_spark.py`, `docs/sda07-definition-of-done-audit.md` | Local contracts and live `sda` Databricks execution verified for all configured detector families, including effective-date rule provenance; CI is intentionally out of scope. Production approval lifecycle, Lakebase/Genie, service-principal promotion, and production-scale evidence remain outside this article |
| SDA 08 | Durable requests, plans, runs, steps, leases, audit history, and artifact decisions | `src/sda/state.py`, `src/sda/state_sqlite.py`, `src/sda/state_postgres.py`, `sql/lakebase_state_schema.sql`, `src/sda/workflows/analyze_scope.py` | Local in-memory/SQLite contracts, restart, retry, concurrency, and workflow handoff verified; live Lakebase migration, OAuth, backup/restore, and scale-to-zero proof pending |

## Release gates

- Local release: full pytest suite, mypy, Ruff, static bundle contract, and
  package hygiene must pass.
- Spark release: marked Spark tests must pass on the supported Python/Spark
  combination.
- Governed release: Databricks bundle validation, UC/Delta integration tests,
  service-principal permissions, and production runbook checks are required.

The current branch satisfies the local release gates only. It must not be
represented as production-ready until the governed release gates are complete.
