# SDA-00–08 Capability Matrix

This matrix is the traceability index for the current SDA milestone. “Local”
means covered by repository contracts and tests. It does not claim governed
Databricks production readiness.

| Article | Capability | Implementation | Tests | Gate |
|---|---|---|---|---|
| SDA-00 | Series charter and capability boundary | `README.md`, `docs/architecture.md`, `src/sda/security.py` | `tests/test_security.py`, `tests/test_safety.py` | Local |
| SDA-01 | Agent scope, safety, and intended use | `src/sda/models.py`, `src/sda/security.py` | `tests/test_models.py`, `tests/test_security.py` | Local; governed UC pending |
| SDA-02 | Tool orchestration, plans, approvals, artifacts | `src/sda/orchestrator.py`, `src/sda/planning.py`, `src/sda/artifacts/` | `tests/test_orchestrator.py`, `tests/test_planning.py`, `tests/test_approval.py` | Local |
| SDA-03 | Declarative bundle bootstrap | `databricks.yml`, `bundle/`, `scripts/check_bundle_contract.py` | `tests/test_bundle_contract.py`, `tests/test_bundle_policy.py` | Static contract and live `sda` dev deployment verified; target permissions remain deployment work |
| SDA-04 | Unity Catalog metadata inventory | `src/sda/tools/uc_metadata_reader.py`, `src/sda/metadata_models.py` | `tests/test_uc_metadata_reader.py`, `tests/test_sda04_regressions.py` | Local contracts and live one-run governed metadata inventory verified |
| SDA-05 | Privacy-safe source profiling | `src/sda/tools/table_profiler.py`, `src/sda/profiling/` | `tests/test_profile_*.py`, `tests/test_conditional_nulls.py`, `tests/test_complex_type_contract.py` | Local bounded paths and live Databricks profile verified; scale/performance remains a capacity exercise |
| SDA-06 | Keys, relationships, fan-out, and graph | `src/sda/relationships/`, `src/sda/relational.py` | `tests/test_relationship_*.py`, `tests/test_relational_validation.py` | Local contracts and live relationship/dependency output verified |
| SDA-07 | Cross-column patterns and business rules | `src/sda/patterns/`, `src/sda/job_entrypoints/pattern_detect_spark.py` | `tests/test_pattern_*.py`, `tests/test_spark_metrics_contract.py` | Local contracts and live quick-mode Spark run verified |
| SDA-08 | Durable requests, plans, runs, steps, leases, retries, audit, and artifact decisions | `src/sda/state*.py`, `src/sda/workflows/analyze_scope.py`, `sql/lakebase_state_schema.sql` | `tests/test_state*.py`, `tests/test_analyze_scope_workflow.py`, `tests/test_end_to_end_lineage.py` | Local contracts and live Lakebase migration/OAuth CRUD verified; backup/restore and post-suspension observation remain operational follow-ups |

## Release evidence

The local gate is reproducible with:

```text
pytest -m "not spark and not databricks and not integration"
ruff check src tests scripts
mypy src tests scripts
python scripts/check_state_schema.py
python scripts/check_bundle_contract.py
python scripts/check_release.py
```

The governed gate additionally requires authenticated Unity Catalog, Delta,
Lakebase/PostgreSQL, service-principal, migration, recovery, and Databricks
bundle evidence. The `sda` dev workspace now supplies the recorded bundle,
Lakebase, PostgreSQL, and SDA-04 through SDA-07 Spark evidence; service-principal
permissions, backup/restore, and post-suspension observation remain target-owner
operational checks.
