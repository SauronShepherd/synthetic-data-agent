# SDA-00–08 Capability Matrix

This matrix is the traceability index for the current SDA milestone. “Local”
means covered by repository contracts and tests. It does not claim governed
Databricks production readiness.

| Article | Capability | Implementation | Tests | Gate |
|---|---|---|---|---|
| SDA-00 | Series charter and capability boundary | `README.md`, `docs/architecture.md`, `src/sda/security.py` | `tests/test_security.py`, `tests/test_safety.py` | Local |
| SDA-01 | Agent scope, safety, and intended use | `src/sda/models.py`, `src/sda/security.py` | `tests/test_models.py`, `tests/test_security.py` | Local; governed UC pending |
| SDA-02 | Tool orchestration, plans, approvals, artifacts | `src/sda/orchestrator.py`, `src/sda/planning.py`, `src/sda/artifacts/` | `tests/test_orchestrator.py`, `tests/test_planning.py`, `tests/test_approval.py` | Local |
| SDA-03 | Declarative bundle bootstrap | `databricks.yml`, `bundle/`, `scripts/check_bundle_contract.py` | `tests/test_bundle_contract.py`, `tests/test_bundle_policy.py` | Static; live deployment pending |
| SDA-04 | Unity Catalog metadata inventory | `src/sda/tools/uc_metadata_reader.py`, `src/sda/metadata_models.py` | `tests/test_uc_metadata_reader.py`, `tests/test_sda04_regressions.py` | Local; live UC pending |
| SDA-05 | Privacy-safe source profiling | `src/sda/tools/table_profiler.py`, `src/sda/profiling/` | `tests/test_profile_*.py`, `tests/test_conditional_nulls.py`, `tests/test_complex_type_contract.py` | Local; Spark/scale pending |
| SDA-06 | Keys, relationships, fan-out, and graph | `src/sda/relationships/`, `src/sda/relational.py` | `tests/test_relationship_*.py`, `tests/test_relational_validation.py` | Local; distributed/live pending |
| SDA-07 | Cross-column patterns and business rules | `src/sda/patterns/`, `src/sda/job_entrypoints/pattern_detect_spark.py` | `tests/test_pattern_*.py`, `tests/test_platform_constraints.py`, `tests/test_spark_metrics_contract.py` | Local non-Spark suite, Ruff, format, mypy, bundle contracts, and release checks pass. The `sda` profile live path covers metadata/profile/relationship handoff plus conditional, missingness, fan-out, temporal-order, temporal-lag, state-transition, and effective-date business-rule detectors; the current DOD evidence matrix and run IDs are in `docs/sda07-definition-of-done-audit.md`. The bundle is deployed to the Free workspace. CI is intentionally out of scope. Trusted production approval, service-principal promotion, Lakebase/Genie integration, and production-scale gates remain outside SDA-07 |
| SDA-08 | Durable requests, plans, runs, steps, leases, retries, audit, and artifact decisions | `src/sda/state*.py`, `src/sda/workflows/analyze_scope.py`, `sql/lakebase_state_schema.sql` | `tests/test_state*.py`, `tests/test_analyze_scope_workflow.py`, `tests/test_end_to_end_lineage.py` | Local; live Lakebase/OAuth/recovery pending |

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

The `sda` profile has exercised a live Unity Catalog/Delta fixture and bundle
deployment for SDA-07. That smoke run is not the governed release gate, which
still requires service-principal, approval lifecycle, migration, recovery,
Lakebase/PostgreSQL, scale, and adversarial evidence where applicable.
