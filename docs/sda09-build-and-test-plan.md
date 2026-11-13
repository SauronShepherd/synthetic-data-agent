# SDA-09 build and test plan

This plan turns the Lakebase selection into a reviewable, executable
architecture decision. SDA-09 does not add synthetic row generation; it makes
the storage boundary, workload probe, and revisit conditions explicit before
the generator arrives in SDA-10.

## Completion contract

SDA-09 is complete for the dev acceptance boundary when:

1. the decision and storage boundary are represented in code and an ADR;
2. Lakebase owns only operational coordination state and Delta remains the
   analytical evidence plane;
3. the PostgreSQL adapter can build its non-secret settings from Databricks
   app/job variables and refresh OAuth credentials on reconnect;
4. the workload probe measures fresh connections and a short request
   transaction without leaving test rows;
5. local contract, static, package, and bundle checks pass;
6. the `sda` workspace verifies bundle state, schema idempotency, PostgreSQL
   CRUD, reconnect behavior, and the probe; and
7. the article reports exactly those implemented and verified boundaries,
   distinguishing optional product capabilities from evidence run here.

## Implementation map

| Concern | Implementation | Verification |
| --- | --- | --- |
| Decision criteria and candidates | `src/sda/lakebase_decision.py` | `tests/test_lakebase_decision.py` |
| State connection settings | `LakebaseConnectionConfig.from_environment` | `tests/test_state_postgres_environment.py` |
| OAuth reconnect contract | `src/sda/state_postgres.py` | `tests/test_state_postgres.py` |
| Workload probe | `scripts/benchmark_state_store.py`, `src/sda/benchmark.py` | `tests/test_benchmark.py`, live probe |
| Full disposable lifecycle | `scripts/run_sda09_live_lifecycle.py` | live request/run/plan/step/attempt/artifact/audit/approval flow |
| Relational state boundary | `schema/lakebase/001_state_schema.sql` | `scripts/check_state_schema.py` |
| Infrastructure boundary | `bundle/lakebase.yml` | bundle validation and contract checks |
| Architecture record | `docs/adr/0004-lakebase-for-operational-state.md` | review against code and live evidence |

## Local test matrix

| ID | Test | Exit criterion |
| --- | --- | --- |
| SDA09-01 | Decision contract | Lakebase, Delta, all three candidates, criteria, non-goals, and revisit triggers are present and internally consistent |
| SDA09-02 | Storage boundary | Operational records and analytical records have one owner; raw rows and large evidence are forbidden in operational payloads |
| SDA09-03 | Environment connection config | `PGHOST`, `PGDATABASE`, `PGUSER`, `PGSSLMODE`, and timeout parse without secrets in the config object |
| SDA09-04 | OAuth retry | The token provider is called again for each connection attempt and failure text does not expose a token |
| SDA09-05 | Benchmark summaries | Empty/negative samples fail; p50, p95, max, and sample count are deterministic |
| SDA09-06 | Repository semantics | SDA-08 state tests continue to prove idempotency, constraints, leases, retries, and history |
| SDA09-07 | Static checks | Ruff, mypy, schema contract, bundle contract, release hygiene, and wheel build pass |

## Live `sda` acceptance

Use the existing dev project and branch declared by the bundle. Do not write a
credential to a file or print it. The commands below are PowerShell examples.

```powershell
databricks bundle validate -t dev --profile sda
databricks bundle deploy -t dev --profile sda
databricks postgres list-projects --profile sda -o json
databricks postgres list-branches projects/<project-id> --profile sda -o json
databricks postgres list-endpoints projects/<project-id>/branches/dev --profile sda -o json
```

Generate a short-lived database credential and set `SDA_POSTGRES_DSN` in the
current process only. Apply the versioned schema twice; the second invocation
must be a data-level no-op. Then run the live repository CRUD and cleanup
checks already defined for SDA-08.

```powershell
python scripts/apply_state_schema.py $env:SDA_POSTGRES_DSN --schema-dir schema/lakebase
python scripts/apply_state_schema.py $env:SDA_POSTGRES_DSN --schema-dir schema/lakebase
python scripts/check_sda08_live_state.py
python scripts/run_sda09_live_lifecycle.py
python scripts/benchmark_state_store.py --iterations 20 --warmup 3
```

The probe measures two operations:

- `connect_and_ping`: a fresh PostgreSQL connection followed by `SELECT 1`,
  which exposes reconnect/cold-start behavior;
- `request_insert_select_delete`: one short transaction against the state
  table, which exercises the point-write path without retaining a row.

The probe is not a controlled benchmark of Delta and an external PostgreSQL
provider. Those candidates are evaluated by the workload and ownership
criteria in ADR 0004. The article must not present an unrun competitor
measurement as fact.

## Recorded dev evidence

The acceptance run used the `sda` profile on 13 November 2026. The deployed
endpoint was observed in `IDLE` before the fresh-connection cycle and `ACTIVE`
after it. The schema was applied twice, and the live state check reported:

```text
sda_tables=11
schema_versions=1
disposable_rows=0
```

The disposable lifecycle reported `live_repository_lifecycle=OK`. With three
warm-up samples and 20 measured samples, the probe reported:

| Operation | p50 | p95 | Maximum |
| --- | ---: | ---: | ---: |
| Fresh connection plus `SELECT 1` | 1,079.9 ms | 1,135.6 ms | 1,139.6 ms |
| Request insert/select/delete transaction | 1,321.1 ms | 1,381.2 ms | 1,519.2 ms |

These are one-run observations, not service-level objectives. They establish
dev viability and reconnect behavior; they do not establish production
capacity, cost, high availability, or a competitor benchmark.

## Cross-series checks

Run the existing SDA-04 through SDA-08 Databricks acceptance jobs after the
bundle deployment. SDA-09 must not change their analytical output boundary:
profiles, relationship evidence, patterns, scope manifests, validation detail,
and future generated tables remain in governed Delta locations. Lakebase keeps
only their compact operational references.

## Definition of done

The implementation is ready for publication when the local checks are green,
the `sda` dev checks are recorded with timestamps and disposable-row cleanup,
the ADR and article agree with the live evidence, and the article explicitly
marks Unity Catalog registration, external PostgreSQL benchmarking, production
service-principal permissions, HA, and cross-region recovery as unverified or
target-specific where they were not exercised.
