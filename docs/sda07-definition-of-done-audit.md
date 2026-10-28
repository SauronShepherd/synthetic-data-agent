# SDA-07 Definition of Done Audit

This audit is derived from the SDA-07 article's Definition of Done. It records
local contract evidence and an end-to-end Databricks verification using the
authenticated `sda` profile.

| Article requirement | Current implementation | Authoritative evidence | Status |
| --- | --- | --- | --- |
| Consume compatible metadata, profile, relationship, and source-snapshot evidence | `pattern_detect_spark.py` validates the complete `PatternInputRefs`; `load_pattern_inputs` resolves the latest complete row from the shared registry and rejects incompatible Delta versions | `tests/test_pattern_inputs.py`, `tests/test_pattern_entrypoint_contract.py`, `tests/test_pattern_upstream.py`; live metadata run `725013143492413`; live scope run `703381910952902`; reference detector run `425590486617445` | Complete |
| Select candidates by roles and measure numerical, categorical, mixed, fan-out, missingness, and segment patterns | `roles.py`, bounded driver-set selection, Spark correlation/conditional/missingness/numeric-group/fan-out adapters | `tests/test_pattern_contracts.py`, `tests/test_spark_metrics_contract.py`; live reference run `425590486617445`; missingness run `563518184861927`; fan-out run `696273407409020` | Complete |
| Detect temporal ordering, lag distributions, and state transitions | Temporal ordering direction inference, lag percentiles, event/ingestion clocks, transitions, censoring, unseen pairs, and bounded stability slices | `tests/test_pattern_entrypoint_contract.py`, `tests/test_spark_metrics_contract.py`; temporal-order run `860924432145115`; temporal-lag run `745552892911128`; transition run `110054133161319` | Complete |
| Evaluate user/domain rules, conflicts, precedence, and effective periods | `rules.py`, `spark_rules.py`, conflict resolver, effective-date filtering, deterministic precedence, independent owner/reviewer validation | `tests/test_spark_rules.py`, `tests/test_pattern_contracts.py`, `tests/test_pattern_persistence_evidence.py` | Complete |
| Preserve support, baselines, violations, stability, provenance, and review state | Versioned registry/evidence rows retain population, baseline, violations, stability, clocks, snapshots, upstream IDs, warnings, and lifecycle | `src/sda/patterns/persistence.py`, `tests/test_pattern_persistence_evidence.py`; SQL evidence query `01f1d2ea-b540-10f8-ad09-d551390634a7`; registry query `01f1d2ea-bacc-13b6-a753-353118af626b` | Complete |
| Keep observed evidence distinct from approved rules | Observed patterns remain `observed_pattern`; hard constraints and approved lifecycle require explicit approved origins and metadata | `tests/test_pattern_contracts.py`, `tests/test_spark_rules.py`; live lifecycle query `01f1d2ea-a97c-161a-9744-ed77deb9a10c` | Complete |
| Emit generation/validation instructions without activating unapproved rules | Actions are proposals, record bounded fallback levels plus explicit global fallback, and observed evidence cannot become hard constraints | `src/sda/patterns/actions.py`, `tests/test_pattern_contracts.py` | Complete |
| Persist an auditable registry and prove the deployed path with adversarial Spark/Databricks tests | Bundle is deployed; fixture data, Unity Catalog metadata, profiles, relationships, graph, all detector families, evidence, and lifecycle rows were executed and queried live | Deployment `databricks bundle deploy -t dev --profile sda`; fixture run `1025205193029638`; metadata run `725013143492413`; scope run `703381910952902`; detector runs listed below; SQL evidence queries listed below | Complete |

## Adversarial coverage

The repository includes deterministic coverage for tiny perfect segments,
multiple-testing noise, p-value evidence without a correction policy, nulls,
late/out-of-order events, right-censoring, rare/unseen transitions, stability
drift by time/source/partition, conflicts, effective dates, fan-out zero-child
parents, and absence of automatic rule promotion.

The detector deliberately uses the versioned descriptive policy
`descriptive_no_p_values_v1`. If p-value evidence is supplied without an
implemented multiple-testing correction policy, the detector fails closed.

## Current deployment evidence

The bundle was deployed and inspected with profile `sda`:

- Workspace: `dbc-916267ef-3b9b.cloud.databricks.com`, organization `7474654810500477`.
- Pattern job: `904431536757321` (`[dev] SDA Pattern Detector`).
- Runtime wheel: `/Workspace/Users/angel.alvarez.pascua@gmail.com/.bundle/synthetic-data-agent/dev/artifacts/.internal/synthetic_data_agent-0.8.0.dev0-py3-none-any.whl`.
- Entrypoint: `/Workspace/Users/angel.alvarez.pascua@gmail.com/.bundle/synthetic-data-agent/dev/files/src/sda/job_entrypoints/pattern_detect_spark.py`.
- Deployment result: `3 files uploaded`, `18 resources unchanged`.
- Bundle validation: `databricks bundle validate -t dev --profile sda` succeeded.

The job parameters include metadata, profile, relationship, dependency-graph,
source-snapshot, stability, fan-out, temporal-clock, output, and evidence-table
inputs required by the article.

## Databricks verification pass

The live fixture job `701020438490636` completed successfully as run
`1025205193029638` and created the six adversarial tables used by this pass.
The Unity Catalog metadata reader completed as run `725013143492413`, producing
metadata artifact
`metadata_inventory_e4a55d7d9b642d491de1871269fd2ffc9fd8f4195cdbc50cc65ba66369ba0034`.
The registered customer/order scope completed as run `703381910952902` and
produced complete profile, relationship, and graph artifacts.

The detector runs completed as follows:

| Detector path | Job run | Task run | Result |
| --- | ---: | ---: | --- |
| Registered reference scope | `425590486617445` | `190511662923027` | 9 patterns: 2 conditional, 1 transition, 3 lag, 3 order |
| Missingness and conditional stability | `563518184861927` | `622110341910827` | 6 patterns: 4 conditional, 2 missingness |
| Fan-out by segment and stability | `696273407409020` | `82973146344814` | 1 fan-out pattern |
| Temporal order | `860924432145115` | `146802087369387` | 2 patterns: 1 order, 1 lag |
| Temporal lag | `745552892911128` | `649391472111480` | 2 patterns: 1 lag, 1 order |
| State transition | `110054133161319` | `481013898467041` | 54 patterns: 52 conditional, 2 transitions |

The SQL API confirmed that all six live analyses have distinct persisted
evidence IDs: fan-out 6 rows, missingness 32, reference 93, temporal lag 18,
temporal order 18, and transition 342. The artifact registry contains a
complete lifecycle row for every detector analysis. Historical `writing` rows
are retained as lifecycle history; consumers select the latest `complete` row.

## Corrections made during this iteration

The live fan-out path exposed a real edge case in `persist_rows`: when a valid
detector produced an empty detail batch, or when the target had `evidence_id`
but the source frame did not, the Delta merge condition referenced an unresolved
`source.evidence_id`. The implementation now treats an empty detail batch as a
valid no-op and adds the evidence predicate only when both sides contain the
column. Regression coverage was added in
`tests/test_delta_persistence_contract.py`; the targeted persistence suite,
Ruff, formatting, and type checks pass after the correction.

The shared artifact loader also resolves the latest complete central registry
row, which is required when retry history leaves both `writing` and `complete`
rows. This behavior is covered by
`test_pattern_loader_uses_latest_complete_rows_from_shared_registry`.

## Local verification

The following checks pass locally:

- `python -m pytest -m "not spark and not databricks and not integration" -q`
- `ruff check src tests`
- `python -m ruff format --check src tests`
- `mypy src tests`
- Targeted artifact and pattern-input regression tests.

Direct local Spark execution on this Windows workstation remains an environment
limitation: the installed Python/PySpark combinations could not start the
Python worker reliably. The complete Spark execution evidence for SDA-07 is
therefore the successful Databricks `sda` runs above, while the local suite
continues to validate contracts and deterministic logic.

## Cross-article and branch iteration

SDA-01 through SDA-06 and their branches were compared with the SDA-07
contracts. The comparison exposed the shared-registry handoff requirement from
the local SDA-08 history (`143dec4`, “Fix live SDA-07 artifact registry
handoff”). SDA-07 now carries that behavior in `ArtifactRegistryStore`,
`SparkArtifactRegistry`, and `load_pattern_inputs`, with regression coverage.

The branch is `SDA-07`; the implementation, tests, bundle deployment, live
Databricks executions, and registry queries recorded here are the closure
evidence for the article's Definition of Done.

## Closure

All SDA-07 Definition-of-Done rows have local contract evidence and live
Databricks evidence. No unresolved implementation gate remains for SDA-07.
