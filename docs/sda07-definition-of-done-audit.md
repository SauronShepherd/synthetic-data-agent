# SDA-07 Definition of Done Audit

This audit is derived from the SDA-07 article's Definition of Done. It is an
evidence index, not a claim that a local test proves a governed Databricks run.

| Article requirement | Current implementation | Authoritative evidence | Status |
| --- | --- | --- | --- |
| Consume compatible metadata, profile, relationship, and source-snapshot evidence | `pattern_detect_spark.py` validates the complete `PatternInputRefs`, loads the upstream registry entries, and rejects incompatible Delta versions | `tests/test_pattern_inputs.py`, `tests/test_pattern_entrypoint_contract.py`, `tests/test_pattern_upstream.py` | Local/CI complete; current live rerun pending |
| Select candidates by roles and measure numerical, categorical, mixed, fan-out, missingness, and segment patterns | `roles.py`, bounded driver-set selection, Spark correlation/conditional/missingness/numeric-group/fan-out adapters | `tests/test_pattern_contracts.py`, `tests/test_spark_metrics_contract.py`, CI Spark job | CI complete |
| Detect temporal ordering, lag distributions, and state transitions | Temporal ordering direction inference, lag percentiles, event/ingestion clocks, transitions, censoring, unseen pairs, and bounded stability slices | `tests/test_pattern_entrypoint_contract.py`, `tests/test_spark_metrics_contract.py`, historical live run `440523671659941` | CI complete; new live paths pending |
| Evaluate user/domain rules, conflicts, precedence, and effective periods | `rules.py`, `spark_rules.py`, conflict resolver, effective-date filtering, deterministic precedence, independent owner/reviewer validation | `tests/test_spark_rules.py`, `tests/test_pattern_contracts.py`, `tests/test_pattern_persistence_evidence.py` | Local/CI complete |
| Preserve support, baselines, violations, stability, provenance, and review state | Versioned registry/evidence rows retain population, baseline, violations, stability, clocks, snapshots, upstream IDs, warnings, and lifecycle | `src/sda/patterns/persistence.py`, `tests/test_pattern_persistence_evidence.py` | Local/CI complete |
| Keep observed evidence distinct from approved rules | Observed patterns remain `observed_pattern`; hard constraints and approved lifecycle require explicit approved origins and metadata | `tests/test_pattern_contracts.py`, `tests/test_spark_rules.py` | Local/CI complete |
| Emit generation/validation instructions without activating unapproved rules | Actions are proposals, record bounded fallback levels plus explicit global fallback, and observed evidence cannot become hard constraints | `src/sda/patterns/actions.py`, `tests/test_pattern_contracts.py` | Local/CI complete |
| Persist an auditable registry and prove the deployed path with adversarial Spark/Databricks tests | Bundle is deployed and CI Spark/adversarial tests pass; historical `sda` evidence proves registry persistence and review state | CI `39753071013`, bundle deployment with profile `sda`, live run `440523671659941` | Partially complete: current Free workspace disables new job runs |

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

Read-only Databricks inspection succeeds with profile `sda`:

- Pattern job: `904431536757321` (`[dev] SDA Pattern Detector`).
- Job deployment kind: `BUNDLE`.
- Runtime wheel: `/Workspace/Users/angel.alvarez.pascua@gmail.com/.bundle/synthetic-data-agent/dev/artifacts/.internal/synthetic_data_agent-0.8.0.dev0-py3-none-any.whl`.
- Entrypoint: `/Workspace/Users/angel.alvarez.pascua@gmail.com/.bundle/synthetic-data-agent/dev/files/src/sda/job_entrypoints/pattern_detect_spark.py`.
- The job parameters include metadata, profile, relationship, dependency-graph,
  source-snapshot, stability, fan-out, temporal-clock, output, and
  evidence-table inputs required by the DOD.

This proves the current bundle is wired and deployed; it does not replace the
missing fresh run and registry query.

The authenticated web console for workspace `7474654810500477` independently
shows the account banner `You've reached your budget. Contact your admin for a
budget increase. Budget resets Nov 1.` The Jobs page lists both the fixture job
`701020438490636` and pattern job `904431536757321`, with historical successful
runs. Selecting `Run now` for the fixture on 2026-10-22 produced the same
organization-disabled error recorded below.

## Remaining gate

The only SDA-07 DOD item that cannot currently be re-proven live is a fresh
Databricks Free Edition end-to-end execution after the latest deployment. The
authenticated `sda` profile targets workspace `7474654810500477`, but the API
currently returns:

```text
Triggering new runs for organization 7474654810500477 is currently disabled temporarily.
```

The OAuth session was revalidated on 2026-10-22 against
`dbc-916267ef-3b9b.cloud.databricks.com`; the same response was returned when
triggering fixture job `701020438490636`. This is an external workspace/account
state, not a repository test failure.
The branch must remain open until that run can execute and its metadata →
profile → relationship → pattern handoff and registry persistence are queried.
