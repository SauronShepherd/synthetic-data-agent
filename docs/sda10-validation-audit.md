# SDA 10 Validation Audit

## Scope and conclusion

This audit compares the claims in the SDA 10 article with the implementation in this repository and with executions in Databricks using profile `sda`, target `dev`. The work is on branch `codex/SDA-10`.

The article is not ready for publication as an exact description of the tested system. The bundle validates and deploys, and the local regression suite passes. After fixing the entrypoint to include the requested row count in the reconstructed plan fingerprint, a live generation run and a live validation run succeeded in Databricks. The implementation still covers only a smaller local generation slice than the article describes.

## Evidence

| Claim or capability | Implementation | Test executed | Result | Article section | Status | Discrepancy or limit |
|---|---|---|---|---|---|---|
| Approved, fingerprinted generation plan | `src/sda/planning.py`, `src/sda/job_entrypoints/generate_spark.py` | Local generation tests; live job with `approved=true` | Pass after entrypoint fix; live generation succeeded | 2.1, 14.1 | Partial | The live contract now reconstructs the tested plan; broader immutable plan loading and approval persistence are still absent. |
| Bundle deployment and standalone generator job | `databricks.yml`, `bundle/resources.yml` | `databricks bundle validate -t dev --profile sda`; deploy with profile `sda` | Validation passed with a workspace-permissions warning; deployment completed | 10.1, 11 | Pass with warning | No article-level end-to-end success was obtained. |
| Exact row-count bound and approved-only execution | `generate_rows`, generator entrypoint | `tests/test_generation.py`; live job with row count 25 and max 100 | Local pass; live run blocked earlier by fingerprint mismatch | 2.1, 9.1, 13.1 | Partial | Bound checks exist, but the live path has not produced a table. |
| Stable synthetic identifiers and deterministic coordinate-based values | `_stable_id`, `_coordinate_seed` in `src/sda/generation.py` | Local reproducibility and uniqueness tests | Pass locally | 3.1, 3.2, 6.2, 14.1 | Partial | Uses Python row loops, not the Spark distributed skeleton described by the article. |
| Spark distributed skeleton and no driver-side generated rows | No implementation; entrypoint calls `generate_rows` and then `createDataFrame` from a Python list | Code inspection; live job attempt | Not implemented; live execution did not reach write | 3.1, 9.2, 9.4 | Fail | Direct contradiction with article claim. |
| Probabilistic and exact allocation for categories/nulls/tails | Only one per-row `null_rate` check and basic random categorical support in local API | Local unit tests | Partial local support | 3.3, 5.2, 8.2 | Partial | No exact allocation mode or largest-remainder implementation is wired into the Databricks entrypoint. |
| Profile-based empirical numerical distributions and tails | `empirical_samples` accepted by local function; no profile resolver or histogram/quantile sampler | Local unit tests only | Not demonstrated end to end | 4.2, 4.4 | Partial | The article presents a complete profile-based sampler; the implementation exposes only a simple sample hook. |
| Weighted categorical frequencies and approved vocabularies | Optional local `weighted_vocabularies` / `vocabularies` arguments | Local tests | Partial local support | 5.1–5.4 | Partial | Databricks `columns-json` cannot provide these collections; no live weighted-category test passed. |
| Format-signature strings and synthetic identifiers | `_render_signature`, `_stable_id` | Local unit tests | Pass locally | 6.1, 6.2 | Partial | No live evidence; output type casting is not implemented as described. |
| Unsupported text fails explicitly | Unsupported model raises `GenerationError` | Local unit tests | Pass locally | 6.3 | Partial | The article's explicit plan-level exclusion/placeholder/specialized-generator policy is not implemented in the entrypoint. |
| Temporal distributions, timezone policy, LTZ/NTZ semantics | Timestamp model emits deterministic January strings | Local tests | Does not implement article behavior | 7.1–7.3 | Fail | No calendar-bucket model, timezone recording, DST handling, or Databricks timestamp typing. |
| Conditional null models and precedence rules | Generic per-column `null_rate`; cross-column overwrite rules exist locally | Local tests | Partial local support | 8.2–8.4 | Partial | No conditional-null model resolver or manifest fallback/provenance in the live path. |
| Run-scoped Delta output and generation manifest | Entry point creates/replaces a Delta table and prints a manifest | Live generation run | Pass for Delta table and printed manifest | 2.2, 11, 13.1 | Partial | Manifest is printed only; it is not persisted in Delta/Lakebase as claimed. |
| Lakebase run state and artifact registration | No call from `generate_spark.py` to Lakebase/state registration | Code inspection | Not implemented in this job | 2.2, 11, 14.2 | Fail | Article claims a cross-component completion path that is absent from the generator entrypoint. |
| Idempotent retry and repair after Delta/Lakebase split | No idempotency/reconciliation logic in generator entrypoint | Live retry was automatically attempted by Databricks | Failed twice at the same fingerprint check | 11, 14.2 | Fail | Retry repeats failure; it does not discover or repair an existing artifact. |
| Generation checks separate from quality validation | Separate `validate_generated` job exists | Live validation run against generated table | Pass | 13 | Partial | The validation job ran successfully, but it is structural validation, not Article 14 quality assessment. |
| Awaiting validation rather than approval/publication | No run-state transition in generator entrypoint | Code inspection | Not implemented in live path | 2.3, 14.3 | Fail | The entrypoint reports `complete` but does not register an awaiting-validation state. |

## Independent reverse check

The implementation contains a bounded local generator supporting identifiers, simple numeric ranges, booleans, fixed date/timestamp strings, format signatures, simple categorical values, optional weighted/empirical inputs, null-rate masking, cross-column overwrites, receipts, and manifests. The article mentions most of these concepts, but it presents them as a broader Spark/Databricks implementation with profile resolution, distributed execution, conditional models, operational state, and idempotent recovery. Those additional behaviors are not reflected accurately by the tested code.

## Required closure work

1. Make the approved plan fingerprint contract identical between plan creation and `generate_spark.py`, then rerun a successful Databricks generation.
2. Implement or remove from the article the claimed distributed Spark generation, profile-based numerical sampling, weighted/exact allocation, temporal models, conditional nulls, schema casting, manifest persistence, Lakebase registration, and idempotent reconciliation.
3. Add live tests for success, rejected plans, invalid strategies, bounds, permissions, output schema, source immutability, validation handoff, retry, cleanup, and rerun.
4. Re-run this matrix after each implementation or article revision and update the article so it states only behavior demonstrated in Databricks with profile `sda`.

## Current verdict

**IMPLEMENTED V1, ARTICLE REVISED, FULL ARTICLE SCOPE NOT READY.** The bounded v1 path is now tested live, but the broader claims listed as future work are intentionally not presented as implemented capabilities.

## Reproducible live evidence

- Bundle validation and deployment completed with `databricks bundle deploy -t dev --profile sda`.
- Standalone generation completed successfully for a 25-row `customers` table in Delta.
- `validate_generated` completed successfully against that output with the expected count and required columns.
- A rerun with the same plan and output table completed successfully, exercising the current explicit replacement policy.
- A live run with `approved=false` failed before writing with `generation requires approved=true`.
- The original fingerprint mismatch was reproduced, fixed by including `requested_row_count` in plan reconstruction, and covered by a local regression test.
- A `bundle run` attempt with incorrectly propagated parameters failed with empty plan identifiers; this is recorded as a harness invocation limitation. Direct `jobs run-now` with explicit job parameters is the validated invocation.

The revised DOCX is written beside the source article as `🤖 SDA 10 Generating Synthetic Tables with Realistic Distributions and Null Behavior - Revised v2.docx`. The final DOCX was opened in Microsoft Word for visual QA: Word reports 18 pages, the first page shows the revised title and bounded-v1 scope, and the final page shows the explicit future-work boundary without visible clipping or overlap. The bundled headless renderer remains unavailable because the runtime does not expose LibreOffice `soffice.exe`.
