# Synthetic Data Agent — Build Plan to 100% Completion

**Scope:** everything required by Articles SDA 01–07 that is missing, partial, or wrongly
implemented in the current repository (branch `SDA-07`, current HEAD verified at report
generation time).
**Method:** every finding below was verified directly against the current source tree
(file:line references throughout) and against the actual article text (extracted from the
`.docx` sources with `python-docx`), not against an older audit. Two prior documents
(`CODEX_BUILD_PLAN_SDA07_AND_GAPS_00_06.md`, `MERGED_SDA07_IMPLEMENTATION_AUDIT.md`, both in
`C:\ANGEL\Personal\Articles\SDA\`) were used as a starting hypothesis and then re-verified —
roughly a third of their P0 items are now fixed; this document supersedes them.
**Current evidence baseline:** the latest pushed Spark integration run for commit `2d8f9db`
completed successfully; local Python 3.13 on Windows still skips Spark worker tests by design.
The non-Spark suite, Ruff, and mypy pass on the current branch. Re-run the exact commands in
the acceptance gates below after every implementation batch; do not infer live workspace
completion from local results.

> Legend: ✅ done · ⚠️ partial/wired-wrong · ❌ missing · 🐛 confirmed defect (reproducible today)

---

## 0. What NOT to redo (already solid — verify, don't rewrite)

- `uc_metadata_reader` (SDA 04): scoped discovery, comments/tags/owners/constraints, RELY kept
  as unvalidated context, sensitivity signal list (not a binary flag), warnings — all present
  and reasonably well tested (`src/sda/tools/uc_metadata_reader.py`, 80% cov).
- `table_profile_spark.py` source-snapshot handling: `DESCRIBE HISTORY` + `VERSION AS OF`
  pinning, data-change vs. maintenance-op distinction, schema-drift detection, profile reuse —
  this is the *reference implementation* SDA 07's Spark job should be copying
  (`src/sda/job_entrypoints/table_profile_spark.py:204-335`).
- Pure profiling helpers (`src/sda/profiling/{numeric,categorical,strings,missingness,
  conditional_nulls,temporal,common,loaders}.py`) are 89–100% covered and match the article's
  metric lists closely.
- SDA 06 gates-before-weights and RELY-is-not-evidence principles are implemented correctly
  (`relationships/detector.py:106-107`, `relationships/scoring.py`).
- SDA 07 rule-safety invariants: an `OBSERVED`-origin rule cannot be `HARD` strength, and an
  unapproved `USER_PROVIDED` hard rule is rejected at construction
  (`src/sda/patterns/rules.py:33-40`, tested).
- `RulePrecedencePolicy` tiers match the article's 6-tier default order exactly
  (`src/sda/patterns/precedence.py:18-26` vs. article §10.3).
- `resolve_rule_conflicts()` now sets a real `winning_rule_id` when origin ranks differ
  (`src/sda/patterns/conflicts.py:41-68`) — this was a confirmed defect in the prior audit and
  is fixed.
- `pattern_detect_spark.py` quick mode now actually samples the source
  (`source.sample(fraction=..., seed=...)`, line 132-137) — sampling flags were previously dead;
  this is fixed.
- `tests/test_spark_metrics_contract.py` runs **real local-Spark-session** tests against every
  SDA 07 Spark metric function including `spark_fanout_by_segment` — the prior audit's "no real
  PySpark proof" finding is **resolved** for the metric layer (still gaps at the entrypoint
  layer — see §4).
- `pattern_detector` **is** wired into `.github/workflows/databricks-integration.yml` (line
  65-68) — the prior audit's "CI doesn't run pattern_detector" finding is **stale**, though the
  step has a gating problem (see §7).

---

## 1. SDA 01 / SDA 02 — Architecture & orchestration contract

These describe the target end-state (agent-as-orchestrator, artifact-first, tool contracts);
SDA 03–07 are the only pieces currently in scope for implementation. No action items here
beyond keeping later work consistent with:

- [ ] Agent never calculates statistics itself — only deterministic tools do (currently true;
      keep true when wiring SDA 07's coordinator).
- [ ] Every tool call must persist an auditable `ArtifactRef` (currently true).

No pending build tasks specific to these two articles; they are satisfied by the sum of the
work below.

---

## 2. SDA 03 — Bundle & environment isolation

**Status: mostly complete.** Dev/staging/prod are separated (`bundle/targets/{dev,staging,
prod}.yml`), variables are target-scoped, `pattern_detector`/`table_profiler`/
`relationship_detector`/`analyze_scope` all have their own job resources with per-environment
tables (`bundle/resources.yml`, `databricks.yml:95-126`).

### Pending
- [ ] Confirm `root_path` is explicit (not `/Shared`) for staging/prod targets — spot-check
      `bundle/targets/staging.yml` / `prod.yml`.
- [ ] Confirm target-level `permissions:` ACL blocks exist for all three targets per the
      article's 3-layer model (deploy identity / run-as identity / UC grants) — not verified in
      this pass; do a direct diff against the article's example.
- [ ] `pyproject.toml:8` description still reads `"SDA 06 governed metadata, profiling, and
      relationship evidence for Databricks."` — cosmetic, but update once SDA 07 is genuinely
      complete so package metadata doesn't undersell/misdescribe the project.

### Tests
- [ ] `scripts/validate_bundle_config.py --target staging` and `--target prod` in CI (currently
      only `dev` is validated per `Makefile:bundle-validate`).

---

## 3. SDA 04 — `uc_metadata_reader`

**Status: strong.** 80% coverage; scoped allowlists, RELY-as-hint, sensitivity signal list,
constraint capture with informational/unvalidated framing are all implemented per the article.

### Pending (minor)
- [ ] Sample the uncovered line ranges in `src/sda/tools/uc_metadata_reader.py` (156, 160-161,
      174-176, 180-201, 215-227, 254, 377, 540-542, 568, 573, 613, 625, 631, 647, 653, 685-688,
      703-704, 715-722, 810, 824, 830-846, 896-898, 920, 939, 944, 949, 963, 973-975, 992,
      996-998, 1002-1006) and confirm each is either genuinely unreachable defensive code or a
      real gap needing a test — this pass did not fully triage all ~70 missing lines.

### Tests
- [ ] Close the coverage gap above with targeted unit tests once triaged (don't chase the
      percentage blindly — only add tests for real production branches).

---

## 4. SDA 05 — `table_profiler`

**Status: strong on the pure path, one confirmed Spark-parity defect.**

### 4.1 Confirmed defect 🐛 — Spark outlier methods silently incomplete
`src/sda/tools/table_profiler.py:560` — `profile_spark()`'s only outlier branch is:
```python
if "iqr" in self.request.outlier_methods and percentile_values[2] is not None:
```
There is **no** `"percentile"` or `"mad"` branch anywhere in `profile_spark()` (confirmed via
grep across the whole file — only one `outlier_methods` membership check exists). The pure
implementation `src/sda/profiling/outliers.py::numeric_outliers()` supports all three methods
(`iqr`, `percentile`, `mad`, lines 21-65). Worse: `table_profile_spark.py:44`'s own CLI default
is `--outlier-methods iqr,percentile` — so **the production default already requests a method
the Spark path silently drops**, with no warning emitted.

- [ ] Implement `percentile`-method and `mad`-method outlier detection in
      `TableProfiler.profile_spark()` (mirror `profiling/outliers.py`'s formulas: p01/p99
      fences for percentile; `median ± 3×1.4826×MAD` for mad), or explicitly emit a
      `"outlier_method_unavailable_in_spark_path"` warning per unsupported requested method
      instead of silently dropping it.
- [ ] Add a parity test: same fixture run through `numeric_outliers()` (pure) and
      `TableProfiler.profile_spark()` (Spark) with `outlier_methods=("iqr","percentile","mad")`
      must produce the same set of methods in the output (values may differ due to
      `percentile_approx`, but methods requested vs. methods returned must match, or the gap
      must be an explicit warning).

### 4.2 Other pending items
- [ ] `src/sda/profiling/complex_types.py` (27% cov) — the article requires "flag binary,
      large-text, and unsupported structures clearly" and "record schema depth and element
      types" for STRUCT; current `complex_metrics()` only returns top-level `field_names`/
      `field_count` for structs, no depth/nested-element-type recording, and there is no
      explicit `binary`/large-text handling branch at all (only `array`/`map`/`struct`/
      fallback-`variant` are handled). Decide whether binary/large-text profiling is in scope
      for this milestone; if not, the function should still emit an explicit warning for those
      types rather than silently falling into the generic "unsupported_variant_profile" branch
      for unrelated types.
- [ ] `src/sda/profiling/freshness.py` (50% cov, 10 lines) — pure `business_freshness()` looks
      correct but is thin; confirm it's actually invoked by the pure (non-Spark) profiling path
      analogous to how `table_profile_spark.py:204-335` computes storage freshness — verify
      wiring, not just existence.
- [ ] `src/sda/profiling/persistence.py` (26% cov) — largest uncovered surface in the profiling
      package; triage which persistence branches (reuse lookup, schema creation, version
      comparison) are production paths vs. untested edge cases.
- [ ] Deterministic hash-based sampling over a stable key (article: "When repeatable row
      selection matters, deterministic hash-based sampling over a stable key is safer than
      relying only on random sampling") — confirm `--stable-key-column` in
      `table_profile_spark.py:39` actually drives hash-based (not `TABLESAMPLE REPEATABLE`)
      sampling when set; not verified in this pass.

### Tests
- [ ] Outlier-method Spark/pure parity test (4.1 above).
- [ ] Complex-type profiling fixtures: array, map, struct with nested fields, binary column,
      large-text column — assert warnings are emitted for anything not fully profiled, and that
      nothing is silently cast to string (article's explicit requirement).
- [ ] Reproducibility test: same snapshot + config + seed → identical profile (article §"Exact,
      Approximate, and Sampled are Different Claims" / quick-vs-full section) — not confirmed to
      exist; add if missing.

---

## 5. SDA 06 — `relationship_detector`

**Status: solid core metrics, one confirmed over-permissive classification defect, one
architecture note on Spark-scale coverage.**

### 5.1 Confirmed defect 🐛 — bridge-table over-classification
Article definition (verbatim): *"A bridge — or junction — table often contains two or more
foreign keys, **repeated values on each foreign-key side**, and a composite uniqueness rule or
equivalent surrogate-key design."*

Actual classification, identical in both places it's implemented:
```python
# src/sda/relationships/detector.py:145-147 (pure)
bridge_tables = sorted(
    table for table, parents in parents_by_child.items() if len(parents) >= 2
)
# src/sda/job_entrypoints/analyze_scope_spark.py:433-435 (Spark) — same expression
```
This flags **any** table with two or more accepted parent relationships as a bridge table —
e.g. an ordinary `orders` table with an unrelated `customer_id → customers` FK and
`store_id → stores` FK would be wrongly classified as a bridge, because there is no check for
repeated values / composite uniqueness at all in the pure path.

- The **Spark** path (`analyze_scope_spark.py:436-455`) does compute follow-up evidence per
  candidate bridge (`bridge_validation[bridge] = {"pair_unique": total_rows == distinct_links,
  "duplicate_rate": ...}`) — but this evidence is *not* used to filter/refine `bridge_tables`
  itself; a false positive with `pair_unique=False` still appears in the `bridge_tables` list,
  just annotated.
- The **pure** path (`relationships/detector.py`) has no `bridge_validation` step at all.

- [ ] Fix the classification itself: a table should only be classified `bridge_tables` when the
      combined FK columns are (near-)unique per row (i.e. `pair_unique` true, or duplicate rate
      below a configured threshold) — not merely "≥2 parents". Move the Spark path's validation
      *before* the classification, not after.
- [ ] Port `bridge_validation`-equivalent evidence (`pair_unique`/`duplicate_rate` per candidate)
      into the pure `RelationshipDetector.detect()` so both paths produce equivalent evidence
      (SDA 07 and later articles need this parity).
- [ ] Add a test fixture: an ordinary two-FK child table that is *not* a bridge (unrelated
      1:N + 1:N, no repeated pair values) — assert it is **not** classified as
      `bridge_tables`, alongside the existing-style genuine many-to-many bridge fixture.

### 5.2 Architecture note — full pipeline is not proven at Spark scale in one place
`relationship_detect_spark.py` (single explicit pair, `measure_spark_join` only — no candidate
discovery, no scoring, no graph) is a different, narrower job from `analyze_scope_spark.py`
(the actual full-scale candidate→score→graph→bridge→cycle pipeline). This is architecturally
fine (explicit single-pair validation vs. full-scope discovery are legitimately different jobs)
but:
- [ ] Confirm test coverage explicitly proves parity between `analyze_scope_spark.py`'s Spark
      pipeline and `relationships/detector.py::RelationshipDetector`'s pure pipeline on the same
      fixture (same accepted/rejected decisions, same cardinality/fan-out/bridge/cycle results).
      `relationships/spark_metrics.py` is only 11% covered — most of it is likely exercised only
      indirectly through `analyze_scope_spark.py`; add direct unit tests for the individual
      Spark metric functions the way `test_spark_metrics_contract.py` does for SDA 07.

### 5.3 Other pending items
- [ ] `MATCH FULL` semantics: confirmed via grep that composite-key partial-null handling exists
      (`relationships/metrics.py:73`, `"partial_composite_keys_present"` warning), but no
      `MATCH FULL`/`match_full` string appears anywhere in `src/`. The article requires
      preserving a declared constraint's configured match semantics rather than imposing a
      different rule during validation — confirm whether Databricks' declared `MATCH FULL`
      setting is read from `uc_metadata_reader`'s constraint metadata at all, and if not, either
      implement it or document explicitly that only implicit (non-MATCH-FULL) semantics are
      supported in this milestone.
- [ ] Composite-key column-order preservation — confirm a test asserts this explicitly (article
      requirement, "Databricks supports composite primary and foreign-key declarations, so
      declared column order must be preserved").

### Tests
- [ ] Non-bridge two-FK fixture (5.1).
- [ ] Genuine bridge fixture with `pair_unique=True` evidence asserted, not just presence in
      `bridge_tables`.
- [ ] Spark/pure parity fixture (5.2).
- [ ] `MATCH FULL` declared-constraint fixture (5.3), once scoped.

---

## 6. SDA 07 — `pattern_detector` (primary focus of this branch)

**Status: the coordinator now executes far more of the article's pipeline than the prior audit
found (conditional distribution, conditional missingness, state transitions, and rule
evaluation are all wired into `detect_coordinated()` and the Spark job now — this is real,
verified progress). However, the coordinator still does not consume upstream artifact
*content*, does not use its own generated candidates, ignores several already-built modules,
and the evidence contract has multiple confirmed gaps.** This is the largest section of the
plan.

### 6.1 Article's required execution pipeline (§11.2) vs. current state

| Stage (article §11.2) | Status | Where |
|---|---|---|
| Read metadata, profiles, relationship graph | ❌ | `input_refs` used only to build ID tuples for fingerprinting (`detector.py:133-138`); content never loaded |
| Validate scope, permissions, approved columns | ⚠️ | `approved_columns` respected if passed (`detector.py:163`); `patterns/inputs.py::require_pattern_inputs` (status/environment/type checks) exists but is **never called** by `detect_coordinated()` |
| Assign driver/outcome/entity/context/time roles | ❌ | `patterns/roles.py::assign_roles` exists, fully implemented, **never called** (0% coverage) |
| Generate and prune candidate patterns | ⚠️ | `generate_candidates()` is called (`detector.py:176`) but its result is discarded — only used for `receipt.candidate_count_total`; actual detection uses ad hoc `categorical[0]`/`numeric[0]`/first-match selection instead |
| Run quick discovery metrics | ⚠️ | Spark job samples in quick mode now (real fix); local coordinator has no quick/full distinction at all |
| Verify strong candidates on approved population | ❌ | no separate verification pass exists in either path |
| Merge observed, declared, and user-provided rules | ⚠️ | only `rules` param (assumed all user/domain-provided) is evaluated; no distinct "declared" rule source from metadata constraints |
| Detect conflicts, weak support, temporal instability | ⚠️ | conflict detection + precedence ranking work (`conflicts.py`), but `stability()` is never called anywhere — every pattern's `stability_quality` is hardcoded `"unknown"` |
| Persist versioned pattern registry | ✅ | `persistence.py`, registry v2, mostly solid |
| Return findings and review questions | ⚠️ | works, but over-broad (every pattern gets a review question — see 6.6) |

### 6.2 Upstream evidence is validated by ID, never consumed by content 🐛
- [ ] `detect_coordinated()` never calls `patterns/loaders.py::load_pattern_inputs()` or
      `patterns/inputs.py::require_pattern_inputs()` — both exist, are correctly implemented
      (status/environment/type/completeness checks), and have **0% test coverage** because
      nothing calls them. Wire one of them into `detect_coordinated()` before candidate
      generation, and make it fail closed (raise `ArtifactNotFoundError`/
      `ArtifactCompatibilityError`) exactly as designed.
- [ ] Once wired, use the loaded metadata (comments/tags/sensitivity), profile (cardinality,
      distributions), and relationship (entity keys, fan-out) content to drive role assignment
      and candidate generation — not just to prove the IDs exist.
- [ ] `pattern_detect_spark.py` has its own separate, duplicate validation logic
      (`main():95-128`, re-checking `ref.artifact_type` inline) instead of reusing
      `inputs.require_pattern_inputs` — consolidate to one validation path shared by both the
      local coordinator and the Spark entrypoint so they can't drift.

### 6.3 Role assignment exists, is unused, and candidate generation is too narrow 🐛
- [ ] Wire `patterns/roles.py::assign_roles()` into `detect_coordinated()` and
      `pattern_detect_spark.py::main()`. Currently `role_overrides` is accepted as a parameter
      (`detector.py:128`) but **never referenced in the method body** — dead parameter.
- [ ] `patterns/candidates.py::generate_candidates()` only ever emits `CORRELATION` and
      `CONDITIONAL_DISTRIBUTION` candidates (lines 77-87). `FanoutSegmentCandidate`,
      `TemporalOrderCandidate`, `StateTransitionCandidate` dataclasses are fully defined
      (lines 36-62) and **never instantiated** anywhere. Extend `generate_candidates()` to emit
      all pattern families the article requires, driven by assigned roles (entity/lifecycle
      columns → temporal + state-transition candidates; relationship fan-out → fanout
      candidates), and make `detect_coordinated()` actually iterate the generated candidate set
      instead of `categorical[0]`/`numeric[0]`/first-match heuristics
      (`detector.py:179-270` throughout).
- [ ] Exclude free text, sensitive attributes, and extremely-high-cardinality identifiers from
      candidate generation by default (article §3.1, explicit requirement) — currently no
      cardinality- or sensitivity-based exclusion exists in `candidates.py` at all;
      `roles.py:19-22`'s excluded-name heuristic (email/ssn/password/token substrings) is the
      closest thing, and it's unused (see above).

### 6.4 Fan-out-by-segment is fully implemented and never called anywhere 🐛
- `patterns/fanout.py::fanout_by_segment()` (pure) — implemented, unit tested
  (`test_fanout_includes_zero_child_parents`), never called by `detector.py`.
- `patterns/spark_metrics.py::spark_fanout_by_segment()` — implemented with proper percentile
  aggregation, unit tested with real Spark
  (`test_spark_fanout_includes_zero_child_parents`), **never imported** by
  `pattern_detect_spark.py` (confirmed: its `spark_metrics` import list at lines 155-161 omits
  it).
- `PatternFamily.FANOUT_BY_SEGMENT` is a defined enum value that is **never emitted** by any
  production code path.
- [ ] Wire `fanout_by_segment`/`spark_fanout_by_segment` into `detect_coordinated()` and
      `pattern_detect_spark.py::main()`, driven by relationship-detector-validated entity keys
      (once §6.2 is wired), following the article's §5.2 worked example ("high-value customers
      have more transactions" — start from the full parent population, not just parents with
      children, so zero-child parents aren't silently excluded — `fanout_by_segment()` already
      does this correctly, it just needs to be called).

### 6.5 Correlation family is incomplete vs. the article
- Article §4.1 explicitly requires **both** Pearson and Spearman, and explicitly warns that
  Spark's `DataFrame.corr`/SQL `corr` are Pearson-only and Spearman needs the MLlib
  `Correlation` API on assembled vectors.
- [ ] `patterns/correlations.py::spearman()` exists and is unit tested but is **never imported**
      by `detector.py` (only `pearson` is imported) or `pattern_detect_spark.py`.
      `PatternConfig.include_spearman` is defined but never read anywhere in the codebase — dead
      config flag. Wire Spearman into the pure coordinator when `include_spearman=True`.
- [ ] There is **no `spark_spearman` function at all** in `spark_metrics.py` — not dead code,
      genuinely unimplemented. Implement it using Spark MLlib's `Correlation` API per the
      article's explicit guidance, since plain `F.corr`/`DataFrame.corr` cannot produce it.
- [ ] `correlation_outlier_diagnostic()` (`correlations.py:38-52`) is implemented and unit
      tested but never called — article §4.1 requires recording "whether outliers dominate the
      result." Wire it in and persist the diagnostic in pattern evidence.
- [ ] Numeric-outcome-by-categorical-driver conditional distributions (article example:
      "amount by product category and currency") have a ready-made helper,
      `patterns/numeric.py::numeric_by_group()`, that is implemented, unit-testable, and
      **never called** anywhere — currently `detect_coordinated()`'s conditional-distribution
      logic only handles categorical outcomes via `conditional_counts()`
      (`detector.py:182-199`). Wire `numeric_by_group()` in for numeric-outcome candidates.

### 6.6 Evidence contract gaps (confirmed, field-by-field)
- [ ] 🐛 `support_rate` is **always `None`** — `detector.py:376` hardcodes `None` as the
      `Pattern.support_rate` positional argument for every pattern emitted by `_pattern()`,
      even though `metric` dicts often contain enough information to compute it (e.g.
      `support_rows / population_rows`). The article's output contract (§12) requires
      `support_rate` on every applicable pattern.
- [ ] 🐛 `association_name` is derived by `next(iter(p.metric), None)` in
      `patterns/persistence.py:75` — the first dict key, not a semantic name. Confirmed for
      correlations: `correlations.py::pearson()` returns `{"value": ..., "valid_pair_count":
      ..., "method": ...}` (dict-insertion order), so every persisted correlation pattern gets
      `association_name = "value"`. Replace with an explicit per-family semantic name
      (`pearson`, `spearman`, `conditional_probability`, `lift`, `null_probability`,
      `transition_probability`, ...).
- [ ] 🐛 `evidence_quality["stability_quality"]` is hardcoded to the literal string `"unknown"`
      at `detector.py:361` for every pattern — `patterns/stability.py::stability()` is fully
      implemented and unit tested but never called from `_pattern()` or `detect_coordinated()`.
      Wire it in once stability slicing (time/source/geography, article §7.3) is scoped.
- [ ] 🐛 `evidence_quality["source_quality"]` is hardcoded to `"compatible"` at
      `detector.py:362` for every pattern regardless of actual source-snapshot compatibility —
      this should reflect whether the upstream artifacts' source snapshot actually matches the
      one used for detection (depends on §6.2 and §6.9 being wired).
- [ ] `patterns/safety.py::safe_pattern_value()` is implemented and unit tested but
      `_pattern()` never routes condition/outcome values through it — raw categorical values go
      straight into `condition`/`outcome` dicts (e.g. `{driver: rows[0].get(driver)}` at
      `detector.py:208`, `{"from_state": transition["from_state"]}` at `detector.py:229-230`).
      `PatternConfig.sensitive_value_policy` is defined but never read anywhere. Wire
      `safe_pattern_value()` into `_pattern()`'s condition/outcome construction, gated by
      `sensitive_value_policy`, once role/sensitivity assignment (§6.3) is available to tell it
      which columns are sensitive.
- [ ] `patterns/actions.py::GenerationAction`/`ValidationAction` dataclasses (with `condition`,
      `fallback_levels`, `tolerance`, `metric` fields) are defined but `_pattern()` builds
      `generation_action`/`validation_action` as hand-rolled dicts with only `kind` and
      `evidence_pattern_id` (`detector.py:383-402`) — the richer fields are never populated.
      Either use the dataclasses directly or extend the hand-rolled dicts to match.
- [ ] `patterns/temporal_lags.py::deterministic_fallback_plan()` is implemented, never called —
      article §5.3/§7.2 explicitly require recording the segment-fallback path in the pattern
      artifact when a subgroup lacks support. `PatternConfig.min_support_rate` exists but is not
      applied as an actual acceptance/suppression gate anywhere in `detect_coordinated()` or
      `pattern_detect_spark.py` beyond a couple of ad hoc `>=` checks against raw row counts —
      audit the whole coordinator for `min_support_rate` usage and make it a real gate,
      including the fallback-to-broader-segment behavior.

### 6.7 Rule-strength taxonomy has a naming/dimension bug
Article §9.1 defines exactly 4 strength classes: **Hard invariant, Conditional requirement,
Probabilistic pattern, Anomaly signal.** Origin (observed/declared/user_provided/
domain_approved) is a *separate* dimension (§2.2).

- [ ] `patterns/rules.py::RuleStrength` has 5 values: `OBSERVED = 1, PROBABILISTIC = 2,
      CONDITIONAL = 3, HARD = 4, ANOMALY_SIGNAL = 0`. `OBSERVED` does not belong in a *strength*
      enum — it's already `PatternOrigin.OBSERVED` in `models.py:23`. Having the same word mean
      two different things (origin vs. strength) on the same rule is a real design defect.
      Either remove `RuleStrength.OBSERVED` (rules without an explicit strength should default
      to `PROBABILISTIC` per the article) or rename it to something that isn't a duplicate
      concept.
- [ ] `RuleStrength.ANOMALY_SIGNAL` exists nominally but **nothing in the codebase ever assigns
      it** — no anomaly-detection logic produces it. Either implement a minimal anomaly-signal
      classification path (e.g. rare/unstable patterns with weak support get flagged this way)
      or explicitly scope it out and note why.
- [ ] `Pattern.rule_strength` (string field on the `Pattern` dataclass itself, default
      `"probabilistic_pattern"`, `models.py:140`) is **never set from `rule.strength`**
      anywhere in `_pattern()` — every emitted pattern, including business-rule findings built
      from an evaluated `BusinessRule` with its own real `strength`, keeps the hardcoded
      default. Fix `_pattern()` to accept and propagate the actual strength.

### 6.8 Receipt fields are mostly unpopulated 🐛
`PatternExecutionReceipt` (`models.py:65-79`) has 14 fields. In `detect_coordinated()`
(`detector.py:273-285`) only 6 are ever set (`candidate_count_total`, `patterns_emitted`,
`patterns_accepted_for_planning`, `patterns_review_required`, `rules_evaluated`,
`source_tables_scanned`, `sample_fraction`, `sample_seed`) — always default/zero:
- [ ] `conflicts_found` — `resolve_rule_conflicts()` result (`rule_conflicts`, line 272) is
      computed and used for warnings/review_questions but its **count is never assigned** to
      `receipt.conflicts_found`. One-line fix: `conflicts_found=len(rule_conflicts)`.
- [ ] `candidate_count_by_family`, `candidate_skipped_by_reason` — never populated; needed once
      §6.3's candidate generation covers all families, so skip-reasons are meaningful.
- [ ] `patterns_rejected`, `patterns_insufficient` — `accepted`/`review_required` are counted
      (line 271) but `REJECTED`/`INSUFFICIENT_EVIDENCE` decisions are not separately tallied,
      even though `PatternDecision` already has those values (`models.py:35-36`).
- [ ] `source_tables_reused` — never set (relevant once artifact reuse short-circuits are
      exercised across multiple tables).
- [ ] `rules_evaluated` is computed by calling `evaluate_rule(rows, rule)` **a second time**
      per rule purely to get a count (`detector.py:278-281`) — it was already evaluated once in
      the loop at lines 234-251. Redundant double computation; thread the already-computed
      `RuleEvaluationResult` through instead of recomputing, and reconsider whether "evaluated"
      should mean "ran" (all rules) vs. "ran and met min_support_rows" (current, narrower
      semantics) — pick one and document it.

### 6.9 Source-snapshot pinning is weaker than SDA 05's own reference implementation
- [ ] `pattern_detect_spark.py:311-313` always sets
      `source_references=(SourceReference(source_table, "TABLE", "best_effort", None, None,
      None),)` — no Delta version is ever pinned, unconditionally `"best_effort"` regardless of
      whether `DESCRIBE HISTORY` would succeed. `table_profile_spark.py:204-256` already
      implements exactly this (history lookup, latest-data-change detection, graceful
      `best_effort` fallback only when unavailable) — port that logic into
      `pattern_detect_spark.py` and use `spark.sql(f"SELECT * FROM {table} VERSION AS OF
      {version}")` the same way, so pattern detection is reproducible against the same snapshot
      its upstream metadata/profile/relationship artifacts were computed against.
- [ ] `detect_coordinated()`'s local artifact_ref (`detector.py:302`) sets
      `source_references=()` — completely empty, not even `"best_effort"`. Fix once the Spark
      entrypoint pattern above is ported (or pass source references through from the caller).

### 6.10 Pattern-level identity is run-dependent, not content-addressable 🐛
- [ ] `_pattern()`'s `pattern_id = "pat_" + fingerprint({"analysis": analysis_id, "family":
      ..., "columns": ..., "condition": ...})` (`detector.py:347-354`), and
      `detect_coordinated()` passes `run_id` as `analysis_id` for every family (directly, and
      via `self.detect(..., analysis_id=run_id)` at line 177) — so `pattern_id` depends on the
      run's `run_id`. Two identical detection runs on unchanged data produce **different**
      `pattern_id`s for the same finding. The artifact-level `reuse_fingerprint`
      (`detector.py:139-148`) is correctly content-based (excludes `run_id`) and short-circuits
      identical reruns via `find_reusable()` — but if that short-circuit *doesn't* fire (e.g.
      one upstream artifact ID legitimately changed while this table's evidence didn't), a
      rerun produces a full set of new pattern IDs for logically-identical findings, breaking
      any downstream tracking (review decisions, lifecycle history) keyed on `pattern_id`.
      Redefine `analysis_id`'s role inside `_pattern()`'s fingerprint input — it should be a
      stable content key (table + config + detector version), with `run_id` staying a
      lineage-only receipt field, not a hashed input.

### 6.11 `pattern_detect_spark.py` entrypoint issues
- [ ] 🐛 Misleading/fragile control flow at `main():95-105`: `input_ids` (built at lines 88-91
      from `[metadata_artifact_id, *profile_ids] + [relationship_artifact_id,
      dependency_graph_artifact_id]`) is **always a non-empty tuple** structurally (3-4
      elements) even when every underlying ID string is `""`. So
      `if input_ids and not args.artifact_registry_table: raise SystemExit(...)` (lines 95-96)
      unconditionally rejects **any** invocation that omits `--artifact-registry-table` —
      including a bare local/dev call requesting no upstream artifacts at all — which
      contradicts the CLI's own `default=""` declaration for that argument (line 39). Then the
      `for artifact_id, expected_type in zip(...)` loop (line 105) is indented at the *same*
      level as `if args.artifact_registry_table and input_ids:` (line 97), i.e. outside that
      block — this only fails to crash today because line 95-96 already forces
      `args.artifact_registry_table` truthy before line 105 can be reached. This reproduces the
      exact shape of a previously-reported crash bug; fix both: (a) only require the registry
      table when actual (non-empty-string) artifact IDs were supplied, and (b) fix the
      indentation so the loop is unambiguously inside its guard regardless of what line 95-96
      does.
- [ ] No rule evaluation, conflict detection, or precedence resolution at all in this file —
      `rules`/`business-rules` isn't even a CLI parameter. Once the local coordinator's rule
      path (§6.7/§6.8) is solid, decide whether rules should be passed to the Spark job too (via
      a rules-table reference) or whether Spark-side pattern detection intentionally excludes
      rule evaluation (document the decision either way — currently it's just silently absent).
- [ ] Role/sensitivity-driven column selection is entirely ad hoc dtype/name heuristics
      (`columns.items() if dtype in {"double","float","int","bigint","decimal"}` for numeric,
      `name.lower().endswith(("_id","id"))` for entity, `name.lower() in {"status","state",
      "stage","lifecycle_state"}` for state) instead of `roles.py` output or metadata
      sensitivity signals — a numeric column tagged sensitive in metadata can still become a
      correlation candidate purely because of its Spark dtype. Depends on §6.2/§6.3.

### Tests (SDA 07)
- [ ] **End-to-end multi-family coordinator fixture** — the single highest-value missing test.
      Currently `tests/test_pattern_contracts.py::test_coordinator_requires_all_upstream_
      artifacts_and_reports_receipt` is the *only* test exercising `detect_coordinated()`, and
      it's a 2-row, 2-numeric-column fixture that only exercises the correlation path. Build a
      compact customers/accounts/orders fixture (per the article's §13.1 adversarial list and
      the prior audit's §57) with: customer segment, premium tier, product category, nullable
      cancellation timestamp, lifecycle timestamps, entity IDs, status transitions, one rare
      valid transition, one legacy-violation cluster, one user rule that conflicts with observed
      data. Run it through `detect_coordinated()` and assert **every applicable family** is
      emitted (correlation, conditional_distribution, conditional_missingness, fanout_by_segment,
      temporal_order, state_transition, business_rule) with populated evidence — not just that
      `patterns_emitted > 0`.
- [ ] `pattern_detect_spark.py::main()` entrypoint tests — none exist today. Cover: required
      args only (no registry, no upstream IDs) succeeds; upstream IDs supplied without registry
      table fails with a clear message (validates the §6.11 fix); wrong artifact type at a
      given ID fails; quick mode samples (assert `sample()` called with configured
      fraction/seed, or assert row-count reduction on a fixture); full mode does not sample;
      persistence success and failure paths.
- [ ] Spearman test: monotonic-but-nonlinear fixture where Pearson is weak and Spearman is
      strong — assert both are computed and both are persisted with distinct
      `association_name`s once §6.5/§6.6 land.
- [ ] Correlation-outlier-dominated fixture — a handful of extreme values flip or inflate an
      otherwise-weak Pearson correlation; assert `correlation_outlier_diagnostic`'s
      `sign_changed` flag is surfaced in evidence once wired.
- [ ] Aggregate-correlation-reverses-after-segmentation (Simpson's paradox) fixture — article
      §13.1 explicit requirement, currently absent.
- [ ] Fan-out fixture through `detect_coordinated()` (not just the standalone
      `fanout_by_segment()` unit test) — zero-child parents preserved in the denominator,
      segment-level lift computed.
- [ ] Conditional-missingness fixture with source-system/period clustering of violations
      (article §6.3's "separate absence from delayed availability") — currently only a flat
      support/violation-rate test exists.
- [ ] Tiny-segment-with-misleading-100%-support fixture (article §13.1 item 6) — assert
      `min_support_rate` actually suppresses it once §6.6's fallback-plan wiring lands.
- [ ] Rule-conflict-through-`detect_coordinated()` fixture — assert `receipt.conflicts_found`
      is correctly populated (currently would fail — always 0) and that
      `review_questions` includes the conflict with a real `reason_code`.
- [ ] `association_name` correctness test — assert a correlation pattern's persisted
      `association_name == "pearson"`, not `"value"` (currently would fail).
- [ ] `support_rate` non-null test — assert a pattern with known population/support rows has
      `support_rate == support_rows / population_rows` (currently would fail — always `None`).
- [ ] `InMemoryArtifactRegistry.require_latest_complete()` test — assert it raises a clear,
      typed error (not `AttributeError`) or is fixed to not need `self.spark`/`self.table`
      (§6.2-adjacent defect, `src/sda/artifacts/registry.py:44-62`).
- [ ] `spark_spearman` unit test (once implemented) mirroring
      `test_spark_metrics_contract.py`'s pattern for the other Spark metric functions.
- [ ] Effective-date rule change fixture (article §13.1 item 10) — a rule valid before date X,
      different/absent after — currently absent.
- [ ] Rename/update stale test-file references: the prior build plan referenced
      `test_pattern_candidates.py`, `test_pattern_correlations.py`, etc. that don't exist —
      the repo has consolidated into `test_pattern_contracts.py`,
      `test_pattern_persistence_evidence.py`, `test_candidate_pruning.py`, etc. Any release
      checklist or CI step referencing the old names must be updated to the real files (none
      found referencing the stale names in this repo's own scripts — only in the external
      prior-audit docs — so no fix needed here, just don't reintroduce the stale names).

---

## 7. CI / release / integration gaps

- [ ] `.github/workflows/databricks-integration.yml:65-68` — the `pattern_detector` smoke step
      is gated behind `if: vars.SDA_PATTERN_METADATA_ID != '' && vars.SDA_PATTERN_PROFILE_ID !=
      '' && vars.SDA_PATTERN_RELATIONSHIP_ID != '' && vars.SDA_PATTERN_GRAPH_ID != ''` — four
      GitHub Environment variables that **nothing in the workflow populates** from the preceding
      `uc_metadata_reader`/`table_profiler`/`relationship_detector`/`analyze_scope` smoke jobs'
      actual output artifact IDs. In a fresh environment (or any environment where those vars
      were never manually set) this step silently no-ops every run, so `pattern_detector` is
      effectively never smoke-tested in practice despite being "wired in." Either (a) capture
      the preceding jobs' output artifact IDs and export them as step outputs feeding this job's
      `--params`, or (b) document explicitly that these vars must be manually maintained and add
      a workflow-level warning/summary line when the step is skipped so it's visible, not silent.
- [ ] This whole workflow is `workflow_dispatch`-only (manual trigger) — confirm that's
      intentional (likely yes, since it needs live Databricks credentials) and not accidentally
      excluded from a required merge gate.
- [ ] Neither SDA 06 nor SDA 07's integration test creates/tears down dedicated short-lived
      managed UC test tables in a dedicated test schema, as both articles explicitly request
      (SDA 06 §"Testing the Detector": *"add an integration test using short-lived managed
      tables in a dedicated Unity Catalog test schema... run uc_metadata_reader and
      relationship_detector together... then remove the test objects"*; SDA 07's prior-audit
      §38 equivalent). Currently the smoke jobs just run against whatever
      `sda_profile_source_table` etc. already point to. Add a managed-UC fixture
      create/populate/run/assert/teardown flow.
- [ ] `bundle-validate` (`Makefile:26`) only validates `-t dev` — extend to staging/prod (see
      §2).

---

## 8. Master "100% Definition of Done" checklist

Use this as the final acceptance gate. Nothing in this document should be considered complete
until every box below is checked **and** the corresponding test in §4/§5/§6/§7 passes.

### Cross-cutting
- [ ] All 28 confirmed defects/gaps listed in §4–§7 fixed.
- [ ] `python -m pytest` green with no reduction in the current 137-test baseline.
- [ ] `python -m pytest -m spark` green in CI (already true — keep it true after changes).
- [ ] Coverage on every file touched by this plan is above the repo's current 69% floor — no
      net regression; prioritize real production-path coverage over chasing the percentage.
- [ ] `ruff check src tests` and `mypy src tests` clean.
- [ ] `python scripts/check_release.py` clean on a fresh `build`/`dist`.

### SDA 05
- [ ] Spark outlier methods match pure implementation (percentile, mad) or explicitly warn.
- [ ] Complex-type profiling (array/map/struct/binary) matches article requirements with
      explicit warnings for anything not fully profiled.
- [ ] Reproducibility test passes (same snapshot+config+seed → identical profile).

### SDA 06
- [ ] Bridge-table classification requires composite-uniqueness evidence, not just parent count.
- [ ] Pure and Spark relationship pipelines produce equivalent evidence on the same fixture.
- [ ] `MATCH FULL` semantics scoped and either implemented or explicitly documented as
      out-of-scope.

### SDA 07 — the coordinator genuinely implements the article end-to-end
- [ ] Metadata artifact is genuinely consumed (content, not just ID validation).
- [ ] Profile artifact is genuinely consumed.
- [ ] Relationship artifact and entity keys are genuinely consumed.
- [ ] Dependency graph/fan-out evidence is genuinely consumed.
- [ ] Source snapshot is pinned and compatible across metadata/profile/relationship/pattern
      artifacts (ported from `table_profile_spark.py`'s reference implementation).
- [ ] Roles (`assign_roles`) drive candidate generation; `role_overrides` is actually used.
- [ ] Sensitive fields are excluded from candidates by policy, and sensitive values are routed
      through `safe_pattern_value()` before persistence.
- [ ] Correlation family supports configured Pearson **and** Spearman, pure and Spark, with
      outlier-diagnostic evidence attached.
- [ ] Conditional distributions include global baseline, lift, and cover numeric outcomes
      (`numeric_by_group`) not just categorical ones.
- [ ] Conditional missingness distinguishes structural/temporary/suspicious absence where
      lineage/source-system evidence is available.
- [ ] Fan-out-by-segment is executed in production (pure and Spark), using validated
      relationship entity keys.
- [ ] Temporal ordering and lag distributions are executed in production, respecting the
      event-time-vs-ingestion-time distinction (`temporal.py::ordered_events`, currently
      unwired into the coordinator).
- [ ] State transitions are executed in production (already true — keep true).
- [ ] Structured user/domain rules are evaluated in the local coordinator (already true — keep
      true) and a documented decision exists for whether Spark-side rule evaluation is in scope.
- [ ] Rule conflicts are detected and `receipt.conflicts_found` accurately reflects them.
- [ ] Rule precedence is applied and the winning rule is persisted (already true — keep true).
- [ ] `RuleStrength` taxonomy matches the article's 4 classes without conflating origin and
      strength; `Pattern.rule_strength` is actually populated from the rule.
- [ ] Minimum support rows **and** minimum support rate are enforced as real gates, with
      segment fallback applied and the fallback path recorded (`deterministic_fallback_plan`
      wired in).
- [ ] Stability is computed (`stability()` wired in) and reflected in `evidence_quality`,
      replacing the hardcoded `"unknown"`/`"compatible"` placeholders.
- [ ] Quick mode performs bounded/sampled discovery; full mode verifies on the complete
      population — a real behavioral difference beyond "quick samples, full doesn't."
- [ ] `support_rate` is populated on every applicable pattern.
- [ ] `association_name` is a semantic name, not a dict-iteration artifact.
- [ ] Pattern identity (`pattern_id`) is content/config-based, not `run_id`-based.
- [ ] Review questions are targeted (only patterns that actually need domain approval, weak/
      unstable evidence, conflicts, etc. — not every single pattern).
- [ ] `pattern_detect_spark.py::main()` is directly tested, including the
      registry-required-vs-not-required control-flow fix.
- [ ] Every Spark pattern metric function (including the new `spark_spearman`) has a real
      local-Spark-session test.
- [ ] A multi-family end-to-end coordinator fixture test passes, covering every pattern family
      together in one run.
- [ ] `pattern_detector` runs in Databricks integration CI **and actually executes** (not
      silently skipped by unset gating variables).
- [ ] `InMemoryArtifactRegistry.require_latest_complete()` no longer raises `AttributeError`.

---

## 9. Suggested execution order

1. **Unblock testability first**: fix `pattern_detect_spark.py`'s registry-guard control flow
   (§6.11) and `InMemoryArtifactRegistry.require_latest_complete()` (§6.2's registry defect) —
   both are cheap, isolated, and unblock writing entrypoint tests.
2. **Wire upstream evidence consumption** (§6.2): `require_pattern_inputs`/`load_pattern_inputs`
   into `detect_coordinated()`, source-snapshot pinning ported from `table_profile_spark.py`
   into `pattern_detect_spark.py`. Everything else in SDA 07 depends on this being real.
3. **Wire role assignment and full candidate generation** (§6.3) — depends on step 2's loaded
   metadata/profile content.
4. **Wire the missing families into both execution paths**: fan-out (§6.4), Spearman + Spark
   Spearman + outlier diagnostics (§6.5), numeric-outcome conditional distributions (§6.5).
5. **Fix the evidence contract** (§6.6, §6.7, §6.8, §6.10) — mostly mechanical once steps 2-4
   supply real data to compute these fields from.
6. **SDA 05/06 fixes** (§4, §5) — independent of the SDA 07 work above, can run in parallel.
7. **Build the end-to-end multi-family fixture and entrypoint tests** (§6 Tests) — write these
   *as* each family is wired in, not all at the end, so regressions are caught immediately.
8. **CI gating fix** (§7) — last, once there's something real for the smoke job to prove.
