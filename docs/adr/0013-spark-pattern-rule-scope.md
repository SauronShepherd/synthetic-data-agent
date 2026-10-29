# ADR 0013: Scope of rule evaluation in Spark pattern detection

## Decision

Structured user and domain rules can be evaluated by both the local coordinator
and the Spark entrypoint. Spark accepts a JSON list of typed rule definitions
with a closed predicate vocabulary, checks effective dates and conflicts, and
persists bounded aggregate evidence. No arbitrary SQL or Python expression is
accepted as a rule predicate.

## Rationale

Job parameters are not a governed approval record. Even if a supplied rule is
marked approved, Spark emits a review-required finding rather than an active
hard constraint. Both paths use the typed rule model and precedence/conflict
handling; Spark evaluates large source tables through aggregate expressions.
Promotion to a hard generation rule still requires an independently governed
owner/reviewer decision and auditable rule artifact.

## Verification

- `PatternDetector.detect_coordinated()` evaluates supplied `BusinessRule` objects and
  records rule conflicts in `PatternExecutionReceipt.conflicts_found`.
- `pattern_detect_spark.py` validates upstream artifacts, pins the source snapshot when
  Delta history is available, and evaluates Spark metric families and typed business
  rules without collecting raw rows.
- The Databricks bundle smoke path runs both paths' applicable checks against a
  managed Unity Catalog fixture; the commands and evidence are recorded locally
  and in `docs/sda07-definition-of-done-audit.md`.
