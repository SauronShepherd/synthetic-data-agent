# ADR 0013: Scope of rule evaluation in Spark pattern detection

## Decision

Structured user and domain rules are evaluated by the local coordinator, where the
approved rule objects, precedence policy, effective dates, and conflict review state are
available. The Spark entrypoint evaluates data-derived pattern families and persists
bounded aggregate evidence; it does not accept or deserialize arbitrary rule definitions.

## Rationale

Passing rule definitions through job parameters would make the rule boundary opaque and
would risk persisting unapproved hard rules. Keeping rule evaluation in the coordinator
preserves the SDA-07 safety contract: origin and strength are validated before evaluation,
precedence is resolved centrally, and conflicts become review questions. Spark remains the
scalable path for raw-row aggregation and receives only validated, bounded inputs.

## Verification

- `PatternDetector.detect_coordinated()` evaluates supplied `BusinessRule` objects and
  records rule conflicts in `PatternExecutionReceipt.conflicts_found`.
- `pattern_detect_spark.py` validates upstream artifact types, pins the source snapshot when
  Delta history is available, and runs the Spark metric families without collecting raw rows.
- The Databricks integration workflow runs both paths' applicable smoke checks and tears down
  its managed Unity Catalog fixture.
