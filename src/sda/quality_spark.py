"""Spark quality validation for the SDA 14 fixture."""

from __future__ import annotations

import json
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.runtime.identifiers import QualifiedName


class QualitySparkError(ValueError):
    """Raised when a quality validation request is unsafe or unavailable."""


def _name(value: str) -> QualifiedName:
    try:
        return QualifiedName.parse(value)
    except Exception as exc:
        raise QualitySparkError("expected a safe catalog.schema.table name") from exc


def validate_quality_spark(
    spark: Any,
    *,
    baseline_table: str,
    candidate_table: str,
    report_table: str,
    run_id: str,
    expected_count: int,
    expected_defects: int = 0,
    intended_use: str = "qa",
) -> dict[str, Any]:
    """Run hard structural gates and a compact defect-evidence check."""
    baseline = _name(baseline_table)
    candidate = _name(candidate_table)
    report = _name(report_table)
    if expected_count < 0 or expected_defects < 0:
        raise QualitySparkError("quality expectations must not be negative")
    if not spark.catalog.tableExists(baseline.full_name):
        raise QualitySparkError(f"baseline table does not exist: {baseline.full_name}")
    if not spark.catalog.tableExists(candidate.full_name):
        raise QualitySparkError(f"candidate table does not exist: {candidate.full_name}")
    left = spark.table(baseline.quoted)
    right = spark.table(candidate.quoted)
    F = __import__("pyspark.sql.functions", fromlist=["functions"])
    baseline_count = left.count()
    candidate_count = right.count()
    required = {"record_id", "email", "status", "event_at"}
    missing = sorted(required - set(right.columns))
    null_keys = right.where(F.col("record_id").isNull()).limit(1).count()
    duplicate_keys = right.groupBy("record_id").count().where(F.col("count") > 1).limit(1).count()
    malformed = right.where(F.col("email").rlike(r"__MALFORMED$")).count()
    checks = [
        {
            "check_id": "row_count",
            "status": "PASS" if candidate_count == expected_count else "FAIL",
            "actual": candidate_count,
            "expected": expected_count,
        },
        {"check_id": "schema", "status": "PASS" if not missing else "FAIL", "missing": missing},
        {"check_id": "key_nulls", "status": "PASS" if null_keys == 0 else "FAIL", "null_keys": null_keys},
        {
            "check_id": "key_duplicates",
            "status": "PASS" if duplicate_keys == 0 else "FAIL",
            "duplicate_keys": duplicate_keys,
        },
        {
            "check_id": "expected_defects",
            "status": "PASS" if malformed == expected_defects else "FAIL",
            "actual": malformed,
            "expected": expected_defects,
        },
    ]
    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    payload = {
        "run_id": run_id,
        "intended_use": intended_use,
        "technical_disposition": status,
        "baseline_rows": baseline_count,
        "candidate_rows": candidate_count,
        "checks": checks,
    }
    payload["report_fingerprint"] = fingerprint(payload)
    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS `{report.catalog}`.`{report.schema}`"
    )
    spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "intended_use": intended_use,
                "technical_disposition": status,
                "report_fingerprint": payload["report_fingerprint"],
                "report_json": json.dumps(payload, sort_keys=True),
            }
        ]
    ).write.format("delta").mode("append").saveAsTable(report.quoted)
    if status != "PASS":
        raise QualitySparkError("quality output failed a hard validation gate")
    return payload
