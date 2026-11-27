"""Distributed Spark adapter for the controlled-noise contract."""

from __future__ import annotations

import re
from typing import Any

from sda.artifacts.fingerprint import fingerprint
from sda.noise import PROFILE_DEFAULT_RATES, SUPPORTED_DEFECTS, NoiseProfile
from sda.runtime.identifiers import QualifiedName


class NoiseSparkError(ValueError):
    """Raised when a Spark noise plan cannot be applied safely."""


def _schema_parts(schema: str) -> tuple[str, str]:
    parts = schema.split(".")
    if len(parts) != 2 or any(not part.strip() for part in parts):
        raise NoiseSparkError("staging schema must be catalog.schema")
    QualifiedName.parse(f"{parts[0]}.{parts[1]}.placeholder")
    return parts[0], parts[1]


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", run_id).strip("_")[:80] or "run"


def noise_table_names(staging_schema: str, run_id: str) -> tuple[str, str, str, str]:
    """Return separate clean, noisy, ledger, and manifest destinations."""
    catalog, schema = _schema_parts(staging_schema)
    suffix = _safe_run_id(run_id)
    return (
        f"{catalog}.{schema}.sda13_clean__{suffix}",
        f"{catalog}.{schema}.sda13_noisy__{suffix}",
        f"{catalog}.{schema}.sda13_truth_ledger__{suffix}",
        f"{catalog}.{schema}.sda13_manifest__{suffix}",
    )


def _target_defects(profile: NoiseProfile, row_count: int, budget: int, rate: float | None) -> int:
    if budget < 0:
        raise NoiseSparkError("noise budget must not be negative")
    if rate is not None and not 0.0 <= rate <= 1.0:
        raise NoiseSparkError("noise rate must be between zero and one")
    if budget and rate is not None:
        raise NoiseSparkError("noise budget and rate are mutually exclusive")
    desired = budget if budget else int((rate if rate is not None else PROFILE_DEFAULT_RATES[profile]) * row_count + 0.5)
    return min(max(desired, 0), row_count)


def generate_noise_spark(
    spark: Any,
    *,
    run_id: str,
    staging_schema: str,
    noise_id: str,
    profile: str = "qa",
    defect_type: str = "malformed_value",
    budget: int = 0,
    rate: float | None = None,
    seed: int = 1729,
    row_count: int = 40,
) -> dict[str, Any]:
    """Write an immutable clean baseline and a deterministic noisy derivative."""
    try:
        normalized_profile = NoiseProfile(profile)
    except ValueError as exc:
        raise NoiseSparkError(f"unsupported noise profile: {profile}") from exc
    if defect_type not in SUPPORTED_DEFECTS:
        raise NoiseSparkError(f"unsupported defect type: {defect_type}")
    if defect_type not in {"malformed_value", "null_injection", "casing", "drift"}:
        raise NoiseSparkError(
            "Spark smoke adapter supports malformed_value, null_injection, casing, and drift"
        )
    if not noise_id.strip() or seed < 0 or row_count < 1:
        raise NoiseSparkError("noise_id, non-negative seed, and positive row_count are required")
    target_defects = _target_defects(normalized_profile, row_count, budget, rate)
    catalog, schema = _schema_parts(staging_schema)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    clean_name, noisy_name, ledger_name, manifest_name = noise_table_names(staging_schema, run_id)
    F = __import__("pyspark.sql.functions", fromlist=["functions"])
    Window = __import__("pyspark.sql.window", fromlist=["Window"]).Window

    baseline_fingerprint = fingerprint(
        {
            "noise_id": noise_id,
            "row_count": row_count,
            "profile": normalized_profile.value,
            "seed": seed,
            "clean_schema": ["record_id", "email", "status", "event_at"],
        }
    )
    clean = (
        spark.range(row_count)
        .withColumnRenamed("id", "record_id")
        .withColumn("email", F.concat(F.lit("user-"), F.col("record_id"), F.lit("@example.test")))
        .withColumn(
            "status",
            F.when(F.pmod(F.col("record_id"), F.lit(2)) == 0, F.lit("active")).otherwise(
                F.lit("inactive")
            ),
        )
        .withColumn(
            "event_at",
            F.to_timestamp(
                F.date_add(F.to_date(F.lit("2025-01-01")), F.col("record_id").cast("int"))
            ),
        )
    )
    selection_token = F.concat_ws(
        "|", F.lit(noise_id), F.lit(str(seed)), F.lit("selection"), F.col("record_id")
    )
    marked = clean.withColumn(
        "__noise_rank",
        F.row_number().over(Window.orderBy(F.xxhash64(selection_token), F.col("record_id"))),
    )
    selected = marked.where(F.col("__noise_rank") <= F.lit(target_defects))
    if defect_type == "malformed_value":
        noisy_value = F.concat(F.col("email"), F.lit("__MALFORMED"))
    elif defect_type == "null_injection":
        noisy_value = F.lit(None).cast("string")
    elif defect_type == "casing":
        noisy_value = F.upper(F.col("email"))
    else:
        noisy_value = F.concat(F.lit("drift-"), F.col("record_id").cast("string"))
    noisy = (
        marked.withColumn(
            "email",
            F.when(F.col("__noise_rank") <= F.lit(target_defects), noisy_value).otherwise(
                F.col("email")
            ),
        )
        .drop("__noise_rank")
    )
    ledger = selected.select(
        F.lit(run_id).alias("run_id"),
        F.col("record_id"),
        F.lit(defect_type).alias("defect_type"),
        F.sha2(
            F.concat_ws("|", F.lit(noise_id), F.lit(str(seed)), F.col("record_id")), 256
        ).alias("selection_fingerprint"),
        F.lit(baseline_fingerprint).alias("baseline_fingerprint"),
    )
    clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        clean_name
    )
    noisy.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        noisy_name
    )
    ledger.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        ledger_name
    )
    ledger_count = ledger.count()
    noisy_count = noisy.count()
    output_fingerprint = fingerprint(
        {
            "baseline_fingerprint": baseline_fingerprint,
            "noise_id": noise_id,
            "defect_type": defect_type,
            "seed": seed,
            "ledger_count": ledger_count,
        }
    )
    manifest = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "noise_id": noise_id,
                "profile": normalized_profile.value,
                "defect_type": defect_type,
                "seed": seed,
                "requested_defects": target_defects,
                "applied_defects": ledger_count,
                "clean_rows": row_count,
                "noisy_rows": noisy_count,
                "baseline_fingerprint": baseline_fingerprint,
                "output_fingerprint": output_fingerprint,
                "status": "NOISY_AWAITING_VALIDATION",
                "clean_table": clean_name,
                "noisy_table": noisy_name,
                "truth_ledger_table": ledger_name,
            }
        ]
    )
    manifest.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        manifest_name
    )
    return {
        "run_id": run_id,
        "status": "NOISY_AWAITING_VALIDATION",
        "clean_rows": row_count,
        "noisy_rows": noisy_count,
        "requested_defects": target_defects,
        "applied_defects": ledger_count,
        "clean_table": clean_name,
        "noisy_table": noisy_name,
        "truth_ledger_table": ledger_name,
        "manifest_table": manifest_name,
        "baseline_fingerprint": baseline_fingerprint,
        "output_fingerprint": output_fingerprint,
    }
