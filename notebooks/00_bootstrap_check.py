# Databricks notebook source
# MAGIC %md
# MAGIC # SDA 03 — Self-Contained Bootstrap Check
# MAGIC
# MAGIC This job proves that the bundle can deploy, pass target-specific parameters,
# MAGIC run under the configured identity, prepare a minimal development Unity Catalog
# MAGIC scope when allowed, and inspect metadata through `system.information_schema`.
# MAGIC
# MAGIC In development, it can create `sda_dev.sample_source`, `sda_dev.sandbox`, and a
# MAGIC tiny `sample_customers` table automatically. In staging and production, the same
# MAGIC code should normally validate existing governed assets instead of creating them.

# COMMAND ----------

import json
import sys
from pathlib import Path


def _ensure_local_src_on_path() -> None:
    """Make bundled source files importable in common Databricks execution layouts."""
    candidates = [
        Path.cwd() / "src",
        Path.cwd().parent / "src",
        Path.cwd().parent.parent / "src",
    ]
    for candidate in candidates:
        if (candidate / "sda").exists():
            sys.path.insert(0, str(candidate))
            return


try:
    from sda.bootstrap import build_bootstrap_summary, load_bootstrap_parameters
    from sda.uc_discovery import (
        discover_unity_catalog_scope,
        prepare_unity_catalog_scope,
    )
except ModuleNotFoundError:
    _ensure_local_src_on_path()
    from sda.bootstrap import build_bootstrap_summary, load_bootstrap_parameters
    from sda.uc_discovery import (
        discover_unity_catalog_scope,
        prepare_unity_catalog_scope,
    )

# COMMAND ----------

requested_parameters = load_bootstrap_parameters(dbutils.widgets.get)  # noqa: F821
print(
    json.dumps(
        {"requested_bootstrap_parameters": requested_parameters.as_dict()},
        indent=2,
        sort_keys=True,
    )
)

# COMMAND ----------

prepared_scope = prepare_unity_catalog_scope(spark, requested_parameters)  # noqa: F821
parameters = prepared_scope.parameters
print(
    json.dumps(
        {
            "effective_bootstrap_parameters": parameters.as_dict(),
            "bootstrap_warnings": list(prepared_scope.warnings),
        },
        indent=2,
        sort_keys=True,
    )
)

# COMMAND ----------

discovery = discover_unity_catalog_scope(spark, parameters)  # noqa: F821

summary = build_bootstrap_summary(
    parameters=parameters,
    visible_tables=discovery.visible_tables,
    visible_columns=discovery.visible_columns,
    warnings=prepared_scope.warnings,
)

summary_payload = summary.as_dict()
print(json.dumps({"bootstrap_summary": summary_payload}, indent=2, sort_keys=True))

# COMMAND ----------

try:
    dbutils.jobs.taskValues.set(  # noqa: F821
        key="bootstrap_summary", value=json.dumps(summary_payload)
    )
except Exception as exc:  # pragma: no cover - Databricks runtime only
    print(f"Could not set task value outside a job context: {exc}")
