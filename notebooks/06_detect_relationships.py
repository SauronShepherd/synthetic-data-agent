"""SDA 06 notebook driver.

The production notebook should replace ``tables`` and ``rows`` with the
normalized uc_metadata_reader and cached table_profiler artifacts. All
relationship logic remains importable from ``sda.relationships``.
"""

from sda.job_entrypoints.relationship_detect import detect_relationships


def run(tables, rows, run_id=None):
    artifact = detect_relationships(tables, rows, run_id=run_id)
    print(
        {
            "relationships": len(artifact["relationships"]),
            "accepted": sum(r.get("decision") == "accepted" for r in artifact["relationships"]),
            "generation_order": artifact["generation_order"],
            "cycles": artifact["cycles"],
        }
    )
    return artifact
