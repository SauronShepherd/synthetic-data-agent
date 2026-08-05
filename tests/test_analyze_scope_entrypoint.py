from argparse import Namespace

from sda.job_entrypoints.analyze_scope_spark import run


def test_analyze_scope_entrypoint_validates_scope_without_scanning_values() -> None:
    result = run(
        object(),
        Namespace(
            catalog="sda_dev",
            schema="sample_source",
            tables="sample_customers",
            run_id="run-1",
            dry_run=True,
        ),
    )

    assert result["status"] == "DRY_RUN"
    assert result["scope"] == ["sda_dev.sample_source.sample_customers"]
