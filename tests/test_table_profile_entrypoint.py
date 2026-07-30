from sda.job_entrypoints.table_profile_spark import parse_args


def test_profile_entrypoint_requires_one_source_and_parses_mode() -> None:
    args = parse_args(
        ["--source-table", "samples.tpcds_sf1.date_dim", "--mode", "quick", "--sample-seed", "7"]
    )
    assert args.source_table == "samples.tpcds_sf1.date_dim"
    assert args.mode == "quick"
    assert args.sample_seed == 7
