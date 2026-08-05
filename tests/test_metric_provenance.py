from sda.profile_models import CalculationMethod, MetricEvidence, PopulationScope


def test_metric_evidence_has_orthogonal_provenance() -> None:
    metric = MetricEvidence(
        value=0.5,
        population_scope=PopulationScope.SAMPLE,
        calculation_method=CalculationMethod.APPROXIMATE,
        sample_fraction=0.1,
        sample_seed=42,
        algorithm="percentile_approx",
    )

    payload = metric.to_dict()
    assert payload["population_scope"] == "sample"
    assert payload["calculation_method"] == "approximate"
    assert payload["sample_seed"] == 42
