import json

from sda.artifacts.models import SourceReference
from sda.patterns.models import Pattern, PatternFamily, PatternOrigin
from sda.patterns.persistence import registry_rows


def test_pattern_registry_rows_preserve_lineage_fields():
    pattern = Pattern(
        pattern_id="p1", analysis_id="a1", family=PatternFamily.CORRELATION,
        origin=PatternOrigin.OBSERVED, primary_table="main.s.t", columns=("x", "y"),
        condition={}, outcome={}, support_rows=10, support_rate=1.0, metric={"value": 1.0},
        evidence_quality={},
    )
    source = SourceReference("main.s.t", "TABLE", "best_effort", None, None, None)
    row = registry_rows(
        (pattern,), configuration_hash="cfg", input_artifact_ids=("m1", "p1"),
        source_references=(source,),
    )[0]
    assert row["configuration_hash"] == "cfg"
    assert json.loads(row["input_artifact_ids_json"]) == ["m1", "p1"]
    assert json.loads(row["source_references_json"])[0]["full_name"] == "main.s.t"
