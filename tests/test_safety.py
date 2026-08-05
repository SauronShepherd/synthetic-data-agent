import pytest

from sda.runtime.errors import AuthorizationScopeError
from sda.runtime.safety import require_output_scope


def test_output_scope_rejects_source_overlap() -> None:
    with pytest.raises(AuthorizationScopeError):
        require_output_scope(
            {"main.source.orders"}, "main.source.orders", evidence_schema="main.source"
        )


def test_output_scope_accepts_configured_evidence_schema() -> None:
    require_output_scope(set(), "main.evidence.artifacts", evidence_schema="main.evidence")
