from __future__ import annotations

import pytest

from sda.models import GenerationRequest, SourceScope


def test_source_scope_requires_tables() -> None:
    with pytest.raises(ValueError, match="at least one"):
        SourceScope(catalog="main", schema="sales", tables=())


def test_source_scope_rejects_duplicate_tables() -> None:
    with pytest.raises(ValueError, match="unique"):
        SourceScope(catalog="main", schema="sales", tables=("customers", "customers"))


def test_generation_request_requires_positive_scale() -> None:
    scope = SourceScope(catalog="main", schema="sales", tables=("customers",))
    with pytest.raises(ValueError, match="greater than zero"):
        GenerationRequest(request_id="req", source=scope, scale_factor=0)


def test_target_location_is_all_or_nothing() -> None:
    scope = SourceScope(catalog="main", schema="sales", tables=("customers",))
    with pytest.raises(ValueError, match="provided together"):
        GenerationRequest(request_id="req", source=scope, target_catalog="main")
