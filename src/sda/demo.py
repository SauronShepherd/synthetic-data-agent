"""Article 02 architecture demonstration."""

from __future__ import annotations

from sda.models import AgentState, GenerationRequest, SourceScope
from sda.orchestrator import SyntheticDataAgent
from sda.tools import article_02_toolchain


def run_design_demo() -> AgentState:
    """Run the design-only customers -> accounts -> transactions vertical slice."""
    request = GenerationRequest(
        request_id="article-02-demo",
        source=SourceScope(
            catalog="main",
            schema="sales",
            tables=("customers", "accounts", "transactions"),
        ),
        scale_factor=10.0,
        preserve_relationships=True,
        privacy_mode="strict",
        target_catalog="main",
        target_schema="synthetic_sales",
    )
    state = AgentState(request=request)
    return SyntheticDataAgent().run(state, article_02_toolchain())
