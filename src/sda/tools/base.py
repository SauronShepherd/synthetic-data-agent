"""Tool interfaces for deterministic execution components."""

from __future__ import annotations

from typing import Protocol

from sda.models import AgentState, ToolName, ToolResult


class AgentTool(Protocol):
    """Protocol implemented by every deterministic tool."""

    @property
    def name(self) -> ToolName:
        """Return the stable tool identifier."""

    def run(self, state: AgentState) -> ToolResult:
        """Execute using the current state and return an auditable result."""
