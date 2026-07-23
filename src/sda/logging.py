"""Central logging configuration."""

from __future__ import annotations

import logging


def configure_logging(level: str = "INFO") -> None:
    """Configure application logging once using a compact format."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        force=True,
    )
