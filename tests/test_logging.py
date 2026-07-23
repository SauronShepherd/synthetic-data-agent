from __future__ import annotations

import logging

from sda.logging import configure_logging


def test_configure_logging_sets_root_level() -> None:
    configure_logging("DEBUG")
    assert logging.getLogger().level == logging.DEBUG
