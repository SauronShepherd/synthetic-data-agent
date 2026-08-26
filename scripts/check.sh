#!/usr/bin/env bash
set -euo pipefail

ruff check .
mypy src
pytest
python scripts/smoke_local.py
