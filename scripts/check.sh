#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="$(pwd)/src${PYTHONPATH:+:${PYTHONPATH}}"

ruff check .
mypy src
pytest
python scripts/smoke_local.py
