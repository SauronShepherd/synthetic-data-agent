#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="$(pwd)/src${PYTHONPATH:+:${PYTHONPATH}}"

python -m ruff check .
python -m mypy src
python -m pytest
python scripts/smoke_local.py
