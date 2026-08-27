.PHONY: test lint format format-check typecheck build spark-test release-check bundle-validate check

test:
	python -m pytest

lint:
	python -m ruff check src tests

format:
	python -m ruff format src tests

format-check:
	python -m ruff format --check src tests

typecheck:
	python -m mypy src tests

build:
	python -m build --sdist --wheel

release-check:
	python scripts/check_release.py

bundle-validate:
	python scripts/validate_bundle_config.py --target dev
	python scripts/validate_bundle_config.py --target staging
	python scripts/validate_bundle_config.py --target prod

spark-test:
	python -m pytest -m spark

check: lint typecheck test
