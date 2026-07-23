.PHONY: test lint typecheck check bundle-validate-dev bundle-deploy-dev bundle-run-dev bundle-summary-dev

test:
	pytest

lint:
	ruff check .

typecheck:
	mypy src tests

check: lint typecheck test

bundle-validate-dev:
	databricks bundle validate -t dev

bundle-deploy-dev:
	databricks bundle deploy -t dev

bundle-run-dev:
	databricks bundle run bootstrap_check -t dev

bundle-summary-dev:
	databricks bundle summary -t dev
