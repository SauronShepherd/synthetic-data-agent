.PHONY: test lint typecheck check

test:
	pytest

lint:
	ruff check .

typecheck:
	mypy src tests

check: lint typecheck test
