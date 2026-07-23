# Development guide — Article 03

Article 03 adds the Databricks bootstrap layer while keeping local development simple. Local checks validate the Python contracts. Databricks checks validate the real workspace path.

## Local development

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
ruff check .
mypy src tests
pytest
```

## Local CLI commands

```powershell
sda hello
sda version
sda config
sda design-demo
```

`design-demo` still runs the Article 02 in-memory orchestration simulation and stops at `plan_drafted`.

## Databricks development loop

From the repository root:

```powershell
databricks bundle validate -t dev
databricks bundle plan -t dev
databricks bundle deploy -t dev
databricks bundle run bootstrap_check -t dev
databricks bundle summary -t dev
```

Use the explicit `-t` target flag even when `dev` is the default. The habit matters once staging and production exist. The development job prepares its own Unity Catalog bootstrap objects when your run identity has the required privileges; otherwise it falls back to a visible catalog/schema for the smoke test.

## Branch milestone

Suggested branch: `feature/article-03-databricks-bootstrap`
Suggested tag after merge: `article-03`

## What belongs where

- `src/sda/`: reusable, testable Python logic.
- `notebooks/`: thin Databricks job entry points.
- `bundle/`: Databricks resource and target declarations.
- `scripts/grants/`: optional reference SQL for controlled staging/prod setup.
- `docs/`: architecture and operational notes.

Do not put long-term business logic directly in notebooks. The bootstrap notebook is a wrapper around small Python contracts and the first Unity Catalog metadata check.
