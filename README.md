# Synthetic Data Agent — Article 01

This repository is the runnable starting point for the **Synthetic Data Agent on Databricks** series.

It accompanies the article:

> **SDA 01: Why Build a Synthetic Data Agent on Databricks?**  
> https://medium.com/towards-data-engineering/sda-01-why-build-a-synthetic-data-agent-on-databricks-1c1b4e0738b7

Article 01 defines the mission: build a governed, explainable workflow that learns from Unity Catalog data and eventually generates useful synthetic datasets while preserving schema, distributions, null behaviour, relationships, and business rules.

This branch intentionally implements only the engineering foundation. It does **not** connect to Databricks, inspect Unity Catalog, call an LLM, or generate synthetic rows yet.

## Included in this milestone

- Installable Python package using a `src/` layout
- Environment-based configuration
- Central logging setup
- Small command-line interface
- Unit tests
- Ruff and mypy configuration
- GitHub Actions continuous integration
- Architecture and contribution documentation

## Requirements

- Python 3.11 or newer

## Install

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Use the CLI

```bash
sda --help
sda hello
sda version
sda config
```

Example:

```text
$ sda hello
Synthetic Data Agent
A governed synthetic-data project, built article by article.
```

Configuration is read from environment variables:

```bash
export SDA_APP_NAME="Synthetic Data Agent"
export SDA_ENVIRONMENT="dev"
export SDA_LOG_LEVEL="DEBUG"
sda config
```

## Run quality checks

```bash
pytest
ruff check .
mypy src
```

Or run everything with:

```bash
make check
```

## Project layout

```text
.
├── .github/workflows/ci.yml
├── docs/
│   ├── architecture.md
│   └── development.md
├── src/sda/
│   ├── __init__.py
│   ├── __main__.py
│   ├── cli.py
│   ├── config.py
│   ├── logging.py
│   └── version.py
├── tests/
├── Makefile
└── pyproject.toml
```

## Scope boundary

Article 01 is the project declaration and repository baseline. Later article branches add architecture, Databricks deployment, Unity Catalog metadata discovery, profiling, relationship detection, pattern detection, durable state, generation, controlled noise, validation, and publication.

Synthetic data is not automatically private or safe. This repository makes no privacy guarantee; future milestones must add explicit governance, privacy assessment, validation, and approval controls.
