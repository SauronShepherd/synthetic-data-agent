# Synthetic Data Agent — Article 02

This repository evolves the Article 01 foundation into the architecture described in **SDA 02: Designing the Synthetic Data Agent**.

Article 02 establishes the system boundary: the agent interprets and orchestrates, while specialized deterministic tools calculate facts, execute data work, and emit auditable artifacts. State and validation are mandatory; generation is not allowed to jump directly from a user prompt to published data.

## What this milestone adds

- Typed source scope and generation request contracts
- Explicit workflow stages and guarded state transitions
- Deterministic tool protocol
- Design stubs for metadata, profiling, relationships, and planning
- Auditable artifact references, warnings, and tool history
- `customers -> accounts -> transactions` design demo
- Architecture decision records
- Unit tests for contracts, transition safety, and CLI behavior

## What it deliberately does not add

- Databricks SDK or Spark
- Unity Catalog queries
- Statistical profiling
- Relationship inference
- LLM integration
- Synthetic row generation
- Lakebase persistence
- Publishing

Those capabilities arrive article by article. This branch stops at a reviewable generation plan by design.

## Requirements and installation

Python 3.11 or newer.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Run the design demo

```powershell
sda design-demo
```

The JSON output records the normalized request, completed tools, stage, and architecture artifacts. The final stage is `plan_drafted`.

## Quality checks

```powershell
ruff check .
mypy src tests
pytest
```

Or:

```powershell
make check
```

## Project layout

```text
src/sda/
├── cli.py
├── config.py
├── demo.py
├── models.py
├── orchestrator.py
└── tools/
    ├── base.py
    └── design_stubs.py

docs/
├── architecture.md
├── development.md
└── adr/
```

## Article references

- Article 01: [Why Build a Synthetic Data Agent on Databricks?](https://medium.com/towards-data-engineering/sda-01-why-build-a-synthetic-data-agent-on-databricks-1c1b4e0738b7)
- Article 02 source document: `🤖 SDA 02 Designing the Synthetic Data Agent`

## Scope and privacy note

Synthetic data is not automatically private. This milestone defines privacy mode as part of the request contract, but it does not claim to implement privacy protection or prove safety. Later milestones must add deterministic controls, validation, review, and governed publication.
