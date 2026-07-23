# Development guide — Article 02

## Windows PowerShell

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
ruff check .
mypy src tests
pytest
```

## Commands

```powershell
sda hello
sda version
sda config
sda design-demo
```

`design-demo` runs a deterministic architecture simulation for:

```text
customers -> accounts -> transactions
```

It stops at `plan_drafted`. No Databricks connection is made and no synthetic rows are generated.

## Branch milestone

Suggested branch: `feature/article-02-agent-design`  
Suggested tag after merge: `article-02`
## Type-checking third-party packages

The project type-checks its own `src` and `tests` trees. Mypy is configured with
`follow_imports = "skip"` so it does not recursively parse unrelated typed packages
from the active virtual environment. This prevents environment-specific packages
(such as NumPy pulled in by another tool) from breaking SDA checks while the SDA
source itself remains checked in strict mode.

