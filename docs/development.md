# Development guide

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
```

## Branch milestone

Suggested branch: `feature/article-01-bootstrap`  
Suggested tag after merge: `article-01`
