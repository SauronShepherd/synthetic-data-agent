from __future__ import annotations

import json

from sda.cli import main
from sda.version import __version__


def test_hello(capsys) -> None:  # type: ignore[no-untyped-def]
    assert main(["hello"]) == 0
    output = capsys.readouterr().out
    assert "Synthetic Data Agent" in output
    assert "built article by article" in output


def test_version(capsys) -> None:  # type: ignore[no-untyped-def]
    assert main(["version"]) == 0
    assert capsys.readouterr().out.strip() == __version__


def test_config(capsys) -> None:  # type: ignore[no-untyped-def]
    assert main(["config"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["environment"] == "dev"
