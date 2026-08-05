from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_bundle_build_is_portable() -> None:
    content = (ROOT / "databricks.yml").read_text(encoding="utf-8")
    assert "C:\\Users\\" not in content
    assert "python -m pip wheel --no-deps --no-build-isolation -w dist ." in content


def test_serverless_dependencies_are_strings() -> None:
    content = (ROOT / "bundle" / "resources.yml").read_text(encoding="utf-8")
    assert "dependencies:\n              - ../dist/*.whl" in content
    assert "libraries:\n            - whl:" not in content


def test_production_targets_do_not_grant_current_user_manage() -> None:
    for target in ("staging.yml", "prod.yml"):
        content = (ROOT / "bundle" / "targets" / target).read_text(encoding="utf-8")
        assert "workspace.current_user.userName" not in content
