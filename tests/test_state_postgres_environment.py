import pytest

from sda.state_postgres import LakebaseConnectionConfig


def test_lakebase_connection_config_reads_databricks_app_variables() -> None:
    values = {
        "PGHOST": "ep.example",
        "PGDATABASE": "sda_state",
        "PGUSER": "sda-app",
        "PGSSLMODE": "require",
        "PGCONNECT_TIMEOUT": "7",
        "TOKEN": "fresh-token",
    }
    config = LakebaseConnectionConfig.from_environment(environ=values, token_variable="TOKEN")
    assert (config.host, config.database, config.user, config.connect_timeout) == (
        "ep.example",
        "sda_state",
        "sda-app",
        7,
    )
    assert config.token_provider() == "fresh-token"


def test_lakebase_connection_config_rejects_missing_or_invalid_environment() -> None:
    with pytest.raises(ValueError, match="PGDATABASE"):
        LakebaseConnectionConfig.from_environment(environ={"PGHOST": "h", "PGUSER": "u"})
    with pytest.raises(ValueError, match="integer"):
        LakebaseConnectionConfig.from_environment(
            environ={
                "PGHOST": "h",
                "PGDATABASE": "d",
                "PGUSER": "u",
                "PGCONNECT_TIMEOUT": "nope",
            }
        )
