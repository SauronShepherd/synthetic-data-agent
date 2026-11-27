from __future__ import annotations

import pytest

from sda.noise_spark import NoiseSparkError, noise_table_names


def test_noise_destinations_keep_clean_and_noisy_baselines_separate() -> None:
    names = noise_table_names("sda_dev.sda_integration", "noise/one")
    assert names[0].endswith("sda13_clean__noise_one")
    assert names[1].endswith("sda13_noisy__noise_one")
    assert names[0] != names[1]


def test_noise_destinations_reject_unsafe_schema() -> None:
    with pytest.raises(NoiseSparkError, match="catalog.schema"):
        noise_table_names("sda_dev", "run")
