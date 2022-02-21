"""Test IRI methods as called from the pipeline."""
from pathlib import Path

import pytest
from conftest import FAKE_AA_DATA_DIR, ISO3

from aatoolbox.datasources.iri.iri_seasonal_forecast import _MODULE_BASENAME

FAKE_IRI_AUTH = "def"


@pytest.fixture(scope="session", autouse=True)
def mock_aa_data_dir(session_mocker):
    """Mock out the AA_DATA_DIR environment variable."""
    session_mocker.patch.dict(
        "aatoolbox.config.pathconfig.os.environ",
        {"AA_DATA_DIR": FAKE_AA_DATA_DIR, "IRI_AUTH": FAKE_IRI_AUTH},
    )


@pytest.fixture(autouse=True)
def xr_load_dataset(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch(
        "aatoolbox.datasources.iri.iri_seasonal_forecast.xr.load_dataset"
    )


def test_iri_load(pipeline, mocker, xr_load_dataset):
    """Test that load_codab calls the HDX API to download."""
    mocker.patch(
        "aatoolbox.datasources.iri.iri_seasonal_forecast."
        "_IriForecast._download"
    )

    geo_bounding_box = pipeline.load_geoboundingbox_coordinates(
        north=6, south=3.2, east=-2, west=3
    )
    pipeline.load_iri_forecast_probability(geo_bounding_box=geo_bounding_box)

    xr_load_dataset.assert_has_calls(
        [
            mocker.call(
                (
                    Path(FAKE_AA_DATA_DIR)
                    / f"private/raw/{ISO3}/{_MODULE_BASENAME}/"
                    f"abc_iri_forecast_seasonal_precipitation_"
                    f"tercile_prob_Np6Sp3Em2Wp3.nc"
                ),
                decode_times=False,
                drop_variables="C",
            ),
            mocker.call(
                Path(FAKE_AA_DATA_DIR)
                / f"private/processed/{ISO3}/{_MODULE_BASENAME}/"
                f"{ISO3}_iri_forecast_seasonal_precipitation_"
                f"tercile_prob_Np6Sp3Em2Wp3.nc"
            ),
        ]
    )
