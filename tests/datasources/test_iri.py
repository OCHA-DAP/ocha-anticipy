"""Tests for the IRI module."""

from pathlib import Path

import cftime
import numpy as np
import pytest
import xarray as xr

# TODO: this will break if there is another conftest
from conftest import FAKE_AA_DATA_DIR, ISO3
from xarray.coding.cftimeindex import CFTimeIndex

from aatoolbox import GeoBoundingBox, IriForecastDominant, IriForecastProb

MODULE_BASENAME = "iri"
FAKE_IRI_AUTH = "FAKE_IRI_AUTH"


@pytest.fixture
def mock_iri(mock_country_config):
    """Create IRI class with mock country config."""
    geo_bounding_box = GeoBoundingBox(north=6, south=3.2, east=-2, west=3)

    def _mock_iri(prob_forecast: bool = True):
        if prob_forecast:
            iri = IriForecastProb(
                country_config=mock_country_config,
                geo_bounding_box=geo_bounding_box,
            )
        else:
            iri = IriForecastDominant(
                country_config=mock_country_config,
                geo_bounding_box=geo_bounding_box,
            )
        return iri

    return _mock_iri


@pytest.fixture
def mock_download(mocker, mock_iri):
    """
    Call download with mocked _download.

    `forecast_type` is the type of forecast to
    test, can be either 'prob' or 'dominant'.
    """
    download_mock = mocker.patch(
        "aatoolbox.datasources.iri.iri_seasonal_forecast._download"
    )

    mocker.patch.dict(
        "aatoolbox.datasources.iri.iri_seasonal_forecast.os.environ",
        {"IRI_AUTH": FAKE_IRI_AUTH},
    )

    def _mock_download(prob_forecast: bool):
        iri = mock_iri(prob_forecast=prob_forecast)
        iri.download()
        _, kwargs_download = download_mock.call_args
        url = kwargs_download["url"]
        filepath = kwargs_download["filepath"]
        return url, filepath

    return _mock_download


def test_download_call_prob(mock_download):
    """Test download for tercile probability forecast."""
    url, filepath = mock_download(prob_forecast=True)
    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
        ".NMME_Seasonal_Forecast/"
        ".Precipitation_ELR/.prob/X/%283.0%29%28-2.0%29RANGEEDGES/"
        "Y/%286.0%29%283.0%29RANGEEDGES/data.nc"
    )

    assert filepath == (
        Path(FAKE_AA_DATA_DIR) / f"private/raw/{ISO3}/{MODULE_BASENAME}/"
        f"abc_iri_forecast_seasonal_precipitation_"
        f"tercile_prob_Np6Sp3Em2Wp3.nc"
    )


def test_download_call_dominant(mock_download):
    """Test download for dominant tercile forecast."""
    url, filepath = mock_download(prob_forecast=False)
    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
        ".NMME_Seasonal_Forecast/"
        ".Precipitation_ELR/.dominant/X/%283.0%29%28-2.0%29RANGEEDGES/"
        "Y/%286.0%29%283.0%29RANGEEDGES/data.nc"
    )

    assert filepath == (
        Path(FAKE_AA_DATA_DIR) / f"private/raw/{ISO3}/{MODULE_BASENAME}/"
        f"abc_iri_forecast_seasonal_"
        f"precipitation_tercile_dominant_Np6Sp3Em2Wp3.nc"
    )


# TODO: need to use a tmp dir but will copy
# that from glofas once that is ready :)
def test_process(mocker, mock_iri):
    """Test process for IRI forecast."""
    ds = xr.DataArray(
        np.reshape(a=np.arange(16), newshape=(2, 2, 2, 2)),
        dims=("L", "X", "Y", "F"),
        coords={
            "L": [1, 2],
            "X": [3, -2],
            "Y": [97, 90],
            "F": [685.5, 686.5],
        },
    ).to_dataset(name="prob")

    ds["F"].attrs["calendar"] = "360"
    ds["F"].attrs["units"] = "months since 1960-01-01"

    iri = mock_iri()

    # TODO: now created `load_raw` to be able to mock but would like
    #  to do it from xr.load_dataset directly
    mocker.patch(
        "aatoolbox.datasources.iri.iri_seasonal_forecast."
        "_IriForecast._load_raw",
        return_value=ds,
    )

    processed_path = iri.process()
    assert processed_path == (
        Path(FAKE_AA_DATA_DIR) / f"private/processed/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_iri_forecast_seasonal_precipitation_"
        f"tercile_prob_Np6Sp3Em2Wp3.nc"
    )

    da_processed = xr.load_dataset(processed_path)
    expected_f = CFTimeIndex(
        [
            cftime.datetime(year=2017, month=2, day=16, calendar="360_day"),
            cftime.datetime(year=2017, month=3, day=16, calendar="360_day"),
        ]
    )

    expected_values = [
        [[[4, 5], [6, 7]], [[0, 1], [2, 3]]],
        [[[12, 13], [14, 15]], [[8, 9], [10, 11]]],
    ]
    assert np.array_equal(da_processed.X.values, [-2, 3])
    assert np.array_equal(da_processed.Y.values, [97, 90])
    assert da_processed.get_index("F").equals(expected_f)
    assert np.array_equal(da_processed.prob.values, expected_values)


def test_process_if_download_not_called(mock_iri):
    """Test that correct error message raised."""
    iri = mock_iri()
    # Make sure file doesn't exist
    Path.unlink(iri._get_raw_path(), missing_ok=True)
    with pytest.raises(FileNotFoundError) as excinfo:
        iri.process()
    assert (
        "Make sure that you have already called the 'download' method"
        in str(excinfo.value)
    )


@pytest.fixture
def mock_xr_load_dataset(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch(
        "aatoolbox.datasources.iri.iri_seasonal_forecast.xr.load_dataset"
    )


def test_iri_load(mocker, mock_xr_load_dataset, mock_iri):
    """Test that load_codab calls the HDX API to download."""
    mocker.patch("aatoolbox.datasources.iri.iri_seasonal_forecast._download")

    iri = mock_iri()
    iri.load()
    mock_xr_load_dataset.assert_has_calls(
        [
            mocker.call(
                Path(FAKE_AA_DATA_DIR)
                / f"private/processed/{ISO3}/{MODULE_BASENAME}/"
                f"{ISO3}_iri_forecast_seasonal_precipitation_"
                f"tercile_prob_Np6Sp3Em2Wp3.nc"
            ),
        ]
    )


def test_load_if_process_not_called(mock_iri):
    """Test that correct error message raised."""
    iri = mock_iri()
    # Make sure file doesn't exist
    Path.unlink(iri._get_processed_path(), missing_ok=True)
    with pytest.raises(FileNotFoundError) as excinfo:
        iri.load()
    assert (
        "Make sure that you have already called the 'process' method"
        in str(excinfo.value)
    )
