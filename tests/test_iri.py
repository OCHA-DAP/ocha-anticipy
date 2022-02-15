"""Tests for the IRI module."""

from pathlib import Path

import cftime
import numpy as np
import pytest
import xarray as xr
from xarray.coding.cftimeindex import CFTimeIndex

import aatoolbox.datasources.iri.iri_seasonal_forecast as aairi
from aatoolbox.datasources.iri.iri_seasonal_forecast import (
    _MODULE_BASENAME,
    IriForecastDominant,
    IriForecastProb,
)
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

FAKE_AA_DATA_DIR = "fake_aa_dir"
ISO3 = "abc"


@pytest.fixture(scope="session", autouse=True)
def mock_aa_data_dir(session_mocker):
    """Mock out the AA_DATA_DIR environment variable."""
    session_mocker.patch.dict(
        "aatoolbox.config.pathconfig.os.environ",
        {"AA_DATA_DIR": FAKE_AA_DATA_DIR},
    )


@pytest.fixture()
def mock_download(mocker):
    """
    Call download with mocked _download.

    `forecast_type` is the type of forecast to
    test, can be either 'prob' or 'dominant'.
    """
    download_mock = mocker.patch(
        "aatoolbox.datasources.iri."
        "iri_seasonal_forecast._IriForecast._download"
    )

    def _mock_download(prob_forecast):
        geo_bounding_box = GeoBoundingBox(north=6, south=3.2, east=-2, west=3)
        if prob_forecast:
            iri_class = IriForecastProb(
                iso3=ISO3, geo_bounding_box=geo_bounding_box
            )
        else:
            iri_class = IriForecastDominant(
                iso3=ISO3, geo_bounding_box=geo_bounding_box
            )
        iri_class.download(iri_auth=None)
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
        Path(FAKE_AA_DATA_DIR) / f"private/raw/{ISO3}/{_MODULE_BASENAME}/"
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
        Path(FAKE_AA_DATA_DIR) / f"private/raw/{ISO3}/{_MODULE_BASENAME}/"
        f"abc_iri_forecast_seasonal_"
        f"precipitation_tercile_dominant_Np6Sp3Em2Wp3.nc"
    )


def test_process(mocker):
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
    geo_bounding_box = GeoBoundingBox(north=6, south=3.2, east=-2, west=3)
    iri_class = IriForecastProb(iso3=ISO3, geo_bounding_box=geo_bounding_box)

    # TODO: now created `load_raw` to be able to mock but would like
    # to do it from xr.load_dataset directly
    # mocker.patch(
    #     "aatoolbox.datasources.iri.iri_seasonal_forecast."
    #     "_IriForecast.load_raw",
    #     return_value=ds,
    # )

    def side_effect(*args, **kwargs):
        side_effect.counter += 1
        if side_effect.counter == 1:
            return ds
        elif side_effect.counter == 2:
            return aairi.xr.load_dataset(*args, **kwargs)
            # return None

    side_effect.counter = 0
    mocker.patch(
        "aatoolbox.datasources.iri.iri_seasonal_forecast.xr.load_dataset",
        side_effect=side_effect,
    )
    processed_path = iri_class.process()
    assert processed_path == (
        Path(FAKE_AA_DATA_DIR)
        / f"private/processed/{ISO3}/{_MODULE_BASENAME}/"
        f"{ISO3}_iri_forecast_seasonal_precipitation_"
        f"tercile_prob_Np6Sp3Em2Wp3.nc"
    )

    da_processed = xr.load_dataset(processed_path)
    # Old method. Leaving here for reference but to be removed once fixed
    # mock_test=mocker.patch("aatoolbox.datasources.iri.iri_seasonal_forecast.xr.load_dataset",return_value=ds)
    # iri_class._process(filepath=tmp_path / "test.nc", ds=ds, clobber=False)
    # da_processed = xr.load_dataset(tmp_path / "test.nc")
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
