"""Tests for the IRI module."""

import cftime
import numpy as np
import pytest
import xarray as xr
from xarray.coding.cftimeindex import CFTimeIndex

from aatoolbox.datasources.iri.iri_seasonal_forecast import (
    IriForecastDominant,
    IriForecastTercile,
)
from aatoolbox.utils.geoboundingbox import GeoBoundingBox


@pytest.fixture()
def mock_download(mocker, tmp_path):
    """
    Call download with mocked _download.

    `forecast_type` is the type of forecast to
    test, can be either 'tercile' or 'dominant'.
    """
    download_mock = mocker.patch(
        "aatoolbox.datasources.iri."
        "iri_seasonal_forecast._IriForecast._download"
    )

    def _mock_download(forecast_type):
        iso3 = "abc"
        geo_bounding_box = GeoBoundingBox(north=6, south=3.2, east=-2, west=3)
        if forecast_type == "tercile":
            iri_class = IriForecastTercile(
                iso3=iso3, geo_bounding_box=geo_bounding_box
            )
        elif forecast_type == "dominant":
            iri_class = IriForecastDominant(
                iso3=iso3, geo_bounding_box=geo_bounding_box
            )
        iri_class._raw_base_dir = tmp_path
        iri_class.download(iri_auth=None)
        _, kwargs_download = download_mock.call_args
        url = kwargs_download["url"]
        filepath = kwargs_download["filepath"]
        return url, filepath, tmp_path

    return _mock_download


def test_download_call_tercile(mock_download):
    """Test download for tercile forecast."""
    url, filepath, tmp_path = mock_download("tercile")
    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
        ".NMME_Seasonal_Forecast/"
        ".Precipitation_ELR/.prob/X/%283.0%29%28-2.0%29RANGEEDGES/"
        "Y/%286.0%29%283.0%29RANGEEDGES/data.nc"
    )

    assert (
        filepath
        == tmp_path
        / "abc_iri_forecast_seasonal_precipitation_tercile_Np6Sp3Em2Wp3.nc"
    )


def test_download_call_dominant(mock_download):
    """Test download for tercile forecast."""
    url, filepath, tmp_path = mock_download("dominant")
    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
        ".NMME_Seasonal_Forecast/"
        ".Precipitation_ELR/.dominant/X/%283.0%29%28-2.0%29RANGEEDGES/"
        "Y/%286.0%29%283.0%29RANGEEDGES/data.nc"
    )

    assert (
        filepath == tmp_path / "abc_iri_forecast_seasonal_precipitation_"
        "tercile_dominant_Np6Sp3Em2Wp3.nc"
    )


def test_process(tmp_path):
    """Test process for IRI forecast."""
    ds = xr.DataArray(
        np.reshape(a=np.arange(16), newshape=(2, 2, 2, 2)),
        coords=[
            [1, 2],
            [3, -2],
            [97, 90],
            [685.5, 686.5],
        ],
        dims=["L", "X", "Y", "F"],
    ).to_dataset(name="prob")
    ds["F"].attrs["calendar"] = "360"
    ds["F"].attrs["units"] = "months since 1960-01-01"
    iso3 = "abc"
    geo_bounding_box = GeoBoundingBox(north=6, south=3.2, east=-2, west=3)
    iri_class = IriForecastTercile(
        iso3=iso3, geo_bounding_box=geo_bounding_box
    )

    iri_class._process(filepath=tmp_path / "test.nc", ds=ds, clobber=False)
    da_processed = xr.load_dataset(tmp_path / "test.nc")
    expected_f = CFTimeIndex(
        [
            cftime.datetime(year=2017, month=2, day=16, calendar="360_day"),
            cftime.datetime(year=2017, month=3, day=16, calendar="360_day"),
        ]
    )
    # TODO: this can be done in a neater way but couldn't figure out how
    expected_values = [
        [[[4, 5], [6, 7]], [[0, 1], [2, 3]]],
        [[[12, 13], [14, 15]], [[8, 9], [10, 11]]],
    ]
    assert np.array_equal(da_processed.X.values, [-2, 3])
    assert np.array_equal(da_processed.Y.values, [97, 90])
    assert da_processed.get_index("F").equals(expected_f)
    assert np.array_equal(da_processed.prob.values, expected_values)
