"""Tests for the USGS NDVI module."""

from datetime import date
from unittest.mock import patch

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import Polygon

from aatoolbox import (
    UsgsNdviMedianAnomaly,
    UsgsNdviPctMedian,
    UsgsNdviSmoothed,
    UsgsNdviYearDifference,
)
from aatoolbox.utils._dates import dekad_to_date

DATASOURCE_BASE_DIR = "usgs_ndvi"
patcher = patch("aatoolbox.datasources.usgs.usgs_ndvi.urlopen")


@pytest.fixture
def mock_ndvi(mock_country_config):
    """Create USGS NDVI class with mock country config."""
    start_date = (2019, 36)
    end_date = (2020, 2)

    instantiator = {
        "smoothed": UsgsNdviSmoothed,
        "pct_median": UsgsNdviPctMedian,
        "anomaly": UsgsNdviMedianAnomaly,
        "difference": UsgsNdviYearDifference,
    }

    def _mock_ndvi(variable: str = "smoothed"):

        ndvi = instantiator[variable](
            country_config=mock_country_config,
            start_date=start_date,
            end_date=end_date,
        )
        return ndvi

    return _mock_ndvi


@pytest.fixture
def mock_download(mocker, mock_ndvi):
    """
    Call download with mocked download.

    Return number of calls to the internal
    download method and the returned filepath.
    """
    download_mock = mocker.patch(
        (
            "aatoolbox.datasources.usgs."
            "usgs_ndvi._UsgsNdvi._download_ndvi_dekad"
        )
    )

    def _mock_download(variable: str = "smoothed"):
        ndvi = mock_ndvi(variable=variable)
        fp = ndvi.download()
        download_calls = download_mock.call_count
        return fp, download_calls

    return _mock_download


@pytest.fixture
def gdf():
    """Create geodataframe for testing."""
    d = {
        "name": ["area_a", "area_b"],
        "geometry": [
            Polygon([(0, 0), (0, 2), (2, 2), (2, 0)]),
            Polygon([(2, 0), (2, 2), (3, 2), (3, 0)]),
        ],
    }
    return gpd.GeoDataFrame(d)


@pytest.fixture
def da():
    """
    Create raster input with date coordinate.

    Used as side effect later to create 3
    consecutive raster values for processing.
    """

    def _da(year_dekad):
        date = dekad_to_date(year_dekad)
        da = (
            xr.DataArray(
                [[[1 + year_dekad[1], 2, 3], [4, 5 + year_dekad[1], 6]]],
                dims=("date", "y", "x"),
                coords={
                    "date": [date],
                    "year": year_dekad[0],
                    "dekad": year_dekad[1],
                    "y": [1.5, 0.5],
                    "x": [0.5, 1.5, 2.5],
                },
            )
            .assign_coords({"modified": 1})
            .rio.write_crs("EPSG:4326")
        )
        return da

    return _da


def test_download_smoothed(mock_download, mock_aa_data_dir):
    """Test download for all NDVI variables."""
    fp, call_count = mock_download(variable="smoothed")

    assert fp == (mock_aa_data_dir / f"public/raw/glb/{DATASOURCE_BASE_DIR}")
    assert call_count == 3


def test_download_pct_median(mock_download, mock_aa_data_dir):
    """Test download for all NDVI variables."""
    fp, call_count = mock_download(variable="pct_median")

    assert fp == (mock_aa_data_dir / f"public/raw/glb/{DATASOURCE_BASE_DIR}")

    assert call_count


def test_download_anomaly(mock_download, mock_aa_data_dir):
    """Test download for all NDVI variables."""
    fp, call_count = mock_download(variable="anomaly")

    assert fp == (mock_aa_data_dir / f"public/raw/glb/{DATASOURCE_BASE_DIR}")

    assert call_count


def test_download_difference(mock_download, mock_aa_data_dir):
    """Test download for all NDVI variables."""
    fp, call_count = mock_download(variable="difference")

    assert fp == (mock_aa_data_dir / f"public/raw/glb/{DATASOURCE_BASE_DIR}")

    assert call_count


def test_process_and_load(
    mocker, mock_ndvi, mock_aa_data_dir, mock_country_config, gdf, da
):
    """Test processing NDVI values."""
    ndvi = mock_ndvi(variable="smoothed")

    # TODO: now created `load_raw` to be able to mock but would like
    #  to do it from xr.load_dataset directly
    mocker.patch(
        "aatoolbox.datasources.usgs.usgs_ndvi._UsgsNdvi.load_raster",
        side_effect=[da((2019, 36)), da((2020, 1)), da((2020, 2))] * 6,
    )

    processed_dir = ndvi.process(gdf=gdf, feature_col="name")
    assert processed_dir == (
        mock_aa_data_dir / f"public/processed/{mock_country_config.iso3}/"
        f"{DATASOURCE_BASE_DIR}"
    )

    df_processed = ndvi.load(feature_col="name")

    expected_dates = [
        date(year=2019, month=12, day=21),
        date(year=2020, month=1, day=1),
        date(year=2020, month=1, day=11),
    ]

    expected_dates = [d for ds in expected_dates for d in [ds] * 2]

    assert np.array_equal(df_processed.name.values, ["area_a", "area_b"] * 3)
    assert np.array_equal(
        pd.to_datetime(df_processed.date).dt.date, expected_dates
    )
    assert np.array_equal(df_processed.dekad.values, [36, 36, 1, 1, 2, 2])
    assert np.array_equal(df_processed.loc[:, "count"].values, [4, 2] * 3)
    assert np.array_equal(
        df_processed.loc[:, "min"].values, [2, 3, 2, 3, 2, 3]
    )
    assert np.allclose(
        df_processed.loc[:, "std"].values,
        [18.069311, 1.5, 1.6583124, 1.5, 1.87082869, 1.5],
    )
    assert np.array_equal(
        df_processed.loc[:, "mean"].values, [21.0, 4.5, 3.5, 4.5, 4.0, 4.5]
    )
    assert np.array_equal(
        df_processed.loc[:, "max"].values, [41, 6, 6, 6, 7, 6]
    )


def test_process_if_download_not_called(mock_ndvi, gdf):
    """Test that correct error message raised."""
    ndvi = mock_ndvi(variable="smoothed")
    with pytest.raises(FileNotFoundError):
        ndvi.process(gdf=gdf, feature_col="name")


def test_load_if_process_not_called(mock_ndvi):
    """Test that correct error message raised."""
    ndvi = mock_ndvi(variable="smoothed")
    with pytest.raises(FileNotFoundError):
        ndvi.load(feature_col="name")
