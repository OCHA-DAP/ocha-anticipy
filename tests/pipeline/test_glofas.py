"""Tests for GloFAS data download and processing."""
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from cdsapi import Client

from aatoolbox.datasources.glofas import glofas
from aatoolbox.datasources.glofas.forecast import (
    GlofasForecast,
    GlofasReforecast,
)
from aatoolbox.datasources.glofas.reanalysis import GlofasReanalysis
from aatoolbox.utils.geoboundingbox import GeoBoundingBox


def test_expand_dims():
    """Simple test case for expand dims."""
    rs = np.random.RandomState(12345)
    size_x, size_y = (10, 20)
    ds = xr.Dataset(
        data_vars={"var_a": (("x", "y"), rs.rand(size_x, size_y))},
        coords={"x": np.arange(size_x), "y": np.arange(size_y)},
    )
    ds.coords["z"] = 1
    assert "z" not in ds.dims.keys()
    ds = glofas.expand_dims(
        ds=ds,
        dataset_name="var_a",
        coord_names=["z", "x", "y"],
        expansion_dim=0,
    )
    assert "z" in ds.dims.keys()


class TestDownload:
    """Tests for GloFAS downloading."""

    country_iso3 = "abc"
    area = GeoBoundingBox(north=1, south=-2, east=3, west=-4)
    year = 2000
    leadtimes = [10, 20]
    expected_area = [1.05, -4.05, -2.05, 3.05]
    expected_months = [str(x + 1).zfill(2) for x in range(12)]
    expected_days = [str(x + 1).zfill(2) for x in range(31)]
    expected_leadtime = ["240", "480"]

    @pytest.fixture()
    def fake_retrieve(self, mocker):
        """Mock out the CDS API."""
        mocker.patch.object(Path, "mkdir", return_value=None)
        mocker.patch.object(Client, "__init__", return_value=None)
        return mocker.patch.object(Client, "retrieve")

    def test_reanalysis_download(self, fake_retrieve, mock_aa_data_dir):
        """
        Test GloFAS reanalysis download.

        Test that the query generated by the download method of GlofasReanlysis
        with default parameters is as expected
        """
        glofas_reanalysis = GlofasReanalysis(
            iso3=self.country_iso3, area=self.area
        )
        glofas_reanalysis.download(
            year_min=self.year,
            year_max=self.year,
        )
        expected_args = {
            "name": "cems-glofas-historical",
            "request": {
                "variable": "river_discharge_in_the_last_24_hours",
                "format": "grib",
                "dataset": ["consolidated_reanalysis"],
                "hyear": f"{self.year}",
                "hmonth": self.expected_months,
                "hday": self.expected_days,
                "area": self.expected_area,
                "system_version": "version_3_1",
                "hydrological_model": "lisflood",
            },
            "target": Path(
                f"{mock_aa_data_dir}/public/raw/{self.country_iso3}"
                "/glofas/version_3/cems-glofas-historical"
                f"/{self.country_iso3}_cems-glofas-historical_v3_2000.grib"
            ),
        }
        fake_retrieve.assert_called_with(**expected_args)

    def test_forecast_download(self, fake_retrieve, mock_aa_data_dir):
        """
        Test GloFAS forecast download.

        Test that the query generated by the download method of GlofasForecast
        with default parameters is as expected
        """
        glofas_forecast = GlofasForecast(
            iso3=self.country_iso3, area=self.area
        )
        glofas_forecast.download(
            leadtimes=self.leadtimes,
            year_min=self.year,
            year_max=self.year,
        )
        expected_args = {
            "name": "cems-glofas-forecast",
            "request": {
                "variable": "river_discharge_in_the_last_24_hours",
                "format": "grib",
                "product_type": [
                    "control_forecast",
                    "ensemble_perturbed_forecasts",
                ],
                "year": f"{self.year}",
                "month": self.expected_months,
                "day": self.expected_days,
                "area": self.expected_area,
                "system_version": "version_3_1",
                "hydrological_model": "lisflood",
                "leadtime_hour": self.expected_leadtime,
            },
            "target": Path(
                f"{mock_aa_data_dir}/public/raw/{self.country_iso3}"
                f"/glofas/version_3/cems-glofas-forecast"
                f"/{self.country_iso3}_cems-glofas-forecast_v3_2000.grib"
            ),
        }
        fake_retrieve.assert_called_with(**expected_args)

    def get_reforecast_expected_args(self, mock_aa_data_dir):
        """
        Get GloFAS reforecast expected args.

        Because there are two different tests of the reforecast,
        this method provides the expected query output for both
        """
        return {
            "name": "cems-glofas-reforecast",
            "request": {
                "variable": "river_discharge_in_the_last_24_hours",
                "format": "grib",
                "product_type": [
                    "control_reforecast",
                    "ensemble_perturbed_reforecasts",
                ],
                "hyear": f"{self.year}",
                "hmonth": self.expected_months,
                "hday": self.expected_days,
                "area": self.expected_area,
                "system_version": "version_3_1",
                "hydrological_model": "lisflood",
                "leadtime_hour": self.expected_leadtime,
            },
            "target": Path(
                f"{mock_aa_data_dir}/public/raw/{self.country_iso3}"
                f"/glofas/version_3/cems-glofas-reforecast"
                f"/{self.country_iso3}_cems-glofas-reforecast_v3_2000.grib"
            ),
        }

    def test_reforecast_download(self, fake_retrieve, mock_aa_data_dir):
        """
        Test GloFAS reforecast download.

        Test that the query generated by the download method of
        GlofasReforecast with default parameters is as expected
        """
        glofas_reforecast = GlofasReforecast(
            iso3=self.country_iso3, area=self.area
        )
        glofas_reforecast.download(
            leadtimes=self.leadtimes,
            year_min=self.year,
            year_max=self.year,
        )
        fake_retrieve.assert_called_with(
            **self.get_reforecast_expected_args(mock_aa_data_dir)
        )

    def test_reforecast_download_split_by_leadtime(
        self, fake_retrieve, mock_aa_data_dir
    ):
        """
        Test GloFAS reforecast download leadtime splitting.

        Test that the query generated by the download method of
        GlofasReforecast with leadtime splitting is as expected.
        """
        glofas_reforecast = GlofasReforecast(
            iso3=self.country_iso3, area=self.area
        )
        glofas_reforecast.download(
            leadtimes=self.leadtimes[:1],
            year_min=self.year,
            year_max=self.year,
            split_by_leadtimes=True,
        )
        expected_args = self.get_reforecast_expected_args(mock_aa_data_dir)
        expected_args["request"]["leadtime_hour"] = self.expected_leadtime[:1]
        expected_args["target"] = Path(
            f"{mock_aa_data_dir}/public/raw/{self.country_iso3}"
            f"/glofas/version_3/cems-glofas-reforecast"
            f"/{self.country_iso3}_cems-glofas-reforecast_v3_2000_lt10d.grib"
        )
        fake_retrieve.assert_called_with(**expected_args)
