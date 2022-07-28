"""Tests for GloFAS processing."""
from datetime import datetime
from typing import List, Union

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.glofas.forecast import GlofasReforecast
from aatoolbox.datasources.glofas.reanalysis import GlofasReanalysis
from aatoolbox.utils.geoboundingbox import GeoBoundingBox


class TestProcess:
    """Tests for GloFAS processing."""

    geo_bounding_box = GeoBoundingBox(
        lat_max=1, lat_min=-2, lon_max=3, lon_min=-4
    )
    numbers = [0, 1, 2, 3, 4, 5, 6]

    def get_raw_data(
        self,
        number_coord: Union[List[int], int] = None,
        include_step: bool = False,
        include_history: bool = False,
        dis24: np.ndarray = None,
    ) -> xr.Dataset:
        """
        Construct a simple fake GloFAS xarray dataset.

        Parameters
        ----------
        number_coord : list or int, default = None
            The ensemble number coordinate
        include_step :  bool, default = False
            Whether to include the forecast step coordinate
        include_history : bool, default = False
            Whether to include the history attribute
        dis24 : np.ndarray, default = None
            Optional array of discharge values, that should have the combined
            dimensions of the coordinates. If not passed, generated using
            random numbers.

        Returns
        -------
        Simplified GloFAS xarray dataset
        """
        rng = np.random.default_rng(12345)
        coords = {}
        if number_coord is not None:
            coords["number"] = number_coord
        coords["time"] = pd.date_range("2014-09-06", periods=2)
        if include_step:
            coords["step"] = [np.datetime64(n + 1, "D") for n in range(5)]
        coords["latitude"] = (
            np.arange(
                start=self.geo_bounding_box.lat_min,
                stop=self.geo_bounding_box.lat_max + 2,
                step=0.1,
            )
            - 0.05
        )
        coords["longitude"] = (
            np.arange(
                start=self.geo_bounding_box.lon_min,
                stop=self.geo_bounding_box.lon_max + 2,
                step=0.1,
            )
            - 0.05
        )
        dims = list(coords.keys())
        if number_coord is not None and isinstance(number_coord, int):
            dims = dims[1:]
        if dis24 is None:
            dis24 = 5000 + 100 * rng.random([len(coords[dim]) for dim in dims])
        attrs = {}
        if include_history:
            attrs = {"history": "fake history"}
        return xr.Dataset({"dis24": (dims, dis24)}, coords=coords, attrs=attrs)

    @pytest.fixture()
    def mock_ensemble_raw(self) -> (xr.Dataset, xr.Dataset, np.ndarray):
        """
        Create fake raw ensemble data.

        For the forecast and reforecast, generate the raw data, which consists
        of the control and perturbed forecast, and combine the discharge
        values for the processed data. Return both xarray datasets and the
        combined array.
        """
        cf_raw = self.get_raw_data(
            number_coord=self.numbers[0],
            include_step=True,
            include_history=True,
        )
        pf_raw = self.get_raw_data(
            number_coord=self.numbers[1:],
            include_step=True,
            include_history=True,
        )
        expected_dis24 = np.concatenate(
            (cf_raw["dis24"].values[np.newaxis, ...], pf_raw["dis24"].values)
        )
        return cf_raw, pf_raw, expected_dis24

    def get_processed_data(
        self,
        country_config: CountryConfig,
        number_coord: [List[int], int] = None,
        include_step: bool = False,
        dis24: np.ndarray = None,
    ) -> xr.Dataset:
        """
        Create a simplified fake processed GloFAS dataset.

        Parameters
        ----------
        country_config : CountryConfig
            Country configuration object
        number_coord : list or int, default = None
            The ensemble number coordinate
        include_step : bool, default = False
            Whether to include the forecast step coordinate
        dis24 : np.ndarray, default = None
            Optional array of discharge values, that should have the combined
            dimensions of the coordinates. If not passed, generated using
            random numbers.

        Returns
        -------
        GloFAS processed xarray dataset
        """
        raw_data = self.get_raw_data(
            number_coord=number_coord, include_step=include_step, dis24=dis24
        )
        coords = {}
        if number_coord is not None:
            coords = {"number": number_coord}
        coords["time"] = raw_data.time
        if include_step:
            coords["step"] = raw_data.step
        return xr.Dataset(
            {
                reporting_point.id: (
                    list(coords.keys()),
                    raw_data["dis24"]
                    .sel(
                        longitude=reporting_point.lon,
                        latitude=reporting_point.lat,
                        method="nearest",
                    )
                    .data,
                    {"name": reporting_point.name},
                )
                for reporting_point in country_config.glofas.reporting_points
            },
            coords=coords,
        )

    @pytest.fixture()
    def mock_processed_data_reanalysis(
        self, mocker, mock_country_config
    ) -> xr.Dataset:
        """Create fake processed GloFAS reanalysis data."""
        mocker.patch(
            "aatoolbox.datasources.glofas.reanalysis.xr.load_dataset",
            return_value=self.get_raw_data(),
        )
        return self.get_processed_data(country_config=mock_country_config)

    @pytest.fixture()
    def mock_processed_data_forecast(
        self, mock_ensemble_raw, mocker, mock_country_config
    ):
        """Create fake processed GloFAS forecast or reforecast data."""
        cf_raw, pf_raw, expected_dis24 = mock_ensemble_raw
        mocker.patch(
            "aatoolbox.datasources.glofas.forecast.xr.load_dataset",
            side_effect=[cf_raw, pf_raw],
        )
        return self.get_processed_data(
            country_config=mock_country_config,
            number_coord=self.numbers,
            include_step=True,
            dis24=mock_ensemble_raw[2],
        )

    def test_reanalysis_process(
        self, mock_country_config, mock_processed_data_reanalysis
    ):
        """Test GloFAS reanalysis process method."""
        glofas_reanalysis = GlofasReanalysis(
            country_config=mock_country_config,
            geo_bounding_box=self.geo_bounding_box,
            date_min=datetime(year=2022, month=1, day=1),
            date_max=datetime(year=2022, month=12, day=31),
        )
        output_filepath = glofas_reanalysis.process()[0]
        # use open_dataset since load_dataset is patched
        with xr.open_dataset(output_filepath) as output_ds:
            assert output_ds.equals(mock_processed_data_reanalysis)

    def test_reforecast_process(
        self, mock_country_config, mock_processed_data_forecast
    ):
        """Test GloFAS reforecast process method."""
        glofas_reforecast = GlofasReforecast(
            country_config=mock_country_config,
            geo_bounding_box=self.geo_bounding_box,
            leadtime_max=3,
            date_min=datetime(year=2022, month=1, day=1),
            date_max=datetime(year=2022, month=1, day=31),
        )
        output_filepath = glofas_reforecast.process()[0]
        with xr.open_dataset(output_filepath) as output_ds:
            assert output_ds.equals(mock_processed_data_forecast)

    def test_forecast_process(
        self, mock_country_config, mock_processed_data_forecast
    ):
        """Test GloFAS forecast process method."""
        glofas_forecast = GlofasReforecast(
            country_config=mock_country_config,
            geo_bounding_box=self.geo_bounding_box,
            leadtime_max=3,
            date_min=datetime(year=2022, month=1, day=1),
            date_max=datetime(year=2022, month=1, day=1),
        )
        output_filepath = glofas_forecast.process()[0]
        with xr.open_dataset(output_filepath) as output_ds:
            assert output_ds.equals(mock_processed_data_forecast)
