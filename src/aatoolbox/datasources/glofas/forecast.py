"""Glofas focast and reforecast."""
import datetime
import logging
from pathlib import Path
from typing import List

import numpy as np
import xarray as xr

from aatoolbox.datasources.glofas import glofas
from aatoolbox.utils.check_file_existence import check_file_existence

logger = logging.getLogger(__name__)


class _GlofasForecastBase(glofas.Glofas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def download(  # type: ignore
        self,
        is_reforecast: bool,
        leadtime_max: int,
        split_by_month: bool = True,
        year_min: int = None,
        year_max: int = None,
        clobber: bool = False,
    ):
        forecast_type = "reforecast" if is_reforecast else "forecast"
        year_min = self._year_min if year_min is None else year_min
        year_max = self._year_max if year_max is None else year_max
        month_range: List = [*range(1, 13)] if split_by_month else [None]
        logger.info(
            f"Downloading GloFAS {forecast_type} for years"
            f" {year_min} - {year_max} and with max lead time {leadtime_max}"
        )
        for year in range(year_min, year_max + 1):
            logger.info(f"...{year}")
            for month in month_range:
                logger.debug(f"...{month}")
                filepath = self._get_raw_filepath(
                    year=year,
                    month=month,
                    leadtime_max=leadtime_max,
                )
                super()._download(
                    filepath=filepath,
                    year=year,
                    month=month,
                    leadtime_max=leadtime_max,
                    clobber=clobber,
                )

    def process(  # type: ignore
        self,
        is_reforecast: bool,
        leadtime_max: int,
        split_by_month: bool = True,
        year_min: int = None,
        year_max: int = None,
        clobber: bool = False,
    ):
        forecast_type = "reforecast" if is_reforecast else "forecast"
        year_min = self._year_min if year_min is None else year_min
        year_max = self._year_max if year_max is None else year_max

        logger.info(
            f"Processing GloFAS {forecast_type} for years"
            f" {year_min} - {year_max} and max lead time {leadtime_max}"
        )

        month_range = [*range(1, 13)] if split_by_month else [None]
        # Get list of files to open
        input_filepath_list = [
            self._get_raw_filepath(
                year=year,
                month=month,
                leadtime_max=leadtime_max,
            )
            for year in range(year_min, year_max + 1)
            for month in month_range
        ]
        filepath = self._get_processed_filepath(leadtime_max=leadtime_max)

        return self._process(
            filepath=filepath,
            input_filepath_list=input_filepath_list,
            clobber=clobber,
        )

    @check_file_existence
    def _process(
        self, filepath: Path, input_filepath_list: List[Path], clobber: bool
    ) -> Path:
        # Read in both the control and ensemble perturbed forecast
        # and combine
        logger.info(f"Reading in {len(input_filepath_list)} files")
        ds = self._read_in_ensemble_and_perturbed_datasets(
            filepath_list=input_filepath_list
        )
        # Create a new product_type with just the station pixels
        logger.info("Looping through reporting_points, this takes some time")
        coord_names = ["number", "time", "step"]
        ds_new = self._get_reporting_point_dataset(
            ds=ds,
            coord_names=coord_names,
        )
        # Write out the new product_type to a file
        return self._write_to_processed_file(ds=ds_new, filepath=filepath)

    def _read_in_ensemble_and_perturbed_datasets(
        self, filepath_list: List[Path]
    ):
        ds_list = []
        for data_type in ["cf", "pf"]:
            with xr.open_mfdataset(
                filepath_list,
                engine="cfgrib",
                backend_kwargs={
                    "indexpath": "",
                    "filter_by_keys": {"dataType": data_type},
                },
            ) as ds:
                # Delete history attribute in order to merge
                del ds.attrs["history"]
                # Extra processing require for control forecast
                if data_type == "cf":
                    ds = _expand_dims(
                        ds=ds,
                        dataset_name=self._RIVER_DISCHARGE_VAR,
                        coord_names=[
                            "number",
                            "time",
                            "step",
                            "latitude",
                            "longitude",
                        ],
                        expansion_dim=0,
                    )
                ds_list.append(ds)
        ds = xr.combine_by_coords(ds_list)
        return ds


# TODO: see if this is fixed
def _expand_dims(
    ds: xr.Dataset, dataset_name: str, coord_names: list, expansion_dim: int
):
    """Expand dims to combine two datasets.

    Using expand_dims seems to cause a bug with Dask like the one
    described here: https://github.com/pydata/xarray/issues/873 (it's
    supposed to be fixed though)
    """
    coords = {coord_name: ds[coord_name] for coord_name in coord_names}
    coords[coord_names[expansion_dim]] = [coords[coord_names[expansion_dim]]]
    ds = xr.Dataset(
        data_vars={
            dataset_name: (
                coord_names,
                np.expand_dims(ds[dataset_name].values, expansion_dim),
            )
        },
        coords=coords,
    )
    return ds


class GlofasForecast(_GlofasForecastBase):
    """GloFAS forecast class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            year_min=2020,
            year_max=datetime.datetime.now().year,
            cds_name="cems-glofas-forecast",
            system_version="operational",
            product_type=["control_forecast", "ensemble_perturbed_forecasts"],
        )

    def download(self, *args, **kwargs):
        """
        Download GloFAS reforecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        super().download(is_reforecast=False, *args, **kwargs)

    def process(self, *args, **kwargs):
        """
        Process GloFAS reforecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        return super().process(is_reforecast=False, *args, **kwargs)


class GlofasReforecast(_GlofasForecastBase):
    """GloFAS reforecast class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            year_min=1999,
            year_max=2018,
            cds_name="cems-glofas-reforecast",
            system_version="version_3_1",
            product_type=[
                "control_reforecast",
                "ensemble_perturbed_reforecasts",
            ],
            date_variable_prefix="h",
        )

    def download(self, *args, **kwargs):
        """
        Download GloFAS forecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        super().download(is_reforecast=True, *args, **kwargs)

    def process(self, *args, **kwargs):
        """
        Process GloFAS forecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        return super().process(is_reforecast=True, *args, **kwargs)
