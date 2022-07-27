"""Glofas focast and reforecast."""
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Union

import numpy as np
import xarray as xr
from dateutil import rrule

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.glofas import glofas
from aatoolbox.utils.check_file_existence import check_file_existence
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)


class _GlofasForecastBase(glofas.Glofas):
    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        leadtime_max: int,
        date_min: datetime,
        date_max: datetime,
        cds_name: str,
        system_version: str,
        product_type: Union[str, List[str]],
        date_variable_prefix: str,
        frequency: int,
    ):
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            date_min=date_min,
            date_max=date_max,
            cds_name=cds_name,
            system_version=system_version,
            product_type=product_type,
            date_variable_prefix=date_variable_prefix,
            frequency=frequency,
            leadtime_max=leadtime_max,
        )

    def process(  # type: ignore
        self,
        clobber: bool = False,
    ) -> List[Path]:
        logger.info(
            f"Processing GloFAS {self._forecast_type} for {self._date_min} - "
            f"{self._date_max} and up to {self._leadtime_max} day lead time"
        )

        # Get list of files to open
        processed_filepaths = []
        for date in self._date_range:
            input_filepath = self._get_filepath(
                year=date.year,
                month=date.month,
                day=date.day,
                leadtime_max=self._leadtime_max,
            )
            output_filepath = self._get_filepath(
                year=date.year,
                month=date.month,
                day=date.day,
                leadtime_max=self._leadtime_max,
                is_processed=True,
            )
            processed_filepath = self._process_single_file(
                input_filepath=input_filepath,
                filepath=output_filepath,
                clobber=clobber,
            )
            processed_filepaths.append(processed_filepath)
        return processed_filepaths

    @check_file_existence
    def _process_single_file(
        self, input_filepath: Path, filepath: Path, clobber: bool
    ) -> Path:
        # Read in both the control and ensemble perturbed forecast
        # and combine
        logger.debug(f"Reading in {input_filepath}")
        ds = self._read_in_ensemble_and_perturbed_datasets(
            filepath=input_filepath
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

    def _read_in_ensemble_and_perturbed_datasets(self, filepath: Path):
        ds_list = []
        for data_type in ["cf", "pf"]:
            ds = xr.load_dataset(
                filepath,
                engine="cfgrib",
                backend_kwargs={
                    "indexpath": "",
                    "filter_by_keys": {"dataType": data_type},
                },
            )
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
        return xr.combine_by_coords(ds_list)


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

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        leadtime_max: int,
        date_min: datetime = None,
        date_max: datetime = None,
    ):
        if date_min is None:
            date_min = datetime(year=2021, month=5, day=26)
        if date_max is None:
            date_max = datetime.utcnow()
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            leadtime_max=leadtime_max,
            date_min=date_min,
            date_max=date_max,
            cds_name="cems-glofas-forecast",
            system_version="operational",
            product_type=["control_forecast", "ensemble_perturbed_forecasts"],
            date_variable_prefix="",
            frequency=rrule.DAILY,
        )


class GlofasReforecast(_GlofasForecastBase):
    """GloFAS reforecast class."""

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        leadtime_max: int,
        date_min: datetime = None,
        date_max: datetime = None,
    ):
        if date_min is None:
            date_min = datetime(year=1999, month=1, day=1)
        if date_max is None:
            date_max = datetime(year=2018, month=12, day=31)
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            leadtime_max=leadtime_max,
            date_min=date_min,
            date_max=date_max,
            cds_name="cems-glofas-reforecast",
            system_version="version_3_1",
            product_type=[
                "control_reforecast",
                "ensemble_perturbed_reforecasts",
            ],
            date_variable_prefix="h",
            frequency=rrule.MONTHLY,
        )
