"""Glofas focast and reforecast."""
import datetime
import logging
from typing import List

from aatoolbox.datasources.glofas import glofas

logger = logging.getLogger(__name__)


class _GlofasForecastBase(glofas.Glofas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _download_forecast(
        self,
        is_reforecast: bool,
        leadtime_max: int,
        split_by_month: bool = False,
        year_min: int = None,
        year_max: int = None,
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
                super()._download(
                    year=year,
                    month=month,
                    leadtime_max=leadtime_max,
                )

    def _process(
        self,
        is_reforecast: bool,
        leadtime_max: int,
        split_by_month: bool = False,
        year_min: int = None,
        year_max: int = None,
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
        filepath_list = [
            self._get_raw_filepath(
                year=year,
                month=month,
                leadtime_max=leadtime_max,
            )
            for year in range(year_min, year_max + 1)
            for month in month_range
        ]
        # Read in both the control and ensemble perturbed forecast
        # and combine
        logger.info(f"Reading in {len(filepath_list)} files")
        ds = self._read_in_ensemble_and_perturbed_datasets(
            filepath_list=filepath_list
        )
        # Create a new dataset with just the station pixels
        logger.info("Looping through reporting_points, this takes some time")
        coord_names = ["number", "time", "step"]
        ds_new = self._get_reporting_point_dataset(
            ds=ds,
            coord_names=coord_names,
        )
        # Write out the new dataset to a file
        return self._write_to_processed_file(
            ds=ds_new,
            leadtime_max=leadtime_max,
        )


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
            dataset=["control_forecast", "ensemble_perturbed_forecasts"],
            dataset_variable_name="product_type",
        )

    def download(self, *args, **kwargs):
        """
        Download GloFAS reforecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        super()._download_forecast(is_reforecast=False, *args, **kwargs)

    def process(self, *args, **kwargs):
        """
        Process GloFAS reforecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        return super()._process(is_reforecast=False, *args, **kwargs)


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
            dataset=["control_reforecast", "ensemble_perturbed_reforecasts"],
            dataset_variable_name="product_type",
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
        super()._download_forecast(is_reforecast=True, *args, **kwargs)

    def process(self, *args, **kwargs):
        """
        Process GloFAS forecast.

        Parameters
        ----------
        args :
        kwargs :
        """
        return super()._process(is_reforecast=True, *args, **kwargs)
