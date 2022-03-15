"""Glofas focast and reforecast."""
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
        leadtimes: List[int],
        version: int = glofas.VERSION,
        split_by_month: bool = False,
        split_by_leadtimes: bool = False,
        year_min: int = None,
        year_max: int = None,
    ):
        forecast_type = "reforecast" if is_reforecast else "forecast"
        year_min = self.year_min if year_min is None else year_min
        year_max = self.year_max if year_max is None else year_max
        month_range: List = [*range(1, 13)] if split_by_month else [None]
        leadtime_range: List = (
            list(leadtimes) if split_by_leadtimes else [leadtimes]
        )
        logger.info(
            f"Downloading GloFAS {forecast_type} v{version} for years"
            f" {year_min} - {year_max} and lead time {leadtimes}"
        )
        for year in range(year_min, year_max + 1):
            logger.info(f"...{year}")
            for month in month_range:
                for leadtime in leadtime_range:
                    super()._download(
                        version=version,
                        year=year,
                        month=month,
                        leadtime=leadtime,
                    )

    def _process(
        self,
        is_reforecast: bool,
        leadtimes: List[int],
        version: int = glofas.VERSION,
        split_by_month: bool = False,
        split_by_leadtimes: bool = False,
        year_min: int = None,
        year_max: int = None,
    ):
        forecast_type = "reforecast" if is_reforecast else "forecast"
        year_min = self.year_min if year_min is None else year_min
        year_max = self.year_max if year_max is None else year_max
        logger.info(
            f"Processing GloFAS {forecast_type} v{version} for years"
            f" {year_min} - {year_max} and lead time {leadtimes}"
        )
        month_range = [*range(1, 13)] if split_by_month else [None]
        leadtime_range: List = (
            list(leadtimes) if split_by_leadtimes else [leadtimes]
        )
        for leadtime in leadtime_range:
            logger.info(f"For lead time {leadtime}")
            # Get list of files to open
            filepath_list = [
                self._get_raw_filepath(
                    version=version,
                    year=year,
                    month=month,
                    leadtime=leadtime,
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
            logger.info(
                "Looping through reporting_points, this takes some time"
            )
            coord_names = ["number", "time"]
            if not split_by_leadtimes:
                coord_names += ["step"]
            ds_new = self._get_reporting_point_dataset(
                ds=ds,
                coord_names=coord_names,
            )
            # Write out the new dataset to a file
            return self._write_to_processed_file(
                version=version,
                ds=ds_new,
                leadtime=leadtime,
            )


class GlofasForecast(_GlofasForecastBase):
    """GloFAS forecast class."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            year_min=2020,
            year_max=2022,
            cds_name="cems-glofas-forecast",
            dataset=["control_forecast", "ensemble_perturbed_forecasts"],
            system_version_minor={2: 1, 3: 1},
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
            year_max=2018,  # TODO: check this!
            cds_name="cems-glofas-reforecast",
            dataset=["control_reforecast", "ensemble_perturbed_reforecasts"],
            dataset_variable_name="product_type",
            system_version_minor={2: 2, 3: 1},
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
