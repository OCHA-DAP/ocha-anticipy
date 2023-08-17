"""Download and process GloFAS forecast and reforecast river discharge data."""
import logging
from datetime import date
from pathlib import Path
from typing import Union

import xarray as xr
from dateutil import rrule

from ochanticipy.config.countryconfig import CountryConfig
from ochanticipy.datasources.glofas import glofas
from ochanticipy.utils.check_file_existence import check_file_existence
from ochanticipy.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)


class GlofasForecast(glofas.Glofas):
    """
    Class for downloading and processing GloFAS forecast data.

    The GloFAS forecast dataset is a global raster presenting river
    discharge forecast from 26 May 2021 until present day (updated daily), see
    `this paper <https://hess.copernicus.org/preprints/hess-2020-532/>`__
    for more details. While CDS does have version 3 pre-release data
    from 2020-2021,
    we understand that there were some small issues that were fixed
    in the final version, so at this point in time this module
    does not support downloading the pre-release data.

    This class downloads the raw raster data
    `from CDS
    <https://cds.climate.copernicus.eu/cdsapp#!/dataset/cems-glofas-forecast?tab=overview>`__,
    and processes it from a raster to a datasets of reporting points from the
    `GloFAS interface
    <https://www.globalfloods.eu/glofas-forecasting/>`_.
    Due to the CDS request size limits, separate files are downloaded per
    day (that contain all requested lead times).

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        The bounding coordinates of the area that should be included
    leadtime_max: int
        The maximum desired lead time D in days. All forecast data for lead
        times 1 to D days are downloaded
    start_date : Union[date, str], default: date(year=2021, month=5, day=26)
        The starting date for the dataset. If left blank, defaults to the
        earliest available date
    end_date : Union[date, str], default: date.today()
        The ending date for the dataset. If left blank, defaults to
        the current date
    model_version : int, default: 4
        The version of the GloFAS model to use, can only be 3 or 4.
        If in doubt, always use the latest (default).

    Examples
    --------
    Download, process and load GloFAS forecast data for the past month,
    for a lead time of 15 days.

    >>> from datetime import date
    >>> from ochanticipy import create_country_config, CodAB, GeoBoundingBox,
    ... GlofasForecast
    >>>
    >>> country_config = create_country_config(iso3="npl")
    >>> codab = CodAB(country_config=country_config)
    >>> codab.download()
    >>> admin_npl = codab.load()
    >>> geo_bounding_box = GeoBoundingBox.from_shape(admin_npl)
    >>>
    >>> glofas_forecast = GlofasForecast(
    ...     country_config=country_config,
    ...     geo_bounding_box=geo_bounding_box,
    ...     leadtime_max=15,
    ...     end_date=date(year=2022, month=10, day=22),
    ...     start_date=date(year=2022, month=9, day=22)
    ... )
    >>> glofas_forecast.download()
    >>> glofas_forecast.process()
    >>>
    >>> npl_glofas_forecast_reporting_points = glofas_forecast.load()
    """

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        leadtime_max: int,
        start_date: Union[date, str] = None,
        end_date: Union[date, str] = None,
        model_version: int = glofas.DEFAULT_MODEL_VERSION,
    ):
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            start_date=start_date,
            end_date=end_date,
            cds_name="cems-glofas-forecast",
            model_version=model_version,
            product_type=["control_forecast", "ensemble_perturbed_forecasts"],
            date_variable_prefix="",
            frequency=rrule.DAILY,
            coord_names=["time", "number", "step"],
            leadtime_max=leadtime_max,
            start_date_min=date(year=2021, month=5, day=26),
        )

    @staticmethod
    def _system_version_dict() -> glofas.SystemVersions:
        system_versions = glofas.SystemVersions()
        system_versions[3] = "version_3_1"
        system_versions[4] = "operational"
        return system_versions

    @check_file_existence
    def _load_single_file(
        self, input_filepath: Path, filepath: Path, clobber: bool
    ) -> xr.Dataset:
        return _read_in_ensemble_and_perturbed_datasets(
            filepath=input_filepath
        ).expand_dims("time")


class GlofasReforecast(glofas.Glofas):
    """
    Class for downloading and processing GloFAS reforecast data.

    The GloFAS reforecast dataset is a global raster presenting river
    discharge forecasted from 1999 until 2018, see
    `this paper <https://hess.copernicus.org/preprints/hess-2020-532/>`_
    for more details.

    This class downloads the raw raster data
    `from CDS
    <https://cds.climate.copernicus.eu/cdsapp#!/dataset/cems-glofas-reforecast?tab=overview>`__,
    and processes it from a raster to a datasets of reporting points from the
    `GloFAS interface
    <https://www.globalfloods.eu/glofas-forecasting/>`_.
    Due to the CDS request size limits, separate files are downloaded per
    month (that contain all requested lead times).

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        The bounding coordinates of the area that should be included
    leadtime_max: int
        The maximum desired lead time D in days. All forecast data for lead
        times 1 to D days are downloaded
    start_date : Union[date, str], default: date(year=1999, month=1, day=1)
        The starting date for the dataset. If left blank, defaults to the
        earliest available date
    end_date : Union[date, str], default: date(year=2018, month=12, day=31)
        The ending date for the dataset. If left blank, defaults to the
        last available date
    model_version : int, default: 4
        The version of the GloFAS model to use, can only be 3 or 4.
        If in doubt, always use the latest (default).

    Examples
    --------
    Download, process and load all available GloFAS reforecast data
    for a lead time of 15 days.

    >>> from ochanticipy import create_country_config, CodAB, GeoBoundingBox,
    ... GlofasReforecast
    >>>
    >>> country_config = create_country_config(iso3="npl")
    >>> codab = CodAB(country_config=country_config)
    >>> codab.download()
    >>> admin_npl = codab.load()
    >>> geo_bounding_box = GeoBoundingBox.from_shape(admin_npl)
    >>>
    >>> glofas_reforecast = GlofasReforecast(
    ...     country_config=country_config,
    ...     geo_bounding_box=geo_bounding_box,
    ...     leadtime_max=15
    ... )
    >>> glofas_reforecast.download()
    >>> glofas_reforecast.process()
    >>>
    >>> npl_glofas_reforecast_reporting_points = glofas_reforecast.load()
    """

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        leadtime_max: int,
        start_date: Union[date, str] = None,
        end_date: Union[date, str] = None,
        model_version: int = glofas.DEFAULT_MODEL_VERSION,
    ):
        # Unfortunately start date min and max, as well as months,
        # depend on the model version. If this were more complicated
        # then it may make sense to use a factory, but that would change
        # how the constructor is called so trying to avoid it for now.
        if model_version == 3:
            start_date_min = date(year=1999, month=1, day=1)
            end_date_max = date(year=2018, month=12, day=31)
            month_list = None
        elif model_version == 4:
            start_date_min = date(year=2003, month=3, day=1)
            end_date_max = date(year=2022, month=8, day=31)
            month_list = list(range(3, 9))  # March to August
        else:
            raise ValueError("Model version must be 3 or 4")
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            start_date=start_date,
            end_date=end_date,
            model_version=model_version,
            cds_name="cems-glofas-reforecast",
            product_type=[
                "control_reforecast",
                "ensemble_perturbed_reforecasts",
            ],
            date_variable_prefix="h",
            frequency=rrule.MONTHLY,
            coord_names=["number", "time", "step"],
            leadtime_max=leadtime_max,
            start_date_min=start_date_min,
            end_date_max=end_date_max,
            month_list=month_list,
        )

    @staticmethod
    def _system_version_dict() -> glofas.SystemVersions:
        system_versions = glofas.SystemVersions()
        system_versions[3] = "version_3_1"
        system_versions[4] = "version_4_0"
        return system_versions

    @check_file_existence
    def _load_single_file(
        self, input_filepath: Path, filepath: Path, clobber: bool
    ) -> xr.Dataset:
        return _read_in_ensemble_and_perturbed_datasets(
            filepath=input_filepath
        )


def _read_in_ensemble_and_perturbed_datasets(filepath: Path):
    """Read in forecast and reforecast data.

    The GloFAS forecast and reforecast data GRIB files contain two
    separate datasets: the control member, generated from the most accurate
    estimate of current conditions, and the perturbed forecast, which
    contains N ensemble members created by perturbing the control forecast.

    This function reads in both datasets and creates an N+1 (perturbed
    + control) ensemble.
    See `this paper <https://hess.copernicus.org/preprints/hess-2020-532/>`__
    for more details.
    """
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
        # Extra processing require for control forecast
        if data_type == "cf":
            ds = ds.expand_dims(dim="number")
        ds_list.append(ds)
    return xr.combine_by_coords(ds_list, combine_attrs="drop_conflicts")
