"""
Functions to download and process seasonal forecast data from the ECMWF API.

The model that produces this forecast is named SEAS5. More info on
this model can be found in the
`user guide
<https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf>`_

This also explains the variables used in the server request.
An overview of the available products and matching Mars requests can
be found `here
<https://apps.ecmwf.int/archive-catalogue/?class=od&stream=msmm&expver=1>`_
with an explanation of all the keywords `here
<https://confluence.ecmwf.int/display/UDOC/Identification+keywords?`_

To access the ECMWF API, you need an authorized account.
More information on the ECMWF API and how to initialize the usage,
can be found
`here <https://www.ecmwf.int/en/forecasts/access-forecasts/ecmwf-web-api>`_
"""

import logging
from datetime import date
from pathlib import Path
from typing import Union

import geopandas as gpd
import pandas as pd
import xarray as xr
from ecmwfapi import ECMWFService
from ecmwfapi.api import APIException

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)
_MODULE_BASENAME = "ecmwf"
# folder structure within the ecmwf dir
_SEAS_DIR = "seasonal-monthly-individual-members"
_PRATE_DIR = "prate"
_GRID_RESOLUTION = 0.4  # degrees
# number of decimals to include in filename
# data is on 0.4 grid so should be 1
_FILENAME_PRECISION = 1
_MIN_DATE = "1992-01-01"


class EcmwfApi(DataSource):
    """
    Work with ECMWF's API data.

    Parameters
    ----------
    iso3 : str
        country iso3
    geo_bounding_box: GeoBoundingBox | gpd.GeoDataFrame
        the bounding coordinates of the area that is included in the data.
        If None, it will be set to the global bounds
    """

    def __init__(
        self,
        iso3: str,
        match_realtime: bool,
        geo_bounding_box: Union[GeoBoundingBox, gpd.GeoDataFrame] = None,
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )

        # TODO: move geobb declaration to pipeline
        # the geobb indicates the boundaries for which data is
        # downloaded and processed
        if type(geo_bounding_box) == gpd.GeoDataFrame:
            geo_bounding_box = GeoBoundingBox.from_shape(geo_bounding_box)
        # TODO: think about default boundingbox being glb
        elif geo_bounding_box is None:
            geo_bounding_box = GeoBoundingBox(
                north=90, south=-90, east=0, west=360
            )
        # round coordinates to correspond with the grid ecmwf publishes
        # its data on
        geo_bounding_box.round_coords(round_val=_GRID_RESOLUTION)
        self._geobb = geo_bounding_box

    def download(
        self,
        min_date: Union[str, date] = None,
        max_date: Union[str, date] = None,
        clobber: bool = False,
        grid: float = _GRID_RESOLUTION,
    ):
        """
        Download the seasonal forecast precipitation by ECMWF from its API.

        From the ECMWF API retrieve the mean forecasted monthly precipitation
        per ensemble member

        Parameters
        ----------
        min_date : Union[str,date], default = None
            The first date to download data for.
            If string in ISO 8601 format, e.g. '2020-01-01'
            If None, set to the first available date
        max_date : Union[str,date], default = None
            The last date to download dat for.
            If string in ISO 8601 format, e.g. '2020-01-01'
            If None, set to the last available date
            All dates between min_date and max_date are downloaded
        clobber: bool, default = False
            If True, overwrite downloaded files if they already exist
        grid: float, default = 0.4
            Grid resolution in degrees

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> (from aatoolbox.datasources.ecmwf.api_seas5_monthly
        ... import EcmwfApi)
        >>> pipeline_mwi = Pipeline("mwi")
        >>> mwi_admin0 = pipeline_mwi.load_codab(admin_level=0)
        >>> ecmwf_api=EcmwfApi(iso3="mwi",geo_bounding_box=mwi_admin0)
        >>> ecmwf_api.download()
        """
        if min_date is None:
            min_date = _MIN_DATE
        if max_date is None:
            max_date = date.today().replace(day=1)
        date_list = pd.date_range(start=min_date, end=max_date, freq="MS")
        for date_forecast in date_list:
            output_path = self._get_raw_path(date_forecast=date_forecast)
            output_path.parent.mkdir(exist_ok=True, parents=True)
            logger.info(f"Downloading file to {output_path}")
            self._download_forecast_from_date(
                filepath=output_path,
                date_forecast=date_forecast,
                clobber=clobber,
                grid=grid,
            )

    def process(
        self,
    ) -> Path:
        """
        Combine the ECMWF seasonal forecast data from the API.

        Data is downloaded per date, combine to one file.

        Returns
        -------
        Path to processed NetCDF file

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> (from aatoolbox.datasources.ecmwf.api_seas5_monthly
        ... import EcmwfApi)
        >>> pipeline_mwi = Pipeline("mwi")
        >>> mwi_admin0 = pipeline_mwi.load_codab(admin_level=0)
        >>> ecmwf_api=EcmwfApi(iso3="mwi",geo_bounding_box=mwi_admin0)
        >>> ecmwf_api.process()
        """
        # get path structure with publication date as wildcard
        raw_path = self._get_raw_path(date_forecast=None)
        filepath_list = list(raw_path.parents[0].glob(raw_path.name))

        output_filepath = self._get_processed_path()
        output_filepath.parent.mkdir(exist_ok=True, parents=True)

        with xr.open_mfdataset(
            filepath_list,
            preprocess=lambda d: self._preprocess(d),
        ) as ds:
            # include the names of all files that are included in the ds
            ds.attrs["included_files"] = [f.stem for f in filepath_list]
            ds.to_netcdf(output_filepath)
        return output_filepath

    def load(self):
        """
        Load the api ecmwf dataset.

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> (from aatoolbox.datasources.ecmwf.api_seas5_monthly
        ... import EcmwfApi)
        >>> pipeline_mwi = Pipeline("mwi")
        >>> mwi_admin0 = pipeline_mwi.load_codab(admin_level=0)
        >>> ecmwf_api=EcmwfApi(iso3="mwi",geo_bounding_box=mwi_admin0)
        >>> ecmwf_api.download()
        >>> ecmwf_api.process()
        >>> ecmwf_api.load()
        """
        return xr.load_dataset(self._get_processed_path())

    def _get_raw_path(self, date_forecast: Union[pd.Timestamp, None]):
        """Get the path to the raw api data for a given `date_forecast`."""
        output_dir = self._raw_base_dir / _SEAS_DIR / _PRATE_DIR
        output_filename = f"{self._iso3}_{_SEAS_DIR}_{_PRATE_DIR}_"
        # wildcard date to extract non-date specific filepath name
        if date_forecast is None:
            output_filename += "*"
        else:
            output_filename += f"{date_forecast.strftime('%Y-%m')}"
        output_filename += (
            f"_{self._geobb.get_filename_repr(p=_FILENAME_PRECISION)}.nc"
        )
        return output_dir / output_filename

    def _download_forecast_from_date(
        self,
        filepath: Path,
        date_forecast: pd.Timestamp,
        clobber: bool,
        grid: float,
    ):
        # the data till 2016 is hindcast data, which only includes 25 members
        # data from 2017 contains 50 members
        if date_forecast.year <= 2016:
            number_str = "/".join(str(i) for i in range(0, 25))
        else:
            number_str = "/".join(str(i) for i in range(0, 51))

        # api request items that don't depend on input
        server_dict_const = {
            # ecmwf classification of data
            # od refers to the the operational archive
            "class": "od",
            # the experiment version. production data is always 1 or 2
            "expver": "1",
            # leadtime months
            "fcmonth": "1/2/3/4/5/6/7",
            # type of horizontal level
            # sfc means surface
            "levtype": "sfc",
            # method of generation, standard is 1
            # method 3 is 13 month ahead forecast
            "method": "1",
            # origin of data
            # for seas5 always ecmwf
            "origin": "ecmf",
            # the meteoroglocial parameter
            # in our cases this the mean total precipitation rate
            # see https://apps.ecmwf.int/codes/grib/param-db/?id=172228
            "param": "228.172",
            # the forecasting system.
            # msmm is the seas forecast system for monthly means (mm)
            "stream": "msmm",
            # version of the system
            "system": "5",
            # runtime. for seas forecast this can only be 00 UTC
            "time": "00:00:00",
            # type of the parameter
            # fcmean refercs to forecast (fc) mean
            # can also be em = ensemble mean
            "type": "fcmean",
            # other option is to download as grib.
            # This is the default when not specifying format
            # we chose for netcdf as this is the newer format
            # the community is moving towards
            "format": "netcdf",
        }

        # items that depend on inputs
        server_dict_var = {
            # publication date to retrieve forecast for
            # get an error if several dates at once, so do one at a time
            "date": date_forecast.strftime("%Y-%m-%d"),
            # ensemble numbers
            "number": number_str,
            # boundaries of geobb to download
            "area": f"{self._geobb.south}/{self._geobb.west}"
            f"/{self._geobb.north}/{self._geobb.east}",
            # resolution of the grid
            "grid": f"{grid}/{grid}",
        }

        # merge the two dicts for the full server request
        server_dict = {**server_dict_const, **server_dict_var}
        logger.debug(f"Querying API with parameters {server_dict}")

        try:
            self._call_api(
                filepath=filepath, server_dict=server_dict, clobber=clobber
            )
        except APIException:
            logger.warning(
                f"No data found for {date_forecast.strftime('%Y-%m-%d')}. "
                f"Skipping to next date."
            )

    @staticmethod
    @check_file_existence
    def _call_api(filepath, server_dict, clobber):
        server = ECMWFService("mars")
        server.execute(
            server_dict,
            filepath,
        )

    def _get_processed_path(self):
        """Return the path to the processed file."""
        output_dir = self._processed_base_dir / _SEAS_DIR / _PRATE_DIR
        output_filename = (
            f"{self._iso3}_{_SEAS_DIR}_{_PRATE_DIR}_api"
            f"_{self._geobb.get_filename_repr(p=_FILENAME_PRECISION)}.nc"
        )
        return output_dir / output_filename

    @staticmethod
    def _preprocess(ds_month: xr.Dataset):
        """Preprocess individual files before combining.

        The individual ECMWF datasets only have a single time parameter,
        that represents the time of the forecast, which have lead times
        from 1 to 7 months. This method changes the time parameter to
        the month the forecast was run, and the step parameter to the
        lead time in months.
        """
        # date of publication.
        # The day is set to 1 so it is more the month of publication
        pub_date = ds_month.time[0]
        # steps indicates the leadtime.
        # Ranging from 1 to 7 since that is ecmwf's convention
        # step/leadtime of 1 means the month being forecasted
        # is the month of pub_date
        steps = (
            (ds_month.time.dt.month - pub_date.dt.month)
            + 12 * (ds_month.time.dt.year - pub_date.dt.year)
            + 1
        )
        ds_month = (
            ds_month.rename({"time": "step"})
            .assign_coords({"time": pub_date, "step": steps.values})
            .expand_dims("time")
        )
        return ds_month
