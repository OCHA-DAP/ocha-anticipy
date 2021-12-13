"""
Functions to download and process seasonal forecast data from the ECMWF API.

The model that produces this forecast is named SEAS5. More info on
this model can be found in the user guide:
https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf
This also explains the variables used in the server request.
A better understanding of the available products and matching Mars requests can
be found at
https://apps.ecmwf.int/archive-catalogue/?class=od&stream=msmm&expver=1 with an
explanation of all the keywords at
https://confluence.ecmwf.int/display/UDOC/Identification+keywords

To access the ECMWF API, you need an authorized account.
More information on the ECMWF API and how to initialize the usage,
can be found at
https://www.ecmwf.int/en/forecasts/access-forecasts/ecmwf-web-api
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
from aatoolbox.utils.area import Area, AreaFromShape
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)
_MODULE_BASENAME = "ecmwf"
# folder structure within the ecmwf dir
SEAS_DIR = "seasonal-monthly-individual-members"
PRATE_DIR = "prate"
GRID_RESOLUTION = 0.4  # degrees


class EcmwfApi(DataSource):
    """
    Work with ECMWF's API data.

    Parameters
    ----------
    iso3 : (str)
        country iso3
    """

    def __init__(self, iso3: str, area):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )

        # the area indicates the boundaries for which data is
        # downloaded and processed
        if type(area) == Area:
            self._area = area
        elif type(area) == gpd.GeoDataFrame:
            self._area = AreaFromShape(area)
        else:
            self._area = Area(north=90, south=-90, east=0, west=360)
        # round coordinates to correspond with the grid ecmwf publishes
        # its data on
        self._area.round_area_coords(round_val=GRID_RESOLUTION)

    def download(
        self,
        min_date: Union[str, date] = None,
        max_date: Union[str, date] = None,
        clobber: bool = False,
        grid: float = GRID_RESOLUTION,
    ):
        """
        Download the seasonal forecast precipitation by ECMWF from its API.

        From the ECMWF API retrieve the mean forecasted monthly precipitation
        per ensemble member

        Parameters
        ----------
        min_date : Union[str,date], default = None
            The first date to download data for.
            If None, set to the first available date
        max_date : Union[str,date], default = None
            The last date to download dat for.
            If None, set to the last available date
            All dates between min_date and max_date are downloaded
        clobber: bool, default = False
            If True, overwrite downloaded files if they already exist
        grid: float, default = 0.4
            Grid resolution in degrees

        Examples
        --------
        >>> import geopandas as gpd
        >>> df_admin_boundaries = gpd.read_file(gpd.datasets.get_path('nybb'))
        >>> download(iso3="nybb",ecmwf_dir=ecmwf_dir,
        ... iso3_gdf=df_admin_boundaries.to_crs("epsg:4326"))
        """
        if min_date is None:
            min_date = "1992-01-01"
        if max_date is None:
            max_date = date.today().replace(day=1)
        date_list = pd.date_range(start=min_date, end=max_date, freq="MS")
        for date_forec in date_list:
            output_path = self._get_raw_path_api(date_forec=date_forec)
            output_path.parent.mkdir(exist_ok=True, parents=True)
            logger.info(f"Downloading file to {output_path}")
            self._download_date(
                filepath=output_path,
                date_forec=date_forec,
                grid=grid,
                clobber=clobber,
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
        """
        raw_path = self._get_raw_path_api(date_forec=None)
        filepath_list = list(raw_path.parents[0].glob(raw_path.name))

        output_filepath = self.get_processed_path()
        output_filepath.parent.mkdir(exist_ok=True, parents=True)

        with xr.open_mfdataset(
            filepath_list,
            preprocess=lambda d: self._preprocess(d),
        ) as ds:
            ds.to_netcdf(output_filepath)
        return output_filepath

    def load(self):
        """Load the api ecmwf dataset."""
        return xr.load_dataset(self.get_processed_path())

    def _set_min_max_date(self, min_date, max_date):
        if min_date is None:
            min_date = "1992-01-01"
        if max_date is None:
            max_date = date.today().replace(day=1)
        return min_date, max_date

    def _get_raw_path_api(self, date_forec: Union[pd.Timestamp, None]):
        """Get the path to the raw api data for a given `date_forec`."""
        output_dir = self._raw_base_dir / SEAS_DIR / PRATE_DIR
        output_filename = f"{self._iso3}_{SEAS_DIR}_{PRATE_DIR}_"
        # wildcard date to extract non-date specific filepath name
        if date_forec is None:
            output_filename += "*"
        else:
            output_filename += f"{date_forec.strftime('%Y-%m')}"
        output_filename += f"_{self._area.get_filename_repr()}"
        output_filename += ".nc"
        return output_dir / output_filename

    def _download_date(
        self,
        filepath: Path,
        date_forec: pd.Timestamp,
        clobber: bool,
        grid: float,
    ):
        # the data till 2016 is hindcast data, which only includes 25 members
        # data from 2017 contains 50 members
        if date_forec.year <= 2016:
            number_str = "/".join(str(i) for i in range(0, 25))
        else:
            number_str = "/".join(str(i) for i in range(0, 51))

        server_dict = {
            # ecmwf classification of data
            # od refers to the the operational archive
            "class": "od",
            # publication date to retrieve forecast for
            # get an error if several dates at once, so do one at a time
            "date": date_forec.strftime("%Y-%m-%d"),
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
            # ensemble numbers
            "number": number_str,
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
            # boundaries of area to download
            "area": f"{self._area.south}/{self._area.west}"
            f"/{self._area.north}/{self._area.east}",
            # resolution of the grid
            "grid": f"{grid}/{grid}",
            # other option is to download as grib.
            # This is the default when not specifying format
            # we chose for netcdf as this is the newer format
            # the community is moving towards
            "format": "netcdf",
        }
        logger.debug(f"Querying API with parameters {server_dict}")

        try:
            self._call_api(
                filepath=filepath, server_dict=server_dict, clobber=clobber
            )
        except APIException:
            logger.warning(
                f"No data found for {date_forec.strftime('%Y-%m-%d')}. "
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

    def get_processed_path(self):
        """Return the path to the processed file."""
        output_dir = self._processed_base_dir / SEAS_DIR / PRATE_DIR
        output_filename = (
            f"{self._iso3}_{SEAS_DIR}_{PRATE_DIR}_api"
            f"_{self._area.get_filename_repr()}.nc"
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
