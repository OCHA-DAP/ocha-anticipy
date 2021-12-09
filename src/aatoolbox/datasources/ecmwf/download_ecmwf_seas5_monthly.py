"""
Functions to download the seasonal forecast data from the ECMWF API.

The model that produces this forecast is named SEAS5. More info on
this model can be found in the user guide:
https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf
This also explains the variables used in the server request.
A better understanding of the available products and matching Mars requests can
be found at
https://apps.ecmwf.int/archive-catalogue/?class=od&stream=msmm&expver=1

To access the ECMWF API, you need an authorized account.
More information on the ECMWF API and how to initialize the usage,
can be found at
https://www.ecmwf.int/en/forecasts/access-forecasts/ecmwf-web-api
"""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Union

import geopandas as gpd
import pandas as pd
from ecmwfapi import ECMWFService
from ecmwfapi.api import APIException

from aatoolbox.utils.area import Area, AreaFromShape
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)

# question: where should we define the folder names + structure?
SEAS_DIR = "seasonal-monthly-individual-members"
# question: should prate be its own dir?
PRATE_DIR = "prate"
GRID_RESOLUTION = 0.4  # degrees


def download_api(
    iso3: str,
    area: Area = None,
    iso3_gdf: gpd.GeoDataFrame = None,
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
    iso3 : str
        iso3 code of country of interest
    iso3_gdf : gpd.GeoDataFrame
        GeoDataFrame which contains geometries describing the area
    area : Area, default = None
        Area object containing the boundary coordinates of the area that
        should be downloaded. If None, retrieved from iso3_gdf
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
    # retrieve coord boundaries for which to download data
    if area is None:
        if iso3_gdf is not None:
            area = AreaFromShape(iso3_gdf)
        else:
            area = Area(north=90, south=-90, east=0, west=360)

    # question: is it valid to do this rounding always or
    # should it be optional?

    # prefer to round the coordinates to integers as this
    # will lead to more correspondence to the grid that ecmwf
    # publishes its data on
    area.round_area_coords(round_val=GRID_RESOLUTION)
    if min_date is None:
        min_date = "1992-01-01"
    if max_date is None:
        max_date = date.today().replace(day=1)
    date_list = pd.date_range(start=min_date, end=max_date, freq="MS")
    for date_forec in date_list:
        output_path = get_raw_path_api(
            iso3=iso3, date_forec=date_forec, area=area
        )
        output_path.parent.mkdir(exist_ok=True, parents=True)
        logger.info(f"Downloading file to {output_path}")
        _download_date(
            filepath=output_path,
            date_forec=date_forec,
            area=area,
            grid=grid,
            clobber=clobber,
        )


def get_raw_path_api(iso3, date_forec: pd.Timestamp, area: Area):
    """
    Get the path to the raw api data for a given `date_forec`.

    Parameters
    ----------
    iso3 : str
        iso3 code of country of interest
    date_forec: pd.Timestamp
        publication date of the data
    area : Area
        Area object containing the boundary coordinates of the area that
        should be downloaded.

    Returns
    -------
    path where the raw api data for `date_forec` is saved

    """
    output_dir = (
        Path(os.environ["AA_DATA_DIR"])
        / "private"
        / "raw"
        / iso3
        / "ecmwf"
        / SEAS_DIR
        / PRATE_DIR
    )
    output_filename = (
        f"{iso3}_{SEAS_DIR}_{PRATE_DIR}_{date_forec.strftime('%Y-%m')}"
    )
    if area is not None:
        output_filename += f"_{area.get_filename_repr()}"
    output_filename += ".nc"
    return output_dir / output_filename


@check_file_existence
def _download_date(
    filepath: Path,
    date_forec: pd.Timestamp,
    area: Area,
    clobber: bool,
    grid: float = 0.4,
):
    # the data till 2016 is hindcast data, which only includes 25 members
    # data from 2017 contains 50 members
    if date_forec.year <= 2016:
        number_str = "/".join(str(i) for i in range(0, 25))
    else:
        number_str = "/".join(str(i) for i in range(0, 51))

    server = ECMWFService("mars")
    # question: should this call be explained more?
    # call to server which downloads file
    # meaning of inputs can be found in the links in the
    # top of this script
    server_dict = {
        "class": "od",
        # get an error if several dates at once, so do one at a time
        "date": date_forec.strftime("%Y-%m-%d"),
        "expver": "1",
        "fcmonth": "1/2/3/4/5/6/7",
        "levtype": "sfc",
        "method": "1",
        "number": number_str,
        "origin": "ecmf",
        "param": "228.172",
        "stream": "msmm",
        "system": "5",
        "time": "00:00:00",
        "type": "fcmean",
        "area": f"{area.south}/{area.west}/{area.north}/{area.east}",
        "grid": f"{grid}/{grid}",
        # question: we now download as netcdf
        # we can also download as grib and then there is more info
        # in the file but we do need to use cfgrib
        # which do we prefer?
        "format": "netcdf",
    }
    logger.debug(f"Querying API with parameters {server_dict}")
    try:
        server.execute(
            server_dict,
            filepath,
        )
    except APIException:
        logger.warning(
            f"No data found for {date_forec.strftime('%Y-%m-%d')}. "
            f"Skipping to next date."
        )
