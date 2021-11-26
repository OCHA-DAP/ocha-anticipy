"""
Functions to download the seasonal forecast data from the ECMWF API.

The model that produces this forecast is named Sea5. More info on
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
from datetime import date
from pathlib import Path
from typing import Union

import geopandas as gpd
import pandas as pd
import xarray as xr
from ecmwfapi import ECMWFService
from ecmwfapi.api import APIException

from aatoolbox.utils.area import Area, AreaFromShape
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)

# Questions:
# - Now written specifically for seasonal forecast,
# monthly mean, precipitation.
# could also write a more general ecmwf function,
# but doubt how much is generalizable
# - how much detail should be included in the filenames?
# e.g. coordinates, grid size
# - when downloading the ecmwf data on a 1/1 grid,
# it is not exactly the same as CDS data. Is this supposed to be?

# question: where should we define the folder names + structure?
SEAS_DIR = "seasonal-monthly-individual-members"
# question: should prate be its own dir?
PRATE_DIR = "prate"


def download(
    iso3: str,
    ecmwf_dir: Union[str, Path],
    iso3_gdf: gpd.GeoDataFrame = None,
    min_date: Union[str, date] = None,
    max_date: Union[str, date] = None,
    area: Area = None,
    clobber: bool = False,
    grid: float = 0.4,
):
    """
    Download the seasonal forecast precipitation by ECMWF from its API.

    From the ECMWF API retrieve the mean forecasted monthly precipitation
    per ensemble member

    Parameters
    ----------
    iso3 : str
        iso3 code of country of interest
    ecmwf_dir : Union[str, Path]
        path to the ecmwf dir to which the data should be written
    iso3_gdf : gpd.GeoDataFrame
        GeoDataFrame which contains geometries describing the area
    min_date : Union[str,date], default = None
        The first date to download data for.
        If None, set to the first available date
    max_date : Union[str,date], default = None
        The last date to download dat for.
        If None, set to the last available date
        All dates between min_date and max_date are downloaded
    area : Area, default = None
        Area object containing the boundary coordinates of the area that
        should be downloaded. If None, retrieved from iso3_gdf
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
    if area is not None and iso3_gdf is not None:
        area = AreaFromShape(iso3_gdf)
        # prefer to round the coordinates to integers as this
        # will lead to more correspondence to the grid that ecmwf
        # publishes its data on
        area.round_area_coords()
    if min_date is None:
        min_date = "1992-01-01"
    if max_date is None:
        max_date = date.today().replace(day=1)
    # TODO: the end date should be updated dynamically
    # or catch the APIException for dates that don't exist
    date_list = pd.date_range(start=min_date, end=max_date, freq="MS")
    for date_forec in date_list:
        # TODO: it would probably be safer to also include the boundary coords
        # in the filename.. Just that the filename then gets massive
        output_filename = (
            f"{iso3}_{SEAS_DIR}_{PRATE_DIR}_{date_forec.strftime('%Y-%m')}.nc"
        )
        output_path = _get_output_path(ecmwf_dir) / output_filename
        output_path.parent.mkdir(exist_ok=True, parents=True)

        _download_date(
            filepath=output_path,
            date_forec=date_forec,
            area=area,
            grid=grid,
            clobber=clobber,
        )


def _get_output_path(ecmwf_dir: Union[str, Path]):
    return Path(ecmwf_dir) / SEAS_DIR / PRATE_DIR


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
        "grid": f"{grid}/{grid}",
        "format": "netcdf",
    }
    if area is not None:
        server_dict[
            "area"
        ] = f"{area.south}/{area.west}/{area.north}/{area.east}"
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


def process(
    iso3: str, raw_dir: Union[str, Path], processed_dir: Union[str, Path]
) -> Path:
    """
    Combine the ECMWF seasonal forecast data into a single NetCDF file.

    Parameters
    ----------
    iso3 : str
        ISO3 code of country of interest
    raw_dir : Union[str, Path]
        Directory where raw data was downloaded
    processed_dir :
        Directory to write processed data

    Returns
    -------
    Path to processed NetCDF file
    """
    # TODO: Just list the files for now, should probably compute
    #  them explicitly by looping over dates
    filepath_list = [
        filename
        for filename in _get_output_path(raw_dir).iterdir()
        if filename.is_file()
    ]
    output_filepath = (
        _get_output_path(processed_dir) / f"{iso3}_{SEAS_DIR}_{PRATE_DIR}.nc"
    )
    output_filepath.parent.mkdir(exist_ok=True, parents=True)

    def _preprocess_monthly_mean_dataset(ds_month: xr.Dataset):
        # The individual ECMWF datasets only have a single time parameter,
        # that represents the time of the forecast, which have lead times
        # from 1 to 7 months. This method changes the time parameter to
        # the month the forecast was run, and the step parameter to the
        # lead time in months.
        return (
            ds_month.rename({"time": "step"})
            .assign_coords(
                {
                    "time": ds_month.time.values[0],
                    "step": [1, 2, 3, 4, 5, 6, 7],
                }
            )
            .expand_dims("time")
        )

    with xr.open_mfdataset(
        filepath_list, preprocess=lambda d: _preprocess_monthly_mean_dataset(d)
    ) as ds:
        ds.to_netcdf(output_filepath)
    return output_filepath
