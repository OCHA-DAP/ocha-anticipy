"""
Functions to download and process ECMWF's seasonal forecast data.

The model that produces this forecast is named SEAS5. More info on
this model can be found in the user guide:
https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf
"""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Literal, Union

import geopandas as gpd
import pandas as pd
import xarray as xr

from aatoolbox.datasources.ecmwf.download_ecmwf_seas5_monthly import (
    download_api,
    get_raw_path_api,
)
from aatoolbox.utils.area import Area, AreaFromShape

logger = logging.getLogger(__name__)

# question: where should we define the folder names + structure?
SEAS_DIR = "seasonal-monthly-individual-members"
# question: should prate be its own dir?
PRATE_DIR = "prate"

# is it best to set this here or as a default value ina  config or so?
# contains all the realtime data, not only seas forecast
ECMWF_REALTIME_RAW_DIR = (
    Path(os.environ["AA_DATA_DIR"]) / "private" / "raw" / "glb" / "ecmwf"
)

ISO3_POINTS_MAPPING = {"mwi": 384}
ISO3_AREA_MAPPING = {"mwi": Area(north=-5, south=-17, east=37, west=33)}


def combine_realtime_api(iso3: str, download: bool = True) -> Path:
    # in order to combine the two datasets, the area should be the same
    # the realtime dataset only has the area mapping
    # so that area should also be used for the api
    """
    Combine the datasets from the two retrieval methods.

    I.e. from the realtime folder and the api
    Parameters
    ----------
     iso3 : str
        iso3 code of country of interest
    download: bool
        if True process and download the data of the individual sources.

    Returns
    -------
    Path to processed NetCDF file

    Examples
    --------
    >>> combine_realtime_api(iso3="mwi")
    """
    area = ISO3_AREA_MAPPING[iso3]
    if download:
        process_realtime(iso3=iso3)
        process_api(iso3=iso3, area=area)
    ds_api = xr.load_dataset(
        _get_processed_path(iso3=iso3, area=area, data_source="api")
    )
    ds_realtime = xr.load_dataset(
        _get_processed_path(iso3=iso3, area=area, data_source="realtime")
    )
    # TODO: realtime and api can contain the same forecast dates
    # not sure yet how to handle that..
    ds_comb = ds_api.merge(ds_realtime)
    output_path = _get_processed_path(iso3=iso3, area=area, data_source="comb")
    ds_comb.to_netcdf(output_path)
    return output_path


# TODO future: I am now manually copying the data
#  from the amazon bucket to our private gdrive
#  It would be much nicer to be able to automatically connect with the bucket
def process_realtime(iso3: str, datavar: str = "fcmean") -> Path:
    """
    Process the seasonal forecast by ECMWF that is shared in realtime.

    This data is private and thus is only shared if you have
    a license agreementwith ECMWF.
    The data lives in ECMWF_REALTIME_RAW_DIR

    Parameters
    ----------
    iso3 : str
        iso3 code of country of interest
    datavar: str, default = fcmean
        Type of variable to extract. fcmean contains the monthly mean
        per ensemble member. em the mean across all ensemble members

    Returns
    -------
    Path to processed NetCDF file

    Examples
    --------
    >>> process_realtime(iso3="mwi")
    """
    # T4L indicates the seasonal forecast with the monthly mean
    # see here for a bit more explanation
    # https://confluence.ecmwf.int/pages/viewpage.action?pageId=111155348
    # without the 1 you also get .idx files which we don't
    # want but not sure if this is the best method to select
    filepath_list = list(ECMWF_REALTIME_RAW_DIR.glob("*T4L*1"))

    # i would think setting concat_dim=["time","step"] makes more sense
    # but get an error "concat_dims has length 2 but the datasets passed
    # are nested in a 1-dimensional structure"
    # it seems to work thought when using concat_dim="time"
    # but would have to test once we have data from several dates..
    output_filepath = _get_processed_path(
        iso3=iso3, area=ISO3_AREA_MAPPING[iso3], data_source="realtime"
    )
    output_filepath.parent.mkdir(exist_ok=True, parents=True)
    with xr.open_mfdataset(
        filepath_list,
        engine="cfgrib",
        filter_by_keys={
            "numberOfPoints": ISO3_POINTS_MAPPING[iso3],
            "dataType": datavar,
        },
        concat_dim=["step"],
        combine="nested",
        preprocess=lambda d: _preprocess_seas_forec_realtime(d),
        # TODO: we currently don't use "verifying_time" so maybe remove that
        backend_kwargs={
            "time_dims": ("time", "forecastMonth", "verifying_time")
        },
    ) as ds:
        ds.to_netcdf(output_filepath)

    return output_filepath


# question: do we want this function here or inside process_realtime()
def _preprocess_seas_forec_realtime(ds_date: xr.Dataset):
    """Set coordinate types and remove irrelevant dimensions."""
    ds_date = ds_date.assign_coords(
        {
            "latitude": ds_date.latitude.astype("float32"),
            "longitude": ds_date.longitude.astype("float32"),
        }
    )
    return (
        ds_date.expand_dims("time")
        # surface is empty
        .drop_vars("surface")
    )


def process_api(
    iso3: str,
    # question: could I pass this with a config?
    iso3_gdf: gpd.GeoDataFrame = None,
    area: Area = None,
    min_date: Union[str, date] = None,
    max_date: Union[str, date] = None,
    download=True,
) -> Path:
    """
    Combine the ECMWF seasonal forecast data from the API.

    Data is downloaded per date, combine to one file.

    Parameters
    ----------
    iso3 : str
        ISO3 code of country of interest
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

    Returns
    -------
    Path to processed NetCDF file
    """
    if min_date is None:
        min_date = "1992-01-01"
    if max_date is None:
        max_date = date.today().replace(day=1)
    if area is None:
        if iso3_gdf is not None:
            area = AreaFromShape(iso3_gdf)
        else:
            area = Area(north=90, south=-90, east=0, west=360)
    # question: does it make sense to have the option here to download the data
    # or should that just be two separate calls?
    # question: there is also the grid and clobber option..
    # should I pass that with kwargs?
    if download:
        download_api(
            iso3=iso3,
            area=area,
            iso3_gdf=iso3_gdf,
            min_date=min_date,
            max_date=max_date,
        )
    filepath_list = [
        get_raw_path_api(iso3=iso3, date_forec=d, area=area)
        for d in pd.date_range(start=min_date, end=max_date, freq="MS")
    ]
    # only include files that exist, e.g. possible that not all dates are
    # downloaded between min and max date
    filepath_list = [
        filename for filename in filepath_list if filename.is_file()
    ]
    output_filepath = _get_processed_path(
        iso3=iso3, area=area, data_source="api"
    )
    output_filepath.parent.mkdir(exist_ok=True, parents=True)

    with xr.open_mfdataset(
        filepath_list, preprocess=lambda d: _preprocess_seas_forec_api(d)
    ) as ds:
        ds.to_netcdf(output_filepath)
    return output_filepath


def _preprocess_seas_forec_api(ds_month: xr.Dataset):
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


def _get_processed_path(
    iso3: str, area: Area, data_source: Literal["realtime", "api", "comb"]
):
    output_dir = (
        Path(os.environ["AA_DATA_DIR"])
        / "private"
        / "processed"
        / iso3
        / "ecmwf"
        / SEAS_DIR
        / PRATE_DIR
    )
    output_filename = f"{iso3}_{SEAS_DIR}_{PRATE_DIR}"
    if data_source == "realtime":
        output_filename += "_realtime"
    if data_source == "api":
        output_filename += "_api"
    if area is not None:
        output_filename += f"_{area.get_filename_repr()}"
    output_filename += ".nc"
    return output_dir / output_filename
