"""Utilities to manipulate and analyze raster data."""

import logging
from typing import Union

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)


def invert_coordinates(
    ds: Union[xr.Dataset, xr.DataArray],
    lon_coord: str,
    lat_coord: str,
):
    """
    Invert latitude and longitude in ``ds``.

    This function checks for inversion of latitude and longitude
    and inverts them if needed. Datasets with inverted coordinates
    can produce incorrect results in certain functions like
    ``rasterstats.zonal_stats()``. Correctly ordered coordinates
    should be:

    * latitude: Largest to smallest.
    * longitude: Smallest to largest.

    If ``ds`` already has correct coordinate ordering, it is
    directly returned. Function largely copied from
    https://github.com/perrygeo/python-rasterstats/issues/218.

    Parameters
    ----------
        ds : Union[xarray.DataArray, xarray.Dataset]
            Dataset with values and coordinates.
        lon_coord : str
            Longitude coordinate in ``ds``.
        lat_coord : str
            Latitude coordinate in ``ds``.

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Dataset or data array with correct coordinate ordering.

    Examples
    --------
    >>> da = xarray.DataArray(
         numpy.arange(16).reshape(4,4),
         coords={"lat":numpy.array([87, 88, 89, 90]),
                 "lon":numpy.array([70, 69, 68, 67])}
    )
    >>> invert_coordinates(ds, "lon", "lat")
    """
    lon_inv, lat_inv = _check_coords_inverted(
        ds=ds, lon_coord=lon_coord, lat_coord=lat_coord
    )

    # Flip the raster as necessary (based on the flags)
    inv_dict = {}

    if lon_inv:
        logger.info("Longitude was inverted, reversing coordinates.")
        inv_dict[lon_coord] = ds[lon_coord][::-1]

    if lat_inv:
        logger.info("Latitude was inverted, reversing coordinates.")
        inv_dict[lat_coord] = ds[lat_coord][::-1]

    if inv_dict:
        ds = ds.reindex(inv_dict)

    return ds


def _check_coords_inverted(ds, lon_coord, lat_coord):
    """Check if latitude and longitude inverted."""
    lat_start = ds[lat_coord][0].item()
    lat_end = ds[lat_coord][ds.dims[lat_coord] - 1].item()
    lon_start = ds[lon_coord][0].item()
    lon_end = ds[lon_coord][ds.dims[lon_coord] - 1].item()
    return lon_start > lon_end, lat_start < lat_end


# TODO: understand when to change longitude range for rasterstats
# to work and when not!!!
def change_longitude_range(
    ds: Union[xr.DataArray, xr.Dataset],
    lon_coord: str,
):
    """Convert longitude range between -180 to 180 and 0 to 360.

    For some raster data, outputs are incorrect if longitude ranges
    are not converted from 0 to 360 to -180 to 180, such as the IRI
    seasonal forecast, but *not* the IRI CAMS observational terciles,
    which are incorrect if ranges are stored as -180 to 180.

    ``change_longitude_range()`` will convert ``ds`` between the
    two coordinate systems based on its current state. If coordinates
    lie solely between 0 and 180 then there is no need for conversion
    and the input ``ds`` will be returned.

    Parameters
    ----------
    ds : Union[xarray.DataArray, xarray.Dataset]
        Dataset with values and coordinates.
    lon_coord : str
        Longitude coordinate in ``ds``.

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Dataset with transformed longitude coordinates.

    Examples
    --------
    >>> da = xarray.DataArray(
         numpy.arange(16).reshape(4,4),
         coords={"lat":numpy.array([87, 88, 89, 90]),
                 "lon":numpy.array([5, 120, 199, 360])}
    )
    >>> change_longitude_range(ds, "lon")
    """
    lon_min = ds.indexes[lon_coord].min()
    lon_max = ds.indexes[lon_coord].max()

    if lon_max > 180:
        logger.info("Converting longitude from 0 360 to -180 to 180.")

        ds = ds.assign_coords(
            {lon_coord: ((ds[lon_coord] + 180) % 360) - 180}
        ).sortby(lon_coord)

    elif lon_min < 0:
        logger.info("Converting longitude from -180 to 180 to 0 to 360.")

        ds = ds.assign_coords(
            {
                lon_coord: np.where(  # noqa: FKA01
                    ds[lon_coord] < 0,
                    ds[lon_coord] + 360,
                    ds[lon_coord],
                )
            }
        ).sortby(lon_coord)

    else:
        logger.info("Indeterminate longitude range and no need to convert.")

    return ds


def correct_calendar(ds: Union[xr.DataArray, xr.Dataset], time_coord: str):
    """Correct calendar attribute for recognition by xarray.

    Some datasets come with a wrong calendar attribute that isn't
    recognized by xarray. This function corrects the coordinate
    attribute to ensure that a ``calendar`` attribute exists
    and specifies a calendar alias that is supportable by
    ``xarray.cftime_range``.

    Currently ensures that calendar attributes that are either
    specified with ``units="months since"`` or ``calendar="360"``
    explicitly have ``calendar="360_day"``. If and when further
    issues are found with calendar attributes, support for
    conversion will be added here.

    Parameters
    ----------
    ds : Union[xarray.DataArray, xarray.Dataset]
        Dataset with values and coordinates.
    lon_coord : str
        Longitude coordinate in ``ds``.

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Dataset with transformed calendar coordinate.

    Examples
    --------
    >>> da = xarray.DataArray(
         numpy.arange(64).reshape(4,4,4),
         coords={"lat":numpy.array([87, 88, 89, 90]),
                 "lon":numpy.array([5, 120, 199, 360]),
                 "t":numpy.array([10,11,12,13])}
    )
    >>> da["t"].attrs["units"] = "months since 1960-01-01"
    >>> da = correct_calendar(da, "t")
    >>> da["t"].attrs["calendar"]
    """
    if "calendar" in ds[time_coord].attrs.keys():
        if ds[time_coord].attrs["calendar"] == "360":
            ds[time_coord].attrs["calendar"] = "360_day"

    elif "units" in ds[time_coord].attrs.keys():
        if "months since" in ds[time_coord].attrs["units"]:
            ds[time_coord].attrs["calendar"] = "360_day"

    return ds
