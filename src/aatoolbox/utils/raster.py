"""Utilities to manipulate and analyze raster data."""

import logging
from typing import Union

import xarray

logger = logging.getLogger(__name__)


def invert_coordinates(
    ds: Union[xarray.Dataset, xarray.DataArray],
    lon_coord: str,
    lat_coord: str,
):
    """
    Invert latitude and longitude in xarray.DataSet.

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
        ds : xarray.DataSet | xarray.DataArray
            Dataset with values and coordinates.
        lon_coord : str
            Longitude coordinate in `ds`.
        lat_coord : str
            Latitude coordinate in `ds`.

    Returns
    -------
    xarray.DataSet | xarray.DataArray
        Dataset or data array with correct coordinate ordering.

    Examples
    --------
    >>> da = xr.DataArray(
    ...     np.arange(16).reshape(4,4),
    ...     coords={"lat":np.array([87, 88, 89, 90]),
                    "lon":np.array([70, 69, 68, 67])}
    ... )
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
