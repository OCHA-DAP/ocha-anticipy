"""Utilities to manipulate and analyze raster data."""

import logging
from typing import Any, Dict, List, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import rioxarray
import xarray as xr

logger = logging.getLogger(__name__)


def invert_coordinates(
    da: Union[xr.Dataset, xr.DataArray],
    lon_coord: str,
    lat_coord: str,
):
    """
    Invert latitude and longitude in ``da``.

    This function checks for inversion of latitude and longitude
    and inverts them if needed. Datasets with inverted coordinates
    can produce incorrect results in certain functions like
    ``rasterstats.zonal_stats()``. Correctly ordered coordinates
    should be:

    * latitude: Largest to smallest.
    * longitude: Smallest to largest.

    If ``da`` already has correct coordinate ordering, it is
    directly returned. Function largely copied from
    https://github.com/perrygeo/python-rasterstats/issues/218.

    Parameters
    ----------
        da : Union[xarray.DataArray, xarray.Dataset]
            Dataset with values and coordinates.
        lon_coord : str
            Longitude coordinate in ``da``.
        lat_coord : str
            Latitude coordinate in ``da``.

    Returns
    -------
    Union[xarray.DataArray, xarray.Dataset]
        Dataset or data array with correct coordinate ordering.

    Examples
    --------
    >>> import xarray
    >>> import numpy
    >>> da = xarray.DataArray(
    ...  numpy.arange(16).reshape(4,4),
    ...  coords={"lat":numpy.array([87, 88, 89, 90]),
    ...          "lon":numpy.array([70, 69, 68, 67])}
    ... )
    >>> da_inv = invert_coordinates(da, "lon", "lat")
    >>> da_inv.get_index("lon")
    Int64Index([67, 68, 69, 70], dtype='int64', name='lon')
    >>> da_inv.get_index("lat")
    Int64Index([90, 89, 88, 87], dtype='int64', name='lat')
    """
    lon_inv, lat_inv = _check_coords_inverted(
        da=da, lon_coord=lon_coord, lat_coord=lat_coord
    )

    # Flip the raster as necessary (based on the flags)
    inv_dict = {}

    if lon_inv:
        logger.info("Longitude was inverted, reversing coordinates.")
        inv_dict[lon_coord] = da[lon_coord][::-1]

    if lat_inv:
        logger.info("Latitude was inverted, reversing coordinates.")
        inv_dict[lat_coord] = da[lat_coord][::-1]

    if inv_dict:
        da = da.reindex(inv_dict)

    return da


def _check_coords_inverted(da, lon_coord, lat_coord):
    """
    Check if latitude and longitude inverted.

    Examples
    --------
    >>> import xarray
    >>> import numpy
    >>> da = xarray.DataArray(
    ...  numpy.arange(16).reshape(4,4),
    ...  coords={"lat":numpy.array([90, 89, 88, 87]),
    ...          "lon":numpy.array([70, 69, 68, 67])}
    ... )
    >>> _check_coords_inverted(da, "lon", "lat")
    (True, False)
    """
    lat = da.get_index(lat_coord)
    lat_start = lat[0]
    lat_end = lat[-1]
    lon = da.get_index(lon_coord)
    lon_start = lon[0]
    lon_end = lon[-1]
    return lon_start > lon_end, lat_start < lat_end


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
    >>> import xarray
    >>> import numpy
    >>> da = xarray.DataArray(
    ...  numpy.arange(16).reshape(4,4),
    ...  coords={"lat":numpy.array([87, 88, 89, 90]),
    ...          "lon":numpy.array([5, 120, 199, 360])}
    ... )
    >>> da_inv = change_longitude_range(da, "lon")
    >>> da_inv.get_index("lon")
    Int64Index([-161, 0, 5, 120], dtype='int64', name='lon')
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
    >>> import xarray
    >>> import numpy
    >>> da = xarray.DataArray(
    ...  numpy.arange(64).reshape(4,4,4),
    ...  coords={"lat":numpy.array([87, 88, 89, 90]),
    ...          "lon":numpy.array([5, 120, 199, 360]),
    ...          "t":numpy.array([10,11,12,13])}
    ... )
    >>> da["t"].attrs["units"] = "months since 1960-01-01"
    >>> da_crct = correct_calendar(da, "t")
    >>> da_crct["t"].attrs["calendar"]
    '360_day'
    """
    if "calendar" in ds[time_coord].attrs.keys():
        if ds[time_coord].attrs["calendar"] == "360":
            ds[time_coord].attrs["calendar"] = "360_day"

    elif "units" in ds[time_coord].attrs.keys():
        if "months since" in ds[time_coord].attrs["units"]:
            ds[time_coord].attrs["calendar"] = "360_day"

    return ds


def compute_raster_statistics(
    gdf: gpd.GeoDataFrame,
    feature_col: str,
    da: xr.DataArray,
    lon_coord: str = "x",
    lat_coord: str = "y",
    stats_list: List[str] = None,
    percentile_list: List[int] = None,
    all_touched: bool = False,
    geom_col: str = "geometry",
):
    """Compute raster statistics for polygon geometry.

    ``compute_raster_statistics()`` is designed to
    quickly compute raster statistics across a polygon
    and its features.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with row per area for stats computation.
    feature_col : str
        Column in ``gdf`` to use as row/feature identifier.
    da : xarray.DataArray
        Raster array for computation. Must have a CRS.
    lon_coord : str, optional
        Longitude coordinate in ``da``, by default "x".
    lat_coord : str, optional
        Longitude coordinate in ``da``, by default "y".
    stats_list : List[str], optional
        List of statistics to calculate, by default None.
        Passed to ``get_attr()``.
    percentile_list : List[int], optional
        List of percentiles to compute, by default None.
    all_touched : bool, optional
        If ``True`` all cells touching the region will be
        included, by default False.
    geom_col : str, optional
        Column in ``gdf`` with geometry, by default "geometry".

    Returns
    -------
    pandas.DataFrame
        Dataframe with computed statistics.
    """
    if da.rio.crs is None:
        raise rioxarray.exceptions.MissingCRS(
            "No CRS found, set CRS before computation."
        )

    df_list = []

    if stats_list is None:
        stats_list = ["mean", "std", "min", "max", "sum", "count"]

    for feature in gdf[feature_col].unique():
        gdf_adm = gdf[gdf[feature_col] == feature]

        da_clip = da.rio.set_spatial_dims(x_dim=lon_coord, y_dim=lat_coord)

        # clip returns error if no overlapping raster cells for geometry
        # so catching this and skipping rest of iteration so no stats computed
        try:
            da_clip = da_clip.rio.clip(
                gdf_adm[geom_col], all_touched=all_touched
            )
        except rioxarray.exceptions.NoDataInBounds:
            logger.warning(
                "No overlapping raster cells for %s, skipping.", feature
            )
            continue

        grid_stat_all = []
        for stat in stats_list:
            # count automatically ignores NaNs
            # therefore skipna can also not be given as an argument
            # implemented count cause needed for computing percentages
            kwargs: Dict[str, Any] = {}
            if stat != "count":
                kwargs["skipna"] = True
            # makes sum return NaN instead of 0 if array
            # only contains NaNs
            if stat == "sum":
                kwargs["min_count"] = 1
            grid_stat = getattr(da_clip, stat)(
                dim=[lon_coord, lat_coord], **kwargs
            ).rename(f"{stat}_{feature_col}")
            grid_stat_all.append(grid_stat)

        if percentile_list is not None:
            grid_quant = [
                da_clip.quantile(quant / 100, dim=[lon_coord, lat_coord])
                .drop("quantile")
                .rename(f"{quant}quant_{feature_col}")
                for quant in percentile_list
            ]
            grid_stat_all.extend(grid_quant)

        # if dims is 0, it throws an error when merging
        # and then converting to a df
        # this occurs when the input da is 2D
        if not grid_stat_all[0].dims:
            df_adm = pd.DataFrame(
                {da_stat.name: [da_stat.values] for da_stat in grid_stat_all}
            )

        else:
            zonal_stats_xr = xr.merge(grid_stat_all)
            df_adm = (
                zonal_stats_xr.to_dataframe()
                .drop("spatial_ref", axis=1)
                .reset_index()
            )
        df_adm[feature_col] = feature
        df_list.append(df_adm)

    df_zonal_stats = pd.concat(df_list).reset_index(drop=True)
    return df_zonal_stats
