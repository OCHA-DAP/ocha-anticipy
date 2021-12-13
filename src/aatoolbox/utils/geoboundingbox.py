"""
Retrieve boundary coordinates for an area and modify them.

It is possible to create an ``GeographicBoundingBox`` object either from
north, south, east, west coordinates,
or from a shapefile that has been read in with geopandas.
"""
from typing import Union

import geopandas as gpd
import numpy as np


class GeographicBoundingBox:
    """Create an object containing the bounds of an area.

    Parameters
    ----------
    north : float
        The northern latitude boundary of the area (degrees)
    south : float
        The southern latitude boundary of the area (degrees)
    east : float
        The easternmost longitude boundary of the area (degrees)
    west : float
        The westernmost longitude boundary of the area (degrees)
    """

    def __init__(self, north: float, south: float, east: float, west: float):
        self.north = north
        self.south = south
        self.east = east
        self.west = west

    def __repr__(self):
        """Print area string."""
        return (
            f"N: {self.north}\nS: {self.south}\n"
            f"E: {self.east}\nW: {self.west}"
        )

    def round_area_coords(
        self,
        offset_val: float = 0.0,
        round_val: Union[int, float] = 1,
    ):
        """
        Round the area coordinates.

        Parameters
        ----------
        offset_val : float, default = 0.0
            Offset the coordinates by this factor. Some CDS datasets require
            coordinates that end in 0.5.
        round_val : int, default = 1
            The decimal to round to. If 1, round to integers
        """
        for direction in ["north", "west", "south", "east"]:
            coord = getattr(self, direction)
            if direction in ("north", "east"):
                function = np.ceil.__call__  # needed for mypy
                offset_factor = 1
            elif direction in ("south", "west"):
                function = np.floor.__call__  # needed for mypy
                offset_factor = -1
            rounded_coord = (
                function(coord / round_val) * round_val
                + offset_factor * offset_val
            )
            setattr(self, direction, rounded_coord)  # noqa: FKA01

    def get_filename_repr(self, p: int = 0) -> str:
        """
        Get succinct boundary representation for usage in filenames.

        Parameters
        ----------
        p : int, default = 0
            Precision, i.e. number of decimal places to round to. Default is
            0 for ints.

        Returns
        -------
        String containing N, S, E and W coordinates.
        """

        def _str_format(coord):
            """Add m indicating minus value and p indicating positive value."""
            if coord < 0:
                return f"m{abs(coord):.{p}f}"
            else:
                return f"p{coord:.{p}f}"

        return (
            f"N{_str_format(self.north)}S{_str_format(self.south)}"
            f"E{_str_format(self.east)}W{_str_format(self.west)}"
        )


class GeographicBoundingBoxFromShape(GeographicBoundingBox):
    """
    Retrieve north,south,west, and eastern bounds from a geopandas object.

    Parameters
    ----------
    shape : geopandas.GeoSeries, geopandas.GeoDataFrame
        A shape whose bounds will be retrieved

    Examples
    --------
    >>> import geopandas as gpd
    >>> df_admin_boundaries = gpd.read_file("admin0_boundaries.gpkg")
    >>> area = GeographicBoundingBoxFromShape(df_admin_boundaries)
    """

    def __init__(self, shape: Union[gpd.GeoSeries, gpd.GeoDataFrame]):
        super().__init__(
            north=shape.total_bounds[3],
            south=shape.total_bounds[1],
            east=shape.total_bounds[2],
            west=shape.total_bounds[0],
        )
