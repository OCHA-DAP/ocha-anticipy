"""
Functionality to retrieve and modify boundary coordinates.

It is possible to create an ``GeoBoundingBox`` object either from
lat_max, lat_min, lon_max, lon_min coordinates,
or from a shapefile that has been read in with geopandas.
"""
import logging
from decimal import Decimal, getcontext
from typing import Union

import geopandas as gpd
import numpy as np

logger = logging.getLogger(__name__)
getcontext().prec = 5  # Assuming we won't need higher than this!


class GeoBoundingBox:
    """Create an object containing the bounds of an area.

    Standard geographic coordinate system is used where latitude runs
    from -90 to 90 degrees, and latitude from -180 to 180.
    North must always be greater than south, and east greater
    than west.

    Parameters
    ----------
    lat_max : float
        The northern latitude boundary of the area (degrees).
        The value must be between -90 and 90, and greater than or equal to the
        southern boundary.
    lat_min : float
        The southern latitude boundary of the area (degrees).
        The value must be between -90 and 90, and less than or equal to the
        northern boundary.
    lon_max : float
        The easternmost longitude boundary of the area (degrees).
        The value must be between -180 and 180, and greater than or equal to
        the western boundary.
    lon_min : float
        The westernmost longitude boundary of the area (degrees).
        The value must be between -180 and 180, and less than or equal to the
        eastern boundary.
    """

    def __init__(
        self, lat_max: float, lat_min: float, lon_max: float, lon_min: float
    ):
        self.lat_max = lat_max
        self.lat_min = lat_min
        self.lon_max = lon_max
        self.lon_min = lon_min

    @property
    def lat_max(self) -> float:
        """Get the northern latitude boundary of the area (degrees)."""
        return float(self._lat_max)

    @lat_max.setter
    def lat_max(self, lat_max):
        _check_latitude(lat_max)
        self._lat_max = Decimal(lat_max)

    @property
    def lat_min(self) -> float:
        """Get the southern latitude boundary of the area (degrees)."""
        return float(self._lat_min)

    @lat_min.setter
    def lat_min(self, lat_min):
        _check_latitude(lat_min)
        if not lat_min <= self.lat_max:
            raise AttributeError(
                "The maximum latitude must be greater than or equal to"
                "the minimum latitude"
            )
        self._lat_min = Decimal(lat_min)

    @property
    def lon_max(self) -> float:
        """Get the eastern longitude boundary of the area (degrees)."""
        return float(self._lon_max)

    @lon_max.setter
    def lon_max(self, lon_max):
        _check_longitude(lon_max)
        self._lon_max = Decimal(lon_max)

    @property
    def lon_min(self) -> float:
        """Get the western longitude boundary of the area (degrees)."""
        return float(self._lon_min)

    @lon_min.setter
    def lon_min(self, lon_min):
        _check_longitude(lon_min)
        if not lon_min <= self.lon_max:
            raise AttributeError(
                "The maximum longitude must be greater than or equal to "
                "the minimum longitude"
            )
        self._lon_min = Decimal(lon_min)

    def __repr__(self):
        """Print bounding box string."""
        return (
            f"N: {self.lat_max}\nS: {self.lat_min}\n"
            f"E: {self.lon_max}\nW: {self.lon_min}"
        )

    @classmethod
    def from_shape(
        cls, shape: Union[gpd.GeoSeries, gpd.GeoDataFrame]
    ) -> "GeoBoundingBox":
        """
        Create ``GeoBoundingBox`` from a geopandas object.

        Parameters
        ----------
        shape : geopandas.GeoSeries, geopandas.GeoDataFrame
            A shape whose bounds will be retrieved

        Returns
        -------
        ``GeoBoundingBox`` from the total bounds of the ``GeoDataFrame``

        Examples
        --------
        >>> import geopandas as gpd
        >>> df_admin_boundaries = gpd.read_file("admin0_boundaries.gpkg")
        >>> geobb = GeoBoundingBox.from_shape(df_admin_boundaries)
        """
        return cls(
            lat_max=shape.total_bounds[3],
            lat_min=shape.total_bounds[1],
            lon_max=shape.total_bounds[2],
            lon_min=shape.total_bounds[0],
        )

    def round_coords(
        self,
        offset_val: float = 0.0,
        round_val: Union[int, float] = 1,
    ) -> "GeoBoundingBox":
        """
        Round the bounding box coordinates.

        Rounding is always done outside the original bounding box,
        i.e. the resulting bounding box is always equal or larger
        than the original bounding box. Rounding can only be done once
        per instance.

        Parameters
        ----------
        offset_val : float, default = 0.0
            Offset the coordinates by this factor.
        round_val : int or float, default = 1
            Rounds to the nearest round_val. Can be an int for integer
            rounding or float for decimal rounding. If 1, round to integers.

        Returns
        -------
        ``GeoBoundingBox`` instance with rounded and offset coordinates
        """
        # TODO: add examples above
        new_coords = {}
        for direction in ["lat_max", "lon_min", "lat_min", "lon_max"]:
            coord = getattr(self, f"_{direction}")
            if direction in ("lat_max", "lon_max"):
                function = np.ceil.__call__  # needed for mypy
                offset_factor = Decimal(1)
            elif direction in ("lat_min", "lon_min"):
                function = np.floor.__call__  # needed for mypy
                offset_factor = Decimal(-1)
            rounded_coord = function(coord / Decimal(round_val)) * Decimal(
                round_val
            ) + offset_factor * Decimal(offset_val)
            new_coords[direction] = float(rounded_coord)
        return GeoBoundingBox(**new_coords)

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
            f"N{_str_format(self.lat_max)}S{_str_format(self.lat_min)}"
            f"E{_str_format(self.lon_max)}W{_str_format(self.lon_min)}"
            # replace the decimal dot with a d for a better filename
            .replace(".", "d")
        )


def _check_latitude(value):
    if not -90 <= value <= 90:
        raise AttributeError("Latitude must range from -90 to 90 degrees")


def _check_longitude(value):
    if not -180 <= value <= 180:
        raise AttributeError("Longitude must range from -180 to 180 degrees")
