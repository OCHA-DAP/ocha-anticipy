"""Geographic area for the CDS API.

Create geographic area for input to the CDS API, which requires the
coordinate parameters to be in a specific format:

* ECMWF: rounded to the nearest degree
* GloFAS: Rounded to the nearest degree and offset by 0.05 degrees

It is possible to create an ``Area`` object either from a dictionary of
stations, or from a shapefile that has been read in with geopandas.

The ``Area`` object should then be passed to the CDS ``download`` method.
"""
from collections import namedtuple
from typing import Dict, List, Union

import geopandas as gpd
import numpy as np

Station = namedtuple("Station", "lon lat")  # noqa: FKA01

Station.__doc__ = """
    The coordinates of a station, used to determine the boundaries of the
    area passed to the CDS API.

    Parameters
    ----------
    lon: float
        The station longitude in degrees
    lat: float
        The station latitude in degrees
"""


class Area:
    """Create a geographic formatted for input to the CDS API.

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

    def list_for_api(
        self,
        round_coords: bool = True,
        offset_val: float = 0.0,
    ) -> List[float]:
        """
        List the coordinates in the order that they're needed for the API.

        Parameters
        ----------
        round_coords: bool, default = False
            Whether or not to round the coordinates. In general this should be
            done, but this option has been left in for some legacy data.
        offset_val: float, default = 0.0
            Offset the coordinates by this factor. Some CDS datasets require
            coordinates that end in 0.5.

        Returns
        -------
        List of coordinates in the correct order for the API (north, west,
        south, east)
        """
        if not round_coords:
            return [self.north, self.west, self.south, self.east]

        return [
            self._round_coord(
                coord=getattr(self, coordinate),  # noqa: FKA01
                direction=direction,
                offset_val=offset_val,
            )
            for coordinate, direction in {
                "north": "up",
                "west": "down",
                "south": "down",
                "east": "up",
            }.items()
        ]

    @staticmethod
    def _round_coord(
        coord: float,
        direction: str,
        offset_val: float = 0.0,
        round_val: int = 1,
    ) -> float:
        if direction == "up":
            function = np.ceil
            offset_factor = 1
        elif direction == "down":
            function = np.floor
            offset_factor = -1
        return (
            function(coord / round_val) * round_val
            + offset_factor * offset_val
        )


class AreaFromStations(Area):
    """
    Create an area input for the CDS API based on station coordinates.

    Parameters
    ----------
    stations : dict
        dictionary of the form ``{station_name: Station}``
    buffer : float, default = 0.2
        degrees above / below the maximum station lat / lon used
        to query the CDS API

    Examples
    --------
    >>> stations = {"station1": Station(lon=1.0, lat=2.0),
    >>>             "station2": Station(lon=3.0, lat=4.0)}
    >>> area = AreaFromStations(stations)
    """

    def __init__(self, stations: Dict[str, Station], buffer: float = 0.2):
        lon_list = [station.lon for station in stations.values()]
        lat_list = [station.lat for station in stations.values()]
        super().__init__(
            north=max(lat_list) + buffer,
            south=min(lat_list) - buffer,
            east=max(lon_list) + buffer,
            west=min(lon_list) - buffer,
        )


class AreaFromShape(Area):
    """
    Create an area input for the CDS API based on a shapefile.

    Parameters
    ----------
    shape : geopandas.GeoSeries, geopandas.GeoDataFrame
        A shape whose bounds will be used to generate the area
        queried by the CDS API

    Examples
    --------
    >>> import geopandas as gpd
    >>> df_admin_boundaries = gpd.read_file("admin0_boundaries.gpkg")
    >>> area = AreaFromShape(df_admin_boundaries.iloc[0].geometry)
    """

    def __init__(self, shape: Union[gpd.GeoSeries, gpd.GeoDataFrame]):
        super().__init__(
            north=shape.total_bounds[3],
            south=shape.total_bounds[1],
            east=shape.total_bounds[2],
            west=shape.total_bounds[0],
        )
