"""Create geographic area for input to the CDS API.

The API requires the coordinate parameters to be in a specific format.

To download non-interpolated data, the coordinates should be:
* ECMWF: rounded to the nearest degree
* GloFAS: Rounded to the nearest degree and offset by 0.05 degrees

It is possible to create an ``Area`` object either from a dictionary of
stations, or from a shapefile that has been read in with geopandas.

The ``Area`` object should then be passed to the CDS ``download`` method.
"""
from collections import namedtuple
from typing import Dict, List

from aatoolbox.utils.area import Area

Station = namedtuple("Station", "lon lat")

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


def area_list_for_api(
    area: Area,
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
    if round_coords:
        area.round_area_coords(offset_val=offset_val)
    return [area.north, area.west, area.south, area.east]


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
