"""Tests for the CDS Area module."""
from geopandas import GeoSeries
from shapely.geometry import Polygon

from aatoolbox.datasources.cds.area import (
    AreaFromShape,
    AreaFromStations,
    Station,
)

FAKE_STATIONS = {
    "station_north": Station(lon=0, lat=1),
    "station_south": Station(lon=0, lat=-2),
    "station_east": Station(lon=3, lat=0),
    "station_west": Station(lon=-4, lat=0),
}


def test_get_area_from_stations():
    """Test that the buffer around area from stations."""
    area = AreaFromStations(FAKE_STATIONS, buffer=0.1)
    assert area.north == 1.1
    assert area.south == -2.1
    assert area.east == 3.1
    assert area.west == -4.1


def test_get_list_for_api():
    """Test that the output list is in the correct order."""
    area = AreaFromStations(FAKE_STATIONS, buffer=0)
    assert area.list_for_api(offset_val=0.05) == [
        1.05,
        -4.05,
        -2.05,
        3.05,
    ]


def test_get_area_from_shape():
    """Test that getting the area from a polygon works as expected."""
    n, s, e, w = (1, -2, 3, -4)
    shape = GeoSeries([Polygon([(e, n), (e, s), (w, s), (w, n)])])
    area = AreaFromShape(shape)
    assert area.north == n
    assert area.south == s
    assert area.east == e
    assert area.west == w
