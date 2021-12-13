"""Tests for the CDS GeographicBoundingBox module."""
from geopandas import GeoSeries
from shapely.geometry import Polygon

from aatoolbox.utils.geoboundingbox import (
    GeographicBoundingBox,
    GeographicBoundingBoxFromShape,
)


def test_geoboundingbox_round_coords():
    """Test that coordinates are correctly rounded and offset."""
    area = GeographicBoundingBox(north=1.05, south=-2.2, east=3.6, west=-4)
    area.round_area_coords(offset_val=0.4)
    assert area.north == 2.4
    assert area.south == -3.4
    assert area.east == 4.4
    assert area.west == -4.4


def test_geoboundingbox_filename():
    """Test that correct file string is returned."""
    area = GeographicBoundingBox(north=1, south=-2, east=3, west=-4)
    assert area.get_filename_repr() == "Np1Sm2Ep3Wm4"


def test_get_geoboundingbox_from_shape():
    """Test that getting the area from a polygon works as expected."""
    n, s, e, w = (1, -2, 3, -4)
    shape = GeoSeries([Polygon([(e, n), (e, s), (w, s), (w, n)])])
    area = GeographicBoundingBoxFromShape(shape)
    assert area.north == n
    assert area.south == s
    assert area.east == e
    assert area.west == w
