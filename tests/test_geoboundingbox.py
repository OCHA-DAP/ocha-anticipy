"""Tests for the CDS GeoBoundingBox module."""
from geopandas import GeoSeries
from shapely.geometry import Polygon

from aatoolbox.utils.geoboundingbox import GeoBoundingBox


def test_geoboundingbox_round_coords():
    """Test that coordinates are correctly rounded and offset."""
    geobb = GeoBoundingBox(north=1.05, south=-2.2, east=3.6, west=-4)
    geobb.round_boundingbox_coords(offset_val=0.4)
    assert geobb.north == 2.4
    assert geobb.south == -3.4
    assert geobb.east == 4.4
    assert geobb.west == -4.4


def test_geoboundingbox_filename():
    """Test that correct file string is returned."""
    geobb = GeoBoundingBox(north=1, south=-2, east=3, west=-4)
    assert geobb.get_filename_repr() == "Np1Sm2Ep3Wm4"


def test_geoboundingbox_from_shape():
    """Test that getting the geobb from a polygon works as expected."""
    n, s, e, w = (1, -2, 3, -4)
    shape = GeoSeries([Polygon([(e, n), (e, s), (w, s), (w, n)])])
    geobb = GeoBoundingBox.from_shape(shape)
    assert geobb.north == n
    assert geobb.south == s
    assert geobb.east == e
    assert geobb.west == w
