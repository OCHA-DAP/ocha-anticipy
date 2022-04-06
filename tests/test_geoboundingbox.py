"""Tests for the CDS GeoBoundingBox module."""
import pytest
from geopandas import GeoSeries
from shapely.geometry import Polygon

from aatoolbox.utils.geoboundingbox import GeoBoundingBox


def test_geoboundingbox_round_coords():
    """Test that coordinates are correctly rounded."""
    geobb = GeoBoundingBox(
        north=1.5, south=-2.2, east=3.6, west=-4.0
    ).round_coords(round_val=0.5)
    assert geobb.north == 1.5
    assert geobb.south == -2.5
    assert geobb.east == 4.0
    assert geobb.west == -4.0


def test_geoboundingbox_offset_coords():
    """Test that coordinates are correctly offset."""
    geobb = GeoBoundingBox(
        north=1.05, south=-2.2, east=3.6, west=-4.0
    ).round_coords(
        offset_val=0.4
    )  # default round val of 1
    assert geobb.north == 2.4
    assert geobb.south == -3.4
    assert geobb.east == 4.4
    assert geobb.west == -4.4


def test_geoboundingbox_round_and_offset_coords():
    """Test that coordinates are correctly rounded and offset."""
    geobb = GeoBoundingBox(
        north=1.05, south=-2.2, east=3.6, west=-4.0
    ).round_coords(round_val=0.1, offset_val=0.05)
    assert geobb.north == 1.15
    assert geobb.south == -2.25
    assert geobb.east == 3.65
    assert geobb.west == -4.05


def test_geoboundingbox_relative_coords():
    """Test that north must be > south and east > west."""
    with pytest.raises(AttributeError):
        # Check south > north
        GeoBoundingBox(north=1, south=2, east=1, west=-1)
        # Check south == north
        GeoBoundingBox(north=1, south=1, east=1, west=-1)
        # Check west > east
        GeoBoundingBox(north=1, south=-1, east=1, west=2)
        # Check west == east
        GeoBoundingBox(north=1, south=-1, east=1, west=1)


def test_geoboundingbox_coordinate_bounds():
    """Test that coords are within limits."""
    with pytest.raises(AttributeError):
        GeoBoundingBox(north=91, south=0, east=0, west=0)
        GeoBoundingBox(north=0, south=-91, east=0, west=0)
        GeoBoundingBox(north=0, south=0, east=181, west=0)
        GeoBoundingBox(north=0, south=0, east=0, west=-181)


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
