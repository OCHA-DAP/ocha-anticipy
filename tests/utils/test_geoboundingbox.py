"""Tests for the CDS GeoBoundingBox module."""
import pytest
from geopandas import GeoSeries
from shapely.geometry import Polygon

from ochanticipy.utils.geoboundingbox import GeoBoundingBox


def test_geoboundingbox_round_coords():
    """Test that coordinates are correctly rounded."""
    geobb = GeoBoundingBox(
        lat_max=1.5, lat_min=-2.2, lon_max=3.6, lon_min=-4.0
    ).round_coords(round_val=0.5)
    assert geobb.lat_max == 1.5
    assert geobb.lat_min == -2.5
    assert geobb.lon_max == 4.0
    assert geobb.lon_min == -4.0


def test_geoboundingbox_offset_coords():
    """Test that coordinates are correctly offset."""
    geobb = GeoBoundingBox(
        lat_max=1.05, lat_min=-2.2, lon_max=3.6, lon_min=-4.0
    ).round_coords(
        offset_val=0.4
    )  # default round val of 1
    assert geobb.lat_max == 2.4
    assert geobb.lat_min == -3.4
    assert geobb.lon_max == 4.4
    assert geobb.lon_min == -4.4


def test_geoboundingbox_round_and_offset_coords():
    """Test that coordinates are correctly rounded and offset."""
    geobb = GeoBoundingBox(
        lat_max=1.05, lat_min=-2.2, lon_max=3.6, lon_min=-4.0
    ).round_coords(round_val=0.1, offset_val=0.05)
    assert geobb.lat_max == 1.15
    assert geobb.lat_min == -2.25
    assert geobb.lon_max == 3.65
    assert geobb.lon_min == -4.05


def test_geoboundingbox_relative_coords():
    """Test that lat_max must be >= lat_min and lon_max >= lon_min."""
    with pytest.raises(AttributeError):
        # Check lat_min > lat_max
        GeoBoundingBox(lat_max=1, lat_min=2, lon_max=1, lon_min=-1)
        # Check lon_min > lon_max
        GeoBoundingBox(lat_max=1, lat_min=-1, lon_max=1, lon_min=2)
    # Check that no error raised:
    # Check lat_min == lat_max
    GeoBoundingBox(lat_max=1, lat_min=1, lon_max=1, lon_min=-1)
    # Check lon_min == lon_max
    GeoBoundingBox(lat_max=1, lat_min=-1, lon_max=1, lon_min=1)


def test_geoboundingbox_coordinate_bounds():
    """Test that coords are within limits."""
    with pytest.raises(AttributeError):
        GeoBoundingBox(lat_max=91, lat_min=0, lon_max=0, lon_min=0)
        GeoBoundingBox(lat_max=0, lat_min=-91, lon_max=0, lon_min=0)
        GeoBoundingBox(lat_max=0, lat_min=0, lon_max=181, lon_min=0)
        GeoBoundingBox(lat_max=0, lat_min=0, lon_max=0, lon_min=-181)


def test_geoboundingbox_filename():
    """Test that correct file string is returned."""
    geobb = GeoBoundingBox(lat_max=1, lat_min=-2, lon_max=3, lon_min=-4)
    assert geobb.get_filename_repr() == "Np1Sm2Ep3Wm4"


def test_geoboundingbox_from_shape():
    """Test that getting the geobb from a polygon works as expected."""
    n, s, e, w = (1, -2, 3, -4)
    shape = GeoSeries([Polygon([(e, n), (e, s), (w, s), (w, n)])])
    geobb = GeoBoundingBox.from_shape(shape)
    assert geobb.lat_max == n
    assert geobb.lat_min == s
    assert geobb.lon_max == e
    assert geobb.lon_min == w
