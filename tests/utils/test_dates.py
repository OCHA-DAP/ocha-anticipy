"""Test dates module."""

from datetime import date

import pytest

from aatoolbox.utils import _dates


def test_get_dekadal_date():
    """Tests get dekadal date."""
    desired_dekad = (2013, 8)
    desired_str = "2013-03-14"
    assert _dates.get_dekadal_date(desired_str) == desired_dekad
    assert _dates.get_dekadal_date(desired_dekad) == desired_dekad
    assert (
        _dates.get_dekadal_date(date.fromisoformat(desired_str))
        == desired_dekad
    )
    assert (
        _dates.get_dekadal_date(input_date=None, default_date=desired_dekad)
        == desired_dekad
    )


def test_get_dekadal_date_value_error():
    """Test that value error raised for bad inputs."""
    with pytest.raises(ValueError):
        _dates.get_dekadal_date("2020-01-1")
    with pytest.raises(ValueError):
        _dates.get_dekadal_date((20, 17))


def test_dekad_to_date():
    """Test conversion from dekad to date."""
    assert _dates.dekad_to_date((2020, 1)) == date.fromisoformat("2020-01-01")
    assert _dates.dekad_to_date((2016, 20)) == date.fromisoformat("2016-07-11")
    assert _dates.dekad_to_date((2024, 36)) == date.fromisoformat("2024-12-21")


def test_date_to_dekad():
    """Test conversion from dekad to date."""
    assert _dates.date_to_dekad(date.fromisoformat("2020-01-09")) == (2020, 1)
    assert _dates.date_to_dekad(date.fromisoformat("2016-07-20")) == (2016, 20)
    assert _dates.date_to_dekad(date.fromisoformat("2024-12-31")) == (2024, 36)


def test_compare_dekads():
    """Test comparing dekads."""
    dekad1 = (2019, 36)
    dekad2 = (2020, 1)
    assert _dates.compare_dekads_gt(dekad2, dekad1)
    assert _dates.compare_dekads_gte(dekad2, dekad1)
    assert _dates.compare_dekads_lt(dekad1, dekad2)
    assert _dates.compare_dekads_lte(dekad1, dekad2)
    assert _dates.compare_dekads_gte(dekad2, dekad2)
    assert _dates.compare_dekads_lte(dekad2, dekad2)


def test_expand_dekads():
    """Test expanding dekads."""
    dekad1 = (2019, 33)
    dekad2 = (2020, 3)
    dekads = _dates.expand_dekads(dekad1, dekad2)
    assert len(dekads) == 7
    assert dekads[0] == dekad1
    assert dekads[6] == dekad2
    assert dekads[4] == (2020, 1)
    # error raised for reverse attempt
    with pytest.raises(ValueError):
        _dates.expand_dekads(dekad2, dekad1)
