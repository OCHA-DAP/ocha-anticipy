"""Test dates module."""

from datetime import date, datetime

import pytest

from ochanticipy.utils import dates


def test_get_date():
    """Tests getting date from string."""
    desired_date = date(year=2018, month=3, day=14)
    desired_str = "2018-03-14"
    desired_datetime = datetime(year=2018, month=3, day=14)
    assert dates.get_date_from_user_input(desired_str) == desired_date
    assert dates.get_date_from_user_input(desired_date) == desired_date
    assert dates.get_date_from_user_input(desired_datetime) == desired_datetime


def test_get_date_value_error():
    """Tests getting date errors."""
    with pytest.raises(ValueError):
        dates.get_date_from_user_input("2013-3-14")
    with pytest.raises(TypeError):
        dates.get_date_from_user_input(None)


def test_get_dekadal_date():
    """Tests get dekadal date."""
    desired_dekad = (2013, 8)
    desired_str = "2013-03-14"
    assert dates.get_dekadal_date(desired_str) == desired_dekad
    assert dates.get_dekadal_date(desired_dekad) == desired_dekad
    assert (
        dates.get_dekadal_date(date.fromisoformat(desired_str))
        == desired_dekad
    )
    assert (
        dates.get_dekadal_date(input_date=None, default_date=desired_dekad)
        == desired_dekad
    )


def test_get_dekadal_date_value_error():
    """Test that value error raised for bad inputs."""
    with pytest.raises(ValueError):
        dates.get_dekadal_date("2020-01-1")
    with pytest.raises(ValueError):
        dates.get_dekadal_date((20, 17))


def test_dekad_to_date():
    """Test conversion from dekad to date."""
    assert dates.dekad_to_date((2020, 1)) == date.fromisoformat("2020-01-01")
    assert dates.dekad_to_date((2016, 20)) == date.fromisoformat("2016-07-11")
    assert dates.dekad_to_date((2024, 36)) == date.fromisoformat("2024-12-21")


def test_date_to_dekad():
    """Test conversion from dekad to date."""
    assert dates.date_to_dekad(date.fromisoformat("2020-01-09")) == (2020, 1)
    assert dates.date_to_dekad(date.fromisoformat("2016-07-20")) == (2016, 20)
    assert dates.date_to_dekad(date.fromisoformat("2024-12-31")) == (2024, 36)


def test_compare_dekads():
    """Test comparing dekads."""
    dekad1 = (2019, 36)
    dekad2 = (2020, 1)
    assert dates.compare_dekads_gt(dekad2, dekad1)
    assert dates.compare_dekads_gte(dekad2, dekad1)
    assert dates.compare_dekads_lt(dekad1, dekad2)
    assert dates.compare_dekads_lte(dekad1, dekad2)
    assert dates.compare_dekads_gte(dekad2, dekad2)
    assert dates.compare_dekads_lte(dekad2, dekad2)


def test_expand_dekads():
    """Test expanding dekads."""
    dekad1 = (2019, 33)
    dekad2 = (2020, 3)
    dekads = dates.expand_dekads(dekad1, dekad2)
    assert len(dekads) == 7
    assert dekads[0] == dekad1
    assert dekads[6] == dekad2
    assert dekads[4] == (2020, 1)
    # error raised for reverse attempt
    with pytest.raises(ValueError):
        dates.expand_dekads(dekad2, dekad1)
