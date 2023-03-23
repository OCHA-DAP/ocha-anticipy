"""Test dates module."""

from datetime import date, datetime

import pytest
from kalendar import Dekad, Pentad

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
    desired_dekad = Dekad(2013, 8)
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
        dates.get_dekadal_date((2017, 37))


def test_kalendar_range():
    """Test expanding dekads."""
    pentad1 = Pentad(2019, 70)
    pentad2 = Pentad(2020, 3)
    pentads = dates.kalendar_range(x=pentad1, y=pentad2)
    assert len(pentads) == 7
    assert pentads[0] == pentad1
    assert pentads[6] == pentad2
    assert pentads[4] == Pentad(2020, 1)
