"""Functions for dealing with dates."""

from datetime import date, datetime
from typing import Union


def _dekad_to_date(year: int, dekad: int):
    """Compute date from dekad and year.

    Date computed from dekad and year in
    datetime object, corresponding to
    first day of the dekad. This
    is based on the USGS (and relatively
    common) dekadal definition of the
    1st and 2nd dekad of a month being
    the first 10 day periods, and the 3rd
    dekad being the remaining days within
    that month.
    """
    month = ((dekad - 1) // 3) + 1
    day = 10 * ((dekad - 1) % 3) + 1
    return datetime(year=year, month=month, day=day)


def _date_to_dekad(date_obj: Union[date, str]):
    """Compute dekad and year from date.

    Dekad computed from date. This
    is based on the USGS (and relatively
    common) dekadal definition of the
    1st and 2nd dekad of a month being
    the first 10 day periods, and the 3rd
    dekad being the remaining days within
    that month.
    """
    if isinstance(date_obj, str):
        date_obj = date.fromisoformat(date_obj)

    year = date_obj.year
    dekad = (date_obj.day // 10) + ((date_obj.month - 1) * 3) + 1
    return year, dekad


def _compare_dekads_lt(y1: int, d1: int, y2: int, d2: int):
    """Is year1/dekad1 less than year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are less than the
    second pair.
    """
    return y1 < y2 or ((y1 == y2) and (d1 < d2))


def _compare_dekads_lte(y1: int, d1: int, y2: int, d2: int):
    """Is year1/dekad1 less than or equal to year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are less than or
    equal to the second pair.
    """
    return y1 < y2 or ((y1 == y2) and (d1 <= d2))


def _compare_dekads_gt(y1: int, d1: int, y2: int, d2: int):
    """Is year1/dekad1 greater than year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are greater than the
    second pair.
    """
    return _compare_dekads_lt(y1=y2, d1=d2, y2=y1, d2=d1)


def _compare_dekads_gte(y1: int, d1: int, y2: int, d2: int):
    """Is year1/dekad1 greater than or equal to year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are greater than or
    equal to the second pair.
    """
    return _compare_dekads_lte(y1=y2, d1=d2, y2=y1, d2=d1)
