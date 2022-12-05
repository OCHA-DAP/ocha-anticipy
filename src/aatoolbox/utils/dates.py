"""Functions for dealing with dates."""

import itertools
from datetime import date
from typing import List, Tuple, Union, cast


def get_date_from_user_input(input_date: Union[date, str]) -> date:
    """Return date from string or date input.

    Processes input data in either ``datetime.date``
    format or as an ISO8601 string. Generates
    error message if different object provided.

    Parameters
    ----------
    input_date : Union[date, str]
        ``datetime.date`` object or ISO8601 string.

    Returns
    -------
    date
        ``datetime.date``
    """
    if isinstance(input_date, date):
        return input_date
    try:
        input_date = date.fromisoformat(input_date)
    except ValueError as err:
        raise ValueError(
            "`date` values passed as a string must follow ISO8601 date "
            "format: YYYY-MM-DD."
        ) from err
    except TypeError as err:
        raise TypeError(
            "`date` values must be either an ISO8601 string or "
            "`datetime.date` object."
        ) from err

    return input_date


def get_dekadal_date(
    input_date: Union[date, str, Tuple[int, int], None],
    default_date: Union[date, str, Tuple[int, int], None] = None,
) -> Tuple[int, int]:
    """Calculate dekadal date from general input.

    Processes input ``input_date`` and returns two
    values, the year and dekad. Input can be of
    format ``datetime.date``, an ISO8601 date
    string, an already calculated ``(year, dekad)``
    format date, or ``None``. If ``None``,
    ``default_date`` is returned. ``default_date``
    can also be passed in the above formats.
    """
    if input_date is None and default_date is not None:
        input_date = default_date

    # convert date to various values
    if not isinstance(input_date, (str, date)):
        input_tuple = cast(Tuple[int, int], input_date)
        if len(input_tuple) == 2:
            year, dekad = input_tuple
            # assert year-dekad values appropriate, not too strict
            if year < 1000 or year > 9999 or dekad < 1 or dekad > 36:
                raise ValueError(
                    f"(year, dekad) tuple ({year}, {dekad}) invalid. "
                    "Year should be a 4-digit year and dekad between "
                    "1 and 36."
                )

        else:
            raise ValueError(
                (
                    "`date` values for dekadal data "
                    "should be passed in as "
                    "`datetime.date` objects, tuples "
                    "of `(year, dekad)` format, or "
                    "ISO8601 date strings."
                )
            )

    else:
        input_as_date = get_date_from_user_input(input_date)
        year, dekad = date_to_dekad(input_as_date)

    return year, dekad


def dekad_to_date(dekad: Tuple[int, int]) -> date:
    """Compute date from dekad and year.

    Date computed from dekad and year in
    datetime object, corresponding to
    first day of the dekad. This
    is based on the
    `common dekadal definition
    <http://iridl.ldeo.columbia.edu/maproom/Food_Security/Locusts/Regional/Dekadal_Rainfall/index.html>`_
    of the 1st and 2nd dekad of a month
    being the first 10 day periods, and
    the 3rd dekad being the remaining
    days within that month.
    """
    year = dekad[0]
    month = ((dekad[1] - 1) // 3) + 1
    day = 10 * ((dekad[1] - 1) % 3) + 1
    return date(year=year, month=month, day=day)


def date_to_dekad(date_obj: date) -> Tuple[int, int]:
    """Compute dekad and year from date.

    Dekad computed from date. This
    is based on the
    `common dekadal definition
    <http://iridl.ldeo.columbia.edu/maproom/Food_Security/Locusts/Regional/Dekadal_Rainfall/index.html>`_
    of the 1st and 2nd dekad of a month
    being the first 10 day periods, and
    the 3rd dekad being the remaining
    days within that month.
    """
    year = date_obj.year
    dekad = min((date_obj.day - 1) // 10, 2) + ((date_obj.month - 1) * 3) + 1
    return (year, dekad)


def compare_dekads_lt(
    dekad1: Tuple[int, int], dekad2: Tuple[int, int]
) -> bool:
    """Is year1/dekad1 less than year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are less than the
    second pair.
    """
    y1, d1 = dekad1
    y2, d2 = dekad2
    return y1 < y2 or ((y1 == y2) and (d1 < d2))


def compare_dekads_lte(
    dekad1: Tuple[int, int], dekad2: Tuple[int, int]
) -> bool:
    """Is year1/dekad1 less than or equal to year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are less than or
    equal to the second pair.
    """
    y1, d1 = dekad1
    y2, d2 = dekad2
    return y1 < y2 or ((y1 == y2) and (d1 <= d2))


def compare_dekads_gt(
    dekad1: Tuple[int, int], dekad2: Tuple[int, int]
) -> bool:
    """Is year1/dekad1 greater than year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are greater than the
    second pair.
    """
    return compare_dekads_lt(dekad1=dekad2, dekad2=dekad1)


def compare_dekads_gte(
    dekad1: Tuple[int, int], dekad2: Tuple[int, int]
) -> bool:
    """Is year1/dekad1 greater than or equal to year2/dekad2.

    Compare two pairs of years and dekads,
    that the first pair are greater than or
    equal to the second pair.
    """
    return compare_dekads_lte(dekad1=dekad2, dekad2=dekad1)


def expand_dekads(
    dekad1: Tuple[int, int], dekad2: Tuple[int, int]
) -> List[Tuple[int, int]]:
    """Expand for all years/dekads between two dates.

    Takes input year and dekads and returns a list
    of year/dekad lists.
    """
    if compare_dekads_gt(dekad1, dekad2):
        raise ValueError("`dekad1` must be less than or equal to `dekad2`.")

    y1, d1 = dekad1
    y2, d2 = dekad2
    year_range = range(y1, y2 + 1)
    dekad_range = range(1, 37)
    date_combos = itertools.product(*[year_range, dekad_range])

    def valid(y, d):
        return not ((y == y1 and d < d1) or (y == y2 and d > d2))

    return [(y, d) for y, d in date_combos if valid(y, d)]
