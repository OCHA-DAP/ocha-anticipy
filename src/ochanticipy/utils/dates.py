"""Functions for dealing with dates."""

from datetime import date
from typing import List, Tuple, Union, cast, overload

from kalendar import Dekad, Pentad


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


@overload
def get_kalendar_date(
    kalendar_class: Dekad,
    input_date: Union[date, str, Tuple[int, int], Dekad, None],
    default_date: Union[date, str, Tuple[int, int], Dekad, None] = None,
) -> Dekad:
    ...


@overload
def get_kalendar_date(  # type: ignore
    kalendar_class: Pentad,
    input_date: Union[date, str, Tuple[int, int], Pentad, None],
    default_date: Union[date, str, Tuple[int, int], Pentad, None] = None,
) -> Pentad:
    ...


def get_kalendar_date(
    kalendar_class: Union[Dekad, Pentad],
    input_date: Union[date, str, Tuple[int, int], Dekad, Pentad, None],
    default_date: Union[
        date, str, Tuple[int, int], Dekad, Pentad, None
    ] = None,
) -> Union[Dekad, Pentad]:
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

    if isinstance(input_date, kalendar_class):
        return input_date
    if isinstance(input_date, date):
        return kalendar_class.fromdate(input_date)
    if isinstance(input_date, str):
        return kalendar_class.fromisoformat(input_date)
    else:
        input_tuple = cast(Tuple[int, int], input_date)
        try:
            return Dekad(*input_tuple)
        except (ValueError, TypeError) as e:
            raise ValueError(
                "`date` values for dekad or pentad date "
                "should be passed in as "
                "`datetime.date` or `kalendar.Dekad/Pentad` "
                "objects, tuples "
                "of `(year, dekad)` format, or "
                "ISO8601 date strings."
            ) from e


@overload
def kalendar_range(x: Dekad, y: Dekad) -> List[Dekad]:
    ...


@overload
def kalendar_range(x: Pentad, y: Pentad) -> List[Pentad]:  # type: ignore
    ...


def kalendar_range(
    x: Union[Dekad, Pentad], y: Union[Dekad, Pentad]
) -> List[Union[Dekad, Pentad]]:
    """Expand between dekads and pentads.

    Takes input dekad or pentad and returns a list
    of dekads/pentads.
    """
    return [x + i for i in range(y - x + 1)]
