"""Function for checking file existence."""
import logging
from typing import Any, Callable, Optional, TypeVar

import wrapt

from ochanticipy.datasources.datasource import DataSource

logger = logging.getLogger(__name__)

# For typing the decorator
F = TypeVar("F", bound=Callable[..., Any])


@wrapt.decorator
def check_file_existence(
    wrapped: F, instance: Optional[DataSource], args: list, kwargs: dict
) -> F:
    """
    Don't overwrite existing data.

    Avoid recreating data if it already exists and if clobber not
    toggled by user. Used to wrap functions that accept filepath
    as a keyword argument.

    Parameters
    ----------
    wrapped : function
        The function to wrap. The function must have "filepath" as
        a keyword parameter, and it can also have an optional
        "clobber" boolean keyword parameter.
    instance : Optional[DataSource]
        Object the wrapped function is bound to. Not used within, but
        ensures that instance methods do not pass `self` to args.
    args : list
        List of positional arguments.
    kwargs : dict
        Dictionary of keyword arguments

    Returns
    -------
    If filepath exists and clobber is False, returns filepath.
    Otherwise, returns the result of the decorated function.

    Raises
    ------
    KeyError
        If `filepath` or `clobber` are not passed as kwargs.
    """
    try:
        filepath = kwargs["filepath"]
        clobber = kwargs["clobber"]
    except KeyError as err:
        raise KeyError(
            (
                "`filepath` and `clobber` must be passed as keyword "
                "arguments for the `check_file_existence`"
                " decorator to work."
            )
        ) from err

    # check existence
    exist_dict = {True: "exists", False: "does not exist"}

    # check filepath exists -> clobber
    usage_dict = {
        True: {True: "overwriting existing", False: "using existing"},
        False: {True: "downloading new", False: "downloading new"},
    }
    fp_exists = filepath.exists()

    logger.info(
        f"File {filepath} {exist_dict[fp_exists]} and clobber "
        f"set to {clobber}, {usage_dict[fp_exists][clobber]} file."
    )

    if fp_exists and not clobber:
        return filepath
    else:
        return wrapped(*args, **kwargs)
