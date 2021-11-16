"""Function for I/O."""
import logging
from pathlib import Path
from typing import Any, Callable, TypeVar, Union, cast

import yaml

logger = logging.getLogger(__name__)

# For typing the decorator
F = TypeVar("F", bound=Callable[..., Any])


def parse_yaml(filename: Union[str, Path]) -> dict:
    """
    Read in a yaml file.

    Parameters
    ----------
    filename : str, Path
    The full filepath of the YAML file

    Returns
    -------
    A dictionary with the YAML file contents
    """
    with open(file=filename, mode="r") as stream:
        config = yaml.safe_load(stream)
    return config


def check_file_existence(func: F) -> F:
    """
    Don't overwrite existing data.

    Avoid recreating data if it already exists and if clobber not
    toggled by user. Can be used to wrap a function whose first parameter
    is a filepath.

    Parameters
    ----------
    func : function
        The function to wrap. The first parameter of this function must
        be the filepath (of type Path), and it can also have an optional
        "clobber" boolean keyword parameter.

    Returns
    -------
    If filepath exists, returns filepath. Otherwise, returns the result of
    the decorated function.

    """

    def wrapper(filepath: Path, *args, **kwargs):
        if filepath.exists() and not kwargs.get("clobber", False):
            logger.info(
                f"File {filepath} exists and clobber set to False, "
                f"using existing files"
            )
            return filepath
        return func(filepath, *args, **kwargs)

    return cast(F, wrapper)
