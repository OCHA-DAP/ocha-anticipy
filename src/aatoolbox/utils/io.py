"""Function for I/O."""
import logging
import zipfile
from pathlib import Path
from typing import Any, Callable, TypeVar, Union

import requests
import wrapt
import yaml

logger = logging.getLogger(__name__)

# For typing the decorator
F = TypeVar("F", bound=Callable[..., Any])


def download_url(
    url: str,
    save_path: Path,
    chunk_size: int = 2048,
):
    """
    Download the file located at `url` to `save_path`.

    Parameters
    ----------
    url : str
        url that contains the file to be downloaded
    save_path : Path
        path to the location the file should be saved
    chunk_size : int
        number of bytes to save at once
    """
    save_path.parent.mkdir(exist_ok=True, parents=True)
    # Remove file if already exists
    save_path.unlink(missing_ok=True)

    # use a session and chunk_size to prevent
    # crashing when downloading large files while
    # not loosing too much speed
    session = requests.Session()
    r = session.get(url, stream=True)
    r.raise_for_status()
    with save_path.open("wb") as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def unzip(
    zip_file_path: Path,
    save_dir: Path,
):
    """
    Unzip a file.

    Parameters
    ----------
    zip_file_path : Path
        path to the location the zip file is saved
    save_dir : Path
        dir path to which the content of the zip
        file should be saved
    """
    with zipfile.ZipFile(file=zip_file_path, mode="r") as zip_ref:
        zip_ref.extractall(save_dir)


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


@wrapt.decorator
def check_file_existence(wrapped: F, instance, args, kwargs) -> F:
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
    instance :
        Object the wrapped function is bound to. Not used within, but
        ensures that instance methods do not pass `self` to args.
    args :
        List of positional arguments.
    kwargs :
        Dictionary of keyword arguments

    Returns
    -------
    If filepath exists, returns filepath. Otherwise, returns the result of
    the decorated function.
    """
    filepath = kwargs.get("filepath")
    if filepath is None:
        raise KeyError(
            (
                "`filepath` must be passed as a keyword "
                "argument for the `check_file_existence`"
                " decorator to work."
            )
        )
    if filepath.exists() and not kwargs.get("clobber", False):
        logger.info(
            f"File {filepath} exists and clobber set to False, "
            f"using existing files"
        )
        return filepath
    else:
        return wrapped(*args, **kwargs)
