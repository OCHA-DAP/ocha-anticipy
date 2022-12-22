"""Function for I/O."""
import logging
import zipfile
from pathlib import Path
from typing import Union

import requests
import yaml

logger = logging.getLogger(__name__)


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
    # TODO: Use missing_ok=True once 3.7 is dropped
    if save_path.exists():
        save_path.unlink()

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
