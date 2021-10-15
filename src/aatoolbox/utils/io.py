"""Function for I/O."""
from pathlib import Path
from typing import Union

import yaml


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
    with open(filename, "r") as stream:
        config = yaml.safe_load(stream)
    return config
