"""Country configuration setting base class."""
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from aatoolbox.utils.io import parse_yaml


class CodABConfig(BaseModel):
    """COD AB configuration.

    Parameters
    ----------
    hdx_address: str
        The page where the COD AB dataset resides on on HDX. Can be found
        by taking the portion of the url after ``data.humdata.org/dataset/``
    hdx_dataset_name: str
        COD AB dataset name on HDX. Can be found by taking the filename as it
        appears on the dataset page.
    layer_base_name: str
        The base name of the different admin layers, that presumably only
        change by a single custom_layer_number depending on the level. Should
        contain {admin_level} in place of the custom_layer_number.
    admin_level_max: int
        The maximum admin level available in the shapefile.
    custom_layer_names: list, optional
        Any additional layer names that don't fit into the admin level paradigm
    """

    hdx_address: str
    hdx_dataset_name: str
    layer_base_name: str  # TODO: validate that it has {admin_level}
    admin_level_max: int
    custom_layer_names: Optional[list]


class CountryConfig(BaseModel):
    """Country configuration."""

    iso3: str
    codab: CodABConfig


def get_country_config(iso3: str) -> CountryConfig:
    """
    Return a country configuration object.

    Parameters
    ----------
    iso3 : str
        Country ISO3, must e exactly 3 characters long

    Returns
    -------
    CountryConfig instance
    """
    # TODO: validate iso3
    parameters = parse_yaml(
        Path(__file__).parent.resolve() / f"countries/{iso3}.yaml"
    )
    return CountryConfig(**parameters)
