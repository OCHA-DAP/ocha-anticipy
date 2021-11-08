"""Country configuration setting base class."""
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from aatoolbox.utils.io import parse_yaml


class _CodAdmin(BaseModel):
    layer_name: str


class _CodABConfig(BaseModel):
    hdx_address: str
    hdx_dataset_name: str
    admin0: _CodAdmin
    admin1: _CodAdmin
    admin2: _CodAdmin
    admin3: Optional[_CodAdmin]
    admin4: Optional[_CodAdmin]


class CountryConfig(BaseModel):
    """Country configuration."""

    iso3: str
    codab: _CodABConfig


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
    # TODO: validate _iso3
    parameters = parse_yaml(
        Path(__file__).parent.resolve() / f"countries/{iso3}.yaml"
    )
    return CountryConfig(**parameters)
