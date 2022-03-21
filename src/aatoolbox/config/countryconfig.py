"""Country configuration setting base class."""
from pathlib import Path
from typing import Optional

from pydantic import BaseModel

from aatoolbox.utils.io import parse_yaml


class CodABConfig(BaseModel):
    """COD AB configuration.

    Parameters
    ----------
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

    hdx_dataset_name: str
    layer_base_name: str  # TODO: validate that it has {admin_level}
    admin_level_max: int
    custom_layer_names: Optional[list]


class FewsNetConfig(BaseModel):
    """FEWS NET configuration.

    Parameters
    ----------
    region_name: str
        Name of the region the country belongs to. Needed to download the
        regional FEWS NET data
    """

    region_name: str


class CountryConfig(BaseModel):
    """Country configuration."""

    iso3: str
    codab: CodABConfig
    fewsnet: Optional[FewsNetConfig]


def create_country_config(iso3: str) -> CountryConfig:
    """
    Return a country configuration object.

    Parameters
    ----------
    iso3 : str
        Country ISO3, must be exactly 3 characters long

    Returns
    -------
    CountryConfig instance
    """
    # TODO: validate iso3
    parameters = parse_yaml(
        Path(__file__).parent.resolve() / f"countries/{iso3}.yaml"
    )
    return CountryConfig(**parameters)
