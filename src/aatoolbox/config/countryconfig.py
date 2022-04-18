"""Country configuration setting base class."""
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, validator

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
    layer_base_name: str
    admin_level_max: int
    custom_layer_names: Optional[list]

    @validator("layer_base_name")
    def _validate_layer_base_name(cls, layer_base_name):
        if "{admin_level}" not in layer_base_name:
            raise ValueError(
                "The layer base name must contain "
                "an {admin_level} placeholder."
            )
        return layer_base_name


class CountryConfig(BaseModel):
    """Country configuration."""

    iso3: str
    codab: Optional[CodABConfig]

    @validator("iso3")
    def _validate_iso3(cls, iso3):
        return _validate_iso3(iso3)


def create_country_config(iso3: str) -> CountryConfig:
    """
    Return a country configuration object from AA Toolbox.

    Parameters
    ----------
    iso3 : str
        Country ISO3, must be exactly 3 letters long

    Returns
    -------
    CountryConfig instance
    """
    iso3 = _validate_iso3(iso3)
    try:
        parameters = parse_yaml(
            Path(__file__).parent.resolve() / f"countries/{iso3}.yaml"
        )
    except FileNotFoundError as err:
        raise FileNotFoundError(
            f"A configuration file for {iso3.upper()} is not yet available "
            f"in AA Toolbox. Try using a custom configuration file with "
            f"create_custom_country_config instead, or contact us to "
            f"request that we add this country."
        ) from err
    return CountryConfig(**parameters)


def create_custom_country_config(filepath: Union[str, Path]) -> CountryConfig:
    """
    Return a custom country configuration object.

    Parameters
    ----------
    filepath: str, pathlib.Path
        Path to the configuration file

    Returns
    -------
    CountryConfig instance
    """
    return CountryConfig(**parse_yaml(filepath))


def _validate_iso3(iso3: str):
    if len(iso3) != 3 or not str.isalpha(iso3):
        raise ValueError("ISO3 must be a three letter string.")
    return iso3.lower()
