"""Country configuration setting base class."""
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, root_validator, validator

from ochanticipy.utils.io import parse_yaml


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
         The maximum admin level available in the shapefile, cannot be
         greater than 4
    admin{level}_name: str, optional
        The names of any admin level layers that do not conform to the
        layer_base_name pattern, where {level} ranges from 0 to 4
    custom_layer_names: list, optional
        Any additional layer names that don't fit into the admin level paradigm
    """

    hdx_dataset_name: str
    layer_base_name: str
    admin_level_max: int
    admin0_name: Optional[str]
    admin1_name: Optional[str]
    admin2_name: Optional[str]
    admin3_name: Optional[str]
    admin4_name: Optional[str]
    custom_layer_names: Optional[list]

    @validator("layer_base_name")
    def _validate_layer_base_name(cls, layer_base_name):
        """Ensure that the layer basename contains {admin_level}."""
        if "{admin_level}" not in layer_base_name:
            raise ValueError(
                "In the COD AB section of the country configuration file, "
                "layer_base_name must contain an {admin_level} placeholder."
            )
        return layer_base_name

    @validator("admin_level_max")
    def _validate_admin_level_max(cls, admin_level_max):
        """Ensure that admin_level_max is between 0 and 4."""
        if not 0 <= admin_level_max <= 4:
            raise ValueError(
                "In the COD AB section of the country configuration file, "
                "admin_level_max must be between 0 and 4."
            )
        return admin_level_max

    @root_validator(pre=False, skip_on_failure=True)
    def _set_admin_levels(cls, values) -> dict:
        """Set admin levels names using layer base name."""
        for admin_level in range(values["admin_level_max"] + 1):
            if values.get(f"admin{admin_level}_name", None) is None:
                values[f"admin{admin_level}_name"] = values[
                    "layer_base_name"
                ].format(admin_level=admin_level)
        return values


class FewsNetConfig(BaseModel):
    """FEWS NET configuration.

    Parameters
    ----------
    region_name: str
        Name of the region the country belongs to. Needed to download the
        regional FEWS NET data
    """

    # values dictionary gets build in order attributes are listed
    # so first define the dict before the region_name
    region_name_code_mapping: Dict[str, str] = {
        "caribbean-central-america": "LAC",
        "central-asia": "CA",
        "east-africa": "EA",
        "southern-africa": "SA",
        "west-africa": "WA",
    }
    region_name: str

    @validator("region_name")
    def regionname_valid(cls, v, values):
        """Check that regionname is one of the valid ones."""
        valid_regionnames = values["region_name_code_mapping"].keys()
        if v not in valid_regionnames:
            raise ValueError(
                f"Invalid region name: {v}. "
                f"Should be one of {', '.join(valid_regionnames)}"
            )
        return v

    @root_validator(pre=False, skip_on_failure=True)
    def _set_region_code(cls, values) -> dict:
        """Set region code based on region name."""
        values["region_code"] = values["region_name_code_mapping"][
            values["region_name"]
        ]
        return values


class ReportingPoints(BaseModel):
    """Coordinates of GloFAS reporting points."""

    name: str
    lon: float
    lat: float


class GlofasConfig(BaseModel):
    """GloFAS configuration."""

    reporting_points: List[ReportingPoints]


class UsgsNdviConfig(BaseModel):
    """USGS NDVI configuration.

    Parameters
    ----------
    area_name : str
        Name of the USGS NDVI coverage area the country belongs to.
        Needed to download the regional NDVI data.
    """

    area_name_mapping: Dict[str, Tuple[str, str]] = {
        "north-africa": ("africa/north", "na"),
        "east-africa": ("africa/east", "ea"),
        "southern-africa": ("africa/southern", "sa"),
        "west-africa": ("africa/west", "wa"),
        "central-asia": ("asia/centralasia", "cta"),
        "yemen": ("asia/middleeast/yemen", "yem"),
        "central-america": ("lac/camcar/centralamerica", "ca"),
        "hispaniola": ("lac/camcar/caribbean/hispaniola", "hi"),
    }
    area_name: str

    @validator("area_name")
    def area_name_valid(cls, v, values) -> str:
        """Check that area_name is valid."""
        valid_area_names = values["area_name_mapping"].keys()
        if v not in valid_area_names:
            raise ValueError(
                f"Invalid area name: {v}. "
                f"Should be one of {', '.join(valid_area_names)}"
            )
        return v

    @root_validator(pre=False, skip_on_failure=True)
    def _set_area_codes(cls, values) -> dict:
        """Set NDVI url and prefix from area."""
        values["area_url"], values["area_prefix"] = values[
            "area_name_mapping"
        ][values["area_name"]]
        return values


class CountryConfig(BaseModel):
    """Country configuration.

    Parameters
    ----------
    iso3 : str
        Country ISO3, must be exactly 3 letters long
    codab: CodABConfig, optional
        Configuration object for COD AB
    fewsnet: FewsNetConfig, optional
        Configuration object for FEWS NET
    glofas: GlofasConfig, optional
        Configuration object for GloFAS
    usgs_ndvi: UsgsNdviConfig, optional
        Configuration object for USGS NDVI
    """

    iso3: str
    codab: Optional[CodABConfig]
    fewsnet: Optional[FewsNetConfig]
    glofas: Optional[GlofasConfig]
    usgs_ndvi: Optional[UsgsNdviConfig]

    @validator("iso3")
    def _validate_iso3(cls, iso3):
        """Ensure ISO3 is length three and alphabet chars only."""
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
    """Ensure ISO3 is length three and alphabet chars only."""
    if len(iso3) != 3 or not str.isalpha(iso3):
        raise ValueError("ISO3 must be a three letter string.")
    return iso3.lower()
