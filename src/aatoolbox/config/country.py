"""Country configuration setting base class."""
from pathlib import Path

from aatoolbox.utils.io import parse_yaml


class _CodAdmin(object):
    def __init__(self, base_layer: str = None):
        self.base_layer = base_layer


class _CodABConfig(object):
    def __init__(self, base_dir: str = None, base_zip: str = None, **kwargs):
        self.base_dir = base_dir
        self.base_zip = base_zip
        self.admin0 = _CodAdmin(**kwargs.get("admin0", {}))
        self.admin1 = _CodAdmin(**kwargs.get("admin1", {}))


class CountryConfig(object):
    """
    Country-specific configuration information, read in from $ISO3.yaml.

    Parameters
    ----------
    iso3 : str
        Country ISO3, must be exactly 3 characters long
    """

    def __init__(self, iso3: str):
        # TODO: validate
        self.iso3 = iso3.lower()
        # TODO: Also check working directory + some env variable for
        #  country config
        # Get directory of this file + the config filename
        parameters = parse_yaml(
            Path(__file__).parent.resolve() / f"countries/{iso3}.yaml"
        )
        self.codab = _CodABConfig(**parameters.get("codab", {}))
