"""Main runtime configuration."""
import os
from pathlib import Path

from aatoolbox.config import country


class Config(object):
    """
    Base configuration object.

    Parameters
    ----------
    iso3 : str
        Country ISO3, must be exactly 3 characters long
    """

    def __init__(self, iso3: str):
        self.country = country.CountryConfig(iso3)
        self.path = _PathConfig()


class _PathConfig(object):
    """Global directory parameters."""

    BASE_DIR_ENV = "AA_DATA_DIR"

    def __init__(self):
        self.base = Path(os.environ[_PathConfig.BASE_DIR_ENV])
        self.public = "public"
        self.private = "private"
        self.raw = "raw"
        self.processed = "processed"
