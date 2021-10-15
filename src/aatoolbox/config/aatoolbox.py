"""Base class for aatoolbox data manipulation."""
from pathlib import Path

from aatoolbox.config.config import Config


class AaToolbox(object):
    """
    Base class object that contains path convenience functions.

    Parameters
    ----------
    iso3 : str
        Country ISO3
    module_base_dir : str
        Module directory name (usually correspond to data source)
    """

    def __init__(self, iso3: str, module_base_dir: str):

        self.config = Config(iso3)
        self.module_base_dir = module_base_dir

    def _get_base_dir(self, is_public=False, is_raw=False):
        public_dir = (
            self.config.path.public if is_public else self.config.path.private
        )
        raw_dir = (
            self.config.path.raw if is_raw else self.config.path.processed
        )
        return (
            self.config.path.base
            / public_dir
            / raw_dir
            / self.config.country.iso3
            / self.module_base_dir
        )

    def get_public_raw_base_dir(self) -> Path:
        """Get the data source public raw directory."""
        return self._get_base_dir(is_public=True, is_raw=True)
