"""Base class for aatoolbox data manipulation."""
from pathlib import Path

from aatoolbox.config.pathconfig import PathConfig


class DataSource(object):
    """
    Base class object that contains path convenience functions.

    Parameters
    ----------
    iso3: str
        Country ISO3
    module_base_dir : str
        Module directory name (usually correspond to data source)
    """

    def __init__(self, iso3: str, module_base_dir: str):

        self.iso3 = iso3
        self.module_base_dir = module_base_dir
        self.path_config = PathConfig()

    def _get_base_dir(self, is_public=False, is_raw=False):
        public_dir = (
            self.path_config.public if is_public else self.path_config.private
        )
        raw_dir = (
            self.path_config.raw if is_raw else self.path_config.processed
        )
        return (
            self.path_config.base_path
            / public_dir
            / raw_dir
            / self.iso3
            / self.module_base_dir
        )

    def _get_public_raw_base_dir(self) -> Path:
        """Get the data source public raw directory."""
        return self._get_base_dir(is_public=True, is_raw=True)
