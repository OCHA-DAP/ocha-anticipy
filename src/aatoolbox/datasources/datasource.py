"""Base class for aatoolbox data source."""
from pathlib import Path

from aatoolbox.config.pathconfig import PathConfig


class DataSource:
    """
    Base class object that contains path convenience functions.

    Parameters
    ----------
    iso3: str
        Country ISO3
    module_base_dir : str
        Module directory name (usually correspond to data source)
    is_public: bool, default = False
        Whether the dataset is public or private. Determines top-level
        directory structure.
    """

    def __init__(
        self, iso3: str, module_base_dir: str, is_public: bool = False
    ):

        self._iso3 = iso3
        self._module_base_dir = module_base_dir
        self._path_config = PathConfig()
        self._raw_base_dir = self._get_base_dir(
            is_public=is_public, is_raw=True
        )
        self._processed_base_dir = self._get_base_dir(
            is_public=is_public, is_raw=False
        )

    def _get_base_dir(self, is_public: bool, is_raw: bool) -> Path:
        permission_dir = (
            self._path_config.public
            if is_public
            else self._path_config.private
        )
        state_dir = (
            self._path_config.raw if is_raw else self._path_config.processed
        )
        return (
            self._path_config.base_path
            / permission_dir
            / state_dir
            / self._iso3
            / self._module_base_dir
        )
