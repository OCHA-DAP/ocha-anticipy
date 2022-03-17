"""Base class for aatoolbox data source."""
from abc import ABC, abstractmethod
from pathlib import Path

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.config.pathconfig import PathConfig


class DataSource(ABC):
    """
    Base abstract class object that contains path convenience functions.

    Cannot itself be instantiated. ``__init__``, ``download()``,
    ``load()``, and ``process()`` methods required for subclass to be
    instantiated.

    Parameters
    ----------
    country_config: CountryConfig
        Country configuration
    module_base_dir : str
        Module directory name (usually correspond to data source)
    is_public: bool, default = False
        Whether the dataset is public or private. Determines top-level
        directory structure.
    """

    @abstractmethod
    def __init__(
        self,
        country_config: CountryConfig,
        module_base_dir: str,
        is_public: bool = False,
    ):

        self._country_config = country_config
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
            / self._country_config.iso3
            / self._module_base_dir
        )

    @abstractmethod
    def download(self, clobber):
        """Abstract method for downloading."""
        pass

    @abstractmethod
    def process(self, clobber):
        """Abstract method for processing."""
        pass

    @abstractmethod
    def load(self):
        """Abstract method for loading."""
        pass
