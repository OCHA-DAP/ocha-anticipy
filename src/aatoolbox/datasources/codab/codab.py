"""Download and manipulate COD administrative boundaries."""
from pathlib import Path

import geopandas as gpd

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.hdx_api import load_dataset_from_hdx
from aatoolbox.utils.io import check_file_existence

_MODULE_BASENAME = "cod_ab"


class CodAB(DataSource):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    """

    def __init__(self, country_config: CountryConfig):
        super().__init__(
            country_config, module_base_dir=_MODULE_BASENAME, is_public=True
        )
        self._raw_filepath = (
            self._raw_base_dir
            / f"{self._country_config.iso3}_{_MODULE_BASENAME}.shp.zip"
        )

    def download(self, clobber: bool = False) -> Path:
        """
        Download COD AB file from HDX.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing COD AB files

        Returns
        -------
        The downloaded filepath

        Examples
        --------
        >>> from aatoolbox import create_country_config, CodAB
        >>> # Download COD administrative boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_cod_shapefile = codab.download()
        """
        return self._download(
            filepath=self._raw_filepath,
            hdx_address=self._country_config.codab.hdx_address,
            hdx_dataset_name=self._country_config.codab.hdx_dataset_name,
            clobber=clobber,
        )

    @staticmethod
    @check_file_existence
    def _download(
        filepath: Path, hdx_address: str, hdx_dataset_name: str, clobber: bool
    ) -> Path:
        return load_dataset_from_hdx(
            hdx_address=hdx_address,
            hdx_dataset_name=hdx_dataset_name,
            output_filepath=filepath,
        )

    def load(self, admin_level: int) -> gpd.GeoDataFrame:
        """
        Get the COD AB data by admin level.

        Parameters
        ----------
        admin_level: int
            The administrative level

        Returns
        -------
        COD AB geodataframe with specified admin level

        Examples
        --------
        >>> from aatoolbox import create_country_config, CodAB
        >>>
        >>> # Retrieve admin 2 boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_admin0 = codab.load(admin_level=2)
        """
        admin_level_max = self._country_config.codab.admin_level_max
        if admin_level > admin_level_max:
            raise AttributeError(
                f"Admin level {admin_level} requested, but maximum set to "
                f"{admin_level_max} in {self._country_config.iso3.upper()} "
                f"config file"
            )
        return self._load_admin_layer(
            layer_name=self._country_config.codab.layer_base_name.format(
                admin_level=admin_level
            )
        )

    def load_custom(self, custom_layer_number: int = 0) -> gpd.GeoDataFrame:
        """
        Get the COD AB data from a custom (non-level) layer.

        Parameters
        ----------
        custom_layer_number: int
            The 0-indexed number of the layer listed in the custom_layer_names
            parameter of the country's config file

        Returns
        -------
        COD AB geodataframe with custom admin level

        Examples
        --------
        >>> from aatoolbox import create_country_config, CodAB
        >>>
        >>> # Retrieve district boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_admin0 = codab.load_custom(custom_layer_number=0)
        """
        # TODO: possibly merge the two load methods
        try:
            # Ignore mypy for this line because custom_layer_names could be
            # None, but this is handled by the caught exceptions
            layer_name = self._country_config.codab.custom_layer_names[
                custom_layer_number
            ]  # type: ignore
        except (IndexError, TypeError):
            raise AttributeError(
                f"{custom_layer_number}th custom layer requested but not "
                f"available in {self._country_config.iso3.upper()} config file"
            )
        return self._load_admin_layer(layer_name=layer_name)

    def _load_admin_layer(self, layer_name: str) -> gpd.GeoDataFrame:
        return gpd.read_file(f"zip:///{self._raw_filepath / layer_name}")
