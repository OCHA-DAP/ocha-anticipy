"""Retrieve COD administrative boundaries."""
import geopandas as gpd

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.hdx_api import get_dataset_from_hdx

MODULE_BASE = "cod_ab"


class CodAB(DataSource):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    iso3 : (str)
        country iso3
    """

    def __init__(self, iso3: str):
        super().__init__(iso3=iso3, module_base_dir=MODULE_BASE)

    def download(self, use_cache=True):
        """
        Download COD AB file from HDX.

        Parameters
        ----------
        use_cache : bool
            Whether to check for cached downloaded data

        Returns
        -------
        The downloaded filepath
        """
        filepath = self._get_raw_filepath()
        if use_cache and filepath.exists():
            return filepath
        return get_dataset_from_hdx(
            hdx_address=self.config.country.codab.hdx_address,
            hdx_dataset_name=self.config.country.codab.hdx_dataset_name,
            output_filepath=self._get_raw_filepath(),
        )

    def _get_raw_filepath(self):
        return (
            self._get_public_raw_base_dir()
            / f"{self.config.country.iso3}_{MODULE_BASE}.shp.zip"
        )

    def get_admin0(self):
        """
        Get the admin level 0 COD AB for a country.

        Returns
        -------
        geopandas dataframe with COD AB admin 0

        Examples
        --------
        >>> from aatoolbox.datasources.codab import CodAB
        >>> # Get admin 0 boundaries for Nepal
        >>> codab = CodAB("npl")
        >>> npl_admin0 = codab.get_admin0()
        """
        shapefile = (
            f"{self._get_raw_filepath()}"
            f"!{self.config.country.codab.admin0.base_layer}"
        )
        return gpd.read_file(f"zip:///{shapefile}")
