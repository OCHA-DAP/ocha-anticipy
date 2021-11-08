"""Retrieve COD administrative boundaries."""
from pathlib import Path

import geopandas as gpd

from aatoolbox.datasources.datasource import DataSource, check_file_existence
from aatoolbox.utils.hdx_api import get_dataset_from_hdx

MODULE_BASE = "cod_ab"


class CodAB(DataSource):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    iso3 : (str)
        country _iso3
    """

    def __init__(self, iso3: str):
        super().__init__(
            iso3=iso3, module_base_dir=MODULE_BASE, is_public=True
        )
        self._raw_filepath = (
            self._raw_base_dir / f"{self._iso3}_{MODULE_BASE}.shp.zip"
        )

    @check_file_existence(filepath_attribute_name="_raw_filepath")
    def download(
        self, hdx_address: str, hdx_dataset_name: str, clobber: bool = False
    ) -> Path:
        """
        Download COD AB file from HDX.

        Parameters
        ----------
        hdx_address: str
            URL suffix of dataset page on HDX
        hdx_dataset_name: str
            Name of dataset on HDX
        clobber : bool, default = False
            If True, overwrites existing download

        Returns
        -------
        The downloaded filepath
        """
        return get_dataset_from_hdx(
            hdx_address=hdx_address,
            hdx_dataset_name=hdx_dataset_name,
            output_filepath=self._raw_filepath,
        )

    def get_admin_layer(self, layer_name: str) -> gpd.GeoDataFrame:
        """
        Get an admin level by layer name.

        Parameters
        ----------
        layer_name: str
            The admin layer name

        Returns
        -------
        geopandas dataframe with COD AB admin information

        Examples
        --------
        >>> from aatoolbox.datasources.codab import CodAB
        >>> # Get admin 0 boundaries for Nepal
        >>> codab = CodAB("npl")
        >>> npl_admin0 = codab.get_admin_layer()
        """
        return gpd.read_file(f"zip:///{self._raw_filepath / layer_name}")
