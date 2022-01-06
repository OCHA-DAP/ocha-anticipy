"""Download and manipulate COD administrative boundaries."""
from pathlib import Path

import geopandas as gpd

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.hdx_api import load_dataset_from_hdx
from aatoolbox.utils.io import check_file_existence

_MODULE_BASENAME = "cod_ab"


class CodAB(DataSource):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    iso3 : (str)
        country iso3
    """

    def __init__(self, iso3: str):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=True
        )
        self._raw_filepath = (
            self._raw_base_dir / f"{self._iso3}_{_MODULE_BASENAME}.shp.zip"
        )

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
            If True, overwrites existing COD AB files

        Returns
        -------
        The downloaded filepath

        Examples
        --------
        >>> from aatoolbox.datasources.codab.codab import CodAB
        >>> # Download COD administrative boundaries for Nepal
        >>> codab = CodAB("npl")
        >>> npl_cod_shapefile = codab.download(
        ...    hdx_address="administrative-bounadries-of-nepal",
        ...    hdx_dataset_name="npl_admbnda_nd_20201117_SHP.zip")
        """
        return self._download(
            filepath=self._raw_filepath,
            hdx_address=hdx_address,
            hdx_dataset_name=hdx_dataset_name,
            clobber=clobber,
        )

    @staticmethod
    @check_file_existence
    def _download(
        filepath: Path, hdx_address: str, hdx_dataset_name: str, clobber: bool
    ):
        return load_dataset_from_hdx(
            hdx_address=hdx_address,
            hdx_dataset_name=hdx_dataset_name,
            output_filepath=filepath,
        )

    def load_admin_layer(self, layer_name: str) -> gpd.GeoDataFrame:
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
        >>> from aatoolbox.datasources.codab.codab import CodAB
        >>> # Retrieve admin 0 boundaries for Nepal
        >>> codab = CodAB("npl")
        >>> npl_admin0 = codab.load_admin_layer(
        ...     layer_name="npl_admbnda_adm2_20201117.shp")
        """
        return gpd.read_file(f"zip:///{self._raw_filepath / layer_name}")
