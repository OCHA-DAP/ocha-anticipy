"""Retrieve COD administrative boundaries."""
import geopandas as gpd

from aatoolbox.config.aatoolboxbase import AaToolboxBase
from aatoolbox.datasources.hdx_download import get_dataset_from_hdx

MODULE_BASE_DIR = "cod_ab"


class CodAB(AaToolboxBase):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    iso3 : (str)
        country iso3
    """

    def __init__(self, iso3: str):
        super().__init__(iso3=iso3, module_base_dir=MODULE_BASE_DIR)

    def _download(self):
        return get_dataset_from_hdx(
            hdx_address=self.config.country.codab.hdx_address,
            hdx_dataset_name=self.config.country.codab.hdx_dataset_name,
            output_directory=self.get_public_raw_base_dir(),
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
            self.get_public_raw_base_dir()
            / self.config.country.codab.base_dir
            / f"{self.config.country.codab.base_zip}"
            f"!{self.config.country.codab.admin0.base_layer}"
        )
        return gpd.read_file(f"zip://{shapefile}")
