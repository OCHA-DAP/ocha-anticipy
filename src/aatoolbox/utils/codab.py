"""Retrieve COD administrative boundaries."""
import geopandas as gpd

from aatoolbox.config.aatoolbox import AaToolbox

MODULE_BASE_DIR = "cod_ab"


class CodAB(AaToolbox):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    iso3 : (str)
        country iso3
    """

    def __init__(self, iso3: str):
        super().__init__(iso3=iso3, module_base_dir=MODULE_BASE_DIR)

    def get_admin0(self):
        """
        Get the admin level 0 COD AB for a country.

        Returns
        -------
        geopandas dataframe with COD AB admin 0

        Examples
        --------
        >>> from aatoolbox.utils.codab import CodAB
        >>> # Get admin 0 boundaries for Nepal
        >>> codab = CodAB("npl")
        >>> npl_admin0 = codab.get_admin0()
        """
        shapefile = (
            self.get_public_raw_dir()
            / self.config.country.codab.base_dir
            / f"{self.config.country.codab.base_zip}"
            f"!{self.config.country.codab.admin0.base_layer}"
        )
        return gpd.read_file(f"zip://{shapefile}")
