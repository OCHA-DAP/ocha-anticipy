"""Retrieve COD administrative boundaries."""
import geopandas as gpd

from aatools.config.config import Config


class CodAB(object):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    iso3 : (str)
        country iso3
    """

    def __init__(self, iso3: str):
        self.config = Config(iso3)

    def _get_raw_base_dir(self):
        return (
            self.config.path.base
            / self.config.path.public
            / self.config.path.raw
            / self.config.country.iso3
            / self.config.codab.base_dir
        )

    def get_admin0(self):
        """
        Get the admin level 0 COD AB for a country.

        Returns
        -------
        geopandas dataframe with COD AB admin 0

        Examples
        --------
        >>> from aatools.utils.codab import CodAB
        >>> # Get admin 0 boundaries for Nepal
        >>> codab = CodAB("npl")
        >>> npl_admin0 = codab.get_admin0()
        """
        shapefile = (
            self._get_raw_base_dir()
            / self.config.country.codab.base_dir
            / f"{self.config.country.codab.base_zip}"
            f"!{self.config.country.codab.admin0.base_layer}"
        )
        return gpd.read_file(f"zip://{shapefile}")
