"""Pipeline initializer."""
import geopandas as gpd

from aatoolbox.config.countryconfig import get_country_config
from aatoolbox.datasources.codab import CodAB


class Pipeline(object):
    """
    Initialize a configured pipeline.

    Parameters
    ----------
    iso3_unvalidated: str
        Country _iso3 passed in by the user
    """

    def __init__(self, iso3_unvalidated: str):
        self._config = get_country_config(iso3_unvalidated)
        self._codab = CodAB(self._config._iso3)

    def get_codab(self, download: bool = True) -> gpd.GeoDataFrame:
        """
        Get the COD AB data.

        Parameters
        ----------
        download : bool, default = True

        Returns
        -------
        COD AB geodataframe

        """
        if download:
            self._codab.download(
                hdx_address=self._config.codab.hdx_address,
                hdx_dataset_name=self._config.codab.hdx_dataset_name,
            )
        return self._codab.get_admin0(
            layer_name=self._config.codab.admin0.layer_name
        )
