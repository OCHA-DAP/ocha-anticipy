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
        self._codab = CodAB(self._config.iso3)

    def get_codab(self, admin_level: int) -> gpd.GeoDataFrame:
        """
        Get the COD AB data.

        Parameters
        ----------
        admin_level: int
            The administrative level

        Returns
        -------
        COD AB geodataframe

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> # Get admin 0 boundaries for Nepal
        >>> pipeline = Pipeline("npl")
        >>> npl_admin0 = pipeline.get_codab(0)
        """
        self._codab.download(
            hdx_address=self._config.codab.hdx_address,
            hdx_dataset_name=self._config.codab.hdx_dataset_name,
        )
        return self._codab.get_admin_layer(
            layer_name=getattr(
                self._config.codab, f"admin{admin_level}"
            ).layer_name
        )
