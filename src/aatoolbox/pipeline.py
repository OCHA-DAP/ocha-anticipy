"""Pipeline initializer."""
import geopandas as gpd

from aatoolbox.config.countryconfig import get_country_config
from aatoolbox.datasources.codab import CodAB


class Pipeline:
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
        admin_level_config = getattr(self._config.codab, f"admin{admin_level}")
        if admin_level_config is None:
            raise AttributeError(
                f"Admin level {admin_level} not implemented in "
                f"{self._config.iso3} config file"
            )
        self._codab.download(
            hdx_address=self._config.codab.hdx_address,
            hdx_dataset_name=self._config.codab.hdx_dataset_name,
        )
        return self._codab.get_admin_layer(
            layer_name=admin_level_config.layer_name
        )
