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
        >>> from aatoolbox.pipeline_caller import Pipeline
        >>> # Get admin 0 boundaries for Nepal
        >>> pipeline = Pipeline("npl")
        >>> npl_admin0 = pipeline.get_codab(admin_level=2)
        """
        admin_level_max = self._config.codab.admin_level_max
        if admin_level > admin_level_max:
            raise AttributeError(
                f"Admin level {admin_level} requested, but maximum set to "
                f"{admin_level_max} in {self._config.iso3.upper()} config file"
            )
        return self._get_codab(
            layer_name=self._config.codab.layer_base_name.format(
                admin_level=admin_level
            )
        )

    def get_codab_custom(self, custom_layer_number: int = 0):
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
        >>> from aatoolbox.pipeline_caller import Pipeline
        >>> # Get district boundaries for Nepal
        >>> pipeline = Pipeline("npl")
        >>> npl_admin0 = pipeline.get_codab_custom(custom_layer_number=0)
        """
        try:
            # Ignore mypy for this line because custom_layer_names could be
            # None, but this is handled by the caught exceptions
            layer_name = self._config.codab.custom_layer_names[
                custom_layer_number
            ]  # type: ignore
        except (IndexError, TypeError):
            raise AttributeError(
                f"{custom_layer_number}th custom layer requested but not "
                f"available in {self._config.iso3.upper()} config file"
            )
        return self._get_codab(layer_name=layer_name)

    def _get_codab(self, layer_name):
        self._codab.download(
            hdx_address=self._config.codab.hdx_address,
            hdx_dataset_name=self._config.codab.hdx_dataset_name,
        )
        return self._codab.get_admin_layer(layer_name=layer_name)
