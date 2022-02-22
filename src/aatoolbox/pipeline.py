"""Pipeline initializer."""
import os

import geopandas as gpd
from geopandas import GeoDataFrame

from aatoolbox.config.countryconfig import get_country_config
from aatoolbox.datasources.codab.codab import CodAB
from aatoolbox.datasources.iri.iri_seasonal_forecast import (
    IriForecastDominant,
    IriForecastProb,
    _IriForecast,
)
from aatoolbox.utils.geoboundingbox import GeoBoundingBox


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

    def load_codab(
        self, admin_level: int, clobber: bool = False
    ) -> gpd.GeoDataFrame:
        """
        Get the COD AB data by admin level.

        Parameters
        ----------
        admin_level: int
            The administrative level
        clobber: bool
            If true, overwrite existing COD AB files

        Returns
        -------
        COD AB geodataframe with specified admin level

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> # Get admin 0 boundaries for Nepal
        >>> pipeline = Pipeline("npl")
        >>> npl_admin0 = pipeline.load_codab(admin_level=2)
        """
        admin_level_max = self._config.codab.admin_level_max
        if admin_level > admin_level_max:
            raise AttributeError(
                f"Admin level {admin_level} requested, but maximum set to "
                f"{admin_level_max} in {self._config.iso3.upper()} config file"
            )
        return self._load_codab(
            layer_name=self._config.codab.layer_base_name.format(
                admin_level=admin_level
            ),
            clobber=clobber,
        )

    def load_codab_custom(
        self, custom_layer_number: int = 0, clobber: bool = False
    ):
        """
        Get the COD AB data from a custom (non-level) layer.

        Parameters
        ----------
        custom_layer_number: int
            The 0-indexed number of the layer listed in the custom_layer_names
            parameter of the country's config file
        clobber: bool
            If true, overwrite existing COD AB files

        Returns
        -------
        COD AB geodataframe with custom admin level

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> # Get district boundaries for Nepal
        >>> pipeline = Pipeline("npl")
        >>> npl_admin0 = pipeline.load_codab_custom(custom_layer_number=0)
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
        return self._load_codab(layer_name=layer_name, clobber=clobber)

    def _load_codab(self, layer_name: str, clobber: bool):
        self._codab.download(
            hdx_address=self._config.codab.hdx_address,
            hdx_dataset_name=self._config.codab.hdx_dataset_name,
            clobber=clobber,
        )
        return self._codab.load_admin_layer(layer_name=layer_name)

    def load_geoboundingbox_gdf(self, gdf: GeoDataFrame):
        """Create a geaboundingbox from a geodataframe.

        Retrieve the geoboundingbox from the outer boundaries
        of the geodataframe.

        Returns
        -------
        GeoBoundingBox object

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> # Get boundingbox of npl codab boundaries
        >>> pipeline = Pipeline("npl")
        >>> npl_geobb = pipeline.load_geoboundingbox_gdf()
        """
        return GeoBoundingBox.from_shape(gdf)

    def load_geoboundingbox_coordinates(
        self, north: float, south: float, east: float, west: float
    ):
        """Retrieve the geoboundingbox from coordinates.

        Returns
        -------
        GeoBoundingBox object

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> # Get boundingbox based on coordinates
        >>> pipeline = Pipeline("npl")
        >>> npl_geobb_coord = (pipeline.load_geoboundingbox_coordinates(
        ... north=15, south=10, east=2, west=-2))
        """
        return GeoBoundingBox(
            north=north,
            south=south,
            east=east,
            west=west,
        )

    def load_iri_all_terciles_seasonal_forecast(
        self, geo_bounding_box: GeoBoundingBox, clobber: bool = False
    ):
        """
        Load the IRI seasonal tercile forecast.

        This data contains a probability per tercile.
        """
        iri_forecast_probability = IriForecastProb(
            self._config.iso3, geo_bounding_box=geo_bounding_box
        )
        return self._load_iri_seasonal(
            iri_class=iri_forecast_probability, clobber=clobber
        )

    def load_iri_dominant_tercile_seasonal_forecast(
        self, geo_bounding_box: GeoBoundingBox, clobber: bool = False
    ):
        """
        Load the IRI seasonal tercile forecast.

        This data only contains the dominant tercile probability.
        """
        iri_forecast_dominant = IriForecastDominant(
            self._config.iso3, geo_bounding_box=geo_bounding_box
        )
        return self._load_iri_seasonal(
            iri_class=iri_forecast_dominant, clobber=clobber
        )

    def _load_iri_seasonal(self, iri_class: _IriForecast, clobber: bool):
        iri_auth = os.getenv("IRI_AUTH")
        if iri_auth is None:
            raise ValueError(
                "`iri_auth` is not set and thus cannot download the data. "
                "Set `iri_auth` to proceed."
            )
        iri_class.download(iri_auth=iri_auth, clobber=clobber)
        iri_class.process(clobber=clobber)
        return iri_class.load()
