"""Pipeline initializer."""
from typing import TypedDict

import geopandas as gpd

from aatoolbox.config.countryconfig import get_country_config
from aatoolbox.datasources.codab.codab import CodAB
from aatoolbox.datasources.ecmwf.realtime_seas5_monthly import EcmwfRealtime
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

    class Coordinates(TypedDict):
        """Create dict of coordinates."""

        north: float
        south: float
        west: float
        east: float

    def load_geoboundingbox(
        self,
        from_codab: bool = True,
        from_config: bool = False,
        coordinates: Coordinates = None,
    ):
        """Create a boundingbox object.

        Parameters
        ----------
        from_codab: bool
            Retrieve the geoboundingbox from the outer boundaries
            of the codab shapefile
        from_config
            Retrieve the geoboundingbox from the coordinates
            in the country config in
            ecmwf_realtime.geoboundaries_mapping
        from_coordinates: Coordinates
            Retrieve the geoboundingbox based on given
            boundingbox coorinates

        Returns
        -------
        GeoBoundingBox object

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> # Get boundingbox of npl codab boundaries
        >>> pipeline = Pipeline("npl")
        >>> npl_geobb = pipeline.load_geoboundingbox()
        """
        if from_codab:
            # doesn't matter which admin level as it takes the outer boundaries
            gdf = self.load_codab(admin_level=0)
            return GeoBoundingBox.from_shape(gdf)
        elif from_config:
            try:
                # Ignore mypy for this line because _config.ecmwf_realtime
                # could be None, but this is handled by the caught exceptions
                geobb_mapping = (
                    self._config.ecmwf_realtime.geoboundaries_mapping  # type: ignore # noqa:E501
                )
                return GeoBoundingBox(
                    north=geobb_mapping.north,
                    south=geobb_mapping.south,
                    east=geobb_mapping.east,
                    west=geobb_mapping.west,
                )
            except (IndexError, TypeError):
                raise AttributeError(
                    f"Cannot load geoboundaries mapping from "
                    f"{self._config.iso3.upper()} config file. "
                    f"ecmwf_realtime.geoboundaries_mapping "
                    f"attribute not found."
                )
        elif coordinates is not None:
            return GeoBoundingBox(
                north=coordinates["north"],
                south=coordinates["south"],
                east=coordinates["east"],
                west=coordinates["west"],
            )
        else:
            raise AttributeError(
                "None of the options given to retrieve a geoboundingbox."
            )

    def load_ecmwf_realtime(self, process: bool = False):
        """Load the realtime ecmwf data."""
        self._ecmwf_realtime = EcmwfRealtime(self._config.iso3)
        # TODO: add clobber
        if process:
            try:
                # Ignore mypy for this line because _config.ecmwf_realtime
                # could be None, but this is handled by the caught exceptions
                number_points_mapping = (
                    self._config.ecmwf_realtime.number_points_mapping  # type: ignore # noqa:E501
                )
            except (IndexError, TypeError):
                raise AttributeError(
                    f"ecmwf_realtime's number_points_mapping needed to "
                    f"process data but not available in "
                    f"{self._config.iso3.upper()} config file"
                )
            self._ecmwf_realtime.process(
                number_points_mapping
            )  # ,clobber=process)
        return self._ecmwf_realtime.load()
