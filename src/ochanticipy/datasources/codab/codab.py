"""Download and manipulate COD administrative boundaries."""
import logging
from pathlib import Path
from typing import List, Union

import geopandas as gpd
from fiona.errors import DriverError

from ochanticipy.config.countryconfig import CountryConfig
from ochanticipy.datasources.datasource import DataSource
from ochanticipy.utils.check_file_existence import check_file_existence
from ochanticipy.utils.hdx_api import load_resource_from_hdx

logger = logging.getLogger(__name__)


class CodAB(DataSource):
    """
    Work with COD AB (administrative boundaries).

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    """

    def __init__(self, country_config: CountryConfig):
        super().__init__(
            country_config,
            datasource_base_dir="cod_ab",
            is_public=True,
            config_datasource_name="codab",
        )

        # account for files stored across multiple resources
        res_name = self._datasource_config.hdx_resource_name
        self._multiple_resources = isinstance(res_name, list)

        if self._multiple_resources:
            zip_filenames = [
                f"{self._country_config.iso3}_adm{i}.shp.zip"
                for i in range(len(res_name))
            ]
            self._hdx_resource_list = res_name
        else:
            zip_filenames = [f"{self._country_config.iso3}_adm.shp.zip"]
            self._hdx_resource_list = [res_name]

        # generate filepaths for all resource zipfiles
        self._raw_filepaths = [
            (self._raw_base_dir / fn) for fn in zip_filenames
        ]

    def download(self, clobber: bool = False) -> Union[Path, List[Path]]:
        """
        Download COD AB file from HDX.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing COD AB files

        Returns
        -------
        The downloaded filepath(s)

        Examples
        --------
        >>> from ochanticipy import create_country_config, CodAB
        >>> # Download COD administrative boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_cod_shapefile = codab.download()
        """
        for filepath, hdx_resource_name in zip(
            self._raw_filepaths, self._hdx_resource_list
        ):
            self._download(
                filepath=filepath,
                hdx_dataset=f"cod-ab-{self._country_config.iso3}",
                hdx_resource_name=hdx_resource_name,
                clobber=clobber,
            )
        return self._raw_base_dir

    def process(self, *args, **kwargs):
        """
        Process COD AB data.

        Method not implemented.
        """
        logger.info("`process()` method not implemented for CodAB.")

    def load(self, admin_level: int = 0) -> gpd.GeoDataFrame:  # type: ignore
        """
        Get the COD AB data by admin level.

        Parameters
        ----------
        admin_level: int, default = 0
            The administrative level

        Returns
        -------
        COD AB geodataframe with specified admin level

        Raises
        ------
        AttributeError
            If the requested admin level is higher than what is available
        FileNotFoundError
            If the requested filename or layer name are not found

        Examples
        --------
        >>> from ochanticipy import create_country_config, CodAB
        >>>
        >>> # Retrieve admin 2 boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_admin2 = codab.load(admin_level=2)
        """
        admin_level_max = self._datasource_config.admin_level_max
        if admin_level > admin_level_max:
            raise AttributeError(
                f"Admin level {admin_level} requested, but maximum set to "
                f"{admin_level_max} in {self._country_config.iso3.upper()} "
                f"config file"
            )
        return self._load_admin_layer(
            layer_name=getattr(
                self._datasource_config, f"admin{admin_level}_name"
            ),
            admin_level=admin_level,
        )

    def load_custom(self, custom_layer_number: int = 0) -> gpd.GeoDataFrame:
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

        Raises
        ------
        AttributeError
            If the requested custom layer number is not available
        FileNotFoundError
            If the requested filename or layer name are not found

        Examples
        --------
        >>> from ochanticipy import create_country_config, CodAB
        >>>
        >>> # Retrieve district boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_district = codab.load_custom(custom_layer_number=0)
        """
        # TODO: possibly merge the two load methods
        try:
            # Ignore mypy for this line because custom_layer_names could be
            # None, but this is handled by the caught exceptions
            layer_name = self._datasource_config.custom_layer_names[
                custom_layer_number
            ]  # type: ignore

        except (IndexError, TypeError) as err:
            raise AttributeError(
                f"{custom_layer_number}th custom layer requested but not "
                f"available in {self._country_config.iso3.upper()} config file"
            ) from err
        return self._load_admin_layer(
            layer_name=layer_name,
            admin_level=0,  # breaks if layer in multiple resources
        )

    def _load_admin_layer(
        self, layer_name: str, admin_level: int
    ) -> gpd.GeoDataFrame:
        fp_index = int(admin_level) if self._multiple_resources else 0

        try:
            zip_path = self._raw_filepaths[fp_index] / layer_name
            return gpd.read_file(f"zip://{zip_path.as_posix()}")
        except DriverError as err:
            raise FileNotFoundError(
                f"Could not read boundary shapefile. Make sure that "
                f"you have already called the 'download' method and "
                f"that the file {self._raw_filepaths[fp_index]} exists."
                f"If it does exist, please check the validity of the "
                f"layer name: '{layer_name}'."
            ) from err

    @check_file_existence
    def _download(
        self,
        filepath: Path,
        hdx_dataset: str,
        hdx_resource_name: str,
        clobber: bool,
    ) -> Path:
        return load_resource_from_hdx(
            hdx_dataset=hdx_dataset,
            hdx_resource_name=hdx_resource_name,
            output_filepath=filepath,
        )
