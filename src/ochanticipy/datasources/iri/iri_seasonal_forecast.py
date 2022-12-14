"""Class to download and load IRI's seasonal forecast."""
import logging
import os
from pathlib import Path

import requests
import xarray as xr
from typing_extensions import Literal

import ochanticipy.utils.raster  # noqa: F401
from ochanticipy.config.countryconfig import CountryConfig
from ochanticipy.datasources.datasource import DataSource
from ochanticipy.utils.check_file_existence import check_file_existence
from ochanticipy.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)

_IRI_AUTH = "IRI_AUTH"


class _IriForecast(DataSource):
    """
    Base class to retrieve IRI's forecast data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should be included in the
        data.
    forecast_type: str
        The type of forecast information to download,  can be either "prob" or
        "dominant". If "prob" the data will be retrieved that contains the
        probability per tercile. If "dominant" the data will be retrieved that
        contains only one probability indicating the dominant tercile.
    """

    def __init__(
        self,
        country_config,
        geo_bounding_box: GeoBoundingBox,
        forecast_type: Literal["prob", "dominant"],
    ):
        super().__init__(
            country_config=country_config,
            datasource_base_dir="iri",
            is_public=False,
        )
        # round coordinates to correspond with the grid IRI publishes
        # its data on, which is 1 degree resolution
        # non-rounded coordinates can be given to the URL which then
        # automatically rounds them, but for file saving we prefer to do
        # this ourselves
        self._geobb = geo_bounding_box.round_coords(round_val=1, offset_val=0)
        self._forecast_type = forecast_type

    def download(
        self,
        clobber: bool = False,
    ) -> Path:
        """
        Download the IRI seasonal tercile forecast as NetCDF file.

        To download data from the IRI API, a key is required for
        authentication, and must be set in the ``IRI_AUTH`` environment
        variable. To obtain this key config you need to create an account
        `here <https://iridl.ldeo.columbia.edu/auth/login>`_.
        Note that this key might be changed over time, and need to be updated
        regularly.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing raw files

        Returns
        -------
        The downloaded filepath
        """
        iri_auth = os.getenv(_IRI_AUTH)
        if iri_auth is None:
            raise ValueError(
                f"Environment variable {_IRI_AUTH} is not set and thus cannot "
                f"download the data. Set {_IRI_AUTH} to proceed."
            )
        output_filepath = self._get_raw_path()
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        url = self._get_url()
        logger.info("Downloading IRI NetCDF file.")
        return self._download(
            filepath=output_filepath,
            url=url,
            iri_auth=iri_auth,
            clobber=clobber,
        )

    def process(self, clobber: bool = False) -> Path:
        """
        Process the IRI forecast.

        Should only be called after the ``download`` method has been
        executed.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing processed files

        Returns
        -------
        The processed filepath
        """
        ds = self._load_raw()
        processed_file_path = self._get_processed_path()
        processed_file_path.parent.mkdir(parents=True, exist_ok=True)
        return self._process(
            filepath=processed_file_path, ds=ds, clobber=clobber
        )

    def load(self) -> xr.Dataset:
        """
        Load the IRI forecast data.

        Should only be called after the ``download`` and ``process`` methods
        have been executed.

        Returns
        -------
        The processed IRI dataset
        """
        processed_path = self._get_processed_path()
        try:
            ds = xr.load_dataset(processed_path)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"Cannot open the netcdf file {processed_path}. "
                f"Make sure that you have already called the 'process' method "
                f"and that the file {processed_path} exists. "
            ) from err
        # TODO: Save coordinate system to a general config
        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _get_file_name(self) -> str:
        file_name = (
            f"{self._country_config.iso3}"
            f"_iri_forecast_seasonal_precipitation_tercile_"
            f"{self._forecast_type}_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_raw_path(self) -> Path:
        return self._raw_base_dir / self._get_file_name()

    def _get_processed_path(self) -> Path:
        return self._processed_base_dir / self._get_file_name()

    def _get_url(self) -> str:

        base_url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
            f".NMME_Seasonal_Forecast/.Precipitation_ELR/"
            f".{self._forecast_type}/"
        )
        return (
            f"{base_url}"
            f"X/%28{self._geobb.lon_min}%29%28{self._geobb.lon_max}"
            f"%29RANGEEDGES/"
            f"Y/%28{self._geobb.lat_max}%29%28{self._geobb.lat_min}"
            f"%29RANGEEDGES/"
            "data.nc"
        )

    def _load_raw(self) -> xr.Dataset:
        try:
            ds = xr.load_dataset(
                self._get_raw_path(),
                decode_times=False,
                drop_variables="C",
            )
            return ds
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"Cannot open the netcdf file {self._get_raw_path()}. Make "
                f"sure that you have already called the 'download' method "
                f"and that the file {self._get_raw_path()} exists. "
            ) from err

    @check_file_existence
    def _download(
        self, filepath: Path, url: str, iri_auth: str, clobber: bool
    ) -> Path:

        response = requests.get(
            url,
            # have to authenticate by using a cookie
            cookies={"__dlauth_id": iri_auth},
        )
        if response.headers["Content-Type"] != "application/x-netcdf":
            msg = (
                f"The request returned headers indicating that the expected "
                f"file type was not returned. In some cases th  is may be due "
                f"to an issue with the authentication. Please check the "
                f"validity of the authentication key found in your "
                f"{_IRI_AUTH} environment variable and try again."
            )
            raise requests.RequestException(msg)
        with open(filepath, "wb") as out_file:
            out_file.write(response.content)
        return filepath

    @check_file_existence
    def _process(self, filepath: Path, ds, clobber: bool) -> Path:
        # fix dates
        ds.oap.set_time_dim(t_dim="F", inplace=True)
        ds.oap.correct_calendar(inplace=True)
        ds = xr.decode_cf(ds)

        # IRI accepts -180 to 180 longitudes and 0 to 360
        # but automatically converts them to -180 to 180
        # so we don't need to do that
        ds.to_netcdf(filepath)
        return filepath


class IriForecastProb(_IriForecast):
    """
    Class to retrieve IRI's forecast data per tercile.

    The retrieved data contains the probability per
    tercile for the given bounding box.
    Automatically all seasons and leadtimes are
    downloaded.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should
        be included in the data.

    Examples
    --------
    >>> from ochanticipy import create_country_config, \
    ...     GeoBoundingBox, IriForecastProb
    >>> country_config = create_country_config(iso3="bfa")
    >>> geo_bounding_box = GeoBoundingBox(lat_max=13.0,
    ...                                   lat_min=12.0,
    ...                                   lon_max=-3.0,
    ...                                   lon_min=-2.0)
    >>>
    >>> # Initialize class and retrieve data
    >>> iri = IriForecastProb(country_config,geo_bounding_box)
    >>> iri.download() # Must have IRI_AUTH environment variable set
    >>> iri.process()
    >>>
    >>> iri_data = iri.load()
    """

    def __init__(self, country_config, geo_bounding_box: GeoBoundingBox):
        super().__init__(
            country_config,
            geo_bounding_box=geo_bounding_box,
            forecast_type="prob",
        )


class IriForecastDominant(_IriForecast):
    """
    Class to retrieve IRI's forecast dominant tercile data.

    The retrieved data contains the dominant probability.
    Automatically all seasons and leadtimes are
    downloaded.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
       the bounding coordinates of the area that should
       be included in the data.

    Examples
    --------
    >>> from ochanticipy import create_country_config, \
    ...     GeoBoundingBox, IriForecastDominant
    >>> country_config = create_country_config(iso3="bfa")
    >>> geo_bounding_box = GeoBoundingBox(lat_max=13.0,
    ...                                   lat_min=12.0,
    ...                                   lon_max=-3.0,
    ...                                   lon_min=-2.0)
    >>>
    >>> # Initialize class and retrieve data
    >>> iri = IriForecastDominant(country_config,geo_bounding_box)
    >>> iri.download() # Must have IRI_AUTH environment variable set
    >>> iri.process()
    >>>
    >>> iri_data = iri.load()
    """

    def __init__(
        self, country_config: CountryConfig, geo_bounding_box: GeoBoundingBox
    ):
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            forecast_type="dominant",
        )
