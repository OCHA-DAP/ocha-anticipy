"""Class to download and load IRI's seasonal forecast.

Data is downloaded from `IRI's maproom
<https://iridl.ldeo.columbia.edu/maproom/Global/Forecasts/NMME_Seasonal_Forecasts/Precipitation_ELR.html>`_

For now only the tercile precipitation forecast has been
implemented. This forecast is published in two formats,
namely the dominant tercile probability and the probability
per tercile. Both variations are implemented here.


"""
import logging
import os
from pathlib import Path

import requests
import xarray as xr
from typing_extensions import Literal

import aatoolbox.utils.raster  # noqa: F401
from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "iri"
_IRI_AUTH = "IRI_AUTH"


class _IriForecast(DataSource):
    """
    Base class to retrieve IRI's forecast data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should
        be included in the data.
    forecast_type: str
        The type of forecast information to download.
        Can be "prob" or "dominant"
        If "prob" the data will be retrieved that
        contains the probability per tercile
        If "dominant" the data will be retrieved that
        contains only one probability indicating
        the dominant tercile

    """

    def __init__(
        self,
        country_config,
        geo_bounding_box: GeoBoundingBox,
        forecast_type: Literal["prob", "dominant"],
    ):
        super().__init__(
            country_config=country_config,
            module_base_dir=_MODULE_BASENAME,
            is_public=False,
        )
        # round coordinates to correspond with the grid IRI publishes
        # its data on, which is 1 degree resolution
        # non-rounded coordinates can be given to the URL which then
        # automatically rounds them, but for file saving we prefer to do
        # this ourselves
        geo_bounding_box.round_coords(round_val=1, offset_val=0)
        self._geobb = geo_bounding_box
        self._forecast_type = forecast_type

    def download(
        self,
        clobber: bool = False,
    ):
        """
        Download the IRI seasonal tercile forecast as NetCDF file.

        To download data from the IRI API, a key is required for
        authentication, and must be set in the ``IRI_AUTH`` environment
        variable.  To obtain this key config you need to create an account
        `here.<https://iridl.ldeo.columbia.edu/auth/login>`_.
        Note that this key might be changed over time, and need to be updated
        regularly.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing raw files
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

    @staticmethod
    @check_file_existence
    def _download(filepath: Path, url: str, iri_auth: str, clobber: bool):

        response = requests.get(
            url,
            # have to authenticate by using a cookie
            cookies={"__dlauth_id": iri_auth},
        )
        with open(filepath, "wb") as out_file:
            out_file.write(response.content)
        return filepath

    def process(self, clobber: bool = False):
        """
        Process the IRI forecast.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing processed files

        """
        ds = self.load_raw()
        processed_file_path = self._get_processed_path()
        processed_file_path.parent.mkdir(parents=True, exist_ok=True)
        return self._process(
            filepath=processed_file_path, ds=ds, clobber=clobber
        )

    @staticmethod
    @check_file_existence
    def _process(filepath: str, ds, clobber: bool):
        # fix dates
        ds.aat.set_time_dim(t_dim="F", inplace=True)
        ds.aat.correct_calendar(inplace=True)
        ds = xr.decode_cf(ds)

        # IRI downloads in the order you give the coordinates
        # so make sure to invert them
        # IRI accepts -180 to 180 longitudes and 0 to 360
        # but automatically converts them to -180 to 180
        # so we don't need to do that
        # TODO: can be removed once we have a check in the
        # geoboundingbox class for south<north
        # TODO: for some reason the `inplace` is not working
        # re-add when we fixed that
        ds = ds.aat.invert_coordinates()
        ds.to_netcdf(filepath)
        return filepath

    def load_raw(self):
        try:
            ds = xr.load_dataset(
                self._get_raw_path(),
                decode_times=False,
                drop_variables="C",
            )
            return ds
        except ValueError as err:
            raise ValueError(
                "Cannot open the netcdf file. "
                "Might be due to invalid download with wrong authentication. "
                "Check the validity of `iri_auth` and try to download again. "
                "Else make sure the correct backend for "
                "opening a netCDF file is installed."
            ) from err

    def load(self):
        """Load the IRI forecast."""
        ds = xr.load_dataset(self._get_processed_path())
        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _get_file_name(self):
        file_name = (
            f"{self._country_config.iso3}"
            f"_iri_forecast_seasonal_precipitation_tercile_"
            f"{self._forecast_type}_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_raw_path(self):
        return self._raw_base_dir / self._get_file_name()

    def _get_processed_path(self):
        return self._processed_base_dir / self._get_file_name()

    def _get_url(self):

        base_url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
            f".NMME_Seasonal_Forecast/.Precipitation_ELR/"
            f".{self._forecast_type}/"
        )
        return (
            f"{base_url}"
            f"X/%28{self._geobb.west}%29%28{self._geobb.east}%29RANGEEDGES/"
            f"Y/%28{self._geobb.north}%29%28{self._geobb.south}%29RANGEEDGES/"
            "data.nc"
        )


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
    >>> from aatoolbox import create_country_config, \
    ...     GeoBoundingBox, IriForecastProb
    >>> country_config = create_country_config(iso3="bfa")
    >>> geo_bounding_box = GeoBoundingBox(north=13.0,
    ...                                   south=12.0,
    ...                                   east=-3.0,
    ...                                   west=-2.0)
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
    >>> from aatoolbox import create_country_config, \
    ...     GeoBoundingBox, IriForecastDominant
    >>> country_config = create_country_config(iso3="bfa")
    >>> geo_bounding_box = GeoBoundingBox(north=13.0,
    ...                                   south=12.0,
    ...                                   east=-3.0,
    ...                                   west=-2.0)
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
