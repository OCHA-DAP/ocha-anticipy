"""Class to download and load IRI's seasonal forecast.

Data is downloaded from `IRI's maproom
<https://iridl.ldeo.columbia.edu/maproom/Global/Forecasts/NMME_Seasonal_Forecasts/Precipitation_ELR.html>`_

For now only the tercile precipitation forecast has been
implemented. This forecast is published in two formats,
namely the dominant tercile probability and the probability
per tercile. Both variations are implemented here.
"""
import logging

import requests
import xarray as xr
from typing_extensions import Literal

import aatoolbox.utils.raster  # noqa: F401
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "iri"


class _IriForecast(DataSource):
    """
    Base class to retrieve IRI's forecast data.

    Parameters
    ----------
    iso3 : str
        country iso3
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
        iso3,
        geo_bounding_box: GeoBoundingBox,
        forecast_type: Literal["prob", "dominant"],
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
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
        iri_auth: str,
        clobber: bool = False,
    ):
        """
        Download the IRI seasonal tercile forecast as NetCDF file.

        Parameters
        ----------
        iri_auth: str
            iri key for authentication. An account is
            needed to get this key config. For an account this key might
            be changed over time, so might need to update it regularly
        clobber : bool, default = False
            If True, overwrites existing raw files
        """
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
    def _download(filepath, url, iri_auth, clobber):
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
        ds = xr.load_dataset(
            self._get_raw_path(),
            decode_times=False,
            drop_variables="C",
        )
        return ds

    def load(self):
        """Load the IRI forecast."""
        ds = xr.load_dataset(self._get_processed_path())
        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _get_file_name(self):
        file_name = (
            f"{self._iso3}_iri_forecast_seasonal_precipitation_tercile_"
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
    iso3 : str
        country iso3
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should
        be included in the data.

    Examples
    --------
    >>> from aatoolbox.pipeline import Pipeline
    >>> from aatoolbox.utils.geoboundingbox import GeoBoundingBox
    >>> (from aatoolbox.datasources.iri.
    ... iri_seasonal_forecast import IriForecastProb)
    #retrieve the bounding box to download data for
    >>> iso3="bfa"
    >>> pipeline_iso = Pipeline(iso3)
    >>> codab_admin1 = pipeline_iso.load_codab(admin_level=1)
    >>> geo_bounding_box = GeoBoundingBox.from_shape(codab_admin1)
    #initialize class and retrieve data
    >>> iri=IriForecastProb(iso3,geo_bounding_box)
    #the iri auth str can e.g. be saved as an env var
    >>> iri.download(os.getenv("IRI_AUTH"))
    >>> iri.process()
    >>> iri.load()
    """

    def __init__(self, iso3, geo_bounding_box: GeoBoundingBox):
        super().__init__(
            iso3=iso3,
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
    iso3 : str
       country iso3
    geo_bounding_box: GeoBoundingBox
       the bounding coordinates of the area that should
       be included in the data.

    Examples
    --------
    >>> from aatoolbox.pipeline import Pipeline
    >>> from aatoolbox.utils.geoboundingbox import GeoBoundingBox
    >>> (from aatoolbox.datasources.iri.
    ... iri_seasonal_forecast import IriForecastDominant)
    #retrieve the bounding box to download data for
    >>> iso3="bfa"
    >>> pipeline_iso = Pipeline(iso3)
    >>> codab_admin1 = pipeline_iso.load_codab(admin_level=1)
    >>> geo_bounding_box = GeoBoundingBox.from_shape(codab_admin1)
    #initialize class and retrieve data
    >>> iri=IriForecastDominant(iso3,geo_bounding_box)
    #the iri auth str can e.g. be saved as an env var
    >>> iri.download(os.getenv("IRI_AUTH"))
    >>> iri.process()
    >>> iri.load()
    """

    def __init__(self, iso3, geo_bounding_box: GeoBoundingBox):
        super().__init__(
            iso3=iso3,
            geo_bounding_box=geo_bounding_box,
            forecast_type="dominant",
        )
