"""Class to download and load IRI's seasonal forecast.

Data is downloaded from `IRI's maproom
<https://iridl.ldeo.columbia.edu/maproom/Global/Forecasts/NMME_Seasonal_Forecasts/Precipitation_ELR.html>`_

For now only the tercile precipitation forecast has been
implemented.
"""

import logging

import requests
import xarray as xr

import aatoolbox.utils.raster  # noqa: F401
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "iri"


class IriForecast(DataSource):
    """
    Work with IRI's forecast data.

    Parameters
    ----------
    iso3 : str
        country iso3
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should
        be included in the data.
    """

    def __init__(self, iso3, geo_bounding_box: GeoBoundingBox):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )
        self._iso3 = iso3
        # round coordinates to correspond with the grid IRI publishes
        # its data on, which is 1 degree resolution
        # non-rounded coordinates can be given to the URL which then
        # automatically rounds them, but for file saving we prefer to do
        # this ourselves
        geo_bounding_box.round_coords(round_val=1, offset_val=0)
        self._geobb = geo_bounding_box

    def download(
        self,
        iri_auth: str,
        # question: I chose to make dominant a function arg
        # instead of class attr. Does this make sense?
        # Does it even make sense to have dominant as an arg
        # or should we have a separate class for it?
        dominant: bool = False,
    ):
        """
        Download the IRI seasonal tercile forecast as NetCDF file.

        Parameters
        ----------
        iri_auth: str
            iri key for authentication. An account is
            needed to get this key config. For an account this key might
            be changed over time, so might need to update it regularly
        dominant: bool
            Two datasets with the tercile forecast exists. One reports
            the probability per tercile, which is downloaded with
            dominant=False. The other only reports the probability
            of the dominant tercile, which is downloaded with
            dominant=True. In this data low negative probabilities indicate the
            below average tercile is dominant and high positive that the above
            average tercile is dominant.

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> from aatoolbox.utils.geoboundingbox import GeoBoundingBox
        >>> (from aatoolbox.datasources.iri.
        ... iri_forecast_seasonal_precipitation_tercile import IriForecast)
        #retrieve the bounding box to download data for
        >>> iso3="bfa"
        >>> pipeline_iso = Pipeline(iso3)
        >>> codab_admin1 = pipeline_iso.load_codab(admin_level=1)
        >>> geo_bounding_box = GeoBoundingBox.from_shape(codab_admin1)
        #initialize class and download data
        >>> iri=IriForecast(iso3,geo_bounding_box)
        #the iri auth str can e.g. be saved as an env var
        >>> iri.download(os.getenv("IRI_AUTH"))
        """
        output_filepath = self._get_raw_path(dominant=dominant)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        # strange things happen when just overwriting the file, so delete it
        # first if it already exists
        output_filepath.unlink(missing_ok=True)

        url = self._get_url(dominant=dominant)
        logger.info("Downloading IRI NetCDF file.")
        response = requests.get(
            url,
            # have to authenticate by using a cookie
            cookies={"__dlauth_id": iri_auth},
        )
        with open(output_filepath, "wb") as out_file:
            out_file.write(response.content)

        return output_filepath

    def _get_url(self, dominant: bool):
        # question: best to define url here or as global constant?
        base_url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
            ".NMME_Seasonal_Forecast/.Precipitation_ELR/"
        )
        if dominant:
            base_url += ".dominant/"
        else:
            base_url += ".prob/"
        return (
            f"{base_url}"
            f"X/%28{self._geobb.west}%29%28{self._geobb.east}%29RANGEEDGES/"
            f"Y/%28{self._geobb.north}%29%28{self._geobb.south}%29RANGEEDGES/"
            "data.nc"
        )

    def load(self, dominant: bool = False):
        """Preprocess and load the IRI forecast.

        dominant: bool
            Two datasets with the tercile forecast exists. One reports
            the probability per tercile, which is loaded with
            dominant=False. The other only reports the probability
            of the dominant tercile, which is loaded with
            dominant=True. In this data low negative probabilities indicate the
            below average tercile is dominant and high positive that the above
            average tercile is dominant.

        Examples
        --------
        >>> from aatoolbox.pipeline import Pipeline
        >>> from aatoolbox.utils.geoboundingbox import GeoBoundingBox
        >>> (from aatoolbox.datasources.iri.
        ... iri_forecast_seasonal_precipitation_tercile import IriForecast)
        >>> iso3="bfa"
        >>> pipeline_iso = Pipeline(iso3)
        >>> codab_admin1 = pipeline_iso.load_codab(admin_level=1)
        >>> geo_bounding_box = GeoBoundingBox.from_shape(codab_admin1)
        >>> iri=IriForecast(iso3,geo_bounding_box)
        >>> iri.load()
        """
        ds = xr.load_dataset(
            self._get_raw_path(dominant),
            decode_times=False,
            drop_variables="C",
        )

        ds = self._process(ds)
        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _process(self, ds):
        # fix dates
        ds.aat.set_time_dim(t_dim="F", inplace=True)
        ds.aat.correct_calendar(inplace=True)
        ds = xr.decode_cf(ds)

        # question: Do you think this makes more sense to do this here
        # or implement in the geoboundingbox class? i.e. restrict
        # the range and that south<north?
        # IRI downloads in the order you give the coordinates
        # and accepts both -180 to 180 longitudes and 0 to 360
        ds.aat.invert_coordinates(inplace=True)
        # TODO: invest if we want -180 to 180 or 0-360
        # and check first if it is already in correct range
        # cause applying this function twice will return
        # you the original range
        ds.aat.change_longitude_range(inplace=True)

        return ds

    def _get_raw_path(self, dominant):
        file_name = f"{self._iso3}_iri_forecast_seasonal_precipitation_tercile"
        if dominant:
            file_name += "_dominant"
        file_name += f"_{self._geobb.get_filename_repr(p=0)}.nc"
        return self._raw_base_dir / file_name
