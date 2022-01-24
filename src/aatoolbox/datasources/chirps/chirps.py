"""Class to download and load CHIRPS observational precipitation data.

Data is downloaded from `IRI's maproom
<http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0>`_
"""

import logging

import requests
import xarray as xr

import aatoolbox.utils.raster  # noqa: F401
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

# from typing import Literal

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "chirps"


class Chirps(DataSource):
    """
    Work with CHIRPS data.

    Parameters
    ----------
    iso3 : str
        country iso3
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should
        be included in the data.
    resolution: float
        resolution of data to be downloaded. Can be
        0.05 or 0.25
    """

    def __init__(
        self,
        iso3: str,
        geo_bounding_box: GeoBoundingBox,
        # TODO: change somehow to Literal[0.05, 0.25]
        # but floats not allowed in literal
        resolution: float = 0.05,
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=True
        )
        self._iso3 = iso3
        # round coordinates to correspond with the grid IRI publishes
        # its data on, which is 1 degree resolution
        # non-rounded coordinates can be given to the URL which then
        # automatically rounds them, but for file saving we prefer to do
        # this ourselves
        geo_bounding_box.round_coords(round_val=resolution)
        self._geobb = geo_bounding_box
        self._resolution = resolution

    def download(
        self,
        # question: I chose to make dominant a function arg
        # instead of class attr. Does this make sense?
        # Does it even make sense to have the time scale as an arg
        # or should we have a separate class for it?
        frequency: str = "daily",
    ):
        """
        Download the CHIRPS observations as NetCDF file.

        Parameters
        ----------
        frequency: str
            Time aggregation of the data to be downloaded.
            Can be "daily", "dekad", or "monthly"
        Examples
        --------

        """
        # question: I chose to always remove the file and redownload
        # for all dates instead of allowing a selection of dates.
        # This because the file is <1MB and only takes about
        # a minute to download. Do you think this makes sense?

        output_filepath = self._get_raw_path(frequency=frequency)
        # strange things happen when just overwriting the file,
        # so delete it first if it already exists
        output_filepath.unlink(missing_ok=True)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        # TODO: if all dates don't exist,
        #  it still downloads and returns empty file..
        # how to fix that
        url = self._get_url(
            frequency=frequency, start_year=2021, end_year=2022
        )
        logger.info("Downloading CHIRPS NetCDF file.")
        response = requests.get(url)
        with open(output_filepath, "wb") as out_file:
            out_file.write(response.content)

        return output_filepath

    # TODO: add resolution
    def _get_url(self, frequency: str, start_year: int, end_year: int):
        # question: best to define url here or as global constant?
        base_url = (
            "http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
            ".CHIRPS/.v2p0/.daily-improved/.global/.0p05/.prcp/"
        )
        # http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/.dekad/.prcp/
        # http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/.monthly/.global/.precipitation/
        # monthly and dekad only at 0.05, just have that
        # as standard for daily as well?
        # if dominant:
        #     base_url += ".dominant/"
        # else:
        #     base_url += ".prob/"
        return (
            f"{base_url}"
            f"X/%28{self._geobb.west}%29%28{self._geobb.east}%29RANGEEDGES/"
            f"Y/%28{self._geobb.north}%29%28{self._geobb.south}%29RANGEEDGES/"
            f"T/%281%20Jan%20{start_year}%29"
            f"%2831%20Dec%20{end_year}%29RANGEEDGES/"
            "data.nc"
        )

    def load(self, frequency="daily"):
        """Preprocess and load the CHIRPS data."""
        ds = xr.load_dataset(
            self._get_raw_path(frequency),
            decode_times=False,
            # drop_variables="C",
        )
        # # fix dates
        # ds.aat.set_time_dim(t_dim="F", inplace=True)
        # ds.aat.correct_calendar(inplace=True)
        # ds = xr.decode_cf(ds)
        #
        # # question: Do you think this makes more sense to do this here
        # # or implement in the geoboundingbox class? i.e. restrict
        # # the range and that south<north?
        # # IRI downloads in the order you give the coordinates
        # # and accepts both -180 to 180 longitudes and 0 to 360
        # ds.aat.invert_coordinates(inplace=True)
        # # TODO: invest if we want -180 to 180 or 0-360
        # # and check first if it is already in correct range
        # # cause applying this function twice will return
        # # you the original range
        # ds.aat.change_longitude_range(inplace=True)

        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _get_raw_path(self, frequency):
        file_name = (
            f"{self._iso3}_chirps_{self._resolution}_{frequency}"
            f"_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return self._raw_base_dir / file_name


#
# class Chirps:
#     def __init__(
#         self,
#         country_iso3: str,
#         date_min: Union[str, date],
#         date_max: Union[str, date],
#         range_x: Tuple[str, str],
#         range_y: Tuple[str, str],
#     ):
#         self.country_iso3 = country_iso3
#
#         if not isinstance(date_min, date):
#             date_min = date.fromisoformat(date_min)
#         self.date_min = date_min
#
#         if not isinstance(date_max, date):
#             date_max = date.fromisoformat(date_max)
#         self.date_max = date_max
#
#         self.range_x = range_x
#         self.range_y = range_y
