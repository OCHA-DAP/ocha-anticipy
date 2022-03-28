"""Class to download and load CHIRPS observational precipitation data.

Data is downloaded from `IRI's maproom
<http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0>`_
"""

import logging
from pathlib import Path
from typing import Optional, Union

import requests
import xarray as xr
from typing_extensions import Literal

import aatoolbox.utils.raster  # noqa: F401
from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox
from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "chirps"


class Chirps(DataSource):
    """
    Base class to retrieve CHIRPS observational precipitation data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should be included in the
        data.
    frequency: str
    resolution: float
        resolution of data to be downloaded. Can be
        0.05 or 0.25
    #TODO: check if want to allow dekad as well
    frequency: str
        Time aggregation of the data to be downloaded.
        Can be "daily", "dekad", or "monthly"
    #TODO: add example
    """

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        # TODO: use input arg or have separate class for both?
        frequency: Literal["daily", "monthly"],
        resolution: float = 0.05,
    ):
        super().__init__(
            country_config=country_config,
            module_base_dir=_MODULE_BASENAME,
            is_public=True,
        )
        # round coordinates to correspond with the grid IRI publishes
        # its data on, which is 1 degree resolution
        # non-rounded coordinates can be given to the URL which then
        # automatically rounds them, but for file saving we prefer to do
        # this ourselves
        # TODO: We should probably make a copy of this as it's being
        #  passed directly by the user
        geo_bounding_box.round_coords(round_val=resolution)
        self._geobb = geo_bounding_box
        self._frequency = frequency

        if resolution not in (0.05, 0.25):
            raise ValueError(
                f"The given resolution is {resolution}, which is "
                "not available. Has to be 0.05 or 0.25."
            )

        if self._frequency == "monthly" and resolution == 0.25:
            logger.error(
                "Monthly data with a resolution of 0.25 is not "
                "available. Automatically switching to 0.05 "
                "resolution."
            )
            resolution = 0.05

        self._resolution = resolution

    def download(
        self,
        min_year: int = None,
        max_year: int = None,
        clobber: bool = False,
    ):
        """
        Download the CHIRPS observed precipitation as NetCDF file.

        #TODO: add years
        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing raw files

        Returns
        -------
        The downloaded filepath
        """
        # TODO: I am in doubt here. Reason that I splitted up to years is cause
        # all years together is pretty large
        # However, you will still have to download the last year again each
        # time you want to update data.
        # Another option is to have one file per date, but then you have so
        # many dates and probably full download takes longer
        if min_year is None:
            min_year = 1981
        if max_year is None:
            # TODO: set date to be this year
            max_year = 2022  # today.year
            # check date is at least x years into year

        for year in range(min_year, max_year + 1):
            self._download_year(year=year, clobber=clobber)

    def _download_year(self, year, clobber):
        output_filepath = self._get_raw_path(year=year)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        url = self._get_url(year=year)
        return _download(
            filepath=output_filepath,
            url=url,
            clobber=clobber,
        )

    # TODO: Combining all in one file, but does mean you have to set clobber to
    # True everytime you want to update it
    def process(self, clobber: bool = False) -> Path:
        """
        Process the CHIRPS data.

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
        return _process(filepath=processed_file_path, ds=ds, clobber=clobber)

    def load(self) -> xr.Dataset:
        """
        Load the CHIRPS data.

        Should only be called after the ``download`` and ``process`` methods
        have been executed.

        Returns
        -------
        The processed CHIRPS dataset
        """
        processed_path = self._get_processed_path()
        try:
            ds = xr.load_dataset(processed_path, decode_times=False)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"Cannot open the netcdf file {processed_path}. "
                f"Make sure that you have already called the 'process' method "
                f"and that the file {processed_path} exists. "
            ) from err
        # TODO: Save coordinate system to a general config
        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _get_file_name(self, year: Optional[Union[int, str]]) -> str:
        # set wildcard for year, to get general filename pattern
        # used to find all files with pattern in folder
        if year is None:
            year = "*"  #
        file_name = (
            f"{self._country_config.iso3}"
            f"_chirps_{year}_"
            f"{self._frequency}_r{self._resolution}"
            f"_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_file_name_processed(self) -> str:
        file_name = (
            f"{self._country_config.iso3}"
            f"_chirps_"
            f"{self._frequency}_r{self._resolution}"
            f"_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_raw_path(self, year: Optional[int]) -> Path:
        return self._raw_base_dir / self._get_file_name(year=year)

    def _get_processed_path(self) -> Path:
        return self._processed_base_dir / self._get_file_name_processed()

    def _get_url(self, year: int) -> str:
        if self._frequency == "daily":
            base_url = (
                "http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
                f".CHIRPS/.v2p0/.daily-improved/.global/."
                f"{str(self._resolution).replace('.', 'p')}/.prcp/"
            )
        else:
            base_url = (
                "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
                ".CHIRPS/.v2p0/.monthly/.global/.precipitation/"
            )

        return (
            f"{base_url}"
            f"X/%28{self._geobb.west}%29%28{self._geobb.east}%29RANGEEDGES/"
            f"Y/%28{self._geobb.north}%29%28{self._geobb.south}%29RANGEEDGES/"
            f"T/%281%20Jan%20{year}%29"
            f"%2831%20Dec%20{year}%29RANGEEDGES/"
            "data.nc"
        )

    def _load_raw(self) -> xr.Dataset:
        raw_path = self._get_raw_path(year=None)
        filepath_list = list(raw_path.parents[0].glob(raw_path.name))
        try:
            with xr.open_mfdataset(
                filepath_list,
                decode_times=False,
            ) as ds:
                # include the names of all files that are included in the ds
                ds.attrs["included_files"] = [f.stem for f in filepath_list]
                #     ds.to_netcdf(output_filepath)
                # ds = xr.load_dataset(
                #     self._get_raw_path(year=year),
                #     decode_times=False,
                # )
                return ds
        # TODO: change to if filepath_list is empty
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"Cannot find the netcdf file "
                f"{self._get_raw_path(year=None)}. Make "
                f"sure that you have already called the 'download' method "
                f"and that the file {self._get_raw_path(year=None)} exists. "
            ) from err


@check_file_existence
def _download(filepath: Path, url: str, clobber: bool) -> Path:
    logger.info("Downloading CHIRPS NetCDF file.")
    response = requests.get(
        url,
    )
    with open(filepath, "wb") as out_file:
        out_file.write(response.content)
    return filepath


@check_file_existence
def _process(filepath: Path, ds, clobber: bool) -> Path:
    # fix dates
    ds.aat.correct_calendar(inplace=True)
    ds = xr.decode_cf(ds)

    # IRI downloads in the order you give the coordinates
    # so make sure to invert them
    # IRI accepts -180 to 180 longitudes and 0 to 360
    # but automatically converts them to -180 to 180
    # so we don't need to do that
    # TODO: can be removed once we have a check in the
    #  geoboundingbox class for south<north
    # TODO: for some reason the `inplace` is not working
    #  re-add when we fixed that
    ds = ds.aat.invert_coordinates()
    ds.to_netcdf(filepath)
    return filepath
