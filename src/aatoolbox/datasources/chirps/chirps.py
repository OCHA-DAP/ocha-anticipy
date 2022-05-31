"""Class to download and load CHIRPS observational precipitation data."""
import calendar
import logging
from abc import abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import cftime
import pandas as pd
import requests
import xarray as xr

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.check_file_existence import check_file_existence
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)

frequency_dict = {"daily": "D", "monthly": "SMS"}

first_available_date = date(year=1981, month=1, day=1)


class _Chirps(DataSource):
    """
    Base class object to retrieve CHIRPS observational precipitation data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should be included in the
        data.
    frequency: str
        time resolution of the data to be downloaded. It can be "daily" or
        "monthly".
    resolution: float, default = 0.05
        resolution of data to be downloaded. Can be 0.05 or 0.25.
    """

    @abstractmethod
    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        frequency: str,
        resolution: float = 0.05,
    ):
        super().__init__(
            country_config=country_config,
            datasource_base_dir="chirps",
            is_public=True,
        )

        # round coordinates to correspond with the grid the CHIRPS data is on
        # non-rounded coordinates can be given to the URL which then
        # automatically rounds them, but for file saving we prefer to do
        # this ourselves
        self._geobb = geo_bounding_box.round_coords(round_val=resolution)
        self._frequency = frequency
        self._resolution = resolution

        valid_resolutions = (0.05, 0.25)
        if resolution not in valid_resolutions:
            raise ValueError(
                f"The given resolution is {self._resolution}, which is "
                "not available. Has to be 0.05 or 0.25."
            )

    def download(  # type: ignore
        self,
        start_date: date = first_available_date,
        end_date: date = None,
        clobber: bool = False,
    ):
        """
        Download the CHIRPS observed precipitation as NetCDF file.

        Parameters
        ----------
        start_date: datetime.date, default = None
            Data will be donwloaded starting from date `start_date`.
            If None, it is set to 1981-1-1.
        end_date: datetime.date, default = None
            Data will be donwloaded up to date `end_date`.
            If None, it is set to the date for which most recent data
            is available.
        clobber : bool, default = False
            If True, overwrites existing raw files

        Returns
        -------
        The folder where the data is downloaded.
        """
        if end_date is None:
            end_date = self._get_last_available_date()

        # Create a list of date tuples and a summarising sentence to be printed
        date_list, sentence_to_print = self._create_date_list(
            start_date=start_date,
            end_date=end_date,
        )

        logger.info(sentence_to_print)

        # Data download
        for d in date_list:
            last_filepath = self._download_prep(d=d, clobber=clobber)

        return last_filepath.parents[0]

    def process(self, clobber: bool = False):
        """
        Process the CHIRPS data.

        Should only be called after data has been download.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing processed files.

        Returns
        -------
        The folder where the data is processed.
        """
        # Create a list with all raw data downloaded
        filepath_list = self._get_downloaded_path_list()
        if not filepath_list:
            raise FileNotFoundError(
                "Cannot find any netcdf file for the chosen combination "
                "of frequency, resolution and area. Make sure "
                "sure that you have downloaded some data."
            )
        for filepath in filepath_list:
            try:
                ds = xr.open_dataset(filepath, decode_times=False)
            except ValueError as err:
                raise ValueError(
                    f"The dataset {filepath} is not a valid netcdf file: "
                    "something probbly went wrong during the download. "
                    "Try downloading the file again."
                ) from err
            processed_file_path = self._get_processed_path(filepath)
            processed_file_path.parent.mkdir(parents=True, exist_ok=True)
            last_filepath = self._process(
                filepath=processed_file_path, ds=ds, clobber=clobber
            )

        return last_filepath.parents[0]

    def load(
        self,
        start_date: date = first_available_date,
        end_date: date = None,
    ) -> xr.Dataset:
        """
        Load the CHIRPS data.

        Should only be called after the data
        has been downloaded and processed.

        Parameters
        ----------
        start_date: int, default = None
            Data will be loaded starting from date `start_date`.
            If None, it is set to 1981-1-1.
        end_date: int, default = None
            Data will be loaded up to date `end_date`.
            If None, it is set to the date for which most recent data
            is available.

        Returns
        -------
        The processed CHIRPS dataset.
        """
        # Get list of filepaths of files to be loaded
        if end_date is None:
            end_date = self._get_last_available_date()

        filepath_list = self._get_to_be_loaded_path_list(
            start_date=start_date,
            end_date=end_date,
        )

        # Merge all files in one dataset
        if not filepath_list:
            raise FileNotFoundError(
                "Cannot find any netcdf file for the chosen combination "
                "of frequency, resolution and area. Make sure "
                "sure that you have already called the 'process' method."
            )

        try:
            ds = xr.open_mfdataset(
                filepath_list,
            )
            # include the names of all files that are included in the ds
            ds.attrs["included_files"] = [f.stem for f in filepath_list]
        except FileNotFoundError as err:
            raise FileNotFoundError(
                "Cannot find one or more netcdf files corresponding "
                "to the selected range. Make sure that you already "
                "downloaded and processed those data."
            ) from err

        return ds.rio.write_crs("EPSG:4326", inplace=True)

    def _download_prep(self, d, clobber):
        # Preparatory steps for the actual download
        year = str(d.year)
        month = f"{d.month:02d}"
        day = f"{d.day:02d}"
        output_filepath = self._get_raw_path(year=year, month=month, day=day)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        url = self._get_url(year=year, month=month, day=day)
        # Actual download
        return self._download(
            filepath=output_filepath,
            url=url,
            clobber=clobber,
        )

    def _create_date_list(self, start_date, end_date):
        """Create list of tuples containing the range of dates of interest."""
        self._check_dates_validity(
            start_date=start_date,
            end_date=end_date,
        )

        date_list = pd.date_range(
            start_date, end_date, freq=frequency_dict[self._frequency]
        ).tolist()

        # Create a sentence containing information on the downloaded data
        sentence_to_print = (
            f"{self._frequency.capitalize()} "
            "data will be downloaded, starting from "
            f"{date_list[0]} to {date_list[-1]}."
        )

        return sorted(set(date_list)), sentence_to_print

    def _check_dates_validity(self, start_date, end_date):
        """Check dates vailidity."""
        start_avail_date = date(year=1981, month=1, day=1)
        end_avail_date = self._get_last_available_date()

        if not start_avail_date <= start_date <= end_date <= end_avail_date:
            raise ValueError(
                "Make sure that the input dates are ordered in the "
                f"following way: {start_avail_date} <= {start_date} "
                f"<= {end_date} <= {end_avail_date}. The two dates above "
                "indicate the range for which CHIRPS data are "
                "currently available."
            )

    def _get_file_name_base(
        self,
        year: Optional[str],
        month: Optional[str],
        day: Optional[str],
    ) -> str:
        # Set wildcard for year, to get general filename pattern
        # used to find all files with pattern in folder
        if year is None:
            year = "*"
        if month is None:
            month = "*"
        if day is None:
            day = "*"

        file_name_base = (
            f"{self._country_config.iso3}_chirps_"
            f"{self._frequency}_{year}_{month}_"
        )

        return file_name_base

    def _get_file_name(
        self,
        year: Optional[str],
        month: Optional[str],
        day: Optional[str],
    ) -> str:
        pass

    def _get_raw_path(
        self, year: Optional[str], month: Optional[str], day: Optional[str]
    ) -> Path:
        return self._raw_base_dir / self._get_file_name(
            year=year, month=month, day=day
        )

    def _get_processed_path(self, raw_path: Path) -> Path:
        return self._processed_base_dir / raw_path.parts[-1]

    def _get_base_url(self):

        base_url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
        )

        location_url = (
            f"X/%28{self._geobb.lon_min}%29%28{self._geobb.lon_max}"
            f"%29RANGEEDGES/"
            f"Y/%28{self._geobb.lat_max}%29%28{self._geobb.lat_min}"
            f"%29RANGEEDGES/"
        )

        return base_url, location_url

    def _get_url(self, year: str, month: str, day: str):
        pass

    def _get_last_available_date(self):
        pass

    def _get_downloaded_path_list(self):
        """Create a list with all raw data downloaded."""
        # Get the path where raw data is stored
        raw_path = self._get_raw_path(year=None, month=None, day=None)
        return list(raw_path.parents[0].glob(raw_path.name))

    def _get_to_be_loaded_path_list(
        self,
        start_date: date = None,
        end_date: date = None,
    ):
        """Get list of filepaths of files to be loaded."""
        date_list, _ = self._create_date_list(
            start_date=start_date,
            end_date=end_date,
        )
        filepath_list = [
            self._get_raw_path(year=date.year, month=date.month, day=date.day)
            for date in date_list
        ]
        filepath_list.sort()

        return filepath_list

    @check_file_existence
    def _download(self, filepath: Path, url: str, clobber: bool) -> Path:
        logger.info("Downloading CHIRPS NetCDF file.")
        response = requests.get(
            url,
        )
        with open(filepath, "wb") as out_file:
            out_file.write(response.content)
        return filepath

    @check_file_existence
    def _process(self, filepath: Path, ds, clobber: bool) -> Path:
        pass


class ChirpsMonthly(_Chirps):
    """
    Class object to retrieve CHIRPS observational monthly precipitation data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should be included in the
        data.

    Examples
    --------
    >>> from aatoolbox import create_country_config, CodAB, ChirpsMonthly
    >>> country_config = create_country_config(iso3="bfa")
    >>> codab = CodAB(country_config=country_config)
    >>> codab.download()
    >>> admin0 = codab.load(admin_level=0)
    >>> geo_bounding_box = GeoBoundingBox.from_shape(admin0)
    >>> chirps_monthly = ChirpsMonthly(country_config=country_config,
                                    geo_bounding_box=geo_bounding_box)
    >>> start_date = datetime.date(year=2007, month=10, day=23)
    >>> end_date = datetime.date(year=2020, month=3, day=2)

    >>> chirps_monthly.download(start_date=start_date, end_date=end_date)
    >>> chirps_monthly.process()
    >>> chirps_monthly_data = chirps_daily.load(
    ... start_date=start_date,
    ... end_date=end_dat
    ... )
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, frequency="monthly", resolution=0.05, **kwargs)

    def _get_file_name(
        self,
        year: Optional[str],
        month: Optional[str],
        day: Optional[str],
    ) -> str:

        file_name_base = self._get_file_name_base(
            year=year,
            month=month,
            day=day,
        )

        file_name = (
            f"{file_name_base}"
            f"r{self._resolution}"
            f"_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_last_available_date(self):
        """Get the most recent date for which data is available."""
        # The url contains a table where the last date available is
        # specified.
        url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
            ".monthly/.global/"
            ".T/last/subgrid/0./add/T/table%3A/1/%3Atable/.csv"
        )

        datetime_object = datetime.strptime(
            pd.read_csv(url).values[0][0], "%b %Y"
        )
        day = calendar.monthrange(datetime_object.year, datetime_object.month)[
            1
        ]
        datetime_object = datetime_object.replace(day=day)

        return datetime_object.date()

    def _get_url(self, year: str, month: str, day: str) -> str:

        # Convert month from month number (in string format) to
        # three-letter name
        month_name = calendar.month_abbr[int(month)]

        base_url, location_url = self._get_base_url()

        url = (
            f"{base_url}"
            ".monthly/.global/.precipitation/"
            f"{location_url}"
            f"T/%28{month_name}%20{year}%29%28{month_name}%20{year}"
            "%29RANGEEDGES/data.nc"
        )

        return url

    @check_file_existence
    def _process(self, filepath: Path, ds, clobber: bool) -> Path:
        # fix dates
        ds.aat.correct_calendar(inplace=True)
        ds = xr.decode_cf(ds)
        ds.to_netcdf(filepath)
        return filepath


class ChirpsDaily(_Chirps):
    """
    Class object to retrieve CHIRPS observational monthly precipitation data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the area that should be included in the
        data.
    resolution: float, default = 0.05
        resolution of data to be downloaded. Can be
        0.05 or 0.25

    Examples
    --------
    >>> from aatoolbox import create_country_config, CodAB, ChirpsMonthly
    >>> country_config = create_country_config(iso3="bfa")
    >>> codab = CodAB(country_config=country_config)
    >>> codab.download()
    >>> admin0 = codab.load(admin_level=0)
    >>> geo_bounding_box = GeoBoundingBox.from_shape(admin0)
    >>> chirps_daily = ChirpsDaily(country_config=country_config,
                                    geo_bounding_box=geo_bounding_box)
    >>> start_date = datetime.date(year=2007, month=10, day=23)
    >>> end_date = datetime.date(year=2020, month=3, day=2)

    >>> chirps_daily.download(start_date=start_date, end_date=end_date)
    >>> chirps_daily.process()
    >>> chirps_daily_data = chirps_daily.load(
    ... start_date=start_date,
    ... end_date=end_date
    ... )
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, frequency="daily", **kwargs)

    def _get_file_name(
        self,
        year: Optional[str],
        month: Optional[str],
        day: Optional[str],
    ) -> str:

        file_name_base = self._get_file_name_base(
            year=year,
            month=month,
            day=day,
        )

        file_name = (
            f"{file_name_base}"
            f"{day}_"
            f"r{self._resolution}"
            f"_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_last_available_date(self):
        """Get the most recent date for which data is available."""
        # The url contains a table where the last date available is
        # specified.
        url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
            ".daily-improved/.global/"
            f"{str(self._resolution).replace('.', 'p')}/"
            ".T/last/subgrid/0./add/T/table%3A/1/%3Atable/.csv"
        )

        datetime_object = datetime.strptime(
            pd.read_csv(url).values[0][0], "%d %b %Y"
        )

        return datetime_object.date()

    def _get_url(self, year: str, month: str, day: str) -> str:

        # Convert month from month number (in string format) to
        # three-letter name
        month_name = calendar.month_abbr[int(month)]

        base_url, location_url = self._get_base_url()

        url = (
            f"{base_url}"
            ".daily-improved/.global/."
            f"{str(self._resolution).replace('.', 'p')}/.prcp/"
            f"{location_url}"
            f"T/%28{day}%20{month_name}%20{year}%29%28{day}"
            f"%20{month_name}%20{year}"
            "%29RANGEEDGES/data.nc"
        )

        return url

    @check_file_existence
    def _process(self, filepath: Path, ds, clobber: bool) -> Path:
        # fix dates
        ds = ds.assign_coords(
            T=cftime.datetime.fromordinal(
                ds.T.values, calendar="standard", has_year_zero=True
            )
        )
        ds.aat.correct_calendar(inplace=True)
        ds = ds.rename({"prcp": "precipitation"})
        ds.to_netcdf(filepath)
        return filepath
