"""Class to download and load CHIRPS observational precipitation data."""
import calendar
import logging
from abc import abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests
import xarray as xr

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.check_file_existence import check_file_existence
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)


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
        "monthly"
    resolution: float, default = 0.05
        resolution of data to be downloaded. Can be
        0.05 or 0.25
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

        valid_frequencies = ("daily", "monthly")
        if self._frequency not in valid_frequencies:
            raise ValueError(
                f"The available frequencies are {*valid_frequencies,}"
            )

        valid_resolutions = (0.05, 0.25)
        if resolution not in valid_resolutions:
            raise ValueError(
                f"The given resolution is {self._resolution}, which is "
                "not available. Has to be 0.05 or 0.25."
            )

        if self._frequency == "monthly" and self._resolution == 0.25:
            logger.error(
                f"{self._frequency}.capitalize() data with a resolution of "
                "0.25 is not available. Automatically switching to 0.05 "
                "resolution."
            )
            self._resolution = 0.05

    # mypy will give error Signature of "download" incompatible with supertype
    # "DataSource" due to the arguments not being present in
    # `DataSource`. This is however valid so ignore mypy.
    def download(
        self,
        start_year: int = None,
        end_year: int = None,
        start_month: int = None,
        end_month: int = None,
        start_day: int = None,
        end_day: int = None,
        clobber: bool = False,
    ):
        """
        Download the CHIRPS observed precipitation as NetCDF file.

        Parameters
        ----------
        start_year: int, default = None
            Data will be donwloaded starting from year `start_year`.
            If None, it is set to 1981.
        end_year: int, default = None
            Data will be donwloaded up to year `end_year`.
            If None, it is set to the year for which most recent data
            is available.
        start_month: int, default = None
            Data will be donwloaded starting from month `start_month`.
            If None, it is set to January.
        end_month: int, default = None
            Data will be donwloaded up to month `end_month`.
            If None, it is set to the December if the year was specified,
            otherwise to the month of the year for which most recent data
            is available.
        start_day: int, default = None
            Data will be donwloaded starting from day `start_day`.
            If None, it is set to 1. Argument ignored when downloading
            monthly data.
        end_day: int, default = None
            Data will be donwloaded up to day `end_day`.
            If None, it is set to the last day of the month if year and month
            were specified, otherwise to the day of the month of the year
            for which most recent data is available.
            Argument ignored when downloading monthly data.
        clobber : bool, default = False
            If True, overwrites existing raw files

        Returns
        -------
        The filepath of the last file downloaded.
        """
        if self._frequency == "monthly":
            start_day = None
            end_day = None

        # Create a list of date tuples and a summarising sentence to be printed
        date_list, sentence_to_print = self._create_date_list(
            start_year=start_year,
            end_year=end_year,
            start_month=start_month,
            end_month=end_month,
            start_day=start_day,
            end_day=end_day,
        )

        # Print information on data overwriting
        clobber_dict = {True: "", False: "not "}

        if len(date_list) > 0:
            sentence_to_print += (
                f" Data already present in the folder "
                f"will {clobber_dict[clobber]}be overwritten."
            )

        logger.info(sentence_to_print)

        # Data download
        if self._frequency == "daily":
            for (year, month, day) in date_list:
                last_filepath = self._download(
                    year=year, month=month, day=day, clobber=clobber
                )
        else:
            for (year, month) in date_list:
                last_filepath = self._download(
                    year=year, month=month, day=None, clobber=clobber
                )

        return last_filepath

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
        The filepath of the last file processed.
        """
        # Create a list with all raw data downloaded
        filepath_list = self._get_downloaded_path_list()
        if len(filepath_list) > 0:
            for filepath in filepath_list:
                ds = xr.open_dataset(filepath, decode_times=False)
                processed_file_path = self._get_processed_path(filepath)
                processed_file_path.parent.mkdir(parents=True, exist_ok=True)
                last_filepath = self._process(
                    filepath=processed_file_path, ds=ds, clobber=clobber
                )

        else:
            raise FileNotFoundError(
                "Cannot find any netcdf file for the chosen combination "
                "of frequency, resolution and area. Make sure "
                "sure that you have downloaded some data."
            )

        return last_filepath

    def load(
        self,
        start_year: int = None,
        end_year: int = None,
        start_month: int = None,
        end_month: int = None,
        start_day: int = None,
        end_day: int = None,
    ) -> xr.Dataset:
        """
        Load the CHIRPS data.

        Should only be called after the data
        has been downloaded and processed. If no arguments are specified,
        all data in the processed data folder (with the appropriate
        resolution, frequency and corresponding to the chosen location)
        will be loaded. If only some of the arguments are
        specified, the others will be set according to the conventions
        below.

        Parameters
        ----------
        start_year: int, default = None
            Data will be loaded starting from year `start_year`.
            If None, it is set to 1981.
        end_year: int, default = None
            Data will be loaded up to year `end_year`.
            If None, it is set to the year for which most recent data
            is available.
        start_month: int, default = None
            Data will be loaded starting from month `start_month`.
            If None, it is set to January.
        end_month: int, default = None
            Data will be loaded up to month `end_month`.
            If None, it is set to the December if the year was specified,
            otherwise to the month of the year for which most recent data
            is available.
        start_day: int, default = None
            Data will be loaded starting from day `start_day`.
            If None, it is set to 1. Argument ignored when downloading
            monthly data.
        end_day: int, default = None
            Data will be loaded up to day `end_day`.
            If None, it is set to the last day of the month if year and month
            were specified, otherwise to the day of the month of the year
            for which most recent data is available.
            Argument ignored when downloading monthly data.

        Returns
        -------
        The processed CHIRPS dataset.
        """
        if self._frequency == "monthly":
            start_day = None
            end_day = None

        # Get list of filepaths of files to be loaded
        filepath_list = self._get_to_be_loaded_path_list(
            start_year=start_year,
            end_year=end_year,
            start_month=start_month,
            end_month=end_month,
            start_day=start_day,
            end_day=end_day,
        )

        # Merge all files in one dataset
        if len(filepath_list) == 0:
            raise FileNotFoundError(
                "Cannot find any netcdf file for the chosen combination "
                "of frequency, resolution and area. Make sure "
                "sure that you have already called the 'process' method."
            )
        else:
            try:
                ds = xr.open_mfdataset(
                    filepath_list,
                    decode_times=False,
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

    def _download(self, year, month, day, clobber):
        # Preparatory steps for the actual download
        output_filepath = self._get_raw_path(year=year, month=month, day=day)
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
        url = self._get_url(year=year, month=month, day=day)
        # Actual download
        return self._actual_download(
            filepath=output_filepath,
            url=url,
            clobber=clobber,
        )

    def _create_date_list(
        self, start_year, end_year, start_month, end_month, start_day, end_day
    ):
        """Create list of tuples containing the range of dates of interest."""
        start_date, end_date = self._check_dates_validity(
            start_year=start_year,
            end_year=end_year,
            start_month=start_month,
            end_month=end_month,
            start_day=start_day,
            end_day=end_day,
        )

        if self._frequency == "monthly":
            date_list = [
                (d.split("-")[0], d.split("-")[1])
                for d in pd.date_range(
                    start_date,
                    end_date,
                    freq="SMS",
                )
                .strftime("%Y-%m")
                .tolist()
            ]
        elif self._frequency == "daily":
            date_list = [
                (d.split("-")[0], d.split("-")[1], d.split("-")[2])
                for d in pd.date_range(
                    start_date,
                    end_date,
                    freq="D",
                )
                .strftime("%Y-%m-%d")
                .tolist()
            ]

        # Create a sentence containing information on the downloaded data
        if len(date_list) == 0:
            sentence_to_print = (
                "No data will be downloaded. "
                "There is no {self._frequency} data available within the "
                "chosen range of dates."
            )
        else:
            sentence_to_print = (
                f"{self._frequency.capitalize()} "
                "data will be downloaded, starting from "
                f"{'-'.join(date_list[0])} to {'-'.join(date_list[-1])}."
            )

        return date_list, sentence_to_print

    def _check_dates_validity(
        self, start_year, end_year, start_month, end_month, start_day, end_day
    ):
        """
        Check dates vailidity.

        Check whether the input dates are valid, assign dates when not
        specified.
        """
        if (
            start_month is not None or start_day is not None
        ) and start_year is None:
            raise ValueError(
                "If you specify starting month or day, you also "
                "need to specify the starting year."
            )

        if (end_month is not None or end_day is not None) and end_year is None:
            raise ValueError(
                "If you specify end month or day, you also "
                "need to specify the end year."
            )

        if start_day is not None and start_month is None:
            raise ValueError(
                "If you specify the starting day, you also "
                "need to specify the starting month."
            )

        if end_day is not None and end_month is None:
            raise ValueError(
                "If you specify the end day, you also "
                "need to specify the end month."
            )

        start_avail_date = date(year=1981, month=1, day=1)
        end_avail_date = self._get_last_available_date()

        if start_year is None:
            start_year = start_avail_date.year
            start_month = start_avail_date.month
            start_day = start_avail_date.day
        if end_year is None:
            end_year = end_avail_date.year
            end_month = end_avail_date.month
            end_day = end_avail_date.day

        if start_month is None:
            start_month = 1
        if end_month is None:
            if end_year == end_avail_date.year:
                end_month = end_avail_date.month
            else:
                end_month = 12

        if start_day is None:
            start_day = 1
        if end_day is None:
            if (
                end_year == end_avail_date.year
                and end_month == end_avail_date.month
            ):
                end_day = end_avail_date.day
            else:
                end_day = calendar.monthrange(end_year, end_month)[1]

        try:
            start_date = date(
                year=start_year, month=start_month, day=start_day
            )
            end_date = date(year=end_year, month=end_month, day=end_day)
        except ValueError:
            raise ValueError(
                "(One of) the dates identified by the given inputs ("
                f"({start_year}-{start_month}-{start_day}), "
                f"({start_year}-{start_month}-{start_day})"
                ") are not valid."
            )

        if not start_avail_date <= start_date <= end_date <= end_avail_date:
            raise ValueError(
                "Make sure that the input dates are ordered in the "
                f"following way: {start_avail_date} <= {start_date} "
                f"<= {end_date} <= {end_avail_date}. The two dates above "
                "indicate the range for which CHIRPS data are "
                "currently available."
            )

        return start_date, end_date

    def _get_file_name(
        self,
        year: Optional[Union[int, str]],
        month: Optional[Union[int, str]],
        day: Optional[Union[int, str]],
    ) -> str:
        # Set wildcard for year, to get general filename pattern
        # used to find all files with pattern in folder
        if year is None:
            year = "*"
        if month is None:
            month = "*"
        else:
            if len(f"{month}") == 1:
                month = f"0{month}"
        if day is None:
            day = "*"
        else:
            if len(f"{day}") == 1:
                day = f"0{day}"

        file_name_base = (
            f"{self._country_config.iso3}_chirps_"
            f"{self._frequency}_{year}_{month}_"
        )
        if self._frequency == "daily":
            file_name_base += f"{day}_"
        file_name = (
            f"{file_name_base}"
            f"r{self._resolution}"
            f"_{self._geobb.get_filename_repr(p=0)}.nc"
        )
        return file_name

    def _get_raw_path(
        self, year: Optional[str], month: Optional[str], day: Optional[str]
    ) -> Path:
        return self._raw_base_dir / self._get_file_name(
            year=year, month=month, day=day
        )

    def _get_processed_path(self, raw_path: Path) -> Path:
        return self._processed_base_dir / raw_path.parts[-1]

    def _get_url(self, year: str, month: str, day: str) -> str:

        # Convert month from month number (in string format) to
        # three-letter name
        month_name = calendar.month_abbr[int(month)]

        base_url = (
            "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
        )

        location_url = (
            f"X/%28{self._geobb.lon_min}%29%28{self._geobb.lon_max}"
            f"%29RANGEEDGES/"
            f"Y/%28{self._geobb.lat_max}%29%28{self._geobb.lat_min}"
            f"%29RANGEEDGES/"
        )

        if self._frequency == "daily":
            url = (
                f"{base_url}"
                ".daily-improved/.global/."
                f"{str(self._resolution).replace('.', 'p')}/.prcp/"
                f"{location_url}"
                f"T/%28{day}%20{month_name}%20{year}%29%28{day}"
                f"%20{month_name}%20{year}"
                "%29RANGEEDGES/data.nc"
            )
        else:  # monthly
            url = (
                f"{base_url}"
                ".monthly/.global/.precipitation/"
                f"{location_url}"
                f"T/%28{month_name}%20{year}%29%28{month_name}%20{year}"
                "%29RANGEEDGES/data.nc"
            )
        return url

    def _get_last_available_date(self):
        """Get the most recent date for which data is available."""
        if self._frequency == "daily":
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

        else:  # monthly
            url = (
                "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0/"
                ".monthly/.global/"
                ".T/last/subgrid/0./add/T/table%3A/1/%3Atable/.csv"
            )

            datetime_object = datetime.strptime(
                pd.read_csv(url).values[0][0], "%b %Y"
            )
            day = calendar.monthrange(
                datetime_object.year, datetime_object.month
            )[1]
            datetime_object = datetime_object.replace(day=day)

        return datetime_object.date()

    def _get_downloaded_path_list(self):
        """Create a list with all raw data downloaded."""
        # Get the path where raw data is stored
        raw_path = self._get_raw_path(year=None, month=None, day=None)
        return list(raw_path.parents[0].glob(raw_path.name))

    def _get_processed_path_list(self):
        """Create a list with all raw data downloaded."""
        # Get the path where raw data is stored
        raw_path = self._get_raw_path(year=None, month=None, day=None)
        processed_path = self._get_processed_path(raw_path)
        return processed_path, list(
            processed_path.parents[0].glob(processed_path.name)
        )

    def _get_to_be_loaded_path_list(
        self,
        start_year: int = None,
        end_year: int = None,
        start_month: int = None,
        end_month: int = None,
        start_day: int = None,
        end_day: int = None,
    ):
        """Get list of filepaths of files to be loaded."""
        # If no arguments are specified, all data in the processed data folder
        # (with the appropriate resolution, frequency and corresponding to the
        # chosen location) will be loaded
        if all(
            [
                x is None
                for x in [
                    start_year,
                    end_year,
                    start_month,
                    end_month,
                    start_day,
                    end_day,
                ]
            ]
        ):
            _, filepath_list = self._get_processed_path_list()
        # otherwise data loaded according to the range given as an input
        else:
            date_list, _ = self._create_date_list(
                start_year=start_year,
                end_year=end_year,
                start_month=start_month,
                end_month=end_month,
                start_day=start_day,
                end_day=end_day,
            )
            if self._frequency == "monthly":
                filepath_list = [
                    self._get_raw_path(year=year, month=month, day=None)
                    for (year, month) in date_list
                ]
            else:
                filepath_list = [
                    self._get_raw_path(year=year, month=month, day=day)
                    for (year, month, day) in date_list
                ]
        filepath_list.sort()

        return filepath_list

    @check_file_existence
    def _actual_download(
        self, filepath: Path, url: str, clobber: bool
    ) -> Path:
        logger.info("Downloading CHIRPS NetCDF file.")
        response = requests.get(
            url,
        )
        with open(filepath, "wb") as out_file:
            out_file.write(response.content)
        return filepath

    @check_file_existence
    def _process(self, filepath: Path, ds, clobber: bool) -> Path:
        # fix dates
        ds.aat.correct_calendar(inplace=True)
        ds = xr.decode_cf(ds)
        ds.to_netcdf(filepath)
        return filepath


class ChirpsMonthly(_Chirps):
    """
    Subclass for monthly data.

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
    >>> chirps_monthly.download(
    ... start_year=2007,
    ... end_year=2020,
    ... start_month=10,
    ... end_month,2
    ... )
    >>> chirps_monthly.process()
    >>> chirps_monthly_data = chirps_daily.load()
    """

    if _Chirps.__doc__ is not None:
        __doc__ = _Chirps.__doc__ + __doc__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, frequency="monthly", **kwargs)


class ChirpsDaily(_Chirps):
    """
    Subclass for daily data.

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
    >>> chirps_daily.download(start_year=2007, start_month=10, start_day=23)
    >>> chirps_daily.process()
    >>> chirps_daily_data = chirps_daily.load(
    ... start_year=2012,
    ... end_year=end_2021,
    ... start_month=6,
    ... end_month=4
    ... )
    """

    if _Chirps.__doc__ is not None:
        __doc__ = _Chirps.__doc__ + __doc__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, frequency="daily", **kwargs)
