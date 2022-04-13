"""Class to download and process USGS FEWS NET NDVI data.

Data is downloaded from the `USGS FEWS NET data portal
<https://earlywarning.usgs.gov/fews>`_. Data is
generated from eMODIS AQUA, with full methodological
details available from the various data pages
on the data portal. The available areas of coverage are:

- `North Africa<https://earlywarning.usgs.gov/fews/product/449>`_
- `East Africa<https://earlywarning.usgs.gov/fews/product/448>`_
- `Southern Africa<https://earlywarning.usgs.gov/fews/product/450>`_
- `West Africa<https://earlywarning.usgs.gov/fews/product/451>`_
- `Central Asia<https://earlywarning.usgs.gov/fews/product/493>`_
- `Yemen<https://earlywarning.usgs.gov/fews/product/502>`_
- `Central America<https://earlywarning.usgs.gov/fews/product/445>`_
- `Hispaniola<https://earlywarning.usgs.gov/fews/product/446>`_

Data is made available on the backend USGS file explorer. For example,
dekadal temporally smooth NDVI data for West Africa is available at
`this link
<https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/west/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/monthly/>`_

The products include temporally smoothed NDVI, median anomaly,
difference from the previous year, and median anomaly
presented as a percentile.

Data by USGS is published quickly after the dekad.
After about 1 month this data is updated with temporal smoothing
and error correction for cloud cover.
"""

# TODO: add progress bar
import logging
from datetime import date
from io import BytesIO
from pathlib import Path
from typing import List, Tuple, Union, cast
from urllib.error import HTTPError
from urllib.request import urlopen
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import regex as re
import rioxarray  # noqa: F401
import xarray as xr
from typing_extensions import Literal

import aatoolbox.utils.raster  # noqa: F401
from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils._dates import (
    _compare_dekads_gt,
    _compare_dekads_lt,
    _date_to_dekad,
    _dekad_to_date,
    _expand_dekads,
)

# TODO: use check_file_existence once the fix is merged
# that allows it to work on class methods (PR is open)
# from aatoolbox.utils.io import check_file_existence

logger = logging.getLogger(__name__)

_VALID_AREAS = Literal[
    "north_africa",
    "east_africa",
    "southern_africa",
    "west_africa",
    "central_asia",
    "yemen",
    "central_america",
    "hispaniola",
]
_VALID_TYPES = Literal[
    "smoothed", "percent_median", "median_anomaly", "difference"
]


class _UsgsNdvi(DataSource):
    """Base class to retrieve USGS NDVI data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    coverage_area : str
        Area of coverage
    data_type : str
        Data type
    start_date : Union[date, str, Tuple[int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    processed_file_suffix : str
        Suffix for processed file.
    """

    def __init__(
        self,
        country_config: CountryConfig,
        coverage_area: _VALID_AREAS,
        data_type: _VALID_TYPES,
        start_date: Union[date, str, Tuple[int], None] = None,
        end_date: Union[date, str, Tuple[int], None] = None,
        processed_file_suffix: str = "",
    ):
        super().__init__(
            country_config=country_config,
            module_base_dir="usgs_ndvi",
            is_public=True,
        )
        self.coverage_area = coverage_area

        # set data type directly in init because should not be
        # changed in an instance
        valid_types = _VALID_TYPES.__args__  # type: ignore
        if data_type not in valid_types:
            raise ValueError(
                "`data_type` is not a valid string value. "
                f"It must be one of {*valid_types,}"
            )
        self._data_type = data_type

        # url code and file suffix
        loc_data_types = {
            "smoothed": ("temporallysmoothedndvi", ""),
            "percent_median": ("percentofmedian", "pct"),
            "median_anomaly": ("mediananomaly", "stmdn"),
            "difference": ("differencepreviousyear", "dif"),
        }
        data_type_values = loc_data_types[data_type]
        self._data_type_url = data_type_values[0]
        self._data_type_suffix = data_type_values[1]

        # set dates for data download and processing
        if start_date is None:
            # earliest dekad of data is 2002-19
            self._start_year, self._start_dekad = 2002, 19
        elif isinstance(start_date, (str, date)):
            self._start_year, self._start_dekad = _date_to_dekad(start_date)
        else:
            self._start_year, self._start_dekad = cast(list, start_date)

        if end_date is None:
            self._end_year, self._end_dekad = _date_to_dekad(date.today())
        elif isinstance(start_date, (str, date)):
            self._end_year, self._end_dekad = _date_to_dekad(end_date)
        else:
            self._end_year, self._end_dekad = cast(list, end_date)

        # set processed file suffix
        self.processed_file_suffix = processed_file_suffix

    @property
    def coverage_area(self):
        """str: Coverage area for NDVI data.

        If the area is set to a valid value, the relevant
        URLs and directory pathings are switched.
        """
        return self._coverage_area

    @coverage_area.setter
    def coverage_area(self, value: _VALID_AREAS):
        valid_args = _VALID_AREAS.__args__  # type: ignore
        if value not in valid_args:
            raise ValueError(
                "`coverage_area` is not a valid string value. "
                f"It must be one of '{*valid_args,}'."
            )
        self._coverage_area = value

        # also adjust relevant URL and file attributes
        # when coverage area is set
        loc_region_types = {
            "north_africa": ("africa/north", "na"),
            "east_africa": ("africa/east", "ea"),
            "southern_africa": ("africa/southern", "sa"),
            "west_africa": ("africa/west", "wa"),
            "central_asia": ("asia/centralasia", "cta"),
            "yemen": ("asia/middleeast/yemen", "yem"),
            "central_america": ("lac/camcar/centralamerica", "ca"),
            "hispaniola": ("lac/camcar/caribbean/hispaniola", "hi"),
        }

        region_values = loc_region_types[value]
        self._region_url = region_values[0]
        self._region_prefix = region_values[1]

        # reset raw base dir to global
        # TODO: change once datasource module
        # dir pathing has been changed
        self._raw_base_dir = self._get_base_dir(
            is_public=True, is_raw=True, is_global=True
        )

    @property
    def processed_file_suffix(self):
        """str: Suffix for processed data.

        NDVI processed data is stored by default
        in a single file. However, suffixes can be
        specified in order to aggregate data to
        different areas.
        """
        return self._processed_file_suffix

    @processed_file_suffix.setter
    def processed_file_suffix(self, value):
        if not isinstance(value, str):
            raise ValueError("`processed_file_suffix` must be a string.")
        self._processed_file_suffix = value

    def download(self, clobber: bool = False) -> Path:
        """Download raw NDVI data as .tif files.

        NDVI data is downloaded from the USGS API,
        with data for individual regions, years, and
        dekads stored as separate .tif files. No
        authentication is required. Data is downloaded
        for all available dekads from ``self.start_date``
        to ``self.end_date``.

        Parameters
        ----------
        clobber : bool, default = False
            If True, overwrites existing files

        Returns
        -------
        Path
            The downloaded filepath

        Examples
        --------
        >>> from aatoolbox import create_country_config, \
        ...  CodAB, UsgsNdviSmoothed
        >>>
        >>> # Retrieve admin 2 boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_admin2 = codab.load(admin_level=2)
        >>>
        >>> # setup NDVI
        >>> UsgsNdviSmoothed(
        ...     country_config=country_config,
        ...     coverage_area="west_africa",
        ...     start_date=[2020, 1],
        ...     end_date=[2020, 3]
        ... )
        >>> UsgsNdviSmoothed.download()
        """
        for year in range(self._start_year, self._end_year + 1):
            for dekad in range(1, 37):
                if year == self._start_year and dekad < self._start_dekad:
                    continue
                if year == self._end_year and dekad > self._end_dekad:
                    break
                else:
                    self._download_ndvi_dekad(
                        year=year, dekad=dekad, clobber=clobber
                    )
        return self._raw_base_dir

    def process(
        self,
        gdf: gpd.GeoDataFrame,
        feature_col: str,
        clobber: bool = False,
        **kwargs,
    ) -> Path:
        """Process NDVI data for specific area.

        NDVI data is clipped to the provided
        ``geometries``, usually a geopandas
        dataframes ``geometry`` feature. ``kwargs``
        are passed on to ``aat.computer_raster_stats()``.
        ``self.processed_file_suffix`` is used to define
        the unique processed files, and should be changed
        if the user wants to analyze across multiple files.

        Parameters
        ----------
        gdf : geopandas.GeoDataFrame
            GeoDataFrame with row per area for stats computation.
            If ``pd.DataFrame`` is passed, geometry column must
            have the name ``geometry``. Passed to
            ``aat.compute_raster_stats()``.
        feature_col : str
            Column in ``gdf`` to use as row/feature identifier.
            and dates. Passed to ``aat.compute_raster_stats()``.
        clobber : bool, default = False
            If True, overwrites existing processed dates. If
            the new file matches the old file, dates will be
            reprocessed and appended to the data frame. If
            files do not much, the old file will be replaced.
            If False, stats are only calculated for year-dekads
            that have not already been calculated within the
            file. However, if False and files do not match,
            value error will be raised.
        **kwargs
            Additional keyword arguments passed to
            ``aat.computer_raster_stats()``.

        Returns
        -------
        Path
            The processed path

        Examples
        --------
        >>> from aatoolbox import create_country_config, \
        ...  CodAB, UsgsNdviSmoothed
        >>>
        >>> # Retrieve admin 2 boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_admin2 = codab.load(admin_level=2)
        >>> npl_admin1 = codab.load(admin_level=2)
        >>>
        >>> # setup NDVI
        >>> UsgsNdviSmoothed(
        ...     country_config=country_config,
        ...     coverage_area="west_africa",
        ...     start_date=[2020, 1],
        ...     end_date=[2020, 3]
        ... )
        >>> UsgsNdviSmoothed.download()
        >>> UsgsNdviSmoothed.process()
        >>>
        >>> # process for admin
        >>> UsgsNdviSmoothed.processed_file_suffix = "adm1"
        >>> UsgsNdviSmoothed.process()
        """
        processed_path = self._get_processed_path()

        # get dates for processing
        process_dates = _expand_dekads(
            y1=self._start_year,
            d1=self._start_dekad,
            y2=self._end_year,
            d2=self._end_dekad,
        )

        # check to see if file exists and remove
        # if clobber or check dates already processed
        # if not
        if processed_path.is_file():
            df = self.load()

            # check that the processed file has the same analyzed
            # indicators and column for aggregation as passed
            # to process()
            cols = kwargs.get(
                "stats_list", ["mean", "std", "min", "max", "sum", "count"]
            )
            percentile_list = kwargs.get("percentile_list")
            if percentile_list is not None:
                for percent in percentile_list:
                    cols.append(f"{percent}quant")
            cols.append(feature_col)
            ncols = df.shape[1]
            exist_cols = df.columns[3:ncols].tolist()
            cols_same = cols == exist_cols

            # get dates that have already been processed
            processed_dates = df[["year", "dekad"]].values.tolist()
            if clobber:
                if cols_same:
                    # remove processed dates from file
                    # so they can be reprocessed
                    keep_rows = [
                        d not in process_dates for d in processed_dates
                    ]
                    df = df[keep_rows]
                else:
                    # erase old data frame since columns don't match
                    # but clobber=True
                    df = pd.DataFrame()
            else:
                if cols_same:
                    # remove processed dates from dates to process
                    process_dates = [
                        date for d in process_dates if d not in processed_dates
                    ]
                    if len(process_dates) == 0:
                        logger.info(
                            (
                                "No new data to process between "
                                f"{self._start_year}, "
                                f"dekad {self._start_dekad} "
                                f"and {self._end_year}, "
                                f"dekad {self._end_dekad}, "
                                "set `clobber = True` to re-process this data."
                            )
                        )
                        return processed_path
                else:
                    raise ValueError(
                        (
                            "`clobber` set to False but "
                            "the statistics and column to "
                            "process to do not match existing "
                            "processed file. Use `self.load()`"
                            "to check existing processed file "
                            "and reconcile call to `process()`. "
                            "Consider setting the "
                            "`self.processed_file_suffix` if you "
                            "want to store separate analysis (such "
                            "as at a different disaggregation)."
                        )
                    )
        else:
            # empty data frame for concatting later
            df = pd.DataFrame()

        # process data for necessary dates
        data = [df]
        for d in process_dates:
            da = self.load_raster(d)
            stats = da.aat.compute_raster_stats(
                gdf=gdf, feature_col=feature_col, **kwargs
            )
            data.append(stats)

        # join data together and sort
        df = pd.concat(data)
        df.sort_values(by="date", inplace=True)
        df.reset_index(inplace=True, drop=True)

        # saving file
        self._processed_base_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(processed_path, index=False)

        return processed_path

    def load(self) -> pd.DataFrame:
        """
        Load the processed USGS NDVI data.

        Returns
        -------
        pd.DataFrame
            The processed NDVI dataset.

        Raises
        ------
        FileNotFoundError
            If the requested file cannot be found.

        Examples
        --------
        >>> from aatoolbox import create_country_config, \
        ...  CodAB, UsgsNdviSmoothed
        >>>
        >>> # Retrieve admin 2 boundaries for Nepal
        >>> country_config = create_country_config(iso3="npl")
        >>> codab = CodAB(country_config=country_config)
        >>> npl_admin2 = codab.load(admin_level=2)
        >>>
        >>> # setup NDVI
        >>> UsgsNdviSmoothed(
        ...     country_config=country_config,
        ...     coverage_area="west_africa",
        ...     start_date=[2020, 1],
        ...     end_date=[2020, 3]
        ... )
        >>> UsgsNdviSmoothed.download()
        >>> UsgsNdviSmoothed.process()
        >>> UsgsNdviSmoothed.load()
        """
        processed_path = self._get_processed_path()
        try:
            df = pd.read_csv(processed_path, parse_dates=["date"])
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"Cannot open the CSV file {processed_path.name}. "
                f"Make sure that you have already called the 'process' method "
                f"and that the file {processed_path} exists."
            ) from err

        # filter loaded data frame between our instances dates
        load_dates = _expand_dekads(
            y1=self._start_year,
            d1=self._start_dekad,
            y2=self._end_year,
            d2=self._end_dekad,
        )
        loaded_dates = df[["year", "dekad"]].values.tolist()
        keep_rows = [d in load_dates for d in loaded_dates]
        df = df[keep_rows]

        return df

    def load_raster(self, date: Union[date, str, Tuple[int]]) -> xr.DataArray:
        """Load raster for specific year and dekad.

        Parameters
        ----------
        date : Union[date, str, Tuple[int]]
            Date. Can be passed as a ``datetime.date``
            object and the relevant dekad will be determined,
            as a date string in ISO8601 format, or as a
            year-dekad tuple, i.e. (2020, 1).

        Returns
        -------
        xr.DataArray
            Data array of NDVI data.

        Raises
        ------
        FileNotFoundError
            If the requested file cannot be found.
        """
        if isinstance(date, (tuple, str)):
            year, dekad = _date_to_dekad(date)
        else:
            year, dekad = cast(list, date)

        filepath = self._get_raw_path(year=year, dekad=dekad)
        try:
            da = rioxarray.open_rasterio(filepath)
            # assign coordinates for year/dekad
            # time dimension
            da = (
                da.assign_coords(
                    {
                        "year": year,
                        "dekad": dekad,
                        "date": _dekad_to_date(year=year, dekad=dekad),
                    }
                )
                .expand_dims("date")
                .squeeze("band", drop=True)
            )

            return da

        except FileNotFoundError as err:
            # check if the requested date is outside the instance bounds
            # don't prevent loading, but use for meaningful error
            gt_end = _compare_dekads_gt(
                y1=year, d1=dekad, y2=self._end_year, d2=self._end_dekad
            )
            lt_start = _compare_dekads_lt(
                y1=year, d1=dekad, y2=self._start_year, d2=self._start_dekad
            )
            if gt_end or lt_start:
                file_warning = (
                    f"The requested year and dekad, {year}-{dekad}"
                    f"are {'greater' if gt_end else 'less'} than the "
                    f"instance {'end' if gt_end else 'start'} year and dekad"
                    f", {self._end_year if gt_end else self._start_year}-"
                    f"{self._end_dekad if gt_end else self._start_dekad}. "
                    "Calling the `download()` method will not download this "
                    "file, and you need to re-instantiate the class to "
                    "include these dates."
                )
            else:
                file_warning = (
                    "Make sure that you have called the `download()` "
                    f"method and that the file {filepath.name} exists "
                    f"in {filepath.parent}."
                )
            raise FileNotFoundError(
                f"Cannot open the .tif file {filepath}. {file_warning}"
            ) from err

    def _get_raw_filename(self, year: int, dekad: int) -> str:
        """Get raw filename (excluding file type suffix).

        Parameters
        ----------
        year : int
            4-digit year
        dekad : int
            Dekad

        Returns
        -------
        str
            File path prefix for .zip file at URL and
            for .tif files stored within the .zip
        """
        file_name = (
            f"{self._region_prefix}{year-2000:02}"
            f"{dekad:02}{self._data_type_suffix}"
        )
        return file_name

    def _get_raw_path(self, year: int, dekad: int) -> Path:
        """Get raw filepath.

        Parameters
        ----------
        year : int
            4-digit year
        dekad : int
            Dekad

        Returns
        -------
        Path
            Path to raw file
        """
        filename = self._get_raw_filename(year=year, dekad=dekad)
        return self._raw_base_dir / f"{filename}.tif"

    def _list_downloaded_dekads(self) -> List[List[int]]:
        """Get list of downloaded years/dekads.

        Returns
        -------
        List
            List of year and dekad lists for files already
            in the system.
        """
        regex = fr"{self._region_prefix}[0-9]{{4}}{self._data_type_suffix}.tif"
        downloaded_dekads = [
            self._fp_year_dekad(filename)
            for filename in self._raw_base_dir
            if re.search(regex, filename)
        ]
        return downloaded_dekads

    def _get_processed_path(self) -> Path:
        return self._processed_base_dir / self._get_processed_filename()

    def _get_processed_filename(self) -> str:
        """Return processed filename.

        Returns the processed filename. The suffix
        is set using ``self.processed_file_suffix``.

        Returns
        -------
        str
            Processed filename
        """
        file_name = (
            f"{self._country_config.iso3}"
            f"_usgs_ndvi_{self._data_type}"
            f"{'_' if len(self.processed_file_suffix) else ''}"
            f"{self.processed_file_suffix}.csv"
        )
        return file_name

    def _get_url(self, filename) -> str:
        """Get USGS NDVI URL.

        Parameters
        ----------
        filename : str
            File name string generated for specific year, dekad, and data type

        Returns
        -------
        str
            Download URL string
        """
        return (
            f"https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/"
            f"{self._region_url}/dekadal/emodis/ndvi_c6/{self._data_type_url}/"
            f"downloads/dekadal/{filename}.zip"
        )

    # TODO: potentially move from static method to
    # wider USGS function repository
    @staticmethod
    def _fp_year_dekad(path: Path) -> List[int]:
        """Extract year and dekad from filepath.

        Parameters
        ----------
        path : Path
            Filepath

        Returns
        -------
        list
            List of year and dekad
        """
        filename = path.stem
        # find two groups, first for year second for dekad
        regex = re.compile(r"(\d{2})(\d{2})")
        return [int(x) for x in regex.findall(filename)[0]]

    def _download_ndvi_dekad(
        self, year: int, dekad: int, clobber: bool
    ) -> None:
        """Download NDVI for specific dekad.

        Parameters
        ----------
        year : int
            Year
        dekad : int
            Dekad
        clobber : bool
            If True, overwrites existing file
        """
        filepath = self._get_raw_path(year=year, dekad=dekad)
        self._download(filepath=filepath, clobber=clobber)

    # @check_file_existence
    def _download(self, filepath: Path, clobber: bool):
        # filepath just necessary for checking file existence
        # now just extract filepath
        filename = filepath.stem

        url = self._get_url(filename=filename)
        try:
            resp = urlopen(url)
        except HTTPError:
            year, dekad = self._fp_year_dekad(filepath)
            logger.error(
                f"No NDVI data available for "
                f"dekad {dekad} of {year}, skipping."
            )
            pass

        # open file within memory
        zf = ZipFile(BytesIO(resp.read()))

        # extract single .tif file from .zip
        for file in zf.infolist():
            if file.filename.endswith(".tif"):
                # rename the file to standardize to name of zip
                file.filename = f"{filename}.tif"
                zf.extract(file, self._raw_base_dir)

        resp.close()
        return filepath


class UsgsNdviSmoothed(DataSource):
    """Base class to retrieve smoothed NDVI data.

    The retrieved data is the smoothed NDVI values
    processed by the USGS. Temporal smoothing is done
    to adjust for cloud cover and other errors.
    Data for the 3 most recent dekads is not fully
    smoothed, and are re-smoothed at the end of the
    3 dekad period.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    coverage_area : str
        Area of coverage
    start_date : Union[date, str, Tuple[int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    processed_file_suffix : str
        Suffix for processed file.
    """

    def __init__(
        self,
        country_config: CountryConfig,
        coverage_area: _VALID_AREAS,
        start_date: Union[date, str, Tuple[int], None] = None,
        end_date: Union[date, str, Tuple[int], None] = None,
        processed_file_suffix: str = "",
    ):
        super().__init__(
            country_config=country_config,
            coverage_area=coverage_area,
            data_type="smoothed",
            start_date=start_date,
            end_date=end_date,
            processed_file_suffix=processed_file_suffix,
        )


class UsgsNdviPctMedian(DataSource):
    """Base class to retrieve % of median NDVI.

    The retrieved data is the percent of median NDVI
    values calculated from 2003 - 2017, as
    processed by the USGS.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    coverage_area : str
        Area of coverage
    start_date : Union[date, str, Tuple[int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    processed_file_suffix : str
        Suffix for processed file.
    """

    def __init__(
        self,
        country_config: CountryConfig,
        coverage_area: _VALID_AREAS,
        start_date: Union[date, str, Tuple[int], None] = None,
        end_date: Union[date, str, Tuple[int], None] = None,
        processed_file_suffix: str = "",
    ):
        super().__init__(
            country_config=country_config,
            coverage_area=coverage_area,
            data_type="percent_median",
            start_date=start_date,
            end_date=end_date,
            processed_file_suffix=processed_file_suffix,
        )


class UsgsNdviMedianAnomaly(DataSource):
    """Base class to retrieve NDVI anomaly data.

    The retrieved data is NDVI anomaly data calculated
    as a subtraction of the median value from the
    current value. Negative values indicate less
    vegetation than the median, positive values indicate
    more vegetation.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    coverage_area : str
        Area of coverage
    start_date : Union[date, str, Tuple[int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    processed_file_suffix : str
        Suffix for processed file.
    """

    def __init__(
        self,
        country_config: CountryConfig,
        coverage_area: _VALID_AREAS,
        start_date: Union[date, str, Tuple[int], None] = None,
        end_date: Union[date, str, Tuple[int], None] = None,
        processed_file_suffix: str = "",
    ):
        super().__init__(
            country_config=country_config,
            coverage_area=coverage_area,
            data_type="median_anomaly",
            start_date=start_date,
            end_date=end_date,
            processed_file_suffix=processed_file_suffix,
        )


class UsgsNdviYearDifference(DataSource):
    """Base class to retrieve NDVI year difference data.

    The retrieved data is NDVI yearly difference data,
    calculated as the subtraction of the previous year's
    NDVI value from the current year's. Negative
    values indicate the current vegetation is less
    than the previous year's, positive that there
    is more vegetation in the current year.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    coverage_area : str
        Area of coverage
    start_date : Union[date, str, Tuple[int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    processed_file_suffix : str
        Suffix for processed file.
    """

    def __init__(
        self,
        country_config: CountryConfig,
        coverage_area: _VALID_AREAS,
        start_date: Union[date, str, Tuple[int], None] = None,
        end_date: Union[date, str, Tuple[int], None] = None,
        processed_file_suffix: str = "",
    ):
        super().__init__(
            country_config=country_config,
            coverage_area=coverage_area,
            data_type="percent_median",
            start_date=start_date,
            end_date=end_date,
            processed_file_suffix=processed_file_suffix,
        )
