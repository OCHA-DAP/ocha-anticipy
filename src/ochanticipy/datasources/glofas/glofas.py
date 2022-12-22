"""Base class for downloading and processing GloFAS river discharge data."""
import logging
import time
from abc import abstractmethod
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import List, Tuple, Union

import cdsapi
import numpy as np
import xarray as xr
from dateutil import rrule

from ochanticipy.config.countryconfig import CountryConfig
from ochanticipy.datasources.datasource import DataSource
from ochanticipy.utils.dates import get_date_from_user_input
from ochanticipy.utils.geoboundingbox import GeoBoundingBox

_MODULE_BASENAME = "glofas"
_HYDROLOGICAL_MODEL = "lisflood"
_RIVER_DISCHARGE_VAR = "dis24"
_REQUEST_SLEEP_TIME = 60  # seconds

logger = logging.getLogger(__name__)


@dataclass
class _QueryParams:
    """
    Class to keep track of CDS query input and output.

    Parameters
    ----------
    filepath: Path
        Full filepath of downloaded CDS file
    query: dict
        Output of _get_query() method, to be submitted to the CDS API
    request_id: str, default = None
        Request ID from CDS, only set after request has been made
    downloaded: bool, default = False
        Whether or not the file has yet been downloaded
    """

    filepath: Path
    query: dict
    request_id: str = None  # type: ignore # TODO: fix
    downloaded: bool = False


class Glofas(DataSource):
    """
    Base class for all GloFAS data downloading and processing.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        The bounding coordinates of the area that should be included
    cds_name : str
        The name of the dataset in CDS
    system_version : str
        An input to the CDS query
    product_type : str or list
        Which product types from the dataset are requested
    date_variable_prefix : str
        Some dates require a prefix for the CDS API query
    frequency : str
        How to split the query (and thus files): in years, months, or days.
        Depends on the maximum query size of the product
    coord_names : list
        Coordinate names in the xarray dataset
    start_date_min: date
        The minimum allowed start date
    end_date_max: date, default = None
        The maximum allowed end date
    start_date : Union[date, str], default = None
        The starting date for the dataset
    end_date : Union[date, str], default = None
        The ending date for the dataset
    leadtime_max : int, default = None
        The maximum lead time in days, for forecast or reforecast data
    """

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        cds_name: str,
        system_version: str,
        product_type: Union[str, List[str]],
        date_variable_prefix: str,
        frequency: int,
        coord_names: List[str],
        start_date_min: date,
        end_date_max: date = None,
        start_date: Union[date, str] = None,
        end_date: Union[date, str] = None,
        leadtime_max: int = None,
    ):
        super().__init__(
            country_config=country_config,
            datasource_base_dir=_MODULE_BASENAME,
            is_public=True,
        )
        # The GloFAS API on CDS requires coordinates have the format x.x5
        self._geo_bounding_box = geo_bounding_box.round_coords(
            offset_val=0.05, round_val=0.1
        )
        self._start_date, self._end_date = _set_dates(
            start_date_min=start_date_min,
            end_date_max=end_date_max,
            start_date=start_date,
            end_date=end_date,
        )
        self._cds_name = cds_name
        self._system_version = system_version
        self._product_type = product_type
        self._date_variable_prefix = date_variable_prefix
        self._frequency = frequency
        self._coord_names = coord_names
        self._leadtime_max = leadtime_max
        self._forecast_type = type(self).__name__.replace("Glofas", "").lower()
        self._date_range = rrule.rrule(
            freq=self._frequency,
            dtstart=self._start_date,
            until=self._end_date,
        )

    def download(  # type: ignore
        self,
        clobber: bool = False,
    ) -> List[Path]:
        """
        Download the GloFAS data by querying CDS.

        The raw GloFAS data is available as a global raster in CDS. This method
        downloads the raster files for the specified region of interest
        and date range. The files are in GRIB format and are split up either
        by day, month, or year depending on the GloFAS product.

        Parameters
        ----------
        clobber : bool, default = False
            Overwrite files that were already downloaded

        Returns
        -------
        A list paths of downloaded files

        """
        msg = (
            f"Downloading GloFAS {self._forecast_type} "
            f"for {self._start_date} - {self._end_date}"
        )
        if self._leadtime_max is not None:
            msg += f"and up to {self._leadtime_max} day lead time"
        logger.info(msg)

        # Make directory
        output_directory = self._get_directory()
        output_directory.mkdir(parents=True, exist_ok=True)

        # Get list of files to open
        query_params_list = []
        for file_date in self._date_range:
            output_filepath = self._get_filepath(
                year=file_date.year,
                month=file_date.month,
                day=file_date.day,
            )
            if not clobber and output_filepath.exists():
                continue
            query_params_list.append(
                _QueryParams(
                    filepath=output_filepath,
                    query=self._get_query(
                        year=file_date.year,
                        month=file_date.month,
                        day=file_date.day,
                    ),
                )
            )

        download_filepaths = self._download(
            query_params_list=query_params_list
        )
        logger.info(
            f"Downloaded {len(download_filepaths)} files to {output_directory}"
        )
        logger.debug(f"Files downloaded: {download_filepaths}")
        return download_filepaths

    def process(  # type: ignore
        self,
        clobber: bool = False,
    ) -> List[Path]:
        """
        Process the downloaded GloFAS files.

        For each raw GRIB file, read it in and extract the river discharge
        from the reporting point coordinates specified in the configuration
        file. Saves the output as a NetCDF file, where files are split by day,
        month or year depending on the GloFAS product.

        Parameters
        ----------
        clobber : bool, default = False
            Overwrite files that were already processed

        Returns
        -------
        A list paths of processed files

        """
        logger.info(
            f"Processing GloFAS {self._forecast_type} for "
            f"{self._start_date} - {self._end_date} and up to "
            f"{self._leadtime_max} day lead time"
        )
        # Make the directory
        output_directory = self._get_directory(is_processed=True)
        output_directory.mkdir(parents=True, exist_ok=True)
        # Get list of files to open
        processed_filepaths = []
        for file_date in self._date_range:
            input_filepath = self._get_filepath(
                year=file_date.year,
                month=file_date.month,
                day=file_date.day,
            )
            output_filepath = self._get_filepath(
                year=file_date.year,
                month=file_date.month,
                day=file_date.day,
                is_processed=True,
            )
            if not clobber and output_filepath.exists():
                continue
            logger.debug(f"Processing {input_filepath}")
            ds_raw = self._load_single_file(
                input_filepath=input_filepath,
                filepath=output_filepath,
                clobber=clobber,
            )
            ds_processed = self._get_reporting_point_dataset(ds=ds_raw)
            # NetCDF doesn't like to overwrite files
            if output_filepath.exists():
                output_filepath.unlink()
            ds_processed.to_netcdf(output_filepath)
            processed_filepaths.append(output_filepath)
            logger.debug(f"Wrote file to {output_filepath}")
        logger.info(
            f"Processed {len(processed_filepaths)} files to {output_directory}"
        )
        logger.debug(f"Files downloaded: {processed_filepaths}")
        return processed_filepaths

    def load(
        self,
    ) -> xr.Dataset:
        """
        Load the processed GloFAS data as an xarray.DataSet.

        Returns
        -------
        A single xarray dataset containing all GloFAS reporting points
        and their associated river discharge

        """
        filepath_list = [
            self._get_filepath(
                year=dataset_date.year,
                month=dataset_date.month,
                day=dataset_date.day,
                is_processed=True,
            )
            for dataset_date in self._date_range
        ]
        with xr.open_mfdataset(
            filepath_list, preprocess=self._preprocess_load
        ) as ds:
            return ds

    def _get_filepath(
        self,
        year: int,
        month: int = None,
        day: int = None,
        is_processed: bool = False,
    ) -> Path:
        """Get downloaded / processed filepaths based on GloFAS product."""
        filename = f"{self._country_config.iso3}_{self._cds_name}_{year}"
        if self._frequency in [rrule.MONTHLY, rrule.DAILY]:
            filename += f"-{str(month).zfill(2)}"
        if self._frequency == rrule.DAILY:
            filename += f"-{str(day).zfill(2)}"
        if self._leadtime_max is not None:
            filename += f"_ltmax{str(self._leadtime_max).zfill(2)}d"
        filename += f"_{self._geo_bounding_box.get_filename_repr(p=2)}"
        if is_processed:
            filename += "_processed.nc"
        else:
            filename += ".grib"
        return self._get_directory(is_processed=is_processed) / Path(filename)

    def _get_directory(self, is_processed: bool = False) -> Path:
        """Get download / processed directory for GloFAS product."""
        return (
            self._processed_base_dir
            if is_processed
            else self._raw_base_dir / self._cds_name
        )

    def _download(
        self,
        query_params_list: List[_QueryParams],
    ) -> List[Path]:
        """
        Download the GloFAS data from CDS.

        Uses query_params_list, which is a list of API request input dicts,
        to query the CDS API, and for each query,  store the request
        ID that is returned, and the downloaded state.

        Then loops through the list of request, checking each one to see
        if it has been completed on the CDS side. If so, it's downloaded,
        and then removed from the list of requests. The process continues
        until the request list is empty
        """
        # First make the requests to the CDS client and store request number
        for query_params in query_params_list:
            logger.debug(f"Making request {query_params.query}")
            query_params.request_id = (
                cdsapi.Client(wait_until_complete=False, delete=False)
                .retrieve(name=self._cds_name, request=query_params.query)
                .reply["request_id"]
            )
        # Loop through the request list and check status until all requests
        # are downloaded
        downloaded_filepaths = []
        while query_params_list:
            for query_params in query_params_list:
                result = cdsapi.api.Result(
                    client=cdsapi.Client(
                        wait_until_complete=False, delete=False
                    ),
                    reply={"request_id": query_params.request_id},
                )
                result.update()
                state = result.reply["state"]
                logger.debug(
                    f"For request {query_params.request_id} and filename "
                    f"{query_params.filepath}, state is {state}"
                )
                if state == "completed":
                    result.download(query_params.filepath)
                    query_params.downloaded = True
                    downloaded_filepaths.append(query_params.filepath)
                elif state == "failed":
                    raise RuntimeError("Query has failed, try again")
            # Remove requests that have been downloaded
            query_params_list = [
                query_params
                for query_params in query_params_list
                if not query_params.downloaded
            ]
            # Sleep a bit before the next loop so that we're not
            # hammering on cds
            if query_params_list:
                time.sleep(_REQUEST_SLEEP_TIME)
        return downloaded_filepaths

    def _get_query(
        self,
        year: int,
        month: int = None,
        day: int = None,
    ) -> dict:
        """Create dictionary for CDS API query input."""
        query = {
            "variable": "river_discharge_in_the_last_24_hours",
            "format": "grib",
            "product_type": self._product_type,
            "system_version": self._system_version,
            "hydrological_model": _HYDROLOGICAL_MODEL,
            f"{self._date_variable_prefix}year": str(year),
            f"{self._date_variable_prefix}month": str(month).zfill(2)
            if self._frequency in [rrule.MONTHLY, rrule.DAILY]
            else [str(x + 1).zfill(2) for x in range(12)],
            f"{self._date_variable_prefix}day": str(day).zfill(2)
            if self._frequency == rrule.DAILY
            else [str(x + 1).zfill(2) for x in range(31)],
            "area": [
                self._geo_bounding_box.lat_max,
                self._geo_bounding_box.lon_min,
                self._geo_bounding_box.lat_min,
                self._geo_bounding_box.lon_max,
            ],
        }
        if self._leadtime_max is not None:
            leadtime = list(np.arange(self._leadtime_max) + 1)
            query["leadtime_hour"] = [
                str(single_leadtime * 24) for single_leadtime in leadtime
            ]
        logger.debug(f"Query: {query}")
        return query

    @abstractmethod
    def _load_single_file(self, *args, **kwargs) -> xr.Dataset:
        """Process a single raw raster file."""
        pass

    def _get_reporting_point_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """Convert raw raster to processed that uses reporting points."""
        if self._country_config.glofas is None:
            raise KeyError(
                "The country configuration file does not contain "
                "any reporting point coordinates. Please update the "
                "configuration file and try again."
            )
        # Check that lat and lon of reporting points are in the bounds
        for reporting_point in self._country_config.glofas.reporting_points:
            if (
                not ds.longitude.min()
                < reporting_point.lon
                < ds.longitude.max()
            ):
                raise IndexError(
                    f"ReportingPoint {reporting_point.id} has out-of-bounds "
                    f"lon value of {reporting_point.lon} (data lon ranges "
                    f"from {ds.longitude.min().values} "
                    f"to {ds.longitude.max().values})"
                )
            if not ds.latitude.min() < reporting_point.lat < ds.latitude.max():
                raise IndexError(
                    f"ReportingPoint {reporting_point.id} has out-of-bounds "
                    f"lat value of {reporting_point.lat} (data lat ranges "
                    f"from {ds.latitude.min().values} "
                    f"to {ds.latitude.max().values})"
                )
        # If reporting points fit then return processed dataset
        return xr.Dataset(
            data_vars={
                reporting_point.name: (
                    self._coord_names,
                    ds.sel(
                        longitude=reporting_point.lon,
                        latitude=reporting_point.lat,
                        method="nearest",
                    )[_RIVER_DISCHARGE_VAR].data,
                )
                # fmt: off
                for reporting_point in
                self._country_config.glofas.reporting_points
                # fmt: on
            },
            coords={
                coord_name: ds[coord_name] for coord_name in self._coord_names
            },
        )

    @staticmethod
    def _preprocess_load(ds: xr.Dataset) -> xr.Dataset:
        """Preprocessing to do before loading the processed data."""
        return ds


def _set_dates(
    start_date_min: date,
    end_date_max: date = None,
    start_date: Union[date, str] = None,
    end_date: Union[date, str] = None,
) -> Tuple[date, date]:
    """Adjust date types and check against limits."""
    # TODO: perhaps this is general enough to be useful for other data sources?
    # Set any missing dates
    if end_date_max is None:
        end_date_max = date.today()
    if start_date is None:
        start_date = start_date_min
    if end_date is None:
        end_date = end_date_max
    # Make sure that dates are all date type
    start_date = get_date_from_user_input(start_date)
    end_date = get_date_from_user_input(end_date)
    # Check against bounds
    if start_date > end_date:
        raise ValueError(
            "Please ensure that the start date is <= the end_date"
        )
    if start_date < start_date_min:
        logger.warning(
            f"Start date {start_date} is too far in the past,"
            f"setting to {start_date_min}"
        )
        start_date = start_date_min
    if end_date > end_date_max:
        logger.warning(
            f"End date {end_date} is too far in the future,"
            f"setting to {end_date_max}"
        )
        end_date = end_date_max
    return start_date, end_date
