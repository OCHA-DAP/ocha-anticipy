"""Base class for downloading and processing GloFAS raster data."""
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Union

import cdsapi
import numpy as np
import xarray as xr
from dateutil import rrule

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

_MODULE_BASENAME = "glofas"
_HYDROLOGICAL_MODEL = "lisflood"
_REQUEST_SLEEP_TIME = 60  # seconds

logger = logging.getLogger(__name__)


@dataclass
class QueryParams:
    """
    Class to keep track of query input and output.

    Parameters
    ----------
    filepath: Path
        Full filepath of downloaded CDS file
    query: dict
        Output of _get_query() method
    request_id: str
        Added after request has been made to CDS
    """

    filepath: Path
    query: dict
    request_id: str = None  # type: ignore # TODO: fix
    downloaded: bool = False


class Glofas(DataSource):
    """
    GloFAS base class.

    Parameters
    ----------
    country_config: CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        The bounding coordinates of the geo_bounding_box that should
        be included in the data
    date_min: int
        The earliest year that the dataset is available
    date_max : int
        The most recent that the dataset is available
    cds_name : str
        The name of the dataset in CDS
    product_type : str or list of strings
        The sub-datasets that you would like to download
    date_variable_prefix : str, default = ""
        Some GloFAS datasets have the prefix "h" in front of some query keys
    frequency: str
    """

    _RIVER_DISCHARGE_VAR = "dis24"

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        date_min: datetime,
        date_max: datetime,
        cds_name: str,
        system_version: str,
        product_type: Union[str, List[str]],
        date_variable_prefix: str,
        frequency: int,
        coord_names: List[str],
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
        self._date_min = date_min
        self._date_max = date_max
        self._cds_name = cds_name
        self._system_version = system_version
        self._product_type = product_type
        self._date_variable_prefix = date_variable_prefix
        self._frequency = frequency
        self._coord_names = coord_names
        self._leadtime_max = leadtime_max
        self._forecast_type = type(self).__name__.replace("Glofas", "").lower()
        self._date_range = rrule.rrule(
            freq=self._frequency, dtstart=self._date_min, until=self._date_max
        )

    def download(  # type: ignore
        self,
        clobber: bool = False,
    ) -> List[Path]:
        """Download."""
        msg = (
            f"Downloading GloFAS {self._forecast_type} "
            f"for {self._date_min} - {self._date_max}"
        )
        if self._leadtime_max is not None:
            msg += f"and up to {self._leadtime_max} day lead time"
        logger.info(msg)

        # Make directory
        output_directory = self._get_directory()
        output_directory.mkdir(parents=True, exist_ok=True)

        # Get list of files to open
        query_params_list = []
        for date in self._date_range:
            output_filepath = self._get_filepath(
                year=date.year,
                month=date.month,
                day=date.day,
                leadtime_max=self._leadtime_max,
            )
            if clobber or not output_filepath.exists():
                query_params_list.append(
                    QueryParams(
                        filepath=output_filepath,
                        query=self._get_query(
                            year=date.year,
                            month=date.month,
                            day=date.day,
                            leadtime_max=self._leadtime_max,
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
        """Process GloFAS data."""
        logger.info(
            f"Processing GloFAS {self._forecast_type} for {self._date_min} - "
            f"{self._date_max} and up to {self._leadtime_max} day lead time"
        )
        # Make the directory
        output_directory = self._get_directory(is_processed=True)
        output_directory.mkdir(parents=True, exist_ok=True)
        # Get list of files to open
        processed_filepaths = []
        for date in self._date_range:
            input_filepath = self._get_filepath(
                year=date.year,
                month=date.month,
                day=date.day,
                leadtime_max=self._leadtime_max,
            )
            output_filepath = self._get_filepath(
                year=date.year,
                month=date.month,
                day=date.day,
                leadtime_max=self._leadtime_max,
                is_processed=True,
            )
            processed_filepath = self._process_single_file(
                input_filepath=input_filepath,
                filepath=output_filepath,
                clobber=clobber,
            )
            processed_filepaths.append(processed_filepath)
        logger.info(
            f"Processed {len(processed_filepaths)} files to {output_directory}"
        )
        logger.debug(f"Files downloaded: {processed_filepaths}")
        return processed_filepaths

    def load(
        self,
    ) -> xr.Dataset:
        """Load GloFAS data."""
        filepath_list = [
            self._get_filepath(
                year=date.year,
                month=date.month,
                day=date.day,
                leadtime_max=self._leadtime_max,
                is_processed=True,
            )
            for date in self._date_range
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
        leadtime_max: int = None,
        is_processed: bool = False,
    ) -> Path:
        filename = f"{self._country_config.iso3}_{self._cds_name}_{year}"
        if self._frequency in [rrule.MONTHLY, rrule.DAILY]:
            filename += f"-{str(month).zfill(2)}"
        if self._frequency == rrule.DAILY:
            filename += f"-{str(day).zfill(2)}"
        if leadtime_max is not None:
            filename += f"_ltmax{str(leadtime_max).zfill(2)}d"
        filename += f"_{self._geo_bounding_box.get_filename_repr(p=2)}"
        if is_processed:
            filename += "_processed.nc"
        else:
            filename += ".grib"
        return self._get_directory(is_processed=is_processed) / Path(filename)

    def _get_directory(self, is_processed: bool = False) -> Path:
        return (
            self._processed_base_dir
            if is_processed
            else self._raw_base_dir / self._cds_name
        )

    def _download(
        self,
        query_params_list: List[QueryParams],
    ) -> List[Path]:
        # First make the requests and store the request number
        for query_params in query_params_list:
            logger.debug(f"Making request {query_params.query}")
            query_params.request_id = (
                cdsapi.Client(wait_until_complete=False, delete=False)
                .retrieve(name=self._cds_name, request=query_params.query)
                .reply["request_id"]
            )
        # Loop through the request list and check status until all
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
        leadtime_max: int = None,
    ) -> dict:
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
        if leadtime_max is not None:
            leadtime = list(np.arange(leadtime_max) + 1)
            query["leadtime_hour"] = [
                str(single_leadtime * 24) for single_leadtime in leadtime
            ]
        logger.debug(f"Query: {query}")
        return query

    def _process_single_file(self, *args, **kwargs):
        pass

    def _get_reporting_point_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Create a product_type from a GloFAS raster based on reporting points.

        Parameters
        ----------
        ds :
        coord_names :

        """
        if self._country_config.glofas is None:
            raise KeyError(
                "The country configuration file does not contain"
                "any reporting point coordinates. Please update the"
                "configuration file and try again."
            )
        # Check that lat and lon are in the bounds
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
        # If they are then return the correct pixel
        return xr.Dataset(
            data_vars={
                reporting_point.id: (
                    self._coord_names,
                    ds.sel(
                        longitude=reporting_point.lon,
                        latitude=reporting_point.lat,
                        method="nearest",
                    )[self._RIVER_DISCHARGE_VAR].data,
                    {"name": reporting_point.name},
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
        return ds


def write_to_processed_file(
    ds: xr.Dataset,
    filepath: Path,
) -> Path:
    """Write a file to netcdf but removing it first if it exists."""
    # Netcdf seems to have problems overwriting; delete the file if
    # it exists
    if filepath.exists():
        filepath.unlink()
    logger.info(f"Writing to {filepath}")
    ds.to_netcdf(filepath)
    return filepath
