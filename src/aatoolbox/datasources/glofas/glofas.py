"""Base class for downloading and processing GloFAS raster data."""
import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

import cdsapi
import numpy as np
import xarray as xr

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

_MODULE_BASENAME = "glofas"
_HYDROLOGICAL_MODEL = "lisflood"

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
    year_min: int
        The earliest year that the dataset is available
    year_max : int
        The most recent that the dataset is available
    cds_name : str
        The name of the dataset in CDS
    product_type : str or list of strings
        The sub-datasets that you would like to download
    date_variable_prefix : str, default = ""
        Some GloFAS datasets have the prefix "h" in front of some query keys
    """

    _RIVER_DISCHARGE_VAR = "dis24"

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        year_min: int,
        year_max: int,
        cds_name: str,
        system_version: str,
        product_type: Union[str, List[str]],
        date_variable_prefix: str = "",
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
        self._year_min = year_min
        self._year_max = year_max
        self._cds_name = cds_name
        self._system_version = system_version
        self._product_type = product_type
        self._date_variable_prefix = date_variable_prefix

    def load(
        self,
        leadtime_max: int = None,
    ):
        """Load GloFAS data."""
        filepath = self._get_processed_filepath(
            leadtime_max=leadtime_max,
        )
        return xr.load_dataset(filepath)

    async def _download(
        self, query_params_list: List[QueryParams], n_consumers: int = 5
    ):
        queue: asyncio.Queue = asyncio.Queue()
        producers = [
            asyncio.create_task(
                self._download_producer(queue=queue, query_params=query_params)
            )
            for query_params in query_params_list
        ]
        consumers = [
            asyncio.create_task(self._download_consumer(queue=queue))
            for _ in range(n_consumers)
        ]
        await asyncio.gather(*producers)
        await queue.join()  # Implicitly awaits consumers, too
        for c in consumers:
            c.cancel()

    async def _download_producer(
        self, queue: asyncio.Queue, query_params: QueryParams
    ):
        Path(query_params.filepath.parent).mkdir(parents=True, exist_ok=True)
        query_params.request_id = (
            cdsapi.Client(wait_until_complete=False, delete=False)
            .retrieve(name=self._cds_name, request=query_params.query)
            .reply["request_id"]
        )
        await queue.put(query_params)
        logger.debug(f"Added {query_params.filepath} to queue")

    @staticmethod
    async def _download_consumer(
        queue: asyncio.Queue, time_between_checks_seconds: int = 60
    ):
        while True:
            query_params = await queue.get()
            while True:
                result = cdsapi.api.Result(
                    cdsapi.Client(wait_until_complete=False, delete=False),
                    {"request_id": query_params.request_id},
                )
                result.update()
                state = result.reply["state"]
                logger.debug(
                    f"For request {query_params.request_id} and filename "
                    f"{query_params.filepath}, state is {state}"
                )
                if state == "completed":
                    break
                logger.debug(f"Waiting {time_between_checks_seconds} seconds")
                await asyncio.sleep(time_between_checks_seconds)

            logger.debug(
                f"Request {query_params.request_id} for filename "
                f"{query_params.filepath} is complete, downloading"
            )
            result.download(query_params.filepath)
            logger.info(f"Filename {query_params.filepath} downloaded")
            queue.task_done()

    def _get_raw_filepath(
        self,
        year: int,
        month: int = None,
        leadtime_max: int = None,
    ):
        directory = self._raw_base_dir / self._cds_name
        filename = f"{self._country_config.iso3}_{self._cds_name}_{year}"
        if month is not None:
            filename += f"-{str(month).zfill(2)}"
        if leadtime_max is not None:
            filename += f"_ltmax{str(leadtime_max).zfill(2)}d"
        filename += f"_{self._geo_bounding_box.get_filename_repr(p=2)}.grib"
        return directory / Path(filename)

    def _get_query(
        self,
        year: int,
        month: int = None,
        leadtime_max: int = None,
    ) -> dict:
        query = {
            "variable": "river_discharge_in_the_last_24_hours",
            "format": "grib",
            "product_type": self._product_type,
            "system_version": self._system_version,
            "hydrological_model": _HYDROLOGICAL_MODEL,
            f"{self._date_variable_prefix}year": str(year),
            f"{self._date_variable_prefix}month": [
                str(x + 1).zfill(2) for x in range(12)
            ]
            if month is None
            else str(month).zfill(2),
            f"{self._date_variable_prefix}day": [
                str(x + 1).zfill(2) for x in range(31)
            ],
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

    def _get_reporting_point_dataset(
        self, ds: xr.Dataset, coord_names: List[str]
    ) -> xr.Dataset:
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
                    f"ReportingPoint {reporting_point.name} has out-of-bounds "
                    f"lon value of {reporting_point.lon} (GloFAS lon ranges "
                    f"from {ds.longitude.min().values} "
                    f"to {ds.longitude.max().values})"
                )
            if not ds.latitude.min() < reporting_point.lat < ds.latitude.max():
                raise IndexError(
                    f"ReportingPoint {reporting_point.name} has out-of-bounds "
                    f"lat value of {reporting_point.lat} (GloFAS lat ranges "
                    f"from {ds.latitude.min().values} "
                    f"to {ds.latitude.max().values})"
                )
        # If they are then return the correct pixel
        return xr.Dataset(
            data_vars={
                reporting_point.name: (
                    coord_names,
                    ds.sel(
                        longitude=reporting_point.lon,
                        latitude=reporting_point.lat,
                        method="nearest",
                    )[self._RIVER_DISCHARGE_VAR].data,
                )
                # fmt: off
                for reporting_point in
                self._country_config.glofas.reporting_points
                # fmt: on
            },
            coords={coord_name: ds[coord_name] for coord_name in coord_names},
        )

    @staticmethod
    def _write_to_processed_file(
        ds: xr.Dataset,
        filepath: Path,
    ) -> Path:
        Path(filepath.parent).mkdir(parents=True, exist_ok=True)
        # Netcdf seems to have problems overwriting; delete the file if
        # it exists
        if filepath.exists():
            filepath.unlink()
        logger.info(f"Writing to {filepath}")
        ds.to_netcdf(filepath)
        return filepath

    def _get_processed_filepath(self, leadtime_max: int = None) -> Path:
        filename = f"{self._country_config.iso3}_{self._cds_name}"
        if leadtime_max is not None:
            filename += f"_ltmax{str(leadtime_max).zfill(2)}d"
        filename += f"_{self._geo_bounding_box.get_filename_repr(p=1)}.nc"
        return self._processed_base_dir / filename
