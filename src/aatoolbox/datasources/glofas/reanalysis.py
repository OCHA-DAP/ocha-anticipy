"""Glofas reanalysis."""
import datetime
import logging
from pathlib import Path
from typing import List

import xarray as xr

from aatoolbox.datasources.glofas import glofas
from aatoolbox.utils.check_file_existence import check_file_existence

logger = logging.getLogger(__name__)


class GlofasReanalysis(glofas.Glofas):
    """Download, process GloFAS reanalysis."""

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            **kwargs,
            year_min=1979,
            year_max=datetime.datetime.now().year,
            cds_name="cems-glofas-historical",
            system_version="version_3_1",
            product_type="consolidated",
            date_variable_prefix="h",
        )

    def download(  # type: ignore
        self, year_min: int = None, year_max: int = None, clobber: bool = False
    ):
        """
        Download GloFAS reanalysis.

        Parameters
        ----------
        year_min :
        year_max :
        clobber :
        """
        year_min = self._year_min if year_min is None else year_min
        year_max = self._year_max if year_max is None else year_max
        logger.info(
            f"Downloading GloFAS reanalysis for years {year_min} -"
            f" {year_max}"
        )
        # Create list of query params
        # TODO: Just ignore existing files for now if clobber is False, may
        #  want to warn explicitly
        query_params_list = [
            glofas.QueryParams(
                self._get_raw_filepath(year), self._get_query(year=year)
            )
            for year in range(year_min, year_max + 1)
            if not self._get_raw_filepath(year).exists() or clobber is True
        ]
        self._download(query_params_list=query_params_list)
        # TODO: return filepath

    def process(  # type: ignore
        self, year_min: int = None, year_max: int = None, clobber: bool = False
    ):
        """
        Process GloFAS data.

        Parameters
        ----------
        year_min :
        year_max :
        clobber :
        """
        year_min = self._year_min if year_min is None else year_min
        year_max = self._year_max if year_max is None else year_max
        filepath = self._get_processed_filepath()
        logger.info(
            f"Processing GloFAS Reanalysis for {year_min} to {year_max}"
        )
        # Get list of files to open
        input_filepath_list = [
            self._get_raw_filepath(
                year=year,
            )
            for year in range(year_min, year_max + 1)
        ]

        return self._process(
            filepath=filepath,
            input_filepath_list=input_filepath_list,
            clobber=clobber,
        )

    @check_file_existence
    def _process(
        self, filepath: Path, input_filepath_list: List[Path], clobber: bool
    ) -> Path:
        # Read in the product_type
        logger.info(f"Reading in {len(input_filepath_list)} files")

        with xr.open_mfdataset(
            input_filepath_list,
            engine="cfgrib",
            backend_kwargs={"indexpath": ""},
        ) as ds:
            # Create a new product_type with just the station pixels
            logger.info(
                "Looping through reporting_points, this takes some time"
            )
            ds_new = self._get_reporting_point_dataset(
                ds=ds, coord_names=["time"]
            )
            # Write out the new product_type to a file
            return self._write_to_processed_file(ds=ds_new, filepath=filepath)
