"""Glofas reanalysis."""
import datetime
import logging

import xarray as xr

from aatoolbox.datasources.glofas import glofas

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

    def download(
        self,
        year_min: int = None,
        year_max: int = None,
    ):
        """
        Download GloFAS reanalysis.

        Parameters
        ----------
        year_min :
        year_max :
        """
        year_min = self._year_min if year_min is None else year_min
        year_max = self._year_max if year_max is None else year_max
        logger.info(
            f"Downloading GloFAS reanalysis for years {year_min} -"
            f" {year_max}"
        )
        for year in range(year_min, year_max + 1):
            logger.info(f"...{year}")
            super()._download(
                year=year,
            )

    def process(
        self,
        year_min: int = None,
        year_max: int = None,
    ):
        """
        Process GloFAS data.

        Parameters
        ----------
        stations :
        year_min :
        year_max :
        """
        year_min = self._year_min if year_min is None else year_min
        year_max = self._year_max if year_max is None else year_max
        # Get list of files to open
        logger.info("Processing GloFAS Reanalysis")
        filepath_list = [
            self._get_raw_filepath(
                year=year,
            )
            for year in range(year_min, year_max + 1)
        ]
        # Read in the product_type
        logger.info(f"Reading in {len(filepath_list)} files")

        with xr.open_mfdataset(
            filepath_list, engine="cfgrib", backend_kwargs={"indexpath": ""}
        ) as ds:
            # Create a new product_type with just the station pixels
            logger.info(
                "Looping through reporting_points, this takes some time"
            )
            ds_new = self._get_reporting_point_dataset(
                ds=ds, coord_names=["time"]
            )
        # Write out the new product_type to a file
        return self._write_to_processed_file(
            ds=ds_new,
        )
