"""Glofas reanalysis."""
import logging
from datetime import datetime
from pathlib import Path
from typing import List

import xarray as xr
from dateutil import rrule

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.glofas import glofas
from aatoolbox.utils.check_file_existence import check_file_existence
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)


class GlofasReanalysis(glofas.Glofas):
    """Download, process GloFAS reanalysis."""

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        date_min: datetime = None,
        date_max: datetime = None,
    ):
        if date_min is None:
            date_min = datetime(year=1971, month=1, day=1)
        if date_max is None:
            date_max = datetime.utcnow()
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            date_min=date_min,
            date_max=date_max,
            cds_name="cems-glofas-historical",
            system_version="version_3_1",
            product_type="consolidated",
            date_variable_prefix="h",
            frequency=rrule.YEARLY,
        )

    def process(self, clobber: bool = False):  # type: ignore
        """
        Process GloFAS data.

        Parameters
        ----------
        clobber :
        """
        filepath = self._get_processed_filepath()
        logger.info(
            f"Processing GloFAS Reanalysis for {self._date_min.year} to "
            f"{self._date_max.year}"
        )
        # Get list of files to open
        input_filepath_list = [
            self._get_raw_filepath(
                year=year,
            )
            for year in range(self._date_min.year, self._date_max.year + 1)
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
