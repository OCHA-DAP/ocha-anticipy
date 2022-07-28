"""Glofas reanalysis."""
import logging
from datetime import datetime
from pathlib import Path

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

    @check_file_existence
    def _process_single_file(
        self, input_filepath: Path, filepath: Path, clobber: bool
    ) -> Path:
        # Read in the product_type
        logger.debug(f"Reading in {input_filepath}")
        ds = xr.load_dataset(
            input_filepath, engine="cfgrib", backend_kwargs={"indexpath": ""}
        )
        # Create a new product_type with just the station pixels
        ds_new = self._get_reporting_point_dataset(ds=ds, coord_names=["time"])
        # Write out the new product_type to a file
        return self._write_to_processed_file(ds=ds_new, filepath=filepath)
