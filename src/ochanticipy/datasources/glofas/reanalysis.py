"""Download and process GloFAS reanalysis river discharge data."""
import logging
from datetime import date
from pathlib import Path
from typing import Union

import xarray as xr
from dateutil import rrule

from ochanticipy.config.countryconfig import CountryConfig
from ochanticipy.datasources.glofas import glofas
from ochanticipy.utils.check_file_existence import check_file_existence
from ochanticipy.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)


class GlofasReanalysis(glofas.Glofas):
    """
    Class for downloading and processing GloFAS reanalysis data.

    The GloFAS reanalysis dataset is a global raster presenting river
    discharnge from 1979 until present day (updated daily), see
    `this paper <https://essd.copernicus.org/articles/12/2043/2020/>`_
    for more details.

    This class downloads the raw raster data
    `from CDS
    <https://cds.climate.copernicus.eu/cdsapp#!/dataset/cems-glofas-historical?tab=overview>`_,
    and processes it from a raster to a datasets of reporting points from the
    `GloFAS interface
    <https://www.globalfloods.eu/glofas-forecasting/>`_.
    Due to the CDS request size limits, separate files are downloaded per
    year.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        The bounding coordinates of the area that should be included
    start_date : Union[date, str], default: date(1979, 1, 1)
        The starting date for the dataset. If left blank, defaults to the
        earliest available date
    end_date : Union[date, str], default: date.today()
        The ending date for the dataset. If left blank, defaults to
        the current date
    Examples
    --------
    Download, process and load all historical GloFAS reanalysis data
    until the current date, set to Oct 22, 2022 for this example.

    >>> from datetime import date
    >>> from ochanticipy import create_country_config, CodAB, GeoBoundingBox,
    ... GlofasReanalysis
    >>>
    >>> country_config = create_country_config(iso3="bgd")
    >>> codab = CodAB(country_config=country_config)
    >>> codab.download()
    >>> admin_npl = codab.load()
    >>> geo_bounding_box = GeoBoundingBox.from_shape(admin_npl)
    >>>
    >>> glofas_reanalysis = GlofasReanalysis(
    ...     country_config=country_config,
    ...     geo_bounding_box=geo_bounding_box,
    ...     end_date=date(year=2022, month=10, day=22)
    ... )
    >>> glofas_reanalysis.download()
    >>> glofas_reanalysis.process()
    >>>
    >>> npl_glofas_reanalysis_reporting_points = glofas_reanalysis.load()
    """

    def __init__(
        self,
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        start_date: Union[date, str] = None,
        end_date: Union[date, str] = None,
    ):
        super().__init__(
            country_config=country_config,
            geo_bounding_box=geo_bounding_box,
            cds_name="cems-glofas-historical",
            system_version="version_3_1",
            product_type="consolidated",
            date_variable_prefix="h",
            frequency=rrule.YEARLY,
            coord_names=["time"],
            start_date_min=date(year=1979, month=1, day=1),
            start_date=start_date,
            end_date=end_date,
        )

    @check_file_existence
    def _load_single_file(
        self, input_filepath: Path, filepath: Path, clobber: bool
    ) -> xr.Dataset:
        return xr.load_dataset(
            input_filepath, engine="cfgrib", backend_kwargs={"indexpath": ""}
        )
