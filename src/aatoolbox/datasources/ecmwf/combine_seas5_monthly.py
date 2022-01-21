"""
Functions to save and load ECMWF's seasonal forecast data.

This data is retrieved from two sources: the API and realtime.
This script combines these two sources.

The model that produces this forecast is named SEAS5. More info on
this model can be found in the `user guide
<https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf>`_
"""

import logging
from pathlib import Path
from typing import Union

import geopandas as gpd
import xarray as xr

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.datasources.ecmwf.api_seas5_monthly import EcmwfApi
from aatoolbox.datasources.ecmwf.realtime_seas5_monthly import EcmwfRealtime
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "ecmwf"
# folder structure within the ecmwf dir
_SEAS_DIR = "seasonal-monthly-individual-members"
_PRATE_DIR = "prate"

_ISO3_GEOBB_MAPPING = {
    "mwi": GeoBoundingBox(north=-5, south=-17, east=37, west=33)
}
_GRID_RESOLUTION = 0.4  # degrees
# number of decimals to include in filename
# data is on 0.4 grid so should be 1
_FILENAME_PRECISION = 1


class Ecmwf(DataSource):
    """
    Work with ECMWF's data.

    Combination of the API and realtime data.

    iso3: str
        country iso3
    geo_bounding_box: GeoBoundingBox | gpd.GeoDataFrame
        the bounding coordinates of the area that is included in the data.
        If None, it will be retrieved from the default list if available
        for the given iso3 and else set to the global bounds
    """

    def __init__(
        self,
        iso3: str,
        match_realtime: bool,
        geo_bounding_box: Union[GeoBoundingBox, gpd.GeoDataFrame, None] = None,
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )
        # the geo_bounding_box indicates the boundaries for which data is
        # downloaded and processed
        if type(geo_bounding_box) == gpd.GeoDataFrame:
            geo_bounding_box = GeoBoundingBox.from_shape(geo_bounding_box)
        elif geo_bounding_box is None:
            if iso3 in _ISO3_GEOBB_MAPPING:
                geo_bounding_box = _ISO3_GEOBB_MAPPING[iso3]
            else:
                geo_bounding_box = GeoBoundingBox(
                    north=90, south=-90, east=0, west=360
                )
        # round coordinates to correspond with the grid ecmwf publishes
        # its data on
        geo_bounding_box.round_coords(round_val=_GRID_RESOLUTION)
        self._geobb = geo_bounding_box

        # question: should we have a download function here as well?

    def process(self, process_sources: bool = True) -> Path:
        """
        Combine the datasets from the two retrieval methods.

        I.e. from the realtime folder and the api
        In order to combine the two datasets,
        the geoboundingbox should be the same

        Returns
        -------
        Path to processed NetCDF file

        """
        ecmwf_rt = EcmwfRealtime(iso3=self._iso3)
        ecmwf_api = EcmwfApi(iso3=self._iso3, geo_bounding_box=self._geobb)
        # question: should we even do this?
        if process_sources:
            ecmwf_rt.process()
            ecmwf_api.process()
        ds_api = xr.load_dataset(ecmwf_api._get_processed_path())
        ds_realtime = xr.load_dataset(ecmwf_rt._get_processed_path())
        # TODO: realtime and api can contain the same forecast dates
        # not sure yet how to handle that..
        ds_comb = ds_api.merge(ds_realtime)
        output_path = self._get_processed_path()
        ds_comb.to_netcdf(output_path)
        return output_path

    def load(self):
        """Load the combined ecmwf dataset."""
        return xr.load_dataset(self._get_processed_path())

    def _get_processed_path(self):
        output_dir = self._processed_base_dir / _SEAS_DIR / _PRATE_DIR
        output_filename = (
            f"{self._iso3}_{_SEAS_DIR}_{_PRATE_DIR}_comb"
            f"_{self._geobb.get_filename_repr(p=_FILENAME_PRECISION)}.nc"
        )
        return output_dir / output_filename
