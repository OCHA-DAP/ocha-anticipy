"""
Functions to save and load ECMWF's seasonal forecast data.

This data is retrieved from two sources: the API and realtime.
This script combines these two sources.

The model that produces this forecast is named SEAS5. More info on
this model can be found in the user guide:
https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf
"""

import logging
from pathlib import Path

import geopandas as gpd
import xarray as xr

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.datasources.ecmwf.api_seas5_monthly import EcmwfApi
from aatoolbox.datasources.ecmwf.realtime_seas5_monthly import EcmwfRealtime
from aatoolbox.utils.area import Area, AreaFromShape

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "ecmwf"
# folder structure within the ecmwf dir
SEAS_DIR = "seasonal-monthly-individual-members"
PRATE_DIR = "prate"

ISO3_AREA_MAPPING = {"mwi": Area(north=-5, south=-17, east=37, west=33)}
GRID_RESOLUTION = 0.4  # degrees


# question: should Ecmwf be a super class of EcmwfApi and EcmwfRealtime?
class Ecmwf(DataSource):
    """
    Work with ECMWF's data.

    Combination of the API and realtime data.
    """

    def __init__(self, iso3: str, area=None):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )

        # the area indicates the boundaries for which data is
        # downloaded and processed
        if type(area) == Area:
            self._area = area
        elif type(area) == gpd.GeoDataFrame:
            self._area = AreaFromShape(area)
        elif iso3 in ISO3_AREA_MAPPING:
            self._area = ISO3_AREA_MAPPING[iso3]
        else:
            self._area = Area(north=90, south=-90, east=0, west=360)
        # round coordinates to correspond with the grid ecmwf publishes
        # its data on
        self._area.round_area_coords(round_val=GRID_RESOLUTION)

    # question: should we have a download function here as well?

    def process(self, process_sources: bool = True) -> Path:
        """
        Combine the datasets from the two retrieval methods.

        I.e. from the realtime folder and the api
        In order to combine the two datasets, the area should be the same

        Returns
        -------
        Path to processed NetCDF file

        """
        ecmwf_rt = EcmwfRealtime(iso3=self._iso3, area=self._area)
        ecmwf_api = EcmwfApi(iso3=self._iso3, area=self._area)
        # question: should we even do this?
        if process_sources:
            ecmwf_rt.process()
            ecmwf_api.process()
        ds_api = xr.load_dataset(ecmwf_api.get_processed_path())
        ds_realtime = xr.load_dataset(ecmwf_rt.get_processed_path())
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
        output_dir = self._processed_base_dir / SEAS_DIR / PRATE_DIR
        output_filename = (
            f"{self._iso3}_{SEAS_DIR}_{PRATE_DIR}_comb"
            f"_{self._area.get_filename_repr()}.nc"
        )
        return output_dir / output_filename
