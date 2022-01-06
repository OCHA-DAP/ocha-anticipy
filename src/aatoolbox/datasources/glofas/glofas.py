"""Base class for downloading and processing GloFAS raster data."""
import logging
from pathlib import Path
from typing import Dict, List, Union

import cdsapi
import numpy as np
import xarray as xr

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

_MODULE_BASENAME = "glofas"
DEFAULT_VERSION = 3
HYDROLOGICAL_MODELS = {2: "htessel_lisflood", 3: "lisflood"}
RIVER_DISCHARGE_VAR = "dis24"

logger = logging.getLogger(__name__)


class Glofas(DataSource):
    """
    GloFAS base class.

    Parameters
    ----------
    iso3 :
    area :
    year_min :
        The earliest year that the dataset is available. Can be a
        single integer, or a dictionary with structure
        {major_version: year_min} if the minimum year depends on the GloFAS
        model version.
    year_max : int
        The most recent that the dataset is available
    cds_name : str
        The name of the dataset in CDS
    dataset :
        The sub-datasets that you would like to download (as a list of strings)
    dataset_variable_name :
        The variable name with which to pass the above datasets in the CDS
        query
    system_version_minor :
        The minor version of the GloFAS model. Depends on the major version,
        so is given as a dictionary with the format {major_version:
        minor_version}
    date_variable_prefix :
        Some GloFAS datasets have the prefix "h" in front of some query keys
    """

    def __init__(
        self,
        iso3: str,
        area: GeoBoundingBox,
        year_min: Union[int, Dict[int, int]],
        year_max: int,
        cds_name: str,
        dataset: List[str],
        dataset_variable_name: str,
        system_version_minor: Dict[int, int],
        date_variable_prefix: str = "",
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=True
        )
        area.round_boundingbox_coords(offset_val=0.5, round_val=1)
        self.area = area
        self.year_min = year_min
        self.year_max = year_max
        self.cds_name = cds_name
        self.dataset = dataset
        self.dataset_variable_name = dataset_variable_name
        self.system_version_minor = system_version_minor
        self.date_variable_prefix = date_variable_prefix

    def _download(
        self,
        version: int,
        year: int,
        month: int = None,
        leadtime: Union[int, list] = None,
        use_cache: bool = True,
    ):
        filepath = self._get_raw_filepath(
            version=version,
            year=year,
            month=month,
            leadtime=leadtime,
        )
        # If caching is on and file already exists, don't downlaod again
        if use_cache and filepath.exists():
            logger.debug(
                f"{filepath} already exists and cache is set to True, skipping"
            )
            return filepath
        Path(filepath.parent).mkdir(parents=True, exist_ok=True)
        logger.debug(f"Querying for {filepath}...")
        cdsapi.Client().retrieve(
            name=self.cds_name,
            request=self._get_query(
                version=version,
                year=year,
                month=month,
                leadtime=leadtime,
            ),
            target=filepath,
        )
        logger.debug(f"...successfully downloaded {filepath}")
        return filepath

    def _get_raw_filepath(
        self,
        version: int,
        year: int,
        month: int = None,
        leadtime: Union[int, list] = None,
    ):
        version_dir = f"version_{version}"
        directory = self._raw_base_dir / version_dir / self.cds_name
        filename = f"{self._iso3}_{self.cds_name}_v{version}_{year}"
        if month is not None:
            filename += f"-{str(month).zfill(2)}"
        if leadtime is not None and isinstance(leadtime, int):
            filename += f"_lt{str(leadtime).zfill(2)}d"
        filename += ".grib"
        return directory / Path(filename)

    def _get_query(
        self,
        version: int,
        year: int,
        month: int = None,
        leadtime: Union[int, list] = None,
    ) -> dict:
        query = {
            "variable": "river_discharge_in_the_last_24_hours",
            "format": "grib",
            self.dataset_variable_name: self.dataset,
            f"{self.date_variable_prefix}year": str(year),
            f"{self.date_variable_prefix}month": [
                str(x + 1).zfill(2) for x in range(12)
            ]
            if month is None
            else str(month).zfill(2),
            f"{self.date_variable_prefix}day": [
                str(x + 1).zfill(2) for x in range(31)
            ],
            "area": [
                self.area.north,
                self.area.west,
                self.area.south,
                self.area.east,
            ],
            "system_version": (
                f"version_{version}_{self.system_version_minor[version]}"
            ),
            "hydrological_model": HYDROLOGICAL_MODELS[version],
        }
        if leadtime is not None:
            if isinstance(leadtime, int):
                leadtime = [leadtime]
            query["leadtime_hour"] = [
                str(single_leadtime * 24) for single_leadtime in leadtime
            ]
        logger.debug(f"Query: {query}")
        return query

    @staticmethod
    def _read_in_ensemble_and_perturbed_datasets(filepath_list: List[Path]):
        ds_list = []
        for data_type in ["cf", "pf"]:
            with xr.open_mfdataset(
                filepath_list,
                engine="cfgrib",
                backend_kwargs={
                    "indexpath": "",
                    "filter_by_keys": {"dataType": data_type},
                },
            ) as ds:
                # Delete history attribute in order to merge
                del ds.attrs["history"]
                # Extra processing require for control forecast
                if data_type == "cf":
                    ds = expand_dims(
                        ds=ds,
                        dataset_name=RIVER_DISCHARGE_VAR,
                        coord_names=[
                            "number",
                            "time",
                            "step",
                            "latitude",
                            "longitude",
                        ],
                        expansion_dim=0,
                    )
                ds_list.append(ds)
        ds = xr.combine_by_coords(ds_list)
        return ds

    def _write_to_processed_file(
        self,
        country_iso3: str,
        version: int,
        ds: xr.Dataset,
        leadtime: Union[int, list] = None,
    ) -> Path:
        filepath = self._get_processed_filepath(
            version=version,
            leadtime=leadtime,
        )
        Path(filepath.parent).mkdir(parents=True, exist_ok=True)
        # Netcdf seems to have problems overwriting; delete the file if
        # it exists
        filepath.unlink(missing_ok=True)
        logger.info(f"Writing to {filepath}")
        ds.to_netcdf(filepath)
        return filepath

    def _get_processed_filepath(
        self, version: int, leadtime: Union[int, list] = None
    ) -> Path:
        filename = f"{self._iso3}_{self.cds_name}_v{version}"
        if leadtime is not None and isinstance(leadtime, int):
            filename += f"_lt{str(leadtime).zfill(2)}d"
        filename += ".nc"
        return self._processed_base_dir / filename

    def load(
        self,
        version: int = DEFAULT_VERSION,
        leadtime: Union[int, list] = None,
    ):
        """Load GloFAS data."""
        filepath = self._get_processed_filepath(
            version=version,
            leadtime=leadtime,
        )
        return xr.load_dataset(filepath)


def expand_dims(
    ds: xr.Dataset, dataset_name: str, coord_names: list, expansion_dim: int
):
    """Expand dims to combine two datasets.

    Using expand_dims seems to cause a bug with Dask like the one
    described here: https://github.com/pydata/xarray/issues/873 (it's
    supposed to be fixed though)
    """
    coords = {coord_name: ds[coord_name] for coord_name in coord_names}
    coords[coord_names[expansion_dim]] = [coords[coord_names[expansion_dim]]]
    ds = xr.Dataset(
        data_vars={
            dataset_name: (
                coord_names,
                np.expand_dims(ds[dataset_name].values, expansion_dim),
            )
        },
        coords=coords,
    )
    return ds
