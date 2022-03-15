"""Base class for downloading and processing GloFAS raster data."""
import logging
from pathlib import Path
from typing import Dict, List, NamedTuple, Union

import cdsapi
import numpy as np
import xarray as xr

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

_MODULE_BASENAME = "glofas"
VERSION = 3
HYDROLOGICAL_MODEL = "lisflood"
RIVER_DISCHARGE_VAR = "dis24"

logger = logging.getLogger(__name__)


class ReportingPoint(NamedTuple):
    """GloFAS reporting point."""

    lon: float
    lat: float


class Glofas(DataSource):
    """
    GloFAS base class.

    Parameters
    ----------
    country_config: CountryConfig
        Country configuration
    geo_bounding_box: GeoBoundingBox
        the bounding coordinates of the geo_bounding_box that should
        be included in the data.
    year_min: int
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
        country_config: CountryConfig,
        geo_bounding_box: GeoBoundingBox,
        year_min: int,
        year_max: int,
        cds_name: str,
        dataset: List[str],
        dataset_variable_name: str,
        system_version_minor: Dict[int, int],
        date_variable_prefix: str = "",
    ):
        super().__init__(
            country_config=country_config,
            module_base_dir=_MODULE_BASENAME,
            is_public=True,
        )
        # The GloFAS API on CDS requires coordinates have the format x.x5
        geo_bounding_box.round_coords(offset_val=0.05, round_val=0.1)
        self.geo_bounding_box = geo_bounding_box
        self.year_min = year_min
        self.year_max = year_max
        self.cds_name = cds_name
        self.dataset = dataset
        self.dataset_variable_name = dataset_variable_name
        self.system_version_minor = system_version_minor
        self.date_variable_prefix = date_variable_prefix

    def load(
        self,
        version: int = VERSION,  # TODO: only version 3
        leadtime: Union[int, list] = None,
    ):
        """Load GloFAS data."""
        filepath = self._get_processed_filepath(
            version=version,
            leadtime=leadtime,
        )
        return xr.load_dataset(filepath)

    def _download(
        self,
        version: int,
        year: int,
        month: int = None,
        leadtime: Union[int, List[int]] = None,
        use_cache: bool = True,
    ):
        filepath = self._get_raw_filepath(
            version=version,
            year=year,
            month=month,
            leadtime=leadtime,
        )
        # If caching is on and file already exists, don't download again
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
        filename = (
            f"{self._country_config.iso3}_{self.cds_name}_v{version}_{year}"
        )
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
            "geo_bounding_box": [
                self.geo_bounding_box.north,
                self.geo_bounding_box.west,
                self.geo_bounding_box.south,
                self.geo_bounding_box.east,
            ],
            "system_version": (
                f"version_{version}_{self.system_version_minor[version]}"
            ),
            "hydrological_model": HYDROLOGICAL_MODEL,
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

    def _get_reporting_point_dataset(
        self, ds: xr.Dataset, coord_names: List[str]
    ) -> xr.Dataset:
        """
        Create a dataset from a GloFAS raster based on the reporting points.

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
                    )[RIVER_DISCHARGE_VAR].data,
                )
                # fmt: off
                for reporting_point in
                self._country_config.glofas.reporting_points
                # fmt: on
            },
            coords={coord_name: ds[coord_name] for coord_name in coord_names},
        )

    def _write_to_processed_file(
        self,
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
        filename = f"{self._country_config.iso3}_{self.cds_name}_v{version}"
        if leadtime is not None and isinstance(leadtime, int):
            filename += f"_lt{str(leadtime).zfill(2)}d"
        filename += ".nc"
        return self._processed_base_dir / filename


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
