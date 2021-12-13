"""
Functions to process ECMWF's realtime seasonal forecast data.

The realtime data is not publicly available.
A data sharing agreement with ECMWF has to be
established to gain access to this data. See more information
https://www.ecmwf.int/en/forecasts/access-forecasts/data-delivery
This script assumes that the user has access to
this data and that this is saved at `ECMWF_REALTIME_RAW_DIR`.

This script processess the seasonal forecast named SEAS5. More info on
this model can be found in the user guide:
https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf
"""

import logging
import os
from pathlib import Path

import xarray as xr

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.area import Area

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "ecmwf"
# folder structure within the ecmwf dir
SEAS_DIR = "seasonal-monthly-individual-members"
PRATE_DIR = "prate"

# is it best to set this here or as a default value ina  config or so?
# contains all the realtime data, not only seas forecast
ECMWF_REALTIME_RAW_DIR = (
    Path(os.environ["AA_DATA_DIR"]) / "private" / "raw" / "glb" / "ecmwf"
)

ISO3_POINTS_MAPPING = {"mwi": 384}
ISO3_AREA_MAPPING = {"mwi": Area(north=-5, south=-17, east=37, west=33)}


class EcmwfRealtime(DataSource):
    """
    Work with ECMWF's realtime data.

    Parameters
    ----------
    iso3: str
        country iso3
    points_mapping: int
        The ECMWF data can contain several areas. These areas each have a
        different number of points, which is indicated by `points_mapping`.
        This number is needed to read the correct area.
        It thus serves as a sort of ID.
        If None, the `points_mapping` will be retrieved from the default
        list if available for the given iso3
    area: Area
        the bounding coordinates of the area that is included in the data.
        If None, it will be retrieved from the default list if available
        for the given iso3
    """

    def __init__(
        self, iso3: str, points_mapping: int = None, area: Area = None
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )
        # question: can I use the datasource class to retrieve this instead of
        # hardcoding it above?
        # overwrite the raw_base_dir assuming that the data lives
        # in the global directory instead of the country specific
        self._raw_base_dir = ECMWF_REALTIME_RAW_DIR

        if points_mapping:
            self._points_mapping = points_mapping
        elif iso3 in ISO3_POINTS_MAPPING:
            self._points_mapping = ISO3_POINTS_MAPPING[iso3]
        else:
            logger.error(
                "No point mapping given or iso3 not found in default point "
                "mappings. Input a point mapping or add the iso3 to defaults."
            )

        if Area:
            self._area = area
        elif iso3 in ISO3_AREA_MAPPING:
            self._area = ISO3_AREA_MAPPING[iso3]
        else:
            logger.error(
                "No area given or iso3 not found in default area map. "
                "Input an area or add the iso3 to defaults."
            )

    def process(self, datavar: str = "fcmean") -> Path:
        """
        Process the seasonal forecast by ECMWF that is shared in realtime.

        This data is private and thus is only shared if you have
        a license agreement with ECMWF.
        The data lives in ECMWF_REALTIME_RAW_DIR

        Parameters
        ----------
        datavar: str, default = fcmean
            variable to extract. fcmean contains the monthly mean
            per ensemble member. em the mean across all ensemble members

        Returns
        -------
        Path to processed NetCDF file

        """
        # T4L indicates the seasonal forecast with the monthly mean
        # see here for a bit more explanation
        # https://confluence.ecmwf.int/pages/viewpage.action?pageId=111155348
        # without the 1 you also get .idx files which we don't want
        filepath_list = list(self._raw_base_dir.glob("*T4L*1"))

        # i would think setting concat_dim=["time","step"] makes more sense
        # but get an error "concat_dims has length 2 but the datasets passed
        # are nested in a 1-dimensional structure"
        # it seems to work thought when using concat_dim="time"
        # but would have to test once we have data from several dates..
        output_filepath = self.get_processed_path()
        output_filepath.parent.mkdir(exist_ok=True, parents=True)
        with xr.open_mfdataset(
            filepath_list,
            engine="cfgrib",
            filter_by_keys={
                "numberOfPoints": self._points_mapping,
                "dataType": datavar,
            },
            concat_dim=["step"],
            combine="nested",
            preprocess=lambda d: self._preprocess(d),
            # time refers to the publication month
            # forecastMonth to the leadtime in months, which is indexed 1-7
            backend_kwargs={"time_dims": ("time", "forecastMonth")},
        ) as ds:
            ds.to_netcdf(output_filepath)

        return output_filepath

    def get_processed_path(self):
        """Return the path to the processed file."""
        output_dir = self._processed_base_dir / SEAS_DIR / PRATE_DIR
        output_filename = (
            f"{self._iso3}_{SEAS_DIR}_{PRATE_DIR}_realtime"
            f"_{self._area.get_filename_repr()}.nc"
        )
        return output_dir / output_filename

    @staticmethod
    def _preprocess(ds_date: xr.Dataset):
        """Set coordinate types and remove irrelevant dimensions."""
        ds_date = ds_date.assign_coords(
            {
                # original type is float64
                # type of api data is float32
                # so convert to float32 to be able to combine the
                # datasets later on
                "latitude": ds_date.latitude.astype("float32"),
                "longitude": ds_date.longitude.astype("float32"),
            }
        )
        return (
            ds_date.expand_dims("time")
            # surface is empty
            .drop_vars("surface")
        )
