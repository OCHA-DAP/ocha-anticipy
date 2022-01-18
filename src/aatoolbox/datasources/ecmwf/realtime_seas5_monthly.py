"""
Functions to process ECMWF's realtime seasonal forecast data.

The realtime data is not publicly available.
A data sharing agreement with ECMWF has to be
established to gain access to this data. See more information `here
<https://www.ecmwf.int/en/forecasts/access-forecasts/data-delivery>`_
This script assumes that the user has access to
this data and that this is saved at `_ECMWF_REALTIME_RAW_DIR`.

This script processess the seasonal forecast named SEAS5. More info on
this model can be found in the `user guide
<https://www.ecmwf.int/sites/default/files/medialibrary/2017-10/System5_guide.pdf>`_
"""

import logging
from pathlib import Path

import xarray as xr

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

logger = logging.getLogger(__name__)

_MODULE_BASENAME = "ecmwf"
# folder structure within the ecmwf dir
_SEAS_DIR = "seasonal-monthly-individual-members"
_PRATE_DIR = "prate"

_ISO3_POINTS_MAPPING = {"mwi": 384}
_ISO3_GEOBB_MAPPING = {
    "mwi": GeoBoundingBox(north=-5, south=-17, east=37, west=33)
}
_GRID_RESOLUTION = 0.4  # degrees
# number of decimals to include in filename
# data is on 0.4 grid so should be 1
_FILENAME_PRECISION = 1


class EcmwfRealtime(DataSource):
    """
    Work with ECMWF's realtime data.

    Parameters
    ----------
    iso3: str
        country iso3
    """

    def __init__(
        self,
        iso3: str,
    ):
        super().__init__(
            iso3=iso3, module_base_dir=_MODULE_BASENAME, is_public=False
        )

        # overwrite the raw_base_dir assuming that the data lives
        # in the global directory instead of the country specific
        glb_datasource_class = DataSource(
            "glb", module_base_dir=_MODULE_BASENAME, is_public=False
        )
        self._raw_base_dir = glb_datasource_class._raw_base_dir

        if iso3 in _ISO3_GEOBB_MAPPING:
            geo_bounding_box = _ISO3_GEOBB_MAPPING[iso3]
        else:
            raise Exception(
                "Iso3 not found in default bounding box map. "
                "Add the iso3 bounding box to the defaults."
            )
        geo_bounding_box.round_coords(round_val=_GRID_RESOLUTION)
        self._geobb = geo_bounding_box

    def process(self, points_mapping: int, datavar: str = "fcmean") -> Path:
        """
        Process the seasonal forecast by ECMWF that is shared in realtime.

        This data is private and thus is only shared if you have
        a license agreement with ECMWF.
        The data lives in _ECMWF_REALTIME_RAW_DIR

        Parameters
        ----------
        datavar: str, default = fcmean
            variable to extract. fcmean contains the monthly mean
            per ensemble member. em the mean across all ensemble members
        points_mapping: int
            The ECMWF data can contain several areas. These areas each have a
            different number of points, which is indicated by `points_mapping`.
            This number is needed to read the correct area.
            It thus serves as a sort of ID.

        Returns
        -------
        Path to processed NetCDF file

        Examples
        --------
        >>> (from aatoolbox.datasources.ecmwf.realtime_seas5_monthly
        ... import EcmwfRealtime)
        >>> ecmwf_rt=EcmwfRealtime(iso3="mwi")
        >>> ecmwf_rt.process(points_mapping=384)
        """
        # T4L indicates the seasonal forecast with the monthly mean
        # see here for a bit more explanation
        # https://confluence.ecmwf.int/pages/viewpage.action?pageId=111155348
        # the 1 is for extra security to not accidentally catch
        # thrash (such as index) files
        filepath_list = list(self._raw_base_dir.glob("*T4L*1"))
        # i would think setting concat_dim=["time","step"] makes more sense
        # but get an error "concat_dims has length 2 but the datasets passed
        # are nested in a 1-dimensional structure"
        # it seems to work thought when using concat_dim="time"
        # but would have to test once we have data from several dates..
        output_filepath = self._get_processed_path()
        output_filepath.parent.mkdir(exist_ok=True, parents=True)
        with xr.open_mfdataset(
            filepath_list,
            engine="cfgrib",
            filter_by_keys={
                "numberOfPoints": points_mapping,
                "dataType": datavar,
            },
            combine="by_coords",
            preprocess=lambda d: self._preprocess(d),
            # time refers to the publication month
            # forecastMonth to the leadtime in months, which is indexed 1-7
            backend_kwargs={
                "time_dims": ("time", "forecastMonth"),
                "indexpath": "",
            },
        ) as ds:
            ds.attrs["included_files"] = [f.stem for f in filepath_list]
            ds.to_netcdf(output_filepath)

        return output_filepath

    def load(self):
        """
        Load the realtime ecmwf dataset.

        Examples
        --------
        >>> (from aatoolbox.datasources.ecmwf.realtime_seas5_monthly
        ... import EcmwfRealtime)
        >>> ecmwf_rt=EcmwfRealtime(iso3="mwi")
        >>> ecmwf_rt.process()
        >>> ecmwf_rt.load()
        """
        return xr.load_dataset(self._get_processed_path())

    def _get_processed_path(self):
        """Return the path to the processed file."""
        output_dir = self._processed_base_dir / _SEAS_DIR / _PRATE_DIR
        output_filename = (
            f"{self._iso3}_{_SEAS_DIR}_{_PRATE_DIR}_realtime"
            # TODO: remove geobb
            f"_{self._geobb.get_filename_repr(p=_FILENAME_PRECISION)}.nc"
        )
        return output_dir / output_filename

    @staticmethod
    def _preprocess(ds_date: xr.Dataset):
        """Set coordinate types and remove irrelevant dimensions."""
        ds_date = ds_date.rename({"forecastMonth": "step"}).assign_coords(
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
            ds_date.expand_dims(["time", "step"])
            # surface is empty
            # TODO: not sure why forecastMonth is set as a var as well..
            .drop_vars(["surface", "forecastMonth"])
        )
