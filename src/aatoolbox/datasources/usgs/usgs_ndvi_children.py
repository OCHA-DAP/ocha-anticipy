"""Classes to download and process USGS FEWS NET NDVI data.

Data is downloaded from the `USGS FEWS NET data portal
<https://earlywarning.usgs.gov/fews>`_. Data is
generated from eMODIS AQUA, with full methodological
details available on the `Documentation page
<https://earlywarning.usgs.gov/fews/product/449#documentation>`_
for the specific product. The available areas of
coverage are:

- `North Africa<https://earlywarning.usgs.gov/fews/product/449>`_
- `East Africa<https://earlywarning.usgs.gov/fews/product/448>`_
- `Southern Africa<https://earlywarning.usgs.gov/fews/product/450>`_
- `West Africa<https://earlywarning.usgs.gov/fews/product/451>`_
- `Central Asia<https://earlywarning.usgs.gov/fews/product/493>`_
- `Yemen<https://earlywarning.usgs.gov/fews/product/502>`_
- `Central America<https://earlywarning.usgs.gov/fews/product/445>`_
- `Hispaniola<https://earlywarning.usgs.gov/fews/product/446>`_

Data is made available on the backend USGS file explorer. For example,
dekadal temporally smooth NDVI data for West Africa is available at
`this link
<https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/west/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/monthly/>`_

The products include temporally smoothed NDVI, median anomaly,
difference from the previous year, and median anomaly
presented as a percentile.

Data by USGS is published quickly after the dekad.
After about 1 month this data is updated with temporal smoothing
and error correction for cloud cover. Files for a specific
dekad and region can range from 30MB up to over 100MB, so
downloading and processing can take a long time.
"""

# TODO: add progress bar
import logging
from datetime import date
from typing import Tuple, Union

from aatoolbox.config.countryconfig import CountryConfig
from aatoolbox.datasources.usgs.usgs_ndvi import _UsgsNdvi

# from aatoolbox.utils.check_file_existence import check_file_existence

logger = logging.getLogger(__name__)


class UsgsNdviSmoothed(_UsgsNdvi):
    """Base class to retrieve smoothed NDVI data.

    The retrieved data is the smoothed NDVI values
    processed by the USGS. Temporal smoothing is done
    to adjust for cloud cover and other errors.
    Data for the 3 most recent dekads is not fully
    smoothed, and are re-smoothed at the end of the
    3 dekad period.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    start_date : Union[date, str, Tuple[int, int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int, int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    """

    def __init__(
        self,
        country_config: CountryConfig,
        start_date: Union[date, str, Tuple[int, int], None] = None,
        end_date: Union[date, str, Tuple[int, int], None] = None,
    ):
        super().__init__(
            country_config=country_config,
            data_variable="smoothed",
            data_variable_suffix="",
            data_variable_url="temporallysmoothedndvi",
            start_date=start_date,
            end_date=end_date,
        )


class UsgsNdviPctMedian(_UsgsNdvi):
    """Base class to retrieve % of median NDVI.

    The retrieved data is the percent of median NDVI
    values calculated from 2003 - 2017, as
    processed by the USGS.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    start_date : Union[date, str, Tuple[int, int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int, int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    """

    def __init__(
        self,
        country_config: CountryConfig,
        start_date: Union[date, str, Tuple[int, int], None] = None,
        end_date: Union[date, str, Tuple[int, int], None] = None,
    ):
        super().__init__(
            country_config=country_config,
            data_variable="percent_median",
            data_variable_suffix="pct",
            data_variable_url="percentofmedian",
            start_date=start_date,
            end_date=end_date,
        )


class UsgsNdviMedianAnomaly(_UsgsNdvi):
    """Base class to retrieve NDVI anomaly data.

    The retrieved data is NDVI anomaly data calculated
    as a subtraction of the median value from the
    current value. Negative values indicate less
    vegetation than the median, positive values indicate
    more vegetation.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    start_date : Union[date, str, Tuple[int, int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int, int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    """

    def __init__(
        self,
        country_config: CountryConfig,
        start_date: Union[date, str, Tuple[int, int], None] = None,
        end_date: Union[date, str, Tuple[int, int], None] = None,
    ):
        super().__init__(
            country_config=country_config,
            data_variable="median_anomaly",
            data_variable_suffix="stmdn",
            data_variable_url="mediananomaly",
            start_date=start_date,
            end_date=end_date,
        )


class UsgsNdviYearDifference(_UsgsNdvi):
    """Base class to retrieve NDVI year difference data.

    The retrieved data is NDVI yearly difference data,
    calculated as the subtraction of the previous year's
    NDVI value from the current year's. Negative
    values indicate the current vegetation is less
    than the previous year's, positive that there
    is more vegetation in the current year.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    start_date : Union[date, str, Tuple[int, int], None]
        Start date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    end_date : Union[date, str, Tuple[int, int], None]
        End date. Can be passed as a ``datetime.date``
        object and the relevant dekad will be determined,
        as a date string in ISO8601 format, or as a
        year-dekad tuple, i.e. (2020, 1).
    """

    def __init__(
        self,
        country_config: CountryConfig,
        start_date: Union[date, str, Tuple[int, int], None] = None,
        end_date: Union[date, str, Tuple[int, int], None] = None,
    ):
        super().__init__(
            country_config=country_config,
            data_variable="difference",
            data_variable_suffix="dif",
            data_variable_url="differencepreviousyear",
            start_date=start_date,
            end_date=end_date,
        )
