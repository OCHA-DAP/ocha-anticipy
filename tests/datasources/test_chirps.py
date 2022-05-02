"""Tests for the Chirps module."""
import calendar
from datetime import date

import cftime
import numpy as np
import pytest
import xarray as xr
from xarray.coding.cftimeindex import CFTimeIndex

from aatoolbox import ChirpsDaily, ChirpsMonthly, GeoBoundingBox

DATASOURCE_BASE_DIR = "chirps"
_START_YEAR = 2020
_START_MONTH = 7
_START_DAY = 15
_END_YEAR = 2020
_END_MONTH = 8
_END_DAY = 1
_END_YEAR_FUTURE = 2100
_END_MONTH_FUTURE = 10
_END_DAY_FUTURE = 2

_START_MONTH_STR = calendar.month_abbr[_START_MONTH]
_END_MONTH_STR = calendar.month_abbr[_END_MONTH]
_END_MONTH_FUTURE_STR = calendar.month_abbr[_END_MONTH_FUTURE]


@pytest.fixture
def mock_chirps(mock_country_config):
    """Create Chirps class with mock country config."""
    geo_bounding_box = GeoBoundingBox(
        lat_max=6, lat_min=3.2, lon_max=2, lon_min=-3
    )

    def _mock_chirps(frequency: str = "daily", resolution: float = 0.05):
        if frequency == "daily":
            chirps = ChirpsDaily(
                country_config=mock_country_config,
                geo_bounding_box=geo_bounding_box,
            )
        else:
            chirps = ChirpsMonthly(
                country_config=mock_country_config,
                geo_bounding_box=geo_bounding_box,
            )
        return chirps

    return _mock_chirps


def test_valid_arguments_class(mock_country_config):
    """Test for resolution in initialisation class."""
    geo_bounding_box = GeoBoundingBox(
        lat_max=6, lat_min=3.2, lon_max=2, lon_min=-3
    )
    with pytest.raises(ValueError):
        ChirpsDaily(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            resolution=0.10,
        )


def test_switching_resolution_class(mock_country_config):
    """Test for switching instance resolution for monthly class."""
    geo_bounding_box = GeoBoundingBox(
        lat_max=6, lat_min=3.2, lon_max=2, lon_min=-3
    )
    chirps = ChirpsMonthly(
        country_config=mock_country_config,
        geo_bounding_box=geo_bounding_box,
        resolution=0.25,
    )
    assert chirps._resolution == 0.05


@pytest.fixture
def mock_download(mocker, mock_chirps):
    """Create mock for download method."""
    download_mock = mocker.patch(
        "aatoolbox.datasources.chirps.chirps._Chirps._actual_download"
    )

    def _mock_download(
        frequency: str,
        start_year,
        end_year,
        start_month,
        end_month,
        start_day,
        end_day,
    ):
        chirps = mock_chirps(frequency=frequency)
        chirps.download(
            start_year=start_year,
            end_year=end_year,
            start_month=start_month,
            end_month=end_month,
            start_day=start_day,
            end_day=end_day,
        )
        _, kwargs_download = download_mock.call_args
        url = kwargs_download["url"]
        filepath = kwargs_download["filepath"]
        return url, filepath

    return _mock_download


def test_download_monthly(
    mock_aa_data_dir, mock_country_config, mock_download
):
    """Test of call download for monthly data."""
    url, filepath = mock_download(
        frequency="monthly",
        start_year=_START_YEAR,
        end_year=_END_YEAR,
        start_month=_START_MONTH,
        end_month=_END_MONTH,
        start_day=_START_DAY,
        end_day=_END_DAY,
    )

    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
        ".CHIRPS/.v2p0/.monthly/.global/.precipitation/"
        f"X/%28-3.0%29%282.0%29RANGEEDGES/"
        f"Y/%286.0%29%283.2%29RANGEEDGES/"
        f"T/%28{_END_MONTH_STR}%20{_END_YEAR}%29"
        f"%28{_END_MONTH_STR}%20{_END_YEAR}"
        "%29RANGEEDGES/data.nc"
    )

    assert (
        filepath
        == mock_aa_data_dir
        / f"public/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
        f"abc_chirps_monthly_{_END_YEAR}_{_END_MONTH:02d}_"
        "r0.05_Np6Sp3Ep2Wm3.nc"
    )


def test_download_daily(mock_aa_data_dir, mock_country_config, mock_download):
    """Test of call download for daily data."""
    url, filepath = mock_download(
        frequency="daily",
        start_year=_START_YEAR,
        end_year=_END_YEAR,
        start_month=_START_MONTH,
        end_month=_END_MONTH,
        start_day=_START_DAY,
        end_day=_END_DAY,
    )

    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
        ".CHIRPS/.v2p0/.daily-improved/.global/.0p05/.prcp/"
        f"X/%28-3.0%29%282.0%29RANGEEDGES/"
        f"Y/%286.0%29%283.2%29RANGEEDGES/"
        f"T/%28{_END_DAY:02d}%20{_END_MONTH_STR}%20{_END_YEAR}"
        f"%29%28{_END_DAY:02d}%20{_END_MONTH_STR}%20{_END_YEAR}"
        "%29RANGEEDGES/data.nc"
    )

    assert (
        filepath
        == mock_aa_data_dir
        / f"public/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
        f"abc_chirps_daily_{_END_YEAR}_{_END_MONTH:02d}_{_END_DAY:02d}_"
        "r0.05_Np6Sp3Ep2Wm3.nc"
    )


def test_create_date_list_daily(mock_chirps):
    """Test the creation of the date list for daily data."""
    chirps = mock_chirps(frequency="daily")
    date_list = [("2020", "07", f"{day:02d}") for day in range(_START_DAY, 32)]
    date_list.append(("2020", "08", "01"))

    assert (
        date_list
        == chirps._create_date_list(
            start_year=_START_YEAR,
            end_year=_END_YEAR,
            start_month=_START_MONTH,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )[0]
    )


def test_create_date_list_monthly(mock_chirps):
    """Test the creation of the date list for monthly data."""
    chirps = mock_chirps(frequency="monthly")
    date_list = [("2020", "07"), ("2020", "08")]

    assert (
        date_list
        == chirps._create_date_list(
            start_year=_START_YEAR,
            end_year=_END_YEAR,
            start_month=_START_MONTH,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )[0]
    )


def test_date_valid(mocker, mock_chirps):
    """Test error when input date is not valid."""
    mocker.patch(
        (
            "aatoolbox.datasources.chirps.chirps."
            "_Chirps._get_last_available_date"
        ),
        return_value=date(year=2025, month=5, day=5),
    )
    chirps = mock_chirps(frequency="daily")

    with pytest.raises(ValueError):
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR_FUTURE,
            start_month=_START_MONTH,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR_FUTURE,
            start_month=_START_MONTH,
            end_month=_END_MONTH_FUTURE,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR_FUTURE,
            start_month=_START_MONTH,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY_FUTURE,
        )
        chirps._check_dates_validity(
            start_year=None,
            end_year=_END_YEAR,
            start_month=_START_MONTH,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=None,
            start_month=_START_MONTH,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR,
            start_month=None,
            end_month=_END_MONTH,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR,
            start_month=_START_MONTH,
            end_month=None,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR,
            start_month=_START_MONTH,
            end_month=None,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )
        chirps._check_dates_validity(
            start_year=_START_YEAR,
            end_year=_END_YEAR,
            start_month=15,
            end_month=None,
            start_day=_START_DAY,
            end_day=_END_DAY,
        )


def test_complete_date_when_incomplete(mocker, mock_chirps):
    """Test automatic completion date when input fate is incomplete."""
    current_date = date(year=2025, month=5, day=5)
    mocker.patch(
        (
            "aatoolbox.datasources.chirps.chirps."
            "_Chirps._get_last_available_date"
        ),
        return_value=current_date,
    )
    chirps = mock_chirps(frequency="daily")

    start_date, end_date = chirps._check_dates_validity(
        start_year=None,
        end_year=None,
        start_month=None,
        end_month=None,
        start_day=None,
        end_day=None,
    )
    assert start_date == date(year=1981, month=1, day=1)
    assert end_date == current_date

    start_date, end_date = chirps._check_dates_validity(
        start_year=1990,
        end_year=None,
        start_month=None,
        end_month=None,
        start_day=None,
        end_day=None,
    )
    assert start_date == date(year=1990, month=1, day=1)

    start_date, end_date = chirps._check_dates_validity(
        start_year=None,
        end_year=2025,
        start_month=None,
        end_month=None,
        start_day=None,
        end_day=None,
    )
    assert end_date == current_date

    start_date, end_date = chirps._check_dates_validity(
        start_year=None,
        end_year=2024,
        start_month=None,
        end_month=None,
        start_day=None,
        end_day=None,
    )
    assert end_date == date(year=2024, month=12, day=31)


def test_process(mocker, mock_chirps, mock_aa_data_dir, mock_country_config):
    """Test process for Chirps."""
    ds = xr.DataArray(
        np.reshape(a=np.arange(8), newshape=(2, 2, 2)),
        dims=("X", "Y", "T"),
        coords={
            "X": [1, 2],
            "Y": [2, -3],
            "T": [685.5, 686.5],
        },
    ).to_dataset(name="prec")

    ds["T"].attrs["calendar"] = "360"
    ds["T"].attrs["units"] = "months since 1960-01-01"

    filepath_list = [
        (
            mock_aa_data_dir
            / f"public/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
            f"abc_chirps_daily_{_END_YEAR}_{_END_MONTH}_{_END_DAY}_"
            "r0.05_Np6Sp3Ep2Wm3.nc"
        )
    ]

    chirps = mock_chirps(frequency="monthly")

    mocker.patch(
        (
            "aatoolbox.datasources.chirps.chirps."
            "_Chirps._get_downloaded_path_list"
        ),
        return_value=filepath_list,
    )

    mocker.patch(
        "aatoolbox.datasources.chirps.chirps.xr.open_dataset",
        return_value=ds,
    )

    processed_path = chirps.process()
    assert processed_path == (
        mock_aa_data_dir
        / f"public/processed/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
        f"abc_chirps_daily_{_END_YEAR}_{_END_MONTH}_{_END_DAY}_"
        "r0.05_Np6Sp3Ep2Wm3.nc"
    )

    ds_processed = xr.load_dataset(processed_path)
    expected_f = CFTimeIndex(
        [
            cftime.datetime(year=2017, month=2, day=16, calendar="360_day"),
            cftime.datetime(year=2017, month=3, day=16, calendar="360_day"),
        ]
    )

    assert np.array_equal(ds_processed.X.values, [1, 2])
    assert np.array_equal(ds_processed.Y.values, [2, -3])
    assert ds_processed.get_index("T").equals(expected_f)
    assert np.array_equal(ds_processed.prec.values, ds.prec.values)


def test_process_with_no_files(mocker, mock_chirps):
    """Test process method where no files are present."""
    mocker.patch(
        (
            "aatoolbox.datasources.chirps.chirps."
            "_Chirps._get_downloaded_path_list"
        ),
        return_value=[],
    )
    with pytest.raises(FileNotFoundError):
        chirps = mock_chirps(frequency="monthly")
        chirps.process()


@pytest.fixture
def mock_xr_load_dataset(mocker):
    """Mock GeoPandas file reading function."""
    ds = xr.Dataset()
    return mocker.patch(
        "aatoolbox.datasources.chirps.chirps.xr.open_mfdataset",
        return_value=ds,
    )


def test_chirps_load(
    mocker,
    mock_xr_load_dataset,
    mock_chirps,
    mock_aa_data_dir,
    mock_country_config,
):
    """Test the load method of the Chirps class."""
    ds = xr.Dataset()

    filepath_list = [
        (
            mock_aa_data_dir / f"public/processed/{mock_country_config.iso3}/"
            f"{DATASOURCE_BASE_DIR}/"
            f"abc_chirps_daily_{_END_YEAR}_{_END_MONTH}_{_END_DAY}_"
            "r0.05_Np6Sp3Ep2Wm3.nc"
        )
    ]

    mocker.patch(
        (
            "aatoolbox.datasources.chirps.chirps."
            "_Chirps._get_to_be_loaded_path_list"
        ),
        return_value=filepath_list,
    )

    chirps = mock_chirps(frequency="monthly")
    chirps.load()
    mock_xr_load_dataset.assert_has_calls(
        [
            mocker.call(filepath_list),
        ]
    )

    ds = mock_xr_load_dataset()
    assert ds.attrs["included_files"] == [filepath_list[0].stem]


def test_load_with_no_files(mocker, mock_chirps):
    """Test load method when no files are present."""
    mocker.patch(
        (
            "aatoolbox.datasources.chirps.chirps."
            "_Chirps._get_to_be_loaded_path_list"
        ),
        return_value=[],
    )
    with pytest.raises(FileNotFoundError):
        chirps = mock_chirps(frequency="monthly")
        chirps.load()
