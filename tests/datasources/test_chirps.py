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

START_DATE = date(year=2020, month=7, day=15)
END_DATE = date(year=2020, month=8, day=1)
CURRENT_DATE = date(year=2022, month=5, day=31)
FUTURE_DATE = date(year=2100, month=10, day=2)

START_YEAR = "2020"
START_MONTH = "07"
START_MONTH_NAME = "Jul"
START_DAY = "15"

END_YEAR = "2020"
END_MONTH = "08"
END_MONTH_NAME = "Aug"
END_DAY = "01"

CURRENT_YEAR = "2022"
CURRENT_MONTH = "05"
CURRENT_MONTH_NAME = "May"
CURRENT_DAY = "31"

FUTURE_YEAR = "2100"
FUTURE_MONTH = "10"
FUTURE_MONTH_NAME = "Oct"
FUTURE_DAY = "02"


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


@pytest.fixture
def mock_xr_load_dataset(mocker):
    """Mock GeoPandas file reading function."""
    ds = xr.Dataset()
    return mocker.patch(
        "aatoolbox.datasources.chirps.chirps.xr.open_mfdataset",
        return_value=ds,
    )


@pytest.fixture
def mock_get_current_date(mocker):
    return mocker.patch(
        ("aatoolbox.datasources.chirps.chirps.Chirps_daily"
        "._get_last_available_date"
        ),
        return_value=CURRENT_DATE
        )


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


@pytest.fixture
def mock_download(mocker, mock_chirps):
    """Create mock for download method."""
    download_mock = mocker.patch(
        "aatoolbox.datasources.chirps.chirps._Chirps._download"
    )

    mocker.patch(
        ("aatoolbox.datasources.chirps.chirps.ChirpsMonthly"
        "._get_last_available_date"
        ),
        return_value=CURRENT_DATE
    )

    def _mock_download(
        frequency: str,
        start_date,
        end_date,
    ):
        chirps = mock_chirps(frequency=frequency)
        chirps.download(
            start_date=start_date,
            end_date=end_date,
        )
        args_download = download_mock.call_args_list
        url_list = [k["url"] for (_, k) in args_download]
        filepath_list = [k["filepath"] for (_, k) in args_download]
        return url_list, filepath_list

    return _mock_download


def test_download_monthly(
    mock_aa_data_dir, mock_country_config, mock_download
):
    """Test of call download for monthly data."""
    url_list, filepath_list = mock_download(
        frequency="monthly",
        start_date=START_DATE,
        end_date=END_DATE,
    )

    url_list_control = [
        (
            "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
            ".CHIRPS/.v2p0/.monthly/.global/.precipitation/"
            f"X/%28-3.0%29%282.0%29RANGEEDGES/"
            f"Y/%286.0%29%283.2%29RANGEEDGES/"
            f"T/%28{MONTH_NAME}%20{START_YEAR}%29"
            f"%28{MONTH_NAME}%20{START_YEAR}"
            "%29RANGEEDGES/data.nc"
        )
        for MONTH_NAME in [
            calendar.month_abbr[month]
            for month in range(int(START_MONTH), int(END_MONTH) + 1)
        ]
    ]

    filepath_list_control = [
        mock_aa_data_dir
        / (
            f"public/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
            f"abc_chirps_monthly_{START_YEAR}_{MONTH:02d}_"
            "r0.05_Np6Sp3Ep2Wm3.nc"
        )
        for MONTH in range(int(START_MONTH), int(END_MONTH) + 1)
    ]

    assert url_list == url_list_control
    assert filepath_list == filepath_list_control


def test_download_daily(mock_aa_data_dir, mock_country_config, mock_download):
    """Test of call download for daily data."""

    url_list, filepath_list = mock_download(
        frequency="daily",
        start_date=START_DATE,
        end_date=END_DATE,
    )

    url_list_control = [
        (
            "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
            ".CHIRPS/.v2p0/.daily-improved/.global/.0p05/.prcp/"
            f"X/%28-3.0%29%282.0%29RANGEEDGES/"
            f"Y/%286.0%29%283.2%29RANGEEDGES/"
            f"T/%28{DAY:02d}%20{START_MONTH_NAME}%20{START_YEAR}"
            f"%29%28{DAY:02d}%20{START_MONTH_NAME}%20{START_YEAR}"
            "%29RANGEEDGES/data.nc"
        )
        for DAY in range(int(START_DAY), 32)
    ]

    url_list_control.append(
        "https://iridl.ldeo.columbia.edu/SOURCES/.UCSB/"
        ".CHIRPS/.v2p0/.daily-improved/.global/.0p05/.prcp/"
        f"X/%28-3.0%29%282.0%29RANGEEDGES/"
        f"Y/%286.0%29%283.2%29RANGEEDGES/"
        f"T/%2801%20{END_MONTH_NAME}%20{START_YEAR}"
        f"%29%2801%20{END_MONTH_NAME}%20{START_YEAR}"
        "%29RANGEEDGES/data.nc"
    )

    filepath_list_control = [
        mock_aa_data_dir
        / (
            f"public/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
            f"abc_chirps_daily_{START_YEAR}_{START_MONTH}_{DAY}_"
            "r0.05_Np6Sp3Ep2Wm3.nc"
        )
        for DAY in range(int(START_DAY), 32)
    ]

    filepath_list_control.append(
        mock_aa_data_dir
        / (
            f"public/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
            f"abc_chirps_daily_{START_YEAR}_{END_MONTH}_{END_DAY}_"
            "r0.05_Np6Sp3Ep2Wm3.nc"
        )
    )

    assert url_list == url_list_control
    assert filepath_list == filepath_list_control


def test_download_future_date_monthly(mocker, mock_chirps):

    mocker.patch(
        ("aatoolbox.datasources.chirps.chirps.ChirpsMonthly"
        "._get_last_available_date"
        ),
        return_value=CURRENT_DATE
    )

    with pytest.raises(ValueError):
        chirps = mock_chirps(frequency="monthly")
        chirps.download( 
            start_date=START_DATE, 
            end_date=FUTURE_DATE
            )


def test_download_future_date_daily(mocker, mock_chirps):

    mocker.patch(
        ("aatoolbox.datasources.chirps.chirps.ChirpsDaily"
        "._get_last_available_date"
        ),
        return_value=CURRENT_DATE
    )

    with pytest.raises(ValueError):
        chirps = mock_chirps(frequency="daily")
        chirps.download( 
            start_date=START_DATE, 
            end_date=FUTURE_DATE
            )


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
            f"abc_chirps_daily_{END_YEAR}_{END_MONTH}_{END_DAY}_"
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
        f"abc_chirps_daily_{END_YEAR}_{END_MONTH}_{END_DAY}_"
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
            f"abc_chirps_daily_{END_YEAR}_{END_MONTH}_{END_DAY}_"
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
