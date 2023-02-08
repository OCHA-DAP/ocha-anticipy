"""Tests for the IRI module."""
import cftime
import numpy as np
import pytest
import requests
import xarray as xr
from xarray.coding.cftimeindex import CFTimeIndex

from ochanticipy import GeoBoundingBox, IriForecastDominant, IriForecastProb

DATASOURCE_BASE_DIR = "iri"
FAKE_IRI_AUTH = "FAKE_IRI_AUTH"


@pytest.fixture
def mock_iri(mock_country_config):
    """Create IRI class with mock country config."""
    geo_bounding_box = GeoBoundingBox(
        lat_max=6, lat_min=3.2, lon_max=2, lon_min=-3
    )

    def _mock_iri(prob_forecast: bool = True):
        if prob_forecast:
            iri = IriForecastProb(
                country_config=mock_country_config,
                geo_bounding_box=geo_bounding_box,
            )
        else:
            iri = IriForecastDominant(
                country_config=mock_country_config,
                geo_bounding_box=geo_bounding_box,
            )
        return iri

    return _mock_iri


@pytest.fixture
def mock_download(mocker, mock_iri):
    """
    Call download with mocked _download.

    `forecast_type` is the type of forecast to
    test, can be either 'prob' or 'dominant'.
    """
    download_mock = mocker.patch(
        (
            "ochanticipy.datasources.iri.iri_seasonal_forecast"
            "._IriForecast._download"
        )
    )

    mocker.patch.dict(
        "ochanticipy.datasources.iri.iri_seasonal_forecast.os.environ",
        {"IRI_AUTH": FAKE_IRI_AUTH},
    )

    def _mock_download(prob_forecast: bool):
        iri = mock_iri(prob_forecast=prob_forecast)
        iri.download()
        _, kwargs_download = download_mock.call_args
        url = kwargs_download["url"]
        filepath = kwargs_download["filepath"]
        return url, filepath

    return _mock_download


def test_download_call_prob(
    mock_download, mock_aa_data_dir, mock_country_config
):
    """Test download for tercile probability forecast."""
    url, filepath = mock_download(prob_forecast=True)
    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
        ".NMME_Seasonal_Forecast/"
        ".Precipitation_ELR/.prob/X/%28-3.0%29%282.0%29RANGEEDGES/"
        "Y/%286.0%29%283.0%29RANGEEDGES/data.nc"
    )

    assert filepath == (
        mock_aa_data_dir
        / f"private/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
        f"abc_iri_forecast_seasonal_precipitation_"
        f"tercile_prob_Np6Sp3Ep2Wm3.nc"
    )


def test_download_call_dominant(
    mock_download, mock_aa_data_dir, mock_country_config
):
    """Test download for dominant tercile forecast."""
    url, filepath = mock_download(prob_forecast=False)
    assert url == (
        "https://iridl.ldeo.columbia.edu/SOURCES/.IRI/.FD/"
        ".NMME_Seasonal_Forecast/"
        ".Precipitation_ELR/.dominant/X/%28-3.0%29%282.0%29RANGEEDGES/"
        "Y/%286.0%29%283.0%29RANGEEDGES/data.nc"
    )

    assert filepath == (
        mock_aa_data_dir
        / f"private/raw/{mock_country_config.iso3}/{DATASOURCE_BASE_DIR}/"
        f"abc_iri_forecast_seasonal_"
        f"precipitation_tercile_dominant_Np6Sp3Ep2Wm3.nc"
    )


@pytest.fixture
def mock_requests(mocker):
    """Mock requests in the download function."""
    requests_mock = mocker.patch(
        "ochanticipy.datasources.iri.iri_seasonal_forecast.requests.get"
    )
    mocker.patch.dict(
        "ochanticipy.datasources.iri.iri_seasonal_forecast.os.environ",
        {"IRI_AUTH": FAKE_IRI_AUTH},
    )

    return requests_mock


def test_download_wrong_auth(
    mock_iri, mock_requests, mock_aa_data_dir, mock_country_config
):
    """Check that wrong download headers raise an error."""
    iri = mock_iri(prob_forecast=True)
    with pytest.raises(requests.RequestException):
        iri.download()


def test_process(mocker, mock_iri, mock_aa_data_dir, mock_country_config):
    """Test process for IRI forecast."""
    ds = xr.DataArray(
        np.reshape(a=np.arange(16), newshape=(2, 2, 2, 2)),
        dims=("L", "X", "Y", "F"),
        coords={
            "L": [1, 2],
            "X": [2, -3],
            "Y": [97, 90],
            "F": [685.5, 686.5],
        },
    ).to_dataset(name="prob")

    ds["F"].attrs["calendar"] = "360"
    ds["F"].attrs["units"] = "months since 1960-01-01"

    iri = mock_iri()

    # TODO: now created `load_raw` to be able to mock but would like
    #  to do it from xr.load_dataset directly
    mocker.patch(
        "ochanticipy.datasources.iri.iri_seasonal_forecast."
        "_IriForecast._load_raw",
        return_value=ds,
    )

    processed_path = iri.process()
    assert processed_path == (
        mock_aa_data_dir / f"private/processed/{mock_country_config.iso3}/"
        f"{DATASOURCE_BASE_DIR}/{mock_country_config.iso3}_"
        f"iri_forecast_seasonal_precipitation_"
        f"tercile_prob_Np6Sp3Ep2Wm3.nc"
    )

    da_processed = xr.load_dataset(processed_path)
    expected_f = CFTimeIndex(
        [
            cftime.datetime(year=2017, month=2, day=16, calendar="360_day"),
            cftime.datetime(year=2017, month=3, day=16, calendar="360_day"),
        ]
    )

    assert np.array_equal(da_processed.X.values, [2, -3])
    assert np.array_equal(da_processed.Y.values, [97, 90])
    assert da_processed.get_index("F").equals(expected_f)
    assert np.array_equal(da_processed.prob.values, ds.prob.values)


def test_process_if_download_not_called(mock_iri):
    """Test that correct error message raised."""
    iri = mock_iri()
    # Make sure file doesn't exist
    if iri._get_raw_path().exists():
        iri._get_raw_path().unlink()
    with pytest.raises(FileNotFoundError) as excinfo:
        iri.process()
    assert (
        "Make sure that you have already called the 'download' method"
        in str(excinfo.value)
    )


@pytest.fixture
def mock_xr_load_dataset(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch(
        "ochanticipy.datasources.iri.iri_seasonal_forecast.xr.load_dataset"
    )


def test_iri_load(
    mocker,
    mock_xr_load_dataset,
    mock_iri,
    mock_aa_data_dir,
    mock_country_config,
):
    """Test that load_codab calls the HDX API to download."""
    mocker.patch(
        (
            "ochanticipy.datasources.iri.iri_seasonal_forecast"
            "._IriForecast._download"
        )
    )

    iri = mock_iri()
    iri.load()
    mock_xr_load_dataset.assert_has_calls(
        [
            mocker.call(
                mock_aa_data_dir
                / f"private/processed/{mock_country_config.iso3}/"
                f"{DATASOURCE_BASE_DIR}/{mock_country_config.iso3}"
                f"_iri_forecast_seasonal_precipitation_"
                f"tercile_prob_Np6Sp3Ep2Wm3.nc"
            ),
        ]
    )


def test_load_if_process_not_called(mock_iri):
    """Test that correct error message raised."""
    iri = mock_iri()
    # Make sure file doesn't exist
    if iri._get_processed_path().exists():
        iri._get_processed_path().unlink()
    with pytest.raises(FileNotFoundError) as excinfo:
        iri.load()
    assert (
        "Make sure that you have already called the 'process' method"
        in str(excinfo.value)
    )
