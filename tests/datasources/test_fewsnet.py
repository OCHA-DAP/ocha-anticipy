"""Tests for the FewsNet module."""
from pathlib import Path

import pytest
from conftest import FAKE_AA_DATA_DIR, ISO2

from aatoolbox.datasources.fewsnet.fewsnet import FewsNet

MODULE_BASENAME = "fewsnet"


@pytest.fixture(autouse=True)
def mock_iso2(mocker):
    """Mock iso2 to iso3 conversion."""
    mocker.patch(
        "aatoolbox.datasources.fewsnet.fewsnet.Country.get_iso2_from_iso3",
        return_value=ISO2.upper(),
    )


def test_download_country(mock_country_config, mock_download_call, mocker):
    """Test that the correct country url and path is returned."""
    fewsnet = FewsNet(country_config=mock_country_config)
    url, output_path = mock_download_call(
        fewsnet_class=fewsnet,
        date_pub="2020-10-01",
        country_data=True,
        region_data=False,
    )
    assert (
        url == "https://fdw.fews.net/api/ipcpackage/"
        f"?country_code={ISO2.upper()}&collection_date=2020-10-01"
    )
    assert (
        output_path
        == Path(FAKE_AA_DATA_DIR)
        / "public"
        / "raw"
        / "glb"
        / MODULE_BASENAME
        / f"{ISO2.upper()}202010"
    )


def test_download_region(mock_country_config, mock_download_call):
    """Test that the correct region url and path is returned."""
    fewsnet = FewsNet(country_config=mock_country_config)
    url, output_path = mock_download_call(
        fewsnet_class=fewsnet,
        date_pub="2020-10-01",
        country_data=False,
        region_data=True,
    )

    assert (
        url == "https://fews.net/data_portal_download/download?"
        "data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
        "HFIC/EA/east-africa202010.zip"
    )
    assert (
        output_path
        == Path(FAKE_AA_DATA_DIR)
        / "public"
        / "raw"
        / "glb"
        / MODULE_BASENAME
        / "EA202010"
    )


# def test_download_nodata(mock_country_config, mock_download_call):
#     """Test that RuntimeError is returned when no data exists."""
#     with pytest.raises(RuntimeError) as e:
#         fewsnet = FewsNet(country_config=mock_country_config)
#         url, output_path = mock_download_call(
#             fewsnet_class=fewsnet,
#             date_pub="2020-10-01",
#             country_data=False,
#             region_data=False,
#         )
#         assert output_path is None
#     assert "No data found for 2020-10" in str(e.value)


@pytest.fixture
def mock_download_call(mock_fake_url, mock_countryregion):
    """Mock call to download."""
    url_mock = mock_fake_url

    def _get_country_mock(fewsnet_class, date_pub, country_data, region_data):
        mock_countryregion(country_data, region_data)

        output_path = fewsnet_class.download(date_pub=date_pub)

        _, kwargs_download_url = url_mock.call_args
        url = kwargs_download_url["url"]

        return url, output_path

    return _get_country_mock


@pytest.fixture
def mock_fake_url(mocker):
    """Mock url and unzip call."""
    fakedownloadurl = mocker.patch(
        "aatoolbox.datasources.fewsnet.fewsnet.download_url"
    )
    mocker.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")
    return fakedownloadurl


@pytest.fixture
def mock_countryregion(mocker):
    """Mock that no country and/or region data exists."""

    def _mock_countryregion(country_data: bool, region_data: bool):
        if country_data:
            return None
        else:
            if region_data:
                return mocker.patch(
                    "aatoolbox.datasources.fewsnet.fewsnet."
                    "FewsNet._download_country",
                    return_value=None,
                )
            else:
                return mocker.patch(
                    "aatoolbox.datasources.fewsnet.fewsnet."
                    "FewsNet._download_country",
                    return_value=None,
                ), mocker.patch(
                    "aatoolbox.datasources.fewsnet.fewsnet."
                    "Fewsnet._download_region",
                    return_value=None,
                )

    return _mock_countryregion
