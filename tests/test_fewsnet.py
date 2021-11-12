"""Tests for the FewsNet module."""
import pytest

from aatoolbox.datasources.fewsnet.fewsnet import download_fewsnet


def test_download_fewsnet_country(mock_download_call, tmp_path):
    """Test that the correct country url and path is returned."""
    url, output_path = mock_download_call(
        date_pub="2020-10-01",
        iso2="et",
        region_name="east-africa",
        region_code="EA",
        country_data=True,
        region_data=False,
    )

    assert (
        url == "https://fdw.fews.net/api/ipcpackage/"
        "?country_code=ET&collection_date=2020-10-01"
    )

    assert output_path == tmp_path / "ET202010"


def test_download_fewsnet_region(mock_download_call, tmp_path):
    """Test that the correct region url and path is returned."""
    url, output_path = mock_download_call(
        date_pub="2020-10-01",
        iso2="et",
        region_name="east-africa",
        region_code="EA",
        country_data=False,
        region_data=True,
    )

    assert (
        url == "https://fews.net/data_portal_download/download?"
        "data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
        "HFIC/EA/east-africa202010.zip"
    )

    assert output_path == tmp_path / "EA202010"


def test_download_fewsnet_nodata(mock_download_call):
    """Test that RuntimeError is returned when no data exists."""
    with pytest.raises(RuntimeError) as e:
        url, output_path = mock_download_call(
            date_pub="2020-10-01",
            iso2="et",
            region_name="east-africa",
            region_code="EA",
            country_data=False,
            region_data=False,
        )
        assert output_path is None
    assert "No data found for 2020-10" in str(e.value)


@pytest.fixture
def mock_download_call(mock_fake_url, mock_countryregion, tmp_path):
    """Mock call to download_fewsnet."""
    url_mock = mock_fake_url

    def _get_country_mock(
        date_pub, iso2, region_name, region_code, country_data, region_data
    ):
        mock_countryregion(country_data, region_data)

        output_path = download_fewsnet(
            date_pub=date_pub,
            iso2=iso2,
            region_name=region_name,
            region_code=region_code,
            output_dir=tmp_path,
            use_cache=False,
        )

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
                    "_download_fewsnet_country",
                    return_value=None,
                )
            else:
                return mocker.patch(
                    "aatoolbox.datasources.fewsnet.fewsnet."
                    "_download_fewsnet_country",
                    return_value=None,
                ), mocker.patch(
                    "aatoolbox.datasources.fewsnet.fewsnet."
                    "_download_fewsnet_region",
                    return_value=None,
                )

    return _mock_countryregion
